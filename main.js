"use strict";

/**
 * Electron main process.
 *
 * Owns: the window, the database, one engine per chain, the OpenSea Stream
 * connection, the Cancel pipeline, and the periodic cache cleanup.
 *
 * The renderer never touches network or keys - it only sends IPC messages and
 * renders the state pushed back to it.
 */

const path = require("path");
const fs = require("fs");
const { Wallet } = require("ethers");
const {
  app, BrowserWindow, ipcMain, shell, session, safeStorage, dialog, powerMonitor
} = require("electron");

/**
 * PROFILE GUARD — FIRST THING, BEFORE ANY PATH IS OPENED (1.25.2)
 *
 *   A sandboxed test launch (OSB_TEST_SANDBOX=1) that resolves to the owner's
 *   production profile exits here with code 87, before instance/DB setup can
 *   write. Real users never set the marker: no-op for them. See profile-guard.js.
 */
{
  const guard = require("./profile-guard").checkSandboxedProfile({
    env: process.env,
    userData: app.getPath("userData"),
    appData: app.getPath("appData"),
    localAppDataKnown: path.join(path.dirname(app.getPath("appData")), "Local")
  });
  // An UNPACKAGED run (npx electron / in-process test) resolves userData to the
  // owner's production profile unless it is a sandboxed test or explicit dev
  // mode (which uses separate "Dev" folders). Refuse instead of guessing.
  const env = String(process.env.OFFERBOT_ENV || "").toLowerCase();
  const devMode = env === "development" || env === "dev" || process.env.OFFERBOT_DEV === "1";
  const unpackagedUnsafe = !app.isPackaged && !devMode && process.env.OSB_TEST_SANDBOX !== "1";
  if (!guard.ok || unpackagedUnsafe) {
    process.stderr.write(`[PROFILE GUARD] refusing to start: ${guard.ok
      ? "unpackaged run without OSB_TEST_SANDBOX=1 (test) or OFFERBOT_ENV=development would open the production profile"
      : guard.reason}\n`);
    app.exit(87);
    process.exit(87);
  }
}

/**
 * CHẾ ĐỘ DEV ĐỔI DANH TÍNH TRƯỚC MỌI THỨ KHÁC
 *
 *   `OFFERBOT_ENV=development` cho bản đang sửa một profile riêng, một named
 *   pipe riêng và updater tắt — để nó chạy cạnh bản production đang gửi tiền
 *   thật mà không chạm vào profile, credential, nhịp ghi hay leader của bản
 *   đó. Phải nằm TRƯỚC `require("./instance")`: instance đọc userData ngay
 *   lúc nạp.
 */
const devRuntime = require("./dev-runtime");
const devIdentity = devRuntime.applyTo(app);

// First, and before anything that reads a path: this claims the instance slot
// and points userData at it. Requiring it later would let Chromium open the
// shared profile the slot exists to avoid.
const instance = require("./instance");

const { logger } = require("./logger");
const productionTrace = require("./production-trace");
productionTrace.configure(app.getPath("userData"));
const cache = require("./cache");
const rateLimiter = require("./rate-limiter");
const opensea = require("./opensea");
const nftSource = require("./nft-source");
const standardStore = require("./standard-store");
const linkList = require("./link-list");
const { profiler } = require("./latency");
const browser = require("./browser");
const wallet = require("./wallet");
const { ShardedOpenSeaStream: OpenSeaStream } = require("./stream-sdk");
/**
 * ENGINE OFFER ITEM CŨ KHÔNG CÒN TRONG SẢN PHẨM
 *
 *   Offer Item của cả hai dashboard là V2 (offer-item-v2/). Engine cũ nằm ở
 *   legacy/ — ngoài build.files, không vào gói — chỉ còn làm bản tham chiếu
 *   cho shadow parity. Luồng Bulk Offer vẫn cần luật ON TOP / OUTBID, và luật
 *   đó được trích nguyên văn ra offer-decision.js.
 */
// ---- Offer Item V2 -------------------------------------------------
//
// Chỉ Offer Item dùng những module này. Offer SLL, Offer Custom và Cancel
// giữ nguyên đường của chúng — V2 không đụng tới bộ giới hạn hay bulk engine
// mà ba tab kia đang dùng.
const { OfferItemV2Bridge } = require("./offer-item-v2/bridge");
const nftName = require("./offer-item-v2/nft-name");
const { QuotaBroker } = require("./offer-item-v2/quota-broker");
const { Database, CHAINS, normalizeChainName, previousSettingsFiles, validatePrices } = require("./db");
const { LicenseGuard, verifyLicense } = require("./license");
const errors = require("./error-registry");
const { Updater } = require("./updater");
const { LicenseClient } = require("./license-client");
const openseaSession = require("./session");
const { SecretStore, migrateSecrets, recoverFailedSecrets } = require("./secret-store");
const { WalletProfiles } = require("./wallet-profiles");
const { SharedCredentials } = require("./shared-credentials");
const { SharedBridge } = require("./shared-credentials-bridge");
const { RemoteHost } = require("./remote-host");
const { AddBatchGate } = require("./add-batch-gate");

// ------------------------------------------------------------------
// Globals
// ------------------------------------------------------------------

let mainWindow = null;
let db = null;
let stream = null;
let remoteHost = null;

/** Exactly-once protection for Add NFT IPC retries and double UI callbacks. */
const addBatchGate = new AddBatchGate();

/** Canonical token id: decimal without leading zeros ("007" and "7" are one token). */
function canonicalTokenId(tokenId) {
  const s = String(tokenId ?? "").trim();
  return /^\d+$/.test(s) ? BigInt(s).toString() : s;
}

/**
 * NFT identity for Add: chain + lowercase contract + canonical tokenId.
 * Never the name, the collection, the row number or the raw URL string.
 */
function nftIdentity(chain, contract, tokenId) {
  return `${normalizeChainName(chain)}:${String(contract || "").toLowerCase()}:${canonicalTokenId(tokenId)}`;
}

/**
 * Identities an Add batch is working on RIGHT NOW (between its duplicate check
 * and its commit). A second batch - a fast second click with different link
 * text, or Remote - sees them as already present, so one NFT can never be
 * fetched and committed twice. Released when the owning batch ends.
 */
const addReservations = new Set();

function duplicateMessage(count) {
  return count === 1 ? "NFT này đã có trong danh sách" : `${count} NFT đã có trong danh sách`;
}

/** Licence gate. Every money-moving action checks this first. */
const license = new LicenseGuard();

/** The server URL the licence client was last built for. */
let licenseServerUrl = null;

/** Encrypted credential store. Only main ever touches plaintext secrets. */
let secrets = null;
/** Latest credential template for a newly created Tool only; existing Tools
 * always use their own encrypted store. */
let credentialDefaults = null;
/** This Tool's stored wallets (one active). Per slot, like secrets.json. */
let walletProfiles = null;

/**
 * The licence, shared by every window on this PC.
 *
 * Separate from `secrets` because it has a different scope: `secrets` is the
 * slot's own credentials, this is the machine's one entitlement. Same DPAPI
 * encryption, same file format - only the location differs.
 */
let licenceVault = null;

/**
 * Kho credential MÃ HOÁ dùng chung với tool Bulk Offer Cancel (cùng tài khoản
 * Windows). Chỉ slot 1 (primary) đồng bộ; Tool khác không bao giờ đọc/ghi kho
 * này. Lỗi của kho chung không bao giờ làm hỏng OfferBot.
 */
let sharedBridge = null;
/** True while a value taken FROM the shared store is being applied (no echo back). */
let sharedAdopting = false;

/**
 * Điều phối hạn mức GHI của Offer Item, dùng chung cho cả máy.
 *
 * 1.19.4 làm mọi cửa sổ dùng chung một OpenSea API key, nên "mỗi tiến trình
 * một bộ giới hạn" trở thành ba cửa sổ × 2 lệnh/giây = 6 lệnh/giây đập vào
 * một key. Broker này bầu một tiến trình làm leader qua named pipe và cấp
 * giấy phép cho tất cả — xem offer-item-v2/quota-broker.js.
 *
 * CHỈ Offer Item. Offer SLL và Cancel giữ bộ giới hạn cũ của chúng.
 */
let offerItemQuota = null;

/** chain -> engine instance. A Map: use .get / .set, never .add. */
const engines = new Map();

/** Timers cleared on quit so nothing keeps the process alive. */
const timers = [];

const CACHE_SWEEP_MS = 60 * 1000;

/** How often the realtime latency percentiles are written to the log. */
const LATENCY_REPORT_MS = 60 * 1000;
const CHROMIUM_CACHE_CLEAR_MS = 30 * 60 * 1000;

/**
 * How many NFTs a batch scan prices at once.
 *
 * Each Best Offer read fans out into up to THREE parallel sub-requests, so this
 * is multiplied by three at the socket. Measured on live data, 12 NFTs:
 *
 *   concurrency 5 -> 15 in flight -> 2x 429 -> the limiter ratchets down -> 19.8s
 *   concurrency 3 ->  9 in flight -> 0x 429 -> 765ms
 *   concurrency 2 ->  6 in flight -> 0x 429 -> 673ms
 *
 * Three is the point where the batch stays fast without ever tripping the
 * limit; going wider is 26x SLOWER because the 429 penalty dwarfs the gain.
 */
const BATCH_SCAN_CONCURRENCY = 3;

/** Ordinary log lines kept after each automatic cleanup. */
const LOG_KEEP_LINES = 300;

// ------------------------------------------------------------------
// Renderer messaging
// ------------------------------------------------------------------

/**
 * REMOTE = CÙNG RENDERER CỦA WINDOWS (port 1.19.61, 1.25.1)
 *
 *   Remote chạy đúng dashboard/Offer SLL/Cancel/Logs của Windows với một
 *   botAPI shim gửi lệnh qua WebSocket. Sự kiện tiến trình/log đi tới Remote
 *   như tới cửa sổ này; TRẠNG THÁI engine đi theo snapshot gộp ~1/giây — không
 *   bao giờ mỗi sự kiện Stream một khung.
 */
const REMOTE_EVENT_CHANNELS = new Set(["bot:log",
  "license:state", "api:state", "session:status"]);

function send(channel, payload) {
  if (REMOTE_EVENT_CHANNELS.has(channel) && remoteHost) {
    try { remoteHost.pushEvent(channel, payload); } catch { /* remote không được làm hỏng cửa sổ */ }
  }
  if (!mainWindow || mainWindow.isDestroyed()) return;
  try {
    mainWindow.webContents.send(channel, payload);
  } catch {
    /* the window is going away */
  }
}

/**
 * Lệnh Remote chạy CHÍNH handler IPC của cửa sổ (cùng cổng licence, cùng job
 * lock) — chỉ những kênh trong danh sách cho phép. Settings/credential/update
 * không có ở đây: chúng là việc của Windows.
 */
const localIpcHandlers = new Map();
const REMOTE_IPC_ALLOW = new Set([
  "bot:getState", "bot:getLogs", "bot:addNfts", "bot:updateNft", "bot:deleteNft", "bot:deleteNfts",
  "bot:start", "bot:pause", "bot:stop", "bot:emergencyStop", "bot:togglePause", "bot:setSaveEnabled", "bot:saveChainConfig",
  "session:status", "bot:keyStats"
]);
{
  const nativeHandle = ipcMain.handle.bind(ipcMain);
  ipcMain.handle = (channel, fn) => { localIpcHandlers.set(channel, fn); return nativeHandle(channel, fn); };
}

/**
 * Snapshot cho Remote: đúng hình dạng renderer cần, KHÔNG có secret —
 * settings chỉ lấy các trường hiển thị, không API key / Private Key / secrets.
 */
function remoteState() {
  const chainsState = {};
  for (const chain of CHAINS) {
    try { chainsState[chain] = getEngine(chain).getState(); }
    catch (error) { chainsState[chain] = { chain, rows: [], error: String(error && error.message || error) }; }
  }
  const settings = publicSettings(db.getSettings());
  let streamStatus = null;
  try { streamStatus = stream ? stream.status() : { connected: false }; } catch { streamStatus = { connected: false }; }
  return {
    version: app.getVersion(),
    wallet: (() => { try { return getEngine("ethereum").walletAddress || null; } catch { return null; } })(),
    settings: { theme: settings.theme, ethereum: settings.ethereum, robinhood: settings.robinhood,
      bulkOfferPrice: settings.bulkOfferPrice, bulkOfferDuration: settings.bulkOfferDuration },
    license: (() => { try { return license.status(); } catch { return null; } })(),
    api: (() => { try { return apiStatus(); } catch { return null; } })(),
    stream: streamStatus,
    chains: chainsState,
    logs: logger.history().slice(-80)
  };
}

/**
 * How often the app asks the licence server whether it may still run.
 *
 * Sixty seconds, on one timer. The client believes an answer for exactly this
 * long, so the tick and the cache agree by construction instead of by luck.
 */
const LICENSE_RECHECK_MS = 60 * 1000;

/**
 * THE licence server. One address, compiled in.
 *
 * It used to come from settings, which defaulted to an empty string - and an
 * empty string meant no client, and no client meant the guard fell back to
 * checking the signature alone. A correctly signed key then ran on any number
 * of machines, and Suspend in the Manager reached none of them, because
 * nothing ever asked. There was no field in the UI to fill in either, so that
 * was not a configuration people had got wrong: it was the only state the
 * shipped app could be in.
 */
const LICENSE_SERVER_URL = "https://offerbot-license.kingluanmeow.workers.dev";

/**
 * MÁY KHÁCH LUÔN HỎI ĐÚNG MỘT SERVER — KHÔNG BAO GIỜ ĐỌC `settings.licenseServerUrl`
 *
 *   Đo trên máy thật (1.19.51): settings.json còn giữ override cũ
 *   `http://127.0.0.1:61575` từ một lần chạy test; bản EXE đọc nó, hỏi một
 *   server không tồn tại, hết grace offline 12h thì KHOÁ app dù Internet và
 *   OpenSea vẫn chạy. Địa chỉ server không phải là một cài đặt của khách.
 *
 *   Tái phát ở 1.25.x (2026-09-24): cây source 1.25 không mang bản sửa
 *   1.19.52; settings production giữ `http://127.0.0.1:63183`, lần check
 *   thành công cuối 01:06, 13:06 app khoá "Đã 12 giờ không liên hệ được máy
 *   chủ license" trong khi server thật vẫn trả lời. Port lại nguyên văn.
 *
 *   Bản đóng gói: LUÔN là LICENSE_SERVER_URL. Không packaged (dev/test): env
 *   `OSB_LICENSE_SERVER_URL`, rồi tới giá trị lưu (smoke đứng server loopback
 *   qua saveSettings). Loopback không bao giờ được dùng trong bản đóng gói.
 */
function resolveLicenseServerUrl(settings = {}) {
  // A packaged EXE launched by the test sandbox (temp profile, OSB_TEST_SANDBOX=1
  // set only by test-exe-sandbox) may use the loopback server the suite stood
  // up. A real install never has that flag, so it always asks production.
  const sandboxed = process.env.OSB_TEST_SANDBOX === "1";
  if (app.isPackaged && !sandboxed) return LICENSE_SERVER_URL;
  const fromEnv = app.isPackaged ? "" : String(process.env.OSB_LICENSE_SERVER_URL || "").trim();
  if (fromEnv) return fromEnv;
  return String(settings.licenseServerUrl || "").trim() || LICENSE_SERVER_URL;
}

/**
 * Gỡ override cũ khỏi settings của bản đóng gói, một lần, lúc boot/update.
 * Trả về true khi có gì để gỡ (để log). Không bao giờ chạm bản dev.
 */
function scrubLicenseServerOverride() {
  if (!app.isPackaged || process.env.OSB_TEST_SANDBOX === "1") return false;
  let stale = "";
  try { stale = String(db.getSettings().licenseServerUrl || "").trim(); } catch { stale = ""; }
  if (!stale) return false;
  db.updateSettings({ licenseServerUrl: "" });
  logger.license(`gỡ override server cũ khỏi settings (${stale}) — bản đóng gói luôn dùng ${LICENSE_SERVER_URL}`);
  return true;
}

const SECRET_KEYS = ["apiKeys", "apiKey", "apiKey2", "privateKey", "licenseKey"];

/**
 * Settings as the renderer is allowed to see them.
 *
 * Secret VALUES never cross the IPC boundary. The tab only needs to know that
 * a credential exists so it can show a masked placeholder; sending the key
 * itself would put it in the page, in devtools, and in any renderer crash dump
 * for no benefit at all.
 */
/**
 * What the renderer is allowed to know about a session.
 *
 * Wallet addresses are public on-chain identifiers, and the user needs to see
 * WHICH wallet is signed in to act on a wrong-wallet state. Nothing else from
 * the session crosses the bridge.
 */
/** The wallet the stored Private Key derives, or null. */
function walletAddressFromPrivateKey(privateKey) {
  const raw = String(privateKey || "").trim();
  const normalized = raw.startsWith("0x") ? raw : `0x${raw}`;
  if (!/^0x[a-fA-F0-9]{64}$/.test(normalized)) return null;
  try { return new Wallet(normalized).address; } catch { return null; }
}

function configuredWallet() {
  try {
    return walletAddressFromPrivateKey(db.getSettings().privateKey);
  } catch {
    return null;
  }
}

function publicSession(state) {
  return {
    state: state.state,
    message: openseaSession.describe(state),
    sessionAddress: state.sessionAddress || null,
    walletAddress: state.walletAddress || null
  };
}

/**
 * Settings as the window may see them.
 *
 * The licence key and the API key travel in full, because the product
 * requires them to be readable in Settings: a key you cannot read is a key
 * you cannot check, and hiding them behind "đã lưu" is what let a Save wipe
 * one. Neither is a spending credential.
 *
 * The PRIVATE KEY never leaves main. It is the one credential here that moves
 * money, so the window is not told it at all - not to display, not to
 * round-trip. Settings shows presence for it and nothing more.
 */
const VISIBLE_SECRETS = Object.freeze([
  "licenseKey", "apiKeys", "apiKey", "apiKey2"
]);

function publicSettings(settings) {
  const copy = { ...settings };
  for (const key of SECRET_KEYS) {
    copy[key] = VISIBLE_SECRETS.includes(key) ? (settings[key] || "") : "";
  }
  return copy;
}

/**
 * Is there an OpenSea API key the app can actually USE right now?
 *
 * One question, one answer, asked of the thing that will do the asking: the
 * key manager is what every request draws from, so its view is the only view
 * that can be right. Reading db or the SecretStore instead is how a key that
 * is stored but not loaded reads as present.
 *
 * Never returns or logs the key itself.
 */
function credentialPresent() {
  try {
    return String(rateLimiter.apiKeys.peek() || "").trim().length > 0;
  } catch {
    return false;
  }
}

/** Which credentials are stored, and whether this machine can encrypt. */
/**
 * Những credential ĐÃ LƯU nhưng KHÔNG giải mã được trên tài khoản Windows
 * này. Tên trường, không bao giờ là giá trị.
 *
 * Điền một lần lúc khởi động và giữ nguyên cho tới khi người dùng nhập lại.
 */
let undecryptableSecrets = [];

function secretStatus() {
  if (!secrets) return { available: false, present: {}, undecryptable: [] };
  const status = secrets.status();
  return {
    available: status.available,
    present: status.present,
    /**
     * TỪNG CHỈ LÀ MỘT DÒNG LOG.
     *
     * DPAPI gắn với tài khoản Windows. Hồ sơ được khôi phục từ máy khác,
     * hoặc chạy dưới một tài khoản khác, thì blob còn nguyên mà mở không
     * ra. App ghi một dòng [ERROR] rồi chạy tiếp với ô trống — nên người
     * dùng thấy Settings rỗng, bot không làm gì, và không có gì nói vì sao.
     *
     * Báo cáo thật của khách hàng "Loi" đúng là cảnh này.
     */
    undecryptable: undecryptableSecrets.slice()
  };
}

/**
 * WORKFLOW GATE.
 *
 * What has actually completed, per tab. A disabled button is a hint the
 * renderer offers; this is the rule. Anything that can reach the IPC channel
 * can skip the button, and a cancel with no scan behind it would act on a
 * snapshot nobody took.
 *
 * Keyed on the link list itself, not on a boolean: reading one list and then
 * cancelling a different one is the same mistake as never reading at all.
 */
const workflow = {
  cancel: { read: "", scanned: "" },
  offer: { read: "" }
};

/** A stable fingerprint of a link list, so two lists cannot be confused. */
function linkFingerprint(links) {
  // Cùng một luật tách với đường thêm NFT: dấu phẩy trong query string của
  // OpenSea không phải dấu phân tách (xem link-list.js).
  const text = linkList.splitLinks(links)
    .slice()
    .sort()
    .join("\n");
  if (!text) return "";
  return require("crypto").createHash("sha256").update(text).digest("hex");
}

function pushEngineState(state) {
  send("bot:update", state);
  remoteHost?.broadcast();
}

async function remoteCommand(payload = {}) {
  if (payload.action === "ipc") {
    const channel = String(payload.channel || "");
    if (!REMOTE_IPC_ALLOW.has(channel)) return { ok: false, error: `Lệnh remote không được phép: ${channel}` };
    if (channel === "bot:getLogs") return { ok: true, logs: logger.history().slice(-200) };
    if (channel === "bot:getState") return { ok: true, ...remoteState() };
    const fn = localIpcHandlers.get(channel);
    if (!fn) return { ok: false, error: `Chưa có handler: ${channel}` };
    const out = await fn(null, payload.payload || {});
    return out && typeof out === "object" ? out : { ok: true, value: out };
  }
  const chain = normalizeChainName(payload.chain);
  const engine = getEngine(chain);
  const urls = Array.isArray(payload.urls) ? payload.urls.map(String).slice(0, 200) : null;
  if (payload.action === "start") {
    const gate = await requireLicense();
    if (!gate.ok) return gate;
    const result = await engine.start(urls);
    engine.emitNow();
    return result;
  }
  if (payload.action === "pause" || payload.action === "stop") {
    const result = engine[payload.action](urls);
    engine.emitNow();
    return result;
  }
  if (payload.action === "remove") {
    if (!urls?.length) return { ok: false, error: "Chưa chọn NFT." };
    for (const url of urls) engine.removeRow(url);
    const result = db.deleteNfts(chain, urls);
    if (stream) stream.setCollections(allCollectionSlugs());
    engine.emitNow();
    return result;
  }
  if (payload.action === "add") {
    const links = linkList.splitLinks(payload.links).slice(0, 100);
    if (!links.length) return { ok: false, error: "Chưa nhập link NFT." };
    const cfg = { ...db.getSettings()[chain], ...(payload.config || {}) };
    const check = validatePrices(cfg);
    if (!check.ok) return check;
    /**
     * REMOTE ADD: ĐĂNG KÝ HÀNG TRƯỚC, METADATA SAU (1.25.0)
     *
     *   Bản cũ gọi fetchNftMeta cho TỪNG link rồi mới db.addNft — tên/ảnh
     *   chặn việc thêm hàng. Nay: parse + loại trùng cục bộ → hàng tối thiểu
     *   vào DB + engine ngay → tên/ảnh/slug giải ở nền (tuần tự, có trần).
     *   Slug về thì Stream đăng ký collection đó ngay.
     */
    const added = [], errors = [], pendingMeta = [];
    let duplicates = 0;
    const identity = p => nftIdentity(chain, p.contract, p.tokenId);
    const tracked = new Set(db.getNfts(chain).map(identity));
    for (const link of links) {
      const parsed = opensea.parseOpenSeaUrl(link, chain);
      if (!parsed || parsed.chain !== chain) { errors.push(`Link không hợp lệ: ${link}`); continue; }
      parsed.tokenId = canonicalTokenId(parsed.tokenId);
      parsed.url = opensea.canonicalItemUrl(chain, parsed.contract, parsed.tokenId) || parsed.url;
      if (tracked.has(identity(parsed)) || addReservations.has(identity(parsed))) { duplicates++; continue; }
      tracked.add(identity(parsed));
      const row = {
        ...parsed,
        name: `#${parsed.tokenId}`,
        image: "",
        collection: "",
        collectionSlug: "",
        minPrice: cfg.minPrice, maxPrice: cfg.maxPrice, step: cfg.step,
        duration: cfg.duration, priorityMode: false
      };
      const stored = db.addNft(chain, row);
      if (!stored.ok) {
        if (stored.code === "DUPLICATE") duplicates++;
        else errors.push(`${parsed.tokenId}: ${stored.error}`);
        continue;
      }
      const runtime = engine.addRow(stored.nft);
      added.push(stored.nft.url);
      pendingMeta.push({ parsed, url: stored.nft.url, runtime });
    }
    engine.emitNow();
    if (pendingMeta.length) resolveRemoteMeta(chain, engine, pendingMeta);
    if (!added.length && duplicates && !errors.length) {
      return { ok: false, code: "DUPLICATE", added: 0, duplicates, errors, error: duplicateMessage(duplicates) };
    }
    return { ok: added.length > 0 || errors.length === 0, added: added.length, duplicates, errors };
  }
  return { ok: false, error: "Lệnh remote không được phép." };
}

/**
 * Tên/ảnh/slug cho hàng Remote vừa thêm — NỀN, tuần tự, không chặn Add.
 * Slug là thứ duy nhất ảnh hưởng tới Offer (Stream + Collection Offer); tên
 * và ảnh chỉ để hiển thị.
 */
function resolveRemoteMeta(chain, engine, entries) {
  (async () => {
    let slugAdded = false;
    for (const { parsed, url, runtime } of entries) {
      if (shuttingDown) return;
      try {
        const meta = await opensea.fetchNftMeta(chain, parsed.contract, parsed.tokenId);
        if (!meta) continue;
        const patch = {};
        if (meta.name) patch.name = meta.name;
        if (meta.image) patch.image = meta.image;
        if (meta.collectionName) patch.collection = meta.collectionName;
        if (meta.collectionSlug) patch.collectionSlug = String(meta.collectionSlug).toLowerCase();
        if (!Object.keys(patch).length) continue;
        try { db.updateNft(chain, url, patch); } catch { /* db là phụ */ }
        if (runtime && typeof runtime === "object") {
          if (patch.name) { runtime.name = patch.name; runtime.namePlaceholder = false; runtime.nameCheckedAt = Date.now(); }
          if (patch.image && !runtime.image) runtime.image = patch.image;
          if (patch.collection && !runtime.collection) runtime.collection = patch.collection;
          if (patch.collectionSlug && !runtime.collectionSlug) { runtime.collectionSlug = patch.collectionSlug; slugAdded = true; }
        }
      } catch (error) {
        logger.engine(`[REMOTE ADD] #${parsed.tokenId} metadata: ${opensea.describeError(error)}`);
      }
    }
    if (slugAdded && stream) stream.setCollections(allCollectionSlugs());
    try { engine.emitNow(); } catch { /* engine đang đóng */ }
  })().catch(error => logger.error(`[REMOTE ADD] metadata: ${error.message}`));
}

// ------------------------------------------------------------------
// Engines
// ------------------------------------------------------------------

function createEngines() {
  /**
   * OFFER ITEM V2 SỞ HỮU CẢ HAI DASHBOARD
   *
   *   Không có cờ chuyển, không có đường lùi ngầm về engine cũ. Hai kiến trúc
   *   chạy song song nghĩa là hai kiến trúc phải bảo trì, và một sự cố sẽ
   *   không nói được nó thuộc về bên nào.
   *
   *   Engine cũ (`OfferEngine`, `RobinhoodEngine`) đã rời khỏi cây production:
   *   nằm ở legacy/, ngoài build.files, và main.js không require nó. Xem
   *   production-import-graph-test.js — bài test ghim đúng điều này.
   */
  const bridgeFor = (chain, label) => new OfferItemV2Bridge({
    chain, label, db,
    quota: offerItemQuota,
    // Cùng pool key mà phần còn lại của app dùng. Lượt GHI nhận DANH SÁCH
    // mọi key; QuotaBroker chọn key ghi được sớm nhất ngay lúc cấp phép.
    getApiKey: () => rateLimiter.apiKeys.peek(Date.now(), { write: true }),
    getApiKeys: () => rateLimiter.apiKeys.writeKeys(),
    onUpdate: pushEngineState,
    onLog: line => logger.engine(line)
  });

  engines.set("ethereum", bridgeFor("ethereum", "ETHEREUM"));
  engines.set("robinhood", bridgeFor("robinhood", "ROBINHOOD"));


  // The engines spend money on their own schedule, so they carry their own
  // licence gate. requireLicense covers IPC handlers - a person pressing a
  // button - and a run already in flight never touches one.
  // Two forms of ONE verdict.
  //
  //   requireLicense       awaits a refresh when the cached answer is stale.
  //                        Runs before any scarce resource is taken.
  //   license.requireValid non-blocking: the same evaluate(), from the
  //                        already-signed local cache, and it still kicks the
  //                        background refresh without awaiting it. Runs at the
  //                        last moment before the POST, where a network wait
  //                        would be holding a write grant the whole process
  //                        shares.
  //
  // Neither weakens the gate: same signature-first evaluation, same offline
  // grace, same fail-closed, same revoke watcher.
  for (const engine of engines.values()) {
    // V2 dùng MỘT cổng, gọi ngay trước khi ký. Nó là bản không chặn
    // (`requireValid`) vì lúc đó không được phép chờ mạng — đúng ngữ nghĩa
    // "nửa sau" của cổng hai nửa mà v1.19.4 đã đặt ra.
    engine.attachLicenceGate(() => license.requireValid());
  }
}

function getEngine(chain) {
  return engines.get(normalizeChainName(chain)) || engines.get("ethereum");
}

/**
 * HAI Ô KEY: Ô 1 = KEY 1 (PRIMARY), Ô 2 = KEY 2 (SECONDARY)
 *
 *   Trước 1.19.37 Settings có MỘT ô `apiKeys` nhận danh sách phân cách bằng
 *   dấu phẩy/xuống dòng. Nay mỗi key có ô riêng, lưu riêng, mã hoá riêng:
 *   `apiKeys` giữ đúng MỘT key (Key 1) để hồ sơ cũ một-key không phải đổi
 *   gì; `apiKey2` là Key 2 và được để trống. Một hồ sơ cũ có hai key trong
 *   `apiKeys` được tách ra một lần ở đây — key thứ nhất ở lại, key thứ hai
 *   sang `apiKey2` nếu ô đó còn trống. Trường `apiKey` cổ (bản đầu) được
 *   dọn về `apiKeys`. Không key nào bị đổi giá trị, chỉ đổi ô.
 *
 *   Trả về true khi có ghi lại — để chỗ gọi biết phải applySettings().
 */
function normalizeApiKeySlots() {
  const settings = db.getSettings();
  const split = value => String(value || "").split(/[\r\n,;]+/).map(v => v.trim()).filter(Boolean);
  const list = [...split(settings.apiKeys), ...split(settings.apiKey)];
  const unique = [...new Set(list)];
  let key1 = unique[0] || "";
  let key2 = String(settings.apiKey2 || "").trim();
  if (!key2 && unique[1]) key2 = unique[1];
  if (key2 && key2 === key1) key2 = "";
  const dropped = unique.filter(k => k !== key1 && k !== key2).length;

  const changed = {};
  if ((settings.apiKeys || "") !== key1) changed.apiKeys = key1;
  if ((settings.apiKey || "") !== "") changed.apiKey = "";
  if ((settings.apiKey2 || "") !== key2) changed.apiKey2 = key2;
  if (!Object.keys(changed).length) return false;

  db.updateSettings(changed);
  if (secrets && secrets.isAvailable()) secrets.save(changed);
  if (credentialDefaults && credentialDefaults.isAvailable()) credentialDefaults.save(changed);
  sharedPush(changed);
  logger.engine(`[API] sắp lại ô key: Key 1 ${key1 ? "có" : "trống"} · Key 2 ${key2 ? "có" : "trống"}` +
    (dropped ? ` · bỏ ${dropped} key thừa (chỉ giữ hai)` : ""));
  return true;
}

/**
 * Ảnh chụp trạng thái hai key cho LiveView. Không bao giờ chứa key: chỉ vân
 * tay, vai trò, hàng đợi, in-flight, cooldown, số 429 và phân bổ request.
 * Mọi con số đều đọc từ chính bộ quản lý key / bộ điều phối đọc / broker —
 * không có gì được tính riêng cho màn hình.
 */
function keyStatsSnapshot() {
  const keys = rateLimiter.apiKeys.stats();
  let reads = { queued: 0, queuedCritical: 0, queuedBackground: 0, active: 0, keys: [] };
  try { reads = opensea.readDispatcher.stats(); } catch { /* chưa có */ }
  let quota = {};
  try { quota = offerItemQuota && typeof offerItemQuota.states === "function" ? offerItemQuota.states() : {}; } catch { quota = {}; }
  const buckets = rateLimiter.stats().buckets || [];
  const order = buckets.find(b => b.kind === "order") || null;
  const now = Date.now();
  const slots = (keys.detail || []).map(entry => {
    const read = reads.keys.find(k => k.fp === entry.fp) || null;
    const q = quota[entry.fp] || quota[String(entry.fp)] || null;
    return {
      slot: entry.slot,
      role: entry.role,
      fp: entry.fp,
      active: !entry.blocked,
      cooldownMs: entry.cooldownMs,
      inFlight: entry.inFlight,
      writesInFlight: entry.writesInFlight,
      rateLimits: entry.rateLimits,
      recentRateLimits: entry.recentRateLimits,
      errors: entry.errors,
      latencyMs: entry.latencyMs,
      served: entry.served || { critical: 0, background: 0, write: 0, cancel: 0 },
      readRps: read ? Number(read.rps.toFixed(2)) : null,
      readActive: read ? read.active : 0,
      readBackground: read ? read.background : 0,
      readCooldownMs: read ? Math.round(read.cooldownMs) : 0,
      // Broker ghi (Offer Item): nhịp tối thiểu giữa hai POST, chặn sau 429,
      // lượt ghi kế tiếp còn cách bao lâu — theo domain = vân tay key này.
      writeBlockedMs: q ? Math.round(q.blockedForMs || 0) : 0,
      writePaceMs: q ? Math.round(q.paceMinMs || 0) : null,
      writeNextInMs: q ? Math.round(q.nextInMs || 0) : 0,
      writeRemaining: q && q.remaining !== undefined && q.remaining !== null ? q.remaining : null
    };
  });
  return {
    at: now,
    count: slots.length,
    healthy: keys.healthy,
    slots,
    readQueue: { total: reads.queued, critical: reads.queuedCritical || 0, background: reads.queuedBackground || 0, active: reads.active },
    writeQueue: order ? { queued: order.queued || 0, inFlight: order.active || 0 } : null
  };
}

let keyStatsTimer = null;
let lastKeyStatsSignature = "";
function startKeyStatsPush() {
  if (keyStatsTimer) return;
  keyStatsTimer = setInterval(() => {
    if (!mainWindow || mainWindow.isDestroyed()) return;
    try {
      const snapshot = keyStatsSnapshot();
      // Đẩy khi có gì đổi (ngoài mốc thời gian) hoặc mỗi 5s để cooldown chạy.
      const sig = JSON.stringify({ ...snapshot, at: 0 });
      const due = (snapshot.at - (Number(lastKeyStatsSignature.split("|")[0]) || 0)) >= 5000;
      if (sig !== lastKeyStatsSignature.split("|").slice(1).join("|") || due) {
        lastKeyStatsSignature = `${snapshot.at}|${sig}`;
        send("bot:keyStats", snapshot);
      }
    } catch { /* màn hình không được làm hỏng runtime */ }
  }, 1000);
  keyStatsTimer.unref?.();
}

/** Apply saved settings to the key manager, both engines and the stream. */
function applySettings() {
  const settings = db.getSettings();

  rateLimiter.apiKeys.configure(settings);
  // Pool key đổi lúc đang chạy → broker theo key mới (không log key, chỉ fingerprint).
  if (offerItemQuota && typeof offerItemQuota.refreshIdentity === "function") {
    offerItemQuota.refreshIdentity()
      .catch(error => logger.error("[QUOTA] không làm mới được danh tính: " + error.message));
  }

  //
  // Pushed rather than pulled, so the client never has to know what a database
  // is. A blank field is not an error: it means "use the key the app ships
  // with", and clearing the box is how you go back to it. The breaker is
  // cleared on a change because a NEW key deserves a fresh attempt even if the
  // previous one had been refused.

  // ---- the server half of the licence -----------------------------
  //
  // Rebuilt whenever the URL changes, and attached before the key is set so a
  // new key activates against the right server. With no URL configured the
  // guard falls back to signature-only, which is exactly how it behaved before
  // there was a server - an owner who has not set one up is not locked out.
  // ---- where the licence is checked --------------------------------
  //
  // A CONSTANT, not a setting. Every shipped copy asks the same server, and
  // there is no field anywhere in the UI to change it - so a customer cannot
  // end up unlicensed because a box was blank, and cannot point the app
  // somewhere else to avoid the question.
  //
  // The stored value survives only as a development override, used by the
  // test suites to stand up a server on a loopback port. It is not a hole:
  // every answer carries an Ed25519 signature from the owner's key, so a
  // server the app is redirected to cannot say ACTIVE and be believed. The
  // signature is the defence; the address never was.
  const serverUrl = resolveLicenseServerUrl(settings);
  if (serverUrl !== licenseServerUrl) {
    licenseServerUrl = serverUrl;
    license.attachClient(serverUrl
      ? new LicenseClient({
          // The cache is per instance; the DEVICE is per PC. Every instance on
          // this machine reads the same device.json and therefore presents the
          // same deviceId, so running Ethereum and Robinhood side by side, or
          // three slots at once, is still one licence device.
          directory: settingsLocation().directory,
          machineDirectory: instance.get().primarySettingsBase,
          serverUrl,
          appVersion: app.getVersion(),
          platform: process.platform,
          onLog: line => logger.license(line)
        })
      : null);
    // The "(chưa cấu hình) — chỉ kiểm chữ ký cục bộ" branch is gone with the
    // state it described: serverUrl cannot be empty any more.
    logger.license(serverUrl === LICENSE_SERVER_URL
      ? `server = ${serverUrl}`
      : `server = ${serverUrl} (override để phát triển)`);
  }

  const licenseState = license.setKey(settings.licenseKey);
  if (settings.licenseKey) {
    logger.license(
      licenseState.valid
        ? `OK - ${licenseState.name || "user"}, còn ${licenseState.daysLeft} ngày`
        : `TỪ CHỐI - ${licenseState.reason}`
    );
  }

  for (const engine of engines.values()) engine.configure(settings);
  // SAVE khôi phục cấu hình và danh sách nhưng luôn PAUSED. Nạp rows vào
  // bridge sau configure để default chain đã sẵn, không Start và không ký.
  for (const chain of CHAINS) {
    const engine = getEngine(chain);
    for (const row of db.getNfts(chain)) {
      if (!engine.nfts?.has(row.url)) engine.addRow(row);
    }
  }

  // The injected wallet reads the key fresh on every request, so changing it in
  // Settings takes effect without restarting the browser session.
  wallet.configure({
    privateKeyProvider: () => db.getSettings().privateKey,
    chain: settings.lastChain || "ethereum"
  });

  if (stream) {
    stream.refreshKey();
    stream.setCollections(allCollectionSlugs());
  }

  return settings;
}

/**
 * Windows went to sleep, or the network came back. Do not wait to find out.
 *
 * A suspended machine leaves its TCP connections looking open from this side.
 * The socket is not closed, no error is raised, and nothing arrives - so the
 * stream's silence watchdog is the only thing that notices, and it takes
 * ninety seconds. For a bot whose whole job is answering a rival's bid within
 * a second, ninety seconds of running-but-deaf is the failure this exists to
 * remove.
 *
 * REGISTERED EXACTLY ONCE. `powerHooksInstalled` is not decoration: these are
 * process-lifetime listeners on a shared Electron singleton, and a second
 * registration would mean two reconnects per resume, then three - the listener
 * leak that soak tests exist to catch.
 */
let powerHooksInstalled = false;
let lastSuspendAt = 0;

function installPowerHooks() {
  if (powerHooksInstalled) return;
  powerHooksInstalled = true;

  /**
   * RESUME = KIỂM CHỨNG, KHÔNG PHẢI PHÁ SOCKET (1.25.0)
   *
   *   1.24.x gọi forceReconnect("resume") rồi onStreamReconnect() cho MỌI
   *   engine sau mỗi lần máy thức — kể cả khi socket vẫn sống, kèm một lượt
   *   đọc REST cho mọi hàng. Nay: gửi một heartbeat và chờ phản hồi ngắn.
   *   Socket trả lời → giữ nguyên, không đọc lại gì. Không trả lời / không
   *   còn socket → đóng qua đường thường; socket mới mở ra sẽ báo engines
   *   ĐÚNG MỘT LẦN, sau khi official SDK nhận ACK global subscription.
   */
  const revalidate = why => {
    if (!stream || typeof stream.revalidate !== "function") return;
    stream.revalidate(why).then(result => {
      logger.stream(`[POWER] ${why} · healthy=${result.healthy} · reconnect=${result.reconnected} · ${result.why}`);
    }).catch(error => {
      logger.error(`[POWER] revalidate (${why}) lỗi: ${error.message}`);
    });
  };

  powerMonitor.on("suspend", () => {
    lastSuspendAt = Date.now();
    logger.stream("[POWER] máy tạm dừng — coi kết nối stream là không còn tin được");
  });

  powerMonitor.on("resume", () => {
    const asleepMs = lastSuspendAt ? Date.now() - lastSuspendAt : 0;
    logger.stream(
      `[POWER] máy hoạt động lại sau ${Math.round(asleepMs / 1000)}s` +
      " — kiểm heartbeat trước khi quyết định nối lại"
    );
    revalidate("resume");
  });

  // Unlock alone is not evidence of a network failure. Resume is revalidated
  // above; a healthy socket must not be torn down on screen unlock.

  logger.stream("[POWER] đã đăng ký suspend/resume — một lần cho cả tiến trình");
}

function allCollectionSlugs() {
  const slugs = new Set();
  for (const engine of engines.values()) {
    for (const slug of engine.collectionSlugs()) slugs.add(slug);
  }
  return Array.from(slugs);
}

// ------------------------------------------------------------------
// Stream
// ------------------------------------------------------------------

/**
 * Aggregated stream counters.
 *
 * A single busy collection emits hundreds of events per second (measured: ~230/s
 * across two collections). Logging one line per event would churn the whole
 * 1000-line buffer every few seconds AND fire an IPC message per line, which is
 * exactly what makes Electron lag. So unmatched events are only counted, and a
 * single summary line is emitted on a timer. Events that actually hit a watched
 * NFT are still logged individually by the engine as [STREAM PRIORITY].
 */
const streamCounters = { total: 0, promoted: 0, byEvent: new Map() };
const STREAM_SUMMARY_MS = 30 * 1000;

function startStream() {
  stream = new OpenSeaStream({
    getApiKey: () => rateLimiter.apiKeys.peek(),
    // Nối lại CHỈ khi tập khoá đã cấu hình đổi. Khoá nào được chọn cho socket
    // lần này là việc của bộ xoay vòng, không phải một thay đổi cấu hình —
    // xem OpenSeaStream.refreshKey.
    getKeySignature: () => rateLimiter.apiKeys.fingerprint(),
    // Thêm Key 2 không nối lại Stream đang khoẻ: chỉ khi key ĐANG DÙNG bị gỡ/thay.
    isKeyConfigured: key => Boolean(rateLimiter.apiKeys.find(key)),

    // The official SDK restored the global subscription after a transport gap.
    // Engines note the reconnect; scoped collection repairs follow the global ACK.
    onReconnect: () => {
      for (const engine of engines.values()) {
        try {
          engine.onStreamReconnect();
        } catch (error) {
          logger.error(`reconnect: ${error.message}`);
        }
      }
    },

    // Global subscription is active again; reconcile only this affected
    // collection so REST remains a bounded recovery path.
    onTopicGap: slug => {
      for (const engine of engines.values()) {
        try { if (typeof engine.onTopicGap === "function") engine.onTopicGap(slug); }
        catch (error) { logger.error(`topic gap ${slug}: ${error.message}`); }
      }
    },

    // The global Stream lost authority. Mark each affected collection at the
    // outage edge so stale Best cannot authorize a SEND during recovery.
    onTopicUnavailable: (slug, at, reason) => {
      for (const engine of engines.values()) {
        try { if (typeof engine.onTopicUnavailable === "function") engine.onTopicUnavailable(slug, at, reason); }
        catch (error) { logger.error(`topic unavailable ${slug}: ${error.message}`); }
      }
    },

    // First global ACK: rows read BEFORE it may have missed events between the
    // authority snapshot and Stream readiness; each engine re-reads only those rows.
    onTopicJoined: (slug, joinedAt) => {
      for (const engine of engines.values()) {
        try { if (typeof engine.onTopicJoined === "function") engine.onTopicJoined(slug, joinedAt); }
        catch (error) { logger.error(`topic joined ${slug}: ${error.message}`); }
      }
    },

    onEvent: decoded => {
      // One event can touch both chains; each engine filters by its own rows.
      let promoted = 0;
      for (const engine of engines.values()) {
        try {
          promoted += engine.handleStreamEvent(decoded);
        } catch (error) {
          logger.error(`stream dispatch: ${error.message}`);
        }
      }

      streamCounters.total++;
      if (promoted) streamCounters.promoted += promoted;

      const seen = streamCounters.byEvent.get(decoded.event) || 0;
      streamCounters.byEvent.set(decoded.event, seen + 1);
    }
  });

  // Every engine can now ask how the stream is. That is what lets
  // reconciliation stand down while the stream is delivering, and step back up
  // the moment it is not.
  for (const engine of engines.values()) {
    engine.attachStreamHealth(slug => (stream
      ? (slug ? stream.healthForCollection(slug) : stream.health())
      : "DISCONNECTED"));
  }

  stream.start(allCollectionSlugs());

  installPowerHooks();

  const summary = setInterval(() => {
    if (!streamCounters.total) return;

    const breakdown = Array.from(streamCounters.byEvent)
      .sort((a, b) => b[1] - a[1])
      .map(([event, count]) => `${event}=${count}`)
      .join(" ");

    logger.stream(
      `${streamCounters.total} events / ${STREAM_SUMMARY_MS / 1000}s, ` +
        `${streamCounters.promoted} priority scan(s) | ${breakdown}`
    );

    streamCounters.total = 0;
    streamCounters.promoted = 0;
    streamCounters.byEvent.clear();
  }, STREAM_SUMMARY_MS);
  summary.unref?.();
  timers.push(summary);
}

// ------------------------------------------------------------------
// Window
// ------------------------------------------------------------------

function createWindow() {
  const windowTitle = `OpenSea Offer Bot — Tool #${instance.get().slot}`;
  mainWindow = new BrowserWindow({
    title: windowTitle,
    width: 1500,
    height: 950,
    minWidth: 1180,
    minHeight: 700,
    backgroundColor: "#0f1115",
    show: false,
    icon: path.join(__dirname, "assets", "icon.ico"),
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
      spellcheck: false,
      backgroundThrottling: false
    }
  });

  mainWindow.setMenuBarVisibility(false);
  mainWindow.on("page-title-updated", event => {
    event.preventDefault();
    mainWindow?.setTitle(windowTitle);
  });
  mainWindow.loadFile(path.join(__dirname, "renderer", "index.html"));

  mainWindow.once("ready-to-show", () => {
    mainWindow.show();
    // Offer Item is API/Stream/signer based. Session pages are lazy and are
    // created only by explicit session-dependent operations (Offer SLL,
    // Cancel, or login), never just because the dashboard opened.
  });

  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url).catch(() => {});
    return { action: "deny" };
  });

  // Closing the app window must end the app.
  //
  // browser.js opens a HIDDEN BrowserWindow for the opensea.io session, and
  // "window-all-closed" only fires once EVERY window is gone. That hidden one
  // outlived the main window, so the event never arrived, app.quit() was never
  // reached, and the process stayed alive with no window to show for it. The
  // next launch was then refused by the single-instance lock - which is what
  // "the tool will not open after I have used it once" actually was.
  // ---- the X always asks --------------------------------------------
  //
  // Asked in the MAIN process, not the renderer: the question has to be
  // answerable even when the window is mid-render, and a renderer dialog
  // cannot hold up a close that Windows has already started.
  //
  // EVERY close, not only a busy one. This app is meant to be left running
  // for hours, so the X is pressed by accident far more often than it is
  // pressed deliberately, and "was anything running?" is not a question the
  // person answering the dialog can see the answer to. What IS running is
  // listed underneath, so the confirmation carries the information instead of
  // deciding on its own whether to appear.
  //
  // Closing THIS window ends THIS instance and nothing else: other instances
  // are separate processes with their own slots, engines and streams.
  mainWindow.on("close", event => {
    if (closeConfirmed) return;

    // ---- only a PERSON gets asked -------------------------------
    //
    // showMessageBoxSync blocks the main process until somebody answers,
    // so a close nobody is watching hangs the app forever. Two of those are
    // real: the auto-updater calls app.quit() to restart into the new
    // version, and the reopen suite closes the window from the renderer.
    // Measured: the app never exited and the suite timed out after 480s.
    //
    // `quitting` is set by before-quit, which fires for every programmatic
    // quit and not for a user pressing X. OSB_NO_EXIT_PROMPT is for the
    // automated close tests, which have no way to click a native dialog;
    // production never sets it and public-release checks that.
    if (quitting || shuttingDown || process.env.OSB_NO_EXIT_PROMPT === "1") {
      return;
    }

    const busy = describeWorkInFlight();

    event.preventDefault();
    const choice = dialog.showMessageBoxSync(mainWindow, {
      type: "question",
      buttons: ["Có, thoát", "Không"],
      defaultId: 1,
      cancelId: 1,
      noLink: true,
      title: "Thoát Offer Bot",
      message: "Bạn có chắc muốn thoát Offer Bot không?",
      detail: [
        ...(busy.length
          ? ["Đang chạy:", ...busy, "", "Thoát sẽ dừng những việc trên."]
          : ["Hiện không có thao tác nào đang chạy."]),
        "",
        "Offer đã gửi lên OpenSea VẪN CÒN sau khi thoát.",
        "Cửa sổ Offer Bot khác (nếu có) vẫn chạy bình thường."
      ].join("\n")
    });

    if (choice === 0) {
      closeConfirmed = true;
      mainWindow.close();
    }
  });

  mainWindow.on("closed", () => {
    mainWindow = null;
    shutdownApp("main window closed");
  });
}

/** Set once the user has answered the close confirmation. */
let closeConfirmed = false;

/**
 * What would be interrupted by closing right now, in the user's words.
 *
 * Empty when nothing is running, which is what keeps the confirmation from
 * appearing on an idle app.
 *
 * @returns {string[]}
 */
function describeWorkInFlight() {
  const lines = [];

  for (const [chain, engine] of engines) {
    let running = 0;
    try {
      for (const row of engine.nfts.values()) if (row.running) running++;
    } catch { /* an engine that cannot be read is not a reason to block */ }
    if (running) {
      const label = normalizeChainName(chain) === "robinhood" ? "Robinhood" : "Ethereum";
      lines.push(`· ${label}: ${running} NFT đang chạy`);
    }
  }

  return lines;
}

/** True once a shutdown is under way, so it only runs once. */
let shuttingDown = false;

/**
 * True once something asked the APP to quit, rather than the user closing a
 * window. The auto-updater is the one that matters: it calls app.quit() to
 * restart into the new version, and a confirmation there would stop the
 * update halfway with nobody to answer it.
 */
let quitting = false;
app.on("before-quit", () => { quitting = true; });

// One concern per listener: the flag above decides whether the close prompt
// is shown; this one makes the last accepted edit durable. quitAndInstall and
// Windows shutdown do not await promises, and Database.write uses
// temp + fsync + rename, so it is durable on return.
app.on("before-quit", () => {
  try {
    if (db) db.write(db.snapshot());
  } catch (error) {
    logger.error(`[SETTINGS] pre-quit snapshot failed: ${error.message}`);
  }
});

/**
 * Release everything this process owns, then quit.
 *
 * Bounded on purpose: if some handle refuses to settle, the process still exits
 * rather than lingering invisibly and blocking the next launch.
 */
/**
 * Establish the OpenSea session once, when the app opens.
 *
 * Before this, the session was created by whichever job ran first, so every
 * scan paid seven seconds for it with nothing on screen. Doing it here means a
 * later job finds it ready.
 *
 * Deliberately after the window is up and skipped once a shutdown starts: an
 * OpenSea navigation still in flight while the app tears down crashes the
 * process. Only the session is restored - nothing is signed, and the wallet
 * stays disarmed throughout.
 */
async function warmSession() {
  if (shuttingDown) return;
  const wallet = configuredWallet();
  logger.send("[SESSION] boot");
  logger.send(`[SESSION] wallet = ${wallet ? openseaSession.shortWallet(wallet) : "(chưa có Private Key)"}`);
  if (!wallet) {
    openseaSession.connectFor(null);
    return;
  }
  logger.send("[SESSION] restoring OpenSea session");
  try {
    await openseaSession.connectFor(wallet);
  } catch (error) {
    if (!shuttingDown) logger.error(`[SESSION] không khởi tạo được: ${error.message}`);
  }
}

function shutdownApp(reason) {
  if (shuttingDown) return;
  shuttingDown = true;

  logger.engine(`[APP] shutdown requested (${reason})`);

  try {
    if (db) db.write(db.snapshot());
  } catch (error) {
    logger.error(`[SETTINGS] shutdown snapshot failed: ${error.message}`);
  }

  for (const timer of timers) clearInterval(timer);
  try { if (stream) stream.stop(); } catch { /* already down */ }
  for (const engine of engines.values()) {
    try { engine.shutdown(); } catch { /* already down */ }
  }
  try { browser.shutdown(); } catch { /* already down */ }
  // Freed here so the next instance can take this slot at once rather than
  // waiting for the pid check to notice this process is gone.
  try { instance.release(); } catch { /* the lock is already gone */ }

  logger.engine("[APP] resources released, quitting");
  app.quit();

  // Last resort. Nothing above should hang, but an app that cannot be reopened
  // is a worse failure than an abrupt exit.
  const forced = setTimeout(() => app.exit(0), 4000);
  if (typeof forced.unref === "function") forced.unref();
}

// ------------------------------------------------------------------
// Housekeeping (spec 19)
// ------------------------------------------------------------------

function startHousekeeping() {
  const sweep = setInterval(() => {
    try {
      const removed = cache.sweepAll();
      if (removed > 50) logger.engine(`cache sweep: ${removed} entries`);

      // Ordinary log lines are trimmed back; deduplicated errors are kept.
      logger.autoClean(LOG_KEEP_LINES);
    } catch (error) {
      logger.error(`cache sweep: ${error.message}`);
    }
  }, CACHE_SWEEP_MS);
  sweep.unref?.();
  timers.push(sweep);

  // Percentiles for the realtime pipeline, so a slow reaction can be traced to
  // a stage instead of guessed at. Silent until there is something to report.
  const latencyReport = setInterval(() => {
    try {
      const { paths } = profiler.report();
      if (!paths.completed) return;
      for (const line of profiler.formatReport().split("\n")) logger.engine(line);
    } catch (error) {
      logger.error(`latency report: ${error.message}`);
    }

    // Runtime census, on the same beat. Read-only: it prints counters and
    // registries, and every number in it comes from those rather than from
    // grepping the log - the ring buffer holds 1000 lines and a busy batch
    // produces thousands, so a log-derived count is quietly wrong.
    for (const engine of engines.values()) {
      try {
        if (engine.nfts.size) engine.logRuntimeCensus();
      } catch (error) {
        logger.error(`runtime census: ${error.message}`);
      }
    }
  }, LATENCY_REPORT_MS);
  latencyReport.unref?.();
  timers.push(latencyReport);

  const chromium = setInterval(() => {
    try {
      // Never clears storage - only the HTTP/image cache, so settings and keys
      // in database.json are untouched.
      session.defaultSession.clearCache().catch(() => {});
    } catch {
      /* ignore */
    }
  }, CHROMIUM_CACHE_CLEAR_MS);
  chromium.unref?.();
  timers.push(chromium);

  /**
   * [NET] (1.25.1) — every number in the "60s" group is a DELTA over the last
   * minute; cumulative figures live only in the separate "total" group, so
   * the two can never be misread as each other. Row readiness is a snapshot.
   */
  let netPrev = null;
  const netEnginePrev = new Map();
  const netSummary = setInterval(() => {
    try {
      const reads = (() => { try { return opensea.readDispatcher.stats().granted || 0; } catch { return 0; } })();
      const st = stream ? stream.status() : {};
      const wk = browser.workerStatus();
      const ses = openseaSession.stats();
      const cur = {
        apiRead: reads, metadata: opensea.metaStats.calls, gql: 0,
        streamEvents: st.marketplaceEvents || st.events || 0,
        reconnect: st.reconnect || st.reconnects || 0,
        topicRejoin: st.resubscribeCount || st.topicRejoins || 0,
        streamFrames: st.framesReceived || 0, streamBytes: st.bytesReceived || 0,
        streamControlFrames: st.controlFrames || 0,
        streamMarketplaceFrames: st.marketplaceEvents || st.events || 0,
        streamUnclassifiedFrames: st.untrackedEvents || st.untrackedDropped || 0,
        streamHandled: st.matchedEvents || st.handled || 0,
        streamDropped: st.untrackedEvents || st.untrackedDropped || 0,
        joinRefused: st.joinRefused || 0, webNav: wk.navigations, profileNav: wk.profileNavigations,
        profileReads: ses.profileReads, workerCreated: wk.created, workerIdle: wk.destroyedIdle
      };
      const d = {};
      for (const k of Object.keys(cur)) d[k] = netPrev ? cur[k] - (netPrev[k] || 0) : cur[k];
      netPrev = cur;

      const parts = [];
      let engineActivity = 0;
      for (const [chain, bridge] of engines.entries()) {
        const eng = bridge && bridge.engine;
        if (!eng || typeof eng.netDiagnostics !== "function") continue;
        const n = eng.netDiagnostics();
        const pr = (n.recovery && n.recovery.byPriority) || {};
        // Flatten every cumulative counter we report, then diff against last minute.
        const now = {
          ...Object.fromEntries(Object.entries(n.total || {}).map(([k, v]) => [`t.${k}`, v])),
          fullRead: n.reads.full || 0, quickRead: n.reads.quick || 0,
          p1: pr[1] || 0, p2: pr[2] || 0, p3: pr[3] || 0,
          yielded: (n.recovery && n.recovery.yieldedToRealtime) || 0,
          watchdogOrphan: n.watchdog.orphans, watchdogOverdue: n.watchdog.retryOverdue,
          watchdogWaiting: n.watchdog.waitingRepairs, gaps: n.stream.gaps
        };
        for (const [fp, v] of Object.entries(n.perKey || {})) {
          now[`k.${fp}.write`] = v.write || 0;
          now[`k.${fp}.429`] = v["429"] || 0;
        }
        const prev = netEnginePrev.get(chain) || {};
        const dd = {};
        for (const [k, v] of Object.entries(now)) dd[k] = v - (prev[k] || 0);
        netEnginePrev.set(chain, now);
        const g = k => dd[`t.${k}`] || 0;
        const perKey = Object.keys(n.perKey || {}).map(fp => `${fp}:w${dd[`k.${fp}.write`] || 0}/429x${dd[`k.${fp}.429`] || 0}`).join(" ");
        const retries = Object.keys(dd).filter(k => k.startsWith("t.retry:") && dd[k]).map(k => `${k.slice(8)}=${dd[k]}`).join(",");
        const drops = ["signedDropOwnDuplicate", "signedDropBestMoved", "signedDropSuperseded", "signedDropOverMax", "signedDropStopped", "signedDropOther"]
          .filter(k => g(k)).map(k => `${k.replace("signedDrop", "")}=${g(k)}`).join(",");
        const r = n.rows || {};
        engineActivity += g("write") + g("429") + (n.writeQueue.ready || 0) + (n.writeQueue.inFlight || 0) + dd.fullRead + dd.quickRead;
        parts.push(`${chain}: 60s write=${g("write")} ok=${g("sendSuccess")} 429=${g("429")} fullRead=${dd.fullRead} quickRead=${dd.quickRead} ` +
          `recovery P1/P2/P3=${dd.p1}/${dd.p2}/${dd.p3} yielded=${dd.yielded} ` +
          `renew expired/sent-ok=${g("renewExpired")}/${g("renewSuccess")} noIntent=${g("noIntent")} ` +
          `streamRemove matched/untracked=${g("streamRemove")}/${g("streamRemoveUntracked")} ` +
          `watchdog orphan/overdue/waiting=${dd.watchdogOrphan}/${dd.watchdogOverdue}/${dd.watchdogWaiting} gaps=${dd.gaps}` +
          (drops ? ` signedDrop[${drops}]` : "") + (retries ? ` retry[${retries}]` : "") + (perKey ? ` keys[${perKey}]` : "") +
          ` · now q(ready=${n.writeQueue.ready} acq=${n.writeQueue.acquiring} fly=${n.writeQueue.inFlight} wait=${n.writeQueue.waiting})` +
          ` quotaWait p50/p95/p99=${n.quotaWaitMs.p50}/${n.quotaWaitMs.p95}/${n.quotaWaitMs.p99}ms` +
          ` rows warm=${r.warm || 0}/${r.running || 0} waitBest=${r.waitingBest || 0} waitRenew=${r.waitingRenew || 0}` +
          ` waitTemplate=${r.waitingTemplate || 0} waitOwn=${r.waitingOwn || 0} sending=${r.sending || 0}` +
          `${n.stream.degraded ? " DEGRADED" : ""} · total write=${(n.total || {}).write || 0} 429=${(n.total || {})["429"] || 0}`);
      }
      const planeActivity = d.apiRead + d.metadata + d.gql + d.webNav + d.profileReads + d.reconnect + d.topicRejoin + d.joinRefused + d.workerCreated;
      if (!planeActivity && !engineActivity && st.mode !== "official-global" && st.mode !== "official-collections") return;
      const subs = st.subscriptions ? ` subs joined=${st.subscriptions.JOINED || 0} joining=${st.subscriptions.JOINING || 0} backoff=${st.subscriptions.BACKOFF || 0}` : "";
      const gl = st.mode === "official-global" || st.mode === "official-collections"
        ? ` STREAM connected=${Boolean(st.socketConnected)} active=${Boolean(st.subscriptionsActive)} mode=${st.mode} state=${st.marketState || st.health} marketplaceEvents60s=${st.marketplaceEvents60s || 0} matched60s=${st.matchedEvents60s || 0} untracked60s=${st.untrackedEvents60s || 0} lastEventAgeMs=${st.lastEventAgeMs ?? -1} reconnect=${st.reconnect || 0} handlerErrors=${st.handlerErrors || 0} gap=${st.streamGapStartedAt || 0} rxFrames=${d.streamFrames} controlFrames=${d.streamControlFrames} lastFrame=${st.lastFrameType || "-"}`
        : st.global ? ` feed=${st.global.mode} tracked=${st.global.tracked} rx=${d.streamFrames}(ctrl=${d.streamControlFrames} market=${d.streamMarketplaceFrames} other=${d.streamUnclassifiedFrames}) handled=${d.streamHandled} dropped=${d.streamDropped} bytes=${d.streamBytes} lastFrame=${st.lastFrameType || "-"} joinReply=${st.lastJoinReplyStatus || "-"}:${st.lastJoinReplyTopic || "-"}` : "";
      let quota = "";
      try {
        const rs = opensea.readDispatcher.stats();
        quota = ` · READ quota used=${rs.global.usedRps60s}/${rs.global.ceilingRps}rps (${Math.round(rs.global.utilization * 100)}%) ` +
          `q=${rs.queued}(crit=${rs.queuedCritical} bg=${rs.queuedBackground}) wait all p50/p95/max=${rs.waitMs.p50}/${rs.waitMs.p95}/${rs.waitMs.max}ms realtime p50/p95/max=${rs.realtimeWaitMs.p50}/${rs.realtimeWaitMs.p95}/${rs.realtimeWaitMs.max}ms ` +
          `429 60s/total=${rs.global.rateLimited60s}/${rs.global.rateLimitedTotal} ` +
          `independent=${rs.global.independentKeys} keys[${rs.keys.map(k => `cap${k.capRps}:${k.usedRps60s}/${k.capRps}rps util=${Math.round(k.utilization * 100)}% aimd=${k.rps.toFixed(2)} 429x${k.rateLimited}`).join(" ")}]`;
        // WRITE: one broker serves all chains; print it once.
        for (const [, bridge] of engines) {
          const eng = bridge && bridge.engine ? bridge.engine : bridge;
          const q = eng && eng.quota;
          if (!q || typeof q.grantRates !== "function") continue;
          const gr = q.grantRates();
          const st = q.status();
          const doms = Object.values(gr).sort((a, b) => b.ceilingRps - a.ceilingRps);
          const cap = doms.reduce((n, d) => n + d.ceilingRps, 0);
          const total = doms.reduce((n, d) => n + d.grants60s, 0);
          const nd = typeof eng.netDiagnostics === "function" ? eng.netDiagnostics() : null;
          const w = nd && nd.quotaWaitMs ? nd.quotaWaitMs : null;
          quota += ` · WRITE grants=${(total / 60).toFixed(2)}/${cap.toFixed(1)}rps (${cap ? Math.round(total / 60 / cap * 100) : 0}%) ` +
            `keys[${doms.map(d => `cap${d.ceilingRps}:${d.grantsPerSec}/${d.ceilingRps}rps util=${Math.round(d.utilization * 100)}% aimd=${d.rate} 429x${d.rateLimits} cd=${d.blockedForMs}ms`).join(" ")}] ` +
            `queue now/peak=${st.queued}/${st.queuedPeak || 0} oldest=${st.oldestQueuedAt ? Date.now() - st.oldestQueuedAt : 0}ms` +
            (w ? ` Decision->grant p50/p95/max=${w.p50}/${w.p95}/${w.max}ms` : "");
          break;
        }
        for (const [chain, bridge] of engines) {
          const eng = bridge && bridge.engine ? bridge.engine : bridge;
          const s = (eng && eng.stats) || {};
          if (eng && typeof eng.bootstrapStats === "function") {
            const b = eng.bootstrapStats();
            if (b.terminals) quota += ` · ${chain} bootstrap firstSend=${b.firstSends}/${b.terminals} ADD->FIRST SEND p50/p95/max=${b.totalMs.p50}/${b.totalMs.p95}/${b.totalMs.max}ms (template p50=${b.templateMs} firstRead p50=${b.firstReadMs} queued p50=${b.queuedMs})`;
          }
          if (eng && eng.degradedPollTimer) quota += ` · ${chain} poll=${s.degradedPolls || 0} hits=${s.degradedPollHits || 0} hot=${eng.hotUntil ? eng.hotUntil.size : 0} coldYield=${s.degradedPollColdYield || 0} err=${s.degradedPollErrors || 0}`;
        }
      } catch { quota = ""; }
      logger.engine(`[NET] 60s${quota} · streamEvents=${d.streamEvents} reconnect=${d.reconnect} rejoin=${d.topicRejoin} joinRefused=${d.joinRefused} · ` +
        `apiRead=${d.apiRead} metadata=${d.metadata} gql=${d.gql} · webNav=${d.webNav} profileNav=${d.profileNav} profileReads=${d.profileReads} ` +
        `worker +${d.workerCreated}/-${d.workerIdle} (${wk.alive ? "alive" : "off"})${subs}${gl} | ` + parts.join(" | "));
    } catch (error) {
      logger.error(`[NET] summary: ${error.message}`);
    }
  }, 60 * 1000);
  netSummary.unref?.();
  timers.push(netSummary);
}

// ------------------------------------------------------------------
// IPC: state
// ------------------------------------------------------------------

/**
 * Every action that can spend money or touch the chain goes through here.
 * A lapsed licence stops the tool without needing a restart, because the guard
 * re-verifies against the clock on each call.
 */
/**
 * Tell the window what the licence allows, whenever that changes.
 *
 * Pushed rather than polled, because the moment that matters is a revoke
 * landing on a running app: the next background check returns REVOKED and the
 * window has to lock there and then, not when the user next opens Settings.
 *
 * Only on a CHANGE, so a check every fifteen seconds is not fifteen seconds of
 * IPC.
 */
/**
 * The updater.
 *
 * Built at boot so the window can ask about updates the moment it opens,
 * including while the app is locked - a user whose licence has expired may
 * well need the newer build to fix whatever expired it.
 */
let updater = null;

function publishUpdate(state) {
  send("update:state", state);
}

let lastLicenseSignature = "";

/**
 * OPENSEA API STATE.
 *
 * Three states, and the middle one is the point of the design:
 *
 *   ACTIVE     a request has succeeded, or nothing says otherwise
 *   LOCKED     OpenSea refused the CREDENTIAL - the app is unusable until
 *              a working key is entered
 *   DEGRADED   OpenSea could not be reached. Says nothing about the key,
 *              so it does NOT lock; it only explains the quiet
 *
 * Locking a working installation because OpenSea had a bad minute is a worse
 * failure than not locking it at all: the user cannot tell the difference
 * from a revoked key, and the only remedy the screen offers - type a new key
 * - cannot fix it. So a lock needs evidence about WHO IS ASKING, which is
 * only ever a 401 or a 403. A 429, a 5xx, a timeout and a dead socket all
 * leave the app running.
 */
const apiState = {
  state: "ACTIVE",
  code: "",
  checkedAt: 0
};
let lastApiSignature = "";

function apiStatus() {
  const entry = apiState.code ? errors.get(apiState.code) : null;
  return {
    state: apiState.state,
    // CLAUDE.md §18: access is LICENCE-only. A refused/missing OpenSea key is
    // reported (state/code/reason) and fails only the operations that need
    // it; it never locks navigation, Settings or Logs. With two keys, one bad
    // key used to disable every tab but Settings.
    locked: false,
    code: apiState.code,
    reason: entry ? entry.userMessage : "",
    checkedAt: apiState.checkedAt
  };
}

function publishApi(force = false) {
  const status = apiStatus();
  const signature = `${status.state}|${status.code}`;
  if (!force && signature === lastApiSignature) return status;
  lastApiSignature = signature;
  send("api:state", status);
  return status;
}

/**
 * One settled OpenSea request, seen from the credential's point of view.
 *
 * Called from opensea.js for every request that either succeeded or failed
 * without being retried away.
 */
function noteApiResult({ ok, error }) {
  apiState.checkedAt = Date.now();

  if (ok) {
    // A 2xx is proof, and it clears a lock without a restart.
    if (apiState.state !== "ACTIVE") {
      logger.api(`API OpenSea hoạt động trở lại`);
    }
    apiState.state = "ACTIVE";
    apiState.code = "";
    publishApi();
    return;
  }

  const code = errors.classifyApiError(error);
  if (!code) return;                       // says nothing about the credential

  if (!errors.isCredentialFailure(code)) {
    // Unreachable, not refused. Recorded so the UI can explain the quiet,
    // and deliberately not a lock.
    if (apiState.state === "LOCKED") return;
    apiState.state = "DEGRADED";
    apiState.code = code;
    publishApi();
    return;
  }

  if (apiState.state === "LOCKED" && apiState.code === code) return;
  apiState.state = "LOCKED";
  apiState.code = code;
  logger.error(errors.line(code));
  publishApi(true);
}

/**
 * Take the licence another window has activated, if this one has none.
 *
 * Deliberately one-directional and only-when-empty. A window that already has
 * a key keeps it: swapping a running window onto a different licence because
 * a file changed underneath it is not sharing, it is a surprise.
 *
 * @returns {boolean} whether a key was adopted
 */
function adoptSharedLicence() {
  if (!licenceVault || !licenceVault.isAvailable()) return false;
  if (String(db.getSettings().licenseKey || "").trim()) return false;

  const shared = String(licenceVault.load().values.licenseKey || "").trim();
  if (!shared) return false;

  db.updateSettings({ licenseKey: shared });
  applySettings();
  logger.license("nhận licence từ cửa sổ khác — không cần nhập lại key");
  publishLicense(true);
  return true;
}

// ---- kho credential dùng chung (shared-credentials-bridge.js) -------------
//
// Ánh xạ THEO VỊ TRÍ key: apiKeys <-> apiKey1, apiKey2 <-> apiKey2,
// privateKey <-> privateKey, licenseKey <-> licenseKey. Chỉ slot 1 (primary).

/** Một giá trị MỚI HƠN từ kho chung được nhận vào kho riêng + db (đồng bộ). */
function applySharedField(field, value) {
  const v = String(value || "").trim();
  if (field === "privateKey" && v && !walletAddressFromPrivateKey(v)) return false;
  const result = db.updateSettings({ [field]: v });
  if (result && result.ok === false) return false;
  if (secrets && secrets.isAvailable()) secrets.save({ [field]: v });
  if (field === "licenseKey" && licenceVault && licenceVault.isAvailable()) licenceVault.save({ licenseKey: v });
  if (field === "privateKey" && walletProfiles) {
    if (v) walletProfiles.upsertActive(v); else walletProfiles.clearActive();
  }
  return true;
}

/** Đẩy giá trị người dùng vừa lưu lên kho chung. Không bao giờ ném lỗi. */
function sharedPush(values) {
  if (!sharedBridge || sharedAdopting) return;
  try { sharedBridge.push(values); } catch { /* kho chung không được làm hỏng việc lưu */ }
}

/** Khởi tạo + bootstrap (chỉ primary), trước khi engine/applySettings chạy. */
function initSharedCredentials() {
  const self = instance.get();
  const primary = self.slot === 1 && !self.overflow;
  if (!primary) return;

  let dir;
  const override = String(process.env.OFFERBOT_SHARED_CRED_DIR || "").trim();
  if (override && !app.isPackaged) dir = override;
  else if (devRuntime.isDev()) {
    logger.engine("[SHARED] bản dev: không dùng kho credential dùng chung của production");
    return;
  }

  // Key positions must be settled BEFORE they are compared with the shared store
  // (a legacy "k1,k2" in apiKeys must not be published as Key 1). Idempotent.
  try { normalizeApiKeySlots(); } catch { /* the regular boot call below still runs */ }

  sharedBridge = new SharedBridge({
    primary: true,
    shared: () => new SharedCredentials({
      dir,
      tool: "offerbot",
      lockWaitMs: 1500,
      log: line => logger.engine(line)
    }),
    metaFile: path.join(self.primarySettingsBase, "shared-sync-meta.json"),
    local: {
      get: field => String(db.getSettings()[field] || ""),
      apply: (field, value) => {
        sharedAdopting = true;
        try { return applySharedField(field, value); } finally { sharedAdopting = false; }
      }
    },
    // Boot: nothing runs yet, everything may be adopted. Later: never swap a key
    // under a running app, and a licence is only taken when this window has none.
    canApply: (field, ctx) => {
      if (ctx && ctx.boot) return true;
      if (field === "licenseKey") return !String(db.getSettings().licenseKey || "").trim();
      return describeWorkInFlight().length === 0;
    },
    log: line => logger.engine(line)
  });
  try { sharedBridge.bootstrap(); } catch { /* never breaks boot */ }
}

/** The 60 s beat: one stat when nothing changed; adopted values reach the running app here. */
function sharedPoll() {
  if (!sharedBridge) return;
  const walletBefore = configuredWallet();
  let adopted = [];
  try { adopted = sharedBridge.poll() || []; } catch { return; }
  if (!adopted.length) return;

  sharedAdopting = true;
  try {
    if (adopted.includes("apiKeys") || adopted.includes("apiKey2")) normalizeApiKeySlots();
  } finally { sharedAdopting = false; }
  const applied = applySettings();
  if (adopted.includes("licenseKey")) publishLicense(true);
  if (adopted.includes("privateKey")) {
    const walletAfter = configuredWallet();
    if (walletBefore !== walletAfter) {
      Promise.all([...engines.values()].map(engine => engine.rebindWallet(applied)))
        .then(() => openseaSession.walletChanged(walletAfter))
        .catch(error => logger.error(`[SESSION] reconnect lỗi: ${error.message}`));
    }
  }
}

function publishLicense(force = false) {
  const status = license.status();
  const signature = `${status.locked}|${status.serverStatus}|${status.lockReason}|${status.name}`;
  if (!force && signature === lastLicenseSignature) return status;
  lastLicenseSignature = signature;

  send("license:state", status);

  // Locking the UI is not enough on its own: the window is only where the
  // buttons are, and an engine that was started while the licence was valid
  // keeps its own schedule. Suspending a licence has to stop the machine,
  // not just grey out its controls.
  if (status.locked) {
    for (const engine of engines.values()) {
      try { engine.haltForLicense(status.lockReason); }
      catch (error) { logger.license(`không dừng được engine: ${error.message}`); }
    }
  }

  logger.license(
    status.locked
      ? `KHOÁ UI · ${status.serverStatus || "INVALID"} · ${status.lockReason}`
      : `MỞ KHOÁ · ${status.serverStatus || "ACTIVE"} · ${status.name || "license"} · còn ${status.daysLeft} ngày`
  );
  return status;
}

/**
 * Refuse work that needs OpenSea while the credential is refused.
 *
 * Separate from requireLicense: a locked API is a different problem with a
 * different fix, and collapsing the two would send a user to the wrong field.
 */
function requireApi() {
  if (apiState.state !== "LOCKED") return { ok: true };
  const entry = errors.get(apiState.code);
  return {
    ok: false,
    code: apiState.code,
    error: entry ? entry.userMessage : "API OpenSea không dùng được."
  };
}

/**
 * Diagnostics numbers, asked of the engine rather than taken from it.
 *
 * WHY THERE IS NOTHING ENGINE-SPECIFIC LEFT IN HERE
 *
 *   This helper used to carry a fallback that read `orderBook`, `inFlight`,
 *   `submittingNfts`, `globalSubmitting` and `streamTriggered` off the older
 *   engine - which meant main.js still knew the shape of an engine's insides,
 *   and a future change to either engine would break this file again. Both
 *   engines now implement `diagnostics()`, so this is a guard and nothing
 *   more.
 *
 *   The guard is the point. Diagnostics is the panel a person opens when
 *   something has already gone wrong; an engine mid-reset, mid-reconnect or
 *   half-constructed must still produce a readable answer rather than take
 *   the window down with it.
 */
function engineDiagnostics(engine) {
  const chain = (engine && engine.chain) || null;
  try {
    if (engine && typeof engine.diagnostics === "function") {
      const out = engine.diagnostics();
      if (out && typeof out === "object") {
        return { book: out.book || {}, engine: out.engine || { chain } };
      }
    }
    return { book: {}, engine: { chain, unavailable: "engine has no diagnostics()" } };
  } catch (error) {
    return {
      book: {},
      engine: { chain, error: String(error && error.message).slice(0, 160) }
    };
  }
}

/**
 * The licence gate in front of every action that needs one.
 *
 * It AWAITS a current answer. The owner pressing Revoke and this machine
 * noticing are two different moments, and an action started in between must
 * not be allowed through on the strength of a cache that is already wrong.
 */
async function requireLicense() {
  const check = await license.requireValidFresh();
  if (!check.ok) logger.license(`chặn thao tác: ${check.error}`);
  return check;
}

function fullState() {
  const chainsState = {};
  for (const chain of CHAINS) chainsState[chain] = getEngine(chain).getState();

  return {
    settings: publicSettings(db.getSettings()),
    secrets: secretStatus(),
    chains: chainsState,
    wallet: getEngine("ethereum").walletAddress,
    license: license.status(),
    api: apiStatus(),
    walletSessions: [],
    stream: stream ? stream.status() : { connected: false },
    limiter: rateLimiter.stats()
  };
}

function registerIpc() {
  ipcMain.handle("bot:getState", () => fullState());
  ipcMain.handle("remote:pairing", () => remoteHost ? remoteHost.pairing() : { enabled: false });

  // One source of truth for the version: whatever electron-builder packaged.
  // Deleting a credential must remove the ciphertext, not just the copy in
  // memory: a blob left behind is a credential the user believes is gone.
  ipcMain.handle("secrets:clear", () => {
    if (!secrets) return { ok: false, error: "Chưa khởi tạo secure storage." };
    const cleared = secrets.clear();
    if (!cleared.ok) return cleared;
    // The shared copy too. Left behind, the licence beat would hand it
    // straight back and the user would watch a delete that did not delete.
    if (licenceVault && licenceVault.isAvailable()) licenceVault.save({ licenseKey: "" });
    db.updateSettings({ apiKeys: "", apiKey: "", apiKey2: "", privateKey: "", licenseKey: "" });
    applySettings();
    logger.engine("Đã xoá credential đã lưu.");
    return { ok: true, secrets: secretStatus() };
  });

  ipcMain.handle("secrets:status", () => ({ ok: true, ...secretStatus() }));

  ipcMain.handle("app:version", () => ({ ok: true, version: app.getVersion(),
    diagnostic: Boolean(require("./package.json").osb?.diagnosticBuild) }));

  ipcMain.handle("bot:getLogs", () => logger.history());

  /**
   * A click handler in the window threw.
   *
   * Recorded so a fault that used to be completely invisible - an unhandled
   * rejection with no message anywhere - leaves a trace with a code on it.
   * The detail is redacted like any other technical string.
   */
  // ---- updating the app -------------------------------------------
  //
  // Deliberately NOT gated on the licence or the API lock. A user who is
  // locked out is often exactly the user who needs the newer build, and an
  // update button that refuses to work when something is wrong is an update
  // button that is missing when it matters.
  ipcMain.handle("update:status", () => (
    updater ? updater.status() : { phase: "IDLE", currentVersion: app.getVersion() }
  ));

  ipcMain.handle("update:check", async () => {
    if (!updater) return { phase: "IDLE", currentVersion: app.getVersion() };
    return updater.check();
  });

  ipcMain.handle("update:download", async () => {
    if (!updater) return { phase: "IDLE", currentVersion: app.getVersion() };
    return updater.download();
  });

  ipcMain.handle("update:install", () => (
    updater ? updater.install() : { ok: false, code: "E_UPDATE_NOT_READY" }
  ));

  ipcMain.handle("ui:error", (_event, payload) => {
    const label = String((payload && payload.label) || "thao tác");
    const detail = errors.redact(String((payload && payload.detail) || ""));
    logger.error(errors.line("E_UI_ACTION", `${label} ·`));
    logger.send(`[UI] ${label} · ${detail}`);
    return { ok: true };
  });

  ipcMain.handle("bot:clearLogs", () => {
    logger.clear();
    return { ok: true };
  });

  ipcMain.handle("bot:license", async (_event, payload) => {
    if (payload && typeof payload.key === "string") {
      const stored = db.updateSettings({ licenseKey: payload.key });
      if (!stored.ok) return stored;
      applySettings();

      // Pressing the button ASKS THE SERVER, and waits for the answer.
      //
      // Answering from cache is what made a reset licence look broken: the
      // machine kept repeating the refusal it had been given a minute earlier,
      // and pressing Kích hoạt changed nothing because nothing was asked.
      await license.activateNow();
    }
    // Activation must unlock the window immediately, not at the next tick.
    const status = publishLicense(true);

    // A licence that verifies is KEPT.
    //
    // Without this it lived only in memory. updateSettings blanks every
    // credential before settings.json is written - deliberately - and the
    // encrypted store is written by bot:saveSettings, which is NOT the channel
    // the "Kích hoạt" button uses. So the key was accepted, the window
    // unlocked, and the next launch asked the customer to activate all over
    // again: after a restart, and after every update.
    //
    // The test is the SIGNATURE, not the server's verdict.
    //
    // status.valid is still false at this instant whenever a licence server is
    // configured: activation is a round trip, and this handler answers before
    // it lands. Gating on it meant the key was never written on the one path
    // that matters - the customer pressing the button - while the in-process
    // test, which stubs the guard, saw it work.
    //
    // verifyLicense is local and synchronous: a forged, corrupted, retired or
    // expired key is refused here and never reaches the disk, and a valid one
    // already stored is not erased by someone typing nonsense into the box. A
    // key the SERVER later refuses is still stored, which is correct - the app
    // stays locked on every check, and the customer keeps the key they paid
    // for while the owner sorts it out.
    const signature = payload && typeof payload.key === "string"
      ? verifyLicense(payload.key)
      : { valid: false };

    if (secrets && signature.valid) {
      // The machine store first: it is the one every other window reads.
      if (licenceVault && licenceVault.isAvailable()) {
        licenceVault.save({ licenseKey: payload.key });
      }
      const saved = secrets.save({ licenseKey: payload.key });
      sharedPush({ licenseKey: payload.key });
      if (!saved.ok) {
        logger.error(
          "[SECRETS] không lưu được License Key — lần mở sau sẽ phải kích hoạt lại."
        );
      }
    }

    return { ok: true, license: status };
  });

  /**
   * Activate an API key by USING it.
   *
   * Typing a key is not activation. The key is put to a real request before
   * anything is stored, so a wrong one is refused at the field instead of
   * being saved and failing later somewhere the user cannot connect it to.
   *
   * The three outcomes are kept distinct on purpose:
   *   valid       stored, unlocked, immediately
   *   refused     not stored, and the field says which kind of refusal
   *   unreachable not stored, and explicitly NOT called a bad key
   */
  ipcMain.handle("bot:activateApi", async (_event, payload) => {
    const key = String((payload && payload.apiKey) || "").trim();
    // Ô 1 = Key 1 (primary, bắt buộc), ô 2 = Key 2 (secondary, được để trống).
    const slot = Number(payload && payload.slot) === 2 ? 2 : 1;
    const field = slot === 2 ? "apiKey2" : "apiKeys";
    const current = db.getSettings();
    const other = String(slot === 2 ? current.apiKeys : current.apiKey2 || "").trim();

    if (!key) {
      if (slot === 1) return { ok: false, code: "E_API_INVALID", error: "Chưa nhập API Key 1." };
      // Xoá Key 2: hợp lệ. Pool quay về một key, mọi việc chạy như cũ.
      const cleared = secrets.save({ apiKey2: "" });
      if (cleared && cleared.ok === false) return cleared;
      db.updateSettings({ apiKey2: "" });
      if (credentialDefaults && credentialDefaults.isAvailable()) credentialDefaults.save({ apiKey2: "" });
      sharedPush({ apiKey2: "" });
      applySettings();
      logger.api("Key 2 đã gỡ · chạy một key");
      return { ok: true, slot, cleared: true, keys: keyStatsSnapshot() };
    }
    if (other && other === key) {
      return { ok: false, code: "E_API_DUPLICATE",
        error: `Key này đã ở ô Key ${slot === 2 ? 1 : 2}. Hai ô phải là hai key khác nhau.` };
    }

    try {
      // A cheap read with THIS key explicitly, bypassing the pool: the point
      // is to test the key in hand, not whichever key is next in rotation.
      await opensea.probeApiKey(key);
    } catch (error) {
      const code = errors.classifyApiError(error) || "E_API_UNAUTHORIZED";
      const entry = errors.get(code);
      logger.error(errors.line(code, errors.redact(opensea.describeError(error))));
      return { ok: false, code, slot, error: entry ? entry.userMessage : "" };
    }

    // Only now is it stored, through the same encrypted path as every other
    // credential - never settings.json.
    const stored = secrets.save({ [field]: key });
    if (stored && stored.ok === false) return stored;

    // BOTH stores, in this order.
    //
    // applySettings() configures the key manager from db, so writing only to
    // the SecretStore left the manager holding the previous value and every
    // consumer - collection detect included - correctly reporting the key as
    // missing. The boot path has always done these two writes together; this
    // handler did only one of them, which is the whole of the reported bug.
    // Saving settings was never affected: that path writes db first.
    db.updateSettings({ [field]: key });
    if (credentialDefaults && credentialDefaults.isAvailable()) {
      credentialDefaults.save({ [field]: key });
    }
    sharedPush({ [field]: key });
    applySettings();
    logger.api(`Key ${slot} kích hoạt · vân tay ${rateLimiter.keyFingerprint(key)} · pool ${rateLimiter.apiKeys.size()} key`);

    // Prove it rather than assume it: the whole failure was a write that
    // looked like it had worked.
    if (!credentialPresent()) {
      logger.error(errors.line(
        "E_API_STATE_DESYNC",
        "sau khi kích hoạt (nguồn: SecretStore → db → key manager)"
      ));
      return {
        ok: false,
        code: "E_API_STATE_DESYNC",
        error: errors.get("E_API_STATE_DESYNC").userMessage
      };
    }

    apiState.state = "ACTIVE";
    apiState.code = "";
    apiState.checkedAt = Date.now();
    publishApi(true);
    if (slot === 2) return { ok: true, slot, keys: keyStatsSnapshot() };
    logger.api("API OpenSea đã được kích hoạt");

    return { ok: true, slot, api: apiStatus(), keys: keyStatsSnapshot() };
  });

  ipcMain.handle("bot:saveSettings", async (_event, incoming) => {
    const walletBefore = configuredWallet();
    /**
     * AN INVALID KEY IS NEVER WRITTEN (1.25.11)
     *
     *   The key used to be saved first and checked after: a typo replaced the
     *   stored, working key on disk (the live session was kept, but the next
     *   start had no wallet). Now a key that derives no address is dropped from
     *   the payload before any write; the active wallet, its signer, Offer Item
     *   and the OpenSea session stay exactly as they were.
     */
    let settings = incoming;
    let keyError = "";
    if (incoming && "privateKey" in incoming && String(incoming.privateKey || "").trim() &&
        !walletAddressFromPrivateKey(String(incoming.privateKey).trim())) {
      keyError = "Private Key không hợp lệ (cần 64 ký tự hex). Ví đang dùng giữ nguyên.";
      settings = { ...incoming };
      delete settings.privateKey;
      logger.error("[SESSION] invalid private key — expected 64 hex characters · không lưu, ví hiện tại giữ nguyên");
      if (!Object.keys(settings).length) {
        return { ok: false, code: "INVALID_PRIVATE_KEY", error: keyError, keyError, wallet: walletBefore || null, secrets: secretStatus() };
      }
    }
    const result = db.updateSettings(settings || {});
    if (!result.ok) return result;

    // Credentials go to the encrypted store, never to settings.json. Only the
    // fields actually present in this payload are touched, so a form that omits
    // a secret the user did not retype leaves it alone instead of erasing it.
    let secretResult = { ok: true, available: true };
    if (secrets) {
      const changed = {};
      for (const field of ["apiKeys", "apiKey", "apiKey2", "privateKey", "licenseKey"]) {
        if (settings && field in settings) changed[field] = settings[field];
      }
      if (Object.keys(changed).length) secretResult = secrets.save(changed);

      /**
       * Nhập lại một credential hỏng thì nó thôi hỏng.
       *
       * Không có bước này, cảnh báo ở Settings đứng nguyên cho tới lần khởi
       * động sau: người dùng làm đúng việc được bảo, mà màn hình vẫn nói là
       * chưa làm.
       */
      if (secretResult.ok && undecryptableSecrets.length) {
        undecryptableSecrets = undecryptableSecrets.filter(
          field => !(field in changed) || !String(changed[field] || "").trim()
        );
      }
      // Settings is also where a licence key is typed in or cleared, and both
      // have to reach the store every window reads.
      if ("licenseKey" in changed && licenceVault && licenceVault.isAvailable()) {
        licenceVault.save({ licenseKey: changed.licenseKey });
      }
    }

    if (settings && ("apiKeys" in settings || "apiKey" in settings || "apiKey2" in settings)) {
      normalizeApiKeySlots();
    }
    // Shared credential store (primary slot only): AFTER the key slots settled,
    // so the final Key 1 / Key 2 positions are what is published.
    if (settings && secretResult.ok) {
      const touched = {};
      const finalSettings = db.getSettings();
      if ("apiKeys" in settings || "apiKey" in settings) touched.apiKeys = finalSettings.apiKeys || "";
      if ("apiKey2" in settings) touched.apiKey2 = finalSettings.apiKey2 || "";
      if ("privateKey" in settings) touched.privateKey = settings.privateKey;
      if ("licenseKey" in settings) touched.licenseKey = settings.licenseKey;
      if (Object.keys(touched).length) sharedPush(touched);
    }
    const applied = applySettings();
    logger.engine("Settings saved.");

    if (!secretResult.ok) {
      // The normal settings did save; say plainly that the credentials did not.
      return {
        ok: true,
        settings: publicSettings(applied),
        secrets: secretStatus(),
        wallet: getEngine("ethereum").walletAddress,
        warning: secretResult.available
          ? `Không lưu được credential: ${secretResult.reason}`
          : "Windows secure storage không dùng được — credential chỉ tồn tại trong phiên này."
      };
    }

    // The address decides, not the key: re-saving the same key, or saving an
    // API key beside it, must not tear down a working session. A different
    // address means the live session belongs to an account this app is no
    // longer configured for, so it is cleared before the new one is built.
    const walletAfter = configuredWallet();
    const keyWasTouched = !!(settings && "privateKey" in settings);
    const keyGiven = keyWasTouched && String(settings.privateKey || "").trim().length > 0;

    if (keyGiven && !walletAfter) {
      // A key that derives no address is a typo, not an instruction to sign
      // out. Tearing down a working session over it would cost the user their
      // connection for nothing.
      logger.error("[SESSION] invalid private key — expected 64 hex characters");
      logger.send("[SESSION] giữ nguyên phiên hiện tại");
    } else if (walletBefore !== walletAfter) {
      // Cancel every old-wallet flight before the signer/provider starts using
      // the new key.  A build captured before this point must never be posted
      // by the replacement wallet.
      await Promise.all([...engines.values()].map(engine => engine.rebindWallet(applied)));
      await openseaSession.walletChanged(walletAfter).catch(error =>
        logger.error(`[SESSION] reconnect lỗi: ${error.message}`));
    } else if (keyWasTouched) {
      logger.send("[SESSION] private key saved · wallet unchanged");
    }

    // This Tool's profile list follows its active key (1.25.11).
    if (walletProfiles && keyWasTouched) {
      if (keyGiven && walletAfter) walletProfiles.upsertActive(String(settings.privateKey).trim());
      else if (!keyGiven) walletProfiles.clearActive();
    }

    // This is a template for a FUTURE Tool, not a shared live credential
    // store. Updating Tool #1 cannot change Tool #2's active wallet/key.
    if (credentialDefaults && secretResult.ok) {
      const changed = {};
      for (const field of ["apiKeys", "apiKey", "apiKey2", "privateKey"]) {
        if (settings && field in settings) changed[field] = settings[field];
      }
      if (Object.keys(changed).length) credentialDefaults.save(changed);
    }

    for (const engine of engines.values()) engine.emitNow();

    return {
      ok: true,
      settings: publicSettings(applied),
      secrets: secretStatus(),
      // The address derived from the saved key, at once - not the engine's or
      // the OpenSea session's view, which may still be catching up.
      wallet: configuredWallet() || null,
      keyError: keyError || undefined
    };
  });

  /**
   * This Tool's wallets (1.25.11). Addresses and ids only - never a key.
   * Windows-only: deliberately not in REMOTE_IPC_ALLOW.
   */
  ipcMain.handle("wallet:state", () => ({
    ok: true,
    tool: instance.get().slot,
    configured: configuredWallet() || null,
    profiles: walletProfiles ? walletProfiles.list() : []
  }));

  /** Switch THIS Tool to a stored wallet - the same path as typing its key. */
  ipcMain.handle("wallet:activate", async (_event, payload) => {
    if (!walletProfiles) return { ok: false, error: "Chưa sẵn sàng." };
    const key = walletProfiles.keyOf(String(payload && payload.id || ""));
    if (!key) return { ok: false, error: "Không đọc được ví đã lưu." };
    const save = localIpcHandlers.get("bot:saveSettings");
    const out = await save(null, { privateKey: key });
    return { ...out, profiles: walletProfiles.list() };
  });

  ipcMain.handle("wallet:removeProfile", (_event, payload) => {
    if (!walletProfiles) return { ok: false, error: "Chưa sẵn sàng." };
    const out = walletProfiles.remove(String(payload && payload.id || ""));
    return { ...out, profiles: walletProfiles.list() };
  });

  /**
   * Persist the Add-NFT panel config. Fired on every edit in that panel, so
   * the values come back on the next launch with no Save button involved.
   */
  ipcMain.handle("bot:saveChainConfig", (_event, payload) => {
    const chain = normalizeChainName(payload?.chain);
    const config = payload?.config || {};

    const stored = db.saveChainConfig(chain, config);
    if (!stored.ok) return stored;

    /**
     * DURATION CỦA PANEL CHỈ LÀ MẶC ĐỊNH CHO NFT THÊM MỚI
     *
     *   Mỗi hàng có Duration riêng (sửa ngay trên bảng) và order KẾ TIẾP của
     *   hàng đó dùng số của hàng. Đổi Duration của panel KHÔNG ghi đè hàng cũ
     *   và không huỷ order đang sống; `applied` luôn là 0.
     */
    let applied = 0;
    const durationRaw = config.duration;
    const duration = Math.round(Number(durationRaw));
    if (durationRaw !== undefined && durationRaw !== null && String(durationRaw) !== "" &&
        Number.isFinite(duration) && duration >= 1) {
      try {
        const engine = getEngine(chain);
        // This is a default for future rows. Existing NFT row durations are
        // intentionally preserved; per-row edits remain authoritative.
        applied = 0;
        if (typeof engine.emitNow === "function") engine.emitNow();
        logger.engine(`[${chain}] Duration mặc định = ${duration} phút → chỉ áp cho NFT mới`);
      } catch (error) {
        logger.error(`[${chain}] không áp được Duration cho hàng đang chạy: ${error.message}`);
      }
    }

    // The Scan Delay bulk-apply used to live here. There is no Scan Delay
    // to apply: every row shares one fixed reaction gap, so saving the
    // panel config no longer has to re-time anything.
    return { ok: true, config: stored.config, applied };
  });

  ipcMain.handle("bot:setSaveEnabled", (_event, payload) => {
    const chain = normalizeChainName(payload?.chain);
    const result = db.setSaveEnabled(chain, payload?.enabled === true);
    logger.engine(`[${chain}] SAVE ${result.saveEnabled ? "ON" : "OFF"}`);
    return result;
  });

  // ---- rows ------------------------------------------------------

  ipcMain.handle("bot:addNfts", (_event, payload) =>
    addBatchGate.run(payload, () => {
      const reserved = [];
      return executeAddNfts(payload, reserved).finally(() => {
        for (const id of reserved) addReservations.delete(id);
      });
    }));

  async function executeAddNfts(payload, reserved = []) {
    const chain = normalizeChainName(payload?.chain);
    const engine = getEngine(chain);
    const config = payload?.config || {};

    const links = linkList.splitLinks(payload?.links);

    if (!links.length) return { ok: false, error: "Chua nhap link NFT." };

    const added = [];
    const errors = [];
    const startedAt = Date.now();

    // ---- parse first, so bad links cost nothing ---------------------
    const parsedLinks = [];
    for (const link of links) {
      const parsed = opensea.parseOpenSeaUrl(link, chain);
      if (!parsed) {
        errors.push(`Link khong hop le: ${link}`);
        continue;
      }
      if (parsed.chain !== chain) {
        errors.push(
          `${link}: link thuoc chain ${parsed.chain}, dashboard dang la ${chain}.`
        );
        continue;
      }
      // One spelling per token: canonical tokenId, canonical URL.
      parsed.tokenId = canonicalTokenId(parsed.tokenId);
      parsed.url = opensea.canonicalItemUrl(chain, parsed.contract, parsed.tokenId) || parsed.url;
      parsedLinks.push(parsed);
    }

    /**
     * LOẠI TRÙNG TRƯỚC KHI CHẠM VÀO MẠNG
     *
     *   Đo trên sản phẩm: dán lại đúng 44 link đang theo dõi thì app đọc
     *   Best, metadata và my-offers cho cả 44 — khoảng hai trăm request —
     *   rồi mới tới `db.addNft` từ chối từng cái một, và báo "thêm 0 NFT, 44
     *   lỗi". Hai trăm request để nói một câu mà ta đã biết từ trước khi gửi
     *   request đầu tiên; và đúng lúc hạn mức đọc đang là thứ quý nhất.
     *
     *   Nên hỏi ba câu ngay sau khi parse, đều là câu hỏi cục bộ:
     *     A. NFT này đã có trong danh sách chưa?
     *     B. Nó có xuất hiện hai lần trong chính lần dán này không?
     *     C. (A và B đã đủ: chưa có request nào để mà trùng.)
     *
     *   Danh tính là chain + contract + tokenId — cùng khoá mà `db.addNft`
     *   dùng — chứ không phải chuỗi URL.
     */
    const duplicates = [];
    {
      const identity = p => nftIdentity(chain, p.contract, p.tokenId);
      const tracked = new Set(db.getNfts(chain).map(identity));
      const seenInBatch = new Set();
      const fresh = [];
      for (const parsed of parsedLinks) {
        const id = identity(parsed);
        // addReservations: another Add batch is committing this NFT right now.
        if (tracked.has(id) || seenInBatch.has(id) || addReservations.has(id)) {
          duplicates.push(parsed);
          continue;
        }
        seenInBatch.add(id);
        fresh.push(parsed);
      }
      // Reserved synchronously (no await since the check), released by the
      // handler when this batch ends.
      for (const parsed of fresh) {
        const id = identity(parsed);
        addReservations.add(id);
        reserved.push(id);
      }
      if (duplicates.length) {
        logger.engine(
          `[ADD] bo qua ${duplicates.length} NFT da co trong danh sach — khong goi mang cho chung`
        );
      }
      parsedLinks.length = 0;
      parsedLinks.push(...fresh);
    }

    if (!parsedLinks.length) {
      // Nothing new. Said out loud with its own code - it used to come back
      // ok:true / added:[] and the window toasted "đã thêm NFT" for a paste
      // that added nothing. No request has been sent.
      if (duplicates.length) {
        const error = [duplicateMessage(duplicates.length), ...errors].join(" | ");
        return { ok: false, code: "DUPLICATE", added: 0, duplicates: duplicates.length, errors, error };
      }
      return { ok: errors.length === 0, added: 0, duplicates: 0, errors, message: "Khong co NFT nao de them." };
    }

    // No wallet-wide my-offers scan here (removed in 1.25.0). It was up to four
    // paged requests IN FRONT of every Add, purely to pre-fill the "Mine"
    // cell. The Best read below is a full list for the token, so an offer of
    // ours on it arrives with that read (maker == self) anyway.

    // ---- NFT data from OpenSea metadata, market data from OpenSea offers ----
    //
    // The link already carries the contract and the token id, so identifying
    // the NFT needs no marketplace at all. What used to happen was one OpenSea
    // request PER TOKEN purely for a name, an image and a slug - 300 requests
    // for 300 NFTs, out of the same quota Best Offer competes for. A batch provider
    // answers a hundred in one request, and brings the slug with them.
    //
    // Best Offer is NOT moved. It stays on OpenSea, uncached, exactly as it
    // was: nothing here is allowed to decide what the top bid is.
    // Từ 1.19.17: dữ liệu NFT đọc từ OpenSea (nft-source, song song có trần,
    // có cache). Không còn Alchemy. Đọc hỏng thì hàng vẫn được thêm với tên
    // "#id" và tên/ảnh được làm mới ở nền (bridge.refreshNames).
    /**
     * ADD PER-ROW, KHÔNG RÀO CHẮN CẢ LÔ (1.25.1)
     *
     *   1.25.0: đọc metadata cả lô → worker đọc Best cho MỌI link → chỉ khi
     *   xong hết mới db.addNft/engine.addRow. NFT #1 đợi NFT #117. Nay mỗi
     *   worker (4 song song, cùng trần cũ) làm đúng một NFT tới nơi: slug tối
     *   thiểu (một metadata mỗi CONTRACT, dùng chung) → Best đầy đủ → ghi hàng
     *   + seed sổ + Stream ngay. Tên/ảnh còn thiếu điền ở nền sau cả lô.
     *   Start được bấm trong lúc lô còn chạy → hàng xong sau đó tự chạy.
     */
    const slugByContract = new Map();
    const slugPromise = new Map();
    const metaByUrl = new Map();
    const resolveSlug = async parsed => {
      const known = slugByContract.get(parsed.contract);
      if (known) return known;
      if (!slugPromise.has(parsed.contract)) {
        slugPromise.set(parsed.contract, (async () => {
          try {
            // 1.25.31: Add's own metadata resolution is bootstrap work for a
            // brand-new row, not routine upkeep. At the default P2 it queued
            // behind ~120 running rows' background traffic — measured live,
            // addNfts took >25s to even resolve one NFT's collection slug.
            const meta = await opensea.fetchNftMeta(chain, parsed.contract, parsed.tokenId, { priority: rateLimiter.PRIORITY.INITIAL });
            if (meta) metaByUrl.set(parsed.url, meta);
            const slug = String(meta?.collectionSlug || "").toLowerCase();
            if (slug) slugByContract.set(parsed.contract, slug);
            return slug;
          } catch { return ""; }
        })());
      }
      return slugPromise.get(parsed.contract);
    };

    const commit = entry => {
      const { parsed, best } = entry;
      if (entry.error) errors.push(`${parsed.tokenId}: ${entry.error}`);
      const meta = metaByUrl.get(parsed.url) || null;
      const row = {
        url: parsed.url,
        chain,
        contract: parsed.contract,
        tokenId: parsed.tokenId,
        // Tên thật → tên collection + #id → #id. Chỗ giữ được điền ở nền.
        name: nftName.pickName({
          name: meta?.name || "",
          collectionName: meta?.collectionName || "",
          tokenId: parsed.tokenId
        }).name,
        ...(() => {
          // Keep EVERY candidate the metadata gave. Dropping imageAlts here
          // (it was hard-coded []) left a row whose preview is a broken CDN
          // stub with no fallback — and a row named at Add never reaches the
          // background refresh that would have filled them (add-render-exe #2).
          const seen = [];
          for (const c of [meta?.image, ...(Array.isArray(meta?.imageAlts) ? meta.imageAlts : [])]) {
            const u = nftSource.normalizeImageUrl(c);
            if (u && !seen.includes(u)) seen.push(u);
          }
          return { image: seen[0] || "", imageAlts: seen.slice(1, 4) };
        })(),
        collection: meta?.collectionName || "",
        collectionSlug: slugByContract.get(parsed.contract) ||
          meta?.collectionSlug || best?.collectionSlug || "",
        minPrice: config.minPrice,
        maxPrice: config.maxPrice,
        step: config.step,
        duration: config.duration,
        priorityMode: false
      };

      const stored = db.addNft(chain, row);
      if (!stored.ok) {
        // Checked again at commit: anything that became present while this
        // row was being read is a duplicate, not an error and not a new row.
        if (stored.code === "DUPLICATE") duplicates.push(parsed);
        else errors.push(`${parsed.tokenId}: ${stored.error}`);
        return;
      }
      // Snapshot đầy đủ đi cùng hàng: engine không đọc lại thứ vừa đọc.
      if (best && best.ok === true && best.partial !== true && Array.isArray(best.collectedOrders)) {
        stored.nft.seed = {
          readAt: Date.now(),
          complete: true,
          orders: best.collectedOrders
            .filter(o => o && o.orderHash && Number(o.pricePerItem) > 0)
            .map(o => ({
              orderHash: String(o.orderHash),
              price: Number(o.pricePerItem),
              maker: String(o.maker || "").toLowerCase(),
              kind: o.kind === "collection" ? "collection" : o.kind === "trait" ? "trait" : "item",
              endTime: Number(o.endTime) || 0,
              quantity: Number(o.quantity) || 1
            }))
        };
      }
      const runtime = engine.addRow(stored.nft);
      if (best?.ok) {
        runtime.best = best.price > 0 ? best.price : 0;
        runtime.bestKind = best.kind || "";
      }
      added.push(stored.nft.url);
      pendingNames.push({ parsed, url: stored.nft.url, runtime, named: Boolean(meta && meta.name) });
      // Stream mapping ngay cho collection mới của hàng này.
      if (row.collectionSlug && stream && !knownSlugs.has(row.collectionSlug)) {
        knownSlugs.add(row.collectionSlug);
        stream.setCollections(allCollectionSlugs());
      }
      // Start TẤT CẢ đã được bấm trong lúc lô này chạy: hàng ready tự chạy.
      if (Number(engine.lastStartAllAt) > startedAt && typeof engine.start === "function") {
        engine.start([stored.nft.url]).catch(error =>
          logger.engine(`[ADD] tự chạy #${parsed.tokenId} lỗi: ${error.message}`));
      }
      try { engine.emitNow(); } catch { /* engine đang đóng */ }
    };
    const pendingNames = [];
    const knownSlugs = new Set(allCollectionSlugs());

    const ADD_PARALLEL = 4;
    {
      let next = 0;
      const worker = async () => {
        for (;;) {
          const i = next++;
          if (i >= parsedLinks.length) return;
          const parsed = parsedLinks[i];
          let entry;
          try {
            const slug = await resolveSlug(parsed);
            let orders = [];
            // 1.25.31: this IS the row's authoritative first read, before it
            // even exists in engine.rows (so row.firstRead can't mark it).
            // Left at the default P0 it queues FIFO behind every existing
            // row's own-state-unknown P0 reads — measured live, Add took
            // over 30s per NFT against 120+ running rows.
            const best = await opensea.fetchBestOffer(chain, parsed.contract, parsed.tokenId, slug, {
              useCache: false,
              priority: rateLimiter.PRIORITY.INITIAL,
              collectOrders: list => { orders = Array.isArray(list) ? list : []; }
            });
            best.collectedOrders = orders;
            entry = { parsed, best };
          } catch (error) {
            entry = { parsed, error: opensea.describeError(error) };
          }
          commit(entry);
        }
      };
      await Promise.all(
        Array.from({ length: Math.min(ADD_PARALLEL, parsedLinks.length) }, worker)
      );
    }

    // Tên/ảnh cho hàng còn là chỗ giữ: MỘT lượt theo lô, ở nền.
    const unnamed = pendingNames.filter(p => !p.named);
    if (unnamed.length) {
      nftSource.getNFTs(chain, unnamed.map(p => ({ contract: p.parsed.contract, tokenId: p.parsed.tokenId })))
        .then(answer => {
          if (!answer || !answer.ok) return;
          answer.nfts.forEach((nft, at) => {
            const p = unnamed[at];
            if (!nft || !p) return;
            const picked = nftName.pickName({ name: nft.name || "", collectionName: nft.collectionName || "", tokenId: p.parsed.tokenId });
            const seen = [];
            for (const c of [nft.image, ...(nft.imageAlts || [])]) {
              const u = nftSource.normalizeImageUrl(c);
              if (u && !seen.includes(u)) seen.push(u);
            }
            const patch = { name: picked.name, ...(seen[0] ? { image: seen[0], imageAlts: seen.slice(1, 4) } : {}),
              ...(nft.collectionName ? { collection: nft.collectionName } : {}) };
            try { db.updateNft(chain, p.url, patch); } catch { /* db là phụ */ }
            if (p.runtime && typeof p.runtime === "object") {
              p.runtime.name = picked.name;
              p.runtime.namePlaceholder = picked.placeholder;
              p.runtime.nameCheckedAt = Date.now();
              if (seen[0] && !p.runtime.image) { p.runtime.image = seen[0]; p.runtime.imageAlts = seen.slice(1, 4); }
              else if (seen[0] && !(p.runtime.imageAlts || []).length) {
                // Image already set (e.g. by the bridge's name refresh) but with
                // no fallbacks: add the other candidates, never replace the image.
                p.runtime.imageAlts = seen.filter(u => u !== p.runtime.image).slice(0, 3);
              }
              if (nft.collectionName && !p.runtime.collection) p.runtime.collection = nft.collectionName;
            }
          });
          try { engine.emitNow(); } catch { /* engine đang đóng */ }
        })
        .catch(error => logger.engine(`[ADD] tên/ảnh nền: ${error.message}`));
    }

    logger.engine(
      `${chain}: xu ly ${parsedLinks.length} link trong ${Date.now() - startedAt}ms`
    );

    if (added.length && stream) stream.setCollections(allCollectionSlugs());

    engine.emitNow();
    logger.engine(
      `${chain}: them ${added.length} NFT` +
      `${duplicates.length ? `, ${duplicates.length} da co san` : ""}` +
      `${errors.length ? `, ${errors.length} loi` : ""}`
    );

    /**
     * "KHÔNG THÊM ĐƯỢC GÌ" VÀ "KHÔNG CÓ GÌ MỚI" LÀ HAI CHUYỆN KHÁC NHAU
     *
     *   `ok: added.length > 0` từng biến lần dán thứ hai của cùng danh sách
     *   thành một thất bại đỏ. Thất bại là khi có LỖI; dán lại thứ đã có là
     *   một thao tác thành công không thay đổi gì.
     */
    if (!added.length && duplicates.length && !errors.length) {
      return {
        ok: false, code: "DUPLICATE", added: 0, duplicates: duplicates.length, errors,
        error: duplicateMessage(duplicates.length)
      };
    }
    return {
      ok: errors.length === 0 || added.length > 0,
      added: added.length, duplicates: duplicates.length, errors
    };
  }

  ipcMain.handle("bot:updateNft", (_event, payload) => {
    const chain = normalizeChainName(payload?.chain);
    const engine = getEngine(chain);

    // Backend validation is authoritative (spec 8): the db rejects first, then
    // the engine, so a bad Min/Max can never reach the decision logic.
    const stored = db.updateNft(chain, payload?.url, payload?.patch || {});
    if (!stored.ok) return stored;

    const applied = engine.applyConfigPatch(payload?.url, payload?.patch || {});
    if (!applied.ok) return applied;

    engine.emitNow();
    return { ok: true };
  });

  ipcMain.handle("bot:deleteNft", (_event, payload) => {
    const chain = normalizeChainName(payload?.chain);
    const engine = getEngine(chain);

    engine.removeRow(payload?.url);
    const result = db.deleteNft(chain, payload?.url);

    if (stream) stream.setCollections(allCollectionSlugs());
    engine.emitNow();
    return result;
  });

  ipcMain.handle("bot:deleteNfts", (_event, payload) => {
    const chain = normalizeChainName(payload?.chain);
    const engine = getEngine(chain);
    const urls = Array.isArray(payload?.urls) ? payload.urls : [];

    for (const url of urls) engine.removeRow(url);
    const result = db.deleteNfts(chain, urls);

    if (stream) stream.setCollections(allCollectionSlugs());
    engine.emitNow();
    return result;
  });

  /**
   * XUẤT DANH SÁCH LINK — CHỈ LINK, KHÔNG GÌ KHÁC
   *
   *   Đọc thẳng từ `db.getNfts(chain)` (nguồn lưu thật của dashboard, không
   *   phải bộ nhớ engine đang chạy) nên xuất đúng những gì SAVE đang giữ, kể
   *   cả hàng chưa Start. Mỗi dòng đúng một URL: không header, không Min/Max/
   *   Step/Duration, không trạng thái, không secret nào — file này có thể đi
   *   ra khỏi máy (dán cho người khác, đưa qua kênh khác), nên nó không được
   *   mang gì hơn danh tính NFT.
   *
   *   Dedupe theo chain+contract+tokenId — danh tính thật của một NFT — dù
   *   `db.addNft` đã chặn trùng từ lúc thêm; kiểm lại ở đây vì đây là bước
   *   xuất ra ngoài, không phải chỗ nên tin ngược lại nguồn nó đọc.
   *
   *   Không đăng ký trong REMOTE_IPC_ALLOW: Remote không có hộp thoại Lưu file
   *   của hệ điều hành, và Settings (nơi hai nút Xuất sống) đã là "việc của
   *   Windows" — xem comment ở khai báo REMOTE_IPC_ALLOW.
   */
  ipcMain.handle("bot:exportNfts", async (event, payload) => {
    const chain = normalizeChainName(payload?.chain);
    const chainLabel = chain === "robinhood" ? "Robinhood" : "Ethereum";
    const rows = db.getNfts(chain);

    const seen = new Set();
    const links = [];
    for (const row of rows) {
      const contract = String(row.contract || "").toLowerCase();
      const tokenId = String(row.tokenId || "");
      if (!contract || !tokenId) continue;
      const key = `${chain}:${contract}:${tokenId}`;
      if (seen.has(key)) continue;
      seen.add(key);
      // URL đã lưu là danh tính hiển thị thật của hàng (Robinhood có thể mang
      // một dạng URL khác Ethereum) — chỉ dựng lại khi hàng cũ thiếu nó.
      const url = row.url || opensea.canonicalItemUrl(chain, contract, tokenId);
      if (url) links.push(url);
    }

    if (!links.length) {
      return { ok: false, code: "EMPTY", error: `Không có NFT nào đang lưu ở ${chainLabel} để xuất.` };
    }

    const win = BrowserWindow.fromWebContents(event.sender) || mainWindow;
    const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
    const defaultPath = path.join(
      app.getPath("documents") || app.getPath("desktop"),
      `OfferBot-${chainLabel}-NFTs-${stamp}.txt`
    );

    let chosen;
    try {
      chosen = await dialog.showSaveDialog(win, {
        title: `Xuất danh sách NFT ${chainLabel}`,
        defaultPath,
        filters: [{ name: "Text (*.txt)", extensions: ["txt"] }]
      });
    } catch (error) {
      return { ok: false, code: "DIALOG_FAILED", error: error.message };
    }
    if (!chosen || chosen.canceled || !chosen.filePath) {
      return { ok: false, code: "CANCELLED" };
    }

    try {
      // Mỗi link một dòng, kết thúc bằng \n — không JSON, không cột, không BOM.
      fs.writeFileSync(chosen.filePath, links.join("\n") + "\n", "utf8");
    } catch (error) {
      return { ok: false, code: "WRITE_FAILED", error: error.message };
    }

    logger.engine(`[EXPORT] ${chainLabel}: ${links.length} link → ${chosen.filePath}`);
    return { ok: true, count: links.length, filePath: chosen.filePath };
  });

  // ---- control ---------------------------------------------------

  ipcMain.handle("bot:start", async (_event, payload) => {
    const gate = await requireLicense();
    if (!gate.ok) return gate;

    const engine = getEngine(payload?.chain);
    const result = engine.start(payload?.urls || null);
    engine.emitNow();
    return result;
  });

  ipcMain.handle("bot:pause", (_event, payload) => {
    const engine = getEngine(payload?.chain);
    const result = engine.pause(payload?.urls || null);
    engine.emitNow();
    return result;
  });

  ipcMain.handle("bot:stop", (_event, payload) => {
    const engine = getEngine(payload?.chain);
    const result = engine.stop(payload?.urls || null);
    engine.emitNow();
    return result;
  });

  /**
   * EMERGENCY STOP for one Offer Item dashboard.
   *
   * Reset on the Ethereum tab calls this with chain "ethereum"; Robinhood
   * with "robinhood". They are separate engines, so one tab stopping cannot
   * touch the other - which is the isolation the owner asked for.
   *
   * Distinct from `bot:stop`, which ends the rows' admission and leaves the
   * scheduled work behind. See Engine#emergencyStop for what else has to go.
   */
  ipcMain.handle("bot:emergencyStop", (_event, payload) => {
    const engine = getEngine(payload?.chain);
    const result = engine.emergencyStop(payload?.reason || "reset");
    engine.emitNow();
    return result;
  });

  ipcMain.handle("bot:togglePause", (_event, payload) => {
    const engine = getEngine(payload?.chain);
    const result = engine.togglePause(payload?.url);
    engine.emitNow();
    return result;
  });

  // Offer Item is the only active trading tool. Session status remains read-only.
  ipcMain.handle("session:status", () => openseaSession.getStatus());

  // ---- OpenSea session -------------------------------------------

  /** Manual fallback: a real window the user can drive themselves. */
  ipcMain.handle("opensea:openWindow", async () => {
    try {
      await browser.openLogin();
      return { ok: true };
    } catch (error) {
      return { ok: false, error: opensea.describeError(error) };
    }
  });

  ipcMain.handle("opensea:logout", async () => {
    browser.shutdown();
    return { ok: true };
  });

  // ---- diagnostics -----------------------------------------------
  //
  // Ask the engine. Reading its internals from here was how the Diagnostics
  // panel came to throw a TypeError on the first line of its own handler the
  // moment the engine behind it changed shape - the panel people open when
  // they are already trying to work out what went wrong.

  ipcMain.handle("bot:keyStats", () => keyStatsSnapshot());

  ipcMain.handle("bot:diagnostics", () => ({
    latency: profiler.report(),
    orderBooks: Array.from(engines.values()).map(e => ({
      chain: e.chain,
      ...engineDiagnostics(e).book
    })),
    license: license.status(),
    limiter: rateLimiter.stats(),
    caches: cache.statsAll(),
    logger: logger.stats(),
    stream: stream ? stream.status() : { connected: false },
    engines: Array.from(engines.values()).map(e => engineDiagnostics(e).engine),
    runtime: {
      appIsPackaged: Boolean(app.isPackaged),
      devIsDev: devRuntime.isDev(),
      spendAllowed: devRuntime.spendAllowed(),
      offerbotEnv: process.env.OFFERBOT_ENV || "",
      offerbotDev: process.env.OFFERBOT_DEV === "1"
    }
  }));

  // Full per-NFT diagnostics are explicitly on-demand. Do not add them to
  // bot:diagnostics: the renderer polls that lightweight endpoint periodically.
  ipcMain.handle("bot:diagnosticSnapshot", () => ({
    ok: true,
    generatedAt: Date.now(),
    runtime: {
      appIsPackaged: Boolean(app.isPackaged),
      devIsDev: devRuntime.isDev(),
      spendAllowed: devRuntime.spendAllowed(),
      offerbotEnv: process.env.OFFERBOT_ENV || "",
      offerbotDev: process.env.OFFERBOT_DEV === "1"
    },
    logs: logger.history(),
    chains: Array.from(engines.values()).map(e =>
      typeof e.diagnosticSnapshot === "function" ? e.diagnosticSnapshot() : { chain: e.chain, unavailable: true })
  }));
}

// ------------------------------------------------------------------
// Boot
// ------------------------------------------------------------------

/**
 * The live snapshot is deliberately not versioned. Electron userData is stable
 * for a given appId and Tool slot, so NSIS cannot strand SAVE ON data inside a
 * previous install/version directory. Old folders are migration sources only.
 */
function settingsLocation() {
  // The slot's own base. Slot 1 is the path the app has always used, so an
  // existing install is unaffected; slots 2+ get their own wallet and settings.
  const base = instance.get().settingsBase;
  const version = app.getVersion();
  return {
    base,
    version,
    directory: path.join(base, version),
    snapshotDirectory: app.getPath("userData"),
    file: path.join(app.getPath("userData"), "offerbot-snapshot.json")
  };
}

function boot() {
  const self = instance.get();
  logger.engine(`[INSTANCE] pid=${self.pid}`);
  logger.engine(`[INSTANCE] instanceId=${self.instanceId} slot=${self.slot}/${instance.MAX_INSTANCES}`);
  logger.engine(`[INSTANCE] userData=${self.userData}`);
  logger.engine(`[INSTANCE] sessionPartition=persist:opensea (riêng cho profile này)`);
  // Says whether this window can read what the other windows saved. "replaced"
  // and "seeded" both mean yes; "no-primary-key" and "failed" mean this slot
  // is on its own key and will legitimately ask for credentials again.
  logger.engine(`[INSTANCE] khoá mã hoá dùng chung = ${self.cryptoShare}`);

  if (self.overflow) {
    logger.error(
      `[INSTANCE] không còn slot trống (tối đa ${instance.MAX_INSTANCES}) — ` +
      "cửa sổ này đang dùng chung profile với instance 1. Đóng bớt một instance."
    );
  }

  const location = settingsLocation();

  // First launch after this fix migrates only history owned by this Tool slot.
  // Tool #2 must never inherit Tool #1 config, SAVE switch, or NFT rows.

  // Chuẩn token (ERC721/ERC1155) là tĩnh: nhớ qua các lần chạy, cạnh settings.
  standardStore.configure(app.getPath("userData"));

  const ownPreviousSettings = previousSettingsFiles(location.base, location.version);
  const legacyDatabase = path.join(app.getPath("userData"), "database.json");
  const migrationSources = [
    ...ownPreviousSettings,
    ...(self.slot === 1 ? [legacyDatabase] : [])
  ];

  db = new Database(location.file, {
    appVersion: location.version,
    migrationSources,
    nftMigrationSources: migrationSources,
    onLog: line => logger.engine(line)
  });

  logger.engine(`[SETTINGS] snapshot=${location.file}`);

  // Wired once the settings exist, so there is an address to compare against.
  openseaSession.configure({
    addressProvider: () => {
      try {
        return walletAddressFromPrivateKey(db.getSettings().privateKey);
      } catch {
        return null;
      }
    }
  });

  // ---- CREDENTIALS BELONG TO THIS TOOL SLOT -----------------------------
  //
  // THE BUG THIS FIXES
  //
  //   `instance.pathsFor()` gives slot 2+ its own settingsBase
  //   ("OpenSea Offer Bot - 2"), and the secret store used to live inside it.
  //   So every window past the first had its OWN secrets.json, seeded once at
  //   first launch and never again. Save a License Key or an OpenSea API key
  //   in window A while window B is open, and B never sees it: B is reading a
  //   different file. Reproduced by the owner on this machine.
  //
  //   Worse, the symptom LIED. A slot whose file did not exist yet, or held an
  //   older seed, surfaced as "đã lưu trên máy này nhưng KHÔNG mở ra được" -
  //   a DPAPI decryption failure - when nothing had failed to decrypt at all.
  //   Nobody could act on that message because it named the wrong cause.
  //
  // WHAT STAYS PER SLOT, AND WHY
  //
  //   The Chromium profile (userData) must not be shared: its cookie/
  //   localStorage LevelDB is held exclusively by whichever process opens it
  //   first, so two instances on one profile means one of them is running on
  //   storage that silently refuses to persist. That is the whole reason
  //   instance.js exists and none of it changes here.
  //
  //   Only the ENCRYPTED CREDENTIAL FILE moves, to one canonical path under
  //   the primary base. DPAPI ties the blob to the Windows account, not to a
  //   directory, so every slot on that account can decrypt it.
  //
  //   Slot 1's path is byte-for-byte what it always was - primarySettingsBase
  //   IS its settingsBase - so an existing install migrates nothing.
  const secretFile = path.join(location.directory, "secrets.json");
  migrateSecrets(
    [
      // This slot's own older-version folders, then the primary base's, then
      // the per-slot file this version used to write - so a 1.19.2 window that
      // had already saved credentials into its own slot folder carries them
      // up to the shared one instead of losing them.
      path.join(location.directory, "secrets.json"),
      // v1.19.30 and earlier accidentally shared this file. It is a one-time
      // migration source only; the destination above is always Tool-local.
      //
      // NOT for a brand-new Tool (1.25.12): that file is Tool #1's OWN live
      // credentials, so a new Tool migrated Tool #1's wallet and never reached
      // the new-Tool template below - it opened on Tool #1's wallet instead of
      // the one chosen most recently. A new Tool has no legacy to carry.
      ...(self.freshProfile ? [] : [path.join(self.primarySettingsBase, location.version, "secrets.json")]),
      ...previousSettingsFiles(location.base, location.version, "secrets.json")
    ],
    secretFile,
    line => logger.engine(line)
  );

  secrets = new SecretStore({
    file: secretFile,
    safeStorage,
    onLog: line => logger.engine(line)
  });
  walletProfiles = new WalletProfiles({
    file: path.join(location.directory, "wallet-profiles.json"),
    safeStorage,
    deriveAddress: key => {
      const a = walletAddressFromPrivateKey(key);
      return a ? String(a).toLowerCase() : null;
    }
  });

  // A separate encrypted seed is updated only when a user saves credentials.
  // It gives a brand-new Tool the most recently entered profile without making
  // running Tools share mutable credentials.
  credentialDefaults = new SecretStore({
    file: path.join(self.primarySettingsBase, "credential-defaults.json"),
    safeStorage,
    onLog: line => logger.engine(line)
  });

  if (!secrets.isAvailable()) {
    logger.error(
      "[SECRETS] Windows secure storage không dùng được — " +
      "key sẽ chỉ tồn tại trong phiên này và KHÔNG được ghi ra đĩa."
    );
  } else {
    let restored = secrets.load();
    if (self.freshProfile && !Object.keys(restored.values).length && credentialDefaults.isAvailable()) {
      const seed = credentialDefaults.load().values;
      const fields = {};
      for (const field of ["apiKeys", "apiKey", "apiKey2", "privateKey"]) {
        if (seed[field]) fields[field] = seed[field];
      }
      if (Object.keys(fields).length && secrets.save(fields).ok) {
        restored = secrets.load();
        logger.engine("[INSTANCE] credential template restored for new Tool");
      }
    }
    if (Object.keys(restored.values).length) {
      db.updateSettings(restored.values);
    }
    // The key this Tool already uses is its active profile (1.25.11). A key
    // restored from the new-Tool template becomes this Tool's own from here.
    if (restored.values.privateKey && walletProfiles && !walletProfiles.list().some(p => p.active)) {
      walletProfiles.upsertActive(restored.values.privateKey);
    }
    if (restored.failed.length) {
      /**
       * TRƯỚC KHI BẢO NGƯỜI DÙNG NHẬP LẠI, HÃY THỬ TÌM LẠI
       *
       *   `migrateSecrets` bỏ qua khi file của phiên bản này đã tồn tại. Đúng
       *   trong phần lớn trường hợp, nhưng nó để hở đúng cảnh mà khách hàng
       *   gặp: file có mặt, các trường có mặt, và giải mã thất bại — vì blob
       *   được ký bằng một khoá os_crypt khác. Lúc ấy không lần khởi động nào
       *   thử lại, và bản cũ ngay cạnh đó vẫn còn blob đọc được.
       *
       *   Chỉ nhận một blob khi nó GIẢI MÃ ĐƯỢC bằng khoá hiện tại; giải mã
       *   được nghĩa là nó thuộc về profile này. Không có phỏng đoán nào.
       */
      const rescue = recoverFailedSecrets({
        store: secrets,
        sources: [
          path.join(location.directory, "secrets.json"),
          ...(self.freshProfile ? [] : [path.join(self.primarySettingsBase, location.version, "secrets.json")]),
          ...previousSettingsFiles(location.base, location.version, "secrets.json")
        ],
        failed: restored.failed,
        safeStorage,
        onLog: line => logger.engine(line)
      });
      if (rescue.recovered.length) {
        const again = secrets.load();
        if (Object.keys(again.values).length) db.updateSettings(again.values);
        restored.failed = again.failed;
      }
    }

    /**
     * MIGRATE ONCE, THEN FORGET THE BROKEN LEGACY FIELD (1.25.1)
     *
     *   The legacy single `apiKey` field is obsolete (keys live in apiKeys /
     *   apiKey2). A copy of it encrypted under an older profile failed to
     *   decrypt, was "recovered" or reported, and the same line came back on
     *   every boot. An undecryptable obsolete field is removed from the
     *   canonical store once; real fields still report so the user can re-enter.
     */
    if (restored.failed.length) {
      const obsolete = restored.failed.filter(field => field === "apiKey");
      if (obsolete.length) {
        const out = secrets.save(Object.fromEntries(obsolete.map(field => [field, ""])));
        if (out && out.ok) {
          restored.failed = restored.failed.filter(field => !obsolete.includes(field));
          logger.engine(`[SECRETS] đã gỡ trường cũ không còn dùng (${obsolete.join(", ")}) khỏi kho — không báo lại`);
        }
      }
    }

    if (restored.failed.length) {
      // Names only. Which field failed is what the user needs; the value is
      // exactly what must never reach a log.
      undecryptableSecrets = restored.failed.slice();
      logger.error(
        `[SECRETS] ${restored.failed.length} credential không giải mã được trên ` +
        `tài khoản Windows này: ${restored.failed.join(", ")} — hãy nhập lại ` +
        "đúng (các) mục đó trong Settings."
      );
    }
  }

  // Override server cũ (dev/test) còn trong settings của bản đóng gói:
  // gỡ trước khi applySettings đọc gì, để không có lần boot nào hỏi loopback.
  scrubLicenseServerOverride();

  // ---- the licence belongs to the MACHINE, not to the slot ----------
  //
  // Everything else in secrets.json is per instance on purpose: each window
  // has its own wallet, its own cookies, its own API key. The licence is not
  // like that. It is one entitlement for one PC - the server binds it to a
  // deviceId that every slot already shares - so keeping a copy per slot gave
  // the wrong behaviour in the case that actually happens: slot 2 is opened,
  // then the customer activates in slot 1, and slot 2 goes on asking for a key
  // it is entitled to. The copy made at profile creation cannot fix that; it
  // has already happened by then.
  //
  // So it lives beside device.json, in the machine base, encrypted by the same
  // OS mechanism as every other credential. One file, every slot.
  licenceVault = new SecretStore({
    file: path.join(self.primarySettingsBase, "license.json"),
    safeStorage,
    onLog: line => logger.engine(line)
  });

  {
    const held = String(db.getSettings().licenseKey || "").trim();
    const shared = String(
      (licenceVault.isAvailable() ? licenceVault.load().values.licenseKey : "") || ""
    ).trim();

    if (shared && shared !== held) {
      // Another slot activated. Adopt it rather than asking again.
      db.updateSettings({ licenseKey: shared });
      logger.license("dùng chung licence đã kích hoạt ở cửa sổ khác");
    } else if (held && !shared) {
      // First run after the update: the key this slot already had becomes the
      // shared one. Nothing is erased - the customer keeps what they paid for.
      licenceVault.save({ licenseKey: held });
      logger.license("licence hiện có được chuyển sang kho dùng chung của máy");
    }

    // Kho credential dùng chung với Bulk Offer Cancel (chỉ slot 1): nhận/đẩy
    // trước khi engine dựng, để key mới hơn từ tool kia vào đúng lần boot này.
    try { initSharedCredentials(); } catch (error) { logger.engine(`[SHARED] init=FAILED ${error.message}`); }

    // Kho canonical đã đọc xong — dù có tìm thấy key hay không. Từ đây trở đi
    // "không có licence" là một KẾT LUẬN, không còn là "chưa kịp xem".
    license.hydrated(true);
  }

  // A new instance must never start on the wallet the previous one uses. The
  // licence and API keys carried over; the Private Key deliberately does not,
  // so this window asks for its own.
  // ---- THE PRIVATE KEY IS NO LONGER BLANKED ON A NEW SLOT --------------
  //
  // This used to wipe `privateKey` whenever a slot opened a fresh profile, on
  // the reasoning that "a second instance exists to run a DIFFERENT wallet".
  // That is a real workflow, but it is not the one people were hitting: the
  // common case is one person opening a second window to watch a second
  // collection with the SAME wallet, and being asked to paste their private
  // key again - into a box that, before the shared store above, then saved it
  // somewhere the first window could not see either.
  //
  // Credentials are now shared by the machine, so a new window inherits the
  // wallet the way it already inherits the licence and the API keys. Running
  // two different wallets is still possible - change the key in Settings and
  // it changes for every window, which is the honest consequence of one
  // shared store and is stated here rather than discovered.
  if (self.freshProfile) {
    logger.engine(
      `[INSTANCE] slot ${self.slot} dùng CHUNG credential của máy ` +
      "(licence, API key, Private Key) — không phải nhập lại."
    );
  }

  // A settings file that could not be parsed is reported, never quietly
  // replaced with defaults: the user needs to know why their config vanished.
  if (db.loadError) logger.error(`[SETTINGS] ${db.loadError}`);

  // ---- Offer Item: hạn mức ghi dùng chung cả máy --------------------
  //
  // Dựng TRƯỚC createEngines vì mỗi engine giữ một tham chiếu tới nó. Bầu
  // leader chạy bất đồng bộ; trong lúc chưa xong thì engine chạy ở chế độ
  // thận trọng, không phải chế độ ghi tự do.
  offerItemQuota = new QuotaBroker({
    getApiKey: () => rateLimiter.apiKeys.fingerprint(),
    onLog: line => logger.engine(line)
  });

  createEngines();
  normalizeApiKeySlots();
  applySettings();
  startKeyStatsPush();

  // KHỞI ĐỘNG BROKER SAU KHI POOL KEY ĐÃ NẠP: danh tính (tên pipe) tính từ
  // key thật. Trước đây start() chạy trước applySettings() nên mọi cửa sổ đều
  // là "nokey" — chia sẻ đúng nhưng sai tên, và không đổi khi đổi key.
  offerItemQuota.start()
    .then(role => logger.engine("[QUOTA] Offer Item · vai = " + role))
    .catch(error => logger.error("[QUOTA] không khởi động được: " + error.message));

  // Forward every log line to the Logs tab.
  logger.subscribe(entry => send("bot:log", entry));
  // Pushed, not polled: the session changes on its own now.
  openseaSession.onStatus(state => send("session:status", state));

  registerIpc();

  remoteHost = new RemoteHost({
    getState: remoteState,
    command: remoteCommand,
    onLog: line => logger.engine(line)
  });
  remoteHost.start(41739 + Math.max(0, self.slot - 1))
    .catch(error => logger.error(`[REMOTE] không mở được host: ${error.message}`));

  // The page's wallet requests are answered here, never in the renderer, and
  // only for opensea.io.
  browser.attachWallet(ipcMain, wallet);

  createWindow();
  startStream();
  startHousekeeping();

  // The first answer the window gets, and a watch for the ones after it. A
  // background refresh that returns REVOKED must lock the app without the user
  // touching anything.
  publishLicense(true);

  // From here on, every settled OpenSea request tells the API guard what it
  // learned about the credential. Registered once, at boot.
  opensea.setCredentialWatcher(noteApiResult);
  publishApi(true);

  if (devRuntime.updaterEnabled()) {
    updater = new Updater({
      currentVersion: app.getVersion(),
      onState: publishUpdate,
      onLog: line => logger.engine(line),
      // Ghi snapshot TRƯỚC khi giao quyền cho installer — không dựa vào việc
      // `quitAndInstall` có phát `before-quit` hay không. Mất dữ liệu sau update
      // là thứ người dùng không tha thứ, và không thể tái hiện để sửa sau.
      onBeforeInstall: () => {
        try { if (db) db.write(db.snapshot()); }
        catch (error) { logger.error(`[SETTINGS] pre-install snapshot failed: ${error.message}`); }
      }
    });
  } else {
    // Bản dev không tải, không cài, và không ghi trạng thái updater nào —
    // nếu không nó có thể kéo bản production đang chạy sang một phiên bản
    // khác giữa chừng.
    logger.engine("[DEV] auto-update TẮT (OFFERBOT_ENV=development)");
  }

  /**
   * The licence beat. ONE timer, sixty seconds, for the life of the app.
   *
   * Every recheck in the app happens on this tick or on an action's own gate.
   * There is deliberately no second loop and no per-feature poller: two timers
   * become two request streams, and two request streams become a licence
   * server answering the same question twice a second for no reason.
   *
   * Overlap is impossible by construction rather than by timing - the client
   * keeps one in-flight check and hands the same promise to anyone who asks
   * while it is running.
   *
   * It is also entirely unrelated to error reporting: nothing here uploads
   * anything. A report is sent when, and only when, the user presses Báo lỗi.
   */
  const licenseWatch = setInterval(() => {
    // A window sitting on the activation screen while ANOTHER window activates
    // should unlock by itself, not on the next restart. Only ever adopts a key
    // when this window has none, so a window is never switched off the key it
    // is already running on.
    try { sharedPoll(); } catch { /* the beat continues regardless */ }
    try { adoptSharedLicence(); } catch { /* the beat continues regardless */ }
    try { publishLicense(); } catch { /* a status check must never break the tick */ }
  }, LICENSE_RECHECK_MS);
  timers.push(licenseWatch);

  logger.engine("OpenSea Offer Bot ready.");
}

/**
 * Nhả broker hạn mức khi cửa sổ đóng.
 *
 * Leader phải ngắt follower ra TRƯỚC khi đóng pipe, nếu không chúng chờ hết
 * hạn thay vì bầu lại ngay — đo được: đóng cửa sổ leader làm hai cửa sổ còn
 * lại đứng 15 giây. Đó là lý do chỗ này gọi stop() tường minh thay vì để
 * tiến trình chết tự nhiên.
 */
app.on("before-quit", () => {
  remoteHost?.stop();
  if (offerItemQuota) {
    offerItemQuota.stop().catch(() => {});
    offerItemQuota = null;
  }
});

// No single-instance lock.
//
// It used to be taken here, which is why opening a second copy of the app made
// the FIRST window blink and the second one exit before it had drawn anything.
// Instances are separated by profile now (see instance.js): each one has its
// own userData, its own OpenSea cookies and its own Private Key, so several can
// run side by side on different wallets without one overwriting the other.
app.whenReady().then(boot);

app.on("window-all-closed", () => {
  if (process.platform !== "darwin") app.quit();
});

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
});

// Also covers quits that do not come from the window: the tray, a task
// manager close, or Windows shutting down.
app.on("before-quit", () => shutdownApp("before-quit"));

// Nothing may take the process down (spec 25).
process.on("unhandledRejection", reason => {
  logger.error(`unhandledRejection: ${reason?.message || reason}`);
});

process.on("uncaughtException", error => {
  logger.error(`uncaughtException: ${error?.message || error}`);
});
