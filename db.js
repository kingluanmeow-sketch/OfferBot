"use strict";

/**
 * JSON-file store.
 *
 * Persistence model:
 *   - non-secret settings (per-chain defaults, engine tuning) ARE persistent
 *   - SECRETS are session-only: API keys, the wallet private key and the
 *     licence key are held in memory and blanked in the file, so closing the
 *     app leaves nothing on disk to steal. They must be entered again next run.
 *   - NFT rows are SESSION-ONLY: closing the app clears the tables
 *
 * This file also owns the authoritative price validation. The renderer
 * validates too, for instant feedback, but the backend rejects independently
 * (spec 8) so a bad value can never reach the engine.
 */

const fs = require("fs");
const path = require("path");

const CHAINS = ["ethereum", "robinhood"];

/**
 * Per-chain "Add NFT" panel config.
 *
 * This IS the saved default: whatever the user last typed into the Add panel is
 * written straight back here, so reopening the app restores it. There is no
 * separate default-price section in Settings to keep in sync.
 */
const DEFAULT_CHAIN_SETTINGS = Object.freeze({
  minPrice: "0.001",
  maxPrice: "0.2",
  step: "0.001",
  duration: "15",
  saveEnabled: false
});

const DEFAULT_SETTINGS = Object.freeze({
  // One key per line. A pool is rotated across requests: OpenSea meters per
  // key, so several keys are several times the quota - but only if traffic is
  // actually spread over them.
  apiKeys: "",
  apiKey: "",
  apiKey2: "",
  privateKey: "",
  licenseKey: "",
  // Where the owner's licence server is. NOT a secret - it is a URL, and it is
  // the licence key beside it that authorises anything.
  licenseServerUrl: "",
  // Dark unless the user says otherwise. The OS preference is deliberately
  // not consulted: once someone picks a theme in the app, that is the answer.
  theme: "dark",
  scanWorkers: "3",
  offerIntervalSeconds: "3",

  ethereum: { ...DEFAULT_CHAIN_SETTINGS },
  robinhood: { ...DEFAULT_CHAIN_SETTINGS }
});

/**
 * Settings that must never be written to disk.
 *
 * The app needs them while it runs, so they live in memory for the session and
 * are blanked in the copy that is persisted. The consequence is deliberate: on
 * the next launch these boxes are empty and have to be filled in again.
 */
const SECRET_FIELDS = Object.freeze([
  "apiKeys", "apiKey", "apiKey2", "privateKey", "licenseKey"
]);

/** Statuses the engine is allowed to write (spec 6). */
const STATUS = Object.freeze({
  ON_TOP: "ON_TOP",
  ACTIVE: "ACTIVE",
  OUTBID: "OUTBID",
  LOW_BALANCE: "LOW_BALANCE",
  SEND_FAILED: "SEND_FAILED",
  // OpenSea read the order and refused it. Not a transport failure: the
  // payload itself is wrong for this contract, so the same bytes will be
  // refused again and the row stops resending them.
  SUBMIT_BLOCKED: "SUBMIT_BLOCKED",
  PAUSED: "PAUSED",
  ERROR: "ERROR"
});

/** Scan-status values (spec 5). Note: never ON TOP / OUTBID. */
/**
 * What a row is doing right now.
 *
 * PREPARING / SEND_OFFER / VERIFYING / RETRYING are the submit broken into
 * the stages a person can act on: waiting for a send slot is not the same as
 * having signed, and "it failed" is not the same as "it is trying again".
 * One state for the whole submit meant a row sat on "Đang gửi offer" for ten
 * seconds of queueing and looked stuck.
 */
const SCAN_STATE = Object.freeze({
  SCANNING: "SCANNING",
  PREPARING: "PREPARING",
  SEND_OFFER: "SEND_OFFER",
  VERIFYING: "VERIFYING",
  RETRYING: "RETRYING",
  NEXT_SCAN: "NEXT_SCAN",
  STOPPED: "STOPPED",
  ERROR: "ERROR"
});

function toStr(value, fallback = "") {
  if (value === undefined || value === null) return fallback;
  return String(value);
}

function toNum(value) {
  const n = Number(String(value ?? "").trim());
  return Number.isFinite(n) ? n : NaN;
}

/**
 * Authoritative price validation (spec 8).
 * @returns {{ok:true, min:number, max:number, step:number}|{ok:false, error:string}}
 */
/*
 * Scan Delay and Max Speed used to live here.
 *
 * Scan Delay was a user-typed number of seconds that decided when a row was
 * next allowed to react, and Max Speed was a per-row flag that swapped that
 * number for a fixed one-second floor. Both are gone: the stream is the
 * trigger now, so a hand-typed polling cadence no longer decides when an NFT
 * is looked at.
 *
 * What replaced Max Speed is Priority - and it is deliberately NOT a speed. It
 * changes the ORDER rows are served in when several are waiting, and nothing
 * else. The minimum gap between two reactions on one NFT is now a fixed
 * internal constant in engine.js, so the protection Scan Delay provided is
 * still there and is no longer something the user can set to a value that
 * would hammer the API.
 */

function validatePrices({ minPrice, maxPrice, step }) {
  const min = toNum(minPrice);
  const max = toNum(maxPrice);
  const stepValue = toNum(step);

  if (!Number.isFinite(min) || min <= 0) {
    return { ok: false, error: "Min Price phai la so > 0." };
  }
  if (!Number.isFinite(max) || max <= 0) {
    return { ok: false, error: "Max Price phai la so > 0." };
  }
  if (!Number.isFinite(stepValue) || stepValue <= 0) {
    return { ok: false, error: "Step phai la so > 0." };
  }
  if (min > max) {
    return { ok: false, error: "Min Price khong duoc lon hon Max Price." };
  }

  return { ok: true, min, max, step: stepValue };
}

function normalizeChainName(chain) {
  const c = toStr(chain, "ethereum").trim().toLowerCase();
  return CHAINS.includes(c) ? c : "ethereum";
}

/**
 * Settings files belonging to other versions of the app, newest first.
 *
 * The store lives in a per-version folder, so a fresh release starts with an
 * empty one. Without this an upgrade would look exactly like the bug it was
 * meant to prevent: the user opens the new build and every setting is gone.
 */
function previousSettingsFiles(baseDir, currentVersion, fileName = "settings.json") {
  let names = [];
  try { names = fs.readdirSync(baseDir); } catch { return []; }

  const parse = v => String(v).split(".").map(n => Number(n) || 0);
  const isVersion = v => /^\d+\.\d+\.\d+$/.test(v);

  // Only OLDER versions are migration sources (1.25.2). A directory named for
  // a NEWER version is never a legitimate predecessor — e.g. "33.4.11", created
  // by unpackaged test runs where app.getVersion() is Electron's version — and
  // migrating from it would import test credentials into production.
  const cmp = (a, b) => { const x = parse(a), y = parse(b); for (let i = 0; i < 3; i++) if (x[i] !== y[i]) return x[i] - y[i]; return 0; };
  return names
    .filter(isVersion)
    .filter(v => v !== currentVersion)
    .filter(v => !isVersion(currentVersion) || cmp(v, currentVersion) < 0)
    .sort((a, b) => {
      const [aa, ab, ac] = parse(a);
      const [ba, bb, bc] = parse(b);
      return bb === ab && ba === aa ? bc - ac : (ba === aa ? bb - ab : ba - aa);
    })
    .map(v => path.join(baseDir, v, fileName))
    .filter(p => fs.existsSync(p));
}

class Database {
  /**
   * @param {string} filePath      where settings are read from and written to
   * @param {object} [options]
   * @param {string[]} [options.migrationSources] older files to seed from, in order
   * @param {string[]} [options.nftMigrationSources] sources owned by this Tool slot
   * @param {string} [options.appVersion] stamped into the file for diagnosis
   * @param {(line:string)=>void} [options.onLog] never receives secret values
   */
  constructor(filePath, options = {}) {
    this.filePath = filePath;
    this.migrationSources = options.migrationSources || [];
    this.nftMigrationSources = new Set(
      (options.nftMigrationSources || []).map(source => path.resolve(source))
    );
    this.appVersion = options.appVersion || "";
    this.onLog = typeof options.onLog === "function" ? options.onLog : () => {};
    this.data = this.emptyData();
    this.ensureDirectory();
    this.load();
  }

  emptyData() {
    return {
      settings: JSON.parse(JSON.stringify(DEFAULT_SETTINGS)),
      nfts: { ethereum: [], robinhood: [] }
    };
  }

  ensureDirectory() {
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
  }

  // ----------------------------------------------------------------
  // Normalisation
  // ----------------------------------------------------------------

  normalizeChainSettings(raw, defaults = DEFAULT_CHAIN_SETTINGS) {
    const value = raw && typeof raw === "object" ? raw : {};
    return {
      minPrice: toStr(value.minPrice, defaults.minPrice),
      maxPrice: toStr(value.maxPrice, defaults.maxPrice),
      step: toStr(value.step ?? value.defaultStep, defaults.step),
      duration: toStr(value.duration ?? value.offerDuration, defaults.duration)
      ,saveEnabled: value.saveEnabled === true
    };
  }

  /**
   * Persist the Add-NFT panel config for one chain.
   * Called on every edit in the panel, so the config survives a restart without
   * the user pressing any Save button.
   */
  saveChainConfig(chain, config) {
    const c = normalizeChainName(chain);
    const merged = { ...this.data.settings[c], ...(config || {}) };

    const check = validatePrices(merged);
    if (!check.ok) return { ok: false, error: check.error };


    this.data.settings[c] = this.normalizeChainSettings(merged);
    this.write(this.data);

    return { ok: true, config: JSON.parse(JSON.stringify(this.data.settings[c])) };
  }

  /** Push one field onto every row of a chain. */
  patchAllNfts(chain, patch) {
    const c = normalizeChainName(chain);
    const list = this.data.nfts[c];

    for (let i = 0; i < list.length; i++) {
      const merged = { ...list[i], ...(patch || {}) };
      const check = validatePrices(merged);
      if (!check.ok) continue;
      list[i] = { ...list[i], ...(patch || {}) };
    }

    this.write(this.data);
    return { ok: true, applied: list.length };
  }

  normalizeSettings(raw) {
    const value = raw && typeof raw === "object" ? raw : {};

    return {
      apiKeys: toStr(value.apiKeys),
      apiKey: toStr(value.apiKey),
      apiKey2: toStr(value.apiKey2),
      privateKey: toStr(value.privateKey),
      licenseKey: toStr(value.licenseKey),
      licenseServerUrl: toStr(value.licenseServerUrl),
      theme: value.theme === "light" ? "light" : "dark",
      scanWorkers: toStr(value.scanWorkers, DEFAULT_SETTINGS.scanWorkers),
      offerIntervalSeconds: toStr(
        value.offerIntervalSeconds,
        DEFAULT_SETTINGS.offerIntervalSeconds
      ),

      ethereum: this.normalizeChainSettings(value.ethereum),
      robinhood: this.normalizeChainSettings(value.robinhood)
    };
  }

  normalizeNft(raw, chain) {
    const x = raw && typeof raw === "object" ? raw : {};
    const c = normalizeChainName(x.chain || chain);

    return {
      // identity
      url: toStr(x.url).trim(),
      chain: c,
      contract: toStr(x.contract).toLowerCase(),
      tokenId: toStr(x.tokenId),

      // metadata
      name: toStr(x.name, "NFT"),
      image: toStr(x.image),
      // Other URLs for the same picture, best first. The screen falls through
      // to these when one fails to load - a CDN can answer 200 with a stub
      // that is not a decodable image, and one URL cannot express that.
      imageAlts: Array.isArray(x.imageAlts)
        ? x.imageAlts.map(v => toStr(v)).filter(Boolean).slice(0, 4) : [],
      collection: toStr(x.collection),
      collectionSlug: toStr(x.collectionSlug || x.collection_slug),


      // config
      minPrice: toStr(x.minPrice, DEFAULT_CHAIN_SETTINGS.minPrice),
      maxPrice: toStr(x.maxPrice, DEFAULT_CHAIN_SETTINGS.maxPrice),
      step: toStr(x.step, DEFAULT_CHAIN_SETTINGS.step),
      duration: toStr(x.duration, DEFAULT_CHAIN_SETTINGS.duration),

      // Priority is per NFT and persisted per NFT, so a row the owner
      // marked stays marked across a restart. It orders the queue; it is
      // not a speed and not a delay.
      priorityMode: Boolean(x.priorityMode),

      // live values
      best: x.best === undefined || x.best === null ? null : Number(x.best),
      mine: x.mine === undefined || x.mine === null ? null : Number(x.mine),
      bestOfferMaker: x.bestOfferMaker || null,
      bestOfferOrderHash: x.bestOfferOrderHash || null,
      bestOfferKind: toStr(x.bestOfferKind),
      myOfferOrderHash: x.myOfferOrderHash || null,
      myOfferOptimistic: Boolean(x.myOfferOptimistic),

      // runtime
      running: false,
      status: toStr(x.status, STATUS.PAUSED),
      scanState: toStr(x.scanState, SCAN_STATE.STOPPED),
      nextScanAt: 0,
      lastScanAt: 0,
      lastError: toStr(x.lastError),
      offersSent: Number(x.offersSent) || 0
    };
  }

  sanitize(raw) {
    const value = raw && typeof raw === "object" ? raw : {};
    const nftsRaw = value.nfts && typeof value.nfts === "object" ? value.nfts : {};

    const nfts = {};
    for (const chain of CHAINS) {
      // Accept the legacy flat V1 shape too (nfts: [], robinhoodNfts: []).
      const legacy =
        chain === "ethereum"
          ? Array.isArray(value.nfts)
            ? value.nfts
            : []
          : Array.isArray(value.robinhoodNfts)
            ? value.robinhoodNfts
            : [];

      const list = Array.isArray(nftsRaw[chain]) ? nftsRaw[chain] : legacy;
      nfts[chain] = list
        .filter(Boolean)
        .map(item => this.normalizeNft(item, chain))
        .filter(item => item.url && item.contract && item.tokenId);
    }

    return { settings: this.normalizeSettings(value.settings), nfts };
  }

  // ----------------------------------------------------------------
  // Load / save
  // ----------------------------------------------------------------

  load() {
    this.onLog(`[SETTINGS] path=${this.filePath}`);

    if (!fs.existsSync(this.filePath)) {
      // First run for this Tool slot, or its one-time migration from the old
      // per-version store into stable Electron userData.
      const seeded = this.seedFromPreviousVersion();
      this.write(this.data);
      this.onLog(seeded
        ? `[SETTINGS] load=OK migrated-from=${seeded}`
        : "[SETTINGS] load=OK new-file");
      return;
    }

    try {
      const parsed = JSON.parse(fs.readFileSync(this.filePath, "utf8"));
      const restored = this.sanitize(parsed);

      // Chỉ những tab bật SAVE mới phục hồi danh sách. Runtime luôn được
      // normalizeNft đưa về PAUSED/running=false, nên mở máy không tự chi tiền.
      for (const chain of CHAINS) {
        if (!restored.settings[chain].saveEnabled) restored.nfts[chain] = [];
      }

      this.data = restored;

      // A file from an earlier build may still hold plaintext secrets. Rewrite
      // it now so they are gone from disk even if the user never changes a
      // setting this session.
      if (SECRET_FIELDS.some(f => restored.settings[f])) this.write(restored);
      this.onLog("[SETTINGS] load=OK");
    } catch (error) {
      // Keep the damaged file. Replacing it without a copy destroys the only
      // evidence of what went wrong, and the user is told rather than quietly
      // handed defaults.
      const backup = `${this.filePath}.bak-${Date.now()}`;
      try { fs.copyFileSync(this.filePath, backup); } catch { /* best effort */ }
      this.loadError = `settings file hỏng (${error.message}); đã sao lưu tại ${backup}`;
      this.onLog(`[SETTINGS] load=FAILED ${error.message} backup=${backup}`);
      this.data = this.emptyData();
      // If an interrupted older build left a damaged current file, recover
      // from this Tool's newest legacy snapshot before falling back to empty.
      // The damaged original remains beside it as the timestamped .bak above.
      const recovered = this.seedFromPreviousVersion();
      this.write(this.data);
      if (recovered) this.onLog(`[SETTINGS] recovered-from=${recovered}`);
    }
  }

  /**
   * Copy non-secret settings from the newest older version that has them.
   * @returns {string|null} the file used, for the log
   */
  seedFromPreviousVersion() {
    for (const source of this.migrationSources) {
      try {
        if (!fs.existsSync(source)) continue;
        const parsed = JSON.parse(fs.readFileSync(source, "utf8"));
        const restored = this.sanitize(parsed);
        // Secrets are never carried across: they are session-only by policy,
        // and an old file that still holds one must not reintroduce it.
        for (const field of SECRET_FIELDS) restored.settings[field] = "";
        // A version upgrade keeps rows for every chain whose SAVE switch was
        // on. Callers provide only this Tool slot's own migration history.
        const mayRestoreNfts = this.nftMigrationSources.has(path.resolve(source));
        for (const chain of CHAINS) {
          if (!mayRestoreNfts || !restored.settings[chain].saveEnabled) {
            restored.nfts[chain] = [];
          }
        }
        this.data = restored;
        return source;
      } catch {
        /* try the next candidate */
      }
    }
    return null;
  }

  write(data) {
    this.data = this.sanitize(data);

    // Secrets are session-only. They stay in this.data so the running app can
    // use them, but the copy that reaches the disk has them blanked: an API
    // key, a wallet private key or a licence token written in plaintext under
    // %APPDATA% outlives the process and is readable by anything running as
    // this user, including any other program on the machine.
    const settings = JSON.parse(JSON.stringify(this.data.settings));
    for (const field of SECRET_FIELDS) settings[field] = "";

    const persistedNft = row => ({
      url: row.url,
      chain: row.chain,
      contract: row.contract,
      tokenId: row.tokenId,
      name: row.name,
      image: row.image,
      imageAlts: row.imageAlts,
      collection: row.collection,
      collectionSlug: row.collectionSlug,
      minPrice: row.minPrice,
      maxPrice: row.maxPrice,
      step: row.step,
      duration: row.duration,
      priorityMode: Boolean(row.priorityMode)
    });
    const nfts = {};
    for (const chain of CHAINS) {
      nfts[chain] = settings[chain]?.saveEnabled
        ? (this.data.nfts[chain] || []).map(persistedNft)
        : [];
    }
    const onDisk = { version: this.appVersion, settings, nfts };

    // Atomic replace: a crash mid-write can never truncate the real file. The
    // directory is re-checked here because the app may be launched long after
    // construction, and a user can delete the folder underneath it.
    this.ensureDirectory();
    const temp = `${this.filePath}.tmp`;
    const handle = fs.openSync(temp, "w");
    try {
      fs.writeFileSync(handle, JSON.stringify(onDisk, null, 2), "utf8");
      // Without this the rename can land before the bytes do, and a power loss
      // leaves an empty file that parses as "no settings".
      fs.fsyncSync(handle);
    } finally {
      fs.closeSync(handle);
    }
    fs.renameSync(temp, this.filePath);
  }

  /** Deep copy so callers can never mutate internal state by reference. */
  snapshot() {
    return JSON.parse(JSON.stringify(this.data));
  }

  // ----------------------------------------------------------------
  // Settings
  // ----------------------------------------------------------------

  getSettings() {
    return JSON.parse(JSON.stringify(this.data.settings));
  }

  updateSettings(patch) {
    const next = { ...this.data.settings };
    const value = patch && typeof patch === "object" ? patch : {};

    // Every scalar setting, derived rather than retyped.
    //
    // This was a hand-written list, and two fields were missing from it:
    // `theme`, which is why a chosen theme reverted on every launch, and
    // `licenseServerUrl`, which meant applySettings read a value nothing was
    // able to write. Both were dropped in silence - the save returned ok.
    //
    // DEFAULT_SETTINGS is the shape of a settings object, so taking the list
    // from it means a field added there is writable the moment it exists.
    // Chains are objects and are merged separately below.
    for (const field of Object.keys(DEFAULT_SETTINGS)) {
      if (CHAINS.includes(field)) continue;
      if (value[field] === undefined) continue;
      next[field] = toStr(value[field]);
    }

    for (const chain of CHAINS) {
      if (!value[chain]) continue;
      const merged = { ...next[chain], ...value[chain] };
      const check = validatePrices(merged);
      if (!check.ok) {
        return { ok: false, error: `${chain}: ${check.error}` };
      }
      next[chain] = this.normalizeChainSettings(merged);
    }

    this.data.settings = this.normalizeSettings(next);
    this.write(this.data);
    return { ok: true, settings: this.getSettings() };
  }

  setSaveEnabled(chain, enabled) {
    const c = normalizeChainName(chain);
    this.data.settings[c].saveEnabled = enabled === true;
    this.write(this.data);
    return { ok: true, chain: c, saveEnabled: this.data.settings[c].saveEnabled };
  }

  // ----------------------------------------------------------------
  // NFT rows
  // ----------------------------------------------------------------

  getNfts(chain) {
    const c = normalizeChainName(chain);
    return JSON.parse(JSON.stringify(this.data.nfts[c] || []));
  }

  findNft(chain, url) {
    const c = normalizeChainName(chain);
    const list = this.data.nfts[c] || [];
    return list.find(item => item.url === url) || null;
  }

  /**
   * Insert a row. Rejects duplicates and invalid prices.
   * @returns {{ok:boolean, error?:string, nft?:object}}
   */
  addNft(chain, nft) {
    const c = normalizeChainName(chain);
    const normalized = this.normalizeNft({ ...nft, chain: c }, c);

    if (!normalized.url || !normalized.contract || !normalized.tokenId) {
      return { ok: false, error: "NFT thieu url / contract / tokenId." };
    }

    const check = validatePrices(normalized);
    if (!check.ok) return { ok: false, error: check.error };

    /**
     * DANH TÍNH CỦA MỘT NFT LÀ chain + contract + tokenId, KHÔNG PHẢI URL
     *
     *   URL chỉ là một cách viết ra danh tính đó, và OpenSea có nhiều cách
     *   viết cho cùng một token (kèm query, kèm tham số theo dõi, dạng
     *   /assets/ cũ). So theo URL nên hai lần dán cùng một NFT có thể lọt qua
     *   thành hai hàng, còn cùng một NFT viết khác đi thì báo trùng nhầm.
     *
     *   Và "đã có" KHÔNG PHẢI LÀ LỖI: mã riêng để người gọi nói đúng câu
     *   "44 NFT đã tồn tại" thay vì "44 lỗi".
     */
    const list = this.data.nfts[c];
    // Token ids compared canonically: "007" and "7" are the same token.
    const canonId = v => {
      const t = String(v ?? "").trim();
      return /^\d+$/.test(t) ? BigInt(t).toString() : t;
    };
    const same = item =>
      String(item.contract || "").toLowerCase() === String(normalized.contract || "").toLowerCase() &&
      canonId(item.tokenId) === canonId(normalized.tokenId);
    if (list.some(item => item.url === normalized.url || same(item))) {
      return { ok: false, code: "DUPLICATE", error: "NFT da co trong danh sach." };
    }

    list.push(normalized);
    this.write(this.data);
    return { ok: true, nft: JSON.parse(JSON.stringify(normalized)) };
  }

  /**
   * Patch a row. Price fields are re-validated on every write, so the engine
   * can trust min <= max unconditionally.
   */
  updateNft(chain, url, patch) {
    const c = normalizeChainName(chain);
    const list = this.data.nfts[c];
    const index = list.findIndex(item => item.url === url);
    if (index === -1) return { ok: false, error: "Khong tim thay NFT." };

    const current = list[index];
    const merged = { ...current, ...(patch || {}), chain: c, url: current.url };

    const touchesPrice =
      patch &&
      ("minPrice" in patch || "maxPrice" in patch || "step" in patch);

    if (touchesPrice) {
      const check = validatePrices(merged);
      if (!check.ok) return { ok: false, error: check.error };
    }

    // normalizeNft resets runtime fields, so re-apply the live ones explicitly.
    const normalized = this.normalizeNft(merged, c);
    normalized.running = merged.running === true;
    normalized.status = toStr(merged.status, current.status);
    normalized.scanState = toStr(merged.scanState, current.scanState);
    normalized.nextScanAt = Number(merged.nextScanAt) || 0;
    normalized.lastScanAt = Number(merged.lastScanAt) || 0;

    list[index] = normalized;
    this.write(this.data);
    return { ok: true, nft: JSON.parse(JSON.stringify(normalized)) };
  }

  deleteNft(chain, url) {
    const c = normalizeChainName(chain);
    const list = this.data.nfts[c];
    const index = list.findIndex(item => item.url === url);
    if (index === -1) return { ok: false, error: "Khong tim thay NFT." };

    list.splice(index, 1);
    this.write(this.data);
    return { ok: true };
  }

  deleteNfts(chain, urls) {
    const c = normalizeChainName(chain);
    const wanted = new Set((urls || []).map(String));
    const before = this.data.nfts[c].length;

    this.data.nfts[c] = this.data.nfts[c].filter(item => !wanted.has(item.url));
    this.write(this.data);

    return { ok: true, removed: before - this.data.nfts[c].length };
  }
}

module.exports = {
  Database,
  SECRET_FIELDS,
  previousSettingsFiles,
  CHAINS,
  STATUS,
  SCAN_STATE,
  DEFAULT_SETTINGS,
  DEFAULT_CHAIN_SETTINGS,
  validatePrices,
  normalizeChainName
};
