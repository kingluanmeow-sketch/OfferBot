"use strict";

/**
 * session.js — is the OpenSea session usable, and by which wallet?
 *
 * "Logged in" turned out to be several different questions wearing one answer,
 * and conflating them cost a full debugging session: a disabled account keeps
 * its profile title and passes any ordinary login check, while its offers page
 * renders no rows - and with no rows there is no collection filter, so a dead
 * account surfaced as "no Collections filter" and sent everyone looking at the
 * sidebar code.
 *
 * So each cause gets its own state and its own sentence:
 *
 *   connected         the session works and belongs to our wallet
 *   signed-out        no session; signing in would fix it
 *   account-disabled  OpenSea disabled this account; signing in cannot fix it
 *   wrong-wallet      a session, but for a different wallet than the stored key
 *   login-required    no Private Key to log in with
 *   login-failed      auto-login ran and produced no session
 *
 * On signing: this module only ever triggers OpenSea's own sign-in flow, and
 * the wallet refuses anything that commits while disarmed - so an offer, a
 * cancel, an approval or a transfer cannot be signed from here even if OpenSea
 * asked for one.
 */

const browser = require("./browser");
const rateLimiter = require("./rate-limiter");
const { logger } = require("./logger");

const TAG = "[SESSION]";

/**
 * OpenSea có đang chặn tốc độ ngay lúc này không.
 *
 * Bộ giới hạn ghi lại từng lần bị 429 và tự lùi tốc độ; `serverLimited`
 * đúng khi nó đang trong lúc lùi. Dùng để PHÂN BIỆT một trang không tải
 * được địa chỉ vì bị chặn tốc độ với một ví thật sự sai — hai chuyện cần
 * hai câu trả lời khác nhau, và trước đây chúng dùng chung một câu.
 */
function openSeaIsThrottling() {
  try {
    const buckets = (rateLimiter.stats() || {}).buckets || [];
    return buckets.some(b => b && (b.serverLimited === true || b.throttledForMs > 0));
  } catch {
    // Không đọc được thì không kết luận là đang bị chặn.
    return false;
  }
}
const PROFILE_URL = "https://opensea.io/profile";

/** Injected, so this module never reaches for a key itself. */
let addressProvider = () => null;

function configure({ addressProvider: provider } = {}) {
  if (typeof provider === "function") addressProvider = provider;
}

/**
 * What the profile page says about the session.
 *
 * Read as page text rather than from the title: the title is served from cache
 * and was observed naming two different accounts on consecutive loads, while
 * the disabled banner and the account address are rendered content.
 */
const PROBE = `(() => {
  const text = document.body ? document.body.innerText || "" : "";
  const lower = text.toLowerCase();
  const btn = el => (el.innerText || el.textContent || "").trim();
  const buttons = [...document.querySelectorAll('button,[role="button"]')]
    .filter(e => e.offsetParent !== null).map(btn);

  return {
    title: document.title || "",
    // Full address: only ever rendered in the disabled-account notice.
    address: (text.match(/0x[0-9a-fA-F]{40}/) || [null])[0],
    // How a healthy profile names the account: the first six hex digits.
    abbrev: (buttons.find(t => /^[0-9a-fA-F]{6}$/.test(t)) || null),
    // The account chrome only renders for a session that can actually be used.
    profileChrome: lower.includes("portfolio") && lower.includes("listings") &&
      lower.includes("offers"),
    disabled: lower.includes("account has been disabled") ||
      lower.includes("account is disabled"),
    connectPrompt: lower.includes("connect wallet"),
    genericTitle: (document.title || "").toLowerCase()
      .startsWith("opensea, exchange everything"),
    profileTitle: (document.title || "").toLowerCase().includes("profile | opensea"),
    length: text.length
  };
})()`;

/**
 * Poll until the page says something definite.
 *
 * A single sample after a fixed sleep is what produced a confident, wrong
 * "logged out" reading earlier: the probe caught the page before hydration.
 */
async function probe(page, { timeoutMs = 25000, intervalMs = 1000 } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const seen = await page.webContents.executeJavaScript(PROBE, true);
    if (seen.disabled || seen.address || seen.abbrev || seen.profileChrome ||
        seen.connectPrompt || seen.genericTitle) return seen;
    if (Date.now() >= deadline) return { ...seen, undecided: true };
    await new Promise(r => setTimeout(r, intervalMs));
  }
}

/**
 * CÓ COOKIE PHIÊN KHÔNG — KHÔNG PHẢI "CÓ COOKIE NÀO KHÔNG"
 *
 *   Một người chưa đăng nhập vẫn mang đầy cookie của opensea.io: đồng ý
 *   cookie, đo lường, thử nghiệm A/B. Đếm số cookie nên là một phép kiểm vô
 *   nghĩa — nó luôn đúng.
 *
 *   Thứ đáng kể là cookie MANG PHIÊN. Tên chính xác thuộc về OpenSea và có
 *   thể đổi, nên không ghim một tên: nhận theo hình dạng, và khi không chắc
 *   thì trả lời KHÔNG — câu trả lời "không" chỉ dẫn tới một lần đăng nhập
 *   lại, còn câu "có" sai thì chặn mất lần đăng nhập đầu tiên.
 */
const AUTH_COOKIE = /(^|[-_.])(session|auth|token|siwe|jwt|sid)([-_.]|$)/i;

async function hasAuthCookie() {
  try {
    const names = await browser.sessionCookies();
    return (names || []).some(n => AUTH_COOKIE.test(String(n)));
  } catch {
    return false;
  }
}

/** Read the session without changing it. */
async function readSession() {
  sessionStats.profileReads++;
  const walletAddress = String(addressProvider() || "").toLowerCase() || null;

  let seen;
  try {
    seen = await probe(await browser.goto(PROFILE_URL));
  } catch (error) {
    return { state: "signed-out", error: error.message, walletAddress, sessionAddress: null };
  }

  const shownFull = seen.address ? seen.address.toLowerCase() : null;
  const shownShort = seen.abbrev ? seen.abbrev.toLowerCase() : null;
  const base = {
    walletAddress,
    sessionAddress: shownFull || (shownShort ? `0x${shownShort}…` : null),
    title: seen.title
  };

  // Checked first: a disabled account still renders a profile title and a full
  // address, so any later test would call it healthy.
  if (seen.disabled) return { ...base, state: "account-disabled" };
  if (seen.connectPrompt || seen.genericTitle) return { ...base, state: "signed-out" };

  // Neither the account chrome nor any identifier: no usable session. Treated
  // as signed-out rather than assumed good, because the alternative is acting
  // on an account we cannot name.
  /**
   * CHƯA ĐỌC ĐƯỢC ≠ ĐÃ ĐĂNG XUẤT
   *
   *   Không thấy khung tài khoản và không thấy địa chỉ nào thì ta KHÔNG biết
   *   gì cả. Gán thẳng nhãn "signed-out" ở đây là biến "chưa biết" thành một
   *   kết luận, và `toUiState` dịch nó ra "SESSION HẾT HẠN" — câu mà khách
   *   hàng đang nhìn thấy trong Settings trong khi phiên vẫn còn tốt.
   *
   *   Có cookie phiên thì cán cân nghiêng hẳn: một phiên đã đăng xuất không
   *   giữ cookie. Lúc đó trạng thái đúng là "chưa xác minh được" — chờ, thử
   *   lại — chứ không phải "hết hạn", và tuyệt đối không phải cái cớ để đăng
   *   nhập lại.
   */
  if (!seen.profileChrome && !shownFull && !shownShort) {
    return { ...base, state: (await hasAuthCookie()) ? "unverified" : "signed-out", undecided: true };
  }

  // Compare on whichever form the page gave. The abbreviation is the first six
  // hex digits of the address, which is enough to catch a different account
  // while never rejecting the right one for want of a full string.
  if (walletAddress) {
    const wantFull = walletAddress;
    const wantShort = walletAddress.slice(2, 8);
    if (shownFull && shownFull !== wantFull) return { ...base, state: "wrong-wallet" };
    if (!shownFull && shownShort && shownShort !== wantShort) {
      return { ...base, state: "wrong-wallet" };
    }
  }

  return { ...base, state: "connected" };
}

/**
 * Make the session usable if it can be, and say plainly when it cannot.
 *
 * Auto-login is attempted for exactly one state: signed-out. A disabled account
 * and a wrong wallet are both left alone - logging in again would not change
 * either, and wallets are never switched on the user's behalf.
 */
async function ensureSession({ allowLogin = true } = {}) {
  const first = await readSession();
  if (first.state === "connected") return first;
  if (first.state === "account-disabled" || first.state === "wrong-wallet") return first;
  if (!allowLogin) return first;
  if (!first.walletAddress) return { ...first, state: "login-required" };

  logger.send(`${TAG} phiên hết hạn — thử đăng nhập lại tự động`);
  let attempt = null;
  try {
    attempt = await browser.autoLogin();
  } catch (error) {
    return { ...first, state: "login-failed", error: error.message };
  }

  const after = await readSession();
  if (after.state === "connected") {
    logger.send(`${TAG} đăng nhập lại tự động THÀNH CÔNG`);
    return after;
  }
  if (after.state === "account-disabled" || after.state === "wrong-wallet") return after;

  return {
    ...after,
    state: "login-failed",
    error: (attempt && attempt.error) || "Không tạo được phiên OpenSea."
  };
}

/** One sentence per state, so the UI never has to invent one. */
function describe(state) {
  switch (state && state.state) {
    case "connected":
      return "Đã kết nối OpenSea.";
    case "signed-out":
      return "Chưa kết nối OpenSea. App đang thử kết nối lại bằng ví trong Settings.";
    case "account-disabled":
      return (
        "Tài khoản OpenSea của phiên này đang bị vô hiệu hoá, nên trang offer " +
        "không hiển thị order nào. Không phải lỗi bộ lọc collection. " +
        "Đổi sang Private Key của ví khác trong Settings để dùng tài khoản khác."
      );
    case "wrong-wallet":
      return (
        "Phiên OpenSea đang là ví khác với Private Key hiện tại. " +
        "App sẽ tự kết nối lại bằng đúng ví."
      );
    case "login-required":
      return "Chưa có Private Key trong Settings nên không thể tự đăng nhập.";
    case "login-failed":
      return "Không tạo được phiên OpenSea. Kiểm tra mạng và Private Key trong Settings.";
    default:
      return "Không xác định được trạng thái phiên OpenSea.";
  }
}

/** Everything but `connected` means no scan, no signature, no cancel. */
function isUsable(state) {
  return !!state && state.state === "connected";
}

/**
 * What the UI shows, and what the rest of the app asks before acting.
 *
 * `wallet` is the address derived from the Private Key in Settings; `session`
 * is the address OpenSea is actually signed in as. They are separate on
 * purpose: when they disagree, nothing may run.
 */
const status = {
  state: "initializing",
  wallet: null,
  session: null,
  message: "Đang khởi tạo phiên OpenSea...",
  at: Date.now()
};

const listeners = new Set();

function onStatus(fn) {
  if (typeof fn === "function") listeners.add(fn);
  return () => listeners.delete(fn);
}

function setStatus(next) {
  Object.assign(status, next, { at: Date.now() });
  for (const fn of listeners) {
    try { fn({ ...status }); } catch { /* a listener must not break the session */ }
  }
  return { ...status };
}

function getStatus() {
  return { ...status };
}

/** Short form for the UI. The full address is public but unreadable at a glance. */
function shortWallet(address) {
  const a = String(address || "");
  return a.length > 12 ? `${a.slice(0, 8)}…${a.slice(-4)}` : a;
}

const STATE_TEXT = {
  initializing: "ĐANG KHỞI TẠO",
  connecting: "ĐANG KẾT NỐI",
  connected: "ĐÃ KẾT NỐI",
  "not-connected": "CHƯA KẾT NỐI",
  expired: "SESSION HẾT HẠN",
  // Đã đăng nhập nhưng chưa đọc được ví của phiên. KHÔNG phải lỗi ví.
  unverified: "CHƯA XÁC MINH ĐƯỢC VÍ",
  error: "LỖI KẾT NỐI"
};

/** Map a readSession()/ensureSession() result onto a UI state. */
function toUiState(result) {
  switch (result && result.state) {
    case "connected": return "connected";
    case "signed-out": return "expired";
    case "login-required": return "not-connected";
    // Chưa đọc được ví KHÔNG phải "error": không có gì hỏng, chỉ là chưa
    // biết. Gộp nó vào error làm giao diện báo hỏng cho một tình trạng sẽ
    // tự khỏi sau vài giây.
    case "unverified": return "unverified";
    case "wrong-wallet":
    case "account-disabled":
    case "login-failed": return "error";
    default: return "error";
  }
}

/**
 * Bring the session up for a specific wallet, and say so as it happens.
 *
 * Reconnecting is decided by the ADDRESS, never by the key: saving the same key
 * again, or saving an API key alongside it, must not tear down a working
 * session. A different address always must.
 */
let inFlight = null;
/** Lần gọi `connectFor` gần nhất, để không thử lại dồn dập. */
let lastAttempt = { address: null, at: 0 };
/** Khoảng nguội giữa hai lần thử kết nối cho CÙNG một ví. */
const RECONNECT_COOLDOWN_MS = 30000;
/**
 * PHIÊN "CHƯA XÁC MINH" DÙNG LẠI 10 PHÚT (1.25.0)
 *
 *   unverified = cookie phiên còn, chưa đọc được ví, KHÔNG có bằng chứng ví
 *   khác. Trước đây mỗi thao tác sau 30s nguội lại nạp /profile (tới 25s dò
 *   + có thể SIWE). Cùng ví + cookie còn → dùng lại trạng thái trong 10 phút.
 *   Đổi ví (force) hoặc mất cookie thì kiểm lại ngay. wrong-wallet vẫn chặn.
 */
const UNVERIFIED_TTL_MS = 10 * 60 * 1000;
/** Bộ đếm cho chẩn đoán: bao nhiêu lần thật sự nạp /profile. */
const sessionStats = { profileReads: 0, connectAttempts: 0, reusedUnverified: 0 };

/**
 * Bring the session to the configured wallet, whatever state it is in now.
 *
 * Each branch is a different amount of work, and the cheapest one that is
 * correct wins: a session already on the right wallet is reused untouched, so
 * opening the app does not cost a signature.
 */
async function resolveFor(address) {
  logger.send(`${TAG} configured wallet = ${shortWallet(address)}`);

  let seen = await readSession();
  logger.send(
    `${TAG} existing session wallet = ${seen.sessionAddress ? shortWallet(seen.sessionAddress) : "(none)"}`
  );
  logger.send(`${TAG} session valid = ${seen.state === "connected" || seen.state === "wrong-wallet"}`);

  // A disabled account cannot be signed into, and clearing it would only lose
  // the evidence of why.
  if (seen.state === "account-disabled") {
    logger.error(`${TAG} ERROR · account disabled`);
    return setStatus({
      state: "error", wallet: address, session: seen.sessionAddress || null,
      message: describe(seen)
    });
  }

  const sessionWallet = seen.sessionAddress ? String(seen.sessionAddress).toLowerCase() : null;
  // The page may show the address abbreviated; compare on what it gave.
  const matches = sessionWallet
    ? (sessionWallet.startsWith("0x") && sessionWallet.length === 42
        ? sessionWallet === address
        : address.startsWith(sessionWallet.replace(/…$/, "")))
    : false;
  logger.send(`${TAG} wallet match = ${matches}`);

  // 1. Already right. Nothing to sign.
  if (seen.state === "connected" && matches) {
    logger.send(`${TAG} existing session valid`);
    logger.send(`${TAG} wallet matched · ${shortWallet(address)}`);
    logger.send(`${TAG} connected`);
    return setStatus({
      state: "connected", wallet: address, session: address,
      message: "Đã kết nối OpenSea."
    });
  }

  /**
   * 1b. PHIÊN CÒN SỐNG NHƯNG CHƯA ĐỌC ĐƯỢC VÍ — KHÔNG ĐĂNG NHẬP LẠI
   *
   *   `matches` là `false` cho CẢ HAI trường hợp: ví khác, và không đọc được
   *   ví nào. Nhánh dưới đây trước kia nhận luôn cả hai, in "session expired"
   *   rồi chạy `autoLogin` — trên một phiên đang hoàn toàn tốt. Mỗi lần như
   *   thế là một lần nạp lại `/profile` và một vòng SIWE, tức là thêm áp lực
   *   lên đúng thứ đang bị OpenSea chặn tốc độ; và vì nó không bao giờ đọc
   *   được địa chỉ nên nó lặp lại mãi.
   *
   *   Không có bằng chứng về một ví KHÁC thì không có lý do gì để phá phiên
   *   hiện tại. Nói thẳng ra là "chưa xác minh được", giữ nguyên phiên, và
   *   để lần kiểm sau trả lời — trang rảnh hơn là đọc được ngay.
   */
  if (!sessionWallet && (seen.state === "connected" || seen.state === "unverified") &&
      await hasAuthCookie()) {
    const limited = openSeaIsThrottling();
    logger.send(`${TAG} phiên còn sống nhưng trang chưa hiện ví` +
      (limited ? " · OpenSea đang giới hạn tốc độ" : "") + " — GIỮ phiên, không đăng nhập lại");
    return setStatus({
      state: "unverified", wallet: address, session: null, rateLimited: limited,
      message: limited
        ? "OpenSea đang giới hạn tốc độ nên chưa đọc được ví của phiên. " +
          "Đây KHÔNG phải lỗi Private Key — phiên vẫn dùng được."
        : "Chưa đọc được ví của phiên; phiên vẫn dùng được. Sẽ tự xác minh lại."
    });
  }

  // 2. Signed in as somebody else. That session has to go before the next one
  //    can be built, or OpenSea simply hands the old one back.
  if (sessionWallet && !matches) {
    logger.send(`${TAG} wallet mismatch`);
    logger.send(`${TAG} expected=${shortWallet(address)}`);
    logger.send(`${TAG} session=${shortWallet(sessionWallet)}`);
    logger.send(`${TAG} clearing old OpenSea session`);
    try {
      await browser.clearSession();
      logger.send(`${TAG} old session cleared`);
    } catch (error) {
      logger.error(`${TAG} không xoá được phiên cũ: ${error.message}`);
    }
  } else {
    logger.send(`${TAG} session expired`);
    logger.send(`${TAG} refreshing session`);
  }

  // 3. Sign in as the configured wallet.
  logger.send(`${TAG} login start · wallet=${shortWallet(address)}`);
  logger.send(`${TAG} signing in with configured wallet`);
  let attempt = null;
  try {
    // browser.autoLogin drives OpenSea own connect + SIWE flow in the page, on
    // the same partition the session lives in. It reports each stage; nothing
    // here reimplements the handshake over HTTP.
    attempt = await browser.autoLogin();
    logger.send(`${TAG} SIWE signature = ${attempt && attempt.ok ? "OK" : "SENT"}`);
    logger.send(`${TAG} session cookie = ${attempt && attempt.cookies ? "PRESENT" : "MISSING"}`);
  } catch (error) {
    logger.error(`${TAG} ERROR · ${error.message}`);
    return setStatus({
      state: "error", wallet: address, session: null,
      message: `Không đăng nhập được OpenSea: ${error.message}`
    });
  }

  // 4. Verify. A sign-in that reports success but lands on another account is
  //    the one outcome that must never be called connected.
  seen = await readSession();
  // The successful SIWE driver is a stronger source than a profile DOM which
  // may still be hydrating. UNKNOWN is therefore unverified, never evidence
  // of a wallet mismatch.
  const after = seen.sessionAddress
    ? String(seen.sessionAddress).toLowerCase()
    : (attempt && attempt.address ? String(attempt.address).toLowerCase() : null);
  const verified = after
    ? (after.startsWith("0x") && after.length === 42
        ? after === address
        : address.startsWith(after.replace(/…$/, "")))
    : false;

  logger.send(`${TAG} verify = ${seen.state === "connected" ? "OK" : "FAIL"} · state=${seen.state}`);
  logger.send(`${TAG} authenticated wallet = ${after ? shortWallet(after) : "UNKNOWN"}`);

  if (seen.state === "connected" && verified) {
    logger.send(`${TAG} SIWE login success`);
    logger.send(`${TAG} authenticated wallet = ${shortWallet(address)}`);
    logger.send(`${TAG} wallet verification = PASS`);
    logger.send(`${TAG} CONNECTED`);
    return setStatus({
      state: "connected", wallet: address, session: address,
      message: "Đã kết nối OpenSea."
    });
  }

  /**
   * HAI CHUYỆN KHÁC HẲN NHAU, TỪNG BỊ GỘP LÀM MỘT.
   *
   *   `after` CÓ giá trị và khác  → phiên đúng là của ví khác. Người dùng cần
   *                                 kiểm tra Private Key. Câu cũ đúng ở đây.
   *
   *   `after` là NULL             → trang có khung tài khoản nhưng KHÔNG hiện
   *                                 địa chỉ nào. Ta không biết phiên thuộc về
   *                                 ai. Đây không phải bằng chứng ví sai.
   *
   * Trường hợp thứ hai từng nhận đúng câu của trường hợp thứ nhất, và nhật ký
   * ghi `session=(unknown)` ngay bên dưới — báo cáo thật của khách hàng
   * "Khoa pap" là đúng cảnh này, kèm 429 của OpenSea trong cùng nhật ký.
   *
   * OpenSea đang chặn tốc độ thì phần đầu trang không tải được địa chỉ, nên
   * "không đọc được ví" gần như luôn là chuyện tốc độ, không phải chuyện ví.
   * Bảo người ta đi sửa Private Key khi khoá nằm ở chỗ khác là gửi họ đi sai
   * đường — và nếu họ nghe theo mà đổi key, họ mất cả ví đang đúng.
   */
  if (seen.state === "connected" && !verified) {
    if (after) {
      logger.error(`${TAG} wallet verification = FAIL`);
      logger.error(`${TAG} session=${shortWallet(after)}`);
      return setStatus({
        state: "wrong-wallet", wallet: address, session: after,
        message: "Đăng nhập xong nhưng phiên vẫn là ví khác. Kiểm tra Private Key."
      });
    }

    const limited = openSeaIsThrottling();
    logger.error(`${TAG} không đọc được ví của phiên (trang không hiện địa chỉ)`);
    logger.error(`${TAG} session=(chưa đọc được)` +
      (limited ? ` · OpenSea đang giới hạn tốc độ` : ""));

    return setStatus({
      state: "unverified",
      wallet: address,
      session: null,
      rateLimited: limited,
      message: limited
        ? "OpenSea đang giới hạn tốc độ nên chưa đọc được ví của phiên. " +
          "Đây KHÔNG phải lỗi Private Key — chờ một lát rồi thử lại."
        : "Đã đăng nhập nhưng chưa đọc được ví của phiên. " +
          "Chưa kết luận được là ví nào; hãy thử lại sau giây lát."
    });
  }

  logger.error(`${TAG} ERROR · wallet=${shortWallet(address)}`);
  logger.error(`${TAG} reason=${seen.state}`);
  return setStatus({
    state: toUiState(seen), wallet: address, session: after,
    message: (attempt && attempt.error) || describe(seen)
  });
}

async function connectFor(wallet, { force = false } = {}) {
  const address = String(wallet || "").toLowerCase() || null;

  if (!address) {
    logger.send(`${TAG} không có Private Key — không thể kết nối`);
    return setStatus({
      state: "not-connected", wallet: null, session: null,
      message: "Chưa có Private Key trong Settings."
    });
  }

  /**
   * MỘT LẦN THỬ MỖI 30 GIÂY, KHÔNG PHẢI MỘT LẦN MỖI CÚ BẤM
   *
   *   Mọi cổng gọi `connectFor` khi trạng thái chưa phải "connected", và mỗi
   *   lượt ấy nạp lại `/profile`, dò tới 25 giây, rồi có thể chạy cả một vòng
   *   SIWE tới 18 giây nữa. Bấm ĐỌC LINK vài lần trong lúc OpenSea đang chặn
   *   tốc độ là tự dựng một trận bão đăng nhập lên đúng trang đang bị chặn —
   *   và mỗi lượt lại làm lượt sau khó thành công hơn.
   *
   *   `inFlight` chỉ gộp những lời gọi ĐỒNG THỜI; chỗ này chặn những lời gọi
   *   nối đuôi nhau. `force` (người dùng đổi ví) luôn đi qua.
   */
  if (!force && lastAttempt.address === address &&
      Date.now() - lastAttempt.at < RECONNECT_COOLDOWN_MS &&
      status.state !== "connected") {
    logger.send(`${TAG} vừa thử kết nối ${Math.round((Date.now() - lastAttempt.at) / 1000)}s trước — chờ hết nguội`);
    return getStatus();
  }

  if (!force && status.state === "unverified" && status.wallet === address &&
      Date.now() - lastAttempt.at < UNVERIFIED_TTL_MS && await hasAuthCookie()) {
    sessionStats.reusedUnverified++;
    return getStatus();
  }

  // Already connected as this wallet, and nobody asked for a fresh one.
  if (!force && status.state === "connected" && status.wallet === address) {
    return getStatus();
  }

  // One connect at a time; a second request joins the first.
  if (inFlight) return inFlight;

  setStatus({
    state: "connecting", wallet: address, session: null,
    message: `Đang kết nối OpenSea · ví ${shortWallet(address)}`
  });
  logger.send(`${TAG} connecting OpenSea · wallet=${shortWallet(address)}`);

  lastAttempt = { address, at: Date.now() };
  sessionStats.connectAttempts++;
  inFlight = (async () => {
    try {
      return await resolveFor(address);
    } finally {
      inFlight = null;
      // Mốc nguội tính từ lúc KẾT THÚC: một lượt dò 25 giây không được ăn
      // mất phần lớn quãng nguội của lượt sau.
      lastAttempt = { address, at: Date.now() };
    }
  })();

  return inFlight;
}

/**
 * React to the Private Key in Settings changing.
 *
 * Same address means the key was re-saved or something else was saved beside
 * it, and a working session must survive that. A different address means the
 * old session belongs to an account the app is no longer configured for, so it
 * is dropped before the new one is built.
 */
async function walletChanged(nextWallet) {
  const address = String(nextWallet || "").toLowerCase() || null;
  const previous = status.wallet;

  if (previous && address && previous === address) {
    logger.send(`${TAG} private key saved · wallet unchanged (${shortWallet(address)})`);
    return getStatus();
  }

  logger.send(`${TAG} private key changed`);
  logger.send(`${TAG} old wallet = ${previous ? shortWallet(previous) : "(none)"}`);
  logger.send(`${TAG} new wallet = ${address ? shortWallet(address) : "(none)"}`);

  if (previous) {
    logger.send(`${TAG} invalidating old session`);
    try {
      await browser.clearSession();
    } catch (error) {
      logger.error(`${TAG} không xoá được phiên cũ: ${error.message}`);
    }
  }

  setStatus({ state: "connecting", wallet: address, session: null,
    message: address ? `Đang kết nối OpenSea · ví ${shortWallet(address)}` : "Chưa có Private Key." });

  return connectFor(address, { force: true });
}

/**
 * The guard a job calls before it acts.
 *
 * A session for a different wallet than the configured key is refused rather
 * than used: running under the previous account is the one outcome that cannot
 * be undone afterwards.
 */
async function requireWallet(wallet) {
  const address = String(wallet || "").toLowerCase() || null;
  if (!address) {
    return { ok: false, state: "not-connected", message: "Chưa có Private Key trong Settings." };
  }

  if (status.state !== "connected" || status.wallet !== address) {
    await connectFor(address);
  }

  /**
   * "CHƯA XÁC MINH ĐƯỢC VÍ" KHÔNG PHẢI LÝ DO ĐỂ KHOÁ OFFER SLL
   *
   *   Cổng này tồn tại để chặn đúng MỘT chuyện: làm việc trên phiên của một
   *   ví khác. `unverified` không nói điều đó — nó nói ta chưa đọc được ví
   *   nào, thường vì OpenSea đang chặn tốc độ. Khoá tính năng vì lý do đó là
   *   biến một sự cố tạm thời của trang thành một app không dùng được, và
   *   đúng cảnh khách hàng đang gặp: Settings báo "Chưa kết nối OpenSea" còn
   *   Offer SLL thì không bấm được.
   *
   *   Phiên vẫn còn cookie và không có bằng chứng về một ví khác thì cho đi
   *   tiếp. Nếu quả thật sai ví, phép kiểm ngay bên dưới — và chính OpenSea
   *   khi từ chối thao tác — vẫn chặn.
   */
  if (status.state === "unverified") {
    logger.send(`${TAG} chưa xác minh được ví nhưng phiên còn sống — cho phép dùng`);
    return { ok: true, state: "unverified", wallet: address, session: null, unverified: true };
  }

  if (status.state !== "connected") {
    return { ok: false, state: status.state, message: status.message, wallet: address };
  }

  // The session's own address, when OpenSea told us one, must be this wallet.
  if (status.session && status.session !== address) {
    logger.error(`${TAG} session wallet không khớp private key`);
    return {
      ok: false, state: "wrong-wallet", wallet: address, session: status.session,
      message: "Phiên OpenSea đang là ví khác với Private Key hiện tại."
    };
  }

  return { ok: true, state: "connected", wallet: address, session: status.session };
}

module.exports = {
  configure, readSession, ensureSession, describe, isUsable, PROFILE_URL,
  connectFor, walletChanged, requireWallet,
  getStatus, onStatus, shortWallet, STATE_TEXT,
  stats: () => ({ ...sessionStats })
};
