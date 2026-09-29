/**
 * browser.js — a real Chromium session on opensea.io.
 *
 * Everything that needs OpenSea to treat us as the logged-in user goes through
 * here. The window is genuine Chromium with a persistent profile: the user logs
 * in themselves, once, and the session survives restarts.
 *
 * Nothing here forges a fingerprint or defeats a challenge. If OpenSea asks for
 * a human, the window is shown so the user can answer it.
 *
 * The private key never reaches the page. Signing requests raised by the page
 * are forwarded to the main process, which signs and returns only a signature.
 */

const { BrowserWindow, session, shell } = require("electron");
const path = require("path");
const { logger } = require("./logger");

const PARTITION = "persist:opensea";
const ORIGIN = "https://opensea.io";
const TAG = "[BROWSER]";

let workerWindow = null;   // hidden, carries requests
let loginWindow = null;    // visible, only for logging in

/**
 * Resolves when the window has finished loading a page.
 *
 * Always bounded. A single-page app can swallow `did-finish-load` on a repeat
 * navigation, and an unbounded wait there hangs the whole login with no output
 * at all - which is exactly how this failed the first time.
 */
function onceLoaded(win, timeout = 30000) {
  return new Promise(resolve => {
    let settled = false;

    const finish = reason => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      win.webContents.off("did-finish-load", ok);
      win.webContents.off("did-fail-load", fail);
      resolve(reason);
    };

    const ok = () => finish("loaded");
    const fail = (_e, code, desc) => finish(`failed(${code}) ${desc}`);
    const timer = setTimeout(() => finish("timeout"), timeout);

    win.webContents.once("did-finish-load", ok);
    win.webContents.once("did-fail-load", fail);
  });
}

/**
 * Run JS in the page, but never wait forever.
 *
 * `executeJavaScript` silently never settles if the page navigates while the
 * script is in flight - and OpenSea redirects itself constantly. Every call
 * goes through here so a redirect costs a timeout, not a hung login.
 */
function runInPage(win, code, timeout = 20000) {
  return Promise.race([
    win.webContents.executeJavaScript(code, true),
    new Promise(resolve => setTimeout(() => resolve(undefined), timeout))
  ]);
}

/**
 * Wait for a condition inside the page instead of sleeping.
 * `expression` is JS evaluated in the page; it should return truthy when ready.
 */
async function waitForFunction(win, expression, { timeout = 20000, interval = 250 } = {}) {
  const deadline = Date.now() + timeout;

  for (;;) {
    let value = null;
    try {
      value = await runInPage(win, `(() => (${expression}))()`, Math.min(timeout, 8000));
    } catch {
      /* page may be mid-navigation; try again */
    }
    if (value) return value;
    if (Date.now() > deadline) {
      throw new Error(`waitForFunction timed out after ${timeout}ms: ${expression.slice(0, 80)}`);
    }
    await new Promise(r => setTimeout(r, interval));
  }
}

/** Wait for the SPA to stop mutating the DOM. */
function waitForSettle(win, { timeout = 20000 } = {}) {
  return runInPage(
    win,
    `new Promise(resolve => {
      let last = -1, stable = 0;
      const started = Date.now();
      const tick = () => {
        const n = document.getElementsByTagName("*").length;
        if (n === last) stable++; else { stable = 0; last = n; }
        if (stable >= 4 || Date.now() - started > ${timeout}) {
          return resolve({ nodes: n, ms: Date.now() - started });
        }
        setTimeout(tick, 300);
      };
      tick();
    })`,
    timeout + 5000
  );
}

// ---------------------------------------------------------------------------
// Windows
// ---------------------------------------------------------------------------

/**
 * EIP-6963 wallet discovery, announced from the page's own world.
 *
 * It must run here rather than in the preload: an event detail created in the
 * isolated world arrives at the page empty, so the wallet would be invisible to
 * anything that discovers wallets this way — which OpenSea does.
 */
const ANNOUNCE_WALLET = `(() => {
  if (window.__osbWalletAnnounced) return "already";
  if (typeof window.ethereum === "undefined") return "no provider";

  const info = Object.freeze({
    uuid: "6f1a2c94-6b3e-4c1a-9c2b-0f3a5d8e7b41",
    name: "OpenSea Offer Bot",
    icon: "data:image/svg+xml;base64," + btoa(
      '<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32">' +
      '<rect width="32" height="32" rx="8" fill="#2081e2"/></svg>'
    ),
    rdns: "com.openseaofferbot.wallet"
  });

  const announce = () => window.dispatchEvent(
    new CustomEvent("eip6963:announceProvider", {
      detail: Object.freeze({ info, provider: window.ethereum })
    })
  );

  window.addEventListener("eip6963:requestProvider", announce);
  announce();

  window.__osbWalletAnnounced = true;
  return "announced";
})()`;

/** Announce as early as the document allows, and again once it has loaded. */
function wireWalletDiscovery(win) {
  const announce = async () => {
    try {
      await runInPage(win, ANNOUNCE_WALLET, 8000);
    } catch {
      /* navigating; the next event will catch it */
    }
  };
  win.webContents.on("dom-ready", announce);
  win.webContents.on("did-finish-load", announce);
}

function makeWindow({ show }) {
  const win = new BrowserWindow({
    width: 1440,
    height: 950,
    show,
    webPreferences: {
      partition: PARTITION,
      contextIsolation: true,
      nodeIntegration: false,
      // The preload injects an EIP-1193 wallet. It needs `require("electron")`
      // for ipcRenderer, which a sandboxed preload does not get.
      sandbox: false,
      preload: path.join(__dirname, "wallet-preload.js")
    }
  });

  wireWalletDiscovery(win);
  return win;
}

/**
 * Route the page's wallet requests to the main process.
 * Registered once; main.js calls this during boot.
 */
function attachWallet(ipcMain, wallet) {
  ipcMain.handle("wallet:request", async (event, request) => {
    // The wallet signs without prompting, so it must only ever answer OpenSea.
    // A redirect or an injected iframe pointing elsewhere gets nothing.
    let origin = "";
    try {
      origin = new URL(event.senderFrame?.url || event.sender.getURL()).origin;
    } catch {
      origin = "";
    }

    if (origin !== ORIGIN) {
      logger.send(`${TAG} từ chối yêu cầu ví từ ${origin || "nguồn lạ"}`);
      return { error: { message: `Origin ${origin} không được phép dùng ví.`, code: 4100 } };
    }

    return wallet.handle(request);
  });

  logger.send(`${TAG} ví đã gắn vào phiên (chỉ ký cho ${ORIGIN})`);
}

/**
 * HIDDEN WORKER LIFECYCLE (1.25.0)
 *
 *   Still created LAZILY - never at boot. But it used to live until shutdown
 *   once created: a full OpenSea SPA rendering in the background 24/7 just to
 *   keep a cookie, which the persistent partition keeps anyway. Now the window
 *   is unloaded after WORKER_IDLE_MS without use, and never while a job holds
 *   it (Offer SLL / Cancel SLL / login) - see holdWorker(). The next use
 *   recreates it on the same partition, so the session survives.
 */
const WORKER_IDLE_MS = 5 * 60 * 1000;
let workerCreating = null;
let workerHolds = 0;
let workerLastUsedAt = 0;
let workerIdleTimer = null;
const workerStats = { created: 0, destroyedIdle: 0, navigations: 0, profileNavigations: 0, sessionFetches: 0 };

function touchWorker() {
  workerLastUsedAt = Date.now();
  if (workerIdleTimer) return;
  workerIdleTimer = setInterval(() => {
    if (!workerWindow || workerWindow.isDestroyed()) {
      clearInterval(workerIdleTimer); workerIdleTimer = null; return;
    }
    if (workerHolds > 0 || workerCreating) return;
    if (Date.now() - workerLastUsedAt < WORKER_IDLE_MS) return;
    try { workerWindow.destroy(); } catch { /* already gone */ }
    workerWindow = null;
    workerStats.destroyedIdle++;
    clearInterval(workerIdleTimer); workerIdleTimer = null;
    logger.send(`${TAG} trang OpenSea ẩn đã nghỉ sau ${Math.round(WORKER_IDLE_MS / 60000)} phút không dùng (phiên vẫn giữ)`);
  }, 30 * 1000);
  workerIdleTimer.unref?.();
}

/**
 * Keep the worker alive for the duration of a job. Returns an idempotent
 * release function; always call it in a finally.
 */
function holdWorker() {
  workerHolds++;
  touchWorker();
  let released = false;
  return () => {
    if (released) return;
    released = true;
    workerHolds = Math.max(0, workerHolds - 1);
    touchWorker();
  };
}

function workerStatus() {
  return {
    alive: Boolean(workerWindow && !workerWindow.isDestroyed()),
    holds: workerHolds,
    idleMs: workerLastUsedAt ? Date.now() - workerLastUsedAt : -1,
    ...workerStats
  };
}

/** The hidden window that carries requests. Created on demand (single-flight). */
async function getWorker() {
  touchWorker();
  if (workerWindow && !workerWindow.isDestroyed()) return workerWindow;
  if (workerCreating) return workerCreating;

  workerCreating = (async () => {
    const win = makeWindow({ show: false });
    workerWindow = win;
    workerStats.created++;
    win.on("closed", () => {
      if (workerWindow === win) workerWindow = null;
    });

    const loaded = onceLoaded(win);
    await win.loadURL(`${ORIGIN}/`);
    await loaded;

    logger.send(`${TAG} phiên sẵn sàng`);
    return win;
  })();
  try {
    return await workerCreating;
  } finally {
    workerCreating = null;
    touchWorker();
  }
}

/**
 * Show a real window for the user to log in with.
 * Resolves once the page reports a logged-in state, or when the user closes it.
 */
async function openLogin({ onStatus = null } = {}) {
  if (loginWindow && !loginWindow.isDestroyed()) {
    loginWindow.focus();
    return { ok: true, alreadyOpen: true };
  }

  loginWindow = makeWindow({ show: true });
  loginWindow.on("closed", () => {
    loginWindow = null;
  });

  // Wallet popups and help links should open in the user's own browser.
  loginWindow.webContents.setWindowOpenHandler(({ url }) => {
    if (!url.startsWith(ORIGIN)) {
      shell.openExternal(url);
      return { action: "deny" };
    }
    return { action: "allow" };
  });

  const loaded = onceLoaded(loginWindow);
  await loginWindow.loadURL(`${ORIGIN}/login`);
  await loaded;

  logger.send(`${TAG} cửa sổ đăng nhập đã mở — đăng nhập rồi đóng lại`);
  if (onStatus) onStatus({ phase: "opened" });

  return { ok: true };
}

function closeLogin() {
  if (loginWindow && !loginWindow.isDestroyed()) loginWindow.close();
  loginWindow = null;
}

// ---------------------------------------------------------------------------
// Session state
// ---------------------------------------------------------------------------

/** Cookies OpenSea sets once a session exists. */
async function sessionCookies() {
  const ses = session.fromPartition(PARTITION);
  const cookies = await ses.cookies.get({ domain: "opensea.io" });
  return cookies.map(c => c.name);
}

/**
 * Ask the page whether it is logged in.
 *
 * The worker window is reloaded first: the session lives in the partition, so a
 * window opened before the user signed in keeps showing the logged-out page and
 * would report a false negative forever.
 */
async function isLoggedIn({ reload = true } = {}) {
  try {
    let win = await getWorker();
    if (reload) win = await goto(`${ORIGIN}/profile`);

    // What a signed-in profile actually renders. The old check looked for an
    // 0x address in the title, but OpenSea titles the page with the account
    // NAME - so a working session reported as signed out.
    const state = await runInPage(
      win,
      `(() => {
        const body = document.body ? document.body.innerText || "" : "";
        const lower = body.toLowerCase();
        const vis = el => el.offsetParent !== null;
        const txt = el => (el.innerText || el.textContent || "").trim();
        const buttons = [...document.querySelectorAll('button,[role="button"]')]
          .filter(vis).map(txt);
        return {
          needsConnect: lower.includes("connect wallet"),
          needsSignIn: /(^|\\s)sign in(\\s|$)/i.test(body),
          disabled: lower.includes("account has been disabled"),
          // The account chrome only renders for a usable session.
          profileChrome: lower.includes("portfolio") && lower.includes("listings") &&
            lower.includes("offers"),
          // How a signed-in profile names the account: the first six hex digits.
          abbrev: buttons.find(t => /^[0-9a-fA-F]{6}$/.test(t)) || null,
          fullAddress: (body.match(/0x[0-9a-fA-F]{40}/) || [null])[0]
        };
      })()`,
      15000
    );

    const cookies = await sessionCookies();

    const signedIn = !!state &&
      !state.disabled &&
      !state.needsConnect &&
      (state.profileChrome || !!state.abbrev || !!state.fullAddress);

    return {
      loggedIn: signedIn,
      address: state && (state.fullAddress || (state.abbrev ? `0x${state.abbrev}` : null)),
      ...state,
      cookies: cookies.length
    };
  } catch (error) {
    return { loggedIn: false, error: error.message };
  }
}

const CLICK_BY_TEXT = pattern => `(() => {
  const vis = el => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const txt = el => (el.innerText || el.textContent || "").trim();
  const btn = [...document.querySelectorAll('button,[role="button"]')]
    .filter(vis)
    .find(el => ${pattern}.test(txt(el)));
  if (!btn) return { found: false };
  btn.click();
  return { found: true, label: txt(btn).slice(0, 40) };
})()`;

/**
 * Sign in without the user having to drive the modals.
 *
 * OpenSea needs two things and they are easy to confuse: connecting a wallet,
 * and then signing a SIWE message to actually establish a session. Opening the
 * login page alone leaves you connected but not signed in, which is why the
 * bulk path stayed switched off.
 */
async function autoLogin({ onStatus = null } = {}) {
  const step = (phase, detail) => {
    logger.send(`${TAG} ${phase}${detail ? ` · ${detail}` : ""}`);
    if (onStatus) onStatus({ phase, detail });
  };

  const win = await goto(`${ORIGIN}/profile`);

  const already = await isLoggedIn({ reload: false });
  if (already.loggedIn) {
    step("đã đăng nhập sẵn");
    return { ok: true, ...already };
  }

  // The page has to have rendered before anything can be clicked. Without this
  // wait the Connect button simply was not there yet, the connect step was
  // skipped, and the sign-in that followed signed for a wallet that had never
  // been connected.
  step("login start", `chờ trang OpenSea`);
  try {
    await waitForFunction(
      win,
      `(() => {
        const t = (document.body ? document.body.innerText || "" : "").toLowerCase();
        return t.includes("connect wallet") || t.includes("portfolio") || t.includes("sign in");
      })()`,
      { timeout: 30000, interval: 500 }
    );
  } catch {
    step("trang OpenSea không sẵn sàng");
  }

  // 1. Connect, picking our own injected wallet from the list.
  const connect = await runInPage(
      win,
      CLICK_BY_TEXT("/connect wallet/i"),
    15000
  );
  if (connect && connect.found) {
    step("mở hộp chọn ví");
    try {
      await waitForFunction(
        win,
        `(() => /opensea offer bot/i.test(document.body ? document.body.innerText || "" : ""))()`,
        { timeout: 20000, interval: 400 }
      );
    } catch {
      step("danh sách ví chưa hiện");
    }

    const pick = await runInPage(
      win,
      CLICK_BY_TEXT("/opensea offer bot/i"),
      15000
    );
    if (!pick || !pick.found) {
      return {
        ok: false,
        error: "Không thấy ví 'OpenSea Offer Bot' trong danh sách. Kiểm tra Private Key trong Settings."
      };
    }
    step("đã chọn ví", pick.label);
    await waitForSettle(win, { timeout: 20000 });
  }

  // 2. Sign the SIWE message. This is the step that creates the session.
  try {
    await waitForFunction(
      win,
      `(() => {
        const vis = el => el.offsetParent !== null;
        const txt = el => (el.innerText || el.textContent || "").trim();
        return [...document.querySelectorAll('button,[role="button"]')]
          .filter(vis).some(b => /^sign in$/i.test(txt(b))) ? true : null;
      })()`,
      { timeout: 20000, interval: 400 }
    );
  } catch {
    step("không thấy nút Sign in");
  }

  const signIn = await runInPage(
      win,
      CLICK_BY_TEXT("/^sign in$/i"),
    15000
  );
  if (!signIn || !signIn.found) step("SIWE nút Sign in = MISSING");
  if (signIn && signIn.found) {
    step("ký message đăng nhập");
    try {
      await waitForFunction(
        win,
        `!/\\bsign in\\b|connect wallet/i.test(document.body.innerText || "")`,
        { timeout: 30000, interval: 500 }
      );
    } catch {
      /* fall through to the status check, which is authoritative */
    }
    await waitForSettle(win, { timeout: 15000 });
  }

  // A session appears a moment after the signature is accepted, so this waits
  // for it instead of sampling once and calling it failed.
  let status = await isLoggedIn({ reload: true });
  for (let i = 0; i < 12 && !status.loggedIn; i++) {
    await new Promise(r => setTimeout(r, 1500));
    status = await isLoggedIn({ reload: false });
  }

  const cookies = await sessionCookies();
  step(`session cookie = ${cookies.length ? "PRESENT" : "MISSING"}`);

  /**
   * VÍ ĐÃ KÝ SIWE LÀ VÍ CỦA PHIÊN — KHÔNG CẦN ĐỌC LẠI TỪ MÀN HÌNH
   *
   *   Chỗ này từng chỉ đọc địa chỉ từ DOM và ghi "authenticated wallet =
   *   UNKNOWN" khi trang không kịp hiện phần đầu — chuyện gần như luôn xảy ra
   *   khi OpenSea đang chặn tốc độ. Rồi "không đọc được ví" bị hiểu thành
   *   "sai ví" và app đăng nhập lại, làm trang càng bị chặn hơn.
   *
   *   Nhưng chính app vừa BƠM khoá vào trang và chính khoá đó vừa ký câu
   *   SIWE mà OpenSea chấp nhận. Phiên vừa tạo thuộc về ví đó — đấy là suy
   *   luận từ chữ ký, không phải phỏng đoán từ giao diện. DOM chỉ còn là
   *   nguồn thứ hai, dùng khi nó nói được.
   */
  let signer = null;
  if (status.loggedIn && !status.address) {
    try {
      // Hỏi chính cái provider mà app bơm vào trang. Không phải DOM, không
      // phải phỏng đoán: đây là ví đã ký câu SIWE mà OpenSea vừa chấp nhận.
      const win = await getWorker();
      signer = await runInPage(
        win,
        `(async () => {
          if (typeof window.ethereum === "undefined") return null;
          try {
            const a = await window.ethereum.request({ method: "eth_accounts" });
            return Array.isArray(a) && a[0] ? String(a[0]) : null;
          } catch { return null; }
        })()`,
        8000
      );
    } catch { signer = null; }
    if (signer) {
      status = { ...status, address: signer, addressSource: "siwe" };
      step("ví của phiên lấy từ ví đã ký SIWE (trang chưa hiện địa chỉ)");
    }
  }
  step(`authenticated wallet = ${status.address || "UNKNOWN"}`);
  step(status.loggedIn ? "đăng nhập THÀNH CÔNG" : "vẫn chưa có phiên");

  return {
    ok: status.loggedIn,
    ...status,
    error: status.loggedIn
      ? null
      : "Chưa tạo được phiên. Mở cửa sổ OpenSea và đăng nhập thủ công."
  };
}

// ---------------------------------------------------------------------------
// Requests carried by the session
// ---------------------------------------------------------------------------

/**
 * Run a fetch from inside the opensea.io page.
 *
 * Because it executes in the page, it is same-origin and carries the user's
 * cookies exactly as a click would. Returns a plain object shaped like a fetch
 * Response so callers can treat it as one.
 */
async function sessionFetch(url, init = {}) {
  const win = await getWorker();
  workerStats.sessionFetches++;

  const payload = JSON.stringify({
    url,
    method: init.method || "GET",
    headers: init.headers || {},
    body: init.body || null
  });

  const result = await runInPage(
      win,
      `(async () => {
      const req = ${payload};
      try {
        const res = await fetch(req.url, {
          method: req.method,
          headers: req.headers,
          body: req.body,
          credentials: "include"
        });
        const text = await res.text();
        const headers = {};
        res.headers.forEach((v, k) => { headers[k] = v; });
        return { ok: true, status: res.status, headers, text };
      } catch (e) {
        return { ok: false, error: String(e && e.message || e) };
      }
    })()`,
    15000
  );

  // runInPage RESOLVES undefined on timeout rather than rejecting, so without
  // this the next line reads .ok off undefined and the real cause - a page
  // request that never came back - is lost behind a TypeError.
  if (!result) {
    throw new Error("session fetch timed out: trang OpenSea không trả lời trong 15s");
  }
  if (!result.ok) throw new Error(`session fetch failed: ${result.error}`);

  return {
    status: result.status,
    ok: result.status >= 200 && result.status < 300,
    headers: {
      get: name => result.headers[String(name).toLowerCase()] ?? null,
      forEach: fn => {
        for (const [k, v] of Object.entries(result.headers)) fn(v, k);
      }
    },
    text: async () => result.text,
    json: async () => JSON.parse(result.text)
  };
}

/** Navigate the worker window and wait for it to settle. */
async function goto(url, { settle = true, timeout = 30000 } = {}) {
  const win = await getWorker();
  workerStats.navigations++;
  if (/\/profile(\b|$|\?)/.test(String(url))) workerStats.profileNavigations++;
  const loaded = onceLoaded(win, timeout);

  try {
    await win.loadURL(url);
  } catch (error) {
    // loadURL rejects on an aborted navigation, which OpenSea does to itself
    // during redirects. The load listener still tells us where we ended up.
    logger.send(`${TAG} điều hướng bị ngắt: ${error.message}`);
  }

  await loaded;
  if (settle) await waitForSettle(win);
  return win;
}

function shutdown() {
  closeLogin();
  if (workerIdleTimer) { clearInterval(workerIdleTimer); workerIdleTimer = null; }
  if (workerWindow && !workerWindow.isDestroyed()) workerWindow.destroy();
  workerWindow = null;
}

/**
 * Drop the OpenSea session entirely.
 *
 * Used when the configured wallet changes: the cookies belong to the previous
 * account, and reusing them would sign the app in as a wallet the user is no
 * longer configured for. The worker window goes with them so the next
 * navigation starts clean.
 */
async function clearSession() {
  try {
    if (workerWindow && !workerWindow.isDestroyed()) {
      workerWindow.destroy();
    }
  } catch {
    /* already gone */
  }
  workerWindow = null;

  await session.fromPartition(PARTITION).clearStorageData({
    storages: ["cookies", "localstorage", "indexdb", "websql", "serviceworkers", "cachestorage"]
  });
  logger.send(`${TAG} đã xoá phiên OpenSea`);
}

module.exports = {
  PARTITION,
  clearSession,
  ORIGIN,
  attachWallet,
  getWorker,
  holdWorker,
  workerStatus,
  openLogin,
  closeLogin,
  autoLogin,
  isLoggedIn,
  sessionCookies,
  sessionFetch,
  goto,
  waitForFunction,
  waitForSettle,
  shutdown
};
