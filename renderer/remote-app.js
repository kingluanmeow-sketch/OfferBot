"use strict";
/**
 * OfferBot Remote (Mac) — CHÍNH renderer của Windows, chạy trên trình duyệt.
 *
 *   Trang này nạp styles.css + dashboard-component + cancel + bulk-offer y hệt
 *   index.html của Windows. Khác biệt duy nhất là `window.botAPI`: ở Windows
 *   nó là preload (IPC), ở đây nó là shim gửi `{action:"ipc", channel, payload}`
 *   qua WebSocket tới remote-host, chạy CÙNG handler mà cửa sổ Windows gọi.
 *   Trạng thái engine tới theo khung `state` (≤ 1/giây), sự kiện (tiến trình
 *   SLL/Cancel, log, licence) tới theo khung `event` ngay lập tức.
 *
 *   Kết nối: cookie phiên, snapshot khi nối, ping/pong, không thấy host 45s →
 *   nối lại với lùi luỹ thừa + jitter; Mac thức / đổi mạng → nối lại ngay.
 *   Lệnh: requestId ngẫu nhiên, chờ ACK; chưa ACK thì gửi lại (host idempotent
 *   theo requestId); đứt WS thì POST /api/command cùng requestId. Lệnh dài
 *   (quét/huỷ/SLL, nhiều phút) không bao giờ bị bỏ vì hết lượt thử: host vẫn
 *   giữ kết quả theo requestId và trả khi xong.
 */
(function () {
  const $ = id => document.getElementById(id);
  const rid = () => (crypto.randomUUID ? crypto.randomUUID().replaceAll("-", "") : Math.random().toString(16).slice(2) + Date.now().toString(16));
  const listeners = new Map();          // channel -> Set<fn>
  const pending = new Map();            // requestId -> { msg, resolve, timer, tries }
  let ws = null, wsAttempt = 0, reconnectTimer = null, lastMessageAt = 0, watchdog = null;
  let snapshot = null;
  let booted = false;

  function toast(text, err = false) {
    const t = $("remoteToast"); if (!t) return;
    t.textContent = text; t.className = "remote-toast" + (err ? " err" : ""); t.hidden = false;
    clearTimeout(toast.timer); toast.timer = setTimeout(() => { t.hidden = true; }, 3200);
  }
  function emit(channel, payload) {
    for (const fn of listeners.get(channel) || []) { try { fn(payload); } catch (error) { console.error("[remote]", channel, error); } }
  }
  const on = (channel, fn) => { if (!listeners.has(channel)) listeners.set(channel, new Set()); listeners.get(channel).add(fn); };

  // ---- ghép nối ---------------------------------------------------------
  async function whoami() {
    try { const r = await fetch("/api/whoami", { cache: "no-store" }); const j = await r.json(); return Boolean(j && j.paired); }
    catch { return false; }
  }
  function showPair(show) {
    $("pair").hidden = !show;
    const app = document.querySelector(".app");
    if (app) app.style.display = show ? "none" : "";
  }
  $("connect").onclick = async () => {
    $("pairError").textContent = "";
    const r = await fetch("/api/session", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify({ token: $("token").value.trim() }) }).catch(() => null);
    if (!r || !r.ok) { $("pairError").textContent = "Mã ghép nối sai hoặc Windows chưa mở."; return; }
    $("token").value = "";
    showPair(false);
    connect();
  };
  $("token").addEventListener("keydown", e => { if (e.key === "Enter") $("connect").click(); });

  // ---- WebSocket --------------------------------------------------------
  function setConn(kind, text) {
    if (state.statusBar) state.statusBar.textContent = text;
    document.body.dataset.conn = kind;
  }
  function connect() {
    clearTimeout(reconnectTimer); reconnectTimer = null;
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
    setConn("warn", wsAttempt ? `Đang kết nối lại… (lần ${wsAttempt})` : "Đang kết nối…");
    let socket;
    try { socket = new WebSocket(`${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`); } catch { scheduleReconnect(); return; }
    ws = socket;
    socket.onopen = () => {
      wsAttempt = 0; lastMessageAt = Date.now();
      setConn("ok", "Đã kết nối Windows");
      for (const entry of pending.values()) socket.send(JSON.stringify(entry.msg));
      startWatchdog();
    };
    socket.onmessage = event => {
      lastMessageAt = Date.now();
      let msg; try { msg = JSON.parse(event.data); } catch { return; }
      if (msg.type === "ping") { try { socket.send(JSON.stringify({ type: "pong", at: Date.now() })); } catch { /* đóng */ } return; }
      if (msg.type === "snapshot" || msg.type === "state") { applySnapshot(msg.state); return; }
      if (msg.type === "event") { emit(msg.channel, msg.payload); return; }
      if (msg.type === "ack") { settle(msg.requestId, msg.result); return; }
    };
    socket.onclose = event => {
      if (ws !== socket) return;
      ws = null; stopWatchdog();
      if (event.code === 1008 || event.code === 4401) { showPair(true); setConn("bad", "Cần ghép nối lại"); return; }
      scheduleReconnect();
    };
    socket.onerror = () => { /* onclose theo sau */ };
  }
  function scheduleReconnect() {
    if (reconnectTimer) return;
    wsAttempt++;
    const base = Math.min(30000, 1000 * Math.pow(2, Math.min(wsAttempt - 1, 5)));
    const wait = Math.round(base * (0.7 + Math.random() * 0.6));
    setConn("bad", `Mất kết nối — nối lại sau ${Math.round(wait / 1000)}s`);
    reconnectTimer = setTimeout(async () => {
      reconnectTimer = null;
      if (wsAttempt % 3 === 0 && !(await whoami())) { showPair(true); setConn("bad", "Cần ghép nối lại"); return; }
      connect();
    }, wait);
  }
  function startWatchdog() {
    stopWatchdog();
    watchdog = setInterval(() => {
      if (!ws || ws.readyState !== WebSocket.OPEN) return;
      if (Date.now() - lastMessageAt > 45000) { setConn("warn", "Không thấy Windows 45s — nối lại"); try { ws.close(); } catch { /* đóng */ } }
    }, 5000);
  }
  function stopWatchdog() { if (watchdog) { clearInterval(watchdog); watchdog = null; } }
  function wakeUp() {
    if (document.visibilityState === "hidden") return;
    if (!ws || ws.readyState !== WebSocket.OPEN) { wsAttempt = 0; connect(); }
    else { try { ws.send(JSON.stringify({ type: "resync" })); } catch { /* đóng */ } }
  }
  document.addEventListener("visibilitychange", wakeUp);
  window.addEventListener("online", wakeUp);
  window.addEventListener("pageshow", wakeUp);
  window.addEventListener("focus", wakeUp);

  // ---- lệnh có ACK ------------------------------------------------------
  function command(msgBody) {
    const requestId = rid();
    const msg = { type: "command", requestId, ...msgBody };
    return new Promise(resolve => {
      const entry = { msg, resolve, tries: 0, timer: null };
      pending.set(requestId, entry);
      const attempt = async () => {
        entry.tries++;
        if (ws && ws.readyState === WebSocket.OPEN) {
          try { ws.send(JSON.stringify(msg)); } catch { /* fallback bên dưới */ }
        } else {
          try {
            const r = await fetch("/api/command", { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(msg) });
            if (r.status === 401) { showPair(true); settle(requestId, { ok: false, error: "Cần ghép nối lại" }); return; }
            const result = await r.json().catch(() => null);
            if (result) { settle(requestId, result); return; }
          } catch { /* thử lại theo hẹn */ }
        }
        // Lệnh dài (quét/huỷ/SLL) chạy nhiều phút: gửi lại chỉ để hỏi kết quả
        // (host idempotent), không bao giờ bỏ cuộc khi vẫn còn kết nối.
        entry.timer = setTimeout(attempt, entry.tries < 3 ? 8000 : 20000);
      };
      attempt();
    });
  }
  function settle(requestId, result) {
    const entry = pending.get(requestId);
    if (!entry) return;
    clearTimeout(entry.timer); pending.delete(requestId);
    entry.resolve(result || { ok: false, error: "Không có kết quả" });
  }
  const ipc = (channel, payload = {}) => command({ action: "ipc", channel, payload });

  // ---- botAPI shim: đúng bề mặt preload.js mà các tab dùng ---------------
  /**
   * botAPI SHIM = cùng bề mặt với preload.js của Windows (1.25.1). Mỗi method
   * gửi đúng kênh IPC mà preload gửi; main chạy CHÍNH handler đó (allowlist).
   * Settings/credential/update là việc của Windows: trả lời rõ ràng, không gửi.
   */
  const windowsOnly = what => () => Promise.resolve({ ok: false, error: `${what} chỉ làm trên Windows.` });
  window.botAPI = {
    getState: () => Promise.resolve(fullStateFromSnapshot()),
    getVersion: () => Promise.resolve({ ok: true, version: (snapshot && snapshot.version) || "" }),
    getLogs: () => ipc("bot:getLogs").then(r => (r && r.logs) || []),
    clearLogs: () => Promise.resolve({ ok: true }),
    getLicense: () => Promise.resolve((snapshot && snapshot.license) || { locked: false }),
    setLicense: windowsOnly("Nhập License"),
    getDiagnostics: () => Promise.resolve({ ok: false }),
    diagnosticSnapshot: () => Promise.resolve({ ok: false }),
    getKeyStats: () => ipc("bot:keyStats"),
    remotePairing: () => Promise.resolve({ enabled: true, remote: true }),
    openSeaLogout: windowsOnly("Đăng xuất OpenSea"),
    saveSettings: windowsOnly("Lưu Settings"),
    secretsStatus: () => Promise.resolve({ ok: true, remote: true }),
    clearSecrets: windowsOnly("Xoá credential"),
    activateApi: windowsOnly("Kích hoạt API key"),
    saveChainConfig: p => ipc("bot:saveChainConfig", p),
    setSaveEnabled: p => ipc("bot:setSaveEnabled", p),
    addNfts: p => ipc("bot:addNfts", p),
    updateNft: p => ipc("bot:updateNft", p),
    deleteNft: p => ipc("bot:deleteNft", p),
    deleteNfts: p => ipc("bot:deleteNfts", p),
    start: p => ipc("bot:start", p),
    pause: p => ipc("bot:pause", p),
    stop: p => ipc("bot:stop", p),
    emergencyStop: p => ipc("bot:emergencyStop", p),
    togglePause: p => ipc("bot:togglePause", p),
    sessionStatus: () => ipc("session:status"),
    updateStatus: () => Promise.resolve({ ok: false }),
    checkUpdate: windowsOnly("Cập nhật"),
    downloadUpdate: windowsOnly("Cập nhật"),
    installUpdate: windowsOnly("Cập nhật"),
    uiLog: () => {},
    reportUiError: () => Promise.resolve({ ok: true }),
    onUpdate: fn => on("bot:update", fn),
    onLog: fn => on("bot:log", fn),
    onSessionStatus: fn => on("session:status", fn),
    onLicenseState: fn => on("license:state", fn),
    onApiState: fn => on("api:state", fn),
    onKeyStats: fn => on("bot:keyStats", fn),
    onUpdateState: fn => on("update:state", fn)
  };
  function fullStateFromSnapshot() {
    if (!snapshot) return { ok: false };
    return { ok: true, settings: snapshot.settings || {}, chains: snapshot.chains || {}, license: snapshot.license,
      api: snapshot.api, stream: snapshot.stream, wallet: snapshot.wallet };
  }

  // ---- shell: cùng bố cục Windows (topbar + tabbar + content + footer) -----
  const { el, setClass } = OSB.utils;
  const TABS = [
    { id: "ethereum", label: "Ethereum" },
    { id: "robinhood", label: "Robinhood" },
    { id: "logs", label: "Logs" }
  ];
  const state = { panels: new Map(), tabButtons: new Map(), statusBar: null, activeTab: "ethereum", registry: null, logBox: null, licenseLocked: false, apiLocked: false };

  function buildShell() {
    const nav = el("nav", { className: "tabbar" });
    for (const tab of TABS) {
      const button = el("button", { className: "tab", text: tab.label, dataset: { tab: tab.id }, on: { click: () => showTab(tab.id) } });
      state.tabButtons.set(tab.id, button);
      nav.appendChild(button);
    }
    state.statusBar = el("div", { className: "statusbar", text: "Đang mở…" });
    const main = el("main", { className: "content" });
    for (const tab of TABS) {
      const panel = el("div", { className: "panel-page", dataset: { tab: tab.id } });
      state.panels.set(tab.id, panel);
      main.appendChild(panel);
    }
    state.footerVersion = el("span", { text: "OfferBot Remote" });
    document.body.appendChild(el("div", { className: "app" }, [
      el("header", { className: "topbar" }, [
        el("div", { className: "brand", text: "◆ OfferBot Remote" }),
        nav, state.statusBar
      ]),
      main,
      el("footer", { className: "app-footer" }, [
        el("span", { className: "footer-ready", text: "●  Mac chỉ là màn hình — engine chạy ở Windows" }),
        state.footerVersion
      ])
    ]));
  }
  function showTab(id) {
    if ((state.licenseLocked || state.apiLocked) && id !== "logs") { toast(state.licenseLocked ? "Licence bị khoá — mở Settings trên Windows." : "API key bị từ chối — sửa trong Settings trên Windows.", true); return; }
    state.activeTab = id;
    for (const [tabId, panel] of state.panels) {
      panel.style.display = tabId === id ? "block" : "none";
      setClass(panel, tabId === id ? "panel-page is-active" : "panel-page");
    }
    document.body.classList.toggle("tab-dashboard", id === "ethereum" || id === "robinhood");
    for (const [tabId, button] of state.tabButtons) setClass(button, tabId === id ? "tab tab-active" : "tab");
    try { localStorage.setItem("offerbot.remote.tab", id); } catch { /* riêng tư */ }
  }

  // ---- logs ----------------------------------------------------------------
  const MAX_LOG_LINES = 400;
  const logNodes = new Map();
  function appendLog(entry) {
    if (!state.logBox || !entry) return;
    if (entry.kind === "clear") { state.logBox.textContent = ""; logNodes.clear(); return; }
    if (entry.remove && entry.id) { const n = logNodes.get(entry.id); if (n) { n.remove(); logNodes.delete(entry.id); } return; }
    if (!entry.line) return;
    if (entry.id && logNodes.has(entry.id)) { logNodes.get(entry.id).textContent = entry.line + (entry.count > 1 ? `  ×${entry.count}` : ""); return; }
    const stick = state.logBox.scrollHeight - state.logBox.scrollTop - state.logBox.clientHeight < 60;
    const node = el("div", { className: "log-line", text: entry.line + (entry.count > 1 ? `  ×${entry.count}` : "") });
    if (entry.kind === "error") node.dataset.error = "1";
    if (entry.id) logNodes.set(entry.id, node);
    state.logBox.appendChild(node);
    while (state.logBox.childElementCount > MAX_LOG_LINES) state.logBox.firstElementChild.remove();
    if (stick) state.logBox.scrollTop = state.logBox.scrollHeight;
  }
  function buildLogsTab(container) {
    state.logBox = el("div", { className: "log-box" });
    container.appendChild(el("section", { className: "logs-tab" }, [
      el("div", { className: "panel-title", text: "Nhật ký (từ Windows)" }),
      state.logBox
    ]));
  }

  // ---- trạng thái -----------------------------------------------------------
  function applySnapshot(next) {
    if (!next) return;
    const first = !snapshot;
    snapshot = next;
    if (!booted) return;
    if (first) hydrate();
    for (const chain of ["ethereum", "robinhood"]) {
      const cs = snapshot.chains && snapshot.chains[chain];
      if (cs) { state.registry.update({ ...cs, chain }); emit("bot:update", { ...cs, chain }); }
    }
    if (snapshot.license) applyLicenseLock(snapshot.license);
    if (snapshot.api) applyApiLock(snapshot.api);
    if (state.footerVersion) state.footerVersion.textContent = `OfferBot Remote · Windows v${snapshot.version || "?"}${snapshot.wallet ? " · " + snapshot.wallet.slice(0, 6) + "…" + snapshot.wallet.slice(-4) : ""}`;
    const s = snapshot.stream || {};
    setConn("ok", `Đã kết nối · Stream ${s.connected ? "đang nhận" : "—"} · ${new Date().toLocaleTimeString()}`);
  }
  function hydrate() {
    OSB.theme.apply(snapshot.settings && snapshot.settings.theme);
    state.registry.hydrate(fullStateFromSnapshot());
    for (const entry of (snapshot.logs || []).slice(-MAX_LOG_LINES)) appendLog(entry);
  }
  function applyLicenseLock(status) {
    const locked = Boolean(status && status.locked);
    state.licenseLocked = locked;
    document.body.classList.toggle("license-locked", locked);
    if (locked) toast(`Licence: ${status.reason || status.message || "bị khoá"} — xử lý trên Windows.`, true);
  }
  function applyApiLock() {
    // CLAUDE.md §18: API key problems never lock the app (licence-only access).
    state.apiLocked = false;
    document.body.classList.remove("api-locked");
  }

  // ---- hàng NFT trên Remote: ảnh → mở OpenSea, tên → copy ----------------------
  //
  //   Bắt ở pha CAPTURE trên document và chặn lan truyền: dashboard-component
  //   (dùng chung với Windows) gắn dblclick "copy link" lên tên/ảnh; ở Remote
  //   tên bấm đúp copy LINK OpenSea canonical của hàng, ảnh bấm mở đúng URL đó (tr[data-url]).
  //   Clipboard API cần secure context; http://100.x qua Tailscale không phải →
  //   fallback execCommand("copy").
  function copyText(text) {
    if (navigator.clipboard && window.isSecureContext) return navigator.clipboard.writeText(text);
    return new Promise((resolve, reject) => {
      const ta = document.createElement("textarea");
      ta.value = text; ta.setAttribute("readonly", ""); ta.style.position = "fixed"; ta.style.opacity = "0";
      document.body.appendChild(ta); ta.select();
      let ok = false; try { ok = document.execCommand("copy"); } catch { ok = false; }
      ta.remove(); ok ? resolve() : reject(new Error("copy"));
    });
  }
  document.addEventListener("dblclick", event => {
    const name = event.target.closest && event.target.closest(".nft-table .nft-name");
    const thumb = event.target.closest && event.target.closest(".nft-table img.nft-thumb");
    if (!name && !thumb) return;
    event.stopPropagation(); event.preventDefault();
    if (!name) return;                       // dblclick ảnh: click đầu đã mở tab, không làm gì thêm
    // Copy CANONICAL OpenSea item URL của đúng hàng (đã lưu ở tr[data-url]);
    // không có thì ghép từ chain/contract/tokenId của chính hàng đó.
    const tr = name.closest("tr[data-url]");
    let url = tr && tr.dataset.url ? String(tr.dataset.url) : "";
    if (!url && tr) {
      const chain = (tr.closest(".panel-page") || {}).dataset?.tab || "";
      const sub = tr.querySelector(".nft-sub");
      const tokenId = sub ? String(sub.textContent || "").replace(/^#/, "").trim() : "";
      const contract = tr.dataset.contract || "";
      if (chain && contract && tokenId) url = `https://opensea.io/item/${chain}/${contract}/${tokenId}`;
    }
    if (!url) { toast("Hàng này chưa có link", true); return; }
    copyText(url).then(() => toast(`Đã copy link: ${url}`)).catch(() => toast("Không copy được", true));
  }, true);
  document.addEventListener("click", event => {
    const thumb = event.target.closest && event.target.closest(".nft-table img.nft-thumb");
    if (!thumb) return;
    event.stopPropagation(); event.preventDefault();   // không chọn/sửa/pause hàng
    const tr = thumb.closest("tr[data-url]");
    const url = tr && tr.dataset.url;
    if (!url) return;
    const last = Number(thumb.dataset.openedAt) || 0;
    if (Date.now() - last < 700) return;               // click thứ hai của một dblclick: không mở tab thứ hai
    thumb.dataset.openedAt = String(Date.now());
    window.open(url, "_blank", "noopener");
  }, true);

  // ---- khởi động ------------------------------------------------------------
  async function boot() {
    buildShell();
    state.registry = OSB.createDashboardRegistry();
    state.registry.mountAll({ ethereum: state.panels.get("ethereum"), robinhood: state.panels.get("robinhood") });
    buildLogsTab(state.panels.get("logs"));
    on("bot:log", appendLog);
    on("license:state", applyLicenseLock);
    on("api:state", applyApiLock);
    let saved = "ethereum";
    try { saved = localStorage.getItem("offerbot.remote.tab") || "ethereum"; } catch { /* riêng tư */ }
    showTab(state.panels.has(saved) ? saved : "ethereum");
    setInterval(() => { try { state.registry.tick(Date.now()); } catch (error) { console.error("[remote] tick", error); } }, 500);
    booted = true;
    if (snapshot) { const s = snapshot; snapshot = null; applySnapshot(s); }
    showPair(false); setConn("", "Đang mở…");
    if (await whoami()) connect();
    else { showPair(true); setConn("", "Chưa ghép nối"); }
  }
  window.addEventListener("DOMContentLoaded", () => { boot().catch(error => { console.error("[remote] boot", error); toast("Không mở được Remote: " + error.message, true); }); });
})();
