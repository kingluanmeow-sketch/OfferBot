"use strict";

/**
 * Renderer entry point.
 *
 * Owns the tab router, the log view, and THE single UI timer that drives every
 * countdown in the app (spec 20). No other file may create an interval.
 */

window.OSB = window.OSB || {};

(function (OSB) {
  const { el, setText, setClass } = OSB.utils;

  const TABS = [
    { id: "ethereum", label: "Ethereum", icon: "♦" },
    { id: "robinhood", label: "Robinhood", icon: "➤" },
    { id: "settings", label: "Settings", icon: "⚙" },
    { id: "logs", label: "Logs", icon: "▤" }
  ];

  /** Matches logger.js so the view can never outgrow the backend buffer. */
  const MAX_LOG_LINES = 1000;

  /** One tick drives every dashboard countdown. */
  const UI_TICK_MS = 250;

  const state = {
    activeTab: "ethereum",
    registry: null,
    settingsTab: null,
    panels: new Map(),
    tabButtons: new Map(),
    logBox: null,
    logCount: 0,
    logNodes: new Map(),
    logPaused: false,

    /**
     * Whether the Logs tab shows the raw backend record.
     *
     * Off by default. The backend log is written for whoever has to debug
     * this app and names the machinery to do it; a customer reading it learns
     * nothing they can act on and rather a lot about how the product works.
     * Every line is still received, still stored, still one click away.
     */
    logDebug: false,
    statusBar: null,

    /**
     * LICENSE LOCKED MODE.
     *
     * While this is set the window is Settings and nothing else. It is not a
     * cosmetic disable: showTab REFUSES to leave Settings, so a tab reached by
     * any means other than clicking still cannot be opened.
     */
    licenseLocked: false,
    lockReason: "",

    /**
     * API LOCKED MODE.
     *
     * Set when OpenSea has refused the credential itself. Separate from the
     * licence lock because the remedy is different: one needs a new licence
     * key, the other a new API key, and a single combined lock would send
     * half the users to the wrong field.
     *
     * Never set by a timeout, a 429 or a 5xx - see the guard in main.
     */
    apiLocked: false,
    apiReason: "",
    apiCode: ""
  };

  // ================================================================
  // Layout
  // ================================================================

  function buildShell() {
    const nav = el("nav", { className: "tabbar" });

    for (const tab of TABS) {
      const button = el("button", {
        className: "tab",
        text: tab.label,
        dataset: { tab: tab.id },
        on: { click: () => showTab(tab.id) }
      });
      state.tabButtons.set(tab.id, button);
      nav.appendChild(button);
    }

    state.statusBar = el("div", { className: "statusbar", text: "Đang khởi động…" });

    const main = el("main", { className: "content" });

    for (const tab of TABS) {
      const panel = el("div", { className: "panel-page", dataset: { tab: tab.id } });
      state.panels.set(tab.id, panel);
      main.appendChild(panel);
    }

    document.body.appendChild(
      el("div", { className: "app" }, [
        el("header", { className: "topbar" }, [
          nav,
          state.statusBar
        ]),
        main,
        el("footer", { className: "app-footer" }, [
          el("span", { className: "footer-ready", text: "●  Đã sẵn sàng" }),
          // Version đọc từ chính app đang chạy, không viết cứng.
          //
          //   Chuỗi viết cứng ở đây từng đứng ở v1.19.27 trong khi package đã là
          //   1.19.33 — người dùng nhìn footer và báo "vẫn bản cũ" dù đã update.
          //   Một nguồn sự thật duy nhất cho version là `app.getVersion()`.
          state.footerVersion = el("span", { text: "OpenSea Offer Bot  |  Made for traders" })
        ])
      ])
    );

    const api = window.botAPI;
    if (api && typeof api.getVersion === "function") {
      api.getVersion().then(r => {
        if (r && r.version) state.footerVersion.textContent =
          `OpenSea Offer Bot v${r.version}${r.diagnostic ? " DIAGNOSTIC" : ""}  |  Made for traders`;
      }).catch(() => {});
    }
  }

  function showTab(id) {
    // The one place a tab change can happen, and therefore the one place that
    // has to refuse. Disabling the buttons alone would leave every other route
    // to a tab open.
    // Both locks refuse here rather than only greying the buttons out: a tab
    // reached by any other route would otherwise still open.
    // Settings and Logs stay reachable while locked (CLAUDE.md §18): the lock
    // reason is only diagnosable from Logs.
    if ((state.licenseLocked || state.apiLocked) && id !== "settings" && id !== "logs") return;

    state.activeTab = id;

    for (const [tabId, panel] of state.panels) {
      panel.style.display = tabId === id ? "block" : "none";
      // A class as well as the inline display, so CSS can tell the visible
      // panel from the hidden ones (:has() cannot read inline display).
      setClass(panel, tabId === id ? "panel-page is-active" : "panel-page");
    }
    // The content area scrolls for Settings and the like; for a dashboard
    // it must not, or the NFT table cannot own the remaining height.
    document.body.classList.toggle("tab-dashboard", id === "ethereum" || id === "robinhood");
    for (const [tabId, button] of state.tabButtons) {
      setClass(button, tabId === id ? "tab tab-active" : "tab");
    }

    if (id === "settings") refreshDiagnostics();
  }

  /**
   * Lock or unlock the whole window on the licence.
   *
   * Called from main's push, so a revoke that lands while the app is running
   * locks it there and then rather than at the next thing the user opens.
   */
  function applyLicenseLock(status) {
    // While main has not read the credential store yet there is no verdict:
    // do not flash a lock / jump to Settings (1.25.1). Trading is gated in main
    // regardless, so this is display only; a real verdict locks as before.
    const loadingPhase = Boolean(status && (status.phase === "loading" || status.hydrated === false));
    const locked = Boolean(status && status.locked) && !loadingPhase;

    /**
     * "Đang tải" is not "chưa nhập", and the window must not confuse them.
     *
     * Before main has read the machine's credential store there is no verdict
     * yet, only an unset guard whose default reason happens to read "Chưa
     * nhập License Key." A window that painted that text was telling someone
     * with a perfectly good licence that they had none - which is the report
     * the owner filed - and there was nothing on screen to distinguish it
     * from the genuine case.
     *
     * So the wording comes from the PHASE, and the missing-licence sentence
     * is reachable only once main says it has looked.
     */
    const phase = (status && status.phase) || "";
    const hydrating = phase === "loading" || (status && status.hydrated === false);
    const reason = hydrating
      ? "Đang tải License…"
      : (status && status.lockReason) || "";

    if (locked === state.licenseLocked && reason === state.lockReason) return;

    state.licenseLocked = locked;
    state.lockReason = reason;

    document.body.classList.toggle("license-locked", locked);
    // While the licence is the problem it owns the screen; the API lock's
    // body class comes off so the two panels cannot both be showing.
    if (locked) document.body.classList.remove("api-locked");
    refreshTabLocks();

    if (locked) showTab("settings");

    if (state.settingsTab && typeof state.settingsTab.setLocked === "function") {
      state.settingsTab.setLocked(locked, reason);
    }
    // Re-apply the API lock underneath: a licence that has just been fixed
    // must reveal an API lock that was waiting behind it, not a normal window.
    if (!locked && state.apiLocked && state.settingsTab &&
        typeof state.settingsTab.setApiLocked === "function") {
      document.body.classList.add("api-locked");
      state.settingsTab.setApiLocked(true, state.apiReason);
      showTab("settings");
    }
    if (state.statusBar) {
      state.statusBar.textContent = locked
        ? `🔒 ${reason}`
        : state.statusBar.textContent.replace(/^🔒 .*/, "Sẵn sàng");
    }
  }

  /**
   * Lock or unlock the window on the OpenSea credential.
   *
   * The licence lock takes precedence: someone without a valid licence has no
   * reason to be entering an API key, and showing them the API screen would
   * be asking them to fix the wrong thing.
   */
  function applyApiLock(status) {
    // CLAUDE.md §18: an OpenSea key problem is shown, never a lock. Tabs,
    // Settings and Logs stay usable; only operations needing the key fail.
    const problem = Boolean(status && (status.state === "LOCKED" || status.locked));
    const reason = (status && status.reason) || "";
    const code = (status && status.code) || "";
    if (problem === state.apiProblem && reason === state.apiReason) return;

    state.apiProblem = problem;
    state.apiLocked = false;
    state.apiReason = reason;
    state.apiCode = code;

    document.body.classList.remove("api-locked");
    refreshTabLocks();

    if (state.settingsTab && typeof state.settingsTab.setApiProblem === "function") {
      state.settingsTab.setApiProblem(problem && !state.licenseLocked
        ? reason || "API OpenSea từ chối key — kiểm tra lại API Key." : "");
    }
  }

  /** One place decides which tabs a user can reach, whichever lock is on. */
  function refreshTabLocks() {
    const locked = state.licenseLocked || state.apiLocked;
    const reason = state.licenseLocked ? state.lockReason : state.apiReason;
    for (const [tabId, button] of state.tabButtons) {
      const blocked = locked && tabId !== "settings" && tabId !== "logs";
      button.disabled = blocked;
      button.title = blocked ? reason : "";
    }
  }

  // ================================================================
  // Logs tab
  // ================================================================

  function buildLogsTab(container) {
    state.logBox = el("div", { className: "log-box" });

    const pauseButton = el("button", {
      className: "btn",
      text: "Tạm dừng",
      on: {
        click: () => {
          state.logPaused = !state.logPaused;
          pauseButton.textContent = state.logPaused ? "Tiếp tục" : "Tạm dừng";
        }
      }
    });

    // The Logs tab is full height. The compact box belonged to the old Offer
    // Trait result list, which is where the request to shrink it came from -
    // applying it here took the log away from the one tab that exists to show
    // it, and the toggle that came with it toggled nothing worth toggling.
    const debugButton = el("button", {
      className: "btn btn-ghost",
      text: "Chi tiết kỹ thuật",
      on: {
        click: () => {
          state.logDebug = !state.logDebug;
          debugButton.textContent = state.logDebug
            ? "Ẩn chi tiết kỹ thuật"
            : "Chi tiết kỹ thuật";
          setClass(debugButton, state.logDebug ? "btn btn-ghost is-on" : "btn btn-ghost");
          renderAllLogLines();
        }
      }
    });

    // Manual clear wipes the backend buffer too, errors included.
    const clearButton = el("button", {
      className: "btn",
      text: "Reset",
      title: "Xoá trắng nhật ký",
      on: {
        click: async () => {
          const accepted = await OSB.utils.confirmDialog({
            title: "Reset nhật ký",
            lines: [
              { text: "Bạn có chắc muốn reset trang không?", strong: true },
              "Nhật ký đang hiển thị sẽ bị xoá và không lấy lại được.",
              "Không ảnh hưởng bot đang chạy hay tab nào khác."
            ],
            requireAck: false,
            tone: "danger",
            confirmLabel: "Reset",
            cancelLabel: "Không"
          });
          if (!accepted) return;
          clearLogView();
          await window.botAPI.clearLogs();
          OSB.utils.toast.info("Đã reset nhật ký.");
        }
      }
    });

    const diagnosticButton = el("button", {
      className: "btn btn-ghost",
      text: "Copy diagnostic snapshot",
      title: "Copy toàn bộ snapshot lifecycle, không chứa secret",
      on: { click: async () => {
        const snapshot = await window.botAPI.diagnosticSnapshot();
        await navigator.clipboard.writeText(JSON.stringify(snapshot, null, 2));
        OSB.utils.toast.success("Đã copy diagnostic snapshot.");
      } }
    });

    const copyLogsButton = el("button", {
      className: "btn btn-ghost",
      text: "Copy toàn bộ log",
      title: "Copy toàn bộ log hiện đang được giữ trong bộ đệm",
      on: { click: async () => {
        const entries = await window.botAPI.getLogs();
        await navigator.clipboard.writeText((entries || []).map(e => e.line || String(e)).join("\n"));
        OSB.utils.toast.success("Đã copy log hiện có.");
      } }
    });

    container.appendChild(
      el("section", { className: "logs-tab" }, [
        el("header", { className: "dash-head" }, [
          el("h2", { className: "dash-title", text: "Nhật ký" }),
          el("div", { className: "toolbar-row" }, [diagnosticButton, copyLogsButton, debugButton, pauseButton, clearButton])
        ]),
        state.logBox
      ])
    );
  }

  /**
   * Render one log entry.
   *
   * The backend deduplicates errors, so a repeated failure arrives with the
   * same id and `replace: true`; the existing node is updated in place instead
   * of a new one being appended. That is what stops a row failing every ten
   * seconds from flooding the view.
   */
  function appendLog(entry) {
    if (!state.logBox || !entry) return;

    if (entry.kind === "clear") {
      clearLogView();
      return;
    }

    if (entry.kind === "prune") {
      prunePlainLines(entry.keep || 300);
      return;
    }

    if (entry.remove && entry.id) {
      const existing = state.logNodes.get(entry.id);
      if (existing) {
        existing.remove();
        state.logNodes.delete(entry.id);
        state.logCount--;
      }
      return;
    }

    if (state.logPaused) return;
    if (!entry.line) return;

    // A repeat of a known error updates its row rather than adding one.
    if (entry.id && state.logNodes.has(entry.id)) {
      const node = state.logNodes.get(entry.id);
      node.dataset.raw = entry.line;
      renderLogLine(node);
      node.classList.add("log-repeat");
      return;
    }

    const node = el("div", { className: "log-line" });
    node.dataset.raw = entry.line;
    renderLogLine(node);
    if (entry.kind === "error") node.dataset.error = "1";

    state.logBox.appendChild(node);
    state.logCount++;
    if (entry.id) state.logNodes.set(entry.id, node);

    prunePlainLines(MAX_LOG_LINES);

    // Only autoscroll when the user is already at the bottom.
    const nearBottom =
      state.logBox.scrollHeight - state.logBox.scrollTop - state.logBox.clientHeight < 60;
    if (nearBottom) state.logBox.scrollTop = state.logBox.scrollHeight;
  }

  /**
   * Put one line on screen in whichever voice is selected.
   *
   * vocab.logView decides both things: what the line SAYS in the normal view,
   * and whether it belongs there at all. A line it does not recognise is
   * technical by definition - it was never written to be read by a customer -
   * so it is hidden rather than guessed at.
   */
  function renderLogLine(node) {
    const raw = node.dataset.raw || "";
    const view = OSB.vocab.logView(raw);

    if (state.logDebug) {
      node.textContent = raw;
      setClass(node, `log-line log-debug ${logClass(raw)}`.trim());
      node.hidden = false;
      return;
    }

    node.hidden = !view.show;
    node.textContent = view.show ? view.text : "";
    setClass(node, `log-line log-${view.level}`);
  }

  function renderAllLogLines() {
    if (!state.logBox) return;
    for (const node of state.logBox.children) renderLogLine(node);
  }

  /** Drop the oldest NON-error rows; kept errors are the useful part. */
  function prunePlainLines(keep) {
    let guard = 0;
    while (state.logCount > keep && guard++ < 5000) {
      const oldest = [...state.logBox.children].find(n => n.dataset.error !== "1");
      if (!oldest) break;
      for (const [id, node] of state.logNodes) {
        if (node === oldest) state.logNodes.delete(id);
      }
      oldest.remove();
      state.logCount--;
    }
  }

  function clearLogView() {
    while (state.logBox.firstChild) state.logBox.removeChild(state.logBox.firstChild);
    state.logNodes.clear();
    state.logCount = 0;
  }

  function logClass(line) {
    if (line.includes("[ERROR]")) return "log-error";
    if (line.includes("[SEND]")) return "log-send";
    if (line.includes("[ON TOP]")) return "log-ontop";
    if (line.includes("[OUTBID]")) return "log-outbid";
    if (line.includes("[LOW BALANCE]")) return "log-low";
    if (line.includes("[STREAM")) return "log-stream";
    if (line.includes("[DECISION]")) return "log-decision";
    return "";
  }

  // ================================================================
  // Status bar
  // ================================================================

  async function refreshDiagnostics() {
    const diagnostics = await window.botAPI.getDiagnostics();
    if (!diagnostics || diagnostics.ok === false) return;

    if (state.settingsTab) state.settingsTab.updateStream(diagnostics.stream);

    const keys = diagnostics.limiter?.keys || {};
    const stream = diagnostics.stream || {};

    const lic = diagnostics.license || {};

    // Three facts the user can act on: is it connected, is the licence good,
    // and for how long. Which API key is in use and how deep the log is are
    // ours to worry about, not theirs.
    void keys;

    /**
     * NÓI RÕ AI ĐANG MẤT KẾT NỐI.
     *
     * Dòng này từng là "○ Mất kết nối · License còn 361 ngày" — hai sự thật
     * không liên quan gì nhau, cách nhau một dấu chấm giữa, và cái đầu tiên
     * không có chủ ngữ. Người đọc ghép chúng lại thành "license bị mất kết
     * nối", trong khi thứ mất kết nối là WebSocket của OpenSea còn license
     * thì hoàn toàn bình thường và còn gần một năm.
     *
     * Và "mất kết nối" thường còn sai cả về sự việc: chưa thêm NFT nào thì
     * chưa có collection nào để nghe, nên stream CHƯA HỀ mở — không có gì
     * "mất" cả. Ba trạng thái, ba câu khác nhau.
     */
    const streamState = stream.connected
      ? "● Stream: đang nhận"
      : (Number(stream.collections) > 0
        ? "○ Stream: mất kết nối"
        : "○ Stream: chưa mở");

    const licence = lic.valid
      ? `License: còn ${lic.daysLeft} ngày`
      : `License: ${lic.lockReason || lic.reason || "chưa hợp lệ"}`;

    setText(state.statusBar, `${streamState}  ·  ${licence}`);
  }

  // ================================================================
  // Boot
  // ================================================================

  async function boot() {
    buildShell();

    state.registry = OSB.createDashboardRegistry();
    state.registry.mountAll({
      ethereum: state.panels.get("ethereum"),
      robinhood: state.panels.get("robinhood")
    });

    state.settingsTab = OSB.createSettingsTab({
      onSaved: settings => state.registry.applyDefaults(settings)
    });
    state.settingsTab.mount(state.panels.get("settings"));

    buildLogsTab(state.panels.get("logs"));

    showTab("ethereum");

    // ---- events --------------------------------------------------
    window.botAPI.onUpdate(update => state.registry.update(update));
    window.botAPI.onLog(line => appendLog(line));
    window.botAPI.onLicenseState(status => applyLicenseLock(status));
    window.botAPI.onApiState(status => applyApiLock(status));
    window.botAPI.onUpdateState(update => {
      if (state.settingsTab && typeof state.settingsTab.renderUpdate === "function") {
        state.settingsTab.renderUpdate(update);
      }
    });

    // ---- initial state -------------------------------------------
    const fullState = await window.botAPI.getState();
    if (fullState && fullState.ok !== false) {
      // Before the tabs paint: the theme decides how all of them look, and it
      // arrives with the rest of the settings.
      OSB.theme.apply(fullState.settings && fullState.settings.theme);

      state.registry.hydrate(fullState);
      state.settingsTab.hydrate(fullState);
      // Both locks come from the snapshot, not from a push.
      //
      // main sends license:state moments after createWindow(), which is long
      // before this file has run and subscribed - so the very first push was
      // dropped. The 15s watcher then deduplicates on an unchanged signature
      // and never re-sends it, so a fresh install stayed unlocked for the whole
      // session. Asking is not subject to that race; the pushes that follow
      // only ever carry CHANGES.
      if (fullState.license) applyLicenseLock(fullState.license);
      if (fullState.api) applyApiLock(fullState.api);
    }

    const history = await window.botAPI.getLogs();
    if (Array.isArray(history)) {
      for (const entry of history.slice(-MAX_LOG_LINES)) appendLog(entry);
    }

    // ---- the single UI timer -------------------------------------
    setInterval(() => {
      const now = Date.now();
      try {
        state.registry.tick(now);
      } catch (error) {
        console.error("[app] tick", error);
      }
    }, UI_TICK_MS);

    // Status bar is cheap and infrequent - it is not part of the render path.
    setInterval(refreshDiagnostics, 5000);
    refreshDiagnostics();
  }

  window.addEventListener("DOMContentLoaded", () => {
    boot().catch(error => {
      console.error("[app] boot failed", error);
      document.body.appendChild(
        el("pre", { className: "boot-error", text: String(error && error.stack) })
      );
    });
  });

  OSB.app = state;
})(window.OSB);
