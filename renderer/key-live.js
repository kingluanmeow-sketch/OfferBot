"use strict";

/**
 * API KEY LIVEVIEW — Key 1 / Key 2, số thật, từ runtime thật.
 *
 *   Mỗi ô hiện đúng những gì bộ quản lý key, bộ điều phối đọc và broker ghi
 *   đang cầm: active hay đang nghỉ (cooldown 429), in-flight, hàng đợi, số
 *   429, nhịp ghi, và phân bổ request (P0 · nền · gửi · huỷ). Không có con
 *   số nào tính riêng cho màn hình; không có gì viết cứng. Nguồn duy nhất là
 *   `bot:keyStats` do main đẩy mỗi giây (và một lượt kéo lúc mount để ô
 *   không trống trước lần đẩy đầu).
 *
 *   Không bao giờ hiện key. Chỉ vân tay (8 hex của SHA-256).
 */
(function (OSB) {
  const { el, setText, setClass } = OSB.utils;

  // Hai key ngang hàng: không còn nhãn mạnh/yếu. Lượt ghi đi key nào rảnh sớm nhất.
  const ROLE_LABEL = {};
  const ROLE_HINT = {};

  function fmtMs(ms) {
    const n = Math.max(0, Number(ms) || 0);
    if (n < 1000) return `${Math.round(n)}ms`;
    return `${(n / 1000).toFixed(n >= 10000 ? 0 : 1)}s`;
  }

  /**
   * @param {{compact?: boolean}} [opts] compact = một dòng cho đầu dashboard;
   *   đầy đủ = thẻ theo key cho Settings.
   */
  function createKeyLive({ compact = false } = {}) {
    let root = null;
    let slotsBox = null;
    let queueLine = null;
    let emptyLine = null;
    let unsubscribe = null;
    const slotNodes = new Map();

    function slotNode() {
      const nodes = {
        name: el("b", { className: "keylive-name", text: "Key ?" }),
        badge: el("span", { className: "state state-idle", text: "…" }),
        fp: el("code", { className: "keylive-fp", text: "" }),
        role: el("span", { className: "keylive-role", text: "" }),
        inflight: el("span", { className: "keylive-metric", text: "" }),
        queue: el("span", { className: "keylive-metric", text: "" }),
        cooldown: el("span", { className: "keylive-metric", text: "" }),
        r429: el("span", { className: "keylive-metric", text: "" }),
        pace: el("span", { className: "keylive-metric", text: "" }),
        dist: el("span", { className: "keylive-dist", text: "" })
      };
      nodes.node = el("div", { className: "keylive-slot" }, [
        el("div", { className: "keylive-head" }, [nodes.name, nodes.badge, nodes.fp, nodes.role]),
        el("div", { className: "keylive-metrics" }, [
          nodes.inflight, nodes.queue, nodes.cooldown, nodes.r429, nodes.pace
        ]),
        nodes.dist
      ]);
      return nodes;
    }

    function render(stats) {
      if (!root || !stats) return;
      const slots = Array.isArray(stats.slots) ? stats.slots : [];

      emptyLine.hidden = slots.length > 0;
      setText(emptyLine, slots.length ? "" : "Chưa có API key nào được kích hoạt.");

      // Reconcile nodes with slots (1..N), never rebuild the whole panel.
      for (const [slot, nodes] of slotNodes) {
        if (!slots.some(s => s.slot === slot)) { nodes.node.remove(); slotNodes.delete(slot); }
      }
      for (const s of slots) {
        let nodes = slotNodes.get(s.slot);
        if (!nodes) { nodes = slotNode(); slotNodes.set(s.slot, nodes); slotsBox.appendChild(nodes.node); }
        setText(nodes.name, `Key ${s.slot}`);
        setText(nodes.fp, s.fp ? `#${s.fp}` : "");
        nodes.fp.title = "Vân tay SHA-256 của key — không phải key";
        setText(nodes.role, ROLE_LABEL[s.role] || "");
        nodes.role.title = ROLE_HINT[s.role] || "";

        const busy = (s.inFlight || 0) > 0 || (s.readActive || 0) > 0;
        if (!s.active) {
          setText(nodes.badge, `Nghỉ ${fmtMs(s.cooldownMs)}`);
          setClass(nodes.badge, "state state-fail");
        } else if (busy) {
          setText(nodes.badge, "Active · đang gửi");
          setClass(nodes.badge, "state state-done");
        } else {
          setText(nodes.badge, "Active");
          setClass(nodes.badge, "state state-done");
        }

        setText(nodes.inflight, `In-flight ${s.inFlight || 0}` +
          ((s.writesInFlight || 0) > 0 ? ` (ghi ${s.writesInFlight})` : ""));
        const readQ = Number(s.readBackground || 0);
        setText(nodes.queue, `Đọc nền ${readQ} · rps ${s.readRps === null || s.readRps === undefined ? "—" : s.readRps}`);
        const cool = Math.max(Number(s.cooldownMs) || 0, Number(s.readCooldownMs) || 0, Number(s.writeBlockedMs) || 0);
        setText(nodes.cooldown, cool > 0 ? `Cooldown ${fmtMs(cool)}` : "Cooldown 0");
        nodes.cooldown.classList.toggle("is-hot", cool > 0);
        setText(nodes.r429, `429: ${s.rateLimits || 0}` +
          ((s.recentRateLimits || 0) > 0 ? ` (${s.recentRateLimits} gần đây)` : ""));
        nodes.r429.classList.toggle("is-hot", (s.recentRateLimits || 0) > 0);
        setText(nodes.pace, s.writePaceMs ? `Nhịp ghi ${fmtMs(s.writePaceMs)}` +
          ((s.writeNextInMs || 0) > 0 ? ` · kế ${fmtMs(s.writeNextInMs)}` : "") : "Nhịp ghi —");

        const d = s.served || {};
        const total = (d.critical || 0) + (d.background || 0) + (d.write || 0) + (d.cancel || 0);
        setText(nodes.dist, total
          ? `Phân bổ: P0 ${d.critical || 0} · nền ${d.background || 0} · gửi ${d.write || 0}` +
            ((d.cancel || 0) ? ` · huỷ ${d.cancel}` : "") + ` · tổng ${total}`
          : "Phân bổ: chưa có request");
      }

      const rq = stats.readQueue || {};
      const wq = stats.writeQueue;
      setText(queueLine,
        `Hàng đợi đọc ${rq.total || 0} (P0 ${rq.critical || 0} · nền ${rq.background || 0}) · đang đọc ${rq.active || 0}` +
        (wq ? ` · hàng đợi ghi ${wq.queued || 0} · đang ghi ${wq.inFlight || 0}` : "") +
        (slots.length > 1 ? ` · ${stats.healthy || 0}/${slots.length} key dùng được` : ""));
    }

    function mount(container) {
      if (root) { container.appendChild(root); return root; }
      slotsBox = el("div", { className: "keylive-slots" });
      queueLine = el("div", { className: "keylive-queue info-line", text: "…" });
      emptyLine = el("div", { className: "info-line", text: "" });
      root = el("section", { className: `keylive${compact ? " is-compact" : ""}` }, [
        el("div", { className: "keylive-title" }, [
          el("span", { className: "panel-title", text: "API Key LiveView" })
        ]),
        emptyLine, slotsBox, queueLine
      ]);
      container.appendChild(root);

      const api = window.botAPI;
      if (api && typeof api.onKeyStats === "function") unsubscribe = api.onKeyStats(render);
      if (api && typeof api.getKeyStats === "function") {
        api.getKeyStats().then(stats => { if (stats && stats.ok !== false) render(stats); }).catch(() => {});
      }
      return root;
    }

    function unmount() {
      if (unsubscribe) { try { unsubscribe(); } catch { /* đã gỡ */ } unsubscribe = null; }
      if (root) root.remove();
    }

    return { mount, unmount, render };
  }

  OSB.createKeyLive = createKeyLive;
})(window.OSB);
