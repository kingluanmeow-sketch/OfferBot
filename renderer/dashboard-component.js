"use strict";

/**
 * THE shared dashboard component.
 *
 * Ethereum and Robinhood both render through this one factory. The only inputs
 * that differ are `chain` and the heading text - layout, column widths, input
 * sizes, button sizes, spacing, fonts, counters, Scan Status, Priority and the
 * Start/Pause/Delete behaviour are literally the same code (spec 2, 3).
 *
 * There is deliberately ONE run control: START and PAUSE. The old STOP button
 * did the same thing as PAUSE and only added a third state to reason about.
 *
 * Rendering rules (spec 20):
 *   - the table skeleton is built ONCE
 *   - update() diffs rows and touches only the cells whose value changed
 *   - there is no per-row setInterval; app.js drives one timer that calls tick()
 */

window.OSB = window.OSB || {};

(function (OSB) {
  const {
    el,
    setText,
    setClass,
    setDisabled,
    fmtPrice,
    scanStatusView,
    statusView,
    validatePrices,
    toast,
    bindCommit,
    confirmDialog,
    bindPasteAsLines,
    withLineNumbers
  } = OSB.utils;

  /**
   * Put the NFT's picture in an <img>, and keep trying if it does not load.
   *
   * A URL is not a picture. Measured on the live provider: Pudgy Penguin #1's
   * cached image is an HTTP 200 `image/png` of ONE HUNDRED AND EIGHT BYTES - a
   * stub the CDN serves when it could not fetch the art. Nothing before the
   * decoder can tell that apart from a real image, so the row showed a URL, a
   * src, and nothing at all. That is the reported bug.
   *
   * So the row carries alternatives, and a load failure moves to the next one.
   * When they all fail the cell is hidden rather than filled with a
   * placeholder: a missing picture has to look missing, or a data problem
   * hides behind an icon for ever.
   */
  function setNftImage(img, row) {
    const first = String(row.image || "");
    const alts = Array.isArray(row.imageAlts) ? row.imageAlts.filter(Boolean) : [];

    img.dataset.queue = JSON.stringify(alts);

    /** Move to the next candidate, or give up. */
    const advance = why => {
      let queue = [];
      try { queue = JSON.parse(img.dataset.queue || "[]"); } catch { queue = []; }
      const next = queue.shift();
      img.dataset.queue = JSON.stringify(queue);

      if (next) {
        img.src = next;
        return;
      }
      // Out of candidates. Nothing pretends there is a picture.
      img.onerror = null;
      img.onload = null;
      img.style.visibility = "hidden";
      img.dataset.imageFailed = why;
    };

    img.onerror = () => advance("error");

    // ---- "it loaded" is not "it is visible" ------------------------------
    //
    // onerror alone was the whole check, and it misses the case this project
    // already knew about: a response that decodes to nothing. The CDN answers
    // 200 with an image content-type and a body that is not a picture, and
    // some sources answer with an image that has no intrinsic size at all -
    // an SVG without width/height, for instance. Neither fires onerror, so the
    // queue never advanced and the row showed an empty box with three working
    // alternatives sitting unused behind it.
    //
    // naturalWidth is the only thing that knows, which is exactly why the EXE
    // tests measure it. Now the renderer does too.
    img.onload = () => {
      if (img.naturalWidth > 0 && img.naturalHeight > 0) {
        img.style.visibility = "visible";
        delete img.dataset.imageFailed;
        return;
      }

      // Zero-sized. Try the next candidate; if this was the last one, keep it
      // rather than blanking the cell - an SVG that CSS can size still draws,
      // and hiding it would lose a picture that was about to appear.
      let queue = [];
      try { queue = JSON.parse(img.dataset.queue || "[]"); } catch { queue = []; }
      if (queue.length) advance("zero-size");
    };

    img.style.visibility = first ? "visible" : "hidden";
    img.src = first;
  }

  // Shared with Offer Trait: the same candidate queue and the same
  // naturalWidth proof, so one fix covers both tables.
  OSB.setNftImage = setNftImage;

  /** Tooltip for the Best Offer cell: the kind of order that is on top. */
  const BEST_SOURCE = Object.freeze({
    item: "Item Offer",
    collection: "Collection Offer",
    trait: "Trait Offer"
  });

  // "Đang xử lý" CHỈ khi thật sự đang ký / đang trên dây (1.25.2). Chờ phụ
  // thuộc (template, xác minh lệnh, đọc Best, API) giữ badge vị thế thị
  // trường; dòng tiến trình bên dưới nói đang chờ gì.
  const LIVE_BUSY_STATES = new Set(["BUILDING", "SIGNING", "SEND_OFFER"]);

  const COLUMNS = [
    { key: "select", label: "☑", width: "38px" },
    // Row number (newest first). The per-row Priority checkbox left the UI in
    // 1.19.31; the key stays "priorityMode" for the th class the layout uses.
    // The header must say what the cell shows (CLAUDE.md §15), so "#".
    { key: "priorityMode", label: "#", title: "Thứ tự", width: "44px" },
    // NFT takes whatever width is left (auto); every other column is fixed, so
    // the table fits the minimum window without a horizontal scrollbar.
    { key: "nft", label: "NFT", width: "auto" },
    { key: "min", label: "Min", width: "84px" },
    { key: "max", label: "Max", width: "84px" },
    { key: "step", label: "Step", width: "84px" },
    { key: "best", label: "Best", width: "100px" },
    { key: "mine", label: "Mine", width: "100px" },
    { key: "duration", label: "Phút", width: "64px" },
    // ONE State column: market position badge + the engine stage under it.
    { key: "scan", label: "Trạng thái", width: "190px" },
    { key: "start", label: "Thao tác", width: "44px" },
    { key: "pause", label: "", width: "44px" },
    { key: "delete", label: "", width: "44px" }
  ];

  /**
   * @param {object} options
   * @param {"ethereum"|"robinhood"} options.chain
   * @param {string} options.title
   */
  function createDashboard({ chain, title }) {
    const api = window.botAPI;

    /** url -> { tr, cells, inputs } */
    const rowNodes = new Map();

    /** urls the user ticked. A Set. */
    const selected = new Set();

    /** Last row payload per url, used to skip untouched cells. */
    const lastRow = new Map();

    let root = null;
    let tbody = null;
    let counterNodes = null;
    let addError = null;
    let toolbarButtons = null;
    let configInputs = null;
    let selectAllBox = null;
    let emptyRow = null;
    let selectionLabel = null;
    let searchInput = null;
    let tableWrap = null;
    let saveButton = null;
    let addButton = null;
    let addInFlight = null;


    /**
     * The WHOLE row turns red for exactly one canonical status (bridge.rowStatus):
     * OVER_MAX - the price to beat is above the user's Max (VERDICT.ABOVE_MAX),
     * so the bot cannot answer and the user must act.
     *
     * NOT for OUTBID_CHASING / TIED / OUTBID / FOLLOWING / ACTIVE: those are the
     * normal seconds of a chase the engine is already answering, and painting
     * them red made rows flash red on every tie or overbid.
     */
    const RED_ROW_STATUS = new Set(["OVER_MAX"]);

    /**
     * HEARTBEAT — the engine's, not ours.
     *
     *   The bridge pushes state on every change and at least every few seconds
     *   while running. `lastPushAt` is the last time that happened. A pulse in
     *   the Progress cell is drawn only while that is fresh; past HEARTBEAT_LOST_MS
     *   the rows say "Đang kết nối lại" instead. A CSS animation running on its
     *   own would keep glowing over a process that died an hour ago.
     */
    let lastPushAt = 0;
    let lastHeartbeat = null;
    let heartbeatLost = false;
    const HEARTBEAT_LOST_MS = 12000;
    const engineAlive = () => Boolean(lastHeartbeat && lastHeartbeat.engineRunning) &&
      (Date.now() - lastPushAt) < HEARTBEAT_LOST_MS;

    /** field -> bindCommit handle for the Add panel. */
    const configBindings = {};

    const CHAIN_LABEL = chain === "robinhood" ? "Robinhood" : "Ethereum";

    /** Persist one Add-panel field. */
    async function commitChainConfig(field, value, binding) {
      const config = readConfig();

      if (field !== "duration") {
        const check = validatePrices(config);
        if (!check.ok) {
          showError(check.error);
          toast.error(check.error);
          return;
        }
      }

      const result = await window.botAPI.saveChainConfig({ chain, config });

      if (!result.ok) {
        showError(result.error || "Lưu cài đặt thất bại.");
        toast.error(result.error || "Lưu cài đặt thất bại.");
        return;
      }

      showError("");
      if (binding) binding.sync(value);

      const label = {
        minPrice: "Min Price",
        maxPrice: "Max Price",
        step: "Step",
        duration: "Duration"
      }[field];

      // The panel Duration is only the DEFAULT for NFTs added from now on.
      // Existing rows keep their own per-row Duration (edited in the table).
      const applied = field === "duration" ? " · chỉ áp cho NFT thêm mới" : "";
      toast.success(`${CHAIN_LABEL}: ${label} = ${value} (đã lưu)${applied}`);
    }

    function readConfig() {
      return {
        minPrice: configInputs.minPrice.value,
        maxPrice: configInputs.maxPrice.value,
        step: configInputs.step.value,
        duration: configInputs.duration.value
      };
    }

    // ================================================================
    // Build
    // ================================================================

    function build() {
      // Mounting the registry twice must return the same tree; otherwise every
      // new tree owns another click callback for the same visible dashboard.
      if (root) return root;
      // ---- header + counters (spec 7: no "Watching") ----------------
      counterNodes = {
        total: el("b", { text: "0" }),
        active: el("b", { text: "0" }),
        onTop: el("b", { text: "0" }),
        outbid: el("b", { text: "0" }),
        working: el("b", { text: "0" }),
        failed: el("b", { text: "0" })
      };


      const countersPanel = el("div", { className: "counters" }, [
          counter("Tổng NFT", counterNodes.total, "c-total"),
          counter("Đang dẫn đầu", counterNodes.onTop, "c-ontop"),
          counter("Đang bị vượt giá", counterNodes.outbid, "c-outbid"),
          counter("Đang xử lý", counterNodes.working, "c-active"),
          counter("Lỗi", counterNodes.failed, "c-error")
        ]);


      // ---- add panel ------------------------------------------------
      const links = bindPasteAsLines(
        el("textarea", {
          className: "links-input",
          placeholder:
            "Dán link OpenSea — mỗi lần dán tự xuống dòng\nhttps://opensea.io/assets/" +
            chain +
            "/0x.../1234"
        })
      );

      configInputs = {
        links,
        minPrice: numberInput("0.001"),
        maxPrice: numberInput("0.2"),
        step: numberInput("0.001"),
        duration: numberInput("15", 1)
      };

      addError = el("div", { className: "form-error" });

      // The Add panel IS the saved default for this chain: every committed edit
      // is written to disk immediately, so there is no separate default-price
      // section in Settings and nothing to press Save on.
      const FIELD_LABELS = {
        minPrice: "Min Price",
        maxPrice: "Max Price",
        step: "Step",
        duration: "Duration"
      };

      for (const field of Object.keys(FIELD_LABELS)) {
        const input = configInputs[field];
        const binding = bindCommit(input, value =>
          commitChainConfig(field, value, binding)
        );
        configBindings[field] = binding;
      }

      addButton = el("button", {
        className: "btn btn-primary",
        text: "＋  Thêm NFT",
        on: { click: onAdd }
      });

      const addPanel = el("section", { className: "panel add-panel" }, [
        el("div", { className: "panel-title", text: "Thêm NFT" }),
        // Numbered, but the numbers are a sibling element - selecting the box
        // copies URLs and nothing else.
        withLineNumbers(links),
        el("div", { className: "config-grid" }, [
          field("Min Price", configInputs.minPrice),
          field("Max Price", configInputs.maxPrice),
          field("Step", configInputs.step),
          field("Duration (phút)", configInputs.duration),
          // Scan Delay and Max Speed were here. Both are gone: the stream is
          // the trigger, and Priority is a per-NFT switch on the row itself -
          // never a panel-wide default, because the whole point is that some
          // rows are marked and others are not.
          el("div", { className: "field field-action" }, [addButton])
        ]),
        addError
      ]);

      // ---- toolbar --------------------------------------------------
      toolbarButtons = {
        // The glyph is its own node: the LABEL is the word, and the word is
        // what tests, vocab and screen readers pin. An icon baked into the
        // text string had silently changed "Bắt đầu" into "▶  Bắt đầu".
        start: el("button", {
          className: "btn btn-start",
          on: { click: () => control("start") }
        }, [
          el("span", { className: "btn-icon", text: "▶", attrs: { "aria-hidden": "true" } }),
          el("span", { text: "Bắt đầu" })
        ]),
        pause: el("button", {
          className: "btn btn-pause",
          on: { click: () => control("pause") }
        }, [
          el("span", { className: "btn-icon", text: "Ⅱ", attrs: { "aria-hidden": "true" } }),
          el("span", { text: "Tạm dừng" })
        ]),
        // RESET, not "Xoá".
        //
        // "Xoá" reads like it might take the wallet, the key or the settings
        // with it, and a user who is not sure what a button destroys does not
        // press it. This one empties the NFT list for THIS network and stops
        // what it was doing - nothing else - and the confirmation says so in
        // as many words.
        remove: el("button", {
          className: "btn btn-danger",
          title: "Xoá danh sách NFT của bảng này. Không đụng tới License, " +
            "API Key, Private Key, ví hay cài đặt.",
          on: { click: onReset }
        }, [
          el("span", { className: "btn-icon", text: "↻", attrs: { "aria-hidden": "true" } }),
          el("span", { text: "RESET" })
        ])

        // No Bulk Offer / Bulk Cancel here. This dashboard is the per-item
        // bot: one NFT, one decision, one offer. Sending or cancelling many at
        // once belongs to the Offer SLL and Cancel tabs, which are built for it.
      };

      selectionLabel = el("span", { className: "selection-label", text: "Đã chọn 0" });
      saveButton = el("button", {
        className: "btn save-toggle",
        text: "SAVE OFF",
        title: "Bật để lưu atomic NFT và cấu hình của tab này; không lưu credential.",
        on: {
          click: async () => {
            const enabled = saveButton.dataset.enabled !== "true";
            const result = await api.setSaveEnabled({ chain, enabled });
            if (!result?.ok) return toast.error(result?.error || "Không đổi được SAVE.");
            renderSave(result.saveEnabled);
          }
        }
      });
      searchInput = el("input", {
        type: "search",
        className: "nft-search",
        placeholder: "Tìm NFT theo tên hoặc #token",
        on: { input: applySearch }
      });

      const toolbar = el("section", { className: "panel toolbar" }, [
        el("div", { className: "panel-title", text: "Điều khiển" }),
        el("div", { className: "toolbar-row" }, [
          toolbarButtons.start,
          toolbarButtons.pause,
          toolbarButtons.remove,
          saveButton,
          selectionLabel,
          el("span", {
            className: "toolbar-hint",
            text: "Không chọn dòng nào thì áp dụng cho tất cả."
          })
        ]),
        countersPanel,
      ]);

      // ---- table ----------------------------------------------------
      const colgroup = el(
        "colgroup",
        {},
        COLUMNS.map(column =>
          el("col", { style: { width: column.width } })
        )
      );

      selectAllBox = el("input", {
        type: "checkbox",
        on: { change: onSelectAll }
      });

      const headRow = el(
        "tr",
        {},
        COLUMNS.map(column =>
          el(
            "th",
            {
              className: `th-${column.key}`,
              ...(column.title ? { title: column.title } : {})
            },
            [column.key === "select" ? selectAllBox : column.label]
          )
        )
      );

      tbody = el("tbody");
      emptyRow = el("tr", { className: "empty-row" }, [
        el("td", {
          text: "Chưa có NFT nào. Dán link OpenSea ở ô phía trên.",
          attrs: { colspan: String(COLUMNS.length) }
        })
      ]);
      tbody.appendChild(emptyRow);

      const table = el("table", { className: "nft-table" }, [
        colgroup,
        el("thead", {}, [headRow]),
        tbody
      ]);

      const scrollOne = direction => {
        if (!tableWrap) return;
        const visible = Array.from(tbody.querySelectorAll("tr:not(.empty-row)"))
          .find(tr => tr.style.display !== "none");
        const height = visible ? visible.getBoundingClientRect().height : 52;
        tableWrap.scrollBy({ top: direction * Math.max(1, height), behavior: "smooth" });
      };
      tableWrap = el("div", { className: "table-wrap" }, [table]);
      const tableShell = el("div", { className: "table-shell" }, [
        el("div", { className: "table-head" }, [
          el("div", { className: "table-title", text: "Danh sách NFT" }),
          searchInput
        ]),
        tableWrap,
        el("div", { className: "one-row-scroll" }, [
          el("button", { text: "▲", title: "Cuộn lên 1 NFT", on: { click: () => scrollOne(-1) } }),
          el("button", { text: "▼", title: "Cuộn xuống 1 NFT", on: { click: () => scrollOne(1) } })
        ])
      ]);

      root = el("section", { className: "dashboard", dataset: { chain } }, [
        el("div", { className: "dashboard-top" }, [addPanel, toolbar]),
        tableShell
      ]);

      return root;
    }

    function applySearch() {
      const needle = String(searchInput?.value || "").trim().toLocaleLowerCase();
      for (const [url, node] of rowNodes) {
        const row = lastRow.get(url) || {};
        const haystack = `${row.name || ""} ${row.tokenId || ""} ${url}`.toLocaleLowerCase();
        node.tr.style.display = !needle || haystack.includes(needle) ? "" : "none";
      }
    }

    function renderSave(enabled) {
      if (!saveButton) return;
      saveButton.dataset.enabled = enabled ? "true" : "false";
      setText(saveButton, enabled ? "▣  SAVE ON" : "▣  SAVE OFF");
      saveButton.classList.toggle("is-on", Boolean(enabled));
    }

    function counter(label, valueNode, cls) {
      return el("div", { className: `counter ${cls}` }, [
        el("span", { className: "counter-label", text: label }),
        valueNode
      ]);
    }

    function field(label, input, extraClass = "") {
      return el("label", { className: `field ${extraClass}`.trim() }, [
        el("span", { className: "field-label", text: label }),
        input
      ]);
    }

    function numberInput(value, step = 0.001, min = "0", extraClass = "") {
      return el("input", {
        type: "number",
        className: extraClass ? `num-input ${extraClass}` : "num-input",
        value,
        attrs: { step: String(step), min: String(min) }
      });
    }

    // ================================================================
    // Actions
    // ================================================================

    function showError(message) {
      setText(addError, message || "");
      setClass(addError, message ? "form-error visible" : "form-error");
    }

    function onAdd() {
      // The assignment happens synchronously, before the first await. Two click
      // events in the same frame therefore share one execution.
      if (addInFlight) return addInFlight;
      setDisabled(addButton, true);
      addButton.classList.add("is-busy");
      setText(addButton, "Đang thêm…");
      addInFlight = executeAdd().finally(() => {
        addInFlight = null;
        setDisabled(addButton, false);
        addButton.classList.remove("is-busy");
        setText(addButton, "＋  Thêm NFT");
      });
      return addInFlight;
    }

    async function executeAdd() {
      const config = readConfig();

      // Instant feedback; main.js validates again and can still refuse.
      const check = validatePrices(config);
      if (!check.ok) {
        showError(check.error);
        toast.error(check.error);
        return;
      }


      const links = configInputs.links.value.trim();
      if (!links) {
        showError("Chưa nhập link NFT.");
        toast.warn("Chưa nhập link NFT.");
        return;
      }

      showError("Đang lấy dữ liệu NFT...");
      toast.info("Đang lấy dữ liệu NFT...");

      const requestId = globalThis.crypto?.randomUUID
        ? globalThis.crypto.randomUUID()
        : `${Date.now()}-${Math.random().toString(36).slice(2)}`;
      const result = await api.addNfts({ chain, links, config, requestId });

      // A replay is an internal delivery detail. The first invocation owns all
      // user feedback; a merged callback must stay silent.
      if (result?.idempotentReplay) return;

      if (!result.ok) {
        const message =
          result.error || (result.errors || []).join(" | ") || "Thêm thất bại.";
        showError(message);
        toast.error(message);
        return;
      }

      // The pasted links stay in the box. Clearing them threw away the list the
      // moment it was submitted, so a partial failure left nothing to correct
      // and re-adding meant pasting everything again. Duplicates are rejected
      // by the database, so pressing ADD twice is harmless.

      const dup = Number(result.duplicates) || 0;
      const dupText = dup ? ` · ${dup} NFT đã có trong danh sách` : "";
      if (result.errors && result.errors.length) {
        showError(`Thêm ${result.added} NFT${dupText}. Lỗi: ${result.errors.join(" | ")}`);
        toast.warn(`Thêm ${result.added} NFT, ${result.errors.length} link lỗi${dupText}`);
      } else if (dup) {
        // Part of the paste was already tracked: never silent.
        showError(`Thêm ${result.added} NFT${dupText}`);
        toast.warn(`${CHAIN_LABEL}: đã thêm ${result.added} NFT${dupText}`);
      } else {
        showError("");
        toast.success(`${CHAIN_LABEL}: đã thêm ${result.added} NFT`);
      }
    }

    const ACTION_LABEL = { start: "START", pause: "PAUSE", stop: "STOP" };

    async function control(action) {
      const urls = selected.size ? Array.from(selected) : null;
      const scope = urls ? `${urls.length} NFT đã chọn` : "tất cả NFT";

      const result = await api[action]({ chain, urls });

      if (result && result.ok === false) {
        showError(result.error || "");
        toast.error(`${ACTION_LABEL[action]} thất bại: ${result.error || ""}`);
        return;
      }

      showError("");

      // Start on THIS chain means the user has moved on from the clean cancel;
      // the banner explaining it goes with them. Only this chain's - the other
      // dashboard keeps whatever state it is in.

      toast.success(`${CHAIN_LABEL}: ${ACTION_LABEL[action]} ${scope}`);
    }

    /**
     * RESET this dashboard - an EMERGENCY STOP first, a list clear second.
     *
     * It used to be only the list clear, and that was the wrong order of
     * operations for a button people reach for when something is going wrong.
     * Deleting rows does stop the work on those rows eventually, but it leaves
     * the engine's own scheduled work - debounced refreshes, the REST
     * verification queue, reaction bookkeeping - to fire straight afterwards,
     * and it does nothing at all when the user has some rows SELECTED and
     * resets only those while the rest keep sending.
     *
     * So: stop this chain's engine, then clear. The stop is scoped to this
     * chain, so the other dashboard keeps running untouched, and the shared
     * stream, licence and rate limiter are not disturbed by either.
     *
     * The confirmation lists what survives as explicitly as what goes. A
     * destructive button whose blast radius is unclear is a button people
     * work around instead of using.
     */
    async function onReset() {
      const urls = selected.size ? Array.from(selected) : Array.from(lastRow.keys());
      if (!urls.length) {
        toast.warn("Không có NFT nào để RESET.");
        return;
      }

      const scope = selected.size
        ? `${urls.length} NFT đang chọn`
        : `TẤT CẢ ${urls.length} NFT của ${CHAIN_LABEL}`;

      const accepted = await OSB.utils.confirmDialog({
        title: `RESET ${CHAIN_LABEL}`,
        lines: [
          { text: `Sẽ xoá khỏi danh sách: ${scope}.`, strong: true },
          `DỪNG KHẨN CẤP mọi thao tác đang chạy của ${CHAIN_LABEL}: ` +
            "quét, chờ gửi và offer chưa ký đều huỷ ngay.",
          "Offer đã ký và gửi lên OpenSea thì không rút lại được.",
          "KHÔNG đụng tới những thứ sau:"
        ],
        bullets: [
          "License Key",
          "API Key OpenSea",
          "Private Key và ví",
          "Cài đặt chung",
          "Offer đã gửi lên OpenSea vẫn còn hiệu lực"
        ],
        acknowledge: "Tôi hiểu danh sách NFT sẽ bị xoá",
        confirmLabel: "RESET",
        cancelLabel: "Không"
      });
      if (!accepted) return;

      // STOP FIRST. The delete below can take a moment on a large list, and
      // anything still scanning during it would keep spending rate limit -
      // and could still reach a submit - for an operation the user has ended.
      // Scoped to this chain: the other dashboard is not touched.
      try {
        await api.emergencyStop({ chain, reason: "reset" });
      } catch {
        // A failed stop must not block the clear: the user asked for both,
        // and leaving the list intact after a failed stop is the worst of
        // the three outcomes.
      }

      const result = await api.deleteNfts({ chain, urls });
      for (const url of urls) selected.delete(url);
      syncSelectAll();

      if (result && result.ok === false) {
        toast.error(result.error || "RESET thất bại.");
        return;
      }
      toast.success(
        `${CHAIN_LABEL}: RESET ${urls.length} NFT · License, API Key, ` +
        "Private Key và cài đặt giữ nguyên"
      );
    }

    function onSelectAll(event) {
      const checked = event.target.checked;
      selected.clear();

      for (const [url, node] of rowNodes) {
        node.inputs.select.checked = checked;
        if (checked) selected.add(url);
      }
      updateToolbar();
    }

    function syncSelectAll() {
      if (!selectAllBox) return;
      const total = rowNodes.size;
      selectAllBox.checked = total > 0 && selected.size === total;
      selectAllBox.indeterminate = selected.size > 0 && selected.size < total;
      updateToolbar();
    }

    function updateToolbar() {
      const scope = selected.size ? "selected" : "all";

      if (selectionLabel) {
        setText(selectionLabel, `Đã chọn ${selected.size}`);
      }
      // The guard was an EMPTY block with the two writes left outside it, so
      // it guarded nothing: any call reaching here before build() has assigned
      // the toolbar - a selection change raised while the panel is still being
      // put together - threw a TypeError and took the rest of the mount with
      // it. Both writes belong inside.
      if (toolbarButtons) {
        toolbarButtons.start.title =
          scope === "selected" ? "Start các dòng đã chọn" : "Start toàn bộ";
        toolbarButtons.pause.title =
          scope === "selected" ? "Pause các dòng đã chọn" : "Pause toàn bộ";
      }
    }

    /**
     * Push one edited row field to the backend, which re-validates it.
     *
     * The engine reads min/max/step fresh on every decision, so an accepted
     * value is in force from the very next scan of that row - no restart.
     */
    async function pushPatch(url, patch, node, label = "") {
      // The Step box shows the AUTO step the bot is using (1.19.6), not the
      // row's configured Step. Merging the box value here silently rewrote the
      // configured Step every time Min/Max/Priority was edited (measured:
      // ticking Priority turned step 0.001 into 0.0001). Send the configured
      // value unless the user is editing Step itself.
      const known = lastRow.get(url) || {};
      const merged = {
        minPrice: node.inputs.minPrice.value,
        maxPrice: node.inputs.maxPrice.value,
        step: known.step !== undefined && known.step !== null && known.step !== ""
          ? String(known.step) : node.inputs.step.value,
        ...patch
      };

      const check = validatePrices(merged);
      if (!check.ok) {
        node.tr.classList.add("row-invalid");
        node.tr.title = check.error;
        showError(check.error);
        toast.error(check.error);
        return false;
      }

      const result = await api.updateNft({ chain, url, patch: merged });

      if (result && result.ok === false) {
        node.tr.classList.add("row-invalid");
        node.tr.title = result.error || "";
        showError(result.error || "");
        toast.error(result.error || "Cập nhật thất bại.");
        return false;
      }

      node.tr.classList.remove("row-invalid");
      node.tr.title = "";
      showError("");

      const name = (lastRow.get(url) || {}).name || "NFT";
      if (label) {
        toast.success(`${name}: ${label} (áp dụng từ lần scan kế tiếp)`);
      }
      return true;
    }

    // ================================================================
    // Row rendering
    // ================================================================

    function createRow(row) {
      const inputs = {
        select: el("input", {
          type: "checkbox",
          on: {
            change: event => {
              if (event.target.checked) selected.add(row.url);
              else selected.delete(row.url);
              syncSelectAll();
            }
          }
        }),
        minPrice: priceInput(row.minPrice),
        maxPrice: priceInput(row.maxPrice),
        step: priceInput(row.step)
      };

      // Double-click the picture or the name to copy that NFT's link. The URL
      // is already the row's identity, so there is nothing to look up.
      const copyLink = async () => {
        const url = row.url;
        if (!url) return;
        try {
          await navigator.clipboard.writeText(url);
          toast.success(`Đã copy link #${row.tokenId}`);
        } catch {
          toast.error("Không copy được vào clipboard");
        }
      };

      const cells = {
        index: el("span", { className: "row-index", text: "" }),
        name: el("div", {
          className: "nft-name",
          text: row.name,
          // Full name in the tooltip: the cell ellipsises long names.
          title: `${row.name} — double-click để copy link`,
          on: { dblclick: copyLink }
        }),
        sub: el("div", { className: "nft-sub", text: `#${row.tokenId}` }),
        image: el("img", {
          className: "nft-thumb",
          attrs: { src: row.image || "", alt: "" },
          title: "Double-click để copy link",
          on: { dblclick: copyLink }
        }),
        // Populated by applyRow. Kept beside the node because the fallback
        // has to survive re-renders that do not change the image.
        imageAlts: [],
        best: el("td", { className: "cell-price", text: fmtPrice(row.best) }),
        mine: el("td", { className: "cell-price", text: fmtPrice(row.mine) }),
        // "row-input" is the class Min/Max/Step use (priceInput()); without it
        // Duration kept .num-input's 118px/32px box in a 64px column instead of
        // matching height/padding/border/radius/focus (the reported "square
        // corner" mismatch — it was never sharing a rule with the price cells).
        duration: el("td", { className: "cell-input" }, [numberInput(row.duration || 15, 1, 1, "row-input")]),
        // The td stays a table cell; the three lines live in a fixed-height
        // box so a long stage text can never make this row taller.
        scan: el("td", { className: "cell-state" }, [
          el("div", { className: "state-box" }, [
            el("span", { className: "status status-paused" }),
            el("span", { className: "scan scan-stopped" }),
            el("span", { className: "scan-live-bar", attrs: { "aria-hidden": "true" } })
          ])
        ])
      };

      const rowName = () => (lastRow.get(row.url) || row).name || "NFT";

      const startBtn = el("button", {
        className: "icon-btn icon-start",
        attrs: { "aria-label": "Start" },
        title: "Start",
        on: {
          click: async () => {
            const result = await api.start({ chain, urls: [row.url] });
            if (result && result.ok === false) toast.error(result.error || "START thất bại.");
            else toast.success(`${rowName()}: START`);
          }
        }
      });

      const pauseBtn = el("button", {
        className: "icon-btn icon-pause",
        attrs: { "aria-label": "Pause" },
        title: "Pause",
        on: {
          click: async () => {
            const result = await api.pause({ chain, urls: [row.url] });
            if (result && result.ok === false) toast.error(result.error || "PAUSE thất bại.");
            else toast.success(`${rowName()}: PAUSE`);
          }
        }
      });

      const deleteBtn = el("button", {
        className: "icon-btn icon-delete",
        attrs: { "aria-label": "Delete" },
        title: "Delete",
        on: {
          click: async () => {
            const name = rowName();
            selected.delete(row.url);
            const result = await api.deleteNft({ chain, url: row.url });
            syncSelectAll();
            if (result && result.ok === false) toast.error(result.error || "Xoá thất bại.");
            else toast.success(`${name}: da xoa, moi thao tac da dung ngay`);
          }
        }
      });

      const tr = el("tr", { dataset: { url: row.url } }, [
        el("td", { className: "cell-select" }, [inputs.select]),
        el("td", { className: "cell-priority" }, [cells.index]),
        // Flex lives on the inner box, never on the <td>: a flex td leaves
        // table layout and its height/border stop matching the row.
        el("td", { className: "cell-nft" }, [
          el("div", { className: "nft-cell" }, [
            el("span", { className: "nft-thumb-box" }, [cells.image]),
            el("div", { className: "nft-meta" }, [cells.name, cells.sub])
          ])
        ]),
        el("td", { className: "cell-input" }, [inputs.minPrice]),
        el("td", { className: "cell-input" }, [inputs.maxPrice]),
        el("td", { className: "cell-input" }, [inputs.step]),
        cells.best,
        cells.mine,
        cells.duration,
        cells.scan,
        el("td", { className: "cell-btn" }, [startBtn]),
        el("td", { className: "cell-btn" }, [pauseBtn]),
        el("td", { className: "cell-btn" }, [deleteBtn])
      ]);

      const node = {
        tr,
        inputs,
        cells,
        buttons: { start: startBtn, pause: pauseBtn, remove: deleteBtn },
        statusSpan: cells.scan.firstChild.children[0],
        scanSpan: cells.scan.firstChild.children[1],
        scanBar: cells.scan.firstChild.children[2]
      };

      // Commit on Enter or on leaving the field (spec: "an ra ngoai hoac an
      // enter thi du lieu duoc ap dung ngay"), each with its own toast.
      node.bindings = {
        minPrice: bindCommit(inputs.minPrice, value =>
          pushPatch(row.url, { minPrice: value }, node, `Min Price = ${value}`).then(ok => {
            if (!ok) return;
            node.bindings.minPrice.sync(value);
          })
        ),
        maxPrice: bindCommit(inputs.maxPrice, value =>
          pushPatch(row.url, { maxPrice: value }, node, `Max Price = ${value}`).then(ok => {
            if (!ok) return;
            node.bindings.maxPrice.sync(value);
          })
        ),
        step: bindCommit(inputs.step, value =>
          pushPatch(row.url, { step: value }, node, `Step = ${value}`).then(ok => {
            if (!ok) return;
            node.bindings.step.sync(value);
          })
        )
        ,duration: bindCommit(cells.duration.firstChild, value =>
          pushPatch(row.url, { duration: value }, node, `Duration = ${value} phút`).then(ok => {
            if (!ok) return;
            node.bindings.duration.sync(value);
          })
        )
      };

      return node;
    }

    /** Tooltip for the progress cell while a row waits for its send slot. */
    function waitTooltip(row) {
      const w = row && row.wait;
      if (!w || !row.running) return "";
      const sec = ms => `${Math.round((Number(ms) || 0) / 1000)}s`;
      const parts = [];
      parts.push(w.state === "BROKER_WAIT" ? "Đang xin lượt gửi ở broker"
        : w.state === "RETRY" ? "Chờ thử lại" : "Sẵn sàng, chờ chỗ gửi");
      if (w.queuedForMs) parts.push(`chờ ${sec(w.queuedForMs)}`);
      if (w.state === "RETRY" && w.retryInMs) parts.push(`thử lại sau ${sec(w.retryInMs)}`);
      if (w.brokerConnected === false) parts.push("broker: MẤT KẾT NỐI");
      else if (w.brokerRole) parts.push(`broker: ${w.brokerRole}${w.brokerQueued ? ` · hàng ${w.brokerQueued}` : ""}`);
      if (w.cooldownMs > 4500) parts.push(`cooldown ${sec(w.cooldownMs)}`);
      if (w.requeues) parts.push(`xếp lại ${w.requeues} lần`);
      return parts.join(" · ");
    }

    function priceInput(value) {
      return el("input", {
        type: "number",
        className: "num-input row-input",
        value: String(value),
        attrs: { step: "0.001", min: "0" }
      });
    }

    /** Only the cells whose value changed are written (spec 4, 20). */
    function applyRow(node, row, previous, now) {
      // Whole row red only while the canonical status is OVER_MAX on a running
      // row. Bound to the status code, never the label; toggled on every render
      // so it cannot go stale (raising Max clears it on the next push).
      node.tr.classList.toggle("row-outbid",
        Boolean(row.running) && RED_ROW_STATUS.has(String(row.status || "")));

      if (!previous || previous.name !== row.name) {
        setText(node.cells.name, row.name);
        node.cells.name.title = `${row.name} — double-click để copy link`;
      }


      // Re-run the candidate queue when the image OR its fallbacks change: the
      // fallbacks can arrive in a later update with the same first image, and
      // a row already showing its broken first candidate must get them.
      if (!previous || previous.image !== row.image ||
          (previous.imageAlts || []).join("|") !== (row.imageAlts || []).join("|")) {
        setNftImage(node.cells.image, row);
      }

      if (!previous || previous.best !== row.best || previous.bestKind !== row.bestKind ||
          previous.bestKnown !== row.bestKnown || previous.running !== row.running) {
        // A running row whose book has not answered yet shows "—", not the
        // price read when the NFT was added: that number may be stale, and
        // the engine is not bidding on it either (it waits for the read).
        const unknown = row.running && row.bestKnown === false;
        setText(node.cells.best, unknown ? "—" : fmtPrice(row.best));
        // Where the number comes from. The engine ranks item, collection and
        // trait offers together; the tooltip says which one is on top.
        node.cells.best.title = unknown ? "" : (BEST_SOURCE[row.bestKind] || "");
      }

      if (!previous || previous.mine !== row.mine || previous.myOptimistic !== row.myOptimistic) {
        setText(node.cells.mine, fmtPrice(row.mine));
        setClass(
          node.cells.mine,
          row.myOptimistic ? "cell-price cell-pending" : "cell-price"
        );
        node.cells.mine.title = row.myOptimistic
          ? "Offer vừa gửi, OpenSea chưa index xong."
          : "";
      }

      if (!previous || previous.duration !== row.duration) {
        node.bindings?.duration?.sync(row.duration || 15);
      }

      // Scan Status is re-derived every tick because of the countdown.
      const scan = heartbeatLost && row.running ? OSB.vocab.scanView("LOST") : scanStatusView(row, now);
      setText(node.scanSpan, scan.label);
      setClass(node.scanSpan, scan.cls);
      // Light diagnostics for "Chờ lượt gửi": how long, where, broker state.
      node.cells.scan.title = [row.lastError || "", waitTooltip(row) || ""].filter(Boolean).join(" · ");
      const progress = {
        SCANNING: 0, WAIT_BEST: 0, RENEW_CHECK: 0, OWN_VERIFY: 0, POST_RECONCILE: 0, RECOVERING: 0, TEMPLATE_WAIT: 0,
        QUOTA: 42, QUEUED: 42, BUILDING: 58, SIGNING: 66,
        SEND_OFFER: 78, VERIFYING: 92, CONFIRMING: 96, RETRYING: 36,
        WATCHING: 0, NEXT_SCAN: 0, WORKING: 58, STOPPED: 0, ERROR: 0
      }[String(row.scanState || "STOPPED")] ?? 0;
      node.scanBar.hidden = progress === 0;
      node.scanBar.style.setProperty("--progress", `${progress}%`);
      setClass(node.scanBar, `scan-live-bar ${progress >= 96 ? "is-complete" : ""}`);

      if (!previous || previous.status !== row.status || previous.scanState !== row.scanState ||
          previous.running !== row.running) {
        // While a real runtime stage is active, the badge describes that same
        // activity instead of showing an older market position beside it.
        const runtime = String(row.scanState || "");
        const status = row.running && LIVE_BUSY_STATES.has(runtime)
          ? OSB.vocab.statusView("ACTIVE")
          : statusView(row);
        setText(node.statusSpan, status.label);
        setClass(node.statusSpan, status.cls);
      }


      // Never fight the user for an input they are typing into. The commit
      // baseline moves with the value so a backend push cannot look like an
      // edit and fire a toast on blur.
      syncInput(node.inputs.minPrice, row.minPrice, node.bindings?.minPrice);
      // Max comes back already on the price grid (rounded DOWN by the engine),
      // so a 0.1025 the user typed reads back as 0.102 - never 0.103.
      syncInput(node.inputs.maxPrice, row.maxPrice, node.bindings?.maxPrice);
      /**
       * STEP IS WHAT THE ENGINE USES, NOT WHAT WAS TYPED
       *
       *   The step follows the tier of the current Best (< 0.1 → 0.0001,
       *   >= 0.1 → 0.001) and flips both ways as Best crosses 0.1. The cell
       *   shows that effective step whenever the engine knows a Best, so the
       *   number on screen is the number in the next order. The typed value
       *   remains the default for rows that do not know a Best yet.
       */
      // Always the engine's step: by the tier of the current Best, or of Min
      // while no Best is known yet. The typed value is only a stored default.
      const autoStep = Number(row.effectiveStep) > 0;
      // Ô Step luôn là giá trị người dùng nhập. effectiveStep chỉ là giá trị
      // quyết định nội bộ MAX(userStep, autoStep), không được ghi ngược lên UI.
      syncInput(node.inputs.step, row.step, node.bindings?.step);
      if (!previous || previous.effectiveStep !== row.effectiveStep || previous.bestKnown !== row.bestKnown) {
        setClass(node.inputs.step, "num-input row-input");
        node.inputs.step.title = autoStep
          ? (row.bestKnown ? `Bước tự động theo Best (${fmtPrice(row.effectiveBest)}): ${row.effectiveStep}` : `Bước tự động (chưa biết Best): ${row.effectiveStep}`)
          : "Bước mặc định; bot tự chọn 0.0001 / 0.001 / 0.01 theo Best";
      }

      // Spec 21: exactly one of the two is enabled at any time.
      if (!previous || previous.running !== row.running) {
        setDisabled(node.buttons.start, row.running);
        setDisabled(node.buttons.pause, !row.running);
      }
    }

    function syncInput(input, value, binding = null) {
      if (document.activeElement === input) return;
      const next = String(value);
      if (input.value !== next) input.value = next;
      if (binding) binding.sync(next);
    }

    // ================================================================
    // Public API
    // ================================================================

    function mount(container) {
      container.appendChild(build());
      updateToolbar();
      return root;
    }

    /** Full state push from the engine. Diffs rows; never rebuilds the table. */
    function update(state) {
      if (!state || state.chain !== chain) return;

      const now = Date.now();
      lastPushAt = now;
      lastHeartbeat = state.heartbeat || { engineRunning: Boolean(state.running) };
      heartbeatLost = false;
      const seen = new Set();

      // NEWEST FIRST.
      //
      // `addedSeq` is the row's position in the engine's own insertion-order
      // Map (bridge.getRows(), 0 = oldest), never stored, never an identity —
      // it is recomputed from CURRENT position every push, which is exactly
      // what makes STT survive Add/Delete/reopen/Save without special-casing
      // any of them. Sorted descending here so rows[0] is the most recently
      // added NFT — the "add #41 and it's at the bottom of a 40-row list" complaint.
      const rows = (state.rows || []).slice().sort(
        (a, b) => (b.addedSeq || 0) - (a.addedSeq || 0));

      // Reconcile DOM order to `rows` exactly, every render — not just for a
      // brand-new row. The old code always inserted a new node at
      // tbody.firstChild, which only produced the right order by accident
      // (only while addedSeq was never populated, so `rows` was actually
      // oldest-first and iterating oldest→newest with "insert at top" happened
      // to stack newest on top). With addedSeq real, that trick inverted the
      // whole table on first mount. `afterNode` walks the desired order
      // top-to-bottom and moves/inserts each row's <tr> right after the
      // previous one; a row already in the right spot costs one pointer
      // comparison and no DOM write.
      let afterNode = null;
      for (const [rowIndex, row] of rows.entries()) {
        seen.add(row.url);

        let node = rowNodes.get(row.url);
        if (!node) {
          node = createRow(row);
          rowNodes.set(row.url, node);
        }

        const desiredNext = afterNode ? afterNode.nextSibling : tbody.firstChild;
        if (node.tr !== desiredNext) tbody.insertBefore(node.tr, desiredNext);
        afterNode = node.tr;

        applyRow(node, row, lastRow.get(row.url), now);
        // Newest-first display, while the number remains the stable ordinal
        // from current position (46, 45, 44… like the mock) — top = count.
        setText(node.cells.index, rows.length - rowIndex);
        lastRow.set(row.url, row);
      }

      for (const [url, node] of Array.from(rowNodes)) {
        if (seen.has(url)) continue;
        node.tr.remove();
        rowNodes.delete(url);
        lastRow.delete(url);
        selected.delete(url);
      }

      applySearch();

      emptyRow.style.display = rows.length ? "none" : "";

      const counters = state.counters || { total: 0, active: 0, onTop: 0, outbid: 0 };

      setText(counterNodes.total, counters.total);
      setText(counterNodes.active, counters.active);
      setText(counterNodes.onTop, counters.onTop);
      setText(counterNodes.outbid, counters.outbid);

      renderProgress(rows, counters);

      // Spec 21: START is dead only when there is nothing left to start,
      // PAUSE only when nothing is running. Never both enabled-looking.
      const anyRunning = rows.some(row => row.running);
      const allRunning = rows.length > 0 && rows.every(row => row.running);
      setDisabled(toolbarButtons.start, rows.length === 0 || allRunning);
      setDisabled(toolbarButtons.pause, !anyRunning);
      setDisabled(toolbarButtons.remove, rows.length === 0);

      syncSelectAll();
    }

    /**
     * The two live figures in the corner that are not simple totals.
     *
     * Counted from the rows the engine last pushed, so they cannot drift from
     * the table underneath them. There is no run-level progress here on
     * purpose: this dashboard watches continuously and has no finish line, so
     * a bar across it would be measuring something that does not exist.
     */
    function renderProgress(rows, counters) {
      if (!counterNodes) return;

      // The engine derives every counter from the same row status the table
      // draws, so when it hands them over they are the truth. The local
      // derivation below only covers a push that carries no counters.
      if (counters && Number.isFinite(Number(counters.working)) && Number.isFinite(Number(counters.failed))) {
        setText(counterNodes.working, counters.working);
        setText(counterNodes.failed, counters.failed);
        return;
      }

      const running = rows.filter(row => row.running);
      const isWorking = row =>
        row.scanState === "SCANNING" || row.scanState === "SEND_OFFER";
      const isFailed = row =>
        row.status === "ERROR" || row.status === "SEND_FAILED" ||
        row.status === "SUBMIT_BLOCKED" || row.status === "LOW_BALANCE";

      // A row mid-scan or mid-send is being worked on; a row that failed is
      // counted on its own so one bad NFT never reads as a broken dashboard.
      setText(counterNodes.working, running.filter(isWorking).length);
      setText(counterNodes.failed, rows.filter(isFailed).length);
    }

    /**
     * Called by the single app-wide UI timer (spec 20). Only touches the Scan
     * Status cell, which is the only thing that changes between engine pushes.
     */
    function tick(now) {
      // Heartbeat watchdog: the engine pushes at least every few seconds while
      // running. Silence past the threshold flips every running row to
      // "Đang kết nối lại" and stops the pulse; the next push restores them.
      const anyRunning = lastHeartbeat && lastHeartbeat.engineRunning;
      const lostNow = Boolean(anyRunning) && lastPushAt > 0 && (now - lastPushAt) > HEARTBEAT_LOST_MS;
      if (lostNow !== heartbeatLost) {
        heartbeatLost = lostNow;
        for (const [url, node] of rowNodes) {
          const row = lastRow.get(url);
          if (row) applyRow(node, row, row, now);
        }
      }
      for (const [url, node] of rowNodes) {
        const row = lastRow.get(url);
        if (!row) continue;
        // Not just NEXT_SCAN: a row holding news renders as WORKING and its
        // label has to be refreshed too, or it would keep showing the
        // countdown it had a moment before the event arrived.
        if (row.scanState !== "NEXT_SCAN") continue;

        const view = scanStatusView(row, now);
        setText(node.scanSpan, view.label);
        setClass(node.scanSpan, view.cls);
      }
    }

    /**
     * Restore the Add panel from the config saved on the last edit.
     * This runs on boot, which is what makes the panel remember its values
     * across restarts without any Save button.
     */
    function setDefaults(defaults) {
      if (!defaults || !configInputs) return;

      const map = {
        minPrice: defaults.minPrice,
        maxPrice: defaults.maxPrice,
        step: defaults.step,
        duration: defaults.duration
      };
      renderSave(defaults.saveEnabled === true);

      for (const [key, value] of Object.entries(map)) {
        if (value === undefined) continue;
        if (document.activeElement === configInputs[key]) continue;
        configInputs[key].value = String(value);
        if (configBindings[key]) configBindings[key].sync(value);
      }

      // Max Speed was restored here. It is gone, and Priority is NOT restored
      // in its place on purpose: Priority belongs to an NFT, not to the panel,
      // so there is no panel-wide value to put back.
    }

    return {
      chain, title, mount, update, tick, setDefaults
    };
  }

  OSB.createDashboard = createDashboard;
  OSB.DASHBOARD_COLUMNS = COLUMNS;
})(window.OSB);
