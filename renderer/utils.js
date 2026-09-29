"use strict";

/**
 * Renderer utilities.
 *
 * Loaded as a classic script (file:// blocks ES modules), so everything is
 * attached to the single window.OSB namespace.
 */

window.OSB = window.OSB || {};

(function (OSB) {
  // ----------------------------------------------------------------
  // DOM
  // ----------------------------------------------------------------

  /**
   * Create an element.
   * @param {string} tag
   * @param {object} [props] className / text / attrs / dataset
   * @param {Array<Node|string>} [children]
   */
  function el(tag, props = {}, children = []) {
    const node = document.createElement(tag);

    if (props.className) node.className = props.className;
    if (props.text !== undefined) node.textContent = String(props.text);
    if (props.html !== undefined) node.innerHTML = props.html;
    if (props.title) node.title = props.title;
    if (props.type) node.type = props.type;
    if (props.value !== undefined) node.value = props.value;
    if (props.placeholder) node.placeholder = props.placeholder;
    if (props.checked !== undefined) node.checked = Boolean(props.checked);

    if (props.attrs) {
      for (const [key, value] of Object.entries(props.attrs)) {
        node.setAttribute(key, String(value));
      }
    }

    // Styles go through the CSSOM, never through a style="" attribute: the
    // renderer runs under a CSP without 'unsafe-inline', which blocks the
    // attribute form. Property assignment is unaffected by CSP.
    if (props.style) {
      for (const [key, value] of Object.entries(props.style)) {
        node.style[key] = String(value);
      }
    }
    if (props.dataset) {
      for (const [key, value] of Object.entries(props.dataset)) {
        node.dataset[key] = String(value);
      }
    }
    if (props.on) {
      for (const [event, handler] of Object.entries(props.on)) {
        node.addEventListener(event, handler);
      }
    }

    for (const child of [].concat(children)) {
      if (child === null || child === undefined) continue;
      node.appendChild(
        typeof child === "string" ? document.createTextNode(child) : child
      );
    }

    return node;
  }

  /** Write text only when it actually changed - avoids needless reflow. */
  function setText(node, text) {
    if (!node) return;
    const value = text === null || text === undefined ? "" : String(text);
    if (node.textContent !== value) node.textContent = value;
  }

  /** Toggle a class only when it actually changed. */
  function setClass(node, className) {
    if (!node) return;
    if (node.className !== className) node.className = className;
  }

  function setDisabled(node, disabled) {
    if (!node) return;
    const value = Boolean(disabled);
    if (node.disabled !== value) node.disabled = value;
  }

  function clear(node) {
    if (!node) return;
    while (node.firstChild) node.removeChild(node.firstChild);
  }

  // ----------------------------------------------------------------
  // Formatting
  // ----------------------------------------------------------------

  /** Prices show as a trimmed decimal, or an em-dash when absent. */
  function fmtPrice(value) {
    if (value === null || value === undefined) return "—";
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return "—";
    return n.toFixed(6).replace(/0+$/, "").replace(/\.$/, "");
  }

  /** Whole seconds remaining, never negative. */
  function secondsUntil(timestamp, now = Date.now()) {
    if (!timestamp) return 0;
    return Math.max(0, Math.ceil((timestamp - now) / 1000));
  }

  function shortAddress(address) {
    const value = String(address || "");
    if (value.length < 12) return value;
    return `${value.slice(0, 6)}...${value.slice(-4)}`;
  }

  // ----------------------------------------------------------------
  // Status presentation (spec 5 and 6)
  // ----------------------------------------------------------------

  /**
   * Scan status and Status, in the user's words.
   *
   * Both tables of labels moved to vocab.js, which is the one file allowed to
   * turn an internal enum into a sentence. These two functions stay because
   * every caller passes a ROW rather than a state, and because NEXT_SCAN has
   * to fold in the live countdown.
   */
  function scanStatusView(row, now = Date.now()) {
    const state = String(row.scanState || "STOPPED");

    if (state === "NEXT_SCAN") {
      // ---- news beats the clock ---------------------------------------
      //
      // `nextScanAt` is the PERIODIC poll's schedule. A row holding a stream
      // event is not waiting for it - buildQueue ignores nextScanAt entirely
      // for such a row - so a countdown here told the owner the bot was being
      // held back at the exact moment it was already acting.
      if (row.reactionDue) return OSB.vocab.scanView("WORKING");

      // ---- no countdown ------------------------------------------------
      //
      // A ticking "10s \u2026 9s \u2026 8s" was read as a promise the bot would act at
      // zero, and it is not one: the poll is a floor, the stream is the real
      // trigger, and a row can be sent long before the number runs out or held
      // past it by a rate limit. Showing a clock the engine does not obey made
      // people wait on it and then report it as broken.
      //
      // Animated dots instead. They say the same true thing at every instant -
      // this row is being worked on - without inventing a deadline.
      const seconds = secondsUntil(row.nextScanAt, now);
      if (seconds <= 0) return OSB.vocab.scanView("SCANNING");
      return OSB.vocab.scanView("NEXT_SCAN");
    }

    return OSB.vocab.scanView(state);
  }

  function statusView(row) {
    return OSB.vocab.statusView(row && row.status);
  }

  // ----------------------------------------------------------------
  // Validation mirror (spec 8)
  // ----------------------------------------------------------------

  /**
   * Same rule as db.validatePrices. This exists for instant UI feedback only -
   * the backend rejects independently, so this is never the last line of defence.
   */
  function validatePrices({ minPrice, maxPrice, step }) {
    const min = Number(minPrice);
    const max = Number(maxPrice);
    const stepValue = Number(step);

    if (!Number.isFinite(min) || min <= 0) {
      return { ok: false, error: "Min Price phải là số lớn hơn 0." };
    }
    if (!Number.isFinite(max) || max <= 0) {
      return { ok: false, error: "Max Price phải là số lớn hơn 0." };
    }
    if (!Number.isFinite(stepValue) || stepValue <= 0) {
      return { ok: false, error: "Step phải là số lớn hơn 0." };
    }
    if (min > max) {
      return { ok: false, error: "Min Price không được lớn hơn Max Price." };
    }
    return { ok: true };
  }

  // ----------------------------------------------------------------
  // Misc
  // ----------------------------------------------------------------

  // ----------------------------------------------------------------
  // Click handlers
  // ----------------------------------------------------------------

  /**
   * Wrap an async click handler so a throw is visible.
   *
   * An exception inside an async listener becomes an unhandled rejection and
   * nothing else: no message, no log, and whatever the handler had already
   * disabled stays disabled. To the user the button simply stops working -
   * which is exactly how a single dead property read turned into a feature
   * that appeared to do nothing at all.
   *
   * @param {string} label what the user was trying to do, in their words
   * @param {() => Promise<void>} run
   * @param {(failed:boolean) => void} [always] runs whatever happened, so a
   *   handler that set a busy flag can clear it
   */
  function guarded(label, run, always) {
    return async () => {
      try {
        await run();
        if (typeof always === "function") always(false);
      } catch (error) {
        if (typeof always === "function") always(true);

        // The user gets a sentence they can act on; the technical view keeps
        // the stack, which is the only place it is any use.
        const detail = (error && error.message) || String(error);
        toast.error(`${label} không thực hiện được. Hãy thử lại.`);
        console.error(`[${label}]`, error);
        if (window.botAPI && window.botAPI.reportUiError) {
          window.botAPI.reportUiError({ label, detail });
        }
      }
    };
  }

  // ----------------------------------------------------------------
  // Toast notifications
  // ----------------------------------------------------------------

  /**
   * Every data entry and every action button reports back visibly, so the user
   * always knows whether the value was accepted (spec: "khi nhap du lieu gi,
   * phai pop up thong bao de biet da nhap chua").
   *
   * The stack is capped and each toast owns a single timeout, so a burst of
   * edits cannot accumulate nodes or timers.
   */
  const TOAST_MS = 2600;
  const MAX_TOASTS = 5;

  let toastHost = null;

  function toastContainer() {
    if (toastHost && document.body.contains(toastHost)) return toastHost;
    toastHost = el("div", { className: "toast-host" });
    document.body.appendChild(toastHost);
    return toastHost;
  }

  /**
   * @param {string} message
   * @param {"success"|"error"|"info"|"warn"} [type]
   */
  function toast(message, type = "success") {
    const text = String(message || "").trim();
    if (!text) return null;

    const host = toastContainer();

    const icon = { success: "✅", error: "❌", warn: "⚠️", info: "ℹ️" }[type] || "ℹ️";
    const node = el("div", { className: `toast toast-${type}` }, [
      el("span", { className: "toast-icon", text: icon }),
      el("span", { className: "toast-text", text })
    ]);

    host.appendChild(node);

    while (host.childElementCount > MAX_TOASTS) {
      host.removeChild(host.firstChild);
    }

    const remove = () => {
      if (!node.parentNode) return;
      node.classList.add("toast-out");
      setTimeout(() => node.remove(), 180);
    };

    const timer = setTimeout(remove, type === "error" ? TOAST_MS * 2 : TOAST_MS);
    node.addEventListener("click", () => {
      clearTimeout(timer);
      remove();
    });

    return node;
  }

  toast.success = (m) => toast(m, "success");
  toast.error = (m) => toast(m, "error");
  toast.warn = (m) => toast(m, "warn");
  toast.info = (m) => toast(m, "info");

  /**
   * Wire an input so its value is applied on Enter AND on blur, reporting the
   * result each time. Re-applying the same value is skipped so tabbing through
   * a row does not fire a toast per field.
   */
  function bindCommit(input, onCommit) {
    let lastCommitted = String(input.value);

    const commit = reason => {
      const value = String(input.value);
      if (value === lastCommitted) return;
      lastCommitted = value;
      onCommit(value, reason);
    };

    input.addEventListener("blur", () => commit("blur"));
    input.addEventListener("keydown", event => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      commit("enter");
      input.blur();
    });

    // Checkboxes and selects have no meaningful blur semantics.
    if (input.type === "checkbox" || input.tagName === "SELECT") {
      input.addEventListener("change", () => commit("change"));
    }

    return {
      /** Keep the baseline in sync when the backend pushes a new value. */
      sync(value) {
        lastCommitted = String(value);
      }
    };
  }

  // ----------------------------------------------------------------
  // Confirm dialog
  // ----------------------------------------------------------------

  /**
   * THE confirmation modal. There is one, and this is it.
   *
   * Deliberately NOT window.confirm: this one spells out exactly what will
   * happen. For a destructive action it also keeps the confirm button disabled
   * until the user ticks the acknowledgement box, so a one-transaction "cancel
   * everything" can never be triggered by a single stray click.
   *
   * Offer Custom used to carry a second, near-identical dialog of its own, and
   * the stylesheet carried a second .modal-card rule to go with it - one whose
   * background came from a variable that does not exist, so it always painted
   * dark. On the light theme that was a dark card under near-black text. Two
   * components is how that happens; one is how it stops.
   *
   * @param {object} options
   * @param {string} options.title
   * @param {Array<string|{text:string, strong?:boolean}>} [options.lines]
   * @param {string[]} [options.bullets]      "this is what gets cancelled" list
   * @param {boolean} [options.requireAck]    tick-to-enable, for destructive ones
   * @param {"danger"|"primary"} [options.tone]
   * @param {string} [options.acknowledge]    checkbox label
   * @param {string} [options.confirmLabel]
   * @param {string} [options.cancelLabel]
   * @returns {Promise<boolean>}
   */
  function confirmDialog({
    title,
    lines = [],
    bullets = [],
    requireAck = true,
    tone = "danger",
    acknowledge = "Tôi đã đọc và đồng ý",
    confirmLabel = "XÁC NHẬN",
    cancelLabel = "Huỷ bỏ"
  }) {
    return new Promise(resolve => {
      const checkbox = el("input", { type: "checkbox" });

      const confirmButton = el("button", {
        className: tone === "primary" ? "btn btn-primary" : "btn btn-danger",
        text: confirmLabel
      });
      // Only a destructive action makes the user tick first. Asking for an
      // acknowledgement before placing an offer trains people to tick without
      // reading, which is exactly what the box exists to prevent.
      confirmButton.disabled = requireAck;

      const cancelButton = el("button", { className: "btn", text: cancelLabel });

      checkbox.addEventListener("change", () => {
        confirmButton.disabled = !checkbox.checked;
      });

      const overlay = el("div", { className: "modal-overlay" });

      let settled = false;
      const close = value => {
        if (settled) return;
        settled = true;
        document.removeEventListener("keydown", onKey, true);
        overlay.remove();
        resolve(value);
      };

      const onKey = event => {
        if (event.key === "Escape") {
          event.preventDefault();
          close(false);
        }
      };

      confirmButton.addEventListener("click", () => {
        if (requireAck && !checkbox.checked) return;
        close(true);
      });
      cancelButton.addEventListener("click", () => close(false));

      // Clicking the backdrop cancels; clicking inside the card must not.
      overlay.addEventListener("click", event => {
        if (event.target === overlay) close(false);
      });

      const body = [];
      for (const line of lines) {
        // A line may be emphasised - "12 NFT, 0.05 WETH mỗi cái" is the one
        // number the reader must not skim past.
        const strong = line && typeof line === "object" && line.strong;
        body.push(el("p", {
          className: strong ? "modal-strong" : "modal-line",
          text: typeof line === "string" ? line : String(line.text || "")
        }));
      }
      if (bullets.length) {
        body.push(
          el(
            "ul",
            { className: "modal-list" },
            bullets.map(item => el("li", { text: item }))
          )
        );
      }

      const card = el("div", {
        className: tone === "primary" ? "modal-card is-primary" : "modal-card"
      }, [
        el("div", {
          className: tone === "primary" ? "modal-title is-primary" : "modal-title",
          text: title
        }),
        el("div", { className: "modal-body" }, body),
        ...(requireAck ? [el("label", { className: "modal-ack" }, [
          checkbox,
          el("span", { text: acknowledge })
        ])] : []),
        el("div", { className: "modal-actions" }, [cancelButton, confirmButton])
      ]);

      overlay.appendChild(card);
      document.body.appendChild(overlay);
      document.addEventListener("keydown", onKey, true);
    });
  }

  function debounce(fn, wait = 250) {
    let timer = null;
    return function debounced(...args) {
      clearTimeout(timer);
      timer = setTimeout(() => fn.apply(this, args), wait);
    };
  }

  /**
   * Make a textarea take one pasted link per line, hands-free.
   *
   * Pasting normally drops the link wherever the caret happens to be, so the
   * next paste lands on the same line unless you press Enter yourself. Here the
   * paste is placed on its own line and the caret is left on a fresh one, and a
   * paste holding several links is split across lines rather than glued into a
   * single unusable string.
   */
  function bindPasteAsLines(textarea) {
    if (!textarea) return textarea;

    textarea.addEventListener("paste", event => {
      const clipboard = event.clipboardData || window.clipboardData;
      if (!clipboard) return;

      const raw = clipboard.getData("text");
      if (!raw) return;

      // Links never contain whitespace, so whitespace is a separator.
      const lines = raw.split(/\s+/).filter(Boolean);
      if (!lines.length) return;

      event.preventDefault();

      const start = textarea.selectionStart;
      const end = textarea.selectionEnd;
      const before = textarea.value.slice(0, start);
      const after = textarea.value.slice(end);

      // Never append to a line that already has something on it.
      const needsBreak = before.length > 0 && !before.endsWith("\n");
      const insert = (needsBreak ? "\n" : "") + lines.join("\n") + "\n";

      textarea.value = before + insert + after;

      const caret = before.length + insert.length;
      textarea.setSelectionRange(caret, caret);
      textarea.scrollTop = textarea.scrollHeight;

      // Let anything listening (counters, validation) see the change.
      textarea.dispatchEvent(new Event("input", { bubbles: true }));
    });

    return textarea;
  }

  /**
   * Number the lines of a textarea WITHOUT putting the numbers in it.
   *
   * The count has to be visible - forty pasted links look identical and
   * "did that paste land?" is a real question - but the numbers must never
   * become part of what is copied out or sent on. Writing "1. https://..."
   * into the box would do exactly that, and every consumer would then need
   * to strip a prefix that should not have existed.
   *
   * So the numbers live in a sibling element: rendered from the line count,
   * scrolled in step, and unselectable. Selecting the textarea selects URLs
   * and nothing else.
   *
   * @param {HTMLTextAreaElement} textarea
   * @returns {HTMLElement} the wrapper to put in the DOM
   */
  function withLineNumbers(textarea) {
    const gutter = el("div", { className: "lines-gutter" });
    const wrap = el("div", { className: "lines-wrap" }, [gutter, textarea]);

    const draw = () => {
      // An empty box still shows "1": the first line exists, it is blank.
      const count = Math.max(1, textarea.value.split("\n").length);
      const wanted = [];
      for (let i = 1; i <= count; i++) wanted.push(String(i));
      const text = wanted.join("\n");
      if (gutter.textContent !== text) gutter.textContent = text;
    };

    textarea.addEventListener("input", draw);
    textarea.addEventListener("scroll", () => {
      gutter.scrollTop = textarea.scrollTop;
    });
    draw();

    return wrap;
  }

  OSB.utils = {
    el,
    bindPasteAsLines,
    withLineNumbers,
    setText,
    setClass,
    setDisabled,
    clear,
    fmtPrice,
    secondsUntil,
    shortAddress,
    guarded,
    scanStatusView,
    statusView,
    validatePrices,
    toast,
    bindCommit,
    confirmDialog,
    debounce
  };
})(window.OSB);
