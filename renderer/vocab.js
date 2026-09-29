"use strict";

/**
 * vocab.js — the only place that decides what words the user reads.
 *
 * The app's internals have their own vocabulary: batches, lanes, queues,
 * workers, admission, generations, attempt ids. That vocabulary is correct
 * INSIDE the code and wrong on screen. It tells a customer nothing they can act
 * on, and it describes how the product is built to anyone who reads it.
 *
 * So every user-facing string that comes from an internal value is translated
 * here, once. Nothing else in the renderer invents a label from an enum: if a
 * screen needs a word for a state, it asks this file. That is what makes the
 * rule testable rather than aspirational — ui-vocab-test.js walks the rendered
 * DOM of every tab and fails on any term in BANNED below.
 *
 * The backend keeps every one of those concepts, unchanged. This file changes
 * what is SAID, never what is DONE.
 */

window.OSB = window.OSB || {};

(function (OSB) {
  /**
   * Words that must never reach the screen.
   *
   * Two kinds live here: implementation nouns (batch, lane, worker) and raw
   * internal enums (ON_TOP, ABORTED_SUPERSEDED). Both leak the same thing.
   * Matching is case-insensitive and on word boundaries, so "Duration" is safe
   * next to "ration" while a column header of "Job" is not.
   */
  const BANNED = Object.freeze([
    "batch", "queue", "worker", "lane", "engine", "job id", "jobid",
    "generation", "attempt id", "attemptid", "admission", "admitted",
    "backpressure", "coalesced", "rate limiter", "ratelimit", "token bucket",
    "submit lane", "order lane", "read lane", "concurrency",
    // "priority" used to be here. It is a USER-FACING name now - the ♿️
    // Priority switch on each row - chosen by the owner, so banning it would
    // ban the label from the feature it names.
    "bulk", "throughput", "mutation", "graphql", "seaport",
    "ON_TOP", "SUBMIT_BLOCKED", "SEND_FAILED", "LOW_BALANCE",
    "ABORTED_SUPERSEDED", "DEVICE_REFUSED", "DEVICE_DISABLED",
    "NOT_ACTIVATED", "CLOCK_ROLLBACK", "VALIDATION ERROR"
  ]);

  /** One regex per term, so a test can name which term leaked. */
  function bannedTermsIn(text) {
    const value = String(text || "");
    const hits = [];
    for (const term of BANNED) {
      const escaped = term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const pattern = new RegExp(
        "(^|[^A-Za-z0-9_])" + escaped + "([^A-Za-z0-9_]|$)", "i"
      );
      if (pattern.test(value)) hits.push(term);
    }
    return hits;
  }

  // ----------------------------------------------------------------
  // The shared progress model (spec 8)
  // ----------------------------------------------------------------

  /**
   * Every multi-item operation wears the same five states, so Offer Item,
   * Offer SLL and Cancel read alike. The enum stays internal; only the words
   * below are shown.
   */
  const PHASE = Object.freeze({
    IDLE: "IDLE",
    READY: "READY",
    PROCESSING: "PROCESSING",
    COMPLETED: "COMPLETED",
    PARTIAL: "PARTIAL",
    FAILED: "FAILED",
    // Người dùng bấm Dừng. KHÔNG phải FAILED — không có gì hỏng — và không
    // phải IDLE, vì đã có việc chạy và có con số để giữ lại.
    STOPPED: "STOPPED"
  });

  const PHASE_VIEW = Object.freeze({
    IDLE: { label: "Chưa chạy", cls: "state state-idle", dot: "○" },
    READY: { label: "Sẵn sàng", cls: "state state-ready", dot: "●" },
    PROCESSING: { label: "Đang xử lý", cls: "state state-run", dot: "●" },
    COMPLETED: { label: "Hoàn tất", cls: "state state-done", dot: "✓" },
    PARTIAL: { label: "Hoàn tất một phần", cls: "state state-warn", dot: "⚠" },
    FAILED: { label: "Không hoàn tất", cls: "state state-fail", dot: "✕" },
    STOPPED: { label: "Đã dừng", cls: "state state-warn", dot: "■" }
  });

  function phaseView(phase) {
    return PHASE_VIEW[String(phase || "IDLE")] || PHASE_VIEW.IDLE;
  }

  // ----------------------------------------------------------------
  // Per-NFT state on a dashboard
  // ----------------------------------------------------------------

  /**
   * What the row is doing, in the user's terms.
   *
   * "Đang dẫn đầu" rather than ON TOP, "Không gửi được" rather than
   * SEND_FAILED. SUBMIT_BLOCKED and SEND_FAILED stay distinct on purpose: one
   * is a price the marketplace refused, the other is a send that did not land,
   * and the user can act on that difference.
   */
  const STATUS_VIEW = Object.freeze({
    ON_TOP: { label: "Đang dẫn đầu", cls: "status status-ontop", dot: "●" },
    // ACTIVE is the row being WORKED RIGHT NOW: signed and on the wire, or
    // being signed. It used to read "Đang theo dõi", which was the default for
    // every running row and hid both leading and outbid.
    //
    // It is deliberately NARROW. It used to cover every row with a target,
    // including rows merely sitting in the scheduler backlog - so a row showed
    // "Chờ lượt gửi" in the progress column and "Đang xử lý" in the status
    // column at the same time, and the two read as a contradiction. The
    // backlog case is FOLLOWING below; ACTIVE now means what it says.
    ACTIVE: { label: "Gửi offer", cls: "status status-active", dot: "●" },
    // Outbid and answering: a target is priced and the row is waiting its turn
    // on the shared write pace. The progress column says WHERE it is waiting;
    // this column says the market position that put it there. Distinct from
    // OUTBID, which is outbid and UNABLE to answer.
    FOLLOWING: { label: "Đang theo dõi", cls: "status status-active", dot: "●" },
    // Bị vượt và đang đuổi (target <= Max).
    OUTBID_CHASING: { label: "Đang theo dõi", cls: "status status-outbid", dot: "●" },
    // Cần giá vượt Max: chỉ theo dõi, không gửi thấp hơn.
    OVER_MAX: { label: "Vượt Max", cls: "status status-outbid", dot: "●" },
    // Nobody has bid and neither have we. Distinct from ACTIVE so an empty
    // book is not dressed up as activity.
    NO_OFFER: { label: "Đang theo dõi", cls: "status status-none", dot: "○" },
    // Outbid AND cannot follow: the price to beat is above Max. The engine
    // only watches; it never sends a lower price.
    OUTBID: { label: "Bị vượt giá", cls: "status status-outbid", dot: "●" },
    LOW_BALANCE: { label: "Không đủ số dư", cls: "status status-low", dot: "✕" },
    SEND_FAILED: { label: "Không gửi được", cls: "status status-sendfail", dot: "⚠" },
    SUBMIT_BLOCKED: { label: "Giá không hợp lệ", cls: "status status-sendfail", dot: "⚠" },
    PAUSED: { label: "Tạm dừng", cls: "status status-paused", dot: "○" },
    ERROR: { label: "Lỗi", cls: "status status-error", dot: "✕" }
  });

  function statusView(status) {
    return STATUS_VIEW[String(status || "PAUSED")] || STATUS_VIEW.PAUSED;
  }

  /** Where the row is in its cycle. Never says who is winning — that is status. */
  /**
   * `dots: true` asks the renderer for the animated ellipsis.
   *
   * It is a CLASS on the cell, never a per-row timer: one CSS keyframe drives
   * every row at once, so a hundred NFTs cost the same as one and nothing in
   * the scheduler, the stream or the limiter is involved.
   */
  const SCAN_VIEW = Object.freeze({
    SCANNING: { label: "Đang theo dõi", cls: "scan scan-next" },
    // Cổng đang đóng nhưng lượt đọc chưa rời hàng đợi (liveness sẽ xếp nếu thiếu).
    WAIT_BEST: { label: "Đang theo dõi", cls: "scan scan-next" },
    // Own offer just expired; the book is being re-confirmed against the full
    // order list before any renewal is priced. Never "Đang dẫn đầu" here.
    RENEW_CHECK: { label: "Đang theo dõi", cls: "scan scan-next" },
    // Static order template missing (prewarm failed / evicted); being rebuilt in
    // the background. Not in the write queue yet, so never "Chờ lượt gửi".
    TEMPLATE_WAIT: { label: "Đang theo dõi", cls: "scan scan-next" },
    // SEND hợp lệ, đang chờ lượt đọc đầy đủ CỦA RIÊNG NFT này để biết lệnh của mình.
    OWN_VERIFY: { label: "Đang theo dõi", cls: "scan scan-next" },
    // POST trước có thể đã tới OpenSea mà mất phản hồi: đối soát trước khi gửi lại.
    POST_RECONCILE: { label: "Đang theo dõi", cls: "scan scan-next" },
    BUILDING: { label: "Gửi offer", cls: "scan scan-send" },
    // The submit, stage by stage. Every one of these is a real moment in
    // engine.submit, not a decoration: PREPARING is queued for write
    // capacity, SEND_OFFER is signed and on the wire, VERIFYING is waiting
    // for the order hash that proves it exists.
    PREPARING: { label: "Gửi offer", cls: "scan scan-send" },
    SIGNING: { label: "Gửi offer", cls: "scan scan-send" },
    QUOTA: { label: "Gửi offer", cls: "scan scan-send" },
    SEND_OFFER: { label: "Gửi offer", cls: "scan scan-send" },
    RECOVERING: { label: "Đang theo dõi", cls: "scan scan-next" },
    // Listening to the stream with nothing in hand. Not the dots (those mean
    // "busy"); a soft pulse that the renderer switches on ONLY while the
    // engine's heartbeat is fresh - so a dead process cannot look alive.
    WATCHING: { label: "Đang theo dõi", cls: "scan scan-next" },
    // Has a target, waiting for its turn on the shared 3–5s write pace. Alive
    // pulse, not busy dots: nothing is happening for THIS row yet, by design.
    QUEUED: { label: "Gửi offer", cls: "scan scan-send" },
    // Heartbeat from the engine has stopped arriving. Set by the renderer
    // itself, the one thing it is allowed to know on its own.
    LOST: { label: "Đang kết nối lại", cls: "scan scan-error" },
    VERIFYING: { label: "Gửi offer", cls: "scan scan-send" },
    RETRYING: { label: "Đang theo dõi", cls: "scan scan-next" },
    // "Chờ lượt sau" read as "the bot is being held back". It is not: this is
    // the NEXT SCHEDULED CHECK of a row that has nothing to react to. A row
    // with news does not show this at all - it shows WORKING.
    //
    // Animated, and with no number attached. The countdown that used to sit
    // here was read as a deadline the engine would honour, and it is not one:
    // the stream can fire first and a rate limit can hold it past zero.
    NEXT_SCAN: { label: "Đang theo dõi", cls: "scan scan-next" },
    WORKING: { label: "Gửi offer", cls: "scan scan-send" },
    LOADING: { label: "Đang theo dõi", cls: "scan scan-next" },
    FINDING: { label: "Đang theo dõi", cls: "scan scan-next" },
    CONFIRMING: { label: "Gửi offer", cls: "scan scan-send" },
    SENT: { label: "Đã gửi", cls: "scan scan-done" },
    STOPPED: { label: "Đã dừng", cls: "scan scan-stopped" },
    ERROR: { label: "Lỗi", cls: "scan scan-error" }
  });

  function scanView(state) {
    return SCAN_VIEW[String(state || "STOPPED")] || SCAN_VIEW.STOPPED;
  }

  // ----------------------------------------------------------------
  // Failures the user can act on
  // ----------------------------------------------------------------

  /**
   * Turn a failure into something with a next step in it.
   *
   * Anything unrecognised falls back to a plain sentence rather than the raw
   * message: an upstream error string is written for whoever wrote the upstream
   * code, and routinely carries an endpoint, a stack or an internal state name.
   */
  /**
   * Each pattern maps to an ERROR CODE, and the words come from the registry.
   *
   * The wording used to live here, which meant the same failure could be
   * described one way in a log line and another in a status badge. The
   * registry is now the single source for both, and this table only decides
   * WHICH failure a raw message is.
   */
  const REASON_CODES = [
    [/superseded|stale target/i, "E_ORDER_SUPERSEDED"],
    // Before the validation pattern: a message naming the PRICE is a price
    // problem, and "invalid price" contains the word validation often enough
    // to be swallowed by it otherwise.
    [/price grid|not on a valid|increment|invalid price|giá không hợp lệ/i, "E_PRICE_GRID"],
    [/validation|invalid price|invalid order|itemtype/i, "E_ORDER_VALIDATION"],
    [/insufficient|not enough|balance/i, "E_BALANCE_LOW"],
    [/rate limit|429|too many/i, "E_ORDER_RATE_LIMIT"],
    [/timeout|timed out|etimedout/i, "E_ORDER_TIMEOUT"],
    [/econn|enotfound|socket|fetch failed|network/i, "E_NETWORK"],
    [/signature|user rejected/i, "E_SIGNATURE"],
    [/rpc/i, "E_RPC"],
    [/device_refused/i, "E_DEVICE_REFUSED"],
    [/device_disabled/i, "E_DEVICE_DISABLED"],
    [/revoked|thu hồi/i, "E_LICENSE_REVOKED"],
    [/not_activated|chưa kích hoạt/i, "E_LICENSE_MISSING"],
    [/license.*(expired|hết hạn)|(expired|hết hạn).*license/i, "E_LICENSE_EXPIRED"],
    [/clock_rollback|đồng hồ/i, "E_CLOCK_ROLLBACK"],
    [/unauthorized|401|403|api key/i, "E_API_UNAUTHORIZED"],
    [/not found|404/i, "E_BEST_EMPTY"],
    [/best offer|không lấy được best/i, "E_BEST_SCAN"]
  ];

  /** The code a raw message belongs to, or "" when nothing recognises it. */
  function reasonCode(raw) {
    const value = String(raw || "").trim();
    if (!value) return "";
    for (const [pattern, code] of REASON_CODES) {
      if (pattern.test(value)) return code;
    }
    return "";
  }

  const REASONS = [
    [/superseded|stale target/i, "Đã bỏ qua vì giá đã thay đổi"],
    [/validation|invalid price|price grid|invalid order/i, "Giá không hợp lệ"],
    [/insufficient|balance|not enough/i, "Số dư WETH không đủ"],
    [/expired|hết hạn/i, "Đã hết hạn"],
    [/rate limit|429|too many/i, "OpenSea đang giới hạn truy cập, sẽ thử lại"],
    [/timeout|timed out|etimedout/i, "OpenSea phản hồi quá chậm"],
    [/econn|enotfound|socket|fetch failed|network/i, "Mất kết nối tới OpenSea"],
    [/signature|user rejected|sign/i, "Không ký được giao dịch"],
    [/unauthorized|401|403|api key/i, "API key không được chấp nhận"],
    [/not found|404/i, "OpenSea không tìm thấy dữ liệu này"],
    [/device_refused/i, "Thiết bị này không được phép dùng license"],
    [/device_disabled/i, "Thiết bị này đã bị khoá"],
    [/revoked|thu hồi/i, "License đã bị thu hồi"],
    [/not_activated|chưa kích hoạt/i, "License chưa được kích hoạt"],
    [/clock_rollback|đồng hồ/i, "Đồng hồ máy không chính xác"]
  ];

  /** Did any rule actually match, or did reasonText fall back? */
  function recognised(raw) {
    const value = String(raw || "").trim();
    return Boolean(value) && REASONS.some(([pattern]) => pattern.test(value));
  }

  function reasonText(raw) {
    const value = String(raw || "").trim();
    if (!value) return "Không thực hiện được";

    // The registry's wording wins where it has an opinion, so a failure reads
    // the same in a log line, a row badge and a result card.
    const code = reasonCode(value);
    const entry = code && OSB.errors ? OSB.errors.get(code) : null;
    if (entry) return entry.userMessage;

    for (const [pattern, text] of REASONS) {
      if (pattern.test(value)) return text;
    }
    // Unrecognised: say so plainly rather than echoing an internal message.
    return "Không thực hiện được";
  }

  // ----------------------------------------------------------------
  // Logs
  // ----------------------------------------------------------------

  /**
   * Which log lines a customer sees, and what they say.
   *
   * The backend's log is a developer's log and stays exactly as it is — it is
   * the record that makes a bug findable. The Logs tab simply stops showing all
   * of it by default: a line is promoted to the normal view only when it
   * matches a rule here AND the sentence produced carries no internal
   * vocabulary. Everything else is still there, one toggle away.
   *
   * Nothing is dropped, deleted or rewritten upstream. This is a filter over a
   * complete record, not a quieter record.
   */
  const LOG_RULES = [
    // ---- what is happening NOW ------------------------------------
    //
    // These come first. "SUBMIT QUEUED" rides the [SEND] tag and the generic
    // SEND rule below would report it as an offer already sent.

    [/\[ADD\]\s*#(\d+)/i,
      m => ({ level: "info", text: "Đang thêm NFT #" + m[1] })],

    [/\[VERIFY START\][\s\S]*?#(\d+)/i,
      m => ({ level: "info", text: "Đang kiểm tra giá NFT #" + m[1] })],

    [/\[BEST\]\s*#(\d+)\s*=\s*([\d.]+)/i,
      m => ({
        level: "info",
        text: Number(m[2]) > 0
          ? "Đã tìm thấy giá tốt nhất NFT #" + m[1] + ": " + m[2]
          : "NFT #" + m[1] + " chưa có ai đặt giá"
      })],

    [/SUBMIT QUEUED[\s\S]*?#(\d+)[\s\S]*?target=([\d.]+)/i,
      m => ({ level: "info", text: "Đang gửi offer #" + m[1] + " · " + m[2] })],

    [/SUBMIT SUCCESS[\s\S]*?#(\d+)/i,
      m => ({ level: "success", text: "Đã gửi offer #" + m[1] })],

    [/SUBMIT FAILED[\s\S]*?#(\d+)/i,
      m => ({ level: "error", text: "Không gửi được offer #" + m[1] })],

    [/\[SEND\][^\d]*([\d.]+)\s*WETH[\s\S]*?#?(\d+)/i,
      m => ({ level: "info", text: "Đã gửi offer " + m[1] + " WETH cho #" + m[2] })],
    [/\[SEND\][\s\S]*?#(\d+)/i,
      m => ({ level: "info", text: "Đã gửi offer cho #" + m[1] })],
    [/\[ON TOP\][\s\S]*?#(\d+)/i,
      m => ({ level: "info", text: "#" + m[1] + " đang dẫn đầu" })],
    [/\[OUTBID\][\s\S]*?#(\d+)/i,
      m => ({ level: "warn", text: "#" + m[1] + " đã bị vượt giá" })],
    [/\[LOW BALANCE\]/i,
      () => ({ level: "warn", code: "E_BALANCE_LOW", text: "Số dư WETH không đủ để tiếp tục gửi offer" })],
    [/\[SUBMIT BLOCKED\][\s\S]*?#(\d+)/i,
      m => ({
        level: "warn",
        code: "E_PRICE_GRID",
        text: "Không gửi được offer cho #" + m[1]
      })],
    [/\[LICENSE\]\s*([\s\S]*)$/i,
      m => ({
        level: "warn",
        text: "License: " + reasonText(m[1]),
        code: reasonCode(m[1]),
        needsReason: m[1]
      })],
    [/\[WALLET\]/i,
      () => ({ level: "info", text: "Đã kết nối ví" })],
    [/\[OFFER SLL\][\s\S]*job start[\s\S]*?(\d+)\s*NFT/i,
      m => ({ level: "info", text: "Bắt đầu gửi offer cho " + m[1] + " NFT" })],
    [/\[OFFER SLL\][\s\S]*job complete[\s\S]*FAILED/i,
      () => ({ level: "error", text: "Đợt gửi offer không hoàn tất" })],
    [/\[CANCEL SLL\][\s\S]*?(\d+)\s*target NFT/i,
      m => ({ level: "info", text: "Đang huỷ offer trên " + m[1] + " NFT" })],
    [/\[ERROR\]\s*([\s\S]*)$/i,
      m => ({
        level: "error",
        text: reasonText(m[1]),
        code: reasonCode(m[1]),
        needsReason: m[1]
      })]
  ];

  /**
   * An upstream message, if it is fit to show; otherwise a sentence that is.
   *
   * Most errors the app surfaces were written for the person reading them and
   * say something useful - which link was rejected, that a session needs
   * reopening. Those pass through unchanged, because replacing them with a
   * generic line would throw away the only thing that helps. The ones that
   * name the machinery are replaced instead of edited: half-scrubbing a
   * sentence leaves something that reads like a bug.
   */
  function safeMessage(raw) {
    const value = String(raw || "").trim();
    if (!value) return reasonText(value);
    return bannedTermsIn(value).length ? reasonText(value) : value;
  }

  /**
   * @returns {{show:boolean, level:string, text:string}} how the normal Logs
   *   view should render this backend line. `show:false` means it belongs to
   *   the technical view only.
   */
  function logView(line) {
    const raw = String(line || "");
    for (const [pattern, build] of LOG_RULES) {
      const match = raw.match(pattern);
      if (!match) continue;
      const out = build(match);
      // A rule that would itself leak cannot promote a line. The rules above
      // are hand-written, and a careless edit to one should fail closed rather
      // than ship a leak.
      if (bannedTermsIn(out.text).length) break;
      // A rule that only survived by falling back to the generic sentence has
      // nothing to tell the user. Showing it produces a log of identical
      // "Không thực hiện được" rows, which is worse than showing nothing:
      // it looks like repeated failure and names none of it.
      if (out.needsReason !== undefined && !recognised(out.needsReason)) break;

      // The code is an identifier, never a replacement for saying what
      // happened: it is prefixed to a sentence that already stands alone.
      const code = out.code || "";
      const text = code ? `[${code}] ${out.text}` : out.text;
      return { show: true, level: out.level, text, code };
    }
    return { show: false, level: "debug", text: raw, code: "" };
  }

  OSB.vocab = {
    BANNED, bannedTermsIn,
    PHASE, phaseView,
    statusView, scanView,
    reasonText, reasonCode, safeMessage, logView
  };
})(window.OSB);
