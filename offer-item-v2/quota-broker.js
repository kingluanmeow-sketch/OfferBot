"use strict";

/**
 * quota-broker.js — một hạn mức ghi cho cả máy, không phải một cho mỗi cửa sổ.
 *
 * VÌ SAO ĐIỀU NÀY TRỞ THÀNH BẮT BUỘC Ở 1.19.4
 *
 *   Trước 1.19.4 mỗi cửa sổ có credential riêng, nên "mỗi tiến trình một bộ
 *   giới hạn" tình cờ đúng. 1.19.4 làm cho mọi cửa sổ dùng CHUNG một OpenSea
 *   API key — và ngay lúc đó, ba cửa sổ mỗi cửa sổ tự giới hạn 2 lệnh/giây
 *   thành sáu lệnh/giây đập vào một hạn mức server duy nhất. Máy chủ không
 *   biết có ba tiến trình; nó chỉ thấy một key vượt hạn mức và trả 429.
 *
 *   Nên hạn mức phải được điều phối ở nơi nó thật sự tồn tại: theo API key,
 *   xuyên tiến trình.
 *
 * KIẾN TRÚC
 *
 *   Một tiến trình được bầu làm LEADER và giữ trạng thái hạn mức. Các tiến
 *   trình khác là FOLLOWER và xin giấy phép ghi qua named pipe của Windows.
 *   Leader chết thì follower bầu lại — người dùng không phải khởi động lại gì.
 *
 *   Bầu bằng chính named pipe: ai tạo được pipe thì người đó là leader. Đây
 *   là một thao tác nguyên tử do hệ điều hành bảo đảm, nên hai tiến trình
 *   không thể cùng thắng. Không cần file khoá, không cần đồng hồ đồng bộ.
 *
 * CHỈ PHỤC VỤ OFFER ITEM
 *
 *   Offer SLL và Offer Custom vẫn dùng bộ giới hạn cũ của chúng. Gộp lại là
 *   một thay đổi ngoài phạm vi, và tệ hơn: một lượt bulk 50 NFT sẽ ăn hết
 *   giấy phép của Offer Item, vốn là thứ nhạy cảm với từng mili giây.
 *
 * KHÔNG BAO GIỜ ĐI VÒNG HẠN MỨC
 *
 *   Broker chỉ mang FINGERPRINT của key, không bao giờ mang key. Mỗi key là
 *   một quota domain với mô hình riêng, và mỗi key được tôn trọng đúng hạn
 *   mức của CHÍNH nó.
 *
 * HAI KEY LÀ HAI CAPACITY THẬT — CHỌN KEY LÚC CẤP, KHÔNG PHẢI LÚC XẾP HÀNG
 *
 *   Bản 1.24.x cho engine `peek()` một key rồi xếp hàng vào domain của key
 *   đó TRƯỚC khi broker cấp gì. Nhiều SEND cùng lúc cùng peek thấy Key 1 và
 *   cùng bị ghim vào Key 1 trong khi Key 2 rảnh. Nay bên xin đưa DANH SÁCH
 *   domain của mọi key; broker chọn domain có thể ghi SỚM NHẤT ngay lúc cấp
 *   và trả domain đó về cùng giấy phép. HTTP dùng đúng key của giấy phép.
 */

const net = require("net");
const crypto = require("crypto");
const os = require("os");
const { EventEmitter } = require("events");

/** Named pipe của Windows; socket trong thư mục tạm ở nơi khác. */
/**
 * TÊN PIPE MANG CẢ MÔI TRƯỜNG
 *
 *   Vân tay khoá quyết định "cùng hạn mức thì cùng broker". Nhưng một bản dev
 *   chạy bằng CÙNG khoá đó không được phép vào chung hàng với bản production
 *   đang gửi tiền thật: nó sẽ ăn nhịp ghi, hoặc tệ hơn, lên leader và quyết
 *   định thay. Namespace tách hai thế giới ngay ở tên pipe.
 */
function pipePathFor(fingerprint) {
  const ns = require("../dev-runtime").brokerNamespace();
  if (process.platform === "win32") {
    return `\\\\.\\pipe\\${ns}-${fingerprint}`;
  }
  return `${os.tmpdir()}/${ns}-${fingerprint}.sock`;
}

/**
 * Dấu vân tay của API key. KHÔNG BAO GIỜ là chính key.
 *
 * Tám ký tự hex là đủ để hai key khác nhau không dùng chung broker, và không
 * đủ để dựng lại key. Chuỗi này đi qua IPC và vào log, nên nó phải là thứ an
 * toàn khi bị đọc.
 */
/**
 * NHỊP GHI HỌC TỪ BẰNG CHỨNG — KHÔNG CÒN GIÃN CỨNG 250–500ms MỖI LỆNH
 *
 *   Bản 1.24.x ép mỗi lượt ghi cách lượt trước ít nhất `paceMinMs` (500ms,
 *   sàn 250ms) và kiểm nhịp đó TRƯỚC cả `remaining` của server. Kết quả: một
 *   key còn 59/60 lượt vẫn bắt SEND thứ hai chờ nửa giây — độ trễ tự tạo.
 *
 *   Nay mỗi key có một GÁO NGẮN HẠN: `burst` lượt dùng ngay, đổ lại theo
 *   `rate` lượt/giây. Gáo còn thì cấp NGAY (chờ = 0). Gáo chỉ tồn tại vì
 *   OpenSea có một giới hạn ngắn hạn không nằm trong header (đo ở v1.19.7:
 *   11 lượt trong một giây → 429); con số khởi điểm là thứ đã đo được đó.
 *   `rate` chỉ GIẢM khi có bằng chứng (429 / 5xx) và HỒI nhanh (×1.25 mỗi
 *   3 giây phản hồi tốt) — không mất hàng phút để thoát một mức phạt cũ.
 */
const PACE_MIN_MS = 500;        // = 1000 / START_RPS, giữ để tương thích export
const PACE_JITTER_MS = 0;
const START_RPS = 2;
const START_BURST = 3;
const RATE_FLOOR = 0.25;
const RATE_CEILING = 8;
const RECOVER_EVERY_MS = 3000;
/** Order-posting ceilings at 80% of the dashboard (Key 1 2/s, Key 2 1/s). */
// Start at the dashboard rate (Key 1 2/s, Key 2 1/s); soft ceilings far above
// it so upgraded keys are used; AIMD (429 halves) finds the real limit.
// Owner 2026-09-30 (final): dashboard ceilings at 90%, keys not added.
const WRITE_CAPS = [2 * 0.9, 1 * 0.9];
const WRITE_START = [2 * 0.9, 1 * 0.9];
const WRITE_BURSTS = [2, 1];

function fingerprint(apiKey) {
  const raw = String(apiKey || "").trim();
  if (!raw) return "nokey";
  return crypto.createHash("sha256").update(raw).digest("hex").slice(0, 8);
}

/**
 * Mô hình hạn mức của MỘT key (một quota domain).
 *
 *   Ba điều kiện AND, theo thứ tự bằng chứng:
 *     1. cooldown server (Retry-After của CHÍNH key này)
 *     2. cửa sổ server (`remaining` / `reset`) khi header đã nói
 *     3. gáo ngắn hạn (burst + rate học từ phản hồi)
 *   Mọi khoảng chờ tính bằng đồng hồ đơn điệu.
 */
class QuotaModel {
  constructor({ startRps = START_RPS, burst = START_BURST,
    rateFloor = RATE_FLOOR, rateCeiling = RATE_CEILING } = {}) {
    this.startRps = startRps;
    this.rate = startRps;
    this.rateFloor = rateFloor;
    this.rateCeiling = Math.max(rateFloor, rateCeiling);
    this.burst = Math.max(1, burst);
    /** Gáo ngắn hạn. Khởi động ĐẦY: không có bằng chứng thì không phạt. */
    this.tokens = this.burst;
    this.lastRefillMono = QuotaModel.mono();

    this.goodResponses = 0;
    this.lastAdjustMono = QuotaModel.mono();
    this.latencyEwmaMs = 0;
    /** Lần cuối key này được cấp — để chia đều khi hai key cùng rảnh. */
    this.lastGrantMono = 0;

    this.limit = 0;
    this.remaining = -1;
    this.resetAtMono = 0;
    this.blockedUntilMono = 0;
    this.learned = false;
    this.lastServerMono = 0;
    this.spentSinceReport = 0;
    this.rateLimits = 0;
  }

  static mono() {
    return Number(process.hrtime.bigint() / 1000000n);
  }

  /** Nhịp tương đương (ms/lượt) — chỉ để hiển thị/chẩn đoán. */
  get paceMinMs() { return Math.round(1000 / Math.max(this.rate, 0.001)); }

  remainingNow(mono = QuotaModel.mono()) {
    if (this.remaining < 0) return -1;
    if (this.resetAtMono > 0 && mono >= this.resetAtMono) {
      return this.limit > 0 ? this.limit : -1;
    }
    return Math.max(0, this.remaining - this.spentSinceReport);
  }

  refill(mono = QuotaModel.mono()) {
    // Không tích token trong lúc đang bị phạt.
    const from = Math.max(this.lastRefillMono, Math.min(mono, this.blockedUntilMono));
    const dt = Math.max(0, mono - from);
    this.lastRefillMono = mono;
    this.tokens = Math.min(this.burst, this.tokens + (dt / 1000) * this.rate);
  }

  /** @returns {{ok:boolean, waitMs:number, why:string}} */
  check(mono = QuotaModel.mono()) {
    if (mono < this.blockedUntilMono) {
      return { ok: false, waitMs: this.blockedUntilMono - mono, why: "retry-after" };
    }
    if (this.learned && this.remaining >= 0) {
      const left = this.remainingNow(mono);
      if (left === 0 && this.resetAtMono > mono) {
        return { ok: false, waitMs: this.resetAtMono - mono, why: "window-exhausted" };
      }
    }
    this.refill(mono);
    if (this.tokens >= 1) return { ok: true, waitMs: 0, why: "" };
    return { ok: false, waitMs: Math.max(1, Math.ceil((1 - this.tokens) / this.rate * 1000)), why: "burst" };
  }

  /** Kế thừa trạng thái (leader cũ / snapshot). Chỉ nới, không rút ngắn cooldown. */
  seedFrom(state, { sameDomain = true } = {}, mono = QuotaModel.mono()) {
    if (!state || typeof state !== "object") return this;
    const blocked = Math.max(0, Number(state.blockedForMs) || 0);
    this.blockedUntilMono = Math.max(this.blockedUntilMono, mono + blocked);
    if (sameDomain) {
      if (Number(state.rate) > 0) {
        this.rate = Math.min(this.rateCeiling, Math.max(this.rateFloor, Number(state.rate)));
      }
      if (Number.isFinite(Number(state.tokens))) {
        this.tokens = Math.min(this.tokens, Math.max(0, Number(state.tokens)));
        // The seeded count is "now": restart the refill clock here. Left at the
        // model's creation time, the next refill() credited every second since
        // this follower started — a promoted leader began with a FULL burst,
        // one extra burst per leader change (measured: 12 grants in 3.5s where
        // one bucket allows ~10; 6 in a single second).
        this.lastRefillMono = Math.max(this.lastRefillMono, mono);
      }
      if (Number(state.latencyEwmaMs) > 0) this.latencyEwmaMs = Number(state.latencyEwmaMs);
      if (Number(state.limit) > 0) this.limit = Number(state.limit);
      if (Number.isFinite(Number(state.remaining)) && Number(state.remaining) >= 0) {
        this.remaining = Number(state.remaining);
        this.spentSinceReport = 0;
        this.learned = true;
      }
      if (Number(state.resetInMs) > 0) this.resetAtMono = mono + Number(state.resetInMs);
      if (Number(state.windowMs) > 0) this.windowMs = Number(state.windowMs);
    }
    return this;
  }

  forgetWindow() {
    this.limit = 0;
    this.remaining = -1;
    this.resetAtMono = 0;
    this.learned = false;
    this.spentSinceReport = 0;
    return this;
  }

  /**
   * Học từ phản hồi thật. 429 → rate ×0.5; 5xx → ×0.8; lỗi mạng (status 0)
   * KHÔNG phải bằng chứng về hạn mức nên không phạt. 2xx: sau ≥5 phản hồi tốt
   * và ≥3s kể từ lần chỉnh trước → rate ×1.25 (hồi nhanh, có trần).
   */
  observe({ status = 0, latencyMs = 0 } = {}, mono = QuotaModel.mono()) {
    const latency = Number(latencyMs) || 0;
    if (latency > 0) {
      this.latencyEwmaMs = this.latencyEwmaMs
        ? Math.round(this.latencyEwmaMs * 0.8 + latency * 0.2)
        : Math.round(latency);
    }
    if (status === 429 || status >= 500) {
      const factor = status === 429 ? 0.5 : 0.8;
      this.rate = Math.max(this.rateFloor, this.rate * factor);
      this.goodResponses = 0;
      this.lastAdjustMono = mono;
      return this.paceMinMs;
    }
    if (status >= 200 && status < 300) {
      this.goodResponses++;
      if (this.rate < this.startRps * 4 && this.goodResponses >= 5 &&
          mono - this.lastAdjustMono >= RECOVER_EVERY_MS) {
        this.rate = Math.min(this.rateCeiling, this.rate * 1.25);
        this.goodResponses = 0;
        this.lastAdjustMono = mono;
      }
    }
    return this.paceMinMs;
  }

  take(mono = QuotaModel.mono()) {
    const c = this.check(mono);
    if (!c.ok) return false;
    this.tokens -= 1;
    this.lastGrantMono = mono;
    if (this.learned && this.remaining >= 0) {
      if (this.resetAtMono > 0 && mono >= this.resetAtMono && this.limit > 0) {
        this.remaining = this.limit;
        this.spentSinceReport = 0;
        this.resetAtMono = mono + (this.windowMs || 60000);
      }
      this.spentSinceReport += 1;
    }
    return true;
  }

  learn(headers = {}, mono = QuotaModel.mono(), wallNow = Date.now()) {
    const num = v => {
      const n = Number(v);
      return Number.isFinite(n) ? n : null;
    };
    const pick = (...names) => {
      for (const n of names) {
        if (headers[n] !== undefined) return num(headers[n]);
        const lower = n.toLowerCase();
        if (headers[lower] !== undefined) return num(headers[lower]);
      }
      return null;
    };

    const limit = pick("x-ratelimit-limit", "X-RateLimit-Limit");
    const remaining = pick("x-ratelimit-remaining", "X-RateLimit-Remaining");
    const reset = pick("x-ratelimit-reset", "X-RateLimit-Reset");

    let touched = false;
    if (limit !== null && limit > 0) { this.limit = limit; touched = true; }
    if (reset !== null) {
      const deltaMs = reset > 1e9
        ? Math.max(0, reset * 1000 - wallNow)
        : Math.max(0, reset * 1000);
      this.resetAtMono = mono + deltaMs;
      this.windowMs = deltaMs > 0 ? deltaMs : (this.windowMs || 60000);
      touched = true;
    }
    if (remaining !== null) {
      this.remaining = remaining;
      this.spentSinceReport = 0;
      touched = true;
    }
    if (touched) {
      this.learned = true;
      this.lastServerMono = mono;
    }
    return this;
  }

  /**
   * Server bảo KEY NÀY chờ. Chờ đúng chừng đó — chỉ key này.
   *   Sau cooldown gáo còn đúng MỘT token (một lệnh thăm dò), rate đã giảm
   *   nửa; phản hồi tốt kế tiếp sẽ nâng lại.
   */
  penalize(retryAfterMs, mono = QuotaModel.mono()) {
    const wait = Math.max(0, Number(retryAfterMs) || 0);
    this.blockedUntilMono = Math.max(this.blockedUntilMono, mono + wait);
    this.remaining = -1;
    this.learned = false;
    this.spentSinceReport = 0;
    this.tokens = Math.min(this.tokens, 1);
    this.lastRefillMono = Math.max(this.lastRefillMono, this.blockedUntilMono);
    this.rateLimits++;
    this.observe({ status: 429 }, mono);
    return this.blockedUntilMono;
  }

  snapshot(mono = QuotaModel.mono()) {
    const c = this.check(mono);
    return {
      paceMinMs: this.paceMinMs,
      adaptive: true,
      rate: Math.round(this.rate * 100) / 100,
      burst: this.burst,
      tokens: Math.floor(this.tokens * 100) / 100,
      latencyEwmaMs: this.latencyEwmaMs,
      nextInMs: c.ok ? 0 : c.waitMs,
      windowMs: this.windowMs || 0,
      limit: this.limit,
      remaining: this.remainingNow(mono),
      resetInMs: this.resetAtMono > mono ? this.resetAtMono - mono : 0,
      blockedForMs: this.blockedUntilMono > mono ? this.blockedUntilMono - mono : 0,
      learned: this.learned,
      rateLimits: this.rateLimits,
      startRps: this.startRps
    };
  }
}
/**
 * Chia lượt giữa những bên cùng xin.
 *
 * Round-robin theo (tiến trình, chain). Không ưu tiên ai, và đó là điểm: một
 * cửa sổ ba dashboard không được ăn hết phần của cửa sổ một dashboard, còn
 * Ethereum không được bỏ đói Robinhood.
 */
class FairQueue {
  constructor() {
    /** lane -> hàng đợi yêu cầu */
    this.lanes = new Map();
    this.order = [];
    this.cursor = 0;
    /** Số lượt ưu tiên liên tiếp đã cấp trong khi còn yêu cầu thường chờ. */
    this.priorityStreak = 0;
  }

  push(lane, request) {
    if (!this.lanes.has(lane)) {
      this.lanes.set(lane, []);
      this.order.push(lane);
    }
    if (!request.at) request.at = Date.now();
    this.lanes.get(lane).push(request);
  }

  /**
   * ƯU TIÊN LÀ THỨ TỰ, KHÔNG PHẢI TỐC ĐỘ — VÀ KHÔNG PHẢI ĐỘC QUYỀN
   *
   *   Chính sách 3:1, đo được và dự đoán được: khi CẢ HAI lớp cùng chờ, tối
   *   đa 3 lượt liên tiếp cho lớp ưu tiên rồi 1 lượt cho yêu cầu THƯỜNG cũ
   *   nhất, rồi lặp. Chỉ một lớp chờ thì lớp đó dùng mọi lượt — không có lượt
   *   trống. Trong mỗi lớp: cũ nhất trước, xuyên mọi lane (mọi cửa sổ/chain),
   *   nên fairness là của broker, không phải của từng engine.
   *
   *   Soak 4 giờ trên bản chỉ có "aging 20s" đo được hàng thường chờ tới
   *   654s khi 3 hàng ưu tiên liên tục bị vượt giá: gom hàng thường đã già
   *   vào chung nhóm "khẩn" không phải là bảo đảm. 3:1 mới là bảo đảm; aging
   *   giữ lại như lớp bảo vệ phụ (một yêu cầu thường chờ quá AGING_MS được
   *   xếp vào lớp ưu tiên).
   *
   *   Không đụng tới KHI NÀO được gửi: nhịp gửi toàn cục ở QuotaModel quyết
   *   định điều đó; ở đây chỉ chọn AI đi tiếp.
   */
  size() {
    let n = 0;
    for (const q of this.lanes.values()) n += q.length;
    return n;
  }

  isUrgent(r, now) {
    return r.priority === true || (now - (r.at || now)) > FairQueue.AGING_MS;
  }

  /** Lấy yêu cầu CŨ NHẤT thoả điều kiện, xuyên mọi lane. */
  takeOldest(pred) {
    let best = null, bestLane = null, bestIndex = -1;
    for (const lane of this.order) {
      const q = this.lanes.get(lane);
      if (!q) continue;
      for (let i = 0; i < q.length; i++) {
        const r = q[i];
        if (!pred(r)) continue;
        if (!best || (r.at || 0) < (best.at || 0)) { best = r; bestLane = lane; bestIndex = i; }
      }
    }
    if (!best) return null;
    const q = this.lanes.get(bestLane);
    q.splice(bestIndex, 1);
    if (!q.length) {
      this.lanes.delete(bestLane);
      this.order = this.order.filter(l => l !== bestLane);
      if (this.cursor >= this.order.length) this.cursor = 0;
    }
    return best;
  }

  shift(now = Date.now(), eligible = () => true) {
    if (!this.order.length) return null;
    let hasUrgent = false, hasNormal = false;
    for (const q of this.lanes.values()) {
      for (const r of q) {
        if (!eligible(r)) continue;
        if (this.isUrgent(r, now)) hasUrgent = true; else hasNormal = true;
      }
      if (hasUrgent && hasNormal) break;
    }
    if (hasUrgent && hasNormal) {
      if (this.priorityStreak < FairQueue.PRIORITY_BURST) {
        this.priorityStreak++;
        return this.takeOldest(r => eligible(r) && this.isUrgent(r, now));
      }
      this.priorityStreak = 0;
      return this.takeOldest(r => eligible(r) && !this.isUrgent(r, now));
    }
    // Chỉ một lớp: không đếm chuỗi, không lượt trống.
    this.priorityStreak = 0;
    return this.takeOldest(eligible);
  }


  /**
   * GỠ MỘT YÊU CẦU ĐÃ HẾT HẠN / BỊ HUỶ KHỎI HÀNG — TRƯỚC KHI NÓ ĂN MỘT LƯỢT
   *
   *   Bản trước để yêu cầu hết hạn nằm lại trong hàng ("một giấy phép thừa
   *   là rẻ"). Điều đó đúng khi giấy phép là token 2/giây; với nhịp gửi
   *   mỗi giấy phép thì một yêu cầu ma vẫn ăn trọn một nhịp mà không
   *   ai gửi gì. Đo bằng mô phỏng: 12 yêu cầu sống + vài nhịp bị chặn (429 /
   *   cửa sổ cạn) → hết hạn → ma → ma ăn nhịp → thêm hết hạn → thêm ma. Sau
   *   một giờ, MỌI nhịp đều cấp cho ma và không NFT nào được gửi — đúng bức
   *   ảnh "Chờ lượt gửi" đứng mãi. Gỡ ngay khi hết hạn, và `pump` bỏ qua
   *   yêu cầu đã huỷ mà KHÔNG lấy nhịp.
   */
  remove(id, lane = null) {
    const lanes = lane ? [lane] : [...this.order];
    for (const l of lanes) {
      const q = this.lanes.get(l);
      if (!q) continue;
      const i = q.findIndex(r => r.id === id);
      if (i < 0) continue;
      const [r] = q.splice(i, 1);
      if (!q.length) {
        this.lanes.delete(l);
        this.order = this.order.filter(x => x !== l);
        if (this.cursor >= this.order.length) this.cursor = 0;
      }
      return r;
    }
    return null;
  }

  /** Yêu cầu cũ nhất còn chờ, để chẩn đoán "đợi từ bao giờ". */
  oldestAt() {
    let at = 0;
    for (const q of this.lanes.values()) for (const r of q) if (!at || (r.at || 0) < at) at = r.at || 0;
    return at;
  }

  /** Bỏ mọi yêu cầu của một lane (tiến trình chết, hoặc Reset). */
  dropLane(lane) {
    const q = this.lanes.get(lane) || [];
    this.lanes.delete(lane);
    this.order = this.order.filter(l => l !== lane);
    if (this.cursor >= this.order.length) this.cursor = 0;
    return q;
  }

  lanes_() { return [...this.order]; }
}

/** Một dòng JSON một bản tin. */
function frame(obj) { return JSON.stringify(obj) + "\n"; }

/**
 * Broker: leader giữ hạn mức, follower xin qua pipe.
 *
 * Cùng một lớp cho cả hai vai, vì vai có thể ĐỔI trong lúc chạy: leader đóng
 * cửa sổ thì một follower phải lên thay mà không ai phải khởi động lại.
 */
class QuotaBroker extends EventEmitter {
  /**
   * @param {object} options
   * @param {() => string} options.getApiKey  đọc key hiện tại; chỉ dùng để lấy vân tay
   * @param {(line:string)=>void} [options.onLog]
   * @param {number} [options.startRps]
   */
  constructor({ getApiKey, onLog, startRps = 2 } = {}) {
    super();
    this.getApiKey = typeof getApiKey === "function" ? getApiKey : () => "";
    this.onLog = typeof onLog === "function" ? onLog : () => {};
    this.startRps = startRps;

    this.fingerprint = "";
    this.pipePath = "";
    this.role = "none";              // "leader" | "follower" | "solo"
    this.server = null;
    this.client = null;

    this.model = new QuotaModel({ startRps });
    /** Một AIMD controller cho mỗi API-key fingerprint/quota domain. */
    this.models = new Map([["default", this.model]]);
    /**
     * 1.25.31 dashboard ceilings for order posting: Key 1 2/s, Key 2 1/s, and
     * OpenSea may share self-serve limits across an account's keys. Run at
     * 80%: per-key caps by position in the request's domain list, plus one
     * shared model every grant must also pass.
     */
    this.shared = new QuotaModel({ startRps: 2 * 0.9, burst: 1, rateCeiling: 2 * 0.9 });
    this.queue = new FairQueue();

    /** Yêu cầu của CHÍNH tiến trình này đang chờ trả lời (khi là follower). */
    this.pending = new Map();
    this.seq = 0;

    /** socket -> laneId, để dọn khi tiến trình đó chết. */
    this.clients = new Map();

    this.pumpTimer = null;
    /** Monotonic wall deadline of the armed one-shot pump. */
    this.pumpDueAt = 0;
    this.pumpingNow = false;
    this.soloTimer = null;
    this.reconnectTimer = null;
    this.closed = false;
    this.started = false;
    this.stats = { granted: 0, queuedPeak: 0, elections: 0, failovers: 0,
      timeouts: 0, cancelled: 0, ghostsSkipped: 0, repairs: 0, lostWakeups: 0, selfWakes: 0 };
    /**
     * LƯỚI AN TOÀN CỦA CHÍNH BROKER — 1 GIÂY, KHÔNG PHẢI 45
     *
     *   Đo trên log thật: leader, hàng chờ 6 yêu cầu, `next 0ms block 0ms`,
     *   không ai đang chờ trả lời — và 45 giây KHÔNG cấp giấy phép nào, cho
     *   tới khi watchdog của engine gọi `repair()`. Repair chỉ làm đúng một
     *   việc có ích: gọi lại `startPump()`. Tức là nhịp bơm đã mất mà vai vẫn
     *   là leader — một cú đánh thức bị rơi giữa hai lần đổi vai/đổi danh tính.
     *
     *   Nhịp này KHÔNG phải scheduler: nó chỉ kiểm bất biến "có việc + tới
     *   lượt ⇒ phải có bơm hoặc hẹn giờ", và tự sửa nếu sai. Rẻ: một lần đọc
     *   kích thước hàng mỗi giây.
     */
    this.invariantTimer = null;
    /** Lần cuối TIẾN TRÌNH NÀY nhận/cấp một giấy phép (đồng hồ tường, để chẩn đoán). */
    this.lastGrantAt = 0;
    /** Follower: lần cuối leader nói gì (grant/state/welcome). Im lặng quá lâu = socket chết. */
    this.lastLeaderMessageAt = 0;
  }

  log(line) { this.onLog(`[QUOTA] ${line}`); }

  modelFor(domain = "default") {
    const id = String(domain || "default");
    if (!this.models.has(id)) this.models.set(id, new QuotaModel({ startRps: this.startRps }));
    return this.models.get(id);
  }

  states() {
    const out = {};
    for (const [domain, model] of this.models) out[domain] = model.snapshot();
    return out;
  }

  /** Bắt đầu. Thử làm leader; không được thì làm follower. */
  async start() {
    this.closed = false;
    this.started = true;
    this.fingerprint = fingerprint(this.getApiKey());
    this.pipePath = pipePathFor(this.fingerprint);
    await this.electOrFollow();
    return this.role;
  }

  /**
   * Key pool vừa đổi: danh tính broker (tên pipe) phải đi theo key thật.
   *
   *   Bản trước tính fingerprint lúc khởi động, TRƯỚC khi pool key được nạp,
   *   nên mọi cửa sổ đều là `nokey` — chia sẻ đúng nhưng sai tên, và không bao
   *   giờ đổi khi người dùng đổi key. Khởi động lại broker khi fingerprint
   *   khác; yêu cầu đang chờ nhận `stopped`/`leader-stopping` và engine tự
   *   xếp lại — không ai ghi tự do trong lúc đó.
   */
  async refreshIdentity() {
    if (!this.started) return this.role;
    const next = fingerprint(this.getApiKey());
    if (next === this.fingerprint) return this.role;
    this.log(`key đổi: ${this.fingerprint} → ${next} · khởi động lại broker`);
    // Domain hạn mức khác: quên cửa sổ đã học; nhịp gửi và cooldown thì giữ —
    // chúng là của MÁY này đối với OpenSea, không phải của riêng key.
    for (const model of this.models.values()) model.forgetWindow();
    this.lastState = null;
    try { await this.stop(); } catch { /* đang đóng */ }
    return this.start();
  }

  async electOrFollow() {
    if (this.closed) return;
    const became = await this.tryBecomeLeader();
    if (became) return;
    const joined = await this.tryFollow();
    if (joined) return;

    // Không tạo được pipe VÀ không nối được: leader vừa chết giữa hai bước.
    // Thử lại sớm; trong lúc đó chạy solo với hạn mức thận trọng, vì thà chậm
    // còn hơn hai tiến trình cùng tưởng mình được ghi tự do.
    this.role = "solo";
    this.becomeAuthority("solo");
    this.startInvariantLoop();
    this.ensurePump("solo");
    this.log("chưa nối được broker — chạy solo thận trọng, sẽ thử lại");
    this.scheduleRetry(300);
  }

  tryBecomeLeader() {
    return new Promise(resolve => {
      const server = net.createServer(socket => this.onLeaderConnection(socket));
      const fail = () => { server.removeAllListeners(); try { server.close(); } catch {} resolve(false); };
      server.once("error", fail);
      server.listen(this.pipePath, () => {
        server.removeListener("error", fail);
        server.on("error", err => this.log(`server lỗi: ${err.message}`));
        /**
         * ĐÃ ĐÓNG THÌ KHÔNG LÊN LEADER
         *
         *   `stop()` đặt `closed` rồi mới dọn nhịp bơm. Một lần `listen` của
         *   lượt bầu TRƯỚC đó về muộn sẽ đặt vai = leader và bật nhịp bơm SAU
         *   khi stop đã dọn — kết quả là một leader không có nhịp bơm, đúng
         *   trạng thái đo được trên log. Trả server lại và rút lui.
         */
        if (this.closed) {
          try { server.close(); } catch { /* đang đóng */ }
          resolve(false);
          return;
        }
        this.server = server;
        this.role = "leader";
        this.stats.elections++;
        this.becomeAuthority("leader");
        this.log(`LEADER cho key ${this.fingerprint} · pid=${process.pid}`);
        this.startPump();
        this.startInvariantLoop();
        this.ensurePump("election");
        this.emit("role", "leader");
        resolve(true);
      });
    });
  }

  tryFollow() {
    return new Promise(resolve => {
      const socket = net.createConnection(this.pipePath);
      let settled = false;
      const done = ok => { if (!settled) { settled = true; resolve(ok); } };

      socket.once("error", () => { try { socket.destroy(); } catch {} done(false); });
      socket.once("connect", () => {
        this.client = socket;
        this.role = "follower";
        this.log(`FOLLOWER của broker key ${this.fingerprint} · pid=${process.pid}`);
        socket.setNoDelay(true);
        this.readLines(socket, line => this.onFollowerMessage(line));
        socket.on("close", () => this.onLeaderLost());
        this.emit("role", "follower");
        done(true);
      });
    });
  }

  /** Leader biến mất: bầu lại ngay, không đợi người dùng. */
  onLeaderLost() {
    if (this.closed) return;
    this.client = null;
    this.role = "none";
    this.stats.failovers++;
    this.log("leader biến mất — bầu lại");

    // Mọi yêu cầu đang chờ phải được trả lời, nếu không chúng treo vĩnh viễn
    // và token đó không bao giờ gửi được nữa. Trả bằng "hãy thử lại": thận
    // trọng, và không hứa một giấy phép mà không ai còn cấp được.
    for (const [, req] of this.pending) {
      req.resolve({ ok: false, waitMs: 250, why: "failover" });
    }
    this.pending.clear();
    this.emit("role", "none");
    this.electOrFollow().then(() => this.ensurePump("failover")).catch(() => {});
  }

  /**
   * Tiến trình này bắt đầu tự cấp phát (leader/solo): nhịp gửi phải sống qua
   * mọi chuyển vai. Kế thừa snapshot leader cũ nếu có (follower vừa lên leader,
   * hoặc start lại sau stop); không có gì thì mốc thận trọng: sớm nhất sau
   * paceMinMs kể từ bây giờ. Không bao giờ rút ngắn một mốc đã có.
   */
  becomeAuthority(role) {
    const mono = QuotaModel.mono();
    const inherited = Boolean(this.lastState || this.lastStates);
    if (this.lastState) this.model.seedFrom(this.lastState, { sameDomain: true }, mono);
    // Kế thừa cooldown/gáo của MỌI domain leader cũ đã phát (không chỉ default).
    if (this.lastStates && typeof this.lastStates === "object") {
      for (const [domain, snapshot] of Object.entries(this.lastStates)) {
        this.modelFor(domain).seedFrom(snapshot, { sameDomain: true }, mono);
      }
    }
    /**
     * A snapshot is only as fresh as the last message this follower got —
     * usually its `welcome`, from BEFORE the old leader spent its own tokens.
     * Seeded from it, a promoted leader started with a full burst on top of
     * the burst the old leader had just used (measured: 5 grants within 60ms
     * where one bucket allows 3). Promotion therefore keeps ONE probe token
     * per domain and refills from now; a first-ever leader keeps its burst.
     */
    if (inherited) {
      for (const model of this.models.values()) {
        model.tokens = Math.min(model.tokens, 1);
        model.lastRefillMono = Math.max(model.lastRefillMono, mono);
      }
    }
    const snap = this.model.snapshot(mono);
    this.log(`${role}: nhịp gửi kế thừa · lượt kế tiếp sau ${snap.nextInMs}ms · cooldown ${snap.blockedForMs}ms`);
  }

  scheduleRetry(ms) {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      if (!this.closed && this.role !== "leader" && this.role !== "follower") {
        this.electOrFollow();
      }
    }, ms);
    if (this.reconnectTimer.unref) this.reconnectTimer.unref();
  }

  // ---- vai LEADER -------------------------------------------------

  onLeaderConnection(socket) {
    socket.setNoDelay(true);
    const lane = `pid?:${this.clients.size}:${Date.now()}`;
    this.clients.set(socket, lane);

    this.readLines(socket, line => {
      let msg;
      try { msg = JSON.parse(line); } catch { return; }
      this.onLeaderMessage(socket, msg);
    });

    const cleanup = () => {
      const laneId = this.clients.get(socket);
      this.clients.delete(socket);
      if (laneId) {
        // Tiến trình đó chết: bỏ mọi yêu cầu của nó khỏi hàng chờ. Giữ lại là
        // cấp giấy phép cho một cửa sổ đã đóng, trong khi cửa sổ còn sống chờ.
        const dropped = this.queue.dropLane(laneId);
        if (dropped.length) this.log(`bỏ ${dropped.length} yêu cầu của lane đã đóng`);
      }
    };
    socket.on("close", cleanup);
    socket.on("error", cleanup);
  }

  onLeaderMessage(socket, msg) {
    if (!msg || !msg.type) return;

    if (msg.type === "hello") {
      const lane = `${msg.pid}:${msg.chain || "?"}`;
      this.clients.set(socket, lane);
      this.send(socket, { type: "welcome", state: this.model.snapshot(), states: this.states() });
      return;
    }

    if (msg.type === "request") {
      const lane = this.clients.get(socket) || `unknown:${Date.now()}`;
      // `at` của follower là mốc xếp hàng thật (đồng hồ tường cùng máy):
      // giữ chỗ qua một lần hết hạn. Không nhận mốc ở tương lai.
      const at = Number(msg.at) > 0 ? Math.min(Number(msg.at), Date.now()) : Date.now();
      this.queue.push(lane, {
        id: msg.id, socket, lane, domain: msg.domain || "default",
        domains: Array.isArray(msg.domains) ? msg.domains.map(String).slice(0, 8) : null,
        priority: msg.priority === true, at, cancelled: false,
        resolve: reply => this.send(socket, { type: "grant", id: msg.id, ...reply })
      });
      this.stats.queuedPeak = Math.max(this.stats.queuedPeak, this.queue.size());
      this.ensurePump("request");
      return;
    }

    if (msg.type === "report") {
      // Follower vừa nhận header thật từ OpenSea. Đó là thông tin duy nhất về
      // hạn mức thật, nên nó được áp cho MỌI tiến trình chứ không riêng nó.
      this.applyReport(msg);
      this.broadcast({ type: "state", state: this.model.snapshot(), states: this.states() });
      return;
    }

    if (msg.type === "cancel") {
      // Gỡ THẬT: một yêu cầu đã huỷ mà còn trong hàng sẽ ăn một nhịp adaptive
      // không ai dùng (xem FairQueue.remove).
      const lane = this.clients.get(socket) || null;
      const r = this.queue.remove(msg.id, lane) || this.queue.remove(msg.id);
      if (r) this.stats.cancelled++;
      // Gỡ đầu hàng cũng là một thay đổi: cái kế tiếp phải được đánh thức.
      this.ensurePump("cancel");
      return;
    }
  }

  applyReport(msg) {
    const model = this.modelFor(msg.domain);
    if (msg.headers) model.learn(msg.headers);
    // A 429 on a possibly shared account pool slows every key.
    if (msg.retryAfterMs || Number(msg.status) === 429) this.shared.observe({ status: 429 });
    if (msg.retryAfterMs) {
      model.penalize(msg.retryAfterMs);
      this.log(`429 → chặn domain ${msg.domain || "default"} ${msg.retryAfterMs}ms cho mọi tiến trình`);
    } else {
      model.observe({ status: Number(msg.status) || 0, latencyMs: Number(msg.latencyMs) || 0 });
    }
  }

  broadcast(obj) {
    for (const socket of this.clients.keys()) this.send(socket, obj);
  }

  send(socket, obj) {
    try { socket.write(frame(obj)); } catch { /* socket đã đóng */ }
  }

  /**
   * BẤT BIẾN: có việc + tới lượt ⇒ luôn có bơm đang chạy (hoặc đang bơm ngay).
   *
   *   Gọi sau MỌI lần hàng chờ đổi (thêm, gỡ, xếp lại) và mọi lần đổi vai.
   *   Rẻ và idempotent — `startPump` tự bỏ qua nếu đã có nhịp.
   */
  ensurePump(why = "") {
    if (this.closed) return false;
    if (this.role === "leader") {
      const armed = Boolean(this.pumpTimer);
      this.pump();
      if (!armed && why) this.stats.selfWakes++;
      return true;
    }
    if (this.role === "solo") { this.stats.selfWakes++; this.soloPump(); return true; }
    return false;
  }

  /** Nhịp kiểm bất biến; chỉ chạy khi broker đang sống. Xem `invariantTimer`. */
  startInvariantLoop() {
    if (this.invariantTimer) return;
    this.invariantTimer = setInterval(() => {
      if (this.closed) return;
      if (this.role !== "leader" && this.role !== "solo") return;
      if (!this.queue.size()) return;
      const due = this.nextQueueWait() === 0;
      const armed = this.role === "leader" && Boolean(this.pumpTimer) &&
        this.pumpDueAt > Date.now() - 100;
      if (!due) return;                       // chờ hợp lệ: nhịp/cooldown chưa tới
      if (armed) return;                      // leader đã có nhịp bơm
      // Tới lượt, có việc, mà không có bơm nào: đúng cú đánh thức bị rơi.
      this.stats.lostWakeups++;
      if (this.stats.lostWakeups <= 3 || this.stats.lostWakeups % 50 === 0) {
        this.log(`tự đánh thức: ${this.queue.size()} yêu cầu tới lượt mà không có nhịp bơm ` +
          `(vai=${this.role}, lần thứ ${this.stats.lostWakeups})`);
      }
      this.ensurePump("invariant");
    }, 1000);
    if (this.invariantTimer.unref) this.invariantTimer.unref();
  }

  startPump() {
    this.armPump(0);
  }

  /**
   * One-shot wakeup at the earliest legal grant time.
   *
   * A permanent interval can leave a truthy Timer handle after its wakeup was
   * lost/cancelled, so every caller believes a pump exists while the queue is
   * dead. A one-shot owns an explicit deadline, clears itself before pumping,
   * and is replaced whenever an earlier deadline appears.
   */
  armPump(waitMs = 0) {
    if (this.closed || this.role !== "leader" || !this.queue.size()) return false;
    const delay = Math.max(0, Math.ceil(Number(waitMs) || 0));
    const dueAt = Date.now() + delay;
    if (this.pumpTimer && this.pumpDueAt <= dueAt && this.pumpDueAt > Date.now() - 100) return true;
    if (this.pumpTimer) clearTimeout(this.pumpTimer);
    const timer = setTimeout(() => {
      if (this.pumpTimer !== timer) return;
      this.pumpTimer = null;
      this.pumpDueAt = 0;
      this.pump();
    }, delay);
    this.pumpTimer = timer;
    this.pumpDueAt = dueAt;
    timer.unref?.();
    return true;
  }

  /** Các domain (key) mà một yêu cầu chấp nhận ghi qua. */
  domainsOf(req) {
    return Array.isArray(req.domains) && req.domains.length ? req.domains : [req.domain || "default"];
  }

  /**
   * Domain ghi được NGAY cho yêu cầu này, hoặc null.
   *   Nhiều key cùng ghi được: key còn nhiều token hơn thắng (ít tải hơn),
   *   hoà thì key được cấp lâu nhất trước thắng — chia đều, không ưu tiên
   *   cố định Key 1.
   */
  capModel(model, index) {
    const i = Math.min(index, WRITE_CAPS.length - 1);
    if (model.capIndex === i) return model;
    const first = model.capIndex === undefined;
    model.capIndex = i;
    model.rateCeiling = WRITE_CAPS[i];
    model.startRps = WRITE_START[i];
    model.rate = first ? WRITE_START[i] : Math.min(model.rate, WRITE_CAPS[i]);
    model.burst = WRITE_BURSTS[i];
    model.tokens = Math.min(model.tokens, model.burst);
    return model;
  }

  pickDomain(req, mono = QuotaModel.mono()) {
    if (!this.shared.check(mono).ok) return null;
    let best = null, bestModel = null;
    const list = this.domainsOf(req);
    for (let i = 0; i < list.length; i++) {
      const domain = list[i];
      const model = this.capModel(this.modelFor(domain), i);
      if (!model.check(mono).ok) continue;
      if (!bestModel ||
          model.tokens > bestModel.tokens + 1e-9 ||
          (Math.abs(model.tokens - bestModel.tokens) <= 1e-9 && model.lastGrantMono < bestModel.lastGrantMono)) {
        best = domain; bestModel = model;
      }
    }
    return best;
  }

  /** Thời gian chờ ngắn nhất tới khi BẤT KỲ domain nào của yêu cầu ghi được. */
  waitFor(req, mono = QuotaModel.mono()) {
    const shared = this.shared.check(mono);
    const sharedWait = shared.ok ? 0 : Math.max(1, Number(shared.waitMs) || 20);
    let wait = Infinity;
    const list = this.domainsOf(req);
    for (let i = 0; i < list.length; i++) {
      const check = this.capModel(this.modelFor(list[i]), i).check(mono);
      if (check.ok) return sharedWait;
      wait = Math.min(wait, Math.max(1, Number(check.waitMs) || 20));
    }
    return Math.max(wait, sharedWait);
  }

  nextQueueWait() {
    let wait = Infinity;
    const mono = QuotaModel.mono();
    for (const q of this.queue.lanes.values()) {
      for (const req of q) {
        if (req.cancelled) continue;
        const w = this.waitFor(req, mono);
        if (w === 0) return 0;
        wait = Math.min(wait, w);
      }
    }
    return Number.isFinite(wait) ? wait : 20;
  }

  /** Cấp cho một yêu cầu: chọn domain + trừ gáo NGUYÊN TỬ. */
  grantOne(req) {
    const domain = this.pickDomain(req);
    if (!domain) return null;
    const model = this.modelFor(domain);
    if (!model.take()) return null;
    this.shared.take();
    this.stats.granted++;
    this.stats.grantsByDomain = this.stats.grantsByDomain || {};
    this.stats.grantsByDomain[domain] = (this.stats.grantsByDomain[domain] || 0) + 1;
    this.lastGrantAt = Date.now();
    req.granted = true;
    return { domain, model };
  }

  /**
   * Bơm cho tiến trình chạy MỘT MÌNH.
   *
   * Giống `pump()` nhưng không đòi vai leader, và tự hẹn lại khi hết token
   * — solo không có nhịp bơm chạy nền như leader.
   */
  soloPump() {
    if (this.role !== "solo") return;
    for (;;) {
      if (!this.queue.size()) return;
      const req = this.queue.shift(Date.now(), item => this.pickDomain(item) !== null);
      if (!req) {
        if (this.soloTimer) return;
        this.soloTimer = setTimeout(() => { this.soloTimer = null; this.soloPump(); },
          Math.max(5, Math.min(this.nextQueueWait(), 1000)));
        if (this.soloTimer.unref) this.soloTimer.unref();
        return;
      }
      if (req.cancelled) { this.stats.ghostsSkipped++; continue; }   // không lấy nhịp cho yêu cầu ma
      const got = this.grantOne(req);
      if (!got) { this.queue.push(req.lane, req); return; }
      req.resolve({ ok: true, waitMs: 0, domain: got.domain });
    }
  }

  pump() {
    if (this.role !== "leader") return;
    if (this.pumpingNow) return;
    this.pumpingNow = true;
    try {
      for (;;) {
        if (!this.queue.size()) return;
        const req = this.queue.shift(Date.now(), item => this.pickDomain(item) !== null);
        if (!req) return;
        if (req.cancelled) { this.stats.ghostsSkipped++; continue; }
        const got = this.grantOne(req);
        if (!got) { this.queue.push(req.lane, req); return; }
        req.resolve({ ok: true, waitMs: 0, state: got.model.snapshot(), domain: got.domain });
      }
    } finally {
      this.pumpingNow = false;
      // The queue itself owns its next wake. Every exit path re-arms it.
      if (this.queue.size()) this.armPump(this.nextQueueWait());
    }
  }

  // ---- vai FOLLOWER -----------------------------------------------

  onFollowerMessage(line) {
    let msg;
    try { msg = JSON.parse(line); } catch { return; }
    if (!msg || !msg.type) return;

    this.lastLeaderMessageAt = Date.now();
    if (msg.type === "grant") {
      if (msg.state) this.lastState = msg.state;   // mốc mới nhất của leader
      if (msg.states) this.lastStates = msg.states;
      const req = this.pending.get(msg.id);
      if (!req) return;
      this.pending.delete(msg.id);
      if (msg.ok !== false) this.lastGrantAt = Date.now();
      req.resolve({ ok: msg.ok !== false, waitMs: msg.waitMs || 0, why: msg.why || "", domain: msg.domain || "" });
      return;
    }
    if (msg.type === "welcome" || msg.type === "state") {
      // Giữ bản sao để hiển thị và để chạy thận trọng nếu mất leader.
      this.lastState = msg.state || null;
      this.lastStates = msg.states || this.lastStates || null;
      if (msg.states && typeof msg.states === "object") {
        for (const [domain, snapshot] of Object.entries(msg.states)) {
          this.modelFor(domain).seedFrom(snapshot, { sameDomain: true });
        }
      }
      this.emit("state", this.lastState);
    }
  }

  // ---- API cho engine ---------------------------------------------

  /**
   * Xin một giấy phép ghi. Chờ cho tới khi được, hoặc tới khi hết hạn chờ.
   *
   * @param {object} opts
   * @param {string} opts.lane  "chain" của bên xin, để chia lượt công bằng
   * @param {number} [opts.timeoutMs]
   * @returns {Promise<{ok:boolean, waitedMs:number, why:string}>}
   */
  async acquire({ lane = "?", domain = "default", domains = null, timeoutMs = 30000, priority = false, at = 0, handle = null } = {}) {
    const list = Array.isArray(domains) && domains.length ? [...new Set(domains.map(String))] : null;
    const started = Date.now();
    /**
     * `at`      giữ CHỖ trong hàng qua một lần hết hạn: bên xin xếp lại với
     *            đúng mốc cũ, không bị đẩy xuống cuối sau mỗi 120 giây chờ.
     * `handle`  bên xin giữ để huỷ sớm (watchdog): `handle.cancel(why)` gỡ
     *            yêu cầu khỏi hàng và trả lời ngay — không giữ Promise vô hạn.
     */
    const queuedAt = Number(at) > 0 ? Number(at) : Date.now();

    /**
     * LEADER XẾP HÀNG NHƯ MỌI NGƯỜI
     *
     *   Bản đầu cho leader lấy thẳng từ mô hình trong một vòng lặp chặt, còn
     *   follower phải gửi yêu cầu qua pipe rồi chờ `pump()`. Hai đường khác
     *   nhau cho cùng một hạn mức, và đường của leader nhanh hơn — nên leader
     *   ăn hết token trước khi yêu cầu của follower kịp tới.
     *
     *   Đo bằng test sáu engine: cửa sổ A (leader) gửi 4 offer, B và C gửi 0.
     *   Không phải chậm hơn — là KHÔNG BAO GIỜ tới lượt.
     *
     *   Nên leader đẩy yêu cầu của chính nó vào ĐÚNG hàng chờ đó, với lane
     *   riêng của nó. `FairQueue` luân phiên giữa các lane, nên ba cửa sổ chia
     *   đều. Với tiến trình đơn ("solo") thì hàng chờ rỗng và rút ngay, nên
     *   không mất gì.
     */
    if (this.role === "leader" || this.role === "solo") {
      const myLane = `${process.pid}:${lane}`;
      const reply = await new Promise(resolve => {
        const id = ++this.seq;
        const req = {
          id, lane: myLane, domain: list ? list[0] : domain, domains: list, priority: priority === true, at: queuedAt, cancelled: false,
          resolve: r => { clearTimeout(timer); resolve(r); }
        };
        // Hết hạn / bị huỷ: GỠ khỏi hàng ngay, để nó không ăn một nhịp.
        const giveUp = why => {
          if (req.cancelled || req.granted) return;
          req.cancelled = true;
          this.queue.remove(id, myLane);
          if (why === "timeout") this.stats.timeouts++; else this.stats.cancelled++;
          req.resolve({ ok: false, why });
        };
        const timer = setTimeout(() => giveUp("timeout"), timeoutMs);
        if (timer.unref) timer.unref();
        if (handle && typeof handle === "object") handle.cancel = why => giveUp(why || "cancelled");
        this.queue.push(myLane, req);
        this.stats.queuedPeak = Math.max(this.stats.queuedPeak, this.queue.size());
        this.ensurePump("acquire");
      });
      return { ok: reply.ok === true, waitedMs: Date.now() - started,
        why: reply.why || "", domain: reply.domain || (list ? "" : domain) };
    }

    if (this.role !== "follower" || !this.client) {
      // Chưa nối được. Chờ ngắn rồi để bên gọi thử lại — KHÔNG ghi tự do.
      return { ok: false, waitedMs: Date.now() - started, why: "no-broker" };
    }

    const id = ++this.seq;
    const reply = await new Promise(resolve => {
      const giveUp = why => {
        if (!this.pending.has(id)) return;
        this.pending.delete(id);
        // Bảo leader gỡ nó khỏi hàng — nếu không nó ăn một nhịp bên đó.
        if (this.client) this.send(this.client, { type: "cancel", id });
        if (why === "timeout") this.stats.timeouts++; else this.stats.cancelled++;
        resolve({ ok: false, waitMs: 0, why });
      };
      const timer = setTimeout(() => {
        /**
         * SOCKET CHẾT MÀ KHÔNG BÁO
         *
         *   Hết hạn chờ mà leader không nói gì suốt cả quãng đó (không grant,
         *   không state) thì không phải hàng dài — là ống đã chết mà 'close'
         *   chưa tới. Đóng nó: `onLeaderLost` bầu lại ngay.
         */
        const quiet = Date.now() - (this.lastLeaderMessageAt || 0);
        giveUp("timeout");
        if (this.client && quiet >= timeoutMs) {
          this.log(`leader im lặng ${Math.round(quiet / 1000)}s trong lúc chờ — coi ống là chết, bầu lại`);
          try { this.client.destroy(); } catch { /* đã đóng */ }
        }
      }, timeoutMs);
      if (timer.unref) timer.unref();
      this.pending.set(id, {
        resolve: r => { clearTimeout(timer); resolve(r); }
      });
      if (handle && typeof handle === "object") handle.cancel = why => { clearTimeout(timer); giveUp(why || "cancelled"); };
      this.send(this.client, { type: "request", id, lane, domain: list ? list[0] : domain, domains: list, priority: priority === true, at: queuedAt });
    });

    return { ok: reply.ok === true, waitedMs: Date.now() - started, why: reply.why || "",
      domain: reply.domain || (list ? "" : domain) };
  }

  /**
   * Báo lại những gì server vừa nói. Bắt buộc gọi sau MỌI phản hồi ghi.
   *
   * Đây là cách duy nhất mô hình hạn mức học được sự thật, và là cách một 429
   * ở cửa sổ A dừng được cửa sổ B và C trước khi chúng ăn thêm 429.
   */
  report({ domain = "default", headers = null, retryAfterMs = 0, status = 0, latencyMs = 0 } = {}) {
    if (this.role === "leader" || this.role === "solo") {
      this.applyReport({ domain, headers, retryAfterMs, status, latencyMs });
      if (this.role === "leader") {
        this.broadcast({ type: "state", state: this.model.snapshot(), states: this.states() });
      }
      // Sau 429 có cooldown; hết cooldown phải có người bơm lại. Nhịp bơm của
      // leader lo việc đó, solo thì tự hẹn — cả hai đi qua đây.
      this.ensurePump("report");
      return;
    }
    if (this.role === "follower" && this.client) {
      this.send(this.client, { type: "report", domain, headers, retryAfterMs, status, latencyMs });
    }
  }

  /** Nói với leader mình là ai, để chia lượt theo tiến trình + chain. */
  hello(chain) {
    if (this.role === "follower" && this.client) {
      this.send(this.client, { type: "hello", pid: process.pid, chain });
    }
  }

  status() {
    const model = this.role === "leader" || this.role === "solo"
      ? this.model.snapshot() : (this.lastState || null);
    return {
      role: this.role,
      fingerprint: this.fingerprint,
      queued: this.queue.size(),
      pending: this.pending.size,
      oldestQueuedAt: this.queue.oldestAt(),
      lanes: this.queue.lanes_().length,
      clients: this.clients.size,
      connected: this.role === "leader" || this.role === "solo" ||
        (this.role === "follower" && Boolean(this.client)),
      lastGrantAt: this.lastGrantAt,
      lastLeaderMessageAt: this.lastLeaderMessageAt,
      pumping: this.pumpingNow || Boolean(this.pumpTimer),
      pumpDueInMs: this.pumpDueAt ? Math.max(0, this.pumpDueAt - Date.now()) : 0,
      invariantArmed: Boolean(this.invariantTimer),
      model,
      models: this.role === "leader" || this.role === "solo" ? this.states() : (this.lastStates || null),
      ...this.stats
    };
  }

  /**
   * TỰ CHỮA KHI ENGINE THẤY KHÔNG CÓ TIẾN TRIỂN
   *
   *   Engine gọi khi có việc chờ mà không có giấy phép nào tới quá lâu (ngoài
   *   cooldown hợp lệ). Mỗi vai một cách chữa, và cách nào cũng không cấp
   *   thêm giấy phép ngoài nhịp:
   *     leader   nhịp bơm phải đang chạy; dọn yêu cầu ma; bơm ngay
   *     solo     bầu lại (có thể leader đã có), bơm ngay
   *     follower ống im lặng quá lâu → đóng, bầu lại; còn nói thì bơm nhẹ
   *     none     bầu lại
   * @returns {string} việc đã làm
   */
  repair(reason = "no-progress") {
    if (this.closed) return "closed";
    this.stats.repairs++;
    const snap = this.status();
    this.log(`repair (${reason}) · vai=${this.role} · hàng=${snap.queued} · chờ trả lời=${snap.pending} · ` +
      `grant cuối ${this.lastGrantAt ? Math.round((Date.now() - this.lastGrantAt) / 1000) + "s trước" : "chưa có"} · ` +
      `model=${snap.model ? `next ${snap.model.nextInMs}ms block ${snap.model.blockedForMs}ms` : "-"}`);
    if (this.role === "leader") {
      // Dọn ma còn sót (yêu cầu đã huỷ mà remove chưa tới) và chắc nhịp bơm sống.
      for (const lane of this.queue.lanes_()) {
        const q = this.queue.lanes.get(lane) || [];
        for (const r of [...q]) if (r.cancelled) this.queue.remove(r.id, lane);
      }
      this.startInvariantLoop();
      this.ensurePump("repair");
      return "leader-pumped";
    }
    if (this.role === "solo") { this.soloPump(); this.scheduleRetry(50); return "solo-reelect"; }
    if (this.role === "follower") {
      const quiet = Date.now() - (this.lastLeaderMessageAt || 0);
      if (!this.client || quiet > 60000) {
        this.log(`follower: leader im lặng ${Math.round(quiet / 1000)}s — đóng ống, bầu lại`);
        if (this.client) { try { this.client.destroy(); } catch { /* đã đóng */ } }
        else this.onLeaderLost();
        return "follower-reelect";
      }
      return "follower-alive";
    }
    this.electOrFollow();
    return "reelect";
  }

  /**
   * Đóng broker này.
   *
   * MỘT LEADER ĐANG ĐÓNG PHẢI NHẢ FOLLOWER RA TRƯỚC
   *
   *   `server.close(cb)` chỉ gọi lại khi MỌI kết nối đã đóng. Leader còn hai
   *   follower đang nối thì nó chờ mãi. Đo được bằng test ba tiến trình: cửa
   *   sổ leader treo hẳn lúc thoát, và vì `pumpTimer` đã bị dừng trước đó nên
   *   hai cửa sổ kia không được cấp giấy phép nào nữa — chúng chờ hết 15 giây
   *   rồi mới hết hạn. Đóng một cửa sổ làm đứng hai cửa sổ còn lại.
   *
   *   Nên trình tự ở đây có chủ đích: bơm nốt hàng chờ, ngắt follower để
   *   chúng bầu lại NGAY, rồi mới đóng server.
   */
  async stop() {
    this.closed = true;
    // Ghi lại mốc cuối để lần start() kế tiếp (cùng key) kế thừa — kể cả khi
    // một đoạn mã sau này thay mô hình.
    try { this.lastState = this.model.snapshot(); } catch { /* không có gì để giữ */ }
    if (this.reconnectTimer) { clearTimeout(this.reconnectTimer); this.reconnectTimer = null; }

    // Bơm nốt những gì còn cấp được. Cái gì không cấp được thì trả lời dứt
    // khoát, để bên xin đi tiếp thay vì chờ hết hạn.
    if (this.role === "leader") {
      try { this.pump(); } catch { /* đang đóng, không quan trọng */ }
      for (;;) {
        const req = this.queue.shift();
        if (!req) break;
        req.resolve({ ok: false, waitMs: 0, why: "leader-stopping" });
      }
    }

    if (this.pumpTimer) { clearTimeout(this.pumpTimer); this.pumpTimer = null; }
    if (this.soloTimer) { clearTimeout(this.soloTimer); this.soloTimer = null; }
    this.pumpDueAt = 0;
    if (this.invariantTimer) { clearInterval(this.invariantTimer); this.invariantTimer = null; }

    for (const [, req] of this.pending) req.resolve({ ok: false, waitMs: 0, why: "stopped" });
    this.pending.clear();

    if (this.client) { try { this.client.destroy(); } catch {} this.client = null; }

    if (this.server) {
      // Ngắt follower TRƯỚC. Chúng thấy socket đóng, chạy onLeaderLost, và
      // bầu lại trong vài chục mili giây thay vì chờ hết hạn.
      for (const socket of [...this.clients.keys()]) {
        try { socket.destroy(); } catch { /* đã đóng */ }
      }
      this.clients.clear();

      await new Promise(resolve => {
        let done = false;
        const finish = () => { if (!done) { done = true; resolve(); } };
        // Vẫn đặt hạn: một socket kẹt ở tầng hệ điều hành không được phép giữ
        // cả tiến trình lại lúc thoát.
        const timer = setTimeout(finish, 1000);
        if (timer.unref) timer.unref();
        try { this.server.close(() => { clearTimeout(timer); finish(); }); }
        catch { clearTimeout(timer); finish(); }
      });
      this.server = null;
    }
    this.role = "none";
  }

  /** Đọc socket theo dòng. Một bản tin một dòng. */
  readLines(socket, onLine) {
    let buffer = "";
    socket.on("data", chunk => {
      buffer += chunk.toString("utf8");
      let at;
      while ((at = buffer.indexOf("\n")) >= 0) {
        const line = buffer.slice(0, at);
        buffer = buffer.slice(at + 1);
        if (line.trim()) onLine(line);
      }
      // Một bên nói bậy không được phép làm phình bộ nhớ vô hạn.
      if (buffer.length > 1_000_000) buffer = "";
    });
  }
}

FairQueue.AGING_MS = 20000;
/** Tối đa bấy nhiêu lượt ưu tiên liên tiếp khi còn yêu cầu thường đang chờ (3:1). */
FairQueue.PRIORITY_BURST = 3;

module.exports = { QuotaBroker, QuotaModel, FairQueue, fingerprint, pipePathFor,
  PACE_MIN_MS, PACE_JITTER_MS };
