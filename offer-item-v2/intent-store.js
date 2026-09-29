"use strict";

/**
 * intent-store.js — mỗi NFT có ĐÚNG MỘT ý định gửi, luôn là cái mới nhất.
 *
 * VÌ SAO KHÔNG PHẢI HÀNG ĐỢI
 *
 *   Engine cũ xếp hàng công việc. Với một token bị đấu giá liên tục, hàng đợi
 *   đó là hàng đợi những GIÁ ĐÃ CŨ: đối thủ nhích hai mươi lần trong lúc chờ
 *   quota, và khi tới lượt thì bot ký hai mươi order, mười chín cái trong đó
 *   thấp hơn giá hiện tại và không cái nào có ích. Đó vừa là tiền phí, vừa là
 *   quota bị tiêu cho công việc đã hết giá trị.
 *
 *   Ở đây mỗi token có một ô. Sự kiện thứ hai mươi GHI ĐÈ lên ô đó. Khi giấy
 *   phép ghi tới, thứ được ký là trạng thái mới nhất — một order, đúng giá.
 *
 * SINGLE-FLIGHT THEO TOKEN, KHÔNG PHẢI TOÀN CỤC
 *
 *   Một token chỉ được có một submit đang bay: hai cái sẽ thành hai offer cho
 *   cùng một NFT. Nhưng token KHÁC NHAU phải chạy song song — engine cũ có
 *   một semaphore chung, nên một NFT có HTTP chậm là hai mươi NFT khác đứng
 *   chờ. Khoá ở đây là khoá theo token, và chỉ có thế.
 *
 * GENERATION
 *
 *   Mỗi ý định mang generation của sổ lúc nó được tạo. Một submit quay về
 *   muộn so generation của mình với generation hiện tại: khác nhau nghĩa là
 *   sổ đã đổi trong lúc nó bay, và nó không được phép kết luận gì về trạng
 *   thái hiện tại.
 */

/** Trạng thái của một ý định. */
const INTENT = Object.freeze({
  IDLE: "IDLE",           // không có gì để gửi
  READY: "READY",         // có target, đang chờ tới lượt
  // Có target hợp lệ nhưng một phụ thuộc CỤC BỘ chưa sẵn (template, own
  // authority của hàng lạnh / sau POST mơ hồ). Ý định KHÔNG biến mất: phụ
  // thuộc sẵn sàng thì chính nơi làm nó sẵn sàng tính lại hàng → READY.
  WAITING: "WAITING",
  BUILDING: "BUILDING",   // đang dựng + ký
  GRANTING: "GRANTING",   // đã ký, đang xin giấy phép ghi
  SENDING: "SENDING",     // đang POST
  DONE: "DONE",
  RETRY: "RETRY",         // lỗi tạm thời, đang chờ tới giờ thử lại (engine hẹn)
  FAILED: "FAILED"
});

class IntentStore {
  constructor({ onLog } = {}) {
    /** @type {Map<string, object>} tokenKey -> intent */
    this.intents = new Map();
    /** tokenKey đang có việc bay. Khoá theo TOKEN, không toàn cục. */
    this.inFlight = new Set();
    this.onLog = typeof onLog === "function" ? onLog : () => {};
    this.stats = { set: 0, overwritten: 0, sent: 0, skippedStale: 0, failed: 0 };
  }

  /**
   * Đặt hoặc GHI ĐÈ ý định của một token.
   *
   * Ghi đè là hành vi bình thường, không phải lỗi: nó chính là latest-wins.
   */
  set(tokenKey, { target, best, mine, generation, reason, at = Date.now() }) {
    const existing = this.intents.get(tokenKey);
    if (existing && existing.state !== INTENT.IDLE) this.stats.overwritten++;

    /**
     * CHỖ TRONG HÀNG SỐNG QUA LATEST-WINS
     *
     *   Ghi đè target là bình thường, nhưng ghi đè MỐC XẾP HÀNG thì không:
     *   một token bị vượt giá liên tục được tính lại mỗi vài giây, và nếu mỗi
     *   lần tính lại là một mốc mới thì nó luôn là "mới nhất" — luôn đứng
     *   cuối khi `pump` sắp theo mốc, và luôn xuống cuối hàng ở broker. Chỗ
     *   trong hàng thuộc về TOKEN đang chờ, không thuộc về con số target.
     */
    const waiting = existing && existing.target &&
      existing.state !== INTENT.DONE && existing.state !== INTENT.FAILED && existing.state !== INTENT.IDLE;

    const intent = {
      tokenKey,
      target,
      best,
      mine,
      generation,
      reason: reason || "",
      // Thời điểm SỰ KIỆN sinh ra ý định này. Mọi số đo độ trễ P0 đều tính từ
      // đây, nên nó phải là thời điểm sự kiện tới chứ không phải lúc dựng.
      firstSeenAt: existing && existing.target ? existing.firstSeenAt : at,
      /** Từ lúc nào token này chờ tới lượt (giữ qua mọi lần tính lại). */
      queuedAt: waiting ? (existing.queuedAt || existing.firstSeenAt || at) : at,
      /** Mốc trạng thái gần nhất đổi — watchdog phân biệt "chờ" với "treo". */
      stateChangedAt: at,
      updatedAt: at,
      state: INTENT.READY,
      attempts: existing ? existing.attempts : 0,
      lastError: ""
    };
    this.intents.set(tokenKey, intent);
    this.stats.set++;
    return intent;
  }

  /** Không còn gì để gửi cho token này. */
  clear(tokenKey) {
    const intent = this.intents.get(tokenKey);
    if (!intent) return false;
    // KHÔNG xoá khi đang bay: khung async đang giữ nó, và xoá ở đây sẽ để một
    // submit thứ hai của cùng token đi qua.
    if (this.inFlight.has(tokenKey)) {
      intent.target = 0;
      intent.state = INTENT.IDLE;
      return false;
    }
    this.intents.delete(tokenKey);
    return true;
  }

  get(tokenKey) { return this.intents.get(tokenKey) || null; }

  /**
   * Những token SẴN SÀNG gửi ngay bây giờ.
   *
   * Không sắp theo thời gian chờ. Sắp theo target giảm dần thì một token đắt
   * luôn chen trước; sắp theo thứ tự sẵn sàng thì công bằng và đủ — thứ quyết
   * định thông lượng là quota, không phải thứ tự trong danh sách này.
   */
  ready(now = Date.now()) {
    const out = [];
    for (const intent of this.intents.values()) {
      if (intent.state !== INTENT.READY) continue;
      if (!intent.target) continue;
      if (this.inFlight.has(intent.tokenKey)) continue;
      // Vừa bị broker từ chối tức thì (chưa có broker / đang bầu lại): chờ
      // một chút trước khi xin lại. Không có mốc này thì `finally` của lượt
      // vừa xong bơm lại NGAY, xin lại NGAY, bị từ chối NGAY — một vòng
      // setImmediate quay chặt (soak đo được 2,3 triệu lượt xin lại trong vài
      // giây broker khởi động lại).
      if (intent.notBefore && intent.notBefore > now) continue;
      out.push(intent);
    }
    return out;
  }

  /**
   * Nhận khoá single-flight của token. `false` nghĩa là đã có cái đang bay.
   */
  acquire(tokenKey) {
    if (this.inFlight.has(tokenKey)) return false;
    this.inFlight.add(tokenKey);
    return true;
  }

  /**
   * Nhả khoá.
   *
   * Luôn gọi trong `finally`. Không nhả ở đây nghĩa là token đó chết hẳn cho
   * tới khi khởi động lại — không có bộ đếm giờ nào cứu nó, và đó là chủ ý:
   * một khoá tự hết hạn sẽ cho phép cái submit thứ hai đi trong lúc cái thứ
   * nhất còn bay.
   */
  release(tokenKey) { this.inFlight.delete(tokenKey); }

  isInFlight(tokenKey) { return this.inFlight.has(tokenKey); }

  setState(tokenKey, state, extra = {}) {
    const intent = this.intents.get(tokenKey);
    if (!intent) return null;
    if (intent.state !== state) intent.stateChangedAt = Date.now();
    intent.state = state;
    Object.assign(intent, extra);
    if (state === INTENT.DONE) this.stats.sent++;
    if (state === INTENT.FAILED) this.stats.failed++;
    return intent;
  }

  /**
   * Ý định này còn nói đúng trạng thái hiện tại không?
   *
   * Gọi NGAY TRƯỚC khi ký. Giữa lúc xếp hàng và lúc tới lượt, sổ có thể đã
   * đổi nhiều lần; ký theo con số cũ là gửi một offer đã lỗi thời ngay khi nó
   * rời khỏi máy.
   */
  isCurrent(tokenKey, generation) {
    const intent = this.intents.get(tokenKey);
    if (!intent) return false;
    return intent.generation === generation;
  }

  /**
   * Dọn những ý định đã kết thúc và đã nguội.
   *
   * Số ý định vốn đã bị chặn theo số NFT đang theo dõi, nên đây không phải là
   * chống rò bộ nhớ — nó là dọn rác cho một tiến trình chạy nhiều ngày: một
   * NFT gửi xong rồi im lặng suốt buổi vẫn giữ bản ghi DONE của nó mãi, cùng
   * với lỗi cuối và mọi con số kèm theo.
   *
   * KHÔNG BAO GIỜ đụng tới ý định đang bay. Xoá bản ghi của một token còn
   * submit trong không trung là mở đường cho cái thứ hai của cùng token đi
   * qua — đúng thứ single-flight tồn tại để chặn.
   *
   * @param {number} now
   * @param {number} olderThanMs
   * @returns {number} số bản ghi đã dọn
   */
  sweep(now = Date.now(), olderThanMs = 5 * 60 * 1000) {
    let removed = 0;
    for (const [key, intent] of this.intents) {
      if (this.inFlight.has(key)) continue;
      const terminal = intent.state === INTENT.DONE ||
        intent.state === INTENT.FAILED ||
        (intent.state === INTENT.IDLE && !intent.target);
      if (!terminal) continue;
      if (now - intent.updatedAt < olderThanMs) continue;
      this.intents.delete(key);
      removed++;
    }
    return removed;
  }

  size() { return this.intents.size; }

  census() {
    const byState = {};
    for (const intent of this.intents.values()) {
      byState[intent.state] = (byState[intent.state] || 0) + 1;
    }
    return { intents: this.intents.size, inFlight: this.inFlight.size, byState,
      ...this.stats };
  }

  /** Reset khẩn cấp: quên mọi ý định CHƯA bay. Cái đang bay tự nhả. */
  reset() {
    for (const key of [...this.intents.keys()]) {
      if (!this.inFlight.has(key)) this.intents.delete(key);
      else this.setState(key, INTENT.IDLE, { target: 0 });
    }
  }
}

module.exports = { IntentStore, INTENT };
