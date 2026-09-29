"use strict";

/**
 * submitter.js — đường từ "đã có giá" tới "OpenSea đã nhận".
 *
 * TRÌNH TỰ, VÀ KHÔNG CÓ GÌ KHÁC TRONG ĐÓ
 *
 *   ý định mới nhất
 *   → đọc lại trạng thái cục bộ
 *   → dựng order cục bộ
 *   → ký cục bộ
 *   → xin giấy phép ghi từ broker
 *   → POST
 *
 *   Không REST GET. Không RPC preflight. Không lane thứ hai. Không limiter
 *   thứ hai. Kiến trúc cũ có ba cổng nối tiếp cho cùng một hạn mức — submit
 *   lane, bộ giới hạn ORDER, rồi hàng chờ rate-limit — nên một offer xếp hàng
 *   ba lần cho một quyền duy nhất, và đo được lane_queued 11,5 giây trên một
 *   submit mà HTTP chỉ mất 456ms.
 *
 * HAI LẦN KIỂM LẠI, KHÔNG PHẢI MỘT
 *
 *   Ngay TRƯỚC KHI KÝ: trạng thái còn đúng không? Ký theo con số đã cũ là
 *   tiêu một chữ ký cho một giá không còn ý nghĩa.
 *
 *   Ngay TRƯỚC KHI GỬI: còn đúng không? Giữa lúc ký và lúc được cấp quota có
 *   thể trôi qua nhiều giây, và trong nhiều giây đó đối thủ đã nhích tiếp.
 *   Gửi một order đã lỗi thời còn tệ hơn không gửi: nó tiêu quota và tạo một
 *   offer sẽ phải huỷ.
 *
 * KẾT NỐI GIỮ MỞ
 *
 *   Một agent HTTPS dùng chung, keep-alive, hâm nóng lúc Start. Bắt tay TLS
 *   mỗi lần POST là 100-300ms cộng thẳng vào đúng đoạn đang cố rút ngắn.
 */

const https = require("https");
const { apiKeys } = require("../rate-limiter");
const { URL } = require("url");

const OPENSEA_API_BASE = "https://api.opensea.io/api/v2";

/**
 * Pool kết nối riêng của Offer Item V2.
 *
 * RIÊNG, không dùng chung với Offer SLL hay Cancel: một lượt bulk 50 NFT sẽ
 * chiếm hết socket và đẩy một offer P0 vào hàng chờ của tầng HTTP — một cổng
 * nghẽn nữa mà không ai đo được vì nó nằm trong thư viện.
 */
class HttpPool {
  constructor({ maxSockets = 8, timeoutMs = 20000 } = {}) {
    this.timeoutMs = timeoutMs;
    this.agent = new https.Agent({
      keepAlive: true,
      keepAliveMsecs: 15000,
      maxSockets,
      maxFreeSockets: maxSockets,
      // Không để một socket rỗi bị phía kia đóng lặng lẽ rồi lần POST sau mới
      // phát hiện: 60 giây ngắn hơn hầu hết idle timeout của CDN.
      scheduling: "fifo"
    });
    this.stats = { requests: 0, reused: 0, created: 0, errors: 0, timeouts: 0 };

  }

  /** Mở sẵn kết nối lúc Start, để offer đầu tiên không phải bắt tay TLS. */
  async warmUp(apiKey) {
    const started = Date.now();
    try {
      await this.request({
        method: "GET",
        path: "/collections?limit=1",
        apiKey,
        timeoutMs: 8000
      });
      return { ok: true, ms: Date.now() - started };
    } catch (error) {
      return { ok: false, ms: Date.now() - started, error: String(error.message) };
    }
  }

  /**
   * Một request. Trả cả header, vì header hạn mức là thứ broker phải học.
   *
   * @returns {Promise<{status:number, headers:object, body:any,
   *                    firstByteMs:number, totalMs:number}>}
   */
  request({ method = "GET", path, body = null, apiKey = "", signal = null,
    timeoutMs = this.timeoutMs, onStage = null } = {}) {
    const url = new URL(OPENSEA_API_BASE + path);
    const payload = body === null ? null : Buffer.from(JSON.stringify(body), "utf8");
    const stage = name => { if (onStage) { try { onStage(name); } catch {} } };

    return new Promise((resolve, reject) => {
      const startedAt = Date.now();
      const lease = apiKeys.leaseKey(apiKey, {write: method === "POST"});
      let deadline, abort;
      let firstByteAt = 0;
      let settled = false;
      // Body trọn vẹn đã giao cho hệ điều hành chưa. Lỗi TRƯỚC mốc này nghĩa là
      // server không thể có đủ request: an toàn để gửi lại (error.notSent).
      let flushed = false;
      const done = fn => (...a) => { if (!settled) { settled = true; clearTimeout(deadline); signal?.removeEventListener("abort", abort); lease.release(); apiKeys.reportLatency(apiKey, Date.now()-startedAt); stage("http_finished"); fn(...a); } };
      const ok = done(resolve);
      const bad = done(err => {
        const e = err instanceof Error ? err : new Error(String(err));
        try { e.notSent = !flushed && !firstByteAt; } catch { /* frozen error */ }
        reject(e);
      });

      const headers = {
        accept: "application/json",
        "x-api-key": apiKey || ""
      };
      if (payload) {
        headers["content-type"] = "application/json";
        headers["content-length"] = String(payload.length);
      }

      this.stats.requests++;
      stage("http_started");

      let req;
      try {
        req = https.request({
          agent: this.agent,
          method,
          hostname: url.hostname,
          path: url.pathname + url.search,
          headers,
          timeout: timeoutMs
        }, res => {
        firstByteAt = Date.now(); stage("first_byte");
        const chunks = [];
        res.on("aborted", () => bad(new Error("HTTP response aborted")));
        res.on("error", bad);
        res.on("data", chunk => {
          if (!firstByteAt) { firstByteAt = Date.now(); stage("first_byte"); }
          chunks.push(chunk);
        });
        res.on("end", () => {
          const raw = Buffer.concat(chunks).toString("utf8");
          let parsed = null;
          try { parsed = raw ? JSON.parse(raw) : null; } catch { parsed = { raw }; }
          const status = res.statusCode || 0;
          if (status >= 200 && status < 300) apiKeys.reportSuccess(apiKey);
          else if (status === 429) apiKeys.reportRateLimited(apiKey, retryAfterMs(res.headers));
          else if (status >= 500) apiKeys.reportFailure(apiKey);
          stage("http_finished");
          ok({
            status,
            headers: res.headers || {},
            body: parsed,
            firstByteMs: (firstByteAt || Date.now()) - startedAt,
            totalMs: Date.now() - startedAt
          });
        });
        });
      } catch (error) {
        bad(error);
        return;
      }

      req.on("socket", socket => {
        stage("socket_assigned");
        if (req.reusedSocket) this.stats.reused++;
        if (socket.connecting) { this.stats.created++; socket.once("lookup",()=>stage("dns_finished")); socket.once("connect",()=>stage("tcp_connected")); socket.once("secureConnect",()=>stage("tls_finished")); }
      });

      req.on("timeout", () => {
        this.stats.timeouts++;
        req.destroy(new Error(`HTTP quá hạn sau ${timeoutMs}ms`));
      });
      req.on("error", err => { this.stats.errors++; bad(err); });

      abort = () => req.destroy(Object.assign(new Error("HTTP cancelled"), {name:"AbortError"}));
      if (signal?.aborted) {
        abort();
        return;
      }
      signal?.addEventListener("abort",abort,{once:true});
      deadline = setTimeout(() => {this.stats.timeouts++; req.destroy(new Error("HTTP wall deadline"));}, timeoutMs);
      deadline.unref?.();
      req.once("finish",()=>{ flushed = true; stage("request_flushed"); });

      if (payload) req.write(payload);
      req.end();
    });
  }

  census() {
    const sockets = Object.values(this.agent.sockets || {})
      .reduce((n, list) => n + list.length, 0);
    const free = Object.values(this.agent.freeSockets || {})
      .reduce((n, list) => n + list.length, 0);
    return { ...this.stats, sockets, freeSockets: free };
  }

  destroy() {
    try { this.agent.destroy(); } catch { /* đã đóng */ }
  }
}

/**
 * Đọc order hash từ phản hồi, chấp nhận cả hai hình dạng API từng dùng.
 *
 * Đường production hiện tại đã đọc phòng thủ như thế này, và đó là bằng chứng
 * cả hai hình dạng đều gặp thật. Ghi lại hình dạng NÀO đã gặp, để lần chạy
 * thật đầu tiên chốt được một fixture thay vì tiếp tục đoán.
 */
function readPostedOrder(body) {
  const b = body || {};
  const hash =
    b.order_hash || b.orderHash ||
    (b.order && (b.order.order_hash || b.order.orderHash)) || "";
  const shape = b.order_hash || b.orderHash
    ? "top-level"
    : (b.order ? "nested-order" : "unknown");
  return { orderHash: String(hash || ""), shape };
}

/** Retry-After của server, ms. 0 khi không nói. */
function retryAfterMs(headers = {}) {
  const raw = headers["retry-after"] ?? headers["Retry-After"];
  if (raw === undefined) return 0;
  const seconds = Number(raw);
  if (Number.isFinite(seconds)) return Math.max(0, seconds * 1000);
  const when = Date.parse(String(raw));
  return Number.isFinite(when) ? Math.max(0, when - Date.now()) : 0;
}

module.exports = { HttpPool, readPostedOrder, retryAfterMs, OPENSEA_API_BASE };
