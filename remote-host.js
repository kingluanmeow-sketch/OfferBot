"use strict";

/**
 * REMOTE HOST — Windows là engine, Mac chỉ là màn hình + nút bấm.
 *
 *   Mac (Brave/Safari) mở http://<tailscale-ip>:<port>, ghép nối bằng mã một
 *   lần, rồi nhận trạng thái qua WebSocket và gửi lệnh về. Không có engine,
 *   không có REST OpenSea, không có key nào ở phía Mac.
 *
 * VÌ SAO WEBSOCKET TỰ VIẾT (không dùng gói `ws`)
 *
 *   Gói `ws` không nằm trong app.asar (không phải dependency của app), và
 *   Electron main không có WebSocket server sẵn. Phía server của RFC 6455 đủ
 *   nhỏ để viết đúng: handshake HTTP Upgrade, khung text/ping/pong/close có
 *   mask từ client, khung không mask từ server. Chỉ text ≤ 1 MB.
 *
 * VÌ SAO KHÔNG CÒN SSE LÀM ĐƯỜNG CHÍNH
 *
 *   Bản trước phát MỌI cập nhật engine (nhiều lần mỗi giây, cả trạng thái
 *   đầy đủ + 100 dòng log) qua SSE, và khi `write()` báo đầy bộ đệm thì ĐÓNG
 *   kết nối → EventSource nối lại → "Đang kết nối lại…" liên tục đúng lúc
 *   engine đang bận. Nay: gộp nhịp (tối đa 4 khung/giây), nén trạng thái
 *   (chỉ những gì màn hình cần), không đóng khi đầy bộ đệm (bỏ khung, đánh
 *   dấu bẩn, gửi lại khi thoát), ping 15 giây, snapshot đầy đủ khi nối/lệnh
 *   `resync`, và lệnh có ACK theo requestId (idempotent — gửi lại không nhân
 *   đôi). SSE giữ lại như đường dự phòng, với cùng các sửa.
 *
 * BIND
 *
 *   Nghe trên MỌI giao diện IPv4 (0.0.0.0): địa chỉ Tailscale có thể đổi sau
 *   khi đăng nhập lại và một bind cứng vào IP cũ là "trang không tải được".
 *   Mã ghép nối là hàng rào; URL công bố dùng IP Tailscale hiện tại.
 */

const crypto = require("crypto");
const fs = require("fs");
const http = require("http");
const os = require("os");
const path = require("path");

const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const SESSION_MS = 12 * 60 * 60 * 1000;
/**
 * Trạng thái là bản đầy đủ của renderer (mọi hàng, mọi cột) để Mac chạy đúng
 * dashboard Windows: 1 khung/giây là đủ mắt người và nhẹ cho Tailscale;
 * sự kiện (tiến trình SLL/Cancel, log) đi riêng, ngay lập tức, không gộp.
 */
const BROADCAST_MIN_MS = 1000;
const PING_MS = 15000;
const MAX_BUFFERED = 2 * 1024 * 1024;

/** Mac tải đúng renderer của Windows: cùng CSS, cùng component, cùng tab. Chỉ những file này. */
const REMOTE_STATIC = new Set([
  "remote.html", "remote.css", "remote-app.js", "styles.css", "theme.js", "error-registry.js", "utils.js", "vocab.js",
  "dashboard-component.js", "ethereum-dashboard.js", "robinhood-dashboard.js", "dashboard.js",
  "key-live.js"
]);

const json = (res, status, value) => {
  const body = Buffer.from(JSON.stringify(value));
  res.writeHead(status, { "content-type": "application/json; charset=utf-8", "content-length": body.length, "cache-control": "no-store" });
  res.end(body);
};

/** Một khung WebSocket text/ping/pong/close, không mask (server → client). */
function wsFrame(opcode, payload = Buffer.alloc(0)) {
  const len = payload.length;
  let header;
  if (len < 126) header = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { header = Buffer.alloc(4); header[0] = 0x80 | opcode; header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[0] = 0x80 | opcode; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([header, payload]);
}

/**
 * Đọc khung từ client (có mask). Trả về {frame, rest} hoặc null khi chưa đủ.
 * Không hỗ trợ fragment (client gửi JSON nhỏ, một khung).
 */
function wsParse(buffer) {
  if (buffer.length < 2) return null;
  const fin = (buffer[0] & 0x80) !== 0;
  const opcode = buffer[0] & 0x0f;
  const masked = (buffer[1] & 0x80) !== 0;
  let len = buffer[1] & 0x7f;
  let offset = 2;
  if (len === 126) { if (buffer.length < 4) return null; len = buffer.readUInt16BE(2); offset = 4; }
  else if (len === 127) { if (buffer.length < 10) return null; len = Number(buffer.readBigUInt64BE(2)); offset = 10; }
  if (len > 1024 * 1024) throw new Error("frame quá lớn");
  const maskLen = masked ? 4 : 0;
  if (buffer.length < offset + maskLen + len) return null;
  let payload = buffer.subarray(offset + maskLen, offset + maskLen + len);
  if (masked) {
    const mask = buffer.subarray(offset, offset + 4);
    const out = Buffer.allocUnsafe(len);
    for (let i = 0; i < len; i++) out[i] = payload[i] ^ mask[i & 3];
    payload = out;
  }
  return { frame: { fin, opcode, payload }, rest: buffer.subarray(offset + maskLen + len) };
}

class RemoteHost {
  constructor({ getState, command, onLog = () => {}, root = path.join(__dirname, "renderer") } = {}) {
    this.getState = getState;
    this.command = command;
    this.onLog = onLog;
    this.root = root;
    this.token = crypto.randomBytes(24).toString("base64url");
    this.sessions = new Map();
    this.results = new Map();
    this.sseClients = new Set();
    this.wsClients = new Set();
    this.server = null;
    this.info = { enabled: false, url: "", needsPairing: true };
    this.seq = 0;
    this.dirty = false;
    this.lastBroadcastAt = 0;
    this.broadcastTimer = null;
    this.pingTimer = null;
    this.heartbeatTimer = null;
    this.stats = { wsConnects: 0, wsClosed: 0, framesSent: 0, framesSkipped: 0, commands: 0, dupCommands: 0 };
  }

  address() {
    for (const list of Object.values(os.networkInterfaces())) {
      for (const item of list || []) {
        if (item.family === "IPv4" && !item.internal && /^100\./.test(item.address)) return item.address;
      }
    }
    return "127.0.0.1";
  }

  sessionOf(req) {
    const cookie = String(req.headers.cookie || "").match(/(?:^|;\s*)offerbot_session=([^;]+)/)?.[1];
    const session = cookie && this.sessions.get(cookie);
    if (!session || session.expiresAt < Date.now()) return null;
    session.expiresAt = Date.now() + SESSION_MS;
    return session;
  }

  authenticated(req) { return Boolean(this.sessionOf(req)); }

  sameOrigin(req) {
    const origin = String(req.headers.origin || "");
    if (!origin) return true;
    return origin === `http://${req.headers.host}`;
  }

  async body(req) {
    const chunks = [];
    let size = 0;
    for await (const chunk of req) {
      size += chunk.length;
      if (size > 128 * 1024) throw new Error("Request quá lớn");
      chunks.push(chunk);
    }
    return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
  }

  /** Lệnh idempotent theo requestId: gửi lại nhận đúng kết quả cũ, không chạy lại. */
  async runCommand(body) {
    const requestId = String(body.requestId || "");
    if (!/^[a-zA-Z0-9_-]{8,80}$/.test(requestId)) return { ok: false, error: "requestId không hợp lệ", requestId };
    if (this.results.has(requestId)) {
      this.stats.dupCommands++;
      // Bản sao tới trong lúc lệnh gốc còn chạy: chờ CÙNG kết quả đó.
      const prior = await this.results.get(requestId);
      return { ...prior, requestId, duplicate: true };
    }
    // Đặt chỗ TRƯỚC khi chạy: hai bản sao tới cùng lúc (WS + retry) chỉ chạy một.
    const pending = (async () => {
      try { return { ...(await this.command(body)), requestId }; }
      catch (error) { return { ok: false, error: String(error && error.message || error), requestId }; }
    })();
    this.results.set(requestId, pending);
    const result = await pending;
    this.results.set(requestId, result);
    if (this.results.size > 500) this.results.delete(this.results.keys().next().value);
    this.stats.commands++;
    this.requestBroadcast(true);
    return result;
  }

  async handle(req, res) {
    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    if (req.method === "GET" && (url.pathname === "/" || REMOTE_STATIC.has(url.pathname.slice(1)))) {
      const file = url.pathname === "/" ? "remote.html" : url.pathname.slice(1);
      const type = file.endsWith(".js") ? "application/javascript" : file.endsWith(".css") ? "text/css" : "text/html";
      // error-registry.js nằm ở gốc app (dùng chung với main), phần còn lại ở renderer/.
      const body = fs.readFileSync(file === "error-registry.js" ? path.join(this.root, "..", file) : path.join(this.root, file));
      res.writeHead(200, { "content-type": `${type}; charset=utf-8`, "content-length": body.length, "cache-control": "no-store" });
      res.end(body);
      return;
    }
    if (req.method === "POST" && url.pathname === "/api/session") {
      if (!this.sameOrigin(req)) return json(res, 403, { ok: false, error: "Origin bị từ chối" });
      const body = await this.body(req);
      const left = Buffer.from(String(body.token || ""));
      const right = Buffer.from(this.token);
      if (left.length !== right.length || !crypto.timingSafeEqual(left, right)) return json(res, 401, { ok: false, error: "Mã ghép nối sai" });
      const id = crypto.randomBytes(24).toString("base64url");
      this.sessions.set(id, { expiresAt: Date.now() + SESSION_MS });
      this.info.needsPairing = false;
      res.setHeader("set-cookie", `offerbot_session=${id}; HttpOnly; SameSite=Strict; Path=/; Max-Age=43200`);
      return json(res, 200, { ok: true });
    }
    // Mac hỏi "tôi còn phiên không?" mà không cần đọc trạng thái.
    if (req.method === "GET" && url.pathname === "/api/whoami") {
      return json(res, 200, { ok: true, paired: this.authenticated(req) });
    }
    if (!this.authenticated(req)) return json(res, 401, { ok: false, error: "Cần ghép nối lại" });
    if (req.method === "GET" && url.pathname === "/api/state") return json(res, 200, this.snapshot());
    if (req.method === "GET" && url.pathname === "/api/events") {
      res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-store", connection: "keep-alive" });
      res.write(`event: state\ndata: ${JSON.stringify(this.snapshot())}\n\n`);
      this.sseClients.add(res);
      req.on("close", () => this.sseClients.delete(res));
      return;
    }
    if (req.method === "POST" && url.pathname === "/api/command") {
      if (!this.sameOrigin(req)) return json(res, 403, { ok: false, error: "Origin bị từ chối" });
      const body = await this.body(req);
      return json(res, 200, await this.runCommand(body));
    }
    json(res, 404, { ok: false, error: "Không tìm thấy" });
  }

  // ---- WebSocket ----------------------------------------------------

  upgrade(req, socket) {
    const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
    const key = req.headers["sec-websocket-key"];
    if (url.pathname !== "/ws" || !key || !this.authenticated(req) || !this.sameOrigin(req)) {
      socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    const accept = crypto.createHash("sha1").update(key + WS_GUID).digest("base64");
    socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
      `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
    socket.setNoDelay(true);
    socket.setKeepAlive(true, 10000);
    const client = { socket, buffer: Buffer.alloc(0), alive: Date.now(), dirty: false, closed: false };
    this.wsClients.add(client);
    this.stats.wsConnects++;
    this.onLog(`[REMOTE] ws nối · ${req.socket.remoteAddress} · ${this.wsClients.size} client`);
    const close = () => {
      if (client.closed) return;
      client.closed = true;
      this.wsClients.delete(client);
      this.stats.wsClosed++;
      try { socket.destroy(); } catch { /* đã đóng */ }
    };
    socket.on("data", chunk => {
      client.buffer = Buffer.concat([client.buffer, chunk]);
      try {
        for (;;) {
          const parsed = wsParse(client.buffer);
          if (!parsed) break;
          client.buffer = parsed.rest;
          this.onFrame(client, parsed.frame);
        }
      } catch (error) {
        this.onLog(`[REMOTE] ws khung hỏng: ${error.message}`);
        close();
      }
    });
    socket.on("close", close);
    socket.on("error", close);
    socket.on("drain", () => { if (client.dirty) this.sendState(client, true); });
    this.send(client, { type: "snapshot", seq: this.seq, state: this.snapshot() });
    this.startPing();
    this.startHeartbeat();
  }

  /**
   * Nhịp tim trạng thái: dashboard Windows coi engine "mất nhịp" khi 12s không
   * có khung. Engine yên (không sự kiện) thì không có gì để phát — nên phát
   * một khung mỗi 5s chừng nào còn client, để Mac không báo LOST giả.
   */
  startHeartbeat() {
    if (this.heartbeatTimer) return;
    this.heartbeatTimer = setInterval(() => {
      if (!this.wsClients.size) { clearInterval(this.heartbeatTimer); this.heartbeatTimer = null; return; }
      if (Date.now() - this.lastBroadcastAt >= 5000) this.requestBroadcast(true);
    }, 2500);
    this.heartbeatTimer.unref?.();
  }

  onFrame(client, frame) {
    client.alive = Date.now();
    if (frame.opcode === 0x8) { this.wsClients.delete(client); client.closed = true; try { client.socket.end(wsFrame(0x8)); } catch { /* đã đóng */ } return; }
    if (frame.opcode === 0x9) { this.write(client, wsFrame(0xA, frame.payload)); return; }
    if (frame.opcode === 0xA) return;
    if (frame.opcode !== 0x1) return;
    let msg;
    try { msg = JSON.parse(frame.payload.toString("utf8")); } catch { return; }
    if (!msg || typeof msg !== "object") return;
    if (msg.type === "pong" || msg.type === "ping") { if (msg.type === "ping") this.send(client, { type: "pong", at: Date.now() }); return; }
    if (msg.type === "resync") { this.send(client, { type: "snapshot", seq: this.seq, state: this.snapshot() }); return; }
    if (msg.type === "command") {
      this.runCommand(msg).then(result => this.send(client, { type: "ack", requestId: result.requestId, result }));
    }
  }

  write(client, buffer) {
    if (client.closed) return false;
    try { return client.socket.write(buffer); } catch { return false; }
  }

  send(client, obj) {
    if (client.closed) return false;
    // Bộ đệm đầy: KHÔNG đóng. Bỏ khung trạng thái này, đánh dấu bẩn, gửi lại
    // khi socket thoát (drain). Lệnh/ack luôn được ghi.
    if (obj.type === "state" && client.socket.writableLength > MAX_BUFFERED) {
      client.dirty = true;
      this.stats.framesSkipped++;
      return false;
    }
    this.stats.framesSent++;
    return this.write(client, wsFrame(0x1, Buffer.from(JSON.stringify(obj))));
  }

  sendState(client, force = false) {
    if (client.closed) return;
    if (!force && client.socket.writableLength > MAX_BUFFERED) { client.dirty = true; this.stats.framesSkipped++; return; }
    client.dirty = false;
    this.send(client, { type: "state", seq: this.seq, state: this.snapshot() });
  }

  startPing() {
    if (this.pingTimer) return;
    this.pingTimer = setInterval(() => {
      const now = Date.now();
      for (const client of [...this.wsClients]) {
        // Không thấy gì từ client trong 3 nhịp ping: ống chết (Mac ngủ, đổi
        // mạng). Đóng để client nối lại và nhận snapshot mới.
        if (now - client.alive > PING_MS * 3) { client.closed = true; this.wsClients.delete(client); try { client.socket.destroy(); } catch { /* đã đóng */ } continue; }
        this.write(client, wsFrame(0x9, Buffer.from("hb")));
        this.send(client, { type: "ping", at: now });
      }
      if (!this.wsClients.size && this.pingTimer) { clearInterval(this.pingTimer); this.pingTimer = null; }
    }, PING_MS);
    this.pingTimer.unref?.();
  }

  // ---- phát trạng thái, gộp nhịp -------------------------------------

  snapshot() {
    this.seq++;
    let state;
    try { state = this.getState(); } catch (error) { state = { error: String(error && error.message || error) }; }
    return { ...state, seq: this.seq, at: Date.now(), host: this.info.url };
  }

  /** main gọi sau mỗi lần engine đổi; gộp về ≤ 1 khung/giây. */
  broadcast() { this.requestBroadcast(false); }

  /** Sự kiện của cửa sổ (tiến trình SLL/Cancel, log, licence…): gửi ngay, không gộp, không lưu. */
  pushEvent(channel, payload) {
    if (!this.wsClients.size) return;
    const obj = { type: "event", channel, payload, at: Date.now() };
    for (const client of this.wsClients) this.send(client, obj);
  }

  requestBroadcast(immediate = false) {
    if (!this.wsClients.size && !this.sseClients.size) return;
    const wait = immediate ? 0 : Math.max(0, BROADCAST_MIN_MS - (Date.now() - this.lastBroadcastAt));
    if (this.broadcastTimer) { if (!immediate) return; clearTimeout(this.broadcastTimer); }
    this.broadcastTimer = setTimeout(() => {
      this.broadcastTimer = null;
      this.lastBroadcastAt = Date.now();
      this.flush();
    }, wait);
    this.broadcastTimer.unref?.();
  }

  flush() {
    if (!this.wsClients.size && !this.sseClients.size) return;
    for (const client of [...this.wsClients]) this.sendState(client);
    if (this.sseClients.size) {
      const frame = `event: state\ndata: ${JSON.stringify(this.snapshot())}\n\n`;
      for (const client of [...this.sseClients]) {
        // Đầy bộ đệm: bỏ khung này, KHÔNG đóng — khung sau mang trạng thái mới hơn.
        if (client.writableLength > MAX_BUFFERED) { this.stats.framesSkipped++; continue; }
        try { client.write(frame); } catch { this.sseClients.delete(client); }
      }
    }
  }

  start(port = 41739) {
    if (this.server) return Promise.resolve(this.info);
    this.server = http.createServer((req, res) => this.handle(req, res).catch(error => json(res, 500, { ok: false, error: error.message })));
    this.server.on("upgrade", (req, socket) => { try { this.upgrade(req, socket); } catch (error) { this.onLog(`[REMOTE] upgrade lỗi: ${error.message}`); try { socket.destroy(); } catch { /* đã đóng */ } } });
    this.server.keepAliveTimeout = 65000;
    this.server.headersTimeout = 70000;
    // SSE giữ kết nối lâu hơn mọi requestTimeout mặc định.
    this.server.requestTimeout = 0;
    return new Promise((resolve, reject) => {
      this.server.once("error", reject);
      // Bind ONLY the private-network address (or loopback) - never 0.0.0.0,
      // which would expose the host on every LAN the PC joins (kept from 1.25.0).
      const bindHost = this.address();
      this.server.listen(port, bindHost, () => {
        const host = bindHost;
        this.info = { enabled: true, url: `http://${host}:${port}`, port, needsPairing: true, tailscale: host !== "127.0.0.1" };
        this.onLog(`[REMOTE] host ${this.info.url} (${this.info.tailscale ? "mạng riêng" : "chỉ máy này"}) · WS /ws`);
        // IP Tailscale có thể xuất hiện/đổi sau khi app mở: cập nhật URL công bố.
        this.addressTimer = setInterval(() => {
          const now = this.address();
          const url = `http://${now}:${port}`;
          if (url !== this.info.url) {
            // The bound address changed: rebind on the new one (never 0.0.0.0).
            this.onLog(`[REMOTE] địa chỉ đổi → ${url} · mở lại host`);
            clearInterval(this.addressTimer); this.addressTimer = null;
            const old = this.server; this.server = null;
            try { old.close(); } catch { /* đã đóng */ }
            this.start(port).catch(error => this.onLog(`[REMOTE] mở lại host lỗi: ${error.message}`));
          }
        }, 30000);
        this.addressTimer.unref?.();
        resolve(this.info);
      });
    });
  }

  pairing() { return { ...this.info, token: this.token, clients: this.wsClients.size, ...this.stats }; }

  stop() {
    for (const client of this.sseClients) { try { client.end(); } catch { /* đã đóng */ } }
    this.sseClients.clear();
    for (const client of [...this.wsClients]) { client.closed = true; try { client.socket.end(wsFrame(0x8)); } catch { /* đã đóng */ } }
    this.wsClients.clear();
    if (this.pingTimer) { clearInterval(this.pingTimer); this.pingTimer = null; }
    if (this.heartbeatTimer) { clearInterval(this.heartbeatTimer); this.heartbeatTimer = null; }
    if (this.addressTimer) { clearInterval(this.addressTimer); this.addressTimer = null; }
    if (this.broadcastTimer) { clearTimeout(this.broadcastTimer); this.broadcastTimer = null; }
    this.server?.close();
    this.server = null;
  }
}

module.exports = { RemoteHost, wsFrame, wsParse };
