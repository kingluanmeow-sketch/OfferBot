"use strict";

/**
 * OpenSea Stream API client (spec 12, 13).
 *
 * Electron's bundled Chromium/Node combination cannot be relied on to expose a
 * usable globalThis.WebSocket in the main process, so this file speaks the
 * WebSocket protocol directly over a Node TLS socket. That also lets us send
 * Phoenix v2 frames exactly as OpenSea expects them:
 *
 *   [join_ref, ref, topic, event, payload]
 *
 * Responsibilities:
 *   - connect / reconnect with exponential backoff
 *   - 25s heartbeat on the "phoenix" topic
 *   - phx_join / phx_leave as the watched collection set changes
 *   - decode events and resolve them to a concrete NFT so the engine can run a
 *     priority scan instead of waiting for nextScanAt
 */

const tls = require("tls");
const crypto = require("crypto");
const { URL } = require("url");

const { logger } = require("./logger");

/**
 * Where the stream lives.
 *
 * Overridable ONLY to a loopback address, exactly as opensea.js allows for its
 * REST base. Without this the stream path could not be exercised inside the
 * packaged binary at all: a test can drive REST and the scheduler, but the one
 * thing it could never do was deliver a real event to the real handler and
 * measure what happened next - which is the whole question when the complaint
 * is "an event waited ten seconds".
 *
 * A non-loopback value is IGNORED with a line in the log, never obeyed. A
 * remote override would send the API key to a host of somebody else's
 * choosing, and no test is worth that.
 */
function resolveStreamUrl() {
  const fallback = "wss://stream-api.opensea.io/socket/websocket";
  const wanted = String(process.env.OSB_STREAM_URL || "").trim();
  if (!wanted) return fallback;

  try {
    const host = new URL(wanted).hostname.toLowerCase();
    const loopback = host === "localhost" || host === "::1" || host.startsWith("127.");
    if (loopback) return wanted.replace(/\/+$/, "");
    process.stderr.write(
      "[STREAM] bỏ qua OSB_STREAM_URL: chỉ nhận địa chỉ loopback\n");
  } catch {
    process.stderr.write("[STREAM] bỏ qua OSB_STREAM_URL: địa chỉ không hợp lệ\n");
  }
  return fallback;
}

const STREAM_URL = resolveStreamUrl();

/**
 * TRANSPORT silence: no frame of ANY kind (heartbeat reply, join reply, event)
 * for this long while heartbeats go out every 25s = three missed heartbeat
 * replies. That is protocol evidence of a dead socket. Market silence never
 * reaches this: a quiet collection still answers heartbeats.
 */
const SILENCE_TIMEOUT_MS = 90 * 1000;
/** Heartbeat interval, and how long a revalidation probe waits for its reply. */
const HEARTBEAT_MS = 25 * 1000;
const PROBE_TIMEOUT_MS = 5 * 1000;
/**
 * Per-slug backoff after a REFUSED join (1.25.1). A refused topic used to be
 * retried at every 10s sync tick forever (production: the same three slugs
 * refused every ~10s). Now: 10s -> 30s -> 2m -> 10m, reset on a successful ACK.
 */
const JOIN_BACKOFF_MS = [10 * 1000, 30 * 1000, 2 * 60 * 1000, 10 * 60 * 1000];
/** A join with no reply at all after this long is treated as refused. */
const JOIN_ACK_TIMEOUT_MS = 20 * 1000;

/** Events the engine reacts to. Anything else is ignored cheaply. */
const HANDLED_EVENTS = new Set([
  "item_received_bid",
  "collection_offer",
  "trait_offer",
  "item_cancelled",
  // Order became invalid / valid again (balance, approval, ...). Emitted by
  // OpenSea (official stream-js EventType ORDER_INVALIDATE / ORDER_REVALIDATE)
  // and ignored before 1.25.14 - the source of many "lệnh đã chết mà Stream
  // không nói" that only a later REST read removed.
  "order_invalidate",
  "order_revalidate"
]);

/**
 * GLOBAL MARKET STREAM (1.25.14)
 *
 *   Verified against the official @opensea/stream-js (0.4.0) README: "to
 *   listen to an event from all collections use wildcard `*` for the
 *   collectionSlug" -> topic `collection:*`; the SDK passes
 *   `{ event_types: [...] }` in the join payload to limit the event families.
 *   One subscription covers every collection a Tool tracks, now or tomorrow:
 *   no per-collection topic that can sit in BACKOFF while its NFTs look WARM.
 *   Frames are filtered by tracked slug BEFORE any decoding (cheap drop).
 *   If OpenSea refuses the wildcard join repeatedly, the client falls back to
 *   per-collection topics (the pre-1.25.14 behaviour) instead of going deaf.
 *   OSB_STREAM_GLOBAL=0 forces the per-collection mode.
 */
const GLOBAL_TOPIC = "*";
const GLOBAL_EVENT_TYPES = Array.from(HANDLED_EVENTS);
const GLOBAL_REFUSALS_BEFORE_FALLBACK = 2;

// ------------------------------------------------------------------
// Minimal RFC 6455 client
// ------------------------------------------------------------------

class RawWebSocket {
  constructor(url, handlers) {
    this.url = new URL(url);
    this.onOpen = handlers.onOpen || (() => {});
    this.onMessage = handlers.onMessage || (() => {});
    this.onError = handlers.onError || (() => {});
    this.onClose = handlers.onClose || (() => {});

    this.socket = null;
    this.buffer = Buffer.alloc(0);
    this.closed = false;
    this.readyState = 0; // 0 connecting, 1 open, 3 closed

    this.connect();
  }

  connect() {
    const host = this.url.hostname;
    const path = `${this.url.pathname}${this.url.search}`;
    const key = crypto.randomBytes(16).toString("base64");

    const handshake = () => {
      const req = [
        `GET ${path} HTTP/1.1`,
        `Host: ${host}`,
        "Upgrade: websocket",
        "Connection: Upgrade",
        `Sec-WebSocket-Key: ${key}`,
        "Sec-WebSocket-Version: 13",
        "\r\n"
      ].join("\r\n");
      this.socket.write(req);
    };

    // ---- a plain socket, and ONLY to loopback --------------------------
    //
    // The line below dials TLS on port 443 with certificate verification on,
    // and that is what OpenSea gets - unconditionally, because the only way to
    // reach this branch is an OSB_STREAM_URL that resolveStreamUrl already
    // checked is a loopback ws:// address. With the variable unset, which is
    // every real run, the code path is byte-for-byte the one that was here
    // before.
    //
    // It exists because the stream could not otherwise be driven inside the
    // packaged binary at all: this client dials 443/TLS whatever URL it is
    // given, so a local server could never be reached, and "an event waited
    // ten seconds" was a claim no EXE test could check. The alternative was a
    // self-signed certificate, which is worse.
    const loopback = host === "localhost" || host === "::1" || host.startsWith("127.");
    if (loopback && this.url.protocol === "ws:") {
      const net = require("net");
      this.socket = net.connect(
        { host, port: Number(this.url.port) || 80 }, handshake);
    } else {
      this.socket = tls.connect(
        { host, port: 443, servername: host, rejectUnauthorized: true },
        handshake
      );
    }

    this.socket.setNoDelay(true);

    let handshaken = false;
    let headerBuf = Buffer.alloc(0);

    this.socket.on("data", chunk => {
      if (!handshaken) {
        headerBuf = Buffer.concat([headerBuf, chunk]);
        const idx = headerBuf.indexOf("\r\n\r\n");
        if (idx === -1) return;

        const header = headerBuf.subarray(0, idx).toString("utf8");
        const rest = headerBuf.subarray(idx + 4);

        if (!/^HTTP\/1\.1 101 /i.test(header)) {
          this.fail(
            new Error(
              `WebSocket handshake failed: ${header.split("\r\n")[0] || "unknown"}`
            )
          );
          return;
        }

        handshaken = true;
        this.readyState = 1;
        headerBuf = Buffer.alloc(0);
        this.onOpen();
        if (rest.length) this.buffer = Buffer.concat([this.buffer, rest]);
      } else {
        this.buffer = Buffer.concat([this.buffer, chunk]);
      }

      this.parseFrames();
    });

    this.socket.on("error", err => this.fail(err));
    this.socket.on("close", () => this.finishClose());
  }

  fail(err) {
    if (this.closed) return;
    try {
      this.onError(err);
    } catch {
      /* ignore */
    }
    try {
      this.socket?.destroy();
    } catch {
      /* ignore */
    }
    this.finishClose();
  }

  finishClose() {
    if (this.closed) return;
    this.closed = true;
    this.readyState = 3;
    this.buffer = Buffer.alloc(0);
    try {
      this.onClose(1006, "socket closed");
    } catch {
      /* ignore */
    }
  }

  send(text) {
    if (this.readyState !== 1 || this.closed) return;

    const payload = Buffer.from(String(text));
    const mask = crypto.randomBytes(4);
    let header;

    if (payload.length < 126) {
      header = Buffer.from([0x81, 0x80 | payload.length]);
    } else if (payload.length < 65536) {
      header = Buffer.alloc(4);
      header[0] = 0x81;
      header[1] = 0xfe;
      header.writeUInt16BE(payload.length, 2);
    } else {
      header = Buffer.alloc(10);
      header[0] = 0x81;
      header[1] = 0xff;
      header.writeBigUInt64BE(BigInt(payload.length), 2);
    }

    const masked = Buffer.alloc(payload.length);
    for (let i = 0; i < payload.length; i++) masked[i] = payload[i] ^ mask[i % 4];

    try {
      this.socket.write(Buffer.concat([header, mask, masked]));
    } catch (error) {
      this.fail(error);
    }
  }

  sendControl(opcode, payload) {
    if (this.readyState !== 1 || this.closed) return;
    const mask = crypto.randomBytes(4);
    const header = Buffer.from([0x80 | opcode, 0x80 | payload.length]);
    const masked = Buffer.alloc(payload.length);
    for (let i = 0; i < payload.length; i++) masked[i] = payload[i] ^ mask[i % 4];
    try {
      this.socket.write(Buffer.concat([header, mask, masked]));
    } catch {
      /* the close handler will clean up */
    }
  }

  pong(payload) {
    this.sendControl(0xa, payload);
  }

  close() {
    if (this.closed) return;
    this.sendControl(0x8, Buffer.alloc(0));
    try {
      this.socket?.end();
    } catch {
      /* ignore */
    }
    this.finishClose();
  }

  parseFrames() {
    while (this.buffer.length >= 2) {
      const b0 = this.buffer[0];
      const b1 = this.buffer[1];
      const opcode = b0 & 0x0f;
      const masked = Boolean(b1 & 0x80);

      let len = b1 & 0x7f;
      let off = 2;

      if (len === 126) {
        if (this.buffer.length < 4) return;
        len = this.buffer.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (this.buffer.length < 10) return;
        const n = this.buffer.readBigUInt64BE(2);
        if (n > BigInt(Number.MAX_SAFE_INTEGER)) {
          this.fail(new Error("WebSocket frame too large"));
          return;
        }
        len = Number(n);
        off = 10;
      }

      let maskKey = null;
      if (masked) {
        if (this.buffer.length < off + 4) return;
        maskKey = this.buffer.subarray(off, off + 4);
        off += 4;
      }

      if (this.buffer.length < off + len) return;

      let payload = this.buffer.subarray(off, off + len);
      this.buffer = this.buffer.subarray(off + len);

      if (masked) {
        const unmasked = Buffer.alloc(len);
        for (let i = 0; i < len; i++) unmasked[i] = payload[i] ^ maskKey[i % 4];
        payload = unmasked;
      }

      if (opcode === 0x1) {
        try {
          this.onMessage(payload.toString("utf8"));
        } catch (error) {
          try {
            this.onError(error);
          } catch {
            /* ignore */
          }
        }
      } else if (opcode === 0x8) {
        try {
          this.socket?.end();
        } catch {
          /* ignore */
        }
        this.finishClose();
        return;
      } else if (opcode === 0x9) {
        this.pong(payload);
      }
    }
  }
}

// ------------------------------------------------------------------
// Event decoding
// ------------------------------------------------------------------

/**
 * OpenSea encodes the token as `nft_id: "ethereum/0xcontract/1234"`.
 * @returns {{chain:string, contract:string, tokenId:string}|null}
 */
function parseNftId(nftId) {
  const parts = String(nftId || "").split("/");
  if (parts.length < 3) return null;

  const [chain, contract, tokenId] = parts;
  if (!/^0x[a-fA-F0-9]{40}$/.test(contract)) return null;
  if (!/^\d+$/.test(tokenId)) return null;

  return {
    chain: String(chain || "").toLowerCase(),
    contract: contract.toLowerCase(),
    tokenId: String(tokenId)
  };
}

function readPrice(payload) {
  const base =
    payload?.base_price ??
    payload?.basePrice ??
    payload?.payment_token?.base_price ??
    null;

  if (base === null || base === undefined || base === "") return 0;

  try {
    const decimals = Number(payload?.payment_token?.decimals ?? 18);
    const value = BigInt(String(base));
    return Number(value) / Math.pow(10, Number.isFinite(decimals) ? decimals : 18);
  } catch {
    const n = Number(base);
    return Number.isFinite(n) ? n : 0;
  }
}

/**
 * Normalise a raw Phoenix frame into something the engine can act on.
 *
 * @returns {{
 *   event:string, topic:string, collectionSlug:string,
 *   nft:{chain:string,contract:string,tokenId:string}|null,
 *   price:number, isCollectionWide:boolean
 * }|null}
 */
/** Sau 401/403: thử kết nối lại thưa, thay vì không bao giờ. */
const AUTH_RETRY_MS = 5 * 60 * 1000;

function decodeEvent(frame) {
  const eventName = String(frame?.event || "");
  if (!HANDLED_EVENTS.has(eventName)) return null;

  // Phoenix wraps the domain payload one level deeper.
  const outer = frame?.payload || {};
  const payload = outer.payload || outer;

  const collectionSlug = String(
    payload?.collection?.slug ||
      payload?.item?.collection?.slug ||
      String(frame?.topic || "").replace(/^collection:/, "") ||
      ""
  ).trim();

  const nft =
    parseNftId(payload?.item?.nft_id) ||
    parseNftId(payload?.nft_id) ||
    parseNftId(payload?.asset?.nft_id) ||
    null;

  /**
   * The trait a trait_offer targets, in one shape.
   *
   * OpenSea sends both `trait_criteria` (one) and `trait_criteria_list`
   * (many). Only single-trait offers are readable as a scope here: an offer
   * over several traits at once means "any token matching ALL of these", and
   * guessing at that is how an offer gets applied to tokens it never covered.
   * So a multi-trait offer returns null and is refused downstream - visibly,
   * rather than approximately.
   */
  function readTraitCriteria(p) {
    const list = Array.isArray(p?.trait_criteria_list) ? p.trait_criteria_list : [];
    // Nhiều trait: measured on live orders to mean AND (see trait-criteria.js).
    // The full list travels in `traitCriteriaList`; this single-trait field
    // stays null for them so nothing downstream mistakes one trait for all.
    if (list.length > 1) return null;
    const c = list[0] || p?.trait_criteria;
    if (!c) return null;
    const traitType = String(c.trait_type ?? c.traitType ?? "").trim();
    const traitName = String(c.trait_name ?? c.traitName ?? c.value ?? "").trim();
    if (!traitType || !traitName) return null;
    return { trait_type: traitType, trait_name: traitName };
  }

  /** The whole criteria list, verbatim - the normalizer decides what it means. */
  function readTraitCriteriaList(p) {
    const list = Array.isArray(p?.trait_criteria_list) ? p.trait_criteria_list : [];
    const numeric = Array.isArray(p?.numeric_trait_criteria_list)
      ? p.numeric_trait_criteria_list : [];
    if (!list.length && !numeric.length) return null;
    return {
      trait_criteria_list: list.map(c => ({
        trait_type: String(c?.trait_type ?? c?.traitType ?? "").trim(),
        trait_name: String(c?.trait_name ?? c?.traitName ?? c?.value ?? "").trim()
      })),
      numeric_trait_criteria_list: numeric
    };
  }

  const isCollectionWide =
    eventName === "collection_offer" || eventName === "trait_offer";

  // Everything the local order book needs, so a bid can be applied without a
  // REST read. Verified against live frames: item_received_bid carries
  // order_hash, maker.address, base_price, payment_token{decimals,symbol},
  // quantity, expiration_date and the full protocol_data; item_cancelled
  // carries the same minus protocol_data, which a removal does not need.
  const parameters = payload?.protocol_data?.parameters || null;

  const decimals = Number(payload?.payment_token?.decimals);
  const safeDecimals = Number.isFinite(decimals) ? decimals : 18;

  const quantityRaw = Number(payload?.quantity);
  const nftItem = (parameters?.consideration || []).find(
    item => Number(item?.itemType) >= 2
  );
  const quantityFromOrder = Number(nftItem?.startAmount);

  const quantity = Math.max(
    Number.isFinite(quantityRaw) && quantityRaw >= 1 ? quantityRaw : 1,
    Number.isFinite(quantityFromOrder) && quantityFromOrder >= 1 ? quantityFromOrder : 1
  );

  /**
   * UNIT PRICE — ONE DEFINITION, FROM THE ORDER ITSELF
   *
   *   `base_price` does NOT mean the same thing on every event. Measured on
   *   live frames: a `collection_offer` for QTY 2 carried base_price 0.1 WETH
   *   with protocol_data offer[0].startAmount 0.2 WETH — base_price is PER
   *   UNIT there — while REST `price.value` for the same kind of order is the
   *   WHOLE order. Dividing base_price by quantity, as this file used to, made
   *   a 0.0371 × QTY 3 collection offer look like 0.0124 to the book, so the
   *   bot sat at 0.0367 under a 0.0371 competitor (measured on #4951).
   *
   *   So the unit price comes from the Seaport parameters whenever they are
   *   present: what is offered (offer[0].startAmount) divided by how many
   *   items it buys (consideration NFT startAmount). That is unambiguous and
   *   identical for Stream and REST. Only without protocol_data do we fall
   *   back to base_price — per unit for collection/trait offers (measured),
   *   whole order for item events (quantity 1 in practice).
   */
  const basePrice = readPrice(payload);
  let pricePerItem = 0;
  const offerItem = Array.isArray(parameters?.offer) ? parameters.offer[0] : null;
  const offeredRaw = offerItem ? Number(offerItem.startAmount) : NaN;
  if (Number.isFinite(offeredRaw) && offeredRaw > 0 && quantityFromOrder >= 1) {
    pricePerItem = offeredRaw / Math.pow(10, safeDecimals) / quantityFromOrder;
  } else if (basePrice > 0) {
    pricePerItem = isCollectionWide ? basePrice : basePrice / quantity;
  }
  // Hút nhiễu số thực (0.0371 × 15 / 15 = 0.036999…) để log và so sánh sạch.
  pricePerItem = Math.round(pricePerItem * 1e12) / 1e12;
  const totalOrderValue = pricePerItem * quantity;

  const endTime =
    Number(parameters?.endTime) ||
    (payload?.expiration_date
      ? Math.floor(new Date(payload.expiration_date).getTime() / 1000)
      : 0);

  const considerationType = Number(nftItem?.itemType);
  const kind = isCollectionWide
    ? eventName === "trait_offer"
      ? "trait"
      : "collection"
    : considerationType >= 4
      ? "collection"
      : "item";

  return {
    event: eventName,
    topic: String(frame?.topic || ""),
    collectionSlug,
    nft,
    isCollectionWide,

    // Kept for callers that only ever wanted a number.
    price: totalOrderValue,

    orderHash: String(payload?.order_hash || "") || null,
    maker: String(payload?.maker?.address || parameters?.offerer || "") || null,
    totalOrderValue,
    quantity,
    pricePerItem,
    currency: String(payload?.payment_token?.symbol || ""),
    decimals: safeDecimals,
    endTime,
    kind,

    /**
     * WHICH TRAIT A trait_offer IS FOR.
     *
     *   Measured on live traffic: 90 seconds across ten busy collections
     *   carried 50,430 events, 504 of them `trait_offer`, and the offer engine
     *   discarded every single one - correctly, under its own safety rule,
     *   because an offer whose criteria cannot be read must never be applied
     *   to a whole collection. Doing that would outbid the wallet against
     *   itself on thousands of NFTs nobody had bid on.
     *
     *   But the criteria were never missing. OpenSea sends them as
     *   `trait_criteria: {trait_type, trait_name}` - live sample: Clothing /
     *   Ninja - and this decoder simply did not carry them across, so the
     *   safety rule fired on every trait offer in existence.
     *
     *   Additive: the older engine reads none of this and is unaffected.
     */
    traitCriteria: readTraitCriteria(payload),
    traitCriteriaList: readTraitCriteriaList(payload),

    hasOrderData: Boolean(parameters),

    // When OPENSEA says the event happened, as opposed to when we received it.
    // Read defensively: it is only used for ordering and for measuring how far
    // behind the wire we are, and a payload without it must not poison either.
    eventTimestamp: parseEventTimestamp(payload),
    // Per-order monotonic revision (official stream-js "Event Versioning").
    version: Number(outer && outer.version) > 0 ? Number(outer.version) : 0,
    receivedAt: Date.now()
  };
}

/**
 * OpenSea event time, in ms, or 0 when the payload does not carry a usable one.
 *
 * Zero rather than null so every caller can compare it without a guard, and
 * anything that does not parse to a real instant is treated as absent - a
 * malformed timestamp must not make an event look older or newer than it is.
 */
function parseEventTimestamp(payload) {
  const raw = payload?.event_timestamp;
  if (!raw) return 0;
  const ms = typeof raw === "number" ? raw : Date.parse(raw);
  return Number.isFinite(ms) && ms > 0 ? ms : 0;
}

// ------------------------------------------------------------------
// Stream client
// ------------------------------------------------------------------

class OpenSeaStream {
  /**
   * @param {object} options
   * @param {() => string} options.getApiKey  read the current key on connect
   * @param {(event:object) => void} options.onEvent
   */
  constructor({ getApiKey, getKeySignature, isKeyConfigured, onEvent, onReconnect, onTopicGap, onTopicJoined, onTopicUnavailable } = {}) {
    this.getApiKey = typeof getApiKey === "function" ? getApiKey : () => "";
    /**
     * Vân tay của TẬP khoá đã cấu hình. Khi có, refreshKey dùng nó thay cho
     * khoá đang được chọn — xem refreshKey để biết vì sao phép so cũ luôn
     * nói "đã đổi".
     */
    this.getKeySignature = typeof getKeySignature === "function" ? getKeySignature : null;
    this.keySignature = this.getKeySignature ? String(this.getKeySignature() || "") : null;
    this.onEvent = typeof onEvent === "function" ? onEvent : () => {};
    /** First join ACK of a topic (1.25.12): see OfferItemEngineV2.firstJoinRecheck. */
    this.onTopicJoined = typeof onTopicJoined === "function" ? onTopicJoined : () => {};
    this.onReconnect = typeof onReconnect === "function" ? onReconnect : () => {};
    this.isKeyConfiguredFn = typeof isKeyConfigured === "function" ? isKeyConfigured : null;
    /**
     * A collection channel was closed/errored BY THE SERVER (phx_error /
     * phx_close / refused join) and has been rejoined: (slug) => void. Only
     * protocol evidence triggers it - never market silence.
     */
    this.onTopicGap = typeof onTopicGap === "function" ? onTopicGap : () => {};
    /** A previously joined topic became unavailable; recovery starts before rejoin. */
    this.onTopicUnavailable = typeof onTopicUnavailable === "function" ? onTopicUnavailable : () => {};
    /** Slugs whose channel dropped on protocol evidence; reported once rejoined. */
    this.topicGapPending = new Set();
    /**
     * slug -> { state: "JOINING"|"JOINED"|"BACKOFF", ref, sentAt, everJoined,
     *           failures, retryAt }. `joined` still means "join sent or ACKed"
     * (health/compat); this map is the truth about each subscription.
     */
    this.joinState = new Map();
    this.joinRef = 0;
    /** Answers "is this exact key still configured?" - see refreshKey. */
    this.isKeyConfigured = null;
    /** A live socket was lost; tell engines once the replacement is open. */
    this.gapPending = false;
    this.lastFrameAt = 0;
    /** slug -> last meaningful event on that topic; slug -> when it was joined. */
    this.lastEventBySlug = new Map();
    this.joinedAt = new Map();

    this.ws = null;
    this.activeKey = "";
    this.closedByUser = true;

    this.reconnectTimer = null;
    this.heartbeatTimer = null;
    this.syncTimer = null;
    /** One pending immediate rejoin after phx_error/phx_close (1.25.12). */
    this.rejoinTimer = null;
    /** slug -> last protocol error time; bounded by the watched collections. */
    this.topicErrorAt = new Map();

    this.collections = new Set();
    /** Lower-cased tracked slugs: the early filter of the global feed. */
    this.trackedSlugs = new Set();
    // Path A subscribes only to tracked collections. Wildcard mode is kept as
    // an explicit diagnostic switch; it is never the production default.
    this.globalMode = process.env.OSB_STREAM_GLOBAL === "1";
    this.globalStats = { received: 0, dropped: 0, droppedType: 0, matched: 0 };

    // Set when OpenSea refuses the key; cleared only by a key change.
    this.authFailed = false;
    this.joined = new Set();
    this.backoff = 1000;

    this.lastEventAt = 0;
    this.silenceWarned = false;

    // ---- health, as distinct from "the socket is open" ------------------
    //
    // `readyState === OPEN` is not evidence that anything is arriving. The
    // failure this exists for is the connection that comes up, joins its
    // topics and then delivers nothing: every check that looks at the socket
    // says fine, and the bot quietly stops reacting in realtime. So these are
    // recorded separately and the state is derived from them, never from the
    // socket alone.
    this.lastMeaningfulEventAt = 0;
    this.lastHeartbeatAt = 0;
    this.lastSubscriptionAt = 0;
    this.lastReconnectAt = 0;
    this.connecting = false;

    this.stats = {
      events: 0,
      handled: 0,
      reconnects: 0,
      joinErrors: 0,
      silentRecoveries: 0,
      topicRejoins: 0,
      connectedAt: 0,
      framesReceived: 0,
      bytesReceived: 0,
      untrackedDropped: 0,
      eventTypeDropped: 0
    };
  }

  /**
   * What the stream actually is right now.
   *
   * FAILED         the key was refused; a timer cannot fix that
   * DISCONNECTED   no socket, and none is being opened
   * RECONNECTING   a socket is being opened, or one is due
   * STALE          open and joined, but the TRANSPORT has gone quiet: no
   *                heartbeat reply / join reply / frame of any kind for
   *                SILENCE_TIMEOUT_MS (checkSilence closes it right after)
   * HEALTHY        open, joined, and answering heartbeats
   *
   * MARKET SILENCE IS NOT FAILURE (1.25.0). `lastMeaningfulEventAt` used to
   * decide STALE, so a collection with no bid for 90s read as a broken stream
   * and pushed the engines into degraded REST polling. It is now only a market
   * activity metric; health is protocol evidence alone.
   */
  health(now = Date.now()) {
    if (this.authFailed) return "FAILED";
    if (this.closedByUser) return "DISCONNECTED";
    if (!this.isConnected()) {
      return this.connecting || this.reconnectTimer ? "RECONNECTING" : "DISCONNECTED";
    }
    if (this.collections.size === 0) return "HEALTHY";  // nothing to deliver
    if (this.joined.size === 0) return "RECONNECTING";  // open but not subscribed

    const proof = this.transportProofAt();
    return proof > 0 && now - proof >= SILENCE_TIMEOUT_MS ? "STALE" : "HEALTHY";
  }

  /** A healthy socket does not imply that this collection's join was ACKed. */
  healthForCollection(slug, now = Date.now()) {
    const transport = this.health(now);
    if (transport !== "HEALTHY") return transport;
    const wanted = String(slug || "").trim().toLowerCase();
    if (!wanted || !this.trackedSlugs.has(wanted)) return "DISCONNECTED";
    const topic = this.globalMode ? GLOBAL_TOPIC : wanted;
    return this.joinState.get(topic)?.state === "JOINED" ? "HEALTHY" : "RECONNECTING";
  }

  /** Latest protocol-level evidence that the socket is alive. */
  transportProofAt() {
    return Math.max(
      this.lastFrameAt || 0,
      this.lastHeartbeatAt || 0,
      this.lastSubscriptionAt || 0,
      this.lastMeaningfulEventAt || 0,
      this.stats.connectedAt || 0
    );
  }

  /**
   * Revalidate after an OS resume / network change WITHOUT tearing down a
   * healthy socket: send one heartbeat and wait up to PROBE_TIMEOUT_MS for any
   * frame. A reply keeps the socket (and every subscription) as it is; no
   * reply, or no socket, is evidence - then and only then reconnect.
   *
   * @returns {Promise<{healthy:boolean, reconnected:boolean, why:string}>}
   */
  async revalidate(why = "revalidate") {
    if (this.closedByUser) return { healthy: false, reconnected: false, why: "stopped" };
    if (this.authFailed) return { healthy: false, reconnected: false, why: "auth-failed" };
    if (!this.isConnected()) {
      if (this.ws || this.connecting || this.reconnectTimer) {
        return { healthy: false, reconnected: false, why: "reconnect-already-in-progress" };
      }
      this.ensureConnection();
      return { healthy: false, reconnected: true, why: "no-socket" };
    }
    const ws = this.ws;
    const sentAt = Date.now();
    this.sendFrame([null, String(sentAt), "phoenix", "heartbeat", {}]);
    const answered = await new Promise(resolve => {
      const started = Date.now();
      const tick = () => {
        if (this.ws !== ws || !this.isConnected()) return resolve(false);
        if ((this.lastFrameAt || 0) >= sentAt) return resolve(true);
        if (Date.now() - started >= PROBE_TIMEOUT_MS) return resolve(false);
        const t = setTimeout(tick, 100);
        t.unref?.();
      };
      tick();
    });
    this.stats.revalidations = (this.stats.revalidations || 0) + 1;
    if (answered) {
      logger.stream(`[${why}] socket vẫn trả lời heartbeat trong ${Date.now() - sentAt}ms — GIỮ kết nối`);
      return { healthy: true, reconnected: false, why: "heartbeat-ok" };
    }
    if (this.ws !== ws) return { healthy: false, reconnected: false, why: "socket-replaced" };
    logger.stream(`[${why}] không có phản hồi heartbeat sau ${PROBE_TIMEOUT_MS}ms — kết nối lại`);
    this.stats.silentRecoveries++;
    // Close through the normal path so handleClose marks the gap and the
    // replacement socket tells the engines exactly once.
    try { ws.close(); } catch { /* reconnect supervisor owns recovery */ }
    return { healthy: false, reconnected: true, why: "heartbeat-timeout" };
  }

  /**
   * Detect the "connected but deaf" state: the socket is open and the topics
   * look joined, yet nothing arrives. Seen live - a connection came up and then
   * delivered zero events. Without this the bot would just quietly stop
   * reacting in realtime and fall back to polling, with no sign in the UI.
   */
  checkSilence() {
    if (!this.isConnected()) return;
    if (this.collections.size === 0) return;

    // ONE definition of silence, shared with health(). Two of them - one
    // counting every frame, the other counting only real events - is how the
    // status line and the recovery can disagree about the same socket, and the
    // one that reconnects must be the strict one.
    // Market silence is valid. A transport/subscription proof is not: if no
    // heartbeat ACK, join ACK, or meaningful event has arrived for the full
    // window, the OPEN socket is deaf and must be allowed to reconnect. A
    // healthy quiet market has a fresh heartbeat and therefore never enters
    // this branch.
    const now = Date.now();
    const proof = this.transportProofAt();
    if (proof > 0 && now - proof >= SILENCE_TIMEOUT_MS) {
      logger.stream(`không có frame nào (kể cả heartbeat) trong ${Math.round((now - proof) / 1000)}s — socket chết, kết nối lại`);
      this.silenceWarned = true;
      this.stats.silentRecoveries++;
      try { this.ws.close(); } catch { /* reconnect supervisor owns recovery */ }
      return;
    }
    this.silenceWarned = false;
  }

  isConnected() {
    return Boolean(this.ws && this.ws.readyState === 1);
  }

  start(collectionSlugs = []) {
    this.closedByUser = false;
    this.collections = new Set(
      (collectionSlugs || []).map(v => String(v || "").trim().toLowerCase()).filter(Boolean)
    );
    this.trackedSlugs = new Set([...this.collections].map(v => v.toLowerCase()));

    // ---- the supervisor runs even when there is nothing to connect to ----
    //
    // It used to be armed only AFTER a successful connect, and both early
    // returns below skipped it. A bot launched with no NFTs yet - a fresh
    // install, or anyone who adds tokens after opening the app - therefore
    // left this timer null for ever: `setCollections` only asked an
    // already-open socket to re-subscribe, so nothing was left that could
    // notice a key had arrived or a collection had been added. The socket
    // never opened, no error was printed, and the bot ran on polling alone
    // with the UI reporting a stream that had simply never been tried.
    //
    // So supervision is unconditional and connecting is its job, not
    // start()'s. The two early returns below now only explain themselves.
    clearInterval(this.syncTimer);
    this.syncTimer = setInterval(() => {
      this.ensureConnection();
      this.syncSubscriptions();
      this.checkSilence();
    }, 10000);
    this.syncTimer.unref?.();

    const apiKey = String(this.getApiKey() || "").trim();
    if (!apiKey) {
      logger.stream("Thiếu Primary API key -> Stream chưa kết nối. Sẽ tự kết nối khi có key.");
      return;
    }

    if (this.collections.size === 0) {
      logger.stream("chưa có collection nào -> chưa mở stream. Sẽ tự mở khi thêm NFT.");
      return;
    }

    this.connect();
  }

  /**
   * Open the socket if it should be open and is not.
   *
   * The one place that decides, so "should there be a connection" cannot be
   * answered differently in three callers. Cheap, and safe to call often:
   * `connect()` refuses when a socket already exists.
   */
  ensureConnection() {
    if (this.closedByUser) return;
    if (this.authFailed) return;
    if (this.ws) return;
    if (this.reconnectTimer) return;              // a reconnect is already due
    if (this.collections.size === 0) return;
    if (!String(this.getApiKey() || "").trim()) return;

    logger.stream("có key và collection -> mở stream");
    this.connect();
  }

  stop() {
    this.closedByUser = true;

    clearTimeout(this.reconnectTimer);
    clearInterval(this.heartbeatTimer);
    clearInterval(this.syncTimer);
    clearTimeout(this.rejoinTimer);
    this.reconnectTimer = null;
    this.heartbeatTimer = null;
    this.syncTimer = null;
    this.rejoinTimer = null;

    this.joined.clear();

    if (this.ws) {
      try {
        this.ws.close();
      } catch {
        /* ignore */
      }
    }
    this.ws = null;
  }

  setCollections(collectionSlugs = []) {
    this.collections = new Set(
      (collectionSlugs || []).map(v => String(v || "").trim().toLowerCase()).filter(Boolean)
    );
    this.trackedSlugs = new Set([...this.collections].map(v => v.toLowerCase()));
    // Adding the first NFT is what makes a connection worth having, so this is
    // where one is opened - `syncSubscriptions` alone can only talk to a socket
    // that already exists.
    this.ensureConnection();
    this.syncSubscriptions();
  }

  /**
   * Treat the current socket as dead and rebuild it now.
   *
   * For the cases where WAITING to find out is the wrong answer: the machine
   * has just come back from sleep, or the OS reports the network came back. A
   * suspended laptop's TCP connection usually looks perfectly open from this
   * side and delivers nothing, and the silence watchdog would take ninety
   * seconds to notice - ninety seconds of a bot that is running, subscribed,
   * and deaf.
   *
   * Idempotent and allocation-free with respect to lifecycle: it reuses
   * stop()/start(), which clear every timer they own before re-arming, so
   * calling it a thousand times leaves one socket, one heartbeat and one sync
   * timer. It deliberately does NOT clear `authFailed`: a rejected key is
   * still rejected after a resume, and hammering it would just log the same
   * 401 forever.
   *
   * @param {string} why for the log, so a reconnect can be attributed
   */
  forceReconnect(why = "forced") {
    if (this.closedByUser) return false;
    if (this.authFailed) {
      logger.stream(`reconnect (${why}) bỏ qua: API key đang bị từ chối`);
      return false;
    }

    const slugs = Array.from(this.collections);
    logger.stream(`RECONNECT (${why}) · ${slugs.length} collection(s)`);
    this.stats.forcedReconnects = (this.stats.forcedReconnects || 0) + 1;
    if (this.isConnected()) this.gapPending = true;

    // stop() sets closedByUser; start() clears it. Between them every timer
    // this object owns is cleared, so nothing from the old socket survives.
    this.stop();
    this.start(slugs);
    return true;
  }

  /**
   * Nối lại khi TẬP API key đổi.
   *
   *   Bản trước so khoá ĐANG ĐƯỢC CHỌN của bộ xoay vòng với khoá đang dùng
   *   cho socket. Với hai khoá trở lên, bộ chọn đổi khoá theo tải một cách
   *   hoàn toàn hợp lệ, nên phép so ấy nói "đổi rồi" mỗi lần được hỏi — và
   *   nhật ký khởi động lặp "API key changed -> reconnecting stream" vài lần
   *   trong vài giây trong khi người dùng không đụng vào gì.
   *
   *   Thứ quyết định có phải nối lại hay không là TẬP khoá đã cấu hình. Khoá
   *   nào được chọn cho socket lần này là chuyện của bộ chọn, không phải một
   *   thay đổi cấu hình.
   */
  refreshKey() {
    if (this.closedByUser) return;
    const signature = typeof this.getKeySignature === "function"
      ? String(this.getKeySignature() || "")
      : null;
    if (signature !== null) {
      if (signature === this.keySignature) return;
      this.keySignature = signature;
      /**
       * POOL CHANGE != TRANSPORT FAILURE (1.25.1)
       *
       *   Adding Key 2 changes the key-set signature. A healthy socket on a key
       *   that is STILL configured keeps running - only a removed/replaced
       *   active key (or auth failure) reconnects.
       */
      if (!this.authFailed && this.activeKey && this.isKeyConfiguredFn &&
          this.isKeyConfiguredFn(this.activeKey) && (this.isConnected() || this.connecting)) {
        logger.stream("API key pool changed, active Stream key still configured -> giữ kết nối");
        return;
      }
    } else {
      const next = String(this.getApiKey() || "").trim();
      if (next === this.activeKey) return;
    }

    // A different key may well be accepted, so let it try again.
    this.authFailed = false;
    this.authRetryLogged = false;
    logger.stream("API key changed -> reconnecting stream.");
    if (this.isConnected()) this.gapPending = true;
    const slugs = Array.from(this.collections);
    this.stop();
    this.start(slugs);
  }

  connect() {
    // SINGLE FLIGHT. `this.ws` is set the moment the socket object exists, but
    // there is a window between deciding to connect and that assignment, and
    // two callers arriving inside it would each open a socket - the second
    // overwriting the first, which then delivers to a handler that ignores it
    // and is never closed. One connection at a time, and the flag is cleared
    // in every exit including open, close and error.
    if (this.closedByUser || this.ws || this.connecting) return;

    const apiKey = String(this.getApiKey() || "").trim();
    if (!apiKey) return;

    this.connecting = true;
    this.activeKey = apiKey;

    const url = `${STREAM_URL}?token=${encodeURIComponent(apiKey)}&vsn=2.0.0`;

    let ws;
    try {
      ws = new RawWebSocket(url, {
        onOpen: () => this.handleOpen(ws),
        onMessage: data => this.handleMessage(data),
        onError: error => this.handleError(error),
        onClose: (code, reason) => this.handleClose(ws, code, reason)
      });
      this.ws = ws;
    } catch (error) {
      logger.stream(`connect error: ${error.message}`);
      this.ws = null;
      this.connecting = false;
      this.scheduleReconnect();
    }
  }

  handleOpen(ws) {
    this.connecting = false;
    if (this.ws !== ws) return;

    this.stats.connectedAt = Date.now();
    this.lastEventAt = Date.now();
    this.lastFrameAt = Date.now();
    // Deliberately NOT lastMeaningfulEventAt: connecting is not delivering,
    // and treating it as such is how a socket that comes up deaf reads as
    // healthy for its first ninety seconds, every ninety seconds.
    this.silenceWarned = false;
    this.joined.clear();

    logger.stream(`connected (${this.collections.size} collection(s))`);
    // A socket outage loses events for every previously joined topic. The
    // reconnect callback only records the gap; each topic needs its own
    // targeted authority repair after its new join ACK.
    if (this.gapPending) {
      for (const slug of this.wantedTopics()) {
        if (this.joinState.get(slug)?.everJoined) this.topicGapPending.add(slug);
      }
    }
    for (const js of this.joinState.values()) if (js.state !== "BACKOFF") js.state = "NOT_JOINED";
    this.syncSubscriptions();

    clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = setInterval(() => {
      if (this.ws === ws && ws.readyState === 1) {
        this.sendFrame([null, String(Date.now()), "phoenix", "heartbeat", {}]);
      }
    }, HEARTBEAT_MS);
    this.heartbeatTimer.unref?.();

    /**
     * ONE GAP, ONE NOTIFICATION — AFTER THE REPLACEMENT IS UP
     *
     *   The engines used to be told from scheduleReconnect, i.e. on every
     *   backoff attempt while the network was still down: each failed attempt
     *   re-queued a Best read for every row. Now the drop only marks the gap,
     *   and the engines hear about it once, when a socket is open again and
     *   events can flow (they buffer, re-check, then replay).
     */
    if (this.gapPending) {
      this.gapPending = false;
      try { this.onReconnect(); } catch (error) { logger.error(`stream reconnect hook: ${error.message}`); }
    }
  }

  handleMessage(data) {
    const receivedMono = Number(process.hrtime.bigint()) / 1e6;
    this.stats.framesReceived++;
    this.stats.bytesReceived += Buffer.byteLength(String(data));
    let message;
    try {
      message = JSON.parse(String(data));
    } catch {
      return;
    }

    let frame = null;

    if (Array.isArray(message)) {
      // Phoenix v2: [join_ref, ref, topic, event, payload]
      frame = {
        ref: message[1] || null,
        topic: message[2] || "",
        event: message[3] || "",
        payload: message[4] || {}
      };
    } else if (message && typeof message === "object") {
      frame = {
        ref: message.ref || null,
        topic: message.topic || "",
        event: message.event || "",
        payload: message.payload || {}
      };
    }

    if (!frame || !frame.event) return;
    // ANY frame is transport proof - including a heartbeat reply.
    this.lastFrameAt = Date.now();
    if (frame.topic === "phoenix" && frame.event === "heartbeat") return;

    // A refused phx_join is the app's worst silent failure: the socket stays
    // "connected" and simply never delivers an event. Surface it and let
    // syncSubscriptions retry the topic.
    if (frame.event === "phx_reply") {
      const status = String(frame.payload?.status || "");

      if (status === "ok") {
        // The two kinds of "ok" answer different questions and are recorded
        // apart: a heartbeat reply proves the SOCKET is alive, a join reply
        // proves the SUBSCRIPTION took. Neither proves events are arriving,
        // which is why neither touches lastMeaningfulEventAt.
        if (frame.topic === "phoenix") this.lastHeartbeatAt = Date.now();
        else if (String(frame.topic || "").startsWith("collection:")) {
          this.lastSubscriptionAt = Date.now();
          // JOINED only on an ok reply to THIS topic's pending join.
          const slug = String(frame.topic).replace(/^collection:/, "");
          const js = this.joinState.get(slug);
          if (js && js.state === "JOINING" && (!frame.ref || !js.ref || String(frame.ref) === String(js.ref))) {
            js.state = "JOINED";
            js.failures = 0;
            js.retryAt = 0;
            js.outageReported = false;
            logger.stream(`[TOPIC_JOINED] collection:${slug}${js.everJoined ? " (rejoin)" : ""}`);
            // A topic that WAS joined, dropped on protocol evidence and is now
            // back: exactly one targeted re-check for this collection.
            // FIRST join of this topic: a row whose first REST read completed
            // before now may have missed events between that read and this
            // ACK - no gap is reported for a first join, so say it here.
            const covered = slug === GLOBAL_TOPIC ? [...this.collections] : [slug];
            if (!js.everJoined) {
              for (const s of covered) { try { this.onTopicJoined(s, Date.now()); } catch (error) { logger.stream(`topic joined (${s}): ${error.message}`); } }
            }
            if (js.everJoined && this.topicGapPending.delete(slug)) {
              this.stats.topicRejoins++;
              for (const s of covered) { try { this.onTopicGap(s); } catch (error) { logger.stream(`topic gap (${s}): ${error.message}`); } }
            }
            js.everJoined = true;
          }
        }
        return;
      }

      if (status && status !== "ok") {
        const slug = String(frame.topic || "").replace(/^collection:/, "");
        this.stats.joinErrors++;
        this.stats.joinRefused = (this.stats.joinRefused || 0) + 1;
        if (slug) this.joinBackoff(slug, JSON.stringify(frame.payload?.response || {}));
      }
      return;
    }

    // The channel crashed server-side; drop it so the next sync rejoins.
    if (frame.event === "phx_error" || frame.event === "phx_close") {
      const slug = String(frame.topic || "").replace(/^collection:/, "");
      if (slug && this.joined.delete(slug)) {
        // Protocol evidence that THIS topic may have missed events - but only a
        // topic that had actually JOINED can have a gap. Rejoin at next sync;
        // the gap is reported once the rejoin is ACKed.
        const js = this.joinState.get(slug);
        if (js && js.everJoined) this.topicGapPending.add(slug);
        if (js && !js.outageReported) {
          js.outageReported = true;
          try { this.onTopicUnavailable(slug, Date.now(), frame.event); }
          catch (error) { logger.stream(`topic unavailable (${slug}): ${error.message}`); }
        }
        if (js) js.state = "NOT_JOINED";
        logger.stream(`channel ${frame.event} for "${slug}" -> will rejoin`);
        // Protocol evidence of a lost topic: rejoin NOW, not at the next 10s
        // sync (up to 10s deaf for that collection). 500ms, or 5s when the same
        // topic already failed within 10s - never a tight rejoin loop (1.25.12).
        const lastErr = this.topicErrorAt.get(slug) || 0;
        const now = Date.now();
        this.topicErrorAt.set(slug, now);
        const delay = now - lastErr < 10000 ? 5000 : 500;
        if (!this.rejoinTimer) {
          this.rejoinTimer = setTimeout(() => {
            this.rejoinTimer = null;
            if (this.ws && this.ws.readyState === 1) this.syncSubscriptions();
          }, delay);
          this.rejoinTimer.unref?.();
        }
      }
      return;
    }

    // EARLY FILTER of the global feed: event family and tracked slug, read
    // straight from the frame - nothing is decoded for untracked collections.
    if (this.globalMode) {
      this.globalStats.received++;
      if (!HANDLED_EVENTS.has(frame.event)) { this.globalStats.droppedType++; this.stats.eventTypeDropped++; this.lastMeaningfulEventAt = Date.now(); return; }
      const inner = frame.payload && (frame.payload.payload || frame.payload);
      const s = String((inner && ((inner.collection && inner.collection.slug) || (inner.item && inner.item.collection && inner.item.collection.slug))) || "").toLowerCase();
      if (!s || !this.trackedSlugs.has(s)) { this.globalStats.dropped++; this.stats.untrackedDropped++; this.lastMeaningfulEventAt = Date.now(); return; }
      this.globalStats.matched++;
    }
    this.stats.events++;
    this.lastEventAt = Date.now();
    // A MEANINGFUL event - not a heartbeat, not a join reply. Only this kind
    // proves the subscription is actually delivering, which is what health is
    // asking about.
    this.lastMeaningfulEventAt = this.lastEventAt;

    const decoded = decodeEvent(frame);
    if (decoded) {
      decoded.receivedMono = receivedMono;
      decoded.decodedMono = Number(process.hrtime.bigint()) / 1e6;
    }
    if (!decoded) return;

    this.stats.handled++;
    if (decoded.collectionSlug) this.lastEventBySlug.set(decoded.collectionSlug, this.lastEventAt);

    try {
      this.onEvent(decoded);
    } catch (error) {
      logger.stream(`event handler error: ${error.message}`);
    }
  }

  handleError(error) {
    const message = error?.message || "socket error";
    logger.stream(`socket error: ${message}`);

    // A rejected handshake is not a transient blip. The key is wrong, and it
    // will still be wrong in thirty seconds, so retrying on a timer only fills
    // the log with the same line for ever. Reconnecting resumes by itself the
    // moment the key changes - refreshKey() clears this flag.
    if (/\b401\b|unauthorized|403|forbidden/i.test(message)) {
      const firstTime = !this.authFailed;
      this.authFailed = true;
      if (!firstTime) return;   // đã nói rồi; lần thử lại thưa tự lo
      logger.error(
        "[STREAM] OpenSea từ chối API key (401). Stream tạm dừng kết nối lại — " +
        "nhập API key hợp lệ trong Settings để bật lại. " +
        "Offer SLL và Cancel SLL KHÔNG dùng Stream nên vẫn chạy bình thường."
      );
    }
  }

  handleClose(ws, code, reason) {
    this.connecting = false;
    if (this.ws !== ws) return;

    const openedAt = this.stats.connectedAt || 0;
    const hadTransportProof = Math.max(this.lastHeartbeatAt || 0, this.lastSubscriptionAt || 0, this.lastMeaningfulEventAt || 0) >= openedAt;
    if (openedAt > 0 && Date.now() - openedAt >= 60000 && hadTransportProof) this.backoff = 1000;

    logger.stream(`disconnected code=${code || ""}${reason ? ` reason=${reason}` : ""}`);

    clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
    this.ws = null;
    this.joined.clear();

    // A socket that WAS open is gone: events published meanwhile are lost.
    if (!this.closedByUser && this.stats.connectedAt) this.gapPending = true;
    if (!this.closedByUser) this.scheduleReconnect();
  }

  scheduleReconnect() {
    clearTimeout(this.reconnectTimer);

    /**
     * MỘT LẦN 401/403 KHÔNG ĐƯỢC GIẾT STREAM MÃI MÃI
     *
     *   Bản trước dừng hẳn mọi kết nối lại sau một handshake bị từ chối, với
     *   lý do "key sai thì 30 giây nữa vẫn sai". Đúng cho key sai; SAI cho
     *   một 403 tạm thời của Cloudflare/WAF hay một phút key bị giới hạn —
     *   những thứ có thật khi treo tool nhiều ngày. Hậu quả: Stream chết im,
     *   engine rơi về đọc REST định kỳ, và người dùng phải bấm Bắt đầu lại
     *   mà không hiểu vì sao.
     *
     *   Giữ nguyên nhãn FAILED cho giao diện và log một lần, nhưng vẫn thử
     *   lại — thưa (5 phút), không phải vòng lặp chặt. Key đổi thì thử ngay.
     */
    if (this.authFailed) {
      const delay = AUTH_RETRY_MS;
      if (!this.authRetryLogged) {
        this.authRetryLogged = true;
        logger.stream(`API key bị từ chối — sẽ thử kết nối lại mỗi ${Math.round(delay / 60000)} phút`);
      }
      this.stats.reconnects++;
      this.lastReconnectAt = Date.now();
      this.reconnectTimer = setTimeout(() => {
        this.reconnectTimer = null;
        // Cho handshake một cơ hội mới; bị từ chối nữa thì handleError đặt lại cờ.
        this.authFailed = false;
        this.connect();
      }, delay);
      this.reconnectTimer.unref?.();
      return;
    }

    // Nothing to listen for. Holding a socket open for zero collections costs a
    // connection, a heartbeat and a reconnect loop, and buys nothing.
    if (!this.collections || this.collections.size === 0) {
      logger.stream("không có collection nào -> không mở lại socket");
      return;
    }

    const delay = Math.min(this.backoff, 30000);
    this.stats.reconnects++;
    this.lastReconnectAt = Date.now();

    // Events published while the socket was down are simply gone; the engines
    // are told ONCE, when the replacement socket opens (see handleOpen).

    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
    this.reconnectTimer.unref?.();

    this.backoff = Math.min(this.backoff * 2, 30000);
  }

  /**
   * NO TOPIC-SILENCE REJOIN (1.25.0)
   *
   *   1.24.x left and rejoined any collection silent for 10 minutes while
   *   another collection delivered, then asked the engines for a REST
   *   re-check. A quiet collection is normal: "topic A has no bids while
   *   topic B does" is not evidence that A is dead. Topics are now rejoined
   *   only on protocol evidence - phx_error / phx_close / a refused join (see
   *   handleMessage) - or with the whole socket.
   */

  /** A join was refused (or never answered): back off THIS slug only. */
  joinBackoff(slug, detail = "") {
    const prev = this.joinState.get(slug) || { everJoined: false, failures: 0 };
    if (prev.everJoined) {
      this.topicGapPending.add(slug);
    }
    const outageReported = Boolean(prev.outageReported);
    if (!outageReported) {
      try { this.onTopicUnavailable(slug, Date.now(), `join-refused:${String(detail).slice(0, 120)}`); }
      catch (error) { logger.stream(`topic unavailable (${slug}): ${error.message}`); }
    }
    const failures = (prev.failures || 0) + 1;
    if (slug === GLOBAL_TOPIC && this.globalMode && failures >= GLOBAL_REFUSALS_BEFORE_FALLBACK) {
      logger.stream(`[GLOBAL] join collection:* refused ${failures}x (${String(detail).slice(0, 120)}) -> per-collection topics`);
      this.globalMode = false;
      this.joined.delete(slug);
      this.joinState.delete(slug);
      this.syncSubscriptions();
      return;
    }
    const wait = JOIN_BACKOFF_MS[Math.min(JOIN_BACKOFF_MS.length - 1, failures - 1)];
    this.joined.delete(slug);
    this.joinState.set(slug, { ...prev, state: "BACKOFF", failures, outageReported: true, retryAt: Date.now() + wait });
    // Log the first refusal and then only when the backoff step changes.
    if (failures <= JOIN_BACKOFF_MS.length) {
      logger.stream(`join refused for "${slug}" (${failures}): ${String(detail).slice(0, 160)} -> thử lại sau ${Math.round(wait / 1000)}s`);
    }
  }

  /** Join newly watched collections, leave the ones nobody watches any more. */
  /** Topics this client must be joined to: `*` in global mode, else one per collection. */
  wantedTopics() {
    if (!this.globalMode) return this.collections;
    return this.collections.size ? new Set([GLOBAL_TOPIC]) : new Set();
  }

  syncSubscriptions() {
    if (!this.isConnected()) return;
    const wanted = this.wantedTopics();

    for (const slug of Array.from(this.joined)) {
      if (wanted.has(slug)) continue;
      this.sendFrame([null, String(Date.now()), `collection:${slug}`, "phx_leave", {}]);
      this.joined.delete(slug);
      this.joinedAt.delete(slug);
      this.lastEventBySlug.delete(slug);
      this.joinState.delete(slug);
    }

    const now = Date.now();
    for (const slug of wanted) {
      const js = this.joinState.get(slug);
      // A join with no reply for too long is a refusal, not a success.
      if (js && js.state === "JOINING" && now - js.sentAt > JOIN_ACK_TIMEOUT_MS) {
        this.joinBackoff(slug, "no reply");
        continue;
      }
      if (this.joined.has(slug)) continue;
      if (js && js.state === "BACKOFF" && now < js.retryAt) continue;
      const ref = String(++this.joinRef);
      this.sendFrame([ref, ref, `collection:${slug}`, "phx_join", { event_types: GLOBAL_EVENT_TYPES }]);
      this.joined.add(slug);
      this.joinedAt.set(slug, now);
      this.joinState.set(slug, { state: "JOINING", ref, sentAt: now,
        everJoined: Boolean(js && js.everJoined), failures: js ? js.failures : 0,
        outageReported: Boolean(js && js.outageReported), retryAt: 0 });
    }
    for (const slug of this.topicGapPending) if (!wanted.has(slug)) this.topicGapPending.delete(slug);
    for (const slug of this.joinState.keys()) if (!wanted.has(slug)) this.joinState.delete(slug);
    for (const slug of this.topicErrorAt.keys()) if (!wanted.has(slug)) this.topicErrorAt.delete(slug);
  }

  sendFrame(frame) {
    if (!this.isConnected()) return;
    try {
      this.ws.send(JSON.stringify(frame));
    } catch {
      /* the close handler reconnects */
    }
  }

  status() {
    const now = Date.now();
    return {
      connected: this.isConnected(),
      health: this.health(now),
      collections: this.collections.size,
      joined: this.joined.size,
      // Ages rather than timestamps: a caller comparing a timestamp against
      // its own clock is a clock-skew bug waiting for a laptop to sleep.
      sinceEventMs: this.lastMeaningfulEventAt ? now - this.lastMeaningfulEventAt : -1,
      sinceFrameMs: this.lastFrameAt ? now - this.lastFrameAt : -1,
      // Global feed (1.25.14): mode + cumulative early-filter counters.
      global: { mode: this.globalMode ? "collection:*" : "per-collection", tracked: this.trackedSlugs.size, ...this.globalStats },
      feed: { mode: this.globalMode ? "collection:*" : "per-collection", trackedTopics: this.trackedSlugs.size,
        joinedTopics: Array.from(this.joined).slice(0, 256), ...this.globalStats },
      subscriptions: (() => { const c = { JOINING: 0, JOINED: 0, BACKOFF: 0, NOT_JOINED: 0 };
        for (const js of this.joinState.values()) c[js.state] = (c[js.state] || 0) + 1; return c; })(),
      sinceHeartbeatMs: this.lastHeartbeatAt ? now - this.lastHeartbeatAt : -1,
      sinceSubscribedMs: this.lastSubscriptionAt ? now - this.lastSubscriptionAt : -1,
      sinceReconnectMs: this.lastReconnectAt ? now - this.lastReconnectAt : -1,
      ...this.stats
    };
  }
}

module.exports = { OpenSeaStream, RawWebSocket, decodeEvent, parseNftId, HANDLED_EVENTS };
