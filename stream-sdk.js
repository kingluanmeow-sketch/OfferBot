"use strict";

// Production Stream adapter. OpenSea's maintained SDK owns Phoenix framing,
// heartbeat, reconnect and resubscribe. This adapter adds OfferBot filtering,
// gap recovery hooks and bounded, secret-safe diagnostics.
const { OpenSeaStreamClient, EventType, LogLevel } = require("@opensea/sdk/stream");
const WebSocket = require("ws");
const { decodeEvent, HANDLED_EVENTS } = require("./stream");
const { logger } = require("./logger");
const productionTrace = require("./production-trace");

const EVENT_TYPES = Object.freeze([
  EventType.ITEM_RECEIVED_BID,
  EventType.COLLECTION_OFFER,
  EventType.TRAIT_OFFER,
  EventType.ITEM_CANCELLED,
  EventType.ORDER_INVALIDATE,
  EventType.ORDER_REVALIDATE
]);
// Observed live 2026-09-29: the subscription ACKs, sale/transfer frames flow,
// yet no order event arrives. A joined socket without order events cannot see
// competitors, so it is DEGRADED (targeted REST fallback) until order events
// are flowing again. Market-wide counts: tracked collections can be quiet.
const ORDER_BLIND_MS = 90 * 1000;
const ORDER_RESUME_EVENTS = 5;
const ORDER_RESUME_WINDOW_MS = 60 * 1000;
const CONTROL_EVENTS = new Set(["phx_reply", "phx_error", "phx_close", "phx_join", "phx_leave", "heartbeat"]);

function safeText(value, max = 180) {
  return String(value || "")
    .replace(/([?&](?:token|api[_-]?key)=)[^&\s"']+/gi, "$1[redacted]")
    .replace(/\b[A-Za-z0-9_-]{32,}\b/g, "[redacted]")
    .replace(/[\r\n\t]+/g, " ")
    .slice(0, max);
}

function eventSlug(event) {
  const payload = event && event.payload;
  return String(payload && payload.collection && payload.collection.slug ||
    payload && payload.item && payload.item.collection && payload.item.collection.slug || "")
    .trim().toLowerCase();
}

class OpenSeaStream {
  constructor(options = {}, dependencies = {}) {
    this.getApiKey = typeof options.getApiKey === "function" ? options.getApiKey : () => "";
    this.getKeySignature = typeof options.getKeySignature === "function" ? options.getKeySignature : null;
    this.isKeyConfigured = typeof options.isKeyConfigured === "function" ? options.isKeyConfigured : null;
    this.onEvent = typeof options.onEvent === "function" ? options.onEvent : () => {};
    this.onReconnect = typeof options.onReconnect === "function" ? options.onReconnect : () => {};
    this.onTopicGap = typeof options.onTopicGap === "function" ? options.onTopicGap : () => {};
    this.onTopicJoined = typeof options.onTopicJoined === "function" ? options.onTopicJoined : () => {};
    this.onTopicUnavailable = typeof options.onTopicUnavailable === "function" ? options.onTopicUnavailable : () => {};
    // 1.25.31: subscribe tracked collections instead of the "*" firehose.
    // Live: "*" pushed 640 MB / 354k events in minutes (97% untracked) and the
    // server closed the socket every 2-3 s with 4500 "configured send limit".
    this.perCollection = options.perCollection === true;
    this.topicUnsubs = new Map();
    this.refusedTopics = new Set();
    this.label = String(options.label || "");
    this.Client = dependencies.OpenSeaStreamClient || OpenSeaStreamClient;
    this.Types = dependencies.EventType || EventType;
    this.WebSocket = dependencies.WebSocket || WebSocket;

    this.client = null;
    this.unsubscribe = null;
    this.activeSocket = null;
    this.clientGeneration = 0;
    this.started = false;
    this.closedByUser = true;
    this.authFailed = false;
    this.connecting = false;
    this.subscriptionActive = false;
    this.everActive = false;
    this.gapStartedAt = 0;
    this.lastFrameAt = 0;
    this.lastMarketplaceEventAt = 0;
    this.lastHeartbeatAt = 0;
    this.lastHeartbeatRttMs = -1;
    this.lastFrameType = "";
    this.lastError = "";
    this.lastKeySignature = this.getKeySignature ? String(this.getKeySignature() || "") : "";
    this.collections = new Set();
    this.trackedSlugs = new Set();
    this.reconnectTimer = null;
    this.joinAckTimer = null;
    this.retryAttempt = 0;
    this.eventSequence = 0;
    this.orderBlind = false;
    this.orderBlindTimer = null;
    this.subscriptionActiveAt = 0;
    this.lastOrderEventAt = 0;
    this.orderResumeTimes = [];
    this.recentBuckets = Array.from({ length: 61 }, () => ({ second: -1, total: 0, matched: 0 }));
    this.heartbeatRefs = new Map();
    this.frameTypes = Object.create(null);
    this.stats = {
      framesReceived: 0, bytesReceived: 0, controlFrames: 0,
      marketplaceEvents: 0, matchedEvents: 0, untrackedEvents: 0,
      handlerErrors: 0, reconnects: 0, resubscribeCount: 0,
      joinRefused: 0, gapRecoveries: 0
    };
  }

  start(collectionSlugs = []) {
    this.closedByUser = false;
    this.started = true;
    this.setCollectionSet(collectionSlugs);
    this.everActive = false;
    this.authFailed = false;
    this.ensureClient();
  }

  setCollectionSet(slugs) {
    const next = new Set((slugs || []).map(x => String(x || "").trim().toLowerCase()).filter(Boolean));
    const previous = this.collections;
    this.collections = next;
    this.trackedSlugs = new Set(next);
    if (this.subscriptionActive) {
      const at = Date.now();
      for (const slug of next) if (!previous.has(slug)) {
        try { this.onTopicJoined(slug, at); } catch (error) { this.recordHandlerError(error, "new collection recovery"); }
      }
    }
  }

  setCollections(slugs = []) {
    this.setCollectionSet(slugs);
    if (!this.started || this.closedByUser) return;
    if (!this.collections.size) this.stopClient();
    else if (!this.ensureClient()) this.syncTopics();
  }

  ensureClient() {
    if (!this.started || this.closedByUser || this.client || !this.collections.size) return false;
    const apiKey = String(this.getApiKey() || "").trim();
    if (!apiKey) { this.connecting = false; return false; }
    this.clientKey = apiKey;
    const generation = ++this.clientGeneration;
    const owner = this;
    const BaseWebSocket = this.WebSocket;
    class ObservedWebSocket extends BaseWebSocket {
      constructor(url, ...args) {
        super(url, ...args);
        owner.activeSocket = this;
        this.on("open", () => owner.onSocketOpen(this, generation));
        this.on("message", (data) => owner.onSocketMessage(this, generation, data));
        this.on("close", (code, reason) => owner.onSocketClose(this, generation, code, reason));
        this.on("error", error => owner.onSocketError(this, generation, error));
      }
      send(data, ...args) {
        owner.observeOutgoing(this, generation, data);
        return super.send(data, ...args);
      }
    }
    try {
      this.connecting = true;
      this.client = new this.Client({
        apiKey,
        connectOptions: {
          transport: ObservedWebSocket,
          timeout: 10000,
          heartbeatIntervalMs: 30000,
          reconnectAfterMs: tries => {
            const base = Math.min(500 * (2 ** Math.max(0, tries - 1)), 30000);
            return Math.max(250, Math.round(base * (0.8 + Math.random() * 0.4)));
          }
        },
        logLevel: LogLevel.ERROR,
        onError: error => this.onSdkError(generation, error)
      });
      if (this.perCollection) {
        this.topicUnsubs.clear();
        for (const slug of this.collections) this.subscribeTopic(slug, generation);
        this.unsubscribe = () => { for (const u of this.topicUnsubs.values()) { try { u(); } catch {} } this.topicUnsubs.clear(); };
      } else {
        this.unsubscribe = this.client.onEvents("*", this.eventTypes(), event => this.handleSdkEvent(generation, event));
      }
      this.lastError = "";
      this.retryAttempt = 0;
      logger.stream(`[STREAM] official SDK v12.10.2 · mode=${this.perCollection ? "official-collections" : "official-global"}${this.label} · tracked=${this.collections.size}`);
      return true;
    } catch (error) {
      this.recordHandlerError(error, "SDK initialization");
      this.stopClient();
      this.scheduleClientRetry("sdk-init");
      return false;
    }
  }

  eventTypes() {
    return [this.Types.ITEM_RECEIVED_BID, this.Types.COLLECTION_OFFER, this.Types.TRAIT_OFFER,
      this.Types.ITEM_CANCELLED, this.Types.ORDER_INVALIDATE, this.Types.ORDER_REVALIDATE];
  }

  subscribeTopic(slug, generation = this.clientGeneration) {
    if (!this.client || this.topicUnsubs.has(slug)) return;
    this.topicUnsubs.set(slug, this.client.onEvents(slug, this.eventTypes(), event => this.handleSdkEvent(generation, event)));
  }

  /** Per-collection mode: add/remove topics on the live socket, no restart. */
  syncTopics() {
    if (!this.perCollection || !this.client) return;
    for (const [slug, unsub] of [...this.topicUnsubs]) if (!this.collections.has(slug)) {
      try { unsub(); } catch {}
      this.topicUnsubs.delete(slug);
      this.refusedTopics.delete(slug);
    }
    for (const slug of this.collections) this.subscribeTopic(slug);
  }

  onSocketOpen(socket, generation) {
    if (generation !== this.clientGeneration || socket !== this.activeSocket) return;
    this.connecting = false;
    logger.stream(`[STREAM] socket open · awaiting global subscription ACK · tracked=${this.collections.size}`);
    this.watchGlobalJoinAck(generation);
  }

  watchGlobalJoinAck(generation) {
    clearTimeout(this.joinAckTimer);
    this.joinAckTimer = setTimeout(() => {
      this.joinAckTimer = null;
      if (generation !== this.clientGeneration || this.subscriptionActive || this.closedByUser) return;
      this.stats.joinRefused++;
      this.lastError = "global subscription acknowledgement timeout";
      this.beginGap("global-join-ack-timeout");
      this.restartAfterJoinFailure();
    }, 12000);
    this.joinAckTimer.unref?.();
  }

  onSocketClose(socket, generation, code, reason) {
    if (generation !== this.clientGeneration || socket !== this.activeSocket) return;
    const wasActive = this.subscriptionActive || this.everActive;
    this.activeSocket = null;
    this.subscriptionActive = false;
    clearTimeout(this.joinAckTimer);
    this.joinAckTimer = null;
    this.connecting = !this.closedByUser;
    this.stats.reconnects++;
    this.heartbeatRefs.clear();
    if (wasActive) this.beginGap(`socket-close-${Number(code) || 0}`);
    if (!this.closedByUser) logger.stream(`[STREAM] socket closed code=${Number(code) || 0} reason=${safeText(reason && reason.toString(), 100)}`);
  }

  onSocketError(socket, generation, error) {
    if (generation !== this.clientGeneration || socket !== this.activeSocket) return;
    this.onSdkError(generation, error);
  }

  onSdkError(generation, error) {
    if (generation !== this.clientGeneration) return;
    const message = safeText(error && error.message || error);
    this.lastError = message;
    this.stats.transportErrors = (this.stats.transportErrors || 0) + 1;
    if (/\b401\b|\b403\b|unauthorized|forbidden/i.test(message)) this.authFailed = true;
    logger.stream(`[STREAM] SDK transport error: ${message}`);
  }

  observeOutgoing(socket, generation, data) {
    if (generation !== this.clientGeneration || socket !== this.activeSocket || typeof data !== "string") return;
    let frame;
    try { frame = JSON.parse(data); } catch { return; }
    if (Array.isArray(frame) && frame[3] === "heartbeat" && frame[1] != null) {
      this.heartbeatRefs.set(String(frame[1]), Date.now());
      while (this.heartbeatRefs.size > 4) this.heartbeatRefs.delete(this.heartbeatRefs.keys().next().value);
    }
  }

  onSocketMessage(socket, generation, data) {
    if (generation !== this.clientGeneration || socket !== this.activeSocket) return;
    const raw = Buffer.isBuffer(data) ? data.toString("utf8") : String(data || "");
    this.stats.framesReceived++;
    this.stats.bytesReceived += Buffer.byteLength(raw);
    this.lastFrameAt = Date.now();
    // The SDK parses every frame already; only control frames matter here.
    if (!raw.includes("\"phx_")) {
      this.lastFrameType = "event";
      return;
    }
    let frame;
    try { frame = JSON.parse(raw); } catch { return; }
    if (!Array.isArray(frame) || frame.length < 5) return;
    const topic = String(frame[2] || "");
    const type = String(frame[3] || "").toLowerCase().replace(/[^a-z0-9_:-]/g, "").slice(0, 64);
    this.lastFrameType = type;
    if (Object.prototype.hasOwnProperty.call(this.frameTypes, type)) this.frameTypes[type]++;
    else if (Object.keys(this.frameTypes).length < 32) this.frameTypes[type] = 1;
    if (CONTROL_EVENTS.has(type)) this.stats.controlFrames++;
    if (type === "phx_reply" && topic === "phoenix") {
      this.lastHeartbeatAt = Date.now();
      const sent = this.heartbeatRefs.get(String(frame[1] || ""));
      if (sent) this.lastHeartbeatRttMs = this.lastHeartbeatAt - sent;
      this.heartbeatRefs.delete(String(frame[1] || ""));
      return;
    }
    if (this.perCollection && type === "phx_reply" && topic.startsWith("collection:") && topic !== "collection:*") {
      const slug = topic.slice("collection:".length);
      const status = String(frame[4] && frame[4].status || "").toLowerCase();
      if (status === "ok") {
        this.refusedTopics.delete(slug);
        if (!this.subscriptionActive) this.onGlobalSubscriptionReady();
      } else {
        this.stats.joinRefused++;
        this.refusedTopics.add(slug);
        this.lastError = safeText(JSON.stringify(frame[4] && frame[4].response || { status }));
        logger.stream(`[STREAM] topic ${safeText(slug, 80)} refused${this.label} status=${safeText(status, 32)} detail=${this.lastError}`);
        try { this.onTopicUnavailable(slug, Date.now(), "topic-join-refused"); } catch (error) { this.recordHandlerError(error, "topic refused hook"); }
      }
      return;
    }
    if (type === "phx_reply" && topic === "collection:*") {
      const status = String(frame[4] && frame[4].status || "").toLowerCase();
      if (status === "ok") this.onGlobalSubscriptionReady();
      else {
        this.stats.joinRefused++;
        this.lastError = safeText(JSON.stringify(frame[4] && frame[4].response || { status }));
        this.beginGap("global-join-refused");
        logger.stream(`[STREAM] global join refused status=${safeText(status, 32)} detail=${this.lastError}`);
        this.restartAfterJoinFailure();
      }
      return;
    }
    if ((type === "phx_error" || type === "phx_close") && topic === "collection:*") {
      this.subscriptionActive = false;
      this.beginGap(type);
      this.watchGlobalJoinAck(generation);
    }
  }

  onGlobalSubscriptionReady() {
    clearTimeout(this.joinAckTimer);
    this.joinAckTimer = null;
    const wasEverActive = this.everActive;
    this.subscriptionActive = true;
    this.subscriptionActiveAt = Date.now();
    this.watchOrderEvents();
    this.connecting = false;
    this.authFailed = false;
    this.retryAttempt = 0;
    this.stats.resubscribeCount += wasEverActive ? 1 : 0;
    const gap = this.gapStartedAt;
    // Still blind to order events: a re-ACK restores nothing the engine can
    // use. Keep the outage open; noteOrderEvent closes it when events return.
    if (gap && this.orderBlind) {
      this.everActive = true;
      logger.stream(`[STREAM] subscription re-ACK while order events are absent · stays DEGRADED`);
      return;
    }
    if (gap) {
      this.gapStartedAt = 0;
      this.stats.gapRecoveries++;
      setImmediate(() => {
        try { this.onReconnect(); } catch (error) { this.recordHandlerError(error, "reconnect hook"); }
        for (const slug of this.collections) {
          try { this.onTopicGap(slug, gap, Date.now()); }
          catch (error) { this.recordHandlerError(error, "gap reconciliation hook"); }
        }
      });
    } else if (!wasEverActive) {
      const joinedAt = Date.now();
      setImmediate(() => {
        for (const slug of this.collections) {
          try { this.onTopicJoined(slug, joinedAt); }
          catch (error) { this.recordHandlerError(error, "initial subscription hook"); }
        }
      });
    }
    this.everActive = true;
    logger.stream(`[STREAM] global subscription active · tracked=${this.collections.size}`);
  }

  watchOrderEvents() {
    if (this.orderBlindTimer) return;
    this.orderBlindTimer = setInterval(() => this.checkOrderBlind(), 5000);
    this.orderBlindTimer.unref?.();
  }

  checkOrderBlind(now = Date.now()) {
    if (this.closedByUser || !this.subscriptionActive || this.orderBlind) return false;
    if (now - Math.max(this.lastOrderEventAt, this.subscriptionActiveAt) < ORDER_BLIND_MS) return false;
    this.orderBlind = true;
    this.orderResumeTimes = [];
    this.stats.orderBlindEntries = (this.stats.orderBlindEntries || 0) + 1;
    logger.stream(`[STREAM] DEGRADED: subscription active but no order events for ${Math.round(ORDER_BLIND_MS / 1000)}s · targeted REST fallback ON`);
    this.beginGap("order-events-blind", { keepSubscription: true });
    return true;
  }

  noteOrderEvent(now = Date.now()) {
    this.lastOrderEventAt = now;
    if (!this.orderBlind) return;
    this.orderResumeTimes.push(now);
    while (this.orderResumeTimes.length && now - this.orderResumeTimes[0] > ORDER_RESUME_WINDOW_MS) this.orderResumeTimes.shift();
    if (this.orderResumeTimes.length < ORDER_RESUME_EVENTS) return;
    this.orderBlind = false;
    this.orderResumeTimes = [];
    const gap = this.gapStartedAt;
    this.gapStartedAt = 0;
    this.stats.gapRecoveries++;
    logger.stream("[STREAM] order events flowing again · REST fallback OFF · Stream-first");
    setImmediate(() => {
      for (const slug of this.collections) {
        try { this.onTopicGap(slug, gap, Date.now()); }
        catch (error) { this.recordHandlerError(error, "order resume hook"); }
      }
    });
  }

  beginGap(reason, { keepSubscription = false } = {}) {
    if (!this.gapStartedAt) {
      this.gapStartedAt = Date.now();
      if (!keepSubscription) this.subscriptionActive = false;
      for (const slug of this.collections) {
        try { this.onTopicUnavailable(slug, this.gapStartedAt, safeText(reason, 80)); }
        catch (error) { this.recordHandlerError(error, "gap start hook"); }
      }
    }
  }

  restartAfterJoinFailure() {
    if (this.closedByUser || this.reconnectTimer) return;
    this.stopClient();
    this.scheduleClientRetry("global-join-refused");
  }

  scheduleClientRetry(reason) {
    if (this.closedByUser || this.reconnectTimer || !this.collections.size) return;
    const base = Math.min(1000 * (2 ** this.retryAttempt), 30000);
    const delay = Math.max(500, Math.round(base * (0.8 + Math.random() * 0.4)));
    this.retryAttempt++;
    this.connecting = true;
    this.stats.reconnects++;
    logger.stream(`[STREAM] retry official global client in ${delay}ms reason=${safeText(reason, 80)}`);
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.ensureClient();
    }, delay);
    this.reconnectTimer.unref?.();
  }

  handleSdkEvent(generation, event) {
    if (generation !== this.clientGeneration || this.closedByUser) return;
    const receivedMono = Number(process.hrtime.bigint()) / 1e6;
    const eventName = String(event && event.event_type || "");
    if (!HANDLED_EVENTS.has(eventName)) return;
    this.stats.marketplaceEvents++;
    this.noteOrderEvent();
    this.lastMarketplaceEventAt = Date.now();
    const slug = eventSlug(event);
    if (!slug || !this.trackedSlugs.has(slug)) {
      this.stats.untrackedEvents++;
      this.recordRecent(false);
      return;
    }
    this.stats.matchedEvents++;
    this.recordRecent(true);
    const correlationId = `s${process.pid}-${++this.eventSequence}`;
    const decoded = decodeEvent({ event: eventName, topic: "collection:*", payload: event });
    // Per-event trace rows are written by the engine only for events that touch
    // a tracked NFT (1.25.31: per-event rows here wrote ~8 MB/min).
    if (!decoded) return;
    decoded.correlationId = correlationId;
    decoded.collectionSlug = slug;
    decoded.receivedMono = receivedMono;
    decoded.decodedMono = Number(process.hrtime.bigint()) / 1e6;
    try {
      this.onEvent(decoded);
    } catch (error) {
      this.recordHandlerError(error, "marketplace event handler");
    }
  }

  recordRecent(matched) {
    const second = Math.floor(Date.now() / 1000);
    const bucket = this.recentBuckets[second % this.recentBuckets.length];
    if (bucket.second !== second) {
      bucket.second = second;
      bucket.total = 0;
      bucket.matched = 0;
    }
    bucket.total++;
    if (matched) bucket.matched++;
  }

  recordHandlerError(error, where) {
    this.stats.handlerErrors++;
    logger.stream(`[STREAM] ${safeText(where, 60)} error: ${safeText(error && error.message || error)}`);
  }

  health() {
    if (this.closedByUser || !this.started || !this.collections.size) return "DISCONNECTED";
    if (this.authFailed) return "FAILED";
    if (this.subscriptionActive && this.activeSocket && this.activeSocket.readyState === this.WebSocket.OPEN) {
      return this.orderBlind ? "DEGRADED" : "HEALTHY";
    }
    return this.connecting || this.client || this.reconnectTimer ? "RECONNECTING" : "DISCONNECTED";
  }

  healthForCollection(slug) {
    const key = String(slug || "").trim().toLowerCase();
    if (!key || !this.trackedSlugs.has(key)) return "DISCONNECTED";
    if (this.refusedTopics.has(key)) return "RECONNECTING";
    return this.health();
  }

  revalidate(why = "revalidate") {
    const healthy = this.health() === "HEALTHY";
    if (!healthy && this.started && !this.closedByUser) this.ensureClient();
    return Promise.resolve({ healthy, reconnected: false, why: healthy ? "sdk-transport-active" : `${why}-recovering` });
  }

  forceReconnect(why = "forced") {
    if (this.closedByUser || !this.started || !this.collections.size) return false;
    this.beginGap(why);
    this.stopClient();
    this.scheduleClientRetry(why);
    return true;
  }

  refreshKey() {
    if (this.closedByUser) return;
    const nextSignature = this.getKeySignature ? String(this.getKeySignature() || "") : "";
    if (nextSignature === this.lastKeySignature) return;
    this.lastKeySignature = nextSignature;
    const currentKey = this.clientKey || "";
    if (currentKey && this.isKeyConfigured && this.isKeyConfigured(currentKey)) return;
    this.authFailed = false;
    this.lastError = "";
    this.beginGap("api-key-changed");
    this.stopClient();
    this.ensureClient();
  }

  stopClient() {
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    clearTimeout(this.joinAckTimer);
    this.joinAckTimer = null;
    const client = this.client;
    const unsubscribe = this.unsubscribe;
    this.client = null;
    this.unsubscribe = null;
    this.clientGeneration++;
    this.clientKey = "";
    this.connecting = false;
    this.subscriptionActive = false;
    this.activeSocket = null;
    this.orderBlind = false;
    clearInterval(this.orderBlindTimer);
    this.orderBlindTimer = null;
    if (unsubscribe) { try { unsubscribe(); } catch {} }
    if (client) { try { client.disconnect(() => {}); } catch {} }
  }

  stop() {
    this.closedByUser = true;
    this.started = false;
    this.everActive = false;
    this.gapStartedAt = 0;
    this.stopClient();
    this.collections.clear();
    this.trackedSlugs.clear();
  }

  status() {
    const now = Date.now();
    const currentSecond = Math.floor(now / 1000);
    let marketplaceEvents60s = 0;
    let matchedEvents60s = 0;
    for (const bucket of this.recentBuckets) {
      if (bucket.second >= currentSecond - 59 && bucket.second <= currentSecond) {
        marketplaceEvents60s += bucket.total;
        matchedEvents60s += bucket.matched;
      }
    }
    const socketConnected = Boolean(this.activeSocket && this.activeSocket.readyState === this.WebSocket.OPEN);
    return {
      mode: this.perCollection ? "official-collections" : "official-global",
      refusedTopics: this.refusedTopics.size,
      connected: socketConnected,
      socketConnected,
      subscriptionsActive: this.subscriptionActive,
      orderEventsBlind: this.orderBlind,
      lastOrderEventAt: this.lastOrderEventAt,
      health: this.health(),
      marketState: matchedEvents60s ? "MARKET_EVENTS_SEEN" : this.subscriptionActive
        ? "CONNECTED_NO_TRACKED_EVENT_IN_WINDOW" : this.health(),
      lastError: this.lastError,
      collections: this.collections.size,
      tracked: this.trackedSlugs.size,
      lastFrameAt: this.lastFrameAt,
      lastFrameType: this.lastFrameType,
      lastMarketplaceEventAt: this.lastMarketplaceEventAt,
      lastEventAgeMs: this.lastMarketplaceEventAt ? now - this.lastMarketplaceEventAt : -1,
      marketplaceEvents60s,
      matchedEvents60s,
      untrackedEvents60s: marketplaceEvents60s - matchedEvents60s,
      heartbeatRttMs: this.lastHeartbeatRttMs,
      streamGapStartedAt: this.gapStartedAt,
      reconnect: this.stats.reconnects,
      resubscribeCount: this.stats.resubscribeCount,
      handlerErrors: this.stats.handlerErrors,
      transportErrors: this.stats.transportErrors || 0,
      joinRefused: this.stats.joinRefused,
      frameTypes: { ...this.frameTypes },
      ...this.stats
    };
  }
}

module.exports = { OpenSeaStream, EVENT_TYPES, ORDER_BLIND_MS, ORDER_RESUME_EVENTS, eventSlug, safeText };

/**
 * Tracked collections spread over several official SDK sockets (1.25.31).
 * Observed live, not a documented limit: one socket accepted 50 collection
 * topics and refused the rest; two sockets accepted all 60. Shards hold at
 * most SHARD_SIZE topics, a slug keeps its shard when others are added, and
 * topics change incrementally on the live socket.
 */
class ShardedOpenSeaStream {
  constructor(options = {}, dependencies = {}) {
    this.options = options;
    this.dependencies = dependencies;
    this.shards = [];
    this.started = false;
  }

  static get SHARD_SIZE() { return 40; }

  normalize(slugs) {
    return [...new Set((slugs || []).map(x => String(x || "").trim().toLowerCase()).filter(Boolean))];
  }

  start(slugs = []) {
    this.started = true;
    this.apply(slugs);
  }

  setCollections(slugs = []) {
    if (!this.started) return;
    this.apply(slugs);
  }

  apply(slugs) {
    const want = new Set(this.normalize(slugs));
    for (const shard of this.shards) {
      for (const slug of [...shard.slugs]) if (!want.has(slug)) { shard.slugs.delete(slug); shard.dirty = true; }
    }
    const assigned = new Set(this.shards.flatMap(shard => [...shard.slugs]));
    for (const slug of want) {
      if (assigned.has(slug)) continue;
      let shard = this.shards.find(x => x.slugs.size < ShardedOpenSeaStream.SHARD_SIZE);
      if (!shard) {
        shard = { slugs: new Set(), stream: null, dirty: true };
        this.shards.push(shard);
      }
      shard.slugs.add(slug);
      shard.dirty = true;
    }
    for (const shard of [...this.shards]) {
      if (!shard.dirty) continue;
      shard.dirty = false;
      if (!shard.slugs.size) {
        if (shard.stream) shard.stream.stop();
        this.shards.splice(this.shards.indexOf(shard), 1);
        continue;
      }
      if (!shard.stream) {
        const index = this.shards.indexOf(shard);
        shard.stream = new OpenSeaStream({ ...this.options, perCollection: true, label: ` shard=${index + 1}` }, this.dependencies);
        shard.stream.start([...shard.slugs]);
      } else {
        shard.stream.setCollections([...shard.slugs]);
      }
    }
  }

  shardFor(slug) {
    const key = String(slug || "").trim().toLowerCase();
    return this.shards.find(shard => shard.slugs.has(key)) || null;
  }

  health() {
    if (!this.started || !this.shards.length) return "DISCONNECTED";
    const states = this.shards.map(shard => shard.stream ? shard.stream.health() : "DISCONNECTED");
    if (states.every(h => h === "HEALTHY")) return "HEALTHY";
    for (const h of ["FAILED", "DEGRADED", "RECONNECTING", "DISCONNECTED", "STALE"]) if (states.includes(h)) return h;
    return states[0];
  }

  healthForCollection(slug) {
    const shard = this.shardFor(slug);
    return shard && shard.stream ? shard.stream.healthForCollection(slug) : "DISCONNECTED";
  }

  revalidate(why = "revalidate") {
    return Promise.all(this.shards.map(shard => shard.stream ? shard.stream.revalidate(why) : { healthy: false }))
      .then(list => ({ healthy: list.every(r => r && r.healthy), reconnected: false, why }));
  }

  forceReconnect(why = "forced") {
    let any = false;
    for (const shard of this.shards) if (shard.stream && shard.stream.forceReconnect(why)) any = true;
    return any;
  }

  refreshKey() { for (const shard of this.shards) if (shard.stream) shard.stream.refreshKey(); }

  stop() {
    this.started = false;
    for (const shard of this.shards) if (shard.stream) shard.stream.stop();
    this.shards = [];
  }

  status() {
    const list = this.shards.map(shard => shard.stream ? shard.stream.status() : null).filter(Boolean);
    const sum = key => list.reduce((n, s) => n + (Number(s[key]) || 0), 0);
    const connected = list.length > 0 && list.every(s => s.socketConnected);
    const lastEventAgeMs = list.map(s => s.lastEventAgeMs).filter(x => x >= 0);
    const matched60 = sum("matchedEvents60s");
    return {
      mode: "official-collections",
      shards: list.length,
      connected, socketConnected: connected,
      subscriptionsActive: list.length > 0 && list.every(s => s.subscriptionsActive),
      health: this.health(),
      marketState: matched60 ? "MARKET_EVENTS_SEEN" : this.health() === "HEALTHY" ? "CONNECTED_NO_TRACKED_EVENT_IN_WINDOW" : this.health(),
      lastError: (list.find(s => s.lastError) || {}).lastError || "",
      collections: sum("collections"), tracked: sum("tracked"), refusedTopics: sum("refusedTopics"),
      lastFrameAt: Math.max(0, ...list.map(s => s.lastFrameAt || 0)),
      lastFrameType: (list[0] || {}).lastFrameType || "",
      lastMarketplaceEventAt: Math.max(0, ...list.map(s => s.lastMarketplaceEventAt || 0)),
      lastEventAgeMs: lastEventAgeMs.length ? Math.min(...lastEventAgeMs) : -1,
      marketplaceEvents60s: sum("marketplaceEvents60s"), matchedEvents60s: matched60, untrackedEvents60s: sum("untrackedEvents60s"),
      heartbeatRttMs: Math.max(-1, ...list.map(s => s.heartbeatRttMs)),
      streamGapStartedAt: Math.min(...list.map(s => s.streamGapStartedAt || Infinity).concat([Infinity])) === Infinity ? 0
        : Math.min(...list.map(s => s.streamGapStartedAt || Infinity)),
      reconnect: sum("reconnect"), resubscribeCount: sum("resubscribeCount"), handlerErrors: sum("handlerErrors"),
      transportErrors: sum("transportErrors"), joinRefused: sum("joinRefused"),
      framesReceived: sum("framesReceived"), bytesReceived: sum("bytesReceived"), controlFrames: sum("controlFrames"),
      marketplaceEvents: sum("marketplaceEvents"), matchedEvents: sum("matchedEvents"), untrackedEvents: sum("untrackedEvents"),
      orderEventsBlind: list.some(s => s.orderEventsBlind)
    };
  }
}

module.exports.ShardedOpenSeaStream = ShardedOpenSeaStream;
