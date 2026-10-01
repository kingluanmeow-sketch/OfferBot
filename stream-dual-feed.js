"use strict";

/**
 * DualStreamFeed (1.25.41 audit fix).
 *
 * Two fully independent `ShardedOpenSeaStream` connections, one per
 * configured API key, both subscribed to the same tracked collections and
 * feeding the same `onEvent` dispatch. Extracted out of main.js so the real
 * wiring (shard assignment, per-feed key, subscription topics) is directly
 * testable without an Electron app -- see stream-dual-feed-test.js.
 *
 * 2026-10-01 audit finding (pre-release, caught before this shipped): the
 * first version of this built feed B with the PLAIN single-socket
 * `OpenSeaStream` and no `perCollection: true`, so it actually opened a
 * GLOBAL `collection:*` subscription regardless of the "same subscribed
 * collections" comment -- a full market-wide event flood on a second
 * socket, and a single socket would not have scaled past ~40 collections
 * either way (see `ShardedOpenSeaStream.SHARD_SIZE`). Feed B now uses the
 * exact same `ShardedOpenSeaStream` class as feed A, so it shares the same
 * per-collection subscription behavior and shard scaling by construction,
 * not by a second, easy-to-drift implementation.
 *
 * Feed B never drives engine health/recovery authority: only feed A's
 * onReconnect/onTopicGap/onTopicUnavailable/onTopicJoined are wired to
 * callers. Feed B exists purely so an event OpenSea failed to deliver on
 * feed A's socket has an independent second chance on feed B's socket.
 */

const { ShardedOpenSeaStream } = require("./stream-sdk");

const DEDUPE_WINDOW_MS = 60 * 1000;
/**
 * How long to wait, after feed B delivers an event feed A has not yet
 * shown, before counting it as "firstSeenOnB" (evidence A actually missed
 * it, not just arrived a few ms later than B). Bounded and per-key: the
 * timer is always cleared, either by firing once or by A's own delivery of
 * the same (event,orderHash) arriving first -- never left to grow
 * unbounded.
 */
const B_CONFIRM_WINDOW_MS = 5000;

class DualStreamFeed {
  constructor({ apiKeys, onEvent, onReconnect, onTopicGap, onTopicUnavailable, onTopicJoined,
    logger, dependencies = {}, confirmWindowMs = B_CONFIRM_WINDOW_MS } = {}) {
    this.apiKeys = apiKeys;
    this.onEvent = typeof onEvent === "function" ? onEvent : () => {};
    this.onReconnect = onReconnect; this.onTopicGap = onTopicGap;
    this.onTopicUnavailable = onTopicUnavailable; this.onTopicJoined = onTopicJoined;
    this.logger = logger || { stream() {}, error() {} };
    this.dependencies = dependencies;
    this.confirmWindowMs = confirmWindowMs;
    this.feedA = null;
    this.feedB = null;
    this.started = false;
    this.collections = [];
    /** (event,orderHash) -> {feed, at}. Pruned by sweep(), caller-driven. */
    this.recent = new Map();
    /** (event,orderHash) -> timer, for events seen on B but not yet confirmed on A. */
    this.pendingB = new Map();
    this.counters = {
      byFeed: { A: { received: 0 }, B: { received: 0 } },
      duplicate: 0, crossFeedDuplicate: 0,
      // An event whose first delivery came from feed B, and that feed A
      // STILL had not delivered after confirmWindowMs -- i.e. actually
      // evidence feed A missed it, not just a timing difference.
      firstSeenOnB: 0
    };
  }

  start(collections) {
    this.collections = [...(collections || [])];
    this.started = true;
    this.feedA = new ShardedOpenSeaStream({
      getApiKey: () => (this.apiKeys.primary() || {}).key || "",
      getKeySignature: () => { const k = this.apiKeys.primary(); return k ? `a:${k.fp || k.key}` : ""; },
      isKeyConfigured: key => Boolean(this.apiKeys.find(key)),
      onReconnect: (...a) => { if (this.onReconnect) this.onReconnect(...a); },
      onTopicGap: (...a) => { if (this.onTopicGap) this.onTopicGap(...a); },
      onTopicUnavailable: (...a) => { if (this.onTopicUnavailable) this.onTopicUnavailable(...a); },
      onTopicJoined: (...a) => { if (this.onTopicJoined) this.onTopicJoined(...a); },
      onEvent: decoded => this._dispatch(decoded, "A")
    }, this.dependencies);
    this.feedA.start(this.collections);
    this.ensureFeedB();
  }

  /**
   * (Re)establish feed B against whichever key is currently in the
   * secondary slot. Idempotent when feed B already exists on an unchanged
   * key. Tears feed B down if the second key is removed, rather than
   * leaving it running on a stale/invalid key.
   */
  ensureFeedB() {
    if (!this.started) return;
    const secondKey = typeof this.apiKeys.secondary === "function" ? this.apiKeys.secondary() : null;
    if (!secondKey || !secondKey.key) {
      if (this.feedB) { try { this.feedB.stop(); } catch { /* already down */ } this.feedB = null; }
      return;
    }
    if (this.feedB) {
      this.feedB.setCollections(this.collections);
      this.feedB.refreshKey();
      return;
    }
    this.feedB = new ShardedOpenSeaStream({
      getApiKey: () => { const k = this.apiKeys.secondary(); return k ? k.key : ""; },
      getKeySignature: () => { const k = this.apiKeys.secondary(); return k ? `b:${k.fp || k.key}` : ""; },
      isKeyConfigured: key => Boolean(this.apiKeys.find(key)),
      // Deliberately no onTopicGap/onTopicUnavailable/onTopicJoined/onReconnect
      // forwarding -- feed B must never drive engine recovery/health
      // authority (see class doc comment). Its reconnect/backoff stays
      // fully internal to this ShardedOpenSeaStream instance.
      onEvent: decoded => this._dispatch(decoded, "B")
    }, this.dependencies);
    this.feedB.start(this.collections);
    this.logger.stream(`[STREAM] second feed active on Key ${secondKey.slot} (fp=${secondKey.fp}) -- redundant event delivery only`);
  }

  setCollections(collections) {
    this.collections = [...(collections || [])];
    if (this.feedA) this.feedA.setCollections(this.collections);
    if (this.feedB) this.feedB.setCollections(this.collections);
  }

  _dispatch(decoded, feedId) {
    const byFeed = this.counters.byFeed[feedId] || (this.counters.byFeed[feedId] = { received: 0 });
    byFeed.received++;

    const key = decoded && decoded.orderHash ? `${decoded.event}:${decoded.orderHash}` : "";
    if (key) {
      const prior = this.recent.get(key);
      if (prior) {
        this.counters.duplicate++;
        if (prior.feed !== feedId) this.counters.crossFeedDuplicate++;
        if (feedId === "A") {
          // A just confirmed an event B saw first -- not a miss after all.
          const pending = this.pendingB.get(key);
          if (pending) { clearTimeout(pending); this.pendingB.delete(key); }
        }
      } else {
        this.recent.set(key, { feed: feedId, at: Date.now() });
        if (feedId === "B") {
          const timer = setTimeout(() => {
            this.pendingB.delete(key);
            this.counters.firstSeenOnB++;
          }, this.confirmWindowMs);
          timer.unref?.();
          this.pendingB.set(key, timer);
        }
      }
    }

    if (decoded && typeof decoded === "object") decoded.feed = feedId;
    this.onEvent(decoded);
  }

  /** Bounded-map maintenance; call periodically (same cadence as any summary timer). */
  sweep(now = Date.now()) {
    for (const [key, entry] of this.recent) if (now - entry.at > DEDUPE_WINDOW_MS) this.recent.delete(key);
  }

  health() { return this.feedA ? this.feedA.health() : "DISCONNECTED"; }
  healthForCollection(slug) { return this.feedA ? this.feedA.healthForCollection(slug) : "DISCONNECTED"; }
  revalidate(why) { return this.feedA ? this.feedA.revalidate(why) : Promise.resolve({ healthy: false, reconnected: false, why }); }
  refreshKey() { if (this.feedA) this.feedA.refreshKey(); this.ensureFeedB(); }

  /**
   * Every existing caller in this codebase predates dual-feed and reads the
   * FLAT shard status shape directly off `stream.status()` (marketplaceEvents,
   * reconnect, framesReceived, etc. -- see ShardedOpenSeaStream.status()).
   * Spreading feed A's status at the top level keeps every one of those
   * reads correct, unchanged: this object IS feed A's status, with `.a`
   * (an explicit alias), `.b`, and `.counters` added alongside for the new
   * redundancy info. Feed A remains the sole source of connected/health
   * truth; `.b` is purely informational.
   */
  status() {
    const a = this.feedA ? this.feedA.status() : { connected: false };
    return {
      ...a,
      a,
      b: this.feedB ? this.feedB.status() : null,
      counters: this.counters
    };
  }

  stop() {
    this.started = false;
    try { if (this.feedA) this.feedA.stop(); } catch { /* already down */ }
    try { if (this.feedB) this.feedB.stop(); } catch { /* already down */ }
    this.feedA = null;
    this.feedB = null;
    for (const timer of this.pendingB.values()) clearTimeout(timer);
    this.pendingB.clear();
    this.recent.clear();
  }
}

module.exports = { DualStreamFeed, DEDUPE_WINDOW_MS, B_CONFIRM_WINDOW_MS };
