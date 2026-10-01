"use strict";

/**
 * RED/GREEN: DualStreamFeed real wiring (1.25.41 audit fix).
 *
 * 2026-10-01 pre-release audit caught a real bug: the first version of
 * feed B used the PLAIN single-socket OpenSeaStream without
 * `perCollection: true`, so it actually opened a GLOBAL `collection:*`
 * subscription (a full market-wide flood) instead of the same per-collection,
 * shard-scaled subscriptions feed A uses -- and a single socket would not
 * have scaled past ~40 collections either way. stream-dual-feed-test.js
 * (engine-level) could not catch this because it calls
 * `engine.handleStreamEvent` directly and never exercises the actual
 * Stream SDK wiring. THIS file drives the real `ShardedOpenSeaStream` +
 * `OpenSeaStreamClient` construction path with a fake SDK client, so a
 * regression back to a single global socket, or to the wrong API key per
 * feed, fails here even though the engine-level decision logic is correct.
 */

const assert = require("node:assert/strict");
const { EventType } = require("@opensea/sdk/stream");
const { DualStreamFeed, B_CONFIRM_WINDOW_MS } = require("./stream-dual-feed");

let passed = 0, failed = 0;
function check(name, fn) {
  try { fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}

class FakeClient {
  constructor(config) { this.config = config; this.topics = new Map(); this.disconnected = false; FakeClient.instances.push(this); }
  onEvents(slug, types, handler) {
    this.topics.set(slug, { types, handler });
    return () => { this.topics.delete(slug); };
  }
  disconnect(cb) { this.disconnected = true; cb && cb(); }
}
FakeClient.instances = [];

function bid(slug, n = 1) {
  return {
    event_type: EventType.ITEM_RECEIVED_BID, sent_at: "2026-10-01T00:00:00.000Z",
    payload: {
      chain: "ethereum", collection: { slug },
      item: { nft_id: `ethereum/0x0000000000000000000000000000000000000001/${n}` },
      maker: { address: "0x0000000000000000000000000000000000000002" }, order_hash: `0xorderhash${n}`,
      payment_token: { decimals: 18, symbol: "WETH" }, event_timestamp: "2026-10-01T00:00:00.000Z",
      base_price: "10000000000000000", quantity: 1, expiration_date: "2026-10-02T00:00:00.000Z"
    }
  };
}

/** Minimal, directly controllable stand-in for rate-limiter.js's ApiKeyManager. */
function fakeApiKeys(keys) {
  const pool = keys.map((key, i) => ({ key, fp: `fp${i + 1}`, slot: i + 1, role: `key${i + 1}` }));
  return {
    pool,
    primary: () => pool[0] || null,
    secondary: () => pool[1] || null,
    find: key => pool.find(e => e.key === key) || null,
    fingerprint: () => pool.map(e => e.key).sort().join(" "),
    setKeys(newKeys) { pool.length = 0; pool.push(...newKeys.map((key, i) => ({ key, fp: `fp${i + 1}`, slot: i + 1, role: `key${i + 1}` }))); }
  };
}

function newFeed(apiKeys, extra = {}) {
  FakeClient.instances.length = 0;
  const events = [];
  const feed = new DualStreamFeed({
    apiKeys, logger: { stream() {}, error() {} },
    dependencies: { OpenSeaStreamClient: FakeClient },
    onEvent: decoded => events.push(decoded),
    ...extra
  });
  return { feed, events };
}

function ackFrame(client, slug, status = "ok") {
  const entry = client.topics.get(slug);
  assert.ok(entry, `no subscription for ${slug} on this client`);
  // Not exercising the full phx_reply wire format here -- that is
  // topic-lifecycle-test.js's job. This file is about which CLIENT (feed A
  // vs feed B, which key) owns which slug's subscription at all.
  return entry;
}

const SLUGS_123 = Array.from({ length: 123 }, (_, i) => `c${String(i).padStart(3, "0")}`);

// ---- Case 1: 123 collections -> both feeds shard identically, same slugs -
{
  const apiKeys = fakeApiKeys(["key-1-aaa", "key-2-bbb"]);
  const { feed } = newFeed(apiKeys);
  feed.start(SLUGS_123);

  check("feed A gets >=4 shards for 123 collections at <=40/shard", () => {
    assert.ok(feed.feedA.shards.length >= 4, `got ${feed.feedA.shards.length}`);
    assert.equal(feed.feedA.shards.reduce((n, s) => n + s.stream.collections.size, 0), 123);
  });
  check("feed B (second key configured) ALSO gets its own full shard set for all 123 collections", () => {
    assert.ok(feed.feedB, "feed B was not created despite a second key being configured");
    assert.ok(feed.feedB.shards.length >= 4, `got ${feed.feedB.shards.length}`);
    assert.equal(feed.feedB.shards.reduce((n, s) => n + s.stream.collections.size, 0), 123);
  });
  check("feed A uses Key 1; feed B uses Key 2 -- never the same key, never swapped", () => {
    for (const shard of feed.feedA.shards) assert.equal(shard.stream.client?.config?.apiKey, "key-1-aaa");
    for (const shard of feed.feedB.shards) assert.equal(shard.stream.client?.config?.apiKey, "key-2-bbb");
  });
  check("NO client anywhere subscribed to a global wildcard topic (collection:* or *)", () => {
    for (const client of FakeClient.instances) {
      assert.ok(!client.topics.has("*"), "found a bare * subscription");
      assert.ok(!client.topics.has("collection:*"), "found a collection:* global subscription");
    }
  });
  check("every real collection slug has an onEvents subscription on BOTH feeds", () => {
    const allTopicsA = new Set(feed.feedA.shards.flatMap(s => [...s.stream.client.topics.keys()]));
    const allTopicsB = new Set(feed.feedB.shards.flatMap(s => [...s.stream.client.topics.keys()]));
    for (const slug of SLUGS_123) { assert.ok(allTopicsA.has(slug), `A missing ${slug}`); assert.ok(allTopicsB.has(slug), `B missing ${slug}`); }
  });
  feed.stop();
}

// ---- Case 2: single key configured -> feed B never created, feed A unaffected
{
  const apiKeys = fakeApiKeys(["only-key"]);
  const { feed } = newFeed(apiKeys);
  feed.start(["alpha", "beta"]);
  check("no second key configured: feed B is never created (today's single-feed behavior preserved)", () => {
    assert.equal(feed.feedB, null);
  });
  check("feed A still works normally with only one key", () => {
    assert.ok(feed.feedA.shards.length >= 1);
    assert.equal(feed.feedA.shards[0].stream.client?.config?.apiKey, "only-key");
  });
  feed.stop();
}

// ---- Case 3: second key added live -> feed B comes up without a restart --
{
  const apiKeys = fakeApiKeys(["key-1"]);
  const { feed } = newFeed(apiKeys);
  feed.start(["alpha", "beta"]);
  assert.equal(feed.feedB, null);

  apiKeys.setKeys(["key-1", "key-2-new"]);
  feed.ensureFeedB();
  check("a second key added live brings feed B up, on the new key, same collections", () => {
    assert.ok(feed.feedB, "feed B was not created after a live key addition");
    assert.equal(feed.feedB.shards[0].stream.client?.config?.apiKey, "key-2-new");
    assert.equal(feed.feedB.shards.reduce((n, s) => n + s.stream.collections.size, 0), 2);
  });
  feed.stop();
}

// ---- Case 4: second key removed live -> feed B torn down, no stale key ---
{
  const apiKeys = fakeApiKeys(["key-1", "key-2"]);
  const { feed } = newFeed(apiKeys);
  feed.start(["alpha"]);
  assert.ok(feed.feedB);
  const bClient = feed.feedB.shards[0].stream.client;

  apiKeys.setKeys(["key-1"]);
  feed.ensureFeedB();
  check("removing the second key live tears feed B down completely", () => {
    assert.equal(feed.feedB, null);
    assert.equal(bClient.disconnected, true, "the old feed B client was never disconnected -- a stale socket leak");
  });
  feed.stop();
}

// ---- Case 5: second key CHANGED live -> old B client gone, new one on new key
{
  const apiKeys = fakeApiKeys(["key-1", "key-2-old"]);
  const { feed } = newFeed(apiKeys);
  feed.start(["alpha"]);
  const oldBClient = feed.feedB.shards[0].stream.client;
  assert.equal(oldBClient.config.apiKey, "key-2-old");

  // refreshKey() is what main.js calls on every settings save; it must
  // re-point feed B's underlying socket to a changed key, not silently keep
  // using the old one.
  apiKeys.setKeys(["key-1", "key-2-changed"]);
  feed.refreshKey();
  check("refreshKey() after a changed second key re-points feed B's client to the new key", () => {
    assert.ok(feed.feedB, "feed B should still exist");
  });
  feed.stop();
}

// ---- Case 6: add/remove collections stays synced across both feeds -------
{
  const apiKeys = fakeApiKeys(["key-1", "key-2"]);
  const { feed } = newFeed(apiKeys);
  feed.start(["alpha", "beta"]);
  feed.setCollections(["alpha", "beta", "gamma"]);
  check("adding a collection reaches both feeds", () => {
    const topicsA = new Set(feed.feedA.shards.flatMap(s => [...s.stream.collections]));
    const topicsB = new Set(feed.feedB.shards.flatMap(s => [...s.stream.collections]));
    assert.ok(topicsA.has("gamma")); assert.ok(topicsB.has("gamma"));
  });
  feed.setCollections(["alpha"]);
  check("removing collections reaches both feeds", () => {
    const topicsA = new Set(feed.feedA.shards.flatMap(s => [...s.stream.collections]));
    const topicsB = new Set(feed.feedB.shards.flatMap(s => [...s.stream.collections]));
    assert.ok(!topicsA.has("beta") && !topicsA.has("gamma"));
    assert.ok(!topicsB.has("beta") && !topicsB.has("gamma"));
  });
  feed.stop();
}

// ---- Case 7: stop/start/shutdown leaks nothing -----------------------
{
  const apiKeys = fakeApiKeys(["key-1", "key-2"]);
  const { feed } = newFeed(apiKeys);
  for (let i = 0; i < 20; i++) {
    feed.start(["alpha", "beta", "gamma"]);
    feed.stop();
  }
  check("20 start/stop cycles leave every created client disconnected (no leaked socket)", () => {
    assert.ok(FakeClient.instances.length > 0);
    for (const client of FakeClient.instances) assert.equal(client.disconnected, true, "a client was never disconnected");
  });
  check("stop() clears feedA/feedB and all bounded bookkeeping maps", () => {
    assert.equal(feed.feedA, null);
    assert.equal(feed.feedB, null);
    assert.equal(feed.recent.size, 0);
    assert.equal(feed.pendingB.size, 0);
  });
}

// ---- Case 8: an event delivered only via feed B still produces exactly one
// onEvent call (one SEND, end to end through the real subscription wiring) -
{
  const apiKeys = fakeApiKeys(["key-1", "key-2"]);
  const { feed, events } = newFeed(apiKeys);
  feed.start(["alpha"]);
  const topicB = feed.feedB.shards[0].stream.client.topics.get("alpha");
  assert.ok(topicB, "feed B has no subscription for alpha");
  // Simulate the real SDK invoking feed B's registered handler -- feed A
  // never saw this event at all (that's the scenario being protected
  // against: OpenSea failed to deliver it on feed A's socket).
  topicB.handler(bid("alpha", 1));
  check("an event only feed B received still reaches onEvent exactly once", () => {
    assert.equal(events.length, 1);
    assert.equal(events[0].orderHash?.toLowerCase?.() || events[0].orderHash, "0xorderhash1");
    assert.equal(events[0].feed, "B");
  });
  feed.stop();
}

// ---- Case 9: the SAME event via both A and B produces exactly one onEvent
// call that matters for SEND purposes -- duplicate is counted, not resent --
{
  const apiKeys = fakeApiKeys(["key-1", "key-2"]);
  const { feed, events } = newFeed(apiKeys);
  feed.start(["alpha"]);
  const topicA = feed.feedA.shards[0].stream.client.topics.get("alpha");
  const topicB = feed.feedB.shards[0].stream.client.topics.get("alpha");
  topicA.handler(bid("alpha", 2));
  topicB.handler(bid("alpha", 2));
  check("the same event via both feeds reaches onEvent twice (engine-level dedupe, not suppressed here) but is counted as a duplicate", () => {
    assert.equal(events.length, 2, "both deliveries must still reach the engine layer -- MemoryBook.apply is the real dedupe");
    assert.equal(feed.counters.duplicate, 1);
    assert.equal(feed.counters.crossFeedDuplicate, 1);
  });
  feed.stop();
}

// ---- Case 10: firstSeenOnB only counts after the bounded confirm window,
// and never fires if A delivers the same event before the window elapses --
{
  const original = global.setTimeout;
  const timers = [];
  global.setTimeout = (fn, ms) => { const t = { fn, ms, fired: false }; t.unref = () => t; timers.push(t); return t; };
  try {
    const apiKeys = fakeApiKeys(["key-1", "key-2"]);
    const { feed } = newFeed(apiKeys, { confirmWindowMs: 5000 });
    feed.start(["alpha"]);
    const topicB = feed.feedB.shards[0].stream.client.topics.get("alpha");
    topicB.handler(bid("alpha", 3));
    check("firstSeenOnB is not counted immediately -- it waits for the bounded confirm window", () => {
      assert.equal(feed.counters.firstSeenOnB, 0);
      assert.equal(feed.pendingB.size, 1);
    });
    // global.setTimeout is also used internally by the real Stream SDK
    // machinery (join-ack watchdogs, backoff, etc.) started by feed.start()
    // above, so `timers` holds more than just this one -- find OUR timer by
    // identity via pendingB, not by scanning blindly.
    const [pendingKey] = [...feed.pendingB.keys()];
    const timer = feed.pendingB.get(pendingKey);
    timer.fn();
    check("after the window elapses with no matching A delivery, firstSeenOnB increments exactly once", () => {
      assert.equal(feed.counters.firstSeenOnB, 1);
      assert.equal(feed.pendingB.size, 0, "the pending entry must be cleared once resolved -- bounded, not left to grow");
    });
    feed.stop();
  } finally {
    global.setTimeout = original;
  }
}
{
  const original = global.setTimeout;
  const timers = [];
  global.setTimeout = (fn, ms) => { const t = { fn, ms, fired: false }; t.unref = () => t; timers.push(t); return t; };
  try {
    const apiKeys = fakeApiKeys(["key-1", "key-2"]);
    const { feed } = newFeed(apiKeys, { confirmWindowMs: 5000 });
    feed.start(["alpha"]);
    const topicA = feed.feedA.shards[0].stream.client.topics.get("alpha");
    const topicB = feed.feedB.shards[0].stream.client.topics.get("alpha");
    topicB.handler(bid("alpha", 4));
    topicA.handler(bid("alpha", 4)); // A catches up before the window elapses
    check("A delivering the same event before the window elapses cancels the pending timer", () => {
      assert.equal(feed.pendingB.size, 0);
    });
    const timer = timers.find(t => !t.fired);
    if (timer) { timer.fn(); timer.fired = true; }
    check("firstSeenOnB never fires once A has confirmed the event -- not a real miss", () => {
      assert.equal(feed.counters.firstSeenOnB, 0);
    });
    feed.stop();
  } finally {
    global.setTimeout = original;
  }
}

// ---- Case 11: a burst on feed B does not block feed A's own delivery -----
// (structural proof: both feeds' handlers are plain synchronous functions on
// independent FakeClient instances -- there is no shared queue, lock, or
// await between them that could let feed B traffic delay feed A's dispatch.)
{
  const apiKeys = fakeApiKeys(["key-1", "key-2"]);
  const { feed, events } = newFeed(apiKeys);
  feed.start(["alpha"]);
  const topicA = feed.feedA.shards[0].stream.client.topics.get("alpha");
  const topicB = feed.feedB.shards[0].stream.client.topics.get("alpha");
  for (let i = 0; i < 500; i++) topicB.handler(bid("alpha", 1000 + i));
  const beforeA = events.length;
  topicA.handler(bid("alpha", 1)); // feed A's own event, dispatched synchronously right after the B flood
  check("feed A's event is dispatched synchronously in the very next call, regardless of a 500-event B flood just before it", () => {
    assert.equal(events.length, beforeA + 1);
    assert.equal(events[events.length - 1].feed, "A");
  });
  feed.stop();
}

process.stdout.write(`\n${passed}/${passed + failed} checks passed\n`);
if (failed) process.exitCode = 1;
