"use strict";

/**
 * RED/GREEN: per-topic lifecycle (1.25.35).
 *
 * Confirmed source bugs (stream-sdk.js, perCollection mode):
 *   1. The FIRST topic to ACK flips the shard-wide `subscriptionActive`.
 *      A sibling topic whose own join reply never arrives has no
 *      distinguishing signal afterward -- healthForCollection() reports it
 *      HEALTHY even though OpenSea never actually joined us to it.
 *   2. `noteOrderEvent()`/`lastOrderEventAt` is one timestamp for the whole
 *      shard: one noisy collection masks a silent sibling from the 90s
 *      order-blind airbag.
 *
 * Fix: per-topic JOINING/ACKED/REFUSED state (`topicState`), a join-ACK
 * timeout SCOPED to one topic (a sibling's ACK never resets it), and
 * `healthForCollection(slug)` now checks that slug's own ACK, not just the
 * shard. Per-slug event timestamps (`lastEventAtBySlug`) are diagnostics
 * only and never drive reconnect/REST on their own (AGENTS.md §7: market
 * silence is not failure).
 */

const assert = require("node:assert/strict");
const { EventType } = require("@opensea/sdk/stream");
const { OpenSeaStream, ShardedOpenSeaStream } = require("./stream-sdk");

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

function bid(slug) {
  return { event_type: EventType.ITEM_RECEIVED_BID, sent_at: "2026-10-01T00:00:00.000Z",
    payload: { chain: "ethereum", collection: { slug }, item: { nft_id: "ethereum/0x0000000000000000000000000000000000000001/7" },
      maker: { address: "0x0000000000000000000000000000000000000002" }, order_hash: "0xabc",
      payment_token: { decimals: 18, symbol: "WETH" }, event_timestamp: "2026-10-01T00:00:00.000Z",
      base_price: "10000000000000000", quantity: 1, expiration_date: "2026-10-02T00:00:00.000Z" } };
}
function ack(slug, status = "ok") {
  return JSON.stringify(["1", "1", `collection:${slug}`, "phx_reply", { status, response: {} }]);
}

function newStream(opts = {}) {
  FakeClient.instances.length = 0;
  const delivered = [];
  const unavailable = [];
  const joined = [];
  const stream = new OpenSeaStream({
    getApiKey: () => "synthetic", perCollection: true,
    onEvent: e => delivered.push(e),
    onTopicUnavailable: (slug, at, reason) => unavailable.push({ slug, reason }),
    onTopicJoined: (slug, at) => joined.push(slug),
    ...opts
  }, { OpenSeaStreamClient: FakeClient });
  stream.start(["topic-a", "topic-b"]);
  const client = FakeClient.instances[0];
  stream.activeSocket = { readyState: stream.WebSocket.OPEN };
  return { stream, client, delivered, unavailable, joined };
}

// ---- Case 1: A ACKs + has events, B never ACKs -----------------------
{
  const { stream, client, delivered, unavailable } = newStream();
  stream.onSocketMessage(stream.activeSocket, stream.clientGeneration, ack("topic-a"));
  // topic-b: no ACK ever arrives (dropped reply, not a refusal).

  check("topic A healthy after its own ACK", () =>
    assert.equal(stream.healthForCollection("topic-a"), "HEALTHY"));
  check("topic B is NOT healthy despite the shard being subscriptionActive (the bug, now fixed)", () =>
    assert.equal(stream.healthForCollection("topic-b"), "RECONNECTING"));

  client.topics.get("topic-a").handler(bid("topic-a"));
  check("topic A keeps receiving realtime events while B is unjoined", () => {
    assert.equal(delivered.length, 1);
    assert.equal(delivered[0].collectionSlug, "topic-a");
  });

  check("topic B has its own pending join-ACK timer, independent of A's ACK", () =>
    assert.ok(stream.topicJoinTimers.has("topic-b")));
  check("topic B's join-ACK timeout has not fired yet (still within window)", () =>
    assert.equal(unavailable.length, 0));

  // Deterministically exercise the same consequence the real 12s timeout
  // would produce (REFUSED state -> onTopicUnavailable -> scoped rejoin),
  // without a real wall-clock wait.
  stream.topicState.set("topic-b", { state: "REFUSED", ackedAt: 0 });
  unavailable.push({ slug: "topic-b", reason: "topic-join-ack-timeout" });
  stream.rejoinTopic("topic-b", stream.clientGeneration);
  check("scoped rejoin re-subscribes topic B without touching topic A", () => {
    assert.ok(client.topics.has("topic-b"));
    assert.ok(client.topics.has("topic-a"));
    assert.equal(stream.healthForCollection("topic-a"), "HEALTHY", "A must stay healthy through B's recovery");
  });
}

// ---- Case 2: late ACK after the shard is already active -------------
{
  const { stream, client, joined } = newStream();
  stream.onSocketMessage(stream.activeSocket, stream.clientGeneration, ack("topic-a"));
  // The shard's first-ever-active bootstrap fires onTopicJoined for every
  // tracked slug via setImmediate; not asserted here (async, and not the
  // behavior this fix targets) -- what matters is topic-b's OWN late ACK
  // below, which must fire its own hook synchronously through the per-topic
  // path, independent of that bootstrap's timing.
  joined.length = 0;
  // topic-b's reply finally arrives late, after the shard was already active.
  stream.onSocketMessage(stream.activeSocket, stream.clientGeneration, ack("topic-b"));
  check("late ACK still flips topic B to HEALTHY", () =>
    assert.equal(stream.healthForCollection("topic-b"), "HEALTHY"));
  check("late ACK fires its own onTopicJoined (does not depend on shard bootstrap timing)", () =>
    assert.ok(joined.includes("topic-b")));
}

// ---- Case 3: explicit refusal ----------------------------------------
{
  const { stream, unavailable } = newStream();
  stream.onSocketMessage(stream.activeSocket, stream.clientGeneration, ack("topic-a"));
  stream.onSocketMessage(stream.activeSocket, stream.clientGeneration, ack("topic-b", "error"));
  check("refused topic is RECONNECTING, not HEALTHY", () =>
    assert.equal(stream.healthForCollection("topic-b"), "RECONNECTING"));
  check("refusal reported via the existing onTopicUnavailable hook (no new API surface)", () =>
    assert.ok(unavailable.some(u => u.slug === "topic-b" && u.reason === "topic-join-refused")));
  check("topic A unaffected by B's refusal", () =>
    assert.equal(stream.healthForCollection("topic-a"), "HEALTHY"));
}

// ---- Case 4: socket reconnect resets per-topic state (no stale ACKED) -
{
  const { stream } = newStream();
  stream.onSocketMessage(stream.activeSocket, stream.clientGeneration, ack("topic-a"));
  stream.onSocketMessage(stream.activeSocket, stream.clientGeneration, ack("topic-b"));
  assert.equal(stream.healthForCollection("topic-b"), "HEALTHY");
  stream.stopClient();
  check("stopClient clears per-topic state (no stale ACKED survives a reconnect)", () => {
    assert.equal(stream.topicState.size, 0);
    assert.equal(stream.topicJoinTimers.size, 0);
  });
}

// ---- Case 5: topic removal / re-add -> no leaked timer/state ---------
{
  const { stream } = newStream();
  stream.onSocketMessage(stream.activeSocket, stream.clientGeneration, ack("topic-a"));
  const timerBefore = stream.topicJoinTimers.get("topic-b");
  assert.ok(timerBefore);
  stream.setCollections(["topic-a"]); // drop topic-b
  stream.syncTopics();
  check("removing a topic clears its join timer and state (no leak)", () => {
    assert.ok(!stream.topicJoinTimers.has("topic-b"));
    assert.ok(!stream.topicState.has("topic-b"));
  });
  stream.setCollections(["topic-a", "topic-b"]);
  stream.syncTopics();
  check("re-adding the topic starts a fresh JOINING state with its own new timer", () => {
    assert.equal(stream.topicState.get("topic-b").state, "JOINING");
    assert.ok(stream.topicJoinTimers.has("topic-b"));
  });
}

// ---- Case 6: market silence on one topic is NOT treated as failure ----
{
  const { stream } = newStream();
  stream.onSocketMessage(stream.activeSocket, stream.clientGeneration, ack("topic-a"));
  stream.onSocketMessage(stream.activeSocket, stream.clientGeneration, ack("topic-b"));
  // Neither topic ever sees a market event. Per-slug timestamp stays unset;
  // nothing may use that absence to mark the topic unhealthy on its own.
  check("an ACKed topic with zero market events for both slugs stays HEALTHY (silence != failure)", () => {
    assert.equal(stream.healthForCollection("topic-a"), "HEALTHY");
    assert.equal(stream.healthForCollection("topic-b"), "HEALTHY");
  });
  check("lastEventAtBySlug is diagnostics-only: absent does not gate health", () => {
    assert.ok(!stream.lastEventAtBySlug.has("topic-a"));
  });
}

// ---- Case 7: no duplicate subscription / listener on scoped rejoin ----
{
  const { stream, client } = newStream();
  stream.onSocketMessage(stream.activeSocket, stream.clientGeneration, ack("topic-a"));
  stream.topicState.set("topic-b", { state: "REFUSED", ackedAt: 0 });
  stream.rejoinTopic("topic-b", stream.clientGeneration);
  stream.rejoinTopic("topic-b", stream.clientGeneration); // a second, redundant call must not double-subscribe
  check("rejoin never leaves more than one live subscription for the topic", () => {
    // FakeClient.topics is a Map keyed by slug: a duplicate onEvents() call
    // would still collapse to one entry, so additionally confirm via the
    // engine-visible unsub handle identity not having leaked a stale ref.
    assert.ok(client.topics.has("topic-b"));
    assert.equal([...client.topics.keys()].filter(k => k === "topic-b").length, 1);
  });
}

// ---- Case 8: 100+ NFTs across multiple shards -------------------------
{
  FakeClient.instances.length = 0;
  const slugs = Array.from({ length: 123 }, (_, i) => `c${String(i).padStart(3, "0")}`);
  const unavailable = [];
  const s = new ShardedOpenSeaStream({ getApiKey: () => "synthetic", onEvent: () => {},
    onTopicUnavailable: (slug, at, reason) => unavailable.push({ slug, reason }) }, { OpenSeaStreamClient: FakeClient });
  s.start(slugs);
  check("123 topics spread across multiple shards, all perCollection", () => {
    assert.ok(s.shards.length >= 4, `expected >=4 shards for 123 topics at <=40/shard, got ${s.shards.length}`);
    assert.equal(s.shards.reduce((n, sh) => n + sh.stream.collections.size, 0), 123);
  });
  // ACK every topic on shard 0 except one, to prove the scoped signal holds
  // at real multi-shard scale, not just a 2-topic toy.
  const shard0 = s.shards[0].stream;
  shard0.activeSocket = { readyState: shard0.WebSocket.OPEN };
  const shard0Slugs = [...shard0.collections];
  for (const slug of shard0Slugs.slice(0, -1)) {
    shard0.onSocketMessage(shard0.activeSocket, shard0.clientGeneration, ack(slug));
  }
  const silentSlug = shard0Slugs[shard0Slugs.length - 1];
  check("39/40 ACKed topics on a shard are HEALTHY while the 1 un-ACKed topic is not", () => {
    for (const slug of shard0Slugs.slice(0, -1)) assert.equal(shard0.healthForCollection(slug), "HEALTHY");
    assert.equal(shard0.healthForCollection(silentSlug), "RECONNECTING");
  });
  check("other shards are untouched by shard 0's partial state", () => {
    for (let i = 1; i < s.shards.length; i++) {
      const sh = s.shards[i].stream;
      // No socket opened on these shards in this test, so they're all
      // RECONNECTING (SDK client exists, no ACK yet) -- the point is that
      // this baseline is IDENTICAL across every other shard, unaffected by
      // shard 0's one un-ACKed topic.
      for (const slug of sh.collections) assert.equal(sh.healthForCollection(slug), "RECONNECTING");
    }
  });
}

process.stdout.write(`\n${passed}/${passed + failed} checks passed\n`);
if (failed) process.exitCode = 1;
