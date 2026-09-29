"use strict";

// 1.25.31: tracked collections instead of the "*" firehose, spread over
// sockets of at most 40 topics (observed: 50 per socket, then refusals).
// Live: "*" delivered 97% untracked events and the server closed the socket
// every 2-3 s with 4500 "Failed to send message within the configured send limit".
const assert = require("node:assert/strict");
const { EventType } = require("@opensea/sdk/stream");
const { ShardedOpenSeaStream } = require("./stream-sdk");

let passed = 0, failed = 0;
function check(name, fn) {
  try { fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}

const clients = [];
class FakeClient {
  constructor(config) { this.config = config; this.topics = new Map(); this.disconnected = false; clients.push(this); }
  onEvents(slug, types, handler) {
    this.topics.set(slug, { types, handler });
    return () => { this.topics.delete(slug); };
  }
  disconnect(cb) { this.disconnected = true; cb && cb(); }
}
function bid(slug) {
  return { event_type: EventType.ITEM_RECEIVED_BID, sent_at: "2026-09-30T00:00:00.000Z",
    payload: { chain: "ethereum", collection: { slug }, item: { nft_id: "ethereum/0x0000000000000000000000000000000000000001/7" },
      maker: { address: "0x0000000000000000000000000000000000000002" }, order_hash: "0xabc",
      payment_token: { decimals: 18, symbol: "WETH" }, event_timestamp: "2026-09-30T00:00:00.000Z",
      base_price: "10000000000000000", quantity: 1, expiration_date: "2026-10-01T00:00:00.000Z" } };
}

const slugs = Array.from({ length: 90 }, (_, i) => `c${String(i).padStart(3, "0")}`);
const delivered = [];
const unavailable = [];
const s = new ShardedOpenSeaStream({ getApiKey: () => "synthetic", onEvent: e => delivered.push(e),
  onTopicUnavailable: slug => unavailable.push(slug) }, { OpenSeaStreamClient: FakeClient });
s.start(slugs);

check("no wildcard subscription: every topic is a tracked collection", () => {
  for (const c of clients) assert.ok(![...c.topics.keys()].includes("*"));
  assert.equal(clients.reduce((n, c) => n + c.topics.size, 0), 90);
});
check("topics are sharded at most 40 per socket", () => {
  assert.equal(clients.length, 3);
  assert.ok(clients.every(c => c.topics.size <= 40));
});
check("a tracked collection event reaches the engine", () => {
  clients[0].topics.get("c000").handler(bid("c000"));
  assert.equal(delivered.length, 1);
  assert.equal(delivered[0].collectionSlug, "c000");
});

const before = clients.length;
s.setCollections([...slugs, "new-one"]);
check("adding a collection subscribes incrementally: no socket restart", () => {
  assert.equal(clients.length, before);
  assert.ok(clients.every(c => !c.disconnected));
  assert.ok(clients.some(c => c.topics.has("new-one")));
});
s.setCollections(slugs.filter(x => x !== "c001"));
check("removing a collection unsubscribes only that topic", () => {
  assert.ok(!clients.some(c => c.topics.has("c001")));
  assert.ok(clients[0].topics.has("c000"));
});

// Per-topic ACK: the shard becomes active; a refused topic is scoped.
const shard0 = s.shards[0].stream;
shard0.activeSocket = { readyState: shard0.WebSocket.OPEN };
shard0.onSocketMessage(shard0.activeSocket, shard0.clientGeneration, JSON.stringify(["1", "1", "collection:c000", "phx_reply", { status: "ok", response: {} }]));
shard0.onSocketMessage(shard0.activeSocket, shard0.clientGeneration, JSON.stringify(["2", "2", "collection:c002", "phx_reply", { status: "error", response: { reason: "too many" } }]));
check("per-collection ACK activates the shard", () => {
  assert.equal(shard0.subscriptionActive, true);
  assert.equal(s.healthForCollection("c000"), "HEALTHY");
});
check("a refused topic is unhealthy on its own and opens targeted recovery", () => {
  assert.equal(s.healthForCollection("c002"), "RECONNECTING");
  assert.ok(unavailable.includes("c002"));
  assert.equal(shard0.client.disconnected, false);
});
check("event frames are not JSON-parsed by the observer (the SDK already did)", () => {
  const before = shard0.stats.controlFrames;
  shard0.onSocketMessage(shard0.activeSocket, shard0.clientGeneration, JSON.stringify([null, null, "collection:c000", "item_received_bid", { x: 1 }]));
  assert.equal(shard0.stats.controlFrames, before);
  assert.equal(shard0.lastFrameType, "event");
});
check("aggregate status is per-collection mode and sums shards", () => {
  const st = s.status();
  assert.equal(st.mode, "official-collections");
  assert.equal(st.shards, 3);
  assert.equal(st.tracked, 89);
});
s.stop();
check("stop disconnects every shard", () => assert.ok(clients.every(c => c.disconnected)));

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
if (failed) process.exitCode = 1;
