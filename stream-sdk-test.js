"use strict";

const assert = require("assert");
const { EventType } = require("@opensea/sdk/stream");
const { OpenSeaStream, EVENT_TYPES } = require("./stream-sdk");

let passed = 0;
let failed = 0;
function check(name, fn) {
  try { fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}

let captured;
class FakeClient {
  constructor(config) { this.config = config; captured = this; }
  onEvents(slug, types, handler) {
    this.slug = slug;
    this.types = types;
    this.handler = handler;
    return () => { this.unsubscribed = true; };
  }
  disconnect(callback) { this.disconnected = true; callback && callback(); }
}

const delivered = [];
const stream = new OpenSeaStream({ getApiKey: () => "synthetic-key-not-logged", onEvent: event => delivered.push(event) }, {
  OpenSeaStreamClient: FakeClient
});
stream.start(["alpha", "beta"]);

check("production adapter registers one official wildcard subscription", () => {
  assert.strictEqual(captured.slug, "*");
  assert.strictEqual(captured.types.length, 6);
  assert.deepStrictEqual(captured.types, [
    EventType.ITEM_RECEIVED_BID, EventType.COLLECTION_OFFER, EventType.TRAIT_OFFER,
    EventType.ITEM_CANCELLED, EventType.ORDER_INVALIDATE, EventType.ORDER_REVALIDATE
  ]);
  assert(!captured.types.includes("item_received_offer"));
});
check("Electron 20 runtime receives the documented ws transport", () => {
  assert.strictEqual(typeof captured.config.connectOptions.transport, "function");
  assert.strictEqual(captured.config.connectOptions.heartbeatIntervalMs, 30000);
});

function itemBid(slug) {
  return {
    event_type: EventType.ITEM_RECEIVED_BID,
    version: 7,
    sent_at: "2026-09-29T12:00:00.000Z",
    payload: {
      chain: "ethereum",
      collection: { slug },
      item: { nft_id: "ethereum/0x0000000000000000000000000000000000000001/42" },
      maker: { address: "0x0000000000000000000000000000000000000002" },
      taker: { address: "0x0000000000000000000000000000000000000003" },
      order_hash: "0xabc",
      payment_token: { decimals: 18, symbol: "WETH" },
      event_timestamp: "2026-09-29T12:00:00.000Z",
      base_price: "15000000000000000",
      quantity: 1,
      expiration_date: "2026-10-01T00:00:00.000Z"
    }
  };
}

captured.handler(itemBid("untracked"));
check("untracked global events are rejected by the slug Set before dispatch", () => {
  assert.strictEqual(stream.stats.untrackedEvents, 1);
  assert.strictEqual(delivered.length, 0);
});

captured.handler(itemBid("alpha"));
check("tracked item bid normalizes into the existing engine event shape", () => {
  assert.strictEqual(stream.stats.matchedEvents, 1);
  assert.strictEqual(delivered.length, 1);
  assert.strictEqual(delivered[0].event, "item_received_bid");
  assert.match(delivered[0].correlationId, /^s\d+-\d+$/);
  assert.strictEqual(delivered[0].collectionSlug, "alpha");
  assert.strictEqual(delivered[0].nft.tokenId, "42");
  assert.strictEqual(delivered[0].orderHash, "0xabc");
  assert.strictEqual(delivered[0].version, 7);
  assert(delivered[0].eventTimestamp > 0);
  assert.strictEqual(delivered[0].pricePerItem, 0.015);
});

const shared = {
  chain: "ethereum",
  collection: { slug: "alpha" },
  item: { nft_id: "ethereum/0x0000000000000000000000000000000000000001/42" },
  maker: { address: "0x0000000000000000000000000000000000000002" },
  taker: { address: "0x0000000000000000000000000000000000000003" },
  payment_token: { decimals: 18, symbol: "WETH" },
  event_timestamp: "2026-09-29T12:00:01.000Z",
  base_price: "20000000000000000",
  quantity: 1,
  expiration_date: "2026-10-01T00:00:00.000Z"
};
const followups = [
  { event_type: EventType.COLLECTION_OFFER, payload: { ...shared, order_hash: "0xc1", collection_criteria: { slug: "alpha" } } },
  { event_type: EventType.TRAIT_OFFER, payload: { ...shared, order_hash: "0xc2", collection_criteria: { slug: "alpha" }, trait_criteria: { trait_type: "Level", trait_name: "5" } } },
  { event_type: EventType.ITEM_CANCELLED, payload: { ...shared, order_hash: "0xc3" } },
  { event_type: EventType.ORDER_INVALIDATE, payload: { ...shared, order_hash: "0xc4" } },
  { event_type: EventType.ORDER_REVALIDATE, payload: { ...shared, order_hash: "0xc5" } }
];
for (const event of followups) captured.handler({ ...event, version: 2 });
check("collection, trait, cancel, invalidate, and revalidate reach the engine adapter", () => {
  assert.deepStrictEqual(delivered.slice(1).map(event => event.event), followups.map(event => event.event_type));
  assert.strictEqual(delivered[2].kind, "trait");
  assert.deepStrictEqual(delivered[2].traitCriteria, { trait_type: "Level", trait_name: "5" });
  assert(delivered.every(event => event.eventTimestamp > 0));
});

const status = stream.status();
check("stream status separates marketplace, matched, and untracked activity", () => {
  assert.strictEqual(status.mode, "official-global");
  assert.strictEqual(status.marketplaceEvents60s, 7);
  assert.strictEqual(status.matchedEvents60s, 6);
  assert.strictEqual(status.untrackedEvents60s, 1);
  assert.strictEqual(status.health, "RECONNECTING");
});

stream.stop();
check("stop unsubscribes and disconnects the official client", () => {
  assert.strictEqual(captured.unsubscribed, true);
  assert.strictEqual(captured.disconnected, true);
  assert.strictEqual(stream.status().health, "DISCONNECTED");
});

process.stdout.write(`\n${passed}/${passed + failed} official Stream SDK checks passed\n`);
process.exitCode = failed ? 1 : 0;
