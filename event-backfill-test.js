"use strict";

const assert = require("node:assert/strict");
const { EventCursor, tokenIdentity, eventType } = require("./offer-item-v2/event-backfill");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");

let now = 1_800_000_000_000;
const cursor = new EventCursor({ now: () => now, lookbackSeconds: 300, overlapSeconds: 5, maxSeen: 2 });
const first = cursor.request("demo");
assert.equal(first.after, 1_799_999_700);
assert.equal(first.next, undefined);

const event = { event_type: "offer", order_hash: "0xabc", asset: {
  contract: "0x1111111111111111111111111111111111111111", identifier: "42"
} };
assert.deepEqual(tokenIdentity(event), { contract: "0x1111111111111111111111111111111111111111", tokenId: "42" });
assert.equal(eventType(event), "offer");
assert.equal(eventType({ event_type: "listing" }), "");

const page1 = cursor.accept("demo", { events: [event], next: "page-two" });
assert.equal(page1.length, 1);
assert.deepEqual(cursor.request("demo"), { after: undefined, next: "page-two" });
const page2 = cursor.accept("demo", { events: [event, { event_type: "trait_offer", order_hash: "0xdef" }], next: null });
assert.equal(page2.length, 1, "overlap event is deduped while later page is retained");
assert.deepEqual(cursor.request("demo"), { after: 1_799_999_995, next: undefined });
assert.equal(cursor.accept("demo", { events: [event], next: null }).length, 0, "overlap window is deduped");
now += 61_000;
assert.deepEqual(cursor.request("demo"), { after: 1_799_999_761, next: undefined }, "periodic deeper cursor catches delayed indexing");
cursor.accept("demo", { events: [], next: null });
assert.deepEqual(cursor.request("demo"), { after: 1_800_000_056, next: undefined }, "normal cursor advances with small overlap");
cursor.prune([]);
assert.equal(cursor.states.size, 0, "removed collections do not leak cursor state");

const engine = Object.create(OfferItemEngineV2.prototype);
engine.state = STATE.RUNNING;
engine.rows = new Map([
  ["a", { key: "a", running: true, collectionSlug: "demo", contract: "0x1111111111111111111111111111111111111111", tokenId: "42" }],
  ["b", { key: "b", running: true, collectionSlug: "demo", contract: "0x1111111111111111111111111111111111111111", tokenId: "43" }],
  ["c", { key: "c", running: true, collectionSlug: "other", contract: "0x1111111111111111111111111111111111111111", tokenId: "42" }]
]);
const reads = [];
engine.queueRead = (row, options) => { reads.push({ row, options }); return true; };
engine.queueCollectionSeed = (slug, reason, options) => { engine.seeded = { slug, reason, options }; return true; };
engine.stats = {};
assert.equal(engine.applyBackfilledOfferEvent("demo", event), 1);
assert.equal(reads.length, 1, "item event reconciles only exact tracked NFT");
assert.equal(reads[0].row.key, "a");
assert.equal(reads[0].options.reason, "event-backfill");
assert.equal(reads[0].options.authoritative, true);
assert.equal(engine.applyBackfilledOfferEvent("demo", { event_type: "collection_offer" }), 1);
assert.deepEqual(engine.seeded, { slug: "demo", reason: "event-backfill", options: { force: true } });
assert.equal(reads.length, 1, "collection offer recovery does not fan out into per-NFT reads");
console.log("event-backfill-test: 12/12 passed");
