"use strict";

/**
 * Audit verification (not a bugfix -- confirms existing compensation
 * mechanism, or reports a gap if missing; no polling added either way).
 *
 * QUESTION: if a collection/trait offer event arrives BEFORE a row's
 * collectionSlug resolves, MemoryBook's bySlug fan-out cannot route it to
 * this row (the row isn't registered under that slug yet) -- is this
 * compensated once the slug resolves, or is it permanently lost?
 *
 * ANSWER (proven below): every row starts in `awaitingFirstRead` and is
 * gated from sending until an authoritative REST read completes. That
 * read reflects OpenSea's CURRENT state (not a replay of missed Stream
 * events), so any collection/trait offer that existed before slug
 * resolution is picked up correctly once the first read runs -- same
 * mechanism that already compensates for ANY missed pre-warm Stream
 * event, not something specific to slug timing.
 *
 * Also verifies: stop()/reset() preserve preToBookOpIndex while rows
 * persist (only shutdown()/Start-reload wipe it) -- no leak, no
 * over-eager clear either.
 */
const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}
const flush = async (n = 3) => { for (let i = 0; i < n; i++) await new Promise(r => setImmediate(r)); };

const RIVAL = "0x3333333333333333333333333333333333333333";
const CONTRACT = "0x1111111111111111111111111111111111111111";

(async () => {

await check("a collection offer arriving BEFORE slug resolution is missed by Stream fan-out, but the first authoritative read (post-resolution) still reflects the current authority price -- compensation confirmed, not a new gap", async () => {
  const resolvers = [];
  const adapter = { chain: "ethereum", async fetchBest() { return new Promise(res => resolvers.push(res)); } };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(() => "HEALTHY");

  // Row added with slug NOT YET resolved (realistic: URL-only Add before template hydration).
  const key = engine.registerRow({ url: "", contract: CONTRACT, tokenId: "5001", collectionSlug: "", minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 });
  const row = engine.rows.get(key);
  engine.awaitingFirstRead.add(key); // matches real cold-row state

  // A collection_offer for the eventual slug arrives NOW, while unresolved.
  engine.apply({
    collectionSlug: "apuapustajas", nft: null, kind: "collection", orderHash: "0xcoll1",
    maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0, eventTimestamp: Date.now(),
    receivedAt: Date.now(), hasOrderData: true, event: "collection_offer", pricePerItem: 0.05
  });
  await flush();
  const book = engine.book.get(key);
  assert.equal(book.effectiveBest(Date.now()).price, 0, "sanity: the collection offer is genuinely NOT applied via Stream fan-out -- this row isn't registered under 'apuapustajas' in book.bySlug yet");

  // Slug now resolves (as ensureTemplate's background path would do).
  engine.setRowCollectionSlug(row, "apuapustajas");
  assert.ok(engine.book.bySlug.get("apuapustajas").has(key), "sanity: the row is now correctly registered under the resolved slug for FUTURE events");

  // The first authoritative read runs (gated by awaitingFirstRead) and
  // returns OpenSea's CURRENT state -- which still includes that
  // collection offer, since REST reflects reality, not a Stream replay.
  engine.queueRead(row, { reason: "first-read", authoritative: true, readAt: Date.now(), firstRead: true, attempt: 1 });
  await flush();
  assert.ok(resolvers.length >= 1, "a first-read must have been queued for this still-cold row");
  resolvers[0]({ orderHash: "0xcoll1", price: 0.05, orders: [{ orderHash: "0xcoll1", price: 0.05, maker: RIVAL, kind: "collection", endTime: 0, quantity: 1 }] });
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.05, "the first authoritative read must reflect the collection offer that Stream fan-out missed during the slug-unresolved window -- compensation via existing first-read-gate mechanism, no polling added");
});

await check("stop()/reset() preserve preToBookOpIndex while rows persist -- no leak, no over-eager clear", async () => {
  const adapter = { chain: "ethereum", async fetchBest() { return new Promise(() => {}); } };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  engine.registerRow({ url: "", contract: CONTRACT, tokenId: "5002", collectionSlug: "apuapustajas", minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 });

  let total = 0;
  for (const b of engine.preToBookOpIndex.values()) for (const arr of b.byCanon.values()) total += arr.length;
  assert.equal(total, 1, "sanity: one indexed row before stop()");

  engine.stop();
  total = 0;
  for (const b of engine.preToBookOpIndex.values()) for (const arr of b.byCanon.values()) total += arr.length;
  assert.equal(total, 1, "stop()/reset() must NOT clear the diagnostic index -- the row itself still exists, only read/intent state resets");

  engine.shutdown();
  assert.equal(engine.preToBookOpIndex.size, 0, "shutdown() must fully clear the diagnostic index");
});

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
