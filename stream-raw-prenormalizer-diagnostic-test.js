"use strict";

/**
 * RED/GREEN: bounded pre-normalizer diagnostic (audit, Apu #5518/#7309).
 *
 * `stream_rx` only fires AFTER `toBookOp()` succeeds AND the contract/
 * tokenId resolve via `byNft` -- if the normalizer returns null, or the
 * lookup misses, NOTHING is recorded, indistinguishable from OpenSea never
 * sending the event at all. Production proof: the real external top orders
 * for Apu #5518/#7309 never appeared even once in `stream_rx`.
 *
 * `stream_raw` is recorded from the RAW event (before `toBookOp()`), bounded
 * to tracked NFTs exactly like `stream_rx` already is. Comparing the two
 * lets a later audit tell apart:
 *   1. stream_raw absent on BOTH feeds -> OpenSea/WS never delivered it
 *      (no further local evidence possible without transport capture).
 *   2. stream_raw present, stream_rx absent -> normalizer/lookup dropped it.
 *   3. stream_raw present on ONE feed only -> dual-feed disagreement.
 *
 * This is diagnostic ONLY -- read-only, no REST, no recovery trigger, must
 * never affect apply()/evaluate()/POST timing or outcome.
 */
const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const productionTrace = require("./production-trace");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}
const flush = async (n = 3) => { for (let i = 0; i < n; i++) await new Promise(r => setImmediate(r)); };

const RIVAL = "0x3333333333333333333333333333333333333333";
const CONTRACT = "0x1111111111111111111111111111111111111111";

function buildEngine() {
  const adapter = { chain: "ethereum", async fetchBest() { return new Promise(() => {}); } };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(() => "HEALTHY");
  const key = `ethereum:${CONTRACT}:9001`;
  const row = { key, running: true, tokenId: "9001", contract: CONTRACT, collectionSlug: "apuapustajas", minPrice: 0.001, maxPrice: 1, step: 0.0001 };
  engine.rows.set(key, row);
  const book = engine.book.add({ key, chain: "ethereum", contract: CONTRACT, tokenId: "9001", collectionSlug: "apuapustajas" });
  book.generation = 1; book.hydratedAt = Date.now() - 1000;
  return { engine, key, book };
}

(async () => {

await check("stream_raw is recorded for a tracked NFT on a normal, well-formed event (case: normal delivery)", async () => {
  const { engine } = buildEngine();
  const recorded = [];
  const real = productionTrace.record;
  productionTrace.record = (stage, event, detail) => { recorded.push({ stage, event, detail }); real.call(productionTrace, stage, event, detail); };
  try {
    engine.apply({
      collectionSlug: "apuapustajas", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9001" },
      kind: "item", orderHash: "0xreal5518", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
      event: "item_received_bid", pricePerItem: 0.0654, feed: "A"
    });
    await flush();
    const raw = recorded.find(r => r.stage === "stream_raw");
    const rx = recorded.find(r => r.stage === "stream_rx");
    assert.ok(raw, "stream_raw must be recorded for a tracked NFT");
    assert.ok(rx, "stream_rx must also be recorded when the event normalizes and applies cleanly");
    assert.equal(raw.detail.tokenId, "9001");
    assert.equal(raw.detail.feed, "A");
  } finally { productionTrace.record = real; }
});

await check("stream_raw fires even when toBookOp() would return null (unusable price) -- distinguishes 'received but dropped' from 'never sent'", async () => {
  const { engine } = buildEngine();
  const recorded = [];
  const real = productionTrace.record;
  productionTrace.record = (stage, event, detail) => { recorded.push({ stage, event, detail }); };
  try {
    // pricePerItem <= 0 makes toBookOp() return null for an UPSERT (see
    // event-normalizer.js) -- apply() returns before anything else runs.
    engine.apply({
      collectionSlug: "apuapustajas", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9001" },
      kind: "item", orderHash: "0xunusable", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
      event: "item_received_bid", pricePerItem: 0, feed: "B"
    });
    await flush();
    const raw = recorded.find(r => r.stage === "stream_raw");
    const rx = recorded.find(r => r.stage === "stream_rx");
    assert.ok(raw, "stream_raw must still fire from the RAW event, even though toBookOp() will reject it");
    assert.ok(!rx, "sanity: stream_rx must NOT fire -- toBookOp() returned null before reaching that point");
  } finally { productionTrace.record = real; }
});

await check("stream_raw is bounded to tracked NFTs only -- an untracked NFT never gets recorded (same I/O bound as stream_rx)", async () => {
  const { engine } = buildEngine();
  const recorded = [];
  const real = productionTrace.record;
  productionTrace.record = (stage, event, detail) => { recorded.push({ stage, event, detail }); };
  try {
    engine.apply({
      collectionSlug: "some-other-collection", nft: { chain: "ethereum", contract: "0x2222222222222222222222222222222222222222", tokenId: "77777" },
      kind: "item", orderHash: "0xuntracked", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
      event: "item_received_bid", pricePerItem: 0.01, feed: "A"
    });
    await flush();
    assert.ok(!recorded.some(r => r.stage === "stream_raw"), "an untracked NFT must never produce a stream_raw row -- bounded exactly like stream_rx");
  } finally { productionTrace.record = real; }
});

await check("stream_raw diagnostic never affects apply()/evaluate()/POST outcome or timing -- read-only, no REST, no recovery side effect", async () => {
  const { engine, book } = buildEngine();
  const t0 = process.hrtime.bigint();
  engine.apply({
    collectionSlug: "apuapustajas", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9001" },
    kind: "item", orderHash: "0xsideeffect", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
    event: "item_received_bid", pricePerItem: 0.05, feed: "A"
  });
  const elapsedMs = Number(process.hrtime.bigint() - t0) / 1e6;
  await flush();
  assert.ok(elapsedMs < 20, `apply() took ${elapsedMs}ms -- the diagnostic must stay cheap/synchronous and never slow the hot path`);
  assert.equal(book.effectiveBest(Date.now()).price, 0.05, "the real event must still apply correctly -- diagnostic is purely additive");
});

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
