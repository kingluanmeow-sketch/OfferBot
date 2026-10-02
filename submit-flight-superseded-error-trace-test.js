"use strict";

/**
 * RED/GREEN: `submitOne()`'s catch(error) branch silently drops an AMBIGUOUS
 * POST outcome when the flight has ALREADY been superseded ("!alive()") --
 * no submit_failure trace, no submit_success trace, and critically no
 * `ownUnknownAt` marker, so the REPLACEMENT flight has no reason to
 * reconcile before sending its own POST.
 *
 * PRODUCTION EVIDENCE (audit, v1.25.61 running trace, 2026-10-02 13:40Z,
 * token 3532): `http_start` recorded, then total silence -- no terminal of
 * any kind -- followed ~1.3s later by a SECOND complete send_queued ->
 * http_start -> submit_success cycle for the SAME NFT, same target price.
 * This exact code path explains it precisely: the SECOND cycle's dispatch
 * only became possible because the FIRST flight's `finally()` released
 * Intent/Flight -- which only happens once `submitOne()` RETURNS. The old
 * code returned here with ZERO signal that the aborted/errored POST might
 * have actually reached OpenSea.
 *
 * CHỨNG MINH / GIẢ THUYẾT:
 *   - CHỨNG MINH (đọc code): the SUCCESS branch (a few lines below) has an
 *     explicit, matching comment for the SAME race ("Watchdog đã thay lượt
 *     này trong lúc POST bay -- nhưng OpenSea ĐÃ nhận order") and handles it
 *     correctly (records own + SUCCESS). The ERROR branch had no equivalent.
 *   - GIẢ THUYẾT (not proven here): that THIS exact mechanism explains the
 *     specific token-3532 production trace line-for-line (vs. e.g. a genuine
 *     network retry at the HTTP layer) -- the production trace does not
 *     carry enough detail (no flight id) to prove it conclusively from logs
 *     alone; the fix is justified by the code-level gap itself, which is a
 *     real, reachable bug regardless.
 */

const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const devRuntime = require("./dev-runtime");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}

const SELF = "0x4444444444444444444444444444444444444444";
const RIVAL = "0x3333333333333333333333333333333333333333";
const CONTRACT = "0x1111111111111111111111111111111111111111";

function buildEngine({ tokenId = "1", slug = "coll", httpImpl } = {}) {
  const key = `ethereum:${CONTRACT}:${tokenId}`;
  const adapter = {
    chain: "ethereum", openseaChain: "ethereum",
    toWei: x => String(Math.round(Number(x) * 1e18)),
    offerBody: () => ({}),
    async fetchBest() { return new Promise(() => {}); }
  };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.recovery.pressure = () => false;
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(() => "HEALTHY");
  engine.templateReady = () => true;
  engine.ownAuthoritative = () => true;
  engine.topicReady = () => true;
  engine.quota = null; // skip the broker entirely -- not what this test audits
  engine.builder = {
    address: SELF,
    async build(k, { amountWei, durationMinutes, onStage }) {
      onStage && onStage("build_started");
      return { components: { endTime: Math.floor(Date.now() / 1000) + 900 }, signature: "0xsig", orderHash: "0xdeadbeef" };
    }
  };
  engine.http = { request: httpImpl };
  const row = { key, running: true, tokenId, contract: CONTRACT, collectionSlug: slug, minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 };
  engine.rows.set(key, row);
  const book = engine.book.add({ key, chain: "ethereum", contract: CONTRACT, tokenId, collectionSlug: slug });
  book.generation = 1;
  book.hydratedAt = Date.now() - 1000;
  book.ownReconciledAt = Date.now() - 1000;
  book.selfAddress = SELF;
  engine.book.setSelfAddress(SELF);
  return { engine, row, key, book };
}

(async () => {
const realSpendAllowed = devRuntime.spendAllowed;
devRuntime.spendAllowed = () => true;

// ---- Case 1: POST errors (ambiguous, not notSent) AFTER the flight was superseded -- must mark ownUnknownAt ----
await check("ambiguous POST error on a SUPERSEDED flight must mark book.ownUnknownAt so the replacement flight reconciles before sending", async () => {
  let httpStartedFired = false;
  let releaseRequest;
  const { engine, row, key, book } = buildEngine({
    tokenId: "3532", slug: "coll",
    httpImpl: ({ onStage }) => new Promise((resolve, reject) => {
      releaseRequest = () => { onStage("http_started"); httpStartedFired = true; reject(Object.assign(new Error("socket hang up"), { notSent: false })); };
    })
  });

  // A real competitor bid raises Best, producing a real SEND decision/target.
  engine.apply({
    collectionSlug: "coll", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "3532" },
    kind: "item", orderHash: "0xrival1", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
    event: "item_received_bid", pricePerItem: 0.0124
  });
  await new Promise(r => setImmediate(r));

  // The flight is now in flight (http_started about to fire). Simulate a
  // watchdog/supersede swapping the flight for this key BEFORE the HTTP
  // promise settles -- the exact race the production trace shows.
  await new Promise(r => setImmediate(r));
  const staleFlight = engine.flights.get(key);
  assert.ok(staleFlight, "a flight must be in progress for this test to mean anything");
  engine.flights.set(key, { id: 999999, controller: new AbortController() }); // a DIFFERENT flight object now "owns" this key

  assert.ok(!book.ownUnknownAt, "sanity: ownUnknownAt must start clear");
  releaseRequest();
  await new Promise(r => setImmediate(r));
  await new Promise(r => setImmediate(r));

  assert.ok(httpStartedFired, "the POST must have actually started (http_start) for this race to be real");
  assert.ok(book.ownUnknownAt > 0, "an ambiguous POST error on a superseded flight must mark the book's own state unknown, so a replacement SEND reconciles first instead of blindly re-posting");
});

// ---- Case 2: a DEFINITELY-NOT-SENT error on a superseded flight must NOT mark ownUnknownAt (no reconciliation needed) ----
await check("definitely-not-sent error on a superseded flight does not mark ownUnknownAt -- nothing to reconcile", async () => {
  let releaseRequest;
  const { engine, row, key, book } = buildEngine({
    tokenId: "77", slug: "coll",
    httpImpl: () => new Promise((resolve, reject) => {
      releaseRequest = () => reject(Object.assign(new Error("DNS failure"), { notSent: true }));
    })
  });
  engine.apply({
    collectionSlug: "coll", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "77" },
    kind: "item", orderHash: "0xrival2", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
    event: "item_received_bid", pricePerItem: 0.02
  });
  await new Promise(r => setImmediate(r));
  await new Promise(r => setImmediate(r));
  engine.flights.set(key, { id: 999999, controller: new AbortController() });
  releaseRequest();
  await new Promise(r => setImmediate(r));
  await new Promise(r => setImmediate(r));
  assert.ok(!book.ownUnknownAt, "a request that definitely never left the machine needs no reconciliation");
});

devRuntime.spendAllowed = realSpendAllowed;
process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
