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
const productionTrace = require("./production-trace");

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

// ---- Case 3 (audit #2): an ambiguous POST error on a SUPERSEDED flight must leave a terminal trace ----
// `http_start` is always recorded for a real POST attempt (see submitOne's onStage hook). Previously the
// `!alive()` branch returned with ZERO trace of any kind on an ambiguous error -- http_start got no
// terminal partner at all, unlike the sibling (still-alive) ambiguous branch which records submit_failure.
await check("ambiguous POST error on a SUPERSEDED flight still records a terminal trace (AMBIGUOUS_OUTCOME), not silence", async () => {
  let releaseRequest;
  const realRecord = productionTrace.record;
  const recorded = [];
  productionTrace.record = (stage, event, detail) => { recorded.push({ stage, event, detail }); };
  try {
    const { engine, key, book } = buildEngine({
      tokenId: "9001", slug: "coll",
      httpImpl: ({ onStage }) => new Promise((resolve, reject) => {
        releaseRequest = () => { onStage("http_started"); reject(Object.assign(new Error("socket hang up"), { notSent: false })); };
      })
    });
    engine.apply({
      collectionSlug: "coll", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9001" },
      kind: "item", orderHash: "0xrival9001", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
      event: "item_received_bid", pricePerItem: 0.03
    });
    await new Promise(r => setImmediate(r));
    await new Promise(r => setImmediate(r));
    engine.flights.set(key, { id: 999999, controller: new AbortController() });
    releaseRequest();
    await new Promise(r => setImmediate(r));
    await new Promise(r => setImmediate(r));

    const httpStart = recorded.find(r => r.stage === "http_start");
    assert.ok(httpStart, "sanity: http_start must have been recorded");
    const terminal = recorded.find(r => r.stage !== "http_start" &&
      ((r.detail && (r.detail.status === "AMBIGUOUS_OUTCOME" || r.detail.status === "SUCCESS" || r.detail.status === "FAILED"))));
    assert.ok(terminal, "every http_start must be followed by a terminal trace (SUCCESS / FAILED / AMBIGUOUS_OUTCOME), never silence on a superseded flight's ambiguous error");
    assert.equal(terminal.detail.status, "AMBIGUOUS_OUTCOME", "an ambiguous (possibly-delivered) error on a superseded flight must terminate as AMBIGUOUS_OUTCOME, not be misreported as a clean FAILED/SUCCESS");
    assert.ok(book.ownUnknownAt > 0, "ownUnknownAt must still be marked so the replacement flight reconciles");
  } finally {
    productionTrace.record = realRecord;
  }
});

// ---- Case 4: http_start -> notSent on a SUPERSEDED flight must still terminate (DEFINITELY_NOT_SENT), not be silent ----
// PROVEN via submitter.js source: stage("http_started") fires unconditionally BEFORE `req` is even
// constructed (line ~121, well before https.request()); `notSent = !flushed && !firstByteAt` and `flushed`
// only becomes true on req.on("finish") AFTER req.write()/req.end() (line ~184). So a DNS failure, refused
// connection, or a synchronous throw while building the request (caught at line ~159) all land with
// notSent===true STRICTLY AFTER http_started was already recorded. This is the ordinary, common shape of
// a notSent error, not an edge case -- the invariant does NOT hold that notSent cannot follow http_started.
await check("http_start -> notSent on a SUPERSEDED flight still records a terminal trace (DEFINITELY_NOT_SENT), no blind reconcile/retry, no duplicate", async () => {
  let releaseRequest;
  const realRecord = productionTrace.record;
  const recorded = [];
  productionTrace.record = (stage, event, detail) => { recorded.push({ stage, event, detail }); };
  try {
    const { engine, key, book } = buildEngine({
      tokenId: "9002", slug: "coll",
      httpImpl: ({ onStage }) => new Promise((resolve, reject) => {
        releaseRequest = () => { onStage("http_started"); reject(Object.assign(new Error("ENOTFOUND api.opensea.io"), { notSent: true })); };
      })
    });
    engine.apply({
      collectionSlug: "coll", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9002" },
      kind: "item", orderHash: "0xrival9002", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
      event: "item_received_bid", pricePerItem: 0.04
    });
    await new Promise(r => setImmediate(r));
    await new Promise(r => setImmediate(r));
    // Supersede the flight BEFORE the DNS failure comes back -- same race as
    // the ambiguous case, but this time the error is DEFINITELY_NOT_SENT.
    engine.flights.set(key, { id: 999999, controller: new AbortController() });
    releaseRequest();
    await new Promise(r => setImmediate(r));
    await new Promise(r => setImmediate(r));

    const httpStart = recorded.find(r => r.stage === "http_start");
    assert.ok(httpStart, "sanity: http_start must have been recorded");
    const terminal = recorded.find(r => r.stage === "submit_success" || r.stage === "submit_failure");
    assert.ok(terminal, "http_start must never be followed by silence, even for a definitely-not-sent error on a superseded flight");
    assert.equal(terminal.detail.status, "DEFINITELY_NOT_SENT", "a definitely-not-sent error must terminate as DEFINITELY_NOT_SENT, not AMBIGUOUS_OUTCOME (nothing to reconcile) and not silence");
    assert.ok(!book.ownUnknownAt, "a request that definitely never left the machine needs no reconciliation -- ownUnknownAt must stay clear");
    // No blind retry/reconcile read was queued for this definitely-not-sent, superseded flight.
    assert.ok(!engine.pendingReads.has(key) || engine.pendingReads.get(key)?.reason !== "post-uncertain",
      "a definitely-not-sent error must not queue a targeted reconcile read -- there is nothing ambiguous to reconcile");
  } finally {
    productionTrace.record = realRecord;
  }
});

// ---- Case 5: http_start -> notSent while STILL alive (not superseded) must also terminate cleanly,
// with the existing fast-retry path, and must not duplicate-POST or mark ownUnknownAt ----
await check("http_start -> notSent while the flight is still alive terminates as DEFINITELY_NOT_SENT, schedules exactly one fast retry, no duplicate POST, no reconcile", async () => {
  let releaseRequest;
  const realRecord = productionTrace.record;
  const recorded = [];
  productionTrace.record = (stage, event, detail) => { recorded.push({ stage, event, detail }); };
  const realScheduleRetry = OfferItemEngineV2.prototype.scheduleRetry;
  const retries = [];
  OfferItemEngineV2.prototype.scheduleRetry = function (...args) { retries.push(args); };
  try {
    const { engine, key, book } = buildEngine({
      tokenId: "9003", slug: "coll",
      httpImpl: ({ onStage }) => new Promise((resolve, reject) => {
        releaseRequest = () => { onStage("http_started"); reject(Object.assign(new Error("connect ECONNREFUSED"), { notSent: true })); };
      })
    });
    engine.apply({
      collectionSlug: "coll", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9003" },
      kind: "item", orderHash: "0xrival9003", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
      event: "item_received_bid", pricePerItem: 0.05
    });
    await new Promise(r => setImmediate(r));
    await new Promise(r => setImmediate(r));
    // Flight is NOT superseded this time -- genuinely still alive.
    releaseRequest();
    await new Promise(r => setImmediate(r));
    await new Promise(r => setImmediate(r));

    const terminal = recorded.find(r => r.stage === "submit_success" || r.stage === "submit_failure");
    assert.ok(terminal, "http_start must have a terminal trace on the still-alive notSent path too");
    assert.equal(terminal.detail.status, "DEFINITELY_NOT_SENT");
    assert.ok(!book.ownUnknownAt, "definitely-not-sent needs no reconciliation even on the still-alive path");
    assert.equal(retries.length, 1, "exactly one fast retry must be scheduled -- no duplicate POST attempts stacked");
    assert.equal(retries[0][3], "not-sent", "the retry must be tagged not-sent, matching the terminal classification");
  } finally {
    productionTrace.record = realRecord;
    OfferItemEngineV2.prototype.scheduleRetry = realScheduleRetry;
  }
});

devRuntime.spendAllowed = realSpendAllowed;
process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
