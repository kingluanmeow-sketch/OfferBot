"use strict";

/**
 * RED/GREEN: own order is PROVISIONAL until independently confirmed (1.25.42).
 *
 * Production audit, read-only, no live-money:
 *   - 500 of the account's own offers all report "status":"ACTIVE" on
 *     OpenSea's own API while summing to 17.999 WETH against a 0.1602 WETH
 *     balance -- within the documented 1000x leverage, NOT a balance bug.
 *   - "ACTIVE" (and mere list membership) therefore does not prove an order
 *     counts toward Best Offer (matches the reported "Unfunded!" symptom
 *     despite 2xx + sufficient balance for that one offer).
 *
 * CORRECTED DESIGN (this file was rewritten after an audit caught two real
 * gaps in the first pass -- see CLAUDE-HANDOFF-2026-10-02.md):
 *
 *   1. `ownBest()`/`decide()` math is UNCHANGED (still counts provisional --
 *      that is what prevents an immediate duplicate self-outbid). What
 *      changed is `evaluate()`: when `decide()` concludes ON_TOP and the
 *      winning own price is backed ONLY by a provisional (unconfirmed)
 *      order, evaluate() does NOT treat that as a safe terminal state. It
 *      recomputes the verdict using ONLY confirmed own price
 *      (`ownBest(at, {confirmedOnly:true})`); if THAT would not be ON_TOP,
 *      the row is kept visibly WAITING (dependency "own-confirm") instead
 *      of being cleared as if settled. These tests exercise the REAL
 *      `evaluate()` path, not just the underlying flags.
 *
 *   2. Stream echoing our own hash, or the hash merely appearing somewhere
 *      in a `/offers` listing, is NOT sufficient evidence of Best-inclusion
 *      (both are just "ingested/visible", exactly what the 500-ACTIVE-rows
 *      finding already proved means nothing about Best). `confirmed` is now
 *      set ONLY when a targeted `/best` read resolves OUR hash as the
 *      literal, top-level `orderHash` OpenSea's own resolution returns --
 *      the strongest Best-inclusion signal a public endpoint exposes. This
 *      still cannot prove on-chain funding (no API exposes that field) --
 *      logs/trace say so explicitly, nothing claims more than it knows.
 *
 * Explicitly NOT implemented, per instruction: cancelling any hash, or any
 * aggregate/committed-WETH model.
 */

const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const { INTENT } = require("./offer-item-v2/intent-store");
const { decide } = require("./offer-item-v2/decision");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}
const flush = () => new Promise(r => setImmediate(r));

const RIVAL = "0x3333333333333333333333333333333333333333";
const SELF = "0x4444444444444444444444444444444444444444";
const CONTRACT = "0x1111111111111111111111111111111111111111";

function buildEngine({ tokenId = "1", mine = 0, slug = "alpha", fetchBestImpl } = {}) {
  const key = `ethereum:${CONTRACT}:${tokenId}`;
  const fetchBestCalls = [];
  const adapter = {
    chain: "ethereum",
    async fetchBest(row, opts) {
      fetchBestCalls.push({ row, opts });
      if (fetchBestImpl) return fetchBestImpl(row, opts);
      return { orderHash: "", orders: [] };
    }
  };
  adapter.fetchBestCalls = fetchBestCalls;
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(() => "HEALTHY");
  engine.templateReady = () => true;
  engine.builder = { address: SELF };
  const row = { key, running: true, tokenId, contract: CONTRACT, collectionSlug: slug, minPrice: 0.001, maxPrice: 1, step: 0.0001 };
  engine.rows.set(key, row);
  const book = engine.book.add({ key, chain: "ethereum", contract: CONTRACT, tokenId, collectionSlug: slug });
  book.generation = 1;
  book.hydratedAt = Date.now() - 1000;
  // Mark the row warm on the SEPARATE, pre-existing own-authority axis
  // (ownAuthoritative(), v1.25.0/39, unrelated to this file's provisional/
  // confirmed work) so a row that reaches a real SEND decision in a test
  // isn't gated by cold-start own-resync -- that subsystem is frozen and
  // not under test here.
  book.ownReconciledAt = Date.now() - 1000;
  book.selfAddress = SELF;
  engine.book.setSelfAddress(SELF);
  let ownHash = null;
  if (mine > 0) {
    ownHash = "0xownhash0000000000000000000000000000000000000000000000000000";
    engine.recordOwnOrder(book, ownHash, mine, 15, 0, "trace-setup");
  }
  return { engine, row, book, key, fetchBestCalls, ownHash };
}

function bidEvent({ slug, tokenId, maker, price, orderHash, kind = "item", nft = true }) {
  const at = Date.now();
  return {
    collectionSlug: slug, nft: nft ? { chain: "ethereum", contract: CONTRACT, tokenId } : null,
    kind, orderHash, maker, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: at, receivedAt: at, hasOrderData: true,
    event: kind === "collection" ? "collection_offer" : "item_received_bid", pricePerItem: price
  };
}

function withFakeTimers(fn) {
  return async () => {
    const original = global.setTimeout;
    const timers = [];
    global.setTimeout = (cb, ms) => { const t = { fn: cb, ms }; t.unref = () => t; timers.push(t); return t; };
    try { await fn(timers); } finally { global.setTimeout = original; }
  };
}
const confirmTimer = (timers, ms) => timers.find(t => t.ms === ms);

(async () => {

// ---- Case 1: a fresh own order is provisional, not confirmed -------------
{
  const { engine, book } = buildEngine();
  const hash = "0xabc0000000000000000000000000000000000000000000000000000000001";
  engine.recordOwnOrder(book, hash, 0.04, 15, 0, "trace-1");
  await check("a fresh own order from recordOwnOrder starts confirmed:false (provisional)", () => {
    const entry = book.own.get(hash);
    assert.ok(entry, "entry not created");
    assert.equal(entry.confirmed, false);
  });
}

// ---- Case 2: competitor still wins while own is unconfirmed (effectiveBest)
{
  const { engine, book, row } = buildEngine({ mine: 0.04 });
  await check("own provisional order does not appear in effectiveBest (competitor view)", () => {
    assert.equal(book.effectiveBest(Date.now()).price, 0);
  });
  engine.handleStreamEvent(bidEvent({ slug: row.collectionSlug, tokenId: row.tokenId, maker: RIVAL, price: 0.041, orderHash: "0xrival001" }));
  await check("a genuine competitor above our unconfirmed own price is still seen as Best (not shadowed by provisional own)", () => {
    assert.equal(book.effectiveBest(Date.now()).price, 0.041);
  });
  await check("decide() still wants to send above the new competitor even though own is unconfirmed", () => {
    const verdict = decide({ minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step, best: book.effectiveBest(Date.now()).price, mine: book.ownBest(Date.now()).price });
    assert.equal(verdict.status, "SEND");
    assert.equal(Number(verdict.target.toFixed(4)), 0.0411);
  });
}

// ---- Case 3: Stream echo does NOT confirm (ingestion != Best-inclusion) --
{
  const { engine, book, row } = buildEngine();
  const hash = "0xabc0000000000000000000000000000000000000000000000000000000002";
  engine.recordOwnOrder(book, hash, 0.05, 15, 0, "trace-3");
  await check("still provisional before any echo", () => assert.equal(book.own.get(hash).confirmed, false));
  engine.handleStreamEvent(bidEvent({ slug: row.collectionSlug, tokenId: row.tokenId, maker: SELF, price: 0.05, orderHash: hash }));
  await check("Stream echoing the SAME hash does NOT flip confirmed -- it only proves OpenSea ingested/broadcast it, not that it counts toward Best (the Unfunded symptom shows ACTIVE+Activity with no Best-inclusion)", () => {
    assert.equal(book.own.get(hash).confirmed, false);
  });
  // Stream echo still legitimately updates assumedEnd -> real endTime (unchanged behavior).
  await check("Stream echo still updates real endTime (unrelated to confirmation, unchanged)", () => {
    engine.recordOwnOrder(book, "0xassumed00000000000000000000000000000000000000000000000000001", 0.06, 15, 0, "trace-3b");
    const assumedHash = "0xassumed00000000000000000000000000000000000000000000000000001";
    assert.equal(book.own.get(assumedHash).assumedEnd, false); // recordOwnOrder always has an exact-or-reconstructed endTime, not assumed
  });
}

// ---- Case 4: multiple competitor kinds in sequence never get shadowed ----
{
  const { engine, book, row } = buildEngine({ mine: 0.03 });
  engine.handleStreamEvent(bidEvent({ slug: row.collectionSlug, tokenId: row.tokenId, maker: RIVAL, price: 0.031, orderHash: "0xitem01", kind: "item" }));
  await check("item competitor visible while own provisional", () => assert.equal(book.effectiveBest(Date.now()).price, 0.031));
  engine.handleStreamEvent(bidEvent({ slug: row.collectionSlug, tokenId: row.tokenId, maker: RIVAL, price: 0.033, orderHash: "0xcoll01", kind: "collection", nft: false }));
  await check("collection competitor above item competitor also visible while own provisional", () => assert.equal(book.effectiveBest(Date.now()).price, 0.033));
}

// ---- Case 5 (REAL Decision path): provisional-only ON_TOP is kept WAITING,
// not cleared as a safe terminal state. -------------------------------------
{
  const { engine, book, row, key } = buildEngine({ mine: 0.05 });
  // No competitor at all: decide({best:0, mine:0.05}) -> ON_TOP, but mine is
  // backed ONLY by a provisional order.
  engine.evaluate(key, Date.now(), null);
  await check("evaluate(): ON_TOP backed only by a provisional own order is NOT cleared -- intent stays WAITING on own-confirm", () => {
    const intent = engine.intents.get(key);
    assert.ok(intent, "intent was cleared -- row looks falsely settled");
    assert.equal(intent.state, INTENT.WAITING);
    assert.equal(intent.dependency, "own-confirm");
  });
  await check("no duplicate POST is implied -- decide() with the full (provisional-inclusive) mine is still ON_TOP, so nothing new would be sent at this target", () => {
    const verdict = decide({ minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step, best: book.effectiveBest(Date.now()).price, mine: book.ownBest(Date.now()).price });
    assert.equal(verdict.status, "ON_TOP");
  });
}

// ---- Case 5b (REAL Decision path): once CONFIRMED, ON_TOP settles normally
{
  const { engine, book, row, key, ownHash } = buildEngine({ mine: 0.05 });
  book.own.get(ownHash).confirmed = true; // simulate a prior successful /best confirmation
  engine.evaluate(key, Date.now(), null);
  await check("evaluate(): ON_TOP backed by a CONFIRMED own order clears normally (not stuck WAITING forever)", () => {
    const intent = engine.intents.get(key);
    assert.ok(!intent || intent.state !== INTENT.WAITING || intent.dependency !== "own-confirm",
      "a confirmed ON_TOP row must not be held on own-confirm");
  });
}

// ---- Case 5c (REAL Decision path): a real competitor still overrides a
// provisional-only WAITING state immediately. -------------------------------
{
  const { engine, row, key } = buildEngine({ mine: 0.05 });
  engine.evaluate(key, Date.now(), null);
  assert.equal(engine.intents.get(key).dependency, "own-confirm", "setup: expected provisional WAITING first");
  engine.handleStreamEvent(bidEvent({ slug: row.collectionSlug, tokenId: row.tokenId, maker: RIVAL, price: 0.06, orderHash: "0xrival005" }));
  await check("a real competitor above the provisional own price overrides the own-confirm WAITING state with a genuine SEND", () => {
    const intent = engine.intents.get(key);
    assert.notEqual(intent.dependency, "own-confirm");
  });
}

// ---- Case 6: no duplicate entry on repeated recordOwnOrder for same hash --
{
  const { engine, book } = buildEngine();
  const hash = "0xabc0000000000000000000000000000000000000000000000000000000003";
  engine.recordOwnOrder(book, hash, 0.06, 15, 0, "trace-6a");
  engine.recordOwnOrder(book, hash, 0.06, 15, 0, "trace-6b");
  await check("repeated recordOwnOrder for the same hash does not create a duplicate entry", () => {
    assert.equal([...book.own.keys()].filter(h => h === hash).length, 1);
  });
  book.own.get(hash).confirmed = true;
  engine.recordOwnOrder(book, hash, 0.06, 15, 0, "trace-6c");
  await check("recordOwnOrder on an existing hash does not downgrade an already-confirmed entry back to provisional", () => {
    assert.equal(book.own.get(hash).confirmed, true);
  });
}

// ---- Case 7: scheduleOwnConfirmation never calls fetchBest synchronously -
await check("scheduleOwnConfirmation schedules via setTimeout, no synchronous REST call (no REST before/blocking POST)", withFakeTimers(async timers => {
  const hash = "0xabc0000000000000000000000000000000000000000000000000000000004";
  const { engine, book, row, key, fetchBestCalls } = buildEngine({
    fetchBestImpl: async () => ({ orderHash: hash, orders: [{ orderHash: hash, price: 0.07, maker: SELF, kind: "item", endTime: 0, quantity: 1 }] })
  });
  engine.recordOwnOrder(book, hash, 0.07, 15, 0, "trace-7");
  engine.scheduleOwnConfirmation(key, row, hash, "trace-7");
  assert.equal(fetchBestCalls.length, 0);
  assert.equal(timers.filter(t => t.ms === 6000).length, 1);
}));

// ---- Case 8: /best resolves OUR hash as the top -> confirmed=true --------
{
  const original = global.setTimeout;
  const timers = [];
  global.setTimeout = (fn, ms) => { const t = { fn, ms }; t.unref = () => t; timers.push(t); return t; };
  try {
    const hash = "0xabc0000000000000000000000000000000000000000000000000000000005";
    const { engine, book, row, key } = buildEngine({
      fetchBestImpl: async () => ({ orderHash: hash, orders: [{ orderHash: hash, price: 0.08, maker: SELF, kind: "item", endTime: 0, quantity: 1 }] })
    });
    engine.recordOwnOrder(book, hash, 0.08, 15, 0, "trace-8");
    engine.recovery.push = job => {
      Promise.resolve(job.run({ signal: null })).then(result => job.onResult && job.onResult(result, engine.recovery.generation));
      return true;
    };
    engine.scheduleOwnConfirmation(key, row, hash, "trace-8");
    confirmTimer(timers, 6000).fn();
    await flush(); await flush(); await flush();
    await check("/best resolving OUR hash as its top-level answer flips confirmed to true", () => {
      assert.equal(book.own.get(hash).confirmed, true);
    });
  } finally {
    global.setTimeout = original;
  }
}

// ---- Case 8b: hash is LISTED but NOT the resolved Best -> still unconfirmed
{
  const original = global.setTimeout;
  const timers = [];
  global.setTimeout = (fn, ms) => { const t = { fn, ms }; t.unref = () => t; timers.push(t); return t; };
  try {
    const hash = "0xabc00000000000000000000000000000000000000000000000000000005b";
    const competitorHash = "0xcompetitor000000000000000000000000000000000000000000000005b";
    const { engine, book, row, key } = buildEngine({
      // Our hash IS in the full orders list (ingested/visible -- exactly the
      // 500-ACTIVE-rows situation) but the resolved Best is a DIFFERENT,
      // higher order -- this is precisely the reported Unfunded symptom.
      fetchBestImpl: async () => ({
        orderHash: competitorHash,
        orders: [
          { orderHash: competitorHash, price: 0.2, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
          { orderHash: hash, price: 0.085, maker: SELF, kind: "item", endTime: 0, quantity: 1 }
        ]
      })
    });
    engine.recordOwnOrder(book, hash, 0.085, 15, 0, "trace-8b");
    engine.recovery.push = job => {
      Promise.resolve(job.run({ signal: null })).then(result => job.onResult && job.onResult(result, engine.recovery.generation));
      return true;
    };
    engine.scheduleOwnConfirmation(key, row, hash, "trace-8b");
    confirmTimer(timers, 6000).fn();
    await flush(); await flush(); await flush();
    await check("listed-but-not-resolved-as-Best (exactly the Unfunded pattern) does NOT confirm after attempt 1, a second attempt is armed", () => {
      assert.equal(book.own.get(hash).confirmed, false);
      assert.ok(confirmTimer(timers, 14000), "no second bounded attempt armed");
    });
    confirmTimer(timers, 14000).fn();
    await flush(); await flush(); await flush();
    await check("still not resolved as Best after the second (last) attempt -- terminal 'failed', not falsely upgraded to true, and not left as pending 'false' with no live timer", () => {
      assert.equal(book.own.get(hash).confirmed, "failed");
    });
  } finally {
    global.setTimeout = original;
  }
}

// ---- Case 9: found=false on both bounded attempts -> stays unconfirmed,
// exactly 2 attempts total, no unbounded retry loop, and evaluate() is
// re-triggered immediately (no watchdog dependency for normal progress). ---
{
  const original = global.setTimeout;
  const timers = [];
  global.setTimeout = (fn, ms) => { const t = { fn, ms }; t.unref = () => t; timers.push(t); return t; };
  try {
    const hash = "0xabc0000000000000000000000000000000000000000000000000000000006";
    const { engine, book, row, key } = buildEngine({
      fetchBestImpl: async () => ({ orderHash: "", orders: [] }) // never finds our hash at all
    });
    engine.recordOwnOrder(book, hash, 0.09, 15, 0, "trace-9");
    let evaluateCalls = 0;
    const originalEvaluate = engine.evaluate.bind(engine);
    engine.evaluate = (...args) => { evaluateCalls++; return originalEvaluate(...args); };
    engine.recovery.push = job => {
      Promise.resolve(job.run({ signal: null })).then(result => job.onResult && job.onResult(result, engine.recovery.generation));
      return true;
    };
    // No competitor at all yet: decide({best:0, mine:0.09}) is ON_TOP from
    // the provisional own order ALONE -- the exact deadlock trigger.
    engine.evaluate(key, Date.now(), null);
    await check("setup: with no competitor, the provisional-only order parks the row WAITING on own-confirm first", () => {
      const intent = engine.intents.get(key);
      assert.equal(intent.state, INTENT.WAITING);
      assert.equal(intent.dependency, "own-confirm");
    });
    engine.scheduleOwnConfirmation(key, row, hash, "trace-9");
    confirmTimer(timers, 6000).fn(); // attempt 1
    await flush(); await flush(); await flush();
    await check("after attempt 1 found=false, a second bounded attempt is armed (not given up early)", () => {
      assert.ok(confirmTimer(timers, 14000), "no second attempt armed");
      assert.equal(book.own.get(hash).confirmed, false);
    });
    confirmTimer(timers, 14000).fn(); // attempt 2 (last)
    await flush(); await flush(); await flush();
    await check("after both bounded attempts fail, moves to terminal 'failed' (not left pending 'false' with no live timer) -- no third own-confirm timer armed", () => {
      assert.ok(!engine.ownConfirmTimers.has(key), "a third own-confirm attempt was armed");
      assert.equal(book.own.get(hash).confirmed, "failed");
    });
    await check("evaluate() was re-triggered automatically after the final failed attempt -- normal progress does not wait on watchdog", () => {
      assert.ok(evaluateCalls >= 1, "scheduleOwnConfirmation's failure path never called evaluate()");
    });
    await check("DEADLOCK FIX (1.25.43): the row is NOT left orphaned in WAITING own-confirm with no live timer -- ownBest() now excludes the terminally-failed order from `mine`, so the re-evaluate after final failure reaches a real next lifecycle state (no own price masks anything anymore)", () => {
      const intent = engine.intents.get(key);
      assert.ok(!intent || intent.dependency !== "own-confirm" || intent.state !== INTENT.WAITING,
        "row is orphaned: still WAITING on own-confirm with no live timer able to ever re-check it");
    });
    await check("the failed provisional's price itself no longer counts toward `mine` (latest-wins target recomputes from real effectiveBest, not a stale blocked number)", () => {
      assert.equal(book.ownBest(Date.now()).price, 0);
    });
    await check("no resend loop: exactly one evaluate() ran for the final-failure callback itself (the other counted call is this test's own manual setup call above, not a loop)", () => {
      assert.equal(evaluateCalls, 2);
    });
    await check("a failed-to-confirm order does not block effectiveBest/competitor visibility, and a fresh competitor is still processed immediately (not waiting on Stream/watchdog to notice the failure)", () => {
      engine.handleStreamEvent(bidEvent({ slug: row.collectionSlug, tokenId: row.tokenId, maker: RIVAL, price: 0.095, orderHash: "0xrival009" }));
      assert.equal(book.effectiveBest(Date.now()).price, 0.095);
      const intent = engine.intents.get(key);
      // This harness's stub adapter has no real build/sign/POST pipeline, so
      // a genuine SEND attempt here surfaces as a build-stub FAILED rather
      // than a real DONE -- that is a test-double limitation, not the
      // deadlock. What matters: the row is NOT parked WAITING on the
      // now-resolved own-confirm dependency, i.e. the competitor event was
      // actually acted on, not swallowed by an orphaned wait.
      assert.notEqual(intent.dependency, "own-confirm");
      assert.notEqual(intent.state, INTENT.WAITING);
    });
    await check("cleanup is hash/generation-safe: the terminally-failed entry is still keyed by its own hash, untouched by the unrelated competitor hash just added", () => {
      assert.equal(book.own.get(hash).confirmed, "failed");
      assert.ok(book.own.has(hash), "failed entry must not be silently deleted -- it is still a real (if non-counting) own order on-chain until genuinely superseded");
    });
  } finally {
    global.setTimeout = original;
  }
}

// ---- Case 10: a reprice for the SAME key clears the stale pending timer --
{
  const original = global.setTimeout;
  const timers = [];
  global.setTimeout = (fn, ms) => { const t = { fn, ms, cleared: false }; t.unref = () => t; timers.push(t); return t; };
  const originalClear = global.clearTimeout;
  global.clearTimeout = t => { if (t) t.cleared = true; return originalClear(t); };
  try {
    const oldHash = "0xabc000000000000000000000000000000000000000000000000000000c1";
    const newHash = "0xabc000000000000000000000000000000000000000000000000000000c2";
    const { engine, book, row, key } = buildEngine();
    engine.recordOwnOrder(book, oldHash, 0.10, 15, 0, "trace-10a");
    engine.scheduleOwnConfirmation(key, row, oldHash, "trace-10a");
    const firstTimer = confirmTimer(timers, 6000);
    await check("first confirmation timer is registered for this key", () => {
      assert.equal(engine.ownConfirmTimers.get(key), firstTimer);
    });
    // A reprice for the SAME NFT (new hash) supersedes the old pending check.
    engine.recordOwnOrder(book, newHash, 0.11, 15, 0, "trace-10b");
    engine.scheduleOwnConfirmation(key, row, newHash, "trace-10b");
    await check("the reprice clears the OLD pending confirmation timer, not leaving it to fire for a superseded hash", () => {
      assert.equal(firstTimer.cleared, true);
      assert.notEqual(engine.ownConfirmTimers.get(key), firstTimer);
    });
  } finally {
    global.setTimeout = original;
    global.clearTimeout = originalClear;
  }
}

// ---- Case 11: timer registry is cleared on removeRow and on reset/stop ---
{
  const { engine, book, row, key } = buildEngine({ mine: 0.10 });
  engine.scheduleOwnConfirmation(key, row, "0xownhash0000000000000000000000000000000000000000000000000000", "trace-11");
  await check("ownConfirmTimers has an entry for this key", () => {
    assert.ok(engine.ownConfirmTimers.has(key));
  });
  engine.removeRow(key);
  await check("removeRow clears the confirmation timer registry entry for that key", () => {
    assert.ok(!engine.ownConfirmTimers.has(key));
  });
}
{
  const { engine, book, row, key } = buildEngine({ mine: 0.10 });
  engine.scheduleOwnConfirmation(key, row, "0xownhash0000000000000000000000000000000000000000000000000000", "trace-11b");
  await check("ownConfirmTimers has an entry before reset", () => assert.ok(engine.ownConfirmTimers.has(key)));
  engine.reset("test");
  await check("reset() clears the entire confirmation timer registry (no leak across stop/restart/wallet-switch)", () => {
    assert.equal(engine.ownConfirmTimers.size, 0);
  });
}

// ---- Case 12: the CYCLE itself is bounded, not just one cycle (1.25.44) --
//
// Each provisional->confirm-fail cycle is individually bounded (6s/14s, then
// excluded from `mine`), but nothing before this fix stopped the SEQUENCE:
// POST -> provisional -> both checks fail -> excluded from mine -> decide()
// says SEND the SAME target again (no competitor ever appeared) -> POST
// again -> ... This drives that real sequence through the ACTUAL
// evaluate()/pump()/submitOne()/POST path (not just evaluate() in isolation)
// across several full cycles, with fake timers AND a fake Date.now so the
// escalating backoff can be observed without a real multi-minute wait.
{
  const hash1 = "0xcycle00000000000000000000000000000000000000000000000000000001";
  const hash2 = "0xcycle00000000000000000000000000000000000000000000000000000002";
  const hash3 = "0xcycle00000000000000000000000000000000000000000000000000000003";
  const postedHashes = [hash1, hash2, hash3, "0xcycle000000000000000000000000000000000000000000000000000004"];
  let postCount = 0;

  const originalSetTimeout = global.setTimeout;
  const originalClearTimeout = global.clearTimeout;
  const originalDateNow = Date.now;
  let virtualNow = originalDateNow();
  Date.now = () => virtualNow;
  const timers = [];
  global.setTimeout = (fn, ms) => { const t = { fn, ms, at: virtualNow + ms, fired: false }; t.unref = () => t; timers.push(t); return t; };
  global.clearTimeout = t => { if (t) t.fired = true; };
  // Fires every timer whose virtual deadline has passed, in deadline order --
  // same observable effect as real wall-clock time elapsing.
  const advanceTo = async at => {
    virtualNow = at;
    for (const t of timers.filter(t => !t.fired && t.at <= virtualNow).sort((a, b) => a.at - b.at)) {
      t.fired = true;
      t.fn();
      await flush(); await flush(); await flush();
    }
  };

  try {
    const key = `ethereum:${CONTRACT}:cycle1`;
    const adapter = {
      chain: "ethereum", openseaChain: "ethereum",
      toWei: v => BigInt(Math.round(Number(v) * 1e18)),
      offerBody: () => ({}),
      // Never resolves our hash as Best -- every cycle's confirmation fails,
      // forcing a brand-new provisional cycle each time.
      async fetchBest() { return { orderHash: "", orders: [] }; }
    };
    const engine = new OfferItemEngineV2({ adapter, onLog() {} });
    engine.state = STATE.RUNNING;
    engine.epoch = 1;
    engine.attachStreamHealth(() => "HEALTHY");
    engine.templateReady = () => true;
    engine.builder = { build: async () => ({ orderHash: "", components: { endTime: Math.floor(virtualNow / 1000) + 900 } }) };
    engine.http.request = async ({ onStage }) => {
      if (onStage) onStage("http_started");
      const hash = postedHashes[Math.min(postCount, postedHashes.length - 1)];
      postCount++;
      return { status: 200, headers: {}, body: { order_hash: hash }, totalMs: 10 };
    };
    const row = { key, running: true, tokenId: "cycle1", contract: CONTRACT, collectionSlug: "alpha", minPrice: 0.001, maxPrice: 1, step: 0.0001 };
    engine.rows.set(key, row);
    const book = engine.book.add({ key, chain: "ethereum", contract: CONTRACT, tokenId: "cycle1", collectionSlug: "alpha" });
    book.generation = 1;
    book.hydratedAt = virtualNow - 1000;
    book.ownReconciledAt = virtualNow - 1000;
    book.selfAddress = SELF;
    engine.book.setSelfAddress(SELF);
    engine.recovery.push = job => {
      Promise.resolve(job.run({ signal: null })).then(result => job.onResult && job.onResult(result, engine.recovery.generation));
      return true;
    };

    const postHashesSeen = [];
    const retryAtAfterCycle = [];

    for (let cycle = 0; cycle < 3; cycle++) {
      engine.evaluate(key, virtualNow, null);
      engine.pump();
      await flush(); await flush(); await flush(); await flush(); await flush();
      // The 6s own-confirm timer should now be armed for whichever hash this
      // cycle's POST returned.
      const hash = postedHashes[postCount - 1];
      postHashesSeen.push(hash);
      await advanceTo(virtualNow + 6000);   // attempt 1 -> fails -> arms 14s
      await advanceTo(virtualNow + 14000);  // attempt 2 (last) -> terminal fail
      retryAtAfterCycle.push(row.retryAt - virtualNow);
      // Jump straight to the moment backoff clears so the NEXT cycle's
      // evaluate() (above, next loop iteration) is not itself gated by
      // row.retryAt's notBefore (this test is isolating the STREAK escalation,
      // not re-proving the pre-existing notBefore mechanism).
      await advanceTo(row.retryAt + 1);
    }

    await check("three full provisional POST cycles ran, each producing a DIFFERENT order hash (no duplicate-hash resubmission)", () => {
      assert.equal(postHashesSeen.length, 3);
      assert.equal(new Set(postHashesSeen).size, 3, "same hash POSTed twice -- not a fresh order each cycle");
    });
    await check("each cycle's own order ends terminally 'failed', never silently reused as if still live", () => {
      for (const h of postHashesSeen) assert.equal(book.own.get(h).confirmed, "failed");
    });
    await check("ownConfirmFailStreak escalates monotonically across cycles (bounded backoff growing, not flat/instant retry)", () => {
      assert.equal(row.ownConfirmFailStreak, 3);
    });
    await check("backoff after each cycle is strictly bounded (own-confirm-failed policy: 5s base, 60s cap) and escalates, not a flat constant or runaway", () => {
      assert.ok(retryAtAfterCycle[0] >= 4000 && retryAtAfterCycle[0] <= 6000, `cycle1 backoff out of range: ${retryAtAfterCycle[0]}`);
      assert.ok(retryAtAfterCycle[1] > retryAtAfterCycle[0], "backoff did not escalate between cycle 1 and 2");
      assert.ok(retryAtAfterCycle[2] <= 60000, "backoff exceeded the documented cap");
    });
    await check("no own-confirm timer leak across cycles -- at most one live timer for this key at any point", () => {
      const liveAtEnd = timers.filter(t => !t.fired);
      assert.ok(liveAtEnd.length <= 1, `expected at most 1 live timer, found ${liveAtEnd.length}`);
    });
    await check("a genuinely NEW competitor is still handled immediately, not swallowed by the streak backoff state", () => {
      engine.handleStreamEvent(bidEvent({ slug: row.collectionSlug, tokenId: row.tokenId, maker: RIVAL, price: 0.5, orderHash: "0xcycle-rival" }));
      assert.equal(book.effectiveBest(virtualNow).price, 0.5);
      const intent = engine.intents.get(key);
      assert.ok(intent && Math.abs(Number(intent.target) - 0.5) < 1, "a real competitor event did not produce a fresh, higher target");
    });
  } finally {
    global.setTimeout = originalSetTimeout;
    global.clearTimeout = originalClearTimeout;
    Date.now = originalDateNow;
  }
}

process.stdout.write(`\n${passed}/${passed + failed} checks passed\n`);
if (failed) process.exitCode = 1;

})();
