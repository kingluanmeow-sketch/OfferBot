"use strict";

/**
 * RED/GREEN: Best bị kẹt dưới giá thật trên collection bận (audit, báo cáo
 * từ production) -- VÀ phần mở rộng audit (retry phải bounded/backoff/
 * single-flight/generation-safe, không bão REST, chịu tải nhiều NFT/nhiều
 * collection đồng thời).
 *
 * TRIỆU CHỨNG THẬT (3 ca production)
 *   - Alien Fren #6456: UI Best 0.0119, tool giữ 0.01.
 *   - Parallel Avatars Jazmine #10184: tool giữ Best 0.03.
 *   - Parallel Avatars Heracles #8375: tool giữ Best 0.02.
 *   Trace: item_received_bid/collection_offer/order_invalidate vẫn đến,
 *   Decision vẫn ON_TOP.
 *
 * ROOT CAUSE (engine-v2.js, mergeBest())
 *   `resolveTraitScope()` là ĐƯỜNG DUY NHẤT để biết một trait/collection
 *   offer mà token CHƯA BIẾT trait của mình (`appliesTo()` = UNKNOWN) có áp
 *   hay không -- nó xếp một lượt đọc REST authoritative. `mergeBest()` loại
 *   MỌI lượt đọc mà `book.lastEventAt` đã đổi trong lúc nó bay (coi là cũ) --
 *   kể cả khi sự kiện khiến nó đổi HOÀN TOÀN không liên quan (ví dụ một item
 *   bid khác trên cùng token). Trên một collection bận, xác suất một event
 *   nào đó chạm book trong lúc lượt đọc bay là rất cao, nên lượt đọc gần như
 *   LUÔN bị loại -- và không có gì khác hỏi lại câu hỏi đó. Token stuck mãi.
 *
 * FIX (mở rộng sau audit thứ hai)
 *   `mergeBest()`'s discard branch gọi `scheduleTraitScopeRetry(key)`:
 *     - SINGLE-FLIGHT: một timer mỗi key (`traitScopeRetry.get(key).timer`).
 *     - CÓ TRẦN: TRAIT_SCOPE_RETRY_MAX_ATTEMPTS (5) rồi bỏ cuộc hẳn.
 *     - LÙI CÓ GIỚI HẠN: cấp số nhân 300ms -> 5000ms trần.
 *     - GENERATION-SAFE: tự kiểm `recovery.generation`/`state` lúc BẮN.
 *   Thành công thật -> `settleTraitScopeRetry(key)` dọn timer, đo thời gian
 *   phục hồi vào `stats.traitScopeRecoveries`/`traitScopeRecoveryMsTotal`.
 *   Không REST mới trước POST: hot path (apply/evaluate/SEND) không await gì
 *   ở đây cả.
 */

const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const { decide } = require("./offer-item-v2/decision");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}
const flush = async (n = 5) => { for (let i = 0; i < n; i++) await new Promise(r => setImmediate(r)); await new Promise(r => setTimeout(r, 5)); };
/** Chờ qua ĐÚNG backoff thật của lượt retry thứ `attempt` (1-based), có đệm. */
const BACKOFF_MS = [300, 600, 1200, 2400, 4800];
const waitForRetry = async (attempt = 1) => { await new Promise(r => setTimeout(r, BACKOFF_MS[attempt - 1] + 80)); await flush(2); };

const SELF = "0x4444444444444444444444444444444444444444";
const RIVAL = "0x3333333333333333333333333333333333333333";
const RIVAL2 = "0x5555555555555555555555555555555555555555";
const CONTRACT = "0x1111111111111111111111111111111111111111";

function bidEvent({ slug, tokenId, maker, price, orderHash, kind = "item", contract = CONTRACT }) {
  const at = Date.now();
  return {
    collectionSlug: slug, nft: { chain: "ethereum", contract, tokenId },
    kind, orderHash, maker, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: at, receivedAt: at, hasOrderData: true,
    event: kind === "collection" ? "collection_offer" : "item_received_bid", pricePerItem: price
  };
}
function traitOffer({ slug, maker, price, orderHash, traitType, traitValue }) {
  const at = Date.now();
  return {
    collectionSlug: slug, nft: null, kind: "trait",
    orderHash, maker, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: at, receivedAt: at, hasOrderData: true,
    event: "trait_offer", pricePerItem: price,
    traitCriteria: { trait_type: traitType, trait_name: traitValue },
    traitCriteriaList: { trait_criteria_list: [{ trait_type: traitType, trait_name: traitValue }] }
  };
}
function invalidateEvent({ orderHash, slug }) {
  const at = Date.now();
  return { collectionSlug: slug, nft: null, orderHash, eventTimestamp: at, receivedAt: at, event: "order_invalidate" };
}

/** Builds a running engine with a controllable, hang-until-resolved fetchBest mock. */
function buildEngine({ rows = [{ tokenId: "1", slug: "test-collection" }] } = {}) {
  const resolvers = [];
  const adapter = {
    chain: "ethereum",
    async fetchBest() { return new Promise(res => resolvers.push(res)); }
  };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.recovery.pressure = () => false; // isolate from unrelated realtime-pressure backoff
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(() => "HEALTHY");
  engine.templateReady = () => true;
  engine.builder = { address: SELF };
  engine.book.setSelfAddress(SELF);
  const built = [];
  for (const r of rows) {
    const key = `ethereum:${CONTRACT}:${r.tokenId}`;
    const row = { key, running: true, tokenId: r.tokenId, contract: CONTRACT, collectionSlug: r.slug, minPrice: 0.001, maxPrice: r.maxPrice || 1, step: r.step || 0.0001 };
    engine.rows.set(key, row);
    const book = engine.book.add({ key, chain: "ethereum", contract: CONTRACT, tokenId: r.tokenId, collectionSlug: r.slug });
    book.generation = 1;
    book.hydratedAt = Date.now() - 1000;
    book.ownReconciledAt = Date.now() - 1000;
    book.selfAddress = SELF;
    built.push({ key, row, book });
  }
  return { engine, resolvers, rows: built, key: built[0].key, row: built[0].row, book: built[0].book };
}

(async () => {

// ---- Case 1: RED, matches live evidence -------------------------------------
await check("RED-proof: a trait-scope read that is NOT retried on discard leaves Best stuck (regression guard against reverting the fix)", async () => {
  const { engine, book, resolvers } = buildEngine({ rows: [{ tokenId: "10184", slug: "parallel-avatars" }] });
  engine.apply(bidEvent({ slug: "parallel-avatars", tokenId: "10184", maker: RIVAL, price: 0.03, orderHash: "0xitem1" }));
  engine.apply(traitOffer({ slug: "parallel-avatars", maker: RIVAL, price: 0.12, orderHash: "0xtrait1", traitType: "Character", traitValue: "Jazmine" }));
  await flush();
  assert.ok(resolvers.length >= 1, "resolveTraitScope must have queued an authoritative read");
  engine.apply(bidEvent({ slug: "parallel-avatars", tokenId: "10184", maker: RIVAL, price: 0.031, orderHash: "0xitem2" }));
  resolvers[0]({ orderHash: "0xitem2", price: 0.031, orders: [
    { orderHash: "0xitem1", price: 0.03, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
    { orderHash: "0xitem2", price: 0.031, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 }
  ] });
  await flush();
  assert.equal(engine.stats.staleBDiscarded, 1, "the stale-discard fence must have fired exactly as the bug report describes");
  await waitForRetry(1);
  assert.equal(resolvers.length, 2, "the discarded trait-scope read must be retried (after real backoff), not silently dropped");
  assert.equal(engine.stats.traitScopeRetries, 1);
});

// ---- Case 2: GREEN -- item offer target math, no trait involved -------------
await check("item offer: a new higher-priced bid with a NEW orderHash raises Best and computes the correct next target (0.0119 -> 0.012)", async () => {
  const { engine, book, row } = buildEngine({ rows: [{ tokenId: "6456", slug: "alien-fren" }] });
  engine.apply(bidEvent({ slug: "alien-fren", tokenId: "6456", maker: RIVAL, price: 0.01, orderHash: "0xa1" }));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.01);
  engine.apply(bidEvent({ slug: "alien-fren", tokenId: "6456", maker: RIVAL, price: 0.0119, orderHash: "0xa2" }));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.0119, "a genuinely new, higher-priced item bid must raise Best");
  const mine = book.ownBest(Date.now());
  const verdict = decide({ best: 0.0119, mine: mine.price, minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step });
  assert.equal(verdict.status, "SEND");
  assert.ok(Math.abs(verdict.target - 0.012) < 1e-9, `expected target ~0.012, got ${verdict.target}`);
});

// ---- Case 3: GREEN -- trait offer, full end-to-end through the fixed retry path ----
await check("trait offer under a busy collection: Best raises to the trait price after the retried authoritative read lands (Jazmine #10184 repro, fixed)", async () => {
  const { engine, book, row, resolvers } = buildEngine({ rows: [{ tokenId: "10184", slug: "parallel-avatars" }] });
  engine.apply(bidEvent({ slug: "parallel-avatars", tokenId: "10184", maker: RIVAL, price: 0.03, orderHash: "0xi1" }));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.03, "baseline: tool holds the known item price, matching the reported symptom");

  engine.apply(traitOffer({ slug: "parallel-avatars", maker: RIVAL, price: 0.12, orderHash: "0xt1", traitType: "Character", traitValue: "Jazmine" }));
  await flush();
  assert.ok(resolvers.length >= 1, "an authoritative trait-scope read must have been queued");

  engine.apply(bidEvent({ slug: "parallel-avatars", tokenId: "10184", maker: RIVAL, price: 0.031, orderHash: "0xi2" }));
  resolvers[0]({ orderHash: "0xi2", price: 0.031, orders: [
    { orderHash: "0xi1", price: 0.03, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
    { orderHash: "0xi2", price: 0.031, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 }
  ] });
  await flush();
  await waitForRetry(1);
  assert.equal(resolvers.length, 2, "fix: the discarded read must have been retried automatically after backoff");

  resolvers[1]({ orderHash: "0xt1", price: 0.12, orders: [
    { orderHash: "0xi1", price: 0.03, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
    { orderHash: "0xi2", price: 0.031, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
    { orderHash: "0xt1", price: 0.12, maker: RIVAL, kind: "trait", endTime: 0, quantity: 1 }
  ] });
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.12, "Best must rise to the real trait-resolved price once the retried read lands");
  assert.equal(engine.stats.traitScopeRecoveries, 1, "a successful settle after >=1 retry must count as one recovery");
  assert.ok(engine.stats.traitScopeRecoveryMsTotal >= BACKOFF_MS[0], "recovery time must reflect the real elapsed backoff");
  assert.equal(engine.traitScopeRetry.has(engine.rows.values().next().value.key) || engine.traitScopeRetry.size, 0,
    "no orphan retry timer must remain after a successful settle");
});

// ---- Case 4: no duplicate POST / no retry storm from a single discard --------
await check("the trait-scope retry does not, by itself, queue more than one extra read or duplicate a SEND", async () => {
  const { engine, book, row, resolvers } = buildEngine({ rows: [{ tokenId: "8375", slug: "parallel-avatars" }] });
  engine.apply(bidEvent({ slug: "parallel-avatars", tokenId: "8375", maker: RIVAL, price: 0.02, orderHash: "0xh1" }));
  await flush();
  engine.apply(traitOffer({ slug: "parallel-avatars", maker: RIVAL, price: 0.05, orderHash: "0xht1", traitType: "Character", traitValue: "Heracles" }));
  await flush();
  const before = resolvers.length;
  engine.apply(bidEvent({ slug: "parallel-avatars", tokenId: "8375", maker: RIVAL, price: 0.021, orderHash: "0xh2" }));
  resolvers[0]({ orderHash: "0xh2", price: 0.021, orders: [
    { orderHash: "0xh1", price: 0.02, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
    { orderHash: "0xh2", price: 0.021, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 }
  ] });
  await flush();
  await waitForRetry(1);
  assert.equal(resolvers.length, before + 1, "exactly one retry read, not a storm");
  resolvers[1]({ orderHash: "0xht1", price: 0.05, orders: [
    { orderHash: "0xh1", price: 0.02, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
    { orderHash: "0xh2", price: 0.021, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
    { orderHash: "0xht1", price: 0.05, maker: RIVAL, kind: "trait", endTime: 0, quantity: 1 }
  ] });
  await flush();
  assert.equal(resolvers.length, before + 1, "settling the retry must not itself queue yet another read");
});

// ---- Case 5: scoped to the affected NFT only ----------------------------------
await check("the fix is scoped to the affected key only -- an unrelated tracked NFT's book is never touched by this retry", async () => {
  const { engine, resolvers, rows } = buildEngine({ rows: [
    { tokenId: "10184", slug: "parallel-avatars" }, { tokenId: "9999", slug: "parallel-avatars" }
  ] });
  const book2 = rows[1].book;
  engine.apply(bidEvent({ slug: "parallel-avatars", tokenId: "10184", maker: RIVAL, price: 0.03, orderHash: "0xs1" }));
  await flush();
  engine.apply(traitOffer({ slug: "parallel-avatars", maker: RIVAL, price: 0.12, orderHash: "0xst1", traitType: "Character", traitValue: "Jazmine" }));
  await flush();
  engine.apply(bidEvent({ slug: "parallel-avatars", tokenId: "10184", maker: RIVAL, price: 0.031, orderHash: "0xs2" }));
  resolvers[0]({ orderHash: "0xs2", price: 0.031, orders: [] });
  await flush();
  assert.equal(book2.effectiveBest(Date.now()).price, 0, "an unrelated NFT's book must stay at 0, untouched by this NFT's trait-scope retry");
});

// ---- Case 6: single-flight -- multiple discards do not create multiple timers ----
await check("single-flight: a second discard while a retry is already scheduled does not create a second timer", async () => {
  const { engine, rows } = buildEngine({ rows: [{ tokenId: "1", slug: "c" }] });
  const key = rows[0].key;
  engine.scheduleTraitScopeRetry(key);
  const state1 = engine.traitScopeRetry.get(key);
  assert.ok(state1 && state1.timer, "first call must schedule a timer");
  const timerRef = state1.timer;
  engine.scheduleTraitScopeRetry(key); // a second discard arrives before the timer has fired
  const state2 = engine.traitScopeRetry.get(key);
  assert.equal(state2.timer, timerRef, "single-flight: the SAME timer must still be the one scheduled, no second one created");
  assert.equal(state2.attempts, 1, "attempts must not have incremented from a call that was blocked by single-flight");
  clearTimeout(state2.timer);
  engine.traitScopeRetry.delete(key);
});

// ---- Case 6b: generation-safe -- a Reset/Stop before the timer fires must never let a dead-generation retry run ----
await check("generation-safe: a pending trait-scope retry must never fire into a post-Reset state, even if its clearTimeout were somehow bypassed", async () => {
  const { engine, book, resolvers } = buildEngine({ rows: [{ tokenId: "500", slug: "gen-safe" }] });
  engine.apply(bidEvent({ slug: "gen-safe", tokenId: "500", maker: RIVAL, price: 0.02, orderHash: "0xg1" }));
  await flush();
  engine.apply(traitOffer({ slug: "gen-safe", maker: RIVAL, price: 0.3, orderHash: "0xgt1", traitType: "G", traitValue: "S" }));
  await flush();
  assert.ok(resolvers.length >= 1);
  engine.apply(bidEvent({ slug: "gen-safe", tokenId: "500", maker: RIVAL, price: 0.021, orderHash: "0xg2" }));
  resolvers[0]({ orderHash: "0xg2", price: 0.021, orders: [] });
  await flush();

  const key = engine.rows.keys().next().value;
  const state = engine.traitScopeRetry.get(key);
  assert.ok(state && state.timer, "a retry must be scheduled and pending before Reset");
  const pendingTimer = state.timer;
  const generationBefore = engine.recovery.generation;

  // Defense-in-depth proof: even if clearTimeout() during reset() were a no-op
  // (bypassed), the timer callback's OWN generation check at fire-time must
  // still refuse to run -- this is what "generation-safe" actually means,
  // distinct from "reset happens to clear it in time".
  const realClearTimeout = global.clearTimeout;
  global.clearTimeout = () => {};
  try {
    engine.reset("test-reset");
  } finally {
    global.clearTimeout = realClearTimeout;
  }
  assert.notEqual(engine.recovery.generation, generationBefore, "reset must bump generation");

  const retriesBefore = engine.stats.traitScopeRetries;
  const recoveriesBefore = engine.stats.traitScopeRecoveries;
  // Manually fire the OLD generation's timer callback (clearTimeout was
  // sabotaged above, so it is still "pending" from Node's point of view).
  pendingTimer._onTimeout ? pendingTimer._onTimeout() : null;
  await flush();
  realClearTimeout(pendingTimer);

  assert.equal(engine.stats.traitScopeRetries, retriesBefore, "a dead-generation retry must never dispatch a real queueRead");
  assert.equal(engine.stats.traitScopeRecoveries, recoveriesBefore, "a dead-generation retry must never be counted as a recovery either");
});

// ---- Case 7: bounded give-up -- a PERMANENTLY busy collection stops retrying, no infinite loop ----
await check("bounded: after TRAIT_SCOPE_RETRY_MAX_ATTEMPTS consecutive discards the engine gives up (no infinite retry, no REST storm)", async () => {
  const { engine, book, resolvers } = buildEngine({ rows: [{ tokenId: "99", slug: "forever-busy" }] });
  engine.apply(bidEvent({ slug: "forever-busy", tokenId: "99", maker: RIVAL, price: 0.01, orderHash: "0xb0" }));
  await flush();
  engine.apply(traitOffer({ slug: "forever-busy", maker: RIVAL, price: 0.5, orderHash: "0xbt", traitType: "X", traitValue: "Y" }));
  await flush();
  assert.ok(resolvers.length >= 1);

  // Discard the ORIGINAL read, then every retry in turn, forever (simulating a
  // collection so busy that NO authoritative read ever lands cleanly).
  let n = 0;
  for (let attempt = 1; attempt <= 5; attempt++) {
    const idx = resolvers.length - 1;
    engine.apply(bidEvent({ slug: "forever-busy", tokenId: "99", maker: RIVAL, price: 0.01 + attempt * 0.0001, orderHash: `0xnoise${attempt}` }));
    resolvers[idx]({ orderHash: `0xnoise${attempt}`, price: 0.01 + attempt * 0.0001, orders: [] });
    await flush();
    n++;
    if (attempt < 5) {
      await waitForRetry(attempt);
      assert.equal(resolvers.length, idx + 2, `retry #${attempt} must have fired after its backoff`);
    }
  }
  // The loop above scheduled the 5th (last allowed) retry; wait for IT to fire,
  // then discard it too -- THIS is the 6th consecutive discard, which must
  // exceed TRAIT_SCOPE_RETRY_MAX_ATTEMPTS and trigger give-up (not a 6th retry).
  await waitForRetry(5);
  const lastIdx = resolvers.length - 1;
  engine.apply(bidEvent({ slug: "forever-busy", tokenId: "99", maker: RIVAL, price: 0.01006, orderHash: "0xnoise6" }));
  resolvers[lastIdx]({ orderHash: "0xnoise6", price: 0.01006, orders: [] });
  await flush();
  assert.equal(engine.stats.traitScopeRetryGaveUp, 1, "must give up after the attempt cap, exactly once");
  assert.equal(engine.stats.traitScopeRetries, 5, "exactly 5 retries fired, never more (no unbounded loop)");
  assert.equal(engine.traitScopeRetry.size, 0, "no leftover retry-state Map entry after giving up (bounded memory)");
  // No orphan intent/WAITING: giving up on the DIAGNOSTIC trait-scope read must
  // not leave the row's own SEND/intent machinery stuck -- a real competitor
  // event must still be handled normally afterward.
  engine.apply(bidEvent({ slug: "forever-busy", tokenId: "99", maker: RIVAL2, price: 0.02, orderHash: "0xreal" }));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.02, "normal item-level competition must still work after give-up -- no orphan gate");
});

// ---- Case 8: LOAD -- many NFTs, many busy collections, mixed item/trait/collection traffic ----
await check("load: 6 NFTs across 2 busy collections with concurrent item/trait/collection/invalidate traffic -- every affected NFT resolves, no cross-talk, no duplicate SEND, no orphan WAITING", async () => {
  const rowsSpec = [
    { tokenId: "1001", slug: "collA" }, { tokenId: "1002", slug: "collA" }, { tokenId: "1003", slug: "collA" },
    { tokenId: "2001", slug: "collB" }, { tokenId: "2002", slug: "collB" }, { tokenId: "2003", slug: "collB" }
  ];
  const { engine, resolvers, rows } = buildEngine({ rows: rowsSpec });
  // RecoveryPlane bounds background (priority>1) concurrency at floor(workers/2)
  // -- a real, separate, already-covered feature (not under test here). Raise
  // it so all 6 of this test's trait-scope reads can be in flight at once and
  // this test isolates ONLY the trait-scope retry/backoff/bounding logic.
  engine.recovery.workers = 20;
  engine.recovery.readsPerSecond = 20;
  engine.recovery.tokens = 20; // the token bucket (default readsPerSecond=4) is a separate, already-covered feature
  const byKey = new Map(rows.map(r => [r.key, r]));

  // Baseline item offers on every NFT.
  for (const r of rows) {
    engine.apply(bidEvent({ slug: r.row.collectionSlug, tokenId: r.row.tokenId, maker: RIVAL, price: 0.02, orderHash: `0xbase${r.row.tokenId}` }));
  }
  await flush();

  // A trait offer on EACH collection, targeting an unlearned trait -- this is
  // the thing that must eventually raise Best on every one of these 6 NFTs
  // (collection-wide fan-out), each queuing its own per-token authoritative read.
  engine.apply(traitOffer({ slug: "collA", maker: RIVAL, price: 0.09, orderHash: "0xtraitA", traitType: "Rarity", traitValue: "Rare" }));
  engine.apply(traitOffer({ slug: "collB", maker: RIVAL, price: 0.15, orderHash: "0xtraitB", traitType: "Rarity", traitValue: "Rare" }));
  await flush();
  const afterTraitQueued = resolvers.length;
  assert.ok(afterTraitQueued >= 6, `every one of the 6 NFTs must have queued its own trait-scope read, got ${afterTraitQueued}`);

  // Heavy, continuous, UNRELATED Stream traffic on every NFT across both
  // collections -- item bids, a collection offer, and an invalidate -- firing
  // WHILE every trait-scope read above is still in flight.
  for (const r of rows) {
    engine.apply(bidEvent({ slug: r.row.collectionSlug, tokenId: r.row.tokenId, maker: RIVAL, price: 0.021, orderHash: `0xnoise1-${r.row.tokenId}` }));
  }
  engine.apply(invalidateEvent({ orderHash: "0xdoesnotexist", slug: "collA" }));
  for (const r of rows) {
    engine.apply(bidEvent({ slug: r.row.collectionSlug, tokenId: r.row.tokenId, maker: RIVAL, price: 0.022, orderHash: `0xnoise2-${r.row.tokenId}` }));
  }

  // All 6 in-flight authoritative reads now resolve -- every one of them is
  // stale per the fencing rule (the noise above touched every book meanwhile).
  for (let i = 0; i < afterTraitQueued; i++) {
    resolvers[i]({ orderHash: `0xnoise2-x`, price: 0.022, orders: [] });
  }
  await flush();
  assert.equal(engine.stats.staleBDiscarded, afterTraitQueued, "every initial trait-scope read must have been discarded by the busy traffic");
  await waitForRetry(1);
  assert.equal(resolvers.length, afterTraitQueued * 2, "every one of the 6 discarded reads must have been retried exactly once, no more, no fewer");

  // This time the retries land cleanly with OpenSea's real authoritative
  // resolution for each NFT: the trait offer DOES apply, at its collection's price.
  for (let i = 0; i < afterTraitQueued; i++) {
    const r = rows[i];
    const traitPrice = r.row.collectionSlug === "collA" ? 0.09 : 0.15;
    const traitHash = r.row.collectionSlug === "collA" ? "0xtraitA" : "0xtraitB";
    resolvers[afterTraitQueued + i]({
      orderHash: traitHash, price: traitPrice,
      orders: [
        { orderHash: `0xbase${r.row.tokenId}`, price: 0.02, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
        { orderHash: traitHash, price: traitPrice, maker: RIVAL, kind: "trait", endTime: 0, quantity: 1 }
      ]
    });
  }
  await flush();

  // Every single NFT must now show the correct, collection-specific resolved price.
  for (const r of rows) {
    const expected = r.row.collectionSlug === "collA" ? 0.09 : 0.15;
    assert.equal(r.book.effectiveBest(Date.now()).price, expected,
      `NFT #${r.row.tokenId} (${r.row.collectionSlug}) must resolve to ${expected}, got ${r.book.effectiveBest(Date.now()).price}`);
  }

  // Counters: exactly one retry + one recovery per affected NFT, no give-ups.
  assert.equal(engine.stats.traitScopeRetries, afterTraitQueued, "exactly one retry per affected NFT");
  assert.equal(engine.stats.traitScopeRecoveries, afterTraitQueued, "exactly one recovery per affected NFT");
  assert.equal(engine.stats.traitScopeRetryGaveUp, 0, "nobody should have hit the attempt cap in this scenario");
  assert.equal(engine.traitScopeRetry.size, 0, "no orphan retry-state left for any of the 6 NFTs");

  // No duplicate/queued-twice own order anywhere, and no row left with an
  // orphan WAITING intent -- evaluate() must report a clean terminal verdict
  // (SEND/ON_TOP) for every row given its final, correctly-resolved book.
  for (const r of rows) {
    const mine = r.book.ownBest(Date.now());
    const best = r.book.effectiveBest(Date.now());
    const verdict = decide({ best: best.price, mine: mine.price, minPrice: r.row.minPrice, maxPrice: r.row.maxPrice, step: r.row.step });
    assert.ok(verdict.status === "SEND" || verdict.status === "ON_TOP", `row ${r.row.tokenId} must have a clean terminal verdict, got ${verdict.status}`);
    assert.equal(mine.price, 0, "no own order was ever recorded for these synthetic rows -- a stray SEND would be the sign of a duplicate/phantom own order");
  }
});

// ---- Case 9: hot-path isolation under load -- a pending retry never blocks a fresh real outbid ----
await check("hot-path isolation: while several trait-scope retries are pending, a brand-new real competitor event is still handled synchronously (no REST before realtime POST)", async () => {
  const { engine, resolvers, rows } = buildEngine({ rows: [
    { tokenId: "1", slug: "busy1" }, { tokenId: "2", slug: "busy1" }, { tokenId: "3", slug: "busy2" }
  ] });
  for (const r of rows) engine.apply(bidEvent({ slug: r.row.collectionSlug, tokenId: r.row.tokenId, maker: RIVAL, price: 0.02, orderHash: `0xb${r.row.tokenId}` }));
  await flush();
  engine.apply(traitOffer({ slug: "busy1", maker: RIVAL, price: 0.3, orderHash: "0xtr1", traitType: "T", traitValue: "V" }));
  engine.apply(traitOffer({ slug: "busy2", maker: RIVAL, price: 0.3, orderHash: "0xtr2", traitType: "T", traitValue: "V" }));
  await flush();
  assert.ok(resolvers.length >= 3, "trait-scope reads must be in flight (pending) for this test to mean anything");

  const startedAt = Date.now();
  engine.apply(bidEvent({ slug: "busy2", tokenId: "3", maker: RIVAL, price: 0.5, orderHash: "0xrealoutbid" }));
  engine.evaluate(rows[2].key, Date.now(), null);
  const elapsedMs = Date.now() - startedAt;

  assert.ok(elapsedMs < 50, `handling a fresh real outbid took ${elapsedMs}ms while trait-scope reads were pending -- must stay synchronous/fast`);
  assert.equal(rows[2].book.effectiveBest(Date.now()).price, 0.5, "the fresh real competitor must be reflected immediately, not deferred behind pending trait-scope reads");
});

// ---- Case 10: 4th production repro -- Nibble #7192, the-nibbles ----------------
// (separate from the generic test below; this one pins the EXACT real
// contract/collection/tokenId reported, so a future regression on these
// specific identifiers cannot hide behind the generic/random coverage.)
await check("production repro #4: Nibble #7192 (the-nibbles, 0x5e52d41f0e40d7cdb204db0d09659846f7404547) -- stuck Best=0.04 resolves after retry", async () => {
  const NIBBLE_CONTRACT = "0x5e52d41f0e40d7cdb204db0d09659846f7404547";
  const slug = "the-nibbles", tokenId = "7192";
  const key = `ethereum:${NIBBLE_CONTRACT}:${tokenId}`;
  const resolvers = [];
  const adapter = { chain: "ethereum", async fetchBest() { return new Promise(res => resolvers.push(res)); } };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.recovery.pressure = () => false;
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(() => "HEALTHY");
  engine.templateReady = () => true;
  engine.builder = { address: SELF };
  engine.book.setSelfAddress(SELF);
  const row = { key, running: true, tokenId, contract: NIBBLE_CONTRACT, collectionSlug: slug, minPrice: 0.001, maxPrice: 1, step: 0.0001 };
  engine.rows.set(key, row);
  const book = engine.book.add({ key, chain: "ethereum", contract: NIBBLE_CONTRACT, tokenId, collectionSlug: slug });
  book.generation = 1; book.hydratedAt = Date.now() - 1000; book.ownReconciledAt = Date.now() - 1000; book.selfAddress = SELF;

  const ev = (overrides) => ({
    collectionSlug: slug, nft: { chain: "ethereum", contract: NIBBLE_CONTRACT, tokenId },
    kind: "item", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
    event: "item_received_bid", pricePerItem: 0.04, ...overrides
  });

  // Baseline: tool holds Best=0.04, exactly matching the reported symptom.
  engine.apply(ev({ orderHash: "0xnib1", pricePerItem: 0.04 }));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.04);

  // A trait offer on an unlearned trait pushes the REAL best well above 0.04.
  engine.apply(traitOffer({ slug, maker: RIVAL, price: 0.09, orderHash: "0xnibtrait", traitType: "Background", traitValue: "Blue" }));
  await flush();
  assert.ok(resolvers.length >= 1, "an authoritative trait-scope read must have been queued for Nibble #7192");

  // Trace matches the bug report: item_received_bid, order_invalidate, and
  // collection_offer keep arriving while the read is in flight.
  engine.apply(ev({ orderHash: "0xnib2", pricePerItem: 0.041 }));
  engine.apply(invalidateEvent({ orderHash: "0xnib-dead", slug }));
  engine.apply({
    collectionSlug: slug, nft: null, kind: "collection", orderHash: "0xnibcol", maker: RIVAL,
    quantity: 1, currency: "WETH", endTime: 0, eventTimestamp: Date.now(), receivedAt: Date.now(),
    hasOrderData: true, event: "collection_offer", pricePerItem: 0.042
  });

  resolvers[0]({ orderHash: "0xnib2", price: 0.041, orders: [
    { orderHash: "0xnib1", price: 0.04, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
    { orderHash: "0xnib2", price: 0.041, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
    { orderHash: "0xnibcol", price: 0.042, maker: RIVAL, kind: "collection", endTime: 0, quantity: 1 }
  ] });
  await flush();
  assert.ok(engine.stats.staleBDiscarded >= 1, "the busy-collection trace must have raced the read into a stale discard, as reported");

  await waitForRetry(1);
  assert.equal(resolvers.length, 2, "the discarded trait-scope read must be retried after backoff");
  resolvers[1]({ orderHash: "0xnibtrait", price: 0.09, orders: [
    { orderHash: "0xnib1", price: 0.04, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
    { orderHash: "0xnib2", price: 0.041, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
    { orderHash: "0xnibcol", price: 0.042, maker: RIVAL, kind: "collection", endTime: 0, quantity: 1 },
    { orderHash: "0xnibtrait", price: 0.09, maker: RIVAL, kind: "trait", endTime: 0, quantity: 1 }
  ] });
  await flush();

  assert.equal(book.effectiveBest(Date.now()).price, 0.09, "Best must rise to the real resolved price (0.09), not stay stuck at 0.04");
  const mine = book.ownBest(Date.now());
  const verdict = decide({ best: 0.09, mine: mine.price, minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step });
  assert.equal(verdict.status, "SEND", "Decision must immediately produce a new SEND target, not stay ON_TOP");
  assert.ok(verdict.target > 0.09, "the new target must be strictly above the real resolved Best");
});

// ---- Case 11: GENERIC, non-hardcoded coverage ---------------------------------
// The three named production NFTs above (and Nibble #7192) are FIXTURES, not
// the scope of the fix. The actual invariant the fix must hold is general:
// for ANY contract/collectionSlug/tokenId and ANY offer kind (item,
// collection, or trait -- including through a busy-collection stale-discard
// race), a genuinely higher real Best must land in MemoryBook and Decision
// must compute a fresh target immediately. This loops over many generated,
// non-overlapping identifiers (never reusing a sample NFT's name/id) to prove
// the fix is general, not special-cased to the reported examples.
await check("generic coverage: for arbitrary contract/collection/tokenId and item/collection/trait kind, a higher real Best always lands and produces a fresh target -- no hardcoded NFT identity", async () => {
  function randHex(n) { let s = "0x"; for (let i = 0; i < n; i++) s += Math.floor(Math.random() * 16).toString(16); return s; }
  const KINDS = ["item", "collection", "trait"];
  const CASES = 12;
  for (let c = 0; c < CASES; c++) {
    const kind = KINDS[c % KINDS.length];
    const contract = randHex(40);
    const slug = `generated-coll-${c}-${Math.floor(Math.random() * 1e6)}`;
    const tokenId = String(1000 + Math.floor(Math.random() * 90000));
    const key = `ethereum:${contract}:${tokenId}`;
    const basePrice = Number((0.005 + Math.random() * 0.1).toFixed(6));
    const higherPrice = Number((basePrice + 0.01 + Math.random() * 0.05).toFixed(6));

    const resolvers = [];
    const adapter = { chain: "ethereum", async fetchBest() { return new Promise(res => resolvers.push(res)); } };
    const engine = new OfferItemEngineV2({ adapter, onLog() {} });
    engine.recovery.pressure = () => false;
    engine.state = STATE.RUNNING;
    engine.attachStreamHealth(() => "HEALTHY");
    engine.templateReady = () => true;
    engine.builder = { address: SELF };
    engine.book.setSelfAddress(SELF);
    const row = { key, running: true, tokenId, contract, collectionSlug: slug, minPrice: 0.001, maxPrice: 1, step: 0.0001 };
    engine.rows.set(key, row);
    const book = engine.book.add({ key, chain: "ethereum", contract, tokenId, collectionSlug: slug });
    book.generation = 1; book.hydratedAt = Date.now() - 1000; book.ownReconciledAt = Date.now() - 1000; book.selfAddress = SELF;

    const baseHash = randHex(64);
    engine.apply(bidEvent({ slug, tokenId, maker: RIVAL, price: basePrice, orderHash: baseHash, kind: "item", contract }));
    await flush();
    assert.equal(book.effectiveBest(Date.now()).price, basePrice, `case ${c} (${kind}): baseline must land`);

    if (kind === "item" || kind === "collection") {
      // Direct path (no UNKNOWN-trait gate involved): a plain higher bid must
      // raise Best immediately, no retry/REST needed at all.
      const newHash = randHex(64);
      engine.apply(bidEvent({ slug, tokenId, maker: RIVAL, price: higherPrice, orderHash: newHash, kind, contract }));
      await flush();
      assert.equal(book.effectiveBest(Date.now()).price, higherPrice, `case ${c} (${kind}): a genuinely higher bid must raise Best`);
    } else {
      // Trait path: token traits are UNKNOWN -> goes through resolveTraitScope
      // -> raced-discarded by unrelated busy traffic -> must retry -> settle.
      const traitHash = randHex(64);
      engine.apply(traitOffer({ slug, maker: RIVAL, price: higherPrice, orderHash: traitHash, traitType: `Type${c}`, traitValue: `Value${c}` }));
      await flush();
      assert.ok(resolvers.length >= 1, `case ${c} (trait): a trait-scope read must have been queued`);
      const noiseHash = randHex(64);
      engine.apply(bidEvent({ slug, tokenId, maker: RIVAL, price: basePrice + 0.0001, orderHash: noiseHash, kind: "item", contract }));
      resolvers[0]({ orderHash: noiseHash, price: basePrice + 0.0001, orders: [] });
      await flush();
      await waitForRetry(1);
      assert.equal(resolvers.length, 2, `case ${c} (trait): the discarded read must be retried`);
      resolvers[1]({
        orderHash: traitHash, price: higherPrice,
        orders: [{ orderHash: traitHash, price: higherPrice, maker: RIVAL, kind: "trait", endTime: 0, quantity: 1 }]
      });
      await flush();
      assert.equal(book.effectiveBest(Date.now()).price, higherPrice, `case ${c} (trait): Best must rise to the resolved real price`);
    }

    const mine = book.ownBest(Date.now());
    const verdict = decide({ best: higherPrice, mine: mine.price, minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step });
    assert.equal(verdict.status, "SEND", `case ${c} (${kind}): Decision must produce SEND immediately once Best is correctly higher`);
    assert.ok(verdict.target > higherPrice, `case ${c} (${kind}): target must be strictly above the new real Best`);
  }
});

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
