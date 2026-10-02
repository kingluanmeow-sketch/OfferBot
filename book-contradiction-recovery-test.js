"use strict";

/**
 * RED/GREEN: "Book contradiction" -- một UPSERT bị `apply()` từ chối là
 * "stale" trong khi giá CỦA CHÍNH op đó CAO HƠN effectiveBest nội bộ hiện
 * tại (audit "shadow": 5/6 NFT có effectiveBest nội bộ thấp hơn thật so với
 * OpenSea `/offers/collection/{slug}/nfts/{id}/best`). Kèm fix thứ hai cho
 * yêu cầu #4: authority snapshot REST mới merge vào sổ không được bị một
 * Stream replay cũ của ĐÚNG hash đó ghi đè ngay sau.
 *
 * KHÔNG dùng "Decision=ON_TOP và best<=mine" làm bằng chứng không miss --
 * bằng chứng ở đây là structural: diagnoseApply() trả "stale" HAY một read
 * đã merge rồi bị ghi đè, bất kể giá trị cuối cùng trông "hợp lý" ra sao.
 *
 * CHỨNG MINH / GIẢ THUYẾT (phân biệt rõ):
 *   - CHỨNG MINH (đọc code): mergeBest()'s settle() KHÔNG stamp `book.seen`
 *     khi merge order REST vào group -- một replay Stream cũ của ĐÚNG hash
 *     đó có thể ghi đè ngay sau vì `isStale()` không có gì để so.
 *   - CHỨNG MINH (đọc code): `reason:"stale"` của `apply()` không phân biệt
 *     "feed B lặp lại CÙNG giá cũ y của feed A" (benign) với "một op có giá
 *     CAO HƠN sổ đang giữ, nhưng bị coi là cũ vì seq nội bộ nói vậy" --
 *     trường hợp sau là mâu thuẫn thật, không phải lặp lại vô hại.
 *   - GIẢ THUYẾT (không chứng minh ở đây): OpenSea Stream event out-of-order
 *     là nguyên nhân GỐC của mọi 5/6 case trong audit shadow -- cần fixture
 *     production thật (order hash + cả 2 feed) để xác nhận TỪNG case, việc
 *     đó KHÔNG làm trong phạm vi RED test tổng hợp này.
 */

const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}
const flush = async (n = 3) => { for (let i = 0; i < n; i++) await new Promise(r => setImmediate(r)); };

const SELF = "0x4444444444444444444444444444444444444444";
const RIVAL = "0x3333333333333333333333333333333333333333";
const CONTRACT = "0x1111111111111111111111111111111111111111";

function bidEvent({ slug, tokenId, maker, price, orderHash, kind = "item", eventTimestamp, contract = CONTRACT }) {
  const at = Date.now();
  return {
    collectionSlug: slug, nft: { chain: "ethereum", contract, tokenId },
    kind, orderHash, maker, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: eventTimestamp || at, receivedAt: at, hasOrderData: true,
    event: kind === "collection" ? "collection_offer" : "item_received_bid", pricePerItem: price
  };
}

function buildEngine({ rows = [{ tokenId: "1", slug: "test-collection" }] } = {}) {
  const resolvers = [];
  const adapter = { chain: "ethereum", async fetchBest() { return new Promise(res => resolvers.push(res)); } };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.recovery.pressure = () => false;
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(() => "HEALTHY");
  engine.templateReady = () => true;
  engine.builder = { address: SELF };
  engine.book.setSelfAddress(SELF);
  const built = [];
  for (const r of rows) {
    const key = `ethereum:${r.contract || CONTRACT}:${r.tokenId}`;
    const row = { key, running: true, tokenId: r.tokenId, contract: r.contract || CONTRACT, collectionSlug: r.slug, minPrice: 0.001, maxPrice: r.maxPrice || 1, step: r.step || 0.0001 };
    engine.rows.set(key, row);
    const book = engine.book.add({ key, chain: "ethereum", contract: r.contract || CONTRACT, tokenId: r.tokenId, collectionSlug: r.slug });
    book.generation = 1;
    book.hydratedAt = Date.now() - 1000;
    book.ownReconciledAt = Date.now() - 1000;
    book.selfAddress = SELF;
    built.push({ key, row, book });
  }
  return { engine, resolvers, rows: built, key: built[0].key, row: built[0].row, book: built[0].book };
}

(async () => {

// ---- Case 1: a higher-priced op rejected as "stale" triggers a targeted background recovery read ----
await check("book contradiction: an op rejected as 'stale' but priced ABOVE the current effectiveBest triggers exactly one targeted /best read", async () => {
  const { engine, book, resolvers } = buildEngine({ rows: [{ tokenId: "1", slug: "coll" }] });
  const t0 = Date.now();
  engine.apply(bidEvent({ slug: "coll", tokenId: "1", maker: RIVAL, price: 0.02, orderHash: "0xh1", eventTimestamp: t0 + 2000 }));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.02);
  // An op for the SAME hash arrives with an OLDER eventTimestamp but a HIGHER
  // price (the sofar-applied 0.02 is wrong per this later-delivered frame) --
  // apply() rejects it as "stale" per internal seq, but the price it carries
  // contradicts what the book is holding.
  engine.apply(bidEvent({ slug: "coll", tokenId: "1", maker: RIVAL, price: 0.05, orderHash: "0xh1", eventTimestamp: t0 + 1000 }));
  await flush();
  assert.ok(resolvers.length >= 1, "a book-contradiction recovery read must have been queued");
  resolvers[0]({ orderHash: "0xh1", price: 0.05, orders: [{ orderHash: "0xh1", price: 0.05, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 }] });
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.05, "the authoritative read must resolve the contradiction toward the real (higher) price");
});

// ---- Case 2: a benign stale replay (SAME price, just reordered) must NOT trigger recovery ----
await check("no contradiction: a stale replay carrying the SAME (not higher) price does not trigger a recovery read -- not a real contradiction", async () => {
  const { engine, book, resolvers } = buildEngine({ rows: [{ tokenId: "2", slug: "coll" }] });
  const t0 = Date.now();
  engine.apply(bidEvent({ slug: "coll", tokenId: "2", maker: RIVAL, price: 0.02, orderHash: "0xh2", eventTimestamp: t0 + 2000 }));
  await flush();
  engine.apply(bidEvent({ slug: "coll", tokenId: "2", maker: RIVAL, price: 0.02, orderHash: "0xh2", eventTimestamp: t0 + 1000 })); // same price, just older/duplicate
  await flush();
  assert.equal(resolvers.length, 0, "a same-price stale replay is benign (duplicate), not a contradiction -- no read triggered");
});

// ---- Case 3: bounded -- repeated contradictions for the SAME key within the cooldown only probe once ----
await check("bounded: repeated book-contradiction signals for the same NFT within the cooldown window trigger only ONE recovery read", async () => {
  const { engine, book, resolvers } = buildEngine({ rows: [{ tokenId: "3", slug: "coll" }] });
  const t0 = Date.now();
  engine.apply(bidEvent({ slug: "coll", tokenId: "3", maker: RIVAL, price: 0.02, orderHash: "0xh3", eventTimestamp: t0 + 5000 }));
  await flush();
  // Two more out-of-order (older eventTimestamp, SAME orderHash -- the only
  // shape `isStale()` actually scopes "stale" to) higher-priced frames for
  // this SAME NFT, both within TRAIT_PROBE_COOLDOWN_MS of each other.
  engine.apply(bidEvent({ slug: "coll", tokenId: "3", maker: RIVAL, price: 0.05, orderHash: "0xh3", eventTimestamp: t0 + 1000 }));
  await flush();
  engine.apply(bidEvent({ slug: "coll", tokenId: "3", maker: RIVAL, price: 0.06, orderHash: "0xh3", eventTimestamp: t0 + 2000 }));
  await flush();
  assert.equal(resolvers.length, 1, "exactly one recovery read within the cooldown window, not a storm");
});

// ---- Case 4: authority snapshot (REST merge) must not be overwritten by a late Stream replay of the SAME hash ----
await check("authority snapshot safety: a REST merge for orderHash H is not clobbered by a subsequently-arriving OLDER Stream frame for the SAME H", async () => {
  const { engine, book, row, resolvers } = buildEngine({ rows: [{ tokenId: "4", slug: "coll" }] });
  engine.apply(bidEvent({ slug: "coll", tokenId: "4", maker: RIVAL, price: 0.02, orderHash: "0xbase4" }));
  await flush();
  // Trigger the authoritative read via the EXISTING, independent trait-scope
  // path (not the new contradiction trigger) so this case isolates ONLY the
  // authority-snapshot-safety fix (book.seen stamped on merge).
  engine.apply({
    collectionSlug: "coll", nft: null, kind: "trait", orderHash: "0xtrig4", maker: RIVAL,
    quantity: 1, currency: "WETH", endTime: 0, eventTimestamp: Date.now(), receivedAt: Date.now(),
    hasOrderData: true, event: "trait_offer", pricePerItem: 0.08,
    traitCriteria: { trait_type: "T", trait_name: "V" },
    traitCriteriaList: { trait_criteria_list: [{ trait_type: "T", trait_name: "V" }] }
  });
  await flush();
  assert.ok(resolvers.length >= 1);
  // The authoritative REST read confirms 0xauth4 at 0.1 -- a real, higher price.
  resolvers[0]({ orderHash: "0xauth4", price: 0.1, orders: [
    { orderHash: "0xbase4", price: 0.02, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 },
    { orderHash: "0xauth4", price: 0.1, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 }
  ] });
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.1, "the authority merge must land at the real confirmed price");

  // NOW a late Stream replay for the EXACT SAME hash (0xauth4) arrives, carrying
  // OLDER/wrong data (e.g. a duplicate dual-feed frame delayed in transit).
  engine.apply(bidEvent({ slug: "coll", tokenId: "4", maker: RIVAL, price: 0.03, orderHash: "0xauth4", eventTimestamp: Date.now() - 120000 }));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.1, "the authoritative merge must survive a late, older Stream replay of the SAME orderHash -- not silently overwritten");
});

// ---- Case 5: hot-path isolation -- the contradiction-triggered read never blocks apply()/evaluate() ----
await check("hot-path: triggering a book-contradiction recovery read does not slow down apply()/evaluate() for this or any other NFT", async () => {
  const { engine, rows } = buildEngine({ rows: [{ tokenId: "5a", slug: "coll" }, { tokenId: "5b", slug: "coll" }] });
  const t0 = Date.now();
  engine.apply(bidEvent({ slug: "coll", tokenId: "5a", maker: RIVAL, price: 0.02, orderHash: "0xh5", eventTimestamp: t0 + 2000 }));
  await flush();
  const startedAt = Date.now();
  engine.apply(bidEvent({ slug: "coll", tokenId: "5a", maker: RIVAL, price: 0.09, orderHash: "0xh5", eventTimestamp: t0 + 1000 }));
  engine.apply(bidEvent({ slug: "coll", tokenId: "5b", maker: RIVAL, price: 0.3, orderHash: "0xh5b" }));
  const elapsedMs = Date.now() - startedAt;
  assert.ok(elapsedMs < 50, `triggering the recovery read took ${elapsedMs}ms alongside -- must stay fast`);
  assert.equal(rows[1].book.effectiveBest(Date.now()).price, 0.3, "an unrelated NFT's real event must be unaffected");
});

// ---- Case 6: MANDATORY production repro -- LT3 #11 (lessthanthree, tokenId 11) ----
await check("production repro: LT3 #11 (lessthanthree) -- a higher-priced frame rejected as stale triggers recovery toward the real OpenSea /best price", async () => {
  const { engine, book, resolvers } = buildEngine({ rows: [{ tokenId: "11", slug: "lessthanthree" }] });
  const t0 = Date.now();
  engine.apply(bidEvent({ slug: "lessthanthree", tokenId: "11", maker: RIVAL, price: 0.015, orderHash: "0xlt3hash", eventTimestamp: t0 + 3000 }));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.015, "baseline: tool holds the known price, matching the 'internal Best lower than OpenSea /best' symptom pattern");
  // An out-of-order frame for the SAME orderHash (the only shape `isStale()`
  // scopes "stale" to) carries a higher real price -- rejected locally as
  // stale per internal seq, but the price it carries contradicts the book.
  engine.apply(bidEvent({ slug: "lessthanthree", tokenId: "11", maker: RIVAL, price: 0.021, orderHash: "0xlt3hash", eventTimestamp: t0 + 500 }));
  await flush();
  assert.ok(resolvers.length >= 1, "LT3 #11 must get a targeted recovery read when a higher real price is rejected as stale");
  resolvers[0]({ orderHash: "0xlt3hash", price: 0.021, orders: [
    { orderHash: "0xlt3hash", price: 0.021, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 }
  ] });
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.021, "LT3 #11 must resolve to the real, higher, OpenSea-confirmed price");
});

// ---- Case 7: GENERIC, non-hardcoded sweep across arbitrary contracts/collections/tokenIds ----
await check("generic coverage: book-contradiction recovery triggers and resolves correctly for arbitrary (non-named) NFTs", async () => {
  function randHex(n) { let s = "0x"; for (let i = 0; i < n; i++) s += Math.floor(Math.random() * 16).toString(16); return s; }
  for (let c = 0; c < 6; c++) {
    const contract = randHex(40);
    const slug = `generated-${c}-${Math.floor(Math.random() * 1e6)}`;
    const tokenId = String(500 + Math.floor(Math.random() * 90000));
    const { engine, book, resolvers } = buildEngine({ rows: [{ tokenId, slug, contract }] });
    const t0 = Date.now();
    const hash = randHex(64); // SAME hash for both frames -- the only shape isStale() scopes "stale" to
    const basePrice = 0.01 + Math.random() * 0.05;
    const realPrice = basePrice + 0.02 + Math.random() * 0.05;
    engine.apply(bidEvent({ slug, tokenId, maker: RIVAL, price: basePrice, orderHash: hash, eventTimestamp: t0 + 3000, contract }));
    await flush();
    engine.apply(bidEvent({ slug, tokenId, maker: RIVAL, price: realPrice, orderHash: hash, eventTimestamp: t0 + 500, contract }));
    await flush();
    assert.ok(resolvers.length >= 1, `case ${c}: must trigger a recovery read`);
    resolvers[0]({ orderHash: hash, price: realPrice, orders: [
      { orderHash: hash, price: realPrice, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 }
    ] });
    await flush();
    assert.ok(Math.abs(book.effectiveBest(Date.now()).price - realPrice) < 1e-9, `case ${c}: must resolve to the real price`);
  }
});

// ---- Case 8 (audit #1): a genuinely NEW real event arriving DURING a REST merge's round-trip
// must not be rejected as stale by a watermark stamped at merge-completion time ----
await check("audit fix #1: a real Stream event for the SAME orderHash, with seq between read-start and merge-completion, is NOT wrongly treated as stale", async () => {
  const { engine, book, row, resolvers } = buildEngine({ rows: [{ tokenId: "8", slug: "coll" }] });
  // Force an authoritative read via the trait-scope path (independent trigger).
  engine.apply({
    collectionSlug: "coll", nft: null, kind: "trait", orderHash: "0xtrig8", maker: RIVAL,
    quantity: 1, currency: "WETH", endTime: 0, eventTimestamp: Date.now(), receivedAt: Date.now(),
    hasOrderData: true, event: "trait_offer", pricePerItem: 0.05,
    traitCriteria: { trait_type: "T", trait_name: "V" },
    traitCriteriaList: { trait_criteria_list: [{ trait_type: "T", trait_name: "V" }] }
  });
  await flush();
  assert.ok(resolvers.length >= 1, "a read must be in flight");
  const readStartedAt = Date.now(); // snapshotStartedAt is stamped inside queueRead's run(), ~now

  // The round-trip takes a little while in real production. Resolve it now,
  // confirming a hash H at price P1 -- this merge completes STRICTLY LATER
  // than readStartedAt (the bug used THIS later "now" as the watermark).
  await new Promise(r => setTimeout(r, 15));
  resolvers[0]({ orderHash: "0xH8", price: 0.1, orders: [
    { orderHash: "0xH8", price: 0.1, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 }
  ] });
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.1, "the authoritative merge must land first");

  // NOW a real, NEW Stream frame for the EXACT SAME hash arrives, carrying a
  // DIFFERENT (higher) price, with an eventTimestamp that falls BETWEEN
  // readStartedAt and the merge-completion "now" -- i.e. it genuinely
  // happened/was received during the read's round-trip, not before it.
  engine.apply(bidEvent({ slug: "coll", tokenId: "8", maker: RIVAL, price: 0.11, orderHash: "0xH8", eventTimestamp: readStartedAt + 5 }));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.11,
    "a genuine new event arriving during the merge's round-trip must be accepted, not rejected as stale against a merge-completion-time watermark");
});

// ---- Case 9 (audit #2): a book-contradiction arriving while the shared cooldown is busy
// must be coalesced into exactly one latest-wins reread, never silently dropped ----
await check("audit fix #2: a book-contradiction signal arriving during the shared traitProbedAt cooldown is coalesced, not dropped", async () => {
  const { engine, book, row, resolvers } = buildEngine({ rows: [{ tokenId: "9", slug: "coll" }] });
  const t0 = Date.now();
  engine.apply(bidEvent({ slug: "coll", tokenId: "9", maker: RIVAL, price: 0.02, orderHash: "0xh9", eventTimestamp: t0 + 5000 }));
  await flush();
  // Simulate the shared cooldown being hot (an unrelated trait-scope probe
  // for this exact NFT fired moments ago).
  engine.traitProbedAt.set(row.key, Date.now());
  assert.equal(resolvers.length, 0, "sanity: no read in flight yet");

  // A real book-contradiction signal arrives WHILE that cooldown is still hot.
  engine.apply(bidEvent({ slug: "coll", tokenId: "9", maker: RIVAL, price: 0.05, orderHash: "0xh9", eventTimestamp: t0 + 1000 }));
  await flush();
  assert.equal(resolvers.length, 0, "must NOT fire immediately while the shared cooldown is hot");
  assert.ok(engine.pendingContradiction.has(row.key), "the contradiction must be coalesced (remembered), not silently dropped");

  // The in-progress trait-scope cycle for this key eventually settles --
  // the coalesced contradiction must fire exactly one latest-wins reread then.
  engine.settleTraitScopeRetry(row.key);
  await flush();
  assert.equal(resolvers.length, 1, "exactly one reread must fire once the current cycle settles");
  assert.ok(!engine.pendingContradiction.has(row.key), "the pending marker must be consumed, not left behind (bounded, finite lifecycle)");

  resolvers[0]({ orderHash: "0xh9", price: 0.05, orders: [
    { orderHash: "0xh9", price: 0.05, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 }
  ] });
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.05, "the coalesced reread must still resolve the real contradiction correctly");
});

// ---- Case 10: bounded -- giving up (max attempts) still consumes any coalesced contradiction once ----
await check("bounded: giving up on retries still consumes a coalesced contradiction exactly once (no permanent loss, no leftover marker)", async () => {
  const { engine, book, row } = buildEngine({ rows: [{ tokenId: "10", slug: "coll" }] });
  engine.pendingContradiction.add(row.key);
  // Simulate a trait-scope retry chain that has exhausted its attempt cap.
  engine.traitScopeRetry.set(row.key, { attempts: 999, firstAt: Date.now(), timer: null });
  const before = engine.stats.bookContradictionProbes || 0;
  engine.scheduleTraitScopeRetry(row.key);
  assert.ok(!engine.pendingContradiction.has(row.key), "give-up must still consume the pending marker, not leave it behind forever");
  assert.ok((engine.stats.bookContradictionProbes || 0) > before, "give-up must still attempt one coalesced reread rather than losing the evidence entirely");
});

// ---- Case 11: SCALE -- 146+ NFTs across many busy collections, some contradicting concurrently.
// Must stay per-NFT single-flight/bounded, never REST-poll untouched NFTs, and P0 (apply/evaluate
// for the hot Stream->Decision->POST path) must not slow down while P2 recovery reads are in flight. ----
await check("scale: 146+ NFTs across busy collections, concurrent contradictions -- bounded recovery, P0 latency unaffected", async () => {
  const TOTAL = 150;
  const rowsSpec = [];
  for (let i = 0; i < TOTAL; i++) {
    rowsSpec.push({ tokenId: String(10000 + i), slug: `busy-coll-${i % 8}` });
  }
  const { engine, rows, resolvers } = buildEngine({ rows: rowsSpec });
  const t0 = Date.now();

  // Seed every NFT with a baseline, then contradict a THIRD of them (50) at
  // once -- a realistic "busy collection" burst, not an isolated single-NFT
  // case like Case 1/5. Each uses its OWN distinct hash (per-NFT single-flight
  // must not conflate them into one shared queue slot).
  for (const r of rows) {
    engine.apply(bidEvent({ slug: r.row.collectionSlug, tokenId: r.row.tokenId, maker: RIVAL, price: 0.02, orderHash: `0xbase-${r.row.tokenId}`, eventTimestamp: t0 + 2000 }));
  }
  await flush();

  const contradicted = rows.filter((_, i) => i % 3 === 0); // 50 of 150
  for (const r of contradicted) {
    engine.apply(bidEvent({ slug: r.row.collectionSlug, tokenId: r.row.tokenId, maker: RIVAL, price: 0.09, orderHash: `0xbase-${r.row.tokenId}`, eventTimestamp: t0 + 500 }));
  }
  await flush();

  // Bounded: this goes through the SAME shared RecoveryPlane as every other
  // background read (workers=6, readsPerSecond=4 token bucket -- existing,
  // frozen, working design, not something this patch touches). A burst of
  // 50 contradictions must NOT storm 50 concurrent reads; the plane admits
  // at most `workers` at once and queues the rest -- but must queue them
  // (bounded backlog), never silently drop them.
  const census = engine.recovery.census();
  assert.ok(resolvers.length >= 1 && resolvers.length <= engine.recovery.workers,
    `expected the recovery plane to admit a bounded number (<= ${engine.recovery.workers}) of the 50 contradictions at once, got ${resolvers.length} dispatched`);
  assert.equal(census.dropped, 0, "no contradiction evidence must be silently dropped -- the rest must be queued, not lost");
  assert.equal(census.queued + resolvers.length, contradicted.length,
    `every contradicted NFT must be accounted for: either dispatched now (${resolvers.length}) or safely queued (${census.queued}), total must equal ${contradicted.length}`);

  // P0 hot path: a real, unrelated Stream event for an UNTOUCHED NFT, measured
  // while dozens of P2 recovery reads are queued/in flight, must still apply fast.
  const untouched = rows[1]; // index 1 is never in the %3===0 contradicted set
  const p0Start = Date.now();
  engine.apply(bidEvent({ slug: untouched.row.collectionSlug, tokenId: untouched.row.tokenId, maker: RIVAL, price: 0.5, orderHash: `0xp0-${untouched.row.tokenId}` }));
  const p0ElapsedMs = Date.now() - p0Start;
  assert.ok(p0ElapsedMs < 50, `P0 apply() for an unrelated NFT took ${p0ElapsedMs}ms with dozens of P2 recovery reads outstanding -- must stay fast`);
  assert.equal(untouched.book.effectiveBest(Date.now()).price, 0.5, "the unrelated NFT's real event must still apply correctly");

  // Resolve whatever was actually dispatched now and confirm each settles to
  // its own real price with no cross-NFT bleed (per-NFT single-flight holds
  // at scale, not just in the 1-2-NFT cases above).
  const dispatchedCount = resolvers.length;
  for (let i = 0; i < dispatchedCount; i++) {
    const r = contradicted[i];
    resolvers[i]({ orderHash: `0xbase-${r.row.tokenId}`, price: 0.09, orders: [
      { orderHash: `0xbase-${r.row.tokenId}`, price: 0.09, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 }
    ] });
  }
  await flush();
  for (let i = 0; i < dispatchedCount; i++) {
    const r = contradicted[i];
    assert.equal(r.book.effectiveBest(Date.now()).price, 0.09, `NFT #${r.row.tokenId} must resolve to its own real price, not bleed from another NFT's recovery`);
  }
});

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
