"use strict";

/**
 * RED/GREEN: "mapped nhưng không áp" không để lại dấu vết nào (audit mở rộng).
 *
 * TRIỆU CHỨNG THẬT (production trace, process v1.25.61 đang chạy)
 *   `stream_rx` ghi "mapped" cho `collection_offer`/`trait_offer`/
 *   `order_invalidate` ở Alien Fren #6456 liên tục trong nhiều phút, nhưng
 *   KHÔNG MỘT `book_update` nào theo sau cho token đó -- và không hề có
 *   dòng nào nói TẠI SAO `apply()` từ chối. Cùng triệu chứng tại Parallel
 *   Avatars Jazmine #10184, Heracles #8375, Nibble #7192.
 *
 * KHÔNG dùng "Decision=ON_TOP và best nội bộ <= mine" để chứng minh "không
 * miss" -- đó là kiểm vòng tròn khi CHÍNH best nội bộ đang stale (best nội
 * bộ không đổi KHÔNG chứng minh gì nếu ta không biết vì sao nó không đổi).
 * Bằng chứng chính ở đây là structural: `touched.notApplied` phải tồn tại
 * và mang đúng lý do, bất kể giá trị cuối cùng của best là gì.
 *
 * ROOT CAUSE (engine-v2.js)
 *   `if (touched.length) productionTrace.record("book_update", ...)` --
 *   nhánh "not-applied" chưa bao giờ THẬT SỰ ghi, dù code tính sẵn đúng
 *   status cho nó (dead branch từ 1.25.40).
 *
 * FIX
 *   `memory-book.js`: `diagnoseApply()` (đọc-chỉ, không mutate) phân loại
 *   ĐÚNG lý do `apply()` từ chối (stale/version-stale/tombstoned/
 *   trait-no-match/trait-unknown/expired/duplicate/no-op), gắn vào
 *   `touched.notApplied` ở cả 3 nhánh của `MemoryBook.apply()` (item trực
 *   tiếp, REMOVE theo collection, fan-out collection/trait).
 *   `engine-v2.js`: ghi một `book_update` "not-applied" CHO MỖI NFT đang
 *   theo dõi bị op đó chạm tới và bị từ chối -- trừ "duplicate"/"no-op"
 *   (cố ý bỏ, quá phổ biến/vô hại, tránh bão I/O đúng kiểu 1.25.40 từng đo).
 *   `production-trace.js`: mở rộng REASONS allowlist + field price/kind.
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
function invalidateEvent({ orderHash, slug, tokenId, contract }) {
  const at = Date.now();
  return {
    collectionSlug: slug, nft: tokenId ? { chain: "ethereum", contract: contract || CONTRACT, tokenId } : null,
    orderHash, eventTimestamp: at, receivedAt: at, event: "order_invalidate"
  };
}

function buildEngine({ rows = [{ tokenId: "1", slug: "test-collection" }] } = {}) {
  const adapter = { chain: "ethereum", async fetchBest() { return new Promise(() => {}); } };
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
  return { engine, rows: built, key: built[0].key, row: built[0].row, book: built[0].book };
}

/** Capture every productionTrace.record() call for the duration of fn(). */
async function captureTrace(fn) {
  const calls = [];
  const orig = productionTrace.record;
  productionTrace.record = (stage, event, detail) => { calls.push({ stage, detail }); return orig.call(productionTrace, stage, event, detail); };
  try { await fn(); } finally { productionTrace.record = orig; }
  return calls;
}

(async () => {

// ---- Case 1: trait-unknown (new/cold NFT, traits never learned) must be diagnosed ----
await check("trait-unknown: a collection-wide trait offer rejected because this token's traits are unlearned leaves a classified trace, not silence", async () => {
  const { engine, book, row } = buildEngine({ rows: [{ tokenId: "6456", slug: "alienfrensnft" }] });
  const calls = await captureTrace(async () => {
    engine.apply(traitOffer({ slug: "alienfrensnft", maker: RIVAL, price: 0.02, orderHash: "0xaa1", traitType: "Background", traitValue: "Blue" }));
    await flush();
  });
  const notApplied = calls.filter(c => c.stage === "book_update" && c.detail.status === "not-applied" && c.detail.tokenId === "6456");
  assert.equal(notApplied.length, 1, "exactly one classified not-applied trace for this NFT");
  assert.equal(notApplied[0].detail.reason, "trait-unknown");
  assert.equal(notApplied[0].detail.price, 0.02);
  assert.equal(notApplied[0].detail.kind, "trait");
  assert.equal(notApplied[0].detail.orderHash, "0xaa1");
});

// ---- Case 2: trait-no-match (traits known, genuinely doesn't apply) also diagnosed ----
await check("trait-no-match: a token that KNOWS its traits and doesn't have the requested one is diagnosed, not silent", async () => {
  const { engine, book, row } = buildEngine({ rows: [{ tokenId: "10184", slug: "parallel-avatars" }] });
  book.setTraits([{ trait_type: "Character", value: "Heracles" }]);
  const calls = await captureTrace(async () => {
    engine.apply(traitOffer({ slug: "parallel-avatars", maker: RIVAL, price: 0.03, orderHash: "0xbb1", traitType: "Character", traitValue: "Jazmine" }));
    await flush();
  });
  const notApplied = calls.filter(c => c.stage === "book_update" && c.detail.status === "not-applied" && c.detail.tokenId === "10184");
  assert.equal(notApplied.length, 1);
  assert.equal(notApplied[0].detail.reason, "trait-no-match");
});

// ---- Case 3: stale (older seq for the same orderHash) is diagnosed ----
await check("stale: a chronologically older frame for the same orderHash is diagnosed, not silent", async () => {
  const { engine, book, row } = buildEngine({ rows: [{ tokenId: "7192", slug: "the-nibbles" }] });
  const t0 = Date.now();
  engine.apply(bidEvent({ slug: "the-nibbles", tokenId: "7192", maker: RIVAL, price: 0.04, orderHash: "0xnib1", eventTimestamp: t0 + 2000 }));
  await flush();
  const calls = await captureTrace(async () => {
    // A late-arriving duplicate feed delivery of an OLDER state for the SAME hash.
    engine.apply(bidEvent({ slug: "the-nibbles", tokenId: "7192", maker: RIVAL, price: 0.039, orderHash: "0xnib1", eventTimestamp: t0 + 1000 }));
    await flush();
  });
  const notApplied = calls.filter(c => c.stage === "book_update" && c.detail.status === "not-applied" && c.detail.tokenId === "7192");
  assert.equal(notApplied.length, 1);
  assert.equal(notApplied[0].detail.reason, "stale");
  assert.equal(book.effectiveBest(Date.now()).price, 0.04, "the real (newer) price must be unaffected by the stale frame");
});

// ---- Case 4: tombstoned (cancelled hash tries to come back) is diagnosed ----
await check("tombstoned: a bid for an already-cancelled orderHash is diagnosed, not silent", async () => {
  const { engine, book, row } = buildEngine({ rows: [{ tokenId: "8375", slug: "parallel-avatars" }] });
  // Explicit, strictly-increasing eventTimestamps -- relying on wall-clock
  // Date.now() across three back-to-back apply() calls is flaky under a
  // spawned-subprocess test runner, where two calls can land in the SAME
  // millisecond and make the third look "stale" (seq<=last) before it ever
  // reaches the tombstone check (found via run-all-tests.js: this exact test
  // flaked there while passing standalone).
  const t0 = Date.now();
  // item_cancelled is a REAL (non-soft) terminal cancel -- unlike order_invalidate,
  // it DOES place a tombstone (see memory-book.js comment on `soft`).
  engine.apply(bidEvent({ slug: "parallel-avatars", tokenId: "8375", maker: RIVAL, price: 0.05, orderHash: "0xdead1", kind: "item", eventTimestamp: t0 + 1000 }));
  await flush();
  engine.apply({
    collectionSlug: "parallel-avatars", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "8375" },
    orderHash: "0xdead1", eventTimestamp: t0 + 2000, receivedAt: Date.now(), event: "item_cancelled"
  });
  await flush();
  const calls = await captureTrace(async () => {
    engine.apply(bidEvent({ slug: "parallel-avatars", tokenId: "8375", maker: RIVAL, price: 0.05, orderHash: "0xdead1", eventTimestamp: t0 + 3000 }));
    await flush();
  });
  const notApplied = calls.filter(c => c.stage === "book_update" && c.detail.status === "not-applied" && c.detail.tokenId === "8375");
  assert.equal(notApplied.length, 1);
  assert.equal(notApplied[0].detail.reason, "tombstoned");
});

// ---- Case 5: bounded -- "duplicate" (dual-feed echo) must NOT be traced ----
await check("bounded: a duplicate (same price/endTime) frame is NOT traced -- common dual-feed echo, not diagnostically interesting", async () => {
  const { engine, book, row } = buildEngine({ rows: [{ tokenId: "6456", slug: "alienfrensnft" }] });
  const t0 = Date.now();
  engine.apply(bidEvent({ slug: "alienfrensnft", tokenId: "6456", maker: RIVAL, price: 0.0117, orderHash: "0xdup1", eventTimestamp: t0 + 1000 }));
  await flush();
  const calls = await captureTrace(async () => {
    // Feed B's echo of the SAME original event: real dual-feed duplicates carry
    // OpenSea's own event_timestamp, so this is typically a STRICTLY LATER
    // wall-clock delivery of an event whose own eventTimestamp may equal or
    // differ slightly; what matters is same orderHash + same price/endTime.
    engine.apply(bidEvent({ slug: "alienfrensnft", tokenId: "6456", maker: RIVAL, price: 0.0117, orderHash: "0xdup1", eventTimestamp: t0 + 1500 }));
    await flush();
  });
  const notApplied = calls.filter(c => c.stage === "book_update" && c.detail.status === "not-applied");
  assert.equal(notApplied.length, 0, "duplicate echoes must not flood the trace");
});

// ---- Case 6: bounded at scale -- an unrelated cancel fanned out across many tracked NFTs must not flood the trace ----
await check("bounded at scale: an unrelated collection-wide invalidate touching 50 tracked NFTs produces ZERO not-applied trace lines (no-op is filtered)", async () => {
  const rows = [];
  for (let i = 0; i < 50; i++) rows.push({ tokenId: String(1000 + i), slug: "busy-collection" });
  const { engine } = buildEngine({ rows });
  const calls = await captureTrace(async () => {
    engine.apply(invalidateEvent({ orderHash: "0xunrelated999", slug: "busy-collection" })); // no nft identity -> collection-wide fan-out
    await flush();
  });
  const notApplied = calls.filter(c => c.stage === "book_update" && c.detail.status === "not-applied");
  assert.equal(notApplied.length, 0, "an unrelated cancel across 50 NFTs must not produce 50 trace lines");
});

// ---- Case 7: hot-path latency -- computing the diagnostic must not meaningfully slow apply() ----
await check("hot-path: a real higher-priced bid is still applied and evaluated synchronously/fast even with the new diagnostic classification running alongside", async () => {
  const { engine, book, row } = buildEngine({ rows: [{ tokenId: "6789", slug: "mfers" }] });
  engine.apply(bidEvent({ slug: "mfers", tokenId: "6789", maker: RIVAL, price: 0.02, orderHash: "0xmf1" }));
  await flush();
  const startedAt = Date.now();
  engine.apply(bidEvent({ slug: "mfers", tokenId: "6789", maker: RIVAL, price: 0.05, orderHash: "0xmf2" }));
  const elapsedMs = Date.now() - startedAt;
  assert.ok(elapsedMs < 50, `apply() took ${elapsedMs}ms -- must stay fast`);
  assert.equal(book.effectiveBest(Date.now()).price, 0.05, "the real new higher bid must still be applied correctly");
});

// ---- Case 8: cold/new NFT lifecycle unaffected -- first bid on a brand-new row is diagnosed as "applied", not spuriously flagged ----
await check("new/cold NFT: the very first bid on a freshly-added row is applied cleanly, no spurious not-applied diagnostic", async () => {
  const { engine, book, row } = buildEngine({ rows: [{ tokenId: "9999", slug: "freshly-added" }] });
  const calls = await captureTrace(async () => {
    engine.apply(bidEvent({ slug: "freshly-added", tokenId: "9999", maker: RIVAL, price: 0.01, orderHash: "0xnew1" }));
    await flush();
  });
  const notApplied = calls.filter(c => c.stage === "book_update" && c.detail.status === "not-applied");
  const applied = calls.filter(c => c.stage === "book_update" && c.detail.status === "applied");
  assert.equal(notApplied.length, 0, "a genuinely new order on a cold row must never be misclassified as not-applied");
  assert.equal(applied.length, 1);
  assert.equal(book.effectiveBest(Date.now()).price, 0.01);
});

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
