"use strict";

/**
 * RED/GREEN: pre_toBookOp diagnostic, sửa lần 3 sau review.
 *
 * HAI LỖI CỦA LẦN 2 (đã sửa ở đây):
 *
 * 1. GATE SAI PHẠM VI: lần 2 ghi cho MỌI event của một collection đang theo
 *    dõi, bất kể token. Một collection có thể có hàng nghìn token trong
 *    khi tool chỉ theo dõi vài cái -- traffic của những token KHÔNG theo
 *    dõi dùng chung quota 20/s với đúng NFT cần bằng chứng, nên khi có
 *    suppressed > 0, "không thấy pre_toBookOp của orderHash X" KHÔNG còn
 *    là bằng chứng "event không tới SDK" (có thể chỉ là bị một token khác
 *    cùng collection chiếm hết quota). Sửa: `matchTrackedNft()` phải khớp
 *    ĐÚNG TOKEN (tra thẳng, hoặc so sánh tokenId quy chuẩn với CHÍNH các
 *    row đang theo dõi của slug đó) trước khi event được coi là liên quan
 *    và chạm tới rate limiter.
 *
 * 2. KHÔNG CÔNG BẰNG GIỮA HAI FEED: một quota chung 20/s để một feed lũ
 *    (reconnect storm) có thể chiếm hết, che mất feed còn lại ngay trong
 *    cửa sổ cần so sánh để phát hiện bất đồng hai feed. Sửa: quota con
 *    riêng mỗi feed (PRE_TOBOOKOP_FEED_SHARE), cộng dedupe CÙNG feed
 *    (không dedupe chéo feed -- đó chính là tín hiệu cần giữ).
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

function hookRecord() {
  const recorded = [];
  const real = productionTrace.record;
  productionTrace.record = (stage, event, detail) => { recorded.push({ stage, event, detail }); };
  return { recorded, restore: () => { productionTrace.record = real; } };
}

function itemEvent({ slug, contract, tokenId, orderHash, price, feed }) {
  return {
    collectionSlug: slug, nft: { chain: "ethereum", contract, tokenId },
    kind: "item", orderHash, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
    event: "item_received_bid", pricePerItem: price, feed
  };
}

(async () => {

await check("normal, correctly-mapped event -> pre_toBookOp status=mapped", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "9001", orderHash: "0xreal5518", price: 0.0654, feed: "A" }));
    await flush();
    const row = recorded.find(r => r.stage === "pre_toBookOp");
    assert.ok(row, "pre_toBookOp must be recorded for an exactly-mapped tracked NFT");
    assert.equal(row.detail.status, "mapped");
  } finally { restore(); }
});

// ---- Fix #1 case: 10,000 events for UNTRACKED tokens in the SAME tracked collection
// must never consume quota, never suppress the tracked token's own evidence ----
await check("10,000 events for UNTRACKED tokens in the same tracked collection do not consume quota or suppress the tracked NFT's evidence", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    for (let i = 0; i < 10000; i++) {
      engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: String(100000 + i), orderHash: `0xnoise${i}`, price: 0.01, feed: "A" }));
    }
    await flush();
    assert.equal(recorded.filter(r => r.stage === "pre_toBookOp").length, 0, "10,000 untracked-token events in the same collection must produce ZERO pre_toBookOp rows");
    assert.equal(engine.preToBookOpCount || 0, 0, "untracked-token events must never touch the rate-limit counter at all");

    // Now the ACTUAL tracked NFT's real event must still get through cleanly.
    engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "9001", orderHash: "0xrealafternoise", price: 0.0654, feed: "A" }));
    await flush();
    const real = recorded.find(r => r.stage === "pre_toBookOp");
    assert.ok(real, "the tracked NFT's own event must still be recorded -- its quota was never touched by the 10,000 unrelated events");
    assert.equal(real.detail.status, "mapped");
  } finally { restore(); }
});

// ---- Fix #2 case: feed A flood must not fully consume the shared quota and hide feed B ----
await check("feed A flood does not hide feed B's evidence -- per-feed fair sub-quota", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    // Flood feed A with 100 DISTINCT orderHashes for the SAME tracked NFT
    // (distinct hashes so dedupe doesn't collapse them) -- far beyond the
    // total 20/s cap.
    for (let i = 0; i < 100; i++) {
      engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "9001", orderHash: `0xfloodA${i}`, price: 0.01 + i * 0.0001, feed: "A" }));
    }
    // A single feed B event for a DIFFERENT distinct orderHash, interleaved mid-flood.
    engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "9001", orderHash: "0xfeedBreal", price: 0.05, feed: "B" }));
    await flush();
    const rows = recorded.filter(r => r.stage === "pre_toBookOp");
    const feedARows = rows.filter(r => r.detail.feed === "A");
    const feedBRows = rows.filter(r => r.detail.feed === "B");
    assert.ok(feedARows.length <= Math.ceil(20 * 0.75), `feed A must not exceed its fair sub-quota, got ${feedARows.length}`);
    assert.equal(feedBRows.length, 1, "feed B's single real event must get through even while feed A is flooding -- not hidden by a shared-only quota");
  } finally { restore(); }
});

// ---- Fix #1 case: wrong contract but correct tracked slug+token -> correct specific reason ----
await check("wrong contract but correct tracked slug+token -> pre_toBookOp status=unmapped-contract", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    engine.apply(itemEvent({ slug: "apuapustajas", contract: "0x9999999999999999999999999999999999999999", tokenId: "9001", orderHash: "0xwrongcontract", price: 0.05, feed: "A" }));
    await flush();
    const row = recorded.find(r => r.stage === "pre_toBookOp");
    assert.ok(row, "a tracked token with a wrong contract must still be recorded -- it matched our tokenId via the slug's tracked-row index");
    assert.equal(row.detail.status, "unmapped-contract");
  } finally { restore(); }
});

await check("correct contract+token but tokenId written in a different (canonicalizable) format -> status=unmapped-token", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    // "9001" is tracked; "009001" canonicalizes to the same value but the
    // exact-string byNft lookup misses it.
    engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "009001", orderHash: "0xformatdiff", price: 0.05, feed: "A" }));
    await flush();
    const row = recorded.find(r => r.stage === "pre_toBookOp");
    assert.ok(row, "a canonicalizable tokenId-format mismatch for a tracked token+contract must still be recorded");
    assert.equal(row.detail.status, "unmapped-token");
  } finally { restore(); }
});

// ---- Fix #1 case: completely unrelated token format (not canonicalizable, wrong contract) -> skipped entirely ----
await check("a tokenId format that cannot be canonicalized AND whose contract matches no tracked row is skipped entirely (genuinely unrelated)", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    engine.apply(itemEvent({ slug: "apuapustajas", contract: "0x8888888888888888888888888888888888888888", tokenId: "not-a-number-garbage", orderHash: "0xtrulyunrelated", price: 0.05, feed: "A" }));
    await flush();
    assert.equal(recorded.filter(r => r.stage === "pre_toBookOp").length, 0, "a non-canonicalizable tokenId with no matching tracked contract must be skipped entirely -- not flagged as possibly ours");
  } finally { restore(); }
});

await check("a tokenId format that cannot be canonicalized BUT whose contract matches a tracked row -> status=unmapped-format", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "not-a-number-garbage", orderHash: "0xformatgarbage", price: 0.05, feed: "A" }));
    await flush();
    const row = recorded.find(r => r.stage === "pre_toBookOp");
    assert.ok(row, "a malformed tokenId whose contract matches a tracked row must still be flagged -- one concrete reason to suspect relevance");
    assert.equal(row.detail.status, "unmapped-format");
  } finally { restore(); }
});

await check("an UNTRACKED collection never produces pre_toBookOp -- bounded to tracked collections only", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    engine.apply(itemEvent({ slug: "some-other-collection", contract: "0x2222222222222222222222222222222222222222", tokenId: "77777", orderHash: "0xuntracked", price: 0.01, feed: "A" }));
    await flush();
    assert.ok(!recorded.some(r => r.stage === "pre_toBookOp"), "an untracked collection must never produce a pre_toBookOp row");
  } finally { restore(); }
});

await check("same-feed redundant delivery (same orderHash+event+tokenId, same feed) within the dedupe window is suppressed without consuming quota twice", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "9001", orderHash: "0xdupe1", price: 0.05, feed: "A" }));
    engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "9001", orderHash: "0xdupe1", price: 0.05, feed: "A" }));
    await flush();
    const rows = recorded.filter(r => r.stage === "pre_toBookOp" && r.detail.orderHash === "0xdupe1");
    assert.equal(rows.length, 1, "a redundant same-feed delivery must be deduped -- only one row");
  } finally { restore(); }
});

await check("the SAME key on a DIFFERENT feed is NOT deduped -- cross-feed disagreement evidence is preserved", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "9001", orderHash: "0xcrossfeed", price: 0.05, feed: "A" }));
    engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "9001", orderHash: "0xcrossfeed", price: 0.05, feed: "B" }));
    await flush();
    const rows = recorded.filter(r => r.stage === "pre_toBookOp" && r.detail.orderHash === "0xcrossfeed");
    assert.equal(rows.length, 2, "cross-feed deliveries of the SAME key must NOT be deduped -- both feeds' evidence must be preserved to detect disagreement");
    assert.deepEqual(rows.map(r => r.detail.feed).sort(), ["A", "B"]);
  } finally { restore(); }
});

await check("diagnosticIncomplete counter increments exactly when tracked-NFT evidence is suppressed -- never for unrelated-token skips", async () => {
  const { engine } = buildEngine();
  const { restore } = hookRecord();
  try {
    for (let i = 0; i < 10000; i++) {
      engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: String(200000 + i), orderHash: `0xnoise2-${i}`, price: 0.01, feed: "A" }));
    }
    await flush();
    assert.equal(engine.stats.preToBookOpDiagnosticIncomplete || 0, 0, "unrelated-token skips must never increment diagnosticIncomplete -- they never reached the rate limiter at all");

    for (let i = 0; i < 100; i++) {
      engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "9001", orderHash: `0xflood2-${i}`, price: 0.02 + i * 0.0001, feed: "A" }));
    }
    await flush();
    assert.ok((engine.stats.preToBookOpDiagnosticIncomplete || 0) > 0, "a real tracked-NFT evidence suppression must increment diagnosticIncomplete -- future audits must check this before treating absence as a Stream miss");
  } finally { restore(); }
});

await check("reset() clears the rate-limiter window/feed counters and the dedupe map", async () => {
  const { engine } = buildEngine();
  const { restore } = hookRecord();
  try {
    for (let i = 0; i < 100; i++) {
      engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "9001", orderHash: `0xpre-reset-${i}`, price: 0.03 + i * 0.0001, feed: "A" }));
    }
    await flush();
    assert.ok(engine.preToBookOpDedupe.size > 0, "sanity: dedupe map has entries before reset");
    engine.reset("reset");
    assert.equal(engine.preToBookOpWindowAt, 0);
    assert.equal(engine.preToBookOpCount, 0);
    assert.deepEqual(engine.preToBookOpFeedCount, {});
    assert.equal(engine.preToBookOpSuppressed, 0);
    assert.equal(engine.preToBookOpDedupe.size, 0, "reset() must clear the dedupe map entirely");
  } finally { restore(); }
});

await check("pre_toBookOp diagnostic never affects apply()/evaluate()/POST outcome or timing -- bounded memory/IO, P0 unaffected", async () => {
  const { engine, book } = buildEngine();
  // Flood with unrelated-token noise AND real tracked events mixed, exactly
  // the adversarial shape this fix targets, then measure P0 cost.
  for (let i = 0; i < 2000; i++) {
    engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: String(300000 + i), orderHash: `0xmix${i}`, price: 0.01, feed: i % 2 === 0 ? "A" : "B" }));
  }
  const t0 = process.hrtime.bigint();
  engine.apply(itemEvent({ slug: "apuapustajas", contract: CONTRACT, tokenId: "9001", orderHash: "0xsideeffect", price: 0.05, feed: "A" }));
  const elapsedMs = Number(process.hrtime.bigint() - t0) / 1e6;
  await flush();
  assert.ok(elapsedMs < 20, `apply() took ${elapsedMs}ms under 2000-event noise -- the diagnostic must stay cheap and never slow the hot path`);
  assert.equal(book.effectiveBest(Date.now()).price, 0.05, "the real event must still apply correctly -- diagnostic is purely additive");
  assert.ok(engine.preToBookOpDedupe.size <= 500, `dedupe map must stay bounded (PRE_TOBOOKOP_DEDUPE_MAX), got ${engine.preToBookOpDedupe.size}`);
});

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
