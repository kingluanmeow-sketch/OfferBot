"use strict";

/**
 * RED/GREEN: bounded pre-`toBookOp()` diagnostic (audit, Apu #5518/#7309),
 * sửa lần 2 sau review.
 *
 * BẢN ĐẦU SAI: `recordRawStreamEvidence()` tra `book.byNft` TRƯỚC khi quyết
 * định có ghi gì không -- nên nếu CHÍNH việc map contract/tokenId sai
 * (binding bug, không phải Stream thật sự im lặng), bằng chứng cũng biến
 * mất giống `stream_rx`, không tách được "SDK đã giao event nhưng lookup
 * sai" khỏi "event không tới".
 *
 * SỬA: điều kiện ghi là `event.collectionSlug` khớp một collection đang
 * theo dõi qua `book.bySlug` (ĐỘC LẬP với `byNft`/contract/tokenId). Việc
 * tra `byNft` chỉ quyết định GIÁ TRỊ `status` ("mapped"/"unmapped"), không
 * quyết định có ghi dòng đó hay không. Stage đổi tên `pre_toBookOp` (rõ
 * nghĩa: đã qua SDK, CHƯA qua `toBookOp` -- không phải raw WebSocket/packet
 * capture).
 *
 * BẢN ĐẦU CŨNG SAI Ở RATE LIMIT: comment nói "20 dòng/s" nhưng không có
 * limiter THẬT nào -- `productionTrace.record()` được gọi cho MỌI event
 * tracked, và `MAX_QUEUE` (production-trace.js) chỉ là trần kích thước
 * hàng đợi ghi file, không phải rate limiter theo thời gian. Dual-feed có
 * thể nhân đôi I/O trên hot path khi một collection bận. SỬA: cửa sổ 1s đo
 * bằng `Date.now()` (không timer sống lâu), tối đa PRE_TOBOOKOP_RATE_LIMIT
 * dòng/giây TOÀN ENGINE; phần vượt chỉ tăng một counter bounded và ghi một
 * dòng summary (qua `this.log`, không qua productionTrace) khi cửa sổ kế
 * tiếp mở.
 *
 * GIỚI HẠN TRUNG THỰC (không che bằng polling): nếu CẢ HAI feed không nhận
 * được gì cho một order, và không có bằng chứng transport/topic gap nào
 * khác, không có cách cục bộ nào phân biệt với "order không tồn tại". Đây
 * cũng KHÔNG phải packet/raw WebSocket capture -- event đã qua SDK giải mã
 * thành object JS; nếu SDK tự giải mã sai `collectionSlug`, chẩn đoán này
 * cũng không giúp được.
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

(async () => {

await check("normal, correctly-mapped event -> pre_toBookOp status=mapped", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    engine.apply({
      collectionSlug: "apuapustajas", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9001" },
      kind: "item", orderHash: "0xreal5518", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
      event: "item_received_bid", pricePerItem: 0.0654, feed: "A"
    });
    await flush();
    const row = recorded.find(r => r.stage === "pre_toBookOp");
    assert.ok(row, "pre_toBookOp must be recorded for a tracked collection");
    assert.equal(row.detail.status, "mapped", "a correctly-mapped NFT must report status=mapped");
    assert.equal(row.detail.tokenId, "9001");
  } finally { restore(); }
});

await check("THE BUG THIS FIXES: collection is tracked (slug matches) but contract/tokenId lookup fails -- pre_toBookOp still fires with status=unmapped, not silence", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    // Same tracked collectionSlug, but a WRONG/unregistered contract address
    // (simulates a byNft mapping bug) -- the raw event genuinely arrived and
    // belongs to a tracked collection, so evidence must NOT disappear.
    engine.apply({
      collectionSlug: "apuapustajas", nft: { chain: "ethereum", contract: "0x9999999999999999999999999999999999999999", tokenId: "9001" },
      kind: "item", orderHash: "0xlookupbug", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
      event: "item_received_bid", pricePerItem: 0.05, feed: "A"
    });
    await flush();
    const row = recorded.find(r => r.stage === "pre_toBookOp");
    assert.ok(row, "pre_toBookOp must still fire -- the collection IS tracked, independent of whether contract/tokenId resolves");
    assert.equal(row.detail.status, "unmapped", "a tracked-collection event whose contract/tokenId lookup fails must report unmapped, not vanish like a never-received event");
  } finally { restore(); }
});

await check("pre_toBookOp fires even when toBookOp() would separately return null (unusable price) -- distinguishes 'received but dropped downstream' from 'never sent'", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    engine.apply({
      collectionSlug: "apuapustajas", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9001" },
      kind: "item", orderHash: "0xunusable", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
      event: "item_received_bid", pricePerItem: 0, feed: "B"
    });
    await flush();
    const pre = recorded.find(r => r.stage === "pre_toBookOp");
    const rx = recorded.find(r => r.stage === "stream_rx");
    assert.ok(pre, "pre_toBookOp must fire from the collection-slug check alone, before toBookOp() even runs");
    assert.equal(pre.detail.status, "mapped", "byNft lookup itself succeeds here -- only the price made toBookOp() reject it downstream");
    assert.ok(!rx, "sanity: stream_rx must NOT fire -- toBookOp() returned null");
  } finally { restore(); }
});

await check("an UNTRACKED collection never produces pre_toBookOp -- bounded to tracked collections only", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    engine.apply({
      collectionSlug: "some-other-collection", nft: { chain: "ethereum", contract: "0x2222222222222222222222222222222222222222", tokenId: "77777" },
      kind: "item", orderHash: "0xuntracked", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
      event: "item_received_bid", pricePerItem: 0.01, feed: "A"
    });
    await flush();
    assert.ok(!recorded.some(r => r.stage === "pre_toBookOp"), "an untracked collection must never produce a pre_toBookOp row");
  } finally { restore(); }
});

await check("real rate limiter: 1000 events/second x 2 feeds never exceeds the configured cap", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  try {
    // Simulate ~1000 events/s x 2 feeds arriving within the SAME 1-second
    // window (no real wall-clock wait -- Date.now() stays effectively
    // constant across this tight loop, exactly the worst-case burst shape).
    for (let i = 0; i < 2000; i++) {
      engine.apply({
        collectionSlug: "apuapustajas", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9001" },
        kind: "item", orderHash: `0xburst${i}`, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
        eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
        event: "item_received_bid", pricePerItem: 0.01 + i * 0.0000001, feed: i % 2 === 0 ? "A" : "B"
      });
    }
    await flush();
    const rows = recorded.filter(r => r.stage === "pre_toBookOp");
    assert.ok(rows.length <= 20, `rate limiter must cap pre_toBookOp at <=20/s regardless of 2000 events arriving in one window, got ${rows.length}`);
    assert.ok(rows.length >= 1, "sanity: at least the first events within the cap must still be recorded");
  } finally { restore(); }
});

await check("summary line reports the correct suppressed count once the next window opens", async () => {
  const { engine } = buildEngine();
  const { recorded, restore } = hookRecord();
  const logs = [];
  const realLog = engine.log.bind(engine);
  engine.log = msg => { logs.push(msg); };
  try {
    for (let i = 0; i < 50; i++) {
      engine.apply({
        collectionSlug: "apuapustajas", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9001" },
        kind: "item", orderHash: `0xburst2-${i}`, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
        eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
        event: "item_received_bid", pricePerItem: 0.02 + i * 0.0000001, feed: "A"
      });
    }
    await flush();
    assert.equal(engine.preToBookOpSuppressed, 30, `expected exactly 30 suppressed (50 - 20 cap), got ${engine.preToBookOpSuppressed}`);
    // Force the next window open and fire one more event to trigger the summary log.
    engine.preToBookOpWindowAt = Date.now() - 1100;
    engine.apply({
      collectionSlug: "apuapustajas", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9001" },
      kind: "item", orderHash: "0xafterwindow", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
      event: "item_received_bid", pricePerItem: 0.03, feed: "A"
    });
    await flush();
    const summary = logs.find(l => l.includes("PRE-TOBOOKOP") && l.includes("+30"));
    assert.ok(summary, `expected a summary log line reporting +30 suppressed, got logs: ${JSON.stringify(logs)}`);
  } finally { restore(); engine.log = realLog; }
});

await check("reset() clears the rate-limiter counters -- no stale suppressed count carries into the next run", async () => {
  const { engine } = buildEngine();
  const { restore } = hookRecord();
  try {
    for (let i = 0; i < 30; i++) {
      engine.apply({
        collectionSlug: "apuapustajas", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9001" },
        kind: "item", orderHash: `0xpre-reset-${i}`, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
        eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
        event: "item_received_bid", pricePerItem: 0.04 + i * 0.0000001, feed: "A"
      });
    }
    await flush();
    assert.ok(engine.preToBookOpSuppressed > 0, "sanity: some suppression happened before reset");
    engine.reset("reset");
    assert.equal(engine.preToBookOpWindowAt, 0, "reset() must clear the window timestamp");
    assert.equal(engine.preToBookOpCount, 0, "reset() must clear the per-window count");
    assert.equal(engine.preToBookOpSuppressed, 0, "reset() must clear the suppressed count -- no stale number leaks into the next run");
  } finally { restore(); }
});

await check("pre_toBookOp diagnostic never affects apply()/evaluate()/POST outcome or timing -- read-only, no REST, no recovery side effect", async () => {
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
