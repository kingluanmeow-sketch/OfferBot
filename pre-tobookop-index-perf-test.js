"use strict";

/**
 * RED/GREEN: pre_toBookOp O(1) index (audit sửa lần 4 -- perf).
 *
 * BẢN TRƯỚC: `matchTrackedNft()` khi exact `byNft` miss duyệt TOÀN BỘ
 * `book.bySlug` của slug rồi `book.get()+canonicalTokenId()` từng row --
 * O(events × tracked rows trong slug) ngay trên `apply()`/hot path. Với
 * 157 NFT cùng collection và nhiều event token không tracked (bình thường
 * trên một collection bận), đây tự tạo lag/miss thật trên P0.
 *
 * SỬA: `preToBookOpIndex` (Map<slug, {byCanon, contracts}>), cập nhật O(1)
 * tại `registerRow`/`removeRow`, không bao giờ quét `this.rows`/`book`
 * trong `apply()`.
 *
 * Bài test này đo TRỰC TIẾP: 157 NFT cùng 1 collection, 10,000-100,000
 * event token KHÔNG tracked qua 2 feed, xen kẽ event thật cho NFT tracked,
 * đo đầy đủ apply()->Decision SEND->intent->http_start, không chỉ
 * apply() đơn lẻ.
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
const SLUG = "bigcollection";
const TRACKED_CONTRACT = "0x1111111111111111111111111111111111111111";

function itemEvent({ contract, tokenId, orderHash, price, feed }) {
  return {
    collectionSlug: SLUG, nft: { chain: "ethereum", contract, tokenId },
    kind: "item", orderHash, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true,
    event: "item_received_bid", pricePerItem: price, feed
  };
}

function buildEngineWith157Rows() {
  const resolvers = [];
  const adapter = {
    chain: "ethereum", openseaChain: "ethereum",
    toWei: x => String(Math.round(Number(x) * 1e18)),
    offerBody: () => ({}),
    async fetchBest() { return new Promise(res => resolvers.push(res)); }
  };
  const SELF = "0x4444444444444444444444444444444444444444";
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(() => "HEALTHY");
  engine.templateReady = () => true;
  engine.ownAuthoritative = () => true;
  engine.topicReady = () => true;
  engine.builder = { address: SELF, async build(k, { onStage }) { onStage && onStage("build_started"); return { components: { endTime: Math.floor(Date.now() / 1000) + 900 }, signature: "0xsig", orderHash: "0xdead" }; } };
  const httpStartAt = new Map();
  const realRecord = productionTrace.record.bind(productionTrace);
  productionTrace.record = (stage, event, detail) => {
    if (stage === "http_start") {
      const cid = (event && event.correlationId) || (detail && detail.correlationId) || "";
      if (cid) httpStartAt.set(cid, Date.now());
    }
  };
  let postCount = 0;
  engine.http = { request: ({ onStage }) => { postCount++; onStage && onStage("http_started"); return new Promise(resolve => setImmediate(() => resolve({ status: 200, headers: {}, body: { order_hash: "0xposted" } }))); } };
  require("./dev-runtime").spendAllowed = () => true;
  engine.book.setSelfAddress(SELF);

  const rows = [];
  for (let i = 0; i < 157; i++) {
    const tokenId = String(70000 + i);
    rows.push(engine.registerRow({
      url: "", contract: TRACKED_CONTRACT, tokenId, collectionSlug: SLUG,
      minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15
    }));
  }
  const trackedKeys = [...engine.rows.keys()];

  return { engine, resolvers, httpStartAt, trackedKeys, getPostCount: () => postCount, restoreTrace: () => { productionTrace.record = realRecord; } };
}

(async () => {

await check("157 tracked NFTs + 100,000 untracked-token events across 2 feeds in the same collection: index stays bounded, zero pre_toBookOp rows for noise, tracked NFT evidence intact", async () => {
  const { engine, trackedKeys, restoreTrace } = buildEngineWith157Rows();
  const recorded = [];
  productionTrace.record = (stage, event, detail) => { recorded.push({ stage, event, detail }); };
  try {
    const t0 = process.hrtime.bigint();
    for (let i = 0; i < 100000; i++) {
      engine.apply(itemEvent({ contract: TRACKED_CONTRACT, tokenId: String(900000 + i), orderHash: `0xnoise${i}`, price: 0.01, feed: i % 2 === 0 ? "A" : "B" }));
    }
    const elapsedMs = Number(process.hrtime.bigint() - t0) / 1e6;
    await flush(5);
    process.stdout.write(`  [100,000 untracked-token events] total=${elapsedMs.toFixed(1)}ms avg=${(elapsedMs / 100000).toFixed(4)}ms/event\n`);
    // O(1) index must keep 100,000 untracked-token events cheap; the rejected
    // O(scan-bySlug-per-event) design measured ~14x slower on this exact case.
    assert.ok(elapsedMs < 600, `100,000 untracked-token events took ${elapsedMs.toFixed(1)}ms -- matchTrackedNft() must be O(1), not scanning book.bySlug per event`);

    assert.equal(recorded.filter(r => r.stage === "pre_toBookOp").length, 0, "100,000 untracked-token events must produce ZERO pre_toBookOp rows");
    // Index size must stay bounded by TRACKED rows (157 contracts x up to 157
    // canon buckets), never growing with the number of untracked events seen.
    let indexEntries = 0;
    for (const bucket of engine.preToBookOpIndex.values()) indexEntries += bucket.byCanon.size;
    assert.ok(indexEntries <= 157, `preToBookOpIndex must stay bounded by tracked rows (<=157 byCanon entries), got ${indexEntries}`);

    // A real event for an ACTUALLY tracked NFT must still produce correct evidence.
    const trackedTokenId = engine.rows.get(trackedKeys[0]).tokenId;
    engine.apply(itemEvent({ contract: TRACKED_CONTRACT, tokenId: trackedTokenId, orderHash: "0xrealtracked", price: 0.05, feed: "A" }));
    await flush();
    const real = recorded.find(r => r.stage === "pre_toBookOp" && r.detail.tokenId === trackedTokenId);
    assert.ok(real, "the tracked NFT's real event must still be recorded after 100,000 unrelated events -- its own quota/index entry was never touched by the noise");
    assert.equal(real.detail.status, "mapped");
  } finally { restoreTrace(); }
});

await check("full pipeline (apply->Decision SEND->intent->http_start) stays fast while 10,000 untracked-token events interleave with real competitor events for tracked NFTs", async () => {
  const { engine, resolvers, httpStartAt, trackedKeys, getPostCount, restoreTrace } = buildEngineWith157Rows();
  try {
    const N_NOISE = 10000;
    const N_REAL = 100;
    const fullPipelineMs = [];
    let noiseIdx = 0;
    const injectNoise = count => {
      for (let i = 0; i < count; i++, noiseIdx++) {
        engine.apply(itemEvent({ contract: TRACKED_CONTRACT, tokenId: String(950000 + noiseIdx), orderHash: `0xnoise2-${noiseIdx}`, price: 0.01, feed: noiseIdx % 2 === 0 ? "A" : "B" }));
      }
    };
    const noisePerRealEvent = Math.floor(N_NOISE / N_REAL);

    for (let i = 0; i < N_REAL; i++) {
      injectNoise(noisePerRealEvent);
      const row = engine.rows.get(trackedKeys[i % trackedKeys.length]);
      const correlationId = `perf-${i}`;
      const t0 = Date.now();
      engine.apply({
        collectionSlug: SLUG, nft: { chain: "ethereum", contract: TRACKED_CONTRACT, tokenId: row.tokenId },
        kind: "item", orderHash: `0xrealcomp-${i}`, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
        eventTimestamp: t0, receivedAt: t0, hasOrderData: true, event: "item_received_bid",
        pricePerItem: 0.03 + (i % 20) * 0.001, correlationId
      });
      await new Promise(res => setImmediate(res));
      const httpStart = httpStartAt.get(correlationId);
      if (httpStart) fullPipelineMs.push(httpStart - t0);
    }
    injectNoise(N_NOISE - noiseIdx);
    await flush(10);

    const pct = (arr, p) => { const s = [...arr].sort((a, b) => a - b); return s.length ? s[Math.min(s.length - 1, Math.floor(s.length * p))] : null; };
    process.stdout.write(`  [full pipeline apply->http_start under 10,000-event noise, n=${fullPipelineMs.length}] p50=${pct(fullPipelineMs, 0.5)}ms p95=${pct(fullPipelineMs, 0.95)}ms max=${fullPipelineMs.length ? Math.max(...fullPipelineMs) : null}ms\n`);
    process.stdout.write(`  [POST count]: ${getPostCount()}\n`);
    assert.ok(fullPipelineMs.length >= N_REAL * 0.8, `expected most of the ${N_REAL} real competitor events to produce a POST despite 10,000 interleaved noise events, got ${fullPipelineMs.length}`);
    if (fullPipelineMs.length) assert.ok(pct(fullPipelineMs, 0.95) < 50, `p95 full-pipeline latency ${pct(fullPipelineMs, 0.95)}ms must stay fast under noise`);
  } finally { restoreTrace(); }
});

await check("add/remove/re-add/Start-reload does not leave stale index entries -- generation-safe", async () => {
  const { engine, restoreTrace } = buildEngineWith157Rows();
  try {
    const key = [...engine.rows.keys()][0];
    const row = engine.rows.get(key);
    const slug = row.collectionSlug;
    const canon = require("./offer-item-v2/engine-v2"); // just to ensure module loaded; canonicalTokenId isn't exported, check via index directly

    const before = engine.preToBookOpIndex.get(slug);
    const beforeCount = before ? [...before.byCanon.values()].reduce((n, a) => n + a.length, 0) : 0;
    engine.removeRow(key);
    const after = engine.preToBookOpIndex.get(slug);
    const afterCount = after ? [...after.byCanon.values()].reduce((n, a) => n + a.length, 0) : 0;
    assert.equal(afterCount, beforeCount - 1, "removeRow must remove exactly this row's index entry, nothing stale left");

    // Re-add the same NFT -- a fresh entry must appear, not a duplicate/ghost.
    engine.registerRow({ url: "", contract: row.contract, tokenId: row.tokenId, collectionSlug: row.collectionSlug, minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 });
    const readded = engine.preToBookOpIndex.get(slug);
    const readdedCount = readded ? [...readded.byCanon.values()].reduce((n, a) => n + a.length, 0) : 0;
    assert.equal(readdedCount, beforeCount, "re-adding the same NFT must restore exactly one index entry, not leak a duplicate");

    // Simulate the exact reload mechanism start() uses (this.rows.clear() +
    // this.preToBookOpIndex.clear(), then registerRow() per new row) --
    // the index must be rebuilt from scratch, never accumulate stale
    // entries from the old set.
    engine.rows.clear();
    engine.preToBookOpIndex.clear();
    engine.registerRow({ url: "", contract: row.contract, tokenId: "999999", collectionSlug: row.collectionSlug, minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 });
    const totalAfter = [...engine.preToBookOpIndex.values()].reduce((n, b) => n + [...b.byCanon.values()].reduce((m, a) => m + a.length, 0), 0);
    assert.equal(totalAfter, 1, `after a reload with exactly 1 new row, the index must contain exactly 1 entry, not accumulate the old 157, got ${totalAfter}`);
  } finally { restoreTrace(); }
});

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
