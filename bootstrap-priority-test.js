"use strict";

// 1.25.31 NEW NFT bootstrap priority. Live: rows with Stream events sat on
// BLOCKED: template for tens of seconds because collection fees were read
// on the background lane (P2, <=1 req/s per key behind 100+ rows).
const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const { ReadDispatcher } = require("./read-dispatcher");
const { RecoveryPlane } = require("./offer-item-v2/recovery-plane");
const { PRIORITY } = require("./rate-limiter");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}
const tick = () => new Promise(r => setImmediate(r));

(async () => {
  {
    const seen = [];
    const adapter = {
      chain: "ethereum",
      async fetchBest() { return { empty: true, orders: [] }; },
      async chainConfig() { return { chainId: 1, wethAddress: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", conduitKey: "0x" + "00".repeat(32), signedZone: "0x000056F7000000EcE9003ca63978907a00FFD100" }; },
      async accountState(address) { return { address, counter: "0" }; },
      async collectionConfig(slug, contract, opts) { seen.push(opts && opts.priority); return { slug, fees: [], requiresSignedZone: false, tokenStandard: "erc721" }; }
    };
    const engine = new OfferItemEngineV2({ adapter, onLog() {} });
    engine.state = STATE.RUNNING;
    const tpl = new Map();
    engine.builder = { address: "0xe86c742415b39aae683c4b020da75e07956e3413", hydrate(k, t) { tpl.set(k, t); }, get: k => tpl.get(k) || null };
    engine.registerRow({ url: "u", contract: "0x1111111111111111111111111111111111111111", tokenId: "5", collectionSlug: "alpha", minPrice: 0.001, maxPrice: 1, step: 0.0001 });
    const key = [...engine.rows.keys()][0];
    engine.ensureTemplate(key, "test");
    for (let i = 0; i < 5; i++) await tick();
    await check("template hydration asks on the realtime lane, not background", () => {
      assert.equal(seen.length, 1);
      assert.ok(seen[0] <= PRIORITY.P0);
    });
    await check("bootstrap records ADD -> template milestone", () => {
      const b = engine.rows.get(key).bootstrap;
      assert.ok(b && b.startAt > 0 && b.templateAt >= b.startAt);
    });
    engine.bootstrapMark(key, "firstReadAt");
    engine.bootstrapMark(key, "decisionAt");
    engine.bootstrapMark(key, "queuedAt");
    engine.bootstrapMark(key, "successAt", Date.now(), "SUCCESS");
    engine.bootstrapMark(key, "successAt", Date.now() + 5000, "SUCCESS");
    await check("bootstrap ends at FIRST SEND SUCCESS and is measured once", () => {
      const st = engine.bootstrapStats();
      assert.equal(st.firstSends, 1);
      assert.ok(st.totalMs.p50 >= 0);
    });
  }

  {
    // Live 2026-09-29: new NFT template stalled 35/38s behind a flood of
    // ordinary P0 own-state-unknown reads from old rows because both asked
    // at the same priority (P0) and the dispatcher is FIFO within a tier.
    const seen = [];
    const adapter = {
      chain: "ethereum",
      async fetchBest() { return { empty: true, orders: [] }; },
      async chainConfig() { return { chainId: 1, wethAddress: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", conduitKey: "0x" + "00".repeat(32), signedZone: "0x000056F7000000EcE9003ca63978907a00FFD100" }; },
      async accountState(address) { return { address, counter: "0" }; },
      async collectionConfig(slug, contract, opts) { seen.push(opts && opts.priority); return { slug, fees: [], requiresSignedZone: false, tokenStandard: "erc721" }; }
    };
    const engine = new OfferItemEngineV2({ adapter, onLog() {} });
    engine.state = STATE.RUNNING;
    engine.builder = { address: "0xe86c742415b39aae683c4b020da75e07956e3413", hydrate() {}, get: () => null };
    engine.registerRow({ url: "u", contract: "0x1111111111111111111111111111111111111111", tokenId: "88", collectionSlug: "alpha", minPrice: 0.001, maxPrice: 1, step: 0.0001 });
    const newKey = [...engine.rows.keys()][0];
    engine.ensureTemplate(newKey, "test");
    await tick();
    await check("a never-templated NEW row asks for its template at INITIAL priority, above ordinary P0", () => {
      assert.equal(seen[0], PRIORITY.INITIAL);
      assert.ok(PRIORITY.INITIAL < PRIORITY.P0, "INITIAL must outrank P0 in the read dispatcher");
    });
    seen.length = 0;
    engine.rows.get(newKey).bootstrap.templateAt = Date.now();
    engine.templateState.delete(newKey);
    engine.ensureTemplate(newKey, "counter-changed-refresh");
    await tick();
    await check("a routine template refresh (already bootstrapped) stays at ordinary P0", () => {
      assert.equal(seen[0], PRIORITY.P0);
    });
  }

  {
    const pool = [{ key: "a", inFlight: 0, blockedUntil: 0 }, { key: "b", inFlight: 0, blockedUntil: 0 }];
    const keys = { pool, find: k => pool.find(e => e.key === k), leaseKey: () => ({ release() {} }) };
    let t = 0;
    const d = new ReadDispatcher({ keys, now: () => t });
    d.global.tokens = 0;
    const order = [];
    const jobs = [
      d.acquire({ priority: PRIORITY.P2 }).then(l => { order.push("background"); l.release(); }),
      d.acquire({ priority: 0.75 }).then(l => { order.push("cold-poll"); l.release(); }),
      d.acquire({ priority: PRIORITY.P0 }).then(l => { order.push("template-P0"); l.release(); }),
      d.acquire({ priority: PRIORITY.INITIAL }).then(l => { order.push("first-read"); l.release(); })
    ];
    for (t = 0; t <= 5000; t += 50) { d.pump(); await tick(); }
    await Promise.all(jobs);
    await check("new NFT first-read and template are served before polling and background", () => {
      assert.deepEqual(order.slice(0, 2), ["first-read", "template-P0"]);
      assert.equal(order[order.length - 1], "background");
    });
  }

  {
    const plane = new RecoveryPlane({ workers: 6, readsPerSecond: 100 });
    const hang = () => new Promise(() => {});
    for (let i = 0; i < 30; i++) plane.push({ kind: "old-row-recovery", priority: 2, run: hang });
    let firstReads = 0;
    for (let i = 0; i < 5; i++) plane.push({ kind: "first-read", priority: 1, run: () => { firstReads++; return hang(); } });
    await new Promise(r => setTimeout(r, 20));
    await check("five NFTs added together all start their first read despite 30 queued old-row jobs", () => {
      assert.ok(firstReads >= 3, `started ${firstReads}`);
      assert.ok(plane.background >= 1, "old rows are not starved either");
    });
    plane.stop();
  }

  process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
  if (failed) process.exitCode = 1;
  process.exit();
})();
