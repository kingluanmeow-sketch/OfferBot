"use strict";

// 1.25.31: raising Max on a RUNNING row that was ABOVE_MAX must act at once:
// clear the stale verdict, wake the row, and take a fresh P0 Best read even
// when the Stream is healthy. Production: Max raised above Best, bot idle.
const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const { INTENT } = require("./offer-item-v2/intent-store");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}
const tick = () => new Promise(r => setImmediate(r));

(async () => {
  const heads = [];
  const adapter = {
    chain: "ethereum",
    async fetchBest() { return { empty: true, orders: [] }; },
    async fetchBestQuick() { return null; },
    async fetchBestHead(row, opts) { heads.push({ tokenId: row.tokenId, priority: opts && opts.priority }); return null; }
  };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(() => "HEALTHY");
  const contract = "0x1111111111111111111111111111111111111111";
  const key = `ethereum:${contract}:9`;
  engine.rows.set(key, { key, tokenId: "9", contract, collectionSlug: "alpha", running: true, minPrice: 0.001, maxPrice: 0.05, step: 0.0001 });
  engine.book.add({ key, chain: "ethereum", contract, tokenId: "9" });
  const row = engine.rows.get(key);
  row.retryAt = Date.now() + 60000;
  engine.intents.set(key, { target: 0.07, best: 0.069, mine: 0, generation: 0, reason: "old", at: Date.now() });
  engine.intents.setState(key, INTENT.FAILED, { lastError: "ABOVE_MAX" });
  const woke = [];
  const realWake = engine.ensureRowProgress.bind(engine);
  engine.ensureRowProgress = (k, why) => { woke.push(why); return realWake(k, why); };

  engine.patchRow(key, { maxPrice: 0.08 });
  await tick(); await tick();
  await check("Max edit wakes the row immediately", () => assert.ok(woke.includes("config")));
  await check("Max edit clears the stale retry timer and FAILED verdict", () => {
    assert.equal(row.retryAt, 0);
    const it = engine.intents.get(key);
    assert.ok(!it || it.state !== INTENT.FAILED);
  });
  await check("Max edit takes a fresh P0 Best read even with a healthy Stream", () => {
    assert.equal(heads.length, 1);
    assert.equal(heads[0].priority, 0);
  });
  heads.length = 0;
  engine.patchRow(key, { duration: 20 });
  await tick(); await tick();
  await check("a duration-only edit does not cost a read", () => assert.equal(heads.length, 0));
  process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
  if (failed) process.exitCode = 1;
})();
