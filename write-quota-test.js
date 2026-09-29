"use strict";

// 1.25.31 order posting (owner, final): dashboard Key A 2/s, Key B 1/s, 90%
// headroom, keys NOT added, no one-second window above 2, 429 slows the pool.
const assert = require("node:assert/strict");
const { QuotaBroker } = require("./offer-item-v2/quota-broker");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}

(async () => {
  const broker = new QuotaBroker({ getApiKey: () => "k", onLog() {} });
  broker.pickDomain({ domains: ["keyAfp", "keyBfp"] });
  await check("per-key posting ceilings match the dashboard at 90%", () => {
    assert.equal(+broker.modelFor("keyAfp").rateCeiling.toFixed(2), 1.8);
    assert.equal(+broker.modelFor("keyBfp").rateCeiling.toFixed(2), 0.9);
  });
  const grants = [];
  const start = Date.now();
  while (Date.now() - start < 3000) {
    const g = broker.grantOne({ domains: ["keyAfp", "keyBfp"] });
    if (g) grants.push({ at: Date.now() - start, domain: g.domain });
    await new Promise(r => setTimeout(r, 5));
  }
  await check("total POST grants stay at one key's rate (keys not added)", () => {
    assert.ok(grants.length <= 1 + 1.8 * 3 + 1, `granted ${grants.length} in 3 s`);
    assert.ok(grants.length >= 4, `underused: ${grants.length}`);
  });
  await check("no one-second window exceeds 2 POSTs", () => {
    for (const g of grants) {
      const n = grants.filter(x => x.at >= g.at && x.at < g.at + 1000).length;
      assert.ok(n <= 2, `window at ${g.at}: ${n}`);
    }
  });
  await check("AIMD cannot raise a key above its ceiling", () => {
    const k = broker.modelFor("keyAfp");
    for (let i = 0; i < 50; i++) { k.lastAdjustMono = 0; k.goodResponses = 10; k.observe({ status: 200 }); }
    assert.ok(k.rate <= 1.8 + 1e-9, `rate ${k.rate}`);
  });
  await check("a 429 slows the shared pool", () => {
    const before = broker.shared.rate;
    broker.applyReport({ domain: "keyBfp", status: 429 });
    assert.ok(broker.shared.rate < before);
  });
  process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
  if (failed) process.exitCode = 1;
  process.exit();
})();
