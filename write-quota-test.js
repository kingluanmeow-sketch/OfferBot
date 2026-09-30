"use strict";

// 1.25.32 order posting: the two keys belong to two DIFFERENT OpenSea
// accounts (owner-confirmed), so their quotas are independent pools.
// Dashboard: Key 1 2/s, Key 2 1/s. Target at 90%: 1.8/s + 0.9/s = 2.7/s.
// 1.25.31 still had a shared model capping the total at ~1.8/s.
const assert = require("node:assert/strict");
const { QuotaBroker } = require("./offer-item-v2/quota-broker");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}
const DOMAINS = ["key1fp", "key2fp"];

async function drain(broker, ms) {
  const grants = [];
  const start = Date.now();
  while (Date.now() - start < ms) {
    const g = broker.grantOne({ domains: DOMAINS });
    if (g) grants.push({ at: Date.now() - start, domain: g.domain });
    await new Promise(r => setTimeout(r, 5));
  }
  return grants;
}
const maxInWindow = (grants, domain) => {
  const list = grants.filter(g => !domain || g.domain === domain);
  let max = 0;
  for (const g of list) max = Math.max(max, list.filter(x => x.at >= g.at && x.at < g.at + 1000).length);
  return max;
};

(async () => {
  {
    const broker = new QuotaBroker({ getApiKey: () => "k", onLog() {} });
    broker.pickDomain({ domains: DOMAINS });
    await check("per-key posting ceilings match each account's dashboard at 90%", () => {
      assert.equal(+broker.modelFor("key1fp").rateCeiling.toFixed(2), 1.8);
      assert.equal(+broker.modelFor("key2fp").rateCeiling.toFixed(2), 0.9);
    });
    const grants = await drain(broker, 3000);
    const k1 = grants.filter(g => g.domain === "key1fp").length;
    const k2 = grants.filter(g => g.domain === "key2fp").length;
    await check("3 s sustained backlog reaches the independent aggregate (~2.7/s), no global 1.8/s cap", () => {
      assert.ok(grants.length >= 7.5, `aggregate ${grants.length} in 3 s (a shared 1.8/s cap allows ~6)`);
      assert.ok(grants.length <= 2 + 2.7 * 3 + 1, `too many: ${grants.length}`);
    });
    await check("Key 1 runs near 1.8/s and Key 2 near 0.9/s", () => {
      assert.ok(k1 >= 5 && k1 <= 7, `key1 ${k1}`);
      assert.ok(k2 >= 2 && k2 <= 4, `key2 ${k2}`);
    });
    await check("backlog uses both keys: Key 2 is not idle while Key 1 paces", () => {
      assert.ok(k2 >= 2, `key2 only ${k2}`);
    });
    await check("no key exceeds its own dashboard rate in any one-second window", () => {
      assert.ok(maxInWindow(grants, "key1fp") <= 2, `key1 window ${maxInWindow(grants, "key1fp")}`);
      assert.ok(maxInWindow(grants, "key2fp") <= 1, `key2 window ${maxInWindow(grants, "key2fp")}`);
    });
    await check("the broker has no shared cross-account write model any more", () => {
      assert.equal(broker.shared, undefined);
    });
  }

  for (const [hit, other] of [["key1fp", "key2fp"], ["key2fp", "key1fp"]]) {
    const broker = new QuotaBroker({ getApiKey: () => "k", onLog() {} });
    broker.pickDomain({ domains: DOMAINS });
    const otherRate = broker.modelFor(other).rate;
    broker.applyReport({ domain: hit, status: 429 });
    const grants = await drain(broker, 2000);
    await check(`${hit} 429: only ${hit} backs off, ${other} keeps its full rate`, () => {
      assert.ok(broker.modelFor(hit).rate < broker.modelFor(hit).rateCeiling, "hit key must slow down");
      assert.equal(broker.modelFor(other).rate, otherRate);
      const o = grants.filter(g => g.domain === other).length;
      assert.ok(o >= (other === "key1fp" ? 3 : 1), `${other} granted only ${o} in 2 s`);
    });
  }

  {
    const broker = new QuotaBroker({ getApiKey: () => "k", onLog() {} });
    const g = broker.grantOne({ domains: DOMAINS });
    await check("a grant is recorded per domain for 60 s instrumentation", () => {
      assert.ok(g);
      const st = broker.grantRates();
      assert.equal(st[g.domain].grants60s, 1);
      assert.ok(st[g.domain].ceilingRps > 0);
    });
  }

  process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
  if (failed) process.exitCode = 1;
  process.exit();
})();
