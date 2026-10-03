"use strict";

// Two owner-confirmed, separate OpenSea accounts: start from measured rates,
// then adapt upward on clean responses and back off per account on 429.
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
    await check("per-key adaptive probe starts at observed rate with bounded 4x ceiling", () => {
      assert.equal(+broker.modelFor("key1fp").rate.toFixed(2), 2);
      assert.equal(+broker.modelFor("key2fp").rate.toFixed(2), 1);
      assert.equal(+broker.modelFor("key1fp").rateCeiling.toFixed(2), 8);
      assert.equal(+broker.modelFor("key2fp").rateCeiling.toFixed(2), 4);
    });
    const grants = await drain(broker, 3000);
    const k1 = grants.filter(g => g.domain === "key1fp").length;
    const k2 = grants.filter(g => g.domain === "key2fp").length;
    await check("3 s sustained backlog uses both independent account pools", () => {
      assert.ok(grants.length >= 8, `aggregate ${grants.length} in 3 s`);
      assert.ok(grants.length <= 2 + 3 * 3 + 1, `too many: ${grants.length}`);
    });
    await check("Key 1 runs near 2/s and Key 2 near 1/s", () => {
      assert.ok(k1 >= 6 && k1 <= 8, `key1 ${k1}`);
      assert.ok(k2 >= 3 && k2 <= 5, `key2 ${k2}`);
    });
    await check("backlog uses both keys: Key 2 is not idle while Key 1 paces", () => {
      assert.ok(k2 >= 2, `key2 only ${k2}`);
    });
    await check("initial probe respects each measured pacing interval", () => {
      for (const [domain, minimumMs] of [["key1fp", 490], ["key2fp", 990]]) {
        const times = grants.filter(g => g.domain === domain).map(g => g.at);
        const smallestGap = Math.min(...times.slice(1).map((at, i) => at - times[i]));
        assert.ok(smallestGap >= minimumMs, `${domain} smallest gap ${smallestGap}ms`);
      }
    });
    await check("the broker has no shared cross-account write model any more", () => {
      assert.equal(broker.shared, undefined);
    });
  }

  {
    const { QuotaModel } = require("./offer-item-v2/quota-broker");
    const model = new QuotaModel({ startRps: 2, rateFloor: 0.25, rateCeiling: 8 });
    const start = QuotaModel.mono();
    for (let i = 1; i <= 30; i++) model.observe({ status: 200, latencyMs: 150, underLoad: true }, start + i * 1000);
    await check("clean sustained demand can rise above the old local cap, but stays bounded", () => {
      assert.ok(model.rate > 2, `rate stayed ${model.rate}`);
      assert.ok(model.rate <= 8, `rate exceeded probe ceiling: ${model.rate}`);
    });
    const idle = new QuotaModel({ startRps: 2, rateFloor: 0.25, rateCeiling: 8 });
    const idleStart = QuotaModel.mono();
    for (let i = 1; i <= 30; i++) idle.observe({ status: 200, underLoad: false }, idleStart + i * 1000);
    await check("idle clean responses do not ramp the rate without queued SEND demand", () => {
      assert.equal(idle.rate, 2);
    });
    const before429 = model.rate;
    model.penalize(1800, start + 31000);
    await check("429 halves only this key and applies server retry cooldown", () => {
      assert.ok(model.rate <= before429 * 0.5 + 1e-9, `rate ${model.rate}, before ${before429}`);
      assert.ok(model.snapshot(start + 31000).blockedForMs >= 1799);
    });
    const headerModel = new QuotaModel({ startRps: 2, rateCeiling: 8 });
    const headerNow = QuotaModel.mono();
    headerModel.learn({ "x-ratelimit-limit": "2", "x-ratelimit-remaining": "0",
      "x-ratelimit-reset": String((Date.now() + 5000) / 1000) }, headerNow);
    await check("server remaining/reset headers block exhausted window", () => {
      assert.equal(headerModel.check(headerNow).why, "window-exhausted");
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
