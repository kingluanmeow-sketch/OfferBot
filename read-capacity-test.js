"use strict";

// 1.25.31 live: after restart, own-authority reads (priority 1, gate SEND)
// waited 40–60 s. Background jobs held 5 of 6 RecoveryPlane workers while
// they waited for background dispatcher slots, and network errors/aborts
// (status 0) halved per-key read rates with no 429 at all.
const assert = require("node:assert/strict");
const { RecoveryPlane } = require("./offer-item-v2/recovery-plane");
const { ReadDispatcher } = require("./read-dispatcher");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}

(async () => {
  {
    const plane = new RecoveryPlane({ workers: 6, readsPerSecond: 100 });
    const hang = () => new Promise(() => {});
    for (let i = 0; i < 20; i++) plane.push({ kind: "topic-gap", priority: 2, run: hang });
    let started = 0;
    for (let i = 0; i < 4; i++) plane.push({ kind: "pre-post-own", priority: 1, run: () => { started++; return hang(); } });
    await new Promise(r => setTimeout(r, 20));
    await check("background recovery cannot occupy more than half the workers", () => {
      assert.ok(plane.background <= 3, `background=${plane.background}`);
    });
    await check("several authority reads run concurrently while background is stuck", () => {
      assert.ok(started >= 3, `authority started=${started}`);
    });
    plane.stop();
  }
  {
    const pool = [{ key: "a", inFlight: 0, blockedUntil: 0 }, { key: "b", inFlight: 0, blockedUntil: 0 }];
    const keys = { pool, find: k => pool.find(e => e.key === k), leaseKey: () => ({ release() {} }) };
    const d = new ReadDispatcher({ keys });
    const before = d.state("a").rps;
    d.report("a", 0);
    await check("a network error or aborted read does not halve the key's read rate", () => {
      assert.equal(d.state("a").rps, before);
      assert.equal(Math.round(d.stats().keys.find(k => k.capRps > 2).cooldownMs), 0);
    });
    d.report("a", 429, 100);
    await check("a real 429 still halves the rate and cools the key down", () => {
      assert.ok(d.state("a").rps < before);
    });
  }
  process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
  if (failed) process.exitCode = 1;
  process.exit();
})();
