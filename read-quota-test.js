"use strict";

// 1.25.31 read quota (owner, final): dashboard Key A 4/s, Key B 2/s, 90%
// headroom, the keys are NOT added (account quota may be shared), 429 = 0.
const assert = require("node:assert/strict");
const { ReadDispatcher } = require("./read-dispatcher");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}
function keyPool() {
  const pool = [{ key: "key-a", inFlight: 0, blockedUntil: 0 }, { key: "key-b", inFlight: 0, blockedUntil: 0 }];
  return { pool, find(key) { return pool.find(e => e.key === key); },
    leaseKey(key) { const e = this.find(key); e.inFlight++; let d = false; return { release() { if (!d) { d = true; e.inFlight--; } } }; } };
}

(async () => {
  let t = 0;
  const d = new ReadDispatcher({ keys: keyPool(), now: () => t });
  await check("per-key ceilings match the dashboard at 90%", () => {
    assert.equal(+d.capFor("key-a").toFixed(2), 3.6);
    assert.equal(+d.capFor("key-b").toFixed(2), 1.8);
  });
  const granted = [];
  for (let i = 0; i < 300; i++) d.acquire({ priority: i % 3 === 0 ? 0 : 0.5 }).then(l => { granted.push({ at: t, key: l.key }); l.release(); });
  for (t = 0; t <= 10000; t += 10) { d.pump(); await new Promise(r => setImmediate(r)); }
  await check("total reads never exceed one key's dashboard rate (keys not added)", () => {
    assert.ok(granted.length <= 3.6 * 10 + 2, `granted ${granted.length} in 10 s`);
    assert.ok(granted.length >= 3.6 * 10 * 0.8, `underused: ${granted.length}`);
  });
  await check("no one-second window exceeds 4 reads", () => {
    for (const g of granted) {
      const n = granted.filter(x => x.at >= g.at && x.at < g.at + 1000).length;
      assert.ok(n <= 4, `window at ${g.at}: ${n}`);
    }
  });
  await check("Key B stays under its 2/s dashboard rate", () => {
    const b = granted.filter(x => x.key === "key-b").length;
    assert.ok(b <= 1.8 * 10 + 2, `key-b ${b}`);
  });
  await check("AIMD growth never raises a key above its ceiling", () => {
    const s = d.state("key-b");
    for (let i = 0; i < 40; i++) { s.good = 100; s.changed = -1e9; d.report("key-b", 200); }
    assert.ok(s.rps <= 1.8 + 1e-9, `rps ${s.rps}`);
  });
  await check("a 429 halves the key and is counted", () => {
    const before = d.state("key-a").rps;
    d.report("key-a", 429, 100);
    assert.ok(d.state("key-a").rps <= before / 2 + 1e-9);
    assert.equal(d.stats().global.rateLimitedTotal, 1);
  });
  await check("polling class sorts behind P0 authority reads", async () => {
    let tt = 0;
    const dd = new ReadDispatcher({ keys: keyPool(), now: () => tt });
    dd.global.tokens = 0;
    const order = [];
    const a = dd.acquire({ priority: 0.25 }).then(l => { order.push("poll"); l.release(); });
    const b = dd.acquire({ priority: 0 }).then(l => { order.push("p0"); l.release(); });
    tt = 1000; dd.pump(); await new Promise(r => setImmediate(r));
    tt = 2000; dd.pump(); await Promise.all([a, b]);
    assert.equal(order[0], "p0");
  });
  process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
  if (failed) process.exitCode = 1;
  process.exit();
})();
