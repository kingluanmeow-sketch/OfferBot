"use strict";

// 1.25.32 reads: the two keys belong to two DIFFERENT OpenSea accounts
// (owner-confirmed), so their read quotas are independent pools.
// Dashboard: Key 1 4/s, Key 2 2/s. Target at 90%: 3.6/s + 1.8/s = 5.4/s.
// 1.25.31 still had one global bucket capping the total at 3.6/s.
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
const maxInWindow = (granted, key) => {
  const list = granted.filter(g => !key || g.key === key);
  let max = 0;
  for (const g of list) max = Math.max(max, list.filter(x => x.at >= g.at && x.at < g.at + 1000).length);
  return max;
};
async function saturate(d, clock, ms, priority = i => (i % 3 === 0 ? 0 : 0.5)) {
  const granted = [];
  for (let i = 0; i < 400; i++) d.acquire({ priority: priority(i) }).then(l => { granted.push({ at: clock.t, key: l.key }); l.release(); });
  const end = clock.t + ms;
  for (; clock.t <= end; clock.t += 10) { d.pump(); await new Promise(r => setImmediate(r)); }
  return granted;
}

(async () => {
  {
    const clock = { t: 0 };
    const d = new ReadDispatcher({ keys: keyPool(), now: () => clock.t });
    await check("per-key ceilings match each account's dashboard at 90%", () => {
      assert.equal(+d.capFor("key-a").toFixed(2), 3.6);
      assert.equal(+d.capFor("key-b").toFixed(2), 1.8);
    });
    const granted = await saturate(d, clock, 10000);
    await check("aggregate reads reach ~5.4/s: no global 3.6/s cap", () => {
      assert.ok(granted.length >= 5.4 * 10 * 0.85, `aggregate ${granted.length} in 10 s (a 3.6/s cap allows ~37)`);
      assert.ok(granted.length <= 5.4 * 10 + 3, `too many: ${granted.length}`);
    });
    await check("no key exceeds its own dashboard rate in any one-second window", () => {
      assert.ok(maxInWindow(granted, "key-a") <= 4, `key-a window ${maxInWindow(granted, "key-a")}`);
      assert.ok(maxInWindow(granted, "key-b") <= 2, `key-b window ${maxInWindow(granted, "key-b")}`);
    });
    await check("both accounts carry load", () => {
      assert.ok(granted.filter(g => g.key === "key-a").length >= 30);
      assert.ok(granted.filter(g => g.key === "key-b").length >= 15);
    });
  }

  for (const [hit, other, otherMin] of [["key-a", "key-b", 14], ["key-b", "key-a", 28]]) {
    const clock = { t: 0 };
    const d = new ReadDispatcher({ keys: keyPool(), now: () => clock.t });
    const otherRate = d.state(other).rps;
    d.report(hit, 429, 3000);
    const granted = await saturate(d, clock, 10000, () => 0);
    await check(`${hit} 429: only ${hit} backs off, ${other} keeps its full rate`, () => {
      assert.ok(d.state(hit).rps < d.capFor(hit), "hit key must slow down");
      assert.equal(d.state(other).rps, otherRate);
      const o = granted.filter(g => g.key === other).length;
      assert.ok(o >= otherMin, `${other} granted only ${o} in 10 s`);
    });
  }

  await check("AIMD growth never raises a key above its ceiling", () => {
    const d = new ReadDispatcher({ keys: keyPool(), now: () => 0 });
    const s = d.state("key-b");
    for (let i = 0; i < 40; i++) { s.good = 100; s.changed = -1e9; d.report("key-b", 200); }
    assert.ok(s.rps <= 1.8 + 1e-9, `rps ${s.rps}`);
  });

  await check("P0 is served before polling and background when capacity is short", async () => {
    let tt = 0;
    const dd = new ReadDispatcher({ keys: keyPool(), now: () => tt });
    dd.state("key-a").tokens = 0; dd.state("key-b").tokens = 0;
    const order = [];
    const jobs = [
      dd.acquire({ priority: 2 }).then(l => { order.push("background"); l.release(); }),
      dd.acquire({ priority: 0.25 }).then(l => { order.push("poll"); l.release(); }),
      dd.acquire({ priority: 0 }).then(l => { order.push("p0"); l.release(); })
    ];
    for (tt = 0; tt <= 3000; tt += 50) { dd.pump(); await new Promise(r => setImmediate(r)); }
    await Promise.all(jobs);
    assert.equal(order[0], "p0");
    assert.equal(order[order.length - 1], "background");
  });

  await check("metrics: per-key grants/60s, aggregate, utilization, P0 wait, 429 per key", () => {
    const clock = { t: 0 };
    const d = new ReadDispatcher({ keys: keyPool(), now: () => clock.t });
    d.acquire({ priority: 0 }).then(l => l.release());
    d.report("key-a", 429, 100);
    const st = d.stats();
    assert.equal(+st.global.ceilingRps.toFixed(2), 5.4);
    assert.ok(st.global.used60s >= 1);
    assert.ok(st.keys.every(k => typeof k.grants60s === "number" && k.capRps > 0));
    assert.ok(st.keys.some(k => k.rateLimited === 1));
    assert.ok(Number.isFinite(st.realtimeWaitMs.p95));
  });

  process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
  if (failed) process.exitCode = 1;
  process.exit();
})();
