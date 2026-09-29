"use strict";

// 1.25.31: while the Stream is DEGRADED, competitor changes are detected by a
// bounded adaptive /best poller instead of a flat 60 s full-read sweep.
// Live 2026-09-29 (Kagami #522): competitor 0.0195 at 16:43:27, next REST read
// of that row at 16:44:37 — 70 s blind while Decision→POST took <1 s.
const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");

let passed = 0;
let failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}
const SELF = "0xe86c742415b39aae683c4b020da75e07956e3413";
const COMP = "0x3333333333333333333333333333333333333333";
const tick = () => new Promise(r => setImmediate(r));

function makeEngine(n = 4, answer = () => null) {
  const calls = [];
  const adapter = {
    chain: "ethereum",
    async fetchBest() { return { empty: true, orders: [] }; },
    async fetchBestQuick(row, opts) { calls.push({ tokenId: row.tokenId, priority: opts && opts.priority }); return answer(row); }
  };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  engine.builder = { address: SELF };
  engine.book.setSelfAddress(SELF);
  engine.attachStreamHealth(() => "DEGRADED");
  engine.degraded = true;
  const merged = [];
  engine.mergeBest = (key, best, generation, opts) => merged.push({ key, best, opts });
  const keys = [];
  for (let i = 0; i < n; i++) {
    const contract = "0x1111111111111111111111111111111111111111";
    const key = `ethereum:${contract}:${i}`;
    const row = { key, tokenId: String(i), contract, collectionSlug: "alpha", running: true, minPrice: 0.001, maxPrice: 1, step: 0.0001 };
    engine.rows.set(key, row);
    const book = engine.book.add({ key, chain: "ethereum", contract, tokenId: String(i) });
    book.collectionSlug = "alpha";
    book.hydratedAt = Date.now(); book.lastFullReadAt = Date.now();
    keys.push(key);
  }
  return { engine, calls, merged, keys };
}

(async () => {
  {
    const { engine, calls, merged } = makeEngine(4, row => ({ contract: "x", tokenId: row.tokenId, price: 0.0195, maker: COMP,
      orderHash: `0xc${row.tokenId}`, kind: "item", endTime: 0, orders: [{ orderHash: `0xc${row.tokenId}`, price: 0.0195, maker: COMP, kind: "item", endTime: 0, quantity: 1 }] }));
    await check("DEGRADED starts an adaptive /best poller", () => {
      assert.equal(typeof engine.degradedPollTick, "function");
      engine.degradedPollTick(Date.now());
    });
    await tick(); await tick();
    await check("a competitor above Mine is merged into the book immediately (Decision/SEND follows)", () => {
      assert.ok(calls.length >= 1, "no /best poll issued");
      assert.ok(merged.length >= 1, "competitor result not merged");
      assert.equal(merged[0].opts.quick, true);
      assert.equal(merged[0].best.price, 0.0195);
    });
    await check("a row that lost top becomes HOT", () => {
      assert.ok((engine.hotUntil.get(merged[0].key) || 0) > Date.now());
    });
    engine.stopDegradedPoll();
  }

  {
    const { engine, calls, merged, keys } = makeEngine(3, row => ({ contract: "x", tokenId: row.tokenId, price: 0.02, maker: SELF,
      orderHash: `0xs${row.tokenId}`, kind: "item", endTime: 0, orders: [{ orderHash: `0xs${row.tokenId}`, price: 0.02, maker: SELF, kind: "item", endTime: 0, quantity: 1 }] }));
    engine.degradedPollTick(Date.now());
    await tick(); await tick();
    await check("OpenSea says Mine is top: no merge, no full-read escalation", () => {
      assert.ok(calls.length >= 1);
      assert.equal(merged.length, 0);
      assert.equal(engine.pendingReads.size, 0);
    });
    engine.stopDegradedPoll();
  }

  {
    let release;
    const gate = new Promise(r => { release = r; });
    const { engine, calls } = makeEngine(6, () => gate.then(() => null));
    const t = Date.now();
    for (let i = 0; i < 20; i++) engine.degradedPollTick(t + i * 1000);
    await check("poller is bounded: at most 5 concurrent /best requests", () => {
      assert.equal(calls.length, 5);
    });
    release();
    await tick(); await tick();
    engine.stopDegradedPoll();
  }

  {
    const { engine, calls, keys } = makeEngine(3, () => null);
    const t = Date.now();
    engine.hotUntil.set(keys[2], t + 60000);
    engine.pollAt.set(keys[0], t - 60000);
    engine.pollAt.set(keys[1], t - 60000);
    engine.pollAt.set(keys[2], t - 3000);
    engine.degradedPollTick(t);
    await check("a due HOT row is polled before older quiet rows", () => {
      assert.equal(calls[0].tokenId, "2");
    });
    await tick();
    const before = calls.length;
    engine.pollAt.set(keys[0], t); engine.pollAt.set(keys[1], t); engine.pollAt.set(keys[2], t);
    engine.degradedPollTick(t + 1000);
    await check("rows are not re-polled before their interval (no storm)", () => {
      assert.equal(calls.length, before);
    });
    engine.stopDegradedPoll();
  }

  {
    // Live 1.25.31 first run: all 106 rows had a background full read queued
    // after the outage edge; the poller skipped them and SEND waited behind
    // the whole background queue for topic authority.
    const { engine, calls, keys } = makeEngine(2, row => ({ contract: "x", tokenId: row.tokenId, price: 0.0015, maker: COMP,
      orderHash: "0xknown", kind: "item", endTime: 0, orders: [{ orderHash: "0xknown", price: 0.0015, maker: COMP, kind: "item", endTime: 0, quantity: 1 }] }));
    const t = Date.now();
    for (const key of keys) {
      engine.hydrating.add(key);
      engine.topicRepairAt.set(key, t - 1000);
      engine.book.get(key).lastFullReadAt = t - 5000;
    }
    await check("topic gate is closed before any post-outage authority", () => {
      assert.equal(engine.topicReady(engine.rows.get(keys[0]), engine.book.get(keys[0])), false);
    });
    engine.degradedPollTick(t);
    await tick(); await tick();
    await check("a background read in the queue does not block the /best poll", () => {
      assert.ok(calls.length >= 1);
    });
    await check("a /best answer after the outage edge opens the topic gate in DEGRADED", () => {
      const key = keys.find(k => engine.rows.get(k).tokenId === calls[0].tokenId);
      assert.equal(engine.topicReady(engine.rows.get(key), engine.book.get(key)), true);
    });
    engine.stopDegradedPoll();
  }

  {
    // Live: /best lagged ~30 s behind the NFT offer list (competitor 0.069
    // visible in the list, /best still 0.0688). Polling must read the list head.
    const { engine, calls } = makeEngine(1, () => null);
    const heads = [];
    engine.adapter.fetchBestHead = async row => { heads.push(row.tokenId); return null; };
    engine.degradedPollTick(Date.now());
    await tick(); await tick();
    await check("poller reads the fresh offer-list head, not the lagging /best", () => {
      assert.equal(heads.length, 1);
      assert.equal(calls.length, 0);
    });
    engine.stopDegradedPoll();
  }

  {
    const { engine, calls } = makeEngine(2, () => null);
    engine.degraded = false;
    engine.degradedPollTick(Date.now());
    await check("healthy Stream: poller issues no REST", () => assert.equal(calls.length, 0));
  }

  process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
  if (failed) process.exitCode = 1;
})();
