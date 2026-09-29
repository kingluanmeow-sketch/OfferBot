"use strict";

const assert = require("assert");
const { ReadDispatcher } = require("./read-dispatcher");

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function keyPool() {
  const pool = [{ key: "key-a", inFlight: 0, blockedUntil: 0 }, { key: "key-b", inFlight: 0, blockedUntil: 0 }];
  return {
    pool,
    find(key) { return pool.find(entry => entry.key === key); },
    leaseKey(key) {
      const entry = this.find(key);
      entry.inFlight++;
      let done = false;
      return { release() { if (!done) { done = true; entry.inFlight--; } } };
    }
  };
}

(async () => {
  const keys = keyPool();
  const dispatcher = new ReadDispatcher({ keys, maxRps: 20, maxActive: 8, globalRps: 1000, globalBurst: 1000 });

  const roleKeys = keyPool();
  const roleDispatcher = new ReadDispatcher({ keys: roleKeys, maxRps: 20, maxActive: 8, globalRps: 1000, globalBurst: 1000 });
  const backgroundPreferred = await roleDispatcher.acquire({ priority: 2 });
  assert.strictEqual(backgroundPreferred.key, "key-b", "background phải ưu tiên Key 2");
  backgroundPreferred.release();
  const priorityKeys = keyPool();
  const priorityDispatcher = new ReadDispatcher({ keys: priorityKeys, maxRps: 20, maxActive: 8, globalRps: 1000, globalBurst: 1000 });
  const p0Preferred = await priorityDispatcher.acquire({ priority: 0 });
  assert.strictEqual(p0Preferred.key, "key-a", "P0 phải ưu tiên Key 1");
  p0Preferred.release();

  const first = await dispatcher.acquire({ priority: 2 });
  const urgent = await dispatcher.acquire({ priority: 0 });
  assert.notStrictEqual(first.key, urgent.key, "P0 phải dùng key còn rảnh");
  first.release(); urgent.release();
  assert.strictEqual(keys.pool.reduce((n, x) => n + x.inFlight, 0), 0, "lease phải release đủ");

  const ctrl = new AbortController();
  const cancelled = dispatcher.acquire({ priority: 2, signal: ctrl.signal, apiKey: "key-a" });
  ctrl.abort();
  await assert.rejects(cancelled, error => error.name === "AbortError");

  const fallbackKeys = keyPool();
  const fallbackDispatcher = new ReadDispatcher({ keys: fallbackKeys, maxRps: 20, maxActive: 8, globalRps: 1000, globalBurst: 1000 });
  fallbackDispatcher.report("key-a", 429, 120);
  const p0Fallback = await fallbackDispatcher.acquire({ priority: 0 });
  assert.strictEqual(p0Fallback.key, "key-b", "Key 2 phải hỗ trợ chéo khi Key 1 cooldown");
  p0Fallback.release();
  const snapshot = fallbackDispatcher.stats();
  assert.ok(snapshot.keys.some(x => x.cooldownMs >= 1900), "429 phải cooldown nhanh");

  await sleep(5);
  assert.strictEqual(dispatcher.stats().queued, 0, "không để job huỷ trong queue");
  process.stdout.write("READ DISPATCHER: PASS\n");
})().catch(error => {
  process.stderr.write((error && error.stack) || String(error));
  process.exit(1);
});
