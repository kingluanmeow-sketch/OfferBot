"use strict";

/**
 * Audit: hai OpenSea API key có thực sự là hai bucket ĐỘC LẬP không, và
 * broker có chọn đúng key sẵn sàng SỚM NHẤT không -- bằng chứng là
 * `X-RateLimit-Limit/Remaining/Reset` + `Retry-After` thật từ response
 * headers, và trạng thái 429/cooldown của một key, không phải suy luận.
 *
 * KHÔNG kiểm bằng cách gọi thật OpenSea -- đây là unit test trên
 * `ApiKeyManager`/`retryDelayFromHeaders` (rate-limiter.js), đúng class
 * `opensea.js` dùng thật (`apiKeys.reportRateLimited(key, retryDelayFromHeaders(headers))`).
 */

const assert = require("node:assert/strict");
const { ApiKeyManager, retryDelayFromHeaders } = require("./rate-limiter");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}

const KEY1 = "test-key-one-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const KEY2 = "test-key-two-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

(async () => {

// ---- Case 1: retryDelayFromHeaders reads REAL header names, in the documented priority ----
await check("retryDelayFromHeaders prefers Retry-After over X-RateLimit-Reset, exactly as OpenSea sends it", async () => {
  // OpenSea measured live: "retry-after: 1" (seconds).
  const ms1 = retryDelayFromHeaders({ "retry-after": "1" });
  assert.equal(ms1, 1000, "retry-after is in SECONDS and must convert to ms");

  // No retry-after, but x-ratelimit-reset (unix seconds) 5s in the future.
  const resetAt = Math.floor(Date.now() / 1000) + 5;
  const ms2 = retryDelayFromHeaders({ "x-ratelimit-reset": String(resetAt) });
  assert.ok(ms2 > 3000 && ms2 <= 5000, `expected ~5000ms from x-ratelimit-reset, got ${ms2}`);

  // Both present: retry-after wins (server's own explicit instruction).
  const ms3 = retryDelayFromHeaders({ "retry-after": "2", "x-ratelimit-reset": String(resetAt) });
  assert.equal(ms3, 2000, "retry-after must take priority over x-ratelimit-reset when both are present");

  // Neither present: falls back, never throws, never returns 0/negative.
  const ms4 = retryDelayFromHeaders({});
  assert.equal(ms4, 2000, "default fallback when the server says nothing");
});

// ---- Case 2: two keys are two INDEPENDENT buckets -- a 429 on key A must never block key B ----
await check("independent buckets: reportRateLimited on key A never touches key B's cooldown/health", async () => {
  const mgr = new ApiKeyManager();
  mgr.configure({ apiKey: KEY1, apiKey2: KEY2 });
  assert.equal(mgr.pool.length, 2, "both keys must be in the pool");

  const headers = { "retry-after": "30" }; // real OpenSea shape, 30s cooldown
  const wait = retryDelayFromHeaders(headers);
  const ok = mgr.reportRateLimited(KEY1, wait);
  assert.equal(ok, true, "at least one key (KEY2) must still be usable");

  const entry1 = mgr.find(KEY1), entry2 = mgr.find(KEY2);
  assert.ok(entry1.blockedUntil > Date.now(), "KEY1 must be in cooldown");
  assert.equal(entry2.blockedUntil, 0, "KEY2's cooldown must be completely untouched by KEY1's 429");

  const healthyNow = mgr.healthy(Date.now());
  assert.equal(healthyNow.length, 1);
  assert.equal(healthyNow[0].key, KEY2, "the healthy pool must report exactly KEY2 while KEY1 cools down");
});

// ---- Case 3: current() picks the EARLIEST-ready key, not a hardcoded role ----
await check("earliest-ready selection: current() returns whichever key is actually unblocked, not always 'key 1'", async () => {
  const mgr = new ApiKeyManager();
  mgr.configure({ apiKey: KEY1, apiKey2: KEY2 });

  // Block KEY1 (simulating a real 429 on it specifically).
  mgr.reportRateLimited(KEY1, 30000);
  const picked = mgr.current(Date.now());
  assert.equal(picked, KEY2, "with KEY1 blocked, current() must serve KEY2 -- no hardcoded key-1 preference");

  // Now block KEY2 too, but for a SHORTER cooldown than KEY1's remaining time --
  // when EVERYTHING is blocked, current() must return whichever frees up soonest.
  mgr.reportRateLimited(KEY2, 1000);
  const soonest = mgr.current(Date.now());
  assert.equal(soonest, KEY2, "when both keys are blocked, the one with the EARLIEST blockedUntil must be returned");
});

// ---- Case 4: when both keys are healthy, the LESS LOADED one wins (not role, not insertion order) ----
await check("load-aware selection: with both keys healthy, current() prefers the key with fewer in-flight requests", async () => {
  const mgr = new ApiKeyManager();
  mgr.configure({ apiKey: KEY1, apiKey2: KEY2 });
  const e1 = mgr.find(KEY1), e2 = mgr.find(KEY2);
  e1.inFlight = 3; // KEY1 is busy
  e2.inFlight = 0; // KEY2 is free
  const picked = mgr.current(Date.now());
  assert.equal(picked, KEY2, "the less-loaded key must win regardless of which one is 'key 1'");
});

// ---- Case 5: reportSuccess clears cooldown -- recovery is real, not permanent exile ----
await check("reportSuccess clears a key's cooldown once it demonstrably works again", async () => {
  const mgr = new ApiKeyManager();
  mgr.configure({ apiKey: KEY1, apiKey2: KEY2 });
  mgr.reportRateLimited(KEY1, 1);
  await new Promise(r => setTimeout(r, 5)); // let the 1ms cooldown actually elapse
  mgr.reportSuccess(KEY1);
  const entry1 = mgr.find(KEY1);
  assert.equal(entry1.blockedUntil, 0, "a demonstrated success after cooldown elapsed must clear blockedUntil");
});

// ---- Case 6: a timeout/transport failure on one key must not escalate to the other ----
await check("reportFailure (timeout/transport error) on one key is isolated from the other", async () => {
  const mgr = new ApiKeyManager();
  mgr.configure({ apiKey: KEY1, apiKey2: KEY2 });
  mgr.reportFailure(KEY1);
  const entry1 = mgr.find(KEY1), entry2 = mgr.find(KEY2);
  assert.ok(entry1.blockedUntil > Date.now(), "KEY1 must be briefly cooled down after a failure");
  assert.equal(entry2.blockedUntil, 0, "KEY2 must be completely unaffected");
});

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
