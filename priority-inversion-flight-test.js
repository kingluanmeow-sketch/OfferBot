"use strict";

/**
 * RED/GREEN: a P2 (background) request that becomes the SingleFlight leader
 * for a URL must not silently swallow the priority of an INITIAL request that
 * joins the SAME flight microseconds later.
 *
 * Live 2026-09-30 (cold-start capture, 309 INITIAL reads): 32/309 reads had
 * at least one acquire() call tagged priority=2 instead of -1 -- some reads
 * ENTIRELY priority=2 across every page. Root cause: opensea.js:360-363
 * builds the SingleFlight key from [domain, url, params, kind] only --
 * priority is NOT part of it. A joiner just awaits the leader's already
 * in-flight promise; the leader's `requestOnce({...options, signal})` closure
 * was built from the LEADER's own `options`, so the joiner's priority never
 * reaches readDispatcher.acquire at all.
 *
 * This test reproduces the mechanism directly against SingleFlight (the
 * actual coalescing primitive), not the full opensea.js stack, to keep it
 * fast and dependency-free.
 */

const assert = require("assert");
const { SingleFlight } = require("./single-flight");

let passed = 0, failed = 0;
function check(name, ok, detail = "") {
  if (ok) { passed++; process.stdout.write(`PASS ${name}\n`); }
  else { failed++; process.stdout.write(`FAIL ${name}${detail ? `: ${detail}` : ""}\n`); }
}

// Mirrors opensea.js's request(): key from [domain, url, params, kind] only.
function keyOf(domain, url, params, kind) { return JSON.stringify([domain, url, params || {}, kind]); }

async function testCurrentBehavior_RED() {
  const flights = new SingleFlight();
  const dispatchedPriorities = [];
  // Simulates requestOnce()'s call into readDispatcher.acquire({priority}).
  function requestOnceLike(options) {
    dispatchedPriorities.push(options.priority);
    return new Promise(resolve => setTimeout(() => resolve({ ok: true, priority: options.priority }), 30));
  }

  const key = keyOf("dom1", "/offers/collection/x/nfts/1", {}, "READ");

  // Leader: a P2 background read arrives first.
  const leaderP = flights.run(key, signal => requestOnceLike({ priority: 2, signal }));
  await new Promise(r => setTimeout(r, 2)); // let the leader's promise start

  // Joiner: an INITIAL (-1) read arrives microseconds later for the SAME key.
  const joinerP = flights.run(key, signal => requestOnceLike({ priority: -1, signal }));

  const [leaderRes, joinerRes] = await Promise.all([leaderP, joinerP]);

  check("only ONE real dispatch happened (dedupe preserved)",
    dispatchedPriorities.length === 1, `dispatches=${dispatchedPriorities.length}`);
  check("[CURRENT/RED] the single dispatch used the LEADER's priority (2), not the joiner's (-1)",
    dispatchedPriorities[0] === 2, `dispatchedPriorities=${JSON.stringify(dispatchedPriorities)}`);
  check("[CURRENT/RED] the INITIAL joiner's own priority never reached the dispatcher (inversion confirmed)",
    !dispatchedPriorities.includes(-1), `dispatchedPriorities=${JSON.stringify(dispatchedPriorities)}`);
  return dispatchedPriorities;
}

// GREEN: same mechanism as opensea.js's fixed request() -- key includes a
// priority TIER (urgent = INITIAL/P0/P1, bg = P2/P3), not the exact key
// import (keeps this test dependency-free / fast), but the identical logic.
const URGENT_PRIORITY = 1; // rate-limiter.js PRIORITY.P1
function tierOf(priority) { return priority > URGENT_PRIORITY ? "bg" : "urgent"; }
function keyOfTiered(domain, url, params, kind, priority) {
  return JSON.stringify([domain, url, params || {}, kind, tierOf(priority)]);
}

async function testFixedBehavior_GREEN() {
  const flights = new SingleFlight();
  const dispatchedPriorities = [];
  function requestOnceLike(options) {
    dispatchedPriorities.push(options.priority);
    return new Promise(resolve => setTimeout(() => resolve({ ok: true, priority: options.priority }), 30));
  }

  // Same URL, but the key now carries the tier -- P2 (bg) and INITIAL (urgent)
  // land in DIFFERENT flights.
  const leaderKey = keyOfTiered("dom1", "/offers/collection/x/nfts/1", {}, "READ", 2);
  const joinerKey = keyOfTiered("dom1", "/offers/collection/x/nfts/1", {}, "READ", -1);
  check("[FIX] P2 and INITIAL for the same URL now get DIFFERENT flight keys",
    leaderKey !== joinerKey, `leaderKey=${leaderKey} joinerKey=${joinerKey}`);

  const leaderP = flights.run(leaderKey, signal => requestOnceLike({ priority: 2, signal }));
  await new Promise(r => setTimeout(r, 2));
  const joinerP = flights.run(joinerKey, signal => requestOnceLike({ priority: -1, signal }));
  await Promise.all([leaderP, joinerP]);

  check("[FIX] the INITIAL request now dispatches with its OWN priority (-1)",
    dispatchedPriorities.includes(-1), `dispatchedPriorities=${JSON.stringify(dispatchedPriorities)}`);
  check("[FIX] the P2 request still dispatches with its OWN priority (2)",
    dispatchedPriorities.includes(2), `dispatchedPriorities=${JSON.stringify(dispatchedPriorities)}`);
  check("[FIX] exactly 2 real dispatches for the 2 priority tiers (bounded, no storm)",
    dispatchedPriorities.length === 2, `dispatches=${dispatchedPriorities.length}`);

  // Dedupe MUST still hold WITHIN a tier: two concurrent P2 requests for the
  // same URL must still coalesce into one real dispatch.
  const dispatched2 = [];
  function counting(options) { dispatched2.push(options.priority); return new Promise(r => setTimeout(() => r({ ok: true }), 30)); }
  const kA = keyOfTiered("dom1", "/offers/collection/y/nfts/2", {}, "READ", 2);
  const kB = keyOfTiered("dom1", "/offers/collection/y/nfts/2", {}, "READ", 2);
  const pA = flights.run(kA, signal => counting({ priority: 2, signal }));
  const pB = flights.run(kB, signal => counting({ priority: 2, signal }));
  await Promise.all([pA, pB]);
  check("[FIX] dedupe still holds within the same tier (two P2 for the same URL -> 1 dispatch)",
    dispatched2.length === 1, `dispatches=${dispatched2.length}`);
}

(async () => {
  await testCurrentBehavior_RED();
  await testFixedBehavior_GREEN();
  process.stdout.write(`\n${passed}/${passed + failed} checks passed\n`);
  if (failed) process.exitCode = 1;
})().catch(e => { console.error("CRASHED:", e.stack || e); process.exitCode = 1; });
