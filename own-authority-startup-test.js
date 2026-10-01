"use strict";

/**
 * RED/GREEN: own-authority at Start with 100+ NFTs (1.25.38 root-cause fix).
 *
 * Confirmed production symptom: with 100+ tracked NFTs, Decision fires
 * immediately from Stream, but SEND sits blocked on `own-state-unknown` for
 * 10-75s because `deferForOwnSync` issued ONE DEDICATED per-token REST read
 * per cold row instead of using the already-wired paginated wallet snapshot
 * (`resyncOwnOrders` / `/account/{address}/offers`, 1-6 requests for 100+
 * NFTs). The fix: a cold row (not POST-ambiguous) joins the shared wallet
 * snapshot; only a row the snapshot genuinely doesn't cover after a bounded
 * grace period falls back to its own per-token read.
 */

const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const { INTENT } = require("./offer-item-v2/intent-store");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}
const flush = () => new Promise(r => setImmediate(r));

const ROWS = 120;

function buildColdEngine({ resyncOwnOrders } = {}) {
  const fetchBestCalls = [];
  const adapter = {
    chain: "ethereum",
    async fetchBest(row) { fetchBestCalls.push(row.key); return { empty: true, complete: true }; },
    resyncOwnOrders: resyncOwnOrders || (async () => ({ orders: [], complete: true, covered: [] }))
  };
  adapter.fetchBestCalls = fetchBestCalls;
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  // queueOwnResync requires a signer/owner address; deferForOwnSync's
  // wallet-snapshot path is a no-op without it.
  engine.builder = { address: "0x9999999999999999999999999999999999999999" };
  const jobs = [];
  engine.recovery.push = job => { jobs.push(job); if (job.run) job.run({ signal: { aborted: false } }).then(r => job.onResult && job.onResult(r, engine.recovery.generation)); return true; };

  const keys = [];
  for (let i = 0; i < ROWS; i++) {
    const contract = "0x1111111111111111111111111111111111111111";
    const tokenId = String(i);
    const key = `ethereum:${contract}:${tokenId}`;
    keys.push(key);
    const row = { key, running: true, tokenId, contract, collectionSlug: "alpha" };
    engine.rows.set(key, row);
    const book = engine.book.add({ key, chain: "ethereum", contract, tokenId });
    // Cold: never reconciled, never synced -- exactly the Start state.
    engine.intents.set(key, { target: 0.02, best: 0.0199, mine: 0, generation: book.generation });
    engine.intents.setState(key, INTENT.WAITING, { dependency: "own" });
  }
  return { engine, jobs, keys };
}

(async () => {

// ---- Case 1: 120 cold rows at Start share ONE wallet snapshot job --------
{
  const { engine, jobs, keys } = buildColdEngine();
  for (const key of keys) {
    const row = engine.rows.get(key);
    engine.deferForOwnSync(key, row, `trace-${key}`);
  }
  await check("120 cold rows never call fetchBest (no per-token REST read)", () => {
    assert.deepEqual(engine.adapter.fetchBestCalls, []);
  });
  await check("120 cold rows produce exactly ONE recovery job (the shared wallet snapshot), not 120", () => {
    const ownJobs = jobs.filter(j => String(j.kind || "").startsWith("own-"));
    assert.equal(ownJobs.length, 1);
  });
  await check("that one job is P1 (reason pre-post-own), not starved at P3", () => {
    const ownJobs = jobs.filter(j => String(j.kind || "").startsWith("own-"));
    assert.equal(ownJobs[0].priority, 1);
  });
  for (const timer of engine.ownWaitTimers.values()) clearTimeout(timer);
  engine.recovery.stop();
}

// ---- Case 2: a complete wallet snapshot unlocks every row at once --------
{
  const { engine, keys } = buildColdEngine({
    resyncOwnOrders: async () => ({ orders: [], complete: true, covered: [] })
  });
  for (const key of keys) {
    const row = engine.rows.get(key);
    engine.deferForOwnSync(key, row, `trace-${key}`);
  }
  // The mocked recovery.push resolves its job via a real Promise chain
  // (job.run().then(onResult)), same as production RecoveryPlane -- flush
  // microtasks/macrotasks so mergeResync has actually run before asserting.
  await flush(); await flush();
  await check("after the complete snapshot resolves, every one of 120 rows is ownAuthoritative", () => {
    for (const key of keys) assert.equal(engine.ownAuthoritative(key), true, `row ${key} not authoritative`);
  });
  for (const timer of engine.ownWaitTimers.values()) clearTimeout(timer);
  engine.recovery.stop();
}

// ---- Case 3: POST-ambiguous row always gets its own read, snapshot or not
{
  const { engine } = buildColdEngine();
  const key = "ethereum:0x2222222222222222222222222222222222222222:1";
  const row = { key, running: true, tokenId: "1", contract: "0x2222222222222222222222222222222222222222", collectionSlug: "beta" };
  engine.rows.set(key, row);
  const book = engine.book.add({ key, chain: "ethereum", contract: row.contract, tokenId: "1" });
  // Row already has reconciled history (ambiguous only because of its OWN
  // just-posted, unconfirmed order) -- this must take the per-token path.
  engine.ownSyncAt = Date.now() - 5000;
  book.ownReconciledAt = Date.now() - 5000;
  book.ownUnknownAt = Date.now() - 100;
  engine.intents.set(key, { target: 0.02, best: 0.0199, mine: 0, generation: book.generation });
  engine.intents.setState(key, INTENT.WAITING, { dependency: "post-reconcile" });

  let perTokenReadCalled = false;
  engine.queueRead = (r, opts) => { if (opts.reason === "post-uncertain") perTokenReadCalled = true; return true; };
  engine.deferForOwnSync(key, row, "trace-ambiguous");
  await check("a POST-ambiguous row takes the per-token read path immediately, not the wallet snapshot", () => {
    assert.equal(perTokenReadCalled, true);
  });
  engine.recovery.stop();
}

// ---- Case 4: row the snapshot doesn't cover falls back after grace -------
{
  const original = global.setTimeout;
  const timers = [];
  global.setTimeout = (fn, ms) => { const t = { fn, ms, unref() {} }; timers.push(t); return t; };
  try {
    const { engine, keys } = buildColdEngine({
      // Snapshot "completes" but never covers this row (e.g. a large/looping
      // wallet whose collection-drain tier gave up on this one).
      resyncOwnOrders: async () => ({ orders: [], complete: false, covered: [] })
    });
    const key = keys[0];
    const row = engine.rows.get(key);
    let perTokenReadCalled = false;
    const originalQueueRead = engine.queueRead.bind(engine);
    engine.queueRead = (r, opts) => {
      if (opts.reason === "pre-post-own" && r.key === key) perTokenReadCalled = true;
      return originalQueueRead(r, opts);
    };
    engine.deferForOwnSync(key, row, "trace-uncovered");
    await check("grace timer is armed for the uncovered row, not an immediate per-token read", () => {
      assert.equal(perTokenReadCalled, false);
      assert.equal(engine.ownWaitTimers.has(key), true);
    });
    const timer = engine.ownWaitTimers.get(key);
    const armed = timers.find(t => t.fn && engine.ownWaitTimers.get(key) === timer);
    assert.ok(armed, "grace timer not found among scheduled timers");
    armed.fn(); // fire the grace timeout directly (deterministic, no real wait)
    await check("after the grace period, the specifically-uncovered row falls back to its own per-token read", () => {
      assert.equal(perTokenReadCalled, true);
    });
    await check("the fallback counter is incremented for diagnostics", () => {
      assert.ok((engine.stats.ownWaitFallback || 0) >= 1);
    });
    engine.recovery.stop();
  } finally {
    global.setTimeout = original;
  }
}

// ---- Case 5: a row that becomes authoritative before grace fires is left alone
{
  const original = global.setTimeout;
  const timers = [];
  global.setTimeout = (fn, ms) => { const t = { fn, ms, unref() {} }; timers.push(t); return t; };
  try {
    const { engine, keys } = buildColdEngine({
      resyncOwnOrders: async () => ({ orders: [], complete: false, covered: [] })
    });
    const key = keys[0];
    const row = engine.rows.get(key);
    let perTokenReadCalled = false;
    engine.queueRead = (r, opts) => { if (r.key === key) perTokenReadCalled = true; return true; };
    engine.deferForOwnSync(key, row, "trace-resolved-early");
    // Simulate the row becoming authoritative through some other path
    // (e.g. mergeResync covering it on a later partial pass) before the
    // grace timer fires.
    engine.ownSyncAt = Date.now();
    const book = engine.book.get(key);
    book.ownReconciledAt = Date.now();
    const armed = timers.find(t => engine.ownWaitTimers.get(key) === t);
    armed.fn();
    await check("a row resolved before grace elapses never gets a redundant per-token read", () => {
      assert.equal(perTokenReadCalled, false);
    });
    engine.recovery.stop();
  } finally {
    global.setTimeout = original;
  }
}

process.stdout.write(`\n${passed}/${passed + failed} checks passed\n`);
if (failed) process.exitCode = 1;

})();
