"use strict";

/**
 * RED/GREEN: every POST (`http_start`) must end in a bounded terminal outcome
 * -- SUCCESS, DEFINITELY_NOT_SENT, or AMBIGUOUS_OUTCOME (-> targeted
 * reconcile) -- never silently vanish (1.25.44).
 *
 * PRODUCTION SYMPTOM AUDITED
 *
 *   Production trace: 247 http_start, 216 submit_success, 0 submit_failure,
 *   31 POST with no terminal result at all. Several NFTs (genuine-undead
 *   #2526, porsche-911 #1026/#2159/#1769, gorgez #5048) retried ~1s apart --
 *   far faster than any of the engine's own backoff policies (minimum
 *   "ambiguous" base is 1000ms, and that only starts counting AFTER a
 *   terminal result).
 *
 * ROOT CAUSE FOUND BY READING abandonFlight() (engine-v2.js)
 *
 *   `watchdog()` calls `abandonFlight(key, "watchdog")` when a flight has
 *   been in-flight > INFLIGHT_STUCK_MS (180s). The OLD abandonFlight
 *   unconditionally deleted the flight's bookkeeping and immediately called
 *   `evaluate()+pump()` -- producing a BRAND NEW SEND/POST -- regardless of
 *   what stage the abandoned flight was at. If that stage was "sending" (the
 *   HTTP POST had actually been dispatched and we have NO confirmed outcome
 *   for it), this is exactly the gap: a new POST fires while the old one's
 *   true server-side result is still unknown, and if the original promise
 *   never settles (the suspected Node/socket edge case under real network
 *   conditions -- `req.destroy()` not always reaching `req.on("error")`
 *   once a response has partially started), that `http_start` NEVER gets
 *   its own terminal trace -- explaining the 31 missing results. The ~1s
 *   retry cadence matches `abandonFlight`'s own immediate
 *   `setImmediate(() => this.pump())` with no backoff at all.
 *
 * FIX (abandonFlight, engine-v2.js; new "submit_ambiguous"/AMBIGUOUS_OUTCOME
 * trace stage/status, production-trace.js)
 *
 *   `abandonFlight` now checks `f.stage === "sending"` (set right before the
 *   HTTP call, unchanged until settle) BEFORE clearing bookkeeping. If true,
 *   it takes EXACTLY the same ambiguous path as a direct HTTP-layer catch:
 *   mark `book.ownUnknownAt`, record an explicit AMBIGUOUS_OUTCOME trace,
 *   queue a targeted reconciliation read for THIS NFT, and apply the
 *   existing bounded "ambiguous" retry policy -- never a blind immediate
 *   resend. Only a flight NOT yet sending (granting/building) still gets
 *   the old immediate re-evaluate (nothing was ever sent, nothing to
 *   reconcile).
 */

const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const { INTENT } = require("./offer-item-v2/intent-store");
const { OfferItemV2Bridge } = require("./offer-item-v2/bridge");

/**
 * A bare `scanStateOf`/`statusOf` caller: the real Bridge's constructor
 * builds a live adapter/engine/DB stack (too heavy for a unit test), but
 * `scanStateOf` itself is a pure read of `this.engine`/`this.streamHealth` --
 * bridge.js's own header says "cầu nối không được chứa logic", so this is a
 * faithful proxy for "what will the UI label say", not a reimplementation.
 */
function bridgeView(engine) {
  const b = Object.create(OfferItemV2Bridge.prototype);
  b.engine = engine;
  b.streamHealth = null;
  return b;
}

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}
const flush = () => new Promise(r => setImmediate(r));

const SELF = "0x4444444444444444444444444444444444444444";
const CONTRACT = "0x1111111111111111111111111111111111111111";

function buildEngine({ tokenId = "1", slug = "alpha" } = {}) {
  const key = `ethereum:${CONTRACT}:${tokenId}`;
  const adapter = {
    chain: "ethereum", openseaChain: "ethereum",
    toWei: v => BigInt(Math.round(Number(v) * 1e18)),
    offerBody: () => ({}),
    async fetchBest() { return { orderHash: "", orders: [] }; }
  };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  engine.epoch = 1;
  engine.attachStreamHealth(() => "HEALTHY");
  engine.templateReady = () => true;
  const row = { key, running: true, tokenId, contract: CONTRACT, collectionSlug: slug, minPrice: 0.001, maxPrice: 1, step: 0.0001 };
  engine.rows.set(key, row);
  const book = engine.book.add({ key, chain: "ethereum", contract: CONTRACT, tokenId, collectionSlug: slug });
  book.generation = 1;
  book.hydratedAt = Date.now() - 1000;
  book.ownReconciledAt = Date.now() - 1000;
  book.selfAddress = SELF;
  engine.book.setSelfAddress(SELF);
  return { engine, book, row, key };
}

/** Puts a row into exactly the state abandonFlight() would see mid-POST. */
function armSendingFlight(engine, key, target = 0.01) {
  engine.intents.set(key, { target, best: 0, mine: 0, generation: 1, reason: "SEND", at: Date.now() });
  engine.intents.acquire(key);
  engine.setIntent(key, INTENT.SENDING);
  const flight = { id: 1, since: Date.now(), stage: "sending", handle: {}, controller: new AbortController() };
  engine.flights.set(key, flight);
  engine.inFlightSince.set(key, flight.since);
  return flight;
}

(async () => {

// ---- Case 1: abandoning a flight mid-SEND is treated as AMBIGUOUS, not a
// blind immediate resend. ----------------------------------------------
{
  const { engine, book, row, key } = buildEngine();
  armSendingFlight(engine, key, 0.01);
  let evaluateCalls = 0;
  const originalEvaluate = engine.evaluate.bind(engine);
  engine.evaluate = (...a) => { evaluateCalls++; return originalEvaluate(...a); };
  let queuedReads = [];
  engine.queueRead = (r, opts) => queuedReads.push({ r, opts });

  engine.abandonFlight(key, "watchdog");

  await check("a flight abandoned while stage=='sending' does NOT call evaluate() synchronously (no blind immediate resend)", () => {
    assert.equal(evaluateCalls, 0);
  });
  await check("the row's own state is marked unknown (ownUnknownAt) so nothing treats this NFT's own coverage as settled", () => {
    assert.ok(book.ownUnknownAt > 0);
  });
  await check("a targeted reconciliation read is queued for THIS exact NFT (via deferForOwnSync, not a bare retry)", () => {
    assert.equal(queuedReads.length, 1);
    assert.equal(queuedReads[0].r.tokenId, row.tokenId);
    assert.equal(queuedReads[0].opts.reason, "post-uncertain");
  });
  await check("UI TRUTH (1.25.45): the intent becomes WAITING with dependency 'post-reconcile' IMMEDIATELY -- not RETRY for a whole backoff window first, and not an instant blind resend either", () => {
    const intent = engine.intents.get(key);
    assert.equal(intent.state, INTENT.WAITING);
    assert.equal(intent.dependency, "post-reconcile");
  });
  await check("the Bridge's progress-column mapping reads this as POST_RECONCILE ('Kết quả POST chưa xác định / Đang đối chiếu'), never the generic ERROR/'Lỗi' label", () => {
    const label = bridgeView(engine).scanStateOf(row, key);
    assert.equal(label, "POST_RECONCILE");
  });
  await check("the flight/in-flight bookkeeping for this key is cleared (single-flight lock released, ready for the reconcile read's own evaluate()+pump() to resend once it resolves)", () => {
    assert.ok(!engine.flights.has(key));
    assert.ok(!engine.inFlightSince.has(key));
  });
}

// ---- Case 2: abandoning a flight that NEVER reached "sending" (still
// granting/building) is unaffected -- nothing was sent, safe to re-evaluate
// immediately. This is the pre-existing behavior; must stay unchanged. ----
{
  const { engine, key } = buildEngine();
  engine.intents.set(key, { target: 0.01, best: 0, mine: 0, generation: 1, reason: "SEND", at: Date.now() });
  engine.intents.acquire(key);
  engine.setIntent(key, INTENT.GRANTING);
  const flight = { id: 2, since: Date.now(), stage: "granting", handle: {}, controller: new AbortController() };
  engine.flights.set(key, flight);
  engine.inFlightSince.set(key, flight.since);
  let evaluateCalls = 0;
  const originalEvaluate = engine.evaluate.bind(engine);
  engine.evaluate = (...a) => { evaluateCalls++; return originalEvaluate(...a); };

  engine.abandonFlight(key, "watchdog");

  await check("a flight abandoned BEFORE 'sending' (nothing sent to network) still re-evaluates immediately -- unrelated pre-existing behavior preserved", () => {
    assert.equal(evaluateCalls, 1);
  });
}

// ---- Case 3: direct HTTP-layer errors are correctly split into
// DEFINITELY_NOT_SENT vs AMBIGUOUS_OUTCOME (not lumped into one generic
// submit_failure as before). ------------------------------------------
{
  const { engine, book, row, key } = buildEngine();
  engine.templateReady = () => true;
  engine.builder = { build: async () => ({ orderHash: "", components: { endTime: Math.floor(Date.now() / 1000) + 900 } }) };
  engine.epoch = 1;

  // (a) definitely not sent: body never left the machine.
  engine.http = { request: async () => { const e = new Error("ECONNREFUSED"); e.notSent = true; throw e; } };
  engine.evaluate(key, Date.now(), null);
  engine.pump();
  await flush(); await flush(); await flush(); await flush(); await flush();
  await check("a 'notSent' HTTP error does not mark the NFT's own state unknown (nothing could have reached the server)", () => {
    assert.equal(book.ownUnknownAt || 0, 0);
  });
  await check("a 'notSent' HTTP error schedules a FAST retry (not-sent policy), not the slower ambiguous one", () => {
    assert.ok(row.retryAt > 0 && row.retryAt - Date.now() <= 5000);
  });

  // (b) ambiguous: body may have reached the server (e.g. timeout after
  // flush, socket reset mid-response).
  row.retryAt = 0;
  engine.http = { request: async () => { const e = new Error("socket hang up"); e.notSent = false; throw e; } };
  let queuedReads = [];
  engine.queueRead = (r, opts) => queuedReads.push({ r, opts });
  engine.evaluate(key, Date.now(), null);
  engine.pump();
  await flush(); await flush(); await flush(); await flush(); await flush();
  await check("an ambiguous HTTP error (could have reached the server) DOES mark the NFT's own state unknown", () => {
    assert.ok(book.ownUnknownAt > 0);
  });
  await check("an ambiguous HTTP error queues a targeted reconciliation read for this exact NFT", () => {
    // >=1, not ===1: this test's stub queueRead never marks pendingReads/
    // hydrating (the real implementation does), so ensureRowProgress's own
    // dedup/safety-net check (correctly, by design) re-arms the read once
    // more via the flight-settled wake -- a real queueRead would dedupe
    // this. What matters here: it is never zero.
    assert.ok(queuedReads.length >= 1);
    assert.equal(queuedReads[0].r.tokenId, row.tokenId);
  });
}

// ---- Case 4: a LATE success for an already-abandoned/reconciling flight
// must not resurrect it as if nothing happened -- but a real accepted order
// must still be recorded (money-safety: never lose a real own order). -----
{
  const { engine, book, row, key } = buildEngine();
  engine.templateReady = () => true;
  const realHash = "0xlate00000000000000000000000000000000000000000000000000000001";
  let resolvePost;
  engine.builder = { build: async () => ({ orderHash: "", components: { endTime: Math.floor(Date.now() / 1000) + 900 } }) };
  engine.http = { request: () => new Promise(resolve => { resolvePost = resolve; }) };
  engine.evaluate(key, Date.now(), null);
  engine.pump();
  await flush(); await flush();
  // Simulate the watchdog abandoning this exact in-flight POST (its promise
  // is still pending -- exactly the ambiguous/late-arrival scenario).
  engine.abandonFlight(key, "watchdog");
  await check("after abandonment, the row is READY again (not stuck SENDING forever) while the original POST promise is still unresolved", () => {
    const intent = engine.intents.get(key);
    assert.ok(intent && intent.state !== INTENT.SENDING);
  });
  // The original (abandoned) request NOW resolves late, as a real 2xx.
  resolvePost({ status: 200, headers: {}, body: { order_hash: realHash }, totalMs: 5000 });
  await flush(); await flush(); await flush(); await flush();
  await check("a late 2xx for an abandoned flight still records the real own order (never silently lost -- money-safety)", () => {
    assert.ok(book.own.has(realHash), "the order OpenSea actually accepted must still end up in the book");
  });
}

// ---- Case 5: multiple NFTs across different collections are isolated --
// one NFT's ambiguous POST must never block or delay another's. ----------
{
  const { engine: e1, row: r1, key: k1 } = buildEngine({ tokenId: "a1", slug: "collA" });
  armSendingFlight(e1, k1, 0.01);
  const beforeOther = Date.now();
  e1.abandonFlight(k1, "watchdog");

  const { engine: e2, key: k2, book: b2, row: r2 } = buildEngine({ tokenId: "b1", slug: "collB" });
  e2.templateReady = () => true;
  e2.epoch = 1;
  e2.builder = { build: async () => ({ orderHash: "", components: { endTime: Math.floor(Date.now() / 1000) + 900 } }) };
  e2.http = { request: async ({ onStage }) => { if (onStage) onStage("http_started"); return { status: 200, headers: {}, body: { order_hash: "0xisolated1" }, totalMs: 5 }; } };
  e2.evaluate(k2, Date.now(), null);
  e2.pump();
  await flush(); await flush(); await flush(); await flush(); await flush();
  await check("NFT #2 (different engine instance == different collection/row in production) completes a normal SUCCESS POST unaffected by NFT #1's ambiguous abandonment", () => {
    assert.ok(b2.own.has("0xisolated1"));
    assert.ok(Date.now() - beforeOther < 1000, "test took unexpectedly long -- no cross-NFT blocking expected");
  });
}

// ---- Case 6: full ambiguous -> reconciling -> RESOLVED lifecycle, through
// the REAL queueRead/mergeBest path (not stubbed) -- proving the UI's
// "Đang đối chiếu" state actually clears into the correct next state once
// the targeted reconcile read lands, both for the "order WAS accepted"
// and "order was NOT accepted" outcomes. ----------------------------------
{
  // 6a: reconcile finds OUR hash resolved as Best -- settles, no longer
  // stuck "Đang đối chiếu", and the real order is in the book.
  const { engine, book, row, key } = buildEngine();
  engine.templateReady = () => true;
  engine.builder = { address: SELF };
  const ourHash = "0xreconcile000000000000000000000000000000000000000000000001";
  engine.recovery.push = job => {
    Promise.resolve(job.run({ signal: { aborted: false } }))
      .then(result => job.onResult(result, engine.recovery.generation));
    return true;
  };
  // Set BEFORE triggering the ambiguous event: deferForOwnSync's own
  // queueRead -> recovery.push -> job.run() all run synchronously inside
  // abandonFlight() itself, so a mock assigned afterwards would be too late
  // to be seen by this reconcile read.
  engine.adapter.fetchBest = async () => ({
    orderHash: ourHash,
    orders: [{ orderHash: ourHash, price: 0.05, maker: SELF, kind: "item", endTime: 0, quantity: 1 }]
  });
  armSendingFlight(engine, key, 0.05);
  engine.abandonFlight(key, "watchdog");
  await flush(); await flush(); await flush(); await flush(); await flush();
  await check("RESOLVED (order found, confirmed as Best): the row is no longer parked on post-reconcile -- UI moves off 'Đang đối chiếu' into a real next state", () => {
    const intent = engine.intents.get(key);
    assert.notEqual(intent && intent.dependency, "post-reconcile");
    assert.notEqual(bridgeView(engine).scanStateOf(row, key), "POST_RECONCILE");
  });
  await check("the real own order the reconcile read found is in the book (upserted via the pre-existing own-authority merge, unchanged)", () => {
    assert.ok(book.own.has(ourHash));
  });

  // 6b: reconcile finds NOTHING of ours -- the POST evidently never landed
  // as a real own order; row must resolve to a fresh SEND, not stay ambiguous.
  const second = buildEngine({ tokenId: "2" });
  second.engine.templateReady = () => true;
  second.engine.builder = { address: SELF };
  second.engine.recovery.push = job => {
    Promise.resolve(job.run({ signal: { aborted: false } }))
      .then(result => job.onResult(result, second.engine.recovery.generation));
    return true;
  };
  armSendingFlight(second.engine, second.key, 0.03);
  second.engine.abandonFlight(second.key, "watchdog");
  second.engine.adapter.fetchBest = async () => ({ orderHash: "", orders: [] });
  await flush(); await flush(); await flush(); await flush(); await flush();
  await check("RESOLVED (nothing of ours found -- the POST evidently never produced a real own order): the row resolves to a fresh SEND/READY, not stuck waiting on a reconcile that already landed", () => {
    const intent = second.engine.intents.get(second.key);
    assert.notEqual(intent && intent.dependency, "post-reconcile");
    assert.ok(intent && intent.target > 0, "no fresh target computed after the reconcile came back empty");
  });
}

// ---- Case 7: rapid repeated evaluate() calls while a POST is genuinely
// in flight must never produce a SECOND POST for the same NFT. -----------
{
  const { engine, key } = buildEngine();
  engine.templateReady = () => true;
  engine.builder = { build: async () => ({ orderHash: "", components: { endTime: Math.floor(Date.now() / 1000) + 900 } }) };
  let postCalls = 0;
  let resolvePost;
  engine.http = { request: () => { postCalls++; return new Promise(resolve => { resolvePost = resolve; }); } };
  engine.evaluate(key, Date.now(), null);
  engine.pump();
  await flush(); await flush();
  // Several rapid re-evaluations while the single-flight lock is held.
  for (let i = 0; i < 5; i++) { engine.evaluate(key, Date.now(), null); engine.pump(); await flush(); }
  await check("rapid repeated evaluate()/pump() calls while a POST is genuinely in flight never create a second POST (single-flight lock holds)", () => {
    assert.equal(postCalls, 1);
  });
  resolvePost({ status: 200, headers: {}, body: { order_hash: "0xsingleflight1" }, totalMs: 5 });
  await flush(); await flush(); await flush();
}

process.stdout.write(`\n${passed}/${passed + failed} checks passed\n`);
if (failed) process.exitCode = 1;

})();
