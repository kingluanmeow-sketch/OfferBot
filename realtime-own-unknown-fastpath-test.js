"use strict";

// Realtime P0 may bypass unknown historical own-state on a warm, known market,
// but never a COLD first read or a row-local ambiguous POST.
const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");

const engine = new OfferItemEngineV2({ adapter: { chain: "ethereum" }, onLog() {} });
engine.state = STATE.RUNNING;
const key = "ethereum:0x1111111111111111111111111111111111111111:6435";
const row = { key, tokenId: "6435", contract: "0x1111111111111111111111111111111111111111", collectionSlug: "goblintownwtf", running: true };
const book = {
  hydratedAt: Date.now() - 1000,
  ownReconciledAt: 0,
  ownKnownAt: 0,
  ownUnknownAt: 0,
  effectiveBest: () => ({ price: 0.0119 }),
  ownBest: () => ({ price: 0 })
};
engine.rows.set(key, row);
engine.book.get = candidate => candidate === key ? book : null;
engine.topicReady = () => true;

function check(name, fn) {
  try { fn(); process.stdout.write(`PASS ${name}\n`); }
  catch (error) { process.stderr.write(`FAIL ${name}: ${error.message}\n`); process.exitCode = 1; }
}
async function checkAsync(name, fn) {
  try { await fn(); process.stdout.write(`PASS ${name}\n`); }
  catch (error) { process.stderr.write(`FAIL ${name}: ${error.message}\n`); process.exitCode = 1; }
}

check("warm P0 Stream with known Best may bypass absent historical own authority", () => {
  assert.equal(engine.canRealtimeSendWithoutOwnAuthority(key, row, book, { realtimeP0: true }), true);
});
check("REST/recovery intent cannot inherit the P0 bypass", () => {
  assert.equal(engine.canRealtimeSendWithoutOwnAuthority(key, row, book, { realtimeP0: false }), false);
});
check("COLD first-read remains gated even when a Stream event arrives", () => {
  book.hydratedAt = 0;
  engine.awaitingFirstRead.add(key);
  assert.equal(engine.canRealtimeSendWithoutOwnAuthority(key, row, book, { realtimeP0: true }), false);
  engine.awaitingFirstRead.delete(key);
  book.hydratedAt = Date.now() - 1000;
});
check("ambiguous POST newer than every own authority timestamp remains gated", () => {
  book.ownUnknownAt = Date.now() + 1;
  assert.equal(engine.canRealtimeSendWithoutOwnAuthority(key, row, book, { realtimeP0: true }), false);
  book.ownUnknownAt = 0;
});
check("a reconciled/known state newer than ambiguity follows ordinary authority", () => {
  book.ownUnknownAt = Date.now();
  book.ownKnownAt = book.ownUnknownAt + 1;
  assert.equal(engine.canRealtimeSendWithoutOwnAuthority(key, row, book, { realtimeP0: true }), true);
});
check("no known competitor Best is never treated as warm enough to bypass", () => {
  const empty = { ...book, effectiveBest: () => ({ price: 0 }) };
  assert.equal(engine.canRealtimeSendWithoutOwnAuthority(key, row, empty, { realtimeP0: true }), false);
});

void (async () => {
  let queuedBackgroundReconcile = 0;
  let reachedRealtimeWriteBroker = false;
  engine.ownAuthoritative = () => false;
  engine.ownDiagnostic = () => ({ covered: false, ownSyncPending: false, ownSyncAt: 0,
    ownReconciledAt: 0, ownKnownAt: 0, ownUnknownAt: 0, uncovered: 1 });
  engine.queueOwnResync = () => { queuedBackgroundReconcile++; return true; };
  engine.templateReady = () => true;
  engine.getApiKeys = () => [];
  engine.getApiKey = () => "test-key";
  engine.quota = { acquire: async () => { reachedRealtimeWriteBroker = true; throw new Error("test-stop-at-broker"); } };
  engine.metrics.start("rt-fastpath-test", { chain: "ethereum", tokenId: row.tokenId, generation: 1, price: 0.012, trigger: "stream" });
  const intent = engine.intents.set(key, { target: 0.012, best: 0.0119, mine: 0, generation: 1, reason: "outbid", at: Date.now() });
  intent.realtimeP0 = true;
  intent.traceId = "rt-fastpath-test";
  intent.correlationId = "s1-1";

  await checkAsync("submitOne reaches the realtime broker without awaiting own-state or starting REST before POST", async () => {
    await assert.rejects(engine.submitOne(key), /test-stop-at-broker/);
    assert.equal(reachedRealtimeWriteBroker, true);
    assert.equal(queuedBackgroundReconcile, 0, "REST reconciliation must not start before the P0 POST transport stage");
  });
  check("own reconciliation is scheduled only after HTTP POST has started", () => {
    assert.equal(engine.scheduleOwnReconcileAfterRealtimePost(key, true), true);
    assert.equal(queuedBackgroundReconcile, 1);
  });
  engine.recovery.stop();
})();
