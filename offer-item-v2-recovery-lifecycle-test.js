"use strict";

const assert = require("assert");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const { INTENT } = require("./offer-item-v2/intent-store");

const key = "ethereum:0x1111111111111111111111111111111111111111:1";
const adapter = { chain: "ethereum", async fetchBest() { throw new Error("test must not fetch"); } };

function engineWithWarmRow() {
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  const row = { key, running: true, tokenId: "1" };
  engine.rows.set(key, row);
  const book = engine.book.add({ key, chain: "ethereum", contract: "0x1111111111111111111111111111111111111111", tokenId: "1" });
  book.hydratedAt = Date.now() - 1000;
  return { engine, row, book };
}

// A healthy WARM row must not acquire REST work merely because five seconds pass.
{
  const { engine } = engineWithWarmRow();
  const original = global.setInterval;
  const intervals = [];
  global.setInterval = (fn, ms) => {
    const timer = { fn, ms, unref() {} };
    intervals.push(timer);
    return timer;
  };
  try {
    engine.startLoops();
    assert.deepStrictEqual(intervals.map(t => t.ms).sort((a, b) => a - b), [1000, 5000]);
    assert.strictEqual(engine.pendingReads.size, 0);
  } finally {
    engine.stopLoops();
    global.setInterval = original;
    engine.recovery.stop();
  }
}

// A Stream event can overtake an own-authority snapshot. The stale result must
// preserve the Stream fence and immediately hand the WAITING send to a new read.
for (const [dependency, expectedReason] of [["own", "pre-post-own"], ["post-reconcile", "post-uncertain"]]) {
  const { engine, book } = engineWithWarmRow();
  const jobs = [];
  engine.recovery.push = job => { jobs.push(job); return true; };
  engine.intents.set(key, { target: 0.02, best: 0.0199, mine: 0, generation: book.generation });
  engine.intents.setState(key, INTENT.WAITING, { dependency });
  const oldRead = { reason: expectedReason, startedSeq: 1, startedAt: Date.now() - 100 };
  engine.pendingReads.set(key, oldRead);
  engine.hydrating.add(key);
  book.streamSeq = 2;
  book.lastEventAt = Date.now();

  engine.mergeBest(key, { empty: true }, engine.recovery.generation,
    { reason: expectedReason, quick: false, readAt: oldRead.startedAt, readOwner: oldRead });

  assert.strictEqual(engine.intents.get(key).state, INTENT.WAITING);
  assert.strictEqual(book.ownReconciledAt || 0, 0, "stale snapshot cannot grant own authority");
  assert.strictEqual(engine.pendingReads.get(key)?.reason, expectedReason,
    "the waiting intent must have a new read owner before mergeBest returns");
  assert.notStrictEqual(engine.pendingReads.get(key), oldRead);
  assert.strictEqual(engine.hydrating.has(key), true);
  assert.strictEqual(jobs.length, 1);
  assert.strictEqual(jobs[0].priority, 1);
  engine.recovery.stop();
}

process.stdout.write("PASS recovery lifecycle: no WARM shadow timer; stale own reads keep an owner\n");

{
  const { engine, row, book } = engineWithWarmRow();
  row.collectionSlug = "alpha";
  book.lastFullReadAt = Date.now() - 2000;
  let healthy = true;
  engine.attachStreamHealth(slug => slug === "alpha" && healthy ? "HEALTHY" : "RECONNECTING");
  assert.strictEqual(engine.topicReady(row, book), true);
  healthy = false;
  assert.strictEqual(engine.topicReady(row, book), false, "lost collection ACK blocks stale-price send");
  healthy = true;
  const gapAt = Date.now();
  engine.topicRepairAt.set(key, gapAt);
  assert.strictEqual(engine.topicReady(row, book), false, "ACK alone cannot validate missed offers");
  book.lastEventAt = gapAt + 1;
  book.effectiveBest = () => ({ price: 0.03 });
  assert.strictEqual(engine.topicReady(row, book), true, "fresh Stream competitor keeps P0 realtime");
  book.lastFullReadAt = gapAt + 1;
  assert.strictEqual(engine.topicReady(row, book), true, "scoped fresh authority reopens send");
  engine.recovery.stop();
}
process.stdout.write("PASS collection ACK and scoped authority gate\n");
