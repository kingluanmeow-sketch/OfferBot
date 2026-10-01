"use strict";

// 1.25.30 originally made a joined-but-eventless Stream DEGRADED after
// ORDER_BLIND_MS of silence, with targeted REST fallback until events
// resumed. 1.25.37 audit: AGENTS.md §7 ("market silence alone is not
// failure") ruled that out -- a quiet market is not evidence anything
// broke. checkOrderBlind() is now diagnostics-only: it never sets DEGRADED,
// never opens a gap, never triggers REST/reconnect, no matter how long the
// shard stays silent. See topic-lifecycle-test.js Case 15 for the per-topic
// equivalent of this same contract.
const assert = require("node:assert/strict");
const { EventType } = require("@opensea/sdk/stream");
const { OpenSeaStream, ORDER_BLIND_MS } = require("./stream-sdk");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const { decide, STATUS } = require("./offer-item-v2/decision");

let passed = 0;
let failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}

let captured;
class FakeClient {
  constructor(config) { this.config = config; captured = this; }
  onEvents(slug, types, handler) { this.handler = handler; return () => {}; }
  disconnect(cb) { cb && cb(); }
}

function bid(slug, n) {
  return {
    event_type: EventType.ITEM_RECEIVED_BID, sent_at: "2026-09-29T12:00:00.000Z",
    payload: {
      chain: "ethereum", collection: { slug },
      item: { nft_id: `ethereum/0x0000000000000000000000000000000000000001/${n}` },
      maker: { address: "0x0000000000000000000000000000000000000002" },
      order_hash: `0x${n}`, payment_token: { decimals: 18, symbol: "WETH" },
      event_timestamp: "2026-09-29T12:00:00.000Z", base_price: "15000000000000000",
      quantity: 1, expiration_date: "2026-10-01T00:00:00.000Z"
    }
  };
}

function joinedStream(callbacks = {}) {
  const stream = new OpenSeaStream({ getApiKey: () => "synthetic-key", onEvent() {}, ...callbacks },
    { OpenSeaStreamClient: FakeClient });
  stream.start(["alpha", "beta"]);
  stream.activeSocket = { readyState: stream.WebSocket.OPEN };
  stream.onGlobalSubscriptionReady();
  return stream;
}

(async () => {
  {
    const unavailable = [];
    const recovered = [];
    const stream = joinedStream({ onTopicUnavailable: s => unavailable.push(s), onTopicGap: s => recovered.push(s) });
    await check("joined subscription is HEALTHY before the blind window elapses", () => {
      assert.equal(stream.health(), "HEALTHY");
      assert.equal(stream.checkOrderBlind(Date.now() + ORDER_BLIND_MS - 1000), false);
    });
    const gapBefore = stream.gapStartedAt;
    stream.checkOrderBlind(Date.now() + ORDER_BLIND_MS + 1);
    await check("ACK ok but no order events for a while stays HEALTHY (silence alone is not failure)", () => {
      assert.equal(stream.health(), "HEALTHY");
      assert.equal(stream.healthForCollection("alpha"), "HEALTHY");
      assert.equal(stream.status().subscriptionsActive, true);
    });
    await check("silence never opens targeted REST recovery for tracked collections", () => {
      assert.deepEqual(unavailable, []);
    });
    stream.checkOrderBlind(Date.now() + 10 * ORDER_BLIND_MS);
    await check("even much longer silence opens no gap and triggers no recovery work", () => {
      assert.equal(unavailable.length, 0);
      assert.equal(stream.gapStartedAt, gapBefore);
    });
    // A re-ACK (the SDK socket reconnects every few minutes in production)
    // must not matter either way, since there was never an outage to close.
    stream.onGlobalSubscriptionReady();
    await new Promise(r => setImmediate(r));
    await check("re-ACK after extended silence: still HEALTHY, nothing to recover", () => {
      assert.equal(stream.health(), "HEALTHY");
      assert.deepEqual(recovered, []);
      assert.equal(stream.gapStartedAt, 0);
    });
    stream.stop();
  }

  {
    const stream = joinedStream();
    stream.lastOrderEventAt = Date.now();
    await check("recent market-wide order event keeps the subscription HEALTHY", () => {
      assert.equal(stream.checkOrderBlind(Date.now() + ORDER_BLIND_MS - 5000), false);
      assert.equal(stream.health(), "HEALTHY");
    });
    stream.stop();
  }

  {
    const engine = new OfferItemEngineV2({ adapter: { chain: "ethereum", async fetchBest() { return { orders: [] }; } }, onLog() {} });
    engine.state = STATE.RUNNING;
    engine.attachStreamHealth(() => "DEGRADED");
    const sweeps = [];
    engine.scheduleRecoverySweep = (reason, opts) => { sweeps.push({ reason, ...opts }); return 1; };
    const realNow = Date.now;
    let t = realNow();
    Date.now = () => t;
    try {
      engine.watchStreamHealth();
      const tick = () => engine.healthTimer._onTimeout();
      tick();
      t += OfferItemEngineV2.DEGRADED_GRACE_MS + 1;
      tick();
      await check("engine treats DEGRADED as unhealthy and starts the bounded REST sweep", () => {
        assert.equal(engine.degraded, true);
        assert.equal(sweeps.length, 1);
        assert.equal(sweeps[0].reason, "stream-degraded");
      });
      await check("trace source is DEGRADED_REST for REST work and STREAM for Stream events", () => {
        assert.equal(engine.triggerSource(null), "DEGRADED_REST");
        assert.equal(engine.triggerSource({ correlationId: "s123-4" }), "STREAM");
      });
      engine.attachStreamHealth(() => "HEALTHY");
      tick();
      await check("healthy Stream turns the REST fallback off", () => {
        assert.equal(engine.degraded, false);
        assert.equal(engine.triggerSource(null), "REST");
      });
    } finally {
      Date.now = realNow;
      engine.stopWatchingStreamHealth();
    }
  }

  await check("competitorBest + step <= Max retakes; target == Max sends", () => {
    const a = decide({ minPrice: 0.001, maxPrice: 0.05, step: 0.0001, best: 0.0450, mine: 0.0441 });
    assert.equal(a.status, STATUS.SEND); assert.equal(a.target, 0.0451);
    const b = decide({ minPrice: 0.001, maxPrice: 0.0451, step: 0.0001, best: 0.0450, mine: 0.0441 });
    assert.equal(b.status, STATUS.SEND); assert.equal(b.target, 0.0451);
  });
  await check("competitorBest + step > Max is blocked as ABOVE_MAX", () => {
    const c = decide({ minPrice: 0.001, maxPrice: 0.0450, step: 0.0001, best: 0.0450, mine: 0.0441 });
    assert.equal(c.status, STATUS.ABOVE_MAX); assert.equal(c.target, 0);
  });

  process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
  if (failed) process.exitCode = 1;
})();
