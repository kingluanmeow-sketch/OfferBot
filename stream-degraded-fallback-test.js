"use strict";

// 1.25.30: a joined Stream that carries no order events is DEGRADED, not
// HEALTHY, and the engine falls back to bounded targeted REST until order
// events flow again. Observed live 2026-09-29: ACK ok, 0 order events.
const assert = require("node:assert/strict");
const { EventType } = require("@opensea/sdk/stream");
const { OpenSeaStream, ORDER_BLIND_MS, ORDER_RESUME_EVENTS } = require("./stream-sdk");
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
    stream.checkOrderBlind(Date.now() + ORDER_BLIND_MS + 1);
    await check("ACK ok but no order events = DEGRADED, never HEALTHY", () => {
      assert.equal(stream.health(), "DEGRADED");
      assert.equal(stream.healthForCollection("alpha"), "DEGRADED");
      assert.equal(stream.status().orderEventsBlind, true);
      assert.equal(stream.status().subscriptionsActive, true, "the subscription itself is kept");
    });
    await check("blindness opens targeted REST recovery for every tracked collection", () => {
      assert.deepEqual(unavailable.sort(), ["alpha", "beta"]);
    });
    stream.checkOrderBlind(Date.now() + 10 * ORDER_BLIND_MS);
    await check("blind detection is one outage, not repeated work", () => {
      assert.equal(unavailable.length, 2);
    });
    // Live 2026-09-29: the SDK socket reconnects every few minutes while still
    // blind. A re-ACK must not close the outage and reset every row's repair
    // mark, or no REST read is ever newer than the mark and SEND stays blocked.
    const gapBefore = stream.gapStartedAt;
    stream.onGlobalSubscriptionReady();
    await new Promise(r => setImmediate(r));
    await check("re-ACK while still blind keeps the outage open (no repair reset)", () => {
      assert.equal(stream.health(), "DEGRADED");
      assert.deepEqual(recovered, []);
      assert.equal(stream.gapStartedAt, gapBefore);
    });
    for (let i = 0; i < ORDER_RESUME_EVENTS - 1; i++) captured.handler(bid("untracked", i));
    await check("a few stray events do not end DEGRADED", () => assert.equal(stream.health(), "DEGRADED"));
    captured.handler(bid("untracked", 99));
    await new Promise(r => setImmediate(r));
    await check("order events flowing again return to Stream-first HEALTHY and reconcile once", () => {
      assert.equal(stream.health(), "HEALTHY");
      assert.deepEqual(recovered.sort(), ["alpha", "beta"]);
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
