"use strict";

/**
 * RED/GREEN: dual-feed Stream redundancy (1.25.40).
 *
 * Production audit confirmed OpenSea's best-effort Stream silently dropping
 * individual `item_received_bid` events while topic/socket health stayed
 * fully healthy and the SAME collection kept delivering other events
 * throughout -- the existing health/reconnect machinery structurally cannot
 * see or fix a per-message loss like this. Three real incidents:
 *
 *   goblintownwtf #1810: external 0.0397, mine 0.0396, step 0.0001 -> target 0.0398
 *   fwogs #2120:          external 0.0300, mine 0.0295, step 0.0001 -> target 0.0301
 *   fwogs #3718:          external 0.0311, mine 0.0308, step 0.0001 -> target 0.0312
 *
 * The fix (main.js) runs a second, fully independent Stream connection on
 * the second configured API key, feeding the SAME MemoryBook through the
 * SAME dispatch path as the primary feed -- not a separate decision path.
 * That means the correctness properties that matter are entirely provable
 * at the engine/MemoryBook level, which is what this file tests:
 *
 *   1. A competing bid that one feed would have missed still reaches the
 *      engine (there is nothing feed-specific about how an event is
 *      applied) and produces the exact correct outbid target for each of
 *      the three real production numbers above.
 *   2. The SAME event delivered twice (what dual-feed delivery actually
 *      produces in the common case: both feeds get it) is idempotent --
 *      MemoryBook.apply's order-hash + content-equality check means a
 *      second identical delivery changes nothing and must NOT produce a
 *      second, duplicate SEND/POST.
 *   3. Decision->Intent latency for the recovered event is bounded (no
 *      REST, no extra hop -- straight to an intent in the same tick).
 *
 * main.js's own socket/dedupe-counter wiring is thin glue over this (see
 * dispatchStreamEvent/ensureStreamB) and is covered by the full-app syntax
 * check + existing per-class stream-sdk-test.js/stream-shard-test.js; it has
 * no independently testable decision logic of its own.
 */

const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const { INTENT } = require("./offer-item-v2/intent-store");

let passed = 0, failed = 0;
function check(name, fn) {
  try { fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}

const RIVAL = "0x3333333333333333333333333333333333333333";
const SELF = "0x4444444444444444444444444444444444444444";

function buildEngine({ slug, tokenId, mine, minPrice = 0.001, maxPrice = 1, step = 0.0001 }) {
  const contract = "0x1111111111111111111111111111111111111111";
  const key = `ethereum:${contract}:${tokenId}`;
  const fetchBestCalls = [];
  const adapter = { chain: "ethereum", async fetchBest() { fetchBestCalls.push(1); throw new Error("test must not fetch -- this is Stream-first, no REST"); } };
  adapter.fetchBestCalls = fetchBestCalls;
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(() => "HEALTHY");
  engine.templateReady = () => true;
  engine.builder = { address: SELF };
  const sends = [];
  // Stand in for the real write path: just record what would have been sent.
  engine.pump = () => {
    for (const intent of engine.intents.intents.values()) {
      if (intent.state === INTENT.READY && intent.target) {
        sends.push({ key: intent.tokenKey, target: intent.target });
        intent.state = INTENT.DONE;
      }
    }
  };
  const row = { key, running: true, tokenId, contract, collectionSlug: slug, minPrice, maxPrice, step };
  engine.rows.set(key, row);
  const book = engine.book.add({ key, chain: "ethereum", contract, tokenId });
  book.generation = 1;
  book.hydratedAt = Date.now() - 1000;
  book.lastFullReadAt = Date.now() - 1000;
  book.selfAddress = SELF;
  engine.book.setSelfAddress(SELF);
  // The bot's own existing offer, exactly as it would already sit in the
  // book before the competing bid arrives.
  book.own.set("0xownorderhash0000000000000000000000000000000000000000000000", {
    orderHash: "0xownorderhash0000000000000000000000000000000000000000000000",
    price: mine, maker: SELF, kind: "item", quantity: 1, currency: "WETH",
    endTime: Math.floor(Date.now() / 1000) + 3600, seq: 0, at: Date.now() - 1000
  });
  return { engine, row, book, key, sends };
}

function rivalBidEvent({ slug, contract, tokenId, price, orderHash, at }) {
  return {
    collectionSlug: slug, nft: { chain: "ethereum", contract, tokenId },
    kind: "item", orderHash, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: at, receivedAt: at, hasOrderData: true,
    event: "item_received_bid", pricePerItem: price
  };
}

const PRODUCTION_CASES = [
  { name: "goblintownwtf #1810", slug: "goblintownwtf", tokenId: "1810", mine: 0.0396, external: 0.0397, expectedTarget: 0.0398 },
  { name: "fwogs #2120", slug: "fwogs", tokenId: "2120", mine: 0.0295, external: 0.0300, expectedTarget: 0.0301 },
  { name: "fwogs #3718", slug: "fwogs", tokenId: "3718", mine: 0.0308, external: 0.0311, expectedTarget: 0.0312 }
];

for (const c of PRODUCTION_CASES) {
  // ---- a single feed delivering the missed event recovers it correctly ---
  {
    const { engine, row, book, key, sends } = buildEngine({ slug: c.slug, tokenId: c.tokenId, mine: c.mine });
    const beforeDecisionAt = Date.now();
    const event = rivalBidEvent({ slug: c.slug, contract: row.contract, tokenId: c.tokenId, price: c.external,
      orderHash: `0x${c.tokenId.padStart(63, "0")}1`, at: beforeDecisionAt });
    engine.handleStreamEvent(event);

    check(`${c.name}: book reflects the recovered external Best (not stale)`, () => {
      assert.equal(book.effectiveBest(Date.now()).price, c.external);
    });
    check(`${c.name}: intent targets exactly the correct outbid price (mine+step above external)`, () => {
      const intent = engine.intents.get(key);
      assert.ok(intent, "no intent created");
      assert.equal(Number(intent.target.toFixed(4)), c.expectedTarget);
    });
    check(`${c.name}: exactly one SEND reaches pump, at the correct target`, () => {
      assert.equal(sends.length, 1);
      assert.equal(Number(sends[0].target.toFixed(4)), c.expectedTarget);
    });
    check(`${c.name}: Decision->Intent was synchronous and used no REST hop`, () => {
      // handleStreamEvent -> apply -> evaluate -> pump all ran synchronously
      // above (no await between the call and the assertions on `sends`),
      // and fetchBest (which throws if ever called) never fired.
      assert.equal(engine.adapter.fetchBestCalls.length, 0);
      assert.equal(sends.length, 1, "a send must already exist by the time this synchronous call returned");
    });
  }

  // ---- the SAME event delivered twice (both feeds got it) is idempotent --
  {
    const { engine, row, book, key, sends } = buildEngine({ slug: c.slug, tokenId: c.tokenId, mine: c.mine });
    const at = Date.now();
    const orderHash = `0x${c.tokenId.padStart(63, "0")}2`;
    const event = rivalBidEvent({ slug: c.slug, contract: row.contract, tokenId: c.tokenId, price: c.external, orderHash, at });
    // Feed A delivers it.
    engine.handleStreamEvent({ ...event });
    // Feed B independently delivers the identical real-world event
    // (same order hash, same price, same endTime) a moment later.
    engine.handleStreamEvent({ ...event, receivedAt: at + 5 });

    check(`${c.name}: duplicate delivery across feeds does not create a second intent cycle`, () => {
      // Only one send should ever have reached pump -- a second identical
      // delivery must be a no-op in MemoryBook.apply (content-equality
      // check), never a second SEND of the same target.
      assert.equal(sends.length, 1, `expected exactly one SEND, got ${sends.length}`);
    });
    check(`${c.name}: book still shows exactly one order at the external price (no duplicate order entry)`, () => {
      const best = book.effectiveBest(Date.now());
      assert.equal(best.price, c.external);
      assert.equal(book.item.size, 1, "duplicate delivery must not create a second book entry");
    });
  }
}

process.stdout.write(`\n${passed}/${passed + failed} checks passed\n`);
if (failed) process.exitCode = 1;
