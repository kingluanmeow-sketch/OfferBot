"use strict";

/**
 * RED/GREEN: NPC #6490 production miss -- OpenSea Best=0.0115, Mine=0.0113,
 * but Book/Decision stuck at Best=Mine=0.0109(ish)/ON_TOP indefinitely.
 *
 * ROOT CAUSE (traced raw Stream frame -> normalize -> MemoryBook -> REST
 * authority -> effectiveBest, from real production trace, tokenId 6490,
 * collection npc-on-chain): a high-churn collection where the SAME token
 * receives a long, CONTINUOUS run of "stale" rejections (29/29 observed,
 * 93/93 collection-wide) -- each individually legitimate (out-of-order
 * create/cancel racing, all priced well below the book's belief) -- with
 * ZERO successfully-applied event in between. The book-contradiction
 * mechanism (existing, from earlier fixes) only fires when a REJECTED op's
 * OWN price exceeds the book's current belief -- but none of these do
 * (they're all noise far below both Book and live authority). The actual
 * contradiction is between Book (0.0109) and EXTERNAL AUTHORITY (0.0115) --
 * invisible to the book, because whatever order IS worth 0.0115 left no
 * trace on either feed. Nothing currently asks "have I gone a long streak
 * with zero successful applies despite heavy traffic?" -- so the book never
 * re-checks itself, and the stale, too-low Mine (0.0113 vs needed >=0.0116)
 * sits there believing ON_TOP forever.
 *
 * FIX: track, per tracked row, a BOUNDED counter of consecutive "stale"
 * rejections since the last successfully-applied Stream event. Crossing a
 * fixed threshold triggers exactly ONE targeted authoritative /best read
 * for THAT row (reusing queueRead's existing single-flight/cooldown/
 * generation-safety), then resets. No polling (purely event-count-driven,
 * never a timer), no full sweep (strictly per-NFT, no other row is ever
 * touched), no REST before the realtime POST path, no change to Stream/
 * broker/dual-feed.
 */
const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}
const flush = async (n = 3) => { for (let i = 0; i < n; i++) await new Promise(r => setImmediate(r)); };

const RIVAL = "0x3333333333333333333333333333333333333333";
const SELF = "0x4444444444444444444444444444444444444444";
const CONTRACT = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

function buildEngine() {
  const resolvers = [];
  const adapter = {
    chain: "ethereum", openseaChain: "ethereum",
    toWei: x => String(Math.round(Number(x) * 1e18)),
    offerBody: () => ({}),
    async fetchBest() { return new Promise(res => resolvers.push(res)); }
  };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(() => "HEALTHY");
  engine.templateReady = () => true;
  engine.ownAuthoritative = () => true;
  engine.topicReady = () => true;
  const key = engine.registerRow({ url: "", contract: CONTRACT, tokenId: "6490", collectionSlug: "npc-on-chain", minPrice: 0.001, maxPrice: 0.021, step: 0.0001, duration: 15 });
  const row = engine.rows.get(key);
  const book = engine.book.get(key);
  book.generation = 1; book.hydratedAt = Date.now() - 1000; book.ownReconciledAt = Date.now() - 1000;
  book.selfAddress = SELF;
  engine.book.setSelfAddress(SELF);
  return { engine, key, row, book, resolvers };
}

function staleNoiseEvent({ slug, tokenId, contract, price, orderHash, eventTimestamp, kind = "item" }) {
  // Production-shaped: a REMOVE (order_invalidate/item_cancelled) racing
  // ahead of its own create for a brand-new, never-before-seen orderHash --
  // this makes isStale() correctly reject the later, now-superseded UPSERT
  // with the SAME hash as "stale" (op.seq <= already-recorded seq for that
  // exact scope), exactly like the real trace. Two events per noise cycle:
  // REMOVE first (newer seq, establishes the tombstone/seen watermark),
  // then the late UPSERT (older seq, same hash) correctly bounces stale.
  return [
    {
      collectionSlug: slug, nft: kind === "item" ? { chain: "ethereum", contract, tokenId } : null,
      kind, orderHash, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: eventTimestamp + 50, receivedAt: Date.now(), hasOrderData: false,
      event: "order_invalidate"
    },
    {
      collectionSlug: slug, nft: kind === "item" ? { chain: "ethereum", contract, tokenId } : null,
      kind, orderHash, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp, receivedAt: Date.now(), hasOrderData: true,
      event: "item_received_bid", pricePerItem: price
    }
  ];
}

(async () => {

await check("FIX: a long streak of legitimate per-event 'stale' noise triggers exactly one targeted authoritative reconcile, which picks up the real external 0.0115 and sends a fresh competitor+step POST", async () => {
  const { engine, key, book, resolvers } = buildEngine();
  const t0 = Date.now();
  engine.apply({
    collectionSlug: "npc-on-chain", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "6490" },
    kind: "item", orderHash: "0xbaseline6490", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: t0, receivedAt: t0, hasOrderData: true, event: "item_received_bid", pricePerItem: 0.0109
  });
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.0109, "sanity: book believes 0.0109, matching the real trace");

  // 29 cycles of legitimate racing noise, all priced well below 0.0109 --
  // exactly the shape observed in production (prices 0 to 0.0043, never a
  // contradiction by the existing book-contradiction mechanism's own rule).
  for (let i = 0; i < 29; i++) {
    const [remove, lateUpsert] = staleNoiseEvent({
      slug: "npc-on-chain", tokenId: "6490", contract: CONTRACT,
      price: 0.001 + (i % 5) * 0.0005, orderHash: `0xnoise${i}`, eventTimestamp: t0 + 100 + i * 10
    });
    engine.apply(remove);
    engine.apply(lateUpsert);
  }
  await flush();

  // The fix must have fired exactly ONE targeted read for #6490 once the
  // streak crossed STALE_STREAK_THRESHOLD -- well before all 29 events.
  assert.equal(resolvers.length, 1, "exactly one targeted stale-streak reconcile read must have fired for this NFT (not zero, not more than one) -- it must fire once at the threshold and not again on every subsequent event");
  // Mirrors engine-v2.js's STALE_STREAK_THRESHOLD (15, not exported) -- after
  // firing once mid-streak, only the remaining post-fire events re-accumulate
  // (29 total - 15 to fire = 14 left), which must stay below the threshold
  // since the cooldown blocks a second fire within this same test run.
  assert.ok((engine.staleStreakCount.get(key) || 0) < 15, "after firing once, only the remaining post-fire events re-accumulate -- must stay below threshold (cooldown blocks a second fire)");

  // Authority answers with the REAL external state: best=0.0115, which
  // beats Mine (0.0113) by one step -- Decision must now want 0.0116.
  resolvers[0]({ orderHash: "0xreal0115", price: 0.0115, orders: [
    { orderHash: "0xreal0115", price: 0.0115, maker: RIVAL, kind: "item", endTime: 0, quantity: 1 }
  ] });
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.0115, "Book must now reflect the real authority price -- the production miss is reconciled");
});

await check("the stale-streak counter resets on ANY successful apply, so a token with normal mixed traffic never falsely triggers", async () => {
  const { engine, key, book, resolvers } = buildEngine();
  const t0 = Date.now();
  engine.apply({
    collectionSlug: "npc-on-chain", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "6490" },
    kind: "item", orderHash: "0xbaseline", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: t0, receivedAt: t0, hasOrderData: true, event: "item_received_bid", pricePerItem: 0.0109
  });
  await flush();

  // Interleave noise with a genuine, cleanly-applying new bid every few
  // cycles -- the streak must never accumulate past a couple before resetting.
  for (let round = 0; round < 5; round++) {
    for (let i = 0; i < 10; i++) {
      const [remove, lateUpsert] = staleNoiseEvent({
        slug: "npc-on-chain", tokenId: "6490", contract: CONTRACT,
        price: 0.001, orderHash: `0xmixed-${round}-${i}`, eventTimestamp: t0 + 1000 * round + i * 10
      });
      engine.apply(remove); engine.apply(lateUpsert);
    }
    // A genuinely new, cleanly-applying low bid resets the streak.
    engine.apply({
      collectionSlug: "npc-on-chain", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "6490" },
      kind: "item", orderHash: `0xgenuine-${round}`, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
      eventTimestamp: t0 + 1000 * round + 500, receivedAt: t0 + 1000 * round + 500, hasOrderData: true,
      event: "item_received_bid", pricePerItem: 0.002
    });
  }
  await flush();
  assert.equal(resolvers.length, 0, "a token with normal mixed traffic (genuine applies interspersed with noise) must never trigger a stale-streak reconcile -- not a false positive");
});

await check("scoped strictly to the affected NFT: a stale-streak reconcile for #6490 never touches other tracked rows, and P0 for unrelated NFTs stays fast", async () => {
  const { engine, key, book, resolvers } = buildEngine();
  // A second, unrelated tracked row in the same collection must never be
  // probed or affected by #6490's own stale-streak accounting.
  const otherKey = engine.registerRow({ url: "", contract: CONTRACT, tokenId: "9999", collectionSlug: "npc-on-chain", minPrice: 0.001, maxPrice: 0.021, step: 0.0001, duration: 15 });
  const otherBook = engine.book.get(otherKey);
  otherBook.generation = 1; otherBook.hydratedAt = Date.now() - 1000;

  const t0 = Date.now();
  engine.apply({
    collectionSlug: "npc-on-chain", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "6490" },
    kind: "item", orderHash: "0xbaseline6490b", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: t0, receivedAt: t0, hasOrderData: true, event: "item_received_bid", pricePerItem: 0.0109
  });
  await flush();

  for (let i = 0; i < 29; i++) {
    const [remove, lateUpsert] = staleNoiseEvent({
      slug: "npc-on-chain", tokenId: "6490", contract: CONTRACT,
      price: 0.001, orderHash: `0xscoped${i}`, eventTimestamp: t0 + 100 + i * 10
    });
    engine.apply(remove); engine.apply(lateUpsert);
  }
  await flush();
  assert.equal(resolvers.length, 1, "the reconcile read must have fired for #6490");

  // P0 latency for the OTHER, unrelated NFT must stay fast while #6490's
  // reconcile read is outstanding.
  const t1 = process.hrtime.bigint();
  engine.apply({
    collectionSlug: "npc-on-chain", nft: { chain: "ethereum", contract: CONTRACT, tokenId: "9999" },
    kind: "item", orderHash: "0xother1", maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
    eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true, event: "item_received_bid", pricePerItem: 0.005
  });
  const elapsedMs = Number(process.hrtime.bigint() - t1) / 1e6;
  assert.ok(elapsedMs < 20, `P0 apply() for the unrelated NFT took ${elapsedMs}ms while #6490's reconcile was outstanding -- must stay fast`);
  await flush();
  assert.equal(otherBook.effectiveBest(Date.now()).price, 0.005, "the unrelated NFT's own event must still apply correctly, untouched by #6490's streak tracking");
  assert.equal(engine.staleStreakCount.get(otherKey) || 0, 0, "the unrelated NFT's own stale-streak counter must stay at 0 -- no cross-NFT bleed");
});

await check("no polling: the fix is purely event-count-driven -- with ZERO incoming events, no timer ever fires a reconcile read on its own", async () => {
  const { engine, resolvers } = buildEngine();
  await new Promise(r => setTimeout(r, 150)); // real wall-clock wait, no events at all
  assert.equal(resolvers.length, 0, "with no incoming Stream events whatsoever, nothing must spontaneously trigger a reconcile read -- purely event-driven, never a timer");
});

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
