"use strict";

/**
 * TERMINAL CANCEL vs ORDERING HEURISTIC (audit 2026-10-04)
 *
 * `TokenBook.apply()` gates every op through `isStale(op)` before it reaches
 * the REMOVE branch. `isStale` compares `op.seq` (the event's
 * `event_timestamp`, or the local `receivedAt` when the payload carries no
 * timestamp) against the highest seq already seen for that order hash, and
 * treats `op.seq <= last` as a replay.
 *
 * For an UPSERT that is the right call: re-applying an identical or older
 * frame only creates work. For a HARD cancel (`item_cancelled` / `item_sold`,
 * `soft === false`) it is not: the cancel is terminal, and discarding it
 * leaves a DEAD order counting as Best -- so the next target is computed as
 * `deadBest + step` and the bot overpays -- and, worse, leaves no tombstone,
 * so the dead hash can be re-added by any later frame.
 *
 * The seq space is not a single clock: `event-normalizer.js` falls back to the
 * local `receivedAt` when `event_timestamp` is absent, so one order's seq
 * history can mix OpenSea's clock with this machine's. A bid that fell back to
 * local time lands ABOVE a genuinely later wire-timestamped cancel, which is
 * how a terminal cancel gets discarded without any replay actually happening.
 *
 * A soft `order_invalidate` is deliberately NOT covered by this: OpenSea
 * documents it as reversible (`order_revalidate` can bring the same hash
 * back), the normalizer marks it `soft: true`, and 1.25.33 established that it
 * must not be treated as a hard cancel. Its ordering behaviour is asserted
 * here only to prove this change did not widen into it.
 */

const assert = require("node:assert/strict");
const { TokenBook, MAX_ORDERS_PER_GROUP } = require("./offer-item-v2/memory-book.js");
const { toBookOp } = require("./offer-item-v2/event-normalizer.js");

let passed = 0, failed = 0;
function check(name, fn) {
  try { fn(); passed++; console.log(`PASS ${name}`); }
  catch (error) { failed++; console.log(`FAIL ${name}: ${error.message}`); }
}

// Shaped on the real production trace for Hooligan #1859
// (saints-of-la-eternal), so the op fields are the ones the live normalizer
// actually produces rather than a hand-made minimum.
const CONTRACT = "0x7cab6c0e4dc14b995901f5d672cdcc8469cc459d";
const TOKEN = "1859";
const SLUG = "saints-of-la-eternal";
const HASH = "0x93e562540b7fc00c2442479ce5fee16f78b72d9b501d35dc2d436f395a146ad0";
const OTHER_HASH = "0x6e249a071875fead7a3d0890a1e82fbf640d7a962a5f31fe3727067a3ee5ba41";
const RIVAL = "0x2222222222222222222222222222222222222222";
const WIRE_TS = 1791055181434;

function bid({ price, evTs, version = 0, orderHash = HASH, quantity = 1 }) {
  return {
    event: "item_received_bid",
    collectionSlug: SLUG,
    nft: { chain: "ethereum", contract: CONTRACT, tokenId: TOKEN },
    kind: "item",
    orderHash,
    maker: RIVAL,
    pricePerItem: price,
    quantity,
    currency: "WETH",
    endTime: Math.floor(Date.now() / 1000) + 3600,
    eventTimestamp: evTs,
    version,
    receivedAt: Date.now(),
    hasOrderData: true
  };
}

function terminal({ evTs, version = 0, eventName = "item_cancelled", orderHash = HASH, quantity = 1 }) {
  return {
    event: eventName,
    collectionSlug: SLUG,
    nft: { chain: "ethereum", contract: CONTRACT, tokenId: TOKEN },
    kind: "item",
    orderHash,
    maker: RIVAL,
    quantity,
    eventTimestamp: evTs,
    version,
    receivedAt: Date.now(),
    hasOrderData: true
  };
}

function seededBook(bidEvent) {
  const book = new TokenBook();
  assert.equal(book.apply(toBookOp(bidEvent)), true, "fixture precondition: the bid must enter the book");
  return book;
}

// ---- Control: a strictly-newer hard cancel already worked, and must keep
// working exactly as before. -------------------------------------------------
check("control: hard cancel with a strictly newer timestamp removes the order and tombstones it", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS }));
  const changed = book.apply(toBookOp(terminal({ evTs: WIRE_TS + 1500 })));
  assert.equal(changed, true, "a newer hard cancel must change the book");
  assert.equal(book.effectiveBest().price, 0, "the cancelled order must stop counting as Best");
  assert.equal(book.isTombstoned(HASH), true, "a hard cancel must tombstone the hash");
});

// ---- The defect: a terminal cancel discarded by the ordering heuristic. ----
check("hard cancel with the SAME timestamp as the bid still removes the dead order from Best", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS }));
  book.apply(toBookOp(terminal({ evTs: WIRE_TS })));
  assert.equal(book.effectiveBest().price, 0,
    "equal-seq is a replay guard for UPSERTs, but a hard cancel is terminal -- a dead order must never keep counting as Best");
});

check("hard cancel with the SAME timestamp as the bid still tombstones the hash", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS }));
  book.apply(toBookOp(terminal({ evTs: WIRE_TS })));
  assert.equal(book.isTombstoned(HASH), true,
    "without the tombstone the cancelled hash can be re-added by any later frame");
});

check("a wire-timestamped hard cancel is not discarded because the bid's seq fell back to local receivedAt", () => {
  // `event_timestamp` absent -> event-normalizer falls back to receivedAt
  // (local clock), which sits far above OpenSea's wire clock. The cancel that
  // follows is genuinely later in real time, not a replay.
  const book = seededBook(bid({ price: 0.0265, evTs: 0 }));
  const cancelOp = toBookOp(terminal({ evTs: WIRE_TS }));
  assert.ok(cancelOp.seq < book.seen.get(cancelOp.scope),
    "fixture precondition: the cancel must look older than the bid under the mixed seq space");
  book.apply(cancelOp);
  assert.equal(book.effectiveBest().price, 0,
    "two clocks share one seq space; that must not be able to discard a terminal cancel");
  assert.equal(book.isTombstoned(HASH), true);
});

// ---- Bounds of the change: the authoritative signal still wins, soft
// invalidation stays reversible, and nothing leaks across order hashes. ------
check("OpenSea's own per-order version still overrides: a hard cancel carrying a LOWER version is still refused", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 5 }));
  book.apply(toBookOp(terminal({ evTs: WIRE_TS + 1500, version: 2 })));
  assert.equal(book.effectiveBest().price, 0.0265,
    "`version` is OpenSea's authoritative revision counter -- a lower one is genuinely older state, not a late delivery");
  assert.equal(book.isTombstoned(HASH), false, "refused state must not tombstone");
});

check("a soft order_invalidate stays reversible: equal-seq is still refused and never tombstones", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS }));
  book.apply(toBookOp(terminal({ evTs: WIRE_TS, eventName: "order_invalidate" })));
  assert.equal(book.isTombstoned(HASH), false,
    "order_invalidate is documented as reversible (order_revalidate) -- it must never tombstone");
});

check("an exact duplicate hard cancel is idempotent: the second one reports no further change", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS }));
  const first = book.apply(toBookOp(terminal({ evTs: WIRE_TS + 1500 })));
  const second = book.apply(toBookOp(terminal({ evTs: WIRE_TS + 1500 })));
  assert.equal(first, true, "the first hard cancel removes the order");
  assert.equal(second, false, "the cross-feed echo must not report a second book change");
  assert.equal(book.effectiveBest().price, 0);
});

// `item_sold` must NOT inherit the terminal bypass. Per the installed SDK
// (node_modules/@opensea/sdk/lib/stream/types.d.ts:165,
// `ItemSoldEventPayload extends BaseOrderEventPayload`) the payload carries
// only the quantity sold in THAT sale -- there is no remaining-quantity
// field -- so a partially filled multi-quantity collection offer is
// indistinguishable from a fully consumed one. Letting it past the seq gate
// would delete and tombstone an order that is still live for the rest of its
// quantity.
check("item_sold does NOT get the terminal bypass: an equal-seq partial fill leaves the still-live order in the book", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, quantity: 5 }));
  const soldOp = toBookOp(terminal({ evTs: WIRE_TS, eventName: "item_sold", quantity: 1 }));
  assert.equal(soldOp.terminal, false, "item_sold must not be flagged terminal");
  book.apply(soldOp);
  assert.equal(book.effectiveBest().price, 0.0265,
    "a partial fill must not wipe an order that still has quantity left");
  assert.equal(book.isTombstoned(HASH), false,
    "and must not tombstone it, which would block the remaining quantity from being re-added");
});

check("item_sold with a strictly newer seq keeps its pre-existing behaviour (unchanged by this patch)", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, quantity: 5 }));
  const changed = book.apply(toBookOp(terminal({ evTs: WIRE_TS + 1500, eventName: "item_sold", quantity: 1 })));
  assert.equal(changed, true, "a newer item_sold still removes the order, exactly as before this patch");
});

// ---- Event-family version scales ------------------------------------------
//
// OpenSea's stream-js "Event Versioning" (ProjectOpenSea/stream-js README)
// defines TWO different scales on the same `version` field:
//
//   order revision counter  item_listed, item_cancelled, item_received_offer,
//                           item_received_bid, collection_offer, trait_offer,
//                           order_invalidate, order_revalidate
//   epoch milliseconds      item_transferred, item_sold, item_metadata_updated
//
// and states: "Never compare `version` across different event families or
// across unrelated entities -- only compare versions for the same entity
// within the same event family." The two scales are "not comparable to each
// other".
//
// `item_sold` is the only epoch-ms event that reaches the book at all
// (item_listed / item_transferred / item_metadata_updated are OP.IGNORE and
// event-normalizer.js:133 drops them), and it shares the filled order's hash
// -- so it is the one event that can push a 13-digit number into the version
// state of an order whose own family counts 1, 2, 3.

const EPOCH_MS_VERSION = 1791055181434;

check("an epoch-ms item_sold version must not block a later order-revision item_cancelled", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 2, quantity: 5 }));
  // Epoch-ms family: a sale against the same order hash poisons the version
  // state. Assert on isStale, not on the end state -- an item_sold that gets
  // through has already emptied the book, so an end-state assertion here
  // would pass even while the version gate was still refusing the cancel.
  book.apply(toBookOp(terminal({ evTs: WIRE_TS + 100, eventName: "item_sold", version: EPOCH_MS_VERSION, quantity: 1 })));
  const cancel = toBookOp(terminal({ evTs: WIRE_TS + 5000, version: 3 }));
  assert.equal(book.isStale(cancel), false,
    "a 13-digit epoch version from item_sold must not make a revision-3 cancellation look older");
});

check("version poisoning cannot survive into a re-added order: cancel still ends a hash re-added after a sale", () => {
  // The order hash is sold (epoch-ms version recorded), then the same hash is
  // re-added by a later order-revision bid, then genuinely cancelled.
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 2, quantity: 5 }));
  book.apply(toBookOp(terminal({ evTs: WIRE_TS + 100, eventName: "item_sold", version: EPOCH_MS_VERSION, quantity: 1 })));
  assert.equal(book.isTombstoned(HASH), true, "item_sold is not soft, so it tombstones");
});

check("an epoch-ms item_sold version must not block a later order-revision bid for the same hash", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 2, quantity: 5 }));
  book.apply(toBookOp(terminal({ evTs: WIRE_TS + 100, eventName: "item_sold", version: EPOCH_MS_VERSION, quantity: 1 })));
  const reprice = toBookOp(bid({ price: 0.0302, evTs: WIRE_TS + 5000, version: 3, quantity: 4 }));
  assert.equal(book.isStale(reprice), false,
    "a revision-3 repricing of a live order must not be refused because of an epoch-ms number");
});

check("an epoch-ms item_sold version must not block a later order-revision order_invalidate", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 2, quantity: 5 }));
  book.apply(toBookOp(terminal({ evTs: WIRE_TS + 100, eventName: "item_sold", version: EPOCH_MS_VERSION, quantity: 1 })));
  const invalidate = toBookOp(terminal({ evTs: WIRE_TS + 5000, eventName: "order_invalidate", version: 3 }));
  assert.equal(book.isStale(invalidate), false,
    "order_invalidate is order-revision family -- it must be ordered against order revisions only");
});

check("the reverse direction too: an order-revision version must not block a later epoch-ms item_sold", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 7, quantity: 5 }));
  const sold = toBookOp(terminal({ evTs: WIRE_TS + 100, eventName: "item_sold", version: EPOCH_MS_VERSION, quantity: 1 }));
  assert.equal(book.isStale(sold), false, "epoch-ms must be ordered against epoch-ms only");
});

check("within the epoch-ms family, an older item_sold is still refused", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 2, quantity: 5 }));
  book.apply(toBookOp(terminal({ evTs: WIRE_TS + 100, eventName: "item_sold", version: EPOCH_MS_VERSION, quantity: 1 })));
  const older = toBookOp(terminal({ evTs: WIRE_TS + 200, eventName: "item_sold", version: EPOCH_MS_VERSION - 60000, quantity: 1 }));
  assert.equal(book.isStale(older), true,
    "same family, lower epoch -- that really is older state and must stay refused");
});

check("within the order-revision family, out-of-order delivery is still refused", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 5 }));
  const replayed = toBookOp(terminal({ evTs: WIRE_TS + 9000, eventName: "order_invalidate", version: 3 }));
  assert.equal(book.isStale(replayed), true,
    "revision 3 after revision 5 in the SAME family is genuinely older state");
});

check("a soft invalidate stays recoverable: no tombstone, so a later higher-revision bid re-adds the hash", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 1 }));
  book.apply(toBookOp(terminal({ evTs: WIRE_TS + 1000, eventName: "order_invalidate", version: 2 })));
  assert.equal(book.effectiveBest().price, 0, "the invalidated order leaves the book");
  assert.equal(book.isTombstoned(HASH), false, "but is never tombstoned");
  book.apply(toBookOp(bid({ price: 0.0265, evTs: WIRE_TS + 2000, version: 3 })));
  assert.equal(book.effectiveBest().price, 0.0265,
    "so a revalidation-driven re-read can bring the same hash back");
});

check("the per-family version state stays bounded and keeps working after the seen-map is capped", () => {
  // noteSeen() caps `seen` and then drops every seenVersion key that is no
  // longer in it. seenVersion is keyed by the bare scope (the order hash),
  // with the two family counters inside the entry -- if that key format ever
  // drifts, this prune silently deletes everything and the version gate stops
  // working without any test noticing.
  const book = new TokenBook();
  const bulk = MAX_ORDERS_PER_GROUP * 4 + 10;
  for (let i = 0; i < bulk; i++) {
    book.apply(toBookOp(bid({
      price: 0.001, evTs: WIRE_TS + i, version: 2,
      orderHash: "0x" + String(i).padStart(64, "0")
    })));
  }
  assert.ok(book.seen.size <= MAX_ORDERS_PER_GROUP * 4,
    "the ordering map must stay under its cap");
  assert.ok(book.seenVersion.size <= book.seen.size,
    "version state must never outgrow the ordering map it is pruned against");
  for (const entry of book.seenVersion.values()) {
    assert.ok(Object.keys(entry).length <= 2,
      "an entry holds at most one counter per event family");
  }
  // The gate still functions for a hash that survived the cap.
  const survivor = [...book.seen.keys()][0];
  const older = toBookOp(bid({ price: 0.002, evTs: WIRE_TS + bulk + 100, version: 1, orderHash: survivor }));
  assert.equal(book.isStale(older), true,
    "a lower same-family revision for a surviving hash must still be refused after the prune");
});

// ---- The ordering-map cap must not evict an order that is still in the book
//
// `noteSeen` caps `seen` (and prunes `seenVersion` against it) once it passes
// MAX_ORDERS_PER_GROUP * 4. It used to keep "the newest by seq", which throws
// away the ordering state of an order that is STILL LIVE in the book whenever
// its own event timestamp is older than the churn around it (a long-expiry
// offer on a busy collection). Losing that state is losing it exactly when it
// is still needed: a later REMOVE carrying an OLDER seq then has nothing to
// compare against, passes isStale, and deletes a live order -- Best drops
// below reality and the bot bids low.

function churnBook(book, count, baseTs) {
  for (let i = 0; i < count; i++) {
    const h = "0x" + String(i).padStart(64, "0");
    book.apply(toBookOp(bid({ price: 0.0001, evTs: baseTs + 1000 + i, version: 1, orderHash: h })));
    book.apply(toBookOp(terminal({ evTs: baseTs + 2000 + i, version: 2, orderHash: h })));
  }
}

check("the ordering-map cap keeps the state of an order that is still in the book", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 2 }));
  churnBook(book, MAX_ORDERS_PER_GROUP * 4 + 50, WIRE_TS);
  assert.equal(book.item.has(HASH), true, "fixture: the valuable order is still in the book");
  assert.equal(book.seen.has(HASH), true,
    "its seq landmark must survive the cap -- it is still a live order");
  const entry = book.seenVersion && book.seenVersion.get(HASH);
  assert.ok(entry && entry.orderRevision === 2,
    "and so must its revision, which is pruned against the same map");
});

check("after the cap, a late OLDER-seq hard cancel can no longer delete a live order", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 2 }));
  churnBook(book, MAX_ORDERS_PER_GROUP * 4 + 50, WIRE_TS);
  const lateCancel = toBookOp(terminal({ evTs: WIRE_TS - 5000, version: 1 }));
  assert.equal(book.isStale(lateCancel), true,
    "older seq AND older revision for a hash still in the book is genuinely stale");
  book.apply(lateCancel);
  assert.equal(book.effectiveBest().price, 0.0265,
    "a stale cancel must not be able to kill a live order just because the map was capped");
});

check("the ordering map stays bounded after the cap, with no unbounded growth", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 2 }));
  churnBook(book, MAX_ORDERS_PER_GROUP * 10, WIRE_TS);
  // Worst case: every live group full (capGroup bounds each at
  // MAX_ORDERS_PER_GROUP) plus the seq-ranked remainder.
  assert.ok(book.seen.size <= MAX_ORDERS_PER_GROUP * 4,
    `seen must stay bounded (saw ${book.seen.size})`);
  assert.ok(book.seenVersion.size <= book.seen.size,
    "version state is pruned against the ordering map and cannot outgrow it");
});

check("a genuinely NEWER cancel still ends a live order after the cap -- no immortal orders", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS, version: 2 }));
  churnBook(book, MAX_ORDERS_PER_GROUP * 4 + 50, WIRE_TS);
  book.apply(toBookOp(terminal({ evTs: WIRE_TS + 900000, version: 3 })));
  assert.equal(book.effectiveBest().price, 0,
    "keeping ordering state must not make the order unkillable");
  assert.equal(book.isTombstoned(HASH), true);
});

check("a hard cancel only ends its own order: a different live hash keeps counting as Best", () => {
  const book = seededBook(bid({ price: 0.0265, evTs: WIRE_TS }));
  assert.equal(book.apply(toBookOp(bid({ price: 0.0301, evTs: WIRE_TS, orderHash: OTHER_HASH }))), true);
  book.apply(toBookOp(terminal({ evTs: WIRE_TS })));
  assert.equal(book.effectiveBest().price, 0.0301,
    "two order hashes have no shared ordering -- cancelling one must not touch the other");
  assert.equal(book.isTombstoned(OTHER_HASH), false);
});

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
