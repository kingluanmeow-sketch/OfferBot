"use strict";

const assert = require("node:assert/strict");
const { parseCriteria, appliesTo, tokenTraits, APPLIES } = require("./offer-item-v2/trait-criteria");
const { toBookOp } = require("./offer-item-v2/event-normalizer");
const { MemoryBook } = require("./offer-item-v2/memory-book");
const { decodeEvent } = require("./stream");

const numericRest = parseCriteria({ criteria: {
  numeric_traits: [{ type: "Level", min: 5, max: 10 }]
} });
assert.equal(numericRest.ambiguous, false);
assert.deepEqual(numericRest.conditions[0], {
  kind: "numeric", type: "Level", key: "level", min: 5, max: 10
});
assert.equal(appliesTo(numericRest, tokenTraits([{ trait_type: "Level", value: "5" }])), APPLIES.MATCH);
assert.equal(appliesTo(numericRest, tokenTraits([{ trait_type: "Level", value: 10 }])), APPLIES.MATCH);
assert.equal(appliesTo(numericRest, tokenTraits([{ trait_type: "Level", value: "10.01" }])), APPLIES.NO_MATCH);
assert.equal(appliesTo(numericRest, tokenTraits([{ trait_type: "Rank", value: "7" }])), APPLIES.NO_MATCH);
assert.equal(appliesTo(numericRest, tokenTraits(undefined)), APPLIES.UNKNOWN);
const restStringAndNumeric = parseCriteria({ criteria: {
  traits: [{ type: "Background", value: "Blue" }],
  numeric_traits: [{ type: "Level", min: 5, max: 10 }]
} });
assert.equal(appliesTo(restStringAndNumeric, tokenTraits([
  { trait_type: "Background", value: "Blue" }, { trait_type: "Level", value: 7 }
])), APPLIES.MATCH);

const openRange = parseCriteria({ numeric_trait_criteria_list: [
  { trait_type: "Power", min_value: 3 }
] });
assert.equal(appliesTo(openRange, tokenTraits([{ trait_type: "Power", value: "3" }])), APPLIES.MATCH);
assert.equal(appliesTo(openRange, tokenTraits([{ trait_type: "Power", value: "2.99" }])), APPLIES.NO_MATCH);
assert.equal(parseCriteria({ numeric_traits: [{ type: "Power", min: 9, max: 2 }] }).ambiguous,
  "numeric-invalid");

const mixedAnd = parseCriteria({
  trait_criteria_list: [{ trait_type: "Background", trait_name: "Blue" }],
  numeric_trait_criteria_list: [{ type: "Level", min: 5, max: 10 }]
});
assert.equal(appliesTo(mixedAnd, tokenTraits([
  { trait_type: "Background", value: "Blue" }, { trait_type: "Level", value: 6 }
])), APPLIES.MATCH);
assert.equal(appliesTo(mixedAnd, tokenTraits([
  { trait_type: "Background", value: "Red" }, { trait_type: "Level", value: 6 }
])), APPLIES.NO_MATCH);

const book = new MemoryBook();
const rows = [
  ["n:5", "5", [{ trait_type: "Level", value: "5" }]],
  ["n:10", "10", [{ trait_type: "Level", value: "10" }]],
  ["n:11", "11", [{ trait_type: "Level", value: "11" }]],
  ["n:no-level", "12", [{ trait_type: "Background", value: "Blue" }]],
  ["n:unknown", "13", undefined]
];
for (const [key, tokenId, traits] of rows) {
  book.add({ key, chain: "ethereum", contract: "0xabc", tokenId, collectionSlug: "numeric-test", traits });
}
const decoded = decodeEvent({ event: "trait_offer", topic: "collection:numeric-test", payload: { payload: {
  order_hash: `0x${"1".repeat(64)}`, maker: { address: "0x2222222222222222222222222222222222222222" },
  base_price: "250000000000000000", payment_token: { decimals: 18, symbol: "WETH" }, quantity: 1,
  numeric_trait_criteria_list: [{ trait_type: "Level", min_value: 5, max_value: 10 }],
  event_timestamp: "2026-01-01T00:00:00Z"
} } });
assert.equal(decoded.kind, "trait");
assert.equal(decoded.pricePerItem, 0.25);
const op = toBookOp(decoded);
assert.ok(op);
const touched = book.apply(op, 100);
assert.deepEqual(new Set(touched), new Set(["n:5", "n:10"]));
assert.equal(book.get("n:5").candidates(100).trait.price, 0.25);
assert.equal(book.get("n:11").candidates(100).trait.price, 0);
assert.equal(book.get("n:no-level").candidates(100).trait.price, 0);
assert.deepEqual(touched.unresolvedTrait, ["n:unknown"]);

process.stdout.write("PASS numeric trait parsing, range matching, and per-token MemoryBook fan-out\n");
