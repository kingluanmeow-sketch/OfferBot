"use strict";
const assert = require("assert");
const opensea = require("./opensea");
const { EthereumAdapter } = require("./offer-item-v2/chain-adapters");
const contract = "0x1111111111111111111111111111111111111111";
const base = { criteria: { collection: { slug: "sample" }, contract: { address: contract }, encoded_token_ids: "10,12:14" } };

for (const criteria of [
  { ...base.criteria, traits: [{ type: "Background", value: "Blue" }] },
  { ...base.criteria, numeric_traits: [{ type: "Rank", min: 1, max: 5 }] },
  { ...base.criteria, trait: { type: "Background", value: "Blue" } }
]) {
  assert.strictEqual(opensea.isTraitCriteria(criteria), true);
  assert.strictEqual(opensea.classifyOffer({ ...base, criteria }, contract, "10"), "trait");
}
assert.strictEqual(opensea.isTraitCriteria(base.criteria), false);
assert.strictEqual(opensea.classifyOffer(base, contract, "10"), "collection");
assert.strictEqual(opensea.criteriaCoversToken(base, "13"), true);
assert.strictEqual(opensea.criteriaCoversToken(base, "15"), false);

const original = opensea.fetchCollectionOffers;
opensea.fetchCollectionOffers = async () => [
  { ...base, criteria: { ...base.criteria, traits: [{ type: "Background", value: "Blue" }] },
    order_hash: `0x${"a".repeat(64)}`, price: { value: "20000000000000000", decimals: 18 },
    remaining_quantity: 1, status: "ACTIVE", protocol_data: { parameters: { offerer: "0x2222222222222222222222222222222222222222", consideration: [{ itemType: 4, startAmount: "1" }], endTime: String(Math.floor(Date.now() / 1000) + 3600) } } }
];
(async () => {
  try {
    const orders = await new EthereumAdapter().fetchCollectionOffersFor("sample");
    assert.strictEqual(orders.length, 1);
    assert.strictEqual(orders[0].kind, "trait");
    assert.strictEqual(orders[0].covers("13"), true);
    assert.strictEqual(orders[0].covers("15"), false);
    process.stdout.write("PASS current OpenSea traits and numeric_traits classification\n");
  } finally { opensea.fetchCollectionOffers = original; }
})().catch(error => { process.stderr.write(`${error.stack}\n`); process.exitCode = 1; });
