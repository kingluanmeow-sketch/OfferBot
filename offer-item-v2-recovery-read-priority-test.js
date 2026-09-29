"use strict";

const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const { ChainAdapter } = require("./offer-item-v2/chain-adapters");
const { PRIORITY } = require("./rate-limiter");
const opensea = require("./opensea");

let passed = 0;
let failed = 0;
function check(name, fn) {
  try { fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}

async function main() {
  const row = { key: "ethereum:0x1111111111111111111111111111111111111111:1",
    contract: "0x1111111111111111111111111111111111111111", tokenId: "1",
    collectionSlug: "alpha", running: true };
  let engineJob = null;
  let engineOptions = null;
  const engine = new OfferItemEngineV2({
    adapter: { async fetchBest(_row, options) { engineOptions = options; return { empty: true, orders: [] }; } },
    onLog() {}
  });
  engine.state = STATE.RUNNING;
  engine.rows.set(row.key, row);
  engine.book.add({ key: row.key, chain: "ethereum", contract: row.contract, tokenId: row.tokenId });
  engine.recovery.push = job => { engineJob = job; return true; };
  engine.queueRead(row, { reason: "topic-gap", firstRead: false, authoritative: false,
    readAt: Date.now(), attempt: 1 });
  if (engineJob) await engineJob.run({ signal: new AbortController().signal });
  check("topic recovery HTTP read is explicitly P2 from engine", () => {
    assert(engineOptions);
    assert.equal(engineOptions.priority, PRIORITY.P2);
  });
  engine.recovery.stop();

  const originalFull = opensea.fetchBestOffer;
  const originalQuick = opensea.fetchBestOfferQuick;
  const fullOptions = [];
  const quickOptions = [];
  opensea.fetchBestOffer = async (...args) => {
    fullOptions.push(args[4]);
    return { ok: true, price: 0.02, maker: "0x2222222222222222222222222222222222222222",
      orderHash: "0xabc", kind: "item" };
  };
  opensea.fetchBestOfferQuick = async (...args) => {
    quickOptions.push(args[4]);
    return { ok: true, price: 0.02, orderHash: "0xabc", kind: "item",
      order: { maker: "0x2222222222222222222222222222222222222222" } };
  };
  try {
    const adapter = new ChainAdapter({ chain: "ethereum", onLog() {} });
    await adapter.fetchBest(row, { priority: PRIORITY.P2 });
    await adapter.fetchBestQuick(row, { priority: PRIORITY.P2 });
    opensea.fetchBestOfferQuick = async (...args) => {
      quickOptions.push(args[4]);
      return { empty: true, reason: "NOT_ACTIVE" };
    };
    await adapter.fetchBestQuick(row, { priority: PRIORITY.P2 });
    check("full and quick adapter reads preserve P2 through OpenSea", () => {
      assert.equal(fullOptions[0].priority, PRIORITY.P2);
      assert.equal(quickOptions[0].priority, PRIORITY.P2);
      assert.equal(quickOptions[1].priority, PRIORITY.P2);
      assert.equal(fullOptions[1].priority, PRIORITY.P2);
    });
  } finally {
    opensea.fetchBestOffer = originalFull;
    opensea.fetchBestOfferQuick = originalQuick;
  }

  const initialOptions = [];
  opensea.fetchBestOffer = async (...args) => {
    initialOptions.push(args[4]);
    return { ok: true, price: 0.02, maker: "0x2222222222222222222222222222222222222222",
      orderHash: "0xabc", kind: "item" };
  };
  try {
    const adapter = new ChainAdapter({ chain: "ethereum", onLog() {} });
    await adapter.fetchBest({ ...row, firstRead: true }, { priority: PRIORITY.INITIAL });
    check("first authority keeps INITIAL priority", () => {
      assert.equal(initialOptions[0].priority, PRIORITY.INITIAL);
    });
  } finally {
    opensea.fetchBestOffer = originalFull;
  }

  process.stdout.write(`\n${passed}/${passed + failed} recovery read-priority checks passed\n`);
  if (failed) process.exitCode = 1;
}

main().catch(error => { process.stderr.write(`${error.stack || error}\n`); process.exitCode = 1; });
