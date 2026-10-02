"use strict";

const assert = require("node:assert/strict");
const http = require("node:http");
const { ethers } = require("ethers");

let passed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (e) { process.stderr.write(`FAIL ${name}: ${e.stack || e}\n`); process.exitCode = 1; }
}
const tick = () => new Promise(r => setImmediate(r));

(async () => {
  await check("off-chain cancel sends current camelCase proof and exact hash", async () => {
    let seen = null;
    const server = http.createServer((req, res) => {
      const chunks = [];
      req.on("data", c => chunks.push(c));
      req.on("end", () => {
        seen = { method: req.method, url: req.url, body: JSON.parse(Buffer.concat(chunks).toString("utf8")) };
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ last_signature_issued_valid_until: "0" }));
      });
    });
    await new Promise(r => server.listen(0, "127.0.0.1", r));
    const old = process.env.OSB_OPENSEA_API_BASE;
    process.env.OSB_OPENSEA_API_BASE = `http://127.0.0.1:${server.address().port}/api/v2`;
    const mod = require.resolve("./opensea");
    delete require.cache[mod];
    try {
      const opensea = require("./opensea");
      const hash = "0x" + "ab".repeat(32);
      await opensea.postOffchainCancel("ethereum", hash, "0x1234", { apiKey: "test" });
      assert.equal(seen.method, "POST");
      assert.ok(seen.url.endsWith(`/${hash}/cancel`));
      assert.deepEqual(seen.body, { offererSignature: "0x1234" });
      assert.equal("offerer_signature" in seen.body, false);
    } finally {
      await new Promise(r => server.close(r));
      if (old === undefined) delete process.env.OSB_OPENSEA_API_BASE; else process.env.OSB_OPENSEA_API_BASE = old;
      delete require.cache[mod];
    }
  });

  await check("new templates always use SignedZone without hot-path reads", async () => {
    const { ChainAdapter } = require("./offer-item-v2/chain-adapters");
    const adapter = new ChainAdapter({ chain: "ethereum" });
    adapter.tokenStandard = async () => "erc721";
    const opensea = require("./opensea");
    const original = opensea.fetchCollectionFees;
    opensea.fetchCollectionFees = async () => ({ fees: [], requiresSignedZone: false });
    try {
      const config = await adapter.collectionConfig("alpha", "0x1111111111111111111111111111111111111111", { tokenId: "1" });
      assert.equal(config.requiresSignedZone, true);
    } finally { opensea.fetchCollectionFees = original; }
  });

  await check("failed ! hash is cancelled gaslessly before one replacement wake", async () => {
    const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
    const calls = [];
    const adapter = {
      chain: "ethereum",
      async cancelOwnOrderOffchain(hash) { calls.push(hash); return { last_signature_issued_valid_until: "0" }; }
    };
    const engine = new OfferItemEngineV2({ adapter, onLog() {} });
    engine.state = STATE.RUNNING;
    engine.epoch = 7;
    engine.builder = { address: "0x4444444444444444444444444444444444444444", wallet: new ethers.Wallet("0x" + "11".repeat(32)) };
    const key = "ethereum:0x1111111111111111111111111111111111111111:1";
    const row = { key, running: true, tokenId: "1", contract: "0x1111111111111111111111111111111111111111", collectionSlug: "alpha", minPrice: .01, maxPrice: 1, step: .0001 };
    engine.rows.set(key, row);
    const book = engine.book.add({ key, chain: "ethereum", contract: row.contract, tokenId: "1", collectionSlug: "alpha" });
    const hash = "0x" + "cd".repeat(32);
    book.own.set(hash, { orderHash: hash, price: .02, confirmed: "failed", endTime: 0, at: Date.now() });
    engine.toolOwnHashes.set(key, new Set([hash]));
    engine.evaluate = () => { engine._wakes = (engine._wakes || 0) + 1; };
    engine.pump = () => {};
    assert.equal(engine.queueGaslessCancel(key, row, hash, "unconfirmed"), true);
    for (let i = 0; i < 6; i++) await tick();
    assert.deepEqual(calls, [hash]);
    assert.equal(book.own.has(hash), false);
    assert.equal(engine._wakes, 1);
    assert.equal(engine.ownCancelJobs.has(key), false);
  });

  await check("confirmed replacement becomes canonical and queues every older exact hash once", async () => {
    const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
    const calls = [];
    const adapter = { chain: "ethereum", async cancelOwnOrderOffchain(hash) { calls.push(hash); return {}; } };
    const engine = new OfferItemEngineV2({ adapter, onLog() {} });
    engine.state = STATE.RUNNING; engine.epoch = 2;
    engine.builder = { address: "0x4444444444444444444444444444444444444444", wallet: new ethers.Wallet("0x" + "22".repeat(32)) };
    const key = "ethereum:0x1111111111111111111111111111111111111111:2";
    const row = { key, running: false, tokenId: "2", collectionSlug: "alpha" };
    engine.rows.set(key, row);
    const book = engine.book.add({ key, chain: "ethereum", contract: "0x1111111111111111111111111111111111111111", tokenId: "2", collectionSlug: "alpha" });
    const old1 = "0x" + "01".repeat(32), old2 = "0x" + "02".repeat(32), fresh = "0x" + "03".repeat(32);
    for (const h of [old1, old2, fresh]) book.own.set(h, { orderHash: h, price: .03, confirmed: true, endTime: 0, at: Date.now() });
    engine.toolOwnHashes.set(key, new Set([old1, old2, fresh]));
    engine.promoteCanonicalOwn(key, row, fresh);
    for (let i = 0; i < 12; i++) await tick();
    assert.equal(engine.canonicalOwnHash.get(key), fresh);
    assert.deepEqual(calls.sort(), [old1, old2].sort());
    assert.equal(book.own.has(fresh), true);
    assert.equal(book.own.has(old1), false);
    assert.equal(book.own.has(old2), false);
  });

  await check("canonical cleanup never cancels same-wallet manual or resynced hashes", async () => {
    const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
    const calls = [];
    const adapter = { chain: "ethereum", async cancelOwnOrderOffchain(hash) { calls.push(hash); return {}; } };
    const engine = new OfferItemEngineV2({ adapter, onLog() {} });
    engine.state = STATE.RUNNING; engine.epoch = 3;
    engine.builder = { address: "0x4444444444444444444444444444444444444444", wallet: new ethers.Wallet("0x" + "33".repeat(32)) };
    const key = "ethereum:0x1111111111111111111111111111111111111111:3";
    const row = { key, running: false, tokenId: "3", collectionSlug: "alpha" };
    engine.rows.set(key, row);
    const book = engine.book.add({ key, chain: "ethereum", contract: "0x1111111111111111111111111111111111111111", tokenId: "3", collectionSlug: "alpha" });
    const manual = "0x" + "11".repeat(32), oldTool = "0x" + "12".repeat(32), fresh = "0x" + "13".repeat(32);
    for (const h of [manual, oldTool, fresh]) book.own.set(h, { orderHash: h, price: .03, confirmed: true, endTime: 0, at: Date.now() });
    engine.toolOwnHashes.set(key, new Set([oldTool, fresh]));
    engine.promoteCanonicalOwn(key, row, fresh);
    for (let i = 0; i < 12; i++) await tick();
    assert.deepEqual(calls, [oldTool]);
    assert.equal(book.own.has(manual), true);
    assert.equal(book.own.has(fresh), true);
  });

  await check("cancel transport failure retains a retry lifecycle instead of orphaning WAITING", async () => {
    const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
    const adapter = { chain: "ethereum", async cancelOwnOrderOffchain() { throw new Error("socket reset"); } };
    const engine = new OfferItemEngineV2({ adapter, onLog() {} });
    engine.state = STATE.RUNNING; engine.epoch = 4;
    engine.builder = { wallet: new ethers.Wallet("0x" + "44".repeat(32)) };
    const key = "ethereum:0x1111111111111111111111111111111111111111:4";
    const row = { key, running: true, tokenId: "4", collectionSlug: "alpha" };
    engine.rows.set(key, row);
    const hash = "0x" + "14".repeat(32);
    engine.toolOwnHashes.set(key, new Set([hash]));
    assert.equal(engine.queueGaslessCancel(key, row, hash, "unconfirmed"), true);
    for (let i = 0; i < 8; i++) await tick();
    const state = engine.ownCancelJobs.get(key);
    assert.ok(state, "job remains represented");
    assert.equal(state.active, false);
    assert.ok(state.timer, "retry timer is live");
    clearTimeout(state.timer);
  });

  await check("queue rejects exact same-wallet hashes not proven tool-created", async () => {
    const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
    let calls = 0;
    const engine = new OfferItemEngineV2({ adapter: { async cancelOwnOrderOffchain() { calls++; } }, onLog() {} });
    engine.state = STATE.RUNNING;
    const key = "ethereum:0x1111111111111111111111111111111111111111:5";
    engine.rows.set(key, { key, running: true, tokenId: "5" });
    assert.equal(engine.queueGaslessCancel(key, engine.rows.get(key), "0x" + "15".repeat(32)), false);
    await tick();
    assert.equal(calls, 0);
  });

  if (!process.exitCode) process.stdout.write(`\n${passed}/${passed} checks passed\n`);
})();
