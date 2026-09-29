"use strict";

const assert = require("assert");
const { ethers } = require("ethers");
const { OfferItemEngineV2 } = require("./offer-item-v2/engine-v2");
const { OpenSeaStream } = require("./stream");
const { QuotaBroker } = require("./offer-item-v2/quota-broker");
const { tokenKey } = require("./offer-item-v2/event-normalizer");
const { LocalOrderBuilder } = require("./offer-item-v2/order-builder");

const TEST_PK = ethers.Wallet.createRandom().privateKey;
const SELF = new ethers.Wallet(TEST_PK).address.toLowerCase();
const RIVAL = "0x2222222222222222222222222222222222222222";
const CONTRACT = "0xa2a6063b910fc7a7a286196f6c9b62b2797fa0ae";
const WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
const CONDUIT = "0x0000007b02230091a7ed01230072f7006a004d60a8d4e71d599b8104250f0000";
const SLUG = "path-ab-selfcancel";
const KEY = tokenKey("ethereum", CONTRACT, "1");
let hseq = 0x9000;
let brokerSeq = 0;
let signedLog = null;
const hash = () => `0x${(++hseq).toString(16).padStart(64, "0")}`;
const nowSec = () => Math.floor(Date.now() / 1000);
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
async function until(fn, ms = 5000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (fn()) return true; await sleep(5); }
  return fn();
}
let passed = 0, failed = 0;
function check(name, ok, detail = "") {
  if (ok) { passed++; process.stdout.write(`PASS ${name}\n`); }
  else { failed++; process.stdout.write(`FAIL ${name}${detail ? `: ${detail}` : ""}\n`); }
}

const originalBuild = LocalOrderBuilder.prototype.build;
LocalOrderBuilder.prototype.build = async function (tokenKey, opts) {
  const signed = await originalBuild.call(this, tokenKey, opts);
  if (signedLog) signedLog.push({ price: Number(ethers.formatEther(opts.amountWei)), orderHash: signed.orderHash });
  return signed;
};

async function setup({ rivals = [{ price: 0.0101, kind: "item" }], row = {}, postDelayMs = 0 } = {}) {
  const readOrders = rivals.map(r => ({ orderHash: r.hash || hash(), price: r.price, pricePerItem: r.price,
    maker: RIVAL, kind: r.kind || "item", collectionSlug: SLUG, quantity: 1,
    endTime: nowSec() + 3600, currency: "WETH" }));
  const ctl = { fetchBest: 0, holdNextBest: false, releaseBest: null,
    weth: ethers.parseEther("1") };
  const snapshot = () => {
    const orders = readOrders.slice();
    const top = orders.reduce((a, b) => !a || b.price > a.price ? b : a, null);
    return top ? { price: top.price, kind: top.kind, orderHash: top.orderHash, maker: top.maker,
      endTime: top.endTime, orders } : { empty: true, price: 0, orders: [] };
  };
  const adapter = {
    chain: "ethereum", openseaChain: "ethereum",
    async chainConfig() { return { chainId: 1, wethAddress: WETH, conduitKey: CONDUIT,
      signedZone: "0x000056f7000000ece9003ca63978907a00ffd100" }; },
    async accountState() { return { at: Date.now(), counter: 0 }; },
    async collectionConfig() { return { slug: SLUG, fees: [], requiresSignedZone: false, tokenStandard: "erc721" }; },
    toWei: v => ethers.parseEther(String(v)), offerBody: s => s.payload,
    async resyncOwnOrders() { return { orders: [], complete: true }; },
    async walletBalance() { return { known: true, wethWei: ctl.weth, at: Date.now() }; },
    async fetchBest() {
      ctl.fetchBest++;
      if (ctl.holdNextBest) {
        ctl.holdNextBest = false;
        await new Promise(resolve => { ctl.releaseBest = resolve; });
      }
      return snapshot();
    }
  };
  const quota = new QuotaBroker({ getApiKey: () => `pathab-${process.pid}-${++brokerSeq}`, startRps: 500 });
  await quota.start();
  const engine = new OfferItemEngineV2({ adapter, quota, getApiKey: () => "key", getApiKeys: () => ["key"], onLog() {} });
  const posts = [], signed = [];
  signedLog = signed;
  engine.http.warmUp = async () => ({ ok: true, ms: 1 });
  engine.http.request = async ({ method, onStage }) => {
    onStage?.("http_started"); onStage?.("first_byte"); onStage?.("http_finished");
    if (method !== "POST") return { status: 200, headers: {}, body: {}, firstByteMs: 1, totalMs: 1 };
    if (postDelayMs) await sleep(postDelayMs);
    const sent = signed[signed.length - 1] || {};
    const result = { price: sent.price, hash: hash(), at: Date.now() };
    posts.push(result);
    return { status: 200, headers: {}, body: { order_hash: result.hash }, firstByteMs: 1, totalMs: 1 };
  };
  await engine.start({ privateKey: TEST_PK, rows: [{ url: "path-ab-row", contract: CONTRACT, tokenId: "1",
    collectionSlug: SLUG, traits: [], minPrice: 0.0001, maxPrice: 1, step: 0.0001, duration: 15, ...row }] });
  return { engine, adapter, ctl, posts, signed, readOrders, book: () => engine.book.get(KEY),
    row: () => engine.rows.get(KEY), stop: async () => { engine.shutdown(); await quota.stop(); } };
}

const cancel = orderHash => ({ event: "item_cancelled", collectionSlug: SLUG,
  nft: { chain: "ethereum", contract: CONTRACT, tokenId: "1" }, kind: "item", orderHash,
  maker: SELF, eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true });
const rivalCancel = orderHash => ({ event: "item_cancelled", collectionSlug: SLUG,
  nft: { chain: "ethereum", contract: CONTRACT, tokenId: "1" }, kind: "item", orderHash,
  maker: RIVAL, eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true });
const bid = price => ({ event: "item_received_bid", collectionSlug: SLUG,
  nft: { chain: "ethereum", contract: CONTRACT, tokenId: "1" }, kind: "item", orderHash: hash(), maker: RIVAL,
  pricePerItem: price, quantity: 1, currency: "WETH", endTime: nowSec() + 3600,
  eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true });

async function main() {
  // RED candidate: a collection Best is known locally, while the independent
  // fresh authority request stays pending. Path A must post before it resolves.
  {
    const w = await setup({ rivals: [{ price: 0.0254, kind: "collection" }], row: { minPrice: 0.0001, maxPrice: 1, step: 0.0001 } });
    await until(() => w.posts.length === 1);
    const ownHash = w.posts[0].hash;
    const reads = w.ctl.fetchBest;
    w.ctl.holdNextBest = true;
    w.engine.handleStreamEvent(cancel(ownHash));
    await until(() => typeof w.ctl.releaseBest === "function");
    await sleep(100);
    check("matched own cancel with known collection Best sends target while Path B is pending",
      w.posts.length === 2 && Math.abs(w.posts[1].price - 0.0255) < 1e-9,
      JSON.stringify({ posts: w.posts.map(p => p.price), reads: w.ctl.fetchBest - reads, pending: Boolean(w.ctl.releaseBest) }));
    check("matched own cancel schedules targeted Path B read independently",
      w.ctl.fetchBest > reads && Boolean(w.ctl.releaseBest), `reads=${reads}->${w.ctl.fetchBest}`);
    w.ctl.releaseBest?.();
    await sleep(50);
    await w.stop();
  }

  // RED candidate: exact maker/NFT identity is enough to wake even when the
  // cancelled hash has drifted out of the own-order map; it must still schedule B.
  {
    const w = await setup({ rivals: [{ price: 0.0254, kind: "item" }], row: { minPrice: 0.0001, maxPrice: 1, step: 0.0001 } });
    await until(() => w.posts.length === 1);
    const ownHash = w.posts[0].hash;
    w.book().own.delete(ownHash);
    w.book().generation++;
    const reads = w.ctl.fetchBest;
    w.ctl.holdNextBest = true;
    w.engine.handleStreamEvent(cancel(ownHash));
    await until(() => typeof w.ctl.releaseBest === "function" || w.posts.length > 1);
    check("hash-drifted self cancel wakes and immediately counters local Best",
      w.posts.length === 2 && Math.abs(w.posts[1].price - 0.0255) < 1e-9,
      JSON.stringify(w.posts.map(p => p.price)));
    check("hash-drifted self cancel also schedules targeted Path B verification",
      (w.ctl.fetchBest > reads && Boolean(w.ctl.releaseBest)) ||
        w.engine.pendingReads.get(KEY)?.reason === "self-cancel",
      `reads=${reads}->${w.ctl.fetchBest}, pending=${w.engine.pendingReads.get(KEY)?.reason || "none"}`);
    w.ctl.releaseBest?.();
    await sleep(50);
    await w.stop();
  }

  // RED candidate: with no known local external Best, only this row waits for B.
  // A fresh higher Stream bid must take over while B is still in flight; the
  // later stale REST snapshot may not overwrite it.
  {
    const w = await setup({ rivals: [{ price: 0.0101, kind: "item" }], row: { minPrice: 0.0001, maxPrice: 1, step: 0.0001 } });
    await until(() => w.posts.length === 1);
    const ownHash = w.posts[0].hash;
    w.book().item.clear(); w.book().collection.clear(); w.book().trait.clear(); w.book().generation++;
    w.readOrders.splice(0, w.readOrders.length, { orderHash: hash(), price: 0.0300, pricePerItem: 0.0300,
      maker: RIVAL, kind: "item", collectionSlug: SLUG, quantity: 1, endTime: nowSec() + 3600, currency: "WETH" });
    w.ctl.holdNextBest = true;
    w.engine.handleStreamEvent(cancel(ownHash));
    await until(() => typeof w.ctl.releaseBest === "function");
    await sleep(80);
    check("unknown local Best waits only on this NFT and does not blindly send Min", w.posts.length === 1,
      JSON.stringify(w.posts.map(p => p.price)));
    w.engine.handleStreamEvent(bid(0.0254));
    await until(() => w.posts.length >= 2);
    check("fresh Stream bid sends immediately while targeted B read remains pending",
      w.posts.length === 2 && Math.abs(w.posts[1].price - 0.0255) < 1e-9 && Boolean(w.ctl.releaseBest),
      JSON.stringify({ posts: w.posts.map(p => p.price), restPending: Boolean(w.ctl.releaseBest) }));
    w.ctl.releaseBest?.();
    await sleep(250);
    check("stale B snapshot cannot replace newer Stream Best", Math.abs(w.book().effectiveBest(Date.now()).price - 0.0254) < 1e-9,
      String(w.book().effectiveBest(Date.now()).price));
    check("stale B snapshot cannot trigger a second simultaneous or stale POST", w.posts.length === 2,
      JSON.stringify(w.posts.map(p => p.price)));
    await w.stop();
  }

  // Reconnect recovery is Path B. A fresh event must still counter while its
  // targeted reconnect read is deliberately held open.
  {
    const w = await setup({ rivals: [{ price: 0.0101, kind: "item" }], row: { minPrice: 0.0001, maxPrice: 1, step: 0.0001 } });
    await until(() => w.posts.length === 1);
    const reads = w.ctl.fetchBest;
    w.ctl.holdNextBest = true;
    w.engine.onStreamReconnect();
    check("socket reconnect does not start a duplicate global read before topic ACK",
      w.ctl.fetchBest === reads && !w.engine.awaitingFirstRead.has(KEY),
      `reads=${reads}->${w.ctl.fetchBest}, awaiting=${w.engine.awaitingFirstRead.has(KEY)}`);
    w.engine.firstJoinRecheck(SLUG, Date.now());
    await until(() => typeof w.ctl.releaseBest === "function");
    w.engine.handleStreamEvent(bid(0.0254));
    await until(() => w.posts.length === 2);
    check("fresh Stream bid counters while reconnect Path B read is pending",
      w.posts.length === 2 && Math.abs(w.posts[1].price - 0.0255) < 1e-9 &&
        w.ctl.fetchBest > reads && Boolean(w.ctl.releaseBest),
      JSON.stringify({ posts: w.posts.map(p => p.price), reads: w.ctl.fetchBest - reads, pending: Boolean(w.ctl.releaseBest) }));
    check("reconnect recovery leaves the WARM NFT send gate open",
      !w.engine.awaitingFirstRead.has(KEY) && !w.engine.downwardAuthority.has(KEY),
      JSON.stringify({ awaiting: w.engine.awaitingFirstRead.has(KEY), downward: w.engine.downwardAuthority.has(KEY) }));
    check("repeated full recovery signal coalesces while Path B is active",
      w.engine.scheduleRecoverySweep("stream-degraded") === 0,
      `queued=${w.engine.recoveryCandidates.size}, pending=${w.engine.pendingReads.size}`);
    w.ctl.releaseBest?.();
    await sleep(80);
    await w.stop();
  }

  // Removing an item top must use a still-known collection Best immediately;
  // Path B may verify hidden lower item offers without gating Path A.
  {
    const w = await setup({ rivals: [
      { price: 0.0254, kind: "item" }, { price: 0.0210, kind: "collection" }
    ], row: { minPrice: 0.0001, maxPrice: 1, step: 0.0001 } });
    await until(() => w.posts.length === 1);
    const removed = w.book().effectiveBest(Date.now()).orderHash;
    w.ctl.holdNextBest = true;
    w.engine.handleStreamEvent(rivalCancel(removed));
    await until(() => typeof w.ctl.releaseBest === "function");
    const remaining = w.book().effectiveBest(Date.now()).price;
    check("item removal preserves known lower collection Best without closing the NFT gate",
      Math.abs(remaining - 0.0210) < 1e-9 && !w.engine.awaitingFirstRead.has(KEY) &&
        !w.engine.downwardAuthority.has(KEY),
      JSON.stringify({ remaining, awaiting: w.engine.awaitingFirstRead.has(KEY), downward: w.engine.downwardAuthority.has(KEY) }));
    check("item removal schedules independent targeted Path B verification",
      Boolean(w.ctl.releaseBest) && w.engine.pendingReads.get(KEY)?.reason === "gap",
      `pending=${w.engine.pendingReads.get(KEY)?.reason || "none"}`);
    w.ctl.releaseBest?.();
    await sleep(50);
    await w.stop();
  }

  // If removal leaves no local external Best, only this row waits for the
  // targeted read. A fresh higher Stream bid releases the gate immediately.
  {
    const w = await setup({ rivals: [{ price: 0.0101, kind: "item" }], row: { minPrice: 0.0001, maxPrice: 1, step: 0.0001 } });
    await until(() => w.posts.length === 1);
    const removed = w.book().effectiveBest(Date.now()).orderHash;
    w.ctl.holdNextBest = true;
    w.engine.handleStreamEvent(rivalCancel(removed));
    await until(() => typeof w.ctl.releaseBest === "function");
    check("unknown post-removal Best gates only its own row during targeted Path B",
      w.engine.downwardAuthority.has(KEY) && w.engine.awaitingFirstRead.has(KEY) && w.posts.length === 1,
      JSON.stringify({ downward: w.engine.downwardAuthority.has(KEY), awaiting: w.engine.awaitingFirstRead.has(KEY), posts: w.posts.length }));
    w.engine.handleStreamEvent(bid(0.0200));
    await until(() => w.posts.length === 2);
    check("fresh higher bid releases post-removal authority gate without waiting for REST",
      w.posts.length === 2 && Math.abs(w.posts[1].price - 0.0201) < 1e-9 && Boolean(w.ctl.releaseBest),
      JSON.stringify({ posts: w.posts.map(p => p.price), restPending: Boolean(w.ctl.releaseBest) }));
    w.ctl.releaseBest?.();
    await sleep(50);
    await w.stop();
  }

  // End-to-end local latency sample: raw Phoenix frame → production decoder →
  // Offer Item engine → intent/quota → stubbed HTTP start (no network POST).
  {
    const w = await setup({ rivals: [{ price: 0.0101, kind: "item" }], row: { minPrice: 0.0001, maxPrice: 1, step: 0.0001 } });
    await until(() => w.posts.length === 1);
    const stream = new OpenSeaStream({ getApiKey: () => "", onEvent: event => w.engine.handleStreamEvent(event) });
    stream.closedByUser = false;
    stream.globalMode = false;
    stream.collections = new Set([SLUG]);
    stream.trackedSlugs = new Set([SLUG]);
    const startPosts = w.posts.length;
    const count = 20;
    for (let i = 0; i < count; i++) {
      const price = 0.0200 + i * 0.0002;
      const payload = { payload: {
        collection: { slug: SLUG },
        item: { nft_id: `ethereum/${CONTRACT}/1` },
        maker: { address: RIVAL }, order_hash: hash(),
        base_price: String(Math.round(price * 1e18)), quantity: 1,
        expiration_date: "2030-01-01T00:00:00Z"
      } };
      stream.handleMessage(JSON.stringify(["1", String(i + 1), `collection:${SLUG}`, "item_received_bid", payload]));
      await until(() => w.posts.length === startPosts + i + 1);
    }
    const metrics = w.engine.metrics.report();
    const spans = metrics.spans;
    const cells = ["frame→decode", "decode→map", "map→book", "book→decision", "decision→intent",
      "intent→quota", "quota wait", "quota grant→HTTP start", "event→HTTP"];
    const report = {};
    for (const group of Object.values(spans)) for (const [label, stats] of Object.entries(group))
      if (cells.includes(label)) report[label] = { p50: stats.p50, p95: stats.p95, p99: stats.p99, max: stats.max, n: stats.n };
    check("latency telemetry covers Stream frame through HTTP start", cells.every(label => report[label]?.n >= count),
      JSON.stringify(report));
    process.stdout.write(`LATENCY_SAMPLE ${JSON.stringify(report)}\n`);
    stream.stop();
    await w.stop();
  }

  process.stdout.write(`\n${passed}/${passed + failed} Path A/B self-cancel checks passed\n`);
  process.exitCode = failed ? 1 : 0;
}

main().catch(error => { process.stderr.write(`${error.stack || error}\n`); process.exitCode = 1; });
