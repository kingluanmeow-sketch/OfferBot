"use strict";

/**
 * Soak/benchmark for the op.soft own-REMOVE fix (memory-book.js apply()).
 * In-process, synthetic network layer -- no real HTTP to OpenSea, no paid
 * traffic. Same construction pattern as offer-item-v2-ab-selfcancel-test.js
 * (which this fix must not regress).
 *
 * Measures: Stream->Decision sync cost, self-cancel (item_cancelled, hard)
 * reaction latency, duplicate-POST count (same NFT + same price within a
 * short window), and throughput under a realistic mix of UPSERT /
 * order_invalidate(soft) / item_cancelled(hard) REMOVE traffic.
 */

const { performance } = require("perf_hooks");
const { ethers } = require("ethers");
const { OfferItemEngineV2 } = require("./offer-item-v2/engine-v2");
const { QuotaBroker } = require("./offer-item-v2/quota-broker");
const { tokenKey } = require("./offer-item-v2/event-normalizer");

const TEST_PK = ethers.Wallet.createRandom().privateKey;
const SELF = new ethers.Wallet(TEST_PK).address.toLowerCase();
const RIVAL = "0x2222222222222222222222222222222222222222";
const WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
const CONDUIT = "0x0000007b02230091a7ed01230072f7006a004d60a8d4e71d599b8104250f0000";
const N_ROWS = 40;
const DURATION_MS = 45000;

let hseq = 0x1000;
const hash = () => `0x${(++hseq).toString(16).padStart(64, "0")}`;
const nowSec = () => Math.floor(Date.now() / 1000);
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function main() {
  const rows = [];
  for (let i = 0; i < N_ROWS; i++) {
    const contract = "0x" + (1000 + i).toString(16).padStart(40, "0");
    rows.push({ url: `row-${i}`, contract, tokenId: String(9000 + i), collectionSlug: `soak-${i}`,
      traits: [], minPrice: 0.0001, maxPrice: 1, step: 0.0001, duration: 15 });
  }
  const keys = rows.map(r => tokenKey("ethereum", r.contract, r.tokenId));
  const byKey = new Map(rows.map((r, i) => [keys[i], r]));
  const bestState = new Map();

  const adapter = {
    chain: "ethereum", openseaChain: "ethereum",
    async chainConfig() { return { chainId: 1, wethAddress: WETH, conduitKey: CONDUIT, signedZone: "0x000056f7000000ece9003ca63978907a00ffd100" }; },
    async accountState() { return { at: Date.now(), counter: 0 }; },
    async collectionConfig(slug) { return { slug, fees: [], requiresSignedZone: false, tokenStandard: "erc721" }; },
    toWei: v => ethers.parseEther(String(v)), offerBody: s => s.payload,
    async resyncOwnOrders() { return { orders: [], complete: true }; },
    async walletBalance() { return { known: true, wethWei: ethers.parseEther("100"), at: Date.now() }; },
    async fetchBest(row) {
      const key = tokenKey("ethereum", row.contract, row.tokenId);
      const price = bestState.get(key) || 0;
      return price > 0 ? { price, kind: "item", orderHash: "0x" + "b".repeat(64), maker: RIVAL, endTime: nowSec() + 3600, orders: [] } : { empty: true, price: 0, orders: [] };
    }
  };

  const quota = new QuotaBroker({ getApiKey: () => `soak-${process.pid}`, startRps: 500 });
  await quota.start();
  const engine = new OfferItemEngineV2({ adapter, quota, getApiKey: () => "key", getApiKeys: () => ["key"], onLog() {} });

  let signedLast = null;
  const origBuild = require("./offer-item-v2/order-builder").LocalOrderBuilder.prototype.build;
  require("./offer-item-v2/order-builder").LocalOrderBuilder.prototype.build = async function (tk, opts) {
    const signed = await origBuild.call(this, tk, opts);
    signedLast = signed;
    return signed;
  };

  engine.http.warmUp = async () => ({ ok: true, ms: 1 });
  engine.http.request = async ({ method, onStage }) => {
    onStage?.("http_started"); onStage?.("first_byte"); onStage?.("http_finished");
    if (method !== "POST") return { status: 200, headers: {}, body: {}, firstByteMs: 1, totalMs: 1 };
    return { status: 200, headers: {}, body: { order_hash: hash() }, firstByteMs: 1, totalMs: 1 };
  };

  const sendLog = [];
  const origLog = engine.log.bind(engine);
  engine.log = function (line) { if (/\[SEND\]/.test(line)) sendLog.push({ t: Date.now(), line }); return origLog(line); };

  await engine.start({ privateKey: TEST_PK, rows });
  await sleep(500);

  const streamToDecisionSamples = [];
  const selfCancelLatencySamples = [];
  let selfCancelCount = 0, streamEvents = 0;

  function emit(ev) {
    const t0 = performance.now();
    engine.handleStreamEvent(ev);
    streamToDecisionSamples.push(performance.now() - t0);
    streamEvents++;
  }

  const start = Date.now();
  while (Date.now() < start + DURATION_MS) {
    const i = Math.floor(Math.random() * N_ROWS);
    const key = keys[i], row = byKey.get(key);
    const roll = Math.random();
    const book = engine.book.get(key);
    const lastOwn = book ? [...book.own.values()].slice(-1)[0] : null;

    if (roll < 0.55) {
      const price = (bestState.get(key) || 0) + 0.0001;
      bestState.set(key, price);
      emit({ event: "item_received_bid", collectionSlug: row.collectionSlug,
        nft: { chain: "ethereum", contract: row.contract, tokenId: row.tokenId }, kind: "item",
        orderHash: hash(), maker: RIVAL, pricePerItem: price, quantity: 1, currency: "WETH",
        endTime: nowSec() + 3600, eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true });
    } else if (roll < 0.80 && lastOwn) {
      emit({ event: "order_invalidate", collectionSlug: row.collectionSlug,
        nft: { chain: "ethereum", contract: row.contract, tokenId: row.tokenId }, kind: "item",
        orderHash: lastOwn.orderHash, maker: null,
        eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true });
    } else if (lastOwn) {
      selfCancelCount++;
      const t0 = performance.now();
      emit({ event: "item_cancelled", collectionSlug: row.collectionSlug,
        nft: { chain: "ethereum", contract: row.contract, tokenId: row.tokenId }, kind: "item",
        orderHash: lastOwn.orderHash, maker: SELF,
        eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true });
      const deadline = performance.now() + 2000;
      while (performance.now() < deadline) {
        const it = engine.intents.get(key);
        if (it && (it.state === "READY" || it.state === "GRANTING" || it.state === "SENDING" || it.state === "BUILDING")) {
          selfCancelLatencySamples.push(performance.now() - t0); break;
        }
        await sleep(1);
      }
    }
    await sleep(2);
  }

  await sleep(1000);
  engine.shutdown();
  await quota.stop();

  function pct(arr, p) { if (!arr.length) return null; const s = [...arr].sort((a, b) => a - b); return s[Math.min(s.length - 1, Math.floor(p / 100 * s.length))]; }
  function stats(arr) { if (!arr.length) return { n: 0 }; return { n: arr.length, p50: pct(arr, 50), p95: pct(arr, 95), max: Math.max(...arr) }; }

  const successLines = sendLog.filter(l => /SUBMIT SUCCESS/.test(l.line));
  const queuedLines = sendLog.filter(l => /SUBMIT QUEUED/.test(l.line));

  const byNft = new Map();
  for (const l of successLines) {
    const m = l.line.match(/#(\S+)\s+([\d.]+)\s+WETH/);
    if (!m) continue;
    const arr = byNft.get(m[1]) || [];
    arr.push({ t: l.t, price: m[2] });
    byNft.set(m[1], arr);
  }
  let dupCount = 0;
  for (const [nft, arr] of byNft.entries()) {
    arr.sort((a, b) => a.t - b.t);
    for (let i = 1; i < arr.length; i++) {
      if (arr[i].price === arr[i - 1].price && arr[i].t - arr[i - 1].t < 3000) {
        dupCount++;
        console.log("  DUP DETAIL nft=" + nft + " price=" + arr[i].price + " gapMs=" + (arr[i].t - arr[i - 1].t));
      }
    }
  }

  console.log("=== own-soft-remove-soak result ===");
  console.log("duration:", DURATION_MS, "ms, rows:", N_ROWS, "streamEvents:", streamEvents);
  console.log("QUEUED:", queuedLines.length, "SUCCESS:", successLines.length, "selfCancelInjected:", selfCancelCount);
  console.log("Stream->Decision (handleStreamEvent sync cost) ms:", JSON.stringify(stats(streamToDecisionSamples)));
  console.log("self-cancel reaction latency ms:", JSON.stringify(stats(selfCancelLatencySamples)), "captured", selfCancelLatencySamples.length, "/", selfCancelCount);
  console.log("DUPLICATE (same NFT, same price, <3s apart) count:", dupCount);
  console.log("throughput SUCCESS/s:", (successLines.length / (DURATION_MS / 1000)).toFixed(2));

  const result = { duplicateCount: dupCount, selfCancelLatencyStats: stats(selfCancelLatencySamples),
    selfCancelCapturedRatio: selfCancelCount ? selfCancelLatencySamples.length / selfCancelCount : 1,
    successCount: successLines.length, queuedCount: queuedLines.length, streamEvents,
    streamToDecisionStats: stats(streamToDecisionSamples) };
  require("fs").writeFileSync(require("path").join(__dirname, "own-soft-remove-soak-result.json"), JSON.stringify(result, null, 2));

  if (dupCount > 0) { console.error("SOAK FAIL: duplicate(s) detected"); process.exitCode = 1; }
}

main().catch(e => { console.error("SOAK CRASHED:", e.stack || e); process.exitCode = 1; });
