"use strict";

// Replays the two production-reported 60s frame counts against the current
// handler. This isolates JS ingress work; it cannot measure OpenSea's network
// filtering or explain remote code=1006 socket closes.
const { OpenSeaStream } = require("./stream");

const GLOBAL_RX = 1_535_583;
const TRACKED_RX = 41_789;
const TRACKED = 58;
const PAYLOAD = slug => JSON.stringify(["1", "1", `collection:${slug}`, "item_received_bid", {
  payload: {
    collection: { slug },
    item: { nft_id: "ethereum/0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/1" },
    maker: { address: "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" },
    order_hash: "0x" + "1".repeat(64),
    base_price: "25400000000000000",
    quantity: 1,
    expiration_date: "2030-01-01T00:00:00Z"
  }
}]);
const trackedFrame = PAYLOAD("tracked-0");
const irrelevantFrame = PAYLOAD("untracked-collection");

function percentile(values, p) {
  const sorted = values.slice().sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))] || 0;
}

function run(mode, count, frameFor) {
  const stream = new OpenSeaStream({ getApiKey: () => "", onEvent() {} });
  stream.closedByUser = false;
  stream.globalMode = mode === "global";
  stream.collections = new Set(Array.from({ length: TRACKED }, (_, i) => `tracked-${i}`));
  stream.trackedSlugs = new Set(stream.collections);
  const samples = [];
  const bytesEach = Buffer.byteLength(frameFor(0));
  const start = process.hrtime.bigint();
  let nextSample = 0;
  for (let i = 0; i < count; i++) {
    const before = process.hrtime.bigint();
    stream.handleMessage(frameFor(i));
    if (i === nextSample) {
      samples.push(Number(process.hrtime.bigint() - before) / 1e6);
      nextSample += Math.max(1, Math.floor(count / 2000));
    }
  }
  const elapsedMs = Number(process.hrtime.bigint() - start) / 1e6;
  const result = {
    mode,
    frames: stream.stats.framesReceived,
    handled: stream.stats.handled,
    dropped: stream.globalStats.dropped,
    droppedType: stream.globalStats.droppedType,
    bytesPerMinute: bytesEach * count,
    elapsedMs: Math.round(elapsedMs * 100) / 100,
    handlerSampleMs: { p50: percentile(samples, 0.50), p95: percentile(samples, 0.95), p99: percentile(samples, 0.99), max: Math.max(0, ...samples) },
    replayRateFramesPerSecond: Math.round(count / (elapsedMs / 1000))
  };
  stream.stop();
  return result;
}

const global = run("global", GLOBAL_RX, i => i < TRACKED_RX ? trackedFrame : irrelevantFrame);
const perCollection = run("per-collection", TRACKED_RX, () => trackedFrame);
const loopWorkReduction = Math.round((1 - perCollection.frames / global.frames) * 10000) / 100;
const ingressRatio = Math.round((global.bytesPerMinute / perCollection.bytesPerMinute) * 100) / 100;
process.stdout.write(JSON.stringify({
  note: "synthetic replay of user-provided production counts; not live server A/B",
  trackedCollections: TRACKED,
  global,
  perCollection,
  estimatedJsFrameWorkReductionPercent: loopWorkReduction,
  modeledIngressByteRatioGlobalToTracked: ingressRatio
}, null, 2) + "\n");
