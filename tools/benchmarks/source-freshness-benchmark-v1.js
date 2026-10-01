"use strict";
/**
 * Read-only, bounded benchmark: for a small, fixed set of real collections,
 * compare WHEN a given order_hash is first seen via:
 *   (a) Stream (dual-feed, A+B)
 *   (b) REST collection-wide offers listing (/api/v2/offers/collection/{slug}/all)
 * Bounded duration, bounded collection count, bounded poll interval -- not a
 * continuous full-NFT scan. No signing, no POST, no live-money.
 */
const path = require("path");
const https = require("https");
const ROOT = "C:\\Users\\kingl\\Desktop\\tool ch\u1ebf\\OpenSeaOfferBot\\.work\\stream-realtime";
const { DualStreamFeed } = require(path.join(ROOT, "stream-dual-feed"));

const KEY_A = process.env.TEST_KEY_A;
const KEY_B = process.env.TEST_KEY_B;
if (!KEY_A || !KEY_B) { console.error("missing TEST_KEY_A/TEST_KEY_B"); process.exit(1); }

const COLLECTIONS = ["goblintownwtf", "fwogs"];
const RUN_MS = 180000; // 3 minutes, bounded
const POLL_MS = 8000;  // bounded REST poll cadence, not continuous scanning

const log = [];
const push = line => { const s = `[${new Date().toISOString()}] ${line}`; log.push(s); console.log(s); };

// orderHash -> { firstSeenBy: "stream"|"rest", streamAt, restAt }
const sightings = new Map();

function noteStream(orderHash, at) {
  if (!orderHash) return;
  let rec = sightings.get(orderHash);
  if (!rec) { rec = {}; sightings.set(orderHash, rec); }
  if (!rec.streamAt) rec.streamAt = at;
}
function noteRest(orderHash, at) {
  if (!orderHash) return;
  let rec = sightings.get(orderHash);
  if (!rec) { rec = {}; sightings.set(orderHash, rec); }
  if (!rec.restAt) rec.restAt = at;
}

function restGet(apiPath, apiKey) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: "api.opensea.io", path: apiPath, method: "GET",
      headers: { "X-API-KEY": apiKey, Accept: "application/json" }, timeout: 8000
    }, res => {
      let body = "";
      res.on("data", c => body += c);
      res.on("end", () => {
        if (res.statusCode >= 200 && res.statusCode < 300) {
          try { resolve(JSON.parse(body)); } catch (e) { reject(e); }
        } else reject(new Error(`HTTP ${res.statusCode}: ${body.slice(0, 200)}`));
      });
    });
    req.on("error", reject);
    req.on("timeout", () => req.destroy(new Error("timeout")));
    req.end();
  });
}

const apiKeys = {
  pool: [{ key: KEY_A, fp: "fpA", slot: 1 }, { key: KEY_B, fp: "fpB", slot: 2 }],
  primary() { return this.pool[0]; }, secondary() { return this.pool[1]; },
  find: key => apiKeys.pool.find(e => e.key === key) || null,
  fingerprint() { return "bench"; }
};

const feed = new DualStreamFeed({
  apiKeys, logger: { stream: () => {}, error: m => push(`STREAM ERROR ${m}`) },
  onEvent: decoded => noteStream(decoded.orderHash, Date.now())
});

push(`starting benchmark: collections=${COLLECTIONS.join(",")} durationMs=${RUN_MS} pollMs=${POLL_MS}`);
feed.start(COLLECTIONS);

let restCalls = 0, restErrors = 0;
const pollTimer = setInterval(async () => {
  for (const slug of COLLECTIONS) {
    try {
      const data = await restGet(`/api/v2/offers/collection/${encodeURIComponent(slug)}/all?limit=50`, KEY_A);
      restCalls++;
      const now = Date.now();
      for (const o of data.offers || []) {
        const hash = o.order_hash || (o.protocol_data && o.protocol_data.orderHash) || "";
        if (hash) noteRest(hash, now);
      }
    } catch (error) {
      restErrors++;
      push(`REST poll error ${slug}: ${error.message}`);
    }
  }
}, POLL_MS);

setTimeout(() => {
  clearInterval(pollTimer);
  feed.stop();

  const rows = [...sightings.entries()].map(([hash, rec]) => ({ hash, ...rec }));
  const both = rows.filter(r => r.streamAt && r.restAt);
  const streamOnly = rows.filter(r => r.streamAt && !r.restAt);
  const restOnly = rows.filter(r => r.restAt && !r.streamAt);

  push(`total distinct order hashes observed: ${rows.length}`);
  push(`seen by BOTH: ${both.length} | stream-only: ${streamOnly.length} | REST-only: ${restOnly.length}`);
  push(`REST polls made: ${restCalls}, errors: ${restErrors}`);

  if (both.length) {
    const deltas = both.map(r => r.streamAt - r.restAt); // negative = stream first
    const streamFirstCount = deltas.filter(d => d < 0).length;
    const restFirstCount = deltas.filter(d => d > 0).length;
    const avgDeltaMs = deltas.reduce((a, b) => a + b, 0) / deltas.length;
    push(`of ${both.length} seen by both: stream-first=${streamFirstCount} rest-first=${restFirstCount} avgDelta(stream-rest)Ms=${avgDeltaMs.toFixed(0)}`);
    push(`(negative avgDelta means Stream is typically faster than the bounded REST poll, as expected)`);
  }

  if (restOnly.length) {
    push(`REST-only hashes (Stream never delivered these during the run): ${restOnly.map(r => r.hash.slice(0, 12)).join(", ")}`);
  }

  const fs = require("fs");
  fs.writeFileSync(path.join(ROOT, "release-artifacts", "source-freshness-benchmark-v1.25.41.txt"), log.join("\n") + "\n");
  push("evidence saved");
  process.exit(0);
}, RUN_MS);
