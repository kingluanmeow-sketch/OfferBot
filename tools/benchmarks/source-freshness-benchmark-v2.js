"use strict";
/**
 * Corrected read-only freshness benchmark (v2), addressing audit findings on v1:
 *  - v1 only read ONE page (limit=50) of the collection-wide offers endpoint,
 *    massively undercounting what REST could see -> "stream-only" counts were
 *    an artifact of incomplete REST coverage, not genuine Stream exclusivity.
 *  - v1 compared raw presence, not filtered by order creation time, so most
 *    "REST-only" hits were pre-existing orders from before the Stream
 *    subscription ACKed (expected -- Stream never replays history -- not a
 *    freshness signal at all).
 *  - v1 did not match on (orderHash, unit price) together.
 *  - v1 only benchmarked Stream vs one collection-wide REST endpoint.
 *
 * v2 fixes all four: full pagination (follows `next` to completion, bounded
 * at a generous page cap, same as production's own NFT_OFFER_MAX_PAGES
 * pattern), a BASELINE snapshot taken before comparison starts (anything in
 * the baseline is excluded from "first-seen" comparisons -- only orders that
 * appear AFTER the baseline, i.e. genuinely new during the run, count),
 * matches on (orderHash, unitPrice), and benchmarks four sources:
 *   (a) Stream A+B (dual feed)
 *   (b) REST /offers/collection/{slug}/nfts/{id}/best  (OpenSea's own resolution)
 *   (c) REST /offers/collection/{slug}/nfts/{id}        (NFT offers list)
 *   (d) REST /api/v2/orders/{chain}/seaport/offers?asset_contract_address=&token_ids=
 *   (e) REST /offers/collection/{slug}/all               (collection-wide, paginated)
 *
 * Tracks: yat #1875 (the actual reproduction case), plus a handful of NFTs
 * auto-discovered from live Stream traffic on goblintownwtf/fwogs during a
 * short warmup, so the comparison is generic, not Yat-specific.
 *
 * SECRET HANDLING: API keys are read from a local file this script deletes
 * immediately after reading (never passed via argv or a literal env
 * assignment in a shell command, so they never appear in process listings).
 * Raw key values are never logged -- only their sha256-derived fingerprint,
 * same scheme as rate-limiter.js's keyFingerprint() / the live dual-feed
 * check from the prior turn.
 *
 * No signing, no POST, no live-money. Bounded duration, bounded NFT count,
 * bounded page caps -- not a continuous full-collection scan.
 */
const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const https = require("https");

// Repo root, not a machine-specific absolute path (this script now lives at
// tools/benchmarks/, committed for reproducibility -- previously only the
// generated artifact was tracked, not this source).
const ROOT = path.join(__dirname, "..", "..");
const { DualStreamFeed } = require(path.join(ROOT, "stream-dual-feed"));

const KEYS_FILE = path.join(__dirname, ".bench-keys.json");
if (!fs.existsSync(KEYS_FILE)) { console.error("missing key file (expected one-time use, already consumed?)"); process.exit(1); }
// finally, not a bare unlink after parse: malformed JSON must not leave the
// key file sitting on disk (audit request -- one-shot must mean one-shot
// even on error/interrupt, not just the happy path).
let KEY_A, KEY_B;
try {
  ({ a: KEY_A, b: KEY_B } = JSON.parse(fs.readFileSync(KEYS_FILE, "utf8")));
} finally {
  try { fs.unlinkSync(KEYS_FILE); } catch { /* already gone */ }
}

const fp = key => crypto.createHash("sha256").update(String(key)).digest("hex").slice(0, 8);
const FP_A = fp(KEY_A), FP_B = fp(KEY_B);

const COLLECTIONS = ["goblintownwtf", "fwogs", "yat"];
const TARGET_NFT = { chain: "ethereum", contract: "0x7d256d82b32d8003d1ca1a1526ed211e6e0da9e2", tokenId: "1875", slug: "yat" };
const WARMUP_MS = 30000;      // discover a few live NFTs from real Stream traffic
const BASELINE_GRACE_MS = 5000; // let the baseline snapshot settle before comparisons start
const RUN_MS = 240000;        // 4 minutes total bounded run
const COLLECTION_POLL_MS = 15000;
const NFT_POLL_MS = 10000;
const MAX_COLLECTION_PAGES = 40; // bounded full-pagination cap, not unlimited

const log = [];
const push = line => { const s = `[${new Date().toISOString()}] ${line}`; log.push(s); console.log(s); };

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
        } else reject(new Error(`HTTP ${res.statusCode}: ${body.slice(0, 150)}`));
      });
    });
    req.on("error", reject);
    req.on("timeout", () => req.destroy(new Error("timeout")));
    req.end();
  });
}

// offer.price.value is the order TOTAL (see opensea.js fetchBestOffer's own
// `totalOrderValue` naming) -- divide by quantity for the unit price.
// All three tracked collections (goblintownwtf, fwogs, yat) are ERC-721
// PFP collections, so quantity is always 1 for item offers; this benchmark
// does not attempt ERC-1155 fractional-quantity collection offers.
function unitPrice(offer) {
  try {
    const raw = BigInt(String(offer.price.value));
    const decimals = Number(offer.price.decimals);
    return Number(raw) / (10 ** decimals);
  } catch { return null; }
}

async function drainCollectionOffers(slug, apiKey, maxPages = MAX_COLLECTION_PAGES) {
  const out = [];
  let next = null, pages = 0;
  for (; pages < maxPages; pages++) {
    const qs = next ? `?limit=50&next=${encodeURIComponent(next)}` : "?limit=50";
    const data = await restGet(`/api/v2/offers/collection/${encodeURIComponent(slug)}/all${qs}`, apiKey);
    for (const o of data.offers || []) out.push(o);
    next = data.next || null;
    if (!next) break;
  }
  return { offers: out, pages: pages + 1, complete: !next };
}

// orderHash|unitPrice -> { createdAt, firstSeen: {source: at} }
const sightings = new Map();
const baseline = new Set(); // orderHash|unitPrice keys known BEFORE comparison starts
const baselineComplete = new Map(); // slug -> boolean, whether ITS baseline fully drained
let comparisonStartAt = 0;
let comparisonActive = false; // gates record() -- false during warmup AND baseline phases

// How many events got discarded specifically because they arrived before
// comparisonActive flipped true (warmup/baseline phases) -- audit-visible
// instead of silently vanishing or (the v2 bug) silently counted as "new"
// with a negative ms offset.
const discardedStats = { preexisting: 0, beforeComparisonActive: 0, replayedOld: 0 };

function keyOf(orderHash, price) { return `${String(orderHash).toLowerCase()}|${Number(price).toFixed(8)}`; }

function record(orderHash, price, source, at, createdAt, slug = "") {
  if (!orderHash || !(price > 0)) return;
  if (!comparisonActive) { discardedStats.beforeComparisonActive++; return; }
  const k = keyOf(orderHash, price);
  if (baseline.has(k)) { discardedStats.preexisting++; return; } // pre-existing, not part of the freshness comparison
  // Exclude anything whose own reported creation time predates comparisonStartAt
  // too (belt + suspenders beyond the baseline snapshot, per audit request).
  if (createdAt && createdAt * 1000 < comparisonStartAt) { discardedStats.replayedOld++; return; }
  let rec = sightings.get(k);
  if (!rec) { rec = { orderHash, price, firstSeen: {}, slug, baselineComplete: slug ? baselineComplete.get(slug) !== false : true }; sightings.set(k, rec); }
  if (!rec.firstSeen[source]) rec.firstSeen[source] = at;
}

// ---- discover live NFTs generically from real Stream traffic -------------
const discovered = new Map(); // "chain:contract:tokenId" -> {chain,contract,tokenId,slug}
discovered.set(`${TARGET_NFT.chain}:${TARGET_NFT.contract.toLowerCase()}:${TARGET_NFT.tokenId}`, TARGET_NFT);

const ordersApiStatus = { attempts: 0, lastStatus: "" };

const apiKeys = {
  pool: [{ key: KEY_A, fp: FP_A, slot: 1 }, { key: KEY_B, fp: FP_B, slot: 2 }],
  primary() { return this.pool[0]; }, secondary() { return this.pool[1]; },
  find: key => apiKeys.pool.find(e => e.key === key) || null,
  fingerprint() { return `${FP_A} ${FP_B}`; }
};

const feed = new DualStreamFeed({
  apiKeys, logger: { stream: () => {}, error: m => push(`STREAM ERROR ${m}`) },
  onEvent: decoded => {
    const at = Date.now();
    const nft = decoded.nft;
    if (nft && nft.contract && nft.tokenId) {
      const k = `${nft.chain || "ethereum"}:${String(nft.contract).toLowerCase()}:${nft.tokenId}`;
      if (!discovered.has(k) && discovered.size < 6) {
        discovered.set(k, { chain: nft.chain || "ethereum", contract: nft.contract, tokenId: nft.tokenId, slug: decoded.collectionSlug });
      }
    }
    // pricePerItem is the unit price (decoded.price is the order TOTAL,
    // i.e. pricePerItem * quantity -- must compare unit price on both sides).
    if (decoded.orderHash && decoded.pricePerItem > 0) {
      record(decoded.orderHash, decoded.pricePerItem, "stream", at, null, decoded.collectionSlug || "");
    }
  }
});

push(`fpA=${FP_A} fpB=${FP_B} (fingerprints only, raw keys never logged)`);
push(`phase 1: warmup ${WARMUP_MS}ms -- discover live NFTs + start Stream`);
feed.start(COLLECTIONS);

(async () => {
  await new Promise(r => setTimeout(r, WARMUP_MS));
  push(`discovered ${discovered.size} NFTs for targeted benchmarking: ${[...discovered.values()].map(n => `${n.slug || "?"}/${n.tokenId}`).join(", ")}`);

  // ---- phase 2: baseline snapshot (collection-wide, paginated) -----------
  push("phase 2: taking baseline snapshot (full pagination) before comparisons start");
  let baselineCount = 0;
  for (const slug of COLLECTIONS) {
    try {
      const { offers, pages, complete } = await drainCollectionOffers(slug, KEY_A);
      for (const o of offers) {
        const hash = o.order_hash || o?.protocol_data?.orderHash || "";
        const price = unitPrice(o);
        if (hash && price > 0) { baseline.add(keyOf(hash, price)); baselineCount++; }
      }
      baselineComplete.set(slug, complete);
      push(`baseline ${slug}: ${offers.length} offers across ${pages} pages (complete=${complete})` +
        (complete ? "" : " -- INCOMPLETE: hit the page cap, this collection's baseline is a lower bound only, excluded from conclusions below"));
    } catch (error) {
      baselineComplete.set(slug, false);
      push(`baseline error ${slug}: ${error.message} -- treated as incomplete, excluded from conclusions`);
    }
  }
  push(`baseline total: ${baselineCount} pre-existing (orderHash,unitPrice) pairs excluded from comparison`);

  await new Promise(r => setTimeout(r, BASELINE_GRACE_MS));
  comparisonStartAt = Date.now();
  comparisonActive = true; // record() only starts counting from exactly here --
  // every Stream/REST event from warmup (phase 1) or the baseline snapshot
  // itself (phase 2) is now structurally impossible to miscount as "new"
  // (previously: onEvent ran during warmup with comparisonStartAt still 0,
  // so the guard was skipped entirely -- the source of the "stream@-59941ms"
  // negative-offset bug).
  push(`phase 3: comparison window starts now (${new Date(comparisonStartAt).toISOString()}), duration=${RUN_MS}ms`);

  // ---- phase 3: parallel bounded polling of all REST sources -------------
  const collectionPoll = setInterval(async () => {
    for (const slug of COLLECTIONS) {
      try {
        const { offers } = await drainCollectionOffers(slug, KEY_A, 10); // bounded per-tick cap
        const now = Date.now();
        for (const o of offers) {
          const hash = o.order_hash || o?.protocol_data?.orderHash || "";
          const price = unitPrice(o);
          const createdAt = o.created_date ? Date.parse(o.created_date) / 1000 : null;
          if (hash && price > 0) record(hash, price, "rest_collection_all", now, createdAt, slug);
        }
      } catch (error) { push(`collection poll error ${slug}: ${error.message}`); }
    }
  }, COLLECTION_POLL_MS);

  const nftPoll = setInterval(async () => {
    for (const nft of discovered.values()) {
      if (!nft.slug) continue;
      const now0 = Date.now();
      // (b) /best
      try {
        const best = await restGet(`/api/v2/offers/collection/${encodeURIComponent(nft.slug)}/nfts/${encodeURIComponent(nft.tokenId)}/best`, KEY_B);
        const hash = best.order_hash || best?.protocol_data?.orderHash || "";
        const price = unitPrice(best);
        if (hash && price > 0) record(hash, price, "rest_best", Date.now(), best.created_date ? Date.parse(best.created_date) / 1000 : null, nft.slug);
      } catch { /* no best offer or transient -- not a benchmark failure */ }
      // (c) NFT offers list
      try {
        const list = await restGet(`/api/v2/offers/collection/${encodeURIComponent(nft.slug)}/nfts/${encodeURIComponent(nft.tokenId)}?limit=50`, KEY_A);
        const now = Date.now();
        for (const o of list.offers || []) {
          const hash = o.order_hash || o?.protocol_data?.orderHash || "";
          const price = unitPrice(o);
          if (hash && price > 0) record(hash, price, "rest_nft_list", now, o.created_date ? Date.parse(o.created_date) / 1000 : null, nft.slug);
        }
      } catch (error) { push(`nft-list poll error ${nft.slug}/${nft.tokenId}: ${error.message}`); }
      // (d) Orders API -- 2026-10-01 audit: the previously-guessed query shape
      // (asset_contract_address/token_ids as GET query params on
      // /orders/{chain}/seaport/offers) returns HTTP 405 Method Not Allowed
      // consistently. Research against the official OpenAPI spec could not
      // confirm the correct shape (docs.opensea.io's reference pages are
      // JS-rendered and not fetchable; the spec itself did not resolve a
      // GET-single-order-by-hash sibling). Rather than keep guessing and
      // silently reporting a misleading 0/0 "no results", this endpoint is
      // explicitly marked NOT VALIDATED and excluded from the source list
      // below -- see ordersApiStatus.
      ordersApiStatus.attempts++;
      try {
        const orders = await restGet(`/api/v2/orders/${nft.chain}/seaport/offers?asset_contract_address=${nft.contract}&token_ids=${nft.tokenId}&limit=50`, KEY_B);
        ordersApiStatus.lastStatus = "200 (shape not independently verified -- see note above)";
        const now = Date.now();
        for (const o of orders.orders || []) {
          const hash = o.order_hash || "";
          const price = unitPrice({ price: { value: o.current_price, decimals: 18 } });
          if (hash && price > 0) record(hash, price, "rest_orders_api", now, o.created_date ? Date.parse(o.created_date) / 1000 : null, nft.slug);
        }
      } catch (error) { ordersApiStatus.lastStatus = error.message; }
    }
  }, NFT_POLL_MS);

  await new Promise(r => setTimeout(r, RUN_MS));
  clearInterval(collectionPoll);
  clearInterval(nftPoll);
  feed.stop();

  // ---- report --------------------------------------------------------
  const allRows = [...sightings.values()];
  // Per audit: a collection whose baseline hit the page cap (complete=false)
  // cannot be trusted to have excluded every pre-existing order -- its rows
  // are reported separately, never folded into the main conclusion.
  const rows = allRows.filter(r => r.baselineComplete);
  const unverifiedRows = allRows.filter(r => !r.baselineComplete);

  push(`\n=== DISCARD AUDIT (why a raw event did NOT become a "genuinely new" row) ===`);
  push(`discarded: beforeComparisonActive(warmup/baseline phase)=${discardedStats.beforeComparisonActive} ` +
    `preexisting(in baseline snapshot)=${discardedStats.preexisting} replayedOld(own createdAt predates window)=${discardedStats.replayedOld}`);

  push(`\n=== RESULTS -- genuinely-new, post-comparisonStart, baseline-COMPLETE collections only ===`);
  push(`total genuinely-new pairs: ${rows.length}` +
    (unverifiedRows.length ? ` (+ ${unverifiedRows.length} from baseline-incomplete collections, reported separately below, NOT included in this count or the conclusions)` : ""));

  const sources = ["stream", "rest_best", "rest_nft_list", "rest_collection_all"];
  for (const src of sources) {
    const seenBySrc = rows.filter(r => r.firstSeen[src]).length;
    push(`seen by ${src}: ${seenBySrc}`);
  }
  push(`rest_orders_api: EXCLUDED from conclusions -- endpoint shape not validated ` +
    `(${ordersApiStatus.attempts} attempts, last status: ${ordersApiStatus.lastStatus || "none"})`);

  for (const row of rows) {
    const entries = Object.entries(row.firstSeen).sort((a, b) => a[1] - b[1]);
    const winner = entries[0];
    const others = entries.slice(1).map(([s, t]) => `${s}+${t - winner[1]}ms`).join(", ");
    push(`[${row.slug}] order ${row.orderHash.slice(0, 14)} price=${row.price} first=${winner[0]}@${winner[1] - comparisonStartAt}ms ${others ? "then " + others : "(only one source saw it)"}`);
  }

  if (unverifiedRows.length) {
    push(`\n=== UNVERIFIED (baseline-incomplete collections: ${[...baselineComplete.entries()].filter(([, c]) => !c).map(([s]) => s).join(", ")}) -- NOT part of conclusions ===`);
    for (const row of unverifiedRows.slice(0, 20)) {
      push(`[${row.slug}] UNVERIFIED order ${row.orderHash.slice(0, 14)} price=${row.price} (baseline for this collection was incomplete -- cannot confirm this wasn't pre-existing)`);
    }
  }

  fs.writeFileSync(path.join(ROOT, "release-artifacts", "source-freshness-benchmark-v2-v1.25.41.txt"), log.join("\n") + "\n");
  push("evidence saved");
  process.exit(0);
})();
