"use strict";

/**
 * Bounded TTL caches (spec §19).
 *
 * Every cache here has BOTH a TTL and a hard entry cap, so a session that runs
 * for many hours cannot grow unbounded. `sweepAll()` is called on a timer from
 * main.js; it only drops expired entries.
 *
 * Nothing in this file ever touches API keys, the private key, or user
 * settings - those live in db.js and are never evicted.
 */

class TTLCache {
  /**
   * @param {string} name
   * @param {number} ttlMs   entry lifetime
   * @param {number} maxSize hard cap; oldest insertions are evicted first
   */
  constructor(name, ttlMs, maxSize = 500) {
    this.name = name;
    this.ttlMs = ttlMs;
    this.maxSize = maxSize;
    this.map = new Map(); // key -> { value, expiresAt }
    this.hits = 0;
    this.misses = 0;
  }

  get(key) {
    const entry = this.map.get(key);
    if (!entry) {
      this.misses++;
      return undefined;
    }
    if (entry.expiresAt <= Date.now()) {
      this.map.delete(key);
      this.misses++;
      return undefined;
    }
    this.hits++;
    return entry.value;
  }

  has(key) {
    return this.get(key) !== undefined;
  }

  set(key, value, ttlMs = this.ttlMs) {
    // Map preserves insertion order: re-inserting moves the key to the end so
    // the eviction below always drops the least-recently-written entry.
    if (this.map.has(key)) this.map.delete(key);
    this.map.set(key, { value, expiresAt: Date.now() + ttlMs });

    while (this.map.size > this.maxSize) {
      const oldest = this.map.keys().next();
      if (oldest.done) break;
      this.map.delete(oldest.value);
    }
    return value;
  }

  delete(key) {
    return this.map.delete(key);
  }

  clear() {
    this.map.clear();
  }

  /** Drop expired entries. Safe to call on a timer. */
  sweep() {
    const now = Date.now();
    let removed = 0;
    for (const [key, entry] of this.map) {
      if (entry.expiresAt <= now) {
        this.map.delete(key);
        removed++;
      }
    }
    return removed;
  }

  get size() {
    return this.map.size;
  }

  stats() {
    return {
      name: this.name,
      size: this.map.size,
      maxSize: this.maxSize,
      ttlMs: this.ttlMs,
      hits: this.hits,
      misses: this.misses
    };
  }
}

// ------------------------------------------------------------------
// Shared cache instances
// ------------------------------------------------------------------

/** NFT name / image / collection slug. Rarely changes -> long TTL. */
const nftMetaCache = new TTLCache("nft-meta", 10 * 60 * 1000, 1000);

/** Collection fee config + signed-zone requirement. */
const collectionFeeCache = new TTLCache("collection-fees", 10 * 60 * 1000, 300);

/** Collection slug lookup by chain+contract. */
const collectionSlugCache = new TTLCache("collection-slug", 30 * 60 * 1000, 500);

/**
 * Token standard ("erc721" / "erc1155") by chain+contract.
 *
 * A property of the CONTRACT, not of a token, and one that cannot change, so
 * it is keyed per contract and kept for a long time. It has to be right: an
 * offer whose consideration item does not match the contract's standard is
 * rejected by OpenSea with a 400 that no retry can fix.
 */
const tokenStandardCache = new TTLCache("token-standard", 6 * 60 * 60 * 1000, 500);

/**
 * Best Offer results. Deliberately SHORT (spec §19: "Best Offer cache TTL
 * ngắn") - it exists only to collapse duplicate reads fired by a stream burst,
 * never to serve a stale price to the decision engine.
 */
const bestOfferCache = new TTLCache("best-offer", 1200, 500);

/** Collection-wide active offers, used when resolving applicable offers. */
const collectionOffersCache = new TTLCache("collection-offers", 5000, 200);

// ------------------------------------------------------------------
// Optimistic My Offer cache (spec §10)
// ------------------------------------------------------------------

/**
 * After a successful submit we record the price locally and refuse to let a
 * not-yet-indexed API response overwrite it. Held for at least 60s.
 *
 * Prevents the classic double-send:
 *   send 0.128 -> API not indexed -> My Offer = "-" -> send 0.128 again
 */
const OPTIMISTIC_TTL_MS = 60 * 1000;

class OptimisticOfferCache {
  constructor(ttlMs = OPTIMISTIC_TTL_MS, maxSize = 500) {
    this.cache = new TTLCache("optimistic-my-offer", ttlMs, maxSize);
  }

  /** @param {string} key `${chain}:${contract}:${tokenId}` */
  record(key, price, orderHash = null) {
    return this.cache.set(key, {
      price: Number(price) || 0,
      orderHash: orderHash || null,
      at: Date.now()
    });
  }

  /**
   * Resolve the value the UI/engine should treat as "My Offer".
   * The optimistic value wins unless the API reports something >= it, which
   * means indexing caught up (or the user bid higher elsewhere).
   *
   * @param {number|null} apiPrice price returned by the API, or null
   * @returns {{price: number|null, orderHash: string|null, optimistic: boolean}}
   */
  resolve(key, apiPrice, apiOrderHash = null) {
    const pending = this.cache.get(key);
    const api = Number(apiPrice);
    const hasApi = Number.isFinite(api) && api > 0;

    if (!pending) {
      return {
        price: hasApi ? api : null,
        orderHash: hasApi ? apiOrderHash : null,
        optimistic: false
      };
    }

    if (hasApi && api >= pending.price) {
      // Indexing caught up - drop the optimistic entry.
      this.cache.delete(key);
      return { price: api, orderHash: apiOrderHash, optimistic: false };
    }

    return {
      price: pending.price,
      orderHash: pending.orderHash || apiOrderHash || null,
      optimistic: true
    };
  }

  peek(key) {
    return this.cache.get(key) || null;
  }

  drop(key) {
    return this.cache.delete(key);
  }

  sweep() {
    return this.cache.sweep();
  }

  stats() {
    return this.cache.stats();
  }
}

const optimisticOffers = new OptimisticOfferCache();

const ALL_CACHES = [
  nftMetaCache,
  collectionFeeCache,
  collectionSlugCache,
  tokenStandardCache,
  bestOfferCache,
  collectionOffersCache
];

/** Sweep every registered cache. Returns how many entries were dropped. */
function sweepAll() {
  let removed = 0;
  for (const cache of ALL_CACHES) removed += cache.sweep();
  removed += optimisticOffers.sweep();
  return removed;
}

function statsAll() {
  return {
    caches: ALL_CACHES.map(c => c.stats()),
    optimistic: optimisticOffers.stats()
  };
}

module.exports = {
  TTLCache,
  OptimisticOfferCache,
  nftMetaCache,
  collectionFeeCache,
  collectionSlugCache,
  tokenStandardCache,
  bestOfferCache,
  collectionOffersCache,
  optimisticOffers,
  OPTIMISTIC_TTL_MS,
  sweepAll,
  statsAll
};
