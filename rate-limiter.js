"use strict";

/**
 * Global rate limiter + API key failover (spec 16, 17).
 *
 * Every OpenSea HTTP call in the app goes through this module. Scan workers do
 * NOT own private limiters - they all queue on the same buckets, so raising
 * SCAN_WORKERS can never multiply the request rate.
 *
 * Buckets are separate per kind because OpenSea meters them differently:
 *   READ   - GET endpoints (metadata, offers, best offer)
 *   ORDER  - POST /orders/.../offers
 *   CANCEL - offer cancellation (offchain POST + order lookups)
 */

const { logger } = require("./logger");

const KIND = Object.freeze({
  READ: "read",
  // Building an order (OpenSea's CreateOfferTimelineQuery) is a write, and it
  // used to ask for a KIND.WRITE that did not exist here. An unknown kind falls
  // back to READ, so the heaviest step of every Offer batch ran in the same
  // lane as Cancel's discovery - and every 429 it earned was charged to that
  // lane. Offer must not be able to throttle Cancel.
  WRITE: "write",
  ORDER: "order",
  CANCEL: "cancel"
});

/**
 * Measured against the live OpenSea API (not guessed):
 *
 *   READ   10 req/s sustained for 12s  -> 0x 429
 *          20 req/s sustained for 12s  -> 12x 429
 *          16 requests fully parallel  -> 0x 429, all done in 420ms
 *
 *   ORDER-DETAIL (used by batch cancel)
 *          6 req/s sustained  -> 0x 429
 *          8 req/s sustained  -> 429s appear
 *          short burst of 18 at concurrency 6 -> 0x 429 in 205ms
 *
 * So the correct model is a TOKEN BUCKET - a sustained rate plus a burst
 * allowance - not a fixed serial gap. The previous fixed 510ms gap capped the
 * whole app at ~2 req/s, roughly 5x slower than the API actually permits, and
 * that was the single biggest source of "the bot is too slow".
 *
 *   ORDER (POST /orders/.../offers) - probed with deliberately INVALID payloads,
 *          which still carry the headers, so nothing was ever created:
 *          the endpoint advertises x-ratelimit-limit: 2
 *          2 req/s sustained -> 0x 429
 *          4 req/s sustained -> 7x 429
 *          So 2/s is the real ceiling for submitting offers. Fifty offers take
 *          about 25 seconds and there is no legitimate way to go faster.
 */
/**
 * How long the API must stay quiet before the limiter climbs one step back up.
 * Spec II: back off on 429, then recover gradually - never ratchet only down.
 */
const RECOVERY_QUIET_MS = 20000;

/**
 * What ONE OpenSea API key is allowed, per second.
 *
 * These are the account limits, not a tuning knob - raising them does not buy
 * throughput, it buys 429s. They live here as named values so a different
 * account tier can be configured in one place instead of being hunted for
 * across the codebase.
 *
 * There is exactly one key. No pool, no round-robin: a second key would not
 * change what a single account is permitted.
 */
/*
 * READ, re-measured against the live endpoint the scanner actually uses
 * (GET /offers/collection/{slug}/nfts/{id}), read-only, on 2026-09-01:
 *
 *   the endpoint advertises x-ratelimit-limit: 120, but `remaining` never
 *   decrements and `reset` never moves, so the header is an advertisement
 *   rather than a live counter and cannot be paced against.
 *
 *   sustained 5s at   4 /s -> 0x 429
 *                     8 /s -> 0x 429
 *                    12 /s -> 0x 429
 *                    16 /s -> 0x 429
 *                    24 /s -> 0x 429   (120 requests, exactly the advertised
 *                                       number, still no refusal)
 *                    40 /s -> 0x 429
 *                    60 /s -> 0x 429
 *                    90 /s -> 0x 429   (450 requests in 5.2s)
 *
 * The enforced ceiling was never reached. 10/s is therefore NOT the edge - it
 * is a deliberate step up from 4 that two independent measurements agree is
 * safe (this probe, and the 10/s-for-12s run recorded above), and it leaves an
 * order of magnitude of headroom rather than sitting on a boundary a five
 * second probe cannot honestly claim to have found.
 */
/**
 * THE ACCOUNT QUOTA.
 *
 * What OpenSea grants THIS account, and the only place it is written down.
 * Rate and concurrency together: they describe one quota and separating them
 * is how two of the four numbers ended up unchangeable.
 *
 * Current entitlement:
 *   READ  4/s      ORDER 2/s      FULFILLMENT 2/s
 *
 * If OpenSea raises the quota, change these numbers and nothing else. No
 * part of the Offer Item flow - stream, book, best, decision, verify, sign,
 * submit - reads them or needs to know they moved.
 *
 * Concurrency is how many may be in flight at once, and it is NOT the rate.
 * A lane at 2/s with a 550ms round trip delivers ~1.8/s at concurrency 1,
 * because the tokens it was granted expire while it waits; two in flight
 * spend them. Concurrency above what the rate can feed buys nothing.
 */
const RATE_LIMITS = {
  READ_RPS: 4,
  WRITE_RPS: 2,
  ORDER_RPS: 2,
  FULFILLMENT_RPS: 2,

  READ_CONCURRENCY: 4,
  WRITE_CONCURRENCY: 2,
  ORDER_CONCURRENCY: 2,
  FULFILLMENT_CONCURRENCY: 2
};

/**
 * Burst and concurrency follow from the rate.
 *
 * Burst is one second of allowance (READ gets a little headroom so a scan
 * that needs two reads is not split across a second). Concurrency is how many
 * may be on the wire at once, which for writes is one: OpenSea orders them
 * anyway, and more in flight only makes the queue harder to reason about.
 */
function limitsFor(rps, { burstFactor = 1, concurrency = null } = {}) {
  return {
    rps,
    burst: Math.max(1, Math.round(rps * burstFactor)),
    concurrency: concurrency === null ? Math.max(1, rps) : concurrency
  };
}

/**
 * The starting limits, derived from the quota block above.
 *
 * Not a second set of numbers: this used to hard-code concurrency again, so
 * the live READ lane ran at 6 while the quota said 4. Two answers to one
 * question, and the configurable one was the one nothing read.
 */
const DEFAULT_LIMITS = {
  [KIND.READ]: limitsFor(RATE_LIMITS.READ_RPS, {
    burstFactor: 2, concurrency: RATE_LIMITS.READ_CONCURRENCY
  }),
  [KIND.WRITE]: limitsFor(RATE_LIMITS.WRITE_RPS, {
    burstFactor: 2, concurrency: RATE_LIMITS.WRITE_CONCURRENCY
  }),
  // Burst 1 on ORDER: the endpoint advertises x-ratelimit-limit 2 and a
  // burst above the rate would spend a second's worth of writes at once.
  [KIND.ORDER]: limitsFor(RATE_LIMITS.ORDER_RPS, {
    burstFactor: 1, concurrency: RATE_LIMITS.ORDER_CONCURRENCY
  }),
  [KIND.CANCEL]: limitsFor(RATE_LIMITS.FULFILLMENT_RPS, {
    burstFactor: 2, concurrency: RATE_LIMITS.FULFILLMENT_CONCURRENCY
  })
};

/**
 * Change the account limits at runtime. Everything already queued keeps its
 * place; only the rate at which the queue drains changes.
 */
function configureLimits(next = {}) {
  for (const key of Object.keys(RATE_LIMITS)) {
    const value = Number(next[key]);
    if (Number.isFinite(value) && value > 0) RATE_LIMITS[key] = value;
  }
  // Every number comes from RATE_LIMITS. Nothing here is hard-coded, so a
  // quota change is a data change.
  const applied = {
    [KIND.READ]: limitsFor(RATE_LIMITS.READ_RPS, {
      burstFactor: 2, concurrency: RATE_LIMITS.READ_CONCURRENCY
    }),
    [KIND.WRITE]: limitsFor(RATE_LIMITS.WRITE_RPS, {
      burstFactor: 2, concurrency: RATE_LIMITS.WRITE_CONCURRENCY
    }),
    [KIND.ORDER]: limitsFor(RATE_LIMITS.ORDER_RPS, {
      burstFactor: 1, concurrency: RATE_LIMITS.ORDER_CONCURRENCY
    }),
    [KIND.CANCEL]: limitsFor(RATE_LIMITS.FULFILLMENT_RPS, {
      burstFactor: 2, concurrency: RATE_LIMITS.FULFILLMENT_CONCURRENCY
    })
  };
  for (const [kind, limits] of Object.entries(applied)) {
    const bucket = buckets.get(kind);
    if (!bucket) continue;
    bucket.rps = limits.rps;
    bucket.burst = limits.burst;
    bucket.concurrency = limits.concurrency;
    if (bucket.tokens > limits.burst) bucket.tokens = limits.burst;
    bucket.pump();
  }
  return { ...RATE_LIMITS };
}

/**
 * How urgent a request is. Lower number wins.
 *
 *   P0  a rival's bid landed on a token we hold and the answer needs a read
 *   P1  a collection-wide offer that could put us under
 *   P2  background verification of a criteria offer that may not even apply
 *   P3  reconciliation, upkeep, anything nobody is waiting on
 *
 * Without this a P0 read queued behind whatever background work happened to
 * ask first: measured REST queue p50 3281ms, p90 6669ms, almost all of it
 * trait verification the row did not need in order to decide.
 */
const PRIORITY = Object.freeze({
  /**
   * An initial obligation the batch owes.
   *
   * Above P0 on purpose, and only for reads. Pressing START promises that
   * every NFT gets looked at; a stream of realtime arrivals must not be able
   * to postpone that promise indefinitely. Measured before this existed: 16 of
   * 25 NFTs sat at `decisions=0` for a whole run because every read they
   * queued was overtaken by newer event-driven reads.
   *
   * This does NOT reorder P0/P1/P2/P3 relative to each other.
   */
  INITIAL: -1,
  P0: 0,
  P1: 1,
  P2: 2,
  P3: 3
});

/**
 * The rejection a cancelled waiter receives.
 *
 * Named so callers can tell "you were cancelled" apart from "the request
 * failed" - the first must never be reported as a failed submit.
 */
function abortError() {
  const error = new Error("rate-limit wait aborted");
  error.name = "AbortError";
  error.aborted = true;
  return error;
}

/** Anything at or below this number is urgent and never held back. */
const URGENT_PRIORITY = PRIORITY.P1;

/**
 * Capacity kept out of reach of background work.
 *
 * Even ordered by priority, a queue of background reads that arrives during a
 * quiet moment will take every token, and the P0 read that arrives one
 * millisecond later then waits for a refill. So background traffic may only
 * draw on the bucket while a slot and a token remain for urgent work.
 */
const RESERVED_SLOTS = 1;
const RESERVED_TOKENS = 1;

/**
 * How long a queued waiter must wait to gain one level of priority.
 *
 * Long enough that it never reorders normal traffic - an urgent submit is
 * served in well under this - and short enough that nothing sits behind a
 * busy stream for tens of seconds.
 */
const STARVATION_STEP_MS = 3000;

/**
 * Token bucket with a concurrency cap.
 *
 * A caller must hold BOTH a token (rate) and a slot (concurrency) before it may
 * fire, and returns the slot when the request settles. Waiters are served by
 * priority, and first-in-first-out within a priority so nothing starves.
 */
class Bucket {
  constructor(kind, { rps, burst, concurrency }) {
    this.kind = kind;
    this.rps = rps;
    this.burst = burst;
    this.concurrency = concurrency;

    this.tokens = burst;
    this.lastRefill = Date.now();
    this.active = 0;
    this.queue = [];

    this.granted = 0;
    this.throttledUntil = 0;
    this.pumpTimer = null;
    this.serverLimited = false;
    this.throttleHits = 0;

    /** Ties inside one priority are broken by arrival order, not by chance. */
    this.seq = 0;
    /**
     * Granted per priority, so the log can show who is actually being served.
     * Indexed with a +1 offset because INITIAL is -1.
     */
    this.grantedByPriority = [0, 0, 0, 0, 0];

    // Recovery bookkeeping: the configured rate is the ceiling we climb back
    // to, and a server-advertised limit lowers that ceiling permanently.
    this.baselineRps = rps;
    this.baselineBurst = burst;
    this.ceilingRps = rps;
    this.lastThrottleAt = 0;
    this.lastRecoveryAt = 0;
  }

  refill(now = Date.now()) {
    const elapsed = now - this.lastRefill;
    if (elapsed <= 0) return;
    this.lastRefill = now;
    this.tokens = Math.min(this.burst, this.tokens + (elapsed * this.rps) / 1000);
  }

  /**
   * Reserve capacity. Resolves with a `release` function the caller MUST call
   * when its request settles.
   * @param {number} [priority] see PRIORITY; lower is served first
   * @returns {Promise<() => void>}
   */
  acquire(priority = PRIORITY.P0, { signal = null } = {}) {
    const level = Number.isFinite(priority)
      ? Math.min(PRIORITY.P3, Math.max(PRIORITY.INITIAL, Math.floor(priority)))
      : PRIORITY.P0;

    return new Promise((resolve, reject) => {
      const waiter = {
        priority: level,
        seq: this.seq++,
        // When this waiter joined the queue, for ageing. See effectivePriority.
        queuedAt: Date.now(),
        // Set the moment this waiter is either granted or cancelled, so the
        // two can never both happen.
        settled: false,
        resolve: null
      };

      let onAbort = null;
      const detach = () => {
        if (onAbort && signal) signal.removeEventListener("abort", onAbort);
      };

      waiter.resolve = release => {
        if (waiter.settled) return false;
        waiter.settled = true;
        detach();
        resolve(release);
        return true;
      };

      // ---- real cancellation ----------------------------------------
      //
      // Racing a timeout against this promise is NOT enough: the waiter stays
      // in the queue, is granted capacity later, and the caller's request then
      // fires with nobody waiting for it. Proven in audit: engine recorded
      // TIMEOUT while the POST went out anyway - a phantom offer, and a token
      // spent on work nobody wanted.
      //
      // So an aborted waiter is REMOVED from the queue and can never be
      // granted. Capacity is not consumed on its behalf.
      if (signal) {
        if (signal.aborted) {
          waiter.settled = true;
          reject(abortError());
          return;
        }
        onAbort = () => {
          if (waiter.settled) return; // already granted: too late to cancel
          waiter.settled = true;
          const at = this.queue.indexOf(waiter);
          if (at >= 0) this.queue.splice(at, 1);
          detach();
          reject(abortError());
        };
        signal.addEventListener("abort", onAbort, { once: true });
      }

      this.queue.push(waiter);
      this.pump();
    });
  }

  /**
   * Can this bucket hold a slot back and still run anything?
   *
   * False when the bucket has no more slots than the reserve wants to keep -
   * reserving then denies every non-urgent waiter permanently instead of
   * merely putting it behind urgent ones.
   */
  reservesSlots() {
    return this.concurrency > RESERVED_SLOTS;
  }

  /** Same question for the token side: a burst of 1 cannot spare a token. */
  reservesTokens() {
    return this.burst > RESERVED_TOKENS;
  }

  /**
   * The waiter that should go next: most urgent first, then oldest.
   *
   * Background work is additionally held back from the last slot and the last
   * token, so an urgent read arriving a moment later still finds room - but
   * only on buckets that have a spare slot to hold back in the first place.
   */
  /**
   * Priority as the queue should see it right now.
   *
   * Urgency decides the order, but it must not decide it forever. On a bucket
   * that grants two a second, a steady stream of P0 submits kept a P3 waiting
   * behind them indefinitely - measured at 54.7 seconds for one offer while
   * event-driven ones went through in a fraction of that. The reserve added
   * in V23 guarantees background work CAN be granted; it says nothing about
   * when.
   *
   * So a waiter gains one level of priority for every STARVATION_STEP_MS it
   * has spent in the queue, and can never age past P1. Fresh urgent work
   * still goes first - that is the whole point - but nothing waits forever,
   * and an offer answering a rival is never overtaken by a routine one that
   * has merely been patient.
   */
  effectivePriority(waiter, at) {
    if (waiter.priority <= URGENT_PRIORITY) return waiter.priority;
    const waited = at - waiter.queuedAt;
    if (waited < STARVATION_STEP_MS) return waiter.priority;
    const levels = Math.floor(waited / STARVATION_STEP_MS);
    return Math.max(URGENT_PRIORITY, waiter.priority - levels);
  }

  takeNext() {
    if (!this.queue.length) return null;

    const at = Date.now();
    let bestIndex = -1;
    let best = null;
    let bestRank = Infinity;

    for (let i = 0; i < this.queue.length; i++) {
      const waiter = this.queue[i];

      // Cancelled between being queued and being chosen. Never grant capacity
      // to a waiter nobody is waiting on.
      if (waiter.settled) continue;

      if (waiter.priority > URGENT_PRIORITY) {
        // A reserve can only hold something back if there is something left
        // over to hold back with.
        //
        // On a bucket with a single execution slot, `slotsLeft` can never
        // exceed RESERVED_SLOTS, so this gate stopped being "urgent work goes
        // first" and became "non-urgent work never runs at all". Measured on
        // the ORDER bucket (concurrency 1): a P3 waiter sat with active=0 and
        // tokens=2 and was never granted, until its 90s deadline fired. Every
        // initial submit takes P3, so every one of them timed out while
        // event-driven P0 submits went through in ~200ms.
        //
        // So the gate applies only where it can actually reserve: a bucket
        // with more than one slot still behaves exactly as before.
        if (this.reservesSlots() && this.concurrency - this.active <= RESERVED_SLOTS) {
          continue;
        }
        if (this.reservesTokens() && this.tokens < 1 + RESERVED_TOKENS) {
          continue;
        }
      }

      // Ties inside a level are still broken by arrival order, because the
      // queue is scanned in order and only a STRICTLY better rank wins.
      const rank = this.effectivePriority(waiter, at);
      if (!best || rank < bestRank) {
        best = waiter;
        bestIndex = i;
        bestRank = rank;
      }
    }

    if (bestIndex < 0) return null;
    this.queue.splice(bestIndex, 1);
    return best;
  }

  pump() {
    const now = Date.now();
    this.refill(now);

    // A 429 puts the whole bucket to sleep so every worker backs off together.
    if (now < this.throttledUntil) {
      this.schedulePump(this.throttledUntil - now);
      return;
    }

    while (this.queue.length && this.tokens >= 1 && this.active < this.concurrency) {
      const waiter = this.takeNext();
      // Only background work is left and the reserve says it must wait: stop,
      // rather than spinning on a queue we have decided not to serve.
      if (!waiter) break;

      // Cancelled in the same tick it was chosen. Capacity is not spent, and
      // the loop moves on to whoever is actually still waiting.
      if (waiter.settled) continue;

      let released = false;
      const release = () => {
        if (released) return;
        released = true;
        this.active = Math.max(0, this.active - 1);
        this.pump();
      };

      // Reserve FIRST, then hand over. `resolve` returns false if the waiter
      // was cancelled a moment ago, and the reservation is undone rather than
      // being left held by nobody.
      this.tokens -= 1;
      this.active++;

      if (!waiter.resolve(release)) {
        this.tokens += 1;
        this.active = Math.max(0, this.active - 1);
        continue;
      }

      this.granted++;
      this.grantedByPriority[waiter.priority + 1]++;
    }

    if (!this.queue.length) return;

    // Still waiting: wake up when the next token matures, or when a slot frees.
    //
    // Background work needs its own wake-up. It can be held back while tokens
    // sit between 1 and the reserve - enough to serve an urgent read, not
    // enough to spend on upkeep - and in that window nothing else is coming to
    // restart the pump. Without this line a background verification could wait
    // for an unrelated release that may never happen.
    // Must agree with `takeNext`: if the token reserve does not apply on this
    // bucket, waking for it would schedule a pump for a threshold that is
    // never checked.
    const needed =
      this.queue.some(w => w.priority <= URGENT_PRIORITY) || !this.reservesTokens()
        ? 1
        : 1 + RESERVED_TOKENS;

    if (this.tokens < needed) {
      const msPerToken = 1000 / this.rps;
      this.schedulePump(
        Math.max(10, Math.ceil((needed - this.tokens) * msPerToken))
      );
    }
  }

  schedulePump(delay) {
    if (this.pumpTimer) return;
    this.pumpTimer = setTimeout(() => {
      this.pumpTimer = null;
      this.pump();
    }, delay);
    // Not unref'd: a queued request is real work in flight.
  }

  /** Called on a 429: stop the whole bucket briefly. */
  penalize(ms) {
    this.throttleHits++;
    this.lastThrottleAt = Date.now();

    const until = Date.now() + ms;
    if (until > this.throttledUntil) this.throttledUntil = until;
    // Drop the burst credit so we do not immediately re-burst into the limit.
    this.tokens = 0;

    // Back off the rate itself, not just this moment, so a sustained overload
    // does not simply repeat. Never below one request per second.
    const reduced = Math.max(1, Math.floor(this.rps * 0.7));
    if (reduced < this.rps) {
      logger.api(`${this.kind}: 429 → giảm ${this.rps}/s xuống ${reduced}/s`);
      this.rps = reduced;
    }

    this.schedulePump(ms);
  }

  /**
   * Climb back toward the configured rate once the API has been quiet.
   *
   * Without this, a single burst of 429s would leave the whole session throttled
   * for good - the bucket would only ever ratchet downwards.
   *
   * A limit the SERVER advertised is treated as a ceiling and never exceeded;
   * recovery only undoes the extra backoff we applied ourselves.
   */
  recover(now = Date.now()) {
    if (this.rps >= this.ceilingRps) return false;
    if (!this.lastThrottleAt) return false;
    if (now - this.lastThrottleAt < RECOVERY_QUIET_MS) return false;
    if (now - this.lastRecoveryAt < RECOVERY_QUIET_MS) return false;

    const next = Math.min(this.ceilingRps, this.rps + 1);
    if (next === this.rps) return false;

    this.rps = next;
    this.burst = Math.max(this.burst, Math.min(this.baselineBurst, next * 2));
    this.lastRecoveryAt = now;

    logger.api(`${this.kind}: không còn 429 → tăng lên ${this.rps}/s`);
    this.pump();
    return true;
  }

  /**
   * Adopt the limit OpenSea itself reports.
   *
   * Measured on a live 429: the response carries
   *   retry-after: 1
   *   x-ratelimit-limit: 4
   *   x-ratelimit-remaining: 0
   *   x-ratelimit-reset: <unix seconds>
   *
   * Guessing a rate and hoping is strictly worse than being told one, so the
   * bucket re-tunes itself downward to whatever the server advertises. It only
   * ever LOWERS the configured rate - a header must not be able to talk us into
   * hammering harder than we chose to.
   */
  adoptServerLimit(headers = {}) {
    const limit = Number(headers["x-ratelimit-limit"]);
    if (!Number.isFinite(limit) || limit <= 0) return false;

    if (limit < this.rps) {
      logger.api(
        `${this.kind}: OpenSea báo giới hạn ${limit}/s, hạ từ ${this.rps}/s xuống ${limit}/s`
      );
      this.rps = limit;
      this.ceilingRps = limit;
      this.burst = Math.max(1, Math.min(this.burst, limit * 2));
      this.serverLimited = true;
      return true;
    }
    return false;
  }

  configure({ rps, burst, concurrency } = {}) {
    if (Number.isFinite(rps) && rps > 0) {
      this.rps = rps;
      this.baselineRps = rps;
      this.ceilingRps = Math.min(rps, this.serverLimited ? this.ceilingRps : rps);
    }
    if (Number.isFinite(burst) && burst > 0) this.burst = burst;
    if (Number.isFinite(concurrency) && concurrency > 0) {
      this.concurrency = Math.floor(concurrency);
    }
    this.pump();
  }

  stats() {
    this.refill();
    return {
      kind: this.kind,
      rps: this.rps,
      burst: this.burst,
      concurrency: this.concurrency,
      tokens: Math.floor(this.tokens),
      active: this.active,
      queued: this.queue.length,
      granted: this.granted,
      // Who the bucket is actually serving. If urgent reads are starving, this
      // is where it shows before the latency report does.
      grantedByPriority: [...this.grantedByPriority],
      urgentQueued: this.queue.filter(w => w.priority <= URGENT_PRIORITY).length,
      ceilingRps: this.ceilingRps,
      serverLimited: this.serverLimited,
      throttleHits: this.throttleHits,
      throttledForMs: Math.max(0, this.throttledUntil - Date.now())
    };
  }
}

const buckets = new Map([
  [KIND.READ, new Bucket(KIND.READ, DEFAULT_LIMITS[KIND.READ])],
  [KIND.WRITE, new Bucket(KIND.WRITE, DEFAULT_LIMITS[KIND.WRITE])],
  [KIND.ORDER, new Bucket(KIND.ORDER, DEFAULT_LIMITS[KIND.ORDER])],
  [KIND.CANCEL, new Bucket(KIND.CANCEL, DEFAULT_LIMITS[KIND.CANCEL])]
]);

const recoveryTimer = setInterval(() => {
  for (const bucket of buckets.values()) {
    try {
      bucket.recover();
    } catch {
      /* recovery must never throw into the timer */
    }
  }
}, 5000);
recoveryTimer.unref?.();

function getBucket(kind) {
  return buckets.get(kind) || buckets.get(KIND.READ);
}

/**
 * Reserve capacity of the given kind.
 * @param {string} [kind]
 * @param {number} [priority] see PRIORITY; urgent work is served first and is
 *   never held behind background verification
 * @returns {Promise<() => void>} release - MUST be called when the request settles.
 */
function acquire(kind = KIND.READ, priority = PRIORITY.P0, opts = {}) {
  return getBucket(kind).acquire(priority, opts);
}

/**
 * Called when the API answers 429.
 *
 * The kind is REQUIRED and must be one of KIND. It is validated rather than
 * defaulted because the one caller that passed a DELAY here instead of a kind
 * silently penalised the READ bucket every time: getBucket falls back to READ
 * for anything it does not recognise, so Offer's throttling was charged to the
 * lane Cancel discovers on, and the server's own Retry-After was discarded.
 */
function penalize(kind, ms = 5000) {
  if (!Object.values(KIND).includes(kind)) {
    logger.api("penalize: kind không hợp lệ (" + String(kind) + ") — bỏ qua");
    return false;
  }
  getBucket(kind).penalize(ms);
  return true;
}

// ------------------------------------------------------------------
// Concurrency gate
// ------------------------------------------------------------------

/**
 * Counting semaphore used by the scan workers and the cancel pipeline.
 * Deliberately explicit rather than Promise.all(everything) (spec 14).
 */
class Semaphore {
  constructor(limit) {
    this.limit = Math.max(1, Number(limit) || 1);
    this.active = 0;
    this.queue = [];

    /** Ties inside one priority are broken by arrival order, not by chance. */
    this.seq = 0;
  }

  /**
   * Take a slot.
   *
   * Waiters are served most-urgent-first, and oldest-first within a priority.
   * A plain `acquire()` is P0, so every existing caller keeps behaving exactly
   * as it did - only work that explicitly says it is background yields.
   *
   * Background waiters are additionally kept off the last slot: ordering alone
   * still lets background work fill every slot in a quiet moment and leave an
   * urgent arrival waiting for one to drain.
   *
   * @param {number} [priority] see PRIORITY
   */
  acquire(priority = PRIORITY.P0) {
    const level = Number.isFinite(priority)
      ? Math.min(PRIORITY.P3, Math.max(PRIORITY.INITIAL, Math.floor(priority)))
      : PRIORITY.P0;

    const reserve = level > URGENT_PRIORITY && this.limit > 1 ? RESERVED_SLOTS : 0;

    if (this.active < this.limit - reserve) {
      this.active++;
      return Promise.resolve();
    }

    return new Promise(resolve => {
      this.queue.push({ resolve, priority: level, seq: this.seq++ });
    });
  }

  /** The waiter that should go next, or null when the reserve holds them all. */
  takeNext() {
    let bestIndex = -1;
    let best = null;

    for (let i = 0; i < this.queue.length; i++) {
      const waiter = this.queue[i];

      // Handing the last slot to background work is what the reserve exists
      // to prevent, on the way out as well as on the way in.
      if (waiter.priority > URGENT_PRIORITY && this.limit > 1) {
        if (this.active >= this.limit - RESERVED_SLOTS) continue;
      }

      if (!best || waiter.priority < best.priority) {
        best = waiter;
        bestIndex = i;
      }
    }

    if (bestIndex < 0) return null;
    this.queue.splice(bestIndex, 1);
    return best;
  }

  release() {
    this.active = Math.max(0, this.active - 1);

    const next = this.takeNext();
    if (next) {
      this.active++;
      next.resolve();
    }
  }

  async run(fn) {
    await this.acquire();
    try {
      return await fn();
    } finally {
      this.release();
    }
  }

  setLimit(limit) {
    const n = Math.max(1, Number(limit) || 1);
    this.limit = n;
    while (this.active < this.limit && this.queue.length) {
      const next = this.takeNext();
      if (!next) break;
      this.active++;
      next.resolve();
    }
  }

  stats() {
    return {
      limit: this.limit,
      active: this.active,
      queued: this.queue.length,
      urgentQueued: this.queue.filter(w => w.priority <= URGENT_PRIORITY).length
    };
  }
}

// ------------------------------------------------------------------
// API key manager - FAILOVER ONLY, never round-robin (spec 16)
// ------------------------------------------------------------------

/**
 * How long a key rests after a 429.
 *
 * Short on purpose: with a pool, a throttled key costs nothing while it rests
 * because traffic simply goes to the others, and OpenSea's window is measured
 * in seconds rather than minutes. The old five-minute exile was built around
 * having exactly one spare.
 */
const KEY_COOLDOWN_MS = 20 * 1000;

/** A timeout or transport error rests the key only briefly. */
const FAILURE_COOLDOWN_MS = 5 * 1000;

/** How long a 429 counts against a key when choosing where to send next. */
const RECENT_WINDOW_MS = 60 * 1000;

/**
 * How loaded a key may be before background work stops sending to it.
 *
 * A submit is the thing a user is actually waiting on and it cannot be retried
 * cheaply; a criteria verification can wait a moment or use a quieter key. So
 * background traffic yields once a key is carrying this many requests, which
 * keeps room on every key for a write that has not arrived yet.
 */
const BACKGROUND_KEY_SOFT_CAP = 2;

/**
 * How loaded Key 1 may be before a send-critical request stops preferring it
 * and falls back to whichever key has capacity. Matches the per-key in-flight
 * ceiling of the read dispatcher.
 */
const PRIMARY_SOFT_CAP = 4;

/** Short, non-reversible identifier for a key. Shown instead of the key. */
function keyFingerprint(key) {
  const raw = String(key || "").trim();
  if (!raw) return "";
  return require("crypto").createHash("sha256").update(raw).digest("hex").slice(0, 8);
}

/**
 * A pool of OpenSea API keys, used in rotation.
 *
 * The old shape was "primary, with a backup for when primary dies". That wasted
 * every key but one: OpenSea meters per key, so N keys are N times the quota,
 * but only if requests are actually spread across them. With one key the write
 * bucket is 2/s, which is what made twenty rows queue behind each other.
 *
 * So: round-robin across every healthy key, and take a key out of rotation only
 * while it is cooling down from a 429 or a failure. A key comes back on its own.
 */
class ApiKeyManager {
  constructor() {
    /** @type {Array<{key:string, blockedUntil:number, uses:number, errors:number, rateLimits:number}>} */
    this.pool = [];
    this.cancelKey = "";
    this.cursor = 0;
  }

  /**
   * Accepts the pool in any of the shapes the app has used:
   *   { apiKeys: "k1\nk2\nk3" }  or  { apiKeys: [...] }
   *   { apiKey, apiKey2 }        (the original two-key settings)
   */
  configure({ apiKeys, apiKey, apiKey2, cancelApiKey } = {}) {
    const collected = [];

    if (Array.isArray(apiKeys)) {
      collected.push(...apiKeys);
    } else if (typeof apiKeys === "string") {
      // One key per line, as the Settings box presents them.
      collected.push(...apiKeys.split(/[\r\n,;]+/));
    }

    collected.push(apiKey, apiKey2);

    const cleaned = [];
    const seen = new Set();
    for (const raw of collected) {
      const key = String(raw || "").trim();
      if (!key || seen.has(key)) continue;
      seen.add(key);
      cleaned.push(key);
    }

    // Keep the health of keys that are still configured; a settings save must
    // not wipe the cooldown on a key that is mid-429.
    const previous = new Map(this.pool.map(entry => [entry.key, entry]));
    this.pool = cleaned.map(
      key =>
        previous.get(key) || {
          key,
          blockedUntil: 0,
          uses: 0,
          errors: 0,
          rateLimits: 0,
          // Capacity signals: what this key is carrying and how recently it
          // was throttled. Selection reads these instead of guessing.
          inFlight: 0,
          recentRateLimits: 0,
          lastRateLimitAt: 0,
          // Reads and writes are counted apart so background verification can
          // be kept off a key that a submit is about to need.
          writesInFlight: 0,
          // Rolling round-trip time, so a key that has gone slow loses ties to
          // one that has not.
          latencyMs: 0,
          // Request distribution, for the LiveView. Counted where a request
          // actually takes a lease (never on a peek).
          served: { critical: 0, background: 0, write: 0, cancel: 0 },
          // Never the key: a short hash so two keys can be told apart on
          // screen and in logs without either being shown.
          fp: keyFingerprint(key),
          role: "extra",
          slot: 0
        }
    );
    // KEY 1 / KEY 2 — HAI CAPACITY NGANG HÀNG
    //
    //   Từ 1.25.0 không còn "Key 1 mạnh / Key 2 yếu". Số thứ tự chỉ để gọi
    //   tên trên màn hình; việc chọn key đi theo capacity thật (cooldown,
    //   đang bay, 429 gần đây, độ trễ). Lượt GHI của Offer Item còn không đi
    //   qua đây nữa: QuotaBroker chọn key ngay lúc cấp giấy phép.
    this.pool.forEach((entry, index) => {
      entry.slot = index + 1;
      entry.role = `key${index + 1}`;
    });

    this.cancelKey = String(cancelApiKey || "").trim();
    if (this.cursor >= this.pool.length) this.cursor = 0;
  }

  /** Key 1 entry, or null. */
  primary() { return this.pool[0] || null; }
  /** Key 2 entry, or null. */
  secondary() { return this.pool[1] || null; }

  /**
   * Mọi key có thể ghi, key chưa cooldown trước. QuotaBroker nhận DANH SÁCH
   * này (dạng vân tay) và tự chọn key ghi được sớm nhất lúc cấp.
   */
  writeKeys(now = Date.now()) {
    const ready = this.pool.filter(e => e.blockedUntil <= now);
    const resting = this.pool.filter(e => e.blockedUntil > now)
      .sort((a, b) => a.blockedUntil - b.blockedUntil);
    return [...ready, ...resting].map(e => e.key);
  }
  /** Public description of one key: role, slot, fingerprint — never the key. */
  describe(key) {
    const entry = this.find(key);
    if (!entry) return null;
    return { slot: entry.slot, role: entry.role, fp: entry.fp };
  }

  /**
   * VÂN TAY CỦA TẬP KHOÁ ĐANG CẤU HÌNH — KHÔNG PHẢI KHOÁ ĐANG ĐƯỢC CHỌN
   *
   *   `current()` trả về khoá nào ĐANG rảnh nhất, và với hai khoá trở lên nó
   *   đổi qua đổi lại hoàn toàn hợp lệ giữa hai lần gọi. Ai dùng giá trị đó
   *   để hỏi "người dùng có đổi API key không" sẽ nhận câu trả lời "có" liên
   *   tục — và Stream bị nối lại mỗi vài giây trong khi chẳng ai đổi gì.
   *
   *   Câu hỏi đúng là về TẬP khoá. Vân tay này đổi khi và chỉ khi tập khoá
   *   đổi; thứ tự không tính, vì thứ tự là việc của bộ chọn.
   *
   *   Không bao giờ chứa khoá: chỉ là một bản rút gọn để so sánh.
   */
  fingerprint() {
    if (!this.pool.length) return "";
    const joined = this.pool.map(e => e.key).sort().join(" ");
    return require("crypto").createHash("sha256").update(joined).digest("hex").slice(0, 16);
  }

  hasAnyKey() {
    return this.pool.length > 0;
  }

  size() {
    return this.pool.length;
  }

  /** Keys not currently cooling down. */
  healthy(now = Date.now()) {
    return this.pool.filter(entry => entry.blockedUntil <= now);
  }

  /**
   * The next key to use.
   *
   * Round-robin over healthy keys. When every key is cooling down the one that
   * recovers soonest is returned rather than nothing: the caller's own limiter
   * and retry are a better place to wait than a hard failure here.
   */
  /**
   * The key with the most capacity right now.
   *
   * Plain round-robin handed the next request to whichever key was next in
   * line, even one already saturated: measured rate-limit wait reached p90
   * 9063ms while other keys sat idle. Capacity is what matters, so a key is
   * scored on what it is actually carrying.
   *
   * A key is picked by, in order: not cooling down, fewest requests in flight,
   * fewest recent 429s, lowest round-trip time, then round-robin to break ties
   * evenly.
   *
   * Background work is additionally kept off a key that is already loaded. A
   * submit cannot be retried cheaply and nobody is waiting on a verification,
   * so when the two compete the submit wins - the read simply uses a quieter
   * key, or waits a moment for one.
   *
   * @param {number} [now]
   * @param {{background?: boolean, write?: boolean}} [opts]
   */
  current(now = Date.now(), opts = {}) {
    if (!this.pool.length) return "";

    const background = opts.background === true;
    const write = opts.write === true;

    const healthy = this.pool.filter(entry => entry.blockedUntil <= now);

    if (!healthy.length) {
      // Everything is resting; hand back whichever frees up first and let the
      // caller's limiter do the waiting.
      const soonest = this.pool.reduce((best, entry) =>
        entry.blockedUntil < best.blockedUntil ? entry : best
      );
      if (!opts.peek) { soonest.uses++; soonest.inFlight++; if(write)soonest.writesInFlight++; }
      return soonest.key;
    }

    // Forget 429s that are no longer relevant, so one bad minute does not
    // sideline a key for the rest of the session.
    for (const entry of healthy) {
      if (entry.lastRateLimitAt && now - entry.lastRateLimitAt > RECENT_WINDOW_MS) {
        entry.recentRateLimits = 0;
      }
    }

    // Background reads leave the loaded keys alone. If every key is loaded the
    // preference is dropped rather than failing: waiting forever for a perfect
    // key would be worse than sharing a busy one.
    let candidates = healthy;
    if (background) {
      const quiet = healthy.filter(
        entry => entry.inFlight < BACKGROUND_KEY_SOFT_CAP
      );
      if (quiet.length) candidates = quiet;
    }

    // NO ROLE PREFERENCE (1.25.0)
    //
    //   Key 1 is no longer "the strong key for sends". Both keys are real
    //   capacity; the scoring below picks whichever is freer. Background work
    //   additionally avoids a key that is carrying writes, so a verification
    //   never sits on the key a submit is using.
    if (background && candidates.length > 1) {
      const noWrites = candidates.filter(entry => !entry.writesInFlight);
      if (noWrites.length) candidates = noWrites;
    }

    let best = null;
    for (const entry of candidates) {
      if (!best) {
        best = entry;
        continue;
      }

      if (entry.inFlight !== best.inFlight) {
        if (entry.inFlight < best.inFlight) best = entry;
        continue;
      }

      if (entry.recentRateLimits !== best.recentRateLimits) {
        if (entry.recentRateLimits < best.recentRateLimits) best = entry;
        continue;
      }

      // A key that has gone slow is still healthy, but it is not the one to
      // hand an urgent request to.
      if (entry.latencyMs !== best.latencyMs) {
        if (entry.latencyMs < best.latencyMs) best = entry;
        continue;
      }

      // Equal on capacity: spread the load rather than always taking the first.
      if (entry.uses < best.uses) best = entry;
    }

    // Several acquisitions can choose a key before any reaches HTTP and owns
    // a lease. Rotate exact ties without touching inFlight; accounting still
    // begins and ends only around the actual request.
    const tied = candidates.filter(entry =>
      entry.inFlight === best.inFlight &&
      entry.recentRateLimits === best.recentRateLimits &&
      entry.latencyMs === best.latencyMs &&
      entry.uses === best.uses
    );
    if (opts.peek && tied.length > 1) {
      best = tied[this.cursor % tied.length];
      this.cursor = (this.cursor + 1) % Math.max(1, this.pool.length);
    }

    if (!opts.peek) {
      best.uses++; best.inFlight++; if (write) best.writesInFlight++;
      this.count(best, { background, write, cancel: opts.cancel === true });
    }
    return best.key;
  }

  /** Request distribution bookkeeping — one increment per real request. */
  count(entry, { background = false, write = false, cancel = false } = {}) {
    if (!entry || !entry.served) return;
    if (cancel) entry.served.cancel++;
    else if (write) entry.served.write++;
    else if (background) entry.served.background++;
    else entry.served.critical++;
  }

  /**
   * Fold one round-trip into a key's rolling latency.
   *
   * An exponential average, so a single slow response nudges the number
   * instead of redefining it.
   */
  peek(now = Date.now(), opts = {}) { return this.current(now, {...opts, peek: true}); }

  leaseKey(key, {write = false, background = false} = {}) {
    const entry = this.find(key);
    if(entry) { entry.inFlight++; entry.uses++; if(write)entry.writesInFlight++; this.count(entry, { write, background }); }
    let released = false;
    return {key, release: () => {if(released)return;released=true;if(entry)this.releaseKey(key,{write});}};
  }

  reportLatency(key, ms) {
    const entry = this.find(key);
    if (!entry || !Number.isFinite(ms) || ms < 0) return;
    entry.latencyMs = entry.latencyMs
      ? Math.round(entry.latencyMs * 0.7 + ms * 0.3)
      : Math.round(ms);
  }

  /**
   * A request finished; the key is carrying one less.
   * Called from exactly one place - the caller's finally - so the count cannot
   * drift no matter which way the request ended.
   */
  releaseKey(key, { write = false } = {}) {
    const entry = this.find(key);
    if (!entry) return;
    if (entry.inFlight > 0) entry.inFlight--;
    if (write && entry.writesInFlight > 0) entry.writesInFlight--;
  }

  /** Cancel traffic uses its own key when one is set, else the pool. */
  currentCancel() {
    return this.cancelKey || this.current(Date.now(), { cancel: true });
  }

  find(key) {
    const wanted = String(key || "").trim();
    return this.pool.find(entry => entry.key === wanted) || null;
  }

  /**
   * Take a key out of rotation for a while.
   * With several keys the next request simply goes to another one, so a single
   * throttled key no longer stalls everything behind it.
   */
  reportRateLimited(key, cooldownMs = KEY_COOLDOWN_MS) {
    const entry = this.find(key);
    if (!entry) return false;

    entry.rateLimits++;
    entry.recentRateLimits++;
    entry.lastRateLimitAt = Date.now();
    entry.blockedUntil = Math.max(entry.blockedUntil, Date.now() + cooldownMs);

    const healthy = this.healthy().length;
    logger.api(
      `Key ${entry.slot} (${entry.fp}) bị 429 → nghỉ ${Math.round(cooldownMs / 1000)}s, ` +
        `còn ${healthy}/${this.pool.length} key dùng được`
    );

    return healthy > 0;
  }

  /** A timeout or transport failure also rests the key, but briefly. */
  reportFailure(key) {
    const entry = this.find(key);
    if (!entry) return false;

    entry.errors++;
    entry.blockedUntil = Date.now() + FAILURE_COOLDOWN_MS;
    return this.healthy().length > 0;
  }

  /** Success clears any cooldown: the key is demonstrably working. */
  reportSuccess(key) {
    const entry = this.find(key);
    if (!entry) return;
    if (entry.blockedUntil <= Date.now()) entry.blockedUntil = 0;
  }

  stats() {
    const now = Date.now();
    return {
      keys: this.pool.length,
      healthy: this.healthy(now).length,
      hasCancelKey: Boolean(this.cancelKey),
      detail: this.pool.map(entry => ({
        slot: entry.slot,
        role: entry.role,
        fp: entry.fp,
        served: { ...entry.served },
        blocked: entry.blockedUntil > now,
        uses: entry.uses,
        inFlight: entry.inFlight,
        rateLimits: entry.rateLimits,
        recentRateLimits: entry.recentRateLimits,
        writesInFlight: entry.writesInFlight,
        latencyMs: entry.latencyMs,
        errors: entry.errors,
        cooldownMs: Math.max(0, entry.blockedUntil - now)
      })),

      // Kept so existing readers of these fields keep working.
      hasPrimary: this.pool.length > 0,
      hasBackup: this.pool.length > 1,
      usingBackup: this.healthy(now).length < this.pool.length
    };
  }
}

const apiKeys = new ApiKeyManager();

/**
 * Deliberately NOT unref'd: a pending rate-limit wait is real in-flight work,
 * and an unref'd timer would let a plain Node process (the check harness, a
 * CLI script) exit while a request was still queued.
 */
/**
 * How long a 429 says to wait, in ms.
 *
 * Prefers the explicit Retry-After header, then the reset timestamp, and only
 * falls back to a guess when the server said nothing. Measured live: OpenSea
 * sends "retry-after: 1".
 */
function retryDelayFromHeaders(headers = {}, fallbackMs = 2000) {
  const retryAfter = Number(headers["retry-after"]);
  if (Number.isFinite(retryAfter) && retryAfter >= 0) {
    return Math.min(60000, Math.max(250, retryAfter * 1000));
  }

  const reset = Number(headers["x-ratelimit-reset"]);
  if (Number.isFinite(reset) && reset > 0) {
    // Seconds-since-epoch in practice; tolerate ms just in case.
    const resetMs = reset > 1e12 ? reset : reset * 1000;
    const delta = resetMs - Date.now();
    if (delta > 0 && delta < 60000) return Math.max(250, delta);
  }

  return fallbackMs;
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function stats() {
  return {
    buckets: Array.from(buckets.values()).map(b => b.stats()),
    keys: apiKeys.stats()
  };
}

module.exports = {
  KIND,
  PRIORITY,
  URGENT_PRIORITY,
  BACKGROUND_KEY_SOFT_CAP,
  Bucket,
  DEFAULT_LIMITS,
  RATE_LIMITS,
  configureLimits,
  limitsFor,
  acquire,
  penalize,
  retryDelayFromHeaders,
  getBucket,
  Semaphore,
  ApiKeyManager,
  apiKeys,
  keyFingerprint,
  PRIMARY_SOFT_CAP,
  sleep,
  stats
};
