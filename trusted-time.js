"use strict";

/**
 * trusted-time.js — a clock the user does not own.
 *
 * The licence expiry is sealed into the key by the Ed25519 signature, but
 * "has it passed yet?" was answered by `Date.now()` - and on a machine the user
 * controls, that is a number they can set. Winding Windows back a month was the
 * whole of the attack.
 *
 * WHERE THE TIME COMES FROM
 *
 * Time.is publishes no machine-readable API. Probed on 2026-09-02, every path
 * (`/UTC+7`, `/`, `/api`, `/json`) answers 403 to a non-browser client, and the
 * page carries no <time> element, no JSON-LD and no epoch value. Their live
 * clock runs over a private WebSocket that is not published for third parties.
 * So there is nothing here that could honestly be called "the Time.is API", and
 * an HTML parser against a page that refuses to serve us is not trusted time.
 *
 * What every one of those responses DOES carry, 403 included, is the HTTP
 * `Date` header - the server's own clock, in a standard field (RFC 9110), with
 * no scraping. Measured against four other reputable hosts it agreed exactly:
 *
 *   time.is 403 · cloudflare 301 · google 200 · nist 200 · apple 200
 *   all 2026-09-02T05:15:33Z, spread 0 ms
 *
 * So: ask several hosts, take the MEDIAN. Time.is is first in the list because
 * it is the reference the owner asked for, but no single host can decide the
 * answer - one machine with a wrong clock, or one host blocking us, must not
 * move the result.
 *
 * WHAT THIS IS NOT
 *
 * One second of resolution, and no cryptographic binding. TLS stops a passive
 * attacker forging a Date header; it does not stop the owner of the machine
 * proxying their own. This RAISES THE BAR on winding the clock back. It is not
 * proof, and nothing that runs on the user's computer could be.
 *
 * It is also only a clock. Whether a licence is ACTIVE or REVOKED is the
 * licence server's business and is never asked of these hosts.
 */

const https = require("https");
const fs = require("fs");
const path = require("path");

/**
 * Where to ask. Time.is first, as the reference; the rest so that no single
 * host - blocked, slow, or simply wrong - decides what time it is.
 */
const TIME_HOSTS = [
  "https://time.is/UTC+7",
  "https://www.cloudflare.com/",
  "https://www.google.com/",
  "https://www.apple.com/"
];

/** A clock check must never hold the app up. */
const REQUEST_TIMEOUT_MS = 6000;

/** How long a sync is good for before another is attempted. */
const SYNC_TTL_MS = 30 * 60 * 1000;

/**
 * How far the wall clock may sit below the highest time we have ever seen
 * before that counts as the clock having been wound back.
 *
 * Generous, because ordinary machines drift, resume from sleep, and correct
 * themselves against NTP. Five minutes is far more than any of that and far
 * less than the days an attacker would need to gain.
 */
const ROLLBACK_TOLERANCE_MS = 5 * 60 * 1000;

/** One HEAD request; the answer we want is in the headers. */
function askHost(url) {
  return new Promise(resolve => {
    const started = Date.now();
    let done = false;
    const finish = value => { if (!done) { done = true; resolve(value); } };

    const req = https.request(
      url,
      { method: "HEAD", headers: { "user-agent": "OpenSeaOfferBot/clock" } },
      res => {
        res.resume();
        const header = res.headers.date;
        const ms = header ? Date.parse(header) : NaN;
        // Half the round trip: the header was written somewhere in the middle
        // of it, and without this every answer reads systematically early.
        const rtt = Date.now() - started;
        finish(Number.isFinite(ms) ? { url, ms: ms + Math.round(rtt / 2), rtt } : null);
      }
    );

    req.on("error", () => finish(null));
    req.setTimeout(REQUEST_TIMEOUT_MS, () => { req.destroy(); finish(null); });
    req.end();
  });
}

/**
 * What time the internet says it is, or null.
 *
 * The median, not the mean: one host answering nonsense should not drag the
 * result, and with an even count the lower of the middle pair is taken so the
 * answer is always one a real host actually gave.
 */
async function fetchInternetTime(hosts = TIME_HOSTS) {
  const answers = (await Promise.all(hosts.map(askHost))).filter(Boolean);
  if (!answers.length) return null;

  const sorted = answers.map(a => a.ms).sort((a, b) => a - b);
  const median = sorted[Math.floor((sorted.length - 1) / 2)];

  return {
    utcMs: median,
    hosts: answers.length,
    spreadMs: sorted[sorted.length - 1] - sorted[0],
    sources: answers.map(a => new URL(a.url).host)
  };
}

/**
 * The clock the licence is judged against.
 *
 * State lives in the machine directory - the same per-PC place as device.json -
 * so every instance shares one view of the time and a rollback cannot be hidden
 * by opening a different instance.
 */
class TrustedClock {
  constructor({ directory, onLog = () => {}, hosts = TIME_HOSTS, ttlMs = SYNC_TTL_MS } = {}) {
    this.directory = directory || ".";
    this.file = path.join(this.directory, "trusted-time.json");
    this.onLog = onLog;
    this.hosts = hosts;
    this.ttlMs = ttlMs;

    /** Monotonic, and therefore immune to the clock moving under us. */
    this.syncedAtMonotonic = null;
    this.syncedUtcMs = null;

    this.state = this.read();
    this.inFlight = null;
  }

  read() {
    try {
      const parsed = JSON.parse(fs.readFileSync(this.file, "utf8"));
      return {
        // The highest time ever observed. Only ever raised - see note().
        highWaterUtcMs: Number(parsed.highWaterUtcMs) || 0,
        lastSyncUtcMs: Number(parsed.lastSyncUtcMs) || 0,
        lastSyncWallMs: Number(parsed.lastSyncWallMs) || 0,
        lastSyncAt: Number(parsed.lastSyncAt) || 0,
        sources: Array.isArray(parsed.sources) ? parsed.sources : []
      };
    } catch {
      return { highWaterUtcMs: 0, lastSyncUtcMs: 0, lastSyncWallMs: 0, lastSyncAt: 0, sources: [] };
    }
  }

  write() {
    try {
      fs.mkdirSync(this.directory, { recursive: true });
      fs.writeFileSync(this.file, JSON.stringify(this.state, null, 2));
    } catch (error) {
      this.onLog(`[CLOCK] không ghi được trusted-time: ${error.message}`);
    }
  }

  /**
   * Record a time we believe, and raise the high-water mark.
   *
   * The mark only ever goes UP. Editing the file downwards is possible - it is
   * a file on the user's own machine and nothing here can sign it - but doing
   * so only erases the evidence of a rollback; it cannot manufacture time the
   * licence has not used, because expiry is still checked against the internet
   * the moment one is reachable.
   */
  note(utcMs, { synced = false, sources = [] } = {}) {
    if (!Number.isFinite(utcMs) || utcMs <= 0) return;
    let changed = false;

    if (utcMs > this.state.highWaterUtcMs) {
      this.state.highWaterUtcMs = utcMs;
      changed = true;
    }
    if (synced) {
      this.state.lastSyncUtcMs = utcMs;
      this.state.lastSyncWallMs = Date.now();
      this.state.lastSyncAt = Date.now();
      this.state.sources = sources;
      this.syncedUtcMs = utcMs;
      this.syncedAtMonotonic = process.hrtime.bigint();
      changed = true;
    }
    if (changed) this.write();
  }

  /** Ask the internet, at most one request set at a time. */
  async sync() {
    if (this.inFlight) return this.inFlight;
    this.inFlight = fetchInternetTime(this.hosts)
      .then(result => {
        if (!result) {
          this.onLog("[CLOCK] không lấy được giờ internet");
          return null;
        }
        this.note(result.utcMs, { synced: true, sources: result.sources });
        this.onLog(
          `[CLOCK] đồng bộ từ ${result.hosts} nguồn (${result.sources.join(", ")}) ` +
          `· lệch so với giờ máy ${result.utcMs - Date.now()}ms`
        );
        return result;
      })
      .finally(() => { this.inFlight = null; });
    return this.inFlight;
  }

  /** Sync when the last one has aged out. Never awaited by a gate. */
  syncIfStale() {
    if (Date.now() - this.state.lastSyncAt <= this.ttlMs) return null;
    return this.sync();
  }

  /**
   * The best answer available, and how much it is worth.
   *
   * `source`:
   *   "monotonic"  synced this process; the clock cannot have moved under it
   *   "wall"       synced earlier; wall clock carries it, rollback checked
   *   "none"       never synced; only the wall clock, which may be anything
   */
  now() {
    if (this.syncedUtcMs !== null && this.syncedAtMonotonic !== null) {
      const elapsedMs = Number(process.hrtime.bigint() - this.syncedAtMonotonic) / 1e6;
      const utcMs = this.syncedUtcMs + elapsedMs;
      this.note(utcMs);
      return { utcMs, source: "monotonic", trusted: true };
    }

    const wall = Date.now();
    this.note(wall);

    if (this.state.lastSyncUtcMs > 0) {
      return { utcMs: wall, source: "wall", trusted: false };
    }
    return { utcMs: wall, source: "none", trusted: false };
  }

  /**
   * Has the clock been wound back?
   *
   * Compared against the highest time ever seen, which survives a restart and a
   * reboot. A machine that has genuinely moved forward never trips this.
   */
  rollback() {
    const wall = Date.now();
    const mark = this.state.highWaterUtcMs;
    if (!mark) return { detected: false, byMs: 0 };
    const behind = mark - wall;
    return behind > ROLLBACK_TOLERANCE_MS
      ? { detected: true, byMs: behind }
      : { detected: false, byMs: Math.max(0, behind) };
  }

  /**
   * Has the licence expired, judged as safely as the evidence allows?
   *
   * Expired if EITHER clock says so. That is fail-closed in both directions:
   * winding forward locks the app early, which is the user's own doing, and
   * winding back cannot revive a licence the trusted clock has already buried.
   *
   * When the clock has been wound back and no trusted time is available, the
   * answer is `undetermined` - the caller locks rather than guesses.
   */
  expiryVerdict(expiresAtMs) {
    const exp = Number(expiresAtMs) || 0;
    if (!exp) return { expired: true, undetermined: false, reason: "no-exp" };

    const trusted = this.now();
    const roll = this.rollback();

    if (trusted.utcMs >= exp) {
      return { expired: true, undetermined: false, reason: trusted.source, trusted };
    }
    if (Date.now() >= exp) {
      return { expired: true, undetermined: false, reason: "wall-clock", trusted };
    }

    // The clock has moved backwards and nothing in this process can say what
    // time it really is. Anything else here would be a guess in the user's
    // favour, made from the one number they control.
    if (roll.detected && !trusted.trusted) {
      return {
        expired: false,
        undetermined: true,
        reason: "clock-rollback",
        rollbackByMs: roll.byMs,
        trusted
      };
    }

    return { expired: false, undetermined: false, reason: trusted.source, trusted };
  }

  /** What the Settings screen shows. No host list beyond the names. */
  status() {
    const now = this.now();
    const roll = this.rollback();
    return {
      utcMs: Math.round(now.utcMs),
      source: now.source,
      trusted: now.trusted,
      lastSyncAt: this.state.lastSyncAt,
      sources: this.state.sources,
      rollbackDetected: roll.detected,
      rollbackByMs: roll.byMs
    };
  }
}

module.exports = {
  TrustedClock,
  fetchInternetTime,
  TIME_HOSTS,
  ROLLBACK_TOLERANCE_MS,
  SYNC_TTL_MS,
  REQUEST_TIMEOUT_MS
};
