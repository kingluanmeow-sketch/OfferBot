"use strict";

// Only actual HTTP requests acquire a lease. Chains share a key's server quota,
// but independent keys have independent AIMD state. No key material in stats.
//
// 1.25.31 ceilings (owner's OpenSea dashboard): Key 1 read 4/s, Key 2 read
// 2/s. OpenSea says self-serve limits may be shared across an account's keys
// and that was not disproven live, so reads also pass one GLOBAL bucket sized
// to the larger single key. Everything runs at 80% of the ceiling for retry,
// reconcile and burst headroom. Burst is capped so a queue drain cannot spike.
// Owner 2026-09-30: start at the dashboard rate, keep soft ceilings well above
// it (keys may be upgraded) and let per-key AIMD find the real limit: a 429
// halves the key, clean responses grow it. Better an occasional 429 than
// starved realtime detection.
const HEADROOM = 0.8;
// Owner 2026-09-30 (final): ceilings must match the dashboard, 90% headroom,
// and the two keys are NOT added together (account quota may be shared).
const DEFAULT_KEY_CAPS = [4 * 0.9, 2 * 0.9];
const DEFAULT_KEY_START = [4 * 0.9, 2 * 0.9];
const WINDOW_MS = 60000;

class ReadDispatcher {
  constructor({ keys, maxRps = 4, maxActive = 8, globalRps = 4 * 0.9, keyCaps = DEFAULT_KEY_CAPS,
    defaultCap = 2 * 0.9, keyStart = DEFAULT_KEY_START, globalBurst = 1, now = () => Number(process.hrtime.bigint()) / 1e6 } = {}) {
    this.keys = keys; this.maxRps = maxRps; this.maxActive = maxActive; this.now = now;
    this.globalRps = globalRps; this.keyCaps = keyCaps; this.defaultCap = defaultCap; this.keyStart = keyStart;
    this.globalBurst = globalBurst;
    this.global = { tokens: globalBurst > 2 ? globalBurst : 1, at: now() };
    this.states = new Map(); this.queue = []; this.active = 0; this.timer = null;
    this.grantLog = []; this.limitLog = []; this.waits = []; this.rtWaits = [];
  }
  capFor(key) {
    const idx = this.keys.pool ? this.keys.pool.findIndex(e => e.key === key) : -1;
    const cap = idx >= 0 && idx < this.keyCaps.length ? this.keyCaps[idx] : this.defaultCap;
    return Math.min(this.maxRps, cap);
  }
  state(key) {
    let s = this.states.get(key);
    const cap = this.capFor(key);
    if (!s) { const idx = this.keys.pool ? this.keys.pool.findIndex(e => e.key === key) : -1; s = { rps: Math.min(cap, idx >= 0 && idx < this.keyStart.length ? this.keyStart[idx] : 2), tokens: 1, bgTokens: 1, at: this.now(), active: 0, background: 0, until: 0, good: 0, changed: this.now(), grants: 0, limited: 0 }; this.states.set(key, s); }
    if (s.rps > cap) s.rps = cap;
    const at = this.now(), dt = Math.max(0, at - s.at) / 1000; s.at = at;
    s.tokens = Math.min(1.5, s.tokens + dt * s.rps);
    s.bgTokens = Math.min(1, s.bgTokens + dt * Math.min(1, s.rps / 2));
    return s;
  }
  refillGlobal() {
    const at = this.now(), dt = Math.max(0, at - this.global.at) / 1000;
    this.global.at = at;
    this.global.tokens = Math.min(this.globalBurst, this.global.tokens + dt * this.globalRps);
    return this.global;
  }
  acquire({ priority = 0, signal, apiKey = null } = {}) {
    return new Promise((resolve, reject) => {
      const job = { priority, signal, apiKey, at: this.now(), resolve, reject };
      job.abort = () => { const i = this.queue.indexOf(job); if (i >= 0) this.queue.splice(i, 1); reject(Object.assign(new Error("Read cancelled"), { name: "AbortError" })); };
      if (signal?.aborted) return job.abort();
      signal?.addEventListener("abort", job.abort, { once: true });
      this.queue.push(job); this.pump();
    });
  }
  pump() {
    clearTimeout(this.timer); this.timer = null;
    this.queue.sort((a,b) => a.priority - b.priority || a.at - b.at);
    while (this.active < this.maxActive && this.queue.length) {
      // One global bucket across all keys: the shared-pool ceiling.
      if (this.refillGlobal().tokens < 1) break;
      let selected = null;
      const criticalQueued = this.queue.some(j => j.priority <= 0);
      for (const job of this.queue) {
        // Background = recovery/metadata classes (P1+). Degraded polling
        // (0 < priority < 1) is realtime detection and not background-capped,
        // but still sorts behind P0/INITIAL authority reads.
        const background = job.priority >= 1;
        // Key order is a preference, never a partition: P0 starts with the
        // primary (strong) key; Add/prewarm/metadata starts with the secondary
        // key. Capacity/cooldown checks below allow either key to cover the
        // other immediately.
        const configured = this.keys.pool.map(e => e.key);
        const candidates = job.apiKey !== null ? [job.apiKey]
          : background && configured.length > 1
            ? [configured[1], configured[0], ...configured.slice(2)]
            : configured;
        for (const key of candidates) {
          const s = this.state(key), health = this.keys.find(key);
          if (s.until > this.now() || (health && health.blockedUntil > Date.now())) continue;
          if (s.active >= 4 || s.tokens < 1) continue;
          // Background cannot occupy the last request slot or consume P0's reserve.
          // Background keeps one slot free for P0 and, while a P0 job is waiting,
          // holds itself to one request per key. With nothing critical queued
          // (Add NFT of 100 links, prewarm) it may use two per key.
          const bgCap = criticalQueued ? 1 : 2;
          if (background && (s.background >= bgCap || s.active >= 3 || s.bgTokens < 1)) continue;
          if (!selected || s.active < selected.s.active) selected = { job, key, s, background };
        }
        if (selected) break;
        if (!candidates.length) {
          this.queue.splice(this.queue.indexOf(job),1); job.signal?.removeEventListener("abort",job.abort); job.reject(new Error("Missing OpenSea API key"));
          return this.pump();
        }
      }
      if (!selected) break;
      const { job, key, s, background } = selected;
      this.queue.splice(this.queue.indexOf(job),1); job.signal?.removeEventListener("abort",job.abort);
      const at = this.now();
      this.global.tokens--;
      this.granted=(this.granted||0)+1; s.grants++; s.tokens--; if(background) { s.bgTokens--; s.background++; } s.active++; this.active++;
      this.grantLog.push(at); this.waits.push(at - job.at); if (this.waits.length > 400) this.waits.splice(0, this.waits.length - 400);
      if (!background) { this.rtWaits.push(at - job.at); if (this.rtWaits.length > 400) this.rtWaits.splice(0, this.rtWaits.length - 400); }
      this.trim(at);
      const lease = this.keys.leaseKey(key, { background }); let released = false;
      job.resolve({ key, release: () => { if(released)return;released=true;lease.release();s.active--;if(background)s.background--;this.active--;this.pump(); } });
    }
    if(this.queue.length) { this.timer=setTimeout(()=>this.pump(),50); this.timer.unref?.(); }
  }
  trim(at = this.now()) {
    while (this.grantLog.length && at - this.grantLog[0] > WINDOW_MS) this.grantLog.shift();
    while (this.limitLog.length && at - this.limitLog[0] > WINDOW_MS) this.limitLog.shift();
  }
  report(key, status, waitMs = 0) {
    const s=this.state(key), now=this.now();
    // Status 0 (timeout, network error, aborted read) is not evidence about
    // the quota: halving on it decayed live rates with zero 429s (1.25.31).
    if(status===429 || status>=500) {
      if (status === 429) { s.limited++; this.limitLog.push(now); this.rateLimited = (this.rateLimited || 0) + 1; }
      s.rps=Math.max(.25,s.rps*.5);s.good=0;s.changed=now;
      s.until=Math.max(s.until,now+Math.max(status===429?2000:1000,waitMs));s.tokens=0;s.bgTokens=0;
    } else if(status>=200&&status<300) {
      s.good++;if(s.good>=20&&now-s.changed>=30000){s.rps=Math.min(this.capFor(key),s.rps+.25);s.changed=now;s.good=0;}
    }
  }
  /** Cheap signal for callers that should yield first (cold polling). */
  pressure() {
    const queuedCritical = this.queue.filter(j => j.priority <= 0).length;
    return { queued: this.queue.length, queuedCritical, globalTokens: this.refillGlobal().tokens };
  }
  stats() {
    const at = this.now(); this.trim(at);
    const queued = this.queue.length;
    const p0 = this.queue.filter(j => j.priority <= 0).length;
    const waits = [...this.waits].sort((a, b) => a - b);
    const pct = q => waits.length ? Math.round(waits[Math.min(waits.length - 1, Math.floor(waits.length * q))]) : 0;
    return { queued, queuedCritical: p0, queuedBackground: this.queue.filter(j => j.priority >= 1).length, active: this.active, granted: this.granted || 0,
      global: { ceilingRps: this.globalRps, used60s: this.grantLog.length, usedRps60s: +(this.grantLog.length / 60).toFixed(2),
        utilization: +(this.grantLog.length / 60 / this.globalRps).toFixed(2), rateLimited60s: this.limitLog.length, rateLimitedTotal: this.rateLimited || 0 },
      waitMs: { p50: pct(0.5), p95: pct(0.95), max: waits.length ? Math.round(waits[waits.length - 1]) : 0 },
      realtimeWaitMs: (() => { const w = [...this.rtWaits].sort((a, b) => a - b); const q = x => w.length ? Math.round(w[Math.min(w.length - 1, Math.floor(w.length * x))]) : 0; return { p50: q(0.5), p95: q(0.95), max: w.length ? Math.round(w[w.length - 1]) : 0 }; })(),
      keys: [...this.states.entries()].map(([key, s]) => ({
        ...(this.keys.describe ? this.keys.describe(key) : {}),
        rps: s.rps, capRps: this.capFor(key), grants: s.grants, rateLimited: s.limited,
        active: s.active, background: s.background, cooldownMs: Math.max(0, s.until - this.now())
      })) };
  }
}
module.exports={ReadDispatcher, HEADROOM, DEFAULT_KEY_CAPS, DEFAULT_KEY_START};
