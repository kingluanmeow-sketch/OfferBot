"use strict";

// Only actual HTTP requests acquire a lease. Chains share a key's server quota,
// but independent keys have independent AIMD state. No key material in stats.
class ReadDispatcher {
  constructor({ keys, maxRps = 4, maxActive = 8, now = () => Number(process.hrtime.bigint()) / 1e6 } = {}) {
    this.keys = keys; this.maxRps = maxRps; this.maxActive = maxActive; this.now = now;
    this.states = new Map(); this.queue = []; this.active = 0; this.timer = null;
  }
  state(key) {
    let s = this.states.get(key);
    if (!s) { s = { rps: Math.min(2, this.maxRps), tokens: 1, bgTokens: 1, at: this.now(), active: 0, background: 0, until: 0, good: 0, changed: this.now() }; this.states.set(key, s); }
    const at = this.now(), dt = Math.max(0, at - s.at) / 1000; s.at = at;
    s.tokens = Math.min(2, s.tokens + dt * s.rps);
    s.bgTokens = Math.min(1, s.bgTokens + dt * Math.min(1, s.rps / 2));
    return s;
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
      let selected = null;
      const criticalQueued = this.queue.some(j => j.priority <= 0);
      for (const job of this.queue) {
        const background = job.priority > 0;
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
      this.granted=(this.granted||0)+1; s.tokens--; if(background) { s.bgTokens--; s.background++; } s.active++; this.active++;
      const lease = this.keys.leaseKey(key, { background }); let released = false;
      job.resolve({ key, release: () => { if(released)return;released=true;lease.release();s.active--;if(background)s.background--;this.active--;this.pump(); } });
    }
    if(this.queue.length) { this.timer=setTimeout(()=>this.pump(),50); this.timer.unref?.(); }
  }
  report(key, status, waitMs = 0) {
    const s=this.state(key), now=this.now();
    if(status===429 || status>=500 || status===0) {
      s.rps=Math.max(.25,s.rps*.5);s.good=0;s.changed=now;
      s.until=Math.max(s.until,now+Math.max(status===429?2000:1000,waitMs));s.tokens=0;s.bgTokens=0;
    } else if(status>=200&&status<300) {
      s.good++;if(s.good>=20&&now-s.changed>=30000){s.rps=Math.min(this.maxRps,s.rps+.25);s.changed=now;s.good=0;}
    }
  }
  stats() {
    const queued = this.queue.length;
    const p0 = this.queue.filter(j => j.priority <= 0).length;
    return { queued, queuedCritical: p0, queuedBackground: queued - p0, active: this.active, granted: this.granted || 0,
      keys: [...this.states.entries()].map(([key, s]) => ({
        ...(this.keys.describe ? this.keys.describe(key) : {}),
        rps: s.rps, active: s.active, background: s.background, cooldownMs: Math.max(0, s.until - this.now())
      })) };
  }
}
module.exports={ReadDispatcher};
