"use strict";

const crypto = require("crypto");

/** Stable identity for one user-visible Add batch; contains no credential. */
function batchSignature(payload = {}) {
  const links = String(payload.links || "")
    .split(/\r?\n/)
    .map(line => line.trim())
    .filter(Boolean)
    .join("\n");
  const config = payload.config || {};
  const canonical = JSON.stringify({
    chain: String(payload.chain || "").toLowerCase(),
    links,
    config: {
      minPrice: String(config.minPrice || ""),
      maxPrice: String(config.maxPrice || ""),
      step: String(config.step || ""),
      duration: String(config.duration || "")
    }
  });
  return crypto.createHash("sha256").update(canonical).digest("hex");
}

/**
 * Exactly-once gate for expensive Add NFT batches.
 *
 * requestId handles IPC retries. Signature handles two renderer callbacks that
 * raced before either could disable the button. Only active signatures merge;
 * a later deliberate click still executes and gets the ordinary "already in
 * list" result.
 */
class AddBatchGate {
  constructor({ retainMs = 5 * 60 * 1000 } = {}) {
    this.retainMs = retainMs;
    this.requests = new Map();
    this.active = new Map();
  }

  run(payload, execute) {
    const requestId = String(payload?.requestId || "").trim();
    const signature = batchSignature(payload);
    const byId = requestId && this.requests.get(requestId);
    if (byId) return byId.then(markReplay);

    const active = this.active.get(signature);
    if (active) {
      if (requestId) this.remember(requestId, active);
      return active.then(markReplay);
    }

    const job = Promise.resolve().then(execute);
    this.active.set(signature, job);
    if (requestId) this.remember(requestId, job);
    job.finally(() => {
      if (this.active.get(signature) === job) this.active.delete(signature);
    }).catch(() => {});
    return job;
  }

  remember(requestId, job) {
    this.requests.set(requestId, job);
    const timer = setTimeout(() => {
      if (this.requests.get(requestId) === job) this.requests.delete(requestId);
    }, this.retainMs);
    timer.unref?.();
  }
}

function markReplay(result) {
  return { ...(result || {}), ok: result?.ok !== false, idempotentReplay: true };
}

module.exports = { AddBatchGate, batchSignature };
