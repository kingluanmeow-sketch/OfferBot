"use strict";

// Allowlisted production trace. Raw SDK/API objects are never accepted, and
// buffered async writes keep disk I/O off the trading hot path.
const fs = require("fs");
const path = require("path");
const MAX_FILE_BYTES = 8 * 1024 * 1024;
const MAX_QUEUE = 20000;
const MAX_BATCH = 256;
const EVENTS = new Set(["item_received_bid", "collection_offer", "trait_offer", "item_cancelled", "order_invalidate", "order_revalidate"]);
const STAGES = new Set(["stream_rx", "stream_mapped", "book_update", "decision", "intent", "blocked", "own_state", "own_update", "send_queued", "http_start", "submit_success", "submit_failure", "rest_request", "rest_response", "bootstrap"]);
const REASONS = new Set(["SEND", "ON_TOP", "ABOVE_MAX", "BAD_CONFIG", "NO_TARGET", "topic-not-ready", "shadow-authority-required", "low-balance", "first-read", "template", "own-state-unknown", "not-applied", "applied", "http", "stale-before-post", "quota", "build", "cancelled", "other"]);
const STATUSES = new Set(["mapped", "scope-event", "decode-failed", "applied", "not-applied", "covered", "uncovered", "SEND", "ON_TOP", "ABOVE_MAX", "BAD_CONFIG", "NO_TARGET", "READY", "WAITING", "QUEUED", "POST", "SUCCESS", "FAILED"]);
const SOURCES = new Set(["STREAM", "DEGRADED_REST", "REST"]);
let filePath = "", queue = [], scheduled = false, flushing = false, dropped = 0;
const cleanSlug = value => { const s = String(value || "").toLowerCase(); return /^[a-z0-9][a-z0-9-]{0,99}$/.test(s) ? s : ""; };
const cleanToken = value => { const s = String(value == null ? "" : value); return /^\d{1,100}$/.test(s) ? s : ""; };
const cleanNumber = value => { const n = Number(value); return Number.isFinite(n) && n >= 0 ? n : null; };
function configure(userDataPath) {
  if (typeof userDataPath !== "string" || !userDataPath) return false;
  filePath = path.join(userDataPath, "offerbot-production-trace.jsonl");
  return true;
}
function record(stage, event, detail = {}) {
  if (!filePath || !STAGES.has(stage) || queue.length >= MAX_QUEUE) { if (filePath && queue.length >= MAX_QUEUE) dropped++; return false; }
  const rawId = String(event && event.correlationId || detail.correlationId || "");
  const correlationId = /^(?:s\d+-\d+|[A-Za-z]+:\d+)$/.test(rawId) ? rawId : "";
  const eventName = String(event && event.event || detail.event || "");
  const row = {
    ts: Date.now(), monoMs: Number(process.hrtime.bigint()) / 1e6, stage, correlationId,
    chain: /^[a-zA-Z0-9_-]{1,20}$/.test(String(detail.chain || "")) ? String(detail.chain) : "", event: EVENTS.has(eventName) ? eventName : "",
    collection: cleanSlug(detail.collection || event && event.collectionSlug),
    tokenId: cleanToken(detail.tokenId || event && (event.tokenId || event.nft && event.nft.tokenId)),
    status: STATUSES.has(detail.status) ? detail.status : "", reason: REASONS.has(detail.reason) ? detail.reason : "",
    best: cleanNumber(detail.best), mine: cleanNumber(detail.mine), step: cleanNumber(detail.step),
    previousBest: cleanNumber(detail.previousBest),
    configuredStep: cleanNumber(detail.configuredStep),
    max: cleanNumber(detail.max), target: cleanNumber(detail.target),
    ownAuthoritative: typeof detail.ownAuthoritative === "boolean" ? detail.ownAuthoritative : null,
    ownSyncPending: typeof detail.ownSyncPending === "boolean" ? detail.ownSyncPending : null,
    ownSyncAt: cleanNumber(detail.ownSyncAt), ownReconciledAt: cleanNumber(detail.ownReconciledAt),
    ownKnownAt: cleanNumber(detail.ownKnownAt), ownUnknownAt: cleanNumber(detail.ownUnknownAt),
    ownReadOwned: typeof detail.ownReadOwned === "boolean" ? detail.ownReadOwned : null,
    affected: Number.isInteger(detail.affected) && detail.affected >= 0 ? detail.affected : null,
    eventTimestamp: cleanNumber(event && event.eventTimestamp),
    source: SOURCES.has(detail.source) ? detail.source : "",
    tTotal: cleanNumber(detail.tTotal), tTemplate: cleanNumber(detail.tTemplate), tFirstRead: cleanNumber(detail.tFirstRead),
    tDecision: cleanNumber(detail.tDecision), tQueued: cleanNumber(detail.tQueued)
  };
  queue.push(JSON.stringify(row) + "\n"); schedule(); return true;
}
function schedule() { if (scheduled || flushing || !queue.length) return; scheduled = true; setImmediate(() => { scheduled = false; void flush(); }); }
async function flush() {
  if (flushing || !filePath || !queue.length) return;
  flushing = true; const batch = queue.splice(0, MAX_BATCH).join("");
  try {
    await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
    let stat = null; try { stat = await fs.promises.stat(filePath); } catch {}
    if (stat && stat.size + Buffer.byteLength(batch) > MAX_FILE_BYTES) {
      try { await fs.promises.unlink(filePath + ".1"); } catch {}
      try { await fs.promises.rename(filePath, filePath + ".1"); } catch {}
    }
    await fs.promises.appendFile(filePath, batch, { encoding: "utf8", mode: 0o600 });
  } catch { dropped++; }
  finally { flushing = false; if (queue.length) schedule(); }
}
function stats() { return { enabled: Boolean(filePath), queued: queue.length, dropped }; }
module.exports = { configure, record, flush, stats, MAX_FILE_BYTES, MAX_QUEUE };
