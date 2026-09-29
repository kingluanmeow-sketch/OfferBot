"use strict";

/**
 * Ring-buffer logger with automatic cleanup.
 *
 * A busy session produces far more log than anyone reads, and the volume is
 * what makes the Logs tab lag. So:
 *
 *   - ordinary lines are capped hard and the oldest are dropped
 *   - ERROR lines are DEDUPLICATED: one entry per distinct problem, with a
 *     repeat counter, so a row failing every 10 seconds does not bury
 *     everything else
 *   - errors survive the pruning of ordinary lines; they are the part worth
 *     keeping
 *   - clear() still wipes everything, for the manual button
 *
 * Sinks receive an object, not a bare string, so the renderer can update a
 * repeated error in place instead of appending a new node.
 */

/** Ordinary (non-error) lines kept in the buffer. */
const MAX_LINES = 1000;

/** Distinct error signatures kept. Beyond this the oldest signature is dropped. */
const MAX_ERROR_SIGNATURES = 120;

const TAGS = Object.freeze({
  STREAM: "STREAM",
  STREAM_PRIORITY: "STREAM PRIORITY",
  SCAN: "SCAN",
  BEST: "BEST",
  MY_OFFER: "MY OFFER",
  DECISION: "DECISION",
  SEND: "SEND",
  ON_TOP: "ON TOP",
  OUTBID: "OUTBID",
  LOW_BALANCE: "LOW BALANCE",
  CANCEL: "CANCEL",
  ENGINE: "ENGINE",
  API: "API",
  LICENSE: "LICENSE",
  ERROR: "ERROR"
});

/**
 * Collapse the variable parts of a message so two occurrences of the same
 * problem share one signature: prices, hashes, addresses, ids and durations all
 * differ run to run while describing the identical failure.
 */
function errorSignature(tag, body) {
  return `${tag}|${String(body)
    .replace(/0x[a-fA-F0-9]{6,}/g, "0x*")
    .replace(/\d+(\.\d+)?/g, "#")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 160)}`;
}

class Logger {
  constructor(maxLines = MAX_LINES) {
    this.maxLines = maxLines;
    this.lines = [];
    this.sinks = new Set();

    /** signature -> { id, tag, body, line, count, firstAt, lastAt } */
    this.errors = new Map();

    this.nextId = 1;
    this.dropped = 0;
  }

  subscribe(fn) {
    if (typeof fn !== "function") return () => {};
    this.sinks.add(fn);
    return () => this.sinks.delete(fn);
  }

  emit(entry) {
    for (const sink of this.sinks) {
      try {
        sink(entry);
      } catch {
        /* a broken sink must never kill the caller */
      }
    }
  }

  log(tag, ...parts) {
    const stamp = new Date().toLocaleTimeString("en-GB", { hour12: false });
    const body = parts
      .map(p => (typeof p === "string" ? p : safeStringify(p)))
      .join(" ");

    const isError = tag === TAGS.ERROR;

    // ---- errors: one entry per distinct problem --------------------
    if (isError) {
      const signature = errorSignature(tag, body);
      const existing = this.errors.get(signature);

      if (existing) {
        existing.count++;
        existing.lastAt = Date.now();
        existing.body = body;
        existing.line = `${stamp} [${tag}] ${body}  (x${existing.count})`;

        // Move to the end so the newest problem is last.
        this.errors.delete(signature);
        this.errors.set(signature, existing);

        this.emit({
          id: existing.id,
          kind: "error",
          tag,
          line: existing.line,
          count: existing.count,
          replace: true
        });
        return existing.line;
      }

      const entry = {
        id: `e${this.nextId++}`,
        tag,
        body,
        line: `${stamp} [${tag}] ${body}`,
        count: 1,
        firstAt: Date.now(),
        lastAt: Date.now()
      };

      this.errors.set(signature, entry);

      // Cap the number of distinct signatures.
      while (this.errors.size > MAX_ERROR_SIGNATURES) {
        const oldest = this.errors.keys().next();
        if (oldest.done) break;
        const removed = this.errors.get(oldest.value);
        this.errors.delete(oldest.value);
        this.emit({ id: removed.id, kind: "error", remove: true });
      }

      this.emit({
        id: entry.id,
        kind: "error",
        tag,
        line: entry.line,
        count: 1,
        replace: false
      });
      return entry.line;
    }

    // ---- ordinary lines: plain ring buffer -------------------------
    const entry = {
      id: `l${this.nextId++}`,
      kind: "line",
      tag,
      line: `${stamp} [${tag}] ${body}`
    };

    this.lines.push(entry);

    if (this.lines.length > this.maxLines) {
      const excess = this.lines.length - this.maxLines;
      this.lines.splice(0, excess);
      this.dropped += excess;
    }

    this.emit(entry);
    return entry.line;
  }

  stream(...p) { return this.log(TAGS.STREAM, ...p); }
  streamPriority(...p) { return this.log(TAGS.STREAM_PRIORITY, ...p); }
  scan(...p) { return this.log(TAGS.SCAN, ...p); }
  best(...p) { return this.log(TAGS.BEST, ...p); }
  myOffer(...p) { return this.log(TAGS.MY_OFFER, ...p); }
  decision(...p) { return this.log(TAGS.DECISION, ...p); }
  send(...p) { return this.log(TAGS.SEND, ...p); }
  onTop(...p) { return this.log(TAGS.ON_TOP, ...p); }
  outbid(...p) { return this.log(TAGS.OUTBID, ...p); }
  lowBalance(...p) { return this.log(TAGS.LOW_BALANCE, ...p); }
  cancel(...p) { return this.log(TAGS.CANCEL, ...p); }
  engine(...p) { return this.log(TAGS.ENGINE, ...p); }
  api(...p) { return this.log(TAGS.API, ...p); }
  license(...p) { return this.log(TAGS.LICENSE, ...p); }
  error(...p) { return this.log(TAGS.ERROR, ...p); }

  /**
   * Snapshot for a renderer that just mounted the Logs tab: the kept errors
   * first, then the recent ordinary lines.
   */
  history() {
    const errors = Array.from(this.errors.values()).map(e => ({
      id: e.id,
      kind: "error",
      tag: e.tag,
      line: e.line,
      count: e.count
    }));
    return [...errors, ...this.lines];
  }

  /**
   * Trim ordinary lines back to `keep`. Errors are never dropped here - they
   * are the part worth keeping.
   */
  autoClean(keep = 300) {
    if (this.lines.length <= keep) return 0;
    const removed = this.lines.length - keep;
    this.lines.splice(0, removed);
    this.dropped += removed;
    this.emit({ kind: "prune", keep });
    return removed;
  }

  /** Manual CLEAR: wipe everything, errors included. */
  clear() {
    this.lines.length = 0;
    this.errors.clear();
    this.dropped = 0;
    this.emit({ kind: "clear" });
  }

  stats() {
    return {
      lines: this.lines.length,
      errors: this.errors.size,
      errorOccurrences: Array.from(this.errors.values()).reduce(
        (sum, e) => sum + e.count,
        0
      ),
      dropped: this.dropped,
      maxLines: this.maxLines
    };
  }
}

function safeStringify(value) {
  if (value === null || value === undefined) return String(value);
  if (value instanceof Error) return value.message || String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

const logger = new Logger();

module.exports = { logger, Logger, TAGS, errorSignature, MAX_ERROR_SIGNATURES };
