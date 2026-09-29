"use strict";

/**
 * latency.js — a stopwatch for one NFT's trip from stream event to submitted
 * offer. It MEASURES ONLY: nothing here changes what the bot decides or sends.
 *
 * One trace per reaction, stamped at each stage:
 *
 *   event_received → event_parsed → local_state_updated → best_calculated
 *   → decision_started → decision_finished → submit_started → submit_finished
 *
 * A trace that reaches a decision prints one line naming which path it took and
 * where its milliseconds went, so a slow reaction points at its own cause
 * instead of inviting a guess.
 */

const { logger } = require("./logger");

/**
 * One monotonic clock for every timestamp in this module.
 *
 * `Date.now()` can step backwards (NTP correction, sleep/resume) and mixing it
 * with a timestamp minted elsewhere is how a span goes negative.
 * `performance.now()` only moves forward.
 */
const now = () => Math.round(performance.now());

/** Stages in the order they should happen. */
const STAGES = [
  "event_received",
  "event_parsed",
  "local_state_updated",
  "best_calculated",
  "decision_started",
  "decision_finished",
  "submit_started",
  "submit_finished"
];

/**
 * Sub-stages inside the coarse ones, so a slow span names its own cause.
 *
 * `state→best` sitting at 4s on a path that is only a Map lookup means the row
 * was WAITING, not computing - these say what for. Same for a 13s submit, which
 * is dominated by something other than the HTTP call itself.
 */
const SUB_STAGES = [
  // between local_state_updated and best_calculated
  "priority_assigned",
  "debounce_fired",
  "rescan_queued",
  "submit_lock_wait",
  "queued_for_scan",
  "scan_started",
  "book_lookup_started",
  "book_lookup_done",
  "rest_queued",
  "rest_started",
  "rest_finished",

  // between decision_finished and submit_finished
  "submit_queued",
  "lane_queued",
  "lane_acquired",
  "submit_slot_queued",
  "submit_slot_acquired",
  "submit_slot_released",
  "verify_started",
  "verify_finished",
  "sign_started",
  "sign_finished",
  "order_queue_started",
  "order_queue_finished",
  "http_started",
  "http_finished",
  "ratelimit_wait_started",
  "ratelimit_wait_finished"
];

/** Gaps reported on the summary line, as [label, from, to]. */
const SPANS = [
  ["stream→parse", "event_received", "event_parsed"],
  ["parse→state", "event_parsed", "local_state_updated"],
  ["state→best", "local_state_updated", "best_calculated"],
  ["best→decision", "best_calculated", "decision_finished"],
  ["decision→submit", "decision_finished", "submit_started"],
  ["submit duration", "submit_started", "submit_finished"]
];

/** The breakdown printed under a slow line, as [label, from, to]. */
const DETAIL_SPANS = [
  // From the moment the row was ranked to the moment a worker took it. This is
  // the scheduler's own delay, and the number that says whether an urgent row
  // is genuinely being served first or merely labelled that way.
  ["  ├ priority wait", "priority_assigned", "scan_started"],
  ["  ├ debounce", "local_state_updated", "debounce_fired"],
  ["  ├ queue wait", "debounce_fired", "scan_started"],
  // Time the row spent HOLDING NEWS IT MAY NOT ACT ON YET, because its Scan
  // The "cadence wait" line was here. Nothing emits cadence_deferred any
  // more - a row with news is eligible immediately - so the line could only
  // ever report 0, and a report that names a wait nobody can incur is worse
  // than one that stays quiet about it.
  ["  ├ book lookup", "book_lookup_started", "book_lookup_done"],
  // Waiting for a REST worker is not the same as the read itself: one is
  // scheduler pressure, the other is OpenSea. They are reported apart.
  ["  ├ REST queue", "rest_queued", "rest_started"],
  ["  ├ REST", "rest_started", "rest_finished"],
  ["  ├ submit queue", "decision_finished", "submit_queued"],
  // The wait for a send slot, measured on the slot itself. It used to run
  // from `submit_queued`, which meant it also counted the verify read, the
  // lane, signing and the whole ORDER queue - so "slot wait p90 13s" was
  // mostly time no slot was involved in at all.
  ["  ├ slot wait", "submit_slot_queued", "submit_slot_acquired"],
  // What a slot is actually held for: the POST, and nothing before it.
  ["  ├ slot hold", "submit_slot_acquired", "submit_slot_released"],
  ["  ├ verify", "verify_started", "verify_finished"],
  // seaport's own Semaphore(1): every offer in the process passes it one by
  // one, so under load this is where a submit actually spends its time.
  ["  ├ lane wait", "lane_queued", "lane_acquired"],
  // The wait for OpenSea write capacity, kept apart from the request itself.
  // "HTTP" used to enclose this, so a submit that never reached the network
  // still reported a 90-second HTTP.
  ["  ├ order queue wait", "order_queue_started", "order_queue_finished"],
  ["  ├ rate-limit wait", "ratelimit_wait_started", "ratelimit_wait_finished"],
  ["  ├ signing", "sign_started", "sign_finished"],
  ["  └ HTTP", "http_started", "http_finished"]
];

/**
 * Stages that take ownership of a job, and the stage that gives it back.
 *
 * A job sitting at `ratelimit_wait_started` is not hung - it is queued in the
 * token bucket, exactly where it belongs, and the ORDER bucket runs at 2/s so
 * under a burst that wait is legitimately long. Killing it on age alone
 * reports a healthy job as dead and abandons a submit that was about to
 * happen.
 *
 * So each of these pairs says "someone owns this job now" / "they are done
 * with it", and the watchdog asks about ownership before it asks about age.
 *
 *   opener -> [closer, owner name]
 */
const OWNER_STAGES = {
  // A debounced reaction is waiting for a scan worker. That is a queue like
  // any other, and leaving it unowned made every reaction still waiting for a
  // worker look abandoned - which is how twenty-five healthy rows were
  // reported as orphans while they were simply next in line.
  // The debounce window itself is ownership: the event has been folded into
  // the row and a refresh is armed. Leaving it unowned made every reaction
  // still inside its debounce look abandoned.
  local_state_updated: ["scan_started", "debounce-window"],
  debounce_fired: ["scan_started", "scan-queue"],
  // A reaction deferred to its next scan is still owned by the scan queue.
  rescan_queued: ["scan_started", "scan-queue"],
  // A reaction whose row is mid-submit cannot be scheduled at all: buildQueue
  // skips a row holding the per-NFT lock. That wait is real ownership, but it
  // had no name - so the trace sat with debounce-window and scan-queue both
  // IDLE and the watchdog called it an orphan. Reproduced deterministically.
  submit_lock_wait: ["scan_started", "submit-lock"],
  rest_queued: ["rest_started", "rest-queue"],
  // From the moment a submit is queued until it actually begins, the row is
  // holding its per-NFT submit lock and nothing else. This used to name the
  // send slot, which the row does not hold for any of that window.
  submit_queued: ["submit_started", "submit-lock"],
  // The send slot proper: claimed when the row starts waiting for one and
  // given back when it has one. Both ends are the resource itself.
  submit_slot_queued: ["submit_slot_acquired", "submit-slot"],
  lane_queued: ["lane_acquired", "submit-lane"],
  sign_started: ["sign_finished", "signing"],
  ratelimit_wait_started: ["ratelimit_wait_finished", "rate-limit-queue"],
  http_started: ["http_finished", "http"]
};

/**
 * closer stage -> EVERY owner it releases.
 *
 * A list, not a single name: `scan_started` ends both the debounce window and
 * the scan-queue wait. Keying one owner per closer silently kept the last one
 * written, so `debounce-window` was added at `local_state_updated` and never
 * removed - every trace then carried a claim on a subsystem it had long since
 * left, and the watchdog reported it as an orphan whenever that subsystem
 * happened to be idle.
 */
const OWNER_CLOSERS = new Map();
for (const [closer, owner] of Object.values(OWNER_STAGES)) {
  if (!OWNER_CLOSERS.has(closer)) OWNER_CLOSERS.set(closer, []);
  OWNER_CLOSERS.get(closer).push(owner);
}

/**
 * The point past which even an owned job is treated as hung.
 *
 * Ownership answers "is anybody still holding this", not "will they ever let
 * go". A lane that deadlocks would otherwise keep its jobs alive forever, so
 * there is still a ceiling - just one far beyond any legitimate queue.
 */
const OWNED_TRACE_MAX_MS = 5 * 60 * 1000;

/** Samples kept per metric for the percentile report. */
const WINDOW = 500;

/** A trace older than this was abandoned; it must not leak. */
const TRACE_TTL_MS = 60 * 1000;

/** Traces alive at once. Bounded so a storm cannot grow memory. */
const MAX_TRACES = 500;

class Trace {
  constructor(id, label) {
    this.id = id;
    this.label = label;
    this.marks = new Map();
    this.path = "UNKNOWN";
    this.details = {};
    this.events = 0;

    /**
     * Who is currently holding this job, by name.
     *
     * Maintained by `mark` from OWNER_STAGES, so no call site has to remember
     * to declare it: a job cannot enter a queue without saying so, because
     * entering the queue IS the stage that says so.
     */
    this.owners = new Set();

    /** The last stage recorded, in real order - not Map insertion order. */
    this.lastStage = "event_received";
    this.lastStageAt = now();

    // ONE clock for every stamp on this trace.
    //
    // Stages were previously stamped from whatever source was handy - the
    // stream's own `receivedAt`, taken on a different machine clock, mixed with
    // local `Date.now()`. Two clocks produced spans like `stream→parse:
    // -31856ms` and a TOTAL smaller than a stage inside it. Everything below
    // now reads the same monotonic source.
    this.createdAt = Date.now();
    this.origin = now();
    this.mark("event_received");
  }

  /**
   * Stamp a stage, once.
   *
   * A stage never moves after it is first recorded: a burst re-entering the
   * trace must not shift an earlier mark later than one already taken, which
   * is precisely how negative spans appeared.
   */
  mark(stage, at = now()) {
    if (!this.marks.has(stage)) this.marks.set(stage, at);

    // Ownership and "where is it now" are tracked even for a repeated stage.
    // A retry re-enters the same queue, and a stage that is not re-recorded is
    // still a stage the job genuinely passed through again - refusing to
    // update here is what would let a retrying job look abandoned.
    this.lastStage = stage;
    this.lastStageAt = at;

    const opener = OWNER_STAGES[stage];
    if (opener) this.owners.add(opener[1]);

    const closing = OWNER_CLOSERS.get(stage);
    if (closing) for (const owner of closing) this.owners.delete(owner);

    return this;
  }

  /** Is anybody still holding this job? */
  hasOwner() {
    return this.owners.size > 0;
  }

  /** Count another event folded into this same reaction. */
  bump() {
    this.events++;
    return this;
  }

  /** Record how the answer was obtained: FAST (memory) or REST_FALLBACK. */
  setPath(path, details = {}) {
    this.path = path;
    Object.assign(this.details, details);
    return this;
  }

  note(key, value) {
    this.details[key] = value;
    return this;
  }

  span(from, to) {
    const a = this.marks.get(from);
    const b = this.marks.get(to);
    if (a === undefined || b === undefined) return null;

    // Stages are stamped in order from one clock, so this cannot go negative.
    // Clamping anyway means a bug here shows up as 0, never as a nonsense
    // number that sends someone hunting the wrong problem.
    return Math.max(0, b - a);
  }

  /**
   * First stamp to last stamp. By construction TOTAL is at least as large as
   * any span inside it, because every mark shares one monotonic origin.
   */
  total() {
    const start = this.marks.get("event_received");
    if (start === undefined) return null;

    let end = start;
    for (const stage of STAGES) {
      const at = this.marks.get(stage);
      if (at !== undefined && at > end) end = at;
    }
    return Math.max(0, end - start);
  }
}

class LatencyProfiler {
  constructor({ enabled = true } = {}) {
    this.enabled = enabled;
    this.traces = new Map();
    this.samples = new Map();
    this.counts = { FAST: 0, FAST_FLOOR: 0, REST_FALLBACK: 0, UNKNOWN: 0, completed: 0, abandoned: 0 };

    /** reason -> how many traces died that way. */
    this.abandonReasons = new Map();

    /**
     * Job accounting, so `created === finalized + active` can be checked
     * rather than assumed. A job that is neither finalized nor active is
     * precisely the leak this version exists to make impossible.
     */
    this.created = 0;
    /** result -> how many jobs finalized that way. */
    this.finalizeReasons = new Map();

    /** Set by the engine so ownership can be checked against real queues. */
    this.livenessProbe = null;

    /** Print the full breakdown for anything slower than this. */
    this.slowThresholdMs = 1000;
  }

  /** Start (or reuse) the trace for one NFT's current reaction. */
  begin(id, label = "") {
    if (!this.enabled) return null;

    const existing = this.traces.get(id);
    // A burst belongs to ONE reaction: keep the first event's clock so latency
    // is measured from when the news arrived, not from the last event before
    // the work happened.
    if (existing) return existing;

    if (this.traces.size >= MAX_TRACES) this.sweep(true);

    const trace = new Trace(id, label);
    this.traces.set(id, trace);
    this.created++;
    return trace;
  }

  /**
   * Finish a job for a reason other than a completed submit.
   *
   * One door out for every branch that is not "the offer was sent": ON TOP,
   * capped by Max, spaced out by the per-NFT interval, coalesced onto a submit
   * already running, failed, superseded. Each of those used to `return`
   * leaving the trace open, and the watchdog then reported a job that had in
   * fact finished thirty seconds earlier as never having finished at all.
   *
   * Safe to call twice: a trace that is already gone is simply not there.
   */
  finalize(id, result = "COMPLETED", details = {}) {
    const trace = this.traces.get(id);
    if (!trace) return null;

    this.finalizeReasons.set(result, (this.finalizeReasons.get(result) || 0) + 1);

    // A finished job is reported and measured exactly like any other; it just
    // did not happen to end in a POST.
    return this.end(id, { log: true, result, details });
  }

  get(id) {
    return this.enabled ? this.traces.get(id) || null : null;
  }

  mark(id, stage) {
    const trace = this.get(id);
    if (trace) trace.mark(stage);
    return trace;
  }

  /** Finish a trace: log one line and fold its spans into the percentiles. */
  end(id, { log = true, result = "", details = null } = {}) {
    const trace = this.get(id);
    if (!trace) return null;

    if (result) trace.note("result", result);
    if (details) Object.assign(trace.details, details);

    this.traces.delete(id);
    this.counts.completed++;
    this.counts[trace.path] = (this.counts[trace.path] || 0) + 1;

    const parts = [];
    for (const [name, from, to] of SPANS) {
      const ms = trace.span(from, to);
      if (ms === null) continue;
      parts.push(`${name}: ${ms}ms`);
      this.record(name, ms);
    }

    const total = trace.total();
    if (total !== null) this.record("total", total);

    // The two numbers worth watching on their own.
    const streamToDecision = trace.span("event_received", "decision_finished");
    if (streamToDecision !== null) this.record("stream→decision", streamToDecision);

    // End-to-end, measured from the news itself rather than from any internal
    // stage. These are the goal: an external bid should reach a decision in
    // well under half a second whenever the local view can answer it.
    for (const [name, to] of [
      ["event→BEST", "best_calculated"],
      ["event→DECISION", "decision_finished"],
      ["event→SUBMIT START", "submit_started"]
    ]) {
      const ms = trace.span("event_received", to);
      if (ms !== null) this.record(name, ms);
    }

    // How old the local view was when it answered. A fast decision taken from
    // a book that is minutes old is fast for the wrong reason, and this is
    // what would show it.
    if (trace.path === "FAST" || trace.path === "FAST_FLOOR") {
      const age = trace.details.bookAgeMs;
      if (Number.isFinite(age)) this.record("FAST decision age", age);
    }

    if (!log) return trace;

    const head = `[LATENCY] ${trace.label || trace.id} [PATH] ${trace.path}`;
    const detail = Object.entries(trace.details)
      .map(([k, v]) => `${k}=${v}`)
      .join(" ");

    logger.send(
      `${head} ${parts.join(" · ")}` +
        (total !== null ? ` · TOTAL: ${total}ms` : "") +
        (detail ? ` · ${detail}` : "")
    );

    // A P0 reaction is reported in full, every time, whether it was fast or
    // slow. The whole claim being made about this pipeline is that urgent work
    // never queues behind background work - so the evidence for or against
    // that has to be on the record for every P0, not only the ones that
    // happened to cross a threshold.
    if (trace.details.P === 0) this.reportP0(trace, total);

    // A slow reaction gets its sub-stages spelled out, so the coarse span that
    // looks bad immediately names which wait inside it was responsible.
    if (total !== null && total >= this.slowThresholdMs) {
      for (const [label, from, to] of DETAIL_SPANS) {
        const ms = trace.span(from, to);
        if (ms === null) continue;
        this.record(label.trim().replace(/^[├└]\s*/, ""), ms);
        if (ms >= 1) logger.send(`${label}: ${ms}ms`);
      }

      // Anything stamped but not covered above, so nothing hides.
      const covered = new Set(
        DETAIL_SPANS.flatMap(([, from, to]) => [from, to]).concat(STAGES)
      );
      const extra = [...trace.marks.keys()].filter(stage => !covered.has(stage));
      if (extra.length) logger.send(`  · other marks: ${extra.join(", ")}`);
    }

    return trace;
  }

  /**
   * One line naming every layer a P0 reaction passed through, and what each
   * one cost it.
   *
   * The point is to make "P0 was blocked" a readable fact rather than an
   * inference. Each stage below is a queue an urgent event can get stuck in,
   * and they are printed in the order it meets them - so the first big number
   * IS the culprit, with no arithmetic needed.
   */
  reportP0(trace, total) {
    const stages = [
      ["event→priority", "event_received", "priority_assigned"],
      ["priority wait", "priority_assigned", "scan_started"],
      ["scan queue", "debounce_fired", "scan_started"],
      ["BEST", "scan_started", "best_calculated"],
      ["REST queue", "rest_queued", "rest_started"],
      ["REST", "rest_started", "rest_finished"],
      ["decision", "best_calculated", "decision_finished"],
      ["submit queue", "decision_finished", "submit_started"],
      ["slot wait", "submit_slot_queued", "submit_slot_acquired"],
      ["API capacity", "lane_queued", "lane_acquired"],
      ["order queue", "order_queue_started", "order_queue_finished"],
      ["rate-limit", "ratelimit_wait_started", "ratelimit_wait_finished"],
      ["HTTP", "http_started", "http_finished"]
    ];

    const parts = [];
    for (const [label, from, to] of stages) {
      const ms = trace.span(from, to);
      if (ms === null) continue;
      parts.push(`${label}=${ms}ms`);
      this.record(`P0 ${label}`, ms);
    }

    if (total !== null) this.record("P0 total", total);

    logger.send(
      `[P0 LATENCY] ${trace.label || trace.id} ${parts.join(" ")}` +
        (total !== null ? ` TOTAL=${total}ms` : "")
    );
  }

  /**
   * Drop a trace that will never finish, and say why.
   *
   * Half the traces disappearing silently is itself a finding: it means work
   * was started for an NFT and then dropped, so the reason is recorded and
   * counted rather than swallowed.
   */
  abandon(id, reason = "unknown", details = {}) {
    const trace = this.traces.get(id);
    if (!trace) return null;

    this.traces.delete(id);
    this.counts.abandoned++;
    this.abandonReasons.set(reason, (this.abandonReasons.get(reason) || 0) + 1);

    const lastStage = [...trace.marks.keys()].pop() || "event_received";

    logger.send(
      `[ABANDON] ${trace.label || id} reason=${reason} ` +
        `lastStage=${lastStage} age=${now() - trace.origin}ms ` +
        `events=${trace.events} path=${trace.path}` +
        (Object.keys(details).length
          ? " " + Object.entries(details).map(([k, v]) => `${k}=${v}`).join(" ")
          : "")
    );

    return trace;
  }

  record(metric, ms) {
    if (!Number.isFinite(ms) || ms < 0) return;
    let list = this.samples.get(metric);
    if (!list) {
      list = [];
      this.samples.set(metric, list);
    }
    list.push(ms);
    if (list.length > WINDOW) list.shift();
  }

  /**
   * Ask whether an owner really is still holding work.
   *
   * The engine registers this, because only the engine can see the actual
   * queues. Without it "owned" would mean "claimed ownership at some point",
   * which a crashed or orphaned entry could claim forever.
   *
   * @param {(owner:string)=>boolean} fn
   */
  setLivenessProbe(fn) {
    this.livenessProbe = typeof fn === "function" ? fn : null;
  }

  /**
   * Is this job legitimately waiting, or actually hung?
   *
   * @returns {{live:boolean, owner:string, reason:string}}
   */
  inspect(trace, at = Date.now()) {
    if (!trace.hasOwner()) {
      return { live: false, owner: "", reason: "no-owner" };
    }

    // A job in flight holds a STACK of owners, not one: a submit inside the
    // lane, inside the HTTP call, inside the rate-limit queue. Insertion order
    // is nesting order, so the last one added is the innermost - the thing
    // actually holding it right now, and the right name to report.
    const owners = [...trace.owners];
    const owner = owners[owners.length - 1];

    // Owned, but for longer than any real queue could justify.
    if (at - trace.createdAt > OWNED_TRACE_MAX_MS) {
      return { live: false, owner, reason: "owner-never-released" };
    }

    // Do those queues actually still have anything in them? An entry dropped
    // without its closing stage would otherwise keep the job alive forever
    // while nothing was working on it.
    //
    // ANY live owner keeps the job: asking only about one of them would
    // declare a job orphaned because an outer layer had already finished, which
    // is the normal shape of a submit waiting on the write bucket.
    if (this.livenessProbe) {
      let anyAlive = false;
      for (const candidate of owners) {
        let alive;
        try {
          alive = this.livenessProbe(candidate);
        } catch {
          alive = true; // a broken probe must not cause false kills
        }
        if (alive !== false) {
          anyAlive = true;
          break;
        }
      }
      if (!anyAlive) {
        return { live: false, owner, reason: "orphaned-job" };
      }
    }

    return { live: true, owner, reason: "waiting-for-capacity" };
  }

  /**
   * Remove traces that never finished, so an abandoned reaction cannot leak.
   *
   * Age alone is not the test any more. A job queued in the token bucket is
   * exactly where it should be, and the ORDER bucket runs at 2/s - so under a
   * burst it can legitimately wait a long time. Killing it on age reported
   * dozens of healthy submits as `timed-out-never-finished` while they were
   * still about to send.
   */
  sweep(force = false) {
    const at = Date.now();
    const cutoff = at - TRACE_TTL_MS;

    for (const [id, trace] of this.traces) {
      if (force) {
        this.abandon(id, "evicted-trace-limit");
        if (this.traces.size < MAX_TRACES / 2) break;
        continue;
      }

      if (trace.createdAt >= cutoff) continue;

      const verdict = this.inspect(trace, at);

      if (verdict.live) {
        // Reported, but not killed. Once per trace, so a long legitimate wait
        // does not fill the log with the same line every tick.
        if (!trace.keptReported) {
          trace.keptReported = true;
          logger.send(
            `[WATCHDOG] ${trace.label || id} state=${trace.lastStage} ` +
              `owner=${verdict.owner} age=${at - trace.createdAt}ms action=KEEP`
          );
        }
        continue;
      }

      if (verdict.owner) {
        // The full ownership graph, not just a count.
        //
        // An orphan means a subsystem was CLAIMED but does not hold the job -
        // a dropped reference. Printing which claims exist and which of them
        // the live structures confirm points straight at the subsystem that
        // dropped it, instead of leaving "orphaned-job=4" to be guessed at.
        const graph = [...trace.owners]
          .map(owner => {
            let live = "?";
            if (this.livenessProbe) {
              try {
                live = this.livenessProbe(owner) === false ? "IDLE" : "busy";
              } catch {
                live = "probe-threw";
              }
            }
            return `${owner}:${live}`;
          })
          .join(" ");

        logger.send(
          `[WATCHDOG] ${trace.label || id} state=${trace.lastStage} ` +
            `path=${trace.path} age=${at - trace.createdAt}ms ` +
            `claims=[${graph}] reason=${verdict.reason} action=FINALIZE`
        );
      }

      this.abandon(id, verdict.reason === "no-owner"
        ? "timed-out-never-finished"
        : verdict.reason);
    }
  }

  static percentile(sorted, p) {
    if (!sorted.length) return null;
    const index = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
    return sorted[index];
  }

  report() {
    const out = {};
    for (const [metric, list] of this.samples) {
      if (!list.length) continue;
      const sorted = [...list].sort((a, b) => a - b);
      out[metric] = {
        n: list.length,
        p50: LatencyProfiler.percentile(sorted, 50),
        p90: LatencyProfiler.percentile(sorted, 90),
        p95: LatencyProfiler.percentile(sorted, 95),
        max: sorted[sorted.length - 1]
      };
    }
    const finalized = this.counts.completed + this.counts.abandoned;
    const active = this.traces.size;

    // Jobs still open but with nobody holding them. This number should be 0;
    // anything else is a job the system has lost track of, which is the whole
    // failure being fixed here.
    const orphans = [];
    for (const [id, trace] of this.traces) {
      if (!this.inspect(trace).live) {
        orphans.push({
          job: id,
          token: trace.label,
          stage: trace.lastStage,
          ageMs: Date.now() - trace.createdAt
        });
      }
    }

    return {
      paths: { ...this.counts },
      abandonReasons: Object.fromEntries(this.abandonReasons),
      finalizeReasons: Object.fromEntries(this.finalizeReasons),
      metrics: out,
      open: active,

      // created = finalized + active, with nothing unaccounted for.
      created: this.created,
      finalized,
      active,
      orphans,
      balanced: this.created === finalized + active
    };
  }

  /**
   * Say whether the job accounting adds up, and name anything that does not.
   *
   * @returns {{balanced:boolean, created:number, finalized:number,
   *   active:number, orphans:Array}}
   */
  reconcile({ log = true } = {}) {
    const r = this.report();
    const summary = {
      balanced: r.balanced && r.orphans.length === 0,
      created: r.created,
      finalized: r.finalized,
      active: r.active,
      orphans: r.orphans
    };

    if (!log) return summary;

    logger.send(
      `[BATCH RECONCILE] created=${r.created} finalized=${r.finalized} ` +
        `active=${r.active} orphan=${r.orphans.length}` +
        (r.balanced ? "" : " KHONG KHOP")
    );

    for (const orphan of r.orphans) {
      logger.send(
        `[BATCH RECONCILE] ORPHAN JOB job=${orphan.job} token=${orphan.token} ` +
          `stage=${orphan.stage} age=${orphan.ageMs}ms`
      );
    }

    return summary;
  }

  /** One readable block, for the log or a report. */
  formatReport() {
    const { paths, metrics } = this.report();
    const lines = [
      `[LATENCY REPORT] FAST=${paths.FAST || 0} REST_FALLBACK=${paths.REST_FALLBACK || 0}` +
        ` completed=${paths.completed} abandoned=${paths.abandoned}`
    ];

    const reasons = Object.entries(this.report().abandonReasons);
    if (reasons.length) {
      lines.push(
        `  abandoned by reason: ` +
          reasons.map(([reason, n]) => `${reason}=${n}`).join(" ")
      );
    }

    const order = [
      "stream→decision",
      "decision→submit",
      "submit duration",
      "total",
      "stream→parse",
      "parse→state",
      "state→best",
      "best→decision",
      // the breakdown, so the report names the wait and not just the span
      "debounce",
      "queue wait",
      "book lookup",
      "REST queue",
      "REST",
      "submit queue",
      "slot wait",
      "verify",
      "lane wait",
      "rate-limit wait",
      "signing",
      "HTTP"
    ];

    for (const metric of order) {
      const m = metrics[metric];
      if (!m) continue;
      lines.push(
        `  ${metric.padEnd(18)} n=${String(m.n).padStart(4)}` +
          ` p50=${String(m.p50).padStart(6)}ms` +
          ` p90=${String(m.p90).padStart(6)}ms` +
          ` p95=${String(m.p95).padStart(6)}ms` +
          ` max=${String(m.max).padStart(6)}ms`
      );
    }

    return lines.join("\n");
  }

  reset() {
    this.traces.clear();
    this.samples.clear();
    this.counts = { FAST: 0, FAST_FLOOR: 0, REST_FALLBACK: 0, UNKNOWN: 0, completed: 0, abandoned: 0 };
  }
}

/** One profiler for the whole process, so both chains report together. */
const profiler = new LatencyProfiler();

module.exports = { LatencyProfiler, profiler, STAGES, SPANS, SUB_STAGES, DETAIL_SPANS };
