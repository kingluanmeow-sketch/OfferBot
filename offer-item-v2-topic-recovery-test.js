"use strict";

const assert = require("node:assert/strict");
const { OpenSeaStream } = require("./stream");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");
const { INTENT } = require("./offer-item-v2/intent-store");

let passed = 0;
let failed = 0;
function check(name, fn) {
  try { fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}

function streamWithTopics(slugs, callbacks = {}) {
  const writes = [];
  const stream = new OpenSeaStream({ getApiKey: () => "test-key", ...callbacks });
  stream.closedByUser = false;
  stream.ws = { readyState: 1, send: value => writes.push(JSON.parse(value)), close() {} };
  stream.setCollections(slugs);
  const ack = slug => {
    const frame = [...writes].reverse().find(f => f[2] === `collection:${slug}` && f[3] === "phx_join");
    assert(frame, `missing join for ${slug}`);
    stream.handleMessage(JSON.stringify([frame[0], frame[1], frame[2], "phx_reply", { status: "ok", response: {} }]));
  };
  return { stream, writes, ack };
}

function makeEngine() {
  const health = new Map([["alpha", "HEALTHY"], ["beta", "HEALTHY"]]);
  const adapter = { chain: "ethereum", async fetchBest() { return { empty: true, knownEmpty: true, orders: [] }; } };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(slug => health.get(slug) || "DISCONNECTED");
  engine.templateReady = () => true;
  engine.pump = () => {};
  const rows = new Map();
  const books = new Map();
  for (const [slug, ids] of [["alpha", ["a1", "a2"]], ["beta", ["b1"]]]) {
    for (const id of ids) {
      const key = `ethereum:0x${slug === "alpha" ? "11" : "22"}11111111111111111111111111111111111111:${id}`;
      const row = { key, tokenId: id, contract: `0x${slug === "alpha" ? "11" : "22"}11111111111111111111111111111111111111`,
        collectionSlug: slug, running: true, minPrice: 0.001, maxPrice: 1, step: 0.001 };
      const book = engine.book.add({ key, chain: "ethereum", contract: row.contract, tokenId: id });
      book.generation = 1;
      book.hydratedAt = Date.now() - 1000;
      book.lastFullReadAt = Date.now() - 1000;
      book.effectiveBest = () => ({ price: 0.02, kind: "item", maker: "0x3333333333333333333333333333333333333333", quantity: 1 });
      book.ownBest = () => ({ price: 0, kind: "item", maker: "", quantity: 1 });
      rows.set(key, row); books.set(key, book); engine.rows.set(key, row);
    }
  }
  const recoveryCalls = [];
  engine.scheduleRecoveryRows = (selected, reason, opts = {}) => {
    recoveryCalls.push({ rows: selected.map(row => row.key), reason, authoritative: Boolean(opts.authoritative) });
    for (const row of selected) engine.recoveryCandidates.set(row.key, { reason, authoritative: Boolean(opts.authoritative) });
    return selected.length;
  };
  return { engine, health, rows, books, recoveryCalls };
}

// A per-topic protocol failure must be reported immediately, while the
// transport and unrelated topic remain healthy.
{
  const unavailable = [];
  const { stream, ack } = streamWithTopics(["alpha", "beta"], {
    onTopicUnavailable: (slug, at, reason) => unavailable.push({ slug, at, reason })
  });
  ack("alpha"); ack("beta");
  stream.handleMessage(JSON.stringify([null, "x", "collection:alpha", "phx_error", { response: {} }]));
  check("partial topic failure is surfaced immediately and scoped", () => {
    assert.equal(stream.health(), "HEALTHY");
    assert.equal(stream.healthForCollection("beta"), "HEALTHY");
    assert.equal(stream.healthForCollection("alpha"), "RECONNECTING");
    assert.equal(unavailable.length, 1);
    assert.equal(unavailable[0].slug, "alpha");
    assert(Number.isFinite(unavailable[0].at));
  });
  stream.stop();
}

// Join ACK timeout and multiple independent topic failures stay scoped.
{
  const unavailable = [];
  const { stream, ack } = streamWithTopics(["alpha", "beta"], {
    onTopicUnavailable: (slug, at, reason) => unavailable.push({ slug, at, reason })
  });
  ack("alpha"); ack("beta");
  stream.joinState.get("alpha").sentAt = Date.now() - 25000;
  stream.joinState.get("alpha").state = "JOINING";
  stream.syncSubscriptions();
  stream.joinBackoff("beta", "server refused");
  check("join ACK timeout and second topic failure report independently", () => {
    assert.equal(unavailable.length, 2);
    assert.deepEqual(new Set(unavailable.map(x => x.slug)), new Set(["alpha", "beta"]));
    assert(unavailable.some(x => /no reply|refused/i.test(x.reason)));
  });
  stream.stop();
}

// A topic rejoins once and recovery authority clears without duplicate work.
{
  const unavailable = [];
  const recovered = [];
  const { stream, ack } = streamWithTopics(["alpha"], {
    onTopicUnavailable: slug => unavailable.push(slug), onTopicGap: slug => recovered.push(slug)
  });
  ack("alpha");
  stream.handleMessage(JSON.stringify([null, "x", "collection:alpha", "phx_error", { response: {} }]));
  stream.joinState.get("alpha").retryAt = 0;
  stream.syncSubscriptions();
  ack("alpha");
  check("topic reconnect closes exactly one outage", () => {
    assert.deepEqual(unavailable, ["alpha"]);
    assert.deepEqual(recovered, ["alpha"]);
    assert.equal(stream.healthForCollection("alpha"), "HEALTHY");
  });
  stream.stop();
}

// A refusal and a missing ACK both transition the exact topic into BACKOFF;
// repeated sync ticks must not emit duplicate outage notifications.
{
  const unavailable = [];
  const { stream, ack } = streamWithTopics(["alpha"], {
    onTopicUnavailable: (slug, at, reason) => unavailable.push({ slug, at, reason })
  });
  ack("alpha");
  stream.joinBackoff("alpha", "server refused");
  stream.joinBackoff("alpha", "server refused again");
  check("join refusal creates one outage edge, not a retry storm", () => {
    assert.equal(unavailable.length, 1);
    assert.equal(unavailable[0].slug, "alpha");
    assert.match(unavailable[0].reason, /refus/i);
  });
  stream.stop();
}

// Recovery belongs only to running rows in the failed collection. It is a full
// read path and is idempotent across repeated reports for the same outage.
{
  const { engine, health, recoveryCalls, rows } = makeEngine();
  health.set("alpha", "RECONNECTING");
  check("engine owns a targeted full recovery for topic outage", () => {
    assert.equal(typeof engine.onTopicUnavailable, "function");
    engine.onTopicUnavailable("alpha", Date.now(), "join-refused");
    assert.equal(recoveryCalls.length, 1);
    assert.equal(recoveryCalls[0].reason, "topic-gap");
    assert.equal(recoveryCalls[0].rows.length, 2);
    assert(recoveryCalls[0].rows.every(key => rows.get(key).collectionSlug === "alpha"));
    assert.equal(engine.topicRepairAt.size, 2);
    engine.onTopicUnavailable("alpha", Date.now() + 1, "repeat-refusal");
    assert.equal(recoveryCalls.length, 1, "duplicate outage reports should coalesce");
  });
  engine.recovery.stop();
}

// Accelerated scoped outage/rejoin cycles expose duplicate scheduling or
// retained per-topic recovery state without creating live HTTP traffic.
{
  const { engine, health, recoveryCalls, rows, books } = makeEngine();
  const alpha = [...rows.entries()].filter(([, value]) => value.collectionSlug === "alpha");
  health.set("alpha", "RECONNECTING");
  check("100 accelerated outage/rejoin cycles remain scoped and bounded", () => {
    for (let i = 0; i < 100; i++) {
      const at = Date.now() + i * 2;
      engine.onTopicUnavailable("alpha", at, "fault-injection");
      for (const [key] of alpha) books.get(key).lastFullReadAt = at + 1;
      health.set("alpha", "HEALTHY");
      engine.onTopicGap("alpha");
      health.set("alpha", "RECONNECTING");
      engine.recoveryCandidates.clear();
    }
    assert.equal(recoveryCalls.filter(call => call.rows.length > 0).length, 100);
    assert.equal(engine.topicRepairAt.size, 0);
    assert.equal([...engine.topicRepairAt.keys()].some(key => rows.get(key)?.collectionSlug === "beta"), false);
  });
  engine.recovery.stop();
}

// Once a newer full snapshot covers an outage, repeated evaluation stays on
// one intent and never starts a second recovery for the healthy topic.
{
  const { engine, health, rows, books, recoveryCalls } = makeEngine();
  const [key, row] = [...rows.entries()].find(([, value]) => value.collectionSlug === "alpha");
  const book = books.get(key);
  const outageAt = Date.now();
  health.set("alpha", "RECONNECTING");
  engine.onTopicUnavailable("alpha", outageAt, "fault-injection");
  for (const [rowKey] of [...rows.entries()].filter(([, value]) => value.collectionSlug === "alpha"))
    books.get(rowKey).lastFullReadAt = outageAt + 5;
  health.set("alpha", "HEALTHY");
  engine.onTopicGap("alpha");
  engine.evaluate(key, Date.now(), null);
  engine.evaluate(key, Date.now(), null);
  check("fresh recovery produces one latest-wins intent and no duplicate read", () => {
    assert.equal(engine.intents.get(key)?.state, INTENT.READY);
    assert.equal(recoveryCalls.filter(call => call.rows.length > 0).length, 1);
    assert.equal(engine.topicRepairAt.has(key), false);
  });
  engine.recovery.stop();
}

// The watchdog must create an owner when a SEND is held by stream-topic and
// must not count a row already owned by a targeted recovery as an orphan.
{
  const { engine, health, recoveryCalls, rows } = makeEngine();
  const row = rows.values().next().value;
  for (const other of rows.values()) if (other.collectionSlug !== "alpha") other.running = false;
  health.set("alpha", "RECONNECTING");
  row.sendBlockedBy = { gate: "stream-topic", at: Date.now() };
  check("watchdog repairs stream-topic SEND instead of repeating evaluate", () => {
    engine.watchdog();
    assert(recoveryCalls.length >= 1);
    assert.equal(recoveryCalls[0].reason, "topic-gap");
    assert.equal(engine.stats.watchdogOrphans || 0, 0);
  });
  engine.recovery.stop();
}

// A fresh complete snapshot newer than topic loss can authorize a fallback
// send while only that topic is down; a stale snapshot cannot.
{
  const { engine, health, rows, books } = makeEngine();
  const [key, row] = [...rows.entries()][0];
  const book = books.get(key);
  const downAt = Date.now();
  health.set("alpha", "RECONNECTING");
  engine.topicRepairAt.set(key, downAt);
  book.lastFullReadAt = downAt - 1;
  check("stale full snapshot cannot authorize outage fallback", () => {
    assert.equal(engine.topicReady(row, book), false);
  });
  book.lastFullReadAt = downAt + 1;
  check("fresh full snapshot authorizes only the failed topic", () => {
    assert.equal(engine.topicReady(row, book), true);
    const beta = [...rows.entries()].find(([, r]) => r.collectionSlug === "beta");
    assert.equal(engine.topicReady(beta[1], books.get(beta[0])), true);
  });
  engine.recovery.stop();
}

// A topic recovery may never include paused rows, and normal healthy topics
// continue to generate an immediately schedulable intent with no read.
{
  const { engine, health, recoveryCalls, rows, books } = makeEngine();
  const alphaRows = [...rows.values()].filter(row => row.collectionSlug === "alpha");
  alphaRows[1].running = false;
  health.set("alpha", "RECONNECTING");
  check("paused rows are excluded from topic recovery", () => {
    engine.onTopicUnavailable("alpha", Date.now(), "phx-close");
    assert.equal(recoveryCalls[0].rows.length, 1);
  });
  const beta = [...rows.entries()].find(([, row]) => row.collectionSlug === "beta");
  engine.evaluate(beta[0], Date.now(), null);
  check("healthy topic keeps Stream-first intent path", () => {
    assert.equal(engine.intents.get(beta[0])?.state, INTENT.READY);
    assert.equal(recoveryCalls.length, 1, "healthy topic does not acquire REST recovery");
  });
  engine.recovery.stop();
}

process.stdout.write(`\n${passed}/${passed + failed} per-topic recovery checks passed\n`);
process.exitCode = failed ? 1 : 0;
