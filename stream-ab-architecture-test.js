"use strict";

const assert = require("assert");
const { OpenSeaStream, HANDLED_EVENTS } = require("./stream");

let passed = 0;
let failed = 0;
function check(name, fn) {
  try { fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}

function makeStream(slugs) {
  const writes = [];
  const stream = new OpenSeaStream({ getApiKey: () => "" });
  stream.closedByUser = false;
  stream.ws = { readyState: 1, send: value => writes.push(JSON.parse(value)) };
  stream.setCollections(slugs);
  return { stream, writes };
}

// The normal path must subscribe to only the tracked topics. The wildcard may
// remain available only as an explicit diagnostic opt-in.
delete process.env.OSB_STREAM_GLOBAL;
{
  const { stream, writes } = makeStream(["alpha", "beta"]);
  check("default mode is tracked per-collection", () => {
    assert.strictEqual(stream.globalMode, false);
    assert.deepStrictEqual(writes.filter(frame => frame[3] === "phx_join").map(frame => frame[2]).sort(),
      ["collection:alpha", "collection:beta"]);
  });
  check("each per-collection join requests only Offer Item event types", () => {
    stream.globalMode = false;
    stream.syncSubscriptions();
    const joins = writes.filter(frame => frame[3] === "phx_join" && frame[2] !== "collection:*");
    assert.strictEqual(joins.length, 2);
    for (const frame of joins) {
      assert.deepStrictEqual(frame[4].event_types, [...HANDLED_EVENTS]);
    }
  });
  const before = writes.length;
  stream.setCollections(["alpha", "gamma"]);
  check("tracked set change leaves and joins only affected topics", () => {
    const delta = writes.slice(before);
    assert.deepStrictEqual(delta.map(frame => [frame[2], frame[3]]), [
      ["collection:beta", "phx_leave"], ["collection:gamma", "phx_join"]
    ]);
  });
  const many = Array.from({ length: 58 }, (_, i) => `tracked-${i}`);
  const manyWritesAt = writes.length;
  stream.setCollections(many);
  check("58 tracked collections create only 58 collection topics", () => {
    const delta = writes.slice(manyWritesAt);
    const joins = delta.filter(frame => frame[3] === "phx_join");
    assert.strictEqual(joins.length, 58);
    assert(joins.every(frame => /^collection:tracked-\d+$/.test(frame[2])));
    assert(!joins.some(frame => frame[2] === "collection:*"));
  });
  stream.stop();
}

// Exercise the regression that made the production reconnect loop fail to
// back off: each short open used to reset exponential delay to one second.
{
  const { stream } = makeStream(["alpha"]);
  const reconnectDelays = [];
  const schedule = stream.scheduleReconnect.bind(stream);
  stream.scheduleReconnect = function () {
    reconnectDelays.push(this.backoff);
    return schedule();
  };
  function openAndClose() {
    const ws = { readyState: 1, send() {}, close() { this.readyState = 3; } };
    stream.ws = ws;
    stream.handleOpen(ws);
    stream.handleClose(ws, 1006, "socket closed");
    clearTimeout(stream.reconnectTimer);
    stream.reconnectTimer = null;
  }
  openAndClose();
  openAndClose();
  openAndClose();
  check("rapid code 1006 reconnects use increasing backoff", () => {
    assert.deepStrictEqual(reconnectDelays.slice(0, 3), [1000, 2000, 4000]);
  });
  stream.stop();
}

// Every tracked collection must have its own ACK. A reconnect loses events
// even when the replacement socket opens successfully; repair starts on ACK.
{
  const repaired = [];
  const { stream, writes } = makeStream(["alpha", "beta"]);
  stream.onTopicGap = slug => repaired.push(slug);
  const acknowledge = slug => {
    const join = [...writes].reverse().find(frame => frame[2] === `collection:${slug}` && frame[3] === "phx_join");
    stream.handleMessage(JSON.stringify([join[0], join[1], join[2], "phx_reply", { status: "ok", response: {} }]));
  };
  acknowledge("alpha");
  check("a joined collection is ready while another waits for ACK", () => {
    assert.strictEqual(stream.healthForCollection("alpha"), "HEALTHY");
    assert.strictEqual(stream.healthForCollection("beta"), "RECONNECTING");
  });
  acknowledge("beta");
  stream.gapPending = true;
  const replacement = { readyState: 1, send: value => writes.push(JSON.parse(value)), close() { this.readyState = 3; } };
  stream.ws = replacement;
  stream.handleOpen(replacement);
  check("reconnected topics are gated before fresh ACK", () => {
    assert.strictEqual(stream.healthForCollection("alpha"), "RECONNECTING");
    assert.strictEqual(stream.healthForCollection("beta"), "RECONNECTING");
  });
  acknowledge("alpha");
  check("only ACKed collection is repaired and reopened", () => {
    assert.deepStrictEqual(repaired, ["alpha"]);
    assert.strictEqual(stream.healthForCollection("alpha"), "HEALTHY");
    assert.strictEqual(stream.healthForCollection("beta"), "RECONNECTING");
  });
  acknowledge("beta");
  check("all affected collections recover exactly once", () => {
    assert.deepStrictEqual(repaired, ["alpha", "beta"]);
  });
  stream.stop();
}

// Explicit diagnostic mode is opt-in; it must never become production's
// default after the environment is changed for an experiment.
{
  process.env.OSB_STREAM_GLOBAL = "1";
  const { stream, writes } = makeStream(["alpha", "beta"]);
  check("global wildcard requires explicit diagnostic opt-in", () => {
    assert.strictEqual(stream.globalMode, true);
    assert.deepStrictEqual(writes.filter(frame => frame[3] === "phx_join").map(frame => frame[2]), ["collection:*"]);
  });
  stream.stop();
  delete process.env.OSB_STREAM_GLOBAL;
}

process.stdout.write(`\n${passed}/${passed + failed} Stream A/B architecture checks passed\n`);
process.exitCode = failed ? 1 : 0;
