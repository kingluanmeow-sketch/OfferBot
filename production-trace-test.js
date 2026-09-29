"use strict";
const assert = require("node:assert/strict");
const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const trace = require("./production-trace");

async function main() {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "offerbot-trace-test-"));
  try {
    assert.equal(trace.configure(dir), true);
    const event = {
      correlationId: `s${process.pid}-1`, event: "item_received_bid",
      collectionSlug: "alpha", nft: { tokenId: "42" }, eventTimestamp: 1234
    };
    trace.record("stream_rx", event, {
      event: event.event, collection: "alpha", tokenId: "42",
      apiKey: "SHOULD_NEVER_BE_WRITTEN", privateKey: "SHOULD_NEVER_BE_WRITTEN",
      status: "mapped"
    });
    trace.record("own_state", event, {
      ownAuthoritative: false, ownSyncPending: true, ownSyncAt: 10,
      ownReconciledAt: 9, ownKnownAt: 8, ownUnknownAt: 11, ownReadOwned: true
    });
    await trace.flush();
    const file = path.join(dir, "offerbot-production-trace.jsonl");
    const saved = await fs.readFile(file, "utf8");
    const lines = saved.trim().split("\n");
    const row = JSON.parse(lines[0]);
    assert.equal(row.correlationId, event.correlationId);
    assert.equal(row.event, "item_received_bid");
    assert.equal(row.collection, "alpha");
    assert.equal(row.tokenId, "42");
    assert.equal(row.eventTimestamp, 1234);
    const own = JSON.parse(lines[1]);
    assert.equal(own.stage, "own_state");
    assert.equal(own.ownAuthoritative, false);
    assert.equal(own.ownSyncPending, true);
    assert.equal(own.ownReadOwned, true);
    assert(!saved.includes("SHOULD_NEVER_BE_WRITTEN"));

    await fs.writeFile(file, Buffer.alloc(trace.MAX_FILE_BYTES));
    trace.record("decision", event, { status: "SEND", target: 0.011, max: 0.02 });
    await trace.flush();
    const rotated = await fs.stat(file + ".1");
    const current = await fs.stat(file);
    assert.equal(rotated.size, trace.MAX_FILE_BYTES);
    assert(current.size < 1024);
    process.stdout.write("PASS bounded rotation and allowlisted secret-free correlated trace\n");
  } finally {
    await fs.rm(dir, { recursive: true, force: true });
  }
}
main().catch(error => { process.stderr.write(`${error.stack || error}\n`); process.exitCode = 1; });
