"use strict";

/**
 * RED/GREEN: `submitOne`'s superseded-flight ambiguous branch (engine-v2.js)
 * records status "AMBIGUOUS_OUTCOME" / reason "superseded-ambiguous" -- but
 * `production-trace.js`'s `record()` silently sanitizes any status/reason
 * not in its own allowlist (STATUSES/REASONS) down to "". Without adding
 * both values to those allowlists, the engine-level fix writes a trace row
 * that reaches disk with status/reason both blanked out -- indistinguishable
 * from a malformed/unknown row, defeating the audit's actual requirement
 * ("http_start always has a terminal trace with a CLEAR outcome").
 */

const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}

(async () => {

await check("AMBIGUOUS_OUTCOME status and superseded-ambiguous reason survive record()'s allowlist and reach disk intact", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "offerbot-trace-test-"));
  delete require.cache[require.resolve("./production-trace")];
  const productionTrace = require("./production-trace");
  productionTrace.configure(dir);
  productionTrace.record("submit_failure", { correlationId: "ethereum:9001" }, {
    chain: "ethereum", collection: "coll", tokenId: "9001",
    status: "AMBIGUOUS_OUTCOME", reason: "superseded-ambiguous", target: 0.03
  });
  await productionTrace.flush();
  const filePath = path.join(dir, "offerbot-production-trace.jsonl");
  const content = fs.readFileSync(filePath, "utf8").trim();
  const row = JSON.parse(content.split("\n")[0]);
  assert.equal(row.status, "AMBIGUOUS_OUTCOME", "status must not be sanitized away");
  assert.equal(row.reason, "superseded-ambiguous", "reason must not be sanitized away");
  fs.rmSync(dir, { recursive: true, force: true });
});

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
