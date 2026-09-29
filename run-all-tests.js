"use strict";

const path = require("node:path");
const { spawnSync } = require("node:child_process");

const suites = [
  { file: "numeric-trait-offer-test.js" },
  { file: "opensea-criteria-test.js" },
  { file: "offer-item-v2-recovery-lifecycle-test.js" },
  { file: "offer-item-v2-topic-recovery-test.js" },
  { file: "offer-item-v2-recovery-read-priority-test.js" },
  { file: "stream-ab-architecture-test.js" },
  { file: "offer-item-v2-ab-selfcancel-test.js" },
  { file: "offer-item-v2-stress-test.js" },
  { file: "update-test.js", electron: true }
];
let failed = 0;
for (const suite of suites) {
  process.stdout.write(`\n=== ${suite.file} ===\n`);
  const command = suite.electron ? require("electron") : process.execPath;
  const result = spawnSync(command, [path.join(__dirname, suite.file)], { stdio: "inherit", windowsHide: true });
  if (result.error || result.status !== 0) {
    failed++;
    process.stderr.write(`FAIL ${suite.file}${result.error ? `: ${result.error.message}` : ` (exit ${result.status})`}\n`);
  }
}
process.stdout.write(`\n${suites.length - failed}/${suites.length} test suites passed\n`);
if (failed) process.exitCode = 1;
