"use strict";

const path = require("node:path");
const { spawnSync } = require("node:child_process");

const suites = [
  { file: "tools/build-gate.js" },
  { file: "numeric-trait-offer-test.js" },
  { file: "stream-best-stale-trait-scope-test.js" },
  { file: "stream-mapped-not-applied-diagnostic-test.js" },
  { file: "two-key-capacity-independence-test.js" },
  { file: "book-contradiction-recovery-test.js" },
  { file: "submit-flight-superseded-error-trace-test.js" },
  { file: "production-trace-superseded-ambiguous-test.js" },
  { file: "pre-tobookop-diagnostic-test.js" },
  { file: "pre-tobookop-index-perf-test.js" },
  { file: "pre-tobookop-slug-lifecycle-test.js" },
  { file: "slug-resolution-compensation-test.js" },
  { file: "stale-streak-reconcile-test.js" },
  { file: "terminal-cancel-ordering-test.js" },
  { file: "revalidate-ordering-test.js" },
  { file: "recovery-plane-relief-slot-test.js" },
  { file: "opensea-criteria-test.js" },
  { file: "offer-item-v2-recovery-lifecycle-test.js" },
  { file: "offer-item-v2-topic-recovery-test.js" },
  { file: "offer-item-v2-recovery-read-priority-test.js" },
  { file: "stream-sdk-test.js" },
  { file: "stream-degraded-fallback-test.js" },
  { file: "stream-shard-test.js" },
  { file: "max-edit-wake-test.js" },
  { file: "bootstrap-priority-test.js" },
  { file: "degraded-poll-test.js" },
  { file: "read-quota-test.js" },
  { file: "read-dispatcher-test.js" },
  { file: "write-quota-test.js" },
  { file: "event-backfill-test.js" },
  { file: "read-capacity-test.js" },
  { file: "quota-broker-test.js" },
  { file: "production-trace-test.js" },
  { file: "stream-ab-architecture-test.js" },
  { file: "offer-item-v2-ab-selfcancel-test.js" },
  { file: "own-remove-grace-test.js" },
  { file: "priority-inversion-flight-test.js" },
  { file: "topic-lifecycle-test.js" },
  { file: "own-authority-startup-test.js" },
  { file: "realtime-own-unknown-fastpath-test.js" },
  { file: "stream-dual-feed-test.js" },
  { file: "stream-dual-feed-wiring-test.js" },
  { file: "offer-item-v2-stress-test.js" },
  { file: "shared-credentials-bridge-test.js" },
  { file: "shared-credentials-hooks-test.js" },
  { file: "shared-credentials-concurrency-test.js" },
  { file: "wallet-profiles-test.js" },
  { file: "update-test.js", electron: true }
];
let failed = 0;
for (const suite of suites) {
  process.stdout.write(`\n=== ${suite.file} ===\n`);
  const command = suite.electron ? require("electron") : process.execPath;
  // ELECTRON_RUN_AS_NODE makes electron.exe behave as plain Node -- no
  // `app`, no BrowserWindow, nothing Electron-specific. If that var is set
  // in the parent shell (observed in this dev environment), every
  // `electron: true` suite silently ran as Node instead of real Electron,
  // so `require("electron").app` was undefined and the suite failed for a
  // reason that had nothing to do with its own correctness. Strip it only
  // for suites that actually declared they need the real app module.
  const env = suite.electron
    ? Object.fromEntries(Object.entries(process.env).filter(([k]) => k !== "ELECTRON_RUN_AS_NODE"))
    : process.env;
  const result = spawnSync(command, [path.join(__dirname, suite.file)], { stdio: "inherit", windowsHide: true, env });
  if (result.error || result.status !== 0) {
    failed++;
    process.stderr.write(`FAIL ${suite.file}${result.error ? `: ${result.error.message}` : ` (exit ${result.status})`}\n`);
  }
}
process.stdout.write(`\n${suites.length - failed}/${suites.length} test suites passed\n`);
if (failed) process.exitCode = 1;
