"use strict";

/**
 * test-exe-sandbox.js — the ONLY way a test may launch a packaged EXE.
 *
 * INCIDENT (2026-09-23): EXE tests set LOCALAPPDATA to a temp dir but Electron
 * resolves userData from the Windows known folder (APPDATA env is ignored), so
 * test EXEs opened and overwrote the owner's production DB
 * %APPDATA%\opensea-offer-bot\offerbot-snapshot.json.
 *
 * sandboxSpawn():
 *   - requires/creates a TEMP LOCALAPPDATA (never the real one),
 *   - always passes --user-data-dir=<that temp>\userData (covers "-N" slots too),
 *   - sets OSB_TEST_SANDBOX=1 so the app's own profile-guard aborts (exit 87)
 *     if anything still resolves to production,
 *   - throws BEFORE spawning if any resolved path is a production location.
 * Temp dirs it creates itself are removed on process exit. Tests that pass
 * their own LOCALAPPDATA keep ownership (and cleanup) of it — the derived
 * userData lives inside it, so relaunching with the same env persists state
 * exactly as a real restart would.
 */

const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawn } = require("child_process");
const { within } = require("./profile-guard");

const REAL_APPDATA = process.env.APPDATA || path.join(os.homedir(), "AppData", "Roaming");
const REAL_LOCALAPPDATA = (() => {
  // The real one, even if a caller already overrode process.env.LOCALAPPDATA.
  const guess = path.join(path.dirname(REAL_APPDATA), "Local");
  return process.env.OSB_REAL_LOCALAPPDATA || guess;
})();
const PRODUCTION_ROOTS = [
  path.join(REAL_APPDATA, "opensea-offer-bot"),
  path.join(REAL_APPDATA, "OpenSea Offer Bot"),
  path.join(REAL_APPDATA, "osb-license-manager"),
  path.join(REAL_APPDATA, "OSB License Manager"),
  path.join(REAL_LOCALAPPDATA, "OpenSea Offer Bot"),
  path.join(REAL_LOCALAPPDATA, "OSB License Manager")
];

const created = [];
/** One temp machine per test process when the test did not bring its own: windows of one test share a machine (licence vault, slot locks), exactly like one PC. */
let processMachine = null;
let exitHook = false;

function isProductionPath(p) {
  return PRODUCTION_ROOTS.some(root => within(p, root));
}

function assertNotProduction(label, p) {
  if (!p) throw new Error(`[EXE SANDBOX] ${label} is empty`);
  if (isProductionPath(p)) throw new Error(`[EXE SANDBOX] ABORT: ${label} ${p} is a PRODUCTION location`);
  const resolved = path.resolve(p).toLowerCase();
  if (resolved === path.resolve(REAL_LOCALAPPDATA).toLowerCase() || resolved === path.resolve(REAL_APPDATA).toLowerCase()) {
    throw new Error(`[EXE SANDBOX] ABORT: ${label} ${p} is the real ${label === "userData" ? "APPDATA" : "LOCALAPPDATA"}`);
  }
}

function tempMachine(tag = "exe") {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `osb-sandbox-${tag}-`));
  created.push(dir);
  if (!exitHook) {
    exitHook = true;
    process.on("exit", () => { for (const d of created) { try { fs.rmSync(d, { recursive: true, force: true }); } catch { /* busy */ } } });
  }
  return dir;
}

/**
 * Resolve the sandbox for an env: { env, args, userData, localAppData }.
 * Throws if the result could touch production.
 */
function sandboxFor(env = process.env, tag = "exe") {
  let local = env && env.LOCALAPPDATA;
  if (!local || path.resolve(local).toLowerCase() === path.resolve(REAL_LOCALAPPDATA).toLowerCase()) {
    if (!processMachine) processMachine = tempMachine(tag);
    local = processMachine;
  }
  const userData = path.join(local, "__userData");
  assertNotProduction("LOCALAPPDATA", local);
  assertNotProduction("userData", userData);
  fs.mkdirSync(userData, { recursive: true });
  return {
    localAppData: local,
    userData,
    args: [`--user-data-dir=${userData}`],
    env: { ...(env || process.env), LOCALAPPDATA: local, OSB_TEST_SANDBOX: "1", OSB_REAL_LOCALAPPDATA: REAL_LOCALAPPDATA }
  };
}

/** Drop-in for child_process.spawn(exe, args, options) for packaged EXEs. */
function sandboxSpawn(exe, args = [], options = {}) {
  const box = sandboxFor(options.env || process.env, path.basename(String(exe)).replace(/\W+/g, "").slice(0, 12));
  const finalArgs = [...box.args, ...(args || []).filter(a => !/^--user-data-dir=/.test(String(a)))];
  const child = spawn(exe, finalArgs, { ...options, env: box.env });
  child.sandbox = box;
  return child;
}

/**
 * For Electron tests that load main.js IN-PROCESS. Call at the very top of the
 * test, before requiring main/instance/dev-runtime: points userData AND
 * LOCALAPPDATA at a temp root and sets the sandbox marker, so main.js's
 * profile-guard verifies nothing resolves to production. A later
 * app.setPath("userData", <another temp dir>) by the test is fine; setting it
 * back to a production path makes main.js abort (exit 87).
 */
function inProcessSandbox(app, tag = "inproc") {
  const root = tempMachine(String(tag).replace(/\W+/g, "").slice(0, 16) || "inproc");
  const local = path.join(root, "local");
  const userData = path.join(root, "userData");
  fs.mkdirSync(local, { recursive: true });
  fs.mkdirSync(userData, { recursive: true });
  assertNotProduction("LOCALAPPDATA", local);
  assertNotProduction("userData", userData);
  process.env.OSB_REAL_LOCALAPPDATA = REAL_LOCALAPPDATA;
  process.env.LOCALAPPDATA = local;
  process.env.OSB_TEST_SANDBOX = "1";
  if (app && typeof app.setPath === "function") app.setPath("userData", userData);
  return { root, localAppData: local, userData };
}

/** Where a sandboxed app keeps userData for a given (temp) LOCALAPPDATA; slot N adds "-N". */
function sandboxUserData(localAppData, slot = 1) {
  const base = path.join(localAppData, "__userData");
  return slot > 1 ? `${base}-${slot}` : base;
}
/** The shared temp machine of this test process (created on first use). */
function processLocalAppData() { if (!processMachine) processMachine = tempMachine("proc"); return processMachine; }

/**
 * NEVER KILL BY NAME (2026-09-23, second incident)
 *
 *   EXE tests cleaned up with `Get-Process 'OpenSea Offer Bot' | Stop-Process
 *   -Force` / `taskkill /IM` — which force-killed the OWNER's running app too
 *   (observed: the owner's app, started 21:12:59, gone after an EXE batch).
 *
 *   A sandboxed process is identifiable: sandboxSpawn always passes
 *   `--user-data-dir=<os.tmpdir()>\…`, and Chromium copies that flag onto every
 *   helper process. The owner's app has no such flag on its main process and
 *   its helpers point at Roaming — so matching name AND a temp user-data-dir
 *   can never select a production process.
 */
const DEFAULT_APP_NAMES = ["OpenSea Offer Bot", "OSB License Manager", "OSB-License-Manager"];
function psQuote(s) { return "'" + String(s).replace(/'/g, "''") + "'"; }
function sandboxProcessFilterPs(names = DEFAULT_APP_NAMES) {
  const list = names.map(n => psQuote(String(n).replace(/\.exe$/i, ""))).join(",");
  const tmp = String(os.tmpdir()).replace(/[[\]*?]/g, "");
  return `Get-CimInstance Win32_Process | Where-Object { (@(${list}) -contains ($_.Name -replace '\\.exe$','')) -and ` +
    `$_.CommandLine -like '*--user-data-dir=*' -and $_.CommandLine -like ${psQuote("*" + tmp + "*")} }`;
}
/** PowerShell command: force-stop ONLY sandboxed test processes. */
function safeKillPs(names = DEFAULT_APP_NAMES) {
  return `${sandboxProcessFilterPs(names)} | ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }`;
}
/** PowerShell command printing how many sandboxed test processes are alive. */
function safeCountPs(names = DEFAULT_APP_NAMES) {
  return `@(${sandboxProcessFilterPs(names)}).Count`;
}
/** Synchronous helpers for tests that do not build their own command. */
function killSandboxedApps(names = DEFAULT_APP_NAMES) {
  try { require("child_process").execFileSync("powershell", ["-NoProfile", "-Command", safeKillPs(names)], { stdio: "ignore" }); } catch { /* none */ }
}
function sandboxedAppCount(names = DEFAULT_APP_NAMES) {
  try {
    return Number(require("child_process").execFileSync("powershell", ["-NoProfile", "-Command", safeCountPs(names)],
      { encoding: "utf8" }).trim()) || 0;
  } catch { return 0; }
}

module.exports = { sandboxUserData, processLocalAppData, sandboxSpawn, sandboxFor, inProcessSandbox, isProductionPath, assertNotProduction, PRODUCTION_ROOTS, REAL_LOCALAPPDATA,
  safeKillPs, safeCountPs, killSandboxedApps, sandboxedAppCount, sandboxProcessFilterPs };
