// 2026-10-01 audit: a prior report called update-test.js "E2E against the
// real GitHub endpoint." That was wrong -- update-test.js intentionally
// mocks `autoUpdater.checkForUpdates`/`downloadUpdate` and never touches the
// network (see the top of that file: "Never let a test reach the network").
// That mock is correct for what update-test.js verifies (IPC/state-machine/
// profile-safety wiring) and must stay mocked there.
//
// THIS file is the real thing: no mocking of checkForUpdates/downloadUpdate,
// real electron-updater, real GitHub Releases endpoint. It does NOT install
// anything and NEVER calls quitAndInstall -- enforced by hard guards below,
// not by hoping nothing calls it.
//
// SCOPE, stated precisely (2026-10-01 re-audit): this harness verifies the
// real GitHub Releases update FEED and the real downloaded ASSET BYTES --
// that checkForUpdates() finds the right version/files and downloadUpdate()
// fetches bytes whose size+SHA512 match the manifest. It does NOT simulate a
// packaged baseline install byte-for-byte: there is no actual old
// packaged binary present in this sandbox to diff against, so differential
// download is explicitly disabled (disableDifferentialDownload = true) and
// only the full-download path runs. "BASELINE_VERSIONS" below only changes
// what `currentVersion` the feed check reports as already-installed; it is
// not a claim that this reproduces a packaged older installer's on-disk
// state. Do not describe this as "simulates a packaged older app."
//
// SAFETY, enforced, not assumed: every download path is tracked and force-
// deleted before exit (own try/finally, independent of success/failure).
// autoInstallOnAppQuit is asserted false before every single
// checkForUpdates/downloadUpdate call (assertSafe below), not just set once
// hopefully-early. quitAndInstall is monkey-patched to throw if anything
// ever calls it. The app identity/cache path is isolated per-run (PID in
// the name) and is asserted, before every call, to NOT be anywhere under
// the real app's identity or Electron's shared generic dev cache -- this
// does not rely on file-lock contention to prevent a real install, which is
// what silently "saved" the very first (unsafe) run of this harness on
// 2026-10-01 before these guards existed.
//
// Not part of `run-all-tests.js` / `npm run test:all`: it depends on network
// reachability to github.com and is meant to run on demand during a release,
// not on every regression pass. Run it with:
//   node run-update-e2e.js          (spawns this under real Electron)
// Its evidence log is written to release-artifacts/update-e2e-log-vX.Y.Z.txt
// and also uploaded as a GitHub release asset (release-artifacts/ is
// gitignored, so the release asset is the persisted copy).

"use strict";

const path = require("path");
const fs = require("fs");
const os = require("os");
const crypto = require("crypto");
const { createRequire } = require("module");
// Must be electron-updater's OWN semver copy, not the top-level one.
// electron-updater ships a nested node_modules/semver, and its internal
// `semver.eq(latest, this.currentVersion)` (AppUpdater.js:345) rejects a
// SemVer instance built by a different copy with 'Invalid version. Must be
// a string. Got type "object"'. Resolving through electron-updater's own
// require means we get whichever copy it actually uses (nested, or the
// deduped top-level one) -- exactly what its constructor parses with.
const euSemver = createRequire(require.resolve("electron-updater"))("semver");

const ROOT = __dirname;
// PID-scoped so two runs (or a stale run) can never collide on the same
// cache directory.
const RUN_ID = `${process.pid}-${Date.now()}`;
const PROFILE = fs.mkdtempSync(path.join(os.tmpdir(), `osb-update-e2e-${RUN_ID}-`));
const HARNESS_IDENTITY = `OfferBotUpdateE2EHarness-${RUN_ID}`;

const { app } = require("electron");
// Without an explicit name, Electron falls back to the generic "Electron"
// app identity in dev mode, and electron-updater's download cache keys off
// app.getPath("appData")/app.name -- NOT userData -- so without this the
// downloaded installer lands in the shared %LOCALAPPDATA%\Electron\pending
// cache instead of staying isolated under this harness's own throwaway
// profile. A PID-scoped name also means this can never collide with a
// previous run's leftover cache directory.
app.setName(HARNESS_IDENTITY);
app.setPath("userData", path.join(PROFILE, "userData"));
app.setPath("appData", PROFILE);
app.setPath("cache", path.join(PROFILE, "cache"));
// 2026-10-01 re-audit, ground truth from node_modules/electron-updater/out/
// AppAdapter.js: electron-updater's cache base ignores ALL Electron
// app.getPath() overrides above -- ElectronAppAdapter.baseCachePath calls
// getAppCacheDir(), which on win32 reads process.env.LOCALAPPDATA (or
// os.homedir() fallback) DIRECTLY, bypassing Electron entirely. Confirmed:
// a real run with appData/userData/cache all overridden still downloaded to
// the real %LOCALAPPDATA%\<app.name>\pending. This env override is the only
// thing that actually redirects it.
process.env.LOCALAPPDATA = path.join(PROFILE, "localappdata");

const pkg = require(path.join(ROOT, "package.json"));
const TARGET_VERSION = pkg.version;
// What `currentVersion` the feed check reports as already-installed. See
// the SCOPE note above: this does not reproduce a packaged binary's bytes.
const BASELINE_VERSIONS = ["1.25.61", "1.25.62", "1.25.63"];

const FORBIDDEN_CACHE_SUBSTRINGS = ["\\Electron\\pending", "/Electron/pending", "OpenSea Offer Bot", "opensea-offer-bot"];

const lines = [];
function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  lines.push(line);
  process.stdout.write(line + "\n");
}

let passed = 0, failed = 0, pending = 0;
async function check(name, fn) {
  try { await fn(); passed++; log(`PASS ${name}`); }
  catch (error) { failed++; log(`FAIL ${name}: ${error.message}`); }
}
/**
 * A gate that CANNOT be evaluated yet and must never be reported as PASS.
 * Used only for candidate-verification gates while the candidate release
 * does not exist on the feed: claiming PASS there would assert an update
 * path that has never actually been exercised.
 */
function markPending(name, reason) {
  pending++;
  log(`PENDING ${name}: ${reason}`);
}

function sha512base64(filePath) {
  const hash = crypto.createHash("sha512");
  hash.update(fs.readFileSync(filePath));
  return hash.digest("base64");
}

/**
 * Fail-fast guard, re-checked immediately before every single network call
 * that could end in a downloaded installer sitting on disk. Throws (not
 * logs-and-continues) on any violation, so a future edit that accidentally
 * re-enables auto-install, or that points the cache at a real app's
 * directory, breaks this harness loudly instead of silently risking a real
 * install again.
 */
function assertSafe(autoUpdater, label) {
  if (autoUpdater.autoInstallOnAppQuit !== false) {
    throw new Error(`SAFETY GUARD: autoInstallOnAppQuit is not explicitly false (${label})`);
  }
  if (typeof autoUpdater.quitAndInstall !== "function" || !autoUpdater.quitAndInstall.__guardedNoop) {
    throw new Error(`SAFETY GUARD: quitAndInstall is not the guarded no-op (${label})`);
  }
  const cacheDir = app.getPath("appData");
  if (!cacheDir.startsWith(PROFILE)) {
    throw new Error(`SAFETY GUARD: appData path left the isolated profile: ${cacheDir} (${label})`);
  }
  // The actual lever that matters: electron-updater's download cache base
  // ignores every Electron app.getPath() override and reads
  // process.env.LOCALAPPDATA directly (see AppAdapter.js getAppCacheDir).
  // Re-checked every call so a future refactor that touches this env var
  // elsewhere in the process can't silently re-widen the blast radius.
  if (!String(process.env.LOCALAPPDATA || "").startsWith(PROFILE)) {
    throw new Error(`SAFETY GUARD: process.env.LOCALAPPDATA left the isolated profile: ${process.env.LOCALAPPDATA} (${label})`);
  }
  for (const bad of FORBIDDEN_CACHE_SUBSTRINGS) {
    if (cacheDir.includes(bad) || String(process.env.LOCALAPPDATA).includes(bad)) {
      throw new Error(`SAFETY GUARD: cache path matches a forbidden real-app/shared-cache substring "${bad}" (${label})`);
    }
  }
}

const downloadedPaths = new Set();

async function runForBaseline(baselineVersion) {
  // electron-updater keeps module-level state; re-require isn't enough to
  // reset it between runs, so each baseline gets delete+fresh-require.
  delete require.cache[require.resolve("electron-updater")];
  const { autoUpdater } = require("electron-updater");

  autoUpdater.logger = { info: m => log(`[eu:${baselineVersion}] ${m}`), warn: m => log(`[eu:${baselineVersion}] WARN ${m}`), error: m => log(`[eu:${baselineVersion}] ERROR ${m}`), debug() {} };
  autoUpdater.autoDownload = false;
  // No actual packaged old binary exists in this sandbox to diff against
  // (see SCOPE note at the top) -- force the full-download path, which is
  // also simpler and removes one more thing that could half-complete.
  autoUpdater.disableDifferentialDownload = true;
  autoUpdater.allowPrerelease = false;
  // CRITICAL, re-asserted by assertSafe() before every call below, not just
  // set once here. electron-updater defaults this to true; a run without it
  // explicitly false fired a REAL silent install attempt on app.exit() on
  // 2026-10-01 (blocked only because the real app's files happened to be
  // locked by an already-running instance -- that is not a safety mechanism
  // and must never be relied on again).
  autoUpdater.autoInstallOnAppQuit = false;
  // Never let anything on this path actually install, no matter what calls
  // it or why. __guardedNoop marks this as the sentinel assertSafe() checks
  // for, so a future accidental reassignment of quitAndInstall is caught by
  // the guard instead of silently restoring the real function.
  const guardedQuitAndInstall = () => { throw new Error("quitAndInstall() called -- this must never happen in the E2E harness"); };
  guardedQuitAndInstall.__guardedNoop = true;
  autoUpdater.quitAndInstall = guardedQuitAndInstall;
  // Real GitHub endpoint -- no mocking. app.isPackaged is false under this
  // harness, so electron-updater needs an explicit opt-in to actually hit
  // the network instead of silently no-op'ing ("not packed and dev update
  // config is not forced").
  autoUpdater.forceDevUpdateConfig = true;
  // Must be a parsed SemVer object, NOT a raw string. Production never
  // assigns this directly -- AppUpdater's constructor does
  // `this.currentVersion = semver.parse(this.app.version)` (AppUpdater.js
  // ~212-217). A raw string survives the update-IS-available path but
  // throws "currentVersion.format is not a function" in the
  // update-NOT-available branch (AppUpdater.js:405), which is exactly the
  // branch a baseline equal to the published latest takes.
  autoUpdater.currentVersion = euSemver.parse(baselineVersion);
  autoUpdater.setFeedURL({
    provider: "github",
    owner: pkg.build.publish[0].owner,
    repo: pkg.build.publish[0].repo,
    releaseType: "release"
  });

  assertSafe(autoUpdater, `${baselineVersion} pre-check`);

  let checkResult;
  await check(`[${baselineVersion}] checkForUpdates() reaches the real GitHub feed and returns a manifest`, async () => {
    checkResult = await autoUpdater.checkForUpdates();
    if (!checkResult || !checkResult.updateInfo) throw new Error("no updateInfo returned");
  });
  if (!checkResult || !checkResult.updateInfo) return;

  const info = checkResult.updateInfo;
  log(`[${baselineVersion}] discovered version=${info.version} isUpdateAvailable=${checkResult.isUpdateAvailable} files=${JSON.stringify((info.files || []).map(f => f.url))}`);

  // electron-updater legitimately offers nothing when the baseline already
  // IS the published latest (allowDowngrade is false). The download chain
  // below would then fail with "Please check update first" -- that is
  // correct product behavior, not a defect, so assert the correct thing for
  // that case instead of demanding a download that must not happen.
  if (!checkResult.isUpdateAvailable) {
    await check(`[${baselineVersion}] no update offered because this baseline already IS the published latest (no downgrade)`, () => {
      if (info.version !== baselineVersion) {
        throw new Error(`no update offered, but feed latest (${info.version}) differs from baseline (${baselineVersion}) -- that would mean a real update was missed`);
      }
    });
    log(`[${baselineVersion}] correctly up-to-date against feed latest ${info.version}; download chain intentionally skipped`);
    return;
  }

  // Pre-publish: the feed still serves the previous stable release, not
  // TARGET_VERSION.  That is correct -- the candidate has not been published.
  // Report PENDING (not FAIL) so the harness exits green while making clear
  // that the candidate update path has NOT been verified yet.
  if (info.version !== TARGET_VERSION) {
    markPending(`[${baselineVersion}] discovered version matches this release's package.json`,
      `feed latest is ${info.version}, not ${TARGET_VERSION} — candidate not yet published`);
    log(`[${baselineVersion}] feed serves ${info.version} (published stable); candidate ${TARGET_VERSION} update path is PENDING until published`);
    return;
  }

  await check(`[${baselineVersion}] discovered version matches this release's package.json`, () => {
    if (info.version !== TARGET_VERSION) throw new Error(`expected ${TARGET_VERSION}, got ${info.version}`);
  });

  const expectedFile = (info.files || [])[0];
  await check(`[${baselineVersion}] manifest names exactly one Windows NSIS asset with size+sha512`, () => {
    if (!expectedFile) throw new Error("no files[] entry in updateInfo");
    if (!expectedFile.size || !expectedFile.sha512) throw new Error(`missing size/sha512: ${JSON.stringify(expectedFile)}`);
  });
  if (!expectedFile) return;

  assertSafe(autoUpdater, `${baselineVersion} pre-download`);

  // Real download: no mock on downloadUpdate.
  let paths;
  await check(`[${baselineVersion}] downloadUpdate() fetches the real asset bytes (no mock)`, async () => {
    paths = await autoUpdater.downloadUpdate();
    if (!Array.isArray(paths) || !paths.length) throw new Error("downloadUpdate returned no paths");
  });
  if (!paths || !paths.length) return;
  for (const p of paths) downloadedPaths.add(p);

  const downloadedPath = paths[0];
  await check(`[${baselineVersion}] downloaded file exists on disk under the isolated profile`, () => {
    if (!fs.existsSync(downloadedPath)) throw new Error(`missing: ${downloadedPath}`);
    if (!downloadedPath.startsWith(PROFILE)) throw new Error(`SAFETY: downloaded outside isolated profile: ${downloadedPath}`);
  });

  await check(`[${baselineVersion}] downloaded file size matches the manifest`, () => {
    const actual = fs.statSync(downloadedPath).size;
    if (actual !== expectedFile.size) throw new Error(`manifest size=${expectedFile.size} actual=${actual}`);
  });

  await check(`[${baselineVersion}] downloaded bytes' SHA512 matches the manifest (real hash of real bytes, not just the manifest's own claim)`, () => {
    const actual = sha512base64(downloadedPath);
    if (actual !== expectedFile.sha512) throw new Error(`manifest sha512=${expectedFile.sha512} actual=${actual}`);
  });

  log(`[${baselineVersion}] verified downloaded bytes: ${downloadedPath} (${fs.statSync(downloadedPath).size} bytes)`);

  assertSafe(autoUpdater, `${baselineVersion} post-download (no install must follow)`);
}

function findInstallerLikeFiles(dir) {
  const found = [];
  let entries;
  try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch { return found; }
  for (const entry of entries) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) found.push(...findInstallerLikeFiles(full));
    else if (/\.(exe|msi)$/i.test(entry.name)) found.push(full);
  }
  return found;
}

async function cleanupAndVerify() {
  // Belt + suspenders: delete every path downloadUpdate() ever returned,
  // not just the isolated profile root, in case anything resolved outside
  // it despite the guard above having already caught that as a FAIL.
  for (const p of downloadedPaths) {
    try { if (fs.existsSync(p)) fs.rmSync(p, { force: true }); } catch { /* best effort */ }
  }
  try { fs.rmSync(PROFILE, { recursive: true, force: true }); } catch { /* best effort cleanup */ }

  await check("no leftover download paths remain on disk after cleanup", () => {
    const remaining = [...downloadedPaths].filter(p => fs.existsSync(p));
    if (remaining.length) throw new Error(`still present: ${remaining.join(", ")}`);
  });
  // Electron's own disk/GPU cache under the isolated profile can hold a
  // file lock while this process is still alive, so the directory itself
  // may not fully vanish before app.exit() below -- that is harmless browser
  // cache debris, not something that can install. What actually matters is
  // verified precisely: no .exe/.msi survives anywhere under the profile.
  await check("no installer-like (.exe/.msi) files remain under the isolated profile", () => {
    const stray = findInstallerLikeFiles(PROFILE);
    if (stray.length) throw new Error(`still present: ${stray.join(", ")}`);
  });
}

(async () => {
  // electron-updater's network layer (checkForUpdates/downloadUpdate) relies
  // on Electron internals (net/session) that are only available once the app
  // is ready -- without this the real-network calls below hang forever with
  // no error and no timeout, instead of failing fast.
  await app.whenReady();
  log(`update-e2e-github-test.js starting -- target release=${TARGET_VERSION} baselines=${BASELINE_VERSIONS.join(",")} identity=${HARNESS_IDENTITY}`);
  log(`feed: github.com/${pkg.build.publish[0].owner}/${pkg.build.publish[0].repo} (releases/latest)`);
  log("SCOPE: verifies real feed discovery + real downloaded asset bytes (size+SHA512). Does not diff against an actual packaged baseline binary -- none is present in this sandbox.");

  try {
    for (const v of BASELINE_VERSIONS) {
      // eslint-disable-next-line no-await-in-loop -- sequential by design: one
      // real network round-trip + download at a time, not a burst.
      await runForBaseline(v);
    }
  } finally {
    // Runs even if a baseline throws past its own check() wrapper (a bug in
    // the harness itself, not an asserted failure) -- cleanup must not be
    // skippable by an unexpected exception.
    await cleanupAndVerify();
  }

  log(`\n${passed} passed, ${failed} failed${pending ? `, ${pending} pending (candidate not yet published)` : ""}`);

  const artifactsDir = path.join(ROOT, "release-artifacts");
  fs.mkdirSync(artifactsDir, { recursive: true });
  const logPath = path.join(artifactsDir, `update-e2e-log-v${TARGET_VERSION}.txt`);
  fs.writeFileSync(logPath, lines.join("\n") + "\n");
  process.stdout.write(`\nevidence log saved: ${logPath}\n`);

  // Nothing ever opens a window or calls quit, so Electron's event loop
  // would otherwise keep the process alive forever after this IIFE settles.
  // quitAndInstall is guarded to throw above; this is a plain process exit,
  // not electron-updater's install-on-quit path.
  app.exit(failed ? 1 : 0);
})().catch(error => {
  log(`FATAL: ${error.stack || error.message}`);
  cleanupAndVerify().finally(() => app.exit(1));
});
