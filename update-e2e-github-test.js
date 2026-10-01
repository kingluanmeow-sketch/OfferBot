// 2026-10-01 audit: a prior report called update-test.js "E2E against the
// real GitHub endpoint." That was wrong -- update-test.js intentionally
// mocks `autoUpdater.checkForUpdates`/`downloadUpdate` and never touches the
// network (see the top of that file: "Never let a test reach the network").
// That mock is correct for what update-test.js verifies (IPC/state-machine/
// profile-safety wiring) and must stay mocked there.
//
// THIS file is the real thing: no mocking of checkForUpdates/downloadUpdate,
// real electron-updater, real GitHub Releases endpoint, for representative
// "currently installed version" configurations matching the oldest channels
// still expected to discover updates (v1.25.32, v1.25.33). It does NOT
// install anything -- quitAndInstall is never called, nothing is unzipped
// into a running install -- but it DOES download the real release asset
// bytes into a throwaway temp directory and verifies them.
//
// Not part of `run-all-tests.js` / `npm run test:all`: it depends on network
// reachability to github.com and is meant to run on demand during a release,
// not on every regression pass. Run it with:
//   node run-update-e2e.js          (spawns this under real Electron)
// Its evidence log is written to release-artifacts/update-e2e-log-vX.Y.Z.txt
// so the result is a persisted source artifact, not just terminal output.

"use strict";

const path = require("path");
const fs = require("fs");
const os = require("os");
const crypto = require("crypto");

const ROOT = __dirname;
const PROFILE = fs.mkdtempSync(path.join(os.tmpdir(), "osb-update-e2e-"));
const { app } = require("electron");
app.setPath("userData", PROFILE);

const pkg = require(path.join(ROOT, "package.json"));
const TARGET_VERSION = pkg.version;
// The oldest still-distributed channels this release must remain discoverable
// from. Extend this list as older installers are confirmed end-of-life.
const BASELINE_VERSIONS = ["1.25.32", "1.25.33"];

const lines = [];
function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  lines.push(line);
  process.stdout.write(line + "\n");
}

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; log(`PASS ${name}`); }
  catch (error) { failed++; log(`FAIL ${name}: ${error.message}`); }
}

function sha512base64(filePath) {
  const hash = crypto.createHash("sha512");
  hash.update(fs.readFileSync(filePath));
  return hash.digest("base64");
}

async function runForBaseline(baselineVersion) {
  // electron-updater keeps module-level state; re-require isn't enough to
  // reset it between runs, so each baseline gets delete+fresh-require.
  delete require.cache[require.resolve("electron-updater")];
  const { autoUpdater } = require("electron-updater");

  autoUpdater.logger = { info: m => log(`[eu:${baselineVersion}] ${m}`), warn: m => log(`[eu:${baselineVersion}] WARN ${m}`), error: m => log(`[eu:${baselineVersion}] ERROR ${m}`), debug() {} };
  autoUpdater.autoDownload = false;
  autoUpdater.allowPrerelease = false;
  // Real GitHub endpoint -- no mocking. app.isPackaged is false under this
  // harness, so electron-updater needs an explicit opt-in to actually hit
  // the network instead of silently no-op'ing ("not packed and dev update
  // config is not forced").
  autoUpdater.forceDevUpdateConfig = true;
  autoUpdater.currentVersion = baselineVersion;
  autoUpdater.setFeedURL({
    provider: "github",
    owner: pkg.build.publish[0].owner,
    repo: pkg.build.publish[0].repo,
    releaseType: "release"
  });

  let checkResult;
  await check(`[${baselineVersion}] checkForUpdates() reaches the real GitHub feed and finds an update`, async () => {
    checkResult = await autoUpdater.checkForUpdates();
    if (!checkResult || !checkResult.updateInfo) throw new Error("no updateInfo returned");
  });
  if (!checkResult || !checkResult.updateInfo) return;

  const info = checkResult.updateInfo;
  log(`[${baselineVersion}] discovered version=${info.version} files=${JSON.stringify((info.files || []).map(f => f.url))}`);

  await check(`[${baselineVersion}] discovered version matches this release's package.json`, () => {
    if (info.version !== TARGET_VERSION) throw new Error(`expected ${TARGET_VERSION}, got ${info.version}`);
  });

  const expectedFile = (info.files || [])[0];
  await check(`[${baselineVersion}] manifest names exactly one Windows NSIS asset with size+sha512`, () => {
    if (!expectedFile) throw new Error("no files[] entry in updateInfo");
    if (!expectedFile.size || !expectedFile.sha512) throw new Error(`missing size/sha512: ${JSON.stringify(expectedFile)}`);
  });
  if (!expectedFile) return;

  // Real download: no mock on downloadUpdate. Point the stage dir at our
  // throwaway profile so nothing lands next to a real install.
  let downloadedPaths;
  await check(`[${baselineVersion}] downloadUpdate() fetches the real asset bytes (no mock)`, async () => {
    downloadedPaths = await autoUpdater.downloadUpdate();
    if (!Array.isArray(downloadedPaths) || !downloadedPaths.length) throw new Error("downloadUpdate returned no paths");
  });
  if (!downloadedPaths || !downloadedPaths.length) return;

  const downloadedPath = downloadedPaths[0];
  await check(`[${baselineVersion}] downloaded file exists on disk under the throwaway profile`, () => {
    if (!fs.existsSync(downloadedPath)) throw new Error(`missing: ${downloadedPath}`);
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
}

(async () => {
  log(`update-e2e-github-test.js starting -- target release=${TARGET_VERSION} baselines=${BASELINE_VERSIONS.join(",")}`);
  log(`feed: github.com/${pkg.build.publish[0].owner}/${pkg.build.publish[0].repo} (releases/latest)`);
  for (const v of BASELINE_VERSIONS) {
    // eslint-disable-next-line no-await-in-loop -- sequential by design: one
    // real network round-trip + download at a time, not a burst.
    await runForBaseline(v);
  }

  log(`\n${passed} passed, ${failed} failed`);

  const artifactsDir = path.join(ROOT, "release-artifacts");
  fs.mkdirSync(artifactsDir, { recursive: true });
  const logPath = path.join(artifactsDir, `update-e2e-log-v${TARGET_VERSION}.txt`);
  fs.writeFileSync(logPath, lines.join("\n") + "\n");
  process.stdout.write(`\nevidence log saved: ${logPath}\n`);

  try { fs.rmSync(PROFILE, { recursive: true, force: true }); } catch { /* best effort cleanup */ }

  if (failed) process.exitCode = 1;
})();
