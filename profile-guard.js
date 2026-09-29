"use strict";

/**
 * profile-guard.js — a SANDBOXED test launch must never open the production profile.
 *
 * INCIDENT (2026-09-23): packaged-EXE tests isolated LOCALAPPDATA but not
 * userData. Electron resolves userData from the Windows known folder (the
 * APPDATA env var is ignored), so every test EXE opened the owner's real
 * %APPDATA%\opensea-offer-bot\offerbot-snapshot.json — the primary NFT/settings
 * DB — and overwrote the production NFT list with test rows.
 *
 * Tests now always launch through test-exe-sandbox.js, which passes
 * --user-data-dir under the test's temp directory AND sets OSB_TEST_SANDBOX=1.
 * This module is the in-app last line of defence: when that marker is present,
 * the app refuses to start if its userData or settings base resolves to a
 * production location — BEFORE instance/DB setup can write anything.
 *
 * Real users never set OSB_TEST_SANDBOX, so for them this is a no-op.
 * OSB_TEST_FORBID_PATHS can only ADD forbidden paths (it never allows one):
 * it exists so the regression can prove the abort path end-to-end without
 * ever pointing the app at the real profile.
 */

const path = require("path");

const norm = p => path.resolve(String(p || "")).replace(/[\\/]+$/, "").toLowerCase();
const within = (p, root) => { const a = norm(p), b = norm(root); return Boolean(b) && (a === b || a.startsWith(b + path.sep) || a.startsWith(b + "-")); };

/**
 * @returns {{ok:true}|{ok:false, reason:string}}
 */
function checkSandboxedProfile({ env = process.env, userData, appData, localAppDataKnown, appName = "opensea-offer-bot" } = {}) {
  if (env.OSB_TEST_SANDBOX !== "1") return { ok: true, sandboxed: false };
  const forbidden = [
    appData && path.join(appData, appName),                           // %APPDATA%\opensea-offer-bot (+ "-N" slots)
    appData && path.join(appData, "OpenSea Offer Bot"),
    localAppDataKnown && path.join(localAppDataKnown, "OpenSea Offer Bot")
  ].filter(Boolean);
  for (const extra of String(env.OSB_TEST_FORBID_PATHS || "").split(";").map(s => s.trim()).filter(Boolean)) forbidden.push(extra);
  for (const root of forbidden) {
    if (within(userData, root)) return { ok: false, reason: `userData ${userData} is inside protected ${root}` };
  }
  const localEnv = env.LOCALAPPDATA;
  if (!localEnv) return { ok: false, reason: "sandboxed launch without a LOCALAPPDATA override" };
  for (const root of forbidden) {
    if (within(path.join(localEnv, "OpenSea Offer Bot"), root)) return { ok: false, reason: `settings base inside protected ${root}` };
  }
  if (localAppDataKnown && norm(localEnv) === norm(localAppDataKnown)) {
    return { ok: false, reason: "sandboxed launch uses the real LOCALAPPDATA" };
  }
  return { ok: true, sandboxed: true };
}

module.exports = { checkSandboxedProfile, within };
