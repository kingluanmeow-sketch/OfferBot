"use strict";

/**
 * shared-credentials-hooks-test.js - source pins for the shared credential
 * hooks in main.js and the packaging list. These are contracts: removing a
 * hook silently would stop OfferBot syncing with the other tool, and widening
 * one (non-primary slot, wallet-profiles) would break the owner's decisions.
 */

const fs = require("fs");
const path = require("path");

let pass = 0, fail = 0;
const say = s => process.stdout.write(s + "\n");
const check = (name, ok) => { if (ok) { pass++; say(`  PASS  ${name}`); } else { fail++; say(`  FAIL  ${name}`); } };

const main = fs.readFileSync(path.join(__dirname, "main.js"), "utf8");
const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, "package.json"), "utf8"));
const fnBody = (src, name) => {
  const start = src.indexOf(`function ${name}(`);
  if (start < 0) return "";
  const next = src.indexOf("\nfunction ", start + 10);
  return src.slice(start, next < 0 ? undefined : next);
};

check("main requires the shared store and the bridge", /require\("\.\/shared-credentials"\)/.test(main) && /require\("\.\/shared-credentials-bridge"\)/.test(main));
check("both modules ship in the installer", ["shared-credentials.js", "shared-credentials-bridge.js"].every(f => pkg.build.files.includes(f)));

const init = fnBody(main, "initSharedCredentials");
check("bridge is built for the PRIMARY slot only (slot 1, not overflow)", /self\.slot === 1 && !self\.overflow/.test(init) && /if \(!primary\) return;/.test(init));
check("dev runtime never opens the production shared store", /devRuntime\.isDev\(\)/.test(init));
check("test dir override is refused in a packaged build", /override && !app\.isPackaged/.test(init));
check("shared tool tag is offerbot", /tool: "offerbot"/.test(init));
check("sidecar timestamps live beside the primary settings, not in secrets.json", /path\.join\(self\.primarySettingsBase, "shared-sync-meta\.json"\)/.test(init));
check("key positions are settled before the first compare", init.indexOf("normalizeApiKeySlots()") > -1 && init.indexOf("normalizeApiKeySlots()") < init.indexOf("new SharedBridge"));
check("canApply: licence only-when-empty, keys/wallet only when nothing runs", /field === "licenseKey"\) return !String\(db\.getSettings\(\)\.licenseKey/.test(init) && /describeWorkInFlight\(\)\.length === 0/.test(init));
check("wallet-profiles is never shared (no sharing call touches wallet-profiles.json)", !/shared[^\n]*wallet-profiles\.json/i.test(main));

const bootIdx = main.indexOf("try { initSharedCredentials(); }");
check("boot hook runs after the licence vault logic and BEFORE license.hydrated(true)", bootIdx > main.indexOf("licenceVault.save({ licenseKey: held })") && bootIdx < main.indexOf("license.hydrated(true)"));
check("boot hook is failure-tolerant", /try \{ initSharedCredentials\(\); \} catch/.test(main));

const save = main.slice(main.indexOf('ipcMain.handle("bot:saveSettings"'), main.indexOf('ipcMain.handle("wallet:state"'));
check("saveSettings pushes AFTER normalizeApiKeySlots (final Key 1/Key 2 positions)", save.indexOf("sharedPush(touched)") > save.indexOf("normalizeApiKeySlots()") && save.indexOf("normalizeApiKeySlots()") > -1);
check("saveSettings pushes only on a successful secret save", /settings && secretResult\.ok/.test(save));
check("wallet:activate still goes through saveSettings (so it syncs for free)", /localIpcHandlers\.get\("bot:saveSettings"\)/.test(main));
const act = main.slice(main.indexOf('ipcMain.handle("bot:activateApi"'), main.indexOf('ipcMain.handle("bot:saveSettings"'));
check("API key activation pushes the activated slot's field", /sharedPush\(\{ \[field\]: key \}\)/.test(act));
check("clearing Key 2 pushes ONLY apiKey2", /sharedPush\(\{ apiKey2: "" \}\)/.test(act));
check("licence activation pushes the licence", /secrets\.save\(\{ licenseKey: payload\.key \}\);\s*sharedPush\(\{ licenseKey: payload\.key \}\)/.test(main));
check("normalizeApiKeySlots publishes the positions it changed", /credentialDefaults\.save\(changed\);\s*sharedPush\(changed\);/.test(main));
check("push is a no-op while adopting (no echo) and never throws", /if \(!sharedBridge \|\| sharedAdopting\) return;/.test(main) && /try \{ sharedBridge\.push\(values\); \} catch/.test(main));
check("poll rides the EXISTING licence beat (no new timer)", /try \{ sharedPoll\(\); \} catch[^\n]*\n\s*try \{ adoptSharedLicence\(\); \}/.test(main) && !/setInterval\([^)]*sharedPoll/.test(main));
check("adoptSharedLicence only-when-empty rule is intact", /if \(String\(db\.getSettings\(\)\.licenseKey \|\| ""\)\.trim\(\)\) return false;/.test(main));

const poll = fnBody(main, "sharedPoll");
check("adopted wallet rebinds through the same path as a manual wallet change", /engine\.rebindWallet\(applied\)/.test(poll) && /openseaSession\.walletChanged\(walletAfter\)/.test(poll));

say(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
