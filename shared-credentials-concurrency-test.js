"use strict";

/**
 * shared-credentials-concurrency-test.js - two REAL processes, REAL Windows
 * DPAPI, a TEMP shared directory (never %LOCALAPPDATA%\OpenSea Tools Shared).
 *
 *   A = OfferBot (through SharedBridge): saves Key 1 / wallet repeatedly
 *   B = the other tool (raw SharedCredentials): writes Key 2 repeatedly
 *
 * Both start together on an EMPTY directory, so they also race to create the
 * DPAPI-wrapped master key. Afterwards: no lost update, no corrupt file, one
 * consistent master key, revision accounts for every write, and the independent
 * fields each hold their writer's last value. Fake values only; nothing printed.
 */

const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawn } = require("child_process");

if (process.argv[2] === "--child") {
  const [, , , role, dir, startFile, countStr, metaFile] = process.argv;
  const { SharedCredentials } = require("./shared-credentials");
  const { SharedBridge } = require("./shared-credentials-bridge");
  const count = Number(countStr);
  const deadline = Date.now() + 30000;
  while (!fs.existsSync(startFile) && Date.now() < deadline) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5);
  const shared = new SharedCredentials({ dir, tool: role, lockWaitMs: 15000 });
  if (role === "offerbot") {
    const state = { apiKeys: "", apiKey2: "", privateKey: "", licenseKey: "" };
    const bridge = new SharedBridge({ primary: true, shared, local: { get: f => state[f], apply: (f, v) => { state[f] = v; return true; } }, metaFile, log: () => {} });
    for (let i = 1; i <= count; i++) {
      state.apiKeys = `A-key1-${i}`;
      state.privateKey = `A-wallet-${i}`;
      const r = bridge.push({ apiKeys: state.apiKeys, privateKey: state.privateKey });
      if (!r) { process.stderr.write("push failed\n"); process.exit(3); }
    }
  } else {
    for (let i = 1; i <= count; i++) shared.set("apiKey2", `B-key2-${i}`);
  }
  process.exit(0);
}

let pass = 0, fail = 0;
const say = s => process.stdout.write(s + "\n");
const check = (name, ok, detail = "") => {
  if (ok) { pass++; say(`  PASS  ${name}`); } else { fail++; say(`  FAIL  ${name}${detail ? " :: " + detail : ""}`); }
};

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), "shared-conc-"));
process.on("exit", () => { try { fs.rmSync(TMP, { recursive: true, force: true }); } catch { /* busy */ } });
const real = (process.env.LOCALAPPDATA || "").toLowerCase();
if (!TMP.toLowerCase().startsWith(os.tmpdir().toLowerCase()) || (real && path.join(real, "opensea tools shared") === TMP.toLowerCase())) {
  say("ABORT: temp dir is not under the OS temp dir"); process.exit(2);
}

const dir = path.join(TMP, "shared");
const startFile = path.join(TMP, "go");
const N = 12;

function run(role) {
  return new Promise(resolve => {
    const child = spawn(process.execPath, [__filename, "--child", role, dir, startFile, String(N), path.join(TMP, `meta-${role}.json`)],
      { stdio: ["ignore", "ignore", "pipe"], windowsHide: true });
    let err = "";
    child.stderr.on("data", d => { err += d; });
    child.on("exit", code => resolve({ code, err }));
  });
}

(async () => {
  const a = run("offerbot");
  const b = run("bulk");
  await new Promise(r => setTimeout(r, 1500)); // both children are waiting at the barrier
  fs.writeFileSync(startFile, "go");
  const [ra, rb] = await Promise.all([a, b]);
  check("both writer processes finished cleanly", ra.code === 0 && rb.code === 0, `${ra.code}/${rb.code} ${ra.err.slice(0, 120)} ${rb.err.slice(0, 120)}`);

  const { SharedCredentials } = require("./shared-credentials");
  const reader = new SharedCredentials({ dir, tool: "reader" });
  check("Key 1 = OfferBot's last write (real DPAPI + AES-GCM round trip)", reader.value("apiKey1") === `A-key1-${N}`);
  check("wallet = OfferBot's last write", reader.value("privateKey") === `A-wallet-${N}`);
  check("Key 2 = the other tool's last write (independent field, not clobbered)", reader.value("apiKey2") === `B-key2-${N}`);
  // N pushes of 2 fields in ONE write each + N writes of Key 2 = 2N revisions (the master-key race adds none).
  check("revision accounts for every write (no lost update)", reader.revision() === 2 * N, `rev=${reader.revision()}`);
  const raw = JSON.parse(fs.readFileSync(path.join(dir, "credentials.json"), "utf8"));
  check("credentials.json is valid and holds exactly the written fields", Object.keys(raw.fields).sort().join(",") === "apiKey1,apiKey2,privateKey");
  check("no plaintext in the shared files", !/A-key1-|A-wallet-|B-key2-/.test(
    ["credentials.json", "credentials.json.bak", "master.dpapi"].filter(f => fs.existsSync(path.join(dir, f))).map(f => fs.readFileSync(path.join(dir, f), "latin1")).join("")));
  check("lock released", !fs.existsSync(path.join(dir, "credentials.lock")));
  check("a single master key file", fs.readdirSync(dir).filter(f => f.startsWith("master.dpapi")).join(",") === "master.dpapi");

  say(`\n${pass} passed, ${fail} failed`);
  process.exit(fail ? 1 : 0);
})();
