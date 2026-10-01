"use strict";

/**
 * shared-credentials-bridge-test.js - unit tests of the OfferBot <-> shared
 * credential store bridge. Real SharedCredentials (real AES-GCM, lock, atomic
 * write, last-writer-wins) over a TEMP dir, with a FAKE master-key wrapper
 * instead of DPAPI (the real-DPAPI two-process test is
 * shared-credentials-concurrency-test.js). Fake values only; no value is ever
 * printed.
 */

const fs = require("fs");
const os = require("os");
const path = require("path");
const { SharedCredentials } = require("./shared-credentials");
const { SharedBridge, FIELD_MAP } = require("./shared-credentials-bridge");

let pass = 0, fail = 0;
const say = s => process.stdout.write(s + "\n");
const check = (name, ok, detail = "") => {
  if (ok) { pass++; say(`  PASS  ${name}`); } else { fail++; say(`  FAIL  ${name}${detail ? " :: " + detail : ""}`); }
};

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), "bridge-test-"));
process.on("exit", () => { try { fs.rmSync(TMP, { recursive: true, force: true }); } catch { /* busy */ } });
let seq = 0;
const fakeCrypto = { protect: b => Buffer.from(b), unprotect: b => Buffer.from(b) };
const K = n => `k-${n}-${"x".repeat(20)}`;

function world(opts = {}) {
  const id = ++seq;
  const dir = path.join(TMP, `shared-${id}`);
  const metaFile = path.join(TMP, `meta-${id}.json`);
  const logs = [];
  const mkShared = tool => new SharedCredentials({ dir, tool, crypto: fakeCrypto, lockWaitMs: 500, log: () => {} });
  const state = { apiKeys: "", apiKey2: "", privateKey: "", licenseKey: "", ...(opts.local || {}) };
  const applied = [];
  let sharedCalls = 0;
  const bridge = new SharedBridge({
    primary: opts.primary !== false,
    shared: () => { sharedCalls++; return opts.sharedFactory ? opts.sharedFactory(mkShared) : mkShared("offerbot"); },
    local: { get: f => state[f] || "", apply: (f, v) => { applied.push(f); state[f] = v; return true; } },
    canApply: opts.canApply,
    metaFile,
    now: opts.now,
    log: line => logs.push(line)
  });
  return { dir, metaFile, logs, state, applied, bridge, other: () => mkShared("bulk"), calls: () => sharedCalls };
}
const noValuesInLogs = (logs, values) => !logs.some(l => values.some(v => v && l.includes(v)));

// ---------------------------------------------------------------- mapping
say("mapping");
check("apiKeys->apiKey1, apiKey2->apiKey2, privateKey, licenseKey",
  FIELD_MAP.apiKeys === "apiKey1" && FIELD_MAP.apiKey2 === "apiKey2" && FIELD_MAP.privateKey === "privateKey" && FIELD_MAP.licenseKey === "licenseKey" &&
  Object.keys(FIELD_MAP).length === 4);

// ---------------------------------------------------------------- first bootstrap / migration
say("first bootstrap");
{
  const w = world({ local: { apiKeys: K(1), apiKey2: K(2), privateKey: K("pk"), licenseKey: K("lic") } });
  const bulk = w.other();
  bulk.set("apiKey1", K("legacy"), { updatedAt: Date.now() - 1000 }); // a legacy value the Bulk tool pushed earlier
  const r = w.bridge.bootstrap();
  const rd = w.other();
  check("bootstrap ok + first", r.ok && r.first);
  check("OfferBot Key 1 beats a legacy shared apiKey1 (position ownership)", rd.value("apiKey1") === K(1));
  check("apiKey2 / privateKey / licenseKey migrated", rd.value("apiKey2") === K(2) && rd.value("privateKey") === K("pk") && rd.value("licenseKey") === K("lic"));
  check("local store untouched (nothing deleted/changed)", w.state.apiKeys === K(1) && w.state.apiKey2 === K(2) && w.applied.length === 0);
  check("sidecar written with 4 timestamps", Object.keys(JSON.parse(fs.readFileSync(w.metaFile, "utf8")).fields).length === 4);
  check("no value in logs", noValuesInLogs(w.logs, [K(1), K(2), K("pk"), K("lic"), K("legacy")]));
  const rev = rd.revision();
  const again = world(); // second boot of the SAME OfferBot: same dir + meta
  const b2 = new SharedBridge({ primary: true, shared: () => new SharedCredentials({ dir: w.dir, tool: "offerbot", crypto: fakeCrypto, log: () => {} }),
    local: { get: f => w.state[f] || "", apply: () => true }, metaFile: w.metaFile, log: () => {} });
  const r2 = b2.bootstrap();
  check("migration is idempotent (second boot: no write, revision unchanged)", r2.ok && !r2.first && r2.wrote.length === 0 && r2.adopted.length === 0 && w.other().revision() === rev);
  void again;
}
{
  // empty local never overwrites shared; it adopts it
  const w = world({ local: { apiKeys: K(1) } });
  const bulk = w.other();
  bulk.set("apiKey2", K("shared2"));
  bulk.set("privateKey", K("sharedpk"));
  const r = w.bridge.bootstrap();
  check("empty local adopts shared apiKey2 + privateKey", r.adopted.includes("apiKey2") && r.adopted.includes("privateKey") && w.state.apiKey2 === K("shared2") && w.state.privateKey === K("sharedpk"));
  check("empty local did not clear shared", w.other().value("apiKey2") === K("shared2"));
}

// ---------------------------------------------------------------- newer wins, both directions
say("newer wins");
{
  let t = 1_000_000;
  const w = world({ now: () => t, local: { apiKeys: K(1) } });
  w.bridge.bootstrap();
  const bulk = w.other();
  t += 5000;
  bulk.set("apiKey1", K("fromBulk"), { updatedAt: t });
  const adopted = w.bridge.poll();
  check("newer shared value adopted on poll", adopted.includes("apiKeys") && w.state.apiKeys === K("fromBulk"));
  t += 5000;
  w.bridge.push({ apiKeys: K("fromApp") });
  check("user save pushed to shared", w.other().value("apiKey1") === K("fromApp"));
  // a stale writer (older timestamp) cannot clobber
  const r = w.other().write({ apiKey1: { value: K("stale"), updatedAt: t - 100000 } });
  check("stale writer skipped by the store", r.skipped.includes("apiKey1") && w.other().value("apiKey1") === K("fromApp"));
  check("replacement logged by field + fingerprint only", w.logs.some(l => /apiKeys.*vân tay [0-9a-f]{8}/.test(l)) && noValuesInLogs(w.logs, [K(1), K("fromBulk"), K("fromApp")]));
}

// ---------------------------------------------------------------- cleared field propagation + independence
say("clear + independence");
{
  let t = 2_000_000;
  const w = world({ now: () => t, local: { apiKeys: K(1), apiKey2: K(2) } });
  w.bridge.bootstrap();
  t += 1000;
  w.bridge.push({ apiKey2: "" });
  check("clearing Key 2 clears shared apiKey2 only", !w.other().value("apiKey2") && w.other().value("apiKey1") === K(1));
  t += 1000;
  w.other().set("apiKey2", K("again"), { updatedAt: t });
  t += 1000;
  w.other().clear("apiKey1", { updatedAt: t });
  const adopted = w.bridge.poll();
  check("Bulk clearing apiKey1 clears local Key 1 and leaves Key 2 as set by Bulk", adopted.includes("apiKeys") && adopted.includes("apiKey2") && w.state.apiKeys === "" && w.state.apiKey2 === K("again"));
  t += 1000;
  w.other().set("apiKey1", K("b1"), { updatedAt: t });
  w.bridge.poll();
  check("Bulk writing only apiKey1 never touches local Key 2", w.state.apiKeys === K("b1") && w.state.apiKey2 === K("again"));
  t += 1000;
  w.bridge.push({ apiKeys: K("m1") });
  check("OfferBot writing Key 1 never touches shared apiKey2", w.other().value("apiKey2") === K("again"));
  check("push of an unchanged value is not a write (no echo / no revision churn)", (() => {
    const rev = w.other().revision();
    t += 1000;
    w.bridge.push({ apiKeys: K("m1") });
    return w.other().revision() === rev;
  })());
}

// ---------------------------------------------------------------- never swap under a running app
say("canApply gating");
{
  let t = 3_000_000, idle = false;
  const w = world({ now: () => t, local: { apiKeys: K(1), privateKey: K("pkA"), licenseKey: K("licA") },
    canApply: (f, ctx) => ctx.boot || (f === "licenseKey" ? !w.state.licenseKey : idle) });
  w.bridge.bootstrap();
  t += 1000;
  const bulk = w.other();
  bulk.set("apiKey1", K("n1"), { updatedAt: t });
  bulk.set("privateKey", K("pkB"), { updatedAt: t });
  bulk.set("licenseKey", K("licB"), { updatedAt: t });
  const r1 = w.bridge.poll();
  check("busy: nothing adopted, key not swapped", r1.length === 0 && w.state.apiKeys === K(1) && w.state.privateKey === K("pkA") && w.state.licenseKey === K("licA"));
  check("busy: shared values stay pending (retried each beat)", w.bridge.pending.size === 3);
  idle = true;
  const r2 = w.bridge.poll();
  check("idle: apiKeys + privateKey adopted", r2.includes("apiKeys") && r2.includes("privateKey") && w.state.apiKeys === K("n1") && w.state.privateKey === K("pkB"));
  check("licence still only-when-empty (held licence kept)", !r2.includes("licenseKey") && w.state.licenseKey === K("licA"));
  w.state.licenseKey = "";
  check("licence adopted once this window has none", w.bridge.poll().includes("licenseKey") && w.state.licenseKey === K("licB"));
}

// ---------------------------------------------------------------- NON-primary slot
say("non-primary slot");
{
  const w = world({ primary: false, local: { apiKeys: K(1), privateKey: K("own"), licenseKey: K("lic") } });
  const seed = w.other();
  seed.set("privateKey", K("sharedpk"));
  seed.set("apiKey1", K("sharedk1"));
  const revBefore = seed.revision();
  const r = w.bridge.bootstrap();
  w.bridge.push({ privateKey: K("other"), apiKeys: K("other1"), licenseKey: K("l2") });
  const polled = w.bridge.poll();
  check("bootstrap is inert", r.ok === false && r.wrote.length === 0 && r.adopted.length === 0);
  check("the shared store is never even constructed", w.calls() === 0);
  check("shared store unchanged (privateKey/apiKeys never written)", w.other().revision() === revBefore && w.other().value("privateKey") === K("sharedpk"));
  check("local wallet/keys never replaced", polled.length === 0 && w.state.privateKey === K("own") && w.state.apiKeys === K(1) && w.applied.length === 0);
  check("no sidecar file created", !fs.existsSync(w.metaFile));
}

// ---------------------------------------------------------------- wallet-profiles never shared
say("wallet-profiles");
{
  const src = fs.readFileSync(path.join(__dirname, "shared-credentials-bridge.js"), "utf8");
  check("bridge never references wallet-profiles", !/wallet-profiles|WalletProfiles/.test(src.replace(/\/\*[\s\S]*?\*\//g, "")));
  check("shared-credentials.js has no wallet-profile field", !/wallet-profiles/.test(fs.readFileSync(path.join(__dirname, "shared-credentials.js"), "utf8")));
}

// ---------------------------------------------------------------- failure tolerance
say("failure tolerance");
{
  let t = 4_000_000;
  const throwing = {
    refresh() { throw new Error("DPAPI unprotect thất bại"); }, get() { throw new Error("x"); }, write() { throw new Error("kho khoá"); },
    verify() { return []; }, hasChanged() { throw new Error("stat"); }, revision() { return 0; }
  };
  const w = world({ now: () => t, sharedFactory: () => throwing, local: { apiKeys: K(1) } });
  let threw = false, r, p, q;
  try { r = w.bridge.bootstrap(); t += 11 * 60 * 1000; p = w.bridge.poll(); q = w.bridge.push({ apiKeys: K(2) }); } catch { threw = true; }
  check("a broken shared store never throws out of bootstrap/poll/push", !threw && r && r.ok === false && Array.isArray(p) && q === null);
  check("local value untouched on failure", w.state.apiKeys === K(1));
  check("redacted failure lines logged, no value", w.logs.some(l => /\[SHARED\].*FAILED/.test(l)) && noValuesInLogs(w.logs, [K(1), K(2)]));
  check("failed push remembered for retry", w.bridge.dirty === true);

  // recovery: a later healthy store receives the local change that never made it
  let healthy = false;
  const real = world({ now: () => t, local: { apiKeys: K(1) } });
  const flaky = { _s: null, get s() { return this._s || (this._s = real.other()); } };
  const w2 = world({ now: () => t, local: { apiKeys: K(1) }, sharedFactory: () => new Proxy({}, { get: (_o, k) => {
    if (!healthy) throw new Error("kho tạm thời không dùng được");
    return flaky.s[k].bind ? flaky.s[k].bind(flaky.s) : flaky.s[k];
  } }) });
  void flaky;
  w2.bridge.bootstrap();
  w2.state.apiKeys = K(7); // the user's save lands in OfferBot's own store first
  w2.bridge.push({ apiKeys: K(7) });
  healthy = true;
  t += 11 * 60 * 1000;
  w2.bridge.poll();
  check("after the store recovers the pending local change is delivered", flaky.s.value("apiKey1") === K(7));
}

// ---------------------------------------------------------------- lock timeout is not fatal
say("lock timeout");
{
  const w = world({ local: { apiKeys: K(1) } });
  fs.mkdirSync(w.dir, { recursive: true });
  fs.writeFileSync(path.join(w.dir, "credentials.lock"), JSON.stringify({ pid: process.pid, token: "t", startedAt: Date.now() }));
  const t0 = Date.now();
  const r = w.bridge.bootstrap();
  check("held lock: bootstrap returns (not ok) within the wait budget, no throw", !r.ok && Date.now() - t0 < 8000);
  fs.unlinkSync(path.join(w.dir, "credentials.lock"));
}

say(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
