"use strict";

/**
 * wallet-profiles-test.js - NATIVE contract test of per-Tool wallet profiles
 * (wallet-profiles.js) and of the rule "a non-primary slot never touches the
 * shared credential store". Runs directly on this tree (no copy). Fake keys,
 * fake safeStorage, temp dirs only; no secret is ever printed.
 */

const fs = require("fs");
const os = require("os");
const path = require("path");
const { WalletProfiles, MAX_PROFILES } = require("./wallet-profiles");
const { SharedCredentials } = require("./shared-credentials");
const { SharedBridge } = require("./shared-credentials-bridge");

let pass = 0, fail = 0;
const say = s => process.stdout.write(s + "\n");
const check = (name, ok, detail = "") => {
  if (ok) { pass++; say(`  PASS  ${name}`); } else { fail++; say(`  FAIL  ${name}${detail ? " :: " + detail : ""}`); }
};

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), "wallet-prof-test-"));
process.on("exit", () => { try { fs.rmSync(TMP, { recursive: true, force: true }); } catch { /* busy */ } });

// Fake DPAPI: reversible, but the stored text is NOT the plain key.
const safeStorage = {
  isEncryptionAvailable: () => true,
  encryptString: s => Buffer.from([...Buffer.from(String(s))].map(b => b ^ 0x5a)),
  decryptString: b => Buffer.from([...b].map(x => x ^ 0x5a)).toString()
};
const derive = key => {
  const m = /^0xkey([0-9a-z]+)$/.exec(String(key).trim());
  return m ? `0xaddr${m[1]}` : null;
};
const KEY = n => `0xkey${n}`;
const mk = (name, ss = safeStorage) => new WalletProfiles({ file: path.join(TMP, name, "wallet-profiles.json"), safeStorage: ss, deriveAddress: derive });

// ---------------------------------------------------------------- per-Tool profiles
say("profiles contract");
{
  const p = mk("slot1");
  check("empty by default", p.list().length === 0 && p.activeId() === null);
  const a = p.upsertActive(KEY("a1"), "main");
  check("upsert returns id+address", a.ok && a.address === "0xaddra1" && typeof a.id === "string");
  const b = p.upsertActive(KEY("b2"));
  check("second key becomes the active one", b.ok && p.activeId() === b.id && p.list().filter(x => x.active).length === 1);
  const again = p.upsertActive(KEY("a1"));
  check("same key refreshes, not duplicates", again.id === a.id && p.list().length === 2 && p.activeId() === a.id);
  check("list() never exposes a key", p.list().every(x => !("key" in x)) && !JSON.stringify(p.list()).includes("key" + "a1"));
  check("stored file holds no plaintext key", !fs.readFileSync(p.file, "utf8").includes(KEY("a1")) && !fs.readFileSync(p.file, "utf8").includes(KEY("b2")));
  check("keyOf decrypts the right profile", p.keyOf(a.id) === KEY("a1") && p.keyOf(b.id) === KEY("b2") && p.keyOf("nope") === null);
  check("active profile cannot be removed", p.remove(a.id).ok === false && p.list().length === 2);
  check("inactive profile can be removed", p.remove(b.id).ok === true && p.list().length === 1);
  check("remove unknown id fails", p.remove("nope").ok === false);
  p.clearActive();
  check("clearActive leaves profiles, no active", p.activeId() === null && p.list().length === 1);
  check("invalid key rejected", p.upsertActive("garbage").ok === false);
  const noSs = mk("slot1", { isEncryptionAvailable: () => false, encryptString: () => { throw new Error("x"); }, decryptString: () => { throw new Error("x"); } });
  check("no secure storage -> upsert refused, nothing written", noSs.upsertActive(KEY("z9")).ok === false && noSs.list().length === 1);
  const big = mk("slotbig");
  let last;
  for (let i = 0; i < MAX_PROFILES + 3; i++) last = big.upsertActive(KEY("n" + i));
  check("profile count capped", big.list().length === MAX_PROFILES && last.ok === false);
  fs.writeFileSync(path.join(TMP, "slotbad.json"), "{not json");
  const bad = new WalletProfiles({ file: path.join(TMP, "slotbad.json"), safeStorage, deriveAddress: derive });
  check("corrupt file reads as empty", bad.list().length === 0 && bad.activeId() === null);
}

say("per-Tool isolation");
{
  const s1 = mk("iso1"), s2 = mk("iso2");
  s1.upsertActive(KEY("11"));
  s2.upsertActive(KEY("22"));
  s2.upsertActive(KEY("23"));
  check("each Tool has its own file and list", s1.list().length === 1 && s2.list().length === 2 && s1.file !== s2.file);
  s1.clearActive();
  check("changing Tool 1 never changes Tool 2", s2.activeId() !== null && s2.list().length === 2);
  check("a profile id of Tool 2 is unknown to Tool 1", s1.keyOf(s2.list()[0].id) === null);
}

// ---------------------------------------------------------------- shared store never reached by non-primary
say("non-primary slot never touches the shared store");
{
  const dir = path.join(TMP, "shared-np");
  let factoryCalls = 0;
  const factory = () => { factoryCalls++; return new SharedCredentials({ dir, tool: "offerbot", crypto: { protect: b => Buffer.from(b), unprotect: b => Buffer.from(b) }, lockWaitMs: 300, log: () => {} }); };
  const state = { apiKeys: "ka", apiKey2: "kb", privateKey: KEY("np"), licenseKey: "lic" };
  const applied = [];
  const meta = path.join(TMP, "np-meta.json");
  const bridge = new SharedBridge({
    primary: false, shared: factory, metaFile: meta,
    local: { get: f => state[f] || "", apply: f => { applied.push(f); return true; } },
    log: () => {}
  });
  const boot = bridge.bootstrap();
  const polled = bridge.poll();
  const pushed = bridge.push({ privateKey: KEY("np2"), apiKeys: "k9", apiKey2: "", licenseKey: "l" });
  check("bootstrap/poll/push are inert", boot.ok === false && polled.length === 0 && pushed === null);
  check("factory never called, no shared dir created", factoryCalls === 0 && !fs.existsSync(dir));
  check("no sidecar written, local untouched", !fs.existsSync(meta) && applied.length === 0 && state.privateKey === KEY("np"));
  check("bridge is not enabled", bridge.enabled === false);
}

// ---------------------------------------------------------------- primary: only its own wallet; profiles never shared
say("primary shares only primary wallet; wallet-profiles stay local");
{
  const dir = path.join(TMP, "shared-p");
  const fakeCrypto = { protect: b => Buffer.from(b), unprotect: b => Buffer.from(b) };
  const mkShared = tool => new SharedCredentials({ dir, tool, crypto: fakeCrypto, lockWaitMs: 500, log: () => {} });
  const profiles1 = mk("pri1");
  const other = mk("pri2");                       // another Tool's profiles
  other.upsertActive(KEY("other"));
  const otherBefore = fs.readFileSync(other.file, "utf8");
  const state = { apiKeys: "", apiKey2: "", privateKey: "", licenseKey: "" };
  const logs = [];
  const bridge = new SharedBridge({
    primary: true, shared: () => mkShared("offerbot"), metaFile: path.join(TMP, "pri-meta.json"),
    local: {
      get: f => state[f] || "",
      apply: (f, v) => { state[f] = v; if (f === "privateKey") profiles1.upsertActive(v); return true; }
    },
    log: l => logs.push(l)
  });
  bridge.bootstrap();
  const bulk = mkShared("bulk");
  bulk.refresh();
  bulk.write({ privateKey: { value: KEY("frombulk"), updatedAt: Date.now() + 5000 } });
  bridge.poll();
  check("primary adopts the shared wallet into ITS profiles", state.privateKey === KEY("frombulk") && profiles1.list().length === 1);
  check("another Tool's wallet-profiles file is untouched", fs.readFileSync(other.file, "utf8") === otherBefore);
  check("shared dir holds only the credential store, no wallet-profiles", !fs.readdirSync(dir).some(f => /wallet-profiles/i.test(f)));
  check("no secret value in logs", !logs.some(l => l.includes(KEY("frombulk"))));
  const s = mkShared("probe"); s.refresh();
  check("shared store has privateKey but no profile list", Boolean(s.get("privateKey")) && !JSON.stringify(Object.keys(s.get("privateKey") || {})).includes("profiles"));
}

// ---------------------------------------------------------------- source pins
say("source pins");
{
  const wp = fs.readFileSync(path.join(__dirname, "wallet-profiles.js"), "utf8");
  const br = fs.readFileSync(path.join(__dirname, "shared-credentials-bridge.js"), "utf8");
  const main = fs.readFileSync(path.join(__dirname, "main.js"), "utf8");
  check("wallet-profiles.js does not know the shared store", !/shared-credentials/.test(wp));
  check("bridge never references wallet-profiles", !/wallet-profiles/.test(br.replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/.*$/gm, "")));
  check("main builds wallet-profiles in the slot's own settings dir", /path\.join\(location\.directory, "wallet-profiles\.json"\)/.test(main));
  check("sharedBridge is only constructed behind the primary guard", /if \(!primary\) return;[\s\S]*new SharedBridge\(/.test(main) && (main.match(/new SharedBridge\(/g) || []).length === 1);
  check("wallet:activate goes through bot:saveSettings (same path as typing a key)", /localIpcHandlers\.get\("bot:saveSettings"\)[\s\S]{0,120}privateKey: key/.test(main));
}

say(`\nwallet-profiles-test: ${pass} pass, ${fail} fail`);
process.exit(fail ? 1 : 0);
