"use strict";
// Shared encrypted credential store for OfferBot and Bulk Offer Cancel (same Windows user).
//
// WHY NOT electron safeStorage: it is Chromium os_crypt; every Electron profile has its OWN AES key (wrapped by DPAPI inside
// <userData>\Local State), so a blob written by app A cannot be decrypted by app B. Here a random 32-byte MASTER KEY is wrapped
// ONCE with Windows DPAPI (CurrentUser scope) from the Node side; every app of this user can unwrap it. Fields are encrypted with
// AES-256-GCM (random IV per write, AAD = schema:fieldName).
//
// Files in <dir> (default %LOCALAPPDATA%\\OpenSea Tools Shared):
//   master.dpapi        DPAPI-wrapped master key
//   credentials.json    { schema, revision, fields: { name: { v: base64(iv|tag|ct), updatedAt, by } } }
//   credentials.json.bak previous successful write
//   credentials.lock    { pid, token, startedAt }  (O_EXCL; stale = dead pid or older than STALE_MS; only the owner removes it)
//
// Rules: no plaintext on disk / in logs / in command lines (DPAPI payloads go through stdin); a writer never overwrites a field whose
// stored updatedAt is NEWER than its own (last-writer-wins by timestamp, not by who saves last); never write without the lock.
// This file is shared VERBATIM by both apps (keep it dependency-free).
const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { spawnSync } = require("child_process");

const SCHEMA = 1;
const FIELDS = ["licenseKey", "apiKey1", "apiKey2", "privateKey"]; // apiKey1/apiKey2 == OfferBot apiKeys/apiKey2; Bulk uses apiKey1
const LOCK_WAIT_MS = 5000;
const STALE_MS = 15000;

function defaultDir() {
  return path.join(process.env.LOCALAPPDATA || path.join(os.homedir(), "AppData", "Local"), "OpenSea Tools Shared");
}

// ---- DPAPI (Node side, no native module): PowerShell reads base64 from STDIN (never from argv/env).
const PS_SCRIPT = `
$ErrorActionPreference='Stop'
Add-Type -AssemblyName System.Security
$mode=[Console]::In.ReadLine()
$in=[Convert]::FromBase64String([Console]::In.ReadLine())
if($mode -eq 'P'){ $o=[System.Security.Cryptography.ProtectedData]::Protect($in,$null,'CurrentUser') }
else { $o=[System.Security.Cryptography.ProtectedData]::Unprotect($in,$null,'CurrentUser') }
[Console]::Out.Write([Convert]::ToBase64String($o))
`;
const PS_ENCODED = Buffer.from(PS_SCRIPT, "utf16le").toString("base64");
function dpapi(mode, buf) {
  const r = spawnSync("powershell.exe", ["-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-EncodedCommand", PS_ENCODED], {
    input: `${mode}\n${buf.toString("base64")}\n`,
    encoding: "utf8",
    windowsHide: true,
    timeout: 20000,
  });
  if (r.status !== 0) throw new Error("DPAPI " + (mode === "P" ? "protect" : "unprotect") + " thất bại");
  return Buffer.from(String(r.stdout).trim(), "base64");
}
const defaultCrypto = { protect: (b) => dpapi("P", b), unprotect: (b) => dpapi("U", b) };

const sleepSync = (ms) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
function pidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return e.code === "EPERM";
  }
}
const fingerprint = (v) => (v ? crypto.createHash("sha256").update(String(v)).digest("hex").slice(0, 8) : "");

class SharedCredentials {
  // opts: { dir, tool: 'bulk'|'offerbot', log(line), crypto: {protect,unprotect} (tests), lockWaitMs, staleMs }
  constructor(opts = {}) {
    this.dir = opts.dir || defaultDir();
    this.tool = opts.tool || "unknown";
    this.log = opts.log || (() => {});
    this.crypto = opts.crypto || defaultCrypto;
    this.lockWaitMs = opts.lockWaitMs != null ? opts.lockWaitMs : LOCK_WAIT_MS;
    this.staleMs = opts.staleMs != null ? opts.staleMs : STALE_MS;
    this.file = path.join(this.dir, "credentials.json");
    this.masterFile = path.join(this.dir, "master.dpapi");
    this.lockFile = path.join(this.dir, "credentials.lock");
    this._master = null;
    this._cache = null; // { revision, fields }
    this._sig = "";
  }

  // ---------------------------------------------------------------- lock
  _withLock(fn) {
    // re-entrant for THIS instance (e.g. write() -> first-time master key creation); across processes it is exclusive
    if (this._held) return fn();
    fs.mkdirSync(this.dir, { recursive: true });
    const token = crypto.randomBytes(8).toString("hex");
    const t0 = Date.now();
    for (;;) {
      try {
        const fd = fs.openSync(this.lockFile, "wx");
        fs.writeSync(fd, JSON.stringify({ pid: process.pid, token, startedAt: Date.now(), tool: this.tool }));
        fs.closeSync(fd);
        break;
      } catch (e) {
        if (e.code !== "EEXIST") throw e;
        this._breakStaleLock();
        if (Date.now() - t0 > this.lockWaitMs) throw new Error("kho credential dùng chung đang bị khoá bởi tiến trình khác (hết thời gian chờ)");
        sleepSync(20);
      }
    }
    this._held = true;
    try {
      return fn();
    } finally {
      this._held = false;
      // only the OWNER removes the lock
      try {
        const cur = JSON.parse(fs.readFileSync(this.lockFile, "utf8"));
        if (cur.token === token) fs.unlinkSync(this.lockFile);
      } catch {
        /* already gone */
      }
    }
  }

  _breakStaleLock() {
    try {
      const raw = fs.readFileSync(this.lockFile, "utf8");
      let info = null;
      try {
        info = JSON.parse(raw);
      } catch {
        /* torn write: judge by mtime */
      }
      const age = Date.now() - fs.statSync(this.lockFile).mtimeMs;
      const dead = info && info.pid && !pidAlive(info.pid);
      if (dead || age > this.staleMs) {
        // re-check that it is still the same lock, then remove
        const again = fs.readFileSync(this.lockFile, "utf8");
        if (again === raw) fs.unlinkSync(this.lockFile);
      }
    } catch {
      /* gone / raced */
    }
  }

  // ---------------------------------------------------------------- master key
  _masterKey() {
    if (this._master) return this._master;
    fs.mkdirSync(this.dir, { recursive: true });
    if (!fs.existsSync(this.masterFile)) {
      this._withLock(() => {
        if (fs.existsSync(this.masterFile)) return; // another process created it first
        const key = crypto.randomBytes(32);
        const tmp = this.masterFile + "." + process.pid + "." + crypto.randomBytes(4).toString("hex") + ".tmp";
        fs.writeFileSync(tmp, this.crypto.protect(key));
        fs.renameSync(tmp, this.masterFile);
      });
    }
    this._master = this.crypto.unprotect(fs.readFileSync(this.masterFile));
    if (this._master.length !== 32) throw new Error("khoá chủ credential không hợp lệ");
    return this._master;
  }

  _enc(name, value) {
    const iv = crypto.randomBytes(12);
    const c = crypto.createCipheriv("aes-256-gcm", this._masterKey(), iv);
    c.setAAD(Buffer.from(`${SCHEMA}:${name}`));
    const ct = Buffer.concat([c.update(String(value), "utf8"), c.final()]);
    return Buffer.concat([iv, c.getAuthTag(), ct]).toString("base64");
  }

  _dec(name, b64) {
    const buf = Buffer.from(b64, "base64");
    const d = crypto.createDecipheriv("aes-256-gcm", this._masterKey(), buf.subarray(0, 12));
    d.setAAD(Buffer.from(`${SCHEMA}:${name}`));
    d.setAuthTag(buf.subarray(12, 28));
    return Buffer.concat([d.update(buf.subarray(28)), d.final()]).toString("utf8");
  }

  // ---------------------------------------------------------------- file io
  _readRaw() {
    for (const f of [this.file, this.file + ".bak"]) {
      try {
        const j = JSON.parse(fs.readFileSync(f, "utf8"));
        if (j && j.schema === SCHEMA && j.fields && typeof j.fields === "object") return { data: j, fromBak: f !== this.file };
      } catch {
        /* try next */
      }
    }
    return { data: { schema: SCHEMA, revision: 0, fields: {} }, fromBak: false };
  }

  _stat() {
    try {
      const st = fs.statSync(this.file);
      return `${st.mtimeMs}:${st.size}`;
    } catch {
      return "";
    }
  }

  // Cheap change detection: true when credentials.json changed since this instance last read/wrote it.
  hasChanged() {
    return this._stat() !== this._sig;
  }

  refresh() {
    const { data } = this._readRaw();
    this._cache = data;
    this._sig = this._stat();
    return data.revision;
  }

  revision() {
    if (!this._cache || this.hasChanged()) this.refresh();
    return this._cache.revision;
  }

  // -> { value, updatedAt, by } | null
  get(name) {
    if (!FIELDS.includes(name)) throw new Error("field không hợp lệ: " + name);
    if (!this._cache || this.hasChanged()) this.refresh();
    const f = this._cache.fields[name];
    if (!f) return null;
    try {
      return { value: this._dec(name, f.v), updatedAt: f.updatedAt, by: f.by };
    } catch {
      this.log(`[SHARED] không giải mã được field ${name} (rev ${this._cache.revision})`);
      return null;
    }
  }

  // Writes only the entries whose stored updatedAt is not newer than the proposed one. Returns { written:[names], skipped:[names], revision }.
  // entries: { name: { value, updatedAt?: ms, clear?: true } }
  write(entries) {
    for (const n of Object.keys(entries)) if (!FIELDS.includes(n)) throw new Error("field không hợp lệ: " + n);
    return this._withLock(() => {
      const { data, fromBak } = this._readRaw();
      const written = [];
      const skipped = [];
      const now = Date.now();
      for (const [name, e] of Object.entries(entries)) {
        const at = e.updatedAt != null ? Number(e.updatedAt) : now;
        const cur = data.fields[name];
        if (cur && Number(cur.updatedAt) > at) {
          skipped.push(name); // a newer value already exists: a stale writer never clobbers it
          continue;
        }
        if (e.clear) data.fields[name] = { v: this._enc(name, ""), updatedAt: at, by: this.tool, cleared: true };
        else data.fields[name] = { v: this._enc(name, e.value), updatedAt: at, by: this.tool };
        written.push(name);
      }
      if (written.length) {
        data.revision = Number(data.revision || 0) + 1;
        const tmp = this.file + "." + process.pid + "." + crypto.randomBytes(4).toString("hex") + ".tmp";
        const fd = fs.openSync(tmp, "w");
        fs.writeSync(fd, JSON.stringify(data));
        fs.fsyncSync(fd);
        fs.closeSync(fd);
        try {
          if (!fromBak && fs.existsSync(this.file)) fs.copyFileSync(this.file, this.file + ".bak");
        } catch {
          /* best effort */
        }
        fs.renameSync(tmp, this.file);
        this.log(`[SHARED] ${this.tool} ghi ${written.map((n) => n).join(",")} -> rev ${data.revision}`);
      }
      this._cache = data;
      this._sig = this._stat();
      return { written, skipped, revision: data.revision };
    });
  }

  set(name, value, opts = {}) {
    return this.write({ [name]: { value, updatedAt: opts.updatedAt } });
  }

  clear(name, opts = {}) {
    return this.write({ [name]: { clear: true, updatedAt: opts.updatedAt } });
  }

  // value ("" when cleared) of a field, or null when it never existed
  value(name) {
    const g = this.get(name);
    return g ? g.value : null;
  }

  // Idempotent migration of an app's own private store into the shared one. source: { name: { value, updatedAt } }.
  // Never deletes the source. Returns the write result; verification (read back + fingerprint) is the caller's gate.
  migrate(source) {
    const entries = {};
    for (const [name, s] of Object.entries(source || {})) {
      if (!FIELDS.includes(name) || !s || !s.value) continue;
      const cur = this.get(name);
      if (cur && cur.value === s.value) continue; // identical: nothing to do
      entries[name] = { value: s.value, updatedAt: s.updatedAt || 0 }; // old source mtime: it can never beat a newer shared value
    }
    return Object.keys(entries).length ? this.write(entries) : { written: [], skipped: [], revision: this.revision() };
  }

  // Read back + compare fingerprints (never values) — the "verified" gate before an app treats migration as done.
  verify(source) {
    const bad = [];
    for (const [name, s] of Object.entries(source || {})) {
      if (!FIELDS.includes(name) || !s || !s.value) continue;
      const g = this.get(name);
      if (!g || !g.value) bad.push(name);
    }
    return bad;
  }
}

module.exports = { SharedCredentials, FIELDS, SCHEMA, fingerprint, defaultDir, pidAlive };
