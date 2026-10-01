"use strict";

/**
 * shared-credentials-bridge.js - keeps OfferBot's own encrypted store and the
 * machine-wide SHARED credential store (shared-credentials.js, also used by
 * Bulk Offer Cancel) in step.
 *
 * Pure logic with injected dependencies, so it is unit-testable without
 * Electron or DPAPI. main.js wires the real SharedCredentials, SecretStore/db
 * and the slot identity; nothing here touches a path except the sidecar below.
 *
 * MAPPING (by key POSITION - OfferBot's 2-key semantics are untouched)
 *
 *   OfferBot apiKeys     <->  shared apiKey1     (Key 1)
 *   OfferBot apiKey2     <->  shared apiKey2     (Key 2)
 *   OfferBot privateKey  <->  shared privateKey  (PRIMARY slot's active wallet)
 *   OfferBot licenseKey  <->  shared licenseKey
 *
 * apiKey1 and apiKey2 are two INDEPENDENT fields: clearing or changing one
 * never touches the other.
 *
 * PRIMARY SLOT ONLY. A bridge built with `primary: false` is inert: it never
 * constructs the shared store and never reads or writes it, so a non-primary
 * Tool keeps its own wallet and keys exactly as before.
 *
 * TIMESTAMPS live in a small NON-secret sidecar (shared-sync-meta.json:
 * { v, fields: { apiKeys: ms, ... } }), because SecretStore only holds a
 * whitelist of secret fields. A timestamp says WHEN the local copy was last
 * changed by the user (or adopted from the shared store); the newer side wins.
 *
 * FIRST BOOTSTRAP (no sidecar yet): OfferBot owns the meaning of the key
 * positions, so every NON-EMPTY local field is stamped "now" and wins over a
 * legacy shared value. An EMPTY local field never overwrites a shared value;
 * it adopts it. After that, plain last-writer-wins by timestamp.
 *
 * NEVER LOSE A KEY: when a shared value replaces a DIFFERENT non-empty local
 * value, the field name and a sha256-8 fingerprint of the replaced value are
 * logged (never the value). No history copy is kept: a second secret store
 * would double the places a key lives, and the wallet is already preserved by
 * wallet-profiles.json.
 *
 * Failure of the shared store (lock timeout, DPAPI error, corrupt file) never
 * propagates: it is logged as a redacted line and OfferBot continues on its
 * own store. Values are never logged - only field names, counts, revisions.
 */

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

/** local (OfferBot) field -> shared field */
const FIELD_MAP = Object.freeze({
  apiKeys: "apiKey1",
  apiKey2: "apiKey2",
  privateKey: "privateKey",
  licenseKey: "licenseKey"
});
const LOCAL_FIELDS = Object.freeze(Object.keys(FIELD_MAP));
const META_VERSION = 1;
const RETRY_MS = 10 * 60 * 1000;

const fingerprint = value => (value ? crypto.createHash("sha256").update(String(value)).digest("hex").slice(0, 8) : "");
const clean = value => String(value == null ? "" : value).trim();

class SharedBridge {
  /**
   * @param {object} o
   * @param {boolean} o.primary         only the primary slot syncs
   * @param {object|Function} o.shared  SharedCredentials, or a factory for it (never called when !primary)
   * @param {{get:(f:string)=>string, apply:(f:string,v:string)=>boolean}} o.local
   * @param {(f:string, ctx:{boot:boolean})=>boolean} [o.canApply]  may a newer shared value replace the local one right now?
   * @param {string} o.metaFile
   * @param {()=>number} [o.now]
   * @param {(line:string)=>void} [o.log]
   */
  constructor({ primary, shared, local, canApply, metaFile, now, log }) {
    this.primary = Boolean(primary);
    this._sharedSource = shared;
    this.shared = null;
    this.local = local;
    this.canApply = typeof canApply === "function" ? canApply : () => true;
    this.metaFile = metaFile;
    this.now = typeof now === "function" ? now : Date.now;
    this.log = typeof log === "function" ? log : () => {};
    this.meta = { v: META_VERSION, fields: {} };
    this.metaExisted = false;
    this.dirty = false;            // a local change could not reach the shared store yet
    this.pending = new Set();      // newer shared values the running app may not take yet
    this._declinedLogged = new Set();
    this.ready = false;
  }

  get enabled() { return this.primary && this.ready; }

  // ---------------------------------------------------------------- plumbing

  _store() {
    if (!this.primary) return null;
    if (!this.shared) {
      this.shared = typeof this._sharedSource === "function" ? this._sharedSource() : this._sharedSource;
    }
    return this.shared;
  }

  _safe(op, fn, fallback) {
    try {
      return fn();
    } catch (error) {
      // Message of the shared store / fs only - none of them carries a value.
      this.log(`[SHARED] ${op}=FAILED ${String((error && error.message) || error).slice(0, 160)} - OfferBot tiếp tục bằng kho riêng`);
      return fallback;
    }
  }

  _loadMeta() {
    this.metaExisted = false;
    try {
      const parsed = JSON.parse(fs.readFileSync(this.metaFile, "utf8"));
      if (parsed && typeof parsed === "object" && parsed.fields && typeof parsed.fields === "object") {
        this.meta = { v: META_VERSION, fields: {} };
        for (const f of LOCAL_FIELDS) {
          const t = Number(parsed.fields[f]);
          if (Number.isFinite(t) && t > 0) this.meta.fields[f] = t;
        }
        this.metaExisted = true;
      }
    } catch { /* absent or damaged: treated as first bootstrap */ }
  }

  _saveMeta() {
    try {
      fs.mkdirSync(path.dirname(this.metaFile), { recursive: true });
      const tmp = `${this.metaFile}.${process.pid}.${crypto.randomBytes(3).toString("hex")}.tmp`;
      fs.writeFileSync(tmp, JSON.stringify(this.meta), "utf8");
      fs.renameSync(tmp, this.metaFile);
      this.metaExisted = true;
    } catch (error) {
      this.log(`[SHARED] meta=FAILED ${String(error.message).slice(0, 120)}`);
    }
  }

  _ts(field) { return this.meta.fields[field] || 0; }

  // ---------------------------------------------------------------- core

  /**
   * One reconciliation pass over the four fields.
   * @returns {{wrote:string[], adopted:string[], pending:string[], unverified:string[]}}
   */
  _reconcile({ boot }) {
    const shared = this._store();
    const out = { wrote: [], adopted: [], pending: [], unverified: [] };
    shared.refresh();
    this.pending.clear();

    const entries = {};
    const verifySource = {};
    let metaChanged = false;

    for (const field of LOCAL_FIELDS) {
      const sf = FIELD_MAP[field];
      const g = shared.get(sf);
      const lv = clean(this.local.get(field));
      const lts = this._ts(field);

      if (!g) {
        // Nothing (readable) in the shared store for this field.
        if (lv) {
          const at = lts || this.now();
          entries[sf] = { value: lv, updatedAt: at };
          verifySource[sf] = { value: lv };
          if (!lts) { this.meta.fields[field] = at; metaChanged = true; }
        }
        continue;
      }

      const sv = clean(g.value);
      const sts = Number(g.updatedAt) || 0;

      if (sv === lv) {
        if (sts > lts) { this.meta.fields[field] = sts; metaChanged = true; }
        continue;
      }

      if (sts > lts) {
        // The shared value is NEWER and different: adopt it, if the running app may take it now.
        if (!this.canApply(field, { boot })) {
          this.pending.add(field);
          out.pending.push(field);
          const tag = `${field}@${sts}`;
          if (!this._declinedLogged.has(tag)) {
            this._declinedLogged.add(tag);
            this.log(`[SHARED] ${field}: có giá trị mới ở kho chung nhưng app đang chạy — chưa áp dụng (giữ nguyên cho tới lúc rảnh)`);
          }
          continue;
        }
        let applied = false;
        try { applied = this.local.apply(field, sv) !== false; }
        catch (error) { this.log(`[SHARED] apply ${field}=FAILED ${String(error.message).slice(0, 120)}`); }
        if (applied) {
          if (lv && sv) {
            this.log(`[SHARED] ${field}: giá trị cũ (vân tay ${fingerprint(lv)}) được thay bằng giá trị mới hơn từ kho chung (vân tay ${fingerprint(sv)})`);
          }
          this.meta.fields[field] = sts;
          metaChanged = true;
          out.adopted.push(field);
        } else {
          this.pending.add(field);
          out.pending.push(field);
        }
        continue;
      }

      if (lts > sts) {
        // OUR value is newer (a user edit that never reached the shared store, or a cleared field).
        entries[sf] = lv ? { value: lv, updatedAt: lts } : { clear: true, updatedAt: lts };
        if (lv) verifySource[sf] = { value: lv };
      }
      // sts === lts with different values: a tie, nobody is newer - leave both alone.
    }

    if (Object.keys(entries).length) {
      const r = shared.write(entries);
      out.wrote = (r.written || []).slice();
      const skipped = r.skipped || [];
      if (skipped.length) this.dirty = true; // someone newer got in first: the next pass adopts it
      out.unverified = shared.verify(verifySource).filter(n => out.wrote.includes(n));
    }
    if (metaChanged || !this.metaExisted) this._saveMeta();
    return out;
  }

  /**
   * Boot: first bootstrap or a normal reconcile. Never throws.
   * @returns {{ok:boolean, first:boolean, wrote:string[], adopted:string[], pending:string[], unverified:string[]}}
   */
  bootstrap() {
    const empty = { ok: false, first: false, wrote: [], adopted: [], pending: [], unverified: [] };
    if (!this.primary) return empty;
    this._loadMeta();
    const first = !this.metaExisted;

    const result = this._safe("bootstrap", () => {
      this._store();
      if (first) {
        // OfferBot owns the position semantics: its non-empty values win, once.
        const t = this.now();
        for (const f of LOCAL_FIELDS) {
          if (clean(this.local.get(f))) this.meta.fields[f] = t;
        }
      }
      const r = this._reconcile({ boot: true });
      this.ready = true;
      this.log(
        `[SHARED] bootstrap${first ? " (lần đầu)" : ""}: ghi=${r.wrote.join(",") || "-"} nhận=${r.adopted.join(",") || "-"} ` +
        `chờ=${r.pending.join(",") || "-"} chưa-xác-minh=${r.unverified.join(",") || "-"} rev=${this.shared.revision()}`
      );
      return { ok: true, first, ...r };
    }, null);

    if (!result) {
      // The shared store is unusable right now. Remember the local state so a later pass can retry.
      this.ready = false;
      this.dirty = true;
      return { ...empty, first };
    }
    return result;
  }

  /** Cheap periodic check (one stat when nothing is pending). Never throws. */
  poll() {
    if (!this.primary) return [];
    if (!this.ready) {
      // Boot could not reach the store: retry once per beat, still without ever throwing.
      if (!this.dirty) return [];
      // A failing store (DPAPI hang, lock) must not stall every beat: retry every 10 minutes.
      if (this.now() < (this._retryAt || 0)) return [];
      this._retryAt = this.now() + RETRY_MS;
      const r = this.bootstrap();
      return r.ok ? r.adopted : [];
    }
    return this._safe("poll", () => {
      const shared = this._store();
      if (!this.dirty && !this.pending.size && !shared.hasChanged()) return [];
      this.dirty = false;
      const r = this._reconcile({ boot: false });
      if (r.adopted.length || r.wrote.length) {
        this.log(`[SHARED] poll: nhận=${r.adopted.join(",") || "-"} ghi=${r.wrote.join(",") || "-"} rev=${shared.revision()}`);
      }
      return r.adopted;
    }, []);
  }

  /**
   * The user saved/cleared something in THIS OfferBot. `values` maps local
   * field -> new value ("" = cleared); fields outside the mapping are ignored.
   * The local timestamp is recorded first, so a failed shared write is retried.
   */
  push(values = {}) {
    if (!this.primary) return null;
    const wanted = LOCAL_FIELDS.filter(f => f in values);
    if (!wanted.length) return null;

    // The local timestamp is recorded first, so a failed shared write is retried by the next poll.
    const prev = {};
    const entries = {};
    for (const f of wanted) {
      prev[f] = this._ts(f);
      const at = Math.max(this.now(), prev[f] + 1);
      this.meta.fields[f] = at;
      const v = clean(values[f]);
      entries[FIELD_MAP[f]] = v ? { value: v, updatedAt: at } : { clear: true, updatedAt: at };
    }
    this._saveMeta();

    const r = this._safe("push", () => {
      const store = this._store();
      // Saving the value the shared store already holds is not a change: no write, no new timestamp
      // (also what stops a value adopted from the other tool from echoing straight back).
      for (const f of wanted) {
        const sf = FIELD_MAP[f];
        const g = store.get(sf);
        const same = g ? clean(g.value) === clean(values[f]) : !clean(values[f]);
        if (same) {
          this.meta.fields[f] = Math.max(prev[f], g ? Number(g.updatedAt) || 0 : 0);
          delete entries[sf];
        }
      }
      this._saveMeta();
      if (!Object.keys(entries).length) return { written: [], skipped: [], revision: store.revision() };
      const w = store.write(entries);
      if ((w.skipped || []).length) this.dirty = true;
      this.log(`[SHARED] đẩy ${(w.written || []).join(",") || "-"} lên kho chung (rev ${w.revision})`);
      return w;
    }, null);
    if (!r) this.dirty = true; // retried by the next poll
    return r;
  }
}

module.exports = { SharedBridge, FIELD_MAP, LOCAL_FIELDS, fingerprint };
