"use strict";

/**
 * Encrypted credential store.
 *
 * Secrets used to be memory-only, which kept them off disk but meant every
 * launch began with the user re-typing a licence, a private key and an API key
 * before the app could do anything. This keeps them across restarts WITHOUT
 * putting readable bytes on disk: Electron's safeStorage encrypts them, and on
 * Windows that is DPAPI, tied to the logged-in Windows account.
 *
 * Consequences that are deliberate, not limitations to work around:
 *   - copying the file to another machine or another Windows user is useless,
 *     because the key that decrypts it never leaves that account
 *   - if safeStorage is unavailable, secrets stay session-only. There is no
 *     plaintext fallback; a fallback that writes readable keys would defeat the
 *     entire reason this file exists
 *
 * Nothing here ever logs a secret, encrypted or otherwise.
 */

const fs = require("fs");
const path = require("path");

/** Bumped only if the on-disk shape changes in a way older builds cannot read. */
const FORMAT_VERSION = 1;

/**
 * Which settings are held here rather than in settings.json.
 * Kept in step with db.SECRET_FIELDS.
 *
 * THE TWO LISTS HAD DRIFTED, AND THE GAP WAS A LOST CREDENTIAL
 *
 *   `db.SECRET_FIELDS` lists every credential, which is what makes db BLANK
 *   it before writing settings.json - correct, a credential does not belong in
 *   a plaintext file. This list did NOT include it, so the encrypted store
 *   never wrote it either.
 *
 *   The result was a field that was stripped from one place and never saved to
 *   the other: the user typed a key, Settings said saved, and the box
 *   was empty on the next launch, every launch. `main.js` was already handing
 *   the field to `save()`; the filter below dropped it on the floor.
 *
 *   Adding it here also makes it survive an update, because `migrateSecrets`
 *   carries the ciphertext of every field in this list forward to the new
 *   version's folder.
 */
const SECRET_FIELDS = Object.freeze([
  "apiKeys", "apiKey", "apiKey2", "privateKey", "licenseKey"
]);

class SecretStore {
  /**
   * @param {object} options
   * @param {string} options.file            where the ciphertext lives
   * @param {object} options.safeStorage     electron.safeStorage
   * @param {(line:string)=>void} [options.onLog] never receives secret material
   */
  constructor({ file, safeStorage, onLog }) {
    this.file = file;
    this.safeStorage = safeStorage;
    this.onLog = typeof onLog === "function" ? onLog : () => {};
    this.lastError = null;
  }

  /**
   * Can this machine encrypt at all?
   *
   * On a fresh Windows account safeStorage is available immediately; on Linux it
   * depends on a keyring being present. Asking every time rather than caching
   * means a keyring that appears later is picked up without a restart.
   */
  isAvailable() {
    try {
      return Boolean(this.safeStorage && this.safeStorage.isEncryptionAvailable());
    } catch {
      return false;
    }
  }

  ensureDirectory() {
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
  }

  readRaw() {
    if (!fs.existsSync(this.file)) return null;
    try {
      return JSON.parse(fs.readFileSync(this.file, "utf8"));
    } catch (error) {
      // Keep the damaged file: overwriting it destroys the only chance of
      // recovering credentials, and the user is told rather than left guessing.
      const backup = `${this.file}.bak-${Date.now()}`;
      try { fs.copyFileSync(this.file, backup); } catch { /* best effort */ }
      this.lastError = `secrets file hỏng; đã sao lưu tại ${backup}`;
      this.onLog(`[SECRETS] read=FAILED ${error.message} backup=${backup}`);
      return null;
    }
  }

  /**
   * Decrypt what is stored.
   * @returns {{available:boolean, values:object, failed:string[]}}
   */
  load() {
    const available = this.isAvailable();
    const result = { available, values: {}, failed: [] };

    if (!available) {
      this.onLog("[SECRETS] load=SKIPPED secure storage unavailable");
      return result;
    }

    const raw = this.readRaw();
    if (!raw || !raw.fields) {
      this.onLog("[SECRETS] load=OK none-stored");
      return result;
    }

    for (const field of SECRET_FIELDS) {
      const encoded = raw.fields[field];
      if (!encoded) continue;
      try {
        result.values[field] = this.safeStorage.decryptString(
          Buffer.from(String(encoded), "base64")
        );
      } catch {
        // A different Windows account, a restored profile, or a corrupted blob.
        // The app keeps running and asks for that one credential again.
        result.failed.push(field);
      }
    }

    const count = Object.keys(result.values).length;
    this.onLog(
      `[SECRETS] load=OK restored=${count} failed=${result.failed.length}`
    );
    return result;
  }

  /**
   * Encrypt and store. Fields not present in `values` are left untouched, so a
   * form that submits only what the user retyped cannot wipe the rest.
   *
   * A field explicitly set to an empty string IS removed - that is how the user
   * clears one credential without clearing them all.
   */
  save(values = {}) {
    if (!this.isAvailable()) {
      this.onLog("[SECRETS] save=SKIPPED secure storage unavailable");
      return { ok: false, available: false, reason: "secure storage unavailable" };
    }

    // The whole read-modify-write runs under one lock. Reading INSIDE it is
    // the point: another window may have written between our last read and
    // this save, and merging into a stale snapshot is how one credential
    // quietly erases another.
    return this.withLock(() => this.saveLocked(values));
  }

  /** @private The body of save(), with the store already locked. */
  saveLocked(values) {
    const raw = this.readRaw() || { version: FORMAT_VERSION, fields: {} };
    if (!raw.fields) raw.fields = {};

    let changed = 0;
    for (const field of SECRET_FIELDS) {
      if (!(field in values)) continue;
      const value = values[field];

      if (!value) {
        if (raw.fields[field]) { delete raw.fields[field]; changed++; }
        continue;
      }

      try {
        raw.fields[field] = this.safeStorage
          .encryptString(String(value))
          .toString("base64");
        changed++;
      } catch (error) {
        this.onLog(`[SECRETS] save=FAILED field could not be encrypted: ${error.message}`);
        return { ok: false, available: true, reason: error.message };
      }
    }

    if (!changed) return { ok: true, available: true, changed: 0 };

    raw.version = FORMAT_VERSION;

    try {
      this.writeAtomic(raw);
    } catch (error) {
      this.onLog(`[SECRETS] save=FAILED ${error.message}`);
      return { ok: false, available: true, reason: error.message };
    }

    this.onLog(`[SECRETS] save=OK fields=${changed}`);
    return { ok: true, available: true, changed };
  }

  /**
   * temp + fsync + rename, so a failed save cannot lose every credential.
   *
   * The temp name carries the pid and a random suffix. It used to be one fixed
   * `${file}.tmp`, which was fine while every instance had its own store and
   * is not now that they share one: two windows saving at the same moment
   * would open, write and rename THE SAME temp path, and one of them would
   * rename a file the other was still writing into.
   */
  writeAtomic(payload) {
    this.ensureDirectory();
    const temp = `${this.file}.${process.pid}.${Math.random().toString(16).slice(2, 8)}.tmp`;
    const handle = fs.openSync(temp, "w");
    try {
      fs.writeFileSync(handle, JSON.stringify(payload, null, 2), "utf8");
      fs.fsyncSync(handle);
    } finally {
      fs.closeSync(handle);
    }
    try {
      fs.renameSync(temp, this.file);
    } catch (error) {
      try { fs.unlinkSync(temp); } catch { /* already gone */ }
      throw error;
    }
  }

  /**
   * Hold an exclusive lock for one read-modify-write.
   *
   * WHY A LOCK AND NOT JUST AN ATOMIC RENAME
   *
   *   The rename is atomic; the CYCLE around it is not. `save()` reads the
   *   whole file, merges the fields it was given, and writes the result back.
   *   Two windows doing that at once both read the same starting point, and
   *   the second rename silently discards whatever the first one added - so
   *   saving an API key in one window could erase a licence key just saved in
   *   another. Sharing one store is what makes that reachable, so the lock
   *   arrives with it.
   *
   *   `wx` creates the lock file or fails, atomically, which is the whole
   *   mechanism. A lock older than LOCK_STALE_MS is treated as abandoned -
   *   a process killed mid-save must not lock the credential store forever.
   *
   * @param {() => T} work
   * @returns {T}
   * @template T
   */
  withLock(work) {
    const lock = `${this.file}.lock`;
    const LOCK_STALE_MS = 10000;
    const deadline = Date.now() + 5000;

    for (;;) {
      try {
        const handle = fs.openSync(lock, "wx");
        fs.closeSync(handle);
        break;
      } catch (error) {
        if (error.code !== "EEXIST") break;          // cannot lock: proceed
        let age = Infinity;
        try { age = Date.now() - fs.statSync(lock).mtimeMs; } catch { age = Infinity; }
        if (age > LOCK_STALE_MS) {
          try { fs.unlinkSync(lock); } catch { /* someone else got there */ }
          continue;
        }
        if (Date.now() > deadline) break;            // waited long enough
        // Busy-wait briefly: a save is milliseconds, and this keeps the
        // store's API synchronous, which every caller already depends on.
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 25);
      }
    }

    try {
      return work();
    } finally {
      try { fs.unlinkSync(lock); } catch { /* never held it */ }
    }
  }

  /** Remove stored credentials outright. */
  clear() {
    try {
      if (fs.existsSync(this.file)) fs.unlinkSync(this.file);
      this.onLog("[SECRETS] cleared");
      return { ok: true };
    } catch (error) {
      this.onLog(`[SECRETS] clear=FAILED ${error.message}`);
      return { ok: false, error: error.message };
    }
  }

  /**
   * What the UI is allowed to know: whether a credential exists, never its value.
   */
  status() {
    const raw = this.readRaw();
    const present = {};
    for (const field of SECRET_FIELDS) {
      present[field] = Boolean(raw && raw.fields && raw.fields[field]);
    }
    return {
      available: this.isAvailable(),
      present,
      file: this.file,
      error: this.lastError
    };
  }
}

/**
 * Copy an older version's ciphertext forward.
 *
 * DPAPI blobs are bound to the Windows account, not to a path, so the same user
 * can still decrypt a file that simply moved. Copying beats decrypt-and-
 * re-encrypt: it never materialises plaintext just to change folders.
 */
/**
 * CỨU NHỮNG TRƯỜNG KHÔNG GIẢI MÃ ĐƯỢC, TỪ CÁC BẢN CŨ
 *
 *   `migrateSecrets` bỏ qua khi file đích ĐÃ TỒN TẠI — đúng, vì chép đè lên
 *   một file đang dùng là cách làm mất credential. Nhưng nó để hở đúng một
 *   trường hợp, và đó là trường hợp người dùng gặp: file của phiên bản này
 *   tồn tại, các trường có mặt, mà giải mã thất bại — vì blob được ký bằng
 *   một khoá os_crypt khác (userData từng khác, một slot khác, một lần chạy
 *   với danh tính dev). Lúc đó không lần khởi động nào thử lại: người dùng
 *   thấy "3 credential không giải mã được" và phải nhập tay, dù bản cũ ngay
 *   cạnh đó vẫn còn blob đọc được.
 *
 *   Nên: với ĐÚNG những trường đã hỏng, tìm ngược qua các file nguồn, và chỉ
 *   nhận một blob khi nó GIẢI MÃ ĐƯỢC bằng khoá hiện tại. Giải mã được nghĩa
 *   là nó thuộc về profile này — không có phỏng đoán nào ở đây.
 *
 *   Giá trị lấy ra được đưa thẳng vào `store.save()` để mã hoá lại bằng khoá
 *   hiện tại, và KHÔNG bao giờ được ghi ra log, kể cả một phần.
 *
 * @returns {{recovered:string[], stillFailed:string[]}}
 */
function recoverFailedSecrets({ store, sources, failed, safeStorage, onLog = () => {} }) {
  const stillFailed = [...failed];
  const values = {};
  if (!failed.length || !store || !safeStorage) return { recovered: [], stillFailed };

  for (const source of sources) {
    if (!stillFailed.length) break;
    let raw;
    try {
      if (!fs.existsSync(source)) continue;
      raw = JSON.parse(fs.readFileSync(source, "utf8"));
    } catch { continue; }
    const fields = (raw && raw.fields) || {};
    for (const field of [...stillFailed]) {
      const encoded = fields[field];
      if (!encoded) continue;
      try {
        const value = safeStorage.decryptString(Buffer.from(String(encoded), "base64"));
        if (!value) continue;
        values[field] = value;
        stillFailed.splice(stillFailed.indexOf(field), 1);
      } catch { /* blob này cũng thuộc profile khác — thử nguồn tiếp theo */ }
    }
  }

  const recovered = Object.keys(values);
  if (!recovered.length) return { recovered, stillFailed };

  // Ghi lại bằng khoá hiện tại. `save` chỉ đụng đúng những trường được truyền
  // vào, nên blob của các trường khác giữ nguyên.
  const out = store.save(values);
  if (!out || out.ok !== true) {
    onLog(`[SECRETS] recover=FAILED không ghi lại được (${(out && out.reason) || "?"})`);
    return { recovered: [], stillFailed: [...failed] };
  }
  // Chỉ TÊN trường, không bao giờ giá trị.
  onLog(`[SECRETS] recover=OK từ bản cũ: ${recovered.join(", ")}`);
  return { recovered, stillFailed };
}

function migrateSecrets(sources, destination, onLog = () => {}) {
  if (fs.existsSync(destination)) return null;

  for (const source of sources) {
    try {
      if (!fs.existsSync(source)) continue;

      // The source must be readable BEFORE it is treated as a migration
      // candidate. Copying a truncated or half-written file would produce a
      // destination that exists - so no later launch retries the migration -
      // and holds nothing, which reads to the user as "my credentials
      // vanished on update".
      const before = JSON.parse(fs.readFileSync(source, "utf8"));
      const wanted = Object.keys((before && before.fields) || {}).sort();
      if (!wanted.length) {
        onLog(`[SECRETS] migrate-skip=${source} rong`);
        continue;
      }

      fs.mkdirSync(path.dirname(destination), { recursive: true });
      fs.copyFileSync(source, destination);

      // READ BACK. A copy that reported success is not evidence the bytes
      // landed: a full disk, an antivirus quarantine or a sync client can all
      // leave a file that exists and does not parse. Verified by FIELD NAMES,
      // never by decrypting - migration must not materialise plaintext, which
      // is the whole reason it copies ciphertext instead of re-encrypting.
      const after = JSON.parse(fs.readFileSync(destination, "utf8"));
      const got = Object.keys((after && after.fields) || {}).sort();
      const same = got.length === wanted.length &&
        got.every((f, i) => f === wanted[i]);

      if (!same) {
        // Remove the BAD DESTINATION so the next launch tries again. The
        // SOURCE is never touched, here or anywhere else in this function:
        // until a migration is verified, the old file is the only copy of the
        // user's credentials that is known to be good.
        try { fs.unlinkSync(destination); } catch { /* nothing written */ }
        onLog(
          `[SECRETS] migrate=FAILED doc-lai-khong-khop ` +
          `nguon=${wanted.length} dich=${got.length} — giu nguyen file cu`
        );
        continue;
      }

      onLog(`[SECRETS] migrated-from=${source} verified=${got.length} fields`);
      return source;
    } catch (error) {
      // Same rule on an exception: leave the source alone, drop a partial
      // destination, and let the next candidate try.
      try { if (fs.existsSync(destination)) fs.unlinkSync(destination); } catch {}
      onLog(`[SECRETS] migrate-skip=${source} ${error.message}`);
    }
  }
  return null;
}

module.exports = { SecretStore, migrateSecrets, recoverFailedSecrets, SECRET_FIELDS, FORMAT_VERSION };
