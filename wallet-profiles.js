"use strict";

/**
 * wallet-profiles.js — the wallets ONE Tool can switch between (1.25.11).
 *
 *   Every Tool (slot) is its own process with its own settings directory, so
 *   this file is per Tool by construction: Tool #1's list and active choice
 *   cannot touch Tool #2's.
 *
 *   ONE wallet is active per Tool. The active key still lives where every
 *   signer reads it (the slot's encrypted secrets.json → settings.privateKey);
 *   this file only remembers the others, so switching back is a pick, not a
 *   re-paste. Concurrent multi-wallet = several Tools, never several signers
 *   in one engine.
 *
 *   Keys are encrypted with the same Electron safeStorage (Windows DPAPI) the
 *   secret store uses; addresses are public and stored as-is. Nothing here is
 *   logged, and no method returns a key except keyOf(), which only main uses to
 *   activate a profile.
 */

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const FORMAT = 1;
const MAX_PROFILES = 50;

class WalletProfiles {
  /**
   * @param {object} o
   * @param {string} o.file              per-slot JSON file
   * @param {object} o.safeStorage       Electron safeStorage
   * @param {function} o.deriveAddress   privateKey → lowercase address | null
   */
  constructor({ file, safeStorage, deriveAddress }) {
    this.file = file;
    this.safeStorage = safeStorage;
    this.deriveAddress = deriveAddress;
  }

  isAvailable() {
    try { return Boolean(this.safeStorage && this.safeStorage.isEncryptionAvailable()); } catch { return false; }
  }

  read() {
    try {
      const data = JSON.parse(fs.readFileSync(this.file, "utf8"));
      if (data && Array.isArray(data.profiles)) {
        return { version: FORMAT, activeId: data.activeId || null, profiles: data.profiles.slice(0, MAX_PROFILES) };
      }
    } catch { /* missing or unreadable: an empty list */ }
    return { version: FORMAT, activeId: null, profiles: [] };
  }

  write(data) {
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    const tmp = `${this.file}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(data, null, 2));
    fs.renameSync(tmp, this.file);
  }

  /** Public view: never a key. */
  list() {
    const data = this.read();
    return data.profiles.map(p => ({
      id: p.id, address: p.address, label: p.label || "", active: p.id === data.activeId
    }));
  }

  activeId() { return this.read().activeId; }

  /**
   * Store (or refresh) the profile for this key and make it the active one.
   * @returns {{ok:true,id:string,address:string}|{ok:false,error:string}}
   */
  upsertActive(privateKey, label = "") {
    const address = this.deriveAddress(privateKey);
    if (!address) return { ok: false, error: "Private Key không hợp lệ." };
    if (!this.isAvailable()) return { ok: false, error: "Windows secure storage không dùng được." };
    const data = this.read();
    const enc = this.safeStorage.encryptString(String(privateKey).trim()).toString("base64");
    let profile = data.profiles.find(p => p.address === address);
    if (profile) {
      profile.key = enc;
      if (label) profile.label = label;
    } else {
      if (data.profiles.length >= MAX_PROFILES) return { ok: false, error: `Tối đa ${MAX_PROFILES} ví.` };
      profile = { id: crypto.randomUUID(), address, label: label || "", key: enc, addedAt: Date.now() };
      data.profiles.push(profile);
    }
    data.activeId = profile.id;
    this.write(data);
    return { ok: true, id: profile.id, address };
  }

  /** Decrypt one profile's key - main only, to activate it. */
  keyOf(id) {
    const profile = this.read().profiles.find(p => p.id === id);
    if (!profile) return null;
    try { return this.safeStorage.decryptString(Buffer.from(profile.key, "base64")); } catch { return null; }
  }

  /** Remove a stored profile. The ACTIVE one cannot be removed. */
  remove(id) {
    const data = this.read();
    if (id === data.activeId) return { ok: false, error: "Không xoá được ví đang dùng — chuyển sang ví khác trước." };
    const before = data.profiles.length;
    data.profiles = data.profiles.filter(p => p.id !== id);
    if (data.profiles.length === before) return { ok: false, error: "Không tìm thấy ví." };
    this.write(data);
    return { ok: true };
  }

  /** The Tool's key was cleared: no profile is active any more. */
  clearActive() {
    const data = this.read();
    if (!data.activeId) return;
    data.activeId = null;
    this.write(data);
  }
}

module.exports = { WalletProfiles, MAX_PROFILES };
