"use strict";

/**
 * license-client.js — the shipped half of the licence system.
 *
 * Two checks, and they answer different questions:
 *
 *   the SIGNATURE says the licence is genuine and when it expires. That is
 *   sealed into the key, so this half works with no network at all and the
 *   user cannot move the expiry date.
 *
 *   the SERVER says whether it has been revoked and whether this device is one
 *   of the ones allowed. Neither fact can live in the key, because both change
 *   after the key is issued.
 *
 * The server's answer is itself signed, with the same Ed25519 key, and cached.
 * That is what makes going offline survivable without making the cache a place
 * to edit REVOKED into ACTIVE: an edited cache no longer verifies.
 *
 * This module holds the PUBLIC key only. It cannot mint a licence and it cannot
 * forge a server answer.
 */

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const { verifyLicense, PUBLIC_KEY_DER_B64 } = require("./license");
const { TrustedClock } = require("./trusted-time");

/**
 * How long an answer is believed without asking again.
 *
 * Sixty seconds. This is the beat the whole enforcement story rests on: the
 * owner pauses, suspends or revokes a licence, and the machine using it finds
 * out within a minute without anyone restarting anything.
 */
const DEFAULT_CHECK_TTL_MS = 60 * 1000;

/**
 * How long the app keeps working when the server cannot be reached.
 *
 * A patchy connection must not stop someone working, so there is a grace
 * window. But it is FINITE, and that is the point: "cached ACTIVE, offline,
 * runs forever" is not a licence, it is a one-time purchase. Twelve hours is
 * long enough to cover a dropped connection, a reboot, or the owner's PC
 * being off overnight, and short enough that a machine which never connects
 * again stops working the next day.
 *
 * Measured from the SERVER's own timestamp, which is inside the signature -
 * never from a local field, which could simply be edited forward.
 *
 * A refusal already received is not softened by this: an answer of REVOKED,
 * SUSPENDED or PAUSED locks immediately and grace never applies to it.
 */
const OFFLINE_GRACE_MS = 12 * 60 * 60 * 1000;

/** A request that hangs must not hang the app. */
const REQUEST_TIMEOUT_MS = 8000;

/** How long to leave the server alone after a failed check. */
const RETRY_AFTER_FAILURE_MS = 10000;

const b64urlDecode = text => Buffer.from(
  String(text).replace(/-/g, "+").replace(/_/g, "/"), "base64"
);

function publicKeyObject() {
  return crypto.createPublicKey({
    key: Buffer.from(PUBLIC_KEY_DER_B64, "base64"),
    format: "der",
    type: "spki"
  });
}

/**
 * A stable id for THIS PC.
 *
 * Machine-wide, deliberately: the app runs several instances side by side -
 * Ethereum and Robinhood, slot 1 and slot 2 - each with its own Chromium
 * profile, its own settings and its own wallet. Those are separate INSTANCES,
 * not separate machines, and a licence that counted them as devices would
 * charge a customer three times for one PC.
 *
 * So the id lives in the machine base (%LOCALAPPDATA%\\OpenSea Offer Bot),
 * which every instance computes identically whatever slot it claimed. That is
 * the same anchor instance.js already uses for its slot locks, and putting a
 * file there changes nothing about profile or session isolation.
 *
 * It also survives what a per-slot file would not: deleting one instance's
 * profile, or reinstalling the app, leaves this directory alone.
 *
 * Random, not derived from the hardware. A disk serial or a MAC identifies the
 * machine and the person; nothing here needs to. The cost is that wiping
 * LOCALAPPDATA looks like a new PC - which is what "reset device" is for.
 */
function readDeviceId(machineDirectory) {
  const file = path.join(machineDirectory, "device.json");

  const read = () => {
    try {
      const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
      if (typeof parsed.deviceId === "string" && parsed.deviceId.length >= 16) {
        return parsed.deviceId;
      }
    } catch {
      /* absent or unreadable */
    }
    return null;
  };

  const existing = read();
  if (existing) return existing;

  const deviceId = crypto.randomBytes(16).toString("hex");
  try {
    fs.mkdirSync(machineDirectory, { recursive: true });
    // "wx" is create-or-fail, atomically. Two instances starting together
    // would otherwise each write their own id and the PC would count twice;
    // the one that loses the race reads the winner's file instead.
    const handle = fs.openSync(file, "wx");
    fs.writeFileSync(handle, JSON.stringify({ deviceId, createdAt: Date.now() }, null, 2));
    fs.closeSync(handle);
    return deviceId;
  } catch (error) {
    if (error.code === "EEXIST") {
      const winner = read();
      if (winner) return winner;
    }
    // An id we cannot persist still works for this run.
    return deviceId;
  }
}

/** POST JSON, with a deadline. Never throws for a non-200. */
async function postJson(url, body, timeoutMs = REQUEST_TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal
    });
    const text = await res.text();
    try { return { ok: res.ok, status: res.status, json: JSON.parse(text) }; }
    catch { return { ok: false, status: res.status, json: null, error: "phản hồi không phải JSON" }; }
  } catch (error) {
    return { ok: false, status: 0, json: null, error: error.message };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Is this really what the server said?
 *
 * The signature covers the answer with the `signature` field removed, which is
 * how it was signed. Without this the cache would be a plain JSON file stating
 * whether the app is allowed to run.
 */
function verifyAnswer(answer) {
  if (!answer || typeof answer !== "object" || !answer.signature) return false;
  const { signature, ...rest } = answer;
  try {
    return crypto.verify(
      null,
      Buffer.from(JSON.stringify(rest), "utf8"),
      publicKeyObject(),
      b64urlDecode(signature)
    );
  } catch {
    return false;
  }
}

class LicenseClient {
  /**
   * @param {object}   options
   * @param {string}   options.directory  where device.json and the cache live
   * @param {string}   options.serverUrl
   * @param {Function} [options.onLog]
   */
  constructor({ directory, machineDirectory, serverUrl, appVersion = "", graceMs = 0,
                platform = process.platform,
                checkTtlMs = DEFAULT_CHECK_TTL_MS,
                clock = null,
                onLog = () => {} } = {}) {
    /** Per instance: where this instance's own cached answer lives. */
    this.directory = directory || ".";

    /**
     * Per PC: where the device identity lives.
     *
     * Every instance on this machine points at the same place and therefore
     * presents the same deviceId, so three instances are one licence device.
     * Falls back to the instance directory only when a caller has not been
     * updated - which would count instances separately, so it is not silent.
     */
    this.machineDirectory = machineDirectory || this.directory;
    if (!machineDirectory) {
      onLog("[LICENSE] chưa có machineDirectory — device sẽ tính theo instance");
    }
    this.serverUrl = String(serverUrl || "").replace(/\/+$/, "");

    /**
     * How long a cached answer survives with no server.
     *
     * A parameter so a test can reach the boundary in seconds instead of in
     * twelve hours. Production never passes it - main.js builds this client
     * with paths, a URL and a logger and nothing else - so what ships is the
     * constant above.
     */
    this.graceMs = Number(graceMs) > 0 ? Number(graceMs) : OFFLINE_GRACE_MS;
    this.appVersion = String(appVersion || "");
    this.platform = String(platform || "");
    this.checkTtlMs = checkTtlMs;
    this.onLog = onLog;

    this.deviceId = readDeviceId(this.machineDirectory);

    /**
     * The clock the expiry is judged against.
     *
     * In the machine directory, beside device.json: every instance on this PC
     * shares one view of the time, so a rollback cannot be hidden by opening a
     * different instance.
     */
    this.clock = clock || new TrustedClock({
      directory: this.machineDirectory,
      onLog
    });
    /**
     * Beside device.json, NOT beside the settings.
     *
     * The settings directory carries the version, so a cache kept there is
     * lost by every update - and the app then asks a customer to activate a
     * licence their machine is already bound to. The cache is keyed by
     * deviceId and by a hash of the key: both per machine, neither per
     * version.
     */
    this.cacheFile = path.join(this.machineDirectory, "license-cache.json");

    /** Where it used to live, for a one-time move. */
    this.legacyCacheFile = path.join(this.directory, "license-cache.json");

    this.migrateCache();

    /** When the last check failed, so a dead server is not asked on every action. */
    this.lastFailureAt = 0;
    this.failureCount = 0;
    this.nextRetryAt = 0;
    this.cache = this.readCache();
    this.inFlight = null;
  }

  /**
   * Move a cache written by an older build.
   *
   * One pass, and only when there is nothing at the new location: a machine
   * that has already answered under this version must not be overwritten by
   * an older per-version file left behind.
   */
  migrateCache() {
    try {
      if (this.cacheFile === this.legacyCacheFile) return;
      if (fs.existsSync(this.cacheFile)) return;
      if (!fs.existsSync(this.legacyCacheFile)) return;

      fs.mkdirSync(this.machineDirectory, { recursive: true });
      fs.copyFileSync(this.legacyCacheFile, this.cacheFile);
      fs.rmSync(this.legacyCacheFile);
      this.onLog("[LICENSE] đã chuyển trạng thái kích hoạt sang thư mục máy");
    } catch (error) {
      // A failed move is not fatal: the worst case is the check the app was
      // going to make anyway.
      this.onLog(`[LICENSE] không chuyển được cache: ${error.message}`);
    }
  }

  readCache() {
    try {
      const parsed = JSON.parse(fs.readFileSync(this.cacheFile, "utf8"));
      // An unverifiable cache is not a cache. Treated as absent, so the app
      // asks the server rather than trusting an edited file.
      if (parsed && verifyAnswer(parsed.answer) && parsed.deviceId === this.deviceId) {
        return parsed;
      }
    } catch {
      /* no cache yet */
    }
    return null;
  }

  writeCache(answer, licenseKey) {
    const payload = {
      deviceId: this.deviceId,
      // The key is fingerprinted, not stored: the cache must not become a
      // second copy of the licence.
      keyHash: crypto.createHash("sha256").update(String(licenseKey)).digest("hex"),
      // Kept for the admin log and for nothing else: every decision is made
      // from answer.serverTime, which cannot be edited without breaking the
      // signature.
      checkedAt: Date.now(),
      answer
    };
    try {
      fs.mkdirSync(this.machineDirectory, { recursive: true });
      fs.writeFileSync(this.cacheFile, JSON.stringify(payload, null, 2));
    } catch (error) {
      this.onLog(`[LICENSE] không ghi được cache: ${error.message}`);
    }
    this.cache = payload;
    return payload;
  }

  cacheMatches(licenseKey) {
    if (!this.cache) return false;
    const hash = crypto.createHash("sha256").update(String(licenseKey)).digest("hex");
    return this.cache.keyHash === hash;
  }

  /** Ask the server. `activate` claims a device slot; `check` does not. */
  async call(endpoint, licenseKey) {
    if (!this.serverUrl) {
      return { ok: false, error: "Chưa cấu hình License Server." };
    }
    const res = await postJson(`${this.serverUrl}/api/license/${endpoint}`, {
      licenseKey,
      deviceId: this.deviceId,
      appVersion: this.appVersion,
      platform: this.platform
    });

    if (!res.json) {
      return { ok: false, error: res.error || `server trả ${res.status}` };
    }
    if (!verifyAnswer(res.json)) {
      // A wrong signature is not a network problem: something is answering
      // that is not the owner's server.
      return { ok: false, error: "Chữ ký phản hồi từ server không hợp lệ." };
    }

    this.writeCache(res.json, licenseKey);
    return { ok: true, answer: res.json };
  }

  activate(licenseKey) { return this.call("activate", licenseKey); }

  /**
   * The state to act on, right now.
   *
   * Returns synchronously from what is already known - a gate cannot wait for
   * the network - and refreshes in the background when the answer is stale.
   */
  evaluate(licenseKey) {
    const local = verifyLicense(licenseKey);

    // ---- expiry, judged against a clock the user does not own -------
    //
    // verifyLicense reads Date.now(), which on the user's own machine is a
    // number they can set. The signature still decides WHEN the licence ends;
    // the trusted clock decides whether that moment has arrived.
    if (local.valid || local.expired) {
      const verdict = this.clock.expiryVerdict(local.expiresAt);

      if (verdict.undetermined) {
        return {
          allowed: false,
          source: "clock",
          status: "CLOCK_ROLLBACK",
          reason:
            "Đồng hồ hệ thống có vẻ bị chỉnh lùi. Cần kết nối mạng để đồng bộ " +
            "giờ chuẩn trước khi dùng tiếp.",
          clock: this.clock.status(),
          local
        };
      }

      if (verdict.expired && local.valid) {
        // The wall clock says the licence lives; the trusted clock says it does
        // not. The trusted one wins.
        return {
          allowed: false,
          source: "clock",
          status: "EXPIRED",
          reason: "License đã hết hạn.",
          clock: this.clock.status(),
          local
        };
      }
    }

    // The signature is the floor. No server answer can rescue a licence that
    // is forged or has run out, and none is asked for.
    if (!local.valid) {
      return {
        allowed: false,
        source: "signature",
        status: local.expired ? "EXPIRED" : "INVALID",
        reason: local.reason,
        local
      };
    }

    if (!this.cacheMatches(licenseKey)) {
      return {
        allowed: false,
        source: "server",
        status: "NOT_ACTIVATED",
        reason: "License chưa được kích hoạt trên thiết bị này.",
        local
      };
    }

    const answer = this.cache.answer;

    // Age is measured from the SERVER's own timestamp, which is inside the
    // signature - not from the checkedAt this process wrote beside it. That
    // field is a plain number in a local file: moving it forward would have
    // renewed the grace window indefinitely, which is precisely the thing the
    // grace window must not allow.
    const age = Date.now() - Number(answer.serverTime || 0);

    if (answer.status !== "ACTIVE") {
      return {
        allowed: false,
        source: "server",
        status: answer.status,
        reason: answer.reason || "License không dùng được.",
        local
      };
    }

    // The offline deadline. An answer that is merely old is still the last
    // thing the server said - but only for so long. Past the window the app
    // locks until it can ask again, so a machine that never reconnects cannot
    // keep running on a stale yes.
    if (age > this.graceMs) {
      return {
        allowed: false,
        source: "cache-expired",
        status: "OFFLINE_GRACE_OVER",
        reason:
          "Đã " + Math.floor(age / 3600000) + " giờ không liên hệ được máy chủ " +
          "license. Hãy kết nối mạng để dùng tiếp.",
        ageMs: age,
        local
      };
    }

    return {
      allowed: true,
      source: age > this.checkTtlMs ? "cache-stale" : "cache",
      status: "ACTIVE",
      reason: "",
      // Shown in the app. It comes from the server, not from the key, so a
      // rename reaches the customer on their next check.
      customerName: answer.customerName || "",
      ageMs: age,
      // What the app is actually running on, for the status line: the licence's
      // own remaining life, not a separate offline allowance.
      expiresAt: local.expiresAt || 0,
      daysLeft: local.daysLeft ?? 0,
      clock: this.clock.status(),
      local
    };
  }

  /**
   * Refresh in the background when the answer is older than its TTL.
   *
   * One at a time: a gate is checked on every action, and a burst of them must
   * not become a burst of requests.
   */
  /**
   * Ask the server to bind this machine, now, whatever the cache says.
   *
   * The button path. It ignores the TTL and the failure backoff on purpose:
   * both exist to keep a background beat polite, and neither should make a
   * person press a button twice.
   */
  activateNow(licenseKey) {
    if (!licenseKey) return Promise.resolve({ ok: false, error: "Chưa nhập License Key." });
    this.lastFailureAt = 0;
    this.failureCount = 0;
    this.nextRetryAt = 0;
    const call = this.call("activate", licenseKey)
      .then(result => {
        if (!result.ok) this.onLog(`[LICENSE] kích hoạt thất bại: ${result.error}`);
        return result;
      })
      .finally(() => { if (this.inFlight === call) this.inFlight = null; });
    this.inFlight = call;
    return call;
  }

  refreshIfStale(licenseKey) {
    if (!licenseKey || this.inFlight) return this.inFlight;
    const fresh = this.cacheMatches(licenseKey) &&
      Date.now() - Number(this.cache.answer.serverTime || 0) <= this.checkTtlMs;
    if (fresh) return null;

    // A gate WAITS for this call, so a server that is down must not turn every
    // action into an eight-second stall. After a failure the next attempt is
    // held off briefly; the cached answer and the grace window decide what
    // happens in the meantime, which is exactly their job.
    if (Date.now() < this.nextRetryAt) return null;

    // The clock rides the same beat. Never awaited: a gate answers now.
    this.clock.syncIfStale();

    const endpoint = this.cacheMatches(licenseKey) ? "check" : "activate";
    this.inFlight = this.call(endpoint, licenseKey)
      .then(result => {
        if (result.ok) {
          this.lastFailureAt = 0;
          this.failureCount = 0;
          this.nextRetryAt = 0;
        }
        else {
          this.lastFailureAt = Date.now();
          this.failureCount = Math.min(6, this.failureCount + 1);
          this.nextRetryAt = this.lastFailureAt + Math.min(
            5 * 60 * 1000,
            RETRY_AFTER_FAILURE_MS * (2 ** (this.failureCount - 1))
          );
          this.onLog(`[LICENSE] check thất bại: ${result.error}`);
        }
        return result;
      })
      .finally(() => { this.inFlight = null; });
    return this.inFlight;
  }
}

module.exports = {
  LicenseClient,
  readDeviceId,
  verifyAnswer,
  OFFLINE_GRACE_MS,
  DEFAULT_CHECK_TTL_MS,
  DEFAULT_CHECK_TTL_MS,
  REQUEST_TIMEOUT_MS
};
