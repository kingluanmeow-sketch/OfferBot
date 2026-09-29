"use strict";

/**
 * instance.js — one process, one profile.
 *
 * The app used to take `app.requestSingleInstanceLock()`, so a second copy quit
 * on the spot and merely focused the first. Removing that lock alone is not
 * enough: every instance would then share ONE Chromium profile directory, and
 * the cookie/localStorage LevelDB in it is held exclusively by whichever
 * process opened it first. Two instances on one profile means one of them is
 * running on a storage layer that silently refuses to persist - which is the
 * same OpenSea session, the same wallet, seen twice.
 *
 * So each instance claims a numbered SLOT and gets its own:
 *
 *   - Chromium profile   (userData)      -> its own cookies, its own
 *                                           persist:opensea partition, its own
 *                                           logged-in OpenSea account
 *   - settings directory (settings.json) -> its own Private Key, its own wallet
 *   - secret store       (secrets.json)  -> its own encrypted credentials
 *
 * Slot 1 keeps the exact paths the app has always used, so an existing install
 * opens on its own data and notices nothing. Slots 2+ are new directories.
 *
 * What CANNOT be isolated, and is not pretended to be: everything inside one
 * process is already per-process (the rate limiter buckets, the GraphQL
 * session, the wallet, the job flags), and everything outside it belongs to
 * OpenSea (the account's own API rate limits). Two instances on two wallets do
 * not share either. Two instances on the SAME wallet share OpenSea's per-account
 * limits, because that is one account - no local design can change that.
 */

const fs = require("fs");
const path = require("path");
const { app } = require("electron");

/** Enough for the three-window workflow with room to spare, and bounded. */
const MAX_INSTANCES = 8;

// Tên thư mục dùng chung cả máy đi theo danh tính: bản dev không bao giờ
// đọc/ghi settings, credential hay khoá của bản production.
const PRIMARY_SETTINGS_BASE = path.join(
  process.env.LOCALAPPDATA || app.getPath("appData"),
  require("./dev-runtime").settingsBaseName("OpenSea Offer Bot")
);

/**
 * Where the slot locks live.
 *
 * Deliberately NOT inside userData: userData is what the slot DECIDES, so the
 * locks have to sit somewhere every instance can agree on before any of them
 * has picked one.
 */
const LOCK_DIR = path.join(PRIMARY_SETTINGS_BASE, "locks");

/** The profile Electron would have used on its own, before any slot applies. */
const DEFAULT_USER_DATA = app.getPath("userData");

let state = null;

/**
 * Is that pid still running?
 *
 * Signal 0 does not signal; it only asks. ESRCH means gone, EPERM means alive
 * but owned by someone else - which still means the lock is real.
 */
function pidAlive(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error.code === "EPERM";
  }
}

function lockPathFor(slot) {
  return path.join(LOCK_DIR, `slot-${slot}.lock`);
}

/**
 * How often a running instance re-stamps its lock, and how old a lock may
 * get before it is treated as abandoned.
 *
 * The heartbeat is the ONLY thing a recycled pid cannot fake. Two minutes is
 * four missed beats: long enough that a busy machine never loses its slot,
 * short enough that an abandoned one comes back on its own.
 */
const LOCK_HEARTBEAT_MS = 30 * 1000;
const LOCK_STALE_MS = 120 * 1000;

/** Keeps our own lock's timestamp current while we run. */
let heartbeat = null;

function stampLock(file) {
  try {
    fs.writeFileSync(file, String(process.pid));
    return true;
  } catch {
    return false;
  }
}

function lockAgeMs(file) {
  try {
    return Date.now() - fs.statSync(file).mtimeMs;
  } catch {
    return Infinity;
  }
}

/**
 * Take `slot` if nothing living holds it.
 *
 * `wx` is half the mechanism: it creates the file and fails if it already
 * exists, atomically, so two instances racing for the same slot cannot both
 * win.
 *
 * The other half is the heartbeat, and it exists because A PID IS NOT PROOF.
 * Windows recycles pids: a lock left behind by a force-killed instance was
 * found still naming a number that by then belonged to dllhost.exe.
 * pidAlive said yes forever, slot 1 was never reclaimed, and every launch
 * after that crash landed on slot 2 with a fresh profile - no wallet, no
 * settings, and nothing on screen to explain it.
 *
 * Holding the file open was tried first and does not work: libuv opens with
 * FILE_SHARE_DELETE, so an open handle does not stop the unlink and proves
 * nothing. A timestamp only a live process keeps refreshing does.
 *
 * So a lock is respected when its pid is alive AND its stamp is recent. The
 * common crash-and-reopen case never reaches the second test - the pid is
 * simply gone - and the long tail, where the number has since been handed to
 * something else, is caught by an age no ghost can update.
 */
function tryClaim(slot) {
  const file = lockPathFor(slot);

  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const handle = fs.openSync(file, "wx");
      fs.writeFileSync(handle, String(process.pid));
      fs.closeSync(handle);
      startHeartbeat(file);
      return true;
    } catch (error) {
      if (error.code !== "EEXIST") return false;

      let holder = 0;
      try {
        holder = Number.parseInt(fs.readFileSync(file, "utf8").trim(), 10);
      } catch {
        holder = 0;
      }

      // Alive AND beating: a real instance owns this slot.
      if (pidAlive(holder) && lockAgeMs(file) <= LOCK_STALE_MS) return false;

      // Gone, or a number that outlived its owner.
      try {
        fs.unlinkSync(file);
      } catch {
        return false;
      }
      // Loop once more: whoever wins the `wx` above owns the slot.
    }
  }

  return false;
}

/**
 * Keep our lock warm.
 *
 * unref'd on purpose: this timer must never be the reason the process stays
 * alive, and it has nothing to do on the way out.
 */
function startHeartbeat(file) {
  if (heartbeat) clearInterval(heartbeat);
  heartbeat = setInterval(() => stampLock(file), LOCK_HEARTBEAT_MS);
  if (typeof heartbeat.unref === "function") heartbeat.unref();
}

/** Slot 1 keeps today's paths exactly; later slots get their own siblings. */
function pathsFor(slot) {
  if (slot === 1) {
    return { userData: DEFAULT_USER_DATA, settingsBase: PRIMARY_SETTINGS_BASE };
  }
  return {
    userData: `${DEFAULT_USER_DATA}-${slot}`,
    settingsBase: `${PRIMARY_SETTINGS_BASE} - ${slot}`
  };
}

/**
 * Make every slot able to decrypt what the other slots wrote.
 *
 * THIS IS WHY SHARING THE CREDENTIAL FILE WAS NOT ENOUGH
 *
 *   1.19.3 moved secrets.json and license.json to one path for the machine,
 *   on the reasoning that DPAPI binds a blob to the Windows ACCOUNT, so any
 *   slot on that account could read it. That reasoning was wrong about which
 *   mechanism `safeStorage` actually uses.
 *
 *   Electron's safeStorage on Windows is Chromium's os_crypt. os_crypt does
 *   not DPAPI-encrypt the data: it generates a RANDOM AES KEY PER PROFILE,
 *   DPAPI-protects that key, and stores it in `<userData>/Local State` under
 *   `os_crypt.encrypted_key`. Every slot has its own userData - it must, a
 *   Chromium profile takes an exclusive LevelDB lock - so every slot had its
 *   own random key. Measured on this machine: four profiles, four different
 *   keys.
 *
 *   So window 2 held the shared ciphertext and none of the key. It reported
 *   exactly that - "4 credential không giải mã được" - and the app showed
 *   "Chưa nhập License Key". The DPAPI warning was never false; it was the
 *   only true thing on the screen, and 1.19.3 dismissed it as noise.
 *
 * THE FIX
 *
 *   Copy the PRIMARY profile's `os_crypt.encrypted_key` into this slot's
 *   `Local State` before Chromium reads it. The blob is DPAPI-protected for
 *   the Windows account, so this account - and only this account - can unwrap
 *   it in any profile. Every slot then derives the same AES key, and the
 *   shared credential file finally means what 1.19.3 intended it to mean.
 *
 * THE COST, STATED RATHER THAN DISCOVERED
 *
 *   os_crypt also encrypts that profile's COOKIES. A slot whose key changes
 *   can no longer read the cookies it wrote under the old key, so an existing
 *   slot 2+ is signed out of OpenSea once and signs in again. That happens
 *   one time per slot, and it buys the thing the owner asked for: type the
 *   licence, the API key and the private key once per machine.
 *
 *   Nothing else in the profile is touched, and every other field in
 *   `Local State` is preserved.
 */
function shareCryptoKey(userDataDir) {
  // Slot 1 IS the primary. Nothing to copy, and nothing may be written to it:
  // its key is the canonical one every other slot adopts.
  if (userDataDir === DEFAULT_USER_DATA) return "primary";

  const primaryFile = path.join(DEFAULT_USER_DATA, "Local State");
  const slotFile = path.join(userDataDir, "Local State");

  let master;
  try {
    master = JSON.parse(fs.readFileSync(primaryFile, "utf8"));
  } catch {
    // The primary has never run, or never used safeStorage. There is no
    // canonical key yet; this slot makes its own and adopts on a later run.
    return "no-primary-key";
  }

  const key = (master && master.os_crypt && master.os_crypt.encrypted_key) || "";
  if (!key) return "no-primary-key";

  let slotState = {};
  try {
    slotState = JSON.parse(fs.readFileSync(slotFile, "utf8")) || {};
  } catch {
    // A brand new profile. Seeding the file means Chromium adopts this key
    // instead of generating one, so a fresh slot never loses a cookie.
    slotState = {};
  }

  if (slotState.os_crypt && slotState.os_crypt.encrypted_key === key) {
    return "already-shared";
  }

  const had = Boolean(slotState.os_crypt && slotState.os_crypt.encrypted_key);
  slotState.os_crypt = { ...(slotState.os_crypt || {}), encrypted_key: key };

  try {
    fs.mkdirSync(userDataDir, { recursive: true });
    // Same temp-then-rename discipline the credential store uses: a half
    // written Local State would cost this profile far more than a cookie.
    const temp = `${slotFile}.${process.pid}.tmp`;
    fs.writeFileSync(temp, JSON.stringify(slotState), "utf8");
    fs.renameSync(temp, slotFile);
  } catch {
    return "failed";
  }

  return had ? "replaced" : "seeded";
}

/**
 * Claim a slot and point this process's storage at it.
 *
 * Must run before anything reads userData and before Chromium opens a profile,
 * which is why this module is required first in main.js and claims on require.
 */
function claim() {
  if (state) return state;

  try {
    fs.mkdirSync(LOCK_DIR, { recursive: true });
  } catch {
    /* falls through to the unlocked path below */
  }

  let slot = 0;
  for (let candidate = 1; candidate <= MAX_INSTANCES; candidate++) {
    if (tryClaim(candidate)) {
      slot = candidate;
      break;
    }
  }

  // Every slot taken, or the lock directory is unwritable. Running anyway on a
  // shared profile is what produced the silent session collisions this module
  // exists to prevent, so it refuses instead - loudly, and only for the extra
  // copy the user just opened.
  const overflow = slot === 0;
  if (overflow) slot = 1;

  const paths = pathsFor(slot);
  const fresh = !fs.existsSync(paths.settingsBase);

  // BEFORE setPath, and before anything can touch safeStorage: the key has to
  // be in place when Chromium first reads this profile's Local State.
  const cryptoShare = overflow ? "primary" : shareCryptoKey(paths.userData);

  if (!overflow && slot !== 1) app.setPath("userData", paths.userData);

  state = {
    slot,
    overflow,
    pid: process.pid,
    /**
     * What happened to this slot's os_crypt key. Reported at boot so a window
     * that cannot read the shared credentials says WHY in one line instead of
     * blaming DPAPI. Never contains key material.
     */
    cryptoShare,
    // Stable across restart/update. The lock already guarantees that only one
    // live process owns a Tool slot; the pid must not become persistence ID.
    instanceId: `tool-${slot}`,
    userData: overflow ? DEFAULT_USER_DATA : paths.userData,
    settingsBase: overflow ? PRIMARY_SETTINGS_BASE : paths.settingsBase,
    primarySettingsBase: PRIMARY_SETTINGS_BASE,
    // True the first time a new slot runs: its settings are seeded from slot 1
    // and the Private Key is deliberately NOT carried over.
    freshProfile: fresh && slot !== 1,
    lockFile: lockPathFor(slot)
  };

  return state;
}

/** Hand the slot back so the next instance can have it. */
function release() {
  if (heartbeat) {
    clearInterval(heartbeat);
    heartbeat = null;
  }
  if (!state || state.overflow) return;
  try {
    const holder = Number.parseInt(fs.readFileSync(state.lockFile, "utf8").trim(), 10);
    if (holder === process.pid) fs.unlinkSync(state.lockFile);
  } catch {
    /* the lock is already gone, or was never ours */
  }
}

function get() {
  return state || claim();
}

// Claimed on require: main.js requires this before anything that reads a path.
claim();

// A crash must not cost a slot forever. The lock also carries a pid, so a
// stale one is reclaimed by the next instance regardless - this is the tidy
// path, not the only one.
process.on("exit", release);

module.exports = { claim, release, get, MAX_INSTANCES, LOCK_DIR };
