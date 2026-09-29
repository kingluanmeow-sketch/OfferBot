"use strict";

/**
 * Licence keys with an expiry date.
 *
 * A key is a signed statement, not a password: the app only ever holds the
 * PUBLIC key, so it can verify a key but cannot mint one. The private half
 * lives in license-signing-key.SECRET.json, which is excluded from the build
 * and must never be shipped or committed.
 *
 * Format:  <tên khách>.OSB1.<base64url(payload JSON)>.<base64url(chữ ký)>
 * Payload: { v, id, name, cn, iat, nbf, exp }   iat/nbf/exp are unix seconds
 *
 * The leading name segment is a copy of the signed `cn`, so it can be read at
 * a glance and cannot be edited into someone else's name.
 *
 * Because the signature covers the payload, a user cannot extend their own
 * expiry by editing the key - any change invalidates the signature.
 */

const crypto = require("crypto");

/** Public half of the signing key. Safe to ship. */
const PUBLIC_KEY_DER_B64 =
  "MCowBQYDK2VwAyEAiIKLHHNQMtf1Xxngv+PmHmwmd9OKLTohcy91I54NRSI=";

const PREFIX = "OSB1";

/**
 * Which licence generation this build accepts.
 *
 * The payload carries `v`. A key whose `v` is anything else - including the
 * keys issued before the field existed, which have none - is refused by
 * verifyLicense, and therefore by every gate in the app and by the server at
 * activate and check.
 *
 * Bumping this retires every licence in circulation. That is the point: it is
 * how a generation is ended without touching the signing key, and without
 * "migrating" anything. An old key is not upgraded; it stops working and the
 * owner issues a new one.
 *
 * The Ed25519 keypair is NOT changed by this. The public half compiled into
 * every copy still verifies the signature; only the payload contract moved.
 */
const LICENSE_EPOCH = 4;

/**
 * The customer's name, as the first segment of their key.
 *
 * A key used to read OSB1.eyJ2Ijo... and nothing else, so the owner holding a
 * list of them could not tell whose was whose without pasting each one into
 * the Manager. Now it reads Khoa.OSB1.eyJ2Ijo...
 *
 * The slug is DERIVED from the signed payload rather than carried beside it,
 * and verifyLicense checks the two agree. That is what stops the label from
 * being decoration: editing "Khoa" to "An" at the front of a key does not
 * produce An's licence, it produces a key that no longer verifies.
 *
 * Diacritics are folded because the segment has to survive being typed,
 * emailed and pasted through anything: "Nguyễn An" is issued as NguyenAn.
 */
function nameSlug(name) {
  const folded = String(name || "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/đ/g, "d")
    .replace(/Đ/g, "D")
    .replace(/[^A-Za-z0-9]/g, "");
  return folded.slice(0, 24) || "user";
}

/** Grace window for clock skew between the issuing machine and this one. */
const CLOCK_SKEW_SECONDS = 60;

function publicKeyObject() {
  return crypto.createPublicKey({
    key: Buffer.from(PUBLIC_KEY_DER_B64, "base64"),
    format: "der",
    type: "spki"
  });
}

function b64urlEncode(buffer) {
  return Buffer.from(buffer)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function b64urlDecode(text) {
  const padded = String(text).replace(/-/g, "+").replace(/_/g, "/");
  return Buffer.from(padded, "base64");
}

/**
 * Verify a licence key.
 *
 * @param {string} key
 * @returns {{valid:boolean, reason?:string, id?:string, name?:string,
 *   expiresAt?:number, daysLeft?:number, expired?:boolean}}
 */
function verifyLicense(key) {
  const raw = String(key || "").trim();

  if (!raw) return { valid: false, reason: "Chưa nhập License Key." };

  // <tên khách>.OSB1.<payload>.<chữ ký>
  //
  // The three-segment form is every key issued before this generation. It is
  // refused here on shape, and would be refused on `v` a few lines below in
  // any case - the epoch bump and the format change are one generation.
  const parts = raw.split(".");

  // Every key issued before this generation is OSB1.<payload>.<signature>.
  // Told apart here so the customer holding one reads "your key is from the
  // old generation, ask for a new one" rather than "wrong format", which
  // sounds like they mistyped it and sends them looking for a typo.
  if (parts.length === 3 && parts[0] === PREFIX) {
    return {
      valid: false,
      legacy: true,
      reason: "License Key thuộc phiên bản cũ và đã ngừng hiệu lực. " +
        "Hãy liên hệ người bán để lấy key mới."
    };
  }

  if (parts.length !== 4 || parts[1] !== PREFIX) {
    return { valid: false, reason: "License Key sai định dạng." };
  }

  let payloadBuf;
  let signature;
  try {
    payloadBuf = b64urlDecode(parts[2]);
    signature = b64urlDecode(parts[3]);
  } catch {
    return { valid: false, reason: "License Key hỏng (không giải mã được)." };
  }

  let ok = false;
  try {
    ok = crypto.verify(null, payloadBuf, publicKeyObject(), signature);
  } catch {
    return { valid: false, reason: "License Key hỏng (chữ ký không đọc được)." };
  }

  if (!ok) {
    return { valid: false, reason: "License Key không hợp lệ (sai chữ ký)." };
  }

  let payload;
  try {
    payload = JSON.parse(payloadBuf.toString("utf8"));
  } catch {
    return { valid: false, reason: "License Key hỏng (payload không hợp lệ)." };
  }

  // The generation check comes BEFORE anything else about the payload: a key
  // from a retired generation is refused whatever else it says, and the
  // message says so rather than blaming the format.
  if (Number(payload.v) !== LICENSE_EPOCH) {
    return {
      valid: false,
      legacy: true,
      reason: "License Key thuộc phiên bản cũ và đã ngừng hiệu lực. " +
        "Hãy liên hệ người bán để lấy key mới."
    };
  }

  // The visible name has to be the signed one.
  //
  // Without this the leading segment would be a comment: anyone could retype
  // it and hand the key on under a different customer's name, and the owner
  // reading their list would believe it. The signature covers the payload,
  // and this is what binds the label to the signature.
  if (parts[0] !== payload.cn) {
    return { valid: false, reason: "License Key sai tên khách hàng." };
  }

  const exp = Number(payload.exp);
  if (!Number.isFinite(exp) || exp <= 0) {
    return { valid: false, reason: "License Key không có hạn sử dụng." };
  }

  const now = Math.floor(Date.now() / 1000);

  // A licence is a RANGE now, not a countdown. nbf is the first second it is
  // usable; a key handed over early is not usable early.
  const nbf = Number(payload.nbf);
  if (!Number.isFinite(nbf) || nbf <= 0) {
    return { valid: false, reason: "License Key không có ngày bắt đầu." };
  }
  if (nbf >= exp) {
    return { valid: false, reason: "License Key có ngày bắt đầu sau ngày hết hạn." };
  }
  if (now + CLOCK_SKEW_SECONDS < nbf) {
    return {
      valid: false,
      notStarted: true,
      id: payload.id,
      name: payload.name,
      startsAt: nbf * 1000,
      expiresAt: exp * 1000,
      daysLeft: 0,
      reason: `License chưa tới ngày bắt đầu (${new Date(nbf * 1000).toLocaleDateString("vi-VN")}).`
    };
  }
  const expiresAt = exp * 1000;
  const secondsLeft = exp - now;

  if (secondsLeft + CLOCK_SKEW_SECONDS <= 0) {
    return {
      valid: false,
      expired: true,
      id: payload.id,
      name: payload.name,
      startsAt: Number(payload.nbf) * 1000 || 0,
      expiresAt,
      daysLeft: 0,
      reason: `License đã hết hạn ngày ${new Date(expiresAt).toLocaleString("vi-VN")}.`
    };
  }

  return {
    valid: true,
    id: String(payload.id || ""),
    name: String(payload.name || ""),
    startsAt: nbf * 1000,
    expiresAt,
    daysLeft: Math.max(0, Math.floor(secondsLeft / 86400))
  };
}

/**
 * Sign a licence. Only usable where the PRIVATE key is available, i.e. the
 * owner's machine running the generator script - never inside the shipped app.
 *
 * @param {object} options
 * @param {string} options.privateKeyDerB64
 * @param {string} options.name       who the key is for
 * @param {number} options.days       validity in days from now
 * @param {string} [options.id]
 * @returns {string} the licence key
 */
function signLicense({ privateKeyDerB64, name, days, id = null,
  startAt = null, expiryAt = null, customerName = null }) {
  const now = Math.floor(Date.now() / 1000);

  // A licence is a RANGE. startAt/expiryAt are the real contract - two moments
  // the owner chose - and `days` survives only for the generator script and for
  // callers that genuinely mean "from now, for N days".
  let start;
  let end;

  if (startAt !== null || expiryAt !== null) {
    start = Math.floor(Number(startAt));
    end = Math.floor(Number(expiryAt));
    if (!Number.isFinite(start) || start <= 0) {
      throw new Error("startAt phải là mốc thời gian hợp lệ");
    }
    if (!Number.isFinite(end) || end <= 0) {
      throw new Error("expiryAt phải là mốc thời gian hợp lệ");
    }
    if (end <= start) {
      throw new Error("expiryAt phải sau startAt");
    }
  } else {
    const validDays = Number(days);
    if (!Number.isFinite(validDays) || validDays <= 0) {
      throw new Error("days phải là số > 0");
    }
    start = now;
    end = now + Math.round(validDays * 86400);
  }

  const payload = {
    // Stamped so this key can be told from every generation before it.
    v: LICENSE_EPOCH,
    id: id || crypto.randomBytes(6).toString("hex"),
    name: String(name || "user"),
    // Whose key this is, as it read the day it was minted.
    //
    // Separate from `name`, which the server fills with the licence id so the
    // owner can rename a customer without minting them a new key. That rename
    // still works: the Manager and the app both show the server's current
    // label, and this only fixes what the key itself says.
    cn: nameSlug(customerName || name),
    iat: now,
    // The first second it works, and the first second it does not.
    nbf: start,
    exp: end
  };

  const payloadBuf = Buffer.from(JSON.stringify(payload), "utf8");

  const privateKey = crypto.createPrivateKey({
    key: Buffer.from(privateKeyDerB64, "base64"),
    format: "der",
    type: "pkcs8"
  });

  const signature = crypto.sign(null, payloadBuf, privateKey);

  // The name segment is derived from the payload that was just signed, so it
  // can never drift from it - and verifyLicense refuses a key where it has.
  return `${payload.cn}.${PREFIX}.` +
    `${b64urlEncode(payloadBuf)}.${b64urlEncode(signature)}`;
}

/**
 * Live licence state for the running app.
 *
 * Re-checked on a timer as well as on every gated action, so a key that lapses
 * while the app is open stops working without needing a restart.
 */
/**
 * Why the app is locked, in one line the user can act on.
 *
 * Written here rather than in the renderer so every surface - the locked
 * screen, the status line, the log - says the same thing, and so a new status
 * cannot quietly render as a blank message.
 */
function describeLock(verdict) {
  const byStatus = {
    NOT_ACTIVATED: "Chưa kích hoạt License trên thiết bị này.",
    INVALID: "License Key không hợp lệ.",
    EXPIRED: "License đã hết hạn.",
    REVOKED: "License đã bị thu hồi.",
    PAUSED: "License đang tạm dừng. Hãy liên hệ người bán để mở lại.",
    SUSPENDED: "License đang bị đình chỉ. Hãy liên hệ người bán.",
    DEVICE_REFUSED: "Thiết bị này không được phép dùng License đó.",
    DEVICE_DISABLED: "Thiết bị này đã bị vô hiệu hoá.",
    CLOCK_ROLLBACK:
      "Đồng hồ hệ thống có vẻ bị chỉnh lùi. Cần kết nối mạng để đồng bộ giờ chuẩn.",
    NOT_STARTED: "License chưa tới ngày bắt đầu.",
    LEGACY: "License Key thuộc phiên bản cũ và đã ngừng hiệu lực. " +
      "Hãy liên hệ người bán để lấy key mới.",
    SIGNATURE_INVALID: "License Key sai chữ ký.",
    OFFLINE_GRACE_OVER:
      "Đã quá lâu không liên hệ được máy chủ license. Hãy kết nối mạng để dùng tiếp.",
    UNKNOWN: "License Key không có trong hệ thống."
  };
  return verdict.reason || byStatus[verdict.status] || "License không dùng được.";
}

class LicenseGuard {
  constructor() {
    this.key = "";
    this.state = { valid: false, reason: "Chưa nhập License Key." };

    /**
     * Has the canonical store been read yet?
     *
     * "No key" and "not looked yet" are the same object above, and telling
     * them apart is not cosmetic: a window that renders before hydration
     * shows "Chưa nhập License Key" to someone whose licence is sitting on
     * disk, which is indistinguishable from the real failure and is exactly
     * what the owner reported. Nothing may render the missing-licence screen
     * until this is true.
     *
     * Set by hydrated(), which main.js calls once the shared credential store
     * has been loaded - whether or not it found anything.
     */
    this.loaded = false;

    /**
     * The server half, when one has been attached.
     *
     * Optional on purpose. The signature check works with no server at all,
     * and a guard with no client behaves exactly as it did before there was
     * one - which is what keeps every existing test and the offline story
     * honest. What a client ADDS is the two facts a signed key cannot carry:
     * revocation, and which devices may use it.
     */
    this.client = null;
  }

  /** @param {object|null} client a LicenseClient, or null to detach */
  attachClient(client) {
    this.client = client || null;
    return this.client;
  }

  setKey(key) {
    this.key = String(key || "").trim();
    this.state = verifyLicense(this.key);
    // A new key is a new activation: claim the device slot straight away
    // rather than waiting for the first gated action to discover it.
    if (this.client && this.state.valid) this.client.refreshIfStale(this.key);
    return this.state;
  }

  /**
   * Activate, because a person just asked for it.
   *
   * Pressing "Kích hoạt" must ASK THE SERVER. It used to answer from cache,
   * and the cache is exactly where the wrong answer lives: a machine that was
   * refused keeps that refusal for the length of the beat, so the customer
   * whose licence the owner had just reset was told "this licence only allows
   * 1 device" for another minute, with nothing they could press to fix it.
   *
   * refreshIfStale is the wrong call here for two reasons - it skips when the
   * cached answer is young, and it backs off after a failure. Both are right
   * for a background beat and wrong for a button.
   *
   * @returns {Promise<object>} the fresh state
   */
  async activateNow(key) {
    if (key !== undefined) this.setKey(key);
    if (this.client && this.state.valid) {
      try {
        await this.client.activateNow(this.key);
      } catch {
        // An unreachable server is not a verdict; evaluate() decides what the
        // app does next, from the cache and the grace window.
      }
    }
    return this.state;
  }

  /** Re-verify against the current clock. */
  refresh() {
    this.state = verifyLicense(this.key);
    return this.state;
  }

  /**
   * Signature first, then the server.
   *
   * The order matters: a forged or expired key is refused without a request,
   * so an unreachable server can never make an invalid licence look merely
   * offline.
   */
  evaluate() {
    const local = this.refresh();
    if (!this.client) {
      return local.valid
        ? { allowed: true, status: "ACTIVE", reason: "", source: "signature", local }
        : {
            allowed: false,
            status: local.expired ? "EXPIRED" : "INVALID",
            reason: local.reason,
            source: "signature",
            local
          };
    }

    // Never awaited: a gate answers now, from what is already known, and the
    // refresh lands in time for the next one.
    this.client.refreshIfStale(this.key);
    return this.client.evaluate(this.key);
  }

  isValid() {
    return this.evaluate().allowed === true;
  }

  /** @returns {{ok:true}|{ok:false, error:string}} */
  requireValid() {
    const verdict = this.evaluate();
    if (verdict.allowed) return { ok: true };
    return { ok: false, error: verdict.reason || "License không hợp lệ." };
  }

  /**
   * The same gate, but it WAITS for a current answer first.
   *
   * evaluate() answers from what is already known and refreshes behind it,
   * which is right for a status line drawn many times a second and wrong for
   * the moment money is about to be spent. Between the owner pressing Revoke
   * and this machine noticing there is a window, and an action that starts
   * inside that window must not be allowed to finish.
   *
   * So before submitting, cancelling, or starting a run, the answer is made
   * current first. When it is already current this costs nothing; when the
   * server is unreachable the client backs off rather than stalling, and the
   * offline grace window decides - which is its job.
   *
   * @returns {Promise<{ok:true}|{ok:false, error:string}>}
   */
  async requireValidFresh() {
    if (this.client) {
      try {
        await this.client.refreshIfStale(this.key);
      } catch {
        // A failed refresh is not a verdict. The evaluation below is.
      }
    }
    return this.requireValid();
  }

  /**
   * The canonical store has been read. Call once at boot, found or not.
   * @param {boolean} ok
   */
  hydrated(ok = true) {
    this.loaded = Boolean(ok);
    return this.loaded;
  }

  /**
   * Which of the states this actually is, as one word.
   *
   * The renderer used to infer this from `locked` plus a reason string, and
   * a string is the wrong thing to branch on: "Chưa nhập License Key" is the
   * guard's INITIAL value as well as its verdict for a machine with no
   * licence, so a window that asked too early was told the same thing as a
   * window that asked correctly.
   */
  phase() {
    if (!this.loaded) return "loading";
    if (!this.key) return "missing";
    const state = this.state || {};
    if (state.status === "SIGNATURE_INVALID" || state.status === "LEGACY") return "invalid";
    if (state.expired) return "expired";
    const verdict = this.evaluate();
    if (verdict.allowed === true) return "active";
    if (verdict.status === "OFFLINE" || verdict.status === "CHECKING") return "server_checking";
    return "invalid";
  }

  status() {
    const state = this.refresh();
    const verdict = this.evaluate();
    return {
      // See phase(). The renderer branches on THIS, never on reason text.
      phase: this.phase(),
      hydrated: this.loaded,
      valid: Boolean(verdict.allowed),
      expired: Boolean(state.expired),
      // The server's current label wins over whatever the key was minted with:
      // the name is metadata the owner can change, and the key cannot follow it.
      name: verdict.customerName || state.name || "",
      id: state.id || "",
      expiresAt: state.expiresAt || 0,
      daysLeft: state.daysLeft ?? 0,
      reason: verdict.reason || state.reason || "",
      // What the app is running on right now, so the UI can say "offline, N
      // hours of grace left" instead of simply "valid".
      serverStatus: verdict.status || "",
      source: verdict.source || "signature",
      deviceId: this.client ? this.client.deviceId : "",

      // The UI locks on exactly this, so the decision is made once, here, and
      // the renderer never has to work out what a status means.
      locked: verdict.allowed !== true,
      lockReason: verdict.allowed === true ? "" : describeLock(verdict),

      // Shown in Settings so a wrong system clock is visible as itself rather
      // than as a licence that mysteriously stopped working.
      clock: verdict.clock || null
    };
  }
}

module.exports = {
  LICENSE_EPOCH,
  PREFIX,
  PUBLIC_KEY_DER_B64,
  nameSlug,
  verifyLicense,
  signLicense,
  LicenseGuard
};
