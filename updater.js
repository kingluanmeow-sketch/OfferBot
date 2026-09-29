"use strict";

/**
 * updater.js — the app updates itself.
 *
 * A user should never have to find a download page, run an installer by hand,
 * or be told to "reinstall". They press one button.
 *
 * WHAT THIS IS NOT ALLOWED TO TOUCH
 *
 *   Their data. An update replaces the program, never the profile: the device
 *   identity under %LOCALAPPDATA%, the encrypted credentials, settings, the
 *   licence. NSIS is configured with deleteAppDataOnUninstall:false and this
 *   module writes nothing outside its own state - but the rule is stated here
 *   because an updater is exactly the component that would break it.
 *
 * WHERE UPDATES COME FROM
 *
 *   The GitHub release the build was published to. Public: the running app
 *   presents no credential and needs none. A token exists only on the machine
 *   that PUBLISHES, in the environment, for the duration of one command.
 *
 * SIGNATURES
 *
 *   electron-updater verifies the installer's Authenticode signature against
 *   the publisher name before running it, and that check is left on. An
 *   unsigned or differently-signed binary is refused rather than installed -
 *   which is the whole reason auto-update is safe to offer at all.
 */

const { autoUpdater } = require("electron-updater");
const errors = require("./error-registry");

/**
 * What the window is told.
 *
 * One shape for every phase, so the renderer never has to assemble a state out
 * of several events arriving in an order it cannot control.
 *
 * @typedef {object} UpdateState
 * @property {"IDLE"|"CHECKING"|"AVAILABLE"|"DOWNLOADING"|"READY"|"UP_TO_DATE"|"ERROR"} phase
 * @property {string} currentVersion
 * @property {string} newVersion     "" unless one was found
 * @property {number} percent        0-100 while downloading
 * @property {number} transferred    bytes
 * @property {number} total          bytes
 * @property {string} code           error code, "" when fine
 * @property {string} message        Vietnamese, for a person
 */

const PHASE = Object.freeze({
  IDLE: "IDLE",
  CHECKING: "CHECKING",
  AVAILABLE: "AVAILABLE",
  DOWNLOADING: "DOWNLOADING",
  READY: "READY",
  UP_TO_DATE: "UP_TO_DATE",
  ERROR: "ERROR"
});

class Updater {
  /**
   * @param {object} options
   * @param {string} options.currentVersion
   * @param {(state:UpdateState)=>void} options.onState  pushed to the window
   * @param {(line:string)=>void} [options.onLog]        never receives a token
   */
  constructor({ currentVersion, onState, onLog, onBeforeInstall }) {
    this.currentVersion = String(currentVersion || "");
    this.onState = typeof onState === "function" ? onState : () => {};
    this.onLog = typeof onLog === "function" ? onLog : () => {};
    /** Chủ app ghi snapshot ở đây, đồng bộ, ngay trước khi giao cho installer. */
    this.onBeforeInstall = typeof onBeforeInstall === "function" ? onBeforeInstall : () => {};

    this.state = {
      phase: PHASE.IDLE,
      currentVersion: this.currentVersion,
      newVersion: "",
      percent: 0,
      transferred: 0,
      total: 0,
      code: "",
      message: ""
    };

    /** Nothing is downloaded until the user asks for it. */
    autoUpdater.autoDownload = false;

    /**
     * Installing on quit is deliberately OFF.
     *
     * An update that applies itself when the app closes can replace the binary
     * in the middle of a run the user thought was still theirs. The restart is
     * an explicit button instead.
     */
    autoUpdater.autoInstallOnAppQuit = false;

    // electron-updater logs its own progress; route it somewhere that redacts.
    autoUpdater.logger = {
      info: line => this.onLog(`[UPDATE] ${errors.redact(String(line))}`),
      warn: line => this.onLog(`[UPDATE] ${errors.redact(String(line))}`),
      error: line => this.onLog(`[UPDATE] ${errors.redact(String(line))}`),
      debug: () => {}
    };

    this.wire();
  }

  /** @param {Partial<UpdateState>} patch */
  set(patch) {
    this.state = { ...this.state, ...patch };
    this.onState(this.state);
    return this.state;
  }

  wire() {
    autoUpdater.on("checking-for-update", () => {
      this.set({ phase: PHASE.CHECKING, code: "", message: "Đang kiểm tra bản mới…" });
    });

    autoUpdater.on("update-available", info => {
      const version = String((info && info.version) || "");
      this.onLog(`[UPDATE] có bản mới ${version}`);
      this.set({
        phase: PHASE.AVAILABLE,
        newVersion: version,
        code: "",
        message: `Đã có phiên bản ${version}.`
      });
    });

    autoUpdater.on("update-not-available", () => {
      this.set({
        phase: PHASE.UP_TO_DATE,
        newVersion: "",
        code: "",
        message: "Bạn đang dùng phiên bản mới nhất."
      });
    });

    autoUpdater.on("download-progress", progress => {
      const percent = Math.max(0, Math.min(100, Number(progress.percent) || 0));
      this.set({
        phase: PHASE.DOWNLOADING,
        percent,
        transferred: Number(progress.transferred) || 0,
        total: Number(progress.total) || 0,
        code: "",
        message: `Đang tải bản mới… ${percent.toFixed(0)}%`
      });
    });

    autoUpdater.on("update-downloaded", info => {
      const version = String((info && info.version) || this.state.newVersion || "");
      this.onLog(`[UPDATE] đã tải xong ${version}`);
      this.set({
        phase: PHASE.READY,
        newVersion: version,
        percent: 100,
        code: "",
        message: `Đã tải xong phiên bản ${version}. Khởi động lại để cập nhật.`
      });
    });

    autoUpdater.on("error", error => this.fail(error));
  }

  /**
   * Turn an updater failure into something a user can act on.
   *
   * The raw messages are network stacks and yaml parse errors; none of them
   * tells anyone what to do. The code is what makes the failure findable later.
   */
  fail(error) {
    const raw = String((error && error.message) || error || "");
    const code = classifyUpdateError(raw);
    const entry = errors.get(code);

    // Redacted: an updater error can carry a URL with a token query on it.
    this.onLog(`[UPDATE] lỗi · ${errors.redact(raw)}`);

    return this.set({
      phase: PHASE.ERROR,
      code,
      message: entry ? entry.userMessage : "Không cập nhật được."
    });
  }

  /** Ask GitHub whether anything newer exists. */
  async check() {
    try {
      this.set({ phase: PHASE.CHECKING, code: "", message: "Đang kiểm tra bản mới…" });
      await autoUpdater.checkForUpdates();
      return this.state;
    } catch (error) {
      return this.fail(error);
    }
  }

  /** Download the update the user just agreed to. */
  async download() {
    if (this.state.phase !== PHASE.AVAILABLE &&
        this.state.phase !== PHASE.ERROR) {
      return this.state;
    }
    try {
      this.set({ phase: PHASE.DOWNLOADING, percent: 0, code: "", message: "Đang tải bản mới…" });
      await autoUpdater.downloadUpdate();
      return this.state;
    } catch (error) {
      return this.fail(error);
    }
  }

  /**
   * Restart into the new version.
   *
   * `isSilent: false` shows the installer, and `isForceRunAfter: true` brings
   * the app back afterwards. Refused unless a download actually finished: a
   * quitAndInstall with nothing staged closes the app and installs nothing,
   * which to the user is the app simply vanishing.
   */
  install() {
    if (this.state.phase !== PHASE.READY) {
      return {
        ok: false,
        code: "E_UPDATE_NOT_READY",
        error: "Chưa tải xong bản mới."
      };
    }
    this.onLog(`[UPDATE] cài đặt ${this.state.newVersion} và khởi động lại`);
    // Autosave trước khi rời tay: chủ app ghi snapshot ở đây, đồng bộ.
    try { this.onBeforeInstall(); } catch (error) { this.onLog(`[UPDATE] pre-install save lỗi: ${error.message}`); }
    setImmediate(() => autoUpdater.quitAndInstall(false, true));
    return { ok: true };
  }

  status() {
    return { ...this.state };
  }
}

/**
 * Which failure is this?
 *
 * Kept narrow and honest: only the shapes that are actually distinguishable
 * from the message get their own code, and everything else is the generic one
 * rather than a guess dressed up as a diagnosis.
 */
function classifyUpdateError(raw) {
  const text = String(raw || "").toLowerCase();

  if (/enotfound|econnrefused|econnreset|socket|network|getaddrinfo|timeout/.test(text)) {
    return "E_UPDATE_NETWORK";
  }
  if (/404|no published versions|cannot find/.test(text)) {
    return "E_UPDATE_NOT_FOUND";
  }
  if (/signature|not signed|publisher|integrity|sha512|checksum/.test(text)) {
    return "E_UPDATE_SIGNATURE";
  }
  if (/permission|eperm|eacces|access is denied/.test(text)) {
    return "E_UPDATE_PERMISSION";
  }
  return "E_UPDATE_FAILED";
}

module.exports = { Updater, PHASE, classifyUpdateError, autoUpdater };
