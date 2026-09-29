"use strict";

/**
 * The only bridge between the renderer and Node.
 *
 * contextIsolation is on and nodeIntegration is off, so the renderer can do
 * exactly what is listed here and nothing else. Every method returns a promise
 * that resolves with a plain object - the renderer never sees an Error object
 * across the boundary.
 */

const { contextBridge, ipcRenderer } = require("electron");

/** Wrap an invoke so a main-process throw becomes {ok:false, error}. */
function invoke(channel, payload) {
  return ipcRenderer
    .invoke(channel, payload)
    .catch(error => ({ ok: false, error: error?.message || String(error) }));
}

/** Subscribe helper that returns an unsubscribe function. */
function on(channel, callback) {
  const handler = (_event, data) => {
    try {
      callback(data);
    } catch (error) {
      console.error(`[preload] ${channel} handler:`, error);
    }
  };
  ipcRenderer.on(channel, handler);
  return () => ipcRenderer.removeListener(channel, handler);
}

contextBridge.exposeInMainWorld("botAPI", {
  // ---- state ----------------------------------------------------
  getState: () => invoke("bot:getState"),
  getVersion: () => invoke("app:version"),
  getLogs: () => invoke("bot:getLogs"),
  clearLogs: () => invoke("bot:clearLogs"),
  getLicense: () => invoke("bot:license"),
  setLicense: key => invoke("bot:license", { key }),
  getDiagnostics: () => invoke("bot:diagnostics"),
  diagnosticSnapshot: () => invoke("bot:diagnosticSnapshot"),
  // Two-key LiveView: fingerprints, queues, in-flight, cooldown, 429s and the
  // request distribution. Pushed every second by main; never contains a key.
  getKeyStats: () => invoke("bot:keyStats"),
  remotePairing: () => invoke("remote:pairing"),

  // OpenSea browser session status is retained for account diagnostics.
  openSeaLogout: () => invoke("opensea:logout"),

  // ---- settings -------------------------------------------------
  saveSettings: settings => invoke("bot:saveSettings", settings),

  // Credentials never travel the other way: main reports only whether one is
  // stored, never its value.
  secretsStatus: () => invoke("secrets:status"),
  clearSecrets: () => invoke("secrets:clear"),

  // Auto-saved Add-NFT panel config.
  saveChainConfig: ({ chain, config }) =>
    invoke("bot:saveChainConfig", { chain, config }),
  setSaveEnabled: ({ chain, enabled }) =>
    invoke("bot:setSaveEnabled", { chain, enabled }),

  // ---- rows -----------------------------------------------------
  addNfts: ({ chain, links, config, requestId }) =>
    invoke("bot:addNfts", { chain, links, config, requestId }),

  updateNft: ({ chain, url, patch }) =>
    invoke("bot:updateNft", { chain, url, patch }),

  deleteNft: ({ chain, url }) => invoke("bot:deleteNft", { chain, url }),

  deleteNfts: ({ chain, urls }) => invoke("bot:deleteNfts", { chain, urls }),

  /** Mở hộp thoại Lưu file, xuất mỗi NFT đang lưu của `chain` thành 1 dòng link. */
  exportNfts: ({ chain }) => invoke("bot:exportNfts", { chain }),
  // This Tool's wallets: addresses/ids only, never a key.
  walletState: () => invoke("wallet:state"),
  walletActivate: ({ id }) => invoke("wallet:activate", { id }),
  walletRemoveProfile: ({ id }) => invoke("wallet:removeProfile", { id }),

  // ---- control --------------------------------------------------
  start: ({ chain, urls = null }) => invoke("bot:start", { chain, urls }),
  pause: ({ chain, urls = null }) => invoke("bot:pause", { chain, urls }),
  stop: ({ chain, urls = null }) => invoke("bot:stop", { chain, urls }),
  /** Reset on an Offer Item tab. Stops that chain only - see Engine#emergencyStop. */
  emergencyStop: ({ chain, reason = "reset" }) =>
    invoke("bot:emergencyStop", { chain, reason }),
  togglePause: ({ chain, url }) => invoke("bot:togglePause", { chain, url }),

  sessionStatus: () => invoke("session:status"),

  // ---- events ---------------------------------------------------
  onUpdate: callback => on("bot:update", callback),
  uiLog: line => ipcRenderer.send("ui:log", line),
  onSessionStatus: callback => on("session:status", callback),
  onLicenseState: callback => on("license:state", callback),
  onApiState: callback => on("api:state", callback),
  onKeyStats: callback => on("bot:keyStats", callback),
  onUpdateState: callback => on("update:state", callback),
  updateStatus: () => invoke("update:status"),
  checkUpdate: () => invoke("update:check"),
  downloadUpdate: () => invoke("update:download"),
  installUpdate: () => invoke("update:install"),
  reportUiError: payload => invoke("ui:error", payload),
  // payload: { apiKey, slot } — slot 1 = Key 1 (primary), slot 2 = Key 2
  // (secondary, may be empty to remove it).
  activateApi: payload => invoke("bot:activateApi", payload),
  onLog: callback => on("bot:log", callback),
});
