/**
 * wallet-preload.js — an EIP-1193 wallet the OpenSea page can talk to.
 *
 * The page sees an ordinary injected wallet: it can ask for accounts, ask for a
 * signature, ask to switch chains. Every request that needs the key is sent to
 * the main process over IPC; the main process signs and returns ONLY the
 * signature.
 *
 * The private key is never exposed here and never enters the page. There is no
 * IPC channel that returns it.
 */

const { contextBridge, ipcRenderer } = require("electron");

const CHANNEL = "wallet:request";

/** Requests the page may make. Anything else is refused. */
const ALLOWED = new Set([
  "eth_accounts",
  "eth_requestAccounts",
  "eth_chainId",
  "net_version",
  "personal_sign",
  "eth_sign",
  "eth_signTypedData",
  "eth_signTypedData_v3",
  "eth_signTypedData_v4",
  "eth_sendTransaction",
  "eth_getBalance",
  "eth_call",
  "eth_estimateGas",
  "eth_blockNumber",
  "eth_getTransactionByHash",
  "eth_getTransactionReceipt",
  "wallet_switchEthereumChain",
  "wallet_addEthereumChain",
  "wallet_requestPermissions",
  "wallet_getPermissions"
]);

const listeners = new Map();

function emit(event, payload) {
  for (const fn of listeners.get(event) || []) {
    try {
      fn(payload);
    } catch {
      /* a page listener throwing is not our problem */
    }
  }
}

const provider = {
  isMetaMask: false,
  isOpenSeaOfferBot: true,

  async request(args) {
    const method = args && args.method;
    const params = (args && args.params) || [];

    if (!ALLOWED.has(method)) {
      const err = new Error(`Method ${method} không được hỗ trợ.`);
      err.code = 4200;
      throw err;
    }

    const result = await ipcRenderer.invoke(CHANNEL, { method, params });

    if (result && result.error) {
      const err = new Error(result.error.message || "Wallet error");
      err.code = result.error.code || 4001;
      throw err;
    }

    return result ? result.value : null;
  },

  on(event, handler) {
    if (!listeners.has(event)) listeners.set(event, []);
    listeners.get(event).push(handler);
    return provider;
  },

  removeListener(event, handler) {
    const list = listeners.get(event) || [];
    const i = list.indexOf(handler);
    if (i >= 0) list.splice(i, 1);
    return provider;
  },

  // Legacy shims some dapps still reach for.
  async enable() {
    return provider.request({ method: "eth_requestAccounts" });
  },
  send(methodOrPayload, paramsOrCallback) {
    if (typeof methodOrPayload === "string") {
      return provider.request({ method: methodOrPayload, params: paramsOrCallback || [] });
    }
    return provider.sendAsync(methodOrPayload, paramsOrCallback);
  },
  sendAsync(payload, callback) {
    provider
      .request(payload)
      .then(value => callback(null, { id: payload.id, jsonrpc: "2.0", result: value }))
      .catch(error => callback(error));
  }
};

contextBridge.exposeInMainWorld("ethereum", provider);

// EIP-6963 discovery is NOT done here. A CustomEvent detail built in this
// isolated world reaches the page stripped: the page sees the event but gets
// `detail.info === null` and no provider, so the wallet stays invisible.
// browser.js announces from the page's own world instead, where the detail
// object survives and can carry `window.ethereum`.

// Chain changes come from the main process, not from the page.
ipcRenderer.on("wallet:event", (_event, { name, payload }) => emit(name, payload));
