/**
 * wallet.js — the main-process half of the injected wallet.
 *
 * `wallet-preload.js` exposes an EIP-1193 provider to the OpenSea page; every
 * request it receives arrives here. Read calls are proxied to an RPC; anything
 * that needs the key is signed here with ethers.
 *
 * The key stays in this process. Nothing in this file returns it, and there is
 * no IPC route that can ask for it.
 */

const { ethers } = require("ethers");
const opensea = require("./opensea");
const { logger } = require("./logger");

const TAG = "[WALLET]";

/** Read-only methods that can go straight to the chain. */
const PASSTHROUGH = new Set([
  "eth_getBalance",
  "eth_call",
  "eth_estimateGas",
  "eth_blockNumber",
  "eth_getTransactionByHash",
  "eth_getTransactionReceipt"
]);

let currentChain = "ethereum";
let getPrivateKey = () => null;

/**
 * Signing safety catch.
 *
 * This wallet answers the page without prompting anyone, so an accidental click
 * would sign for real. Everything that commits — signatures and transactions —
 * is refused unless the app has explicitly armed it for the action in hand.
 * Refused requests are still logged in full, which is what makes it safe to
 * explore OpenSea's flows and see exactly what they ask for.
 */
let armed = false;
let lastRefused = null;

const COMMITTING = new Set([
  "personal_sign",
  "eth_sign",
  "eth_signTypedData",
  "eth_signTypedData_v3",
  "eth_signTypedData_v4",
  "eth_sendTransaction"
]);

/** Sign-in needs a signature before anything is armed, so it is allowed. */
function isSignInMessage(method, params) {
  if (method !== "personal_sign") return false;
  const raw = params?.[0];
  if (typeof raw !== "string") return false;
  const text = raw.startsWith("0x")
    ? Buffer.from(raw.slice(2), "hex").toString("utf8")
    : raw;
  return /sign in|wants you to sign in|opensea\.io/i.test(text);
}

function arm(reason) {
  armed = true;
  logger.send(`${TAG} MỞ KHOÁ KÝ: ${reason}`);
}

function disarm() {
  armed = false;
}

function isArmed() {
  return armed;
}

function getLastRefused() {
  return lastRefused;
}

const providers = new Map();
const chainIds = new Map();

/** main.js supplies this so the key is read fresh from settings each time. */
function configure({ privateKeyProvider, chain = "ethereum" }) {
  if (typeof privateKeyProvider === "function") getPrivateKey = privateKeyProvider;
  currentChain = opensea.normalizeChain(chain);
}

function setChain(chain) {
  currentChain = opensea.normalizeChain(chain);
}

function providerFor(chain) {
  const c = opensea.normalizeChain(chain);
  if (!providers.has(c)) {
    providers.set(c, new ethers.JsonRpcProvider(opensea.getRpcList(c)[0]));
  }
  return providers.get(c);
}

/** Ask the chain for its id rather than hardcoding one we might get wrong. */
async function chainIdOf(chain) {
  const c = opensea.normalizeChain(chain);
  if (chainIds.has(c)) return chainIds.get(c);

  const network = await providerFor(c).getNetwork();
  const id = Number(network.chainId);
  chainIds.set(c, id);
  return id;
}

function walletFor(chain) {
  const key = getPrivateKey();
  if (!key) throw Object.assign(new Error("Chưa có Private Key trong Settings."), { code: 4100 });
  return new ethers.Wallet(key, providerFor(chain));
}

function addressOf() {
  const key = getPrivateKey();
  if (!key) return null;
  try {
    return new ethers.Wallet(key).address;
  } catch {
    return null;
  }
}

/**
 * Typed-data signing.
 *
 * ethers derives EIP712Domain itself and rejects the payload if it is still
 * listed, so it is stripped here. Everything else is passed through untouched,
 * including a Seaport BulkOrder tree - that is what makes one signature cover
 * many orders.
 */
async function signTypedData(chain, payload) {
  const data = typeof payload === "string" ? JSON.parse(payload) : payload;

  const types = { ...(data.types || {}) };
  delete types.EIP712Domain;

  const wallet = walletFor(chain);
  const signature = await wallet.signTypedData(data.domain || {}, types, data.message || {});

  const primary = data.primaryType || Object.keys(types)[0] || "?";
  logger.send(`${TAG} đã ký typed data (${primary})`);

  return signature;
}

/** Handle one EIP-1193 request. Returns { value } or { error }. */
async function handle({ method, params = [] }) {
  try {
    // Record and refuse anything that would commit, unless armed. Signing in is
    // exempt: it costs nothing and nothing else works without it.
    if (COMMITTING.has(method) && !armed && !isSignInMessage(method, params)) {
      lastRefused = { at: Date.now(), method, params };
      logger.send(`${TAG} CHẶN ${method} — ví chưa được mở khoá`);
      return {
        error: {
          message: `${method} bị chặn: ví chưa mở khoá cho thao tác này.`,
          code: 4001
        }
      };
    }

    switch (method) {
      case "eth_accounts":
      case "eth_requestAccounts": {
        const address = addressOf();
        if (!address) throw Object.assign(new Error("Chưa có Private Key."), { code: 4100 });
        return { value: [address] };
      }

      case "eth_chainId": {
        const id = await chainIdOf(currentChain);
        return { value: "0x" + id.toString(16) };
      }

      case "net_version": {
        const id = await chainIdOf(currentChain);
        return { value: String(id) };
      }

      case "personal_sign":
      case "eth_sign": {
        // personal_sign is (message, address); eth_sign is (address, message).
        const [a, b] = params;
        const message = method === "personal_sign" ? a : b;
        const wallet = walletFor(currentChain);
        const bytes = typeof message === "string" && message.startsWith("0x")
          ? ethers.getBytes(message)
          : message;
        const value = await wallet.signMessage(bytes);
        logger.send(`${TAG} đã ký message`);
        return { value };
      }

      case "eth_signTypedData":
      case "eth_signTypedData_v3":
      case "eth_signTypedData_v4": {
        // Params are (address, data) except for the oldest form.
        const data = method === "eth_signTypedData" ? params[0] : params[1];
        return { value: await signTypedData(currentChain, data) };
      }

      case "eth_sendTransaction": {
        const [tx] = params;
        const wallet = walletFor(currentChain);
        const sent = await wallet.sendTransaction({
          to: tx.to,
          data: tx.data,
          value: tx.value ? BigInt(tx.value) : undefined,
          gasLimit: tx.gas ? BigInt(tx.gas) : undefined
        });
        logger.send(`${TAG} đã gửi tx ${sent.hash}`);
        return { value: sent.hash };
      }

      case "wallet_switchEthereumChain": {
        const [{ chainId }] = params;
        const wanted = Number(chainId);
        for (const c of ["ethereum", "robinhood"]) {
          if ((await chainIdOf(c)) === wanted) {
            setChain(c);
            logger.send(`${TAG} chuyển sang ${c}`);
            return { value: null };
          }
        }
        throw Object.assign(new Error(`Chain ${chainId} không được hỗ trợ.`), { code: 4902 });
      }

      case "wallet_addEthereumChain":
        return { value: null };

      case "wallet_requestPermissions":
      case "wallet_getPermissions":
        return { value: [{ parentCapability: "eth_accounts" }] };

      default: {
        if (PASSTHROUGH.has(method)) {
          const value = await providerFor(currentChain).send(method, params);
          return { value };
        }
        throw Object.assign(new Error(`Method ${method} không hỗ trợ.`), { code: 4200 });
      }
    }
  } catch (error) {
    logger.send(`${TAG} lỗi ${method}: ${error.message}`);
    return { error: { message: error.message, code: error.code || 4001 } };
  }
}

module.exports = {
  configure,
  setChain,
  handle,
  addressOf,
  chainIdOf,
  signTypedData,
  arm,
  disarm,
  isArmed,
  getLastRefused
};
