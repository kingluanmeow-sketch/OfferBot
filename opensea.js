"use strict";

/**
 * OpenSea REST layer.
 *
 * Responsibilities:
 *  - chain constants (WETH, RPC, conduit keys) shared with seaport.js
 *  - every HTTP call funnelled through the global rate limiter + key failover
 *  - Best Offer resolution that is correct for item / collection / trait offers
 *  - My Offer discovery
 *  - read-side helpers for the Cancel tab
 *
 * This module must NEVER require seaport.js (seaport.js requires this one).
 */

const axios = require("axios");
const https = require("https");
const { ethers } = require("ethers");

const { logger } = require("./logger");
const rateLimiter = require("./rate-limiter");
const { KIND, PRIORITY, apiKeys } = rateLimiter;
const { ReadDispatcher } = require("./read-dispatcher");
const { SingleFlight } = require("./single-flight");
const readDispatcher = new ReadDispatcher({ keys: apiKeys });
const readFlights = new SingleFlight();
const {
  nftMetaCache,
  collectionFeeCache,
  collectionSlugCache,
  tokenStandardCache,
  collectionOffersCache,
  bestOfferCache
} = require("./cache");

// ------------------------------------------------------------------
// Constants
// ------------------------------------------------------------------

/**
 * Where OpenSea is.
 *
 * Overridable, but ONLY to a loopback address. A test needs to point the
 * packaged app at a server it controls so it can say what OpenSea received;
 * an env var that could name any host would be a way to send this app's API
 * key somewhere else, which is not a trade worth making for a test. 127.0.0.1
 * and localhost are the whole allowlist, and anything else is ignored with a
 * line in the log rather than silently obeyed.
 */
function resolveApiBase() {
  const fallback = "https://api.opensea.io/api/v2";
  const wanted = String(process.env.OSB_OPENSEA_API_BASE || "").trim();
  if (!wanted) return fallback;

  try {
    const host = new URL(wanted).hostname.toLowerCase();
    const loopback = host === "localhost" || host === "::1" || host.startsWith("127.");
    if (loopback) return wanted.replace(/\/+$/, "");
    process.stderr.write(
      "[OPENSEA] bỏ qua OSB_OPENSEA_API_BASE: chỉ nhận địa chỉ loopback\n");
  } catch {
    process.stderr.write("[OPENSEA] bỏ qua OSB_OPENSEA_API_BASE: địa chỉ không hợp lệ\n");
  }
  return fallback;
}

const OPENSEA_API_BASE = resolveApiBase();

const SEAPORT_V1_6 = "0x0000000000000068f116a894984e2db1123eb395";
const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";
const ZERO_BYTES32 =
  "0x0000000000000000000000000000000000000000000000000000000000000000";

const OPENSEA_SIGNED_ZONE_V2 = "0x000056f7000000ece9003ca63978907a00ffd100";
const OPENSEA_FEE_RECIPIENT = "0x0000a26b00c1F0DF003000390027140000fAa719";
const OPENSEA_FEE_BPS = 100;

const OPENSEA_CONDUIT_KEYS = {
  ethereum:
    "0x0000007b02230091a7ed01230072f7006a004d60a8d4e71d599b8104250f0000",
  polygon:
    "0x0000007b02230091a7ed01230072f7006a004d60a8d4e71d599b8104250f0000",
  robinhood:
    "0x61159fefdfada89302ed55f8b9e89e2d67d8258712b3a3f89aa88525877f1d5e"
};

const WETH = {
  ethereum: "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
  polygon: "0x7ceB23fD6bC0adD59E62ac25578270cFf1b9f619",
  robinhood: "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"
};

const RPC_URLS = {
  // Measured 2026-09-23: cloudflare-eth.com answers every call with -32046
  // "Cannot fulfill request" (gateway discontinued) and eth.llamarpc.com with
  // Cloudflare 525 — with only those two behind a timing-out publicnode, the
  // wallet session / balance / counter had NO working endpoint. drpc and
  // mevblocker answered every probe; the rest are depth.
  ethereum: [
    "https://eth.drpc.org",
    "https://rpc.mevblocker.io",
    "https://ethereum-rpc.publicnode.com",
    "https://1rpc.io/eth",
    "https://rpc.flashbots.net"
  ],
  polygon: [
    "https://polygon-bor-rpc.publicnode.com",
    "https://polygon.llamarpc.com",
    "https://polygon-rpc.com"
  ],
  robinhood: ["https://rpc.mainnet.chain.robinhood.com"]
};

/** The two chains the UI exposes. Both dashboards render from this list. */
const SUPPORTED_CHAINS = Object.freeze(["ethereum", "robinhood"]);

const httpsAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 24,
  minVersion: "TLSv1.2",
  maxVersion: "TLSv1.3"
});

// ------------------------------------------------------------------
// Chain helpers
// ------------------------------------------------------------------

function normalizeChain(chain) {
  let c = String(chain || "ethereum").trim().toLowerCase();
  if (c === "eth") c = "ethereum";
  if (c === "poly" || c === "matic") c = "polygon";
  if (c === "hood" || c === "robinhood-chain" || c === "robinhoodchain") {
    c = "robinhood";
  }
  return c;
}

function getWeth(chain) {
  const c = normalizeChain(chain);
  const address = WETH[c];
  if (!address) throw new Error(`Chain khong ho tro WETH: ${c}`);
  return address;
}

function getRpcList(chain) {
  const c = normalizeChain(chain);
  const list = RPC_URLS[c];
  if (!list) throw new Error(`Chain khong ho tro: ${c}`);
  return list;
}

function getOpenSeaConduitKey(chain) {
  const c = normalizeChain(chain);
  return OPENSEA_CONDUIT_KEYS[c] || OPENSEA_CONDUIT_KEYS.ethereum;
}

function getApiHeaders(apiKey) {
  const headers = {
    Accept: "application/json",
    "Content-Type": "application/json",
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
    "User-Agent": "OpenSea-Offer-Bot/1.0.0"
  };
  const key = String(apiKey || "").trim();
  if (key) headers["X-API-KEY"] = key;
  return headers;
}

// ------------------------------------------------------------------
// Credential health
// ------------------------------------------------------------------

/**
 * Told when a request fails in a way that says something about the API KEY.
 *
 * An observer, never a decision: this module keeps classifying, retrying,
 * resting keys and throwing exactly as before, and whoever registers here
 * decides what a credential failure means for the app. Kept as a hook rather
 * than an import so this file gains no opinion about locking.
 */
let onCredentialResult = null;

function setCredentialWatcher(fn) {
  onCredentialResult = typeof fn === "function" ? fn : null;
}

/** Report one settled request. Never throws into the request path. */
function reportCredential(ok, error, context) {
  if (!onCredentialResult) return;
  try {
    onCredentialResult({ ok, error: error || null, context: context || "" });
  } catch {
    /* a watcher must never break a request */
  }
}

// ------------------------------------------------------------------
// Error classification
// ------------------------------------------------------------------

function isRateLimitError(error) {
  const status = error?.response?.status;
  if (status === 429) return true;

  const text = String(
    error?.response?.data
      ? typeof error.response.data === "string"
        ? error.response.data
        : JSON.stringify(error.response.data)
      : error?.message || ""
  ).toLowerCase();

  return (
    text.includes("rate limit") ||
    text.includes("too many requests") ||
    text.includes("ratelimit")
  );
}

/** Spec 18: every phrasing OpenSea / the RPC uses for "you cannot pay". */
const LOW_BALANCE_PATTERNS = [
  "insufficient balance",
  "not enough balance",
  "does not have enough balance",
  "erc20 insufficient balance",
  "insufficient funds",
  "insufficient weth",
  "balance is too low",
  "exceeds balance"
];

function isInsufficientBalanceError(errorOrText) {
  const raw =
    typeof errorOrText === "string"
      ? errorOrText
      : errorOrText?.response?.data
        ? typeof errorOrText.response.data === "string"
          ? errorOrText.response.data
          : JSON.stringify(errorOrText.response.data)
        : errorOrText?.message || String(errorOrText || "");

  const text = raw.toLowerCase();
  return LOW_BALANCE_PATTERNS.some(pattern => text.includes(pattern));
}

/**
 * Why a submit failed, in the only two categories a retry policy cares about.
 *
 *   "transient"  the same payload could well work in a moment - a timeout, a
 *                reset socket, a 429, a 5xx. Retrying is the right answer.
 *
 *   "permanent"  the SERVER READ THE PAYLOAD AND REJECTED IT. A 400 from a
 *                validation check says the order is malformed or does not
 *                match the contract; sending the identical bytes again gets
 *                the identical answer. Retrying is not persistence, it is a
 *                loop - one NFT sent the same rejected order dozens of times
 *                in a few minutes and spent the whole write budget on it.
 *
 * The distinction is made on the STATUS CODE first, because that is what the
 * server actually told us. 4xx other than 408/429 is the client's fault by
 * definition, and that means ours.
 */
const PERMANENT_SUBMIT_PATTERNS = [
  // Thiếu ETH trả gas. Thử lại bốn lần không làm ví có thêm tiền — nó chỉ
  // bắt người dùng chờ lâu hơn để nhận đúng câu trả lời đã biết.
  "insufficient funds",
  "not enough funds",
  "gas required exceeds allowance",
  "không đủ eth",
  "itemtype does not match",
  "does not match the standard of the contract",
  "validation error",
  "invalid order",
  "malformed",
  "unsupported",
  "is not a valid address",
  "invalid signature"
];

function classifySubmitError(errorOrText) {
  const status =
    typeof errorOrText === "object" && errorOrText
      ? errorOrText.response?.status || errorOrText.status || 0
      : 0;

  const text = String(
    typeof errorOrText === "string" ? errorOrText : describeError(errorOrText)
  ).toLowerCase();

  // Retryable by status, whatever the body says.
  if (status === 408 || status === 429) return "transient";
  if (status >= 500) return "transient";

  // A 4xx the server chose deliberately. The payload is the problem.
  if (status >= 400 && status < 500) {
    // ...except "you are short of WETH", which is about the wallet, not the
    // order, and is already handled as its own state.
    if (isInsufficientBalanceError(text)) return "transient";
    return "permanent";
  }

  // No status at all: a transport failure. Judge by the text.
  if (PERMANENT_SUBMIT_PATTERNS.some(p => text.includes(p))) return "permanent";
  return "transient";
}

/**
 * A short, stable reason for a permanent rejection, for logging and for the
 * row's blocked state. Not the whole body - that is logged once, in full.
 */
function permanentSubmitReason(errorOrText) {
  const text = String(
    typeof errorOrText === "string" ? errorOrText : describeError(errorOrText)
  ).toLowerCase();
  if (text.includes("itemtype does not match") ||
      text.includes("does not match the standard of the contract")) {
    return "itemtype-mismatch";
  }
  if (text.includes("invalid signature")) return "invalid-signature";
  if (text.includes("validation error")) return "validation-error";
  if (text.includes("invalid order")) return "invalid-order";
  if (text.includes("unsupported")) return "unsupported-contract";
  return "rejected-by-opensea";
}

function describeError(error) {
  if (!error) return "unknown error";
  if (error.response?.data) {
    try {
      const data = error.response.data;
      const body =
        typeof data === "string" ? data : JSON.stringify(data);
      return `HTTP ${error.response.status}: ${body}`;
    } catch {
      return `HTTP ${error.response.status}`;
    }
  }
  return error.message || String(error);
}

// ------------------------------------------------------------------
// HTTP core - the ONLY place axios is called
// ------------------------------------------------------------------

/**
 * @param {object} options
 * @param {"get"|"post"} options.method
 * @param {string} options.url
 * @param {object} [options.params]
 * @param {object} [options.data]
 * @param {string} [options.kind]     rate-limiter bucket
 * @param {number} [options.priority] PRIORITY.P0..P3; urgent work is served
 *   first and background work never takes the last of the capacity
 * @param {number} [options.timeout]
 * @param {number} [options.retries]  retries on 429 / 5xx
 * @param {string} [options.apiKey]   explicit key; otherwise the key manager
 */
async function request(options) {
  if ((options.method || "get").toLowerCase() !== "get") return requestOnce(options);
  const domain = require("crypto").createHash("sha256").update(String(options.apiKey ?? apiKeys.fingerprint())).digest("hex");
  const key = JSON.stringify([domain, options.url, options.params || {}, options.kind || KIND.READ]);
  return readFlights.run(key, signal => requestOnce({...options, signal}), options.signal);
}
async function requestOnce(options) {
  const {
    method = "get",
    url,
    params,
    data,
    kind = KIND.READ,
    priority = PRIORITY.P0,
    // Cancels BOTH the wait for capacity and the HTTP call itself. Without it
    // a timed-out caller leaves a waiter in the bucket that is granted later
    // and fires a request nobody wants.
    signal = null,
    timeout = 12000,
    retries = 2,
    apiKey = null,
    label = ""
  } = options || {};

  // Background work yields the limiter's reserve and the busier keys. A write
  // is never background: nobody can retry it cheaply.
  const isWrite = kind === KIND.ORDER;
  const isBackground = !isWrite && priority > rateLimiter.URGENT_PRIORITY;

  let attempt = 0;
  let lastError = null;

  while (attempt <= retries) {
    // The limiter hands back a release function; the concurrency slot is only
    // returned once this request has actually settled, so N in-flight requests
    // can never exceed the bucket's measured concurrency.
    //
    // The wait for that slot is measured: the ORDER bucket runs at 2/s with a
    // concurrency of 1, so under load this is where a write actually queues,
    // and an opaque "submit duration" would hide it.
    if (options.onStage) options.onStage("ratelimit_wait_started");
    const readLease = kind === KIND.READ ? await readDispatcher.acquire({priority, signal, apiKey}) : null;
    const release = readLease ? readLease.release : await rateLimiter.acquire(kind, priority, { signal });
    if (options.onStage) options.onStage("ratelimit_wait_finished");

    // ---- the caller takes ITS resource only now --------------------
    //
    // A submit used to hold the engine's send slot across this wait, so six
    // slots sat occupied by rows that were doing nothing but queueing for
    // write capacity at 2/s - measured at a p90 slot wait of 13s while the
    // holders were themselves idle. `beforeSend` inverts that: the caller is
    // told the moment capacity is granted and takes its slot then, so the
    // slot covers the send and nothing else.
    //
    // The lock order is fixed and one-way - rate limit first, caller resource
    // second - which is what makes it deadlock-free: nothing that holds a
    // send slot ever waits on this bucket.
    if (options.beforeSend) {
      try {
        await options.beforeSend();
      } catch (error) {
        // The grant is given straight back; nothing was sent.
        release();
        throw error;
      }
    }

    const key =
      readLease ? readLease.key : apiKey !== null
        ? apiKey
        : kind === KIND.CANCEL
          ? apiKeys.currentCancel()
          : apiKeys.current(Date.now(), {
              background: isBackground,
              write: isWrite
            });

    const sentAt = Date.now();

    try {
      const response = await axios({
        method,
        url,
        params,
        data,
        headers: getApiHeaders(key),
        timeout,
        signal,
        httpsAgent,
        validateStatus: status => status >= 200 && status < 300
      });

      if(readLease)readDispatcher.report(key,response.status);
      apiKeys.reportSuccess(key);
      // A 2xx is the only proof a credential is good, and it is proof enough
      // to clear a lock that a single bad answer put on.
      reportCredential(true, null, label || url);
      return response;
    } catch (error) {
      lastError = error;
      if(readLease)readDispatcher.report(key,error?.response?.status || 0,rateLimiter.retryDelayFromHeaders(error?.response?.headers || {},2000));
      if (signal?.aborted || error?.name === "AbortError") throw error;

      if (isRateLimitError(error)) {
        const headers = error?.response?.headers || {};

        // Do what the server told us instead of guessing: it sends
        // Retry-After and x-ratelimit-limit on a 429.
        const bucket = rateLimiter.getBucket(kind);
        bucket.adoptServerLimit(headers);

        const wait = rateLimiter.retryDelayFromHeaders(headers, 2000);

        // Rest the throttled key. With a pool the retry simply goes to another
        // key, so the wait is only needed when every key is resting.
        const haveOtherKeys = apiKeys.reportRateLimited(key, wait);

        if (haveOtherKeys) {
          logger.api(
            `429 ${label || url} → thử key khác ngay (lần ${attempt + 1}/${retries + 1})`
          );
          attempt++;
          continue;
        }

        rateLimiter.penalize(kind, wait);
        logger.api(
          `429 ${label || url} → mọi key đang nghỉ, chờ ${wait}ms ` +
            `(lần ${attempt + 1}/${retries + 1})`
        );

        attempt++;
        continue;
      }

      const status = error?.response?.status;
      if (status && status >= 500 && attempt < retries) {
        // A 5xx or a timeout may be this key's problem; rest it briefly so the
        // retry has a chance of landing somewhere healthy.
        apiKeys.reportFailure(key);
        rateLimiter.penalize(kind, 1000);
        attempt++;
        continue;
      }

      // A transport failure with no response at all: same treatment.
      if (!error?.response && attempt < retries) {
        apiKeys.reportFailure(key);
        attempt++;
        continue;
      }

      // Reported on the way out, so the watcher sees exactly the failures the
      // caller sees - not the ones that were retried away.
      reportCredential(false, error, label || url);
      throw error;
    } finally {
      release();

      // How long this key took, so a key that has gone slow loses ties to one
      // that has not. Recorded whichever way the request ended: a timeout is
      // exactly the signal worth carrying forward.
      if (apiKey === null) apiKeys.reportLatency(key, Date.now() - sentAt);

      // The key is carrying one less request. This is the ONLY place that
      // decrements: reporting it in the success and failure handlers too would
      // subtract twice and skew every later choice.
      if (!readLease && apiKey === null) apiKeys.releaseKey(key, { write: isWrite });
    }
  }

  throw lastError || new Error("Request failed");
}

/**
 * Does THIS key work?
 *
 * The key is passed explicitly so the pool is bypassed: activation has to
 * test the key the user just typed, not whichever key rotation would have
 * picked, or a wrong key could be accepted because a different one answered.
 *
 * A cheap authenticated read, no retries, and a short timeout: an activation
 * that takes half a minute to say no is an activation nobody waits for. The
 * error is thrown as-is so the caller can tell a refusal from a bad moment.
 *
 * Deliberately does NOT go through the credential watcher's success path
 * being enough on its own - main sets the state explicitly after storing.
 */
async function probeApiKey(key) {
  const response = await request({
    method: "get",
    // OPENSEA_API_BASE already carries /api/v2.
    url: `${OPENSEA_API_BASE}/collections`,
    params: { limit: 1 },
    kind: KIND.READ,
    priority: PRIORITY.P0,
    timeout: 12000,
    retries: 0,
    apiKey: String(key || ""),
    label: "kiểm tra API key"
  });
  return { ok: true, status: response.status };
}

function apiGet(path, opts = {}) {
  return request({ ...opts, method: "get", url: `${OPENSEA_API_BASE}${path}` });
}

function apiPost(path, data, opts = {}) {
  return request({
    ...opts,
    method: "post",
    url: `${OPENSEA_API_BASE}${path}`,
    data
  });
}

// ------------------------------------------------------------------
// URL parsing
// ------------------------------------------------------------------

/**
 * Accepts:
 *   https://opensea.io/assets/ethereum/0xabc.../123
 *   https://opensea.io/item/ethereum/0xabc.../123
 *   https://opensea.io/assets/robinhood/0xabc.../123
 *   ethereum/0xabc.../123
 *   0xabc.../123          (chain defaults to the dashboard chain)
 */
function parseOpenSeaUrl(url, defaultChain = "ethereum") {
  const original = String(url || "").trim();
  if (!original) return null;

  const cleaned = original
    .replace(/^https?:\/\//i, "")
    .replace(/^(www\.|testnets\.)/i, "")
    .replace(/^opensea\.io\//i, "")
    .replace(/[?#].*$/, "")
    .replace(/\/+$/, "");

  const segments = cleaned.split("/").filter(Boolean);

  // Drop a leading "assets" / "item" / "collection" marker.
  while (
    segments.length &&
    /^(assets|asset|item|items)$/i.test(segments[0])
  ) {
    segments.shift();
  }

  let chain = "";
  let contract = "";
  let tokenId = "";

  if (segments.length >= 3) {
    chain = segments[0];
    contract = segments[1];
    tokenId = segments[2];
  } else if (segments.length === 2) {
    chain = defaultChain;
    contract = segments[0];
    tokenId = segments[1];
  } else {
    return null;
  }

  if (!/^0x[a-fA-F0-9]{40}$/.test(contract)) return null;
  if (!/^\d+$/.test(String(tokenId))) return null;

  let normalizedContract;
  try {
    normalizedContract = ethers.getAddress(contract).toLowerCase();
  } catch {
    return null;
  }

  const normalizedChain = normalizeChain(chain);

  return {
    url: `https://opensea.io/assets/${normalizedChain}/${normalizedContract}/${tokenId}`,
    original,
    chain: normalizedChain,
    contract: normalizedContract,
    tokenId: String(tokenId)
  };
}

/**
 * The canonical item URL, built rather than parsed.
 *
 * Same string `parseOpenSeaUrl` returns, from the same three parts, so a link
 * this produces and a link a user pasted normalise to exactly one form:
 * `/assets/` (never `/item/`), lowercase contract, no query, no fragment.
 * Kept beside the parser deliberately - two places that build this URL is two
 * places for them to disagree, and a URL that disagrees dedupes as two NFTs.
 *
 * @returns {string} empty when the parts are not a real item
 */
function canonicalItemUrl(chain, contract, tokenId) {
  const parsed = parseOpenSeaUrl(
    `${chain}/${contract}/${tokenId}`,
    normalizeChain(chain)
  );
  return parsed ? parsed.url : "";
}

/** Formats that play by themselves and cost memory and CPU per visible row. */
const ANIMATED_EXTENSIONS = /\.(gif|mp4|webm|mov|avif|apng)(\?|#|$)/i;

/** OpenSea's own image CDN, which can resize and flatten on request. */
const OPENSEA_CDN = /(^|\.)seadn\.io$/i;

/**
 * Turn a metadata image into something cheap to render.
 *
 * Rows show a thumbnail a few dozen pixels wide, so pulling the full asset -
 * and worse, an animated one that keeps decoding frames for as long as it is on
 * screen - costs far more than it is worth once a table holds a hundred NFTs.
 *
 * Animated sources are either flattened by the CDN or dropped; nothing here
 * ever returns a video or an `animation_url`.
 */
function normalizeImage(url, { size = 96 } = {}) {
  let raw = String(url || "").trim();
  if (!raw) return "";

  if (raw.startsWith("ipfs://")) {
    raw = `https://ipfs.io/ipfs/${raw.slice("ipfs://".length)}`;
  }

  let parsed;
  try {
    parsed = new URL(raw);
  } catch {
    return "";
  }

  if (OPENSEA_CDN.test(parsed.hostname)) {
    // The CDN serves a resized still frame when asked for one, so even a GIF
    // arrives as a single small image.
    parsed.searchParams.set("w", String(size));
    parsed.searchParams.set("h", String(size));
    parsed.searchParams.set("fit", "cover");
    parsed.searchParams.set("auto", "format");
    parsed.searchParams.set("frame", "1");
    return parsed.toString();
  }

  // Not the CDN and self-animating: no way to flatten it, so show nothing
  // rather than pay for playback on every row.
  if (ANIMATED_EXTENSIONS.test(parsed.pathname)) return "";

  return parsed.toString();
}

/** Stable key used by every cache and lock in the app. */
function nftKey(chain, contract, tokenId) {
  return `${normalizeChain(chain)}:${String(contract).toLowerCase()}:${String(tokenId)}`;
}

// ------------------------------------------------------------------
// Order field extraction
// ------------------------------------------------------------------

function unwrapOrder(payload) {
  if (!payload || typeof payload !== "object") return null;
  if (payload.order && typeof payload.order === "object") return payload.order;
  return payload;
}

function getProtocolParameters(order) {
  return (
    order?.protocol_data?.parameters ||
    order?.protocolData?.parameters ||
    order?.parameters ||
    order?.order?.protocol_data?.parameters ||
    order?.order?.protocolData?.parameters ||
    order?.order?.parameters ||
    null
  );
}

function weiToEth(value) {
  if (value === undefined || value === null || value === "") return 0;
  try {
    return Number(ethers.formatEther(BigInt(String(value))));
  } catch {
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
  }
}

/** Price in ETH of an offer order, tolerant of every response shape. */
/**
 * Which ERC20 is an offer denominated in?
 *
 * Robinhood lists collections in WETH (18 decimals) AND in USDG (6 decimals) -
 * measured: 12 of 14 collections use WETH, `stockedfish` and `hedgehogsonrh`
 * use USDG. Their numbers are not comparable: 0.03 USDG is about three cents
 * while 0.0001 WETH is about thirty. Ranking them together picks nonsense
 * winners, so Best Offer only ever compares offers in one currency.
 *
 * @returns {string|null} lowercase token address, or null if undeterminable
 */
function extractOrderCurrency(order) {
  const o = unwrapOrder(order);
  if (!o) return null;

  const parameters = getProtocolParameters(o);
  const offerItems = Array.isArray(parameters?.offer) ? parameters.offer : [];
  for (const item of offerItems) {
    // itemType 1 = ERC20; that is the money side of an offer.
    if (Number(item?.itemType) === 1 && item?.token) {
      return String(item.token).toLowerCase();
    }
  }

  const priceToken =
    o.price?.currency?.address ||
    o.price?.current?.currency ||
    o.payment_token_contract?.address;
  return priceToken ? String(priceToken).toLowerCase() : null;
}

function extractOrderPrice(order) {
  const o = unwrapOrder(order);
  if (!o) return 0;

  // v2 "price" object.
  const priceObj = o.price || o.current_price || o.currentPrice;
  if (priceObj && typeof priceObj === "object") {
    const value =
      priceObj.value ??
      priceObj.amount ??
      priceObj.current?.value ??
      priceObj.current?.amount;
    const decimals = Number(
      priceObj.decimals ?? priceObj.current?.decimals ?? 18
    );
    if (value !== undefined && value !== null && value !== "") {
      try {
        return Number(
          ethers.formatUnits(
            BigInt(String(value)),
            Number.isFinite(decimals) ? decimals : 18
          )
        );
      } catch {
        /* fall through */
      }
    }
  }

  if (typeof priceObj === "string" || typeof priceObj === "number") {
    const direct = Number(priceObj);
    // Raw wei strings are far larger than any plausible ETH amount.
    if (Number.isFinite(direct) && direct > 0 && direct < 1e6) return direct;
    return weiToEth(priceObj);
  }

  for (const field of ["current_price", "base_price", "eth_price"]) {
    if (o[field] !== undefined && o[field] !== null && o[field] !== "") {
      const eth = weiToEth(o[field]);
      if (eth > 0) return eth;
    }
  }

  // Last resort: sum the ERC20 offer items in the Seaport parameters.
  const parameters = getProtocolParameters(o);
  const offerItems = Array.isArray(parameters?.offer) ? parameters.offer : [];
  let total = 0n;
  for (const item of offerItems) {
    if (Number(item.itemType) !== 1) continue; // ERC20 only
    try {
      total += BigInt(String(item.startAmount ?? item.endAmount ?? "0"));
    } catch {
      /* ignore malformed item */
    }
  }
  if (total > 0n) return Number(ethers.formatEther(total));

  return 0;
}

function extractOrderMaker(order) {
  const o = unwrapOrder(order);
  if (!o) return null;

  const candidates = [
    o.maker?.address,
    o.maker,
    o.protocol_data?.parameters?.offerer,
    o.protocolData?.parameters?.offerer,
    o.parameters?.offerer,
    o.offerer
  ];

  for (const candidate of candidates) {
    if (!candidate) continue;
    const value = typeof candidate === "string" ? candidate : candidate.address;
    if (!value) continue;
    try {
      return ethers.getAddress(String(value)).toLowerCase();
    } catch {
      /* try next */
    }
  }
  return null;
}

function extractOrderHash(order) {
  const o = unwrapOrder(order);
  if (!o) return null;

  const candidates = [
    o.order_hash,
    o.orderHash,
    o.hash,
    o.protocol_data?.order_hash,
    o.protocolData?.orderHash,
    o.order?.order_hash,
    o.order?.orderHash
  ];

  for (const candidate of candidates) {
    if (typeof candidate === "string" && /^0x[a-fA-F0-9]{64}$/.test(candidate)) {
      return candidate.toLowerCase();
    }
  }
  return null;
}

function isSignedZoneOrder(order) {
  const parameters = getProtocolParameters(unwrapOrder(order));
  if (!parameters) return false;
  const zone = String(parameters.zone || "").toLowerCase();
  const orderType = Number(parameters.orderType);
  if (zone === OPENSEA_SIGNED_ZONE_V2.toLowerCase()) return true;
  return orderType === 2 || orderType === 3;
}

// ------------------------------------------------------------------
// Offer classification (spec 11)
// ------------------------------------------------------------------

const OFFER_KIND = Object.freeze({
  ITEM: "item",
  COLLECTION: "collection",
  TRAIT: "trait",
  UNKNOWN: "unknown"
});

/** OpenSea replaced criteria.trait with traits/numeric_traits in May 2026. */
function isTraitCriteria(criteria) {
  return Boolean(criteria && (
    criteria.trait ||
    (Array.isArray(criteria.traits) && criteria.traits.length > 0) ||
    (Array.isArray(criteria.numeric_traits) && criteria.numeric_traits.length > 0)
  ));
}

/**
 * Decide what an order actually is. An item endpoint response is NOT proof
 * that the order is an exact token offer - OpenSea also returns the collection
 * and trait offers that apply to the token.
 */
function classifyOffer(order, contract, tokenId) {
  const o = unwrapOrder(order);
  if (!o) return OFFER_KIND.UNKNOWN;

  const normalizedContract = String(contract).toLowerCase();
  const requestedTokenId = String(tokenId);

  // Explicit criteria payloads mean collection or trait.
  const criteria = o.criteria || o.order?.criteria || null;
  if (criteria && typeof criteria === "object") {
    if (isTraitCriteria(criteria)) return OFFER_KIND.TRAIT;
    if (criteria.encoded_token_ids || criteria.collection) {
      return OFFER_KIND.COLLECTION;
    }
  }

  const asset = o.asset || o.order?.asset || null;
  if (asset?.contract && asset?.identifier !== undefined) {
    try {
      const assetContract = ethers.getAddress(String(asset.contract)).toLowerCase();
      if (
        assetContract === normalizedContract &&
        String(asset.identifier) === requestedTokenId
      ) {
        return OFFER_KIND.ITEM;
      }
    } catch {
      /* fall through */
    }
  }

  const parameters = getProtocolParameters(o);
  const consideration = Array.isArray(parameters?.consideration)
    ? parameters.consideration
    : [];

  for (const item of consideration) {
    const itemType = Number(item.itemType);
    const token = String(item.token || "").toLowerCase();
    if (token !== normalizedContract) continue;

    // ERC721 (2) / ERC1155 (3) with a concrete identifier -> exact item offer.
    if (itemType === 2 || itemType === 3) {
      const id = String(item.identifierOrCriteria ?? item.identifier ?? "");
      if (id === requestedTokenId) return OFFER_KIND.ITEM;
      continue;
    }

    // ERC721_WITH_CRITERIA (4) / ERC1155_WITH_CRITERIA (5).
    if (itemType === 4 || itemType === 5) {
      const criteriaValue = String(item.identifierOrCriteria ?? "0");
      // 0 means "any token in the collection"; a merkle root means a trait or
      // token subset, which we can only confirm via the criteria payload.
      return criteriaValue === "0" ? OFFER_KIND.COLLECTION : OFFER_KIND.TRAIT;
    }
  }

  return OFFER_KIND.UNKNOWN;
}

function getOrderExpiration(order) {
  const o = unwrapOrder(order);
  const values = [
    o?.expiration_time,
    o?.expirationTime,
    o?.protocol_data?.parameters?.endTime,
    o?.protocolData?.parameters?.endTime,
    o?.parameters?.endTime
  ];
  for (const value of values) {
    if (value === undefined || value === null || value === "") continue;
    const n = Number(value);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return 0;
}

function getRemainingQuantity(order) {
  const o = unwrapOrder(order);
  const values = [o?.remaining_quantity, o?.remainingQuantity];
  for (const value of values) {
    if (value === undefined || value === null || value === "") continue;
    try {
      return BigInt(String(value));
    } catch {
      /* ignore */
    }
  }
  return null;
}

/** Reject inactive / expired / cancelled / fully-filled orders. */
function isActiveOrder(order) {
  const o = unwrapOrder(order);
  if (!o) return false;

  const status = String(o.status || o.order_status || "").toUpperCase();
  if (status && !["ACTIVE", "VALID", "LIVE", "OPEN"].includes(status)) {
    return false;
  }

  if (o.cancelled || o.finalized || o.marked_invalid) return false;

  const expiration = getOrderExpiration(o);
  if (expiration && expiration <= Math.floor(Date.now() / 1000)) return false;

  const remaining = getRemainingQuantity(o);
  if (remaining !== null && remaining <= 0n) return false;

  return true;
}

// ------------------------------------------------------------------
// NFT metadata
// ------------------------------------------------------------------

/**
 * "erc721" / "erc1155" / "" - whatever OpenSea called it, in one spelling.
 */
function normalizeTokenStandard(raw) {
  const value = String(raw || "").trim().toLowerCase().replace(/[-_s]/g, "");
  if (value === "erc721" || value === "721") return "erc721";
  if (value === "erc1155" || value === "1155") return "erc1155";
  return "";
}

/**
 * Which token standard this contract implements.
 *
 * An offer names the NFT as its first consideration item, and that item's
 * itemType has to match the contract - ERC721 or ERC1155. Getting it wrong is
 * not a soft failure: OpenSea rejects the order with
 *
 *   "the first consideration item itemType does not match the standard of
 *    the contract"
 *
 * and no amount of retrying changes the answer, because the payload is the
 * problem. So this is resolved per contract, from OpenSea's own metadata, and
 * cached for hours - it is a fact about the contract that cannot change.
 *
 * Returns "" when it genuinely cannot be determined; the caller decides what
 * to do rather than being handed a guess.
 */
/**
 * The cached standard for a contract, or "" - no request, no await.
 *
 * Used by callers that need to know whether the answer has CHANGED since they
 * last built a payload, which must not cost a round trip.
 */
function peekTokenStandard(chain, contract) {
  try {
    const c = normalizeChain(chain);
    const normalized = ethers.getAddress(contract).toLowerCase();
    return tokenStandardCache.get(`${c}:${normalized}`) || "";
  } catch {
    return "";
  }
}

/**
 * @returns {Promise<{standard:string, source:string}>} standard is "" when it
 *   genuinely could not be determined. `source` names where the answer came
 *   from, so a wrong answer can be traced to the thing that gave it.
 */
async function fetchTokenStandard(chain, contract, tokenId) {
  const c = normalizeChain(chain);
  const normalizedContract = ethers.getAddress(contract).toLowerCase();
  const cacheKey = `${c}:${normalizedContract}`;

  const cached = tokenStandardCache.get(cacheKey);
  if (cached) return { standard: cached, source: "cache" };

  try {
    const response = await apiGet(
      `/chain/${c}/contract/${normalizedContract}`,
      { kind: KIND.READ, timeout: 12000, label: "contract-standard" }
    );
    const standard = normalizeTokenStandard(
      response.data?.contract_standard || response.data?.token_standard
    );
    if (standard) {
      tokenStandardCache.set(cacheKey, standard);
      return { standard, source: "metadata" };
    }
  } catch {
    /* fall through to the per-token endpoint */
  }

  // The contract endpoint does not always carry it; the NFT endpoint does,
  // and fetchNftMeta caches whatever it finds.
  if (tokenId !== undefined && tokenId !== null && String(tokenId) !== "") {
    try {
      const meta = await fetchNftMeta(c, normalizedContract, tokenId);
      if (meta.tokenStandard) {
        return { standard: meta.tokenStandard, source: "nft-metadata" };
      }
    } catch {
      /* the caller handles an unknown standard */
    }
  }

  const late = tokenStandardCache.get(cacheKey);
  return late ? { standard: late, source: "cache" } : { standard: "", source: "" };
}

// Metadata is Add-NFT / prewarm / name-refresh work: background by default so
// it starts on Key 2 and never takes the last read slot from a P0 market read.
/** Aggregate metadata call count for the [NET] diagnostics line. */
const metaStats = { calls: 0 };

async function fetchNftMeta(chain, contract, tokenId, { force = false, priority = PRIORITY.P2 } = {}) {
  metaStats.calls++;
  const c = normalizeChain(chain);
  const normalizedContract = ethers.getAddress(contract).toLowerCase();
  const key = nftKey(c, normalizedContract, tokenId);

  if (!force) {
    const cached = nftMetaCache.get(key);
    if (cached) return cached;
  }

  const response = await apiGet(
    `/chain/${c}/contract/${normalizedContract}/nfts/${encodeURIComponent(tokenId)}`,
    { kind: KIND.READ, timeout: 12000, label: "nft-meta", priority }
  );

  const nft = response.data?.nft;
  if (!nft) throw new Error(`Khong lay duoc metadata NFT ${tokenId}.`);

  let collectionSlug = "";
  let collectionName = "";

  if (typeof nft.collection === "string") {
    collectionSlug = nft.collection.trim();
    collectionName = collectionSlug;
  } else if (nft.collection && typeof nft.collection === "object") {
    collectionSlug = String(
      nft.collection.slug || nft.collection.collection_slug || ""
    ).trim();
    collectionName = String(nft.collection.name || collectionSlug || "").trim();
  }

  collectionSlug =
    collectionSlug ||
    String(nft.collection_slug || nft.collectionSlug || "").trim();
  collectionName =
    collectionName || String(nft.collection_name || collectionSlug || "").trim();

  const standard = normalizeTokenStandard(nft.token_standard || nft.tokenStandard);
  if (standard) tokenStandardCache.set(`${c}:${normalizedContract}`, standard);

  const meta = {
    name: nft.name || `NFT #${tokenId}`,
    tokenStandard: standard,
    // display_image_url is OpenSea's flattened preview; image_url can be the
    // original animation. animation_url is never used.
    image: normalizeImage(nft.display_image_url || nft.image_url || ""),
    // Ứng viên ảnh dự phòng: ảnh gốc khi bản xem trước không tải/không vẽ được
    // (renderer tự lần lượt thử — xem setNftImage).
    imageAlts: [normalizeImage(nft.image_url || "")]
      .filter(u => u && u !== normalizeImage(nft.display_image_url || nft.image_url || "")),
    collectionSlug,
    collectionName:
      collectionName ||
      `${normalizedContract.slice(0, 6)}...${normalizedContract.slice(-4)}`,
    // Trait của token (OpenSea: [{trait_type, value}]) — nguồn bản chụp trait
    // của Offer Item từ khi không còn Alchemy.
    traits: Array.isArray(nft.traits) ? nft.traits : []
  };

  nftMetaCache.set(key, meta);
  if (collectionSlug) {
    collectionSlugCache.set(`${c}:${normalizedContract}`, collectionSlug);
  }
  return meta;
}

/**
 * Collection slug straight from the CONTRACT - no tokenId needed.
 *
 * Bulk offers only need the slug to look up the collection's required fees, and
 * that is a per-collection fact. Asking the per-token NFT endpoint meant one
 * request per NFT for what is really one request per contract. Measured: 279ms,
 * and the answer is cached for 30 minutes.
 */
async function fetchContractCollection(chain, contract) {
  const c = normalizeChain(chain);
  const normalized = ethers.getAddress(contract).toLowerCase();
  const cacheKey = `${c}:${normalized}`;

  const cached = collectionSlugCache.get(cacheKey);
  if (cached) return cached;

  const response = await apiGet(`/chain/${c}/contract/${normalized}`, {
    kind: KIND.READ,
    priority: PRIORITY.P2,
    timeout: 10000,
    retries: 1,
    label: "contract-collection"
  });

  const slug = String(response.data?.collection || "").trim();
  if (slug) collectionSlugCache.set(cacheKey, slug);
  return slug;
}

/**
 * Every active offer on ONE NFT, with full Seaport parameters attached.
 *
 * Verified live: this endpoint returns `protocol_data.parameters` for every
 * offer it lists (50/50 on pudgypenguins #1). That matters enormously for the
 * Cancel tab - it means discovery and the data needed to cancel ON-CHAIN come
 * from the SAME single request, instead of a per-order lookup afterwards.
 *
 * @returns {Promise<Array<object>>} raw offer objects
 */
async function fetchNftOffers(chain, slug, tokenId, {
  limit = 50, maxPages = 1, signal = null,
  kind = KIND.READ, priority = PRIORITY.P2
} = {}) {
  const offers = [];
  const seenOrders = new Set();
  const seenCursors = new Set();
  let next = null;
  let status = "PAGE_LIMIT";

  for (let page = 0; page < Math.max(1, Number(maxPages) || 1); page++) {
    const params = { limit };
    if (next) params.next = next;
    const response = await apiGet(
      `/offers/collection/${encodeURIComponent(slug)}/nfts/${encodeURIComponent(tokenId)}`,
      {
        kind,
        priority,
        timeout: 15000,
        retries: 2,
        signal,
        params,
        label: "nft-offers-own"
      }
    );
    const data = response.data || {};
    const pageOffers = Array.isArray(data.offers) ? data.offers
      : Array.isArray(data.orders) ? data.orders : [];
    for (const offer of pageOffers) {
      const hash = extractOrderHash(offer);
      if (hash && seenOrders.has(hash)) continue;
      if (hash) seenOrders.add(hash);
      offers.push(offer);
    }
    const previous = next;
    next = data.next || null;
    if (!next) { status = "END_OF_DATA"; break; }
    if (next === previous || seenCursors.has(next)) { status = "CURSOR_LOOP"; break; }
    seenCursors.add(next);
  }
  offers.complete = status === "END_OF_DATA";
  offers.partial = !offers.complete;
  offers.status = status;
  return offers;
}

/**
 * MỌI offer đang mở trên một collection — item, collection và trait — theo
 * trang 100. `/offers/collection/{slug}/all` trả `protocol_data` cho từng
 * order (đo live: consideration itemType 2 kèm identifier cho item offer), nên
 * MỘT lượt drain trả lời câu hỏi "ví này còn item offer nào trên collection
 * này" cho cả trăm NFT cùng collection, thay cho trăm lượt đọc từng NFT.
 *
 * Không sắp theo gì cả và collection lớn có hàng nghìn offer, nên lượt drain
 * có trần trang và tự nói nó KHÔNG đầy đủ (`complete === false`) — người gọi
 * chỉ được coi một collection là "đã soát" khi END_OF_DATA.
 *
 * @returns {Promise<Array<object> & {complete:boolean, status:string, pages:number}>}
 */
async function fetchCollectionAllOffers(slug, {
  limit = 100, maxPages = 4, signal = null, priority = PRIORITY.P2, kind = KIND.READ
} = {}) {
  const offers = [];
  const seenOrders = new Set();
  const seenCursors = new Set();
  let next = null;
  let status = "PAGE_LIMIT";
  let pages = 0;

  for (let page = 0; page < Math.max(1, Number(maxPages) || 1); page++) {
    const params = { limit };
    if (next) params.next = next;
    const response = await apiGet(
      `/offers/collection/${encodeURIComponent(slug)}/all`,
      { kind, priority, timeout: 15000, retries: 2, signal, params, label: "collection-all-offers" }
    );
    pages++;
    const data = response.data || {};
    const pageOffers = Array.isArray(data.offers) ? data.offers
      : Array.isArray(data.orders) ? data.orders : [];
    for (const offer of pageOffers) {
      const hash = extractOrderHash(offer);
      if (hash && seenOrders.has(hash)) continue;
      if (hash) seenOrders.add(hash);
      offers.push(offer);
    }
    const previous = next;
    next = data.next || null;
    if (!next) { status = "END_OF_DATA"; break; }
    if (next === previous || seenCursors.has(next)) { status = "CURSOR_LOOP"; break; }
    seenCursors.add(next);
  }
  offers.complete = status === "END_OF_DATA";
  offers.partial = !offers.complete;
  offers.status = status;
  offers.pages = pages;
  return offers;
}

/**
 * Nếu `order` là một ITEM offer đang hoạt động do `wallet` đặt, trả về bản
 * rút gọn `{contract, tokenId, orderHash, price, endTime}`; không thì null.
 * Dùng chung cho mọi nguồn có `protocol_data` (NFT endpoint, collection /all).
 */
function ownItemFromOrder(order, wallet) {
  if (!order || !isActiveOrder(order)) return null;
  const me = String(wallet || "").toLowerCase();
  if (!me || String(extractOrderMaker(order) || "").toLowerCase() !== me) return null;
  const orderHash = extractOrderHash(order);
  if (!orderHash) return null;
  const params = getProtocolParameters(order);
  const consideration = Array.isArray(params && params.consideration) ? params.consideration : [];
  let contract = "", tokenId = "";
  for (const item of consideration) {
    const itemType = Number(item.itemType);
    if (itemType !== 2 && itemType !== 3) continue;
    contract = String(item.token || "").toLowerCase();
    tokenId = String(item.identifierOrCriteria ?? item.identifier ?? "");
    break;
  }
  if (!contract || !tokenId) {
    const asset = order.asset || null;
    if (asset && asset.contract && asset.identifier !== undefined && asset.identifier !== null) {
      contract = String(asset.contract).toLowerCase();
      tokenId = String(asset.identifier);
    }
  }
  if (!contract || !tokenId) return null;
  return {
    contract, tokenId, orderHash,
    price: Number(extractOrderPrice(order)) || 0,
    endTime: Number(params && params.endTime) || 0
  };
}

/** Slug lookup that avoids a metadata call when another NFT already resolved it. */
async function resolveCollectionSlug(chain, contract, tokenId, { priority = PRIORITY.P2 } = {}) {
  const c = normalizeChain(chain);
  const normalizedContract = ethers.getAddress(contract).toLowerCase();

  const cached = collectionSlugCache.get(`${c}:${normalizedContract}`);
  if (cached) return cached;

  const meta = await fetchNftMeta(c, normalizedContract, tokenId, { priority });
  return meta.collectionSlug || "";
}

function feeToBps(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return 0;
  // OpenSea returns percentages (2.5), not basis points.
  return Math.round(n * 100);
}

/**
 * The contract address behind a collection slug.
 *
 * The one OpenSea call Alchemy cannot replace: Alchemy is addressed by
 * contract, and a person types a slug. It is ONE request per collection, and
 * it buys a whole trait distribution from Alchemy that would otherwise have
 * been read from OpenSea - so it pays for itself immediately.
 */
async function fetchCollectionContract(collectionSlug) {
  const slug = String(collectionSlug || "").trim();
  if (!slug) return "";

  const cached = collectionSlugCache.get(`contract:${slug}`);
  if (cached !== undefined) return cached;

  const response = await apiGet(`/collections/${encodeURIComponent(slug)}`, {
    kind: KIND.READ,
    timeout: 12000,
    label: "collection-contract"
  });

  const contracts = Array.isArray(response.data?.contracts)
    ? response.data.contracts : [];

  let address = "";
  for (const entry of contracts) {
    if (!entry || !entry.address) continue;
    try {
      address = ethers.getAddress(entry.address).toLowerCase();
      break;
    } catch {
      /* try the next one */
    }
  }

  if (address) collectionSlugCache.set(`contract:${slug}`, address);
  return address;
}

/**
 * How many items OpenSea says the collection has.
 *
 * The expected count that `total_supply` gives is the only external number a
 * finished scan can be checked against: without it "we walked until the
 * cursor ran out" is the only completion signal there is, and a feed that
 * quietly stops early is indistinguishable from a feed that finished.
 *
 * Cached beside the contract, from the same endpoint the contract lookup
 * already calls, so this adds no request on the common path.
 *
 * @returns {Promise<number>} 0 when OpenSea does not say
 */
async function fetchCollectionSupply(collectionSlug) {
  const slug = String(collectionSlug || "").trim();
  if (!slug) return 0;

  const cached = collectionSlugCache.get(`supply:${slug}`);
  if (cached !== undefined) return cached;

  let total = 0;
  try {
    const response = await apiGet(`/collections/${encodeURIComponent(slug)}`, {
      kind: KIND.READ,
      timeout: 12000,
      label: "collection-supply"
    });
    total = Number(response.data && response.data.total_supply) || 0;
  } catch {
    // Not knowing the expected count is a smaller problem than failing the
    // scan over it: the walk still has pagination exhaustion to rely on.
    total = 0;
  }

  if (total > 0) collectionSlugCache.set(`supply:${slug}`, total);
  return total;
}

async function fetchCollectionFees(collectionSlug, { priority = PRIORITY.P2 } = {}) {
  const slug = String(collectionSlug || "").trim();
  if (!slug) throw new Error("Thieu collection slug de lay fees.");

  const cached = collectionFeeCache.get(slug);
  if (cached) return cached;

  // Once per collection, cached 30 minutes, needed to BUILD a template — not
  // on the per-send path (a missing template defers the send; it never holds
  // a write slot). Background lane, Key 2 first.
  // 1.25.31: a template that gates a pending SEND asks at P0 (live: rows sat
  // on BLOCKED: template behind the background lane); default stays P2.
  const response = await apiGet(`/collections/${encodeURIComponent(slug)}`, {
    kind: KIND.READ,
    priority,
    timeout: 12000,
    label: "collection-fees"
  });

  const fees = Array.isArray(response.data?.fees) ? response.data.fees : [];
  const feeMap = new Map();

  for (const fee of fees) {
    if (!fee?.recipient) continue;

    let recipient;
    try {
      recipient = ethers.getAddress(fee.recipient).toLowerCase();
    } catch {
      continue;
    }

    const basisPoints = feeToBps(fee.fee);
    if (basisPoints <= 0) continue;

    const required = fee.required !== false;
    const existing = feeMap.get(recipient);
    if (!existing || basisPoints > existing.basisPoints) {
      feeMap.set(recipient, { recipient, basisPoints, required });
    }
  }

  const marketplaceKey = OPENSEA_FEE_RECIPIENT.toLowerCase();
  if (!feeMap.has(marketplaceKey)) {
    feeMap.set(marketplaceKey, {
      recipient: marketplaceKey,
      basisPoints: OPENSEA_FEE_BPS,
      required: true
    });
  }

  const allFees = Array.from(feeMap.values()).filter(
    x => x.required && x.basisPoints > 0
  );

  const totalBps = allFees.reduce((sum, x) => sum + x.basisPoints, 0);
  if (totalBps >= 10000) {
    throw new Error(`Creator fees tong ${totalBps} bps khong hop le.`);
  }

  const result = {
    fees: allFees,
    // A creator fee beyond the marketplace fee forces OpenSea's signed zone.
    requiresSignedZone: allFees.some(x => x.recipient !== marketplaceKey)
  };

  collectionFeeCache.set(slug, result);
  return result;
}

// ------------------------------------------------------------------
// BEST OFFER (spec 11)
// ------------------------------------------------------------------

/**
 * Resolve the highest offer that actually applies to ONE specific NFT.
 *
 * Passes, in order:
 *   1. /offers/collection/{slug}/nfts/{id}/best - OpenSea's own resolution,
 *      which already accounts for collection and trait offers.
 *   2. /offers/collection/{slug}/nfts/{id} - scoped to the NFT; every returned
 *      order is classified (item / collection / trait) and inactive ones dropped.
 *   3. /orders/{chain}/seaport/offers filtered down to exact item orders.
 *
 * Never returns offers[0] blindly and never assumes an item endpoint response
 * is an exact token offer.
 *
 * @returns {Promise<{ok:boolean, price:number, maker:string|null,
 *   orderHash:string|null, collectionSlug:string, kind:string, source:string,
 *   reason?:string}>}
 */
/**
 * Collection-wide offers, cached per collection.
 *
 * Every NFT in the same collection shares this answer, so watching 20 Pudgy
 * Penguins costs ONE request for the collection side instead of 20.
 */
/**
 * How many pages of offers to drain. The offers endpoints return at most 50 per
 * page and, critically, do NOT sort by price: measured on cashcatss #3906 the
 * first page arrived as 0.086, 0.255, 0.083, ... across 87 offers on 2 pages.
 * Reading only the first page therefore misses the top bid whenever it happens
 * to land on a later page - which is exactly the "Best Offer sai" symptom.
 *
 * Collection enrichment keeps a small bound. Exact NFT authority uses a
 * separate larger bound and follows the cursor to completion.
 */
const OFFER_PAGE_LIMIT = 50;
const OFFER_MAX_PAGES = 4;
const NFT_OFFER_MAX_PAGES = 100;

/**
 * Drain an offers endpoint page by page, deduping by order hash.
 * @returns {Promise<{offers: Array, complete: boolean, pages: number}>}
 */
async function fetchOfferPages(path, {
  params = {},
  label,
  timeout = 6000,
  maxPages = OFFER_MAX_PAGES,
  priority = PRIORITY.P0, signal = null
}) {
  const offers = [];
  const seen = new Set();
  let next = null;
  let pages = 0;
  let complete = false;
  const seenCursors = new Set();

  for (; pages < maxPages; pages++) {
    const query = { ...params, limit: OFFER_PAGE_LIMIT };
    if (next) query.next = next;

    const response = await apiGet(path, {
      kind: KIND.READ,
      priority,
      timeout,
      retries: 0,
      params: query,
      label, signal
    });

    const data = response.data || {};
    const batch = Array.isArray(data.offers)
      ? data.offers
      : Array.isArray(data.orders)
        ? data.orders
        : [];

    for (const offer of batch) {
      const hash = extractOrderHash(offer);
      const dedupe = hash || JSON.stringify(offer?.protocol_data?.parameters?.salt ?? offer);
      if (seen.has(dedupe)) continue;
      seen.add(dedupe);
      offers.push(offer);
    }

    const cursor = data.next;
    if (cursor == null || cursor === "") {
      complete = true;
      pages++;
      break;
    }
    if (typeof cursor !== "string" || cursor.length > 2048 || !batch.length ||
        seenCursors.has(cursor)) {
      pages++;
      break;
    }
    seenCursors.add(cursor);
    next = cursor;
  }

  return { offers, complete, pages };
}

async function fetchCollectionOffers(slug, { useCache = true, priority = PRIORITY.P2, signal = null } = {}) {
  const key = String(slug || "").trim();
  if (!key) return [];

  if (useCache) {
    const cached = collectionOffersCache.get(key);
    if (cached) return cached;
  }

  try {
    /**
     * COLLECTION OFFER LÀ LÀM GIÀU, KHÔNG PHẢI LÀ ĐIỀU KIỆN ĐỂ BẮT ĐẦU
     *
     *   Lượt này đọc tới bốn trang cho MỖI collection, và lúc Start có hai
     *   mươi tám collection. Ở hạng mặc định (P0) chúng chen trước lượt đọc
     *   Best của từng NFT — thứ mà cổng lượt-đọc-đầu đang chờ để mở — nên
     *   bảng đứng im trong khi hạn mức bị tiêu vào phần làm giàu. Đặt xuống
     *   hạng nền: vẫn đọc, nhưng đọc sau thứ chặn đường.
     */
    const { offers } = await fetchOfferPages(`/offers/collection/${encodeURIComponent(key)}`, {
      label: "collection-offers", priority, signal
    });
    collectionOffersCache.set(key, offers);
    return offers;
  } catch (error) {
    /**
     * "ĐỌC HỎNG" KHÔNG ĐƯỢC TRỞ THÀNH "KHÔNG CÓ COLLECTION OFFER NÀO"
     *
     *   Nuốt lỗi và trả `[]` ở đây từng làm một cú 429 trông y hệt một
     *   collection không có offer criteria nào. Người gọi ghi mốc TTL, im
     *   lặng năm phút, và trong năm phút đó bot đặt giá DƯỚI một Collection
     *   Offer có thật — cùng một họ lỗi với §24 "chưa biết không phải là
     *   không có".
     *
     *   Nên đánh dấu kết quả là KHÔNG ĐÁNG TIN thay vì ném (ném sẽ làm một
     *   cú 429 lan thành lỗi Start): mảng vẫn là mảng, mọi vòng lặp hiện có
     *   vẫn chạy, và ai cần biết thì đọc `failed`.
     */
    const out = [];
    out.failed = true;
    out.reason = String((error && error.message) || error).slice(0, 120);
    return out;
  }
}

/**
 * Does a criteria offer cover this specific token?
 *
 * `encoded_token_ids` is "*" for a whole-collection offer, or an explicit list
 * such as "1,71,121,150" for a token-subset offer. Verified live on
 * pudgypenguins: both forms come back from the API.
 */
function criteriaCoversToken(offer, tokenId) {
  const encoded = offer?.criteria?.encoded_token_ids;
  if (encoded === undefined || encoded === null) return false;

  const text = String(encoded).trim();
  if (text === "*") return true;

  const wanted = String(tokenId);
  for (const part of text.split(",")) {
    const chunk = part.trim();
    if (!chunk) continue;

    if (chunk.includes(":")) {
      // Range form "100:200".
      const [lo, hi] = chunk.split(":").map(v => Number(v.trim()));
      const n = Number(wanted);
      if (Number.isFinite(lo) && Number.isFinite(hi) && n >= lo && n <= hi) return true;
      continue;
    }
    if (chunk === wanted) return true;
  }
  return false;
}

/**
 * The highest ITEM offer on this exact token.
 *
 * Collection and trait offers are deliberately not counted. They are a standing
 * bid on any token in the set, not a bid on this one, and treating them as
 * competition made every row permanently OUTBID: measured on rekt-tradooor
 * #7417 the best item offer was 0.065 while one collection-wide bid sat at
 * 0.333, far above any sane Max. Ignoring them also removes a whole request
 * per scan.
 */
/**
 * BEST OFFER = the highest ACTIVE offer that applies to ONE exact NFT.
 *
 * An NFT is (chain, contract, tokenId). Everything below was checked against
 * live responses rather than assumed - see diag-api.js:
 *
 *   endpoint   GET /api/v2/offers/collection/{slug}/nfts/{tokenId}
 *              Purpose-built for one token, and it does the criteria matching
 *              itself: asking for two different tokens of the same collection
 *              returned overlapping-but-different sets (20 / 20, 10 shared).
 *              So a collection or trait offer that comes back here genuinely
 *              covers this token, and we must NOT re-derive that ourselves.
 *
 *   amount     price.value      raw integer string
 *   currency   price.currency   e.g. "WETH"
 *   decimals   price.decimals   e.g. 18  (Robinhood USDG is 6)
 *              price = value / 10**decimals. The raw integer is never a price.
 *
 *   maker      protocol_data.parameters.offerer
 *   expiry     protocol_data.parameters.endTime   unix seconds
 *   status     "ACTIVE" | "CANCELLED"             authoritative, so it is used
 *                                                 instead of guessing
 *   order id   order_hash
 *   paging     `next` cursor; measured 98 offers over 2 pages on pudgypenguins #1
 *
 * Our own wallet's offer counts. If we hold the top bid, Best Offer IS our bid,
 * and the caller's own `My Offer >= Best Offer` test is what makes that ON TOP.
 */
/**
 * How many NFTs one order is buying.
 *
 * `remaining_quantity` is the field to use; the NFT item on the consideration
 * side carries the same number, and the two agreed on every order measured.
 * When they disagree the larger wins, because dividing by too small a number is
 * what produces an inflated price.
 */
function resolveOfferQuantity(offer, parameters) {
  const fromField = Number(offer?.remaining_quantity);

  const nftItem = (parameters?.consideration || []).find(
    item => Number(item?.itemType) >= 2
  );
  const fromConsideration = Number(nftItem?.startAmount);

  const candidates = [fromField, fromConsideration].filter(
    value => Number.isFinite(value) && value >= 1
  );

  if (!candidates.length) return 1;
  return Math.max(...candidates);
}

/**
 * Confirm the top bid on one NFT, in a single request.
 *
 * GET /offers/collection/{slug}/nfts/{id}/best - OpenSea's own resolution.
 * Measured against the paginated read on 2026-09-03: agreed on every token
 * where both returned an order, and on five tokens where ranking by TOTAL
 * and ranking by PER ITEM select different orders it returned the per-item
 * winner every time. That is the rule this app needs; a 6.44 offer buying
 * four items is 1.61 each and must not outrank a 4.29 item offer.
 *
 * Returns ONE order. That makes it a confirmation, not a survey: it cannot
 * seed a book, because a book of one order goes empty on a single cancel
 * and an empty-but-fresh book reads as "no offers".
 *
 * TELLING "NOTHING" FROM "DON'T KNOW"
 *
 *   `empty: true` means OpenSea answered and there is no offer on this token.
 *   That is the only result that licenses Best = 0. Every other failure -
 *   timeout, 5xx, an unparseable body - returns `ok: false` WITHOUT `empty`,
 *   and the caller must treat it as unknown and fall back. Reading 0 out of a
 *   failure is how a bot bids Min Price into a live market.
 *
 * @returns {Promise<{ok:boolean, empty?:boolean, price:number,
 *   order:object|null, reason?:string}>} `order` is shaped for
 *   OrderBook.upsert / OrderBook.confirm.
 */
async function fetchBestOfferQuick(chain, contract, tokenId, collectionSlug,
  { priority = PRIORITY.P0, signal = null } = {}) {
  const c = normalizeChain(chain);
  const slug = String(collectionSlug || "").trim();
  if (!slug) return { ok: false, reason: "NO_COLLECTION_SLUG", price: 0, order: null };

  let response;
  try {
    response = await apiGet(
      `/offers/collection/${encodeURIComponent(slug)}` +
      `/nfts/${encodeURIComponent(String(tokenId))}/best`,
      {
        kind: KIND.READ,
        priority,
        timeout: 6000,
        retries: 0,
        label: "nft-best", signal
      }
    );
  } catch (error) {
    // Every failure is the caller's cue to fall back to the full read.
    return { ok: false, reason: describeError(error), price: 0, order: null };
  }

  // An answer, or a failure that merely resolved?
  //
  // Only a 2xx is OpenSea speaking. A 500 with an empty body is not "this
  // token has no offers" - and reading it that way is how a failed request
  // becomes Best = 0 and the bot bids Min Price into a live market. Callers
  // that see no `empty` flag fall back to the full read, which is correct.
  const status = Number(response.status);
  if (Number.isFinite(status) && (status < 200 || status >= 300)) {
    return { ok: false, reason: `HTTP ${status}`, price: 0, order: null };
  }

  const data = response.data || {};
  const price = data.price || {};
  const decimals = Number(price.decimals);

  // A 200 with no price is OpenSea saying this token has no offer. That is a
  // FACT, not a failure, and it is the only thing that licenses Best = 0.
  if (price.value === undefined || price.value === null) {
    return {
      ok: false, empty: true, reason: "NO_OFFER", price: 0, order: null,
      collectionSlug: slug, source: "best-endpoint"
    };
  }

  // A price we cannot interpret is NOT an empty token.
  if (!Number.isFinite(decimals)) {
    return { ok: false, reason: "NO_DECIMALS", price: 0, order: null };
  }

  // OpenSea states the status; trust it, exactly as the paginated read does.
  // An order that exists but is not active means nothing is biddable right
  // now, which is the same fact as no offer.
  if (data.status && String(data.status).toUpperCase() !== "ACTIVE") {
    return {
      ok: false, empty: true, reason: "NOT_ACTIVE", price: 0, order: null,
      collectionSlug: slug, source: "best-endpoint"
    };
  }

  let totalOrderValue;
  try {
    totalOrderValue = Number(
      ethers.formatUnits(BigInt(String(price.value)), decimals));
  } catch {
    return { ok: false, reason: "BAD_PRICE", price: 0, order: null };
  }
  if (!(totalOrderValue > 0)) {
    return { ok: false, reason: "ZERO_PRICE", price: 0, order: null };
  }

  const parameters = data.protocol_data && data.protocol_data.parameters;

  const now = Math.floor(Date.now() / 1000);
  const endTime = Number(parameters && parameters.endTime);
  if (Number.isFinite(endTime) && endTime > 0 && endTime <= now) {
    // The top offer has run out, so nothing is biddable. A fact, like NOT_ACTIVE.
    return {
      ok: false, empty: true, reason: "EXPIRED", price: 0, order: null,
      collectionSlug: slug, source: "best-endpoint"
    };
  }

  // The same per-item rule as the paginated read. Ranking on the total picks
  // the wrong order whenever a multi-item offer is in play.
  const quantity = resolveOfferQuantity(data, parameters);
  const pricePerItem = totalOrderValue / quantity;

  const normalizedContract = String(contract).toLowerCase();
  return {
    ok: true,
    price: pricePerItem,
    bestOffer: pricePerItem,
    pricePerItem,
    totalOrderValue,
    quantity,
    collectionSlug: slug,
    kind: classifyOffer(data, normalizedContract, String(tokenId)),
    orderHash: data.order_hash || null,
    source: "best-endpoint",
    // Shaped for OrderBook.upsert, so one request both answers and updates.
    order: {
      orderHash: data.order_hash || null,
      maker: parameters && parameters.offerer
        ? String(parameters.offerer).toLowerCase() : null,
      totalOrderValue,
      quantity,
      pricePerItem,
      currency: String(price.currency || ""),
      decimals,
      endTime: Number(parameters && parameters.endTime) || 0,
      kind: classifyOffer(data, normalizedContract, String(tokenId)),
      source: "best-endpoint"
    }
  };
}

async function fetchBestOffer(chain, contract, tokenId, collectionSlug = "", opts = {}) {
  // `collectOrders` receives every eligible order this read saw, so one HTTP
  // call can both answer now and seed the caller's local book for later.
  //
  // `priority` says whether a row is blocked on this answer (P0) or whether it
  // is upkeep nobody is waiting for (P2). Background reads yield the limiter's
  // reserve and the busier API keys, so they cannot delay an urgent one.
  const {
    useCache = true,
    collectOrders = null,
    priority = PRIORITY.P0, signal = null,
    // 1.25.31: head read for DEGRADED polling. The list endpoint is sorted by
    // price and current, while /best lagged ~30 s live; one page is enough to
    // see a new top. Never cached, never used to prune (partial).
    firstPageOnly = false
  } = opts;

  const c = normalizeChain(chain);
  const normalizedContract = ethers.getAddress(contract).toLowerCase();
  const requestedTokenId = String(tokenId);
  const key = nftKey(c, normalizedContract, requestedTokenId);

  if (useCache) {
    const cached = bestOfferCache.get(key);
    if (cached) return { ...cached, cached: true };
  }

  const fail = (reason, source, slug = "") => ({
    ok: false,
    reason,
    source,
    price: 0,
    bestOffer: 0,
    currency: "",
    decimals: 0,
    maker: null,
    orderId: null,
    orderHash: null,
    kind: OFFER_KIND.UNKNOWN,
    collectionSlug: slug
  });

  // The endpoint is addressed by slug, so resolve it once per contract.
  let slug = String(collectionSlug || "").trim();
  if (!slug) {
    try {
      // On the Best path this read is blocking a row: keep the caller lane.
      slug = await resolveCollectionSlug(c, normalizedContract, requestedTokenId, { priority });
    } catch (error) {
      return fail(describeError(error), "meta-failed");
    }
  }
  if (!slug) return fail("NO_COLLECTION_SLUG", "no-slug");

  // ---- read every page ---------------------------------------------
  let pages;
  try {
    pages = await fetchOfferPages(
      `/offers/collection/${encodeURIComponent(slug)}/nfts/${encodeURIComponent(
        requestedTokenId
      )}`,
      { label: firstPageOnly ? "nft-offers-head" : "nft-offers", priority, signal,
        maxPages: firstPageOnly ? 1 : NFT_OFFER_MAX_PAGES }
    );
  } catch (error) {
    return fail(describeError(error), "read-failed", slug);
  }

  const now = Math.floor(Date.now() / 1000);

  const tally = { item: 0, collection: 0, trait: 0, unknown: 0 };
  let ignoredStatus = 0;
  let ignoredExpired = 0;
  let ignoredNoPrice = 0;

  let winner = null;
  const eligible = collectOrders ? [] : null;

  for (const offer of pages.offers) {
    // OpenSea states the status; trust it rather than re-deriving one.
    if (offer.status && String(offer.status).toUpperCase() !== "ACTIVE") {
      ignoredStatus++;
      continue;
    }

    const parameters = getProtocolParameters(offer);

    const endTime = Number(parameters?.endTime);
    if (Number.isFinite(endTime) && endTime > 0 && endTime <= now) {
      ignoredExpired++;
      continue;
    }

    // Price from the currency's own decimals. Never the raw integer.
    const raw = offer?.price?.value;
    const decimals = Number(offer?.price?.decimals);
    if (raw === undefined || raw === null || !Number.isFinite(decimals)) {
      ignoredNoPrice++;
      continue;
    }

    let totalOrderValue;
    try {
      totalOrderValue = Number(ethers.formatUnits(BigInt(String(raw)), decimals));
    } catch {
      ignoredNoPrice++;
      continue;
    }
    if (!(totalOrderValue > 0)) {
      ignoredNoPrice++;
      continue;
    }

    // `price.value` is the value of the WHOLE order, not the price of one NFT.
    //
    // A collection offer buying 10 items for 0.333 reports price.value = 0.333,
    // which is 0.0333 per item - lower than a plain item offer of 0.0625 that
    // reports 0.0625. Ranking on the total therefore picks the wrong order, and
    // by a wide margin: measured on rekt-tradooor #7417 it returned 0.333 while
    // the page showed $152.71, and on pudgypenguins #1 it returned 6.44 while
    // the page showed $11.6K (the real winner being a 4.73 item offer).
    const quantity = resolveOfferQuantity(offer, parameters);
    const pricePerItem = totalOrderValue / quantity;

    const kind = classifyOffer(offer, normalizedContract, requestedTokenId);
    tally[kind] = (tally[kind] || 0) + 1;

    if (eligible) {
      eligible.push({
        orderHash: offer.order_hash || null,
        maker: parameters?.offerer ? String(parameters.offerer).toLowerCase() : null,
        totalOrderValue,
        quantity,
        pricePerItem,
        currency: String(offer?.price?.currency || ""),
        decimals,
        endTime: Number(parameters?.endTime) || 0,
        kind,
        source: "rest"
      });
    }

    // Fees are NOT deducted: the page shows the gross offer price.
    if (!winner || pricePerItem > winner.pricePerItem) {
      winner = {
        pricePerItem,
        totalOrderValue,
        quantity,
        currency: String(offer?.price?.currency || ""),
        decimals,
        maker: parameters?.offerer ? String(parameters.offerer).toLowerCase() : null,
        orderHash: offer.order_hash || null,
        endTime: Number(parameters?.endTime) || 0,
        kind
      };
    }
  }

  // Hand the caller the full eligible set, but only when the read was whole:
  // seeding a book from a truncated page would bake the gap into memory.
  if (collectOrders && pages.complete) {
    try {
      collectOrders(eligible);
    } catch (error) {
      logger.best(`collectOrders lỗi: ${error.message}`);
    }
  }

  const ignored = ignoredStatus + ignoredExpired + ignoredNoPrice;

  // A truncated read may not have seen the top bid; say so, so the caller can
  // refuse to lower a price it already trusts.
  const failedSources = pages.complete ? [] : ["nft-offers-truncated"];

  const result = {
    ok: true,
    reason: winner ? "" : "NO_ACTIVE_OFFER",
    source: "nft-offers",

    // The answer is the price of ONE nft. `bestOffer` and `price` are the same
    // number under two names so existing callers keep working.
    bestOffer: winner ? winner.pricePerItem : 0,
    price: winner ? winner.pricePerItem : 0,
    pricePerItem: winner ? winner.pricePerItem : 0,

    // Kept so it is always possible to see WHY this order won.
    totalOrderValue: winner ? winner.totalOrderValue : 0,
    quantity: winner ? winner.quantity : 0,

    currency: winner ? winner.currency : "",
    decimals: winner ? winner.decimals : 0,
    maker: winner ? winner.maker : null,
    orderId: winner ? winner.orderHash : null,
    orderHash: winner ? winner.orderHash : null,
    // Expiry of the winning order, so a local book can retire it on its own
    // clock instead of asking again.
    endTime: winner ? winner.endTime : 0,
    kind: winner ? winner.kind : OFFER_KIND.UNKNOWN,

    collectionSlug: slug,
    partial: failedSources.length > 0,
    failedSources,
    counts: {
      ...tally,
      considered: pages.offers.length,
      applicable: tally.item + tally.collection + tally.trait + tally.unknown,
      ignored,
      ignoredStatus,
      ignoredExpired,
      ignoredNoPrice,
      pages: pages.pages
    },
    breakdown: { item: 0, collection: 0, trait: 0 }
  };

  // Highest of each kind, so a log line can explain which type holds the top.
  // Per item here as well - a breakdown in order totals would not line up with
  // the winner and would send anyone reading it down the wrong path.
  for (const offer of pages.offers) {
    if (offer.status && String(offer.status).toUpperCase() !== "ACTIVE") continue;
    const raw = offer?.price?.value;
    const decimals = Number(offer?.price?.decimals);
    if (raw === undefined || !Number.isFinite(decimals)) continue;

    const parameters = getProtocolParameters(offer);
    let pricePerItem;
    try {
      pricePerItem =
        Number(ethers.formatUnits(BigInt(String(raw)), decimals)) /
        resolveOfferQuantity(offer, parameters);
    } catch {
      continue;
    }

    const kind = classifyOffer(offer, normalizedContract, requestedTokenId);
    if (result.breakdown[kind] !== undefined && pricePerItem > result.breakdown[kind]) {
      result.breakdown[kind] = pricePerItem;
    }
  }

  logger.best(
    `#${requestedTokenId} · ${pages.offers.length} offer / ${pages.pages} trang · ` +
      `item:${tally.item} collection:${tally.collection} trait:${tally.trait} · ` +
      `bỏ ${ignored} · Best ${result.bestOffer}${result.currency ? " " + result.currency : ""}` +
      (winner && winner.quantity > 1
        ? ` (= ${winner.totalOrderValue} / ${winner.quantity} NFT)`
        : "")
  );

  if (!firstPageOnly) bestOfferCache.set(key, result);
  return result;
}

// ------------------------------------------------------------------
// MY OFFERS
// ------------------------------------------------------------------

/**
 * All active offers made by the wallet, keyed by `${chain}:${contract}:${tokenId}`.
 * Only exact item offers are recorded - a collection offer the user made is not
 * "my offer on this NFT" for outbid purposes.
 */
async function fetchMyOffers(chain, walletAddress, { maxPages = 10 } = {}) {
  const requestedChain = normalizeChain(chain);
  const result = new Map();

  if (!walletAddress || !ethers.isAddress(walletAddress)) return result;

  const wallet = ethers.getAddress(walletAddress).toLowerCase();
  let next = null;

  for (let page = 0; page < maxPages; page++) {
    const params = { limit: 50, sort_by: "START_TIME", sort_direction: "desc" };
    if (next) params.after = next;

    let response;
    try {
      response = await apiGet(`/account/${wallet}/offers`, {
        kind: KIND.READ,
        timeout: 15000,
        retries: 1,
        params,
        label: "my-offers"
      });
    } catch (error) {
      logger.myOffer(`fetch error: ${describeError(error)}`);
      break;
    }

    const data = response.data || {};
    const offers = Array.isArray(data.offers) ? data.offers : [];

    for (const offer of offers) {
      if (!isActiveOrder(offer)) continue;

      const ref = extractOfferAssetRef(offer, requestedChain);
      if (!ref) continue;
      if (ref.chain !== requestedChain) continue;

      const price = extractOrderPrice(offer);
      if (!(price > 0)) continue;

      const key = nftKey(ref.chain, ref.contract, ref.tokenId);
      const existing = result.get(key);
      if (!existing || price > existing.price) {
        result.set(key, {
          price,
          orderHash: extractOrderHash(offer),
          maker: wallet,
          chain: ref.chain,
          contract: ref.contract,
          tokenId: ref.tokenId
        });
      }
    }

    next = data.next || null;
    if (!next || !offers.length) break;
  }

  return result;
}

/**
 * Extract the exact NFT an offer targets. Returns null for collection/trait
 * offers, which have no single token.
 */
function extractOfferAssetRef(offer, fallbackChain = "ethereum") {
  const o = unwrapOrder(offer);
  if (!o) return null;

  let chain = normalizeChain(
    o.chain || o.chain_identifier || o.protocol_chain || fallbackChain
  );

  // A collection or trait offer has no single token. The account endpoint still
  // returns an `asset` block for it, but with `identifier: null` and a criteria
  // payload alongside - so `identifier !== undefined` is NOT enough to accept
  // it. Verified live: that check produced keys like "<contract>:null" for every
  // collection offer in the wallet.
  if (o.criteria) return null;

  const asset = o.asset || null;
  const identifier = asset?.identifier;

  if (
    asset?.contract &&
    identifier !== undefined &&
    identifier !== null &&
    /^\d+$/.test(String(identifier))
  ) {
    try {
      return {
        chain: normalizeChain(asset.chain || chain),
        contract: ethers.getAddress(String(asset.contract)).toLowerCase(),
        tokenId: String(identifier)
      };
    } catch {
      /* fall through */
    }
  }

  const parameters = getProtocolParameters(o);
  const consideration = Array.isArray(parameters?.consideration)
    ? parameters.consideration
    : [];

  for (const item of consideration) {
    const itemType = Number(item.itemType);
    if (itemType !== 2 && itemType !== 3) continue; // exact ERC721 / ERC1155

    const id = String(item.identifierOrCriteria ?? item.identifier ?? "");
    if (!/^\d+$/.test(id)) continue;

    try {
      return {
        chain,
        contract: ethers.getAddress(String(item.token)).toLowerCase(),
        tokenId: id
      };
    } catch {
      /* try next item */
    }
  }

  return null;
}

// ------------------------------------------------------------------
// NFT details (used when the user adds a row)
// ------------------------------------------------------------------

async function fetchNftDetails(chain, contract, tokenId, walletAddress = null) {
  const c = normalizeChain(chain);

  try {
    if (!apiKeys.hasAnyKey()) {
      return { success: false, error: "Thieu OpenSea API Key." };
    }

    const normalizedContract = ethers.getAddress(contract).toLowerCase();
    const meta = await fetchNftMeta(c, normalizedContract, tokenId);
    const best = await fetchBestOffer(
      c,
      normalizedContract,
      tokenId,
      meta.collectionSlug,
      { useCache: false }
    );

    let myOffer = null;
    let myOfferOrderHash = null;

    if (walletAddress && ethers.isAddress(walletAddress)) {
      try {
        const mine = await fetchMyOffers(c, walletAddress, { maxPages: 4 });
        const entry = mine.get(nftKey(c, normalizedContract, tokenId));
        if (entry) {
          myOffer = entry.price;
          myOfferOrderHash = entry.orderHash;
        }
      } catch {
        /* my-offer lookup is best effort while adding a row */
      }
    }

    return {
      success: true,
      name: meta.name,
      image: meta.image,
      collection: meta.collectionName,
      collectionSlug: meta.collectionSlug,
      bestOfferOk: best.ok,
      bestOffer: best.price,
      bestOfferMaker: best.maker,
      bestOfferOrderHash: best.orderHash,
      bestOfferKind: best.kind,
      myOffer,
      myOfferOrderHash
    };
  } catch (error) {
    return {
      success: false,
      error: describeError(error),
      name: `NFT #${tokenId}`,
      image: "",
      collection: "",
      collectionSlug: "",
      bestOfferOk: false,
      bestOffer: 0,
      bestOfferMaker: null,
      bestOfferOrderHash: null,
      myOffer: null,
      myOfferOrderHash: null
    };
  }
}

// ------------------------------------------------------------------
// Cancel-tab read side
// ------------------------------------------------------------------

/**
 * Load EVERY active offer the wallet has, in bulk.
 *
 * This replaces the old per-NFT discovery loop. Measured against the live API:
 *   per-NFT loop : 2 requests per NFT, serialised -> ~1.4s per NFT
 *   this function: 50 offers per request       -> ~110ms per 50 offers
 *
 * The account endpoint does NOT return protocol_data (verified: the field is
 * always null), so callers that need Seaport order components must look each
 * order up individually. Off-chain cancel and the UI need only what is here.
 *
 * @param {object} options
 * @param {string} options.chain
 * @param {string} options.walletAddress
 * @param {Array<{contract:string, tokenId:string}>|null} [options.filter]
 *        restrict to these NFTs; null/empty means every offer in the wallet
 * @param {number} [options.maxPages]
 * @param {(p:object)=>void} [options.onProgress]
 * @returns {Promise<Array<object>>}
 */
async function fetchWalletOffers({
  chain,
  walletAddress,
  filter = null,
  maxPages = 40,
  signal = null,
  onProgress = null,
  quiet = false
}) {
  const c = normalizeChain(chain);

  if (!walletAddress || !ethers.isAddress(walletAddress)) {
    throw new Error("Khong xac dinh duoc wallet tu Private Key.");
  }

  const wallet = ethers.getAddress(walletAddress).toLowerCase();

  // Build the allow-list once so filtering stays O(1) per offer.
  let wanted = null;
  if (Array.isArray(filter) && filter.length) {
    wanted = new Set(
      filter
        .map(ref => {
          try {
            return nftKey(c, ethers.getAddress(ref.contract), ref.tokenId);
          } catch {
            return null;
          }
        })
        .filter(Boolean)
    );
  }

  const report = payload => {
    if (!onProgress) return;
    try {
      onProgress(payload);
    } catch {
      /* progress must never break discovery */
    }
  };

  const results = [];

  /**
   * Pages OVERLAP. Verified live: draining one wallet returned 200 offers
   * across 4 pages but only 149 distinct order hashes - page 2 repeated 6,
   * page 3 repeated 13, page 4 repeated 32. The cursor drifts because the
   * underlying list keeps changing while you page through it.
   *
   * Without this guard the Cancel tab lists the same offer several times and
   * the counts are simply wrong, which is exactly what "quet sai" looked like.
   */
  const seenHashes = new Set();

  let next = null;
  let scanned = 0;
  let duplicates = 0;

  /**
   * WHY THE FEED HAS TO SAY HOW IT ENDED
   *
   * This loop has six exits and they do NOT mean the same thing:
   *
   *   next === null          the API itself said there is no further page
   *   page threw             the sweep is PARTIAL
   *   offers.length === 0    a page came back empty
   *   freshThisPage === 0    the cursor stalled - defensive stop, not proof
   *   next === previousCursor the cursor repeated - defensive stop, not proof
   *   maxPages reached       truncated
   *
   * Only the first is evidence that the wallet's offers were seen in full.
   * The function used to return a bare array, so every caller had to assume
   * the worst - and `discoverMyOffersForNfts` did exactly that: it re-checked
   * EVERY NFT it had not found an offer for, one REST read each. Measured on
   * a wallet with no offers at all and a 1.000-NFT selection: 500.4 seconds,
   * 1.000 requests, to rediscover "still nothing".
   *
   * `pages` is counted so the caller can tell a single-page answer from a
   * paged one. That distinction matters because the drift this loop guards
   * against is a property of PAGING: a scan that never followed a cursor
   * cannot have skipped anything behind one.
   */
  let pages = 0;
  let errored = false;
  let stopReason = "max-pages";

  /**
   * MỘT TRANG KHÔNG CÓ GÌ MỚI CHƯA PHẢI LÀ HẾT DỮ LIỆU
   *
   *   Bản trước dừng ngay lần đầu một trang không thêm được offer nào. Đo trên
   *   ví thật thì có hai hiện tượng KHÁC NHAU cùng cho ra dấu hiệu đó: một
   *   vòng lặp cursor thật (trang 3 trở đi lặp lại mãi), và một trang lặp
   *   thoáng qua khi danh sách bị sắp xếp lại ngay dưới con trỏ — lần sau
   *   cursor lại chạy tiếp bình thường.
   *
   *   Dừng ở trang lặp đầu tiên xử lý hai thứ đó như một, và ở trường hợp thứ
   *   hai nó lặng lẽ bỏ lại những offer còn ở phía sau. Nên chịu đựng vài
   *   trang lặp liên tiếp rồi mới kết luận, và lần nào cũng nói rõ là quét
   *   KHÔNG đầy đủ.
   */
  const STALL_TOLERANCE = 2;
  let stalledRun = 0;

  for (let page = 0; page < maxPages; page++) {
    pages = page + 1;
    const params = { limit: 50 };
    if (next) params.next = next;

    let response;
    try {
      response = await apiGet(`/account/${wallet}/offers`, {
        kind: KIND.READ,
        priority: PRIORITY.P2, signal,
        timeout: 20000,
        retries: 1,
        params,
        label: "wallet-offers"
      });
    } catch (error) {
      report({ phase: "error", error: describeError(error) });
      errored = true;
      stopReason = "page-error";
      break;
    }

    const data = response.data || {};
    const offers = Array.isArray(data.offers) ? data.offers : [];

    let freshThisPage = 0;

    for (const offer of offers) {
      scanned++;
      if (!isActiveOrder(offer)) continue;

      const offerChain = normalizeChain(offer.chain || c);
      if (offerChain !== c) continue;

      const orderHash = extractOrderHash(offer);
      if (!orderHash) continue;

      if (seenHashes.has(orderHash)) {
        duplicates++;
        continue;
      }
      seenHashes.add(orderHash);
      freshThisPage++;

      const asset = offer.asset || null;
      let contract = "";
      let tokenId = "";

      if (asset?.contract) {
        try {
          contract = ethers.getAddress(String(asset.contract)).toLowerCase();
        } catch {
          contract = "";
        }
      }

      // `asset.identifier` is null for collection and trait offers. Stringifying
      // it produced the literal "null", which then went into the lookup key -
      // so a filtered scan could never match a real token and the Cancel tab
      // reported finding nothing at all.
      const hasIdentifier =
        asset?.identifier !== undefined &&
        asset?.identifier !== null &&
        String(asset.identifier) !== "";

      if (hasIdentifier) tokenId = String(asset.identifier);

      // A collection or trait offer has criteria instead of a single asset.
      const isCriteria = !contract || !hasIdentifier || Boolean(offer.criteria);

      if (wanted) {
        // A filtered scan only wants exact tokens the user listed.
        if (isCriteria) continue;
        if (!wanted.has(nftKey(c, contract, tokenId))) continue;
      }

      results.push({
        chain: c,
        contract,
        tokenId,
        orderHash,
        price: extractOrderPrice(offer),
        criteria: isCriteria,
        status: offer.status || null,
        // protocol_data is null on this endpoint, so signed-zone status is not
        // known yet; the cancel layer resolves it only when it needs to.
        signedZone: null,
        createdAt: Number(offer.order_created_at) || 0,
        // Hạn của order (epoch giây) — expiration_time / protocol_data.endTime /
        // closing_date. 0 khi endpoint không nói. Không có nó, Offer Item V2
        // giữ một own order "bất tử" sau resync và không bao giờ đặt lại.
        endTime: getOrderExpiration(offer) ||
          (offer.closing_date ? Math.floor(Date.parse(offer.closing_date) / 1000) || 0 : 0)
      });
    }

    report({
      phase: "scanning",
      page: page + 1,
      scanned,
      found: results.length,
      duplicates
    });

    const previousCursor = next;
    next = data.next || null;

    if (!next || !offers.length) {
      // `next === null` is the API's own statement that the list is finished.
      // An empty page WITH a cursor still pending is not that, so the two are
      // recorded apart rather than lumped into one "we stopped".
      stopReason = next ? "empty-page" : "exhausted";
      break;
    }

    // The cursor can stop advancing while still handing back a `next`: measured
    // on a live wallet, pages 3 onward returned the same 50 offers forever, so
    // 600 fetched offers held only 52 distinct ones. Once a page adds nothing
    // new, paging further is pure cost.
    // Cursor lặp lại CHÍNH NÓ là bằng chứng dứt khoát: trang sau sẽ y hệt
    // trang này, mãi mãi. Không có gì để chịu đựng ở đây.
    if (previousCursor && next === previousCursor) {
      if (!quiet) logger.cancel(`wallet scan: cursor lặp lại ở trang ${page + 1} → dừng, quét KHÔNG đầy đủ`);
      stopReason = "cursor-repeat";
      break;
    }

    if (!freshThisPage) {
      stalledRun++;
      if (stalledRun <= STALL_TOLERANCE) {
        if (!quiet) logger.cancel(
          `wallet scan: trang ${page + 1} không có offer mới (lần ${stalledRun}/${STALL_TOLERANCE}) → đi tiếp`
        );
        continue;
      }
      if (!quiet) logger.cancel(
        `wallet scan: ${stalledRun} trang liên tiếp không có offer mới → dừng, quét KHÔNG đầy đủ`
      );
      stopReason = "cursor-stalled";
      break;
    }
    stalledRun = 0;
  }

  report({ phase: "scanned", scanned, found: results.length, duplicates });

  if (duplicates) {
    if (!quiet) logger.cancel(
      `wallet scan: bo qua ${duplicates} ban sao do cursor trung trang, con ${results.length} offer`
    );
  }

  /**
   * COMPLETE means: every offer this wallet holds is in `results`.
   *
   * Deliberately the STRICTEST reading, not the most useful one:
   *
   *   pages === 1        no cursor was ever followed
   *   stopReason
   *     === "exhausted"  the API said there is no next page
   *   !errored           and no page failed on the way
   *
   * A multi-page sweep that also ended on `next === null` is NOT claimed as
   * complete, and that is on purpose. The drift this loop guards against is a
   * property of paging: measured live, four pages of 50 held only 149 distinct
   * hashes, so the list was being reordered underneath the cursor. A reorder
   * that can repeat an entry can also skip one, and the final page ending
   * cleanly does not prove nothing was skipped earlier. A scan that never
   * followed a cursor has no such window - there was one request and one
   * answer.
   *
   * So the fast path is claimed only where it is provable, and every other
   * ending keeps the per-NFT re-check exactly as it was.
   *
   * Carried as properties ON the array rather than by changing the return
   * type: `Array.isArray`, `.length` and `for...of` are all unaffected, so
   * every existing caller keeps working untouched.
   */
  results.complete = pages === 1 && stopReason === "exhausted" && !errored;
  results.stopReason = stopReason;

  /**
   * BỐN KẾT CỤC, GỌI ĐÚNG TÊN
   *
   *   `complete` trả lời một câu hỏi hẹp và nghiêm ngặt: "có được phép bỏ qua
   *   lượt đọc lại từng NFT không". Nó cố ý chỉ đúng cho lượt quét một trang,
   *   và nó phải giữ nguyên nghĩa đó — Cancel đang dựa vào.
   *
   *   Nhưng người dùng và log cần biết THÊM một chuyện khác: lượt quét này
   *   dừng vì hết dữ liệu, hay vì nó bị chặn giữa chừng. Trước đây hai chuyện
   *   ấy gộp làm một và một lượt quét partial trông y như một lượt quét xong.
   */
  results.status =
    errored ? "PARTIAL_SCAN"
      : stopReason === "exhausted" ? "END_OF_DATA"
        : stopReason === "cursor-repeat" ? "CURSOR_LOOP"
          : stopReason === "cursor-stalled" ? "DUPLICATE_PAGE"
            : "PARTIAL_SCAN";
  results.partial = results.status !== "END_OF_DATA";
  results.duplicates = duplicates;
  if (results.partial) {
    if (!quiet) logger.cancel(
      `wallet scan: KHÔNG ĐẦY ĐỦ (${results.status}) — ${results.length} offer qua ${pages} trang`
    );
  }
  results.pages = pages;

  return results;
}

/**
 * Attach display metadata (name + image) to a list of offers.
 * One request per distinct token, served from nftMetaCache on repeats, and
 * capped so a wallet with hundreds of offers cannot stall the UI.
 */
async function decorateOffersWithMeta(offers, { limit = 60, onProgress = null } = {}) {
  const distinct = new Map();
  for (const offer of offers) {
    if (offer.criteria || !offer.contract) continue;
    const key = nftKey(offer.chain, offer.contract, offer.tokenId);
    if (!distinct.has(key)) distinct.set(key, offer);
  }

  let done = 0;
  for (const [, offer] of distinct) {
    if (done >= limit) break;
    try {
      const meta = await fetchNftMeta(offer.chain, offer.contract, offer.tokenId);
      const key = nftKey(offer.chain, offer.contract, offer.tokenId);
      for (const target of offers) {
        if (target.criteria || !target.contract) continue;
        if (nftKey(target.chain, target.contract, target.tokenId) !== key) continue;
        target.name = meta.name;
        target.image = meta.image;
        target.collection = meta.collectionName;
      }
    } catch {
      /* metadata is cosmetic: a failure must not block cancelling */
    }
    done++;
    if (onProgress) {
      try {
        onProgress({ phase: "meta", done, total: Math.min(distinct.size, limit) });
      } catch {
        /* ignore */
      }
    }
  }

  return offers;
}

/**
 * Find every active offer the wallet has on the given NFTs.
 *
 * @param {Array<{chain:string, contract:string, tokenId:string}>} nftRefs
 * @param {string} walletAddress
 * @param {(p:object)=>void} [onProgress]
 * @returns {Promise<Array<object>>}
 */
async function fetchAllMyOffersForNfts(nftRefs, walletAddress, onProgress = null) {
  if (!walletAddress || !ethers.isAddress(walletAddress)) {
    throw new Error("Khong xac dinh duoc wallet tu Private Key.");
  }

  const wallet = ethers.getAddress(walletAddress).toLowerCase();
  const refs = Array.isArray(nftRefs) ? nftRefs.filter(Boolean) : [];
  const results = [];
  const total = refs.length;

  const report = payload => {
    if (!onProgress) return;
    try {
      onProgress(payload);
    } catch {
      /* progress must never break discovery */
    }
  };

  for (let index = 0; index < refs.length; index++) {
    const ref = refs[index];
    const c = normalizeChain(ref.chain);

    let contract;
    try {
      contract = ethers.getAddress(ref.contract).toLowerCase();
    } catch {
      report({
        phase: "error",
        index,
        total,
        error: `Contract khong hop le: ${ref.contract}`
      });
      continue;
    }

    const tokenId = String(ref.tokenId);

    report({ phase: "scanning", index, total, chain: c, contract, tokenId });

    try {
      const meta = await fetchNftMeta(c, contract, tokenId);
      const slug = meta.collectionSlug;

      if (!slug) {
        report({ phase: "error", index, total, error: "Khong co collection slug." });
        continue;
      }

      const response = await apiGet(
        `/offers/collection/${encodeURIComponent(slug)}/nfts/${encodeURIComponent(tokenId)}`,
        {
          kind: KIND.CANCEL,
          timeout: 15000,
          params: { limit: 50 },
          label: "cancel-discovery"
        }
      );

      const rawOrders = Array.isArray(response.data?.offers)
        ? response.data.offers
        : Array.isArray(response.data?.orders)
          ? response.data.orders
          : [];

      const mine = [];

      for (const order of rawOrders) {
        if (!isActiveOrder(order)) continue;

        const maker = extractOrderMaker(order);
        if (!maker || maker !== wallet) continue;

        const orderHash = extractOrderHash(order);
        if (!orderHash) continue;

        mine.push({
          chain: c,
          contract,
          tokenId,
          name: meta.name,
          image: meta.image,
          collection: meta.collectionName,
          collectionSlug: slug,
          orderHash,
          price: extractOrderPrice(order),
          signedZone: isSignedZoneOrder(order),
          order
        });
      }

      results.push(...mine);

      report({
        phase: "scanned",
        index,
        total,
        chain: c,
        contract,
        tokenId,
        name: meta.name,
        image: meta.image,
        found: mine.length
      });
    } catch (error) {
      report({
        phase: "error",
        index,
        total,
        chain: c,
        contract,
        tokenId,
        error: describeError(error)
      });
    }
  }

  return results;
}

module.exports = {
  metaStats,
  readDispatcher, readFlights,
  // constants
  OPENSEA_API_BASE,
  SEAPORT_V1_6,
  ZERO_ADDRESS,
  ZERO_BYTES32,
  OPENSEA_SIGNED_ZONE_V2,
  OPENSEA_FEE_RECIPIENT,
  OPENSEA_FEE_BPS,
  SUPPORTED_CHAINS,
  OFFER_KIND,
  isTraitCriteria,
  httpsAgent,

  // chain helpers
  normalizeChain,
  getWeth,
  getRpcList,
  getOpenSeaConduitKey,
  getApiHeaders,

  // http
  request,
  apiGet,
  apiPost,

  // parsing / classification
  parseOpenSeaUrl,
  canonicalItemUrl,
  normalizeImage,
  nftKey,
  classifyOffer,
  isActiveOrder,
  isSignedZoneOrder,
  extractOrderPrice,
  extractOrderMaker,
  extractOrderHash,
  extractOfferAssetRef,
  getProtocolParameters,
  describeError,
  fetchBestOfferQuick,
  setCredentialWatcher,
  probeApiKey,
  isRateLimitError,
  isInsufficientBalanceError,
  classifySubmitError,
  permanentSubmitReason,

  // data
  fetchNftMeta,
  fetchCollectionContract,
  fetchCollectionSupply,
  fetchTokenStandard,
  peekTokenStandard,
  normalizeTokenStandard,
  fetchContractCollection,
  fetchNftOffers,
  fetchCollectionAllOffers,
  ownItemFromOrder,
  resolveCollectionSlug,
  fetchCollectionFees,
  fetchBestOffer,
  fetchCollectionOffers,
  criteriaCoversToken,
  fetchMyOffers,
  fetchNftDetails,
  fetchAllMyOffersForNfts,
  fetchWalletOffers,
  decorateOffersWithMeta,
  extractOrderCurrency,
  fetchOfferPages,
  OFFER_MAX_PAGES
};
