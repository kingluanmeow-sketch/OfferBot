"use strict";

/**
 * error-registry.js — the project's fixed vocabulary of failures.
 *
 * A log line answers "what happened". A code answers "which failure is this,
 * exactly" — and that is the question you are actually asking six months later
 * when a user sends a screenshot. So every failure the app can surface has a
 * stable identifier here, alongside what it means, what triggers it, what the
 * app is expected to do about it, and which modules it comes from.
 *
 * THE CONTRACT
 *
 *   A code means one thing, for ever. E_PRICE_GRID is a price that is not on a
 *   valid increment; it will never come to mean a timeout. If a code turns out
 *   to cover two distinct failures it is SPLIT into new codes and the old one
 *   is retired in RETIRED below — never quietly repurposed. A reused code is
 *   worse than no code: it silently invalidates every older log that carries
 *   it, including the ones already sent to you by users.
 *
 * WHERE THE CODES CAME FROM
 *
 *   Every entry below was taken from a failure path that exists in this
 *   codebase today. There is deliberately no code here "for completeness":
 *   a registry padded with codes nothing emits teaches you to distrust it.
 *
 * LOADED FROM BOTH SIDES
 *
 *   main requires this file; the renderer loads it as a classic script. It is
 *   the same file in both, because two copies of a contract is one copy of a
 *   contract and one copy of a bug.
 */

const SEVERITY = Object.freeze({
  /** The run continues; this row or request is affected. */
  WARN: "WARN",
  /** This operation failed. Others are unaffected. */
  ERROR: "ERROR",
  /** The app cannot work until the user does something. */
  BLOCKING: "BLOCKING"
});

const CATEGORY = Object.freeze({
  PRICE: "PRICE",
  MARKET: "MARKET",
  ORDER: "ORDER",
  WALLET: "WALLET",
  NETWORK: "NETWORK",
  SESSION: "SESSION",
  LICENSE: "LICENSE",
  API: "API",
  TIME: "TIME"
});

/**
 * @typedef {object} ErrorEntry
 * @property {string} code
 * @property {string} title            short Vietnamese name
 * @property {string} category
 * @property {string} severity
 * @property {boolean} retryable       does retrying the SAME thing help?
 * @property {string} userMessage      what the user reads. Vietnamese.
 * @property {string} technicalMeaning what actually happened, for a developer
 * @property {string} trigger          the condition that produces it
 * @property {string} expectedBehavior what the app is supposed to do
 * @property {string[]} relatedModules where it comes from
 */

/** @type {Record<string, ErrorEntry>} */
const ERRORS = {
  // ---- price ------------------------------------------------------
  E_PRICE_GRID: {
    code: "E_PRICE_GRID",
    title: "Giá không đúng bậc",
    category: CATEGORY.PRICE,
    severity: SEVERITY.ERROR,
    retryable: false,
    userMessage: "Giá offer không hợp lệ",
    technicalMeaning:
      "The target price is not on a valid increment for its tier " +
      "(<0.1 → 0.0001, 0.1–0.999 → 0.001, ≥1.0 → 0.01). OpenSea refuses the " +
      "order outright rather than rounding it.",
    trigger:
      "A target computed by adding a step to a best offer that was itself off " +
      "grid, or a Step the user set that is finer than the tier allows.",
    expectedBehavior:
      "computeDecision normalises the target onto the grid BEFORE submitting; " +
      "the row does not resend the same figure.",
    relatedModules: ["offer-item-v2/engine-v2.js", "seaport.js"]
  },
  E_PRICE_RANGE: {
    code: "E_PRICE_RANGE",
    title: "Giá vượt giới hạn",
    category: CATEGORY.PRICE,
    severity: SEVERITY.WARN,
    retryable: false,
    userMessage: "Giá cần trả đã vượt Max Price",
    technicalMeaning:
      "The price needed to lead exceeds the row's maxPrice, so no offer is " +
      "sent. Not a failure: the ceiling did its job.",
    trigger: "A competitor's offer at or above the row's Max Price.",
    expectedBehavior:
      "The row stays quiet and keeps watching; it does not send and does not " +
      "count as an error.",
    relatedModules: ["offer-item-v2/engine-v2.js"]
  },

  // ---- the market read --------------------------------------------
  E_BEST_SCAN: {
    code: "E_BEST_SCAN",
    title: "Không đọc được Best Offer",
    category: CATEGORY.MARKET,
    severity: SEVERITY.ERROR,
    retryable: true,
    userMessage: "Không lấy được Best Offer",
    technicalMeaning:
      "fetchBestOffer threw: the request failed, timed out, or returned " +
      "something unparseable.",
    trigger: "An OpenSea read that did not complete.",
    expectedBehavior: "The row retries on its next scan; no offer is sent on a blind read.",
    relatedModules: ["opensea.js", "offer-item-v2/engine-v2.js"]
  },
  E_BEST_STALE: {
    code: "E_BEST_STALE",
    title: "Best Offer đã cũ",
    category: CATEGORY.MARKET,
    severity: SEVERITY.WARN,
    retryable: true,
    userMessage: "Giá đã thay đổi, đang kiểm tra lại",
    technicalMeaning:
      "The cached book entry is older than the freshness window, so a belief " +
      "that we lead cannot be trusted. Being on top is NOT self-correcting: " +
      "nothing arrives to tell us we have been beaten.",
    trigger: "A row believed to be leading whose last confirmed read has aged out.",
    expectedBehavior: "Re-read before deciding; never re-affirm a lead from a stale entry.",
    relatedModules: ["offer-item-v2/memory-book.js", "offer-item-v2/engine-v2.js"]
  },
  E_BEST_EMPTY: {
    code: "E_BEST_EMPTY",
    title: "Chưa có Best Offer",
    category: CATEGORY.MARKET,
    severity: SEVERITY.WARN,
    retryable: true,
    userMessage: "Chưa có offer nào trên NFT này",
    technicalMeaning: "The read succeeded and returned no competing offer.",
    trigger: "An NFT nobody has bid on yet.",
    expectedBehavior: "Offer at Min Price; this is a normal state, not a fault.",
    relatedModules: ["opensea.js", "offer-item-v2/engine-v2.js"]
  },

  // ---- placing an order -------------------------------------------
  E_ORDER_RATE_LIMIT: {
    code: "E_ORDER_RATE_LIMIT",
    title: "Bị giới hạn tần suất",
    category: CATEGORY.ORDER,
    severity: SEVERITY.WARN,
    retryable: true,
    userMessage: "Đang bị giới hạn số lần gửi, sẽ thử lại",
    technicalMeaning:
      "HTTP 429. The limiter adopts the server's own Retry-After and " +
      "x-ratelimit headers rather than guessing, and rests the throttled key.",
    trigger: "Too many requests on one key, or a burst across the pool.",
    expectedBehavior:
      "Retry on another key immediately if one is free, otherwise back off. " +
      "This is NEVER treated as a credential problem — see E_API_INVALID.",
    relatedModules: ["opensea.js", "rate-limiter.js"]
  },
  E_ORDER_TIMEOUT: {
    code: "E_ORDER_TIMEOUT",
    title: "Gửi offer quá lâu",
    category: CATEGORY.ORDER,
    severity: SEVERITY.ERROR,
    retryable: true,
    userMessage: "OpenSea phản hồi quá chậm",
    technicalMeaning:
      "HTTP 408 or a client timeout. Whether the order landed is UNKNOWN, " +
      "which is why the row verifies rather than assuming either way.",
    trigger: "OpenSea slow or unreachable mid-submit.",
    expectedBehavior: "Verify, then retry if the order is genuinely absent.",
    relatedModules: ["opensea.js", "seaport.js"]
  },
  E_ORDER_VALIDATION: {
    code: "E_ORDER_VALIDATION",
    title: "Order bị từ chối vì sai dữ liệu",
    category: CATEGORY.ORDER,
    severity: SEVERITY.ERROR,
    retryable: false,
    userMessage: "OpenSea từ chối order này",
    technicalMeaning:
      "A deliberate 4xx naming the payload: validation error, invalid order, " +
      "or an itemType that does not match the contract standard. The same " +
      "bytes will be refused again.",
    trigger: "An order built for the wrong token standard, or a malformed field.",
    expectedBehavior:
      "The row goes to blocked and stops resending. Retrying is pointless and " +
      "burns write capacity.",
    relatedModules: ["seaport.js", "opensea.js"]
  },
  E_ORDER_REJECTED: {
    code: "E_ORDER_REJECTED",
    title: "Order bị từ chối",
    category: CATEGORY.ORDER,
    severity: SEVERITY.ERROR,
    retryable: false,
    userMessage: "OpenSea không nhận offer này",
    technicalMeaning:
      "A 4xx refusal that is not one of the recognised validation shapes. " +
      "Distinct from E_ORDER_VALIDATION so an unrecognised refusal does not " +
      "hide behind a diagnosis nobody verified.",
    trigger: "A deliberate refusal whose reason the app does not recognise.",
    expectedBehavior: "Treated as permanent; the full body is logged once.",
    relatedModules: ["seaport.js", "opensea.js"]
  },
  E_ORDER_SUPERSEDED: {
    code: "E_ORDER_SUPERSEDED",
    title: "Bỏ qua vì giá đã đổi",
    category: CATEGORY.ORDER,
    severity: SEVERITY.WARN,
    retryable: false,
    userMessage: "Đã bỏ qua vì giá đã thay đổi",
    technicalMeaning:
      "The target this submit was built for is no longer current — a newer " +
      "read or a stream event overtook it mid-flight. Sending it would place " +
      "an offer at a price the app has already decided against.",
    trigger: "A price change arriving between decision and submit.",
    expectedBehavior:
      "Abort THIS attempt and settle its obligation; the next decision runs " +
      "on the newer price. Not a failure.",
    relatedModules: ["offer-item-v2/engine-v2.js"]
  },

  // ---- wallet and chain -------------------------------------------
  E_SIGNATURE: {
    code: "E_SIGNATURE",
    title: "Không ký được",
    category: CATEGORY.WALLET,
    severity: SEVERITY.ERROR,
    retryable: false,
    userMessage: "Không ký được giao dịch",
    technicalMeaning:
      "EIP-712 signing failed, or OpenSea rejected the signature it was given.",
    trigger: "A bad private key, a wrong counter, or a malformed order.",
    expectedBehavior: "Stop; nothing is resent until the wallet is usable.",
    relatedModules: ["seaport.js", "wallet.js"]
  },
  E_BALANCE_LOW: {
    code: "E_BALANCE_LOW",
    title: "Không đủ WETH",
    category: CATEGORY.WALLET,
    severity: SEVERITY.WARN,
    retryable: true,
    userMessage: "Số dư WETH không đủ để gửi offer",
    technicalMeaning:
      "The wallet cannot cover the offer. Classified as transient, not as an " +
      "order fault: the order is fine, the wallet is short.",
    trigger: "Offers outstanding beyond the WETH balance.",
    expectedBehavior:
      "The row shows the shortfall and keeps watching; topping up resumes it " +
      "without a restart.",
    relatedModules: ["seaport.js", "offer-item-v2/engine-v2.js"]
  },
  E_RPC: {
    code: "E_RPC",
    title: "Lỗi RPC",
    category: CATEGORY.NETWORK,
    severity: SEVERITY.ERROR,
    retryable: true,
    userMessage: "Không kết nối được mạng blockchain",
    technicalMeaning:
      "An RPC endpoint failed — reading a balance, or the Seaport counter. " +
      "The list is tried in order.",
    trigger: "An RPC host down, rate limiting, or refusing the call.",
    expectedBehavior: "Fall through to the next RPC; report only when all fail.",
    relatedModules: ["seaport.js", "opensea.js"]
  },
  E_NETWORK: {
    code: "E_NETWORK",
    title: "Mất kết nối",
    category: CATEGORY.NETWORK,
    severity: SEVERITY.ERROR,
    retryable: true,
    userMessage: "Mất kết nối tới OpenSea",
    technicalMeaning:
      "A transport failure with no HTTP response at all: DNS, socket, or an " +
      "aborted connection. Nothing about the credential is proven.",
    trigger: "Connectivity loss, or OpenSea unreachable.",
    expectedBehavior:
      "Retry with backoff. Explicitly NOT an API-credential failure — see " +
      "E_API_UNAVAILABLE.",
    relatedModules: ["opensea.js", "opensea-gql.js"]
  },

  // ---- cancelling --------------------------------------------------
  E_CANCEL_REJECTED: {
    code: "E_CANCEL_REJECTED",
    title: "Không huỷ được offer",
    category: CATEGORY.ORDER,
    severity: SEVERITY.ERROR,
    retryable: true,
    userMessage: "Không huỷ được offer này",
    technicalMeaning: "OpenSea refused the cancellation for these orders.",
    trigger: "Orders already gone, or a refused request.",
    expectedBehavior:
      "The failed ids are kept so only they are retried — never the whole run.",
    relatedModules: ["bulk-cancel.js", "seaport.js"]
  },
  E_CANCEL_ONCHAIN: {
    code: "E_CANCEL_ONCHAIN",
    title: "Giao dịch huỷ thất bại",
    category: CATEGORY.WALLET,
    severity: SEVERITY.ERROR,
    retryable: true,
    userMessage: "Giao dịch huỷ không thành công",
    technicalMeaning:
      "The on-chain wipe could not be broadcast or confirmed at that nonce.",
    trigger: "A nonce clash, gas, or an RPC that dropped the transaction.",
    expectedBehavior: "Report the nonce and stop; do not silently rebroadcast.",
    relatedModules: ["seaport.js"]
  },

  // ---- the OpenSea session ----------------------------------------
  E_SESSION_EXPIRED: {
    code: "E_SESSION_EXPIRED",
    title: "Phiên OpenSea hết hạn",
    category: CATEGORY.SESSION,
    severity: SEVERITY.ERROR,
    retryable: false,
    userMessage: "Phiên đăng nhập OpenSea đã hết hạn",
    technicalMeaning: "The browser session is no longer accepted.",
    trigger: "A login that aged out or was invalidated elsewhere.",
    expectedBehavior: "Reopen the session; rescanning alone cannot fix it.",
    relatedModules: ["session.js", "browser.js"]
  },
  E_SESSION_WALLET: {
    code: "E_SESSION_WALLET",
    title: "Ví không khớp",
    category: CATEGORY.SESSION,
    severity: SEVERITY.BLOCKING,
    retryable: false,
    userMessage: "Ví đăng nhập không khớp với Private Key",
    technicalMeaning:
      "The signed-in OpenSea account is a different address from the one the " +
      "private key controls.",
    trigger: "A private key changed without re-logging in, or the wrong account.",
    expectedBehavior:
      "Refuse to act. Cancelling or offering from a mismatched pair touches " +
      "the wrong account's orders.",
    relatedModules: ["session.js", "wallet.js"]
  },

  // ---- the licence -------------------------------------------------
  E_LICENSE_MISSING: {
    code: "E_LICENSE_MISSING",
    title: "Chưa kích hoạt",
    category: CATEGORY.LICENSE,
    severity: SEVERITY.BLOCKING,
    retryable: false,
    userMessage: "Chưa kích hoạt license",
    technicalMeaning: "No key has been entered.",
    trigger: "A fresh install.",
    expectedBehavior: "Locked mode; only the licence field is reachable.",
    relatedModules: ["license.js"]
  },
  E_LICENSE_INVALID: {
    code: "E_LICENSE_INVALID",
    title: "License không hợp lệ",
    category: CATEGORY.LICENSE,
    severity: SEVERITY.BLOCKING,
    retryable: false,
    userMessage: "License không hợp lệ",
    technicalMeaning:
      "The Ed25519 signature over the payload does not verify against the " +
      "public key in the build.",
    trigger: "A mistyped key, or one issued by something other than the server.",
    expectedBehavior: "Locked mode.",
    relatedModules: ["license.js", "license-client.js"]
  },
  E_LICENSE_EXPIRED: {
    code: "E_LICENSE_EXPIRED",
    title: "License hết hạn",
    category: CATEGORY.LICENSE,
    severity: SEVERITY.BLOCKING,
    retryable: false,
    userMessage: "License đã hết hạn",
    technicalMeaning:
      "Past `exp`, judged against the trusted clock — the offline allowance " +
      "IS the licence term, and the client cannot extend it by itself.",
    trigger: "The term ending.",
    expectedBehavior: "Locked mode until a new key is entered.",
    relatedModules: ["license.js", "trusted-time.js"]
  },
  E_LICENSE_REVOKED: {
    code: "E_LICENSE_REVOKED",
    title: "License bị thu hồi",
    category: CATEGORY.LICENSE,
    severity: SEVERITY.BLOCKING,
    retryable: false,
    userMessage: "License đã bị thu hồi",
    technicalMeaning: "The server's signed answer says REVOKED.",
    trigger: "The owner revoking it.",
    expectedBehavior: "Locked mode.",
    relatedModules: ["license-client.js", "license-server/server.js"]
  },
  E_DEVICE_REFUSED: {
    code: "E_DEVICE_REFUSED",
    title: "Thiết bị không được phép",
    category: CATEGORY.LICENSE,
    severity: SEVERITY.BLOCKING,
    retryable: false,
    userMessage: "Thiết bị này không được phép dùng license",
    technicalMeaning:
      "The device count is already at maxDevices and this device is not one " +
      "of them. One Windows user on one PC is one device, however many " +
      "instances they run.",
    trigger: "Activating on a further machine.",
    expectedBehavior: "Locked mode; the owner can reset the device binding.",
    relatedModules: ["license-client.js", "license-server/server.js"]
  },
  E_DEVICE_DISABLED: {
    code: "E_DEVICE_DISABLED",
    title: "Thiết bị bị khoá",
    category: CATEGORY.LICENSE,
    severity: SEVERITY.BLOCKING,
    retryable: false,
    userMessage: "Thiết bị này đã bị khoá",
    technicalMeaning: "This device is registered but disabled by the owner.",
    trigger: "The owner disabling it.",
    expectedBehavior: "Locked mode.",
    relatedModules: ["license-client.js", "license-server/server.js"]
  },

  // ---- the OpenSea API credential ---------------------------------
  //
  // These four are kept apart on purpose. Locking a working app because
  // OpenSea was briefly down is a worse failure than not locking it, so
  // nothing here fires without evidence about the CREDENTIAL itself.
  E_API_INVALID: {
    code: "E_API_INVALID",
    title: "API Key không hợp lệ",
    category: CATEGORY.API,
    severity: SEVERITY.BLOCKING,
    retryable: false,
    userMessage: "API OpenSea không hợp lệ. Vui lòng kiểm tra lại API Key.",
    technicalMeaning:
      "OpenSea answered 401/403 and the body names the key itself — invalid, " +
      "unknown or malformed.",
    trigger: "A wrong or deleted key.",
    expectedBehavior: "API locked mode; only the API field is reachable.",
    relatedModules: ["opensea.js", "main.js"]
  },
  E_API_EXPIRED: {
    code: "E_API_EXPIRED",
    title: "API Key hết hạn",
    category: CATEGORY.API,
    severity: SEVERITY.BLOCKING,
    retryable: false,
    userMessage: "API Key OpenSea đã hết hạn. Vui lòng kích hoạt API Key mới.",
    technicalMeaning: "A 401/403 whose body says the key has expired.",
    trigger: "A key past its validity.",
    expectedBehavior: "API locked mode.",
    relatedModules: ["opensea.js", "main.js"]
  },
  E_API_UNAUTHORIZED: {
    code: "E_API_UNAUTHORIZED",
    title: "API bị từ chối",
    category: CATEGORY.API,
    severity: SEVERITY.BLOCKING,
    retryable: false,
    userMessage: "OpenSea từ chối API Key này.",
    technicalMeaning:
      "A 401/403 that is clearly about authentication but names neither an " +
      "invalid nor an expired key. Separate from E_API_INVALID so an " +
      "unexplained refusal is not recorded as a diagnosis nobody made.",
    trigger: "A key without the required permission, or one refused outright.",
    expectedBehavior: "API locked mode.",
    relatedModules: ["opensea.js", "main.js"]
  },
  E_API_UNAVAILABLE: {
    code: "E_API_UNAVAILABLE",
    title: "Không kết nối được OpenSea",
    category: CATEGORY.API,
    severity: SEVERITY.WARN,
    retryable: true,
    userMessage:
      "Không thể kết nối OpenSea. Chưa xác định đây là lỗi API Key.",
    technicalMeaning:
      "5xx, a timeout, or no response at all. This proves nothing about the " +
      "credential, so it MUST NOT lock the app.",
    trigger: "OpenSea down or unreachable.",
    expectedBehavior:
      "Keep working and retry. The distinction from E_API_INVALID is the " +
      "whole point of this group.",
    relatedModules: ["opensea.js", "main.js"]
  },

  E_API_STATE_DESYNC: {
    code: "E_API_STATE_DESYNC",
    title: "Trạng thái API chưa đồng bộ",
    category: CATEGORY.API,
    severity: SEVERITY.ERROR,
    retryable: true,
    userMessage:
      "Trạng thái OpenSea API chưa được đồng bộ. Vui lòng kích hoạt lại API.",
    technicalMeaning:
      "The app believes the API is active, but the component about to make a " +
      "request cannot obtain the credential. The key was written to one store " +
      "and not another, so a working key reads as missing.",
    trigger:
      "A credential written to the SecretStore without also being loaded into " +
      "the key manager the request path draws from.",
    expectedBehavior:
      "Report it as a desync, NOT as a missing or invalid key, and do not lock " +
      "the app: nothing is wrong with the credential itself.",
    relatedModules: ["main.js", "rate-limiter.js"]
  },

  // ---- clean cancel -------------------------------------------------
  //
  // Three outcomes, one invariant: the network is stopped in all of them.
  E_CLEAN_CANCEL: {
    code: "E_CLEAN_CANCEL",
    title: "Đã huỷ sạch",
    category: CATEGORY.ORDER,
    severity: SEVERITY.WARN,
    retryable: false,
    userMessage: "Đã huỷ sạch và dừng toàn bộ hoạt động của mạng này",
    technicalMeaning:
      "Every cancellable order was retired and the chain's engine was halted. " +
      "Only an explicit Start resumes it.",
    trigger: "The user pressing Huỷ hết on a chain.",
    expectedBehavior:
      "Cancel, then halt: no scan, no stream reaction, no cadence tick, no new " +
      "offer on that chain until Start.",
    relatedModules: ["main.js", "offer-item-v2/engine-v2.js", "seaport.js"]
  },
  E_CLEAN_CANCEL_PARTIAL: {
    code: "E_CLEAN_CANCEL_PARTIAL",
    title: "Huỷ sạch hoàn tất một phần",
    category: CATEGORY.ORDER,
    severity: SEVERITY.WARN,
    retryable: true,
    userMessage: "Đã huỷ được một phần. Mạng này vẫn đã được dừng",
    technicalMeaning:
      "Some orders could not be retired. The halt is applied regardless.",
    trigger: "A wipe where some cancellations were refused.",
    expectedBehavior:
      "Report both figures and STILL halt. Leaving the engine running because " +
      "a few cancels failed is how a wallet the user was clearing gets fresh " +
      "offers placed on it.",
    relatedModules: ["main.js", "offer-item-v2/engine-v2.js"]
  },
  E_CLEAN_CANCEL_FAILED: {
    code: "E_CLEAN_CANCEL_FAILED",
    title: "Huỷ sạch thất bại",
    category: CATEGORY.ORDER,
    severity: SEVERITY.ERROR,
    retryable: true,
    userMessage: "Không huỷ được. Mạng này vẫn đã được dừng để an toàn",
    technicalMeaning:
      "The cancellation did not go through. The halt is applied anyway, on " +
      "purpose: the user asked for this network to stop, and that half of the " +
      "request does not depend on the other half succeeding.",
    trigger: "A wipe that failed outright.",
    expectedBehavior: "Halt, say plainly that nothing was cancelled, and wait for Start.",
    relatedModules: ["main.js", "offer-item-v2/engine-v2.js"]
  },

  // ---- updating the app --------------------------------------------
  //
  // An update that fails must say WHICH failure it was: a user can retry a
  // network problem and cannot do anything at all about a bad signature.
  E_UPDATE_NETWORK: {
    code: "E_UPDATE_NETWORK",
    title: "Không kết nối được để cập nhật",
    category: CATEGORY.NETWORK,
    severity: SEVERITY.WARN,
    retryable: true,
    userMessage: "Không kết nối được để kiểm tra bản mới. Hãy thử lại.",
    technicalMeaning:
      "The update feed could not be reached: DNS, socket, or a timeout. It says nothing about whether an update exists.",
    trigger: "No connectivity, or GitHub unreachable.",
    expectedBehavior: "Leave the running version alone and let the user retry.",
    relatedModules: ["updater.js"]
  },
  E_UPDATE_NOT_FOUND: {
    code: "E_UPDATE_NOT_FOUND",
    title: "Chưa có bản phát hành nào",
    category: CATEGORY.NETWORK,
    severity: SEVERITY.WARN,
    retryable: true,
    userMessage: "Chưa có bản cập nhật nào được phát hành.",
    technicalMeaning:
      "The feed answered but carries no release for this platform - commonly a repository with no published release yet, or one without latest.yml.",
    trigger: "A build published without its updater metadata, or an empty repo.",
    expectedBehavior: "Say so plainly. This is not an error the user caused.",
    relatedModules: ["updater.js"]
  },
  E_UPDATE_SIGNATURE: {
    code: "E_UPDATE_SIGNATURE",
    title: "Bản cập nhật không hợp lệ",
    category: CATEGORY.API,
    severity: SEVERITY.BLOCKING,
    retryable: false,
    userMessage:
      "Bản cập nhật không hợp lệ và đã bị từ chối. Đừng cài thủ công.",
    technicalMeaning:
      "The downloaded installer failed its Authenticode or checksum check. electron-updater refuses to run it, and that refusal is the feature.",
    trigger: "A corrupted download, or a binary that is not ours.",
    expectedBehavior:
      "Refuse. Never offer a way to install it anyway - a user talked into bypassing this is exactly the attack the check exists for.",
    relatedModules: ["updater.js"]
  },
  E_UPDATE_PERMISSION: {
    code: "E_UPDATE_PERMISSION",
    title: "Không có quyền cài đặt",
    category: CATEGORY.API,
    severity: SEVERITY.ERROR,
    retryable: true,
    userMessage:
      "Không có quyền cài bản mới. Hãy đóng app và mở lại bằng quyền quản trị.",
    technicalMeaning:
      "Windows refused the write - the install directory is protected, or a file is locked by the running process.",
    trigger: "Installed under Program Files without elevation.",
    expectedBehavior: "Report it; the current version keeps running.",
    relatedModules: ["updater.js"]
  },
  E_UPDATE_NOT_READY: {
    code: "E_UPDATE_NOT_READY",
    title: "Chưa tải xong",
    category: CATEGORY.ORDER,
    severity: SEVERITY.WARN,
    retryable: true,
    userMessage: "Chưa tải xong bản mới.",
    technicalMeaning:
      "Install was requested with nothing staged. quitAndInstall would close the app and install nothing, which reads as the app vanishing.",
    trigger: "Pressing restart before the download finished.",
    expectedBehavior: "Refuse and keep running.",
    relatedModules: ["updater.js"]
  },
  E_UPDATE_INSTANCE_BUSY: {
    code: "E_UPDATE_INSTANCE_BUSY",
    title: "Cửa sổ khác đang cập nhật",
    category: CATEGORY.API,
    severity: SEVERITY.WARN,
    retryable: true,
    userMessage: "Một cửa sổ OfferBot khác đang quản lý cập nhật. Hãy kiểm tra tại cửa sổ đó.",
    technicalMeaning: "A per-installation owner lock prevents parallel check, download, and installer runs.",
    trigger: "A second Tool window requested an update while the owner was active.",
    expectedBehavior: "Keep the update operation single-owner and direct the user to its window.",
    relatedModules: ["updater.js"]
  },
  E_UPDATE_FAILED: {
    code: "E_UPDATE_FAILED",
    title: "Cập nhật thất bại",
    category: CATEGORY.API,
    severity: SEVERITY.ERROR,
    retryable: true,
    userMessage: "Không cập nhật được. Hãy thử lại sau.",
    technicalMeaning:
      "An update failure that none of the specific codes recognises. Kept separate from them so an unexplained failure is never recorded as a diagnosis nobody made.",
    trigger: "Anything else the updater reports.",
    expectedBehavior: "Report, keep the current version running.",
    relatedModules: ["updater.js"]
  },

  // ---- the window itself -------------------------------------------
  E_UI_ACTION: {
    code: "E_UI_ACTION",
    title: "Thao tác không chạy được",
    category: CATEGORY.ORDER,
    severity: SEVERITY.ERROR,
    retryable: true,
    userMessage: "Thao tác không thực hiện được. Hãy thử lại.",
    technicalMeaning:
      "An exception escaped a click handler in the renderer. Without this the " +
      "failure is an unhandled rejection and produces nothing at all - no " +
      "message, no log - so the control simply appears dead.",
    trigger:
      "A programming fault in the window: reading a control that was removed, " +
      "a null node, a bad payload.",
    expectedBehavior:
      "Tell the user in one sentence, release whatever the handler had " +
      "disabled, and record the detail for the technical view.",
    relatedModules: ["renderer/utils.js", "main.js"]
  },

  // ---- doing things in order ---------------------------------------
  E_WORKFLOW_ORDER: {
    code: "E_WORKFLOW_ORDER",
    title: "Chưa đủ điều kiện để thực hiện",
    category: CATEGORY.ORDER,
    severity: SEVERITY.WARN,
    retryable: false,
    userMessage: "Hãy hoàn thành bước trước đã",
    technicalMeaning:
      "A step was requested whose predecessor has not completed for THIS link " +
      "list - cancelling without a scan, or scanning without a read. Checked in " +
      "main, not only in the window: a disabled button is a hint, and anything " +
      "that can reach the IPC channel can skip it.",
    trigger:
      "Clicking out of order, a stale window, or a direct call to the channel.",
    expectedBehavior:
      "Refuse and say which step is missing. Nothing is sent, nothing is spent.",
    relatedModules: ["main.js", "renderer/cancel.js", "renderer/bulk-offer.js"]
  },

  // ---- reading a collection ---------------------------------------
  E_COLLECTION_DETECT: {
    code: "E_COLLECTION_DETECT",
    title: "Không xác định được collection",
    category: CATEGORY.MARKET,
    severity: SEVERITY.ERROR,
    retryable: true,
    userMessage: "Không xác định được collection từ link NFT",
    technicalMeaning:
      "The contract could not be resolved to a collection slug. Distinct from " +
      "a missing credential: a request was made and did not answer usefully.",
    trigger: "A contract OpenSea does not return a collection for, or a read that failed.",
    expectedBehavior:
      "Say the collection could not be read. Never report it as a missing API " +
      "key when a key is present - that sends the user to fix the wrong thing.",
    relatedModules: ["collection-detect.js", "main.js"]
  },

  // ---- time --------------------------------------------------------
  E_TIME_SYNC: {
    code: "E_TIME_SYNC",
    title: "Không lấy được giờ chuẩn",
    category: CATEGORY.TIME,
    severity: SEVERITY.WARN,
    retryable: true,
    userMessage: "Chưa lấy được giờ chuẩn",
    technicalMeaning:
      "No reference host answered with a Date header, so there is no trusted " +
      "time this session.",
    trigger: "Offline, or every reference host unreachable.",
    expectedBehavior:
      "Keep the last high-water mark. An honest offline machine is not " +
      "punished; a wound-back one is still caught.",
    relatedModules: ["trusted-time.js"]
  },
  E_CLOCK_ROLLBACK: {
    code: "E_CLOCK_ROLLBACK",
    title: "Đồng hồ bị lùi",
    category: CATEGORY.TIME,
    severity: SEVERITY.BLOCKING,
    retryable: false,
    userMessage: "Đồng hồ máy không chính xác",
    technicalMeaning:
      "The system clock is behind the recorded high-water mark by more than " +
      "the tolerance, and no trusted time is available to settle it.",
    trigger: "Winding Windows back to extend a licence.",
    expectedBehavior:
      "Fail closed: the expiry verdict is undetermined rather than a free pass.",
    relatedModules: ["trusted-time.js", "license-client.js"]
  }
};

/**
 * Codes that once existed and must never be reissued.
 *
 * Empty today. When a code is split, its old name goes here with what it
 * became, so an old log line stays readable instead of resolving to nothing.
 * @type {Record<string, {retiredIn:string, replacedBy:string[], note:string}>}
 */
const RETIRED = {
  // 1.19.17: Offer Custom và Trait Metadata Editor bị gỡ khỏi app. Mã giữ tên
  // để không bao giờ bị dùng lại cho một lỗi khác.
  E_OTRAIT_SCAN: "1.19.17 — Offer Custom đã gỡ",
  E_OTRAIT_OFFER: "1.19.17 — Offer Custom đã gỡ",
  E_TRAIT_DISABLED: "1.19.17 — Trait Metadata Editor đã gỡ",
  E_TRAIT_SCAN: "1.19.17 — Trait Metadata Editor đã gỡ",
  E_TRAIT_MAKE: "1.19.17 — Trait Metadata Editor đã gỡ",
  E_TRAIT_REFRESH: "1.19.17 — Trait Metadata Editor đã gỡ",
  // 1.25.0: tính năng Báo lỗi (gửi log lên máy chủ) bị gỡ hẳn khỏi app.
  E_REPORT_NO_SERVER: "1.25.0 — Báo lỗi đã gỡ",
  E_REPORT_LOOPBACK_ENDPOINT: "1.25.0 — Báo lỗi đã gỡ",
  E_REPORT_BUILD: "1.25.0 — Báo lỗi đã gỡ",
  E_REPORT_SEND: "1.25.0 — Báo lỗi đã gỡ"
};

// ------------------------------------------------------------------
// Lookup
// ------------------------------------------------------------------

function get(code) {
  return ERRORS[String(code || "")] || null;
}

function all() {
  return Object.values(ERRORS);
}

function codes() {
  return Object.keys(ERRORS);
}

// ------------------------------------------------------------------
// Classification
// ------------------------------------------------------------------

const HTTP_STATUS = error =>
  (error && (error.status || (error.response && error.response.status))) || 0;

/** The body as searchable text, whatever shape it arrived in. */
function bodyText(error) {
  if (!error) return "";
  if (typeof error === "string") return error;
  const data = error.response && error.response.data;
  let body = "";
  if (data !== undefined && data !== null) {
    try {
      body = typeof data === "string" ? data : JSON.stringify(data);
    } catch {
      body = "";
    }
  }
  return `${error.message || ""} ${body}`.toLowerCase();
}

/**
 * Is this failure evidence that the API CREDENTIAL is bad?
 *
 * Deliberately narrow. Everything that is merely a bad moment for OpenSea —
 * 429, 5xx, a timeout, no response at all — returns E_API_UNAVAILABLE, which
 * never locks anything. Only a 401/403 counts, because that is the only answer
 * that is about who is asking rather than about what was asked.
 *
 * @returns {string|null} an API code, or null when the failure says nothing
 *   about the credential at all.
 */
function classifyApiError(error) {
  const status = HTTP_STATUS(error);
  const text = bodyText(error);

  // A throttle is not a credential problem, and treating it as one would lock
  // a perfectly good key in the middle of a busy run.
  if (status === 429) return null;
  if (status >= 500) return "E_API_UNAVAILABLE";
  if (status === 408) return "E_API_UNAVAILABLE";

  if (status === 401 || status === 403) {
    if (/expired|hết hạn/.test(text)) return "E_API_EXPIRED";
    if (/invalid api key|invalid key|api key not found|unknown api key|bad api key/.test(text)) {
      return "E_API_INVALID";
    }
    // A refusal we cannot explain is still a refusal about the credential.
    return "E_API_UNAUTHORIZED";
  }

  // No response at all: the request never got an answer, so it carries no
  // evidence either way.
  if (!status) return "E_API_UNAVAILABLE";

  return null;
}

/** True for the three codes that mean the credential itself is the problem. */
function isCredentialFailure(code) {
  return code === "E_API_INVALID" ||
    code === "E_API_EXPIRED" ||
    code === "E_API_UNAUTHORIZED";
}

/**
 * The best code for a failure, given where it happened.
 *
 * `context` narrows the answer: the same 429 is E_ORDER_RATE_LIMIT when
 * submitting and nothing at all when merely reading, and the caller knows
 * which it was doing.
 *
 * @param {"order"|"read"|"cancel"|"api"|"wallet"} context
 */
function classify(context, error) {
  const status = HTTP_STATUS(error);
  const text = bodyText(error);

  if (context === "api") return classifyApiError(error);

  if (status === 429) return "E_ORDER_RATE_LIMIT";
  if (status === 408 || /timeout|timed out|etimedout/.test(text)) return "E_ORDER_TIMEOUT";

  if (/insufficient|not enough|balance/.test(text)) return "E_BALANCE_LOW";
  if (/invalid signature|signature/.test(text)) return "E_SIGNATURE";

  if (status >= 400 && status < 500) {
    if (/validation error|invalid order|itemtype does not match|does not match the standard/
      .test(text)) {
      return "E_ORDER_VALIDATION";
    }
    return context === "cancel" ? "E_CANCEL_REJECTED" : "E_ORDER_REJECTED";
  }

  if (!status) return "E_NETWORK";
  if (status >= 500) return "E_NETWORK";

  return context === "read" ? "E_BEST_SCAN" : "E_ORDER_REJECTED";
}

// ------------------------------------------------------------------
// Redaction
// ------------------------------------------------------------------

/**
 * Remove credentials from anything about to be written down.
 *
 * Applied to technical detail before it reaches a log, because the technical
 * view exists to be pasted into a support message and a key pasted with it is
 * a key that has left the machine. Shapes, not a list of known values: a
 * redactor that only knows the current key stops working the moment it rotates.
 */
const SECRET_PATTERNS = [
  [/(0x)?[0-9a-fA-F]{64}\b/g, "«đã ẩn»"],                    // private key / hash-length hex
  [/\bOSB1\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+/g, "«đã ẩn»"],     // licence key
  [/\b[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}\b/g,
    "«đã ẩn»"],                                              // uuid-shaped API key
  // The value may sit behind a scheme word ("Authorization: Bearer xyz"), so
  // the scheme is part of the separator rather than being mistaken for the
  // secret - which is how the token after it survived.
  [/(x-api-key|api[_-]?key|authorization|token)(["'\s:=]+)(bearer\s+)?([^\s"',}]{6,})/gi,
    (_m, k, sep, scheme) => `${k}${sep}${scheme || ""}«đã ẩn»`]
];

/**
 * Shared client keys removed by VALUE. Since 1.19.17 the app ships no shared
 * client credential (the built-in Alchemy key is gone), so there is nothing
 * to match exactly; the labelled patterns above are the whole defence.
 */
function redactSharedKeys(value) {
  return value;
}

function redact(text) {
  let value = String(text === undefined || text === null ? "" : text);
  for (const [pattern, replacement] of SECRET_PATTERNS) {
    value = value.replace(pattern, replacement);
  }
  return redactSharedKeys(value);
}

// ------------------------------------------------------------------
// Log lines
// ------------------------------------------------------------------

const LEVEL_LABEL = Object.freeze({
  WARN: "CẢNH BÁO",
  ERROR: "LỖI",
  BLOCKING: "LỖI"
});

/**
 * One user-facing line: Vietnamese, with the code in it.
 *
 * The code is an identifier and never a substitute for saying what happened —
 * "[E_PRICE_GRID]" alone tells a user nothing, so `detail` carries the actual
 * figures. Detail is redacted, because this line is meant to be shared.
 *
 * @example "[LỖI] [E_PRICE_GRID] #4142 Giá offer không hợp lệ: 0.1008 WETH"
 */
function line(code, detail = "") {
  const entry = get(code);
  const level = LEVEL_LABEL[entry ? entry.severity : "ERROR"] || "LỖI";
  const message = entry ? entry.userMessage : "Không thực hiện được";
  const extra = redact(String(detail || "")).trim();
  return `[${level}] [${code}] ${extra ? extra + " " : ""}${message}`;
}

const API = {
  SEVERITY, CATEGORY, ERRORS, RETIRED,
  get, all, codes,
  classify, classifyApiError, isCredentialFailure,
  redact, line
};

// Loaded by main through require, and by the renderer as a classic script.
// One file, so the contract cannot fork.
if (typeof module !== "undefined" && module.exports) {
  module.exports = API;
} else if (typeof window !== "undefined") {
  window.OSB = window.OSB || {};
  window.OSB.errors = API;
}
