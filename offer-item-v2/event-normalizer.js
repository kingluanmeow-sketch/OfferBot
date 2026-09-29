"use strict";

/**
 * event-normalizer.js — một frame Stream thành một sự kiện sổ lệnh.
 *
 * VÌ SAO ĐÂY LÀ MODULE RIÊNG
 *
 *   `stream.js` đã chuẩn hoá frame ở tầng vận chuyển, và V2 dùng lại kết quả
 *   đó chứ không parse lại — parse hai lần là hai định nghĩa "giá là gì", và
 *   chúng sẽ lệch nhau. Cái module này làm việc khác: nó biến một sự kiện đã
 *   parse thành một THAO TÁC LÊN SỔ, và gắn cho nó thứ tự.
 *
 * THỨ TỰ MỚI LÀ VẤN ĐỀ, KHÔNG PHẢI PARSE
 *
 *   OpenSea Stream không hứa giao đúng thứ tự. Ba hệ quả cụ thể, và cả ba đều
 *   là tiền:
 *
 *     - `item_cancelled` tới trước `item_received_bid` của cùng order thì
 *       lệnh đã huỷ SỐNG LẠI trong sổ, và bot vượt giá một lệnh không tồn tại.
 *     - Một bid cũ tới sau bid mới của cùng order sẽ KÉO best về giá cũ.
 *     - Cùng một frame giao hai lần thì sổ đếm hai lệnh.
 *
 *   Nên mỗi thao tác mang một `seq` (thứ tự trong phạm vi của chính nó) và
 *   `scope` (phạm vi đó là gì). So `seq` giữa hai scope khác nhau là vô nghĩa
 *   và bị cấm ở đây — hai order khác nhau không có thứ tự chung.
 *
 * PHẠM VI SO SÁNH
 *
 *   Scope là ORDER HASH khi có. Đó là đơn vị duy nhất mà "cũ hơn / mới hơn"
 *   có nghĩa: cùng một order, hai trạng thái. Không có order hash thì sự kiện
 *   không so được với gì cả và luôn được coi là mới — thà xử lý thừa một lần
 *   còn hơn bỏ qua một lệnh thật.
 */

/** Thao tác lên sổ. */
const { parseCriteria } = require("./trait-criteria");

const OP = Object.freeze({
  UPSERT: "UPSERT",     // thêm hoặc cập nhật một lệnh
  REMOVE: "REMOVE",     // huỷ / hết hạn / đã khớp
  // An order that is valid again (1.25.14). The event carries no price, so it
  // cannot be applied to the book: it asks for a targeted read of its token.
  REVALIDATE: "REVALIDATE",
  IGNORE: "IGNORE"      // không liên quan tới sổ
});

/** Sự kiện nào làm gì. */
const EVENT_OP = Object.freeze({
  item_received_bid: OP.UPSERT,
  item_received_offer: OP.UPSERT,
  collection_offer: OP.UPSERT,
  trait_offer: OP.UPSERT,
  item_cancelled: OP.REMOVE,
  item_sold: OP.REMOVE,
  // 1.25.14: OpenSea's order_invalidate (balance/approval gone) takes the order
  // out of the book WITHOUT a tombstone - it may come back (order_revalidate).
  order_invalidate: OP.REMOVE,
  order_revalidate: OP.REVALIDATE,
  item_listed: OP.IGNORE,
  item_transferred: OP.IGNORE,
  item_metadata_updated: OP.IGNORE
});

/**
 * Sự kiện nào áp cho NFT nào.
 *
 *   item        đúng một token
 *   collection  mọi token của collection
 *   trait       mọi token mang đúng trait đó
 *
 * Ba loại này KHÔNG thể gộp: một offer theo collection áp cho NFT đang theo
 * dõi mà không hề nhắc tên nó, còn một offer theo trait chỉ áp khi token thật
 * sự mang trait ấy. Gộp lại thành "offer" là cách best bị tính sai theo cả hai
 * chiều — vượt giá thứ không áp cho mình, và bỏ qua thứ có áp.
 */
const SCOPE_KIND = Object.freeze({
  ITEM: "item",
  COLLECTION: "collection",
  TRAIT: "trait"
});

const lower = v => String(v == null ? "" : v).trim().toLowerCase();

/** Khoá định danh một NFT trong toàn bộ V2. */
function tokenKey(chain, contract, tokenId) {
  return `${lower(chain)}:${lower(contract)}:${String(tokenId)}`;
}

/**
 * Trait mà một `trait_offer` nhắm tới, nếu đọc được.
 *
 * Trả `null` khi không đọc được — và `null` ở đây KHÔNG có nghĩa "áp cho mọi
 * token". Nó có nghĩa "không biết", và MemoryBook từ chối áp một offer không
 * biết nhắm vào đâu. Coi không-biết là áp-tất-cả sẽ vượt giá hàng loạt NFT vì
 * một offer chỉ nhắm một trait duy nhất.
 */
function readTraitCriteria(raw) {
  const c = raw && (raw.trait_criteria || raw.traitCriteria ||
    (raw.payload && (raw.payload.trait_criteria || raw.payload.traitCriteria)));
  if (!c) return null;
  const type = String(c.trait_type ?? c.traitType ?? "").trim();
  const value = String(c.trait_name ?? c.traitName ?? c.value ?? "").trim();
  if (!type || !value) return null;
  return { type, value };
}

/**
 * Criteria đầy đủ của một trait offer — một hoặc nhiều trait.
 *
 *   Đọc từ `traitCriteriaList` (stream.js mang nguyên danh sách qua), rơi về
 *   `traitCriteria` một-trait, rồi về payload thô. Ngữ nghĩa nhiều-trait là
 *   AND, đo trên order thật — xem trait-criteria.js.
 */
function readCriteria(e) {
  const raw = e.raw || e;
  return parseCriteria(e.traitCriteriaList || raw.traitCriteriaList ||
    (e.traitCriteria ? { trait_criteria: e.traitCriteria } : null) ||
    raw.payload || raw) ||
    (e.traitCriteria ? parseCriteria({ trait_criteria: e.traitCriteria }) : null);
}

/**
 * Một sự kiện đã chuẩn hoá (từ stream.js) thành một thao tác sổ.
 *
 * @param {object} e sự kiện của stream.js
 * @returns {object|null} null khi không dùng được cho sổ
 */
function toBookOp(e) {
  if (!e || typeof e !== "object") return null;

  const eventName = String(e.event || "");
  const op = EVENT_OP[eventName];
  if (!op || op === OP.IGNORE) return null;

  const orderHash = e.orderHash ? String(e.orderHash) : null;

  // Giá THEO MỖI ITEM, không phải giá cả order.
  //
  // Một offer ERC-1155 cho 5 bản với tổng 0.5 WETH không phải là offer 0.5 cho
  // token này. stream.js đã chia sẵn; chỗ này chỉ từ chối những gì không dùng
  // được để so sánh.
  const price = Number(e.pricePerItem);
  const usablePrice = Number.isFinite(price) && price > 0 ? price : 0;
  if (op === OP.UPSERT && !usablePrice) return null;

  const kind = e.kind === "collection"
    ? SCOPE_KIND.COLLECTION
    : e.kind === "trait"
      ? SCOPE_KIND.TRAIT
      : SCOPE_KIND.ITEM;

  // Offer theo trait mà không đọc được criteria thì không áp cho token nào.
  // Nhiều trait KHÔNG còn bị từ chối: chúng là AND, đo trên order thật.
  const criteria = kind === SCOPE_KIND.TRAIT ? readCriteria(e) : null;
  if (kind === SCOPE_KIND.TRAIT && !criteria && op === OP.UPSERT) return null;
  // `trait` giữ cho chỗ đọc cũ: một-trait thì là điều kiện duy nhất.
  const trait = criteria && criteria.conditions.length === 1
    ? { type: criteria.conditions[0].type, value: criteria.conditions[0].value }
    : (kind === SCOPE_KIND.TRAIT ? readTraitCriteria(e.raw || e) : null);

  return {
    op,
    kind,
    eventName,
    orderHash,

    // Phạm vi so sánh thứ tự. Order hash khi có; nếu không thì một chuỗi
    // không bao giờ trùng, để sự kiện đó không bị coi là cũ so với bất kỳ cái
    // gì khác.
    scope: orderHash || `nohash:${eventName}:${e.receivedAt || 0}:${Math.random()}`,

    // Thứ tự TRONG scope. Thời điểm OpenSea nói sự kiện xảy ra; 0 nghĩa là
    // payload không nói, và khi đó thời điểm nhận được dùng thay — kém chính
    // xác nhưng vẫn đơn điệu theo chiều tới.
    seq: Number(e.eventTimestamp) > 0
      ? Number(e.eventTimestamp)
      : Number(e.receivedAt) || Date.now(),
    seqFromWire: Number(e.eventTimestamp) > 0,
    // OpenSea's per-order revision counter (1.25.14): an event carrying a LOWER
    // version than one already seen for the same order is older state.
    version: Number(e.version) > 0 ? Number(e.version) : 0,

    collectionSlug: lower(e.collectionSlug),
    contract: e.nft ? lower(e.nft.contract) : "",
    tokenId: e.nft ? String(e.nft.tokenId) : "",
    chain: e.nft ? lower(e.nft.chain) : "",

    trait,
    criteria,
    maker: lower(e.maker),
    price: usablePrice,
    quantity: Number(e.quantity) || 1,
    currency: String(e.currency || ""),
    endTime: Number(e.endTime) || 0,

    receivedAt: Number(e.receivedAt) || Date.now(),
    hasOrderData: Boolean(e.hasOrderData),
    // No tombstone for an invalidation: a later revalidation must be able to
    // bring the same order hash back.
    soft: eventName === "order_invalidate"
  };
}

module.exports = { toBookOp, tokenKey, OP, SCOPE_KIND, EVENT_OP, readTraitCriteria };
