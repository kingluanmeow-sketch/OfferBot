"use strict";

/**
 * nft-source.js — dữ liệu NFT (tên, ảnh, collection, trait) từ OpenSea.
 *
 * VÌ SAO KHÔNG CÒN ALCHEMY
 *
 *   Alchemy từng được đưa vào để đọc một trăm NFT trong một request cho Offer
 *   Custom. Offer Custom đã bị gỡ; ba chỗ còn lại dùng nó (thêm NFT, làm mới
 *   tên, bản chụp trait của Offer Item) đều đã có đường OpenSea dự phòng và
 *   đều chạy ở NỀN — không nằm trên đường nóng quyết định/gửi. Một nhà cung
 *   cấp thứ hai, một API key thứ hai, một key mặc định phải giấu trong gói:
 *   tất cả chỉ để tiết kiệm vài request nền. Bỏ.
 *
 *   Hình dạng trả về giữ đúng như `alchemy-client.getNFTs` để TraitSnapshot,
 *   cầu nối và đường thêm NFT không phải đổi cách đọc.
 */

const opensea = require("./opensea");

/** Bao nhiêu token đọc song song. Bộ giới hạn của opensea.js vẫn là trần thật. */
const PARALLEL = 4;

/** Ảnh IPFS/Arweave về dạng https, giữ nguyên các URL khác. */
function normalizeImageUrl(raw) {
  const text = String(raw === undefined || raw === null ? "" : raw).trim();
  if (!text) return "";
  if (text.startsWith("ipfs://")) {
    const cid = text.slice("ipfs://".length).replace(/^ipfs\//, "");
    return cid ? `https://ipfs.io/ipfs/${cid}` : "";
  }
  if (text.startsWith("ar://")) {
    const id = text.slice("ar://".length);
    return id ? `https://arweave.net/${id}` : "";
  }
  return text;
}

/** Trait của một NFT ở dạng { type, value }, đọc từ metadata OpenSea. */
function readAttributes(nft) {
  const list = Array.isArray(nft && nft.attributes) ? nft.attributes
    : Array.isArray(nft && nft.traits) ? nft.traits : [];
  return list
    .map(entry => ({
      type: String((entry && (entry.trait_type ?? entry.traitType ?? entry.type)) || ""),
      value: String((entry && entry.value) ?? "")
    }))
    .filter(entry => entry.type && entry.value);
}

/**
 * Đọc nhiều NFT. Trả `{ ok, nfts }` với `nfts[i]` ứng với `items[i]` (null khi
 * không đọc được token đó). Chạy song song có trần; mỗi token một request
 * metadata OpenSea (có cache; `refresh` bỏ cache).
 */
async function getNFTs(chain, items, { refresh = false } = {}) {
  const list = Array.isArray(items) ? items : [];
  const nfts = new Array(list.length).fill(null);
  let failed = 0;
  let cursor = 0;
  const worker = async () => {
    for (;;) {
      const at = cursor++;
      if (at >= list.length) return;
      const it = list[at] || {};
      try {
        const meta = await opensea.fetchNftMeta(chain, it.contract, it.tokenId, { force: refresh });
        if (!meta) { failed++; continue; }
        nfts[at] = {
          chain,
          contract: String(it.contract || "").toLowerCase(),
          tokenId: String(it.tokenId ?? ""),
          name: String(meta.name || "").trim(),
          image: normalizeImageUrl(meta.image),
          imageAlts: (Array.isArray(meta.imageAlts) ? meta.imageAlts : []).map(normalizeImageUrl).filter(Boolean),
          collectionName: String(meta.collectionName || "").trim(),
          openseaSlug: String(meta.collectionSlug || "").trim(),
          attributes: readAttributes({ traits: meta.traits }),
          tokenStandard: meta.tokenStandard || ""
        };
      } catch {
        failed++;
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(PARALLEL, list.length || 1) }, worker));
  return { ok: true, nfts, failed, source: "opensea" };
}

module.exports = { getNFTs, readAttributes, normalizeImageUrl };
