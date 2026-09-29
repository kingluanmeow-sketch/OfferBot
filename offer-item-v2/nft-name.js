"use strict";

/**
 * nft-name.js — tên NFT để HIỂN THỊ, và cách biết một cái tên chỉ là chỗ giữ.
 *
 * VÌ SAO CÓ MODULE NÀY
 *
 *   Tên được lấy đúng một lần lúc Thêm NFT rồi ghi thẳng vào hàng. Với một
 *   collection chưa reveal, nguồn metadata trả "preReveal" (hoặc "Hidden",
 *   "???", "#4286") và cái tên đó sống mãi trong bảng — sau khi collection
 *   đã reveal, OpenSea hiện "Habbo #10416" còn tool vẫn hiện "preReveal".
 *   Không có chỗ nào phân biệt "tên thật" với "chỗ giữ", nên không có chỗ nào
 *   biết là phải hỏi lại.
 *
 * LUẬT
 *
 *   1. tên thật từ metadata hiện tại;
 *   2. trống hoặc là chỗ giữ → tên collection + " #tokenId";
 *   3. không có gì → "#tokenId".
 *
 *   Chỗ giữ được nhận diện theo hình dạng, không theo một danh sách cứng:
 *   chuỗi chỉ có số/ký hiệu, chuỗi bằng đúng "#id" / "NFT #id", và các từ mà
 *   collection chưa reveal hay dùng. Thà nghi oan một cái tên lạ (rồi hỏi
 *   lại nguồn và giữ nguyên nếu nguồn nói vậy) còn hơn giữ "preReveal" mãi.
 */

// CẢ tên chỉ là một trong những từ này (có thể kèm "#id" phía sau): "preReveal",
// "Hidden #12", "Unrevealed". "Hidden Penguin" là một cái tên thật và không khớp.
const PLACEHOLDER_WORDS =
  /^(?:pre[\s_-]?reveal(?:ed)?|unreveal(?:ed)?|not\s+revealed|reveal(?:ing)?\s+soon|hidden|placeholder|mystery(?:\s+box)?|coming\s+soon|unknown|untitled|tbd|loading|n\/a)(?:\s*[#:\-]?\s*\d+)?$/i;

/** Tên này có phải chỗ giữ (chưa reveal / rỗng / chỉ là số) không? */
function isPlaceholderName(name, tokenId = "") {
  const n = String(name || "").trim();
  if (!n) return true;
  const id = String(tokenId || "").trim();
  if (id && (n === `#${id}` || n === id || n.toLowerCase() === `nft #${id}` || n.toLowerCase() === `token #${id}`)) return true;
  if (/^[#\s\d?_\-.*]+$/.test(n)) return true;          // "#4286", "???", "----"
  if (PLACEHOLDER_WORDS.test(n)) return true;
  return false;
}

/**
 * Tên để hiển thị theo luật trên.
 * @returns {{name:string, placeholder:boolean, source:"metadata"|"collection"|"tokenId"}}
 */
function pickName({ name, collectionName, tokenId } = {}) {
  const id = String(tokenId || "").trim();
  if (!isPlaceholderName(name, id)) return { name: String(name).trim(), placeholder: false, source: "metadata" };
  const col = String(collectionName || "").trim();
  if (col && !isPlaceholderName(col)) return { name: id ? `${col} #${id}` : col, placeholder: true, source: "collection" };
  return { name: id ? `#${id}` : "NFT", placeholder: true, source: "tokenId" };
}

module.exports = { isPlaceholderName, pickName, PLACEHOLDER_WORDS };
