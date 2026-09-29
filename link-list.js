"use strict";

/**
 * link-list.js — một ô dán nhiều link thành một danh sách link.
 *
 * VÌ SAO KHÔNG CẮT THEO DẤU PHẨY
 *
 *   Link thật của OpenSea mang query string, và query string của OpenSea dùng
 *   dấu phẩy:
 *
 *     https://opensea.io/item/ethereum/0x…/306?activityTypes=sale,mint,offer
 *
 *   Cắt theo `[\s,]+` biến MỘT link thành ba mẩu: link thật, `mint`, `offer`.
 *   Người dùng thấy "Thêm 1 NFT" kèm hai dòng "Link không hợp lệ: mint /
 *   offer" — đúng thứ đã xảy ra với #306.
 *
 *   Nên phân tách chỉ theo XUỐNG DÒNG và KHOẢNG TRẮNG. Dấu phẩy chỉ là dấu
 *   phân tách khi nó KHÔNG nằm trong một URL — nghĩa là khi mẩu phía sau nó
 *   trông như bắt đầu một link mới. Hai cách viết mà người dùng thật sự dùng
 *   ("mỗi dòng một link", "dán một link có query") đều ra đúng kết quả.
 */

/** Một mẩu có trông như bắt đầu một link/đường dẫn OpenSea không? */
function looksLikeStart(text) {
  const t = String(text || "").trim();
  if (!t) return false;
  return /^(https?:\/\/|www\.|opensea\.io\/|testnets\.opensea\.io\/|item\/|assets\/)/i.test(t);
}

/**
 * Tách ô nhập thành danh sách link.
 *
 * @param {string} raw
 * @returns {string[]} đã trim, bỏ rỗng, giữ nguyên thứ tự
 */
function splitLinks(raw) {
  const text = String(raw === undefined || raw === null ? "" : raw);
  const out = [];
  for (const line of text.split(/[\r\n]+/)) {
    for (const chunk of line.split(/\s+/)) {
      const piece = chunk.trim();
      if (!piece) continue;
      // Dấu phẩy: chỉ cắt khi phần sau nó mở đầu một link mới. "…?a=sale,mint"
      // giữ nguyên; "link1,link2" thành hai link.
      if (piece.includes(",")) {
        const parts = piece.split(",");
        let current = parts[0];
        for (let i = 1; i < parts.length; i++) {
          if (looksLikeStart(parts[i])) {
            if (current.trim()) out.push(current.trim());
            current = parts[i];
          } else {
            current += "," + parts[i];
          }
        }
        if (current.trim()) out.push(current.trim());
        continue;
      }
      out.push(piece);
    }
  }
  return out;
}

/** Cùng một danh sách, đã bỏ trùng, giữ thứ tự xuất hiện. */
function uniqueLinks(raw) {
  const seen = new Set();
  const out = [];
  for (const link of splitLinks(raw)) {
    const key = link.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(link);
  }
  return out;
}

module.exports = { splitLinks, uniqueLinks, looksLikeStart };
