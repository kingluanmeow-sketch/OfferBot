"use strict";

/**
 * trait-snapshot.js — trait của từng token Offer Item đang theo dõi.
 *
 * VÌ SAO V2 TỰ GIỮ, KHÔNG SỬA DB CHUNG
 *
 *   `db.js` không lưu trait cho hàng Offer Item, và sửa nó nghĩa là đụng vào
 *   một module mà Offer SLL, Offer Custom và Cancel cùng dựa vào. Cái V2 cần
 *   chỉ là: với mỗi token đang theo dõi, nó mang trait gì, biết từ đâu, và
 *   biết lúc nào. Một bản chụp trong phạm vi Offer Item là đủ, và không làm
 *   ai khác đổi hành vi.
 *
 * NGUỒN
 *
 *   `nft-source.getNFTs` — đọc theo lô, có cache riêng, đã được phần
 *   còn lại của app dùng để hiện ảnh và tên. Cùng một nguồn, cùng một hình
 *   dạng trait ({type, value}), không có bản dịch thứ hai để trôi.
 *
 * KHI NÀO ĐỌC
 *
 *   Lúc Start, ở nền, theo lô — không đợi tới khi một trait offer xuất hiện
 *   mới đi hỏi từng token. Sau khi nạp xong, một trait offer được quyết định
 *   hoàn toàn cục bộ: REST = 0 trên đường nóng. Token nào nạp hỏng thì ở
 *   trạng thái CHƯA BIẾT, và engine có đường riêng cho chưa-biết.
 *
 * MỘT TOKEN KHÔNG CÓ TRAIT NÀO ≠ MỘT TOKEN CHƯA BIẾT
 *
 *   Nguồn trả về danh sách rỗng là một câu trả lời thật: token này không mang
 *   trait nào, và mọi trait offer đều không áp. Nguồn không trả lời là một câu
 *   hỏi còn mở. Hai thứ được lưu khác nhau và trả lời khác nhau.
 */

const { tokenTraits } = require("./trait-criteria");

/** Bao nhiêu token hỏi trong một lượt (nguồn đọc song song có trần bên trong). */
const BATCH = 100;

class TraitSnapshot {
  /**
   * @param {object} o
   * @param {(chain:string, items:Array<{contract:string,tokenId:string}>) => Promise<object>} o.getNFTs
   *   giống `nft-source.getNFTs`; tiêm vào để test không chạm mạng
   * @param {(nft:object) => Array<{type:string,value:string}>} o.readAttributes
   * @param {(line:string)=>void} [o.onLog]
   */
  constructor({ getNFTs, readAttributes, onLog } = {}) {
    this.getNFTs = getNFTs;
    this.readAttributes = readAttributes;
    this.onLog = typeof onLog === "function" ? onLog : () => {};

    /** tokenKey -> { known, keys, version, hydratedAt, source, tokenId, contract } */
    this.entries = new Map();
    this.version = 0;
    this.stats = { hydrated: 0, empty: 0, failed: 0, batches: 0 };
  }

  /** Trait của một token, ở dạng `appliesTo` đọc được. Chưa biết → known:false. */
  get(tokenKey) {
    const e = this.entries.get(tokenKey);
    if (!e) return { known: false, keys: new Set(), numericValues: new Map(), duplicateNumericTypes: new Set() };
    return { known: e.known, keys: e.keys, numericValues: e.numericValues,
      duplicateNumericTypes: e.duplicateNumericTypes };
  }

  has(tokenKey) {
    const e = this.entries.get(tokenKey);
    return Boolean(e && e.known);
  }

  /** Quên những token không còn theo dõi. Có trần theo đúng danh sách hàng. */
  retain(tokenKeys) {
    const keep = new Set(tokenKeys);
    for (const key of [...this.entries.keys()]) {
      if (!keep.has(key)) this.entries.delete(key);
    }
  }

  /**
   * Nạp trait cho một danh sách hàng, theo lô, tuần tự — bounded by design.
   *
   * @param {string} chain
   * @param {Array<{key:string, contract:string, tokenId:string}>} rows
   * @param {{signal?:AbortSignal, onBatch?:(done:number,total:number)=>void}} [opts]
   * @returns {Promise<{hydrated:number, failed:number, empty:number, batches:number}>}
   */
  async hydrate(chain, rows, { signal, onBatch } = {}) {
    if (typeof this.getNFTs !== "function") {
      return { hydrated: 0, failed: rows.length, empty: 0, batches: 0, disabled: true };
    }
    const out = { hydrated: 0, failed: 0, empty: 0, batches: 0 };
    const pending = rows.filter(r => !this.has(r.key));

    for (let i = 0; i < pending.length; i += BATCH) {
      if (signal && signal.aborted) break;
      const slice = pending.slice(i, i + BATCH);
      out.batches++;
      let answer;
      try {
        answer = await this.getNFTs(chain, slice.map(r => ({
          contract: r.contract, tokenId: r.tokenId
        })));
      } catch (error) {
        out.failed += slice.length;
        this.onLog(`trait snapshot: lô ${out.batches} lỗi: ${String(error.message).slice(0, 100)}`);
        continue;
      }
      if (!answer || answer.ok === false) {
        out.failed += slice.length;
        this.onLog(`trait snapshot: lô ${out.batches} không đọc được: ${answer && answer.reason || "?"}`);
        continue;
      }

      const nfts = Array.isArray(answer.nfts) ? answer.nfts : [];
      for (let j = 0; j < slice.length; j++) {
        const row = slice[j];
        const nft = nfts[j];
        if (!nft) { out.failed++; continue; }   // nguồn không trả lời token này
        const list = this.readAttributes ? this.readAttributes(nft) : [];
        const traits = tokenTraits(list);
        this.version++;
        this.entries.set(row.key, {
          known: true, keys: traits.keys, numericValues: traits.numericValues,
          duplicateNumericTypes: traits.duplicateNumericTypes, version: this.version,
          hydratedAt: Date.now(), source: "opensea",
          contract: row.contract, tokenId: row.tokenId
        });
        if (traits.keys.size === 0) out.empty++;
        out.hydrated++;
      }
      if (onBatch) onBatch(Math.min(i + BATCH, pending.length), pending.length);
    }

    this.stats.hydrated += out.hydrated;
    this.stats.failed += out.failed;
    this.stats.empty += out.empty;
    this.stats.batches += out.batches;
    return out;
  }

  census() {
    let known = 0;
    for (const e of this.entries.values()) if (e.known) known++;
    return { tokens: this.entries.size, known, version: this.version, ...this.stats };
  }
}

module.exports = { TraitSnapshot, BATCH };
