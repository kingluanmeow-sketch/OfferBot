"use strict";

/**
 * standard-store.js — chuẩn token (ERC721/ERC1155) theo CONTRACT, nhớ qua lần chạy.
 *
 * VÌ SAO CÓ FILE NÀY
 *
 *   Chuẩn token là thuộc tính TĨNH của contract: hỏi một lần là biết mãi. Bản
 *   trước hỏi lại ERC165 mỗi lần Start (và mỗi lần dựng lại template), nên một
 *   contract mà ERC165 trả lời chậm/không trả lời làm chậm Start và, tệ hơn,
 *   chặn template của mọi NFT thuộc contract đó — đo trên #782.
 *
 *   Chỉ nhớ câu trả lời CHẮC CHẮN. "Chưa biết" không bao giờ được cất vào đây:
 *   một lần mạng hỏng không được biến thành một kết luận vĩnh viễn.
 *
 * GHI ĐĨA LÀ TUỲ CHỌN
 *
 *   `configure(dir)` bật phần ghi đĩa (main.js trỏ vào userData). Không gọi thì
 *   store vẫn chạy hoàn toàn trong bộ nhớ — bài kiểm và tiến trình phụ không
 *   phải dựng một thư mục chỉ để đọc một chuỗi.
 */

const fs = require("fs");
const path = require("path");

/** Đổi khi hình dạng bản ghi đổi; bản cũ bị bỏ, không cố đọc. */
const SCHEMA = 1;
/** Trần bản ghi: một máy theo dõi vài trăm contract là nhiều. */
const MAX_ENTRIES = 2000;

const memory = new Map();          // "chain:contract" -> { standard, source, at }
let file = "";
let dirty = false;
let writeTimer = null;

function valid(standard) {
  return standard === "erc721" || standard === "erc1155";
}

function configure(directory) {
  if (!directory) return { ok: false };
  file = path.join(directory, "token-standards.json");
  try {
    const raw = JSON.parse(fs.readFileSync(file, "utf8"));
    if (raw && raw.schema === SCHEMA && raw.entries && typeof raw.entries === "object") {
      for (const [key, value] of Object.entries(raw.entries)) {
        if (value && valid(value.standard)) memory.set(key, value);
      }
    }
  } catch { /* chưa có file, hoặc file hỏng: bắt đầu trống */ }
  return { ok: true, loaded: memory.size, file };
}

function get(key) {
  const hit = memory.get(key);
  return hit && valid(hit.standard) ? hit.standard : "";
}

function set(key, standard, source = "") {
  if (!key || !valid(standard)) return false;     // "chưa biết" không được nhớ
  const before = memory.get(key);
  if (before && before.standard === standard) return false;
  memory.set(key, { standard, source: String(source || ""), at: Date.now() });
  if (memory.size > MAX_ENTRIES) {
    const oldest = [...memory.entries()].sort((a, b) => (a[1].at || 0) - (b[1].at || 0));
    for (const [k] of oldest.slice(0, memory.size - MAX_ENTRIES)) memory.delete(k);
  }
  scheduleWrite();
  return true;
}

/** Ghi gộp: nhiều contract được biết trong cùng một lượt Start là một lần ghi. */
function scheduleWrite() {
  dirty = true;
  if (!file || writeTimer) return;
  writeTimer = setTimeout(() => { writeTimer = null; flush(); }, 1000);
  if (writeTimer.unref) writeTimer.unref();
}

function flush() {
  if (!file || !dirty) return false;
  dirty = false;
  try {
    const entries = Object.fromEntries(memory);
    fs.writeFileSync(file, JSON.stringify({ schema: SCHEMA, entries }, null, 0));
    return true;
  } catch {
    return false;
  }
}

function census() { return { entries: memory.size, file, schema: SCHEMA }; }

/** Chỉ cho bài kiểm: quên hết. */
function _reset() { memory.clear(); file = ""; dirty = false; if (writeTimer) clearTimeout(writeTimer); writeTimer = null; }

module.exports = { configure, get, set, flush, census, SCHEMA, _reset };
