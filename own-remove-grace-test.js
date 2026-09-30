"use strict";

/**
 * RED/GREEN: `order_invalidate` (soft REMOVE) không được xoá own ngay.
 *
 * Live 2026-09-30 (449 REMOVE trúng own-hash trong 10 phút, xác minh bằng
 * instrumentation trên tiến trình thật): 100% (67/67, còn lại không phải
 * REMOVE) REMOVE trúng đúng own-hash VỪA POST là `order_invalidate`, KHÔNG
 * có `item_cancelled` hay `item_sold` nào. `order_invalidate` tự nhận nó
 * "có thể quay lại" (event-normalizer.js đặt `soft: eventName ===
 * "order_invalidate"`, và code gốc vốn đã không tombstone nó vì lý do đó) —
 * nhưng vẫn xoá `own` NGAY, làm sổ tưởng mất offer và gửi lại NGUYÊN GIÁ VỪA
 * GỬI (2 order thật cùng giá). Xem offer-item-v2/memory-book.js apply().
 *
 * 2 phương án trước (grace-window cố định theo thời gian, rồi REST-verify)
 * đều bị revert vì phá offer-item-v2-ab-selfcancel-test.js — path A/B
 * own-cancel THẬT (dùng `item_cancelled`, không soft) cần phản ứng NGAY.
 * `op.soft` là tín hiệu có sẵn, xác nhận bằng bằng chứng live, không thêm
 * REST/grace nào vào hot path — fix chỉ là một nhánh if trên field đã có.
 */

const assert = require("assert");
const { TokenBook } = require("./offer-item-v2/memory-book");
const { OP, SCOPE_KIND } = require("./offer-item-v2/event-normalizer");

let passed = 0, failed = 0;
function check(name, ok, detail = "") {
  if (ok) { passed++; process.stdout.write(`PASS ${name}\n`); }
  else { failed++; process.stdout.write(`FAIL ${name}${detail ? `: ${detail}` : ""}\n`); }
}

function makeBook() {
  const book = new TokenBook("ethereum:0xabc:1", { chain: "ethereum", contract: "0xabc", tokenId: "1" });
  book.selfAddress = "0xself";
  return book;
}

function ownEntry(hash, price, at) {
  return { orderHash: hash, price, maker: "0xself", kind: "item",
    quantity: 1, currency: "WETH", endTime: Math.floor(at / 1000) + 900, assumedEnd: false, seq: 0, at };
}

// ---- Case 1: order_invalidate (soft) on our own just-posted order --------
{
  const book = makeBook();
  const t0 = 1_000_000;
  const hash = "0x7fe6b92a2230"; // matches the live-captured incident's hash shape
  book.own.set(hash, ownEntry(hash, 0.0697, t0));

  const touched = book.apply({ op: OP.REMOVE, orderHash: hash, scope: hash, seq: 1, soft: true }, t0 + 385);

  check("order_invalidate (soft) does NOT delete a just-posted own order",
    book.own.has(hash), `own.has(hash)=${book.own.has(hash)}`);
  check("order_invalidate (soft) does NOT tombstone it either",
    !book.isTombstoned(hash), `tombstoned=${book.isTombstoned(hash)}`);
  check("ownBest still reflects the just-posted price (no false SEND)",
    book.ownBest(t0 + 400).price === 0.0697, `mine=${book.ownBest(t0 + 400).price}`);
  check("apply() reports NOT touched for own-only soft REMOVE (no spurious re-eval)",
    touched === false, `touched=${touched}`);
}

// ---- Case 2: item_cancelled (hard, real self-cancel) still removes own NOW
{
  const book = makeBook();
  const t0 = 2_000_000;
  const hash = "0xreal-cancel";
  book.own.set(hash, ownEntry(hash, 0.05, t0));

  const touched = book.apply({ op: OP.REMOVE, orderHash: hash, scope: hash, seq: 1, soft: false }, t0 + 385);

  check("item_cancelled (hard) still deletes own immediately",
    !book.own.has(hash), `own.has(hash)=${book.own.has(hash)}`);
  check("item_cancelled (hard) still tombstones immediately",
    book.isTombstoned(hash), `tombstoned=${book.isTombstoned(hash)}`);
  check("apply() reports touched=true (row must re-evaluate now)",
    touched === true, `touched=${touched}`);
}

// ---- Case 3: item_sold (hard, matched) still removes own NOW -------------
{
  const book = makeBook();
  const t0 = 3_000_000;
  const hash = "0xsold";
  book.own.set(hash, ownEntry(hash, 0.05, t0));
  const touched = book.apply({ op: OP.REMOVE, orderHash: hash, scope: hash, seq: 1, soft: false }, t0 + 50);
  check("item_sold (hard) still deletes own immediately",
    !book.own.has(hash), `own.has(hash)=${book.own.has(hash)}`);
}

// ---- Case 4: order_invalidate on item/collection/trait is UNCHANGED ------
{
  const book = makeBook();
  const t0 = 4_000_000;
  const hash = "0xrival-item";
  book.apply({ op: OP.UPSERT, orderHash: hash, scope: hash, seq: 1, kind: SCOPE_KIND.ITEM,
    price: 0.06, maker: "0xrival", quantity: 1, currency: "WETH", endTime: Math.floor(t0 / 1000) + 900 }, t0);
  const hadItem = book.item.has(hash);
  const changed = book.apply({ op: OP.REMOVE, orderHash: hash, scope: hash, seq: 2, soft: true }, t0 + 10);
  check("order_invalidate on a rival item order still removes it now (own-only protection)",
    hadItem && !book.item.has(hash), `hadItem=${hadItem} item.has=${book.item.has(hash)}`);
  check("order_invalidate (soft) does not tombstone a rival item order (unchanged pre-existing behavior)",
    !book.isTombstoned(hash), `tombstoned=${book.isTombstoned(hash)}`);
  check("changed=true for the item-group removal", changed === true, `changed=${changed}`);
}

// ---- Case 5: a LATER order_invalidate for a hash no longer in own is a no-op
{
  const book = makeBook();
  const t0 = 5_000_000;
  const hash = "0xno-longer-own";
  const changed = book.apply({ op: OP.REMOVE, orderHash: hash, scope: hash, seq: 1, soft: true }, t0);
  check("order_invalidate for an untracked hash is a safe no-op",
    changed === false, `changed=${changed}`);
}

process.stdout.write(`\n${passed}/${passed + failed} checks passed\n`);
if (failed) process.exitCode = 1;
