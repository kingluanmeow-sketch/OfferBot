"use strict";

/**
 * RED/GREEN: `setRowCollectionSlug()` -- atomic collectionSlug lifecycle
 * (audit sửa lần 5, lỗi THẬT thấy trực tiếp trong source).
 *
 * LỖI: `registerRow()` gọi `addToPreToBookOpIndex()` ngay cả khi
 * `row.collectionSlug` còn rỗng (NFT mới Add trước khi slug giải được).
 * `ensureTemplate()` sau đó GÁN THẲNG `row.collectionSlug = collection.slug`
 * khi slug giải ra ở nền -- KHÔNG gỡ/thêm index nào. Hậu quả KÉP:
 *   1. `preToBookOpIndex` (diagnostic) giữ entry dưới slug rỗng/stale mãi,
 *      entry đúng dưới slug thật không bao giờ được thêm.
 *   2. `book.bySlug`/`book.collectionSlug` (MemoryBook THẬT, dùng để
 *      fan-out MỌI `collection_offer`) cũng không đồng bộ -- một NFT thêm
 *      trước khi có slug KHÔNG BAO GIỜ nhận Collection Offer của chính
 *      collection nó, dù slug sau đó giải đúng. Đây là lỗi production
 *      THẬT (ảnh hưởng Decision/Best), không chỉ lỗi diagnostic.
 *
 * SỬA: `setRowCollectionSlug(row, slug)` -- đường DUY NHẤT đổi
 * `row.collectionSlug` sau `registerRow()`, cập nhật nguyên tử CẢ
 * `preToBookOpIndex` VÀ `book.bySlug`/`book.collectionSlug`.
 */
const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}${error.stack ? "\n  " + error.stack.split("\n").slice(1, 3).join("\n  ") : ""}\n`); }
}

const CONTRACT = "0x1111111111111111111111111111111111111111";

function buildEngine() {
  const adapter = { chain: "ethereum", async fetchBest() { return new Promise(() => {}); } };
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  engine.state = STATE.RUNNING;
  return engine;
}

function indexEntryCount(engine, slug) {
  const bucket = engine.preToBookOpIndex.get(slug);
  if (!bucket) return 0;
  let n = 0;
  for (const arr of bucket.byCanon.values()) n += arr.length;
  return n;
}

(async () => {

await check("Add NFT with empty slug -> no diagnostic index entry under '' (not wasted, not stale)", async () => {
  const engine = buildEngine();
  const rowKey = engine.registerRow({ url: "", contract: CONTRACT, tokenId: "1001", collectionSlug: "", minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 }); const row = engine.rows.get(rowKey);
  assert.equal(indexEntryCount(engine, ""), 0, "registerRow with an empty slug must never create a diagnostic index entry under the empty-string key");
  assert.ok(!engine.book.bySlug.has(""), "MemoryBook.bySlug must never register an entry under the empty slug either");
});

await check("background slug resolution (empty -> real) via setRowCollectionSlug correctly adds BOTH the diagnostic index AND book.bySlug", async () => {
  const engine = buildEngine();
  const rowKey = engine.registerRow({ url: "", contract: CONTRACT, tokenId: "1002", collectionSlug: "", minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 }); const row = engine.rows.get(rowKey);
  engine.setRowCollectionSlug(row, "apuapustajas");
  assert.equal(row.collectionSlug, "apuapustajas");
  assert.equal(indexEntryCount(engine, "apuapustajas"), 1, "the diagnostic index must now have exactly one entry under the resolved slug");
  const book = engine.book.get(row.key);
  assert.equal(book.collectionSlug, "apuapustajas", "MemoryBook's own collectionSlug must be synced, not left stale -- this feeds real Collection Offer fan-out");
  assert.ok(engine.book.bySlug.get("apuapustajas").has(row.key), "book.bySlug must register this key under the resolved slug -- a Collection Offer for this slug must now reach this NFT");
});

await check("resolving the SAME slug twice does not duplicate the index entry or double the refcount", async () => {
  const engine = buildEngine();
  const rowKey = engine.registerRow({ url: "", contract: CONTRACT, tokenId: "1003", collectionSlug: "", minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 }); const row = engine.rows.get(rowKey);
  engine.setRowCollectionSlug(row, "apuapustajas");
  engine.setRowCollectionSlug(row, "apuapustajas"); // idempotent re-resolution (e.g. template refresh)
  assert.equal(indexEntryCount(engine, "apuapustajas"), 1, "resolving the same slug twice must not create a duplicate index entry");
  const bucket = engine.preToBookOpIndex.get("apuapustajas");
  assert.equal(bucket.contracts.get(CONTRACT), 1, "the contract refcount must stay at 1, not double-counted");
});

await check("resolving slug A then B cleans up A entirely -- both the diagnostic index AND book.bySlug", async () => {
  const engine = buildEngine();
  const rowKey = engine.registerRow({ url: "", contract: CONTRACT, tokenId: "1004", collectionSlug: "", minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 }); const row = engine.rows.get(rowKey);
  engine.setRowCollectionSlug(row, "slug-a");
  assert.equal(indexEntryCount(engine, "slug-a"), 1);
  engine.setRowCollectionSlug(row, "slug-b");
  assert.equal(indexEntryCount(engine, "slug-a"), 0, "the OLD slug's diagnostic index entry must be fully removed after re-resolving to a different slug");
  assert.ok(!engine.preToBookOpIndex.has("slug-a"), "an emptied slug bucket must itself be removed, not left as a dangling empty Map entry");
  assert.equal(indexEntryCount(engine, "slug-b"), 1, "the NEW slug must have exactly one entry");
  assert.ok(!engine.book.bySlug.has("slug-a") || !engine.book.bySlug.get("slug-a").has(row.key), "book.bySlug must no longer map this key under the OLD slug");
  assert.ok(engine.book.bySlug.get("slug-b").has(row.key), "book.bySlug must map this key under the NEW slug");
  assert.equal(engine.book.get(row.key).collectionSlug, "slug-b", "MemoryBook's own collectionSlug must reflect the final resolved slug");
});

await check("removeRow after a slug migration (A->B) cleans up B entirely -- no entry left under either slug", async () => {
  const engine = buildEngine();
  const rowKey = engine.registerRow({ url: "", contract: CONTRACT, tokenId: "1005", collectionSlug: "", minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 }); const row = engine.rows.get(rowKey);
  engine.setRowCollectionSlug(row, "slug-a");
  engine.setRowCollectionSlug(row, "slug-b");
  engine.removeRow(row.key);
  assert.equal(indexEntryCount(engine, "slug-a"), 0);
  assert.equal(indexEntryCount(engine, "slug-b"), 0, "removeRow after a slug migration must remove the entry under the CURRENT (migrated-to) slug, not an entry that no longer exists under the old one");
  assert.ok(!engine.preToBookOpIndex.has("slug-a") && !engine.preToBookOpIndex.has("slug-b"), "no dangling empty bucket should remain for either slug");
});

await check("Add NFT with empty slug: exact byNft match works immediately (slug-independent); the slug-fallback path only becomes available once the slug resolves", async () => {
  const engine = buildEngine();
  const rowKey = engine.registerRow({ url: "", contract: CONTRACT, tokenId: "1006", collectionSlug: "", minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 }); const row = engine.rows.get(rowKey);
  // byNft is keyed by contract+tokenId alone, set at registerRow() time
  // regardless of slug -- the exact match must already work, proving the
  // diagnostic doesn't need the slug to recognize an exactly-matching event.
  const exactBeforeSlug = engine.matchTrackedNft("apuapustajas", CONTRACT, "1006");
  assert.equal(exactBeforeSlug, "mapped", "exact byNft match must succeed even before the slug resolves -- it does not depend on collectionSlug timing");

  // The FALLBACK path (wrong contract, same tokenId) DOES depend on the
  // slug-keyed index -- before resolution, it correctly finds nothing for
  // this row (no crash, no stale/placeholder entry).
  const fallbackBeforeSlug = engine.matchTrackedNft("apuapustajas", "0x9999999999999999999999999999999999999999", "1006");
  assert.equal(fallbackBeforeSlug, null, "before slug resolution, the fallback (non-exact) path has no index entry for this row under 'apuapustajas' -- correctly null, not a crash or a stale false-positive");

  engine.setRowCollectionSlug(row, "apuapustajas");
  const fallbackAfterSlug = engine.matchTrackedNft("apuapustajas", "0x9999999999999999999999999999999999999999", "1006");
  assert.equal(fallbackAfterSlug, "unmapped-contract", "after slug resolution, the fallback path must now find this row via the diagnostic index");
});

await check("157 NFTs, mixed empty/resolved slugs at Start/reset/stop/re-add: index size equals exactly the number of rows with a VALID (non-empty) slug", async () => {
  const engine = buildEngine();
  const rows = [];
  for (let i = 0; i < 157; i++) {
    const rowKey = engine.registerRow({ url: "", contract: CONTRACT, tokenId: String(80000 + i), collectionSlug: i % 10 === 0 ? "" : "bigcollection", minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 }); const row = engine.rows.get(rowKey);
    rows.push(row);
  }
  const expectedWithSlug = rows.filter(r => r.collectionSlug).length;
  let total = 0;
  for (const bucket of engine.preToBookOpIndex.values()) for (const arr of bucket.byCanon.values()) total += arr.length;
  assert.equal(total, expectedWithSlug, `index size must equal exactly the number of rows with a non-empty slug (${expectedWithSlug}), got ${total}`);

  // Now resolve the remaining empty-slug rows in the background.
  for (const row of rows) if (!row.collectionSlug) engine.setRowCollectionSlug(row, "bigcollection");
  total = 0;
  for (const bucket of engine.preToBookOpIndex.values()) for (const arr of bucket.byCanon.values()) total += arr.length;
  assert.equal(total, 157, "after all slugs resolve, the index must contain exactly 157 entries");

  // Remove a handful, then re-add -- no stale growth.
  for (let i = 0; i < 10; i++) engine.removeRow(rows[i].key);
  total = 0;
  for (const bucket of engine.preToBookOpIndex.values()) for (const arr of bucket.byCanon.values()) total += arr.length;
  assert.equal(total, 147, "after removing 10 rows, the index must shrink to exactly 147");

  for (let i = 0; i < 10; i++) engine.registerRow({ url: "", contract: CONTRACT, tokenId: String(80000 + i), collectionSlug: "bigcollection", minPrice: 0.001, maxPrice: 1, step: 0.0001, duration: 15 });
  total = 0;
  for (const bucket of engine.preToBookOpIndex.values()) for (const arr of bucket.byCanon.values()) total += arr.length;
  assert.equal(total, 157, "re-adding the removed 10 rows must restore exactly 157, no leak/duplicate");

  // Simulate a Start-reload: this.rows.clear() + preToBookOpIndex.clear().
  engine.rows.clear();
  engine.preToBookOpIndex.clear();
  assert.equal(engine.preToBookOpIndex.size, 0, "a Start-reload must leave zero stale entries");
});

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exitCode = failed ? 1 : 0;

})();
