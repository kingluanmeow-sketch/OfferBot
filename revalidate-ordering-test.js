"use strict";

/**
 * ORDER_REVALIDATE: hai lỗ đã chứng minh (audit 2026-10-04)
 *
 * `order_revalidate` là event duy nhất KHÔNG đi qua `MemoryBook.apply()`:
 * engine-v2 chặn nó ngay ở `handleStreamEvent` (`if (op.op === OP.REVALIDATE)
 * { this.onOrderRevalidate(...); return; }`) vì payload không mang giá — nó
 * chỉ có thể xin một lượt đọc targeted. Vì đi đường riêng, nó bỏ mất hai thứ
 * mà mọi op khác được hưởng:
 *
 *   A. GHI NỢ KHI ĐANG CÓ LƯỢT ĐỌC BAY. `onOrderRevalidate` trả 0 khi
 *      `pendingReads` đang bận. Một lượt đọc bắt đầu TRƯỚC khi revalidate tới
 *      trả về ảnh chụp chưa có order vừa sống lại, và V2 không có nhịp quét
 *      nào hỏi lại ⇒ mất vĩnh viễn. Đường trait đã vá đúng lỗi này từ trước
 *      bằng `rereadAfter` (xem `resolveTraitScope`), đường revalidate thì
 *      chưa.
 *
 *   B. MỐC THỨ TỰ. Không gì ghi lại revision của revalidate, nên một
 *      `order_invalidate` tới muộn mang revision NHỎ HƠN vẫn được áp và xoá
 *      order mà OpenSea vừa nói là còn hiệu lực ⇒ Best tụt dưới thật, ta trả
 *      thấp và bị vượt.
 *
 * Cả hai sửa bằng cơ chế đã có, không thêm timer/polling/sweep, không đổi
 * priority (`authoritative: false` giữ nguyên), không chạm Stream→POST.
 */

const assert = require("node:assert/strict");
const { OfferItemEngineV2, STATE } = require("./offer-item-v2/engine-v2");

let passed = 0, failed = 0;
async function check(name, fn) {
  try { await fn(); passed++; process.stdout.write(`PASS ${name}\n`); }
  catch (error) { failed++; process.stdout.write(`FAIL ${name}: ${error.message}\n`); }
}
const flush = async (n = 6) => { for (let i = 0; i < n; i++) await new Promise(r => setImmediate(r)); };

const RIVAL = "0x3333333333333333333333333333333333333333";
const SELF = "0x4444444444444444444444444444444444444444";
const CONTRACT = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SLUG = "revalidate-audit-collection";
const HASH = "0xf00d";

function buildEngine() {
  const resolvers = [];
  const rejecters = [];
  const adapter = {
    chain: "ethereum", openseaChain: "ethereum",
    toWei: x => String(Math.round(Number(x) * 1e18)),
    offerBody: () => ({}),
    async fetchBest() {
      return new Promise((res, rej) => { resolvers.push(res); rejecters.push(rej); });
    }
  };
  const posts = [];
  const engine = new OfferItemEngineV2({ adapter, onLog() {} });
  if (engine.http) {
    engine.http.warmUp = async () => ({ ok: true, ms: 1 });
    engine.http.request = async () => {
      posts.push(Date.now());
      return { status: 200, headers: {}, body: { order_hash: "0x" + "11".repeat(32) }, firstByteMs: 1, totalMs: 1 };
    };
  }
  engine.state = STATE.RUNNING;
  engine.attachStreamHealth(() => "HEALTHY");
  engine.templateReady = () => true;
  engine.ownAuthoritative = () => true;
  engine.topicReady = () => true;
  const key = engine.registerRow({ url: "", contract: CONTRACT, tokenId: "1859",
    collectionSlug: SLUG, minPrice: 0.001, maxPrice: 0.09, step: 0.0001, duration: 15 });
  const row = engine.rows.get(key);
  const book = engine.book.get(key);
  book.generation = 1; book.hydratedAt = Date.now() - 1000; book.ownReconciledAt = Date.now() - 1000;
  book.selfAddress = SELF;
  engine.book.setSelfAddress(SELF);
  // A second tracked row, never involved in any revalidation below -- any read
  // issued for it would mean the repair stopped being per-NFT.
  const otherKey = engine.registerRow({ url: "", contract: CONTRACT, tokenId: "2000",
    collectionSlug: SLUG, minPrice: 0.001, maxPrice: 0.09, step: 0.0001, duration: 15 });
  const otherBook = engine.book.get(otherKey);
  otherBook.generation = 1; otherBook.hydratedAt = Date.now() - 1000;
  otherBook.ownReconciledAt = Date.now() - 1000; otherBook.selfAddress = SELF;
  return { engine, key, row, book, resolvers, rejecters, posts, otherKey };
}

const bidEvent = (at, price, version) => ({
  collectionSlug: SLUG, nft: { chain: "ethereum", contract: CONTRACT, tokenId: "1859" },
  kind: "item", orderHash: HASH, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
  eventTimestamp: at, receivedAt: at, hasOrderData: true,
  event: "item_received_bid", pricePerItem: price, version
});

const removeEvent = (at, version, event) => ({
  collectionSlug: SLUG, nft: { chain: "ethereum", contract: CONTRACT, tokenId: "1859" },
  kind: "item", orderHash: HASH, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0,
  eventTimestamp: at, receivedAt: at, hasOrderData: false, event, version
});

// Shaped exactly like what `toBookOp` produces for an order_revalidate frame.
const revalOp = (at, version) => ({
  op: "REVALIDATE", kind: "item", eventName: "order_revalidate",
  orderHash: HASH, scope: HASH, seq: at, version, versionFamily: "orderRevision",
  collectionSlug: SLUG, contract: CONTRACT, tokenId: "1859", chain: "ethereum",
  maker: RIVAL, price: 0, quantity: 1, currency: "WETH", endTime: 0,
  receivedAt: at, hasOrderData: false, soft: false, terminal: false
});

async function seeded() {
  const ctx = buildEngine();
  const t0 = Date.now();
  ctx.engine.apply(bidEvent(t0, 0.0265, 1));
  await flush();
  assert.equal(ctx.book.effectiveBest(Date.now()).price, 0.0265, "fixture: the competitor bid is the Best");
  return Object.assign(ctx, { t0 });
}

(async () => {

// ---- A: debt while a read is already in flight ---------------------------

await check("A: a revalidate arriving while a read is in flight is not dropped -- exactly one follow-up read runs after that read settles", async () => {
  const { engine, key, row, resolvers } = await seeded();
  engine.queueRead(row, { reason: "audit", authoritative: false, readAt: Date.now(), firstRead: false, attempt: 1 });
  await flush();
  assert.equal(resolvers.length, 1, "fixture: one read is in flight");
  const pr = engine.pendingReads.get(key);
  assert.ok(pr && pr.startedAt, "fixture: the in-flight read has left the machine");

  const at = pr.startedAt + 50;
  assert.equal(engine.onOrderRevalidate(revalOp(at, 3), at), 1, "the revalidation must be accounted for, not dropped");
  assert.equal(resolvers.length, 1, "and must NOT open a second parallel read while the first is still flying");

  resolvers.shift()({ orderHash: "", price: 0, orders: [] });
  await flush(10);
  assert.equal(resolvers.length, 1,
    "once the pre-revalidation snapshot settles, the debt must produce exactly one follow-up read");
});

await check("A: the follow-up read stays background (authoritative:false) and is labelled revalidate, so it cannot outrank P0", async () => {
  const { engine, key, row, resolvers } = await seeded();
  engine.queueRead(row, { reason: "audit", authoritative: false, readAt: Date.now(), firstRead: false, attempt: 1 });
  await flush();
  const pr = engine.pendingReads.get(key);
  const at = pr.startedAt + 50;
  engine.onOrderRevalidate(revalOp(at, 3), at);

  const seen = [];
  const realQueueRead = engine.queueRead.bind(engine);
  engine.queueRead = (r, opts) => { seen.push(opts); return realQueueRead(r, opts); };
  resolvers.shift()({ orderHash: "", price: 0, orders: [] });
  await flush(10);
  const followUp = seen.find(o => o.reason === "revalidate");
  assert.ok(followUp, "the follow-up must carry the revalidate reason, not a trait label");
  assert.equal(followUp.authoritative, false, "a revalidation repair must not escalate to an authoritative read");
});

await check("A: the trait path keeps its own reason/authority when it arms the same debt", async () => {
  const { engine, key, row, resolvers } = await seeded();
  engine.queueRead(row, { reason: "audit", authoritative: false, readAt: Date.now(), firstRead: false, attempt: 1 });
  await flush();
  const pr = engine.pendingReads.get(key);
  // Arm the debt the way resolveTraitScope does: timestamp only, no label.
  pr.rereadAfter = pr.startedAt + 50;

  const seen = [];
  const realQueueRead = engine.queueRead.bind(engine);
  engine.queueRead = (r, opts) => { seen.push(opts); return realQueueRead(r, opts); };
  resolvers.shift()({ orderHash: "", price: 0, orders: [] });
  await flush(10);
  const followUp = seen.find(o => o.reason === "trait-scope");
  assert.ok(followUp, "an unlabelled debt must still default to the trait path's reason");
  assert.equal(followUp.authoritative, true, "and to its authoritative read -- unchanged by this patch");
});

await check("A: the repair stays per-NFT -- no other tracked row is read", async () => {
  const { engine, key, row, resolvers, otherKey } = await seeded();
  engine.queueRead(row, { reason: "audit", authoritative: false, readAt: Date.now(), firstRead: false, attempt: 1 });
  await flush();
  const pr = engine.pendingReads.get(key);
  const at = pr.startedAt + 50;
  engine.onOrderRevalidate(revalOp(at, 3), at);

  const rowsRead = [];
  const realQueueRead = engine.queueRead.bind(engine);
  engine.queueRead = (r, opts) => { rowsRead.push(r.key); return realQueueRead(r, opts); };
  resolvers.shift()({ orderHash: "", price: 0, orders: [] });
  await flush(10);
  assert.ok(!rowsRead.includes(otherKey), "an unrelated row must never be pulled into a revalidation repair");
});

await check("A: a stopped row is never read for a revalidation", async () => {
  const { engine, key, row, resolvers } = await seeded();
  row.running = false;
  const before = resolvers.length;
  assert.equal(engine.onOrderRevalidate(revalOp(Date.now(), 3), Date.now()), 0);
  await flush();
  assert.equal(resolvers.length, before, "a paused/stopped row must not consume read capacity");
});

await check("A: with no read in flight, a revalidation still asks exactly one targeted read", async () => {
  const { engine, resolvers } = await seeded();
  assert.equal(resolvers.length, 0, "fixture: nothing in flight");
  assert.equal(engine.onOrderRevalidate(revalOp(Date.now(), 3), Date.now()), 1);
  await flush();
  assert.equal(resolvers.length, 1, "unchanged pre-existing behaviour: one targeted read");
});

// ---- B: revalidate as an ordering landmark -------------------------------

await check("B: a revalidation records its order revision", async () => {
  const { engine, book, t0 } = await seeded();
  engine.onOrderRevalidate(revalOp(t0 + 1000, 3), t0 + 1000);
  await flush();
  const entry = book.seenVersion && book.seenVersion.get(HASH);
  assert.ok(entry, "the hash must have version state after a revalidation");
  assert.equal(entry.orderRevision, 3, "and it must be the revalidation's revision");
});

await check("B: a late LOWER-revision order_invalidate can no longer wipe a revalidated order", async () => {
  const { engine, book, t0 } = await seeded();
  engine.onOrderRevalidate(revalOp(t0 + 1000, 3), t0 + 1000);
  await flush();
  engine.apply(removeEvent(t0 + 2000, 2, "order_invalidate"));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.0265,
    "OpenSea said revision 3 is valid -- a revision-2 invalidation is older state and must not remove it");
});

await check("B: a late LOWER-revision item_cancelled is also refused after a revalidation", async () => {
  const { engine, book, t0 } = await seeded();
  engine.onOrderRevalidate(revalOp(t0 + 1000, 5), t0 + 1000);
  await flush();
  engine.apply(removeEvent(t0 + 2000, 4, "item_cancelled"));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.0265,
    "the terminal-cancel bypass must not smuggle a lower same-family revision past the version gate");
  assert.equal(book.isTombstoned(HASH), false, "and refused state must not tombstone");
});

await check("B: a HIGHER-revision cancellation after a revalidation still ends the order", async () => {
  const { engine, book, t0 } = await seeded();
  engine.onOrderRevalidate(revalOp(t0 + 1000, 3), t0 + 1000);
  await flush();
  engine.apply(removeEvent(t0 + 2000, 4, "item_cancelled"));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0,
    "a genuinely newer cancellation must still win -- the fix must not make orders immortal");
  assert.equal(book.isTombstoned(HASH), true);
});

await check("B: recording the revision does not block a genuinely newer bid for the same hash", async () => {
  const { engine, book, t0 } = await seeded();
  engine.onOrderRevalidate(revalOp(t0 + 1000, 3), t0 + 1000);
  await flush();
  engine.apply(bidEvent(t0 + 2000, 0.0302, 4));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.0302,
    "seq ordering is deliberately untouched by noteVersion -- a newer revision must still reprice");
});

await check("B: an epoch-ms item_sold cannot poison the revision a revalidation recorded", async () => {
  const { engine, book, t0 } = await seeded();
  engine.apply(removeEvent(t0 + 500, 1791055181434, "item_sold"));
  await flush();
  engine.onOrderRevalidate(revalOp(t0 + 1000, 3), t0 + 1000);
  await flush();
  const entry = book.seenVersion.get(HASH);
  assert.equal(entry.orderRevision, 3, "order-revision state must be kept apart from the epoch-ms scale");
  assert.equal(entry.epochMs, 1791055181434, "and the epoch-ms counter must still be recorded under its own family");
});

// ---- C: millisecond-granularity arming ----------------------------------
//
// `startedAt` and `at` are both Date.now(). A read that genuinely left the
// machine BEFORE the event can still land in the same millisecond, and a
// strict `<` then drops the revalidation for good. Equal timestamps are
// "unknown", not "already included", so the debt is armed -- at a cost of at
// most one extra background read for that one row.

await check("C: a revalidate arriving in the SAME millisecond the read started is not dropped", async () => {
  const { engine, key, row, resolvers } = await seeded();
  engine.queueRead(row, { reason: "audit", authoritative: false, readAt: Date.now(), firstRead: false, attempt: 1 });
  await flush();
  const pr = engine.pendingReads.get(key);
  const at = pr.startedAt;
  assert.ok(at, "fixture: the read has left the machine");
  assert.equal(engine.onOrderRevalidate(revalOp(at, 3), at), 1,
    "equal timestamps are ambiguous -- the revalidation must still be accounted for");
  assert.equal(resolvers.length, 1, "and must not open a parallel read");
  resolvers.shift()({ orderHash: "", price: 0, orders: [] });
  await flush(12);
  assert.equal(resolvers.length, 1, "exactly one follow-up read, no more");
});

await check("C: several revalidates during one in-flight read coalesce into exactly one follow-up", async () => {
  const { engine, key, row, resolvers } = await seeded();
  engine.queueRead(row, { reason: "audit", authoritative: false, readAt: Date.now(), firstRead: false, attempt: 1 });
  await flush();
  const pr = engine.pendingReads.get(key);
  const at = pr.startedAt;
  const results = [
    engine.onOrderRevalidate(revalOp(at, 3), at),
    engine.onOrderRevalidate(revalOp(at + 1, 4), at + 1),
    engine.onOrderRevalidate(revalOp(at + 2, 5), at + 2)
  ];
  assert.deepEqual(results, [1, 0, 0], "the debt coalesces -- only the first arms it");
  assert.equal(resolvers.length, 1, "still no parallel read");
  resolvers.shift()({ orderHash: "", price: 0, orders: [] });
  await flush(12);
  assert.equal(resolvers.length, 1, "three revalidations must still cost exactly one follow-up read");
});

await check("C: the follow-up read carries no debt of its own, so it cannot loop", async () => {
  const { engine, key, row, resolvers } = await seeded();
  engine.queueRead(row, { reason: "audit", authoritative: false, readAt: Date.now(), firstRead: false, attempt: 1 });
  await flush();
  const pr = engine.pendingReads.get(key);
  engine.onOrderRevalidate(revalOp(pr.startedAt, 3), pr.startedAt);
  resolvers.shift()({ orderHash: "", price: 0, orders: [] });
  await flush(12);
  const followUpOwner = engine.pendingReads.get(key);
  assert.ok(followUpOwner, "fixture: the follow-up read is in flight");
  assert.notEqual(followUpOwner.rereadPending, true,
    "a fresh read owner must start with no debt -- otherwise each follow-up would spawn another");
  resolvers.shift()({ orderHash: "", price: 0, orders: [] });
  await flush(12);
  assert.equal(resolvers.length, 0, "and the chain terminates");
});

// ---- D: the in-flight read fails, then retries --------------------------
//
// On failure the engine keeps the pendingReads record and schedules a backoff
// retry (READ_RETRY_MS 5s/15s/30s/60s). That retry calls queueRead again,
// which REPLACES the read owner -- carrying only `since`/`correlationId`, so
// the debt flag does not survive. That is correct rather than a leak: the
// replacement read necessarily leaves the machine AFTER the revalidate event
// (the backoff is seconds), so its own snapshot is authoritative for the
// revalidation and a separate follow-up would be a redundant read. This test
// pins the whole lifecycle: one retry, the revalidation reflected, a clean
// terminal state, and no storm.

await check("D: a failed in-flight read retries exactly once and its own snapshot covers the revalidation, with no storm", async () => {
  const realSetTimeout = global.setTimeout;
  const realClearTimeout = global.clearTimeout;
  const timers = [];
  global.setTimeout = (fn, ms) => { const h = { fn, ms, cleared: false, unref() { return h; } }; timers.push(h); return h; };
  global.clearTimeout = h => { if (h && typeof h === "object") h.cleared = true; else realClearTimeout(h); };
  try {
    const ctx = buildEngine();
    const t0 = Date.now();
    ctx.engine.apply(bidEvent(t0, 0.0265, 1));
    await flush();
    const { engine, key, row, book, resolvers, rejecters } = ctx;

    engine.queueRead(row, { reason: "audit", authoritative: false, readAt: Date.now(), firstRead: false, attempt: 1 });
    await flush();
    assert.equal(resolvers.length, 1, "fixture: one read is in flight");
    const pr1 = engine.pendingReads.get(key);
    const at = pr1.startedAt;
    assert.equal(engine.onOrderRevalidate(revalOp(at, 3), at), 1, "fixture: the debt is armed");

    // Fail that read through the engine's real failure path.
    resolvers.shift();
    rejecters.shift()(new Error("REST 503"));
    await flush(12);

    assert.equal(engine.pendingReads.has(key), true, "the read record must survive a failure");
    assert.equal(engine.pendingReads.get(key), pr1, "and must still be the same owner while it waits to retry");
    assert.equal(engine.hydrating.has(key), false, "but must no longer look like an in-flight read");
    const backoff = timers.filter(t => !t.cleared && t.ms >= 1000).map(t => t.ms);
    assert.ok(backoff.length >= 1, `a bounded backoff retry must be scheduled (saw ${backoff.join(",")})`);
    // READ_RETRY_MS[0] is 5s. This is what makes the dropped debt harmless:
    // the replacement read cannot leave the machine until at least 5 seconds
    // after the failure, which is itself after the revalidate event was
    // armed -- so the retry's own snapshot necessarily covers the
    // revalidation, and carrying the debt forward would only add a redundant
    // read. Pin the number so a future shortening of this backoff cannot
    // quietly invalidate that reasoning.
    assert.ok(backoff.includes(5000),
      `the first retry must wait the full 5s floor (saw ${backoff.join(",")})`);
    assert.ok(at >= pr1.startedAt && at <= Date.now(),
      "the revalidate event happened during the failed read, before the retry was scheduled");

    for (const t of [...timers]) {
      if (!t.cleared && typeof t.fn === "function" && t.ms >= 1000) { t.cleared = true; t.fn(); }
    }
    await flush(12);

    const pr2 = engine.pendingReads.get(key);
    assert.notEqual(pr2, pr1, "the retry replaces the read owner");
    assert.equal(resolvers.length, 1, "exactly one retry read is issued, not a burst");

    resolvers.shift()({
      orderHash: HASH, price: 0.0265,
      orders: [{ orderHash: HASH, price: 0.0265, maker: RIVAL, quantity: 1, currency: "WETH", endTime: 0, kind: "item" }]
    });
    await flush(14);

    assert.equal(book.effectiveBest(Date.now()).price, 0.0265,
      "the retry's own snapshot reflects the revalidated order");
    assert.equal(engine.pendingReads.has(key), false, "a successful read must clear the record");
    assert.equal(engine.hydrating.has(key), false, "and must not leave the row looking busy");
    assert.equal(resolvers.length, 0, "no extra reads left behind -- no storm");
  } finally {
    global.setTimeout = realSetTimeout;
    global.clearTimeout = realClearTimeout;
  }
});

// ---- E: the row is PAUSED when the revalidation arrives -----------------
//
// Pausing a row (`suspendRow`) only stops READS and SENDS -- the row stays
// registered, its book stays in place, and `MemoryBook.apply()` keeps
// processing Stream events for it (pinned below). So a revalidation that
// arrives while paused still matters: if its revision is thrown away just
// because the row is not running, a later LOWER-revision order_invalidate is
// accepted and removes the order from Best. On Resume the engine then
// believes there is no competitor and bids low.

await check("E: a paused row still applies Stream events -- the premise the rest of E depends on", async () => {
  const { engine, key, book } = await seeded();
  engine.suspendRow(key);
  assert.equal(engine.rows.has(key), true, "pause must keep the row registered");
  assert.ok(engine.book.get(key), "and must keep its book");
  const seqBefore = book.streamSeq;
  engine.apply(bidEvent(Date.now() + 10, 0.0266, 3));
  await flush();
  assert.notEqual(book.streamSeq, seqBefore, "a paused row must still ingest Stream events");
  assert.equal(book.effectiveBest(Date.now()).price, 0.0266, "and its Best must still move");
});

await check("E: a revalidation arriving while paused still records its order revision", async () => {
  const { engine, key, book, t0 } = await seeded();
  engine.suspendRow(key);
  engine.onOrderRevalidate(revalOp(t0 + 1000, 9), t0 + 1000);
  await flush();
  const entry = book.seenVersion && book.seenVersion.get(HASH);
  assert.ok(entry, "the hash must still have version state while paused");
  assert.equal(entry.orderRevision, 9,
    "pausing stops reads and sends, not the ordering landmarks the book needs");
});

await check("E: a revalidation arriving while paused issues no read at all", async () => {
  const { engine, key, resolvers, t0 } = await seeded();
  engine.suspendRow(key);
  const before = resolvers.length;
  assert.equal(engine.onOrderRevalidate(revalOp(t0 + 1000, 9), t0 + 1000), 0,
    "a paused row must report no read issued");
  await flush();
  assert.equal(resolvers.length, before,
    "a paused/stopped row must never spend read capacity -- that gate is absolute");
});

await check("E: a later LOWER-revision invalidate cannot wipe an order revalidated while paused", async () => {
  const { engine, key, book, t0 } = await seeded();
  engine.suspendRow(key);
  engine.onOrderRevalidate(revalOp(t0 + 1000, 9), t0 + 1000);
  await flush();
  // Revision 5: below the revalidation (9), above the seed (1).
  engine.apply(removeEvent(t0 + 2000, 5, "order_invalidate"));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0.0265,
    "OpenSea said revision 9 is valid -- a revision-5 invalidation is older state, paused or not");
});

await check("E: a HIGHER-revision invalidate while paused still removes the order -- no immortal orders", async () => {
  const { engine, key, book, t0 } = await seeded();
  engine.suspendRow(key);
  engine.onOrderRevalidate(revalOp(t0 + 1000, 9), t0 + 1000);
  await flush();
  engine.apply(removeEvent(t0 + 2000, 10, "order_invalidate"));
  await flush();
  assert.equal(book.effectiveBest(Date.now()).price, 0,
    "a genuinely newer invalidation must still win while paused");
});

await check("E: resuming after a paused revalidation keeps the book and emits no wrong or duplicate POST", async () => {
  const { engine, key, row, book, resolvers, posts, t0 } = await seeded();
  engine.suspendRow(key);
  engine.onOrderRevalidate(revalOp(t0 + 1000, 9), t0 + 1000);
  await flush();
  engine.apply(removeEvent(t0 + 2000, 5, "order_invalidate"));
  await flush();
  const postsBefore = posts.length;
  const readsBefore = resolvers.length;

  engine.resumeRow(key);
  await flush(14);

  assert.equal(row.running, true, "the row is running again");
  assert.equal(book.effectiveBest(Date.now()).price, 0.0265,
    "the revalidated competitor is still the Best after Resume");
  assert.equal(posts.length - postsBefore, 0,
    "Resume alone must not fire a POST -- the decision path owns that, not the revalidation repair");
  assert.ok(resolvers.length - readsBefore <= 1,
    `Resume must not burst reads (saw ${resolvers.length - readsBefore})`);
});

await check("E: a revalidation for an NFT that is no longer tracked records nothing and does not throw", async () => {
  const { engine, key, resolvers, t0 } = await seeded();
  engine.removeRow(key);
  const before = resolvers.length;
  assert.equal(engine.onOrderRevalidate(revalOp(t0 + 1000, 9), t0 + 1000), 0);
  await flush();
  assert.equal(resolvers.length, before, "a removed row must never be read for");
});

process.stdout.write(`\n${passed} passed, ${failed} failed\n`);
process.exit(failed ? 1 : 0);
})();
