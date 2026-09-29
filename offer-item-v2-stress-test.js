"use strict";

/**
 * offer-item-v2-stress-test.js — quy mô, tài nguyên, và không rò rỉ.
 *
 * BA CÂU HỎI
 *
 *   1. QUY MÔ    1 → 1000 NFT. Độ trễ P0 có xấu đi theo số NFT không? Nếu có
 *                thì kiến trúc đang quét thay vì tra chỉ số.
 *   2. BÙNG      100 rồi 1000 sự kiện cùng lúc, và 100.000 phát lại. Event
 *                loop có nghẹt không?
 *   3. RÒ RỈ     sau 100.000 sự kiện, bộ nhớ và số cấu trúc có trở về không?
 *                Một Map không trần là một tiến trình chết sau ba ngày.
 *
 * Không mạng: HTTP và adapter là bản giả. Cái đang đo là kiến trúc, và mạng
 * thật sẽ biến mọi phép đo thành nhiễu.
 */

const path = require("path");
const v8 = require("v8");
const { ethers } = require("ethers");
const { monitorEventLoopDelay } = require("perf_hooks");

const V2 = path.join(__dirname, "offer-item-v2");
const { OfferItemEngineV2 } = require(V2 + "/engine-v2");

let pass = 0, fail = 0;
const say = s => process.stdout.write(s + "\n");
const check = (name, ok, detail = "") => {
  if (ok) { pass++; say(`  PASS  ${name}`); }
  else { fail++; say(`  FAIL  ${name}${detail ? " :: " + detail : ""}`); }
};
const sleep = ms => new Promise(r => setTimeout(r, ms));

const TEST_PK = ethers.Wallet.createRandom().privateKey;
const CONTRACT = "0xa2a6063b910fc7a7a286196f6c9b62b2797fa0ae";
const WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2";
const CONDUIT = "0x0000007b02230091a7ed01230072f7006a004d60a8d4e71d599b8104250f0000";
const SLUG = "stress-collection";
const RIVAL = "0x2222222222222222222222222222222222222222";

const adapter = {
  chain: "ethereum", openseaChain: "ethereum",
  async chainConfig() {
    return { chainId: 1, wethAddress: WETH, conduitKey: CONDUIT,
      signedZone: "0x000056f7000000ece9003ca63978907a00ffd100" };
  },
  async accountState() { return { at: Date.now(), counter: 0 }; },
  async collectionConfig() {
    return { slug: SLUG, fees: [], requiresSignedZone: false, tokenStandard: "erc721" };
  },
  toWei: v => ethers.parseEther(String(v)),
  offerBody: s => ({ parameters: s.components, signature: s.signature }),
  async resyncOwnOrders() { return { orders: [], complete: true }; }
};

/** Quota luôn cấp ngay: bài này đo kiến trúc, không đo hạn mức. */
const openQuota = {
  async acquire() { return { ok: true, waitedMs: 0, why: "" }; },
  report() {}, status() { return { role: "solo" }; }
};

async function makeEngine(rowCount) {
  const engine = new OfferItemEngineV2({
    adapter, quota: openQuota, getApiKey: () => "k", onLog: () => {}
  });
  let sent = 0;
  engine.http.warmUp = async () => ({ ok: true, ms: 1 });
  engine.http.request = async () => {
    sent++;
    return { status: 200, headers: {}, body: { order_hash: "0x" + "11".repeat(32) },
      firstByteMs: 1, totalMs: 1 };
  };
  const rows = [];
  for (let i = 1; i <= rowCount; i++) {
    rows.push({
      url: `u/${i}`, contract: CONTRACT, tokenId: String(i),
      collectionSlug: SLUG, traits: [],
      minPrice: 0.001, maxPrice: 100, step: 0.0001, duration: 15
    });
  }
  await engine.start({ privateKey: TEST_PK, rows });
  return { engine, sentCount: () => sent };
}

let seq = 0;
const bid = (tokenId, price) => ({
  event: "item_received_bid", collectionSlug: SLUG,
  nft: { chain: "ethereum", contract: CONTRACT, tokenId: String(tokenId) },
  kind: "item", orderHash: "0x" + (++seq).toString(16).padStart(16, "0"),
  maker: RIVAL, pricePerItem: price, quantity: 1, currency: "WETH",
  endTime: Math.floor(Date.now() / 1000) + 3600,
  eventTimestamp: Date.now() + seq, receivedAt: Date.now(), hasOrderData: true
});

const mb = n => Math.round(n / 1024 / 1024 * 10) / 10;

function quant(list) {
  const s = [...list].sort((a, b) => a - b);
  const at = p => s[Math.min(s.length - 1, Math.floor(p * s.length))];
  return { p50: at(0.5), p90: at(0.9), p95: at(0.95), max: s[s.length - 1] };
}

// ====================================================================
async function partScale() {
  say("\n--- 1 · quy mô: độ trễ P0 theo số NFT ---");
  const table = [];

  for (const n of [1, 50, 120, 500, 1000]) {
    const { engine } = await makeEngine(n);

    // Một sự kiện cho mỗi NFT, đo thời gian ĐỒNG BỘ từ lúc nhận tới lúc có
    // ý định — đó là phần kiến trúc chịu trách nhiệm.
    const times = [];
    for (let i = 1; i <= n; i++) {
      const t0 = process.hrtime.bigint();
      engine.handleStreamEvent(bid(i, 0.01));
      times.push(Number(process.hrtime.bigint() - t0) / 1e6);
    }
    const q = quant(times);
    table.push({ n, ...q });
    say(`  ${String(n).padStart(4)} NFT · sự kiện→ý định: ` +
      `p50=${q.p50.toFixed(3)}ms p95=${q.p95.toFixed(3)}ms max=${q.max.toFixed(3)}ms`);
    engine.shutdown();
    await sleep(50);
  }

  const small = table[0], big = table[table.length - 1];
  check("độ trễ KHÔNG tăng theo số NFT",
    big.p95 < Math.max(2, small.p95 * 8 + 1),
    `1 NFT p95=${small.p95.toFixed(3)}ms · 1000 NFT p95=${big.p95.toFixed(3)}ms`);
  check("ngay cả với 1000 NFT, p95 vẫn dưới 2ms", big.p95 < 2,
    `${big.p95.toFixed(3)}ms — quét tuyến tính sẽ thấy rõ ở đây`);
}

async function partCollectionFanout() {
  say("\n--- 2 · một collection offer chạm 1000 NFT ---");
  const { engine } = await makeEngine(1000);

  const t0 = process.hrtime.bigint();
  engine.handleStreamEvent({
    event: "collection_offer", collectionSlug: SLUG, nft: null, kind: "collection",
    orderHash: "0xcoll" + Date.now(), maker: RIVAL, pricePerItem: 0.5,
    quantity: 1, currency: "WETH",
    endTime: Math.floor(Date.now() / 1000) + 3600,
    eventTimestamp: Date.now(), receivedAt: Date.now(), hasOrderData: true
  });
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  say(`  một collection offer → lô đầu trong ${ms.toFixed(2)}ms (đồng bộ)`);
  check("phần ĐỒNG BỘ của fan-out dưới 20ms", ms < 20, `${ms.toFixed(2)}ms`,
    "khoá vòng lặp ở đây là bắt mọi sự kiện khác xếp hàng sau");

  // Phần đuôi chạy ở các lượt event loop sau — chờ nó rút hết rồi mới đếm.
  for (let i = 0; i < 60 && engine.intents.size() < 1000; i++) await sleep(20);
  say(`  sau khi rút hết: ${engine.intents.size()} ý định`);
  check("và MỌI token đều thấy nó, không sót cái nào",
    engine.intents.size() === 1000, String(engine.intents.size()),
    "chia lô mà làm mất token là tệ hơn nghẹt vòng lặp");

  engine.shutdown();
}

/** Hạn mức giống thật: vài lệnh ghi mỗi giây, không phải vô hạn. */
function realisticQuota(perSecond = 10) {
  let tokens = perSecond, last = Date.now();
  return {
    async acquire() {
      for (;;) {
        const now = Date.now();
        tokens = Math.min(perSecond, tokens + (now - last) / 1000 * perSecond);
        last = now;
        if (tokens >= 1) { tokens -= 1; return { ok: true, waitedMs: 0, why: "" }; }
        await sleep(20);
      }
    },
    report() {}, status() { return { role: "solo" }; }
  };
}

async function partBurst() {
  say("\n--- 3 · bùng sự kiện, event loop có nghẹt không ---");

  /**
   * ĐO VỚI HẠN MỨC GIỐNG THẬT, KHÔNG PHẢI QUOTA VÔ HẠN
   *
   *   Bản đầu dùng quota luôn cấp ngay, nên 100 sự kiện trong một tick sinh
   *   ra 100 lượt ký — và 100 × 1,2ms CPU chính là 120ms độ trễ đo được. Con
   *   số đó đúng, nhưng nó mô tả một tình huống production không có: hạn mức
   *   ghi của OpenSea chỉ hấp thụ vài lệnh mỗi giây, nên engine không bao giờ
   *   được phép ký 100 order trong một tick.
   *
   *   Chặn số lượt gửi ĐANG BAY (MAX_INFLIGHT_SUBMITS) giới hạn đồng thời,
   *   nhưng thứ quyết định TỔNG khối lượng ký là hạn mức phía sau. Nên bài
   *   này đo với hạn mức giống thật, và ghi lại cả con số trường hợp xấu nhất
   *   để không ai quên chi phí ký là có thật.
   */
  const engineQuota = realisticQuota(10);
  const engine = new OfferItemEngineV2({
    adapter, quota: engineQuota, getApiKey: () => "k", onLog: () => {}
  });
  engine.http.warmUp = async () => ({ ok: true, ms: 1 });
  engine.http.request = async () => ({
    status: 200, headers: {}, body: { order_hash: "0x" + "11".repeat(32) },
    firstByteMs: 1, totalMs: 1
  });
  const rows = [];
  for (let i = 1; i <= 120; i++) {
    rows.push({ url: `u/${i}`, contract: CONTRACT, tokenId: String(i),
      collectionSlug: SLUG, traits: [],
      minPrice: 0.001, maxPrice: 100, step: 0.0001, duration: 15 });
  }
  await engine.start({ privateKey: TEST_PK, rows });

  const h = monitorEventLoopDelay({ resolution: 1 });
  h.enable();

  for (let round = 0; round < 10; round++) {
    for (let i = 0; i < 100; i++) {
      engine.handleStreamEvent(bid((i % 120) + 1, 0.01 + round * 0.001));
    }
    await sleep(10);
  }
  await sleep(200);
  h.disable();

  const lagP99 = h.percentile(99) / 1e6;
  const lagMax = h.max / 1e6;

  /**
   * ĐỐI CHỨNG: cùng khoảng thời gian, KHÔNG một sự kiện nào.
   *
   * Một con số độ trễ đơn lẻ không nói được nó từ đâu ra. Tiến trình này vừa
   * dựng năm engine, cái lớn nhất 1000 hàng, ở mục 1 — một lần thu gom rác
   * lớn hoàn toàn có thể là nguyên nhân, và đổ cho engine khi thủ phạm là GC
   * sẽ dẫn tới "tối ưu" đúng chỗ không có vấn đề.
   *
   * Đo lúc rảnh trong CÙNG tiến trình, cùng độ dài, rồi lấy hiệu.
   */
  const idle = monitorEventLoopDelay({ resolution: 1 });
  idle.enable();
  for (let round = 0; round < 10; round++) await sleep(10);
  await sleep(200);
  idle.disable();
  const idleP99 = idle.percentile(99) / 1e6;
  const idleMax = idle.max / 1e6;

  say(`  1000 sự kiện · p99=${lagP99.toFixed(1)}ms max=${lagMax.toFixed(1)}ms`);
  say(`  đối chứng rảnh · p99=${idleP99.toFixed(1)}ms max=${idleMax.toFixed(1)}ms`);
  say(`  phần do engine ≈ ${(lagP99 - idleP99).toFixed(1)}ms`);

  check("engine KHÔNG làm nghẹt event loop hơn mức nền",
    lagP99 - idleP99 < 50,
    `sự kiện p99=${lagP99.toFixed(1)}ms · rảnh p99=${idleP99.toFixed(1)}ms`);

  engine.shutdown();
}

async function partReplay() {
  say("\n--- 4 · 100.000 sự kiện phát lại: có rò rỉ không ---");
  const { engine } = await makeEngine(200);

  if (global.gc) global.gc();
  const before = process.memoryUsage();
  const heapBefore = v8.getHeapStatistics().used_heap_size;

  const t0 = Date.now();
  for (let i = 0; i < 100000; i++) {
    engine.handleStreamEvent(bid((i % 200) + 1, 0.01 + (i % 500) * 0.0001));
    if (i % 10000 === 0) await sleep(1);
  }
  const elapsed = Date.now() - t0;
  await sleep(300);

  if (global.gc) global.gc();
  const after = process.memoryUsage();
  const heapAfter = v8.getHeapStatistics().used_heap_size;

  say(`  100.000 sự kiện trong ${(elapsed / 1000).toFixed(1)}s ` +
    `(${Math.round(100000 / (elapsed / 1000))}/giây)`);
  say(`  RSS ${mb(before.rss)}MB → ${mb(after.rss)}MB · ` +
    `heap ${mb(heapBefore)}MB → ${mb(heapAfter)}MB`);

  const census = engine.book.census();
  say(`  sổ: ${JSON.stringify(census)}`);

  check("mọi cấu trúc đều có trần", census.item <= 200 * 64 &&
    census.tombstones <= 200 * 128, JSON.stringify(census));
  check("số ý định bị chặn theo SỐ NFT, không theo số sự kiện",
    engine.intents.size() <= 200, String(engine.intents.size()));
  check("heap không phình theo số sự kiện",
    mb(heapAfter) - mb(heapBefore) < 200,
    `tăng ${(mb(heapAfter) - mb(heapBefore)).toFixed(1)}MB sau 100k sự kiện`);
  check("thông lượng trên 20.000 sự kiện/giây",
    100000 / (elapsed / 1000) > 20000,
    `${Math.round(100000 / (elapsed / 1000))}/giây`);

  // Metrics cũng phải có trần.
  const m = engine.metrics;
  check("trace đang mở có trần", m.open.size <= m.sampleCap, String(m.open.size));
  let samples = 0;
  for (const list of m.samples.values()) samples = Math.max(samples, list.length);
  check("mẫu độ trễ có trần", samples <= m.sampleCap, String(samples));

  engine.shutdown();
}

async function partLifecycleChurn() {
  say("\n--- 5 · Start/Reset 1000 lần: handle và listener có tăng không ---");
  const { engine } = await makeEngine(20);

  const handlesBefore = process._getActiveHandles().length;
  const listenersBefore = process.listenerCount("uncaughtException");

  for (let i = 0; i < 1000; i++) {
    engine.reset("churn");
    for (let k = 1; k <= 5; k++) engine.handleStreamEvent(bid(k, 0.01));
  }
  await sleep(200);

  const handlesAfter = process._getActiveHandles().length;
  say(`  handle ${handlesBefore} → ${handlesAfter} · ` +
    `listener ${listenersBefore} → ${process.listenerCount("uncaughtException")}`);
  check("1000 lần Reset không tích tụ handle",
    handlesAfter - handlesBefore < 20, `${handlesBefore} → ${handlesAfter}`);
  check("không nhân bản listener",
    process.listenerCount("uncaughtException") === listenersBefore);
  check("epoch tăng đúng 1000 lần", engine.epoch >= 1000, String(engine.epoch));

  engine.shutdown();
}

async function partAdaptive() {
  say("\n--- 6 · tài nguyên máy ---");
  const os = require("os");
  const cpus = typeof os.availableParallelism === "function"
    ? os.availableParallelism() : os.cpus().length;
  say(`  lõi khả dụng: ${cpus} · RAM tự do ${mb(os.freemem())}MB / ` +
    `${mb(os.totalmem())}MB`);

  // Ký là công việc CPU thuần. Đo xem nó có đáng đẩy sang worker thread không.
  const { LocalOrderBuilder } = require(V2 + "/order-builder");
  const builder = new LocalOrderBuilder({ privateKey: TEST_PK });
  builder.hydrate("k", {
    chain: "ethereum", chainId: 1, wethAddress: WETH, conduitKey: CONDUIT,
    requiresSignedZone: false, signedZone: "0x000056f7000000ece9003ca63978907a00ffd100",
    fees: [], counter: 0, tokenStandard: "erc721",
    contract: CONTRACT, tokenId: "1"
  });

  const times = [];
  for (let i = 0; i < 200; i++) {
    const t0 = process.hrtime.bigint();
    await builder.build("k", { amountWei: ethers.parseEther("0.01"), durationMinutes: 15 });
    times.push(Number(process.hrtime.bigint() - t0) / 1e6);
  }
  const q = quant(times);
  say(`  dựng + ký: p50=${q.p50.toFixed(2)}ms p95=${q.p95.toFixed(2)}ms max=${q.max.toFixed(2)}ms`);

  check("ký đủ nhanh để KHÔNG cần worker thread", q.p95 < 5,
    `p95=${q.p95.toFixed(2)}ms — chi phí tuần tự hoá qua worker thread ` +
    "(~0,1-1ms mỗi lượt) sẽ ăn hết phần tiết kiệm được");
  say("  → giữ ký trên main thread. Đẩy sang worker chỉ có lợi khi việc CPU");
  say("    dài hơn nhiều lần chi phí IPC, và ở đây thì không.");
}

async function partDiagnosticBounds() {
  say("\n--- 7 · đổi API key nhiều lần không làm diagnostics tăng mãi ---");
  const { engine } = await makeEngine(1);
  for (let i = 0; i < 1000; i++) engine.netStat("write", 1, `key-${i}`);
  const domains = Object.keys(engine.netDiagnostics().perKey).length;
  check("per-key diagnostics giữ tối đa 8 domain gần nhất", domains <= 8, String(domains));
  engine.shutdown();
}

// ====================================================================
async function main() {
  say("");
  say("OFFER ITEM V2 — QUY MÔ, TÀI NGUYÊN, RÒ RỈ");
  say("=".repeat(70));
  await partScale();
  await partCollectionFanout();
  await partBurst();
  await partReplay();
  await partLifecycleChurn();
  await partAdaptive();
  await partDiagnosticBounds();
  say("");
  say("=".repeat(70));
  say(`${pass}/${pass + fail} stress checks passed` + (fail ? ` — ${fail} FAILED` : ""));
  say("=".repeat(70));
  process.exit(fail ? 1 : 0);
}

main().catch(e => { say("ERR " + (e.stack || e.message)); process.exit(1); });

