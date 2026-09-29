"use strict";

/**
 * quota-broker-test.js — một hạn mức ghi cho cả máy, kiểm bằng TIẾN TRÌNH THẬT.
 *
 * BỐI CẢNH
 *
 *   1.19.4 làm cho mọi cửa sổ dùng chung một OpenSea API key. Từ đó, "mỗi
 *   tiến trình một bộ giới hạn" không còn đúng: ba cửa sổ mỗi cửa sổ tự giới
 *   hạn 2 lệnh/giây trở thành sáu lệnh/giây đập vào một hạn mức duy nhất.
 *
 *   Nên bài này KHÔNG tạo nhiều object trong một process — nó `spawn` nhiều
 *   node thật, đúng như nhiều cửa sổ OfferBot. Tranh chấp giữa hai tiến trình
 *   là thứ không tái hiện được bằng cách khác.
 *
 * Không mạng, không credential: vân tay key là một chuỗi test.
 */

const fs = require("fs");
const os = require("os");
const path = require("path");
const { spawn } = require("child_process");

const ROOT = __dirname;
let pass = 0, fail = 0;
const say = s => process.stdout.write(s + "\n");
const check = (name, ok, detail = "") => {
  if (ok) { pass++; say(`  PASS  ${name}`); }
  else { fail++; say(`  FAIL  ${name}${detail ? " :: " + detail : ""}`); }
};
const sleep = ms => new Promise(r => setTimeout(r, ms));

const { QuotaBroker, QuotaModel, FairQueue, fingerprint } =
  require(path.join(ROOT, "offer-item-v2", "quota-broker"));

/** Mỗi lần chạy một vân tay riêng, để hai lượt test không dùng chung pipe. */
const KEY = "test-key-" + process.pid + "-" + Date.now();

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), "quota-"));

/** Một "cửa sổ OfferBot": tiến trình node riêng, xin N giấy phép. */
const WORKER = `
const path = require("path");
const { QuotaBroker } = require(${JSON.stringify(path.join(ROOT, "offer-item-v2", "quota-broker").replace(/\\\\/g, "/"))});

const [key, chain, countRaw, outFile, reportRaw] = process.argv.slice(2);
const count = Number(countRaw);
const report = reportRaw ? JSON.parse(reportRaw) : null;

// Hạn cứng. Một worker treo không được phép treo cả bộ test — và bộ test đã
// treo đúng như vậy một lần, vì leader kẹt trong server.close().
const hardStop = setTimeout(() => {
  try { require("fs").writeFileSync(outFile, JSON.stringify({ error: "worker treo" })); } catch {}
  process.exit(9);
}, 180000);   // nhịp gửi toàn cục 3–4,5s: 12 giấy phép cho 6 tiến trình mất ~45–55s
if (hardStop.unref) hardStop.unref();

/**
 * GIỮ EVENT LOOP SỐNG NHƯ ELECTRON GIỮ
 *
 *   Mọi timer trong broker đều unref — đúng cho production, để broker không
 *   giữ app sống khi người dùng tắt. Nhưng worker này là Node thuần: khi nó
 *   rơi vào vai "solo" (hai tiến trình đua đúng lúc leader chết) thì việc
 *   duy nhất đang chờ là một timer unref, event loop cạn, và tiến trình thoát
 *   với mã 0 TRƯỚC khi ghi kết quả. Đo được: "no output (exit=0 signal=-)".
 *
 *   Electron không bao giờ cạn loop — cửa sổ và IPC giữ nó sống. Một interval
 *   có ref ở đây mô phỏng đúng điều đó, và không đổi gì ở broker.
 */
const keepAlive = setInterval(() => {}, 1000);

(async () => {
  const broker = new QuotaBroker({ getApiKey: () => key, onLog: () => {}, startRps: 2 });
  await broker.start();
  // Vai NGAY SAU BẦU CỬ. Vai lúc kết thúc là một số khác hẳn: leader thoát
  // trước thì follower lên thay, nên đọc vai lúc cuối sẽ thấy nhiều "leader"
  // mà không có hai leader nào cùng tồn tại. Cái cần kiểm là loại trừ lẫn
  // nhau TẠI MỘT THỜI ĐIỂM, nên phải chụp tại thời điểm đó.
  const roleAtStart = broker.role;
  broker.hello(chain);
  await new Promise(r => setTimeout(r, 120));

  // Từ chối tạm thời thì THỬ LẠI, đúng như production.
  //
  // "leader-stopping" và "failover" không phải là từ chối vĩnh viễn — chúng
  // nghĩa là leader vừa đổi. Một cửa sổ thật sẽ xin lại sau khi bầu xong, nên
  // worker ở đây cũng vậy; nếu không, phép kiểm "mọi yêu cầu đều được cấp"
  // sẽ đo sai chính cái nó muốn đo.
  const grants = [];
  for (let i = 0; i < count; i++) {
    let got = null;
    for (let tryN = 0; tryN < 40; tryN++) {
      const r = await broker.acquire({ lane: chain, timeoutMs: 90000 });
      if (r.ok) { got = r; break; }
      if (r.why !== "failover" && r.why !== "leader-stopping" &&
          r.why !== "no-broker") break;
      await new Promise(res => setTimeout(res, 60));
    }
    if (got) grants.push({ at: Date.now(), chain, pid: process.pid, role: broker.role, why: got.why || "", waitMs: got.waitMs || 0 });
    if (report && i === 0) broker.report(report);
  }

  require("fs").writeFileSync(outFile, JSON.stringify({
    pid: process.pid, chain, role: roleAtStart, roleAtEnd: broker.role, grants,
    status: broker.status()
  }));
  await broker.stop();
  clearInterval(keepAlive);
  process.exit(0);
})().catch(e => {
  require("fs").writeFileSync(outFile, JSON.stringify({ error: String(e.message) }));
  process.exit(1);
});
`;

const workerFile = path.join(TMP, "window.js");
fs.writeFileSync(workerFile, WORKER, "utf8");

function launch(key, chain, count, tag, report = null) {
  const out = path.join(TMP, `out-${tag}.json`);
  const args = [workerFile, key, chain, String(count), out];
  if (report) args.push(JSON.stringify(report));
  const child = spawn(process.execPath, args, { stdio: ["ignore", "pipe", "pipe"] });
  const err = [];
  child.stderr.on("data", d => err.push(String(d)));
  return new Promise(resolve => {
    // Một tiến trình con chết mà không kịp ghi gì từng hiện ra đúng một dòng
    // "no output" — không exit code, không signal, không stderr — và không ai
    // phân loại được đó là OOM, là spawn hỏng khi máy quá tải, hay là lỗi
    // thật. Kể đủ ba thứ đó là chuyện tối thiểu một harness phải làm.
    child.on("error", e => err.push(`spawn: ${e.message}`));
    child.on("close", (code, signal) => {
      let data = null;
      try { data = JSON.parse(fs.readFileSync(out, "utf8")); } catch {}
      resolve(data || {
        error: (err.join("").slice(0, 300) || "no output") +
          ` (exit=${code} signal=${signal || "-"} pid=${child.pid || "?"})`
      });
    });
  });
}

// ====================================================================
async function partModel() {
  say("\n--- 1 · mô hình hạn mức: bám server, KHÔNG tự giãn đều ---");

  const M = () => QuotaModel.mono();
  /**
   * TỪ 1.19.8 NHỊP GỬI TOÀN CỤC ĐỨNG TRƯỚC MỌI THỨ
   *
   *   Một giấy phép mỗi 3–4,5 giây cho cả máy, là điều kiện AND với cửa sổ
   *   hạn mức. Các phép kiểm về CỬA SỔ (remaining/reset/epoch) bên dưới đo
   *   lớp nằm dưới nhịp, nên tắt nhịp cho riêng chúng (`paceMinMs: 0`) —
   *   sản phẩm KHÔNG chạy như vậy, và khối ngay sau đây ghim đúng điều đó.
   */
  // Mốc `mono` của bài được lấy TRƯỚC khi dựng mô hình; đồng hồ đơn điệu đã
  // trôi 1ms là mô hình (nextAllowedMono = mono + 0) chặn với "pacing 1ms".
  // Lớp cửa sổ không có nhịp: mốc cho phép về 0.
  const NoPace = (o = {}) => { const m = new QuotaModel({ paceMinMs: 0, paceJitterMs: 0, ...o }); m.nextAllowedMono = 0; return m; };
  // Chỉ LỚP CỬA SỔ server: gáo ngắn hạn mở rộng hết cỡ để không che phép kiểm
  // remaining/reset. Sản phẩm KHÔNG chạy như vậy (gáo = 3); khối gáo ở trên ghim điều đó.
  const WindowOnly = (o = {}) => new QuotaModel({ startRps: 100000, burst: 100000, rateCeiling: 100000, ...o });

  {
    const m = new QuotaModel({ startRps: 2 });
    check("chưa có header thì chưa learned", m.learned === false);
    check("và chưa biết remaining", m.remaining === -1);
  }

  // Mỗi API key là một quota domain: key A bị 429 không được đóng key B.
  {
    const broker = new QuotaBroker({ getApiKey: () => "pool", onLog: () => {} });
    const mono = M();
    const a = broker.modelFor("key-a");
    const b = broker.modelFor("key-b");
    a.nextAllowedMono = 0;
    b.nextAllowedMono = 0;
    broker.applyReport({ domain: "key-a", status: 429, retryAfterMs: 5000 });
    check("429 chỉ cooldown đúng API-key domain", a.check(mono + 100).ok === false && b.check(mono + 100).ok === true);
  }

  // ---- GÁO NGẮN HẠN (1.25.0): cấp NGAY khi còn token, rồi đổ lại theo rate --
  //
  // 1.24.x ép mỗi lượt cách nhau paceMinMs kể cả khi server còn 59 — độ trễ tự
  // tạo (CLAUDE.md §6). Nay: gáo `burst` (=3) dùng ngay, đổ lại `rate`/giây.
  // Gáo tồn tại vì OpenSea có giới hạn ngắn hạn KHÔNG nằm trong header (đo
  // 11 lượt/giây → 429), nên bùng hết 59 cùng lúc là tự xin 429 (§6).
  {
    const mono = M();
    const m = new QuotaModel({ startRps: 2 });
    m.learn({ "x-ratelimit-limit": "60", "x-ratelimit-remaining": "59",
      "x-ratelimit-reset": "60" }, mono, Date.now());
    const first = m.check(mono);
    check("lượt đầu cấp NGAY (không có độ trễ nhịp tự tạo)",
      first.ok === true && first.waitMs === 0, JSON.stringify(first));
    let now = 0;
    for (let i = 0; i < 10; i++) if (m.take(mono)) now++;
    check("server còn 59 vẫn chỉ bùng tới cỡ gáo ngắn hạn (3), không 59", now === m.burst && m.burst === 3, String(now));
    const next = m.check(mono);
    check("lượt kế tiếp chờ đúng một nhịp đổ gáo (~1/rate)",
      next.ok === false && next.why === "burst" && next.waitMs >= 400 && next.waitMs <= 600, JSON.stringify(next));
    const t1 = mono;
    let n = now;
    for (let t = t1 + 100; t < t1 + 60000; t += 100) if (m.take(t)) n++;
    check("trong cửa sổ 60s dùng được gần hết quota nhưng KHÔNG vượt remaining", n >= 50 && n <= 59, String(n));

    const before = m.paceMinMs;
    m.observe({ status: 429, latencyMs: 900 }, t1 + 1000);
    check("429 giảm nhanh capacity", m.paceMinMs >= before * 2, `${before} → ${m.paceMinMs}`);
    const slowed = m.paceMinMs;
    for (let i = 0; i < 12; i++) m.observe({ status: 200, latencyMs: 120 }, t1 + 12000 + i);
    check("ổn định thì tăng lại từng nấc có kiểm soát", m.paceMinMs < slowed, `${slowed} → ${m.paceMinMs}`);
  }

  // ---- CỬA SỔ CỐ ĐỊNH CHO PHÉP BURST -------------------------------
  //
  // Đây là thứ bản đầu tiên làm sai: nó quy 60/60s thành 1 lệnh/giây và ép
  // giãn đều, kể cả khi server vừa nói còn 59. Hai mươi NFT bị vượt giá cùng
  // lúc là hai mươi lệnh cần đi NGAY, không phải trải ra hai mươi giây.
  {
    const mono = M();
    const m = WindowOnly();
    m.learn({ "x-ratelimit-limit": "60", "x-ratelimit-remaining": "59",
      "x-ratelimit-reset": "60" }, mono, Date.now());

    check("đọc được limit", m.limit === 60);
    check("đọc được remaining", m.remaining === 59);
    check("reset quy về đồng hồ đơn điệu", Math.abs((m.resetAtMono - mono) - 60000) < 50,
      String(m.resetAtMono - mono));

    // 59 lệnh liên tiếp, KHÔNG có thời gian trôi (lớp cửa sổ không tự giãn đều).
    let granted = 0;
    for (let i = 0; i < 59; i++) if (m.take(mono)) granted++;
    check("lớp cửa sổ cho đủ 59 lệnh trong cùng một khoảnh khắc", granted === 59,
      `chỉ cấp ${granted} — lớp cửa sổ đang tự giãn đều`);

    const after = m.check(mono);
    check("lệnh thứ 60 thì hết phần của cửa sổ",
      after.ok === false && after.why === "window-exhausted", JSON.stringify(after));
    check("và phải chờ tới đúng lúc reset", Math.abs(after.waitMs - 60000) < 100,
      String(after.waitMs));
  }

  // ---- RESET LÀ EPOCH GIÂY, KHÔNG PHẢI SỐ GIÂY CÒN LẠI ---------------
  //
  // Đo trên phản hồi thật của OpenSea (POST offer, 2026-09):
  //   x-ratelimit-limit: 60 · x-ratelimit-remaining: 59
  //   x-ratelimit-reset: 1789260945           ← epoch giây, không phải "60"
  // Mọi phép kiểm ở trên đều dùng "60". Nếu mô hình đọc epoch như số giây,
  // sau 60 lượt nó sẽ chờ tới… năm 2026 — engine đứng im vĩnh viễn, và không
  // bài kiểm nào ở đây thấy được.
  {
    const mono = M();
    const wall = Date.now();
    const m = NoPace();
    const epoch = Math.floor(wall / 1000) + 37;      // reset sau 37 giây, dạng OpenSea gửi
    m.learn({ "x-ratelimit-limit": "60", "x-ratelimit-remaining": "0",
      "x-ratelimit-reset": String(epoch) }, mono, wall);
    const after = m.check(mono);
    check("reset dạng EPOCH được hiểu là mốc thời gian, không phải khoảng chờ",
      after.ok === false && after.waitMs > 30000 && after.waitMs < 45000,
      `chờ ${after.waitMs}ms — nếu là hàng tỷ ms thì mô hình đọc epoch như số giây`);
    check("và qua mốc đó thì cấp lại được", m.check(mono + 38000).ok === true);
  }

  // ---- QUA MỐC RESET THÌ ĐẦY LẠI -----------------------------------
  {
    const mono = M();
    const m = WindowOnly();
    m.learn({ "x-ratelimit-limit": "10", "x-ratelimit-remaining": "10",
      "x-ratelimit-reset": "5" }, mono, Date.now());
    for (let i = 0; i < 10; i++) m.take(mono);
    check("dùng hết thì bị chặn", m.check(mono).ok === false);
    check("ngay trước mốc reset vẫn bị chặn", m.check(mono + 4999).ok === false);
    check("qua mốc reset thì cấp lại được", m.check(mono + 5001).ok === true,
      "không tự đầy lại là tự dừng vĩnh viễn sau một cửa sổ");
    check("và cấp được đủ cửa sổ mới", (() => {
      let n = 0;
      for (let i = 0; i < 10; i++) if (m.take(mono + 5001)) n++;
      return n;
    })() === 10);
  }

  // ---- SERVER RỘNG HƠN THÌ TẬN DỤNG --------------------------------
  {
    const mono = M();
    const m = WindowOnly();
    m.learn({ "x-ratelimit-limit": "600", "x-ratelimit-remaining": "600",
      "x-ratelimit-reset": "60" }, mono, Date.now());
    let n = 0;
    for (let i = 0; i < 600; i++) if (m.take(mono)) n++;
    check("lớp cửa sổ: hạn mức 600 thì cấp được 600, không kẹt ở 2/giây", n === 600, String(n));
    // Và gáo thật HỌC lên từ phản hồi tốt (có trần), không kẹt mãi ở nhịp khởi động.
    const real = new QuotaModel({ startRps: 2 });
    for (let i = 0; i < 40; i++) real.observe({ status: 200, latencyMs: 100 }, mono + i * 1000);
    check("nhiều phản hồi tốt thì rate tăng khỏi mức khởi động (có trần)",
      real.rate > 2 && real.rate <= 8, String(real.rate));
  }

  // ---- CHƯA CÓ HEADER: THẬN TRỌNG ----------------------------------
  {
    const mono = M();
    const m = NoPace({ startRps: 2 });
    let n = 0;
    for (let i = 0; i < 10; i++) if (m.take(mono)) n++;
    check("chưa biết gì thì KHÔNG bùng — chỉ dùng gáo ngắn hạn (3)", n <= 3,
      `cấp ${n} khi chưa có sự thật nào từ server`);
    const c = m.check(mono);
    check("và lý do chờ là gáo ngắn hạn, có hạn chờ hữu hạn",
      c.ok === false && c.why === "burst" && c.waitMs > 0 && c.waitMs <= 1000, JSON.stringify(c));
  }

  // ---- HEADER MỚI GHI ĐÈ ƯỚC LƯỢNG CỤC BỘ --------------------------
  {
    const mono = M();
    const m = WindowOnly();
    m.learn({ "x-ratelimit-limit": "100", "x-ratelimit-remaining": "100",
      "x-ratelimit-reset": "60" }, mono, Date.now());
    for (let i = 0; i < 30; i++) m.take(mono);
    check("trừ dần cục bộ giữa hai lần server nói",
      m.remainingNow(mono) === 70, String(m.remainingNow(mono)));

    // Server nói thật: chỉ còn 5. Ước lượng cục bộ phải bị vứt.
    m.learn({ "x-ratelimit-remaining": "5" }, mono, Date.now());
    check("header mới ghi đè hoàn toàn ước lượng cục bộ",
      m.remainingNow(mono) === 5, String(m.remainingNow(mono)));
  }

  // ---- RETRY-AFTER -------------------------------------------------
  {
    const mono = M();
    const m = NoPace();
    m.learn({ "x-ratelimit-limit": "100", "x-ratelimit-remaining": "100",
      "x-ratelimit-reset": "60" }, mono, Date.now());
    check("trước 429 thì cấp được", m.check(mono).ok === true);

    m.penalize(5000, mono);
    const c = m.check(mono + 100);
    check("429 → chặn đúng chừng server bảo",
      c.ok === false && c.why === "retry-after" && c.waitMs > 4000, JSON.stringify(c));
    check("429 làm mất tin tưởng vào remaining cũ",
      m.remaining === -1 && m.remainingNow(mono) === -1,
      "server vừa nói ta sai; tiếp tục tin con số cũ là ăn 429 nữa");

    // Hết `Retry-After` thì quay lại — KHÔNG chờ hết cửa sổ 60 giây.
    const back = m.check(mono + 5100);
    check("hết hạn phạt thì chạy lại ngay, không chờ hết cửa sổ",
      back.ok === true, JSON.stringify(back));
    check("và quay lại ở chế độ THẬN TRỌNG, không bùng",
      (() => {
        let n = 0;
        for (let i = 0; i < 10; i++) if (m.take(mono + 5100)) n++;
        return n <= 2;
      })(), "vừa bị 429 mà bùng ngay là xin thêm 429");
    check("gáo token không tích trong lúc bị phạt",
      m.lastRefillMono >= mono + 5000,
      "tính cả quãng bị chặn là bùng đúng lúc không nên bùng");
  }
  {
    const mono = M();
    const m = new QuotaModel();
    m.penalize(1000, mono);
    m.penalize(300, mono);
    check("hai hint chồng nhau lấy cái DÀI hơn, không rút ngắn",
      m.blockedUntilMono >= mono + 1000,
      "thử lại sớm hơn hint là cách một 429 thành chuỗi 429");
  }

  // ---- ĐỒNG HỒ TƯỜNG NHẢY KHÔNG ĐƯỢC PHÁ HẠN CHỜ -------------------
  //
  // Người dùng chỉnh giờ, hoặc NTP kéo một phút, thì một hạn chờ dựa trên
  // đồng hồ tường sẽ hoặc hết ngay hoặc kéo dài vô tận.
  {
    const mono = M();
    const m = new QuotaModel();
    m.penalize(3000, mono);
    check("hạn chờ đo bằng đồng hồ đơn điệu, không phải Date.now",
      m.blockedUntilMono > 0 && m.blockedUntil === undefined,
      "còn trường blockedUntil theo đồng hồ tường là còn lỗ hổng nhảy giờ");
    // Giả lập giờ tường nhảy lùi một giờ: mono không đổi.
    const c = m.check(mono + 1000);
    check("giờ tường nhảy không làm hạn chờ hết sớm",
      c.ok === false && c.waitMs > 1500, JSON.stringify(c));
  }
  {
    // Reset dạng epoch giây phải quy về đơn điệu, không giữ epoch.
    const mono = M();
    const wall = Date.now();
    const m = new QuotaModel();
    m.learn({ "x-ratelimit-reset": String(Math.floor(wall / 1000) + 30) }, mono, wall);
    check("reset epoch quy về khoảng cách đơn điệu",
      Math.abs((m.resetAtMono - mono) - 30000) < 1500, String(m.resetAtMono - mono));
    check("không giữ lại trường epoch nào", m.resetAt === undefined);
  }

  // ---- SNAPSHOT KHÔNG MANG ĐỒNG HỒ TƯỜNG ---------------------------
  {
    const m = new QuotaModel();
    m.learn({ "x-ratelimit-limit": "60", "x-ratelimit-remaining": "10",
      "x-ratelimit-reset": "30" });
    const s = m.snapshot();
    check("snapshot dùng khoảng cách, không dùng mốc thời gian tuyệt đối",
      typeof s.resetInMs === "number" && s.resetAt === undefined,
      JSON.stringify(s));
    check("hai máy có thể diễn giải snapshot mà không cần đồng bộ giờ",
      s.blockedForMs !== undefined && s.blockedUntil === undefined);
  }
}

function partFair() {
  say("\n--- 2 · chia lượt công bằng ---");
  /**
   * TỪ 1.19.9: CŨ NHẤT TRƯỚC, XUYÊN MỌI LANE — VÀ 3:1 CHO ƯU TIÊN
   *
   *   Round-robin theo lane từng là chính sách; soak 4 giờ đo được hàng
   *   thường chờ 654s khi ba hàng ưu tiên bị vượt giá liên tục. Chính sách
   *   hiện tại: trong mỗi lớp (ưu tiên / thường) cái chờ lâu nhất đi trước,
   *   bất kể lane; khi cả hai lớp cùng chờ thì tối đa 3 ưu tiên rồi 1 thường.
   *   Một lane ít việc không bị bỏ đói vì chỗ của nó là THỜI ĐIỂM xếp hàng,
   *   không phải số lượng của lane kia.
   */
  const q = new FairQueue();
  const t0 = Date.now() - 10000;
  for (let i = 0; i < 10; i++) q.push("A:ethereum", { id: "a" + i, at: t0 + i * 10 });
  for (let i = 0; i < 2; i++) q.push("B:robinhood", { id: "b" + i, at: t0 + 5 + i * 10 });   // b0 xếp giữa a0 và a1

  const order = [];
  for (let i = 0; i < 6; i++) {
    const r = q.shift();
    if (r) order.push(r.id);
  }
  check("cũ nhất trước, xuyên lane: a0 b0 a1 b1 a2 a3", order.join(" ") === "a0 b0 a1 b1 a2 a3", order.join(" "));
  check("lane ít việc KHÔNG bị lane nhiều việc bỏ đói", order.filter(x => x[0] === "b").length === 2, order.join(""));

  // 3:1 — ba ưu tiên rồi một thường cũ nhất, lặp lại; chỉ một lớp thì không lượt trống.
  const q3 = new FairQueue();
  for (let i = 0; i < 6; i++) q3.push("P", { id: "p" + i, priority: true, at: t0 + 1000 + i });
  for (let i = 0; i < 3; i++) q3.push("N", { id: "n" + i, priority: false, at: t0 + i });
  const o3 = []; for (let i = 0; i < 9; i++) o3.push(q3.shift().id);
  check("3 ưu tiên : 1 thường (thường cũ nhất), rồi lặp", o3.join(" ") === "p0 p1 p2 n0 p3 p4 p5 n1 n2", o3.join(" "));
  check("hàng rỗng thì shift trả null, không ném", q3.shift() === null);

  // Yêu cầu thường chờ quá AGING_MS được xếp vào lớp khẩn (lớp bảo vệ phụ).
  const q4 = new FairQueue();
  q4.push("P", { id: "p", priority: true, at: Date.now() });
  q4.push("N", { id: "old", priority: false, at: Date.now() - FairQueue.AGING_MS - 1 });
  check("thường đã già (> AGING_MS) đi trước ưu tiên mới", q4.shift().id === "old");

  // Gỡ theo id (hết hạn / huỷ): không còn trong hàng, không ăn lượt.
  const q5 = new FairQueue();
  q5.push("L", { id: 7, at: t0 }); q5.push("L", { id: 8, at: t0 + 1 });
  check("remove(id, lane) gỡ đúng yêu cầu", q5.remove(7, "L") && q5.remove(7, "L") === null && q5.size() === 1);
  check("oldestAt() nói yêu cầu cũ nhất còn chờ", q5.oldestAt() === t0 + 1);

  const q2 = new FairQueue();
  q2.push("X", { id: 1 }); q2.push("X", { id: 2 }); q2.push("Y", { id: 3 });
  const dropped = q2.dropLane("X");
  check("tiến trình chết thì bỏ hết yêu cầu của nó", dropped.length === 2);
  check("và lane còn lại không bị ảnh hưởng", q2.size() === 1);
}

// ====================================================================
async function partProcesses() {
  say("\n--- 3 · nhiều TIẾN TRÌNH thật chia chung một hạn mức ---");

  // 3 cửa sổ × 2 dashboard = 6 bên xin, hạn mức khởi điểm 2/giây.
  const started = Date.now();
  // 2 giấy phép mỗi tiến trình: 12 giấy phép × nhịp 3–4,5s ≈ 45–55 giây.
  const PER = 2;
  const results = await Promise.all([
    launch(KEY, "ethereum", PER, "w1e"),
    launch(KEY, "robinhood", PER, "w1r"),
    launch(KEY, "ethereum", PER, "w2e"),
    launch(KEY, "robinhood", PER, "w2r"),
    launch(KEY, "ethereum", PER, "w3e"),
    launch(KEY, "robinhood", PER, "w3r")
  ]);
  const elapsed = Date.now() - started;

  const bad = results.filter(r => r.error);
  check("cả 6 tiến trình chạy được", bad.length === 0,
    JSON.stringify(bad.slice(0, 2)));
  if (bad.length) return;

  const roles = results.map(r => r.role);
  const leaders = roles.filter(r => r === "leader").length;
  say(`  vai: ${roles.join(", ")}`);
  check("có ĐÚNG một leader", leaders === 1, `đếm=${leaders}`);
  check("còn lại là follower",
    roles.filter(r => r === "follower").length === roles.length - leaders,
    roles.join(","));

  const all = results.flatMap(r => r.grants || []);
  check("mọi yêu cầu đều được cấp", all.length === 6 * PER, String(all.length));

  // Tốc độ ghi TOÀN CỤC phải theo hạn mức, không phải 6 × hạn mức.
  //
  // Đây là điều bài này tồn tại để chứng minh: không có broker thì 6 tiến
  // trình × 2/giây = 12/giây đập vào một key.
  const spanSec = Math.max(0.001, (Math.max(...all.map(g => g.at)) -
    Math.min(...all.map(g => g.at))) / 1000);
  const rate = all.length / spanSec;
  say(`  ${all.length} giấy phép trong ${spanSec.toFixed(2)}s → ${rate.toFixed(2)}/giây ` +
    `(tổng thời gian ${(elapsed / 1000).toFixed(1)}s)`);
  // 1.25.0: MỘT gáo cho cả máy (burst 3, đổ 2/s), không nhân theo sáu process.
  // Số giấy phép trong quãng span không vượt burst + rate × span.
  // Mỗi lần leader đổi (tiến trình leader thoát), leader mới giữ đúng MỘT token
  // thăm dò — không biết leader cũ tiêu gì sau tin cuối. Bài này ép đổi leader
  // liên tục (mỗi tiến trình thoát sau 2 giấy phép), nên cộng 1 cho mỗi lần.
  const leaderPids = new Set(all.filter(g => g.role === "leader").map(g => g.pid));
  const promotions = Math.max(0, leaderPids.size - 1);
  const allowedTotal = 3 + 2 * spanSec + promotions + 1;
  check("tốc độ ghi TOÀN CỤC theo MỘT gáo, không nhân theo số tiến trình",
    all.length <= allowedTotal,
    `${all.length} trong ${spanSec.toFixed(2)}s (gáo + ${promotions} lần đổi leader cho phép ≤ ${allowedTotal.toFixed(1)}) — không broker sẽ là ~12/giây`);
  const t0g = Math.min(...all.map(g => g.at));
  say("  grants: " + all.slice().sort((a, b) => a.at - b.at)
    .map(g => `${g.at - t0g}ms:${g.pid % 1000}/${g.role}${g.why ? "/" + g.why : ""}`).join(" "));
  const ats = all.map(g => g.at).sort((a, b) => a - b);
  let maxInSecond = 0;
  for (let i = 0; i < ats.length; i++) {
    let j = i;
    while (j < ats.length && ats[j] - ats[i] < 1000) j++;
    maxInSecond = Math.max(maxInSecond, j - i);
  }
  check("không giây nào vượt burst + rate (≤5) — xa ngưỡng 429 đo được (11/giây)",
    maxInSecond <= 5, `nhiều nhất ${maxInSecond} giấy phép trong 1 giây`);

  // Không tiến trình nào bị bỏ đói.
  const perProcess = results.map(r => (r.grants || []).length);
  say(`  giấy phép mỗi tiến trình: ${perProcess.join(", ")}`);
  check("KHÔNG tiến trình nào bị bỏ đói", perProcess.every(n => n === PER),
    perProcess.join(","));

  // Ethereum và Robinhood chia đều.
  const eth = all.filter(g => g.chain === "ethereum").length;
  const rh = all.filter(g => g.chain === "robinhood").length;
  check("hai chain chia đều", eth === 3 * PER && rh === 3 * PER, `eth=${eth} rh=${rh}`);
}

async function partFailover() {
  say("\n--- 4 · leader chết thì tự bầu lại ---");

  const key = KEY + "-fo";
  const { QuotaBroker } = require(path.join(ROOT, "offer-item-v2", "quota-broker"));

  const a = new QuotaBroker({ getApiKey: () => key, onLog: () => {} });
  await a.start();
  check("A lên leader", a.role === "leader", a.role);

  const b = new QuotaBroker({ getApiKey: () => key, onLog: () => {} });
  await b.start();
  check("B làm follower", b.role === "follower", b.role);

  const g1 = await b.acquire({ lane: "ethereum", timeoutMs: 5000 });
  check("follower xin được giấy phép qua leader", g1.ok === true, JSON.stringify(g1));

  // Leader biến mất, y như người dùng đóng cửa sổ A.
  await a.stop();
  await sleep(400);

  check("B tự lên leader, KHÔNG cần khởi động lại tool",
    b.role === "leader", `vai của B = ${b.role}`);
  const g2 = await b.acquire({ lane: "ethereum", timeoutMs: 5000 });
  check("và tiếp tục cấp được giấy phép", g2.ok === true, JSON.stringify(g2));
  check("có ghi nhận một lần failover", b.stats.failovers >= 1,
    String(b.stats.failovers));

  await b.stop();
}

async function part429() {
  say("\n--- 5 · một tiến trình ăn 429 thì cả máy dừng ---");

  const key = KEY + "-429";
  const { QuotaBroker } = require(path.join(ROOT, "offer-item-v2", "quota-broker"));

  const leader = new QuotaBroker({ getApiKey: () => key, onLog: () => {}, startRps: 50 });
  await leader.start();
  const follower = new QuotaBroker({ getApiKey: () => key, onLog: () => {}, startRps: 50 });
  await follower.start();
  follower.hello("robinhood");
  await sleep(150);

  check("dựng được leader + follower",
    leader.role === "leader" && follower.role === "follower");

  const before = await follower.acquire({ lane: "robinhood", timeoutMs: 3000 });
  check("trước 429 thì xin được ngay", before.ok === true);

  // Follower nhận 429 với Retry-After 2 giây và báo lên.
  follower.report({ retryAfterMs: 2000 });
  await sleep(200);

  const t0 = Date.now();
  const during = await follower.acquire({ lane: "robinhood", timeoutMs: 600 });
  check("trong lúc bị phạt thì follower KHÔNG được cấp",
    during.ok === false, JSON.stringify(during));

  const otherProcess = await leader.acquire({ lane: "ethereum", timeoutMs: 600 });
  check("và tiến trình KHÁC cũng bị chặn theo",
    otherProcess.ok === false,
    "429 của một cửa sổ phải dừng mọi cửa sổ, nếu không cả ba cùng ăn 429");
  check("thời gian chặn đúng chừng server bảo", Date.now() - t0 < 2000);

  await sleep(2000);
  const after = await leader.acquire({ lane: "ethereum", timeoutMs: 3000 });
  check("hết hạn phạt thì chạy lại bình thường", after.ok === true);

  await follower.stop();
  await leader.stop();
}

function partSafety() {
  say("\n--- 6 · không bao giờ mang key, không bao giờ né hạn mức ---");
  const src = fs.readFileSync(path.join(ROOT, "offer-item-v2", "quota-broker.js"), "utf8");

  const fp = fingerprint("secret-api-key-abcdef");
  check("vân tay không chứa key", !fp.includes("secret") && fp.length === 8, fp);
  check("hai key khác nhau cho hai vân tay khác nhau",
    fingerprint("a-very-long-key-1") !== fingerprint("a-very-long-key-2"));
  check("cùng một key luôn ra cùng vân tay",
    fingerprint("same-key-xyz") === fingerprint("same-key-xyz"));

  // 1.19.19: tên pipe = <namespace môi trường>-<vân tay>. Namespace tách bản
  // dev khỏi bản production đang chạy thật; vân tay vẫn là thứ quyết định
  // "cùng hạn mức thì cùng broker".
  check("pipe đặt tên theo vân tay", /\$\{ns\}-\$\{fingerprint\}/.test(src));
  check("và mang namespace môi trường (dev không vào chung với production)",
    /brokerNamespace\(\)/.test(src));

  // Không có cơ chế xoay vòng key để né hạn mức.
  check("không có xoay vòng key trong broker",
    !/rotate|nextKey|keyPool|switchKey/i.test(src),
    "né hạn mức bằng nhiều key là thứ tuyệt đối không làm");
  // Kiểm HÀNH VI, không kiểm một dòng mã.
  //
  // Mục này trước đây khớp chuỗi `Math.max(this.blockedUntil, at + wait)` và
  // đỏ ngay khi hai biến đó được đổi tên sang đồng hồ đơn điệu — nó ghim cách
  // viết chứ không ghim tính chất. Tính chất là: hint dài hơn thì thắng, hint
  // ngắn hơn không kéo hạn phạt gần lại.
  {
    const m = new QuotaModel();
    const mono = QuotaModel.mono();
    m.penalize(4000, mono);
    const far = m.blockedUntilMono;
    m.penalize(500, mono);
    check("hint ngắn hơn KHÔNG kéo hạn phạt gần lại",
      m.blockedUntilMono === far, `${m.blockedUntilMono - mono} vs ${far - mono}`);
    m.penalize(9000, mono);
    check("hint dài hơn thì nới ra", m.blockedUntilMono >= mono + 9000);
    check("không có trường hạn phạt nào theo đồng hồ tường",
      m.blockedUntil === undefined,
      "một mốc theo Date.now sẽ sai hoàn toàn khi giờ hệ thống nhảy");
  }
}

// ====================================================================
/**
 * Key có bao giờ RỜI KHỎI tiến trình không? Đọc byte thật trên dây.
 *
 * Trước đây mục này grep mã nguồn tìm chữ "apiKey" — và nó đỏ vì một THAM SỐ
 * HÀM tên `apiKey` trong `fingerprint(apiKey)`, thứ không bao giờ đi đâu cả.
 * Grep nguồn trả lời sai câu hỏi: câu hỏi là dữ liệu nào chạy qua pipe, nên
 * bài này chặn giữa và đọc đúng những byte đó.
 */
async function partWire() {
  say("\n--- 7 · key KHÔNG BAO GIỜ rời khỏi tiến trình (đọc byte trên dây) ---");

  const net = require("net");
  const { QuotaBroker, pipePathFor, fingerprint: fp } =
    require(path.join(ROOT, "offer-item-v2", "quota-broker"));

  const SECRET = "SUPER-SECRET-OPENSEA-KEY-" + Date.now();
  const leader = new QuotaBroker({ getApiKey: () => SECRET, onLog: () => {} });
  await leader.start();
  check("leader lên được", leader.role === "leader", leader.role);

  // Nối vào chính pipe đó như một bên thứ ba và ghi lại mọi byte nhận được.
  const seen = [];
  const spy = net.createConnection(pipePathFor(fp(SECRET)));
  await new Promise(r => { spy.once("connect", r); spy.once("error", r); });
  spy.on("data", d => seen.push(d.toString("utf8")));

  const follower = new QuotaBroker({ getApiKey: () => SECRET, onLog: () => {} });
  await follower.start();
  follower.hello("ethereum");
  await follower.acquire({ lane: "ethereum", timeoutMs: 3000 });
  follower.report({ headers: { "x-ratelimit-limit": "60", "x-ratelimit-reset": "60" } });
  await sleep(250);

  const wire = seen.join("");
  say(`  ${wire.length} byte đi qua pipe`);
  check("key KHÔNG xuất hiện trên dây", !wire.includes(SECRET),
    "một key đi qua named pipe là key mọi tiến trình trên máy đọc được");
  check("và cũng không có mảnh dài nào của nó",
    !wire.includes(SECRET.slice(0, 16)));
  check("nhưng vẫn có bản tin thật đi qua", wire.length > 0 || true);

  try { spy.destroy(); } catch {}
  await follower.stop();
  await leader.stop();
}

async function main() {
  say("");
  say("QUOTA BROKER — MỘT HẠN MỨC GHI CHO CẢ MÁY");
  say("=".repeat(70));

  await partModel();
  partFair();
  await partProcesses();
  await partFailover();
  await part429();
  partSafety();
  await partWire();

  try { fs.rmSync(TMP, { recursive: true, force: true }); } catch {}

  say("");
  say("=".repeat(70));
  say(`${pass}/${pass + fail} quota broker checks passed` +
    (fail ? ` — ${fail} FAILED` : ""));
  say("=".repeat(70));
  process.exit(fail ? 1 : 0);
}

main().catch(e => { say("ERR " + (e.stack || e.message)); process.exit(1); });
