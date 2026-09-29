"use strict";

/**
 * bridge.js — V2 mặc đúng bộ đồ mà `main.js` đang mong đợi.
 *
 * VÌ SAO CÓ MỘT LỚP CẦU NỐI THAY VÌ SỬA main.js
 *
 *   `main.js` gọi 22 method trên engine Offer Item, và nó cũng là nơi chứa
 *   Offer SLL, Offer Custom, Cancel, Settings, License và auto-update — toàn
 *   những thứ v1.19.4 vừa sửa xong và KHÔNG được đụng tới. Viết lại chỗ gọi
 *   sẽ chạm vào file đó ở hàng chục điểm.
 *
 *   Nên V2 nhận đúng hình dạng cũ ở bề mặt, và bên trong là kiến trúc mới.
 *   Đổi engine trở thành đổi MỘT dòng `new`, và mọi tab khác không biết có gì
 *   thay đổi.
 *
 * CẦU NỐI KHÔNG ĐƯỢC CHỨA LOGIC
 *
 *   Nó dịch tên gọi và hình dạng dữ liệu, không quyết định gì. Mọi quyết định
 *   nằm trong `engine-v2.js`. Một cầu nối bắt đầu "xử lý thêm một chút" là
 *   một engine thứ ba mà không ai biết mình đang bảo trì.
 */

const { ethers } = require("ethers");

const { OfferItemEngineV2, STATE } = require("./engine-v2");
const { tokenKey } = require("./event-normalizer");
const { decide, STATUS: VERDICT } = require("./decision");
const { normalizeMaxDown, effectiveStepFor } = require("./pricing");
const { isPlaceholderName, pickName } = require("./nft-name");

/** OpenSea metadata: giải lúc gọi (lười, để smoke vá được). */
let openseaModule = null;
function openseaClient() {
  if (!openseaModule) openseaModule = require("../opensea");
  return openseaModule;
}

/**
 * LÀM MỚI TÊN NFT — VIỆC NỀN, KHÔNG CHẠM ĐƯỜNG NÓNG
 *
 *   Hàng có tên là chỗ giữ (pre-reveal, "#id", rỗng) được hỏi lại: Alchemy
 *   với `refreshCache` (đọc lại từ token URI), rồi OpenSea metadata (thường
 *   cập nhật sớm hơn sau reveal). Lùi dần 5' → 15' → 30' → 60' cho hàng vẫn
 *   là chỗ giữ; hàng đã có tên thật kiểm lại mỗi 24h. Không có nhịp quét
 *   liên tục, không REST trên đường Stream → quyết định → gửi.
 */
const NAME_TTL_MS = 24 * 60 * 60 * 1000;
const NAME_RETRY_MS = [5, 15, 30, 60].map(m => m * 60 * 1000);
const NAME_SWEEP_MS = 10 * 60 * 1000;
const NAME_META_PER_PASS = 20;

/** Max của người dùng về lưới giá, làm tròn XUỐNG. Rỗng/không hợp lệ giữ nguyên. */
function gridMax(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? normalizeMaxDown(n) : value;
}

/**
 * Giải tên ở THỜI ĐIỂM GỌI, không destructure lúc require.
 *
 * Bộ smoke test vá thẳng lên object module (`opensea.fetchBestOffer = …`)
 * TRƯỚC khi main.js nạp, và đó là cách toàn bộ phần còn lại của app được kiểm
 * end-to-end mà không cần mạng. Destructure lúc require giữ tham chiếu tới
 * hàm GỐC, nên bản vá không có tác dụng — và Offer Item sẽ là phần duy nhất
 * của app không kiểm được bằng smoke.
 *
 * Giữ một cách viết cho cả cây nguồn rẻ hơn dựng một cơ chế tiêm phụ thuộc
 * riêng cho V2.
 */
const chainAdapters = require("./chain-adapters");

/** Cùng lý do lười như trên: giải lúc gọi để bản vá của smoke có tác dụng. */
let nftSourceModule = null;
function nftSource() {
  if (!nftSourceModule) nftSourceModule = require("../nft-source");
  return nftSourceModule;
}

/** Trạng thái hàng, đúng tên mà giao diện đang đọc. */
const STATUS = Object.freeze({
  IDLE: "IDLE",
  RUNNING: "RUNNING",
  PAUSED: "PAUSED",
  /**
   * Ví không đủ WETH. Giao diện đã có sẵn câu cho nó — "Không đủ số dư" — và
   * đó là một câu người dùng hành động được, khác hẳn "Lỗi" hay "HTTP 400".
   */
  LOW_BALANCE: "LOW_BALANCE",
  ERROR: "ERROR"
});

/**
 * Gộp tín hiệu đổi của engine thành một lần đẩy bảng.
 *
 *   Một khung hình (~16ms) là đủ để mắt thấy "ngay"; 120ms của bản trước
 *   là thấy được. Renderer chỉ vẽ ô nào đổi, nên chi phí của một lần đẩy là
 *   IPC + so sánh, không phải vẽ lại bảng. Hot path của engine không đi qua
 *   đây — đây chỉ là lịch đẩy ra màn hình.
 */
const UI_COALESCE_MS = 16;

/**
 * Nhịp đẩy "vẫn sống" khi engine chạy mà không có gì đổi (Stream im, chợ
 * vắng). Không phải nhịp quét; chỉ để renderer có mốc tươi mà vẽ hoạt ảnh
 * đúng sự thật. 3 giây, unref, một cho mỗi cầu nối.
 */
const HEARTBEAT_MS = 3000;

class OfferItemV2Bridge {
  /**
   * @param {object} o
   * @param {string} o.chain
   * @param {object} o.db
   * @param {object} o.quota      QuotaBroker dùng chung cả máy
   * @param {(payload:object)=>void} [o.onUpdate]
   * @param {(line:string)=>void} [o.onLog]
   * @param {() => string} o.getApiKey
   */
  constructor({ chain, db, quota, onUpdate, onLog, getApiKey, getApiKeys, label } = {}) {
    this.chain = chain;
    this.label = label || String(chain || "").toUpperCase();
    this.db = db;
    this.onUpdate = typeof onUpdate === "function" ? onUpdate : () => {};
    this.onLog = typeof onLog === "function" ? onLog : () => {};

    this.adapter = chainAdapters.adapterFor(chain, { onLog: this.onLog });
    this.engine = new OfferItemEngineV2({
      adapter: this.adapter,
      quota,
      getApiKey,
      getApiKeys,
      onLog: this.onLog,
      onUpdate: () => this.emitNow(),
      /**
       * Nguồn trait: metadata OpenSea qua `nft-source` (cùng nguồn tên/ảnh).
       * Giải ở thời điểm gọi để smoke vá được; nguồn hỏng thì engine chỉ mất
       * trait — không mất Offer Item.
       */
      traitSource: {
        getNFTs: (chain, items) => nftSource().getNFTs(chain, items),
        readAttributes: nft => nftSource().readAttributes(nft)
      }
    });

    /** url -> hàng, đúng hình dạng giao diện đang vẽ. */
    this.nfts = new Map();
    this.settings = {};
    this.privateKey = "";
    this.running = false;
    /** Lần cuối người dùng bấm Start TẤT CẢ — Add đang chạy dùng để tự chạy hàng mới (1.25.1). */
    this.lastStartAllAt = 0;

    /** Batch lock của Cancel/Clean-cancel. Giữ nguyên ngữ nghĩa v1.19.4. */
    this.batchLocked = new Set();

    /**
     * DỪNG SAU KHI HUỶ SẠCH — KHÔNG PHẢI TẠM DỪNG
     *
     *   "Huỷ hết" là hai lời hứa: rút hết order, và DỪNG mạng này. Lời hứa
     *   thứ hai không phụ thuộc vào lời hứa thứ nhất — một engine còn chạy
     *   sau một lượt huỷ hỏng sẽ đặt offer mới bằng đúng cái ví người dùng
     *   vừa dọn, và đó là kết cục duy nhất họ không hoàn tác được.
     *
     *   `main.js` đọc cờ này TRỰC TIẾP (`requireChainRunning`) để chặn Offer
     *   SLL và các lượt huỷ chạy tiếp trên một mạng đã dừng, và `getCounters`
     *   đẩy nó ra để bảng vẽ banner. Bản cắt sang V2 bỏ sót cả hai, nên
     *   `getEngine(chain).haltForCleanCancel()` ném TypeError ngay lúc người
     *   dùng bấm Huỷ hết.
     */
    this.haltedByLicense = false;
    this.licenseHaltReason = "";

    /**
     * Bộ nhớ đệm offer của chính ví này, do `main.js` đọc VÀ GHI
     * (`engine.myOffersMap = …`). Nó thuộc về luồng Cancel, không phải V2 —
     * nhưng nó phải sống ở đây, vì đó là chỗ luồng Cancel để nó.
     */
    this.myOffersMap = null;

    /** Min/Max/Step/Duration mặc định của chain, do `configure()` đặt. */
    this.chainConfig = { minPrice: 0, maxPrice: 0, step: 0, duration: 15 };

    /** Hàm báo sức khoẻ Stream, do `attachStreamHealth()` gắn. */
    this.streamHealth = null;

    this.dirty = false;

    /**
     * ĐẨY BẢNG THEO SỰ KIỆN, GỘP TRONG MỘT KHUNG NGẮN
     *
     *   Engine gọi `onChange` mỗi khi sổ/ý định đổi — hàng nghìn lần một
     *   giây khi collection sôi động. Mỗi lần đẩy cả bảng qua IPC là cách
     *   renderer đứng hình. Nên tín hiệu đầu tiên hẹn MỘT lần đẩy sau
     *   UI_COALESCE_MS; mọi tín hiệu trong quãng đó nhập vào lần đẩy ấy.
     */
    this.emitTimer = null;
    this.engine.onChange = () => this.scheduleEmit();
    this.heartbeatTimer = setInterval(() => { if (this.running) this.scheduleEmit(); }, HEARTBEAT_MS);
    if (this.heartbeatTimer.unref) this.heartbeatTimer.unref();

    /** Làm mới tên NFT ở nền: hẹn ngắn sau khi thêm hàng, quét nhẹ định kỳ. */
    this.nameTimer = null;
    this.nameRefreshing = false;
    this.nameSweep = setInterval(() => this.refreshNames("sweep").catch(() => {}), NAME_SWEEP_MS);
    if (this.nameSweep.unref) this.nameSweep.unref();

    /**
     * NIÊM PHONG: KHÔNG AI ĐƯỢC GẮN THÊM THUỘC TÍNH VÀO ĐÂY
     *
     *   JavaScript cho phép `engine.somethingNew = x` ở bất cứ đâu, và đó
     *   chính là cách `myOffersMap` xuất hiện: một chỗ trong luồng Cancel ghi
     *   ngược một Map vào engine, không có khai báo, không có ai sở hữu. Nó
     *   chạy được với engine cũ vì engine cũ cũng chẳng khai báo gì — nên
     *   không ai phát hiện ra rằng đó là một phần của hợp đồng.
     *
     *   `Object.seal` giữ mọi thuộc tính ĐÃ KHAI BÁO ở trên vẫn ghi được
     *   (`myOffersMap`, `running`, `haltedByCleanCancel`… đều là trạng thái
     *   thật), nhưng một thuộc tính MỚI sẽ ném lỗi ngay tại dòng ghi trong
     *   chế độ strict — tại chỗ, có stack, thay vì im lặng rồi hỏng ở một
     *   nơi khác sáu tháng sau.
     *
     *   Muốn thêm trạng thái mới thì khai báo nó ở đây. Đó là cái giá, và nó
     *   rẻ hơn nhiều so với việc đi tìm một hợp đồng ngầm.
     */
    Object.seal(this);
  }

  /**
   * Địa chỉ ví đang ký. Suy từ chính bộ ký, không lưu riêng.
   *
   * `main.js` đọc nó ở sáu chỗ (báo cáo, Cancel, Offer SLL). Trả "" khi chưa
   * Start là đúng: chưa có Private Key thì chưa có ví, và một địa chỉ đoán ra
   * từ cấu hình cũ sẽ khiến luồng Cancel đi tìm offer của một ví khác.
   */
  get walletAddress() {
    /**
     * BIẾT VÍ TỪ LÚC CÓ KEY, KHÔNG PHẢI TỪ LÚC START
     *
     *   Engine cũ dẫn xuất địa chỉ ngay trong `configure()`. Bản đầu của cầu
     *   nối chỉ trả địa chỉ khi bộ ký đã dựng — tức là sau Start — nên mọi
     *   thứ hỏi ví TRƯỚC khi bấm Start nhận về rỗng: báo cáo, kiểm tra khôi
     *   phục credential sau update, luồng Cancel mở trước Offer Item.
     *
     *   Đo được: hai suite trên bản đóng gói đỏ với cùng một dòng
     *   "ví vẫn khôi phục được :: null" — key khôi phục đúng, chỉ là chưa ai
     *   Start nên cầu nối chưa chịu nói ví là gì.
     *
     *   \`null\` khi chưa có key, đúng kiểu engine cũ trả: nơi gọi phân biệt
     *   "chưa có ví" với "ví rỗng" bằng chính giá trị đó.
     */
    if (this.engine.builder) return this.engine.builder.address;
    if (!this.privateKey) return null;
    try {
      return new ethers.Wallet(this.privateKey).address;
    } catch {
      return null;
    }
  }

  // ---- cấu hình -----------------------------------------------------

  configure(settings = {}) {
    this.settings = settings || {};
    this.privateKey = String(settings.privateKey || "");
    const chainCfg = settings[this.chain] || {};
    this.chainConfig = {
      minPrice: Number(chainCfg.minPrice),
      maxPrice: gridMax(Number(chainCfg.maxPrice)),
      step: Number(chainCfg.step),
      duration: Number(chainCfg.duration) || 15
    };
    return { ok: true };
  }

  /** Replace a wallet without permitting an old build, queue entry or HTTP
   * request to cross the identity boundary. Running rows are cold-started from
   * the new account only after every old flight was aborted. */
  async rebindWallet(settings = {}) {
    const active = [...this.nfts.values()].filter(row => row.running).map(row => row.url);
    this.engine.reset("wallet-changed");
    this.running = false;
    this.configure(settings);
    for (const row of this.nfts.values()) {
      row.running = false;
      row.status = STATUS.PAUSED;
      row.target = 0;
      // Errors/status produced under the OLD wallet do not follow the row to
      // the new one (1.25.12). Min/Max/Step/Duration and a user Pause stay.
      row.lastError = "";
    }
    if (!active.length || !this.privateKey) { this.emitNow(); return { ok: true, resumed: 0 }; }
    return this.start(active);
  }

  /**
   * Sửa Min/Max/Step/Duration của MỘT hàng, hoặc của cả chain.
   *
   * HAI ĐỐI SỐ, KHÔNG PHẢI MỘT
   *
   *   `main.js` gọi `applyConfigPatch(url, patch)`. Bản đầu của cầu nối khai
   *   báo `applyConfigPatch(patch)` — nên nó nhận chuỗi URL làm patch, gán
   *   `Object.assign(row, "https://…")` lên MỌI hàng, và giá người dùng vừa gõ
   *   không bao giờ tới nơi. Bộ smoke bắt được: gõ 0.15 rồi Enter, đọc lại vẫn
   *   là 0.1.
   *
   *   `url` rỗng nghĩa là áp cho cả chain — đó là đường mà thay đổi mặc định
   *   của dashboard đi qua.
   */
  applyConfigPatch(url, patch = {}) {
    // Empty URL is the chain default, not a broadcast mutation. Row duration
    // is per-NFT and must never be overwritten by a later global default.
    if (!url && patch && Object.prototype.hasOwnProperty.call(patch, "duration")) {
      this.defaultDuration = Math.max(1, Number(patch.duration) || 15);
      return { ok: true, updated: 0, defaultDuration: this.defaultDuration };
    }
    const clean = {};
    for (const field of ["minPrice", "maxPrice", "step", "duration"]) {
      if (patch[field] !== undefined) clean[field] = patch[field];
    }
    // Max về lưới giá (xuống). Giao diện đọc lại hàng và hiện đúng giá đã chỉnh.
    if (clean.maxPrice !== undefined) clean.maxPrice = gridMax(clean.maxPrice);
    // Ô tick Priority của bảng đi qua đúng đường này. Bỏ sót nó nghĩa là
    // người dùng tick xong, ô tự bật lại, và không có gì xảy ra.
    if (patch.priorityMode !== undefined) {
      clean.priorityMode = Boolean(patch.priorityMode);
    }

    const rows = url ? [this.nfts.get(url)].filter(Boolean)
      : [...this.nfts.values()];
    if (url && !rows.length) return { ok: false, error: "Khong tim thay NFT." };

    for (const row of rows) {
      Object.assign(row, clean);
      // Engine giữ bản sao riêng để đường nóng không phải tra ngược qua URL.
      this.engine.patchRow(
        tokenKey(this.chain, row.contract, row.tokenId), clean);
    }

    if (!url) this.chainConfig = { ...this.chainConfig, ...clean };
    this.markDirty();
    return { ok: true, updated: rows.length };
  }

  attachLicenceGate(gate) {
    this.engine.attachLicenceGate(gate);
    return { ok: true };
  }

  attachStreamHealth(fn) {
    this.streamHealth = fn;
    // Engine cần biết Stream còn sống không: khi nó chết, engine đọc lại qua
    // REST cho tới khi nó về. Không có thông tin này thì engine mù.
    this.engine.attachStreamHealth(fn);
    return { ok: true };
  }

  // ---- danh sách NFT -------------------------------------------------

  addRow(row) {
    if (!row || !row.url) return { ok: false, error: "thiếu url" };
    this.nfts.set(row.url, {
      url: row.url,
      chain: this.chain,
      contract: String(row.contract || "").toLowerCase(),
      tokenId: String(row.tokenId || ""),
      // Không bao giờ vẽ một chỗ giữ như "preReveal": tên thật, hoặc collection + #id.
      name: pickName({ name: row.name, collectionName: row.collection, tokenId: row.tokenId }).name,
      image: row.image || "",
      imageAlts: row.imageAlts || [],
      collection: row.collection || "",
      collectionSlug: String(row.collectionSlug || "").toLowerCase(),
      traits: row.traits || [],
      /**
       * Snapshot offer đã đọc lúc Add (đường `bot:addNfts` đọc cả danh sách).
       * Giữ ở hàng để lần `start`/`addRows` kế tiếp đưa thẳng vào sổ — không
       * đọc lại thứ vừa đọc, và không đóng cổng lượt-đọc-đầu vô ích.
       */
      seed: row.seed || null,
      priorityMode: Boolean(row.priorityMode),
      minPrice: row.minPrice ?? this.chainConfig?.minPrice,
      maxPrice: gridMax(row.maxPrice ?? this.chainConfig?.maxPrice),
      step: row.step ?? this.chainConfig?.step,
      duration: row.duration ?? this.chainConfig?.duration,
      // Hàng mới thêm KHÔNG tự chạy, kể cả khi engine đang chạy — đó là
      // ràng buộc của spec 22. PAUSED nói đúng điều đó: tồn tại, chưa chạy,
      // bấm Start là chạy.
      status: STATUS.PAUSED,
      running: false,
      best: 0, mine: 0, target: 0,
      bestKind: "", myOrderHash: "",
      lastError: "",
      /** Tên là chỗ giữ? Lần kiểm tên gần nhất, số lần đã hỏi lại. */
      namePlaceholder: isPlaceholderName(row.name, row.tokenId),
      nameCheckedAt: 0,
      nameChecks: 0
    });
    this.markDirty();
    this.scheduleNameRefresh("add");

    /**
     * TRẢ VỀ CHÍNH ĐỐI TƯỢNG HÀNG, KHÔNG PHẢI MỘT BIÊN LAI
     *
     *   `main.js` viết:
     *
     *       const runtime = engine.addRow(stored.nft);
     *       if (best?.ok) runtime.best = best.price;
     *       if (mine) runtime.mine = mine.price;
     *
     *   Engine cũ trả về hàng thật nên hai phép gán đó tới đúng chỗ. Bản cắt
     *   đầu của cầu nối trả `{ok:true}`, nên chúng ghi vào một object dùng
     *   một lần rồi vứt — và ô Best Offer của mọi NFT vừa thêm hiện dấu gạch,
     *   mãi mãi, cho tới khi có một sự kiện Stream chạm đúng token đó.
     *
     *   Bộ kiểm hợp đồng không thấy: `runtime` không phải một biến giữ
     *   engine, nó là GIÁ TRỊ TRẢ VỀ của một method. Đó là dạng phụ thuộc thứ
     *   ba, sau "gọi method" và "đọc trường dữ liệu".
     */
    return this.nfts.get(row.url);
  }

  removeRow(url) {
    const row = this.nfts.get(url);
    if (!row) return false;
    this.nfts.delete(url);
    this.engine.removeRow(tokenKey(this.chain, row.contract, row.tokenId));
    this.markDirty();
    return true;
  }

  resolveTargets(urls) {
    if (!urls || !urls.length) return [...this.nfts.values()];
    const want = new Set(urls.map(String));
    return [...this.nfts.values()].filter(r => want.has(r.url));
  }

  collectionSlugs() {
    const out = new Set();
    for (const row of this.nfts.values()) {
      if (row.collectionSlug) out.add(row.collectionSlug);
    }
    return [...out];
  }

  // ---- vòng đời ------------------------------------------------------

  async start(urls = null) {
    if (!urls) this.lastStartAllAt = Date.now();
    this.scheduleNameRefresh("start");
    const targets = this.resolveTargets(urls);
    if (!targets.length) return { ok: false, error: "Khong co NFT nao de chay." };
    if (!this.privateKey) return { ok: false, error: "Thieu Private Key trong Settings." };

    // Start là cách duy nhất gỡ một lượt dừng — và Start tự nó đã qua cổng
    // licence, nên một máy bị treo licence không thể chỉ bấm nút là chạy lại.
    this.haltedByLicense = false;
    this.licenseHaltReason = "";

    for (const row of targets) {
      row.running = true;
      row.status = STATUS.RUNNING;
      row.lastError = "";
    }

    /**
     * KHỞI ĐỘNG HỎNG THÌ HÀNG KHÔNG ĐƯỢC TRÔNG NHƯ ĐANG CHẠY
     *
     *   Hàng được đặt `running = true` TRƯỚC khi engine khởi động, để giao
     *   diện phản hồi ngay. Nhưng nếu engine thất bại — hoặc ném — mà không
     *   hoàn tác, thì bảng hiện "Đang theo dõi" cho một engine chưa hề chạy,
     *   và người dùng đợi một thứ sẽ không bao giờ tới. Đo được trên bản đóng
     *   gói: sáu hàng ACTIVE, không target, không best, không cả lỗi.
     */
    const shape = r => ({
      url: r.url, contract: r.contract, tokenId: r.tokenId,
      collectionSlug: r.collectionSlug, traits: r.traits,
      priorityMode: r.priorityMode,
      minPrice: r.minPrice, maxPrice: r.maxPrice,
      step: r.step, duration: r.duration,
      // Chỉ dùng MỘT lần: sau khi vào sổ, snapshot không còn là sự thật.
      seed: (() => { const s = r.seed; r.seed = null; return s; })()
    });

    /**
     * ENGINE ĐANG CHẠY → THÊM/CHẠY LẠI TĂNG DẦN, KHÔNG RESTART
     *
     *   `engine.start()` thay toàn bộ bảng hàng bằng danh sách được truyền.
     *   Gọi nó với chỉ hàng vừa thêm (như main.js làm khi Add lúc đang chạy)
     *   là cách mọi NFT cũ rời khỏi engine trong im lặng — sổ còn, Best còn
     *   hiện, quyết định thì không. Đo trên sản phẩm: sau hai lần Add, #4951
     *   và #1172 "Đang xử lý" mãi mà không có DECISION nào; log "START · 1
     *   NFT" là dấu vết. Engine đang chạy thì đi qua `addRows`. Chỉ khi
     *   engine chưa chạy mới `start()` — và với TOÀN BỘ hàng đang bật.
     */
    let result;
    try {
      /**
       * BẮT ĐẦU SAU KHI TẠM DỪNG = RESUME, KHÔNG PHẢI START LẠI
       *
       *   Tạm dừng không tháo engine (xem `pause`): sổ, own, template, trait
       *   và Stream vẫn sống. Nhưng `start()` không có `urls` vẫn đi đường
       *   lạnh — `engine.start` xoá bảng hàng, prewarm lại mọi collection,
       *   đóng cổng lượt-đọc-đầu cho TẤT CẢ và đọc lại từng token. Với 80 NFT
       *   đó là hàng chục lượt REST và nhiều chục giây trước khi hàng đầu tiên
       *   chạy lại. Engine còn ấm thì bật lại quyền ghi và tính từ sổ hiện tại.
       */
      const warm = this.engine.state === STATE.RUNNING && this.engine.builder &&
        targets.every(r => this.engine.rows.has(tokenKey(this.chain, r.contract, r.tokenId)));
      if (warm) {
        result = this.engine.resumeRows(targets.map(shape));
      } else if (this.running && this.engine.state === STATE.RUNNING && urls) {
        result = await this.engine.addRows(targets.map(shape));
      } else {
        const all = [...this.nfts.values()].filter(r => r.running);
        result = await this.engine.start({ privateKey: this.privateKey, rows: all.map(shape) });
      }
    } catch (error) {
      result = { ok: false, error: String(error && error.message || error) };
    }

    if (!result || result.ok !== true) {
      const why = (result && result.error) || "Không khởi động được engine.";
      for (const row of targets) {
        row.running = false;
        row.status = STATUS.ERROR;
        row.lastError = why;
      }
      this.running = false;
      this.onLog(`[V2 ${this.label}] START THẤT BẠI: ${why}`);
      this.emitNow();
      return { ok: false, error: why };
    }

    this.running = true;
    this.emitNow();
    // Số liệu của đường ấm (resumed/refreshed) đi tiếp ra ngoài: giao diện và
    // bài kiểm phân biệt được "resume" với "start lạnh".
    return { ok: true, started: targets.length,
      ...(result && result.resumed !== undefined
        ? { resumed: result.resumed, refreshed: result.refreshed, warm: true } : {}) };
  }

  pause(urls = null) {
    for (const row of this.resolveTargets(urls)) {
      row.running = false;
      row.status = STATUS.PAUSED;
      const key = tokenKey(this.chain, row.contract, row.tokenId);
      this.engine.suspendRow(key);
    }
    this.markDirty();
    return { ok: true, paused: this.resolveTargets(urls).length };
  }

  stop(urls = null) {
    const targets = this.resolveTargets(urls);
    for (const row of targets) {
      row.running = false;
      row.status = STATUS.PAUSED;
      row.target = 0;
      const key = tokenKey(this.chain, row.contract, row.tokenId);
      this.engine.suspendRow(key);
    }
    if (!urls || !urls.length) { this.engine.stop(); this.running = false; }
    this.markDirty();
    return { ok: true, stopped: targets.length };
  }

  togglePause(url) {
    const row = this.nfts.get(url);
    if (!row) return { ok: false, error: "Khong tim thay NFT." };
    return row.running ? this.pause([url]) : this.start([url]);
  }

  /** Reset = dừng khẩn cấp. Giữ đúng ngữ nghĩa v1.19.4. */
  emergencyStop(reason = "reset") {
    const out = this.engine.reset(reason);
    for (const row of this.nfts.values()) {
      row.running = false;
      row.status = STATUS.IDLE;
      row.target = 0;
    }
    this.running = false;
    this.emitNow();
    return { ok: true, chain: this.chain, epoch: out.epoch,
      stopped: this.nfts.size };
  }

  haltForLicense(reason) {
    this.engine.reset("licence");
    this.running = false;
    this.haltedByLicense = true;
    this.licenseHaltReason = String(reason || "License không hợp lệ.");
    for (const row of this.nfts.values()) {
      row.running = false;
      row.status = STATUS.IDLE;
      row.lastError = reason || "licence";
    }
    this.emitNow();
    return { ok: true, chain: this.chain, halted: true };
  }

  shutdown() {
    this.running = false;
    if (this.emitTimer) clearTimeout(this.emitTimer);
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    if (this.nameTimer) clearTimeout(this.nameTimer);
    if (this.nameSweep) clearInterval(this.nameSweep);
    this.emitTimer = this.heartbeatTimer = this.nameTimer = this.nameSweep = null;
    this.engine.shutdown();
  }

  // ---- stream --------------------------------------------------------

  handleStreamEvent(event) { this.engine.handleStreamEvent(event); }
  /** A joined collection topic failed; start scoped recovery immediately. */
  onTopicUnavailable(slug, at, reason) {
    try { return this.engine.onTopicUnavailable(slug, at, reason); } catch { return 0; }
  }
  /** Kênh Stream của một collection vừa im lặng/rejoin: kiểm tra lại collection đó một lần. */
  onTopicGap(slug) {
    try { return this.engine.onTopicGap(slug); } catch { return 0; }
  }
  /** First join ACK of a collection topic: re-read rows read before it (1.25.12). */
  onTopicJoined(slug, joinedAt) {
    try { return this.engine.firstJoinRecheck(slug, joinedAt); } catch { return 0; }
  }
  onStreamReconnect() {
    // Tên/ảnh không liên quan tới khoảng mù Stream — không kéo metadata theo.
    this.engine.onStreamReconnect();
  }

  /** Hẹn một lượt làm mới tên sau 2 giây; nhiều lần gọi gộp thành một. */
  scheduleNameRefresh(reason) {
    if (this.nameTimer) return;
    const timer = setTimeout(() => {
      this.nameTimer = null;
      this.refreshNames(reason).catch(error => this.onLog(`[NFT NAME] ${reason}: ${error.message}`));
    }, 2000);
    if (timer.unref) timer.unref();
    this.nameTimer = timer;
  }

  /** Hàng nào tới lượt hỏi lại tên. */
  nameDue(row, now, reason) {
    const since = now - (row.nameCheckedAt || 0);
    if (row.namePlaceholder) {
      if (reason === "add" || reason === "start") return row.nameCheckedAt === 0 || since > NAME_RETRY_MS[0];
      const wait = NAME_RETRY_MS[Math.min(NAME_RETRY_MS.length - 1, row.nameChecks || 0)];
      return since > wait;
    }
    // Tên thật đã có: KHÔNG làm mới định kỳ (1.25.0). Metadata là P3 — một
    // tên tốt không đáng một request mỗi 24h cho mỗi NFT.
    return false;
  }

  /**
   * Hỏi lại tên cho các hàng tới lượt. Alchemy theo lô (refreshCache), rồi
   * OpenSea metadata cho những hàng vẫn là chỗ giữ (tối đa NAME_META_PER_PASS
   * mỗi lượt). Tên đổi → hàng + db + đẩy bảng. Mọi lỗi chỉ ghi log.
   */
  async refreshNames(reason = "sweep") {
    if (this.nameRefreshing) return { skipped: true };
    /**
     * METADATA NHƯỜNG REALTIME
     *
     *   Có SEND đang chờ/bay, vừa 429, hay recovery đang có hàng đợi: bỏ lượt
     *   này — lượt quét kế tiếp (10 phút) hoặc lần thêm hàng sau sẽ làm.
     *   Tên/ảnh không bao giờ quan trọng hơn một offer.
     */
    let busy = false;
    try {
      busy = this.engine.realtimePressure() ||
        (this.engine.recovery && this.engine.recovery.queue.length > 0);
    } catch { busy = false; }
    if (busy && reason !== "add") return { skipped: true, busy: true };
    this.nameRefreshing = true;
    const now = Date.now();
    const due = [...this.nfts.values()].filter(r => r.contract && r.tokenId && this.nameDue(r, now, reason));
    let updated = 0, asked = 0;
    try {
      if (!due.length) return { updated, asked };
      for (const r of due) { r.nameCheckedAt = now; r.nameChecks = (r.nameChecks || 0) + 1; }

      const apply = (r, candidate, collectionName, image, imageAlts = []) => {
        const picked = pickName({ name: candidate, collectionName: collectionName || r.collection, tokenId: r.tokenId });
        const changed = picked.name !== r.name;
        r.namePlaceholder = picked.placeholder;
        if (!changed) return false;
        const before = r.name;
        r.name = picked.name;
        // The image AND its fallbacks travel together: an image set without
        // them made the later background pass skip the fallbacks ("image
        // already known"), and a broken CDN preview had nothing to fall to.
        const alts = (Array.isArray(imageAlts) ? imageAlts : []).filter(u => u && u !== image).slice(0, 3);
        const setImage = Boolean(image && !r.image);
        if (setImage) { r.image = image; r.imageAlts = alts; }
        else if (image && r.image === image && !(r.imageAlts || []).length && alts.length) r.imageAlts = alts;
        if (this.db && typeof this.db.updateNft === "function") {
          try { this.db.updateNft(this.chain, r.url, { name: r.name, ...(setImage ? { image, imageAlts: alts } : {}) }); } catch { /* db là phụ */ }
        }
        this.onLog(`[NFT NAME] #${r.tokenId}: "${before}" → "${r.name}" (${picked.source}, ${reason})`);
        return true;
      };

      // 1 · Không còn nguồn theo lô: mọi hàng tới lượt đi thẳng đường OpenSea
      //     bên dưới (có trần mỗi lượt, đọc lại tươi cho hàng đang là chỗ giữ).
      const stillPlaceholder = due.slice();

      // 2 · OpenSea metadata cho hàng vẫn là chỗ giữ — có trần mỗi lượt.
      const os = openseaClient();
      for (const r of stillPlaceholder.filter(x => x.namePlaceholder).slice(0, NAME_META_PER_PASS)) {
        try {
          asked++;
          // Chỉ hàng còn là chỗ giữ tới được đây; realtime bận giữa chừng thì dừng lượt.
          if (reason !== "add" && this.engine.realtimePressure()) break;
          const meta = await os.fetchNftMeta(this.chain, r.contract, r.tokenId, { force: true });
          if (meta && apply(r, meta.name, meta.collectionName, meta.image, meta.imageAlts)) updated++;
        } catch (error) {
          this.onLog(`[NFT NAME] #${r.tokenId} opensea: ${String(error.message).slice(0, 80)}`);
        }
      }
      if (updated) this.emitNow();
      return { updated, asked };
    } finally {
      this.nameRefreshing = false;
    }
  }

  // ---- khoá batch của Cancel ------------------------------------------

  lockForBatch(urls = []) {
    for (const url of urls) this.batchLocked.add(url);
    const keys = [];
    for (const url of urls) {
      const row = this.nfts.get(url);
      if (!row) continue;
      const key = tokenKey(this.chain, row.contract, row.tokenId);
      keys.push(key);
      this.engine.suspendRow(key);
    }
    return { ok: true, locked: keys.length };
  }

  unlockAfterBatch(urls = []) {
    for (const url of urls) {
      this.batchLocked.delete(url);
      const row = this.nfts.get(url);
      if (!row || !row.running) continue;
      this.engine.resumeRow(tokenKey(this.chain, row.contract, row.tokenId));
    }
    return { ok: true };
  }

  /**
   * Offer của mình vừa gửi xong nhưng sổ chưa thấy nó chưa?
   *
   *   Sau khi POST thành công, OpenSea cần một lúc để index và phát lại qua
   *   Stream. Trong quãng đó ý định đã DONE ở giá X trong khi sổ own vẫn còn
   *   trống hoặc còn giá cũ. Giao diện gọi quãng đó là "đang chờ" và tô khác
   *   đi — nói rằng con số này chưa được xác nhận.
   */
  isOptimistic(key, mine) {
    const intent = this.engine.intents.get(key);
    if (!intent || !intent.target) return false;
    if (intent.state !== "DONE" && intent.state !== "SENDING") return false;
    const seen = Number(mine && mine.price) || 0;
    return Math.abs(seen - Number(intent.target)) > 1e-12;
  }

  /** Cancel tab hỏi: offer nào của mình đang treo trên NFT này. */
  resolveMyOffer(url) {
    const row = this.nfts.get(url);
    if (!row) return null;
    const book = this.engine.book.get(tokenKey(this.chain, row.contract, row.tokenId));
    if (!book) return null;
    const own = book.ownBest();
    return own.price > 0
      ? { price: own.price, orderHash: own.orderHash, endTime: own.endTime }
      : null;
  }

  /**
   * Số liệu cho panel Diagnostics.
   *
   * VÌ SAO ĐÂY LÀ MỘT METHOD CHỨ KHÔNG PHẢI SÁU THUỘC TÍNH
   *
   *   `main.js` đọc thẳng ruột engine cũ: `e.orderBook.snapshot()`,
   *   `e.inFlight.size`, `e.submittingNfts.size`, `e.globalSubmitting`,
   *   `e.streamTriggered.size`, `e.stats`. V2 không có cái nào trong sáu cái
   *   đó — nó có sổ riêng và cửa vào riêng — nên handler `bot:diagnostics`
   *   ném TypeError ngay dòng đầu trên một bản đã cắt sang V2. Panel chẩn
   *   đoán hỏng đúng vào lúc người ta mở nó ra để tìm hiểu vì sao có chuyện.
   *
   *   Nên engine tự trả lời câu hỏi đó, và `main.js` chỉ hỏi.
   *
   * NHỮNG CON SỐ KHÔNG CÒN NGHĨA THÌ NÓI THẲNG LÀ KHÔNG CÒN
   *
   *   `globalSubmitting` là cửa một-lượt-toàn-cục của kiến trúc cũ, và V2
   *   không có nó: mỗi token một dòng chảy riêng, trần đồng thời là
   *   `MAX_INFLIGHT_SUBMITS`. Báo `false` kèm `concurrency` thật thì đúng;
   *   bịa một con số cho vừa cái tên cũ thì không.
   */
  /**
   * Full per-NFT snapshot for `bot:diagnosticSnapshot` (on demand only).
   * main.js asked the BRIDGE for it and, finding no such method, reported
   * every chain as `unavailable` — the engine's snapshot was never reachable.
   */
  diagnosticSnapshot() {
    try {
      return { chain: this.chain, ...this.engine.diagnosticSnapshot() };
    } catch (error) {
      return { chain: this.chain, unavailable: true, error: String(error && error.message || error) };
    }
  }

  diagnostics() {
    /**
     * MỖI NỬA CÓ LỚP CHẮN RIÊNG
     *
     *   Panel này là thứ người ta mở ra KHI ĐÃ CÓ CHUYỆN. Nếu chính nó ném
     *   lỗi thì nó lấy đi đúng thông tin cần nhất, đúng lúc cần nhất — và đó
     *   không phải giả thuyết: handler `bot:diagnostics` đã từng sập ngay
     *   dòng đầu của chính nó vì đọc một internal không còn tồn tại.
     *
     *   Bọc riêng từng nửa chứ không bọc chung một khối: sổ hỏng thì phần số
     *   liệu engine vẫn còn đọc được, và ngược lại. Một `try` bao cả hai sẽ
     *   biến một hỏng hóc nhỏ thành một báo cáo trống.
     */
    const brief = error => String((error && error.message) || error).slice(0, 160);

    let book;
    try {
      book = this.engine.book.census();
    } catch (error) {
      book = { error: brief(error) };
    }

    let engine;
    try {
      const intents = this.engine.intents.census();
      const sending = (intents.byState && intents.byState.SENDING) || 0;
      engine = {
        chain: this.chain,
        running: this.running === true,
        inFlight: intents.inFlight,
        submitting: sending,
        // Không còn cửa toàn cục nào để mà mở hay đóng.
        globalSubmitting: false,
        concurrency: intents.inFlight,
        streamTriggered: intents.intents,
        stats: { ...this.engine.stats, intents }
      };
    } catch (error) {
      engine = { chain: this.chain, error: brief(error) };
    }

    return { book, engine };
  }

  // ---- trạng thái ra giao diện ---------------------------------------

  markDirty() { this.dirty = true; }

  /** Xem `emitTimer` ở constructor. */
  scheduleEmit() {
    if (this.emitTimer) return;
    const timer = setTimeout(() => {
      this.emitTimer = null;
      this.emitNow();
    }, UI_COALESCE_MS);
    if (timer.unref) timer.unref();
    this.emitTimer = timer;
  }

  emitNow() {
    this.dirty = false;
    if (this.emitTimer) { clearTimeout(this.emitTimer); this.emitTimer = null; }
    try { this.onUpdate(this.getState()); } catch { /* UI không làm hỏng engine */ }
  }

  emitIfDirty() { if (this.dirty) this.emitNow(); }

  /**
   * MỘT NGUỒN SỰ THẬT CHO "HÀNG NÀY ĐANG Ở ĐÂU"
   *
   *   Decision Engine, cột Best Offer, cột Trạng thái và các ô đếm trên đầu
   *   đều đọc từ ĐÚNG MỘT chỗ: sổ của token (`effectiveBest` / `ownBest`)
   *   đưa qua `decide()` — cùng hàm engine dùng để quyết định có gửi hay
   *   không. Không có phép tính thứ hai ở đây: nếu engine thấy 0.0275 thì
   *   bảng hiện 0.0275, và ngược lại.
   *
   * @returns {{best:object, mine:object, verdict:object|null, book:object|null}}
   */
  readRow(row, key, now) {
    const book = this.engine.book.get(key);
    const best = book ? book.effectiveBest(now) : { price: 0, kind: "", orderHash: null };
    const mine = book ? book.ownBest(now) : { price: 0, orderHash: null, endTime: 0 };
    const engineRow = this.engine.rows.get(key);
    const verdict = engineRow ? decide({
      best: best.price, mine: mine.price,
      minPrice: engineRow.minPrice, maxPrice: engineRow.maxPrice, step: engineRow.step
    }) : null;
    return { book, best, mine, verdict, engineRow };
  }

  /**
   * Trạng thái CẠNH TRANH của một hàng, đúng từ vựng giao diện đang vẽ.
   *
   *   ON_TOP        offer của mình đang cao hơn mọi đối thủ áp cho token
   *   OUTBID        đối thủ cao hơn VÀ vượt không được (quá Max) — bot chỉ
   *                 theo dõi, không gửi giá thấp hơn
   *   ACTIVE        đang xử lý: cần gửi và đang/sắp gửi
   *   NO_OFFER      không ai đặt, mình cũng chưa — chờ lượt gửi Min
   *   LOW_BALANCE / SEND_FAILED / SUBMIT_BLOCKED / ERROR
   *
   *   "Đang theo dõi" không còn là mặc định: nó từng che mất cả dẫn đầu lẫn
   *   bị vượt giá. Mỗi hàng đang chạy nay rơi vào đúng một trong các ô trên.
   */
  rowStatus(row, key, now, read = null) {
    /**
     * HÀNG KHÔNG CHẠY GIỮ NGUYÊN LÝ DO NÓ KHÔNG CHẠY
     *
     *   Reset đặt hàng về IDLE ("chưa có gì xảy ra"), còn Stop/Pause đặt về
     *   PAUSED ("đã chạy, đang tạm nghỉ"). Cả hai đều `running === false`;
     *   làm phẳng thành PAUSED là báo cáo một cú Reset thành một cú Pause.
     */
    if (!row.running) return row.status || STATUS.PAUSED;

    const r = read || this.readRow(row, key, now);
    // Ví thiếu tiền đứng trước mọi lý do khác: dù đang dẫn đầu hay bị vượt
    // giá, điều người dùng cần biết là không gửi được vì hết WETH.
    if (r.engineRow && r.engineRow.lowBalance) return STATUS.LOW_BALANCE;

    const intent = this.engine.intents.get(key);
    /**
     * HAI CỘT, HAI CÂU HỎI KHÁC NHAU — KHÔNG ĐƯỢC TRẢ LỜI CHỒNG NHAU
     *
     *   Cột Tiến trình trả lời "engine đang làm gì với hàng này" và nó đã nói
     *   đủ: Chờ lượt gửi, Đang ký, Đang gửi Offer. Cột Trạng thái trả lời
     *   "ai đang thắng". Bản trước cho cột Trạng thái nói "Đang xử lý" ngay
     *   khi hàng có ý định — kể cả khi hàng mới chỉ nằm trong hàng đợi của
     *   scheduler — nên người dùng đọc được đồng thời "Chờ lượt gửi" và
     *   "Đang xử lý" trên cùng một dòng và không biết tin cột nào.
     *
     *   Nay "Đang xử lý" chỉ dành cho lúc THẬT SỰ đang làm (ký hoặc đang trên
     *   dây). Nằm chờ lượt là FOLLOWING — "Đang bám giá" — đúng vị thế thị
     *   trường đã đẩy hàng vào hàng đợi, trong khi cột Tiến trình nói nó đang
     *   chờ ở đâu.
     */
    const working = intent && (intent.state === "BUILDING" || intent.state === "SENDING");
    if (working) return "ACTIVE";
    // Lỗi tạm thời đang chờ thử lại: đang bám giá, không phải lỗi — cột Tiến
    // trình đã nói "Đang thử lại".
    if (intent && intent.state === "RETRY") return "FOLLOWING";
    if (!r.book || !r.verdict) return this.engine.intents.isInFlight(key) ? "ACTIVE" : "FOLLOWING";
    // Dẫn đầu là dẫn đầu, kể cả khi lượt gửi TRƯỚC ĐÓ hỏng: sổ nói offer của
    // mình đang cao nhất, và đó là sự thật mới hơn một lỗi cũ.
    if (r.verdict.status === VERDICT.ON_TOP) return "ON_TOP";
    if (intent && intent.state === "FAILED" && (intent.lastError || (r.engineRow && r.engineRow.lastError))) {
      return "SEND_FAILED";
    }

    switch (r.verdict.status) {
      case VERDICT.ON_TOP:     return "ON_TOP";
      case VERDICT.ABOVE_MAX:  return "OVER_MAX";
      case VERDICT.BAD_CONFIG: return "SUBMIT_BLOCKED";
      case VERDICT.SEND:
        // Chưa ai đặt và mình cũng chưa: hàng đang chờ lượt gửi Min. Nói
        // đúng điều đó thay vì "đang xử lý" — trong lúc đọc REST chưa xong,
        // sổ trống không có nghĩa là "không ai đặt".
        if (r.best.price <= 0 && r.mine.price <= 0 && !this.engine.hydrating.has(key) &&
            !this.engine.awaitingFirstRead.has(key)) return "NO_OFFER";
        // Equality is a SEND decision, never a leading state or a separate UI
        // badge. The active intent/progress column shows when that send starts.
        if (r.mine.price > 0 && Math.abs(r.mine.price - r.best.price) < 1e-12) return "FOLLOWING";
        if (r.mine.price > 0 && r.mine.price < r.best.price) return "OUTBID_CHASING";
        return "FOLLOWING";
      default:                 return "FOLLOWING";
    }
  }

  /**
   * Cột "Tiến trình": engine ĐANG LÀM GÌ với hàng này. Không bao giờ nói ai
   * đang thắng — đó là việc của cột Trạng thái.
   *
   *   SCANNING    đang đọc trạng thái ban đầu qua REST
   *   SIGNING     dựng + ký cục bộ
   *   QUOTA       đã ký, chờ tới lượt ghi
   *   SEND_OFFER  đang trên đường dây
   *   ERROR       lượt gửi gần nhất hỏng
   *   WATCHING    đang nghe Stream, không có việc dở
   *
   * `nextScanAt` luôn 0: V2 không có nhịp hẹn giờ nào để đếm tới.
   */
  scanStateOf(row, key) {
    if (!row.running) return "STOPPED";
    // Đang đọc, hoặc đang chờ đọc lại sau một lượt hỏng: chưa biết Best.
    // Own vừa hết hạn, đang xác nhận lại Best trước khi đặt lại (cổng đóng).
    // Nhãn đọc CHỈ khi có việc đọc thật (đang bay / nợ đọc / hẹn giờ). Cờ
    // trơ không được hiện "Đang cập nhật Best" — liveness sẽ tạo lại việc.
    const e = this.engine;
    const readJob = (e.hydrating && e.hydrating.has(key)) || (e.pendingReads && e.pendingReads.has(key)) ||
      (e.firstReadTimers && e.firstReadTimers.has(key));
    const book = e.book.get(key);
    const cold = !book || !(book.hydratedAt > 0 || book.lastEventAt > 0);
    if (cold && readJob) return "SCANNING";
    if (cold && e.awaitingFirstRead && e.awaitingFirstRead.has(key)) return "WAIT_BEST";
    if (e.downwardAuthority && e.downwardAuthority.has(key)) return readJob ? "RENEW_CHECK" : "WAIT_BEST";
    if (e.topicRepairAt && e.topicRepairAt.has(key)) return readJob ? "SCANNING" : "RECOVERING";

    const intent = this.engine.intents.get(key);
    // Chưa có template tĩnh (prewarm hỏng / dựng lại ở nền): chưa ở hàng chờ
    // ghi thật — nói "Đang chuẩn bị", không phải "Chờ lượt gửi".
    if (this.engine.templateReady && !this.engine.templateReady(key) && !(intent && intent.state === "FAILED") &&
        !(intent && intent.state === "WAITING" && !/template/.test(String(intent.deferredReason || "")))) {
      const r = this.readRow(row, key, Date.now());
      if (r.verdict && r.verdict.status === VERDICT.SEND) return "TEMPLATE_WAIT";
    }
    switch (intent ? intent.state : "") {
      // Có target, đang chờ lượt của controller adaptive theo API key.
      case "READY":     return intent.target && intent.state === "READY" ? "QUEUED" : "WATCHING";
      case "GRANTING":  return "QUOTA";
      case "BUILDING":  return "BUILDING";
      case "SENDING":   return "SEND_OFFER";
      case "DONE":      return "CONFIRMING";
      case "RETRY":     return "RETRYING";
      // SEND hợp lệ đang chờ một phụ thuộc cục bộ (template / đối soát own
      // của riêng hàng): đang chuẩn bị order, chưa xin lượt ghi.
      // WAITING nói ĐÚNG phụ thuộc đang chờ (1.25.2) — không gộp mọi thứ vào "chuẩn bị order".
      case "WAITING": {
        const why = String(intent.deferredReason || "");
        if (/template/.test(why)) return "TEMPLATE_WAIT";
        if (intent.dependency === "post-reconcile" || /post-uncertain/.test(why)) return "POST_RECONCILE";
        return "OWN_VERIFY";
      }
      case "FAILED":    return "ERROR";
      default: break;
    }
    // Stream đứt/đang nối lại/im bất thường: engine đang phục hồi, không phải
    // "theo dõi" — nói đúng để người dùng không tưởng nó đang nghe.
    let health = "";
    try { health = this.streamHealth ? String(this.streamHealth(row.collectionSlug)) : ""; } catch { health = ""; }
    if (health === "RECONNECTING" || health === "DISCONNECTED" || health === "STALE" || health === "FAILED") {
      return "RECOVERING";
    }
    return "WATCHING";
  }

  getRows() {
    const now = Date.now();
    /**
     * addedSeq — VỊ TRÍ, KHÔNG PHẢI DANH TÍNH (1.25.2)
     *
     *   `this.nfts` là Map, và thứ tự lặp của nó CHÍNH LÀ thứ tự thêm: nạp lại
     *   khi khởi động giữ đúng thứ tự `db.getNfts()` (mảng chỉ được `push`,
     *   không bao giờ sắp lại), và Add mới luôn `set()` vào cuối Map. Vị trí
     *   lặp — không lưu ở đâu cả — là chỉ số tăng dần theo đúng thời điểm
     *   thêm: 0 = cũ nhất. Renderer sắp giảm dần để hàng mới nhất lên đầu và
     *   đánh STT `rows.length - rowIndex` — không đọc lại thì mọi hàng đều có
     *   `addedSeq` mặc định 0, sắp xếp thành vô nghĩa (danh sách vẫn hiện thị
     *   mới-lên-đầu nhờ một quy tắc DOM khác, nhưng SỐ THỨ TỰ hiện bị đảo
     *   ngược: đỉnh mang số nhỏ nhất, đáy mang số lớn nhất).
     */
    const ordered = [...this.nfts.values()];
    return ordered.map((row, addedSeq) => {
      const key = tokenKey(this.chain, row.contract, row.tokenId);
      const read = this.readRow(row, key, now);
      const { book, best, mine } = read;
      const intent = this.engine.intents.get(key);
      return {
        url: row.url, chain: row.chain,
        contract: row.contract, tokenId: row.tokenId,
        name: row.name, image: row.image, imageAlts: row.imageAlts,
        collection: row.collection, collectionSlug: row.collectionSlug,
        minPrice: row.minPrice, maxPrice: row.maxPrice,
        step: row.step, duration: row.duration,
        status: this.rowStatus(row, key, now, read),
        running: row.running,
        // Display-only ordinal (renderer: STT). NEVER an identity — a row is
        // still identified by chain+contract+tokenId (see nftKey/tokenKey).
        addedSeq,

        /**
         * `priorityMode` không còn ô tick trong bảng (bỏ ở 1.19.31); trường
         * này vẫn được engine/lịch sử dữ liệu giữ, chỉ không còn vẽ ra UI.
         * `myOptimistic` nói rằng offer vừa gửi xong nhưng OpenSea chưa index.
         */
        priorityMode: Boolean(row.priorityMode),
        myOptimistic: this.isOptimistic(key, mine),

        scanState: this.scanStateOf(row, key),
        nextScanAt: 0,

        /**
         * SỔ THẮNG KHI NÓ BIẾT; GIÁ LÚC THÊM DÙNG KHI NÓ CHƯA BIẾT.
         *
         *   Lúc thêm NFT, `main.js` đã đọc Best Offer qua REST và gán vào
         *   hàng. Sổ lúc đó còn trống, nên số 0 của sổ không phải câu trả
         *   lời. Khi sổ đã hydrate hoặc Stream đã nói, sổ thắng — kể cả khi
         *   nó nói 0: giá lúc thêm có thể là offer của CHÍNH MÌNH.
         */
        best: Math.max(best.price, mine.price) > 0 ? Math.max(best.price, mine.price)
          : (book && (book.hydratedAt > 0 || book.lastEventAt > 0)) ? 0
          : (Number(row.best) || 0),
        // Nguồn của Best: item / collection / trait — để tooltip nói đúng.
        bestKind: mine.price > best.price ? "item" : best.price > 0 ? String(best.kind || "") : "",
        mine: mine.price > 0 ? mine.price : (Number(row.mine) || 0),
        target: intent ? intent.target : 0,
        lastError: row.lastError || (intent ? intent.lastError : "") || "",

        /**
         * ROW VIEW MODEL — một hình dạng, renderer chỉ vẽ, không tính lại.
         *
         *   effectiveBest  Best đối thủ đang ÁP (sổ; 0 khi chưa biết)
         *   bestKnown      sổ đã biết chưa (chưa biết → giao diện hiện "—")
         *   effectiveStep  bước tăng engine ĐANG dùng — theo bậc của Best,
         *                  không theo ô Step; đổi cả hai chiều khi Best qua 0.1
         *   desiredTarget  giá mới nhất engine muốn gửi (0 khi không cần)
         *   myOffer        own order thật đang sống (0 khi chưa có)
         *   max            trần đã về lưới giá
         *   progress/status/error/generation
         */
        effectiveBest: best.price > 0 ? best.price : 0,
        bestKnown: Boolean(book && (book.hydratedAt > 0 || book.lastEventAt > 0)),
        bestExpiry: best.price > 0 && best.orderHash && book
          ? (() => { const o = book.groupFor(best.kind).get(best.orderHash); return o ? Number(o.endTime) || 0 : 0; })()
          : 0,
        effectiveStep: read.verdict ? read.verdict.effectiveStep
          : effectiveStepFor(best.price, Number(row.step)),
        desiredTarget: intent ? Number(intent.target) || 0 : 0,
        myOffer: mine.price > 0 ? mine.price : 0,
        max: read.verdict ? read.verdict.max : gridMax(row.maxPrice),
        progress: this.scanStateOf(row, key),
        error: row.lastError || (intent ? intent.lastError : "") || "",
        generation: book ? book.generation : 0,
        /**
         * CHẨN ĐOÁN NHẸ CHO "CHỜ LƯỢT GỬI" (tooltip): chờ từ bao giờ, đang ở
         * đâu (READY / đang xin lượt / thử lại), broker còn nối không, còn
         * cooldown bao lâu. Renderer chỉ ghép chuỗi; không tính gì.
         */
        wait: this.waitDiagnostics(key, intent, now)
      };
    });
  }

  waitDiagnostics(key, intent, now) {
    if (!intent) return null;
    const st = String(intent.state || "");
    if (st !== "READY" && st !== "GRANTING" && st !== "RETRY") return null;
    let q = null;
    try { q = this.engine.quota ? this.engine.quota.status() : null; } catch { q = null; }
    const model = q && q.model ? q.model : null;
    const engineRow = this.engine.rows.get(key);
    return {
      state: st === "GRANTING" ? "BROKER_WAIT" : st,
      queuedForMs: intent.queuedAt ? Math.max(0, now - intent.queuedAt) : 0,
      brokerConnected: q ? Boolean(q.connected) : null,
      brokerRole: q ? q.role : "",
      brokerQueued: q ? Number(q.queued) || 0 : 0,
      cooldownMs: model ? Math.max(Number(model.nextInMs) || 0, Number(model.blockedForMs) || 0) : 0,
      retryInMs: engineRow && engineRow.retryAt ? Math.max(0, engineRow.retryAt - now) : 0,
      requeues: Number(intent.requeues) || 0,
      templateReady: this.engine.templateReady ? this.engine.templateReady(key) : true
    };
  }

  /**
   * Các ô đếm trên đầu bảng, suy ra từ ĐÚNG trạng thái từng hàng — cùng
   * `rowStatus` mà cột Trạng thái vẽ, nên tổng luôn khớp với những gì thấy.
   */
  getCounters() {
    const now = Date.now();
    let onTop = 0, outbid = 0, working = 0, failed = 0, noOffer = 0;
    for (const row of this.nfts.values()) {
      if (!row.running) continue;
      const key = tokenKey(this.chain, row.contract, row.tokenId);
      switch (this.rowStatus(row, key, now)) {
        case "ON_TOP":    onTop++; break;
        case "OUTBID":
        case "OVER_MAX":  outbid++; break;
        // Hoà / bị vượt mà đang đuổi: bot đang lo — cùng ô với Đang xử lý.
        case "TIED":
        case "OUTBID_CHASING": working++; break;
        // Ô đếm "Đang xử lý" của bảng là "bao nhiêu hàng bot đang lo", nên nó
        // gộp cả hàng đang ký/đang gửi lẫn hàng đang chờ lượt. Chi tiết "chờ
        // ở đâu" thuộc về từng dòng, không thuộc về một con số tổng.
        case "ACTIVE":
        case "FOLLOWING": working++; break;
        case "NO_OFFER":  noOffer++; break;
        default:          failed++; break;   // LOW_BALANCE / SEND_FAILED / SUBMIT_BLOCKED / ERROR
      }
    }
    return {
      total: this.nfts.size, onTop, outbid, working, failed, noOffer,
      // `active` là ô đếm thứ nhất của bảng, và `halted` là thứ quyết định
      // banner "đã dừng sau khi huỷ sạch". Thiếu chúng thì bảng vẽ 0 và
      // banner không bao giờ hiện.
      active: onTop + outbid + working + noOffer,
      licenseHalted: this.haltedByLicense === true,
      licenseHaltReason: this.licenseHaltReason || ""
    };
  }


  getState() {
    /**
     * PHẦN THIẾT YẾU KHÔNG ĐƯỢC CHẾT VÌ PHẦN PHỤ
     *
     *   `main.js` gọi hàm này mỗi lần đẩy bảng ra giao diện. Bản đầu tính
     *   census và báo cáo độ trễ ngay trong cùng một object literal — nên khi
     *   metrics ném (tiêm lỗi: "metrics hỏng"), cả hàm ném, và bảng NFT ngừng
     *   cập nhật hoàn toàn vì một con số chẩn đoán không ai đang nhìn.
     *
     *   Hàng và bộ đếm là thứ người dùng thấy; phần `v2` là để chẩn đoán.
     *   Mỗi phần chẩn đoán được bọc riêng: hỏng cái nào thì cái đó báo lỗi,
     *   phần còn lại vẫn có.
     */
    const safe = fn => { try { return fn(); } catch (error) {
      return { error: String(error && error.message || error).slice(0, 120) }; } };
    const now = Date.now();
    let streamHealth = "UNKNOWN";
    try { streamHealth = this.streamHealth ? String(this.streamHealth()) : "UNKNOWN"; } catch { streamHealth = "UNKNOWN"; }
    return {
      chain: this.chain,
      running: this.running,
      /**
       * NHỊP SỐNG cho renderer: hoạt ảnh "đang sống" chỉ khi mốc còn tươi và
       * Stream còn khoẻ; Stream đứt thì tiến trình nói "Đang phục hồi" chứ
       * không nhấp nháy như thể mọi thứ ổn.
       */
      heartbeat: {
        at: now,
        engineRunning: this.running === true && this.engine.state === STATE.RUNNING,
        engineState: this.engine.state,
        streamHealth,
        lastStreamEventAt: this.engine.lastStreamEventAt || 0,
        lastActivityAt: this.engine.lastActivityAt || 0
      },
      rows: this.getRows(),
      counters: this.getCounters(),
      stats: this.engine.stats,
      // Chỉ V2 mới có: dùng cho chẩn đoán, giao diện cũ bỏ qua trường lạ.
      v2: {
        state: this.engine.state,
        epoch: this.engine.epoch,
        book: safe(() => this.engine.book.census()),
        intents: safe(() => this.engine.intents.census()),
        traits: safe(() => this.engine.traits ? this.engine.traits.census() : null),
        latency: safe(() => this.engine.latencyReport()),
        violations: safe(() => this.engine.violations()),
        quota: safe(() => this.engine.quota ? this.engine.quota.status() : null)
      }
    };
  }

  logRuntimeCensus() {
    const s = this.engine.getState();
    this.onLog(`[V2 ${this.label}] census · ${JSON.stringify({
      rows: s.rows, book: s.book, intents: s.intents,
      http: s.http, recovery: s.recovery
    })}`);
    return s;
  }
}

module.exports = { OfferItemV2Bridge, STATUS };
