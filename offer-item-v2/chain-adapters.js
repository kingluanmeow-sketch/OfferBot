"use strict";

/**
 * chain-adapters.js — Ethereum và Robinhood khác nhau ở ĐÚNG những chỗ này.
 *
 * MỘT KIẾN TRÚC, HAI ADAPTER — KHÔNG PHẢI HAI ENGINE
 *
 *   Fork engine thành hai bản là cách hai chain từ từ trôi ra xa nhau: một
 *   bản sửa lỗi, bản kia không, và sáu tháng sau không ai nói được chúng còn
 *   giống nhau ở đâu. `engine-v2.js` không có một câu `if (chain === ...)`
 *   nào; mọi khác biệt nằm ở đây.
 *
 * KHÁC BIỆT THẬT GIỮA HAI CHAIN
 *
 *   chainId          ký EIP-712 sai chainId thì chữ ký vô nghĩa
 *   WETH             hai địa chỉ khác nhau
 *   conduitKey       hai giá trị khác nhau, đo từ chính cấu hình production
 *   slug chain       OpenSea gọi tên chain trong URL
 *   RPC              chỉ dùng ở tầng nạp sẵn, không bao giờ trên đường nóng
 *
 * MỌI LƯỢT CHỜ MẠNG NẰM Ở ĐÂY, VÀ CHỈ Ở ĐÂY
 *
 *   `chainConfig`, `accountState`, `collectionConfig`, `resyncOwnOrders` đều
 *   được gọi lúc Start hoặc từ mặt phẳng recovery. Đường nóng không gọi cái
 *   nào trong số chúng.
 */

const { ethers } = require("ethers");

const opensea = require("../opensea");
const standardStore = require("../standard-store");
const { PRIORITY, KIND } = require("../rate-limiter");
const { openseaOfferPayload } = require("./order-builder");

/** Bao lâu thì counter/số dư coi là cũ. Làm mới ở nền, không chặn đường nóng. */
const ACCOUNT_TTL_MS = 5 * 60 * 1000;
/** Phí và chuẩn token của collection gần như không đổi. */
const COLLECTION_TTL_MS = 30 * 60 * 1000;

const SEAPORT_COUNTER_ABI = [
  "function getCounter(address offerer) external view returns (uint256 counter)"
];
const ERC165_ABI = [
  "function supportsInterface(bytes4 interfaceId) external view returns (bool)"
];
/**
 * chainId dự phòng, dùng KHI VÀ CHỈ KHI RPC không trả lời.
 *
 * Cả hai đã được kiểm chứng on-chain: order dựng cục bộ bằng chúng cho ra
 * hash trùng khớp với `getOrderHash` của Seaport 1.6 trên chính chain đó.
 * Xem offer-item-v2-onchain-sim.js.
 */
const KNOWN_CHAIN_IDS = Object.freeze({ ethereum: 1, robinhood: 4663 });

const ERC20_BALANCE_ABI = [
  "function balanceOf(address owner) external view returns (uint256)"
];
const ERC721_INTERFACE = "0x80ac58cd";
const ERC1155_INTERFACE = "0xd9b67a26";

class ChainAdapter {
  /**
   * @param {object} o
   * @param {string} o.chain             tên nội bộ: "ethereum" | "robinhood"
   * @param {(line:string)=>void} [o.onLog]
   */
  constructor({ chain, onLog } = {}) {
    this.chain = opensea.normalizeChain(chain);
    /** Tên chain trong URL của OpenSea. Trùng tên nội bộ cho cả hai chain này. */
    this.openseaChain = this.chain;
    this.onLog = typeof onLog === "function" ? onLog : () => {};

    this._provider = null;
    this._config = null;
    this._account = null;
    /** Số dư WETH đã đọc được, kèm cờ "có đọc được không". */
    this._balance = null;
    /** slug -> { at, value } */
    this._collections = new Map();
    /** slug -> mốc (ms) tới đó không thử drain collection /all nữa (quá lớn). */
    this._collectionDrainSkip = new Map();
  }

  log(line) { this.onLog(`[ADAPTER ${this.chain}] ${line}`); }

  /** Provider dùng chung, tạo một lần. KHÔNG dùng trên đường nóng. */
  provider() {
    if (this._provider) return this._provider;
    const urls = opensea.getRpcList(this.chain);
    if (!urls || !urls.length) throw new Error(`không có RPC cho ${this.chain}`);
    this._provider = new ethers.JsonRpcProvider(urls[0]);
    return this._provider;
  }

  /**
   * Hằng số của chain. Đọc một lần lúc Start.
   *
   * `chainId` lấy TỪ MẠNG chứ không hardcode: một hằng số chép sai ở đây tạo
   * ra chữ ký hợp lệ về mặt toán nhưng vô nghĩa với contract, và lỗi đó chỉ
   * lộ ra khi OpenSea từ chối order.
   */
  async chainConfig() {
    if (this._config) return this._config;

    /**
     * RPC IM LẶNG KHÔNG ĐƯỢC LÀM CHẾT CẢ TÍNH NĂNG
     *
     *   Bản đầu để lỗi từ `getNetwork()` lan thẳng ra ngoài, nên `start()`
     *   NÉM khi RPC hỏng và Offer Item không khởi động được. Đo được: bộ kiểm
     *   no-cadence chạy bản đóng gói không có JSON-RPC, và cả sáu hàng đứng
     *   im — không target, không best, không cả thông báo lỗi.
     *
     *   Điều đó nặng hơn nhiều so với vẻ ngoài của nó: đường tiền của sản
     *   phẩm là API của OpenSea, không phải RPC. Mất Alchemy mà chết hẳn
     *   Offer Item là biến một sự cố phụ thành một sự cố toàn phần. Kiến
     *   trúc cũ chịu được điều này.
     *
     * DỰ PHÒNG LÀ GIÁ TRỊ ĐÃ ĐO, KHÔNG PHẢI GIÁ TRỊ ĐOÁN
     *
     *   Mạng vẫn là nguồn chính, vì một chainId chép sai tạo ra chữ ký hợp lệ
     *   về mặt toán mà vô nghĩa với contract. Chỉ khi nó im lặng mới dùng con
     *   số dự phòng — và hai con số dưới đây đã được kiểm chứng ON-CHAIN:
     *   order dựng cục bộ với chúng cho ra hash TRÙNG với `getOrderHash` của
     *   Seaport trên cả hai chain.
     */
    let chainId = KNOWN_CHAIN_IDS[this.chain] || 0;
    try {
      const net = await this.provider().getNetwork();
      chainId = Number(net.chainId);
    } catch (error) {
      if (!chainId) throw error;      // chain lạ: không có gì để dựa vào
      this.log(`không đọc được chainId (${error.message}) — ` +
        `dùng giá trị đã kiểm chứng on-chain: ${chainId}`);
    }

    this._config = {
      chainId,
      wethAddress: opensea.getWeth(this.chain),
      conduitKey: opensea.getOpenSeaConduitKey(this.chain),
      signedZone: opensea.OPENSEA_SIGNED_ZONE_V2,
      seaport: opensea.SEAPORT_V1_6
    };
    this.log(`chainId=${this._config.chainId} WETH=${this._config.wethAddress}`);
    return this._config;
  }

  /** Counter Seaport của ví. Có TTL; làm mới ở nền. */
  async accountState(address, { force = false } = {}) {
    const now = Date.now();
    // Keyed by wallet: after a wallet switch, wallet A's counter must never be
    // served for wallet B (1.25.11).
    const sameWallet = this._account && String(this._account.address || "").toLowerCase() === String(address || "").toLowerCase();
    if (!force && sameWallet && now - this._account.at < ACCOUNT_TTL_MS) {
      return this._account;
    }
    /**
     * COUNTER LÀ uint256, KHÔNG PHẢI Number
     *
     *   Seaport 1.6 không tăng counter lên 1: `incrementCounter` đặt nó thành
     *   một số gần-ngẫu-nhiên 128-bit. Ví nào từng gọi nó (để vô hiệu mọi
     *   order cũ một lượt) mang một counter cỡ 2,89e39 — và `Number()` làm
     *   tròn con số đó, nên struct đem ký mang một counter KHÔNG PHẢI của ví,
     *   và ethers từ chối ngay khi mã hoá: "invalid BigNumberish string
     *   2.891857727792955e+39".
     *
     *   Đo được trên ví thật lúc live proof; mọi bài kiểm trước dùng counter
     *   0 nên không thấy. Giữ chuỗi thập phân xuyên suốt — EIP-712 uint256
     *   nhận chuỗi, và không có phép toán nào trên counter cả.
     */
    let counter = "0";
    try {
      // `provider()` NẰM TRONG try: nó ném khi không có RPC nào cấu hình
      // được, và một lỗi ở đây từng làm cả `start()` chết — Offer Item không
      // khởi động nổi chỉ vì Alchemy trục trặc, trong khi đường tiền thật của
      // sản phẩm là API của OpenSea và nó vẫn chạy.
      const seaport = new ethers.Contract(
        opensea.SEAPORT_V1_6, SEAPORT_COUNTER_ABI, this.provider());
      counter = (await seaport.getCounter(address)).toString();
    } catch (error) {
      // Không đọc được counter thì 0 là giá trị đúng cho một ví chưa bao giờ
      // gọi incrementCounter — và nếu sai, OpenSea từ chối order chứ không
      // tạo ra một order khớp nhầm.
      this.log(`không đọc được counter: ${error.message}`);
    }
    this._account = { at: now, address, counter };
    return this._account;
  }

  /**
   * Counter đã đổi chưa? Gọi từ recovery, không gọi từ đường nóng.
   *
   * Ví gọi `incrementCounter` sẽ vô hiệu hoá MỌI order đã ký với counter cũ,
   * nên template phải được dựng lại. Đây là lý do counter có invalidation
   * tường minh chứ không chỉ có TTL.
   */
  async counterChanged(address) {
    const before = this._account &&
      String(this._account.address || "").toLowerCase() === String(address || "").toLowerCase()
      ? String(this._account.counter) : null;
    const after = await this.accountState(address, { force: true });
    return before !== null && before !== String(after.counter);
  }

  /** Phí và chuẩn token của một collection. Có TTL. */
  async collectionConfig(slug, contract, { peek = false, tokenId = "" } = {}) {
    const cached = this._collections.get(slug);
    if (cached && Date.now() - cached.at < COLLECTION_TTL_MS) return cached.value;
    // `peek`: chỉ trả thứ đã có trong cache. Prewarm dùng nó để KHÔNG chờ
    // mạng — hàng chưa có template được dựng ở nền (xem engine.ensureTemplate).
    if (peek) return null;

    /**
     * THIẾU SLUG KHÔNG PHẢI LÀ MỘT LỖI — NÓ LÀ MỘT CÂU HỎI CHƯA AI HỎI
     *
     *   #1236 đọc được Best nhưng template chết với "Thiếu collection slug để
     *   lấy fees", rồi thử lại 2s → 5s → 15s → mãi mãi. Nghịch lý ấy có thật
     *   và có lý do: `fetchBestOffer` TỰ giải slug từ contract khi hàng không
     *   mang theo slug, dùng xong rồi vứt — không ai ghi ngược lại. Template
     *   thì đi thẳng vào `fetchCollectionFees("")` và ném.
     *
     *   Cùng một câu hỏi, cùng một bộ nhớ đệm: hỏi lại ở đây gần như luôn
     *   trúng cache của lượt đọc Best vừa xong, và hàng thoát khỏi vòng thử
     *   lại vô hạn ngay lượt đầu.
     */
    let use = slug;
    if (!use && contract) {
      use = await opensea.resolveCollectionSlug(this.chain, contract, tokenId);
      if (!use) throw new Error("Chưa xác định được collection của NFT này để lấy phí.");
    }

    const fees = await opensea.fetchCollectionFees(use);
    const tokenStandard = await this.tokenStandard(contract, tokenId);

    // Cache và kết quả mang SLUG ĐÃ GIẢI, không mang chuỗi rỗng ban đầu:
    // người gọi dùng `value.slug` để vá lại hàng của mình, và lượt sau trúng
    // cache ngay.
    const value = {
      slug: use,
      fees: (fees.fees || []).map(f => ({
        recipient: f.recipient, basisPoints: f.basisPoints
      })),
      requiresSignedZone: Boolean(fees.requiresSignedZone),
      tokenStandard
    };
    this._collections.set(use, { at: Date.now(), value });
    return value;
  }

  /**
   * ERC-721 hay ERC-1155.
   *
   * Đoán là cách một order bị từ chối SAU KHI đã tiêu một chữ ký, nên hỏi
   * contract. Một lần cho mỗi contract, lúc Start.
   */
  /**
   * CHUẨN TOKEN LÀ THUỘC TÍNH CỦA CONTRACT, VÀ CÓ NHIỀU NGUỒN
   *
   *   Bản trước chỉ hỏi ERC165 qua MỘT RPC. Contract trả lời "không phải cả
   *   hai" (proxy lạ, ERC165 cài sai) là ném — và template của mọi NFT thuộc
   *   contract đó không bao giờ dựng được: đo trên #782
   *   (0x266cd5a2…d36f) là 60 lượt thử, mỗi lượt một lần ném.
   *
   *   Nay hỏi theo thứ tự rẻ → đắt, dừng ở nguồn đầu tiên trả lời:
   *     1. cache theo chain+contract (bộ nhớ, và đĩa qua `standardStore`)
   *     2. metadata OpenSea (`contract_standard` / `token_standard`) — thứ
   *        chính OpenSea dùng để nhận order, nên đây là nguồn ĐÚNG NHẤT
   *     3. ERC165 qua TỪNG RPC trong danh sách (không phụ thuộc một endpoint)
   *   Không có nguồn nào trả lời thì NÉM (không đoán): 1.19.16 đã bảo đảm
   *   hàng thiếu template không ăn lượt ghi, và đoán sai là một order bị
   *   OpenSea từ chối vĩnh viễn.
   */
  async tokenStandard(contract, tokenId = "") {
    const key = `${this.chain}:${String(contract).toLowerCase()}`;
    const remembered = standardStore.get(key);
    if (remembered) return remembered;

    // 2 · metadata OpenSea (có cache riêng trong opensea.js).
    try {
      const peeked = opensea.peekTokenStandard(this.chain, contract);
      if (peeked) { standardStore.set(key, peeked, "opensea-cache"); return peeked; }
      const found = await opensea.fetchTokenStandard(this.chain, contract, tokenId);
      if (found && found.standard) {
        standardStore.set(key, found.standard, `opensea:${found.source}`);
        this.log(`chuẩn token ${contract} = ${found.standard} (OpenSea ${found.source})`);
        return found.standard;
      }
    } catch { /* sang RPC */ }

    // 3 · ERC165 qua từng RPC — một endpoint im lặng không được giết cả hàng.
    let urls = [];
    try { urls = opensea.getRpcList(this.chain) || []; } catch { urls = []; }
    let answeredNeither = false;
    for (const url of urls) {
      let provider;
      try { provider = new ethers.JsonRpcProvider(url); } catch { continue; }
      const nft = new ethers.Contract(contract, ERC165_ABI, provider);
      let asked = false;
      try {
        if (await nft.supportsInterface(ERC721_INTERFACE)) {
          standardStore.set(key, "erc721", "erc165");
          return "erc721";
        }
        asked = true;
      } catch { /* thử tiếp */ }
      try {
        if (await nft.supportsInterface(ERC1155_INTERFACE)) {
          standardStore.set(key, "erc1155", "erc165");
          return "erc1155";
        }
        asked = true;
      } catch { /* thử tiếp */ }
      if (asked) answeredNeither = true;
      try { provider.destroy && provider.destroy(); } catch { /* không sao */ }
    }

    /**
     * KHÔNG AI TRẢ LỜI ≠ CONTRACT NÓI "KHÔNG PHẢI CẢ HAI"
     *
     *   Mạng hỏng / RPC im / OpenSea không trả field: ta KHÔNG biết, và dừng
     *   cả tính năng vì một lượt đọc hỏng là đắt hơn nhiều so với một order
     *   bị OpenSea từ chối. Giữ nguyên lối cũ: tạm coi ERC721, ghi rõ, và
     *   KHÔNG cất vào cache (để lần sau còn hỏi lại).
     *
     *   Contract TRẢ LỜI cả hai câu và nói không phải — đó là câu trả lời
     *   thật, và đoán tiếp là sai. Hàng ở lại TEMPLATE RECOVERY.
     */
    if (answeredNeither) {
      throw new Error(`không xác định được chuẩn token của ${contract} (contract trả lời không phải ERC721 lẫn ERC1155)`);
    }
    return this.guessedStandard(contract, "không nguồn nào trả lời");
  }

  /**
   * Không hỏi được thì đoán ERC-721 — và nói rõ là đang đoán.
   *
   * ĐOÁN HAY ĐỨNG IM
   *
   *   Chú thích ở trên đúng: đoán chuẩn token là cách một order bị từ chối
   *   SAU KHI đã tiêu một chữ ký. Nhưng lập luận đó áp dụng khi ta HỎI ĐƯỢC
   *   mà lại lười hỏi. Khi RPC im lặng hoàn toàn, lựa chọn không còn là
   *   "đoán hay hỏi" — nó là "đoán hay đứng im".
   *
   *   Cái giá của đoán sai là một order bị OpenSea từ chối: không mất tiền,
   *   mất một lượt gửi, và người dùng thấy lý do trong log. Cái giá của đứng
   *   im là Offer Item không hoạt động chút nào trong suốt thời gian RPC
   *   hỏng — trong khi đường tiền của sản phẩm là API của OpenSea và nó vẫn
   *   đang chạy tốt.
   *
   *   ERC-721 là chuẩn của đại đa số NFT trên OpenSea, nên đây là phía đoán
   *   ít sai nhất.
   */
  guessedStandard(contract, why) {
    this.log(`không hỏi được chuẩn token của ${contract} (${why}) — ` +
      "tạm coi là erc721; OpenSea sẽ từ chối nếu sai");
    return "erc721";
  }

  /** Giá WETH → wei. Thuần cục bộ, dùng trên đường nóng. */
  toWei(amountEth) {
    return ethers.parseEther(String(amountEth));
  }

  /**
   * Số dư WETH của ví. Đọc lúc Start và từ recovery — KHÔNG trên đường nóng.
   *
   * VÌ SAO ĐỌC TRƯỚC THAY VÌ ĐỂ OPENSEA TỪ CHỐI
   *
   *   Một order gửi khi ví thiếu WETH chắc chắn bị từ chối, nhưng nó vẫn tiêu
   *   một chữ ký và một suất hạn mức GHI mà cả máy dùng chung. Với 500 NFT
   *   đang chạy, đó là 500 lượt gửi hỏng, 500 suất quota, và một tab khác
   *   phải xếp hàng sau chúng.
   *
   *   `seaport.js` của kiến trúc cũ gọi đây là "Balance pre-check" và làm
   *   đúng như vậy. V2 giữ nguyên ý đó, chỉ đổi chỗ: đọc ở tầng nạp sẵn, để
   *   đường từ sự kiện tới chữ ký không có lượt chờ mạng nào.
   *
   * KHÔNG ĐỌC ĐƯỢC KHÔNG PHẢI LÀ KHÔNG CÓ
   *
   *   RPC hỏng thì trả `null`, và engine cứ gửi như thường — để OpenSea trả
   *   lời. Kết luận "ví rỗng" từ một lượt đọc hỏng sẽ làm bot đứng im trong
   *   khi ví có đủ tiền, và đó là hỏng nặng hơn nhiều so với một order bị từ
   *   chối.
   */
  async walletBalance(address, { force = false } = {}) {
    const now = Date.now();
    // Keyed by wallet, like accountState (1.25.11).
    const sameWallet = this._balance && String(this._balance.address || "").toLowerCase() === String(address || "").toLowerCase();
    if (!force && sameWallet && now - this._balance.at < ACCOUNT_TTL_MS) {
      return this._balance;
    }
    try {
      const weth = new ethers.Contract(
        opensea.getWeth(this.chain), ERC20_BALANCE_ABI, this.provider());
      const wei = await weth.balanceOf(address);
      this._balance = { at: now, address, wethWei: BigInt(wei), known: true };
      this.log(`số dư WETH = ${ethers.formatEther(wei)}`);
      return this._balance;
    } catch (error) {
      this.log(`không đọc được số dư WETH: ${error.message}`);
      this._balance = { at: now, address, wethWei: 0n, known: false };
      return this._balance;
    }
  }

  /**
   * Thân request POST offer, đúng hình dạng OpenSea nhận.
   *
   * Hình dạng này được dựng ở `order-builder.js`, cạnh thứ đã ký nó. Hàm này
   * chỉ chuyển tiếp. Hai chỗ dựng cùng một thân request là hai chỗ để chúng
   * trôi ra khỏi nhau, và sai lệch đó chỉ lộ ra khi OpenSea từ chối một order
   * ĐÃ TIÊU một chữ ký. `build()` cũng gắn sẵn kết quả này vào `signed.payload`,
   * nên đường nóng không phải dựng lại lần nữa.
   */
  offerBody(signed, row) {
    return signed.payload || openseaOfferPayload(signed);
  }

  /**
   * Giá tốt nhất hiện tại của MỘT NFT, đọc qua REST.
   *
   * VÌ SAO ĐIỀU NÀY BẮT BUỘC PHẢI CÓ
   *
   *   V2 học giá đối thủ từ Stream. Nhưng Stream chỉ kể những gì XẢY RA TỪ
   *   LÚC subscribe — nó không kể trạng thái hiện tại. Một đối thủ đặt offer
   *   một giờ trước rồi im lặng thì không sinh sự kiện nào nữa, nên một engine
   *   chỉ-nghe-Stream sẽ khởi động với sổ RỖNG và tin rằng không ai cạnh
   *   tranh — rồi đặt đúng giá Min.
   *
   *   Bộ smoke test phơi ra đúng điều này: nó không phát sự kiện Stream nào,
   *   và V2 không gửi offer nào. Engine cũ không có lỗi đó vì nó quét REST.
   *
   *   Nên trạng thái ban đầu PHẢI đọc một lần qua REST. Việc đó chạy ở mặt
   *   phẳng recovery, không chặn đường nóng, và kết quả chỉ được ghép vào chỗ
   *   Stream CHƯA nói gì — xem `mergeBest` trong engine.
   *
   * @param {{contract:string, tokenId:string, collectionSlug:string}} row
   */
  async fetchBest(row, {signal} = {}) {
    /**
     * ĐỌC TƯƠI. `fetchBestOffer` mặc định trả bản cache dùng chung của cả
     * app (TTL theo cache.js). Recovery tồn tại để sửa sổ theo sự thật hiện
     * tại; một bản cache là sự thật của lượt đọc trước — có thể là của tab
     * khác, trước cả khi order đó bị huỷ.
     */
    /**
     * VÀ ĐỌC CẢ SỔ, KHÔNG CHỈ NGƯỜI THẮNG.
     *
     *   Cùng một lượt HTTP trả về MỌI offer đang áp cho token này — item,
     *   collection, trait (OpenSea đã tự xét trait có khớp). Chỉ giữ người
     *   thắng là tự làm sổ mù: khi order đó bị huỷ, sổ trống và engine tưởng
     *   không còn ai — trong khi Collection Offer 0.0138 vẫn nằm đó. Giữ cả
     *   danh sách thì huỷ một cái là biết ngay cái kế tiếp, không cần hỏi lại.
     */
    let orders = [];
    /**
     * LƯỢT ĐỌC ĐẦU ĐỨNG TRƯỚC MỌI LƯỢT ĐỌC DO SỰ KIỆN SINH RA
     *
     *   Cổng lượt-đọc-đầu chặn hàng cho tới khi có câu trả lời; mọi giây nó
     *   chờ là một giây hàng đó không thể gửi gì. Trong khi đó Stream vẫn đẻ
     *   ra lượt đọc mới liên tục, và ở cùng hạng thì hàng mới luôn tới sau
     *   nhưng lại được phục vụ ngang. PRIORITY.INITIAL tồn tại đúng cho lời
     *   hứa này: bấm Start là mọi NFT sẽ được nhìn tới.
     */
    const best = await opensea.fetchBestOffer(
      this.chain, row.contract, row.tokenId, row.collectionSlug || "",
      { useCache: false, signal, priority: row.firstRead ? PRIORITY.INITIAL : PRIORITY.P0,
        collectOrders: list => { orders = Array.isArray(list) ? list : []; } });
    /**
     * KHÔNG ĐỌC ĐƯỢC ≠ KHÔNG AI ĐẶT
     *
     *   Bản đầu trả `null` cho CẢ HAI: REST hỏng (429, timeout, không giải
     *   được slug) và "200, danh sách trống". Engine hiểu `null` là "không ai
     *   đặt" và gửi Min — nên một lượt 429 lúc Start biến thành một offer 0.01
     *   trên NFT đang có Best 0.052. Đây là đường lọt qua cổng lượt-đọc-đầu.
     *
     *   Hỏng thì NÉM: recovery đếm lỗi, cổng lượt-đọc-đầu hẹn đọc lại, và
     *   không ai được kết luận gì. Đọc bị cắt trang (`partial`) cũng là chưa
     *   biết: Best có thể nằm ở trang chưa đọc.
     */
    if (!best || best.ok === false) {
      // 404 "No offers found" là câu trả lời của OpenSea (token không có offer
      // nào), không phải lỗi vận chuyển — xem fetchBestQuick.
      if (best && /HTTP 404|no offers found/i.test(String(best.reason || ""))) {
        return { empty: true, orders: [], knownEmpty: true };
      }
      throw new Error(`không đọc được offer của #${row.tokenId}: ${(best && best.reason) || "no-response"}`);
    }
    if (best.partial) {
      throw new Error(`đọc offer của #${row.tokenId} bị cắt trang (${(best.failedSources || []).join(",")})`);
    }
    const price = Number(best.price) || 0;
    // 200 và không có order nào: đó là câu trả lời "không ai đặt".
    if (price <= 0) return { empty: true, orders: [] };
    return {
      contract: String(row.contract).toLowerCase(),
      tokenId: String(row.tokenId),
      price,
      maker: String(best.maker || "").toLowerCase(),
      orderHash: String(best.orderHash || ""),
      // Hạn của order, để sổ tự loại nó đúng lúc mà không cần hỏi lại.
      endTime: Number(best.endTime || (best.order && best.order.endTime)) || 0,
      orders: orders
        .filter(o => o && o.orderHash && Number(o.pricePerItem) > 0)
        .map(o => ({
          orderHash: String(o.orderHash),
          price: Number(o.pricePerItem),
          maker: String(o.maker || "").toLowerCase(),
          kind: o.kind === "collection" ? "collection" : o.kind === "trait" ? "trait" : "item",
          endTime: Number(o.endTime) || 0,
          quantity: Number(o.quantity) || 1
        })),
      kind: best.kind === "collection" ? "collection"
        : best.kind === "trait" ? "trait" : "item"
    };
  }

  /**
   * LƯỢT ĐỌC BAN ĐẦU RẺ: chỉ order ĐANG DẪN ĐẦU của token (một request, một
   * trang), thay vì cả danh sách offer áp cho token (2 trang × N NFT).
   *
   *   `/offers/collection/{slug}/nfts/{id}/best` là cách OpenSea tự phân
   *   xử — đã tính cả collection và trait offer. Đó là đủ để trả lời câu hỏi
   *   duy nhất lượt đọc đầu phải trả lời: có đối thủ không, và cao nhất là
   *   bao nhiêu. `empty: true` là "200, không có offer" — câu trả lời "không
   *   ai đặt" duy nhất được phép dẫn tới Min. Mọi thất bại khác NÉM.
   *
   *   Phần còn lại của sổ (collection/trait offer đang mở) đến từ một lượt
   *   đọc theo COLLECTION, một lần cho mọi NFT cùng collection — xem
   *   `fetchCollectionOffersFor`. Item offer thấp hơn top không được đọc lúc
   *   này; khi top bị huỷ mà sổ không còn item nào, engine đọc lại đúng
   *   token đó một lần (gap recovery).
   */
  async fetchBestQuick(row, {signal} = {}) {
    if (!row.collectionSlug) return this.fetchBest(row, {signal});   // không slug: đường đầy đủ tự giải slug
    const r = await opensea.fetchBestOfferQuick(
      this.chain, row.contract, row.tokenId, row.collectionSlug,
      { signal, priority: row.firstRead ? PRIORITY.INITIAL : PRIORITY.P2 });
    // "Rỗng" chỉ đáng tin khi OpenSea nói KHÔNG CÓ offer. Top hết hạn / không
    // hoạt động (NOT_ACTIVE, EXPIRED) không nói gì về những order còn lại →
    // đọc cả danh sách thay vì kết luận "không ai".
    if (r && r.empty === true && r.reason !== "NO_OFFER") return this.fetchBest(row, {signal});
    if (r && r.empty === true) return { empty: true, orders: [] };
    // 404 "No offers found for NFT …" là CÂU TRẢ LỜI của OpenSea, không phải
    // lỗi vận chuyển: token này không có offer nào. Ném nó ra như 429/timeout
    // là đọc lại 5 giây một lần mãi mãi (đo trên log thật, #7348).
    if (r && r.ok !== true && /HTTP 404|no offers found/i.test(String(r.reason || ""))) {
      return { empty: true, orders: [], knownEmpty: true };
    }
    if (!r || r.ok !== true) {
      throw new Error(`không đọc được best của #${row.tokenId}: ${(r && r.reason) || "no-response"}`);
    }
    const o = r.order || {};
    const one = {
      orderHash: String(r.orderHash || o.orderHash || ""),
      price: Number(r.price) || 0,
      maker: String(o.maker || "").toLowerCase(),
      kind: r.kind === "collection" ? "collection" : r.kind === "trait" ? "trait" : "item",
      endTime: Number(o.endTime) || 0,
      quantity: Number(r.quantity) || 1
    };
    if (!one.orderHash || !(one.price > 0)) throw new Error(`best của #${row.tokenId} không đọc được giá/hash`);
    return { contract: String(row.contract).toLowerCase(), tokenId: String(row.tokenId),
      price: one.price, maker: one.maker, orderHash: one.orderHash, kind: one.kind,
      endTime: one.endTime, orders: [one] };
  }

  /**
   * Các collection offer từ endpoint của MỘT collection. Nếu API trả kèm
   * trait offer, nhận dạng bằng `criteria.traits` / `numeric_traits` hiện hành.
   * Chỉ fan-out tới NFT được `encoded_token_ids` bao phủ.
   *
   * @returns {Promise<Array<{orderHash:string, price:number, maker:string,
   *   kind:string, endTime:number, covers:(tokenId:string)=>boolean}>>}
   */
  async fetchCollectionOffersFor(slug, {signal} = {}) {
    if (!slug) return [];
    const raw = await opensea.fetchCollectionOffers(slug, { useCache: false, priority: PRIORITY.P2, signal });
    const out = [];
    const nowSec = Math.floor(Date.now() / 1000);
    for (const offer of raw || []) {
      const params = opensea.getProtocolParameters(offer);
      const price = offer && offer.price;
      const value = price && price.value;
      const decimals = Number(price && price.decimals);
      if (value === undefined || value === null || !Number.isFinite(decimals)) continue;
      let total;
      try { total = Number(ethers.formatUnits(BigInt(String(value)), decimals)); } catch { continue; }
      if (!(total > 0)) continue;
      const nftItem = (params && params.consideration || []).find(i => Number(i && i.itemType) >= 2);
      const qty = Math.max(1, Number(offer.remaining_quantity) || 0, Number(nftItem && nftItem.startAmount) || 0);
      const endTime = Number(params && params.endTime) || 0;
      if (endTime > 0 && endTime <= nowSec) continue;
      if (offer.status && String(offer.status).toUpperCase() !== "ACTIVE") continue;
      const hash = String(offer.order_hash || "");
      if (!hash) continue;
      const criteria = offer.criteria || null;
      out.push({
        orderHash: hash,
        price: total / qty,
        quantity: qty,
        maker: String(params && params.offerer || "").toLowerCase(),
        kind: opensea.isTraitCriteria(criteria) ? "trait" : "collection",
        endTime,
        covers: tokenId => opensea.criteriaCoversToken(offer, tokenId)
      });
    }
    // Cờ "đọc hỏng" đi xuyên qua tầng chuẩn hoá: người gọi phải phân biệt
    // được "collection này không có criteria offer nào" với "lượt đọc không
    // thành công", vì hai điều đó dẫn tới hai hành vi khác hẳn nhau.
    if (raw && raw.failed) { out.failed = true; out.reason = raw.reason || ""; }
    return out;
  }

  /**
   * Đọc lại những offer ví này đang mở. CHỈ recovery gọi.
   *
   * BA TẦNG, RẺ TRƯỚC — VÀ CHỈ ĐỌC RIÊNG NFT KHI THẬT SỰ MƠ HỒ
   *
   *   1. Ví: `/account/{wallet}/offers` lọc theo `targets`. 1–6 request cho
   *      cả trăm NFT. Kết thúc END_OF_DATA ⇒ mọi target đều "đã soát".
   *   2. Collection: khi cursor ví lặp (ví đang nhận/đặt offer liên tục),
   *      drain `/offers/collection/{slug}/all` cho từng collection mà target
   *      thuộc về — vài request thay cho trăm. Collection lớn không drain hết
   *      trong trần trang thì nhớ 10 phút để lượt sau không thử lại vô ích.
   *   3. Từng NFT: chỉ những target CHƯA được tầng 1/2 xác nhận. Hàng có dấu
   *      own (`ownHint`) đọc trước, đọc nền qua read dispatcher (key phụ
   *      trước), tối đa `perNftBudget` hàng mỗi lượt — phần còn lại trả về
   *      là "chưa soát" để engine mở khoá dần từng hàng đã soát và xin lượt
   *      kế tiếp CHỈ cho phần còn lại. Không hàng nào chờ hàng khác.
   *
   *   Kết quả luôn nói rõ hàng nào đã được một nguồn ĐẦY ĐỦ xác nhận
   *   (`covered`). `complete === true` khi và chỉ khi mọi target đều covered.
   *
   * @param {string} address
   * @param {{signal?:AbortSignal, targets?:Array<object>|null, perNftBudget?:number}} [opts]
   */
  async resyncOwnOrders(address, { signal, targets = null, perNftBudget = 24 } = {}) {
    if (!address) return { orders: [], complete: false, covered: [] };
    const me = String(address).toLowerCase();
    const scoped = Array.isArray(targets);
    const stats = { walletPages: 0, collectionReads: 0, nftReads: 0, source: "wallet" };

    const found = await opensea.fetchWalletOffers({
      chain: this.chain, walletAddress: address, maxPages: scoped ? 6 : 40, signal,
      filter: scoped ? targets : null,
      quiet: scoped
    });
    const byHash = new Map();
    for (const offer of found || []) {
      if (signal && signal.aborted) break;
      if (offer.criteria) continue;
      if (!offer.contract || !offer.tokenId) continue;
      byHash.set(String(offer.orderHash).toLowerCase(), {
        contract: String(offer.contract).toLowerCase(),
        tokenId: String(offer.tokenId),
        orderHash: offer.orderHash,
        price: Number(offer.price) || 0,
        endTime: Number(offer.endTime) || 0
      });
    }
    const keyOf = t => `${String(t.contract || "").toLowerCase()}:${String(t.tokenId || "")}`;
    const wanted = scoped ? targets.map(row => ({
      key: keyOf(row),
      contract: String(row.contract || "").toLowerCase(),
      tokenId: String(row.tokenId || ""),
      collectionSlug: String(row.collectionSlug || ""),
      ownHint: Boolean(row.ownHint)
    })).filter(t => t.contract && t.tokenId) : [];

    if (found && found.complete === true) {
      return { orders: [...byHash.values()], complete: true,
        covered: wanted.map(t => t.key), stats };
    }
    if (!scoped || !wanted.length) return { orders: [...byHash.values()], complete: false, covered: [], stats };

    const covered = new Set();
    const now = Date.now();

    // ---- tầng 2: theo collection ---------------------------------------
    stats.source = "collection";
    const groups = new Map();
    for (const t of wanted) {
      if (signal?.aborted) break;
      let slug = t.collectionSlug;
      if (!slug) {
        try { slug = await opensea.resolveCollectionSlug(this.chain, t.contract, t.tokenId); } catch { slug = ""; }
        t.collectionSlug = slug || "";
      }
      if (!slug) continue;
      if (!groups.has(slug)) groups.set(slug, []);
      groups.get(slug).push(t);
    }
    for (const [slug, members] of groups) {
      if (signal?.aborted) break;
      const skipUntil = this._collectionDrainSkip.get(slug) || 0;
      if (skipUntil > now) continue;
      try {
        const all = await opensea.fetchCollectionAllOffers(slug, {
          limit: 100, maxPages: 4, signal, priority: PRIORITY.P2
        });
        stats.collectionReads += all.pages || 1;
        const memberKeys = new Set(members.map(m => m.key));
        for (const order of all) {
          const own = opensea.ownItemFromOrder(order, me);
          if (!own) continue;
          const k = `${own.contract}:${own.tokenId}`;
          if (!memberKeys.has(k)) continue;
          byHash.set(String(own.orderHash).toLowerCase(), own);
        }
        if (all.complete) {
          for (const m of members) covered.add(m.key);
        } else {
          // Collection quá lớn cho trần trang: không thử lại trong 10 phút.
          this._collectionDrainSkip.set(slug, now + 10 * 60 * 1000);
        }
      } catch {
        /* tầng 3 sẽ lo phần còn lại */
      }
    }

    // ---- tầng 3: từng NFT, chỉ phần còn mơ hồ, có ngân sách -------------
    const remaining = wanted.filter(t => !covered.has(t.key));
    if (remaining.length) stats.source = "nft";
    remaining.sort((a, b) => Number(b.ownHint) - Number(a.ownHint));
    const queue = remaining.slice(0, Math.max(0, Number(perNftBudget) || 0));
    const worker = async () => {
      while (queue.length) {
        if (signal?.aborted) return;
        const t = queue.shift();
        try {
          const slug = t.collectionSlug ||
            await opensea.resolveCollectionSlug(this.chain, t.contract, t.tokenId);
          if (!slug) continue;
          const raw = await opensea.fetchNftOffers(this.chain, slug, t.tokenId, {
            limit: 50, maxPages: 3, signal, kind: KIND.READ, priority: PRIORITY.P2
          });
          stats.nftReads++;
          for (const order of raw) {
            const own = opensea.ownItemFromOrder(order, me);
            if (!own) continue;
            byHash.set(String(own.orderHash).toLowerCase(),
              { ...own, contract: t.contract, tokenId: t.tokenId });
          }
          if (raw.complete) covered.add(t.key);
        } catch {
          /* hàng này giữ nguyên "chưa soát" */
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(3, queue.length) }, worker));

    const complete = wanted.every(t => covered.has(t.key));
    return { orders: [...byHash.values()], complete, covered: [...covered],
      uncovered: wanted.length - covered.size, stats };
  }

  census() {
    return {
      chain: this.chain,
      configLoaded: Boolean(this._config),
      chainId: this._config ? this._config.chainId : null,
      counter: this._account ? this._account.counter : null,
      wethKnown: this._balance ? this._balance.known : false,
      collections: this._collections.size
    };
  }
}

/** Ethereum. Không có gì đặc thù ngoài những gì lấy từ cấu hình chung. */
class EthereumAdapter extends ChainAdapter {
  constructor(opts = {}) { super({ ...opts, chain: "ethereum" }); }
}

/**
 * Robinhood.
 *
 * Cùng giao thức Seaport 1.6, cùng hình dạng API, khác chainId/WETH/conduit.
 * Những khác biệt đó đã nằm trong `opensea.js` và được đọc qua lớp cha, nên
 * lớp này tồn tại để NÓI RÕ rằng Robinhood là một chain riêng được kiểm
 * riêng — không phải một nhánh `if` lẫn trong mã Ethereum.
 */
class RobinhoodAdapter extends ChainAdapter {
  constructor(opts = {}) { super({ ...opts, chain: "robinhood" }); }
}

function adapterFor(chain, opts = {}) {
  const c = opensea.normalizeChain(chain);
  if (c === "robinhood") return new RobinhoodAdapter(opts);
  return new EthereumAdapter(opts);
}

module.exports = {
  ChainAdapter, EthereumAdapter, RobinhoodAdapter, adapterFor,
  ACCOUNT_TTL_MS, COLLECTION_TTL_MS
};
