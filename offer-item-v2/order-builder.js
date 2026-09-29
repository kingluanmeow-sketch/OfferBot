"use strict";

/**
 * order-builder.js — dựng và ký một Seaport offer HOÀN TOÀN CỤC BỘ.
 *
 * VÌ SAO PHẢI VIẾT LẠI THAY VÌ GỌI seaport-js
 *
 *   Đường cũ gọi `seaport.createOrder(...).executeAllActions()`. Đo trên
 *   chính mã đó, giữa lúc biết giá và lúc có chữ ký có tới sáu lượt chờ mạng:
 *   metadata collection, phí collection, tạo provider, dựng session, đọc số
 *   dư, và dò chuẩn token — chưa kể `createOrder` tự đi đọc counter, còn
 *   `executeAllActions` có thể GỬI một giao dịch approve WETH rồi chờ nó được
 *   đào. Trên một RPC chậm, thứ đó không bao giờ xong.
 *
 *   Với Offer Item, khoảng từ "biết giá" tới "có chữ ký" phải là công việc
 *   thuần CPU. Mọi thứ trong đó không phụ thuộc giá đối thủ đều nạp sẵn lúc
 *   Start, và `build()` chỉ thay giá, thời gian, salt rồi ký.
 *
 * KHÔNG CÓ MỘT LỆNH GỌI MẠNG NÀO TRONG FILE NÀY
 *
 *   Đó là bất biến, và có test riêng canh nó. Nếu một ngày cần thêm dữ liệu,
 *   dữ liệu đó phải vào `Template` lúc hydrate, không được lẻn vào `build()`.
 *
 * KÝ BẰNG EIP-712 TRỰC TIẾP
 *
 *   `ethers.Wallet.signTypedData` là toán cục bộ trên khoá riêng. Không
 *   provider, không RPC, không thể treo.
 */

const { ethers } = require("ethers");

/** Seaport 1.6, địa chỉ canonical cross-chain. Đã đối chiếu tài liệu hiện hành. */
const SEAPORT_V1_6 = "0x0000000000000068f116a894984e2db1123eb395";

const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000";
const ZERO_BYTES32 =
  "0x0000000000000000000000000000000000000000000000000000000000000000";

/** itemType của Seaport. */
const ITEM_TYPE = Object.freeze({
  NATIVE: 0, ERC20: 1, ERC721: 2, ERC1155: 3,
  ERC721_WITH_CRITERIA: 4, ERC1155_WITH_CRITERIA: 5
});

/**
 * orderType của Seaport.
 *
 *   0 FULL_OPEN        ai cũng khớp được
 *   2 FULL_RESTRICTED  chỉ zone cho phép mới khớp
 *
 * OpenSea dùng FULL_RESTRICTED cùng Signed Zone khi collection có phí bắt
 * buộc. Chọn sai thì order bị từ chối, hoặc tệ hơn là khớp được mà không trả
 * phí cho creator.
 */
const ORDER_TYPE = Object.freeze({ FULL_OPEN: 0, FULL_RESTRICTED: 2 });

/** Kiểu EIP-712 của Seaport. Phải khớp từng chữ với contract. */
const EIP712_TYPES = Object.freeze({
  OrderComponents: [
    { name: "offerer", type: "address" },
    { name: "zone", type: "address" },
    { name: "offer", type: "OfferItem[]" },
    { name: "consideration", type: "ConsiderationItem[]" },
    { name: "orderType", type: "uint8" },
    { name: "startTime", type: "uint256" },
    { name: "endTime", type: "uint256" },
    { name: "zoneHash", type: "bytes32" },
    { name: "salt", type: "uint256" },
    { name: "conduitKey", type: "bytes32" },
    { name: "counter", type: "uint256" }
  ],
  OfferItem: [
    { name: "itemType", type: "uint8" },
    { name: "token", type: "address" },
    { name: "identifierOrCriteria", type: "uint256" },
    { name: "startAmount", type: "uint256" },
    { name: "endAmount", type: "uint256" }
  ],
  ConsiderationItem: [
    { name: "itemType", type: "uint8" },
    { name: "token", type: "address" },
    { name: "identifierOrCriteria", type: "uint256" },
    { name: "startAmount", type: "uint256" },
    { name: "endAmount", type: "uint256" },
    { name: "recipient", type: "address" }
  ]
});

/**
 * Mọi thứ KHÔNG phụ thuộc giá đối thủ, nạp một lần lúc Start.
 *
 * Mỗi trường ở đây là một lượt chờ mạng đã bị đẩy ra khỏi đường nóng. Thêm
 * một trường vào đây là đúng; đọc nó trong `build()` là sai.
 */
class Template {
  /**
   * @param {object} o
   * @param {string} o.chain
   * @param {number} o.chainId          để ký EIP-712; đọc một lần lúc Start
   * @param {string} o.wallet           địa chỉ người đặt offer
   * @param {string} o.wethAddress
   * @param {string} o.conduitKey
   * @param {boolean} o.requiresSignedZone
   * @param {string} o.signedZone       địa chỉ zone khi bị hạn chế
   * @param {Array<{recipient:string, basisPoints:number}>} o.fees
   * @param {string|number|bigint} o.counter  Seaport counter của ví — uint256,
   *                                    giữ dạng chuỗi thập phân (xem chain-adapters)
   * @param {string} o.tokenStandard    "erc721" | "erc1155"
   * @param {string} o.contract
   * @param {string} o.tokenId
   */
  constructor(o) {
    this.chain = String(o.chain || "").toLowerCase();
    this.chainId = Number(o.chainId);
    this.wallet = ethers.getAddress(o.wallet);
    this.wethAddress = ethers.getAddress(o.wethAddress);
    this.conduitKey = String(o.conduitKey);
    this.requiresSignedZone = Boolean(o.requiresSignedZone);
    this.signedZone = o.signedZone ? ethers.getAddress(o.signedZone) : ZERO_ADDRESS;
    this.fees = (o.fees || []).map(f => ({
      recipient: ethers.getAddress(f.recipient),
      basisPoints: Number(f.basisPoints)
    }));
    this.counter = normalizeCounter(o.counter);
    this.tokenStandard = String(o.tokenStandard || "erc721").toLowerCase();
    this.contract = ethers.getAddress(o.contract);
    this.tokenId = String(o.tokenId);

    /** Khi nào những thứ trên được nạp, để có chính sách hết hạn rõ ràng. */
    this.hydratedAt = Number(o.hydratedAt) || Date.now();

    this.validate();
  }

  validate() {
    if (!Number.isFinite(this.chainId) || this.chainId <= 0) {
      throw new Error("Template thiếu chainId — không ký EIP-712 được.");
    }
    if (!/^0x[0-9a-fA-F]{64}$/.test(this.conduitKey)) {
      throw new Error("conduitKey không đúng dạng bytes32.");
    }
    if (this.tokenStandard !== "erc721" && this.tokenStandard !== "erc1155") {
      // Đoán chuẩn token là cách một order bị OpenSea từ chối SAU KHI đã tiêu
      // một chữ ký. Thà từ chối ở đây.
      throw new Error(`tokenStandard không xác định: ${this.tokenStandard}`);
    }
    for (const fee of this.fees) {
      if (!Number.isFinite(fee.basisPoints) || fee.basisPoints < 0) {
        throw new Error("fee basisPoints không hợp lệ.");
      }
    }
  }

  /** Bao nhiêu tổng basis point đi cho phí. */
  totalFeeBps() {
    return this.fees.reduce((n, f) => n + f.basisPoints, 0);
  }

  /** Template còn dùng được không, theo chính sách hết hạn của bên gọi. */
  isFresh(ttlMs, now = Date.now()) {
    return now - this.hydratedAt < ttlMs;
  }
}

/**
 * Salt duy nhất cho mỗi order.
 *
 * Hai order giống hệt nhau về mọi mặt sẽ có cùng order hash, và cái thứ hai
 * là một bản trùng mà Seaport coi là cùng một order. Salt là thứ tách chúng
 * ra. Dùng ngẫu nhiên mật mã: một bộ đếm sẽ trùng giữa hai cửa sổ cùng ví.
 */
/**
 * Counter về dạng chuỗi thập phân của một uint256. Ném nếu không phải số
 * nguyên không âm: một counter sai là một chữ ký vô nghĩa, và thà chết ở đây.
 */
function normalizeCounter(value) {
  if (typeof value === "bigint") return value.toString();
  const s = String(value ?? "0").trim();
  if (/^\d+$/.test(s)) return BigInt(s).toString();
  // Number đã bị làm tròn (dạng 2.89e+39) là dấu hiệu mất chính xác — từ chối.
  throw new Error(`counter không phải số nguyên uint256: ${s}`);
}

function freshSalt() {
  return BigInt("0x" + ethers.hexlify(ethers.randomBytes(24)).slice(2)).toString();
}

/**
 * Dựng OrderComponents cho một offer WETH lên một NFT.
 *
 * THUẦN TÍNH TOÁN. Không await, không mạng.
 *
 * @param {Template} t
 * @param {object} o
 * @param {string|bigint} o.amountWei   giá offer, wei
 * @param {number} o.startTime          giây epoch
 * @param {number} o.endTime            giây epoch
 * @param {string} [o.salt]
 * @returns {object} OrderComponents
 */
function buildOrderComponents(t, { amountWei, startTime, endTime, salt }) {
  const total = BigInt(amountWei);
  if (total <= 0n) throw new Error("amountWei phải > 0.");

  /**
   * PHÍ TRỪ VÀO OFFER, KHÔNG CỘNG THÊM
   *
   *   Người đặt offer bỏ ra đúng `total` WETH. Người bán nhận phần còn lại
   *   sau phí, và mỗi người nhận phí nhận phần của họ — tất cả lấy TRONG
   *   `total`. Cộng phí lên trên sẽ tiêu nhiều hơn con số người dùng đặt Max,
   *   và đó là tiền của họ.
   *
   *   Chia theo basis point rồi đưa phần dư cho người bán: tổng các khoản
   *   consideration phải bằng đúng `total`, nếu lệch một wei thì Seaport từ
   *   chối khớp.
   */
  const considerations = [];
  let feeSum = 0n;
  for (const fee of t.fees) {
    const part = (total * BigInt(fee.basisPoints)) / 10000n;
    if (part <= 0n) continue;
    feeSum += part;
    considerations.push({
      itemType: ITEM_TYPE.ERC20,
      token: t.wethAddress,
      identifierOrCriteria: "0",
      startAmount: part.toString(),
      endAmount: part.toString(),
      recipient: fee.recipient
    });
  }

  const nftItem = {
    itemType: t.tokenStandard === "erc1155" ? ITEM_TYPE.ERC1155 : ITEM_TYPE.ERC721,
    token: t.contract,
    identifierOrCriteria: t.tokenId,
    startAmount: "1",
    endAmount: "1",
    recipient: t.wallet
  };

  // NFT đứng TRƯỚC trong consideration: đó là thứ người đặt offer nhận về.
  const consideration = [nftItem, ...considerations];

  return {
    offerer: t.wallet,
    zone: t.requiresSignedZone ? t.signedZone : ZERO_ADDRESS,
    offer: [{
      itemType: ITEM_TYPE.ERC20,
      token: t.wethAddress,
      identifierOrCriteria: "0",
      startAmount: total.toString(),
      endAmount: total.toString()
    }],
    consideration,
    orderType: t.requiresSignedZone ? ORDER_TYPE.FULL_RESTRICTED : ORDER_TYPE.FULL_OPEN,
    startTime: String(startTime),
    endTime: String(endTime),
    zoneHash: ZERO_BYTES32,
    salt: salt || freshSalt(),
    conduitKey: t.conduitKey,
    counter: String(t.counter),

    // Không nằm trong EIP-712, nhưng phần lớn API muốn nó trong payload.
    totalOriginalConsiderationItems: consideration.length,

    // Ghi lại để đối chiếu, không đi vào chữ ký.
    _feeSum: feeSum.toString()
  };
}

/** Miền EIP-712 của Seaport trên một chain. */
function domainFor(chainId) {
  return {
    name: "Seaport",
    version: "1.6",
    chainId: Number(chainId),
    verifyingContract: SEAPORT_V1_6
  };
}

/** Chỉ những trường ĐI VÀO chữ ký. Thừa một trường là chữ ký sai. */
/**
 * Thân request POST offer, đúng hình dạng OpenSea nhận.
 *
 * MỘT BẢN DỰNG, KHÔNG PHẢI HAI
 *
 *   Hình dạng này từng chỉ sống trong `chain-adapters.js`, nên kết quả của
 *   `build()` chưa phải là thứ gửi được — người đọc nó vẫn phải đi qua một
 *   file khác để biết cái gì thật sự lên đường dây. Hai chỗ dựng cùng một
 *   thân request là hai chỗ để chúng trôi ra khỏi nhau, và sai lệch đó chỉ
 *   lộ ra khi OpenSea từ chối một order ĐÃ TIÊU một chữ ký.
 *
 *   Nên nó nằm ở đây, cạnh thứ đã ký nó, và adapter gọi lại hàm này.
 *
 * `counter` NẰM TRONG parameters
 *
 *   Nó không thuộc struct đã ký (xem `signableFrom`), nhưng OpenSea đọc nó
 *   trong `parameters`. Bỏ nó ra là order bị từ chối; đưa nó vào struct ký là
 *   chữ ký sai. Hai chuyện khác nhau, và đây là chỗ chúng gặp nhau.
 */
function openseaOfferPayload(signed) {
  const c = signed.components;
  return {
    parameters: {
      offerer: c.offerer,
      zone: c.zone,
      offer: c.offer,
      consideration: c.consideration,
      orderType: c.orderType,
      startTime: c.startTime,
      endTime: c.endTime,
      zoneHash: c.zoneHash,
      salt: c.salt,
      conduitKey: c.conduitKey,
      totalOriginalConsiderationItems: c.totalOriginalConsiderationItems,
      counter: c.counter
    },
    signature: signed.signature,
    protocol_address: signed.protocolAddress
  };
}

function signableFrom(components) {
  return {
    offerer: components.offerer,
    zone: components.zone,
    offer: components.offer,
    consideration: components.consideration,
    orderType: components.orderType,
    startTime: components.startTime,
    endTime: components.endTime,
    zoneHash: components.zoneHash,
    salt: components.salt,
    conduitKey: components.conduitKey,
    counter: components.counter
  };
}

/**
 * ORDER HASH — struct hash, KHÔNG PHẢI digest chữ ký.
 *
 * HAI CON SỐ KHÁC NHAU, VÀ TÔI ĐÃ NHẦM CHÚNG
 *
 *   digest chữ ký = keccak256(0x1901 ‖ domainSeparator ‖ structHash)
 *                 = TypedDataEncoder.hash(domain, types, value)
 *   order hash    = structHash
 *                 = TypedDataEncoder.hashStruct("OrderComponents", types, value)
 *
 *   Bản đầu trả về digest chữ ký và gọi nó là order hash. Nó vẫn khớp giữa
 *   hai lần gọi cục bộ, nên test parity với seaport-js xanh; nó chỉ lộ ra khi
 *   hỏi chính contract: `Seaport.getOrderHash()` trả về một con số khác hẳn,
 *   trên CẢ hai chain.
 *
 *   Hậu quả nếu để nguyên: OpenSea và Stream báo `order_hash` theo struct
 *   hash. Sổ lệnh khớp huỷ, bia mộ và "offer của chính mình" đều theo order
 *   hash. Dùng digest chữ ký nghĩa là không lần nào khớp — bot sẽ không nhận
 *   ra chính offer của nó, và một lệnh đã huỷ sẽ không bao giờ bị gỡ.
 *
 *   Chữ ký KHÔNG bị ảnh hưởng: `signTypedData` tự tính digest bên trong.
 */
function orderHashOf(components) {
  return ethers.TypedDataEncoder.hashStruct(
    "OrderComponents", EIP712_TYPES, signableFrom(components));
}

/** Digest EIP-712 — thứ được ký. Khác order hash; xem chú thích trên. */
function signingDigest(components, chainId) {
  return ethers.TypedDataEncoder.hash(
    domainFor(chainId), EIP712_TYPES, signableFrom(components));
}

/**
 * Người dựng offer cho một dashboard. Giữ ví và template.
 */
class LocalOrderBuilder {
  /**
   * @param {object} o
   * @param {string} o.privateKey  KHÔNG bao giờ được log
   */
  constructor({ privateKey }) {
    // Ví KHÔNG có provider. Đó là chủ ý: không provider thì không có đường
    // nào để một lệnh gọi mạng lẻn vào đường ký.
    this.wallet = new ethers.Wallet(String(privateKey));
    this.address = this.wallet.address;
    /** tokenKey -> Template */
    this.templates = new Map();
    this.stats = { built: 0, signed: 0, rejected: 0 };
  }

  /** Nạp template cho một NFT. Gọi lúc Start, không gọi lúc có sự kiện. */
  hydrate(tokenKey, templateInput) {
    const t = new Template({ ...templateInput, wallet: this.address });
    this.templates.set(tokenKey, t);
    return t;
  }

  get(tokenKey) { return this.templates.get(tokenKey) || null; }
  forget(tokenKey) { return this.templates.delete(tokenKey); }
  clear() { this.templates.clear(); }

  /**
   * Dựng + ký. Thuần cục bộ.
   *
   * @returns {Promise<object>} { components, signature, orderHash, domain }
   */
  async build(tokenKey, { amountWei, durationMinutes, now = Date.now(), salt, onStage = () => {} }) {
    const t = this.templates.get(tokenKey);
    if (!t) throw new Error(`chưa hydrate template cho ${tokenKey}`);

    const startTime = Math.floor(now / 1000);
    const minutes = Math.max(1, Math.floor(Number(durationMinutes) || 15));
    const endTime = startTime + minutes * 60;

    onStage("build_started");
    const components = buildOrderComponents(t, {
      amountWei, startTime, endTime, salt
    });
    this.stats.built++;
    onStage("build_finished");
    onStage("sign_started");

    // `signTypedData` của ethers là toán trên khoá riêng: không provider,
    // không RPC, không thể treo.
    const signature = await this.wallet.signTypedData(
      domainFor(t.chainId), EIP712_TYPES, signableFrom(components));
    this.stats.signed++;
    onStage("sign_finished");

    const signed = {
      components,
      signature,
      orderHash: orderHashOf(components),
      signingDigest: signingDigest(components, t.chainId),
      domain: domainFor(t.chainId),
      /**
       * Bộ typed data ĐẦY ĐỦ, không chỉ mỗi domain.
       *
       *   EIP-712 là bộ ba (domain, types, message). Trả về mỗi domain thì
       *   người đọc kết quả này phải tự đi lấy hai phần kia ở chỗ khác và tự
       *   dựng lại `message` — và một bản dựng lại hơi khác bản đã ký sẽ cho
       *   ra một chữ ký "hợp lệ" cho một order khác. Đây chính xác là hình
       *   dạng đã được ký, nên mọi thứ kiểm chứng sau đó (mô phỏng on-chain,
       *   so khớp hash) nói về đúng cái order đã ký.
       */
      typedData: {
        domain: domainFor(t.chainId),
        types: EIP712_TYPES,
        primaryType: "OrderComponents",
        message: signableFrom(components)
      },
      protocolAddress: SEAPORT_V1_6,
      chain: t.chain
    };
    signed.payload = openseaOfferPayload(signed);
    return signed;
  }

  census() {
    return { templates: this.templates.size, ...this.stats };
  }
}

module.exports = {
  LocalOrderBuilder, Template,
  buildOrderComponents, orderHashOf, signingDigest, domainFor, signableFrom, freshSalt, openseaOfferPayload,
  SEAPORT_V1_6, ZERO_ADDRESS, ZERO_BYTES32, ITEM_TYPE, ORDER_TYPE, EIP712_TYPES
};
