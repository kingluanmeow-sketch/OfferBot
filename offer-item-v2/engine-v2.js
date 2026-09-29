"use strict";

/**
 * engine-v2.js — Offer Item, hoàn toàn hướng sự kiện.
 *
 * ĐƯỜNG NÓNG, VÀ KHÔNG CÓ GÌ KHÁC TRONG ĐÓ
 *
 *   frame Stream
 *   → chuẩn hoá
 *   → cập nhật sổ trong bộ nhớ
 *   → tính effective best
 *   → tính target
 *   → ghi ý định (mới nhất thắng)
 *   → dựng order cục bộ
 *   → ký cục bộ
 *   → xin giấy phép ghi
 *   → POST
 *
 *   Không REST. Không polling. Không debounce. Không tick scheduler. Không
 *   thời gian chờ cố định. Nếu sự kiện cộng trạng thái trong bộ nhớ đã đủ để
 *   biết nên gửi giá nào, thì giá đó có NGAY trong cùng một lượt gọi.
 *
 * SUBSCRIBE TRƯỚC, HYDRATE SAU
 *
 *   Thứ tự ngược lại tạo ra một khoảng mù: hydrate REST mất vài giây tới vài
 *   chục giây, và mọi sự kiện xảy ra trong quãng đó bị mất hẳn. Ở đây Stream
 *   được đăng ký TRƯỚC, sự kiện được đệm lại, rồi mới hydrate, rồi phát lại
 *   đệm theo đúng thứ tự. Không có khoảng nào không ai nghe.
 *
 * CÁI GÌ KHÔNG THUỘC VỀ ĐÂY
 *
 *   Bộ điều phối này không biết gì về Offer SLL, Offer Custom hay Cancel. Nó
 *   cũng không sở hữu quota broker — broker là của cả máy, dùng chung giữa
 *   các cửa sổ, và engine chỉ xin giấy phép.
 */

const { ethers } = require("ethers");

const { toBookOp, tokenKey, OP } = require("./event-normalizer");
const { MemoryBook } = require("./memory-book");
const { decide, STATUS } = require("./decision");
const { normalizeMaxDown } = require("./pricing");
const { IntentStore, INTENT } = require("./intent-store");
const { LocalOrderBuilder } = require("./order-builder");
const { fingerprint: quotaFingerprint } = require("./quota-broker");
// Cùng lý do như trong bridge.js: giải ở thời điểm dựng để smoke vá được.
const submitterModule = require("./submitter");
const { readPostedOrder, retryAfterMs } = submitterModule;
const { RecoveryPlane } = require("./recovery-plane");
const { TraitSnapshot } = require("./trait-snapshot");
// Dùng CHÍNH bộ nhận diện mà phần còn lại của app dùng. Một danh sách mẫu câu
// thứ hai sẽ trôi khỏi cái thứ nhất, và lúc đó hai nửa sản phẩm bất đồng về
// việc ví có đủ tiền hay không.
const opensea = require("../opensea");
const devRuntime = require("../dev-runtime");
const { Metrics, mono } = require("./metrics");
const { PRIORITY } = require("../rate-limiter");
const productionTrace = require("../production-trace");

const STATE = Object.freeze({
  IDLE: "IDLE",
  HYDRATING: "HYDRATING",
  RUNNING: "RUNNING",
  SUSPENDED: "SUSPENDED",
  STOPPED: "STOPPED"
});

/**
 * Bao nhiêu token được tính lại trong MỘT lượt đồng bộ.
 *
 * Đủ lớn để phần lớn sự kiện xử lý trọn trong một lượt; đủ nhỏ để một
 * collection offer chạm 1000 token không khoá event loop. Xem `apply()`.
 */
const EVAL_CHUNK = 100;

/**
 * MỖI LƯỢT BƠM CHỌN ĐÚNG MỘT NGƯỜI ĐI TIẾP
 *
 * Trước đây một lượt bơm khởi động tới tám lượt gửi, và cả tám cùng nằm chờ
 * trong broker. Controller ghi adaptive theo API key, nên
 * tám chỗ trong hàng không đi nhanh hơn một chỗ — chúng chỉ khoá trước thứ tự
 * phục vụ ở một thời điểm cũ. Xem `MAX_ACQUIRING`.
 */

/**
 * Bao nhiêu lượt gửi được phép ĐANG BAY cùng lúc cho một engine.
 *
 * Đây mới là cái chặn thật, không phải số lượt khởi động mỗi tick. Ký một
 * order tốn ~1,2ms CPU thuần; một đợt bùng 100 sự kiện khởi động 100 lượt ký
 * và đo được độ trễ event loop p99 = 120ms. Trong 120ms đó mọi sự kiện khác
 * đứng chờ — kể cả cú vượt giá đang cần phản ứng ngay.
 *
 * Sáu, bằng số socket HTTP: nhiều hơn thì cũng phải xếp hàng ở tầng HTTP, và
 * hạn mức ghi phía sau còn hẹp hơn nữa. Ký nhiều hơn mức có thể gửi là ký để
 * rồi vứt — mọi order thừa sẽ bị bỏ ở lần kiểm "đã cũ" ngay trước POST.
 *
 * Không mất gì: ý định là latest-wins, nên thứ bị hoãn không phải một cơ hội
 * bị bỏ lỡ mà là một con số sẽ được tính lại bằng giá mới hơn.
 */
const MAX_INFLIGHT_SUBMITS = 6;

/**
 * MỖI ENGINE CHỈ MỘT LƯỢT XIN GIẤY PHÉP ĐANG CHỜ
 *
 *   Trước đây engine đẩy tới sáu lượt `acquire` vào broker cùng lúc: broker
 *   giữ sáu yêu cầu của CÙNG một cửa sổ, và log đọc ra "in-flight=6, hàng=6"
 *   trong khi chưa có POST nào. Ba hệ quả đo được:
 *
 *     - tồn đọng của engine bị đếm nhầm thành sáu lượt gửi đang bay;
 *     - một cửa sổ nhiều NFT chiếm hàng chờ chung của broker, cửa sổ ít NFT
 *       phải xếp sau sáu yêu cầu đó;
 *     - thứ tự phục vụ bị đóng băng lúc xin: sáu yêu cầu đã nằm trong broker
 *       thì một NFT vừa bị vượt giá phải chờ hết sáu cái đó.
 *
 *   Controller cũ chỉ cấp một lượt chậm, nên giữ nhiều hơn một chỗ trong hàng
 *   không tăng thông lượng — nó chỉ chuyển hàng đợi từ
 *   engine (nơi có thể sắp lại theo giá mới nhất) sang broker (nơi không thể).
 *   Một lượt xin, tồn đọng nằm ở engine, và chọn ai đi tiếp ngay trước khi
 *   xin: mới nhất, công bằng nhất.
 */
// Several different NFTs may wait for independent per-key controllers at the
// same time. Identity locks still serialize one NFT/order; this only removes
// the engine-wide head-of-line queue created by MAX_ACQUIRING=1.
const MAX_ACQUIRING = 2;

/**
 * Khi Stream KHÔNG khoẻ, đọc lại Best qua REST mỗi quãng này.
 *
 * Đây không phải nhịp quét của kiến trúc cũ quay lại: nó CHỈ tồn tại trong
 * lúc Stream hỏng, chạy trên mặt phẳng nền, và tắt ngay khi Stream trở lại.
 * Xem `watchStreamHealth`.
 */
const DEGRADED_REHYDRATE_MS = 45 * 1000;
/** Path B staging cap: at most four candidate submissions per second. */
const RECOVERY_CANDIDATE_INTERVAL_MS = 250;
/** Repeated reconnect/degraded signals coalesce into one full sweep per window. */
const FULL_RECOVERY_COOLDOWN_MS = 60 * 1000;
/** Bao lâu kiểm sức khoẻ Stream một lần. Rẻ: chỉ đọc một chuỗi trong bộ nhớ. */
const STREAM_HEALTH_POLL_MS = 5 * 1000;

/** Sau một lần bị chặn vì thiếu tiền, bao lâu mới đọc lại số dư. */
const BALANCE_PROBE_COOLDOWN_MS = 30 * 1000;
/**
 * Ceiling for the LOW_BALANCE recovery back-off. One wallet-level timer, only
 * while at least one running row is LOW_BALANCE: 30s, 60s, 120s, … ≤ 5 min.
 */
const BALANCE_PROBE_MAX_GAP_MS = 5 * 60 * 1000;
/** A NEW LOW_BALANCE is verified at once; this floor only coalesces a burst. */
const BALANCE_PROBE_URGENT_FLOOR_MS = 2 * 1000;
/** A probe dispatched longer ago than this is treated as lost (RecoveryPlane only reports success). */
const BALANCE_PROBE_INFLIGHT_MS = 20 * 1000;
/**
 * A balance read younger than this answers a NEW low-balance incident without
 * another RPC (1.25.11): one fresh read serves every row blocked in the window.
 */
const BALANCE_RESULT_FRESH_MS = 10 * 1000;

/** Mỗi token tối đa một lượt hỏi phạm vi trait trong quãng này. */
const TRAIT_PROBE_COOLDOWN_MS = 60 * 1000;
/** Và tối đa bấy nhiêu token cho MỘT sự kiện trait offer. */
const TRAIT_PROBE_MAX_PER_EVENT = 8;
/**
 * Nhịp dọn lệnh hết hạn theo đồng hồ local. Chỉ quét bộ nhớ; 10 giây là đủ
 * mịn cho hạn tính bằng phút và đủ thưa để không đáng kể với 1000 sổ.
 */
const EXPIRY_SWEEP_MS = 10 * 1000;
/**
 * Hạn chờ lượt ghi — LƯỚI AN TOÀN, không phải cơ chế lập lịch. Broker nhìn
 * capacity của MỌI key nên một lượt chỉ chờ khi toàn pool thật sự hết chỗ;
 * hết hạn thì xếp lại đúng chỗ cũ (chưa ký gì, không mất gì).
 */
const ACQUIRE_TIMEOUT_MS = 60 * 1000;
/**
 * THỬ LẠI THEO LOẠI LỖI (1.25.0) — không còn một backoff 3→6→12→…→60s chung
 *
 *   quota      429 của MỘT key: key đó đã cooldown ở broker; lượt mới nhất
 *              được xin lại gần như ngay và broker cho đi key kia nếu rảnh.
 *   not-sent   request chắc chắn chưa rời máy (lỗi trước khi gửi xong body):
 *              thử lại nhanh, có trần.
 *   server     5xx: có thể server bận — backoff ngắn có jitter.
 *   ambiguous  POST có thể đã tới server: KHÔNG gửi lại mù — hàng chờ đọc
 *              đối soát của chính nó (ownUnknownAt), rồi mới tính lại.
 *   generic    lỗi khác/exception: backoff cũ nhưng trần 30s.
 */
const RETRY_POLICY = {
  quota:     { base: 250,  max: 2000 },
  "not-sent": { base: 500,  max: 5000 },
  server:    { base: 1000, max: 15000 },
  ambiguous: { base: 1000, max: 10000 },
  generic:   { base: 3000, max: 30000 }
};
const RETRY_BASE_MS = RETRY_POLICY.generic.base;
const RETRY_MAX_MS = RETRY_POLICY.generic.max;
/** Stream phải hỏng LIÊN TỤC bấy lâu mới bật đọc REST định kỳ (không bật vì một cú nối lại 1s). */
const DEGRADED_GRACE_MS = 15 * 1000;
/** Ý định WAITING lâu hơn thế mà không ai tính lại: airbag của watchdog (đếm). */
const WAITING_STUCK_MS = 60 * 1000;
/** Nhịp quét thử-lại và watchdog; rẻ, unref, chỉ khi RUNNING. */
const RETRY_SWEEP_MS = 1000;
const WATCHDOG_MS = 5000;
/** In-flight lâu hơn thế này là một promise không bao giờ về: thả khoá. */
const INFLIGHT_STUCK_MS = 180 * 1000;
/** Có việc, còn chỗ, mà không dispatch gì trong bấy lâu: bơm lại. */
const DISPATCH_STALL_MS = 30 * 1000;
/** Own order quá hạn hơn thế này mà còn trong sổ là nhịp dọn đã bỏ lỡ. */
const OWN_EXPIRY_TOLERANCE_S = 30;
/** Đọc lại một token sau khi top bị huỷ: tối đa một lần mỗi quãng này. */
const GAP_READ_COOLDOWN_MS = 60 * 1000;
/**
 * BỘ ĐIỀU PHỐI KHÔNG TIẾN TRIỂN — ngưỡng cơ sở, cộng thêm cooldown hợp lệ của
 * broker (nhịp adaptive kế tiếp + Retry-After). Khi có
 * việc chờ, MÁY NÀY phải thấy một giấy phép (của bất kỳ ai) trong vài giây;
 * 45 giây không thấy gì mà không có cooldown là treo, không phải chờ.
 */
const PROGRESS_STALL_MS = 45 * 1000;
/** Đọc lại (reconnect / gap / nhất quán) hỏng thì đọc lại theo bậc này, mãi. */
const READ_RETRY_MS = [5000, 15000, 30000, 60000];
/** Lượt đọc own theo hàng (chủ của WAITING own): 1s → 3s → 10s → 30s. */
const OWN_READ_RETRY_MS = [1000, 3000, 10000, 30000];
/**
 * Một lượt đọc `/best` chỉ được phép xoá lệnh ghi TRƯỚC mốc đọc trừ quãng
 * này: OpenSea index có trễ, và một order vừa vào sổ từ Stream vài giây
 * trước lượt đọc có thể chưa có trong câu trả lời.
 */
const READ_GRACE_MS = 30 * 1000;
/** Đọc nhất quán cho một collection (kênh Stream im lặng/rejoin): tối đa một lần mỗi quãng này. */
const CONSISTENCY_READ_COOLDOWN_MS = 10 * 60 * 1000;
/**
 * OFFER CỦA MÌNH HẾT HẠN → ĐẶT LẠI CHỈ TỪ TRẠNG THÁI CÓ THẨM QUYỀN
 *
 *   Sổ chỉ "đủ" để đặt lại khi CẢ DANH SÁCH của token đã được đọc gần đây
 *   (không phải chỉ `/best` — nó trả MỘT order và từng che Item Offer đối
 *   thủ) và Stream không có khoảng mù từ đó. Quá quãng này, hoặc có nợ đọc,
 *   hoặc Stream vừa gãy → xác nhận lại bằng một lượt đọc đầy đủ cho ĐÚNG
 *   token đó trước khi tính giá. Đo trên #44: sổ chỉ còn Collection 0,0036
 *   trong khi Item 0,0455 sống → bot đặt lại 0,0037.
 */
const RENEW_FRESH_MS = 15 * 60 * 1000;
/** Own order vừa POST cần thời gian index: bản ghi trẻ hơn thế không bị coi là LOST. */
const OWN_LOST_GRACE_MS = 60 * 1000;

/** Chờ xác nhận lại Best lâu hơn mức này thì NÓI RA MỘT LẦN (không bỏ cuộc). */
const RENEW_STUCK_WARN_MS = 90 * 1000;
/**
 * RESUME KHÔNG PHẢI START
 *
 *   Tạm dừng không tháo engine: sổ, template, trait, Stream đều còn và vẫn
 *   được cập nhật suốt lúc dừng. Nên Bắt đầu lại chỉ là bật lại quyền ghi và
 *   tính lại từ sổ HIỆN TẠI. Chỉ hàng nào sổ đã cũ hơn quãng này (hoặc có
 *   khoảng mù Stream sau lượt đọc cuối) mới cần đọc lại — và đọc ở nền, không
 *   chặn hàng khác. Đo trên 80 NFT: Start lạnh tốn hàng chục lượt REST và
 *   nhiều chục giây; resume ấm gần như tức thì.
 */
const RESUME_FRESH_MS = 10 * 60 * 1000;
/** Đọc lại criteria của một collection tối đa một lần mỗi quãng này. */
const COLLECTION_SEED_TTL_MS = 5 * 60 * 1000;
/**
 * TEMPLATE TĨNH THIẾU THÌ DỰNG LẠI, KHÔNG XIN LƯỢT GHI
 *
 *   Đo trên #650: prewarm hỏng một lần (cấu hình collection / chuẩn token
 *   không đọc được) → không có template → mỗi lượt: xếp hàng, ăn một giấy phép
 *   một lượt quota, dựng hỏng "chưa hydrate template", FAILED, watchdog thấy hàng mồ
 *   côi → xếp lại → vô hạn, nhiều phút, và mỗi vòng lấy mất một nhịp của cả
 *   máy. Template là phụ thuộc TĨNH: dựng lại ở nền theo bậc này, và hàng chỉ
 *   vào hàng chờ ghi khi template đã sẵn.
 */
const TEMPLATE_RETRY_MS = [2000, 5000, 15000, 30000, 60000];

/**
 * Câu lỗi OpenSea trả về, dù nó nằm ở đâu trong thân phản hồi.
 *
 * Hình dạng đã gặp thật: {detail}, {error}, {errors:[…]}, hoặc chuỗi thuần.
 * Đọc hụt thì người dùng nhận một mã số thay vì một câu họ hành động được.
 */
function readErrorText(body) {
  if (!body) return "";
  if (typeof body === "string") return body.slice(0, 160);
  const first = body.detail || body.error ||
    (Array.isArray(body.errors) ? body.errors[0] : "");
  if (typeof first === "string") return first.slice(0, 160);
  return JSON.stringify(body).slice(0, 160);
}

let traceSeq = 0;

class OfferItemEngineV2 {
  /**
   * @param {object} o
   * @param {object} o.adapter        bộ chuyển đổi theo chain (xem chain-adapters)
   * @param {object} o.quota          QuotaBroker dùng chung cả máy
   * @param {() => string} o.getApiKey
   * @param {(line:string)=>void} [o.onLog]
   * @param {(payload:object)=>void} [o.onUpdate]
   */
  constructor({ adapter, quota, getApiKey, getApiKeys, onLog, onUpdate, traitSource } = {}) {
    if (!adapter) throw new Error("EngineV2 cần một chain adapter.");
    this.adapter = adapter;
    this.chain = adapter.chain;
    this.quota = quota || null;
    this.getApiKey = typeof getApiKey === "function" ? getApiKey : () => "";
    /**
     * MỌI key ghi được — broker chọn key lúc cấp (xem submitOne). Không có
     * nguồn danh sách thì rơi về một key như trước.
     */
    this.getApiKeys = typeof getApiKeys === "function"
      ? getApiKeys
      : () => { const k = this.getApiKey(); return k ? [k] : []; };
    /** Lần cuối một POST ghi nhận 429 — tín hiệu backpressure cho recovery. */
    this.last429At = 0;
    this.onLog = typeof onLog === "function" ? onLog : () => {};
    this.onUpdate = typeof onUpdate === "function" ? onUpdate : () => {};
    /**
     * "CÓ GÌ ĐÓ VỪA ĐỔI" — tín hiệu rẻ cho giao diện, KHÔNG phải trạng thái.
     *
     *   `onUpdate` đẩy cả bảng và chỉ được gọi ở Start/Stop/Reset. Trước đây
     *   không có gì gọi nó khi sổ đổi vì một sự kiện Stream, khi một offer vừa
     *   gửi xong, hay khi REST vừa đọc xong — nên bảng NFT đứng ở trạng thái
     *   lúc bấm Start cho tới lần bấm kế tiếp. Đo trên app thật: Collection
     *   Offer đã vào sổ, engine đã quyết định đúng, và cột Best Offer vẫn hiện
     *   con số lúc thêm NFT.
     *
     *   Hook này chỉ nói "đổi rồi"; cầu nối gộp nhiều tín hiệu trong một
     *   khung ngắn thành MỘT lần đẩy. Một collection offer chạm 1000 NFT sinh
     *   1000 tín hiệu và đúng một lần vẽ.
     */
    this.onChange = () => {};

    this.book = new MemoryBook();
    this.intents = new IntentStore();
    this.metrics = new Metrics();
    this.recovery = new RecoveryPlane({
      onLog: line => this.onLog(`${this.chain}: ${line}`),
      // Recovery P2/P3 nhường realtime: có SEND đang chờ/bay, hoặc vừa 429.
      pressure: () => this.realtimePressure()
    });
    this.http = new submitterModule.HttpPool({ maxSockets: 8 });

    /** Đặt lúc Start, khi đã có Private Key. */
    this.builder = null;

    /** tokenKey -> cấu hình hàng (min/max/step/duration). */
    this.rows = new Map();
    /**
     * NFTs added during this run get one P1 scheduling boost once their first
     * valid SEND is ready. The marker is consumed only by a successful POST.
     */
    this.firstPostPending = new Set();
    this.firstPostCompleted = new Set();

    this.state = STATE.IDLE;

    /**
     * Đệm sự kiện trong lúc hydrate. Xem chú thích đầu file: đây là thứ chặn
     * khoảng mù giữa "đã subscribe" và "đã biết trạng thái hiện tại".
     */
    this.buffer = [];
    this.buffering = false;
    /** Trần đệm: một collection sôi động sinh hàng nghìn sự kiện mỗi giây. */
    this.bufferCap = 20000;

    /** Tăng mỗi lần Reset/Stop. Chặn mọi thứ của lượt cũ ghi tiếp. */
    this.epoch = 0;

    /** Cổng licence, gắn từ ngoài. Giữ nguyên ngữ nghĩa của v1.19.4. */
    this.licenceGate = null;

    /**
     * Những token đang được ĐỌC LẦN ĐẦU qua REST.
     *
     * Giao diện cần biết chuyện này. Trong khoảng giữa "đã bấm Start" và "đã
     * biết ai đang dẫn đầu", hàng đó THẬT SỰ đang lấy Best Offer, và nói đúng
     * điều đó khác hẳn với việc để hàng hiện "Đã dừng" trong lúc nó đang chạy.
     */
    this.hydrating = new Set();

    /**
     * Mỗi khoá trong `hydrating` bắt đầu từ lúc nào.
     *
     * VÌ SAO MỘT `Set` LÀ KHÔNG ĐỦ
     *
     *   `hydrating` là dấu "đang có lượt đọc cho hàng này", và MỌI lưới an
     *   toàn phía trên đều coi nó là bằng chứng có việc thật: watchdog cổng bỏ
     *   qua hàng đang `hydrating`, watchdog nợ đọc cũng vậy. Nhưng một cái Set
     *   không phân biệt được "đang đọc, hơi chậm" với "dấu bị bỏ quên từ mười
     *   phút trước".
     *
     *   Nên một dấu bị rò không chỉ làm hàng kẹt ở "Đang lấy Best Offer" —
     *   nó làm hàng kẹt một cách VÔ HÌNH với chính những lưới sinh ra để cứu.
     *   Có mốc thời gian thì "bận mà không có việc" mới trở thành câu hỏi trả
     *   lời được.
     */
    this.hydratingSince = new Map();

    /**
     * NHỮNG TOKEN CHƯA CÓ LƯỢT ĐỌC ĐẦU TIÊN — KHÔNG GỬI KHI CHƯA BIẾT BEST
     *
     *   Lúc Start, Stream đã nghe nhưng sổ mới chỉ biết những gì vừa xảy ra.
     *   Một bid nhỏ tới trước lượt REST ban đầu làm engine tính "Best 0.01 →
     *   gửi 0.0101", rồi REST về nói Best thật là 0.149 và engine gửi tiếp
     *   0.150: hai order, cái đầu vô nghĩa. Với sổ trống thì còn tệ hơn: gửi
     *   Min trước khi biết có ai.
     *
     *   Nên trước lượt đọc đầu tiên của token, quyết định KHÔNG tạo ý định
     *   gửi (giao diện vẫn thấy sổ đổi). Lượt đọc về — kể cả "không ai đặt"
     *   — mới là lúc gửi target đầu tiên đúng. Chỉ áp cho lượt đọc lúc Start;
     *   đọc lại khi reconnect/suy giảm không chặn đường nóng.
     */
    this.awaitingFirstRead = new Set();
    /** WARM token whose cancelled top left an uncertain lower market. */
    this.downwardAuthority = new Map();
    /** tokenKey -> join/gap time requiring a full read after subscription ACK. */
    this.topicRepairAt = new Map();

    /** tokenKey -> lần cuối đi hỏi phạm vi của một trait offer. */
    this.traitProbedAt = new Map();

    /**
     * NHỊP DỌN LỆNH HẾT HẠN — đồng hồ local, không cần mạng.
     *
     *   `effectiveBest` đã bỏ lệnh quá `endTime` lúc ĐỌC, nhưng không ai đọc
     *   nếu không có sự kiện: một collection offer 0.014 hết hạn lúc 3 giờ
     *   sáng không sinh sự kiện nào, và hàng bị "vượt giá" bởi nó cứ đứng im
     *   cho tới khi có ai đó đặt giá mới. Nhịp này chỉ quét sổ trong bộ nhớ
     *   (rẻ), và chỉ tính lại những sổ THẬT SỰ mất lệnh. Không phải nhịp
     *   quét REST, và không nằm trên đường nóng.
     */
    this.expiryTimer = null;
    /** Nhịp thử lại sau lỗi tạm thời và watchdog bộ điều phối. */
    this.retryTimer = null;
    this.watchdogTimer = null;
    /** tokenKey -> lúc bắt đầu bay; cho watchdog in-flight. */
    this.inFlightSince = new Map();
    this.lastDispatchAt = 0;
    /**
     * MỖI LƯỢT BAY CÓ MỘT SỐ HIỆU — khoá single-flight chỉ được nhả bởi ĐÚNG
     * lượt đã lấy nó. Watchdog bỏ một lượt kẹt bằng cách đổi số hiệu (và huỷ
     * yêu cầu ở broker); lượt cũ tỉnh dậy thấy số hiệu khác thì rút lui mà
     * không đụng khoá của lượt mới. tokenKey -> { id, since, stage, handle }.
     */
    this.flights = new Map();
    this.flightSeq = 0;
    /** Tiến triển THẬT của bộ điều phối: giấy phép nhận được / POST xong. */
    /** Số lượt xin giấy phép của engine này đang nằm ở broker (≤ MAX_ACQUIRING). */
    this.acquiring = 0;
    /** tokenKey -> lần cuối NFT đó được cấp một lượt gửi; cho vòng tròn công bằng. */
    this.servedAt = new Map();
    this.lastGrantAt = 0;
    this.lastPostAt = 0;
    this.lastRepairAt = 0;
    this.runningSince = 0;
    /**
     * LƯỢT ĐỌC CÒN NỢ — key -> { reason, authoritative, attempt, timer }.
     *
     *   Trước đây chỉ lượt đọc ĐẦU mới được đọc lại khi hỏng; đọc lại sau
     *   reconnect / gap / kênh im lặng là bắn-rồi-quên: 429 hay bị
     *   `invalidate` (reconnect kế tiếp) là mất, và một Item Offer đặt trong
     *   khoảng mù không bao giờ được biết. Sổ nợ này giữ cho tới khi đọc
     *   THÀNH CÔNG; watchdog xếp lại những lượt bị rơi.
     */
    this.pendingReads = new Map();
    /** One bounded, coalescing Path B candidate queue, keyed by tracked NFT. */
    this.recoveryCandidates = new Map();
    /** Per-collection candidate FIFOs, drained round-robin for cross-topic fairness. */
    this.recoveryCandidateQueues = new Map();
    this.recoveryCandidateSlugOrder = [];
    this.recoveryCandidateTimer = null;
    this.recoverySweepActive = false;
    this.lastFullRecoveryAt = 0;
    /** slug -> lần cuối đọc nhất quán cho collection đó. */
    this.slugReadAt = new Map();
    /** slug -> lần cuối đọc criteria (seed) và slug đang đọc dở. */
    this.slugSeedAt = new Map();
    this.slugSeedInFlight = new Set();
    /** Số liệu đọc: để đo "resume ấm tốn bao nhiêu REST". */
    this.readStats = { requested: 0, deduped: 0, full: 0, quick: 0, collection: 0, collectionDeduped: 0,
      resumeReads: 0, seeded: 0, orphanFirstReads: 0 };
    /** Token đang xác nhận lại Best sau khi offer của mình hết hạn (cổng đóng). */
    this.renewCheck = new Set();
    /**
     * tokenKey -> mốc cổng lượt-đọc-đầu/renew đóng lần cuối (fencing, port từ
     * 1.19.61). Chỉ một lượt đọc XẾP SAU mốc này mới được mở cổng; lượt đọc
     * cũ về muộn chỉ bổ sung sổ rồi tự xếp lượt kế tiếp.
     */
    this.gateClosedAt = new Map();
    /**
     * Khoá → lúc hàng bắt đầu mang nhãn "Đang cập nhật Best", và đã kêu chưa.
     *
     * Chỉ để CHẨN ĐOÁN. Một lượt xác nhận lại có thể kéo dài hợp lệ (429, đọc
     * đầy đủ 4 trang, mạng chậm) nên nó không phải cái đồng hồ đếm ngược để
     * bỏ cuộc — bỏ cuộc ở đây nghĩa là đặt lại giá dựa trên một cuốn sổ chưa
     * được xác nhận, đúng cái lỗi #44. Nó chỉ nói ra MỘT LẦN, kèm đủ thứ để
     * biết vì sao, thay vì để người dùng nhìn một spinner không lời.
     */
    this.renewSince = new Map();
    /** Hàng đã có sẵn danh sách đầy đủ từ đường Add, tính trong lần Start này. */
    this.seededRows = new Set();
    /**
     * TRẠNG THÁI TEMPLATE THEO TOKEN — key -> { state, attempts, retryAt, timer, lastError, promise }
     *   ready      builder có template (kiểm bằng builder.get, không tin cờ)
     *   hydrating  một lượt dựng đang bay (single-flight)
     *   retry      dựng hỏng, đã hẹn dựng lại theo TEMPLATE_RETRY_MS
     */
    this.templateState = new Map();
    /** Cấu hình chain + counter ví đọc ở prewarm, dùng lại khi dựng template lẻ. */
    this.chainCfg = null;
    this.accountCfg = null;
    /** Lần cuối Stream có khoảng mù (reconnect / suy giảm): sổ trước mốc này không đủ để đặt lại. */
    this.lastStreamGapAt = 0;
    // Own-order authority is wallet-wide.  It must not be inferred from
    // token market hydration (`lastFullReadAt`), otherwise a startup quick
    // read forces a multi-page REST read directly in front of every POST.
    this.ownSyncAt = 0;
    this.ownUnknownAt = 0;
    this.ownSyncPending = false;
    this.ownSyncRetryTimer = null;
    /** tokenKey -> timer hẹn đọc lại lượt đầu; để reset dọn và watchdog biết. */
    this.firstReadTimers = new Map();
    /** tokenKey -> lần cuối đọc lại vì top bị huỷ (gap recovery). */
    this.gapReadAt = new Map();

    /**
     * BẢN CHỤP TRAIT CỦA TỪNG TOKEN — nạp lúc Start, ở nền, theo lô.
     *
     *   Có nó thì một trait offer được quyết định hoàn toàn cục bộ: MATCH /
     *   NO_MATCH mà không đụng mạng. Không có nó (nguồn tắt, hoặc lô nạp
     *   hỏng) thì token ở trạng thái CHƯA BIẾT và đi đường recovery có trần.
     *   Nguồn được TIÊM để bài kiểm không chạm mạng; sản phẩm đưa
     *   `nft-source` (metadata OpenSea) vào qua cầu nối.
     */
    this.traits = new TraitSnapshot({
      getNFTs: traitSource && traitSource.getNFTs,
      readAttributes: traitSource && traitSource.readAttributes,
      onLog: line => this.log(line)
    });

    /**
     * Số dư WETH đã biết: { wethWei, known }.
     *
     * `known:false` nghĩa là ĐỌC HỎNG, không phải "ví rỗng" — và engine phải
     * cư xử khác hẳn giữa hai điều đó.
     */
    this.balance = null;
    /** Lần cuối đi đọc lại số dư vì một hàng đang kẹt LOW_BALANCE. */
    this.balanceProbedAt = 0;

    /**
     * Sức khoẻ Stream, hỏi từ ngoài. Không có thì coi như không biết — và
     * "không biết" KHÔNG kích chế độ suy giảm.
     */
    this.streamHealth = null;
    this.healthTimer = null;
    /** Lần cuối đọc lại toàn bộ vì Stream hỏng. */
    this.degradedRehydratedAt = 0;
    /** Đang ở chế độ suy giảm không — để log đúng một lần mỗi lượt vào/ra. */
    this.degraded = false;
    /** tokenKey -> lý do: đánh thức hàng đã hẹn (tối đa MỘT mỗi hàng, gộp trong microtask). */
    this.pendingWakes = new Map();
    this.wakeScheduled = false;
    /** Hẹn giờ hết hạn CHÍNH XÁC (order sớm nhất trong mọi sổ) — xem noteExpiry. */
    this.expiryWakeTimer = null;
    this.expiryWakeAt = 0;
    /** Lượt resync own toàn ví nền: backoff khi hỏng/hết hạn (P3, không phải cổng gửi). */
    this.ownSyncBackoffMs = 0;
    /** tokenKey -> lần cuối log ORPHAN (giới hạn log, không giới hạn chữa). */
    this.orphanLog = new Map();
    /** Stream không khoẻ liên tục từ lúc này (0 = đang khoẻ). */
    this.unhealthySince = 0;
    /** Bộ đếm mạng gộp — xem netStat/netDiagnostics. */
    this.net = null;

    this.stats = { events: 0, applied: 0, decided: 0, submitted: 0, dropped: 0,
      traitProbes: 0, traitProbeSkipped: 0 };
    this.pumping = false;

    /**
     * NHỊP SỐNG — để giao diện không giả vờ engine còn chạy.
     *
     *   `lastStreamEventAt`   sự kiện Stream cuối cùng chạm engine này
     *   `lastActivityAt`      lần cuối engine thật sự làm gì (áp sổ, quyết
     *                          định, gửi, đọc lại, dọn hết hạn)
     *   Renderer chỉ vẽ "đang sống" khi những mốc này còn tươi; không có nó,
     *   một hoạt ảnh CSS chạy mãi sẽ che một tiến trình đã chết.
     */
    this.lastStreamEventAt = 0;
    this.lastActivityAt = 0;
  }

  /** Ghi nhận engine vừa làm việc. Rẻ; gọi ở mọi đường có thật. */
  touch() { this.lastActivityAt = Date.now(); }

  log(line) { this.onLog(`[V2 ${this.chain.toUpperCase()}] ${line}`); }

  attachLicenceGate(gate) { this.licenceGate = gate; }

  /** Cho engine biết hỏi sức khoẻ Stream ở đâu. Gắn từ cầu nối. */
  attachStreamHealth(fn) {
    this.streamHealth = typeof fn === "function" ? fn : null;
  }

  /** Trace label: a Stream event, or REST while the Stream is degraded. */
  triggerSource(event) {
    if (/^s\d+-\d+$/.test(String(event?.correlationId || ""))) return "STREAM";
    return this.degraded ? "DEGRADED_REST" : "REST";
  }

  /** Only the ACK for this row's topic establishes live Stream coverage. */
  topicReady(row, book = null) {
    if (!row) return false;
    const repairAt = this.topicRepairAt.get(row.key) || 0;
    // A complete authority read started after this topic's outage is enough
    // to safely use the local book while that one subscription rejoins.
    if (repairAt && Number(book?.lastFullReadAt) > repairAt) return true;
    if (this.streamHealth) {
      let health = "UNKNOWN";
      try { health = String(this.streamHealth(row.collectionSlug)); } catch { health = "FAILED"; }
      if (health !== "HEALTHY" && health !== "UNKNOWN") return false;
    }
    if (!repairAt || Number(book?.lastFullReadAt) > repairAt) return true;
    // A fresh event after the ACK is P0. Let MemoryBook counter it while the
    // scoped REST repair continues in spare capacity; an old intent remains
    // blocked until a new event or a complete authority read arrives.
    return Number(book?.lastEventAt) > repairAt &&
      Number(book?.effectiveBest?.(Date.now())?.price) > 0;
  }

  /**
   * STREAM CHẾT THÌ KHÔNG ĐƯỢC MÙ
   *
   * VÌ SAO CÓ MỘT NHỊP Ở ĐÂY, TRONG MỘT KIẾN TRÚC KHÔNG NHỊP
   *
   *   V2 không quét định kỳ: giá đổi thì Stream báo, và đó là toàn bộ lý do
   *   nó nhanh. Nhưng lập luận đó chỉ đứng khi Stream ĐANG CHẠY. Khi socket
   *   chết — mạng, OpenSea, hay key bị từ chối — một engine chỉ-nghe-Stream
   *   không nhận được gì và không đi hỏi gì: nó đứng im trong khi thị trường
   *   chạy, và trông y hệt một thị trường yên tĩnh.
   *
   *   Kiến trúc cũ không có lỗ này vì nó quét mọi lúc. Trả lại nhịp quét mọi
   *   lúc là trả lại cái giá đã đo được (8 giây p90 chờ REST trên đường
   *   phản ứng). Nên nhịp này có đúng một điều kiện: Stream KHÔNG khoẻ. Nó
   *   chạy trên mặt phẳng nền với bộ giới hạn riêng, không chạm đường nóng,
   *   và tắt ngay khi Stream trở lại.
   *
   *   Đo được trên bản đóng gói không có Stream: thế giới đổi giá, engine
   *   không hề biết trong 30 giây — 0/3 hàng được đọc lại.
   */
  watchStreamHealth() {
    if (this.healthTimer) return;
    const tick = () => {
      if (this.state !== STATE.RUNNING) return;
      let health = "UNKNOWN";
      try { health = this.streamHealth ? String(this.streamHealth()) : "UNKNOWN"; }
      catch { health = "UNKNOWN"; }

      /**
       * SUY GIẢM CHỈ KHI STREAM HỎNG THẬT
       *
       *   Health của Stream từ 1.25.0 chỉ dựa trên bằng chứng giao thức
       *   (socket, heartbeat ACK, join) — thị trường im lặng không bao giờ
       *   là STALE. Và phải hỏng LIÊN TỤC DEGRADED_GRACE_MS: một cú nối lại
       *   1 giây đã có đường đọc lại riêng (onStreamReconnect), không cần bật
       *   thêm vòng REST toàn bộ mỗi 45 giây.
       */
      const unhealthy = health === "FAILED" || health === "DISCONNECTED" ||
        health === "RECONNECTING" || health === "STALE" || health === "DEGRADED";
      const now = Date.now();
      if (!unhealthy) this.unhealthySince = 0;
      else if (!this.unhealthySince) this.unhealthySince = now;
      const confirmed = unhealthy && now - this.unhealthySince >= OfferItemEngineV2.DEGRADED_GRACE_MS;

      if (confirmed && !this.degraded) {
        this.degraded = true;
        this.lastStreamGapAt = this.unhealthySince;
        this.stats.degradedEntries = (this.stats.degradedEntries || 0) + 1;
        this.log(`STREAM ${health} kéo dài — Path B recovery có giới hạn; Path A vẫn mở`);
      } else if (!unhealthy && this.degraded) {
        this.degraded = false;
        this.log(`STREAM ${health} — dừng đọc lại qua REST`);
      }

      if (this.degraded &&
          Date.now() - this.degradedRehydratedAt >= DEGRADED_REHYDRATE_MS) {
        this.degradedRehydratedAt = Date.now();
        // B-only round robin. Fresh Stream events keep flowing while these
        // bounded full reads repair missed state at the Path B rate.
        this.scheduleRecoverySweep("stream-degraded", { authoritative: true });
      }
    };
    this.healthTimer = setInterval(tick, STREAM_HEALTH_POLL_MS);
    if (this.healthTimer.unref) this.healthTimer.unref();
  }

  stopWatchingStreamHealth() {
    if (this.healthTimer) clearInterval(this.healthTimer);
    this.healthTimer = null;
    this.degraded = false;
    this.unhealthySince = 0;
  }

  /**
   * Realtime đang cần mạng/hạn mức? Tín hiệu backpressure chung cho recovery
   * P2/P3 và metadata: có SEND sẵn sàng/đang xin lượt/đang bay, hoặc một POST
   * vừa ăn 429. Thuần cục bộ, rẻ.
   */
  realtimePressure(now = Date.now()) {
    if (this.state !== STATE.RUNNING) return false;
    if (this.acquiring > 0 || this.intents.inFlight.size > 0) return true;
    if (now - (this.last429At || 0) < 10000) return true;
    return this.intents.ready(now).length > 0;
  }

  // ================================================================
  // Vòng đời
  // ================================================================

  /**
   * Bắt đầu chạy.
   *
   * @param {object} o
   * @param {string} o.privateKey
   * @param {Array<object>} o.rows  [{ url, contract, tokenId, collectionSlug,
   *                                   traits, minPrice, maxPrice, step, duration }]
   */
  async start({ privateKey, rows = [] }) {
    if (!privateKey) return { ok: false, error: "Thiếu Private Key." };

    this.epoch++;
    this.state = STATE.HYDRATING;
    this.builder = new LocalOrderBuilder({ privateKey });
    this.book.setSelfAddress(this.builder.address);

    // ---- 1. ĐỆM TRƯỚC ----------------------------------------------
    //
    // Bật đệm TRƯỚC khi làm bất cứ việc gì mất thời gian. Từ giây này trở đi
    // không sự kiện nào bị mất, kể cả khi hydrate mất ba mươi giây.
    this.buffering = true;
    this.buffer.length = 0;

    // ---- 2. nạp danh sách hàng --------------------------------------
    this.rows.clear();
    const pendingFirstPost = new Set(this.firstPostPending);
    this.firstPostPending.clear();
    for (const row of rows) {
      const key = tokenKey(this.chain, row.contract, row.tokenId);
      this.registerRow(row);
      // Cold-start rows, like live-added rows, get one priority turn once
      // their own readiness gate opens. A prior pending turn survives Stop.
      if (pendingFirstPost.has(key) || !this.firstPostCompleted.has(key)) this.firstPostPending.add(key);
    }
    // Vòng tròn phục vụ chỉ nói về hàng ĐANG có. Hàng đã bỏ mà còn mốc thì
    // mốc đó không ảnh hưởng thứ tự (không ai xếp hàng cho nó) nhưng vẫn là
    // rác tích lại qua mỗi lần Start — dọn ở đúng chỗ danh sách hàng đổi.
    for (const key of this.servedAt.keys()) if (!this.rows.has(key)) this.servedAt.delete(key);

    /**
     * CỔNG LƯỢT-ĐỌC-ĐẦU ĐÓNG TỪ TRƯỚC KHI CÓ BẤT KỲ SỰ KIỆN NÀO ĐƯỢC TÍNH
     *
     *   Bản 1.19.6 chỉ đóng cổng trong `hydrateBook("startup")` — được gọi
     *   SAU khi xả đệm. Sự kiện đệm trong lúc prewarm được phát lại vào một
     *   cổng còn mở: một bid nhỏ → gửi ngay, rồi lượt đọc mới về nói Best
     *   thật. Đóng cổng ở đây, trước prewarm, trước xả đệm.
     */
    // Hàng nào đã mang snapshot đầy đủ từ đường Add thì không chờ lượt đọc đầu.
    this.awaitingFirstRead = new Set([...this.rows.keys()].filter(key => {
      const book = this.book.get(key);
      return !(book && book.lastFullReadAt > 0);
    }));

    const warm = await this.prewarm();
    // A wallet switch landed while this start was warming up: the newer start
    // owns the engine now; this one must not go RUNNING on the old wallet.
    if (warm && warm.superseded) return { ok: false, superseded: true };

    // ---- 4. phát lại đệm --------------------------------------------
    //
    // RUNNING phải được đặt TRƯỚC khi xả đệm. `pump()` thoát ngay khi engine
    // chưa RUNNING, nên xả đệm trước sẽ tạo ý định rồi bỏ đó — sự kiện được
    // giữ đúng, tính đúng, và không bao giờ được gửi. Đo bằng test: sự kiện
    // tới giữa lúc hydrate vào đệm, đệm rỗng sau đó, và không có offer nào.
    this.state = STATE.RUNNING;
    this.runningSince = Date.now();
    this.lastGrantAt = 0; this.lastPostAt = 0; this.lastRepairAt = 0;
    this.watchStreamHealth();
    this.startExpirySweep();
    this.startLoops();
    const replayed = this.flushBuffer();

    // Trait của từng token: nạp ở NỀN, theo lô 100, không chặn Start và không
    // chặn đường nóng. Xong lô nào thì sổ biết lô đó; trait offer tới trước
    // khi xong vẫn có đường recovery cho token chưa biết.
    this.hydrateTraits("startup");

    // Own resync lúc prewarm trả về từng phần (cursor ví lặp): phần còn lại
    // tiếp tục ở NỀN, chỉ cho những hàng chưa soát. Hàng đã soát gửi ngay.
    if (this.prewarmOwnIncomplete) {
      this.prewarmOwnIncomplete = false;
      this.queueOwnResync("prewarm");
    }

    // ---- 5. đọc trạng thái hiện tại, ở NỀN --------------------------
    //
    // Sau khi đã nghe Stream và đã phát lại đệm. Thứ tự đó quan trọng: đọc
    // trước rồi mới nghe sẽ tạo một khoảng mù, còn nghe trước rồi đọc thì
    // không mất gì — và kết quả đọc chỉ bù vào chỗ Stream chưa nói.
    const hydrating = this.hydrateBook("startup");
    if (!hydrating) {
      // Không có nguồn đọc (adapter không có fetchBest): không có gì để đợi.
      this.awaitingFirstRead.clear();
      for (const key of this.rows.keys()) this.evaluate(key, Date.now(), null);
      this.pump();
    }
    this.log(`START · ${this.rows.size} NFT · prewarm ${warm.ms}ms ` +
      `(template ${warm.templates}/${this.rows.size}) · phát lại ${replayed} sự kiện đã đệm · đọc nền ${hydrating} NFT`);
    this.emit();
    return { ok: true, rows: this.rows.size, replayed, prewarm: warm };
  }

  /**
   * Đăng ký MỘT hàng vào engine: bảng hàng + sổ. Cùng một đường cho Start
   * (mọi hàng) và cho Add lúc đang chạy (`addRows`), để hai đường không lệch.
   */
  registerRow(row) {
    const key = tokenKey(this.chain, row.contract, row.tokenId);
    this.rows.set(key, {
      key, url: row.url,
      contract: String(row.contract).toLowerCase(),
      tokenId: String(row.tokenId),
      collectionSlug: String(row.collectionSlug || "").toLowerCase(),
      minPrice: Number(row.minPrice),
      // Max về lưới giá, làm tròn XUỐNG — không tự nâng trần của người dùng.
      maxPrice: normalizeMaxDown(row.maxPrice),
      step: Number(row.step),
      duration: Number(row.duration) || 15,
      running: true,
      /**
       * ƯU TIÊN LÀ THỨ TỰ HÀNG ĐỢI, KHÔNG PHẢI TỐC ĐỘ
       *
       *   Giữ nguyên ngữ nghĩa v1.19.4: đánh dấu một NFT không làm nó được
       *   gửi nhanh hơn và cũng không cho nó một hạn mức riêng — nó chỉ
       *   được xét TRƯỚC khi có nhiều hàng cùng sẵn sàng và chỗ thì có hạn.
       *
       *   Khác biệt chỉ lộ ra đúng lúc nó có ý nghĩa: hạn mức ghi chật, sáu
       *   suất đang bay, mười hàng chờ. Không có ưu tiên thì thứ tự là ngẫu
       *   nhiên theo thứ tự Map.
       */
      priorityMode: Boolean(row.priorityMode),
      /** One dispatch boost for an explicit user Start/Resume. */
      manualPriorityAt: 0,
      /** OpenSea vừa từ chối vì ví không đủ WETH. */
      lowBalance: false,
      lastError: "",
      /** Đã ghi log "licence từ chối" với lý do này chưa (không spam). */
      licenceRefusedLogged: "",
      /** Lỗi tạm thời liên tiếp và mốc thử lại (xem scheduleRetry). */
      failures: 0,
      retryAt: 0
    });
    /**
     * DANH SÁCH RỖNG TỪ CẤU HÌNH KHÔNG PHẢI LÀ "KHÔNG CÓ TRAIT"
     *
     *   `main.js` truyền `traits: token.traits || []` — và `db.js` không lưu
     *   trait cho hàng Offer Item, nên nó LUÔN là []. Nếu [] được đưa vào sổ
     *   như một câu trả lời ("token này không có trait nào"), mọi trait
     *   offer đều NO_MATCH, và điểm mù cũ quay lại qua một cửa khác — im
     *   lặng hơn lần trước, vì lần này nó trông như một câu trả lời đúng.
     *
     *   Chỉ NGUỒN THẬT (bản chụp trait, đọc từ metadata) mới được nói
     *   "rỗng thật". Cấu hình có danh sách thì dùng; rỗng thì là CHƯA BIẾT.
     */
    const configuredTraits = Array.isArray(row.traits) && row.traits.length
      ? row.traits : undefined;
    this.book.add({
      key, chain: this.chain,
      contract: row.contract, tokenId: row.tokenId,
      collectionSlug: row.collectionSlug, traits: configuredTraits
    });
    /**
     * LƯỢT ĐỌC CỦA ĐƯỜNG THÊM NFT ĐƯỢC DÙNG LẠI, KHÔNG ĐỌC HAI LẦN
     *
     *   `bot:addNfts` đã đọc CẢ DANH SÁCH offer của token (đó là dòng
     *   `[BEST] #306 … Best 0.0201` trong log) rồi mới đăng ký hàng. Bản
     *   trước vứt kết quả đó đi: hàng vào sổ rỗng, cổng lượt-đọc-đầu đóng, và
     *   engine đi đọc lại đúng thứ vừa đọc — một lượt REST thừa cho mỗi NFT
     *   thêm vào, và một cửa sổ để cổng kẹt lại nếu lượt đọc thứ hai rơi.
     *
     *   Nay snapshot đi thẳng vào sổ. Đủ thẩm quyền (danh sách đầy đủ, không
     *   cắt trang) thì hàng KHÔNG cần lượt đọc đầu nào nữa.
     */
    if (row.seed && Array.isArray(row.seed.orders)) this.seedBook(key, row.seed);
    return key;
  }

  /**
   * Đưa một snapshot đã đọc (đường thêm NFT) vào sổ.
   *
   * @param {string} key
   * @param {{orders:Array, readAt:number, complete:boolean}} seed
   * @returns {boolean} snapshot có đủ thẩm quyền để bỏ qua lượt đọc đầu không
   */
  seedBook(key, seed) {
    const book = this.book.get(key);
    if (!book) return false;
    const now = Date.now();
    const sec = Math.floor(now / 1000);
    const self = this.builder ? this.builder.address.toLowerCase() : "";
    let merged = 0;
    for (const order of seed.orders || []) {
      if (!order || !order.orderHash || !(Number(order.price) > 0)) continue;
      if (Number(order.endTime) > 0 && Number(order.endTime) <= sec) continue;
      if (book.isTombstoned(order.orderHash)) continue;
      const mine = self && String(order.maker || "").toLowerCase() === self;
      if (mine) { this.upsertOwn(book, this.rows.get(key), order, now); merged++; continue; }
      const group = order.kind === "collection" ? book.collection
        : order.kind === "trait" ? book.trait : book.item;
      if (group.has(order.orderHash)) continue;
      group.set(order.orderHash, {
        orderHash: order.orderHash, price: Number(order.price),
        maker: String(order.maker || "").toLowerCase(), kind: order.kind,
        quantity: Number(order.quantity) || 1, currency: "WETH",
        endTime: Number(order.endTime) || 0, seq: 0, at: now
      });
      book.capGroup(group);
      merged++;
    }
    book.generation++;
    if (seed.complete) {
      // Danh sách đầy đủ và không cắt trang: đây CHÍNH là lượt đọc đầu.
      book.hydratedAt = now;
      book.lastFullReadAt = Number(seed.readAt) > 0 ? Number(seed.readAt) : now;
      // Danh sách đầy đủ của token cũng là một lượt soát own cho RIÊNG hàng này
      // (giống mergeBest): hàng vừa Add không phải chờ resync toàn ví.
      book.ownReconciledAt = Math.max(book.ownReconciledAt || 0, book.lastFullReadAt);
      this.readStats.seeded++;
      // Dấu này chỉ sống trong lần Start hiện tại: nó trả lời đúng một câu
      // hỏi — "hàng này vừa được đường Add đưa cho một danh sách đầy đủ, nên
      // đừng đọc lại đúng thứ vừa đọc". Mọi lượt hydrate SAU đó là một câu
      // hỏi khác và phải được đi.
      this.seededRows.add(key);
      return true;
    }
    return false;
  }

  /**
   * BẤT BIẾN CỦA CỔNG LƯỢT-ĐỌC-ĐẦU
   *
   *   `awaitingFirstRead` có mặt ⇒ phải có ĐÚNG MỘT trong: đang đọc
   *   (`hydrating`), có nợ đọc kèm hẹn giờ (`pendingReads.timer`), hoặc có
   *   hẹn đọc lại (`firstReadTimers`). Không có cái nào là hàng chờ mãi một
   *   lượt đọc không tồn tại — đo được trên #306: 39 lần watchdog "xếp lại"
   *   trong vài phút và không có lượt gửi nào.
   *
   *   Ở đây không chỉ đếm: nếu sổ ĐÃ có thẩm quyền (đã đọc đầy đủ) thì mở
   *   cổng luôn — không đi đọc thêm lần nữa; nếu chưa thì xếp một lượt đọc.
   *
   * @returns {number} số hàng mồ côi đã chữa
   */
  enforceFirstReadInvariant(now = Date.now()) {
    let orphans = 0;
    /**
     * `renewCheck` ĐI CÙNG BẤT BIẾN NÀY, KHÔNG ĐỨNG NGOÀI
     *
     *   Nhãn "Đang cập nhật Best" trên màn hình do `renewCheck` quyết định,
     *   và nó chỉ được xoá ở một chỗ duy nhất: một lượt đọc ĐẦY ĐỦ ghép được
     *   vào sổ. Mọi đường khác mà lượt đọc ấy có thể biến mất — kết quả bị
     *   bỏ vì đã cũ, việc bị huỷ, hàng bị dừng giữa chừng — đều để lại một
     *   hàng mang nhãn "đang cập nhật" mà không có gì đang cập nhật.
     *
     *   Nên duyệt cả hai tập. Hàng nào trong `renewCheck` mà không còn nằm
     *   trong `awaitingFirstRead` vẫn phải có một lượt đọc thật, nếu không
     *   nhãn đó là một lời nói dối đứng yên vĩnh viễn.
     */
    const watched = new Set([...this.awaitingFirstRead, ...this.renewCheck]);
    for (const key of watched) {
      const row = this.rows.get(key);
      // Hàng đã bỏ, hoặc đã dừng: không còn gì để chờ. Giữ nhãn "đang cập
      // nhật" cho một hàng không chạy là dựng cảnh, và nó còn nuôi những lượt
      // đọc lại không ai cần.
      if (!row || !row.running) {
        this.awaitingFirstRead.delete(key);
        this.renewCheck.delete(key);
        this.renewSince.delete(key);
        continue;
      }
      const pending = this.pendingReads.get(key);
      /**
       * `hydratingIsReal`, KHÔNG phải `hydrating.has`.
       *
       *   Một dấu "đang đọc" bị bỏ quên trông y hệt một lượt đọc đang chạy, và
       *   bản trước tin nó vô điều kiện — nên đúng những hàng kẹt nặng nhất là
       *   những hàng lưới này bỏ qua. Quá mốc thì coi như KHÔNG có việc, dọn
       *   dấu, và để đoạn dưới xếp lại lượt đọc.
       */
      if (this.hydrating.has(key) && !this.hydratingIsReal(key, now)) {
        this.clearHydrating(key);
        this.stats.stuckReadRepairs = (this.stats.stuckReadRepairs || 0) + 1;
        this.log(`[GATE] #${row.tokenId} mang dấu "đang đọc" quá ` +
          `${OfferItemEngineV2.HYDRATING_STUCK_MS / 1000}s mà không có lượt đọc nào về — dọn dấu và đọc lại`);
      }
      const covered = this.hydrating.has(key) || this.firstReadTimers.has(key) ||
        Boolean(pending && pending.timer);

      /**
       * MỘT LẦN, KHI CHỜ ĐÃ QUÁ LÂU — KHÔNG PHẢI MỖI GIÂY
       *
       *   Có lượt đọc thật mà vẫn chờ hàng phút là chuyện CÓ THẬT và hợp lệ:
       *   429 đẩy hạn mức đọc xuống 1/giây, một lượt đọc đầy đủ là bốn trang.
       *   Nên đây không phải chỗ để bỏ cuộc — bỏ cuộc nghĩa là đặt lại giá
       *   trên một cuốn sổ chưa xác nhận, đúng cái lỗi #44 đã sửa ở 1.19.15.
       *
       *   Nhưng người dùng đang nhìn một spinner không lời. Nói một lần, kèm
       *   đúng những thứ trả lời được "vì sao": có đang đọc không, nợ đọc còn
       *   không, hẹn giờ còn không, và lần đọc đầy đủ gần nhất cách bao lâu.
       */
      const since = this.renewSince.get(key);
      if (since && !since.warned && now - since.at > RENEW_STUCK_WARN_MS) {
        since.warned = true;
        const book = this.book.get(key);
        const full = book && book.lastFullReadAt ? Math.round((now - book.lastFullReadAt) / 1000) + "s trước" : "chưa có";
        this.log(`[CHẨN ĐOÁN] #${row.tokenId} đang xác nhận lại Best ${Math.round((now - since.at) / 1000)}s ` +
          `(đang đọc=${this.hydrating.has(key)} nợ đọc=${Boolean(pending)} hẹn giờ=${Boolean(pending && pending.timer) || this.firstReadTimers.has(key)} ` +
          `đọc đầy đủ cuối=${full}) — vẫn đang thử, KHÔNG đặt lại giá theo sổ chưa xác nhận`);
      }

      if (covered) continue;
      orphans++;
      this.readStats.orphanFirstReads++;
      const book = this.book.get(key);
      if (book && book.lastFullReadAt > 0 && !this.renewCheck.has(key)) {
        // Sổ đã có thẩm quyền: cổng lẽ ra đã phải mở. Mở, đừng đọc lại.
        this.awaitingFirstRead.delete(key);
        this.pendingReads.delete(key);
        this.log(`[GATE] #${row.tokenId} đã có lượt đọc đầy đủ nhưng cổng còn đóng — mở cổng`);
        this.evaluate(key, now, null);
        continue;
      }
      this.log(`[GATE] #${row.tokenId} chờ lượt đọc đầu mà không có lượt đọc nào — xếp lại`);
      this.queueRead(row, { reason: "gate-repair", authoritative: false, readAt: now, firstRead: true, attempt: 1 });
    }
    if (orphans) this.pump();
    return orphans;
  }

  /**
   * THÊM HÀNG KHI ĐANG CHẠY — INCREMENTAL, KHÔNG RESTART
   *
   *   Đo trên sản phẩm: Add một NFT lúc engine đang chạy → `bridge.start(urls
   *   vừa thêm)` → `engine.start({ rows: chỉ hàng mới })` → `rows.clear()`.
   *   Hàng cũ rời khỏi `this.rows` trong khi sổ của nó vẫn còn: sự kiện
   *   vẫn cập nhật Best (giao diện thấy), nhưng `evaluate()` gặp
   *   `rows.get(key)` = undefined và im lặng thoát — không quyết định,
   *   không gửi, "Đang xử lý" mãi. Log "START · 1 NFT" chính là dấu vết.
   *
   *   Nay Add lúc đang chạy đi qua đây: đăng ký hàng mới (bảng hàng + sổ),
   *   dựng template, đóng cổng lượt-đọc-đầu cho RIÊNG hàng mới, đọc trạng
   *   thái ban đầu + trait cho hàng mới. Hàng cũ không bị đụng: sổ, own,
   *   ý định, lịch thử lại, epoch — tất cả giữ nguyên.
   *
   * @returns {{ok:boolean, added:number, resumed:number, tracked:number}}
   */
  async addRows(rows = []) {
    if (this.state !== STATE.RUNNING || !this.builder) {
      return { ok: false, error: "engine chưa chạy — dùng start()" };
    }
    const fresh = [];
    let resumed = 0;
    for (const row of rows) {
      const key = tokenKey(this.chain, row.contract, row.tokenId);
      const existing = this.rows.get(key);
      if (existing) {
        // Đã có: chỉ cập nhật cấu hình và cho chạy lại nếu đang tạm dừng.
        this.patchRow(key, row);
        if (!existing.running) { this.resumeRow(key); resumed++; }
        continue;
      }
      this.registerRow(row);
      this.firstPostPending.add(key);
      // Seed đủ thẩm quyền (đường Add vừa đọc cả danh sách) ⇒ không cần lượt
      // đọc đầu; cổng không đóng, hàng chạy ngay.
      const seeded = Boolean(row.seed && row.seed.complete && this.book.get(key) &&
        this.book.get(key).lastFullReadAt > 0);
      if (!seeded) this.awaitingFirstRead.add(key);
      const added = this.rows.get(key);
      added.seeded = seeded;
      fresh.push(added);
    }
    if (fresh.length) {
      try {
        await this.prewarm(fresh);
      } catch (error) {
        this.log(`add: prewarm lỗi: ${error.message}`);
      }
      const slugs = new Set(fresh.map(r => r.collectionSlug).filter(Boolean));
      if (typeof this.adapter.fetchCollectionOffersFor === "function") {
        for (const slug of slugs) this.queueCollectionSeed(slug, "add");
      }
      const readAt = Date.now();
      let queued = 0;
      if (typeof this.adapter.fetchBest === "function") {
        for (const row of fresh) {
          if (row.seeded) { this.evaluate(row.key, readAt, null); continue; }   // đã có snapshot
          this.queueRead(row, { reason: "startup", authoritative: false, readAt, firstRead: true, attempt: 1 });
          queued++;
        }
      } else {
        for (const row of fresh) { this.awaitingFirstRead.delete(row.key); this.evaluate(row.key, readAt, null); }
        this.pump();
      }
      this.hydrateTraits("add");
      const slugCount = new Set([...this.rows.values()].map(r => r.collectionSlug).filter(Boolean)).size;
      this.log(`ADD · +${fresh.length} NFT · tracked=${this.rows.size} · collections=${slugCount} · đọc nền ${queued}`);
    } else if (resumed) {
      this.log(`ADD · chạy lại ${resumed} NFT · tracked=${this.rows.size}`);
    }
    this.changed();
    return { ok: true, added: fresh.length, resumed, tracked: this.rows.size };
  }

  /**
   * Nạp sẵn mọi thứ đường nóng sẽ cần.
   *
   * Mỗi trường ở đây là một lượt chờ mạng đã bị đẩy ra khỏi khoảng
   * target_ready → sign_finished.
   */
  async prewarm(only = null) {
    const started = Date.now();
    let templates = 0;
    const rowsToWarm = only ? only : [...this.rows.values()];

    // Cấu hình chain: chainId, WETH, conduit, zone. Một lần cho cả engine.
    const config = await this.adapter.chainConfig();
    this.chainCfg = config;

    // Counter của ví: đọc nền, có TTL, KHÔNG đọc lại mỗi offer.
    const prewarmEpoch = this.epoch;
    const account = await this.adapter.accountState(this.builder.address);
    if (this.epoch !== prewarmEpoch) return { ok: false, superseded: true };   // wallet switched meanwhile
    this.accountCfg = account;

    /**
     * OFFER ĐANG MỞ CỦA CHÍNH MÌNH — một lượt REST, TRƯỚC mọi phép tính.
     *
     *   `fetchBest` chỉ trả BEST của NFT. Khi offer của mình đã bị vượt, best
     *   là của đối thủ và sổ không hề biết mình đang có một order sống — nên
     *   engine tính lại và gửi một order MỚI chồng lên cái cũ: hai offer cùng
     *   ví trên một NFT, tiền bị khoá hai lần. Reconnect đã có resync; Start
     *   thì chưa. Đo trên ví thật lúc live proof.
     *
     *   Đặt ở prewarm chứ không ở nền: sổ own phải đúng TRƯỚC khi hydrate
     *   tính lại bất kỳ hàng nào, nếu không cũng vẫn là hai offer.
     */
    /**
     * KHÔNG CÒN RÀO CHẮN TOÀN CỤC Ở PREWARM (1.25.1)
     *
     *   Resync own toàn ví và đọc số dư từng được await ở đây — Start phải đợi
     *   chúng mới RUNNING. Nay cả hai chạy nền: resync qua RecoveryPlane (P1,
     *   xếp sau khi RUNNING), số dư qua một promise tách rời. Hàng nào đã có
     *   lượt đọc đầy đủ của riêng nó (own soát theo hàng) gửi được ngay; số
     *   dư chưa biết thì không chặn (LOW_BALANCE đã biết vẫn chặn).
     */
    this.prewarmOwnIncomplete = true;
    if (typeof this.adapter.walletBalance === "function") {
      const epoch = this.epoch;
      this.adapter.walletBalance(this.builder.address).then(balance => {
        if (this.epoch === epoch) this.balance = balance;
      }).catch(error => {
        this.log(`prewarm: không đọc được số dư: ${error.message}`);
      });
    }

    const bySlug = new Map();
    for (const row of rowsToWarm) {
      if (!bySlug.has(row.collectionSlug)) bySlug.set(row.collectionSlug, []);
      bySlug.get(row.collectionSlug).push(row);
    }

    /**
     * TEMPLATE DỰNG Ở NỀN, KHÔNG CHẶN START
     *
     *   `collectionConfig` đọc phí qua REST và chuẩn token (có thể qua RPC).
     *   Với 80 NFT trên chục collection, chờ cho xong hết trước khi engine
     *   chạy là chờ chục lượt mạng nối tiếp — đúng cái làm Start chậm. Từ
     *   1.19.16 hàng không có template đã không thể xin lượt ghi, nên dựng
     *   template ở nền là an toàn: hàng nào xong trước chạy trước.
     */
    for (const [slug, rows] of bySlug) {
      let collection = null;
      try {
        collection = await this.adapter.collectionConfig(slug, rows[0].contract, { peek: true });
      } catch (error) {
        // KHÔNG bỏ qua trong im lặng: từng hàng của collection này được hẹn
        // dựng lại ở nền (xem ensureTemplate) — hàng không có template không
        // bao giờ vào hàng chờ ghi.
        this.log(`prewarm: không lấy được cấu hình ${slug}: ${error.message} — sẽ dựng template lại ở nền`);
        for (const row of rows) this.noteTemplateFailure(row, error.message);
        continue;
      }
      if (!collection) {
        // Chưa có sẵn trong cache: dựng ở NỀN cho từng hàng (single-flight),
        // Start không chờ.
        for (const row of rows) setImmediate(() => this.ensureTemplate(row.key, "prewarm-bg"));
        continue;
      }
      for (const row of rows) {
        try {
          this.applyTemplate(row, config, account, collection);
          templates++;
        } catch (error) {
          row.lastError = `template: ${error.message}`;
          this.log(`prewarm: #${row.tokenId} ${error.message} — sẽ dựng template lại ở nền`);
          this.noteTemplateFailure(row, error.message);
        }
      }
    }

    // Mở sẵn kết nối HTTPS: offer đầu tiên không phải bắt tay TLS.
    // Hâm nóng TLS: tối ưu tách rời, không đứng trước RUNNING.
    this.http.warmUp(this.getApiKey()).catch(() => {});

    return {
      ms: Date.now() - started, templates,
      chainId: config.chainId, counter: account.counter,
      http: "detached", httpMs: 0
    };
  }

  /**
   * DỪNG KHẨN CẤP.
   *
   * Giữ đúng ngữ nghĩa Reset của v1.19.4: vô hiệu hoá lượt chạy của CHÍNH tab
   * này, không đụng tab khác, không tắt thứ dùng chung — broker là của cả máy
   * và các cửa sổ khác vẫn cần nó.
   */
  reset(reason = "reset") {
    this.epoch++;
    // Lượt đang bay: huỷ ở broker (không ăn nhịp của epoch mới) và NHẢ KHOÁ
    // single-flight TRƯỚC khi dọn ý định — `finally` của lượt cũ không nhả
    // nữa vì số hiệu đã đổi; không nhả ở đây là token đó chết hẳn.
    for (const [key, f] of this.flights) {
      try { if (f.handle && f.handle.cancel) f.handle.cancel("reset"); } catch { /* đã xong */ }
      try { f.controller?.abort("reset"); } catch { /* đã xong */ }
      this.intents.release(key);
    }
    this.flights.clear();
    this.acquiring = 0;
    this.servedAt.clear();
    this.intents.reset();
    // Every open latency trace belonged to a flight/intent that no longer exists.
    this.metrics.discardOpen();
    this.recovery.invalidate(reason);
    this.ownSyncPending = false;
    this.ownSyncAt = 0;
    this.ownUnknownAt = Date.now();
    if (this.ownSyncRetryTimer) clearTimeout(this.ownSyncRetryTimer);
    this.ownSyncRetryTimer = null;
    // Stop/Reset thật: lần Start kế tiếp phải qua cổng own LẠNH của từng hàng
    // (resync lúc prewarm hoặc lượt đọc đầy đủ) — ownAuthoritative là per-row.
    for (const book of this.book.books.values()) {
      if (reason === "wallet-changed") {
        book.own.clear();
        book.ownUnknownAt = this.ownUnknownAt;
      }
      book.ownKnownAt = 0;
      book.ownReconciledAt = 0;
    }
    this.buffer.length = 0;
    this.buffering = false;
    this.hydrating.clear(); this.hydratingSince.clear();
    this.awaitingFirstRead.clear();
    this.downwardAuthority.clear();
    this.topicRepairAt.clear();
    this.traitProbedAt.clear();
    this.traits.retain([...this.rows.keys()]);
    this.balanceProbedAt = 0;
    this.balanceResultAt = 0;
    this.balanceCeiling = null;
    if (reason === "wallet-changed") {
      /**
       * THE OLD SIGNER IS GONE, AND THE ENGINE IS NOT "WARM" ANY MORE (1.25.12)
       *
       *   reset() left state RUNNING and the builder in place, so the bridge's
       *   rebind saw a "warm" engine and RESUMED the rows instead of starting
       *   one: the engine went on signing with wallet A after the user had
       *   switched to B (production: "đổi sang ví B vẫn báo Không đủ số dư").
       *   Stopping here and dropping the builder forces the cold start that
       *   creates B's signer; STOPPED also fences every late A callback that
       *   checks the state.
       */
      this.state = STATE.STOPPED;
      this.builder = null;
      if (this.book && typeof this.book.setSelfAddress === "function") this.book.setSelfAddress("");
      // EVERYTHING wallet-dependent belongs to the OLD wallet (1.25.11/12):
      // balance, counter, LOW_BALANCE incidents, server refusals, errors and
      // retry state it produced. The new wallet starts clean.
      this.balance = null;
      this.accountCfg = null;
      this.balanceProbeFailures = 0;
      this.balanceProbeInFlightAt = 0;
      for (const row of this.rows.values()) {
        row.lowBalance = false; row.serverLowBal = null; row.sendBlockedBy = null;
        row.lastError = ""; row.failures = 0; row.retryAt = 0; row.renewPending = false;
      }
    }
    this.stopWatchingStreamHealth();
    this.stopRecoveryScheduling();
    this.stopExpirySweep();
    this.stopLoops();
    for (const timer of this.firstReadTimers.values()) clearTimeout(timer);
    this.firstReadTimers.clear();
    this.inFlightSince.clear();
    for (const pr of this.pendingReads.values()) if (pr.timer) clearTimeout(pr.timer);
    this.pendingReads.clear();
    this.slugReadAt.clear();
    this.renewCheck.clear();
    this.renewSince.clear();
    this.gateClosedAt.clear();
    this.orphanLog.clear();
    this.pendingWakes.clear();
    this.ownSyncBackoffMs = 0;
    this.seededRows.clear();
    for (const ts of this.templateState.values()) if (ts.timer) clearTimeout(ts.timer);
    this.templateState.clear();
    this.slugSeedAt.clear();
    this.slugSeedInFlight.clear();
    this.gapReadAt.clear();
    for (const row of this.rows.values()) { row.running = false; row.retryAt = 0; row.failures = 0; }
    this.log(`RESET (${reason}) · epoch=${this.epoch}`);
    this.emit();
    return { ok: true, epoch: this.epoch };
  }

  stop() {
    this.reset("stop");
    this.state = STATE.STOPPED;
    return { ok: true };
  }

  /**
   * Cho MỘT hàng ngừng phản ứng, và quên thứ nó đang định gửi.
   *
   * VÌ SAO ĐÂY LÀ MỘT METHOD CHỨ KHÔNG PHẢI BA DÒNG Ở CHỖ GỌI
   *
   *   Cầu nối cần việc này ở bốn chỗ — Pause, Stop, khoá batch của Cancel, và
   *   gỡ khoá — và ở cả bốn nó tự tay làm ba việc: tìm hàng trong
   *   `engine.rows`, đặt `running = false`, rồi gọi `engine.intents.clear`.
   *   Đó là cầu nối biết cấu trúc bên trong engine, và nó sai theo đúng cách
   *   mà `main.js` đọc `orderBook` là sai: ngày engine đổi cách lưu hàng hay
   *   đổi cách huỷ ý định, bốn chỗ đó hỏng cùng lúc, im lặng.
   *
   *   Bốn chỗ gọi, một chỗ biết. Đó là ranh giới.
   *
   * QUÊN Ý ĐỊNH LÀ PHẦN BẮT BUỘC
   *
   *   Chỉ đặt `running = false` thôi thì một ý định đã xếp hàng vẫn có thể
   *   được `pump()` nhặt lên — và nó sẽ gửi một offer cho một NFT mà người
   *   dùng vừa tạm dừng, hoặc cho một NFT mà tab Cancel đang huỷ dở. Đó là
   *   chỗ sinh ra offer mồ côi.
   *
   * @returns {boolean} có hàng nào để dừng không
   */
  suspendRow(key) {
    const row = this.rows.get(key);
    if (row) row.running = false;
    this.recoveryCandidates.delete(key);
    this.intents.clear(key);
    return Boolean(row);
  }

  /**
   * XOÁ HẲN MỘT HÀNG (1.25.2): dừng + dọn MỌI thứ thuộc về nó — ý định (lượt
   * đang bay tự nhả và thấy hàng đã mất), lượt đọc/hẹn giờ, cổng, renew,
   * template, đánh thức, sổ. Trước đây bridge chỉ xoá sổ: hàng vẫn đăng ký,
   * vẫn running, hẹn giờ vẫn sống.
   */
  removeRow(key) {
    const row = this.rows.get(key);
    if (row) row.running = false;
    const f = this.flights.get(key);
    if (f) { try { if (f.handle && f.handle.cancel) f.handle.cancel("removed"); } catch { /* đã xong */ } }
    this.intents.clear(key);
    this.recoveryCandidates.delete(key);
    const pr = this.pendingReads.get(key);
    if (pr && pr.timer) clearTimeout(pr.timer);
    this.pendingReads.delete(key);
    const ft = this.firstReadTimers.get(key);
    if (ft) clearTimeout(ft);
    this.firstReadTimers.delete(key);
    this.clearHydrating(key);
    this.awaitingFirstRead.delete(key);
    this.downwardAuthority.delete(key);
    this.topicRepairAt.delete(key);
    this.renewCheck.delete(key); this.renewSince.delete(key);
    this.gateClosedAt.delete(key);
    this.pendingWakes.delete(key);
    this.orphanLog.delete(key);
    this.servedAt.delete(key);
    this.firstPostPending.delete(key);
    this.firstPostCompleted.delete(key);
    this.gapReadAt.delete(key);
    this.traitProbedAt.delete(key);
    const ts = this.templateState.get(key);
    if (ts && ts.timer) clearTimeout(ts.timer);
    this.templateState.delete(key);
    this.rows.delete(key);
    this.book.remove(key);
    this.changed();
    return Boolean(row);
  }

  /** Cho một hàng phản ứng trở lại. Không tự dựng lại ý định — sự kiện kế tiếp sẽ. */
  resumeRow(key) {
    const row = this.rows.get(key);
    if (!row) return false;
    row.running = true;
    row.manualPriorityAt = Date.now();
    // Chạy lại là một thay đổi trạng thái thật: tính lại ngay, không đợi sự kiện.
    if (this.state === STATE.RUNNING) { this.evaluate(key, Date.now(), null); this.pump(); }
    return true;
  }

  /**
   * Sửa Min/Max/Step/Duration của một hàng ĐANG CHẠY.
   *
   * Giá trị mới có hiệu lực từ sự kiện kế tiếp, không cần Start lại — đó là
   * ngữ nghĩa v1.19.4 và người dùng dựa vào nó khi họ chỉnh giá giữa chừng.
   */
  patchRow(key, fields) {
    const row = this.rows.get(key);
    if (!row) return false;
    for (const field of ["minPrice", "maxPrice", "step", "duration"]) {
      if (fields[field] !== undefined) row[field] = Number(fields[field]);
    }
    if (fields.maxPrice !== undefined) row.maxPrice = normalizeMaxDown(fields.maxPrice);
    // Ưu tiên là cờ, không phải số: đi qua Number() sẽ thành 0/1 rồi thành
    // false ở mọi chỗ đọc nó.
    if (fields.priorityMode !== undefined) {
      row.priorityMode = Boolean(fields.priorityMode);
    }
    // Min/Max/Step đổi là một transition của verdict: đánh thức hàng (1.25.2).
    if (fields.minPrice !== undefined || fields.maxPrice !== undefined || fields.step !== undefined) {
      this.scheduleRowWake(key, "config");
    }
    return true;
  }

  suspend() {
    this.state = STATE.SUSPENDED;
    this.stopWatchingStreamHealth();
    this.stopRecoveryScheduling();
    this.recovery.stop();
    this.log("SUSPEND");
    return { ok: true };
  }

  async resume() {
    // Sau khi máy ngủ dậy: kết nối cũ có thể đã chết mà chưa ai báo.
    this.recovery.resume();
    this.state = STATE.RUNNING;
    this.watchStreamHealth();
    await this.http.warmUp(this.getApiKey());
    this.log("RESUME · đã hâm nóng lại kết nối");
    this.emit();
    return { ok: true };
  }

  /**
   * Path B recovery requests are drained through one bounded per-NFT queue.
   * Full degraded-feed sweeps coalesce; reconnect repairs enter as scoped
   * per-topic requests after that topic's join or gap ACK.
   */
  scheduleRecoverySweep(reason, { slug = null, authoritative = false } = {}) {
    if (this.state !== STATE.RUNNING || typeof this.adapter.fetchBest !== "function") return 0;
    // A full degraded sweep already walks the current NFT set. Further health
    // ticks during its recovery window coalesce instead of rereading rows.
    if (!slug) {
      const now = Date.now();
      if (this.recoverySweepActive || now - this.lastFullRecoveryAt < FULL_RECOVERY_COOLDOWN_MS) return 0;
      this.lastFullRecoveryAt = now;
    }
    const wantedSlug = slug ? String(slug).toLowerCase() : "";
    const rows = [...this.rows.values()].filter(row => row.running &&
      (!wantedSlug || String(row.collectionSlug || "").toLowerCase() === wantedSlug));
    return this.scheduleRecoveryRows(rows, reason, { authoritative });
  }

  scheduleRecoveryRows(rows, reason, { authoritative = false } = {}) {
    if (this.state !== STATE.RUNNING || typeof this.adapter.fetchBest !== "function") return 0;
    let added = 0;
    let deduped = 0;
    for (const row of rows || []) {
      if (!row || !row.running || !this.rows.has(row.key)) continue;
      // A pending read already owns this token. Keeping it out of B's staging
      // queue prevents a retry/reconnect tick from producing another REST read.
      if (this.pendingReads.has(row.key) || this.hydrating.has(row.key)) { deduped++; continue; }
      if (!this.recoveryCandidates.has(row.key)) {
        added++;
        const slug = String(row.collectionSlug || `row:${row.key}`).toLowerCase();
        let queue = this.recoveryCandidateQueues.get(slug);
        if (!queue) {
          queue = [];
          this.recoveryCandidateQueues.set(slug, queue);
          this.recoveryCandidateSlugOrder.push(slug);
        }
        queue.push(row.key);
      } else deduped++;
      this.recoveryCandidates.set(row.key, { reason, authoritative: Boolean(authoritative) });
    }
    if (added) this.stats.recoveryCandidatesQueued = (this.stats.recoveryCandidatesQueued || 0) + added;
    if (deduped) this.stats.recoveryCandidatesDeduped = (this.stats.recoveryCandidatesDeduped || 0) + deduped;
    this.stats.recoveryCandidateMaxDepth = Math.max(this.stats.recoveryCandidateMaxDepth || 0, this.recoveryCandidates.size);
    if (this.recoveryCandidates.size) {
      this.recoverySweepActive = true;
      this.pumpRecoveryCandidate();
    }
    return added;
  }

  pumpRecoveryCandidate() {
    if (this.recoveryCandidateTimer) return;
    if (this.state !== STATE.RUNNING) {
      this.stopRecoveryScheduling();
      return;
    }
    let next = null;
    while (this.recoveryCandidateSlugOrder.length && !next) {
      const slug = this.recoveryCandidateSlugOrder.shift();
      const queue = this.recoveryCandidateQueues.get(slug);
      if (!queue) continue;
      while (queue.length) {
        const key = queue.shift();
        const request = this.recoveryCandidates.get(key);
        if (!request) continue;
        this.recoveryCandidates.delete(key);
        next = [key, request];
        break;
      }
      if (queue.length) this.recoveryCandidateSlugOrder.push(slug);
      else this.recoveryCandidateQueues.delete(slug);
    }
    if (!next) {
      this.recoveryCandidateQueues.clear();
      this.recoveryCandidateSlugOrder.length = 0;
      this.recoverySweepActive = false;
      return;
    }
    const [key, request] = next;
    const row = this.rows.get(key);
    if (row && row.running && !this.pendingReads.has(key) && !this.hydrating.has(key)) {
      this.queueRead(row, { reason: request.reason, authoritative: request.authoritative,
        readAt: Date.now(), firstRead: false, attempt: 1 });
    }
    if (this.recoveryCandidates.size) {
      this.recoveryCandidateTimer = setTimeout(() => {
        this.recoveryCandidateTimer = null;
        this.pumpRecoveryCandidate();
      }, RECOVERY_CANDIDATE_INTERVAL_MS);
      this.recoveryCandidateTimer.unref?.();
    } else {
      this.recoverySweepActive = false;
    }
  }

  stopRecoveryScheduling() {
    if (this.recoveryCandidateTimer) clearTimeout(this.recoveryCandidateTimer);
    this.recoveryCandidateTimer = null;
    this.recoveryCandidates.clear();
    this.recoveryCandidateQueues.clear();
    this.recoveryCandidateSlugOrder.length = 0;
    this.recoverySweepActive = false;
  }

  /** Stream reconnected. Path A resumes immediately; Path B repairs in background. */
  onStreamReconnect() {
    if (this.state !== STATE.RUNNING) return;
    this.lastStreamGapAt = Date.now();
    this.stats.streamGaps = (this.stats.streamGaps || 0) + 1;
    // Expiry is local book maintenance. Keep it off the callback that reopens
    // the realtime path, and never close a WARM row's send gate for recovery.
    // Path B is scoped to each collection's join/gap ACK below; starting a
    // second full sweep here duplicates those confirmed per-topic repairs.
    setImmediate(() => { if (this.state === STATE.RUNNING) this.sweepExpired(Date.now()); });
    this.log("RECONNECT · Path A mở ngay · Path B sẽ kiểm tra theo ACK từng collection");
  }

  shutdown() {
    this.stopWatchingStreamHealth();
    this.stopRecoveryScheduling();
    this.lastFullRecoveryAt = 0;
    this.stopExpirySweep();
    this.stopLoops();
    this.recovery.stop();
    this.http.destroy();
    this.state = STATE.STOPPED;
  }

  // ================================================================
  // Đường nóng
  // ================================================================

  /**
   * Một sự kiện Stream. ĐÂY LÀ ĐƯỜNG NÓNG — đồng bộ tới tận lúc có ý định.
   */
  handleStreamEvent(event) {
    if (this.state === STATE.STOPPED) return;
    this.stats.events++;
    if (!Number.isFinite(event.receivedMono)) event.receivedMono = mono();
    this.lastStreamEventAt = Date.now();
    this.lastActivityAt = this.lastStreamEventAt;

    if (this.buffering) {
      if (this.buffer.length < this.bufferCap) this.buffer.push(event);
      else this.stats.dropped++;
      return;
    }

    this.apply(event);
  }

  apply(event) {
    const receivedAt = Date.now();
    const op = toBookOp(event);
    if (!op) return;
    event.mappedMono = mono();
    if (op.op === OP.REVALIDATE) { this.onOrderRevalidate(op, receivedAt); return; }

    const selfMaker = String(this.builder?.address || "").toLowerCase();
    const isSelfCancel = op.op === OP.REMOVE && op.contract && op.tokenId && op.maker &&
      String(op.maker).toLowerCase() === selfMaker;
    const selfCancelKey = isSelfCancel && this.book.byNft
      ? this.book.byNft.get(`${op.contract}:${op.tokenId}`) : null;
    const selfCancelRow = selfCancelKey ? this.rows.get(selfCancelKey) : null;

    const trackedNftKey = op.contract && op.tokenId ? this.book.byNft?.get(`${op.contract}:${op.tokenId}`) : null;
    const preEventBook = trackedNftKey ? this.book.get(trackedNftKey) : null;
    const preEventBest = preEventBook ? preEventBook.effectiveBest(receivedAt).price : 0;
    const touched = this.book.apply(op, receivedAt);
    const trackedBookAfter = trackedNftKey ? this.book.get(trackedNftKey) : null;
    productionTrace.record("book_update", event, {
      chain: this.chain, collection: op.collectionSlug || event.collectionSlug,
      tokenId: op.tokenId, affected: touched.length,
      status: touched.length ? "applied" : "not-applied",
      previousBest: trackedNftKey ? preEventBest : null,
      best: trackedBookAfter ? trackedBookAfter.effectiveBest(receivedAt).price : null
    });
    if (op.endTime > 0 && touched.length) this.noteExpiry(op.endTime);
    // Drift diagnostics (1.25.1): a Stream REMOVE that matched nothing in any
    // tracked book vs one that did. REST later finding a dead order that a
    // matched REMOVE should have taken out would be an app bug.
    if (op.op === OP.REMOVE) this.netStat(touched.length ? "streamRemove" : "streamRemoveUntracked", 1);
    if (op.maker && String(op.maker).toLowerCase() === String(this.builder?.address || "").toLowerCase()) {
      for (const key of touched) {
        const book = this.book.get(key);
        if (book) {
          book.ownKnownAt = receivedAt;
          productionTrace.record("own_update", event, {
            chain: this.chain, collection: book.collectionSlug, tokenId: book.tokenId,
            status: "applied", mine: book.ownBest(receivedAt).price
          });
        }
      }
    }
    event.bookUpdatedMono = mono();
    if (op.orderHash && op.maker && String(op.maker).toLowerCase() === String(this.builder?.address || "").toLowerCase()) this.metrics.confirm(op.orderHash, event.receivedMono);

    /**
     * GAP RECOVERY: top vừa bị huỷ mà sổ không còn item offer nào.
     *
     *   Lượt đọc đầu chỉ mang về order dẫn đầu (+ criteria offer của
     *   collection), không phải item offer thấp hơn. Khi top rời đi mà nhóm
     *   item trống, có thể còn một item offer cũ hơn mà ta chưa thấy: đọc lại
     *   đúng token đó một lần (mặt phẳng nền, nguội 60s), trước khi gửi giá
     *   dựa trên phần còn lại của sổ.
     */
    if (op.op === OP.REMOVE && !isSelfCancel && touched.length && typeof this.adapter.fetchBest === "function") {
      for (const key of touched) {
        const book = this.book.get(key);
        if (!book || book.item.size > 0) continue;
        const last = this.gapReadAt.get(key) || 0;
        if (receivedAt - last < GAP_READ_COOLDOWN_MS) continue;
        this.gapReadAt.set(key, receivedAt);
        const row = this.rows.get(key);
        if (!row || !row.running) continue;
        const localBestAfter = book.effectiveBest(receivedAt).price;
        if (localBestAfter <= 0) {
          this.downwardAuthority.set(key, { fallback: preEventBest, at: receivedAt });
          this.closeGate(key, receivedAt); // gate riêng NFT này cho tới khi có evidence mới
        }
        this.scheduleRecoveryRows([row], "gap");
        this.stats.gapReads = (this.stats.gapReads || 0) + 1;
      }
    }

    // A fresh higher Stream bid is new market evidence. It immediately
    // supersedes an uncertain downward fallback, even while its read flies.
    if (op.op === OP.UPSERT) {
      for (const key of touched) {
        const gate = this.downwardAuthority.get(key);
        if (gate && op.price > gate.fallback + 1e-12) {
          this.downwardAuthority.delete(key);
          this.awaitingFirstRead.delete(key);
        }
      }
    }

    // Trait offer chạm những token mà ta không biết có mang trait đó không:
    // đi hỏi nguồn thật, ngoài đường nóng. Làm TRƯỚC lần thoát sớm bên dưới,
    // vì trường hợp phổ biến nhất là "không token nào áp được" — đúng cái
    // trường hợp cần hỏi.
    if (touched.unresolvedTrait) {
      this.resolveTraitScope(op, touched.unresolvedTrait, receivedAt);
    }

    /**
     * SELF REMOVE WHOSE HASH DRIFTED OUT OF THE BOOK (1.25.10)
     *
     *   A confirmed cancel of OUR order on a tracked NFT (the user cancelled on
     *   OpenSea) must wake that NFT now. When the exact hash was no longer in
     *   book.own (prior drift), book.apply reports "nothing changed" and the
     *   row slept until an unrelated event. The tombstone is already placed by
     *   book.apply; nothing else is removed here - evaluate() reads the CURRENT
     *   book, so a newer own order that is still live keeps the row ON_TOP.
     *   Pause and any OfferBot cancel flow (row.running === false) are
     *   respected by evaluate().
     */
    if (!touched.length && isSelfCancel) {
      const key = this.book.byNft && this.book.byNft.get(`${op.contract}:${op.tokenId}`);
      const row = key ? this.rows.get(key) : null;
      if (row && row.running) {
        this.stats.selfRemoveWakes = (this.stats.selfRemoveWakes || 0) + 1;
        const book = this.book.get(key);
        const known = book && (book.hydratedAt > 0 || book.lastEventAt > 0) && book.effectiveBest(receivedAt).price > 0;
        if (!known) {
          this.downwardAuthority.set(key, { fallback: preEventBest, at: receivedAt });
          this.closeGate(key, receivedAt);
        }
        this.evaluate(key, receivedAt, event);
        this.pump();
        this.queueRead(row, { reason: "self-cancel", authoritative: false, readAt: receivedAt, firstRead: !known, attempt: 1 });
      }
      return;
    }

    if (!touched.length) {
      // A tracked NFT whose event changed nothing (duplicate, tombstoned or
      // older order): say so, so "the competitor bid and nothing happened" is
      // never silent (1.25.12 diagnostics, off the hot path).
      if (op.contract && op.tokenId && this.book.byNft && this.book.byNft.get(`${op.contract}:${op.tokenId}`)) {
        this.streamDiag(op, [], "not-applied");
      }
      return;
    }
    this.stats.applied++;
    // Sổ đã đổi: bảng phải thấy — gộp, không phải mỗi sự kiện một lần vẽ.
    this.changed();

    // Collection/trait offer vừa trở thành Best của bao nhiêu NFT đang theo dõi.
    // Chỉ ghi khi nó THẬT SỰ dẫn đầu ở ít nhất một NFT — không spam offer thấp.
    if (op.op === OP.UPSERT && op.kind !== "item") {
      let leads = 0;
      for (const key of touched) {
        const b = this.book.get(key);
        if (b && b.effectiveBest(receivedAt).orderHash === op.orderHash) leads++;
      }
      if (leads) {
        this.log(`[BOOK] ${op.kind} ${op.collectionSlug} best=${op.price} qty=${op.quantity || 1} → affected=${leads}/${touched.length} NFT`);
      }
    }

    /**
     * CHIA LÔ KHI MỘT SỰ KIỆN CHẠM NHIỀU TOKEN
     *
     *   Một `collection_offer` áp cho MỌI token của collection. Với 1000 NFT
     *   đang theo dõi, tính lại cả nghìn cái trong một lượt đồng bộ đo được
     *   57ms, và một loạt sự kiện như vậy đẩy độ trễ event loop lên p99 =
     *   124ms. Trong 124ms đó, mọi sự kiện KHÁC — kể cả một cú vượt giá đang
     *   cần phản ứng ngay — đều phải chờ.
     *
     *   Lô đầu tính ngay, phần còn lại nhường sang lượt sau. NFT đầu tiên vẫn
     *   phản ứng trong micro giây; phần đuôi chậm hơn vài lượt event loop
     *   thay vì khoá cả vòng lặp. Đây KHÔNG phải độ trễ cố định: nó chỉ xuất
     *   hiện khi một sự kiện chạm nhiều hơn EVAL_CHUNK token.
     */
    if (touched.length <= EVAL_CHUNK) {
      const selfCancelBook = isSelfCancel ? this.book.get(selfCancelKey) : null;
      const selfCancelKnown = selfCancelBook && (selfCancelBook.hydratedAt > 0 || selfCancelBook.lastEventAt > 0) &&
        selfCancelBook.effectiveBest(receivedAt).price > 0;
      if (isSelfCancel && selfCancelRow?.running && !selfCancelKnown) {
        this.downwardAuthority.set(selfCancelKey, { fallback: preEventBest, at: receivedAt });
        this.closeGate(selfCancelKey, receivedAt);
      }
      for (const key of touched) this.evaluate(key, receivedAt, event);
      this.pump();
      if (isSelfCancel && selfCancelRow?.running) {
        this.queueRead(selfCancelRow, { reason: "self-cancel", authoritative: false, readAt: receivedAt, firstRead: !selfCancelKnown, attempt: 1 });
      }
      this.streamDiag(op, touched);
      return;
    }
    this.streamDiag(op, touched);

    const rest = touched.slice(EVAL_CHUNK);
    const selfCancelBook = isSelfCancel ? this.book.get(selfCancelKey) : null;
    const selfCancelKnown = selfCancelBook && (selfCancelBook.hydratedAt > 0 || selfCancelBook.lastEventAt > 0) &&
      selfCancelBook.effectiveBest(receivedAt).price > 0;
    if (isSelfCancel && selfCancelRow?.running && !selfCancelKnown) {
      this.downwardAuthority.set(selfCancelKey, { fallback: preEventBest, at: receivedAt });
      this.closeGate(selfCancelKey, receivedAt);
    }
    for (let i = 0; i < EVAL_CHUNK; i++) {
      this.evaluate(touched[i], receivedAt, event);
    }
    this.pump();
    if (isSelfCancel && selfCancelRow?.running) {
      this.queueRead(selfCancelRow, { reason: "self-cancel", authoritative: false, readAt: receivedAt, firstRead: !selfCancelKnown, attempt: 1 });
    }

    const drain = () => {
      if (this.state !== STATE.RUNNING) return;
      const slice = rest.splice(0, EVAL_CHUNK);
      for (const key of slice) this.evaluate(key, receivedAt, event);
      this.pump();
      if (rest.length) setImmediate(drain);
    };
    setImmediate(drain);
  }

  /**
   * STREAM → DISPATCH, ONE LINE PER TRACKED EVENT (1.25.12)
   *
   *   Production must answer "the competitor just outbid me" from the log
   *   alone: A event never arrived (no [STREAM RX]) · B arrived, not mapped
   *   ("tracked but not applied") · C mapped, decision not SEND (ON_TOP /
   *   ABOVE_MAX) · D SEND held by a blocker (low balance, first read,
   *   template) · E SEND with an intent/flight (then [SEND] SUBMIT … lines).
   *
   *   OFF the hot path: called after evaluate()+pump() already ran, and the
   *   line itself is built in setImmediate. Bounded: 20 lines/s per engine,
   *   the rest counted into one summary line. Only tracked NFTs are logged.
   */
  streamDiag(op, touched, note = "") {
    const now = Date.now();
    if (now - (this.diagWindowAt || 0) >= 1000) {
      if (this.diagSuppressed) this.log(`[STREAM RX] +${this.diagSuppressed} sự kiện NFT theo dõi không ghi chi tiết (giới hạn 20 dòng/s)`);
      this.diagWindowAt = now; this.diagLines = 0; this.diagSuppressed = 0;
    }
    if ((this.diagLines = (this.diagLines || 0) + 1) > 20) { this.diagSuppressed = (this.diagSuppressed || 0) + 1; return; }
    // The decision is captured NOW (right after evaluate+pump, what the event
    // caused); only the text is built later. Reading state in setImmediate
    // would report the world after the POST already landed.
    const t = Date.now();
    const snap = [];
    for (const key of touched.slice(0, 3)) {
      const row = this.rows.get(key);
      const book = this.book.get(key);
      if (!row || !book) continue;
      const best = book.effectiveBest(t).price;
      const mine = book.ownBest(t).price;
      const v = decide({ best, mine, minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step });
      const it = this.intents.get(key);
      snap.push({ tokenId: row.tokenId, best, mine, status: v.status, target: v.target, running: row.running,
        low: row.lowBalance, cold: this.awaitingFirstRead.has(key) && !(book.hydratedAt > 0 && best > 0), gate: row.sendBlockedBy && row.sendBlockedBy.gate,
        flying: this.intents.isInFlight(key), intent: it ? it.state : "NONE" });
    }
    const more = touched.length - Math.min(3, touched.length);
    setImmediate(() => {
      try {
        const short = v => (v ? `${String(v).slice(0, 6)}…${String(v).slice(-4)}` : "-");
        const who = op.contract && op.tokenId ? `#${op.tokenId}` : (op.collectionSlug || "-");
        this.log(`[STREAM RX] ${op.eventName || op.op} ${op.kind || ""} ${op.collectionSlug || "-"} ${who} maker=${short(op.maker)} price=${op.price} hash=${short(op.orderHash)}` +
          (note === "not-applied" ? " → tracked NFT but NOT applied (duplicate / tombstoned / older order)" : ` → [STREAM MAP] tracked rows=${touched.length}`));
        for (const d of snap) {
          let dispatch;
          if (!d.running) dispatch = "row paused/stopped";
          else if (d.status !== STATUS.SEND) dispatch = String(d.status);
          else if (d.low) dispatch = `SEND ${d.target} BLOCKED: SEND_REQUIRED_BUT_BALANCE_BLOCKED`;
          else if (d.cold) dispatch = `SEND ${d.target} BLOCKED: first-read`;
          else if (d.gate) dispatch = `SEND ${d.target} BLOCKED: ${d.gate}`;
          else if (d.flying) dispatch = `SEND ${d.target} · in flight (${d.intent})`;
          else dispatch = `SEND ${d.target} · intent=${d.intent}`;
          this.log(`[DISPATCH] #${d.tokenId} best=${d.best} mine=${d.mine} → ${dispatch}`);
        }
        if (more > 0) this.log(`[DISPATCH] … +${more} NFT khác cùng sự kiện`);
      } catch { /* diagnostics never break the engine */ }
    });
  }

  /**
   * Sổ đổi → giá cần gửi là bao nhiêu. Thuần cục bộ.
   *
   * `origin` (1.25.12) ranks the SEND it may create - REALTIME ALWAYS WINS:
   *   P0 (0)  a Stream event (competitor, equal price, self-cancel wake) or an
   *           own-offer expiry that needs an immediate renew;
   *   P1 (1)  first/cold authority, reads, resume (the default);
   *   P2 (2)  LOW_BALANCE recovery.
   * Without it a fresh counter-offer queued BEHIND every row a balance probe
   * had just unblocked (round-robin + oldest-first), one acquire at a time.
   */
  evaluate(key, receivedAt, event, origin = null) {
    const decisionStarted = mono();
    const rank = event || origin === "expiry" ? 0 : origin === "recovery" ? 2 : 1;
    const row = this.rows.get(key);
    if (!row || !row.running) return;
    const book = this.book.get(key);
    if (!book) return;

    const best = book.effectiveBest(receivedAt);
    const mine = book.ownBest(receivedAt);

    const verdict = decide({
      best: best.price, mine: mine.price,
      minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step
    });
    this.stats.decided++;
    productionTrace.record("decision", event, {
      chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId,
      status: verdict.status, reason: verdict.status, best: best.price,
      mine: mine.price, step: verdict.effectiveStep, configuredStep: row.step,
      max: verdict.max, target: verdict.target, source: this.triggerSource(event)
    });

    if (verdict.status !== STATUS.SEND) {
      if (verdict.status === STATUS.BAD_CONFIG && row.lastError !== verdict.reason) {
        row.lastError = verdict.reason;
        this.log(`#${row.tokenId} ${verdict.reason}`);
      }
      // LOW_BALANCE is a SEND blocker. When the current canonical verdict no
      // longer asks to send (ON_TOP / ABOVE_MAX / …) the old block must not
      // keep overriding the row's status (1.25.10).
      if (row.lowBalance) this.clearStaleLowBalance(row, verdict.status);
      this.intents.clear(key);
      return;
    }

    if (!this.topicReady(row, book)) {
      row.sendBlockedBy = { gate: "stream-topic", target: verdict.target, at: Date.now() };
      this.intents.clear(key);
      this.stats.topicHeld = (this.stats.topicHeld || 0) + 1;
      productionTrace.record("blocked", event, { chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId, status: "WAITING", reason: "topic-not-ready", target: verdict.target });
      // A row can resume after the outage callback, or its first topic join
      // can fail before any outage callback existed. The gate itself must
      // establish the targeted recovery owner; healthy topics never enter it.
      this.onTopicUnavailable(row.collectionSlug, row.sendBlockedBy.at, "send-blocked-topic");
      return;
    }

    if (this.downwardAuthority.has(key)) {
      row.sendBlockedBy = { gate: "shadow-authority-required", target: verdict.target, at: Date.now() };
      this.intents.clear(key);
      this.stats.downwardHeld = (this.stats.downwardHeld || 0) + 1;
      productionTrace.record("blocked", event, { chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId, status: "WAITING", reason: "shadow-authority-required", target: verdict.target });
      return;
    }

    // Insufficient WETH is a blocked state, not a transient retry. Only the
    // balance probe clears this flag while SEND is still needed; Stream churn
    // must not recreate SEND.
    if (row.lowBalance) {
      this.intents.clear(key);
      this.stats.lowBalanceBlocked = (this.stats.lowBalanceBlocked || 0) + 1;
      row.sendBlockedBy = { gate: "low-balance", target: verdict.target, at: Date.now() };
      productionTrace.record("blocked", event, { chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId, status: "WAITING", reason: "low-balance", target: verdict.target });
      // Never a silent dead end: a blocked row always has a recovery owner.
      if (!this.balanceRetryTimer && !(this.balanceProbeInFlightAt && Date.now() - this.balanceProbeInFlightAt < BALANCE_PROBE_INFLIGHT_MS)) {
        this.refreshBalanceSoon(Date.now());
      }
      return;
    }

    // Chưa có lượt đọc đầu tiên: KHÔNG gửi. Xem `awaitingFirstRead`.
    // WARM IS STICKY (CLAUDE.md §3A, 1.25.13): a gap/reconnect/renew/resume
    // read re-closes this gate on a row that is already WARM; that repair runs
    // in the background and must not hold a fresh counter. Only a COLD row
    // (never read) or an empty book (would send Min blind) waits.
    if (this.awaitingFirstRead.has(key) && !(book.hydratedAt > 0 && best.price > 0)) {
      this.stats.heldBeforeFirstRead = (this.stats.heldBeforeFirstRead || 0) + 1;
      row.sendBlockedBy = { gate: "first-read", target: verdict.target, at: Date.now() };
      this.intents.clear(key);
      productionTrace.record("blocked", event, { chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId, status: "WAITING", reason: "first-read", target: verdict.target });
      return;
    }
    // Chưa có template tĩnh: KHÔNG tạo ý định (không xin lượt ghi). Dựng lại ở
    // nền; xong thì hàng được tính lại từ sổ mới nhất.
    if (!this.templateReady(key)) {
      // SEND vẫn hợp lệ, chỉ thiếu template: GIỮ ý định ở WAITING (không xin
      // lượt ghi). ensureTemplate xong sẽ tính lại hàng → READY.
      this.stats.heldForTemplate = (this.stats.heldForTemplate || 0) + 1;
      row.sendBlockedBy = { gate: "template", target: verdict.target, at: Date.now() };
      productionTrace.record("blocked", event, { chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId, status: "WAITING", reason: "template", target: verdict.target });
      this.holdIntent(key, verdict, best, mine, book, receivedAt, "waiting-template");
      this.ensureTemplate(key, "evaluate");
      return;
    }

    const pendingTrace = this.intents.get(key)?.traceId;
    // Một event mới thay intent cũ theo latest-wins, nên trace cũng phải bắt
    // đầu tại event mới. Tái dùng trace recovery (không có event_received)
    // từng làm mất toàn bộ latency Stream → POST và trộn hai quyết định.
    if (event && this.metrics.get(pendingTrace)) {
      this.metrics.get(pendingTrace).fail("superseded-by-stream");
      this.metrics.finish(pendingTrace);
    }
    const traceId = (!event && this.metrics.get(pendingTrace))
      ? pendingTrace
      : `${this.chain}:${++traceSeq}`;
    if (!this.metrics.get(traceId)) {
      this.metrics.start(traceId, {chain:this.chain,tokenId:row.tokenId,generation:book.generation,price:verdict.target,trigger:event?'stream':'recovery'});
      if (event) {
        this.metrics.mark(traceId, "frame_received", event.receivedMono ?? decisionStarted);
        this.metrics.mark(traceId, "stream_decoded", event.decodedMono ?? event.receivedMono ?? decisionStarted);
        this.metrics.mark(traceId, "stream_mapped", event.mappedMono ?? event.decodedMono ?? decisionStarted);
        this.metrics.mark(traceId, "event_received", event.receivedMono ?? decisionStarted);
        this.metrics.mark(traceId, "book_updated", event.bookUpdatedMono ?? decisionStarted);
      }
      this.metrics.mark(traceId, "decision_ready");
      this.metrics.mark(traceId, "order_queue_enter");
    }

    /**
     * BẢN GHI KỸ THUẬT — KHÔNG PHẢI NHỊP QUÉT ĐỊNH KỲ
     *
     *   Mục 52 yêu cầu bỏ spam `[SCAN]` định kỳ, và V2 bỏ hẳn: không còn nhịp
     *   nào để mà ghi. Nhưng lúc cắt sang V2, dòng ghi QUYẾT ĐỊNH biến mất
     *   theo, và đó là mất mát chứ không phải dọn dẹp — tab Logs của v1.19.4
     *   kể cho người dùng "đang gửi offer #X" rồi "đã gửi offer #X", còn bản
     *   đầu của V2 im lặng hoàn toàn về mọi offer nó gửi.
     *
     *   Nên ghi lại, nhưng theo SỰ KIỆN chứ không theo đồng hồ: một dòng khi
     *   giá cần gửi ĐỔI. Giá không đổi thì không có dòng nào — đó đúng là
     *   khác biệt giữa một bản ghi và spam.
     */
    const previous = this.intents.get(key);
    if (!previous ||
        Math.abs(Number(previous.target) - Number(verdict.target)) >= 1e-12) {
      // Cả BA ứng viên ghi rõ, rồi mới tới người thắng: nhìn log là biết
      // nguồn nào đang thắng và nguồn nào vắng — không còn "Item biến mất"
      // mà không có dấu vết.
      const c = book.candidates(receivedAt);
      const p = v => (v && v.price > 0 ? v.price : "none");
      const mk = best.maker ? `${String(best.maker).slice(0, 6)}…${String(best.maker).slice(-4)}` : "-";
      this.log(`[DECISION] #${row.tokenId} item=${p(c.item)} collection=${p(c.collection)} trait=${p(c.trait)} ` +
        `-> competitor=${best.price} kind=${String(best.kind || "-").toUpperCase()} qty=${best.quantity || 1} maker=${mk} ` +
        `mine=${mine.price} step=${verdict.effectiveStep} target=${verdict.target}`);
    }

    /**
     * QUYẾT ĐỊNH GỬI VÀ Ô Ý ĐỊNH LÀ MỘT BƯỚC, KHÔNG PHẢI HAI
     *
     *   Log sản phẩm có chuỗi: `[DECISION] … target` → `[WATCHDOG] 1 hàng cần
     *   gửi mà không có ý định — tính lại` → `SUBMIT QUEUED`. Tức là giữa lúc
     *   quyết định và lúc có ô ý định, ô đó biến mất (bị `clear` bởi một
     *   nhánh khác chạy xen) và chỉ watchdog 5 giây sau mới dựng lại. Watchdog
     *   trở thành đường chạy bình thường.
     *
     *   Nay ghi ô rồi KIỂM NGAY: ô phải tồn tại và mang đúng target trước khi
     *   `evaluate` trả về. Không thì ghi lại một lần và đếm — con số đó là
     *   thước đo để biết lỗ hổng đã đóng.
     */
    // Một quyết định mới thay thế mọi lịch thử lại cũ (latest-wins).
    const retryAt = row.retryAt || 0;
    this.intents.set(key, {
      target: verdict.target, best: best.price, mine: mine.price,
      generation: book.generation, reason: verdict.reason, at: receivedAt
    });
    let entry = this.intents.get(key);
    if (!entry || Math.abs(Number(entry.target) - Number(verdict.target)) >= 1e-12) {
      this.stats.intentLost = (this.stats.intentLost || 0) + 1;
      this.intents.set(key, {
        target: verdict.target, best: best.price, mine: mine.price,
        generation: book.generation, reason: verdict.reason, at: receivedAt
      });
      entry = this.intents.get(key);
    }
    this.metrics.mark(traceId, "intent_updated");
    if (entry) {
      entry.traceId = traceId;
      entry.correlationId = event?.correlationId || previous?.correlationId || entry.correlationId || "";
      entry.notBefore = Math.max(entry.notBefore || 0, retryAt);
      // A newer decision can only RAISE the priority of a pending SEND.
      const prevRank = previous && previous.target && Number.isFinite(previous.priorityRank) ? previous.priorityRank : 3;
      entry.priorityRank = Math.min(prevRank, rank);
    }
    productionTrace.record("intent", event || { correlationId: entry?.correlationId }, {
      chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId,
      status: "READY", best: best.price, mine: mine.price,
      step: verdict.effectiveStep, max: verdict.max, target: verdict.target,
      source: this.triggerSource(event)
    });
    row.sendBlockedBy = null;
    // Recovery completions do not always have an outer event drain.  Once the
    // decision is committed, make it schedulable in this same turn instead of
    // relying on a watchdog to rediscover it.
    this.pump();
  }

  /**
   * Đẩy những ý định sẵn sàng đi tiếp.
   *
   * CONTINUOUS CHASE — KHÔNG CÒN "SỰ KIỆN → POST NGAY"
   *
   *   Stream vẫn cập nhật sổ, Best và target NGAY. Nhưng lượt GỬI đi qua
   *   controller adaptive theo API key ở QuotaBroker. Ở
   *   đây chỉ xếp hàng: mỗi token một dòng chảy, không ký trước, không sleep.
   *
   * THỨ TỰ LUÔN ĐƯỢC SẮP, KHÔNG PHỤ THUỘC THỨ TỰ MAP
   *
   *   Ưu tiên (ô tick) trước, rồi ý định CŨ hơn trước. Cờ ưu tiên đi tiếp
   *   xuống broker (`acquire({ priority })`), nên nó có tác dụng thật ở hàng
   *   chờ chung — và không bao giờ vượt nhịp gửi.
   */
  /**
   * TỒN ĐỌNG THEO TỪNG NFT, CHỌN NGƯỜI ĐI TIẾP NGAY TRƯỚC KHI XIN LƯỢT
   *
   *   Một NFT một ô ý định (latest-wins đã bảo đảm điều đó), nên tồn đọng là
   *   danh sách NFT KHÁC NHAU — không bao giờ có hai chỗ cho cùng một token.
   *   Thứ tự: ưu tiên trước (ô tick), rồi NFT nào vừa được phục vụ GẦN ĐÂY
   *   NHẤT đi sau, rồi chờ lâu nhất. Vòng thứ hai của một NFT nóng không được
   *   chen trước vòng thứ nhất của những NFT khác đang chờ.
   *
   *   `queuedAt` giữ nguyên qua mọi lần tính lại (xem IntentStore.set), nên
   *   một NFT bị vượt giá năm mươi lần vẫn giữ chỗ cũ chứ không thành "yêu
   *   cầu mới" mỗi lần.
   */
  pump() {
    if (this.state !== STATE.RUNNING) return;

    const census = this.intents.census();
    const room = MAX_INFLIGHT_SUBMITS - census.inFlight;
    if (room <= 0) return;   // đầy; lượt đang bay sẽ gọi lại pump khi xong
    // Chỉ MỘT lượt xin giấy phép của engine này được nằm trong broker.
    if (this.acquiring >= MAX_ACQUIRING) return;

    const ready = this.intents.ready();
    if (!ready.length) return;
    ready.sort((a, b) => {
      const pa = this.rows.get(a.tokenKey);
      const pb = this.rows.get(b.tokenKey);
      const ma = pa && pa.manualPriorityAt ? 1 : 0;
      const mb = pb && pb.manualPriorityAt ? 1 : 0;
      // REALTIME ALWAYS WINS (CLAUDE.md §1/§10): P0 before P1 before P2.
      const ra = Number.isFinite(a.priorityRank) ? a.priorityRank : 1;
      const rb = Number.isFinite(b.priorityRank) ? b.priorityRank : 1;
      if (ra !== rb) return ra - rb;
      // Within non-realtime work, a live-added NFT gets one first-POST turn.
      // It only enters this ready queue after its own authority gate is open.
      const fa = this.firstPostPending.has(a.tokenKey) ? 1 : 0;
      const fb = this.firstPostPending.has(b.tokenKey) ? 1 : 0;
      if (fa !== fb) return fb - fa;
      if (ma !== mb) return mb - ma;
      const va = pa && pa.priorityMode ? 1 : 0;
      const vb = pb && pb.priorityMode ? 1 : 0;
      if (va !== vb) return vb - va;
      // Vừa được phục vụ thì nhường lượt: vòng tròn theo NFT khác nhau.
      const sa = this.servedAt.get(a.tokenKey) || 0;
      const sb = this.servedAt.get(b.tokenKey) || 0;
      if (sa !== sb) return sa - sb;
      // Chờ LÂU NHẤT trước — mốc xếp hàng, không phải mốc tính lại gần nhất.
      return (a.queuedAt || a.firstSeenAt || a.at || 0) - (b.queuedAt || b.firstSeenAt || b.at || 0);
    });

    const batch = ready.slice(0, Math.min(MAX_ACQUIRING - this.acquiring, room));
    for (const intent of batch) {
      if (!this.intents.acquire(intent.tokenKey)) continue;
      this.metrics.mark(intent.traceId, "order_queue_exit");
      const flight = {
        id: ++this.flightSeq,
        since: Date.now(),
        stage: "queued",
        handle: {},
        controller: new AbortController(),
        manualPriority: Boolean(this.rows.get(intent.tokenKey)?.manualPriorityAt)
      };
      const dispatchRow = this.rows.get(intent.tokenKey);
      if (dispatchRow) dispatchRow.manualPriorityAt = 0;
      this.flights.set(intent.tokenKey, flight);
      this.inFlightSince.set(intent.tokenKey, flight.since);
      this.lastDispatchAt = flight.since;
      this.acquiring++;
      this.servedAt.set(intent.tokenKey, flight.since);
      this.stats.dispatched = (this.stats.dispatched || 0) + 1;
      // Không await: mỗi token là một dòng chảy riêng, và một token chậm
      // không được giữ những token khác lại.
      this.submitOne(intent.tokenKey, flight).catch(error => {
        this.log(`submit lỗi: ${String(error && error.message || error).slice(0, 140)}`);
        // Một exception bất ngờ không được làm token chết: coi là tạm thời.
        if (this.flights.get(intent.tokenKey) === flight) {
          try { this.scheduleRetry(intent.tokenKey, 0, `exception: ${String(error && error.message || error).slice(0, 80)}`); } catch { /* đã hết đường */ }
        }
      }).finally(() => {
        // CHỈ nhả khoá của ĐÚNG lượt này. Lượt đã bị watchdog thay số hiệu thì
        // khoá đang thuộc về lượt mới — không đụng.
        if (this.flights.get(intent.tokenKey) === flight) {
          this.flights.delete(intent.tokenKey);
          this.intents.release(intent.tokenKey);
          this.inFlightSince.delete(intent.tokenKey);
        }
        if (!flight.acquireDone) { flight.acquireDone = true; this.acquiring = Math.max(0, this.acquiring - 1); }
        // Flight settle là một transition: hàng phải có đường tiến (hoặc thật
        // sự không cần gì). Rẻ, cục bộ, không tạo việc trùng.
        if (this.state === STATE.RUNNING) setImmediate(() => { this.ensureRowProgress(intent.tokenKey, "flight-settled"); this.pump(); });
      });
    }
  }

  /** Lượt bay này còn là lượt đang giữ khoá của token không? */
  flightAlive(key, flight) { return this.flights.get(key) === flight; }

  /**
   * Own state is temporarily unknown. Keep the SEND intent visible and
   * terminally accounted instead of deleting it: reconciliation will
   * re-evaluate it into READY, superseded, or no-longer-needed.
   */
  deferForOwnSync(key, row, traceId, reason = "own-state-unknown") {
    const intent = this.intents.get(key);
    if (!intent) return false;
    // WAITING, không phải RETRY: không có mốc thử lại nào để watchdog "cứu".
    // Chính lượt đối soát (read của hàng / resync) tính lại hàng khi xong.
    this.setIntent(key, INTENT.WAITING, {
      lastError: reason,
      deferredReason: reason,
      waitingSince: Date.now()
    });
    this.stats.ownSyncDeferred = (this.stats.ownSyncDeferred || 0) + 1;
    // Trace STAYS OPEN through the own-authority wait (1.25.2): the eventual POST
    // is still the answer to this event, so its latency must include the wait.
    // (Finishing here made every first-after-Start send unmeasured.)
    this.metrics.mark(traceId, "own_wait_started");
    productionTrace.record("blocked", { correlationId: intent.correlationId }, {
      chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId,
      status: "WAITING", reason: "own-state-unknown", target: intent.target
    });
    const own = this.ownDiagnostic(key);
    // Hàng chỉ mù vì POST mơ hồ của CHÍNH nó: đối soát đúng token đó (đọc
    // đầy đủ của token là một lượt kiểm own trọn vẹn). Hàng lạnh chưa từng
    // được soát: đồng bộ own ưu tiên P1 cho những hàng còn mơ hồ.
    /**
     * OWN AUTHORITY THEO TỪNG HÀNG (1.25.2)
     *
     *   1.25.1 giao hàng lạnh cho `queueOwnResync("pre-post-own")`: MỘT job
     *   ví + collection + đọc từng NFT cho 60–90 hàng chưa soát, hết hạn 120s,
     *   rơi, xếp lại — trong lúc đó hàng cần gửi nằm WAITING (production:
     *   waitOwn=14→23, "already-pending-or-unavailable" lặp). Nay chủ của chờ
     *   đợi là MỘT lượt đọc ĐẦY ĐỦ của đúng token này (P1): danh sách đầy đủ
     *   chứng minh own của token → ownReconciledAt → settle tính lại → gửi.
     *   Resync toàn ví chỉ còn là tối ưu nền P3.
     */
    const book = this.book.get(key);
    const rowAmbiguous = book && (book.ownUnknownAt || 0) > 0 &&
      Math.max(this.ownSyncAt || 0, book.ownReconciledAt || 0) > 0;
    const readReason = rowAmbiguous ? "post-uncertain" : "pre-post-own";
    const intentNow = this.intents.get(key);
    if (intentNow) intentNow.dependency = readReason === "post-uncertain" ? "post-reconcile" : "own";
    // Không có đường đọc token (adapter không có fetchBest): chủ duy nhất có
    // thể là resync ví — không xếp một lượt đọc không bao giờ chạy được.
    const queued = typeof this.adapter.fetchBest !== "function"
      ? (this.queueOwnResync("pre-post-own") || this.ownSyncPending)
      : (this.queueRead(row, { reason: readReason, authoritative: false, readAt: Date.now(), firstRead: false, attempt: 1,
          correlationId: intent.correlationId }) ||
        this.hydrating.has(key) || this.pendingReads.has(key));
    productionTrace.record("own_state", { correlationId: intent.correlationId }, {
      chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId,
      status: "WAITING", reason: "own-state-unknown", target: intent.target,
      ownAuthoritative: own.covered, ownSyncPending: own.ownSyncPending,
      ownSyncAt: own.ownSyncAt, ownReconciledAt: own.ownReconciledAt,
      ownKnownAt: own.ownKnownAt, ownUnknownAt: own.ownUnknownAt,
      ownReadOwned: queued
    });
    const logNow = Date.now();
    if (logNow - (row.lastDeferLogAt || 0) < 30000) return true;
    row.lastDeferLogAt = logNow;
    this.log(`[DIAG][${traceId || "no-trace"}] SUBMIT DEFERRED NFT #${row.tokenId} ` +
      `reason=${reason} ownAuthoritative=false ownSyncPending=${this.ownSyncPending} ` +
      `ownSyncAt=${this.ownSyncAt || 0} covered=${own.covered} uncovered=${own.uncovered} ` +
      `ownRead=${queued ? readReason + ":owned" : "NONE"} ` +
      `nextRetry=${this.intents.get(key)?.notBefore || 0}`);
    return true;
  }

  /** Licence từ chối ngay trước khi ký: hàng dừng ở FAILED, log một lần mỗi lý do. */
  refuseForLicence(key, row, traceId, gate) {
    const why = `licence: ${(gate && gate.error) || "không hợp lệ"}`;
    row.lastError = why;
    this.setIntent(key, INTENT.FAILED, { lastError: why });
    this.metrics.get(traceId)?.fail("licence");
    this.metrics.finish(traceId);
    if (row.licenceRefusedLogged !== why) {
      row.licenceRefusedLogged = why;
      this.log(`[SEND] SUBMIT BLOCKED NFT #${row.tokenId} ${why}`);
    }
  }

  /**
   * Một lượt gửi. Thứ tự: XIN LƯỢT → đọc lại trạng thái mới nhất → ký →
   * kiểm lần cuối → POST.
   *
   * KHÔNG KÝ TRƯỚC KHI CÓ LƯỢT
   *
   *   Khi server cooldown, một token có thể chờ nhiều giây trong hàng.
   *   Ký trước rồi ngồi chờ là cầm một giá cũ; tới lượt thì Best đã khác.
   *   Nên xin lượt trước (không tốn gì ngoài một chỗ trong hàng), và chỉ khi
   *   có lượt mới tính lại từ sổ HIỆN TẠI: Best, own, bước, target, Max —
   *   rồi ký, rồi kiểm lần cuối, rồi POST. Latest-wins đúng nghĩa.
   *
   * LỖI TẠM THỜI KHÔNG PHẢI DEAD-END
   *
   *   429 / 5xx / lỗi mạng / timeout → hẹn thử lại (backoff có trần), và lần
   *   thử lại tính lại từ sổ, không gửi lại giá cũ. Chỉ lỗi không tự hồi
   *   được (4xx khác, ví thiếu tiền) mới là FAILED.
   */
  async submitOne(key, flight = null) {
    const epochAtStart = this.epoch;
    const row = this.rows.get(key);
    if (!row) return;

    const first = this.intents.get(key);
    if (!first || !first.target) return;
    const traceId = first.traceId;
    const M = stage => this.metrics.mark(traceId, stage);
    const alive = () => !flight || this.flightAlive(key, flight);
    // The wallet this submit belongs to (1.25.12): a wallet switch bumps the
    // epoch, and nothing this submit learns afterwards may touch the new one.
    const submitEpoch = this.epoch;
    const stage = s => { if (flight) flight.stage = s; };
    /**
     * KEY ĐƯỢC CHỌN LÚC CẤP, KHÔNG PHẢI LÚC XẾP HÀNG
     *
     *   Bản 1.24.x `peek()` một key ở đây rồi xếp hàng vào domain của nó: năm
     *   SEND cùng lúc cùng thấy Key 1 và cùng chờ Key 1 trong khi Key 2 rảnh.
     *   Nay đưa DANH SÁCH domain của mọi key; broker trả về domain đã cấp và
     *   HTTP dùng đúng key đó — quota và HTTP luôn tính cho CÙNG một key.
     */
    const poolKeys = (this.getApiKeys() || []).filter(Boolean);
    const poolDomains = poolKeys.map(k => quotaFingerprint(k));
    let selectedApiKey = poolKeys[0] || this.getApiKey();
    let quotaDomain = quotaFingerprint(selectedApiKey);
    // Template phải sẵn TRƯỚC khi xin lượt: một hàng biết chắc chưa dựng được
    // order không được ăn một nhịp ghi. Ý định KHÔNG bị xoá — WAITING tới khi
    // template xong (ensureTemplate tính lại hàng).
    if (!this.templateReady(key)) {
      this.setIntent(key, INTENT.WAITING, { deferredReason: "waiting-template", waitingSince: Date.now() });
      this.metrics.finish(traceId);
      this.ensureTemplate(key, "pre-acquire");
      return;
    }

    // Own authority của RIÊNG hàng này — thuần cục bộ (xem ownAuthoritative).
    // Chỉ hàng lạnh chưa từng được soát, hoặc hàng vừa có POST mơ hồ, mới
    // phải chờ đối soát; không có kiểm mạng nào trên đường bình thường.
    if (!this.ownAuthoritative(key)) {
      this.deferForOwnSync(key, row, traceId);
      return;
    }
    {
      const own = this.ownDiagnostic(key);
      productionTrace.record("own_state", { correlationId: first.correlationId || traceId }, {
        chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId,
        status: "covered", target: first.target, ownAuthoritative: own.covered,
        ownSyncPending: own.ownSyncPending, ownSyncAt: own.ownSyncAt,
        ownReconciledAt: own.ownReconciledAt, ownKnownAt: own.ownKnownAt,
        ownUnknownAt: own.ownUnknownAt, ownReadOwned: false
      });
    }

    // ---- 1 · xin lượt ghi (nhịp toàn cục, có ưu tiên) --------------
    this.setIntent(key, INTENT.GRANTING, { brokerQueuedAt: Date.now() });
    stage("granting");
    M("quota_requested");
    // Chỉ ghi một dòng cho mỗi lần XẾP HÀNG MỚI; xếp lại sau hết hạn chờ thì im.
    if (!first.requeues) {
      this.log(`[SEND] SUBMIT QUEUED NFT #${row.tokenId} target=${first.target}` +
        (row.priorityMode ? " (ưu tiên)" : ""));
    }
    productionTrace.record("send_queued", { correlationId: first.correlationId || traceId }, {
      chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId,
      status: "QUEUED", best: first.best, mine: first.mine,
      max: row.maxPrice, target: first.target
    });
    if (this.quota) {
      const grant = await this.quota.acquire({
        lane: this.chain, domain: quotaDomain,
        domains: poolDomains.length ? poolDomains : null,
        timeoutMs: ACQUIRE_TIMEOUT_MS,
        priority: Boolean(row.priorityMode || flight?.manualPriority),
        // Giữ CHỖ trong hàng qua một lần hết hạn: mốc xếp hàng của token.
        at: first.queuedAt || first.firstSeenAt || 0,
        handle: flight ? flight.handle : null
      });
      // Chỗ "đang xin" được nhả NGAY khi broker trả lời — từ đây lượt này đi
      // tiếp bằng chỗ in-flight của nó, và NFT kế tiếp được xin lượt song song
      // với phần dựng/ký/POST của lượt này.
      if (flight && !flight.acquireDone) { flight.acquireDone = true; this.acquiring = Math.max(0, this.acquiring - 1); }
      if (this.state === STATE.RUNNING) setImmediate(() => this.pump());
      if (!alive()) return;   // watchdog đã thay lượt này; khoá thuộc lượt mới
      if (this.epoch !== epochAtStart) { this.setIntent(key, INTENT.IDLE); return; }
      if (!grant.ok) {
        /**
         * TỪ CHỐI TẠM THỜI PHẢI ĐƯỢC THỬ LẠI
         *
         *   `pump()` chỉ chạy khi có việc mới, nên một ý định bị từ chối nằm im
         *   cho tới sự kiện kế tiếp — và sự kiện kế tiếp có thể không bao giờ
         *   tới. Hẹn bơm lại theo gợi ý của broker; broker mới bầu xong thì
         *   yêu cầu xếp lại từ đầu. Không phải sleep trên đường nóng.
         */
        const cur = this.intents.get(key);
        if (cur) cur.requeues = (cur.requeues || 0) + 1;
        // Từ chối TỨC THÌ (no-broker / failover / stopped) thì đợi một quãng
        // trước khi xin lại; hết hạn chờ (đã đợi 120s) thì xin lại ngay.
        const instant = grant.why !== "timeout" && (Number(grant.waitedMs) || 0) < 50;
        const wait = instant ? Math.min(2000, Math.max(250, grant.waitMs || 250)) : 0;
        this.setIntent(key, INTENT.READY, { lastError: `quota:${grant.why}`, notBefore: wait ? Date.now() + wait : 0 });
        this.stats.acquireRequeues = (this.stats.acquireRequeues || 0) + 1;
        this.metrics.get(traceId)?.fail(`quota:${grant.why}`);
        this.metrics.finish(traceId);
        if (this.state === STATE.RUNNING) {
          const timer = setTimeout(() => this.pump(), Math.max(100, wait || 100));
          if (timer.unref) timer.unref();
        }
        return;
      }
      // Giấy phép thật: tiến triển thật của bộ điều phối.
      this.lastGrantAt = Date.now();
      if (grant.domain) {
        const at = poolDomains.indexOf(grant.domain);
        if (at >= 0) { selectedApiKey = poolKeys[at]; quotaDomain = grant.domain; }
      }
      this.stats.quotaWaitMs = Number(grant.waitedMs) || 0;
      this.netStat("quotaWait", Number(grant.waitedMs) || 0);
    }
    M("quota_granted");
    stage("granted");
    const curIntent = this.intents.get(key);
    if (curIntent) { curIntent.grantedAt = Date.now(); curIntent.requeues = 0; }

    // ---- 2 · tính lại từ sổ HIỆN TẠI -------------------------------
    const book = this.book.get(key);
    if (!book || this.epoch !== epochAtStart || !row.running) {
      this.setIntent(key, INTENT.IDLE);
      this.metrics.finish(traceId);
      return;
    }
    if (!this.topicReady(row, book)) {
      this.setIntent(key, INTENT.IDLE);
      this.metrics.get(traceId)?.fail("topic-not-ready");
      this.metrics.finish(traceId);
      this.stats.topicHeld = (this.stats.topicHeld || 0) + 1;
      return;
    }
    if (this.awaitingFirstRead.has(key) && !(book.hydratedAt > 0 && book.effectiveBest(Date.now()).price > 0)) {
      // Chưa có lượt đọc đầu (reconnect chen ngang): không gửi vào khoảng mù.
      // A WARM row with a known live competitor is not blind (§3A, 1.25.13).
      this.setIntent(key, INTENT.RETRY, {
        lastError: "waiting-first-read",
        notBefore: Date.now() + 250,
        deferredReason: "waiting-first-read"
      });
      this.log(`[SEND] SUBMIT DEFERRED NFT #${row.tokenId} waiting-first-read`);
      this.metrics.finish(traceId);
      return;
    }
    if (!this.templateReady(key)) {
      // Template rơi mất trong lúc chờ lượt: trả lượt về (không dựng), dựng lại ở nền.
      this.setIntent(key, INTENT.WAITING, {
        lastError: "waiting-template",
        deferredReason: "waiting-template",
        waitingSince: Date.now()
      });
      this.log(`[SEND] SUBMIT DEFERRED NFT #${row.tokenId} waiting-template`);
      this.metrics.finish(traceId);
      this.ensureTemplate(key, "post-grant");
      return;
    }
    M("validation_started");
    const now = Date.now();
    const best = book.effectiveBest(now);
    const mine = book.ownBest(now);
    const verdict = decide({
      best: best.price, mine: mine.price,
      minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step
    });
    if (verdict.status !== STATUS.SEND) {
      // Tới lượt thì không còn gì để gửi (đã dẫn đầu / vượt trần). Một lượt
      // của nhịp bị bỏ trống — rẻ hơn một order sai.
      if (row.lowBalance) this.clearStaleLowBalance(row, verdict.status);
      this.intents.clear(key);
      this.metrics.count("staleDropped");
      this.metrics.get(traceId)?.fail("no-longer-needed");
      this.metrics.finish(traceId);
      this.log(`#${row.tokenId} tới lượt nhưng không cần gửi nữa: ${verdict.reason}`);
      return;
    }
    const target = verdict.target;
    // Ghi target mới nhất vào ý định (latest-wins) rồi đi tiếp.
    const latestIntent = this.intents.set(key, {
      target, best: best.price, mine: mine.price,
      generation: book.generation, reason: verdict.reason, at: now
    });
    latestIntent.traceId = traceId;
    if (Math.abs(Number(first.target) - Number(target)) >= 1e-12) {
      this.log(`[DECISION] #${row.tokenId} tới lượt: best=${best.price} mine=${mine.price} → target=${target} (lúc xếp hàng: ${first.target})`);
    }

    // ---- 3 · số dư ---------------------------------------------------
    /**
     * ONE REFUSAL, NOT ONE PER ROW (1.25.12)
     *
     *   OpenSea answering "insufficient" for amount X says the wallet holds
     *   less than X NOW. The cached balance read (up to 5 min old) still said
     *   "enough", so every other row chasing at >= X signed and POSTed into the
     *   same refusal until the urgent probe came back (soak: 17 refusals in one
     *   logical minute when a wallet was drained). The refusal is a ceiling
     *   for every SEND >= X until a balance read NEWER than it answers.
     */
    const ceiling = this.balanceCeiling;
    if (ceiling && BigInt(this.adapter.toWei(target)) >= ceiling.wei) {
      row.lowBalance = true;
      row.lastError = `insufficient balance: OpenSea refused ${ethers.formatEther(ceiling.wei)} — chờ đọc số dư mới`;
      this.setIntent(key, INTENT.FAILED, { lastError: row.lastError });
      this.stats.ceilingHeld = (this.stats.ceilingHeld || 0) + 1;
      this.metrics.get(traceId)?.fail("low-balance");
      this.metrics.finish(traceId);
      this.refreshBalanceSoon(Date.now(), { urgent: true });
      return;
    }
    if (this.balance && this.balance.known) {
      const needWei = this.adapter.toWei(target);
      if (this.balance.wethWei < BigInt(needWei)) {
        row.lowBalance = true;
        row.lastError = "insufficient balance: WETH " +
          `${ethers.formatEther(this.balance.wethWei)} < ${target}`;
        this.setIntent(key, INTENT.FAILED, { lastError: row.lastError });
        this.metrics.get(traceId)?.fail("low-balance");
        this.metrics.finish(traceId);
        this.log(`[LOW BALANCE] #${row.tokenId} ${row.lastError}`);
        // The cached balance may be stale: verify with a fresh read now.
        this.refreshBalanceSoon(Date.now(), { urgent: true });
        return;
      }
    }

    // ---- 3b · cổng licence, NGAY TRƯỚC khi tiêu một chữ ký -------------
    //
    //   Cùng ngữ nghĩa v1.19.4: hỏi cổng ở thời điểm cuối trước khi ký. Bản
    //   viết lại Continuous Chase (1.19.8) làm rơi lời gọi này — nhịp licence
    //   vẫn dừng engine khi bị khoá, nhưng giữa lúc chủ sở hữu thu hồi và lúc
    //   nhịp đó chạy, một lượt gửi vẫn có thể đi. Cổng trả lời từ trạng thái
    //   đã biết (không chờ mạng), nên nó không nằm trên đường chờ.
    if (typeof this.licenceGate === "function") {
      let gate = null;
      try { gate = await this.licenceGate(); } catch (error) { gate = { ok: false, error: String(error && error.message || error) }; }
      if (!alive()) return;
      if (!gate || gate.ok === false) {
        this.refuseForLicence(key, row, traceId, gate);
        return;
      }
    }

    // ---- 4 · dựng + ký, thuần cục bộ --------------------------------
    //
    // DURATION ĐỌC NGAY LÚC NÀY, từ cấu hình HIỆN TẠI của hàng (`row` là
    // chính object `patchRow` sửa) — không phải bản chụp lúc ý định được
    // tạo. Người dùng đổi Duration khi ý định đang chờ lượt thì order ký ở
    // đây đã dùng số mới; retry và tự đặt lại sau hết hạn cũng đi qua đây.
    this.setIntent(key, INTENT.BUILDING);
    stage("building");
    M("validation_finished");
    let signed;
    const durationMinutes = Math.max(1, Number(row.duration) || 15);
    try {
      // Adapter thử nghiệm hoặc adapter bên thứ ba có thể chưa phát các mốc
      // chi tiết. Mark là idempotent, nên hai mốc bao ngoài vừa giữ telemetry
      // đầy đủ, vừa không ghi đè timestamp chính xác từ LocalOrderBuilder.
      M("build_started");
      const signer = this.builder;
      if (!signer || this.epoch !== submitEpoch || !alive()) return;
      signed = await signer.build(key, {
        amountWei: this.adapter.toWei(target),
        durationMinutes, onStage: M
      });
      M("build_finished");
      M("sign_finished");
    } catch (error) {
      const msg = String(error && error.message || error).slice(0, 140);
      if (/chưa hydrate template|template/i.test(msg)) {
        // Phụ thuộc tĩnh thiếu — KHÔNG phải lỗi gửi. Dựng lại ở nền, tính lại sau.
        this.setIntent(key, INTENT.WAITING, {
          lastError: "template-rebuild",
          deferredReason: "template-rebuild",
          waitingSince: Date.now()
        });
        this.log(`[SEND] SUBMIT DEFERRED NFT #${row.tokenId} template-rebuild`);
        this.metrics.get(traceId)?.fail("template");
        this.metrics.finish(traceId);
        this.noteTemplateFailure(row, msg);
        this.ensureTemplate(key, "build");
        return;
      }
      // Không dựng/ký được là lỗi cấu hình/ký — không tự hồi được.
      row.lastError = `build: ${msg}`;
      this.setIntent(key, INTENT.FAILED, { lastError: row.lastError });
      this.metrics.get(traceId)?.fail("build");
      this.metrics.finish(traceId);
      this.log(`[SEND] SUBMIT FAILED NFT #${row.tokenId} ${row.lastError}`);
      return;
    }

    if (!alive()) return;

    // ---- 5 · kiểm lần cuối ngay trước khi gửi — THUẦN CỤC BỘ ----------
    //   latest-wins (target còn là ý định mới nhất), epoch, row còn chạy,
    //   <= Max, own cục bộ chưa dẫn, Best cục bộ chưa vượt target, và own
    //   authority cục bộ của hàng. Không REST, không resync, không /profile.
    const bookNow = this.book.get(key);
    if (!bookNow || !this.ownAuthoritative(key)) {
      this.deferForOwnSync(key, row, traceId);
      return;
    }
    /**
     * GUARD CUỐI = CHÍNH decide(), KHÔNG PHẢI MỘT BIỂU THỨC RIÊNG (1.25.1)
     *
     *   1.25.0 dùng `mineNow >= bestNow` ở đây trong khi decide() dùng
     *   `mine > best`. Mine == Best (vd. Item 0.0317 của mình, Collection
     *   0.1268/4 = 0.0317 của đối thủ) → decide nói SEND 0.0318, guard nói
     *   "mình đang dẫn" → bỏ order đã ký → evaluate → SEND lại → vòng lặp
     *   SEND → ký → bỏ vô tận (đã thấy trên production).
     *
     *   Nay: chạy lại decide() trên sổ cục bộ hiện tại. SEND cùng target →
     *   POST. SEND target khác → order đã ký đã cũ, tính lại ngay. Bản sao own
     *   CHỈ khi own hiện tại >= CHÍNH target định gửi (semantic 1.19.61) — đó
     *   là sự thật cục bộ đã settle: không tính lại, chờ state đổi thật.
     */
    const nowWanted = this.intents.get(key);
    const stillWanted = nowWanted && Math.abs(Number(nowWanted.target) - Number(target)) < 1e-12;
    const atPost = Date.now();
    const bestNow = bookNow.effectiveBest(atPost).price;
    const mineNow = bookNow.ownBest(atPost).price;
    const vNow = decide({ best: bestNow, mine: mineNow,
      minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step });
    const ownDuplicate = mineNow > 0 && mineNow >= target - 1e-12;
    let dropReason = "";
    if (this.epoch !== epochAtStart || !row.running) dropReason = "stopped";
    else if (!this.topicReady(row, bookNow)) dropReason = "topic-not-ready";
    else if (ownDuplicate) dropReason = "own-duplicate";
    else if (!stillWanted) dropReason = "superseded";
    else if (vNow.status === STATUS.SEND && Math.abs(Number(vNow.target) - Number(target)) >= 1e-12) dropReason = "best-moved";
    else if (vNow.status === STATUS.ABOVE_MAX || target > Number(row.maxPrice) + 1e-12) dropReason = "over-max";
    else if (vNow.status !== STATUS.SEND) dropReason = vNow.status === STATUS.ON_TOP ? "on-top" : "no-longer-needed";
    if (dropReason) {
      const counter = { "own-duplicate": "signedDropOwnDuplicate", "best-moved": "signedDropBestMoved",
        superseded: "signedDropSuperseded", "over-max": "signedDropOverMax", stopped: "signedDropStopped" }[dropReason] ||
        "signedDropOther";
      this.stats[counter] = (this.stats[counter] || 0) + 1;
      this.netStat(counter, 1);
      this.metrics.count("staleDropped");
      this.metrics.get(traceId)?.fail(`stale-before-post:${dropReason}`);
      this.metrics.finish(traceId);
      this.log(`#${row.tokenId} bỏ order đã ký ${target} (${dropReason}): best=${bestNow} mine=${mineNow}` +
        (vNow.status === STATUS.SEND ? ` → target mới ${vNow.target}` : ` → ${vNow.status}`));
      // Terminal cho state hiện tại: own đã đạt target / đã dẫn / quá Max / đã dừng.
      // Chỉ target cũ (best-moved, superseded) mới tính lại ngay.
      this.setIntent(key, INTENT.IDLE, { target: 0 });
      if ((dropReason === "best-moved" || dropReason === "superseded") &&
          this.epoch === epochAtStart && row.running) {
        this.evaluate(key, Date.now(), null);
        setImmediate(() => this.pump());
      }
      return;
    }

    // ---- 6 · gửi ------------------------------------------------------
    //
    // CHẾ ĐỘ DEV KHÔNG GỬI TIỀN THẬT. Chặn ở đây, ngay trước POST, chứ không
    // chỉ ở tầng gọi: mọi đường (retry, đặt lại sau hết hạn, watchdog) đều đi
    // qua chỗ này. Bật lại phải cố ý bằng OFFERBOT_DEV_SPEND=yes.
    if (!devRuntime.spendAllowed()) {
      row.lastError = "dev: không gửi offer thật (OFFERBOT_ENV=development)";
      this.setIntent(key, INTENT.FAILED, { lastError: row.lastError });
      this.metrics.finish(traceId);
      if (!this.devSpendLogged) {
        this.devSpendLogged = true;
        this.log("[DEV] KHÔNG gửi offer thật — mọi lượt gửi bị chặn ở bước POST");
      }
      return;
    }
    this.setIntent(key, INTENT.SENDING);
    stage("sending");
    this.stats.submitted++;
    this.netStat("write", 1, quotaDomain);
    let res;
    try {
      res = await this.http.request({
        method: "POST",
        path: `/orders/${this.adapter.openseaChain}/seaport/offers`,
        apiKey: selectedApiKey,
        signal: flight?.controller?.signal || null,
        body: this.adapter.offerBody(signed, row),
        onStage: stage => {
          M(stage);
          if (stage === "http_started") productionTrace.record("http_start", { correlationId: first.correlationId || traceId }, {
            chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId,
            status: "POST", best: first.best, mine: first.mine, max: row.maxPrice, target
          });
        }
      });
    } catch (error) {
      this.lastPostAt = Date.now();
      if (!alive()) return;
      if (this.quota) {
        this.quota.report({ domain: quotaDomain, status: 0, latencyMs: 0 });
      }
      const msg = String(error && error.message || error).slice(0, 120);
      this.metrics.get(traceId)?.fail("http");
      this.metrics.finish(traceId);
      productionTrace.record("submit_failure", { correlationId: first.correlationId || traceId }, {
        chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId,
        status: "FAILED", reason: "http", target
      });
      /**
       * CHẮC CHẮN CHƯA GỬI vs CÓ THỂ ĐÃ TỚI SERVER
       *
       *   HttpPool đánh dấu `notSent` khi body chưa rời máy trọn vẹn (lỗi
       *   DNS/kết nối/huỷ trước khi ghi xong). Khi đó không thể có order nào
       *   ở server: thử lại nhanh. Ngược lại (timeout sau khi đã gửi, socket
       *   rớt giữa chừng) thì order CÓ THỂ đã được nhận: không gửi lại mù —
       *   đánh dấu own của RIÊNG hàng này là mơ hồ; lượt thử lại sẽ chờ một
       *   lượt đọc đối soát đúng token đó trước khi dựng order mới.
       */
      if (error && error.notSent === true) {
        this.netStat("notSent", 1);
        this.log(`[SEND] SUBMIT NOT SENT NFT #${row.tokenId} ${msg} — thử lại nhanh`);
        this.scheduleRetry(key, 0, msg, "not-sent");
        return;
      }
      if (bookNow) bookNow.ownUnknownAt = Date.now();
      this.netStat("ambiguous", 1);
      this.log(`[SEND] SUBMIT AMBIGUOUS NFT #${row.tokenId} ${msg} — đối soát token trước khi gửi lại`);
      this.queueRead(row, { reason: "post-uncertain", authoritative: false, readAt: Date.now(), firstRead: false, attempt: 1 });
      this.scheduleRetry(key, 0, msg, "ambiguous");
      return;
    }
    this.lastPostAt = Date.now();

    // Header hạn mức là sự thật duy nhất về quota — báo lại cho MỌI cửa sổ.
    // 429 không kèm Retry-After vẫn phạt CHÍNH key đó tối thiểu 1s.
    const retryAfter = res.status === 429 ? (retryAfterMs(res.headers) || 1000) : 0;
    if (res.status === 429) { this.last429At = Date.now(); this.netStat("429", 1, quotaDomain); }
    if (this.quota) {
      this.quota.report({
        domain: quotaDomain,
        headers: res.headers,
        retryAfterMs: retryAfter,
        status: res.status,
        latencyMs: res.totalMs
      });
    }

    if (res.status >= 200 && res.status < 300) {
      const posted = readPostedOrder(res.body);
      // HTTP 2xx is the one-shot completion point even if this Flight was
      // superseded while in flight; failed/ambiguous responses keep the boost.
      if (this.epoch === submitEpoch && this.rows.has(row.key)) {
        this.firstPostPending.delete(row.key);
        this.firstPostCompleted.add(row.key);
      }
      if (!alive()) {
        // A wallet switch happened while this POST flew: the order is the OLD
        // wallet's and must never become the new wallet's Mine (1.25.12).
        if (this.epoch !== submitEpoch) {
          this.log(`[SEND] SUBMIT SUCCESS NFT #${row.tokenId} của ví cũ trả về sau khi đổi ví — bỏ qua, không ghi vào ví mới`);
          return;
        }
        // Watchdog đã thay lượt này trong lúc POST bay — nhưng OpenSea ĐÃ nhận
        // order: vẫn phải ghi own, nếu không lượt mới sẽ gửi chồng.
        this.recordOwnOrder(bookNow, posted.orderHash || signed.orderHash, target, durationMinutes,
          Number(signed.components && signed.components.endTime) || 0, first.correlationId || traceId);
        productionTrace.record("submit_success", { correlationId: first.correlationId || traceId }, {
          chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId,
          status: "SUCCESS", mine: target, max: row.maxPrice, target
        });
        this.log(`[SEND] SUBMIT SUCCESS NFT #${row.tokenId} ${target} WETH (lượt đã bị thay, vẫn ghi own)`);
        return;
      }
      M("committed");
      this.metrics.finish(traceId, posted.orderHash);
      this.lastResponseShape = posted.shape;
      row.lastError = "";
      row.lowBalance = false;
      row.serverLowBal = null;
      row.failures = 0;
      row.retryAt = 0;
      this.log(`[SEND] SUBMIT SUCCESS NFT #${row.tokenId} ${target} WETH ` +
        `orderHash=${posted.orderHash || "(phản hồi không kèm hash)"}`);
      // Order OpenSea vừa nhận là offer của mình — vào sổ ngay, không đợi
      // Stream vọng lại (xem recordOwnOrder). Hạn = endTime ĐÃ KÝ. Hash: của
      // phản hồi, hoặc hash Seaport của chính order đã ký khi phản hồi không
      // kèm - để một SELF REMOVE về sau khớp đúng order này (1.25.10).
      this.recordOwnOrder(bookNow, posted.orderHash || signed.orderHash, target, durationMinutes,
        Number(signed.components && signed.components.endTime) || 0, first.correlationId || traceId);
      productionTrace.record("submit_success", { correlationId: first.correlationId || traceId }, {
        chain: this.chain, collection: row.collectionSlug, tokenId: row.tokenId,
        status: "SUCCESS", best: bestNow, mine: target, max: row.maxPrice, target
      });
      /**
       * POST 2xx LÀ TERMINAL CHO Ý ĐỊNH NÀY (port 1.19.61)
       *
       *   Own đã vào sổ đồng bộ ở trên, nên DONE chỉ còn là một nhãn "đang xác
       *   nhận" giả chờ một nhịp dọn. IDLE ngay; khoá single-flight vẫn giữ
       *   tới finally. UI đọc từ sổ cục bộ ngay lượt đẩy kế tiếp.
       */
      //
      // A DECISION TAKEN WHILE THIS POST FLEW SURVIVES IT (1.25.12)
      //
      //   evaluate() during the flight writes a NEW intent (latest-wins), e.g.
      //   a competitor at exactly the price just posted → SEND +step. IDLE here
      //   used to land on THAT intent and erase it; the row then depended on
      //   the flight-settled wake, and a timer running before it (watchdog)
      //   met "[ORPHAN] cần gửi mà không có ý định" (soak 1.25.12: every wallet
      //   switch, the old wallet's live offer equal to the new target). The
      //   newer decision is re-derived right here from the book, which now
      //   holds the order just accepted.
      const newer = this.intents.get(key);
      const carried = newer && Number(newer.target) > 0 &&
        Math.abs(Number(newer.target) - Number(target)) >= 1e-12
        ? (Number.isFinite(newer.priorityRank) ? newer.priorityRank : 1) : null;
      this.setIntent(key, INTENT.IDLE, { target: 0, lastError: "" });
      this.stats.sendSuccess = (this.stats.sendSuccess || 0) + 1;
      this.netStat("sendSuccess", 1);
      if (row.renewPending) { row.renewPending = false; this.netStat("renewSuccess", 1); }
      if (carried !== null && row.running && this.state === STATE.RUNNING) {
        this.stats.decidedDuringPost = (this.stats.decidedDuringPost || 0) + 1;
        this.evaluate(key, Date.now(), null);
        const again = this.intents.get(key);
        if (again && Number(again.target) > 0) again.priorityRank = Math.min(Number.isFinite(again.priorityRank) ? again.priorityRank : 3, carried);
      }
      return;
    }
    if (!alive()) return;

    const bodyText = res.body && typeof res.body === "object"
      ? JSON.stringify(res.body) : String(res.body || "");
    const said = readErrorText(res.body);
    const lowBalance = opensea.isInsufficientBalanceError(bodyText);
    const why = lowBalance
      ? (said || "insufficient balance")
      : `HTTP ${res.status}${said ? ": " + said : ""}`;
    this.metrics.get(traceId)?.fail(why);
    this.metrics.finish(traceId);

    if (lowBalance) {
      this.setIntent(key, INTENT.FAILED, { lastError: why });
      row.lastError = why;
      row.lowBalance = true;
      // OpenSea's own refusal, counted per row: a second one while the raw
      // balance still looks enough starts a bounded back-off (applyBalance).
      {
        const prev = row.serverLowBal;
        row.serverLowBal = {
          count: (prev ? prev.count : 0) + 1,
          at: Date.now(),
          wei: prev && prev.wei != null ? prev.wei : (this.balance && this.balance.known ? this.balance.wethWei : null),
          target: Number(target)
        };
      }
      {
        const wei = BigInt(this.adapter.toWei(target));
        const cur = this.balanceCeiling;
        // balWei: the raw balance believed at the refusal - a later read showing
        // MORE than that means the wallet was funded since: the refusal is stale.
        this.balanceCeiling = { wei: cur && cur.wei < wei ? cur.wei : wei, at: Date.now(),
          balWei: cur ? cur.balWei : (this.balance && this.balance.known ? this.balance.wethWei : null) };
      }
      this.log(`[LOW BALANCE] #${row.tokenId} ${why}`);
      // Same recoverable lifecycle as the local pre-check: without this one
      // "insufficient balance" answer blocked the row until the next Start.
      // Verified with a fresh read now, not after a cooldown.
      this.refreshBalanceSoon(Date.now(), { urgent: true });
      return;
    }
    if (res.status === 429) {
      // 429 là của MỘT key: broker đã cooldown đúng key đó. Xin lại gần như
      // ngay — broker cấp key kia nếu nó rảnh, hoặc chờ đúng cooldown thật.
      this.log(`[SEND] SUBMIT DEFERRED NFT #${row.tokenId} ${why} (key ${quotaDomain}) — xin lại, key khác nếu rảnh`);
      this.scheduleRetry(key, 0, why, "quota");
      return;
    }
    if (res.status >= 500) {
      // 5xx: server đã trả lời — không có order (không phải trường hợp mơ hồ).
      this.log(`[SEND] SUBMIT DEFERRED NFT #${row.tokenId} ${why} — thử lại`);
      this.scheduleRetry(key, 0, why, "server");
      return;
    }
    // 4xx khác: OpenSea từ chối order này — không tự hồi được.
    this.setIntent(key, INTENT.FAILED, { lastError: why });
    row.lastError = why;
    this.log(`[SEND] SUBMIT FAILED NFT #${row.tokenId} ${why}`);
  }

  /**
   * Hẹn thử lại cho một token sau lỗi TẠM THỜI. Backoff 3s·2^n, trần 60s,
   * hoặc Retry-After của server nếu dài hơn. Lần thử lại đi qua `evaluate`
   * (tính lại từ sổ) rồi vào hàng chờ như mọi ý định khác — latest-wins.
   */
  scheduleRetry(key, retryAfterMs = 0, why = "", kind = "generic") {
    const row = this.rows.get(key);
    if (!row) return;
    row.failures = (row.failures || 0) + 1;
    const policy = RETRY_POLICY[kind] || RETRY_POLICY.generic;
    const exp = policy.base * Math.pow(2, Math.min(6, row.failures - 1));
    const jitter = kind === "server" || kind === "generic" ? Math.floor(Math.random() * 250) : 0;
    const backoff = Math.min(policy.max, exp) + jitter;
    const wait = Math.max(backoff, Number(retryAfterMs) || 0);
    this.netStat("retry:" + kind, 1);
    row.retryAt = Date.now() + wait;
    row.lastError = why ? `tạm thời: ${why}` : "";
    if (this.intents.get(key)) this.setIntent(key, INTENT.RETRY, { lastError: row.lastError });
    else this.changed();
    this.stats.retriesScheduled = (this.stats.retriesScheduled || 0) + 1;
  }

  /** Nhịp 1 giây: token nào tới giờ thử lại thì tính lại từ sổ và xếp hàng. */
  sweepRetries(now = Date.now()) {
    if (this.state !== STATE.RUNNING) return 0;
    let due = 0;
    for (const [key, row] of this.rows) {
      if (!row.retryAt || row.retryAt > now || !row.running) continue;
      if (this.intents.isInFlight(key)) continue;   // đang bay rồi thì không chen
      row.retryAt = 0;
      this.intents.clear(key);
      this.evaluate(key, now, null);
      due++;
    }
    if (due) this.pump();
    return due;
  }

  /**
   * WATCHDOG CHO BỘ ĐIỀU PHỐI — engine đang RUNNING thì phải tự chạy.
   *
   *   Không có nhịp quét nào ở đây; chỉ là lưới an toàn cho ba cách một
   *   token có thể chết trong im lặng:
   *     - in-flight kẹt quá lâu (một promise không bao giờ về): thả khoá và
   *       tính lại;
   *     - có ý định sẵn sàng, còn chỗ, mà không dispatch gì trong 30 giây:
   *       bơm lại;
   *     - hàng đang chờ lượt đọc đầu mà không có lượt đọc nào đang chạy hay
   *       được hẹn: xếp đọc lại.
   */
  watchdog(now = Date.now()) {
    if (this.state !== STATE.RUNNING) return;
    /**
     * LƯỢT BAY KẸT: bỏ lượt, KHÔNG bỏ token.
     *
     *   Đang chờ giấy phép → huỷ yêu cầu ở broker (gỡ khỏi hàng, không ăn
     *   nhịp) — lượt cũ tỉnh dậy thấy số hiệu khác thì rút; token về READY
     *   và xếp lại với đúng chỗ cũ. Đang POST → HTTP có hạn 20s riêng; quá
     *   180s là promise không bao giờ về: thay số hiệu, lượt cũ nếu về với
     *   2xx vẫn ghi own (xem submitOne) nhưng không đụng khoá của lượt mới.
     */
    for (const [key, since] of this.inFlightSince) {
      if (now - since <= INFLIGHT_STUCK_MS) continue;
      const f = this.flights.get(key);
      this.log(`[WATCHDOG] #${(this.rows.get(key) || {}).tokenId} kẹt in-flight ${Math.round((now - since) / 1000)}s ` +
        `(${f ? f.stage : "?"}) — bỏ lượt, xếp lại`);
      this.abandonFlight(key, "watchdog");
      this.stats.watchdogReleases = (this.stats.watchdogReleases || 0) + 1;
    }
    const room = MAX_INFLIGHT_SUBMITS - this.intents.census().inFlight;
    if (room > 0 && this.acquiring < MAX_ACQUIRING && this.intents.ready(now).length &&
        now - this.lastDispatchAt > DISPATCH_STALL_MS) {
      this.stats.watchdogPumps = (this.stats.watchdogPumps || 0) + 1;
      this.pump();
    }
    this.watchProgress(now);
    this.metrics.expire();   // open-trace TTL (1.25.12); stops at the first young trace

    /**
     * THỬ LẠI QUÁ HẠN MÀ CHƯA AI TÍNH LẠI: nhịp quét 1s có thể bị bỏ đói
     * (máy ngủ, event loop nghẽn) hoặc ý định RETRY mà mốc thử lại đã bị xoá.
     * Cả hai đều là "RETRY chết": tính lại ngay.
     */
    let overdue = 0;
    for (const [key, row] of this.rows) {
      if (!row.running || this.intents.isInFlight(key)) continue;
      const it = this.intents.get(key);
      if (!it || it.state !== INTENT.RETRY) continue;
      // Quá hạn THẬT: nhịp quét 1s có quyền chậm một nhịp; chỉ chữa khi đã
      // qua mốc hơn ba nhịp quét (hoặc mốc đã mất hẳn).
      if (row.retryAt && row.retryAt + 3 * RETRY_SWEEP_MS > now) continue;
      row.retryAt = 0;
      this.intents.clear(key);
      this.evaluate(key, now, null);
      overdue++;
    }
    if (overdue) {
      this.stats.watchdogRetryOverdue = (this.stats.watchdogRetryOverdue || 0) + overdue;
      this.log(`[WATCHDOG] ${overdue} hàng RETRY quá hạn mà chưa tính lại — tính lại`);
      this.pump();
    }

    // Lượt đọc còn nợ mà không có lượt nào đang chạy hay được hẹn (bị
    // `invalidate` bỏ rơi): xếp lại. Sổ nợ chỉ xoá khi đọc THÀNH CÔNG.
    for (const [key, pr] of this.pendingReads) {
      // Cùng lý do với watchdog cổng: dấu "đang đọc" bị bỏ quên không được
      // tính là một lượt đọc đang chạy, nếu không sổ nợ này không bao giờ
      // được xếp lại.
      if (this.hydrating.has(key) && !this.hydratingIsReal(key, now)) {
        this.clearHydrating(key);
        this.stats.stuckReadRepairs = (this.stats.stuckReadRepairs || 0) + 1;
      }
      if (this.hydrating.has(key) || pr.timer || this.firstReadTimers.has(key)) continue;
      const row = this.rows.get(key);
      if (!row || !row.running) { this.pendingReads.delete(key); continue; }
      this.stats.readRequeues = (this.stats.readRequeues || 0) + 1;
      this.queueRead(row, { reason: pr.reason, authoritative: pr.authoritative, readAt: now,
        firstRead: this.awaitingFirstRead.has(key), attempt: (pr.attempt || 1) + 1 });
    }
    /**
     * HÀNG MỒ CÔI: đang chạy, đã biết Best, đối thủ cao hơn mình, target
     * trong Max — mà không có ý định READY/RETRY/đang bay nào. Đây đúng là
     * trạng thái "Đang xử lý" ma mà không ai xử lý. Lưới an toàn (root cause
     * Add-khi-đang-chạy đã sửa ở addRows): tính lại và xếp hàng.
     */
    // Hết hạn theo đồng hồ là transition hợp lệ, không phải mồ côi: xử lý nó
    // TRƯỚC khi kiểm bất biến (hẹn giờ chính xác có thể trễ vài ms).
    this.sweepExpired(now);
    let orphans = 0;
    for (const [key, row] of this.rows) {
      if (!row.running || this.awaitingFirstRead.has(key) || this.intents.isInFlight(key)) continue;
      if (this.pendingWakes.has(key)) continue;
      // LOW_BALANCE deliberately has no SEND intent. Treating that as an
      // orphan made the watchdog recreate it every cycle and hammer quota.
      if (row.lowBalance) continue;
      const it = this.intents.get(key);
      if (it && it.state === INTENT.WAITING) {
        // Phụ thuộc cục bộ đang được chờ. Chỉ là airbag khi chờ quá lâu mà
        // không ai tính lại (mất đánh thức) — đếm riêng để thấy trong chẩn đoán.
        if (now - (it.waitingSince || it.stateChangedAt || now) > WAITING_STUCK_MS) {
          this.stats.watchdogWaitingRepairs = (this.stats.watchdogWaitingRepairs || 0) + 1;
          it.waitingSince = now;
          if (!this.templateReady(key)) this.ensureTemplate(key, "watchdog-waiting");
          else if (!this.ownAuthoritative(key)) this.deferForOwnSync(key, row, it.traceId, it.deferredReason || "own-state-unknown");
          else this.evaluate(key, now, null);
        }
        continue;
      }
      if (it && (it.state === INTENT.READY || it.state === INTENT.RETRY || it.state === INTENT.GRANTING ||
                 it.state === INTENT.BUILDING || it.state === INTENT.SENDING)) continue;
      if (row.retryAt && row.retryAt > now) continue;
      const book = this.book.get(key);
      if (!book || !(book.hydratedAt > 0 || book.lastEventAt > 0)) continue;
      if (row.sendBlockedBy?.gate === "stream-topic" && !this.topicReady(row, book)) {
        this.onTopicUnavailable(row.collectionSlug, row.sendBlockedBy.at || now, "watchdog-stream-topic");
        continue;
      }
      // Lỗi terminal (4xx, licence, build) trên CÙNG trạng thái sổ: chờ state
      // đổi thật hoặc user — không để watchdog biến nó thành vòng gửi lại.
      if (it && it.state === INTENT.FAILED && it.generation === book.generation) continue;
      const verdict = decide({ best: book.effectiveBest(now).price, mine: book.ownBest(now).price,
        minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step });
      if (verdict.status !== STATUS.SEND) continue;
      if (!this.topicReady(row, book)) {
        row.sendBlockedBy = { gate: "stream-topic", target: verdict.target, at: row.sendBlockedBy?.at || now };
        this.onTopicUnavailable(row.collectionSlug, row.sendBlockedBy.at, "watchdog-stream-topic");
        continue;
      }
      // Cần gửi mà thiếu template: chữa TEMPLATE, không xếp gửi mù.
      if (!this.templateReady(key)) { this.ensureTemplate(key, "watchdog"); continue; }
      orphans++;
      this.netStat("noIntent", 1);
      const lastOrphan = this.orphanLog.get(key) || 0;
      if (now - lastOrphan > 60000) {
        this.orphanLog.set(key, now);
        this.log(`[ORPHAN] #${row.tokenId} cần gửi (${verdict.target}) mà không có intent/flight/read/retry — ` +
          `intent=${it ? it.state : "none"} gen=${book.generation} retryAt=${row.retryAt || 0} ` +
          `gate=${this.awaitingFirstRead.has(key)} renew=${this.renewCheck.has(key)} read=${this.pendingReads.has(key)}`);
      }
      this.ensureRowProgress(key, "watchdog-orphan");
    }
    // Template hẹn dựng lại mà timer/promise đã mất: dựng lại.
    for (const [key, ts] of this.templateState) {
      if (ts.state === "ready" || ts.promise || ts.timer) continue;
      if (!this.rows.has(key)) { this.templateState.delete(key); continue; }
      if (ts.retryAt && ts.retryAt > now) continue;
      this.stats.watchdogTemplate = (this.stats.watchdogTemplate || 0) + 1;
      this.ensureTemplate(key, "watchdog-lost");
    }
    if (orphans) {
      this.stats.watchdogOrphans = (this.stats.watchdogOrphans || 0) + orphans;
      // Đường chạy bình thường KHÔNG được đi qua đây: `evaluate` đã ghi ô ý
      // định ngay khi quyết định là GỬI. Mỗi dòng ở đây là một lỗ hổng trạng
      // thái, nên nó nói rõ hàng nào và đang ở đâu.
      this.log(`[WATCHDOG] ${orphans} hàng cần gửi mà không có ý định — tính lại ` +
        `(tổng ${this.stats.watchdogOrphans}; ý định mất khi ghi: ${this.stats.intentLost || 0})`);
      this.pump();
    }

    // Own order quá hạn quá dung sai mà vẫn nằm trong sổ: nhịp dọn đã bỏ lỡ
    // (timer bị bỏ đói / ngủ) — dọn và tính lại ngay.
    const sec = Math.floor(now / 1000);
    let staleOwn = 0;
    for (const [key, book] of this.book.books) {
      for (const o of book.own.values()) {
        if (o.endTime > 0 && o.endTime + OWN_EXPIRY_TOLERANCE_S <= sec) { staleOwn++; break; }
      }
      if (staleOwn) { this.stats.watchdogOwnExpiry = (this.stats.watchdogOwnExpiry || 0) + 1; this.sweepExpired(now); break; }
    }
    this.enforceFirstReadInvariant(now);
  }


  // ================================================================
  // Template tĩnh: sẵn sàng / dựng lại (single-flight, backoff)
  // ================================================================

  /** Template của token có trong builder không (kiểm cache thật, không tin cờ). */
  templateReady(key) {
    return Boolean(this.builder && this.builder.get(key));
  }

  /** Dựng template cho MỘT hàng từ cấu hình đã có; ném khi không dựng được. */
  applyTemplate(row, config, account, collection) {
    this.builder.hydrate(row.key, {
      chain: this.chain,
      chainId: config.chainId,
      wethAddress: config.wethAddress,
      conduitKey: config.conduitKey,
      requiresSignedZone: collection.requiresSignedZone,
      signedZone: config.signedZone,
      fees: collection.fees,
      counter: account.counter,
      tokenStandard: collection.tokenStandard,
      contract: row.contract,
      tokenId: row.tokenId,
      hydratedAt: Date.now()
    });
    if (!this.builder.get(row.key)) throw new Error("template không vào cache");
    const ts = this.templateState.get(row.key);
    if (ts && ts.timer) clearTimeout(ts.timer);
    this.templateState.set(row.key, { state: "ready", attempts: 0, retryAt: 0, timer: null, lastError: "", promise: null });
    if (row.lastError && /^template:/.test(row.lastError)) row.lastError = "";
  }

  /** Ghi nhận dựng hỏng và hẹn dựng lại theo bậc; không xin lượt ghi. */
  noteTemplateFailure(row, why = "") {
    const key = row.key;
    const prev = this.templateState.get(key) || { attempts: 0 };
    const attempts = (prev.attempts || 0) + 1;
    const wait = TEMPLATE_RETRY_MS[Math.min(TEMPLATE_RETRY_MS.length - 1, attempts - 1)];
    if (prev.timer) clearTimeout(prev.timer);
    const ts = { state: "retry", attempts, retryAt: Date.now() + wait, timer: null, lastError: String(why || "").slice(0, 140), promise: null };
    const epoch = this.epoch;
    ts.timer = setTimeout(() => {
      ts.timer = null;
      if (this.epoch !== epoch || this.state !== STATE.RUNNING) return;
      this.ensureTemplate(key, "retry");
    }, wait);
    if (ts.timer.unref) ts.timer.unref();
    this.templateState.set(key, ts);
    row.lastError = `template: ${ts.lastError}`;
    this.stats.templateFailures = (this.stats.templateFailures || 0) + 1;
    if (attempts <= 3 || attempts % 10 === 0) {
      this.log(`[TEMPLATE] #${row.tokenId} chưa dựng được template (${ts.lastError}) — dựng lại sau ${wait / 1000}s (lượt ${attempts}); không xin lượt ghi`);
    }
    this.changed();
  }

  /**
   * Bảo đảm template của token tồn tại: có rồi → true; đang dựng → false;
   * chưa → dựng ở nền (single-flight), xong thì tính lại hàng từ sổ mới nhất.
   */
  ensureTemplate(key, why = "") {
    if (this.templateReady(key)) return true;
    if (this.state !== STATE.RUNNING || !this.builder) return false;
    const row = this.rows.get(key);
    if (!row) return false;
    const ts = this.templateState.get(key);
    if (ts && ts.promise) return false;                         // đang dựng
    if (ts && ts.timer && ts.retryAt > Date.now()) return false; // đã hẹn
    const epoch = this.epoch;
    const state = ts || { attempts: 0 };
    state.state = "hydrating"; state.timer = null;
    state.promise = (async () => {
      const config = this.chainCfg || (this.chainCfg = await this.adapter.chainConfig());
      // Wallet-fenced (1.25.12): an account read begun under wallet A must
      // never be written back once wallet B is active (epoch changed), and a
      // cached account is used only if it belongs to the signer in use.
      const signerAddress = String(this.builder && this.builder.address || "").toLowerCase();
      let account = this.accountCfg;
      if (!account || (account.address && String(account.address).toLowerCase() !== signerAddress)) {
        account = await this.adapter.accountState(signerAddress);
        if (this.epoch !== epoch) return;
        this.accountCfg = account;
      }
      const collection = await this.adapter.collectionConfig(row.collectionSlug, row.contract, { tokenId: row.tokenId });
      if (this.epoch !== epoch) return;
      /**
       * SLUG GIẢI ĐƯỢC THÌ GHI NGƯỢC VÀO HÀNG
       *
       *   Không ghi lại thì mỗi lượt dựng template sau này lại đi giải slug
       *   từ đầu, và — nặng hơn — hàng vẫn vô hình với đường đọc Collection
       *   Offer, vốn gom theo `row.collectionSlug`: một NFT không có slug
       *   không bao giờ nhận được Collection Offer của chính collection nó.
       */
      if (!row.collectionSlug && collection && collection.slug) {
        row.collectionSlug = collection.slug;
        this.log(`#${row.tokenId} đã xác định collection: ${collection.slug}`);
      }
      this.applyTemplate(row, config, account, collection);
    })().then(() => {
      if (this.epoch !== epoch || this.state !== STATE.RUNNING) return;
      this.stats.templateHydrated = (this.stats.templateHydrated || 0) + 1;
      this.log(`[TEMPLATE] #${row.tokenId} template sẵn sàng (${why}) — tính lại`);
      if (row.running) { this.evaluate(key, Date.now(), null); this.pump(); }
      this.changed();
    }).catch(error => {
      if (this.epoch !== epoch) return;
      this.noteTemplateFailure(row, String(error && error.message || error));
    }).finally(() => {
      const cur = this.templateState.get(key);
      if (cur) cur.promise = null;
    });
    this.templateState.set(key, state);
    return false;
  }

  /**
   * Bỏ lượt bay hiện tại của một token (không bỏ token): huỷ yêu cầu ở
   * broker, thay số hiệu để lượt cũ rút lui, nhả khoá, tính lại và xếp lại.
   */
  abandonFlight(key, why = "abandon") {
    const f = this.flights.get(key);
    if (f && f.handle && typeof f.handle.cancel === "function") {
      try { f.handle.cancel(why); } catch { /* đã trả lời */ }
    }
    try { f?.controller?.abort(why); } catch { /* request đã settle */ }
    this.flights.delete(key);
    this.intents.release(key);
    this.inFlightSince.delete(key);
    const it = this.intents.get(key);
    if (it && (it.state === INTENT.GRANTING || it.state === INTENT.BUILDING || it.state === INTENT.SENDING)) {
      it.requeues = (it.requeues || 0) + 1;
      this.setIntent(key, INTENT.READY, { lastError: `${why}: xếp lại` });
    }
    const row = this.rows.get(key);
    if (row && row.running && this.epoch && this.state === STATE.RUNNING) {
      this.evaluate(key, Date.now(), null);
      setImmediate(() => this.pump());
    }
  }

  /**
   * WATCHDOG TIẾN TRIỂN — phân biệt CHỜ HỢP LỆ với TREO.
   *
   *   Có việc đủ điều kiện (READY có target, hoặc đang chờ giấy phép) là bình
   *   thường khi nhiều NFT cùng chờ cooldown hợp lệ. Nhưng khi đó máy này phải
   *   thấy tiến triển đều: một giấy phép (của bất kỳ ai — broker ghi nhận),
   *   một POST xong, hay ít nhất leader còn nói chuyện. Không thấy gì quá
   *   ngưỡng (cơ sở + cooldown hợp lệ của broker) là treo: chữa ở tầng bộ
   *   điều phối / broker, KHÔNG quét REST — Best đã biết rồi.
   */
  watchProgress(now = Date.now()) {
    let eligible = 0, granting = 0, oldestQueuedAt = 0;
    for (const it of this.intents.intents.values()) {
      const row = this.rows.get(it.tokenKey);
      if (!row || !row.running || !it.target) continue;
      if (it.state === INTENT.READY || it.state === INTENT.GRANTING) {
        eligible++;
        if (it.state === INTENT.GRANTING) granting++;
        const q = it.queuedAt || it.firstSeenAt || 0;
        if (q && (!oldestQueuedAt || q < oldestQueuedAt)) oldestQueuedAt = q;
      }
    }
    if (!eligible) return;

    let q = null;
    try { q = this.quota ? this.quota.status() : null; } catch { q = null; }
    const model = q && q.model ? q.model : null;
    const cooldown = model ? Math.max(Number(model.nextInMs) || 0, Number(model.blockedForMs) || 0) : 0;
    const threshold = PROGRESS_STALL_MS + cooldown;
    const brokerProgress = q ? Math.max(Number(q.lastGrantAt) || 0, Number(q.lastLeaderMessageAt) || 0) : 0;
    // "Không tiến triển" chỉ có nghĩa khi việc chờ đã chờ đủ lâu: một ý định
    // vừa xuất hiện sau nhiều phút yên tĩnh chưa phải là treo.
    const lastProgress = Math.max(this.lastGrantAt, this.lastPostAt, brokerProgress, this.lastRepairAt, this.runningSince, oldestQueuedAt);
    const idle = now - lastProgress;
    if (idle <= threshold) return;
    // Có việc chờ nhưng chưa ai xin giấy phép (bơm chết) — cũng là treo.
    this.lastRepairAt = now;
    this.stats.watchdogStalls = (this.stats.watchdogStalls || 0) + 1;
    const waited = oldestQueuedAt ? Math.round((now - oldestQueuedAt) / 1000) : 0;
    this.log(`[WATCHDOG] bộ điều phối không tiến triển ${Math.round(idle / 1000)}s (ngưỡng ${Math.round(threshold / 1000)}s) · ` +
      `chờ=${eligible} (đang xin lượt ${granting}) · chờ lâu nhất ${waited}s · in-flight=${this.intents.census().inFlight} · ` +
      `broker=${q ? `${q.role}${q.connected ? "" : "/mất kết nối"} hàng=${q.queued}` : "không có"} — tự chữa`);
    // 1 · broker: nhịp bơm / ống / bầu lại.
    if (this.quota && typeof this.quota.repair === "function") {
      try { this.log(`[WATCHDOG] broker: ${this.quota.repair("engine-no-progress")}`); } catch (e) { this.log(`[WATCHDOG] broker repair lỗi: ${e.message}`); }
    }
    // 2 · lượt đang chờ giấy phép quá ngưỡng: huỷ và xếp lại (giữ chỗ) —
    //     một yêu cầu đang treo ở broker cũ không thể tự về.
    for (const [key, f] of [...this.flights]) {
      if (f.stage === "granting" && now - f.since > threshold) this.abandonFlight(key, "no-progress");
    }
    // 3 · bơm lại theo trạng thái mới nhất.
    this.lastDispatchAt = 0;
    this.pump();
  }

  /**
   * BẬT LẠI QUYỀN GHI CHO NHỮNG HÀNG ĐANG TẠM DỪNG — KHÔNG START LẠI
   *
   *   Tạm dừng chỉ tắt quyền ghi; sổ, own, template, trait, Stream vẫn sống
   *   và vẫn được cập nhật. Vậy Bắt đầu lại = bật `running`, tính lại từ sổ
   *   hiện tại, và CHỈ đọc lại những hàng mà sổ đã cũ (hoặc có khoảng mù
   *   Stream sau lượt đọc cuối). Không prewarm lại, không nạp lại trait,
   *   không đọc lại criteria trong TTL, không chờ hàng cuối cùng.
   *
   * @returns {{ok:boolean, resumed:number, refreshed:number, tracked:number}}
   */
  resumeRows(rows = null) {
    if (this.state !== STATE.RUNNING || !this.builder) {
      return { ok: false, error: "engine chưa chạy — dùng start()" };
    }
    const now = Date.now();
    const keys = rows && rows.length
      ? rows.map(r => tokenKey(this.chain, r.contract, r.tokenId)).filter(k => this.rows.has(k))
      : [...this.rows.keys()];
    let resumed = 0, refreshed = 0;
    const slugs = new Set();
    for (const key of keys) {
      const row = this.rows.get(key);
      if (!row) continue;
      // Cấu hình có thể đã đổi trong lúc dừng (Min/Max/Step/Duration/ưu tiên).
      const patch = rows && rows.find(r => tokenKey(this.chain, r.contract, r.tokenId) === key);
      if (patch) this.patchRow(key, patch);
      row.running = true;
      row.manualPriorityAt = now;
      row.retryAt = 0;
      resumed++;
      const book = this.book.get(key);
      /**
       * SỔ CÒN ẤM = ĐÃ TỪNG BIẾT, VÀ CÓ TIN SAU KHOẢNG MÙ GẦN NHẤT
       *
       *   Đã từng đọc (`hydratedAt`) là điều kiện cần: chưa đọc lần nào thì
       *   chưa biết Best. Và phải có TIN sau khoảng mù Stream gần nhất — một
       *   lượt đọc, hoặc một sự kiện Stream của chính token đó. Đòi mỗi hàng
       *   một lượt đọc sau mỗi lần reconnect ngắn chính là cơn đọc hàng loạt
       *   mà bản này tồn tại để bỏ.
       */
      const lastKnownAt = book ? Math.max(book.lastFullReadAt || 0, book.hydratedAt || 0, book.lastEventAt || 0) : 0;
      const fresh = book && book.hydratedAt > 0 &&
        lastKnownAt >= this.lastStreamGapAt && now - lastKnownAt < RESUME_FRESH_MS;
      if (this.downwardAuthority.has(key) && typeof this.adapter.fetchBest === "function") {
        const pending = this.pendingReads.get(key);
        if (!this.hydrating.has(key) && !(pending && pending.timer)) {
          this.queueRead(row, { reason: "gap", authoritative: false,
            readAt: now, firstRead: false, attempt: 1 });
        }
      }
      if (!fresh && typeof this.adapter.fetchBest === "function") {
        // Sổ đã cũ: đọc lại ĐÚNG hàng đó, ở nền, không chặn hàng khác. Cổng
        // lượt-đọc-đầu chỉ đóng cho hàng này (không gửi khi chưa biết Best).
        this.awaitingFirstRead.add(key);
        if (this.queueRead(row, { reason: "resume", authoritative: false, readAt: now, firstRead: true, attempt: 1 })) {
          refreshed++; this.readStats.resumeReads++;
        }
        if (row.collectionSlug) slugs.add(row.collectionSlug);
      } else {
        this.evaluate(key, now, null);
      }
      if (!this.templateReady(key)) this.ensureTemplate(key, "resume");
    }
    // Criteria chỉ đọc lại cho collection có hàng đã cũ, và chỉ ngoài TTL.
    if (typeof this.adapter.fetchCollectionOffersFor === "function") {
      for (const slug of slugs) this.queueCollectionSeed(slug, "resume");
    }
    this.pump();
    this.changed();
    this.log(`RESUME · ${resumed} NFT chạy lại · ${refreshed} cần đọc lại (${resumed - refreshed} dùng sổ đang ấm) · ` +
      `đọc criteria ${slugs.size} collection`);
    return { ok: true, resumed, refreshed, tracked: this.rows.size };
  }

  /**
   * Sổ của token này có đủ thẩm quyền để đặt lại offer không?
   *
   *   Đủ = cả danh sách đã được đọc trong RENEW_FRESH_MS, SAU khoảng mù Stream
   *   gần nhất, không còn nợ đọc, Stream đang khoẻ. Thiếu một điều là "chưa
   *   biết" — và chưa biết KHÔNG phải "không có Item Offer".
   */
  renewAuthorityFresh(key, book, now = Date.now()) {
    const full = Number(book.lastFullReadAt) || 0;
    if (!full) return false;
    if (full <= this.lastStreamGapAt) return false;
    /**
     * ĐỐI THỦ ĐÃ BIẾT = STREAM-FIRST (port 1.19.61)
     *
     *   Cả danh sách đã đọc SAU khoảng mù gần nhất và Stream giữ sổ liên tục
     *   từ đó: đối thủ trong sổ là tin mới nhất — TTL không áp. TTL chỉ quan
     *   trọng khi sổ TRỐNG và bot sắp đặt Min.
     */
    const knownCompetitor = book.effectiveBest(now).price > 0;
    if (!knownCompetitor && now - full > RENEW_FRESH_MS) return false;
    if (this.pendingReads.has(key) || this.awaitingFirstRead.has(key)) return false;
    if (this.degraded) return false;
    let health = "UNKNOWN";
    try { health = this.streamHealth ? String(this.streamHealth(this.rows.get(key)?.collectionSlug)) : "UNKNOWN"; } catch { health = "UNKNOWN"; }
    if (health === "FAILED" || health === "DISCONNECTED" || health === "RECONNECTING" || health === "STALE") return false;
    return true;
  }

  /**
   * Own vừa hết hạn mà sổ không đủ thẩm quyền: đóng cổng gửi, quên ý định
   * cũ, đọc CẢ DANH SÁCH của token (không phải `/best`), rồi mergeBest mở
   * cổng và tính lại từ Item + Collection + Trait đã xác nhận.
   * @returns {boolean} true khi đã giữ lại (không tính giá ngay)
   */
  /**
   * OWN HẾT HẠN / MẤT, SỔ CÓ ĐỐI THỦ SỐNG → GỬI LẠI NGAY TỪ SỔ (port 1.19.61)
   *
   *   Không bắt resync ví, REST đầy đủ hay TTL thẩm quyền đứng trước POST.
   *   Thiếu thẩm quyền đầy đủ (khoảng mù/suy giảm) thì xếp một lượt đọc đối
   *   chiếu ở NỀN, không chặn gửi. Chỉ sổ TRỐNG mới giữ lại chờ đọc.
   * @returns {boolean} true khi gửi được ngay từ sổ
   */
  renewFromBook(key, book, now = Date.now()) {
    if (!(book.effectiveBest(now).price > 0)) return false;
    if (!this.renewAuthorityFresh(key, book, now)) {
      const row = this.rows.get(key);
      if (row && row.running && !this.pendingReads.has(key) && typeof this.adapter.fetchBest === "function") {
        this.queueRead(row, { reason: "authority", authoritative: false, readAt: now, firstRead: false, attempt: 1 });
      }
    }
    this.stats.renewFromBook = (this.stats.renewFromBook || 0) + 1;
    return true;
  }

  /** Đóng cổng gửi-khi-sổ-trống của MỘT hàng tại mốc `at` (fencing). */
  closeGate(key, at = Date.now()) {
    const closedAt = Number(at) > 0 ? Number(at) : Date.now();
    this.awaitingFirstRead.add(key);
    this.gateClosedAt.set(key, Math.max(this.gateClosedAt.get(key) || 0, closedAt));
    return closedAt;
  }

  holdRenewal(key, row, book, now = Date.now()) {
    if (this.renewFromBook(key, book, now)) return false;
    if (this.renewAuthorityFresh(key, book, now)) return false;
    if (typeof this.adapter.fetchBest !== "function") return false;
    const c = book.candidates(now);
    this.closeGate(key, now);
    this.renewCheck.add(key);
    if (!this.renewSince.has(key)) this.renewSince.set(key, { at: now, warned: false });
    this.intents.clear(key);
    this.stats.renewChecks = (this.stats.renewChecks || 0) + 1;
    this.log(`#${row.tokenId} offer của mình đã hết hạn — sổ chưa đủ thẩm quyền (item=${c.item.price || "none"} ` +
      `collection=${c.collection.price || "none"} trait=${c.trait.price || "none"}; đọc đầy đủ cuối ` +
      `${book.lastFullReadAt ? Math.round((now - book.lastFullReadAt) / 1000) + "s trước" : "chưa có"}) — xác nhận lại Best trước khi đặt lại`);
    if (!this.hydrating.has(key)) {
      this.queueRead(row, { reason: "renew", authoritative: false, readAt: now, firstRead: false, attempt: 1 });
    }
    return true;
  }

  /**
   * ĐỌC NHẤT QUÁN CHO MỘT COLLECTION — khi kênh Stream của nó im lặng bất
   * thường / vừa rejoin. Một `/best` cho mỗi token của collection, qua sổ nợ
   * (đọc lại tới khi thành công), nguội 10 phút mỗi collection. Không phải
   * nhịp quét: chỉ chạy khi Stream tự khai là có thể đã bỏ sót.
   */
  /**
   * ORDER REVALIDATED (1.25.14): OpenSea says an order is valid again but the
   * event has no price. The smallest targeted repair: re-read the token it
   * names (or, for a collection/trait order with no token, that collection).
   * A background P2 read; it never gates SEND (WARM stays sticky, §3A).
   */
  onOrderRevalidate(op, at = Date.now()) {
    if (this.state !== STATE.RUNNING || typeof this.adapter.fetchBest !== "function") return 0;
    this.stats.revalidateEvents = (this.stats.revalidateEvents || 0) + 1;
    if (op.contract && op.tokenId) {
      const key = this.book.byNft && this.book.byNft.get(`${op.contract}:${op.tokenId}`);
      const row = key ? this.rows.get(key) : null;
      if (!row || !row.running) return 0;
      if (this.hydrating.has(key) || this.pendingReads.has(key)) return 0;
      this.queueRead(row, { reason: "revalidate", authoritative: false, readAt: at, firstRead: false, attempt: 1 });
      return 1;
    }
    return op.collectionSlug ? this.consistencyRead(op.collectionSlug, "revalidate") : 0;
  }

  /**
   * THE FIRST SUBSCRIPTION CAN COME AFTER THE FIRST READ (1.25.12)
   *
   *   A new NFT of a new collection: its REST first read and the topic's
   *   phx_join race. When the read completes first, events between the read's
   *   snapshot and the join ACK are simply never delivered - and a FIRST join
   *   is not a "gap", so nothing re-checked: the row stayed silent until some
   *   later event (found by the 1.25.12 real-socket soak). Only rows whose read
   *   completed before the ACK (+1s: a read that returned just after may carry
   *   an older snapshot) get one targeted read. At app start the Stream is
   *   joined before rows are read, so this re-reads nothing there.
   */
  firstJoinRecheck(slug, joinedAt = Date.now()) {
    if (this.state !== STATE.RUNNING || !slug) return 0;
    if (typeof this.adapter.fetchBest !== "function") return 0;
    const s = String(slug).toLowerCase();
    const eligible = [];
    for (const [key, row] of this.rows) {
      if (row.collectionSlug !== s || !row.running) continue;
      const book = this.book.get(key);
      const pending = this.pendingReads.get(key);
      const readBeforeJoin = book && book.hydratedAt > 0 &&
        Number(book.lastFullReadAt || book.hydratedAt) <= joinedAt;
      const inFlightBeforeJoin = pending?.startedAt > 0 && pending.startedAt <= joinedAt;
      if (readBeforeJoin || inFlightBeforeJoin) {
        this.topicRepairAt.set(key, joinedAt);
        if (!this.hydrating.has(key) && !pending) eligible.push(row);
      } else if (book && book.lastFullReadAt > joinedAt) {
        // A first read that started after the ACK already covers this topic.
        this.evaluate(key, Date.now(), null);
      }
    }
    const queued = this.scheduleRecoveryRows(eligible, "first-join");
    if (this.topicRepairAt.size) this.changed();
    if (queued) {
      this.stats.firstJoinReads = (this.stats.firstJoinReads || 0) + queued;
      this.log(`[TOPIC_JOINED] ${s}: ${queued} NFT đã đọc trước khi join xong — đọc lại một lần`);
    }
    return queued;
  }

  consistencyRead(slug, reason = "consistency") {
    if (this.state !== STATE.RUNNING || !slug) return 0;
    if (typeof this.adapter.fetchBest !== "function") return 0;
    const s = String(slug).toLowerCase();
    const now = Date.now();
    if (reason !== "topic-gap" &&
        now - (this.slugReadAt.get(s) || 0) < CONSISTENCY_READ_COOLDOWN_MS) return 0;
    if (reason === "topic-gap") {
      for (const [key, row] of this.rows) {
        if (row.collectionSlug === s && row.running) this.topicRepairAt.set(key, now);
      }
    }
    const eligible = [...this.rows.values()].filter(row => row.collectionSlug === s && row.running &&
      !this.hydrating.has(row.key) && !this.pendingReads.has(row.key));
    const queued = this.scheduleRecoveryRows(eligible, reason);
    if (reason === "topic-gap") this.changed();
    if (queued) {
      this.slugReadAt.set(s, now);
      this.stats.consistencyReads = (this.stats.consistencyReads || 0) + queued;
      if (reason === "topic-gap") this.stats.topicGapRecoveryRows = (this.stats.topicGapRecoveryRows || 0) + queued;
      this.log(`đọc nhất quán ${s} (${reason}): ${queued} NFT`);
    }
    return queued;
  }

  /** Start one scoped full authority recovery as soon as a joined topic drops. */
  onTopicUnavailable(slug, unavailableAt = Date.now(), reason = "topic-unavailable") {
    if (this.state !== STATE.RUNNING || !slug || typeof this.adapter.fetchBest !== "function") return 0;
    const s = String(slug).toLowerCase();
    const matching = [...this.rows.values()].filter(row => row.collectionSlug === s && row.running);
    if (!matching.length) return 0;
    const at = Number(unavailableAt) || Date.now();
    let newOutage = false;
    for (const row of matching) if (!this.topicRepairAt.has(row.key)) {
      this.topicRepairAt.set(row.key, at);
      newOutage = true;
    }
    const eligible = matching.filter(row => {
      const repairAt = this.topicRepairAt.get(row.key) || at;
      const book = this.book.get(row.key);
      const snapshotFresh = Number(book?.lastFullReadAt) > repairAt;
      return !snapshotFresh && !this.hydrating.has(row.key) && !this.pendingReads.has(row.key) &&
        !this.recoveryCandidates.has(row.key);
    });
    if (!eligible.length) {
      if (newOutage) this.stats.topicGapOutages = (this.stats.topicGapOutages || 0) + 1;
      if (newOutage) this.changed();
      return 0;
    }
    const queued = this.scheduleRecoveryRows(eligible, "topic-gap", { authoritative: false });
    if (newOutage) this.stats.topicGapOutages = (this.stats.topicGapOutages || 0) + 1;
    if (queued) this.stats.topicGapRecoveryRows = (this.stats.topicGapRecoveryRows || 0) + queued;
    this.changed();
    this.log(`topic ${s} unavailable (${String(reason).slice(0, 80)}): targeted recovery ${queued}/${eligible.length}`);
    return queued;
  }

  /** Rejoin closes the outage. Re-read only if no full snapshot covered it. */
  onTopicGap(slug) {
    if (this.state !== STATE.RUNNING || !slug) return 0;
    const s = String(slug).toLowerCase();
    const now = Date.now();
    const rows = [...this.rows.values()].filter(row => row.collectionSlug === s && row.running);
    const uncovered = [];
    for (const row of rows) {
      const repairAt = this.topicRepairAt.get(row.key) || 0;
      const book = this.book.get(row.key);
      if (repairAt && Number(book?.lastFullReadAt) > repairAt) {
        this.topicRepairAt.delete(row.key);
        this.evaluate(row.key, now, null);
      } else {
        this.topicRepairAt.set(row.key, now);
        if (!this.hydrating.has(row.key) && !this.pendingReads.has(row.key)) uncovered.push(row);
      }
    }
    const queued = this.scheduleRecoveryRows(uncovered, "topic-gap", { authoritative: false });
    if (queued) this.stats.topicGapRecoveryRows = (this.stats.topicGapRecoveryRows || 0) + queued;
    if (rows.length) this.changed();
    return queued;
  }

  // ================================================================
  // Đệm và đồng bộ
  // ================================================================

  flushBuffer() {
    const events = this.buffer.slice();
    this.buffer.length = 0;
    this.buffering = false;
    // Theo đúng thứ tự nhận. Sổ tự lo chuyện lệch thứ tự bằng seq của nó.
    for (const event of events) this.apply(event);
    return events.length;
  }

  /**
   * Đọc trạng thái hiện tại của mọi NFT một lần, qua REST, ở MẶT PHẲNG NỀN.
   *
   *   Stream chỉ kể những gì xảy ra từ lúc subscribe. Một offer đặt trước đó
   *   rồi im lặng không sinh sự kiện nào, nên nếu không đọc một lần thì engine
   *   khởi động với sổ rỗng và tưởng không ai cạnh tranh.
   *
   *   Chạy qua RecoveryPlane nên nó có bộ giới hạn đọc riêng, generation
   *   riêng, và KHÔNG BAO GIỜ chặn đường nóng: một sự kiện Stream tới trong
   *   lúc việc này còn chạy vẫn được xử lý ngay, và kết quả REST về sau sẽ
   *   không ghi đè nó — xem `mergeBest`.
   */
  hydrateBook(reason = "startup") {
    if (typeof this.adapter.fetchBest !== "function") return 0;
    /**
     * KHI STREAM CHẾT, REST LÀ NGUỒN DUY NHẤT — VÀ NÓ PHẢI ĐƯỢC PHÉP SỬA SỔ
     *
     *   Lúc Start, Stream đang chạy song song và mới hơn theo định nghĩa, nên
     *   kết quả REST chỉ được BỔ SUNG chỗ trống. Nhưng trong chế độ suy giảm
     *   không có Stream nào cả: nếu REST vẫn chỉ được bổ sung, một offer đã
     *   bị huỷ hoặc đã bị thay bằng giá khác sẽ nằm mãi trong sổ, và engine
     *   đọc lại đúng nhịp mà vẫn tin vào một thị trường đã không còn.
     *
     *   Đo được trên bản đóng gói không Stream: server đổi giá 0.01 → 0.02,
     *   engine đọc lại đủ ba lần, và Best trong app vẫn 0.01.
     *
     *   `readAt` là mốc để phân xử: sự kiện Stream nào tới SAU mốc đó vẫn
     *   thắng — kể cả khi Stream vừa sống lại đúng lúc REST đang bay.
     */
    const authoritative = reason === "stream-degraded";
    const firstRead = reason === "startup";
    const readAt = Date.now();
    let queued = 0;
    // Một lượt đọc theo COLLECTION cho mọi NFT cùng collection — trước các
    // lượt đọc từng token, để sổ có sẵn collection/trait offer khi token
    // được tính. Không phải điều kiện của cổng lượt-đọc-đầu.
    if (typeof this.adapter.fetchCollectionOffersFor === "function") {
      const slugs = new Set();
      for (const row of this.rows.values()) if (row.collectionSlug) slugs.add(row.collectionSlug);
      for (const slug of slugs) { this.queueCollectionSeed(slug, reason); queued++; }
    }
    for (const row of this.rows.values()) {
      const book = this.book.get(row.key);
      /**
       * CHỈ BỎ QUA THỨ VỪA ĐƯỢC ĐỌC, KHÔNG PHẢI THỨ TỪNG ĐƯỢC ĐỌC
       *
       *   Điều kiện cũ là "sổ đã từng có một lượt đọc đầy đủ". Nó đúng cho
       *   cảnh nó được viết ra (Add kèm snapshot, ngay lúc Start) và sai cho
       *   mọi lần `hydrateBook` chạy lại sau đó — nối lại Stream, chế độ suy
       *   giảm, đọc lại thủ công — vì lúc ấy sổ luôn "đã từng" được đọc, nên
       *   lượt đọc mới bị bỏ và REST không còn sửa được sổ nữa.
       *
       *   Thứ cần hỏi là "hàng này có phải vừa được seed trong lần Start này
       *   không", và đó là một câu hỏi có câu trả lời chính xác.
       */
      if (firstRead && this.seededRows.has(row.key) && book && book.lastFullReadAt > 0 &&
          !this.awaitingFirstRead.has(row.key)) {
        this.seededRows.delete(row.key);            // dùng một lần
        this.evaluate(row.key, readAt, null);
        continue;                                   // snapshot của đường Add đã đủ
      }
      this.queueRead(row, { reason, authoritative, readAt, firstRead, attempt: 1 });
      queued++;
    }
    return queued;
  }

  /**
   * Đọc criteria offer của một collection MỘT lần và fan-out cục bộ.
   *
   *   13 NFT cùng collection từng đọc 13 × 2 trang cùng 55 collection offer.
   *   Giờ là một lượt cho collection + một request nhỏ (`/best`) cho mỗi
   *   token. Kết quả chỉ BỔ SUNG sổ (không ghi đè, không hồi sinh bia mộ).
   */
  queueCollectionSeed(slug, reason = "startup", { force = false } = {}) {
    if (!slug) return false;
    /**
     * MỘT COLLECTION, MỘT LƯỢT ĐỌC
     *
     *   80 NFT trên chục collection: reconnect/resume/Add có thể cùng lúc đòi
     *   criteria của cùng một collection. Đọc một lần, mọi NFT của collection
     *   đó dùng chung kết quả; trong TTL thì không đọc lại (Stream vẫn cập
     *   nhật collection offer theo thời gian thực).
     */
    const now = Date.now();
    if (this.slugSeedInFlight.has(slug)) { this.readStats.collectionDeduped++; return false; }
    if (!force && now - (this.slugSeedAt.get(slug) || 0) < COLLECTION_SEED_TTL_MS) {
      this.readStats.collectionDeduped++;
      return false;
    }
    this.slugSeedInFlight.add(slug);
    /**
     * MỐC TTL LÀ PHẦN THƯỞNG CHO MỘT LƯỢT ĐỌC THÀNH CÔNG
     *
     *   Ghi mốc TRƯỚC khi đọc nghĩa là một lượt đọc hỏng cũng mua được năm
     *   phút im lặng: sổ tin rằng collection không có Collection Offer nào,
     *   và bot đặt giá dưới một offer có thật cho tới khi TTL hết. Mốc chỉ
     *   được ghi khi đã thực sự biết câu trả lời.
     */
    this.readStats.collection++;
    const release = () => this.slugSeedInFlight.delete(slug);
    this.recovery.push({
      kind: `collection:${reason}`,
      onDrop: release,
      run: async ({signal}) => { try { return await this.adapter.fetchCollectionOffersFor(slug, {signal}); } finally { release(); } },
      onResult: (offers, generation) => {
        if (generation !== this.recovery.generation || !Array.isArray(offers)) return;
        if (offers.failed) {
          // Không biết ≠ không có. Không ghi mốc ⇒ lượt sau đọc lại ngay.
          this.readStats.collectionFailed = (this.readStats.collectionFailed || 0) + 1;
          this.log(`đọc Collection Offer của ${slug} không thành công (${offers.reason || "?"}) — sẽ đọc lại`);
          return;
        }
        this.slugSeedAt.set(slug, Date.now());
        const self = this.builder ? this.builder.address.toLowerCase() : "";
        const now = Date.now();
        let merged = 0;
        const touched = [];
        for (const [key, row] of this.rows) {
          if (row.collectionSlug !== slug) continue;
          const book = this.book.get(key);
          if (!book) continue;
          let changed = false;
          for (const o of offers) {
            if (!o.covers(row.tokenId)) continue;
            if (book.isTombstoned(o.orderHash)) continue;
            const mine = self && o.maker === self;
            const group = mine ? book.own : o.kind === "trait" ? book.trait : book.collection;
            if (group.has(o.orderHash)) continue;
            group.set(o.orderHash, { orderHash: o.orderHash, price: o.price, maker: o.maker,
              kind: mine ? "item" : o.kind, quantity: Number(o.quantity) || 1, currency: "WETH", endTime: o.endTime, seq: 0, at: now });
            book.capGroup(group);
            changed = true; merged++;
          }
          if (changed) { book.generation++; touched.push(key); }
        }
        if (touched.length) {
          this.changed();
          for (const key of touched) this.evaluate(key, now, null);
          this.pump();
        }
        this.log(`đọc collection ${slug}: ${offers.length} criteria offer · ghép ${merged} vào ${touched.length} NFT`);
      }
    });
    return true;
  }

  /** Số lần đọc lại tối đa cho lượt đọc ĐẦU TIÊN của một token khi REST hỏng. */
  static get FIRST_READ_ATTEMPTS() { return 5; }

  /**
   * Bao lâu thì một dấu "đang đọc" chắc chắn là dấu bị bỏ quên.
   *
   *   Rộng hơn mốc dừng của RecoveryPlane (120s) một quãng, vì một việc còn
   *   phải xếp hàng trước khi chạy. Quá mốc này thì không còn là "chậm" nữa:
   *   mặt phẳng phục hồi lẽ ra đã phải bỏ nó và gọi `onDrop`.
   */
  static get HYDRATING_STUCK_MS() { return 180000; }

  markHydrating(key) {
    this.hydrating.add(key);
    if (!this.hydratingSince.has(key)) this.hydratingSince.set(key, Date.now());
  }

  clearHydrating(key) {
    this.hydrating.delete(key);
    this.hydratingSince.delete(key);
  }

  /**
   * Dấu "đang đọc" này có còn là một lượt đọc thật không.
   *
   *   Đây là câu hỏi mà mọi lưới an toàn phải hỏi thay cho `hydrating.has()`.
   */
  hydratingIsReal(key, now = Date.now()) {
    if (!this.hydrating.has(key)) return false;
    const since = this.hydratingSince.get(key);
    if (!since) return true;                       // vừa đặt, chưa kịp ghi mốc
    return now - since < OfferItemEngineV2.HYDRATING_STUCK_MS;
  }

  /**
   * Một lượt đọc REST cho một token, qua RecoveryPlane.
   *
   *   Xoá dấu `hydrating` trong `finally` chứ không trong `onResult`:
   *   `onResult` KHÔNG chạy khi việc lỗi, cũng không chạy khi kết quả về muộn
   *   và bị bỏ vì cũ — và một hàng mắc kẹt vĩnh viễn ở "Đang lấy Best Offer"
   *   là lời nói dối đắt hơn nhiều so với việc không hiện gì.
   *
   *   LƯỢT ĐỌC ĐẦU TIÊN HỎNG THÌ ĐỌC LẠI, KHÔNG GỬI MIN
   *
   *   "Không ai đặt" là một câu trả lời (200, danh sách trống); "không đọc
   *   được" thì không. Gửi Min sau một lượt đọc hỏng là gửi Min trước khi
   *   biết Best — đúng lỗi §24 dưới một cái tên khác. Nên hỏng thì hẹn đọc
   *   lại (5 giây, tối đa 5 lần); hết lượt mới tính với những gì Stream đã
   *   nói, còn hơn đứng im mãi.
   */
  queueRead(row, { reason, authoritative, readAt, firstRead, attempt, correlationId = "" }) {
    const epoch = this.epoch;
    /**
     * HÀNG ĐÃ DỪNG KHÔNG CÒN ĐỌC NỮA
     *
     *   Hẹn giờ đọc lại được đặt lúc hàng đang chạy và sống lâu hơn nút Dừng:
     *   một hàng bị tạm dừng vẫn tiếp tục tiêu hạn mức đọc 5 giây, 30 giây,
     *   mãi mãi — và đúng lúc đang 429 thì chỗ đó là chỗ đắt nhất. Chặn ở
     *   cổng vào, một lần, thay vì ở từng chỗ đặt hẹn giờ.
     */
    if (!row || !this.rows.has(row.key)) return false;
    if (row.running === false) {
      this.clearHydrating(row.key);
      this.awaitingFirstRead.delete(row.key);
      this.renewCheck.delete(row.key);
      this.renewSince.delete(row.key);
      const pr = this.pendingReads.get(row.key);
      if (pr && pr.timer) clearTimeout(pr.timer);
      this.pendingReads.delete(row.key);
      const ft = this.firstReadTimers.get(row.key);
      if (ft) { clearTimeout(ft); this.firstReadTimers.delete(row.key); }
      return false;
    }
    /**
     * MỘT TOKEN, MỘT LƯỢT ĐỌC CÓ THẨM QUYỀN
     *
     *   `gap`, `own-top`, `renew`, `reconnect`, `stream-degraded` có thể hỏi
     *   cùng một token trong cùng một giây. Mỗi lý do một request là nhân đôi,
     *   nhân ba tải REST đúng lúc đang cần nó nhất. Đang có lượt đọc thì lý do
     *   mới chỉ NÂNG CẤP lượt đó (rẻ → đầy đủ) chứ không tạo lượt thứ hai.
     */
    this.readStats.requested++;
    this.netStat(`read:${reason}`, 1);
    /**
     * "gate-repair" PHẢI LÀ LƯỢT ĐỌC ĐẦY ĐỦ
     *
     *   Lượt chữa của bất biến cổng được xếp khi một hàng đang chờ mà không
     *   có lượt đọc nào. Nếu nó đi đường `/best` rẻ thì với hàng đang
     *   `renewCheck` nó KHÔNG BAO GIỜ mở được cổng — `mergeBest` cố ý không
     *   settle theo một câu trả lời rẻ ở đó — nên mỗi vòng watchdog tốn thêm
     *   một lượt đọc mà màn hình không đổi. Lượt chữa phải là lượt đọc có
     *   khả năng kết thúc việc nó được sinh ra để kết thúc.
     */
    const wantsFull = reason === "gap" || reason === "reconnect" || reason === "stream-degraded" ||
      reason === "topic-gap" || reason === "first-join" ||
      reason === "own-top" || reason === "renew" || reason === "self-cancel" ||
      reason === "contradiction" || reason === "gate-repair" || reason === "post-uncertain" ||
      reason === "pre-post-own" || authoritative;
    const recoveryPriority = firstRead || (this.awaitingFirstRead.has(row.key) && !(this.book.get(row.key)?.hydratedAt > 0)) ||
      reason === "pre-post-own" || reason === "post-uncertain" ? 1 : 2;
    if (this.hydrating.has(row.key)) {
      const cur = this.pendingReads.get(row.key);
      if (cur) {
        cur.reason = wantsFull ? reason : cur.reason;
        cur.authoritative = cur.authoritative || authoritative;
        cur.upgradeToFull = cur.upgradeToFull || wantsFull;
      }
      this.readStats.deduped++;
      return false;
    }
    this.markHydrating(row.key);
    // Ghi nợ: chỉ xoá khi đọc THÀNH CÔNG (xem finally và watchdog).
    const prev = this.pendingReads.get(row.key);
    if (prev && prev.timer) clearTimeout(prev.timer);
    this.pendingReads.set(row.key, { reason, authoritative, attempt: attempt || 1, timer: null,
      since: prev ? prev.since : Date.now(), correlationId: correlationId || prev?.correlationId || "" });
    let quick = !wantsFull && typeof this.adapter.fetchBestQuick === "function";
    const readOwner = this.pendingReads.get(row.key);
    if (quick) this.readStats.quick++; else this.readStats.full++;
    const pushed = this.recovery.push({
      kind: reason,
      // 1 = authority chặn hàng chưa sẵn sàng; 2 = recovery có mục tiêu.
      priority: recoveryPriority,
      // Việc bị bỏ vì generation đổi (reconnect chen ngang): dọn dấu "đang
      // đọc" NGAY, nếu không hàng kẹt ở "Đang lấy Best Offer" và watchdog
      // tưởng đang có lượt đọc nên không cứu.
      onDrop: () => {
        if (this.pendingReads.get(row.key) !== readOwner) return;
        this.clearHydrating(row.key);
        this.stats.readsDropped = (this.stats.readsDropped || 0) + 1;
        if (this.state === STATE.RUNNING && this.epoch === epoch && this.rows.has(row.key) &&
            (this.awaitingFirstRead.has(row.key) || this.pendingReads.has(row.key))) {
          const firstGate = this.awaitingFirstRead.has(row.key);
          const timer = setTimeout(() => {
            if (firstGate && this.firstReadTimers.get(row.key) === timer) this.firstReadTimers.delete(row.key);
            if (!firstGate && readOwner.timer === timer) readOwner.timer = null;
            if (this.epoch !== epoch || this.state !== STATE.RUNNING) return;
            if (!this.rows.has(row.key)) return;
            if (this.pendingReads.get(row.key) !== readOwner) return;
            this.queueRead(row, { reason, authoritative, readAt: Date.now(), firstRead, attempt,
              correlationId: readOwner.correlationId });
          }, 250);
          if (timer.unref) timer.unref();
          // The retry itself is canonical lifecycle state.  Recording it here
          // prevents the first-read watchdog from seeing a fake orphan in the
          // 250ms handoff window after a stale completion.
          if (firstGate) {
            const old = this.firstReadTimers.get(row.key);
            if (old) clearTimeout(old);
            this.firstReadTimers.set(row.key, timer);
          } else {
            readOwner.timer = timer;
          }
        }
        this.changed();
      },
      run: async ({signal}) => {
        let readOk = false;
        try {
          /**
           * "KHÔNG AI ĐẶT" CŨNG LÀ MỘT CÂU TRẢ LỜI
           *
           *   `fetchBest` trả null khi NFT chưa có offer nào. Bản đầu để null
           *   rơi qua `onResult` như thể không có gì xảy ra — nên hàng đó
           *   không bao giờ được tính, và không bao giờ nhận offer đầu tiên ở
           *   giá Min. Nên "không ai" được gói thành một giá trị có thật để
           *   `mergeBest` vẫn được gọi, và nó tính giá cho hàng đó.
           */
          // "gap": cần cả danh sách (item offer thấp hơn top) → đường đầy đủ.
          // Đọc có thẩm quyền (suy giảm) thay cả sổ → cần danh sách đầy đủ.
          // Cờ tạm, đọc bởi adapter để chọn hạng ưu tiên. Đặt ngay trước
          // lượt gọi và gỡ ngay sau, để không có hàng nào mang cờ "lượt đọc
          // đầu" sang một lượt đọc khác.
          row.firstRead = Boolean(firstRead) || (this.awaitingFirstRead.has(row.key) && !(this.book.get(row.key)?.hydratedAt > 0));
          const httpReadPriority = row.firstRead ? PRIORITY.INITIAL :
            recoveryPriority <= 1 ? PRIORITY.P0 : PRIORITY.P2;
          // Mốc rời máy: một sự kiện tới SAU mốc này có thể không nằm trong
          // câu trả lời (xem resolveTraitScope).
          readOwner.startedAt = Date.now();
          readOwner.startedSeq = this.book.get(row.key)?.streamSeq || 0;
          let best;
          try {
            if (readOwner.upgradeToFull) quick = false;
            best = quick ? await this.adapter.fetchBestQuick(row, { signal, priority: httpReadPriority })
              : await this.adapter.fetchBest(row, { signal, priority: httpReadPriority });
            if (quick && readOwner.upgradeToFull && !signal.aborted) {
              quick = false; best = await this.adapter.fetchBest(row, { signal, priority: httpReadPriority });
            }
            if(signal.aborted)throw Object.assign(new Error("Read cancelled"),{name:"AbortError"});
            authoritative = authoritative || readOwner.authoritative;
          } finally {
            row.firstRead = false;
          }
          readOk = true;
          return best || { empty: true };
        } finally {
          if (this.pendingReads.get(row.key) !== readOwner) return;
          this.clearHydrating(row.key);
          const gated = this.awaitingFirstRead.has(row.key);
          if (readOk) {
            // Keep this owner until mergeBest settles the result.  A reconnect
            // can invalidate RecoveryPlane after fetchBest resolves but before
            // onResult runs; deleting here orphaned the first-read gate.
          } else if (!gated && this.epoch === epoch && this.state === STATE.RUNNING) {
            /**
             * ĐỌC LẠI KHÔNG PHẢI LƯỢT ĐẦU MÀ HỎNG: VẪN PHẢI ĐỌC LẠI
             *
             *   Reconnect / gap / nhất quán từng là bắn-rồi-quên. Một 429 lúc
             *   nối lại là một Item Offer đặt trong khoảng mù không bao giờ được
             *   biết. Bậc 5s → 15s → 30s → 60s, mãi cho tới khi đọc được.
             */
            // Lượt đọc đang là CHỦ của một SEND chờ own: lùi nhanh hơn, vẫn có trần.
            const table = reason === "pre-post-own" || reason === "post-uncertain" ? OWN_READ_RETRY_MS : READ_RETRY_MS;
            const wait = table[Math.min(table.length - 1, Math.max(0, attempt - 1))];
            const pr = this.pendingReads.get(row.key);
            if (pr) {
              if (pr.timer) clearTimeout(pr.timer);
              pr.timer = setTimeout(() => {
                pr.timer = null;
                if (this.epoch !== epoch || this.state !== STATE.RUNNING) return;
                if (!this.pendingReads.has(row.key) || this.hydrating.has(row.key)) return;
                if (!this.rows.has(row.key)) { this.pendingReads.delete(row.key); return; }
                this.queueRead(row, { reason, authoritative, readAt: Date.now(), firstRead: false,
                  attempt: attempt + 1, correlationId: readOwner.correlationId });
              }, wait);
              if (pr.timer.unref) pr.timer.unref();
            }
            this.stats.readRetries = (this.stats.readRetries || 0) + 1;
            if (attempt <= 2) this.log(`#${row.tokenId} đọc lại (${reason}) hỏng (lượt ${attempt}) — đọc lại sau ${wait / 1000}s`);
          }
          if (gated && !readOk && this.epoch === epoch && this.state === STATE.RUNNING) {
            /**
             * CHƯA BIẾT KHÔNG BAO GIỜ THÀNH "KHÔNG AI ĐẶT"
             *
             *   Đọc hỏng thì đọc lại: 5 giây cho những lượt đầu, rồi 30 giây
             *   mãi cho tới khi đọc được hoặc người dùng dừng. Không có "hết
             *   lượt thì gửi Min" — Min chỉ đi khi REST nói danh sách trống.
             */
            const wait = attempt < OfferItemEngineV2.FIRST_READ_ATTEMPTS ? 5000 : 30000;
            const timer = setTimeout(() => {
              this.firstReadTimers.delete(row.key);
              if (this.epoch !== epoch || this.state !== STATE.RUNNING) return;
              if (!this.awaitingFirstRead.has(row.key)) return;
              this.queueRead(row, { reason, authoritative, readAt: Date.now(), firstRead: true, attempt: attempt + 1 });
            }, wait);
            if (timer.unref) timer.unref();
            const old = this.firstReadTimers.get(row.key);
            if (old) clearTimeout(old);
            this.firstReadTimers.set(row.key, timer);
            this.stats.firstReadRetries = (this.stats.firstReadRetries || 0) + 1;
            this.log(`#${row.tokenId} chưa đọc được trạng thái ban đầu (lượt ${attempt}) — đọc lại sau ${wait / 1000}s, KHÔNG gửi`);
          }
          // Đọc thành công: cổng mở TRONG `mergeBest`, khi kết quả thật sự
          // được ghép — kết quả bị bỏ vì cũ (reconnect chen ngang) thì cổng
          // còn nguyên và lượt đọc của reconnect sẽ mở nó.
          this.changed();
        }
      },
      onResult: (best, generation) =>
        this.mergeBest(row.key, best, generation, { authoritative, readAt, quick, reason, readOwner })
    });
    // Mặt phẳng nền đang dừng (máy ngủ): việc không được nhận. Không giữ dấu
    // "đang đọc" giả; sổ nợ còn đó và watchdog xếp lại sau khi thức.
    if (!pushed) this.clearHydrating(row.key);
    return pushed;
  }

  /**
   * Đọc lại số dư, có nguội — để biết khi người dùng vừa nạp thêm WETH.
   *
   * Không có nhịp định kỳ nào ở đây: nó chỉ chạy sau một lần bị chặn vì
   * thiếu tiền, và tối đa một lần trong mỗi quãng nguội. Không có nó thì một
   * ví được nạp lại vẫn đứng im cho tới lần Start kế tiếp.
   */
  refreshBalanceSoon(now = Date.now(), { urgent = false } = {}) {
    if (typeof this.adapter.walletBalance !== "function") return false;
    /**
     * ONE PROBE IN FLIGHT, AND NEVER A DEAD END (1.25.11)
     *
     *   A probe already on its way answers for every blocked row. Returning
     *   here used to leave no timer behind: a probe slower than the 2s
     *   coalescing timer, which then came back "still short" (or never came
     *   back - RecoveryPlane only reports success), left the row LOW_BALANCE
     *   with nothing scheduled. The lost-probe watch below always exists while
     *   a probe is out, and the result itself re-arms the next step.
     */
    if (this.balanceProbeInFlightAt && now - this.balanceProbeInFlightAt < BALANCE_PROBE_INFLIGHT_MS) {
      this.armBalanceTimer(this.balanceProbeInFlightAt + BALANCE_PROBE_INFLIGHT_MS - now);
      return false;
    }
    let gap;
    if (urgent) {
      /**
       * A NEW INCIDENT IS NOT PARKED BEHIND AN OLD BACK-OFF (1.25.11)
       *
       *   The wallet back-off belongs to rows a fresh read already found short.
       *   A row that has JUST become LOW_BALANCE is answered from a read at most
       *   BALANCE_RESULT_FRESH_MS old - reused without RPC when there is one -
       *   and otherwise by a fresh read after the small coalescing floor.
       */
      if (this.balance && this.balance.known && this.balanceResultAt &&
          now - this.balanceResultAt < BALANCE_RESULT_FRESH_MS &&
          !(this.balanceCeiling && this.balanceResultAt < this.balanceCeiling.at)) {
        this.applyBalance(this.balance, now);
        this.armBalanceTimer(this.balanceResultAt + BALANCE_RESULT_FRESH_MS - now);
        return false;
      }
      gap = BALANCE_PROBE_URGENT_FLOOR_MS;
    } else {
      gap = this.balanceProbeGapMs();
    }
    // Inside the gap: never dropped - the one wallet-level timer runs it.
    const wait = this.balanceProbedAt + gap - now;
    if (wait > 0) { this.armBalanceTimer(wait); return false; }
    this.balanceProbedAt = now;
    this.balanceProbeInFlightAt = now;
    // Lost-probe watch: replaced by the result's own next step.
    this.armBalanceTimer(BALANCE_PROBE_INFLIGHT_MS, { replace: true });
    const epoch = this.epoch;
    const probeAddress = this.builder && this.builder.address;
    const probeStartedAt = now;
    if (!probeAddress) { this.balanceProbeInFlightAt = 0; return false; }
    this.recovery.push({
      kind: "balance",
      run: async () => this.adapter.walletBalance(probeAddress, { force: true }),
      onResult: (balance, generation) => {
        this.balanceProbeInFlightAt = 0;
        if (generation !== this.recovery.generation || epoch !== this.epoch) return;
        if (!balance || !balance.known) {
          // A failed read is not a verdict: try again after the normal gap.
          if (balance) this.balance = balance;
          this.armBalanceTimer(this.balanceProbeGapMs(), { replace: true });
          return;
        }
        this.balance = balance;
        this.balanceResultAt = Date.now();
        if (this.balanceCeiling && probeStartedAt >= this.balanceCeiling.at) this.balanceCeiling = null;
        this.applyBalance(balance, this.balanceResultAt);
      }
    });
    return true;
  }

  /**
   * A known balance against every LOW_BALANCE row, each on its OWN current
   * target (decide()), and the ONE next step for the wallet timer.
   */
  applyBalance(balance, at = Date.now()) {
    // The balance this decision is made on is the one the send pre-check uses
    // next; a stale cached value there would re-block a row just unblocked.
    if (balance && balance.known) this.balance = balance;
    if (this.balanceCeiling && balance && balance.known && this.balanceCeiling.balWei != null &&
        balance.wethWei > this.balanceCeiling.balWei) this.balanceCeiling = null;
    const freed = [];
    let stillShort = 0;
    let serverDue = 0;
    for (const [key, row] of this.rows) {
      if (!row.lowBalance) continue;
      const verdict = this.currentVerdict(key, at);
      if (!verdict || verdict.status !== STATUS.SEND) {
        this.clearStaleLowBalance(row, verdict ? verdict.status : "NO_BOOK");
        freed.push(key);
        continue;
      }
      const need = BigInt(this.adapter.toWei(String(verdict.target)));
      if (balance.wethWei < need || (this.balanceCeiling && need >= this.balanceCeiling.wei)) {
        if (row.running) stillShort++;
        continue;
      }
      /**
       * OPENSEA'S REFUSAL OUTRANKS A RAW balanceOf (1.25.11)
       *
       *   WETH 0.08 ≥ target 0.0161, yet OpenSea answered "insufficient" again:
       *   clearing on every read made clear → POST → reject every ~2s. After a
       *   second refusal the row waits its own bounded back-off (30s, 60s, …
       *   ≤ 5 min) unless something meaningful changed: the wallet holds MORE
       *   than when it was last retried, or the target went DOWN.
       */
      const ev = row.serverLowBal;
      if (ev && ev.count >= 2) {
        const improved = (ev.wei != null && balance.wethWei > ev.wei) ||
          Number(verdict.target) < Number(ev.target) - 1e-12;
        const until = ev.at + Math.min(BALANCE_PROBE_MAX_GAP_MS, BALANCE_PROBE_COOLDOWN_MS * (2 ** (ev.count - 2)));
        if (!improved && at < until) {
          if (row.running) serverDue = serverDue ? Math.min(serverDue, until) : until;
          continue;
        }
      }
      if (ev) ev.wei = balance.wethWei;   // the baseline this retry is made on
      row.lowBalance = false;
      if (/insufficient/i.test(String(row.lastError || ""))) row.lastError = "";
      freed.push(key);
    }
    this.balanceProbeFailures = stillShort ? Math.min(8, (this.balanceProbeFailures || 0) + 1) : 0;
    if (freed.length) {
      this.log(`[LOW BALANCE] số dư mới: gỡ chặn ${freed.length} hàng${stillShort ? ` · còn ${stillShort} hàng thiếu` : ""}`);
    }
    // The next step, if any row is still blocked.
    if (stillShort) this.armBalanceTimer(this.balanceProbeGapMs(), { replace: true });
    if (serverDue) this.armBalanceTimer(Math.max(0, serverDue - Date.now()) + 50, stillShort ? {} : { replace: true });
    if (!stillShort && !serverDue && !freed.length) this.cancelBalanceTimer();
    // Clearing the block recreates the SEND intent at once from the local
    // book: no Stream event needed to wake it.
    for (const key of freed) this.evaluate(key, Date.now(), null, "recovery");
    if (freed.length) this.pump();
  }

  /**
   * Back-off between balance probes while rows stay LOW_BALANCE, counted from
   * the fresh reads that CONFIRMED a shortfall: 30s, 60s, 120s, … ≤ 5 min.
   */
  balanceProbeGapMs() {
    const n = Math.max(0, (Number(this.balanceProbeFailures) || 0) - 1);
    return Math.min(BALANCE_PROBE_MAX_GAP_MS, BALANCE_PROBE_COOLDOWN_MS * (2 ** n));
  }

  /**
   * ONE wallet-level timer (never one per NFT). By default it is only ever
   * pulled EARLIER; `replace` sets it outright (the result of a probe decides
   * the next step). Fires only while a running row is LOW_BALANCE, then ends.
   * Cleared in stopLoops() (Stop / reset / shutdown / wallet change). Background
   * work only - never on the Stream → POST path.
   */
  armBalanceTimer(delayMs, { replace = false } = {}) {
    if (this.state !== STATE.RUNNING) return;
    const due = Date.now() + Math.max(250, Number(delayMs) || 0);
    if (this.balanceRetryTimer) {
      if (!replace && this.balanceRetryDueAt && this.balanceRetryDueAt <= due) return;
      clearTimeout(this.balanceRetryTimer);
    }
    this.balanceRetryDueAt = due;
    this.balanceRetryTimer = setTimeout(() => {
      this.balanceRetryTimer = null;
      this.balanceRetryDueAt = 0;
      if (this.state !== STATE.RUNNING) return;
      let blocked = false;
      for (const row of this.rows.values()) if (row.lowBalance && row.running) { blocked = true; break; }
      if (!blocked) { this.balanceProbeFailures = 0; return; }
      this.refreshBalanceSoon();
    }, due - Date.now());
    this.balanceRetryTimer.unref?.();
  }

  /** Kept for callers of the 1.25.10 name. */
  scheduleBalanceProbe(delayMs) { this.armBalanceTimer(delayMs); }

  cancelBalanceTimer() {
    if (this.balanceRetryTimer) clearTimeout(this.balanceRetryTimer);
    this.balanceRetryTimer = null;
    this.balanceRetryDueAt = 0;
  }

  /** The canonical verdict for a row from its CURRENT book (same decide()). */
  currentVerdict(key, at = Date.now()) {
    const row = this.rows.get(key);
    const book = this.book.get(key);
    if (!row || !book) return null;
    return decide({
      best: book.effectiveBest(at).price, mine: book.ownBest(at).price,
      minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step
    });
  }

  /** A LOW_BALANCE that no longer blocks anything: current state decides. */
  clearStaleLowBalance(row, status) {
    if (!row || !row.lowBalance) return;
    row.lowBalance = false;
    if (/insufficient/i.test(String(row.lastError || ""))) row.lastError = "";
    this.stats.lowBalanceStaleCleared = (this.stats.lowBalanceStaleCleared || 0) + 1;
    this.log(`[LOW BALANCE] #${row.tokenId} gỡ chặn cũ: trạng thái hiện tại ${status}, không cần gửi`);
    this.changed();
  }

  /**
   * Trait offer vừa tới có áp cho những token này không? Đi hỏi, đừng đoán.
   *
   * VẤN ĐỀ, ĐO ĐƯỢC CHỨ KHÔNG SUY RA
   *
   *   Nghe Stream thật 90 giây trên mười collection đông người: 78.580 sự
   *   kiện, 452 trong đó là trait offer. Hàng Offer Item KHÔNG mang danh
   *   sách trait nào — `db.js` không lưu trait cho hàng Offer Item — nên sổ
   *   không thể nói một trait offer có áp cho token hay không.
   *
   *   Bỏ qua là AN TOÀN: áp bừa một trait offer cho cả collection là tự vượt
   *   giá mình trên hàng nghìn NFT không ai đặt offer. Nhưng an toàn không
   *   phải là đúng — kết quả là ta bị vượt giá mà không biết, và vì V2 không
   *   có nhịp quét định kỳ nào, sẽ KHÔNG có gì phát hiện ra sau đó. Engine
   *   cũ tình cờ được cứu bởi việc nó quét REST theo chu kỳ.
   *
   * CÁCH GIẢI: HỎI NGUỒN THẬT, NGOÀI ĐƯỜNG NÓNG
   *
   *   `fetchBest` trả về giá tốt nhất mà OpenSea công nhận cho ĐÚNG token
   *   đó, đã tính cả offer theo collection và theo trait. Nó trả lời câu hỏi
   *   mà ta không tự trả lời được, và nó chạy trên mặt phẳng recovery: có bộ
   *   giới hạn đọc riêng, generation riêng, không bao giờ chặn phản ứng.
   *
   * VÀ NÓ PHẢI CÓ TRẦN
   *
   *   Không có trần thì mỗi trait offer sinh một lượt đọc cho mỗi token của
   *   collection — đúng kiểu bão đọc mà V2 sinh ra để loại bỏ. Ba lớp chặn:
   *
   *     1. GIÁ    offer thấp hơn best hiện tại của token thì không đổi được
   *               gì, dù nó có áp hay không. Không hỏi.
   *     2. NGUỘI  mỗi token tối đa một lượt hỏi trong TRAIT_PROBE_COOLDOWN_MS.
   *     3. TRẦN   tối đa TRAIT_PROBE_MAX_PER_EVENT token cho mỗi sự kiện.
   */
  resolveTraitScope(op, keys, receivedAt) {
    if (this.state !== STATE.RUNNING) return 0;
    if (typeof this.adapter.fetchBest !== "function") return 0;

    const price = Number(op.price) || 0;
    if (price <= 0) return 0;

    let asked = 0;
    for (const key of keys) {
      if (asked >= TRAIT_PROBE_MAX_PER_EVENT) break;
      const row = this.rows.get(key);
      if (!row || !row.running) continue;

      // 0 · đang được đọc thì câu hỏi này SẮP có câu trả lời rồi — NẾU lượt
      //     đọc ấy chưa rời máy. Xếp thêm một lượt nữa là hai việc cho một
      //     câu hỏi — đo được: REST trễ 5 giây, hàng đợi phình gấp đôi.
      //
      //     Nhưng một lượt đọc ĐÃ RỜI MÁY trước khi sự kiện tới có thể trả về
      //     danh sách chưa có offer này — và V2 không có nhịp quét nào hỏi
      //     lại. Đó là một lượt vượt giá bị bỏ lỡ vĩnh viễn. Nên ghi nợ một
      //     lượt hỏi tiếp NGAY SAU khi lượt đang bay settle (mergeBest), và
      //     tính nó vào trần của sự kiện này.
      if (this.hydrating.has(key)) {
        const pr = this.pendingReads.get(key);
        if (pr && pr.startedAt && pr.startedAt < receivedAt && !(pr.rereadAfter >= receivedAt)) {
          pr.rereadAfter = receivedAt;
          this.traitProbedAt.set(key, receivedAt);
          this.stats.traitProbeFollowUps = (this.stats.traitProbeFollowUps || 0) + 1;
          asked++;
          continue;
        }
        this.stats.traitProbeSkipped++;
        continue;
      }

      // 1 · không đổi được gì thì không hỏi.
      const book = this.book.get(key);
      if (!book) continue;
      if (price <= book.effectiveBest(receivedAt).price) continue;
      // Cao hơn Max của hàng thì dù có áp ta cũng không gửi được.
      if (price > Number(row.maxPrice)) continue;

      // 2 · nguội. Đếm cả lượt bị bỏ qua: bài stress cần thấy 99 lượt đi đâu.
      const last = this.traitProbedAt.get(key) || 0;
      if (receivedAt - last < TRAIT_PROBE_COOLDOWN_MS) {
        this.stats.traitProbeSkipped++;
        continue;
      }
      this.traitProbedAt.set(key, receivedAt);

      this.queueRead(row, {reason:"trait-scope", authoritative:true, readAt:receivedAt, firstRead:false, attempt:1});
      asked++;
    }
    if (asked) this.stats.traitProbes = (this.stats.traitProbes || 0) + asked;
    return asked;
  }

  /**
   * Nạp trait cho mọi hàng, ở nền, theo lô — rồi đưa vào sổ.
   *
   *   Chạy qua RecoveryPlane để có cùng bộ giới hạn, cùng generation và cùng
   *   khả năng huỷ khi Reset. Mỗi lô xong là `setTraits` cho những token của
   *   lô đó — sổ biết dần, không đợi tất cả.
   */
  hydrateTraits(reason = "startup") {
    if (!this.traits || typeof this.traits.hydrate !== "function") return 0;
    // Không có nguồn thì không có việc — và không có dòng log nào để mà kể.
    if (typeof this.traits.getNFTs !== "function") return 0;
    const rows = [...this.rows.values()]
      .filter(r => !this.traits.has(r.key))
      .map(r => ({ key: r.key, contract: r.contract, tokenId: r.tokenId }));
    if (!rows.length) return 0;

    this.recovery.push({
      kind: `traits:${reason}`,
      run: async ({ signal }) => this.traits.hydrate(this.chain, rows, {
        signal,
        onBatch: () => this.applyTraitSnapshot()
      }),
      onResult: (out, generation) => {
        if (generation !== this.recovery.generation) return;
        this.applyTraitSnapshot();
        if (out) this.log(`trait: nạp ${out.hydrated}/${rows.length} token ` +
          `(${out.empty} không có trait, ${out.failed} chưa biết) qua ${out.batches} lô`);
      }
    });
    return rows.length;
  }

  /** Đưa những gì bản chụp đã biết vào sổ. Idempotent. */
  applyTraitSnapshot() {
    let learned = 0;
    for (const row of this.rows.values()) {
      const book = this.book.get(row.key);
      if (!book || book.traitsKnown) continue;
      const view = this.traits.get(row.key);
      if (!view.known) continue;
      book.traits = new Set(view.keys);
      book.numericTraits = new Map(view.numericValues || []);
      book.duplicateNumericTraitTypes = new Set(view.duplicateNumericTypes || []);
      book.traitsKnown = true;
      learned++;
      /**
       * VỪA BIẾT TRAIT → ÁP LẠI NHỮNG TRAIT OFFER ĐÃ TỚI TRƯỚC ĐÓ
       *
       *   Trait offer tới lúc token còn "chưa biết" bị bỏ qua đúng luật. Giờ
       *   biết rồi, sổ áp lại chúng từ bộ nhớ của collection — thuần cục bộ,
       *   không hỏi mạng — và tính lại nếu có gì đổi.
       */
      if (this.book.replayTraitOps(row.key, Date.now()) && this.state === STATE.RUNNING) {
        this.stats.traitReplayed = (this.stats.traitReplayed || 0) + 1;
        this.evaluate(row.key, Date.now(), null);
      }
    }
    if (learned) { this.changed(); this.pump(); }
  }

  /**
   * Ghép một giá đọc từ REST vào sổ.
   *
   * REST BỔ SUNG, KHÔNG GHI ĐÈ — VÀ "BỔ SUNG" KHÔNG CÓ NGHĨA LÀ "CHỈ KHI SỔ TRỐNG"
   *
   *   Bản đầu bỏ qua kết quả REST ngay khi Stream đã nói BẤT CỨ ĐIỀU GÌ về
   *   token đó (`lastEventAt > 0`). Ý định là đúng — REST là ảnh chụp cũ hơn
   *   Stream — nhưng phép thử thì sai: một collection sôi động sinh sự kiện
   *   liên tục, nên trước khi lượt đọc đầu tiên kịp về, sổ đã nhận một bid
   *   nhỏ nào đó, và Collection Offer 0.014 đặt TỪ TRƯỚC KHI subscribe — thứ
   *   không bao giờ sinh sự kiện nữa — bị vứt đi. Engine thấy Item 0.0105,
   *   không thấy Collection 0.014, và gửi 0.0106 vào một thị trường mà nó
   *   đang thua. Đây là nguồn gốc của "Offer Item chỉ thấy Item Offer".
   *
   *   Sự thật cần giữ là: Stream mới hơn về CÙNG MỘT ORDER. Order được khoá
   *   bằng hash, nên "không ghi đè" đã được đảm bảo bởi hai kiểm tra bên
   *   dưới — có bia mộ (Stream đã thấy nó bị huỷ) thì không hồi sinh; đã có
   *   trong sổ (Stream đã thấy nó) thì không chạm. Một order REST trả về mà
   *   sổ chưa từng thấy là thông tin mới, và thêm nó vào chỉ có thể làm
   *   effective best ĐÚNG HƠN: best là giá cao nhất, nên một order cũ hơn
   *   không bao giờ kéo best xuống (bài stale-recovery đo đúng điều này).
   *
   *   Đọc CÓ THẨM QUYỀN (chế độ suy giảm, không có Stream) vẫn thay cả phần
   *   đối thủ của sổ, và chỉ khi không có sự kiện nào mới hơn mốc đọc.
   */
  mergeBest(key, best, generation, { authoritative = false, readAt = 0, quick = false, reason = "", readOwner = null } = {}) {
    if (!best || generation !== this.recovery.generation) { this.scheduleRowWake(key, "stale-read"); return; }
    const book = this.book.get(key);
    if (!book) return;
    const now = Date.now();
    const row = this.rows.get(key);

    // Recovery results are snapshots from when the request started. A Stream
    // event that arrived while the request was in flight makes that snapshot
    // unsafe to merge or settle over the newer per-NFT book state.
    const snapshotStartedAt = Number(readOwner?.startedAt) || Number(readAt) || now;
    const streamAdvanced = Number.isFinite(Number(readOwner?.startedSeq)) &&
      book.streamSeq > Number(readOwner.startedSeq);
    if (streamAdvanced || book.lastEventAt > snapshotStartedAt) {
      this.stats.staleBDiscarded = (this.stats.staleBDiscarded || 0) + 1;
      const ownedRead = readOwner && this.pendingReads.get(key) === readOwner;
      if (ownedRead) {
        if (readOwner.timer) clearTimeout(readOwner.timer);
        this.pendingReads.delete(key);
      }
      this.clearHydrating(key);
      // A newer Stream event fences this snapshot, but a WAITING send still
      // needs a full own-authority read. Hand it directly to a new read owner;
      // otherwise the intent is left waiting until the watchdog repairs it.
      const intent = this.intents.get(key);
      if (ownedRead && !quick && row?.running && this.state === STATE.RUNNING &&
          intent?.state === INTENT.WAITING && !this.ownAuthoritative(key) &&
          (intent.dependency === "own" || intent.dependency === "post-reconcile")) {
        this.queueRead(row, { reason: intent.dependency === "post-reconcile" ? "post-uncertain" : "pre-post-own",
          authoritative: false, readAt: Date.now(), firstRead: false, attempt: 1 });
      }
      const cold = this.awaitingFirstRead.has(key) && !(book.hydratedAt > 0 && book.effectiveBest(now).price > 0);
      if (cold && row?.running && this.state === STATE.RUNNING) {
        setImmediate(() => {
          if (this.state === STATE.RUNNING && this.rows.get(key) === row && !this.hydrating.has(key))
            this.queueRead(row, { reason: "first-read-stale", authoritative: false, readAt: Date.now(), firstRead: true, attempt: 1 });
        });
      }
      if (this.topicRepairAt.has(key) && row?.running && this.state === STATE.RUNNING) {
        setImmediate(() => {
          if (this.rows.get(key) === row && !this.hydrating.has(key) &&
              !this.pendingReads.has(key)) {
            this.queueRead(row, { reason: "topic-gap", authoritative: false,
              readAt: Date.now(), firstRead: false, attempt: 1 });
          }
        });
      }
      this.changed();
      return;
    }

    const settle = () => {
      book.hydratedAt = now;
      // Cả danh sách vừa được ghép: từ mốc đọc này sổ có thẩm quyền để đặt lại.
      if (!quick) {
        const at = readAt > 0 ? readAt : now;
        book.lastFullReadAt = at;
        // Một danh sách ĐẦY ĐỦ của token (adapter ném nếu bị cắt trang) là một
        // lượt kiểm own trọn vẹn cho token này: own order nào còn sống đều
        // nằm trong đó (maker == ta → upsertOwn ở dưới). Nó gỡ khoảng mù của
        // riêng hàng (POST không rõ kết quả) đặt TRƯỚC mốc đọc — không gỡ
        // khoảng mù đặt sau, vì lượt đọc này có thể chưa thấy order đó.
        book.ownReconciledAt = Math.max(book.ownReconciledAt || 0, at);
        if ((book.ownUnknownAt || 0) > 0 && book.ownUnknownAt <= at) book.ownUnknownAt = 0;
      }
      if (reason === "pre-post-own" || reason === "post-uncertain") {
        const own = this.ownDiagnostic(key);
        productionTrace.record("own_state", { correlationId: readOwner?.correlationId }, {
          chain: this.chain, collection: row?.collectionSlug || book.collectionSlug,
          tokenId: row?.tokenId || book.tokenId,
          status: own.covered ? "covered" : "uncovered", reason: "own-state-unknown",
          ownAuthoritative: own.covered, ownSyncPending: own.ownSyncPending,
          ownSyncAt: own.ownSyncAt, ownReconciledAt: own.ownReconciledAt,
          ownKnownAt: own.ownKnownAt, ownUnknownAt: own.ownUnknownAt,
          ownReadOwned: Boolean(readOwner && this.pendingReads.get(key) === readOwner)
        });
      }
      const topicRepairAt = this.topicRepairAt.get(key) || 0;
      if (topicRepairAt && !quick && book.lastFullReadAt > topicRepairAt &&
          (!this.streamHealth || this.streamHealth(row?.collectionSlug) === "HEALTHY")) {
        this.topicRepairAt.delete(key);
      }
      // Kết quả REST đã vào sổ: từ đây hàng được phép gửi — NẾU lượt đọc này
      // được xếp SAU mốc cổng đóng (fencing). Lượt cũ hơn chỉ bổ sung sổ và
      // tự xếp một lượt mới: cổng không bao giờ mồ côi.
      const closedAt = this.gateClosedAt.get(key) || 0;
      const snapshotAt = readAt > 0 ? readAt : now;
      const gateOpened = snapshotAt >= closedAt;
      if (gateOpened) {
        this.awaitingFirstRead.delete(key);
        this.gateClosedAt.delete(key);
        if (!quick) {
          this.renewCheck.delete(key); this.renewSince.delete(key);
          this.downwardAuthority.delete(key);
        }
      } else {
        this.stats.gateHeldStaleRead = (this.stats.gateHeldStaleRead || 0) + 1;
      }
      // Never clear a newer read that replaced this one.
      if (readOwner && this.pendingReads.get(key) === readOwner) {
        if (readOwner.timer) clearTimeout(readOwner.timer);
        this.pendingReads.delete(key);
        if (!gateOpened && row && row.running && this.state === STATE.RUNNING) {
          setImmediate(() => { if (this.rows.has(key)) this.queueRead(row, { reason: "gate-fence", authoritative: false, readAt: Date.now(), firstRead: true, attempt: 1 }); });
        }
        // Một trait offer đã tới trong lúc lượt này bay: hỏi lại một lần.
        if (readOwner.rereadAfter > (readOwner.startedAt || 0) && row && row.running &&
            this.state === STATE.RUNNING) {
          this.queueRead(row, { reason: "trait-scope", authoritative: true, readAt: now, firstRead: false, attempt: 1 });
        }
      }
      const pendingTopicRepairAt = this.topicRepairAt.get(key) || 0;
      if (pendingTopicRepairAt && book.lastFullReadAt <= pendingTopicRepairAt && row?.running && this.state === STATE.RUNNING) {
        setImmediate(() => {
          if (this.rows.get(key) === row && !this.hydrating.has(key) &&
              !this.pendingReads.has(key) && !this.recoveryCandidates.has(key)) {
            this.queueRead(row, { reason: "topic-gap", authoritative: false,
              readAt: Date.now(), firstRead: false, attempt: 1 });
          }
        });
      }
      this.changed();
      this.evaluate(key, now, null);
      this.pump();
      this.armNextExpiry();
    };
    const bestBefore = book.effectiveBest(now).price;
    const escalateQuick = why => {
      if (!row || !row.running || this.state !== STATE.RUNNING) return false;
      // The quick read still owns pendingReads until mergeBest settles. Hand
      // that ownership to the full read before deduplication checks it.
      if (readOwner && this.pendingReads.get(key) === readOwner) this.pendingReads.delete(key);
      if (!this.pendingReads.has(key)) {
        this.queueRead(row, { reason: why, authoritative: false,
          readAt: Date.now(), firstRead: false, attempt: 1 });
      }
      return true;
    };
    if (quick && this.renewCheck.has(key) && escalateQuick("renew")) return;

    if (authoritative && book.lastEventAt < readAt) {
      book.item.clear();
      book.collection.clear();
      book.trait.clear();
      book.generation++;
    }

    /**
     * REST VÀ STREAM GHÉP NHAU, THEO ĐỘ TƯƠI — KHÔNG THAY NHAU, KHÔNG BỎ NHAU
     *
     *   Thêm: mọi order REST trả mà sổ chưa biết (bên dưới) — một Item Offer
     *   đặt trước khi subscribe, hay trong khoảng mù reconnect, vào sổ ở đây.
     *   Bớt: lệnh đối thủ ghi TRƯỚC mốc đọc (trừ quãng trễ index) mà OpenSea
     *   nói không còn: `/best` trả P ⇒ không lệnh nào > P còn sống; danh sách
     *   đầy đủ ⇒ không lệnh nào ngoài danh sách còn sống. Lệnh vào sổ SAU mốc
     *   đọc là tin mới hơn REST và được giữ nguyên.
     */
    const cutoff = readAt > 0 ? readAt - READ_GRACE_MS : 0;
    // Không có đối thủ nào: vẫn phải TÍNH, để hàng nhận offer đầu tiên ở Min.
    if (best.empty) {
      if (quick && book.lastEventAt <= readAt &&
          (bestBefore > 0 || book.ownBest(now).price > 0) && escalateQuick("contradiction")) {
        this.stats.contradictionReads = (this.stats.contradictionReads || 0) + 1;
        return;
      }
      /**
       * "KHÔNG AI ĐẶT" TỪ `/best` KHÔNG ĐƯỢC XOÁ SỔ
       *
       *   `/best` trả rỗng khi top hết hạn/không hoạt động hay chỉ vì cách nó
       *   phân xử — không phải bằng chứng rằng MỌI order khác đã chết. Bản
       *   1.19.14 xoá hết lệnh đối thủ cũ ở đây; sau đó own hết hạn, sổ chỉ
       *   còn Collection 0,0036 mới tới và bot đặt lại 0,0037 dưới Item 0,0455.
       *   Chỉ DANH SÁCH ĐẦY ĐỦ rỗng mới được nói "không còn ai".
       */
      if (!authoritative && !quick && cutoff > 0) {
        const removed = book.pruneCompetitorsAbove(0, cutoff);
        if (removed) { this.stats.prunedByRead = (this.stats.prunedByRead || 0) + removed;
          this.log(`#${row ? row.tokenId : key} danh sách đầy đủ trống — bỏ ${removed} lệnh cũ khỏi sổ`); }
      }
      if (!quick && cutoff > 0) this.pruneLostOwn(book, key, row, [], cutoff);
      if (quick && this.renewCheck.has(key)) return;   // chờ danh sách đầy đủ
      return settle();
    }

    /**
     * CẢ DANH SÁCH KHI CÓ, NGƯỜI THẮNG KHI KHÔNG.
     *
     *   Adapter trả về mọi order đang áp cho token (item + collection + trait
     *   mà OpenSea đã xét khớp). Ghép hết vào đúng nhóm: khi cái cao nhất bị
     *   huỷ, sổ tự biết cái kế tiếp. Một order chỉ được ghép khi sổ CHƯA
     *   biết nó và Stream CHƯA thấy nó bị huỷ.
     *
     *   BEST LÀ CỦA CHÍNH MÌNH: đó là "offer đang mở của mình", không phải
     *   "không có gì" — vào nhóm own, để hàng hiện "dẫn đầu" thay vì "đang
     *   theo dõi" sau khi mở lại app. Đo trên ví thật.
     */
    const self = this.builder ? this.builder.address.toLowerCase() : "";
    const list = Array.isArray(best.orders) && best.orders.length ? best.orders : [best];
    const sec = Math.floor(now / 1000);
    for (const order of list) {
      if (!order || !order.orderHash || !(Number(order.price) > 0)) continue;
      if (Number(order.endTime) > 0 && Number(order.endTime) <= sec) continue;
      if (book.isTombstoned(order.orderHash)) continue;
      const mine = self && String(order.maker || "").toLowerCase() === self;
      if (mine) {
        // Own: qua upsertOwn để không bao giờ thiếu hạn, và hạn thật thay hạn giả định.
        this.upsertOwn(book, this.rows.get(key), order, now);
        continue;
      }
      const group = order.kind === "collection" ? book.collection
        : order.kind === "trait" ? book.trait : book.item;
      if (group.has(order.orderHash)) continue;
      group.set(order.orderHash, {
        orderHash: order.orderHash, price: Number(order.price), maker: String(order.maker || "").toLowerCase(),
        kind: order.kind, quantity: Number(order.quantity) || 1, currency: "WETH",
        endTime: Number(order.endTime) || 0, seq: 0, at: now
      });
      book.capGroup(group);
      book.generation++;
    }
    if (!authoritative && cutoff > 0 && !quick) {
      // Chỉ DANH SÁCH ĐẦY ĐỦ mới được bỏ lệnh: lệnh ghi trước mốc đọc mà không
      // có trong danh sách là lệnh đã chết. `/best` (một order) không bao giờ
      // được dùng để xoá — nó từng thấp hơn sổ và xoá mất Item Offer thật.
      const keep = new Set(list.map(o => o && o.orderHash).filter(Boolean));
      const removed = book.pruneCompetitorsNotIn(keep, cutoff);
      if (removed) {
        this.stats.prunedByRead = (this.stats.prunedByRead || 0) + removed;
        this.log(`#${row ? row.tokenId : key} đọc lại (${reason || "rest"}): bỏ ${removed} lệnh đã chết mà Stream không nói`);
      }
    }
    /**
     * OWN LOST (1.25.1): một danh sách ĐẦY ĐỦ của token không còn order của
     * mình mà sổ nghĩ còn sống → order đó đã mất (huỷ/thay/không còn). Xoá
     * bản ghi ma để hàng không tưởng mình dẫn mãi; settle() tính lại ngay.
     * Chỉ xoá bản ghi cũ hơn mốc đọc một quãng ân hạn (OpenSea cần thời gian
     * index order vừa POST — xoá sớm là gửi trùng).
     */
    if (cutoff > 0 && !quick) this.pruneLostOwn(book, key, row, list, cutoff);
    /**
     * `/best` THẤP HƠN SỔ = MÂU THUẪN, KHÔNG PHẢI LỆNH XOÁ
     *
     *   Hoặc sổ giữ một lệnh đã chết (huỷ trong khoảng mù), hoặc `/best` không
     *   phản ánh đủ. Không đoán bên nào: đọc CẢ DANH SÁCH cho token đó một lần
     *   (có sổ nợ, đọc lại tới khi được), và danh sách mới quyết định.
     */
    if (quick && book.lastEventAt <= readAt && Number(best.price) > 0 &&
        bestBefore > Number(best.price) + 1e-12 && escalateQuick("contradiction")) {
      this.stats.contradictionReads = (this.stats.contradictionReads || 0) + 1;
      this.log(`#${row.tokenId} /best=${best.price} thấp hơn sổ (${bestBefore}) — đọc đầy đủ để phân xử, không xoá theo /best`);
      return;
    }
    /**
     * `/best` TRẢ ORDER CỦA CHÍNH MÌNH: SỔ CHƯA BIẾT AI ĐỨNG DƯỚI
     *
     *   Lượt đọc rẻ chỉ mang về MỘT order. Khi đó là offer của mình, mọi
     *   Item Offer đối thủ thấp hơn vẫn vô hình — và khi offer mình hết hạn,
     *   engine tính Best chỉ từ Collection Offer rồi gửi Collection + 1 bước,
     *   dưới một Item Offer đang sống (đo: My 0.0064 = Collection 0.0063 +
     *   bước, trong khi Item 0.015 tồn tại). Đọc cả danh sách cho token đó,
     *   một lần, qua sổ nợ.
     */
    if (quick && list.length === 1 && self && String(list[0].maker || "").toLowerCase() === self &&
        escalateQuick("own-top")) {
      this.stats.ownTopReads = (this.stats.ownTopReads || 0) + 1;
      return;
    }
    if (quick && this.renewCheck.has(key)) { this.changed(); return; }   // chờ danh sách đầy đủ mở cổng
    // Có trạng thái ban đầu rồi thì tính ngay — không đợi sự kiện kế tiếp.
    settle();
  }

  /** Own LOST trên một danh sách ĐẦY ĐỦ (xem mergeBest). */
  pruneLostOwn(book, key, row, list, cutoff) {
    const self = this.builder ? this.builder.address.toLowerCase() : "";
    {
      const keepOwn = new Set(list.map(o => o && o.orderHash).filter(Boolean));
      const ownPrices = list.filter(o => o && self && String(o.maker || "").toLowerCase() === self).map(o => Number(o.price));
      let lost = 0;
      for (const [hash, own] of [...book.own]) {
        if (keepOwn.has(hash)) continue;
        if (String(hash).startsWith("pending:") && ownPrices.some(p => Math.abs(p - own.price) < 1e-12)) continue;
        if ((Number(own.at) || 0) > cutoff - OWN_LOST_GRACE_MS) continue;
        book.own.delete(hash);
        lost++;
      }
      if (lost) {
        book.generation++;
        this.netStat("ownLost", lost);
        this.stats.ownLost = (this.stats.ownLost || 0) + lost;
        this.log(`#${row ? row.tokenId : key} đọc đầy đủ: ${lost} offer của mình không còn trên OpenSea — gỡ khỏi sổ, tính lại`);
      }
    }
  }

  /** Kết quả đồng bộ nền: chỉ bù chỗ thiếu, KHÔNG ghi đè cái mới hơn. */
  /**
   * OWN ORDER KHÔNG BAO GIỜ ĐƯỢC "BẤT TỬ"
   *
   *   Đo trên sản phẩm: offer của mình hết hạn sau Duration nhưng bot không
   *   đặt lại cho tới khi có sự kiện khác hoặc bấm Bắt đầu. Nguyên nhân: own
   *   order đưa vào sổ từ resync ví (`fetchWalletOffers`) KHÔNG có `endTime`
   *   (endpoint không trả protocol_data) → `endTime: 0` → `ownBest` bỏ qua
   *   kiểm hạn → hàng "Đang dẫn đầu" mãi với một order đã chết. Nay mọi own
   *   order vào sổ đều có hạn: hạn thật nếu nguồn nói; không thì giả định
   *   theo Duration hiện tại của hàng (trần trên — order còn ACTIVE lúc đọc
   *   nên không thể sống lâu hơn một Duration nữa). Có hạn thì nhịp dọn 10s
   *   loại nó đúng lúc và tính lại hàng — không cần Stream, không cần REST.
   */
  ownEndTime(row, endTime, now = Date.now()) {
    const n = Number(endTime) || 0;
    if (n > 0) return n;
    const minutes = Math.max(1, Number(row && row.duration) || 15);
    return Math.floor(now / 1000) + minutes * 60;
  }

  /** Thêm/cập nhật một own order; hạn thật luôn thay hạn giả định. */
  upsertOwn(book, row, { orderHash, price, endTime, kind = "item" }, now = Date.now()) {
    if (!orderHash || !(Number(price) > 0)) return false;
    const existing = book.own.get(orderHash);
    const realEnd = Number(endTime) || 0;
    if (existing) {
      if (realEnd > 0 && (!existing.endTime || existing.assumedEnd)) {
        existing.endTime = realEnd; existing.assumedEnd = false; book.generation++;
        return true;
      }
      return false;
    }
    const ownEnd = this.ownEndTime(row, realEnd, now);
    book.own.set(orderHash, {
      orderHash, price: Number(price), maker: book.selfAddress, kind,
      quantity: 1, currency: "WETH",
      endTime: ownEnd, assumedEnd: realEnd <= 0,
      seq: 0, at: now
    });
    book.generation++;
    // An own order learned from a read or the wallet resync expires on the
    // clock like one we just posted: arm the exact expiry wake for it too
    // (1.25.12). Without it the 10s sweep was the only wake and the 5s
    // watchdog met the row first as "[ORPHAN] cần gửi mà không có ý định".
    this.noteExpiry(ownEnd);
    return true;
  }

  /**
   * OWN AUTHORITY: THEO VÍ HOẶC THEO TỪNG HÀNG
   *
   *   Một lượt resync ĐẦY ĐỦ (`ownSyncAt`) là thẩm quyền cho cả ví. Nhưng khi
   *   cursor ví lặp, lượt resync trả về TỪNG PHẦN: những hàng đã được một nguồn
   *   đầy đủ xác nhận (`book.ownReconciledAt`) không phải chờ những hàng
   *   khác — với 100 NFT, đó là khác biệt giữa gửi ngay và đứng im vài phút.
   *
   *   Một khoảng mù (`ownUnknownAt` của ví, hoặc của riêng hàng sau POST
   *   không rõ kết quả) chỉ được gỡ bởi một mốc thẩm quyền MỚI HƠN nó: resync
   *   ví đầy đủ, hàng này được soát trong lượt từng phần, POST 2xx của chính
   *   ta (`recordOwnOrder`), hay sự kiện Stream maker == ta cho hàng này —
   *   cái cuối chỉ tính khi đã từng có một lượt đồng bộ, như trước.
   */
  ownAuthoritative(key) {
    const book = this.book.get(key);
    if (!book) return false;
    /**
     * THUẦN CỤC BỘ, THEO TỪNG HÀNG (1.25.0)
     *
     *   Cần đúng hai điều: hàng đã từng được soát own ít nhất một lần (lượt
     *   đọc đầy đủ của token, hoặc resync ví — cổng lạnh), và không có POST
     *   mơ hồ nào của CHÍNH hàng này mới hơn lần soát/xác nhận gần nhất.
     *
     *   Khoảng mù Stream toàn ví (`this.ownUnknownAt`) KHÔNG còn chặn gửi:
     *   own order của app vào sổ lúc POST 2xx, nên Stream rớt không làm mất
     *   chúng; bắt mọi hàng chờ một lượt resync ví sau mỗi lần nối lại là
     *   đặt một đồng bộ mạng lên đường nóng.
     */
    const reconciled = Math.max(this.ownSyncAt || 0, book.ownReconciledAt || 0);
    if (!(reconciled > 0)) return false;
    const authorityAt = Math.max(reconciled, book.ownKnownAt || 0);
    if ((book.ownUnknownAt || 0) > authorityAt) return false;
    return true;
  }

  /**
   * Hàng nào cần một lượt đồng bộ own. Kèm dấu `ownHint` để adapter đọc
   * trước những hàng nhiều khả năng đang có own order (sổ có own, hàng vừa
   * POST không rõ kết quả) — đó là những hàng mà đọc sai giá nhất.
   */
  ownResyncTargets(onlyUnknown = true) {
    const out = [];
    for (const [key, row] of this.rows) {
      if (onlyUnknown && this.ownAuthoritative(key)) continue;
      const book = this.book.get(key);
      out.push({
        contract: row.contract, tokenId: row.tokenId, collectionSlug: row.collectionSlug || "",
        ownHint: Boolean(book && (book.own.size > 0 || (book.ownUnknownAt || 0) > 0))
      });
    }
    return out;
  }

  mergeResync(result, generation) {
    if (!result || generation !== this.recovery.generation) return;
    let merged = 0;
    const now = Date.now();
    // FENCING (1.25.2): snapshot toàn ví bắt đầu lúc `snapAt`. Hàng đã có lượt
    // soát riêng MỚI HƠN mốc đó giữ nguyên — snapshot cũ không được ghi đè.
    const snapAt = Number(result.startedAt) > 0 ? Number(result.startedAt) : now;
    const fresher = key => { const b = this.book.get(key); return Boolean(b && (b.ownReconciledAt || 0) > snapAt); };
    for (const own of result.orders || []) {
      const key = tokenKey(this.chain, own.contract, own.tokenId);
      const book = this.book.get(key);
      if (!book) continue;
      if (book.isTombstoned(own.orderHash)) continue;
      if (fresher(key)) continue;
      if (this.upsertOwn(book, this.rows.get(key), own, now)) merged++;
    }
    const touched = new Set();
    if (result.complete === true) {
      this.ownSyncAt = Math.max(this.ownSyncAt || 0, snapAt);
      this.ownUnknownAt = 0;
      for (const [key, book] of this.book.books) {
        if (fresher(key)) continue;
        // Chỉ gỡ khoảng mù đặt TRƯỚC khi snapshot bắt đầu.
        if ((book.ownUnknownAt || 0) <= snapAt) book.ownUnknownAt = 0;
        book.ownReconciledAt = Math.max(book.ownReconciledAt || 0, snapAt);
        if (this.rows.has(key)) touched.add(key);
      }
    } else if (Array.isArray(result.covered) && result.covered.length) {
      // Từng phần: mở khoá đúng những hàng đã được nguồn đầy đủ xác nhận.
      for (const ref of result.covered) {
        const [contract, tokenId] = String(ref).split(":");
        const key = tokenKey(this.chain, contract, tokenId);
        const book = this.book.get(key);
        if (!book || fresher(key)) continue;
        book.ownReconciledAt = Math.max(book.ownReconciledAt || 0, snapAt);
        if ((book.ownUnknownAt || 0) <= snapAt) book.ownUnknownAt = 0;
        if (this.rows.has(key)) touched.add(key);
      }
      this.stats.ownSyncPartial = (this.stats.ownSyncPartial || 0) + 1;
    }
    if (merged) {
      this.changed();
      this.log(`đồng bộ nền: bù ${merged} offer của mình mà Stream chưa nói`);
    }
    if (result.stats && result.complete !== true) {
      const st = result.stats;
      this.log(`đồng bộ own từng phần: soát ${result.covered?.length || 0} hàng, còn ${result.uncovered || 0} · ` +
        `ví ${st.walletPages || 0}tr · collection ${st.collectionReads || 0} · NFT ${st.nftReads || 0}`);
    }
    if (touched.size) {
      for (const key of touched) this.evaluate(key, now, null);
      this.pump();
    }
  }

  queueOwnResync(reason = "own-state") {
    if (!this.builder || typeof this.adapter.resyncOwnOrders !== "function" || this.ownSyncPending) return false;
    this.ownSyncPending = true;
    this.log(`[DIAG] own-resync scheduled reason=${reason} targets=${this.ownResyncTargets(true).length}`);
    const generation = this.recovery.generation;
    // Nền P3: hỏng/hết hạn thì lùi 30s → 60s → … → 10 phút, và chỉ khi vẫn còn
    // hàng chưa soát. Không bao giờ là một vòng 1,5s quét lại toàn ví.
    const retry = () => {
      if (this.state !== STATE.RUNNING || generation !== this.recovery.generation || this.ownSyncRetryTimer) return;
      if (!this.ownResyncTargets(true).length) { this.ownSyncBackoffMs = 0; return; }
      this.ownSyncBackoffMs = Math.min(10 * 60 * 1000, Math.max(30000, (this.ownSyncBackoffMs || 15000) * 2));
      this.ownSyncRetryTimer = setTimeout(() => {
        this.ownSyncRetryTimer = null;
        this.queueOwnResync(reason);
      }, this.ownSyncBackoffMs);
      this.ownSyncRetryTimer.unref?.();
    };
    const startedAt = Date.now();
    let scheduled = false;
    try {
      scheduled = this.recovery.push({
      // Có hàng đang WAITING vì own chưa soát → P1; còn lại là đối soát nền P3.
      kind: `own-${reason}`, // Nền P3 — KHÔNG phải cổng gửi. Ngoại lệ duy nhất: adapter không có đường
      // đọc token, khi đó resync ví LÀ chủ của SEND đang chờ ("pre-post-own") → P1.
      priority: reason === "pre-post-own" ? 1 : 3,
      run: async ({ signal }) => {
        try {
          // Chỉ những hàng CÒN mơ hồ. Lượt trước đã soát hàng nào thì hàng đó
          // không tốn thêm request nào ở lượt này.
          const targets = this.ownResyncTargets(true);
          if (!targets.length) return { orders: [], complete: true, covered: [], startedAt };
          const owner = this.builder && this.builder.address;
          if (!owner) return { orders: [], complete: false, error: "no-signer", startedAt };
          const out = await this.adapter.resyncOwnOrders(owner, { signal, targets });
          return out && typeof out === "object" ? { ...out, startedAt } : out;
        }
        catch (error) { return { orders: [], complete: false, error: String(error?.message || error), startedAt }; }
      },
      onResult: (result, gen) => {
        this.ownSyncPending = false;
        this.log(`[DIAG] own-resync result reason=${reason} complete=${result?.complete === true} ` +
          `covered=${result?.covered?.length || 0} uncovered=${result?.uncovered || 0} ` +
          `error=${result?.error || "none"}`);
        if (result) this.mergeResync(result, gen);
        if (result?.complete !== true) retry(); else this.ownSyncBackoffMs = 0;
      },
      onDrop: () => { this.ownSyncPending = false; this.log(`[DIAG] own-resync dropped reason=${reason}`); retry(); }
      });
    } catch (error) {
      this.ownSyncPending = false;
      this.log(`[DIAG] own-resync schedule error reason=${reason} error=${String(error?.message || error)}`);
      retry();
      return false;
    }
    // RecoveryPlane.push() can refuse a job while stopped. Do not leave the
    // wallet permanently marked pending when no recovery job exists: that is
    // the lost-wakeup state which strands rows behind ownAuthoritative=false.
    if (!scheduled) {
      this.ownSyncPending = false;
      this.log(`[DIAG] own-resync dropped-before-queue reason=${reason}`);
      retry();
    }
    return scheduled;
  }

  // ================================================================

  emit() {
    try { this.onUpdate(this.getState()); } catch { /* UI không được làm hỏng engine */ }
  }

  /**
   * Ghi order vừa được OpenSea nhận vào nhóm own của sổ. Xem `submitOne`.
   *
   *   Không có hash trong phản hồi thì dùng một hash tạm; khi Stream/resync
   *   mang hash thật cùng giá về, hash tạm được gỡ để luồng Cancel tìm đúng
   *   order (`resolveMyOffer` trả hash).
   */
  recordOwnOrder(book, orderHash, price, durationMinutes, signedEndTime = 0, correlationId = "") {
    if (!book || !(Number(price) > 0)) return;
    const now = Date.now();
    book.ownKnownAt = now;
    const hash = orderHash || `pending:${book.key}:${now}`;
    // The SIGNED endTime is the order's real expiry (1.25.10). Rebuilding it
    // from "now + duration" after the POST returned placed the local expiry
    // later than the order by the build/sign/network time, so the renew woke
    // late. The reconstruction stays only as a fallback.
    const exact = Number(signedEndTime);
    const endTime = exact > 0 ? Math.floor(exact) : Math.floor(now / 1000) + Math.max(1, Number(durationMinutes) || 15) * 60;
    /**
     * CÙNG HASH ĐÃ CÓ → CẬP NHẬT, KHÔNG BỎ QUA (1.25.2)
     *
     *   Bản cũ `return` khi hash đã có: nếu phản hồi mang lại một hash đã biết,
     *   giá MỚI không bao giờ vào sổ, mine vẫn là giá cũ, decide nói SEND lại —
     *   và từ khi flight-settled tự đánh thức hàng, đó là một vòng gửi chặt
     *   (test: 699.203 POST cho 500 lượt vượt giá). Bản ghi phải phản ánh order
     *   vừa được nhận.
     */
    if (book.own.has(hash)) {
      const o = book.own.get(hash);
      o.price = Number(price); o.endTime = endTime; o.assumedEnd = false; o.at = now;
      book.generation++;
      this.noteExpiry(endTime);
      this.changed();
      productionTrace.record("own_update", { correlationId }, {
        chain: this.chain, collection: book.collectionSlug, tokenId: book.tokenId,
        status: "applied", mine: book.ownBest(now).price
      });
      return;
    }
    // Hash thật vừa tới cho giá này: gỡ bản tạm cùng giá.
    if (orderHash) {
      for (const [h, o] of book.own) {
        if (h.startsWith("pending:") && Math.abs(o.price - Number(price)) < 1e-12) book.own.delete(h);
      }
    }
    book.own.set(hash, {
      orderHash: hash, price: Number(price), maker: book.selfAddress, kind: "item",
      quantity: 1, currency: "WETH",
      // Hạn = endTime ĐÃ KÝ (exact); hẹn giờ hết hạn đánh thức đúng lúc đó.
      endTime,
      assumedEnd: false,
      seq: 0, at: now
    });
    book.generation++;
    this.noteExpiry(endTime);
    this.changed();
    productionTrace.record("own_update", { correlationId }, {
      chain: this.chain, collection: book.collectionSlug, tokenId: book.tokenId,
      status: "applied", mine: book.ownBest(now).price
    });
  }

  /** Tín hiệu "có gì đó đổi" cho giao diện. Rẻ; người nhận tự gộp. */
  changed() {
    this.lastActivityAt = Date.now();
    try { this.onChange(); } catch { /* UI không được làm hỏng engine */ }
  }

  /**
   * Đổi trạng thái ý định VÀ báo cho giao diện. Mọi mốc trong `submitOne`
   * đi qua đây: "đang ký", "đang chờ quota", "đang gửi", "lỗi" là những thứ
   * người dùng nhìn thấy ở cột Tiến trình — chúng phải hiện ĐÚNG LÚC, không
   * phải ở lần đẩy bảng kế tiếp.
   */
  setIntent(key, state, extra) {
    this.intents.setState(key, state, extra);
    this.changed();
  }

  /**
   * BẤT BIẾN LIVENESS CỦA MỘT HÀNG (1.25.1)
   *
   *   Hàng Running luôn phải ở ĐÚNG MỘT trạng thái có đường tiến: dẫn đầu
   *   thật / intent đang sống / flight / WAITING có owner / lượt đọc (đầu,
   *   renew, gate) có request hoặc hẹn giờ / thử lại có mốc / quá Max /
   *   lỗi terminal. Gọi ở mọi transition quan trọng; idempotent, cục bộ,
   *   không tạo việc trùng. Watchdog chỉ gọi lại chính hàm này.
   * @returns {string} trạng thái tiến đã xác nhận
   */
  ensureRowProgress(key, why = "") {
    if (this.state !== STATE.RUNNING) return "not-running";
    const row = this.rows.get(key);
    if (!row || !row.running) return "row-stopped";
    if (this.intents.isInFlight(key)) return "flight";
    const now = Date.now();
    if (this.awaitingFirstRead.has(key) || this.renewCheck.has(key)) {
      const pending = this.pendingReads.get(key);
      const covered = this.hydrating.has(key) || this.firstReadTimers.has(key) || Boolean(pending && pending.timer);
      if (!covered) {
        this.stats.progressReadRepairs = (this.stats.progressReadRepairs || 0) + 1;
        this.queueRead(row, { reason: this.renewCheck.has(key) ? "renew" : "gate-repair", authoritative: false,
          readAt: now, firstRead: !this.renewCheck.has(key), attempt: 1 });
      }
      return "read";
    }
    const it = this.intents.get(key);
    if (it && (it.state === INTENT.READY || it.state === INTENT.GRANTING ||
               it.state === INTENT.BUILDING || it.state === INTENT.SENDING)) return "intent";
    if (it && it.state === INTENT.RETRY) {
      if (!(row.retryAt > 0)) row.retryAt = now;   // RETRY không mốc = không ai đánh thức
      return "retry";
    }
    if (row.retryAt && row.retryAt > now) return "retry";
    if (it && it.state === INTENT.WAITING) {
      if (!this.templateReady(key)) { this.ensureTemplate(key, "progress"); return "waiting-template"; }
      if (!this.ownAuthoritative(key)) {
        // Chủ của chờ đợi = lượt đọc đầy đủ CỦA HÀNG NÀY (đang bay/nợ/hẹn giờ).
        const pr = this.pendingReads.get(key);
        if (!this.hydrating.has(key) && !(pr && pr.timer)) {
          this.deferForOwnSync(key, row, it.traceId, it.deferredReason || "own-state-unknown");
        }
        return "waiting-own";
      }
      this.evaluate(key, now, null);           // phụ thuộc đã sẵn: đánh thức NGAY
      this.pump();
      return "woken";
    }
    if (row.lowBalance) return "low-balance";
    const book = this.book.get(key);
    if (!book) return "no-book";
    if (it && it.state === INTENT.FAILED && it.generation === book.generation) return "terminal";
    const verdict = decide({ best: book.effectiveBest(now).price, mine: book.ownBest(now).price,
      minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step });
    if (verdict.status !== STATUS.SEND) return String(verdict.status).toLowerCase();
    this.evaluate(key, now, null);
    this.pump();
    const after = this.intents.get(key);
    return after && after.state !== INTENT.IDLE ? "intent-created" : "evaluated";
  }

  /**
   * ĐÁNH THỨC HÀNG — IDEMPOTENT, GỘP TRONG MỘT MICROTASK (1.25.2)
   *
   *   Mỗi hàng tối đa MỘT lần đánh thức đang chờ; nhiều transition cùng lượt
   *   gộp lại. Khi chạy: ensureRowProgress trên trạng thái MỚI NHẤT. Dùng ở
   *   mọi transition có thể đổi verdict mà không đi qua evaluate (config,
   *   kết quả async cũ bị bỏ, ...).
   */
  scheduleRowWake(key, why = "") {
    if (this.state !== STATE.RUNNING || !this.rows.has(key)) return false;
    if (this.pendingWakes.has(key)) return false;
    this.pendingWakes.set(key, why);
    if (!this.wakeScheduled) {
      this.wakeScheduled = true;
      queueMicrotask(() => this.drainWakes());
    }
    return true;
  }

  drainWakes() {
    this.wakeScheduled = false;
    if (!this.pendingWakes.size) return;
    const list = [...this.pendingWakes];
    this.pendingWakes.clear();
    for (const [key, why] of list) {
      try { this.ensureRowProgress(key, why); } catch (error) { this.log(`wake ${why} lỗi: ${error.message}`); }
    }
  }

  /** Ghi ý định SEND nhưng giữ ở WAITING vì một phụ thuộc cục bộ. */
  holdIntent(key, verdict, best, mine, book, at, reason) {
    const cur = this.intents.get(key);
    if (cur && cur.state === INTENT.WAITING && cur.deferredReason === reason &&
        Math.abs(Number(cur.target) - Number(verdict.target)) < 1e-12) return;
    if (this.intents.isInFlight(key)) return;
    this.intents.set(key, {
      target: verdict.target, best: best.price, mine: mine.price,
      generation: book.generation, reason: verdict.reason, at
    });
    this.intents.setState(key, INTENT.WAITING, { deferredReason: reason, waitingSince: Date.now() });
    this.changed();
  }

  /**
   * Bộ đếm mạng/ghi GỘP (không log từng request). Cửa sổ 60 giây xoay vòng,
   * cộng dồn tổng; quota wait giữ 256 mẫu gần nhất cho p50/p95/p99.
   */
  netStat(name, value = 1, domain = "") {
    const n = this.net || (this.net = { windowStart: Date.now(), window: {}, total: {}, quotaWaits: [], perKey: {} });
    const now = Date.now();
    if (now - n.windowStart >= 60000) { n.last = n.window; n.window = {}; n.windowStart = now; }
    if (name === "quotaWait") {
      n.quotaWaits.push(value);
      if (n.quotaWaits.length > 256) n.quotaWaits.shift();
      return;
    }
    n.window[name] = (n.window[name] || 0) + value;
    n.total[name] = (n.total[name] || 0) + value;
    if (domain) {
      let k = n.perKey[domain];
      if (!k) {
        const domains = Object.keys(n.perKey);
        if (domains.length >= 8) delete n.perKey[domains[0]];
        k = n.perKey[domain] = {};
      }
      k[name] = (k[name] || 0) + value;
    }
  }

  netDiagnostics() {
    const n = this.net || { window: {}, total: {}, quotaWaits: [], perKey: {} };
    const sorted = [...n.quotaWaits].sort((a, b) => a - b);
    const pct = p => sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor(p * sorted.length))] : 0;
    const census = this.intents.census();
    return {
      lastMinute: n.last || n.window, total: n.total, perKey: n.perKey,
      quotaWaitMs: { p50: pct(0.5), p95: pct(0.95), p99: pct(0.99), samples: sorted.length },
      writeQueue: { ready: this.intents.ready().length, acquiring: this.acquiring, inFlight: census.inFlight,
        waiting: census.byState.WAITING || 0 },
      recovery: this.recovery.census(),
      reads: { ...this.readStats },
      watchdog: { orphans: this.stats.watchdogOrphans || 0, intentLost: this.stats.intentLost || 0,
        retryOverdue: this.stats.watchdogRetryOverdue || 0, waitingRepairs: this.stats.watchdogWaitingRepairs || 0,
        releases: this.stats.watchdogReleases || 0 },
      stream: { gaps: this.stats.streamGaps || 0, degradedEntries: this.stats.degradedEntries || 0, degraded: this.degraded },
      rows: this.rowCensus()
    };
  }

  /** Readiness per row, local only (cheap): how many are warm / waiting on what. */
  rowCensus() {
    const out = { running: 0, warm: 0, waitingBest: 0, waitingRenew: 0, waitingTemplate: 0, waitingOwn: 0, sending: 0,
      waitOwnMaxAgeMs: 0, waitTemplateMaxAgeMs: 0, waitOwnNoOwner: 0 };
    const nowMs = Date.now();
    for (const [key, row] of this.rows) {
      if (!row.running) continue;
      out.running++;
      if (this.renewCheck.has(key)) { out.waitingRenew++; continue; }
      if (this.awaitingFirstRead.has(key)) { out.waitingBest++; continue; }
      out.warm++;
      const it = this.intents.get(key);
      if (!it) continue;
      if (it.state === INTENT.WAITING) {
        const age = nowMs - (it.waitingSince || it.stateChangedAt || nowMs);
        if (/template/.test(String(it.deferredReason || ""))) {
          out.waitingTemplate++; out.waitTemplateMaxAgeMs = Math.max(out.waitTemplateMaxAgeMs, age);
        } else {
          out.waitingOwn++; out.waitOwnMaxAgeMs = Math.max(out.waitOwnMaxAgeMs, age);
          const pr = this.pendingReads.get(key);
          if (!this.hydrating.has(key) && !(pr && pr.timer) && !this.pendingWakes.has(key)) out.waitOwnNoOwner++;
        }
      } else if (it.state !== INTENT.IDLE && it.state !== INTENT.DONE && it.state !== INTENT.FAILED) out.sending++;
    }
    return out;
  }

  /** Nhịp thử lại + watchdog. Idempotent: Start hai lần không nhân đôi. */
  startLoops() {
    this.stopLoops();
    this.retryTimer = setInterval(() => { try { this.sweepRetries(); } catch (e) { this.log(`retry sweep lỗi: ${e.message}`); } }, RETRY_SWEEP_MS);
    if (this.retryTimer.unref) this.retryTimer.unref();
    this.watchdogTimer = setInterval(() => { try { this.watchdog(); } catch (e) { this.log(`watchdog lỗi: ${e.message}`); } }, WATCHDOG_MS);
    if (this.watchdogTimer.unref) this.watchdogTimer.unref();
  }

  stopLoops() {
    if (this.retryTimer) clearInterval(this.retryTimer);
    if (this.watchdogTimer) clearInterval(this.watchdogTimer);
    this.retryTimer = null;
    this.watchdogTimer = null;
    this.cancelBalanceTimer();
    this.balanceProbeFailures = 0;
    this.balanceProbeInFlightAt = 0;
  }

  /** Xem `expiryTimer` ở constructor. */
  /**
   * HẾT HẠN LÀ MỘT TRANSITION, KHÔNG CHỈ LÀ MỘT NHỊP QUÉT (1.25.2)
   *
   *   effectiveBest(now)/ownBest(now) bỏ order hết hạn THEO ĐỒNG HỒ, nên
   *   verdict có thể thành SEND mà không có sự kiện nào. Nhịp quét 10s tới
   *   sau watchdog 5s → "[ORPHAN] cần gửi mà không có ý định" rồi watchdog
   *   gửi (production 1.25.1: noIntent=13/phút). Nay một hẹn giờ duy nhất trỏ
   *   ĐÚNG order sớm hết hạn nhất; khi tới giờ thì quét và hẹn lại.
   */
  noteExpiry(endTimeSec) {
    const t = Number(endTimeSec) * 1000;
    if (!(t > 0) || this.state !== STATE.RUNNING) return;
    if (this.expiryWakeAt && this.expiryWakeAt <= t) return;
    this.armExpiryWake(t);
  }

  armExpiryWake(t) {
    if (this.expiryWakeTimer) clearTimeout(this.expiryWakeTimer);
    this.expiryWakeAt = t;
    const delay = Math.min(2 ** 31 - 1, Math.max(20, t - Date.now() + 5));
    this.expiryWakeTimer = setTimeout(() => {
      this.expiryWakeTimer = null;
      this.expiryWakeAt = 0;
      if (this.state !== STATE.RUNNING) return;
      this.stats.expiryWakes = (this.stats.expiryWakes || 0) + 1;
      this.sweepExpired(Date.now());
      this.armNextExpiry();
    }, delay);
    this.expiryWakeTimer.unref?.();
  }

  /** Hẹn lại cho order kế tiếp hết hạn trong mọi sổ đang theo dõi. */
  armNextExpiry(now = Date.now()) {
    const sec = Math.floor(now / 1000);
    let next = 0;
    for (const [key, book] of this.book.books) {
      if (!this.rows.has(key)) continue;
      for (const group of [book.own, book.item, book.collection, book.trait]) {
        if (!group) continue;
        for (const o of group.values()) {
          if (o.endTime > sec && (!next || o.endTime < next)) next = o.endTime;
        }
      }
    }
    if (next) this.noteExpiry(next);
  }

  startExpirySweep() {
    this.stopExpirySweep();
    this.armNextExpiry();
    const timer = setInterval(() => this.sweepExpired(), EXPIRY_SWEEP_MS);
    if (timer.unref) timer.unref();
    this.expiryTimer = timer;
  }

  stopExpirySweep() {
    if (this.expiryTimer) clearInterval(this.expiryTimer);
    this.expiryTimer = null;
    if (this.expiryWakeTimer) clearTimeout(this.expiryWakeTimer);
    this.expiryWakeTimer = null;
    this.expiryWakeAt = 0;
  }

  /**
   * Bỏ lệnh đã quá hạn khỏi mọi sổ và tính lại ĐÚNG những hàng mất lệnh.
   *
   * @returns {number} số hàng được tính lại
   */
  sweepExpired(now = Date.now()) {
    if (this.state !== STATE.RUNNING) return 0;
    let recomputed = 0;
    for (const [key, book] of this.book.books) {
      const ownBefore = book.ownBest(now - EXPIRY_SWEEP_MS).price;   // own còn sống ở nhịp trước?
      if (!book.pruneExpired(now)) continue;
      const row = this.rows.get(key);
      if (ownBefore > 0 && book.ownBest(now).price <= 0 && row) {
        // Offer của mình vừa hết hạn: đây là một thay đổi trạng thái thật —
        // hàng về "chưa có offer". Đặt lại CHỈ khi sổ đủ thẩm quyền; không
        // thì đóng cổng và xác nhận lại Best của đúng token này trước.
        this.stats.ownExpired = (this.stats.ownExpired || 0) + 1;
        this.netStat("renewExpired", 1);
        row.renewPending = true;
        if (row.running && this.holdRenewal(key, row, book, now)) { recomputed++; continue; }
        this.log(`#${row.tokenId} offer của mình đã hết hạn — sổ còn tươi, tính lại`);
      }
      this.evaluate(key, now, null);
      this.ensureRowProgress(key, "own-expired");
      recomputed++;
    }
    if (recomputed) {
      this.stats.expiredSwept = (this.stats.expiredSwept || 0) + recomputed;
      this.changed();
      this.pump();
    }
    return recomputed;
  }

  getState() {
    return {
      chain: this.chain, state: this.state, epoch: this.epoch,
      rows: this.rows.size,
      book: this.book.census(),
      intents: this.intents.census(),
      traits: this.traits ? this.traits.census() : null,
      recovery: this.recovery.census(),
      http: this.http.census(),
      quota: this.quota ? this.quota.status() : null,
      stats: { ...this.stats },
      responseShape: this.lastResponseShape || null,
      lastStreamEventAt: this.lastStreamEventAt,
      lastActivityAt: this.lastActivityAt,
      reads: { ...this.readStats, awaitingFirstRead: this.awaitingFirstRead.size },
      scheduler: { pending: this.intents.ready().length, acquiring: this.acquiring,
        inFlight: this.intents.census().inFlight, served: this.servedAt.size },
      dispatcher: { flights: this.flights.size, pendingReads: this.pendingReads.size, renewCheck: this.renewCheck.size,
        templatesMissing: [...this.rows.keys()].filter(k => !this.templateReady(k)).length,
        lastGrantAt: this.lastGrantAt, lastPostAt: this.lastPostAt, lastRepairAt: this.lastRepairAt }
    };
  }

  ownDiagnostic(key) {
    const book = this.book.get(key);
    const covered = this.ownAuthoritative(key);
    return {
      covered,
      uncovered: covered ? 0 : 1,
      ownSyncPending: Boolean(this.ownSyncPending),
      ownSyncAt: this.ownSyncAt || 0,
      ownReconciledAt: book?.ownReconciledAt || 0,
      ownKnownAt: book?.ownKnownAt || 0,
      ownUnknownAt: Math.max(this.ownUnknownAt || 0, book?.ownUnknownAt || 0)
    };
  }

  diagnosticSnapshot() {
    const now = Date.now();
    const census = this.intents.census();
    const states = {};
    for (const intent of this.intents.intents.values()) {
      states[intent.state] = (states[intent.state] || 0) + 1;
    }
    const rows = [...this.rows.values()].map(row => {
      const key = row.key;
      const book = this.book.get(key);
      const intent = this.intents.get(key);
      const flight = this.flights.get(key);
      const template = this.templateState.get(key);
      const best = book?.effectiveBest(now);
      const mine = book?.ownBest(now);
      const own = this.ownDiagnostic(key);
      const verdict = book ? decide({ best: best.price, mine: mine.price,
        minPrice: row.minPrice, maxPrice: row.maxPrice, step: row.step }) : { status: STATUS.NO_TARGET };
      const topicRepairAt = this.topicRepairAt.get(key) || 0;
      let topicHealth = "UNKNOWN";
      try { if (this.streamHealth) topicHealth = String(this.streamHealth(row.collectionSlug)); } catch { topicHealth = "FAILED"; }
      const authorityAfterTopicLoss = Boolean(topicRepairAt && Number(book?.lastFullReadAt) > topicRepairAt);
      const topicReadOwner = this.pendingReads.has(key) || this.hydrating.has(key) || this.recoveryCandidates.has(key);
      const inFlight = this.intents.isInFlight(key) || Boolean(flight);
      let progressState = "HEALTHY";
      if (!row.running) progressState = "PAUSED";
      else if (inFlight || [INTENT.GRANTING, INTENT.BUILDING, INTENT.SENDING].includes(intent?.state)) progressState = "SENDING";
      else if (intent?.state === INTENT.READY) progressState = "READY";
      else if (intent?.state === INTENT.RETRY || row.retryAt > now) progressState = "WAITING_RETRY";
      else if (intent?.state === INTENT.WAITING) {
        progressState = /template/i.test(String(intent.deferredReason || "")) ? "WAITING_TEMPLATE" : "WAITING_OWN";
      } else if (verdict.status === STATUS.SEND) {
        const topicBlocked = row.sendBlockedBy?.gate === "stream-topic" ||
          (topicRepairAt && !authorityAfterTopicLoss) || (topicHealth !== "HEALTHY" && topicHealth !== "UNKNOWN");
        if (topicBlocked && (topicReadOwner || authorityAfterTopicLoss)) progressState = "WAITING_STREAM_RECOVERY";
        else if (row.sendBlockedBy?.gate === "first-read" || row.sendBlockedBy?.gate === "shadow-authority-required") progressState = "WAITING_READ";
        else if (row.sendBlockedBy?.gate === "template") progressState = "WAITING_TEMPLATE";
        else if (row.sendBlockedBy?.gate === "low-balance") progressState = "WAITING_BALANCE";
        else progressState = "ORPHAN";
      } else if (verdict.status === STATUS.ABOVE_MAX) progressState = "MAX_BLOCKED";
      else if (this.awaitingFirstRead.has(key) || this.renewCheck.has(key) || this.pendingReads.has(key)) progressState = "WAITING_READ";
      return {
        token: row.tokenId, key, url: row.url, running: row.running,
        bestOffer: best?.price || 0, myOffer: mine?.price || 0,
        progressState, sendBlockedBy: row.sendBlockedBy?.gate || null,
        topicRecovery: topicRepairAt ? { outageAt: topicRepairAt, health: topicHealth,
          authorityAfterOutage: authorityAfterTopicLoss, owner: topicReadOwner ? "read-or-queue" : authorityAfterTopicLoss ? "fresh-authority" : "missing" } : null,
        desiredAction: intent?.target ? "SEND" : (best?.price || mine?.price ? "WATCH" : "UNKNOWN"),
        intent: intent ? { id: intent.traceId || null, state: intent.state, ageMs: intent.updatedAt ? now - intent.updatedAt : null,
          target: intent.target || 0, retryReason: intent.lastError || "", retryCount: intent.requeues || row.failures || 0,
          lastTransition: intent.stateChangedAt || 0, lastError: intent.lastError || "" } : null,
        ownAuthoritative: own.covered, ownSyncPending: own.ownSyncPending, ownSyncAt: own.ownSyncAt,
        ownCoverage: own, awaitingFirstRead: this.awaitingFirstRead.has(key), renewCheck: this.renewCheck.has(key),
        templateReady: this.templateReady(key), template: template ? { state: template.state, ageMs: template.retryAt ? Math.max(0, now - (template.retryAt || now)) : null, attempts: template.attempts || 0, lastError: template.lastError || "" } : null,
        quota: this.quota?.status?.() || null, acquiring: this.intents.isInFlight(key) ? 1 : 0,
        flight: flight ? { id: flight.id, stage: flight.stage, ageMs: now - flight.since } : null,
        paused: !row.running, suppressed: Boolean(row.suppressed), balanceGate: Boolean(row.lowBalance),
        licenceGate: Boolean(row.licenceRefusedLogged), retryAt: row.retryAt || 0, lastError: row.lastError || ""
      };
    });
    return { generatedAt: now, chain: this.chain, state: this.state, rows,
      intentCensus: { ...census, byState: states },
      ownSync: { pending: Boolean(this.ownSyncPending), lastSuccess: this.ownSyncAt || 0,
        covered: rows.filter(r => r.ownAuthoritative).length, uncovered: rows.filter(r => !r.ownAuthoritative).length },
      broker: this.quota?.status?.() || null,
      engine: { running: this.state === STATE.RUNNING, scheduler: this.getState().scheduler,
        progress: { permanentOrphans: rows.filter(r => r.progressState === "ORPHAN").length,
          waitingStreamRecovery: rows.filter(r => r.progressState === "WAITING_STREAM_RECOVERY").length,
          paused: rows.filter(r => r.progressState === "PAUSED").length },
        recovery: this.recovery.census(),
        pathB: { queuedCandidates: this.recoveryCandidates.size, sweepActive: this.recoverySweepActive,
          queuedTotal: this.stats.recoveryCandidatesQueued || 0,
          dedupedTotal: this.stats.recoveryCandidatesDeduped || 0,
          maxQueueDepth: this.stats.recoveryCandidateMaxDepth || 0,
          candidateTimer: Boolean(this.recoveryCandidateTimer),
          lastFullSweepAt: this.lastFullRecoveryAt || 0 },
        pendingWakeups: this.retryTimer ? 1 : 0, activeJobs: this.flights.size }
    };
  }

  latencyReport() { return this.metrics.report(); }
  violations() { return this.metrics.violations(); }
}

/** Stream phải hỏng liên tục bấy lâu mới vào suy giảm (test có thể rút ngắn). */
OfferItemEngineV2.DEGRADED_GRACE_MS = DEGRADED_GRACE_MS;

module.exports = { OfferItemEngineV2, STATE };
