"use strict";

/**
 * pricing.js — giá nào OpenSea nhận, và giá nào đáng gửi.
 *
 * ĐÂY LÀ LOGIC ĐƯỢC CHUYỂN SANG, KHÔNG PHẢI VIẾT LẠI
 *
 *   Quy tắc dưới đây đã chạy trong production và đã được kiểm bằng test. Nó
 *   nằm trong `engine.js`, và `engine.js` thì bị gỡ khỏi production. Viết lại
 *   toán giá từ đầu để "cho mới" là cách nhanh nhất đánh mất thứ duy nhất
 *   không được sai: một offer sai precision bị OpenSea trả 400, và một hàng
 *   cứ tính lại đúng con số đó thì cứ bị từ chối mãi.
 *
 *   Nên nó được CHUYỂN nguyên vẹn, và được xác minh lại với tài liệu hiện
 *   hành trước khi chuyển.
 *
 * XÁC MINH LẠI VỚI TÀI LIỆU HIỆN HÀNH (09/2026)
 *
 *   docs.opensea.io/changelog/api-changes — "Offers in ETH/WETH will follow a
 *   tiered precision system based on price range":
 *
 *     0.0001-0.0999 ETH   4 chữ số thập phân
 *     0.100-0.999 ETH     3 chữ số thập phân
 *     1.00+ ETH           2 chữ số thập phân
 *
 *   Khớp đúng ba bậc dưới đây.
 *
 *   Có một changelog CŨ HƠN (bidding-precision-decision) nói một quy tắc duy
 *   nhất "làm tròn tới 0.0001". Nó đã bị thay bởi hệ ba bậc ở trên. Ghi lại ở
 *   đây vì đọc nhầm bản cũ sẽ dẫn tới gửi 0.1008 — bốn chữ số thập phân, hợp
 *   lệ theo bản cũ, và bị bậc 0.100-0.999 từ chối.
 */

/** Ranh giới đổi bậc, và bước nhỏ nhất mỗi bậc cho phép. */
const PRICE_TIERS = Object.freeze([
  { from: 1, grid: 0.01 },
  { from: 0.1, grid: 0.001 },
  { from: 0, grid: 0.0001 }
]);

/** Dưới mức này thì Step của người dùng được dùng nguyên. Xem nextTargetAbove. */
const GRID_SWITCH = 0.1;

/** Bước nhỏ nhất mà bậc 0.1-0.999 chấp nhận. */
const COARSE_STEP = 0.001;

/**
 * Làm tròn về 8 chữ số.
 *
 * Số thực nhị phân không biểu diễn được 0.0001, nên phép cộng để lại đuôi kiểu
 * 0.008199999999999999. Gửi nguyên chuỗi đó đi là một lỗi 400 vì thừa chữ số,
 * chứ không phải vì sai giá.
 */
function roundPrice(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  return Math.round(n * 1e8) / 1e8;
}

/** Bước OpenSea chấp nhận cho một mức giá. */
function gridFor(value) {
  const n = Number(value) || 0;
  for (const tier of PRICE_TIERS) {
    if (n >= tier.from - 1e-12) return tier.grid;
  }
  return PRICE_TIERS[PRICE_TIERS.length - 1].grid;
}

/**
 * Một giá OpenSea thật sự nhận.
 *
 * NÂNG lên đúng lưới, không bao giờ hạ: hạ xuống có thể đưa offer thấp hơn
 * chính cái giá nó được dựng ra để vượt.
 */
function normalizeTarget(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return 0;
  const grid = gridFor(n);
  // 1e-9 hút nhiễu số thực: 0.101 / 0.001 ra 100.99999999999999, không có nó
  // thì một giá đã đúng lưới bị đẩy lên nguyên một bậc.
  return roundPrice(Math.ceil(n / grid - 1e-9) * grid);
}

/** Giá này có nằm đúng lưới của bậc nó không? */
function isValidPrecision(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return false;
  const grid = gridFor(n);
  const slots = n / grid;
  return Math.abs(slots - Math.round(slots)) < 1e-6;
}

/**
 * Giá kế tiếp đáng gửi, cao hơn `best`.
 *
 * Dưới 0.1 thì đúng bằng Best + Step người dùng đặt — TRỪ khi bước đó vượt qua
 * ranh giới bậc, thì dừng ĐÚNG TẠI ranh giới. Best 0.0997 với Step 0.001 cho
 * 0.1000 chứ không phải 0.1007: 0.1007 không phải giá OpenSea nhận, và nó còn
 * nhảy qua cả một nấc thang mà chẳng được gì.
 *
 * Từ 0.1 trở lên thì Step người dùng không được dùng: lưới của bậc đã là mức
 * tăng nhỏ nhất hợp lệ, nên câu trả lời là ô kế tiếp ngay trên Best.
 */
function nextTargetAbove(best, step) {
  const b = Number(best) || 0;
  return normalizeTarget(roundPrice(b + effectiveStepFor(b, step)));
}

/**
 * BƯỚC TĂNG THỰC TẾ — THEO BEST HIỆN TẠI, KHÔNG THEO Ô STEP
 *
 *   Luật Offer Item: Best < 0.1 → 0.0001; Best >= 0.1 → 0.001 (và >= 1 → 0.01,
 *   bậc precision của OpenSea). Ô Step người dùng nhập chỉ là cấu hình mặc
 *   định; khi đã biết Best thì bước tăng là bước của bậc giá Best đang ở, và
 *   nó đổi cả hai chiều khi Best đi qua ranh giới — 0.105 bị huỷ, Best về
 *   0.0987 thì bước về 0.0001, không giữ 0.001 cũ.
 */
function effectiveStepFor(best, userStep = 0) {
  const auto = Number(best) >= 0.1 ? 0.001 : 0.0001;
  return Math.max(Number(userStep) || 0, auto);
}

/**
 * Max của người dùng đưa về lưới giá — LUÔN LÀM TRÒN XUỐNG.
 *
 *   0.1025 nằm ngoài bậc 0.001. Làm tròn lên 0.103 là tự nâng giới hạn tiền
 *   người dùng cho phép; xuống 0.102 thì không. Một Max ngoài lưới không
 *   bao giờ được gửi nguyên trạng, và giao diện hiện lại đúng giá đã chỉnh.
 */
function normalizeMaxDown(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return 0;
  const grid = gridFor(n);
  return roundPrice(Math.floor(n / grid + 1e-9) * grid);
}

module.exports = {
  PRICE_TIERS, GRID_SWITCH, COARSE_STEP,
  roundPrice, gridFor, normalizeTarget, isValidPrecision, nextTargetAbove,
  effectiveStepFor, normalizeMaxDown
};
