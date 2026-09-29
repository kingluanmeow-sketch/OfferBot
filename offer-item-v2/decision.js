"use strict";

/**
 * decision.js — sổ nói gì thì nên gửi giá nào.
 *
 * Hàm thuần: vào là trạng thái, ra là một quyết định. Không mạng, không thời
 * gian trôi, không phụ thuộc thứ tự gọi. Đó là điều kiện để nó kiểm được bằng
 * test bảng, và toán giá là chỗ duy nhất trong V2 mà một lỗi lặng lẽ sẽ tiêu
 * tiền thật thay vì chỉ chậm.
 */

const { normalizeTarget, nextTargetAbove, isValidPrecision, gridFor,
  roundPrice, effectiveStepFor, normalizeMaxDown } = require("./pricing");

const STATUS = Object.freeze({
  SEND: "SEND",                 // gửi đi, có target
  ON_TOP: "ON_TOP",             // đang dẫn đầu, không làm gì
  ABOVE_MAX: "ABOVE_MAX",       // cần vượt nhưng vượt trần
  BAD_CONFIG: "BAD_CONFIG",     // cấu hình không tạo được giá hợp lệ
  NO_TARGET: "NO_TARGET"        // không có gì để làm
});

/**
 * @param {object} row
 * @param {number} row.best      giá cao nhất của NGƯỜI KHÁC đang áp
 * @param {number} row.mine      giá của chính mình đang treo, 0 nếu không có
 * @param {number} row.minPrice
 * @param {number} row.maxPrice  trần của người dùng; được đưa về lưới (XUỐNG)
 * @param {number} row.step      cấu hình mặc định — chỉ kiểm hợp lệ, KHÔNG
 *                               quyết định bước tăng (xem effectiveStep)
 * @returns {{status:string, target:number, best:number, mine:number,
 *            reason:string, canSend:boolean, effectiveStep:number, max:number}}
 */
function decide(row) {
  const min = Number(row.minPrice);
  // MAX LUÔN Ở TRÊN LƯỚI GIÁ, LÀM TRÒN XUỐNG. 0.1025 → 0.102: một trần ngoài
  // lưới không bao giờ được nâng lên 0.103 — đó là tự tăng tiền người dùng.
  const maxRaw = Number(row.maxPrice);
  const max = Number.isFinite(maxRaw) && maxRaw > 0 ? normalizeMaxDown(maxRaw) : maxRaw;
  const configuredStep = Number(row.step);
  const best = Number(row.best) || 0;
  const mine = Number(row.mine) || 0;

  /**
   * BƯỚC TĂNG THỰC TẾ THEO BEST, KHÔNG THEO Ô STEP
   *
   *   Luật Offer Item: Best < 0.1 → 0.0001; Best >= 0.1 → 0.001 (>= 1 → 0.01).
   *   Ô Step là cấu hình mặc định, không phải bước gửi: Best 0.102 với ô Step
   *   0.0001 KHÔNG sinh 0.1021 — target là 0.103. Bước lấy theo BEST ĐỐI THỦ,
   *   không theo offer của mình (mình 0.09, đối thủ 0.102 → bước 0.001), và
   *   đổi cả hai chiều khi Best đi qua 0.1.
   */
  const step = effectiveStepFor(best, configuredStep);

  const out = (status, target, reason) => ({
    status, target: target || 0, best, mine, reason: reason || "", min, max,
    effectiveStep: step,
    canSend: status === STATUS.SEND
  });

  if (!Number.isFinite(min) || !Number.isFinite(max) || !Number.isFinite(configuredStep)) {
    return out(STATUS.BAD_CONFIG, 0, "Min/Max/Step phải là số.");
  }
  if (min <= 0 || max <= 0 || configuredStep <= 0) {
    return out(STATUS.BAD_CONFIG, 0, "Min/Max/Step phải lớn hơn 0.");
  }
  if (min > max + 1e-12) {
    return out(STATUS.BAD_CONFIG, 0, "Min lớn hơn Max.");
  }

  // KHÔNG CÓ ĐỐI THỦ: đặt sàn.
  //
  // Min là giá vào cuộc, nên nó cũng phải hợp lệ theo bậc precision. Một Min
  // như 0.1008 sẽ bị 400 mãi mãi, và đó là lỗi cấu hình chứ không phải lỗi
  // nhất thời — nên nói ra thay vì thử lại.
  if (best <= 0) {
    const floor = normalizeTarget(min);
    if (!floor || floor > max + 1e-12) {
      return out(STATUS.BAD_CONFIG, 0,
        `Min ${min} không tạo được giá hợp lệ dưới Max ${max}.`);
    }
    if (mine > 0 && mine >= floor - 1e-12) return out(STATUS.ON_TOP, 0, "Đang dẫn đầu.");
    return out(STATUS.SEND, floor, "Chưa có offer nào khác — đặt Min.");
  }

  // ĐANG DẪN ĐẦU: không tự vượt giá chính mình.
  if (mine > 0 && mine > best + 1e-12) {
    return out(STATUS.ON_TOP, 0, "Offer của mình đang cao hơn.");
  }

  // target = Best + bước của bậc Best, rồi đưa về lưới. Qua ranh giới thì đáp
  // đúng ranh giới: 0.0999 → 0.1000; từ 0.1000 bước là 0.001 → 0.101.
  const target = normalizeTarget(nextTargetAbove(best, step));

  if (!target) {
    return out(STATUS.BAD_CONFIG, 0, "Không tính được giá hợp lệ.");
  }
  // BẤT BIẾN: không bao giờ đề nghị một giá <= Best.
  if (target <= best + 1e-12) {
    return out(STATUS.BAD_CONFIG, 0, `Giá ${target} không cao hơn Best ${best}.`);
  }
  if (!isValidPrecision(target)) {
    return out(STATUS.BAD_CONFIG, 0,
      `Giá ${target} sai precision của bậc ${gridFor(target)}.`);
  }

  // MAX LÀ TRẦN CỨNG: vượt trần là đứng nhìn. Không gửi giá thấp hơn Best,
  // không "gửi tối đa Max", không gửi lại giá cũ.
  if (target > max + 1e-12) {
    return out(STATUS.ABOVE_MAX, 0,
      `Cần ${roundPrice(target)} để vượt ${best}, quá Max ${max}.`);
  }

  if (target < min - 1e-12) {
    // Đối thủ đang thấp hơn sàn của mình: vẫn phải đặt ít nhất bằng Min.
    const floor = normalizeTarget(min);
    if (floor > max + 1e-12) {
      return out(STATUS.BAD_CONFIG, 0, `Min ${min} vượt Max ${max}.`);
    }
    if (mine > 0 && mine >= floor - 1e-12) return out(STATUS.ON_TOP, 0, "Đang dẫn đầu.");
    return out(STATUS.SEND, floor, "Vượt bằng Min.");
  }

  // ĐÃ ĐÚNG GIÁ ĐÓ RỒI: không gửi lại.
  //
  // Nếu không có nhánh này, mỗi sự kiện của đối thủ giá thấp hơn sẽ sinh một
  // offer y hệt cái đang treo — tốn quota, không đổi vị trí.
  if (mine > 0 && Math.abs(mine - target) < 1e-12) {
    return out(STATUS.ON_TOP, 0, "Đã ở đúng giá cần.");
  }

  return out(STATUS.SEND, target, `Vượt ${best} bằng ${target} (bước ${step}).`);
}


module.exports = { decide, STATUS };
