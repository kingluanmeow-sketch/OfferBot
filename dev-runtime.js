"use strict";

/**
 * dev-runtime.js — bản dev chạy cạnh bản production mà không chạm vào nó.
 *
 * VÌ SAO PHẢI CÓ MỘT FILE RIÊNG CHO ĐIỀU NÀY
 *
 *   Trong lúc sửa, bản production THẬT vẫn đang chase bằng tiền thật của chủ
 *   sở hữu. Một bản dev dùng chung profile Electron, chung named pipe của
 *   QuotaBroker, chung kho credential hay chung updater là một bản dev có thể
 *   cướp nhịp ghi, ghi đè settings, hoặc bầu lại leader của bản đang chạy —
 *   và tất cả những chuyện đó xảy ra trong im lặng.
 *
 *   Nên chế độ dev đổi DANH TÍNH của tiến trình, đúng một lần, trước khi bất
 *   cứ module nào đọc đường dẫn:
 *
 *     tên app        "OpenSea Offer Bot Dev"
 *     userData       %APPDATA%\OpenSea Offer Bot Dev
 *     settings base  %LOCALAPPDATA%\OpenSea Offer Bot Dev
 *     broker pipe    offerbot-dev-<vân tay khoá>
 *     updater        tắt
 *     gửi tiền thật  tắt
 *
 *   Bật bằng biến môi trường `OFFERBOT_ENV=development` (hoặc `OFFERBOT_DEV=1`).
 *   Không bật thì file này không làm gì cả — production đi đúng đường cũ.
 */

const path = require("path");

/** Hậu tố dùng chung cho mọi danh tính của bản dev. */
const DEV_SUFFIX = " Dev";

/**
 * MỘT BIẾN MÔI TRƯỜNG SÓT LẠI KHÔNG ĐƯỢC PHÉP ĐỔI DANH TÍNH BẢN ĐÃ CÀI
 *
 *   Chế độ dev đổi `app.setName` và `userData`. Nghe thì vô hại, nhưng khoá
 *   mã hoá credential của Electron (os_crypt) nằm TRONG `userData/Local
 *   State`: đổi userData là đổi khoá, và mọi credential đã lưu bỗng "không
 *   giải mã được". Người dùng thấy đúng một triệu chứng — phải nhập lại API
 *   key và private key — mà không có gì nói vì sao.
 *
 *   `OFFERBOT_ENV=development` có thể tồn tại trong environment của một máy
 *   vì đủ thứ lý do không liên quan: một lần `setx` từ lâu, một shortcut,
 *   một terminal của người viết code. Không cái nào là lời tuyên bố "bản
 *   đang cài này là bản dev".
 *
 *   Nên chế độ dev đòi HAI điều kiện: biến môi trường, VÀ tiến trình không
 *   phải bản đã đóng gói. Bản cài đặt chạy đường production, luôn luôn.
 */
let packaged = null;

/** Ghi nhớ một lần: tiến trình này có phải bản đã đóng gói không. */
function notePackaged(app) {
  if (packaged === null && app && typeof app.isPackaged === "boolean") {
    packaged = app.isPackaged;
  }
  return packaged === true;
}

function isDev() {
  if (packaged === true) return false;
  const env = String(process.env.OFFERBOT_ENV || "").toLowerCase();
  return env === "development" || env === "dev" || process.env.OFFERBOT_DEV === "1";
}

/** Tiền tố namespace của QuotaBroker: dev không bao giờ gặp production. */
function brokerNamespace() {
  return isDev() ? "offerbot-dev" : "offerbot-quota";
}

/** Bản dev không bao giờ tự cập nhật, và không ghi trạng thái updater nào. */
function updaterEnabled() { return !isDev(); }

/**
 * Bản dev có được phép POST order thật không.
 *
 * Mặc định KHÔNG. Bật phải cố ý (`OFFERBOT_DEV_SPEND=yes`) và vẫn phải qua mọi
 * cổng khác của app — biến này chỉ là cái chốt cuối, không phải giấy phép.
 */
function spendAllowed() {
  return !isDev() || String(process.env.OFFERBOT_DEV_SPEND || "").toLowerCase() === "yes";
}

/**
 * Đổi danh tính tiến trình. Gọi MỘT LẦN, sớm nhất có thể trong main, trước
 * `instance` và trước mọi `app.getPath("userData")`.
 *
 * @param {object} app Electron app
 * @returns {{dev:boolean, name:string, userData:string}}
 */
function applyTo(app) {
  if (!app || typeof app.setName !== "function") return { dev: false };
  // Chốt danh tính TRƯỚC khi hỏi `isDev()`: từ đây trở đi bản đã cài không
  // bao giờ đi nhánh dev, dù môi trường nói gì.
  notePackaged(app);
  if (!isDev()) return { dev: false, name: app.getName(), userData: app.getPath("userData") };

  const name = app.getName() + DEV_SUFFIX;
  app.setName(name);
  // `getPath("userData")` đọc lại theo tên vừa đặt trên Windows/macOS; đặt
  // tường minh để không phụ thuộc hành vi đó.
  const base = app.getPath("appData");
  const userData = path.join(base, name);
  app.setPath("userData", userData);
  // Cache của Electron (Code Cache, GPUCache…) cũng phải rời khỏi profile
  // production, nếu không hai tiến trình khoá LevelDB của nhau.
  try { app.setPath("sessionData", userData); } catch { /* bản Electron cũ */ }
  return { dev: true, name, userData };
}

/** Thư mục gốc của settings dùng chung cả máy (instance.js đọc). */
function settingsBaseName(defaultName = "OpenSea Offer Bot") {
  return isDev() ? defaultName + DEV_SUFFIX : defaultName;
}

module.exports = { isDev, applyTo, notePackaged, brokerNamespace, updaterEnabled, spendAllowed, settingsBaseName, DEV_SUFFIX };
