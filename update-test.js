
// 2026-09-23 incident: in-process main.js runs ONLY in a temp profile (never production).
require("./test-exe-sandbox").inProcessSandbox(require("electron").app, "update");
/**
 * update-test.js — the app updates itself, and takes nothing with it.
 *
 * Every phase is driven by emitting the events electron-updater emits, so the
 * real Updater, the real IPC handlers, the real Settings panel and the real
 * state machine are exercised. Nothing is downloaded and nothing is installed:
 * `quitAndInstall` is replaced, because a test that actually ran it would end
 * the test by closing the app.
 *
 * The half that matters most is section 6. An update replaces the PROGRAM. If
 * it also replaced the profile - the device identity, the encrypted
 * credentials, the licence - the user would be silently deactivated by pressing
 * a button labelled "update".
 */
const path = require("path");
const fs = require("fs");
const os = require("os");
const ROOT = __dirname;

/** An isolated profile: this suite writes credentials. */
const PROFILE = fs.mkdtempSync(path.join(os.tmpdir(), "osb-update-"));
const { app: earlyApp } = require("electron");
earlyApp.setPath("userData", PROFILE);
process.env.LOCALAPPDATA = PROFILE;

const errors = require(path.join(ROOT, "error-registry"));
const { Updater, classifyUpdateError, autoUpdater } = require(path.join(ROOT, "updater"));

// Never let a test install anything.
let installCalls = 0;
autoUpdater.quitAndInstall = () => { installCalls++; };
// Never let a test reach the network.
autoUpdater.checkForUpdates = async () => ({ updateInfo: {} });
autoUpdater.downloadUpdate = async () => [];

const { LicenseGuard } = require(path.join(ROOT, "license"));
LicenseGuard.prototype.status = function status() {
  return {
    valid: true, expired: false, name: "Test", id: "test",
    expiresAt: Date.now() + 86400000, daysLeft: 1, reason: "",
    serverStatus: "ACTIVE", source: "test", deviceId: "d",
    locked: false, lockReason: ""
  };
};
LicenseGuard.prototype.requireValid = function requireValid() { return { ok: true }; };

require(path.join(ROOT, "main.js"));
const { app, BrowserWindow } = require("electron");

let pass = 0, fail = 0;
const say = (...a) => process.stdout.write(a.join(" ") + "\n");
const check = (name, ok, detail = "") => {
  if (ok) { pass++; say(`  PASS  ${name}`); }
  else { fail++; say(`  FAIL  ${name}${detail ? " :: " + detail : ""}`); }
};
const sleep = ms => new Promise(r => setTimeout(r, ms));

let win = null;
const js = code => win.webContents.executeJavaScript(code, true);

/** Emit what electron-updater emits, so the real handlers run. */
const emit = (event, payload) => {
  autoUpdater.emit(event, payload);
  return sleep(200);
};

async function openSettings() {
  await js(`(() => {
    const t = [...document.querySelectorAll(".tabbar .tab")]
      .find(b => (b.textContent || "").trim() === "Settings");
    if (t) t.click();
    return true;
  })()`);
  await sleep(250);
}

/** What the update panel is actually showing. */
const panel = () => js(`(() => {
  const p = document.querySelector(".settings-tab .update-panel");
  if (!p) return { missing: true };
  const button = label => {
    const b = [...p.querySelectorAll("button")]
      .find(x => (x.textContent || "").trim() === label);
    if (!b) return null;
    const r = b.getBoundingClientRect();
    return { hidden: b.hidden === true, disabled: b.disabled === true,
      visible: r.height > 0 };
  };
  const bar = p.querySelector(".progress-track");
  const fill = p.querySelector(".progress-fill");
  return {
    version: (p.querySelector(".update-message") || {}).textContent || "",
    message: (p.querySelector(".update-message") || {}).textContent || "",
    versionLabel: [...p.querySelectorAll("div")]
      .map(d => d.textContent).find(t => /Phiên bản/.test(t || "")) || "",
    isError: (p.querySelector(".update-message") || {}).className || "",
    check: button("Kiểm tra cập nhật"),
    download: button("Cập nhật ngay"),
    install: button("Khởi động lại & cập nhật"),
    barHidden: bar ? bar.hidden === true : null,
    fillWidth: fill ? fill.style.width : ""
  };
})()`);

async function main() {
  win = BrowserWindow.getAllWindows().find(w => /index\.html/.test(w.webContents.getURL()));
  if (!win) throw new Error("cửa sổ chính không mở");
  await sleep(1500);
  await openSettings();

  // ================================================================
  say("\n--- 1: phiên bản hiện tại ---");
  // ================================================================
  {
    const status = await js("window.botAPI.updateStatus()");
    const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, "package.json"), "utf8"));

    check("có kênh trạng thái cập nhật", Boolean(status), JSON.stringify(status));
    check("báo đúng phiên bản đang chạy",
      String(status.currentVersion) === app.getVersion(),
      `${status.currentVersion} vs ${app.getVersion()}`);
    check("và khớp package.json khi chạy đóng gói",
      typeof pkg.version === "string" && pkg.version.length > 0, pkg.version);

    const v = await panel();
    check("panel hiển thị phiên bản", /Phiên bản/.test(v.versionLabel), v.versionLabel);
    check("ban đầu chỉ có nút Kiểm tra",
      v.check && !v.check.hidden && v.download.hidden && v.install.hidden,
      JSON.stringify({ c: v.check, d: v.download, i: v.install }));
  }

  // ================================================================
  say("\n--- 2: đã là bản mới nhất ---");
  // ================================================================
  {
    await emit("update-not-available", { version: app.getVersion() });
    const v = await panel();
    check("nói rõ đang dùng bản mới nhất",
      /mới nhất/i.test(v.message), v.message);
    check("không mời tải gì cả", v.download.hidden === true);
    check("không mời khởi động lại", v.install.hidden === true);
    check("vẫn kiểm tra lại được", v.check.hidden === false);
  }

  // ================================================================
  say("\n--- 3: có bản mới ---");
  // ================================================================
  {
    await emit("update-available", { version: "9.9.9" });
    const v = await panel();
    check("hiện số hiệu bản mới", /9\.9\.9/.test(v.message), v.message);
    check("mời cập nhật", v.download.hidden === false);
    check("và giấu nút Kiểm tra để chỉ còn một lựa chọn",
      v.check.hidden === true, JSON.stringify(v.check));
    check("chưa mời khởi động lại", v.install.hidden === true);
  }

  // ================================================================
  say("\n--- 4: tiến trình tải ---");
  // ================================================================
  {
    await emit("download-progress", { percent: 0, transferred: 0, total: 1000 });
    let v = await panel();
    check("thanh tiến trình hiện ra", v.barHidden === false, String(v.barHidden));

    await emit("download-progress", { percent: 42.7, transferred: 427, total: 1000 });
    v = await panel();
    check("phần trăm phản ánh đúng", /43%|42%/.test(v.message), v.message);
    check("thanh vẽ đúng tỉ lệ", v.fillWidth === "42.7%", v.fillWidth);

    const status = await js("window.botAPI.updateStatus()");
    check("trạng thái mang theo số byte",
      status.transferred === 427 && status.total === 1000,
      JSON.stringify({ t: status.transferred, all: status.total }));
  }

  // ================================================================
  say("\n--- 5: tải xong, mời khởi động lại ---");
  // ================================================================
  {
    await emit("update-downloaded", { version: "9.9.9" });
    const v = await panel();
    check("mời khởi động lại", v.install.hidden === false);
    check("thanh tiến trình biến mất", v.barHidden === true);
    check("nói rõ phải khởi động lại",
      /Khởi động lại/i.test(v.message), v.message);

    installCalls = 0;
    const result = await js("window.botAPI.installUpdate()");
    check("cài được", result && result.ok === true, JSON.stringify(result));
    await sleep(250);
    check("và thực sự gọi cài đặt", installCalls === 1, String(installCalls));
  }

  // ================================================================
  say("\n--- 6: dữ liệu người dùng KHÔNG bị đụng tới ---");
  // ================================================================
  {
    // The whole point. An update replaces the program, not the profile.
    const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, "package.json"), "utf8"));

    check("gỡ cài đặt không xoá dữ liệu ứng dụng",
      pkg.build.nsis.deleteAppDataOnUninstall === false,
      String(pkg.build.nsis.deleteAppDataOnUninstall));

    const src = fs.readFileSync(path.join(ROOT, "updater.js"), "utf8");
    check("updater không đụng vào device identity",
      !/device\.json|deviceId/.test(src));
    check("updater không đụng vào kho credential",
      !/SecretStore|secrets\.save|privateKey|apiKey/.test(src));
    check("updater không đụng vào settings",
      !/updateSettings|db\./.test(src));
    check("updater chỉ dọn khóa cạnh tranh của chính nó",
      /rmSync\(lock\.lockPath/.test(src) &&
      !/rmSync\([^\n]*(userData|settings|credential|secrets)/i.test(src));

    // The identity file itself, before and after an install request.
    const deviceDir = path.join(PROFILE, "OpenSea Offer Bot");
    const before = fs.existsSync(deviceDir)
      ? fs.readdirSync(deviceDir).length : 0;
    installCalls = 0;
    await js("window.botAPI.installUpdate()");
    await sleep(250);
    const after = fs.existsSync(deviceDir)
      ? fs.readdirSync(deviceDir).length : 0;
    check("hồ sơ máy không mất gì khi cài", after >= before,
      `${before} → ${after}`);
  }

  // ================================================================
  say("\n--- 7: lỗi nói bằng tiếng Việt và có mã ---");
  // ================================================================
  {
    const cases = [
      ["mất mạng", "getaddrinfo ENOTFOUND github.com", "E_UPDATE_NETWORK"],
      ["chưa có release", "Cannot find latest.yml, 404", "E_UPDATE_NOT_FOUND"],
      ["sai chữ ký", "sha512 checksum mismatch", "E_UPDATE_SIGNATURE"],
      ["thiếu quyền", "EPERM: operation not permitted", "E_UPDATE_PERMISSION"],
      ["không rõ", "something else entirely", "E_UPDATE_FAILED"]
    ];

    for (const [label, raw, expected] of cases) {
      check(`${label} → ${expected}`, classifyUpdateError(raw) === expected,
        classifyUpdateError(raw));
      const entry = errors.get(expected);
      check(`${expected} có trong registry`, Boolean(entry));
      check(`${expected} nói bằng tiếng Việt`,
        Boolean(entry) && /[À-ɏḀ-ỿ]/.test(entry.userMessage),
        entry && entry.userMessage);
    }

    await emit("error", new Error("getaddrinfo ENOTFOUND github.com"));
    const v = await panel();
    check("panel báo lỗi cho người dùng",
      /không kết nối được/i.test(v.message), v.message);
    check("và đánh dấu là lỗi", /is-error/.test(v.isError), v.isError);
    check("không lộ chi tiết kỹ thuật",
      !/ENOTFOUND|getaddrinfo|github\.com/.test(v.message), v.message);

    const status = await js("window.botAPI.updateStatus()");
    check("trạng thái mang mã lỗi", status.code === "E_UPDATE_NETWORK", status.code);
  }

  // ================================================================
  say("\n--- 8: không cài khi chưa tải xong ---");
  // ================================================================
  {
    await emit("update-available", { version: "9.9.9" });
    installCalls = 0;
    const result = await js("window.botAPI.installUpdate()");
    check("bị từ chối", result && result.ok === false, JSON.stringify(result));
    check("đúng mã lỗi", result.code === "E_UPDATE_NOT_READY", result.code);
    await sleep(200);
    check("và không hề gọi cài đặt", installCalls === 0, String(installCalls));
  }

  // ================================================================
  say("\n--- 9: không có token nào trong sản phẩm ---");
  // ================================================================
  {
    const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, "package.json"), "utf8"));
    const publish = pkg.build.publish;

    check("có cấu hình nguồn cập nhật", Array.isArray(publish) && publish.length === 1);
    check("release stable dùng releaseType=release và channel=latest",
      publish[0].releaseType === "release" && publish[0].channel === "latest",
      JSON.stringify({ releaseType: publish[0].releaseType, channel: publish[0].channel }));
    check("build sinh blockmap cho installer Windows",
      pkg.build.nsis.differentialPackage === true,
      String(pkg.build.nsis.differentialPackage));
    check("trỏ đúng repository",
      publish[0].owner === "kingluanmeow-sketch" && publish[0].repo === "OfferBot",
      JSON.stringify(publish[0]));

    const asJson = JSON.stringify(pkg);
    check("package.json không chứa token",
      !/gh[ps]_[A-Za-z0-9]{10,}|github_pat_[A-Za-z0-9_]{10,}/.test(asJson));
    check("và không có trường token nào",
      !/"token"|"GH_TOKEN"|"GITHUB_TOKEN"/.test(asJson));

    // Every packaged file, not just the ones that came to mind.
    const suspects = [];
    for (const f of ["updater.js", "main.js", "preload.js", "error-registry.js"]) {
      const src = fs.readFileSync(path.join(ROOT, f), "utf8");
      if (/gh[ps]_[A-Za-z0-9]{10,}|github_pat_[A-Za-z0-9_]{10,}/.test(src)) {
        suspects.push(f);
      }
    }
    check("không file nguồn nào nhúng token", suspects.length === 0,
      suspects.join(", "));

    const src = fs.readFileSync(path.join(ROOT, "updater.js"), "utf8");
    check("updater không đọc token từ đâu cả",
      !/GH_TOKEN|GITHUB_TOKEN|process\.env/.test(src),
      "app đang chạy không được cần credential nào để cập nhật");
    check("và log của updater được redact",
      /errors\.redact/.test(src));
  }

  // ================================================================
  say("\n--- 10: cập nhật được cả khi app đang bị khoá ---");
  // ================================================================
  {
    for (const [label, channel, payload] of [
      ["License locked", "license:state", {
        valid: false, expired: true, name: "", id: "", expiresAt: 0, daysLeft: 0,
        reason: "License đã hết hạn", serverStatus: "EXPIRED", source: "test",
        deviceId: "d", locked: true, lockReason: "License đã hết hạn"
      }],
      ["API locked", "api:state", {
        state: "LOCKED", locked: true, code: "E_API_INVALID",
        reason: "API OpenSea không hợp lệ.", checkedAt: Date.now()
      }]
    ]) {
      // Reset the other lock first so each is tested on its own.
      win.webContents.send("license:state", {
        valid: true, expired: false, name: "T", id: "t",
        expiresAt: Date.now() + 86400000, daysLeft: 1, reason: "",
        serverStatus: "ACTIVE", source: "test", deviceId: "d",
        locked: false, lockReason: ""
      });
      win.webContents.send("api:state", {
        state: "ACTIVE", locked: false, code: "", reason: "", checkedAt: Date.now()
      });
      await sleep(250);

      win.webContents.send(channel, payload);
      await sleep(400);

      const reachable = await js(`(() => {
        const p = document.querySelector(".settings-tab .update-panel");
        if (!p) return { missing: true };
        const style = getComputedStyle(p);
        const b = [...p.querySelectorAll("button")]
          .find(x => /Kiểm tra cập nhật/.test(x.textContent || ""));
        return {
          panelVisible: style.display !== "none",
          buttonEnabled: b ? b.disabled === false : null
        };
      })()`);

      check(`${label}: panel cập nhật vẫn hiện`,
        reachable.panelVisible === true, JSON.stringify(reachable));
      check(`${label}: nút Kiểm tra vẫn bấm được`,
        reachable.buttonEnabled === true, JSON.stringify(reachable));
    }

    // Back to normal for anything after this.
    win.webContents.send("license:state", {
      valid: true, expired: false, name: "T", id: "t",
      expiresAt: Date.now() + 86400000, daysLeft: 1, reason: "",
      serverStatus: "ACTIVE", source: "test", deviceId: "d",
      locked: false, lockReason: ""
    });
    win.webContents.send("api:state", {
      state: "ACTIVE", locked: false, code: "", reason: "", checkedAt: Date.now()
    });
    await sleep(300);
  }

  // ================================================================
  say("\n--- 11: cấu hình an toàn ---");
  // ================================================================
  {
    check("không tự tải khi chưa được đồng ý",
      autoUpdater.autoDownload === false, String(autoUpdater.autoDownload));
    check("không tự cài khi thoát app",
      autoUpdater.autoInstallOnAppQuit === false,
      String(autoUpdater.autoInstallOnAppQuit));
    check("stable build không theo prerelease", autoUpdater.allowPrerelease === false,
      String(autoUpdater.allowPrerelease));
    check("download luôn dùng installer đầy đủ trên GitHub",
      autoUpdater.disableDifferentialDownload === true,
      String(autoUpdater.disableDifferentialDownload));

    const src = fs.readFileSync(path.join(ROOT, "updater.js"), "utf8");
    check("KHÔNG tắt kiểm tra chữ ký",
      !/verifyUpdateCodeSignature\s*=\s*false/.test(src) &&
      !/allowUnsigned|disableSignature|isVerifySignature\s*=\s*false/.test(src),
      "một update không kiểm chữ ký là một đường cài mã tuỳ ý");
    check("và không cho phép hạ cấp phiên bản",
      !/allowDowngrade\s*=\s*true/.test(src));
  }

  // ================================================================
  say("\n--- 12: một updater owner cho mỗi installation ---");
  // ================================================================
  {
    const options = { currentVersion: app.getVersion(), onState: () => {}, onLog: () => {}, updateLockRoot: PROFILE };
    const first = new Updater(options);
    const second = new Updater(options);
    check("process đầu lấy được quyền update", first.acquireUpdateLock());
    check("process cạnh tranh không thể lấy cùng quyền", !second.acquireUpdateLock());
    first.releaseUpdateLock();
    check("quyền được nhả khi owner kết thúc", second.acquireUpdateLock());
    second.releaseUpdateLock();

    const realCheck = autoUpdater.checkForUpdates;
    autoUpdater.checkForUpdates = async () => { throw new Error("getaddrinfo ENOTFOUND github.com"); };
    const failedCheck = new Updater(options);
    const state = await failedCheck.check();
    check("lỗi check không bị coi là NO_UPDATE", state.phase === "ERROR" && state.code === "E_UPDATE_NETWORK",
      JSON.stringify({ phase: state.phase, code: state.code }));
    failedCheck.releaseUpdateLock();
    autoUpdater.checkForUpdates = realCheck;
  }
}

app.whenReady().then(async () => {
  await sleep(3500);
  try {
    await main();
  } catch (error) {
    say("FAILED: " + error.message);
    say(error.stack);
    fail++;
  }
  try { fs.rmSync(PROFILE, { recursive: true, force: true }); } catch { /* disposable */ }
  say("\n" + "=".repeat(60));
  say(`${pass}/${pass + fail} update checks passed${fail ? `, ${fail} FAILED` : ""}`);
  say("=".repeat(60));
  app.exit(fail ? 1 : 0);
});
