"use strict";

/**
 * Settings tab (spec 16).
 *
 * API Key 1 / API Key 2, the private key, and engine tuning.
 *
 * Default prices live in each dashboard's Add-NFT panel, which saves itself
 * on every edit - there is deliberately no duplicate copy of them here.
 *
 * Backup is FAILOVER ONLY - the UI says so explicitly so nobody expects
 * alternating keys. Leaving Backup empty is fully supported.
 */

window.OSB = window.OSB || {};

(function (OSB) {
  const { el, setText, setClass, setDisabled, shortAddress, toast } = OSB.utils;

  function createSettingsTab({ onSaved } = {}) {
    const api = window.botAPI;

    let root = null;
    let inputs = null;
    let statusLine = null;
    let reportButton = null;
    let apiButton = null;
    let apiButton2 = null;
    let apiNotice = null;
    let keyLive = null;
    let apiPanel = null;
    let updatePanel = null;
    let updateLine = null;
    let updateBar = null;
    let updateCheckButton = null;
    let updateDownloadButton = null;
    let updateInstallButton = null;

    /** Last state pushed by main. The panel is a pure function of it. */
    let updateState = null;
    let licenseExpiry = null;
    let licenseName = null;
    let licenseBadge = null;
    let lockNotice = null;
    let licenseButton = null;
    let themeSelect = null;
    let openSeaLine = null;
    let openSeaWallet = null;
    /** 1.25.11: the CONFIGURED wallet of this Tool, separate from the session. */
    let walletConfigured = null;
    let walletKeyStatus = null;
    let walletProfileSelect = null;
    let walletRemoveButton = null;
    let configuredAddress = "";
    /** Licence or API lock: the wallet picker is as locked as every other field. */
    let walletControlsLocked = false;
    let lastSessionState = null;
    let lastSessionWallet = "";
    let openSeaNote = null;
    let remoteLine = null;
    let remotePairing = null;

    /** Shows the packaged version, so a stale EXE is obvious at a glance. */
    let versionLine = null;
    let keyLiveMount = null;

    /** Whether credentials are stored and whether this machine can encrypt. */
    let clearSecretsButton = null;
    let exportEthButton = null;
    let exportRobinhoodButton = null;

    /**
     * What each field was last stored as.
     *
     * Autosave fires on blur as well as change, so tabbing across an untouched
     * field would otherwise re-send a value that is already stored - and every
     * one of those is a private-key decrypt and a wallet re-derive.
     */
    const lastSaved = Object.create(null);

    function secretInput(placeholder) {
      return el("input", {
        type: "password",
        className: "text-input",
        placeholder,
        attrs: { autocomplete: "off", spellcheck: "false" }
      });
    }

    function numberInput(step = "0.001") {
      return el("input", {
        type: "number",
        className: "num-input",
        attrs: { step, min: "0" }
      });
    }

    function field(label, input) {
      return el("label", { className: "field" }, [
        el("span", { className: "field-label", text: label }),
        input
      ]);
    }

    /**
     * Paint the session state.
     *
     * Pushed from main whenever it changes, so this never has to ask - and it
     * must never claim a connection on anything but a live, matching session.
     */
    const STATE_UI = {
      initializing: ["⚪", "Đang khởi tạo"],
      connecting: ["🟡", "Đang kết nối"],
      connected: ["🟢", "Đã kết nối"],
      unverified: ["🟡", "Chưa xác minh được ví"],
      "not-connected": ["🔴", "Chưa kết nối"],
      expired: ["🔴", "Phiên đăng nhập đã hết hạn"],
      error: ["🔴", "Lỗi kết nối"],
      "wrong-wallet": ["🔴", "Ví không khớp"]
    };

    function renderSession(state) {
      if (!openSeaLine) return;
      if (state && state._keep) state = { ...(lastSessionState || {}), wallet: state.wallet };
      else lastSessionState = state;
      const [dot, label] = STATE_UI[state && state.state] || STATE_UI.error;
      // The Ví panel truncates this line to one, so the panel's height never
      // depends on it (section E); the title carries the full text on hover.
      const line = `Trạng thái: ${dot} ${label}`;
      setText(openSeaLine, line);
      openSeaLine.title = line;

      if (openSeaWallet) {
        // The SESSION's wallet - never the source of "which wallet is
        // configured" (that line is walletConfigured, from the saved key).
        const wallet = state && state.wallet ? String(state.wallet) : "";
        lastSessionWallet = wallet;
        let walletLine = wallet ? `Phiên OpenSea: ${wallet}` : "Phiên OpenSea: chưa có ví đăng nhập";
        if (wallet && configuredAddress && wallet.toLowerCase() !== configuredAddress.toLowerCase()) {
          walletLine += " · KHÁC ví đang dùng (đang cập nhật)";
        }
        setText(openSeaWallet, walletLine);
        setClass(openSeaWallet, walletLine.includes("KHÁC") ? "info-line is-error" : "info-line");
        openSeaWallet.title = walletLine;
      }
      if (openSeaNote) {
        // The message only appears when it adds something the label does not.
        const note = state && state.state !== "connected" ? state.message || "" : "";
        setText(openSeaNote, note);
        setClass(openSeaNote, note ? "info-line is-error" : "info-line");
        openSeaNote.title = note;
      }
    }

    function build() {
      versionLine = el("div", { className: "info-line", text: "…" });

      inputs = {
        /**
         * API keys, comma or newline separated.
         *
         * A one-line input, not a textarea: it was several lines tall and
         * pushed everything below it - including Save - off the screen. The
         * value is still a list; the separator is just no longer a keystroke.
         *
         * Not masked. It is a throughput key, not a spending key, and hiding it
         * only makes it hard to check which one is loaded.
         */
        /**
         * HAI Ô, HAI KEY NGANG HÀNG.
         *
         *   Cả hai là capacity thật; lượt gửi đi key nào rảnh sớm nhất. Key 2
         *   được để trống — khi đó app chạy một key. Mỗi ô kích hoạt và lưu
         *   RIÊNG (mã hoá, theo Tool #N).
         */
        apiKeys: el("input", {
          type: "text",
          className: "text-input",
          placeholder: "Dán OpenSea API Key 1",
          attrs: { autocomplete: "off", spellcheck: "false" }
        }),
        apiKey2: el("input", {
          type: "text",
          className: "text-input",
          placeholder: "Dán OpenSea API Key 2 (để trống nếu chỉ dùng một key)",
          attrs: { autocomplete: "off", spellcheck: "false" }
        }),
        privateKey: secretInput("Private Key của ví"),
        licenseKey: el("input", {
          type: "text",
          className: "text-input",
          placeholder: "Dán License Key",
          attrs: { autocomplete: "off", spellcheck: "false" }
        }),
        scanWorkers: el("input", {
          type: "number",
          className: "num-input",
          attrs: { min: "1", max: "8", step: "1" }
        }),
        offerIntervalSeconds: el("input", {
          type: "number",
          className: "num-input",
          attrs: { min: "0", step: "1" }
        })
      };

      statusLine = el("div", { className: "form-error" });

      clearSecretsButton = el("button", {
        className: "btn",
        text: "Xoá thông tin đã lưu",
        on: { click: onClearSecrets }
      });

      exportEthButton = el("button", {
        className: "btn",
        text: "Xuất ETH",
        on: { click: () => onExport("ethereum", exportEthButton) }
      });
      exportRobinhoodButton = el("button", {
        className: "btn",
        text: "Xuất Robinhood",
        on: { click: () => onExport("robinhood", exportRobinhoodButton) }
      });
      // Its own class: the version line beside it is also an info-line,
      // and anything selecting by that took whichever came first.
      updateLine = el("div", { className: "info-line update-message", text: "" });
      const updateFill = el("div", { className: "progress-fill" });
      updateBar = {
        node: el("div", { className: "progress-track" }, [updateFill]),
        set(done, total) {
          const pct = total > 0 ? Math.max(0, Math.min(100, done / total * 100)) : 0;
          updateFill.style.width = `${pct}%`;
        }
      };
      updateBar.node.hidden = true;

      updateCheckButton = el("button", {
        className: "btn",
        text: "Kiểm tra cập nhật",
        on: { click: OSB.utils.guarded("Kiểm tra cập nhật", onCheckUpdate) }
      });
      updateDownloadButton = el("button", {
        className: "btn btn-primary",
        text: "Cập nhật ngay",
        on: { click: OSB.utils.guarded("Cập nhật", onDownloadUpdate) }
      });
      updateDownloadButton.hidden = true;
      updateInstallButton = el("button", {
        className: "btn btn-start",
        text: "Khởi động lại & cập nhật",
        on: { click: OSB.utils.guarded("Khởi động lại", onInstallUpdate) }
      });
      updateInstallButton.hidden = true;

      apiNotice = el("div", { className: "lock-notice", text: "" });
      apiNotice.hidden = true;
      apiButton = el("button", {
        className: "btn btn-start",
        text: "Kích hoạt Key 1",
        on: { click: () => onActivateApi(1) }
      });
      apiButton2 = el("button", {
        className: "btn",
        text: "Kích hoạt Key 2",
        on: { click: () => onActivateApi(2) }
      });

      licenseName = el("div", { className: "license-name", text: "—" });
      licenseExpiry = el("div", { className: "license-expiry", text: "" });
      licenseBadge = el("span", { className: "state state-idle", text: "Chưa kích hoạt" });
      // Filled by main, never composed here: one wording, one source.
      // Trusted time, so a wrong system clock reads as itself rather than as a
      // licence that mysteriously stopped working.
      lockNotice = el("div", { className: "lock-notice", text: "" });
      lockNotice.hidden = true;
  
      licenseButton = el("button", {
        className: "btn btn-primary",
        text: "Kích hoạt",
        on: { click: onActivate }
      });

      // Every field stores itself as you leave it.
      //
      // The API key also gets ACTIVATED after it is stored - saving a key the
      // app has not tried is how a key sat there looking correct while nothing
      // worked. The private key and the two scan settings only need storing.
      autosaveOn(inputs.apiKeys, "apiKeys", () => onActivateApi(1));
      // Key 2 đi thẳng đường kích hoạt (probe → lưu), không autosave trước:
      // một key phụ sai không được lọt vào pool rồi mới bị phát hiện ở một
      // lượt đọc nền nào đó. Rời ô với giá trị trống = gỡ Key 2.
      activateOnLeave(inputs.apiKey2, 2);
      autosaveOn(inputs.privateKey, "privateKey");
      autosaveOn(inputs.scanWorkers, "scanWorkers");
      autosaveOn(inputs.offerIntervalSeconds, "offerIntervalSeconds");

      themeSelect = el("select", { className: "select-input" }, [
        el("option", { value: "dark", text: "Tối" }),
        el("option", { value: "light", text: "Sáng" })
      ]);
      themeSelect.value = OSB.theme.current();

      // Applied immediately so the change is visible while choosing, and saved
      // so it survives a restart. It is a normal setting, not a secret.
      themeSelect.addEventListener("change", async () => {
        const theme = OSB.theme.apply(themeSelect.value);
        const res = await api.saveSettings({ theme }).catch(() => null);
        if (res && res.ok === false) {
          toast.error("Không lưu được theme.");
          return;
        }
        toast.success(theme === "dark" ? "Đã chuyển Dark." : "Đã chuyển Light.");
      });

      openSeaLine = el("div", { className: "info-line", text: "Trạng thái: đang khởi tạo" });
      openSeaWallet = el("div", { className: "info-line", text: "" });
      walletConfigured = el("div", { className: "info-line wallet-configured", text: "Ví đang dùng: …" });
      walletKeyStatus = el("div", { className: "info-line" });
      walletProfileSelect = el("select", { className: "wallet-profile-select", title: "Ví đã lưu của Tool này" });
      walletProfileSelect.addEventListener("change", () => onPickWallet(walletProfileSelect.value));
      walletRemoveButton = el("button", {
        className: "btn", text: "Xoá ví đã lưu", title: "Xoá ví đang chọn trong danh sách (không xoá được ví đang dùng)",
        on: { click: () => onRemoveWallet() }
      });
      openSeaNote = el("div", { className: "info-line" });
      remoteLine = el("div", { className: "info-line", text: "Remote đang khởi tạo…" });
      const copyRemote = el("button", {
        className: "btn",
        text: "Copy mã kết nối",
        on: {
          click: async () => {
            if (!remotePairing?.enabled) return toast.warn("Remote host chưa sẵn sàng.");
            await navigator.clipboard.writeText(String(remotePairing.token || ""));
            toast.success("Đã copy mã kết nối.");
          }
        }
      });


      // Everything on this page saves itself. There is no Save button because
      // there is nothing left for one to do: a form where every field is
      // already stored, with a button that claims to store them, is a button
      // that teaches the user their edits are not safe until they press it.
      reportButton = el("span");

      // Everything below the licence panel. Hidden wholesale in locked mode
      // rather than disabled field by field: a locked app has no business
      // showing an API key box at all, and one forgotten field would be a way
      // in.
      // Compact on purpose.
      //
      // Every paragraph of explanation that used to sit between these fields
      // pushed Save below the fold, so the one control the user came for was
      // the one they had to hunt for. What a field means belongs in its label
      // and its placeholder; a Settings tab is a form, not a manual.
      //
      // What is masked: the Private Key, and only the Private Key. It is the
      // one credential here that spends money. The API key and the License key
      // are shown, because a key you cannot read is a key you cannot check.
      root = el("section", { className: "settings-tab" }, [
        // ---- version + update, one line ------------------------------
        //
        // A whole panel for "which version am I on" was three lines of chrome
        // around one line of fact.
        el("div", { className: "settings-columns settings-columns-top" }, [
          updatePanel = el("section", { className: "panel update-panel" }, [
            el("div", { className: "srow" }, [
              el("div", { className: "panel-title", text: "Phiên bản" }),
              versionLine,
              updateLine,
              el("div", { className: "srow-actions" }, [
                updateCheckButton, updateDownloadButton, updateInstallButton
              ])
            ]),
            updateBar.node
          ]),

          // ---- licence, one card ------------------------------------
          el("section", { className: "panel license-panel" }, [
            el("div", { className: "srow" }, [
              el("div", { className: "panel-title", text: "License" }),
              licenseName,
              licenseBadge,
              licenseExpiry
            ]),
            lockNotice,
            el("div", { className: "srow" }, [
              field("License Key", inputs.licenseKey),
              el("div", { className: "srow-actions" }, [licenseButton])
            ])
          ])
        ]),

        // ---- the two credentials, side by side ----------------------
        el("div", { className: "settings-columns" }, [
          apiPanel = el("section", { className: "panel api-panel" }, [
            el("div", { className: "panel-title", text: "OpenSea API" }),
            apiNotice,
            el("div", { className: "srow" }, [
              field("API Key 1", inputs.apiKeys),
              el("div", { className: "srow-actions" }, [apiButton])
            ]),
            el("div", { className: "srow" }, [
              field("API Key 2", inputs.apiKey2),
              el("div", { className: "srow-actions" }, [apiButton2])
            ]),
            keyLiveMount = el("div", { className: "keylive-host" })
          ]),

          el("section", { className: "panel session-wallet-panel" }, [
            el("div", { className: "panel-title", text: "Ví" }),
            walletConfigured,
            el("div", { className: "srow" }, [
              field("Ví đã lưu", walletProfileSelect),
              el("div", { className: "srow-actions" }, [walletRemoveButton])
            ]),
            el("div", { className: "srow" }, [
              field("Private Key", inputs.privateKey)
            ]),
            walletKeyStatus,
            openSeaWallet,
            openSeaLine,
            openSeaNote,
            el("div", { className: "panel-title remote-title", text: "Remote Mac" }),
            remoteLine,
            copyRemote
          ])
        ]),

        // ---- everything else, one strip ------------------------------
        //
        // Theme, the two scan settings and the connection state are all
        // one-line facts. Given a panel each they cost more height than the
        // credentials above them.
        el("section", { className: "panel settings-strip" }, [
          el("label", { className: "field field-inline" }, [
            el("span", { className: "field-label", text: "Giao diện" }),
            themeSelect
          ]),
          field("Số luồng quét", inputs.scanWorkers),
          field("Nghỉ giữa 2 lần gửi (giây)", inputs.offerIntervalSeconds)
          // openSeaLine lives in session-wallet-panel now (right after
          // openSeaWallet, where "Trạng thái: ..." reads next to the wallet
          // it describes). A DOM node has one parent: appending the SAME
          // node here too silently moved it out of the Ví panel on every
          // mount, leaving that panel missing its status line, and made this
          // strip's own height depend on 429/cooldown/Active text length —
          // exactly what section E's "không được co giãn" rules out.
        ]),

        // ---- the footer ---------------------------------------------
        el("div", { className: "settings-footer" }, [
          statusLine,
          el("div", { className: "srow-actions" }, [
            reportButton, exportEthButton, exportRobinhoodButton, clearSecretsButton
          ])
        ])
      ]);

      return root;
    }

    function setStatus(message, isError = false) {
      setText(statusLine, message || "");
      setClass(
        statusLine,
        message ? `form-error visible${isError ? " is-error" : ""}` : "form-error"
      );
    }

    /** Verify + persist the licence key on its own, without the rest of the form. */
    async function onActivate() {
      const key = inputs.licenseKey.value.trim();
      if (!key) {
        toast.warn("Chưa nhập License Key.");
        return;
      }

      setDisabled(licenseButton, true);
      const result = await api.setLicense(key);
      setDisabled(licenseButton, false);

      if (!result.ok) {
        toast.error(result.error || "Kích hoạt thất bại.");
        return;
      }

      renderLicense(result.license);
      if (result.license.valid) {
        toast.success(
          `License hợp lệ - còn ${result.license.daysLeft} ngày`
        );
      } else {
        toast.error(result.license.reason || "License không hợp lệ.");
      }
    }

    /**
     * The licence, as four facts.
     *
     * Who it belongs to, when it runs out, and whether it works. When it does
     * not work the reason is the one from the guard, which is already written
     * for the person reading it - never a status code, a device id or anything
     * about how the key is checked.
     */
    function renderLicense(state) {
      if (!state || !licenseBadge) return;

      setText(licenseName, state.valid
        ? (state.name || "Đã kích hoạt")
        : "License chưa hợp lệ");

      if (state.valid) {
        const until = state.expiresAt
          ? new Date(state.expiresAt).toLocaleDateString("vi-VN")
          : "—";
        // The customer's own name was on a line of its own; it is the badge's
        // job to say working or not, and the only fact worth the space beside
        // it is when it stops.
        setText(licenseExpiry, `Còn ${state.daysLeft} ngày · đến ${until}`);
        setText(licenseBadge, "Đang hoạt động");
        setClass(licenseBadge, "state state-done");
      } else {
        setText(licenseExpiry, state.reason || "Chưa kích hoạt");
        setText(licenseBadge, "Không dùng được");
        setClass(licenseBadge, "state state-fail");
      }
    }

    /**
     * Xuất mỗi NFT đang lưu của `chain` ra một file .txt, mỗi dòng một link.
     * Chỉ đọc db (không chạm engine đang chạy), chỉ ghi ra file người dùng
     * chọn qua hộp thoại Lưu của hệ điều hành — không có đường nào tới
     * production DB hay bất kỳ credential nào.
     */
    async function onExport(chain, button) {
      const label = chain === "robinhood" ? "Robinhood" : "Ethereum";
      setDisabled(button, true);
      let result;
      try {
        result = await api.exportNfts({ chain });
      } finally {
        setDisabled(button, false);
      }

      if (!result || result.ok === false) {
        if (result && result.code === "CANCELLED") return; // người dùng tự huỷ hộp thoại
        const message = (result && result.error) || `Xuất ${label} thất bại.`;
        toast.error(message);
        return;
      }

      toast.success(`Đã xuất ${result.count} NFT ${label} → ${result.filePath}`);
    }

    async function onClearSecrets() {
      setDisabled(clearSecretsButton, true);
      const result = await api.clearSecrets();
      setDisabled(clearSecretsButton, false);

      if (!result || result.ok === false) {
        const message = (result && result.error) || "Không xoá được credential.";
        setStatus(message, true);
        toast.error(message);
        return;
      }

      renderSecrets(result.secrets);
      setStatus("Đã xoá credential đã lưu. Nhập lại để dùng tiếp.");
      toast.success("Đã xoá credential đã lưu.");
    }

    /**
     * Save one field, the moment the user finishes with it.
     *
     * Only the field that changed is sent. A blank box still means "leave the
     * stored one alone", never "delete it" - the boxes for stored credentials
     * start blank on purpose, and sending an empty string would erase a key
     * nobody touched.
     *
     * Failure is said out loud. Silent autosave that silently fails is worse
     * than a Save button, because there is no moment at which the user finds
     * out.
     */
    /**
     * Fields where EMPTY is a choice, not an omission.
     *
     * Blanking a private key means "I did not retype it", so an empty value
     * there must be ignored or the key would be destroyed by tabbing past it.
     * (Alchemy key — the one clearable field — is gone since 1.19.17.)
     */
    const CLEARABLE = new Set([]);

    /** "Ví đang dùng" from this Tool's saved key - independent of the session. */
    async function refreshWallet(knownAddress) {
      if (!api || typeof api.walletState !== "function") return;
      const st = await api.walletState().catch(() => null);
      if (!st || !st.ok) return;
      configuredAddress = knownAddress !== undefined && knownAddress !== null && knownAddress !== ""
        ? String(knownAddress) : String(st.configured || "");
      const line = configuredAddress
        ? `Ví đang dùng (Tool #${st.tool}): ${configuredAddress}`
        : `Ví đang dùng (Tool #${st.tool}): chưa có Private Key`;
      setText(walletConfigured, line);
      walletConfigured.title = line;
      setClass(walletConfigured, configuredAddress ? "info-line wallet-configured" : "info-line wallet-configured is-error");
      // Profile list: addresses only.
      walletProfileSelect.innerHTML = "";
      const profiles = Array.isArray(st.profiles) ? st.profiles : [];
      if (!profiles.length) {
        walletProfileSelect.appendChild(el("option", { value: "", text: "(chưa có ví đã lưu)" }));
      }
      for (const p of profiles) {
        const short = `${p.address.slice(0, 6)}…${p.address.slice(-4)}`;
        walletProfileSelect.appendChild(el("option", {
          value: p.id, text: `${p.active ? "● " : ""}${p.label ? p.label + " · " : ""}${short}`
        }));
        if (p.active) walletProfileSelect.value = p.id;
      }
      setDisabled(walletRemoveButton, profiles.length < 2 || walletControlsLocked);
      setDisabled(walletProfileSelect, walletControlsLocked);
      // Re-draw the session line against the configured wallet.
      // Only once a real session state has arrived (else it would paint an
      // error before main's first push).
      if (openSeaWallet && lastSessionState) {
        renderSession({ wallet: lastSessionWallet, _keep: true });
      }
    }

    function lockWalletControls(locked) {
      walletControlsLocked = locked;
      if (walletProfileSelect) setDisabled(walletProfileSelect, locked);
      if (walletRemoveButton) setDisabled(walletRemoveButton, locked || walletProfileSelect.options.length < 2);
    }

    function renderKeyStatus(ok, message) {
      if (!walletKeyStatus) return;
      const text = ok ? "✓ Private Key hợp lệ · đã lưu cho Tool này" : (message || "");
      setText(walletKeyStatus, text);
      setClass(walletKeyStatus, ok ? "info-line is-ok" : (text ? "info-line is-error" : "info-line"));
    }

    async function onPickWallet(id) {
      if (!id || !api || typeof api.walletActivate !== "function") return;
      const res = await api.walletActivate({ id }).catch(() => null);
      if (!res || res.ok === false) {
        toast.error((res && res.error) || "Không chuyển được ví.");
        refreshWallet();
        return;
      }
      renderKeyStatus(true);
      toast.success(`Tool này đang dùng ví ${res.wallet}`);
      refreshWallet(res.wallet || "");
    }

    async function onRemoveWallet() {
      const id = walletProfileSelect && walletProfileSelect.value;
      if (!id || !api || typeof api.walletRemoveProfile !== "function") return;
      const res = await api.walletRemoveProfile({ id }).catch(() => null);
      if (!res || res.ok === false) { toast.warn((res && res.error) || "Không xoá được."); return; }
      toast.info("Đã xoá ví khỏi danh sách của Tool này.");
      refreshWallet();
    }

    async function autosave(fieldName, rawValue, { quiet = false } = {}) {
      const value = String(rawValue === undefined ? "" : rawValue).trim();
      /**
       * "KHÔNG CÓ GÌ ĐỂ LƯU" TRẢ VỀ KHÁC VỚI "ĐÃ LƯU"
       *
       *   Trước đây cả hai đều trả `true`, và `autosaveOn` hiểu `true` là
       *   "có thay đổi" rồi chạy tiếp `after()` — với ô API key thì `after`
       *   là cả một lượt kích hoạt API: probe mạng, lưu secret, nạp lại cấu
       *   hình, nối lại Stream. Rời ô mà không sửa gì cũng chạy đủ chuỗi đó,
       *   và một ô vừa `change` vừa `blur` chạy hai lần.
       */
      if (!value && !CLEARABLE.has(fieldName)) return { ok: true, changed: false };
      if (lastSaved[fieldName] === value) return { ok: true, changed: false };

      const result = await api.saveSettings({ [fieldName]: value }).catch(() => null);

      if (!result || result.ok === false) {
        const message = (result && result.error) || "Không lưu được.";
        setStatus(message, true);
        toast.error(message);
        if (fieldName === "privateKey") {
          // The wallet in use did not change - say which one it still is.
          renderKeyStatus(false, message);
          refreshWallet();
        }
        return { ok: false, changed: false };
      }
      if (fieldName === "privateKey") {
        renderKeyStatus(Boolean(result.wallet), result.wallet ? "" : "Đã xoá Private Key.");
        refreshWallet(result.wallet || "");
      }

      lastSaved[fieldName] = value;
      // BOTH arguments. Called with only the first, `settings` was undefined
      // inside renderSecrets, which blanked every field it repaints from a
      // value - so a key that had just been saved successfully was wiped from
      // the box a moment later, and the label beside it went with it.
      if (result.secrets) renderSecrets(result.secrets, result.settings);
      if (result.settings && typeof onSaved === "function") onSaved(result.settings);
      if (!quiet) setStatus("Đã lưu.");
      return { ok: true, changed: true };
    }

    /**
     * Lưu khi rời ô và khi Enter — nhưng chỉ chạy `after` khi THẬT SỰ đổi.
     *
     *   Ba trình nghe (change, blur, Enter) cho cùng một thao tác của người
     *   dùng: gõ xong rồi nhấn Enter rồi rời ô là ba lượt. Gộp chúng bằng một
     *   nhịp chờ ngắn, và chỉ chạy phần việc nặng khi giá trị đã khác.
     */
    function autosaveOn(input, fieldName, after = null) {
      let timer = null;
      const run = async () => {
        const out = await autosave(fieldName, input.value);
        if (out && out.ok && out.changed && typeof after === "function") await after();
      };
      const schedule = () => {
        if (timer) clearTimeout(timer);
        timer = setTimeout(() => { timer = null; run(); }, 250);
      };
      input.addEventListener("change", schedule);
      input.addEventListener("blur", schedule);
      input.addEventListener("keydown", event => {
        if (event.key === "Enter") schedule();
      });
    }

    /** Fill the form from the persisted settings. */
    const MASK = "••••••••••••";

    /**
     * Show WHETHER a credential is stored, never what it is.
     *
     * The placeholder carries the signal; the value stays empty so that saving
     * without retyping cannot wipe a stored key.
     */
    /**
     * Fill the credential fields.
     *
     * The licence key and the API key are shown in full. Neither spends
     * anything, and a key you cannot read is a key you cannot check - which is
     * the whole reason a stored-but-unloaded key went unnoticed until a job
     * failed on it.
     *
     * The private key is the exception and stays masked: its value never
     * reaches this process at all, so the box shows that one is stored and
     * leaving it empty means leave it alone.
     */
    function renderSecrets(status, settings) {
      const present = (status && status.present) || {};
      const values = settings || {};

      if (inputs.apiKeys) {
        inputs.apiKeys.value = values.apiKeys || values.apiKey || "";
      }
      if (inputs.apiKey2 && document.activeElement !== inputs.apiKey2) {
        inputs.apiKey2.value = values.apiKey2 || "";
        lastSaved.apiKey2 = String(inputs.apiKey2.value).trim();
      }
      if (inputs.licenseKey) {
        inputs.licenseKey.value = values.licenseKey || "";
      }
      if (inputs.privateKey) {
        inputs.privateKey.value = "";
        inputs.privateKey.placeholder = present.privateKey
          ? MASK + "  (đã lưu — để trống nếu không đổi)"
          : inputs.privateKey.dataset.emptyPlaceholder ||
            inputs.privateKey.placeholder;
      }

      // How credentials are stored is not a setting, so it is not shown. The
      // one case worth interrupting for is storage being unavailable, because
      // then what the user types will not survive the session - and that gets
      // said in the status line, where warnings already go.
      if (status && status.available === false) {
        setStatus("Máy này không lưu được credential — nhập lại sau khi mở lại app.", true);
      }

      /**
       * CREDENTIAL ĐÃ LƯU MÀ MỞ KHÔNG RA.
       *
       * Windows mã hoá credential theo TÀI KHOẢN. Một hồ sơ chép từ máy khác,
       * hoặc chạy dưới một tài khoản Windows khác, thì tệp còn nguyên nhưng
       * giải mã ra không được.
       *
       * Chuyện này từng chỉ có trong nhật ký. Người dùng thấy ô trống, bot
       * không làm gì, và không có gì nói vì sao — đúng báo cáo của khách hàng
       * "Loi". Giờ nó nói ra, ngay chỗ phải nhập lại, và gọi tên đúng những ô
       * cần nhập.
       */
      const broken = (status && status.undecryptable) || [];
      if (broken.length) {
        const NAMES = {
          apiKeys: "OpenSea API Key",
          apiKey: "OpenSea API Key",
          apiKey2: "OpenSea API Key 2",
          privateKey: "Private Key",
          licenseKey: "License Key"
        };
        const shown = [...new Set(broken.map(f => NAMES[f] || f))];
        setStatus(
          `${shown.join(" và ")} đã lưu trên máy này nhưng KHÔNG mở ra được — ` +
          "thường là do hồ sơ được chép từ máy khác, hoặc đang chạy dưới một " +
          "tài khoản Windows khác. Hãy nhập lại đúng (các) ô đó rồi lưu.",
          true
        );
        for (const field of broken) {
          const input = inputs[field];
          if (input) input.classList.add("needs-reentry");
        }
      } else {
        for (const input of Object.values(inputs)) {
          if (input && input.classList) input.classList.remove("needs-reentry");
        }
      }
    }

    function hydrate(state) {
      if (!state) return;
      const settings = state.settings || {};

      // Keep the control showing what is actually applied.
      if (themeSelect) themeSelect.value = OSB.theme.apply(settings.theme);

      // The licence and API keys arrive with the settings and are shown; the
      // private key's value never leaves main and only its presence is known.
      renderSecrets(state.secrets, settings);
      // Asked for, not waited on: a push that arrived before this tab existed
      // would otherwise leave the panel blank.
      window.botAPI.updateStatus().then(renderUpdate).catch(() => {});
      window.botAPI.remotePairing().then(info => {
        remotePairing = info;
        if (!remoteLine) return;
        const line = info?.enabled ? `${info.url}` : "Remote host chưa sẵn sàng.";
        setText(remoteLine, line);
        remoteLine.title = line;
      }).catch(() => { setText(remoteLine, "Remote host lỗi."); remoteLine.title = ""; });
      renderLicense(state.license);
      inputs.scanWorkers.value = settings.scanWorkers || "3";
      inputs.offerIntervalSeconds.value = settings.offerIntervalSeconds || "3";

      // What is already stored is what autosave must not send again.
      lastSaved.scanWorkers = String(inputs.scanWorkers.value);
      lastSaved.offerIntervalSeconds = String(inputs.offerIntervalSeconds.value);
      if (inputs.apiKeys.value) lastSaved.apiKeys = String(inputs.apiKeys.value).trim();
      lastSaved.apiKey2 = String(inputs.apiKey2.value || "").trim();

      updateStream(state.stream);
    }

    /**
     * Kept as a no-op on purpose.
     *
     * main pushes stream status here and app.js calls it; the tab simply has
     * nothing to show for it any more. Whether a websocket is connected is
     * plumbing, and the one line this page has for state belongs to the
     * OpenSea session, which is the thing a user can actually act on.
     */
    function updateStream() {}

    function mount(container) {
      if (api && typeof api.getVersion === "function") {
        api.getVersion().then(r => {
          if (r && r.version) setText(versionLine, r.version);
        }).catch(() => {});
      }

      container.appendChild(build());
      if (OSB.createKeyLive && keyLiveMount) {
        keyLive = OSB.createKeyLive({ compact: false });
        keyLive.mount(keyLiveMount);
      }

      // Pushed from main as it changes; the current value is fetched once so a
      // tab opened later is not blank until the next change.
      refreshWallet();
      if (api && typeof api.onSessionStatus === "function") {
        api.onSessionStatus(state => renderSession(state));
      }
      if (api && typeof api.sessionStatus === "function") {
        api.sessionStatus().then(renderSession).catch(() => {});
      }

      return root;
    }

    /**
     * Enter or leave LICENSE LOCKED MODE.
     *
     * The panels other than License are removed from the layout entirely, and
     * the tab stops scrolling - there is nothing below the fold to reach, and
     * a page that still scrolls invites the user to look for it.
     */
    /**
     * Put the key to work before believing in it.
     *
     * The check happens in main against the real API, so a key that only
     * looks right is refused here rather than saved and left to fail later
     * somewhere the user cannot connect back to this field.
     */
    async function onActivateApi(slot = 1) {
      const input = slot === 2 ? inputs.apiKey2 : inputs.apiKeys;
      const button = slot === 2 ? apiButton2 : apiButton;
      const key = String(input.value || "").trim();
      if (!key && slot === 1) {
        setApiNotice("Chưa nhập API Key 1.");
        return;
      }

      setDisabled(button, true);
      const previous = button.textContent;
      setText(button, key ? "Đang kiểm tra…" : "Đang gỡ…");
      try {
        const result = await window.botAPI.activateApi({ apiKey: key, slot });
        if (result && result.ok) {
          // The field keeps the key it just activated. Emptying it made the
          // one field the user is looking at disagree with what is in force.
          setApiNotice("");
          lastSaved[slot === 2 ? "apiKey2" : "apiKeys"] = key;
          if (result.cleared) toast.success("Đã gỡ Key 2 — chạy một key.");
          else toast.success(slot === 2 ? "Key 2 đã kích hoạt — hai key active-active." : "Key 1 đã kích hoạt.");
          if (keyLive && result.keys) keyLive.render(result.keys);
          return;
        }
        const message = (result && result.error) || `Không kích hoạt được API Key ${slot}.`;
        setApiNotice(message);
        toast.error(message);
      } finally {
        setDisabled(button, false);
        setText(button, previous);
      }
    }

    /**
     * Kích hoạt khi rời ô / Enter — chỉ khi giá trị THẬT SỰ đổi so với bản
     * đã lưu (cùng nhịp gộp 250ms như autosaveOn).
     */
    function activateOnLeave(input, slot) {
      const name = slot === 2 ? "apiKey2" : "apiKeys";
      let timer = null;
      const run = () => {
        const value = String(input.value || "").trim();
        if ((lastSaved[name] || "") === value) return;
        onActivateApi(slot);
      };
      const schedule = () => {
        if (timer) clearTimeout(timer);
        timer = setTimeout(() => { timer = null; run(); }, 250);
      };
      input.addEventListener("change", schedule);
      input.addEventListener("blur", schedule);
      input.addEventListener("keydown", event => { if (event.key === "Enter") schedule(); });
    }

    /**
     * Draw the panel from one state.
     *
     * Exactly one action is offered at a time. Three buttons all live at once
     * would make the user decide which phase they are in - which is the
     * panel's job, not theirs.
     */
    function renderUpdate(state) {
      if (!updateLine) return;
      updateState = state || updateState;
      const s = updateState || {};
      const phase = s.phase || "IDLE";

      setText(versionLine, s.currentVersion || "—");
      setText(updateLine, s.message || "");
      setClass(updateLine, phase === "ERROR"
        ? "info-line update-message is-error"
        : "info-line update-message");

      const downloading = phase === "DOWNLOADING";
      updateBar.node.hidden = !downloading;
      if (downloading) updateBar.set(Number(s.percent) || 0, 100);

      updateCheckButton.hidden = phase === "AVAILABLE" || phase === "READY";
      setDisabled(updateCheckButton, phase === "CHECKING" || downloading);

      updateDownloadButton.hidden = phase !== "AVAILABLE";
      setDisabled(updateDownloadButton, downloading);

      updateInstallButton.hidden = phase !== "READY";
    }

    async function onCheckUpdate() {
      renderUpdate(await window.botAPI.checkUpdate());
    }

    async function onDownloadUpdate() {
      renderUpdate(await window.botAPI.downloadUpdate());
    }

    async function onInstallUpdate() {
      const result = await window.botAPI.installUpdate();
      if (result && result.ok === false) {
        toast.error(result.error || "Chưa cài được bản mới.");
      }
      // On success the app is closing; there is nothing left to draw.
    }

    function setApiNotice(message) {
      if (!apiNotice) return;
      apiNotice.textContent = message || "";
      apiNotice.hidden = !message;
    }

    /**
     * API-locked mode: one field, one button.
     *
     * Everything else is disabled rather than merely hidden. A control that is
     * only hidden is still reachable by keyboard, and saving a Private Key or
     * a scan setting while the app cannot talk to OpenSea is a change the user
     * cannot verify took effect.
     */
    function setApiLocked(locked, reason) {
      if (!root) return;
      root.classList.toggle("is-api-locked", Boolean(locked));
      setApiNotice(locked ? reason || "API OpenSea không dùng được." : "");

      for (const [name, input] of Object.entries(inputs)) {
        if (name === "apiKeys" || name === "apiKey2") continue;
        setDisabled(input, Boolean(locked));
      }
      setDisabled(clearSecretsButton, Boolean(locked));
      setDisabled(licenseButton, Boolean(locked));
      setDisabled(themeSelect, Boolean(locked));
      lockWalletControls(Boolean(locked) || root.classList.contains("is-locked"));
      setDisabled(apiButton, false);
      setDisabled(apiButton2, false);
      // Same reason as the licence lock.
      setDisabled(updateCheckButton, false);
      setDisabled(updateDownloadButton, false);
      setDisabled(updateInstallButton, false);
    }

    /**
     * Licence-locked mode: one field, one button.
     *
     * The other panels are hidden by CSS, which already takes them out of the
     * tab order - but they are disabled as well, the same way API-locked mode
     * does it. Two independent mechanisms for "you cannot touch this" is cheap,
     * and it means a later layout change that stops hiding something does not
     * silently make it editable again.
     */
    function setLocked(locked, reason) {
      if (!root) return;
      root.classList.toggle("is-locked", Boolean(locked));

      for (const [name, input] of Object.entries(inputs)) {
        if (name === "licenseKey") continue;
        setDisabled(input, Boolean(locked));
      }
      // Updating stays available. A user locked out by an expired licence is
      // often the one who needs the newer build most.
      setDisabled(updateCheckButton, false);
      setDisabled(updateDownloadButton, false);
      setDisabled(updateInstallButton, false);
      setDisabled(clearSecretsButton, Boolean(locked));
      setDisabled(themeSelect, Boolean(locked));
      lockWalletControls(Boolean(locked) || root.classList.contains("is-api-locked"));
      if (apiButton) setDisabled(apiButton, Boolean(locked));
      if (apiButton2) setDisabled(apiButton2, Boolean(locked));
      setDisabled(licenseButton, false);
      // The licence screen owns the window while it is up; an API notice
      // underneath it would be a second thing to fix that cannot be fixed yet.
      if (locked) root.classList.remove("is-api-locked");
      if (lockNotice) {
        lockNotice.textContent = reason || "";
        lockNotice.hidden = !locked || !reason;
      }
    }

    return {
      setLocked,
      setApiLocked,
      /** Non-locking notice for a refused/missing OpenSea key (CLAUDE.md §18). */
      setApiProblem: message => setApiNotice(message),
      renderUpdate,
      mount, hydrate, updateStream, renderLicense };
  }

  OSB.createSettingsTab = createSettingsTab;
})(window.OSB);
