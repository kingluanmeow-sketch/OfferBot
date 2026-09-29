# OpenSea Offer Bot

Desktop app (Electron + Node.js + Seaport 1.6 + OpenSea REST/Stream API) tự động
theo dõi Best Offer của từng NFT và gửi offer mới khi bị outbid, cho
**Ethereum** và **Robinhood Chain**.

---

## Chạy nhanh

```bash
npm install
npm start          # chạy app
npm run check      # 149 kiểm tra logic (không cần API key, không cần Electron)
npm run smoke      # 105 kiểm tra end-to-end trong Electron thật (network được stub)
npm run build      # tạo dist/OpenSea-Offer-Bot-Setup-1.0.0.exe

node make-license.js "Tên khách" 30    # cấp license 30 ngày
node make-license.js --check OSB1.x.y  # kiểm tra một license
```

Sau `npm run build`, người dùng cuối chỉ cần mở file `.exe` trong `dist/`.

---

## Cấu trúc

```text
OpenSeaOfferBot/
├── main.js                 Electron main: window, IPC, engine, stream, cancel, housekeeping
├── preload.js              contextBridge - cầu nối DUY NHẤT giữa renderer và Node
├── db.js                   JSON store + validate giá (authoritative)
├── engine.js               OfferEngine: priority queue, locks, state machine
├── robinhood-engine.js     Subclass mỏng, CHỈ khác phần RPC của Robinhood
├── opensea.js              REST layer: Best Offer, My Offers, metadata, fees
├── stream.js               OpenSea Stream API (WebSocket + Phoenix frames)
├── seaport.js              Ký/POST order, cancel offchain + onchain, đọc balance
├── rate-limiter.js         Global limiter (READ/ORDER/CANCEL) + API key failover
├── cache.js                TTL cache có giới hạn + optimistic My Offer cache
├── logger.js               Log có tag, tự dọn, gộp lỗi trùng
├── license.js              Xác minh license Ed25519 (chỉ chứa khoá CÔNG KHAI)
│
├── renderer/
│   ├── index.html          Nạp script theo đúng thứ tự (classic script, không ES module)
│   ├── styles.css          MỘT stylesheet cho cả hai dashboard
│   ├── utils.js            DOM helper, format, trạng thái, toast, modal xác nhận
│   ├── dashboard-component.js   ⭐ Component dashboard DÙNG CHUNG
│   ├── ethereum-dashboard.js    Wrapper mỏng: chain = "ethereum"
│   ├── robinhood-dashboard.js   Wrapper mỏng: chain = "robinhood"
│   ├── dashboard.js        Registry: route state theo chain, 1 tick chung
│   ├── cancel.js           Tab Cancel (độc lập hoàn toàn với dashboard)
│   ├── bulk-offer.js       Tab Offer SLL - bắn offer hàng loạt theo cụm 50
│   ├── settings.js         API keys, private key, engine tuning (KHÔNG có giá mặc định)
│   └── app.js              Tab router, Logs, DUY NHẤT 1 UI timer
│
├── assets/icon.ico
├── make-license.js         Công cụ cấp license (KHÔNG nằm trong bản build)
├── license-signing-key.SECRET.json   Khoá ký RIÊNG - tuyệt đối không chia sẻ
├── scripts-check.js        Dev harness (không nằm trong bản build)
├── smoke-test.js           Dev harness (không nằm trong bản build)
└── README.md
```

> `scripts-check.js` và `smoke-test.js` là hai file phát sinh thêm so với cây thư
> mục trong đề bài. Chúng chỉ phục vụ phát triển và **đã được loại khỏi
> `build.files`**, nên không đi vào file `.exe`.

---

## Hai dashboard giống nhau bằng cách nào

`ethereum-dashboard.js` và `robinhood-dashboard.js` mỗi file chỉ có ~10 dòng:

```js
OSB.createDashboard({ chain: "ethereum", title: "Dashboard Ethereum" });
OSB.createDashboard({ chain: "robinhood", title: "Dashboard Robinhood" });
```

Toàn bộ layout, độ rộng cột, kích thước input, kích thước button, spacing, font,
counter, Scan Status, Max Speed, Start/Pause/Stop/Delete đều nằm trong
`dashboard-component.js`. Không thể lệch nhau vì chỉ có một bản code.

`smoke-test.js` kiểm tra điều này bằng cách so sánh trực tiếp DOM của hai
dashboard: danh sách cột, `col.style.width`, nhãn button, nhãn counter.

Backend cũng vậy: `RobinhoodEngine extends OfferEngine` và **không** override
`decide` / `scanOne` / `scheduleNext` / `getCounters` / `tierOf` — `npm run check`
sẽ fail nếu ai đó override chúng.

---

## Logic offer

```text
Best Offer
    ↓
My Offer  (API + optimistic cache)
    ↓
My >= Best ?  → 🟢 ON TOP, không gửi
    ↓ không
Target = Best + Step        (Best = 0 → Target = Min Price)
Target < Min → Target = Min
    ↓
Target <= Max ?
    ├── có   → 🔵 ACTIVE  → gửi offer Target
    └── không → 🟡 OUTBID  → KHÔNG gửi, giữ NFT, scan lại mỗi 10s
```

Trước khi ký, engine đọc lại Best Offer **không qua cache** một lần nữa
(final verification). Nếu giá đã nhảy khiến `Target > Max` thì huỷ gửi.

### Status

| Status | Điều kiện |
|---|---|
| 🟢 ON TOP | `My >= Best` |
| 🔵 ACTIVE | `Best > My` và `Best + Step <= Max` |
| 🟡 OUTBID | `Best + Step > Max` — giữ NFT, scan lại 10s |
| 🔴 LOW BALANCE | không đủ WETH — scan lại 10s |
| ⚪ PAUSED | NFT bị pause |
| 🔴 ERROR | lỗi API/xử lý — chỉ NFT đó, các NFT khác vẫn chạy |

### Scan Status

Không bao giờ hiển thị ON TOP hay OUTBID. Chỉ có 5 giá trị:

```text
🔵 SCANNING...     xanh nước biển
🟢 SEND OFFER      xanh lá
🟡 3s NEXT SCAN    vàng, đếm ngược thật
🔴 STOPPED
🔴 ERROR
```

Đếm ngược được suy ra từ `nextScanAt` do engine đặt, nên con số trên UI luôn khớp
đúng thời điểm scan thật sự chạy. Về 0 là scan ngay (`buildQueue` nhặt row có
`nextScanAt <= now`). Không có trạng thái `READY TO SCAN`.

### Counters

`Total NFT` · `Active` · `On Top` · `Outbid`. Không có `Watching`.
**OUTBID không được tính vào Active.**

---

## Chống gửi trùng offer

Ba lớp, tất cả đều có test:

1. **Global send lock** — `engine.globalSubmitting`. Toàn app chỉ một offer đang
   ký/gửi tại một thời điểm.
2. **Per-NFT lock** — `engine.submittingNfts` (một `Set`).
3. **Optimistic My Offer cache** — sau khi gửi thành công, giá vừa gửi được ghi
   vào `cache.optimisticOffers` và giữ **ít nhất 60 giây**. API trả `null` hoặc
   giá thấp hơn sẽ **không** ghi đè. Nhờ vậy không xảy ra:
   `gửi 0.128 → API chưa index → My Offer = — → gửi lại 0.128`.

Ngoài ra còn một `Offer Interval` toàn cục (mặc định 3s) tính từ offer thành công
gần nhất.

---

## Best Offer chính xác

Một NFT có thể bị áp bởi **item offer**, **collection offer** và **trait offer**;
collection offer hoàn toàn có thể cao hơn item offer. Xác nhận trên dữ liệu
thật: Best Offer của **Milady #1 là một collection offer 1.03 ETH**, không phải
item offer.

`fetchBestOffer` đi qua 2 pass:

1. `/offers/collection/{slug}/nfts/{id}/best` — chính OpenSea quyết định offer
   cao nhất đang áp cho token này (đã tính cả collection/trait offer).
2. `/offers/collection/{slug}/nfts/{id}` — endpoint đã scope theo NFT; mọi order
   được **phân loại** (`classifyOffer` → item / collection / trait) và loại bỏ
   order inactive / expired / cancelled / đã fill hết, rồi mới lấy giá cao nhất.

Không bao giờ lấy `offers[0]`, và không coi mọi response của endpoint item là
exact token offer.

> Từng có pass 3 gọi `GET /orders/{chain}/seaport/offers`. Route đó **chỉ nhận
> POST** trong API v2 và trả **405 Method Not Allowed** cho mọi GET, nên pass đó
> không bao giờ đóng góp được kết quả. Đã xoá thay vì để một fallback luôn hỏng
> trong im lặng.

### My Offer chỉ tính item offer

`/account/{wallet}/offers` trả **cả collection offer** kèm một block `asset` có
`identifier: null`. Điều kiện `identifier !== undefined` là **chưa đủ** — nó
từng tạo ra key rác dạng `ethereum:0x...:null` cho mọi collection offer trong
ví. Giờ `extractOfferAssetRef` loại thẳng order có `criteria`, và bắt buộc
`identifier` khớp `^\d+$`. Kiểm chứng lại trên 611 offer thật: **0 key hỏng**.

---

## OpenSea Stream

REST dựng state ban đầu, Stream đẩy realtime:

```text
REST → initial state
STREAM → offer event → priority scan ngay → update Best → decision → send
```

Events xử lý: `item_received_offer`, `item_received_bid`, `collection_offer`,
`trait_offer`, `item_cancelled`.

Khi có event trúng NFT đang theo dõi, engine **không chờ hết countdown**: đặt
`nextScanAt = 0` và promote NFT lên tier ưu tiên cao nhất; lần scan đó cũng bỏ
qua cache Best Offer.

### Ba quyết định lấy từ đo thực tế

Đo trên API thật (2 collection `milady` + `pudgypenguins`): **~195 event/giây**,
11.728 event trong 60 giây. Ở tốc độ đó có ba chỗ phải xử lý riêng:

1. **Log được gộp, không log từng event.** `main.js` chỉ tăng counter trong
   handler và in **một** dòng tổng kết mỗi 30 giây. Nếu log từng event thì ring
   buffer 1000 dòng bị ghi đè hết sau ~5 giây và mỗi giây bắn ~195 IPC message
   sang renderer — chắc chắn lag. Đo lại sau khi sửa: **0.7 dòng/giây**.
   Event trúng NFT đang xem vẫn được log riêng dưới tag `[STREAM PRIORITY]`,
   và một event chạm nhiều row chỉ in một dòng.

2. **Tra cứu bằng index, không quét bảng.** `engine.byNftKey` và `engine.bySlug`
   (`Map<string, Set<url>>`) cho lookup O(1). Trước đó `handleStreamEvent` quét
   toàn bộ row hai lần cho mỗi event, cho mỗi engine.

3. **Row đang bị chặn không phản ứng với offer mới.** Best Offer là max trên các
   offer đang active, nên một offer **mới** (`item_received_offer`,
   `item_received_bid`, `collection_offer`, `trait_offer`) chỉ có thể làm Best
   **tăng hoặc giữ nguyên**. Row đang `OUTBID` / `LOW BALANCE` lại đang chờ Best
   **giảm** — nên scan vì mấy event đó là gọi API chắc chắn vô ích.
   Vì vậy row bị chặn chỉ được đánh thức bởi `item_cancelled` (event duy nhất có
   thể làm Best giảm), còn lại giữ nguyên nhịp fallback 10 giây.
   Đo thực tế: **40 scan → 18 scan** trong 60 giây, đúng bằng nhịp 10s, không mất
   khả năng phản ứng. Row `ACTIVE` vẫn phản ứng với **mọi** event như spec §12.

Ngoài ra `stream.js` xử lý `phx_reply` để phát hiện `phx_join` bị từ chối
(trước đó bị bỏ qua hoàn toàn → socket vẫn "connected" nhưng không bao giờ nhận
event, và không có gì báo), `phx_error` để rejoin channel bị crash, và có
watchdog: connected + có subscription mà **90 giây không có frame nào** thì log
cảnh báo và ép reconnect. Trạng thái này đã gặp thật một lần khi test.

`stream.js` tự nói giao thức WebSocket trên `tls` socket của Node thay vì dựa vào
`globalThis.WebSocket` (không đảm bảo có trong Electron main process), và gửi
đúng Phoenix v2 frame `[join_ref, ref, topic, event, payload]`, kèm reconnect
backoff, heartbeat 25s, `phx_join` / `phx_leave` theo tập collection đang xem.

---

## Scan queue và concurrency

Priority queue, không quét tuần tự toàn bộ:

| Tier | Loại |
|---|---|
| 1 | NFT do Stream kích hoạt |
| 2 | Max Speed |
| 3 | Bình thường |
| 4 | Outbid |
| 5 | Low Balance |

Trong cùng tier, row nào quá hạn lâu hơn được ưu tiên (không starvation).
Số worker mặc định 3 (chỉnh trong Settings, 1–8). Không dùng `Promise.all(100)`.

**Max Speed** được ưu tiên nhưng vẫn xếp hàng ở rate limiter chung, chu kỳ 800ms
— không bao giờ 0ms. Khi NFT rơi vào OUTBID, Max Speed **không bị tắt**, chỉ giãn
xuống 10s; Best giảm lại là quay về chu kỳ nhanh.

---

## API keys

| Key | Vai trò |
|---|---|
| Primary | luôn dùng trước |
| Backup | **chỉ** dùng khi Primary dính 429 / rate limit |
| Cancel | dành riêng cho traffic huỷ offer; trống thì dùng key thường |

Không luân phiên `API1 → API2 → API1`. Sau khi failover, Primary được thử lại
định kỳ (mỗi 60s) và được lấy lại hẳn sau khi hết cooldown (5 phút) hoặc ngay khi
một request Primary thành công. Chỉ nhập một key thì Primary hoạt động bình thường.

## Rate limiter

Một limiter toàn cục, ba bucket tách biệt:

```text
READ    ~510ms/request
ORDER  ~1050ms/request
CANCEL  ~700ms/request
```

Worker **không** tự quản lý rate limit. Tăng số worker hoặc bật Max Speed không
làm tăng tốc độ gọi API — chỉ làm giảm thời gian chờ rỗi.

---

## Cache và bộ nhớ dài hạn

* log tối đa 1000 dòng (cả backend lẫn DOM của tab Logs)
* mọi cache đều có TTL **và** giới hạn số entry
* optimistic offer cache TTL 60s
* Best Offer cache TTL rất ngắn (1.2s) — chỉ để gộp burst từ stream
* metadata / fees cache 10 phút
* quét dọn cache mỗi 60s; xoá cache HTTP của Chromium mỗi 30 phút

Không bao giờ xoá: API key, private key, user settings.

---

## Hiệu năng UI

* Bảng chỉ dựng **một lần**; `update()` diff theo `url` và chỉ ghi lại ô có
  thay đổi (Best / My / Scan Status / Status / Countdown).
* **Một** timer duy nhất trong `app.js` (250ms) chạy countdown cho mọi row.
  Không có `setInterval` cho từng NFT.
* Không dùng `innerHTML = entireTable`.
* Không dùng `cursor: wait` ở bất kỳ đâu; button disabled dùng `cursor: default`.
* Style đặt qua CSSOM (`node.style.width`) chứ không qua attribute `style=""`,
  vì renderer chạy dưới CSP không có `unsafe-inline`.

---

## Start / Pause / Stop

* Đang chạy: `▶` disabled, `⏸` enabled.
* Đang pause: `▶` enabled, `⏸` disabled.
* NFT thêm vào **khi bot đang chạy sẽ KHÔNG tự chạy** — phải bấm START.

---

## Bulk Offer / Bulk Cancel — batch theo phase

Một batch là **50 NFT** và được chạy như **một đơn vị thực thi**, ba phase tách
bạch, không chồng lấn:

```text
BULK OFFER            BULK CANCEL
50 NFT                50 NFT
  ↓                     ↓
PREPARE ALL           DISCOVER ALL
  ↓                     ↓
SIGN ALL              PREPARE + SIGN ALL
  ↓                     ↓
SUBMIT ALL            BROADCAST ALL
  ↓                     ↓
next 50               next 50
```

Trước bản này, submit chạy **song song** với prepare/sign
(`Promise.all([signAll(), ...submitWorkers])`) — NFT 1 đang gửi trong khi NFT 20
còn đang chuẩn bị. Giờ ba hàm tách hẳn:

```js
const prep   = await prepareOfferBatch(items);   // PHASE 1 — không gửi gì
const signed = await signOfferBatch(prep);       // PHASE 2 — 1 chữ ký cho cả batch
const sent   = await submitOfferBatch(signed);   // PHASE 3 — chỉ chạy sau khi ký xong
```

gọi tuần tự, và có test chặn việc quay lại mô hình cũ.

### Khả năng thật của OpenSea / Seaport (đã đo, không đoán)

| | Kết quả |
|---|---|
| Ký hàng loạt | ✅ **Có thật.** Seaport EIP-712 bulk order: 50 order từ **1** lần `signTypedData` |
| Huỷ hàng loạt | ✅ **Có thật.** `Seaport.cancel(OrderComponents[])` — 1 giao dịch huỷ nhiều order |
| Gửi offer hàng loạt | ❌ **Không có.** `/offers/bulk`, `/bulk-offers`, `/offers/batch` đều **404** |
| Giới hạn ghi | **2 req/s** (`x-ratelimit-limit: 2`); probe 4/s → 7 lỗi 429 |

Vì vậy: **50 offer mất khoảng 25 giây và không có cách hợp lệ nào nhanh hơn.**
Code không giả vờ có endpoint batch, và không nói 50 order đi trong một HTTP
request khi thực tế không phải vậy.

> Về browser automation: frontend OpenSea gửi order qua **cùng service bị giới
> hạn 2 req/s đó**, nên điều khiển UI không nhanh hơn — chỉ mong manh hơn và
> trái ToS. Bulk signing ở tầng protocol cho đúng lợi ích thật mà không cần
> automation.

Đo trên dữ liệu thật, 50 NFT cùng một collection:

* **PREPARE**: 2 request đọc (slug + fees dùng chung cho cả 50), **0 POST**, 722ms
* Cả batch dùng **một** cửa sổ hết hạn, đã cộng dư thời gian submit → 31.9 phút
* **SIGN**: 1 chữ ký; nếu ký gộp hỏng thì tự **ký lẻ** — 1 NFT hỏng không giết 49 cái còn lại
* **SUBMIT**: chỉ retry **những order lỗi**, không bao giờ chạy lại cả batch

### Cancel: NFT ≠ order

Một NFT thường có nhiều offer của cùng một ví. Đo thật: **8 NFT → 11 order**,
trong đó 3 NFT có hơn 1 offer. UI đếm riêng `Scanned` (NFT) và `Offers Found`
(order), không gộp làm một.

Cancel ký sẵn **toàn bộ transaction** với nonce liên tiếp ngay ở phase PREPARE,
rồi broadcast tất cả ở phase SUBMIT. Nhờ vậy không có nonce collision và không
phải chờ từng lần gửi trả về mới dựng được nonce tiếp theo.

---

## Nhập liệu và thông báo

* Ô cấu hình trong phần **"1. Thêm NFT"** của mỗi dashboard **chính là giá trị
  mặc định được lưu**. Gõ xong, rời ô hoặc bấm Enter là lưu ngay xuống đĩa —
  không có nút Save, và lần mở app sau nó tự khôi phục.
* Vì vậy Settings **không còn** ô giá mặc định nào. Settings chỉ còn API keys,
  Private Key, Scan Workers và Offer Interval.
* Đổi **Scan Delay** ở ô đó sẽ **áp ngay xuống tất cả NFT** của chain đó, kể cả
  các hàng đang đếm ngược dở: hàng được tính lại từ lần scan gần nhất, nên giảm
  Scan Delay có hiệu lực tức thì chứ không phải chờ hết chu kỳ cũ.
  (Hàng đang OUTBID / LOW BALANCE giữ nhịp 10s riêng, Max Speed giữ nhịp riêng.)
* Mọi ô số trong bảng NFT áp dụng khi **rời ô hoặc bấm Enter**, và mỗi lần đều
  hiện **popup thông báo** ở góc phải dưới cho biết đã nhận hay bị từ chối.
  Thông số mới có hiệu lực ngay từ lần scan kế tiếp của hàng đó.
* Mọi nút thao tác (Add / Start / Pause / Stop / Delete / Save / Scan / Cancel)
  đều có popup xác nhận kết quả.

## Xoá NFT là dừng ngay lập tức

`removeRow()` đặt cờ `deleted` **trước** khi dọn state. Mọi vòng scan và submit
kiểm tra cờ đó sau **mỗi** `await`, nên một request đang bay không thể ghi
trạng thái, không thể gửi offer, và không thể làm hàng đã xoá sống lại.

Cùng lúc đó nó xoá sạch cache của riêng NFT ấy: `bestOfferCache`,
`optimisticOffers` và entry trong `myOffersMap`. Nếu không xoá, thêm lại đúng
NFT đó sẽ thấy Best Offer cũ trong cache và optimistic My Offer cũ có thể chặn
mất offer đầu tiên.

## Tab Cancel

Độc lập hoàn toàn với Offer Dashboard (state riêng, không gọi kênh IPC nào có thể
start/pause/sửa row của engine).

### Tại sao bản cũ chậm, và đã sửa thế nào

Bản đầu quét **từng NFT một**: mỗi NFT tốn 2 request nối tiếp qua rate limiter,
rồi huỷ **từng order một**. 50 offer ≈ 70 giây.

Đo trên API thật để chọn đường tối ưu:

| Endpoint | Tốc độ | Có `protocol_data`? |
|---|---|---|
| `/account/{w}/offers` | **~110ms / 50 offer** | ❌ luôn `null` |
| `/orders/{chain}/seaport/offers?maker` | **405** — chỉ nhận POST | — |
| `/orders/chain/.../{hash}` | ~541ms / 1 order | ✅ |

Không có endpoint bulk nào trả `protocol_data`, nên thiết kế thành 3 đường:

**1. Quét ví (bulk).** Một lượt `/account/{wallet}/offers`, 50 offer mỗi
request. Dán link giờ là **tuỳ chọn** — để trống là quét sạch offer trong ví;
có link thì chỉ lọc đúng những NFT đó.

**2. CANCEL SELECTED / CANCEL ALL — 50 order mỗi giao dịch.**
`Seaport.cancel()` nhận **mảng**, nên tối đa 50 order bị vô hiệu trong **một**
giao dịch thay vì 50 lần gọi API. Phần còn lại là đọc `protocol_data` cho từng
order (chạy song song có giới hạn, vẫn qua rate limiter chung).

**3. ⚡ HỦY SẠCH — 1 giao dịch, không đọc order nào.**
Hai nút riêng, **HỦY SẠCH ETH** và **HỦY SẠCH ROBINHOOD**, nằm sát mép phải
thanh công cụ, tách hẳn khỏi các nút dùng hằng ngày. Chúng gọi
`Seaport.incrementCounter()` — đổi bộ đếm của ví, khiến **mọi** chữ ký Seaport
cũ vô hiệu ngay lập tức. Đây là cách nhanh nhất có thể: 0 request API, 1 giao
dịch. Chính OpenSea cũng dùng cách này cho "cancel all".

Vì nó **huỷ cả listing**, mỗi lần bấm đều bật một **popup cảnh báo** liệt kê
đúng những gì sẽ mất, và nút xác nhận **bị khoá cho tới khi tick vào ô đồng ý**.
Bấm nhầm một cái không thể xoá sạch ví.

---

## License key có hạn sử dụng

License là **chữ ký Ed25519**, không phải mật khẩu. App chỉ mang **khoá công
khai** nên có thể xác minh nhưng không thể tự tạo key. Khoá riêng nằm ở
`license-signing-key.SECRET.json`, **đã bị loại khỏi bản build**.

```bash
node make-license.js "Anh Long" 30     # tạo key 30 ngày
node make-license.js --check <key>     # xem key còn bao lâu
```

Khách dán key vào tab **Settings → License → KÍCH HOẠT**.

Vì chữ ký phủ lên cả hạn dùng, khách **không thể tự sửa ngày hết hạn** — đổi một
ký tự là chữ ký sai ngay. Guard kiểm tra lại theo đồng hồ ở **mỗi** thao tác, nên
key hết hạn giữa lúc đang mở app sẽ bị chặn luôn, không cần khởi động lại.

Bị chặn khi license không hợp lệ: **START**, **Cancel**, **Huỷ sạch**,
**Bắn offer SLL**. Xem và sửa settings thì vẫn được.

---

## Tab Offer SLL (offer hàng loạt)

Dán link → **ĐỌC LINK** → nhập giá → **BẮN OFFER**. Không quét gì cả, không dò
Best Offer: giá bạn nhập được áp y hệt cho mọi NFT.

OpenSea **không có** endpoint tạo offer hàng loạt — mỗi offer là một POST riêng.
Nên "bắn 50 lệnh một lúc" ở đây nghĩa là: **ký toàn bộ 50 order tại máy** (không
tốn mạng, counter Seaport được truyền vào nên seaport-js không gọi RPC cho từng
order) rồi **POST song song có giới hạn**, chia cụm 50.

Có popup xác nhận kèm ô tick trước khi bắn, hiển thị tổng số tiền nếu tất cả
đều được khớp.

> Giới hạn ghi của OpenSea **chưa đo được** vì đo sẽ tạo offer thật bằng tiền
> của bạn. Bucket ORDER đang để mức thận trọng; nếu dính 429 thì limiter tự lùi.

---

## Log tự dọn

* Dòng thường: giữ tối đa 1000, tự cắt còn 300 mỗi phút.
* **Lỗi: gộp theo từng loại.** Số, hash và địa chỉ bị chuẩn hoá khi tạo chữ ký
  lỗi, nên cùng một sự cố lặp lại chỉ chiếm **một dòng** kèm bộ đếm `(x200)`.
  Đo thực: 205 lần lỗi → **2 mục**.
* Lỗi **không bị** dọn tự động — đó là phần đáng giữ.
* Nút **🧹 XOÁ HẾT LOG** xoá sạch cả lỗi, cả ở backend.

---

## Logging

```text
[STREAM] [STREAM PRIORITY] [SCAN] [BEST] [MY OFFER] [DECISION]
[SEND] [ON TOP] [OUTBID] [LOW BALANCE] [CANCEL] [ENGINE] [API] [ERROR]
```

Ví dụ:

```text
[STREAM] item_received_offer
[STREAM PRIORITY] Apu #123 <- item_received_offer
[SCAN] Apu #123 Best=0.125 My=0.12 kind=item
[DECISION] Apu #123 Target=0.126 Max=0.13
[SEND] Apu #123 0.126 ETH (ethereum)
```

---

## Xử lý lỗi

Một NFT lỗi **chỉ** làm NFT đó chuyển ERROR và lùi 10s; các NFT khác chạy tiếp.
`dispatch()` luôn `.catch()` promise của scan, và main process có handler
`unhandledRejection` / `uncaughtException` ghi vào log thay vì chết app.

---

## Kiểm thử

`npm run check` — 149 kiểm tra, không cần API key:
validate giá, ON TOP / ACTIVE / OUTBID, counters, LOW BALANCE, chống gửi trùng,
lock, error isolation, Start/Pause/Stop, NFT mới không tự chạy, priority queue,
Max Speed, stream decode + priority, API key failover, rate limiter, cancel,
phân loại offer, giới hạn bộ nhớ, index stream + watchdog phx_join, và kiểm tra tĩnh rằng mọi hàm được gọi đều tồn
tại (chống lỗi kiểu `calculateTargetPrice is not a function`) và không dùng
`.get()` cho `Set` / `.add()` cho `Map`.

`npm run smoke` — 105 kiểm tra end-to-end trong Electron thật, network được stub:
IPC preload ↔ main, render bảng, hai dashboard giống hệt nhau (so sánh DOM),
thêm NFT Ethereum + Robinhood, từ chối link sai chain, Min > Max bị backend chặn,
START/PAUSE/STOP, trạng thái button, countdown chạy thật và scan khi về 0,
Scan Status không bao giờ là ON TOP/OUTBID, log tới tab Logs, tab Cancel.

---

## Lưu ý về dữ liệu

* `database.json` nằm trong thư mục userData của Electron.
* **Settings được lưu vĩnh viễn** (API keys, private key, mặc định từng chain).
* **Danh sách NFT là session-only**: đóng app rồi mở lại, bảng NFT trống.
  Đây là hành vi cố ý, giữ nguyên từ bản V1.

Private key được lưu ở dạng plaintext trong `database.json` (giống V1). Hãy dùng
ví riêng cho bot và giữ máy an toàn.
