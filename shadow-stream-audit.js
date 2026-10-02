"use strict";

/**
 * SHADOW STREAM AUDIT -- READ-ONLY.
 *
 * Answers one question with evidence: when a Stream frame is "mapped" for a
 * tracked NFT but the MemoryBook does not apply it, WHICH reason is it, and is
 * it a benign no-op (duplicate / unrelated invalidate / trait that does not
 * match) or a real UPSERT that could make Best go stale?
 *
 * What it does:  production snapshot (read-only file) -> MemoryBook rows ->
 *   DualStreamFeed (2 independent keys, same ShardedOpenSeaStream as the app)
 *   -> the app's own toBookOp() normalizer -> MemoryBook.apply() with the
 *   pre-apply diagnoseApply() classification -> counters + bounded samples.
 * What it never does: sign, build an order, POST, cancel, touch the running
 *   app or its userData (the snapshot is only read), sweep or poll /best.
 *
 * Targeted /best: one GET for ONE NFT, only when a real mismatch is evidenced
 *   (an external UPSERT priced above the book's Best was rejected for a reason
 *   that is not benign), max 1 per NFT per 5 min and MAX_TARGETED total.
 *
 * Keys come ONLY from env OFFERBOT_SHADOW_KEY1 / OFFERBOT_SHADOW_KEY2 and are
 * never printed; reports carry a short sha256 fingerprint at most.
 *
 *   node shadow-stream-audit.js --selftest          offline, synthetic frames
 *   OFFERBOT_SHADOW_KEY1=.. OFFERBOT_SHADOW_KEY2=.. node shadow-stream-audit.js [--minutes 12]
 *       [--snapshot <path>] [--out <report.json>] [--no-traits]
 */

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { MemoryBook, diagnoseApply } = require("./offer-item-v2/memory-book");
const { toBookOp, OP } = require("./offer-item-v2/event-normalizer");

const WATCH_TOKENS = new Map([
  ["6456", "Alien Fren #6456"], ["10184", "Jazmine #10184"],
  ["8375", "Heracles #8375"], ["7192", "Nibble #7192"]
]);
const BENIGN = new Set(["duplicate", "stale-echo", "no-op", "trait-no-match"]);
const MAX_SAMPLES = 60;
const MAX_TARGETED = 6;
const TARGETED_COOLDOWN_MS = 5 * 60 * 1000;
const RESCAN_SNAPSHOT_MS = 60 * 1000;

const fp = secret => crypto.createHash("sha256").update(String(secret)).digest("hex").slice(0, 8);
const shortHash = h => (h ? String(h).slice(0, 12) : "");

function loadRows(snapshotFile) {
  const j = JSON.parse(fs.readFileSync(snapshotFile, "utf8")); // read-only
  const rows = [];
  for (const chain of Object.keys(j.nfts || {})) {
    for (const n of j.nfts[chain] || []) {
      if (!n.contract || !n.tokenId) continue;
      rows.push({
        chain, contract: String(n.contract).toLowerCase(), tokenId: String(n.tokenId),
        slug: n.collectionSlug || "", name: n.name || ""
      });
    }
  }
  return rows;
}

class Auditor {
  constructor({ now = () => Date.now() } = {}) {
    this.now = now;
    this.book = new MemoryBook();
    this.meta = new Map();            // book key -> { name, tokenId, slug, watch }
    this.counters = {
      frames: 0, unmapped: 0, revalidate: 0, mappedOps: 0, touchedOps: 0,
      byFeed: { A: 0, B: 0 }, byEvent: {}, byKind: {},
      notApplied: {},                 // reason -> count (all tracked NFTs)
      notAppliedUpsert: {},           // reason -> count, UPSERT ops only
      benignRemove: 0, suspect: 0, mismatch: 0
    };
    this.samples = [];                // bounded suspect samples
    this.perWatch = new Map();        // tokenId -> { applied, byReason, last[] }
    this.pendingTargeted = [];        // NFT keys that have mismatch evidence
  }

  addRow(r) {
    const key = `${r.chain}:${r.contract}:${r.tokenId}`;
    if (this.meta.has(key)) return false;
    const book = this.book.add({ key, chain: r.chain, contract: r.contract, tokenId: r.tokenId, collectionSlug: r.slug });
    book.generation = 1; book.hydratedAt = this.now(); book.ownReconciledAt = this.now();
    this.meta.set(key, { name: r.name, tokenId: r.tokenId, slug: r.slug, watch: WATCH_TOKENS.get(r.tokenId) || "", added: this.now() });
    return true;
  }

  slugs() { return [...new Set([...this.meta.values()].map(m => m.slug).filter(Boolean))]; }

  _w(tokenId) {
    let w = this.perWatch.get(tokenId);
    if (!w) { w = { applied: 0, byReason: {}, last: [] }; this.perWatch.set(tokenId, w); }
    return w;
  }

  _bump(obj, k) { obj[k] = (obj[k] || 0) + 1; }

  /** Same entry the app uses: decoded Stream event in, MemoryBook op out. */
  onEvent(event) {
    const c = this.counters;
    c.frames++; this._bump(c.byFeed, event.feed === "B" ? "B" : "A");
    this._bump(c.byEvent, event.event || "?");
    const receivedAt = this.now();
    const op = toBookOp(event);
    if (!op) { c.unmapped++; return; }
    if (op.op === OP.REVALIDATE) { c.revalidate++; return; }
    c.mappedOps++; this._bump(c.byKind, op.op === OP.REMOVE ? "remove" : op.kind || "?");

    // Per-NFT view BEFORE apply (diagnoseApply is read-only, but apply() writes
    // `seen`/tombstones even on a rejected branch -- same ordering as the engine).
    const tracked = op.contract && op.tokenId ? this.book.byNft.get(`${op.contract}:${op.tokenId}`) : null;
    const preBest = tracked ? this.book.get(tracked).effectiveBest(receivedAt).price : 0;
    const touched = this.book.apply(op, receivedAt);
    if (touched.length) c.touchedOps++;
    for (const key of touched) {
      const m = this.meta.get(key);
      if (m && m.watch) this._w(m.tokenId).applied++;
    }
    for (const na of touched.notApplied || []) {
      const { key } = na;
      let reason = na.reason;
      // Same orderHash re-delivered with identical price/endTime (dual-feed echo
      // carrying the same sequence) is rejected as "stale" by isStale(); the book
      // already holds exactly that order, so it is an echo, not a miss.
      if (reason === "stale" && op.op !== OP.REMOVE) {
        const b = this.book.get(key);
        const g = b && (b.own.get(op.orderHash) || b.groupFor(op.kind).get(op.orderHash));
        if (g && g.price === op.price && g.endTime === op.endTime) reason = "stale-echo";
      }
      this._bump(c.notApplied, reason);
      const upsert = op.op !== OP.REMOVE;
      if (upsert) this._bump(c.notAppliedUpsert, reason);
      const m = this.meta.get(key) || {};
      if (m.watch) {
        const w = this._w(m.tokenId);
        this._bump(w.byReason, reason);
        if (w.last.length >= 20) w.last.shift();
        w.last.push({ t: receivedAt, reason, upsert, price: op.price, kind: op.kind, feed: event.feed || "A", orderHash: shortHash(op.orderHash) });
      }
      if (BENIGN.has(reason) || !upsert) { if (!upsert) c.benignRemove++; continue; }
      // Non-benign UPSERT rejection: potential miss.
      c.suspect++;
      const book = this.book.get(key);
      const best = book ? book.effectiveBest(receivedAt).price : 0;
      const higher = Number(op.price) > best;
      if (higher) { c.mismatch++; if (!this.pendingTargeted.includes(key)) this.pendingTargeted.push(key); }
      if (this.samples.length < MAX_SAMPLES) {
        this.samples.push({
          t: receivedAt, nft: m.watch || `${m.slug}#${m.tokenId}`, reason, kind: op.kind,
          price: op.price, bookBest: best, higherThanBook: higher, event: event.event,
          feed: event.feed || "A", orderHash: shortHash(op.orderHash), version: op.version || 0
        });
      }
    }
  }

  report() {
    return {
      counters: this.counters,
      tracked: this.meta.size,
      watch: Object.fromEntries([...this.perWatch].map(([id, w]) => [WATCH_TOKENS.get(id) || id, w])),
      suspectSamples: this.samples
    };
  }
}

function verdict(rep, targeted, streamOk) {
  const c = rep.counters;
  const lines = [];
  if (!streamOk) lines.push("Stream không có frame -> CHƯA ĐỦ EVIDENCE.");
  const up = c.notAppliedUpsert;
  const risky = Object.entries(up).filter(([r]) => !BENIGN.has(r));
  if (!c.frames) lines.push("0 frame nhận được: chưa đủ evidence.");
  else if (!risky.length) lines.push("Mọi UPSERT mapped-nhưng-không-áp trong cửa sổ đều benign (duplicate/no-op/trait-no-match); không có evidence một UPSERT hợp lệ bị bỏ.");
  else lines.push(`UPSERT không-áp KHÔNG benign: ${risky.map(([r, n]) => `${r}=${n}`).join(", ")} (xem suspectSamples).`);
  if (c.mismatch) lines.push(`${c.mismatch} lần giá op > Best sổ mà bị từ chối -> evidence mismatch.`);
  const confirmed = (targeted || []).filter(t => t.restBest !== null && Math.abs(t.restBest - t.bookBest) > 1e-12);
  if (targeted && targeted.length) lines.push(confirmed.length
    ? `/best đích xác nhận lệch sổ ở ${confirmed.length}/${targeted.length} NFT -> root cause theo reason của sample tương ứng.`
    : `/best đích: ${targeted.length} lần, sổ KHỚP REST (không xác nhận miss).`);
  else lines.push("Không có /best đích (không có evidence mismatch).");
  return lines;
}

/* ---------------- self test (offline, synthetic) ---------------- */
async function selftest() {
  const assert = require("node:assert/strict");
  let t = 1_000_000;
  const a = new Auditor({ now: () => t });
  const C = "0x1111111111111111111111111111111111111111";
  a.addRow({ chain: "ethereum", contract: C, tokenId: "6456", slug: "alienfrensnft", name: "A" });
  a.addRow({ chain: "ethereum", contract: C, tokenId: "99", slug: "alienfrensnft", name: "B" });
  const mk = (over) => ({ collectionSlug: "alienfrensnft", nft: { chain: "ethereum", contract: C, tokenId: "6456" },
    kind: "item", orderHash: "0xaaaaaaaaaaaaaaaa", maker: "0x3333333333333333333333333333333333333333", quantity: 1,
    currency: "WETH", endTime: 0, eventTimestamp: t, receivedAt: t, hasOrderData: true, event: "item_received_bid", pricePerItem: 0.01, feed: "A", ...over });
  a.onEvent(mk({}));                                    // applied
  a.onEvent(mk({ feed: "B" }));                         // duplicate (benign)
  a.onEvent(mk({ eventTimestamp: t - 5000, pricePerItem: 0.009 })); // older + other price -> real stale (suspect)
  a.onEvent({ collectionSlug: "alienfrensnft", nft: null, orderHash: "0xdead", eventTimestamp: t + 1, receivedAt: t, event: "order_invalidate" }); // no-op
  a.onEvent({ ...mk({ kind: "trait", nft: null, orderHash: "0xbbbbbbbbbbbbbbbb", event: "trait_offer", pricePerItem: 0.5,
    traitCriteria: { trait_type: "Bg", trait_name: "Blue" }, traitCriteriaList: { trait_criteria_list: [{ trait_type: "Bg", trait_name: "Blue" }] } }) });
  const r = a.report();
  assert.equal(r.counters.frames, 5);
  assert.equal(r.counters.notApplied["stale-echo"], 1);
  assert.equal(r.counters.notApplied.stale, 1);
  assert.ok(r.counters.notApplied["no-op"] >= 1);
  assert.equal(r.counters.notApplied["trait-unknown"], 2, "traits never loaded -> unknown on both tokens");
  assert.ok(r.counters.suspect >= 1, "stale + trait-unknown are non-benign UPSERT rejections");
  assert.ok(r.suspectSamples.every(s => !("maker" in s)), "no wallet addresses in samples");
  assert.equal(JSON.stringify(r).includes("0x3333"), false, "maker never serialised");
  process.stdout.write("selftest PASS\n" + JSON.stringify({ notApplied: r.counters.notApplied, suspect: r.counters.suspect, mismatch: r.counters.mismatch }) + "\n");
}

/* ---------------- live run ---------------- */
async function live(args) {
  const k1 = process.env.OFFERBOT_SHADOW_KEY1 || "", k2 = process.env.OFFERBOT_SHADOW_KEY2 || "";
  if (!k1 || !k2 || k1 === k2) { process.stderr.write("Cần OFFERBOT_SHADOW_KEY1 và OFFERBOT_SHADOW_KEY2 (2 key KHÁC nhau) trong env.\n"); process.exit(2); }
  const minutes = Math.min(15, Math.max(1, Number(args.minutes) || 12));
  const snapshotFile = args.snapshot || path.join(process.env.APPDATA || "", "opensea-offer-bot", "offerbot-snapshot.json");
  const outFile = args.out || path.join(process.env.CLAUDE_JOB_DIR || ".", "tmp", "shadow-stream-report.json");

  const rl = require("./rate-limiter");
  rl.apiKeys.configure({ apiKey: k1, apiKey2: k2 });
  const { DualStreamFeed } = require("./stream-dual-feed");
  const opensea = require("./opensea");
  const quiet = { stream() {}, error() {}, engine() {}, info() {}, warn() {}, debug() {} };

  const auditor = new Auditor();
  for (const r of loadRows(snapshotFile)) auditor.addRow(r);
  const log = s => process.stdout.write(s + "\n"); // never receives key material
  log(`shadow: tracked=${auditor.meta.size} slugs=${auditor.slugs().length} key1=${fp(k1)} key2=${fp(k2)} minutes=${minutes}`);

  // Traits: same TraitSnapshot + nft-source metadata the app uses (bounded, once).
  if (!args["no-traits"]) {
    try {
      const { TraitSnapshot } = require("./offer-item-v2/trait-snapshot");
      const nftSource = require("./nft-source");
      const snap = new TraitSnapshot({ getNFTs: nftSource.getNFTs, readAttributes: nftSource.readAttributes, onLog() {} });
      const rows = [...auditor.meta.entries()].map(([key, m]) => ({ key, contract: key.split(":")[1], tokenId: m.tokenId }));
      snap.hydrate("ethereum", rows, { onBatch: () => applyTraits() }).then(applyTraits).catch(() => {});
      var applyTraits = () => {
        for (const [key] of auditor.meta) {
          const book = auditor.book.get(key); const view = snap.get(key);
          if (!book || book.traitsKnown || !view.known) continue;
          book.traits = new Set(view.keys); book.numericTraits = new Map(view.numericValues || []);
          book.duplicateNumericTraitTypes = new Set(view.duplicateNumericTypes || []); book.traitsKnown = true;
          auditor.book.replayTraitOps(key, Date.now());
        }
      };
    } catch (e) { log(`shadow: traits disabled (${String(e.message).slice(0, 80)})`); }
  }

  const feed = new DualStreamFeed({ apiKeys: rl.apiKeys, logger: quiet, onEvent: ev => { try { auditor.onEvent(ev); } catch (e) { auditor.counters.handlerErrors = (auditor.counters.handlerErrors || 0) + 1; } } });
  feed.start(auditor.slugs());

  const targeted = []; const lastTargeted = new Map();
  const timers = [];
  timers.push(setInterval(async () => {                 // targeted /best, evidence-gated
    const key = auditor.pendingTargeted.shift();
    if (!key || targeted.length >= MAX_TARGETED) return;
    if (Date.now() - (lastTargeted.get(key) || 0) < TARGETED_COOLDOWN_MS) return;
    lastTargeted.set(key, Date.now());
    const m = auditor.meta.get(key); const [chain, contract, tokenId] = key.split(":");
    const bookBest = auditor.book.get(key).effectiveBest(Date.now()).price;
    let restBest = null;
    try { const b = await opensea.fetchBestOffer(chain, contract, tokenId, m.slug, { useCache: false }); restBest = b && b.ok !== false ? Number(b.price) || 0 : null; } catch { restBest = null; }
    targeted.push({ nft: m.watch || `${m.slug}#${tokenId}`, bookBest, restBest, at: Date.now() });
  }, 5000));
  timers.push(setInterval(() => {                       // pick up NEW items (read-only re-read)
    try { let n = 0; for (const r of loadRows(snapshotFile)) if (auditor.addRow(r)) { n++; auditor.counters.newItems = (auditor.counters.newItems || 0) + 1; } if (n) feed.setCollections(auditor.slugs()); } catch { /* snapshot mid-write */ }
  }, RESCAN_SNAPSHOT_MS));
  timers.push(setInterval(() => feed.sweep(), 30000));
  timers.push(setInterval(() => { const c = auditor.counters; log(`shadow: t=${Math.round((Date.now() - t0) / 1000)}s frames=${c.frames} mapped=${c.mappedOps} touched=${c.touchedOps} suspect=${c.suspect} mismatch=${c.mismatch}`); }, 60000));

  const t0 = Date.now();
  await new Promise(r => setTimeout(r, minutes * 60 * 1000));
  for (const t of timers) clearInterval(t);
  const st = feed.status();
  try { feed.stop(); } catch { /* down */ }
  const rep = auditor.report();
  rep.stream = { aConnected: Boolean(st.a && (st.a.connected || st.a.socketConnected)), dual: st.counters, minutes };
  rep.targetedBest = targeted;
  rep.verdict = verdict(rep, targeted, rep.counters.frames > 0);
  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  fs.writeFileSync(outFile, JSON.stringify(rep, null, 1));
  log(`shadow: report ${outFile}`); log(rep.verdict.join("\n"));
  setTimeout(() => process.exit(0), 500).unref();
}

const args = {};
for (let i = 2; i < process.argv.length; i++) {
  const a = process.argv[i];
  if (a.startsWith("--")) { const k = a.slice(2); const nx = process.argv[i + 1]; if (nx && !nx.startsWith("--")) { args[k] = nx; i++; } else args[k] = true; }
}
(args.selftest ? selftest() : live(args)).catch(e => { process.stderr.write(`shadow: ${String(e && e.message).slice(0, 200)}\n`); process.exit(1); });
