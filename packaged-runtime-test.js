"use strict";

// Exercises the exact app.asar and its packaged Windows application. Every EXE
// launch goes through test-exe-sandbox.js so it cannot open the user's profile.
const fs = require("fs");
const http = require("http");
const net = require("net");
const os = require("os");
const path = require("path");
const asar = require("@electron/asar");
const { sandboxSpawn } = require("./test-exe-sandbox");

const ROOT = __dirname;
const BUILD_OUTPUT = require("./package.json").build.directories.output;
const DEFAULT_DIR = path.join(ROOT, BUILD_OUTPUT, "win-unpacked");
const EXE = path.resolve(process.argv[3] || path.join(DEFAULT_DIR, "OpenSea Offer Bot.exe"));
const ARCHIVE = path.resolve(process.argv[2] || path.join(DEFAULT_DIR, "resources", "app.asar"));
const FEATURE_FILES = [
  "renderer/sll-layout.js", "renderer/cancel.js", "renderer/bulk-offer.js",
  "bulk-ui.js", "bulk-ui-runner.js", "bulk-cancel.js", "bulk-engine.js",
  "offer-sll.js", "offer-sll-outcomes.js", "offer-item-outcomes.js", "seaport.js",
  "opensea-gql.js", "collection-detect.js"
];

let passed = 0;
let failed = 0;
function check(label, ok, detail = "") {
  if (ok) { passed++; process.stdout.write(`PASS ${label}\n`); }
  else { failed++; process.stdout.write(`FAIL ${label}${detail ? `: ${detail}` : ""}\n`); }
}

function cleanComments(source) {
  return source.replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/(^|[^:\\])\/\/.*$/gm, "$1");
}

function resolveArchivePath(from, spec, files) {
  const base = path.posix.normalize(path.posix.join(path.posix.dirname(from), spec));
  for (const candidate of [base, `${base}.js`, `${base}.cjs`, `${base}.mjs`, `${base}.json`,
    `${base}.node`, `${base}/index.js`, `${base}/index.json`]) {
    if (files.has(candidate)) return candidate;
  }
  return null;
}

function checkArchiveGraph() {
  const files = new Set(asar.listPackage(ARCHIVE).map(name =>
    String(name).replace(/\\/g, "/").replace(/^\/+/, "")));
  for (const entry of ["main.js", "opensea.js", "offer-item-v2/engine-v2.js", "preload.js", "renderer/index.html"]) {
    check(`app.asar contains ${entry}`, files.has(entry));
  }

  const missing = [];
  const queued = ["main.js", "preload.js", "wallet-preload.js"]
    .filter(file => files.has(file));
  const htmlEntries = [...files].filter(file => /^renderer\/.*\.html$/i.test(file));
  for (const htmlFile of htmlEntries) {
    const html = asar.extractFile(ARCHIVE, htmlFile).toString("utf8");
    const scripts = /<script\b[^>]*\bsrc=["']([^"']+)["']/gi;
    let match;
    while ((match = scripts.exec(html))) {
      const src = match[1];
      if (/^(?:https?:)?\/\//i.test(src)) continue;
      const candidates = src.startsWith("/")
        ? [path.posix.normalize(src.slice(1)), path.posix.normalize(path.posix.join("renderer", src.slice(1)))]
        : [path.posix.normalize(path.posix.join(path.posix.dirname(htmlFile), src))];
      const resolved = candidates.find(candidate => files.has(candidate));
      if (resolved) queued.push(resolved);
      else missing.push(`${htmlFile} -> ${src}`);
    }
  }

  const visited = new Set();
  while (queued.length) {
    const file = queued.pop();
    if (visited.has(file)) continue;
    visited.add(file);
    if (!/\.(?:js|cjs|mjs)$/i.test(file)) continue;
    let source;
    try { source = asar.extractFile(ARCHIVE, file).toString("utf8"); }
    catch (error) { missing.push(`${file} unreadable: ${error.message}`); continue; }
    const imports = /(?:\brequire\s*\(\s*|\bfrom\s*|\bimport\s*\(\s*)["'](\.{1,2}\/[^"']+)["']/g;
    let match;
    while ((match = imports.exec(cleanComments(source)))) {
      const resolved = resolveArchivePath(file, match[1], files);
      if (!resolved) missing.push(`${file} -> ${match[1]}`);
      else queued.push(resolved);
    }
  }

  check("packaged main / preload / renderer dependency closure is complete",
    missing.length === 0, missing.join(" | "));
  const featureFiles = FEATURE_FILES.filter(file => files.has(file));
  check("SLL / Cancel / Bulk feature entrypoints are absent from app.asar",
    featureFiles.length === 0, featureFiles.join(", "));
  if (["main.js", "preload.js", "renderer/app.js", "offer-item-v2/bridge.js"].every(file => files.has(file))) {
    const source = Object.fromEntries(["main.js", "preload.js", "renderer/app.js", "offer-item-v2/bridge.js"]
      .map(file => [file, asar.extractFile(ARCHIVE, file).toString("utf8")]));
    const connected = /function pushEngineState\(state\)\s*\{\s*send\("bot:update", state\)/.test(source["main.js"]) &&
      /onUpdate:\s*callback\s*=>\s*on\("bot:update", callback\)/.test(source["preload.js"]) &&
      /onUpdate\(update\s*=>\s*state\.registry\.update\(update\)\)/.test(source["renderer/app.js"]) &&
      /this\.engine\.onChange\s*=\s*\(\)\s*=>\s*this\.scheduleEmit\(\)/.test(source["offer-item-v2/bridge.js"]) &&
      /const UI_COALESCE_MS\s*=\s*16/.test(source["offer-item-v2/bridge.js"]);
    check("backend → IPC → preload → renderer update path is wired and coalesced", connected);
    check("preload subscriptions return a removeListener cleanup function",
      /ipcRenderer\.on\(channel, handler\)[\s\S]{0,140}return \(\) => ipcRenderer\.removeListener\(channel, handler\)/.test(source["preload.js"]));
  }
  return { files, missing };
}

async function checkPackagedOfferItemLoad() {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), "offerbot-packaged-runtime-"));
  try {
    asar.extractAll(ARCHIVE, temp);
    try {
      require(path.join(temp, "opensea.js"));
      require(path.join(temp, "offer-item-v2", "engine-v2.js"));
      const { decide } = require(path.join(temp, "offer-item-v2", "decision.js"));
      const { parseCriteria, tokenTraits, appliesTo, APPLIES } = require(path.join(temp, "offer-item-v2", "trait-criteria.js"));
      check("packaged Offer Item modules load with their archived dependencies", true);
      const tie = decide({ best: 0.0425, mine: 0.0425, minPrice: 0.0001, maxPrice: 0.05, step: 0.0001 });
      check("equal competitor price requires a strictly higher offer",
        tie.status === "SEND" && tie.target === 0.0426);
      const numeric = parseCriteria({ criteria: { numeric_traits: [{ type: "Level", min: 5, max: 10 }] } });
      const applies = appliesTo(numeric, tokenTraits([{ trait_type: "Level", value: "7" }]));
      check("packaged app matches active numeric trait offer ranges", applies === APPLIES.MATCH);
    } catch (error) {
      check("packaged Offer Item modules load with their archived dependencies", false,
        `${error.toString()}${error.requireStack ? `\n${error.requireStack.join("\n")}` : ""}`);
    }
  } finally {
    fs.rmSync(temp, { recursive: true, force: true });
  }
}

function getJson(url, timeoutMs = 1000) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, response => {
      let body = "";
      response.setEncoding("utf8");
      response.on("data", chunk => { body += chunk; });
      response.on("end", () => {
        try { resolve(JSON.parse(body)); } catch (error) { reject(error); }
      });
    });
    req.setTimeout(timeoutMs, () => req.destroy(new Error("request timed out")));
    req.on("error", reject);
  });
}

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close(error => error ? reject(error) : resolve(port));
    });
  });
}

async function connectCdp(url) {
  const socket = new WebSocket(url);
  await new Promise((resolve, reject) => {
    socket.addEventListener("open", resolve, { once: true });
    socket.addEventListener("error", reject, { once: true });
  });
  let sequence = 0;
  const pending = new Map();
  socket.addEventListener("message", event => {
    let message;
    try { message = JSON.parse(String(event.data)); } catch { return; }
    if (!message.id || !pending.has(message.id)) return;
    const { resolve, reject } = pending.get(message.id);
    pending.delete(message.id);
    if (message.error) reject(new Error(message.error.message));
    else resolve(message.result);
  });
  const send = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++sequence;
    const timeout = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`${method} timed out`));
    }, 5000);
    pending.set(id, {
      resolve: value => { clearTimeout(timeout); resolve(value); },
      reject: error => { clearTimeout(timeout); reject(error); }
    });
    socket.send(JSON.stringify({ id, method, params }));
  });
  return { send, close: () => socket.close() };
}

async function checkPackagedAppBoot() {
  if (!fs.existsSync(EXE)) {
    check("packaged Windows application exists", false, EXE);
    return;
  }
  const port = await freePort();
  const env = { ...process.env, OSB_LICENSE_SERVER_URL: "http://127.0.0.1:1" };
  delete env.ELECTRON_RUN_AS_NODE;
  const child = sandboxSpawn(EXE, [`--remote-debugging-port=${port}`, "--disable-background-networking"], {
    env, stdio: ["ignore", "pipe", "pipe"]
  });
  let output = "";
  child.stdout.on("data", data => { output += String(data); });
  child.stderr.on("data", data => { output += String(data); });
  let page = null;
  let browser = null;
  try {
    for (let attempt = 0; attempt < 45 && !page; attempt++) {
      if (child.exitCode !== null) break;
      await new Promise(resolve => setTimeout(resolve, 500));
      try {
        const targets = await getJson(`http://127.0.0.1:${port}/json/list`);
        page = targets.find(target => target.type === "page" && /index\.html/.test(target.url || ""));
      } catch { /* Electron is still starting. */ }
    }
    const fatal = output.split(/\r?\n/).filter(line =>
      /Uncaught Exception|Cannot find module|JavaScript error/i.test(line));
    check("packaged Windows app opens its main renderer", Boolean(page),
      fatal.join(" | ") || `exit=${child.exitCode}`);
    check("no missing-module or uncaught main-process exception", fatal.length === 0,
      fatal.join(" | "));
    if (!page) return;

    const cdp = await connectCdp(page.webSocketDebuggerUrl);
    try {
      await cdp.send("Runtime.enable");
      let result;
      for (let attempt = 0; attempt < 30; attempt++) {
        result = await cdp.send("Runtime.evaluate", {
        expression: `(() => {
          const panels = [...document.querySelectorAll('.add-panel')];
          const panel = panels.find(p => p.offsetParent !== null) || panels[0];
          return {
            title: document.title,
            readyState: document.readyState,
            bodyText: (document.body?.innerText || '').slice(0, 1200),
            bodyHTML: (document.body?.innerHTML || '').slice(0, 1000),
            tabs: [...document.querySelectorAll('.tab')].map(button => (button.textContent || '').trim()),
            panelCount: panels.length,
            textarea: Boolean(panel && panel.querySelector('textarea')),
            button: panel && [...panel.querySelectorAll('button')].some(b => /Thêm NFT/.test(b.textContent || ''))
          };
        })()`,
        returnByValue: true
        });
        const candidate = result.result.value || {};
        if (candidate.panelCount > 0 && candidate.tabs?.length >= 4) break;
        await new Promise(resolve => setTimeout(resolve, 200));
      }
      const ui = result.result.value || {};
      check("Offer Item UI renders", /OpenSea Offer Bot/i.test(ui.title || "") && ui.panelCount > 0,
        JSON.stringify(ui));
      const expectedTabs = ["Ethereum", "Robinhood", "Settings", "Logs"];
      check("active tool navigation contains Offer Item only",
        JSON.stringify(ui.tabs) === JSON.stringify(expectedTabs), JSON.stringify(ui.tabs));
      check("Add NFT path controls load", ui.textarea === true && ui.button === true,
        JSON.stringify(ui));
      const runtime = await cdp.send("Runtime.evaluate", {
        expression: `window.botAPI.getState().then(s => ({
          hasStream: Boolean(s && s.stream),
          mode: s && s.stream && s.stream.global && s.stream.global.mode,
          tracked: s && s.stream && s.stream.global && s.stream.global.tracked,
          joined: s && s.stream && s.stream.joined
        }))`,
        awaitPromise: true,
        returnByValue: true
      });
      const streamState = runtime.result.value || {};
      check("packaged Stream subsystem initializes in per-collection mode",
        streamState.hasStream && streamState.mode === "per-collection",
        JSON.stringify(streamState));
      const uiDelta = await cdp.send("Runtime.evaluate", {
        expression: `(() => {
          const rows = Array.from({ length: 120 }, (_, i) => ({
            url: 'https://ui-fixture.invalid/' + i, tokenId: String(i), name: 'Fixture ' + i,
            image: '', imageAlts: [], addedSeq: i + 1, running: true, status: 'ON_TOP', scanState: 'WATCHING',
            best: 0.0123, mine: 0.0122, bestKind: 'item', bestKnown: true,
            minPrice: 0.0001, maxPrice: 1, step: 0.0001, effectiveStep: 0.0001,
            effectiveBest: 0.0123, duration: 15, lastError: ''
          }));
          const state = { chain: 'ethereum', running: true, heartbeat: { engineRunning: true }, rows,
            counters: { total: rows.length, active: rows.length, onTop: rows.length, outbid: 0, working: 0, failed: 0 } };
          window.OSB.app.registry.update(state);
          const table = document.querySelector('[data-tab="ethereum"] .nft-table');
          const body = table && table.tBodies[0];
          const target = body && body.querySelector('tr[data-url="https://ui-fixture.invalid/20"]');
          window.__offerbotUiFixture = { table, target };
          const changed = rows.map(row => ({ ...row }));
          changed[20] = { ...changed[20], best: 0.0321, mine: 0.0320, running: false,
            status: 'PAUSED', scanState: 'STOPPED' };
          changed[21] = { ...changed[21], status: 'ERROR', scanState: 'ERROR', lastError: 'fixture recovery error' };
          window.OSB.app.registry.update({ ...state, rows: changed,
            counters: { total: changed.length, active: changed.length - 1, onTop: changed.length - 2,
              outbid: 0, working: 0, failed: 1 } });
          const row20 = body && body.querySelector('tr[data-url="https://ui-fixture.invalid/20"]');
          const row21 = body && body.querySelector('tr[data-url="https://ui-fixture.invalid/21"]');
          return {
            tableRows: body ? body.querySelectorAll('tr:not(.empty-row)').length : 0,
            sameTable: window.__offerbotUiFixture.table === table,
            sameRow: window.__offerbotUiFixture.target === row20,
            best: row20 && row20.querySelectorAll('td.cell-price')[0].textContent,
            mine: row20 && row20.querySelectorAll('td.cell-price')[1].textContent,
            paused: row20 && row20.querySelector('.status')?.textContent,
            error: row21 && row21.querySelector('td.cell-state')?.title,
            failureCount: [...document.querySelectorAll('[data-tab="ethereum"] .counter-value')].map(n => n.textContent)
          };
        })()`,
        returnByValue: true
      });
      const uiProof = uiDelta.result.value || {};
      check("renderer applies live row, Best, Mine, pause and error deltas for 120 NFTs",
        uiProof.tableRows === 120 && uiProof.best === "0.0321" && uiProof.mine === "0.032" &&
          /tạm dừng|pause/i.test(uiProof.paused || "") && /fixture recovery error/.test(uiProof.error || ""),
        JSON.stringify(uiProof));
      check("renderer retains table and row nodes across state updates",
        uiProof.sameTable === true && uiProof.sameRow === true, JSON.stringify(uiProof));
      await cdp.send("Runtime.evaluate", { expression: `document.querySelector('.tab[data-tab="settings"]')?.click()` });
      await cdp.send("Runtime.evaluate", { expression: `window.OSB.app.registry.update({chain:'ethereum',running:true,heartbeat:{engineRunning:true},rows:[{url:'https://ui-fixture.invalid/20',tokenId:'20',name:'Fixture 20',image:'',imageAlts:[],addedSeq:21,running:false,status:'PAUSED',scanState:'STOPPED',best:0.0321,mine:0.032,minPrice:0.0001,maxPrice:1,step:0.0001,duration:15,lastError:''}],counters:{total:1,active:0,onTop:0,outbid:0,working:0,failed:0}})` });
      await cdp.send("Runtime.evaluate", { expression: `document.querySelector('.tab[data-tab="ethereum"]')?.click()` });
      const reopen = await cdp.send("Runtime.evaluate", { expression: `(() => ({
        rows: document.querySelectorAll('[data-tab="ethereum"] .nft-table tbody tr:not(.empty-row)').length,
        tableSame: window.__offerbotUiFixture.table === document.querySelector('[data-tab="ethereum"] .nft-table'),
        currentBest: document.querySelector('[data-tab="ethereum"] tr[data-url="https://ui-fixture.invalid/20"] td.cell-price')?.textContent
      }))()`, returnByValue: true });
      check("hidden dashboard continues to apply updates and survives tab return",
        reopen.result.value?.rows === 1 && reopen.result.value?.tableSame === true && reopen.result.value?.currentBest === "0.0321",
        JSON.stringify(reopen.result.value));
      if (ui.textarea && ui.button) {
        await cdp.send("Runtime.evaluate", {
          expression: `(() => {
            const panel = [...document.querySelectorAll('.add-panel')].find(p => p.offsetParent !== null) || document.querySelector('.add-panel');
            [...panel.querySelectorAll('button')].find(b => /Thêm NFT/.test(b.textContent || '')).click();
          })()`, returnByValue: true
        });
        await new Promise(resolve => setTimeout(resolve, 100));
        const feedback = await cdp.send("Runtime.evaluate", {
          expression: `(() => {
            const panel = [...document.querySelectorAll('.add-panel')].find(p => p.offsetParent !== null) || document.querySelector('.add-panel');
            return (panel.querySelector('.form-error') || {}).textContent || '';
          })()`, returnByValue: true
        });
        check("Add NFT button reaches local empty-input validation without submitting", 
          /link NFT/i.test(feedback.result.value || ""), String(feedback.result.value || "").trim());
      }
    } finally { cdp.close(); }
  } catch (error) {
    check("packaged app smoke test completed", false, error.message);
  } finally {
    try {
      const version = await getJson(`http://127.0.0.1:${port}/json/version`);
      browser = await connectCdp(version.webSocketDebuggerUrl);
      await browser.send("Browser.close");
    } catch { /* The packaged app may have exited after an error. */ }
    browser?.close();
    if (child.exitCode === null) {
      child.kill();
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
}

async function main() {
  if (!fs.existsSync(ARCHIVE)) {
    check("app.asar exists", false, ARCHIVE);
  } else {
    const graph = checkArchiveGraph();
    if (graph.missing.length === 0) await checkPackagedOfferItemLoad();
    else {
      try {
        const temp = fs.mkdtempSync(path.join(os.tmpdir(), "offerbot-packaged-red-"));
        try {
          asar.extractAll(ARCHIVE, temp);
          require(path.join(temp, "offer-item-v2", "engine-v2.js"));
        } finally { fs.rmSync(temp, { recursive: true, force: true }); }
      } catch (error) {
        process.stdout.write(`PACKAGED LOAD ERROR ${error.toString()}\n`);
        if (error.requireStack) for (const item of error.requireStack) process.stdout.write(`  ${item}\n`);
      }
    }
    if (graph.missing.length === 0) await checkPackagedAppBoot();
  }
  process.stdout.write(`\n${passed}/${passed + failed} packaged runtime checks passed\n`);
  process.exitCode = failed ? 1 : 0;
}

main().catch(error => {
  process.stderr.write(`packaged runtime test failed: ${error.stack || error}\n`);
  process.exitCode = 1;
});
