"use strict";

/**
 * tools/build-gate.js - MANDATORY closure gate. Run before any build, before any
 * scratch copy is launched in Electron, and from run-all-tests.js (first step).
 *
 *   node tools/build-gate.js                  gate the canonical tree (main entry + package.json build.files)
 *   node tools/build-gate.js <scratchDir>     gate a scratch copy of this tree: every .js in it must
 *                                             resolve its relative requires INSIDE the copy (compareCopy),
 *                                             and package.json "main" must exist there
 *
 * Exit code 1 (and a precise MISSING line) when any relative require does not resolve.
 */

const fs = require("fs");
const path = require("path");
const { checkClosure, compareCopy } = require("./require-closure-check");

const ROOT = path.resolve(__dirname, "..");
const pkg = JSON.parse(fs.readFileSync(path.join(ROOT, "package.json"), "utf8"));

function packagedEntries() {
  const entries = [pkg.main || "main.js"];
  const excluded = new Set((pkg.build.files || []).filter(f => f.startsWith("!")).map(f => f.slice(1).replace(/\\/g, "/")));
  for (const f of pkg.build.files || []) {
    if (f.startsWith("!") || f.includes("*") && !f.endsWith("/**/*")) continue;
    const n = f.replace(/\\/g, "/");
    if (n.endsWith("/**/*")) {
      const dir = n.slice(0, -5);
      const abs = path.join(ROOT, dir);
      if (!fs.existsSync(abs)) { entries.push(dir); continue; }
      const walk = rel => {
        for (const e of fs.readdirSync(path.join(ROOT, rel), { withFileTypes: true })) {
          const child = `${rel}/${e.name}`;
          if (e.isDirectory()) walk(child);
          else if (/\.(?:cjs|mjs|js)$/.test(e.name) && !excluded.has(child)) entries.push(child);
        }
      };
      walk(dir);
    } else if (/\.(?:cjs|mjs|js)$/.test(n)) entries.push(n);
  }
  return entries;
}

function gateCanonical() {
  const entries = packagedEntries();
  const r = checkClosure(ROOT, entries, { nodeModules: true });
  // Every file the closure reaches must also be listed for packaging (else asar lacks it).
  const listed = new Set(entries.map(e => e.replace(/\\/g, "/")));
  const notPackaged = r.files.map(f => f.replace(/\\/g, "/"))
    .filter(f => /\.(?:cjs|mjs|js)$/.test(f) && !listed.has(f) && !f.startsWith("node_modules/") && !f.startsWith("renderer/") && !f.startsWith("offer-item-v2/") && !f.startsWith("assets/"));
  return { ok: r.ok && notPackaged.length === 0, files: r.files.length, missing: r.missing, notPackaged };
}

function gateScratch(dir) {
  const abs = path.resolve(dir);
  const main = pkg.main || "main.js";
  const r = compareCopy(ROOT, abs, { nodeModules: false });
  const missing = r.missing.slice();
  if (!fs.existsSync(path.join(abs, main))) missing.push({ from: "(package main)", spec: main, line: 0 });
  return { ok: missing.length === 0, files: r.files.length, missing, notPackaged: [], drift: r.drift };
}

function run(scratch) {
  return scratch ? gateScratch(scratch) : gateCanonical();
}

module.exports = { run, packagedEntries };

if (require.main === module) {
  const r = run(process.argv[2]);
  for (const m of r.missing) console.error(`MISSING ${m.from}${m.line ? ":" + m.line : ""} requires "${m.spec}"`);
  for (const f of r.notPackaged) console.error(`NOT-PACKAGED ${f} is required but absent from package.json build.files`);
  console.log(r.ok ? `build-gate ok (${r.files} files resolved)` : "build-gate FAILED");
  process.exit(r.ok ? 0 : 1);
}
