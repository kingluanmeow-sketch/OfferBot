"use strict";
// Dependency-closure gate for scratch / test copies (run BEFORE launching Electron or any test on a copy).
// Walks every relative require()/import/export-from/require.resolve of the given entry files (recursively) and reports each one that does
// not resolve to a real file inside the tree it was copied to. A partial copy (e.g. "./opensea-gql" missing) therefore fails here with a
// precise message instead of crashing Electron main or a test at runtime. Bare module names (npm packages / builtins) are checked only
// when `nodeModules: true` (then they must be resolvable from the tree).
//
// CLI:  node tools/require-closure-check.js <treeDir> <entry...> [--node-modules]   (entries relative to treeDir; a directory = all .js in it)
//       node tools/require-closure-check.js --compare <srcTree> <copyTree>          (every file present in the copy: closure resolved INSIDE the copy)
// API:  checkClosure(treeDir, entries, opts) -> { ok, files, missing:[{from, spec, line}] }
const fs = require("fs");
const path = require("path");
const { builtinModules } = require("module");

const REQ = /(?:\brequire(?:\.resolve)?\s*\(\s*|\bfrom\s+|\bimport\s*\(\s*|\bimport\s+)(["'])([^"']+)\1/g;
const EXT = ["", ".js", ".cjs", ".mjs", ".json", ".node"];

function resolveRel(fromFile, spec) {
  const base = path.resolve(path.dirname(fromFile), spec);
  for (const e of EXT) {
    if (fs.existsSync(base + e) && fs.statSync(base + e).isFile()) return base + e;
  }
  if (fs.existsSync(base) && fs.statSync(base).isDirectory()) {
    for (const idx of ["index.js", "index.json"]) if (fs.existsSync(path.join(base, idx))) return path.join(base, idx);
    try {
      const pj = JSON.parse(fs.readFileSync(path.join(base, "package.json"), "utf8"));
      if (pj.main && fs.existsSync(path.join(base, pj.main))) return path.join(base, pj.main);
    } catch {
      /* no package.json */
    }
  }
  return null;
}

function lineOf(src, index) {
  return src.slice(0, index).split("\n").length;
}

function listEntries(treeDir, entries) {
  const out = [];
  for (const e of entries) {
    const p = path.resolve(treeDir, e);
    if (!fs.existsSync(p)) {
      out.push({ missingEntry: e });
    } else if (fs.statSync(p).isDirectory()) {
      for (const f of fs.readdirSync(p)) if (/\.(js|cjs|mjs)$/.test(f)) out.push(path.join(p, f));
    } else out.push(p);
  }
  return out;
}

function checkClosure(treeDir, entries, opts = {}) {
  const root = path.resolve(treeDir);
  const seen = new Set();
  const missing = [];
  const queue = [];
  for (const e of listEntries(root, entries)) {
    if (e.missingEntry) missing.push({ from: "(entry list)", spec: e.missingEntry, line: 0 });
    else queue.push(e);
  }
  while (queue.length) {
    const file = queue.pop();
    if (seen.has(file)) continue;
    seen.add(file);
    if (!/\.(js|cjs|mjs)$/.test(file)) continue;
    let src;
    try {
      src = fs.readFileSync(file, "utf8");
    } catch {
      continue;
    }
    REQ.lastIndex = 0;
    let m;
    while ((m = REQ.exec(src))) {
      const spec = m[2];
      if (spec.startsWith(".")) {
        const r = resolveRel(file, spec);
        if (!r) missing.push({ from: path.relative(root, file), spec, line: lineOf(src, m.index) });
        else if (!r.startsWith(root)) missing.push({ from: path.relative(root, file), spec: spec + " (resolves OUTSIDE the tree: " + r + ")", line: lineOf(src, m.index) });
        else queue.push(r);
      } else if (opts.nodeModules && /^[A-Za-z@][\w.@/-]*$/.test(spec)) { // a real module specifier (no spaces: ignores prose in comments)
        const name = spec.startsWith("@") ? spec.split("/").slice(0, 2).join("/") : spec.split("/")[0];
        if (spec.startsWith("node:") || builtinModules.includes(name) || name === "electron") continue;
        if (!fs.existsSync(path.join(root, "node_modules", ...name.split("/"), "package.json"))) missing.push({ from: path.relative(root, file), spec, line: lineOf(src, m.index) });
      }
    }
  }
  return { ok: missing.length === 0, files: [...seen].map((f) => path.relative(root, f)), missing };
}

// every .js of the copy: closure must resolve INSIDE the copy; also lists files whose content differs from the source tree
function compareCopy(srcTree, copyTree, opts = {}) {
  const files = fs.readdirSync(copyTree).filter((f) => /\.(js|cjs|mjs)$/.test(f));
  const res = checkClosure(copyTree, files, opts);
  const drift = [];
  for (const f of files) {
    const a = path.join(srcTree, f);
    const b = path.join(copyTree, f);
    if (fs.existsSync(a) && fs.readFileSync(a, "utf8") !== fs.readFileSync(b, "utf8")) drift.push(f);
  }
  return { ...res, drift };
}

module.exports = { checkClosure, compareCopy, resolveRel };

if (require.main === module) {
  const args = process.argv.slice(2);
  const nm = args.includes("--node-modules");
  const rest = args.filter((a) => a !== "--node-modules");
  let r;
  if (rest[0] === "--compare") r = compareCopy(rest[1], rest[2], { nodeModules: nm });
  else r = checkClosure(rest[0], rest.slice(1), { nodeModules: nm });
  for (const m of r.missing) console.error(`MISSING ${m.from}${m.line ? ":" + m.line : ""} requires "${m.spec}"`);
  if (r.drift && r.drift.length) console.log("drift vs source tree (informational): " + r.drift.join(", "));
  console.log(r.ok ? `closure ok (${r.files.length} files resolved)` : `closure FAILED: ${r.missing.length} unresolved require(s)`);
  process.exit(r.ok ? 0 : 1);
}
