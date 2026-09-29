"use strict";

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const root = __dirname;
const config = require("./package.json").build;
const excluded = new Set((config.files || [])
  .filter(item => item.startsWith("!"))
  .map(item => item.slice(1).replace(/\\/g, "/")));
const files = new Set();

function walk(relative) {
  const absolute = path.join(root, relative);
  if (!fs.existsSync(absolute)) return;
  for (const entry of fs.readdirSync(absolute, { withFileTypes: true })) {
    const child = `${relative}/${entry.name}`.replace(/\\/g, "/");
    if (entry.isDirectory()) walk(child);
    else if (entry.isFile() && /\.(?:cjs|mjs|js)$/i.test(entry.name) && !excluded.has(child)) files.add(child);
  }
}

for (const item of config.files || []) {
  if (item.startsWith("!")) continue;
  const normalized = item.replace(/\\/g, "/");
  if (normalized.endsWith("/**/*")) walk(normalized.slice(0, -4).replace(/\/+$/, ""));
  else if (/\.(?:cjs|mjs|js)$/i.test(normalized) && !normalized.includes("*")) files.add(normalized);
}

let failed = 0;
for (const file of [...files].sort()) {
  const result = spawnSync(process.execPath, ["--check", path.join(root, file)], { encoding: "utf8" });
  if (result.status === 0) process.stdout.write(`PASS syntax ${file}\n`);
  else {
    failed++;
    process.stderr.write(`FAIL syntax ${file}\n${result.stderr || result.stdout || "node --check failed"}`);
  }
}
process.stdout.write(`${files.size - failed}/${files.size} packaged JavaScript syntax checks passed\n`);
if (failed) process.exitCode = 1;
