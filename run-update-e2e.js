// Spawns update-e2e-github-test.js under real Electron (not plain Node --
// electron-updater needs app.setPath), stripping ELECTRON_RUN_AS_NODE the
// same way run-all-tests.js does for its electron:true suites, since that
// var silently turns electron.exe into plain Node and breaks require("electron").app.
"use strict";
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const env = Object.fromEntries(Object.entries(process.env).filter(([k]) => k !== "ELECTRON_RUN_AS_NODE"));
const result = spawnSync(require("electron"), [path.join(__dirname, "update-e2e-github-test.js")], { stdio: "inherit", windowsHide: true, env });
process.exitCode = result.status === 0 ? 0 : 1;
