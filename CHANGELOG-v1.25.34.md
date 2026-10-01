# v1.25.34 - Shared credential store with Bulk Offer Cancel

## Change
- OfferBot and Bulk Offer Cancel now keep ONE machine-wide encrypted credential store in step (`shared-credentials.js`, DPAPI, file lock, atomic write, last-writer-wins by timestamp).
- Mapping is by key position: `apiKeys` <-> `apiKey1`, `apiKey2` <-> `apiKey2`, `privateKey` <-> `privateKey`, `licenseKey` <-> `licenseKey`. Key 1 and Key 2 are independent: changing or clearing one never touches the other.
- PRIMARY slot only (Tool 1, not overflow). Any other Tool never constructs, reads, writes or migrates the shared store and keeps its own wallet and keys exactly as before.
- Wallet profiles (`wallet-profiles.json`) stay per Tool and are never shared; only the primary's ACTIVE wallet is.
- A newer shared key is never swapped under running work (adopted at boot, or when the engine is idle; a licence only when this window has none).
- No secret is logged: only field names, revisions and sha256-8 fingerprints.
- Dev runtime never opens the production shared store; the test directory override is refused in a packaged build.

## Quality gates
- `tools/build-gate.js` (uses `tools/require-closure-check.js`) is MANDATORY: first step of `run-all-tests.js`, part of `npm run build` and `npm run test:all`. It resolves every relative require of the main entry and every packaged file and fails the build if one is missing or not listed in `build.files`. For a scratch copy: `node tools/build-gate.js <scratchDir>` before launching Electron.
- New native `wallet-profiles-test.js` (per-Tool profile contract + non-primary slot never touches the shared store).
- Tests: `shared-credentials-bridge-test.js`, `shared-credentials-hooks-test.js`, `shared-credentials-concurrency-test.js`.

## Unchanged
Pricing, Best/Max logic, Stream, scheduler, write pacing, cancel and recovery behavior are untouched.
