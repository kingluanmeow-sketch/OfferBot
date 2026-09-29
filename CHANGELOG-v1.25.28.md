# v1.25.28

## Production trace candidate

- Adds bounded, rotated `offerbot-production-trace.jsonl` output under the app's user data directory.
- Correlates tracked Stream events through Book updates, decisions, intent ownership, quota submission, HTTP start, and local own-order commit.
- Writes only allowlisted event, NFT, price, state, and timing fields; credentials and raw OpenSea payloads are excluded.
- Does not change offer pricing, Max handling, Stream subscription behavior, or submit safety gates.
- This candidate is for production evidence collection. Real OpenSea repeated-outbid acceptance has not yet been verified.
