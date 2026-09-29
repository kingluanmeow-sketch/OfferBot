# v1.25.30 — Stream without order events is DEGRADED, REST fallback retakes

## Root cause (live, 2026-09-29)
- The official SDK subscription ACKs and sale/transfer frames flow, but OpenSea delivered **0 order events** (bids, collection/trait offers, cancels) in a 5-minute market-wide probe. 1.25.27–1.25.29 still reported the Stream HEALTHY, so no REST fallback ran for competitor changes.
- Observed live behavior (not a documented OpenSea limit): 60 per-collection topics on one socket → 50 joined, 10 refused; split across 2 sockets → 60/60.

## Fix
- `@opensea/sdk/stream` stays primary. A joined subscription with no market-wide order event for 90 s is `DEGRADED`, never `HEALTHY`.
- DEGRADED opens the existing targeted, deduplicated, rate-limited REST recovery (Path B) for running rows; REST results feed Book → Decision → Send.
- After 5 order events within 60 s the Stream is HEALTHY again, affected collections reconcile once, and the REST fallback turns off.
- Production trace records `source` = `STREAM`, `DEGRADED_REST` or `REST` on decisions and intents.
- Pricing, Max ceiling (target == Max sends, > Max = ABOVE_MAX), own-state safety unchanged.
- A subscription re-ACK while order events are still absent keeps the outage open. Found in the live test: the SDK socket reconnects every few minutes, and each re-ACK reset every row's repair mark, so no REST read was ever newer and SENDs stayed blocked (539 `topic-not-ready` holds in ~5 minutes; 6 after the fix).

## Live validation (packaged 1.25.30, real profile, wallet and API keys, 2026-09-29)
- Stream DEGRADED 90 s after the subscription became active; REST fallback on 16 s later; 0 HTTP 429.
- metahero-generative #4637 (Step 0.0001, Max 0.0021), competitor test wallet:
  - competitor 0.0017 → Mine 0.0018 in ~20 s
  - competitor 0.0019 → Mine 0.0020 in ~80 s
  - competitor 0.0020 (tie) → Mine 0.0021 = Max in ~40 s
  - each step traced DEGRADED_REST decision → POST → SUCCESS, and OpenSea's own best-offer endpoint returned the app wallet on top.
- 150 POSTs, none above Max; ABOVE_MAX rows (5 tokens) never sent. A competitor bid above Max could not be placed live (test wallet WETH balance).
- Detection latency in DEGRADED depends on the REST sweep cadence (roughly 20–80 s), not Stream speed.
