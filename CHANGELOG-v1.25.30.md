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
