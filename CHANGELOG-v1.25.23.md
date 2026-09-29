# v1.25.23 — first-join recovery and topic authority hardening

- Report a refused initial collection-topic join once so affected running NFTs acquire a targeted recovery owner even before the first successful join.
- Require the complete authority snapshot timestamp to be strictly newer than the topic outage timestamp before it can authorize the fallback path.
- Ensure a row blocked by an unhealthy topic establishes targeted recovery when it is resumed or otherwise reaches the topic gate.
- Expose per-row progress and topic-recovery ownership in bounded engine diagnostics, including permanent-orphan and waiting-recovery totals.
- Add regression coverage for first-join refusal, same-millisecond stale authority, paused/resumed rows, expiry during outage, recovery churn coalescing, and partial failure across 60 collections.

This is a pre-release. Automated regression and stress checks pass; real OpenSea validation and long wall soak remain outstanding.
