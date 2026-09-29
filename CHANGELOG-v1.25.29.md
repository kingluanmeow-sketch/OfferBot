# v1.25.29

## Live trace completeness candidate

- Adds per-event Book before/after Best fields for tracked NFT events.
- Persists own-authority coverage, reconciliation timestamps, and whether an owned read is active when the SEND path is deferred or resumes.
- Persists the local own-order update after a successful POST, correlated to the originating Stream event.
- Does not change pricing, Max behavior, API scheduling, or Stream subscription behavior.
- This is a local live-test candidate only. Real-market behavior remains unverified; no prerelease is intended.
