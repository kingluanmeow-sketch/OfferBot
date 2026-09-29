# v1.25.25 — fair topic recovery scheduling

- Drain targeted Path B recovery candidates round-robin by collection so one large failed collection cannot monopolize recovery for other failed topics.
- Preserve per-NFT deduplication and recovery priority while rotating collections; clear per-collection queue state on Stop.
- Add a regression test where a second collection receives recovery on the next scheduling turn despite a larger first-collection backlog.

This is a pre-release. Automated tests and packaged runtime checks pass; real OpenSea validation and long wall soak remain outstanding.
