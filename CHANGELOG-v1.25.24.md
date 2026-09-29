# v1.25.24 — keep topic recovery in background read capacity

- Carry the RecoveryPlane priority through the Offer Item engine and chain adapter into OpenSea reads.
- Run targeted topic-gap recovery at P2 so it cannot consume the reserved urgent read capacity while healthy realtime work is active.
- Preserve P0 for urgent own-authority reads and INITIAL for first authority; propagate P2 through quick-read fallbacks as well.
- Add a regression test covering engine-to-adapter priority propagation for full, quick, and fallback reads.

This is a pre-release. Automated tests and packaged runtime checks pass; real OpenSea validation and long wall soak remain outstanding.
