# v1.25.22 — per-topic recovery ownership

- Notify the Offer Item engine immediately when a previously joined collection topic is refused or closes, while leaving the healthy socket and unrelated topics running.
- Start a coalesced full Best recovery only for running NFTs in the affected collection.
- Keep SEND blocked until a complete snapshot started after the topic outage covers that NFT; a stale read cannot open the gate.
- Let a fresh full snapshot safely authorize that topic during a prolonged reconnect, then reconcile on rejoin without duplicate recovery.
- Make the watchdog create targeted recovery ownership for stream-topic blocked rows instead of counting and re-evaluating them as ordinary orphans.
- Add fault tests for refused joins, ACK timeout, partial topic failure, reconnect, scoped recovery, stale snapshots, paused rows, healthy-topic Stream priority, latest-wins intents, and 100 accelerated outage/rejoin cycles.

This is a pre-release. The source regression suite and fault injection pass; post-release OpenSea validation and long wall soak remain outstanding.
