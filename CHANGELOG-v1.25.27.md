# v1.25.27

## OpenSea Stream

- Replaced the production Stream connection with OpenSea's maintained `@opensea/sdk/stream` client and one server-filtered global `*` subscription.
- Uses the SDK's documented `ws` transport for Electron's Node 20 runtime.
- Subscribes to item bids, collection offers, trait offers, item cancellations, order invalidations, and order revalidations. It does not subscribe to `item_received_offer`.
- Filters marketplace events against tracked collection slugs before dispatching them to the existing Offer Item engines and MemoryBook.
- Adds bounded counters for marketplace, matched, and untracked events, transport frames, reconnects, subscription recovery, and handler errors. No API key is included in logs.
- Reconciles affected collections after the official SDK restores the global subscription following a connection gap.

## Validation

- `npm run test:all`: 10/10 suites passed.
- Packaged Windows app smoke test: 23/23 checks passed.
- Real OpenSea marketplace event validation and long-duration production soak remain pending after installation.
- Windows installer is not digitally signed.
