"use strict";

const lower = value => String(value == null ? "" : value).trim().toLowerCase();

function tokenIdentity(event) {
  const candidates = [event?.nft, event?.asset, event?.item, event?.nft_data];
  for (const asset of candidates) {
    if (!asset || typeof asset !== "object") continue;
    const nftId = String(asset.nft_id || "");
    const identifier = String(asset.identifier || "");
    const parsed = nftId.match(/^(?:ethereum|polygon|robinhood)\/(0x[a-f0-9]{40})\/(\d+)$/i);
    const parsedIdentifier = identifier.match(/^(?:ethereum|polygon|robinhood)\/(0x[a-f0-9]{40})\/(\d+)$/i);
    const contract = lower(asset.contract?.address || (typeof asset.contract === "string" ? asset.contract : "") ||
      asset.contract_address || asset.address || parsed?.[1] || parsedIdentifier?.[1]);
    const tokenId = String(asset.token_id ?? asset.tokenId ?? parsed?.[2] ?? parsedIdentifier?.[2] ?? identifier);
    if (contract && tokenId) return { contract, tokenId };
  }
  const rawId = String(event?.nft_id || event?.item?.nft_id || "");
  const match = rawId.match(/^(?:ethereum|polygon|robinhood)\/(0x[a-f0-9]{40})\/(\d+)$/i);
  return match ? { contract: lower(match[1]), tokenId: match[2] } : null;
}

function eventType(event) {
  const type = lower(event?.event_type || event?.event || event?.type);
  if (["trait_offer", "collection_offer"].includes(type)) return type;
  if (type === "offer" || type === "item_received_bid" || type === "item_received_offer") return "offer";
  return "";
}

/** Small bounded cursor helper. It returns one page at a time and dedupes the
 * overlap window by event identity; the engine owns scheduling and priorities. */
class EventCursor {
  constructor({ overlapSeconds = 5, lookbackSeconds = 300, deepEverySeconds = 60,
    deepLookbackSeconds = 300, maxSeen = 5000, now = Date.now } = {}) {
    this.overlapSeconds = overlapSeconds;
    this.lookbackSeconds = lookbackSeconds;
    this.deepEverySeconds = deepEverySeconds;
    this.deepLookbackSeconds = deepLookbackSeconds;
    this.maxSeen = maxSeen;
    this.now = now;
    this.states = new Map();
  }

  request(slug) {
    const key = lower(slug);
    let state = this.states.get(key);
    if (!state) {
      const nowSec = Math.floor(this.now() / 1000);
      state = { after: Math.max(0, nowSec - this.lookbackSeconds), next: null,
        nextDeepAt: nowSec + this.deepEverySeconds, seen: new Map() };
      this.states.set(key, state);
    }
    if (state.next) return { after: undefined, next: state.next };
    const nowSec = Math.floor(this.now() / 1000);
    if (nowSec >= state.nextDeepAt) {
      state.nextDeepAt = nowSec + this.deepEverySeconds;
      return { after: Math.max(0, nowSec - this.deepLookbackSeconds), next: undefined };
    }
    return { after: state.after, next: undefined };
  }

  accept(slug, page) {
    const key = lower(slug);
    const state = this.states.get(key) || (this.request(key), this.states.get(key));
    const nowSec = Math.floor(this.now() / 1000);
    const events = [];
    for (const event of Array.isArray(page?.events) ? page.events : []) {
      const type = eventType(event);
      if (!type) continue;
      const hash = lower(event.order_hash || event.orderHash || event.id);
      const identity = `${type}:${hash || JSON.stringify(tokenIdentity(event) || {})}:${String(event.event_timestamp || event.eventTimestamp || "")}`;
      if (state.seen.has(identity)) continue;
      state.seen.set(identity, nowSec);
      events.push(event);
    }
    while (state.seen.size > this.maxSeen) state.seen.delete(state.seen.keys().next().value);
    state.next = typeof page?.next === "string" && page.next ? page.next : null;
    if (!state.next) state.after = Math.max(0, nowSec - this.overlapSeconds);
    return events;
  }

  prune(activeSlugs) {
    const keep = new Set((activeSlugs || []).map(lower));
    for (const key of this.states.keys()) if (!keep.has(key)) this.states.delete(key);
  }
}

module.exports = { EventCursor, tokenIdentity, eventType };
