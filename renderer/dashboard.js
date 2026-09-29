"use strict";

/**
 * Dashboard registry.
 *
 * Holds one instance of the shared component per chain, routes engine pushes to
 * the right one, and exposes a single tick() that app.js drives from ONE timer
 * (spec 20 - never one interval per NFT).
 */

window.OSB = window.OSB || {};

(function (OSB) {
  function createDashboardRegistry() {
    /** chain -> dashboard instance. A Map. */
    const dashboards = new Map();

    function register(dashboard, container) {
      dashboards.set(dashboard.chain, dashboard);
      dashboard.mount(container);
      return dashboard;
    }

    function mountAll(containers) {
      register(OSB.createEthereumDashboard(), containers.ethereum);
      register(OSB.createRobinhoodDashboard(), containers.robinhood);
      return dashboards;
    }

    /** Route one engine state push. Unknown chains are ignored, not thrown on. */
    function update(state) {
      if (!state || !state.chain) return;
      const dashboard = dashboards.get(state.chain);
      if (!dashboard) return;
      dashboard.update(state);
    }

    /** Apply the whole snapshot returned by bot:getState. */
    function hydrate(fullState) {
      if (!fullState) return;

      const chains = fullState.chains || {};
      for (const [chain, state] of Object.entries(chains)) {
        const dashboard = dashboards.get(chain);
        if (dashboard) dashboard.update(state);
      }

      applyDefaults(fullState.settings);
    }

    function applyDefaults(settings) {
      if (!settings) return;
      for (const [chain, dashboard] of dashboards) {
        if (settings[chain]) dashboard.setDefaults(settings[chain]);
      }
    }

    /** Route batch progress to the dashboard that owns the chain. */
    function batchProgress(payload) {
      if (!payload || !payload.chain) return;
      const dashboard = dashboards.get(payload.chain);
      if (dashboard && dashboard.onBatchProgress) dashboard.onBatchProgress(payload);
    }

    /** One call per UI frame for every dashboard. */
    function tick(now) {
      for (const dashboard of dashboards.values()) {
        try {
          dashboard.tick(now);
        } catch (error) {
          console.error(`[dashboard:${dashboard.chain}] tick`, error);
        }
      }
    }

    function get(chain) {
      return dashboards.get(chain) || null;
    }

    return {
      mountAll, register, update, hydrate, applyDefaults, tick, get,
      batchProgress
    };
  }

  OSB.createDashboardRegistry = createDashboardRegistry;
})(window.OSB);
