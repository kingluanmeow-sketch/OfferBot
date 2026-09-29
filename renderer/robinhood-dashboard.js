"use strict";

/**
 * Robinhood dashboard.
 *
 * Same factory, same component, same everything - only the chain key and the
 * heading differ (spec 3). The chain-specific behaviour lives in the backend
 * (robinhood-engine.js / opensea.js), never in the UI.
 */

window.OSB = window.OSB || {};

(function (OSB) {
  OSB.createRobinhoodDashboard = function createRobinhoodDashboard() {
    return OSB.createDashboard({
      chain: "robinhood",
      title: "Robinhood"
    });
  };
})(window.OSB);
