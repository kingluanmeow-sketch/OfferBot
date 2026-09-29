"use strict";

/**
 * Ethereum dashboard.
 *
 * Intentionally thin. Every pixel and every behaviour comes from
 * dashboard-component.js, so Ethereum and Robinhood cannot drift apart:
 * same layout, same input sizes, same button sizes, same columns, same
 * spacing, same fonts, same counters, same Scan Status, same Priority,
 * same Start / Pause / Stop / Delete (spec 2, 3).
 *
 * If you are tempted to add UI code here - add it to the shared component
 * instead, behind an option, so both chains get it at once.
 */

window.OSB = window.OSB || {};

(function (OSB) {
  OSB.createEthereumDashboard = function createEthereumDashboard() {
    return OSB.createDashboard({
      chain: "ethereum",
      title: "Ethereum"
    });
  };
})(window.OSB);
