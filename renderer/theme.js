"use strict";

/**
 * theme.js — the app's one theme switch.
 *
 * Applying a theme means stamping data-theme on the root element; every colour
 * in the stylesheet comes from variables defined for that attribute. No tab
 * carries its own theme state, so no tab can disagree with another.
 */

window.OSB = window.OSB || {};

(function (OSB) {
  const VALID = ["dark", "light"];

  /** Dark is the default: a missing or unrecognised value is not an error. */
  function normalize(value) {
    return VALID.includes(value) ? value : "dark";
  }

  function apply(value) {
    const theme = normalize(value);
    document.documentElement.setAttribute("data-theme", theme);
    return theme;
  }

  function current() {
    return normalize(document.documentElement.getAttribute("data-theme"));
  }

  OSB.theme = { apply, current, normalize, VALID };
})(window.OSB);
