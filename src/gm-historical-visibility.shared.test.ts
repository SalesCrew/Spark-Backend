import assert from "node:assert/strict";
import test from "node:test";
import { includeGmIppPeriod } from "./gm-historical-visibility.shared.js";

test("inactive GMs keep historical IPP periods with visits or an adjustment", () => {
  assert.equal(includeGmIppPeriod({ isActive: false, includeEmptyGms: true, hasSamples: true, hasAdjustment: false }), true);
  assert.equal(includeGmIppPeriod({ isActive: false, includeEmptyGms: true, hasSamples: false, hasAdjustment: true }), true);
});

test("inactive GMs do not create empty current-period IPP rows", () => {
  assert.equal(includeGmIppPeriod({ isActive: false, includeEmptyGms: true, hasSamples: false, hasAdjustment: false }), false);
  assert.equal(includeGmIppPeriod({ isActive: true, includeEmptyGms: true, hasSamples: false, hasAdjustment: false }), true);
  assert.equal(includeGmIppPeriod({ isActive: true, includeEmptyGms: false, hasSamples: false, hasAdjustment: false }), false);
});
