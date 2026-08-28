import assert from "node:assert/strict";
import test from "node:test";

import {
  buildSmSeriesDates,
  isAssignmentPlanningMutable,
  isIsoDate,
  normalizeWeekdays,
  replacementOrNull,
  resolveSmAssignmentValues,
} from "./sm-planning.shared.js";

test("validates calendar dates strictly", () => {
  assert.equal(isIsoDate("2028-02-29"), true);
  assert.equal(isIsoDate("2027-02-29"), false);
  assert.equal(isIsoDate("2026-13-01"), false);
  assert.equal(isIsoDate("24.08.2026"), false);
});

test("materializes weekly occurrences on the selected ISO weekdays", () => {
  assert.deepEqual(buildSmSeriesDates({
    validFrom: "2026-08-24",
    validTo: "2026-09-06",
    weekdays: [1, 3, 5],
    frequency: "weekly",
  }), ["2026-08-24", "2026-08-26", "2026-08-28", "2026-08-31", "2026-09-02", "2026-09-04"]);
});

test("anchors biweekly recurrence to the first series week", () => {
  assert.deepEqual(buildSmSeriesDates({
    validFrom: "2026-08-26",
    validTo: "2026-09-16",
    weekdays: [1, 3],
    frequency: "biweekly",
  }), ["2026-08-26", "2026-09-07", "2026-09-09"]);
});

test("normalizes weekdays and enforces the materialization limit", () => {
  assert.deepEqual(normalizeWeekdays([5, 1, 5, 3]), [1, 3, 5]);
  assert.throws(() => buildSmSeriesDates({
    validFrom: "2026-08-24",
    validTo: "2026-09-30",
    weekdays: [1, 2, 3, 4, 5],
    frequency: "weekly",
    maxOccurrences: 3,
  }), /at most 3 assignments/);
});

test("replacement values become effective without overwriting originals", () => {
  const original = {
    originalWorkDate: "2026-08-24",
    originalSmUserId: "sm-original",
    originalSmMarketId: "market-original",
    originalMarketInternalId: "120001",
    originalPlannedMinutes: 90,
  };
  assert.deepEqual(resolveSmAssignmentValues(original), {
    workDate: "2026-08-24",
    smUserId: "sm-original",
    smMarketId: "market-original",
    marketInternalId: "120001",
    plannedMinutes: 90,
  });
  assert.deepEqual(resolveSmAssignmentValues({
    ...original,
    replacementWorkDate: "2026-08-26",
    replacementSmUserId: "sm-current",
    replacementSmMarketId: "market-current",
    replacementMarketInternalId: "120099",
    replacementPlannedMinutes: 120,
  }), {
    workDate: "2026-08-26",
    smUserId: "sm-current",
    smMarketId: "market-current",
    marketInternalId: "120099",
    plannedMinutes: 120,
  });
});

test("clears a replacement when the requested value equals the original", () => {
  assert.equal(replacementOrNull("original", "replacement"), "replacement");
  assert.equal(replacementOrNull("original", "original"), null);
});

test("only pre-execution states can be replanned", () => {
  for (const status of ["planned", "confirmed", "open"]) assert.equal(isAssignmentPlanningMutable(status), true);
  for (const status of ["in_progress", "completed", "cancelled", "missed"]) assert.equal(isAssignmentPlanningMutable(status), false);
});
