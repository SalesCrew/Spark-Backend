import assert from "node:assert/strict";
import test from "node:test";
import { planSmSeriesChange, type SeriesOccurrence, type SeriesChange } from "./sm-series.shared.js";
import { buildSmSeriesDates } from "./sm-planning.shared.js";

const edit: SeriesChange = { action: "edit", effectiveFromDate: "2026-09-14", smUserId: "new-sm", smMarketId: "market", plannedMinutes: 90, frequency: "weekly", weekdays: [1], validTo: "2026-09-28" };
function row(day: string, extra: Partial<SeriesOccurrence> = {}): SeriesOccurrence {
  return { id: day, originalWorkDate: day, originalSmUserId: "old-sm", originalSmMarketId: "market", originalMarketInternalId: "123", originalPlannedMinutes: 60, status: "planned", startedAt: null, completedAt: null, hasHistory: false, cancelledBySchedule: false, ...extra };
}
test("new dates plus existing updates preserve original objects", () => {
  const rows = [row("2026-09-14")]; const copy = structuredClone(rows);
  const plan = planSmSeriesChange(rows, edit, "2026-09-07");
  assert.deepEqual(plan.updateIds, ["2026-09-14"]); assert.deepEqual(plan.createDates, ["2026-09-21", "2026-09-28"]); assert.deepEqual(rows, copy);
});
test("completed, started, missed, historical and manually cancelled rows are protected", () => {
  for (const extra of [{ status: "completed" }, { status: "in_progress" }, { status: "missed" }, { status: "cancelled" }, { hasHistory: true }, { startedAt: new Date() }, { completedAt: new Date() }]) {
    const plan = planSmSeriesChange([row("2026-09-14", extra)], edit, "2026-09-07");
    assert.deepEqual(plan.updateIds, []); assert.deepEqual(plan.protectedIds, ["2026-09-14"]); assert.ok(!plan.createDates.includes("2026-09-14"));
  }
});
test("weekday changes cancel removed dates, never edit immutable dates", () => {
  const plan = planSmSeriesChange([row("2026-09-14")], { ...edit, weekdays: [2] }, "2026-09-07");
  assert.deepEqual(plan.cancelIds, ["2026-09-14"]); assert.deepEqual(plan.createDates, ["2026-09-15", "2026-09-22"]);
});
test("only cancellations caused by a prior schedule edit can be restored", () => {
  const plan = planSmSeriesChange([row("2026-09-14", { status: "cancelled", cancelledBySchedule: true }), row("2026-09-21", { status: "cancelled" })], edit, "2026-09-07");
  assert.deepEqual(plan.restoreIds, ["2026-09-14"]); assert.deepEqual(plan.protectedIds, ["2026-09-21"]);
});
test("stop uses the actual date and includes moved-in future work", () => {
  const plan = planSmSeriesChange([row("2026-09-07", { replacementWorkDate: "2026-09-15" }), row("2026-09-14", { replacementWorkDate: "2026-09-13" })], { action: "stop", effectiveFromDate: "2026-09-14" }, "2026-09-07");
  assert.deepEqual(plan.cancelIds, ["2026-09-07"]); assert.deepEqual(plan.createDates, []); assert.deepEqual(plan.protectedIds, ["2026-09-14"]);
});
test("edits preserve moved dates and avoid double booking their target day", () => {
  const plan = planSmSeriesChange([row("2026-09-14", { replacementWorkDate: "2026-09-21" })], edit, "2026-09-07");
  assert.deepEqual(plan.preservedDateIds, ["2026-09-14"]); assert.deepEqual(plan.blockedDates, ["2026-09-21"]); assert.deepEqual(plan.createDates, ["2026-09-28"]);
});
test("biweekly anchor survives a cutoff in an off-week and year/DST boundaries", () => {
  assert.deepEqual(buildSmSeriesDates({ validFrom: "2026-09-14", validTo: "2026-10-05", weekdays: [1], frequency: "biweekly", anchorDate: "2026-09-07" }), ["2026-09-21", "2026-10-05"]);
  assert.deepEqual(buildSmSeriesDates({ validFrom: "2026-12-28", validTo: "2027-01-20", weekdays: [1], frequency: "biweekly", anchorDate: "2026-12-21" }), ["2027-01-04", "2027-01-18"]);
});
test("restored shifted occurrences also reserve their actual day", () => {
  const plan = planSmSeriesChange([row("2026-09-14", { status: "cancelled", cancelledBySchedule: true, replacementWorkDate: "2026-09-21" })], edit, "2026-09-07");
  assert.deepEqual(plan.restoreIds, ["2026-09-14"]); assert.deepEqual(plan.blockedDates, ["2026-09-21"]); assert.deepEqual(plan.createDates, ["2026-09-28"]);
});
