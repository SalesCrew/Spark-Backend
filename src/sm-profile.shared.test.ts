import assert from "node:assert/strict";
import test from "node:test";
import { smProfileWeek, summarizeSmProfileWeek } from "./sm-profile.shared.js";

test("profile week follows ISO Monday to Sunday across year boundaries", () => {
  assert.deepEqual(smProfileWeek("2026-09-16"), { from: "2026-09-14", to: "2026-09-20" });
  assert.deepEqual(smProfileWeek("2027-01-01"), { from: "2026-12-28", to: "2027-01-03" });
});

test("profile totals use active assignments and current recorded times", () => {
  assert.deepEqual(summarizeSmProfileWeek([
    { status: "completed", effective: { plannedMinutes: 90 }, actualMinutes: 82 },
    { status: "planned", effective: { plannedMinutes: 60 }, actualMinutes: null },
    { status: "cancelled", effective: { plannedMinutes: 120 }, actualMinutes: 120 },
  ]), { assignmentCount: 2, completedAssignmentCount: 1, plannedMinutes: 150, actualMinutes: 82 });
  assert.deepEqual(summarizeSmProfileWeek([]), { assignmentCount: 0, completedAssignmentCount: 0, plannedMinutes: 0, actualMinutes: 0 });
});
