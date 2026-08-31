import assert from "node:assert/strict";
import test from "node:test";
import { buildKuehlerVisitSlots, kuehlerSubmissionInDateRange, planKuehlerMarketAddition } from "./lib/kuehler-repeat-visits.js";

const units = [{ id: "cooler-a" }, { id: "cooler-b" }];
const submitted = (sessionId: string, kuehlerUnitId: string | null, submittedAt = "2026-08-20T10:00:00Z") => ({ sessionId, kuehlerUnitId, submittedAt: new Date(submittedAt) });

test("adding a new market plans one visit per physical cooler, or one legacy visit", () => {
  assert.deepEqual(planKuehlerMarketAddition([], 2), { assignmentSlot: 1, visitTargetCount: 2 });
  assert.deepEqual(planKuehlerMarketAddition([], 0), { assignmentSlot: 1, visitTargetCount: 1 });
});

test("adding an existing market appends a round without changing earlier assignments", () => {
  const existing = [{ assignmentSlot: 1, visitTargetCount: 2, isDeleted: false }];
  assert.deepEqual(planKuehlerMarketAddition(existing, 2), { assignmentSlot: 2, visitTargetCount: 2 });
  assert.deepEqual(existing, [{ assignmentSlot: 1, visitTargetCount: 2, isDeleted: false }]);
});

test("older implicit multi-cooler targets are filled before adding a whole new round", () => {
  const existing = [{ assignmentSlot: 1, visitTargetCount: 1, isDeleted: false }];
  const addition = planKuehlerMarketAddition(existing, 2);
  assert.equal(addition.visitTargetCount, 3);
  assert.equal(buildKuehlerVisitSlots(units, 1 + addition.visitTargetCount, []).length, 4);
});

test("soft-deleted assignments reserve their slot but do not add to the target", () => {
  assert.deepEqual(planKuehlerMarketAddition([{ assignmentSlot: 3, visitTargetCount: 9, isDeleted: true }], 2), {
    assignmentSlot: 4, visitTargetCount: 2,
  });
});

test("a submitted first round never automatically completes its repeat round", () => {
  const rows = buildKuehlerVisitSlots(units, 4, [submitted("a1", "cooler-a"), submitted("b1", "cooler-b")]);
  assert.deepEqual(rows.map((row) => [row.unit?.id, row.visitNumber, row.submission?.sessionId ?? null]), [
    ["cooler-a", 1, "a1"], ["cooler-b", 1, "b1"], ["cooler-a", 2, null], ["cooler-b", 2, null],
  ]);
});

test("each repeat occurrence retains its own exact submitted session, oldest first", () => {
  const rows = buildKuehlerVisitSlots([units[0]!], 2, [submitted("a2", "cooler-a", "2026-08-31T10:00:00Z"), submitted("a1", "cooler-a")]);
  assert.deepEqual(rows.map((row) => row.submission?.sessionId), ["a1", "a2"]);
});

test("duplicated section joins consume a submitted visit only once", () => {
  const first = submitted("a1", "cooler-a");
  const rows = buildKuehlerVisitSlots([units[0]!], 2, [first, first]);
  assert.deepEqual(rows.map((row) => row.submission?.sessionId ?? null), ["a1", null]);
});

test("extra submissions of one cooler cannot complete another cooler", () => {
  const rows = buildKuehlerVisitSlots(units, 2, [submitted("a1", "cooler-a"), submitted("a2", "cooler-a")]);
  assert.deepEqual(rows.map((row) => Boolean(row.submission)), [true, false]);
});

test("legacy visits fill empty slots once and do not duplicate unit submissions", () => {
  const rows = buildKuehlerVisitSlots(units, 4, [submitted("a1", "cooler-a"), submitted("legacy", null)]);
  assert.deepEqual(rows.map((row) => row.submission?.sessionId ?? null), ["a1", "legacy", null, null]);
  assert.equal(buildKuehlerVisitSlots([], 2, [submitted("legacy", null)]).filter((row) => row.submission).length, 1);
});

test("a draft without a submitted timestamp cannot complete a planned visit", () => {
  const rows = buildKuehlerVisitSlots(units, 2, [{ sessionId: "draft", kuehlerUnitId: "cooler-a", submittedAt: null }]);
  assert.ok(rows.every((row) => row.submission === null));
});

test("Vienna date filters are inclusive, including DST and midnight boundaries", () => {
  const range = { dateFrom: "2026-08-31", dateTo: "2026-08-31" };
  assert.equal(kuehlerSubmissionInDateRange(new Date("2026-08-30T22:00:00Z"), range), true);
  assert.equal(kuehlerSubmissionInDateRange(new Date("2026-08-31T21:59:59Z"), range), true);
  assert.equal(kuehlerSubmissionInDateRange(new Date("2026-08-31T22:00:00Z"), range), false);
  assert.equal(kuehlerSubmissionInDateRange(new Date("2026-12-30T23:00:00Z"), { dateFrom: "2026-12-31" }), true);
});

test("date filtering happens after occurrence assignment so visit 2 keeps its identity", () => {
  const rows = buildKuehlerVisitSlots([units[0]!], 2, [submitted("a1", "cooler-a"), submitted("a2", "cooler-a", "2026-08-31T10:00:00Z")]);
  const visible = rows.filter((row) => kuehlerSubmissionInDateRange(row.submission?.submittedAt ?? null, { dateFrom: "2026-08-31", dateTo: "2026-08-31" }));
  assert.deepEqual(visible.map((row) => [row.visitNumber, row.submission?.sessionId]), [[2, "a2"]]);
});
