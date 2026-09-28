import test from "node:test";
import assert from "node:assert/strict";
// The frontend package is CommonJS; tsx exposes its TypeScript exports under default.
const intervalModule = await import("../../src/lib/ipp-dashboard/intervals.js");
const dataModule = await import("../../src/lib/gm-dashboard/data.js");
const { buildIntervals, findPreviousYearIntervalId } = (
  "default" in intervalModule ? intervalModule.default : intervalModule
) as typeof import("../../src/lib/ipp-dashboard/intervals.js");
const { calendarToday, averageIppYtd, availabilitySummary } = (
  "default" in dataModule ? dataModule.default : dataModule
) as typeof import("../../src/lib/gm-dashboard/data.js");
import { aggregateDashboard } from "./lib/gm-dashboard.js";
import type { DashboardScope } from "./gm-dashboard.shared.js";
const scope: DashboardScope = {
  region: null,
  gmId: null,
  chain: null,
  marketId: null,
  stc: null,
};
test("ISO week-year and entire weekend, actual RED ordinal/year comparison", () => {
  const week = buildIntervals({
    mode: "week",
    now: new Date("2024-12-31T12:00:00Z"),
    count: 2,
  })[0]!;
  assert.equal(week.id, "week-2025-01");
  assert.equal(week.start, "2024-12-30");
  assert.equal(week.end, "2025-01-05");
  const calendar = [2025, 2026].flatMap((year) =>
    Array.from({ length: 13 }, (_, i) => ({
      id: `${year}-${i + 1}`,
      redPeriodId: null,
      redMonthYearId: null,
      label: `RED ${i + 1}`,
      periodIndex: i + 1,
      periodIndexFromAnchor: year === 2026 ? 100 + i : i,
      start: `${year}-${String(Math.min(i + 1, 12)).padStart(2, "0")}-01`,
      end: `${year}-${String(Math.min(i + 1, 12)).padStart(2, "0")}-28`,
      lookupEnd: "",
      year,
      status: "active" as const,
      isCurrent: false,
      daysUntilEnd: 0,
    })),
  );
  const intervals = buildIntervals({
    mode: "redmonth",
    count: 30,
    redMonthCalendar: calendar,
  });
  assert.equal(findPreviousYearIntervalId(intervals, "2026-9"), "2025-9"); // not hardcoded -12, even with 13 periods.
});
test("Vienna midnight and calendar-year-only YTD", () => {
  assert.equal(calendarToday(new Date("2026-12-31T23:30:00Z")), "2027-01-01");
  const data = aggregateDashboard(
    [
      {
        id: "old",
        label: "2025",
        shortLabel: "Old",
        start: "2025-12-01",
        end: "2025-12-31",
      },
      {
        id: "new",
        label: "2026",
        shortLabel: "New",
        start: "2026-09-01",
        end: "2026-09-30",
      },
    ],
    [],
    scope,
  );
  data.points[0]!.ipp = 100;
  data.points[1]!.ipp = 2;
  assert.equal(averageIppYtd(data.points, "2026-09-28"), 2);
  data.points[1]!.ipp = null;
  assert.equal(averageIppYtd(data.points, "2026-09-28"), null);
});
test("score distribution is counts, left chart is mean; missing categories remain empty", () => {
  const point = aggregateDashboard(
    [
      {
        id: "test",
        label: "Test",
        shortLabel: "T",
        start: "2026-09-01",
        end: "2026-09-30",
      },
    ],
    [],
    scope,
  ).points[0]!;
  point.availability.Cooler = {
    top: 2,
    mediocre: 1,
    bad: 1,
    total: 4,
    average: 62.5,
  };
  assert.deepEqual(availabilitySummary(point, "Cooler"), {
    top: 2,
    mediocre: 1,
    bad: 1,
    total: 4,
    average: 62.5,
    topPct: 50,
    mediocrePct: 25,
    badPct: 25,
  });
  assert.equal(availabilitySummary(point, "Warehouse").average, null);
  assert.equal(availabilitySummary(point, null).average, 62.5);
});
