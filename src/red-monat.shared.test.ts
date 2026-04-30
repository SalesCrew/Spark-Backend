import assert from "node:assert/strict";
import test from "node:test";
import { RED_ANCHOR_START, getCurrentRedPeriod, startOfDay } from "./lib/red-monat.js";

function toYmd(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

test("red monat anchor day starts the first 4-week period", async () => {
  const period = getCurrentRedPeriod(new Date(2026, 0, 27));
  assert.equal(toYmd(period.start), toYmd(RED_ANCHOR_START));
  assert.equal(toYmd(period.end), "2026-02-23");
});

test("red monat transitions at period boundary", async () => {
  const lastDayOfFirst = getCurrentRedPeriod(new Date(2026, 1, 23));
  const firstDayOfSecond = getCurrentRedPeriod(new Date(2026, 1, 24));

  assert.equal(toYmd(lastDayOfFirst.start), "2026-01-27");
  assert.equal(toYmd(lastDayOfFirst.end), "2026-02-23");
  assert.equal(toYmd(firstDayOfSecond.start), "2026-02-24");
  assert.equal(toYmd(firstDayOfSecond.end), "2026-03-23");
});

test("days until red period end is stable and non-negative around anchor", async () => {
  const daysUntilEnd = (value: Date): number => {
    const period = getCurrentRedPeriod(value);
    const today = startOfDay(value);
    return Math.max(0, Math.floor((startOfDay(period.end).getTime() - today.getTime()) / (24 * 60 * 60 * 1000)));
  };
  const oneDayBeforeAnchor = daysUntilEnd(new Date(2026, 0, 26, 12, 0, 0));
  const onAnchorDay = daysUntilEnd(new Date(2026, 0, 27, 12, 0, 0));

  assert.equal(oneDayBeforeAnchor >= 0, true);
  assert.equal(onAnchorDay, 27);
});
