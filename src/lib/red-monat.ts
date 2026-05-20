import { getCachedRedMonthCalendarConfig } from "./red-month-calendar.js";

export const RED_ANCHOR_START = new Date(2026, 0, 27);
export const RED_CYCLE_WEEKS = [4, 4, 5] as const;

export type RedPeriodInfo = {
  start: Date;
  end: Date;
  cycleIndex: number;
  periodIndexFromAnchor: number;
};

export function startOfDay(date: Date): Date {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

export function addDays(date: Date, days: number): Date {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next;
}

function isBusinessDay(date: Date): boolean {
  const day = date.getDay();
  return day >= 1 && day <= 5;
}

function alignToMonday(date: Date): Date {
  const normalized = startOfDay(date);
  const day = normalized.getDay(); // 0=Sun, 1=Mon, ... 6=Sat
  const distanceToMonday = (day + 6) % 7;
  return addDays(normalized, -distanceToMonday);
}

function addBusinessDays(date: Date, businessDays: number): Date {
  if (businessDays === 0) return startOfDay(date);
  const step = businessDays > 0 ? 1 : -1;
  let remaining = Math.abs(businessDays);
  let cursor = startOfDay(date);
  while (remaining > 0) {
    cursor = addDays(cursor, step);
    if (isBusinessDay(cursor)) remaining -= 1;
  }
  return cursor;
}

function cycleWeeksAt(index: number): number {
  const config = getCachedRedMonthCalendarConfig();
  return config.cycleWeeks[index] ?? config.cycleWeeks[0] ?? RED_CYCLE_WEEKS[0];
}

export function getRedPeriodForDate(now = new Date()): RedPeriodInfo {
  const target = startOfDay(now);
  const config = getCachedRedMonthCalendarConfig();
  const anchor = alignToMonday(config.anchorStart);
  let periodStart = new Date(anchor);
  let cycleIndex = 0;
  let periodIndexFromAnchor = 0;

  if (target >= anchor) {
    while (true) {
      const periodLengthBusinessDays = cycleWeeksAt(cycleIndex) * 5;
      const periodEnd = addBusinessDays(periodStart, periodLengthBusinessDays - 1);
      // Weekends are carried by the preceding RED period while period end remains Friday.
      const periodLookupEnd = addDays(periodEnd, 2);
      if (target <= periodLookupEnd) {
        return {
          start: periodStart,
          end: periodEnd,
          cycleIndex,
          periodIndexFromAnchor,
        };
      }
      periodStart = addBusinessDays(periodStart, periodLengthBusinessDays);
      cycleIndex = (cycleIndex + 1) % config.cycleWeeks.length;
      periodIndexFromAnchor += 1;
    }
  }

  while (target < periodStart) {
    cycleIndex = (cycleIndex - 1 + config.cycleWeeks.length) % config.cycleWeeks.length;
    periodStart = addBusinessDays(periodStart, -cycleWeeksAt(cycleIndex) * 5);
    periodIndexFromAnchor -= 1;
  }

  const periodEnd = addBusinessDays(periodStart, cycleWeeksAt(cycleIndex) * 5 - 1);
  return {
    start: periodStart,
    end: periodEnd,
    cycleIndex,
    periodIndexFromAnchor,
  };
}

export function getCurrentRedPeriod(now = new Date()): { start: Date; end: Date } {
  const period = getRedPeriodForDate(now);
  return { start: period.start, end: period.end };
}

export function getRedPeriodLabel(now = new Date()): string {
  const period = getRedPeriodForDate(now);
  const periodNumber = period.periodIndexFromAnchor + 1;
  const normalized = periodNumber > 0 ? periodNumber : 0;
  return `RED ${String(normalized).padStart(2, "0")}`;
}

export function getRedYear(now = new Date()): number {
  return getRedPeriodForDate(now).start.getFullYear();
}

