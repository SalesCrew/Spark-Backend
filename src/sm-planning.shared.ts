const ISO_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const DAY_MS = 86_400_000;

export type SmPlanningFrequency = "weekly" | "biweekly";

export type SmAssignmentValueSet = {
  originalWorkDate: string;
  originalSmUserId: string;
  originalSmMarketId: string;
  originalMarketInternalId: string;
  originalPlannedMinutes: number;
  replacementWorkDate?: string | null;
  replacementSmUserId?: string | null;
  replacementSmMarketId?: string | null;
  replacementMarketInternalId?: string | null;
  replacementPlannedMinutes?: number | null;
};

export type SmAssignmentEffectiveValues = {
  workDate: string;
  smUserId: string;
  smMarketId: string;
  marketInternalId: string;
  plannedMinutes: number;
};

export function isIsoDate(value: string): boolean {
  if (!ISO_DATE_PATTERN.test(value)) return false;
  const year = Number(value.slice(0, 4));
  const month = Number(value.slice(5, 7));
  const day = Number(value.slice(8, 10));
  const date = new Date(Date.UTC(year, month - 1, day));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day;
}

export function isoDateToEpochDay(value: string): number {
  if (!isIsoDate(value)) throw new Error(`Invalid ISO date: ${value}`);
  const year = Number(value.slice(0, 4));
  const month = Number(value.slice(5, 7));
  const day = Number(value.slice(8, 10));
  return Math.floor(Date.UTC(year, month - 1, day) / DAY_MS);
}

export function epochDayToIsoDate(epochDay: number): string {
  return new Date(epochDay * DAY_MS).toISOString().slice(0, 10);
}

export function isoWeekday(value: string): number {
  const weekday = new Date(isoDateToEpochDay(value) * DAY_MS).getUTCDay();
  return weekday === 0 ? 7 : weekday;
}

export function normalizeWeekdays(values: number[]): number[] {
  return [...new Set(values)].sort((left, right) => left - right);
}

export function buildSmSeriesDates(input: {
  validFrom: string;
  validTo: string;
  weekdays: number[];
  frequency: SmPlanningFrequency;
  maxOccurrences?: number;
}): string[] {
  const startDay = isoDateToEpochDay(input.validFrom);
  const endDay = isoDateToEpochDay(input.validTo);
  if (endDay < startDay) throw new Error("Series end must not be before its start.");

  const weekdays = normalizeWeekdays(input.weekdays);
  if (weekdays.length === 0 || weekdays.some((weekday) => weekday < 1 || weekday > 7)) {
    throw new Error("A series requires valid ISO weekdays.");
  }

  const maxOccurrences = input.maxOccurrences ?? 1_000;
  const dates: string[] = [];
  const startWeekday = isoWeekday(input.validFrom);
  const firstWeekMonday = startDay - (startWeekday - 1);
  const weekInterval = input.frequency === "biweekly" ? 2 : 1;

  for (let day = startDay; day <= endDay; day += 1) {
    const weeksFromStart = Math.floor((day - firstWeekMonday) / 7);
    if (weeksFromStart % weekInterval !== 0) continue;
    const value = epochDayToIsoDate(day);
    if (!weekdays.includes(isoWeekday(value))) continue;
    dates.push(value);
    if (dates.length > maxOccurrences) throw new Error(`A series may contain at most ${maxOccurrences} assignments.`);
  }

  return dates;
}

export function resolveSmAssignmentValues(row: SmAssignmentValueSet): SmAssignmentEffectiveValues {
  return {
    workDate: row.replacementWorkDate ?? row.originalWorkDate,
    smUserId: row.replacementSmUserId ?? row.originalSmUserId,
    smMarketId: row.replacementSmMarketId ?? row.originalSmMarketId,
    marketInternalId: row.replacementMarketInternalId ?? row.originalMarketInternalId,
    plannedMinutes: row.replacementPlannedMinutes ?? row.originalPlannedMinutes,
  };
}

export function replacementOrNull<T>(original: T, requested: T): T | null {
  return Object.is(original, requested) ? null : requested;
}

export function isAssignmentPlanningMutable(status: string): boolean {
  return status === "planned" || status === "confirmed" || status === "open";
}
