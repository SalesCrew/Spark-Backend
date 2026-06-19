import { and, asc, desc, eq, gte, lte } from "drizzle-orm";
import { db, sql as pgSql } from "./db.js";
import { getCachedRedMonthCalendarConfig, refreshRedMonthCalendarConfig } from "./red-month-calendar.js";
import {
  addDays,
  getRedPeriodForDate as getLegacyRedPeriodForDate,
  getRedPeriodLabel as getLegacyRedPeriodLabel,
  getRedYear as getLegacyRedYear,
  startOfDay,
} from "./red-monat.js";
import { redMonthPeriods, redMonthYears } from "./schema.js";

const DEFAULT_TIMEZONE = "Europe/Vienna";
const DEFAULT_PERIOD_COUNT = 13;
const DEFAULT_CYCLE_WEEKS = [4, 4, 5] as const;
const RED_MONTH_CACHE_TTL_MS = 5 * 60 * 1000;

export type RedMonthYearStatus = "draft" | "active" | "locked";

export type GeneratedRedMonthPeriod = {
  periodIndex: number;
  label: string;
  start: Date;
  end: Date;
  lookupEnd: Date;
  cycleIndex: number;
  cycleWeeks: number;
};

export type ResolvedRedMonthPeriod = GeneratedRedMonthPeriod & {
  id: string | null;
  redPeriodId: string | null;
  redMonthYearId: string | null;
  redYear: number;
  anchorStart: Date;
  yearCycleWeeks: number[];
  periodCount: number;
  periodIndexFromAnchor: number;
  timezone: string;
  status: RedMonthYearStatus;
  updatedAt: Date | null;
};

export type RedMonthYearRecord = {
  id: string;
  redYear: number;
  anchorStart: Date;
  cycleWeeks: number[];
  periodCount: number;
  timezone: string;
  status: RedMonthYearStatus;
  createdAt: Date;
  updatedAt: Date;
};

let checkedPeriodTablesReadiness = false;
let hasPeriodTables = false;
let cachedPeriods: ResolvedRedMonthPeriod[] = [];
let cachedYears: RedMonthYearRecord[] = [];
let cacheLoadedAt = 0;

function makeRedMonthError(message: string, code: string, statusCode = 409): Error & { statusCode: number; code: string } {
  const error = new Error(message) as Error & { statusCode: number; code: string };
  error.statusCode = statusCode;
  error.code = code;
  return error;
}

function toYmd(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function parseYmdDate(raw: string): Date {
  const [rawYear, rawMonth, rawDay] = raw.split("-");
  const year = Number(rawYear ?? "0");
  const month = Number(rawMonth ?? "0");
  const day = Number(rawDay ?? "0");
  const parsed = new Date(year, Math.max(0, month - 1), Math.max(1, day));
  const normalized = new Date(parsed.getFullYear(), parsed.getMonth(), parsed.getDate());
  if (toYmd(normalized) !== raw) {
    throw makeRedMonthError("Ungültiges RED-Monat Datum.", "invalid_red_month_date", 400);
  }
  return normalized;
}

function normalizeCycleWeeks(raw: unknown): number[] {
  if (!Array.isArray(raw)) return [...DEFAULT_CYCLE_WEEKS];
  const values = raw
    .map((entry) => Number(entry))
    .filter((entry) => Number.isFinite(entry) && entry > 0)
    .map((entry) => Math.floor(entry));
  return values.length > 0 ? values : [...DEFAULT_CYCLE_WEEKS];
}

function normalizePeriodCount(raw: unknown): number {
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 1) return DEFAULT_PERIOD_COUNT;
  return Math.floor(parsed);
}

function normalizeStatus(raw: unknown): RedMonthYearStatus {
  return raw === "draft" || raw === "locked" || raw === "active" ? raw : "draft";
}

function legacyPeriodForDate(date: Date): ResolvedRedMonthPeriod {
  const legacy = getLegacyRedPeriodForDate(date);
  const config = getCachedRedMonthCalendarConfig();
  const label = getLegacyRedPeriodLabel(legacy.start);
  const year = getLegacyRedYear(legacy.start);
  return {
    id: toYmd(legacy.start),
    redPeriodId: null,
    redMonthYearId: null,
    redYear: year,
    anchorStart: config.anchorStart,
    yearCycleWeeks: config.cycleWeeks,
    periodCount: DEFAULT_PERIOD_COUNT,
    periodIndex: legacy.periodIndexFromAnchor + 1,
    periodIndexFromAnchor: legacy.periodIndexFromAnchor,
    label,
    start: legacy.start,
    end: legacy.end,
    lookupEnd: addDays(legacy.end, 2),
    cycleIndex: legacy.cycleIndex,
    cycleWeeks: config.cycleWeeks[legacy.cycleIndex] ?? config.cycleWeeks[0] ?? DEFAULT_CYCLE_WEEKS[0],
    timezone: config.timezone,
    status: "active",
    updatedAt: config.updatedAt ?? null,
  };
}

export function generateRedMonthPeriodsPreview(input: {
  anchorStart: string;
  cycleWeeks?: number[];
  periodCount?: number;
}): GeneratedRedMonthPeriod[] {
  const anchorStart = parseYmdDate(input.anchorStart);
  if (anchorStart.getDay() !== 1) {
    throw makeRedMonthError("RED-Monat Startdatum muss ein Montag sein.", "red_month_anchor_not_monday", 400);
  }
  const cycleWeeks = normalizeCycleWeeks(input.cycleWeeks);
  const periodCount = normalizePeriodCount(input.periodCount);
  const periods: GeneratedRedMonthPeriod[] = [];
  let cursor = startOfDay(anchorStart);
  for (let index = 0; index < periodCount; index += 1) {
    const cycleIndex = index % cycleWeeks.length;
    const weeks = cycleWeeks[cycleIndex] ?? cycleWeeks[0] ?? DEFAULT_CYCLE_WEEKS[0];
    periods.push({
      periodIndex: index + 1,
      periodIndexFromAnchor: index,
      label: `RED ${String(index + 1).padStart(2, "0")}`,
      start: cursor,
      end: addDays(cursor, weeks * 7 - 3),
      lookupEnd: addDays(cursor, weeks * 7 - 1),
      cycleIndex,
      cycleWeeks: weeks,
    } as GeneratedRedMonthPeriod);
    cursor = addDays(cursor, weeks * 7);
  }
  return periods;
}

export async function ensureRedMonthPeriodTablesReady(): Promise<boolean> {
  if (checkedPeriodTablesReadiness) return hasPeriodTables;
  const result = await pgSql<{ red_month_years: string | null; red_month_periods: string | null }[]>`
    select
      to_regclass('public.red_month_years') as red_month_years,
      to_regclass('public.red_month_periods') as red_month_periods
  `;
  hasPeriodTables = Boolean(result[0]?.red_month_years) && Boolean(result[0]?.red_month_periods);
  checkedPeriodTablesReadiness = true;
  return hasPeriodTables;
}

function mapPeriodRow(row: {
  period: typeof redMonthPeriods.$inferSelect;
  year: typeof redMonthYears.$inferSelect;
}): ResolvedRedMonthPeriod {
  const start = parseYmdDate(row.period.startDate);
  const end = parseYmdDate(row.period.endDate);
  const lookupEnd = parseYmdDate(row.period.lookupEndDate);
  return {
    id: row.period.id,
    redPeriodId: row.period.id,
    redMonthYearId: row.period.redMonthYearId,
    redYear: row.period.redYear,
    anchorStart: parseYmdDate(row.year.anchorStart),
    yearCycleWeeks: normalizeCycleWeeks(row.year.cycleWeeks),
    periodCount: normalizePeriodCount(row.year.periodCount),
    periodIndex: row.period.periodIndex,
    periodIndexFromAnchor: row.period.periodIndex - 1,
    label: row.period.label,
    start,
    end,
    lookupEnd,
    cycleIndex: row.period.cycleIndex,
    cycleWeeks: row.period.cycleWeeks,
    timezone: row.year.timezone?.trim() || DEFAULT_TIMEZONE,
    status: normalizeStatus(row.year.status),
    updatedAt: row.period.updatedAt ?? row.year.updatedAt ?? null,
  };
}

function mapYearRow(row: typeof redMonthYears.$inferSelect): RedMonthYearRecord {
  return {
    id: row.id,
    redYear: row.redYear,
    anchorStart: parseYmdDate(row.anchorStart),
    cycleWeeks: normalizeCycleWeeks(row.cycleWeeks),
    periodCount: normalizePeriodCount(row.periodCount),
    timezone: row.timezone?.trim() || DEFAULT_TIMEZONE,
    status: normalizeStatus(row.status),
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

async function loadPeriodCache(): Promise<void> {
  if (!(await ensureRedMonthPeriodTablesReady())) {
    await refreshRedMonthCalendarConfig();
    cachedPeriods = [];
    cachedYears = [];
    cacheLoadedAt = Date.now();
    return;
  }

  const [periodRows, yearRows] = await Promise.all([
    db
      .select({
        period: redMonthPeriods,
        year: redMonthYears,
      })
      .from(redMonthPeriods)
      .innerJoin(redMonthYears, eq(redMonthYears.id, redMonthPeriods.redMonthYearId))
      .where(and(eq(redMonthPeriods.isDeleted, false), eq(redMonthYears.isDeleted, false)))
      .orderBy(asc(redMonthPeriods.startDate)),
    db
      .select()
      .from(redMonthYears)
      .where(eq(redMonthYears.isDeleted, false))
      .orderBy(desc(redMonthYears.redYear)),
  ]);

  cachedPeriods = periodRows.map(mapPeriodRow);
  cachedYears = yearRows.map(mapYearRow);
  cacheLoadedAt = Date.now();
}

export async function refreshRedMonthPeriodCache(): Promise<void> {
  checkedPeriodTablesReadiness = false;
  await loadPeriodCache();
}

async function ensureFreshCache(): Promise<void> {
  if (Date.now() - cacheLoadedAt > RED_MONTH_CACHE_TTL_MS) {
    await loadPeriodCache();
  }
}

function findCachedPeriod(date: Date): ResolvedRedMonthPeriod | null {
  const target = toYmd(startOfDay(date));
  return cachedPeriods.find((period) => target >= toYmd(period.start) && target <= toYmd(period.lookupEnd)) ?? null;
}

export async function resolveRedPeriodForDate(date = new Date()): Promise<ResolvedRedMonthPeriod> {
  if (!(await ensureRedMonthPeriodTablesReady())) {
    await refreshRedMonthCalendarConfig();
    return legacyPeriodForDate(date);
  }

  await ensureFreshCache();
  const fromCache = findCachedPeriod(date);
  if (fromCache) return fromCache;

  const targetYmd = toYmd(startOfDay(date));
  const [row] = await db
    .select({
      period: redMonthPeriods,
      year: redMonthYears,
    })
    .from(redMonthPeriods)
    .innerJoin(redMonthYears, eq(redMonthYears.id, redMonthPeriods.redMonthYearId))
    .where(
      and(
        eq(redMonthPeriods.isDeleted, false),
        eq(redMonthYears.isDeleted, false),
        lte(redMonthPeriods.startDate, targetYmd),
        gte(redMonthPeriods.lookupEndDate, targetYmd),
      ),
    )
    .orderBy(desc(redMonthPeriods.startDate))
    .limit(1);

  if (row) return mapPeriodRow(row);
  throw makeRedMonthError(
    "Für dieses Datum ist kein RED-Monat gespeichert. Bitte zuerst ein RED-Jahr im Kalender anlegen.",
    "red_month_period_missing",
  );
}

export async function resolveCurrentRedPeriod(now = new Date()): Promise<ResolvedRedMonthPeriod> {
  return resolveRedPeriodForDate(now);
}

export async function listRedMonthPeriods(input: {
  from?: string;
  to?: string;
  now?: Date;
} = {}): Promise<ResolvedRedMonthPeriod[]> {
  const now = input.now ?? new Date();
  if (!(await ensureRedMonthPeriodTablesReady())) {
    await refreshRedMonthCalendarConfig();
    const current = legacyPeriodForDate(now);
    const from = input.from ? parseYmdDate(input.from) : addDays(current.start, -200);
    const to = input.to ? parseYmdDate(input.to) : addDays(current.end, 400);
    const periods: ResolvedRedMonthPeriod[] = [];
    let cursor = getLegacyRedPeriodForDate(from).start;
    for (let i = 0; i < 500; i += 1) {
      const legacy = legacyPeriodForDate(cursor);
      if (legacy.start > to) break;
      periods.push(legacy);
      cursor = addDays(legacy.lookupEnd, 1);
    }
    return periods.filter((period) => period.lookupEnd >= from && period.start <= to);
  }

  await ensureFreshCache();
  const current = await resolveCurrentRedPeriod(now);
  const fromYmd = input.from ?? toYmd(addDays(current.start, -200));
  const toYmdValue = input.to ?? toYmd(addDays(current.end, 400));
  return cachedPeriods.filter((period) => toYmd(period.lookupEnd) >= fromYmd && toYmd(period.start) <= toYmdValue);
}

export async function listRedMonthYears(): Promise<RedMonthYearRecord[]> {
  if (!(await ensureRedMonthPeriodTablesReady())) return [];
  await ensureFreshCache();
  return [...cachedYears];
}

export async function createRedMonthYear(input: {
  redYear: number;
  anchorStart: string;
  cycleWeeks: number[];
  periodCount?: number;
  timezone?: string;
  status?: RedMonthYearStatus;
  createdByUserId?: string | null;
}): Promise<{ year: RedMonthYearRecord; periods: ResolvedRedMonthPeriod[] }> {
  if (!(await ensureRedMonthPeriodTablesReady())) {
    throw makeRedMonthError("RED-Jahr Tabellen sind noch nicht vorhanden.", "red_month_tables_missing", 503);
  }

  const periods = generateRedMonthPeriodsPreview({
    anchorStart: input.anchorStart,
    cycleWeeks: input.cycleWeeks,
    ...(input.periodCount !== undefined ? { periodCount: input.periodCount } : {}),
  });
  const cycleWeeks = normalizeCycleWeeks(input.cycleWeeks);
  const periodCount = normalizePeriodCount(input.periodCount);
  const timezone = input.timezone?.trim() || DEFAULT_TIMEZONE;
  const status = input.status ?? "draft";
  const now = new Date();

  const result = await db.transaction(async (tx) => {
    const [existing] = await tx
      .select({ id: redMonthYears.id })
      .from(redMonthYears)
      .where(and(eq(redMonthYears.redYear, input.redYear), eq(redMonthYears.isDeleted, false)))
      .limit(1);
    if (existing) {
      throw makeRedMonthError("Dieses RED-Jahr existiert bereits.", "red_month_year_exists", 409);
    }

    const [yearRow] = await tx
      .insert(redMonthYears)
      .values({
        redYear: input.redYear,
        anchorStart: input.anchorStart,
        cycleWeeks,
        periodCount,
        timezone,
        status,
        createdByUserId: input.createdByUserId ?? null,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      })
      .returning();
    if (!yearRow) {
      throw makeRedMonthError("RED-Jahr konnte nicht erstellt werden.", "red_month_year_create_failed", 500);
    }
    if (status === "active") {
      await tx
        .update(redMonthYears)
        .set({ status: "locked", updatedAt: now })
        .where(and(eq(redMonthYears.status, "active"), eq(redMonthYears.isDeleted, false)));
      await tx
        .update(redMonthYears)
        .set({ status: "active", updatedAt: now })
        .where(eq(redMonthYears.id, yearRow.id));
      yearRow.status = "active";
    }

    const insertedPeriods = await tx
      .insert(redMonthPeriods)
      .values(
        periods.map((period) => ({
          redMonthYearId: yearRow.id,
          redYear: input.redYear,
          periodIndex: period.periodIndex,
          label: period.label,
          startDate: toYmd(period.start),
          endDate: toYmd(period.end),
          lookupEndDate: toYmd(period.lookupEnd),
          cycleIndex: period.cycleIndex,
          cycleWeeks: period.cycleWeeks,
          isDeleted: false,
          deletedAt: null,
          createdAt: now,
          updatedAt: now,
        })),
      )
      .returning();

    return {
      yearRow,
      periodRows: insertedPeriods,
    };
  });

  await refreshRedMonthPeriodCache();
  const year = mapYearRow(result.yearRow);
  const resolvedPeriods = result.periodRows.map((period) => mapPeriodRow({ period, year: result.yearRow }));
  return { year, periods: resolvedPeriods };
}

export async function updateDraftRedMonthYear(input: {
  id: string;
  anchorStart: string;
  cycleWeeks: number[];
  periodCount?: number;
  timezone?: string;
}): Promise<{ year: RedMonthYearRecord; periods: ResolvedRedMonthPeriod[] }> {
  if (!(await ensureRedMonthPeriodTablesReady())) {
    throw makeRedMonthError("RED-Jahr Tabellen sind noch nicht vorhanden.", "red_month_tables_missing", 503);
  }

  const cycleWeeks = normalizeCycleWeeks(input.cycleWeeks);
  const periodCount = normalizePeriodCount(input.periodCount);
  const periods = generateRedMonthPeriodsPreview({
    anchorStart: input.anchorStart,
    cycleWeeks,
    periodCount,
  });
  const now = new Date();

  const result = await db.transaction(async (tx) => {
    const [existing] = await tx
      .select()
      .from(redMonthYears)
      .where(and(eq(redMonthYears.id, input.id), eq(redMonthYears.isDeleted, false)))
      .limit(1);
    if (!existing) throw makeRedMonthError("RED-Jahr nicht gefunden.", "red_month_year_not_found", 404);
    if (existing.status !== "draft") {
      throw makeRedMonthError("Nur zukünftige Entwürfe können bearbeitet werden.", "red_month_year_not_editable", 409);
    }

    const [yearRow] = await tx
      .update(redMonthYears)
      .set({
        anchorStart: input.anchorStart,
        cycleWeeks,
        periodCount,
        timezone: input.timezone?.trim() || existing.timezone || DEFAULT_TIMEZONE,
        updatedAt: now,
      })
      .where(eq(redMonthYears.id, existing.id))
      .returning();
    if (!yearRow) throw makeRedMonthError("RED-Jahr konnte nicht gespeichert werden.", "red_month_year_update_failed", 500);

    await tx
      .update(redMonthPeriods)
      .set({ isDeleted: true, deletedAt: now, updatedAt: now })
      .where(and(eq(redMonthPeriods.redMonthYearId, existing.id), eq(redMonthPeriods.isDeleted, false)));

    const periodRows = await tx
      .insert(redMonthPeriods)
      .values(
        periods.map((period) => ({
          redMonthYearId: yearRow.id,
          redYear: yearRow.redYear,
          periodIndex: period.periodIndex,
          label: period.label,
          startDate: toYmd(period.start),
          endDate: toYmd(period.end),
          lookupEndDate: toYmd(period.lookupEnd),
          cycleIndex: period.cycleIndex,
          cycleWeeks: period.cycleWeeks,
          isDeleted: false,
          deletedAt: null,
          createdAt: now,
          updatedAt: now,
        })),
      )
      .returning();

    return { yearRow, periodRows };
  });

  await refreshRedMonthPeriodCache();
  const year = mapYearRow(result.yearRow);
  return { year, periods: result.periodRows.map((period) => mapPeriodRow({ period, year: result.yearRow })) };
}

export async function activateRedMonthYear(id: string): Promise<RedMonthYearRecord> {
  if (!(await ensureRedMonthPeriodTablesReady())) {
    throw makeRedMonthError("RED-Jahr Tabellen sind noch nicht vorhanden.", "red_month_tables_missing", 503);
  }
  const now = new Date();
  const row = await db.transaction(async (tx) => {
    const [target] = await tx
      .select()
      .from(redMonthYears)
      .where(and(eq(redMonthYears.id, id), eq(redMonthYears.isDeleted, false)))
      .limit(1);
    if (!target) return null;
    await tx
      .update(redMonthYears)
      .set({ status: "locked", updatedAt: now })
      .where(and(eq(redMonthYears.status, "active"), eq(redMonthYears.isDeleted, false)));
    const [activated] = await tx
      .update(redMonthYears)
      .set({ status: "active", updatedAt: now })
      .where(eq(redMonthYears.id, id))
      .returning();
    return activated ?? null;
  });
  if (!row) throw makeRedMonthError("RED-Jahr nicht gefunden.", "red_month_year_not_found", 404);
  await refreshRedMonthPeriodCache();
  return mapYearRow(row);
}

export function redMonthPeriodToYmd(period: Pick<ResolvedRedMonthPeriod, "start" | "end">): {
  startYmd: string;
  endYmd: string;
} {
  return {
    startYmd: toYmd(period.start),
    endYmd: toYmd(period.end),
  };
}
