import { and, desc, eq } from "drizzle-orm";
import { db, sql as pgSql } from "./db.js";
import { redMonthCalendarConfig } from "./schema.js";

const DEFAULT_ANCHOR_START = new Date(2026, 0, 27);
const DEFAULT_CYCLE_WEEKS = [4, 4, 5] as const;
const DEFAULT_TIMEZONE = "Europe/Vienna";
const DEFAULT_REFRESH_INTERVAL_MS = 5 * 60 * 1000;
const STARTUP_REFRESH_DELAY_MS = 1_000;

export type ActiveRedMonthCalendarConfig = {
  anchorStart: Date;
  cycleWeeks: number[];
  timezone: string;
  updatedAt: Date | null;
};

let cachedConfig: ActiveRedMonthCalendarConfig = {
  anchorStart: new Date(DEFAULT_ANCHOR_START),
  cycleWeeks: [...DEFAULT_CYCLE_WEEKS],
  timezone: DEFAULT_TIMEZONE,
  updatedAt: null,
};
let checkedCalendarTableReadiness = false;
let hasCalendarConfigTable = false;

function copyConfig(input: ActiveRedMonthCalendarConfig): ActiveRedMonthCalendarConfig {
  return {
    anchorStart: new Date(input.anchorStart),
    cycleWeeks: [...input.cycleWeeks],
    timezone: input.timezone,
    updatedAt: input.updatedAt ? new Date(input.updatedAt) : null,
  };
}

function normalizeCycleWeeks(raw: unknown): number[] {
  if (!Array.isArray(raw)) return [...DEFAULT_CYCLE_WEEKS];
  const normalized = raw
    .map((entry) => Number(entry))
    .filter((entry) => Number.isFinite(entry) && entry > 0)
    .map((entry) => Math.floor(entry));
  return normalized.length > 0 ? normalized : [...DEFAULT_CYCLE_WEEKS];
}

function parseYmdDate(raw: string): Date {
  const [rawYear, rawMonth, rawDay] = raw.split("-");
  const year = Number(rawYear ?? "0");
  const month = Number(rawMonth ?? "0");
  const day = Number(rawDay ?? "0");
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day) || month <= 0 || day <= 0) {
    return new Date(DEFAULT_ANCHOR_START);
  }
  return new Date(year, month - 1, day);
}

function msUntilNextLocalMidnight(now = new Date()): number {
  const nextMidnight = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
  return Math.max(1_000, nextMidnight.getTime() - now.getTime());
}

export function getCachedRedMonthCalendarConfig(): ActiveRedMonthCalendarConfig {
  return copyConfig(cachedConfig);
}

async function readActiveConfigFromDb(): Promise<ActiveRedMonthCalendarConfig | null> {
  if (!(await ensureCalendarConfigTableReady())) return null;
  const [row] = await db
    .select()
    .from(redMonthCalendarConfig)
    .where(and(eq(redMonthCalendarConfig.isActive, true), eq(redMonthCalendarConfig.isDeleted, false)))
    .orderBy(desc(redMonthCalendarConfig.updatedAt))
    .limit(1);
  if (!row) return null;
  return {
    anchorStart: parseYmdDate(row.anchorStart),
    cycleWeeks: normalizeCycleWeeks(row.cycleWeeks),
    timezone: row.timezone?.trim() || DEFAULT_TIMEZONE,
    updatedAt: row.updatedAt ?? null,
  };
}

async function ensureCalendarConfigTableReady(): Promise<boolean> {
  if (checkedCalendarTableReadiness) return hasCalendarConfigTable;
  const result = await pgSql<{ red_month_calendar_config: string | null }[]>`
    select to_regclass('public.red_month_calendar_config') as red_month_calendar_config
  `;
  hasCalendarConfigTable = Boolean(result[0]?.red_month_calendar_config);
  checkedCalendarTableReadiness = true;
  return hasCalendarConfigTable;
}

export async function refreshRedMonthCalendarConfig(): Promise<ActiveRedMonthCalendarConfig> {
  const fromDb = await readActiveConfigFromDb();
  if (fromDb) {
    cachedConfig = copyConfig(fromDb);
  }
  return getCachedRedMonthCalendarConfig();
}

export async function setActiveRedMonthCalendarConfig(input: {
  anchorStart: string;
  cycleWeeks: number[];
  timezone?: string;
}): Promise<ActiveRedMonthCalendarConfig> {
  if (!(await ensureCalendarConfigTableReady())) {
    throw new Error("RED-Monat Konfigurationstabelle ist nicht vorhanden.");
  }
  const cycleWeeks = normalizeCycleWeeks(input.cycleWeeks);
  const timezone = input.timezone?.trim() || DEFAULT_TIMEZONE;
  const now = new Date();
  const [upserted] = await db.transaction(async (tx) => {
    const [activeRow] = await tx
      .select()
      .from(redMonthCalendarConfig)
      .where(and(eq(redMonthCalendarConfig.isActive, true), eq(redMonthCalendarConfig.isDeleted, false)))
      .orderBy(desc(redMonthCalendarConfig.updatedAt))
      .limit(1);
    if (activeRow) {
      return tx
        .update(redMonthCalendarConfig)
        .set({
          anchorStart: input.anchorStart,
          cycleWeeks,
          timezone,
          updatedAt: now,
        })
        .where(eq(redMonthCalendarConfig.id, activeRow.id))
        .returning();
    }
    return tx
      .insert(redMonthCalendarConfig)
      .values({
        anchorStart: input.anchorStart,
        cycleWeeks,
        timezone,
        isActive: true,
        isDeleted: false,
      })
      .returning();
  });
  if (!upserted) {
    throw new Error("RED-Monat Konfiguration konnte nicht gespeichert werden.");
  }
  const nextConfig: ActiveRedMonthCalendarConfig = {
    anchorStart: parseYmdDate(upserted.anchorStart),
    cycleWeeks: normalizeCycleWeeks(upserted.cycleWeeks),
    timezone: upserted.timezone?.trim() || DEFAULT_TIMEZONE,
    updatedAt: upserted.updatedAt ?? now,
  };
  cachedConfig = copyConfig(nextConfig);
  return getCachedRedMonthCalendarConfig();
}

export function startRedMonthCalendarScheduler(): () => void {
  let stopped = false;
  let midnightTimer: ReturnType<typeof setTimeout> | null = null;
  let running = false;

  const runRefresh = async () => {
    if (stopped || running) return;
    running = true;
    try {
      await refreshRedMonthCalendarConfig();
    } catch (error) {
      console.error("[red-month-calendar] refresh failed", error);
    } finally {
      running = false;
    }
  };

  const scheduleMidnightRefresh = () => {
    if (stopped) return;
    if (midnightTimer) clearTimeout(midnightTimer);
    midnightTimer = setTimeout(() => {
      void runRefresh();
      scheduleMidnightRefresh();
    }, msUntilNextLocalMidnight() + 1_000);
  };

  const startupTimer = setTimeout(() => {
    void runRefresh();
  }, STARTUP_REFRESH_DELAY_MS);
  const interval = setInterval(() => {
    void runRefresh();
  }, DEFAULT_REFRESH_INTERVAL_MS);
  scheduleMidnightRefresh();

  return () => {
    stopped = true;
    clearTimeout(startupTimer);
    clearInterval(interval);
    if (midnightTimer) clearTimeout(midnightTimer);
  };
}

