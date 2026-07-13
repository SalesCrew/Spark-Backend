import type { Responses } from "openai/resources/responses/responses";
import { z } from "zod";
import type { DaySessionPayload } from "./admin-zeiterfassung.js";
import { sql as pgSql } from "./db.js";
import { loadZeiterfassungDaySessions } from "./zeiterfassung-days.js";

const TIMEZONE = "Europe/Vienna";
const YMD_PATTERN = /^\d{4}-\d{2}-\d{2}$/;
const METRICS = [
  "pure_work_minutes",
  "workday_minutes",
  "pause_minutes",
  "km_driven",
  "visit_count",
  "visit_duration_minutes",
  "extra_time_minutes",
] as const;

const inputSchema = z.object({
  gmUserIds: z.array(z.string().uuid()).max(24),
  gmQuery: z.string().trim().max(120).nullable(),
  region: z.string().trim().max(120).nullable(),
  from: z.string().regex(YMD_PATTERN).nullable(),
  to: z.string().regex(YMD_PATTERN).nullable(),
  granularity: z.enum(["day", "week", "month"]),
  metric: z.enum(METRICS),
  calculation: z.enum(["sum", "average_per_tracked_day"]),
  aggregation: z.enum(["per_gm", "team_total", "team_average"]),
  includeLive: z.boolean(),
  activeOnly: z.boolean(),
}).strict();

type Input = z.infer<typeof inputSchema>;
type GmRow = { id: string; name: string; region: string };
type Period = { key: string; label: string; from: string; to: string };

export const ADMIN_KURTI_WORKTIME_ANALYTICS_TOOL: Responses.FunctionTool = {
  type: "function",
  name: "get_worktime_analytics",
  description: "Returns exact display-ready Admin Zeiterfassung analytics for up to 24 GMs. It produces aligned daily, ISO-weekly, or monthly series for pure work, workday, pauses, KM, visit count, visit duration, or extra-time duration; supports per-GM lines, team totals, and team averages. Missing GM/period observations are null rather than zero. Use this for every work-time chart, comparison, ranking, heatmap, or trend instead of rebuilding values from raw sessions. Defaults to the last 28 days when dates are null.",
  strict: true,
  parameters: {
    type: "object",
    properties: {
      gmUserIds: { type: "array", maxItems: 24, items: { type: "string", format: "uuid" } },
      gmQuery: { type: ["string", "null"], maxLength: 120 },
      region: { type: ["string", "null"], maxLength: 120 },
      from: { type: ["string", "null"], pattern: "^\\d{4}-\\d{2}-\\d{2}$" },
      to: { type: ["string", "null"], pattern: "^\\d{4}-\\d{2}-\\d{2}$" },
      granularity: { type: "string", enum: ["day", "week", "month"] },
      metric: { type: "string", enum: METRICS },
      calculation: { type: "string", enum: ["sum", "average_per_tracked_day"] },
      aggregation: { type: "string", enum: ["per_gm", "team_total", "team_average"] },
      includeLive: { type: "boolean" },
      activeOnly: { type: "boolean" },
    },
    required: ["gmUserIds", "gmQuery", "region", "from", "to", "granularity", "metric", "calculation", "aggregation", "includeLive", "activeOnly"],
    additionalProperties: false,
  },
};

function utcDate(ymd: string): Date {
  const date = new Date(`${ymd}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime()) || date.toISOString().slice(0, 10) !== ymd) {
    throw new Error(`Invalid date: ${ymd}`);
  }
  return date;
}

function ymd(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function addDays(value: string, days: number): string {
  const date = utcDate(value);
  date.setUTCDate(date.getUTCDate() + days);
  return ymd(date);
}

function dayDistance(from: string, to: string): number {
  return Math.round((utcDate(to).getTime() - utcDate(from).getTime()) / 86_400_000) + 1;
}

function defaultTo(): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date());
}

function isoWeekStart(value: string): string {
  const date = utcDate(value);
  const day = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() - day + 1);
  return ymd(date);
}

function isoWeekNumber(value: string): number {
  const date = utcDate(value);
  const day = date.getUTCDay() || 7;
  date.setUTCDate(date.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  return Math.ceil((((date.getTime() - yearStart.getTime()) / 86_400_000) + 1) / 7);
}

function formatShortDate(value: string): string {
  return new Intl.DateTimeFormat("de-AT", { day: "2-digit", month: "2-digit", timeZone: "UTC" })
    .format(utcDate(value));
}

function periodKey(value: string, granularity: Input["granularity"]): string {
  if (granularity === "day") return value;
  if (granularity === "week") return isoWeekStart(value);
  return value.slice(0, 7);
}

function buildPeriods(from: string, to: string, granularity: Input["granularity"]): Period[] {
  const periods = new Map<string, Period>();
  for (let value = from; value <= to; value = addDays(value, 1)) {
    const key = periodKey(value, granularity);
    if (periods.has(key)) continue;
    if (granularity === "day") {
      periods.set(key, { key, label: formatShortDate(value), from: value, to: value });
    } else if (granularity === "week") {
      const end = addDays(key, 6);
      periods.set(key, {
        key,
        label: `KW ${isoWeekNumber(key)} · ${formatShortDate(key)}–${formatShortDate(end)}`,
        from: key < from ? from : key,
        to: end > to ? to : end,
      });
    } else {
      const first = `${key}-01`;
      const nextMonth = utcDate(first);
      nextMonth.setUTCMonth(nextMonth.getUTCMonth() + 1);
      const end = addDays(ymd(nextMonth), -1);
      const label = new Intl.DateTimeFormat("de-AT", { month: "short", year: "numeric", timeZone: "UTC" }).format(utcDate(first));
      periods.set(key, { key, label, from: first < from ? from : first, to: end > to ? to : end });
    }
  }
  return [...periods.values()];
}

function metricValue(session: DaySessionPayload, metric: Input["metric"]): number | null {
  if (metric === "pure_work_minutes") return session.stats.reineArbeitszeit;
  if (metric === "workday_minutes") return session.stats.arbeitstag;
  if (metric === "pause_minutes") return session.stats.pauseMin;
  if (metric === "km_driven") return session.stats.kmGefahren;
  if (metric === "visit_count") return session.stats.marktbesuche;
  if (metric === "visit_duration_minutes") {
    return session.entries.filter((entry) => entry.kind === "marktbesuch")
      .reduce((sum, entry) => sum + entry.durationMin, 0);
  }
  return session.entries.filter((entry) => entry.kind === "zusatzzeit")
    .reduce((sum, entry) => sum + entry.durationMin, 0);
}

function round(value: number): number {
  return Number(value.toFixed(2));
}

function summarize(values: Array<number | null>): { total: number | null; average: number | null; observedPeriods: number; missingPeriods: number } {
  const observed = values.filter((value): value is number => value !== null);
  return {
    total: observed.length ? round(observed.reduce((sum, value) => sum + value, 0)) : null,
    average: observed.length ? round(observed.reduce((sum, value) => sum + value, 0) / observed.length) : null,
    observedPeriods: observed.length,
    missingPeriods: values.length - observed.length,
  };
}

function rangeLimit(granularity: Input["granularity"]): number {
  if (granularity === "day") return 92;
  if (granularity === "week") return 730;
  return 1_860;
}

export async function executeAdminKurtiWorktimeAnalytics(raw: unknown): Promise<Record<string, unknown>> {
  const input = inputSchema.parse(raw);
  const to = input.to ?? defaultTo();
  const from = input.from ?? addDays(to, -27);
  const rangeDays = dayDistance(from, to);
  if (rangeDays < 1) throw new Error("The work-time range start must not be after its end.");
  if (rangeDays > rangeLimit(input.granularity)) {
    throw new Error(input.granularity === "day"
      ? "Daily work-time analytics support up to 92 days. Use weekly or monthly granularity for longer ranges."
      : "The requested work-time range is too large for this granularity.");
  }

  const query = input.gmQuery?.trim() || null;
  const region = input.region?.trim() || null;
  const requestedIds = input.gmUserIds;
  const gms = await pgSql<GmRow[]>`
    select u.id::text, concat_ws(' ', u.first_name, u.last_name) as name, coalesce(u.region, '') as region
    from users u
    where u.role = 'gm'
      and u.deleted_at is null
      and (${input.activeOnly}::boolean = false or u.is_active = true)
      and (${requestedIds.length === 0}::boolean or u.id = any(${requestedIds}::uuid[]))
      and (${query}::text is null or concat_ws(' ', u.first_name, u.last_name, u.email) ilike '%' || ${query} || '%')
      and (${region}::text is null or u.region ilike '%' || ${region} || '%')
    order by u.last_name, u.first_name
    limit 24
  `;
  const periods = buildPeriods(from, to, input.granularity);
  if (gms.length === 0) {
    return { tool: "get_worktime_analytics", range: { from, to }, metric: input.metric, series: [], periods, summaries: [], dataBasis: "No matching GMs." };
  }

  const sessions = await loadZeiterfassungDaySessions({
    from,
    to,
    gmUserIds: gms.map((gm) => gm.id),
    includeLive: input.includeLive,
    timezone: TIMEZONE,
  });
  const eligible = sessions.filter((session) => input.includeLive || session.status === "submitted");
  const byGmAndPeriod = new Map<string, number[]>();
  const trackedDaysByGm = new Map<string, Set<string>>();
  for (const session of eligible) {
    const value = metricValue(session, input.metric);
    if (value === null) continue;
    const key = `${session.gmId}::${periodKey(session.date, input.granularity)}`;
    const bucket = byGmAndPeriod.get(key) ?? [];
    bucket.push(value);
    byGmAndPeriod.set(key, bucket);
    const days = trackedDaysByGm.get(session.gmId) ?? new Set<string>();
    days.add(session.date);
    trackedDaysByGm.set(session.gmId, days);
  }

  const perGmValues = gms.map((gm) => ({
    key: gm.id,
    label: gm.name,
    region: gm.region,
    values: periods.map((period) => {
      const values = byGmAndPeriod.get(`${gm.id}::${period.key}`);
      if (!values?.length) return null;
      const total = values.reduce((sum, value) => sum + value, 0);
      return round(input.calculation === "sum" ? total : total / values.length);
    }),
  }));

  const series = input.aggregation === "per_gm" ? perGmValues : [{
    key: input.aggregation,
    label: input.aggregation === "team_total" ? "Team gesamt" : "Team-Durchschnitt",
    region: region ?? "Alle Regionen",
    values: periods.map((_, periodIndex) => {
      const observed = perGmValues.map((gm) => gm.values[periodIndex]).filter((value): value is number => value !== null);
      if (!observed.length) return null;
      const total = observed.reduce((sum, value) => sum + value, 0);
      return round(input.aggregation === "team_total" ? total : total / observed.length);
    }),
  }];

  return {
    tool: "get_worktime_analytics",
    range: { from, to, timezone: TIMEZONE, granularity: input.granularity, periodCount: periods.length },
    selection: { gmCount: gms.length, query, region, activeOnly: input.activeOnly },
    metric: input.metric,
    unit: input.metric === "km_driven" ? "km" : input.metric === "visit_count" ? "count" : "minutes",
    calculation: input.calculation,
    aggregation: input.aggregation,
    periods,
    series,
    summaries: perGmValues.map((gm) => ({
      key: gm.key,
      label: gm.label,
      region: gm.region,
      trackedDays: trackedDaysByGm.get(gm.key)?.size ?? 0,
      ...summarize(gm.values),
    })),
    completeness: periods.map((period, periodIndex) => ({
      periodKey: period.key,
      gmWithValue: perGmValues.filter((gm) => gm.values[periodIndex] !== null).length,
      gmMissing: perGmValues.filter((gm) => gm.values[periodIndex] === null).length,
    })),
    dataBasis: input.includeLive
      ? "Submitted and currently visible/live Admin Zeiterfassung day sessions; live values are provisional."
      : "Submitted Admin Zeiterfassung day sessions using the same calculation code as the admin UI.",
    missingValueRule: "null means no eligible observation; it is not zero and must remain a gap in charts.",
  };
}
