import { and, asc, desc, eq, gte, ilike, inArray, isNotNull, lt, lte, or, sql } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { logAction, startActionTimer } from "../lib/logger.js";
import {
  buildDaySessionPayload,
  buildGmAggregates,
  resolvePrimaryQuestionnaireType,
  toYmdInTimezone,
  validateTimelineInterval,
  type DaySessionPayload,
  type TimelineInterval,
} from "../lib/admin-zeiterfassung.js";
import { db } from "../lib/db.js";
import { requireKundeAdminPermission } from "../lib/kunde-access.js";
import {
  gmDaySessionPauses,
  gmDaySessions,
  lager,
  lagerGmAssignments,
  markets,
  timeTrackingEntries,
  users,
  visitSessionSections,
  visitSessions,
} from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const adminZeiterfassungRouter = Router();
adminZeiterfassungRouter.use(requireAuth(["admin", "kunde"]));
adminZeiterfassungRouter.use(requireKundeAdminPermission);
adminZeiterfassungRouter.use((req, res, next) => {
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "admin_zeiterfassung_read_completed", {
      req,
      action: "admin_zeiterfassung_read",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

const ymdRegex = /^\d{4}-\d{2}-\d{2}$/;
const querySchema = z
  .object({
    from: z.string().regex(ymdRegex).optional(),
    to: z.string().regex(ymdRegex).optional(),
    gmUserIds: z.string().optional(),
    region: z.string().trim().min(1).max(120).optional(),
    search: z.string().trim().max(120).optional(),
    includeLive: z.string().optional(),
    timezone: z.string().trim().min(1).max(120).optional(),
  })
  .strict();
const diaetenExportQuerySchema = z
  .object({
    month: z.coerce.number().int().min(0).max(11),
    year: z.coerce.number().int().min(2020).max(2100),
    timezone: z.string().trim().min(1).max(120).optional(),
  })
  .strict();

const editableSegmentKindSchema = z.enum(["marktbesuch", "pause", "zusatzzeit"]);
const hhmmRegex = /^([01]\d|2[0-3]):([0-5]\d)$/;
const segmentPatchSchema = z
  .object({
    sessionId: z.string().uuid(),
    startTime: z.string().regex(hhmmRegex).optional(),
    endTime: z.string().regex(hhmmRegex).optional(),
    comment: z.string().trim().max(2_000).nullable().optional(),
  })
  .strict();
const daySessionPatchSchema = z
  .object({
    startTime: z.string().regex(hhmmRegex).optional(),
    endTime: z.string().regex(hhmmRegex).optional(),
  })
  .strict();

type QueryInput = {
  from: string;
  to: string;
  gmUserIds: string[];
  region?: string;
  search?: string;
  includeLive: boolean;
  timezone: string;
};

function parseYmdCsv(value: string | undefined): string[] {
  if (!value) return [];
  return Array.from(
    new Set(
      value
        .split(",")
        .map((part) => part.trim())
        .filter((part) => part.length > 0),
    ),
  );
}

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function addDaysToYmd(ymd: string, days: number): string {
  const date = new Date(`${ymd}T12:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function toMonthYmd(year: number, month: number, day: number): string {
  const date = new Date(Date.UTC(year, month, day, 12, 0, 0, 0));
  return date.toISOString().slice(0, 10);
}

function getMonthBounds(year: number, month: number): { from: string; to: string; next: string } {
  const from = toMonthYmd(year, month, 1);
  const next = toMonthYmd(year, month + 1, 1);
  return { from, next, to: addDaysToYmd(next, -1) };
}

function serializeDate(value: Date | null | undefined): string | null {
  return value ? value.toISOString() : null;
}

function parseQuery(req: AuthedRequest): QueryInput | null {
  const raw = {
    from: typeof req.query.from === "string" ? req.query.from : undefined,
    to: typeof req.query.to === "string" ? req.query.to : undefined,
    gmUserIds: typeof req.query.gmUserIds === "string" ? req.query.gmUserIds : undefined,
    region: typeof req.query.region === "string" ? req.query.region : undefined,
    search: typeof req.query.search === "string" ? req.query.search : undefined,
    includeLive: typeof req.query.includeLive === "string" ? req.query.includeLive : undefined,
    timezone: typeof req.query.timezone === "string" ? req.query.timezone : undefined,
  };
  const parsed = querySchema.safeParse(raw);
  if (!parsed.success) return null;
  const now = new Date();
  const timezone = parsed.data.timezone ?? "Europe/Vienna";
  const defaultTo = toYmdInTimezone(now, timezone);
  const defaultFrom = addDaysToYmd(defaultTo, -30);
  const from = parsed.data.from ?? defaultFrom;
  const to = parsed.data.to ?? defaultTo;
  const includeLive = parsed.data.includeLive ? parsed.data.includeLive !== "false" : true;
  const gmUserIds = parseYmdCsv(parsed.data.gmUserIds);
  if (gmUserIds.some((id) => !isUuid(id))) return null;
  return {
    from,
    to,
    gmUserIds,
    includeLive,
    timezone,
    ...(parsed.data.region ? { region: parsed.data.region } : {}),
    ...(parsed.data.search ? { search: parsed.data.search } : {}),
  };
}

function resolveDiaetenReasonLabel(reason: string): string {
  switch (reason) {
    case "marktbesuch":
      return "Marktbesuch";
    case "sonderaufgabe":
      return "Sonderaufgabe";
    case "werkstatt":
      return "Werkstatt";
    case "lager":
      return "Lager";
    case "hotel":
    case "hoteluebernachtung":
      return "Hotel";
    case "dienstreise":
      return "Dienstreise";
    case "heimfahrt":
      return "Heimfahrt";
    case "schulung":
      return "Schulung (Auto)";
    case "arzt":
    case "arztbesuch":
      return "Arztbesuch";
    case "homeoffice":
      return "Homeoffice";
    default:
      return reason;
  }
}

function formatLagerLocation(input: { address: string; postalCode: string; city: string }): string {
  const place = [input.postalCode, input.city].map((value) => value.trim()).filter(Boolean).join(" ");
  return [input.address, place].map((value) => value.trim()).filter(Boolean).join(", ");
}

type DayRow = {
  sessionId: string;
  gmId: string;
  workDate: string;
  timezone: string;
  status: "started" | "ended" | "submitted";
  dayStartedAt: Date | null;
  dayEndedAt: Date | null;
  startKm: number | null;
  endKm: number | null;
  createdAt: Date;
  gmFirstName: string;
  gmLastName: string;
  gmRegion: string | null;
};

type DiaetenGmBucket = {
  gmId: string;
  firstName: string;
  lastName: string;
  dayTrackings: Array<{
    id: string;
    date: string;
    dayStartAt: string | null;
    dayEndAt: string | null;
    startKm: number | null;
    endKm: number | null;
  }>;
  marketVisits: Array<{
    id: string;
    createdAt: string;
    startAt: string;
    endAt: string;
    marketName: string;
    marketAddress: string;
    marketCity: string;
    marketPostalCode: string;
  }>;
  zusatzEntries: Array<{
    id: string;
    entryDate: string;
    reason: string;
    reasonLabel: string;
    startAt: string;
    endAt: string;
    isWorkTimeDeduction: boolean;
    marketName: string | null;
    location: string | null;
    schulungOrt: string | null;
  }>;
  pauses: Array<{
    id: string;
    date: string;
    startAt: string;
    endAt: string;
  }>;
};

function getTimeZoneOffsetMs(date: Date, timeZone: string): number {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const parts = formatter.formatToParts(date);
  const map = new Map(parts.map((part) => [part.type, part.value]));
  const year = Number(map.get("year") ?? "0");
  const month = Number(map.get("month") ?? "1");
  const day = Number(map.get("day") ?? "1");
  const hour = Number(map.get("hour") ?? "0");
  const minute = Number(map.get("minute") ?? "0");
  const second = Number(map.get("second") ?? "0");
  const asUtcEpoch = Date.UTC(year, month - 1, day, hour, minute, second, 0);
  return asUtcEpoch - date.getTime();
}

function parseWorkDateHmToUtc(workDate: string, hm: string, timeZone: string): Date | null {
  const match = hhmmRegex.exec(hm.trim());
  if (!match) return null;
  const [yearRaw, monthRaw, dayRaw] = workDate.split("-");
  const year = Number(yearRaw);
  const month = Number(monthRaw);
  const day = Number(dayRaw);
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (
    !Number.isFinite(year) ||
    !Number.isFinite(month) ||
    !Number.isFinite(day) ||
    !Number.isFinite(hour) ||
    !Number.isFinite(minute)
  ) {
    return null;
  }
  const utcGuess = new Date(Date.UTC(year, month - 1, day, hour, minute, 0, 0));
  const firstOffset = getTimeZoneOffsetMs(utcGuess, timeZone);
  let candidateEpoch = utcGuess.getTime() - firstOffset;
  const secondOffset = getTimeZoneOffsetMs(new Date(candidateEpoch), timeZone);
  if (secondOffset !== firstOffset) {
    candidateEpoch = utcGuess.getTime() - secondOffset;
  }
  const candidate = new Date(candidateEpoch);
  if (toYmdInTimezone(candidate, timeZone) !== workDate) return null;
  return candidate;
}

async function loadSessionIntervals(input: {
  daySessionId: string;
  gmUserId: string;
  workDate: string;
  timezone: string;
}): Promise<TimelineInterval[]> {
  const dayStartExpr = sql`(${input.workDate}::timestamp at time zone ${input.timezone})`;
  const dayEndExpr = sql`((${input.workDate}::timestamp at time zone ${input.timezone}) + interval '1 day')`;

  const [pauseRows, visitRows, extraRows] = await Promise.all([
    db
      .select({
        id: gmDaySessionPauses.id,
        startAt: gmDaySessionPauses.pauseStartedAt,
        endAt: gmDaySessionPauses.pauseEndedAt,
      })
      .from(gmDaySessionPauses)
      .where(
        and(
          eq(gmDaySessionPauses.daySessionId, input.daySessionId),
          eq(gmDaySessionPauses.isDeleted, false),
          isNotNull(gmDaySessionPauses.pauseEndedAt),
        ),
      ),
    db
      .select({
        id: visitSessions.id,
        startAt: visitSessions.startedAt,
        endAt: visitSessions.submittedAt,
      })
      .from(visitSessions)
      .where(
        and(
          eq(visitSessions.gmUserId, input.gmUserId),
          eq(visitSessions.status, "submitted"),
          eq(visitSessions.isDeleted, false),
          sql`${visitSessions.startedAt} >= ${dayStartExpr}`,
          sql`${visitSessions.startedAt} < ${dayEndExpr}`,
          isNotNull(visitSessions.submittedAt),
        ),
      ),
    db
      .select({
        id: timeTrackingEntries.id,
        startAt: timeTrackingEntries.startAt,
        endAt: timeTrackingEntries.endAt,
      })
      .from(timeTrackingEntries)
      .where(
        and(
          eq(timeTrackingEntries.gmUserId, input.gmUserId),
          eq(timeTrackingEntries.status, "submitted"),
          eq(timeTrackingEntries.isDeleted, false),
          isNotNull(timeTrackingEntries.startAt),
          isNotNull(timeTrackingEntries.endAt),
          sql`${timeTrackingEntries.startAt} >= ${dayStartExpr}`,
          sql`${timeTrackingEntries.startAt} < ${dayEndExpr}`,
        ),
      ),
  ]);

  const intervals: TimelineInterval[] = [];
  for (const row of pauseRows) {
    if (!row.endAt || row.endAt.getTime() <= row.startAt.getTime()) continue;
    intervals.push({ kind: "pause", id: row.id, startAt: row.startAt, endAt: row.endAt });
  }
  for (const row of visitRows) {
    if (!row.endAt || row.endAt.getTime() <= row.startAt.getTime()) continue;
    intervals.push({ kind: "marktbesuch", id: row.id, startAt: row.startAt, endAt: row.endAt });
  }
  for (const row of extraRows) {
    if (!row.startAt || !row.endAt || row.endAt.getTime() <= row.startAt.getTime()) continue;
    intervals.push({ kind: "zusatzzeit", id: row.id, startAt: row.startAt, endAt: row.endAt });
  }
  return intervals;
}

async function loadAdminDaySessions(input: QueryInput): Promise<DaySessionPayload[]> {
  const statuses = input.includeLive
    ? (["started", "ended", "submitted"] as const)
    : (["submitted"] as const);

  const whereClauses = [
    eq(gmDaySessions.isDeleted, false),
    eq(users.isActive, true),
    eq(users.role, "gm"),
    inArray(gmDaySessions.status, statuses),
    gte(gmDaySessions.workDate, input.from),
    lte(gmDaySessions.workDate, input.to),
  ];
  if (input.region) {
    whereClauses.push(eq(users.region, input.region));
  }
  const gmUserIds = input.gmUserIds;
  if (gmUserIds.length > 0) {
    whereClauses.push(inArray(users.id, gmUserIds));
  }
  if (input.search && input.search.trim().length > 0) {
    const q = `%${input.search.trim()}%`;
    whereClauses.push(
      or(
        ilike(users.firstName, q),
        ilike(users.lastName, q),
      ) as NonNullable<ReturnType<typeof or>>,
    );
  }

  const dayRows = await db
    .select({
      sessionId: gmDaySessions.id,
      gmId: gmDaySessions.gmUserId,
      workDate: gmDaySessions.workDate,
      timezone: gmDaySessions.timezone,
      status: gmDaySessions.status,
      dayStartedAt: gmDaySessions.dayStartedAt,
      dayEndedAt: gmDaySessions.dayEndedAt,
      startKm: gmDaySessions.startKm,
      endKm: gmDaySessions.endKm,
      createdAt: gmDaySessions.createdAt,
      gmFirstName: users.firstName,
      gmLastName: users.lastName,
      gmRegion: users.region,
    })
    .from(gmDaySessions)
    .innerJoin(users, eq(users.id, gmDaySessions.gmUserId))
    .where(and(...whereClauses))
    .orderBy(desc(gmDaySessions.workDate), asc(users.lastName), asc(users.firstName)) as DayRow[];

  if (dayRows.length === 0) return [];

  const daySessionIds = Array.from(new Set(dayRows.map((row) => row.sessionId)));
  const gmIdsFromRows = Array.from(new Set(dayRows.map((row) => row.gmId)));
  const daySessionByKey = new Map(dayRows.map((row) => [`${row.gmId}__${row.workDate}`, row.sessionId]));
  const actionsBySessionId = new Map<
    string,
    Array<{
      id: string;
      kind: "marktbesuch" | "pause" | "zusatzzeit";
      startAt: Date;
      endAt: Date;
      marketName?: string;
      marketAddress?: string;
      subtype?: string;
      comment?: string;
      questionnaireType?: string;
      questionnaireTypes?: string[];
    }>
  >();
  for (const id of daySessionIds) actionsBySessionId.set(id, []);

  const now = new Date();
  const rangeStartExpr = sql`(${input.from}::timestamp at time zone ${input.timezone})`;
  const rangeEndExpr = sql`((${input.to}::timestamp at time zone ${input.timezone}) + interval '1 day')`;

  const pauses = await db
    .select({
      id: gmDaySessionPauses.id,
      daySessionId: gmDaySessionPauses.daySessionId,
      pauseStartedAt: gmDaySessionPauses.pauseStartedAt,
      pauseEndedAt: gmDaySessionPauses.pauseEndedAt,
    })
    .from(gmDaySessionPauses)
    .where(
      and(
        inArray(gmDaySessionPauses.daySessionId, daySessionIds),
        eq(gmDaySessionPauses.isDeleted, false),
        isNotNull(gmDaySessionPauses.pauseStartedAt),
      ),
    )
    .orderBy(asc(gmDaySessionPauses.pauseStartedAt));

  for (const pause of pauses) {
    const startAt = pause.pauseStartedAt;
    const endAt = pause.pauseEndedAt ?? (input.includeLive ? now : null);
    if (!endAt || endAt.getTime() <= startAt.getTime()) continue;
    actionsBySessionId.get(pause.daySessionId)?.push({
      id: pause.id,
      kind: "pause",
      startAt,
      endAt,
    });
  }

  // Keep timeline deterministic: only persisted submitted visits count as actions.
  // Draft duplicates can exist during retries/offline flows and would otherwise double-render.
  const visitStatuses = ["submitted"] as const;
  const visitRows = await db
    .select({
      id: visitSessions.id,
      gmUserId: visitSessions.gmUserId,
      status: visitSessions.status,
      startedAt: visitSessions.startedAt,
      submittedAt: visitSessions.submittedAt,
      marketName: markets.name,
      marketAddress: markets.address,
    })
    .from(visitSessions)
    .leftJoin(markets, eq(markets.id, visitSessions.marketId))
    .where(
      and(
        inArray(visitSessions.gmUserId, gmIdsFromRows),
        inArray(visitSessions.status, visitStatuses),
        eq(visitSessions.isDeleted, false),
        isNotNull(visitSessions.startedAt),
        sql`${visitSessions.startedAt} >= ${rangeStartExpr}`,
        sql`${visitSessions.startedAt} < ${rangeEndExpr}`,
      ),
    )
    .orderBy(asc(visitSessions.startedAt));

  const visitIds = visitRows.map((row) => row.id);
  const visitSections =
    visitIds.length === 0
      ? []
      : await db
          .select({
            visitSessionId: visitSessionSections.visitSessionId,
            section: visitSessionSections.section,
          })
          .from(visitSessionSections)
          .where(and(inArray(visitSessionSections.visitSessionId, visitIds), eq(visitSessionSections.isDeleted, false)));

  const sectionTypesByVisitId = new Map<string, string[]>();
  for (const row of visitSections) {
    const bucket = sectionTypesByVisitId.get(row.visitSessionId) ?? [];
    bucket.push(row.section);
    sectionTypesByVisitId.set(row.visitSessionId, bucket);
  }

  for (const row of visitRows) {
    const startAt = row.startedAt;
    const endAt = row.submittedAt ?? (row.status === "draft" && input.includeLive ? now : null);
    if (!startAt || !endAt || endAt.getTime() <= startAt.getTime()) continue;
    const dayKey = `${row.gmUserId}__${toYmdInTimezone(startAt, input.timezone)}`;
    const sessionId = daySessionByKey.get(dayKey);
    if (!sessionId) continue;
    const sectionTypes = Array.from(
      new Set((sectionTypesByVisitId.get(row.id) ?? []).map((value) => String(value).toLowerCase())),
    );
    const primaryQuestionnaireType = resolvePrimaryQuestionnaireType(sectionTypes);
    actionsBySessionId.get(sessionId)?.push({
      id: row.id,
      kind: "marktbesuch",
      startAt,
      endAt,
      ...(row.marketName ? { marketName: row.marketName } : {}),
      ...(row.marketAddress ? { marketAddress: row.marketAddress } : {}),
      ...(primaryQuestionnaireType ? { questionnaireType: primaryQuestionnaireType } : {}),
      ...(sectionTypes.length > 0 ? { questionnaireTypes: sectionTypes } : {}),
    });
  }

  // Keep timeline deterministic: only submitted Zusatzzeiterfassung rows become canonical actions.
  const extraStatuses = ["submitted"] as const;
  const extraRows = await db
    .select({
      id: timeTrackingEntries.id,
      gmUserId: timeTrackingEntries.gmUserId,
      status: timeTrackingEntries.status,
      activityType: timeTrackingEntries.activityType,
      comment: timeTrackingEntries.comment,
      startAt: timeTrackingEntries.startAt,
      endAt: timeTrackingEntries.endAt,
      marketName: markets.name,
      marketAddress: markets.address,
    })
    .from(timeTrackingEntries)
    .leftJoin(markets, eq(markets.id, timeTrackingEntries.marketId))
    .where(
      and(
        inArray(timeTrackingEntries.gmUserId, gmIdsFromRows),
        inArray(timeTrackingEntries.status, extraStatuses),
        eq(timeTrackingEntries.isDeleted, false),
        isNotNull(timeTrackingEntries.startAt),
        sql`${timeTrackingEntries.startAt} >= ${rangeStartExpr}`,
        sql`${timeTrackingEntries.startAt} < ${rangeEndExpr}`,
      ),
    )
    .orderBy(asc(timeTrackingEntries.startAt));

  for (const row of extraRows) {
    const startAt = row.startAt;
    const endAt = row.endAt ?? (row.status === "draft" && input.includeLive ? now : null);
    if (!startAt || !endAt || endAt.getTime() <= startAt.getTime()) continue;
    const dayKey = `${row.gmUserId}__${toYmdInTimezone(startAt, input.timezone)}`;
    const sessionId = daySessionByKey.get(dayKey);
    if (!sessionId) continue;
    actionsBySessionId.get(sessionId)?.push({
      id: row.id,
      kind: "zusatzzeit",
      startAt,
      endAt,
      subtype: row.activityType,
      ...(row.comment ? { comment: row.comment } : {}),
      ...(row.marketName ? { marketName: row.marketName } : {}),
      ...(row.marketAddress ? { marketAddress: row.marketAddress } : {}),
    });
  }

  const payload = dayRows.map((row) => {
    const gmName = `${row.gmFirstName} ${row.gmLastName}`.trim();
    const startAt = row.dayStartedAt ?? row.createdAt;
    const defaultEnd = row.dayEndedAt ?? (row.status === "submitted" ? startAt : now);
    const endAt =
      defaultEnd.getTime() > startAt.getTime() ? defaultEnd : new Date(startAt.getTime() + 60_000);
    const status: "started" | "ended" | "submitted" =
      row.status === "started" || row.status === "ended" || row.status === "submitted"
        ? row.status
        : "started";
    return buildDaySessionPayload({
      sessionId: row.sessionId,
      date: row.workDate,
      gmId: row.gmId,
      gmName,
      region: row.gmRegion ?? "",
      status,
      startAt,
      endAt,
      startKm: row.startKm,
      endKm: row.endKm,
      actions: actionsBySessionId.get(row.sessionId) ?? [],
      timezone: input.timezone,
    });
  });

  return payload.sort((a, b) => b.date.localeCompare(a.date) || a.gmName.localeCompare(b.gmName));
}

adminZeiterfassungRouter.get("/days", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = parseQuery(req);
    if (!parsed) {
      res.status(400).json({ error: "Ungültige Abfrage für Admin-Zeiterfassung." });
      return;
    }
    const sessions = await loadAdminDaySessions(parsed);
    res.status(200).json({
      sessions,
      meta: {
        from: parsed.from,
        to: parsed.to,
        includeLive: parsed.includeLive,
        timezone: parsed.timezone,
        totalSessions: sessions.length,
      },
    });
  } catch (error) {
    next(error);
  }
});

adminZeiterfassungRouter.get("/gm-aggregates", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = parseQuery(req);
    if (!parsed) {
      res.status(400).json({ error: "Ungültige Abfrage für Admin-Zeiterfassung." });
      return;
    }
    const sessions = await loadAdminDaySessions(parsed);
    const rows = buildGmAggregates({ sessions, timezone: parsed.timezone });
    res.status(200).json({
      rows,
      meta: {
        from: parsed.from,
        to: parsed.to,
        includeLive: parsed.includeLive,
        timezone: parsed.timezone,
        totalGms: rows.length,
      },
    });
  } catch (error) {
    next(error);
  }
});

adminZeiterfassungRouter.get("/diaeten-export", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = diaetenExportQuerySchema.safeParse({
      month: typeof req.query.month === "string" ? req.query.month : undefined,
      year: typeof req.query.year === "string" ? req.query.year : undefined,
      timezone: typeof req.query.timezone === "string" ? req.query.timezone : undefined,
    });
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültiger Monat für den Diäten-Export." });
      return;
    }

    const { month, year } = parsed.data;
    const timezone = parsed.data.timezone ?? "Europe/Vienna";
    const bounds = getMonthBounds(year, month);
    const rangeStart = parseWorkDateHmToUtc(bounds.from, "00:00", timezone);
    const rangeEnd = parseWorkDateHmToUtc(bounds.next, "00:00", timezone);
    if (!rangeStart || !rangeEnd) {
      res.status(400).json({ error: "Zeitraum konnte nicht berechnet werden." });
      return;
    }

    const [visitRows, zusatzRows] = await Promise.all([
      db
        .select({
          id: visitSessions.id,
          gmId: visitSessions.gmUserId,
          firstName: users.firstName,
          lastName: users.lastName,
          createdAt: visitSessions.createdAt,
          startAt: visitSessions.startedAt,
          endAt: visitSessions.submittedAt,
          marketName: markets.name,
          marketAddress: markets.address,
          marketCity: markets.city,
          marketPostalCode: markets.postalCode,
        })
        .from(visitSessions)
        .innerJoin(users, eq(users.id, visitSessions.gmUserId))
        .innerJoin(markets, eq(markets.id, visitSessions.marketId))
        .where(
          and(
            eq(users.role, "gm"),
            eq(visitSessions.status, "submitted"),
            eq(visitSessions.isDeleted, false),
            isNotNull(visitSessions.submittedAt),
            gte(visitSessions.startedAt, rangeStart),
            lt(visitSessions.startedAt, rangeEnd),
          ),
        )
        .orderBy(asc(users.lastName), asc(users.firstName), asc(visitSessions.startedAt)),
      db
        .select({
          id: timeTrackingEntries.id,
          gmId: timeTrackingEntries.gmUserId,
          firstName: users.firstName,
          lastName: users.lastName,
          activityType: timeTrackingEntries.activityType,
          comment: timeTrackingEntries.comment,
          startAt: timeTrackingEntries.startAt,
          endAt: timeTrackingEntries.endAt,
          marketName: markets.name,
        })
        .from(timeTrackingEntries)
        .innerJoin(users, eq(users.id, timeTrackingEntries.gmUserId))
        .leftJoin(markets, eq(markets.id, timeTrackingEntries.marketId))
        .where(
          and(
            eq(users.role, "gm"),
            eq(timeTrackingEntries.status, "submitted"),
            eq(timeTrackingEntries.isDeleted, false),
            isNotNull(timeTrackingEntries.startAt),
            isNotNull(timeTrackingEntries.endAt),
            gte(timeTrackingEntries.startAt, rangeStart),
            lt(timeTrackingEntries.startAt, rangeEnd),
          ),
        )
        .orderBy(asc(users.lastName), asc(users.firstName), asc(timeTrackingEntries.startAt)),
    ]);

    const buckets = new Map<string, DiaetenGmBucket>();
    const ensureBucket = (gmId: string, firstName: string, lastName: string): DiaetenGmBucket => {
      const existing = buckets.get(gmId);
      if (existing) return existing;
      const created: DiaetenGmBucket = {
        gmId,
        firstName,
        lastName,
        dayTrackings: [],
        marketVisits: [],
        zusatzEntries: [],
        pauses: [],
      };
      buckets.set(gmId, created);
      return created;
    };

    const lagerGmIds = Array.from(new Set(zusatzRows.filter((row) => row.activityType === "lager").map((row) => row.gmId)));
    const lagerLocationByGmId = new Map<string, string>();
    if (lagerGmIds.length > 0) {
      const assignedLagerRows = await db
        .select({
          gmId: lagerGmAssignments.gmUserId,
          address: lager.address,
          postalCode: lager.postalCode,
          city: lager.city,
        })
        .from(lagerGmAssignments)
        .innerJoin(lager, eq(lager.id, lagerGmAssignments.lagerId))
        .where(
          and(
            inArray(lagerGmAssignments.gmUserId, lagerGmIds),
            eq(lagerGmAssignments.isDeleted, false),
            eq(lager.isDeleted, false),
          ),
        )
        .orderBy(asc(lagerGmAssignments.createdAt));
      for (const row of assignedLagerRows) {
        if (!lagerLocationByGmId.has(row.gmId)) {
          lagerLocationByGmId.set(row.gmId, formatLagerLocation(row));
        }
      }

      const missingLegacyGmIds = lagerGmIds.filter((gmId) => !lagerLocationByGmId.has(gmId));
      if (missingLegacyGmIds.length > 0) {
        const legacyLagerRows = await db
          .select({
            gmId: lager.gmUserId,
            address: lager.address,
            postalCode: lager.postalCode,
            city: lager.city,
          })
          .from(lager)
          .where(and(inArray(lager.gmUserId, missingLegacyGmIds), eq(lager.isDeleted, false)))
          .orderBy(asc(lager.createdAt));
        for (const row of legacyLagerRows) {
          if (row.gmId && !lagerLocationByGmId.has(row.gmId)) {
            lagerLocationByGmId.set(row.gmId, formatLagerLocation(row));
          }
        }
      }
    }

    for (const row of visitRows) {
      if (!row.endAt || row.endAt.getTime() <= row.startAt.getTime()) continue;
      const bucket = ensureBucket(row.gmId, row.firstName, row.lastName);
      bucket.marketVisits.push({
        id: row.id,
        createdAt: row.createdAt.toISOString(),
        startAt: row.startAt.toISOString(),
        endAt: row.endAt.toISOString(),
        marketName: row.marketName,
        marketAddress: row.marketAddress,
        marketCity: row.marketCity,
        marketPostalCode: row.marketPostalCode,
      });
    }

    for (const row of zusatzRows) {
      if (!row.startAt || !row.endAt || row.endAt.getTime() <= row.startAt.getTime()) continue;
      const bucket = ensureBucket(row.gmId, row.firstName, row.lastName);
      const reason = row.activityType;
      const location = reason === "lager" ? (lagerLocationByGmId.get(row.gmId) ?? null) : null;
      bucket.zusatzEntries.push({
        id: row.id,
        entryDate: toYmdInTimezone(row.startAt, timezone),
        reason,
        reasonLabel: resolveDiaetenReasonLabel(reason),
        startAt: row.startAt.toISOString(),
        endAt: row.endAt.toISOString(),
        isWorkTimeDeduction: false,
        marketName: row.marketName ?? null,
        location,
        schulungOrt: null,
      });
    }

    const gmIds = Array.from(buckets.keys());
    if (gmIds.length > 0) {
      const [dayRows, pauseRows] = await Promise.all([
        db
          .select({
            id: gmDaySessions.id,
            gmId: gmDaySessions.gmUserId,
            workDate: gmDaySessions.workDate,
            dayStartedAt: gmDaySessions.dayStartedAt,
            dayEndedAt: gmDaySessions.dayEndedAt,
            startKm: gmDaySessions.startKm,
            endKm: gmDaySessions.endKm,
          })
          .from(gmDaySessions)
          .where(
            and(
              inArray(gmDaySessions.gmUserId, gmIds),
              eq(gmDaySessions.isDeleted, false),
              gte(gmDaySessions.workDate, bounds.from),
              lte(gmDaySessions.workDate, bounds.to),
            ),
          )
          .orderBy(asc(gmDaySessions.workDate)),
        db
          .select({
            id: gmDaySessionPauses.id,
            gmId: gmDaySessionPauses.gmUserId,
            workDate: gmDaySessions.workDate,
            startAt: gmDaySessionPauses.pauseStartedAt,
            endAt: gmDaySessionPauses.pauseEndedAt,
          })
          .from(gmDaySessionPauses)
          .innerJoin(gmDaySessions, eq(gmDaySessions.id, gmDaySessionPauses.daySessionId))
          .where(
            and(
              inArray(gmDaySessionPauses.gmUserId, gmIds),
              eq(gmDaySessionPauses.isDeleted, false),
              eq(gmDaySessions.isDeleted, false),
              isNotNull(gmDaySessionPauses.pauseEndedAt),
              gte(gmDaySessions.workDate, bounds.from),
              lte(gmDaySessions.workDate, bounds.to),
            ),
          )
          .orderBy(asc(gmDaySessionPauses.pauseStartedAt)),
      ]);

      for (const row of dayRows) {
        const bucket = buckets.get(row.gmId);
        if (!bucket) continue;
        bucket.dayTrackings.push({
          id: row.id,
          date: row.workDate,
          dayStartAt: serializeDate(row.dayStartedAt),
          dayEndAt: serializeDate(row.dayEndedAt),
          startKm: row.startKm,
          endKm: row.endKm,
        });
      }

      for (const row of pauseRows) {
        if (!row.endAt || row.endAt.getTime() <= row.startAt.getTime()) continue;
        const bucket = buckets.get(row.gmId);
        if (!bucket) continue;
        bucket.pauses.push({
          id: row.id,
          date: row.workDate,
          startAt: row.startAt.toISOString(),
          endAt: row.endAt.toISOString(),
        });
      }
    }

    const gls = Array.from(buckets.values()).sort((a, b) => {
      const last = a.lastName.localeCompare(b.lastName, "de-AT");
      if (last !== 0) return last;
      return a.firstName.localeCompare(b.firstName, "de-AT");
    });

    res.status(200).json({
      month,
      year,
      timezone,
      range: bounds,
      gls,
    });
  } catch (error) {
    next(error);
  }
});

adminZeiterfassungRouter.patch("/segments/:kind/:segmentId", async (req: AuthedRequest, res, next) => {
  try {
    const kindParse = editableSegmentKindSchema.safeParse(req.params.kind);
    if (!kindParse.success) {
      res.status(400).json({ error: "Unbekannter Segment-Typ." });
      return;
    }
    const segmentId = String(req.params.segmentId ?? "").trim();
    if (!isUuid(segmentId)) {
      res.status(400).json({ error: "Ungültige Segment-ID." });
      return;
    }
    const parsed = segmentPatchSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Bearbeitungsdaten." });
      return;
    }
    const kind = kindParse.data;
    const body = parsed.data;
    const hasAnyUpdate =
      body.startTime !== undefined || body.endTime !== undefined || body.comment !== undefined;
    if (!hasAnyUpdate) {
      res.status(400).json({ error: "Keine ?nderung ?bermittelt." });
      return;
    }
    if (body.comment !== undefined && kind !== "zusatzzeit") {
      res.status(400).json({ error: "Kommentar kann nur bei Zusatzzeiterfassung bearbeitet werden." });
      return;
    }

    const [session] = await db
      .select({
        id: gmDaySessions.id,
        gmUserId: gmDaySessions.gmUserId,
        workDate: gmDaySessions.workDate,
        timezone: gmDaySessions.timezone,
        status: gmDaySessions.status,
        dayStartedAt: gmDaySessions.dayStartedAt,
        dayEndedAt: gmDaySessions.dayEndedAt,
      })
      .from(gmDaySessions)
      .where(
        and(
          eq(gmDaySessions.id, body.sessionId),
          eq(gmDaySessions.isDeleted, false),
        ),
      )
      .limit(1);
    if (!session) {
      res.status(404).json({ error: "Arbeitstag nicht gefunden." });
      return;
    }
    if (session.status !== "submitted" && session.status !== "ended") {
      res.status(409).json({ error: "Nur beendete/gespeicherte Arbeitstage können bearbeitet werden." });
      return;
    }

    const timezone = session.timezone || "Europe/Vienna";
    const dayStartAt = session.dayStartedAt;
    const dayEndAt = session.dayEndedAt;
    if (!dayStartAt || !dayEndAt) {
      res.status(409).json({ error: "Arbeitstag ist unvollständig und kann nicht bearbeitet werden." });
      return;
    }

    const now = new Date();
    if (dayEndAt.getTime() > now.getTime()) {
      res.status(409).json({ error: "Zukünftige Zeiten können nicht bearbeitet werden." });
      return;
    }

    let targetStartAt: Date | null = null;
    let targetEndAt: Date | null = null;

    if (kind === "marktbesuch") {
      const [visit] = await db
        .select({
          id: visitSessions.id,
          gmUserId: visitSessions.gmUserId,
          startAt: visitSessions.startedAt,
          endAt: visitSessions.submittedAt,
        })
        .from(visitSessions)
        .where(
          and(
            eq(visitSessions.id, segmentId),
            eq(visitSessions.gmUserId, session.gmUserId),
            eq(visitSessions.isDeleted, false),
            eq(visitSessions.status, "submitted"),
            isNotNull(visitSessions.submittedAt),
          ),
        )
        .limit(1);
      if (!visit) {
        res.status(404).json({ error: "Marktbesuch nicht gefunden." });
        return;
      }

      targetStartAt = body.startTime
        ? parseWorkDateHmToUtc(session.workDate, body.startTime, timezone)
        : visit.startAt;
      targetEndAt = body.endTime
        ? parseWorkDateHmToUtc(session.workDate, body.endTime, timezone)
        : visit.endAt;
      if (!targetStartAt || !targetEndAt) {
        res.status(400).json({ error: "Ungültige Uhrzeit." });
        return;
      }
      if (targetEndAt.getTime() <= targetStartAt.getTime()) {
        res.status(400).json({ error: "Endzeit muss nach Startzeit liegen." });
        return;
      }
      if (targetStartAt.getTime() < dayStartAt.getTime() || targetEndAt.getTime() > dayEndAt.getTime()) {
        res.status(400).json({ error: "Zeit liegt ausserhalb des Arbeitstags." });
        return;
      }
      if (targetEndAt.getTime() > now.getTime()) {
        res.status(400).json({ error: "Zukünftige Zeiten sind nicht erlaubt." });
        return;
      }

      const intervals = await loadSessionIntervals({
        daySessionId: session.id,
        gmUserId: session.gmUserId,
        workDate: session.workDate,
        timezone,
      });
      const validation = validateTimelineInterval({
        kind: "marktbesuch",
        id: segmentId,
        startAt: targetStartAt,
        endAt: targetEndAt,
        dayStartAt,
        dayEndAt,
        intervals,
      });
      if (!validation.ok) {
        res.status(validation.code === "overlap" ? 409 : 400).json({
          error: validation.message,
          code: validation.code,
        });
        return;
      }

      await db
        .update(visitSessions)
        .set({
          startedAt: targetStartAt,
          submittedAt: targetEndAt,
          updatedAt: now,
        })
        .where(eq(visitSessions.id, segmentId));
    } else if (kind === "pause") {
      const [pause] = await db
        .select({
          id: gmDaySessionPauses.id,
          daySessionId: gmDaySessionPauses.daySessionId,
          startAt: gmDaySessionPauses.pauseStartedAt,
          endAt: gmDaySessionPauses.pauseEndedAt,
        })
        .from(gmDaySessionPauses)
        .where(
          and(
            eq(gmDaySessionPauses.id, segmentId),
            eq(gmDaySessionPauses.daySessionId, session.id),
            eq(gmDaySessionPauses.isDeleted, false),
            isNotNull(gmDaySessionPauses.pauseEndedAt),
          ),
        )
        .limit(1);
      if (!pause || !pause.endAt) {
        res.status(404).json({ error: "Pause nicht gefunden." });
        return;
      }

      targetStartAt = body.startTime
        ? parseWorkDateHmToUtc(session.workDate, body.startTime, timezone)
        : pause.startAt;
      targetEndAt = body.endTime
        ? parseWorkDateHmToUtc(session.workDate, body.endTime, timezone)
        : pause.endAt;
      if (!targetStartAt || !targetEndAt) {
        res.status(400).json({ error: "Ungültige Uhrzeit." });
        return;
      }
      if (targetEndAt.getTime() <= targetStartAt.getTime()) {
        res.status(400).json({ error: "Endzeit muss nach Startzeit liegen." });
        return;
      }
      if (targetStartAt.getTime() < dayStartAt.getTime() || targetEndAt.getTime() > dayEndAt.getTime()) {
        res.status(400).json({ error: "Zeit liegt ausserhalb des Arbeitstags." });
        return;
      }
      if (targetEndAt.getTime() > now.getTime()) {
        res.status(400).json({ error: "Zukünftige Zeiten sind nicht erlaubt." });
        return;
      }

      const intervals = await loadSessionIntervals({
        daySessionId: session.id,
        gmUserId: session.gmUserId,
        workDate: session.workDate,
        timezone,
      });
      const validation = validateTimelineInterval({
        kind: "pause",
        id: segmentId,
        startAt: targetStartAt,
        endAt: targetEndAt,
        dayStartAt,
        dayEndAt,
        intervals,
      });
      if (!validation.ok) {
        res.status(validation.code === "overlap" ? 409 : 400).json({
          error: validation.message,
          code: validation.code,
        });
        return;
      }

      await db
        .update(gmDaySessionPauses)
        .set({
          pauseStartedAt: targetStartAt,
          pauseEndedAt: targetEndAt,
          updatedAt: now,
        })
        .where(eq(gmDaySessionPauses.id, segmentId));
    } else {
      const [extra] = await db
        .select({
          id: timeTrackingEntries.id,
          gmUserId: timeTrackingEntries.gmUserId,
          startAt: timeTrackingEntries.startAt,
          endAt: timeTrackingEntries.endAt,
        })
        .from(timeTrackingEntries)
        .where(
          and(
            eq(timeTrackingEntries.id, segmentId),
            eq(timeTrackingEntries.gmUserId, session.gmUserId),
            eq(timeTrackingEntries.status, "submitted"),
            eq(timeTrackingEntries.isDeleted, false),
            isNotNull(timeTrackingEntries.startAt),
            isNotNull(timeTrackingEntries.endAt),
          ),
        )
        .limit(1);
      if (!extra || !extra.startAt || !extra.endAt) {
        res.status(404).json({ error: "Zusatzzeiterfassung nicht gefunden." });
        return;
      }

      targetStartAt = body.startTime
        ? parseWorkDateHmToUtc(session.workDate, body.startTime, timezone)
        : extra.startAt;
      targetEndAt = body.endTime
        ? parseWorkDateHmToUtc(session.workDate, body.endTime, timezone)
        : extra.endAt;
      if (!targetStartAt || !targetEndAt) {
        res.status(400).json({ error: "Ungültige Uhrzeit." });
        return;
      }
      if (targetEndAt.getTime() <= targetStartAt.getTime()) {
        res.status(400).json({ error: "Endzeit muss nach Startzeit liegen." });
        return;
      }
      if (targetStartAt.getTime() < dayStartAt.getTime() || targetEndAt.getTime() > dayEndAt.getTime()) {
        res.status(400).json({ error: "Zeit liegt ausserhalb des Arbeitstags." });
        return;
      }
      if (targetEndAt.getTime() > now.getTime()) {
        res.status(400).json({ error: "Zukünftige Zeiten sind nicht erlaubt." });
        return;
      }

      const intervals = await loadSessionIntervals({
        daySessionId: session.id,
        gmUserId: session.gmUserId,
        workDate: session.workDate,
        timezone,
      });
      const validation = validateTimelineInterval({
        kind: "zusatzzeit",
        id: segmentId,
        startAt: targetStartAt,
        endAt: targetEndAt,
        dayStartAt,
        dayEndAt,
        intervals,
      });
      if (!validation.ok) {
        res.status(validation.code === "overlap" ? 409 : 400).json({
          error: validation.message,
          code: validation.code,
        });
        return;
      }

      await db
        .update(timeTrackingEntries)
        .set({
          startAt: targetStartAt,
          endAt: targetEndAt,
          ...(body.comment !== undefined ? { comment: body.comment?.trim() || null } : {}),
          updatedAt: now,
        })
        .where(eq(timeTrackingEntries.id, segmentId));
    }

    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

adminZeiterfassungRouter.patch("/day-sessions/:sessionId", async (req: AuthedRequest, res, next) => {
  try {
    const sessionId = String(req.params.sessionId ?? "").trim();
    if (!isUuid(sessionId)) {
      res.status(400).json({ error: "Ungueltige Arbeitstag-ID." });
      return;
    }
    const parsed = daySessionPatchSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Bearbeitungsdaten." });
      return;
    }
    const body = parsed.data;
    if (body.startTime === undefined && body.endTime === undefined) {
      res.status(400).json({ error: "Keine Aenderung uebermittelt." });
      return;
    }

    const [session] = await db
      .select({
        id: gmDaySessions.id,
        gmUserId: gmDaySessions.gmUserId,
        workDate: gmDaySessions.workDate,
        timezone: gmDaySessions.timezone,
        status: gmDaySessions.status,
        dayStartedAt: gmDaySessions.dayStartedAt,
        dayEndedAt: gmDaySessions.dayEndedAt,
      })
      .from(gmDaySessions)
      .where(and(eq(gmDaySessions.id, sessionId), eq(gmDaySessions.isDeleted, false)))
      .limit(1);

    if (!session) {
      res.status(404).json({ error: "Arbeitstag nicht gefunden." });
      return;
    }
    if (session.status !== "submitted" && session.status !== "ended") {
      res.status(409).json({ error: "Nur beendete/gespeicherte Arbeitstage koennen bearbeitet werden." });
      return;
    }
    if (!session.dayStartedAt || !session.dayEndedAt) {
      res.status(409).json({ error: "Arbeitstag ist unvollstaendig und kann nicht bearbeitet werden." });
      return;
    }

    const timezone = session.timezone || "Europe/Vienna";
    const nextDayStartAt = body.startTime
      ? parseWorkDateHmToUtc(session.workDate, body.startTime, timezone)
      : session.dayStartedAt;
    const nextDayEndAt = body.endTime
      ? parseWorkDateHmToUtc(session.workDate, body.endTime, timezone)
      : session.dayEndedAt;
    if (!nextDayStartAt || !nextDayEndAt) {
      res.status(400).json({ error: "Bitte gueltige Uhrzeiten im Format HH:MM eingeben." });
      return;
    }
    if (nextDayEndAt.getTime() <= nextDayStartAt.getTime()) {
      res.status(400).json({ error: "Tagesende muss nach Tagesstart liegen." });
      return;
    }

    const intervals = await loadSessionIntervals({
      daySessionId: session.id,
      gmUserId: session.gmUserId,
      workDate: session.workDate,
      timezone,
    });
    const outsideInterval = intervals.find(
      (interval) =>
        interval.startAt.getTime() < nextDayStartAt.getTime() ||
        interval.endAt.getTime() > nextDayEndAt.getTime(),
    );
    if (outsideInterval) {
      res.status(400).json({
        error: "Tagesstart und Tagesende muessen alle Eintraege vollstaendig umfassen.",
        code: "outside_day",
      });
      return;
    }

    await db
      .update(gmDaySessions)
      .set({
        ...(body.startTime !== undefined ? { dayStartedAt: nextDayStartAt } : {}),
        ...(body.endTime !== undefined ? { dayEndedAt: nextDayEndAt } : {}),
        updatedAt: new Date(),
      })
      .where(eq(gmDaySessions.id, session.id));

    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

export { adminZeiterfassungRouter };
