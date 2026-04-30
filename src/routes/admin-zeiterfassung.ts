import { and, asc, desc, eq, gte, ilike, inArray, isNotNull, lte, or, sql } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import {
  buildDaySessionPayload,
  buildGmAggregates,
  resolvePrimaryQuestionnaireType,
  toYmdInTimezone,
  type DaySessionPayload,
} from "../lib/admin-zeiterfassung.js";
import { db } from "../lib/db.js";
import {
  gmDaySessionPauses,
  gmDaySessions,
  markets,
  timeTrackingEntries,
  users,
  visitSessionSections,
  visitSessions,
} from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const adminZeiterfassungRouter = Router();
adminZeiterfassungRouter.use(requireAuth(["admin"]));

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
      res.status(400).json({ error: "Ungueltige Abfrage fuer Admin-Zeiterfassung." });
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
      res.status(400).json({ error: "Ungueltige Abfrage fuer Admin-Zeiterfassung." });
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

export { adminZeiterfassungRouter };
