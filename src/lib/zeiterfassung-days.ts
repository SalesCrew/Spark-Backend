import { and, asc, desc, eq, gte, ilike, inArray, isNotNull, lte, or, sql } from "drizzle-orm";
import {
  buildDaySessionPayload,
  resolvePrimaryQuestionnaireType,
  toYmdInTimezone,
  type BaseAction,
  type DaySessionPayload,
} from "./admin-zeiterfassung.js";
import { db } from "./db.js";
import {
  gmDaySessionPauses,
  gmDaySessions,
  markets,
  timeTrackingEntries,
  users,
  visitSessionSections,
  visitSessions,
} from "./schema.js";

type ZeiterfassungDaySessionsInput = {
  from: string;
  to: string;
  gmUserIds: string[];
  region?: string;
  search?: string;
  includeLive: boolean;
  timezone: string;
};

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

async function loadZeiterfassungDaySessions(input: ZeiterfassungDaySessionsInput): Promise<DaySessionPayload[]> {
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
  if (input.gmUserIds.length > 0) {
    whereClauses.push(inArray(users.id, input.gmUserIds));
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
  const actionsBySessionId = new Map<string, BaseAction[]>();
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
        eq(visitSessions.status, "submitted"),
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
    const endAt = row.submittedAt;
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

  const extraRows = await db
    .select({
      id: timeTrackingEntries.id,
      gmUserId: timeTrackingEntries.gmUserId,
      status: timeTrackingEntries.status,
      activityType: timeTrackingEntries.activityType,
      comment: timeTrackingEntries.comment,
      startAt: timeTrackingEntries.startAt,
      endAt: timeTrackingEntries.endAt,
      doctorConfirmationPath: timeTrackingEntries.doctorConfirmationPath,
      doctorConfirmationFileName: timeTrackingEntries.doctorConfirmationFileName,
      doctorConfirmationUploadedAt: timeTrackingEntries.doctorConfirmationUploadedAt,
      marketName: markets.name,
      marketAddress: markets.address,
    })
    .from(timeTrackingEntries)
    .leftJoin(markets, eq(markets.id, timeTrackingEntries.marketId))
    .where(
      and(
        inArray(timeTrackingEntries.gmUserId, gmIdsFromRows),
        eq(timeTrackingEntries.status, "submitted"),
        eq(timeTrackingEntries.isDeleted, false),
        isNotNull(timeTrackingEntries.startAt),
        sql`${timeTrackingEntries.startAt} >= ${rangeStartExpr}`,
        sql`${timeTrackingEntries.startAt} < ${rangeEndExpr}`,
      ),
    )
    .orderBy(asc(timeTrackingEntries.startAt));

  for (const row of extraRows) {
    const startAt = row.startAt;
    const endAt = row.endAt;
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
      ...(row.activityType === "arztbesuch"
        ? {
            doctorConfirmation: {
              isRequired: true,
              isUploaded: Boolean(row.doctorConfirmationPath),
              uploadedAt: row.doctorConfirmationUploadedAt?.toISOString() ?? null,
              fileName: row.doctorConfirmationFileName ?? null,
            },
          }
        : {}),
      ...(row.marketName ? { marketName: row.marketName } : {}),
      ...(row.marketAddress ? { marketAddress: row.marketAddress } : {}),
    });
  }

  const payload = dayRows.map((row) => {
    const gmName = `${row.gmFirstName} ${row.gmLastName}`.trim();
    const startAt = row.dayStartedAt ?? row.createdAt;
    const defaultEnd = row.dayEndedAt ?? (row.status === "submitted" ? startAt : now);
    const endAt = defaultEnd.getTime() > startAt.getTime() ? defaultEnd : new Date(startAt.getTime() + 60_000);
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

export { loadZeiterfassungDaySessions, type ZeiterfassungDaySessionsInput };
