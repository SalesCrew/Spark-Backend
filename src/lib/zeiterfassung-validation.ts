import { and, desc, eq, inArray, isNotNull, sql } from "drizzle-orm";
import type { Response } from "express";
import {
  TIMELINE_OVERLAP_ERROR_MESSAGE,
  isAllowedTimelineOverlap,
  type TimelineInterval,
  type TimelineIntervalKind,
  validateTimelineInterval,
} from "./admin-zeiterfassung.js";
import { db } from "./db.js";
import {
  gmDaySessionPauses,
  gmDaySessions,
  timeTrackingEntries,
  visitSessions,
} from "./schema.js";

type DbTransaction = Parameters<Parameters<typeof db.transaction>[0]>[0];
type DbLike = typeof db | DbTransaction;

const TIMELINE_DAY_NOT_STARTED_MESSAGE = "Bitte zuerst den Arbeitstag starten.";
const TIMELINE_OUTSIDE_DAY_MESSAGE = "Zeit liegt ausserhalb des Arbeitstags.";

type DaySessionForValidation = {
  id: string;
  gmUserId: string;
  workDate: string;
  timezone: string;
  status: string;
  dayStartedAt: Date | null;
  dayEndedAt: Date | null;
};

type TimelineValidationErrorCode =
  | "day_not_started"
  | "invalid_interval"
  | "outside_day"
  | "overlap";

class TimelineValidationError extends Error {
  readonly status: number;
  readonly code: TimelineValidationErrorCode;

  constructor(status: number, code: TimelineValidationErrorCode, message: string) {
    super(message);
    this.name = "TimelineValidationError";
    this.status = status;
    this.code = code;
  }
}

function respondTimelineValidationError(res: Response, error: unknown): boolean {
  if (!(error instanceof TimelineValidationError)) return false;
  res.status(error.status).json({ error: error.message, code: error.code });
  return true;
}

function normalizeEffectiveDayEnd(session: DaySessionForValidation, now: Date): Date {
  if (session.dayEndedAt) return session.dayEndedAt;
  return now;
}

function toPgTimestampParam(date: Date): string {
  return date.toISOString();
}

async function loadNearestStartedDaySession(input: {
  gmUserId: string;
  startAt: Date;
  database: DbLike;
}): Promise<DaySessionForValidation | null> {
  const [session] = await input.database
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
        eq(gmDaySessions.gmUserId, input.gmUserId),
        eq(gmDaySessions.isDeleted, false),
        sql`${gmDaySessions.status}::text in ('started', 'ended', 'submitted')`,
        isNotNull(gmDaySessions.dayStartedAt),
        sql`${gmDaySessions.dayStartedAt} <= ${toPgTimestampParam(input.startAt)}::timestamptz`,
      ),
    )
    .orderBy(desc(gmDaySessions.dayStartedAt))
    .limit(1);
  return session ?? null;
}

function pushInterval(
  intervals: TimelineInterval[],
  interval: TimelineInterval,
) {
  if (interval.endAt.getTime() <= interval.startAt.getTime()) return;
  intervals.push(interval);
}

async function loadValidationIntervalsForSession(input: {
  gmUserId: string;
  daySessionId: string;
  dayStartAt: Date;
  dayEndAt: Date;
  now: Date;
  database: DbLike;
}): Promise<TimelineInterval[]> {
  const dayStartAt = toPgTimestampParam(input.dayStartAt);
  const dayEndAt = toPgTimestampParam(input.dayEndAt);
  const now = toPgTimestampParam(input.now);
  const [pauseRows, visitRows, extraRows] = await Promise.all([
    input.database
      .select({
        id: gmDaySessionPauses.id,
        startAt: gmDaySessionPauses.pauseStartedAt,
        endAt: gmDaySessionPauses.pauseEndedAt,
      })
      .from(gmDaySessionPauses)
      .where(
        and(
          eq(gmDaySessionPauses.daySessionId, input.daySessionId),
          eq(gmDaySessionPauses.gmUserId, input.gmUserId),
          eq(gmDaySessionPauses.isDeleted, false),
          sql`${gmDaySessionPauses.pauseStartedAt} < ${dayEndAt}::timestamptz`,
          sql`coalesce(${gmDaySessionPauses.pauseEndedAt}, ${now}::timestamptz) > ${dayStartAt}::timestamptz`,
        ),
      ),
    input.database
      .select({
        id: visitSessions.id,
        status: visitSessions.status,
        startAt: visitSessions.startedAt,
        endAt: visitSessions.submittedAt,
      })
      .from(visitSessions)
      .where(
        and(
          eq(visitSessions.gmUserId, input.gmUserId),
          inArray(visitSessions.status, ["draft", "submitted"]),
          eq(visitSessions.isDeleted, false),
          sql`${visitSessions.startedAt} < ${dayEndAt}::timestamptz`,
          sql`coalesce(${visitSessions.submittedAt}, ${now}::timestamptz) > ${dayStartAt}::timestamptz`,
        ),
      ),
    input.database
      .select({
        id: timeTrackingEntries.id,
        status: timeTrackingEntries.status,
        startAt: timeTrackingEntries.startAt,
        endAt: timeTrackingEntries.endAt,
      })
      .from(timeTrackingEntries)
      .where(
        and(
          eq(timeTrackingEntries.gmUserId, input.gmUserId),
          sql`(${timeTrackingEntries.status}::text = 'submitted' or (${timeTrackingEntries.status}::text = 'draft' and ${timeTrackingEntries.endAt} is null))`,
          eq(timeTrackingEntries.isDeleted, false),
          isNotNull(timeTrackingEntries.startAt),
          sql`${timeTrackingEntries.startAt} < ${dayEndAt}::timestamptz`,
          sql`coalesce(${timeTrackingEntries.endAt}, ${now}::timestamptz) > ${dayStartAt}::timestamptz`,
        ),
      ),
  ]);

  const intervals: TimelineInterval[] = [];
  for (const row of pauseRows) {
    const endAt = row.endAt ?? input.now;
    pushInterval(intervals, { id: row.id, kind: "pause", startAt: row.startAt, endAt });
  }
  for (const row of visitRows) {
    const endAt = row.endAt ?? input.now;
    pushInterval(intervals, { id: row.id, kind: "marktbesuch", startAt: row.startAt, endAt });
  }
  for (const row of extraRows) {
    if (!row.startAt) continue;
    const endAt = row.endAt ?? input.now;
    pushInterval(intervals, { id: row.id, kind: "zusatzzeit", startAt: row.startAt, endAt });
  }
  return intervals;
}

async function getValidatedDayContext(input: {
  gmUserId: string;
  startAt: Date;
  endAt?: Date;
  now?: Date;
  database?: DbLike;
}): Promise<{
  session: DaySessionForValidation;
  dayStartAt: Date;
  dayEndAt: Date;
  now: Date;
}> {
  const now = input.now ?? new Date();
  const database = input.database ?? db;
  const session = await loadNearestStartedDaySession({
    gmUserId: input.gmUserId,
    startAt: input.startAt,
    database,
  });
  if (!session?.dayStartedAt) {
    throw new TimelineValidationError(409, "day_not_started", TIMELINE_DAY_NOT_STARTED_MESSAGE);
  }
  const dayEndAt = normalizeEffectiveDayEnd(session, now);
  const endAt = input.endAt ?? input.startAt;
  if (
    input.startAt.getTime() < session.dayStartedAt.getTime() ||
    endAt.getTime() > dayEndAt.getTime()
  ) {
    throw new TimelineValidationError(409, "outside_day", TIMELINE_OUTSIDE_DAY_MESSAGE);
  }
  return { session, dayStartAt: session.dayStartedAt, dayEndAt, now };
}

async function validateGmTimelineInterval(input: {
  gmUserId: string;
  kind: TimelineIntervalKind;
  id: string;
  startAt: Date;
  endAt: Date;
  now?: Date;
  database?: DbLike;
}): Promise<{ session: DaySessionForValidation }> {
  const context = await getValidatedDayContext({
    gmUserId: input.gmUserId,
    startAt: input.startAt,
    endAt: input.endAt,
    ...(input.now ? { now: input.now } : {}),
    ...(input.database ? { database: input.database } : {}),
  });
  const intervals = await loadValidationIntervalsForSession({
    gmUserId: input.gmUserId,
    daySessionId: context.session.id,
    dayStartAt: context.dayStartAt,
    dayEndAt: context.dayEndAt,
    now: context.now,
    database: input.database ?? db,
  });
  const result = validateTimelineInterval({
    kind: input.kind,
    id: input.id,
    startAt: input.startAt,
    endAt: input.endAt,
    dayStartAt: context.dayStartAt,
    dayEndAt: context.dayEndAt,
    intervals,
  });
  if (!result.ok) {
    const status = result.code === "invalid_interval" || result.code === "outside_day" ? 400 : 409;
    throw new TimelineValidationError(status, result.code, result.message);
  }
  return { session: context.session };
}

async function validateGmTimelineIntervalStrict(input: {
  gmUserId: string;
  kind: TimelineIntervalKind;
  id: string;
  startAt: Date;
  endAt: Date;
  now?: Date;
  database?: DbLike;
}): Promise<{ session: DaySessionForValidation }> {
  const context = await getValidatedDayContext({
    gmUserId: input.gmUserId,
    startAt: input.startAt,
    endAt: input.endAt,
    ...(input.now ? { now: input.now } : {}),
    ...(input.database ? { database: input.database } : {}),
  });
  if (input.endAt.getTime() <= input.startAt.getTime()) {
    throw new TimelineValidationError(400, "invalid_interval", "Die Endzeit muss nach der Startzeit liegen.");
  }
  const intervals = await loadValidationIntervalsForSession({
    gmUserId: input.gmUserId,
    daySessionId: context.session.id,
    dayStartAt: context.dayStartAt,
    dayEndAt: context.dayEndAt,
    now: context.now,
    database: input.database ?? db,
  });
  const overlaps = intervals.some((interval) => {
    if (interval.kind === input.kind && interval.id === input.id) return false;
    return input.startAt.getTime() < interval.endAt.getTime()
      && input.endAt.getTime() > interval.startAt.getTime();
  });
  if (overlaps) {
    throw new TimelineValidationError(409, "overlap", TIMELINE_OVERLAP_ERROR_MESSAGE);
  }
  return { session: context.session };
}

async function validateGmTimelineStartPoint(input: {
  gmUserId: string;
  kind: TimelineIntervalKind;
  id: string;
  startAt: Date;
  now?: Date;
  database?: DbLike;
}): Promise<{ session: DaySessionForValidation }> {
  const baseNow = input.now ?? new Date();
  const validationNow = input.startAt.getTime() > baseNow.getTime() ? input.startAt : baseNow;
  const context = await getValidatedDayContext({
    gmUserId: input.gmUserId,
    startAt: input.startAt,
    now: validationNow,
    ...(input.database ? { database: input.database } : {}),
  });
  const intervals = await loadValidationIntervalsForSession({
    gmUserId: input.gmUserId,
    daySessionId: context.session.id,
    dayStartAt: context.dayStartAt,
    dayEndAt: context.dayEndAt,
    now: context.now,
    database: input.database ?? db,
  });
  const pointMs = input.startAt.getTime();
  for (const interval of intervals) {
    if (interval.kind === input.kind && interval.id === input.id) continue;
    if (isAllowedTimelineOverlap(input.kind, interval.kind)) continue;
    if (interval.startAt.getTime() <= pointMs && interval.endAt.getTime() > pointMs) {
      throw new TimelineValidationError(409, "overlap", TIMELINE_OVERLAP_ERROR_MESSAGE);
    }
  }
  return { session: context.session };
}

export {
  TIMELINE_DAY_NOT_STARTED_MESSAGE,
  TIMELINE_OUTSIDE_DAY_MESSAGE,
  TimelineValidationError,
  respondTimelineValidationError,
  validateGmTimelineInterval,
  validateGmTimelineIntervalStrict,
  validateGmTimelineStartPoint,
};
