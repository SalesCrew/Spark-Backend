import { and, desc, eq, gte, lte, sql } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { formatExtraLabel } from "../lib/admin-zeiterfassung.js";
import { db } from "../lib/db.js";
import { validateGmTimelineInterval } from "../lib/zeiterfassung-validation.js";
import {
  gmDaySessionPauses,
  gmDaySessions,
  markets,
  timeEntryChangeRequests,
  timeTrackingEntries,
  users,
  visitSessions,
} from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const hhmmRegex = /^([01]\d|2[0-3]):([0-5]\d)$/;
const ymdRegex = /^\d{4}-\d{2}-\d{2}$/;
const sourceKindSchema = z.enum(["day_start", "day_end", "day_km", "marktbesuch", "pause", "zusatzzeit"]);

const gmCreateTimeChangeRequestSchema = z
  .object({
    sessionId: z.string().uuid(),
    kind: sourceKindSchema,
    segmentId: z.string().uuid(),
    requestedStartTime: z.string().regex(hhmmRegex).optional(),
    requestedEndTime: z.string().regex(hhmmRegex).optional(),
    requestedStartKm: z.number().int().min(0).max(10_000_000).optional(),
    requestedEndKm: z.number().int().min(0).max(10_000_000).optional(),
    requestNote: z.string().trim().max(2_000).optional(),
  })
  .strict()
  .superRefine((value, context) => {
    if (value.kind === "day_km") {
      if (value.requestedStartKm === undefined && value.requestedEndKm === undefined) {
        context.addIssue({ code: z.ZodIssueCode.custom, message: "Mindestens ein KM-Wert ist erforderlich." });
      }
      return;
    }
    if (!value.requestedStartTime || !value.requestedEndTime) {
      context.addIssue({ code: z.ZodIssueCode.custom, message: "Start- und Endzeit sind erforderlich." });
    }
  });

const gmListTimeChangeRequestsSchema = z
  .object({
    from: z.string().regex(ymdRegex).optional(),
    to: z.string().regex(ymdRegex).optional(),
  })
  .strict();

const adminDecisionSchema = z
  .object({
    adminNote: z.string().trim().max(2_000).optional(),
  })
  .strict();

type SourceKind = z.infer<typeof sourceKindSchema>;

type DaySessionContext = {
  id: string;
  gmUserId: string;
  workDate: string;
  timezone: string;
  status: string;
  dayStartedAt: Date | null;
  dayEndedAt: Date | null;
  startKm: number | null;
  endKm: number | null;
};

type SourceSnapshot = {
  id: string;
  kind: SourceKind;
  startAt: Date;
  endAt: Date;
  title: string;
  subtitle: string | null;
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
  return Date.UTC(year, month - 1, day, hour, minute, second, 0) - date.getTime();
}

function toYmdInTimezone(date: Date, timeZone: string): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
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

function toIso(value: Date | null | undefined): string | null {
  return value ? value.toISOString() : null;
}

function buildHttpError(message: string, status: number, code: string) {
  const error = new Error(message) as Error & { status?: number; code?: string };
  error.status = status;
  error.code = code;
  return error;
}

function sameMinute(left: Date, right: Date): boolean {
  return Math.floor(left.getTime() / 60_000) === Math.floor(right.getTime() / 60_000);
}

function mapRequestRow(row: {
  id: string;
  daySessionId: string;
  gmUserId: string;
  sourceKind: SourceKind;
  sourceId: string;
  workDate: string;
  timezone: string;
  titleSnapshot: string;
  subtitleSnapshot: string | null;
  originalStartAt: Date;
  originalEndAt: Date;
  requestedStartAt: Date;
  requestedEndAt: Date;
  originalStartKm: number | null;
  originalEndKm: number | null;
  requestedStartKm: number | null;
  requestedEndKm: number | null;
  requestNote: string | null;
  status: "pending" | "approved" | "rejected" | "cancelled";
  reviewedByUserId: string | null;
  reviewedAt: Date | null;
  appliedAt: Date | null;
  adminNote: string | null;
  createdAt: Date;
  updatedAt: Date;
  gmFirstName?: string | null;
  gmLastName?: string | null;
  gmEmail?: string | null;
  gmRegion?: string | null;
}) {
  return {
    id: row.id,
    daySessionId: row.daySessionId,
    gmUserId: row.gmUserId,
    sourceKind: row.sourceKind,
    sourceId: row.sourceId,
    workDate: row.workDate,
    timezone: row.timezone,
    title: row.titleSnapshot,
    subtitle: row.subtitleSnapshot,
    originalStartAt: row.originalStartAt.toISOString(),
    originalEndAt: row.originalEndAt.toISOString(),
    requestedStartAt: row.requestedStartAt.toISOString(),
    requestedEndAt: row.requestedEndAt.toISOString(),
    originalStartKm: row.originalStartKm,
    originalEndKm: row.originalEndKm,
    requestedStartKm: row.requestedStartKm,
    requestedEndKm: row.requestedEndKm,
    requestNote: row.requestNote,
    status: row.status,
    reviewedByUserId: row.reviewedByUserId,
    reviewedAt: toIso(row.reviewedAt),
    appliedAt: toIso(row.appliedAt),
    adminNote: row.adminNote,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
    gm: row.gmEmail
      ? {
          id: row.gmUserId,
          name: [row.gmFirstName, row.gmLastName].filter(Boolean).join(" ").trim() || row.gmEmail,
          email: row.gmEmail,
          region: row.gmRegion ?? null,
        }
      : null,
  };
}

async function loadDaySession(sessionId: string, gmUserId?: string): Promise<DaySessionContext | null> {
  const clauses = [
    eq(gmDaySessions.id, sessionId),
    eq(gmDaySessions.isDeleted, false),
  ];
  if (gmUserId) clauses.push(eq(gmDaySessions.gmUserId, gmUserId));
  const [session] = await db
    .select({
      id: gmDaySessions.id,
      gmUserId: gmDaySessions.gmUserId,
      workDate: gmDaySessions.workDate,
      timezone: gmDaySessions.timezone,
      status: gmDaySessions.status,
      dayStartedAt: gmDaySessions.dayStartedAt,
      dayEndedAt: gmDaySessions.dayEndedAt,
      startKm: gmDaySessions.startKm,
      endKm: gmDaySessions.endKm,
    })
    .from(gmDaySessions)
    .where(and(...clauses))
    .limit(1);
  return session ?? null;
}

async function loadFirstRealActionStart(session: DaySessionContext, now = new Date()): Promise<Date | null> {
  if (!session.dayStartedAt) return null;
  const lower = session.dayStartedAt;
  const upper = session.dayEndedAt ?? now;

  const [visitRows, extraRows, pauseRows] = await Promise.all([
    db
      .select({ startAt: visitSessions.startedAt })
      .from(visitSessions)
      .where(
        and(
          eq(visitSessions.gmUserId, session.gmUserId),
          eq(visitSessions.status, "submitted"),
          eq(visitSessions.isDeleted, false),
          sql`${visitSessions.startedAt} is not null`,
          sql`${visitSessions.submittedAt} is not null`,
          gte(visitSessions.startedAt, lower),
          lte(visitSessions.startedAt, upper),
        ),
      )
      .orderBy(sql`${visitSessions.startedAt} asc`)
      .limit(1),
    db
      .select({ startAt: timeTrackingEntries.startAt })
      .from(timeTrackingEntries)
      .where(
        and(
          eq(timeTrackingEntries.gmUserId, session.gmUserId),
          eq(timeTrackingEntries.status, "submitted"),
          eq(timeTrackingEntries.isDeleted, false),
          sql`${timeTrackingEntries.startAt} is not null`,
          sql`${timeTrackingEntries.endAt} is not null`,
          gte(timeTrackingEntries.startAt, lower),
          lte(timeTrackingEntries.startAt, upper),
        ),
      )
      .orderBy(sql`${timeTrackingEntries.startAt} asc`)
      .limit(1),
    db
      .select({ startAt: gmDaySessionPauses.pauseStartedAt })
      .from(gmDaySessionPauses)
      .where(
        and(
          eq(gmDaySessionPauses.daySessionId, session.id),
          eq(gmDaySessionPauses.gmUserId, session.gmUserId),
          eq(gmDaySessionPauses.isDeleted, false),
          sql`${gmDaySessionPauses.pauseStartedAt} is not null`,
          sql`${gmDaySessionPauses.pauseEndedAt} is not null`,
          gte(gmDaySessionPauses.pauseStartedAt, lower),
          lte(gmDaySessionPauses.pauseStartedAt, upper),
        ),
      )
      .orderBy(sql`${gmDaySessionPauses.pauseStartedAt} asc`)
      .limit(1),
  ]);

  const starts = [visitRows[0]?.startAt, extraRows[0]?.startAt, pauseRows[0]?.startAt].filter((value): value is Date => value instanceof Date);
  const [firstStart, ...remainingStarts] = starts;
  if (!firstStart) return null;
  return remainingStarts.reduce((earliest, value) => (value.getTime() < earliest.getTime() ? value : earliest), firstStart);
}

async function loadLastRealActionEnd(session: DaySessionContext, now = new Date()): Promise<Date | null> {
  if (!session.dayStartedAt) return null;
  const lower = session.dayStartedAt;
  const upper = session.dayEndedAt ?? now;

  const [visitRows, extraRows, pauseRows] = await Promise.all([
    db
      .select({ endAt: visitSessions.submittedAt })
      .from(visitSessions)
      .where(
        and(
          eq(visitSessions.gmUserId, session.gmUserId),
          eq(visitSessions.status, "submitted"),
          eq(visitSessions.isDeleted, false),
          sql`${visitSessions.startedAt} is not null`,
          sql`${visitSessions.submittedAt} is not null`,
          gte(visitSessions.submittedAt, lower),
          lte(visitSessions.submittedAt, upper),
        ),
      )
      .orderBy(sql`${visitSessions.submittedAt} desc`)
      .limit(1),
    db
      .select({ endAt: timeTrackingEntries.endAt })
      .from(timeTrackingEntries)
      .where(
        and(
          eq(timeTrackingEntries.gmUserId, session.gmUserId),
          eq(timeTrackingEntries.status, "submitted"),
          eq(timeTrackingEntries.isDeleted, false),
          sql`${timeTrackingEntries.startAt} is not null`,
          sql`${timeTrackingEntries.endAt} is not null`,
          gte(timeTrackingEntries.endAt, lower),
          lte(timeTrackingEntries.endAt, upper),
        ),
      )
      .orderBy(sql`${timeTrackingEntries.endAt} desc`)
      .limit(1),
    db
      .select({ endAt: gmDaySessionPauses.pauseEndedAt })
      .from(gmDaySessionPauses)
      .where(
        and(
          eq(gmDaySessionPauses.daySessionId, session.id),
          eq(gmDaySessionPauses.gmUserId, session.gmUserId),
          eq(gmDaySessionPauses.isDeleted, false),
          sql`${gmDaySessionPauses.pauseStartedAt} is not null`,
          sql`${gmDaySessionPauses.pauseEndedAt} is not null`,
          gte(gmDaySessionPauses.pauseEndedAt, lower),
          lte(gmDaySessionPauses.pauseEndedAt, upper),
        ),
      )
      .orderBy(sql`${gmDaySessionPauses.pauseEndedAt} desc`)
      .limit(1),
  ]);

  const ends = [visitRows[0]?.endAt, extraRows[0]?.endAt, pauseRows[0]?.endAt].filter((value): value is Date => value instanceof Date);
  const [lastEnd, ...remainingEnds] = ends;
  if (!lastEnd) return null;
  return remainingEnds.reduce((latest, value) => (value.getTime() > latest.getTime() ? value : latest), lastEnd);
}

async function validateDayStartChange(input: {
  session: DaySessionContext;
  sourceId: string;
  requestedStartAt: Date;
  requestedEndAt: Date;
  now?: Date;
}) {
  if (!input.session.dayStartedAt) {
    throw buildHttpError("Arbeitstag ist unvollständig und kann nicht korrigiert werden.", 409, "day_incomplete");
  }
  if (input.sourceId !== input.session.id && input.sourceId !== `anfahrt-${input.session.id}`) {
    throw buildHttpError("Anfahrt konnte nicht eindeutig zugeordnet werden.", 404, "time_source_not_found");
  }
  const timezone = input.session.timezone || "Europe/Vienna";
  if (toYmdInTimezone(input.requestedStartAt, timezone) !== input.session.workDate || toYmdInTimezone(input.requestedEndAt, timezone) !== input.session.workDate) {
    throw buildHttpError("Startzeit muss am Arbeitstag liegen.", 409, "outside_day");
  }

  const boundary = await loadFirstRealActionStart(input.session, input.now ?? new Date()) ?? input.session.dayEndedAt ?? input.now ?? new Date();
  if (!sameMinute(input.requestedEndAt, boundary)) {
    throw buildHttpError("Das Ende der Anfahrt wird durch den nächsten Eintrag bestimmt. Bitte nur die Startzeit ändern.", 409, "day_start_end_locked");
  }
  if (input.requestedStartAt.getTime() >= boundary.getTime()) {
    throw buildHttpError("Die Startzeit darf den nächsten Eintrag nicht überschneiden.", 409, "overlap");
  }
  if (input.requestedStartAt.getTime() >= input.requestedEndAt.getTime()) {
    throw buildHttpError("Startzeit muss vor der nächsten Zeitbuchung liegen.", 409, "invalid_interval");
  }
}

async function validateDayEndChange(input: {
  session: DaySessionContext;
  sourceId: string;
  requestedStartAt: Date;
  requestedEndAt: Date;
  now?: Date;
}) {
  if (!input.session.dayStartedAt || !input.session.dayEndedAt) {
    throw buildHttpError("Arbeitstag ist unvollständig und kann nicht korrigiert werden.", 409, "day_incomplete");
  }
  if (input.sourceId !== input.session.id && input.sourceId !== `heimfahrt-${input.session.id}`) {
    throw buildHttpError("Heimfahrt konnte nicht eindeutig zugeordnet werden.", 404, "time_source_not_found");
  }
  const timezone = input.session.timezone || "Europe/Vienna";
  if (toYmdInTimezone(input.requestedStartAt, timezone) !== input.session.workDate || toYmdInTimezone(input.requestedEndAt, timezone) !== input.session.workDate) {
    throw buildHttpError("Endzeit muss am Arbeitstag liegen.", 409, "outside_day");
  }

  const boundary = await loadLastRealActionEnd(input.session, input.now ?? new Date()) ?? input.session.dayStartedAt;
  if (!sameMinute(input.requestedStartAt, boundary)) {
    throw buildHttpError("Der Start der Heimfahrt wird durch den letzten Eintrag bestimmt. Bitte nur die Endzeit ändern.", 409, "day_end_start_locked");
  }
  if (input.requestedEndAt.getTime() <= boundary.getTime()) {
    throw buildHttpError("Die Endzeit muss nach dem letzten Eintrag liegen.", 409, "invalid_interval");
  }
}

async function loadSourceSnapshot(kind: SourceKind, sourceId: string, session: DaySessionContext): Promise<SourceSnapshot | null> {
  if (kind === "day_start") {
    if (!session.dayStartedAt) return null;
    if (sourceId !== session.id && sourceId !== `anfahrt-${session.id}`) return null;
    const endAt = await loadFirstRealActionStart(session) ?? session.dayEndedAt ?? new Date();
    if (session.dayStartedAt.getTime() >= endAt.getTime()) return null;
    return {
      id: session.id,
      kind,
      startAt: session.dayStartedAt,
      endAt,
      title: "Anfahrt",
      subtitle: null,
    };
  }

  if (kind === "day_end") {
    if (!session.dayEndedAt) return null;
    if (sourceId !== session.id && sourceId !== `heimfahrt-${session.id}`) return null;
    const startAt = await loadLastRealActionEnd(session) ?? session.dayStartedAt;
    if (!startAt || startAt.getTime() >= session.dayEndedAt.getTime()) return null;
    return {
      id: session.id,
      kind,
      startAt,
      endAt: session.dayEndedAt,
      title: "Heimfahrt",
      subtitle: null,
    };
  }

  if (kind === "marktbesuch") {
    const [visit] = await db
      .select({
        id: visitSessions.id,
        startAt: visitSessions.startedAt,
        endAt: visitSessions.submittedAt,
        marketName: markets.name,
        marketAddress: markets.address,
      })
      .from(visitSessions)
      .leftJoin(markets, eq(markets.id, visitSessions.marketId))
      .where(
        and(
          eq(visitSessions.id, sourceId),
          eq(visitSessions.gmUserId, session.gmUserId),
          eq(visitSessions.status, "submitted"),
          eq(visitSessions.isDeleted, false),
          sql`${visitSessions.submittedAt} is not null`,
        ),
      )
      .limit(1);
    if (!visit?.endAt) return null;
    return {
      id: visit.id,
      kind,
      startAt: visit.startAt,
      endAt: visit.endAt,
      title: visit.marketName?.trim() || "Marktbesuch",
      subtitle: visit.marketAddress?.trim() || null,
    };
  }

  if (kind === "pause") {
    const [pause] = await db
      .select({
        id: gmDaySessionPauses.id,
        startAt: gmDaySessionPauses.pauseStartedAt,
        endAt: gmDaySessionPauses.pauseEndedAt,
      })
      .from(gmDaySessionPauses)
      .where(
        and(
          eq(gmDaySessionPauses.id, sourceId),
          eq(gmDaySessionPauses.daySessionId, session.id),
          eq(gmDaySessionPauses.gmUserId, session.gmUserId),
          eq(gmDaySessionPauses.isDeleted, false),
          sql`${gmDaySessionPauses.pauseEndedAt} is not null`,
        ),
      )
      .limit(1);
    if (!pause?.endAt) return null;
    return {
      id: pause.id,
      kind,
      startAt: pause.startAt,
      endAt: pause.endAt,
      title: "Pause",
      subtitle: null,
    };
  }

  const [extra] = await db
    .select({
      id: timeTrackingEntries.id,
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
        eq(timeTrackingEntries.id, sourceId),
        eq(timeTrackingEntries.gmUserId, session.gmUserId),
        eq(timeTrackingEntries.status, "submitted"),
        eq(timeTrackingEntries.isDeleted, false),
        sql`${timeTrackingEntries.startAt} is not null`,
        sql`${timeTrackingEntries.endAt} is not null`,
      ),
    )
    .limit(1);
  if (!extra?.startAt || !extra.endAt) return null;
  return {
    id: extra.id,
    kind,
    startAt: extra.startAt,
    endAt: extra.endAt,
    title: formatExtraLabel(extra.activityType),
    subtitle: extra.comment?.trim() || extra.marketAddress?.trim() || extra.marketName?.trim() || null,
  };
}

function assertFinishedDay(session: DaySessionContext) {
  if (session.status !== "ended" && session.status !== "submitted") {
    const error = new Error("Nur beendete Tage können korrigiert werden.") as Error & { status?: number; code?: string };
    error.status = 409;
    error.code = "day_not_finished";
    throw error;
  }
  if (!session.dayStartedAt || !session.dayEndedAt) {
    const error = new Error("Arbeitstag ist unvollständig und kann nicht korrigiert werden.") as Error & { status?: number; code?: string };
    error.status = 409;
    error.code = "day_incomplete";
    throw error;
  }
}

function assertStartedDay(session: DaySessionContext) {
  if (session.status !== "started" && session.status !== "ended" && session.status !== "submitted") {
    const error = new Error("Arbeitstag wurde noch nicht gestartet.") as Error & { status?: number; code?: string };
    error.status = 409;
    error.code = "day_not_started";
    throw error;
  }
  if (!session.dayStartedAt) {
    const error = new Error("Arbeitstag ist unvollstaendig und kann nicht korrigiert werden.") as Error & { status?: number; code?: string };
    error.status = 409;
    error.code = "day_incomplete";
    throw error;
  }
}

function validateKmChange(input: {
  session: DaySessionContext;
  sourceId: string;
  requestedStartKm: number | null;
  requestedEndKm: number | null;
}) {
  if (input.sourceId !== input.session.id) {
    throw buildHttpError("Kilometerstand konnte nicht eindeutig zugeordnet werden.", 404, "km_source_not_found");
  }
  if (input.requestedStartKm === null && input.requestedEndKm === null) {
    throw buildHttpError("Mindestens ein Kilometerstand muss angegeben werden.", 400, "invalid_km");
  }
  const nextStartKm = input.requestedStartKm ?? input.session.startKm;
  const nextEndKm = input.requestedEndKm ?? input.session.endKm;
  if (nextStartKm !== null && nextEndKm !== null && nextEndKm < nextStartKm) {
    throw buildHttpError("End-KM darf nicht kleiner als Start-KM sein.", 409, "invalid_km_range");
  }
  if (nextStartKm === input.session.startKm && nextEndKm === input.session.endKm) {
    throw buildHttpError("Die Kilometerstände wurden nicht geändert.", 409, "km_unchanged");
  }
}

async function applyApprovedTimeChange(input: {
  kind: SourceKind;
  sourceId: string;
  session: DaySessionContext;
  requestedStartAt: Date;
  requestedEndAt: Date;
  requestedStartKm: number | null;
  requestedEndKm: number | null;
  now: Date;
}) {
  if (input.kind === "day_km") {
    validateKmChange({
      session: input.session,
      sourceId: input.sourceId,
      requestedStartKm: input.requestedStartKm,
      requestedEndKm: input.requestedEndKm,
    });
    const [updated] = await db
      .update(gmDaySessions)
      .set({
        ...(input.requestedStartKm !== null
          ? { startKm: input.requestedStartKm, startKmDeferred: false, isStartKmCompleted: true }
          : {}),
        ...(input.requestedEndKm !== null
          ? { endKm: input.requestedEndKm, endKmDeferred: false, isEndKmCompleted: true }
          : {}),
        updatedAt: input.now,
      })
      .where(and(eq(gmDaySessions.id, input.session.id), eq(gmDaySessions.gmUserId, input.session.gmUserId), eq(gmDaySessions.isDeleted, false)))
      .returning({ id: gmDaySessions.id });
    return Boolean(updated);
  }

  if (input.kind === "day_start") {
    await validateDayStartChange({
      session: input.session,
      sourceId: input.sourceId,
      requestedStartAt: input.requestedStartAt,
      requestedEndAt: input.requestedEndAt,
      now: input.now,
    });
    const [updated] = await db
      .update(gmDaySessions)
      .set({ dayStartedAt: input.requestedStartAt, updatedAt: input.now })
      .where(and(eq(gmDaySessions.id, input.session.id), eq(gmDaySessions.gmUserId, input.session.gmUserId), eq(gmDaySessions.isDeleted, false)))
      .returning({ id: gmDaySessions.id });
    return Boolean(updated);
  }

  if (input.kind === "day_end") {
    await validateDayEndChange({
      session: input.session,
      sourceId: input.sourceId,
      requestedStartAt: input.requestedStartAt,
      requestedEndAt: input.requestedEndAt,
      now: input.now,
    });
    const [updated] = await db
      .update(gmDaySessions)
      .set({ dayEndedAt: input.requestedEndAt, updatedAt: input.now })
      .where(and(eq(gmDaySessions.id, input.session.id), eq(gmDaySessions.gmUserId, input.session.gmUserId), eq(gmDaySessions.isDeleted, false)))
      .returning({ id: gmDaySessions.id });
    return Boolean(updated);
  }

  await validateGmTimelineInterval({
    gmUserId: input.session.gmUserId,
    kind: input.kind,
    id: input.sourceId,
    startAt: input.requestedStartAt,
    endAt: input.requestedEndAt,
    now: input.now,
  });

  if (input.kind === "marktbesuch") {
    const [updated] = await db
      .update(visitSessions)
      .set({ startedAt: input.requestedStartAt, submittedAt: input.requestedEndAt, updatedAt: input.now })
      .where(
        and(
          eq(visitSessions.id, input.sourceId),
          eq(visitSessions.gmUserId, input.session.gmUserId),
          eq(visitSessions.status, "submitted"),
          eq(visitSessions.isDeleted, false),
        ),
      )
      .returning({ id: visitSessions.id });
    return Boolean(updated);
  }

  if (input.kind === "pause") {
    const [updated] = await db
      .update(gmDaySessionPauses)
      .set({ pauseStartedAt: input.requestedStartAt, pauseEndedAt: input.requestedEndAt, updatedAt: input.now })
      .where(
        and(
          eq(gmDaySessionPauses.id, input.sourceId),
          eq(gmDaySessionPauses.daySessionId, input.session.id),
          eq(gmDaySessionPauses.gmUserId, input.session.gmUserId),
          eq(gmDaySessionPauses.isDeleted, false),
        ),
      )
      .returning({ id: gmDaySessionPauses.id });
    return Boolean(updated);
  }

  const [updated] = await db
    .update(timeTrackingEntries)
    .set({ startAt: input.requestedStartAt, endAt: input.requestedEndAt, updatedAt: input.now })
    .where(
      and(
        eq(timeTrackingEntries.id, input.sourceId),
        eq(timeTrackingEntries.gmUserId, input.session.gmUserId),
        eq(timeTrackingEntries.status, "submitted"),
        eq(timeTrackingEntries.isDeleted, false),
      ),
    )
    .returning({ id: timeTrackingEntries.id });
  return Boolean(updated);
}

export const gmTimeEntryChangeRequestsRouter = Router();
gmTimeEntryChangeRequestsRouter.use(requireAuth(["gm"]));

gmTimeEntryChangeRequestsRouter.get("/", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const parsed = gmListTimeChangeRequestsSchema.safeParse({
      from: typeof req.query.from === "string" ? req.query.from : undefined,
      to: typeof req.query.to === "string" ? req.query.to : undefined,
    });
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Anfrage." });
      return;
    }
    const clauses = [
      eq(timeEntryChangeRequests.gmUserId, gmUserId),
      eq(timeEntryChangeRequests.isDeleted, false),
    ];
    if (parsed.data.from) clauses.push(gte(timeEntryChangeRequests.workDate, parsed.data.from));
    if (parsed.data.to) clauses.push(lte(timeEntryChangeRequests.workDate, parsed.data.to));

    const rows = await db
      .select()
      .from(timeEntryChangeRequests)
      .where(and(...clauses))
      .orderBy(
        sql`case when ${timeEntryChangeRequests.status} = 'pending' then 0 else 1 end`,
        desc(timeEntryChangeRequests.updatedAt),
      )
      .limit(200);

    res.status(200).json({ requests: rows.map((row) => mapRequestRow(row)) });
  } catch (error) {
    next(error);
  }
});

gmTimeEntryChangeRequestsRouter.post("/", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const parsed = gmCreateTimeChangeRequestSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Änderungsanfrage ist ungültig.", code: "invalid_time_change_request" });
      return;
    }

    const session = await loadDaySession(parsed.data.sessionId, gmUserId);
    if (!session) {
      res.status(404).json({ error: "Arbeitstag nicht gefunden.", code: "day_session_not_found" });
      return;
    }
    assertStartedDay(session);

    let title = "Kilometerstand";
    let subtitle: string | null = null;
    let originalStartAt = session.dayStartedAt ?? new Date();
    let originalEndAt = session.dayEndedAt ?? originalStartAt;
    let requestedStartAt = originalStartAt;
    let requestedEndAt = originalEndAt;
    let originalStartKm: number | null = null;
    let originalEndKm: number | null = null;
    let requestedStartKm: number | null = null;
    let requestedEndKm: number | null = null;

    if (parsed.data.kind === "day_km") {
      requestedStartKm = parsed.data.requestedStartKm ?? null;
      requestedEndKm = parsed.data.requestedEndKm ?? null;
      validateKmChange({
        session,
        sourceId: parsed.data.segmentId,
        requestedStartKm,
        requestedEndKm,
      });
      originalStartKm = session.startKm;
      originalEndKm = session.endKm;
      subtitle = session.workDate;
    } else {
      const source = await loadSourceSnapshot(parsed.data.kind, parsed.data.segmentId, session);
      if (!source) {
        res.status(404).json({ error: "Zeiteintrag nicht gefunden.", code: "time_source_not_found" });
        return;
      }
      if (!parsed.data.requestedStartTime || !parsed.data.requestedEndTime) {
        res.status(400).json({ error: "Ungültige Uhrzeit.", code: "invalid_time" });
        return;
      }
      const parsedStartAt = parseWorkDateHmToUtc(session.workDate, parsed.data.requestedStartTime, session.timezone || "Europe/Vienna");
      const parsedEndAt = parseWorkDateHmToUtc(session.workDate, parsed.data.requestedEndTime, session.timezone || "Europe/Vienna");
      if (!parsedStartAt || !parsedEndAt) {
        res.status(400).json({ error: "Ungültige Uhrzeit.", code: "invalid_time" });
        return;
      }
      title = source.title;
      subtitle = source.subtitle;
      originalStartAt = source.startAt;
      originalEndAt = source.endAt;
      requestedStartAt = parsedStartAt;
      requestedEndAt = parsedEndAt;

      if (parsed.data.kind === "day_start") {
        await validateDayStartChange({ session, sourceId: parsed.data.segmentId, requestedStartAt, requestedEndAt });
      } else if (parsed.data.kind === "day_end") {
        await validateDayEndChange({ session, sourceId: parsed.data.segmentId, requestedStartAt, requestedEndAt });
      } else {
        await validateGmTimelineInterval({
          gmUserId,
          kind: parsed.data.kind,
          id: parsed.data.segmentId,
          startAt: requestedStartAt,
          endAt: requestedEndAt,
        });
      }
    }

    const now = new Date();
    const [saved] = await db.transaction(async (tx) => {
      const [existing] = await tx
        .select()
        .from(timeEntryChangeRequests)
        .where(
          and(
            eq(timeEntryChangeRequests.gmUserId, gmUserId),
            eq(timeEntryChangeRequests.sourceKind, parsed.data.kind),
            eq(timeEntryChangeRequests.sourceId, parsed.data.segmentId),
            eq(timeEntryChangeRequests.status, "pending"),
            eq(timeEntryChangeRequests.isDeleted, false),
          ),
        )
        .limit(1);

      if (existing) {
        return tx
          .update(timeEntryChangeRequests)
          .set({
            titleSnapshot: title,
            subtitleSnapshot: subtitle,
            requestedStartAt,
            requestedEndAt,
            requestedStartKm,
            requestedEndKm,
            requestNote: parsed.data.requestNote?.trim() || null,
            updatedAt: now,
          })
          .where(eq(timeEntryChangeRequests.id, existing.id))
          .returning();
      }

      return tx
        .insert(timeEntryChangeRequests)
        .values({
          daySessionId: session.id,
          gmUserId,
          sourceKind: parsed.data.kind,
          sourceId: parsed.data.segmentId,
          workDate: session.workDate,
          timezone: session.timezone || "Europe/Vienna",
          titleSnapshot: title,
          subtitleSnapshot: subtitle,
          originalStartAt,
          originalEndAt,
          requestedStartAt,
          requestedEndAt,
          originalStartKm,
          originalEndKm,
          requestedStartKm,
          requestedEndKm,
          requestNote: parsed.data.requestNote?.trim() || null,
          status: "pending",
          createdAt: now,
          updatedAt: now,
        })
        .returning();
    });

    res.status(201).json({ ok: true, request: saved ? mapRequestRow(saved) : null });
  } catch (error) {
    const err = error as Error & { status?: number; code?: string };
    if (err.status) {
      res.status(err.status).json({ error: err.message, code: err.code });
      return;
    }
    next(error);
  }
});

export const adminTimeEntryChangeRequestsRouter = Router();
adminTimeEntryChangeRequestsRouter.use(requireAuth(["admin"]));

adminTimeEntryChangeRequestsRouter.get("/", async (_req: AuthedRequest, res, next) => {
  try {
    const rows = await db
      .select({
        id: timeEntryChangeRequests.id,
        daySessionId: timeEntryChangeRequests.daySessionId,
        gmUserId: timeEntryChangeRequests.gmUserId,
        sourceKind: timeEntryChangeRequests.sourceKind,
        sourceId: timeEntryChangeRequests.sourceId,
        workDate: timeEntryChangeRequests.workDate,
        timezone: timeEntryChangeRequests.timezone,
        titleSnapshot: timeEntryChangeRequests.titleSnapshot,
        subtitleSnapshot: timeEntryChangeRequests.subtitleSnapshot,
        originalStartAt: timeEntryChangeRequests.originalStartAt,
        originalEndAt: timeEntryChangeRequests.originalEndAt,
        requestedStartAt: timeEntryChangeRequests.requestedStartAt,
        requestedEndAt: timeEntryChangeRequests.requestedEndAt,
        originalStartKm: timeEntryChangeRequests.originalStartKm,
        originalEndKm: timeEntryChangeRequests.originalEndKm,
        requestedStartKm: timeEntryChangeRequests.requestedStartKm,
        requestedEndKm: timeEntryChangeRequests.requestedEndKm,
        requestNote: timeEntryChangeRequests.requestNote,
        status: timeEntryChangeRequests.status,
        reviewedByUserId: timeEntryChangeRequests.reviewedByUserId,
        reviewedAt: timeEntryChangeRequests.reviewedAt,
        appliedAt: timeEntryChangeRequests.appliedAt,
        adminNote: timeEntryChangeRequests.adminNote,
        createdAt: timeEntryChangeRequests.createdAt,
        updatedAt: timeEntryChangeRequests.updatedAt,
        gmFirstName: users.firstName,
        gmLastName: users.lastName,
        gmEmail: users.email,
        gmRegion: users.region,
      })
      .from(timeEntryChangeRequests)
      .innerJoin(users, eq(users.id, timeEntryChangeRequests.gmUserId))
      .where(eq(timeEntryChangeRequests.isDeleted, false))
      .orderBy(
        sql`case when ${timeEntryChangeRequests.status} = 'pending' then 0 else 1 end`,
        desc(timeEntryChangeRequests.createdAt),
      )
      .limit(250);

    res.status(200).json({ requests: rows.map((row) => mapRequestRow(row)) });
  } catch (error) {
    next(error);
  }
});

adminTimeEntryChangeRequestsRouter.patch("/:requestId/reject", async (req: AuthedRequest, res, next) => {
  try {
    const requestId = String(req.params.requestId ?? "");
    if (!z.string().uuid().safeParse(requestId).success) {
      res.status(400).json({ error: "Ungültige Anfrage-ID.", code: "invalid_request_id" });
      return;
    }
    const parsed = adminDecisionSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültiges Entscheidungs-Payload.", code: "invalid_payload" });
      return;
    }
    const now = new Date();
    const [updated] = await db
      .update(timeEntryChangeRequests)
      .set({
        status: "rejected",
        reviewedByUserId: req.authUser?.appUserId ?? null,
        reviewedAt: now,
        adminNote: parsed.data.adminNote || null,
        updatedAt: now,
      })
      .where(and(eq(timeEntryChangeRequests.id, requestId), eq(timeEntryChangeRequests.status, "pending"), eq(timeEntryChangeRequests.isDeleted, false)))
      .returning();
    if (!updated) {
      res.status(404).json({ error: "Offene Anfrage wurde nicht gefunden.", code: "request_not_found" });
      return;
    }
    res.status(200).json({ ok: true, request: mapRequestRow(updated) });
  } catch (error) {
    next(error);
  }
});

adminTimeEntryChangeRequestsRouter.patch("/:requestId/approve", async (req: AuthedRequest, res, next) => {
  try {
    const requestId = String(req.params.requestId ?? "");
    if (!z.string().uuid().safeParse(requestId).success) {
      res.status(400).json({ error: "Ungültige Anfrage-ID.", code: "invalid_request_id" });
      return;
    }
    const parsed = adminDecisionSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültiges Entscheidungs-Payload.", code: "invalid_payload" });
      return;
    }

    const now = new Date();
    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`select id from ${timeEntryChangeRequests} where ${timeEntryChangeRequests.id} = ${requestId} for update`);
      const [requestRow] = await tx
        .select()
        .from(timeEntryChangeRequests)
        .where(and(eq(timeEntryChangeRequests.id, requestId), eq(timeEntryChangeRequests.isDeleted, false)))
        .limit(1);
      if (!requestRow || requestRow.status !== "pending") {
        const error = new Error("Offene Anfrage wurde nicht gefunden.") as Error & { status?: number; code?: string };
        error.status = 404;
        error.code = "request_not_found";
        throw error;
      }

      const session = await loadDaySession(requestRow.daySessionId, requestRow.gmUserId);
      if (!session) {
        const error = new Error("Arbeitstag nicht gefunden.") as Error & { status?: number; code?: string };
        error.status = 404;
        error.code = "day_session_not_found";
        throw error;
      }
      assertStartedDay(session);

      const applied = await applyApprovedTimeChange({
        kind: requestRow.sourceKind,
        sourceId: requestRow.sourceId,
        session,
        requestedStartAt: requestRow.requestedStartAt,
        requestedEndAt: requestRow.requestedEndAt,
        requestedStartKm: requestRow.requestedStartKm,
        requestedEndKm: requestRow.requestedEndKm,
        now,
      });
      if (!applied) {
        const error = new Error("Zeiteintrag konnte nicht aktualisiert werden.") as Error & { status?: number; code?: string };
        error.status = 404;
        error.code = "time_source_not_found";
        throw error;
      }

      const [updated] = await tx
        .update(timeEntryChangeRequests)
        .set({
          status: "approved",
          reviewedByUserId: req.authUser?.appUserId ?? null,
          reviewedAt: now,
          appliedAt: now,
          adminNote: parsed.data.adminNote || null,
          updatedAt: now,
        })
        .where(eq(timeEntryChangeRequests.id, requestRow.id))
        .returning();
      return updated;
    });

    res.status(200).json({ ok: true, request: result ? mapRequestRow(result) : null });
  } catch (error) {
    const err = error as Error & { status?: number; code?: string };
    if (err.status) {
      res.status(err.status).json({ error: err.message, code: err.code });
      return;
    }
    next(error);
  }
});
