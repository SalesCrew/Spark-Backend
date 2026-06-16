import { and, desc, eq, inArray, isNotNull, sql } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import {
  buildDaySessionPayload,
  resolvePrimaryQuestionnaireType,
  validateTimelineInterval,
  type BaseAction,
  type DaySessionPayload,
  type TimelineInterval,
} from "../lib/admin-zeiterfassung.js";
import { logAction, startActionTimer } from "../lib/logger.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { db } from "../lib/db.js";
import { DEFAULT_TIMEZONE, getCurrentDaySessionForUser, toYmdInTimezone } from "../lib/day-session.js";
import {
  TimelineValidationError,
  respondTimelineValidationError,
  validateGmTimelineInterval,
} from "../lib/zeiterfassung-validation.js";
import {
  gmDaySessionPauses,
  gmDaySessions,
  markets,
  timeTrackingEntries,
  users,
  visitSessionSections,
  visitSessions,
} from "../lib/schema.js";

const daySessionRouter = Router();
daySessionRouter.use(requireAuth(["gm", "admin"]));
daySessionRouter.use((req, res, next) => {
  if (req.method.toUpperCase() === "GET") {
    next();
    return;
  }
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "day_session_action_completed", {
      req,
      action: "day_session_mutation",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

const timezoneSchema = z.string().trim().min(1).max(120);
const kmSchema = z.number().int().min(0).max(2_000_000);
const isoDateTimeSchema = z.string().datetime();
const hhmmRegex = /^([01]\d|2[0-3]):([0-5]\d)$/;

const startSchema = z
  .object({
    timezone: timezoneSchema.optional(),
    startedAt: isoDateTimeSchema.optional(),
  })
  .strict();

const setKmSchema = z
  .object({
    km: kmSchema,
  })
  .strict();

const setEndSchema = z
  .object({
    endAt: isoDateTimeSchema.optional(),
  })
  .strict();

const manualPauseSchema = z
  .object({
    startAt: isoDateTimeSchema,
    endAt: isoDateTimeSchema,
  })
  .strict();

const editableReviewSegmentKindSchema = z.enum(["day_start", "day_end", "marktbesuch", "pause", "zusatzzeit"]);
const reviewEditsSchema = z
  .object({
    sessionId: z.string().uuid(),
    edits: z
      .array(
        z
          .object({
            kind: editableReviewSegmentKindSchema,
            segmentId: z.string().uuid(),
            startTime: z.string().regex(hhmmRegex),
            endTime: z.string().regex(hhmmRegex),
          })
          .strict(),
      )
      .max(80),
  })
  .strict();

const submitSchema = z
  .object({
    comment: z.string().trim().max(2_000).optional(),
  })
  .strict();

const MAX_REQUESTED_START_AGE_MS = 36 * 60 * 60 * 1000;
const MAX_REQUESTED_START_FUTURE_MS = 2 * 60 * 1000;

function resolveAcceptedStartAt(startedAt: string | undefined, now: Date): Date {
  if (!startedAt) return now;
  const requested = new Date(startedAt);
  const requestedMs = requested.getTime();
  if (!Number.isFinite(requestedMs)) return now;
  const nowMs = now.getTime();
  if (requestedMs > nowMs + MAX_REQUESTED_START_FUTURE_MS) return now;
  if (requestedMs < nowMs - MAX_REQUESTED_START_AGE_MS) return now;
  return requested;
}

function serializeDaySession(row: typeof gmDaySessions.$inferSelect | null) {
  if (!row) {
    return null;
  }
  return {
    id: row.id,
    gmUserId: row.gmUserId,
    workDate: row.workDate,
    timezone: row.timezone,
    status: row.status,
    dayStartedAt: row.dayStartedAt?.toISOString() ?? null,
    dayEndedAt: row.dayEndedAt?.toISOString() ?? null,
    startKm: row.startKm,
    endKm: row.endKm,
    startKmDeferred: row.startKmDeferred,
    endKmDeferred: row.endKmDeferred,
    isStartKmCompleted: row.isStartKmCompleted,
    isEndKmCompleted: row.isEndKmCompleted,
    comment: row.comment,
    submittedAt: row.submittedAt?.toISOString() ?? null,
    cancelledAt: row.cancelledAt?.toISOString() ?? null,
  };
}

type SubmissionItem = {
  id: string;
  kind: "day" | "markt" | "zusatz" | "pause";
  submittedAt: string;
  label: string;
  timeText: string;
};

function serializePause(row: typeof gmDaySessionPauses.$inferSelect) {
  return {
    id: row.id,
    daySessionId: row.daySessionId,
    gmUserId: row.gmUserId,
    pauseStartedAt: row.pauseStartedAt.toISOString(),
    pauseEndedAt: row.pauseEndedAt?.toISOString() ?? null,
  };
}

function timelineSegmentToLegacyItem(segment: DaySessionPayload["timeline"][number]): SubmissionItem {
  const kind: SubmissionItem["kind"] =
    segment.kind === "marktbesuch"
      ? "markt"
      : segment.kind === "zusatzzeit"
        ? "zusatz"
        : segment.kind === "pause"
          ? "pause"
          : "day";
  return {
    id: segment.id,
    kind,
    submittedAt: `${segment.start}-${segment.end}`,
    label: segment.title,
    timeText: `${segment.start} - ${segment.end}`,
  };
}

async function loadTodaySession(gmUserId: string, now: Date, timezone: string) {
  return getCurrentDaySessionForUser({ gmUserId, now, timezone });
}

async function loadCurrentWritableSession(gmUserId: string, now: Date, timezone: string) {
  return (await loadTodaySession(gmUserId, now, timezone)) ?? (await loadAnyOpenSessionForUser(gmUserId));
}

async function loadAnyOpenSessionForUser(gmUserId: string) {
  const [row] = await db
    .select()
    .from(gmDaySessions)
    .where(
      and(
        eq(gmDaySessions.gmUserId, gmUserId),
        eq(gmDaySessions.isDeleted, false),
        sql`${gmDaySessions.status} in ('started','ended')`,
      ),
    )
    .orderBy(desc(gmDaySessions.dayStartedAt), desc(gmDaySessions.updatedAt))
    .limit(1);
  return row ?? null;
}

async function loadOpenPause(gmUserId: string, daySessionId: string) {
  const [row] = await db
    .select()
    .from(gmDaySessionPauses)
    .where(
      and(
        eq(gmDaySessionPauses.gmUserId, gmUserId),
        eq(gmDaySessionPauses.daySessionId, daySessionId),
        eq(gmDaySessionPauses.isDeleted, false),
        isNotNull(gmDaySessionPauses.pauseStartedAt),
        sql`${gmDaySessionPauses.pauseEndedAt} IS NULL`,
      ),
    )
    .orderBy(desc(gmDaySessionPauses.pauseStartedAt))
    .limit(1);
  return row ?? null;
}

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

async function loadReviewIntervals(input: {
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
          eq(gmDaySessionPauses.gmUserId, input.gmUserId),
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

daySessionRouter.get("/current", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const todaySession = await loadTodaySession(gmUserId, now, DEFAULT_TIMEZONE);
    const session = todaySession ?? (await loadAnyOpenSessionForUser(gmUserId));
    const openPause = session ? await loadOpenPause(gmUserId, session.id) : null;
    res.status(200).json({
      session: serializeDaySession(session ?? null),
      gate: {
        dayStarted: Boolean(session && session.status === "started" && session.dayStartedAt),
        startKmPending: Boolean(session && !session.isStartKmCompleted),
        endKmPending: Boolean(session && Boolean(session.dayEndedAt) && !session.isEndKmCompleted),
        pauseOpen: Boolean(openPause),
      },
    });
  } catch (error) {
    next(error);
  }
});

daySessionRouter.post("/pause/start", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const session = await loadTodaySession(gmUserId, now, DEFAULT_TIMEZONE);
    if (!session || session.status !== "started") {
      res.status(409).json({ error: "Arbeitstag wurde noch nicht gestartet.", code: "day_not_started" });
      return;
    }
    const openPause = await loadOpenPause(gmUserId, session.id);
    if (openPause) {
      res.status(409).json({ error: "Pause laeuft bereits.", code: "pause_already_started" });
      return;
    }
    const [created] = await db
      .insert(gmDaySessionPauses)
      .values({
        daySessionId: session.id,
        gmUserId,
        pauseStartedAt: now,
      })
      .returning();
    if (!created) throw new Error("Pause konnte nicht gestartet werden.");
    res.status(200).json({ pause: serializePause(created) });
  } catch (error) {
    next(error);
  }
});

daySessionRouter.post("/pause/end", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const session = await loadTodaySession(gmUserId, now, DEFAULT_TIMEZONE);
    if (!session || session.status !== "started") {
      res.status(409).json({ error: "Arbeitstag wurde noch nicht gestartet.", code: "day_not_started" });
      return;
    }
    const openPause = await loadOpenPause(gmUserId, session.id);
    if (!openPause) {
      res.status(409).json({ error: "Keine aktive Pause gefunden.", code: "pause_not_started" });
      return;
    }
    const [updated] = await db
      .update(gmDaySessionPauses)
      .set({
        pauseEndedAt: now,
        updatedAt: now,
      })
      .where(eq(gmDaySessionPauses.id, openPause.id))
      .returning();
    if (!updated) throw new Error("Pause konnte nicht beendet werden.");
    res.status(200).json({ pause: serializePause(updated) });
  } catch (error) {
    next(error);
  }
});

daySessionRouter.post("/pause/manual", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const parsed = manualPauseSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Bitte gueltige Pausenzeiten uebermitteln.", code: "invalid_pause_time" });
      return;
    }
    const startAt = new Date(parsed.data.startAt);
    const endAt = new Date(parsed.data.endAt);
    if (
      !Number.isFinite(startAt.getTime()) ||
      !Number.isFinite(endAt.getTime()) ||
      endAt.getTime() <= startAt.getTime()
    ) {
      res.status(400).json({ error: "Endzeit muss nach Startzeit liegen.", code: "invalid_interval" });
      return;
    }

    const { session } = await validateGmTimelineInterval({
      gmUserId,
      kind: "pause",
      id: "manual-pause-preview",
      startAt,
      endAt,
    });

    if (session.status !== "started") {
      res.status(409).json({ error: "Arbeitstag wurde noch nicht gestartet.", code: "day_not_started" });
      return;
    }

    const openPause = await loadOpenPause(gmUserId, session.id);
    if (openPause) {
      res.status(409).json({ error: "Pause laeuft bereits.", code: "pause_already_started" });
      return;
    }

    const [created] = await db
      .insert(gmDaySessionPauses)
      .values({
        daySessionId: session.id,
        gmUserId,
        pauseStartedAt: startAt,
        pauseEndedAt: endAt,
      })
      .returning();
    if (!created) throw new Error("Pause konnte nicht gespeichert werden.");
    res.status(200).json({ pause: serializePause(created) });
  } catch (error) {
    if (error instanceof TimelineValidationError && error.code === "overlap") {
      res.status(409).json({
        error:
          "Diese Pause ist nicht moeglich, weil sie sich mit einem Marktbesuch oder einer anderen Pause ueberschneidet. Pausen duerfen nur Zusatzzeiterfassung schneiden.",
        code: "overlap",
      });
      return;
    }
    if (respondTimelineValidationError(res, error)) return;
    next(error);
  }
});

daySessionRouter.patch("/review-edits", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const parsed = reviewEditsSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Zeitkorrekturen.", code: "invalid_review_edits" });
      return;
    }

    const { sessionId, edits } = parsed.data;
    if (edits.length === 0) {
      res.status(200).json({ ok: true });
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
          eq(gmDaySessions.id, sessionId),
          eq(gmDaySessions.gmUserId, gmUserId),
          eq(gmDaySessions.isDeleted, false),
        ),
      )
      .limit(1);

    if (!session) {
      res.status(404).json({ error: "Arbeitstag nicht gefunden.", code: "day_not_found" });
      return;
    }
    if (session.status !== "ended") {
      res.status(409).json({
        error: "Zeitkorrekturen sind erst nach Tagesende und vor Tag abschliessen moeglich.",
        code: "day_not_in_review",
      });
      return;
    }
    if (!session.dayStartedAt || !session.dayEndedAt) {
      res.status(409).json({ error: "Arbeitstag ist unvollstaendig.", code: "day_incomplete" });
      return;
    }

    const timezone = session.timezone || DEFAULT_TIMEZONE;
    const currentIntervals = await loadReviewIntervals({
      daySessionId: session.id,
      gmUserId,
      workDate: session.workDate,
      timezone,
    });
    const intervalByKey = new Map(currentIntervals.map((interval) => [`${interval.kind}:${interval.id}`, interval]));
    const nextIntervals = currentIntervals.map((interval) => ({ ...interval }));
    const updates = new Map<
      string,
      {
        kind: "marktbesuch" | "pause" | "zusatzzeit";
        id: string;
        startAt: Date;
        endAt: Date;
      }
    >();
    let nextDayStartAt = session.dayStartedAt;
    let nextDayEndAt = session.dayEndedAt;
    const daySessionUpdates: { dayStartedAt?: Date; dayEndedAt?: Date } = {};

    for (const edit of edits) {
      if (edit.kind === "day_start" || edit.kind === "day_end") {
        if (edit.segmentId !== session.id) {
          res.status(404).json({ error: "Arbeitstag nicht gefunden.", code: "day_segment_not_found" });
          return;
        }
        const editedAt = parseWorkDateHmToUtc(
          session.workDate,
          edit.kind === "day_start" ? edit.startTime : edit.endTime,
          timezone,
        );
        if (!editedAt) {
          res.status(400).json({ error: "Bitte gueltige Uhrzeiten im Format HH:MM eingeben.", code: "invalid_time" });
          return;
        }
        if (edit.kind === "day_start") {
          nextDayStartAt = editedAt;
          daySessionUpdates.dayStartedAt = editedAt;
        } else {
          nextDayEndAt = editedAt;
          daySessionUpdates.dayEndedAt = editedAt;
        }
        continue;
      }

      const key = `${edit.kind}:${edit.segmentId}`;
      const current = intervalByKey.get(key);
      if (!current) {
        res.status(404).json({ error: "Zeiteintrag nicht gefunden.", code: "segment_not_found" });
        return;
      }
      const startAt = parseWorkDateHmToUtc(session.workDate, edit.startTime, timezone);
      const endAt = parseWorkDateHmToUtc(session.workDate, edit.endTime, timezone);
      if (!startAt || !endAt) {
        res.status(400).json({ error: "Bitte gueltige Uhrzeiten im Format HH:MM eingeben.", code: "invalid_time" });
        return;
      }
      const index = nextIntervals.findIndex((interval) => interval.kind === edit.kind && interval.id === edit.segmentId);
      if (index < 0) {
        res.status(404).json({ error: "Zeiteintrag nicht gefunden.", code: "segment_not_found" });
        return;
      }
      const existingInterval = nextIntervals[index];
      if (!existingInterval) {
        res.status(404).json({ error: "Zeiteintrag nicht gefunden.", code: "segment_not_found" });
        return;
      }
      nextIntervals[index] = { ...existingInterval, startAt, endAt };
      updates.set(key, { kind: edit.kind, id: edit.segmentId, startAt, endAt });
    }

    if (nextDayEndAt.getTime() <= nextDayStartAt.getTime()) {
      res.status(400).json({ error: "Tagesende muss nach Tagesstart liegen.", code: "invalid_day_interval" });
      return;
    }

    for (const interval of nextIntervals) {
      const validation = validateTimelineInterval({
        kind: interval.kind,
        id: interval.id,
        startAt: interval.startAt,
        endAt: interval.endAt,
        dayStartAt: nextDayStartAt,
        dayEndAt: nextDayEndAt,
        intervals: nextIntervals,
      });
      if (!validation.ok) {
        const message =
          validation.code === "overlap"
            ? "Dieser Zeitraum ist nicht moeglich, weil er sich mit einem bestehenden Eintrag ueberschneidet."
            : validation.code === "outside_day"
              ? "Tagesstart und Tagesende muessen alle Eintraege vollstaendig umfassen."
            : validation.message;
        res.status(validation.code === "overlap" ? 409 : 400).json({
          error: message,
          code: validation.code,
        });
        return;
      }
    }

    const now = new Date();
    await db.transaction(async (tx) => {
      if (daySessionUpdates.dayStartedAt || daySessionUpdates.dayEndedAt) {
        await tx
          .update(gmDaySessions)
          .set({
            ...daySessionUpdates,
            updatedAt: now,
          })
          .where(and(eq(gmDaySessions.id, session.id), eq(gmDaySessions.gmUserId, gmUserId)));
      }
      for (const update of updates.values()) {
        if (update.kind === "marktbesuch") {
          await tx
            .update(visitSessions)
            .set({
              startedAt: update.startAt,
              submittedAt: update.endAt,
              updatedAt: now,
            })
            .where(and(eq(visitSessions.id, update.id), eq(visitSessions.gmUserId, gmUserId)));
        } else if (update.kind === "pause") {
          await tx
            .update(gmDaySessionPauses)
            .set({
              pauseStartedAt: update.startAt,
              pauseEndedAt: update.endAt,
              updatedAt: now,
            })
            .where(
              and(
                eq(gmDaySessionPauses.id, update.id),
                eq(gmDaySessionPauses.daySessionId, session.id),
                eq(gmDaySessionPauses.gmUserId, gmUserId),
              ),
            );
        } else {
          await tx
            .update(timeTrackingEntries)
            .set({
              startAt: update.startAt,
              endAt: update.endAt,
              updatedAt: now,
            })
            .where(and(eq(timeTrackingEntries.id, update.id), eq(timeTrackingEntries.gmUserId, gmUserId)));
        }
      }
    });

    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

daySessionRouter.get("/today-submissions", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const timezone = DEFAULT_TIMEZONE;
    const session = (await loadTodaySession(gmUserId, now, timezone)) ?? (await loadAnyOpenSessionForUser(gmUserId));
    if (!session || !session.dayStartedAt) {
      res.status(200).json({ items: [], timeline: [], stats: null, session: null });
      return;
    }

    const workDate = session.workDate || toYmdInTimezone(session.dayStartedAt, timezone);
    const dayStartExpr = sql`(${workDate}::timestamp at time zone ${timezone})`;
    const dayEndExpr = sql`((${workDate}::timestamp at time zone ${timezone}) + interval '1 day')`;

    const [userRows, visitRows, zusatzRows, pauseRows] = await Promise.all([
      db
        .select({
          firstName: users.firstName,
          lastName: users.lastName,
          region: users.region,
        })
        .from(users)
        .where(eq(users.id, gmUserId))
        .limit(1),
      db
        .select({
          id: visitSessions.id,
          startedAt: visitSessions.startedAt,
          submittedAt: visitSessions.submittedAt,
          marketName: markets.name,
          marketAddress: markets.address,
        })
        .from(visitSessions)
        .leftJoin(markets, eq(markets.id, visitSessions.marketId))
        .where(
          and(
            eq(visitSessions.gmUserId, gmUserId),
            eq(visitSessions.isDeleted, false),
            eq(visitSessions.status, "submitted"),
            isNotNull(visitSessions.startedAt),
            isNotNull(visitSessions.submittedAt),
            sql`${visitSessions.startedAt} >= ${dayStartExpr}`,
            sql`${visitSessions.startedAt} < ${dayEndExpr}`,
          ),
        )
        .orderBy(desc(visitSessions.startedAt)),
      db
        .select({
          id: timeTrackingEntries.id,
          startAt: timeTrackingEntries.startAt,
          endAt: timeTrackingEntries.endAt,
          submittedAt: timeTrackingEntries.submittedAt,
          status: timeTrackingEntries.status,
          activityType: timeTrackingEntries.activityType,
          comment: timeTrackingEntries.comment,
          marketName: markets.name,
          marketAddress: markets.address,
        })
        .from(timeTrackingEntries)
        .leftJoin(markets, eq(markets.id, timeTrackingEntries.marketId))
        .where(
          and(
            eq(timeTrackingEntries.gmUserId, gmUserId),
            eq(timeTrackingEntries.isDeleted, false),
            eq(timeTrackingEntries.status, "submitted"),
            isNotNull(timeTrackingEntries.startAt),
            isNotNull(timeTrackingEntries.endAt),
            sql`${timeTrackingEntries.startAt} >= ${dayStartExpr}`,
            sql`${timeTrackingEntries.startAt} < ${dayEndExpr}`,
          ),
        )
        .orderBy(desc(timeTrackingEntries.startAt)),
      db
        .select({
          id: gmDaySessionPauses.id,
          pauseStartedAt: gmDaySessionPauses.pauseStartedAt,
          pauseEndedAt: gmDaySessionPauses.pauseEndedAt,
        })
        .from(gmDaySessionPauses)
        .where(
          and(
            eq(gmDaySessionPauses.gmUserId, gmUserId),
            eq(gmDaySessionPauses.daySessionId, session.id),
            eq(gmDaySessionPauses.isDeleted, false),
            isNotNull(gmDaySessionPauses.pauseStartedAt),
          ),
        )
        .orderBy(desc(gmDaySessionPauses.pauseStartedAt)),
    ]);

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

    const actions: BaseAction[] = [];
    for (const row of visitRows) {
      if (!row.startedAt || !row.submittedAt || row.submittedAt.getTime() <= row.startedAt.getTime()) continue;
      const sectionTypes = Array.from(
        new Set((sectionTypesByVisitId.get(row.id) ?? []).map((value) => String(value).toLowerCase())),
      );
      const primaryQuestionnaireType = resolvePrimaryQuestionnaireType(sectionTypes);
      actions.push({
        id: row.id,
        kind: "marktbesuch",
        startAt: row.startedAt,
        endAt: row.submittedAt,
        ...(row.marketName ? { marketName: row.marketName } : {}),
        ...(row.marketAddress ? { marketAddress: row.marketAddress } : {}),
        ...(primaryQuestionnaireType ? { questionnaireType: primaryQuestionnaireType } : {}),
        ...(sectionTypes.length > 0 ? { questionnaireTypes: sectionTypes } : {}),
      });
    }
    for (const row of pauseRows) {
      const endAt = row.pauseEndedAt ?? (session.status === "started" ? now : null);
      if (!endAt) continue;
      if (endAt.getTime() <= row.pauseStartedAt.getTime()) continue;
      actions.push({
        id: row.id,
        kind: "pause",
        startAt: row.pauseStartedAt,
        endAt,
      });
    }
    for (const row of zusatzRows) {
      if (!row.startAt || !row.endAt || row.endAt.getTime() <= row.startAt.getTime()) continue;
      actions.push({
        id: row.id,
        kind: "zusatzzeit",
        startAt: row.startAt,
        endAt: row.endAt,
        subtype: row.activityType,
        ...(row.comment ? { comment: row.comment } : {}),
        ...(row.marketName ? { marketName: row.marketName } : {}),
        ...(row.marketAddress ? { marketAddress: row.marketAddress } : {}),
      });
    }

    const user = userRows[0];
    const gmName = `${user?.firstName ?? ""} ${user?.lastName ?? ""}`.trim();
    const defaultEnd = session.dayEndedAt ?? (session.status === "submitted" ? session.dayStartedAt : now);
    const dayEndAt =
      defaultEnd.getTime() > session.dayStartedAt.getTime()
        ? defaultEnd
        : new Date(session.dayStartedAt.getTime() + 60_000);
    const status: "started" | "ended" | "submitted" =
      session.status === "started" || session.status === "ended" || session.status === "submitted"
        ? session.status
        : "started";
    const payload = buildDaySessionPayload({
      sessionId: session.id,
      date: workDate,
      gmId: gmUserId,
      gmName,
      region: user?.region ?? "",
      status,
      startAt: session.dayStartedAt,
      endAt: dayEndAt,
      startKm: session.startKm,
      endKm: session.endKm,
      actions,
      timezone,
    });

    res.status(200).json({
      items: payload.timeline.map(timelineSegmentToLegacyItem),
      timeline: payload.timeline,
      stats: payload.stats,
      session: {
        id: payload.id,
        date: payload.date,
        status: payload.status,
        startTime: payload.startTime,
        endTime: payload.endTime,
        startKm: payload.startKm,
        endKm: payload.endKm,
      },
    });
  } catch (error) {
    next(error);
  }
});

daySessionRouter.post("/start", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = startSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Tagesstart-Daten." });
      return;
    }
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const timezone = parsed.data.timezone ?? DEFAULT_TIMEZONE;
    const acceptedStartAt = resolveAcceptedStartAt(parsed.data.startedAt, now);
    const workDate = toYmdInTimezone(acceptedStartAt, timezone);
    const existing = await loadTodaySession(gmUserId, acceptedStartAt, timezone);
    if (existing) {
      if (existing.status === "submitted") {
        res.status(409).json({ error: "Der Arbeitstag wurde bereits abgeschlossen.", code: "day_already_submitted" });
        return;
      }
      if (existing.status === "started" || existing.status === "ended") {
        res.status(200).json({ session: serializeDaySession(existing) });
        return;
      }
      const [restarted] = await db
        .update(gmDaySessions)
        .set({
          status: "started",
          dayStartedAt: existing.dayStartedAt ?? now,
          updatedAt: now,
        })
        .where(eq(gmDaySessions.id, existing.id))
        .returning();
      if (!restarted) throw new Error("Arbeitstag konnte nicht gestartet werden.");
      res.status(200).json({ session: serializeDaySession(restarted) });
      return;
    }

    const openSession = await loadAnyOpenSessionForUser(gmUserId);
    if (openSession) {
      res.status(200).json({ session: serializeDaySession(openSession) });
      return;
    }

    const [created] = await db
      .insert(gmDaySessions)
      .values({
        gmUserId,
        workDate,
        timezone,
        status: "started",
        dayStartedAt: acceptedStartAt,
      })
      .returning();
    if (!created) throw new Error("Arbeitstag konnte nicht gestartet werden.");
    res.status(200).json({ session: serializeDaySession(created) });
  } catch (error) {
    next(error);
  }
});

daySessionRouter.patch("/start-km", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = setKmSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltiger Start-KM-Wert." });
      return;
    }
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const session = await loadCurrentWritableSession(gmUserId, now, DEFAULT_TIMEZONE);
    if (!session || (session.status !== "started" && session.status !== "ended")) {
      res.status(409).json({ error: "Arbeitstag wurde noch nicht gestartet.", code: "day_not_started" });
      return;
    }
    const [updated] = await db
      .update(gmDaySessions)
      .set({
        startKm: parsed.data.km,
        isStartKmCompleted: true,
        startKmDeferred: false,
        updatedAt: now,
      })
      .where(eq(gmDaySessions.id, session.id))
      .returning();
    if (!updated) throw new Error("Start-KM konnte nicht gespeichert werden.");
    res.status(200).json({ session: serializeDaySession(updated) });
  } catch (error) {
    next(error);
  }
});

daySessionRouter.patch("/start-km/defer", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const session = await loadCurrentWritableSession(gmUserId, now, DEFAULT_TIMEZONE);
    if (!session || (session.status !== "started" && session.status !== "ended")) {
      res.status(409).json({ error: "Arbeitstag wurde noch nicht gestartet.", code: "day_not_started" });
      return;
    }
    const [updated] = await db
      .update(gmDaySessions)
      .set({
        startKm: null,
        startKmDeferred: true,
        isStartKmCompleted: false,
        updatedAt: now,
      })
      .where(eq(gmDaySessions.id, session.id))
      .returning();
    if (!updated) throw new Error("Start-KM konnte nicht auf spaeter gesetzt werden.");
    res.status(200).json({ session: serializeDaySession(updated) });
  } catch (error) {
    next(error);
  }
});

daySessionRouter.patch("/end", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = setEndSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Tagesende-Daten." });
      return;
    }
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const session = await loadCurrentWritableSession(gmUserId, now, DEFAULT_TIMEZONE);
    if (!session || (session.status !== "started" && session.status !== "ended")) {
      res.status(409).json({ error: "Arbeitstag wurde noch nicht gestartet.", code: "day_not_started" });
      return;
    }
    const sessionTimezone = session.timezone || DEFAULT_TIMEZONE;
    const currentWorkDate = toYmdInTimezone(now, sessionTimezone);
    if (session.workDate < currentWorkDate && !parsed.data.endAt) {
      res.status(400).json({
        error: "Bitte eine Endzeit fuer den offenen Arbeitstag erfassen.",
        code: "stale_day_end_time_required",
      });
      return;
    }
    const endAt = parsed.data.endAt ? new Date(parsed.data.endAt) : now;
    if (toYmdInTimezone(endAt, sessionTimezone) !== session.workDate) {
      res.status(400).json({
        error: "Tagesende muss am selben Kalendertag wie der Tagesstart liegen.",
        code: "day_end_wrong_work_date",
      });
      return;
    }
    if (session.dayStartedAt && endAt.getTime() < session.dayStartedAt.getTime()) {
      res.status(400).json({ error: "Tagesende darf nicht vor Tagesstart liegen." });
      return;
    }
    const [updated] = await db
      .update(gmDaySessions)
      .set({
        status: "ended",
        dayEndedAt: endAt,
        updatedAt: now,
      })
      .where(eq(gmDaySessions.id, session.id))
      .returning();
    if (!updated) throw new Error("Tagesende konnte nicht gespeichert werden.");
    res.status(200).json({ session: serializeDaySession(updated) });
  } catch (error) {
    next(error);
  }
});

daySessionRouter.patch("/end-km", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = setKmSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltiger End-KM-Wert." });
      return;
    }
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const session = await loadCurrentWritableSession(gmUserId, now, DEFAULT_TIMEZONE);
    if (!session || (session.status !== "started" && session.status !== "ended")) {
      res.status(409).json({ error: "Der Arbeitstag wurde noch nicht gestartet.", code: "day_not_started" });
      return;
    }
    if (session.startKm != null && parsed.data.km < session.startKm) {
      res.status(400).json({ error: "End-KM darf nicht kleiner als Start-KM sein." });
      return;
    }
    const [updated] = await db
      .update(gmDaySessions)
      .set({
        endKm: parsed.data.km,
        isEndKmCompleted: true,
        endKmDeferred: false,
        updatedAt: now,
      })
      .where(eq(gmDaySessions.id, session.id))
      .returning();
    if (!updated) throw new Error("End-KM konnte nicht gespeichert werden.");
    res.status(200).json({ session: serializeDaySession(updated) });
  } catch (error) {
    next(error);
  }
});

daySessionRouter.patch("/end-km/defer", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const session = await loadCurrentWritableSession(gmUserId, now, DEFAULT_TIMEZONE);
    if (!session || (session.status !== "started" && session.status !== "ended")) {
      res.status(409).json({ error: "Der Arbeitstag wurde noch nicht gestartet.", code: "day_not_started" });
      return;
    }
    const [updated] = await db
      .update(gmDaySessions)
      .set({
        endKm: null,
        endKmDeferred: true,
        isEndKmCompleted: false,
        updatedAt: now,
      })
      .where(eq(gmDaySessions.id, session.id))
      .returning();
    if (!updated) throw new Error("End-KM konnte nicht auf spaeter gesetzt werden.");
    res.status(200).json({ session: serializeDaySession(updated) });
  } catch (error) {
    next(error);
  }
});

daySessionRouter.post("/submit", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = submitSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Speicherdaten fuer den Arbeitstag." });
      return;
    }
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const session = await loadCurrentWritableSession(gmUserId, now, DEFAULT_TIMEZONE);
    if (!session || session.status !== "ended") {
      res.status(409).json({ error: "Der Arbeitstag muss zuerst beendet werden.", code: "day_not_ended" });
      return;
    }
    if (!session.dayStartedAt || !session.dayEndedAt) {
      res.status(400).json({ error: "Tagesstart und Tagesende muessen gesetzt sein." });
      return;
    }
    if (!session.isStartKmCompleted || !session.isEndKmCompleted) {
      res.status(400).json({ error: "Start-KM und End-KM muessen gesetzt sein.", code: "km_required" });
      return;
    }
    const [updated] = await db
      .update(gmDaySessions)
      .set({
        status: "submitted",
        submittedAt: now,
        comment: parsed.data.comment?.trim() || null,
        updatedAt: now,
      })
      .where(eq(gmDaySessions.id, session.id))
      .returning();
    if (!updated) throw new Error("Arbeitstag konnte nicht gespeichert werden.");
    res.status(200).json({ session: serializeDaySession(updated) });
  } catch (error) {
    next(error);
  }
});

daySessionRouter.patch("/cancel", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const session = await loadCurrentWritableSession(gmUserId, now, DEFAULT_TIMEZONE);
    if (!session) {
      res.status(404).json({ error: "Kein offener Arbeitstag gefunden." });
      return;
    }
    if (session.status === "submitted") {
      res.status(409).json({ error: "Ein gespeicherter Arbeitstag kann nicht abgebrochen werden." });
      return;
    }
    const [updated] = await db
      .update(gmDaySessions)
      .set({
        status: "cancelled",
        cancelledAt: now,
        isDeleted: true,
        deletedAt: now,
        updatedAt: now,
      })
      .where(and(eq(gmDaySessions.id, session.id), eq(gmDaySessions.gmUserId, gmUserId)))
      .returning();
    if (!updated) throw new Error("Arbeitstag konnte nicht abgebrochen werden.");
    res.status(200).json({ session: serializeDaySession(updated) });
  } catch (error) {
    next(error);
  }
});

export { daySessionRouter };
