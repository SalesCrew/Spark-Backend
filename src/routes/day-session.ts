import { and, desc, eq, isNotNull, sql } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { db } from "../lib/db.js";
import { DEFAULT_TIMEZONE, getCurrentDaySessionForUser, toYmdInTimezone } from "../lib/day-session.js";
import { gmDaySessionPauses, gmDaySessions, markets, timeTrackingEntries, visitSessions } from "../lib/schema.js";

const daySessionRouter = Router();
daySessionRouter.use(requireAuth(["gm", "admin"]));

const timezoneSchema = z.string().trim().min(1).max(120);
const kmSchema = z.number().int().min(0).max(2_000_000);
const isoDateTimeSchema = z.string().datetime();

const startSchema = z
  .object({
    timezone: timezoneSchema.optional(),
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

const submitSchema = z
  .object({
    comment: z.string().trim().max(2_000).optional(),
  })
  .strict();

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

function toHm(iso: string): string {
  return new Intl.DateTimeFormat("de-AT", {
    timeZone: DEFAULT_TIMEZONE,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(new Date(iso));
}

function formatActivityLabel(activityType: string): string {
  const byType: Record<string, string> = {
    sonderaufgabe: "Sonderaufgabe",
    arztbesuch: "Arztbesuch",
    werkstatt: "Werkstatt",
    homeoffice: "Homeoffice",
    schulung: "Schulung",
    lager: "Lager",
    heimfahrt: "Heimfahrt",
    hotel: "Hotel",
    hoteluebernachtung: "Hoteluebernachtung",
  };
  return byType[activityType] ?? activityType;
}

async function loadTodaySession(gmUserId: string, now: Date, timezone: string) {
  return getCurrentDaySessionForUser({ gmUserId, now, timezone });
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

daySessionRouter.get("/current", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const session = await loadTodaySession(gmUserId, now, DEFAULT_TIMEZONE);
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

daySessionRouter.get("/today-submissions", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const now = new Date();
    const timezone = DEFAULT_TIMEZONE;
    const todayYmd = toYmdInTimezone(now, timezone);
    const dayStartExpr = sql`(${todayYmd}::timestamp at time zone ${timezone})`;
    const dayEndExpr = sql`((${todayYmd}::timestamp at time zone ${timezone}) + interval '1 day')`;

    const [dayRows, visitRows, zusatzRows, pauseRows] = await Promise.all([
      db
        .select({
          id: gmDaySessions.id,
          submittedAt: gmDaySessions.submittedAt,
        })
        .from(gmDaySessions)
        .where(
          and(
            eq(gmDaySessions.gmUserId, gmUserId),
            eq(gmDaySessions.isDeleted, false),
            eq(gmDaySessions.status, "submitted"),
            eq(gmDaySessions.workDate, todayYmd),
            isNotNull(gmDaySessions.submittedAt),
          ),
        )
        .orderBy(desc(gmDaySessions.submittedAt)),
      db
        .select({
          id: visitSessions.id,
          submittedAt: visitSessions.submittedAt,
          marketName: markets.name,
        })
        .from(visitSessions)
        .leftJoin(markets, eq(markets.id, visitSessions.marketId))
        .where(
          and(
            eq(visitSessions.gmUserId, gmUserId),
            eq(visitSessions.isDeleted, false),
            eq(visitSessions.status, "submitted"),
            isNotNull(visitSessions.submittedAt),
            sql`${visitSessions.submittedAt} >= ${dayStartExpr}`,
            sql`${visitSessions.submittedAt} < ${dayEndExpr}`,
          ),
        )
        .orderBy(desc(visitSessions.submittedAt)),
      db
        .select({
          id: timeTrackingEntries.id,
          submittedAt: timeTrackingEntries.submittedAt,
          activityType: timeTrackingEntries.activityType,
          marketName: markets.name,
        })
        .from(timeTrackingEntries)
        .leftJoin(markets, eq(markets.id, timeTrackingEntries.marketId))
        .where(
          and(
            eq(timeTrackingEntries.gmUserId, gmUserId),
            eq(timeTrackingEntries.isDeleted, false),
            eq(timeTrackingEntries.status, "submitted"),
            isNotNull(timeTrackingEntries.submittedAt),
            sql`${timeTrackingEntries.submittedAt} >= ${dayStartExpr}`,
            sql`${timeTrackingEntries.submittedAt} < ${dayEndExpr}`,
          ),
        )
        .orderBy(desc(timeTrackingEntries.submittedAt)),
      db
        .select({
          id: gmDaySessionPauses.id,
          pauseStartedAt: gmDaySessionPauses.pauseStartedAt,
          pauseEndedAt: gmDaySessionPauses.pauseEndedAt,
        })
        .from(gmDaySessionPauses)
        .innerJoin(gmDaySessions, eq(gmDaySessions.id, gmDaySessionPauses.daySessionId))
        .where(
          and(
            eq(gmDaySessionPauses.gmUserId, gmUserId),
            eq(gmDaySessionPauses.isDeleted, false),
            eq(gmDaySessions.isDeleted, false),
            eq(gmDaySessions.workDate, todayYmd),
          ),
        )
        .orderBy(desc(gmDaySessionPauses.pauseStartedAt)),
    ]);

    const items: SubmissionItem[] = [];
    for (const row of dayRows) {
      if (!row.submittedAt) continue;
      const submittedIso = row.submittedAt.toISOString();
      items.push({
        id: row.id,
        kind: "day",
        submittedAt: submittedIso,
        label: "Tag gespeichert",
        timeText: toHm(submittedIso),
      });
    }
    for (const row of visitRows) {
      if (!row.submittedAt) continue;
      const submittedIso = row.submittedAt.toISOString();
      items.push({
        id: row.id,
        kind: "markt",
        submittedAt: submittedIso,
        label: row.marketName?.trim() || "Marktbesuch",
        timeText: toHm(submittedIso),
      });
    }
    for (const row of zusatzRows) {
      if (!row.submittedAt) continue;
      const submittedIso = row.submittedAt.toISOString();
      const baseLabel = formatActivityLabel(row.activityType);
      items.push({
        id: row.id,
        kind: "zusatz",
        submittedAt: submittedIso,
        label: row.marketName?.trim() ? `${baseLabel} - ${row.marketName}` : baseLabel,
        timeText: toHm(submittedIso),
      });
    }
    for (const row of pauseRows) {
      const effectiveEnd = row.pauseEndedAt ?? now;
      const endIso = effectiveEnd.toISOString();
      const startIso = row.pauseStartedAt.toISOString();
      items.push({
        id: row.id,
        kind: "pause",
        submittedAt: endIso,
        label: "Pause",
        timeText: `${toHm(startIso)} - ${toHm(endIso)}`,
      });
    }

    items.sort((a, b) => b.submittedAt.localeCompare(a.submittedAt));
    res.status(200).json({ items });
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
    const workDate = toYmdInTimezone(now, timezone);
    const existing = await loadTodaySession(gmUserId, now, timezone);
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
      if (openSession.workDate === workDate) {
        res.status(200).json({ session: serializeDaySession(openSession) });
        return;
      }
      await db
        .update(gmDaySessions)
        .set({
          status: "cancelled",
          cancelledAt: now,
          isDeleted: true,
          deletedAt: now,
          updatedAt: now,
        })
        .where(eq(gmDaySessions.id, openSession.id));
    }

    const [created] = await db
      .insert(gmDaySessions)
      .values({
        gmUserId,
        workDate,
        timezone,
        status: "started",
        dayStartedAt: now,
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
    const session = await loadTodaySession(gmUserId, now, DEFAULT_TIMEZONE);
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
    const session = await loadTodaySession(gmUserId, now, DEFAULT_TIMEZONE);
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
    const session = await loadTodaySession(gmUserId, now, DEFAULT_TIMEZONE);
    if (!session || (session.status !== "started" && session.status !== "ended")) {
      res.status(409).json({ error: "Arbeitstag wurde noch nicht gestartet.", code: "day_not_started" });
      return;
    }
    const endAt = parsed.data.endAt ? new Date(parsed.data.endAt) : now;
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
    const session = await loadTodaySession(gmUserId, now, DEFAULT_TIMEZONE);
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
    const session = await loadTodaySession(gmUserId, now, DEFAULT_TIMEZONE);
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
    const session = await loadTodaySession(gmUserId, now, DEFAULT_TIMEZONE);
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
    const session = await loadTodaySession(gmUserId, now, DEFAULT_TIMEZONE);
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
