import { and, desc, eq, inArray, isNull } from "drizzle-orm";
import { db, sql } from "./db.js";
import { gmDaySessionPauses, gmDaySessions, visitSessions } from "./schema.js";

const DEFAULT_TIMEZONE = "Europe/Vienna";
let checkedDaySessionTable = false;
let hasDaySessionTable = false;

function toYmdInTimezone(date: Date, timezone: string): string {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return formatter.format(date);
}

async function getCurrentDaySessionForUser(input: {
  gmUserId: string;
  now?: Date;
  timezone?: string;
}) {
  if (!(await ensureDaySessionTableReady())) return null;
  const now = input.now ?? new Date();
  const timezone = input.timezone ?? DEFAULT_TIMEZONE;
  const todayYmd = toYmdInTimezone(now, timezone);
  const [row] = await db
    .select()
    .from(gmDaySessions)
    .where(
      and(
        eq(gmDaySessions.gmUserId, input.gmUserId),
        eq(gmDaySessions.workDate, todayYmd),
        eq(gmDaySessions.isDeleted, false),
      ),
    )
    .limit(1);
  return row;
}

async function getCurrentWritableDaySessionForUser(input: {
  gmUserId: string;
  now?: Date;
  timezone?: string;
}) {
  if (!(await ensureDaySessionTableReady())) return null;
  const todaySession = await getCurrentDaySessionForUser(input);
  if (todaySession) return todaySession;

  const [openSession] = await db
    .select()
    .from(gmDaySessions)
    .where(
      and(
        eq(gmDaySessions.gmUserId, input.gmUserId),
        eq(gmDaySessions.isDeleted, false),
        inArray(gmDaySessions.status, ["started", "ended"]),
      ),
    )
    .orderBy(desc(gmDaySessions.dayStartedAt), desc(gmDaySessions.updatedAt))
    .limit(1);
  return openSession ?? null;
}

async function ensureDaySessionTableReady(): Promise<boolean> {
  if (checkedDaySessionTable) return hasDaySessionTable;
  const result = await sql<{ gm_day_sessions: string | null }[]>`
    select to_regclass('public.gm_day_sessions') as gm_day_sessions
  `;
  hasDaySessionTable = Boolean(result[0]?.gm_day_sessions);
  checkedDaySessionTable = true;
  return hasDaySessionTable;
}

async function ensureStartedDaySession(input: {
  gmUserId: string;
  now?: Date;
  timezone?: string;
}): Promise<
  | { ok: true; session: typeof gmDaySessions.$inferSelect | null; skipped: boolean }
  | { ok: false; reason: "day_not_started" | "stale_day_open" }
> {
  if (!(await ensureDaySessionTableReady())) {
    return { ok: true, session: null, skipped: true };
  }
  const now = input.now ?? new Date();
  const timezone = input.timezone ?? DEFAULT_TIMEZONE;
  const session = await getCurrentWritableDaySessionForUser({ ...input, now, timezone });
  if (!session) return { ok: false, reason: "day_not_started" };
  if (!inArrayValue(session.status, ["started", "ended"])) return { ok: false, reason: "day_not_started" };
  if (!session.dayStartedAt) return { ok: false, reason: "day_not_started" };
  if (session.workDate < toYmdInTimezone(now, session.timezone || timezone)) {
    return { ok: false, reason: "stale_day_open" };
  }
  return { ok: true, session, skipped: false };
}

type GmSubmissionGateReason =
  | "day_not_started"
  | "stale_day_open"
  | "pause_active"
  | "active_visit_open";

type GmSubmissionGateResult =
  | {
      ok: true;
      session: typeof gmDaySessions.$inferSelect | null;
      skipped: boolean;
    }
  | {
      ok: false;
      reason: GmSubmissionGateReason;
      activeVisitSessionId?: string;
    };

async function ensureGmSubmissionGate(input: {
  gmUserId: string;
  now?: Date;
  timezone?: string;
  blockActiveVisit?: boolean;
}): Promise<GmSubmissionGateResult> {
  const dayGuard = await ensureStartedDaySession(input);
  if (!dayGuard.ok) return dayGuard;
  if (dayGuard.skipped) return dayGuard;
  if (!dayGuard.session || dayGuard.session.status !== "started") {
    return { ok: false, reason: "day_not_started" };
  }

  const [openPause] = await db
    .select({ id: gmDaySessionPauses.id })
    .from(gmDaySessionPauses)
    .where(
      and(
        eq(gmDaySessionPauses.gmUserId, input.gmUserId),
        eq(gmDaySessionPauses.daySessionId, dayGuard.session.id),
        eq(gmDaySessionPauses.isDeleted, false),
        isNull(gmDaySessionPauses.pauseEndedAt),
      ),
    )
    .limit(1);
  if (openPause) return { ok: false, reason: "pause_active" };

  if (input.blockActiveVisit) {
    const [activeVisit] = await db
      .select({ id: visitSessions.id })
      .from(visitSessions)
      .where(
        and(
          eq(visitSessions.gmUserId, input.gmUserId),
          eq(visitSessions.status, "draft"),
          eq(visitSessions.isDeleted, false),
          isNull(visitSessions.submittedAt),
        ),
      )
      .orderBy(desc(visitSessions.startedAt), desc(visitSessions.updatedAt))
      .limit(1);
    if (activeVisit) {
      return {
        ok: false,
        reason: "active_visit_open",
        activeVisitSessionId: activeVisit.id,
      };
    }
  }

  return dayGuard;
}

function gmSubmissionGateError(reason: GmSubmissionGateReason): { error: string; code: GmSubmissionGateReason } {
  switch (reason) {
    case "stale_day_open":
      return { error: "Bitte zuerst den offenen Arbeitstag vom Vortag abschließen.", code: reason };
    case "pause_active":
      return { error: "Bitte zuerst die aktive Pause beenden.", code: reason };
    case "active_visit_open":
      return { error: "Bitte zuerst den laufenden Fragebogen abschließen.", code: reason };
    case "day_not_started":
    default:
      return { error: "Bitte zuerst den Arbeitstag starten.", code: "day_not_started" };
  }
}

function inArrayValue<T extends string>(value: T, values: readonly T[]): boolean {
  return values.includes(value);
}

export {
  DEFAULT_TIMEZONE,
  getCurrentDaySessionForUser,
  getCurrentWritableDaySessionForUser,
  ensureDaySessionTableReady,
  ensureGmSubmissionGate,
  ensureStartedDaySession,
  gmSubmissionGateError,
  toYmdInTimezone,
};
export type { GmSubmissionGateReason, GmSubmissionGateResult };
