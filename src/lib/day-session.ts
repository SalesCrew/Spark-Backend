import { and, desc, eq, inArray } from "drizzle-orm";
import { db, sql } from "./db.js";
import { gmDaySessions } from "./schema.js";

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

function inArrayValue<T extends string>(value: T, values: readonly T[]): boolean {
  return values.includes(value);
}

export {
  DEFAULT_TIMEZONE,
  getCurrentDaySessionForUser,
  getCurrentWritableDaySessionForUser,
  ensureDaySessionTableReady,
  ensureStartedDaySession,
  toYmdInTimezone,
};
