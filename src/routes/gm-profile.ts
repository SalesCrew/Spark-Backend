import { and, desc, eq, gte, isNotNull, lt, sql } from "drizzle-orm";
import { Router } from "express";
import { ensureAndGetGmKpiCache } from "../lib/gm-kpi-cache.js";
import { redMonthPeriodToYmd, resolveCurrentRedPeriod } from "../lib/red-month-periods.js";
import { loadZeiterfassungDaySessions } from "../lib/zeiterfassung-days.js";
import { db } from "../lib/db.js";
import { markets, users, visitSessions } from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const gmProfileRouter = Router();

const VIENNA_TIMEZONE = "Europe/Vienna";

function ymdInTimezone(date: Date, timezone = VIENNA_TIMEZONE): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);
  const year = parts.find((part) => part.type === "year")?.value ?? "1970";
  const month = parts.find((part) => part.type === "month")?.value ?? "01";
  const day = parts.find((part) => part.type === "day")?.value ?? "01";
  return `${year}-${month}-${day}`;
}

function startOfWeekYmd(todayYmd: string): string {
  const date = new Date(`${todayYmd}T12:00:00.000Z`);
  const day = (date.getUTCDay() + 6) % 7;
  date.setUTCDate(date.getUTCDate() - day);
  return date.toISOString().slice(0, 10);
}

function addOneDay(date: Date): Date {
  const copy = new Date(date);
  copy.setUTCDate(copy.getUTCDate() + 1);
  return copy;
}

gmProfileRouter.use(requireAuth(["gm"]));

gmProfileRouter.get("/profile", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }

    const now = new Date();
    const todayYmd = ymdInTimezone(now);
    const weekStartYmd = startOfWeekYmd(todayYmd);
    const redPeriod = await resolveCurrentRedPeriod(now);
    const redPeriodDates = redMonthPeriodToYmd(redPeriod);
    const redPeriodEndExclusive = addOneDay(redPeriod.end);

    const [
      userRows,
      currentRedVisitRows,
      allVisitRows,
      latestVisitRows,
      kpiSummary,
      weekSessions,
    ] = await Promise.all([
      db.select().from(users).where(eq(users.id, gmUserId)).limit(1),
      db
        .select({ count: sql<number>`count(*)::int` })
        .from(visitSessions)
        .where(
          and(
            eq(visitSessions.gmUserId, gmUserId),
            eq(visitSessions.status, "submitted"),
            eq(visitSessions.isDeleted, false),
            isNotNull(visitSessions.submittedAt),
            gte(visitSessions.submittedAt, redPeriod.start),
            lt(visitSessions.submittedAt, redPeriodEndExclusive),
          ),
        ),
      db
        .select({ count: sql<number>`count(*)::int` })
        .from(visitSessions)
        .where(
          and(
            eq(visitSessions.gmUserId, gmUserId),
            eq(visitSessions.status, "submitted"),
            eq(visitSessions.isDeleted, false),
            isNotNull(visitSessions.submittedAt),
          ),
        ),
      db
        .select({
          id: visitSessions.id,
          startedAt: visitSessions.startedAt,
          submittedAt: visitSessions.submittedAt,
          marketName: markets.name,
          marketAddress: markets.address,
          marketPostalCode: markets.postalCode,
          marketCity: markets.city,
        })
        .from(visitSessions)
        .leftJoin(markets, eq(markets.id, visitSessions.marketId))
        .where(
          and(
            eq(visitSessions.gmUserId, gmUserId),
            eq(visitSessions.status, "submitted"),
            eq(visitSessions.isDeleted, false),
            isNotNull(visitSessions.submittedAt),
          ),
        )
        .orderBy(desc(visitSessions.submittedAt))
        .limit(1),
      ensureAndGetGmKpiCache(gmUserId),
      loadZeiterfassungDaySessions({
        from: weekStartYmd,
        to: todayYmd,
        gmUserIds: [gmUserId],
        includeLive: true,
        timezone: VIENNA_TIMEZONE,
      }),
    ]);

    const user = userRows[0];
    if (!user || !user.isActive || user.deletedAt || user.role !== "gm") {
      res.status(404).json({ error: "GM Profil wurde nicht gefunden." });
      return;
    }

    const weekWorkMinutes = weekSessions.reduce((sum, session) => sum + Math.max(0, session.stats.reineArbeitszeit), 0);
    const finishedWeekSessions = weekSessions.filter((session) => session.status === "ended" || session.status === "submitted");
    const averageWorkdayMin = finishedWeekSessions.length > 0
      ? Math.round(finishedWeekSessions.reduce((sum, session) => sum + Math.max(0, session.stats.reineArbeitszeit), 0) / finishedWeekSessions.length)
      : 0;
    const latestVisit = latestVisitRows[0] ?? null;

    res.status(200).json({
      profile: {
        id: user.id,
        firstName: user.firstName,
        lastName: user.lastName,
        email: user.email,
        phone: user.phone ?? "",
        address: user.address ?? "",
        city: user.city ?? "",
        postalCode: user.postalCode ?? "",
        region: user.region ?? "",
        isBillaGm: user.isBillaGm,
        createdAt: user.createdAt.toISOString(),
        updatedAt: user.updatedAt.toISOString(),
      },
      stats: {
        redPeriod: {
          redPeriodId: redPeriod.redPeriodId,
          redMonthYearId: redPeriod.redMonthYearId,
          label: redPeriod.label,
          startDate: redPeriodDates.startYmd,
          endDate: redPeriodDates.endYmd,
          redYear: redPeriod.redYear,
        },
        currentRedVisitCount: Number(currentRedVisitRows[0]?.count ?? 0),
        allTimeVisitCount: Number(allVisitRows[0]?.count ?? 0),
        latestVisit: latestVisit
          ? {
              id: latestVisit.id,
              startedAt: latestVisit.startedAt?.toISOString() ?? null,
              submittedAt: latestVisit.submittedAt?.toISOString() ?? null,
              marketName: latestVisit.marketName ?? "",
              marketAddress: [
                latestVisit.marketAddress,
                [latestVisit.marketPostalCode, latestVisit.marketCity].filter(Boolean).join(" "),
              ].filter(Boolean).join(", "),
            }
          : null,
        ippAllTimeAvg: kpiSummary.ippAllTimeAvg,
        ippSampleCount: kpiSummary.ippSampleCount,
        bonusCumulativeEur: kpiSummary.bonusCumulativeEur,
        weekWorkMinutes,
        averageWorkdayMin,
        trackedWeekDays: weekSessions.length,
      },
    });
  } catch (error) {
    next(error);
  }
});

export { gmProfileRouter };
