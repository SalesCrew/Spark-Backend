import { and, desc, eq, isNotNull, isNull, ne, or, sql } from "drizzle-orm";
import { db } from "./db.js";
import { getCurrentWritableDaySessionForUser } from "./day-session.js";
import { EMPLOYEE_AGREEMENT_KEY, EMPLOYEE_AGREEMENT_VERSION } from "./employee-agreement-meta.js";
import { readGmKpiCache } from "./gm-kpi-cache.js";
import { redMonthPeriodToYmd, resolveCurrentRedPeriod } from "./red-month-periods.js";
import {
  campaignMarketAssignments,
  campaigns,
  employeeAgreementAcceptances,
  gmDaySessionPauses,
  gmTextSettings,
  markets,
  timeEntryChangeRequests,
  timeTrackingEntries,
  users,
  visitAnswerChangeRequests,
  visitSessionDeleteRequests,
  visitSessionSections,
  visitSessions,
} from "./schema.js";
import { loadZeiterfassungDaySessions } from "./zeiterfassung-days.js";

const VIENNA_TIMEZONE = "Europe/Vienna";

function toYmdInVienna(date: Date): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: VIENNA_TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function startOfWeekYmd(todayYmd: string): string {
  const date = new Date(`${todayYmd}T12:00:00.000Z`);
  const weekdayFromMonday = (date.getUTCDay() + 6) % 7;
  date.setUTCDate(date.getUTCDate() - weekdayFromMonday);
  return date.toISOString().slice(0, 10);
}

function formatDateTime(value: Date | null): string | null {
  if (!value) return null;
  return new Intl.DateTimeFormat("de-AT", {
    timeZone: VIENNA_TIMEZONE,
    weekday: "short",
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(value);
}

function cleanText(value: unknown, maxLength = 180): string | null {
  if (typeof value !== "string") return null;
  const cleaned = value.replace(/[\u0000-\u001f\u007f]/g, " ").replace(/\s+/g, " ").trim();
  if (!cleaned) return null;
  return cleaned.slice(0, maxLength);
}

function minutesLabel(value: number): string {
  const minutes = Math.max(0, Math.round(value));
  const hours = Math.floor(minutes / 60);
  const rest = minutes % 60;
  if (hours === 0) return `${rest} Min.`;
  if (rest === 0) return `${hours} Std.`;
  return `${hours} Std. ${rest} Min.`;
}

function groupVisitRows(rows: Array<{
  sessionId: string;
  marketName: string;
  marketAddress: string;
  marketPostalCode: string;
  marketCity: string;
  startedAt: Date;
  submittedAt: Date | null;
  campaignName: string | null;
  section: string | null;
}>): Array<Record<string, unknown>> {
  const grouped = new Map<string, Record<string, unknown> & { campaigns: Set<string>; sections: Set<string> }>();
  for (const row of rows) {
    const current = grouped.get(row.sessionId) ?? {
      market: cleanText(row.marketName) ?? "Unbekannter Markt",
      address: [cleanText(row.marketAddress), cleanText(row.marketPostalCode), cleanText(row.marketCity)].filter(Boolean).join(", "),
      startedAt: formatDateTime(row.startedAt),
      submittedAt: formatDateTime(row.submittedAt),
      campaigns: new Set<string>(),
      sections: new Set<string>(),
    };
    const campaignName = cleanText(row.campaignName);
    const section = cleanText(row.section);
    if (campaignName) current.campaigns.add(campaignName);
    if (section) current.sections.add(section);
    grouped.set(row.sessionId, current);
  }

  return Array.from(grouped.values()).map((row) => ({
    market: row.market,
    address: row.address,
    startedAt: row.startedAt,
    submittedAt: row.submittedAt,
    campaigns: Array.from(row.campaigns),
    sections: Array.from(row.sections),
  }));
}

async function resolveCurrentRedPeriodSafely(now: Date) {
  try {
    return await resolveCurrentRedPeriod(now);
  } catch {
    return null;
  }
}

export async function buildKurtiGmContext(gmUserId: string, now = new Date()): Promise<Record<string, unknown>> {
  const todayYmd = toYmdInVienna(now);
  const weekStartYmd = startOfWeekYmd(todayYmd);
  const redPeriod = await resolveCurrentRedPeriodSafely(now);
  const redRange = redPeriod ? redMonthPeriodToYmd(redPeriod) : null;
  const liveCampaignCondition = sql<boolean>`(
    ${campaigns.status} = 'active'
    and (
      ${campaigns.scheduleType} = 'always'
      or (
        ${campaigns.scheduleType} = 'scheduled'
        and ${campaigns.startDate} is not null
        and ${campaigns.endDate} is not null
        and ${campaigns.startDate} <= current_date
        and ${campaigns.endDate} >= current_date
      )
    )
  )`;

  const [
    profileRows,
    dayRows,
    activeDaySession,
    activePauseRows,
    activeTimeDraftRows,
    activeVisitRows,
    recentVisitRows,
    weeklyVisitCountRows,
    redVisitRows,
    redVisitCountRows,
    allVisitCountRows,
    latestVisitRows,
    kpiSummary,
    agreementRows,
    textSettingsRows,
    assignmentRows,
    campaignProgressRows,
    globalFlexRows,
    pendingAnswerRows,
    pendingTimeRows,
    pendingDeleteRows,
  ] = await Promise.all([
    db
      .select({
        firstName: users.firstName,
        lastName: users.lastName,
        email: users.email,
        phone: users.phone,
        address: users.address,
        postalCode: users.postalCode,
        city: users.city,
        region: users.region,
        isBillaGm: users.isBillaGm,
      })
      .from(users)
      .where(and(eq(users.id, gmUserId), eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
      .limit(1),
    loadZeiterfassungDaySessions({
      from: weekStartYmd,
      to: todayYmd,
      gmUserIds: [gmUserId],
      includeLive: true,
      timezone: VIENNA_TIMEZONE,
    }),
    getCurrentWritableDaySessionForUser({ gmUserId, now, timezone: VIENNA_TIMEZONE }),
    db
      .select({
        startedAt: gmDaySessionPauses.pauseStartedAt,
        endedAt: gmDaySessionPauses.pauseEndedAt,
      })
      .from(gmDaySessionPauses)
      .where(
        and(
          eq(gmDaySessionPauses.gmUserId, gmUserId),
          eq(gmDaySessionPauses.isDeleted, false),
          isNull(gmDaySessionPauses.pauseEndedAt),
        ),
      )
      .limit(1),
    db
      .select({
        activityType: timeTrackingEntries.activityType,
        startAt: timeTrackingEntries.startAt,
        endAt: timeTrackingEntries.endAt,
      })
      .from(timeTrackingEntries)
      .where(
        and(
          eq(timeTrackingEntries.gmUserId, gmUserId),
          eq(timeTrackingEntries.status, "draft"),
          eq(timeTrackingEntries.isDeleted, false),
        ),
      )
      .orderBy(desc(timeTrackingEntries.updatedAt)),
    db
      .select({
        sessionId: visitSessions.id,
        marketName: markets.name,
        marketAddress: markets.address,
        marketPostalCode: markets.postalCode,
        marketCity: markets.city,
        startedAt: visitSessions.startedAt,
        submittedAt: visitSessions.submittedAt,
        campaignName: campaigns.name,
        section: visitSessionSections.section,
      })
      .from(visitSessions)
      .innerJoin(markets, eq(markets.id, visitSessions.marketId))
      .leftJoin(
        visitSessionSections,
        and(eq(visitSessionSections.visitSessionId, visitSessions.id), eq(visitSessionSections.isDeleted, false)),
      )
      .leftJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
      .where(
        and(
          eq(visitSessions.gmUserId, gmUserId),
          eq(visitSessions.status, "draft"),
          eq(visitSessions.isDeleted, false),
        ),
      )
      .orderBy(desc(visitSessions.startedAt)),
    db
      .select({
        sessionId: visitSessions.id,
        marketName: markets.name,
        marketAddress: markets.address,
        marketPostalCode: markets.postalCode,
        marketCity: markets.city,
        startedAt: visitSessions.startedAt,
        submittedAt: visitSessions.submittedAt,
        campaignName: campaigns.name,
        section: visitSessionSections.section,
      })
      .from(visitSessions)
      .innerJoin(markets, eq(markets.id, visitSessions.marketId))
      .leftJoin(
        visitSessionSections,
        and(eq(visitSessionSections.visitSessionId, visitSessions.id), eq(visitSessionSections.isDeleted, false)),
      )
      .leftJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
      .where(
        and(
          eq(visitSessions.gmUserId, gmUserId),
          eq(visitSessions.status, "submitted"),
          eq(visitSessions.isDeleted, false),
          isNotNull(visitSessions.submittedAt),
          sql`${visitSessions.submittedAt} >= (${weekStartYmd}::timestamp at time zone ${VIENNA_TIMEZONE})`,
        ),
      )
      .orderBy(desc(visitSessions.submittedAt))
      .limit(60),
    db
      .select({ count: sql<number>`count(*)::int` })
      .from(visitSessions)
      .where(
        and(
          eq(visitSessions.gmUserId, gmUserId),
          eq(visitSessions.status, "submitted"),
          eq(visitSessions.isDeleted, false),
          isNotNull(visitSessions.submittedAt),
          sql`${visitSessions.submittedAt} >= (${weekStartYmd}::timestamp at time zone ${VIENNA_TIMEZONE})`,
        ),
      ),
    redRange
      ? db
          .select({
            sessionId: visitSessions.id,
            marketName: markets.name,
            marketAddress: markets.address,
            marketPostalCode: markets.postalCode,
            marketCity: markets.city,
            startedAt: visitSessions.startedAt,
            submittedAt: visitSessions.submittedAt,
            campaignName: campaigns.name,
            section: visitSessionSections.section,
          })
          .from(visitSessions)
          .innerJoin(markets, eq(markets.id, visitSessions.marketId))
          .leftJoin(
            visitSessionSections,
            and(eq(visitSessionSections.visitSessionId, visitSessions.id), eq(visitSessionSections.isDeleted, false)),
          )
          .leftJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
          .where(
            and(
              eq(visitSessions.gmUserId, gmUserId),
              eq(visitSessions.status, "submitted"),
              eq(visitSessions.isDeleted, false),
              isNotNull(visitSessions.submittedAt),
              sql`${visitSessions.submittedAt} >= (${redRange.startYmd}::timestamp at time zone ${VIENNA_TIMEZONE})`,
              sql`${visitSessions.submittedAt} < ((${redRange.endYmd}::timestamp at time zone ${VIENNA_TIMEZONE}) + interval '1 day')`,
            ),
          )
          .orderBy(desc(visitSessions.submittedAt))
          .limit(100)
      : Promise.resolve([]),
    redRange
      ? db
          .select({ count: sql<number>`count(*)::int` })
          .from(visitSessions)
          .where(
            and(
              eq(visitSessions.gmUserId, gmUserId),
              eq(visitSessions.status, "submitted"),
              eq(visitSessions.isDeleted, false),
              isNotNull(visitSessions.submittedAt),
              sql`${visitSessions.submittedAt} >= (${redRange.startYmd}::timestamp at time zone ${VIENNA_TIMEZONE})`,
              sql`${visitSessions.submittedAt} < ((${redRange.endYmd}::timestamp at time zone ${VIENNA_TIMEZONE}) + interval '1 day')`,
            ),
          )
      : Promise.resolve([{ count: 0 }]),
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
        marketName: markets.name,
        marketAddress: markets.address,
        marketPostalCode: markets.postalCode,
        marketCity: markets.city,
        submittedAt: visitSessions.submittedAt,
      })
      .from(visitSessions)
      .innerJoin(markets, eq(markets.id, visitSessions.marketId))
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
    readGmKpiCache(gmUserId),
    db
      .select({
        acceptedAt: employeeAgreementAcceptances.acceptedAt,
        agreementVersion: employeeAgreementAcceptances.agreementVersion,
      })
      .from(employeeAgreementAcceptances)
      .where(
        and(
          eq(employeeAgreementAcceptances.userId, gmUserId),
          eq(employeeAgreementAcceptances.agreementKey, EMPLOYEE_AGREEMENT_KEY),
          eq(employeeAgreementAcceptances.agreementVersion, EMPLOYEE_AGREEMENT_VERSION),
        ),
      )
      .orderBy(desc(employeeAgreementAcceptances.acceptedAt))
      .limit(1),
    db
      .select({ textScalePercent: gmTextSettings.textScalePercent })
      .from(gmTextSettings)
      .where(and(eq(gmTextSettings.userId, gmUserId), eq(gmTextSettings.isDeleted, false)))
      .limit(1),
    db
      .select({
        assignmentId: campaignMarketAssignments.id,
        campaignId: campaigns.id,
        campaignName: campaigns.name,
        section: campaigns.section,
        marketId: markets.id,
        marketName: markets.name,
        marketAddress: markets.address,
        marketPostalCode: markets.postalCode,
        marketCity: markets.city,
        standardMarketNumber: markets.standardMarketNumber,
        flexNumber: markets.flexNumber,
        visitTargetCount: campaignMarketAssignments.visitTargetCount,
      })
      .from(campaignMarketAssignments)
      .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
      .innerJoin(markets, eq(markets.id, campaignMarketAssignments.marketId))
      .where(
        and(
          or(
            and(ne(campaigns.section, "flex"), eq(campaignMarketAssignments.gmUserId, gmUserId)),
            and(eq(campaigns.section, "flex"), eq(campaigns.assignedGmUserId, gmUserId)),
          ),
          eq(campaignMarketAssignments.isDeleted, false),
          eq(campaigns.isDeleted, false),
          eq(markets.isDeleted, false),
          eq(markets.isActive, true),
          liveCampaignCondition,
        ),
      )
      .orderBy(campaigns.name, markets.name),
    redRange
      ? db
          .select({
            campaignId: visitSessionSections.campaignId,
            marketId: visitSessions.marketId,
            submittedVisits: sql<number>`count(distinct ${visitSessions.id})::int`,
          })
          .from(visitSessionSections)
          .innerJoin(visitSessions, eq(visitSessions.id, visitSessionSections.visitSessionId))
          .where(
            and(
              eq(visitSessions.gmUserId, gmUserId),
              eq(visitSessions.status, "submitted"),
              eq(visitSessions.isDeleted, false),
              eq(visitSessionSections.isDeleted, false),
              isNotNull(visitSessions.submittedAt),
              sql`${visitSessions.submittedAt} >= (${redRange.startYmd}::timestamp at time zone ${VIENNA_TIMEZONE})`,
              sql`${visitSessions.submittedAt} < ((${redRange.endYmd}::timestamp at time zone ${VIENNA_TIMEZONE}) + interval '1 day')`,
            ),
          )
          .groupBy(visitSessionSections.campaignId, visitSessions.marketId)
      : Promise.resolve([]),
    db
      .select({ name: campaigns.name })
      .from(campaigns)
      .where(
        and(
          eq(campaigns.section, "flex"),
          or(isNull(campaigns.assignedGmUserId), eq(campaigns.assignedGmUserId, gmUserId)),
          eq(campaigns.isDeleted, false),
          liveCampaignCondition,
        ),
      )
      .orderBy(campaigns.name),
    db
      .select({ count: sql<number>`count(*)::int` })
      .from(visitAnswerChangeRequests)
      .where(
        and(
          eq(visitAnswerChangeRequests.gmUserId, gmUserId),
          eq(visitAnswerChangeRequests.status, "pending"),
          eq(visitAnswerChangeRequests.isDeleted, false),
        ),
      ),
    db
      .select({ count: sql<number>`count(*)::int` })
      .from(timeEntryChangeRequests)
      .where(
        and(
          eq(timeEntryChangeRequests.gmUserId, gmUserId),
          eq(timeEntryChangeRequests.status, "pending"),
          eq(timeEntryChangeRequests.isDeleted, false),
        ),
      ),
    db
      .select({ count: sql<number>`count(*)::int` })
      .from(visitSessionDeleteRequests)
      .where(
        and(
          eq(visitSessionDeleteRequests.gmUserId, gmUserId),
          eq(visitSessionDeleteRequests.status, "pending"),
          eq(visitSessionDeleteRequests.isDeleted, false),
        ),
      ),
  ]);

  const profile = profileRows[0];
  if (!profile) {
    const error = new Error("GM-Profil wurde nicht gefunden.") as Error & { statusCode: number; code: string };
    error.statusCode = 404;
    error.code = "gm_profile_not_found";
    throw error;
  }

  const progressByCampaignMarket = new Map(
    campaignProgressRows.map((row) => [`${row.campaignId}:${row.marketId}`, Number(row.submittedVisits ?? 0)]),
  );
  const assignmentByCampaignMarket = new Map<string, {
    campaignId: string;
    campaignName: string;
    section: string;
    marketId: string;
    marketName: string;
    marketAddress: string;
    marketPostalCode: string;
    marketCity: string;
    standardMarketNumber: string | null;
    flexNumber: string | null;
    targetVisits: number;
  }>();
  for (const row of assignmentRows) {
    const key = `${row.campaignId}:${row.marketId}`;
    const current = assignmentByCampaignMarket.get(key);
    if (current) {
      current.targetVisits += row.visitTargetCount;
      continue;
    }
    assignmentByCampaignMarket.set(key, {
      campaignId: row.campaignId,
      campaignName: row.campaignName,
      section: row.section,
      marketId: row.marketId,
      marketName: row.marketName,
      marketAddress: row.marketAddress,
      marketPostalCode: row.marketPostalCode,
      marketCity: row.marketCity,
      standardMarketNumber: row.standardMarketNumber,
      flexNumber: row.flexNumber,
      targetVisits: row.visitTargetCount,
    });
  }
  const campaignMap = new Map<string, {
    name: string;
    section: string;
    targetVisits: number;
    submittedVisits: number;
    marketKeys: Set<string>;
    openMarketCount: number;
    openMarkets: Array<Record<string, unknown>>;
  }>();
  for (const row of assignmentByCampaignMarket.values()) {
    const progress = progressByCampaignMarket.get(`${row.campaignId}:${row.marketId}`) ?? 0;
    const current = campaignMap.get(row.campaignId) ?? {
      name: cleanText(row.campaignName) ?? "Kampagne",
      section: row.section,
      targetVisits: 0,
      submittedVisits: 0,
      marketKeys: new Set<string>(),
      openMarketCount: 0,
      openMarkets: [],
    };
    current.targetVisits += row.targetVisits;
    current.submittedVisits += Math.min(progress, row.targetVisits);
    current.marketKeys.add(row.marketId);
    if (progress < row.targetVisits) {
      current.openMarketCount += 1;
      if (current.openMarkets.length < 30) {
        current.openMarkets.push({
          market: cleanText(row.marketName),
          address: [cleanText(row.marketAddress), cleanText(row.marketPostalCode), cleanText(row.marketCity)].filter(Boolean).join(", "),
          marketNumber: cleanText(row.standardMarketNumber) ?? cleanText(row.flexNumber),
          progress: `${progress}/${row.targetVisits}`,
        });
      }
    }
    campaignMap.set(row.campaignId, current);
  }

  const weeklyWorkMinutes = dayRows.reduce((sum, day) => sum + day.stats.reineArbeitszeit, 0);
  const weeklyKm = dayRows.reduce((sum, day) => sum + (day.stats.kmGefahren ?? 0), 0);
  const activeDay = activeDaySession ?? null;
  const activeVisits = groupVisitRows(activeVisitRows);
  const weeklyVisits = groupVisitRows(recentVisitRows);
  const currentRedVisits = groupVisitRows(redVisitRows);
  const latestVisit = latestVisitRows[0] ?? null;
  const finishedWeekDays = dayRows.filter((day) => day.status === "ended" || day.status === "submitted");
  const averageWorkdayMinutes = finishedWeekDays.length > 0
    ? Math.round(finishedWeekDays.reduce((sum, day) => sum + day.stats.reineArbeitszeit, 0) / finishedWeekDays.length)
    : 0;

  return {
    generatedAt: formatDateTime(now),
    gm: {
      firstName: cleanText(profile.firstName),
      fullName: [cleanText(profile.firstName), cleanText(profile.lastName)].filter(Boolean).join(" "),
      email: cleanText(profile.email),
      phone: cleanText(profile.phone),
      address: [cleanText(profile.address), cleanText(profile.postalCode), cleanText(profile.city)].filter(Boolean).join(", "),
      region: cleanText(profile.region),
      billaAssignment: profile.isBillaGm,
      textSizeIncreasePercent: textSettingsRows[0]?.textScalePercent ?? 0,
      allTimeSubmittedMarketVisits: Number(allVisitCountRows[0]?.count ?? 0),
      latestSubmittedVisit: latestVisit
        ? {
            market: cleanText(latestVisit.marketName),
            address: [cleanText(latestVisit.marketAddress), cleanText(latestVisit.marketPostalCode), cleanText(latestVisit.marketCity)].filter(Boolean).join(", "),
            submittedAt: formatDateTime(latestVisit.submittedAt),
          }
        : null,
      kpi: kpiSummary
        ? {
            ippAverage: kpiSummary.ippAllTimeAvg,
            ippSampleCount: kpiSummary.ippSampleCount,
            cumulativeBonusEuro: kpiSummary.bonusCumulativeEur,
            lastComputedAt: formatDateTime(new Date(kpiSummary.lastComputedAt)),
          }
        : null,
    },
    currentRedPeriod: redPeriod && redRange
      ? {
          label: redPeriod.label,
          start: redRange.startYmd,
          end: redRange.endYmd,
          submittedMarketVisits: Number(redVisitCountRows[0]?.count ?? 0),
          recentVisitsShown: currentRedVisits.slice(0, 25).length,
          recentVisits: currentRedVisits.slice(0, 25),
        }
      : null,
    workday: activeDay
      ? {
          workDate: activeDay.workDate,
          status: activeDay.status,
          startedAt: formatDateTime(activeDay.dayStartedAt),
          endedAt: formatDateTime(activeDay.dayEndedAt),
          startKm: activeDay.startKm,
          endKm: activeDay.endKm,
          pauseCurrentlyRunning: activePauseRows.length > 0,
        }
      : { status: "not_started" },
    currentDraftActivities: activeTimeDraftRows.map((row) => ({
      type: row.activityType,
      startedAt: formatDateTime(row.startAt),
      endedAt: formatDateTime(row.endAt),
    })),
    activeQuestionnaire: activeVisits[0] ?? null,
    activeQuestionnaires: activeVisits,
    week: {
      from: weekStartYmd,
      to: todayYmd,
      pureWorkTime: minutesLabel(weeklyWorkMinutes),
      pureWorkMinutes: weeklyWorkMinutes,
      drivenKm: weeklyKm,
      submittedMarketVisits: Number(weeklyVisitCountRows[0]?.count ?? 0),
      visitsShown: weeklyVisits.length,
      averageFinishedWorkday: minutesLabel(averageWorkdayMinutes),
      averageFinishedWorkdayMinutes: averageWorkdayMinutes,
      days: dayRows.map((day) => ({
        date: day.date,
        status: day.status,
        start: day.startTime,
        end: day.endTime,
        pureWorkTime: minutesLabel(day.stats.reineArbeitszeit),
        pauseTime: minutesLabel(day.stats.pauseMin),
        marketVisits: day.stats.marktbesuche,
        additionalEntries: day.stats.zusatz,
        drivenKm: day.stats.kmGefahren,
      })),
      visits: weeklyVisits,
    },
    activeCampaigns: Array.from(campaignMap.values()).map((campaign) => ({
      name: campaign.name,
      section: campaign.section,
      assignedMarkets: campaign.marketKeys.size,
      progress: `${campaign.submittedVisits}/${campaign.targetVisits}`,
      openMarketCount: campaign.openMarketCount,
      openMarketsShown: campaign.openMarkets.length,
      openMarkets: campaign.openMarkets,
    })),
    activeGlobalFlexQuestionnaires: globalFlexRows.map((row) => cleanText(row.name)).filter(Boolean),
    pendingRequests: {
      answerChanges: Number(pendingAnswerRows[0]?.count ?? 0),
      timeChanges: Number(pendingTimeRows[0]?.count ?? 0),
      questionnaireDeletions: Number(pendingDeleteRows[0]?.count ?? 0),
    },
    privacyAndAgreement: {
      gmPrivacyNoticePath: "/datenschutz/gm",
      employeeAgreementPath: "/vereinbarung",
      privacyContact: "datenschutz@merch.at",
      currentAgreementVersion: EMPLOYEE_AGREEMENT_VERSION,
      currentAgreementAccepted: agreementRows.length > 0,
      currentAgreementAcceptedAt: formatDateTime(agreementRows[0]?.acceptedAt ?? null),
      kurtiSparkMemoryMinutes: 15,
    },
  };
}
