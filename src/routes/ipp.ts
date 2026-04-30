import { and, asc, eq, gte, inArray, isNotNull, lt } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { computeMarketIppForPeriod } from "../lib/ipp.js";
import { addDays, getRedPeriodForDate, getRedPeriodLabel, getRedYear, startOfDay } from "../lib/red-monat.js";
import { refreshRedMonthCalendarConfig } from "../lib/red-month-calendar.js";
import { db, sql as pgSql } from "../lib/db.js";
import { ippMarketRedmonthResults, markets, users, visitSessions } from "../lib/schema.js";
import { requireAuth } from "../middleware/auth.js";

const adminIppRouter = Router();
adminIppRouter.use(requireAuth(["admin"]));
let checkedIppArchiveTableReadiness = false;
let hasIppArchiveTable = false;

type IppListRow = {
  id: string;
  marketId: string;
  marketName: string;
  chain: string;
  postalCode: string;
  city: string;
  region: string;
  gmName: string;
  redMonatLabel: string;
  redPeriodStart: string;
  redPeriodEnd: string;
  redPeriodYear: number;
  marketIpp: number;
  includedInAverage: boolean;
  isFinalized: boolean;
  sourceSubmissionCount: number;
  contributingQuestionCount: number;
};

function toYmd(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function parseYmdOrNull(raw: string | null | undefined): Date | null {
  if (!raw || !/^\d{4}-\d{2}-\d{2}$/.test(raw)) return null;
  const parsed = new Date(`${raw}T12:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function toMoneyNumber(input: unknown): number {
  if (typeof input === "number" && Number.isFinite(input)) return input;
  if (typeof input === "string" && input.trim().length > 0) {
    const parsed = Number(input);
    if (Number.isFinite(parsed)) return parsed;
  }
  return 0;
}

function normalizeUnique(values: string[]): string[] {
  return Array.from(new Set(values.map((value) => value.trim()).filter((value) => value.length > 0)));
}

async function ensureIppArchiveTableReady(): Promise<boolean> {
  if (checkedIppArchiveTableReadiness) return hasIppArchiveTable;
  const result = await pgSql<{ ipp_market_redmonth_results: string | null }[]>`
    select to_regclass('public.ipp_market_redmonth_results') as ipp_market_redmonth_results
  `;
  hasIppArchiveTable = Boolean(result[0]?.ipp_market_redmonth_results);
  checkedIppArchiveTableReadiness = true;
  return hasIppArchiveTable;
}

async function loadCurrentPeriodLatestGmByMarket(input: {
  periodStart: Date;
  periodEnd: Date;
}): Promise<Map<string, string>> {
  const start = startOfDay(input.periodStart);
  const endExclusive = addDays(startOfDay(input.periodEnd), 1);
  const rows = await db
    .select({
      marketId: visitSessions.marketId,
      gmUserId: visitSessions.gmUserId,
      submittedAt: visitSessions.submittedAt,
    })
    .from(visitSessions)
    .where(
      and(
        eq(visitSessions.status, "submitted"),
        eq(visitSessions.isDeleted, false),
        isNotNull(visitSessions.submittedAt),
        gte(visitSessions.submittedAt, start),
        lt(visitSessions.submittedAt, endExclusive),
      ),
    )
    .orderBy(asc(visitSessions.marketId));

  const byMarket = new Map<string, { gmUserId: string; submittedAt: Date }>();
  for (const row of rows) {
    if (!row.submittedAt) continue;
    const previous = byMarket.get(row.marketId);
    if (!previous || previous.submittedAt.getTime() < row.submittedAt.getTime()) {
      byMarket.set(row.marketId, { gmUserId: row.gmUserId, submittedAt: row.submittedAt });
    }
  }

  const gmIds = normalizeUnique(Array.from(byMarket.values()).map((entry) => entry.gmUserId));
  const gmRows =
    gmIds.length === 0
      ? []
      : await db
          .select({
            id: users.id,
            firstName: users.firstName,
            lastName: users.lastName,
          })
          .from(users)
          .where(inArray(users.id, gmIds));
  const gmNameById = new Map(gmRows.map((row) => [row.id, `${row.firstName} ${row.lastName}`.trim()]));

  const output = new Map<string, string>();
  for (const [marketId, value] of byMarket.entries()) {
    output.set(marketId, gmNameById.get(value.gmUserId) ?? "");
  }
  return output;
}

async function buildIppRows(now: Date): Promise<IppListRow[]> {
  await refreshRedMonthCalendarConfig();
  const marketRows = await db
    .select()
    .from(markets)
    .where(eq(markets.isDeleted, false));
  const marketById = new Map(marketRows.map((row) => [row.id, row]));

  const currentPeriod = getRedPeriodForDate(now);
  const currentStartYmd = toYmd(currentPeriod.start);
  const currentEndYmd = toYmd(currentPeriod.end);
  const currentLabel = getRedPeriodLabel(now);
  const currentYear = getRedYear(now);

  const currentLatestGmByMarket = await loadCurrentPeriodLatestGmByMarket({
    periodStart: currentPeriod.start,
    periodEnd: currentPeriod.end,
  });

  const archivedRows =
    (await ensureIppArchiveTableReady())
      ? await db
          .select()
          .from(ippMarketRedmonthResults)
          .where(and(eq(ippMarketRedmonthResults.isDeleted, false), eq(ippMarketRedmonthResults.isFinalized, true)))
      : [];

  const output = new Map<string, IppListRow>();
  for (const row of archivedRows) {
    const market = marketById.get(row.marketId);
    if (!market) continue;
    const rowKey = `${row.marketId}::${row.redPeriodStart}`;
    output.set(rowKey, {
      id: rowKey,
      marketId: row.marketId,
      marketName: market.name,
      chain: market.dbName ?? "",
      postalCode: market.postalCode,
      city: market.city,
      region: market.region,
      gmName: market.currentGmName ?? "",
      redMonatLabel: row.redPeriodLabel,
      redPeriodStart: row.redPeriodStart,
      redPeriodEnd: row.redPeriodEnd,
      redPeriodYear: row.redPeriodYear,
      marketIpp: toMoneyNumber(row.marketIpp),
      includedInAverage: toMoneyNumber(row.marketIpp) > 0,
      isFinalized: row.isFinalized,
      sourceSubmissionCount: row.sourceSubmissionCount,
      contributingQuestionCount: row.contributingQuestionCount,
    });
  }

  const currentMarketRows = await db
    .select({
      marketId: visitSessions.marketId,
    })
    .from(visitSessions)
    .where(
      and(
        eq(visitSessions.status, "submitted"),
        eq(visitSessions.isDeleted, false),
        isNotNull(visitSessions.submittedAt),
        gte(visitSessions.submittedAt, startOfDay(currentPeriod.start)),
        lt(visitSessions.submittedAt, addDays(startOfDay(currentPeriod.end), 1)),
      ),
    );
  const currentMarketIds = normalizeUnique(currentMarketRows.map((row) => row.marketId));

  for (const marketId of currentMarketIds) {
    const market = marketById.get(marketId);
    if (!market) continue;
    const computed = await computeMarketIppForPeriod({
      marketId,
      periodStart: currentPeriod.start,
      periodEnd: currentPeriod.end,
    });
    const rowKey = `${marketId}::${currentStartYmd}`;
    output.set(rowKey, {
      id: rowKey,
      marketId,
      marketName: market.name,
      chain: market.dbName ?? "",
      postalCode: market.postalCode,
      city: market.city,
      region: market.region,
      gmName: currentLatestGmByMarket.get(marketId) ?? market.currentGmName ?? "",
      redMonatLabel: currentLabel,
      redPeriodStart: currentStartYmd,
      redPeriodEnd: currentEndYmd,
      redPeriodYear: currentYear,
      marketIpp: computed.marketIpp,
      includedInAverage: computed.marketIpp > 0,
      isFinalized: false,
      sourceSubmissionCount: computed.sourceSubmissionCount,
      contributingQuestionCount: computed.contributingQuestionCount,
    });
  }

  return Array.from(output.values()).sort((left, right) => {
    if (left.redPeriodStart !== right.redPeriodStart) return right.redPeriodStart.localeCompare(left.redPeriodStart);
    return left.marketName.localeCompare(right.marketName, "de");
  });
}

adminIppRouter.get("/ipp", async (_req, res, next) => {
  try {
    const rows = await buildIppRows(new Date());
    const filters = {
      regions: normalizeUnique(rows.map((row) => row.region)).sort((a, b) => a.localeCompare(b, "de")),
      gms: normalizeUnique(rows.map((row) => row.gmName)).sort((a, b) => a.localeCompare(b, "de")),
      chains: normalizeUnique(rows.map((row) => row.chain)).sort((a, b) => a.localeCompare(b, "de")),
      redMonats: normalizeUnique(rows.map((row) => row.redMonatLabel)),
    };
    res.status(200).json({ rows, filters });
  } catch (error) {
    next(error);
  }
});

const detailQuerySchema = z.object({
  periodStart: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
});

adminIppRouter.get("/ipp/:marketId/detail", async (req, res, next) => {
  try {
    const marketId = String(req.params.marketId ?? "");
    if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(marketId)) {
      res.status(400).json({ error: "Ungueltige Markt-ID.", code: "invalid_market_id" });
      return;
    }
    const parsed = detailQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: "periodStart muss im Format YYYY-MM-DD uebergeben werden.", code: "invalid_period" });
      return;
    }

    const periodStartDate = parseYmdOrNull(parsed.data.periodStart);
    if (!periodStartDate) {
      res.status(400).json({ error: "Ungueltiger periodStart.", code: "invalid_period" });
      return;
    }

    const [market] = await db
      .select()
      .from(markets)
      .where(and(eq(markets.id, marketId), eq(markets.isDeleted, false)))
      .limit(1);
    if (!market) {
      res.status(404).json({ error: "Markt nicht gefunden.", code: "market_not_found" });
      return;
    }

    const [archived] =
      (await ensureIppArchiveTableReady())
        ? await db
            .select()
            .from(ippMarketRedmonthResults)
            .where(
              and(
                eq(ippMarketRedmonthResults.marketId, marketId),
                eq(ippMarketRedmonthResults.redPeriodStart, parsed.data.periodStart),
                eq(ippMarketRedmonthResults.isDeleted, false),
              ),
            )
            .limit(1)
        : [];

    const periodInfo = getRedPeriodForDate(periodStartDate);
    const periodStart = parseYmdOrNull(parsed.data.periodStart) ?? periodInfo.start;
    const currentStartYmd = toYmd(getRedPeriodForDate(new Date()).start);
    const requestedStartYmd = toYmd(periodStart);
    const isCurrentPeriod = requestedStartYmd === currentStartYmd;
    const isClosedPeriod = requestedStartYmd < currentStartYmd;

    if (isClosedPeriod && !archived) {
      res.status(404).json({
        error: "Kein finalisierter Snapshot fuer diesen abgeschlossenen RED-Monat vorhanden.",
        code: "snapshot_not_found",
      });
      return;
    }

    if (archived && !isCurrentPeriod) {
      const snapshotRows = Array.isArray(archived.questionRowsSnapshot)
        ? (archived.questionRowsSnapshot as unknown[])
        : [];
      const questionRows = snapshotRows.filter((row): row is Record<string, unknown> => Boolean(row) && typeof row === "object");
      const archivedMarketIpp = toMoneyNumber(archived.marketIpp);
      res.status(200).json({
        record: {
          id: `${marketId}::${archived.redPeriodStart}`,
          marketId,
          marketName: market.name,
          chain: market.dbName ?? "",
          postalCode: market.postalCode,
          city: market.city,
          region: market.region,
          gmName: market.currentGmName ?? "",
          redMonatLabel: archived.redPeriodLabel,
          redPeriodStart: archived.redPeriodStart,
          redPeriodEnd: archived.redPeriodEnd,
          redPeriodYear: archived.redPeriodYear,
          marketIpp: archivedMarketIpp,
          includedInAverage: archivedMarketIpp > 0,
          isFinalized: archived.isFinalized,
          sourceSubmissionCount: archived.sourceSubmissionCount,
          contributingQuestionCount: archived.contributingQuestionCount,
          questionRows,
        },
      });
      return;
    }

    const periodEnd = periodInfo.end;
    const computed = await computeMarketIppForPeriod({
      marketId,
      periodStart,
      periodEnd,
    });

    const marketIpp = computed.marketIpp;
    res.status(200).json({
      record: {
        id: `${marketId}::${toYmd(periodStart)}`,
        marketId,
        marketName: market.name,
        chain: market.dbName ?? "",
        postalCode: market.postalCode,
        city: market.city,
        region: market.region,
        gmName: market.currentGmName ?? "",
        redMonatLabel: getRedPeriodLabel(periodStart),
        redPeriodStart: toYmd(periodStart),
        redPeriodEnd: toYmd(periodEnd),
        redPeriodYear: getRedYear(periodStart),
        marketIpp,
        includedInAverage: marketIpp > 0,
        isFinalized: false,
        sourceSubmissionCount: computed.sourceSubmissionCount,
        contributingQuestionCount: computed.contributingQuestionCount,
        questionRows: computed.questionRows,
      },
    });
  } catch (error) {
    next(error);
  }
});

export { adminIppRouter };
