import { and, eq, inArray } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { computeMarketIppForPeriod, computeMarketIppSummariesForPeriod } from "../lib/ipp.js";
import { requireKundeAdminPermission } from "../lib/kunde-access.js";
import { logAction, startActionTimer } from "../lib/logger.js";
import { redMonthPeriodToYmd, resolveCurrentRedPeriod, resolveRedPeriodForDate, type ResolvedRedMonthPeriod } from "../lib/red-month-periods.js";
import { db, sql as pgSql } from "../lib/db.js";
import { ippMarketRedmonthResults, markets, users } from "../lib/schema.js";
import { requireAuth } from "../middleware/auth.js";

const adminIppRouter = Router();
adminIppRouter.use(requireAuth(["admin", "kunde"]));
adminIppRouter.use(requireKundeAdminPermission);
adminIppRouter.use("/ipp", (req, res, next) => {
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "admin_ipp_action_completed", {
      req,
      action: "admin_ipp_read",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});
let checkedIppArchiveTableReadiness = false;
let hasIppArchiveTable = false;
const IPP_ROUTE_BATCH_SIZE = 500;

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

type IppPeriodPayload = {
  redPeriodId?: string | null;
  redPeriodStart: string;
  redPeriodEnd: string;
  redMonatLabel: string;
  redPeriodYear: number;
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

function chunkArray<T>(values: T[], size = IPP_ROUTE_BATCH_SIZE): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

function buildIppPeriodPayload(period: ResolvedRedMonthPeriod): IppPeriodPayload {
  const { startYmd, endYmd } = redMonthPeriodToYmd(period);
  return {
    redPeriodId: period.redPeriodId,
    redPeriodStart: startYmd,
    redPeriodEnd: endYmd,
    redMonatLabel: period.label,
    redPeriodYear: period.redYear,
  };
}

function sortIppRows(rows: IppListRow[]): IppListRow[] {
  return [...rows].sort((left, right) => {
    if (left.redPeriodStart !== right.redPeriodStart) return right.redPeriodStart.localeCompare(left.redPeriodStart);
    return left.marketName.localeCompare(right.marketName, "de");
  });
}

function buildIppFilters(rows: IppListRow[]) {
  return {
    regions: normalizeUnique(rows.map((row) => row.region)).sort((a, b) => a.localeCompare(b, "de")),
    gms: normalizeUnique(rows.map((row) => row.gmName)).sort((a, b) => a.localeCompare(b, "de")),
    chains: normalizeUnique(rows.map((row) => row.chain)).sort((a, b) => a.localeCompare(b, "de")),
    redMonats: normalizeUnique(rows.map((row) => row.redMonatLabel)),
  };
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

async function loadMarketMapByIds(marketIds: string[]) {
  const rows: Array<typeof markets.$inferSelect> = [];
  for (const chunk of chunkArray(normalizeUnique(marketIds))) {
    const chunkRows = await db
      .select()
      .from(markets)
      .where(and(inArray(markets.id, chunk), eq(markets.isDeleted, false)));
    rows.push(...chunkRows);
  }
  return new Map(rows.map((row) => [row.id, row]));
}

async function loadGmNameMapByIds(gmIds: string[]) {
  const rows: Array<{ id: string; firstName: string; lastName: string }> = [];
  for (const chunk of chunkArray(normalizeUnique(gmIds))) {
    const chunkRows = await db
      .select({
        id: users.id,
        firstName: users.firstName,
        lastName: users.lastName,
      })
      .from(users)
      .where(inArray(users.id, chunk));
    rows.push(...chunkRows);
  }
  return new Map(rows.map((row) => [row.id, `${row.firstName} ${row.lastName}`.trim()]));
}

async function buildArchivedIppRows(): Promise<IppListRow[]> {
  const archivedRows =
    (await ensureIppArchiveTableReady())
      ? await db
          .select({
            marketId: ippMarketRedmonthResults.marketId,
            redPeriodStart: ippMarketRedmonthResults.redPeriodStart,
            redPeriodEnd: ippMarketRedmonthResults.redPeriodEnd,
            redPeriodLabel: ippMarketRedmonthResults.redPeriodLabel,
            redPeriodYear: ippMarketRedmonthResults.redPeriodYear,
            marketIpp: ippMarketRedmonthResults.marketIpp,
            isFinalized: ippMarketRedmonthResults.isFinalized,
            sourceSubmissionCount: ippMarketRedmonthResults.sourceSubmissionCount,
            contributingQuestionCount: ippMarketRedmonthResults.contributingQuestionCount,
          })
          .from(ippMarketRedmonthResults)
          .where(and(eq(ippMarketRedmonthResults.isDeleted, false), eq(ippMarketRedmonthResults.isFinalized, true)))
      : [];
  const marketById = await loadMarketMapByIds(archivedRows.map((row) => row.marketId));

  const output: IppListRow[] = [];
  for (const row of archivedRows) {
    const market = marketById.get(row.marketId);
    if (!market) continue;
    const rowKey = `${row.marketId}::${row.redPeriodStart}`;
    output.push({
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

  return sortIppRows(output);
}

async function buildCurrentIppRows(now: Date): Promise<IppListRow[]> {
  const currentPeriod = await resolveCurrentRedPeriod(now);
  const currentMeta = buildIppPeriodPayload(currentPeriod);
  const currentSummaries = await computeMarketIppSummariesForPeriod({
    periodStart: currentPeriod.start,
    periodEnd: currentPeriod.end,
  });
  const currentSummaryRows = Array.from(currentSummaries.values());
  const marketById = await loadMarketMapByIds(currentSummaryRows.map((row) => row.marketId));
  const currentGmNameById = await loadGmNameMapByIds(
    currentSummaryRows.map((row) => row.latestGmUserId ?? ""),
  );

  const output: IppListRow[] = [];
  for (const computed of currentSummaryRows) {
    const marketId = computed.marketId;
    const market = marketById.get(marketId);
    if (!market) continue;
    const rowKey = `${marketId}::${currentMeta.redPeriodStart}`;
    output.push({
      id: rowKey,
      marketId,
      marketName: market.name,
      chain: market.dbName ?? "",
      postalCode: market.postalCode,
      city: market.city,
      region: market.region,
      gmName: computed.latestGmUserId
        ? currentGmNameById.get(computed.latestGmUserId) ?? market.currentGmName ?? ""
        : market.currentGmName ?? "",
      redMonatLabel: currentMeta.redMonatLabel,
      redPeriodStart: currentMeta.redPeriodStart,
      redPeriodEnd: currentMeta.redPeriodEnd,
      redPeriodYear: currentMeta.redPeriodYear,
      marketIpp: computed.marketIpp,
      includedInAverage: computed.marketIpp > 0,
      isFinalized: false,
      sourceSubmissionCount: computed.sourceSubmissionCount,
      contributingQuestionCount: computed.contributingQuestionCount,
    });
  }

  return sortIppRows(output);
}

function mergeIppRows(rows: IppListRow[]): IppListRow[] {
  const byId = new Map<string, IppListRow>();
  for (const row of rows) {
    byId.set(row.id, row);
  }
  return sortIppRows(Array.from(byId.values()));
}

adminIppRouter.get("/ipp", async (_req, res, next) => {
  try {
    const now = new Date();
    const [archivedRows, currentRows] = await Promise.all([
      buildArchivedIppRows(),
      buildCurrentIppRows(now),
    ]);
    const rows = mergeIppRows([...archivedRows, ...currentRows]);
    res.status(200).json({
      rows,
      filters: buildIppFilters(rows),
      currentPeriod: buildIppPeriodPayload(await resolveCurrentRedPeriod(now)),
    });
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
      res.status(400).json({ error: "Ungültige Markt-ID.", code: "invalid_market_id" });
      return;
    }
    const parsed = detailQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: "periodStart muss im Format YYYY-MM-DD ?bergeben werden.", code: "invalid_period" });
      return;
    }

    const periodStartDate = parseYmdOrNull(parsed.data.periodStart);
    if (!periodStartDate) {
      res.status(400).json({ error: "Ungültiger periodStart.", code: "invalid_period" });
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

    const periodInfo = await resolveRedPeriodForDate(periodStartDate);
    const periodStart = parseYmdOrNull(parsed.data.periodStart) ?? periodInfo.start;
    const currentStartYmd = toYmd((await resolveCurrentRedPeriod(new Date())).start);
    const requestedStartYmd = toYmd(periodStart);
    const isCurrentPeriod = requestedStartYmd === currentStartYmd;
    const isClosedPeriod = requestedStartYmd < currentStartYmd;

    if (isClosedPeriod && !archived) {
      res.status(404).json({
        error: "Kein finalisierter Snapshot für diesen abgeschlossenen RED-Monat vorhanden.",
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
        redMonatLabel: periodInfo.label,
        redPeriodStart: toYmd(periodStart),
        redPeriodEnd: toYmd(periodEnd),
        redPeriodYear: periodInfo.redYear,
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
