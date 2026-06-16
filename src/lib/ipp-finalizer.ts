import { and, eq, gte, inArray, isNotNull, lt } from "drizzle-orm";
import { env } from "../config/env.js";
import { db, sql as pgSql } from "./db.js";
import { recomputeGmKpiCacheForMarketPeriod } from "./gm-kpi-cache.js";
import { computeMarketIppForPeriod } from "./ipp.js";
import { logAction, logger, serializeError, startActionTimer } from "./logger.js";
import {
  resolveCurrentRedPeriod,
  resolveRedPeriodForDate,
  type ResolvedRedMonthPeriod,
} from "./red-month-periods.js";
import { startOfDay } from "./red-monat.js";
import { ippMarketRedmonthResults, ippRecalcQueue, visitAnswers, visitSessions } from "./schema.js";

const IPP_FINALIZER_LOCK_KEY = 294_015_771;
const DEFAULT_STARTUP_DELAY_MS = 15_000;

type ClosedPeriod = {
  redPeriodId: string | null;
  start: Date;
  end: Date;
  label: string;
  year: number;
};

type FinalizerRunResult = {
  ok: boolean;
  skippedByLock: boolean;
  periodsProcessed: number;
  rowsFinalized: number;
  queueItemsProcessed: number;
};

function toYmd(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function isBeforeDay(left: Date, right: Date): boolean {
  return startOfDay(left).getTime() < startOfDay(right).getTime();
}

function normalizeUnique(values: string[]): string[] {
  return Array.from(new Set(values.map((value) => value.trim()).filter((value) => value.length > 0)));
}

function parseYmd(raw: string): Date {
  return new Date(`${raw}T12:00:00.000Z`);
}

async function isClosedPeriod(period: { end: Date }, now: Date): Promise<boolean> {
  const currentStart = startOfDay((await resolveCurrentRedPeriod(now)).start);
  return isBeforeDay(period.end, currentStart);
}

function toClosedPeriod(period: ResolvedRedMonthPeriod): ClosedPeriod {
  return {
    redPeriodId: period.redPeriodId,
    start: period.start,
    end: period.end,
    label: period.label,
    year: period.redYear,
  };
}

async function withPgAdvisoryLock<T>(key: number, runner: () => Promise<T>): Promise<{ locked: boolean; result?: T }> {
  const lockRows = await pgSql<{ locked: boolean }[]>`select pg_try_advisory_lock(${key}) as locked`;
  const locked = Boolean(lockRows[0]?.locked);
  if (!locked) return { locked: false };
  try {
    const result = await runner();
    return { locked: true, result };
  } finally {
    await pgSql`select pg_advisory_unlock(${key})`;
  }
}

async function loadClosedPeriodsWithSubmissions(now: Date): Promise<ClosedPeriod[]> {
  const currentPeriod = await resolveCurrentRedPeriod(now);
  const currentStart = startOfDay(currentPeriod.start);

  const rows = await db
    .select({
      submittedAt: visitSessions.submittedAt,
    })
    .from(visitSessions)
    .where(
      and(
        eq(visitSessions.status, "submitted"),
        eq(visitSessions.isDeleted, false),
        isNotNull(visitSessions.submittedAt),
        lt(visitSessions.submittedAt, currentStart),
      ),
    );

  const periodMap = new Map<string, ClosedPeriod>();
  for (const row of rows) {
    if (!row.submittedAt) continue;
    const period = await resolveRedPeriodForDate(row.submittedAt);
    if (!isBeforeDay(period.end, currentStart)) continue;
    const key = period.redPeriodId ?? toYmd(period.start);
    if (!periodMap.has(key)) {
      periodMap.set(key, toClosedPeriod(period));
    }
  }

  return Array.from(periodMap.values()).sort((left, right) => left.start.getTime() - right.start.getTime());
}

async function loadMarketIdsWithSubmittedVisitsInPeriod(period: ClosedPeriod): Promise<string[]> {
  const start = startOfDay(period.start);
  const endExclusive = new Date(startOfDay(period.end));
  endExclusive.setDate(endExclusive.getDate() + 1);

  const rows = await db
    .select({
      marketId: visitSessions.marketId,
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
    );
  return normalizeUnique(rows.map((row) => row.marketId));
}

async function upsertArchivedMarketPeriodResult(input: {
  marketId: string;
  period: ClosedPeriod;
  marketIpp: number;
  sourceSubmissionCount: number;
  contributingQuestionCount: number;
  questionRowsSnapshot: Array<Record<string, unknown>>;
}): Promise<void> {
  const now = new Date();
  const redPeriodStart = toYmd(input.period.start);
  const redPeriodEnd = toYmd(input.period.end);

  const [existing] = await db
    .select({ id: ippMarketRedmonthResults.id })
    .from(ippMarketRedmonthResults)
    .where(
      and(
        eq(ippMarketRedmonthResults.marketId, input.marketId),
        eq(ippMarketRedmonthResults.redPeriodStart, redPeriodStart),
        eq(ippMarketRedmonthResults.isDeleted, false),
      ),
    )
    .limit(1);

  if (existing) {
    await db
      .update(ippMarketRedmonthResults)
      .set({
        redPeriodId: input.period.redPeriodId,
        redPeriodEnd,
        redPeriodLabel: input.period.label,
        redPeriodYear: input.period.year,
        marketIpp: String(input.marketIpp),
        sourceSubmissionCount: input.sourceSubmissionCount,
        contributingQuestionCount: input.contributingQuestionCount,
        questionRowsSnapshot: input.questionRowsSnapshot,
        snapshotVersion: 1,
        isFinalized: true,
        computedAt: now,
        finalizedAt: now,
        updatedAt: now,
      })
      .where(eq(ippMarketRedmonthResults.id, existing.id));
    return;
  }

  await db.insert(ippMarketRedmonthResults).values({
    marketId: input.marketId,
    redPeriodId: input.period.redPeriodId,
    redPeriodStart,
    redPeriodEnd,
    redPeriodLabel: input.period.label,
    redPeriodYear: input.period.year,
    marketIpp: String(input.marketIpp),
    sourceSubmissionCount: input.sourceSubmissionCount,
    contributingQuestionCount: input.contributingQuestionCount,
    questionRowsSnapshot: input.questionRowsSnapshot,
    snapshotVersion: 1,
    isFinalized: true,
    computedAt: now,
    finalizedAt: now,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
}

async function enqueueIppRecalcForPeriod(input: {
  marketId: string;
  period: ClosedPeriod;
  reason: string;
}): Promise<boolean> {
  const now = new Date();
  const redPeriodStart = toYmd(input.period.start);
  const redPeriodEnd = toYmd(input.period.end);

  const [existing] = await db
    .select({ id: ippRecalcQueue.id })
    .from(ippRecalcQueue)
    .where(
      and(
        eq(ippRecalcQueue.marketId, input.marketId),
        eq(ippRecalcQueue.redPeriodStart, redPeriodStart),
        eq(ippRecalcQueue.isDeleted, false),
        inArray(ippRecalcQueue.status, ["pending", "processing"]),
      ),
    )
    .limit(1);
  if (existing) return false;

  await db.insert(ippRecalcQueue).values({
    marketId: input.marketId,
    redPeriodId: input.period.redPeriodId,
    redPeriodStart,
    redPeriodEnd,
    reason: input.reason,
    status: "pending",
    attempts: 0,
    lastError: null,
    queuedAt: now,
    startedAt: null,
    finishedAt: null,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  return true;
}

export async function enqueueIppRecalcForDate(
  marketId: string,
  changedAt: Date,
  reason: string,
  now = new Date(),
): Promise<boolean> {
  const period = await resolveRedPeriodForDate(changedAt);
  if (!(await isClosedPeriod(period, now))) return false;
  return enqueueIppRecalcForPeriod({
    marketId,
    period: toClosedPeriod(period),
    reason,
  });
}

export async function enqueueIppRecalcForQuestionScoringChanges(
  questionIds: string[],
  reason: string,
  now = new Date(),
): Promise<number> {
  const ids = normalizeUnique(questionIds);
  if (ids.length === 0) return 0;

  const rows = await db
    .select({
      marketId: visitSessions.marketId,
      submittedAt: visitSessions.submittedAt,
    })
    .from(visitAnswers)
    .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
    .where(
      and(
        inArray(visitAnswers.questionId, ids),
        eq(visitAnswers.isDeleted, false),
        eq(visitSessions.status, "submitted"),
        eq(visitSessions.isDeleted, false),
        isNotNull(visitSessions.submittedAt),
      ),
    );

  const keys = new Map<string, { marketId: string; period: ClosedPeriod }>();
  for (const row of rows) {
    if (!row.submittedAt) continue;
    const periodInfo = await resolveRedPeriodForDate(row.submittedAt);
    if (!(await isClosedPeriod(periodInfo, now))) continue;
    const key = `${row.marketId}::${periodInfo.redPeriodId ?? toYmd(periodInfo.start)}`;
    if (!keys.has(key)) {
      keys.set(key, {
        marketId: row.marketId,
        period: toClosedPeriod(periodInfo),
      });
    }
  }

  let enqueued = 0;
  for (const value of keys.values()) {
    const inserted = await enqueueIppRecalcForPeriod({
      marketId: value.marketId,
      period: value.period,
      reason,
    });
    if (inserted) enqueued += 1;
  }
  return enqueued;
}

async function processRecalcQueue(now: Date): Promise<number> {
  const pending = await db
    .select()
    .from(ippRecalcQueue)
    .where(
      and(
        eq(ippRecalcQueue.isDeleted, false),
        inArray(ippRecalcQueue.status, ["pending", "failed"]),
      ),
    )
    .orderBy(ippRecalcQueue.queuedAt);

  let processed = 0;
  for (const item of pending) {
    const [claimed] = await db
      .update(ippRecalcQueue)
      .set({
        status: "processing",
        attempts: item.attempts + 1,
        startedAt: now,
        updatedAt: now,
      })
      .where(and(eq(ippRecalcQueue.id, item.id), inArray(ippRecalcQueue.status, ["pending", "failed"])))
      .returning({ id: ippRecalcQueue.id });
    if (!claimed) continue;

    try {
      const period = toClosedPeriod(await resolveRedPeriodForDate(parseYmd(item.redPeriodStart)));
      const computed = await computeMarketIppForPeriod({
        marketId: item.marketId,
        periodStart: period.start,
        periodEnd: period.end,
      });
      await upsertArchivedMarketPeriodResult({
        marketId: item.marketId,
        period,
        marketIpp: computed.marketIpp,
        sourceSubmissionCount: computed.sourceSubmissionCount,
        contributingQuestionCount: computed.contributingQuestionCount,
        questionRowsSnapshot: computed.questionRows as Array<Record<string, unknown>>,
      });
      await recomputeGmKpiCacheForMarketPeriod({
        marketId: item.marketId,
        redPeriodStart: item.redPeriodStart,
        redPeriodEnd: item.redPeriodEnd,
      });
      await db
        .update(ippRecalcQueue)
        .set({
          status: "done",
          lastError: null,
          finishedAt: new Date(),
          updatedAt: new Date(),
        })
        .where(eq(ippRecalcQueue.id, item.id));
      processed += 1;
    } catch (error) {
      logger.error("ipp_finalizer_queue_item_failed", {
        action: "ipp_finalizer_process_queue_item",
        result: "failure",
        queueItemId: item.id,
        marketId: item.marketId,
        redPeriodStart: item.redPeriodStart,
        redPeriodEnd: item.redPeriodEnd,
        error: serializeError(error),
      });
      await db
        .update(ippRecalcQueue)
        .set({
          status: "failed",
          lastError: error instanceof Error ? error.message.slice(0, 1500) : "Unknown queue processing error.",
          finishedAt: new Date(),
          updatedAt: new Date(),
        })
        .where(eq(ippRecalcQueue.id, item.id));
    }
  }
  return processed;
}

export async function runIppFinalizerOnce(now = new Date()): Promise<FinalizerRunResult> {
  const startedAtNs = startActionTimer();
  const lockResult = await withPgAdvisoryLock(IPP_FINALIZER_LOCK_KEY, async () => {
    const periods = await loadClosedPeriodsWithSubmissions(now);
    let rowsFinalized = 0;

    for (const period of periods) {
      const marketIds = await loadMarketIdsWithSubmittedVisitsInPeriod(period);
      if (marketIds.length === 0) continue;

      const existingRows = await db
        .select({
          marketId: ippMarketRedmonthResults.marketId,
          isFinalized: ippMarketRedmonthResults.isFinalized,
        })
        .from(ippMarketRedmonthResults)
        .where(
          and(
            period.redPeriodId
              ? eq(ippMarketRedmonthResults.redPeriodId, period.redPeriodId)
              : eq(ippMarketRedmonthResults.redPeriodStart, toYmd(period.start)),
            eq(ippMarketRedmonthResults.isDeleted, false),
            inArray(ippMarketRedmonthResults.marketId, marketIds),
          ),
        );
      const finalizedMarketIds = new Set(
        existingRows.filter((row) => row.isFinalized).map((row) => row.marketId),
      );

      for (const marketId of marketIds) {
        if (finalizedMarketIds.has(marketId)) continue;
        const computed = await computeMarketIppForPeriod({
          marketId,
          periodStart: period.start,
          periodEnd: period.end,
        });
        await upsertArchivedMarketPeriodResult({
          marketId,
          period,
          marketIpp: computed.marketIpp,
          sourceSubmissionCount: computed.sourceSubmissionCount,
          contributingQuestionCount: computed.contributingQuestionCount,
          questionRowsSnapshot: computed.questionRows as Array<Record<string, unknown>>,
        });
        await recomputeGmKpiCacheForMarketPeriod({
          marketId,
          redPeriodStart: toYmd(period.start),
          redPeriodEnd: toYmd(period.end),
        });
        rowsFinalized += 1;
      }
    }

    const queueItemsProcessed = await processRecalcQueue(now);

    return {
      ok: true,
      skippedByLock: false,
      periodsProcessed: periods.length,
      rowsFinalized,
      queueItemsProcessed,
    } satisfies FinalizerRunResult;
  });

  if (!lockResult.locked || !lockResult.result) {
    logAction("debug", "ipp_finalizer_run_skipped_lock", {
      action: "ipp_finalizer_run",
      result: "skipped",
      startedAtNs,
      details: { reason: "advisory_lock_not_acquired" },
    });
    return {
      ok: true,
      skippedByLock: true,
      periodsProcessed: 0,
      rowsFinalized: 0,
      queueItemsProcessed: 0,
    };
  }
  logAction("info", "ipp_finalizer_run_success", {
    action: "ipp_finalizer_run",
    result: "success",
    startedAtNs,
    details: {
      periodsProcessed: lockResult.result.periodsProcessed,
      rowsFinalized: lockResult.result.rowsFinalized,
      queueItemsProcessed: lockResult.result.queueItemsProcessed,
    },
  });
  return lockResult.result;
}

export function startIppFinalizerScheduler(): () => void {
  if (!env.IPP_FINALIZER_ENABLED) {
    logger.info("ipp_finalizer_scheduler_disabled");
    return () => {};
  }

  let running = false;
  const run = async () => {
    if (running) return;
    running = true;
    const startedAtNs = startActionTimer();
    try {
      const result = await runIppFinalizerOnce(new Date());
      if (!result.skippedByLock) {
        logAction("info", "ipp_finalizer_scheduler_run_completed", {
          action: "ipp_finalizer_scheduler_tick",
          result: "success",
          startedAtNs,
          details: {
            periodsProcessed: result.periodsProcessed,
            rowsFinalized: result.rowsFinalized,
            queueItemsProcessed: result.queueItemsProcessed,
          },
        });
      } else {
        logAction("debug", "ipp_finalizer_scheduler_run_skipped_lock", {
          action: "ipp_finalizer_scheduler_tick",
          result: "skipped",
          startedAtNs,
          details: { reason: "run_locked" },
        });
      }
    } catch (error) {
      logAction("error", "ipp_finalizer_scheduler_run_failed", {
        action: "ipp_finalizer_scheduler_tick",
        result: "failure",
        startedAtNs,
        error,
      });
    } finally {
      running = false;
    }
  };

  const startupTimer = setTimeout(() => {
    void run();
  }, DEFAULT_STARTUP_DELAY_MS);
  const interval = setInterval(() => {
    void run();
  }, env.IPP_FINALIZER_INTERVAL_MS);

  return () => {
    clearTimeout(startupTimer);
    clearInterval(interval);
  };
}
