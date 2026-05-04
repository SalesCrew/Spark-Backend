import { and, desc, eq, gte, isNotNull, lt } from "drizzle-orm";
import { db, sql as pgSql } from "./db.js";
import { gmKpiCache, visitSessions } from "./schema.js";

export type GmKpiSummary = {
  ippAllTimeAvg: number;
  ippSampleCount: number;
  bonusCumulativeEur: number;
  lastComputedAt: string;
};

let checkedGmKpiCacheTable = false;
let hasGmKpiCacheTable = false;
let checkedIppSnapshotTable = false;
let hasIppSnapshotTable = false;
let checkedBonusTotalsTable = false;
let hasBonusTotalsTable = false;

function normalizeNumber(input: unknown): number {
  if (typeof input === "number" && Number.isFinite(input)) return input;
  if (typeof input === "string" && input.trim().length > 0) {
    const parsed = Number(input);
    if (Number.isFinite(parsed)) return parsed;
  }
  return 0;
}

function normalizeSummary(input: {
  ippAllTimeAvg: number;
  ippSampleCount: number;
  bonusCumulativeEur: number;
  lastComputedAt: Date;
}): GmKpiSummary {
  return {
    ippAllTimeAvg: Number(input.ippAllTimeAvg.toFixed(4)),
    ippSampleCount: Math.max(0, Math.round(input.ippSampleCount)),
    bonusCumulativeEur: Number(input.bonusCumulativeEur.toFixed(2)),
    lastComputedAt: input.lastComputedAt.toISOString(),
  };
}

async function ensureGmKpiCacheTableReady(): Promise<boolean> {
  if (checkedGmKpiCacheTable) return hasGmKpiCacheTable;
  const rows = await pgSql<{ gm_kpi_cache: string | null }[]>`
    select to_regclass('public.gm_kpi_cache') as gm_kpi_cache
  `;
  hasGmKpiCacheTable = Boolean(rows[0]?.gm_kpi_cache);
  checkedGmKpiCacheTable = hasGmKpiCacheTable;
  return hasGmKpiCacheTable;
}

async function ensureIppSnapshotTableReady(): Promise<boolean> {
  if (checkedIppSnapshotTable) return hasIppSnapshotTable;
  const rows = await pgSql<{ ipp_market_redmonth_results: string | null }[]>`
    select to_regclass('public.ipp_market_redmonth_results') as ipp_market_redmonth_results
  `;
  hasIppSnapshotTable = Boolean(rows[0]?.ipp_market_redmonth_results);
  checkedIppSnapshotTable = hasIppSnapshotTable;
  return hasIppSnapshotTable;
}

async function ensureBonusTotalsTableReady(): Promise<boolean> {
  if (checkedBonusTotalsTable) return hasBonusTotalsTable;
  const rows = await pgSql<{ praemien_gm_wave_totals: string | null }[]>`
    select to_regclass('public.praemien_gm_wave_totals') as praemien_gm_wave_totals
  `;
  hasBonusTotalsTable = Boolean(rows[0]?.praemien_gm_wave_totals);
  checkedBonusTotalsTable = hasBonusTotalsTable;
  return hasBonusTotalsTable;
}

async function loadIppAggregateForGm(gmUserId: string): Promise<{ avg: number; count: number }> {
  if (!(await ensureIppSnapshotTableReady())) {
    return { avg: 0, count: 0 };
  }
  const rows = await pgSql<{ avg_ipp: string | number | null; sample_count: number | null }[]>`
    select
      coalesce(avg(r.market_ipp), 0) as avg_ipp,
      count(*)::int as sample_count
    from ipp_market_redmonth_results r
    join lateral (
      select vs.gm_user_id
      from visit_sessions vs
      where
        vs.market_id = r.market_id
        and vs.status = 'submitted'
        and vs.is_deleted = false
        and vs.submitted_at is not null
        and (vs.submitted_at at time zone 'UTC')::date >= r.red_period_start
        and (vs.submitted_at at time zone 'UTC')::date <= r.red_period_end
      order by vs.submitted_at desc
      limit 1
    ) latest on true
    where
      r.is_deleted = false
      and r.is_finalized = true
      and r.market_ipp > 0
      and latest.gm_user_id = ${gmUserId}
  `;
  const row = rows[0];
  return {
    avg: normalizeNumber(row?.avg_ipp),
    count: Number(row?.sample_count ?? 0),
  };
}

async function loadBonusCumulativeForGm(gmUserId: string): Promise<number> {
  if (!(await ensureBonusTotalsTableReady())) {
    return 0;
  }
  const rows = await pgSql<{ bonus_total: string | number | null }[]>`
    select coalesce(sum(current_reward_eur), 0) as bonus_total
    from praemien_gm_wave_totals
    where gm_user_id = ${gmUserId}
  `;
  return normalizeNumber(rows[0]?.bonus_total);
}

export async function readGmKpiCache(gmUserId: string): Promise<GmKpiSummary | null> {
  if (!(await ensureGmKpiCacheTableReady())) return null;
  const [row] = await db
    .select()
    .from(gmKpiCache)
    .where(and(eq(gmKpiCache.gmUserId, gmUserId), eq(gmKpiCache.isDeleted, false)))
    .limit(1);
  if (!row) return null;
  return normalizeSummary({
    ippAllTimeAvg: normalizeNumber(row.ippAllTimeAvg),
    ippSampleCount: row.ippSampleCount,
    bonusCumulativeEur: normalizeNumber(row.bonusCumulativeEur),
    lastComputedAt: row.lastComputedAt,
  });
}

export async function recomputeGmKpiCache(gmUserId: string): Promise<GmKpiSummary> {
  const [ippAgg, bonusCumulativeEur] = await Promise.all([
    loadIppAggregateForGm(gmUserId),
    loadBonusCumulativeForGm(gmUserId),
  ]);
  const now = new Date();
  const normalized = normalizeSummary({
    ippAllTimeAvg: ippAgg.avg,
    ippSampleCount: ippAgg.count,
    bonusCumulativeEur,
    lastComputedAt: now,
  });
  if (!(await ensureGmKpiCacheTableReady())) return normalized;

  const [existing] = await db
    .select({ id: gmKpiCache.id })
    .from(gmKpiCache)
    .where(and(eq(gmKpiCache.gmUserId, gmUserId), eq(gmKpiCache.isDeleted, false)))
    .limit(1);

  if (existing) {
    await db
      .update(gmKpiCache)
      .set({
        ippAllTimeAvg: String(normalized.ippAllTimeAvg),
        ippSampleCount: normalized.ippSampleCount,
        bonusCumulativeEur: String(normalized.bonusCumulativeEur),
        lastComputedAt: now,
        updatedAt: now,
      })
      .where(eq(gmKpiCache.id, existing.id));
    return normalized;
  }

  await db.insert(gmKpiCache).values({
    gmUserId,
    ippAllTimeAvg: String(normalized.ippAllTimeAvg),
    ippSampleCount: normalized.ippSampleCount,
    bonusCumulativeEur: String(normalized.bonusCumulativeEur),
    lastComputedAt: now,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  return normalized;
}

export async function ensureAndGetGmKpiCache(gmUserId: string): Promise<GmKpiSummary> {
  const existing = await readGmKpiCache(gmUserId);
  if (existing) return existing;
  return recomputeGmKpiCache(gmUserId);
}

export async function recomputeGmKpiCacheForMarketPeriod(input: {
  marketId: string;
  redPeriodStart: string;
  redPeriodEnd: string;
}): Promise<void> {
  const periodStart = new Date(`${input.redPeriodStart}T00:00:00.000Z`);
  const periodEndExclusive = new Date(`${input.redPeriodEnd}T00:00:00.000Z`);
  periodEndExclusive.setDate(periodEndExclusive.getDate() + 1);

  const [latestSession] = await db
    .select({ gmUserId: visitSessions.gmUserId })
    .from(visitSessions)
    .where(
      and(
        eq(visitSessions.marketId, input.marketId),
        eq(visitSessions.status, "submitted"),
        eq(visitSessions.isDeleted, false),
        isNotNull(visitSessions.submittedAt),
        gte(visitSessions.submittedAt, periodStart),
        lt(visitSessions.submittedAt, periodEndExclusive),
      ),
    )
    .orderBy(desc(visitSessions.submittedAt))
    .limit(1);

  if (!latestSession?.gmUserId) return;
  await recomputeGmKpiCache(latestSession.gmUserId);
}
