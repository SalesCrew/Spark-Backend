import { createHash } from "node:crypto";
import { computeMarketIppSummariesForRedPeriods } from "./ipp.js";
import { db, sql as pgSql } from "./db.js";
import { redMonthPeriods, users } from "./schema.js";
import { and, asc, eq, inArray, isNull } from "drizzle-orm";
import { redMonthPeriodToYmd, resolveCurrentRedPeriod } from "./red-month-periods.js";

export type IppPeriodMeta = {
  id: string;
  redYear: number;
  periodIndex: number;
  label: string;
  startDate: string;
  endDate: string;
};

export type IppGmAdjustmentEvent = {
  id: string;
  revisionNumber: number;
  requestId: string;
  gmUserId: string;
  redPeriodId: string;
  eventType: "set" | "clear";
  correctedIpp: number | null;
  baseCalculatedIpp: number;
  baseSampleCount: number;
  baseFingerprint: string;
  reason: string;
  createdByUserId: string;
  createdByName: string;
  createdAt: string;
};

export type EffectiveGmIppPeriodRow = {
  gmUserId: string;
  gmName: string;
  region: string;
  redPeriodId: string;
  redPeriodLabel: string;
  redPeriodYear: number;
  periodIndex: number;
  periodStart: string;
  periodEnd: string;
  calculatedIpp: number;
  effectiveIpp: number;
  difference: number;
  marketSampleCount: number;
  zeroOrUnscoredMarketCount: number;
  sourceSubmissionCount: number;
  baseFingerprint: string;
  calculationSource: "finalized" | "live" | "live_fallback" | "no_data";
  adjustment: IppGmAdjustmentEvent | null;
  adjustmentIsStale: boolean;
};

type MarketSample = {
  redPeriodId: string;
  marketId: string;
  gmUserId: string | null;
  marketIpp: number;
  sourceSubmissionCount: number;
  contributingQuestionCount: number;
  isFinalized: boolean;
};

type AdjustmentSqlRow = {
  id: string;
  revision_number: string | number;
  request_id: string;
  gm_user_id: string;
  red_period_id: string;
  event_type: "set" | "clear";
  corrected_ipp: string | number | null;
  base_calculated_ipp: string | number;
  base_sample_count: number;
  base_fingerprint: string;
  reason: string;
  created_by_user_id: string;
  created_by_name: string;
  created_at: string | Date;
};

type FinalizedSqlRow = {
  red_period_id: string | null;
  red_period_start: string;
  market_id: string;
  market_ipp: string | number;
  source_submission_count: number;
  contributing_question_count: number;
  gm_user_id: string | null;
};

let adjustmentTableReady: boolean | null = null;

function toNumber(value: unknown): number {
  const parsed = typeof value === "number" ? value : Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function roundIpp(value: number): number {
  return Number(value.toFixed(4));
}

function mapAdjustmentEvent(row: AdjustmentSqlRow): IppGmAdjustmentEvent {
  return {
    id: row.id,
    revisionNumber: Number(row.revision_number),
    requestId: row.request_id,
    gmUserId: row.gm_user_id,
    redPeriodId: row.red_period_id,
    eventType: row.event_type,
    correctedIpp: row.corrected_ipp == null ? null : toNumber(row.corrected_ipp),
    baseCalculatedIpp: toNumber(row.base_calculated_ipp),
    baseSampleCount: Number(row.base_sample_count ?? 0),
    baseFingerprint: row.base_fingerprint,
    reason: row.reason,
    createdByUserId: row.created_by_user_id,
    createdByName: row.created_by_name,
    createdAt: row.created_at instanceof Date ? row.created_at.toISOString() : new Date(row.created_at).toISOString(),
  };
}

function fingerprintBase(input: {
  gmUserId: string;
  redPeriodId: string;
  samples: MarketSample[];
}): string {
  const evidence = input.samples
    .slice()
    .sort((left, right) => left.marketId.localeCompare(right.marketId))
    .map((sample) => [
      sample.marketId,
      roundIpp(sample.marketIpp),
      sample.sourceSubmissionCount,
      sample.contributingQuestionCount,
    ]);
  return createHash("sha256")
    .update(JSON.stringify({ version: 1, gmUserId: input.gmUserId, redPeriodId: input.redPeriodId, evidence }))
    .digest("hex");
}

export async function isIppAdjustmentTableReady(): Promise<boolean> {
  if (adjustmentTableReady !== null) return adjustmentTableReady;
  const rows = await pgSql<{ table_name: string | null }[]>`
    select to_regclass('public.ipp_gm_redmonth_adjustment_events')::text as table_name
  `;
  adjustmentTableReady = Boolean(rows[0]?.table_name);
  return adjustmentTableReady;
}

export async function loadIppPeriodCatalog(options?: { throughCurrent?: boolean }): Promise<IppPeriodMeta[]> {
  const rows = await db
    .select({
      id: redMonthPeriods.id,
      redYear: redMonthPeriods.redYear,
      periodIndex: redMonthPeriods.periodIndex,
      label: redMonthPeriods.label,
      startDate: redMonthPeriods.startDate,
      endDate: redMonthPeriods.endDate,
    })
    .from(redMonthPeriods)
    .where(eq(redMonthPeriods.isDeleted, false))
    .orderBy(asc(redMonthPeriods.startDate));
  if (!options?.throughCurrent) return rows;
  const current = redMonthPeriodToYmd(await resolveCurrentRedPeriod(new Date()));
  return rows.filter((row) => row.startDate <= current.startYmd);
}

async function loadMarketSamples(periods: IppPeriodMeta[]): Promise<MarketSample[]> {
  if (periods.length === 0) return [];
  const periodIds = periods.map((period) => period.id);
  const periodStarts = periods.map((period) => period.startDate);
  const current = redMonthPeriodToYmd(await resolveCurrentRedPeriod(new Date()));
  const computed = await computeMarketIppSummariesForRedPeriods({ redPeriodIds: periodIds });
  const finalized = await pgSql<FinalizedSqlRow[]>`
    select
      r.red_period_id::text,
      r.red_period_start::text,
      r.market_id::text,
      r.market_ipp,
      r.source_submission_count,
      r.contributing_question_count,
      latest.gm_user_id::text
    from ipp_market_redmonth_results r
    left join lateral (
      select vs.gm_user_id
      from visit_sessions vs
      where vs.market_id = r.market_id
        and vs.is_deleted = false
        and vs.status = 'submitted'
        and vs.submitted_at is not null
        and vs.submitted_at >= (r.red_period_start::timestamp at time zone 'Europe/Vienna')
        and vs.submitted_at < ((r.red_period_end + 1)::timestamp at time zone 'Europe/Vienna')
      order by vs.submitted_at desc
      limit 1
    ) latest on true
    where r.is_deleted = false
      and r.is_finalized = true
      and r.red_period_start::text = any(${periodStarts}::text[])
  `;

  const periodById = new Map(periods.map((period) => [period.id, period]));
  const periodByStart = new Map(periods.map((period) => [period.startDate, period]));
  const byKey = new Map<string, MarketSample>();
  for (const summary of computed.values()) {
    byKey.set(`${summary.redPeriodId}::${summary.marketId}`, {
      redPeriodId: summary.redPeriodId,
      marketId: summary.marketId,
      gmUserId: summary.latestGmUserId,
      marketIpp: roundIpp(summary.marketIpp),
      sourceSubmissionCount: summary.sourceSubmissionCount,
      contributingQuestionCount: summary.contributingQuestionCount,
      isFinalized: false,
    });
  }
  for (const row of finalized) {
    const period = (row.red_period_id ? periodById.get(row.red_period_id) : undefined)
      ?? periodByStart.get(row.red_period_start);
    if (!period || period.startDate >= current.startYmd) continue;
    byKey.set(`${period.id}::${row.market_id}`, {
      redPeriodId: period.id,
      marketId: row.market_id,
      gmUserId: row.gm_user_id,
      marketIpp: roundIpp(toNumber(row.market_ipp)),
      sourceSubmissionCount: Number(row.source_submission_count ?? 0),
      contributingQuestionCount: Number(row.contributing_question_count ?? 0),
      isFinalized: true,
    });
  }
  return Array.from(byKey.values());
}

async function loadLatestAdjustmentEvents(gmUserIds: string[], redPeriodIds: string[]) {
  const result = new Map<string, IppGmAdjustmentEvent>();
  if (gmUserIds.length === 0 || redPeriodIds.length === 0 || !(await isIppAdjustmentTableReady())) return result;
  const rows = await pgSql<AdjustmentSqlRow[]>`
    select distinct on (event.gm_user_id, event.red_period_id)
      event.id,
      event.revision_number,
      event.request_id,
      event.gm_user_id::text,
      event.red_period_id::text,
      event.event_type::text,
      event.corrected_ipp,
      event.base_calculated_ipp,
      event.base_sample_count,
      event.base_fingerprint,
      event.reason,
      event.created_by_user_id::text,
      concat_ws(' ', actor.first_name, actor.last_name) as created_by_name,
      event.created_at
    from ipp_gm_redmonth_adjustment_events event
    join users actor on actor.id = event.created_by_user_id
    where event.gm_user_id = any(${gmUserIds}::uuid[])
      and event.red_period_id = any(${redPeriodIds}::uuid[])
    order by event.gm_user_id, event.red_period_id, event.revision_number desc
  `;
  for (const row of rows) {
    result.set(`${row.gm_user_id}::${row.red_period_id}`, mapAdjustmentEvent(row));
  }
  return result;
}

export async function loadEffectiveGmIppPeriods(input: {
  redPeriodIds: string[];
  gmUserIds?: string[];
  includeEmptyGms?: boolean;
}): Promise<EffectiveGmIppPeriodRow[]> {
  const requestedPeriodIds = Array.from(new Set(input.redPeriodIds));
  if (requestedPeriodIds.length === 0) return [];
  const catalog = (await loadIppPeriodCatalog()).filter((period) => requestedPeriodIds.includes(period.id));
  const samples = await loadMarketSamples(catalog);
  const requestedGmIds = input.gmUserIds ? new Set(input.gmUserIds) : null;
  const gmRows = await db
    .select({ id: users.id, firstName: users.firstName, lastName: users.lastName, region: users.region })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)));
  const eligibleGms = gmRows.filter((gm) => !requestedGmIds || requestedGmIds.has(gm.id));
  const gmById = new Map(eligibleGms.map((gm) => [gm.id, gm]));
  const samplesByKey = new Map<string, MarketSample[]>();
  for (const sample of samples) {
    if (!sample.gmUserId || !gmById.has(sample.gmUserId)) continue;
    const key = `${sample.gmUserId}::${sample.redPeriodId}`;
    const bucket = samplesByKey.get(key) ?? [];
    bucket.push(sample);
    samplesByKey.set(key, bucket);
  }
  const latestEvents = await loadLatestAdjustmentEvents(
    eligibleGms.map((gm) => gm.id),
    catalog.map((period) => period.id),
  );
  const current = redMonthPeriodToYmd(await resolveCurrentRedPeriod(new Date()));
  const output: EffectiveGmIppPeriodRow[] = [];
  for (const period of catalog) {
    for (const gm of eligibleGms) {
      const key = `${gm.id}::${period.id}`;
      const gmSamples = samplesByKey.get(key) ?? [];
      const latestEvent = latestEvents.get(key) ?? null;
      if (!input.includeEmptyGms && gmSamples.length === 0 && !latestEvent) continue;
      const positive = gmSamples.filter((sample) => sample.marketIpp > 0);
      const calculatedIpp = positive.length > 0
        ? roundIpp(positive.reduce((sum, sample) => sum + sample.marketIpp, 0) / positive.length)
        : 0;
      const baseFingerprint = fingerprintBase({ gmUserId: gm.id, redPeriodId: period.id, samples: gmSamples });
      const activeAdjustment = latestEvent?.eventType === "set" ? latestEvent : null;
      const effectiveIpp = activeAdjustment?.correctedIpp ?? calculatedIpp;
      const finalizedCount = gmSamples.filter((sample) => sample.isFinalized).length;
      const calculationSource = gmSamples.length === 0
        ? "no_data"
        : period.startDate >= current.startYmd
          ? "live"
          : finalizedCount === gmSamples.length
            ? "finalized"
            : "live_fallback";
      output.push({
        gmUserId: gm.id,
        gmName: `${gm.firstName} ${gm.lastName}`.trim(),
        region: gm.region ?? "",
        redPeriodId: period.id,
        redPeriodLabel: period.label,
        redPeriodYear: period.redYear,
        periodIndex: period.periodIndex,
        periodStart: period.startDate,
        periodEnd: period.endDate,
        calculatedIpp,
        effectiveIpp: roundIpp(effectiveIpp),
        difference: roundIpp(effectiveIpp - calculatedIpp),
        marketSampleCount: positive.length,
        zeroOrUnscoredMarketCount: gmSamples.length - positive.length,
        sourceSubmissionCount: gmSamples.reduce((sum, sample) => sum + sample.sourceSubmissionCount, 0),
        baseFingerprint,
        calculationSource,
        adjustment: activeAdjustment,
        adjustmentIsStale: Boolean(activeAdjustment && activeAdjustment.baseFingerprint !== baseFingerprint),
      });
    }
  }
  return output.sort((left, right) =>
    right.periodStart.localeCompare(left.periodStart) || left.gmName.localeCompare(right.gmName, "de"));
}

export async function loadIppAdjustmentHistory(input: { gmUserId: string; redPeriodId: string }) {
  if (!(await isIppAdjustmentTableReady())) return [];
  const rows = await pgSql<AdjustmentSqlRow[]>`
    select
      event.id,
      event.revision_number,
      event.request_id,
      event.gm_user_id::text,
      event.red_period_id::text,
      event.event_type::text,
      event.corrected_ipp,
      event.base_calculated_ipp,
      event.base_sample_count,
      event.base_fingerprint,
      event.reason,
      event.created_by_user_id::text,
      concat_ws(' ', actor.first_name, actor.last_name) as created_by_name,
      event.created_at
    from ipp_gm_redmonth_adjustment_events event
    join users actor on actor.id = event.created_by_user_id
    where event.gm_user_id = ${input.gmUserId}
      and event.red_period_id = ${input.redPeriodId}
    order by event.revision_number desc
  `;
  return rows.map(mapAdjustmentEvent);
}

export class IppAdjustmentConflictError extends Error {
  constructor(public readonly code: string, message: string) {
    super(message);
    this.name = "IppAdjustmentConflictError";
  }
}

async function loadCurrentBase(gmUserId: string, redPeriodId: string) {
  const [row] = await loadEffectiveGmIppPeriods({
    redPeriodIds: [redPeriodId],
    gmUserIds: [gmUserId],
    includeEmptyGms: true,
  });
  if (!row) throw new IppAdjustmentConflictError("ipp_adjustment_target_not_found", "GL oder RED-Monat wurde nicht gefunden.");
  return row;
}

export async function appendIppAdjustmentEvent(input: {
  requestId: string;
  gmUserId: string;
  redPeriodId: string;
  eventType: "set" | "clear";
  correctedIpp: number | null;
  reason: string;
  createdByUserId: string;
  expectedBaseFingerprint: string;
  expectedLatestRevisionNumber: number | null;
}): Promise<EffectiveGmIppPeriodRow> {
  if (!(await isIppAdjustmentTableReady())) {
    throw new IppAdjustmentConflictError("ipp_adjustment_schema_not_ready", "Die IPP-Korrekturtabelle ist noch nicht verfuegbar.");
  }
  const base = await loadCurrentBase(input.gmUserId, input.redPeriodId);
  if (base.baseFingerprint !== input.expectedBaseFingerprint) {
    throw new IppAdjustmentConflictError("ipp_adjustment_base_changed", "Die berechnete IPP-Basis hat sich geaendert. Bitte neu laden und erneut pruefen.");
  }
  if (input.eventType === "set" && base.marketSampleCount <= 0) {
    throw new IppAdjustmentConflictError("ipp_adjustment_no_samples", "Ohne mindestens einen bewerteten Markt kann kein GL-IPP korrigiert werden.");
  }

  await pgSql.begin(async (transaction) => {
    await transaction`select pg_advisory_xact_lock(hashtextextended(${`ipp-gm:${input.gmUserId}:${input.redPeriodId}`}, 0))`;
    const existingRequest = await transaction<AdjustmentSqlRow[]>`
      select event.*, concat_ws(' ', actor.first_name, actor.last_name) as created_by_name
      from ipp_gm_redmonth_adjustment_events event
      join users actor on actor.id = event.created_by_user_id
      where event.request_id = ${input.requestId}
      limit 1
    `;
    if (existingRequest[0]) {
      const existing = mapAdjustmentEvent(existingRequest[0]);
      if (
        existing.gmUserId !== input.gmUserId
        || existing.redPeriodId !== input.redPeriodId
        || existing.eventType !== input.eventType
        || existing.createdByUserId !== input.createdByUserId
      ) {
        throw new IppAdjustmentConflictError("ipp_adjustment_request_reused", "Diese Request-ID wurde bereits fuer eine andere Korrektur verwendet.");
      }
      return;
    }
    const latest = await transaction<{ revision_number: string | number; event_type: "set" | "clear" }[]>`
      select revision_number, event_type::text
      from ipp_gm_redmonth_adjustment_events
      where gm_user_id = ${input.gmUserId} and red_period_id = ${input.redPeriodId}
      order by revision_number desc
      limit 1
    `;
    const latestRevision = latest[0] ? Number(latest[0].revision_number) : null;
    if (latestRevision !== input.expectedLatestRevisionNumber) {
      throw new IppAdjustmentConflictError("ipp_adjustment_revision_changed", "Die Korrektur wurde zwischenzeitlich geaendert. Bitte neu laden.");
    }
    if (input.eventType === "clear" && latest[0]?.event_type !== "set") {
      throw new IppAdjustmentConflictError("ipp_adjustment_not_active", "Es gibt keine aktive Korrektur zum Zuruecksetzen.");
    }
    await transaction`
      insert into ipp_gm_redmonth_adjustment_events (
        request_id, gm_user_id, red_period_id, event_type, corrected_ipp,
        base_calculated_ipp, base_sample_count, base_fingerprint, reason, created_by_user_id
      ) values (
        ${input.requestId}, ${input.gmUserId}, ${input.redPeriodId}, ${input.eventType}, ${input.correctedIpp},
        ${base.calculatedIpp}, ${base.marketSampleCount}, ${base.baseFingerprint}, ${input.reason.trim()}, ${input.createdByUserId}
      )
    `;
  });

  return loadCurrentBase(input.gmUserId, input.redPeriodId);
}
