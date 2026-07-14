import { and, asc, desc, eq, inArray, isNotNull, notInArray, sql } from "drizzle-orm";
import { db, sql as pgSql } from "./db.js";
import { DEFAULT_TIMEZONE, toYmdInTimezone } from "./day-session.js";
import { recomputeGmKpiCache } from "./gm-kpi-cache.js";
import { logAction, logger, serializeError, startActionTimer } from "./logger.js";
import { calculateWaveReward, type PraemienPillarRewardInput } from "./praemien-rewards.js";
import { redMonthPeriodToYmd, resolveCurrentRedPeriod, resolveRedPeriodForDate } from "./red-month-periods.js";
import {
  markets,
  praemienGmWaveContributions,
  praemienGmWavePillarTotals,
  praemienGmWaveTotals,
  praemienWaveFlexScores,
  praemienWavePillarMetrics,
  praemienWavePillars,
  praemienWavePillarTierConditions,
  praemienWavePillarTiers,
  praemienWaveQualityScores,
  praemienWaveSources,
  praemienWaveThresholds,
  praemienWaves,
  visitAnswerOptions,
  visitAnswers,
  visitSessionQuestions,
  visitSessionSections,
  visitSessions,
} from "./schema.js";

type BonusFinalizeResult = {
  applied: boolean;
  waveId: string | null;
};

export type BonusWaveRecomputeResult = {
  applied: boolean;
  waveId: string;
  processedSessions: number;
  affectedGmUserIds: string[];
  skippedReason?: "tables_missing" | "wave_not_found" | "wave_not_active";
};

type BonusDbExecutor = {
  select: typeof db.select;
  insert: typeof db.insert;
  update: typeof db.update;
  delete: typeof db.delete;
};

let checkedBonusTableReadiness = false;
let hasBonusTables = false;

async function ensureBonusTablesReady(): Promise<boolean> {
  if (checkedBonusTableReadiness) return hasBonusTables;
  const result = await pgSql<{
    praemien_waves: string | null;
    praemien_wave_sources: string | null;
    praemien_wave_pillar_metrics: string | null;
    praemien_wave_pillar_tiers: string | null;
    praemien_wave_pillar_tier_conditions: string | null;
    praemien_wave_thresholds: string | null;
    praemien_wave_quality_scores: string | null;
    praemien_wave_flex_scores: string | null;
    praemien_gm_wave_contributions: string | null;
    praemien_gm_wave_pillar_totals: string | null;
    praemien_gm_wave_totals: string | null;
  }[]>`
    select
      to_regclass('public.praemien_waves') as praemien_waves,
      to_regclass('public.praemien_wave_sources') as praemien_wave_sources,
      to_regclass('public.praemien_wave_pillar_metrics') as praemien_wave_pillar_metrics,
      to_regclass('public.praemien_wave_pillar_tiers') as praemien_wave_pillar_tiers,
      to_regclass('public.praemien_wave_pillar_tier_conditions') as praemien_wave_pillar_tier_conditions,
      to_regclass('public.praemien_wave_thresholds') as praemien_wave_thresholds,
      to_regclass('public.praemien_wave_quality_scores') as praemien_wave_quality_scores,
      to_regclass('public.praemien_wave_flex_scores') as praemien_wave_flex_scores,
      to_regclass('public.praemien_gm_wave_contributions') as praemien_gm_wave_contributions,
      to_regclass('public.praemien_gm_wave_pillar_totals') as praemien_gm_wave_pillar_totals,
      to_regclass('public.praemien_gm_wave_totals') as praemien_gm_wave_totals
  `;
  const row = result[0];
  hasBonusTables = Boolean(
    row?.praemien_waves &&
    row?.praemien_wave_sources &&
    row?.praemien_wave_pillar_metrics &&
    row?.praemien_wave_pillar_tiers &&
    row?.praemien_wave_pillar_tier_conditions &&
    row?.praemien_wave_thresholds &&
    row?.praemien_wave_quality_scores &&
    row?.praemien_wave_flex_scores &&
    row?.praemien_gm_wave_contributions &&
    row?.praemien_gm_wave_pillar_totals &&
    row?.praemien_gm_wave_totals,
  );
  // Cache only positive readiness so a running dev process can recover
  // automatically after tables are pushed/migrated.
  checkedBonusTableReadiness = hasBonusTables;
  return hasBonusTables;
}

type GmBonusSummaryGoal = {
  pillarId: string;
  name: string;
  color: string;
  points: number;
  maxPoints: number;
  percent: number;
  isManual: boolean;
  isPending: boolean;
  targetPoints: number | null;
  rewardEur: number;
  achieved: boolean;
  earnedRewardEur: number;
  maxRewardEur: number;
  metricValues: Record<string, number>;
  achievedTierLabels: string[];
  nextTierLabel: string | null;
};

export type GmBonusSummary = {
  hasActiveWave: boolean;
  waveId: string | null;
  waveName: string | null;
  year: number | null;
  quarter: number | null;
  startDate: string | null;
  endDate: string | null;
  rewardModel: "global_thresholds" | "pillar_targets" | "pillar_tiers" | null;
  totalPoints: number;
  totalMaxPoints: number;
  currentRewardEur: number;
  fullRewardEur: number;
  goals: GmBonusSummaryGoal[];
  thresholds: Array<{ label: string; minPoints: number; rewardEur: number }>;
};

function normalizeNumber(input: unknown): number {
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

function parseRawAnswerKeys(valueJson: Record<string, unknown> | null): string[] {
  const raw = (valueJson as { raw?: unknown } | null | undefined)?.raw;
  if (typeof raw === "string") return [raw];
  if (Array.isArray(raw)) return raw.filter((entry): entry is string => typeof entry === "string");
  if (raw && typeof raw === "object") {
    const top = typeof (raw as { sel?: unknown }).sel === "string" ? [(raw as { sel: string }).sel] : [];
    const subs = Array.isArray((raw as { subs?: unknown }).subs)
      ? ((raw as { subs: unknown[] }).subs.filter((entry): entry is string => typeof entry === "string"))
      : [];
    return [...top, ...subs];
  }
  return [];
}

function normalizeScoreToken(input: string): string {
  const token = input.trim().toLowerCase();
  if (token === "ja" || token === "yes" || token === "true" || token === "1") return "__yes__";
  if (token === "nein" || token === "no" || token === "false" || token === "0") return "__no__";
  return token;
}

function computeSourceContribution(input: {
  answer: typeof visitAnswers.$inferSelect;
  options: Array<typeof visitAnswerOptions.$inferSelect>;
  sourceScoreKey: string;
  boniValue: number;
  isFactorMode: boolean;
}): number {
  const answer = input.answer;
  if (answer.answerStatus !== "answered" || !answer.isValid) return 0;

  const questionType = String(answer.questionType);
  if (input.isFactorMode || input.sourceScoreKey === "__value__") {
    if (!["numeric", "slider", "likert"].includes(questionType)) return 0;
    const numeric = normalizeNumber(answer.valueNumber ?? answer.valueText);
    return Number((numeric * input.boniValue).toFixed(4));
  }

  const keyCandidates = normalizeUnique([
    ...(typeof answer.valueText === "string" ? [answer.valueText] : []),
    ...input.options.map((entry) => entry.optionValue),
    ...parseRawAnswerKeys((answer.valueJson as Record<string, unknown> | null) ?? null),
  ]);
  const normalizedSourceKey = normalizeScoreToken(input.sourceScoreKey);
  const matched = keyCandidates.some((candidate) => normalizeScoreToken(candidate) === normalizedSourceKey);
  if (!matched) return 0;
  return Number(input.boniValue.toFixed(4));
}

function normalizePillarName(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z]/g, "");
}

function isFlexPillarName(value: string): boolean {
  return normalizePillarName(value).includes("flexziel");
}

function isQualityPillarName(value: string): boolean {
  const normalized = normalizePillarName(value);
  return normalized.includes("qualitatsziele") || normalized.includes("qualitaetsziele") || normalized.includes("qualitat");
}

type PillarMetricRow = typeof praemienWavePillarMetrics.$inferSelect;
type PillarTierRow = typeof praemienWavePillarTiers.$inferSelect;
type PillarConditionRow = typeof praemienWavePillarTierConditions.$inferSelect;

function resolvePillarMetricValues(input: {
  metrics: PillarMetricRow[];
  points: number;
  maxPoints: number;
  quality: {
    zeiterfassung: number;
    reporting: number;
    accuracy: number;
    totalPoints: number;
  } | null;
  flex: {
    totalPoints: number;
    componentValues: Record<string, number>;
  } | null;
}): Record<string, number> {
  const result: Record<string, number> = { points: input.points };
  for (const metric of input.metrics) {
    let value = 0;
    if (metric.valueSource === "contribution_points") value = input.points;
    if (metric.valueSource === "contribution_percent") {
      value = input.maxPoints > 0 ? (input.points / input.maxPoints) * 100 : 0;
    }
    if (metric.valueSource === "quality_zeiterfassung") value = input.quality?.zeiterfassung ?? 0;
    if (metric.valueSource === "quality_reporting") value = input.quality?.reporting ?? 0;
    if (metric.valueSource === "quality_accuracy") value = input.quality?.accuracy ?? 0;
    if (metric.valueSource === "quality_average") value = input.quality?.totalPoints ?? 0;
    if (metric.valueSource === "flex_total_points") value = input.flex?.totalPoints ?? 0;
    if (metric.valueSource === "flex_component") {
      value = metric.sourceKey ? normalizeNumber(input.flex?.componentValues?.[metric.sourceKey]) : 0;
    }
    result[metric.key] = Number(value.toFixed(4));
  }
  return result;
}

function buildPillarTierInputs(input: {
  pillarId: string;
  tiers: PillarTierRow[];
  conditions: PillarConditionRow[];
  metricById: Map<string, PillarMetricRow>;
}) {
  const conditionsByTierId = new Map<string, PillarConditionRow[]>();
  for (const condition of input.conditions) {
    if (condition.pillarId !== input.pillarId) continue;
    const list = conditionsByTierId.get(condition.tierId) ?? [];
    list.push(condition);
    conditionsByTierId.set(condition.tierId, list);
  }
  return input.tiers
    .filter((tier) => tier.pillarId === input.pillarId)
    .map((tier) => ({
      tierId: tier.id,
      label: tier.label,
      orderIndex: tier.orderIndex,
      rewardEur: normalizeNumber(tier.rewardEur),
      conditions: (conditionsByTierId.get(tier.id) ?? []).map((condition) => ({
        metricKey: input.metricById.get(condition.metricId)?.key ?? "",
        operator: condition.operator as "gte" | "lte" | "eq",
        thresholdValue: normalizeNumber(condition.thresholdValue),
      })).filter((condition) => condition.metricKey.length > 0),
    }));
}

type GmWaveRewardSnapshot = {
  totalPoints: number;
  currentRewardEur: number;
  fullRewardEur: number;
  contributionCount: number;
  lastContributionAt: Date | null;
  pillarResults: ReturnType<typeof calculateWaveReward>["pillarResults"];
};

async function calculateGmWaveRewardSnapshot(
  executor: BonusDbExecutor,
  wave: typeof praemienWaves.$inferSelect,
  gmUserId: string,
): Promise<GmWaveRewardSnapshot> {
  const [pillars, thresholds, metrics, tiers, tierConditions, sourceMaximumRows, contributionRows, qualityRows, flexRows] = await Promise.all([
    executor
      .select()
      .from(praemienWavePillars)
      .where(and(eq(praemienWavePillars.waveId, wave.id), eq(praemienWavePillars.isDeleted, false)))
      .orderBy(asc(praemienWavePillars.orderIndex)),
    executor
      .select({
        minPoints: praemienWaveThresholds.minPoints,
        rewardEur: praemienWaveThresholds.rewardEur,
      })
      .from(praemienWaveThresholds)
      .where(and(eq(praemienWaveThresholds.waveId, wave.id), eq(praemienWaveThresholds.isDeleted, false))),
    executor
      .select()
      .from(praemienWavePillarMetrics)
      .where(and(eq(praemienWavePillarMetrics.waveId, wave.id), eq(praemienWavePillarMetrics.isDeleted, false))),
    executor
      .select()
      .from(praemienWavePillarTiers)
      .where(and(eq(praemienWavePillarTiers.waveId, wave.id), eq(praemienWavePillarTiers.isDeleted, false))),
    executor
      .select()
      .from(praemienWavePillarTierConditions)
      .where(and(eq(praemienWavePillarTierConditions.waveId, wave.id), eq(praemienWavePillarTierConditions.isDeleted, false))),
    executor
      .select({
        pillarId: praemienWaveSources.pillarId,
        maxPoints: sql<number>`coalesce(sum(${praemienWaveSources.boniValue}), 0)`,
      })
      .from(praemienWaveSources)
      .where(and(eq(praemienWaveSources.waveId, wave.id), eq(praemienWaveSources.isDeleted, false)))
      .groupBy(praemienWaveSources.pillarId),
    executor
      .select({
        pillarId: praemienWaveSources.pillarId,
        points: sql<number>`coalesce(sum(${praemienGmWaveContributions.appliedValue}), 0)`,
        contributionCount: sql<number>`count(*)`,
        lastContributionAt: sql<Date | string | null>`max(${praemienGmWaveContributions.submittedAt})`,
      })
      .from(praemienGmWaveContributions)
      .innerJoin(
        praemienWaveSources,
        and(
          eq(praemienWaveSources.id, praemienGmWaveContributions.sourceId),
          eq(praemienWaveSources.waveId, wave.id),
          eq(praemienWaveSources.isDeleted, false),
        ),
      )
      .where(and(eq(praemienGmWaveContributions.waveId, wave.id), eq(praemienGmWaveContributions.gmUserId, gmUserId)))
      .groupBy(praemienWaveSources.pillarId),
    executor
      .select({
        zeiterfassung: praemienWaveQualityScores.zeiterfassung,
        reporting: praemienWaveQualityScores.reporting,
        accuracy: praemienWaveQualityScores.accuracy,
        totalPoints: praemienWaveQualityScores.totalPoints,
      })
      .from(praemienWaveQualityScores)
      .where(and(
        eq(praemienWaveQualityScores.waveId, wave.id),
        eq(praemienWaveQualityScores.gmUserId, gmUserId),
        eq(praemienWaveQualityScores.isDeleted, false),
      ))
      .limit(1),
    executor
      .select({
        totalPoints: praemienWaveFlexScores.totalPoints,
        componentValues: praemienWaveFlexScores.componentValues,
      })
      .from(praemienWaveFlexScores)
      .where(and(
        eq(praemienWaveFlexScores.waveId, wave.id),
        eq(praemienWaveFlexScores.gmUserId, gmUserId),
        eq(praemienWaveFlexScores.isDeleted, false),
      ))
      .limit(1),
  ]);

  const pointsByPillar = new Map(contributionRows.map((row) => [row.pillarId, normalizeNumber(row.points)]));
  const maxPointsByPillar = new Map(sourceMaximumRows.map((row) => [row.pillarId, normalizeNumber(row.maxPoints)]));
  const metricsByPillarId = new Map<string, PillarMetricRow[]>();
  for (const metric of metrics) {
    const list = metricsByPillarId.get(metric.pillarId) ?? [];
    list.push(metric);
    metricsByPillarId.set(metric.pillarId, list);
  }
  const metricById = new Map(metrics.map((metric) => [metric.id, metric]));
  const pillarInputs: PraemienPillarRewardInput[] = pillars.map((pillar) => {
    let points = pointsByPillar.get(pillar.id) ?? 0;
    if (isQualityPillarName(pillar.name)) points = normalizeNumber(qualityRows[0]?.totalPoints);
    if (isFlexPillarName(pillar.name)) points = normalizeNumber(flexRows[0]?.totalPoints);
    const metricValues = resolvePillarMetricValues({
      metrics: metricsByPillarId.get(pillar.id) ?? [],
      points,
      maxPoints: maxPointsByPillar.get(pillar.id) ?? 0,
      quality: qualityRows[0] ?? null,
      flex: flexRows[0] ? {
        totalPoints: flexRows[0].totalPoints,
        componentValues: flexRows[0].componentValues ?? {},
      } : null,
    });
    return {
      pillarId: pillar.id,
      points: Number(points.toFixed(4)),
      targetPoints: pillar.targetPoints == null ? null : normalizeNumber(pillar.targetPoints),
      rewardEur: normalizeNumber(pillar.rewardEur),
      payoutMode: pillar.payoutMode as "highest_tier" | "sum_earned_tiers",
      maxRewardEur: normalizeNumber(pillar.maxRewardEur),
      metricValues,
      tiers: buildPillarTierInputs({
        pillarId: pillar.id,
        tiers,
        conditions: tierConditions,
        metricById,
      }),
    };
  });
  const totalPoints = Number(pillarInputs.reduce((sum, pillar) => sum + pillar.points, 0).toFixed(4));
  const reward = calculateWaveReward({
    rewardModel: wave.rewardModel,
    totalPoints,
    thresholds: thresholds.map((threshold) => ({
      minPoints: normalizeNumber(threshold.minPoints),
      rewardEur: normalizeNumber(threshold.rewardEur),
    })),
    pillars: pillarInputs,
  });
  const lastContributionAt = contributionRows.reduce<Date | null>((latest, row) => {
    if (row.lastContributionAt == null) return latest;
    const candidate = row.lastContributionAt instanceof Date ? row.lastContributionAt : new Date(row.lastContributionAt);
    if (Number.isNaN(candidate.getTime())) return latest;
    return !latest || candidate > latest ? candidate : latest;
  }, null);

  return {
    totalPoints,
    currentRewardEur: reward.currentRewardEur,
    fullRewardEur: reward.fullRewardEur,
    contributionCount: contributionRows.reduce((sum, row) => sum + Number(row.contributionCount ?? 0), 0),
    lastContributionAt,
    pillarResults: reward.pillarResults,
  };
}

async function persistGmWaveRewardSnapshot(
  executor: BonusDbExecutor,
  wave: typeof praemienWaves.$inferSelect,
  gmUserId: string,
  calculatedAt: Date,
): Promise<GmWaveRewardSnapshot> {
  const snapshot = await calculateGmWaveRewardSnapshot(executor, wave, gmUserId);
  const pillarIds = snapshot.pillarResults.map((pillar) => pillar.pillarId);
  if (pillarIds.length === 0) {
    await executor
      .delete(praemienGmWavePillarTotals)
      .where(and(eq(praemienGmWavePillarTotals.waveId, wave.id), eq(praemienGmWavePillarTotals.gmUserId, gmUserId)));
  } else {
    await executor
      .delete(praemienGmWavePillarTotals)
      .where(and(
        eq(praemienGmWavePillarTotals.waveId, wave.id),
        eq(praemienGmWavePillarTotals.gmUserId, gmUserId),
        notInArray(praemienGmWavePillarTotals.pillarId, pillarIds),
      ));
    for (const pillar of snapshot.pillarResults) {
      await executor
        .insert(praemienGmWavePillarTotals)
        .values({
          waveId: wave.id,
          gmUserId,
          pillarId: pillar.pillarId,
          points: String(pillar.points),
          targetPoints: pillar.targetPoints == null ? null : String(pillar.targetPoints),
          rewardEur: String(pillar.rewardEur),
          earnedRewardEur: String(pillar.earnedRewardEur),
          goalAchieved: pillar.achieved,
          metricValues: pillar.metricValues,
          achievedTierIds: pillar.achievedTierIds,
          achievedTierLabels: pillar.achievedTierLabels,
          nextTierId: pillar.nextTierId,
          nextTierLabel: pillar.nextTierLabel,
          calculatedAt,
          createdAt: calculatedAt,
          updatedAt: calculatedAt,
        })
        .onConflictDoUpdate({
          target: [
            praemienGmWavePillarTotals.waveId,
            praemienGmWavePillarTotals.gmUserId,
            praemienGmWavePillarTotals.pillarId,
          ],
          set: {
            points: String(pillar.points),
            targetPoints: pillar.targetPoints == null ? null : String(pillar.targetPoints),
            rewardEur: String(pillar.rewardEur),
            earnedRewardEur: String(pillar.earnedRewardEur),
            goalAchieved: pillar.achieved,
            metricValues: pillar.metricValues,
            achievedTierIds: pillar.achievedTierIds,
            achievedTierLabels: pillar.achievedTierLabels,
            nextTierId: pillar.nextTierId,
            nextTierLabel: pillar.nextTierLabel,
            calculatedAt,
            updatedAt: calculatedAt,
          },
        });
    }
  }

  await executor
    .insert(praemienGmWaveTotals)
    .values({
      waveId: wave.id,
      gmUserId,
      totalPoints: String(snapshot.totalPoints),
      currentRewardEur: String(snapshot.currentRewardEur),
      contributionCount: snapshot.contributionCount,
      lastContributionAt: snapshot.lastContributionAt,
      createdAt: calculatedAt,
      updatedAt: calculatedAt,
    })
    .onConflictDoUpdate({
      target: [praemienGmWaveTotals.waveId, praemienGmWaveTotals.gmUserId],
      set: {
        totalPoints: String(snapshot.totalPoints),
        currentRewardEur: String(snapshot.currentRewardEur),
        contributionCount: snapshot.contributionCount,
        lastContributionAt: snapshot.lastContributionAt,
        updatedAt: calculatedAt,
      },
    });

  return snapshot;
}

async function loadActiveWaveForInstant(input: {
  at: Date;
  executor: BonusDbExecutor;
  fallbackTimezone?: string;
}) {
  const waves = await input.executor
    .select()
    .from(praemienWaves)
    .where(and(eq(praemienWaves.status, "active"), eq(praemienWaves.isDeleted, false)))
    .orderBy(desc(praemienWaves.updatedAt))
    .limit(250);

  const fallbackTimezone = input.fallbackTimezone?.trim() || DEFAULT_TIMEZONE;
  for (const wave of waves) {
    const waveTimezone = wave.timezone?.trim() || fallbackTimezone;
    const waveDateYmd = toYmdInTimezone(input.at, waveTimezone);
    if (wave.startDate <= waveDateYmd && wave.endDate >= waveDateYmd) {
      return wave;
    }
  }
  return null;
}

type FinalizeBonusInput = {
  sessionId: string;
  gmUserId: string;
  marketId: string;
  submittedAt: Date;
};

async function finalizeBonusForSubmittedVisitSessionWithExecutor(
  executor: BonusDbExecutor,
  input: FinalizeBonusInput,
): Promise<BonusFinalizeResult> {
  if (!(await ensureBonusTablesReady())) return { applied: false, waveId: null };
  const submittedAt = input.submittedAt;
  const redPeriod = await resolveRedPeriodForDate(submittedAt);
  const wave = await loadActiveWaveForInstant({
    at: submittedAt,
    executor,
    fallbackTimezone: redPeriod.timezone || DEFAULT_TIMEZONE,
  });
  if (!wave) return { applied: false, waveId: null };

  return finalizeBonusForSubmittedVisitSessionForWaveWithExecutor(executor, wave, redPeriod, input);
}

async function finalizeBonusForSubmittedVisitSessionForWaveWithExecutor(
  executor: BonusDbExecutor,
  wave: typeof praemienWaves.$inferSelect,
  redPeriod: Awaited<ReturnType<typeof resolveRedPeriodForDate>>,
  input: FinalizeBonusInput,
  persistSnapshot = true,
): Promise<BonusFinalizeResult> {
  const submittedAt = input.submittedAt;

  const [sources, marketRow] = await Promise.all([
    executor
      .select({
        sourceId: praemienWaveSources.id,
        waveId: praemienWaveSources.waveId,
        pillarId: praemienWaveSources.pillarId,
        sectionType: praemienWaveSources.sectionType,
        questionId: praemienWaveSources.questionId,
        scoreKey: praemienWaveSources.scoreKey,
        boniValue: praemienWaveSources.boniValue,
        isFactorMode: praemienWaveSources.isFactorMode,
        distributionFreqRule: praemienWaveSources.distributionFreqRule,
      })
      .from(praemienWaveSources)
      .where(and(eq(praemienWaveSources.waveId, wave.id), eq(praemienWaveSources.isDeleted, false))),
    executor
      .select({ visitFrequencyPerYear: markets.visitFrequencyPerYear })
      .from(markets)
      .where(eq(markets.id, input.marketId))
      .limit(1),
  ]);
  if (sources.length === 0) return { applied: false, waveId: wave.id };

  const visitFrequency = marketRow[0]?.visitFrequencyPerYear ?? 0;
  const filteredSources = sources.filter((source) => {
    if (!source.distributionFreqRule) return true;
    if (source.distributionFreqRule === "lt8") return visitFrequency <= 8;
    return visitFrequency >= 8;
  });
  if (filteredSources.length === 0) return { applied: false, waveId: wave.id };

  const sections = await executor
    .select({ id: visitSessionSections.id, section: visitSessionSections.section })
    .from(visitSessionSections)
    .where(and(eq(visitSessionSections.visitSessionId, input.sessionId), eq(visitSessionSections.isDeleted, false)));
  const sectionIds = sections.map((entry) => entry.id);
  if (sectionIds.length === 0) return { applied: false, waveId: wave.id };

  const questions = await executor
    .select({
      id: visitSessionQuestions.id,
      visitSessionSectionId: visitSessionQuestions.visitSessionSectionId,
      questionId: visitSessionQuestions.questionId,
    })
    .from(visitSessionQuestions)
    .where(and(inArray(visitSessionQuestions.visitSessionSectionId, sectionIds), eq(visitSessionQuestions.isDeleted, false)));
  const questionIds = questions.map((entry) => entry.id);
  if (questionIds.length === 0) return { applied: false, waveId: wave.id };

  const answers = await executor
    .select()
    .from(visitAnswers)
    .where(and(inArray(visitAnswers.visitSessionQuestionId, questionIds), eq(visitAnswers.isDeleted, false)));
  const answerIds = answers.map((entry) => entry.id);
  const options = answerIds.length === 0
    ? []
    : await executor
        .select()
        .from(visitAnswerOptions)
        .where(and(inArray(visitAnswerOptions.visitAnswerId, answerIds), eq(visitAnswerOptions.isDeleted, false)));

  const sectionById = new Map(sections.map((entry) => [entry.id, entry.section]));
  const questionBySectionAndId = new Map(
    questions.map((entry) => [`${sectionById.get(entry.visitSessionSectionId) ?? ""}:${entry.questionId}`, entry.id]),
  );
  const answerByQuestionRowId = new Map(answers.map((entry) => [entry.visitSessionQuestionId, entry]));
  const optionsByAnswerId = new Map<string, Array<typeof visitAnswerOptions.$inferSelect>>();
  for (const option of options) {
    const bucket = optionsByAnswerId.get(option.visitAnswerId) ?? [];
    bucket.push(option);
    optionsByAnswerId.set(option.visitAnswerId, bucket);
  }

  const redPeriodDates = redMonthPeriodToYmd(redPeriod);
  const redPeriodStart = redPeriodDates.startYmd;
  const redPeriodEnd = redPeriodDates.endYmd;

  const contributionPayload = filteredSources.flatMap((source) => {
    const questionRowId = questionBySectionAndId.get(`${source.sectionType}:${source.questionId}`);
    if (!questionRowId) return [];
    const answer = answerByQuestionRowId.get(questionRowId);
    if (!answer) return [];
    const appliedValue = computeSourceContribution({
      answer,
      options: optionsByAnswerId.get(answer.id) ?? [],
      sourceScoreKey: source.scoreKey,
      boniValue: normalizeNumber(source.boniValue),
      isFactorMode: source.isFactorMode,
    });
    return [{
      waveId: wave.id,
      gmUserId: input.gmUserId,
      marketId: input.marketId,
      sourceId: source.sourceId,
      pillarId: source.pillarId,
      visitSessionId: input.sessionId,
      redPeriodId: redPeriod.redPeriodId,
      redPeriodStart,
      redPeriodEnd,
      questionId: source.questionId,
      scoreKey: source.scoreKey,
      appliedValue: String(appliedValue),
      submittedAt,
      updatedAt: submittedAt,
      createdAt: submittedAt,
    }];
  });
  if (contributionPayload.length === 0) return { applied: false, waveId: wave.id };

  for (const row of contributionPayload) {
    await executor
      .insert(praemienGmWaveContributions)
      .values(row)
      .onConflictDoUpdate({
        target: [
          praemienGmWaveContributions.waveId,
          praemienGmWaveContributions.gmUserId,
          praemienGmWaveContributions.marketId,
          praemienGmWaveContributions.redPeriodStart,
          praemienGmWaveContributions.sourceId,
        ],
        set: {
          visitSessionId: row.visitSessionId,
          redPeriodEnd: row.redPeriodEnd,
          questionId: row.questionId,
          scoreKey: row.scoreKey,
          appliedValue: row.appliedValue,
          submittedAt: row.submittedAt,
          updatedAt: row.updatedAt,
        },
      });
  }

  if (persistSnapshot) {
    await persistGmWaveRewardSnapshot(executor, wave, input.gmUserId, submittedAt);
  }

  return { applied: true, waveId: wave.id };
}

export async function recomputeBonusWaveTx(
  executor: BonusDbExecutor,
  waveId: string,
): Promise<BonusWaveRecomputeResult> {
  if (!(await ensureBonusTablesReady())) {
    return { applied: false, waveId, processedSessions: 0, affectedGmUserIds: [], skippedReason: "tables_missing" };
  }

  const [wave] = await executor
    .select()
    .from(praemienWaves)
    .where(and(eq(praemienWaves.id, waveId), eq(praemienWaves.isDeleted, false)))
    .limit(1);
  if (!wave) {
    return { applied: false, waveId, processedSessions: 0, affectedGmUserIds: [], skippedReason: "wave_not_found" };
  }
  if (wave.status !== "active") {
    return { applied: false, waveId, processedSessions: 0, affectedGmUserIds: [], skippedReason: "wave_not_active" };
  }

  await executor.delete(praemienGmWaveContributions).where(eq(praemienGmWaveContributions.waveId, waveId));
  await executor.delete(praemienGmWavePillarTotals).where(eq(praemienGmWavePillarTotals.waveId, waveId));
  await executor.delete(praemienGmWaveTotals).where(eq(praemienGmWaveTotals.waveId, waveId));

  const activeSourceProbe = await executor
    .select({ id: praemienWaveSources.id })
    .from(praemienWaveSources)
    .where(and(eq(praemienWaveSources.waveId, waveId), eq(praemienWaveSources.isDeleted, false)))
    .limit(1);
  const timezone = wave.timezone?.trim() || DEFAULT_TIMEZONE;
  const sessions = activeSourceProbe.length === 0
    ? []
    : await executor
        .select({
          sessionId: visitSessions.id,
          gmUserId: visitSessions.gmUserId,
          marketId: visitSessions.marketId,
          submittedAt: visitSessions.submittedAt,
        })
        .from(visitSessions)
        .where(and(
          eq(visitSessions.status, "submitted"),
          eq(visitSessions.isDeleted, false),
          isNotNull(visitSessions.submittedAt),
          sql`(${visitSessions.submittedAt} at time zone ${timezone})::date >= ${wave.startDate}::date`,
          sql`(${visitSessions.submittedAt} at time zone ${timezone})::date <= ${wave.endDate}::date`,
        ))
        .orderBy(asc(visitSessions.submittedAt));

  const affectedGmUserIdSet = new Set<string>();
  for (const session of sessions) {
    if (!session.submittedAt) continue;
    const redPeriod = await resolveRedPeriodForDate(session.submittedAt);
    await finalizeBonusForSubmittedVisitSessionForWaveWithExecutor(executor, wave, redPeriod, {
      sessionId: session.sessionId,
      gmUserId: session.gmUserId,
      marketId: session.marketId,
      submittedAt: session.submittedAt,
    }, false);
    affectedGmUserIdSet.add(session.gmUserId);
  }

  const [qualityGmRows, flexGmRows] = await Promise.all([
    executor
      .select({ gmUserId: praemienWaveQualityScores.gmUserId })
      .from(praemienWaveQualityScores)
      .where(and(eq(praemienWaveQualityScores.waveId, waveId), eq(praemienWaveQualityScores.isDeleted, false))),
    executor
      .select({ gmUserId: praemienWaveFlexScores.gmUserId })
      .from(praemienWaveFlexScores)
      .where(and(eq(praemienWaveFlexScores.waveId, waveId), eq(praemienWaveFlexScores.isDeleted, false))),
  ]);
  for (const row of [...qualityGmRows, ...flexGmRows]) affectedGmUserIdSet.add(row.gmUserId);
  const recalculatedAt = new Date();
  for (const gmUserId of affectedGmUserIdSet) {
    await persistGmWaveRewardSnapshot(executor, wave, gmUserId, recalculatedAt);
  }

  return {
    applied: true,
    waveId,
    processedSessions: sessions.length,
    affectedGmUserIds: Array.from(affectedGmUserIdSet),
  };
}

export async function finalizeBonusForSubmittedVisitSessionTx(
  tx: BonusDbExecutor,
  input: FinalizeBonusInput,
): Promise<BonusFinalizeResult> {
  return finalizeBonusForSubmittedVisitSessionWithExecutor(tx, input);
}

export async function finalizeBonusForSubmittedVisitSession(input: FinalizeBonusInput): Promise<BonusFinalizeResult> {
  const startedAtNs = startActionTimer();
  const result = await db.transaction(async (tx) => finalizeBonusForSubmittedVisitSessionWithExecutor(tx, input));
  logAction("info", "bonus_finalizer_finalize_completed", {
    action: "bonus_finalize_submitted_visit",
    result: result.applied ? "success" : "skipped",
    startedAtNs,
    details: {
      sessionId: input.sessionId,
      gmUserId: input.gmUserId,
      marketId: input.marketId,
      waveId: result.waveId,
      applied: result.applied,
    },
  });
  try {
    await recomputeGmKpiCache(input.gmUserId);
  } catch (error) {
    logAction("error", "bonus_finalizer_gm_kpi_recompute_failed", {
      action: "bonus_finalize_submitted_visit",
      result: "failure",
      startedAtNs,
      details: {
        gmUserId: input.gmUserId,
        sessionId: input.sessionId,
      },
      error,
    });
  }
  return result;
}

export async function readGmActiveBonusSummary(gmUserId: string, now = new Date()): Promise<GmBonusSummary> {
  if (!(await ensureBonusTablesReady())) {
    return {
      hasActiveWave: false,
      waveId: null,
      waveName: null,
      year: null,
      quarter: null,
      startDate: null,
      endDate: null,
      rewardModel: null,
      totalPoints: 0,
      totalMaxPoints: 0,
      currentRewardEur: 0,
      fullRewardEur: 0,
      goals: [],
      thresholds: [],
    };
  }
  const wave = await loadActiveWaveForInstant({
    at: now,
    executor: db,
    fallbackTimezone: (await resolveCurrentRedPeriod(now)).timezone || DEFAULT_TIMEZONE,
  });
  if (!wave) {
    return {
      hasActiveWave: false,
      waveId: null,
      waveName: null,
      year: null,
      quarter: null,
      startDate: null,
      endDate: null,
      rewardModel: null,
      totalPoints: 0,
      totalMaxPoints: 0,
      currentRewardEur: 0,
      fullRewardEur: 0,
      goals: [],
      thresholds: [],
    };
  }

  const [pillars, sources, thresholds, metrics, tiers, tierConditions, pillarRows, qualityRows, flexRows] = await Promise.all([
    db
      .select()
      .from(praemienWavePillars)
      .where(and(eq(praemienWavePillars.waveId, wave.id), eq(praemienWavePillars.isDeleted, false)))
      .orderBy(asc(praemienWavePillars.orderIndex)),
    db
      .select()
      .from(praemienWaveSources)
      .where(and(eq(praemienWaveSources.waveId, wave.id), eq(praemienWaveSources.isDeleted, false))),
    db
      .select({
        label: praemienWaveThresholds.label,
        minPoints: praemienWaveThresholds.minPoints,
        rewardEur: praemienWaveThresholds.rewardEur,
        orderIndex: praemienWaveThresholds.orderIndex,
      })
      .from(praemienWaveThresholds)
      .where(and(eq(praemienWaveThresholds.waveId, wave.id), eq(praemienWaveThresholds.isDeleted, false)))
      .orderBy(asc(praemienWaveThresholds.orderIndex)),
    db
      .select()
      .from(praemienWavePillarMetrics)
      .where(and(eq(praemienWavePillarMetrics.waveId, wave.id), eq(praemienWavePillarMetrics.isDeleted, false)))
      .orderBy(asc(praemienWavePillarMetrics.orderIndex)),
    db
      .select()
      .from(praemienWavePillarTiers)
      .where(and(eq(praemienWavePillarTiers.waveId, wave.id), eq(praemienWavePillarTiers.isDeleted, false)))
      .orderBy(asc(praemienWavePillarTiers.orderIndex)),
    db
      .select()
      .from(praemienWavePillarTierConditions)
      .where(and(eq(praemienWavePillarTierConditions.waveId, wave.id), eq(praemienWavePillarTierConditions.isDeleted, false)))
      .orderBy(asc(praemienWavePillarTierConditions.orderIndex)),
    db
      .select({
        pillarId: praemienWaveSources.pillarId,
        points: sql<number>`coalesce(sum(${praemienGmWaveContributions.appliedValue}), 0)`,
      })
      .from(praemienGmWaveContributions)
      .innerJoin(
        praemienWaveSources,
        and(
          eq(praemienWaveSources.id, praemienGmWaveContributions.sourceId),
          eq(praemienWaveSources.waveId, wave.id),
          eq(praemienWaveSources.isDeleted, false),
        ),
      )
      .where(and(eq(praemienGmWaveContributions.waveId, wave.id), eq(praemienGmWaveContributions.gmUserId, gmUserId)))
      .groupBy(praemienWaveSources.pillarId),
    db
      .select({
        zeiterfassung: praemienWaveQualityScores.zeiterfassung,
        reporting: praemienWaveQualityScores.reporting,
        accuracy: praemienWaveQualityScores.accuracy,
        totalPoints: praemienWaveQualityScores.totalPoints,
      })
      .from(praemienWaveQualityScores)
      .where(and(
        eq(praemienWaveQualityScores.waveId, wave.id),
        eq(praemienWaveQualityScores.gmUserId, gmUserId),
        eq(praemienWaveQualityScores.isDeleted, false),
      ))
      .limit(1),
    db
      .select({
        totalPoints: praemienWaveFlexScores.totalPoints,
        componentValues: praemienWaveFlexScores.componentValues,
      })
      .from(praemienWaveFlexScores)
      .where(and(
        eq(praemienWaveFlexScores.waveId, wave.id),
        eq(praemienWaveFlexScores.gmUserId, gmUserId),
        eq(praemienWaveFlexScores.isDeleted, false),
      ))
      .limit(1),
  ]);

  const maxByPillar = new Map<string, number>();
  for (const source of sources) {
    const current = maxByPillar.get(source.pillarId) ?? 0;
    maxByPillar.set(source.pillarId, current + normalizeNumber(source.boniValue));
  }
  const pointsByPillar = new Map<string, number>();
  for (const row of pillarRows) {
    pointsByPillar.set(row.pillarId, normalizeNumber(row.points));
  }
  const metricsByPillarId = new Map<string, PillarMetricRow[]>();
  for (const metric of metrics) {
    const list = metricsByPillarId.get(metric.pillarId) ?? [];
    list.push(metric);
    metricsByPillarId.set(metric.pillarId, list);
  }
  const metricById = new Map(metrics.map((metric) => [metric.id, metric]));

  const rawGoals = pillars.map((pillar) => {
    const buildRewardFields = (points: number, maxPoints: number) => ({
      targetPoints: pillar.targetPoints == null ? null : normalizeNumber(pillar.targetPoints),
      rewardEur: normalizeNumber(pillar.rewardEur),
      payoutMode: pillar.payoutMode as "highest_tier" | "sum_earned_tiers",
      maxRewardEur: normalizeNumber(pillar.maxRewardEur),
      metricValues: resolvePillarMetricValues({
        metrics: metricsByPillarId.get(pillar.id) ?? [],
        points,
        maxPoints,
        quality: qualityRows[0] ?? null,
        flex: flexRows[0] ? {
          totalPoints: flexRows[0].totalPoints,
          componentValues: flexRows[0].componentValues ?? {},
        } : null,
      }),
      tiers: buildPillarTierInputs({
        pillarId: pillar.id,
        tiers,
        conditions: tierConditions,
        metricById,
      }),
    });
    if (isQualityPillarName(pillar.name)) {
      const qualityScore = qualityRows[0];
      const points = qualityScore ? normalizeNumber(qualityScore.totalPoints) : 0;
      const maxPoints = qualityScore ? 100 : 0;
      return {
        pillarId: pillar.id,
        name: pillar.name,
        color: pillar.color,
        points,
        maxPoints,
        isManual: true,
        isPending: !qualityScore,
        ...buildRewardFields(points, maxPoints),
      };
    }
    if (isFlexPillarName(pillar.name)) {
      const flexScore = flexRows[0];
      const points = flexScore ? normalizeNumber(flexScore.totalPoints) : 0;
      const maxPoints = flexScore ? 100 : 0;
      return {
        pillarId: pillar.id,
        name: pillar.name,
        color: pillar.color,
        points,
        maxPoints,
        isManual: true,
        isPending: !flexScore,
        ...buildRewardFields(points, maxPoints),
      };
    }
    const maxPoints = Number((maxByPillar.get(pillar.id) ?? 0).toFixed(4));
    const points = Number((pointsByPillar.get(pillar.id) ?? 0).toFixed(4));
    return {
      pillarId: pillar.id,
      name: pillar.name,
      color: pillar.color,
      points,
      maxPoints,
      isManual: Boolean(pillar.isManual),
      isPending: false,
      ...buildRewardFields(points, maxPoints),
    };
  });

  const normalizedThresholds = thresholds.map((entry) => ({
    label: entry.label,
    minPoints: normalizeNumber(entry.minPoints),
    rewardEur: normalizeNumber(entry.rewardEur),
  }));
  const totalPoints = Number(rawGoals.reduce((sum, entry) => sum + entry.points, 0).toFixed(4));
  const reward = calculateWaveReward({
    rewardModel: wave.rewardModel,
    totalPoints,
    thresholds: normalizedThresholds,
    pillars: rawGoals.map((goal) => ({
      pillarId: goal.pillarId,
      points: goal.points,
      targetPoints: goal.targetPoints,
      rewardEur: goal.rewardEur,
      payoutMode: goal.payoutMode,
      maxRewardEur: goal.maxRewardEur,
      metricValues: goal.metricValues,
      tiers: goal.tiers,
    })),
  });
  const rewardByPillarId = new Map(reward.pillarResults.map((pillar) => [pillar.pillarId, pillar]));
  const goals: GmBonusSummaryGoal[] = rawGoals.map((goal) => {
    const pillarReward = rewardByPillarId.get(goal.pillarId);
    const progressTarget = wave.rewardModel === "pillar_targets" && pillarReward?.targetPoints != null
      ? pillarReward.targetPoints
      : goal.maxPoints;
    return {
      ...goal,
      percent: progressTarget > 0
        ? Math.max(0, Math.min(100, Math.round((goal.points / progressTarget) * 100)))
        : 0,
      targetPoints: pillarReward?.targetPoints ?? null,
      rewardEur: pillarReward?.rewardEur ?? goal.rewardEur,
      achieved: pillarReward?.achieved ?? false,
      earnedRewardEur: wave.rewardModel === "pillar_targets" || wave.rewardModel === "pillar_tiers"
        ? (pillarReward?.earnedRewardEur ?? 0)
        : 0,
      maxRewardEur: pillarReward?.maxRewardEur ?? goal.maxRewardEur,
      metricValues: pillarReward?.metricValues ?? goal.metricValues,
      achievedTierLabels: pillarReward?.achievedTierLabels ?? [],
      nextTierLabel: pillarReward?.nextTierLabel ?? null,
    };
  });
  const fullRewardEur = reward.fullRewardEur;
  const currentRewardEur = reward.currentRewardEur;
  const totalMaxPoints = Number(goals.reduce((sum, entry) => sum + entry.maxPoints, 0).toFixed(4));

  return {
    hasActiveWave: true,
    waveId: wave.id,
    waveName: wave.name,
    year: wave.year,
    quarter: wave.quarter,
    startDate: wave.startDate,
    endDate: wave.endDate,
    rewardModel: wave.rewardModel,
    totalPoints,
    totalMaxPoints,
    currentRewardEur,
    fullRewardEur,
    goals,
    thresholds: normalizedThresholds,
  };
}
