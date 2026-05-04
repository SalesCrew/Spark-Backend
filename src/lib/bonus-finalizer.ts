import { and, asc, desc, eq, inArray, sql } from "drizzle-orm";
import { db, sql as pgSql } from "./db.js";
import { DEFAULT_TIMEZONE, toYmdInTimezone } from "./day-session.js";
import { recomputeGmKpiCache } from "./gm-kpi-cache.js";
import { getCurrentRedPeriod } from "./red-monat.js";
import { refreshRedMonthCalendarConfig } from "./red-month-calendar.js";
import {
  markets,
  praemienGmWaveContributions,
  praemienGmWaveTotals,
  praemienWavePillars,
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

type BonusDbExecutor = {
  select: typeof db.select;
  insert: typeof db.insert;
  update: typeof db.update;
};

let checkedBonusTableReadiness = false;
let hasBonusTables = false;

async function ensureBonusTablesReady(): Promise<boolean> {
  if (checkedBonusTableReadiness) return hasBonusTables;
  const result = await pgSql<{
    praemien_waves: string | null;
    praemien_wave_sources: string | null;
    praemien_wave_thresholds: string | null;
    praemien_gm_wave_contributions: string | null;
    praemien_gm_wave_totals: string | null;
  }[]>`
    select
      to_regclass('public.praemien_waves') as praemien_waves,
      to_regclass('public.praemien_wave_sources') as praemien_wave_sources,
      to_regclass('public.praemien_wave_thresholds') as praemien_wave_thresholds,
      to_regclass('public.praemien_gm_wave_contributions') as praemien_gm_wave_contributions,
      to_regclass('public.praemien_gm_wave_totals') as praemien_gm_wave_totals
  `;
  const row = result[0];
  hasBonusTables = Boolean(
    row?.praemien_waves &&
    row?.praemien_wave_sources &&
    row?.praemien_wave_thresholds &&
    row?.praemien_gm_wave_contributions &&
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
};

export type GmBonusSummary = {
  hasActiveWave: boolean;
  waveId: string | null;
  waveName: string | null;
  year: number | null;
  quarter: number | null;
  startDate: string | null;
  endDate: string | null;
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

function pickRewardForPoints(
  totalPoints: number,
  thresholds: Array<{ minPoints: number; rewardEur: number }>,
): number {
  const sorted = thresholds.slice().sort((left, right) => left.minPoints - right.minPoints);
  const matched = sorted.filter((entry) => totalPoints >= entry.minPoints).at(-1);
  return matched?.rewardEur ?? 0;
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
  const calendarConfig = await refreshRedMonthCalendarConfig();
  const submittedAt = input.submittedAt;
  const wave = await loadActiveWaveForInstant({
    at: submittedAt,
    executor,
    fallbackTimezone: calendarConfig.timezone,
  });
  if (!wave) return { applied: false, waveId: null };

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

  const redPeriod = getCurrentRedPeriod(submittedAt);
  const redMonthTimezone = calendarConfig.timezone?.trim() || DEFAULT_TIMEZONE;
  const redPeriodStart = toYmdInTimezone(redPeriod.start, redMonthTimezone);
  const redPeriodEnd = toYmdInTimezone(redPeriod.end, redMonthTimezone);

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

  const [aggregate] = await executor
    .select({
      totalPoints: sql<number>`coalesce(sum(${praemienGmWaveContributions.appliedValue}), 0)`,
      contributionCount: sql<number>`count(*)`,
    })
    .from(praemienGmWaveContributions)
    .where(and(eq(praemienGmWaveContributions.waveId, wave.id), eq(praemienGmWaveContributions.gmUserId, input.gmUserId)));
  const thresholds = await executor
    .select({
      minPoints: praemienWaveThresholds.minPoints,
      rewardEur: praemienWaveThresholds.rewardEur,
    })
    .from(praemienWaveThresholds)
    .where(and(eq(praemienWaveThresholds.waveId, wave.id), eq(praemienWaveThresholds.isDeleted, false)));

  const totalPoints = normalizeNumber(aggregate?.totalPoints);
  const currentRewardEur = pickRewardForPoints(
    totalPoints,
    thresholds.map((entry) => ({
      minPoints: normalizeNumber(entry.minPoints),
      rewardEur: normalizeNumber(entry.rewardEur),
    })),
  );

  await executor
    .insert(praemienGmWaveTotals)
    .values({
      waveId: wave.id,
      gmUserId: input.gmUserId,
      totalPoints: String(totalPoints),
      currentRewardEur: String(currentRewardEur),
      contributionCount: Number(aggregate?.contributionCount ?? 0),
      lastContributionAt: submittedAt,
      createdAt: submittedAt,
      updatedAt: submittedAt,
    })
    .onConflictDoUpdate({
      target: [praemienGmWaveTotals.waveId, praemienGmWaveTotals.gmUserId],
      set: {
        totalPoints: String(totalPoints),
        currentRewardEur: String(currentRewardEur),
        contributionCount: Number(aggregate?.contributionCount ?? 0),
        lastContributionAt: submittedAt,
        updatedAt: submittedAt,
      },
    });

  return { applied: true, waveId: wave.id };
}

export async function finalizeBonusForSubmittedVisitSessionTx(
  tx: BonusDbExecutor,
  input: FinalizeBonusInput,
): Promise<BonusFinalizeResult> {
  return finalizeBonusForSubmittedVisitSessionWithExecutor(tx, input);
}

export async function finalizeBonusForSubmittedVisitSession(input: FinalizeBonusInput): Promise<BonusFinalizeResult> {
  const result = await db.transaction(async (tx) => finalizeBonusForSubmittedVisitSessionWithExecutor(tx, input));
  try {
    await recomputeGmKpiCache(input.gmUserId);
  } catch (error) {
    console.error("[bonus-finalizer] gm kpi recompute failed", {
      gmUserId: input.gmUserId,
      sessionId: input.sessionId,
      error: error instanceof Error ? error.message : String(error),
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
      totalPoints: 0,
      totalMaxPoints: 0,
      currentRewardEur: 0,
      fullRewardEur: 0,
      goals: [],
      thresholds: [],
    };
  }
  const calendarConfig = await refreshRedMonthCalendarConfig();
  const wave = await loadActiveWaveForInstant({
    at: now,
    executor: db,
    fallbackTimezone: calendarConfig.timezone,
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
      totalPoints: 0,
      totalMaxPoints: 0,
      currentRewardEur: 0,
      fullRewardEur: 0,
      goals: [],
      thresholds: [],
    };
  }

  const [pillars, sources, thresholds, totalRow, pillarRows] = await Promise.all([
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
      .from(praemienGmWaveTotals)
      .where(and(eq(praemienGmWaveTotals.waveId, wave.id), eq(praemienGmWaveTotals.gmUserId, gmUserId)))
      .limit(1),
    db
      .select({
        pillarId: praemienGmWaveContributions.pillarId,
        points: sql<number>`coalesce(sum(${praemienGmWaveContributions.appliedValue}), 0)`,
      })
      .from(praemienGmWaveContributions)
      .where(and(eq(praemienGmWaveContributions.waveId, wave.id), eq(praemienGmWaveContributions.gmUserId, gmUserId)))
      .groupBy(praemienGmWaveContributions.pillarId),
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

  const goals = pillars.map((pillar) => {
    const maxPoints = Number((maxByPillar.get(pillar.id) ?? 0).toFixed(4));
    const points = Number((pointsByPillar.get(pillar.id) ?? 0).toFixed(4));
    const percent = maxPoints > 0 ? Math.max(0, Math.min(100, Math.round((points / maxPoints) * 100))) : 0;
    return {
      pillarId: pillar.id,
      name: pillar.name,
      color: pillar.color,
      points,
      maxPoints,
      percent,
    };
  });

  const normalizedThresholds = thresholds.map((entry) => ({
    label: entry.label,
    minPoints: normalizeNumber(entry.minPoints),
    rewardEur: normalizeNumber(entry.rewardEur),
  }));
  const totalPoints = totalRow[0] ? normalizeNumber(totalRow[0].totalPoints) : Number(
    goals.reduce((sum, entry) => sum + entry.points, 0).toFixed(4),
  );
  const fullRewardEur = Math.max(0, ...normalizedThresholds.map((entry) => entry.rewardEur));
  const currentRewardEur = totalRow[0]
    ? normalizeNumber(totalRow[0].currentRewardEur)
    : pickRewardForPoints(totalPoints, normalizedThresholds);
  const totalMaxPoints = Math.max(
    0,
    ...normalizedThresholds.map((entry) => entry.minPoints),
    Number(goals.reduce((sum, entry) => sum + entry.maxPoints, 0).toFixed(4)),
  );

  return {
    hasActiveWave: true,
    waveId: wave.id,
    waveName: wave.name,
    year: wave.year,
    quarter: wave.quarter,
    startDate: wave.startDate,
    endDate: wave.endDate,
    totalPoints,
    totalMaxPoints,
    currentRewardEur,
    fullRewardEur,
    goals,
    thresholds: normalizedThresholds,
  };
}
