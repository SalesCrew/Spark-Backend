import { and, asc, eq, inArray, sql } from "drizzle-orm";
import { type Response, Router } from "express";
import { z } from "zod";
import { env } from "../config/env.js";
import { db, sql as pgSql } from "../lib/db.js";
import { recomputeBonusWaveTx, type BonusWaveRecomputeResult } from "../lib/bonus-finalizer.js";
import { recomputeGmKpiCache } from "../lib/gm-kpi-cache.js";
import { requireKundeAdminPermission } from "../lib/kunde-access.js";
import { logAction, logger, startActionTimer } from "../lib/logger.js";
import { isWaveAnswerPersistencePillarName } from "../lib/praemien-answer-persistence.js";
import {
  buildSourceCatalog,
  normalizeMoney,
  sourceCatalogKey,
  type BonusSourceCatalogRow,
  type PraemienSectionType,
} from "../lib/praemien-source-catalog.js";
import {
  praemienWavePillars,
  praemienWavePillarMetrics,
  praemienWavePillarTierConditions,
  praemienWavePillarTiers,
  praemienWaveFlexScores,
  praemienWaveQualityScores,
  praemienWaveSources,
  praemienWaveThresholds,
  praemienWaves,
  users,
} from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const adminPraemienRouter = Router();
adminPraemienRouter.use(requireAuth(["admin", "kunde"]));
adminPraemienRouter.use(requireKundeAdminPermission);
adminPraemienRouter.use((req, res, next) => {
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "admin_praemien_action_completed", {
      req,
      action: req.method.toUpperCase() === "GET" ? "admin_praemien_read" : "admin_praemien_mutation",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

const waveStatusSchema = z.enum(["draft", "active", "archived"]);
const rewardModelSchema = z.enum(["global_thresholds", "pillar_targets", "pillar_tiers"]);
const pillarPayoutModeSchema = z.enum(["highest_tier", "sum_earned_tiers"]);
const pillarMetricUnitSchema = z.enum(["points", "percent", "count", "currency"]);
const pillarMetricValueSourceSchema = z.enum([
  "contribution_points",
  "contribution_percent",
  "quality_zeiterfassung",
  "quality_reporting",
  "quality_accuracy",
  "quality_average",
  "flex_total_points",
  "flex_component",
]);
const pillarConditionOperatorSchema = z.enum(["gte", "lte", "eq"]);
const sectionTypeSchema = z.enum(["standard", "flex", "billa", "kuehler", "mhd"]) satisfies z.ZodType<PraemienSectionType>;
const distributionFreqRuleSchema = z.enum(["lt8", "gt8"]);
const dateYmdSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/);
const isoDatetimeSchema = z.string().datetime({ offset: true });

const thresholdInputSchema = z.object({
  id: z.string().uuid().optional(),
  label: z.string().trim().min(1).max(120),
  orderIndex: z.number().int().min(0),
  minPoints: z.number().finite().gte(0),
  rewardEur: z.number().finite().gte(0),
});

const pillarMetricInputSchema = z.object({
  id: z.string().uuid().optional(),
  key: z.string().trim().min(1).max(100).regex(/^[a-z][a-z0-9_]*$/),
  label: z.string().trim().min(1).max(160),
  unit: pillarMetricUnitSchema,
  valueSource: pillarMetricValueSourceSchema,
  sourceKey: z.string().trim().min(1).max(100).nullable().optional(),
  orderIndex: z.number().int().min(0),
});

const pillarTierConditionInputSchema = z.object({
  id: z.string().uuid().optional(),
  metricKey: z.string().trim().min(1).max(100),
  operator: pillarConditionOperatorSchema,
  thresholdValue: z.number().finite().gte(0),
  orderIndex: z.number().int().min(0),
});

const pillarTierInputSchema = z.object({
  id: z.string().uuid().optional(),
  label: z.string().trim().min(1).max(160),
  orderIndex: z.number().int().min(0),
  rewardEur: z.number().finite().gte(0),
  conditions: z.array(pillarTierConditionInputSchema).min(1),
});

const pillarInputSchema = z.object({
  id: z.string().uuid().optional(),
  name: z.string().trim().min(1).max(120),
  description: z.string().trim().max(1000).optional().default(""),
  color: z.string().trim().min(1).max(40).optional().default("#DC2626"),
  orderIndex: z.number().int().min(0),
  isManual: z.boolean().optional().default(false),
  payoutMode: pillarPayoutModeSchema.optional().default("highest_tier"),
  maxRewardEur: z.number().finite().gte(0).optional().default(0),
  targetPoints: z.number().finite().gt(0).nullable().optional(),
  rewardEur: z.number().finite().gte(0).optional(),
  metrics: z.array(pillarMetricInputSchema).optional().default([]),
  tiers: z.array(pillarTierInputSchema).optional().default([]),
}).superRefine((value, ctx) => {
  const metricKeys = new Set(value.metrics.map((metric) => metric.key));
  if (metricKeys.size !== value.metrics.length) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: "Messgrößen-Keys müssen je Säule eindeutig sein." });
  }
  const metricOrders = new Set(value.metrics.map((metric) => metric.orderIndex));
  if (metricOrders.size !== value.metrics.length) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: "Messgrößen-Reihenfolge enthält Duplikate." });
  }
  const tierOrders = new Set(value.tiers.map((tier) => tier.orderIndex));
  if (tierOrders.size !== value.tiers.length) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: "Auszahlungsstufen-Reihenfolge enthält Duplikate." });
  }
  const tierLabels = new Set(value.tiers.map((tier) => tier.label.toLowerCase()));
  if (tierLabels.size !== value.tiers.length) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: "Auszahlungsstufen-Namen müssen je Säule eindeutig sein." });
  }
  for (const tier of value.tiers) {
    for (const condition of tier.conditions) {
      if (!metricKeys.has(condition.metricKey)) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: `Unbekannte Messgröße ${condition.metricKey} in ${tier.label}.` });
      }
    }
  }
  const configuredMaximum = value.payoutMode === "sum_earned_tiers"
    ? value.tiers.reduce((sum, tier) => sum + tier.rewardEur, 0)
    : Math.max(0, ...value.tiers.map((tier) => tier.rewardEur));
  if (configuredMaximum > value.maxRewardEur + 0.001) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: "Auszahlungsstufen überschreiten den Maximalbetrag der Säule." });
  }
});

const sourceInputSchema = z.object({
  id: z.string().uuid().optional(),
  pillarId: z.string().uuid(),
  sectionType: sectionTypeSchema,
  fragebogenId: z.string().uuid().nullable().optional(),
  fragebogenName: z.string().trim().max(200).optional().default(""),
  moduleId: z.string().uuid().nullable().optional(),
  moduleName: z.string().trim().max(200).optional().default(""),
  questionId: z.string().uuid(),
  questionText: z.string().trim().max(2000).optional().default(""),
  scoringKey: z.string().trim().min(1).max(200).optional(),
  scoreKey: z.string().trim().min(1).max(200).optional(),
  displayLabel: z.string().trim().max(200).optional().default(""),
  isFactorMode: z.boolean().optional().default(false),
  boniValue: z.number().finite(),
  distributionFreqRule: distributionFreqRuleSchema.nullable().optional(),
}).superRefine((value, ctx) => {
  if (!value.scoringKey && !value.scoreKey) {
    ctx.addIssue({ code: z.ZodIssueCode.custom, message: "scoringKey ist erforderlich." });
  }
});

const qualityScoreInputSchema = z.object({
  id: z.string().uuid().optional(),
  gmUserId: z.string().uuid(),
  zeiterfassung: z.number().int().min(0).max(100),
  reporting: z.number().int().min(0).max(100),
  accuracy: z.number().int().min(0).max(100),
  note: z.string().trim().max(2000).nullable().optional(),
});

const flexScoreInputSchema = z.object({
  id: z.string().uuid().optional(),
  gmUserId: z.string().uuid(),
  totalPoints: z.number().int().min(0).max(100),
  componentValues: z.record(z.string(), z.number().finite().gte(0)).optional().default({}),
  note: z.string().trim().max(2000).nullable().optional(),
});

const createWaveSchema = z.object({
  name: z.string().trim().min(1).max(180),
  year: z.number().int().min(2000).max(2100),
  quarter: z.number().int().min(1).max(4),
  startDate: dateYmdSchema,
  endDate: dateYmdSchema,
  status: waveStatusSchema.optional().default("draft"),
  description: z.string().trim().max(2000).optional().default(""),
  timezone: z.string().trim().min(1).max(120).optional().default("Europe/Vienna"),
  rewardModel: rewardModelSchema.optional().default("global_thresholds"),
  thresholds: z.array(thresholdInputSchema).optional().default([]),
  pillars: z.array(pillarInputSchema).optional().default([]),
});

const updateWaveSchema = z.object({
  name: z.string().trim().min(1).max(180).optional(),
  year: z.number().int().min(2000).max(2100).optional(),
  quarter: z.number().int().min(1).max(4).optional(),
  startDate: dateYmdSchema.optional(),
  endDate: dateYmdSchema.optional(),
  status: waveStatusSchema.optional(),
  description: z.string().trim().max(2000).optional(),
  timezone: z.string().trim().min(1).max(120).optional(),
  rewardModel: rewardModelSchema.optional(),
});

const replaceThresholdsSchema = z.object({
  thresholds: z.array(thresholdInputSchema).min(2),
  expectedUpdatedAt: isoDatetimeSchema.optional(),
});

const replacePillarsSchema = z.object({
  pillars: z.array(pillarInputSchema).min(1),
  expectedUpdatedAt: isoDatetimeSchema.optional(),
});

const replaceSourcesSchema = z.object({
  sources: z.array(sourceInputSchema),
  expectedUpdatedAt: isoDatetimeSchema.optional(),
});

const replaceQualityScoresSchema = z.object({
  qualityScores: z.array(qualityScoreInputSchema),
  expectedUpdatedAt: isoDatetimeSchema.optional(),
});

const replaceFlexScoresSchema = z.object({
  flexScores: z.array(flexScoreInputSchema),
  expectedUpdatedAt: isoDatetimeSchema.optional(),
});

const waveResponseSchema = z.object({
  id: z.string().uuid(),
  name: z.string(),
  year: z.number().int(),
  quarter: z.number().int().min(1).max(4),
  status: waveStatusSchema,
  startDate: z.string(),
  endDate: z.string(),
  description: z.string(),
  timezone: z.string(),
  rewardModel: rewardModelSchema,
  thresholds: z.array(
    z.object({
      id: z.string().uuid(),
      label: z.string(),
      minPoints: z.number(),
      rewardEur: z.number(),
    }),
  ),
  pillars: z.array(
    z.object({
      id: z.string().uuid(),
      name: z.string(),
      description: z.string(),
      color: z.string(),
      isManual: z.boolean(),
      payoutMode: pillarPayoutModeSchema,
      maxRewardEur: z.number(),
      targetPoints: z.number().nullable(),
      rewardEur: z.number(),
      metrics: z.array(z.object({
        id: z.string().uuid(),
        key: z.string(),
        label: z.string(),
        unit: pillarMetricUnitSchema,
        valueSource: pillarMetricValueSourceSchema,
        sourceKey: z.string().nullable(),
        orderIndex: z.number().int(),
      })),
      tiers: z.array(z.object({
        id: z.string().uuid(),
        label: z.string(),
        orderIndex: z.number().int(),
        rewardEur: z.number(),
        conditions: z.array(z.object({
          id: z.string().uuid(),
          metricKey: z.string(),
          operator: pillarConditionOperatorSchema,
          thresholdValue: z.number(),
          orderIndex: z.number().int(),
        })),
      })),
      sourceRefs: z.array(
        z.object({
          id: z.string().uuid(),
          sectionType: sectionTypeSchema,
          fragebogenId: z.string(),
          fragebogenName: z.string(),
          moduleId: z.string(),
          moduleName: z.string(),
          questionId: z.string(),
          questionText: z.string(),
          scoringKey: z.string(),
          boniValue: z.number(),
          isFactorMode: z.boolean(),
          displayLabel: z.string(),
          distributionFreqRule: distributionFreqRuleSchema.optional(),
        }),
      ),
    }),
  ),
  qualitySubmissions: z.array(
    z.object({
      gmId: z.string().uuid(),
      gmName: z.string(),
      scores: z.object({
        zeiterfassung: z.number().int(),
        reporting: z.number().int(),
        accuracy: z.number().int(),
      }),
      totalPoints: z.number().int(),
      note: z.string().optional(),
      updatedAt: z.string().datetime({ offset: true }),
    }),
  ),
  flexSubmissions: z.array(
    z.object({
      gmId: z.string().uuid(),
      gmName: z.string(),
      totalPoints: z.number().int(),
      componentValues: z.record(z.string(), z.number()),
      note: z.string().optional(),
      updatedAt: z.string().datetime({ offset: true }),
    }),
  ),
  createdAt: z.string().datetime({ offset: true }),
  updatedAt: z.string().datetime({ offset: true }),
});

class PraemienDomainError extends Error {
  code: string;
  status: number;
  constructor(code: string, status: number, message: string) {
    super(message);
    this.code = code;
    this.status = status;
  }
}

function isPgUniqueViolation(error: unknown): boolean {
  return Boolean(error && typeof error === "object" && "code" in error && (error as { code?: string }).code === "23505");
}

function ensureWaveDateRange(startDate: string, endDate: string) {
  if (startDate > endDate) {
    throw new PraemienDomainError("wave_invalid_date_range", 400, "Startdatum darf nicht nach Enddatum liegen.");
  }
}

function ensureThresholdsStrictlyAscending(thresholds: Array<{ label: string; orderIndex: number; minPoints: number }>) {
  if (thresholds.length < 2) {
    throw new PraemienDomainError("thresholds_min_two_required", 400, "Mindestens zwei Schwellwerte sind erforderlich.");
  }
  const orderSet = new Set<number>();
  const labelSet = new Set<string>();
  for (const threshold of thresholds) {
    if (orderSet.has(threshold.orderIndex)) {
      throw new PraemienDomainError("threshold_order_duplicate", 400, "Schwellwert-Reihenfolge enthält Duplikate.");
    }
    orderSet.add(threshold.orderIndex);
    const labelKey = threshold.label.trim().toLowerCase();
    if (labelSet.has(labelKey)) {
      throw new PraemienDomainError("threshold_label_duplicate", 400, "Schwellwert-Labels müssen eindeutig sein.");
    }
    labelSet.add(labelKey);
  }

  const sorted = [...thresholds].sort((a, b) => a.minPoints - b.minPoints);
  for (let index = 1; index < sorted.length; index += 1) {
    const current = sorted[index];
    const previous = sorted[index - 1];
    if (!current || !previous) continue;
    if (current.minPoints <= previous.minPoints) {
      throw new PraemienDomainError("thresholds_not_strictly_ascending", 400, "Schwellwerte müssen strikt aufsteigend sein.");
    }
  }
  const keinBonus = sorted.find((entry) => entry.label.trim().toLowerCase() === "kein bonus");
  if (keinBonus && keinBonus.minPoints !== 0) {
    throw new PraemienDomainError("threshold_kein_bonus_must_start_at_zero", 400, "Kein Bonus muss bei 0 Punkten starten.");
  }
}

async function loadWaveGraph(waveId: string) {
  const [wave] = await db
    .select()
    .from(praemienWaves)
    .where(and(eq(praemienWaves.id, waveId), eq(praemienWaves.isDeleted, false)))
    .limit(1);
  if (!wave) return null;

  const [thresholds, pillars, metrics, tiers, tierConditions, sources, qualityRows, flexRows] = await Promise.all([
    db
      .select()
      .from(praemienWaveThresholds)
      .where(and(eq(praemienWaveThresholds.waveId, waveId), eq(praemienWaveThresholds.isDeleted, false)))
      .orderBy(asc(praemienWaveThresholds.orderIndex)),
    db
      .select()
      .from(praemienWavePillars)
      .where(and(eq(praemienWavePillars.waveId, waveId), eq(praemienWavePillars.isDeleted, false)))
      .orderBy(asc(praemienWavePillars.orderIndex)),
    db
      .select()
      .from(praemienWavePillarMetrics)
      .where(and(eq(praemienWavePillarMetrics.waveId, waveId), eq(praemienWavePillarMetrics.isDeleted, false)))
      .orderBy(asc(praemienWavePillarMetrics.orderIndex)),
    db
      .select()
      .from(praemienWavePillarTiers)
      .where(and(eq(praemienWavePillarTiers.waveId, waveId), eq(praemienWavePillarTiers.isDeleted, false)))
      .orderBy(asc(praemienWavePillarTiers.orderIndex)),
    db
      .select()
      .from(praemienWavePillarTierConditions)
      .where(and(eq(praemienWavePillarTierConditions.waveId, waveId), eq(praemienWavePillarTierConditions.isDeleted, false)))
      .orderBy(asc(praemienWavePillarTierConditions.orderIndex)),
    db
      .select()
      .from(praemienWaveSources)
      .where(and(eq(praemienWaveSources.waveId, waveId), eq(praemienWaveSources.isDeleted, false)))
      .orderBy(asc(praemienWaveSources.createdAt)),
    db
      .select({
        id: praemienWaveQualityScores.id,
        waveId: praemienWaveQualityScores.waveId,
        gmUserId: praemienWaveQualityScores.gmUserId,
        zeiterfassung: praemienWaveQualityScores.zeiterfassung,
        reporting: praemienWaveQualityScores.reporting,
        accuracy: praemienWaveQualityScores.accuracy,
        totalPoints: praemienWaveQualityScores.totalPoints,
        note: praemienWaveQualityScores.note,
        updatedAt: praemienWaveQualityScores.updatedAt,
        gmFirstName: users.firstName,
        gmLastName: users.lastName,
      })
      .from(praemienWaveQualityScores)
      .innerJoin(users, eq(users.id, praemienWaveQualityScores.gmUserId))
      .where(and(eq(praemienWaveQualityScores.waveId, waveId), eq(praemienWaveQualityScores.isDeleted, false)))
      .orderBy(asc(users.lastName), asc(users.firstName)),
    db
      .select({
        id: praemienWaveFlexScores.id,
        waveId: praemienWaveFlexScores.waveId,
        gmUserId: praemienWaveFlexScores.gmUserId,
        totalPoints: praemienWaveFlexScores.totalPoints,
        componentValues: praemienWaveFlexScores.componentValues,
        note: praemienWaveFlexScores.note,
        updatedAt: praemienWaveFlexScores.updatedAt,
        gmFirstName: users.firstName,
        gmLastName: users.lastName,
      })
      .from(praemienWaveFlexScores)
      .innerJoin(users, eq(users.id, praemienWaveFlexScores.gmUserId))
      .where(and(eq(praemienWaveFlexScores.waveId, waveId), eq(praemienWaveFlexScores.isDeleted, false)))
      .orderBy(asc(users.lastName), asc(users.firstName)),
  ]);

  const metricById = new Map(metrics.map((metric) => [metric.id, metric]));
  const metricsByPillarId = new Map<string, typeof metrics>();
  for (const metric of metrics) {
    const list = metricsByPillarId.get(metric.pillarId) ?? [];
    list.push(metric);
    metricsByPillarId.set(metric.pillarId, list);
  }
  const conditionsByTierId = new Map<string, typeof tierConditions>();
  for (const condition of tierConditions) {
    const list = conditionsByTierId.get(condition.tierId) ?? [];
    list.push(condition);
    conditionsByTierId.set(condition.tierId, list);
  }
  const tiersByPillarId = new Map<string, typeof tiers>();
  for (const tier of tiers) {
    const list = tiersByPillarId.get(tier.pillarId) ?? [];
    list.push(tier);
    tiersByPillarId.set(tier.pillarId, list);
  }

  const sourceRefsByPillarId = new Map<
    string,
    Array<{
      id: string;
      sectionType: PraemienSectionType;
      fragebogenId: string;
      fragebogenName: string;
      moduleId: string;
      moduleName: string;
      questionId: string;
      questionText: string;
      scoringKey: string;
      boniValue: number;
      isFactorMode: boolean;
      displayLabel: string;
      distributionFreqRule?: "lt8" | "gt8";
    }>
  >();
  for (const row of sources) {
    const list = sourceRefsByPillarId.get(row.pillarId) ?? [];
    const sourceRef = {
      id: row.id,
      sectionType: row.sectionType,
      fragebogenId: row.fragebogenId ?? "",
      fragebogenName: row.fragebogenName,
      moduleId: row.moduleId ?? "",
      moduleName: row.moduleName,
      questionId: row.questionId,
      questionText: row.questionText,
      scoringKey: row.scoreKey,
      boniValue: normalizeMoney(row.boniValue),
      isFactorMode: row.isFactorMode,
      displayLabel: row.displayLabel,
      ...(row.distributionFreqRule ? { distributionFreqRule: row.distributionFreqRule } : {}),
    };
    list.push(sourceRef);
    sourceRefsByPillarId.set(row.pillarId, list);
  }

  const payload = {
    id: wave.id,
    name: wave.name,
    year: wave.year,
    quarter: wave.quarter,
    status: wave.status,
    startDate: wave.startDate,
    endDate: wave.endDate,
    description: wave.description,
    timezone: wave.timezone,
    rewardModel: wave.rewardModel,
    createdAt: wave.createdAt.toISOString(),
    updatedAt: wave.updatedAt.toISOString(),
    thresholds: thresholds.map((row) => ({
      id: row.id,
      label: row.label,
      minPoints: normalizeMoney(row.minPoints),
      rewardEur: normalizeMoney(row.rewardEur),
    })),
    pillars: pillars.map((row) => ({
      id: row.id,
      name: row.name,
      description: row.description,
      color: row.color,
      isManual: row.isManual,
      payoutMode: row.payoutMode as "highest_tier" | "sum_earned_tiers",
      maxRewardEur: normalizeMoney(row.maxRewardEur),
      targetPoints: row.targetPoints == null ? null : normalizeMoney(row.targetPoints),
      rewardEur: normalizeMoney(row.rewardEur),
      metrics: (metricsByPillarId.get(row.id) ?? []).map((metric) => ({
        id: metric.id,
        key: metric.key,
        label: metric.label,
        unit: metric.unit as "points" | "percent" | "count" | "currency",
        valueSource: metric.valueSource as z.infer<typeof pillarMetricValueSourceSchema>,
        sourceKey: metric.sourceKey,
        orderIndex: metric.orderIndex,
      })),
      tiers: (tiersByPillarId.get(row.id) ?? []).map((tier) => ({
        id: tier.id,
        label: tier.label,
        orderIndex: tier.orderIndex,
        rewardEur: normalizeMoney(tier.rewardEur),
        conditions: (conditionsByTierId.get(tier.id) ?? []).map((condition) => ({
          id: condition.id,
          metricKey: metricById.get(condition.metricId)?.key ?? "",
          operator: condition.operator as "gte" | "lte" | "eq",
          thresholdValue: normalizeMoney(condition.thresholdValue),
          orderIndex: condition.orderIndex,
        })),
      })),
      sourceRefs: sourceRefsByPillarId.get(row.id) ?? [],
    })),
    qualitySubmissions: qualityRows.map((row) => ({
      gmId: row.gmUserId,
      gmName: `${row.gmFirstName} ${row.gmLastName}`.trim(),
      scores: {
        zeiterfassung: row.zeiterfassung,
        reporting: row.reporting,
        accuracy: row.accuracy,
      },
      totalPoints: row.totalPoints,
      note: row.note ?? undefined,
      updatedAt: row.updatedAt.toISOString(),
    })),
    flexSubmissions: flexRows.map((row) => ({
      gmId: row.gmUserId,
      gmName: `${row.gmFirstName} ${row.gmLastName}`.trim(),
      totalPoints: row.totalPoints,
      componentValues: row.componentValues ?? {},
      note: row.note ?? undefined,
      updatedAt: row.updatedAt.toISOString(),
    })),
  };
  if (env.NODE_ENV !== "production") {
    waveResponseSchema.parse(payload);
  }
  return payload;
}

function logMutation(req: AuthedRequest, action: string, payload: Record<string, unknown>) {
  logger.info("praemien_mutation", {
    action,
    result: "success",
    actorUserId: req.authUser?.appUserId ?? null,
    ...payload,
  });
}

type Tx = Parameters<Parameters<typeof db.transaction>[0]>[0];

function parseDateish(value: unknown): Date | null {
  if (value instanceof Date) return value;
  if (typeof value === "string" || typeof value === "number") {
    const parsed = new Date(value);
    if (!Number.isNaN(parsed.getTime())) return parsed;
  }
  return null;
}

async function lockWaveTx(tx: Tx, waveId: string, expectedUpdatedAt?: string) {
  const lockRows = await tx.execute<{ id: string; updatedAt: Date | string }>(
    sql`select id, updated_at as "updatedAt" from praemien_waves where id = ${waveId} and is_deleted = false for update`,
  );
  const locked = lockRows[0];
  if (!locked) throw new PraemienDomainError("wave_not_found", 404, "Prämien-Welle nicht gefunden.");
  if (expectedUpdatedAt) {
    const current = parseDateish(locked.updatedAt);
    const expected = parseDateish(expectedUpdatedAt);
    if (!current || !expected || current.toISOString() !== expected.toISOString()) {
      throw new PraemienDomainError("wave_stale_write", 409, "Die Prämien-Welle wurde zwischenzeitlich geändert.");
    }
  }
}

async function touchWaveTx(tx: Tx, waveId: string, now: Date) {
  await tx.update(praemienWaves).set({ updatedAt: now }).where(eq(praemienWaves.id, waveId));
}

async function refreshBonusKpiCachesAfterRecompute(result: BonusWaveRecomputeResult | null | undefined) {
  if (!result?.applied || result.affectedGmUserIds.length === 0) return;
  for (const gmUserId of result.affectedGmUserIds) {
    try {
      await recomputeGmKpiCache(gmUserId);
    } catch (error) {
      logger.error("praemien_recompute_kpi_refresh_failed", {
        gmUserId,
        waveId: result.waveId,
        error,
      });
    }
  }
}

const BONUS_WAVE_RECOMPUTE_DEBOUNCE_MS = 250;
const bonusWaveRecomputeQueue = new Map<string, Promise<void>>();

function scheduleBonusWaveRecompute(waveId: string, reason: string) {
  const previous = bonusWaveRecomputeQueue.get(waveId) ?? Promise.resolve();
  const queued = previous
    .catch(() => undefined)
    .then(async () => {
      await new Promise<void>((resolve) => {
        setTimeout(resolve, BONUS_WAVE_RECOMPUTE_DEBOUNCE_MS);
      });
      const startedAtNs = startActionTimer();
      const result = await db.transaction(async (tx) => recomputeBonusWaveTx(tx, waveId));
      await refreshBonusKpiCachesAfterRecompute(result);
      logAction("info", "praemien_bonus_wave_recompute_completed", {
        action: "praemien_bonus_wave_recompute",
        result: result.applied ? "success" : "skipped",
        startedAtNs,
        details: {
          waveId,
          reason,
          processedSessions: result.processedSessions,
          affectedGmUserIds: result.affectedGmUserIds.length,
          skippedReason: result.skippedReason ?? null,
        },
      });
    })
    .catch((error) => {
      logger.error("praemien_bonus_wave_recompute_failed", {
        waveId,
        reason,
        error,
      });
    });

  bonusWaveRecomputeQueue.set(waveId, queued);
  void queued.finally(() => {
    if (bonusWaveRecomputeQueue.get(waveId) === queued) {
      bonusWaveRecomputeQueue.delete(waveId);
    }
  });
}

async function replaceThresholdsTx(tx: Tx, waveId: string, thresholds: z.infer<typeof thresholdInputSchema>[]) {
  ensureThresholdsStrictlyAscending(thresholds.map((entry) => ({ label: entry.label, orderIndex: entry.orderIndex, minPoints: entry.minPoints })));
  const now = new Date();
  await tx
    .update(praemienWaveThresholds)
    .set({ isDeleted: true, deletedAt: now, updatedAt: now })
    .where(and(eq(praemienWaveThresholds.waveId, waveId), eq(praemienWaveThresholds.isDeleted, false)));

  for (const entry of thresholds) {
    await tx.insert(praemienWaveThresholds).values({
      waveId,
      label: entry.label,
      orderIndex: entry.orderIndex,
      minPoints: String(entry.minPoints),
      rewardEur: String(entry.rewardEur),
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
  }
  await touchWaveTx(tx, waveId, now);
}

async function replacePillarsTx(tx: Tx, waveId: string, pillars: z.infer<typeof pillarInputSchema>[]) {
  const orderSet = new Set<number>();
  const nameSet = new Set<string>();
  for (const pillar of pillars) {
    if (orderSet.has(pillar.orderIndex)) {
      throw new PraemienDomainError("pillar_order_duplicate", 400, "Säulen-Reihenfolge enthält Duplikate.");
    }
    orderSet.add(pillar.orderIndex);
    const normalizedName = pillar.name.trim().toLowerCase();
    if (nameSet.has(normalizedName)) {
      throw new PraemienDomainError("pillar_name_duplicate", 400, "Säulen-Namen müssen eindeutig sein.");
    }
    nameSet.add(normalizedName);
  }

  const now = new Date();
  const existingPillars = await tx
    .select({
      id: praemienWavePillars.id,
      name: praemienWavePillars.name,
      targetPoints: praemienWavePillars.targetPoints,
      rewardEur: praemienWavePillars.rewardEur,
      carryAnswersForWave: praemienWavePillars.carryAnswersForWave,
    })
    .from(praemienWavePillars)
    .where(and(eq(praemienWavePillars.waveId, waveId), eq(praemienWavePillars.isDeleted, false)));
  const existingById = new Map(existingPillars.map((pillar) => [pillar.id, pillar]));
  const existingByName = new Map(existingPillars.map((pillar) => [pillar.name.trim().toLowerCase(), pillar]));
  await tx
    .update(praemienWavePillars)
    .set({ isDeleted: true, deletedAt: now, updatedAt: now })
    .where(and(eq(praemienWavePillars.waveId, waveId), eq(praemienWavePillars.isDeleted, false)));

  const retainedPillarIds = new Set<string>();
  for (const entry of pillars) {
    const previous = (entry.id ? existingById.get(entry.id) : undefined)
      ?? existingByName.get(entry.name.trim().toLowerCase());
    const targetPoints = entry.targetPoints !== undefined
      ? (entry.targetPoints == null ? null : String(entry.targetPoints))
      : (previous?.targetPoints ?? null);
    const rewardEur = entry.rewardEur !== undefined
      ? String(entry.rewardEur)
      : (previous?.rewardEur ?? "0");
    const carryAnswersForWave = previous?.carryAnswersForWave === true
      || isWaveAnswerPersistencePillarName(entry.name);
    const pillarValues = {
      name: entry.name,
      description: entry.description,
      color: entry.color,
      orderIndex: entry.orderIndex,
      isManual: entry.isManual,
      payoutMode: entry.payoutMode,
      maxRewardEur: String(entry.maxRewardEur),
      targetPoints,
      rewardEur,
      carryAnswersForWave,
      isDeleted: false,
      deletedAt: null,
      updatedAt: now,
    } as const;
    const pillarId = previous
      ? previous.id
      : (await tx.insert(praemienWavePillars).values({
          waveId,
          ...pillarValues,
          createdAt: now,
        }).returning({ id: praemienWavePillars.id }))[0]?.id;
    if (!pillarId) throw new Error("praemien_pillar_create_failed");
    if (previous) {
      await tx.update(praemienWavePillars).set(pillarValues).where(eq(praemienWavePillars.id, pillarId));
    }
    retainedPillarIds.add(pillarId);

    await tx
      .update(praemienWavePillarTierConditions)
      .set({ isDeleted: true, deletedAt: now, updatedAt: now })
      .where(and(eq(praemienWavePillarTierConditions.pillarId, pillarId), eq(praemienWavePillarTierConditions.isDeleted, false)));
    await tx
      .update(praemienWavePillarTiers)
      .set({ isDeleted: true, deletedAt: now, updatedAt: now })
      .where(and(eq(praemienWavePillarTiers.pillarId, pillarId), eq(praemienWavePillarTiers.isDeleted, false)));
    await tx
      .update(praemienWavePillarMetrics)
      .set({ isDeleted: true, deletedAt: now, updatedAt: now })
      .where(and(eq(praemienWavePillarMetrics.pillarId, pillarId), eq(praemienWavePillarMetrics.isDeleted, false)));

    const metricIdByKey = new Map<string, string>();
    for (const metric of entry.metrics) {
      const [createdMetric] = await tx.insert(praemienWavePillarMetrics).values({
        waveId,
        pillarId,
        key: metric.key,
        label: metric.label,
        unit: metric.unit,
        valueSource: metric.valueSource,
        sourceKey: metric.sourceKey ?? null,
        orderIndex: metric.orderIndex,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      }).returning({ id: praemienWavePillarMetrics.id });
      if (!createdMetric) throw new Error("praemien_pillar_metric_create_failed");
      metricIdByKey.set(metric.key, createdMetric.id);
    }

    for (const tier of entry.tiers) {
      const [createdTier] = await tx.insert(praemienWavePillarTiers).values({
        waveId,
        pillarId,
        label: tier.label,
        orderIndex: tier.orderIndex,
        rewardEur: String(tier.rewardEur),
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      }).returning({ id: praemienWavePillarTiers.id });
      if (!createdTier) throw new Error("praemien_pillar_tier_create_failed");
      for (const condition of tier.conditions) {
        const metricId = metricIdByKey.get(condition.metricKey);
        if (!metricId) throw new PraemienDomainError("pillar_tier_metric_missing", 400, "Auszahlungsstufe verweist auf eine unbekannte Messgröße.");
        await tx.insert(praemienWavePillarTierConditions).values({
          waveId,
          pillarId,
          tierId: createdTier.id,
          metricId,
          operator: condition.operator,
          thresholdValue: String(condition.thresholdValue),
          orderIndex: condition.orderIndex,
          isDeleted: false,
          deletedAt: null,
          createdAt: now,
          updatedAt: now,
        });
      }
    }
  }

  for (const existing of existingPillars) {
    if (retainedPillarIds.has(existing.id)) continue;
    await tx
      .update(praemienWaveSources)
      .set({ isDeleted: true, deletedAt: now, updatedAt: now })
      .where(and(eq(praemienWaveSources.pillarId, existing.id), eq(praemienWaveSources.isDeleted, false)));
  }
  await touchWaveTx(tx, waveId, now);
}

async function replaceSourcesTx(tx: Tx, waveId: string, sources: z.infer<typeof sourceInputSchema>[]) {
  const now = new Date();
  const pillars = await tx
    .select()
    .from(praemienWavePillars)
    .where(and(eq(praemienWavePillars.waveId, waveId), eq(praemienWavePillars.isDeleted, false)));
  const pillarById = new Map(pillars.map((entry) => [entry.id, entry]));
  const dedupeKeySet = new Set<string>();
  const liveCatalog = await buildSourceCatalog();
  const liveByKey = new Map(liveCatalog.map((entry) => [entry.key, entry]));

  for (const source of sources) {
    const scoringKey = source.scoringKey ?? source.scoreKey;
    if (!scoringKey) {
      throw new PraemienDomainError("source_scoring_key_required", 400, "Quell-Scoring-Key fehlt.");
    }
    const pillar = pillarById.get(source.pillarId);
    if (!pillar) throw new PraemienDomainError("source_invalid_pillar", 400, "Mindestens eine Quelle verweist auf eine ungültige Säule.");
    const dedupeKey = `${source.questionId}__${scoringKey}`;
    if (dedupeKeySet.has(dedupeKey)) {
      throw new PraemienDomainError("source_duplicate_question_score", 400, "Eine Frage/Score-Kombination wurde mehrfach zugewiesen.");
    }
    dedupeKeySet.add(dedupeKey);

    const catalogKey = sourceCatalogKey({
      sectionType: source.sectionType,
      fragebogenId: source.fragebogenId ?? null,
      moduleId: source.moduleId ?? null,
      questionId: source.questionId,
      scoringKey,
    });
    const live = liveByKey.get(catalogKey);
    if (!live) {
      throw new PraemienDomainError("source_not_in_live_catalog", 400, "Mindestens eine Quelle ist nicht mehr im gültigen Boni-Katalog vorhanden.");
    }
    const sameBoni = Math.abs(live.boniValue - source.boniValue) < 0.0001;
    if (!sameBoni) {
      throw new PraemienDomainError("source_boni_mismatch", 400, "Mindestens eine Quelle hat einen veralteten Boni-Wert.");
    }

    const isDistributionPillar = pillar.name === "Distributionsziel";
    if (!isDistributionPillar && source.distributionFreqRule != null) {
      throw new PraemienDomainError("distribution_rule_invalid_target", 400, "Frequenzregel ist nur für Distributionsziel erlaubt.");
    }
  }

  await tx
    .update(praemienWaveSources)
    .set({ isDeleted: true, deletedAt: now, updatedAt: now })
    .where(and(eq(praemienWaveSources.waveId, waveId), eq(praemienWaveSources.isDeleted, false)));

  for (const source of sources) {
    const scoringKey = source.scoringKey ?? source.scoreKey;
    if (!scoringKey) continue;
    await tx.insert(praemienWaveSources).values({
      waveId,
      pillarId: source.pillarId,
      sectionType: source.sectionType,
      fragebogenId: source.fragebogenId ?? null,
      fragebogenName: source.fragebogenName,
      moduleId: source.moduleId ?? null,
      moduleName: source.moduleName,
      questionId: source.questionId,
      questionText: source.questionText,
      scoreKey: scoringKey,
      displayLabel: source.displayLabel,
      isFactorMode: source.isFactorMode,
      boniValue: String(source.boniValue),
      distributionFreqRule: source.distributionFreqRule ?? null,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
  }
  await touchWaveTx(tx, waveId, now);
}

async function replaceQualityScoresTx(tx: Tx, waveId: string, qualityScores: z.infer<typeof qualityScoreInputSchema>[]) {
  const now = new Date();
  const gmIds = Array.from(new Set(qualityScores.map((entry) => entry.gmUserId)));
  const gmUsers = gmIds.length
    ? await tx
        .select({ id: users.id, role: users.role })
        .from(users)
        .where(and(inArray(users.id, gmIds), eq(users.isActive, true), eq(users.role, "gm"), sql`${users.deletedAt} is null`))
    : [];
  const validGmIdSet = new Set(gmUsers.map((entry) => entry.id));
  for (const entry of qualityScores) {
    if (!validGmIdSet.has(entry.gmUserId)) {
      throw new PraemienDomainError("quality_invalid_gm", 400, "Mindestens eine Qualitätsbewertung referenziert keinen gültigen GM.");
    }
  }

  await tx
    .update(praemienWaveQualityScores)
    .set({ isDeleted: true, deletedAt: now, updatedAt: now })
    .where(and(eq(praemienWaveQualityScores.waveId, waveId), eq(praemienWaveQualityScores.isDeleted, false)));

  for (const entry of qualityScores) {
    const totalPoints = Math.round((entry.zeiterfassung + entry.reporting + entry.accuracy) / 3);
    await tx.insert(praemienWaveQualityScores).values({
      waveId,
      gmUserId: entry.gmUserId,
      zeiterfassung: entry.zeiterfassung,
      reporting: entry.reporting,
      accuracy: entry.accuracy,
      totalPoints,
      note: entry.note ?? null,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
  }
  await touchWaveTx(tx, waveId, now);
}

async function replaceFlexScoresTx(tx: Tx, waveId: string, flexScores: z.infer<typeof flexScoreInputSchema>[]) {
  const now = new Date();
  const gmIds = Array.from(new Set(flexScores.map((entry) => entry.gmUserId)));
  const gmUsers = gmIds.length
    ? await tx
        .select({ id: users.id, role: users.role })
        .from(users)
        .where(and(inArray(users.id, gmIds), eq(users.isActive, true), eq(users.role, "gm"), sql`${users.deletedAt} is null`))
    : [];
  const validGmIdSet = new Set(gmUsers.map((entry) => entry.id));
  for (const entry of flexScores) {
    if (!validGmIdSet.has(entry.gmUserId)) {
      throw new PraemienDomainError("flex_invalid_gm", 400, "Mindestens eine Flexbewertung referenziert keinen gültigen GM.");
    }
  }

  await tx
    .update(praemienWaveFlexScores)
    .set({ isDeleted: true, deletedAt: now, updatedAt: now })
    .where(and(eq(praemienWaveFlexScores.waveId, waveId), eq(praemienWaveFlexScores.isDeleted, false)));

  for (const entry of flexScores) {
    await tx.insert(praemienWaveFlexScores).values({
      waveId,
      gmUserId: entry.gmUserId,
      totalPoints: entry.totalPoints,
      componentValues: entry.componentValues,
      note: entry.note ?? null,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
  }
  await touchWaveTx(tx, waveId, now);
}

function readWaveIdParam(req: AuthedRequest): string | null {
  const raw = req.params.waveId;
  if (typeof raw === "string" && raw.length > 0) return raw;
  if (Array.isArray(raw) && typeof raw[0] === "string" && raw[0].length > 0) return raw[0];
  return null;
}

function sendDomainError(res: Response, error: PraemienDomainError) {
  res.status(error.status).json({ error: error.message, code: error.code });
}

let checkedPraemienTables = false;
let hasPraemienTables = false;

async function ensurePraemienTablesReady(): Promise<boolean> {
  if (checkedPraemienTables) return hasPraemienTables;
  const result = await pgSql<{
    praemien_waves: string | null;
    praemien_wave_pillars: string | null;
    praemien_wave_pillar_metrics: string | null;
    praemien_wave_pillar_tiers: string | null;
    praemien_wave_pillar_tier_conditions: string | null;
    praemien_wave_thresholds: string | null;
    praemien_wave_sources: string | null;
    praemien_wave_quality_scores: string | null;
    praemien_wave_flex_scores: string | null;
  }[]>`
    select
      to_regclass('public.praemien_waves') as praemien_waves,
      to_regclass('public.praemien_wave_pillars') as praemien_wave_pillars,
      to_regclass('public.praemien_wave_pillar_metrics') as praemien_wave_pillar_metrics,
      to_regclass('public.praemien_wave_pillar_tiers') as praemien_wave_pillar_tiers,
      to_regclass('public.praemien_wave_pillar_tier_conditions') as praemien_wave_pillar_tier_conditions,
      to_regclass('public.praemien_wave_thresholds') as praemien_wave_thresholds,
      to_regclass('public.praemien_wave_sources') as praemien_wave_sources,
      to_regclass('public.praemien_wave_quality_scores') as praemien_wave_quality_scores,
      to_regclass('public.praemien_wave_flex_scores') as praemien_wave_flex_scores
  `;
  const row = result[0];
  hasPraemienTables = Boolean(
    row?.praemien_waves &&
    row?.praemien_wave_pillars &&
    row?.praemien_wave_pillar_metrics &&
    row?.praemien_wave_pillar_tiers &&
    row?.praemien_wave_pillar_tier_conditions &&
    row?.praemien_wave_thresholds &&
    row?.praemien_wave_sources &&
    row?.praemien_wave_quality_scores &&
    row?.praemien_wave_flex_scores,
  );
  // Cache only positive readiness. If DB is pushed later in a running dev process,
  // the next request should re-check and unlock the feature automatically.
  checkedPraemienTables = hasPraemienTables;
  return hasPraemienTables;
}

function sendPraemienNotInitialized(res: Response) {
  res.status(503).json({
    error: "Prämien-Backend ist noch nicht initialisiert.",
    code: "praemien_not_initialized",
  });
}

adminPraemienRouter.get("/waves", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = z
      .object({
        year: z.coerce.number().int().min(2000).max(2100).optional(),
        status: waveStatusSchema.optional(),
        limit: z.coerce.number().int().min(1).max(200).optional().default(50),
        offset: z.coerce.number().int().min(0).optional().default(0),
      })
      .safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Prämien-Wellen Filter." });
      return;
    }
    if (!(await ensurePraemienTablesReady())) {
      res.status(200).json({
        waves: [],
        limit: parsed.data.limit,
        offset: parsed.data.offset,
        total: 0,
      });
      return;
    }
    const whereParts = [eq(praemienWaves.isDeleted, false)];
    if (parsed.data.year != null) whereParts.push(eq(praemienWaves.year, parsed.data.year));
    if (parsed.data.status) whereParts.push(eq(praemienWaves.status, parsed.data.status));
    const totalRows = await db.select({ total: sql<number>`count(*)` }).from(praemienWaves).where(and(...whereParts));
    const rows = await db
      .select()
      .from(praemienWaves)
      .where(and(...whereParts))
      .orderBy(asc(praemienWaves.year), asc(praemienWaves.quarter), asc(praemienWaves.createdAt))
      .limit(parsed.data.limit)
      .offset(parsed.data.offset);
    res.status(200).json({
      waves: rows.map((row) => ({
        id: row.id,
        name: row.name,
        year: row.year,
        quarter: row.quarter,
        startDate: row.startDate,
        endDate: row.endDate,
        status: row.status,
        description: row.description,
        timezone: row.timezone,
        rewardModel: row.rewardModel,
        createdAt: row.createdAt.toISOString(),
        updatedAt: row.updatedAt.toISOString(),
      })),
      limit: parsed.data.limit,
      offset: parsed.data.offset,
      total: Number(totalRows[0]?.total ?? 0),
    });
  } catch (error) {
    next(error);
  }
});

adminPraemienRouter.post("/waves", async (req: AuthedRequest, res, next) => {
  try {
    if (!(await ensurePraemienTablesReady())) {
      sendPraemienNotInitialized(res);
      return;
    }
    const parsed = createWaveSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Daten für Prämien-Welle.", code: "invalid_payload" });
      return;
    }
    ensureWaveDateRange(parsed.data.startDate, parsed.data.endDate);
    if (parsed.data.thresholds.length > 0) {
      ensureThresholdsStrictlyAscending(parsed.data.thresholds.map((entry) => ({
        label: entry.label,
        orderIndex: entry.orderIndex,
        minPoints: entry.minPoints,
      })));
    }
    const created = await db.transaction(async (tx) => {
      const now = new Date();
      const [wave] = await tx
        .insert(praemienWaves)
        .values({
          name: parsed.data.name,
          year: parsed.data.year,
          quarter: parsed.data.quarter,
          startDate: parsed.data.startDate,
          endDate: parsed.data.endDate,
          status: parsed.data.status,
          description: parsed.data.description,
          timezone: parsed.data.timezone,
          rewardModel: parsed.data.rewardModel,
          createdAt: now,
          updatedAt: now,
        })
        .returning();
      if (!wave) throw new Error("wave_create_failed");

      if (parsed.data.pillars.length > 0) {
        await replacePillarsTx(tx, wave.id, parsed.data.pillars);
      }
      if (parsed.data.thresholds.length > 0) {
        await replaceThresholdsTx(tx, wave.id, parsed.data.thresholds);
      }
      return wave;
    });
    const payload = await loadWaveGraph(created.id);
    logMutation(req, "create_wave", { waveId: created.id, thresholds: payload?.thresholds.length ?? 0, pillars: payload?.pillars.length ?? 0 });
    res.status(201).json({ wave: payload });
  } catch (error) {
    if (error instanceof PraemienDomainError) {
      sendDomainError(res, error);
      return;
    }
    if (isPgUniqueViolation(error)) {
      res.status(409).json({ error: "Für dieses Jahr/Quartal existiert bereits eine aktive Welle.", code: "wave_active_conflict" });
      return;
    }
    next(error);
  }
});

adminPraemienRouter.get("/waves/:waveId", async (req: AuthedRequest, res, next) => {
  try {
    if (!(await ensurePraemienTablesReady())) {
      sendPraemienNotInitialized(res);
      return;
    }
    const waveId = readWaveIdParam(req);
    if (!waveId) {
      res.status(400).json({ error: "Wave-ID fehlt.", code: "wave_id_missing" });
      return;
    }
    const payload = await loadWaveGraph(waveId);
    if (!payload) {
      res.status(404).json({ error: "Prämien-Welle nicht gefunden.", code: "wave_not_found" });
      return;
    }
    res.status(200).json({ wave: payload });
  } catch (error) {
    next(error);
  }
});

adminPraemienRouter.patch("/waves/:waveId", async (req: AuthedRequest, res, next) => {
  try {
    if (!(await ensurePraemienTablesReady())) {
      sendPraemienNotInitialized(res);
      return;
    }
    const waveId = readWaveIdParam(req);
    if (!waveId) {
      res.status(400).json({ error: "Wave-ID fehlt.", code: "wave_id_missing" });
      return;
    }
    const parsed = updateWaveSchema.extend({ expectedUpdatedAt: isoDatetimeSchema.optional() }).safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Wellen-?nderungen.", code: "invalid_payload" });
      return;
    }
    const shouldScheduleRecompute = await db.transaction(async (tx) => {
      await lockWaveTx(tx, waveId, parsed.data.expectedUpdatedAt);
      const [existing] = await tx
        .select()
        .from(praemienWaves)
        .where(and(eq(praemienWaves.id, waveId), eq(praemienWaves.isDeleted, false)))
        .limit(1);
      if (!existing) throw new PraemienDomainError("wave_not_found", 404, "Prämien-Welle nicht gefunden.");
      const nextStartDate = parsed.data.startDate ?? existing.startDate;
      const nextEndDate = parsed.data.endDate ?? existing.endDate;
      ensureWaveDateRange(nextStartDate, nextEndDate);
      const shouldRecompute =
        parsed.data.startDate !== undefined ||
        parsed.data.endDate !== undefined ||
        parsed.data.status !== undefined ||
        parsed.data.timezone !== undefined ||
        parsed.data.rewardModel !== undefined;
      await tx
        .update(praemienWaves)
        .set({
          name: parsed.data.name,
          year: parsed.data.year,
          quarter: parsed.data.quarter,
          startDate: parsed.data.startDate,
          endDate: parsed.data.endDate,
          status: parsed.data.status,
          description: parsed.data.description,
          timezone: parsed.data.timezone,
          rewardModel: parsed.data.rewardModel,
          updatedAt: new Date(),
        })
        .where(eq(praemienWaves.id, waveId));
      return shouldRecompute;
    });
    if (shouldScheduleRecompute) {
      scheduleBonusWaveRecompute(waveId, "patch_wave");
    }
    const payload = await loadWaveGraph(waveId);
    logMutation(req, "patch_wave", {
      waveId,
      recomputeScheduled: shouldScheduleRecompute,
    });
    res.status(200).json({ wave: payload });
  } catch (error) {
    if (error instanceof PraemienDomainError) {
      sendDomainError(res, error);
      return;
    }
    if (isPgUniqueViolation(error)) {
      res.status(409).json({ error: "Für dieses Jahr/Quartal existiert bereits eine aktive Welle.", code: "wave_active_conflict" });
      return;
    }
    next(error);
  }
});

adminPraemienRouter.put("/waves/:waveId/thresholds", async (req: AuthedRequest, res, next) => {
  try {
    if (!(await ensurePraemienTablesReady())) {
      sendPraemienNotInitialized(res);
      return;
    }
    const waveId = readWaveIdParam(req);
    if (!waveId) {
      res.status(400).json({ error: "Wave-ID fehlt.", code: "wave_id_missing" });
      return;
    }
    const parsed = replaceThresholdsSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Schwellwerte.", code: "invalid_payload" });
      return;
    }
    await db.transaction(async (tx) => {
      await lockWaveTx(tx, waveId, parsed.data.expectedUpdatedAt);
      await replaceThresholdsTx(tx, waveId, parsed.data.thresholds);
    });
    scheduleBonusWaveRecompute(waveId, "replace_thresholds");
    const payload = await loadWaveGraph(waveId);
    logMutation(req, "replace_thresholds", {
      waveId,
      count: parsed.data.thresholds.length,
      recomputeScheduled: true,
    });
    res.status(200).json({ wave: payload });
  } catch (error) {
    if (error instanceof PraemienDomainError) {
      sendDomainError(res, error);
      return;
    }
    next(error);
  }
});

adminPraemienRouter.put("/waves/:waveId/pillars", async (req: AuthedRequest, res, next) => {
  try {
    if (!(await ensurePraemienTablesReady())) {
      sendPraemienNotInitialized(res);
      return;
    }
    const waveId = readWaveIdParam(req);
    if (!waveId) {
      res.status(400).json({ error: "Wave-ID fehlt.", code: "wave_id_missing" });
      return;
    }
    const parsed = replacePillarsSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Säulen.", code: "invalid_payload" });
      return;
    }
    await db.transaction(async (tx) => {
      await lockWaveTx(tx, waveId, parsed.data.expectedUpdatedAt);
      await replacePillarsTx(tx, waveId, parsed.data.pillars);
    });
    scheduleBonusWaveRecompute(waveId, "replace_pillars");
    const payload = await loadWaveGraph(waveId);
    logMutation(req, "replace_pillars", {
      waveId,
      count: parsed.data.pillars.length,
      recomputeScheduled: true,
    });
    res.status(200).json({ wave: payload });
  } catch (error) {
    if (error instanceof PraemienDomainError) {
      sendDomainError(res, error);
      return;
    }
    next(error);
  }
});

adminPraemienRouter.put("/waves/:waveId/sources", async (req: AuthedRequest, res, next) => {
  try {
    if (!(await ensurePraemienTablesReady())) {
      sendPraemienNotInitialized(res);
      return;
    }
    const waveId = readWaveIdParam(req);
    if (!waveId) {
      res.status(400).json({ error: "Wave-ID fehlt.", code: "wave_id_missing" });
      return;
    }
    const parsed = replaceSourcesSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Quellenzuordnung.", code: "invalid_payload" });
      return;
    }
    await db.transaction(async (tx) => {
      await lockWaveTx(tx, waveId, parsed.data.expectedUpdatedAt);
      await replaceSourcesTx(tx, waveId, parsed.data.sources);
    });
    scheduleBonusWaveRecompute(waveId, "replace_sources");
    const payload = await loadWaveGraph(waveId);
    logMutation(req, "replace_sources", {
      waveId,
      count: parsed.data.sources.length,
      recomputeScheduled: true,
    });
    res.status(200).json({ wave: payload });
  } catch (error) {
    if (error instanceof PraemienDomainError) {
      sendDomainError(res, error);
      return;
    }
    next(error);
  }
});

adminPraemienRouter.put("/waves/:waveId/quality-scores", async (req: AuthedRequest, res, next) => {
  try {
    if (!(await ensurePraemienTablesReady())) {
      sendPraemienNotInitialized(res);
      return;
    }
    const waveId = readWaveIdParam(req);
    if (!waveId) {
      res.status(400).json({ error: "Wave-ID fehlt.", code: "wave_id_missing" });
      return;
    }
    const parsed = replaceQualityScoresSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Qualitätsbewertungen.", code: "invalid_payload" });
      return;
    }
    await db.transaction(async (tx) => {
      await lockWaveTx(tx, waveId, parsed.data.expectedUpdatedAt);
      await replaceQualityScoresTx(tx, waveId, parsed.data.qualityScores);
    });
    const payload = await loadWaveGraph(waveId);
    logMutation(req, "replace_quality_scores", { waveId, count: parsed.data.qualityScores.length });
    res.status(200).json({ wave: payload });
  } catch (error) {
    if (error instanceof PraemienDomainError) {
      sendDomainError(res, error);
      return;
    }
    next(error);
  }
});

adminPraemienRouter.put("/waves/:waveId/flex-scores", async (req: AuthedRequest, res, next) => {
  try {
    if (!(await ensurePraemienTablesReady())) {
      sendPraemienNotInitialized(res);
      return;
    }
    const waveId = readWaveIdParam(req);
    if (!waveId) {
      res.status(400).json({ error: "Wave-ID fehlt.", code: "wave_id_missing" });
      return;
    }
    const parsed = replaceFlexScoresSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Flexbewertungen.", code: "invalid_payload" });
      return;
    }
    await db.transaction(async (tx) => {
      await lockWaveTx(tx, waveId, parsed.data.expectedUpdatedAt);
      await replaceFlexScoresTx(tx, waveId, parsed.data.flexScores);
    });
    const payload = await loadWaveGraph(waveId);
    logMutation(req, "replace_flex_scores", { waveId, count: parsed.data.flexScores.length });
    res.status(200).json({ wave: payload });
  } catch (error) {
    if (error instanceof PraemienDomainError) {
      sendDomainError(res, error);
      return;
    }
    next(error);
  }
});

adminPraemienRouter.patch("/waves/:waveId/delete", async (req: AuthedRequest, res, next) => {
  try {
    if (!(await ensurePraemienTablesReady())) {
      sendPraemienNotInitialized(res);
      return;
    }
    const waveId = readWaveIdParam(req);
    if (!waveId) {
      res.status(400).json({ error: "Wave-ID fehlt.", code: "wave_id_missing" });
      return;
    }
    const now = new Date();
    const [updated] = await db
      .update(praemienWaves)
      .set({ isDeleted: true, deletedAt: now, updatedAt: now })
      .where(and(eq(praemienWaves.id, waveId), eq(praemienWaves.isDeleted, false)))
      .returning({ id: praemienWaves.id });
    if (!updated) {
      res.status(404).json({ error: "Prämien-Welle nicht gefunden.", code: "wave_not_found" });
      return;
    }
    logMutation(req, "delete_wave", { waveId });
    res.status(200).json({ ok: true, waveId });
  } catch (error) {
    next(error);
  }
});

adminPraemienRouter.get("/sources", async (_req: AuthedRequest, res, next) => {
  try {
    const rows = await buildSourceCatalog();
    res.status(200).json({
      sources: rows.map((row: BonusSourceCatalogRow) => ({
        ...row,
        scoringKey: row.scoringKey,
        scoreKey: row.scoringKey,
      })),
    });
  } catch (error) {
    next(error);
  }
});

export { adminPraemienRouter };
