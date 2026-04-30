import { and, asc, eq, inArray, sql } from "drizzle-orm";
import { type Response, Router } from "express";
import { z } from "zod";
import { env } from "../config/env.js";
import { db, sql as pgSql } from "../lib/db.js";
import {
  buildSourceCatalog,
  normalizeMoney,
  sourceCatalogKey,
  type BonusSourceCatalogRow,
  type PraemienSectionType,
} from "../lib/praemien-source-catalog.js";
import {
  praemienWavePillars,
  praemienWaveQualityScores,
  praemienWaveSources,
  praemienWaveThresholds,
  praemienWaves,
  users,
} from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const adminPraemienRouter = Router();
adminPraemienRouter.use(requireAuth(["admin"]));

const waveStatusSchema = z.enum(["draft", "active", "archived"]);
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

const pillarInputSchema = z.object({
  id: z.string().uuid().optional(),
  name: z.string().trim().min(1).max(120),
  description: z.string().trim().max(1000).optional().default(""),
  color: z.string().trim().min(1).max(40).optional().default("#DC2626"),
  orderIndex: z.number().int().min(0),
  isManual: z.boolean().optional().default(false),
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

const createWaveSchema = z.object({
  name: z.string().trim().min(1).max(180),
  year: z.number().int().min(2000).max(2100),
  quarter: z.number().int().min(1).max(4),
  startDate: dateYmdSchema,
  endDate: dateYmdSchema,
  status: waveStatusSchema.optional().default("draft"),
  description: z.string().trim().max(2000).optional().default(""),
  timezone: z.string().trim().min(1).max(120).optional().default("Europe/Vienna"),
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
      throw new PraemienDomainError("threshold_order_duplicate", 400, "Schwellwert-Reihenfolge enthaelt Duplikate.");
    }
    orderSet.add(threshold.orderIndex);
    const labelKey = threshold.label.trim().toLowerCase();
    if (labelSet.has(labelKey)) {
      throw new PraemienDomainError("threshold_label_duplicate", 400, "Schwellwert-Labels muessen eindeutig sein.");
    }
    labelSet.add(labelKey);
  }

  const sorted = [...thresholds].sort((a, b) => a.minPoints - b.minPoints);
  for (let index = 1; index < sorted.length; index += 1) {
    const current = sorted[index];
    const previous = sorted[index - 1];
    if (!current || !previous) continue;
    if (current.minPoints <= previous.minPoints) {
      throw new PraemienDomainError("thresholds_not_strictly_ascending", 400, "Schwellwerte muessen strikt aufsteigend sein.");
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

  const [thresholds, pillars, sources, qualityRows] = await Promise.all([
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
  ]);

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
  };
  if (env.NODE_ENV !== "production") {
    waveResponseSchema.parse(payload);
  }
  return payload;
}

function logMutation(req: AuthedRequest, action: string, payload: Record<string, unknown>) {
  console.info("[praemien]", {
    action,
    actorUserId: req.authUser?.appUserId ?? null,
    at: new Date().toISOString(),
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
  if (!locked) throw new PraemienDomainError("wave_not_found", 404, "Praemien-Welle nicht gefunden.");
  if (expectedUpdatedAt) {
    const current = parseDateish(locked.updatedAt);
    const expected = parseDateish(expectedUpdatedAt);
    if (!current || !expected || current.toISOString() !== expected.toISOString()) {
      throw new PraemienDomainError("wave_stale_write", 409, "Die Praemien-Welle wurde zwischenzeitlich geaendert.");
    }
  }
}

async function touchWaveTx(tx: Tx, waveId: string, now: Date) {
  await tx.update(praemienWaves).set({ updatedAt: now }).where(eq(praemienWaves.id, waveId));
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
      throw new PraemienDomainError("pillar_order_duplicate", 400, "Saeulen-Reihenfolge enthaelt Duplikate.");
    }
    orderSet.add(pillar.orderIndex);
    const normalizedName = pillar.name.trim().toLowerCase();
    if (nameSet.has(normalizedName)) {
      throw new PraemienDomainError("pillar_name_duplicate", 400, "Saeulen-Namen muessen eindeutig sein.");
    }
    nameSet.add(normalizedName);
  }

  const now = new Date();
  await tx
    .update(praemienWaveSources)
    .set({ isDeleted: true, deletedAt: now, updatedAt: now })
    .where(and(eq(praemienWaveSources.waveId, waveId), eq(praemienWaveSources.isDeleted, false)));
  await tx
    .update(praemienWavePillars)
    .set({ isDeleted: true, deletedAt: now, updatedAt: now })
    .where(and(eq(praemienWavePillars.waveId, waveId), eq(praemienWavePillars.isDeleted, false)));

  for (const entry of pillars) {
    await tx.insert(praemienWavePillars).values({
      waveId,
      name: entry.name,
      description: entry.description,
      color: entry.color,
      orderIndex: entry.orderIndex,
      isManual: entry.isManual,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
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
    if (!pillar) throw new PraemienDomainError("source_invalid_pillar", 400, "Mindestens eine Quelle verweist auf eine ungueltige Saeule.");
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
      throw new PraemienDomainError("source_not_in_live_catalog", 400, "Mindestens eine Quelle ist nicht mehr im gueltigen Boni-Katalog vorhanden.");
    }
    const sameBoni = Math.abs(live.boniValue - source.boniValue) < 0.0001;
    if (!sameBoni) {
      throw new PraemienDomainError("source_boni_mismatch", 400, "Mindestens eine Quelle hat einen veralteten Boni-Wert.");
    }

    const isDistributionPillar = pillar.name === "Distributionsziel";
    if (!isDistributionPillar && source.distributionFreqRule != null) {
      throw new PraemienDomainError("distribution_rule_invalid_target", 400, "Frequenzregel ist nur fuer Distributionsziel erlaubt.");
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
      throw new PraemienDomainError("quality_invalid_gm", 400, "Mindestens eine Qualitaetsbewertung referenziert keinen gueltigen GM.");
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
    praemien_wave_thresholds: string | null;
    praemien_wave_sources: string | null;
    praemien_wave_quality_scores: string | null;
  }[]>`
    select
      to_regclass('public.praemien_waves') as praemien_waves,
      to_regclass('public.praemien_wave_pillars') as praemien_wave_pillars,
      to_regclass('public.praemien_wave_thresholds') as praemien_wave_thresholds,
      to_regclass('public.praemien_wave_sources') as praemien_wave_sources,
      to_regclass('public.praemien_wave_quality_scores') as praemien_wave_quality_scores
  `;
  const row = result[0];
  hasPraemienTables = Boolean(
    row?.praemien_waves &&
    row?.praemien_wave_pillars &&
    row?.praemien_wave_thresholds &&
    row?.praemien_wave_sources &&
    row?.praemien_wave_quality_scores,
  );
  // Cache only positive readiness. If DB is pushed later in a running dev process,
  // the next request should re-check and unlock the feature automatically.
  checkedPraemienTables = hasPraemienTables;
  return hasPraemienTables;
}

function sendPraemienNotInitialized(res: Response) {
  res.status(503).json({
    error: "Praemien-Backend ist noch nicht initialisiert.",
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
      res.status(400).json({ error: "Ungueltige Praemien-Wellen Filter." });
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
      res.status(400).json({ error: "Ungueltige Daten fuer Praemien-Welle.", code: "invalid_payload" });
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
      res.status(409).json({ error: "Fuer dieses Jahr/Quartal existiert bereits eine aktive Welle.", code: "wave_active_conflict" });
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
      res.status(404).json({ error: "Praemien-Welle nicht gefunden.", code: "wave_not_found" });
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
      res.status(400).json({ error: "Ungueltige Wellen-Aenderungen.", code: "invalid_payload" });
      return;
    }
    await db.transaction(async (tx) => {
      await lockWaveTx(tx, waveId, parsed.data.expectedUpdatedAt);
      const [existing] = await tx
        .select()
        .from(praemienWaves)
        .where(and(eq(praemienWaves.id, waveId), eq(praemienWaves.isDeleted, false)))
        .limit(1);
      if (!existing) throw new PraemienDomainError("wave_not_found", 404, "Praemien-Welle nicht gefunden.");
      const nextStartDate = parsed.data.startDate ?? existing.startDate;
      const nextEndDate = parsed.data.endDate ?? existing.endDate;
      ensureWaveDateRange(nextStartDate, nextEndDate);
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
          updatedAt: new Date(),
        })
        .where(eq(praemienWaves.id, waveId));
    });
    const payload = await loadWaveGraph(waveId);
    logMutation(req, "patch_wave", { waveId });
    res.status(200).json({ wave: payload });
  } catch (error) {
    if (error instanceof PraemienDomainError) {
      sendDomainError(res, error);
      return;
    }
    if (isPgUniqueViolation(error)) {
      res.status(409).json({ error: "Fuer dieses Jahr/Quartal existiert bereits eine aktive Welle.", code: "wave_active_conflict" });
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
      res.status(400).json({ error: "Ungueltige Schwellwerte.", code: "invalid_payload" });
      return;
    }
    await db.transaction(async (tx) => {
      await lockWaveTx(tx, waveId, parsed.data.expectedUpdatedAt);
      await replaceThresholdsTx(tx, waveId, parsed.data.thresholds);
    });
    const payload = await loadWaveGraph(waveId);
    logMutation(req, "replace_thresholds", { waveId, count: parsed.data.thresholds.length });
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
      res.status(400).json({ error: "Ungueltige Saeulen.", code: "invalid_payload" });
      return;
    }
    await db.transaction(async (tx) => {
      await lockWaveTx(tx, waveId, parsed.data.expectedUpdatedAt);
      await replacePillarsTx(tx, waveId, parsed.data.pillars);
    });
    const payload = await loadWaveGraph(waveId);
    logMutation(req, "replace_pillars", { waveId, count: parsed.data.pillars.length });
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
      res.status(400).json({ error: "Ungueltige Quellenzuordnung.", code: "invalid_payload" });
      return;
    }
    await db.transaction(async (tx) => {
      await lockWaveTx(tx, waveId, parsed.data.expectedUpdatedAt);
      await replaceSourcesTx(tx, waveId, parsed.data.sources);
    });
    const payload = await loadWaveGraph(waveId);
    logMutation(req, "replace_sources", { waveId, count: parsed.data.sources.length });
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
      res.status(400).json({ error: "Ungueltige Qualitaetsbewertungen.", code: "invalid_payload" });
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
      res.status(404).json({ error: "Praemien-Welle nicht gefunden.", code: "wave_not_found" });
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
