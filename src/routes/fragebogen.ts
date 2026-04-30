import { and, asc, desc, eq, inArray, sql } from "drizzle-orm";
import { type NextFunction, type Request, type Response, Router } from "express";
import { z } from "zod";
import { db, sql as pgSql } from "../lib/db.js";
import { enqueueIppRecalcForQuestionScoringChanges } from "../lib/ipp-finalizer.js";
import { requireAuth } from "../middleware/auth.js";
import {
  fragebogenKuehler,
  fragebogenKuehlerModule,
  fragebogenMain,
  fragebogenMainModule,
  fragebogenMainSpezialItems,
  fragebogenMhd,
  fragebogenMhdModule,
  moduleKuehler,
  moduleKuehlerQuestion,
  moduleMain,
  moduleMainQuestion,
  moduleMhd,
  moduleMhdQuestion,
  moduleQuestionChains,
  markets,
  photoTags,
  questionAttachments,
  questionBankShared,
  questionMatrix,
  questionPhotoTags,
  questionRules,
  questionRuleTargets,
  questionScoring,
} from "../lib/schema.js";

const adminFragebogenRouter = Router();
adminFragebogenRouter.use(requireAuth(["admin"]));

const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const isUuid = (value: string | undefined | null): value is string => Boolean(value && uuidRegex.test(value));

const scopeSchema = z.enum(["main", "kuehler", "mhd"]);
const mainSectionSchema = z.enum(["standard", "flex", "billa"]);

const questionTypeSchema = z.enum([
  "single",
  "yesno",
  "yesnomulti",
  "multiple",
  "likert",
  "text",
  "numeric",
  "slider",
  "photo",
  "matrix",
]);

const scoreValueSchema = z
  .object({
    ipp: z.number().finite().gte(-99999).lte(99999).optional(),
    boni: z.number().finite().gte(-99999).lte(99999).optional(),
  })
  .strict();

const ruleSchema = z
  .object({
    id: z.string().optional(),
    triggerQuestionId: z.string().optional().default(""),
    operator: z.string().min(1).default("equals"),
    triggerValue: z.string().default(""),
    triggerValueMax: z.string().default(""),
    action: z.enum(["hide", "show"]).default("hide"),
    targetQuestionIds: z.array(z.string()).default([]),
  })
  .strict();

const questionSchema = z
  .object({
    id: z.string().optional(),
    type: questionTypeSchema,
    text: z.string().default(""),
    required: z.boolean().default(true),
    config: z.record(z.string(), z.unknown()).default({}),
    rules: z.array(ruleSchema).default([]),
    scoring: z.record(z.string(), scoreValueSchema).default({}),
    chains: z.array(z.string()).optional(),
  })
  .strict();

const moduleSchema = z
  .object({
    id: z.string().optional(),
    name: z.string().min(1),
    description: z.string().default(""),
    questions: z.array(questionSchema).default([]),
    sectionKeywords: z.array(mainSectionSchema).optional(),
    createdAt: z.string().optional(),
    usedInCount: z.number().optional(),
  })
  .strict();

const fragebogenSchema = z
  .object({
    id: z.string().optional(),
    name: z.string().min(1),
    description: z.string().default(""),
    moduleIds: z.array(z.string()).default([]),
    scheduleType: z.enum(["always", "scheduled"]).default("always"),
    startDate: z.string().optional(),
    endDate: z.string().optional(),
    status: z.enum(["active", "scheduled", "inactive"]).default("inactive"),
    sectionKeywords: z.array(mainSectionSchema).optional(),
    nurEinmalAusfuellbar: z.boolean().default(false),
    spezialfragen: z.array(questionSchema).default([]),
    createdAt: z.string().optional(),
  })
  .strict();

type Scope = z.infer<typeof scopeSchema>;
type MainSection = z.infer<typeof mainSectionSchema>;
type UiQuestion = z.infer<typeof questionSchema>;
type UiModule = z.infer<typeof moduleSchema>;
type UiFragebogen = z.infer<typeof fragebogenSchema>;
type DbTx = Parameters<Parameters<typeof db.transaction>[0]>[0];
type DbLike = typeof db | DbTx;
type AnyTable = any;
let checkedModuleQuestionChainsReadiness = false;
let hasModuleQuestionChainsTable = false;

class DomainValidationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "DomainValidationError";
  }
}

async function ensureModuleQuestionChainsReady(): Promise<boolean> {
  if (checkedModuleQuestionChainsReadiness) return hasModuleQuestionChainsTable;
  const result = await pgSql<{ module_question_chains: string | null }[]>`
    select to_regclass('public.module_question_chains') as module_question_chains
  `;
  hasModuleQuestionChainsTable = Boolean(result[0]?.module_question_chains);
  checkedModuleQuestionChainsReadiness = true;
  return hasModuleQuestionChainsTable;
}

function getScopeParam(params: Record<string, string | undefined>): Scope {
  return scopeSchema.parse(params.scope ?? params["scope(main|kuehler|mhd)"]);
}

function parseArrayField(raw: unknown): string[] {
  if (Array.isArray(raw)) {
    return raw.filter((entry): entry is string => typeof entry === "string");
  }
  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? parsed.filter((entry): entry is string => typeof entry === "string") : [];
    } catch {
      return [];
    }
  }
  return [];
}

function normalizeChainDbNames(rawChains: string[] | undefined): string[] {
  if (!rawChains) return [];
  return Array.from(
    new Set(
      rawChains
        .map((entry) => entry.trim())
        .filter((entry) => entry.length > 0),
    ),
  );
}

function ensureAllUuids(ids: string[], fieldName: string) {
  const invalid = ids.filter((id) => !isUuid(id));
  if (invalid.length > 0) {
    throw new DomainValidationError(`Ungueltige ${fieldName}-IDs gefunden.`);
  }
}

function parseIsoDateOrThrow(value: string, fieldName: string): string {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) {
    throw new DomainValidationError(`${fieldName} muss im Format YYYY-MM-DD sein.`);
  }
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (Number.isNaN(date.getTime())) {
    throw new DomainValidationError(`${fieldName} ist kein gueltiges Datum.`);
  }
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() + 1 !== month ||
    date.getUTCDate() !== day
  ) {
    throw new DomainValidationError(`${fieldName} ist kein gueltiges Kalenderdatum.`);
  }
  return value;
}

function validateQuestionDomain(question: UiQuestion) {
  const config = (question.config ?? {}) as Record<string, unknown>;
  const scoringKeys = Object.keys(question.scoring ?? {});
  if ((question.type === "numeric" || question.type === "slider") && scoringKeys.some((key) => key !== "__value__")) {
    throw new DomainValidationError("Numerische Fragen duerfen nur den Scoring-Key '__value__' verwenden.");
  }

  if (question.type === "single" || question.type === "multiple") {
    const options = parseArrayField(config.options);
    if (options.length === 0) {
      throw new DomainValidationError("Auswahlfragen benoetigen mindestens eine Option.");
    }
  }

  if (question.type === "yesnomulti") {
    const rawAnswers = parseArrayField(config.answers)
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0);
    const legacyAnswers = parseArrayField(config.options)
      .map((entry) => entry.trim())
      .filter((entry) => entry.length > 0);
    const answers = Array.from(new Set(rawAnswers.length > 0 ? rawAnswers : legacyAnswers));
    if (answers.length === 0) {
      throw new DomainValidationError("Yes/No-Multi Fragen benoetigen mindestens eine Antwort.");
    }

    const branchEntries: Array<{ answer: string; enabled: boolean; options: string[] }> = [];
    const rawBranches = config.branches;
    if (Array.isArray(rawBranches)) {
      for (const rawBranch of rawBranches) {
        if (!rawBranch || typeof rawBranch !== "object") continue;
        const branchRecord = rawBranch as Record<string, unknown>;
        const answer = typeof branchRecord.answer === "string" ? branchRecord.answer.trim() : "";
        if (!answer) continue;
        const options = parseArrayField(branchRecord.options)
          .map((entry) => entry.trim())
          .filter((entry) => entry.length > 0);
        const enabled = typeof branchRecord.enabled === "boolean" ? branchRecord.enabled : true;
        branchEntries.push({ answer, enabled, options });
      }
    } else if (rawBranches && typeof rawBranches === "object") {
      for (const [branchAnswer, branchConfig] of Object.entries(rawBranches as Record<string, unknown>)) {
        if (!branchConfig || typeof branchConfig !== "object") continue;
        const branchRecord = branchConfig as Record<string, unknown>;
        const options = parseArrayField(branchRecord.options)
          .map((entry) => entry.trim())
          .filter((entry) => entry.length > 0);
        const enabled = Boolean(branchRecord.enabled);
        branchEntries.push({ answer: branchAnswer.trim(), enabled, options });
      }
    }
    for (const branch of branchEntries) {
      if (!answers.includes(branch.answer)) {
        throw new DomainValidationError("Yes/No-Multi Branch verweist auf eine ungueltige Antwort.");
      }
      if (branch.enabled && branch.options.length === 0) {
        throw new DomainValidationError("Yes/No-Multi Branches mit Multi-Auswahl benoetigen mindestens eine Option.");
      }
    }

    const legacyTrigger = typeof config.triggerAnswer === "string" ? config.triggerAnswer.trim() : "";
    if (legacyTrigger) {
      if (!answers.includes(legacyTrigger)) {
        throw new DomainValidationError("Yes/No-Multi Trigger verweist auf eine ungueltige Antwort.");
      }
      const legacyOptions = parseArrayField(config.options)
        .map((entry) => entry.trim())
        .filter((entry) => entry.length > 0);
      if (legacyOptions.length === 0) {
        throw new DomainValidationError("Yes/No-Multi Trigger mit Multi-Auswahl benoetigt mindestens eine Option.");
      }
    }
  }

  if (question.type === "likert") {
    const labels = parseArrayField(config.labels);
    const min = typeof config.min === "number" ? config.min : undefined;
    const max = typeof config.max === "number" ? config.max : undefined;
    const validLabels = labels.length >= 2;
    const validRange = min != null && max != null && Number.isFinite(min) && Number.isFinite(max) && min < max;
    if (!validLabels && !validRange) {
      throw new DomainValidationError("Likert-Fragen benoetigen Labels oder einen gueltigen min/max Bereich.");
    }
  }

  if (question.type === "photo") {
    const tagsEnabled = Boolean(config.tagsEnabled);
    const tagIds = parseArrayField(config.tagIds);
    if (tagsEnabled && tagIds.length === 0) {
      throw new DomainValidationError("Foto-Fragen mit aktivierten Tags benoetigen mindestens ein Tag.");
    }
  }

  if (question.type === "text") {
    const minLength = typeof config.minLength === "number" ? config.minLength : undefined;
    const maxLength = typeof config.maxLength === "number" ? config.maxLength : undefined;
    if (minLength != null && (!Number.isInteger(minLength) || minLength < 0)) {
      throw new DomainValidationError("Textfragen minLength muss eine positive Ganzzahl sein.");
    }
    if (maxLength != null && (!Number.isInteger(maxLength) || maxLength < 1)) {
      throw new DomainValidationError("Textfragen maxLength muss eine positive Ganzzahl sein.");
    }
    if (minLength != null && maxLength != null && minLength > maxLength) {
      throw new DomainValidationError("Textfragen minLength darf nicht groesser als maxLength sein.");
    }
  }

  if (question.type === "numeric" || question.type === "slider") {
    const min = typeof config.min === "number" ? config.min : undefined;
    const max = typeof config.max === "number" ? config.max : undefined;
    if (min != null && !Number.isFinite(min)) {
      throw new DomainValidationError("Numerische Fragen min muss eine gueltige Zahl sein.");
    }
    if (max != null && !Number.isFinite(max)) {
      throw new DomainValidationError("Numerische Fragen max muss eine gueltige Zahl sein.");
    }
    if (min != null && max != null && min > max) {
      throw new DomainValidationError("Numerische Fragen min darf nicht groesser als max sein.");
    }
  }

  if (question.type === "matrix") {
    const rows = parseArrayField(config.rows);
    const columns = parseArrayField(config.columns);
    if (rows.length === 0 || columns.length === 0) {
      throw new DomainValidationError("Matrix-Fragen benoetigen mindestens eine Zeile und eine Spalte.");
    }
  }
}

async function ensureQuestionRefsExist(dbLike: DbLike, ids: string[]) {
  const uniqueIds = Array.from(new Set(ids.filter(isUuid)));
  if (uniqueIds.length === 0) return;
  const rows = await dbLike
    .select({ id: questionBankShared.id })
    .from(questionBankShared)
    .where(and(eq(questionBankShared.isDeleted, false), inArray(questionBankShared.id, uniqueIds)));
  const found = new Set(rows.map((row) => row.id));
  const missing = uniqueIds.filter((id) => !found.has(id));
  if (missing.length > 0) {
    throw new DomainValidationError("Mindestens eine referenzierte Frage existiert nicht oder ist geloescht.");
  }
}

async function ensurePhotoTagsActive(dbLike: DbLike, ids: string[]) {
  const uniqueIds = Array.from(new Set(ids.filter(isUuid)));
  if (uniqueIds.length === 0) return;
  const rows = await dbLike
    .select({ id: photoTags.id })
    .from(photoTags)
    .where(and(inArray(photoTags.id, uniqueIds), eq(photoTags.isDeleted, false)));
  const found = new Set(rows.map((row) => row.id));
  const missing = uniqueIds.filter((id) => !found.has(id));
  if (missing.length > 0) {
    throw new DomainValidationError("Mindestens ein Foto-Tag ist ungueltig oder geloescht.");
  }
}

function sanitizeQuestionConfig(config: Record<string, unknown>) {
  const next = { ...config };
  delete next.rows;
  delete next.columns;
  delete next.matrixSubtype;
  delete next.images;
  delete next.tagIds;
  return next;
}

function normalizeAndRemapRuleRef(
  ref: string | undefined,
  idMap: Map<string, string>,
  fieldName: "trigger" | "target",
): string {
  const raw = (ref ?? "").trim();
  if (raw.length === 0) return "";
  const remapped = idMap.get(raw) ?? raw;
  if (!isUuid(remapped)) {
    if (fieldName === "trigger") {
      throw new DomainValidationError("Regel-Trigger verweist auf eine unbekannte Frage-ID.");
    }
    throw new DomainValidationError("Regel-Ziel verweist auf eine unbekannte Frage-ID.");
  }
  return remapped;
}

function remapAndValidateQuestionRules(
  rules: UiQuestion["rules"],
  idMap: Map<string, string>,
  sourceQuestionId?: string,
  questionOrderById?: Map<string, number>,
): UiQuestion["rules"] {
  const remappedRules: UiQuestion["rules"] = [];
  const sourceOrderIndex = sourceQuestionId ? questionOrderById?.get(sourceQuestionId) : undefined;
  for (const rule of rules) {
    const triggerQuestionId = normalizeAndRemapRuleRef(rule.triggerQuestionId, idMap, "trigger");
    const targetQuestionIds = Array.from(
      new Set(
        rule.targetQuestionIds
          .map((targetQuestionId) => normalizeAndRemapRuleRef(targetQuestionId, idMap, "target"))
          .filter((targetQuestionId) => targetQuestionId.length > 0),
      ),
    );
    if (sourceQuestionId && targetQuestionIds.includes(sourceQuestionId)) {
      throw new DomainValidationError("Regeln duerfen nicht auf dieselbe Frage verweisen.");
    }
    if (sourceQuestionId && sourceOrderIndex != null) {
      for (const targetQuestionId of targetQuestionIds) {
        const targetOrderIndex = questionOrderById?.get(targetQuestionId);
        if (targetOrderIndex == null || targetOrderIndex <= sourceOrderIndex) {
          throw new DomainValidationError("Betroffene Fragen muessen nach der ausloesenden Frage im Modul kommen.");
        }
      }
    }
    remappedRules.push({
      ...rule,
      triggerQuestionId,
      targetQuestionIds,
    });
  }
  return remappedRules;
}

async function persistQuestionRulesTx(
  tx: DbTx,
  questionId: string,
  rules: UiQuestion["rules"],
  now: Date,
) {
  const previousRuleRows = await tx
    .select({ id: questionRules.id })
    .from(questionRules)
    .where(and(eq(questionRules.questionId, questionId), eq(questionRules.isDeleted, false)));
  const previousRuleIds = previousRuleRows.map((row) => row.id);
  if (previousRuleIds.length > 0) {
    await tx
      .update(questionRuleTargets)
      .set({
        isDeleted: true,
        deletedAt: now,
        updatedAt: now,
      })
      .where(and(inArray(questionRuleTargets.ruleId, previousRuleIds), eq(questionRuleTargets.isDeleted, false)));
  }
  await tx
    .update(questionRules)
    .set({
      isDeleted: true,
      deletedAt: now,
      updatedAt: now,
    })
    .where(and(eq(questionRules.questionId, questionId), eq(questionRules.isDeleted, false)));

  for (const [index, rule] of rules.entries()) {
    const [createdRule] = await tx
      .insert(questionRules)
      .values({
        questionId,
        triggerQuestionId: isUuid(rule.triggerQuestionId) ? rule.triggerQuestionId : null,
        operator: rule.operator,
        triggerValue: rule.triggerValue || null,
        triggerValueMax: rule.triggerValueMax || null,
        action: rule.action,
        orderIndex: index,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      })
      .returning();
    if (!createdRule) {
      throw new Error("Regel konnte nicht erstellt werden.");
    }

    if (rule.targetQuestionIds.length === 0) continue;
    for (const [targetIndex, targetQuestionId] of rule.targetQuestionIds.entries()) {
      const [restored] = await tx
        .update(questionRuleTargets)
        .set({
          orderIndex: targetIndex,
          isDeleted: false,
          deletedAt: null,
          updatedAt: now,
        })
        .where(
          and(
            eq(questionRuleTargets.ruleId, createdRule.id),
            eq(questionRuleTargets.targetQuestionId, targetQuestionId),
          ),
        )
        .returning({ id: questionRuleTargets.id });
      if (!restored) {
        await tx.insert(questionRuleTargets).values({
          ruleId: createdRule.id,
          targetQuestionId,
          orderIndex: targetIndex,
          isDeleted: false,
          deletedAt: null,
          createdAt: now,
          updatedAt: now,
        });
      }
    }
  }
}

function pickScopeConfig(scope: Scope) {
  if (scope === "main") {
    return {
      moduleTable: moduleMain,
      linkTable: moduleMainQuestion,
      fbTable: fragebogenMain,
      fbModuleLink: fragebogenMainModule,
    };
  }
  if (scope === "kuehler") {
    return {
      moduleTable: moduleKuehler,
      linkTable: moduleKuehlerQuestion,
      fbTable: fragebogenKuehler,
      fbModuleLink: fragebogenKuehlerModule,
    };
  }
  return {
    moduleTable: moduleMhd,
    linkTable: moduleMhdQuestion,
    fbTable: fragebogenMhd,
    fbModuleLink: fragebogenMhdModule,
  };
}

async function upsertModuleQuestionChainsTx(
  tx: DbTx,
  input: {
    scope: Scope;
    moduleId: string;
    questionChainInputs: Array<{ questionId: string; chains?: string[] }>;
  },
) {
  if (!(await ensureModuleQuestionChainsReady())) return;
  const now = new Date();
  await tx
    .update(moduleQuestionChains)
    .set({
      isDeleted: true,
      deletedAt: now,
      updatedAt: now,
    })
    .where(
      and(
        eq(moduleQuestionChains.scope, input.scope),
        eq(moduleQuestionChains.moduleId, input.moduleId),
        eq(moduleQuestionChains.isDeleted, false),
      ),
    );

  for (const chainInput of input.questionChainInputs) {
    const chains = normalizeChainDbNames(chainInput.chains);
    if (chains.length === 0) continue;
    for (const chainDbName of chains) {
      await tx
        .insert(moduleQuestionChains)
        .values({
          scope: input.scope,
          moduleId: input.moduleId,
          questionId: chainInput.questionId,
          chainDbName,
          isDeleted: false,
          deletedAt: null,
          createdAt: now,
          updatedAt: now,
        });
    }
  }
}

async function saveSpezialfragenTx(tx: DbTx, fragebogenId: string, spezialfragen: UiQuestion[]) {
  const now = new Date();
  await tx
    .update(fragebogenMainSpezialItems)
    .set({
      isDeleted: true,
      deletedAt: now,
      updatedAt: now,
    })
    .where(and(eq(fragebogenMainSpezialItems.fragebogenId, fragebogenId), eq(fragebogenMainSpezialItems.isDeleted, false)));
  if (spezialfragen.length === 0) return;

  const rows: Array<typeof fragebogenMainSpezialItems.$inferInsert> = [];
  const referencedSharedQuestionIds = new Set<string>();
  for (const [orderIndex, spezial] of spezialfragen.entries()) {
    const parsed = questionSchema.parse(spezial);
    validateQuestionDomain(parsed);
    if (parsed.id && !isUuid(parsed.id)) {
      throw new DomainValidationError("Spezialfrage-ID muss eine gueltige UUID sein.");
    }
    for (const rule of parsed.rules ?? []) {
      if (rule.triggerQuestionId && !isUuid(rule.triggerQuestionId)) {
        throw new DomainValidationError("Spezialfrage-Regeltrigger muss eine gueltige UUID sein.");
      }
      ensureAllUuids(rule.targetQuestionIds, "Spezialfrage-Regelziel");
      if (rule.triggerQuestionId) {
        referencedSharedQuestionIds.add(rule.triggerQuestionId);
      }
      for (const targetId of rule.targetQuestionIds) {
        referencedSharedQuestionIds.add(targetId);
      }
      if (parsed.id && isUuid(parsed.id)) {
        if (rule.triggerQuestionId === parsed.id) {
          throw new DomainValidationError("Spezialfrage-Regeln duerfen nicht auf sich selbst triggern.");
        }
        if (rule.targetQuestionIds.some((id) => id === parsed.id)) {
          throw new DomainValidationError("Spezialfrage-Regeln duerfen sich nicht selbst als Ziel haben.");
        }
      }
    }
    const tagIds = parseArrayField(parsed.config?.tagIds);
    if (tagIds.length > 0) {
      ensureAllUuids(tagIds, "Foto-Tag");
      await ensurePhotoTagsActive(tx, tagIds);
    }
    rows.push({
      fragebogenId,
      questionType: parsed.type,
      text: parsed.text,
      required: parsed.required,
      chains: parsed.chains && parsed.chains.length > 0 ? parsed.chains : null,
      config: parsed.config ?? {},
      rules: parsed.rules ?? [],
      scoring: parsed.scoring ?? {},
      orderIndex,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
  }
  await ensureQuestionRefsExist(tx, Array.from(referencedSharedQuestionIds));
  await tx.insert(fragebogenMainSpezialItems).values(rows);
}

async function loadSpezialfragenByFragebogenIds(dbLike: DbLike, ids: string[]): Promise<Map<string, UiQuestion[]>> {
  const validIds = Array.from(new Set(ids.filter(isUuid)));
  const spezialByFragebogen = new Map<string, UiQuestion[]>();
  if (validIds.length === 0) return spezialByFragebogen;

  const rows = await dbLike
    .select()
    .from(fragebogenMainSpezialItems)
    .where(
      and(
        inArray(fragebogenMainSpezialItems.fragebogenId, validIds),
        eq(fragebogenMainSpezialItems.isDeleted, false),
      ),
    )
    .orderBy(asc(fragebogenMainSpezialItems.fragebogenId), asc(fragebogenMainSpezialItems.orderIndex));

  for (const row of rows) {
    const current = spezialByFragebogen.get(row.fragebogenId) ?? [];
    current.push({
      id: row.id,
      type: row.questionType as UiQuestion["type"],
      text: row.text,
      required: row.required,
      config: (row.config ?? {}) as Record<string, unknown>,
      rules: (row.rules ?? []) as UiQuestion["rules"],
      scoring: (row.scoring ?? {}) as UiQuestion["scoring"],
      chains: row.chains ?? undefined,
    });
    spezialByFragebogen.set(row.fragebogenId, current);
  }
  return spezialByFragebogen;
}

async function fetchQuestionsByIds(dbLike: DbLike, ids: string[]): Promise<Map<string, UiQuestion>> {
  const uniqueIds = Array.from(new Set(ids.filter(isUuid)));
  if (uniqueIds.length === 0) return new Map();

  const baseRows = await dbLike
    .select()
    .from(questionBankShared)
    .where(and(eq(questionBankShared.isDeleted, false), inArray(questionBankShared.id, uniqueIds)));
  if (baseRows.length === 0) return new Map();

  const questionIds = baseRows.map((row) => row.id);
  const matrixRows = await dbLike
    .select()
    .from(questionMatrix)
    .where(and(inArray(questionMatrix.questionId, questionIds), eq(questionMatrix.isDeleted, false)));
  const scoringRows = await dbLike
    .select()
    .from(questionScoring)
    .where(and(inArray(questionScoring.questionId, questionIds), eq(questionScoring.isDeleted, false)));
  const ruleRows = await dbLike
    .select()
    .from(questionRules)
    .where(and(inArray(questionRules.questionId, questionIds), eq(questionRules.isDeleted, false)))
    .orderBy(asc(questionRules.questionId), asc(questionRules.orderIndex));
  const attachmentRows = await dbLike
    .select()
    .from(questionAttachments)
    .where(and(inArray(questionAttachments.questionId, questionIds), eq(questionAttachments.isDeleted, false)))
    .orderBy(asc(questionAttachments.questionId), asc(questionAttachments.orderIndex));
  const tagRows = await dbLike
    .select()
    .from(questionPhotoTags)
    .where(and(inArray(questionPhotoTags.questionId, questionIds), eq(questionPhotoTags.isDeleted, false)));

  const ruleIds = ruleRows.map((row) => row.id);
  const targetRows =
    ruleIds.length === 0
      ? []
      : await dbLike
          .select()
          .from(questionRuleTargets)
          .where(and(inArray(questionRuleTargets.ruleId, ruleIds), eq(questionRuleTargets.isDeleted, false)))
          .orderBy(asc(questionRuleTargets.ruleId), asc(questionRuleTargets.orderIndex));

  const matrixByQuestion = new Map(matrixRows.map((row) => [row.questionId, row]));
  const scoringByQuestion = new Map<string, Record<string, { ipp?: number; boni?: number }>>();
  for (const row of scoringRows) {
    const current = scoringByQuestion.get(row.questionId) ?? {};
    const value: { ipp?: number; boni?: number } = {};
    if (row.ipp != null) value.ipp = Number(row.ipp);
    if (row.boni != null) value.boni = Number(row.boni);
    current[row.scoreKey] = value;
    scoringByQuestion.set(row.questionId, current);
  }

  const targetsByRule = new Map<string, string[]>();
  for (const target of targetRows) {
    const current = targetsByRule.get(target.ruleId) ?? [];
    current.push(target.targetQuestionId);
    targetsByRule.set(target.ruleId, current);
  }

  const rulesByQuestion = new Map<string, UiQuestion["rules"]>();
  for (const rule of ruleRows) {
    const current = rulesByQuestion.get(rule.questionId) ?? [];
    current.push({
      id: rule.id,
      triggerQuestionId: rule.triggerQuestionId ?? "",
      operator: rule.operator,
      triggerValue: rule.triggerValue ?? "",
      triggerValueMax: rule.triggerValueMax ?? "",
      action: rule.action,
      targetQuestionIds: targetsByRule.get(rule.id) ?? [],
    });
    rulesByQuestion.set(rule.questionId, current);
  }

  const attachmentsByQuestion = new Map<string, string[]>();
  for (const attachment of attachmentRows) {
    const current = attachmentsByQuestion.get(attachment.questionId) ?? [];
    current.push(attachment.payload);
    attachmentsByQuestion.set(attachment.questionId, current);
  }

  const tagsByQuestion = new Map<string, string[]>();
  for (const tag of tagRows) {
    const current = tagsByQuestion.get(tag.questionId) ?? [];
    current.push(tag.photoTagId);
    tagsByQuestion.set(tag.questionId, current);
  }

  const output = new Map<string, UiQuestion>();
  for (const base of baseRows) {
    const config = { ...(base.config ?? {}) } as Record<string, unknown>;
    const matrix = matrixByQuestion.get(base.id);
    if (matrix) {
      config.matrixSubtype = matrix.matrixSubtype;
      config.rows = matrix.rows;
      config.columns = matrix.columns;
    }
    const images = attachmentsByQuestion.get(base.id);
    if (images && images.length > 0) {
      config.images = images;
    }
    const tagIds = tagsByQuestion.get(base.id);
    if (String(base.questionType) === "photo") {
      config.tagsEnabled = (tagIds?.length ?? 0) > 0 || Boolean(config.tagsEnabled);
      config.tagIds = tagIds ?? [];
    }

    output.set(base.id, {
      id: base.id,
      type: base.questionType as UiQuestion["type"],
      text: base.text,
      required: base.required,
      config,
      rules: rulesByQuestion.get(base.id) ?? [],
      scoring: scoringByQuestion.get(base.id) ?? {},
      chains: undefined,
    });
  }
  return output;
}

async function upsertQuestionGraphTx(tx: DbTx, input: UiQuestion): Promise<UiQuestion> {
  const parsed = questionSchema.parse(input);
  validateQuestionDomain(parsed);

  let questionId = parsed.id && isUuid(parsed.id) ? parsed.id : null;
  if (questionId) {
    const [existing] = await tx.select({ id: questionBankShared.id }).from(questionBankShared).where(eq(questionBankShared.id, questionId)).limit(1);
    if (!existing) questionId = null;
  }

  const now = new Date();
  const config = { ...(parsed.config ?? {}) } as Record<string, unknown>;
  const images = parseArrayField(config.images);
  const matrixRows = parseArrayField(config.rows);
  const matrixColumns = parseArrayField(config.columns);
  const matrixSubtype = typeof config.matrixSubtype === "string" ? config.matrixSubtype : "toggle";
  const rawTagIds = parseArrayField(config.tagIds);
  ensureAllUuids(rawTagIds, "Foto-Tag");
  const tagIds = Array.from(new Set(rawTagIds));

  const rules = parsed.rules ?? [];
  for (const rule of rules) {
    if (rule.triggerQuestionId && !isUuid(rule.triggerQuestionId)) {
      throw new DomainValidationError("Regel-Trigger muss eine gueltige Frage-ID sein.");
    }
    ensureAllUuids(rule.targetQuestionIds, "Regel-Ziel");
  }
  const referencedIds = rules.flatMap((rule) => [
    ...(rule.triggerQuestionId ? [rule.triggerQuestionId] : []),
    ...rule.targetQuestionIds,
  ]);
  if (questionId && rules.some((rule) => rule.targetQuestionIds.includes(questionId!))) {
    throw new DomainValidationError("Regeln duerfen nicht auf dieselbe Frage verweisen.");
  }
  await ensureQuestionRefsExist(tx, referencedIds);
  await ensurePhotoTagsActive(tx, tagIds);

  const cleanConfig = sanitizeQuestionConfig(config);
  if (questionId) {
    await tx
      .update(questionBankShared)
      .set({
        questionType: parsed.type,
        text: parsed.text,
        required: parsed.required,
        config: cleanConfig,
        rules: [],
        scoring: {},
        updatedAt: now,
      })
      .where(eq(questionBankShared.id, questionId));
  } else {
    const [created] = await tx
      .insert(questionBankShared)
      .values({
        questionType: parsed.type,
        text: parsed.text,
        required: parsed.required,
        config: cleanConfig,
        rules: [],
        scoring: {},
        createdAt: now,
        updatedAt: now,
      })
      .returning();
    if (!created) {
      throw new Error("Frage konnte nicht erstellt werden.");
    }
    questionId = created.id;
  }

  await tx
    .update(questionScoring)
    .set({
      isDeleted: true,
      deletedAt: now,
      updatedAt: now,
    })
    .where(and(eq(questionScoring.questionId, questionId), eq(questionScoring.isDeleted, false)));
  const scoringEntries = Object.entries(parsed.scoring ?? {});
  for (const [scoreKey, value] of scoringEntries) {
    const [restored] = await tx
      .update(questionScoring)
      .set({
        ipp: value.ipp != null ? String(value.ipp) : null,
        boni: value.boni != null ? String(value.boni) : null,
        isDeleted: false,
        deletedAt: null,
        updatedAt: now,
      })
      .where(and(eq(questionScoring.questionId, questionId), eq(questionScoring.scoreKey, scoreKey)))
      .returning({ id: questionScoring.id });
    if (!restored) {
      await tx.insert(questionScoring).values({
        questionId,
        scoreKey,
        ipp: value.ipp != null ? String(value.ipp) : null,
        boni: value.boni != null ? String(value.boni) : null,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      });
    }
  }

  const dedupedRules = rules.map((rule) => ({
    ...rule,
    targetQuestionIds: Array.from(new Set(rule.targetQuestionIds)),
  }));
  await persistQuestionRulesTx(tx, questionId, dedupedRules, now);

  if (parsed.type === "matrix") {
    await tx
      .insert(questionMatrix)
      .values({
        questionId,
        matrixSubtype,
        rows: matrixRows,
        columns: matrixColumns,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      })
      .onConflictDoUpdate({
        target: questionMatrix.questionId,
        set: {
          matrixSubtype,
          rows: matrixRows,
          columns: matrixColumns,
          isDeleted: false,
          deletedAt: null,
          updatedAt: now,
        },
      });
  } else {
    await tx
      .update(questionMatrix)
      .set({
        isDeleted: true,
        deletedAt: now,
        updatedAt: now,
      })
      .where(and(eq(questionMatrix.questionId, questionId), eq(questionMatrix.isDeleted, false)));
  }

  await tx
    .update(questionAttachments)
    .set({
      isDeleted: true,
      deletedAt: now,
      updatedAt: now,
    })
    .where(and(eq(questionAttachments.questionId, questionId), eq(questionAttachments.isDeleted, false)));
  if (images.length > 0) {
    await tx.insert(questionAttachments).values(
      images.map((payload, orderIndex) => ({
        questionId,
        payload,
        orderIndex,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      })),
    );
  }

  await tx
    .update(questionPhotoTags)
    .set({
      isDeleted: true,
      deletedAt: now,
      updatedAt: now,
    })
    .where(and(eq(questionPhotoTags.questionId, questionId), eq(questionPhotoTags.isDeleted, false)));
  if (tagIds.length > 0) {
    for (const photoTagId of tagIds) {
      const [restored] = await tx
        .update(questionPhotoTags)
        .set({
          isDeleted: false,
          deletedAt: null,
          updatedAt: now,
        })
        .where(and(eq(questionPhotoTags.questionId, questionId), eq(questionPhotoTags.photoTagId, photoTagId)))
        .returning({ questionId: questionPhotoTags.questionId });
      if (!restored) {
        await tx.insert(questionPhotoTags).values({
          questionId,
          photoTagId,
          isDeleted: false,
          deletedAt: null,
          createdAt: now,
          updatedAt: now,
        });
      }
    }
  }

  const hydrated = await fetchQuestionsByIds(tx, [questionId]);
  const output = hydrated.get(questionId);
  if (!output) throw new Error("Frage konnte nach dem Speichern nicht geladen werden.");
  return output;
}

async function validateModuleIdsExist(tx: DbTx, scope: Scope, moduleIds: string[]) {
  ensureAllUuids(moduleIds, "Modul");
  const uniqueIds = Array.from(new Set(moduleIds));
  if (uniqueIds.length === 0) return;
  const cfg = pickScopeConfig(scope);
  const rows = await tx
    .select({ id: (cfg.moduleTable as AnyTable).id })
    .from(cfg.moduleTable as AnyTable)
    .where(and(inArray((cfg.moduleTable as AnyTable).id, uniqueIds), eq((cfg.moduleTable as AnyTable).isDeleted, false)));
  const found = new Set(rows.map((row) => row.id));
  const missing = uniqueIds.filter((id) => !found.has(id));
  if (missing.length > 0) {
    throw new DomainValidationError("Mindestens ein Modul existiert im Zielbereich nicht.");
  }
}

function normalizeSchedule(input: Pick<UiFragebogen, "scheduleType" | "startDate" | "endDate">) {
  if (input.scheduleType === "always") {
    return { scheduleType: "always" as const, startDate: null, endDate: null };
  }
  if (!input.startDate || !input.endDate) {
    throw new DomainValidationError("Geplante Frageboegen benoetigen Start- und Enddatum.");
  }
  const startDate = parseIsoDateOrThrow(input.startDate, "startDate");
  const endDate = parseIsoDateOrThrow(input.endDate, "endDate");
  if (startDate > endDate) {
    throw new DomainValidationError("Das Startdatum darf nicht nach dem Enddatum liegen.");
  }
  return { scheduleType: "scheduled" as const, startDate, endDate };
}

export async function fetchModulesUi(scope: Scope, ids?: string[]): Promise<UiModule[]> {
  const cfg = pickScopeConfig(scope);
  const validIds = ids ? Array.from(new Set(ids.filter(isUuid))) : [];
  const moduleRows = await db
    .select()
    .from(cfg.moduleTable as AnyTable)
    .where(
      ids
        ? and(eq((cfg.moduleTable as AnyTable).isDeleted, false), inArray((cfg.moduleTable as AnyTable).id, validIds))
        : eq((cfg.moduleTable as AnyTable).isDeleted, false),
    )
    .orderBy(desc((cfg.moduleTable as AnyTable).createdAt));
  if (moduleRows.length === 0) return [];

  const moduleIds = moduleRows.map((row) => row.id);
  const links = await db
    .select()
    .from(cfg.linkTable as AnyTable)
    .where(
      and(
        inArray((cfg.linkTable as AnyTable).moduleId, moduleIds),
        eq((cfg.linkTable as AnyTable).isDeleted, false),
      ),
    )
    .orderBy(asc((cfg.linkTable as AnyTable).moduleId), asc((cfg.linkTable as AnyTable).orderIndex));

  const questionIds = Array.from(new Set(links.map((link) => link.questionId)));
  const questionMap = await fetchQuestionsByIds(db, questionIds);
  const chainsReady = await ensureModuleQuestionChainsReady();
  const chainRows =
    moduleIds.length === 0 || !chainsReady
      ? []
      : await db
          .select({
            moduleId: moduleQuestionChains.moduleId,
            questionId: moduleQuestionChains.questionId,
            chainDbName: moduleQuestionChains.chainDbName,
          })
          .from(moduleQuestionChains)
          .where(
            and(
              eq(moduleQuestionChains.scope, scope),
              inArray(moduleQuestionChains.moduleId, moduleIds),
              eq(moduleQuestionChains.isDeleted, false),
            ),
          )
          .orderBy(
            asc(moduleQuestionChains.moduleId),
            asc(moduleQuestionChains.questionId),
            asc(moduleQuestionChains.chainDbName),
          );
  const chainsByModuleQuestion = new Map<string, string[]>();
  for (const row of chainRows) {
    const key = `${row.moduleId}:${row.questionId}`;
    const current = chainsByModuleQuestion.get(key) ?? [];
    current.push(row.chainDbName);
    chainsByModuleQuestion.set(key, current);
  }

  const usageTable: AnyTable =
    scope === "main" ? fragebogenMainModule : scope === "kuehler" ? fragebogenKuehlerModule : fragebogenMhdModule;

  const usageRows = await db
    .select({
      moduleId: usageTable.moduleId,
      count: sql<number>`count(*)::int`,
    })
    .from(usageTable)
    .where(and(inArray(usageTable.moduleId, moduleIds), eq(usageTable.isDeleted, false)))
    .groupBy(usageTable.moduleId);
  const usageMap = new Map(usageRows.map((row) => [row.moduleId, Number(row.count)]));

  const linksByModule = new Map<string, string[]>();
  for (const link of links) {
    const current = linksByModule.get(link.moduleId) ?? [];
    current.push(link.questionId);
    linksByModule.set(link.moduleId, current);
  }

  return moduleRows.map((row) => ({
    id: row.id,
    name: row.name,
    description: row.description ?? "",
    createdAt: row.createdAt.toISOString(),
    usedInCount: usageMap.get(row.id) ?? 0,
    sectionKeywords: scope === "main" ? ((row as typeof moduleMain.$inferSelect).sectionKeywords ?? ["standard"]) : undefined,
    questions: (linksByModule.get(row.id) ?? [])
      .map((questionId) => {
        const question = questionMap.get(questionId);
        if (!question) return null;
        const chains = chainsByModuleQuestion.get(`${row.id}:${questionId}`);
        return {
          ...question,
          chains: chains && chains.length > 0 ? chains : undefined,
        };
      })
      .filter(Boolean) as UiQuestion[],
  }));
}

export async function fetchFragebogenUi(scope: Scope, ids?: string[]): Promise<UiFragebogen[]> {
  const cfg = pickScopeConfig(scope);
  const validIds = ids ? Array.from(new Set(ids.filter(isUuid))) : [];
  const fbRows = await db
    .select()
    .from(cfg.fbTable as AnyTable)
    .where(
      ids
        ? and(eq((cfg.fbTable as AnyTable).isDeleted, false), inArray((cfg.fbTable as AnyTable).id, validIds))
        : eq((cfg.fbTable as AnyTable).isDeleted, false),
    )
    .orderBy(desc((cfg.fbTable as AnyTable).createdAt));
  if (fbRows.length === 0) return [];

  const fragebogenIds = fbRows.map((row) => row.id);
  const moduleLinks = await db
    .select()
    .from(cfg.fbModuleLink as AnyTable)
    .where(
      and(
        inArray((cfg.fbModuleLink as AnyTable).fragebogenId, fragebogenIds),
        eq((cfg.fbModuleLink as AnyTable).isDeleted, false),
      ),
    )
    .orderBy(
      asc((cfg.fbModuleLink as AnyTable).fragebogenId),
      asc((cfg.fbModuleLink as AnyTable).orderIndex),
    );
  const moduleIdsByFragebogen = new Map<string, string[]>();
  for (const link of moduleLinks) {
    const current = moduleIdsByFragebogen.get(link.fragebogenId) ?? [];
    current.push(link.moduleId);
    moduleIdsByFragebogen.set(link.fragebogenId, current);
  }

  const spezialByFragebogen = scope === "main" ? await loadSpezialfragenByFragebogenIds(db, fragebogenIds) : new Map<string, UiQuestion[]>();

  return fbRows.map((row) => ({
    id: row.id,
    name: row.name,
    description: row.description ?? "",
    moduleIds: moduleIdsByFragebogen.get(row.id) ?? [],
    markets: [],
    scheduleType: row.scheduleType,
    startDate: row.startDate ? String(row.startDate) : undefined,
    endDate: row.endDate ? String(row.endDate) : undefined,
    createdAt: row.createdAt.toISOString(),
    status: row.status,
    sectionKeywords: scope === "main" ? ((row as typeof fragebogenMain.$inferSelect).sectionKeywords ?? ["standard"]) : undefined,
    nurEinmalAusfuellbar: Boolean(row.nurEinmalAusfuellbar),
    spezialfragen: spezialByFragebogen.get(row.id) ?? [],
  }));
}

adminFragebogenRouter.get("/photo-tags", async (_req, res, next) => {
  try {
    const rows = await db
      .select()
      .from(photoTags)
      .where(eq(photoTags.isDeleted, false))
      .orderBy(asc(photoTags.label));
    res.status(200).json({
      tags: rows.map((row) => ({
        id: row.id,
        label: row.label,
        deletedAt: row.deletedAt?.toISOString() ?? null,
        createdAt: row.createdAt.toISOString(),
      })),
    });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.get("/markets/chains", async (_req, res, next) => {
  try {
    const rows = await db
      .select({ dbName: markets.dbName })
      .from(markets)
      .where(and(eq(markets.isDeleted, false), sql`nullif(trim(${markets.dbName}), '') is not null`))
      .orderBy(asc(markets.dbName));
    const chains = Array.from(
      new Set(
        rows
          .map((row) => row.dbName.trim())
          .filter((value) => value.length > 0),
      ),
    );
    res.status(200).json({ chains });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.post("/photo-tags", async (req, res, next) => {
  try {
    const parsed = z.object({ label: z.string().trim().min(1) }).safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltiger Tag." });
      return;
    }
    const now = new Date();
    const [created] = await db
      .insert(photoTags)
      .values({ label: parsed.data.label, isDeleted: false, deletedAt: null, createdAt: now, updatedAt: now })
      .returning();
    if (!created) {
      throw new Error("Tag konnte nicht erstellt werden.");
    }
    res.status(201).json({
      tag: {
        id: created.id,
        label: created.label,
        deletedAt: created.deletedAt?.toISOString() ?? null,
        createdAt: created.createdAt.toISOString(),
      },
    });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.patch("/photo-tags/:id", async (req, res, next) => {
  try {
    const parsed = z
      .object({
        label: z.string().trim().min(1).optional(),
        deleted: z.boolean().optional(),
      })
      .strict()
      .safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltiger Tag-Payload." });
      return;
    }
    const now = new Date();
    await db
      .update(photoTags)
      .set({
        label: parsed.data.label,
        isDeleted: parsed.data.deleted === undefined ? undefined : parsed.data.deleted,
        deletedAt: parsed.data.deleted === undefined ? undefined : parsed.data.deleted ? now : null,
        updatedAt: now,
      })
      .where(eq(photoTags.id, req.params.id));
    const [updated] = await db.select().from(photoTags).where(eq(photoTags.id, req.params.id)).limit(1);
    if (!updated) {
      res.status(404).json({ error: "Tag nicht gefunden." });
      return;
    }
    res.status(200).json({
      tag: {
        id: updated.id,
        label: updated.label,
        deletedAt: updated.deletedAt?.toISOString() ?? null,
        createdAt: updated.createdAt.toISOString(),
      },
    });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.get("/questions", async (_req, res, next) => {
  try {
    const rows = await db
      .select({ id: questionBankShared.id })
      .from(questionBankShared)
      .where(eq(questionBankShared.isDeleted, false))
      .orderBy(desc(questionBankShared.updatedAt));
    const questionsMap = await fetchQuestionsByIds(db, rows.map((row) => row.id));
    res.status(200).json({ questions: rows.map((row) => questionsMap.get(row.id)).filter(Boolean) });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.post("/questions", async (req, res, next) => {
  try {
    const parsed = questionSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Frage." });
      return;
    }
    const question = await db.transaction((tx) => upsertQuestionGraphTx(tx, parsed.data));
    if (question.id) {
      await enqueueIppRecalcForQuestionScoringChanges([question.id], "question_scoring_changed");
    }
    res.status(201).json({ question });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.patch("/questions/:id", async (req, res, next) => {
  try {
    if (!isUuid(req.params.id)) {
      res.status(400).json({ error: "Ungueltige Frage-ID." });
      return;
    }
    const partial = questionSchema.partial().safeParse(req.body);
    if (!partial.success) {
      res.status(400).json({ error: "Ungueltige Frage." });
      return;
    }
    const currentMap = await fetchQuestionsByIds(db, [req.params.id]);
    const existing = currentMap.get(req.params.id);
    if (!existing) {
      res.status(404).json({ error: "Frage nicht gefunden." });
      return;
    }
    const merged: UiQuestion = {
      id: req.params.id,
      type: partial.data.type ?? existing.type,
      text: partial.data.text ?? existing.text,
      required: partial.data.required ?? existing.required,
      config: partial.data.config ?? existing.config,
      rules: partial.data.rules ?? existing.rules,
      scoring: partial.data.scoring ?? existing.scoring,
      chains: partial.data.chains ?? existing.chains,
    };
    const question = await db.transaction((tx) => upsertQuestionGraphTx(tx, merged));
    if (question.id) {
      await enqueueIppRecalcForQuestionScoringChanges([question.id], "question_scoring_changed");
    }
    res.status(200).json({ question });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.get("/modules/:scope", async (req, res, next) => {
  try {
    const scope = getScopeParam(req.params as Record<string, string | undefined>);
    const modules = await fetchModulesUi(scope);
    res.status(200).json({ modules });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.post("/modules/:scope", async (req, res, next) => {
  try {
    const scope = getScopeParam(req.params as Record<string, string | undefined>);
    const parsed = moduleSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltiges Modul." });
      return;
    }
    const payload = parsed.data;
    const now = new Date();

    const affectedQuestionIds: string[] = [];
    const moduleId = await db.transaction(async (tx) => {
      const cfg = pickScopeConfig(scope);
      const [created] = await tx
        .insert(cfg.moduleTable as typeof moduleMain)
        .values({
          name: payload.name,
          description: payload.description ?? "",
          ...(scope === "main"
            ? {
                sectionKeywords:
                  payload.sectionKeywords && payload.sectionKeywords.length > 0
                    ? payload.sectionKeywords
                    : (["standard"] as MainSection[]),
              }
            : {}),
          createdAt: now,
          updatedAt: now,
        } as typeof moduleMain.$inferInsert)
        .returning();
      if (!created) {
        throw new Error("Modul konnte nicht erstellt werden.");
      }

      const questionIds: string[] = [];
      const questionIdMap = new Map<string, string>();
      const savedQuestionRows: Array<{ sourceQuestion: UiQuestion; persistedQuestionId: string }> = [];
      const questionChainInputs: Array<{ questionId: string; chains?: string[] }> = [];
      for (const question of payload.questions) {
        const saved = await upsertQuestionGraphTx(tx, { ...question, rules: [] });
        if (saved.id) {
          questionIds.push(saved.id);
          affectedQuestionIds.push(saved.id);
          if (question.id) {
            questionIdMap.set(question.id, saved.id);
          }
          questionIdMap.set(saved.id, saved.id);
          savedQuestionRows.push({ sourceQuestion: question, persistedQuestionId: saved.id });
          questionChainInputs.push(
            question.chains ? { questionId: saved.id, chains: question.chains } : { questionId: saved.id },
          );
        }
      }
      const questionOrderById = new Map<string, number>(questionIds.map((questionId, orderIndex) => [questionId, orderIndex]));
      for (const savedQuestionRow of savedQuestionRows) {
        const remappedRules = remapAndValidateQuestionRules(
          savedQuestionRow.sourceQuestion.rules ?? [],
          questionIdMap,
          savedQuestionRow.persistedQuestionId,
          questionOrderById,
        );
        const referencedIds = remappedRules.flatMap((rule) => [
          ...(rule.triggerQuestionId ? [rule.triggerQuestionId] : []),
          ...rule.targetQuestionIds,
        ]);
        await ensureQuestionRefsExist(tx, referencedIds);
        await persistQuestionRulesTx(tx, savedQuestionRow.persistedQuestionId, remappedRules, now);
      }
      if (questionIds.length > 0) {
        await tx.insert(cfg.linkTable as typeof moduleMainQuestion).values(
          questionIds.map((questionId, orderIndex) => ({
            moduleId: created.id,
            questionId,
            orderIndex,
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          })),
        );
      }
      await upsertModuleQuestionChainsTx(tx, {
        scope,
        moduleId: created.id,
        questionChainInputs,
      });
      return created.id;
    });

    if (affectedQuestionIds.length > 0) {
      await enqueueIppRecalcForQuestionScoringChanges(affectedQuestionIds, "module_scoring_changed");
    }

    const [module] = await fetchModulesUi(scope, [moduleId]);
    res.status(201).json({ module });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.patch("/modules/:scope/:id", async (req, res, next) => {
  try {
    const scope = getScopeParam(req.params as Record<string, string | undefined>);
    if (!isUuid(req.params.id)) {
      res.status(400).json({ error: "Ungueltige Modul-ID." });
      return;
    }
    const parsed = moduleSchema.safeParse({ ...req.body, id: req.params.id });
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltiges Modul." });
      return;
    }
    const now = new Date();

    const affectedQuestionIds: string[] = [];
    await db.transaction(async (tx) => {
      const cfg = pickScopeConfig(scope);
      const [existing] = await tx
        .select({ id: (cfg.moduleTable as typeof moduleMain).id, isDeleted: (cfg.moduleTable as typeof moduleMain).isDeleted })
        .from(cfg.moduleTable as typeof moduleMain)
        .where(eq((cfg.moduleTable as typeof moduleMain).id, req.params.id))
        .limit(1);
      if (!existing || existing.isDeleted) {
        throw new Error("Modul nicht gefunden.");
      }

      await tx
        .update(cfg.moduleTable as typeof moduleMain)
        .set({
          name: parsed.data.name,
          description: parsed.data.description ?? "",
          ...(scope === "main"
            ? {
                sectionKeywords:
                  parsed.data.sectionKeywords && parsed.data.sectionKeywords.length > 0
                    ? parsed.data.sectionKeywords
                    : (["standard"] as MainSection[]),
              }
            : {}),
          updatedAt: now,
        } as Partial<typeof moduleMain.$inferInsert>)
        .where(eq((cfg.moduleTable as typeof moduleMain).id, req.params.id));

      await tx
        .update(cfg.linkTable as typeof moduleMainQuestion)
        .set({
          isDeleted: true,
          deletedAt: now,
          updatedAt: now,
        })
        .where(
          and(
            eq((cfg.linkTable as typeof moduleMainQuestion).moduleId, req.params.id),
            eq((cfg.linkTable as typeof moduleMainQuestion).isDeleted, false),
          ),
        );
      const questionIds: string[] = [];
      const questionIdMap = new Map<string, string>();
      const savedQuestionRows: Array<{ sourceQuestion: UiQuestion; persistedQuestionId: string }> = [];
      const questionChainInputs: Array<{ questionId: string; chains?: string[] }> = [];
      for (const question of parsed.data.questions) {
        const saved = await upsertQuestionGraphTx(tx, { ...question, rules: [] });
        if (saved.id) {
          questionIds.push(saved.id);
          affectedQuestionIds.push(saved.id);
          if (question.id) {
            questionIdMap.set(question.id, saved.id);
          }
          questionIdMap.set(saved.id, saved.id);
          savedQuestionRows.push({ sourceQuestion: question, persistedQuestionId: saved.id });
          questionChainInputs.push(
            question.chains ? { questionId: saved.id, chains: question.chains } : { questionId: saved.id },
          );
        }
      }
      const questionOrderById = new Map<string, number>(questionIds.map((questionId, orderIndex) => [questionId, orderIndex]));
      for (const savedQuestionRow of savedQuestionRows) {
        const remappedRules = remapAndValidateQuestionRules(
          savedQuestionRow.sourceQuestion.rules ?? [],
          questionIdMap,
          savedQuestionRow.persistedQuestionId,
          questionOrderById,
        );
        const referencedIds = remappedRules.flatMap((rule) => [
          ...(rule.triggerQuestionId ? [rule.triggerQuestionId] : []),
          ...rule.targetQuestionIds,
        ]);
        await ensureQuestionRefsExist(tx, referencedIds);
        await persistQuestionRulesTx(tx, savedQuestionRow.persistedQuestionId, remappedRules, now);
      }
      if (questionIds.length > 0) {
        for (const [orderIndex, questionId] of questionIds.entries()) {
          await tx
            .insert(cfg.linkTable as typeof moduleMainQuestion)
            .values({
              moduleId: req.params.id,
              questionId,
              orderIndex,
              isDeleted: false,
              deletedAt: null,
              createdAt: now,
              updatedAt: now,
            })
            .onConflictDoUpdate({
              target: [
                (cfg.linkTable as typeof moduleMainQuestion).moduleId,
                (cfg.linkTable as typeof moduleMainQuestion).questionId,
              ],
              set: {
                orderIndex,
                isDeleted: false,
                deletedAt: null,
                updatedAt: now,
              },
            });
        }
      }
      await upsertModuleQuestionChainsTx(tx, {
        scope,
        moduleId: req.params.id,
        questionChainInputs,
      });
    });

    if (affectedQuestionIds.length > 0) {
      await enqueueIppRecalcForQuestionScoringChanges(affectedQuestionIds, "module_scoring_changed");
    }

    const [module] = await fetchModulesUi(scope, [req.params.id]);
    if (!module) {
      res.status(404).json({ error: "Modul nicht gefunden." });
      return;
    }
    res.status(200).json({ module });
  } catch (error) {
    if (error instanceof Error && error.message === "Modul nicht gefunden.") {
      res.status(404).json({ error: error.message });
      return;
    }
    next(error);
  }
});

adminFragebogenRouter.patch("/modules/:scope/:id/delete", async (req, res, next) => {
  try {
    const scope = getScopeParam(req.params as Record<string, string | undefined>);
    if (!isUuid(req.params.id)) {
      res.status(400).json({ error: "Ungueltige Modul-ID." });
      return;
    }
    const cfg = pickScopeConfig(scope);
    await db
      .update(cfg.moduleTable as typeof moduleMain)
      .set({ isDeleted: true, updatedAt: new Date() })
      .where(eq((cfg.moduleTable as typeof moduleMain).id, req.params.id));
    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.get("/fragebogen/:scope", async (req, res, next) => {
  try {
    const scope = getScopeParam(req.params as Record<string, string | undefined>);
    const fragebogen = await fetchFragebogenUi(scope);
    res.status(200).json({ fragebogen });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.post("/fragebogen/:scope", async (req, res, next) => {
  try {
    const scope = getScopeParam(req.params as Record<string, string | undefined>);
    const parsed = fragebogenSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltiger Fragebogen." });
      return;
    }
    if (scope !== "main" && parsed.data.spezialfragen.length > 0) {
      res.status(400).json({ error: "Spezialfragen sind nur fuer den Main-Scope erlaubt." });
      return;
    }

    const now = new Date();
    const schedule = normalizeSchedule(parsed.data);
    const fragebogenId = await db.transaction(async (tx) => {
      await validateModuleIdsExist(tx, scope, parsed.data.moduleIds);
      const cfg = pickScopeConfig(scope);

      const [created] = await tx
        .insert(cfg.fbTable as typeof fragebogenMain)
        .values({
          name: parsed.data.name,
          description: parsed.data.description ?? "",
          ...(scope === "main"
            ? {
                sectionKeywords:
                  parsed.data.sectionKeywords && parsed.data.sectionKeywords.length > 0
                    ? parsed.data.sectionKeywords
                    : (["standard"] as MainSection[]),
              }
            : {}),
          nurEinmalAusfuellbar: parsed.data.nurEinmalAusfuellbar ?? false,
          status: parsed.data.status,
          scheduleType: schedule.scheduleType,
          startDate: schedule.startDate,
          endDate: schedule.endDate,
          createdAt: now,
          updatedAt: now,
        } as typeof fragebogenMain.$inferInsert)
        .returning();
      if (!created) {
        throw new Error("Fragebogen konnte nicht erstellt werden.");
      }

      const moduleIds = Array.from(new Set(parsed.data.moduleIds));
      if (moduleIds.length > 0) {
        await tx.insert(cfg.fbModuleLink as typeof fragebogenMainModule).values(
          moduleIds.map((moduleId, orderIndex) => ({
            fragebogenId: created.id,
            moduleId,
            orderIndex,
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          })),
        );
      }

      if (scope === "main") {
        await saveSpezialfragenTx(tx, created.id, parsed.data.spezialfragen);
      }

      return created.id;
    });

    const [fragebogen] = await fetchFragebogenUi(scope, [fragebogenId]);
    res.status(201).json({ fragebogen });
  } catch (error) {
    if (error instanceof Error && error.message.includes("Modul")) {
      res.status(400).json({ error: error.message });
      return;
    }
    next(error);
  }
});

adminFragebogenRouter.patch("/fragebogen/:scope/:id", async (req, res, next) => {
  try {
    const scope = getScopeParam(req.params as Record<string, string | undefined>);
    if (!isUuid(req.params.id)) {
      res.status(400).json({ error: "Ungueltige Fragebogen-ID." });
      return;
    }
    const parsed = fragebogenSchema.safeParse({ ...req.body, id: req.params.id });
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltiger Fragebogen." });
      return;
    }
    if (scope !== "main" && parsed.data.spezialfragen.length > 0) {
      res.status(400).json({ error: "Spezialfragen sind nur fuer den Main-Scope erlaubt." });
      return;
    }

    const now = new Date();
    const schedule = normalizeSchedule(parsed.data);
    await db.transaction(async (tx) => {
      await validateModuleIdsExist(tx, scope, parsed.data.moduleIds);
      const cfg = pickScopeConfig(scope);

      const [existing] = await tx
        .select({ id: (cfg.fbTable as typeof fragebogenMain).id, isDeleted: (cfg.fbTable as typeof fragebogenMain).isDeleted })
        .from(cfg.fbTable as typeof fragebogenMain)
        .where(eq((cfg.fbTable as typeof fragebogenMain).id, req.params.id))
        .limit(1);
      if (!existing || existing.isDeleted) {
        throw new Error("Fragebogen nicht gefunden.");
      }

      await tx
        .update(cfg.fbTable as typeof fragebogenMain)
        .set({
          name: parsed.data.name,
          description: parsed.data.description ?? "",
          ...(scope === "main"
            ? {
                sectionKeywords:
                  parsed.data.sectionKeywords && parsed.data.sectionKeywords.length > 0
                    ? parsed.data.sectionKeywords
                    : (["standard"] as MainSection[]),
              }
            : {}),
          nurEinmalAusfuellbar: parsed.data.nurEinmalAusfuellbar ?? false,
          status: parsed.data.status,
          scheduleType: schedule.scheduleType,
          startDate: schedule.startDate,
          endDate: schedule.endDate,
          updatedAt: now,
        } as Partial<typeof fragebogenMain.$inferInsert>)
        .where(eq((cfg.fbTable as typeof fragebogenMain).id, req.params.id));

      await tx
        .update(cfg.fbModuleLink as typeof fragebogenMainModule)
        .set({
          isDeleted: true,
          deletedAt: now,
          updatedAt: now,
        })
        .where(
          and(
            eq((cfg.fbModuleLink as typeof fragebogenMainModule).fragebogenId, req.params.id),
            eq((cfg.fbModuleLink as typeof fragebogenMainModule).isDeleted, false),
          ),
        );
      const moduleIds = Array.from(new Set(parsed.data.moduleIds));
      if (moduleIds.length > 0) {
        for (const [orderIndex, moduleId] of moduleIds.entries()) {
          await tx
            .insert(cfg.fbModuleLink as typeof fragebogenMainModule)
            .values({
              fragebogenId: req.params.id,
              moduleId,
              orderIndex,
              isDeleted: false,
              deletedAt: null,
              createdAt: now,
              updatedAt: now,
            })
            .onConflictDoUpdate({
              target: [
                (cfg.fbModuleLink as typeof fragebogenMainModule).fragebogenId,
                (cfg.fbModuleLink as typeof fragebogenMainModule).moduleId,
              ],
              set: {
                orderIndex,
                isDeleted: false,
                deletedAt: null,
                updatedAt: now,
              },
            });
        }
      }

      if (scope === "main") {
        await saveSpezialfragenTx(tx, req.params.id, parsed.data.spezialfragen);
      }
    });

    const [fragebogen] = await fetchFragebogenUi(scope, [req.params.id]);
    if (!fragebogen) {
      res.status(404).json({ error: "Fragebogen nicht gefunden." });
      return;
    }
    res.status(200).json({ fragebogen });
  } catch (error) {
    if (error instanceof Error && (error.message.includes("Modul") || error.message.includes("Fragebogen"))) {
      res.status(error.message.includes("nicht gefunden") ? 404 : 400).json({ error: error.message });
      return;
    }
    next(error);
  }
});

adminFragebogenRouter.patch("/fragebogen/:scope/:id/delete", async (req, res, next) => {
  try {
    const scope = getScopeParam(req.params as Record<string, string | undefined>);
    if (!isUuid(req.params.id)) {
      res.status(400).json({ error: "Ungueltige Fragebogen-ID." });
      return;
    }
    const cfg = pickScopeConfig(scope);
    await db
      .update(cfg.fbTable as typeof fragebogenMain)
      .set({ isDeleted: true, updatedAt: new Date() })
      .where(eq((cfg.fbTable as typeof fragebogenMain).id, req.params.id));
    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.post("/fragebogen/:scope/:id/duplicate", async (req, res, next) => {
  try {
    const sourceScope = getScopeParam(req.params as Record<string, string | undefined>);
    if (!isUuid(req.params.id)) {
      res.status(400).json({ error: "Ungueltige Fragebogen-ID." });
      return;
    }
    const body = z
      .object({
        targetScope: scopeSchema.optional(),
        sectionKeywords: z.array(mainSectionSchema).optional(),
      })
      .strict()
      .safeParse(req.body ?? {});
    if (!body.success) {
      res.status(400).json({ error: "Ungueltige Duplicate-Payload." });
      return;
    }
    const targetScope = body.data.targetScope ?? sourceScope;

    const [source] = await fetchFragebogenUi(sourceScope, [req.params.id]);
    if (!source) {
      res.status(404).json({ error: "Quelle nicht gefunden." });
      return;
    }

    if (targetScope !== "main" && (source.spezialfragen?.length ?? 0) > 0) {
      res.status(400).json({ error: "Spezialfragen koennen nur im Main-Scope dupliziert werden." });
      return;
    }

    const now = new Date();
    const createdId = await db.transaction(async (tx) => {
      await validateModuleIdsExist(tx, targetScope, source.moduleIds);
      const targetCfg = pickScopeConfig(targetScope);
      const [created] = await tx
        .insert(targetCfg.fbTable as typeof fragebogenMain)
        .values({
          name: `Kopie von ${source.name}`,
          description: source.description ?? "",
          ...(targetScope === "main"
            ? {
                sectionKeywords:
                  body.data.sectionKeywords && body.data.sectionKeywords.length > 0
                    ? body.data.sectionKeywords
                    : ((source.sectionKeywords as MainSection[] | undefined) ?? (["standard"] as MainSection[])),
              }
            : {}),
          nurEinmalAusfuellbar: source.nurEinmalAusfuellbar ?? false,
          status: "inactive",
          scheduleType: "always",
          startDate: null,
          endDate: null,
          createdAt: now,
          updatedAt: now,
        } as typeof fragebogenMain.$inferInsert)
        .returning();
      if (!created) {
        throw new Error("Fragebogen-Duplikat konnte nicht erstellt werden.");
      }

      const moduleIds = Array.from(new Set(source.moduleIds));
      if (moduleIds.length > 0) {
        await tx.insert(targetCfg.fbModuleLink as typeof fragebogenMainModule).values(
          moduleIds.map((moduleId, orderIndex) => ({
            fragebogenId: created.id,
            moduleId,
            orderIndex,
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          })),
        );
      }
      if (targetScope === "main") {
        await saveSpezialfragenTx(tx, created.id, source.spezialfragen ?? []);
      }
      return created.id;
    });

    const [fragebogen] = await fetchFragebogenUi(targetScope, [createdId]);
    res.status(201).json({ fragebogen });
  } catch (error) {
    if (error instanceof Error && error.message.includes("Modul")) {
      res.status(400).json({ error: error.message });
      return;
    }
    next(error);
  }
});

adminFragebogenRouter.post("/modules/:scope/:id/duplicate", async (req, res, next) => {
  try {
    const sourceScope = getScopeParam(req.params as Record<string, string | undefined>);
    if (!isUuid(req.params.id)) {
      res.status(400).json({ error: "Ungueltige Modul-ID." });
      return;
    }
    const body = z
      .object({
        targetScope: scopeSchema.optional(),
        sectionKeywords: z.array(mainSectionSchema).optional(),
      })
      .strict()
      .safeParse(req.body ?? {});
    if (!body.success) {
      res.status(400).json({ error: "Ungueltige Duplicate-Payload." });
      return;
    }
    const targetScope = body.data.targetScope ?? sourceScope;

    const [source] = await fetchModulesUi(sourceScope, [req.params.id]);
    if (!source) {
      res.status(404).json({ error: "Quelle nicht gefunden." });
      return;
    }

    const now = new Date();
    const createdId = await db.transaction(async (tx) => {
      const cfg = pickScopeConfig(targetScope);
      const [created] = await tx
        .insert(cfg.moduleTable as typeof moduleMain)
        .values({
          name: `Kopie von ${source.name}`,
          description: source.description ?? "",
          ...(targetScope === "main"
            ? {
                sectionKeywords:
                  body.data.sectionKeywords && body.data.sectionKeywords.length > 0
                    ? body.data.sectionKeywords
                    : ((source.sectionKeywords as MainSection[] | undefined) ?? (["standard"] as MainSection[])),
              }
            : {}),
          createdAt: now,
          updatedAt: now,
        } as typeof moduleMain.$inferInsert)
        .returning();
      if (!created) {
        throw new Error("Modul-Duplikat konnte nicht erstellt werden.");
      }

      const questionIds = source.questions.map((question) => question.id).filter((id): id is string => typeof id === "string");
      const sourceChainsByQuestionId = new Map(
        source.questions
          .filter((question) => typeof question.id === "string")
          .map((question) => [question.id as string, question.chains]),
      );
      ensureAllUuids(questionIds, "Frage");
      if (questionIds.length > 0) {
        const existing = await tx
          .select({ id: questionBankShared.id })
          .from(questionBankShared)
          .where(and(inArray(questionBankShared.id, questionIds), eq(questionBankShared.isDeleted, false)));
        const found = new Set(existing.map((row) => row.id));
        const missing = questionIds.filter((id) => !found.has(id));
        if (missing.length > 0) {
          throw new DomainValidationError("Mindestens eine verknuepfte Frage existiert nicht mehr.");
        }
      }
      if (questionIds.length > 0) {
        await tx.insert(cfg.linkTable as typeof moduleMainQuestion).values(
          questionIds.map((questionId, orderIndex) => ({
            moduleId: created.id,
            questionId,
            orderIndex,
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          })),
        );
      }
      await upsertModuleQuestionChainsTx(tx, {
        scope: targetScope,
        moduleId: created.id,
        questionChainInputs: questionIds.map((questionId) => {
          const chains = sourceChainsByQuestionId.get(questionId);
          return chains ? { questionId, chains } : { questionId };
        }),
      });
      return created.id;
    });

    const [module] = await fetchModulesUi(targetScope, [createdId]);
    res.status(201).json({ module });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.get("/question-map", async (req, res, next) => {
  try {
    const parsed = z
      .object({
        ids: z.string().optional(),
      })
      .safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Query." });
      return;
    }
    const ids = (parsed.data.ids ?? "")
      .split(",")
      .map((entry) => entry.trim())
      .filter(isUuid);
    if (ids.length === 0) {
      res.status(200).json({ questions: [] });
      return;
    }
    const questionMap = await fetchQuestionsByIds(db, ids);
    res.status(200).json({ questions: ids.map((id) => questionMap.get(id)).filter(Boolean) });
  } catch (error) {
    next(error);
  }
});

adminFragebogenRouter.use((err: unknown, _req: Request, res: Response, next: NextFunction) => {
  if (err instanceof DomainValidationError) {
    res.status(400).json({ error: err.message });
    return;
  }
  if (err instanceof z.ZodError) {
    res.status(400).json({ error: "Ungueltige Eingabedaten." });
    return;
  }
  next(err);
});

export { adminFragebogenRouter };
