import { createHash, randomUUID } from "node:crypto";
import { and, asc, desc, eq, inArray, or, sql } from "drizzle-orm";
import { type NextFunction, type Request, type Response, Router } from "express";
import { z } from "zod";

import { db } from "../lib/db.js";
import { logAction, startActionTimer } from "../lib/logger.js";
import {
  smAnswerOptionVersions,
  smModules,
  smModuleVersionQuestions,
  smModuleVersions,
  smQuestionLogicRules,
  smQuestionLogicRuleTargets,
  smQuestions,
  smQuestionnaireTemplates,
  smQuestionnaireVersionModules,
  smQuestionnaireVersions,
  smQuestionVersions,
} from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const smQuestionTypes = [
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
] as const;
const smLogicOperators = [
  "equals",
  "not_equals",
  "contains",
  "not_contains",
  "greater_than",
  "less_than",
  "between",
  "is_answered",
  "is_not_answered",
] as const;
const smOosCategories = [
  "action_placements",
  "softdrinks_energy",
  "water_near_water",
  "juice_iced_tea",
] as const;
const smOosOutcomes = [
  "oos_present",
  "oos_absent",
  "resolved",
  "partially_resolved",
  "not_resolved",
  "not_applicable",
] as const;

const conditionalRuleSchema = z.object({
  id: z.string().max(200),
  triggerQuestionId: z.string().min(1).max(200),
  operator: z.enum(smLogicOperators),
  triggerValue: z.string().max(10_000).default(""),
  triggerValueMax: z.string().max(10_000).default(""),
  action: z.enum(["hide", "show"]),
  targetQuestionIds: z.array(z.string().min(1).max(200)).max(500),
}).strict();

const oosConfigSchema = z.object({
  enabled: z.boolean().optional(),
  role: z.enum(["detection", "remediation"]).optional(),
  category: z.enum(smOosCategories).optional(),
  detectionQuestionId: z.string().min(1).max(200).optional(),
  answerOutcomes: z.record(z.string().max(1_000), z.enum(smOosOutcomes)).optional(),
  partialCountsAsResolved: z.boolean().optional(),
  behobenAnswer: z.string().max(1_000).optional(),
  nichtBehobenAnswer: z.string().max(1_000).optional(),
}).strict();

const questionSchema = z.object({
  id: z.string().min(1).max(200),
  text: z.string().trim().min(1).max(20_000),
  type: z.enum(smQuestionTypes),
  required: z.boolean(),
  options: z.array(z.string().max(1_000)).max(500),
  config: z.record(z.string(), z.unknown()),
  rules: z.array(conditionalRuleSchema).max(500),
  oos: oosConfigSchema.optional(),
}).strict();

const moduleSchema = z.object({
  id: z.string().min(1).max(200),
  name: z.string().trim().min(1).max(500),
  description: z.string().trim().max(10_000).default(""),
  questions: z.array(questionSchema).min(1).max(500),
  createdAt: z.string().datetime().optional(),
}).strict();

const questionnaireSchema = z.object({
  id: z.string().min(1).max(200),
  name: z.string().trim().min(1).max(500),
  description: z.string().trim().max(10_000).default(""),
  moduleIds: z.array(z.string().uuid()).min(1).max(500),
  status: z.enum(["active", "inactive"]),
  version: z.number().int().positive().optional(),
  createdAt: z.string().datetime().optional(),
  nurEinmalAusfuellbar: z.boolean().optional(),
}).strict();

type SmModuleInput = z.infer<typeof moduleSchema>;
type SmQuestionInput = z.infer<typeof questionSchema>;
type SmQuestionnaireInput = z.infer<typeof questionnaireSchema>;
type DbTx = Parameters<Parameters<typeof db.transaction>[0]>[0];

type UiQuestion = SmQuestionInput;
type UiModule = {
  id: string;
  name: string;
  description: string;
  questions: UiQuestion[];
  createdAt: string;
};
type UiQuestionnaire = {
  id: string;
  name: string;
  description: string;
  moduleIds: string[];
  status: "active" | "inactive";
  version: number;
  createdAt: string;
  nurEinmalAusfuellbar: boolean;
};

class SmQuestionnaireDomainError extends Error {
  constructor(public readonly statusCode: number, message: string) {
    super(message);
  }
}

function isUuid(value: string): boolean {
  return z.string().uuid().safeParse(value).success;
}

function routeId(req: Request): string {
  const value = req.params.id;
  return Array.isArray(value) ? value[0] ?? "" : value ?? "";
}

function stableCode(prefix: "q" | "module" | "questionnaire", id: string): string {
  return `${prefix}_${id.replaceAll("-", "")}`;
}

function jsonValue(value: string): string | number | boolean | null {
  const trimmed = value.trim();
  if (!trimmed) return null;
  if (trimmed === "true") return true;
  if (trimmed === "false") return false;
  const numberValue = Number(trimmed.replace(",", "."));
  if (Number.isFinite(numberValue) && /^-?\d+(?:[.,]\d+)?$/.test(trimmed)) return numberValue;
  return value;
}

function jsonValueToInput(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return JSON.stringify(value);
}

function optionsForQuestion(question: SmQuestionInput): string[] {
  if (question.type === "yesno") return ["Ja", "Nein"];
  if (question.type === "yesnomulti") {
    const answers = Array.isArray(question.config.answers) ? question.config.answers : question.options;
    return answers.map((value) => String(value).trim()).filter(Boolean);
  }
  if (question.type === "single" || question.type === "multiple") {
    const values = Array.isArray(question.config.options) ? question.config.options : question.options;
    return values.map((value) => String(value).trim()).filter(Boolean);
  }
  if (question.type === "likert") {
    const min = Number(question.config.min ?? 1);
    const max = Number(question.config.max ?? 5);
    if (!Number.isInteger(min) || !Number.isInteger(max) || min > max || max - min > 20) return [];
    return Array.from({ length: max - min + 1 }, (_, index) => String(min + index));
  }
  return [];
}

function validateModule(input: SmModuleInput): void {
  const ids = new Set<string>();
  const questionsById = new Map<string, SmQuestionInput>();
  const questionIndex = new Map<string, number>();
  input.questions.forEach((question, index) => {
    if (ids.has(question.id)) throw new SmQuestionnaireDomainError(400, "Jede Frage im Modul benötigt eine eindeutige ID.");
    ids.add(question.id);
    questionsById.set(question.id, question);
    questionIndex.set(question.id, index);
    if (["single", "yesno", "yesnomulti", "multiple", "likert"].includes(question.type) && optionsForQuestion(question).length < 2) {
      throw new SmQuestionnaireDomainError(400, `Die Auswahlfrage „${question.text}“ benötigt mindestens zwei Antworten.`);
    }
  });

  for (const [ownerIndex, question] of input.questions.entries()) {
    if (question.oos?.enabled) {
      if (!question.oos.role || !question.oos.category) {
        throw new SmQuestionnaireDomainError(400, `Die OOS-Zuordnung bei „${question.text}“ ist nicht vollständig.`);
      }
      if (!question.oos.answerOutcomes || Object.keys(question.oos.answerOutcomes).length === 0) {
        throw new SmQuestionnaireDomainError(400, `Bitte ordne bei „${question.text}“ mindestens eine Antwort einer OOS-Auswertung zu.`);
      }
      if (question.oos.role === "remediation") {
        const detection = question.oos.detectionQuestionId ? questionsById.get(question.oos.detectionQuestionId) : undefined;
        if (!detection || detection.oos?.enabled !== true || detection.oos.role !== "detection" || detection.oos.category !== question.oos.category) {
          throw new SmQuestionnaireDomainError(400, `Die OOS-Behebungsfrage „${question.text}“ benötigt eine passende Erkennungsfrage derselben Kategorie.`);
        }
      }
    }

    for (const rule of question.rules) {
      const triggerIndex = questionIndex.get(rule.triggerQuestionId);
      if (triggerIndex === undefined) throw new SmQuestionnaireDomainError(400, "Eine Logikregel verweist auf eine nicht vorhandene Auslöserfrage.");
      if (rule.targetQuestionIds.length === 0) throw new SmQuestionnaireDomainError(400, "Eine Logikregel benötigt mindestens eine betroffene Frage.");
      for (const targetId of new Set(rule.targetQuestionIds)) {
        const targetIndex = questionIndex.get(targetId);
        if (targetIndex === undefined) throw new SmQuestionnaireDomainError(400, "Eine Logikregel verweist auf eine nicht vorhandene Zielfrage.");
        if (targetId === rule.triggerQuestionId || targetIndex <= triggerIndex) {
          throw new SmQuestionnaireDomainError(400, "Bedingte Logik darf nur spätere Fragen steuern.");
        }
      }
      if (rule.operator === "between" && (!rule.triggerValue.trim() || !rule.triggerValueMax.trim())) {
        throw new SmQuestionnaireDomainError(400, "Für den Operator „zwischen“ werden Minimum und Maximum benötigt.");
      }
      if (ownerIndex < triggerIndex) {
        throw new SmQuestionnaireDomainError(400, "Eine Logikregel kann keine spätere Frage als Auslöser verwenden.");
      }
    }
  }
}

function contentHash(payload: unknown): string {
  return createHash("sha256").update(JSON.stringify(canonicalize(payload))).digest("hex");
}

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value as Record<string, unknown>)
      .filter(([, item]) => item !== undefined)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, item]) => [key, canonicalize(item)]));
  }
  return value;
}

function questionSignature(question: SmQuestionInput): string {
  const options = optionsForQuestion(question);
  const outcomes = Object.fromEntries(options
    .map((label) => [label, question.oos?.answerOutcomes?.[label]])
    .filter((entry) => Boolean(entry[1])));
  return contentHash({
    text: question.text.trim(),
    type: question.type,
    required: question.required,
    options,
    config: question.config,
    rules: question.rules.map((rule) => ({
      triggerQuestionId: rule.triggerQuestionId,
      operator: rule.operator,
      triggerValue: rule.triggerValue,
      triggerValueMax: rule.triggerValueMax,
      action: rule.action,
      targetQuestionIds: Array.from(new Set(rule.targetQuestionIds)),
    })),
    oos: question.oos?.enabled ? {
      enabled: true,
      role: question.oos.role,
      category: question.oos.category,
      detectionQuestionId: question.oos.detectionQuestionId,
      answerOutcomes: outcomes,
      partialCountsAsResolved: question.oos.partialCountsAsResolved,
    } : undefined,
  });
}

async function loadCurrentModuleGraph(tx: DbTx, moduleId: string): Promise<{
  version: typeof smModuleVersions.$inferSelect;
  questions: Map<string, { versionId: string; input: SmQuestionInput }>;
  order: string[];
} | null> {
  const [version] = await tx.select().from(smModuleVersions).where(and(
    eq(smModuleVersions.moduleId, moduleId),
    eq(smModuleVersions.status, "published"),
    eq(smModuleVersions.isDeleted, false),
  )).orderBy(desc(smModuleVersions.versionNumber)).limit(1);
  if (!version) return null;

  const links = await tx.select().from(smModuleVersionQuestions).where(and(
    eq(smModuleVersionQuestions.moduleVersionId, version.id),
    eq(smModuleVersionQuestions.isDeleted, false),
  )).orderBy(asc(smModuleVersionQuestions.orderIndex));
  const versionIds = links.map((link) => link.questionVersionId);
  if (versionIds.length === 0) return { version, questions: new Map(), order: [] };

  const questionVersions = await tx.select().from(smQuestionVersions).where(and(
    inArray(smQuestionVersions.id, versionIds),
    eq(smQuestionVersions.isDeleted, false),
  ));
  const versionById = new Map(questionVersions.map((row) => [row.id, row]));
  const rootByVersionId = new Map(questionVersions.map((row) => [row.id, row.questionId]));

  const optionRows = await tx.select().from(smAnswerOptionVersions).where(and(
    inArray(smAnswerOptionVersions.questionVersionId, versionIds),
    eq(smAnswerOptionVersions.isDeleted, false),
  )).orderBy(asc(smAnswerOptionVersions.orderIndex));
  const optionsByVersion = new Map<string, typeof optionRows>();
  for (const option of optionRows) {
    const values = optionsByVersion.get(option.questionVersionId) ?? [];
    values.push(option);
    optionsByVersion.set(option.questionVersionId, values);
  }

  const ownerGroupCodes = versionIds.map((id) => `ui_owner_version:${id}`);
  const ruleRows = await tx.select().from(smQuestionLogicRules).where(and(
    or(
      inArray(smQuestionLogicRules.triggerQuestionVersionId, versionIds),
      inArray(smQuestionLogicRules.groupCode, ownerGroupCodes),
    ),
    eq(smQuestionLogicRules.isDeleted, false),
  )).orderBy(asc(smQuestionLogicRules.orderIndex));
  const ruleIds = ruleRows.map((rule) => rule.id);
  const targetRows = ruleIds.length === 0 ? [] : await tx.select().from(smQuestionLogicRuleTargets).where(and(
    inArray(smQuestionLogicRuleTargets.ruleId, ruleIds),
    eq(smQuestionLogicRuleTargets.isDeleted, false),
  )).orderBy(asc(smQuestionLogicRuleTargets.orderIndex));
  const targetsByRule = new Map<string, typeof targetRows>();
  for (const target of targetRows) {
    const values = targetsByRule.get(target.ruleId) ?? [];
    values.push(target);
    targetsByRule.set(target.ruleId, values);
  }
  const missingRuleVersionIds = Array.from(new Set([
    ...ruleRows.map((rule) => rule.triggerQuestionVersionId),
    ...targetRows.map((target) => target.targetQuestionVersionId),
  ])).filter((id) => !rootByVersionId.has(id));
  if (missingRuleVersionIds.length > 0) {
    const referencedVersions = await tx.select({ id: smQuestionVersions.id, questionId: smQuestionVersions.questionId })
      .from(smQuestionVersions).where(inArray(smQuestionVersions.id, missingRuleVersionIds));
    for (const referenced of referencedVersions) rootByVersionId.set(referenced.id, referenced.questionId);
  }
  const rulesByOwnerRoot = new Map<string, SmQuestionInput["rules"]>();
  for (const rule of ruleRows) {
    const triggerRootId = rootByVersionId.get(rule.triggerQuestionVersionId);
    if (!triggerRootId) continue;
    const ownerVersionId = rule.groupCode.startsWith("ui_owner_version:") ? rule.groupCode.slice("ui_owner_version:".length) : undefined;
    const ownerRootId = ownerVersionId
      ? rootByVersionId.get(ownerVersionId) ?? triggerRootId
      : rule.groupCode.startsWith("ui_owner:") ? rule.groupCode.slice("ui_owner:".length) : triggerRootId;
    const values = rulesByOwnerRoot.get(ownerRootId) ?? [];
    values.push({
      id: rule.id,
      triggerQuestionId: triggerRootId,
      operator: rule.operator,
      triggerValue: jsonValueToInput(rule.triggerValue),
      triggerValueMax: jsonValueToInput(rule.triggerValueMax),
      action: rule.action,
      targetQuestionIds: (targetsByRule.get(rule.id) ?? [])
        .map((target) => rootByVersionId.get(target.targetQuestionVersionId))
        .filter((value): value is string => Boolean(value)),
    });
    rulesByOwnerRoot.set(ownerRootId, values);
  }

  const questions = new Map<string, { versionId: string; input: SmQuestionInput }>();
  for (const link of links) {
    const row = versionById.get(link.questionVersionId);
    if (!row) continue;
    const options = optionsByVersion.get(row.id) ?? [];
    const metricConfig = row.metricConfig && typeof row.metricConfig === "object" ? row.metricConfig : {};
    const role = row.metricRole === "oos_detection" ? "detection" : row.metricRole === "oos_remediation" ? "remediation" : undefined;
    const answerOutcomes = Object.fromEntries(options
      .filter((option) => smOosOutcomes.includes(option.metricOutcomeCode as typeof smOosOutcomes[number]))
      .map((option) => [option.label, option.metricOutcomeCode])) as Record<string, typeof smOosOutcomes[number]>;
    questions.set(row.questionId, {
      versionId: row.id,
      input: {
        id: row.questionId,
        text: row.questionText,
        type: row.questionType,
        required: row.required,
        options: options.map((option) => option.label),
        config: row.config,
        rules: rulesByOwnerRoot.get(row.questionId) ?? [],
        ...(role && row.oosCategory ? {
          oos: {
            enabled: true,
            role,
            category: row.oosCategory,
            ...(typeof metricConfig.detectionQuestionId === "string" ? { detectionQuestionId: metricConfig.detectionQuestionId } : {}),
            answerOutcomes,
            ...(typeof metricConfig.partialCountsAsResolved === "boolean" ? { partialCountsAsResolved: metricConfig.partialCountsAsResolved } : {}),
          },
        } : {}),
      },
    });
  }
  return {
    version,
    questions,
    order: links.map((link) => rootByVersionId.get(link.questionVersionId)).filter((value): value is string => Boolean(value)),
  };
}

async function loadWorkspace(): Promise<{ modules: UiModule[]; questionnaires: UiQuestionnaire[] }> {
  const moduleRoots = await db.select().from(smModules).where(eq(smModules.isDeleted, false)).orderBy(asc(smModules.createdAt));
  const moduleRootIds = moduleRoots.map((row) => row.id);
  const allModuleVersions = moduleRootIds.length === 0 ? [] : await db
    .select()
    .from(smModuleVersions)
    .where(and(inArray(smModuleVersions.moduleId, moduleRootIds), eq(smModuleVersions.status, "published"), eq(smModuleVersions.isDeleted, false)))
    .orderBy(desc(smModuleVersions.versionNumber));
  const latestModuleVersionByRoot = new Map<string, typeof smModuleVersions.$inferSelect>();
  const moduleVersionById = new Map<string, typeof smModuleVersions.$inferSelect>();
  for (const version of allModuleVersions) {
    moduleVersionById.set(version.id, version);
    if (!latestModuleVersionByRoot.has(version.moduleId)) latestModuleVersionByRoot.set(version.moduleId, version);
  }

  const latestModuleVersionIds = Array.from(latestModuleVersionByRoot.values()).map((row) => row.id);
  const moduleQuestionLinks = latestModuleVersionIds.length === 0 ? [] : await db
    .select()
    .from(smModuleVersionQuestions)
    .where(and(inArray(smModuleVersionQuestions.moduleVersionId, latestModuleVersionIds), eq(smModuleVersionQuestions.isDeleted, false)))
    .orderBy(asc(smModuleVersionQuestions.orderIndex));
  const questionVersionIds = Array.from(new Set(moduleQuestionLinks.map((row) => row.questionVersionId)));
  const questionVersions = questionVersionIds.length === 0 ? [] : await db
    .select()
    .from(smQuestionVersions)
    .where(and(inArray(smQuestionVersions.id, questionVersionIds), eq(smQuestionVersions.isDeleted, false)));
  const questionVersionById = new Map(questionVersions.map((row) => [row.id, row]));
  const questionRootIds = Array.from(new Set(questionVersions.map((row) => row.questionId)));
  const questionRoots = questionRootIds.length === 0 ? [] : await db
    .select()
    .from(smQuestions)
    .where(and(inArray(smQuestions.id, questionRootIds), eq(smQuestions.isDeleted, false)));
  const activeQuestionRootIds = new Set(questionRoots.map((row) => row.id));
  const rootIdByVersionId = new Map(questionVersions.map((row) => [row.id, row.questionId]));

  const optionRows = questionVersionIds.length === 0 ? [] : await db
    .select()
    .from(smAnswerOptionVersions)
    .where(and(inArray(smAnswerOptionVersions.questionVersionId, questionVersionIds), eq(smAnswerOptionVersions.isDeleted, false)))
    .orderBy(asc(smAnswerOptionVersions.orderIndex));
  const optionsByQuestionVersion = new Map<string, typeof optionRows>();
  for (const option of optionRows) {
    const list = optionsByQuestionVersion.get(option.questionVersionId) ?? [];
    list.push(option);
    optionsByQuestionVersion.set(option.questionVersionId, list);
  }

  const ownerGroupCodes = questionVersionIds.map((id) => `ui_owner_version:${id}`);
  const ruleRows = questionVersionIds.length === 0 ? [] : await db
    .select()
    .from(smQuestionLogicRules)
    .where(and(or(
      inArray(smQuestionLogicRules.triggerQuestionVersionId, questionVersionIds),
      inArray(smQuestionLogicRules.groupCode, ownerGroupCodes),
    ), eq(smQuestionLogicRules.isDeleted, false)))
    .orderBy(asc(smQuestionLogicRules.orderIndex));
  const ruleIds = ruleRows.map((row) => row.id);
  const targetRows = ruleIds.length === 0 ? [] : await db
    .select()
    .from(smQuestionLogicRuleTargets)
    .where(and(inArray(smQuestionLogicRuleTargets.ruleId, ruleIds), eq(smQuestionLogicRuleTargets.isDeleted, false)))
    .orderBy(asc(smQuestionLogicRuleTargets.orderIndex));
  const targetsByRule = new Map<string, typeof targetRows>();
  for (const target of targetRows) {
    const list = targetsByRule.get(target.ruleId) ?? [];
    list.push(target);
    targetsByRule.set(target.ruleId, list);
  }
  const missingRuleVersionIds = Array.from(new Set([
    ...ruleRows.map((rule) => rule.triggerQuestionVersionId),
    ...targetRows.map((target) => target.targetQuestionVersionId),
  ])).filter((id) => !rootIdByVersionId.has(id));
  if (missingRuleVersionIds.length > 0) {
    const referencedVersions = await db.select({ id: smQuestionVersions.id, questionId: smQuestionVersions.questionId })
      .from(smQuestionVersions).where(inArray(smQuestionVersions.id, missingRuleVersionIds));
    for (const referenced of referencedVersions) rootIdByVersionId.set(referenced.id, referenced.questionId);
  }
  const rulesByOwnerRoot = new Map<string, UiQuestion["rules"]>();
  for (const rule of ruleRows) {
    const triggerRootId = rootIdByVersionId.get(rule.triggerQuestionVersionId);
    if (!triggerRootId) continue;
    const ownerVersionId = rule.groupCode.startsWith("ui_owner_version:") ? rule.groupCode.slice("ui_owner_version:".length) : undefined;
    const ownerFromGroup = ownerVersionId
      ? rootIdByVersionId.get(ownerVersionId)
      : rule.groupCode.startsWith("ui_owner:") ? rule.groupCode.slice("ui_owner:".length) : triggerRootId;
    const ownerRootId = ownerFromGroup && activeQuestionRootIds.has(ownerFromGroup) ? ownerFromGroup : triggerRootId;
    const rules = rulesByOwnerRoot.get(ownerRootId) ?? [];
    rules.push({
      id: rule.id,
      triggerQuestionId: triggerRootId,
      operator: rule.operator,
      triggerValue: jsonValueToInput(rule.triggerValue),
      triggerValueMax: jsonValueToInput(rule.triggerValueMax),
      action: rule.action,
      targetQuestionIds: (targetsByRule.get(rule.id) ?? [])
        .map((target) => rootIdByVersionId.get(target.targetQuestionVersionId))
        .filter((value): value is string => Boolean(value)),
    });
    rulesByOwnerRoot.set(ownerRootId, rules);
  }

  const linksByModuleVersion = new Map<string, typeof moduleQuestionLinks>();
  for (const link of moduleQuestionLinks) {
    const list = linksByModuleVersion.get(link.moduleVersionId) ?? [];
    list.push(link);
    linksByModuleVersion.set(link.moduleVersionId, list);
  }

  const modules: UiModule[] = [];
  for (const root of moduleRoots) {
    const version = latestModuleVersionByRoot.get(root.id);
    if (!version) continue;
    const questions: UiQuestion[] = [];
    for (const link of linksByModuleVersion.get(version.id) ?? []) {
      const questionVersion = questionVersionById.get(link.questionVersionId);
      if (!questionVersion || !activeQuestionRootIds.has(questionVersion.questionId)) continue;
      const options = optionsByQuestionVersion.get(questionVersion.id) ?? [];
      const metricConfig = questionVersion.metricConfig && typeof questionVersion.metricConfig === "object"
        ? questionVersion.metricConfig as Record<string, unknown>
        : {};
      const oosRole = questionVersion.metricRole === "oos_detection"
        ? "detection"
        : questionVersion.metricRole === "oos_remediation"
          ? "remediation"
          : undefined;
      const answerOutcomes = Object.fromEntries(options
        .filter((option) => smOosOutcomes.includes(option.metricOutcomeCode as typeof smOosOutcomes[number]))
        .map((option) => [option.label, option.metricOutcomeCode])) as Record<string, typeof smOosOutcomes[number]>;
      questions.push({
        id: questionVersion.questionId,
        text: questionVersion.questionText,
        type: questionVersion.questionType,
        required: questionVersion.required,
        options: options.map((option) => option.label),
        config: questionVersion.config,
        rules: rulesByOwnerRoot.get(questionVersion.questionId) ?? [],
        ...(oosRole && questionVersion.oosCategory ? {
          oos: {
            enabled: true,
            role: oosRole,
            category: questionVersion.oosCategory,
            ...(typeof metricConfig.detectionQuestionId === "string" ? { detectionQuestionId: metricConfig.detectionQuestionId } : {}),
            answerOutcomes,
            ...(typeof metricConfig.partialCountsAsResolved === "boolean" ? { partialCountsAsResolved: metricConfig.partialCountsAsResolved } : {}),
          },
        } : {}),
      });
    }
    modules.push({ id: root.id, name: version.name, description: version.description, questions, createdAt: root.createdAt.toISOString() });
  }

  const questionnaireRoots = await db
    .select()
    .from(smQuestionnaireTemplates)
    .where(and(eq(smQuestionnaireTemplates.isDeleted, false), inArray(smQuestionnaireTemplates.status, ["active", "inactive"])))
    .orderBy(asc(smQuestionnaireTemplates.createdAt));
  const questionnaireRootIds = questionnaireRoots.map((row) => row.id);
  const questionnaireVersions = questionnaireRootIds.length === 0 ? [] : await db
    .select()
    .from(smQuestionnaireVersions)
    .where(and(
      inArray(smQuestionnaireVersions.questionnaireTemplateId, questionnaireRootIds),
      eq(smQuestionnaireVersions.status, "published"),
      eq(smQuestionnaireVersions.isDeleted, false),
    ))
    .orderBy(desc(smQuestionnaireVersions.versionNumber));
  const latestQuestionnaireVersionByRoot = new Map<string, typeof smQuestionnaireVersions.$inferSelect>();
  for (const version of questionnaireVersions) {
    if (!latestQuestionnaireVersionByRoot.has(version.questionnaireTemplateId)) latestQuestionnaireVersionByRoot.set(version.questionnaireTemplateId, version);
  }
  const latestQuestionnaireVersionIds = Array.from(latestQuestionnaireVersionByRoot.values()).map((row) => row.id);
  const questionnaireModuleLinks = latestQuestionnaireVersionIds.length === 0 ? [] : await db
    .select()
    .from(smQuestionnaireVersionModules)
    .where(and(
      inArray(smQuestionnaireVersionModules.questionnaireVersionId, latestQuestionnaireVersionIds),
      eq(smQuestionnaireVersionModules.isDeleted, false),
    ))
    .orderBy(asc(smQuestionnaireVersionModules.orderIndex));
  const questionnaireLinksByVersion = new Map<string, typeof questionnaireModuleLinks>();
  for (const link of questionnaireModuleLinks) {
    const list = questionnaireLinksByVersion.get(link.questionnaireVersionId) ?? [];
    list.push(link);
    questionnaireLinksByVersion.set(link.questionnaireVersionId, list);
  }
  const questionnaires: UiQuestionnaire[] = [];
  for (const root of questionnaireRoots) {
    const version = latestQuestionnaireVersionByRoot.get(root.id);
    if (!version) continue;
    questionnaires.push({
      id: root.id,
      name: version.name,
      description: version.description,
      moduleIds: (questionnaireLinksByVersion.get(version.id) ?? [])
        .map((link) => moduleVersionById.get(link.moduleVersionId)?.moduleId)
        .filter((value): value is string => Boolean(value)),
      status: root.status === "active" ? "active" : "inactive",
      version: version.versionNumber,
      createdAt: root.createdAt.toISOString(),
      nurEinmalAusfuellbar: version.oncePerMarket,
    });
  }
  return { modules, questionnaires };
}

async function createQuestionGraph(
  tx: DbTx,
  input: SmModuleInput,
  actorUserId: string,
  currentQuestions: Map<string, { versionId: string; input: SmQuestionInput }>,
  changedQuestionIds: Set<string>,
): Promise<Array<{ inputId: string; rootId: string; versionId: string }>> {
  const mapped: Array<{ input: SmQuestionInput; inputId: string; rootId: string; versionId: string; changed: boolean; versionNumber: number }> = [];
  for (const question of input.questions) {
    let rootId = question.id;
    let changed = changedQuestionIds.has(question.id);
    if (!isUuid(rootId)) {
      rootId = randomUUID();
      changed = true;
      await tx.insert(smQuestions).values({
        id: rootId,
        stableCode: stableCode("q", rootId),
        createdByUserId: actorUserId,
        updatedByUserId: actorUserId,
      });
    } else if (!currentQuestions.has(rootId)) {
      throw new SmQuestionnaireDomainError(400, "Eine bestehende SM-Frage gehört nicht zu diesem Modul.");
    }

    const current = currentQuestions.get(rootId);
    let versionNumber = 0;
    let versionId = current?.versionId ?? randomUUID();
    if (changed) {
      const [latestVersion] = await tx.select({ versionNumber: smQuestionVersions.versionNumber })
        .from(smQuestionVersions).where(eq(smQuestionVersions.questionId, rootId))
        .orderBy(desc(smQuestionVersions.versionNumber)).limit(1);
      versionNumber = (latestVersion?.versionNumber ?? 0) + 1;
      versionId = randomUUID();
    }
    mapped.push({ input: question, inputId: question.id, rootId, versionId, changed, versionNumber });
  }

  const mappedByInputId = new Map(mapped.map((row) => [row.inputId, row]));
  for (const row of mapped.filter((item) => item.changed)) {
    const question = row.input;
    const metricRole: "none" | "oos_detection" | "oos_remediation" = question.oos?.enabled
      ? question.oos.role === "detection" ? "oos_detection" : "oos_remediation"
      : "none";
    const detectionQuestionId = question.oos?.detectionQuestionId
      ? mappedByInputId.get(question.oos.detectionQuestionId)?.rootId
      : undefined;
    await tx.insert(smQuestionVersions).values({
      id: row.versionId,
      questionId: row.rootId,
      versionNumber: row.versionNumber,
      status: "draft",
      questionType: question.type,
      questionText: question.text,
      required: question.required,
      metricRole,
      oosCategory: question.oos?.enabled ? question.oos.category : null,
      maxPoints: "0",
      config: question.config,
      metricConfig: question.oos?.enabled ? {
        ...(detectionQuestionId ? { detectionQuestionId } : {}),
        ...(typeof question.oos.partialCountsAsResolved === "boolean" ? { partialCountsAsResolved: question.oos.partialCountsAsResolved } : {}),
      } : {},
      createdByUserId: actorUserId,
    });
    await tx.update(smQuestions).set({ updatedByUserId: actorUserId }).where(eq(smQuestions.id, row.rootId));

    const options = optionsForQuestion(row.input);
    if (options.length > 0) {
      await tx.insert(smAnswerOptionVersions).values(options.map((label, orderIndex) => {
        const outcome = row.input.oos?.answerOutcomes?.[label];
        const notApplicable = outcome === "not_applicable";
        return {
          questionVersionId: row.versionId,
          stableCode: `option_${orderIndex + 1}`,
          label,
          earnedPoints: "0",
          possiblePoints: "0",
          metricOutcomeCode: outcome ?? null,
          marksNotApplicable: notApplicable,
          countsInDenominator: !notApplicable,
          orderIndex,
          config: {},
        };
      }));
    }
  }

  for (const owner of mapped.filter((item) => item.changed)) {
    for (const [orderIndex, rule] of owner.input.rules.entries()) {
      const trigger = mappedByInputId.get(rule.triggerQuestionId);
      if (!trigger) throw new SmQuestionnaireDomainError(400, "Eine Logikregel verweist auf eine unbekannte Auslöserfrage.");
      const [createdRule] = await tx.insert(smQuestionLogicRules).values({
        triggerQuestionVersionId: trigger.versionId,
        operator: rule.operator,
        triggerValue: jsonValue(rule.triggerValue),
        triggerValueMax: jsonValue(rule.triggerValueMax),
        action: rule.action,
        groupCode: `ui_owner_version:${owner.versionId}`,
        groupMatch: "all",
        orderIndex,
      }).returning({ id: smQuestionLogicRules.id });
      if (!createdRule) throw new Error("SM_LOGIC_RULE_CREATE_FAILED");
      const targets = Array.from(new Set(rule.targetQuestionIds)).map((targetInputId, targetOrder) => {
        const target = mappedByInputId.get(targetInputId);
        if (!target) throw new SmQuestionnaireDomainError(400, "Eine Logikregel verweist auf eine unbekannte Zielfrage.");
        return { ruleId: createdRule.id, targetQuestionVersionId: target.versionId, orderIndex: targetOrder };
      });
      if (targets.length > 0) await tx.insert(smQuestionLogicRuleTargets).values(targets);
    }
  }

  const now = new Date();
  for (const row of mapped.filter((item) => item.changed)) {
    await tx.update(smQuestionVersions).set({ status: "published", publishedAt: now }).where(eq(smQuestionVersions.id, row.versionId));
  }
  return mapped.map(({ inputId, rootId, versionId }) => ({ inputId, rootId, versionId }));
}

async function saveModule(input: SmModuleInput, actorUserId: string): Promise<string> {
  validateModule(input);
  return db.transaction(async (tx) => {
    let moduleId = input.id;
    let currentGraph: Awaited<ReturnType<typeof loadCurrentModuleGraph>> = null;
    if (isUuid(moduleId)) {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_module:${moduleId}`}, 0))`);
      const [existing] = await tx.select().from(smModules).where(and(eq(smModules.id, moduleId), eq(smModules.isDeleted, false))).limit(1);
      if (!existing) throw new SmQuestionnaireDomainError(404, "SM-Modul nicht gefunden.");
      currentGraph = await loadCurrentModuleGraph(tx, moduleId);
      if (!currentGraph) throw new SmQuestionnaireDomainError(404, "SM-Modulversion nicht gefunden.");
    } else {
      moduleId = randomUUID();
      await tx.insert(smModules).values({
        id: moduleId,
        stableCode: stableCode("module", moduleId),
        createdByUserId: actorUserId,
        updatedByUserId: actorUserId,
      });
    }

    const currentQuestions = currentGraph?.questions ?? new Map<string, { versionId: string; input: SmQuestionInput }>();
    const changedQuestionIds = new Set(input.questions
      .filter((question) => !isUuid(question.id) || questionSignature(question) !== questionSignature(currentQuestions.get(question.id)?.input ?? question))
      .map((question) => question.id));
    const inputExistingIds = new Set(input.questions.map((question) => question.id).filter(isUuid));
    const removedQuestionIds = Array.from(currentQuestions.keys()).filter((id) => !inputExistingIds.has(id));
    const moduleChanged = !currentGraph
      || currentGraph.version.name !== input.name
      || currentGraph.version.description !== input.description
      || JSON.stringify(currentGraph.order) !== JSON.stringify(input.questions.map((question) => question.id))
      || changedQuestionIds.size > 0
      || removedQuestionIds.length > 0;
    if (!moduleChanged) return moduleId;

    const questionGraph = await createQuestionGraph(tx, input, actorUserId, currentQuestions, changedQuestionIds);
    const moduleVersionId = randomUUID();
    await tx.insert(smModuleVersions).values({
      id: moduleVersionId,
      moduleId,
      versionNumber: (currentGraph?.version.versionNumber ?? 0) + 1,
      status: "draft",
      name: input.name,
      description: input.description,
      createdByUserId: actorUserId,
    });
    await tx.insert(smModuleVersionQuestions).values(questionGraph.map((question, orderIndex) => ({
      moduleVersionId,
      questionVersionId: question.versionId,
      orderIndex,
    })));
    await tx.update(smModuleVersions).set({ status: "published", publishedAt: new Date() }).where(eq(smModuleVersions.id, moduleVersionId));
    if (removedQuestionIds.length > 0) {
      await tx.update(smQuestions).set({ isDeleted: true, updatedByUserId: actorUserId }).where(inArray(smQuestions.id, removedQuestionIds));
    }
    await tx.update(smModules).set({ updatedByUserId: actorUserId }).where(eq(smModules.id, moduleId));
    return moduleId;
  });
}

async function saveQuestionnaire(input: SmQuestionnaireInput, actorUserId: string): Promise<string> {
  return db.transaction(async (tx) => {
    let templateId = input.id;
    let existingTemplate: typeof smQuestionnaireTemplates.$inferSelect | undefined;
    if (isUuid(templateId)) {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_questionnaire:${templateId}`}, 0))`);
      [existingTemplate] = await tx.select().from(smQuestionnaireTemplates).where(and(
        eq(smQuestionnaireTemplates.id, templateId),
        eq(smQuestionnaireTemplates.isDeleted, false),
      )).limit(1);
      if (!existingTemplate) throw new SmQuestionnaireDomainError(404, "SM-Fragebogen nicht gefunden.");
    } else {
      templateId = randomUUID();
      await tx.insert(smQuestionnaireTemplates).values({
        id: templateId,
        stableCode: stableCode("questionnaire", templateId),
        status: input.status,
        createdByUserId: actorUserId,
        updatedByUserId: actorUserId,
      });
    }

    const moduleRoots = await tx.select().from(smModules).where(and(
      inArray(smModules.id, input.moduleIds),
      eq(smModules.isDeleted, false),
    ));
    if (moduleRoots.length !== new Set(input.moduleIds).size) {
      throw new SmQuestionnaireDomainError(400, "Mindestens ein ausgewähltes SM-Modul wurde nicht gefunden.");
    }
    const publishedModuleVersions = await tx.select().from(smModuleVersions).where(and(
      inArray(smModuleVersions.moduleId, input.moduleIds),
      eq(smModuleVersions.status, "published"),
      eq(smModuleVersions.isDeleted, false),
    )).orderBy(desc(smModuleVersions.versionNumber));
    const latestByModule = new Map<string, typeof smModuleVersions.$inferSelect>();
    for (const version of publishedModuleVersions) {
      if (!latestByModule.has(version.moduleId)) latestByModule.set(version.moduleId, version);
    }
    if (latestByModule.size !== new Set(input.moduleIds).size) {
      throw new SmQuestionnaireDomainError(400, "Jedes ausgewählte Modul muss eine veröffentlichte Version besitzen.");
    }

    const [latestVersion] = await tx
      .select()
      .from(smQuestionnaireVersions)
      .where(and(
        eq(smQuestionnaireVersions.questionnaireTemplateId, templateId),
        eq(smQuestionnaireVersions.status, "published"),
        eq(smQuestionnaireVersions.isDeleted, false),
      ))
      .orderBy(desc(smQuestionnaireVersions.versionNumber))
      .limit(1);
    const moduleVersionIds = input.moduleIds.map((moduleId) => latestByModule.get(moduleId)!.id);
    const currentLinks = latestVersion ? await tx.select().from(smQuestionnaireVersionModules).where(and(
      eq(smQuestionnaireVersionModules.questionnaireVersionId, latestVersion.id),
      eq(smQuestionnaireVersionModules.isDeleted, false),
    )).orderBy(asc(smQuestionnaireVersionModules.orderIndex)) : [];
    const versionChanged = !latestVersion
      || latestVersion.name !== input.name
      || latestVersion.description !== input.description
      || latestVersion.oncePerMarket !== (input.nurEinmalAusfuellbar ?? false)
      || JSON.stringify(currentLinks.map((link) => link.moduleVersionId)) !== JSON.stringify(moduleVersionIds);
    const statusChanged = !existingTemplate || existingTemplate.status !== input.status;
    if (!versionChanged) {
      if (statusChanged) {
        await tx.update(smQuestionnaireTemplates).set({ status: input.status, updatedByUserId: actorUserId })
          .where(eq(smQuestionnaireTemplates.id, templateId));
      }
      return templateId;
    }

    const versionNumber = (latestVersion?.versionNumber ?? 0) + 1;
    const questionnaireVersionId = randomUUID();
    await tx.insert(smQuestionnaireVersions).values({
      id: questionnaireVersionId,
      questionnaireTemplateId: templateId,
      versionNumber,
      status: "draft",
      name: input.name,
      description: input.description,
      oncePerMarket: input.nurEinmalAusfuellbar ?? false,
      timezone: "Europe/Vienna",
    });
    await tx.insert(smQuestionnaireVersionModules).values(moduleVersionIds.map((moduleVersionId, orderIndex) => ({
      questionnaireVersionId,
      moduleVersionId,
      orderIndex,
    })));
    const now = new Date();
    await tx.update(smQuestionnaireVersions).set({
      status: "published",
      publishedAt: now,
      publishedByUserId: actorUserId,
      contentHash: contentHash({ templateId, versionNumber, name: input.name, description: input.description, oncePerMarket: input.nurEinmalAusfuellbar ?? false, moduleVersionIds }),
    }).where(eq(smQuestionnaireVersions.id, questionnaireVersionId));
    await tx.update(smQuestionnaireTemplates).set({
      status: input.status,
      updatedByUserId: actorUserId,
    }).where(eq(smQuestionnaireTemplates.id, templateId));
    return templateId;
  });
}

export const adminSmQuestionnairesRouter = Router();
adminSmQuestionnairesRouter.use(requireAuth(["admin"]));

adminSmQuestionnairesRouter.use((req, res, next) => {
  if (req.method === "GET") {
    next();
    return;
  }
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "sm_questionnaire_action_completed", {
      req,
      action: "sm_questionnaire_mutation",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

adminSmQuestionnairesRouter.get("/workspace", async (_req, res, next) => {
  try {
    res.status(200).json(await loadWorkspace());
  } catch (error) {
    next(error);
  }
});

adminSmQuestionnairesRouter.post("/modules", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = moduleSchema.safeParse(req.body);
    if (!parsed.success) throw new SmQuestionnaireDomainError(400, "Bitte Modul, Fragen und Antwortoptionen vollständig ausfüllen.");
    const moduleId = await saveModule(parsed.data, req.authUser!.appUserId);
    const workspace = await loadWorkspace();
    const module = workspace.modules.find((row) => row.id === moduleId);
    if (!module) throw new Error("SM_MODULE_READ_AFTER_WRITE_FAILED");
    res.status(201).json({ module });
  } catch (error) {
    next(error);
  }
});

adminSmQuestionnairesRouter.patch("/modules/:id", async (req: AuthedRequest, res, next) => {
  try {
    const id = routeId(req);
    if (!isUuid(id)) throw new SmQuestionnaireDomainError(400, "Ungültige SM-Modul-ID.");
    const parsed = moduleSchema.safeParse({ ...req.body, id });
    if (!parsed.success) throw new SmQuestionnaireDomainError(400, "Bitte Modul, Fragen und Antwortoptionen vollständig ausfüllen.");
    const moduleId = await saveModule(parsed.data, req.authUser!.appUserId);
    const workspace = await loadWorkspace();
    const module = workspace.modules.find((row) => row.id === moduleId);
    if (!module) throw new Error("SM_MODULE_READ_AFTER_WRITE_FAILED");
    res.status(200).json({ module });
  } catch (error) {
    next(error);
  }
});

adminSmQuestionnairesRouter.patch("/modules/:id/delete", async (req: AuthedRequest, res, next) => {
  try {
    const id = routeId(req);
    if (!isUuid(id)) throw new SmQuestionnaireDomainError(400, "Ungültige SM-Modul-ID.");
    const workspace = await loadWorkspace();
    if (workspace.questionnaires.some((questionnaire) => questionnaire.moduleIds.includes(id))) {
      throw new SmQuestionnaireDomainError(409, "Das Modul wird noch in einem Fragebogen verwendet. Entferne es dort zuerst.");
    }
    const [deleted] = await db.update(smModules).set({
      isDeleted: true,
      updatedByUserId: req.authUser!.appUserId,
    }).where(and(eq(smModules.id, id), eq(smModules.isDeleted, false))).returning({ id: smModules.id });
    if (!deleted) throw new SmQuestionnaireDomainError(404, "SM-Modul nicht gefunden.");
    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

adminSmQuestionnairesRouter.post("/questionnaires", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = questionnaireSchema.safeParse(req.body);
    if (!parsed.success) throw new SmQuestionnaireDomainError(400, "Bitte Fragebogenname und mindestens ein Modul auswählen.");
    const questionnaireId = await saveQuestionnaire(parsed.data, req.authUser!.appUserId);
    const workspace = await loadWorkspace();
    const questionnaire = workspace.questionnaires.find((row) => row.id === questionnaireId);
    if (!questionnaire) throw new Error("SM_QUESTIONNAIRE_READ_AFTER_WRITE_FAILED");
    res.status(201).json({ questionnaire });
  } catch (error) {
    next(error);
  }
});

adminSmQuestionnairesRouter.patch("/questionnaires/:id", async (req: AuthedRequest, res, next) => {
  try {
    const id = routeId(req);
    if (!isUuid(id)) throw new SmQuestionnaireDomainError(400, "Ungültige SM-Fragebogen-ID.");
    const parsed = questionnaireSchema.safeParse({ ...req.body, id });
    if (!parsed.success) throw new SmQuestionnaireDomainError(400, "Bitte Fragebogenname und mindestens ein Modul auswählen.");
    const questionnaireId = await saveQuestionnaire(parsed.data, req.authUser!.appUserId);
    const workspace = await loadWorkspace();
    const questionnaire = workspace.questionnaires.find((row) => row.id === questionnaireId);
    if (!questionnaire) throw new Error("SM_QUESTIONNAIRE_READ_AFTER_WRITE_FAILED");
    res.status(200).json({ questionnaire });
  } catch (error) {
    next(error);
  }
});

adminSmQuestionnairesRouter.patch("/questionnaires/:id/delete", async (req: AuthedRequest, res, next) => {
  try {
    const id = routeId(req);
    if (!isUuid(id)) throw new SmQuestionnaireDomainError(400, "Ungültige SM-Fragebogen-ID.");
    const [deleted] = await db.update(smQuestionnaireTemplates).set({
      isDeleted: true,
      updatedByUserId: req.authUser!.appUserId,
    }).where(and(
      eq(smQuestionnaireTemplates.id, id),
      eq(smQuestionnaireTemplates.isDeleted, false),
    )).returning({ id: smQuestionnaireTemplates.id });
    if (!deleted) throw new SmQuestionnaireDomainError(404, "SM-Fragebogen nicht gefunden.");
    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

adminSmQuestionnairesRouter.use((error: unknown, _req: Request, res: Response, next: NextFunction) => {
  if (error instanceof SmQuestionnaireDomainError) {
    res.status(error.statusCode).json({ error: error.message });
    return;
  }
  next(error);
});
