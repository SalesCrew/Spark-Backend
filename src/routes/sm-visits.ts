import { randomUUID } from "node:crypto";
import { and, asc, desc, eq, gte, inArray, isNull, lte, or, sql } from "drizzle-orm";
import { Router, type Response } from "express";
import { z } from "zod";

import { computeHiddenQuestionIds } from "../lib/conditional-visibility.js";
import { db } from "../lib/db.js";
import { logger } from "../lib/logger.js";
import {
  smAnswerOptionVersions,
  smAssignmentEvents,
  smAssignments,
  smAssignmentTimeSubmissions,
  smMarkets,
  smModules,
  smModuleVersionQuestions,
  smModuleVersions,
  smQuestionAnswerEvents,
  smQuestionAnswerFiles,
  smQuestionAnswerMatrixCells,
  smQuestionAnswerOptions,
  smQuestionAnswers,
  smQuestionLogicRules,
  smQuestionLogicRuleTargets,
  smQuestions,
  smQuestionnaireSubmissionQuestions,
  smQuestionnaireSubmissionSections,
  smQuestionnaireSubmissions,
  smQuestionnaireGlobalAssignments,
  smQuestionnaireTemplates,
  smQuestionnaireVersionModules,
  smQuestionnaireVersions,
  smQuestionVersions,
  users,
} from "../lib/schema.js";
import { supabaseAdmin } from "../lib/supabase.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { resolveSmAssignmentValues } from "../sm-planning.shared.js";
import {
  isCompleteSmVisitAnswer,
  isAnsweredSmVisitPayload,
  normalizeSmVisitAnswer,
  SmVisitAnswerValidationError,
  smVisitAnswerSchema,
  smVisitAnswerToRuleValue,
  stableSmVisitAnswer,
  type SmVisitAnswerPayload,
  type SmVisitQuestionSnapshot,
} from "../sm-visit.shared.js";

type AssignmentRow = typeof smAssignments.$inferSelect;
type DbTx = Parameters<Parameters<typeof db.transaction>[0]>[0];
type DbExecutor = typeof db | DbTx;

const assignmentIdSchema = z.string().uuid();
const startSchema = z.object({
  mode: z.enum(["timer", "manual"]),
  travelMinutes: z.number().int().min(0).max(1440).nullable().optional(),
  clientSubmissionToken: z.string().trim().min(8).max(300),
}).strict();
const discardSchema = z.object({ confirmation: z.literal("SOFT_DELETE_SM_VISIT") }).strict();
const saveAnswerSchema = z.object({
  answer: smVisitAnswerSchema,
  clientMutationToken: z.string().trim().min(8).max(300),
  expectedAnswerVersion: z.number().int().min(0),
}).strict();
const timingSchema = z.object({
  travelMinutes: z.number().int().min(0).max(1440).nullable().optional(),
  manualVisitMinutes: z.number().int().min(1).max(1440).nullable().optional(),
}).strict().refine((value) => value.travelMinutes !== undefined || value.manualVisitMinutes !== undefined, {
  message: "Mindestens ein Zeitwert ist erforderlich.",
});
const submitSchema = z.object({
  clientMutationToken: z.string().trim().min(8).max(300),
  actualMinutes: z.number().int().min(1).max(1440).optional(),
  visitStartedAt: z.string().datetime({ offset: true }).optional(),
  visitCompletedAt: z.string().datetime({ offset: true }).optional(),
}).strict().superRefine((value, context) => {
  const hasStart = value.visitStartedAt !== undefined;
  const hasEnd = value.visitCompletedAt !== undefined;
  if (hasStart !== hasEnd) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      message: "Start- und Endzeit müssen gemeinsam angegeben werden.",
      path: hasStart ? ["visitCompletedAt"] : ["visitStartedAt"],
    });
    return;
  }
  if (!value.visitStartedAt || !value.visitCompletedAt) return;
  const startedAt = new Date(value.visitStartedAt);
  const completedAt = new Date(value.visitCompletedAt);
  const elapsedMinutes = Math.round((completedAt.getTime() - startedAt.getTime()) / 60_000);
  if (elapsedMinutes < 1 || elapsedMinutes > 1440) {
    context.addIssue({
      code: z.ZodIssueCode.custom,
      message: "Die Endzeit muss nach der Startzeit liegen und höchstens 24 Stunden später sein.",
      path: ["visitCompletedAt"],
    });
  }
});
const photoInitializeSchema = z.object({ submissionQuestionId: z.string().uuid() }).strict();
const photoPresignSchema = z.object({
  answerId: z.string().uuid(),
  extension: z.string().trim().max(10).optional(),
}).strict();
const photoCommitSchema = z.object({
  answerId: z.string().uuid(),
  photos: z.array(z.object({
    storageBucket: z.literal("sm-visit-photos"),
    storagePath: z.string().trim().min(1).max(1_000),
    originalFileName: z.string().trim().max(500).optional(),
    mimeType: z.enum(["image/jpeg", "image/png", "image/webp"]),
    byteSize: z.number().int().min(1).max(15 * 1024 * 1024),
    widthPx: z.number().int().positive().max(30_000).optional(),
    heightPx: z.number().int().positive().max(30_000).optional(),
  }).strict()).min(1).max(20),
}).strict();
const photoCleanupSchema = z.object({
  answerId: z.string().uuid(),
  storageBucket: z.literal("sm-visit-photos"),
  storagePath: z.string().trim().min(1).max(1_000),
}).strict();

const SM_VISIT_PHOTO_BUCKET = "sm-visit-photos";
const SM_VISIT_PHOTO_READ_URL_TTL_SECONDS = 30 * 60;

function normalizePhotoExtension(value: string | undefined): "jpg" | "png" | "webp" {
  const normalized = (value ?? "jpg").toLowerCase().replace(/[^a-z0-9]/g, "");
  if (normalized === "jpeg" || normalized === "jpg") return "jpg";
  if (normalized === "png") return "png";
  if (normalized === "webp") return "webp";
  throw new SmVisitError(400, "sm_visit_photo_extension_invalid", "Dieses Fotoformat wird nicht unterstützt.");
}

async function storageObjectExists(bucket: string, storagePath: string): Promise<boolean> {
  const slash = storagePath.lastIndexOf("/");
  if (slash <= 0 || slash >= storagePath.length - 1) return false;
  const folder = storagePath.slice(0, slash);
  const fileName = storagePath.slice(slash + 1);
  const { data, error } = await supabaseAdmin.storage.from(bucket).list(folder, { search: fileName, limit: 10 });
  return !error && Boolean(data?.some((entry) => entry.name === fileName));
}

class SmVisitError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly code: string,
    message: string,
    public readonly details?: Record<string, unknown>,
  ) {
    super(message);
  }
}

function sendError(error: unknown, res: Response): boolean {
  if (error instanceof SmVisitAnswerValidationError) {
    res.status(400).json({ error: error.message, code: "sm_visit_answer_invalid" });
    return true;
  }
  if (!(error instanceof SmVisitError)) return false;
  res.status(error.statusCode).json({ error: error.message, code: error.code, ...(error.details ? { details: error.details } : {}) });
  return true;
}

function param(req: AuthedRequest, key: string): string {
  const value = req.params[key];
  return Array.isArray(value) ? value[0] ?? "" : value ?? "";
}

function requireAuthUser(req: AuthedRequest) {
  if (!req.authUser) throw new SmVisitError(401, "auth_required", "Anmeldung erforderlich.");
  return req.authUser;
}

async function loadOwnedAssignment(executor: DbExecutor, assignmentId: string, smUserId: string, lock = false): Promise<AssignmentRow> {
  let query = executor.select().from(smAssignments).where(and(
    eq(smAssignments.id, assignmentId),
    eq(smAssignments.isDeleted, false),
  )).limit(1);
  if (lock && "transaction" in executor === false) query = query.for("update") as typeof query;
  const [assignment] = await query;
  if (!assignment) throw new SmVisitError(404, "sm_visit_assignment_not_found", "Der Einsatz wurde nicht gefunden.");
  const effective = resolveSmAssignmentValues(assignment);
  if (effective.smUserId !== smUserId) throw new SmVisitError(403, "sm_visit_assignment_forbidden", "Dieser Einsatz gehört zu einem anderen Shelf Merchandiser.");
  return assignment;
}

async function loadContext(executor: DbExecutor, assignment: AssignmentRow, smUserId: string) {
  const effective = resolveSmAssignmentValues(assignment);
  const [[user], [market]] = await Promise.all([
    executor.select({
      id: users.id,
      firstName: users.firstName,
      lastName: users.lastName,
      travelTimeEnabled: users.travelTimeEnabled,
    }).from(users).where(and(eq(users.id, smUserId), eq(users.role, "sm"), eq(users.isActive, true), isNull(users.deletedAt))).limit(1),
    executor.select().from(smMarkets).where(and(eq(smMarkets.id, effective.smMarketId), eq(smMarkets.isDeleted, false))).limit(1),
  ]);
  if (!user) throw new SmVisitError(403, "sm_visit_sm_inactive", "Der Shelf-Merchandiser-Zugang ist nicht aktiv.");
  if (!market) throw new SmVisitError(409, "sm_visit_market_missing", "Der zugeordnete SM-Markt wurde nicht gefunden.");
  return { effective, user, market };
}

async function effectivePublishedVersions(executor: DbExecutor, workDate: string) {
  return executor.select({
    id: smQuestionnaireVersions.id,
    questionnaireTemplateId: smQuestionnaireVersions.questionnaireTemplateId,
    versionNumber: smQuestionnaireVersions.versionNumber,
    name: smQuestionnaireVersions.name,
    description: smQuestionnaireVersions.description,
    oncePerMarket: smQuestionnaireVersions.oncePerMarket,
    timezone: smQuestionnaireVersions.timezone,
  }).from(smQuestionnaireVersions)
    .innerJoin(smQuestionnaireTemplates, eq(smQuestionnaireTemplates.id, smQuestionnaireVersions.questionnaireTemplateId))
    .where(and(
      eq(smQuestionnaireVersions.status, "published"),
      eq(smQuestionnaireVersions.isDeleted, false),
      eq(smQuestionnaireTemplates.status, "active"),
      eq(smQuestionnaireTemplates.isDeleted, false),
      or(isNull(smQuestionnaireVersions.effectiveFrom), lte(smQuestionnaireVersions.effectiveFrom, workDate)),
      or(isNull(smQuestionnaireVersions.effectiveTo), gte(smQuestionnaireVersions.effectiveTo, workDate)),
    ))
    .orderBy(desc(smQuestionnaireVersions.versionNumber));
}

async function effectiveGlobalQuestionnaireVersion(executor: DbExecutor, workDate: string) {
  const [globalAssignment] = await executor.select({
    questionnaireTemplateId: smQuestionnaireGlobalAssignments.questionnaireTemplateId,
  }).from(smQuestionnaireGlobalAssignments).where(and(
    eq(smQuestionnaireGlobalAssignments.isDeleted, false),
    isNull(smQuestionnaireGlobalAssignments.supersededAt),
  )).limit(1);
  if (!globalAssignment) return null;

  const candidates = await effectivePublishedVersions(executor, workDate);
  const selected = candidates.find((candidate) => candidate.questionnaireTemplateId === globalAssignment.questionnaireTemplateId);
  if (!selected) {
    throw new SmVisitError(409, "sm_visit_global_questionnaire_unavailable", "Der zentral ausgewählte SM-Fragebogen ist für diesen Einsatztag nicht aktiv oder veröffentlicht.");
  }
  return selected;
}

async function resolveQuestionnaireVersion(tx: DbTx, assignment: AssignmentRow) {
  const effective = resolveSmAssignmentValues(assignment);
  const globalSelection = await effectiveGlobalQuestionnaireVersion(tx, effective.workDate);
  if (globalSelection) {
    if (assignment.questionnaireVersionId !== globalSelection.id) {
      await tx.update(smAssignments).set({ questionnaireVersionId: globalSelection.id, updatedAt: new Date() }).where(eq(smAssignments.id, assignment.id));
    }
    return globalSelection;
  }
  const candidates = await effectivePublishedVersions(tx, effective.workDate);
  if (assignment.questionnaireVersionId) {
    const selected = candidates.find((candidate) => candidate.id === assignment.questionnaireVersionId);
    if (!selected) throw new SmVisitError(409, "sm_visit_questionnaire_unavailable", "Der für diesen Einsatz geplante Fragebogen ist nicht veröffentlicht oder für dieses Datum nicht gültig.");
    return selected;
  }
  if (candidates.length === 0) throw new SmVisitError(409, "sm_visit_questionnaire_missing", "Für diesen Einsatz ist noch kein veröffentlichter SM-Fragebogen verfügbar.");
  if (candidates.length > 1) throw new SmVisitError(409, "sm_visit_questionnaire_ambiguous", "Für diesen Einsatz sind mehrere Fragebögen verfügbar. Bitte ordne in der Verplanung einen Fragebogen zu.");
  const selected = candidates[0]!;
  await tx.update(smAssignments).set({ questionnaireVersionId: selected.id, updatedAt: new Date() }).where(eq(smAssignments.id, assignment.id));
  return selected;
}

async function createSubmissionGraph(tx: DbTx, input: {
  submissionId: string;
  questionnaireVersionId: string;
}) {
  const moduleLinks = await tx.select({
    orderIndex: smQuestionnaireVersionModules.orderIndex,
    moduleVersionId: smModuleVersions.id,
    moduleRootId: smModules.id,
    moduleCode: smModules.stableCode,
    moduleName: smModuleVersions.name,
    moduleDescription: smModuleVersions.description,
  }).from(smQuestionnaireVersionModules)
    .innerJoin(smModuleVersions, eq(smModuleVersions.id, smQuestionnaireVersionModules.moduleVersionId))
    .innerJoin(smModules, eq(smModules.id, smModuleVersions.moduleId))
    .where(and(
      eq(smQuestionnaireVersionModules.questionnaireVersionId, input.questionnaireVersionId),
      eq(smQuestionnaireVersionModules.isDeleted, false),
      eq(smModuleVersions.status, "published"),
      eq(smModuleVersions.isDeleted, false),
      eq(smModules.isDeleted, false),
    )).orderBy(asc(smQuestionnaireVersionModules.orderIndex));
  if (moduleLinks.length === 0) throw new SmVisitError(409, "sm_visit_questionnaire_empty", "Der veröffentlichte Fragebogen enthält keine Module.");

  const sectionRows = moduleLinks.map((module) => ({
    id: randomUUID(),
    submissionId: input.submissionId,
    moduleVersionId: module.moduleVersionId,
    moduleCodeSnapshot: module.moduleCode,
    moduleNameSnapshot: module.moduleName,
    moduleDescriptionSnapshot: module.moduleDescription,
    orderIndex: module.orderIndex,
  }));
  await tx.insert(smQuestionnaireSubmissionSections).values(sectionRows);
  const sectionByModule = new Map(sectionRows.map((row) => [row.moduleVersionId, row]));

  const moduleVersionIds = moduleLinks.map((module) => module.moduleVersionId);
  const questionLinks = await tx.select({
    moduleVersionId: smModuleVersionQuestions.moduleVersionId,
    orderIndex: smModuleVersionQuestions.orderIndex,
    questionVersionId: smQuestionVersions.id,
    questionRootId: smQuestions.id,
    questionCode: smQuestions.stableCode,
    questionType: smQuestionVersions.questionType,
    questionText: smQuestionVersions.questionText,
    required: smQuestionVersions.required,
    metricRole: smQuestionVersions.metricRole,
    oosCategory: smQuestionVersions.oosCategory,
    maxPoints: smQuestionVersions.maxPoints,
    config: smQuestionVersions.config,
    metricConfig: smQuestionVersions.metricConfig,
  }).from(smModuleVersionQuestions)
    .innerJoin(smQuestionVersions, eq(smQuestionVersions.id, smModuleVersionQuestions.questionVersionId))
    .innerJoin(smQuestions, eq(smQuestions.id, smQuestionVersions.questionId))
    .where(and(
      inArray(smModuleVersionQuestions.moduleVersionId, moduleVersionIds),
      eq(smModuleVersionQuestions.isDeleted, false),
      eq(smQuestionVersions.status, "published"),
      eq(smQuestionVersions.isDeleted, false),
      eq(smQuestions.isDeleted, false),
    )).orderBy(asc(smModuleVersionQuestions.orderIndex));
  if (questionLinks.length === 0) throw new SmVisitError(409, "sm_visit_questionnaire_no_questions", "Der veröffentlichte Fragebogen enthält keine Fragen.");

  const questionVersionIds = questionLinks.map((question) => question.questionVersionId);
  const optionRows = await tx.select().from(smAnswerOptionVersions).where(and(
    inArray(smAnswerOptionVersions.questionVersionId, questionVersionIds),
    eq(smAnswerOptionVersions.isDeleted, false),
  )).orderBy(asc(smAnswerOptionVersions.orderIndex));
  const optionsByQuestion = new Map<string, typeof optionRows>();
  for (const option of optionRows) optionsByQuestion.set(option.questionVersionId, [...(optionsByQuestion.get(option.questionVersionId) ?? []), option]);

  const rules = await tx.select().from(smQuestionLogicRules).where(and(
    inArray(smQuestionLogicRules.groupCode, questionVersionIds.map((id) => `ui_owner_version:${id}`)),
    eq(smQuestionLogicRules.isDeleted, false),
  )).orderBy(asc(smQuestionLogicRules.orderIndex));
  const ruleIds = rules.map((rule) => rule.id);
  const targets = ruleIds.length ? await tx.select().from(smQuestionLogicRuleTargets).where(and(
    inArray(smQuestionLogicRuleTargets.ruleId, ruleIds),
    eq(smQuestionLogicRuleTargets.isDeleted, false),
  )).orderBy(asc(smQuestionLogicRuleTargets.orderIndex)) : [];
  const targetsByRule = new Map<string, typeof targets>();
  for (const target of targets) targetsByRule.set(target.ruleId, [...(targetsByRule.get(target.ruleId) ?? []), target]);
  const codeByVersion = new Map(questionLinks.map((question) => [question.questionVersionId, question.questionCode]));
  const rulesByOwnerVersion = new Map<string, Array<Record<string, unknown>>>();
  for (const rule of rules) {
    const ownerVersionId = rule.groupCode.slice("ui_owner_version:".length);
    const triggerQuestionId = codeByVersion.get(rule.triggerQuestionVersionId);
    if (!triggerQuestionId) continue;
    const targetQuestionIds = (targetsByRule.get(rule.id) ?? []).map((target) => codeByVersion.get(target.targetQuestionVersionId)).filter((value): value is string => Boolean(value));
    if (!targetQuestionIds.length) continue;
    rulesByOwnerVersion.set(ownerVersionId, [...(rulesByOwnerVersion.get(ownerVersionId) ?? []), {
      id: rule.id,
      triggerQuestionId,
      operator: rule.operator,
      triggerValue: rule.triggerValue == null ? "" : String(rule.triggerValue),
      triggerValueMax: rule.triggerValueMax == null ? "" : String(rule.triggerValueMax),
      action: rule.action,
      targetQuestionIds,
    }]);
  }

  const submissionQuestions = questionLinks.map((question) => {
    const section = sectionByModule.get(question.moduleVersionId);
    if (!section) throw new SmVisitError(500, "sm_visit_snapshot_failed", "Der Fragebogen konnte nicht vorbereitet werden.");
    const options = optionsByQuestion.get(question.questionVersionId) ?? [];
    return {
      id: randomUUID(),
      submissionId: input.submissionId,
      submissionSectionId: section.id,
      questionVersionId: question.questionVersionId,
      questionCodeSnapshot: question.questionCode,
      questionTypeSnapshot: question.questionType,
      questionTextSnapshot: question.questionText,
      requiredSnapshot: question.required,
      metricRoleSnapshot: question.metricRole,
      oosCategorySnapshot: question.oosCategory,
      maxPointsSnapshot: question.maxPoints,
      configSnapshot: question.config,
      metricConfigSnapshot: question.metricConfig,
      answerOptionsSnapshot: options.map((option) => ({
        id: option.id,
        code: option.stableCode,
        label: option.label,
        earnedPoints: option.earnedPoints,
        possiblePoints: option.possiblePoints,
        metricOutcomeCode: option.metricOutcomeCode,
        marksNotApplicable: option.marksNotApplicable,
        countsInDenominator: option.countsInDenominator,
        orderIndex: option.orderIndex,
      })),
      logicRulesSnapshot: rulesByOwnerVersion.get(question.questionVersionId) ?? [],
      orderIndex: question.orderIndex,
    };
  });
  await tx.insert(smQuestionnaireSubmissionQuestions).values(submissionQuestions);
  await tx.update(smQuestionnaireSubmissions).set({ resolvedQuestionCount: submissionQuestions.length }).where(eq(smQuestionnaireSubmissions.id, input.submissionId));
}

function publicAssignment(assignment: AssignmentRow, context: Awaited<ReturnType<typeof loadContext>>) {
  return {
    id: assignment.id,
    status: assignment.status,
    workDate: context.effective.workDate,
    plannedMinutes: context.effective.plannedMinutes,
    market: {
      id: context.market.id,
      name: context.market.name,
      internalId: context.market.internalMarketId ?? context.effective.marketInternalId,
      address: context.market.address,
      postalCode: context.market.postalCode,
      city: context.market.city,
      region: context.market.region,
    },
  };
}

function optionSnapshot(value: unknown): Array<{ code: string; label: string; marksNotApplicable?: boolean }> {
  if (!Array.isArray(value)) return [];
  return value.flatMap((entry) => {
    if (!entry || typeof entry !== "object") return [];
    const row = entry as Record<string, unknown>;
    return typeof row.code === "string" && typeof row.label === "string"
      ? [{ code: row.code, label: row.label, ...(typeof row.marksNotApplicable === "boolean" ? { marksNotApplicable: row.marksNotApplicable } : {}) }]
      : [];
  });
}

async function loadVisitPayload(assignment: AssignmentRow, smUserId: string) {
  const context = await loadContext(db, assignment, smUserId);
  const [submission] = await db.select().from(smQuestionnaireSubmissions).where(and(
    eq(smQuestionnaireSubmissions.assignmentId, assignment.id),
    eq(smQuestionnaireSubmissions.isDeleted, false),
    eq(smQuestionnaireSubmissions.isCurrent, true),
  )).limit(1);
  if (!submission) {
    const globalSelection = await effectiveGlobalQuestionnaireVersion(db, context.effective.workDate);
    const publishedCandidates = globalSelection ? [globalSelection] : await effectivePublishedVersions(db, context.effective.workDate);
    const candidates = globalSelection
      ? publishedCandidates
      : assignment.questionnaireVersionId
        ? publishedCandidates.filter((candidate) => candidate.id === assignment.questionnaireVersionId)
        : publishedCandidates;
    return {
      assignment: publicAssignment(assignment, context),
      profile: { name: `${context.user.firstName} ${context.user.lastName}`.trim(), travelTimeEnabled: context.user.travelTimeEnabled },
      questionnaireAvailability: { count: candidates.length, names: candidates.map((candidate) => candidate.name) },
      submission: null,
      sections: [],
      answers: {},
      answerVersions: {},
      photoFiles: {},
    };
  }

  const [timeSubmission] = submission.status === "submitted"
    ? await db.select({ actualMinutes: smAssignmentTimeSubmissions.actualMinutes })
      .from(smAssignmentTimeSubmissions)
      .where(and(
        eq(smAssignmentTimeSubmissions.assignmentId, assignment.id),
        eq(smAssignmentTimeSubmissions.isDeleted, false),
        eq(smAssignmentTimeSubmissions.isCurrent, true),
      ))
      .limit(1)
    : [];

  const sections = await db.select().from(smQuestionnaireSubmissionSections).where(and(
    eq(smQuestionnaireSubmissionSections.submissionId, submission.id),
    eq(smQuestionnaireSubmissionSections.isDeleted, false),
  )).orderBy(asc(smQuestionnaireSubmissionSections.orderIndex));
  const questions = await db.select().from(smQuestionnaireSubmissionQuestions).where(and(
    eq(smQuestionnaireSubmissionQuestions.submissionId, submission.id),
    eq(smQuestionnaireSubmissionQuestions.isDeleted, false),
  )).orderBy(asc(smQuestionnaireSubmissionQuestions.orderIndex));
  const answers = await db.select().from(smQuestionAnswers).where(and(
    eq(smQuestionAnswers.submissionId, submission.id),
    eq(smQuestionAnswers.isDeleted, false),
    eq(smQuestionAnswers.isCurrent, true),
  ));
  const answerByQuestion = new Map(answers.map((answer) => [answer.submissionQuestionId, answer]));
  const photoRows = answers.length ? await db.select().from(smQuestionAnswerFiles).where(and(
    inArray(smQuestionAnswerFiles.answerId, answers.map((answer) => answer.id)),
    eq(smQuestionAnswerFiles.isDeleted, false),
  )).orderBy(asc(smQuestionAnswerFiles.uploadedAt)) : [];
  const questionIdByAnswerId = new Map(answers.map((answer) => [answer.id, answer.submissionQuestionId]));
  const signedPhotoRows = await Promise.all(photoRows.map(async (photo) => {
    const { data, error } = await supabaseAdmin.storage
      .from(photo.storageBucket)
      .createSignedUrl(photo.storagePath, SM_VISIT_PHOTO_READ_URL_TTL_SECONDS);
    return {
      id: photo.id,
      questionId: questionIdByAnswerId.get(photo.answerId) ?? null,
      fileName: photo.originalFileName,
      mimeType: photo.mimeType,
      byteSize: photo.byteSize,
      signedUrl: error ? null : data.signedUrl,
    };
  }));
  const photoFilesByQuestion = new Map<string, typeof signedPhotoRows>();
  for (const photo of signedPhotoRows) {
    if (!photo.questionId) continue;
    photoFilesByQuestion.set(photo.questionId, [...(photoFilesByQuestion.get(photo.questionId) ?? []), photo]);
  }
  const questionsBySection = new Map<string, typeof questions>();
  for (const question of questions) questionsBySection.set(question.submissionSectionId, [...(questionsBySection.get(question.submissionSectionId) ?? []), question]);

  return {
    assignment: publicAssignment(assignment, context),
    profile: { name: `${context.user.firstName} ${context.user.lastName}`.trim(), travelTimeEnabled: context.user.travelTimeEnabled },
    questionnaireAvailability: { count: 1, names: [submission.questionnaireNameSnapshot] },
    submission: {
      id: submission.id,
      status: submission.status,
      questionnaireName: submission.questionnaireNameSnapshot,
      questionnaireVersion: submission.questionnaireVersionSnapshot,
      visitTimeMode: submission.visitTimeMode,
      travelMinutes: submission.travelMinutes,
      manualVisitMinutes: submission.manualVisitMinutes,
      actualMinutes: timeSubmission?.actualMinutes ?? null,
      visitStartedAt: submission.visitStartedAt?.toISOString() ?? null,
      visitCompletedAt: submission.visitCompletedAt?.toISOString() ?? null,
      submittedAt: submission.submittedAt?.toISOString() ?? null,
      lastSavedAt: submission.lastSavedAt.toISOString(),
      answeredQuestionCount: submission.answeredQuestionCount,
      resolvedQuestionCount: submission.resolvedQuestionCount,
    },
    sections: sections.map((section) => ({
      id: section.id,
      code: section.moduleCodeSnapshot,
      name: section.moduleNameSnapshot,
      description: section.moduleDescriptionSnapshot,
      questions: (questionsBySection.get(section.id) ?? []).map((question) => ({
        id: question.id,
        questionCode: question.questionCodeSnapshot,
        type: question.questionTypeSnapshot,
        text: question.questionTextSnapshot,
        required: question.requiredSnapshot,
        config: question.configSnapshot,
        options: optionSnapshot(question.answerOptionsSnapshot),
        rules: question.logicRulesSnapshot,
        applicable: question.isApplicable,
        applicabilityReason: question.applicabilityReason,
      })),
    })),
    answers: Object.fromEntries(questions.map((question) => {
      const answer = answerByQuestion.get(question.id);
      return [question.id, answer?.valueJson ?? null];
    })),
    answerVersions: Object.fromEntries(questions.map((question) => [question.id, answerByQuestion.get(question.id)?.answerVersion ?? 0])),
    photoFiles: Object.fromEntries(questions.map((question) => [question.id, photoFilesByQuestion.get(question.id) ?? []])),
  };
}

async function recomputeApplicability(tx: DbTx, submissionId: string, actorUserId: string) {
  const questions = await tx.select().from(smQuestionnaireSubmissionQuestions).where(and(
    eq(smQuestionnaireSubmissionQuestions.submissionId, submissionId),
    eq(smQuestionnaireSubmissionQuestions.isDeleted, false),
  ));
  const answers = await tx.select().from(smQuestionAnswers).where(and(
    eq(smQuestionAnswers.submissionId, submissionId),
    eq(smQuestionAnswers.isDeleted, false),
    eq(smQuestionAnswers.isCurrent, true),
  ));
  const answerByQuestion = new Map(answers.map((answer) => [answer.submissionQuestionId, answer]));
  const ruleValues = new Map<string, string | string[] | undefined>();
  for (const question of questions) {
    const answer = answerByQuestion.get(question.id)?.valueJson as SmVisitAnswerPayload | null | undefined;
    ruleValues.set(question.id, smVisitAnswerToRuleValue(answer, optionSnapshot(question.answerOptionsSnapshot)));
  }
  const hidden = computeHiddenQuestionIds(questions.map((question) => ({
    id: question.id,
    questionId: question.questionCodeSnapshot,
    rules: question.logicRulesSnapshot,
  })), ruleValues);

  const now = new Date();
  for (const question of questions) {
    const shouldApply = !hidden.has(question.id);
    if (question.isApplicable !== shouldApply) {
      await tx.update(smQuestionnaireSubmissionQuestions).set({
        isApplicable: shouldApply,
        applicabilityReason: shouldApply ? null : "hidden_by_rule",
        updatedAt: now,
      }).where(eq(smQuestionnaireSubmissionQuestions.id, question.id));
    }
    if (!shouldApply) {
      const current = answerByQuestion.get(question.id);
      if (current) {
        await tx.update(smQuestionAnswers).set({
          isCurrent: false,
          answerState: "invalidated",
          invalidatedAt: now,
          invalidatedByUserId: actorUserId,
          invalidationReason: "hidden_by_rule",
          updatedAt: now,
        }).where(eq(smQuestionAnswers.id, current.id));
        await tx.insert(smQuestionAnswerEvents).values({
          answerId: current.id,
          submissionId,
          eventType: "state_change",
          answerVersion: current.answerVersion,
          payload: { from: current.answerState, to: "invalidated", reason: "hidden_by_rule" },
          actorUserId,
        });
        answerByQuestion.delete(question.id);
      }
    }
  }
  const answeredCount = [...answerByQuestion.values()].filter((answer) => answer.answerState === "answered").length;
  await tx.update(smQuestionnaireSubmissions).set({ answeredQuestionCount: answeredCount, lastSavedAt: now, updatedAt: now }).where(eq(smQuestionnaireSubmissions.id, submissionId));
  return { questions, hidden, answeredCount };
}

export const smVisitsRouter = Router();
smVisitsRouter.use(requireAuth(["sm"]));

smVisitsRouter.get("/:assignmentId", async (req: AuthedRequest, res, next) => {
  try {
    const actor = requireAuthUser(req);
    const assignmentId = assignmentIdSchema.parse(param(req, "assignmentId"));
    const assignment = await loadOwnedAssignment(db, assignmentId, actor.appUserId);
    res.json(await loadVisitPayload(assignment, actor.appUserId));
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Ungültige Einsatz-ID.", code: "sm_visit_assignment_id_invalid" });
    if (!sendError(error, res)) next(error);
  }
});

smVisitsRouter.delete("/:assignmentId", async (req: AuthedRequest, res, next) => {
  try {
    const actor = requireAuthUser(req);
    const assignmentId = assignmentIdSchema.parse(param(req, "assignmentId"));
    discardSchema.parse(req.body);
    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_visit:${assignmentId}`}, 0))`);
      const assignment = await loadOwnedAssignment(tx, assignmentId, actor.appUserId, true);
      const [submission] = await tx.select().from(smQuestionnaireSubmissions).where(and(
        eq(smQuestionnaireSubmissions.assignmentId, assignmentId),
        eq(smQuestionnaireSubmissions.smUserId, actor.appUserId),
        eq(smQuestionnaireSubmissions.isDeleted, false),
        eq(smQuestionnaireSubmissions.isCurrent, true),
      )).limit(1).for("update");
      if (!submission) throw new SmVisitError(404, "sm_visit_draft_missing", "Der laufende Fragebogen wurde nicht gefunden.");
      if (submission.status !== "draft" || submission.submittedAt) {
        throw new SmVisitError(409, "sm_visit_discard_not_draft", "Nur ein laufender, nicht abgeschlossener Fragebogen kann verworfen werden.");
      }
      if (assignment.status !== "in_progress") {
        throw new SmVisitError(409, "sm_visit_assignment_not_in_progress", "Der Einsatz ist nicht mehr in Arbeit.");
      }

      const answerRows = await tx.select({ id: smQuestionAnswers.id }).from(smQuestionAnswers).where(eq(smQuestionAnswers.submissionId, submission.id));
      const answerIds = answerRows.map((row) => row.id);
      const photoRows = answerIds.length > 0
        ? await tx.select({ bucket: smQuestionAnswerFiles.storageBucket, path: smQuestionAnswerFiles.storagePath })
          .from(smQuestionAnswerFiles)
          .where(and(inArray(smQuestionAnswerFiles.answerId, answerIds), eq(smQuestionAnswerFiles.isDeleted, false)))
        : [];
      const [startEvent] = await tx.select({ beforeState: smAssignmentEvents.beforeState })
        .from(smAssignmentEvents)
        .where(and(eq(smAssignmentEvents.assignmentId, assignmentId), eq(smAssignmentEvents.reason, "SM Marktbesuch gestartet")))
        .orderBy(desc(smAssignmentEvents.createdAt))
        .limit(1);
      const previousStatusValue = startEvent?.beforeState?.status;
      const restoredStatus = previousStatusValue === "planned" || previousStatusValue === "confirmed" || previousStatusValue === "open"
        ? previousStatusValue
        : "planned";
      const previousStartedAtValue = startEvent?.beforeState?.startedAt;
      const parsedPreviousStartedAt = typeof previousStartedAtValue === "string" ? new Date(previousStartedAtValue) : null;
      const restoredStartedAt = parsedPreviousStartedAt && Number.isFinite(parsedPreviousStartedAt.getTime()) ? parsedPreviousStartedAt : null;
      const now = new Date();

      if (answerIds.length > 0) {
        await tx.update(smQuestionAnswerOptions).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(and(inArray(smQuestionAnswerOptions.answerId, answerIds), eq(smQuestionAnswerOptions.isDeleted, false)));
        await tx.update(smQuestionAnswerMatrixCells).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(and(inArray(smQuestionAnswerMatrixCells.answerId, answerIds), eq(smQuestionAnswerMatrixCells.isDeleted, false)));
        await tx.update(smQuestionAnswerFiles).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(and(inArray(smQuestionAnswerFiles.answerId, answerIds), eq(smQuestionAnswerFiles.isDeleted, false)));
        await tx.update(smQuestionAnswers).set({ isCurrent: false, isDeleted: true, deletedAt: now, updatedAt: now }).where(and(eq(smQuestionAnswers.submissionId, submission.id), eq(smQuestionAnswers.isDeleted, false)));
      }
      await tx.update(smQuestionnaireSubmissionQuestions).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(and(eq(smQuestionnaireSubmissionQuestions.submissionId, submission.id), eq(smQuestionnaireSubmissionQuestions.isDeleted, false)));
      await tx.update(smQuestionnaireSubmissionSections).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(and(eq(smQuestionnaireSubmissionSections.submissionId, submission.id), eq(smQuestionnaireSubmissionSections.isDeleted, false)));
      await tx.update(smQuestionnaireSubmissions).set({
        status: "cancelled",
        cancellationReason: "Vom Shelf Merchandiser verworfen",
        cancelledAt: now,
        isCurrent: false,
        isDeleted: true,
        deletedAt: now,
        updatedAt: now,
        lastSavedAt: now,
      }).where(eq(smQuestionnaireSubmissions.id, submission.id));
      const [restoredAssignment] = await tx.update(smAssignments).set({
        status: restoredStatus,
        startedAt: restoredStartedAt,
        completedAt: null,
        updatedByUserId: actor.appUserId,
        updatedAt: now,
      }).where(eq(smAssignments.id, assignmentId)).returning();
      if (!restoredAssignment) throw new SmVisitError(409, "sm_visit_assignment_restore_failed", "Der Einsatz konnte nicht zurückgesetzt werden.");
      await tx.insert(smAssignmentEvents).values({
        assignmentId,
        seriesId: assignment.seriesId,
        eventType: "updated",
        actorUserId: actor.appUserId,
        reason: "SM Marktbesuch verworfen",
        beforeState: { status: assignment.status, startedAt: assignment.startedAt?.toISOString() ?? null, submissionId: submission.id },
        afterState: { status: restoredStatus, startedAt: restoredStartedAt?.toISOString() ?? null, submissionId: null },
      });
      return { photoRows, restoredStatus };
    });

    const pathsByBucket = new Map<string, string[]>();
    for (const photo of result.photoRows) pathsByBucket.set(photo.bucket, [...(pathsByBucket.get(photo.bucket) ?? []), photo.path]);
    for (const [bucket, paths] of pathsByBucket.entries()) {
      try {
        const { error } = await supabaseAdmin.storage.from(bucket).remove(Array.from(new Set(paths)));
        if (error) logger.warn("sm_visit_discard_photo_cleanup_failed", { assignmentId, bucket, error: error.message });
      } catch (cleanupError) {
        logger.warn("sm_visit_discard_photo_cleanup_failed", { assignmentId, bucket, error: cleanupError instanceof Error ? cleanupError.message : String(cleanupError) });
      }
    }
    res.json({ ok: true, assignmentId, status: result.restoredStatus });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Die Bestätigung zum Verwerfen ist ungültig.", code: "sm_visit_discard_confirmation_invalid" });
    if (!sendError(error, res)) next(error);
  }
});

smVisitsRouter.post("/:assignmentId/start", async (req: AuthedRequest, res, next) => {
  try {
    const actor = requireAuthUser(req);
    const assignmentId = assignmentIdSchema.parse(param(req, "assignmentId"));
    const input = startSchema.parse(req.body);
    await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_visit:${assignmentId}`}, 0))`);
      const assignment = await loadOwnedAssignment(tx, assignmentId, actor.appUserId, true);
      const context = await loadContext(tx, assignment, actor.appUserId);
      const [existing] = await tx.select().from(smQuestionnaireSubmissions).where(and(
        eq(smQuestionnaireSubmissions.assignmentId, assignmentId),
        eq(smQuestionnaireSubmissions.isDeleted, false),
        eq(smQuestionnaireSubmissions.isCurrent, true),
      )).limit(1).for("update");
      if (existing) return;
      if (["cancelled", "missed", "completed"].includes(assignment.status)) {
        throw new SmVisitError(409, "sm_visit_assignment_locked", "Dieser Einsatz kann nicht mehr gestartet werden.");
      }
      const version = await resolveQuestionnaireVersion(tx, assignment);
      if (version.oncePerMarket) {
        const [completed] = await tx.select({ id: smQuestionnaireSubmissions.id }).from(smQuestionnaireSubmissions).where(and(
          eq(smQuestionnaireSubmissions.questionnaireTemplateId, version.questionnaireTemplateId),
          eq(smQuestionnaireSubmissions.smMarketId, context.market.id),
          eq(smQuestionnaireSubmissions.status, "submitted"),
          eq(smQuestionnaireSubmissions.isDeleted, false),
          eq(smQuestionnaireSubmissions.isCurrent, true),
        )).limit(1);
        if (completed) throw new SmVisitError(409, "sm_visit_once_per_market_completed", "Dieser einmalige Fragebogen wurde für den Markt bereits abgeschlossen.");
      }
      const now = new Date();
      const submissionId = randomUUID();
      await tx.insert(smQuestionnaireSubmissions).values({
        id: submissionId,
        assignmentId,
        questionnaireTemplateId: version.questionnaireTemplateId,
        questionnaireVersionId: version.id,
        smUserId: actor.appUserId,
        smMarketId: context.market.id,
        clientSubmissionToken: input.clientSubmissionToken,
        timezone: version.timezone,
        oncePerMarketSnapshot: version.oncePerMarket,
        questionnaireNameSnapshot: version.name,
        questionnaireVersionSnapshot: version.versionNumber,
        smNameSnapshot: `${context.user.firstName} ${context.user.lastName}`.trim(),
        marketNameSnapshot: context.market.name,
        marketAddressSnapshot: context.market.address,
        marketPostalCodeSnapshot: context.market.postalCode,
        marketCitySnapshot: context.market.city,
        visitTimeMode: input.mode,
        travelMinutes: context.user.travelTimeEnabled ? input.travelMinutes ?? null : null,
        manualVisitMinutes: null,
        visitStartedAt: input.mode === "timer" ? now : null,
        lastSavedAt: now,
      });
      await createSubmissionGraph(tx, { submissionId, questionnaireVersionId: version.id });
      const [updated] = await tx.update(smAssignments).set({
        status: "in_progress",
        startedAt: assignment.startedAt ?? now,
        updatedByUserId: actor.appUserId,
        updatedAt: now,
      }).where(eq(smAssignments.id, assignmentId)).returning();
      if (updated) await tx.insert(smAssignmentEvents).values({
        assignmentId,
        seriesId: assignment.seriesId,
        eventType: "updated",
        actorUserId: actor.appUserId,
        reason: "SM Marktbesuch gestartet",
        beforeState: { status: assignment.status, startedAt: assignment.startedAt?.toISOString() ?? null },
        afterState: { status: updated.status, startedAt: updated.startedAt?.toISOString() ?? null },
      });
    });
    const assignment = await loadOwnedAssignment(db, assignmentId, actor.appUserId);
    res.status(200).json(await loadVisitPayload(assignment, actor.appUserId));
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Die Startdaten sind ungültig.", code: "sm_visit_start_invalid", details: { issues: error.issues } });
    if (!sendError(error, res)) next(error);
  }
});

smVisitsRouter.post("/:assignmentId/photos/initialize", async (req: AuthedRequest, res, next) => {
  try {
    const actor = requireAuthUser(req);
    const assignmentId = assignmentIdSchema.parse(param(req, "assignmentId"));
    const input = photoInitializeSchema.parse(req.body);
    const answerId = await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_visit:${assignmentId}:${input.submissionQuestionId}`}, 0))`);
      const assignment = await loadOwnedAssignment(tx, assignmentId, actor.appUserId, true);
      if (assignment.status !== "in_progress") throw new SmVisitError(409, "sm_visit_not_in_progress", "Der Einsatz ist nicht in Arbeit.");
      const [submission] = await tx.select().from(smQuestionnaireSubmissions).where(and(
        eq(smQuestionnaireSubmissions.assignmentId, assignmentId),
        eq(smQuestionnaireSubmissions.smUserId, actor.appUserId),
        eq(smQuestionnaireSubmissions.status, "draft"),
        eq(smQuestionnaireSubmissions.isDeleted, false),
        eq(smQuestionnaireSubmissions.isCurrent, true),
      )).limit(1).for("update");
      if (!submission) throw new SmVisitError(409, "sm_visit_draft_missing", "Der laufende Fragebogen wurde nicht gefunden.");
      const [question] = await tx.select().from(smQuestionnaireSubmissionQuestions).where(and(
        eq(smQuestionnaireSubmissionQuestions.id, input.submissionQuestionId),
        eq(smQuestionnaireSubmissionQuestions.submissionId, submission.id),
        eq(smQuestionnaireSubmissionQuestions.questionTypeSnapshot, "photo"),
        eq(smQuestionnaireSubmissionQuestions.isApplicable, true),
        eq(smQuestionnaireSubmissionQuestions.isDeleted, false),
      )).limit(1);
      if (!question) throw new SmVisitError(404, "sm_visit_photo_question_not_found", "Die Foto-Frage wurde nicht gefunden.");
      const [current] = await tx.select().from(smQuestionAnswers).where(and(
        eq(smQuestionAnswers.submissionQuestionId, question.id),
        eq(smQuestionAnswers.isDeleted, false),
        eq(smQuestionAnswers.isCurrent, true),
      )).limit(1).for("update");
      if (current) return current.id;
      const id = randomUUID();
      await tx.insert(smQuestionAnswers).values({
        id,
        submissionId: submission.id,
        submissionQuestionId: question.id,
        answerVersion: 1,
        isCurrent: true,
        answerState: "unanswered",
        valueJson: { kind: "photo", fileIds: [] },
        answeredByUserId: actor.appUserId,
      });
      await tx.insert(smQuestionAnswerEvents).values({
        answerId: id,
        submissionId: submission.id,
        eventType: "clear",
        answerVersion: 1,
        payload: { initializedForUpload: true },
        actorUserId: actor.appUserId,
      });
      return id;
    });
    res.json({ answerId });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Die Foto-Antwort ist ungültig.", code: "sm_visit_photo_initialize_invalid" });
    if (!sendError(error, res)) next(error);
  }
});

smVisitsRouter.post("/:assignmentId/photos/presign", async (req: AuthedRequest, res, next) => {
  try {
    const actor = requireAuthUser(req);
    const assignmentId = assignmentIdSchema.parse(param(req, "assignmentId"));
    const input = photoPresignSchema.parse(req.body);
    const assignment = await loadOwnedAssignment(db, assignmentId, actor.appUserId);
    if (assignment.status !== "in_progress") throw new SmVisitError(409, "sm_visit_not_in_progress", "Der Einsatz ist nicht in Arbeit.");
    const [answer] = await db.select({ id: smQuestionAnswers.id, submissionId: smQuestionAnswers.submissionId }).from(smQuestionAnswers)
      .innerJoin(smQuestionnaireSubmissions, eq(smQuestionnaireSubmissions.id, smQuestionAnswers.submissionId))
      .innerJoin(smQuestionnaireSubmissionQuestions, eq(smQuestionnaireSubmissionQuestions.id, smQuestionAnswers.submissionQuestionId))
      .where(and(
        eq(smQuestionAnswers.id, input.answerId),
        eq(smQuestionAnswers.isDeleted, false),
        eq(smQuestionAnswers.isCurrent, true),
        eq(smQuestionnaireSubmissions.assignmentId, assignmentId),
        eq(smQuestionnaireSubmissions.smUserId, actor.appUserId),
        eq(smQuestionnaireSubmissions.status, "draft"),
        eq(smQuestionnaireSubmissions.isDeleted, false),
        eq(smQuestionnaireSubmissionQuestions.questionTypeSnapshot, "photo"),
        eq(smQuestionnaireSubmissionQuestions.isApplicable, true),
      )).limit(1);
    if (!answer) throw new SmVisitError(404, "sm_visit_photo_answer_not_found", "Die Foto-Antwort wurde nicht gefunden.");
    const extension = normalizePhotoExtension(input.extension);
    const path = `sm-visits/${actor.appUserId}/${answer.submissionId}/${answer.id}/${randomUUID()}.${extension}`;
    const { data, error } = await supabaseAdmin.storage.from(SM_VISIT_PHOTO_BUCKET).createSignedUploadUrl(path);
    if (error || !data) throw new SmVisitError(502, "sm_visit_photo_presign_failed", "Der geschützte Foto-Upload konnte nicht vorbereitet werden.");
    res.json({ upload: { bucket: SM_VISIT_PHOTO_BUCKET, path: data.path, signedUrl: data.signedUrl, token: data.token } });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Die Foto-Upload-Daten sind ungültig.", code: "sm_visit_photo_presign_invalid" });
    if (!sendError(error, res)) next(error);
  }
});

smVisitsRouter.post("/:assignmentId/photos/commit", async (req: AuthedRequest, res, next) => {
  try {
    const actor = requireAuthUser(req);
    const assignmentId = assignmentIdSchema.parse(param(req, "assignmentId"));
    const input = photoCommitSchema.parse(req.body);
    const committed = await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_visit:${assignmentId}:${input.answerId}:photos`}, 0))`);
      const assignment = await loadOwnedAssignment(tx, assignmentId, actor.appUserId, true);
      if (assignment.status !== "in_progress") throw new SmVisitError(409, "sm_visit_not_in_progress", "Der Einsatz ist nicht in Arbeit.");
      const [answer] = await tx.select({
        id: smQuestionAnswers.id,
        submissionId: smQuestionAnswers.submissionId,
        submissionQuestionId: smQuestionAnswers.submissionQuestionId,
        answerVersion: smQuestionAnswers.answerVersion,
      }).from(smQuestionAnswers)
        .innerJoin(smQuestionnaireSubmissions, eq(smQuestionnaireSubmissions.id, smQuestionAnswers.submissionId))
        .innerJoin(smQuestionnaireSubmissionQuestions, eq(smQuestionnaireSubmissionQuestions.id, smQuestionAnswers.submissionQuestionId))
        .where(and(
          eq(smQuestionAnswers.id, input.answerId),
          eq(smQuestionAnswers.isDeleted, false),
          eq(smQuestionAnswers.isCurrent, true),
          eq(smQuestionnaireSubmissions.assignmentId, assignmentId),
          eq(smQuestionnaireSubmissions.smUserId, actor.appUserId),
          eq(smQuestionnaireSubmissions.status, "draft"),
          eq(smQuestionnaireSubmissions.isDeleted, false),
          eq(smQuestionnaireSubmissionQuestions.questionTypeSnapshot, "photo"),
          eq(smQuestionnaireSubmissionQuestions.isApplicable, true),
        )).limit(1).for("update");
      if (!answer) throw new SmVisitError(404, "sm_visit_photo_answer_not_found", "Die Foto-Antwort wurde nicht gefunden.");
      const expectedPrefix = `sm-visits/${actor.appUserId}/${answer.submissionId}/${answer.id}/`;
      for (const photo of input.photos) {
        if (!photo.storagePath.startsWith(expectedPrefix) || photo.storagePath.includes("..")) throw new SmVisitError(400, "sm_visit_photo_path_invalid", "Der Foto-Pfad gehört nicht zu diesem Einsatz.");
        if (!(await storageObjectExists(photo.storageBucket, photo.storagePath))) throw new SmVisitError(409, "sm_visit_photo_not_uploaded", "Ein Foto wurde noch nicht vollständig hochgeladen.");
      }
      const existing = await tx.select().from(smQuestionAnswerFiles).where(and(
        eq(smQuestionAnswerFiles.answerId, answer.id),
        eq(smQuestionAnswerFiles.isDeleted, false),
      ));
      const existingPaths = new Set(existing.map((photo) => photo.storagePath));
      const fresh = input.photos.filter((photo) => !existingPaths.has(photo.storagePath));
      if (existing.length + fresh.length > 20) throw new SmVisitError(400, "sm_visit_photo_limit_exceeded", "Pro Foto-Frage sind maximal 20 Fotos erlaubt.");
      const inserted = fresh.length ? await tx.insert(smQuestionAnswerFiles).values(fresh.map((photo) => ({
        answerId: answer.id,
        storageBucket: photo.storageBucket,
        storagePath: photo.storagePath,
        originalFileName: photo.originalFileName ?? null,
        mimeType: photo.mimeType,
        byteSize: photo.byteSize,
        widthPx: photo.widthPx ?? null,
        heightPx: photo.heightPx ?? null,
      }))).returning() : [];
      const allFiles = [...existing, ...inserted];
      const now = new Date();
      await tx.update(smQuestionAnswers).set({
        answerState: allFiles.length ? "answered" : "unanswered",
        valueJson: { kind: "photo", fileIds: allFiles.map((photo) => photo.id) },
        answeredAt: allFiles.length ? now : null,
        answeredByUserId: actor.appUserId,
        updatedAt: now,
      }).where(eq(smQuestionAnswers.id, answer.id));
      await tx.insert(smQuestionAnswerEvents).values({
        answerId: answer.id,
        submissionId: answer.submissionId,
        eventType: "set",
        answerVersion: answer.answerVersion,
        payload: { addedFileIds: inserted.map((photo) => photo.id) },
        actorUserId: actor.appUserId,
      });
      await recomputeApplicability(tx, answer.submissionId, actor.appUserId);
      return allFiles.map((photo) => photo.id);
    });
    res.json({ fileIds: committed });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Die Foto-Daten sind ungültig.", code: "sm_visit_photo_commit_invalid", details: { issues: error.issues } });
    if (!sendError(error, res)) next(error);
  }
});

smVisitsRouter.post("/:assignmentId/photos/cleanup", async (req: AuthedRequest, res, next) => {
  try {
    const actor = requireAuthUser(req);
    const assignmentId = assignmentIdSchema.parse(param(req, "assignmentId"));
    const input = photoCleanupSchema.parse(req.body);
    const assignment = await loadOwnedAssignment(db, assignmentId, actor.appUserId);
    if (assignment.status !== "in_progress") throw new SmVisitError(409, "sm_visit_not_in_progress", "Der Einsatz ist nicht in Arbeit.");
    const [answer] = await db.select({
      id: smQuestionAnswers.id,
      submissionId: smQuestionAnswers.submissionId,
    }).from(smQuestionAnswers)
      .innerJoin(smQuestionnaireSubmissions, eq(smQuestionnaireSubmissions.id, smQuestionAnswers.submissionId))
      .innerJoin(smQuestionnaireSubmissionQuestions, eq(smQuestionnaireSubmissionQuestions.id, smQuestionAnswers.submissionQuestionId))
      .where(and(
        eq(smQuestionAnswers.id, input.answerId),
        eq(smQuestionAnswers.isDeleted, false),
        eq(smQuestionAnswers.isCurrent, true),
        eq(smQuestionnaireSubmissions.assignmentId, assignmentId),
        eq(smQuestionnaireSubmissions.smUserId, actor.appUserId),
        eq(smQuestionnaireSubmissions.status, "draft"),
        eq(smQuestionnaireSubmissions.isDeleted, false),
        eq(smQuestionnaireSubmissionQuestions.questionTypeSnapshot, "photo"),
      )).limit(1);
    if (!answer) throw new SmVisitError(404, "sm_visit_photo_answer_not_found", "Die Foto-Antwort wurde nicht gefunden.");
    const expectedPrefix = `sm-visits/${actor.appUserId}/${answer.submissionId}/${answer.id}/`;
    if (!input.storagePath.startsWith(expectedPrefix) || input.storagePath.includes("..")) {
      throw new SmVisitError(400, "sm_visit_photo_path_invalid", "Der Foto-Pfad gehört nicht zu diesem Einsatz.");
    }
    const [committed] = await db.select({ id: smQuestionAnswerFiles.id }).from(smQuestionAnswerFiles).where(and(
      eq(smQuestionAnswerFiles.answerId, answer.id),
      eq(smQuestionAnswerFiles.storageBucket, input.storageBucket),
      eq(smQuestionAnswerFiles.storagePath, input.storagePath),
      eq(smQuestionAnswerFiles.isDeleted, false),
    )).limit(1);
    if (committed) return res.json({ removed: false, committed: true });
    const { error } = await supabaseAdmin.storage.from(input.storageBucket).remove([input.storagePath]);
    if (error) throw new SmVisitError(502, "sm_visit_photo_cleanup_failed", "Der unvollständige Foto-Upload konnte nicht bereinigt werden.");
    res.json({ removed: true, committed: false });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Die Foto-Bereinigungsdaten sind ungültig.", code: "sm_visit_photo_cleanup_invalid" });
    if (!sendError(error, res)) next(error);
  }
});

smVisitsRouter.delete("/:assignmentId/photos/:fileId", async (req: AuthedRequest, res, next) => {
  try {
    const actor = requireAuthUser(req);
    const assignmentId = assignmentIdSchema.parse(param(req, "assignmentId"));
    const fileId = z.string().uuid().parse(param(req, "fileId"));
    const removedPhoto = await db.transaction(async (tx) => {
      await loadOwnedAssignment(tx, assignmentId, actor.appUserId, true);
      const [file] = await tx.select({
        id: smQuestionAnswerFiles.id,
        answerId: smQuestionAnswerFiles.answerId,
        storageBucket: smQuestionAnswerFiles.storageBucket,
        storagePath: smQuestionAnswerFiles.storagePath,
        submissionId: smQuestionAnswers.submissionId,
        answerVersion: smQuestionAnswers.answerVersion,
      }).from(smQuestionAnswerFiles)
        .innerJoin(smQuestionAnswers, eq(smQuestionAnswers.id, smQuestionAnswerFiles.answerId))
        .innerJoin(smQuestionnaireSubmissions, eq(smQuestionnaireSubmissions.id, smQuestionAnswers.submissionId))
        .where(and(
          eq(smQuestionAnswerFiles.id, fileId),
          eq(smQuestionAnswerFiles.isDeleted, false),
          eq(smQuestionAnswers.isDeleted, false),
          eq(smQuestionAnswers.isCurrent, true),
          eq(smQuestionnaireSubmissions.assignmentId, assignmentId),
          eq(smQuestionnaireSubmissions.smUserId, actor.appUserId),
          eq(smQuestionnaireSubmissions.status, "draft"),
        )).limit(1).for("update");
      if (!file) throw new SmVisitError(404, "sm_visit_photo_not_found", "Das Foto wurde nicht gefunden.");
      const now = new Date();
      await tx.update(smQuestionAnswerFiles).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(smQuestionAnswerFiles.id, file.id));
      const remaining = await tx.select({ id: smQuestionAnswerFiles.id }).from(smQuestionAnswerFiles).where(and(
        eq(smQuestionAnswerFiles.answerId, file.answerId),
        eq(smQuestionAnswerFiles.isDeleted, false),
      ));
      await tx.update(smQuestionAnswers).set({
        answerState: remaining.length ? "answered" : "unanswered",
        valueJson: { kind: "photo", fileIds: remaining.map((photo) => photo.id) },
        answeredAt: remaining.length ? now : null,
        updatedAt: now,
      }).where(eq(smQuestionAnswers.id, file.answerId));
      await tx.insert(smQuestionAnswerEvents).values({
        answerId: file.answerId,
        submissionId: file.submissionId,
        eventType: remaining.length ? "set" : "clear",
        answerVersion: file.answerVersion,
        payload: { removedFileId: file.id },
        actorUserId: actor.appUserId,
      });
      await recomputeApplicability(tx, file.submissionId, actor.appUserId);
      return { storageBucket: file.storageBucket, storagePath: file.storagePath };
    });
    try {
      const { error } = await supabaseAdmin.storage.from(removedPhoto.storageBucket).remove([removedPhoto.storagePath]);
      if (error) logger.warn("sm_visit_photo_delete_storage_cleanup_failed", {
        assignmentId,
        fileId,
        bucket: removedPhoto.storageBucket,
        path: removedPhoto.storagePath,
        error: error.message,
      });
    } catch (cleanupError) {
      logger.warn("sm_visit_photo_delete_storage_cleanup_failed", {
        assignmentId,
        fileId,
        bucket: removedPhoto.storageBucket,
        path: removedPhoto.storagePath,
        error: cleanupError instanceof Error ? cleanupError.message : String(cleanupError),
      });
    }
    res.status(204).send();
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Ungültige Foto-ID.", code: "sm_visit_photo_id_invalid" });
    if (!sendError(error, res)) next(error);
  }
});

smVisitsRouter.put("/:assignmentId/answers/:submissionQuestionId", async (req: AuthedRequest, res, next) => {
  try {
    const actor = requireAuthUser(req);
    const assignmentId = assignmentIdSchema.parse(param(req, "assignmentId"));
    const questionId = z.string().uuid().parse(param(req, "submissionQuestionId"));
    const input = saveAnswerSchema.parse(req.body);
    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_visit:${assignmentId}:${questionId}`}, 0))`);
      const assignment = await loadOwnedAssignment(tx, assignmentId, actor.appUserId, true);
      if (assignment.status !== "in_progress") throw new SmVisitError(409, "sm_visit_not_in_progress", "Der Einsatz ist nicht in Arbeit.");
      const [submission] = await tx.select().from(smQuestionnaireSubmissions).where(and(
        eq(smQuestionnaireSubmissions.assignmentId, assignmentId),
        eq(smQuestionnaireSubmissions.smUserId, actor.appUserId),
        eq(smQuestionnaireSubmissions.status, "draft"),
        eq(smQuestionnaireSubmissions.isDeleted, false),
        eq(smQuestionnaireSubmissions.isCurrent, true),
      )).limit(1).for("update");
      if (!submission) throw new SmVisitError(409, "sm_visit_draft_missing", "Der laufende Fragebogen wurde nicht gefunden.");
      const [question] = await tx.select().from(smQuestionnaireSubmissionQuestions).where(and(
        eq(smQuestionnaireSubmissionQuestions.id, questionId),
        eq(smQuestionnaireSubmissionQuestions.submissionId, submission.id),
        eq(smQuestionnaireSubmissionQuestions.isDeleted, false),
      )).limit(1).for("update");
      if (!question) throw new SmVisitError(404, "sm_visit_question_not_found", "Die Frage wurde nicht gefunden.");
      if (!question.isApplicable) throw new SmVisitError(409, "sm_visit_question_hidden", "Diese Frage ist aufgrund einer vorherigen Antwort nicht relevant.");
      const options = optionSnapshot(question.answerOptionsSnapshot);
      const normalized = normalizeSmVisitAnswer({
        type: question.questionTypeSnapshot,
        config: question.configSnapshot,
        options,
      } satisfies SmVisitQuestionSnapshot, input.answer);
      if (normalized.kind === "photo") throw new SmVisitError(501, "sm_visit_photo_upload_required", "Fotos müssen zuerst über den geschützten Foto-Upload gespeichert werden.");
      const [current] = await tx.select().from(smQuestionAnswers).where(and(
        eq(smQuestionAnswers.submissionQuestionId, question.id),
        eq(smQuestionAnswers.isDeleted, false),
        eq(smQuestionAnswers.isCurrent, true),
      )).limit(1).for("update");
      if (current?.valueJson && stableSmVisitAnswer(current.valueJson as SmVisitAnswerPayload) === stableSmVisitAnswer(normalized)) {
        return { saved: false, answerVersion: current.answerVersion };
      }
      const currentVersion = current?.answerVersion ?? 0;
      if (currentVersion !== input.expectedAnswerVersion) {
        throw new SmVisitError(409, "sm_visit_answer_version_conflict", "Diese Antwort wurde zwischenzeitlich in einem anderen Fenster geändert. Bitte prüfe den aktuellen Stand und speichere erneut.", {
          currentAnswerVersion: currentVersion,
          submissionQuestionId: question.id,
        });
      }
      const now = new Date();
      if (current) await tx.update(smQuestionAnswers).set({ isCurrent: false, updatedAt: now }).where(eq(smQuestionAnswers.id, current.id));
      const answerId = randomUUID();
      const isAnswered = isAnsweredSmVisitPayload(normalized);
      const version = currentVersion + 1;
      await tx.insert(smQuestionAnswers).values({
        id: answerId,
        submissionId: submission.id,
        submissionQuestionId: question.id,
        supersedesAnswerId: current?.id ?? null,
        answerVersion: version,
        isCurrent: true,
        answerState: isAnswered ? "answered" : "unanswered",
        valueText: normalized.kind === "text" ? normalized.value : null,
        valueNumber: normalized.kind === "number" ? String(normalized.value) : null,
        valueJson: normalized,
        answeredByUserId: actor.appUserId,
        answeredAt: isAnswered ? now : null,
      });
      const selectedCodes = normalized.kind === "choice" ? [normalized.optionCode]
        : normalized.kind === "multi" ? normalized.optionCodes
          : normalized.kind === "yesnomulti" ? [normalized.optionCode]
            : [];
      if (selectedCodes.length) {
        const fullOptions = Array.isArray(question.answerOptionsSnapshot) ? question.answerOptionsSnapshot as Array<Record<string, unknown>> : [];
        await tx.insert(smQuestionAnswerOptions).values(selectedCodes.map((code, orderIndex) => {
          const option = fullOptions.find((entry) => entry.code === code) ?? {};
          return {
            answerId,
            answerOptionVersionId: typeof option.id === "string" ? option.id : null,
            optionCodeSnapshot: code,
            optionLabelSnapshot: typeof option.label === "string" ? option.label : code,
            earnedPointsSnapshot: typeof option.earnedPoints === "string" ? option.earnedPoints : "0",
            possiblePointsSnapshot: typeof option.possiblePoints === "string" ? option.possiblePoints : "0",
            metricOutcomeCodeSnapshot: typeof option.metricOutcomeCode === "string" ? option.metricOutcomeCode : null,
            orderIndex,
          };
        }));
      }
      if (normalized.kind === "matrix" && normalized.cells.length) {
        await tx.insert(smQuestionAnswerMatrixCells).values(normalized.cells.map((cell, orderIndex) => ({
          answerId,
          rowCode: cell.rowCode,
          columnCode: cell.columnCode,
          selected: cell.selected,
          orderIndex,
        })));
      }
      await tx.insert(smQuestionAnswerEvents).values({
        answerId,
        submissionId: submission.id,
        eventType: isAnswered ? "set" : "clear",
        answerVersion: version,
        payload: { clientMutationToken: input.clientMutationToken },
        actorUserId: actor.appUserId,
      });
      await recomputeApplicability(tx, submission.id, actor.appUserId);
      return { saved: true, answerVersion: version };
    });
    res.json(result);
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Die Antwortdaten sind ungültig.", code: "sm_visit_answer_payload_invalid", details: { issues: error.issues } });
    if (!sendError(error, res)) next(error);
  }
});

smVisitsRouter.patch("/:assignmentId/timing", async (req: AuthedRequest, res, next) => {
  try {
    const actor = requireAuthUser(req);
    const assignmentId = assignmentIdSchema.parse(param(req, "assignmentId"));
    const input = timingSchema.parse(req.body);
    await db.transaction(async (tx) => {
      const assignment = await loadOwnedAssignment(tx, assignmentId, actor.appUserId, true);
      if (assignment.status !== "in_progress") throw new SmVisitError(409, "sm_visit_not_in_progress", "Der Einsatz ist nicht in Arbeit.");
      const context = await loadContext(tx, assignment, actor.appUserId);
      const [submission] = await tx.select().from(smQuestionnaireSubmissions).where(and(
        eq(smQuestionnaireSubmissions.assignmentId, assignmentId),
        eq(smQuestionnaireSubmissions.status, "draft"),
        eq(smQuestionnaireSubmissions.isDeleted, false),
        eq(smQuestionnaireSubmissions.isCurrent, true),
      )).limit(1).for("update");
      if (!submission) throw new SmVisitError(409, "sm_visit_draft_missing", "Der laufende Fragebogen wurde nicht gefunden.");
      if (input.travelMinutes !== undefined && !context.user.travelTimeEnabled) throw new SmVisitError(403, "sm_visit_travel_time_disabled", "Fahrtzeit ist für diesen Zugang nicht aktiviert.");
      if (input.manualVisitMinutes !== undefined && submission.visitTimeMode !== "manual") throw new SmVisitError(409, "sm_visit_manual_time_not_allowed", "Die manuelle Besuchszeit ist für diesen Timer-Einsatz nicht verfügbar.");
      await tx.update(smQuestionnaireSubmissions).set({
        ...(input.travelMinutes !== undefined ? { travelMinutes: input.travelMinutes } : {}),
        ...(input.manualVisitMinutes !== undefined ? { manualVisitMinutes: input.manualVisitMinutes } : {}),
        lastSavedAt: new Date(),
        updatedAt: new Date(),
      }).where(eq(smQuestionnaireSubmissions.id, submission.id));
    });
    const assignment = await loadOwnedAssignment(db, assignmentId, actor.appUserId);
    res.json(await loadVisitPayload(assignment, actor.appUserId));
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Die Zeitangaben sind ungültig.", code: "sm_visit_timing_invalid", details: { issues: error.issues } });
    if (!sendError(error, res)) next(error);
  }
});

smVisitsRouter.post("/:assignmentId/submit", async (req: AuthedRequest, res, next) => {
  try {
    const actor = requireAuthUser(req);
    const assignmentId = assignmentIdSchema.parse(param(req, "assignmentId"));
    const input = submitSchema.parse(req.body);
    const receipt = await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_visit:${assignmentId}:submit`}, 0))`);
      const assignment = await loadOwnedAssignment(tx, assignmentId, actor.appUserId, true);
      const [submission] = await tx.select().from(smQuestionnaireSubmissions).where(and(
        eq(smQuestionnaireSubmissions.assignmentId, assignmentId),
        eq(smQuestionnaireSubmissions.isDeleted, false),
        eq(smQuestionnaireSubmissions.isCurrent, true),
      )).limit(1).for("update");
      if (!submission) throw new SmVisitError(409, "sm_visit_draft_missing", "Der Fragebogen wurde nicht gefunden.");
      if (submission.status === "submitted") {
        const [persistedTime] = await tx.select({ actualMinutes: smAssignmentTimeSubmissions.actualMinutes })
          .from(smAssignmentTimeSubmissions)
          .where(and(
            eq(smAssignmentTimeSubmissions.assignmentId, assignmentId),
            eq(smAssignmentTimeSubmissions.isDeleted, false),
            eq(smAssignmentTimeSubmissions.isCurrent, true),
          ))
          .limit(1);
        return {
          submissionId: submission.id,
          submittedAt: submission.submittedAt?.toISOString() ?? submission.updatedAt.toISOString(),
          actualMinutes: persistedTime?.actualMinutes ?? submission.manualVisitMinutes,
        };
      }
      if (submission.status !== "draft" || assignment.status !== "in_progress") throw new SmVisitError(409, "sm_visit_not_submittable", "Dieser Fragebogen kann nicht abgeschlossen werden.");
      await recomputeApplicability(tx, submission.id, actor.appUserId);
      const applicableQuestions = await tx.select().from(smQuestionnaireSubmissionQuestions).where(and(
        eq(smQuestionnaireSubmissionQuestions.submissionId, submission.id),
        eq(smQuestionnaireSubmissionQuestions.isDeleted, false),
        eq(smQuestionnaireSubmissionQuestions.isApplicable, true),
      ));
      const currentAnswers = await tx.select().from(smQuestionAnswers).where(and(
        eq(smQuestionAnswers.submissionId, submission.id),
        eq(smQuestionAnswers.isDeleted, false),
        eq(smQuestionAnswers.isCurrent, true),
        eq(smQuestionAnswers.answerState, "answered"),
      ));
      const currentAnswerByQuestionId = new Map(currentAnswers.map((answer) => [answer.submissionQuestionId, answer]));
      const missing = applicableQuestions.filter((question) => {
        if (!question.requiredSnapshot) return false;
        const answer = currentAnswerByQuestionId.get(question.id);
        return !isCompleteSmVisitAnswer({
          type: question.questionTypeSnapshot,
          config: question.configSnapshot,
          options: optionSnapshot(question.answerOptionsSnapshot),
        }, answer?.valueJson as SmVisitAnswerPayload | null | undefined);
      });
      if (missing.length) throw new SmVisitError(409, "sm_visit_required_answers_missing", "Bitte beantworte alle Pflichtfragen.", {
        questionIds: missing.map((question) => question.id),
      });
      const now = new Date();
      const selectedVisitStartedAt = input.visitStartedAt ? new Date(input.visitStartedAt) : null;
      const selectedVisitCompletedAt = input.visitCompletedAt ? new Date(input.visitCompletedAt) : null;
      const effectiveVisitStartedAt = selectedVisitStartedAt ?? submission.visitStartedAt;
      const effectiveVisitCompletedAt = selectedVisitCompletedAt ?? (effectiveVisitStartedAt ? now : null);
      const elapsedMinutes = effectiveVisitStartedAt && effectiveVisitCompletedAt
        ? Math.max(1, Math.round((effectiveVisitCompletedAt.getTime() - effectiveVisitStartedAt.getTime()) / 60_000))
        : null;
      const actualMinutes = selectedVisitStartedAt && selectedVisitCompletedAt
        ? elapsedMinutes
        : input.actualMinutes ?? (submission.visitTimeMode === "manual" ? submission.manualVisitMinutes : elapsedMinutes);
      if (!actualMinutes || actualMinutes < 1 || actualMinutes > 1440) throw new SmVisitError(409, "sm_visit_actual_time_missing", "Bitte trage vor dem Abschluss die tatsächliche Besuchszeit ein.");
      const [existingTime] = await tx.select().from(smAssignmentTimeSubmissions).where(and(
        eq(smAssignmentTimeSubmissions.assignmentId, assignmentId),
        eq(smAssignmentTimeSubmissions.isDeleted, false),
        eq(smAssignmentTimeSubmissions.isCurrent, true),
      )).limit(1).for("update");
      if (!existingTime) await tx.insert(smAssignmentTimeSubmissions).values({
        assignmentId,
        revisionNumber: 1,
        actualMinutes,
        submittedByUserId: actor.appUserId,
        submittedAt: now,
      });
      await tx.update(smQuestionnaireSubmissions).set({
        status: "submitted",
        visitStartedAt: effectiveVisitStartedAt,
        visitCompletedAt: effectiveVisitCompletedAt,
        submittedAt: now,
        reportingAvailableAt: now,
        answeredQuestionCount: currentAnswers.length,
        lastSavedAt: now,
        updatedAt: now,
      }).where(eq(smQuestionnaireSubmissions.id, submission.id));
      await tx.update(smAssignments).set({
        status: "completed",
        completedAt: effectiveVisitCompletedAt ?? now,
        updatedByUserId: actor.appUserId,
        updatedAt: now,
      }).where(eq(smAssignments.id, assignmentId));
      await tx.insert(smAssignmentEvents).values({
        assignmentId,
        seriesId: assignment.seriesId,
        eventType: "updated",
        actorUserId: actor.appUserId,
        reason: "SM Marktbesuch abgeschlossen",
        beforeState: { status: assignment.status },
        afterState: {
          status: "completed",
          actualMinutes,
          visitStartedAt: effectiveVisitStartedAt?.toISOString() ?? null,
          visitCompletedAt: effectiveVisitCompletedAt?.toISOString() ?? null,
          clientMutationToken: input.clientMutationToken,
        },
      });
      return { submissionId: submission.id, submittedAt: now.toISOString(), actualMinutes };
    });
    res.json({ receipt });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Die Abschlussdaten sind ungültig.", code: "sm_visit_submit_invalid", details: { issues: error.issues } });
    if (!sendError(error, res)) next(error);
  }
});
