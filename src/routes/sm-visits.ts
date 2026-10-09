import { randomUUID } from "node:crypto";
import { assertSMDurcharbeitOverride, loadSMDurcharbeitSelectionCatalog, resolveSMDurcharbeitSelection, SMDurcharbeitSelectionError } from "../sm-SMDurcharbeit-selection.shared.js";
import { and, asc, desc, eq, gte, inArray, isNull, lte, or, sql } from "drizzle-orm";
import { Router, type Response } from "express";
import { z } from "zod";
import { smAnswerComment, smCommentMissing } from "../sm-comment.shared.js";
import { SMDurcharbeitCampaignError, loadSMDurcharbeitOwnedVisit, loadSMDurcharbeitTarget, lockSMDurcharbeitTarget,
  assertSMDurcharbeitAvailable, loadSMDurcharbeitVersion, SMDurcharbeitToday, SMDurcharbeitMonth,
  SMDurcharbeitEvent, reconcileSMDurcharbeitTarget, saveSMDurcharbeitVisitTime } from "../sm-SMDurcharbeit-campaign.shared.js";
import { copySMDurcharbeitMonthlyAnswers, SMDurcharbeitAnswerFiles } from "../sm-SMDurcharbeit-answer-reuse.shared.js";

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
  smSMDurcharbeitVisits,
  smSMDurcharbeitTargets,
  smSMDurcharbeitTimeRevisions,
  smSMDurcharbeitFileLinks,
  smSMDurcharbeitAnswerProvenance,
} from "../lib/schema.js";
import { supabaseAdmin } from "../lib/supabase.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { resolveSmAssignmentValues } from "../sm-planning.shared.js";
import { assertSmVisitTimeAvailable, SmTimeOverlapError } from "../sm-time-overlap.js";
import { lockSmPlanning } from "../sm-planning-lock.js";
import { smDeactivationToday } from "../sm-market-deactivation.js";
import { buildSmAssignmentCompletionUpdate } from "../sm-visit-time.shared.js";
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
type SMDurcharbeitExecution = {
  id: string; status: "in_progress" | "completed" | "cancelled"; seriesId: null; startedAt: Date | null;
  SMDurcharbeit: Awaited<ReturnType<typeof loadSMDurcharbeitOwnedVisit>>;
};
type VisitExecution = AssignmentRow | SMDurcharbeitExecution;
function isSMDurcharbeitExecution(execution: VisitExecution): execution is SMDurcharbeitExecution {
  return "SMDurcharbeit" in execution;
}
function executionValues(execution: VisitExecution) {
  if (!isSMDurcharbeitExecution(execution)) return resolveSmAssignmentValues(execution);
  const { visit, context } = execution.SMDurcharbeit;
  return { smUserId: visit.smUserId, smMarketId: context.membership.smMarketId,
    workDate: null, plannedMinutes: null, marketInternalId: String(context.target.marketSnapshot.internalId ?? context.market.id) };
}
function executionCondition(execution: VisitExecution) {
  return isSMDurcharbeitExecution(execution)
    ? eq(smQuestionnaireSubmissions.SMDurcharbeitVisitId, execution.id)
    : eq(smQuestionnaireSubmissions.assignmentId, execution.id);
}
type DbTx = Parameters<Parameters<typeof db.transaction>[0]>[0];
type DbExecutor = typeof db | DbTx;

const assignmentIdSchema = z.string().uuid();
const startSchema = z.object({
  mode: z.enum(["timer", "manual"]),
  travelMinutes: z.number().int().min(0).max(1440).nullable().optional(),
  clientSubmissionToken: z.string().trim().min(8).max(300),
  SMDurcharbeitExpectedSelectionRevision: z.string().max(2000).optional(),
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
  if (error instanceof SMDurcharbeitCampaignError) {
    res.status(error.statusCode).json({ error: error.message, code: error.code });
    return true;
  }
  if (error instanceof SmTimeOverlapError) {
    res.status(error.statusCode).json({ error: error.message, code: error.code, details: error.details });
    return true;
  }
  if (error instanceof SmVisitAnswerValidationError) {
    res.status(400).json({ error: error.message, code: "sm_visit_answer_invalid" });
    return true;
  }
  if (!(error instanceof SmVisitError) && !(error instanceof SMDurcharbeitSelectionError)) return false;
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

async function loadDatedOwnedAssignment(executor: DbExecutor, assignmentId: string, smUserId: string, lock = false): Promise<AssignmentRow> {
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

async function loadContext(executor: DbExecutor, assignment: VisitExecution, smUserId: string) {
  const effective = executionValues(assignment);
  const [[user], [market]] = await Promise.all([
    executor.select({
      id: users.id,
      firstName: users.firstName,
      lastName: users.lastName,
      travelTimeEnabled: users.travelTimeEnabled,
    }).from(users).where(and(eq(users.id, smUserId), eq(users.role, "sm"), eq(users.isActive, true), isNull(users.deletedAt))).limit(1),
    executor.select().from(smMarkets).where(and(eq(smMarkets.id, effective.smMarketId), isSMDurcharbeitExecution(assignment) ? undefined : eq(smMarkets.isDeleted, false))).limit(1),
  ]);
  if (!user) throw new SmVisitError(403, "sm_visit_sm_inactive", "Der Shelf-Merchandiser-Zugang ist nicht aktiv.");
  if (!market) throw new SmVisitError(409, "sm_visit_market_missing", "Der zugeordnete SM-Markt wurde nicht gefunden.");
  if (isSMDurcharbeitExecution(assignment)) {
    const frozen = assignment.SMDurcharbeit.context.target.marketSnapshot;
    return { effective, user, market: { ...market, name: String(frozen.name ?? market.name), address: String(frozen.address ?? market.address),
      postalCode: String(frozen.postalCode ?? market.postalCode), city: String(frozen.city ?? market.city), region: String(frozen.region ?? market.region) } };
  }
  return { effective, user, market };
}

async function resolveQuestionnaireVersion(tx: DbTx, assignment: AssignmentRow, expectedRevision?: string) {
  const catalog = await loadSMDurcharbeitSelectionCatalog(tx);
  await assertSMDurcharbeitOverride(tx, assignment, catalog);
  const resolution = resolveSMDurcharbeitSelection(catalog, assignment);
  if (expectedRevision && resolution.selection.revision !== expectedRevision) {
    throw new SMDurcharbeitSelectionError(409, "smdurcharbeit_selection_stale", "Der Fragebogen wurde zwischenzeitlich geändert. Bitte die Vorschau neu laden.");
  }
  if (!resolution.selection.available || !resolution.version) {
    throw new SMDurcharbeitSelectionError(409, "smdurcharbeit_questionnaire_unavailable", resolution.selection.blockReason ?? "Kein Fragebogen verfügbar.");
  }
  if (assignment.questionnaireVersionId !== resolution.version.id) {
    await tx.update(smAssignments).set({ questionnaireVersionId: resolution.version.id, updatedAt: new Date() }).where(eq(smAssignments.id, assignment.id));
  }
  return resolution.version;
}

async function createSubmissionGraph(tx: DbTx, input: {
  submissionId: string;
  questionnaireVersionId: string;
  SMDurcharbeitPinned?: boolean;
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
      input.SMDurcharbeitPinned ? undefined : eq(smModules.isDeleted, false),
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
      input.SMDurcharbeitPinned ? undefined : eq(smQuestions.isDeleted, false),
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

function publicAssignment(assignment: VisitExecution, context: Awaited<ReturnType<typeof loadContext>>) {
  return {
    id: isSMDurcharbeitExecution(assignment) ? `SMDurcharbeit:${assignment.id}` : assignment.id,
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

async function loadVisitPayload(assignment: VisitExecution, smUserId: string) {
  const context = await loadContext(db, assignment, smUserId);
  const [submission] = await db.select().from(smQuestionnaireSubmissions).where(and(
    executionCondition(assignment),
    eq(smQuestionnaireSubmissions.isDeleted, false),
    eq(smQuestionnaireSubmissions.isCurrent, true),
  )).limit(1);
  if (!submission) {
    if (isSMDurcharbeitExecution(assignment)) throw new SmVisitError(404, "smdurcharbeit_submission_missing", "Der Besuch wurde nicht gefunden.");
    const resolved = resolveSMDurcharbeitSelection(await loadSMDurcharbeitSelectionCatalog(db), assignment);
    return {
      assignment: publicAssignment(assignment, context),
      profile: { name: `${context.user.firstName} ${context.user.lastName}`.trim(), travelTimeEnabled: context.user.travelTimeEnabled },
      questionnaireAvailability: { count: resolved.count, names: resolved.selection.name ? [resolved.selection.name] : [] },
      SMDurcharbeitQuestionnaireSelection: resolved.selection,
      submission: null,
      sections: [],
      answers: {},
      answerVersions: {},
      photoFiles: {},
    };
  }

  const [timeSubmission] = submission.status === "submitted" && isSMDurcharbeitExecution(assignment)
    ? await db.select({ actualMinutes: smSMDurcharbeitTimeRevisions.actualMinutes }).from(smSMDurcharbeitTimeRevisions)
      .where(and(eq(smSMDurcharbeitTimeRevisions.visitId, assignment.id), eq(smSMDurcharbeitTimeRevisions.isCurrent, true))).limit(1)
    : submission.status === "submitted"
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
  const photoRows = isSMDurcharbeitExecution(assignment) ? await SMDurcharbeitAnswerFiles(db, answers.map(answer => answer.id)) : answers.length ? await db.select().from(smQuestionAnswerFiles).where(and(
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
      ...("SMDurcharbeitInherited" in photo ? { SMDurcharbeitInherited: photo.SMDurcharbeitInherited, uploadedAt: photo.uploadedAt.toISOString() } : {}),
    };
  }));
  const photoFilesByQuestion = new Map<string, typeof signedPhotoRows>();
  for (const photo of signedPhotoRows) {
    if (!photo.questionId) continue;
    photoFilesByQuestion.set(photo.questionId, [...(photoFilesByQuestion.get(photo.questionId) ?? []), photo]);
  }
  const questionsBySection = new Map<string, typeof questions>();
  for (const question of questions) questionsBySection.set(question.submissionSectionId, [...(questionsBySection.get(question.submissionSectionId) ?? []), question]);

  const selection = isSMDurcharbeitExecution(assignment) ? {
    questionnaireTemplateId: submission.questionnaireTemplateId, questionnaireVersionId: submission.questionnaireVersionId,
    name: submission.questionnaireNameSnapshot, versionNumber: submission.questionnaireVersionSnapshot,
    catalogScope: "SMDurcharbeit" as const, source: "submission" as const, available: true, blockReason: null, revision: submission.id,
  } : resolveSMDurcharbeitSelection(await loadSMDurcharbeitSelectionCatalog(db), assignment, submission).selection;
  const inheritedAnswers = isSMDurcharbeitExecution(assignment) && answers.length
    ? await db.select({ answerId: smSMDurcharbeitAnswerProvenance.answerId, sourceSubmissionId: smSMDurcharbeitAnswerProvenance.sourceSubmissionId })
      .from(smSMDurcharbeitAnswerProvenance).where(inArray(smSMDurcharbeitAnswerProvenance.answerId, answers.map(answer => answer.id))) : [];
  let SMDurcharbeitReadOnlyReason: string | null = null;
  if (isSMDurcharbeitExecution(assignment) && submission.status === "draft") {
    try { await assertSMDurcharbeitAvailable(db, assignment.SMDurcharbeit.context, smUserId); }
    catch (error) { if (error instanceof SMDurcharbeitCampaignError) SMDurcharbeitReadOnlyReason = error.message; else throw error; }
  }
  return {
    assignment: publicAssignment(assignment, context),
    profile: { name: `${context.user.firstName} ${context.user.lastName}`.trim(), travelTimeEnabled: context.user.travelTimeEnabled },
    questionnaireAvailability: { count: 1, names: [submission.questionnaireNameSnapshot] },
    SMDurcharbeitQuestionnaireSelection: selection,
    ...(isSMDurcharbeitExecution(assignment) ? { SMDurcharbeitContext: {
      visitId: assignment.id, targetId: assignment.SMDurcharbeit.context.target.id,
      campaignId: assignment.SMDurcharbeit.context.campaign.id, campaignName: assignment.SMDurcharbeit.context.campaign.name,
      month: assignment.SMDurcharbeit.context.period.month, targetRevision: assignment.SMDurcharbeit.context.target.revision,
      basisSubmissionId: assignment.SMDurcharbeit.visit.basisSubmissionId, basisRevision: assignment.SMDurcharbeit.visit.basisRevision,
      readOnlyReason: SMDurcharbeitReadOnlyReason,
      inheritedQuestionIds: inheritedAnswers.flatMap(source => { const answer = answers.find(a => a.id === source.answerId); return answer ? [answer.submissionQuestionId] : []; }),
    } } : {}),
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
      if (isSMDurcharbeitExecution(assignment) && submission.status === "draft" && question.questionTypeSnapshot === "photo") {
        const value = answer?.valueJson as SmVisitAnswerPayload | null;
        if (value?.kind === "photo") {
          const visibleIds = new Set((photoFilesByQuestion.get(question.id) ?? []).map(photo => photo.id));
          return [question.id, { ...value, fileIds: value.fileIds.filter(id => visibleIds.has(id)) }];
        }
      }
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

/** Creates a physical visit without a dated planning assignment. Runs wholly in its caller's transaction. */
export async function initializeSMDurcharbeitVisit(tx: DbTx, targetId: string, smUserId: string, input: {
  mode: "timer" | "manual"; travelMinutes?: number | null | undefined; clientSubmissionToken: string;
  expectedRevision: number; followUp: boolean;
}) {
  // Token and target locks make retries and two-tab starts resolve to the same draft.
  await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`SMDurcharbeit_start:${smUserId}:${input.clientSubmissionToken}`}, 0))`);
  const [replayed] = await tx.select().from(smQuestionnaireSubmissions).where(and(
    eq(smQuestionnaireSubmissions.smUserId, smUserId), eq(smQuestionnaireSubmissions.clientSubmissionToken, input.clientSubmissionToken), eq(smQuestionnaireSubmissions.isDeleted, false),
  )).limit(1);
  if (replayed) {
    if (replayed.SMDurcharbeitTargetId !== targetId || !replayed.SMDurcharbeitVisitId) throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_token_reused", "Dieser Start wurde bereits für einen anderen Besuch verwendet.");
    return { visitId: replayed.SMDurcharbeitVisitId, submissionId: replayed.id, replayed: true };
  }
  await lockSMDurcharbeitTarget(tx, targetId);
  const context = await loadSMDurcharbeitTarget(tx, targetId);
  const user = await assertSMDurcharbeitAvailable(tx, context, smUserId);
  const [draft] = await tx.select().from(smQuestionnaireSubmissions).where(and(
    eq(smQuestionnaireSubmissions.SMDurcharbeitTargetId, targetId), eq(smQuestionnaireSubmissions.status, "draft"), eq(smQuestionnaireSubmissions.isCurrent, true), eq(smQuestionnaireSubmissions.isDeleted, false),
  )).limit(1);
  if (draft) {
    if (draft.smUserId !== smUserId) throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_draft_owner_changed", "Dieser Monatsbesuch gehört einem anderen SM.");
    return { visitId: draft.SMDurcharbeitVisitId!, submissionId: draft.id, replayed: true };
  }
  if (context.target.revision !== input.expectedRevision) throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_target_stale", "Der Monatsstand wurde geändert. Bitte neu laden.");
  const version = await loadSMDurcharbeitVersion(tx, context.period.questionnaireVersionId);
  const [latestRow] = await tx.select({ submission: smQuestionnaireSubmissions }).from(smQuestionnaireSubmissions)
    .innerJoin(smSMDurcharbeitVisits, eq(smSMDurcharbeitVisits.id, smQuestionnaireSubmissions.SMDurcharbeitVisitId)).where(and(
    eq(smQuestionnaireSubmissions.SMDurcharbeitTargetId, targetId), eq(smQuestionnaireSubmissions.status, "submitted"), eq(smQuestionnaireSubmissions.isCurrent, true), eq(smQuestionnaireSubmissions.isDeleted, false),
  )).orderBy(desc(smSMDurcharbeitVisits.basisRevision), desc(smQuestionnaireSubmissions.submittedAt), desc(smQuestionnaireSubmissions.id)).limit(1);
  const latest = latestRow?.submission;
  if (Boolean(latest) !== input.followUp) throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_followup_confirmation", latest ? "Das Monatsziel ist erledigt. Bitte einen Folgebesuch ausdrücklich starten." : "Für diesen Monat gibt es noch keinen abgeschlossenen Besuch.");
  const visitId = randomUUID(), submissionId = randomUUID(), now = new Date();
  await tx.insert(smSMDurcharbeitVisits).values({ id: visitId, targetId, ownerRevisionId: context.target.ownerRevisionId,
    smUserId, basisSubmissionId: latest?.id ?? null, basisRevision: context.target.revision + 1 });
  await tx.insert(smQuestionnaireSubmissions).values({ id: submissionId, SMDurcharbeitVisitId: visitId, SMDurcharbeitTargetId: targetId,
    assignmentId: null, questionnaireTemplateId: version.questionnaireTemplateId, questionnaireVersionId: version.id,
    smUserId, smMarketId: context.membership.smMarketId, clientSubmissionToken: input.clientSubmissionToken,
    timezone: "Europe/Vienna", oncePerMarketSnapshot: false, questionnaireNameSnapshot: version.name, questionnaireVersionSnapshot: version.versionNumber,
    smNameSnapshot: `${user.firstName} ${user.lastName}`.trim(), marketNameSnapshot: String(context.target.marketSnapshot.name ?? context.market.name),
    marketAddressSnapshot: String(context.target.marketSnapshot.address ?? context.market.address), marketPostalCodeSnapshot: String(context.target.marketSnapshot.postalCode ?? context.market.postalCode),
    marketCitySnapshot: String(context.target.marketSnapshot.city ?? context.market.city), visitTimeMode: input.mode,
    travelMinutes: user.travelTimeEnabled ? input.travelMinutes ?? null : null, visitStartedAt: input.mode === "timer" ? now : null, lastSavedAt: now });
  await createSubmissionGraph(tx, { submissionId, questionnaireVersionId: version.id, SMDurcharbeitPinned: true });
  if (latest) await copySMDurcharbeitMonthlyAnswers(tx, { sourceSubmissionId: latest.id, submissionId, actorId: smUserId, basisRevision: context.target.revision });
  await recomputeApplicability(tx, submissionId, smUserId);
  await tx.update(smSMDurcharbeitTargets).set({ revision: context.target.revision + 1, updatedAt: now }).where(eq(smSMDurcharbeitTargets.id, targetId));
  await SMDurcharbeitEvent(tx, { campaignId: context.campaign.id, targetId, visitId, actorUserId: smUserId, action: latest ? "followup_started" : "visit_started",
    reason: latest ? "Folgebesuch mit Monatsantworten gestartet" : "Monatsbesuch gestartet", afterState: { submissionId, basisSubmissionId: latest?.id ?? null, month: context.period.month } });
  return { visitId, submissionId, replayed: false };
}

async function discardSMDurcharbeitDraft(tx: DbTx, execution: SMDurcharbeitExecution, actorId: string, reason = "Vom Shelf Merchandiser verworfen") {
  const { submission, context } = execution.SMDurcharbeit, now = new Date();
  if (submission.status !== "draft" || submission.submittedAt) throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_draft_required", "Abgeschlossene Besuche können nicht als Entwurf verworfen werden.");
  const answerRows = await tx.select({ id: smQuestionAnswers.id }).from(smQuestionAnswers).where(eq(smQuestionAnswers.submissionId, submission.id));
  const answerIds = answerRows.map(row => row.id);
  const photoRows = answerIds.length ? await tx.select({ bucket: smQuestionAnswerFiles.storageBucket, path: smQuestionAnswerFiles.storagePath })
    .from(smQuestionAnswerFiles).where(and(inArray(smQuestionAnswerFiles.answerId, answerIds), eq(smQuestionAnswerFiles.isDeleted, false),
      sql`not exists (select 1 from sm_smdurcharbeit_answer_file_links l where l.file_id = ${smQuestionAnswerFiles.id} and not l.is_deleted)`)) : [];
  if (answerIds.length) {
    await tx.update(smSMDurcharbeitFileLinks).set({ isDeleted: true }).where(inArray(smSMDurcharbeitFileLinks.answerId, answerIds));
    await tx.update(smQuestionAnswerOptions).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(inArray(smQuestionAnswerOptions.answerId, answerIds));
    await tx.update(smQuestionAnswerMatrixCells).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(inArray(smQuestionAnswerMatrixCells.answerId, answerIds));
    await tx.update(smQuestionAnswerFiles).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(inArray(smQuestionAnswerFiles.answerId, answerIds));
    await tx.update(smQuestionAnswers).set({ isCurrent: false, isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(smQuestionAnswers.submissionId, submission.id));
  }
  await tx.update(smQuestionnaireSubmissionQuestions).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(smQuestionnaireSubmissionQuestions.submissionId, submission.id));
  await tx.update(smQuestionnaireSubmissionSections).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(smQuestionnaireSubmissionSections.submissionId, submission.id));
  await tx.update(smQuestionnaireSubmissions).set({ status: "cancelled", isCurrent: false, isDeleted: true, deletedAt: now, cancelledAt: now,
    cancellationReason: reason, updatedAt: now, lastSavedAt: now }).where(eq(smQuestionnaireSubmissions.id, submission.id));
  await reconcileSMDurcharbeitTarget(tx, context.target.id, actorId, "draft_discarded");
  return { photoRows, restoredStatus: "planned" as const };
}

/** An explicit admin action can release a protected draft even after its month/owner closes. */
export async function cancelSMDurcharbeitDraft(tx: DbTx, targetId: string, actorId: string, input: { expectedRevision: number; visitId: string; reason: string }) {
  await lockSMDurcharbeitTarget(tx, targetId);
  const context = await loadSMDurcharbeitTarget(tx, targetId);
  if (context.target.revision !== input.expectedRevision) throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_target_stale", "Der Monatsstand wurde geändert. Bitte neu laden.");
  const [visit] = await tx.select().from(smSMDurcharbeitVisits).where(and(eq(smSMDurcharbeitVisits.id, input.visitId), eq(smSMDurcharbeitVisits.targetId, targetId))).limit(1);
  const [submission] = visit ? await tx.select().from(smQuestionnaireSubmissions).where(and(eq(smQuestionnaireSubmissions.SMDurcharbeitVisitId, visit.id), eq(smQuestionnaireSubmissions.isDeleted, false), eq(smQuestionnaireSubmissions.isCurrent, true))).limit(1).for("update") : [];
  if (!visit || !submission || submission.status !== "draft" || submission.submittedAt) throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_draft_required", "Der ausgewählte Entwurf ist nicht mehr offen. Bitte neu laden.");
  const result = await discardSMDurcharbeitDraft(tx, { id: visit.id, status: "in_progress", seriesId: null, startedAt: submission.visitStartedAt, SMDurcharbeit: { context, visit, submission } }, actorId, input.reason);
  await SMDurcharbeitEvent(tx, { campaignId: context.campaign.id, targetId, visitId: visit.id, actorUserId: actorId,
    action: "draft_cancelled_by_admin", reason: input.reason, beforeState: { submissionId: submission.id, ownerUserId: visit.smUserId, month: context.period.month }, afterState: { status: "cancelled" } });
  return result;
}

export async function cleanupSMDurcharbeitDraftPhotos(photos: Array<{ bucket: string; path: string }>) {
  const byBucket = new Map<string, string[]>();
  for (const photo of photos) byBucket.set(photo.bucket, [...(byBucket.get(photo.bucket) ?? []), photo.path]);
  for (const [bucket, paths] of byBucket) {
    try { const { error } = await supabaseAdmin.storage.from(bucket).remove([...new Set(paths)]); if (error) logger.warn("smdurcharbeit_cancel_photo_cleanup_failed", { bucket }); }
    catch { logger.warn("smdurcharbeit_cancel_photo_cleanup_failed", { bucket }); }
  }
}

async function removeSMDurcharbeitPhoto(tx: DbTx, execution: SMDurcharbeitExecution, fileId: string, actorId: string) {
  const { submission } = execution.SMDurcharbeit;
  if (submission.status !== "draft") throw new SmVisitError(409, "sm_visit_not_in_progress", "Dieser Besuch ist nicht mehr in Arbeit.");
  const currentAnswers = await tx.select().from(smQuestionAnswers).where(and(eq(smQuestionAnswers.submissionId, submission.id), eq(smQuestionAnswers.isCurrent, true), eq(smQuestionAnswers.isDeleted, false)));
  const file = (await SMDurcharbeitAnswerFiles(tx, currentAnswers.map(answer => answer.id))).find(row => row.id === fileId);
  const answer = currentAnswers.find(row => row.id === file?.answerId);
  if (!file || !answer) throw new SmVisitError(404, "sm_visit_photo_not_found", "Das Foto wurde nicht gefunden.");
  const now = new Date();
  let deleteStorage = false;
  if (file.SMDurcharbeitInherited) {
    await tx.update(smSMDurcharbeitFileLinks).set({ isDeleted: true }).where(and(eq(smSMDurcharbeitFileLinks.answerId, answer.id), eq(smSMDurcharbeitFileLinks.fileId, file.id)));
  } else {
    await tx.update(smQuestionAnswerFiles).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(smQuestionAnswerFiles.id, file.id));
    const [retained] = await tx.select({ id: smSMDurcharbeitFileLinks.answerId }).from(smSMDurcharbeitFileLinks).where(and(eq(smSMDurcharbeitFileLinks.fileId, file.id), eq(smSMDurcharbeitFileLinks.isDeleted, false))).limit(1);
    deleteStorage = !retained;
  }
  const remaining = await SMDurcharbeitAnswerFiles(tx, [answer.id]);
  await tx.update(smQuestionAnswers).set({ answerState: remaining.length ? "answered" : "unanswered",
    valueJson: { kind: "photo", fileIds: remaining.map(photo => photo.id), ...(remaining.length && smAnswerComment(answer.valueJson) ? { comment: smAnswerComment(answer.valueJson) } : {}) },
    answeredAt: remaining.length ? now : null, updatedAt: now }).where(eq(smQuestionAnswers.id, answer.id));
  await tx.insert(smQuestionAnswerEvents).values({ answerId: answer.id, submissionId: submission.id, eventType: remaining.length ? "set" : "clear", answerVersion: answer.answerVersion,
    payload: { removedFileId: file.id, SMDurcharbeitInherited: file.SMDurcharbeitInherited }, actorUserId: actorId });
  await recomputeApplicability(tx, submission.id, actorId);
  return { storageBucket: file.storageBucket, storagePath: file.storagePath, deleteStorage };
}

export function createSmVisitsRouter(SMDurcharbeit = false) {
const smVisitsRouter = Router();
const visitCondition = (id: string) => SMDurcharbeit ? eq(smQuestionnaireSubmissions.SMDurcharbeitVisitId, id) : eq(smQuestionnaireSubmissions.assignmentId, id);
const loadOwnedAssignment = async (executor: DbExecutor, id: string, smUserId: string, lock = false): Promise<VisitExecution> => {
  if (!SMDurcharbeit) return loadDatedOwnedAssignment(executor, id, smUserId, lock);
  const context = await loadSMDurcharbeitOwnedVisit(executor, id, smUserId, lock);
  return { id, status: context.submission.status === "draft" ? "in_progress" : context.submission.status === "submitted" ? "completed" : "cancelled",
    seriesId: null, startedAt: context.submission.visitStartedAt, SMDurcharbeit: context };
};
smVisitsRouter.use(requireAuth(["sm"]));

smVisitsRouter.get("/:assignmentId", async (req: AuthedRequest, res, next) => {
  try {
    const actor = requireAuthUser(req);
    const assignmentId = assignmentIdSchema.parse(param(req, "assignmentId"));
    const assignment = await loadOwnedAssignment(db, assignmentId, actor.appUserId);
    if (assignment.status === "cancelled") throw new SmVisitError(409, "sm_visit_assignment_cancelled", "Dieser Einsatz wurde abgesagt. Bitte kehre zur Übersicht zurück.");
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
      if (!SMDurcharbeit) await lockSmPlanning(tx);
      const assignment = await loadOwnedAssignment(tx, assignmentId, actor.appUserId, true);
      const [submission] = await tx.select().from(smQuestionnaireSubmissions).where(and(
        visitCondition(assignmentId),
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
      if (isSMDurcharbeitExecution(assignment)) return discardSMDurcharbeitDraft(tx, assignment, actor.appUserId);

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
      const previousStatus = previousStatusValue === "planned" || previousStatusValue === "confirmed" || previousStatusValue === "open"
        ? previousStatusValue
        : "planned";
      const effective = resolveSmAssignmentValues(assignment);
      const [market] = await tx.select({ isActive: smMarkets.isActive, isDeleted: smMarkets.isDeleted }).from(smMarkets).where(eq(smMarkets.id, effective.smMarketId)).limit(1);
      const cancelInactive = effective.workDate >= smDeactivationToday() && (!market || !market.isActive || market.isDeleted);
      const restoredStatus = cancelInactive ? "cancelled" : previousStatus;
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
        ...(cancelInactive ? { statusBeforeCancellation: previousStatus, cancelledAt: now, cancelledByUserId: actor.appUserId, cancellationReason: "Fragebogen verworfen; Markt inzwischen inaktiv" } : {}),
        startedAt: restoredStartedAt,
        completedAt: null,
        updatedByUserId: actor.appUserId,
        updatedAt: now,
      }).where(eq(smAssignments.id, assignmentId)).returning();
      if (!restoredAssignment) throw new SmVisitError(409, "sm_visit_assignment_restore_failed", "Der Einsatz konnte nicht zurückgesetzt werden.");
      await tx.insert(smAssignmentEvents).values({
        assignmentId,
        seriesId: assignment.seriesId,
        eventType: cancelInactive ? "cancelled" : "updated",
        actorUserId: actor.appUserId,
        reason: cancelInactive ? "SM Marktbesuch verworfen; Markt inzwischen inaktiv – Einsatz abgesagt" : "SM Marktbesuch verworfen",
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
      if (!SMDurcharbeit) await lockSmPlanning(tx);
      const assignment = await loadOwnedAssignment(tx, assignmentId, actor.appUserId, true);
      const context = await loadContext(tx, assignment, actor.appUserId);
      if (isSMDurcharbeitExecution(assignment)) return; // New visits begin at their monthly target; this endpoint only resumes.
      const [existing] = await tx.select().from(smQuestionnaireSubmissions).where(and(
        visitCondition(assignmentId),
        eq(smQuestionnaireSubmissions.isDeleted, false),
        eq(smQuestionnaireSubmissions.isCurrent, true),
      )).limit(1).for("update");
      if (existing) return;
      if (!context.market.isActive) throw new SmVisitError(409, "sm_visit_market_inactive", "Dieser Markt ist inaktiv. Der Einsatz kann nicht gestartet werden. Bitte wende dich an die Einsatzplanung.");
      if (["cancelled", "missed", "completed"].includes(assignment.status)) {
        throw new SmVisitError(409, "sm_visit_assignment_locked", "Dieser Einsatz kann nicht mehr gestartet werden.");
      }
      const version = await resolveQuestionnaireVersion(tx, assignment, input.SMDurcharbeitExpectedSelectionRevision);
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
        visitCondition(assignmentId),
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
    if (isSMDurcharbeitExecution(assignment)) await assertSMDurcharbeitAvailable(db, assignment.SMDurcharbeit.context, actor.appUserId);
    if (assignment.status !== "in_progress") throw new SmVisitError(409, "sm_visit_not_in_progress", "Der Einsatz ist nicht in Arbeit.");
    const [answer] = await db.select({ id: smQuestionAnswers.id, submissionId: smQuestionAnswers.submissionId }).from(smQuestionAnswers)
      .innerJoin(smQuestionnaireSubmissions, eq(smQuestionnaireSubmissions.id, smQuestionAnswers.submissionId))
      .innerJoin(smQuestionnaireSubmissionQuestions, eq(smQuestionnaireSubmissionQuestions.id, smQuestionAnswers.submissionQuestionId))
      .where(and(
        eq(smQuestionAnswers.id, input.answerId),
        eq(smQuestionAnswers.isDeleted, false),
        eq(smQuestionAnswers.isCurrent, true),
        visitCondition(assignmentId),
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
        valueJson: smQuestionAnswers.valueJson,
      }).from(smQuestionAnswers)
        .innerJoin(smQuestionnaireSubmissions, eq(smQuestionnaireSubmissions.id, smQuestionAnswers.submissionId))
        .innerJoin(smQuestionnaireSubmissionQuestions, eq(smQuestionnaireSubmissionQuestions.id, smQuestionAnswers.submissionQuestionId))
        .where(and(
          eq(smQuestionAnswers.id, input.answerId),
          eq(smQuestionAnswers.isDeleted, false),
          eq(smQuestionAnswers.isCurrent, true),
          visitCondition(assignmentId),
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
      const inherited = isSMDurcharbeitExecution(assignment) ? (await SMDurcharbeitAnswerFiles(tx, [answer.id])).filter(file => file.SMDurcharbeitInherited) : [];
      const existingPaths = new Set(existing.map((photo) => photo.storagePath));
      const fresh = input.photos.filter((photo) => !existingPaths.has(photo.storagePath));
      if (existing.length + inherited.length + fresh.length > 20) throw new SmVisitError(400, "sm_visit_photo_limit_exceeded", "Pro Foto-Frage sind maximal 20 Fotos erlaubt.");
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
      const allFiles = [...existing, ...inherited, ...inserted];
      const now = new Date();
      await tx.update(smQuestionAnswers).set({
        answerState: allFiles.length ? "answered" : "unanswered",
        valueJson: { kind: "photo", fileIds: allFiles.map((photo) => photo.id), ...(smAnswerComment(answer.valueJson) ? { comment: smAnswerComment(answer.valueJson) } : {}) },
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
        visitCondition(assignmentId),
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
      const execution = await loadOwnedAssignment(tx, assignmentId, actor.appUserId, true);
      if (isSMDurcharbeitExecution(execution)) return removeSMDurcharbeitPhoto(tx, execution, fileId, actor.appUserId);
      const [file] = await tx.select({
        id: smQuestionAnswerFiles.id,
        answerId: smQuestionAnswerFiles.answerId,
        storageBucket: smQuestionAnswerFiles.storageBucket,
        storagePath: smQuestionAnswerFiles.storagePath,
        submissionId: smQuestionAnswers.submissionId,
        answerVersion: smQuestionAnswers.answerVersion,
        valueJson: smQuestionAnswers.valueJson,
      }).from(smQuestionAnswerFiles)
        .innerJoin(smQuestionAnswers, eq(smQuestionAnswers.id, smQuestionAnswerFiles.answerId))
        .innerJoin(smQuestionnaireSubmissions, eq(smQuestionnaireSubmissions.id, smQuestionAnswers.submissionId))
        .where(and(
          eq(smQuestionAnswerFiles.id, fileId),
          eq(smQuestionAnswerFiles.isDeleted, false),
          eq(smQuestionAnswers.isDeleted, false),
          eq(smQuestionAnswers.isCurrent, true),
          visitCondition(assignmentId),
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
        valueJson: { kind: "photo", fileIds: remaining.map((photo) => photo.id), ...(remaining.length && smAnswerComment(file.valueJson) ? { comment: smAnswerComment(file.valueJson) } : {}) },
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
      return { storageBucket: file.storageBucket, storagePath: file.storagePath, deleteStorage: true };
    });
    if (removedPhoto.deleteStorage) try {
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
        visitCondition(assignmentId),
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
      if (question.questionTypeSnapshot === "photo") {
        // Comment-only update: never create, remove, replace or move uploaded files.
        const files = current && isSMDurcharbeitExecution(assignment) ? await SMDurcharbeitAnswerFiles(tx, [current.id]) : current ? await tx.select({ id: smQuestionAnswerFiles.id }).from(smQuestionAnswerFiles).where(and(
          eq(smQuestionAnswerFiles.answerId, current.id), eq(smQuestionAnswerFiles.isDeleted, false),
        )) : [];
        if (!current || normalized.kind !== "photo" || files.length !== normalized.fileIds.length || files.some((file) => !normalized.fileIds.includes(file.id))) {
          throw new SmVisitError(409, "sm_visit_photo_upload_required", "Bitte speichere zuerst alle Fotos, bevor du den Kommentar ergänzt.");
        }
        const version = currentVersion + 1;
        await tx.update(smQuestionAnswers).set({ valueJson: normalized, answerVersion: version, updatedAt: now }).where(eq(smQuestionAnswers.id, current.id));
        await tx.insert(smQuestionAnswerEvents).values({
          answerId: current.id, submissionId: submission.id, eventType: "set", answerVersion: version,
          payload: { clientMutationToken: input.clientMutationToken, changeKind: "comment", before: current.valueJson, after: normalized },
          actorUserId: actor.appUserId,
        });
        await recomputeApplicability(tx, submission.id, actor.appUserId);
        return { saved: true, answerVersion: version };
      }
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
        visitCondition(assignmentId),
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
        visitCondition(assignmentId),
        eq(smQuestionnaireSubmissions.isDeleted, false),
        eq(smQuestionnaireSubmissions.isCurrent, true),
      )).limit(1).for("update");
      if (!submission) throw new SmVisitError(409, "sm_visit_draft_missing", "Der Fragebogen wurde nicht gefunden.");
      if (submission.status === "submitted") {
        const [persistedTime] = isSMDurcharbeitExecution(assignment)
          ? await tx.select({ actualMinutes: smSMDurcharbeitTimeRevisions.actualMinutes }).from(smSMDurcharbeitTimeRevisions)
            .where(and(eq(smSMDurcharbeitTimeRevisions.visitId, assignmentId), eq(smSMDurcharbeitTimeRevisions.isCurrent, true))).limit(1)
          : await tx.select({ actualMinutes: smAssignmentTimeSubmissions.actualMinutes })
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
        const answer = currentAnswerByQuestionId.get(question.id);
        const snapshot = { type: question.questionTypeSnapshot, config: question.configSnapshot, options: optionSnapshot(question.answerOptionsSnapshot) };
        if (!question.requiredSnapshot) return smCommentMissing(snapshot, answer?.valueJson as SmVisitAnswerPayload | undefined);
        return !isCompleteSmVisitAnswer({
          type: question.questionTypeSnapshot,
          config: question.configSnapshot,
          options: optionSnapshot(question.answerOptionsSnapshot),
        }, answer?.valueJson as SmVisitAnswerPayload | null | undefined);
      });
      if (missing.length) throw new SmVisitError(409, "sm_visit_required_answers_missing", "Bitte ergänze alle Pflichtantworten und erforderlichen Kommentare.", {
        questionIds: missing.map((question) => question.id),
      });
      const now = new Date();
      const selectedVisitStartedAt = input.visitStartedAt ? new Date(input.visitStartedAt) : null;
      const selectedVisitCompletedAt = input.visitCompletedAt ? new Date(input.visitCompletedAt) : null;
      const effectiveVisitStartedAt = selectedVisitStartedAt ?? submission.visitStartedAt;
      const effectiveVisitCompletedAt = selectedVisitCompletedAt ?? (effectiveVisitStartedAt ? now : null);
      if (!effectiveVisitStartedAt || !effectiveVisitCompletedAt) {
        throw new SmVisitError(409, "sm_visit_timestamps_required", "Bitte trage Start und Ende deines Marktbesuchs ein. Nur mit beiden Uhrzeiten können wir prüfen, dass sich deine Einsätze nicht überschneiden.");
      }
      const elapsedMs = effectiveVisitCompletedAt.getTime() - effectiveVisitStartedAt.getTime();
      if (!Number.isFinite(elapsedMs) || elapsedMs < 60_000 || elapsedMs > 86_400_000) {
        throw new SmVisitError(409, "sm_visit_time_range_invalid", "Die Endzeit muss mindestens eine Minute nach der Startzeit und höchstens 24 Stunden später liegen.");
      }
      const elapsedMinutes = effectiveVisitStartedAt && effectiveVisitCompletedAt
        ? Math.max(1, Math.round((effectiveVisitCompletedAt.getTime() - effectiveVisitStartedAt.getTime()) / 60_000))
        : null;
      const actualMinutes = elapsedMinutes;
      if (!actualMinutes || actualMinutes < 1 || actualMinutes > 1440) throw new SmVisitError(409, "sm_visit_actual_time_missing", "Bitte trage vor dem Abschluss die tatsächliche Besuchszeit ein.");
      if (isSMDurcharbeitExecution(assignment)) {
        const { visit, context } = assignment.SMDurcharbeit;
        if (visit.basisRevision !== context.target.revision || visit.basisSubmissionId !== context.target.latestSubmissionId) {
          throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_basis_changed", "Der überprüfte Monatsstand wurde geändert. Deine Antworten bleiben gespeichert. Bitte den Stand vor dem Abschluss mit der Verwaltung prüfen.");
        }
        const startDate = SMDurcharbeitToday(effectiveVisitStartedAt), endDate = SMDurcharbeitToday(effectiveVisitCompletedAt);
        if (SMDurcharbeitMonth(startDate) !== context.period.month || SMDurcharbeitMonth(endDate) !== context.period.month || startDate < context.campaign.startDate || endDate > context.campaign.endDate) {
          throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_time_month_invalid", "Start und Ende müssen im Kampagnenzeitraum dieses Kalendermonats liegen.");
        }
        await assertSmVisitTimeAvailable(tx, { smUserId: actor.appUserId, SMDurcharbeitVisitId: assignmentId, startedAt: effectiveVisitStartedAt, completedAt: effectiveVisitCompletedAt });
        await saveSMDurcharbeitVisitTime(tx, assignmentId, actor.appUserId, { startedAt: effectiveVisitStartedAt, completedAt: effectiveVisitCompletedAt,
          travelMinutes: submission.travelMinutes ?? 0, reason: "Monatsbesuch abgeschlossen", expectedRevision: 0 });
      } else {
      await assertSmVisitTimeAvailable(tx, { smUserId: actor.appUserId, assignmentId, startedAt: effectiveVisitStartedAt, completedAt: effectiveVisitCompletedAt });
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
      }
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
      if (isSMDurcharbeitExecution(assignment)) {
        await reconcileSMDurcharbeitTarget(tx, assignment.SMDurcharbeit.context.target.id, actor.appUserId, "visit_submitted");
      } else {
      await tx.update(smAssignments).set(buildSmAssignmentCompletionUpdate({
        visitStartedAt: effectiveVisitStartedAt,
        visitCompletedAt: effectiveVisitCompletedAt,
        actorUserId: actor.appUserId,
        updatedAt: now,
      })).where(eq(smAssignments.id, assignmentId));
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
      }
      return { submissionId: submission.id, submittedAt: now.toISOString(), actualMinutes };
    });
    res.json({ receipt });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Die Abschlussdaten sind ungültig.", code: "sm_visit_submit_invalid", details: { issues: error.issues } });
    if (!sendError(error, res)) next(error);
  }
});

return smVisitsRouter;
}
export const smVisitsRouter = createSmVisitsRouter();
export const SMDurcharbeitVisitsRouter = createSmVisitsRouter(true);
