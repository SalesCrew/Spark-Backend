import { randomUUID } from "node:crypto";
import { and, asc, desc, eq, inArray, isNull, sql } from "drizzle-orm";
import { Router, type Response } from "express";
import { z } from "zod";

import { computeHiddenQuestionIds } from "../lib/conditional-visibility.js";
import { db } from "../lib/db.js";
import {
  smAnswerChangeRequests,
  smAssignmentTimeChangeRequests,
  smAssignmentTimeSubmissions,
  smAssignments,
  smMarkets,
  smQuestionAnswerEvents,
  smQuestionAnswerFiles,
  smQuestionAnswerMatrixCells,
  smQuestionAnswerOptions,
  smQuestionAnswers,
  smQuestionnaireSubmissionDeleteRequests,
  smQuestionnaireSubmissionQuestions,
  smQuestionnaireSubmissionSections,
  smQuestionnaireSubmissions,
  users,
} from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { resolveSmAssignmentValues } from "../sm-planning.shared.js";
import {
  isAnsweredSmVisitPayload,
  isCompleteSmVisitAnswer,
  normalizeSmVisitAnswer,
  SmVisitAnswerValidationError,
  smVisitAnswerSchema,
  smVisitAnswerToRuleValue,
  stableSmVisitAnswer,
  type SmVisitAnswerPayload,
  type SmVisitQuestionSnapshot,
} from "../sm-visit.shared.js";

type DbTx = Parameters<Parameters<typeof db.transaction>[0]>[0];
type DbExecutor = typeof db | DbTx;

class SmActivityError extends Error {
  constructor(public readonly statusCode: number, public readonly code: string, message: string, public readonly details?: Record<string, unknown>) {
    super(message);
  }
}

function sendKnownError(error: unknown, res: Response): boolean {
  if (error instanceof SmVisitAnswerValidationError) {
    res.status(400).json({ error: error.message, code: "sm_activity_answer_invalid" });
    return true;
  }
  if (!(error instanceof SmActivityError)) return false;
  res.status(error.statusCode).json({ error: error.message, code: error.code, ...(error.details ? { details: error.details } : {}) });
  return true;
}

function authUser(req: AuthedRequest) {
  if (!req.authUser) throw new SmActivityError(401, "auth_required", "Anmeldung erforderlich.");
  return req.authUser;
}

const uuidSchema = z.string().uuid();
const listSchema = z.object({ limit: z.coerce.number().int().min(1).max(120).default(80) }).partial();
const answerRequestSchema = z.object({
  answer: smVisitAnswerSchema,
  requestedAnswerSummary: z.string().trim().max(1_000).optional(),
  reason: z.string().trim().min(2).max(2_000),
  clientRequestToken: z.string().trim().min(8).max(300),
}).strict();
const deleteRequestSchema = z.object({
  reason: z.string().trim().min(2).max(2_000),
  clientRequestToken: z.string().trim().min(8).max(300),
}).strict();
const reviewSchema = z.object({ adminNote: z.string().trim().max(2_000).optional() }).strict();

function optionSnapshot(value: unknown): Array<{ code: string; label: string; marksNotApplicable?: boolean }> {
  if (!Array.isArray(value)) return [];
  return value.flatMap((entry) => {
    if (!entry || typeof entry !== "object") return [];
    const row = entry as Record<string, unknown>;
    if (typeof row.code !== "string" || typeof row.label !== "string") return [];
    return [{ code: row.code, label: row.label, ...(typeof row.marksNotApplicable === "boolean" ? { marksNotApplicable: row.marksNotApplicable } : {}) }];
  });
}

function questionSnapshot(question: typeof smQuestionnaireSubmissionQuestions.$inferSelect): SmVisitQuestionSnapshot {
  return {
    type: question.questionTypeSnapshot,
    config: question.configSnapshot,
    options: optionSnapshot(question.answerOptionsSnapshot),
  };
}

function answerSummary(answer: SmVisitAnswerPayload, options: Array<{ code: string; label: string }>): string {
  const label = (code: string) => options.find((option) => option.code === code)?.label ?? code;
  if (answer.kind === "empty") return "Antwort entfernen";
  if (answer.kind === "choice") return label(answer.optionCode);
  if (answer.kind === "multi") return answer.optionCodes.map(label).join(", ");
  if (answer.kind === "yesnomulti") return [label(answer.optionCode), ...answer.subOptions].join(": ");
  if (answer.kind === "text") return answer.value.trim().slice(0, 500);
  if (answer.kind === "number") return String(answer.value);
  if (answer.kind === "matrix") return `${answer.cells.filter((cell) => cell.selected).length} Matrixwerte`;
  return `${answer.fileIds.length} Foto${answer.fileIds.length === 1 ? "" : "s"}`;
}

async function answerSnapshot(executor: DbExecutor, answer: typeof smQuestionAnswers.$inferSelect | undefined) {
  if (!answer) return { answerId: null, answerVersion: 0, answerState: "unanswered", value: { kind: "empty" }, options: [], matrixCells: [], files: [] };
  const [options, matrixCells, files] = await Promise.all([
    executor.select().from(smQuestionAnswerOptions).where(and(eq(smQuestionAnswerOptions.answerId, answer.id), eq(smQuestionAnswerOptions.isDeleted, false))).orderBy(asc(smQuestionAnswerOptions.orderIndex)),
    executor.select().from(smQuestionAnswerMatrixCells).where(and(eq(smQuestionAnswerMatrixCells.answerId, answer.id), eq(smQuestionAnswerMatrixCells.isDeleted, false))).orderBy(asc(smQuestionAnswerMatrixCells.orderIndex)),
    executor.select().from(smQuestionAnswerFiles).where(and(eq(smQuestionAnswerFiles.answerId, answer.id), eq(smQuestionAnswerFiles.isDeleted, false))).orderBy(asc(smQuestionAnswerFiles.uploadedAt)),
  ]);
  return {
    answerId: answer.id,
    answerVersion: answer.answerVersion,
    answerState: answer.answerState,
    value: (answer.valueJson ?? { kind: "empty" }) as SmVisitAnswerPayload,
    options: options.map((row) => ({ code: row.optionCodeSnapshot, label: row.optionLabelSnapshot })),
    matrixCells: matrixCells.map((row) => ({ rowCode: row.rowCode, columnCode: row.columnCode, selected: row.selected })),
    files: files.map((row) => ({ id: row.id, fileName: row.originalFileName, mimeType: row.mimeType, byteSize: row.byteSize })),
  };
}

async function currentAnswer(executor: DbExecutor, questionId: string, lock = false) {
  let query = executor.select().from(smQuestionAnswers).where(and(
    eq(smQuestionAnswers.submissionQuestionId, questionId),
    eq(smQuestionAnswers.isCurrent, true),
    eq(smQuestionAnswers.isDeleted, false),
  )).limit(1);
  if (lock) query = query.for("update") as typeof query;
  const [row] = await query;
  return row;
}

async function validateConditionalResult(executor: DbExecutor, submissionId: string, targetQuestionId: string, candidate: SmVisitAnswerPayload) {
  const questions = await executor.select().from(smQuestionnaireSubmissionQuestions).where(and(
    eq(smQuestionnaireSubmissionQuestions.submissionId, submissionId),
    eq(smQuestionnaireSubmissionQuestions.isDeleted, false),
  )).orderBy(asc(smQuestionnaireSubmissionQuestions.orderIndex));
  const answers = await executor.select().from(smQuestionAnswers).where(and(
    eq(smQuestionAnswers.submissionId, submissionId),
    eq(smQuestionAnswers.isCurrent, true),
    eq(smQuestionAnswers.isDeleted, false),
  ));
  const valueByQuestion = new Map(answers.map((row) => [row.submissionQuestionId, (row.valueJson ?? { kind: "empty" }) as SmVisitAnswerPayload]));
  valueByQuestion.set(targetQuestionId, candidate);
  const hidden = computeHiddenQuestionIds(questions.map((question) => ({
    id: question.id,
    questionId: question.questionCodeSnapshot,
    rules: question.logicRulesSnapshot,
  })), new Map(questions.map((question) => [
    question.id,
    smVisitAnswerToRuleValue(valueByQuestion.get(question.id), optionSnapshot(question.answerOptionsSnapshot)),
  ])));
  const missingRequired = questions.filter((question) => !hidden.has(question.id) && question.requiredSnapshot && !isCompleteSmVisitAnswer(questionSnapshot(question), valueByQuestion.get(question.id)));
  return { questions, hidden, missingRequired };
}

async function recomputeConditionalState(tx: DbTx, submissionId: string, actorUserId: string) {
  const questions = await tx.select().from(smQuestionnaireSubmissionQuestions).where(and(
    eq(smQuestionnaireSubmissionQuestions.submissionId, submissionId),
    eq(smQuestionnaireSubmissionQuestions.isDeleted, false),
  ));
  const answers = await tx.select().from(smQuestionAnswers).where(and(
    eq(smQuestionAnswers.submissionId, submissionId),
    eq(smQuestionAnswers.isCurrent, true),
    eq(smQuestionAnswers.isDeleted, false),
  ));
  const answerByQuestion = new Map(answers.map((row) => [row.submissionQuestionId, row]));
  const hidden = computeHiddenQuestionIds(questions.map((question) => ({ id: question.id, questionId: question.questionCodeSnapshot, rules: question.logicRulesSnapshot })), new Map(questions.map((question) => [
    question.id,
    smVisitAnswerToRuleValue((answerByQuestion.get(question.id)?.valueJson ?? { kind: "empty" }) as SmVisitAnswerPayload, optionSnapshot(question.answerOptionsSnapshot)),
  ])));
  const now = new Date();
  for (const question of questions) {
    const applicable = !hidden.has(question.id);
    if (question.isApplicable !== applicable) await tx.update(smQuestionnaireSubmissionQuestions).set({
      isApplicable: applicable,
      applicabilityReason: applicable ? null : "hidden_by_rule_after_admin_correction",
      updatedAt: now,
    }).where(eq(smQuestionnaireSubmissionQuestions.id, question.id));
    const answer = answerByQuestion.get(question.id);
    if (!applicable && answer) {
      await tx.update(smQuestionAnswers).set({
        isCurrent: false,
        answerState: "invalidated",
        invalidatedAt: now,
        invalidatedByUserId: actorUserId,
        invalidationReason: "hidden_by_rule_after_admin_correction",
        updatedAt: now,
      }).where(eq(smQuestionAnswers.id, answer.id));
      await tx.insert(smQuestionAnswerEvents).values({
        answerId: answer.id,
        submissionId,
        eventType: "state_change",
        answerVersion: answer.answerVersion,
        payload: { source: "sm_answer_change_request_approved", from: answer.answerState, to: "invalidated", reason: "hidden_by_rule" },
        actorUserId,
      });
      answerByQuestion.delete(question.id);
    }
  }
  const answeredCount = [...answerByQuestion.values()].filter((row) => row.answerState === "answered").length;
  const earnedPoints = [...answerByQuestion.values()].reduce((sum, row) => sum + Number(row.earnedPoints), 0);
  const possiblePoints = [...answerByQuestion.values()].reduce((sum, row) => sum + Number(row.possiblePoints), 0);
  await tx.update(smQuestionnaireSubmissions).set({ answeredQuestionCount: answeredCount, earnedPoints: String(earnedPoints), possiblePoints: String(possiblePoints), lastSavedAt: now, updatedAt: now }).where(eq(smQuestionnaireSubmissions.id, submissionId));
}

async function publicAnswerRequests(smUserId?: string) {
  const rows = await db.select({
    request: smAnswerChangeRequests,
    questionType: smQuestionnaireSubmissionQuestions.questionTypeSnapshot,
    questionConfig: smQuestionnaireSubmissionQuestions.configSnapshot,
    questionOptions: smQuestionnaireSubmissionQuestions.answerOptionsSnapshot,
    required: smQuestionnaireSubmissionQuestions.requiredSnapshot,
    questionApplicable: smQuestionnaireSubmissionQuestions.isApplicable,
    moduleName: smQuestionnaireSubmissionSections.moduleNameSnapshot,
    assignmentId: smQuestionnaireSubmissions.assignmentId,
    questionnaireName: smQuestionnaireSubmissions.questionnaireNameSnapshot,
    questionnaireVersion: smQuestionnaireSubmissions.questionnaireVersionSnapshot,
    submittedAt: smQuestionnaireSubmissions.submittedAt,
    submissionStatus: smQuestionnaireSubmissions.status,
    submissionDeleted: smQuestionnaireSubmissions.isDeleted,
    smFirstName: users.firstName,
    smLastName: users.lastName,
    smEmail: users.email,
    marketName: smMarkets.name,
    marketAddress: smMarkets.address,
    marketPostalCode: smMarkets.postalCode,
    marketCity: smMarkets.city,
  }).from(smAnswerChangeRequests)
    .innerJoin(smQuestionnaireSubmissions, eq(smQuestionnaireSubmissions.id, smAnswerChangeRequests.submissionId))
    .innerJoin(smQuestionnaireSubmissionQuestions, eq(smQuestionnaireSubmissionQuestions.id, smAnswerChangeRequests.submissionQuestionId))
    .innerJoin(smQuestionnaireSubmissionSections, eq(smQuestionnaireSubmissionSections.id, smQuestionnaireSubmissionQuestions.submissionSectionId))
    .innerJoin(users, eq(users.id, smAnswerChangeRequests.smUserId))
    .innerJoin(smMarkets, eq(smMarkets.id, smAnswerChangeRequests.smMarketId))
    .where(and(eq(smAnswerChangeRequests.isDeleted, false), ...(smUserId ? [eq(smAnswerChangeRequests.smUserId, smUserId)] : [])))
    .orderBy(sql`case when ${smAnswerChangeRequests.status} = 'pending' then 0 else 1 end`, desc(smAnswerChangeRequests.updatedAt))
    .limit(250);
  return Promise.all(rows.map(async (row) => {
    let autoApplicable = row.request.status !== "pending" || (!row.submissionDeleted && row.submissionStatus === "submitted" && row.questionApplicable);
    let autoApplicabilityError: string | null = autoApplicable ? null : "Der Fragebogen oder die Frage ist nicht mehr aktiv.";
    if (row.request.status === "pending" && autoApplicable) {
      try {
        const current = await currentAnswer(db, row.request.submissionQuestionId);
        if ((current?.id ?? null) !== row.request.originalAnswerId) {
          autoApplicable = false;
          autoApplicabilityError = "Die gespeicherte Antwort hat sich seit der Anfrage geändert.";
        } else {
          const normalized = normalizeSmVisitAnswer({ type: row.questionType, config: row.questionConfig, options: optionSnapshot(row.questionOptions) }, row.request.requestedAnswerPayload);
          const conditional = await validateConditionalResult(db, row.request.submissionId, row.request.submissionQuestionId, normalized);
          if (conditional.missingRequired.length) {
            autoApplicable = false;
            autoApplicabilityError = `Die Änderung würde ${conditional.missingRequired.length} Pflichtfrage${conditional.missingRequired.length === 1 ? "" : "n"} unbeantwortet lassen.`;
          }
        }
      } catch (error) {
        autoApplicable = false;
        autoApplicabilityError = error instanceof Error ? error.message : "Die Anfrage kann nicht automatisch angewendet werden.";
      }
    }
    return {
      id: row.request.id,
      status: row.request.status,
      createdAt: row.request.createdAt.toISOString(),
      updatedAt: row.request.updatedAt.toISOString(),
      reviewedAt: row.request.reviewedAt?.toISOString() ?? null,
      adminNote: row.request.adminNote,
      appliedAnswerId: row.request.appliedAnswerId,
      appliedAt: row.request.appliedAt?.toISOString() ?? null,
      submissionId: row.request.submissionId,
      submissionQuestionId: row.request.submissionQuestionId,
      originalAnswerId: row.request.originalAnswerId,
      questionText: row.request.questionTextSnapshot,
      questionType: row.questionType,
      questionConfig: row.questionConfig,
      questionOptions: optionSnapshot(row.questionOptions),
      required: row.required,
      originalAnswerSnapshot: row.request.originalAnswerSnapshot,
      requestedAnswerPayload: row.request.requestedAnswerPayload,
      requestedAnswerSummary: row.request.requestedAnswerSummary,
      requestReason: row.request.requestReason,
      autoApplicable,
      autoApplicabilityError,
      sm: { id: row.request.smUserId, name: `${row.smFirstName} ${row.smLastName}`.trim() || row.smEmail, email: row.smEmail },
      market: { id: row.request.smMarketId, name: row.marketName, address: row.marketAddress, postalCode: row.marketPostalCode, city: row.marketCity },
      submission: { assignmentId: row.assignmentId, questionnaireName: row.questionnaireName, questionnaireVersion: row.questionnaireVersion, submittedAt: row.submittedAt?.toISOString() ?? null, moduleName: row.moduleName },
    };
  }));
}

async function publicDeleteRequests(smUserId?: string) {
  const rows = await db.select({ request: smQuestionnaireSubmissionDeleteRequests, smFirstName: users.firstName, smLastName: users.lastName, smEmail: users.email })
    .from(smQuestionnaireSubmissionDeleteRequests)
    .innerJoin(users, eq(users.id, smQuestionnaireSubmissionDeleteRequests.smUserId))
    .where(and(eq(smQuestionnaireSubmissionDeleteRequests.isDeleted, false), ...(smUserId ? [eq(smQuestionnaireSubmissionDeleteRequests.smUserId, smUserId)] : [])))
    .orderBy(sql`case when ${smQuestionnaireSubmissionDeleteRequests.status} = 'pending' then 0 else 1 end`, desc(smQuestionnaireSubmissionDeleteRequests.updatedAt))
    .limit(250);
  return rows.map((row) => ({
    id: row.request.id,
    status: row.request.status,
    createdAt: row.request.createdAt.toISOString(),
    updatedAt: row.request.updatedAt.toISOString(),
    reviewedAt: row.request.reviewedAt?.toISOString() ?? null,
    adminNote: row.request.adminNote,
    appliedAt: row.request.appliedAt?.toISOString() ?? null,
    submissionId: row.request.submissionId,
    requestReason: row.request.requestReason,
    questionnaireName: row.request.questionnaireNameSnapshot,
    questionnaireVersion: row.request.questionnaireVersionSnapshot,
    market: { id: row.request.smMarketId, name: row.request.marketNameSnapshot },
    submittedAt: row.request.submittedAtSnapshot?.toISOString() ?? null,
    sm: { id: row.request.smUserId, name: `${row.smFirstName} ${row.smLastName}`.trim() || row.smEmail, email: row.smEmail },
  }));
}

async function publicTimeRequests() {
  const rows = await db.select({ request: smAssignmentTimeChangeRequests, assignment: smAssignments, smFirstName: users.firstName, smLastName: users.lastName, smEmail: users.email })
    .from(smAssignmentTimeChangeRequests)
    .innerJoin(smAssignments, eq(smAssignments.id, smAssignmentTimeChangeRequests.assignmentId))
    .innerJoin(users, eq(users.id, smAssignmentTimeChangeRequests.smUserId))
    .where(eq(smAssignmentTimeChangeRequests.isDeleted, false))
    .orderBy(sql`case when ${smAssignmentTimeChangeRequests.status} = 'pending' then 0 else 1 end`, desc(smAssignmentTimeChangeRequests.updatedAt))
    .limit(250);
  const effectiveMarketIds = [...new Set(rows.map((row) => resolveSmAssignmentValues(row.assignment).smMarketId))];
  const marketRows = effectiveMarketIds.length
    ? await db.select({ id: smMarkets.id, name: smMarkets.name }).from(smMarkets).where(inArray(smMarkets.id, effectiveMarketIds))
    : [];
  const marketNameById = new Map(marketRows.map((market) => [market.id, market.name]));
  return rows.map((row) => {
    const effective = resolveSmAssignmentValues(row.assignment);
    return {
      id: row.request.id,
      status: row.request.status,
      createdAt: row.request.createdAt.toISOString(),
      updatedAt: row.request.updatedAt.toISOString(),
      reviewedAt: row.request.reviewedAt?.toISOString() ?? null,
      adminNote: row.request.adminNote,
      assignmentId: row.request.assignmentId,
      kind: row.request.requestKind,
      originalMinutes: row.request.originalMinutes,
      requestedMinutes: row.request.requestedMinutes,
      timestampCorrectionVersion: row.request.timestampCorrectionVersion,
      originalStartedAt: row.request.originalStartedAt?.toISOString() ?? null,
      originalCompletedAt: row.request.originalCompletedAt?.toISOString() ?? null,
      requestedStartedAt: row.request.requestedStartedAt?.toISOString() ?? null,
      requestedCompletedAt: row.request.requestedCompletedAt?.toISOString() ?? null,
      requestReason: row.request.requestReason,
      workDate: effective.workDate,
      sm: { id: row.request.smUserId, name: `${row.smFirstName} ${row.smLastName}`.trim() || row.smEmail, email: row.smEmail },
      market: { id: effective.smMarketId, name: marketNameById.get(effective.smMarketId) ?? effective.marketInternalId },
    };
  });
}

export const smActivityRouter = Router();
smActivityRouter.use(requireAuth(["sm"]));

smActivityRouter.get("/completed", async (req: AuthedRequest, res, next) => {
  try {
    const actor = authUser(req);
    const limit = listSchema.parse(req.query).limit ?? 80;
    const rows = await db.select({ submission: smQuestionnaireSubmissions, assignment: smAssignments, market: smMarkets, actualMinutes: smAssignmentTimeSubmissions.actualMinutes })
      .from(smQuestionnaireSubmissions)
      .innerJoin(smAssignments, eq(smAssignments.id, smQuestionnaireSubmissions.assignmentId))
      .innerJoin(smMarkets, eq(smMarkets.id, smQuestionnaireSubmissions.smMarketId))
      .leftJoin(smAssignmentTimeSubmissions, and(eq(smAssignmentTimeSubmissions.assignmentId, smAssignments.id), eq(smAssignmentTimeSubmissions.isCurrent, true), eq(smAssignmentTimeSubmissions.isDeleted, false)))
      .where(and(eq(smQuestionnaireSubmissions.smUserId, actor.appUserId), eq(smQuestionnaireSubmissions.status, "submitted"), eq(smQuestionnaireSubmissions.isCurrent, true), eq(smQuestionnaireSubmissions.isDeleted, false)))
      .orderBy(desc(smQuestionnaireSubmissions.submittedAt), desc(smQuestionnaireSubmissions.createdAt))
      .limit(limit);
    const submissionIds = rows.map((row) => row.submission.id);
    const questionCounts = submissionIds.length ? await db.select({ submissionId: smQuestionnaireSubmissionQuestions.submissionId, count: sql<number>`count(*)::int` }).from(smQuestionnaireSubmissionQuestions).where(and(inArray(smQuestionnaireSubmissionQuestions.submissionId, submissionIds), eq(smQuestionnaireSubmissionQuestions.isDeleted, false))).groupBy(smQuestionnaireSubmissionQuestions.submissionId) : [];
    const answerCounts = submissionIds.length ? await db.select({ submissionId: smQuestionAnswers.submissionId, count: sql<number>`count(*) filter (where ${smQuestionAnswers.answerState} = 'answered')::int` }).from(smQuestionAnswers).where(and(inArray(smQuestionAnswers.submissionId, submissionIds), eq(smQuestionAnswers.isCurrent, true), eq(smQuestionAnswers.isDeleted, false))).groupBy(smQuestionAnswers.submissionId) : [];
    const photoCounts = submissionIds.length ? await db.select({ submissionId: smQuestionAnswers.submissionId, count: sql<number>`count(${smQuestionAnswerFiles.id})::int` }).from(smQuestionAnswers).innerJoin(smQuestionAnswerFiles, and(eq(smQuestionAnswerFiles.answerId, smQuestionAnswers.id), eq(smQuestionAnswerFiles.isDeleted, false))).where(and(inArray(smQuestionAnswers.submissionId, submissionIds), eq(smQuestionAnswers.isCurrent, true), eq(smQuestionAnswers.isDeleted, false))).groupBy(smQuestionAnswers.submissionId) : [];
    const questionsBySubmission = new Map(questionCounts.map((row) => [row.submissionId, Number(row.count)]));
    const answersBySubmission = new Map(answerCounts.map((row) => [row.submissionId, Number(row.count)]));
    const photosBySubmission = new Map(photoCounts.map((row) => [row.submissionId, Number(row.count)]));
    res.json({ visits: rows.map((row) => {
      const effective = resolveSmAssignmentValues(row.assignment);
      return {
        submissionId: row.submission.id,
        assignmentId: row.assignment.id,
        workDate: effective.workDate,
        plannedMinutes: effective.plannedMinutes,
        actualMinutes: row.actualMinutes ?? null,
        questionnaireName: row.submission.questionnaireNameSnapshot,
        questionnaireVersion: row.submission.questionnaireVersionSnapshot,
        market: { id: row.market.id, name: row.submission.marketNameSnapshot, internalId: row.market.internalMarketId, address: row.submission.marketAddressSnapshot, postalCode: row.submission.marketPostalCodeSnapshot, city: row.submission.marketCitySnapshot },
        visitStartedAt: row.submission.visitStartedAt?.toISOString() ?? null,
        visitCompletedAt: row.submission.visitCompletedAt?.toISOString() ?? null,
        submittedAt: row.submission.submittedAt?.toISOString() ?? null,
        totals: { questionCount: questionsBySubmission.get(row.submission.id) ?? 0, answeredCount: answersBySubmission.get(row.submission.id) ?? 0, photoCount: photosBySubmission.get(row.submission.id) ?? 0 },
      };
    }) });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Ungültige Aktivitätsabfrage.", code: "sm_activity_query_invalid" });
    next(error);
  }
});

smActivityRouter.get("/requests", async (req: AuthedRequest, res, next) => {
  try {
    const actor = authUser(req);
    const [answerRequests, deleteRequests] = await Promise.all([publicAnswerRequests(actor.appUserId), publicDeleteRequests(actor.appUserId)]);
    res.json({ answerRequests, deleteRequests });
  } catch (error) { next(error); }
});

smActivityRouter.post("/submissions/:submissionId/questions/:questionId/change-requests", async (req: AuthedRequest, res, next) => {
  try {
    const actor = authUser(req);
    const submissionId = uuidSchema.parse(req.params.submissionId);
    const questionId = uuidSchema.parse(req.params.questionId);
    const input = answerRequestSchema.parse(req.body);
    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_answer_request_token:${actor.appUserId}:${input.clientRequestToken}`}, 0))`);
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_answer_request:${questionId}`}, 0))`);
      const [replayed] = await tx.select().from(smAnswerChangeRequests).where(and(eq(smAnswerChangeRequests.smUserId, actor.appUserId), eq(smAnswerChangeRequests.clientRequestToken, input.clientRequestToken), eq(smAnswerChangeRequests.isDeleted, false))).limit(1);
      if (replayed) {
        if (replayed.submissionId !== submissionId || replayed.submissionQuestionId !== questionId) {
          throw new SmActivityError(409, "sm_activity_request_token_conflict", "Dieser Anfrage-Token wurde bereits für eine andere Antwort verwendet.");
        }
        const [replayQuestion] = await tx.select().from(smQuestionnaireSubmissionQuestions).where(and(eq(smQuestionnaireSubmissionQuestions.id, questionId), eq(smQuestionnaireSubmissionQuestions.submissionId, submissionId))).limit(1);
        const normalizedReplay = replayQuestion ? normalizeSmVisitAnswer(questionSnapshot(replayQuestion), input.answer) : input.answer;
        if (stableSmVisitAnswer(replayed.requestedAnswerPayload as SmVisitAnswerPayload) !== stableSmVisitAnswer(normalizedReplay) || replayed.requestReason !== input.reason) {
          throw new SmActivityError(409, "sm_activity_request_token_conflict", "Dieser Anfrage-Token wurde bereits mit anderen Änderungsdaten verwendet.");
        }
        return { row: replayed, replayed: true };
      }
      const [submission] = await tx.select().from(smQuestionnaireSubmissions).where(and(eq(smQuestionnaireSubmissions.id, submissionId), eq(smQuestionnaireSubmissions.smUserId, actor.appUserId), eq(smQuestionnaireSubmissions.status, "submitted"), eq(smQuestionnaireSubmissions.isCurrent, true), eq(smQuestionnaireSubmissions.isDeleted, false))).limit(1).for("update");
      if (!submission) throw new SmActivityError(404, "sm_activity_submission_not_found", "Der abgeschlossene Fragebogen wurde nicht gefunden.");
      const [question] = await tx.select().from(smQuestionnaireSubmissionQuestions).where(and(eq(smQuestionnaireSubmissionQuestions.id, questionId), eq(smQuestionnaireSubmissionQuestions.submissionId, submission.id), eq(smQuestionnaireSubmissionQuestions.isApplicable, true), eq(smQuestionnaireSubmissionQuestions.isDeleted, false))).limit(1).for("update");
      if (!question) throw new SmActivityError(404, "sm_activity_question_not_found", "Die Frage wurde nicht gefunden oder ist nicht anwendbar.");
      const [pending] = await tx.select().from(smAnswerChangeRequests).where(and(eq(smAnswerChangeRequests.submissionQuestionId, question.id), eq(smAnswerChangeRequests.status, "pending"), eq(smAnswerChangeRequests.isDeleted, false))).limit(1).for("update");
      if (pending) return { row: pending, replayed: true };
      const normalized = normalizeSmVisitAnswer(questionSnapshot(question), input.answer);
      if (question.requiredSnapshot && !isCompleteSmVisitAnswer(questionSnapshot(question), normalized)) throw new SmActivityError(400, "sm_activity_required_answer_empty", "Eine Pflichtfrage kann nicht geleert werden.");
      const original = await currentAnswer(tx, question.id, true);
      const originalValue = (original?.valueJson ?? { kind: "empty" }) as SmVisitAnswerPayload;
      if (stableSmVisitAnswer(originalValue) === stableSmVisitAnswer(normalized)) throw new SmActivityError(400, "sm_activity_answer_unchanged", "Die gewünschte Antwort ist bereits gespeichert.");
      if (normalized.kind === "photo") {
        const currentFiles = original ? await tx.select({ id: smQuestionAnswerFiles.id }).from(smQuestionAnswerFiles).where(and(eq(smQuestionAnswerFiles.answerId, original.id), eq(smQuestionAnswerFiles.isDeleted, false))) : [];
        const allowed = new Set(currentFiles.map((row) => row.id));
        if (normalized.fileIds.some((id) => !allowed.has(id))) throw new SmActivityError(400, "sm_activity_photo_request_invalid", "Foto-Anfragen dürfen nur bereits gespeicherte Fotos behalten oder entfernen.");
      }
      const conditional = await validateConditionalResult(tx, submission.id, question.id, normalized);
      if (conditional.missingRequired.length) throw new SmActivityError(409, "sm_activity_request_required_side_effect", "Die Änderung würde eine Pflichtfrage unbeantwortet lassen.", { questionIds: conditional.missingRequired.map((row) => row.id) });
      const snapshot = await answerSnapshot(tx, original);
      const [created] = await tx.insert(smAnswerChangeRequests).values({
        submissionId: submission.id,
        submissionQuestionId: question.id,
        originalAnswerId: original?.id ?? null,
        smUserId: actor.appUserId,
        smMarketId: submission.smMarketId,
        questionTextSnapshot: question.questionTextSnapshot,
        originalAnswerSnapshot: snapshot,
        requestedAnswerPayload: normalized,
        requestedAnswerSummary: answerSummary(normalized, optionSnapshot(question.answerOptionsSnapshot)),
        requestReason: input.reason,
        clientRequestToken: input.clientRequestToken,
      }).returning();
      if (!created) throw new SmActivityError(500, "sm_activity_request_write_failed", "Die Anfrage konnte nicht gespeichert werden.");
      return { row: created, replayed: false };
    });
    res.status(result.replayed ? 200 : 201).json({ request: { id: result.row.id, status: result.row.status, createdAt: result.row.createdAt.toISOString(), updatedAt: result.row.updatedAt.toISOString() }, replayed: result.replayed });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Die Änderungsanfrage ist ungültig.", code: "sm_activity_request_invalid", details: { issues: error.issues } });
    if (!sendKnownError(error, res)) next(error);
  }
});

smActivityRouter.post("/submissions/:submissionId/delete-requests", async (req: AuthedRequest, res, next) => {
  try {
    const actor = authUser(req);
    const submissionId = uuidSchema.parse(req.params.submissionId);
    const input = deleteRequestSchema.parse(req.body);
    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_submission_delete_request_token:${actor.appUserId}:${input.clientRequestToken}`}, 0))`);
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_submission_delete_request:${submissionId}`}, 0))`);
      const [replayed] = await tx.select().from(smQuestionnaireSubmissionDeleteRequests).where(and(eq(smQuestionnaireSubmissionDeleteRequests.smUserId, actor.appUserId), eq(smQuestionnaireSubmissionDeleteRequests.clientRequestToken, input.clientRequestToken), eq(smQuestionnaireSubmissionDeleteRequests.isDeleted, false))).limit(1);
      if (replayed) {
        if (replayed.submissionId !== submissionId) {
          throw new SmActivityError(409, "sm_activity_delete_request_token_conflict", "Dieser Anfrage-Token wurde bereits für einen anderen Fragebogen verwendet.");
        }
        if (replayed.requestReason !== input.reason) {
          throw new SmActivityError(409, "sm_activity_delete_request_token_conflict", "Dieser Anfrage-Token wurde bereits mit einem anderen Löschgrund verwendet.");
        }
        return { row: replayed, replayed: true };
      }
      const [submission] = await tx.select().from(smQuestionnaireSubmissions).where(and(eq(smQuestionnaireSubmissions.id, submissionId), eq(smQuestionnaireSubmissions.smUserId, actor.appUserId), eq(smQuestionnaireSubmissions.status, "submitted"), eq(smQuestionnaireSubmissions.isCurrent, true), eq(smQuestionnaireSubmissions.isDeleted, false))).limit(1).for("update");
      if (!submission) throw new SmActivityError(404, "sm_activity_submission_not_found", "Der abgeschlossene Fragebogen wurde nicht gefunden.");
      const [pending] = await tx.select().from(smQuestionnaireSubmissionDeleteRequests).where(and(eq(smQuestionnaireSubmissionDeleteRequests.submissionId, submission.id), eq(smQuestionnaireSubmissionDeleteRequests.status, "pending"), eq(smQuestionnaireSubmissionDeleteRequests.isDeleted, false))).limit(1).for("update");
      if (pending) return { row: pending, replayed: true };
      const [created] = await tx.insert(smQuestionnaireSubmissionDeleteRequests).values({
        submissionId: submission.id,
        smUserId: actor.appUserId,
        smMarketId: submission.smMarketId,
        questionnaireNameSnapshot: submission.questionnaireNameSnapshot,
        questionnaireVersionSnapshot: submission.questionnaireVersionSnapshot,
        marketNameSnapshot: submission.marketNameSnapshot,
        submittedAtSnapshot: submission.submittedAt,
        requestReason: input.reason,
        clientRequestToken: input.clientRequestToken,
      }).returning();
      if (!created) throw new SmActivityError(500, "sm_activity_delete_request_write_failed", "Die Löschanfrage konnte nicht gespeichert werden.");
      return { row: created, replayed: false };
    });
    res.status(result.replayed ? 200 : 201).json({ request: { id: result.row.id, status: result.row.status, createdAt: result.row.createdAt.toISOString(), updatedAt: result.row.updatedAt.toISOString() }, replayed: result.replayed });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Die Löschanfrage ist ungültig.", code: "sm_activity_delete_request_invalid" });
    if (!sendKnownError(error, res)) next(error);
  }
});

export const adminSmActivityRouter = Router();
adminSmActivityRouter.use(requireAuth(["admin", "sm_admin"]));

adminSmActivityRouter.get("/requests", async (_req, res, next) => {
  try {
    const [answerRequests, deleteRequests, timeRequests] = await Promise.all([publicAnswerRequests(), publicDeleteRequests(), publicTimeRequests()]);
    res.json({ answerRequests, deleteRequests, timeRequests });
  } catch (error) { next(error); }
});

adminSmActivityRouter.post("/answer-change-requests/:requestId/reject", async (req: AuthedRequest, res, next) => {
  try {
    const actor = authUser(req);
    const requestId = uuidSchema.parse(req.params.requestId);
    const input = reviewSchema.parse(req.body ?? {});
    const result = await db.transaction(async (tx) => {
      const [identity] = await tx.select({ submissionId: smAnswerChangeRequests.submissionId, submissionQuestionId: smAnswerChangeRequests.submissionQuestionId }).from(smAnswerChangeRequests).where(and(eq(smAnswerChangeRequests.id, requestId), eq(smAnswerChangeRequests.isDeleted, false))).limit(1);
      if (!identity) throw new SmActivityError(404, "sm_activity_request_not_found", "Die Anfrage wurde nicht gefunden.");
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_answer_request:${identity.submissionQuestionId}`}, 0))`);
      await tx.select({ id: smQuestionnaireSubmissions.id }).from(smQuestionnaireSubmissions).where(eq(smQuestionnaireSubmissions.id, identity.submissionId)).limit(1).for("update");
      await tx.select({ id: smQuestionnaireSubmissionQuestions.id }).from(smQuestionnaireSubmissionQuestions).where(eq(smQuestionnaireSubmissionQuestions.id, identity.submissionQuestionId)).limit(1).for("update");
      const [request] = await tx.select().from(smAnswerChangeRequests).where(and(eq(smAnswerChangeRequests.id, requestId), eq(smAnswerChangeRequests.isDeleted, false))).limit(1).for("update");
      if (!request) throw new SmActivityError(404, "sm_activity_request_not_found", "Die Anfrage wurde nicht gefunden.");
      if (request.status === "rejected") return { row: request, replayed: true };
      if (request.status !== "pending") throw new SmActivityError(409, "sm_activity_request_closed", "Die Anfrage wurde bereits bearbeitet.");
      const now = new Date();
      const [updated] = await tx.update(smAnswerChangeRequests).set({ status: "rejected", reviewedByUserId: actor.appUserId, reviewedAt: now, adminNote: input.adminNote?.trim() || null, updatedAt: now }).where(eq(smAnswerChangeRequests.id, request.id)).returning();
      return { row: updated!, replayed: false };
    });
    res.json({ request: { id: result.row.id, status: result.row.status }, replayed: result.replayed });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Ungültige Entscheidung.", code: "sm_activity_review_invalid" });
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmActivityRouter.post("/answer-change-requests/:requestId/approve", async (req: AuthedRequest, res, next) => {
  try {
    const actor = authUser(req);
    const requestId = uuidSchema.parse(req.params.requestId);
    const input = reviewSchema.parse(req.body ?? {});
    const result = await db.transaction(async (tx) => {
      const [identity] = await tx.select({ submissionId: smAnswerChangeRequests.submissionId, submissionQuestionId: smAnswerChangeRequests.submissionQuestionId }).from(smAnswerChangeRequests).where(and(eq(smAnswerChangeRequests.id, requestId), eq(smAnswerChangeRequests.isDeleted, false))).limit(1);
      if (!identity) throw new SmActivityError(404, "sm_activity_request_not_found", "Die Anfrage wurde nicht gefunden.");
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_answer_request:${identity.submissionQuestionId}`}, 0))`);
      const [submission] = await tx.select().from(smQuestionnaireSubmissions).where(eq(smQuestionnaireSubmissions.id, identity.submissionId)).limit(1).for("update");
      await tx.select({ id: smQuestionnaireSubmissionQuestions.id }).from(smQuestionnaireSubmissionQuestions).where(eq(smQuestionnaireSubmissionQuestions.id, identity.submissionQuestionId)).limit(1).for("update");
      const [request] = await tx.select().from(smAnswerChangeRequests).where(and(eq(smAnswerChangeRequests.id, requestId), eq(smAnswerChangeRequests.isDeleted, false))).limit(1).for("update");
      if (!request) throw new SmActivityError(404, "sm_activity_request_not_found", "Die Anfrage wurde nicht gefunden.");
      if (request.status === "approved") return { row: request, replayed: true };
      if (request.status !== "pending") throw new SmActivityError(409, "sm_activity_request_closed", "Die Anfrage wurde bereits bearbeitet.");
      if (!submission || submission.status !== "submitted" || !submission.isCurrent || submission.isDeleted) throw new SmActivityError(409, "sm_activity_submission_stale", "Der Fragebogen ist nicht mehr als aktive Einreichung verfügbar.");
      const [question] = await tx.select().from(smQuestionnaireSubmissionQuestions).where(and(eq(smQuestionnaireSubmissionQuestions.id, request.submissionQuestionId), eq(smQuestionnaireSubmissionQuestions.submissionId, submission.id), eq(smQuestionnaireSubmissionQuestions.isApplicable, true), eq(smQuestionnaireSubmissionQuestions.isDeleted, false))).limit(1).for("update");
      if (!question) throw new SmActivityError(409, "sm_activity_question_stale", "Die Frage ist nicht mehr anwendbar.");
      const current = await currentAnswer(tx, question.id, true);
      if ((current?.id ?? null) !== request.originalAnswerId) throw new SmActivityError(409, "sm_activity_request_stale", "Die gespeicherte Antwort hat sich seit der Anfrage geändert. Bitte lehne die veraltete Anfrage ab.");
      const normalized = normalizeSmVisitAnswer(questionSnapshot(question), request.requestedAnswerPayload);
      if (question.requiredSnapshot && !isCompleteSmVisitAnswer(questionSnapshot(question), normalized)) throw new SmActivityError(400, "sm_activity_required_answer_empty", "Eine Pflichtfrage kann nicht geleert werden.");
      const conditional = await validateConditionalResult(tx, submission.id, question.id, normalized);
      if (conditional.missingRequired.length) throw new SmActivityError(409, "sm_activity_request_required_side_effect", "Die Änderung würde eine Pflichtfrage unbeantwortet lassen.", { questionIds: conditional.missingRequired.map((row) => row.id) });
      const currentVersion = current?.answerVersion ?? 0;
      const now = new Date();
      if (current) await tx.update(smQuestionAnswers).set({ isCurrent: false, updatedAt: now }).where(eq(smQuestionAnswers.id, current.id));
      const answerId = randomUUID();
      const answered = isAnsweredSmVisitPayload(normalized);
      await tx.insert(smQuestionAnswers).values({
        id: answerId,
        submissionId: submission.id,
        submissionQuestionId: question.id,
        supersedesAnswerId: current?.id ?? null,
        answerVersion: currentVersion + 1,
        isCurrent: true,
        answerState: answered ? "answered" : "unanswered",
        valueText: normalized.kind === "text" ? normalized.value : null,
        valueNumber: normalized.kind === "number" ? String(normalized.value) : null,
        valueJson: normalized,
        answeredByUserId: actor.appUserId,
        answeredAt: answered ? now : null,
      });
      const selectedCodes = normalized.kind === "choice" ? [normalized.optionCode] : normalized.kind === "multi" ? normalized.optionCodes : normalized.kind === "yesnomulti" ? [normalized.optionCode] : [];
      if (selectedCodes.length) {
        const sourceOptions = Array.isArray(question.answerOptionsSnapshot) ? question.answerOptionsSnapshot as Array<Record<string, unknown>> : [];
        await tx.insert(smQuestionAnswerOptions).values(selectedCodes.map((code, orderIndex) => {
          const option = sourceOptions.find((entry) => entry.code === code) ?? {};
          return { answerId, answerOptionVersionId: typeof option.id === "string" ? option.id : null, optionCodeSnapshot: code, optionLabelSnapshot: typeof option.label === "string" ? option.label : code, earnedPointsSnapshot: typeof option.earnedPoints === "string" ? option.earnedPoints : "0", possiblePointsSnapshot: typeof option.possiblePoints === "string" ? option.possiblePoints : "0", metricOutcomeCodeSnapshot: typeof option.metricOutcomeCode === "string" ? option.metricOutcomeCode : null, orderIndex };
        }));
      }
      if (normalized.kind === "matrix" && normalized.cells.length) await tx.insert(smQuestionAnswerMatrixCells).values(normalized.cells.map((cell, orderIndex) => ({ answerId, rowCode: cell.rowCode, columnCode: cell.columnCode, selected: cell.selected, orderIndex })));
      if (normalized.kind === "photo") {
        if (!current) throw new SmActivityError(409, "sm_activity_photo_request_stale", "Die ursprünglichen Fotos wurden nicht gefunden.");
        const sourceFiles = await tx.select().from(smQuestionAnswerFiles).where(and(eq(smQuestionAnswerFiles.answerId, current.id), inArray(smQuestionAnswerFiles.id, normalized.fileIds), eq(smQuestionAnswerFiles.isDeleted, false)));
        if (sourceFiles.length !== normalized.fileIds.length) throw new SmActivityError(409, "sm_activity_photo_request_stale", "Mindestens ein Foto ist nicht mehr verfügbar.");
        await tx.insert(smQuestionAnswerFiles).values(sourceFiles.map((file) => ({ answerId, storageBucket: file.storageBucket, storagePath: file.storagePath, originalFileName: file.originalFileName, mimeType: file.mimeType, byteSize: file.byteSize, widthPx: file.widthPx, heightPx: file.heightPx, sha256: file.sha256, uploadedAt: file.uploadedAt })));
      }
      await tx.insert(smQuestionAnswerEvents).values({ answerId, submissionId: submission.id, eventType: answered ? "set" : "clear", answerVersion: currentVersion + 1, payload: { source: "sm_answer_change_request_approved", requestId: request.id, originalAnswerId: current?.id ?? null }, actorUserId: actor.appUserId });
      await recomputeConditionalState(tx, submission.id, actor.appUserId);
      const [updated] = await tx.update(smAnswerChangeRequests).set({ status: "approved", reviewedByUserId: actor.appUserId, reviewedAt: now, adminNote: input.adminNote?.trim() || null, appliedAnswerId: answerId, appliedAt: now, updatedAt: now }).where(eq(smAnswerChangeRequests.id, request.id)).returning();
      return { row: updated!, replayed: false };
    });
    res.json({ request: { id: result.row.id, status: result.row.status, appliedAnswerId: result.row.appliedAnswerId }, replayed: result.replayed });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Ungültige Entscheidung.", code: "sm_activity_review_invalid" });
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmActivityRouter.post("/submission-delete-requests/:requestId/reject", async (req: AuthedRequest, res, next) => {
  try {
    const actor = authUser(req);
    const requestId = uuidSchema.parse(req.params.requestId);
    const input = reviewSchema.parse(req.body ?? {});
    const result = await db.transaction(async (tx) => {
      const [identity] = await tx.select({ submissionId: smQuestionnaireSubmissionDeleteRequests.submissionId }).from(smQuestionnaireSubmissionDeleteRequests).where(and(eq(smQuestionnaireSubmissionDeleteRequests.id, requestId), eq(smQuestionnaireSubmissionDeleteRequests.isDeleted, false))).limit(1);
      if (!identity) throw new SmActivityError(404, "sm_activity_delete_request_not_found", "Die Löschanfrage wurde nicht gefunden.");
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_submission_delete_request:${identity.submissionId}`}, 0))`);
      await tx.select({ id: smQuestionnaireSubmissions.id }).from(smQuestionnaireSubmissions).where(eq(smQuestionnaireSubmissions.id, identity.submissionId)).limit(1).for("update");
      const [request] = await tx.select().from(smQuestionnaireSubmissionDeleteRequests).where(and(eq(smQuestionnaireSubmissionDeleteRequests.id, requestId), eq(smQuestionnaireSubmissionDeleteRequests.isDeleted, false))).limit(1).for("update");
      if (!request) throw new SmActivityError(404, "sm_activity_delete_request_not_found", "Die Löschanfrage wurde nicht gefunden.");
      if (request.status === "rejected") return { row: request, replayed: true };
      if (request.status !== "pending") throw new SmActivityError(409, "sm_activity_request_closed", "Die Anfrage wurde bereits bearbeitet.");
      const now = new Date();
      const [updated] = await tx.update(smQuestionnaireSubmissionDeleteRequests).set({ status: "rejected", reviewedByUserId: actor.appUserId, reviewedAt: now, adminNote: input.adminNote?.trim() || null, updatedAt: now }).where(eq(smQuestionnaireSubmissionDeleteRequests.id, request.id)).returning();
      return { row: updated!, replayed: false };
    });
    res.json({ request: { id: result.row.id, status: result.row.status }, replayed: result.replayed });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Ungültige Entscheidung.", code: "sm_activity_review_invalid" });
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmActivityRouter.post("/submission-delete-requests/:requestId/approve", async (req: AuthedRequest, res, next) => {
  try {
    const actor = authUser(req);
    const requestId = uuidSchema.parse(req.params.requestId);
    const input = reviewSchema.parse(req.body ?? {});
    const result = await db.transaction(async (tx) => {
      const [identity] = await tx.select({ submissionId: smQuestionnaireSubmissionDeleteRequests.submissionId }).from(smQuestionnaireSubmissionDeleteRequests).where(and(eq(smQuestionnaireSubmissionDeleteRequests.id, requestId), eq(smQuestionnaireSubmissionDeleteRequests.isDeleted, false))).limit(1);
      if (!identity) throw new SmActivityError(404, "sm_activity_delete_request_not_found", "Die Löschanfrage wurde nicht gefunden.");
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_submission_delete_request:${identity.submissionId}`}, 0))`);
      const [submission] = await tx.select().from(smQuestionnaireSubmissions).where(eq(smQuestionnaireSubmissions.id, identity.submissionId)).limit(1).for("update");
      const [request] = await tx.select().from(smQuestionnaireSubmissionDeleteRequests).where(and(eq(smQuestionnaireSubmissionDeleteRequests.id, requestId), eq(smQuestionnaireSubmissionDeleteRequests.isDeleted, false))).limit(1).for("update");
      if (!request) throw new SmActivityError(404, "sm_activity_delete_request_not_found", "Die Löschanfrage wurde nicht gefunden.");
      if (request.status === "approved") return { row: request, replayed: true };
      if (request.status !== "pending") throw new SmActivityError(409, "sm_activity_request_closed", "Die Anfrage wurde bereits bearbeitet.");
      if (!submission || submission.status !== "submitted" || !submission.isCurrent || submission.isDeleted) throw new SmActivityError(409, "sm_activity_submission_stale", "Der Fragebogen ist nicht mehr als aktive Einreichung verfügbar.");
      const answers = await tx.select({ id: smQuestionAnswers.id }).from(smQuestionAnswers).where(eq(smQuestionAnswers.submissionId, submission.id));
      const answerIds = answers.map((row) => row.id);
      const now = new Date();
      if (answerIds.length) {
        await tx.update(smQuestionAnswerFiles).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(and(inArray(smQuestionAnswerFiles.answerId, answerIds), eq(smQuestionAnswerFiles.isDeleted, false)));
        await tx.update(smQuestionAnswerOptions).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(and(inArray(smQuestionAnswerOptions.answerId, answerIds), eq(smQuestionAnswerOptions.isDeleted, false)));
        await tx.update(smQuestionAnswerMatrixCells).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(and(inArray(smQuestionAnswerMatrixCells.answerId, answerIds), eq(smQuestionAnswerMatrixCells.isDeleted, false)));
      }
      await tx.update(smQuestionAnswers).set({ isCurrent: false, isDeleted: true, deletedAt: now, updatedAt: now }).where(and(eq(smQuestionAnswers.submissionId, submission.id), eq(smQuestionAnswers.isDeleted, false)));
      await tx.update(smQuestionnaireSubmissionQuestions).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(and(eq(smQuestionnaireSubmissionQuestions.submissionId, submission.id), eq(smQuestionnaireSubmissionQuestions.isDeleted, false)));
      await tx.update(smQuestionnaireSubmissionSections).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(and(eq(smQuestionnaireSubmissionSections.submissionId, submission.id), eq(smQuestionnaireSubmissionSections.isDeleted, false)));
      await tx.update(smAnswerChangeRequests).set({ status: "cancelled", updatedAt: now }).where(and(eq(smAnswerChangeRequests.submissionId, submission.id), eq(smAnswerChangeRequests.status, "pending"), eq(smAnswerChangeRequests.isDeleted, false)));
      await tx.update(smQuestionnaireSubmissions).set({ status: "invalidated", isCurrent: false, invalidatedAt: now, invalidatedByUserId: actor.appUserId, invalidationReason: `SM-Löschanfrage: ${request.requestReason}`, isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(smQuestionnaireSubmissions.id, submission.id));
      const [updated] = await tx.update(smQuestionnaireSubmissionDeleteRequests).set({ status: "approved", reviewedByUserId: actor.appUserId, reviewedAt: now, adminNote: input.adminNote?.trim() || null, appliedAt: now, updatedAt: now }).where(eq(smQuestionnaireSubmissionDeleteRequests.id, request.id)).returning();
      return { row: updated!, replayed: false };
    });
    res.json({ request: { id: result.row.id, status: result.row.status, appliedAt: result.row.appliedAt?.toISOString() ?? null }, replayed: result.replayed });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Ungültige Entscheidung.", code: "sm_activity_review_invalid" });
    if (!sendKnownError(error, res)) next(error);
  }
});
