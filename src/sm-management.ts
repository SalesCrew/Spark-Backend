import { createHash, randomUUID } from "node:crypto";
import { and, asc, desc, eq, inArray, sql } from "drizzle-orm";
import { z } from "zod";
import type { db } from "./lib/db.js";
import {
  smQuestionnaireSubmissions as submissions,
  smQuestionnaireSubmissionSections as sections,
  smQuestionnaireSubmissionQuestions as questions,
  smQuestionAnswers as answers,
  smQuestionAnswerOptions as options,
  smQuestionAnswerMatrixCells as cells,
  smQuestionAnswerFiles as files,
  smQuestionAnswerEvents as events,
} from "./lib/schema.js";
import { computeHiddenQuestionIds } from "./lib/conditional-visibility.js";
import { smCommentMissing } from "./sm-comment.shared.js";
import {
  isAnsweredSmVisitPayload, isCompleteSmVisitAnswer, normalizeSmVisitAnswer,
  smVisitAnswerSchema, smVisitAnswerToRuleValue, type SmVisitAnswerPayload, type SmVisitQuestionSnapshot,
} from "./sm-visit.shared.js";

export type SmManagementTx = Parameters<Parameters<typeof db.transaction>[0]>[0];
type Question = typeof questions.$inferSelect;
type Answer = typeof answers.$inferSelect;
type Photo = typeof files.$inferSelect;
export type SmVerifiedAdminPhoto = Omit<typeof files.$inferInsert, "answerId"> & { id: string; questionId: string };

export class SmManagementError extends Error {
  constructor(public status: number, public code: string, message: string, public details?: Record<string, unknown>) { super(message); }
}
function fail(status: number, code: string, message: string, details?: Record<string, unknown>): never {
  throw new SmManagementError(status, code, message, details);
}
export const smAdminPhotoReceiptSchema = z.object({
  id: z.string().uuid(), questionId: z.string().uuid(), storagePath: z.string().max(600),
  originalFileName: z.string().trim().min(1).max(255), mimeType: z.enum(["image/jpeg", "image/png", "image/webp"]),
  byteSize: z.number().int().min(1).max(20 * 1024 * 1024), expiresAt: z.number().int().positive(), proof: z.string().regex(/^[a-f0-9]{64}$/),
}).strict();
export type SmAdminPhotoReceipt = z.infer<typeof smAdminPhotoReceiptSchema>;
export const smAdminCorrectionSchema = z.object({
  expectedVersion: z.string().regex(/^[a-f0-9]{64}$/),
  clientMutationToken: z.string().uuid(),
  reason: z.string().trim().min(3).max(2000),
  changes: z.array(z.object({ questionId: z.string().uuid(), answer: smVisitAnswerSchema }).strict()).min(1).max(200),
  uploads: z.array(smAdminPhotoReceiptSchema).max(40).default([]),
}).strict().refine(value => new Set(value.changes.map(change => change.questionId)).size === value.changes.length, "Eine Frage darf nur einmal geändert werden.")
  .refine(value => new Set(value.uploads.map(upload => upload.id)).size === value.uploads.length, "Doppelte Foto-IDs.");
export type SmAdminCorrection = z.infer<typeof smAdminCorrectionSchema>;

function canonical(value: unknown): unknown {
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([key, child]) => [key, canonical(child)]));
  return value;
}
export function smManagementHash(value: unknown): string { return createHash("sha256").update(JSON.stringify(canonical(value))).digest("hex"); }
export function smManagementQuestionSnapshot(question: Question): SmVisitQuestionSnapshot {
  return {
    type: question.questionTypeSnapshot, config: question.configSnapshot,
    options: question.answerOptionsSnapshot.filter(option => typeof option.code === "string" && typeof option.label === "string")
      .map(option => ({ code: option.code as string, label: option.label as string, marksNotApplicable: option.marksNotApplicable === true })),
  };
}

/** Caller holds a transaction. Share lock gives the read DTO one coherent answer generation. */
export async function loadSmManagementState(tx: SmManagementTx, id: string, write = false) {
  const query = tx.select().from(submissions).where(eq(submissions.id, id)).limit(1);
  const [submission] = await (write ? query.for("update") : query.for("share"));
  if (!submission || submission.isDeleted || !submission.isCurrent || submission.status !== "submitted") {
    return fail(409, "sm_management_submission_unavailable", "Dieser abgeschlossene Fragebogen ist nicht mehr verfügbar. Bitte aktualisiere die Übersicht.");
  }
  const questionRows = await tx.select().from(questions).where(and(eq(questions.submissionId, id), eq(questions.isDeleted, false)))
    .orderBy(asc(questions.submissionSectionId), asc(questions.orderIndex), asc(questions.id));
  const answerRows = await tx.select().from(answers).where(and(eq(answers.submissionId, id), eq(answers.isDeleted, false), eq(answers.isCurrent, true))).orderBy(asc(answers.id));
  const photoRows = answerRows.length ? await tx.select().from(files).where(and(inArray(files.answerId, answerRows.map(answer => answer.id)), eq(files.isDeleted, false))).orderBy(asc(files.uploadedAt), asc(files.id)) : [];
  const values = new Map<string, SmVisitAnswerPayload>();
  for (const answer of answerRows) {
    const value = (answer.valueJson ?? { kind: "empty" }) as SmVisitAnswerPayload;
    // Historical correction paths could copy file rows without updating the old IDs in value_json.
    // The owning file rows are authoritative, never expose another answer's file IDs.
    values.set(answer.submissionQuestionId, value.kind === "photo"
      ? { ...value, fileIds: photoRows.filter(file => file.answerId === answer.id).map(file => file.id) }
      : value);
  }
  const version = smManagementHash({ submission, questions: questionRows, answers: answerRows, photos: photoRows });
  return { submission, questions: questionRows, answers: answerRows, photos: photoRows, values, version };
}
export type SmManagementState = Awaited<ReturnType<typeof loadSmManagementState>>;

export function smManagementVisibility(state: Pick<SmManagementState, "questions" | "values">) {
  const hidden = computeHiddenQuestionIds(state.questions.map(question => ({ id: question.id, questionId: question.questionCodeSnapshot, rules: question.logicRulesSnapshot })),
    new Map(state.questions.map(question => [question.id, smVisitAnswerToRuleValue(state.values.get(question.id), smManagementQuestionSnapshot(question).options)])));
  // Do not revive exclusions unrelated to editable conditional rules.
  for (const question of state.questions) if (!question.isApplicable && question.applicabilityReason && !question.applicabilityReason.startsWith("hidden_by_rule")) hidden.add(question.id);
  return hidden;
}

function chosenOptions(question: Question, value: SmVisitAnswerPayload) {
  const codes = value.kind === "choice" || value.kind === "yesnomulti" ? [value.optionCode] : value.kind === "multi" ? value.optionCodes : [];
  return codes.map(code => question.answerOptionsSnapshot.find(option => option.code === code)!);
}
function numeric(value: unknown): number { const n = Number(value ?? 0); return Number.isFinite(n) && n >= 0 ? n : 0; }

async function appendAnswer(tx: SmManagementTx, state: SmManagementState, question: Question, value: SmVisitAnswerPayload,
  actor: string, uploaded: SmVerifiedAdminPhoto[], reason: string) {
  const current = state.answers.find(answer => answer.submissionQuestionId === question.id);
  const [latest] = await tx.select().from(answers).where(eq(answers.submissionQuestionId, question.id)).orderBy(desc(answers.answerVersion)).limit(1);
  const now = new Date();
  const id = randomUUID(), version = (latest?.answerVersion ?? 0) + 1;
  let nextValue = value;
  const photosToInsert: Array<typeof files.$inferInsert> = [];
  if (value.kind === "photo") {
    for (const fileId of value.fileIds) {
      const previous = state.photos.find(file => file.id === fileId && file.answerId === current?.id);
      const fresh = uploaded.find(file => file.id === fileId && file.questionId === question.id);
      if (!previous && !fresh) return fail(400, "sm_management_photo_forbidden", "Ein Foto gehört nicht zu dieser Frage.");
      const photo = previous ?? fresh!;
      photosToInsert.push({ id: randomUUID(), answerId: id, storageBucket: photo.storageBucket, storagePath: photo.storagePath,
        originalFileName: photo.originalFileName, mimeType: photo.mimeType, byteSize: photo.byteSize,
        widthPx: photo.widthPx, heightPx: photo.heightPx, sha256: photo.sha256, uploadedAt: photo.uploadedAt });
    }
    nextValue = { ...value, fileIds: photosToInsert.map(photo => photo.id!) };
  }
  const selected = chosenOptions(question, value);
  const notApplicable = selected.some(option => option.marksNotApplicable === true);
  const answered = isAnsweredSmVisitPayload(value);
  const possible = notApplicable ? 0 : selected.reduce((sum, option) => sum + numeric(option.possiblePoints), 0);
  const earned = notApplicable ? 0 : Math.min(possible, selected.reduce((sum, option) => sum + numeric(option.earnedPoints), 0));
  if (current) await tx.update(answers).set({ isCurrent: false, updatedAt: now }).where(eq(answers.id, current.id));
  await tx.insert(answers).values({ id, submissionId: state.submission.id, submissionQuestionId: question.id,
    supersedesAnswerId: latest?.id ?? null, answerVersion: version, isCurrent: true,
    answerState: notApplicable ? "not_applicable" : answered ? "answered" : "unanswered",
    applicabilityReason: notApplicable ? "selected_not_applicable_option" : null,
    valueJson: nextValue, valueText: value.kind === "text" ? value.value : null, valueNumber: value.kind === "number" ? String(value.value) : null,
    earnedPoints: String(earned), possiblePoints: String(possible), answeredByUserId: actor, answeredAt: answered ? now : null,
  });
  if (selected.length) await tx.insert(options).values(selected.map((option, orderIndex) => ({ answerId: id,
    answerOptionVersionId: typeof option.id === "string" ? option.id : null,
    optionCodeSnapshot: String(option.code), optionLabelSnapshot: String(option.label),
    earnedPointsSnapshot: String(numeric(option.earnedPoints)), possiblePointsSnapshot: String(numeric(option.possiblePoints)),
    metricOutcomeCodeSnapshot: typeof option.metricOutcomeCode === "string" ? option.metricOutcomeCode : null, orderIndex,
  })));
  if (value.kind === "matrix" && value.cells.length) await tx.insert(cells).values(value.cells.map((cell, orderIndex) => ({ answerId: id, ...cell, orderIndex })));
  if (photosToInsert.length) await tx.insert(files).values(photosToInsert);
  await tx.insert(events).values({ answerId: id, submissionId: state.submission.id, eventType: "correction", answerVersion: version,
    actorUserId: actor, payload: { source: "sm_admin_correction", reason, previousAnswerId: current?.id ?? null,
      before: state.values.get(question.id) ?? { kind: "empty" }, after: nextValue } });
  return { questionId: question.id, answerId: id, answerVersion: version };
}

/** All writers of completed SM answers first lock the submission, including existing request approvals. */
export async function applySmAdminCorrection(tx: SmManagementTx, submissionId: string, input: SmAdminCorrection, actor: string,
  verifyUploads: (uploads: SmAdminPhotoReceipt[]) => Promise<SmVerifiedAdminPhoto[]> = async uploads => {
    if (uploads.length) return fail(400, "sm_management_upload_unverified", "Foto-Upload muss verifiziert werden.");
    return [];
  }) {
  // Lock the same row as answer/delete-request approvals; never acquire their per-question locks afterwards.
  const [identity] = await tx.select({ id: submissions.id }).from(submissions).where(eq(submissions.id, submissionId)).limit(1).for("update");
  if (!identity) return fail(404, "sm_management_submission_not_found", "Der Fragebogen wurde nicht gefunden.");
  const requestHash = smManagementHash(input);
  const [prior] = await tx.select({ payload: events.payload }).from(events).where(and(eq(events.submissionId, submissionId), eq(events.actorUserId, actor),
    sql`${events.payload}->>'source' = 'sm_admin_correction_committed'`, sql`${events.payload}->>'clientMutationToken' = ${input.clientMutationToken}`)).limit(1);
  if (prior) {
    if (prior.payload.requestHash !== requestHash) return fail(409, "sm_management_token_reused", "Diese Speicher-ID wurde bereits für andere Änderungen verwendet.");
    return { replayed: true, result: prior.payload.result as { submissionId: string; answerIds: string[] } };
  }
  const state = await loadSmManagementState(tx, submissionId, true);
  if (state.version !== input.expectedVersion) return fail(409, "sm_management_version_conflict", "Der Fragebogen wurde inzwischen geändert. Bitte lade den aktuellen Stand; dein Entwurf bleibt erhalten.");
  const values = new Map(state.values);
  const normalized = input.changes.map(change => {
    const question = state.questions.find(row => row.id === change.questionId);
    if (!question) return fail(400, "sm_management_question_forbidden", "Eine Frage gehört nicht zu diesem Fragebogen.");
    const answer = normalizeSmVisitAnswer(smManagementQuestionSnapshot(question), change.answer);
    values.set(question.id, answer);
    return { question, answer };
  });
  const hidden = smManagementVisibility({ questions: state.questions, values });
  const missing = state.questions.filter(question => !hidden.has(question.id) && (
    smCommentMissing(smManagementQuestionSnapshot(question), values.get(question.id))
    || question.requiredSnapshot && !isCompleteSmVisitAnswer(smManagementQuestionSnapshot(question), values.get(question.id))));
  if (missing.length) return fail(400, "sm_management_required_answers", "Bitte ergänze die markierten Pflichtantworten und Kommentare.", { questionIds: missing.map(question => question.id) });
  for (const change of normalized) if (hidden.has(change.question.id)) return fail(400, "sm_management_question_hidden", "Ausgeblendete Fragen können nicht geändert werden.", { questionIds: [change.question.id] });
  const effective = normalized.filter(change => smManagementHash(change.answer) !== smManagementHash(state.values.get(change.question.id) ?? { kind: "empty" }));
  if (!effective.length) return fail(400, "sm_management_no_changes", "Es wurden keine Antworten geändert.");
  for (const upload of input.uploads) if (!effective.some(change => change.question.id === upload.questionId && change.answer.kind === "photo" && change.answer.fileIds.includes(upload.id))) {
    return fail(400, "sm_management_unused_upload", "Ein Foto ist keiner geänderten Antwort zugeordnet.");
  }
  const verified = await verifyUploads(input.uploads);
  if (verified.length !== input.uploads.length || verified.some(photo => !input.uploads.some(upload => upload.id === photo.id && upload.questionId === photo.questionId))) {
    return fail(400, "sm_management_upload_unverified", "Foto-Verifikation unvollständig.");
  }
  const changed: Array<{ questionId: string; answerId: string; answerVersion: number }> = [];
  for (const change of effective) changed.push(await appendAnswer(tx, state, change.question, change.answer, actor, verified, input.reason));
  const now = new Date();
  for (const question of state.questions) {
    const applicable = !hidden.has(question.id);
    if (applicable !== question.isApplicable) await tx.update(questions).set({ isApplicable: applicable,
      applicabilityReason: applicable ? null : "hidden_by_rule_after_admin_correction", updatedAt: now }).where(eq(questions.id, question.id));
    const previous = state.answers.find(answer => answer.submissionQuestionId === question.id);
    if (!applicable && previous) {
      await tx.update(answers).set({ isCurrent: false, answerState: "invalidated", invalidatedAt: now, invalidatedByUserId: actor,
        invalidationReason: "hidden_by_rule_after_admin_correction", updatedAt: now }).where(eq(answers.id, previous.id));
      await tx.insert(events).values({ answerId: previous.id, submissionId, eventType: "state_change", answerVersion: previous.answerVersion,
        actorUserId: actor, payload: { source: "sm_admin_correction", reason: input.reason, from: previous.answerState, to: "invalidated", cause: "hidden_by_rule" } });
    }
  }
  const current = await tx.select().from(answers).where(and(eq(answers.submissionId, submissionId), eq(answers.isCurrent, true), eq(answers.isDeleted, false)));
  await tx.update(submissions).set({ answeredQuestionCount: current.filter(answer => answer.answerState === "answered").length,
    earnedPoints: String(current.reduce((sum, answer) => sum + numeric(answer.earnedPoints), 0)),
    possiblePoints: String(current.reduce((sum, answer) => sum + numeric(answer.possiblePoints), 0)), lastSavedAt: now, updatedAt: now,
  }).where(eq(submissions.id, submissionId));
  const result = { submissionId, answerIds: changed.map(change => change.answerId) };
  const anchor = changed[0]!;
  // Indexed by submission; row lock serializes token checks/inserts, including concurrent retries.
  await tx.insert(events).values({ answerId: anchor.answerId, submissionId, eventType: "correction", answerVersion: anchor.answerVersion, actorUserId: actor,
    payload: { source: "sm_admin_correction_committed", clientMutationToken: input.clientMutationToken, requestHash, reason: input.reason, result } });
  return { replayed: false, result };
}

export async function smManagementDetail(tx: SmManagementTx, id: string) {
  const state = await loadSmManagementState(tx, id);
  const sectionRows = await tx.select().from(sections).where(and(eq(sections.submissionId, id), eq(sections.isDeleted, false))).orderBy(asc(sections.orderIndex));
  const submission = state.submission;
  return {
    version: state.version,
    visit: { id: submission.id, assignmentId: submission.assignmentId, smUserId: submission.smUserId, smName: submission.smNameSnapshot,
      marketId: submission.smMarketId, marketName: submission.marketNameSnapshot, address: submission.marketAddressSnapshot,
      questionnaireId: submission.questionnaireTemplateId, questionnaireName: submission.questionnaireNameSnapshot, questionnaireVersion: submission.questionnaireVersionSnapshot,
      startedAt: submission.visitStartedAt?.toISOString() ?? null, completedAt: submission.visitCompletedAt?.toISOString() ?? null,
      submittedAt: submission.submittedAt?.toISOString() ?? null, travelMinutes: submission.travelMinutes, updatedAt: submission.updatedAt.toISOString() },
    sections: sectionRows.map(section => ({ id: section.id, title: section.moduleNameSnapshot, description: section.moduleDescriptionSnapshot,
      questions: state.questions.filter(question => question.submissionSectionId === section.id).map(question => ({
        id: question.id, questionCode: question.questionCodeSnapshot, text: question.questionTextSnapshot, type: question.questionTypeSnapshot,
        config: question.configSnapshot, options: question.answerOptionsSnapshot, rules: question.logicRulesSnapshot,
        required: question.requiredSnapshot, applicable: question.isApplicable, applicabilityReason: question.applicabilityReason,
        answer: state.values.get(question.id) ?? { kind: "empty" },
        answerId: state.answers.find(answer => answer.submissionQuestionId === question.id)?.id ?? null,
        answerState: state.answers.find(answer => answer.submissionQuestionId === question.id)?.answerState ?? "unanswered",
        photos: state.photos.filter(photo => state.answers.some(answer => answer.id === photo.answerId && answer.submissionQuestionId === question.id))
          .map(photo => ({ id: photo.id, fileName: photo.originalFileName, mimeType: photo.mimeType, byteSize: photo.byteSize, storageBucket: photo.storageBucket, storagePath: photo.storagePath })),
      })) })),
  };
}
