import { and, asc, eq, inArray } from "drizzle-orm";
import { z } from "zod";
import { smQuestionnaireSubmissions as submissions, smQuestionnaireSubmissionSections as sections,
  smQuestionnaireSubmissionQuestions as questions, smQuestionAnswers as answers, users,
  smSMDurcharbeitTimeRevisions as timeRevisions } from "./lib/schema.js";
import { listSmManagedVisits, smManagementListSchema } from "./sm-management-list.shared.js";
import { SMDurcharbeitAnswerFiles } from "./sm-SMDurcharbeit-answer-reuse.shared.js";
import { SmManagementError, type SmManagementTx } from "./sm-management.js";
import type { SmVisitAnswerPayload } from "./sm-visit.shared.js";

// A report always covers the entire selection. Page cursors and limits are rejected.
export const smManagementReportSchema = z.object({
  from: z.string(), to: z.string(), smUserId: z.string().optional(), marketId: z.string().optional(),
  questionnaireId: z.string().optional(), search: z.string().optional(),
  SMDurcharbeitCatalogScope: z.enum(["standard", "SMDurcharbeit"]).optional(),
  SMDurcharbeitCampaignId: z.string().optional(), SMDurcharbeitMonth: z.string().optional(),
}).strict().transform(input => {
  const { limit: _limit, ...selection } = smManagementListSchema.parse(input);
  return selection;
});
export type SmManagementReportQuery = z.infer<typeof smManagementReportSchema>;
const maxVisits = 3000, maxQuestions = 100000, maxPhotos = 20000;
function tooLarge(): never {
  throw new SmManagementError(413, "sm_report_too_large", "Die Auswertung ist zu groß. Bitte grenze den Zeitraum, Mitarbeiter oder Fragebogen weiter ein.");
}

/** Pure read DTO. The caller supplies a repeatable-read, read-only transaction. */
export async function loadSmManagementReport(tx: SmManagementTx, input: SmManagementReportQuery) {
  const visits: Awaited<ReturnType<typeof listSmManagedVisits>>["visits"] = [];
  let cursor: { date: string; id: string } | null = null;
  do {
    const page = await listSmManagedVisits(tx, { ...input, limit: 100,
      ...(cursor ? { cursorDate: cursor.date, cursorId: cursor.id } : {}) }, false, false);
    visits.push(...page.visits);
    if (visits.length > maxVisits || (visits.length === maxVisits && page.nextCursor)) tooLarge();
    cursor = page.nextCursor;
  } while (cursor);
  const generatedAt = new Date().toISOString();
  if (!visits.length) return { generatedAt, filters: input, visits: [], answers: [] };
  const ids = visits.map(visit => visit.id);
  const submissionRows = await tx.select().from(submissions).where(inArray(submissions.id, ids));
  const sectionRows = await tx.select({ id: sections.id, title: sections.moduleNameSnapshot, order: sections.orderIndex })
    .from(sections).where(and(inArray(sections.submissionId, ids), eq(sections.isDeleted, false)));
  const questionRows = await tx.select().from(questions).where(and(inArray(questions.submissionId, ids), eq(questions.isDeleted, false)))
    .orderBy(asc(questions.submissionId), asc(questions.orderIndex), asc(questions.id)).limit(maxQuestions + 1);
  const answerRows = await tx.select().from(answers).where(and(inArray(answers.submissionId, ids), eq(answers.isCurrent, true), eq(answers.isDeleted, false)))
    .limit(maxQuestions + 1);
  if (questionRows.length > maxQuestions || answerRows.length > maxQuestions) tooLarge();
  const answerMap = new Map(answerRows.map(answer => [answer.submissionQuestionId, answer]));
  if (answerMap.size !== answerRows.length) throw new SmManagementError(409, "sm_report_conflicting_answers", "Eine Frage enthält mehrere aktuelle Antworten. Bitte aktualisiere die Übersicht.");
  const actorIds = [...new Set(answerRows.flatMap(answer => answer.answeredByUserId ? [answer.answeredByUserId] : []))];
  const actorNames = new Map<string, string>();
  for (let offset = 0; offset < actorIds.length; offset += 500) {
    const actors = await tx.select({ id: users.id, firstName: users.firstName, lastName: users.lastName }).from(users)
      .where(inArray(users.id, actorIds.slice(offset, offset + 500)));
    for (const actor of actors) actorNames.set(actor.id, [actor.firstName, actor.lastName].filter(Boolean).join(" "));
  }
  const photoAnswers = answerRows.filter(answer => (answer.valueJson as SmVisitAnswerPayload | null)?.kind === "photo");
  const photos: Awaited<ReturnType<typeof SMDurcharbeitAnswerFiles>> = [];
  for (let offset = 0; offset < photoAnswers.length; offset += 500) {
    photos.push(...await SMDurcharbeitAnswerFiles(tx, photoAnswers.slice(offset, offset + 500).map(answer => answer.id)));
    if (photos.length > maxPhotos) tooLarge();
  }
  const photosByAnswer = new Map<string, typeof photos>();
  for (const photo of photos) { const list = photosByAnswer.get(photo.answerId) ?? []; list.push(photo); photosByAnswer.set(photo.answerId, list); }
  const submissionMap = new Map(submissionRows.map(row => [row.id, row]));
  const sectionMap = new Map(sectionRows.map(row => [row.id, row]));
  const timeIds = submissionRows.flatMap(row => row.SMDurcharbeitVisitId ? [row.SMDurcharbeitVisitId] : []);
  const times = timeIds.length ? await tx.select().from(timeRevisions).where(and(inArray(timeRevisions.visitId, timeIds), eq(timeRevisions.isCurrent, true))) : [];
  const timeMap = new Map(times.map(time => [time.visitId, time]));
  const reportVisits = visits.map(visit => {
    const row = submissionMap.get(visit.id)!;
    const time = row.SMDurcharbeitVisitId ? timeMap.get(row.SMDurcharbeitVisitId) : undefined;
    const startedAt = time?.startedAt ?? row.visitStartedAt, completedAt = time?.completedAt ?? row.visitCompletedAt;
    return { ...visit, startedAt, completedAt, postalCode: row.marketPostalCodeSnapshot, city: row.marketCitySnapshot,
      travelMinutes: time?.travelMinutes ?? row.travelMinutes,
      actualMinutes: startedAt && completedAt ? Math.round((completedAt.getTime() - startedAt.getTime()) / 60000) : row.manualVisitMinutes,
      originalStartedAt: row.visitStartedAt, originalCompletedAt: row.visitCompletedAt,
      updatedAt: row.updatedAt, timeMode: row.visitTimeMode, revision: row.revisionNumber,
    };
  });
  const visitIds = new Set(ids);
  const reportAnswers = questionRows.sort((a, b) => a.submissionId.localeCompare(b.submissionId)
    || (sectionMap.get(a.submissionSectionId)?.order ?? 0) - (sectionMap.get(b.submissionSectionId)?.order ?? 0)
    || a.orderIndex - b.orderIndex || a.id.localeCompare(b.id)).map(question => {
    const answer = answerMap.get(question.id);
    const submission = submissionMap.get(question.submissionId)!;
    const answerPhotos = answer ? photosByAnswer.get(answer.id) ?? [] : [];
    const rawValue = answer?.valueJson as SmVisitAnswerPayload | null;
    // As in the management drawer, authoritative file ownership wins over stale JSON IDs.
    const valueJson = rawValue?.kind === "photo" ? { ...rawValue, fileIds: answerPhotos.map(photo => photo.id) } : rawValue;
    if (!visitIds.has(question.submissionId)) throw new Error("Answer outside the report selection");
    return { module: sectionMap.get(question.submissionSectionId)?.title ?? "",
      question: { id: question.id, submissionId: question.submissionId, questionVersionId: question.questionVersionId,
        questionCodeSnapshot: question.questionCodeSnapshot, questionTypeSnapshot: question.questionTypeSnapshot,
        questionTextSnapshot: question.questionTextSnapshot, answerOptionsSnapshot: question.answerOptionsSnapshot,
        configSnapshot: question.configSnapshot, isApplicable: question.isApplicable, applicabilityReason: question.applicabilityReason },
      answer: answer ? { id: answer.id, answerState: answer.answerState, answerVersion: answer.answerVersion,
        valueJson: valueJson ?? null, answeredAt: answer.answeredAt, answeredByUserId: answer.answeredByUserId,
        answeredByName: answer.answeredByUserId === submission.smUserId ? submission.smNameSnapshot : actorNames.get(answer.answeredByUserId ?? "") || null,
        earnedPoints: answer.earnedPoints, possiblePoints: answer.possiblePoints } : null,
      photos: answerPhotos.map(photo => ({ id: photo.id, fileName: photo.originalFileName, mimeType: photo.mimeType,
        byteSize: photo.byteSize, uploadedAt: photo.uploadedAt })),
    };
  });
  return { generatedAt, filters: input, visits: reportVisits, answers: reportAnswers };
}
