import { and, asc, desc, eq, inArray, isNotNull } from "drizzle-orm";
import { smQuestionAnswers as answers, smQuestionnaireSubmissionQuestions as questions,
  smQuestionnaireSubmissions as submissions, smSMDurcharbeitVisits as visits,
  smSMDurcharbeitTimeRevisions as times, smSMDurcharbeitAnswerProvenance as provenance } from "./lib/schema.js";
import { isAnsweredSmVisitPayload, type SmVisitAnswerPayload } from "./sm-visit.shared.js";
import { SMDurcharbeitAnswerFiles } from "./sm-SMDurcharbeit-answer-reuse.shared.js";
import { listSMDurcharbeitTargets, SMDurcharbeitCampaignError, type SMDurcharbeitExecutor } from "./sm-SMDurcharbeit-campaign.shared.js";

type Question = typeof questions.$inferSelect;
type Answer = typeof answers.$inferSelect;
export type SMDurcharbeitReportAnswer = {
  question: Question; answer: Answer | null; targetId: string; smName: string;
  sourceSubmissionId: string | null; sourceAnswerId: string | null;
};
export function summarizeSMDurcharbeitAnswers(rows: SMDurcharbeitReportAnswer[]) {
  const groups = new Map<string, { questionVersionId: string; questionCode: string; text: string; type: string;
    applicable: number; answered: number; unanswered: number; average: number | null;
    distribution: Array<{ code: string; label: string; count: number; percentage: number | null }>; numbers: number[] }>();
  for (const row of rows) {
    const q = row.question, key = `${q.questionVersionId}:${q.questionCodeSnapshot}`;
    let group = groups.get(key);
    if (!group) {
      group = { questionVersionId: q.questionVersionId, questionCode: q.questionCodeSnapshot,
        text: q.questionTextSnapshot, type: q.questionTypeSnapshot, applicable: 0, answered: 0, unanswered: 0, average: null,
        distribution: q.answerOptionsSnapshot.flatMap(option => typeof option.code === "string" && typeof option.label === "string"
          ? [{ code: option.code, label: option.label, count: 0, percentage: null }] : []), numbers: [] };
      groups.set(key, group);
    }
    group.applicable++;
    const value = row.answer?.valueJson as SmVisitAnswerPayload | null;
    if (row.answer?.answerState !== "answered" || !value || !isAnsweredSmVisitPayload(value)) { group.unanswered++; continue; }
    group.answered++;
    const codes = value.kind === "choice" || value.kind === "yesnomulti" ? [value.optionCode]
      : value.kind === "multi" ? value.optionCodes : [];
    for (const code of new Set(codes)) { const option = group.distribution.find(option => option.code === code); if (option) option.count++; }
    if (value.kind === "number" && Number.isFinite(value.value)) group.numbers.push(value.value);
  }
  return [...groups.values()].map(({ numbers, ...group }) => ({ ...group,
    average: numbers.length ? numbers.reduce((sum, value) => sum + value, 0) / numbers.length : null,
    distribution: group.distribution.map(option => ({ ...option, percentage: group.answered ? option.count * 100 / group.answered : null })),
  }));
}

/** Run inside a read-only repeatable-read transaction so every total has the same basis. */
export async function loadSMDurcharbeitReport(executor: SMDurcharbeitExecutor, input: { campaignId: string; month: string; smUserId?: string }) {
  const targets = await listSMDurcharbeitTargets(executor, input);
  const required = targets.filter(target => target.eligibility === "required");
  const ids = targets.map(target => target.id), latestIds = required.flatMap(target => target.latestSubmissionId ? [target.latestSubmissionId] : []);
  const questionRows = latestIds.length ? await executor.select({ question: questions, answer: answers,
    targetId: submissions.SMDurcharbeitTargetId, smName: submissions.smNameSnapshot,
    sourceSubmissionId: provenance.sourceSubmissionId, sourceAnswerId: provenance.sourceAnswerId })
    .from(questions).innerJoin(submissions, eq(submissions.id, questions.submissionId))
    .leftJoin(answers, and(eq(answers.submissionQuestionId, questions.id), eq(answers.isCurrent, true), eq(answers.isDeleted, false)))
    .leftJoin(provenance, eq(provenance.answerId, answers.id))
    .where(and(inArray(questions.submissionId, latestIds), eq(questions.isApplicable, true), eq(questions.isDeleted, false)))
    .orderBy(asc(submissions.id), asc(questions.submissionSectionId), asc(questions.orderIndex)).limit(200001) : [];
  if (questionRows.length > 200000) throw new SMDurcharbeitCampaignError(400, "smdurcharbeit_report_too_large", "Bitte die Auswertung auf einen SM eingrenzen.");
  const photoAnswerIds = questionRows.flatMap(row => row.answer && row.question.questionTypeSnapshot === "photo" ? [row.answer.id] : []);
  const fileRows: Awaited<ReturnType<typeof SMDurcharbeitAnswerFiles>> = [];
  // Bounded parameters, also for a large questionnaire/roster; no query per market.
  for (let offset = 0; offset < photoAnswerIds.length; offset += 5000) fileRows.push(...await SMDurcharbeitAnswerFiles(executor, photoAnswerIds.slice(offset, offset + 5000)));
  const fileIdsByAnswer = new Map<string, Set<string>>();
  for (const file of fileRows) { const ids = fileIdsByAnswer.get(file.answerId) ?? new Set<string>(); ids.add(file.id); fileIdsByAnswer.set(file.answerId, ids); }
  const answerRows: SMDurcharbeitReportAnswer[] = questionRows.map(row => {
    const value = row.answer?.valueJson as SmVisitAnswerPayload | null;
    // Access withdrawal affects report access, never the original stored answer.
    const answer = row.answer && value?.kind === "photo" ? { ...row.answer, valueJson: { ...value,
      fileIds: value.fileIds.filter(id => fileIdsByAnswer.get(row.answer!.id)?.has(id)) } } : row.answer;
    return { ...row, targetId: row.targetId!, answer };
  });
  const physicalRows = ids.length ? await executor.select({ visit: visits, submission: submissions, time: times })
    .from(visits).innerJoin(submissions, eq(submissions.SMDurcharbeitVisitId, visits.id))
    .leftJoin(times, and(eq(times.visitId, visits.id), eq(times.isCurrent, true)))
    .where(and(inArray(visits.targetId, ids), isNotNull(submissions.submittedAt)))
    .orderBy(desc(submissions.revisionNumber), desc(submissions.createdAt), desc(submissions.id)).limit(100001) : [];
  if (physicalRows.length > 100000) throw new SMDurcharbeitCampaignError(400, "smdurcharbeit_report_too_large", "Bitte die Auswertung auf einen SM eingrenzen.");
  const uniqueVisits = new Map<string, typeof physicalRows[number]>();
  for (const row of physicalRows) if (!uniqueVisits.has(row.visit.id)) uniqueVisits.set(row.visit.id, row);
  const physicalVisits = [...uniqueVisits.values()].map(({ visit, submission, time }) => ({ id: visit.id, targetId: visit.targetId,
    submissionId: submission.id, smUserId: visit.smUserId, smName: submission.smNameSnapshot, marketName: submission.marketNameSnapshot,
    questionnaireValid: submission.status === "submitted" && submission.isCurrent && !submission.isDeleted,
    submittedAt: submission.submittedAt!.toISOString(), originalStartedAt: submission.visitStartedAt?.toISOString() ?? null,
    originalCompletedAt: submission.visitCompletedAt?.toISOString() ?? null,
    startedAt: time?.startedAt.toISOString() ?? null, completedAt: time?.completedAt.toISOString() ?? null,
    actualMinutes: time?.actualMinutes ?? null, travelMinutes: time?.travelMinutes ?? null, timeRevision: time?.revisionNumber ?? null,
  }));
  const completed = required.filter(target => target.completed).length;
  return { month: input.month, targets, answers: answerRows, questionResults: summarizeSMDurcharbeitAnswers(answerRows), physicalVisits,
    summary: { required: required.length, completed, waived: targets.length - required.length,
      coveragePercentage: required.length ? completed * 100 / required.length : null,
      latestSubmissions: latestIds.length, physicalVisits: physicalVisits.length,
      validQuestionnaireVisits: physicalVisits.filter(visit => visit.questionnaireValid).length,
      actualMinutes: physicalVisits.reduce((sum, visit) => sum + (visit.actualMinutes ?? 0), 0),
      travelMinutes: physicalVisits.reduce((sum, visit) => sum + (visit.travelMinutes ?? 0), 0),
      availablePhotoUploads: new Set(fileRows.map(file => file.id)).size },
  };
}
