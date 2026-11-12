import { and, asc, desc, eq, gte, lte, sql } from "drizzle-orm";
import { z } from "zod";
import { smQuestionnaireSubmissions as submissions, smAssignments, smQuestionAnswers as answers,
  smQuestionnaireSubmissionQuestions as questions, smQuestionnaireTemplates,
  smSMDurcharbeitTimeRevisions as SMDurcharbeitTimes, smSMDurcharbeitTargets as SMDurcharbeitTargets,
  smSMDurcharbeitPeriods as SMDurcharbeitPeriods, smSMDurcharbeitCampaigns as SMDurcharbeitCampaigns } from "./lib/schema.js";
import { isIsoDate, isoDateToEpochDay } from "./sm-planning.shared.js";
import { SmManagementError, type SmManagementTx } from "./sm-management.js";

const uuid = z.string().uuid();
const date = z.string().refine(isIsoDate);
export const smManagementListSchema = z.object({
  from: date, to: date, smUserId: uuid.optional(), marketId: uuid.optional(), questionnaireId: uuid.optional(),
  search: z.string().trim().max(200).optional(), limit: z.coerce.number().int().min(1).max(100).default(40),
  cursorDate: date.optional(), cursorId: uuid.optional(),
  SMDurcharbeitCatalogScope: z.enum(["standard", "SMDurcharbeit"]).optional(),
  SMDurcharbeitCampaignId: uuid.optional(), SMDurcharbeitMonth: date.optional(),
}).strict().refine(input => isoDateToEpochDay(input.to) >= isoDateToEpochDay(input.from) && isoDateToEpochDay(input.to) - isoDateToEpochDay(input.from) < 93, "Zeitraum maximal 93 Tage.")
  .refine(input => Boolean(input.cursorDate) === Boolean(input.cursorId), "Ungültiger Seitencursor.");

const workDate = sql<string>`coalesce((${SMDurcharbeitTimes.startedAt} at time zone 'Europe/Vienna')::date, (${submissions.visitStartedAt} at time zone 'Europe/Vienna')::date,
  ${smAssignments.replacementWorkDate}, ${smAssignments.originalWorkDate}, (${submissions.submittedAt} at time zone 'Europe/Vienna')::date)`;

export async function listSmManagedVisits(tx: SmManagementTx, input: z.infer<typeof smManagementListSchema>, campaignMonthOnly = false, includeFacets = true) {
  if (campaignMonthOnly && (!input.SMDurcharbeitCampaignId || !input.SMDurcharbeitMonth)) {
    throw new SmManagementError(400, "sm_review_scope_required", "Kampagne und Kalendermonat sind erforderlich.");
  }
  const scope = [eq(submissions.isDeleted, false), eq(submissions.isCurrent, true), eq(submissions.status, "submitted"),
    ...(campaignMonthOnly ? [] : [gte(workDate, input.from), lte(workDate, input.to)])];
  if (input.SMDurcharbeitCatalogScope) scope.push(sql`(case when starts_with(${smQuestionnaireTemplates.stableCode}, 'smdurcharbeit_') then 'SMDurcharbeit' else 'standard' end) = ${input.SMDurcharbeitCatalogScope}`);
  if (input.SMDurcharbeitCampaignId) scope.push(eq(SMDurcharbeitCampaigns.id, input.SMDurcharbeitCampaignId));
  if (input.SMDurcharbeitMonth) scope.push(eq(SMDurcharbeitPeriods.month, input.SMDurcharbeitMonth));
  const filters = [...scope];
  if (input.smUserId) filters.push(eq(submissions.smUserId, input.smUserId));
  if (input.marketId) filters.push(eq(submissions.smMarketId, input.marketId));
  if (input.questionnaireId) filters.push(eq(submissions.questionnaireTemplateId, input.questionnaireId));
  if (input.search) {
    const recordSearch = sql`strpos(lower(concat_ws(' ', ${submissions.id}::text, ${submissions.smUserId}::text, ${submissions.smMarketId}::text,
      ${submissions.smNameSnapshot}, ${submissions.marketNameSnapshot}, ${submissions.marketAddressSnapshot},
      ${submissions.marketPostalCodeSnapshot}, ${submissions.marketCitySnapshot}, ${submissions.questionnaireNameSnapshot},
      ${submissions.questionnaireVersionSnapshot}::text, ${SMDurcharbeitCampaigns.name}, ${SMDurcharbeitPeriods.month}::text,
      ${SMDurcharbeitTargets.marketSnapshot}::text, ${workDate}::text, to_char(${workDate}::date, 'DD.MM.YYYY'))), lower(${input.search})) > 0`;
    if (campaignMonthOnly) filters.push(sql`(${recordSearch} or exists (
      select 1 from ${answers} inner join ${questions} on ${questions.id} = ${answers.submissionQuestionId}
      where ${answers.submissionId} = ${submissions.id} and ${answers.isCurrent} = true and ${answers.isDeleted} = false
      and ${answers.answerState} = 'answered' and (strpos(lower(concat_ws(' ', ${questions.questionTextSnapshot},
        ${answers.valueJson}::text)), lower(${input.search})) > 0 or exists (
        select 1 from jsonb_array_elements(${questions.answerOptionsSnapshot}) as chosen
        where jsonb_path_query_array(${answers.valueJson}, '$.**') @> jsonb_build_array(chosen->>'code')
        and strpos(lower(chosen->>'label'), lower(${input.search})) > 0
      ))
    ))`);
    else filters.push(sql`strpos(lower(${submissions.smNameSnapshot} || ' ' || ${submissions.marketNameSnapshot} || ' ' || ${submissions.marketAddressSnapshot} || ' ' || ${submissions.questionnaireNameSnapshot}), lower(${input.search})) > 0`);
  }
  if (input.cursorDate && input.cursorId) filters.push(sql`(${workDate}, ${submissions.id}) < (${input.cursorDate}::date, ${input.cursorId}::uuid)`);
  const rows = await tx.select({ id: submissions.id, assignmentId: submissions.assignmentId, workDate: sql<string>`${workDate}::text`,
    smUserId: submissions.smUserId, smName: submissions.smNameSnapshot, marketId: submissions.smMarketId,
    marketName: submissions.marketNameSnapshot, address: submissions.marketAddressSnapshot,
    questionnaireId: submissions.questionnaireTemplateId, questionnaireName: submissions.questionnaireNameSnapshot,
    questionnaireVersion: submissions.questionnaireVersionSnapshot, startedAt: sql<Date | null>`coalesce(${SMDurcharbeitTimes.startedAt},${submissions.visitStartedAt})`,
    completedAt: sql<Date | null>`coalesce(${SMDurcharbeitTimes.completedAt},${submissions.visitCompletedAt})`, submittedAt: submissions.submittedAt, answeredCount: submissions.answeredQuestionCount,
    SMDurcharbeitContext: sql<unknown>`case when ${submissions.SMDurcharbeitVisitId} is not null then json_build_object('visitId',${submissions.SMDurcharbeitVisitId},'targetId',${SMDurcharbeitTargets.id},'campaignId',${SMDurcharbeitCampaigns.id},'campaignName',${SMDurcharbeitCampaigns.name},'month',${SMDurcharbeitPeriods.month},'timeRevision',${SMDurcharbeitTimes.revisionNumber}) end`,
    SMDurcharbeitCatalogScope: sql<"standard" | "SMDurcharbeit">`case when starts_with(${smQuestionnaireTemplates.stableCode}, 'smdurcharbeit_') then 'SMDurcharbeit' else 'standard' end`,
  }).from(submissions).leftJoin(smAssignments, eq(smAssignments.id, submissions.assignmentId)).leftJoin(smQuestionnaireTemplates, eq(smQuestionnaireTemplates.id, submissions.questionnaireTemplateId))
    .leftJoin(SMDurcharbeitTimes, and(eq(SMDurcharbeitTimes.visitId, submissions.SMDurcharbeitVisitId),eq(SMDurcharbeitTimes.isCurrent,true)))
    .leftJoin(SMDurcharbeitTargets,eq(SMDurcharbeitTargets.id,submissions.SMDurcharbeitTargetId)).leftJoin(SMDurcharbeitPeriods,eq(SMDurcharbeitPeriods.id,SMDurcharbeitTargets.periodId))
    .leftJoin(SMDurcharbeitCampaigns,eq(SMDurcharbeitCampaigns.id,SMDurcharbeitTargets.campaignId)).where(and(...filters))
    .orderBy(desc(workDate), desc(submissions.id)).limit(input.limit + 1);
  const facets = includeFacets ? await tx.selectDistinct({ smUserId: submissions.smUserId, smName: submissions.smNameSnapshot,
    marketId: submissions.smMarketId, marketName: submissions.marketNameSnapshot,
    questionnaireId: submissions.questionnaireTemplateId, questionnaireName: submissions.questionnaireNameSnapshot,
    SMDurcharbeitCampaignId: SMDurcharbeitCampaigns.id, SMDurcharbeitCampaignName: SMDurcharbeitCampaigns.name, SMDurcharbeitMonth: SMDurcharbeitPeriods.month,
    SMDurcharbeitCatalogScope: sql<"standard" | "SMDurcharbeit">`case when starts_with(${smQuestionnaireTemplates.stableCode}, 'smdurcharbeit_') then 'SMDurcharbeit' else 'standard' end`,
  }).from(submissions).leftJoin(smAssignments, eq(smAssignments.id, submissions.assignmentId)).leftJoin(smQuestionnaireTemplates, eq(smQuestionnaireTemplates.id, submissions.questionnaireTemplateId))
    .leftJoin(SMDurcharbeitTimes, and(eq(SMDurcharbeitTimes.visitId, submissions.SMDurcharbeitVisitId),eq(SMDurcharbeitTimes.isCurrent,true)))
    .leftJoin(SMDurcharbeitTargets,eq(SMDurcharbeitTargets.id,submissions.SMDurcharbeitTargetId)).leftJoin(SMDurcharbeitPeriods,eq(SMDurcharbeitPeriods.id,SMDurcharbeitTargets.periodId))
    .leftJoin(SMDurcharbeitCampaigns,eq(SMDurcharbeitCampaigns.id,SMDurcharbeitTargets.campaignId)).where(and(...scope))
    .orderBy(asc(submissions.smNameSnapshot), asc(submissions.marketNameSnapshot), asc(submissions.questionnaireNameSnapshot)).limit(2001) : [];
  const items = rows.slice(0, input.limit), last = items.at(-1);
  return { visits: items, nextCursor: rows.length > input.limit && last ? { date: last.workDate, id: last.id } : null,
    facets: facets.slice(0, 2000), facetsTruncated: facets.length > 2000 };
}
