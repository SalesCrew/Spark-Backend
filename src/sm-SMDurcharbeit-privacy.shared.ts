import { and, eq, or, sql } from "drizzle-orm";
import { smSMDurcharbeitOwnerRevisions as owners, smSMDurcharbeitTargets as targets,
  smSMDurcharbeitVisits as visits, smSMDurcharbeitTimeRevisions as times, smSMDurcharbeitTimeRequests as requests,
  smSMDurcharbeitAnswerProvenance as provenance, smSMDurcharbeitFileLinks as links, smSMDurcharbeitEvents as events,
  smQuestionAnswers as answers, smQuestionnaireSubmissions as submissions } from "./lib/schema.js";
import type { SMDurcharbeitExecutor } from "./sm-SMDurcharbeit-campaign.shared.js";

/** Metadata inventory only: no personal answer text, photo URLs, mutation or purge. */
export async function loadSMDurcharbeitPrivacyCounts(executor: SMDurcharbeitExecutor, smUserId: string) {
  const [ownerRows, targetRows, visitRows, timeRows, requestRows, provenanceRows, linkRows, eventRows] = await Promise.all([
    executor.select({ count: sql<number>`count(*)::int` }).from(owners).where(eq(owners.smUserId, smUserId)),
    executor.select({ count: sql<number>`count(distinct ${targets.id})::int` }).from(targets)
      .innerJoin(owners, and(eq(owners.campaignMarketId, targets.campaignMarketId), eq(owners.smUserId, smUserId)))
      .where(sql`${owners.month} = (select p.month from sm_smdurcharbeit_campaign_periods p where p.id = ${targets.periodId})`),
    executor.select({ count: sql<number>`count(*)::int` }).from(visits).where(eq(visits.smUserId, smUserId)),
    executor.select({ count: sql<number>`count(*)::int` }).from(times).innerJoin(visits, eq(visits.id, times.visitId)).where(eq(visits.smUserId, smUserId)),
    executor.select({ count: sql<number>`count(*)::int` }).from(requests).where(eq(requests.smUserId, smUserId)),
    executor.select({ count: sql<number>`count(*)::int` }).from(provenance).innerJoin(answers, eq(answers.id, provenance.answerId))
      .innerJoin(submissions, eq(submissions.id, answers.submissionId)).where(eq(submissions.smUserId, smUserId)),
    executor.select({ count: sql<number>`count(*)::int` }).from(links).innerJoin(answers, eq(answers.id, links.answerId))
      .innerJoin(submissions, eq(submissions.id, answers.submissionId)).where(eq(submissions.smUserId, smUserId)),
    executor.select({ count: sql<number>`count(*)::int` }).from(events).where(or(eq(events.actorUserId, smUserId),
      sql`exists(select 1 from sm_smdurcharbeit_visits v where v.id=${events.visitId} and v.sm_user_id=${smUserId}::uuid)`,
      sql`exists(select 1 from sm_smdurcharbeit_month_targets t join sm_smdurcharbeit_campaign_periods p on p.id=t.period_id join sm_smdurcharbeit_assignment_revisions o on o.campaign_market_id=t.campaign_market_id and o.month=p.month where t.id=${events.targetId} and o.sm_user_id=${smUserId}::uuid)`)),
  ]);
  const count = (rows: Array<{ count: number }>) => Number(rows[0]?.count ?? 0);
  return { targets: count(targetRows), ownerRevisions: count(ownerRows), visits: count(visitRows),
    timeRevisions: count(timeRows), timeRequests: count(requestRows), answerProvenance: count(provenanceRows), fileLinks: count(linkRows), events: count(eventRows) };
}
