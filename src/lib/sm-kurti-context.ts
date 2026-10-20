import { sql } from "drizzle-orm";
import { db } from "./db.js";
import { listSMDurcharbeitTargets, SMDurcharbeitMonth, SMDurcharbeitToday } from "../sm-SMDurcharbeit-campaign.shared.js";

/** Explicit owner queries only; no GM context or admin/profile notes. */
export async function buildKurtiSmContext(smUserId: string, now = new Date()) {
  const today = SMDurcharbeitToday(now), month = SMDurcharbeitMonth(today);
  const from = new Date(`${today}T12:00:00Z`); from.setUTCDate(from.getUTCDate() - (from.getUTCDay() + 6) % 7);
  const to = new Date(from); to.setUTCDate(to.getUTCDate() + 13);
  const [people, assignments, targets, visits, inbox, times, requests] = await Promise.all([
    db.execute(sql`select first_name as "firstName", last_name as "lastName", sm_travel_time_enabled as "travelTimeEnabled"
      from users where id=${smUserId}::uuid and role='sm' and is_active and deleted_at is null`),
    db.execute(sql`select a.id, coalesce(a.replacement_work_date,a.original_work_date)::text as date,
      a.status, m.name as market, m.address, m.city,
      coalesce(a.replacement_planned_minutes,a.original_planned_minutes) as "plannedMinutes"
      from sm_assignments a join sm_markets m on m.id=coalesce(a.replacement_sm_market_id,a.original_sm_market_id)
      where not a.is_deleted and a.status<>'cancelled' and coalesce(a.replacement_sm_user_id,a.original_sm_user_id)=${smUserId}::uuid
      and coalesce(a.replacement_work_date,a.original_work_date) between ${from.toISOString().slice(0,10)}::date and ${to.toISOString().slice(0,10)}::date
      order by coalesce(a.replacement_work_date,a.original_work_date),a.id limit 101`),
    listSMDurcharbeitTargets(db, { month, smUserId, employeeVisible: true }),
    db.execute(sql`select s.id, s.market_name_snapshot as market, s.questionnaire_name_snapshot as questionnaire,
      s.questionnaire_version_snapshot as version, s.status, s.answered_question_count as "answeredQuestions",
      s.resolved_question_count as "questionCount", s.last_saved_at as "lastSavedAt", s.submitted_at as "submittedAt",
      s.smdurcharbeit_visit_id as "monthlyVisitId"
      from sm_questionnaire_submissions s
      where s.sm_user_id=${smUserId}::uuid and not s.is_deleted and s.is_current and s.status in ('draft','submitted')
      and (s.status='draft' or s.submitted_at >= ${month}::date)
      and (s.smdurcharbeit_target_id is null or exists(select 1 from sm_smdurcharbeit_month_targets t
        join sm_smdurcharbeit_campaigns c on c.id=t.campaign_id where t.id=s.smdurcharbeit_target_id and c.status<>'paused'))
      order by (s.status='draft') desc,s.last_saved_at desc,s.id limit 31`),
    db.execute(sql`select count(*)::int as unread from sm_message_recipients r join sm_messages m on m.id=r.message_id
      where r.sm_user_id=${smUserId}::uuid and not r.is_deleted and not m.is_deleted and r.read_at is null`),
    db.execute(sql`select s.market_name_snapshot as market,coalesce(mt.started_at,s.visit_started_at) as "startedAt",
      coalesce(mt.completed_at,s.visit_completed_at) as "completedAt",
      coalesce(mt.actual_minutes,t.actual_minutes) as "actualMinutes",coalesce(mt.travel_minutes,s.travel_minutes) as "travelMinutes",
      (s.status='submitted' and s.is_current and not s.is_deleted) as "questionnaireComplete"
      from sm_questionnaire_submissions s
      left join sm_assignment_time_submissions t on t.assignment_id=s.assignment_id and t.is_current and not t.is_deleted
      left join sm_smdurcharbeit_visit_time_revisions mt on mt.visit_id=s.smdurcharbeit_visit_id and mt.is_current
      where s.sm_user_id=${smUserId}::uuid and s.is_current and (t.id is not null or mt.id is not null)
      and (coalesce(mt.started_at,s.visit_started_at) at time zone 'Europe/Vienna')::date>=${month}::date
      and (s.smdurcharbeit_target_id is null or exists(select 1 from sm_smdurcharbeit_month_targets tg
        join sm_smdurcharbeit_campaigns c on c.id=tg.campaign_id where tg.id=s.smdurcharbeit_target_id and c.status<>'paused'))
      order by coalesce(mt.started_at,s.visit_started_at) desc,s.id limit 101`),
    db.execute(sql`select
      (select count(*)::int from sm_answer_change_requests r where r.sm_user_id=${smUserId}::uuid and not r.is_deleted and r.status='pending' and exists(select 1 from sm_questionnaire_submissions s where s.id=r.submission_id and (s.smdurcharbeit_target_id is null or exists(select 1 from sm_smdurcharbeit_month_targets tg join sm_smdurcharbeit_campaigns c on c.id=tg.campaign_id where tg.id=s.smdurcharbeit_target_id and c.status<>'paused')))) as answers,
      (select count(*)::int from sm_assignment_time_change_requests where sm_user_id=${smUserId}::uuid and not is_deleted and status='pending') as "datedTimes",
      (select count(*)::int from sm_smdurcharbeit_time_change_requests r where r.sm_user_id=${smUserId}::uuid and r.status='pending' and exists(select 1 from sm_smdurcharbeit_visits v join sm_smdurcharbeit_month_targets tg on tg.id=v.target_id join sm_smdurcharbeit_campaigns c on c.id=tg.campaign_id where v.id=r.visit_id and c.status<>'paused')) as "monthlyTimes",
      (select count(*)::int from sm_questionnaire_submission_delete_requests r where r.sm_user_id=${smUserId}::uuid and not r.is_deleted and r.status='pending' and exists(select 1 from sm_questionnaire_submissions s where s.id=r.submission_id and (s.smdurcharbeit_target_id is null or exists(select 1 from sm_smdurcharbeit_month_targets tg join sm_smdurcharbeit_campaigns c on c.id=tg.campaign_id where tg.id=s.smdurcharbeit_target_id and c.status<>'paused')))) as deletions`),
  ]);
  if (!people.length) throw new Error("SM-Profil nicht verfügbar.");
  return { generatedAt: now.toISOString(), timezone:"Europe/Vienna", today, profile: people[0] ? {userId:smUserId,...people[0]} : null,
    planning: { from:from.toISOString().slice(0,10), to:to.toISOString().slice(0,10), truncated:assignments.length>100, assignments:assignments.slice(0,100) },
    durcharbeit: { month, count:targets.length, truncated:targets.length>100, targets:targets.slice(0,100).map(t=>({
      market:t.market,campaign:t.campaignName, status:t.campaignStatus, eligibility:t.eligibility, available:t.available,
      completed:t.completed, visitCount:t.visitCount, hasOpenVisit:!!t.draftVisitId, month:t.month })) },
    visits: { truncated:visits.length>30, recent:visits.slice(0,30) }, unreadMessages:inbox[0],
    times:{from:month,truncated:times.length>100,entries:times.slice(0,100)},pendingRequests:requests[0] };
}

