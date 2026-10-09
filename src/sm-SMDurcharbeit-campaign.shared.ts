import { createHash, randomUUID } from "node:crypto";
import { and, asc, desc, eq, inArray, isNull, sql } from "drizzle-orm";
import type { db } from "./lib/db.js";
import { smSMDurcharbeitCampaigns as campaigns, smSMDurcharbeitCampaignMarkets as memberships,
  smSMDurcharbeitPeriods as periods, smSMDurcharbeitTargets as targets, smSMDurcharbeitOwnerRevisions as owners,
  smSMDurcharbeitVisits as visits, smSMDurcharbeitEvents as events, smQuestionnaireSubmissions as submissions,
  smSMDurcharbeitTimeRevisions as times, smQuestionnaireVersions, smQuestionnaireTemplates,
  smMarkets, smSMDurcharbeitMarkets, users, smQuestionnaireVersionModules, smModuleVersions, smModuleVersionQuestions, smQuestionVersions } from "./lib/schema.js";
import { isIsoDate } from "./sm-planning.shared.js";

export type SMDurcharbeitTx = Parameters<Parameters<typeof db.transaction>[0]>[0];
export type SMDurcharbeitExecutor = typeof db | SMDurcharbeitTx;
export class SMDurcharbeitCampaignError extends Error {
  constructor(public statusCode: number, public code: string, message: string) { super(message); }
}
const fail = (code: string, message: string, status = 409): never => { throw new SMDurcharbeitCampaignError(status, code, message); };
export function SMDurcharbeitToday(at = new Date()) {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Vienna", year: "numeric", month: "2-digit", day: "2-digit" }).format(at);
}
export function SMDurcharbeitMonth(value = SMDurcharbeitToday()) {
  if (!isIsoDate(value)) return fail("smdurcharbeit_month_invalid", "Ungültiger Kalendermonat.", 400);
  return `${value.slice(0,7)}-01`;
}
export function SMDurcharbeitMonths(start: string, end: string) {
  if (!isIsoDate(start) || !isIsoDate(end) || end < start) return fail("smdurcharbeit_window_invalid", "Bitte einen gültigen Kampagnenzeitraum auswählen.", 400);
  const result: string[] = [];
  for (let date = SMDurcharbeitMonth(start); date <= SMDurcharbeitMonth(end);) {
    result.push(date);
    if (result.length > 24) return fail("smdurcharbeit_window_too_large", "Eine Kampagne darf höchstens 24 Kalendermonate umfassen.", 400);
    const next = new Date(`${date}T12:00:00Z`); next.setUTCMonth(next.getUTCMonth()+1); date = next.toISOString().slice(0,10);
  }
  return result;
}
export async function lockSMDurcharbeitTarget(tx: SMDurcharbeitTx, targetId: string) {
  const [identity] = await tx.select({ campaignId: targets.campaignId }).from(targets).where(eq(targets.id, targetId)).limit(1);
  if (!identity) return fail("smdurcharbeit_target_missing", "Dieses Monatsziel wurde nicht gefunden.", 404);
  // All execution/roster/review writers use campaign -> target -> submission.
  await tx.select({ id: campaigns.id }).from(campaigns).where(eq(campaigns.id, identity.campaignId)).for("update");
  await tx.select({ id: targets.id }).from(targets).where(eq(targets.id,targetId)).for("update");
}
export async function lockSMDurcharbeitSubmissionContext(tx: SMDurcharbeitTx, submissionId: string) {
  const [identity] = await tx.select({ targetId: submissions.SMDurcharbeitTargetId }).from(submissions).where(eq(submissions.id, submissionId)).limit(1);
  if (identity?.targetId) await lockSMDurcharbeitTarget(tx, identity.targetId);
  return identity?.targetId ?? null;
}
export async function SMDurcharbeitEvent(tx: SMDurcharbeitTx, input: typeof events.$inferInsert) { await tx.insert(events).values(input); }
export async function loadSMDurcharbeitTarget(executor: SMDurcharbeitExecutor, targetId: string) {
  const [row] = await executor.select({ target: targets, campaign: campaigns, period: periods, membership: memberships, owner: owners, market: smMarkets })
    .from(targets).innerJoin(campaigns,eq(campaigns.id,targets.campaignId)).innerJoin(periods,eq(periods.id,targets.periodId))
    .innerJoin(memberships,eq(memberships.id,targets.campaignMarketId)).innerJoin(owners,eq(owners.id,targets.ownerRevisionId))
    .innerJoin(smMarkets,eq(smMarkets.id,memberships.smMarketId)).where(eq(targets.id,targetId)).limit(1);
  if (!row) return fail("smdurcharbeit_target_missing", "Dieses Monatsziel wurde nicht gefunden.",404);
  return row;
}
export type SMDurcharbeitTargetContext = Awaited<ReturnType<typeof loadSMDurcharbeitTarget>>;
export async function assertSMDurcharbeitAvailable(executor: SMDurcharbeitExecutor, context: SMDurcharbeitTargetContext, smUserId: string, at = new Date()) {
  const today = SMDurcharbeitToday(at);
  if (context.owner.smUserId !== smUserId) return fail("smdurcharbeit_target_forbidden", "Dieser Markt ist einem anderen SM zugewiesen.",403);
  const [user] = await executor.select().from(users).where(and(eq(users.id,smUserId),eq(users.role,"sm"),eq(users.isActive,true),isNull(users.deletedAt))).limit(1);
  if (!user) return fail("smdurcharbeit_user_inactive", "Der SM-Zugang ist nicht aktiv.",403);
  if (context.campaign.status !== "published" || today < context.campaign.startDate || today > context.campaign.endDate || context.period.month !== SMDurcharbeitMonth(today)) {
    return fail("smdurcharbeit_period_unavailable", "Dieser Kampagnenmonat ist nicht für neue Besuche geöffnet.");
  }
  if (context.target.eligibility !== "required" || !context.market.isActive || context.market.isDeleted) return fail("smdurcharbeit_target_unavailable", "Dieser Markt ist derzeit nicht für Durcharbeit freigegeben.");
  return user;
}
export async function loadSMDurcharbeitVersion(executor: SMDurcharbeitExecutor, versionId: string) {
  const [row] = await executor.select({ version: smQuestionnaireVersions, template: smQuestionnaireTemplates }).from(smQuestionnaireVersions)
    .innerJoin(smQuestionnaireTemplates,eq(smQuestionnaireTemplates.id,smQuestionnaireVersions.questionnaireTemplateId))
    .where(eq(smQuestionnaireVersions.id,versionId)).limit(1);
  if (!row || !row.template.stableCode.startsWith("smdurcharbeit_") || row.version.status !== "published" || row.version.isDeleted || row.template.isDeleted || row.template.status !== "active") return fail("smdurcharbeit_version_unavailable", "Bitte einen aktiven veröffentlichten Durcharbeit-Fragebogen auswählen.");
  const [question] = await executor.select({ id: smQuestionVersions.id }).from(smQuestionnaireVersionModules)
    .innerJoin(smModuleVersions, eq(smModuleVersions.id, smQuestionnaireVersionModules.moduleVersionId))
    .innerJoin(smModuleVersionQuestions, eq(smModuleVersionQuestions.moduleVersionId, smModuleVersions.id))
    .innerJoin(smQuestionVersions, eq(smQuestionVersions.id, smModuleVersionQuestions.questionVersionId))
    .where(and(eq(smQuestionnaireVersionModules.questionnaireVersionId, versionId), eq(smQuestionnaireVersionModules.isDeleted, false),
      eq(smModuleVersions.status, "published"), eq(smModuleVersions.isDeleted, false), eq(smModuleVersionQuestions.isDeleted, false),
      eq(smQuestionVersions.status, "published"), eq(smQuestionVersions.isDeleted, false))).limit(1);
  if (!question) return fail("smdurcharbeit_version_empty", "Der veröffentlichte Durcharbeit-Fragebogen enthält keine verfügbaren Fragen.");
  return row.version;
}

export async function SMDurcharbeitPublicationPreview(executor: SMDurcharbeitExecutor, campaignId: string) {
  const [campaign] = await executor.select().from(campaigns).where(eq(campaigns.id,campaignId)).limit(1);
  if (!campaign) return fail("smdurcharbeit_campaign_missing","Kampagne nicht gefunden.",404);
  const version = await loadSMDurcharbeitVersion(executor,campaign.questionnaireVersionId);
  const months = SMDurcharbeitMonths(campaign.startDate,campaign.endDate);
  if (!campaign.rosterDraft.length || campaign.rosterDraft.length > 5000 || new Set(campaign.rosterDraft.map(r=>r.smMarketId)).size !== campaign.rosterDraft.length) return fail("smdurcharbeit_roster_invalid","Bitte unterschiedliche Durcharbeit-Märkte auswählen (maximal 5000).",400);
  const marketIds = campaign.rosterDraft.map(row=>row.smMarketId);
  const markets = await executor.select({ market: smMarkets, source: smSMDurcharbeitMarkets }).from(smSMDurcharbeitMarkets)
    .innerJoin(smMarkets,eq(smMarkets.id,smSMDurcharbeitMarkets.smMarketId)).where(inArray(smMarkets.id,marketIds)).orderBy(asc(smMarkets.id));
  if (markets.length !== marketIds.length || markets.some(r=>r.market.isDeleted || !r.market.isActive)) return fail("smdurcharbeit_roster_market_unavailable","Ein ausgewählter Durcharbeit-Markt ist nicht mehr aktiv.");
  const peopleIds = [...new Set(campaign.rosterDraft.flatMap(r=>r.smUserId?[r.smUserId]:[]))];
  const people = peopleIds.length ? await executor.select({id:users.id,isActive:users.isActive,role:users.role,deletedAt:users.deletedAt}).from(users).where(inArray(users.id,peopleIds)).orderBy(asc(users.id)) : [];
  const activeIds = new Set(people.filter(p=>p.isActive && p.role==="sm" && !p.deletedAt).map(p=>p.id));
  const unresolved = campaign.rosterDraft.filter(r=>!r.smUserId || !activeIds.has(r.smUserId)).map(r=>r.smMarketId);
  // Preview detects new campaign obligations overlapping an existing published one.
  const overlapping = await executor.select({ id: campaigns.id, name: campaigns.name, marketId: memberships.smMarketId }).from(memberships)
    .innerJoin(campaigns,eq(campaigns.id,memberships.campaignId)).where(and(inArray(memberships.smMarketId,marketIds),eq(campaigns.status,"published"),
      sql`${campaigns.id} <> ${campaign.id}::uuid and ${campaigns.startDate} <= ${campaign.endDate}::date and ${campaigns.endDate} >= ${campaign.startDate}::date`));
  const legacy = await executor.execute(sql<{id:string;marketId:string;workDate:string}>`select id, coalesce(replacement_sm_market_id,original_sm_market_id) as "marketId",coalesce(replacement_work_date,original_work_date)::text as "workDate"
    from sm_assignments where not is_deleted and status in ('planned','confirmed','open','in_progress')
      and ${inArray(sql`coalesce(replacement_sm_market_id,original_sm_market_id)`,marketIds)}
      and coalesce(replacement_work_date,original_work_date) between ${campaign.startDate}::date and ${campaign.endDate}::date order by id`);
  const previewToken = createHash("sha256").update(JSON.stringify({campaign,versionId:version.id,markets,people,months,overlapping,legacy})).digest("hex");
  return { campaign,version,months,marketCount:markets.length,targetCount:markets.length*months.length,unresolved,overlapping,legacy,previewToken,markets };
}
export async function publishSMDurcharbeitCampaign(tx: SMDurcharbeitTx, id: string, actorId: string, input: {expectedRevision:number;previewToken:string;confirmOverlap?:boolean | undefined}) {
  await tx.select({id:campaigns.id}).from(campaigns).where(eq(campaigns.id,id)).for("update");
  const preview = await SMDurcharbeitPublicationPreview(tx,id);
  if (preview.campaign.status !== "draft") return fail("smdurcharbeit_campaign_not_draft","Diese Kampagne wurde bereits veröffentlicht.");
  if (preview.campaign.revision !== input.expectedRevision || preview.previewToken !== input.previewToken) return fail("smdurcharbeit_preview_stale","Die Vorschau wurde geändert. Bitte erneut prüfen.");
  if (preview.unresolved.length) return fail("smdurcharbeit_roster_unassigned","Bitte jedem Markt einen aktiven SM zuweisen.");
  if ((preview.overlapping.length || preview.legacy.length) && !input.confirmOverlap) return fail("smdurcharbeit_roster_overlap","Es gibt bereits Kampagnen oder offene datierte Einsätze für diese Märkte. Bitte die Überschneidung ausdrücklich bestätigen.");
  const memberRows = preview.markets.map(({market})=>({id:randomUUID(),campaignId:id,smMarketId:market.id}));
  const periodRows = preview.months.map(month=>({id:randomUUID(),campaignId:id,month,questionnaireVersionId:preview.version.id}));
  await tx.insert(memberships).values(memberRows); await tx.insert(periods).values(periodRows);
  const drafts = new Map(preview.campaign.rosterDraft.map(r=>[r.smMarketId,r]));
  const sources = new Map(preview.markets.map(r=>[r.market.id,r]));
  for (const period of periodRows) {
    const ownerRows = memberRows.map(member=>({id:randomUUID(),campaignMarketId:member.id,month:period.month,smUserId:drafts.get(member.smMarketId)!.smUserId!,sourcePerson:sources.get(member.smMarketId)!.source.SMDurcharbeitVerplanung,reason:"Kampagne veröffentlicht",actorUserId:actorId}));
    await tx.insert(owners).values(ownerRows);
    await tx.insert(targets).values(memberRows.map((member,index)=>{
      const m = sources.get(member.smMarketId)!.market;
      return {id:randomUUID(),campaignId:id,periodId:period.id,campaignMarketId:member.id,ownerRevisionId:ownerRows[index]!.id,
        marketSnapshot:{id:m.id,name:m.name,chain:m.chain,address:m.address,postalCode:m.postalCode,city:m.city,region:m.region,internalId:m.internalMarketId??m.id}};
    }));
  }
  await tx.update(campaigns).set({status:"published",revision:sql`${campaigns.revision}+1`,updatedByUserId:actorId,updatedAt:new Date()}).where(eq(campaigns.id,id));
  await SMDurcharbeitEvent(tx,{campaignId:id,actorUserId:actorId,action:"published",reason:"Monatliche Durcharbeit veröffentlicht",afterState:{months:preview.months,marketCount:preview.marketCount}});
}

export async function listSMDurcharbeitTargets(executor: SMDurcharbeitExecutor, input:{month:string;campaignId?:string;smUserId?:string}) {
  const month = SMDurcharbeitMonth(input.month);
  const filters = [eq(periods.month,month),sql`${campaigns.status} <> 'draft'`];
  if (input.campaignId) filters.push(eq(campaigns.id,input.campaignId));
  if (input.smUserId) filters.push(eq(owners.smUserId,input.smUserId));
  const rows = await executor.select({target:targets,campaign:campaigns,period:periods,owner:owners,member:memberships,
    marketAvailable: sql<boolean>`not ${smMarkets.isDeleted} and ${smMarkets.isActive}`,
    user:{firstName:users.firstName,lastName:users.lastName,isActive:users.isActive,deletedAt:users.deletedAt}})
    .from(targets).innerJoin(campaigns,eq(campaigns.id,targets.campaignId)).innerJoin(periods,eq(periods.id,targets.periodId))
    .innerJoin(owners,eq(owners.id,targets.ownerRevisionId)).innerJoin(memberships,eq(memberships.id,targets.campaignMarketId)).innerJoin(users,eq(users.id,owners.smUserId))
    .innerJoin(smMarkets,eq(smMarkets.id,memberships.smMarketId))
    .where(and(...filters)).orderBy(asc(campaigns.name),asc(targets.id)).limit(10001);
  if (rows.length > 10000) return fail("smdurcharbeit_list_too_large","Bitte die Liste auf eine Kampagne eingrenzen.",400);
  const ids = rows.map(r=>r.target.id);
  const saved = ids.length ? await executor.execute<{targetId:string; draftVisitId:string|null; latestVisitId:string|null; latestSubmissionId:string|null; latestVisitSmUserId:string|null; completedAt:string|null; visitCount:number}>(sql`
    select smdurcharbeit_target_id as "targetId",
      (array_agg(smdurcharbeit_visit_id order by v.basis_revision desc,s.created_at desc,s.id desc) filter (where status='draft'))[1] as "draftVisitId",
      (array_agg(smdurcharbeit_visit_id order by v.basis_revision desc,submitted_at desc,s.id desc) filter (where status='submitted'))[1] as "latestVisitId",
      (array_agg(s.id order by v.basis_revision desc,submitted_at desc,s.id desc) filter (where status='submitted'))[1] as "latestSubmissionId",
      (array_agg(s.sm_user_id order by v.basis_revision desc,submitted_at desc,s.id desc) filter (where status='submitted'))[1] as "latestVisitSmUserId",
      max(submitted_at) filter (where status='submitted') as "completedAt",
      (count(*) filter (where status='submitted'))::int as "visitCount"
    from sm_questionnaire_submissions s join sm_smdurcharbeit_visits v on v.id=s.smdurcharbeit_visit_id
    where ${inArray(sql`s.smdurcharbeit_target_id`,ids)} and not s.is_deleted and s.is_current and s.status in ('draft','submitted')
    group by smdurcharbeit_target_id`) : [];
  const grouped = new Map(saved.map(row => [row.targetId, row]));
  const today = SMDurcharbeitToday();
  return rows.map(r=>{
    const history = grouped.get(r.target.id);
    const available = r.campaign.status==="published" && month===SMDurcharbeitMonth(today) && today>=r.campaign.startDate && today<=r.campaign.endDate && r.target.eligibility==="required"
      && r.marketAvailable && r.user.isActive && !r.user.deletedAt;
    return {id:r.target.id,revision:r.target.revision,campaignId:r.campaign.id,campaignName:r.campaign.name,campaignStatus:r.campaign.status,startDate:r.campaign.startDate,endDate:r.campaign.endDate,month,market:r.target.marketSnapshot,smUserId:r.owner.smUserId,smName:`${r.user.firstName} ${r.user.lastName}`.trim(),eligibility:r.target.eligibility,waiverReason:r.target.waiverReason,completed:Boolean(history?.latestSubmissionId),available,draftVisitId:history?.draftVisitId??null,latestVisitId:history?.latestVisitId??null,latestVisitSmUserId:history?.latestVisitSmUserId??null,latestSubmissionId:history?.latestSubmissionId??null,completedAt:history?.completedAt ? new Date(history.completedAt).toISOString() : null,visitCount:history?.visitCount??0};
  });
}
export async function reconcileSMDurcharbeitTarget(tx:SMDurcharbeitTx,targetId:string,actorId:string,action:string) {
  await lockSMDurcharbeitTarget(tx,targetId);
  const [latest] = await tx.select({id:submissions.id}).from(submissions).innerJoin(visits,eq(visits.id,submissions.SMDurcharbeitVisitId)).where(and(eq(submissions.SMDurcharbeitTargetId,targetId),eq(submissions.status,"submitted"),eq(submissions.isCurrent,true),eq(submissions.isDeleted,false))).orderBy(desc(visits.basisRevision),desc(submissions.submittedAt),desc(submissions.id)).limit(1);
  const context=await loadSMDurcharbeitTarget(tx,targetId);
  await tx.update(targets).set({latestSubmissionId:latest?.id??null,revision:sql`${targets.revision}+1`,updatedAt:new Date()}).where(eq(targets.id,targetId));
  await SMDurcharbeitEvent(tx,{campaignId:context.campaign.id,targetId,actorUserId:actorId,action,reason:"Monatsstand aktualisiert",afterState:{latestSubmissionId:latest?.id??null}});
}
export async function editSMDurcharbeitTarget(tx:SMDurcharbeitTx,targetId:string,actorId:string,input:{expectedRevision:number;reason:string;smUserId?:string | undefined;eligibility?:"required"|"waived" | undefined;scope:"month"|"future"}) {
  const identity=await loadSMDurcharbeitTarget(tx,targetId);
  await tx.select({id:campaigns.id}).from(campaigns).where(eq(campaigns.id,identity.campaign.id)).for("update");
  await lockSMDurcharbeitTarget(tx,targetId);
  const context=await loadSMDurcharbeitTarget(tx,targetId);
  if(context.target.revision!==input.expectedRevision) return fail("smdurcharbeit_target_stale","Der Monatsstand wurde geändert. Bitte neu laden.");
  if(context.period.month<SMDurcharbeitMonth()) return fail("smdurcharbeit_month_closed","Abgeschlossene Kalendermonate bleiben unverändert.");
  const selected = input.scope==="future" ? await tx.select({id:targets.id}).from(targets).innerJoin(periods,eq(periods.id,targets.periodId)).where(and(eq(targets.campaignMarketId,context.target.campaignMarketId),sql`${periods.month} >= ${context.period.month}::date`)).orderBy(asc(periods.month)) : [{id:targetId}];
  for(const row of selected) {
    await lockSMDurcharbeitTarget(tx,row.id); const current=await loadSMDurcharbeitTarget(tx,row.id);
    const [draft]=await tx.select({id:submissions.id}).from(submissions).where(and(eq(submissions.SMDurcharbeitTargetId,row.id),eq(submissions.status,"draft"),eq(submissions.isCurrent,true),eq(submissions.isDeleted,false))).limit(1);
    if(draft) return fail("smdurcharbeit_draft_protected","Ein laufender Besuch muss zuerst beendet oder ausdrücklich verworfen werden.");
    if(input.eligibility==="waived" && current.target.latestSubmissionId && current.period.month===SMDurcharbeitMonth()) return fail("smdurcharbeit_completed_target_protected","Ein erledigtes Monatsziel bleibt in der aktuellen Auswertung erhalten. Wähle einen zukünftigen Monat.");
    let ownerId=current.target.ownerRevisionId;
    if(input.smUserId && input.smUserId!==current.owner.smUserId) {
      const [person]=await tx.select({id:users.id}).from(users).where(and(eq(users.id,input.smUserId),eq(users.role,"sm"),eq(users.isActive,true),isNull(users.deletedAt))).limit(1);
      if(!person) return fail("smdurcharbeit_user_inactive","Bitte einen aktiven SM auswählen.");
      const [owner]=await tx.insert(owners).values({campaignMarketId:current.target.campaignMarketId,month:current.period.month,smUserId:input.smUserId,reason:input.reason,actorUserId:actorId}).returning(); ownerId=owner!.id;
    }
    await tx.update(targets).set({ownerRevisionId:ownerId,...(input.eligibility?{eligibility:input.eligibility,waiverReason:input.eligibility==="waived"?input.reason:null}:{}),revision:sql`${targets.revision}+1`,updatedAt:new Date()}).where(eq(targets.id,row.id));
    await SMDurcharbeitEvent(tx,{campaignId:current.campaign.id,targetId:row.id,actorUserId:actorId,action:"roster_changed",reason:input.reason,beforeState:{ownerRevisionId:current.target.ownerRevisionId,eligibility:current.target.eligibility},afterState:{ownerRevisionId:ownerId,eligibility:input.eligibility??current.target.eligibility}});
  }
}
export async function SMDurcharbeitHasCampaignReference(executor:SMDurcharbeitExecutor,templateId:string) {
  const [row]=await executor.select({id:campaigns.id}).from(campaigns).innerJoin(periods,eq(periods.campaignId,campaigns.id)).innerJoin(smQuestionnaireVersions,eq(smQuestionnaireVersions.id,periods.questionnaireVersionId))
    .where(and(eq(smQuestionnaireVersions.questionnaireTemplateId,templateId),sql`${campaigns.status} in ('published','paused') and ${campaigns.endDate} >= ${SMDurcharbeitToday()}::date`)).limit(1);
  return Boolean(row);
}

export async function loadSMDurcharbeitOwnedVisit(executor:SMDurcharbeitExecutor,visitId:string,smUserId:string,lock=false) {
  const [visit]=await executor.select().from(visits).where(eq(visits.id,visitId)).limit(1);
  if(!visit) return fail("smdurcharbeit_visit_missing","Besuch nicht gefunden.",404);
  if(lock) await lockSMDurcharbeitTarget(executor as SMDurcharbeitTx,visit.targetId);
  if(visit.smUserId!==smUserId) return fail("smdurcharbeit_visit_forbidden","Dieser Besuch gehört einem anderen SM.",403);
  const context=await loadSMDurcharbeitTarget(executor,visit.targetId);
  const [submission]=await executor.select().from(submissions).where(and(eq(submissions.SMDurcharbeitVisitId,visitId),eq(submissions.isDeleted,false),eq(submissions.isCurrent,true))).limit(1);
  if(!submission) return fail("smdurcharbeit_submission_missing","Der Fragebogen wurde nicht gefunden.",404);
  if(lock && submission.status==="draft") {
    await assertSMDurcharbeitAvailable(executor,context,smUserId);
    if(visit.ownerRevisionId!==context.target.ownerRevisionId) return fail("smdurcharbeit_owner_changed","Die Marktzuweisung wurde geändert. Bitte die Planung kontaktieren.");
  }
  return {visit,submission,context};
}
export async function saveSMDurcharbeitVisitTime(tx:SMDurcharbeitTx,visitId:string,actorId:string,input:{startedAt:Date;completedAt:Date;travelMinutes:number;reason:string;expectedRevision?:number}) {
  const [current]=await tx.select().from(times).where(and(eq(times.visitId,visitId),eq(times.isCurrent,true))).limit(1).for("update");
  if(input.expectedRevision!==undefined && input.expectedRevision!==(current?.revisionNumber??0)) return fail("smdurcharbeit_time_stale","Die Besuchszeit wurde geändert. Bitte neu laden.");
  const duration=Math.round((input.completedAt.getTime()-input.startedAt.getTime())/60000);
  if(duration<1 || duration>1440) return fail("smdurcharbeit_time_invalid","Die Besuchszeit muss zwischen einer Minute und 24 Stunden liegen.",400);
  const [last]=await tx.select({revision:times.revisionNumber}).from(times).where(eq(times.visitId,visitId)).orderBy(desc(times.revisionNumber)).limit(1);
  if(current) await tx.update(times).set({isCurrent:false}).where(eq(times.id,current.id));
  const [created]=await tx.insert(times).values({visitId,revisionNumber:(last?.revision??0)+1,startedAt:input.startedAt,completedAt:input.completedAt,actualMinutes:duration,travelMinutes:input.travelMinutes,reason:input.reason,actorUserId:actorId}).returning();
  return created!;
}
