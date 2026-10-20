import { SM_KURTI_EXPORT_TOOL, type SmKurtiExport } from "./sm-kurti-exports.js";
import type { Responses } from "openai/resources/responses/responses";
import { and, eq, desc, sql, type SQL } from "drizzle-orm";
import { z } from "zod";
import { logger } from "./logger.js";
import { db } from "./db.js";
import * as schema from "./schema.js";
import { isFullAdminRole } from "./admin-role.js";
import { buildKurtiSmContext } from "./sm-kurti-context.js";
import { SMDurcharbeitCampaignError, listSMDurcharbeitTargets, SMDurcharbeitMonth, SMDurcharbeitToday } from "../sm-SMDurcharbeit-campaign.shared.js";
import { previewSMDurcharbeitQuestionnaireUpdate } from "../sm-SMDurcharbeit-questionnaire-update.shared.js";
import { loadSMDurcharbeitReport } from "../sm-SMDurcharbeit-report.shared.js";
import { loadSmDashboardRows } from "../routes/sm-dashboard.js";
import { aggregateSmDashboard } from "../sm-dashboard.shared.js";
import { isIsoDate, isoDateToEpochDay } from "../sm-planning.shared.js";
import { smManagementDetail } from "../sm-management.js";
import { ADMIN_KURTI_VISUALIZATION_TOOLS } from "./admin-kurti-visualization-tools.js";
import { ADMIN_KURTI_TOOLS, executeAdminKurtiTool } from "./admin-kurti-tools.js";
const id=z.string().uuid(), nullableId=id.nullable(), text=z.string().trim().max(200).nullable();
const date=z.string().refine(isIsoDate,"Ungültiges Kalenderdatum.");
const limit=z.number().int().min(1).max(150);
const month=date.refine(v=>v.endsWith("-01"),"Monat muss mit dem ersten Tag angegeben werden.");
const scope=z.enum(["standard","SMDurcharbeit"]).nullable();
const rangeFields={from:date,to:date,smUserId:nullableId,marketId:nullableId,scope};
type Range=z.infer<z.ZodObject<typeof rangeFields>>;
type Actor={appUserId:string;role:schema.UserRole};
const uuidJson={type:"string",format:"uuid"}, nullableUuidJson={type:["string","null"],format:"uuid"};
const stringJson={type:["string","null"],maxLength:200};
const dateJson={type:"string",pattern:"^\\d{4}-\\d{2}-\\d{2}$"};
const rangeJson={from:dateJson,to:dateJson,smUserId:nullableUuidJson,marketId:nullableUuidJson,
 scope:{type:["string","null"],enum:["standard","SMDurcharbeit",null]}};
const limitJson={type:"integer",minimum:1,maximum:150};
const registry=new Map<string,{definition:Responses.FunctionTool;parse:z.ZodType;run:(args:any)=>Promise<unknown>}>();
function register(name:string,description:string,properties:Record<string,unknown>,parse:z.ZodType,run:(args:any)=>Promise<unknown>){
 registry.set(name,{definition:{type:"function",name,description,strict:true,parameters:{type:"object",properties,required:Object.keys(properties),additionalProperties:false}},parse,run});
}
function ranged<T extends z.ZodRawShape>(extra:T){return z.object({...rangeFields,...extra}).strict().refine(a=>{
 const r=a as Range;return r.to>=r.from&&isoDateToEpochDay(r.to)-isoDateToEpochDay(r.from)<=366;
},"Zeitraum muss aufsteigend und höchstens 366 Tage lang sein.");}
function page<T>(rows:T[],max:number){return {rows:rows.slice(0,max),shown:Math.min(rows.length,max),truncated:rows.length>max};}
function search(value:string|null,expression:SQL){return value?sql`${expression} ilike ${"%"+value.replace(/[\\%_]/g,"\\$&")+"%"}`:sql`true`;}
function visitWhere(a:Range, effectiveDate:SQL=sql`coalesce(s.visit_started_at,s.submitted_at,s.created_at)`, retainedTimes=false){
 const identity=retainedTimes?sql`((s.smdurcharbeit_visit_id is null and not s.is_deleted and s.is_current and s.status='submitted') or (s.smdurcharbeit_visit_id is not null and exists(select 1 from sm_smdurcharbeit_visit_time_revisions old_time where old_time.visit_id=s.smdurcharbeit_visit_id) and s.id=(select latest.id from sm_questionnaire_submissions latest where latest.smdurcharbeit_visit_id=s.smdurcharbeit_visit_id order by latest.revision_number desc,latest.created_at desc,latest.id desc limit 1)))`:sql`not s.is_deleted and s.is_current`;
 return sql`${identity}
 and (${effectiveDate} at time zone 'Europe/Vienna')::date between ${a.from}::date and ${a.to}::date
 and (${a.smUserId}::uuid is null or s.sm_user_id=${a.smUserId}::uuid) and (${a.marketId}::uuid is null or s.sm_market_id=${a.marketId}::uuid)
 and (${a.scope}::text is null or (case when qt.stable_code like 'smdurcharbeit\\_%' then 'SMDurcharbeit' else 'standard' end)=${a.scope})`;}
register("get_sm_app_guide","SM navigation, ownership/version semantics and KPI definitions. No GM concepts.",{},z.object({}).strict(),async()=>({
 pages:["/admin/sm/dashboard","/admin/sm/maerkte","/admin/sm/durcharbeit-maerkte","/admin/sm/verplanung","/admin/sm/durcharbeit-verplanung","/admin/sm/fragebogen","/admin/sm/fbmanagement","/admin/sm/zeiterfassung","/admin/sm/nachrichten","/admin/sm/fotoarchiv"],
 rules:["Kurti liest nur. Änderungen erfolgen auf der jeweiligen Seite.",
 "Durcharbeit nutzt Kalendermonate, nicht RED-Monate. Mehrere Kampagnen sind möglich.",
 "Dauerhafte SM-ID am Markt, Monatsvertretung und historischer Besuchsautor unterscheiden.",
 "Pausiert ist im SM-UI unsichtbar. Alte Besuche behalten ihre Fragen/Antworten; neue veröffentlichte Version bewusst in der Kampagne übernehmen.",
 "Besuchte Märkte=distinct marketId; Marktbesuche=alle gültigen abgeschlossenen Besuche; Monatsziel-Erfüllung ist kein Besuchszähler.",
 "Basistätigkeiten=(Ja+0.5*Teilweise)/(Ja+Teilweise+Nein)*100, explizite Zuordnungen unabhängig vom Antwortwortlaut.",
 "OOS-Marktquote ist die bestehende Dashboard-Marktquote; foundCategoryCases zählt Kategorie-Fälle je Besuch. Keine Nenner vermischen.",
 "Verfügbarkeit vor=(Prüfungen-gefunden)/Prüfungen; Gewinn=behoben/Prüfungen; nach=vor+Gewinn. Prozentpunkte, kein relatives Wachstum. Null=keine Beobachtung."]}));
register("get_sm_overview","Exact live SM/market/draft counts and monthly campaign states.",{},z.object({}).strict(),async()=>{
 const counts=await db.execute(sql`select
 (select count(*)::int from users where role='sm' and is_active and deleted_at is null) as "activeSms",
 (select count(*)::int from sm_markets where not is_deleted and is_active) as "activeMarkets",
 (select count(*)::int from sm_smdurcharbeit_markets d join sm_markets m on m.id=d.sm_market_id where not m.is_deleted and m.is_active) as "durcharbeitMarkets",
 (select count(*)::int from sm_smdurcharbeit_markets d join sm_markets m on m.id=d.sm_market_id where not m.is_deleted and m.is_active and d.smdurcharbeit_sm_user_id is null) as "durcharbeitWithoutSmId",
 (select count(*)::int from sm_questionnaire_submissions where not is_deleted and is_current and status='draft') as "openVisits"`);
 return {counts:counts[0],campaigns:await db.execute(sql`select status,count(*)::int as count from sm_smdurcharbeit_campaigns group by status`),today:SMDurcharbeitToday(),month:SMDurcharbeitMonth()};
});
register("search_sms","Find SM IDs by name/email and active status. No GM/customer/admin rows or credentials.",
 {search:stringJson,active:{type:["boolean","null"]},limit:limitJson},z.object({search:text,active:z.boolean().nullable(),limit}).strict(),async a=>
 page(await db.execute(sql`select id,first_name as "firstName",last_name as "lastName",email,is_active as active,sm_travel_time_enabled as "travelTimeEnabled"
 from users where role='sm' and deleted_at is null and (${a.active}::boolean is null or is_active=${a.active})
 and ${search(a.search,sql`concat_ws(' ',first_name,last_name,email)`)} order by last_name,first_name,id limit ${a.limit+1}`),a.limit));
register("get_sm_context","Own operational context of a specific active SM, including effective planning, visible targets, recent visits and unread count.",
 {smUserId:uuidJson},z.object({smUserId:id}).strict(),a=>buildKurtiSmContext(a.smUserId));
register("search_sm_markets","Search SM markets by name/address/city/internal ID, chain, region, owner and standard/Durcharbeit registry. Distinguishes both permanent owner IDs.",
 {search:stringJson,chain:stringJson,region:stringJson,smUserId:nullableUuidJson,scope:rangeJson.scope,limit:limitJson},
 z.object({search:text,chain:text,region:text,smUserId:nullableId,scope,limit}).strict(),async a=>
 page(await db.execute(sql`select m.id,m.name,m.chain,m.address,m.postal_code as "postalCode",m.city,m.region,m.internal_market_id as "internalMarketId",
 m.is_active as active,m.assigned_sm_user_id as "standardSmUserId",d.smdurcharbeit_sm_user_id as "durcharbeitSmUserId",
 d.smdurcharbeit_verplanung as "sourcePerson",concat_ws(' ',u.first_name,u.last_name) as "durcharbeitSmName"
 from sm_markets m left join sm_smdurcharbeit_markets d on d.sm_market_id=m.id left join users u on u.id=d.smdurcharbeit_sm_user_id
 where not m.is_deleted and (${a.chain}::text is null or m.chain=${a.chain}) and (${a.region}::text is null or m.region=${a.region})
 and (${a.scope}::text is null or (${a.scope}='SMDurcharbeit' and d.sm_market_id is not null) or (${a.scope}='standard' and d.sm_market_id is null))
 and (${a.smUserId}::uuid is null or (case when ${a.scope}='SMDurcharbeit' then d.smdurcharbeit_sm_user_id else m.assigned_sm_user_id end)=${a.smUserId}::uuid)
 and ${search(a.search,sql`concat_ws(' ',m.name,m.address,m.city,m.postal_code,m.internal_market_id,m.flex_number)`)}
 order by m.name,m.id limit ${a.limit+1}`),a.limit));
register("get_sm_market_context","One SM market, permanent owners, weekly plan and monthly memberships; no import blobs/admin notes.",
 {marketId:uuidJson},z.object({marketId:id}).strict(),async a=>({
 market:(await db.execute(sql`select m.id,m.name,m.chain,m.address,m.city,m.postal_code,m.region,m.is_active,m.internal_market_id,
 m.assigned_sm_user_id as "standardSmUserId",d.smdurcharbeit_sm_user_id as "durcharbeitSmUserId",d.smdurcharbeit_verplanung as "sourcePerson",
 m.monday_hours,m.tuesday_hours,m.wednesday_hours,m.thursday_hours,m.friday_hours
 from sm_markets m left join sm_smdurcharbeit_markets d on d.sm_market_id=m.id where m.id=${a.marketId}::uuid and not m.is_deleted`))[0]??null,
 campaigns:page(await db.execute(sql`select c.id,c.name,c.status,c.start_date,c.end_date from sm_smdurcharbeit_campaign_markets m
 join sm_smdurcharbeit_campaigns c on c.id=m.campaign_id where m.sm_market_id=${a.marketId}::uuid order by c.start_date desc,c.id limit 51`),50)}));
register("get_sm_planning","Effective dated assignments, replacements, cancellation and planned minutes. Monthly targets use get_sm_targets.",
 {...rangeJson,status:stringJson,limit:limitJson},ranged({status:text,limit}),async a=>
 page(await db.execute(sql`select a.id,a.status,a.source_type as "sourceType",coalesce(a.replacement_work_date,a.original_work_date)::text as date,
 coalesce(a.replacement_sm_user_id,a.original_sm_user_id) as "smUserId",coalesce(a.replacement_sm_market_id,a.original_sm_market_id) as "marketId",
 coalesce(a.replacement_planned_minutes,a.original_planned_minutes) as "plannedMinutes",m.name as market,concat_ws(' ',u.first_name,u.last_name) as "smName"
 from sm_assignments a join sm_markets m on m.id=coalesce(a.replacement_sm_market_id,a.original_sm_market_id)
 join users u on u.id=coalesce(a.replacement_sm_user_id,a.original_sm_user_id)
 where not a.is_deleted and coalesce(a.replacement_work_date,a.original_work_date) between ${a.from}::date and ${a.to}::date
 and (${a.smUserId}::uuid is null or u.id=${a.smUserId}::uuid) and (${a.marketId}::uuid is null or m.id=${a.marketId}::uuid)
 and (${a.status}::text is null or a.status::text=${a.status})
 and (${a.scope}::text is null or (${a.scope}='SMDurcharbeit')=(a.smdurcharbeit_questionnaire_override_version_id is not null))
 order by coalesce(a.replacement_work_date,a.original_work_date),a.id limit ${a.limit+1}`),a.limit));
register("get_sm_campaigns","Monthly campaigns and assigned questionnaire version; admins can inspect paused/draft/archived. Date range tests campaign overlap.",
 {from:dateJson,to:dateJson,search:stringJson,status:{type:["string","null"],enum:["draft","published","paused","archived",null]},limit:limitJson},
 z.object({from:date,to:date,search:text,status:z.enum(["draft","published","paused","archived"]).nullable(),limit}).strict().refine(a=>a.to>=a.from),async a=>
 page(await db.execute(sql`select c.id,c.name,c.status,c.start_date as "startDate",c.end_date as "endDate",c.revision,
 v.id as "questionnaireVersionId",v.questionnaire_template_id as "questionnaireId",v.name as questionnaire,v.version_number as version
 from sm_smdurcharbeit_campaigns c join sm_questionnaire_versions v on v.id=c.questionnaire_version_id
 where c.start_date<=${a.to}::date and c.end_date>=${a.from}::date and (${a.status}::text is null or c.status=${a.status})
 and ${search(a.search,sql`c.name`)} order by c.start_date desc,c.id limit ${a.limit+1}`),a.limit));
register("get_sm_campaign_context","Campaign and period versions plus newest-questionnaire update preview. Never applies updates.",
 {campaignId:uuidJson,month:dateJson},z.object({campaignId:id,month}).strict(),async a=>{
 const [campaign]=await db.select({id:schema.smSMDurcharbeitCampaigns.id,name:schema.smSMDurcharbeitCampaigns.name,status:schema.smSMDurcharbeitCampaigns.status,
 startDate:schema.smSMDurcharbeitCampaigns.startDate,endDate:schema.smSMDurcharbeitCampaigns.endDate,questionnaireVersionId:schema.smSMDurcharbeitCampaigns.questionnaireVersionId,
 revision:schema.smSMDurcharbeitCampaigns.revision}).from(schema.smSMDurcharbeitCampaigns).where(eq(schema.smSMDurcharbeitCampaigns.id,a.campaignId));
 if(!campaign)return {campaign:null};
 const periods=await db.select().from(schema.smSMDurcharbeitPeriods).where(eq(schema.smSMDurcharbeitPeriods.campaignId,a.campaignId)).limit(121);
 let questionnaireUpdate:unknown;
 try {questionnaireUpdate=await previewSMDurcharbeitQuestionnaireUpdate(db,a.campaignId,a.month);}
 catch(error){if(!(error instanceof SMDurcharbeitCampaignError))throw error;questionnaireUpdate={available:false,code:error.code,message:error.message};}
 return {campaign,periods:page(periods,120),questionnaireUpdate};
});
register("get_sm_targets","Canonical monthly targets with effective owner, override, waived/required, availability, completion and true visit count. Includes paused campaigns for admins.",
 {month:dateJson,campaignId:nullableUuidJson,smUserId:nullableUuidJson,limit:limitJson},
 z.object({month,campaignId:nullableId,smUserId:nullableId,limit}).strict(),async a=>{
 const targets=await listSMDurcharbeitTargets(db,{month:a.month,...(a.campaignId?{campaignId:a.campaignId}:{}),...(a.smUserId?{smUserId:a.smUserId}:{})});
 return {...page(targets,a.limit),total:targets.length,required:targets.filter(t=>t.eligibility==="required").length,
 completed:targets.filter(t=>t.eligibility==="required"&&t.completed).length,waived:targets.filter(t=>t.eligibility==="waived").length};
});
register("search_sm_visits","Current draft/submitted visits filtered by Vienna visit date, employee, market and catalog. Drafts never count as completed KPIs.",
 {...rangeJson,status:{type:["string","null"],enum:["draft","submitted",null]},search:stringJson,limit:limitJson},
 ranged({status:z.enum(["draft","submitted"]).nullable(),search:text,limit}),async a=>
 page(await db.execute(sql`select s.id,s.assignment_id as "assignmentId",s.smdurcharbeit_visit_id as "durcharbeitVisitId",s.sm_user_id as "smUserId",
 s.sm_market_id as "marketId",s.sm_name_snapshot as "smName",s.market_name_snapshot as market,s.market_city_snapshot as city,
 s.questionnaire_name_snapshot as questionnaire,s.questionnaire_version_snapshot as version,s.status,s.visit_started_at as "startedAt",
 s.submitted_at as "submittedAt",s.answered_question_count as "answeredQuestions",s.resolved_question_count as "questionCount",count(*) over()::int as "totalMatches"
 from sm_questionnaire_submissions s join sm_questionnaire_templates qt on qt.id=s.questionnaire_template_id
 where ${visitWhere(a)} and s.status in ('draft','submitted') and (${a.status}::text is null or s.status::text=${a.status})
 and ${search(a.search,sql`concat_ws(' ',s.sm_name_snapshot,s.market_name_snapshot,s.market_address_snapshot,s.market_city_snapshot,s.questionnaire_name_snapshot)`)}
 order by coalesce(s.visit_started_at,s.submitted_at,s.created_at) desc,s.id limit ${a.limit+1}`),a.limit));
register("get_sm_visit_context","Saved question snapshots, answers/comments and available photo metadata for a current submitted visit. Same review loader as UI; no signed images/storage paths.",
 {submissionId:uuidJson},z.object({submissionId:id}).strict(),async a=>{
 const [s]=await db.select({id:schema.smQuestionnaireSubmissions.id}).from(schema.smQuestionnaireSubmissions).where(and(
 eq(schema.smQuestionnaireSubmissions.id,a.submissionId),eq(schema.smQuestionnaireSubmissions.status,"submitted"),
 eq(schema.smQuestionnaireSubmissions.isCurrent,true),eq(schema.smQuestionnaireSubmissions.isDeleted,false)));
 if(!s)return {visit:null,message:"Kein aktueller abgeschlossener Fragebogen. Entwürfe siehe search_sm_visits."};
 return db.transaction(async tx=>{
 const detail=await smManagementDetail(tx,a.submissionId,{lockRead:false});
 return {...detail,version:undefined,sections:detail.sections.map(section=>({...section,questions:section.questions.map(q=>({...q,
 photos:q.photos.map(p=>({id:p.id,mimeType:p.mimeType,byteSize:p.byteSize,inherited:p.SMDurcharbeitInherited,uploadedAt:p.uploadedAt}))}))}))};
 },{isolationLevel:"repeatable read",accessMode:"read only"});
});
register("get_sm_dashboard_metrics","Canonical SM dashboard: activity, weighted basics, OOS charts and availability before/gain/after, with the UI's exact selected filters. Only valid completed visits.",
 {...rangeJson,region:stringJson,chain:stringJson},ranged({region:text,chain:text}),async a=>{
 const filters={from:a.from,to:a.to,...(a.smUserId?{smUserId:a.smUserId}:{}),...(a.marketId?{marketId:a.marketId}:{}),
 ...(a.region?{region:a.region}:{}),...(a.chain?{chain:a.chain}:{}),...(a.scope?{SMDurcharbeitCatalogScope:a.scope}:{})};
 return db.transaction(async tx=>{
 const {visits,oosRows,overview}=await loadSmDashboardRows(filters,tx,true);
 return {filters,...aggregateSmDashboard(visits,oosRows),overview,
 interpretation:"availabilityGain is recovered share of observed category checks (percentage points), never relative growth or an assumed visits×4 denominator."};
 },{isolationLevel:"repeatable read",accessMode:"read only"});
});
register("get_sm_time_context","Current corrected visit/travel minutes for dated and monthly visits. Original visit author remains after reassignment. Retained monthly times stay visible after questionnaire withdrawal; questionnaireComplete identifies reporting validity. No GM odometers/day sessions.",
 {...rangeJson,limit:limitJson},ranged({limit}),async a=>
 page(await db.execute(sql`select s.id as "submissionId",s.smdurcharbeit_visit_id as "durcharbeitVisitId",s.sm_name_snapshot as "smName",
 s.sm_user_id as "smUserId",s.market_name_snapshot as market,s.sm_market_id as "marketId",
 (s.status='submitted' and s.is_current and not s.is_deleted) as "questionnaireComplete",
 coalesce(mt.started_at,s.visit_started_at) as "startedAt",coalesce(mt.completed_at,s.visit_completed_at) as "completedAt",
 case when s.smdurcharbeit_visit_id is not null then mt.actual_minutes else t.actual_minutes end as "actualMinutes",
 coalesce(mt.travel_minutes,s.travel_minutes) as "travelMinutes",coalesce(mt.revision_number,t.revision_number) as "timeRevision"
 from sm_questionnaire_submissions s join sm_questionnaire_templates qt on qt.id=s.questionnaire_template_id
 left join sm_assignment_time_submissions t on t.assignment_id=s.assignment_id and t.is_current and not t.is_deleted
 left join sm_smdurcharbeit_visit_time_revisions mt on mt.visit_id=s.smdurcharbeit_visit_id and mt.is_current
 where ${visitWhere(a,sql`coalesce(mt.started_at,s.visit_started_at,s.submitted_at,s.created_at)`,true)}
 order by coalesce(mt.started_at,s.visit_started_at) desc,s.id limit ${a.limit+1}`),a.limit));
register("get_sm_questionnaires","Published/draft SM questionnaire versions, catalog scopes and standard global assignment. These are definitions, not historical visit snapshots.",
 {search:stringJson,scope:rangeJson.scope,limit:limitJson},z.object({search:text,scope,limit}).strict(),async a=>{
 const rows=await db.execute(sql`select t.id as "questionnaireId",t.stable_code as code,t.status as "templateStatus",v.id as "versionId",
 v.version_number as version,v.status,v.name,v.description,v.effective_from,v.effective_to,v.once_per_market
 from sm_questionnaire_templates t join sm_questionnaire_versions v on v.questionnaire_template_id=t.id
 where not t.is_deleted and not v.is_deleted and (${a.scope}::text is null or
 (case when t.stable_code like 'smdurcharbeit\\_%' then 'SMDurcharbeit' else 'standard' end)=${a.scope})
 and ${search(a.search,sql`concat_ws(' ',v.name,t.stable_code)`)} order by v.updated_at desc,v.id limit ${a.limit+1}`);
 const global=await db.select({questionnaireId:schema.smQuestionnaireGlobalAssignments.questionnaireTemplateId}).from(schema.smQuestionnaireGlobalAssignments).where(and(eq(schema.smQuestionnaireGlobalAssignments.isDeleted,false),sql`superseded_at is null`)).limit(1);
 return {...page(rows,a.limit),globalStandardAssignment:global};
});
register("get_sm_questionnaire_context","Modules/questions for a specific version, answer options and explicit OOS/basic-activity mappings.",
 {versionId:uuidJson},z.object({versionId:id}).strict(),async a=>({
 version:(await db.select({id:schema.smQuestionnaireVersions.id,name:schema.smQuestionnaireVersions.name,status:schema.smQuestionnaireVersions.status,version:schema.smQuestionnaireVersions.versionNumber})
 .from(schema.smQuestionnaireVersions).where(and(eq(schema.smQuestionnaireVersions.id,a.versionId),eq(schema.smQuestionnaireVersions.isDeleted,false))))[0]??null,
 ...page(await db.execute(sql`select mv.id as "moduleVersionId",mv.name as module,qv.id as "questionVersionId",qv.question_id as "questionId",
 qv.question_text as question,qv.question_type as type,qv.required,qv.metric_role,qv.oos_category,qv.config,qv.metric_config,
 (select jsonb_agg(jsonb_build_object('label',o.label,'value',o.stable_code,'metricOutcome',o.metric_outcome_code) order by o.order_index)
 from sm_answer_option_versions o where o.question_version_id=qv.id and not o.is_deleted) as options
 from sm_questionnaire_version_modules qm join sm_module_versions mv on mv.id=qm.module_version_id
 join sm_module_version_questions mq on mq.module_version_id=mv.id join sm_question_versions qv on qv.id=mq.question_version_id
 where qm.questionnaire_version_id=${a.versionId}::uuid and not qm.is_deleted and not mq.is_deleted and not qv.is_deleted and not mv.is_deleted
 order by qm.order_index,mq.order_index,qv.id limit 151`),150)}));
register("get_sm_answer_analytics","Current submitted answer observation distribution, mapped outcomes and distinct visit counts; category observations are not visits.",
 {...rangeJson,questionId:nullableUuidJson,limit:limitJson},ranged({questionId:nullableId,limit}),async a=>
 page(await db.execute(sql`select q.question_text_snapshot as question,qv.question_id as "questionId",a.metric_outcome_code as outcome,
 (case when q.question_type_snapshot='photo' then jsonb_build_object('kind','photo') else a.value_json end) as value,count(*)::int as observations,count(distinct s.id)::int as visits
 from sm_questionnaire_submissions s join sm_questionnaire_templates qt on qt.id=s.questionnaire_template_id
 join sm_questionnaire_submission_questions q on q.submission_id=s.id and not q.is_deleted and q.is_applicable
 join sm_question_versions qv on qv.id=q.question_version_id
 join sm_question_answers a on a.submission_question_id=q.id and a.submission_id=s.id and a.is_current and not a.is_deleted and a.answer_state='answered'
 where ${visitWhere(a)} and s.status='submitted' and (${a.questionId}::uuid is null or qv.question_id=${a.questionId}::uuid)
 group by q.question_text_snapshot,qv.question_id,a.metric_outcome_code,(case when q.question_type_snapshot='photo' then jsonb_build_object('kind','photo') else a.value_json end)
 order by count(*) desc,qv.question_id limit ${a.limit+1}`),a.limit));
register("get_sm_pending_requests","Separate answer/delete/dated-time/monthly-time queues by status/person; no approvals or sensitive free-text reasons.",
 {smUserId:nullableUuidJson,status:{type:"string",enum:["pending","approved","rejected","cancelled"]},limit:limitJson},
 z.object({smUserId:nullableId,status:z.enum(["pending","approved","rejected","cancelled"]),limit}).strict(),async a=>{
 const tables=[schema.smAnswerChangeRequests,schema.smQuestionnaireSubmissionDeleteRequests,schema.smAssignmentTimeChangeRequests,schema.smSMDurcharbeitTimeRequests] as const;
 const names=["answer","delete","dated_time","monthly_time"];
 return Promise.all(tables.map(async(table,i)=>({kind:names[i],...page(await db.select({id:table.id,status:table.status,smUserId:table.smUserId,createdAt:table.createdAt})
 .from(table).where(and(eq(table.status,a.status),...("isDeleted" in table?[eq(table.isDeleted,false)]:[]),...(a.smUserId?[eq(table.smUserId,a.smUserId)]:[]))).orderBy(desc(table.createdAt)).limit(a.limit+1),a.limit)})));
});
register("get_sm_campaign_history","Recorded campaign decisions and reasons, month ownership, market/version changes. No unrecorded history inferred.",
 {campaignId:uuidJson,limit:limitJson},z.object({campaignId:id,limit}).strict(),async a=>
 page(await db.select({id:schema.smSMDurcharbeitEvents.id,action:schema.smSMDurcharbeitEvents.action,reason:schema.smSMDurcharbeitEvents.reason,
 before:schema.smSMDurcharbeitEvents.beforeState,after:schema.smSMDurcharbeitEvents.afterState,createdAt:schema.smSMDurcharbeitEvents.createdAt})
 .from(schema.smSMDurcharbeitEvents).where(eq(schema.smSMDurcharbeitEvents.campaignId,a.campaignId)).orderBy(desc(schema.smSMDurcharbeitEvents.createdAt)).limit(a.limit+1),a.limit));
register("get_sm_message_context","Message subjects and recipient/read counts, optionally for one employee; never sends or marks read.",
 {smUserId:nullableUuidJson,search:stringJson,limit:limitJson},z.object({smUserId:nullableId,search:text,limit}).strict(),async a=>
 page(await db.execute(sql`select m.id,m.subject,m.sent_at as "sentAt",count(*)::int as recipients,count(*) filter(where r.read_at is not null)::int as read
 from sm_messages m join sm_message_recipients r on r.message_id=m.id
 where not m.is_deleted and not r.is_deleted and (${a.smUserId}::uuid is null or r.sm_user_id=${a.smUserId}::uuid)
 and ${search(a.search,sql`m.subject`)} group by m.id order by m.sent_at desc,m.id limit ${a.limit+1}`),a.limit));
register("get_sm_access_context","Customer SM page grants only, no passwords/auth identities or access changes.",
 {search:stringJson,limit:limitJson},z.object({search:text,limit}).strict(),async a=>
 page(await db.execute(sql`select u.id,concat_ws(' ',u.first_name,u.last_name) as name,u.is_active as active,
 (select coalesce(jsonb_object_agg(key,value),'{}'::jsonb) from jsonb_each(k.page_permissions) where key like 'sm\\_%' or key='shelfmerchandiser') as "smPermissions"
 from users u join kunde_users k on k.user_id=u.id where u.role='kunde' and u.deleted_at is null and not k.is_deleted
 and ${search(a.search,sql`concat_ws(' ',u.first_name,u.last_name,u.email)`)} order by u.last_name,u.id limit ${a.limit+1}`),a.limit));
register("get_sm_kurti_chat_history","Specific SM's still-active 15-minute chat for an explicit support/quality/error reason. No GM chats and no expiry renewal.",
 {smUserId:uuidJson,reason:{type:"string",minLength:10,maxLength:300},limit:{type:"integer",minimum:1,maximum:40}},
 z.object({smUserId:id,reason:z.string().trim().min(10).max(300),limit:z.number().int().min(1).max(40)}).strict(),async a=>
 page(await db.execute(sql`select m.role,m.content,m.created_at as "createdAt",m.expires_at as "expiresAt"
 from gm_kurti_messages m join users u on u.id=m.gm_user_id where u.id=${a.smUserId}::uuid and u.role='sm'
 and u.deleted_at is null and m.expires_at>now() order by m.created_at desc,m.id limit ${a.limit+1}`),a.limit));
register("search_sm_photos","Available photo metadata for current completed visits. Uses canonical review so withdrawn/deleted inherited photos stay inaccessible. No image URLs or storage paths.",
 {...rangeJson,limit:{type:"integer",minimum:1,maximum:20}},ranged({limit:z.number().int().min(1).max(20)}),async a=>{
 const visits=await db.execute<{id:string;market:string}>(sql`select s.id,s.market_name_snapshot as market
 from sm_questionnaire_submissions s join sm_questionnaire_templates qt on qt.id=s.questionnaire_template_id
 where ${visitWhere(a)} and s.status='submitted' order by s.submitted_at desc,s.id limit ${a.limit+1}`);
 const rows=[];
 for(const visit of visits.slice(0,a.limit)){
  const detail=await db.transaction(tx=>smManagementDetail(tx,visit.id,{lockRead:false}),{accessMode:"read only"});
  rows.push({submissionId:visit.id,market:visit.market,photos:detail.sections.flatMap(section=>section.questions.flatMap(q=>q.photos.map(p=>({
   id:p.id,question:q.text,mimeType:p.mimeType,byteSize:p.byteSize,inherited:p.SMDurcharbeitInherited,uploadedAt:p.uploadedAt
  }))))});
 }
 return {rows,visitsScanned:rows.length,truncated:visits.length>a.limit,note:"Visit sample, not a full photo archive total."};
});
register("get_sm_time_analytics","Aggregate current standard/monthly visit and travel minutes by SM, day, month or market. Retained monthly times include withdrawn questionnaires and therefore can differ from completed-visit dashboard hours. Null times stay unknown and missing-time counts are explicit.",
 {...rangeJson,groupBy:{type:"string",enum:["sm","day","month","market"]},limit:limitJson},ranged({groupBy:z.enum(["sm","day","month","market"]),limit}),async a=>{
 const key=a.groupBy==="sm"?sql`s.sm_user_id::text`:a.groupBy==="market"?sql`s.sm_market_id::text`:
 a.groupBy==="month"?sql`to_char(coalesce(mt.started_at,s.visit_started_at) at time zone 'Europe/Vienna','YYYY-MM')`:
 sql`to_char(coalesce(mt.started_at,s.visit_started_at) at time zone 'Europe/Vienna','YYYY-MM-DD')`;
 const label=a.groupBy==="sm"?sql`s.sm_name_snapshot`:a.groupBy==="market"?sql`s.market_name_snapshot`:key;
 return page(await db.execute(sql`select ${key} as key,max(${label}) as label,count(*)::int as visits,
 sum(case when s.smdurcharbeit_visit_id is not null then mt.actual_minutes else t.actual_minutes end)::float as "marketMinutes",
 sum(coalesce(mt.travel_minutes,s.travel_minutes))::float as "travelMinutes",
 count(*) filter(where (case when s.smdurcharbeit_visit_id is not null then mt.actual_minutes else t.actual_minutes end) is null)::int as "missingTimeVisits"
 from sm_questionnaire_submissions s join sm_questionnaire_templates qt on qt.id=s.questionnaire_template_id
 left join sm_assignment_time_submissions t on t.assignment_id=s.assignment_id and t.is_current and not t.is_deleted
 left join sm_smdurcharbeit_visit_time_revisions mt on mt.visit_id=s.smdurcharbeit_visit_id and mt.is_current
 where ${visitWhere(a,sql`coalesce(mt.started_at,s.visit_started_at,s.submitted_at,s.created_at)`,true)}
 group by ${key} order by key limit ${a.limit+1}`),a.limit);
});
register("get_sm_visit_analytics","Submitted visits and distinct visited markets/SMs by day, month, chain, market, SM or questionnaire. Same submitted-only basis, explicit filters and no raw answer content.",
 {...rangeJson,groupBy:{type:"string",enum:["day","month","chain","market","sm","questionnaire"]},limit:limitJson},
 ranged({groupBy:z.enum(["day","month","chain","market","sm","questionnaire"]),limit}),async a=>{
 const key=a.groupBy==="sm"?sql`s.sm_user_id::text`:a.groupBy==="market"?sql`s.sm_market_id::text`:
 a.groupBy==="chain"?sql`m.chain`:a.groupBy==="questionnaire"?sql`s.questionnaire_template_id::text`:
 a.groupBy==="month"?sql`to_char(s.visit_started_at at time zone 'Europe/Vienna','YYYY-MM')`:sql`to_char(s.visit_started_at at time zone 'Europe/Vienna','YYYY-MM-DD')`;
 const label=a.groupBy==="sm"?sql`s.sm_name_snapshot`:a.groupBy==="market"?sql`s.market_name_snapshot`:
 a.groupBy==="questionnaire"?sql`s.questionnaire_name_snapshot`:key;
 return page(await db.execute(sql`select ${key} as key,max(${label}) as label,count(*)::int as visits,
 count(distinct s.sm_market_id)::int as "visitedMarkets",count(distinct s.sm_user_id)::int as merchandisers
 from sm_questionnaire_submissions s join sm_questionnaire_templates qt on qt.id=s.questionnaire_template_id join sm_markets m on m.id=s.sm_market_id
 where ${visitWhere(a)} and s.status='submitted' group by ${key} order by key limit ${a.limit+1}`),a.limit);
});
register("get_sm_campaign_report","Canonical campaign/month report: required/completed/waived, coverage, latest answers, true follow-up visits, times and available photo totals. Distinguishes latest monthly state from physical visits.",
 {campaignId:uuidJson,month:dateJson,smUserId:nullableUuidJson,limit:limitJson},z.object({campaignId:id,month,smUserId:nullableId,limit}).strict(),async a=>{
 const report=await loadSMDurcharbeitReport(db,{campaignId:a.campaignId,month:a.month,...(a.smUserId?{smUserId:a.smUserId}:{})});
 return {month:report.month,summary:report.summary,questionResults:page(report.questionResults,a.limit),physicalVisits:page(report.physicalVisits,a.limit)};
});
/** Shared presentation tools have no business-data access; never expose GM data/export tools. */
const presentationNames=new Set(["load_admin_visualization_skill","render_admin_chart"]);
const presentation=ADMIN_KURTI_TOOLS.filter(t=>presentationNames.has(t.name));
export const SM_ADMIN_KURTI_TOOLS:Responses.FunctionTool[]=[...registry.values()].map(r=>r.definition).concat(presentation,ADMIN_KURTI_VISUALIZATION_TOOLS,SM_KURTI_EXPORT_TOOL);
export const SM_ADMIN_KURTI_TOOL_NAMES=new Set(SM_ADMIN_KURTI_TOOLS.map(t=>t.name));
export async function executeSmKurtiTool(name:string,raw:string,actor:Actor):Promise<string>{
 if(!isFullAdminRole(actor.role))return JSON.stringify({error:"forbidden"});
 const handler=registry.get(name);
 if(!handler){
  if(name==="load_admin_visualization_skill")return executeAdminKurtiTool(name,raw);
  return JSON.stringify({error:"unknown_sm_tool",tool:name});
 }
 try{
  const args=handler.parse.parse(raw.trim()?JSON.parse(raw):{});
  const result=await handler.run(args);
  const serialized=JSON.stringify({tool:name,generatedAt:new Date().toISOString(),data:result});
  if(serialized.length>48000)return JSON.stringify({tool:name,error:"result_too_large",message:"Bitte Zeitraum oder Limit verkleinern."});
  return serialized;
 }catch(error){
  logger.warn("sm_kurti_tool_failed", {tool:name,code:(error as {code?:string})?.code ?? "read_failed"});
  if(error instanceof z.ZodError||error instanceof SyntaxError)return JSON.stringify({tool:name,error:"invalid_arguments",message:"Bitte Parameter, Datumsbereich und IDs prüfen."});
  return JSON.stringify({tool:name,error:"sm_tool_failed",message:"Daten konnten nicht gelesen werden. Nicht als Null interpretieren; Filter prüfen oder erneut versuchen."});
 }
}




/** Export executes fixed server tools again on download, not model-authored spreadsheet values. */
export async function loadSmKurtiExport(spec:SmKurtiExport,actor:Actor){
 if(!isFullAdminRole(actor.role))throw new Error("Forbidden");
 const f=spec.filters,common={from:f.from,to:f.to,smUserId:f.smUserId,marketId:f.marketId,scope:f.scope,limit:5000};
 const requests={
  markets:{name:"search_sm_markets",args:{search:f.search,chain:f.chain,region:f.region,smUserId:f.smUserId,scope:f.scope,limit:5000}},
  planning:{name:"get_sm_planning",args:{...common,status:null}},
  targets:{name:"get_sm_targets",args:{month:f.month,campaignId:f.campaignId,smUserId:f.smUserId,limit:5000}},
  visits:{name:"search_sm_visits",args:{...common,status:"submitted",search:f.search}},
  times:{name:"get_sm_time_context",args:common},
  answers:{name:"get_sm_answer_analytics",args:{...common,questionId:null}}
 };
 const request=requests[spec.dataset];
 const data=await registry.get(request.name)!.run(request.args) as {rows:Record<string,unknown>[];truncated:boolean};
 if(data.truncated)throw new Error("Mehr als 5.000 Zeilen. Bitte Filter eingrenzen.");
 return {title:spec.title,filters:f,generatedAt:new Date().toISOString(),rows:data.rows};
}
