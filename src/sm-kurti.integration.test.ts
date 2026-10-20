import assert from "node:assert/strict";
import test from "node:test";
import { randomUUID } from "node:crypto";
import request from "supertest";
import { eq } from "drizzle-orm";
import { createSmKurtiFixture,toolCall } from "../tests/sm-kurti-fixture.js";
import { syntheticShelfPhoto } from "../tests/synthetic-photo-storage.js";
const range={from:"2026-10-01",to:"2026-10-31",smUserId:null,marketId:null,scope:null};
test("SM Kurti: authenticated worker, deep admin research, tool isolation, memory, exports and unchanged business history",async t=>{
 const f=await createSmKurtiFixture();
 const admin=(method:"get"|"post",url:string)=>request(f.app)[method](url).auth("synthetic-sm-admin",{type:"bearer"});
 const sm=(method:"get"|"post",url:string)=>request(f.app)[method](url).auth("synthetic-sm",{type:"bearer"});
 try{
 const question=(type:string,text:string)=>({id:"new-"+randomUUID(),type,text,required:false,options:[],config:{},rules:[]});
 const module=(await admin("post","/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({id:"new-"+randomUUID(),name:"Synthetic SM module",description:"",questions:[question("text","Produktstatus"),question("numeric","Bestand"),question("photo","Dokumentation")]}).expect(201)).body.module;
 const form=(await admin("post","/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({id:"new-"+randomUUID(),name:"Synthetic Durcharbeit FB",description:"",status:"active",moduleIds:[module.id]}).expect(201)).body.questionnaire;
 const [version]=await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId,form.id));
 await f.database.insert(f.schema.smSMDurcharbeitMarkets).values({smMarketId:f.market,SMDurcharbeitSmUserId:f.employee});
 const createCampaign=async(name:string)=>{
 const c=(await admin("post","/admin/sm-smdurcharbeit-campaigns").send({name,startDate:"2026-10-01",endDate:"2026-11-30",questionnaireVersionId:version!.id,rosterDraft:[{smMarketId:f.market}]}).expect(201)).body.campaign;
 const preview=(await admin("get",`/admin/sm-smdurcharbeit-campaigns/${c.id}/preview`).expect(200)).body;
 await admin("post",`/admin/sm-smdurcharbeit-campaigns/${c.id}/publish`).send({expectedRevision:c.revision,previewToken:preview.previewToken,confirmOverlap:true}).expect(200);
 return c;
 };
 const c=await createCampaign("Synthetic live campaign");
 const targets=(await sm("get","/sm/smdurcharbeit/targets").expect(200)).body.targets;
 const started=(await sm("post",`/sm/smdurcharbeit/targets/${targets[0].id}/start`).send({expectedRevision:targets[0].revision,followUp:false,mode:"manual",clientSubmissionToken:randomUUID()}).expect(201)).body;
 const visitPath=`/sm/smdurcharbeit/visits/${started.visitId}`;
 const payload=(await sm("get",visitPath).expect(200)).body;
 const q=payload.sections[0].questions[0];
 await request(f.app).put(visitPath+"/answers/"+q.id).auth("synthetic-sm",{type:"bearer"}).send({expectedAnswerVersion:0,answer:{kind:"text",value:"Synthetic saved answer"},clientMutationToken:randomUUID()}).expect(200);
 const photoQ=payload.sections[0].questions.find((question:any)=>question.type==="photo");
 const photoAnswer=(await sm("post",visitPath+"/photos/initialize").send({submissionQuestionId:photoQ.id}).expect(200)).body.answerId;
 const upload=(await sm("post",visitPath+"/photos/presign").send({answerId:photoAnswer,extension:"png"}).expect(200)).body.upload;
 const photoBytes=syntheticShelfPhoto(true);f.storage.put(upload.path,photoBytes);
 await sm("post",visitPath+"/photos/commit").send({answerId:photoAnswer,photos:[{storageBucket:upload.bucket,storagePath:upload.path,mimeType:"image/png",byteSize:photoBytes.length}]}).expect(200);
 await sm("post",visitPath+"/submit").send({visitStartedAt:"2026-10-20T08:00:00Z",visitCompletedAt:"2026-10-20T08:30:00Z",clientMutationToken:randomUUID()}).expect(200);
 const paused=await createCampaign("Synthetic paused campaign");
 await f.database.update(f.schema.smSMDurcharbeitCampaigns).set({status:"paused"}).where(eq(f.schema.smSMDurcharbeitCampaigns.id,paused.id));
 const assignment=await f.assignment();
 await f.database.update(f.schema.smAssignments).set({replacementSmUserId:f.other,replacementWorkDate:"2026-10-21"}).where(eq(f.schema.smAssignments.id,assignment.id));
 const businessTables=(await f.pg.query<{table_name:string}>("select table_name from information_schema.tables where table_schema='public' and (table_name like 'sm_%' or table_name='users') order by table_name")).rows.map(r=>r.table_name);
 // Some SM relation tables have composite IDs; serialize rows with deterministic JSON ordering instead.
 const graph=async()=>{const rows=await Promise.all(businessTables.map(async name=>(await f.pg.query("select row_to_json(t) as row from "+name+" t order by row_to_json(t)::text")).rows));return JSON.stringify(rows);};
 const before=await graph();
 const call=async(name:string,args:unknown)=>{
 const result=JSON.parse(await f.tools.executeSmKurtiTool(name,JSON.stringify(args),f.actor));
 assert.equal(result.error,undefined,JSON.stringify({name,result,errors:f.toolErrors}));
 return result.data;
 };
 await t.test("worker role and agreement gates; only own SM data and no paused campaign",async()=>{
 await sm("get","/sm/kurti/messages").expect(403);
 await f.accept();
 await sm("get","/sm/kurti/messages").expect(200);
 await sm("get","/gm/kurti/messages").expect(403);
 for(const token of ["synthetic-sm","synthetic-gm","synthetic-gm-admin"])await request(f.app).get("/admin/sm-kurti/messages").auth(token,{type:"bearer"}).expect(403);
 await request(f.app).get("/admin/sm-kurti/messages").expect(401);
 await sm("post","/sm/kurti/messages").send({message:"Was habe ich zu tun?",smUserId:f.other}).expect(400);
 await sm("post","/sm/kurti/messages").send({message:"Wo sind meine Einsätze?"}).expect(200);
 const model=f.calls.at(-1)!;
 assert.match(model.instructions,/Shelf Merchandiser/);
 const live=JSON.parse(model.input[0].content.split("\n").slice(1).join("\n"));
 assert.equal(live.profile.firstName,`[user:${f.employee}]`);assert.equal(live.durcharbeit.count,1);
 assert.equal(live.planning.assignments.some((a:any)=>a.id===assignment.id),false);
 assert.doesNotMatch(JSON.stringify(live),/Synthetic paused campaign|Second|onlyGm|adminInfoNote|storagePath|password/);
 const otherContext=await f.ctx.buildKurtiSmContext(f.other);
 assert.equal(otherContext.planning.assignments.some((a:any)=>a.id===assignment.id),true);
 });
 await t.test("every SM research query runs on a migrated disposable DB and uses canonical results",async()=>{
 const args:Record<string,unknown>={
 get_sm_app_guide:{},get_sm_overview:{},search_sms:{search:null,active:null,limit:50},get_sm_context:{smUserId:f.employee},
 search_sm_markets:{search:"Synthetic",chain:null,region:null,smUserId:null,scope:"SMDurcharbeit",limit:50},get_sm_market_context:{marketId:f.market},
 get_sm_planning:{...range,status:null,limit:50},get_sm_campaigns:{from:range.from,to:range.to,search:null,status:null,limit:50},
 get_sm_campaign_context:{campaignId:c.id,month:"2026-10-01"},get_sm_targets:{month:"2026-10-01",campaignId:c.id,smUserId:f.employee,limit:50},
 search_sm_visits:{...range,search:null,status:"submitted",limit:50},get_sm_visit_context:{submissionId:started.submissionId},
 get_sm_dashboard_metrics:{...range,region:null,chain:null},get_sm_time_context:{...range,limit:50},get_sm_time_analytics:{...range,groupBy:"sm",limit:50},get_sm_visit_analytics:{...range,groupBy:"chain",limit:50},get_sm_campaign_report:{campaignId:c.id,month:"2026-10-01",smUserId:null,limit:50},
 get_sm_questionnaires:{search:null,scope:"SMDurcharbeit",limit:50},get_sm_questionnaire_context:{versionId:version!.id},
 get_sm_answer_analytics:{...range,questionId:null,limit:50},get_sm_pending_requests:{smUserId:null,status:"pending",limit:50},
 get_sm_campaign_history:{campaignId:c.id,limit:50},get_sm_message_context:{smUserId:null,search:null,limit:50},
 get_sm_access_context:{search:null,limit:50},get_sm_kurti_chat_history:{smUserId:f.employee,reason:"Konkreten Navigationsfehler prüfen",limit:40},
 search_sm_photos:{...range,limit:10}};
 const results:Record<string,any>={};
 for(const [name,input] of Object.entries(args))results[name]=await call(name,input);
 assert.ok(results.get_sm_app_guide.pages.some((p:any)=>p.href==="/admin/sm/durcharbeit"));
 assert.ok(results.get_sm_app_guide.pages.some((p:any)=>p.href==="/admin/shelfmerchandiser"));
 assert.equal(results.search_sms.rows.length,2);assert.equal(results.search_sm_markets.rows[0].durcharbeitSmUserId,f.employee);
 assert.equal(results.get_sm_planning.rows[0].smUserId,f.other);
 assert.equal(results.get_sm_targets.completed,1);assert.equal(results.search_sm_visits.rows.length,1);
 assert.equal(results.get_sm_dashboard_metrics.overview.activity.visits,1);
 assert.equal(results.get_sm_dashboard_metrics.overview.activity.marketHours,.5);
 assert.equal(results.get_sm_time_context.rows[0].actualMinutes,30);
 assert.equal(results.get_sm_time_analytics.rows[0].marketMinutes,30);

 assert.equal(results.get_sm_visit_context.sections[0].questions[0].answer.value,"Synthetic saved answer");
 assert.equal(results.get_sm_questionnaire_context.rows.length,3);
 assert.equal(results.search_sm_photos.rows[0].photos.length,1);
 assert.doesNotMatch(JSON.stringify(results.search_sm_photos),/storagePath|storageBucket|signedUrl/);
 assert.equal(results.get_sm_answer_analytics.rows[0].observations,1);
 assert.equal(results.get_sm_kurti_chat_history.rows.length,2);
 assert.equal((await call("get_sm_targets",{month:"2026-10-01",campaignId:paused.id,smUserId:null,limit:50})).rows[0].campaignStatus,"paused");
 for(const invalid of [{...range,to:"2026-02-30",region:null,chain:null},{...range,to:"2030-01-01",region:null,chain:null},{...range,region:null,chain:null,sql:"select 1"}]){
 const result=JSON.parse(await f.tools.executeSmKurtiTool("get_sm_dashboard_metrics",JSON.stringify(invalid),f.actor));assert.equal(result.error,"invalid_arguments");
 }
 assert.equal(JSON.parse(await f.tools.executeSmKurtiTool("get_gm_context","{}",f.actor)).error,"unknown_sm_tool");
 assert.equal(JSON.parse(await f.tools.executeSmKurtiTool("get_sm_overview","{}",{appUserId:f.employee,role:"sm"})).error,"forbidden");
 });
 await t.test("actual multi-round model loop, unavailable tool rejection, rendering and fresh SM Excel",async()=>{
 let step=0;
 f.setAnswer(body=>{
 assert.doesNotMatch(JSON.stringify(body.tools),/"name":"(?:get_gm_context|search_gms|prepare_admin_excel_export)"/);
 if(step++===0)return toolCall("get_sm_dashboard_metrics",{...range,region:null,chain:null});
 const outputs=body.input.filter((i:any)=>i.type==="function_call_output");
 assert.equal(JSON.parse(outputs[0].output).data.overview.activity.visits,1);
 return {output:[],output_text:"Ein Marktbesuch mit 0,5 Leistungsstunden."};
 });
 const reply=(await admin("post","/admin/sm-kurti/messages").send({message:"Zeig mir die Leistung im Oktober."}).expect(200)).body;
 assert.match(reply.assistantMessage.content,/0,5/);
 assert.match(f.calls.at(-1)!.instructions,/SM-Adminbereich/);
 step=0;f.setAnswer(body=>step++===0?toolCall("get_gm_context",{gmUserId:f.employee}):{output:[],output_text:"GM ist hier nicht verfügbar."});
 await admin("post","/admin/sm-kurti/messages").send({message:"Lies GM Daten"}).expect(200);
 assert.equal(JSON.parse(f.calls.at(-1)!.input.findLast((i:any)=>i.type==="function_call_output").output).error,"tool_not_available");
 step=0;f.setAnswer(()=>step++===0?toolCall("render_metrics_visualization",{title:"SM Übersicht",subtitle:null,sourceLabel:"SM Dashboard",timeframe:"Oktober 2026",note:null,columns:"2",items:[{label:"Marktbesuche",value:1,displayValue:null,valueFormat:"number",delta:null,deltaLabel:null,progress:null,status:"neutral"}]}):{output:[],output_text:"Ein abgeschlossener Marktbesuch."});
 const visual=(await admin("post","/admin/sm-kurti/messages").send({message:"Zeig mir eine Kennzahl"}).expect(200)).body;
 assert.equal(visual.assistantMessage.visualizations[0].kind,"metrics");
 const filters={...range,search:null,region:null,chain:null,campaignId:null,month:null};
 step=0;f.setAnswer(()=>step++===0?toolCall("prepare_sm_excel_export",{dataset:"times",title:"SM Zeiten Oktober",filters}):{output:[],output_text:"Hier sind die SM-Zeiten."});
 const exported=(await admin("post","/admin/sm-kurti/messages").send({message:"Zeiten als Excel"}).expect(200)).body;
 const spec=exported.assistantMessage.smExports[0];
 assert.equal(spec.dataset,"times");assert.doesNotMatch(exported.assistantMessage.content,/sm-kurti-exports/);
 const excel=(await admin("post","/admin/sm-kurti/export").send(spec).expect(200)).body;
 assert.equal(excel.rows[0].actualMinutes,30);
 await sm("post","/admin/sm-kurti/export").send(spec).expect(403);
 await admin("post","/admin/sm-kurti/export").send({...spec,filters:{...filters,chain:"SPAR"}}).expect(400);
 });
 await t.test("workspace-separated memory, expiry isolation, support read does not extend worker TTL, no business mutations",async()=>{
 const smHistory=(await admin("get","/admin/sm-kurti/messages").expect(200)).body;
 assert.equal(smHistory.capabilities.crossSm,true);assert.equal(smHistory.capabilities.crossGm,false);
 assert.equal((await admin("get","/admin/kurti/messages").expect(200)).body.messages.length,0);
 await f.memory.saveAdminKurtiExchange({adminUserId:f.admin,userContent:"GM only",assistantContent:"GM history",workspace:"gm",now:new Date("2026-10-20T10:00:00Z")});
 const smRows=await f.database.select().from(f.schema.adminKurtiMessages);
 const smExpiry=smRows.find(r=>r.content.includes("Zeiten als Excel"))!.expiresAt.toISOString();
 await f.memory.saveAdminKurtiExchange({adminUserId:f.admin,userContent:"GM again",assistantContent:"GM next",workspace:"gm",now:new Date("2026-10-20T11:00:00Z")});
 assert.equal((await f.database.select().from(f.schema.adminKurtiMessages)).find(r=>r.content.includes("Zeiten als Excel"))!.expiresAt.toISOString(),smExpiry);
 const own=await f.database.select().from(f.schema.gmKurtiMessages),expiry=own[0]!.expiresAt.toISOString();
 await call("get_sm_kurti_chat_history",{smUserId:f.employee,reason:"Konkrete Fehlermeldung untersuchen",limit:40});
 assert.equal((await f.database.select().from(f.schema.gmKurtiMessages))[0]!.expiresAt.toISOString(),expiry);
 assert.equal(await graph(),before);
 await f.database.update(f.schema.smQuestionnaireSubmissions).set({isDeleted:true,deletedAt:new Date()}).where(eq(f.schema.smQuestionnaireSubmissions.id,started.submissionId));
 const retained=await call("get_sm_time_context",{...range,limit:50});assert.equal(retained.rows[0].actualMinutes,30);assert.equal(retained.rows[0].questionnaireComplete,false);
 const hidden=await call("get_sm_visit_context",{submissionId:started.submissionId});assert.equal(hidden.visit,null);
 });
 await t.test("factual person-level visit comparison keeps canonical counts, scoped period and redacted labels",async()=>{
 const [original]=await f.database.select().from(f.schema.smQuestionnaireSubmissions).where(eq(f.schema.smQuestionnaireSubmissions.id,started.submissionId));
 for(const [userId,count,name] of [[f.employee,1,"Annika Musterfrau"],[f.other,2,"Carolin Beispielsm"]] as const){
  for(let index=0;index<count;index++){
   const assignment=await f.assignment();
   await f.database.insert(f.schema.smQuestionnaireSubmissions).values({...original!,id:randomUUID(),assignmentId:assignment.id,SMDurcharbeitVisitId:null,SMDurcharbeitTargetId:null,clientSubmissionToken:randomUUID(),smUserId:userId,smNameSnapshot:name,isDeleted:false,deletedAt:null});
  }
 }
 const scoped={...range,groupBy:"sm",limit:50};
 const data=await call("get_sm_visit_analytics",scoped);
 assert.deepEqual(data.rows.map((row:any)=>row.visits).sort((a:number,b:number)=>b-a),[2,1]);
 assert.ok(data.rows.every((row:any)=>row.visitedMarkets===1));
 f.setAnswer(body=>{
  const outputs=body.input.filter((i:any)=>i.type==="function_call_output");
  if(!outputs.length)return toolCall("get_sm_visit_analytics",scoped);
  const rows=JSON.parse(outputs[0].output).data.rows.sort((a:any,b:any)=>b.visits-a.visits);
  assert.deepEqual(rows.map((row:any)=>row.visits),[2,1]);
  assert.doesNotMatch(JSON.stringify(rows),/Annika|Musterfrau|Carolin|Beispielsm/);
  return {output:[],output_text:`Erfasste Marktbesuche – absteigend sortiert, Oktober 2026, alle SMs/Marktfilter; Quelle: Spark get_sm_visit_analytics. ${rows.map((row:any)=>`[user:${row.key}]: ${row.visits}`).join("; ")}. Erfasste Zahlen allein belegen keine Arbeitsqualität.`};
 });
 const result=(await admin("post","/admin/sm-kurti/messages").send({message:"Wer hat im Oktober die wenigsten Besuche? Zeig die Marktbesuche absteigend."}).expect(200)).body;
 assert.match(result.assistantMessage.content,/absteigend sortiert/);
 assert.match(result.assistantMessage.content,/Quelle: Spark/);
 assert.match(result.assistantMessage.content,new RegExp(`\\[user:${f.other}\\]: 2`));
 assert.equal(JSON.parse(await f.tools.executeSmKurtiTool("get_sm_visit_analytics",JSON.stringify({...scoped,from:"2026-11-01",to:"2026-11-30"}),f.actor)).data.rows.length,0);
 });
 }finally{await f.pg.close();}
});

