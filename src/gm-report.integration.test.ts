import {after,before,test} from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import {reportFixture,emptyReportScope} from "../tests/gm-report-fixture.js";
import {physicalQuantities,suggestReportMapping,weekPeriods,type DashboardReport,type ReportRequest} from "./gm-report.shared.js";
let f:Awaited<ReturnType<typeof reportFixture>>;
let beforeState:string;
let beforePhotos:string;
before(async()=>{f=await reportFixture();beforePhotos=JSON.stringify(await f.pg.query("select * from visit_answer_photos order by id"));beforeState=JSON.stringify(await f.pg.query("select id,value_text,value_number,is_deleted from visit_answers order by id"));});
after(async()=>{if(!f)return;assert.equal(JSON.stringify(await f.pg.query("select * from visit_answer_photos order by id")),beforePhotos);assert.equal(JSON.stringify(await f.pg.query("select id,value_text,value_number,is_deleted from visit_answers order by id")),beforeState);await f.close();});
const body=(patch:Partial<ReportRequest>={}):ReportRequest=>({kind:"availability",periods:[{id:"oct",label:"RED Oktober 2026",start:"2026-10-05",end:"2026-10-30"}],cutoff:"2026-10-20",scope:emptyReportScope,...patch});
async function query(input=body(),token="synthetic-admin"){return request(f.app).post("/admin/gm-dashboard/reports/query").set("Authorization",`Bearer ${token}`).send(input);}
test("actual route reads isolated DB, applies cutoff and preserves distinct markets",async()=>{
  const r=await query();assert.equal(r.status,200,JSON.stringify(r.body));const d=r.body as DashboardReport;
  assert.equal(d.visitCount,24);assert.equal(d.uniqueMarketCount,8);assert.deepEqual(d.groups,{rewe:3,spar:3,other:2});
  assert.equal(d.availability?.total.counts.Cooler.total,24);assert.equal(d.photos.length,8);assert.equal(d.periods[0]?.end,"2026-10-20");
  assert.match(r.headers["cache-control"],/no-store/);assert.equal(d.availability?.regions.length,4);assert.equal(d.availability?.chains.length,8);
});
test("physical quantities use latest market state, not visit sums or configured 777/999 points",async()=>{
  const r=await query(body({kind:"competitor"}));assert.equal(r.status,200,JSON.stringify(r.body));const c=r.body.competitor;
  assert.equal(c.cooler.find((x:{brand:string})=>x.brand==="Coke").values[0],26);
  assert.ok(c.cooler.every((r:{values:number[]})=>r.values.every(n=>n<100)));assert.equal(c.issues.length,0);
});
test("each comparison period gets its own latest state and unknown never becomes zero",async()=>{
  const r=await query(body({kind:"competitor",periods:[{id:"sep",label:"RED September",start:"2026-08-31",end:"2026-10-02"},{id:"oct",label:"RED Oktober",start:"2026-10-05",end:"2026-10-30"}],cutoff:"2026-10-24"}));assert.equal(r.status,200,JSON.stringify(r.body));
  const c=r.body.competitor;assert.equal(c.cooler.find((v:{brand:string})=>v.brand==="Coke").values[1],null);assert.ok(c.issues.some((v:{text:string})=>v.text==="Antwort ungültig"));
  assert.ok(c.cooler.find((v:{brand:string})=>v.brand==="Monster").values.every((v:unknown)=>typeof v==="number"));
});
test("all card scope dimensions apply; supplemental snapshots and draft/deleted visits excluded",async()=>{
  const scoped=body({scope:{...emptyReportScope,chains:["Billa","SPAR"],marketIds:[f.ids.market,f.ids.spar]}});
  const r=await query(scoped);assert.equal(r.status,200);assert.equal(r.body.visitCount,6);assert.equal(r.body.uniqueMarketCount,2);
  const single=await query(body({scope:{...emptyReportScope,gmId:f.gm2,region:"Nord",chains:["SPAR"],stc:"silver"}}));assert.equal(single.status,200);assert.equal(single.body.visitCount,3);
  const extra=await query(body({scope:{...emptyReportScope,gmId:f.extra}}));assert.equal(extra.body.visitCount,0);
});
test("photo reader only gets filtered valid included answers and never customer without photo access",async()=>{
  f.signCalls.length=0;
  const customer=await query(body(),"synthetic-kunde-no-photos");assert.equal(customer.status,200);assert.equal(customer.body.photosAllowed,false);assert.deepEqual(customer.body.photos,[]);assert.equal(f.signCalls.length,0);
  const r=await query(body({scope:{...emptyReportScope,marketId:f.ids.spar}}));assert.equal(r.status,200);assert.ok(r.body.photos.every((p:{marketName:string})=>p.marketName==="Synthetic SPAR"));assert.equal(f.signCalls.length,3);
  const snapshot=await f.pg.query("select count(*)::int as n from visit_answer_photos");assert.ok((snapshot.rows[0] as {n:number}).n>50);
});
test("auth, invalid periods, future dates and unrelated mapping IDs fail cleanly",async()=>{
  assert.equal((await request(f.app).post("/admin/gm-dashboard/reports/query").send(body())).status,401);
  assert.equal((await query(body(),"synthetic-gm")).status,403);
  assert.equal((await query(body({cutoff:"2099-01-01"}))).status,400);
  assert.equal((await query(body({periods:[{id:"x",label:"bad",start:"2026-02-30",end:"2026-03-01"}]}))).status,400);
  assert.equal((await query(body({kind:"competitor",periods:[...body().periods,...body().periods]}))).status,400);
  assert.equal((await query(body({kind:"competitor",mappings:[{questionId:f.ids.admin,kind:"cooler",brand:"Coke"}]}))).status,400);
});
test("per-export mapping never changes stored questions or answers",async()=>{
  const r=await query(body({kind:"competitor",mappings:[{questionId:f.quantityQuestions[0]!.id,kind:"cooler",brand:"Coke",excluded:true}]}));assert.equal(r.status,200,JSON.stringify(r.body));assert.equal(r.body.competitor.excludedQuestions,1);assert.equal(r.body.competitor.cooler[0].values[0],null);
  await assert.rejects(f.pg.exec("update visit_answers set value_text='forbidden'"),/read.only/i);
});
test("quantity parsing: explicit numbers, absences, strict brand labels, duplicates and conflicts",()=>{
 const q={valueText:"Ja",valueNumber:null,raw:null,options:[],comment:"Red Bull: 2; Monster: 1"};const map={questionId:"q",kind:"cooler" as const,brand:null};
 assert.deepEqual([...physicalQuantities(q,map).values],[["RedBull",2],["Monster",1]]);
 assert.equal(physicalQuantities({...q,comment:""},map).issue!==null,true);
 assert.equal(physicalQuantities({...q,valueText:"Nein",comment:""},map).values.get("Coke"),0);
 assert.ok(physicalQuantities({...q,valueText:"Nein"},map).issue);
 assert.ok(physicalQuantities({...q,valueNumber:"7",comment:"Coke:8"},{...map,brand:"Coke"}).issue);
 assert.ok(physicalQuantities({...q,comment:"Red Bull:2; Unknown:4"},map).issue);
 assert.ok(physicalQuantities({...q,comment:"Red Bull:2; Red Bull:4"},map).issue);
 assert.equal(physicalQuantities({...q,comment:"Red Bull:2; Red Bull:2"},map).values.get("RedBull"),2);
 assert.equal(physicalQuantities({...q,valueNumber:"7",comment:""},{...map,brand:"Coke"}).values.get("Coke"),7);
 assert.equal(suggestReportMapping({questionId:"q",text:"Wie viele Jack&Coke Kühler?",isCokePlacement:true}).brand,"Jack&Coke");
 assert.equal(weekPeriods("2026-10-05","2026-10-30").map(p=>p.label).join(","),"KW41,KW42,KW43,KW44");
});
