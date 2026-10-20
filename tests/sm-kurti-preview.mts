import express from "express";
import { EMPLOYEE_AGREEMENT_KEY,EMPLOYEE_AGREEMENT_VERSION } from "../src/lib/employee-agreement-meta.js";
import { eq } from "drizzle-orm";
import { createSmKurtiFixture,toolCall } from "./sm-kurti-fixture.js";
const f=await createSmKurtiFixture();
await f.accept();
await f.database.insert(f.schema.employeeAgreementAcceptances).values({userId:f.admin,agreementKey:EMPLOYEE_AGREEMENT_KEY,agreementVersion:EMPLOYEE_AGREEMENT_VERSION,agreementTitle:"Synthetic",agreementHash:"synthetic"});
await f.database.insert(f.schema.smSMDurcharbeitMarkets).values({smMarketId:f.market,SMDurcharbeitSmUserId:f.employee});
const a=await f.assignment();await f.database.update(f.schema.smAssignments).set({replacementWorkDate:new Date().toISOString().slice(0,10)}).where(eq(f.schema.smAssignments.id,a.id));
const emptyFilters={from:null,to:null,smUserId:null,marketId:null,scope:null,search:null,region:null,chain:null,campaignId:null,month:null};
f.setAnswer(body=>{
 const user=[...body.input].reverse().find((i:any)=>i.role==="user")?.content??"";
 const person=user.match(/\[user:[0-9a-f-]{36}\]/i)?.[0];
 if(person){
  if(!body.tools)return {output:[],output_text:`${person}: 12 Besuche.`};
  if(!body.input.some((i:any)=>i.type==="function_call_output"))return toolCall(body.tools.some((t:any)=>t.name==="get_sm_context")?"get_sm_context":"get_admin_overview",body.tools.some((t:any)=>t.name==="get_sm_context")?{smUserId:person.slice(6,-1)}:{});
  return {output:[],output_text:`${person}: 12 Besuche.`};
 }
 if(!body.tools && !body.instructions.includes("Die Person im Chat ist ein SM am Handy"))return {output:[],output_text:"Deine eigenen Daten findest du auf Home."};
 if(body.instructions.includes("Die Person im Chat ist ein SM am Handy")){
  return {output:[],output_text:"Deine Durcharbeit findest du als Komponente auf Home. Tippe darauf, um deine Monatsmärkte zu öffnen. Bereits gespeicherte Antworten und Fotos bleiben im gleichen Besuch erhalten."};
 }
 const outputs=body.input.filter((i:any)=>i.type==="function_call_output");
 if(/excel/i.test(user)){
  if(!outputs.length)return toolCall("prepare_sm_excel_export",{dataset:"markets",title:"SM Märkte",filters:emptyFilters});
  return {output:[],output_text:"Hier ist dein SM-Marktauszug mit dem aktuellen Datenstand beim Download."};
 }
 if(!outputs.length)return toolCall("get_sm_overview",{});
 if(outputs.length===1){
  const data=JSON.parse(outputs[0].output).data;
  return toolCall("render_metrics_visualization",{title:"SM im Überblick",subtitle:null,sourceLabel:"SM Marktstamm",timeframe:"Aktueller Stand",note:null,columns:"2",items:[
   {label:"Aktive SMs",value:data.counts.activeSms,displayValue:null,valueFormat:"number",delta:null,deltaLabel:null,progress:null,status:"neutral"},
   {label:"Aktive Märkte",value:data.counts.activeMarkets,displayValue:null,valueFormat:"number",delta:null,deltaLabel:null,progress:null,status:"neutral"}
  ]});
 }
 return {output:[],output_text:"Hier ist der aktuelle SM-Überblick aus dem Marktstamm."};
});
const app=express();app.use(express.json());
app.use((req,res,next)=>{
 res.setHeader("Access-Control-Allow-Origin","http://127.0.0.1:3038");res.setHeader("Access-Control-Allow-Headers","Authorization, Content-Type, x-coke-spark-page-key");
 res.setHeader("Access-Control-Allow-Methods","GET, POST, PUT, PATCH, DELETE, OPTIONS");if(req.method==="OPTIONS"){res.sendStatus(204);return;}next();
});
const userFor=(token:string)=>token==="synthetic-sm"?{id:f.employee,role:"sm",firstName:"Annika",lastName:"Musterfrau",email:"sm@preview.test",isActive:true}:
 token==="synthetic-gm"?{id:f.admin,role:"gm",firstName:"Benedikt",lastName:"Musteradmin",email:"gm@preview.test",isActive:true}:
 {id:f.admin,role:"sm_admin",firstName:"Benedikt",lastName:"Musteradmin",email:"admin@preview.test",isActive:true};
const session=(token:string)=>({user:userFor(token),session:{accessToken:token,refreshToken:token,expiresAt:Math.floor(Date.now()/1000)+86400}});
app.post("/auth/login",(req,res)=>req.body.password==="preview"?res.json(session(req.body.email==="sm@preview.test"?"synthetic-sm":req.body.email==="gm@preview.test"?"synthetic-gm":"synthetic-sm-admin")):res.sendStatus(401));
app.post("/auth/refresh",(req,res)=>res.json(session(req.body.refreshToken)));
app.get("/auth/me",(req,res)=>res.json({user:userFor(req.header("authorization")?.replace("Bearer ","")??"")}));
app.get("/employee-agreement/current",(_req,res)=>res.json({accepted:true,agreement:{key:"synthetic",version:"synthetic",title:"Synthetic",sections:[]},acceptance:{acceptedAt:"2026-10-01T00:00:00Z"}}));
app.post("/telemetry/events",(_req,res)=>res.json({ok:true}));
app.get("/__fixture/state",(_req,res)=>res.json({wire:f.calls,calls:f.calls.map(c=>({sm:c.instructions.includes("SM"),tools:c.tools?.map((t:any)=>t.name),outputs:c.input.filter((i:any)=>i.type==="function_call_output").map((i:any)=>JSON.parse(i.output))}))}));
app.use(f.app);
const server=app.listen(4038,"127.0.0.1",()=>console.log("Synthetic Kurti fixture ready on 4038"));
process.on("SIGTERM",()=>server.close(()=>{void f.pg.close().then(()=>process.exit(0));}));

