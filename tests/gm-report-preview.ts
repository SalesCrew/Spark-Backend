// Dedicated synthetic preview. Never load .env or application entrypoints.
import {reportFixture} from "./gm-report-fixture.js";
import {randomUUID} from "node:crypto";
const origin=process.env.REPORT_API_ORIGIN??"http://127.0.0.1:4070";
if(new URL(origin).hostname!=="127.0.0.1"||[3000,4000].includes(Number(new URL(origin).port)))throw new Error("Use an independent local preview port");
const f=await reportFixture(origin),today=new Intl.DateTimeFormat("sv-SE",{timeZone:"Europe/Vienna"}).format(new Date());
const user=(role:string)=>({id:role==="admin"?f.ids.admin:f.gm2,role:role.startsWith("kunde")?"kunde":role,email:`${role}@preview.invalid`,firstName:"Vorschau",lastName:"Synthetisch",permissions:role.startsWith("kunde")?{gm_dashboard:["read"],...(role==="kunde"?{fotoarchiv:["read"]}:{})}:null});
f.app.post("/auth/login",(req,res)=>{const role=["kunde","kunde-no-photos"].find(r=>req.body.email?.startsWith(r+"@"))??"admin";res.json({user:user(role),session:{accessToken:`synthetic-${role}`,refreshToken:`synthetic-refresh-${role}`,expiresAt:Math.floor(Date.now()/1000)+86400}});});
f.app.get("/auth/me",(req,res)=>res.json({user:user(req.headers.authorization?.replace("Bearer synthetic-","")??"admin")}));
f.app.get("/employee-agreement/current",(_req,res)=>res.json({accepted:true,agreement:{key:"synthetic",version:"1",title:"Synthetic",hash:"synthetic",effectiveDate:"2026-01-01",sections:[]},acceptance:{acceptedAt:new Date().toISOString(),version:"1",hash:"synthetic"}}));
const periods=[];
for(const year of [2024,2025,2026,2027]){
 let start=new Date(`${year}-01-01T12:00:00Z`);start.setUTCDate(start.getUTCDate()+(8-start.getUTCDay())%7);
 for(let index=1;index<=12;index++){
  const end=new Date(start);end.setUTCDate(start.getUTCDate()+[4,4,5][(index-1)%3]!*7-3);
  periods.push({id:randomUUID(),label:`RED ${String(index).padStart(2,"0")}`,periodIndex:index,periodIndexFromAnchor:(year-2026)*12+index-1,start:start.toISOString().slice(0,10),end:end.toISOString().slice(0,10),year,status:year<2026?"locked":"active",isCurrent:start.toISOString().slice(0,10)<=today&&end.toISOString().slice(0,10)>=today,daysUntilEnd:0});
  start.setUTCDate(start.getUTCDate()+[4,4,5][(index-1)%3]!*7);
 }
}
f.app.get("/red-month/calendar",(_req,res)=>res.json({periods}));f.app.get("/red-month/current",(_req,res)=>res.json({current:periods.find(p=>p.isCurrent)??periods[34],config:{anchorStart:"2026-01-05",cycleWeeks:[4,4,5],periodCount:12,timezone:"Europe/Vienna",status:"active",updatedAt:null}}));
f.app.get("/__synthetic/identity",(_req,res)=>res.json({synthetic:true,feature:"gm-dashboard-reports",today}));
f.app.post("/__synthetic/scenario",(req,res)=>{f.setFailure(!!req.body.failRead);res.json({ok:true});});
f.app.get(["/admin/modules","/admin/fragebogen"],(_req,res)=>res.json({modules:[],fragebogen:[]}));
f.app.get(["/admin/campaigns/answer-change-requests","/admin/time-change-requests","/admin/campaigns/visit-session-delete-requests"],(_req,res)=>res.json({requests:[]}));
f.app.get("/admin/praemien/workspace/status",(_req,res)=>res.json({ready:true}));f.app.get("/admin/praemien/workspace/waves",(_req,res)=>res.json({waves:[]}));
f.app.get(/^\/admin\/(modules|fragebogen)(\/.*)?$/,(_req,res)=>res.json({modules:[],fragebogen:[],usage:[]}));
f.app.put("/admin/kurti/layout",(_req,res)=>res.json({ok:true}));
f.app.get("/admin/campaigns",(_req,res)=>res.json({campaigns:[]}));f.app.get(["/markets/chains","/admin/markets/chains"],(_req,res)=>res.json({chains:f.markets.map(m=>m.chain)}));
f.app.get("/admin/kurti/layout",(_req,res)=>res.json({layout:null}));f.app.post(["/telemetry/events","/client-telemetry"],(_req,res)=>res.sendStatus(204));
f.app.use((req,res)=>{console.info(`Synthetic endpoint missing: ${req.method} ${req.path}`);res.status(404).json({error:"Not provided in synthetic preview"});});
f.app.use((error:Error,_req:unknown,res:import("express").Response,_next:unknown)=>{console.error(error.message);res.status(500).json({error:"Synthetische Berichtsdaten konnten nicht geladen werden."});});
f.app.listen(Number(new URL(origin).port),"127.0.0.1",()=>console.info(`Synthetic report API ${origin} · admin@preview.invalid · any password`));
