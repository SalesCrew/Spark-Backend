// In-memory synthetic database. No environment files, network clients, app bootstrap or jobs.
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import express from "express";
import { availabilityHttpFixture } from "./gm-availability-fixture.js";
import * as s from "../src/lib/schema.js";
import { modelDatabase } from "../src/lib/praemien-workspace.js";
import { createGmDashboardRouter } from "../src/routes/gm-dashboard.js";
import { coolerBrands, placementBrands } from "../src/gm-report.shared.js";

export async function reportFixture(origin = "http://127.0.0.1:4070") {
  const syntheticPhoto=await readFile(new URL("./fixtures/gm-report-photo.png",import.meta.url));
  const f=await availabilityHttpFixture();
  await f.pg.exec("set default_transaction_read_only=off");
  const gm2=randomUUID(),extra=randomUUID();
  await f.database.insert(s.users).values([
    {id:gm2,firstName:"Robin",lastName:"Beispiel",role:"gm",isActive:true,isSupplementalGm:false,email:"gm2@preview.invalid",region:"West"},
    {id:extra,firstName:"Zusatz",lastName:"Beispiel",role:"gm",isActive:true,isSupplementalGm:true,email:"extra@preview.invalid"},
  ]);
  const markets=[{id:f.ids.market,chain:"Billa",region:"Nord"},{id:f.ids.plus,chain:"Billa+",region:"Nord"},{id:f.ids.corso,chain:"Billa Corso",region:"Nord"},{id:f.ids.spar,chain:"SPAR",region:"Nord"}];
  for(const [i,chain] of ["EUROSPAR","INTERSPAR","Hofer","Lidl"].entries()){
    const id=randomUUID(),region=["Ost","Süd","West","West"][i]!;
    await f.database.insert(s.markets).values({id,dbName:chain,name:`Beispielmarkt ${chain}`,region,visitFrequencyPerYear:12,address:`Musterstraße ${i+1}`,city:"Musterstadt",postalCode:"1010"});
    markets.push({id,chain,region});
  }
  const availabilityIds=["Cooler","SingleServe","MultiServe","Promos","Warehouse"].map(()=>randomUUID());
  const quantityQuestions=[...coolerBrands.map(brand=>({id:randomUUID(),brand,kind:"cooler" as const})),...placementBrands.map(brand=>({id:randomUUID(),brand,kind:"placement" as const}))];
  for(const q of quantityQuestions)await f.database.insert(s.questionScoring).values({questionId:q.id,scoreKey:"yes",mitbewerberabfrage:q.brand==="Coke"?null:"999",zweitplatzierung:q.brand==="Coke"?"777":null});
  const seeds:{session:string;market:string;date:string;cooler:number;photo:string|null}[]=[];
  const seed=async(input:{market:number;date:string;index:number;supplemental?:boolean;status?:"draft"|"submitted";deleted?:boolean;ambiguous?:boolean;invalid?:boolean;repeat?:boolean})=>{
    const m=markets[input.market]!,session=randomUUID(),section=randomUUID(),at=new Date(input.date+"T10:00:00+01:00");
    await f.database.insert(s.visitSessions).values({id:session,gmUserId:input.supplemental?extra:input.market%2?gm2:f.ids.gm,marketId:m.id,status:input.status??"submitted",isDeleted:!!input.deleted,isSupplementalGmSnapshot:!!input.supplemental,startedAt:at,submittedAt:input.status==="draft"?null:new Date(at.getTime()+3600000)});
    await f.database.insert(s.visitSessionSections).values({id:section,visitSessionId:session,campaignId:f.ids.campaign,section:"standard",status:input.status??"submitted"});
    let photo:string|null=null;
    for(const [i,type] of ["Cooler","SingleServe","MultiServe","Promos","Warehouse"].entries()){
      const q=randomUUID(),a=randomUUID(),category=(input.index+i)%8===0?"Bad":(input.index+i)%4===0?"Mediocre":"Top";
      await f.database.insert(s.visitSessionQuestions).values({id:q,visitSessionSectionId:section,questionId:availabilityIds[i]!,questionType:"single",questionTextSnapshot:`Verfügbarkeit ${type}`,moduleNameSnapshot:"Verfügbarkeit",singleChoiceAvailabilitySnapshot:true,singleChoiceAvailabilityTypeSnapshot:type,appliesToMarketChainSnapshot:true,orderIndex:i});
      await f.database.insert(s.visitAnswers).values({id:a,visitSessionId:session,visitSessionSectionId:section,visitSessionQuestionId:q,questionId:availabilityIds[i]!,questionType:"single",valueText:category,answerStatus:"answered",isValid:!input.invalid,changedAt:at});
      if(i===0){photo=randomUUID();await f.database.insert(s.visitAnswerPhotos).values({id:photo,visitAnswerId:a,storageBucket:"synthetic-photos",storagePath:`synthetic/${photo}.png`,mimeType:"image/png",uploadedAt:at});}
    }
    for(const [i,def] of quantityQuestions.entries()){
      const q=randomUUID(),a=randomUUID(),n=(input.index+i)%7;
      await f.database.insert(s.visitSessionQuestions).values({id:q,visitSessionSectionId:section,questionId:def.id,questionType:"numeric",questionTextSnapshot:`Wie viele ${def.brand} ${def.kind==="cooler"?"Kühler":"Großplatzierungen"} sind vorhanden?`,moduleNameSnapshot:"Abfrage Mitbewerb",appliesToMarketChainSnapshot:true,orderIndex:i+5});
      await f.database.insert(s.visitAnswers).values({id:a,visitSessionId:session,visitSessionSectionId:section,visitSessionQuestionId:q,questionId:def.id,questionType:"numeric",valueNumber:input.ambiguous&&i===0?null:String(n),valueText:input.ambiguous&&i===0?"Ja":null,answerStatus:"answered",isValid:!input.invalid||i>0,changedAt:at});
    }
    seeds.push({session,market:m.id,date:input.date,cooler:input.index%7,photo});
    return {session,section};
  };
  for(const [month,dates] of [[4,[6,13,20]],[8,[3,10,17]],[9,[7,14,21]],[10,[5,12,19,26]]] as const){
    for(const [wi,day] of dates.entries())for(let mi=0;mi<markets.length;mi++)await seed({market:mi,date:`2026-${String(month).padStart(2,"0")}-${String(day).padStart(2,"0")}`,index:wi*8+mi+month});
  }
  await seed({market:0,date:"2026-11-02",index:5});await seed({market:3,date:"2026-11-05",index:6,ambiguous:true});
  const special=await seed({market:0,date:"2026-10-20",index:999,supplemental:true});
  await seed({market:0,date:"2026-10-21",index:998,status:"draft"});
  await seed({market:0,date:"2026-10-22",index:997,deleted:true});
  await seed({market:0,date:"2026-10-23",index:996,invalid:true});
  // Work done as Zusatz remains excluded even if the employee is now regular.
  await f.database.insert(s.visitSessions).values({id:randomUUID(),gmUserId:f.ids.gm,marketId:f.ids.market,status:"submitted",isSupplementalGmSnapshot:true,startedAt:new Date("2026-10-20T12:00Z"),submittedAt:new Date("2026-10-20T13:00Z")});
  const reference=seeds.find(v=>v.market===f.ids.market&&v.date==="2026-10-19")!;
  const refSection=(await f.pg.query<{id:string}>("select id from visit_session_sections where visit_session_id=$1",[reference.session])).rows[0]!.id;
  const triggerId=randomUUID(),triggerSnapshot=randomUUID(),hiddenId=randomUUID(),hiddenSnapshot=randomUUID(),hiddenAnswer=randomUUID(),hiddenPhoto=randomUUID();
  await f.database.insert(s.visitSessionQuestions).values([
    {id:triggerSnapshot,visitSessionSectionId:refSection,questionId:triggerId,questionType:"single",questionTextSnapshot:"Frage einblenden?",moduleNameSnapshot:"Steuerung",appliesToMarketChainSnapshot:true},
    {id:hiddenSnapshot,visitSessionSectionId:refSection,questionId:hiddenId,questionType:"numeric",questionTextSnapshot:"Wie viele Coke Kühler?",moduleNameSnapshot:"Abfrage Mitbewerb",appliesToMarketChainSnapshot:true,questionRulesSnapshot:[{triggerQuestionId:triggerId,operator:"equals",triggerValue:"Nein",action:"hide",targetQuestionIds:[hiddenId]}]},
  ]);
  await f.database.insert(s.visitAnswers).values([
    {visitSessionId:reference.session,visitSessionSectionId:refSection,visitSessionQuestionId:triggerSnapshot,questionId:triggerId,questionType:"single",valueText:"Nein",answerStatus:"answered",isValid:true},
    {id:hiddenAnswer,visitSessionId:reference.session,visitSessionSectionId:refSection,visitSessionQuestionId:hiddenSnapshot,questionId:hiddenId,questionType:"numeric",valueNumber:"9000",answerStatus:"answered",isValid:true},
  ]);
  await f.database.insert(s.visitAnswerPhotos).values({id:hiddenPhoto,visitAnswerId:hiddenAnswer,storageBucket:"synthetic-photos",storagePath:`synthetic/${hiddenPhoto}.png`});
  const app=express();app.use(express.json());app.use((_req,res,next)=>{res.set("Access-Control-Allow-Origin","*");res.set("Access-Control-Allow-Headers","authorization,content-type,x-kunde-page-key");res.set("Access-Control-Allow-Methods","GET,POST,OPTIONS");next();});app.options(/.*/,(_req,res)=>res.sendStatus(204));
  let fail=false;const signCalls:string[]=[];
  app.use("/admin/gm-dashboard",(req,res,next)=>{
    if(!req.headers.authorization){res.sendStatus(401);return;}
    if(!["Bearer synthetic-admin","Bearer synthetic-kunde","Bearer synthetic-kunde-no-photos"].includes(req.headers.authorization)){res.sendStatus(403);return;}
    if(fail&&req.path==="/reports/query"){res.status(503).json({error:"Synthetischer Verbindungsfehler"});return;}next();
  },createGmDashboardRouter(modelDatabase(f.database),undefined,{
    canRead:async req=>req.headers.authorization!=="Bearer synthetic-kunde-no-photos",
    sign:async p=>{signCalls.push(p.id);return `${origin}/__synthetic/photos/${p.id}.png`;},
  }));
  app.get("/__synthetic/photos/:id.png",(_req,res)=>res.type("png").send(syntheticPhoto));
  await f.pg.exec("set default_transaction_read_only=on");
  return {...f,app,markets,availabilityIds,quantityQuestions,seeds,gm2,extra,special,signCalls,setFailure:(v:boolean)=>{fail=v;},close:()=>f.pg.close()};
}
export const emptyReportScope={region:null,gmId:null,marketId:null,chain:null,stc:null};
