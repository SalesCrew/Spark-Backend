import * as operatingPolicy from "../src/lib/kurti-operating-policy.js";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import express from "express";
import { eq } from "drizzle-orm";
import * as schema from "../src/lib/schema.js";
import * as role from "../src/lib/admin-role.js";
import * as planning from "../src/sm-planning.shared.js";
import * as catalog from "../src/sm-SMDurcharbeit-catalog.shared.js";
import * as visitShared from "../src/sm-visit.shared.js";
import * as comments from "../src/sm-comment.shared.js";
import * as basics from "../src/sm-basic-activity.shared.js";
import * as conditional from "../src/lib/conditional-visibility.js";
import * as monthlyReuse from "../src/sm-SMDurcharbeit-answer-reuse.shared.js";
import * as overview from "../src/sm-dashboard-overview.shared.js";
import * as dashboard from "../src/sm-dashboard.shared.js";
import * as chart from "../src/lib/admin-kurti-charts.js";
import * as exports from "../src/lib/admin-kurti-exports.js";
import * as smExports from "../src/lib/sm-kurti-exports.js";
import * as visualizations from "../src/lib/admin-kurti-visualizations.js";
import * as visualizationTools from "../src/lib/admin-kurti-visualization-tools.js";
import * as skills from "../src/lib/admin-kurti-visualization-skill-loader.js";
import * as modelContext from "../src/lib/admin-kurti-model-context.js";
import * as smPrompt from "../src/lib/sm-kurti-system-prompt.js";
import * as smAdminPrompt from "../src/lib/sm-admin-kurti-system-prompt.js";
import * as gmPrompt from "../src/lib/kurti-system-prompt.js";
import * as adminPrompt from "../src/lib/admin-kurti-system-prompt.js";
import * as gmAgreement from "../src/lib/employee-agreement-meta.js";
import * as smAgreement from "../src/lib/sm-employee-agreement.js";
import { createSyntheticPhotoStorage } from "./synthetic-photo-storage.js";
import { createSMDurcharbeitFixture } from "./SMDurcharbeit-fixture.js";
import { isolatedModule } from "./isolated-module.js";
export type ModelBody=Record<string,any>;
export type ModelReply={output_text:string;output:any[]};
export async function createSmKurtiFixture(){
 const other=randomUUID();
 const storage=createSyntheticPhotoStorage();
 const f=await createSMDurcharbeitFixture({photoStorage:storage.storage,additionalSmUsers:[{id:other,token:"synthetic-other-sm"}]});
 try{
 await f.database.update(schema.users).set({firstName:"Annika",lastName:"Musterfrau"}).where(eq(schema.users.id,f.employee));
 await f.database.update(schema.users).set({firstName:"Benedikt",lastName:"Musteradmin"}).where(eq(schema.users.id,f.admin));
 await f.database.update(schema.users).set({firstName:"Carolin",lastName:"Beispielsm"}).where(eq(schema.users.id,other));
 for(const migration of ["0058_employee_agreement_acceptances.sql","0070_gm_kurti_messages.sql","0072_admin_kurti_messages.sql","0073_admin_kurti_window_preferences.sql","0074_admin_kurti_window_collapsed.sql"]){
  await f.pg.exec(await readFile(new URL("../drizzle/"+migration,import.meta.url),"utf8"));
 }
 const base={"./db.js":{db:f.dbForRoutes},"../lib/db.js":{db:f.dbForRoutes},"./schema.js":schema,"../lib/schema.js":schema};
 const monthly=await isolatedModule<typeof import("../src/sm-SMDurcharbeit-campaign.shared.js")>(new URL("../src/sm-SMDurcharbeit-campaign.shared.ts",import.meta.url),{
  "./lib/schema.js":schema,"./sm-planning.shared.js":planning,"./sm-SMDurcharbeit-catalog.shared.js":catalog,"./sm-SMDurcharbeit-privacy.shared.js":await import("../src/sm-SMDurcharbeit-privacy.shared.js")
 });
 const update=await isolatedModule<typeof import("../src/sm-SMDurcharbeit-questionnaire-update.shared.js")>(new URL("../src/sm-SMDurcharbeit-questionnaire-update.shared.ts",import.meta.url),{
  "./lib/schema.js":schema,"./sm-planning-lock.js":await import("../src/sm-planning-lock.js"),"./sm-SMDurcharbeit-campaign.shared.js":monthly
 });
 const management=await isolatedModule<typeof import("../src/sm-management.js")>(new URL("../src/sm-management.ts",import.meta.url),{
  "./lib/schema.js":schema,"./sm-basic-activity.shared.js":basics,"./sm-SMDurcharbeit-catalog.shared.js":catalog,
  "./sm-SMDurcharbeit-campaign.shared.js":monthly,"./sm-SMDurcharbeit-answer-reuse.shared.js":monthlyReuse,
  "./lib/conditional-visibility.js":conditional,"./sm-comment.shared.js":comments,"./sm-visit.shared.js":visitShared
 });
 const ctx=await isolatedModule<typeof import("../src/lib/sm-kurti-context.js")>(new URL("../src/lib/sm-kurti-context.ts",import.meta.url),{
  ...base,"../sm-SMDurcharbeit-campaign.shared.js":monthly
 });
 const identity=await isolatedModule<typeof import("../src/lib/kurti-identity.js")>(new URL("../src/lib/kurti-identity.ts",import.meta.url),{...base,"./admin-role.js":role});
 const toolErrors:unknown[]=[];
 const report=await isolatedModule(new URL("../src/sm-SMDurcharbeit-report.shared.ts",import.meta.url),{"./lib/schema.js":schema,"./sm-visit.shared.js":visitShared,"./sm-SMDurcharbeit-answer-reuse.shared.js":monthlyReuse,"./sm-SMDurcharbeit-campaign.shared.js":monthly});
 const tools=await isolatedModule<typeof import("../src/lib/sm-kurti-tools.js")>(new URL("../src/lib/sm-kurti-tools.ts",import.meta.url),{
  ...base,"./logger.js":{logger:{warn:(_event:string,detail:unknown)=>toolErrors.push(detail)},serializeError:(error:any)=>({message:error.message,code:error.code,cause:error.cause?.message})},"./admin-role.js":role,"./sm-kurti-context.js":ctx,"../sm-SMDurcharbeit-campaign.shared.js":monthly,
  "../sm-SMDurcharbeit-questionnaire-update.shared.js":update,"../routes/sm-dashboard.js":f.reporting,"../sm-SMDurcharbeit-report.shared.js":report,"../sm-dashboard.shared.js":dashboard,
  "../sm-planning.shared.js":planning,"../sm-management.js":management,"./admin-kurti-visualization-tools.js":visualizationTools,
  "./admin-kurti-tools.js":{ADMIN_KURTI_TOOLS:[],executeAdminKurtiTool:async()=>JSON.stringify({skills:[]})},"./sm-kurti-exports.js":smExports
 });
 const logger={logger:{warn(){},info(){},error(){}},serializeError:()=>({}),getRequestLogMeta:()=>({})};
 const memory=await isolatedModule<typeof import("../src/lib/admin-kurti-memory.js")>(new URL("../src/lib/admin-kurti-memory.ts",import.meta.url),{
  ...base,"./logger.js":logger,"./admin-kurti-charts.js":chart,"./admin-kurti-exports.js":exports,
  "./sm-kurti-exports.js":smExports,"./admin-kurti-visualizations.js":visualizations
 });
 const employeeMemory=await isolatedModule<typeof import("../src/lib/kurti-memory.js")>(new URL("../src/lib/kurti-memory.ts",import.meta.url),{...base,"./logger.js":logger});
 const calls:ModelBody[]=[];
 let answer:(body:ModelBody)=>ModelReply|Promise<ModelReply>=()=>({output:[],output_text:"Synthetic Kurti Antwort."});
 class FakeOpenAI{
  static APIError=class extends Error{};
  responses={create:async(body:ModelBody)=>{calls.push(structuredClone(body));return answer(body);}};
 }
 const env={OPENAI_API_KEY:"synthetic-only",OPENAI_KURTI_TIMEOUT_MS:5000,OPENAI_KURTI_MODEL:"synthetic-model",OPENAI_KURTI_MAX_OUTPUT_TOKENS:3000};
 const router=await isolatedModule<typeof import("../src/routes/admin-kurti.js")>(new URL("../src/routes/admin-kurti.ts",import.meta.url),{
  ...base,"../lib/kurti-identity.js":identity,"../lib/kurti-operating-policy.js":operatingPolicy,"openai":FakeOpenAI,"../config/env.js":{env},"../middleware/auth.js":f.auth,"../lib/logger.js":logger,
  "../lib/admin-kurti-charts.js":chart,"../lib/admin-kurti-exports.js":exports,"../lib/admin-kurti-visualizations.js":visualizations,
  "../lib/admin-kurti-visualization-tools.js":visualizationTools,"../lib/admin-kurti-visualization-skill-loader.js":skills,
  "../lib/admin-kurti-memory.js":memory,"../lib/admin-kurti-model-context.js":modelContext,"../lib/admin-kurti-system-prompt.js":adminPrompt,
  "../lib/admin-kurti-tool-routing.js":{selectAdminKurtiTools:()=>[{type:"function",name:"get_admin_overview",parameters:{},strict:false}],addAdminKurtiToolGroups:()=>[],parseAdminKurtiToolGroupArguments:()=>[]},
  "../lib/admin-kurti-tools.js":{ADMIN_KURTI_TOOLS:[],executeAdminKurtiTool:async()=>JSON.stringify({gm:true})},
  "../lib/sm-admin-kurti-system-prompt.js":smAdminPrompt,"../lib/sm-kurti-tools.js":tools,"../lib/sm-kurti-exports.js":smExports
 });
 const worker=await isolatedModule<typeof import("../src/routes/gm-kurti.js")>(new URL("../src/routes/gm-kurti.ts",import.meta.url),{
  ...base,"../lib/kurti-identity.js":identity,"../lib/kurti-operating-policy.js":operatingPolicy,"openai":FakeOpenAI,"../config/env.js":{env},"../middleware/auth.js":f.auth,"../lib/logger.js":logger,
  "../lib/employee-agreement-meta.js":gmAgreement,"../lib/sm-employee-agreement.js":smAgreement,
  "../lib/kurti-context.js":{buildKurtiGmContext:async()=>({onlyGm:true})},"../lib/sm-kurti-context.js":ctx,
  "../lib/kurti-system-prompt.js":gmPrompt,"../lib/sm-kurti-system-prompt.js":smPrompt,"../lib/kurti-memory.js":employeeMemory
 });
 const app=express();app.use(express.json());
 app.use("/admin/sm-kurti",router.smAdminKurtiRouter);app.use("/admin/kurti",router.adminKurtiRouter);
 app.use("/sm/kurti",worker.smKurtiRouter);app.use("/gm/kurti",worker.gmKurtiRouter);app.use(f.app);
 const actor={appUserId:f.admin,role:"sm_admin" as const};
 const accept=async(userId=f.employee)=>f.database.insert(schema.employeeAgreementAcceptances).values({userId,agreementKey:smAgreement.SM_EMPLOYEE_AGREEMENT.key,agreementVersion:smAgreement.SM_EMPLOYEE_AGREEMENT.version,agreementTitle:smAgreement.SM_EMPLOYEE_AGREEMENT.title,agreementHash:smAgreement.SM_EMPLOYEE_AGREEMENT.hash});
 return {...f,app,identity,other,storage,tools,ctx,memory,employeeMemory,calls,actor,accept,toolErrors,setAnswer:(fn:typeof answer)=>{answer=fn;}};
 }catch(e){await f.pg.close();throw e;}
}
export function toolCall(name:string,args:unknown,id=randomUUID()):ModelReply{
 return {output_text:"",output:[{type:"function_call",name,arguments:JSON.stringify(args),call_id:id,id}]};
}

