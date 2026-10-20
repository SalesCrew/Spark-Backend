import assert from "node:assert/strict";
import test from "node:test";
import request from "supertest";
import { randomUUID } from "node:crypto";
import { createSmKurtiFixture,toolCall } from "../tests/sm-kurti-fixture.js";
import { EMPLOYEE_AGREEMENT_KEY,EMPLOYEE_AGREEMENT_VERSION } from "./lib/employee-agreement-meta.js";

test("Kurti privacy boundary protects all four surfaces and preserves canonical IDs",async t=>{
 const f=await createSmKurtiFixture();
 const admin=(workspace="sm")=>request(f.app).post(workspace==="sm"?"/admin/sm-kurti/messages":"/admin/kurti/messages").auth("synthetic-sm-admin",{type:"bearer"});
 try{
 await f.accept();await f.database.insert(f.schema.employeeAgreementAcceptances).values({userId:f.admin,agreementKey:EMPLOYEE_AGREEMENT_KEY,agreementVersion:EMPLOYEE_AGREEMENT_VERSION,agreementTitle:"Synthetic",agreementHash:"synthetic"});
 const before=JSON.stringify((await f.database.select().from(f.schema.users)));
 const id=`[user:${f.employee}]`;
 await t.test("plain names handled before provider; unselected @ and forged IDs rejected",async()=>{
  const count=f.calls.length;
  const plain=(await admin().send({message:"Was macht annika?"}).expect(200)).body;
  assert.match(plain.assistantMessage.content,/kenne keine Namen/);assert.equal(f.calls.length,count);
  await admin().send({message:"Was macht @Annika?"}).expect(200);assert.equal(f.calls.length,count);
  await admin().send({message:`Was macht [user:${randomUUID()}]?`}).expect(403);
  await admin().send({message:"Was macht [user:malformed]?"}).expect(403);
  await request(f.app).post("/sm/kurti/messages").auth("synthetic-sm",{type:"bearer"}).send({message:`Was macht [user:${f.other}]?`}).expect(403);
 });
 await t.test("directory is app-only and owner/role scoped",async()=>{
  const own=(await request(f.app).get("/sm/kurti/people").auth("synthetic-sm",{type:"bearer"}).expect(200)).body;
  assert.deepEqual(own.people.map((p:any)=>p.id),[f.employee]);assert.equal(own.people[0].label,"Annika Musterfrau");
  const all=(await request(f.app).get("/admin/sm-kurti/people").auth("synthetic-sm-admin",{type:"bearer"}).expect(200)).body;
  assert.ok(all.people.some((p:any)=>p.id===f.other));assert.doesNotMatch(JSON.stringify(all),/email|supabase|password|token/i);
  await request(f.app).get("/admin/kurti/people").auth("synthetic-gm-admin",{type:"bearer"}).expect(403);
  await request(f.app).get("/admin/sm-kurti/people").expect(401);
 });
 await t.test("every admin round removes live/legacy names and contacts, model returns IDs",async()=>{
  for(const workspace of ["gm","sm"] as const){
   await f.memory.saveAdminKurtiExchange({adminUserId:f.admin,workspace,userContent:"Unbekannte Altperson hat einen offenen Besuch",assistantContent:"Benedikt Musteradmin prüft",now:new Date()});
   f.setAnswer(body=>{
    const outputs=body.input.filter((i:any)=>i.type==="function_call_output");
    if(!outputs.length)return toolCall(workspace==="sm"?"search_sms":"get_admin_overview",workspace==="sm"?{search:null,active:null,limit:10}:{});
    return {output:[],output_text:`${id} hat 12 Besuche.`};
   });
   const reply=(await admin(workspace).send({message:`Wie viele Besuche hat ${id}?`}).expect(200)).body;
   assert.equal(reply.assistantMessage.content,`${id} hat 12 Besuche.`);
   assert.ok(reply.people.some((p:any)=>p.label==="Annika Musterfrau"));
  }
  for(const call of f.calls){assert.equal(call.model,"gpt-6-luna");assert.equal(call.store,false);assert.match(call.instructions,/HÖCHSTE PRIORITÄT/);assert.ok(!JSON.stringify(call).includes("Unbekannte Altperson"));assert.doesNotMatch(JSON.stringify(call),/Annika|Musterfrau|Benedikt|Musteradmin|Carolin|Beispielsm|Denise|Doris|\bKurt\b|second@preview/i);}
 });
 await t.test("both worker roles sanitize profile and old chats before provider",async()=>{
  f.setAnswer(()=>({output:[],output_text:`${id}: Dein Besuch ist gespeichert.`}));
  for(const [workspace,token,userId] of [["sm","synthetic-sm",f.employee],["gm","synthetic-gm",f.admin]]){
   await f.employeeMemory.saveKurtiExchange({gmUserId:userId,userContent:"Annika ist im Markt",assistantContent:"Benedikt sieht das",now:new Date()});
   await request(f.app).post(`/${workspace}/kurti/messages`).auth(token,{type:"bearer"}).send({message:"Was steht heute an?"}).expect(200);
   const body=f.calls.at(-1)!;assert.doesNotMatch(JSON.stringify(body),/Annika|Musterfrau|Benedikt|Musteradmin|Denise|Doris|\bKurt\b/i);assert.equal(body.model,"gpt-6-luna");
  }
 });
 await t.test("historical names, Unicode, name collisions, schemas and free-text minimization",()=>{
  const other=randomUUID(),actor={...f.actor,appUserId:f.admin};
  const boundary=f.identity.createKurtiIdentityBoundary([
   {id:f.admin,firstName:"Luna",lastName:"Müller",email:"luna@example.invalid",role:"admin"},
   {id:other,firstName:"Luna",lastName:"Groß",email:"gross@example.invalid",role:"sm"}
  ],actor);
  assert.equal(boundary.redactText("LUNA"),"[Person]");
  assert.equal(boundary.redactText("Luna Mueller"),`[user:${f.admin}]`);
  assert.equal(boundary.redactText("🙂 Luna Müller!"),`🙂 [user:${f.admin}]!`);
  assert.equal(boundary.redactText("Lu\u200bna Müller"),`[user:${f.admin}]`);
  assert.equal(boundary.redactText("Luna Mu\u0308ller"),`[user:${f.admin}]`);
  const mixed=boundary.sanitize({smUserId:other,createdByUserId:f.admin,smName:"Luna Groß",createdByName:"Luna Müller"});
  assert.equal(mixed.smName,`[user:${other}]`);assert.equal(mixed.createdByName,`[user:${f.admin}]`);
  const keyed=boundary.sanitize({"Luna Müller":12});assert.equal(keyed[`[user:${f.admin}]`],12);
  const raw={gm_user_id:f.admin,gm_name:"Historische Person",comment:"Ein Fremder Namens Frederik",email:"secret@example.invalid",nested:{sm_name_snapshot:"Ehemalige Person"}};
  const safe=boundary.sanitizeToolResult(JSON.stringify(raw),"get_gm_context");
  assert.doesNotMatch(safe,/Historische|Ehemalige|Frederik|secret@/);assert.match(safe,/Freitext nicht übermittelt/);
  assert.doesNotMatch(boundary.redactText("Historische Person meldet sich"),/Historische/);
  const exportArgs=JSON.parse(boundary.toolArguments(JSON.stringify({filters:{gmNames:[`[user:${f.admin}]`],gmUserIds:[]}})));
  assert.deepEqual(exportArgs.filters,{gmNames:[],gmUserIds:[f.admin]});
  const schema={type:"function",name:"search_sms",parameters:{type:"object",properties:{firstName:{type:"string"},email:{type:"string"}}}};
  const body=boundary.providerRequest({model:"gpt-6-luna",instructions:"Hallo Luna",tools:[schema],input:[{role:"user",content:"Luna",id:"luna"},{type:"reasoning",encrypted_content:"Luna opaque"}]});
  assert.equal(body.model,"gpt-6-luna");assert.deepEqual(body.tools,[schema]);assert.equal(body.input[0].role,"user");assert.equal(body.input[1].encrypted_content,"Luna opaque");assert.doesNotMatch(body.instructions!,/Luna/);
 });
 assert.equal(JSON.stringify(await f.database.select().from(f.schema.users)),before,"No business identity rows were rewritten");
 }finally{await f.pg.close();}
});
