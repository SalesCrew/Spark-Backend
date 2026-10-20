import assert from "node:assert/strict";
import test from "node:test";
import request from "supertest";
import { redactRecognisablePersonalText } from "./lib/kurti-personal-text.js";
import { kurtiEnabledSchema } from "./lib/kurti-settings.js";
import { createSmKurtiFixture, toolCall } from "../tests/sm-kurti-fixture.js";
import { EMPLOYEE_AGREEMENT_KEY, EMPLOYEE_AGREEMENT_VERSION } from "./lib/employee-agreement-meta.js";
import { EventEmitter } from "node:events";
import { isolatedModule } from "../tests/isolated-module.js";

test("audit records one completion, cleans listeners, handles cancellation and tolerates log failure", async () => {
  const events: Record<string,unknown>[]=[];
  let fail=false;
  const auditModule=await isolatedModule<typeof import("./lib/kurti-observability.js")>(new URL("./lib/kurti-observability.ts",import.meta.url),{
    "./logger.js":{logger:{info:(_event:string,payload:Record<string,unknown>)=>{events.push(payload);if(fail)throw new Error("synthetic logging unavailable");}}},
  });
  class Connection extends EventEmitter {statusCode=200;writableEnded=false;}
  const connect=(response:Connection)=>auditModule.observeKurtiTurn({requestId:"Private Name private@example.invalid",body:{message:"Never log this"}} as never,response as never,{workspace:"sm",audience:"worker"});
  const completed=new Connection(),audit=connect(completed);
  audit.providerCall();audit.providerResponse({_request_id:"req_synthetic_ok"});audit.providerError({requestID:"req_synthetic_error"});
  completed.writableEnded=true;completed.emit("finish");completed.emit("close");
  assert.equal(events.length,1);assert.equal(completed.listenerCount("finish"),0);assert.equal(completed.listenerCount("close"),0);
  assert.deepEqual([...events[0]!.providerRequestIds as string[]],["req_synthetic_ok","req_synthetic_error"]);
  assert.equal(events[0]!.requestId,null);assert.doesNotMatch(JSON.stringify(events),/Private Name|private@example|Never log/);
  const cancelled=new Connection();connect(cancelled);cancelled.emit("close");cancelled.emit("finish");
  assert.equal(events.length,2);assert.equal(events[1]!.result,"cancelled");assert.equal(events[1]!.statusCode,499);
  fail=true;const brokenLog=new Connection();connect(brokenLog);brokenLog.writableEnded=true;
  assert.doesNotThrow(()=>brokenLog.emit("finish"));assert.equal(brokenLog.listenerCount("close"),0);
});

test("local privacy rules remove clear introductions, name lists and labelled contacts", () => {
  for (const text of ["Mein Name ist Evelina Beispielmann.", "Ich heiße Frederike Beispielperson", "My name is Jonathan Exampleperson", "Frau Cornelia Beispielmann ruft an.", "Alex Beate Julius Margit Peter Aphrodite"]) {
    assert.doesNotMatch(redactRecognisablePersonalText(text), /Evelina|Frederike|Jonathan|Cornelia|Alex|Beate|Julius|Margit|Peter|Aphrodite/);
  }
  assert.equal(redactRecognisablePersonalText("Telefon: +43 660 1234567, 42 Besuche"), "Telefon: [Kontakt entfernt], 42 Besuche");
  assert.equal(redactRecognisablePersonalText("mail@example.invalid"), "[Kontakt entfernt]");
  // Existing operational questions, numbers, brands, dates and ID references retain their meaning.
  for (const text of ["Besuchte Märkte Marktbesuche Leistungsstunden Merchandizer", "Billa Spar Hofer Lidl", "Juni Juli August September", "Wie viele Marktbesuche im Oktober?", "852 Besuche, 599 OOS, 76,3 % behoben", "Zeige ein Linechart aller GMs im RED 10", "Was macht Kurti?", "Show Visits By Employee", "Erkläre Mir Den Besuch", "Compare Market Availability Before And After", "[user:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee] hat 12 Besuche."]) {
    assert.equal(redactRecognisablePersonalText(text), text);
  }
});

test("emergency configuration defaults to available and validates both boolean states", () => {
  assert.equal(kurtiEnabledSchema.parse(undefined), true);
  for (const value of [true, "true", "1", "yes", " ON "]) assert.equal(kurtiEnabledSchema.parse(value), true);
  for (const value of [false, "false", "0", "no", " OFF "]) assert.equal(kurtiEnabledSchema.parse(value), false);
  for (const value of ["", "enable", "maybe", 0, null]) assert.equal(kurtiEnabledSchema.safeParse(value).success, false);
});

test("four Kurti surfaces preserve normal replies, minimise audits and support an operator pause", async t => {
  const f = await createSmKurtiFixture();
  const surfaces = [
    ["/sm/kurti", "synthetic-sm", "sm", "worker"], ["/gm/kurti", "synthetic-gm", "gm", "worker"],
    ["/admin/kurti", "synthetic-sm-admin", "gm", "admin"], ["/admin/sm-kurti", "synthetic-sm-admin", "sm", "admin"],
  ] as const;
  const snapshot = async () => JSON.stringify({
    users: await f.database.select().from(f.schema.users),
    employeeMemory: await f.database.select().from(f.schema.gmKurtiMessages),
    adminMemory: await f.database.select().from(f.schema.adminKurtiMessages),
  });
  try {
    await f.accept();
    await f.database.insert(f.schema.employeeAgreementAcceptances).values({userId:f.admin,agreementKey:EMPLOYEE_AGREEMENT_KEY,agreementVersion:EMPLOYEE_AGREEMENT_VERSION,agreementTitle:"Synthetic",agreementHash:"synthetic"});
    f.setAnswer(() => ({output:[],output_text:"RED 10: 12 Marktbesuche. Quelle: Spark.",_request_id:"req_synthetic_provider_1"}));
    for (const [path, token, workspace, audience] of surfaces) {
      await t.test(`${workspace} ${audience}: same reply and no content in the audit`, async () => {
        const start = f.logs.length;
        const response = await request(f.app).post(path+"/messages").auth(token,{type:"bearer"}).set("user-agent","Private Name private@example.invalid").send({message:"Zeige die Marktbesuche im RED 10."}).expect(200);
        assert.equal(response.body.assistantMessage.content, "RED 10: 12 Marktbesuche. Quelle: Spark.");
        assert.equal(response.headers["cache-control"], "private, no-store");
        const logs = f.logs.slice(start).filter(log=>log.event==="kurti_turn_completed");
        assert.equal(logs.length,1);
        const payload=logs[0]!.payload;
        assert.equal(payload.workspace,workspace);assert.equal(payload.audience,audience);assert.equal(payload.result,"answered");
        assert.equal(payload.providerCalls,1);assert.equal(payload.toolCalls,0);assert.equal(payload.statusCode,200);
        assert.deepEqual([...payload.providerRequestIds as string[]],["req_synthetic_provider_1"]);
        assert.match(payload.requestId as string,/^[0-9a-f-]{36}$/i);
        assert.doesNotMatch(JSON.stringify(logs),/Annika|Benedikt|Musterfrau|Private Name|private@example|Marktbesuche|Quelle|synthetic-sm|authorization/);
        assert.ok(!JSON.stringify(logs).includes(f.employee));assert.ok(!JSON.stringify(logs).includes(f.admin));
        const history=await request(f.app).get(path+"/messages").auth(token,{type:"bearer"}).expect(200);
        assert.equal(history.headers["cache-control"],"private, no-store");assert.equal(history.body.configured,true);
      });
    }
    await t.test("admin handles unregistered name lists locally; old text is also filtered before later provider rounds",async()=>{
      const start=f.calls.length;
      const response=await request(f.app).post("/admin/kurti/messages").auth("synthetic-sm-admin",{type:"bearer"}).send({message:"Alex Beate Julius Margit Peter Aphrodite"}).expect(200);
      assert.equal(f.calls.length,start);assert.match(response.body.assistantMessage.content,/Wähle die Person mit @/);
      assert.doesNotMatch(JSON.stringify(response.body.messages),/Alex|Beate|Julius|Margit|Peter|Aphrodite/);
      assert.equal(f.logs.at(-1)!.payload.result,"mention_required");assert.equal(f.logs.at(-1)!.payload.providerCalls,0);
      await f.memory.saveAdminKurtiExchange({adminUserId:f.admin,workspace:"gm",userContent:"Alex Beate Julius Margit Peter Aphrodite",assistantContent:"Alte Antwort",identityProtected:true});
      const oldRows=JSON.stringify(await f.database.select().from(f.schema.adminKurtiMessages));
      await request(f.app).post("/admin/kurti/messages").auth("synthetic-sm-admin",{type:"bearer"}).send({message:"Zeig die Werte."}).expect(200);
      assert.doesNotMatch(JSON.stringify(f.calls.at(-1)),/Alex|Beate|Julius|Margit|Peter|Aphrodite/);
      assert.match(oldRows,/Alex Beate/); // Existing storage is not rewritten.
      assert.match(JSON.stringify(await f.database.select().from(f.schema.adminKurtiMessages)),/Alex Beate/);
    });
    await t.test("worker text and every tool round use the same local safeguard",async()=>{
      for (const [path,token] of surfaces.filter(row=>row[3]==="worker")) {
        await request(f.app).post(path+"/messages").auth(token,{type:"bearer"}).send({message:"Mein Name ist Evelina Beispielmann. Wie viele Besuche habe ich?"}).expect(200);
        assert.doesNotMatch(JSON.stringify(f.calls.at(-1)),/Evelina|Beispielmann/);
      }
      let round=0;
      f.setAnswer(()=>round++===0?toolCall("get_admin_overview",{}):{output:[],output_text:"12 Besuche.",_request_id:"req_synthetic_provider_2"});
      await request(f.app).post("/admin/kurti/messages").auth("synthetic-sm-admin",{type:"bearer"}).send({message:"Zeige den Überblick."}).expect(200);
      const payload=f.logs.filter(log=>log.event==="kurti_turn_completed").at(-1)!.payload;
      assert.equal(payload.providerCalls,2);assert.equal(payload.toolCalls,1);
      assert.deepEqual([...payload.providerRequestIds as string[]],["req_synthetic_provider_2"]);
    });
    await t.test("untrusted provider identifiers never become log content",async()=>{
      f.setAnswer(()=>({output:[],output_text:"12 Besuche.",_request_id:"Private Person private@example.invalid"}));
      await request(f.app).post("/admin/kurti/messages").auth("synthetic-sm-admin",{type:"bearer"}).send({message:"Zeige die Werte."}).expect(200);
      assert.deepEqual([...f.logs.at(-1)!.payload.providerRequestIds as string[]],[]);
      assert.doesNotMatch(JSON.stringify(f.logs.at(-1)),/Private Person|private@example/);
    });
    await t.test("provider failures keep history intact and correlate errors without logging echoed input",async()=>{
      const before=await snapshot();
      f.setAnswer(()=>{throw f.providerError("Private Person private@example.invalid echoed by provider",500,"req_synthetic_failed");});
      for(const [path,token] of surfaces){
        const start=f.logs.length;
        await request(f.app).post(path+"/messages").auth(token,{type:"bearer"}).send({message:"Zeige die Werte."}).expect(502);
        const events=f.logs.slice(start);
        const audit=events.filter(log=>log.event==="kurti_turn_completed");
        assert.equal(audit.length,1);assert.equal(audit[0]!.payload.result,"provider_error");
        assert.deepEqual([...audit[0]!.payload.providerRequestIds as string[]],["req_synthetic_failed"]);
        assert.doesNotMatch(JSON.stringify(events),/Private Person|private@example|echoed by provider/);
      }
      assert.equal(await snapshot(),before);
      f.setAnswer(()=>({output:[],output_text:"12 Besuche."}));
    });
    await t.test("disabled service calls no provider, changes no saved data, retains access checks and can resume",async()=>{
      const before=await snapshot(), calls=f.calls.length;
      f.env.OPENAI_KURTI_ENABLED=false;
      for (const [path,token] of surfaces) {
        const response=await request(f.app).post(path+"/messages").auth(token,{type:"bearer"}).send({message:"Zeige die Werte."}).expect(503);
        assert.equal(response.body.code,"kurti_temporarily_unavailable");
        assert.equal(f.logs.at(-1)!.payload.result,"disabled");assert.equal(f.logs.at(-1)!.payload.providerCalls,0);
      }
      await request(f.app).post("/admin/kurti/messages").send({message:"Zeige die Werte."}).expect(401);
      await request(f.app).post("/admin/kurti/messages").auth("synthetic-sm",{type:"bearer"}).send({message:"Zeige die Werte."}).expect(403);
      assert.equal(f.calls.length,calls);assert.equal(await snapshot(),before);
      const history=await request(f.app).get("/admin/kurti/messages").auth("synthetic-sm-admin",{type:"bearer"}).expect(200);
      assert.equal(history.body.configured,false);assert.ok(history.body.messages.length>0);
      f.env.OPENAI_KURTI_ENABLED=true;
      await request(f.app).post("/admin/kurti/messages").auth("synthetic-sm-admin",{type:"bearer"}).send({message:"Zeige die Werte."}).expect(200);
    });
  } finally { await f.pg.close(); }
});
