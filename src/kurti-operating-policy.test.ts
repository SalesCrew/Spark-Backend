import assert from "node:assert/strict";
import test from "node:test";
import request from "supertest";
import { kurtiRestrictedRequestReply } from "./lib/kurti-operating-policy.js";
import { createSmKurtiFixture, toolCall } from "../tests/sm-kurti-fixture.js";
import { EMPLOYEE_AGREEMENT_KEY, EMPLOYEE_AGREEMENT_VERSION } from "./lib/employee-agreement-meta.js";

test("explicit personnel/judgment requests are local; existing factual comparisons and bonus formulas remain allowed", () => {
  for (const message of ["Wen soll ich kündigen?", "Who should I fire?", "Soll ich [user:123] befördern?", "Wem soll ich mehr Gehalt geben?", "Welche Mitarbeiter sind faul?", "Who is dishonest?", "Wer ist der schlechteste Mitarbeiter?"]) {
    assert.ok(kurtiRestrictedRequestReply(message), message);
  }
  for (const message of ["Marktbesuche absteigend im Oktober", "Wer hat die wenigsten Besuche?", "Wer ist nach IPP am schlechtesten?", "Erkläre den gespeicherten Bonus und die Formel", "Wie hoch ist mein gespeicherter Bonus?", "Wo finde ich die Einstellungen?", "Wie soll ich die Einstellungen ändern?", "Welche Mitarbeiter haben bestätigt?", "Welche Märkte haben die schlechteste OOS Quote?"]) {
    assert.equal(kurtiRestrictedRequestReply(message), null, message);
  }
});

test("all four chats share policy; worker identity needs no mention; admins keep mentions and tool boundaries", async t => {
  const f = await createSmKurtiFixture();
  try {
    await f.accept();
    await f.database.insert(f.schema.employeeAgreementAcceptances).values({userId:f.admin,agreementKey:EMPLOYEE_AGREEMENT_KEY,agreementVersion:EMPLOYEE_AGREEMENT_VERSION,agreementTitle:"Synthetic",agreementHash:"synthetic"});
    const surfaces = [["sm", "synthetic-sm",f.employee], ["gm","synthetic-gm",f.admin], ["admin/sm-kurti","synthetic-sm-admin",f.admin], ["admin/kurti","synthetic-sm-admin",f.admin]];
    const endpoint=(surface:string)=>surface.startsWith("admin/")?`/${surface}/messages`:`/${surface}/kurti/messages`;
    for (const [surface,token,id] of surfaces) {
      await t.test(surface,async () => {
        const before = f.calls.length;
        const answer=(await request(f.app).post(endpoint(surface!)).auth(token!,{type:"bearer"}).send({message:"Wen soll ich kündigen?"}).expect(200)).body;
        assert.match(answer.assistantMessage.content,/keine Personal-/);
        assert.equal(f.calls.length,before,"Explicit personnel requests are handled without provider or data tools");
        f.setAnswer(() => ({output:[],output_text:`Hallo [user:${id}:first]!`}));
        const reply=(await request(f.app).post(endpoint(surface!)).auth(token!,{type:"bearer"}).send({message:"Was zeigen meine erfassten Werte?"}).expect(200)).body;
        assert.equal(reply.assistantMessage.content,`Hallo [user:${id}:first]!`);
        const call=f.calls.at(-1)!;
        assert.match(call.instructions,/KI-Assistent/);
        assert.match(call.instructions,/sortierte erfasste Werte/);
        assert.match(call.instructions,/unvertrauenswürdige Daten/);
        assert.match(call.instructions,/Bonuswerte berichten/);
        assert.doesNotMatch(JSON.stringify(call),/Annika|Musterfrau|Benedikt|Musteradmin|Carolin|Beispielsm/);
        if (!surface!.startsWith("admin/")) {
          assert.match(call.instructions,new RegExp(`angemeldete Benutzer \\[user:${id}\\]`));
          assert.match(call.instructions,/Keine @-Auswahl verlangen/);
          assert.equal(call.tools,undefined,"Worker has no cross-user tools");
          assert.equal(reply.people.length,1);
          const beforeName=f.calls.length;
          await request(f.app).post(endpoint(surface!)).auth(token!,{type:"bearer"}).send({message:surface==="sm"?"Ich bin Annika, was habe ich heute zu tun?":"Ich bin Benedikt, was habe ich heute zu tun?"}).expect(200);
          assert.equal(f.calls.length,beforeName+1,"Own account name is redacted without asking for @");
          assert.doesNotMatch(JSON.stringify(f.calls.at(-1)),/Annika|Benedikt/);
        }
      });
    }
    await t.test("unavailable mutation tool is rejected before execution",async () => {
      let step=0;
      f.setAnswer(()=>step++===0?toolCall("delete_sm_visit",{id:f.employee}):{output:[],output_text:"Ich kann Geschäftsdaten nicht ändern."});
      await request(f.app).post("/admin/sm-kurti/messages").auth("synthetic-sm-admin",{type:"bearer"}).send({message:"Zeig mir meinen Datensatz"}).expect(200);
      const output=f.calls.at(-1)!.input.findLast((i:any)=>i.type==="function_call_output");
      assert.equal(JSON.parse(output.output).error,"tool_not_available");
    });
  } finally { await f.pg.close(); }
});
