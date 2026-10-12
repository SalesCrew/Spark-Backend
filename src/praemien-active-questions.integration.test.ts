import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
import { activeBonusQuestions } from "./lib/praemien-active-questions.js";
import { createPraemienWorkspaceRouter } from "./routes/praemien-workspace.js";
import { mutateWorkspace } from "./lib/praemien-workspace.js";

type Fixture = Awaited<ReturnType<typeof praemienFixture>>;
const at = new Date("2026-10-12T10:00:00Z");
async function campaign(f: Fixture, section: string, patch: {
  status?: string; schedule?: string; start?: string; end?: string;
  deleted?: boolean; questionnaireDeleted?: boolean; sections?: string[];
} = {}) {
  const scope = ["standard", "flex", "billa"].includes(section) ? "main" : section;
  const id = randomUUID(), questionnaire = randomUUID(), module = randomUUID(), question = randomUUID();
  await f.pg.query(`insert into fragebogen_${scope}(id,section_keywords,is_deleted) values($1,$2,$3)`, [questionnaire, patch.sections ?? [section], patch.questionnaireDeleted ?? false]);
  await f.pg.query(`insert into module_${scope}(id) values($1)`, [module]);
  await f.pg.query(`insert into fragebogen_${scope}_module(fragebogen_id,module_id) values($1,$2)`, [questionnaire,module]);
  await f.pg.query("insert into question_bank_shared(id,text,question_type) values($1,$2,'numeric')", [question,`Synthetic ${section} ${id}`]);
  await f.pg.query(`insert into module_${scope}_question(module_id,question_id) values($1,$2)`, [module,question]);
  await f.pg.query("insert into question_scoring(question_id,score_key,boni) values($1,'__value__',2)",[question]);
  await f.pg.query("insert into campaigns(id,section,current_fragebogen_id,status,schedule_type,start_date,end_date,is_deleted) values($1,$2,$3,$4,$5,$6,$7,$8)", [id,section,questionnaire,patch.status ?? "active",patch.schedule ?? "always",patch.start ?? null,patch.end ?? null,patch.deleted ?? false]);
  return { id, questionnaire, module, question, scope };
}

test("Boni sources include only live campaign questionnaires across all six GM visit sections", async () => {
  const f = await praemienFixture();
  try {
    const live = [];
    for (const section of ["standard","flex","billa","kuehler","mhd","durcharbeit"]) live.push(await campaign(f, section));
    const scheduled = await campaign(f,"standard",{status:"scheduled",schedule:"scheduled",start:"2026-10-12",end:"2026-10-12"});
    const specialIds: string[] = [];
    for (const scope of ["main","kuehler","mhd","durcharbeit"]) {
      const x = live.find(x=>x.scope===scope)!, id=randomUUID();
      await f.pg.query("insert into question_bank_shared(id,text,question_type,is_spezial) values($1,$2,'yesno',true)",[id,`Synthetic special ${scope}`]);
      await f.pg.query("insert into question_scoring(question_id,score_key,boni) values($1,'Ja',3)",[id]);
      await f.pg.query(`insert into fragebogen_${scope}_spezial_question(fragebogen_id,question_id) values($1,$2)`,[x.questionnaire,id]);
      specialIds.push(id);
    }
    const exclusions = [
      await campaign(f,"standard",{status:"inactive"}),
      await campaign(f,"standard",{status:"scheduled",schedule:"scheduled",start:"2026-10-13",end:"2026-10-30"}),
      await campaign(f,"standard",{status:"scheduled",schedule:"scheduled",start:"2026-09-01",end:"2026-10-11"}),
      await campaign(f,"standard",{status:"active",schedule:"scheduled",start:"2026-09-01",end:"2026-10-11"}),
      await campaign(f,"standard",{status:"scheduled"}),
      await campaign(f,"standard",{schedule:"scheduled",end:"2026-10-30"}),
      await campaign(f,"standard",{schedule:"scheduled",start:"2026-10-01"}),
      await campaign(f,"standard",{deleted:true}),
      await campaign(f,"standard",{questionnaireDeleted:true}),
      await campaign(f,"billa",{sections:["standard"]}),
    ];
    for (const value of [null,0]) {
      const x=await campaign(f,"standard");
      await f.pg.query("update question_scoring set boni=$2 where question_id=$1",[x.question,value]);
      exclusions.push(x);
    }
    const withoutScores=await campaign(f,"standard");
    await f.pg.query("delete from question_scoring where question_id=$1",[withoutScores.question]);
    exclusions.push(withoutScores);
    const removedScore=await campaign(f,"standard");
    await f.pg.query("update question_scoring set is_deleted=true where question_id=$1",[removedScore.question]);
    exclusions.push(removedScore);
    for (const table of ["module_main","module_main_question","fragebogen_main_module","question_bank_shared"]) {
      const x = await campaign(f,"standard");
      const key = table === "module_main" ? "id" : table === "module_main_question" ? "question_id" : table === "fragebogen_main_module" ? "fragebogen_id" : "id";
      const value = table === "module_main" ? x.module : table === "fragebogen_main_module" ? x.questionnaire : x.question;
      await f.pg.query(`update ${table} set is_deleted=true where ${key}=$1`,[value]);
      exclusions.push(x);
    }
    const orphan = randomUUID();
    await f.pg.query("insert into question_bank_shared(id,text,question_type) values($1,'Unlinked old question','numeric')",[orphan]);
    const rows = await activeBonusQuestions(f.database,at), ids = new Set(rows.map(q => q.id));
    for (const x of [...live,scheduled]) {
      assert.ok(ids.has(x.question),"Editorially inactive questionnaire remains eligible through its live campaign");
      const section=live.indexOf(x)>=0 ? ["standard","flex","billa","kuehler","mhd","durcharbeit"][live.indexOf(x)] : "standard";
      assert.deepEqual(rows.find(q=>q.id===x.question)!.activeSections,[section]);
    }
    for (const id of specialIds) assert.ok(ids.has(id),"Linked Spezialfragen supported across all questionnaire scopes");
    for (const x of exclusions) assert.ok(!ids.has(x.question),"Inactive, expired, future, deleted or mismatched context excluded");
    assert.ok(!ids.has(orphan));
    assert.equal(rows.length,12);
    assert.equal(new Set(rows.map(q => q.id)).size,rows.length);
  } finally { await f.pg.close(); }
});

test("Vienna date boundaries, shared-question deduplication, score preservation and timed Spezialfragen", async () => {
  const f = await praemienFixture();
  try {
    const x = await campaign(f,"standard",{status:"scheduled",schedule:"scheduled",start:"2026-10-12",end:"2026-10-12"});
    await f.pg.query("insert into campaigns(id,section,current_fragebogen_id,status,schedule_type) values($1,'standard',$2,'active','always')",[randomUUID(),f.ids.catalogFragebogen]);
    await f.pg.query("insert into question_scoring(question_id,score_key,boni) values($1,'obsolete',999)",[f.ids.question]);
    await f.pg.query("update question_scoring set is_deleted=true where score_key='obsolete'");
    const specials: { id: string; visible: boolean }[] = [];
    for (const [config,visible] of [
      [{},true],
      [{spezialfragePeriod:{startDate:"2026-10-12",endDate:"2026-10-12"}},true],
      [{spezialfragePeriod:{startDate:"2026-10-13",endDate:"2026-10-14"}},false],
      [{spezialfragePeriod:{startDate:"2026-10-01",endDate:"2026-10-11"}},false],
      [{spezialfragePeriod:{startDate:"bad",endDate:"2026-10-12"}},false],
    ] as const) {
      const id = randomUUID();
      await f.pg.query("insert into question_bank_shared(id,text,question_type,is_spezial,config) values($1,$2,'yesno',true,$3)",[id,`Special ${id}`,JSON.stringify(config)]);
      await f.pg.query("insert into question_scoring(question_id,score_key,boni) values($1,'Ja',1)",[id]);
      await f.pg.query("insert into fragebogen_main_spezial_question(fragebogen_id,question_id) values($1,$2)",[x.questionnaire,id]);
      specials.push({id,visible});
    }
    const before = await activeBonusQuestions(f.database,new Date("2026-10-11T21:59:59Z"));
    assert.ok(!before.some(q => q.id===x.question));
    const first = await activeBonusQuestions(f.database,new Date("2026-10-11T22:00:00Z"));
    assert.ok(first.some(q => q.id===x.question));
    for (const s of specials) assert.equal(first.some(q=>q.id===s.id),s.visible);
    assert.deepEqual(first.find(q=>q.id===f.ids.question)!.scores,[{scoreKey:"__value__",weight:1}]);
    assert.equal(first.filter(q=>q.id===f.ids.question).length,1);
    assert.ok((await activeBonusQuestions(f.database,new Date("2026-10-12T21:59:59Z"))).some(q=>q.id===x.question));
    assert.ok(!(await activeBonusQuestions(f.database,new Date("2026-10-12T22:00:00Z"))).some(q=>q.id===x.question));
    // The same local-midnight rule holds after the daylight-saving transition.
    await f.pg.query("update campaigns set start_date='2026-12-12',end_date='2026-12-12' where id=$1",[x.id]);
    assert.ok(!(await activeBonusQuestions(f.database,new Date("2026-12-11T22:59:59Z"))).some(q=>q.id===x.question));
    assert.ok((await activeBonusQuestions(f.database,new Date("2026-12-11T23:00:00Z"))).some(q=>q.id===x.question));
  } finally { await f.pg.close(); }
});

test("HTTP filtering never rewrites saved sources, values or frozen bonus history", async () => {
  const f = await praemienFixture();
  try {
    const old = await campaign(f,"standard",{status:"inactive"});
    const model = structuredClone(f.model), metric = model.pillars[0]!.metrics[0]!;
    metric.sources = [old.question,f.ids.question].map(questionId=>({questionId,label:"Saved synthetic source",section:"standard",scoreKey:"__value__",factor:true,weight:2,minFrequency:8,chains:["Billa"],counting:"once" as const}));
    const saved = await mutateWorkspace(f.database,f.ids.wave,f.workspace.revision,{id:f.ids.admin,name:"Synthetic admin"},{type:"rules",model});
    const app = express(); app.use(express.json());
    app.use((req,res,next)=>{if(req.headers.authorization!=="Bearer local-admin"){res.sendStatus(401);return;} (req as any).authUser={appUserId:f.ids.admin,role:"admin"};next();});
    app.use("/admin/praemien/workspace",createPraemienWorkspaceRouter(f.database));
    const url = `/admin/praemien/workspace/waves/${f.ids.wave}`;
    const sources = "/admin/praemien/workspace/sources";
    assert.equal((await request(app).get(sources)).status,401);
    const before = (await f.pg.query("select model,revision,closed_snapshot from praemien_wave_settings")).rows;
    const catalog = await request(app).get(sources).set("Authorization","Bearer local-admin");
    assert.equal(catalog.status,200);
    assert.equal(catalog.headers["cache-control"],"private, no-store");
    assert.deepEqual(catalog.body.questions.map((q:any)=>q.id),[f.ids.question]);
    assert.deepEqual((await f.pg.query("select model,revision,closed_snapshot from praemien_wave_settings")).rows,before);
    const updated = await request(app).put(url).set("Authorization","Bearer local-admin").send({revision:saved.revision,type:"rules",model});
    assert.equal(updated.status,200);
    assert.deepEqual(updated.body.model.pillars[0].metrics[0].sources,metric.sources);
    assert.deepEqual(updated.body.entries,saved.entries);
    assert.deepEqual(updated.body.results.map((r:any)=>r.earned),saved.results.map(r=>r.earned));
    await f.pg.query("update campaigns set status='inactive' where id=$1",[f.ids.catalogCampaign]);
    assert.deepEqual((await request(app).get(sources).set("Authorization","Bearer local-admin")).body.questions,[]);
    const reopened = await request(app).get(url).set("Authorization","Bearer local-admin");
    assert.deepEqual(reopened.body.model.pillars[0].metrics[0].sources,metric.sources);
    const snapshot = {model:reopened.body.model,results:reopened.body.results};
    await f.pg.query("update praemien_wave_settings set closed_snapshot=$2,closed_at=now() where wave_id=$1",[f.ids.wave,JSON.stringify(snapshot)]);
    await request(app).get(sources).set("Authorization","Bearer local-admin");
    assert.deepEqual((await f.pg.query("select closed_snapshot from praemien_wave_settings where wave_id=$1",[f.ids.wave])).rows[0]!.closed_snapshot,snapshot);
  } finally { await f.pg.close(); }
});
