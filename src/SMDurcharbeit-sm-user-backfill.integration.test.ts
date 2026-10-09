import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";
import { buildSMDurcharbeitSmLinkBackfill, reviewSMDurcharbeitSmLinks, type SMDurcharbeitSmLinkInventory } from "../operations/SMDurcharbeit-sm-user-backfill.js";

type Fixture = Awaited<ReturnType<typeof createSMDurcharbeitFixture>>;
async function inventory(f: Fixture): Promise<SMDurcharbeitSmLinkInventory> {
  return {
    markets: (await f.pg.query(`select r.sm_market_id as "marketId",r.smdurcharbeit_verplanung as "sourcePerson",r.smdurcharbeit_sm_user_id as "linkedUserId",
      m.assigned_sm_user_id as "assignedUserId",m.is_active as active,m.is_deleted as deleted
      from sm_smdurcharbeit_markets r join sm_markets m on m.id=r.sm_market_id`)).rows as SMDurcharbeitSmLinkInventory["markets"],
    people: (await f.pg.query(`select id,first_name as "firstName",last_name as "lastName" from users where role='sm' and is_active=true and deleted_at is null`)).rows as SMDurcharbeitSmLinkInventory["people"],
  };
}
async function apply(f: Fixture, query: string) {
  try { await f.pg.exec(query); } catch (error) { await f.pg.exec("ROLLBACK"); throw error; }
}

test("SMDurcharbeit backfill reviews exact full names and refuses uncertain or conflicting owners", () => {
  const first = randomUUID(), duplicateA = randomUUID(), duplicateB = randomUUID(), other = randomUUID();
  const people = [{ id: first, firstName: "Synthetic", lastName: "Straßner" },
    { id: duplicateA, firstName: "Equal", lastName: "Person" }, { id: duplicateB, firstName: "Equal", lastName: "Person" }];
  const m = (sourcePerson: string | null, overrides: Partial<SMDurcharbeitSmLinkInventory["markets"][number]> = {}) =>
    ({ marketId: randomUUID(), sourcePerson, assignedUserId: null, linkedUserId: null, active: true, deleted: false, ...overrides });
  const rows = [m("Strassner Synthetic"),m("Synthetic Strassner"),m("Equal Person"),m("Missing Person"),m(null),
    m("Synthetic Straßner",{ linkedUserId: other }),m("Synthetic Straßner",{ assignedUserId: other }),
    m("Synthetic Straßner",{ active: false }),m("Synthetic Straßner",{ deleted: true }),m("Synthetic Straßner",{ linkedUserId: first })];
  const review = reviewSMDurcharbeitSmLinks({ markets: rows, people });
  assert.deepEqual(review.rows.map(row => row.reason), ["ready","ready","ambiguous_name","no_match","no_match","existing_link_conflict","assignment_conflict","inactive_market","inactive_market","already_linked"]);
  assert.equal(review.approved.length, 3);
  assert.deepEqual(review.approved.map(row => row.userId),[first,first,first]);
  assert.throws(() => reviewSMDurcharbeitSmLinks({ markets: [rows[0]!,rows[0]!],people }),/Duplicate/);
  assert.throws(() => buildSMDurcharbeitSmLinkBackfill({ markets: [],people }),/No reviewed/);
  assert.throws(() => buildSMDurcharbeitSmLinkBackfill({ markets: [m("$SMDurcharbeit_backfill$ Synthetic Straßner")],people: [{id:first,firstName:"$SMDurcharbeit_backfill$ Synthetic",lastName:"Straßner"}] }),/delimiter/);
});

test("SMDurcharbeit guarded backfill changes only saved IDs, retains duplicate rows and is repeatable", async () => {
  const f = await createSMDurcharbeitFixture();
  const other = randomUUID(), secondMarket = randomUUID();
  try {
    await f.pg.query("insert into users(id,first_name,last_name,role) values($1,'Léa',$2,'sm')",[other,"O'Neil"]);
    await f.pg.query("insert into sm_markets(id,name,chain,address,postal_code,city,region) values($1,'Synthetic same address','Spar','Synthetic 1','1010','Wien','Ost')",[secondMarket]);
    await f.pg.query("insert into sm_smdurcharbeit_markets(sm_market_id,smdurcharbeit_verplanung,smdurcharbeit_source_values) values($1,'SM Local',$3::jsonb),($2,$4,$3::jsonb)",[f.market,secondMarket,JSON.stringify({Original:"Preserved","Quote":"O'Neil"}),"O'Neil Léa"]);
    await f.assignment();
    const before = (await f.pg.query("select * from sm_smdurcharbeit_markets order by sm_market_id")).rows;
    const canonical = (await f.pg.query("select * from sm_markets order by id")).rows;
    const history = (await f.pg.query("select * from sm_assignments order by id")).rows;
    const plan = buildSMDurcharbeitSmLinkBackfill(await inventory(f));
    assert.equal(plan.review.approved.length,2);
    await apply(f,plan.query);
    const after = (await f.pg.query("select * from sm_smdurcharbeit_markets order by sm_market_id")).rows;
    assert.deepEqual(after.map(({smdurcharbeit_sm_user_id,...row})=>row),before.map(({smdurcharbeit_sm_user_id,...row})=>row));
    assert.equal(after.find(row=>row.sm_market_id===f.market)!.smdurcharbeit_sm_user_id,f.employee);
    assert.equal(after.find(row=>row.sm_market_id===secondMarket)!.smdurcharbeit_sm_user_id,other);
    assert.deepEqual((await f.pg.query("select * from sm_markets order by id")).rows,canonical);
    assert.deepEqual((await f.pg.query("select * from sm_assignments order by id")).rows,history);
    await apply(f,plan.query);
    assert.deepEqual((await f.pg.query("select * from sm_smdurcharbeit_markets order by sm_market_id")).rows,after);
    assert.deepEqual((await f.pg.query("select count(*)::int as count from sm_smdurcharbeit_campaigns")).rows,[{count:0}]);
    assert.deepEqual((await f.pg.query("select count(*)::int as count from sm_smdurcharbeit_month_targets")).rows,[{count:0}]);
  } finally { await f.pg.close(); }
});

test("SMDurcharbeit backfill rolls back stale contexts and any unexpected source-field side effect", async t => {
  const changes = [
    {name:"changed imported name",sql:"update sm_smdurcharbeit_markets set smdurcharbeit_verplanung='Changed Synthetic'",error:/context changed/},
    {name:"changed canonical assignment",sql:"update sm_markets set assigned_sm_user_id=$1 where id=$2",error:/context changed/},
    {name:"different saved link",sql:"update sm_smdurcharbeit_markets set smdurcharbeit_sm_user_id=$1",error:/context changed/},
    {name:"inactive account",sql:"update users set is_active=false where id=$1",error:/directory changed/},
    {name:"new matching account",sql:"insert into users(id,first_name,last_name,role) values($1,'Local','SM','sm')",error:/directory changed/},
    {name:"missing market registry",sql:"delete from sm_smdurcharbeit_markets",error:/context changed/},
  ];
  for (const change of changes) await t.test(change.name, async () => {
    const f = await createSMDurcharbeitFixture();
    try {
      await f.pg.query("insert into sm_smdurcharbeit_markets(sm_market_id,smdurcharbeit_verplanung) values($1,'Local SM')",[f.market]);
      const query = buildSMDurcharbeitSmLinkBackfill(await inventory(f)).query;
      const params = change.name === "changed canonical assignment" ? [f.admin,f.market]
        : change.name === "different saved link" ? [f.admin] : change.name === "inactive account" ? [f.employee]
        : change.name === "new matching account" ? [randomUUID()] : [];
      await f.pg.query(change.sql,params);
      const before = (await f.pg.query("select * from sm_smdurcharbeit_markets")).rows;
      await assert.rejects(apply(f,query),change.error);
      assert.deepEqual((await f.pg.query("select * from sm_smdurcharbeit_markets")).rows,before);
    } finally { await f.pg.close(); }
  });
  await t.test("unexpected UPDATE trigger source mutation",async () => {
    const f = await createSMDurcharbeitFixture();
    try {
      await f.pg.query("insert into sm_smdurcharbeit_markets(sm_market_id,smdurcharbeit_verplanung) values($1,'Local SM')",[f.market]);
      const query = buildSMDurcharbeitSmLinkBackfill(await inventory(f)).query;
      await f.pg.exec("create function synthetic_source_change() returns trigger language plpgsql as $$ begin new.smdurcharbeit_verplanung='Unwanted change'; return new; end $$; create trigger synthetic_source_change before update on sm_smdurcharbeit_markets for each row execute function synthetic_source_change();");
      await assert.rejects(apply(f,query),/source fields changed/);
      assert.deepEqual((await f.pg.query("select smdurcharbeit_verplanung,smdurcharbeit_sm_user_id from sm_smdurcharbeit_markets")).rows,[{smdurcharbeit_verplanung:"Local SM",smdurcharbeit_sm_user_id:null}]);
    } finally { await f.pg.close(); }
  });
});

test("SMDurcharbeit saved IDs supply campaign defaults; only a published current campaign exposes employee visits",async () => {
  class Clock extends Date { constructor(value?: string | number | Date) { super(value === undefined ? "2026-10-09T12:00:00Z" : value instanceof Date ? value.getTime() : value); } }
  const f = await createSMDurcharbeitFixture({clock:Clock as typeof Date});
  const admin=(method:"get"|"post",path:string)=>request(f.app)[method](path).auth("synthetic-sm-admin",{type:"bearer"});
  const sm=(method:"get"|"post"|"put",path:string)=>request(f.app)[method](path).auth("synthetic-sm",{type:"bearer"});
  const base="/admin/sm-smdurcharbeit-campaigns", targetPath="/sm/smdurcharbeit/targets?month=2026-10-01";
  try {
    await f.pg.query("insert into sm_smdurcharbeit_markets(sm_market_id,smdurcharbeit_verplanung) values($1,'SM Local')",[f.market]);
    await apply(f,buildSMDurcharbeitSmLinkBackfill(await inventory(f)).query);
    let options=(await admin("get",base+"/options").expect(200)).body;
    assert.equal(options.markets[0].assignedSmUserId,f.employee,"Registry ID works even when canonical assignment is NULL");
    assert.equal((await sm("get",targetPath).expect(200)).body.targets.length,0,"A saved account link does not create visits");
    await f.pg.query("update sm_markets set assigned_sm_user_id=$1 where id=$2",[f.admin,f.market]);
    options=(await admin("get",base+"/options").expect(200)).body;
    assert.equal(options.markets[0].assignedSmUserId,f.employee,"Saved Durcharbeit identity takes precedence in new campaign defaults");
    await f.pg.query("update sm_smdurcharbeit_markets set smdurcharbeit_sm_user_id=null where sm_market_id=$1",[f.market]);
    await f.pg.query("update sm_markets set assigned_sm_user_id=$1 where id=$2",[f.employee,f.market]);
    assert.equal((await admin("get",base+"/options").expect(200)).body.markets[0].assignedSmUserId,f.employee,"Existing imported canonical links remain supported");
    await f.pg.query("update sm_smdurcharbeit_markets set smdurcharbeit_sm_user_id=$1 where sm_market_id=$2",[f.employee,f.market]);
    const module=(await admin("post","/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({id:"new-"+randomUUID(),name:"Synthetic linked module",description:"",questions:[{id:"new-"+randomUUID(),text:"Synthetic answer",type:"yesno",required:true,options:["Ja","Nein"],config:{},rules:[]}]}).expect(201)).body.module;
    const form=(await admin("post","/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({id:"new-"+randomUUID(),name:"Synthetic linked form",description:"",status:"active",moduleIds:[module.id]}).expect(201)).body.questionnaire;
    const [version]=await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId,form.id));
    const campaign=(await admin("post",base).send({name:"Synthetic linked campaign",startDate:"2026-10-01",endDate:"2026-10-31",questionnaireVersionId:version!.id,rosterDraft:options.markets.map((m:any)=>({smMarketId:m.id,smUserId:m.assignedSmUserId}))}).expect(201)).body.campaign;
    assert.equal((await sm("get",targetPath).expect(200)).body.targets.length,0,"Draft campaigns remain invisible");
    const preview=(await admin("get",`${base}/${campaign.id}/preview`).expect(200)).body;
    await admin("post",`${base}/${campaign.id}/publish`).send({expectedRevision:campaign.revision,previewToken:preview.previewToken}).expect(200);
    const target=(await sm("get",targetPath).expect(200)).body.targets[0];
    assert.equal(target.available,true);
    const visit=(await sm("post",`/sm/smdurcharbeit/targets/${target.id}/start`).send({expectedRevision:target.revision,followUp:false,mode:"manual",clientSubmissionToken:randomUUID()}).expect(201)).body;
    const path=`/sm/smdurcharbeit/visits/${visit.visitId}`;
    const payload=(await sm("get",path).expect(200)).body;
    const question=payload.sections[0].questions[0];
    await sm("put",`${path}/answers/${question.id}`).send({answer:{kind:"choice",optionCode:question.options[0].code},expectedAnswerVersion:0,clientMutationToken:randomUUID()}).expect(200);
    await sm("post",path+"/submit").send({visitStartedAt:"2026-10-09T08:00:00Z",visitCompletedAt:"2026-10-09T08:15:00Z",clientMutationToken:randomUUID()}).expect(200);
    assert.equal((await sm("get",targetPath).expect(200)).body.targets[0].completed,true);
    const report=(await admin("get",`${base}/${campaign.id}/results?month=2026-10-01`).expect(200)).body;
    assert.equal(report.summary.latestSubmissions,1);
    assert.equal(report.questionResults[0].answered,1);
  } finally { await f.pg.close(); }
});
