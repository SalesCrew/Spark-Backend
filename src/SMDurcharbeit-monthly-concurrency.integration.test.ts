import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("native disposable PostgreSQL serializes monthly and legacy writers without duplicate work", { skip: !process.env.SMDURCHARBEIT_SYNTHETIC_PG_SOCKET_DIR, timeout: 30_000 }, async t => {
  class Clock extends Date { constructor(value?: string | number | Date) { super(value === undefined ? "2026-10-09T12:00:00Z" : value instanceof Date ? value.getTime() : value); } static now() { return Date.parse("2026-10-09T12:00:00Z"); } }
  const f = await createSMDurcharbeitFixture({ clock: Clock as typeof Date });
  assert.equal(f.nativePostgres, true, "Concurrent checks must run on native PostgreSQL, never PGlite");
  const admin = (method: "get" | "post" | "patch", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
  const sm = (method: "get" | "post", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" });
  try {
    const marketIds = [randomUUID(), randomUUID()];
    await f.database.insert(f.schema.smMarkets).values(marketIds.map((id,index) => ({ id, name: `Concurrent synthetic ${index}`, chain: "Spar", address: "Fixture only 1", postalCode: "1010", city: "Wien", region: "Ost", assignedSmUserId: f.employee })));
    await f.database.insert(f.schema.smSMDurcharbeitMarkets).values(marketIds.map(smMarketId => ({ smMarketId })));
    const module = (await admin("post", "/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Concurrent fixture", description: "", questions: [{ id: `new-${randomUUID()}`, type: "yesno", text: "Optional synthetic answer", required: false, options: ["Ja","Nein"], config: {}, rules: [] }] }).expect(201)).body.module;
    const form = (await admin("post", "/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Concurrent fixture", description: "", status: "active", moduleIds: [module.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    const campaign = (await admin("post", "/admin/sm-smdurcharbeit-campaigns").send({ name: "Concurrent fixture", startDate: "2026-10-01", endDate: "2026-12-31", questionnaireVersionId: version!.id, rosterDraft: marketIds.map(smMarketId => ({ smMarketId, smUserId: f.employee })) }).expect(201)).body.campaign;
    const preview = (await admin("get", `/admin/sm-smdurcharbeit-campaigns/${campaign.id}/preview`).expect(200)).body;
    const publication = { expectedRevision: campaign.revision, previewToken: preview.previewToken };
    await t.test("simultaneous publication yields one complete roster and one conflict", async () => {
      const responses = await Promise.all([admin("post", `/admin/sm-smdurcharbeit-campaigns/${campaign.id}/publish`).send(publication), admin("post", `/admin/sm-smdurcharbeit-campaigns/${campaign.id}/publish`).send(publication)]);
      assert.deepEqual(responses.map(r=>r.status).sort(), [200,409]);
      assert.equal((await f.database.select().from(f.schema.smSMDurcharbeitTargets)).length,6);
      assert.equal((await f.database.select().from(f.schema.smAssignments)).length,0);
    });
    const targets = (await sm("get", "/sm/smdurcharbeit/targets").expect(200)).body.targets;
    let first = "", second = "", winningVisit = "", originalSubmission = "";
    await t.test("twelve competing start tokens return one live draft and one immutable graph", async () => {
      const responses = await Promise.all(Array.from({length:12},()=>sm("post", `/sm/smdurcharbeit/targets/${targets[0].id}/start`).send({ expectedRevision:targets[0].revision, followUp:false, mode:"manual", clientSubmissionToken:randomUUID() })));
      assert.equal(responses.filter(r=>r.status===201).length,1);
      assert.ok(responses.every(r=>r.status===200||r.status===201), JSON.stringify(responses.map(r=>r.body)));
      assert.equal(new Set(responses.map(r=>r.body.visitId)).size,1); first=responses[0]!.body.visitId;
      const starts=await f.database.select().from(f.schema.smSMDurcharbeitVisits); assert.equal(starts.length,1);
      const submissions=await f.database.select().from(f.schema.smQuestionnaireSubmissions); assert.equal(submissions.length,1);
      second=(await sm("post", `/sm/smdurcharbeit/targets/${targets[1].id}/start`).send({ expectedRevision:targets[1].revision, followUp:false, mode:"manual", clientSubmissionToken:randomUUID() }).expect(201)).body.visitId;
    });
    const interval={visitStartedAt:"2026-10-09T08:00:00Z",visitCompletedAt:"2026-10-09T08:15:00Z"};
    await t.test("simultaneous overlapping target submissions persist only one person's interval", async () => {
      const responses=await Promise.all([first,second].map(id=>sm("post",`/sm/smdurcharbeit/visits/${id}/submit`).send({...interval,clientMutationToken:randomUUID()})));
      assert.deepEqual(responses.map(r=>r.status).sort(),[200,409]);
      winningVisit=[first,second][responses.findIndex(r=>r.status===200)]!;
      const times=await f.database.select().from(f.schema.smSMDurcharbeitTimeRevisions); assert.equal(times.length,1); assert.equal(times[0]!.visitId,winningVisit);
      const payload=(await sm("get",`/sm/smdurcharbeit/visits/${winningVisit}`).expect(200)).body; originalSubmission=payload.submission.id;
      await sm("post",`/sm/smdurcharbeit/visits/${winningVisit}/submit`).send({...interval,clientMutationToken:randomUUID()}).expect(200);
      assert.equal((await f.database.select().from(f.schema.smSMDurcharbeitTimeRevisions)).length,1);
    });
    await t.test("two simultaneous corrections preserve the frozen receipt and append only one current revision", async () => {
      const input={expectedVisitId:originalSubmission,expectedRevision:1,expectedStartedAt:interval.visitStartedAt,expectedCompletedAt:interval.visitCompletedAt,visitStartedAt:interval.visitStartedAt,reason:"Synthetic concurrent correction"};
      const responses=await Promise.all(["08:20","08:25"].map(end=>admin("patch",`/admin/sm-smdurcharbeit-times/${winningVisit}`).send({...input,visitCompletedAt:`2026-10-09T${end}:00Z`})));
      assert.deepEqual(responses.map(r=>r.status).sort(),[200,409]);
      const times=await f.database.select().from(f.schema.smSMDurcharbeitTimeRevisions); assert.equal(times.length,2); assert.equal(times.filter(r=>r.isCurrent).length,1);
      const [receipt]=await f.database.select().from(f.schema.smQuestionnaireSubmissions).where(eq(f.schema.smQuestionnaireSubmissions.id,originalSubmission));
      assert.equal(receipt!.visitCompletedAt!.toISOString(),new Date(interval.visitCompletedAt).toISOString());
    });
    await t.test("database constraints reject wrong author, month, latest/basis and rewritten identities", async () => {
      const [live]=await f.database.select().from(f.schema.smSMDurcharbeitVisits).where(eq(f.schema.smSMDurcharbeitVisits.id,winningVisit));
      const otherTarget=targets.find((row:any)=>row.id!==live!.targetId);
      const futureOwner=(await f.database.select().from(f.schema.smSMDurcharbeitOwnerRevisions)).find(row=>row.month==="2026-11-01");
      await assert.rejects(f.pg.query("insert into sm_smdurcharbeit_visits(target_id,owner_revision_id,sm_user_id,basis_revision) values($1,$2,$3,1)",[live!.targetId,live!.ownerRevisionId,f.admin]),/foreign key/);
      await assert.rejects(f.pg.query("insert into sm_smdurcharbeit_visits(target_id,owner_revision_id,sm_user_id,basis_revision) values($1,$2,$3,1)",[live!.targetId,futureOwner!.id,f.employee]),/context/);
      await assert.rejects(f.pg.query("insert into sm_smdurcharbeit_visits(target_id,owner_revision_id,sm_user_id,basis_revision,basis_submission_id) values($1,$2,$3,1,$4)",[otherTarget.id,otherTarget.ownerRevisionId??(await f.database.select().from(f.schema.smSMDurcharbeitTargets).where(eq(f.schema.smSMDurcharbeitTargets.id,otherTarget.id)))[0]!.ownerRevisionId,f.employee,originalSubmission]),/foreign key/);
      await assert.rejects(f.pg.query("update sm_smdurcharbeit_month_targets set latest_submission_id=$1 where id=$2",[originalSubmission,otherTarget.id]),/foreign key/);
      await assert.rejects(f.pg.query("update sm_smdurcharbeit_assignment_revisions set month='2026-11-01' where id=$1",[live!.ownerRevisionId]),/immutable/);
      await assert.rejects(f.pg.query("update sm_smdurcharbeit_visit_time_revisions set actual_minutes=99 where visit_id=$1 and is_current",[winningVisit]),/check constraint/);
      assert.equal((await f.database.select().from(f.schema.smSMDurcharbeitVisits)).length,2);
    });
    await t.test("a dated and monthly visit competing for one interval cannot both submit", async () => {
      const [assignment]=await f.database.insert(f.schema.smAssignments).values({idempotencyKey:randomUUID(),sourceType:"single",status:"planned",originalWorkDate:"2026-10-09",originalSmUserId:f.employee,originalSmMarketId:f.market,originalMarketInternalId:"SYNTHETIC-1",originalPlannedMinutes:15,SMDurcharbeitQuestionnaireOverrideVersionId:version!.id,createdByUserId:f.admin,updatedByUserId:f.admin}).returning();
      await sm("post",`/sm/visits/${assignment.id}/start`).send({mode:"manual",clientSubmissionToken:randomUUID()}).expect(200);
      const remaining=winningVisit===first?second:first;
      const timing={visitStartedAt:"2026-10-09T09:00:00Z",visitCompletedAt:"2026-10-09T09:15:00Z",actualMinutes:15,clientMutationToken:randomUUID()};
      const results=await Promise.all([sm("post",`/sm/visits/${assignment.id}/submit`).send(timing),sm("post",`/sm/smdurcharbeit/visits/${remaining}/submit`).send({...timing,clientMutationToken:randomUUID()})]);
      assert.deepEqual(results.map(r=>r.status).sort(),[200,409],JSON.stringify(results.map(r=>r.body)));
    });
    await t.test("all monthly tables deny browser reads and carry only intended server privileges", async () => {
      const rows=(await f.pg.query<{name:string;rls:boolean;server_read:boolean;server_delete:boolean}>(`select c.relname as name,c.relrowsecurity as rls,has_table_privilege('service_role',c.oid,'select') as server_read,has_table_privilege('service_role',c.oid,'delete') as server_delete from pg_class c where c.relkind='r' and c.relname like 'sm_smdurcharbeit_%'`)).rows;
      assert.equal(rows.length,12); assert.ok(rows.every(row=>row.rls&&row.server_read&&!row.server_delete));
      for(const role of ["anon","authenticated"]) { await f.pg.exec(`set role ${role}`); try { for(const row of rows) await assert.rejects(f.pg.query(`select * from "${row.name}" limit 1`),/permission denied/); } finally { await f.pg.exec("reset role"); } }
    });
    await t.test("starts racing waiver, reassignment and pause cannot create work after the winning transition", async () => {
      const other = randomUUID();
      await f.database.insert(f.schema.users).values({ id: other, role: "sm", firstName: "Other", lastName: "Synthetic", email: "race@preview.test", isActive: true });
      for (const action of ["waive", "reassign", "pause"] as const) {
        const campaign = (await admin("post", "/admin/sm-smdurcharbeit-campaigns").send({ name: `Synthetic race ${action}`, startDate: "2026-10-01", endDate: "2026-10-31", questionnaireVersionId: version!.id, rosterDraft: [{ smMarketId: marketIds[0], smUserId: f.employee }] }).expect(201)).body.campaign;
        const preview = (await admin("get", `/admin/sm-smdurcharbeit-campaigns/${campaign.id}/preview`).expect(200)).body;
        await admin("post", `/admin/sm-smdurcharbeit-campaigns/${campaign.id}/publish`).send({ expectedRevision: campaign.revision, previewToken: preview.previewToken, confirmOverlap: true }).expect(200);
        const target = (await admin("get", `/admin/sm-smdurcharbeit-campaigns/${campaign.id}/targets`).expect(200)).body.targets[0];
        const change = action === "pause" ? admin("patch", `/admin/sm-smdurcharbeit-campaigns/${campaign.id}/state`).send({ expectedRevision: campaign.revision + 1, status: "paused", reason: "Synthetic race pause" })
          : admin("patch", `/admin/sm-smdurcharbeit-campaigns/targets/${target.id}`).send({ expectedRevision: target.revision, scope: "month", reason: "Synthetic race roster", ...(action === "waive" ? { eligibility: "waived" } : { smUserId: other }) });
        const [startResult, changeResult] = await Promise.all([
          sm("post", `/sm/smdurcharbeit/targets/${target.id}/start`).send({ expectedRevision: target.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }), change,
        ]);
        assert.ok([201, 403, 409].includes(startResult.status), JSON.stringify(startResult.body));
        assert.ok([200, 409].includes(changeResult.status), JSON.stringify(changeResult.body));
        const graph = await f.database.select().from(f.schema.smQuestionnaireSubmissions).where(eq(f.schema.smQuestionnaireSubmissions.SMDurcharbeitTargetId, target.id));
        if (action === "pause") {
          assert.equal(changeResult.status, 200); assert.equal(graph.length, startResult.status === 201 ? 1 : 0);
          if (startResult.status === 201) {
            const read = (await sm("get", `/sm/smdurcharbeit/visits/${startResult.body.visitId}`).expect(200)).body;
            assert.ok(read.SMDurcharbeitContext.readOnlyReason);
            await sm("post", `/sm/smdurcharbeit/visits/${startResult.body.visitId}/submit`).send({ visitStartedAt: "2026-10-09T10:00:00Z", visitCompletedAt: "2026-10-09T10:10:00Z", clientMutationToken: randomUUID() }).expect(409);
          }
        } else {
          assert.notEqual(startResult.status === 201 && changeResult.status === 200, true, "A roster transition cannot bypass the live-draft guard");
          assert.equal(graph.length, startResult.status === 201 ? 1 : 0);
          if (startResult.status === 201) assert.equal(changeResult.status, 409);
          else assert.equal(changeResult.status, 200);
        }
      }
    });
  } finally { await f.pg.close(); }
});
