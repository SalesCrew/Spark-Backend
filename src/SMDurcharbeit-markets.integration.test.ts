import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("SMDurcharbeit dedicated markets use isolated identities, authoritative selection and unchanged history", async t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-10-07T15:00:00Z') });
  const f = await createSMDurcharbeitFixture();
  const admin = (method: "get" | "post" | "patch" | "put", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" }).timeout({ response: 5000, deadline: 8000 });
  const sm = (method: "get" | "post" | "put", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" }).timeout({ response: 5000, deadline: 8000 });
  const form = async (scope: "standard" | "SMDurcharbeit") => {
    const module = (await admin("post", `/admin/sm-questionnaires/modules?scope=${scope}`).send({ id: "new-" + randomUUID(), name: scope, description: "", questions: [{ id: "new-" + randomUUID(), text: "Synthetische Kontrolle", type: "yesno", required: true, options: ["Ja", "Nein"], config: {}, rules: [] }] }).expect(201)).body.module;
    const questionnaire = (await admin("post", `/admin/sm-questionnaires/questionnaires?scope=${scope}`).send({ id: "new-" + randomUUID(), name: scope, description: "", status: "active", moduleIds: [module.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, questionnaire.id));
    return { questionnaire, version: version! };
  };
  const complete = async (id: string, hour: number) => {
    const payload = (await sm("post", `/sm/visits/${id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }).expect(200)).body;
    const q = payload.sections[0].questions[0];
    await sm("put", `/sm/visits/${id}/answers/${q.id}`).send({ answer: { kind: "choice", optionCode: q.options[0].code }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
    await sm("post", `/sm/visits/${id}/submit`).send({ actualMinutes: 15, visitStartedAt: `2026-10-07T${hour}:00:00Z`, visitCompletedAt: `2026-10-07T${hour}:15:00Z`, clientMutationToken: randomUUID() }).expect(200);
    return payload;
  };
  try {
    const standard = await form("standard"), durcharbeit = await form("SMDurcharbeit");
    await admin("put", "/admin/sm-planning/questionnaire-assignment").send({ questionnaireTemplateId: standard.questionnaire.id }).expect(200);
    const history = await f.assignment();
    await complete(history.id, 10);
    const historicalTables = ["sm_markets", "sm_assignments", "sm_questionnaire_submissions", "sm_question_answers", "sm_assignment_events", "sm_assignment_time_submissions"];
    const capture = async () => Object.fromEntries(await Promise.all(historicalTables.map(async name => [name, JSON.stringify((await f.pg.query(`select * from ${name} order by id`)).rows)])));
    const original = await capture();
    await t.test("additive registry migration preserves old rows and denies direct browser writes", async () => {
      // This historical migration replay precedes campaign data in the disposable fixture.
      // Detach and restore its empty dependent FK so the original registry can be replayed.
      await f.pg.exec("alter table sm_smdurcharbeit_campaign_markets drop constraint sm_smdurcharbeit_campaign_markets_sm_market_id_fkey");
      await f.pg.exec("drop table sm_smdurcharbeit_markets");
      await f.pg.exec(await readFile(new URL("../supabase/migrations/20261007133647_SMDurcharbeit_market_registry.sql", import.meta.url), "utf8"));
      await f.pg.exec(await readFile(new URL("../supabase/migrations/20261008125753_SMDurcharbeit_market_import.sql", import.meta.url), "utf8"));
      await f.pg.exec("alter table sm_smdurcharbeit_campaign_markets add constraint sm_smdurcharbeit_campaign_markets_sm_market_id_fkey foreign key(sm_market_id) references sm_smdurcharbeit_markets(sm_market_id) on delete restrict");
      assert.deepEqual(await capture(), original);
      const result = await f.pg.query<{ relrowsecurity: boolean }>("select relrowsecurity from pg_class where relname='sm_smdurcharbeit_markets'");
      assert.equal(result.rows[0]?.relrowsecurity, true);
      await f.pg.exec("set role authenticated");
      try { await assert.rejects(f.pg.query("insert into sm_smdurcharbeit_markets(sm_market_id) values($1)", [f.market]), /permission denied/); }
      finally { await f.pg.exec("reset role"); }
    });
    const special = randomUUID();
    await f.database.insert(f.schema.smMarkets).values({ id: special, internalMarketId: "SYNTHETIC-DA", name: "Dedicated Durcharbeit", chain: "Spar", address: "Test 2", postalCode: "1020", city: "Wien", region: "Ost" });
    await f.database.insert(f.schema.smSMDurcharbeitMarkets).values({ smMarketId: special });
    const create = (version?: string) => admin("post", "/admin/sm-planning/assignments").send({ smMarketId: special, smUserId: f.employee, workDate: "2026-10-07", plannedMinutes: 45, idempotencyKey: randomUUID(), ...(version ? { SMDurcharbeitQuestionnaireOverrideVersionId: version } : {}) });
    await t.test("directory scopes separate dedicated markets while all preserves stable IDs and admin authorization", async () => {
      const normal = (await admin("get", "/admin/sm-markets").expect(200)).body.markets;
      assert.deepEqual(normal.map((r: any) => r.id), [f.market]);
      const scoped = (await admin("get", "/admin/sm-markets?SMDurcharbeitMarketScope=SMDurcharbeit").expect(200)).body.markets;
      assert.equal(scoped[0].id, special); assert.equal(scoped[0].SMDurcharbeitMarket, true);
      const all = (await admin("get", "/admin/sm-markets?SMDurcharbeitMarketScope=all").expect(200)).body.markets;
      assert.equal(all.length, 2); assert.equal(all.find((r: any) => r.id === f.market).SMDurcharbeitMarket, false);
      await sm("get", "/admin/sm-markets?SMDurcharbeitMarketScope=SMDurcharbeit").expect(403);
      await admin("get", "/admin/sm-markets?SMDurcharbeitMarketScope=invalid").expect(400);
      await assert.rejects(f.pg.query("insert into sm_smdurcharbeit_markets(sm_market_id) values($1)", [randomUUID()]), /foreign key/);
    });
    await t.test("missing, standard and unavailable questionnaires cannot create partial assignments or audit rows", async () => {
      const before = JSON.stringify((await f.pg.query("select * from sm_assignments order by id")).rows);
      const events = JSON.stringify((await f.pg.query("select * from sm_assignment_events order by id")).rows);
      await create().expect(409); await create(standard.version.id).expect(409); await create(randomUUID()).expect(409);
      assert.equal(JSON.stringify((await f.pg.query("select * from sm_assignments order by id")).rows), before);
      assert.equal(JSON.stringify((await f.pg.query("select * from sm_assignment_events order by id")).rows), events);
    });
    await t.test("holiday adjustment validates the final date and leaves no partial assignment", async () => {
      const [bounded] = await f.database.insert(f.schema.smQuestionnaireVersions).values({ ...durcharbeit.version, id: randomUUID(), versionNumber: 99, status: "draft", publishedAt: null, publishedByUserId: null, contentHash: null, effectiveTo: "2026-10-26" }).returning();
      const links = await f.database.select().from(f.schema.smQuestionnaireVersionModules).where(eq(f.schema.smQuestionnaireVersionModules.questionnaireVersionId, durcharbeit.version.id));
      await f.database.insert(f.schema.smQuestionnaireVersionModules).values(links.map(row => ({ ...row, id: randomUUID(), questionnaireVersionId: bounded!.id })));
      await f.database.update(f.schema.smQuestionnaireVersions).set({ status: "published", publishedAt: new Date(), publishedByUserId: f.admin, contentHash: "synthetic-bounded" }).where(eq(f.schema.smQuestionnaireVersions.id, bounded!.id));
      const before = JSON.stringify((await f.pg.query("select * from sm_assignments order by id")).rows);
      const events = JSON.stringify((await f.pg.query("select * from sm_assignment_events order by id")).rows);
      await admin("post", "/admin/sm-planning/assignments").send({ smMarketId: special, smUserId: f.employee, workDate: "2026-10-26", plannedMinutes: 45, idempotencyKey: randomUUID(), SMDurcharbeitQuestionnaireOverrideVersionId: bounded!.id }).expect(409);
      assert.equal(JSON.stringify((await f.pg.query("select * from sm_assignments order by id")).rows), before);
      assert.equal(JSON.stringify((await f.pg.query("select * from sm_assignment_events order by id")).rows), events);
    });
    let plannedId = "";
    await t.test("explicit published Durcharbeit creates one pinned Einsatz and employee/admin views agree", async () => {
      const key = randomUUID(), input = { smMarketId: special, smUserId: f.employee, workDate: "2026-10-07", plannedMinutes: 45, idempotencyKey: key, SMDurcharbeitQuestionnaireOverrideVersionId: durcharbeit.version.id };
      plannedId = (await admin("post", "/admin/sm-planning/assignments").send(input).expect(201)).body.assignmentId;
      assert.equal((await admin("post", "/admin/sm-planning/assignments").send(input).expect(200)).body.assignmentId, plannedId);
      for (const endpoint of ["/admin/sm-planning/assignments?from=2026-10-07&to=2026-10-07", "/sm/planning/assignments?from=2026-10-07&to=2026-10-07"]) {
        const rows = (await (endpoint.startsWith("/admin") ? admin("get", endpoint) : sm("get", endpoint)).expect(200)).body.assignments;
        const row = rows.find((r: any) => r.id === plannedId);
        assert.equal(row.SMDurcharbeitMarket, true); assert.equal(row.SMDurcharbeitQuestionnaireSelection.catalogScope, "SMDurcharbeit");
        assert.equal(row.SMDurcharbeitQuestionnaireSelection.questionnaireVersionId, durcharbeit.version.id);
      }
    });
    await t.test("reset and wrong-type edits roll back; standard directory cannot edit/delete dedicated markets", async () => {
      const [row] = await f.database.select().from(f.schema.smAssignments).where(eq(f.schema.smAssignments.id, plannedId));
      for (const value of [null, standard.version.id]) await admin("patch", `/admin/sm-planning/assignments/${plannedId}`).send({ expectedUpdatedAt: row!.updatedAt.toISOString(), plannedMinutes: 90, SMDurcharbeitQuestionnaireOverrideVersionId: value }).expect(409);
      const [after] = await f.database.select().from(f.schema.smAssignments).where(eq(f.schema.smAssignments.id, plannedId));
      assert.deepEqual(after, row);
      await admin("patch", `/admin/sm-markets/${special}`).send({}).expect(409);
      await request(f.app).delete(`/admin/sm-markets/${special}`).auth("synthetic-sm-admin", { type: "bearer" }).expect(409);
      const sync = await admin("post", "/admin/sm-markets/sync-sm-users/manual").send({ marketIds: [special], smUserId: f.employee }).expect(200);
      assert.equal(sync.body.matched.length, 0);
    });
    await t.test("recurring defaults cannot bypass dedicated-market selection in create or series changes", async () => {
      const input = { smMarketId: special, smUserId: f.employee, plannedMinutes: 15, frequency: "weekly", weekdays: [3], validFrom: "2026-10-07", validTo: "2026-10-28", idempotencyKey: randomUUID() };
      await admin("post", "/admin/sm-planning/series").send(input).expect(409);
      const series = (await admin("post", "/admin/sm-planning/series").send({ ...input, smMarketId: f.market, idempotencyKey: randomUUID() }).expect(201)).body;
      await admin("post", `/admin/sm-planning/series/${series.seriesId}/preview`).send({ action: "edit", effectiveFromDate: "2026-10-07", smMarketId: special, smUserId: f.employee, plannedMinutes: 15, frequency: "weekly", weekdays: [3], validTo: "2026-10-28" }).expect(409);
    });
    await t.test("start, answers, submit and reporting keep the dedicated visit graph and frozen type", async () => {
      const payload = await complete(plannedId, 11);
      assert.equal(payload.submission.questionnaireName, durcharbeit.questionnaire.name);
      const row = (await admin("get", "/admin/sm-planning/assignments?from=2026-10-07&to=2026-10-07").expect(200)).body.assignments.find((r: any) => r.id === plannedId);
      assert.equal(row.status, "completed"); assert.equal(row.SMDurcharbeitQuestionnaireSelection.source, "submission");
      assert.equal(row.SMDurcharbeitQuestionnaireSelection.catalogScope, "SMDurcharbeit");
      await admin("patch", `/admin/sm-planning/assignments/${plannedId}`).send({ expectedUpdatedAt: row.updatedAt, SMDurcharbeitQuestionnaireOverrideVersionId: standard.version.id }).expect(409);
      const report = await admin("get", "/admin/sm-dashboard?from=2026-10-07&to=2026-10-07").expect(200);
      assert.equal(report.body.SMDurcharbeitBreakdown.SMDurcharbeit.completedVisits, 1);
    });
    await t.test("original completed answers, time and audit stay byte-for-byte unchanged", async () => {
      for (const name of historicalTables.filter(name => name !== "sm_markets")) {
        const beforeRows = JSON.parse(original[name]) as any[];
        const afterRows = JSON.parse(JSON.stringify((await f.pg.query(`select * from ${name}`)).rows)) as any[];
        for (const before of beforeRows) assert.deepEqual(afterRows.find(row => row.id === before.id), before, `${name} ${before.id}`);
      }
      const result = await f.pg.query("select * from sm_markets where id=$1", [f.market]);
      assert.deepEqual(JSON.parse(JSON.stringify(result.rows[0])), JSON.parse(original.sm_markets)[0], "original market unchanged");
    });
  } finally { await f.pg.close(); }
});
