import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("monthly transitions preserve ownership, pinned versions and historical state", async t => {
  let now = Date.parse("2026-10-09T12:00:00Z");
  class Clock extends Date { constructor(value?: string | number | Date) { super(value === undefined ? now : value instanceof Date ? value.getTime() : value); } static now() { return now; } }
  const second = randomUUID();
  const f = await createSMDurcharbeitFixture({ clock: Clock as typeof Date, additionalSmUsers: [{ id: second, token: "synthetic-second-sm" }] });
  const admin = (method: "get" | "post" | "patch", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
  const sm = (method: "get" | "post", path: string, token = "synthetic-sm") => request(f.app)[method](path).auth(token, { type: "bearer" });
  const root = "/admin/sm-smdurcharbeit-campaigns", employeeRoot = "/sm/smdurcharbeit";
  const versionFor = async (name: string) => {
    const module = (await admin("post", "/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name, description: "", questions: [{ id: `new-${randomUUID()}`, type: "yesno", text: name, required: false, options: ["Ja", "Nein"], config: {}, rules: [] }] }).expect(201)).body.module;
    const form = (await admin("post", "/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name, description: "", status: "active", moduleIds: [module.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    return { form, version: version! };
  };
  const create = async (version: string, extra: Record<string, unknown> = {}) => (await admin("post", root).send({ name: "Synthetic transitions", startDate: "2026-10-01", endDate: "2026-12-31", questionnaireVersionId: version, rosterDraft: [{ smMarketId: f.market, smUserId: f.employee }], ...extra }).expect(201)).body.campaign;
  const campaign = async (id: string) => (await admin("get", root).expect(200)).body.campaigns.find((row: any) => row.id === id);
  const target = async (id: string, month = "2026-10-01") => (await admin("get", `${root}/${id}/targets?month=${month}`).expect(200)).body.targets[0];
  const start = (row: any, token = "synthetic-sm") => sm("post", `${employeeRoot}/targets/${row.id}/start`, token).send({ expectedRevision: row.revision, followUp: row.completed, mode: "manual", clientSubmissionToken: randomUUID() });
  const publish = async (id: string, expectedStatus: number, confirmOverlap = false) => { const p = (await admin("get", `${root}/${id}/preview`).expect(200)).body; return admin("post", `${root}/${id}/publish`).send({ expectedRevision: p.campaign.revision, previewToken: p.previewToken, confirmOverlap }).expect(expectedStatus); };
  try {
    await f.database.insert(f.schema.smSMDurcharbeitMarkets).values({ smMarketId: f.market, SMDurcharbeitVerplanung: "Local SM" });
    const original = await versionFor("Original synthetic version"), replacement = await versionFor("Replacement synthetic version");
    const c = await create(original.version.id);
    await t.test("drafts stay private and unresolved owners cannot publish", async () => {
      assert.equal((await sm("get", `${employeeRoot}/targets`).expect(200)).body.targets.length, 0);
      const unresolved = await create(original.version.id, { rosterDraft: [{ smMarketId: f.market, smUserId: null }] });
      const p = (await admin("get", `${root}/${unresolved.id}/preview`).expect(200)).body;
      assert.deepEqual(p.unresolved, [f.market]); await publish(unresolved.id, 409);
      assert.equal((await f.database.select().from(f.schema.smSMDurcharbeitPeriods)).length, 0);
    });
    await t.test("a roster change after preview requires a fresh preview", async () => {
      const p = (await admin("get", `${root}/${c.id}/preview`).expect(200)).body;
      await f.database.update(f.schema.smMarkets).set({ name: "Renamed before publication" }).where(eq(f.schema.smMarkets.id, f.market));
      await admin("post", `${root}/${c.id}/publish`).send({ expectedRevision: c.revision, previewToken: p.previewToken }).expect(409);
      assert.equal((await f.database.select().from(f.schema.smSMDurcharbeitTargets)).length, 0);
      await publish(c.id, 200);
    });
    await t.test("an independent overlapping campaign needs explicit confirmation", async () => {
      const other = await create(original.version.id); await publish(other.id, 409);
      await publish(other.id, 200, true);
      const row = await campaign(other.id); await admin("patch", `${root}/${other.id}/state`).send({ expectedRevision: row.revision, status: "archived", reason: "Synthetic overlap retained in history" }).expect(200);
    });
    await t.test("current versions and catalog deactivation remain protected; only future months can change", async () => {
      await admin("patch", `/admin/sm-questionnaires/questionnaires/${original.form.id}/delete?scope=SMDurcharbeit`).expect(409);
      await admin("patch", `/admin/sm-questionnaires/questionnaires/${original.form.id}?scope=SMDurcharbeit`).send({ ...original.form, status: "inactive" }).expect(409);
      const periods = (await admin("get", `${root}/${c.id}/periods`).expect(200)).body.periods;
      const input = { expectedRevision: (await campaign(c.id)).revision, questionnaireVersionId: replacement.version.id, reason: "Synthetic future version" };
      await admin("patch", `${root}/${c.id}/periods/${periods[0].id}/questionnaire`).send(input).expect(409);
      await admin("patch", `${root}/${c.id}/periods/${periods[1].id}/questionnaire`).send(input).expect(200);
      await admin("patch", `${root}/${c.id}/periods/${periods[2].id}/questionnaire`).send(input).expect(409);
      const after = (await admin("get", `${root}/${c.id}/periods`).expect(200)).body.periods;
      assert.deepEqual(after.map((row: any) => row.questionnaireVersionId), [original.version.id, replacement.version.id, original.version.id]);
    });
    await t.test("registry changes never transfer campaign ownership or change its market snapshot", async () => {
      const before = await target(c.id);
      await f.database.update(f.schema.smMarkets).set({ assignedSmUserId: second, name: "Registry metadata changed" }).where(eq(f.schema.smMarkets.id, f.market));
      const after = await target(c.id); assert.equal(after.smUserId, f.employee); assert.deepEqual(after.market, before.market);
      await admin("patch", `/admin/sm-markets/${f.market}`).send({ name: "Wrong normal catalog" }).expect(409);
      await request(f.app).delete(`/admin/sm-markets/${f.market}`).auth("synthetic-sm-admin", { type: "bearer" }).expect(409);
    });
    await t.test("inactive owner or market blocks mutation without deleting the obligation", async () => {
      const before = await target(c.id);
      await f.database.update(f.schema.users).set({ isActive: false }).where(eq(f.schema.users.id, f.employee));
      assert.equal((await target(c.id)).available, false); await start(before).expect(403);
      await f.database.update(f.schema.users).set({ isActive: true }).where(eq(f.schema.users.id, f.employee));
      await f.database.update(f.schema.smMarkets).set({ isActive: false }).where(eq(f.schema.smMarkets.id, f.market));
      assert.equal((await target(c.id)).available, false); await start(before).expect(409);
      await f.database.update(f.schema.smMarkets).set({ isActive: true }).where(eq(f.schema.smMarkets.id, f.market));
      assert.equal((await target(c.id)).revision, before.revision); assert.equal((await target(c.id)).available, true);
    });
    await t.test("waive, reopen and future reassignment are audited and do not rewrite older targets", async () => {
      const october = await target(c.id); await admin("patch", `${root}/targets/${october.id}`).send({ expectedRevision: october.revision, eligibility: "waived", scope: "month", reason: "Synthetic exception" }).expect(200);
      const waived = await target(c.id); assert.equal(waived.eligibility, "waived"); await start(waived).expect(409);
      await admin("patch", `${root}/targets/${waived.id}`).send({ expectedRevision: waived.revision, eligibility: "required", scope: "month", reason: "Synthetic reopening" }).expect(200);
      const preserved = await target(c.id), november = await target(c.id, "2026-11-01");
      await admin("patch", `${root}/targets/${november.id}`).send({ expectedRevision: november.revision, smUserId: second, scope: "future", reason: "Synthetic future assignment" }).expect(200);
      assert.deepEqual(await target(c.id), preserved);
      assert.equal((await target(c.id, "2026-11-01")).smUserId, second); assert.equal((await target(c.id, "2026-12-01")).smUserId, second);
    });
    await t.test("live drafts prevent transfer; pause preserves the graph and explicit resume uses the same visit", async () => {
      const row = await target(c.id), started = (await start(row).expect(201)).body;
      const afterStart = await target(c.id);
      await admin("patch", `${root}/targets/${row.id}`).send({ expectedRevision: afterStart.revision, smUserId: second, scope: "month", reason: "Synthetic protected transfer" }).expect(409);
      const current = await campaign(c.id); await admin("patch", `${root}/${c.id}/state`).send({ expectedRevision: current.revision, status: "paused", reason: "Synthetic pause" }).expect(200);
      const paused = (await sm("get", `${employeeRoot}/visits/${started.visitId}`).expect(200)).body;
      assert.ok(paused.SMDurcharbeitContext.readOnlyReason); await start(await target(c.id)).expect(409);
      const p = await campaign(c.id); await admin("patch", `${root}/${c.id}/state`).send({ expectedRevision: p.revision, status: "published", reason: "Synthetic resumption" }).expect(200);
      assert.equal((await start(await target(c.id)).expect(200)).body.visitId, started.visitId);
    });
    await t.test("Vienna month boundary selects the new version, leaves new answers empty and closes the old draft", async () => {
      now = Date.parse("2026-10-31T23:05:00Z");
      const list = (await sm("get", `${employeeRoot}/targets`, "synthetic-second-sm").expect(200)).body; assert.equal(list.currentMonth, "2026-11-01");
      const november = list.targets.find((row: any) => row.campaignId === c.id);
      const started = (await start(november, "synthetic-second-sm").expect(201)).body;
      const graph = (await sm("get", `${employeeRoot}/visits/${started.visitId}`, "synthetic-second-sm").expect(200)).body;
      assert.equal(graph.submission.questionnaireName, replacement.form.name); assert.ok(Object.values(graph.answers).every(answer => answer === null));
      await start(await target(c.id)).expect(409);
      await admin("patch", `${root}/targets/${(await target(c.id)).id}`).send({ expectedRevision: (await target(c.id)).revision, eligibility: "waived", scope: "month", reason: "Cannot rewrite last month" }).expect(409);
      const events = (await admin("get", `${root}/${c.id}/history`).expect(200)).body.events;
      assert.ok(events.some((row: any) => row.action === "future_questionnaire_changed")); assert.ok(events.some((row: any) => row.action === "roster_changed"));
    });
    await t.test("audit history paginates tied timestamps without duplicates or GET writes", async () => {
      await f.database.insert(f.schema.smSMDurcharbeitEvents).values(Array.from({ length: 137 }, () => ({ campaignId: c.id, actorUserId: f.admin, action: "synthetic_pagination", reason: "Synthetic audit only", createdAt: new Date("2026-11-01T10:00:00Z") })));
      const before = await f.database.select().from(f.schema.smSMDurcharbeitEvents);
      const expected = before.filter(row => row.campaignId === c.id).map(row => row.id).sort(), found: string[] = [];
      let query = "";
      do {
        const page = (await admin("get", `${root}/${c.id}/history${query}`).expect(200)).body;
        assert.ok(page.events.length <= 50); assert.ok(page.events.every((row: any) => row.actorName));
        found.push(...page.events.map((row: any) => row.id));
        query = page.nextCursor ? `?${new URLSearchParams(page.nextCursor)}` : "";
      } while (query);
      assert.deepEqual(found.sort(), expected); assert.equal(new Set(found).size, found.length);
      assert.deepEqual(await f.database.select().from(f.schema.smSMDurcharbeitEvents), before);
      await admin("get", `${root}/${c.id}/history?beforeId=${randomUUID()}`).expect(400);
      await sm("get", `${root}/${c.id}/history`).expect(403);
    });
  } finally { await f.pg.close(); }
});
