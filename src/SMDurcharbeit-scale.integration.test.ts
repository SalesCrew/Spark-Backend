import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("native maximum roster publishes and extends atomically with complete monthly totals", { skip: !process.env.SMDURCHARBEIT_SYNTHETIC_PG_SOCKET_DIR, timeout: 120_000 }, async () => {
  const f = await createSMDurcharbeitFixture();
  const admin = (method: "get" | "post", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
  try {
    const markets = Array.from({ length: 5000 }, (_, index) => ({ id: randomUUID(), name: `Scale ${String(index).padStart(4, "0")}`, chain: "Spar", address: `Synthetic ${index}`, postalCode: "1010", city: "Wien", region: "Ost", assignedSmUserId: f.employee }));
    for (let offset = 0; offset < markets.length; offset += 1000) {
      await f.database.insert(f.schema.smMarkets).values(markets.slice(offset, offset + 1000));
      await f.database.insert(f.schema.smSMDurcharbeitMarkets).values(markets.slice(offset, offset + 1000).map(row => ({ smMarketId: row.id })));
    }
    const module = (await admin("post", "/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Synthetic scale", description: "", questions: [{ id: `new-${randomUUID()}`, type: "yesno", text: "Synthetic only", required: false, options: ["Ja", "Nein"], config: {}, rules: [] }] }).expect(201)).body.module;
    const form = (await admin("post", "/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Synthetic scale", description: "", status: "active", moduleIds: [module.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    const input = { name: "Maximum synthetic roster", startDate: "2026-10-01", endDate: "2026-12-31", questionnaireVersionId: version!.id, rosterDraft: markets.map(row => ({ smMarketId: row.id, smUserId: f.employee })) };
    await admin("post", "/admin/sm-smdurcharbeit-campaigns").send({ ...input, rosterDraft: [...input.rosterDraft, input.rosterDraft[0]] }).expect(400);
    await admin("post", "/admin/sm-smdurcharbeit-campaigns").send({ ...input, endDate: "2028-10-31" }).expect(400);
    const campaign = (await admin("post", "/admin/sm-smdurcharbeit-campaigns").send(input).expect(201)).body.campaign;
    const base = `/admin/sm-smdurcharbeit-campaigns/${campaign.id}`;
    const preview = (await admin("get", `${base}/preview`).expect(200)).body;
    assert.equal(preview.targetCount, 15_000);
    await admin("post", `${base}/publish`).send({ expectedRevision: 1, previewToken: preview.previewToken }).expect(200);
    const initial = (await admin("get", `${base}/targets?month=2026-10-01`).expect(200)).body;
    assert.equal(initial.targets.length, 5000); assert.deepEqual(initial.summary, { required: 5000, completed: 0, waived: 0, physicalVisits: 0 });
    const ids = initial.targets.map((row: any) => row.id);
    await admin("post", `${base}/extend`).send({ expectedRevision: 2, endDate: "2028-09-30", reason: "Synthetic maximum window", reactivate: false }).expect(200);
    const totals = (await f.pg.query<{ targets: number; periods: number; owners: number }>("select (select count(*)::int from sm_smdurcharbeit_month_targets) as targets,(select count(*)::int from sm_smdurcharbeit_campaign_periods) as periods,(select count(*)::int from sm_smdurcharbeit_assignment_revisions) as owners")).rows[0];
    assert.deepEqual(totals, { targets: 120_000, periods: 24, owners: 120_000 });
    assert.deepEqual((await admin("get", `${base}/targets?month=2026-10-01`).expect(200)).body.targets.map((row: any) => row.id), ids, "Extension preserves old targets");
    const last = (await admin("get", `${base}/targets?month=2028-09-01`).expect(200)).body;
    assert.equal(last.targets.length, 5000); assert.ok(last.targets.every((row: any) => !row.available && !row.completed && !row.draftVisitId && row.smUserId === f.employee));
    assert.equal((await f.database.select().from(f.schema.smAssignments)).length, 0);
    assert.equal((await f.database.select().from(f.schema.smQuestionnaireSubmissions)).length, 0);
  } catch (error) { throw new Error(error instanceof Error && "cause" in error && error.cause instanceof Error ? error.cause.message : error instanceof Error ? error.message : String(error)); }
  finally { await f.pg.close(); }
});
