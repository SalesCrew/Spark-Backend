import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("registry SM IDs plan selected markets automatically and pin actual visit history", async t => {
  class Clock extends Date { constructor(value?: string | number | Date) { super(value === undefined ? "2026-10-13T12:00:00Z" : value instanceof Date ? value.getTime() : value); } static now() { return Date.parse("2026-10-13T12:00:00Z"); } }
  const second = randomUUID(), objects = new Set<string>();
  const f = await createSMDurcharbeitFixture({ clock: Clock as typeof Date, additionalSmUsers: [{ id: second, token: "synthetic-second-sm" }], photoStorage: { from: () => ({
    createSignedUploadUrl: async (path: string) => ({ data: { path, signedUrl: `https://synthetic.invalid/${path}`, token: "synthetic" }, error: null }),
    list: async (folder: string, options: { search: string }) => ({ data: objects.has(`${folder}/${options.search}`) ? [{ name: options.search }] : [], error: null }),
    createSignedUrl: async (path: string) => ({ data: { signedUrl: `https://synthetic.invalid/${path}` }, error: null }),
    remove: async () => { throw new Error("Automatic planning must never remove photos"); },
  }) } });
  const root = "/admin/sm-smdurcharbeit-campaigns", employeeRoot = "/sm/smdurcharbeit";
  const admin = (method: "get" | "post" | "patch", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
  const sm = (method: "get" | "post" | "put", path: string, token = "synthetic-sm") => request(f.app)[method](path).auth(token, { type: "bearer" });
  const snapshot = async (tables: string[]) => Object.fromEntries(await Promise.all(tables.map(async table => [table, (await f.pg.query(`select * from ${table} order by id`)).rows])));
  const stateTables = ["sm_smdurcharbeit_campaigns", "sm_smdurcharbeit_campaign_markets", "sm_smdurcharbeit_month_targets", "sm_smdurcharbeit_assignment_revisions", "sm_smdurcharbeit_events", "sm_smdurcharbeit_visits", "sm_questionnaire_submissions", "sm_question_answers", "sm_question_answer_files", "sm_smdurcharbeit_visit_time_revisions"];
  try {
    const unselected = randomUUID();
    await f.database.insert(f.schema.smMarkets).values({ id: unselected, name: "Unselected synthetic SPAR", chain: "Spar", address: "Fixture only 2", postalCode: "1010", city: "Wien", region: "Ost", assignedSmUserId: f.employee });
    await f.database.insert(f.schema.smSMDurcharbeitMarkets).values([{ smMarketId: f.market, SMDurcharbeitSmUserId: f.employee, SMDurcharbeitVerplanung: "Local SM" }, { smMarketId: unselected, SMDurcharbeitSmUserId: second, SMDurcharbeitVerplanung: "Second SM" }]);
    const module = (await admin("post", "/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Automatic synthetic", description: "", questions: [
      { id: `new-${randomUUID()}`, type: "yesno", text: "Synthetic completion", required: true, options: ["Ja", "Nein"], config: {}, rules: [] },
      { id: `new-${randomUUID()}`, type: "photo", text: "Synthetic photo", required: false, options: [], config: {}, rules: [] },
    ] }).expect(201)).body.module;
    const form = (await admin("post", "/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Automatic synthetic", description: "", status: "active", moduleIds: [module.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    const create = async (rosterDraft: unknown[]) => (await admin("post", root).send({ name: "Automatic synthetic", startDate: "2026-09-01", endDate: "2026-12-31", questionnaireVersionId: version!.id, rosterDraft }).expect(201)).body.campaign;
    const campaign = await create([{ smMarketId: f.market, smUserId: second }]), base = `${root}/${campaign.id}`;
    const source = (id: string | null) => f.database.update(f.schema.smSMDurcharbeitMarkets).set({ SMDurcharbeitSmUserId: id }).where(eq(f.schema.smSMDurcharbeitMarkets.smMarketId, f.market));
    const list = async (month = "2026-10-01") => (await admin("get", `${base}/targets?month=${month}`).expect(200)).body.targets;
    await t.test("legacy client owner values are ignored; create and save use the registry and preserve exact selection", async () => {
      assert.deepEqual(campaign.rosterDraft, [{ smMarketId: f.market, smUserId: f.employee }]);
      const updated = (await admin("patch", base).send({ name: campaign.name, startDate: campaign.startDate, endDate: campaign.endDate, questionnaireVersionId: campaign.questionnaireVersionId, rosterDraft: [{ smMarketId: f.market, smUserId: second }], expectedRevision: 1 }).expect(200)).body.campaign;
      assert.deepEqual(updated.rosterDraft, campaign.rosterDraft);
      assert.equal((await sm("get", `${employeeRoot}/targets`).expect(200)).body.targets.length, 0);
      const missing = await create([{ smMarketId: unselected }]);
      assert.equal(missing.rosterDraft[0].smUserId, second);
    });
    await t.test("missing and inactive registry IDs block publication without falling back to normal market IDs", async () => {
      await source(null);
      const preview = (await admin("get", `${base}/preview`).expect(200)).body;
      assert.deepEqual(preview.unresolved, [f.market]);
      const before = await snapshot(stateTables);
      await admin("post", `${base}/publish`).send({ expectedRevision: 2, previewToken: preview.previewToken }).expect(409);
      assert.deepEqual(await snapshot(stateTables), before);
      await source(f.employee);
      await f.database.update(f.schema.users).set({ isActive: false }).where(eq(f.schema.users.id, f.employee));
      assert.deepEqual((await admin("get", `${base}/preview`).expect(200)).body.unresolved, [f.market]);
      await f.database.update(f.schema.users).set({ isActive: true }).where(eq(f.schema.users.id, f.employee));
    });
    await t.test("registry changes invalidate publication preview; only selected stable market IDs are published", async () => {
      const stale = (await admin("get", `${base}/preview`).expect(200)).body;
      await source(second);
      const before = await snapshot(stateTables);
      await admin("post", `${base}/publish`).send({ expectedRevision: 2, previewToken: stale.previewToken }).expect(409);
      assert.deepEqual(await snapshot(stateTables), before);
      const current = (await admin("get", `${base}/preview`).expect(200)).body;
      assert.equal(current.marketCount, 1);
      await admin("post", `${base}/publish`).send({ expectedRevision: 2, previewToken: current.previewToken }).expect(200);
      assert.equal((await list()).length, 1); assert.equal((await list())[0].smUserId, second);
      assert.ok((await f.database.select().from(f.schema.smSMDurcharbeitCampaignMarkets)).every(row => row.smMarketId === f.market));
      assert.equal((await f.database.select().from(f.schema.smAssignments)).length, 0);
    });
    let target: any, visitId = "", submissionId = "";
    await t.test("an existing open campaign follows a changed registry ID using read-only resolution; past months remain frozen", async () => {
      const before = await snapshot(stateTables), past = await list("2026-09-01");
      await source(f.employee);
      target = (await list())[0]; assert.equal(target.smUserId, f.employee);
      assert.equal((await sm("get", `${employeeRoot}/targets`).expect(200)).body.targets[0].id, target.id);
      assert.equal((await sm("get", `${employeeRoot}/targets`, "synthetic-second-sm").expect(200)).body.targets.length, 0);
      assert.deepEqual(await list("2026-09-01"), past);
      assert.equal((await list("2026-11-01"))[0].smUserId, f.employee);
      assert.deepEqual(await snapshot(stateTables), before);
      const input = { expectedRevision: target.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() };
      await sm("post", `${employeeRoot}/targets/${target.id}/start`, "synthetic-second-sm").send(input).expect(403);
      await admin("patch", `${root}/targets/${target.id}`).send({ expectedRevision: target.revision, smUserId: second, scope: "month", reason: "Synthetic forbidden override" }).expect(400);
      assert.deepEqual(await snapshot(stateTables), before);
    });
    await t.test("starting work appends a pinned owner revision and retains all older owner records", async () => {
      const owners = await f.database.select().from(f.schema.smSMDurcharbeitOwnerRevisions);
      const started = (await sm("post", `${employeeRoot}/targets/${target.id}/start`).send({ expectedRevision: target.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
      visitId = started.visitId; submissionId = started.submissionId;
      const after = await f.database.select().from(f.schema.smSMDurcharbeitOwnerRevisions);
      assert.equal(after.length, owners.length + 1);
      assert.deepEqual(after.filter(row => owners.some(old => old.id === row.id)), owners);
      const [visit] = await f.database.select().from(f.schema.smSMDurcharbeitVisits);
      assert.equal(visit!.smUserId, f.employee); assert.equal(visit!.ownerRevisionId, after.find(row => row.smUserId === f.employee)!.id);
      await source(second);
      assert.equal((await list())[0].smUserId, f.employee, "A saved-ID change cannot steal a live visit");
      assert.equal((await list("2026-11-01"))[0].smUserId, second);
      await sm("get", `${employeeRoot}/visits/${visitId}`, "synthetic-second-sm").expect(403);
      await sm("get", `${employeeRoot}/visits/${visitId}`).expect(200);
    });
    await t.test("answers, photos, submission, management and photo archive keep the visit's recorded SM", async () => {
      const path = `${employeeRoot}/visits/${visitId}`, graph = (await sm("get", path).expect(200)).body;
      const question = graph.sections[0].questions[0], photo = graph.sections[0].questions[1];
      await sm("put", `${path}/answers/${question.id}`).send({ answer: { kind: "choice", optionCode: question.options[0].code }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
      const initialized = (await sm("post", `${path}/photos/initialize`).send({ submissionQuestionId: photo.id }).expect(200)).body;
      const upload = (await sm("post", `${path}/photos/presign`).send({ answerId: initialized.answerId, extension: "jpg" }).expect(200)).body.upload;
      objects.add(upload.path);
      const committed = (await sm("post", `${path}/photos/commit`).send({ answerId: initialized.answerId, photos: [{ storageBucket: "sm-visit-photos", storagePath: upload.path, mimeType: "image/jpeg", byteSize: 123 }] }).expect(200)).body;
      await sm("post", `${path}/submit`).send({ visitStartedAt: "2026-10-13T08:00:00Z", visitCompletedAt: "2026-10-13T08:15:00Z", clientMutationToken: randomUUID() }).expect(200);
      const before = await snapshot(stateTables), row = (await list())[0];
      assert.equal(row.smUserId, f.employee); assert.equal(row.completed, true); assert.equal(row.latestSubmissionId, submissionId);
      const result = (await admin("get", `${base}/results?month=2026-10-01`).expect(200)).body;
      assert.equal(result.summary.completed, 1); assert.equal(result.summary.availablePhotoUploads, 1);
      const management = (await admin("get", `/admin/sm-activity/completed/${submissionId}`).expect(200)).body;
      assert.ok(JSON.stringify(management).includes(submissionId));
      const archive = (await admin("get", "/admin/sm-photos").expect(200)).body;
      assert.equal(archive.total, 1); assert.equal(archive.photos[0].id, committed.fileIds[0]); assert.equal(archive.photos[0].submissionId, submissionId);
      assert.ok(objects.has(upload.path)); assert.deepEqual(await snapshot(stateTables), before);
    });
    await t.test("extensions use the registry for new months, fail closed on missing IDs, and preserve older visits", async () => {
      const tables = ["sm_smdurcharbeit_visits", "sm_questionnaire_submissions", "sm_question_answers", "sm_question_answer_files", "sm_smdurcharbeit_visit_time_revisions"];
      const history = await snapshot(tables), existing = await f.database.select().from(f.schema.smSMDurcharbeitTargets);
      const input = { expectedRevision: 3, endDate: "2027-01-31", reason: "Synthetic automatic extension", reactivate: false };
      await source(null);
      const before = await snapshot(stateTables);
      await admin("post", `${base}/extend`).send(input).expect(409);
      assert.deepEqual(await snapshot(stateTables), before);
      await source(second);
      await admin("post", `${base}/extend`).send(input).expect(200);
      assert.equal((await list("2027-01-01"))[0].smUserId, second);
      const current = await f.database.select().from(f.schema.smSMDurcharbeitTargets);
      assert.deepEqual(current.filter(row => existing.some(old => old.id === row.id)), existing);
      assert.deepEqual(await snapshot(tables), history);
    });
  } finally { await f.pg.close(); }
});
