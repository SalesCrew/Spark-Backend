import assert from "node:assert/strict";
import test from "node:test";
import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("Durcharbeit defaults and monthly substitutes remain separate across the complete visit flow", { timeout: 30_000 }, async t => {
  let now = "2026-09-15T12:00:00Z";
  class Clock extends Date { constructor(value?: string | number | Date) { super(value === undefined ? now : value instanceof Date ? value.getTime() : value); } static now() { return Date.parse(now); } }
  const second = randomUUID(), reader = randomUUID(), normalEditor = randomUUID(), dedicatedEditor = randomUUID(), objects = new Set<string>();
  const f = await createSMDurcharbeitFixture({ clock: Clock as typeof Date, additionalSmUsers: [{ id: second, token: "synthetic-second-sm" }], customerUsers: [{ id: reader, token: "synthetic-reader", permissions: { sm_durcharbeit_maerkte: ["read"], sm_durcharbeit_verplanung: ["read"] } }, { id: normalEditor, token: "synthetic-normal-editor", permissions: { sm_maerkte: ["read", "write", "update"] } }, { id: dedicatedEditor, token: "synthetic-dedicated-editor", permissions: { sm_durcharbeit_maerkte: ["read", "write", "update"] } }], photoStorage: { from: () => ({
    createSignedUploadUrl: async (path: string) => ({ data: { path, signedUrl: `https://synthetic.invalid/${path}`, token: "synthetic" }, error: null }),
    list: async (folder: string, options: { search: string }) => ({ data: objects.has(`${folder}/${options.search}`) ? [{ name: options.search }] : [], error: null }),
    createSignedUrl: async (path: string) => ({ data: { signedUrl: `https://synthetic.invalid/${path}` }, error: null }),
    remove: async () => { throw new Error("This flow must not delete stored photos"); },
  }) } });
  const admin = (method: "get" | "post" | "patch", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
  const sm = (method: "get" | "post" | "put", path: string, token = "synthetic-sm") => request(f.app)[method](path).auth(token, { type: "bearer" });
  const root = "/admin/sm-smdurcharbeit-campaigns", employeeRoot = "/sm/smdurcharbeit";
  const rows = async (table: string) => (await f.pg.query(`select row_to_json(t) as row from ${table} t`)).rows.map(row => JSON.stringify(row)).sort();
  const historyTables = ["sm_smdurcharbeit_visits", "sm_questionnaire_submissions", "sm_questionnaire_submission_questions", "sm_question_answers", "sm_question_answer_files", "sm_smdurcharbeit_visit_time_revisions"];
  const graph = async () => Object.fromEntries(await Promise.all([...historyTables, "sm_smdurcharbeit_assignment_revisions", "sm_smdurcharbeit_month_targets", "sm_smdurcharbeit_events", "sm_markets", "sm_smdurcharbeit_markets"].map(async table => [table, await rows(table)])));
  try {
    const input = { fileName: "Synthetic-owner.xlsx", sheetName: "Fixture", mapping: { SMDurcharbeitVertriebstyp: "A", name: "B", address: "C", postalCode: "D", city: "E", SMDurcharbeitEmEh: "F", shelfMerchandiserName: "G" }, rows: [["Vertriebstyp", "Firma/Betrieb", "Straße", "PLZ", "Ort", "EM/EH", "Verplanung"], ["Spar", "Synthetic owner market", "Fixture 2", "1010", "Wien", "EM", "Local SM"]] };
    const imported = (await admin("post", "/admin/sm-markets/SMDurcharbeit/import").send(input).expect(200)).body.markets[0];
    const marketId = imported.id, sourceBefore = (await f.database.select().from(f.schema.smSMDurcharbeitMarkets))[0]!;
    const standardBefore = await f.database.select().from(f.schema.smMarkets).where(eq(f.schema.smMarkets.id, f.market));
    const module = (await admin("post", "/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Synthetic owners", description: "", questions: [{ id: `new-${randomUUID()}`, type: "yesno", text: "Synthetic completion", required: true, options: ["Ja", "Nein"], config: {}, rules: [] }, { id: `new-${randomUUID()}`, type: "photo", text: "Synthetic proof", required: false, options: [], config: {}, rules: [] }] }).expect(201)).body.module;
    const form = (await admin("post", "/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Synthetic owners", description: "", status: "active", moduleIds: [module.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    const campaign = (await admin("post", root).send({ name: "Synthetic owners", startDate: "2026-09-01", endDate: "2026-12-31", questionnaireVersionId: version!.id, rosterDraft: [{ smMarketId: marketId }] }).expect(201)).body.campaign, base = `${root}/${campaign.id}`;
    const preview = (await admin("get", `${base}/preview`).expect(200)).body;
    await admin("post", `${base}/publish`).send({ expectedRevision: 1, previewToken: preview.previewToken }).expect(200);
    const target = async (month = "2026-10-01") => (await admin("get", `${base}/targets?month=${month}`).expect(200)).body.targets[0];
    const change = (row: any, id: string | null) => admin("patch", `${root}/targets/${row.id}/owner`).send({ expectedRevision: row.revision, SMDurcharbeitSmUserId: id, reason: "Synthetic monthly substitute" });
    const market = async () => (await admin("get", "/admin/sm-markets?SMDurcharbeitMarketScope=SMDurcharbeit").expect(200)).body.markets.find((row: any) => row.id === marketId);
    const changeDefault = async (id: string | null, status = 200) => admin("patch", `/admin/sm-markets/SMDurcharbeit/${marketId}/owner`).send({ SMDurcharbeitSmUserId: id, expectedUpdatedAt: (await market()).updatedAt }).expect(status);
    const complete = async (row: any, day: string, token: string) => {
      const started = (await sm("post", `${employeeRoot}/targets/${row.id}/start`, token).send({ expectedRevision: row.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
      const path = `${employeeRoot}/visits/${started.visitId}`, visit = (await sm("get", path, token).expect(200)).body, question = visit.sections[0].questions[0], photo = visit.sections[0].questions[1];
      await sm("put", `${path}/answers/${question.id}`, token).send({ answer: { kind: "choice", optionCode: question.options[0].code }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
      const initialized = (await sm("post", `${path}/photos/initialize`, token).send({ submissionQuestionId: photo.id }).expect(200)).body;
      const upload = (await sm("post", `${path}/photos/presign`, token).send({ answerId: initialized.answerId, extension: "png" }).expect(200)).body.upload; objects.add(upload.path);
      await sm("post", `${path}/photos/commit`, token).send({ answerId: initialized.answerId, photos: [{ storageBucket: upload.bucket, storagePath: upload.path, originalFileName: "Synthetic.png", mimeType: "image/png", byteSize: 12, widthPx: 1, heightPx: 1 }] }).expect(200);
      await sm("post", `${path}/submit`, token).send({ visitStartedAt: `${day}T08:00:00Z`, visitCompletedAt: `${day}T08:15:00Z`, clientMutationToken: randomUUID() }).expect(200);
      return started;
    };
    await complete(await target("2026-09-01"), "2026-09-15", "synthetic-sm");
    now = "2026-10-14T12:00:00Z";
    const past = await target("2026-09-01"), historyBefore = Object.fromEntries(await Promise.all(historyTables.map(async table => [table, await rows(table)])));
    await t.test("monthly override moves only this open target to the new employee", async () => {
      await change(await target(), second).expect(200);
      assert.equal((await target()).smUserId, second); assert.equal((await target()).SMDurcharbeitSmOverride, true);
      assert.equal((await target("2026-11-01")).smUserId, f.employee);
      assert.equal((await market()).assignedSmUserId, f.employee);
      assert.equal((await sm("get", `${employeeRoot}/targets`).expect(200)).body.targets.length, 0);
      assert.equal((await sm("get", `${employeeRoot}/targets`, "synthetic-second-sm").expect(200)).body.targets[0].smUserId, second);
    });
    await t.test("default changes save exact IDs/names, preserve imported source fields and cannot override monthly choices", async () => {
      const oldMarket = await market();
      const updated = (await changeDefault(second)).body.market;
      assert.equal(updated.assignedSmUserId, second); assert.equal(updated.shelfMerchandiserName, "Second SM");
      const [source] = await f.database.select().from(f.schema.smSMDurcharbeitMarkets);
      assert.deepEqual({ ...source, SMDurcharbeitSmUserId: sourceBefore.SMDurcharbeitSmUserId }, sourceBefore);
      await admin("patch", `/admin/sm-markets/SMDurcharbeit/${marketId}/owner`).send({ SMDurcharbeitSmUserId: f.employee, expectedUpdatedAt: oldMarket.updatedAt }).expect(409);
      await changeDefault(f.employee);
      assert.equal((await target()).smUserId, second);
      assert.equal((await target("2026-11-01")).smUserId, f.employee);
      assert.deepEqual(await f.database.select().from(f.schema.smMarkets).where(eq(f.schema.smMarkets.id, f.market)), standardBefore);
    });
    await t.test("reimport preserves edited IDs/names and explicit unassignment", async () => {
      await changeDefault(second);
      const replay = (await admin("post", "/admin/sm-markets/SMDurcharbeit/import").send(input).expect(200)).body;
      assert.equal(replay.markets[0].assignedSmUserId, second); assert.equal(replay.markets[0].shelfMerchandiserName, "Second SM");
      await changeDefault(null);
      await admin("post", "/admin/sm-markets/SMDurcharbeit/import").send(input).expect(200);
      assert.equal((await market()).assignedSmUserId, null);
      assert.equal((await target()).smUserId, second);
      await changeDefault(f.employee);
    });
    await t.test("restoring automatic assignment appends a revision; prior owner records remain unchanged", async () => {
      const previousOwners = await rows("sm_smdurcharbeit_assignment_revisions");
      await change(await target(), null).expect(200);
      assert.equal((await target()).SMDurcharbeitSmOverride, false); assert.equal((await target()).smUserId, f.employee);
      const currentOwners = await rows("sm_smdurcharbeit_assignment_revisions");
      assert.ok(previousOwners.every(row => currentOwners.includes(row)));
      await change(await target(), second).expect(200);
    });
    await t.test("permissions, role/account checks, stale revisions and closed months fail atomically", async () => {
      const before = await graph(), row = await target();
      await request(f.app).patch(`${root}/targets/${row.id}/owner`).auth("synthetic-reader", { type: "bearer" }).set("x-coke-spark-page-key", "sm_durcharbeit_verplanung").send({ expectedRevision: row.revision, SMDurcharbeitSmUserId: f.employee, reason: "Synthetic blocked" }).expect(403);
      await request(f.app).patch(`/admin/sm-markets/SMDurcharbeit/${marketId}/owner`).auth("synthetic-reader", { type: "bearer" }).set("x-coke-spark-page-key", "sm_durcharbeit_maerkte").send({ SMDurcharbeitSmUserId: second, expectedUpdatedAt: (await market()).updatedAt }).expect(403);
      for (const prefix of ["SMDurcharbeit", "smdurcharbeit", "SmDurcharbeit"]) {
        await request(f.app).patch(`/admin/sm-markets/${prefix}/${marketId}/owner`).auth("synthetic-normal-editor", { type: "bearer" }).set("x-coke-spark-page-key", "sm_maerkte").send({ SMDurcharbeitSmUserId: second, expectedUpdatedAt: (await market()).updatedAt }).expect(403);
      }
      await request(f.app).patch(`/admin/sm-markets/${f.market}?SMDurcharbeitMarketScope=SMDurcharbeit`).auth("synthetic-dedicated-editor", { type: "bearer" }).send({ name: "Must remain unchanged" }).expect(403);
      await request(f.app).patch(`${root}/targets/${row.id}/owner`).auth("synthetic-dedicated-editor", { type: "bearer" }).send({ expectedRevision: row.revision, SMDurcharbeitSmUserId: f.employee, reason: "Wrong page permission" }).expect(403);
      await change(row, f.admin).expect(400);
      await changeDefault(f.admin, 400);
      await change({ ...row, revision: row.revision - 1 }, f.employee).expect(409);
      await change(past, second).expect(409);
      await admin("patch", `${root}/targets/${row.id}/owner`).send({ expectedRevision: row.revision, SMDurcharbeitSmUserId: f.employee, scope: "future", reason: "Not allowed" }).expect(400);
      assert.deepEqual(await graph(), before);
      for (const state of [{ isActive: false, deletedAt: null }, { isActive: true, deletedAt: new Date(now) }]) {
        await f.database.update(f.schema.users).set(state).where(eq(f.schema.users.id, second));
        const blocked = await graph(); await change(await target(), second).expect(400); await changeDefault(second, 400);
        assert.deepEqual(await graph(), blocked);
      }
      await f.database.update(f.schema.users).set({ isActive: true, deletedAt: null }).where(eq(f.schema.users.id, second));
    });
    await t.test("dedicated market update access accepts all routed case aliases without granting campaign access", async () => {
      for (const prefix of ["SMDurcharbeit", "smdurcharbeit", "SmDurcharbeit"]) {
        await request(f.app).patch(`/admin/sm-markets/${prefix}/${marketId}/owner`).auth("synthetic-dedicated-editor", { type: "bearer" }).set("x-coke-spark-page-key", "sm_maerkte").send({ SMDurcharbeitSmUserId: f.employee, expectedUpdatedAt: (await market()).updatedAt }).expect(200);
        assert.equal((await market()).assignedSmUserId, f.employee);
      }
    });
    await t.test("an active draft is protected, then the substitute submits answers/photos/time under their own identity", async () => {
      const row = await target(), started = (await sm("post", `${employeeRoot}/targets/${row.id}/start`, "synthetic-second-sm").send({ expectedRevision: row.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
      const before = await graph(); await change(await target(), f.employee).expect(409); assert.deepEqual(await graph(), before);
      const path = `${employeeRoot}/visits/${started.visitId}`, visit = (await sm("get", path, "synthetic-second-sm").expect(200)).body, question = visit.sections[0].questions[0], photo = visit.sections[0].questions[1];
      await sm("put", `${path}/answers/${question.id}`, "synthetic-second-sm").send({ answer: { kind: "choice", optionCode: question.options[0].code }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
      const initialized = (await sm("post", `${path}/photos/initialize`, "synthetic-second-sm").send({ submissionQuestionId: photo.id }).expect(200)).body;
      const upload = (await sm("post", `${path}/photos/presign`, "synthetic-second-sm").send({ answerId: initialized.answerId, extension: "png" }).expect(200)).body.upload; objects.add(upload.path);
      await sm("post", `${path}/photos/commit`, "synthetic-second-sm").send({ answerId: initialized.answerId, photos: [{ storageBucket: upload.bucket, storagePath: upload.path, originalFileName: "Substitute.png", mimeType: "image/png", byteSize: 12, widthPx: 1, heightPx: 1 }] }).expect(200);
      await sm("post", `${path}/submit`, "synthetic-second-sm").send({ visitStartedAt: "2026-10-14T08:00:00Z", visitCompletedAt: "2026-10-14T08:15:00Z", clientMutationToken: randomUUID() }).expect(200);
      const done = await target(); assert.equal(done.smUserId, second); assert.equal(done.completed, true);
      const completed = await graph(); await change(done, f.employee).expect(409); assert.deepEqual(await graph(), completed);
      const report = (await admin("get", `${base}/results?month=2026-10-01`).expect(200)).body;
      assert.equal(report.summary.completed, 1); assert.equal(report.summary.actualMinutes, 15); assert.equal(report.summary.availablePhotoUploads, 1);
      const management = (await admin("get", `/admin/sm-activity/completed/${done.latestSubmissionId}`).expect(200)).body;
      assert.ok(JSON.stringify(management).includes(second));
      const archive = (await admin("get", "/admin/sm-photos").expect(200)).body; assert.equal(archive.total, 2);
      assert.ok(archive.photos.some((photo: any) => photo.submissionId === done.latestSubmissionId));
    });
    await t.test("month rollover and a new campaign use the default without resetting or rewriting history", async () => {
      now = "2026-11-02T12:00:00Z";
      assert.equal((await target()).smUserId, second);
      assert.equal((await target("2026-11-01")).smUserId, f.employee);
      assert.equal((await sm("get", `${employeeRoot}/targets`).expect(200)).body.targets[0].month, "2026-11-01");
      assert.equal((await sm("get", `${employeeRoot}/targets`, "synthetic-second-sm").expect(200)).body.targets.length, 0);
      assert.deepEqual(await target("2026-09-01"), past);
      const newCampaign = (await admin("post", root).send({ name: "Synthetic next campaign", startDate: "2026-11-01", endDate: "2026-11-30", questionnaireVersionId: version!.id, rosterDraft: [{ smMarketId: marketId }] }).expect(201)).body.campaign;
      assert.equal(newCampaign.rosterDraft[0].smUserId, f.employee);
      const newPreview = (await admin("get", `${root}/${newCampaign.id}/preview`).expect(200)).body;
      await admin("post", `${root}/${newCampaign.id}/publish`).send({ expectedRevision: 1, previewToken: newPreview.previewToken, confirmOverlap: true }).expect(200);
      assert.equal((await admin("get", `${root}/${newCampaign.id}/targets?month=2026-11-01`).expect(200)).body.targets[0].smUserId, f.employee);
      for (const table of historyTables) { const after = await rows(table); assert.ok(historyBefore[table].every(row => after.includes(row))); }
    });
    if (f.nativePostgres) {
      await t.test("racing a monthly switch with visit start cannot steal a started draft", async () => {
        const row = await target("2026-11-01"), history = Object.fromEntries(await Promise.all(historyTables.map(async table => [table, await rows(table)])));
        const [changed, started] = await Promise.all([change(row, second), sm("post", `${employeeRoot}/targets/${row.id}/start`).send({ expectedRevision: row.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() })]);
        assert.ok((changed.status === 200 && started.status === 403) || (changed.status === 409 && started.status === 201));
        assert.equal((await target("2026-11-01")).smUserId, started.status === 201 ? f.employee : second);
        for (const table of historyTables) { const after = await rows(table); assert.ok(history[table].every(record => after.includes(record))); }
      });
      await t.test("two saves with the same target revision produce exactly one new owner revision", async () => {
        const row = await target("2026-12-01"), ownersBefore = await rows("sm_smdurcharbeit_assignment_revisions");
        const responses = await Promise.all([change(row, second), change(row, null)]);
        assert.deepEqual(responses.map(response => response.status).sort(), [200, 409]);
        assert.equal((await rows("sm_smdurcharbeit_assignment_revisions")).length, ownersBefore.length + 1);
      });
      await t.test("a default-market save racing visit start updates IDs atomically and leaves a started visit pinned", async () => {
        now = "2026-12-02T12:00:00Z";
        await change(await target("2026-12-01"), null).expect(200);
        const row = await target("2026-12-01"), history = Object.fromEntries(await Promise.all(historyTables.map(async table => [table, await rows(table)])));
        const [updated, started] = await Promise.all([changeDefault(second), sm("post", `${employeeRoot}/targets/${row.id}/start`).send({ expectedRevision: row.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() })]);
        assert.equal(updated.status, 200); assert.ok([201, 403].includes(started.status));
        assert.equal((await market()).assignedSmUserId, second);
        assert.equal((await target("2026-12-01")).smUserId, started.status === 201 ? f.employee : second);
        for (const table of historyTables) { const after = await rows(table); assert.ok(history[table].every(record => after.includes(record))); }
      });
    }
    await t.test("campaign expiry freezes historical ownership even when the market default changes later", async () => {
      now = "2027-01-02T12:00:00Z";
      const december = await target("2026-12-01");
      const before = await graph(); await change(december, second).expect(409); assert.deepEqual(await graph(), before);
      await changeDefault(second);
      assert.deepEqual(await target("2026-12-01"), december);
      assert.deepEqual(await target("2026-09-01"), past);
    });
  } finally { await f.pg.close(); }
});
