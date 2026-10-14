import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq, inArray } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("published campaign market edits preserve history, protect visits, and apply a reviewed selection atomically", async t => {
  class Clock extends Date { constructor(value?: string | number | Date) { super(value === undefined ? "2026-10-13T12:00:00Z" : value instanceof Date ? value.getTime() : value); } static now() { return Date.parse("2026-10-13T12:00:00Z"); } }
  const objects = new Set<string>();
  const secondSm = randomUUID();
  const f = await createSMDurcharbeitFixture({ clock: Clock as typeof Date, additionalSmUsers: [{ id: secondSm, token: "synthetic-second-sm" }], photoStorage: { from: () => ({
    createSignedUploadUrl: async (path: string) => ({ data: { path, signedUrl: `https://synthetic.invalid/${path}`, token: "synthetic" }, error: null }),
    list: async (folder: string, options: { search: string }) => ({ data: objects.has(`${folder}/${options.search}`) ? [{ name: options.search }] : [], error: null }),
    createSignedUrl: async (path: string) => ({ data: { signedUrl: `https://synthetic.invalid/${path}` }, error: null }),
    remove: async () => { throw new Error("Market editing must never delete photo objects"); },
  }) } });
  const admin = (method: "get" | "post" | "patch", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
  const sm = (method: "get" | "post" | "put", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" });
  const prefix = "/admin/sm-smdurcharbeit-campaigns";
  const snapshot = async (names: string[]) => Object.fromEntries(await Promise.all(names.map(async name => [name, (await f.pg.query(`select * from ${name} order by ${name === "sm_smdurcharbeit_markets" ? "sm_market_id" : "id"}`)).rows])));
  try {
    const markets = Array.from({ length: 424 }, (_, i) => ({ id: randomUUID(), name: `${i < 324 ? "SPAR" : "Billa"} Synthetic ${i}`, chain: i < 324 ? "Spar" : "Billa", address: `Synthetic ${i}`, postalCode: "1010", city: "Wien", region: "Ost", assignedSmUserId: f.employee }));
    await f.database.insert(f.schema.smMarkets).values(markets);
    await f.database.insert(f.schema.smSMDurcharbeitMarkets).values(markets.map(row => ({ smMarketId: row.id, SMDurcharbeitSmUserId: f.employee })));
    const module = (await admin("post", "/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Synthetic roster audit", description: "", questions: [{ id: `new-${randomUUID()}`, type: "photo", text: "Synthetic photo", required: false, options: [], config: {}, rules: [] }] }).expect(201)).body.module;
    const form = (await admin("post", "/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Synthetic roster questionnaire", description: "", status: "active", moduleIds: [module.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    const campaign = (await admin("post", prefix).send({ name: "Synthetic complete roster", startDate: "2026-09-01", endDate: "2026-12-31", questionnaireVersionId: version!.id, rosterDraft: markets.map(row => ({ smMarketId: row.id, smUserId: f.employee })) }).expect(201)).body.campaign;
    const base = `${prefix}/${campaign.id}`;
    const publication = (await admin("get", `${base}/preview`).expect(200)).body;
    await admin("post", `${base}/publish`).send({ expectedRevision: 1, previewToken: publication.previewToken }).expect(200);
    const list = async (month = "2026-10-01") => (await admin("get", `${base}/targets?month=${month}`).expect(200)).body;
    const start = async (marketId: string) => {
      const target = (await list()).targets.find((row: any) => row.market.id === marketId);
      return (await sm("post", `/sm/smdurcharbeit/targets/${target.id}/start`).send({ expectedRevision: target.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
    };
    const draftVisit = await start(markets[324]!.id), completedVisit = await start(markets[325]!.id);
    const visitPath = `/sm/smdurcharbeit/visits/${completedVisit.visitId}`;
    const payload = (await sm("get", visitPath).expect(200)).body, question = payload.sections[0].questions[0];
    const initialized = (await sm("post", `${visitPath}/photos/initialize`).send({ submissionQuestionId: question.id }).expect(200)).body;
    const upload = (await sm("post", `${visitPath}/photos/presign`).send({ answerId: initialized.answerId, extension: "jpg" }).expect(200)).body.upload;
    objects.add(upload.path);
    const committed = (await sm("post", `${visitPath}/photos/commit`).send({ answerId: initialized.answerId, photos: [{ storageBucket: "sm-visit-photos", storagePath: upload.path, mimeType: "image/jpeg", byteSize: 123 }] }).expect(200)).body;
    await sm("put", `${visitPath}/answers/${question.id}`).send({ answer: { kind: "photo", fileIds: committed.fileIds, comment: "Preserve me" }, expectedAnswerVersion: 1, clientMutationToken: randomUUID() }).expect(200);
    await sm("post", `${visitPath}/submit`).send({ visitStartedAt: "2026-10-13T08:00:00Z", visitCompletedAt: "2026-10-13T08:15:00Z", clientMutationToken: randomUUID() }).expect(200);
    const historyTables = ["sm_smdurcharbeit_visits", "sm_questionnaire_submissions", "sm_questionnaire_submission_sections", "sm_questionnaire_submission_questions", "sm_question_answers", "sm_question_answer_files", "sm_question_answer_events", "sm_smdurcharbeit_visit_time_revisions"];
    const historyBefore = await snapshot(historyTables), past = await list("2026-09-01");
    let revision = 2;
    const selection = (roster = markets.slice(0, 324).map(row => ({ smMarketId: row.id })), month = "2026-10-01", scope = "future") => ({ expectedRevision: revision, month, scope, reason: "Synthetic SPAR correction", roster });
    const preview = async (input: any) => (await admin("post", `${base}/markets/preview`).send(input).expect(200)).body;
    const apply = async (input: any, reviewed: any) => (await admin("patch", `${base}/markets`).send({ ...input, previewToken: reviewed.previewToken }).expect(200)).body;
    await t.test("preview is read-only and shows actual totals including protected draft/completed visits", async () => {
      const before = await snapshot(["sm_smdurcharbeit_campaigns", "sm_smdurcharbeit_month_targets", "sm_smdurcharbeit_events"]);
      const reviewed = await preview(selection());
      assert.equal(reviewed.changedTargets, 298); assert.equal(reviewed.protected.length, 2);
      assert.deepEqual(reviewed.months.map((m: any) => [m.month, m.required, m.excluded, m.protected]), [["2026-10-01", 326, 98, 2], ["2026-11-01", 324, 100, 0], ["2026-12-01", 324, 100, 0]]);
      assert.deepEqual(await snapshot(["sm_smdurcharbeit_campaigns", "sm_smdurcharbeit_month_targets", "sm_smdurcharbeit_events"]), before);
    });
    await t.test("bulk correction changes requirements and employee availability without changing any visit or past month", async () => {
      const input = selection(), reviewed = await preview(input);
      assert.equal((await apply(input, reviewed)).updated, true); revision++;
      const current = await list(); assert.equal(current.summary.required, 326); assert.equal(current.summary.waived, 98);
      assert.equal((await list("2026-11-01")).summary.required, 324);
      assert.deepEqual(await list("2026-09-01"), past);
      assert.deepEqual(await snapshot(historyTables), historyBefore);
      const waived = current.targets.find((row: any) => row.market.id === markets[326]!.id);
      await sm("post", `/sm/smdurcharbeit/targets/${waived.id}/start`).send({ expectedRevision: waived.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }).expect(409);
      assert.equal((await sm("get", `/sm/smdurcharbeit/visits/${draftVisit.visitId}`).expect(200)).body.submission.status, "draft");
      const report = (await admin("get", `${base}/results?month=2026-10-01`).expect(200)).body;
      assert.equal(report.summary.required, 326); assert.equal(report.summary.availablePhotoUploads, 1); assert.ok(objects.has(upload.path));
      const repeated = await admin("patch", `${base}/markets`).send({ ...input, previewToken: reviewed.previewToken }).expect(409);
      assert.equal(repeated.body.code, "smdurcharbeit_campaign_stale");
    });
    await t.test("an excluded market can be included for one month without reopening future months", async () => {
      const input = selection([...markets.slice(0, 324), markets[324]!, markets[325]!, markets[326]!].map(row => ({ smMarketId: row.id })), "2026-10-01", "month");
      await apply(input, await preview(input)); revision++;
      assert.equal((await list()).summary.required, 327); assert.equal((await list("2026-11-01")).summary.required, 324);
      assert.deepEqual(await snapshot(historyTables), historyBefore);
    });
    await t.test("100 open markets follow saved registry IDs without rewriting history or owner records", async () => {
      const before = await snapshot(["sm_smdurcharbeit_campaigns", "sm_smdurcharbeit_month_targets", "sm_smdurcharbeit_assignment_revisions", "sm_smdurcharbeit_events"]);
      const currentBefore = await list();
      await f.database.update(f.schema.smSMDurcharbeitMarkets).set({ SMDurcharbeitSmUserId: secondSm }).where(inArray(f.schema.smSMDurcharbeitMarkets.smMarketId, markets.slice(324).map(row => row.id)));
      const current = await list(); assert.deepEqual(current.summary, currentBefore.summary);
      assert.equal(current.targets.filter((row: any) => row.smUserId === secondSm).length, 98);
      assert.equal((await list("2026-11-01")).targets.filter((row: any) => row.smUserId === secondSm).length, 100);
      assert.deepEqual(await list("2026-09-01"), past); assert.deepEqual(await snapshot(historyTables), historyBefore);
      assert.deepEqual(await snapshot(Object.keys(before)), before, "Loading automatic ownership never updates a campaign, target or owner revision");
      const next = (await request(f.app).get("/sm/smdurcharbeit/targets").auth("synthetic-second-sm", { type: "bearer" }).expect(200)).body.targets;
      assert.equal(next.length, 98); const included = next.find((row: any) => row.market.id === markets[326]!.id);
      assert.equal(included.available, true); assert.equal(included.eligibility, "required");
      await sm("get", `/sm/smdurcharbeit/targets/${included.id}`).expect(403);
      await request(f.app).get(`/sm/smdurcharbeit/targets/${included.id}`).auth("synthetic-second-sm", { type: "bearer" }).expect(200);
      await admin("patch", `${prefix}/targets/${included.id}`).send({ expectedRevision: included.revision, reason: "Synthetic manual owner rejected", scope: "month", smUserId: f.employee }).expect(400);
      assert.deepEqual(await snapshot(Object.keys(before)), before); assert.ok(objects.has(upload.path));
    });
    await t.test("a stale target preview cannot partially apply and reload preserves the changed target", async () => {
      const input = selection(), reviewed = await preview(input), current = await list();
      const target = current.targets.find((row: any) => row.market.id === markets[327]!.id);
      await admin("patch", `${prefix}/targets/${target.id}`).send({ expectedRevision: target.revision, reason: "Synthetic concurrent correction", scope: "month", eligibility: "required" }).expect(200);
      const before = await snapshot(["sm_smdurcharbeit_campaigns", "sm_smdurcharbeit_month_targets", "sm_smdurcharbeit_events"]);
      const failed = await admin("patch", `${base}/markets`).send({ ...input, previewToken: reviewed.previewToken }).expect(409);
      assert.equal(failed.body.code, "smdurcharbeit_market_preview_stale");
      assert.deepEqual(await snapshot(["sm_smdurcharbeit_campaigns", "sm_smdurcharbeit_month_targets", "sm_smdurcharbeit_events"]), before);
    });
    await t.test("new registry markets can be added, overlap is explicit, and no past targets are backfilled", async () => {
      const id = randomUUID();
      await f.database.insert(f.schema.smMarkets).values({ ...markets[0]!, id, name: "New synthetic SPAR", assignedSmUserId: null });
      await f.database.insert(f.schema.smSMDurcharbeitMarkets).values({ smMarketId: id });
      const input = selection([...markets.slice(0, 324).map(row => ({ smMarketId: row.id })), { smMarketId: id, smUserId: f.employee }]);
      const unassigned = { ...input, roster: [...input.roster.slice(0, -1), { smMarketId: id }] };
      assert.equal((await admin("post", `${base}/markets/preview`).send(unassigned).expect(409)).body.code, "smdurcharbeit_roster_unassigned");
      await f.database.update(f.schema.smSMDurcharbeitMarkets).set({ SMDurcharbeitSmUserId: f.employee }).where(eq(f.schema.smSMDurcharbeitMarkets.smMarketId, id));
      const other = (await admin("post", prefix).send({ name: "Synthetic overlap", startDate: "2026-11-01", endDate: "2026-12-31", questionnaireVersionId: version!.id, rosterDraft: [{ smMarketId: id, smUserId: f.employee }] }).expect(201)).body.campaign;
      const otherPreview = (await admin("get", `${prefix}/${other.id}/preview`).expect(200)).body;
      await admin("post", `${prefix}/${other.id}/publish`).send({ expectedRevision: 1, previewToken: otherPreview.previewToken }).expect(200);
      const reviewed = await preview(input); assert.equal(reviewed.overlapping, 1); assert.ok(reviewed.months.every((row: any) => row.added === 1));
      await admin("patch", `${base}/markets`).send({ ...input, previewToken: reviewed.previewToken }).expect(409);
      await admin("patch", `${base}/markets`).send({ ...input, previewToken: reviewed.previewToken, confirmOverlap: true }).expect(200); revision++;
      for (const month of ["2026-10-01", "2026-11-01", "2026-12-01"]) assert.equal((await list(month)).targets.find((row: any) => row.market.id === id).smUserId, f.employee);
      assert.deepEqual(await list("2026-09-01"), past); assert.deepEqual(await snapshot(historyTables), historyBefore);
    });
    await t.test("all exclusions are supported, no-op saves do not churn history, and extension inherits exclusions", async () => {
      const input = selection([], "2026-12-01", "month"); await apply(input, await preview(input)); revision++;
      assert.equal((await list("2026-12-01")).summary.required, 0);
      const before = await snapshot(["sm_smdurcharbeit_campaigns", "sm_smdurcharbeit_month_targets", "sm_smdurcharbeit_events"]);
      const noop = selection([], "2026-12-01", "month"); assert.equal((await apply(noop, await preview(noop))).updated, false);
      assert.deepEqual(await snapshot(["sm_smdurcharbeit_campaigns", "sm_smdurcharbeit_month_targets", "sm_smdurcharbeit_events"]), before);
      await admin("post", `${base}/extend`).send({ expectedRevision: revision, endDate: "2027-01-31", reason: "Synthetic extension", reactivate: false }).expect(200); revision++;
      assert.equal((await list("2027-01-01")).summary.required, 0); assert.deepEqual(await snapshot(historyTables), historyBefore);
    });
    await t.test("authorization, malformed/foreign selections, closed months and archive protection", async () => {
      await request(f.app).post(`${base}/markets/preview`).auth("synthetic-sm", { type: "bearer" }).send(selection()).expect(403);
      await request(f.app).patch(`${base}/markets`).auth("synthetic-gm-admin", { type: "bearer" }).send({ ...selection(), previewToken: "0".repeat(64) }).expect(403);
      await admin("post", `${base}/markets/preview`).send(selection([{ smMarketId: f.market }])).expect(409);
      await admin("post", `${base}/markets/preview`).send(selection([{ smMarketId: markets[0]!.id }, { smMarketId: markets[0]!.id }])).expect(400);
      await admin("post", `${base}/markets/preview`).send(selection([], "2026-09-01")).expect(409);
      await admin("post", `${base}/markets/preview`).send(selection([], "2026-10-02")).expect(409);
      await admin("post", `${base}/markets/preview`).send({ ...selection(), unexpected: true }).expect(400);
      await admin("patch", `${base}/state`).send({ expectedRevision: revision, status: "paused", reason: "Synthetic pause" }).expect(200); revision++;
      await preview(selection());
      await admin("patch", `${base}/state`).send({ expectedRevision: revision, status: "archived", reason: "Synthetic archive" }).expect(200); revision++;
      await admin("post", `${base}/markets/preview`).send(selection()).expect(409);
      assert.deepEqual(await snapshot(historyTables), historyBefore);
    });
  } finally { await f.pg.close(); }
});
