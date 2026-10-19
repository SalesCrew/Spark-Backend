import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import { createQuestionnaireUpdateFixture } from "../tests/SMDurcharbeit-questionnaire-update-fixture.js";

test("pause hides one campaign throughout employee APIs until explicit resume, preserving history", async t => {
  const f = await createQuestionnaireUpdateFixture();
  const campaignUrl = `${f.root}/${f.campaign.id}`;
  const campaign = async () => (await f.admin("get", f.root).expect(200)).body.campaigns.find((row: any) => row.id === f.campaign.id);
  const state = async (status: string) => f.admin("patch", `${campaignUrl}/state`).send({ expectedRevision: (await campaign()).revision, status, reason: "Synthetic pause visibility" }).expect(200);
  const list = async (month = "2026-10-01") => (await f.sm("get", `${f.employeeRoot}?month=${month}`).expect(200)).body;
  const history = async () => JSON.stringify(await Promise.all([
    f.schema.smQuestionnaireSubmissions, f.schema.smQuestionnaireSubmissionSections, f.schema.smQuestionnaireSubmissionQuestions,
    f.schema.smQuestionAnswers, f.schema.smQuestionAnswerOptions, f.schema.smQuestionAnswerFiles, f.schema.smQuestionAnswerEvents,
    f.schema.smSMDurcharbeitVisits, f.schema.smSMDurcharbeitTimeRevisions,
  ].map(table => f.database.select().from(table as any))));
  try {
    const before = await history();
    const draft = (await f.sm("get", `${f.visitRoot}/${f.draft.visitId}`).expect(200)).body;
    const timesUrl = "/sm/smdurcharbeit-times?from=2026-10-01&to=2026-10-31";
    assert.equal((await f.sm("get", timesUrl).expect(200)).body.entries.length, 1);
    assert.equal((await f.sm("get", "/sm/activity/completed").expect(200)).body.visits.length, 1);
    await state("paused");
    await t.test("markets, campaign options and month choices exclude paused work, while other campaigns remain", async () => {
      const current = await list(); assert.equal(current.targets.length, 3);
      assert.ok(current.targets.every((row: any) => row.campaignId === f.otherCampaign.id));
      const future = await list("2026-11-01"); assert.equal(future.targets.length, 3);
      for (const target of f.targets) await f.sm("get", `${f.employeeRoot}/${target.id}`).expect(404);
      const admin = (await f.admin("get", `${campaignUrl}/targets?month=2026-10-01`).expect(200)).body;
      assert.equal(admin.targets.length, 3); assert.equal(admin.summary.completed, 1);
      assert.equal(await history(), before);
    });
    await t.test("direct saved links, start/replay, answer/photo saves and submit cannot expose or mutate paused work", async () => {
      for (const id of [f.completed.visitId, f.draft.visitId]) {
        const result = await f.sm("get", `${f.visitRoot}/${id}`).expect(404);
        assert.equal(result.body.code, "smdurcharbeit_campaign_paused");
        assert.equal(result.body.sections, undefined);
      }
      const [submission] = await f.database.select().from(f.schema.smQuestionnaireSubmissions).where(eq(f.schema.smQuestionnaireSubmissions.id, draft.submission.id));
      for (const token of [randomUUID(), submission!.clientSubmissionToken]) await f.sm("post", `${f.employeeRoot}/${f.targets[1].id}/start`).send({ expectedRevision: f.targets[1].revision, followUp: false, mode: "manual", clientSubmissionToken: token }).expect(404);
      const q = draft.sections[0].questions[0];
      await f.sm("put", `${f.visitRoot}/${f.draft.visitId}/answers/${q.id}`).send({ answer: { kind: "choice", optionCode: q.options[0].code }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(404);
      await f.sm("post", `${f.visitRoot}/${f.draft.visitId}/photos/initialize`).send({ submissionQuestionId: q.id }).expect(404);
      await f.sm("post", `${f.visitRoot}/${f.draft.visitId}/submit`).send({ visitStartedAt: "2026-10-19T09:00:00Z", visitCompletedAt: "2026-10-19T09:15:00Z", clientMutationToken: randomUUID() }).expect(404);
      assert.equal(await history(), before);
    });
    await t.test("employee activity/time/profile hide the campaign; management and photo reporting retain it", async () => {
      assert.equal((await f.sm("get", "/sm/activity/completed").expect(200)).body.visits.length, 0);
      assert.equal((await f.sm("get", timesUrl).expect(200)).body.entries.length, 0);
      await f.sm("get", `/sm/smdurcharbeit-times/${f.completed.visitId}/history`).expect(404);
      const profile = (await f.sm("get", "/sm/planning/profile").expect(200)).body;
      assert.equal(profile.summary.SMDurcharbeitVisitCount, 0);
      assert.equal((await f.admin("get", "/admin/sm-smdurcharbeit-times?from=2026-10-01&to=2026-10-31").expect(200)).body.entries.length, 1);
      const report = (await f.admin("get", `${campaignUrl}/results?month=2026-10-01`).expect(200)).body;
      assert.equal(report.summary.physicalVisits, 1); assert.equal(report.summary.availablePhotoUploads, 1);
      assert.equal(f.objects.size, 1); assert.equal(await history(), before);
    });
    await t.test("extension and passing dates cannot implicitly unpause a campaign", async () => {
      await f.admin("post", `${campaignUrl}/extend`).send({ expectedRevision: (await campaign()).revision, endDate: "2026-12-31", reason: "Synthetic extension", reactivate: true }).expect(409);
      await f.admin("post", `${campaignUrl}/extend`).send({ expectedRevision: (await campaign()).revision, endDate: "2026-12-31", reason: "Synthetic extension", reactivate: false }).expect(200);
      assert.equal((await campaign()).status, "paused");
      const december = await list("2026-12-01"); assert.equal(december.targets.length, 0); assert.ok(!december.months.includes("2026-12-01"));
      f.setNow("2026-10-26T10:00:00Z");
      assert.ok((await list()).targets.every((row: any) => row.campaignId !== f.campaign.id));
      assert.equal((await campaign()).status, "paused"); assert.equal(await history(), before);
    });
    await t.test("explicit resume restores the exact saved visits, progress and photos", async () => {
      await state("published");
      assert.equal((await list()).targets.length, 6);
      assert.ok((await list("2026-12-01")).months.includes("2026-12-01"));
      const resumed = (await f.sm("get", `${f.visitRoot}/${f.draft.visitId}`).expect(200)).body;
      assert.deepEqual(resumed.sections, draft.sections); assert.deepEqual(resumed.answers, draft.answers);
      const target = (await list()).targets.find((row: any) => row.id === f.targets[1].id);
      const replayed = (await f.sm("post", `${f.employeeRoot}/${target.id}/start`).send({ expectedRevision: target.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }).expect(200)).body;
      assert.equal(replayed.visitId, f.draft.visitId);
      assert.equal((await f.sm("get", "/sm/activity/completed").expect(200)).body.visits.length, 1);
      assert.equal((await f.sm("get", timesUrl).expect(200)).body.entries.length, 1);
      assert.equal(await history(), before);
    });
    await t.test("if every campaign is paused, no employee target or campaign month remains", async () => {
      await state("paused");
      const other = (await f.admin("get", f.root).expect(200)).body.campaigns.find((row: any) => row.id === f.otherCampaign.id);
      await f.admin("patch", `${f.root}/${other.id}/state`).send({ expectedRevision: other.revision, status: "paused", reason: "Synthetic all paused" }).expect(200);
      const empty = await list(); assert.deepEqual(empty.targets, []); assert.deepEqual(empty.months, []);
      assert.equal((await f.admin("get", `${campaignUrl}/targets?month=2026-10-01`).expect(200)).body.targets.length, 3);
      assert.equal(await history(), before);
    });
  } finally { await f.pg.close(); }
});
