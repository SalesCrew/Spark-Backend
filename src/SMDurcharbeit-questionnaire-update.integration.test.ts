import assert from "node:assert/strict";
import test from "node:test";
import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { createQuestionnaireUpdateFixture } from "../tests/SMDurcharbeit-questionnaire-update-fixture.js";

test("Durcharbeit update review → explicit adoption → new questions; original visits/answers/photos remain immutable", async t => {
  const f = await createQuestionnaireUpdateFixture();
  const url = `${f.root}/${f.campaign.id}/questionnaire-update`;
  const preview = async (month = "2026-10-01", token = "synthetic-sm-admin") => (await f.admin("get", `${url}?month=${month}`, token).expect(200)).body;
  const input = (p: any) => ({ month: p.month, expectedRevision: p.campaignRevision, previewToken: p.previewToken });
  const history = async () => JSON.stringify(await Promise.all([f.schema.smQuestionnaireSubmissions, f.schema.smQuestionnaireSubmissionSections,
    f.schema.smQuestionnaireSubmissionQuestions, f.schema.smQuestionAnswers, f.schema.smQuestionAnswerOptions, f.schema.smQuestionAnswerFiles,
    f.schema.smQuestionAnswerEvents, f.schema.smSMDurcharbeitVisits, f.schema.smSMDurcharbeitTimeRevisions].map(table => f.database.select().from(table as any))));
  try {
    let p = await preview();
    const initialHistory = await history();
    await t.test("module-only edits/deletions are detected; preview/cancel never publishes or changes a link", async () => {
      const before = JSON.stringify(await Promise.all([f.database.select().from(f.schema.smQuestionnaireVersions), f.database.select().from(f.schema.smSMDurcharbeitPeriods), f.database.select().from(f.schema.smSMDurcharbeitEvents)]));
      assert.equal(p.available, true); assert.equal(p.requiresPublication, true);
      assert.equal(p.current.questionCount, 7); assert.equal(p.next.questionCount, 4);
      assert.deepEqual(p.changes, { added: 1, removed: 4, changed: 1 });
      assert.deepEqual(p.preserved, { drafts: 1, completed: 1 });
      await preview();
      assert.equal(JSON.stringify(await Promise.all([f.database.select().from(f.schema.smQuestionnaireVersions), f.database.select().from(f.schema.smSMDurcharbeitPeriods), f.database.select().from(f.schema.smSMDurcharbeitEvents)])), before);
      assert.equal(await history(), initialHistory);
    });
    await t.test("read/update permissions and month/campaign/token validation are enforced atomically", async () => {
      await preview("2026-10-01", "synthetic-reader");
      for (const token of ["synthetic-sm", "synthetic-gm", "synthetic-reader", "synthetic-normal-editor"]) await f.admin("patch", url, token).send(input(p)).expect(403);
      await f.admin("patch", url).send({ ...input(p), expectedRevision: p.campaignRevision + 1 }).expect(409);
      await f.admin("patch", url).send({ ...input(p), previewToken: "0".repeat(64) }).expect(409);
      await f.admin("patch", `${f.root}/${f.otherCampaign.id}/questionnaire-update`).send(input(p)).expect(409);
      await f.admin("patch", url).send({ ...input(p), month: "2026-11-01" }).expect(409);
      await f.admin("get", `${url}?month=2026-09-01`).expect(409);
      await f.admin("get", `${url}?month=2026-10-02`).expect(400);
      assert.equal(await history(), initialHistory);
    });
    await t.test("further module edits invalidate a preview rather than adopting unseen content", async () => {
      await f.editModule(true);
      await f.admin("patch", url).send(input(p)).expect(409);
      p = await preview(); assert.equal(p.next.versionNumber, 2);
    });
    const oldPeriods = await f.database.select().from(f.schema.smSMDurcharbeitPeriods);
    let adopted: any;
    await t.test("confirmation publishes latest modules and updates only the selected month", async () => {
      adopted = (await f.admin("patch", url, "synthetic-editor").send(input(p)).expect(200)).body;
      assert.equal(adopted.questionnaire.versionNumber, 2);
      assert.equal(await history(), initialHistory);
      const periods = await f.database.select().from(f.schema.smSMDurcharbeitPeriods);
      for (const period of periods) {
        const before = oldPeriods.find(row => row.id === period.id)!;
        assert.equal(period.questionnaireVersionId, period.campaignId === f.campaign.id && period.month === "2026-10-01" ? adopted.questionnaire.id : before.questionnaireVersionId);
      }
      assert.equal((await preview()).available, false);
      await f.admin("patch", url).send(input(p)).expect(409);
    });
    await t.test("an existing draft resumes its old questions; a fresh start receives the new graph", async () => {
      const resumed = (await f.sm("get", `${f.visitRoot}/${f.draft.visitId}`).expect(200)).body;
      assert.equal(resumed.sections[0].questions.length, 7);
      assert.equal(resumed.SMDurcharbeitQuestionnaireSelection.versionNumber, 1);
      const newVisit = await f.start(f.targets[2].id);
      const current = (await f.sm("get", `${f.visitRoot}/${newVisit.visitId}`).expect(200)).body;
      assert.equal(current.sections[0].questions.length, 4);
      assert.ok(!current.sections[0].questions.some((q: any) => q.text.startsWith("Entfernte Frage")));
      assert.ok(current.sections[0].questions.some((q: any) => q.text === "Erneut aktualisierte Notiz"));
      assert.ok(Object.values(current.answers).every(value => value === null));
      await f.sm("post", `${f.visitRoot}/${newVisit.visitId}/submit`).send({ visitStartedAt: "2026-10-19T09:00:00Z", visitCompletedAt: "2026-10-19T09:20:00Z", clientMutationToken: randomUUID() }).expect(200);
      const submitted = (await f.sm("get", `${f.visitRoot}/${newVisit.visitId}`).expect(200)).body;
      assert.equal(submitted.submission.status, "submitted");
      assert.equal(submitted.sections[0].questions.length, 4);
    });
    await t.test("follow-ups carry only unchanged question versions/photos; edited and new answers stay blank", async () => {
      const followUp = await f.start(f.targets[0].id, true);
      const current = (await f.sm("get", `${f.visitRoot}/${followUp.visitId}`).expect(200)).body;
      const byText = Object.fromEntries(current.sections[0].questions.map((q: any) => [q.text, q]));
      assert.equal(current.answers[byText["Erneut aktualisierte Notiz"].id], null);
      assert.equal(current.answers[byText["Neue Kontrollfrage"].id], null);
      assert.equal(current.answers[byText["Bestand geprüft"].id].kind, "choice");
      assert.equal(current.photoFiles[byText.Regalfoto.id][0].id, f.photoId);
      const original = (await f.sm("get", `${f.visitRoot}/${f.completed.visitId}`).expect(200)).body;
      assert.equal(original.sections[0].questions.length, 7);
      assert.equal(original.photoFiles[original.sections[0].questions.find((q: any) => q.text === "Regalfoto").id][0].id, f.photoId);
      assert.equal(f.objects.size, 1);
    });
    await t.test("another month can adopt an already published latest questionnaire without creating a duplicate", async () => {
      const november = await preview("2026-11-01");
      assert.equal(november.requiresPublication, false); assert.equal(november.next.id, adopted.questionnaire.id);
      const count = (await f.database.select().from(f.schema.smQuestionnaireVersions)).length;
      await f.admin("patch", url).send(input(november)).expect(200);
      assert.equal((await f.database.select().from(f.schema.smQuestionnaireVersions)).length, count);
      assert.equal((await f.admin("get", `${f.root}/${f.otherCampaign.id}/questionnaire-update?month=2026-10-01`).expect(200)).body.available, true);
    });
    await t.test("inactive/archived campaigns reject adoption without publishing or changing history", async () => {
      const otherUrl = `${f.root}/${f.otherCampaign.id}/questionnaire-update`;
      const other = (await f.admin("get", `${otherUrl}?month=2026-10-01`).expect(200)).body;
      await f.database.update(f.schema.smSMDurcharbeitCampaigns).set({ status: "archived" }).where(eq(f.schema.smSMDurcharbeitCampaigns.id, f.otherCampaign.id));
      await f.admin("patch", otherUrl).send(input(other)).expect(409);
      await f.database.update(f.schema.smQuestionnaireTemplates).set({ status: "inactive" }).where(eq(f.schema.smQuestionnaireTemplates.id, f.form.id));
      await f.admin("get", `${url}?month=2026-10-01`).expect(409);
    });
  } finally { await f.pg.close(); }
});

test("a new start invalidates the reviewed preservation counts before adoption", async () => {
  const f = await createQuestionnaireUpdateFixture();
  try {
    const url = `${f.root}/${f.campaign.id}/questionnaire-update`;
    const old = (await f.admin("get", `${url}?month=2026-10-01`).expect(200)).body;
    const started = await f.start(f.targets[2].id);
    await f.admin("patch", url).send({ month: old.month, expectedRevision: old.campaignRevision, previewToken: old.previewToken }).expect(409);
    const fresh = (await f.admin("get", `${url}?month=2026-10-01`).expect(200)).body;
    assert.equal(fresh.preserved.drafts, 2);
    await f.admin("patch", url).send({ month: fresh.month, expectedRevision: fresh.campaignRevision, previewToken: fresh.previewToken }).expect(200);
    const retained = (await f.sm("get", `${f.visitRoot}/${started.visitId}`).expect(200)).body;
    assert.equal(retained.sections[0].questions.length, 7);
    assert.equal(retained.SMDurcharbeitQuestionnaireSelection.versionNumber, 1);
  } finally { await f.pg.close(); }
});

test("native PostgreSQL serializes adoption against starts and duplicate confirmations", { skip: !process.env.SMDURCHARBEIT_SYNTHETIC_PG_SOCKET_DIR, timeout: 30_000 }, async t => {
  const f = await createQuestionnaireUpdateFixture();
  assert.equal(f.nativePostgres, true);
  try {
    const url = `${f.root}/${f.campaign.id}/questionnaire-update`;
    const p = (await f.admin("get", `${url}?month=2026-10-01`).expect(200)).body;
    const target = (await f.list()).find((row: any) => row.id === f.targets[2].id);
    const [updated, started] = await Promise.all([
      f.admin("patch", url).send({ month: p.month, expectedRevision: p.campaignRevision, previewToken: p.previewToken }),
      f.sm("post", `${f.employeeRoot}/${target.id}/start`).send({ expectedRevision: target.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }),
    ]);
    await t.test("each start receives a complete old or new graph consistent with the winning transaction", async () => {
      assert.equal(started.status, 201, JSON.stringify(started.body));
      assert.ok([200, 409].includes(updated.status), JSON.stringify(updated.body));
      const graph = (await f.sm("get", `${f.visitRoot}/${started.body.visitId}`).expect(200)).body;
      assert.equal(graph.sections[0].questions.length, updated.status === 200 ? 4 : 7);
      assert.equal(graph.SMDurcharbeitQuestionnaireSelection.versionNumber, updated.status === 200 ? 2 : 1);
      const original = (await f.sm("get", `${f.visitRoot}/${f.completed.visitId}`).expect(200)).body;
      assert.equal(original.sections[0].questions.length, 7);
      assert.equal(f.objects.size, 1);
    });
    await t.test("competing confirmations create exactly one published questionnaire version", async () => {
      const otherUrl = `${f.root}/${f.otherCampaign.id}/questionnaire-update`;
      const other = (await f.admin("get", `${otherUrl}?month=2026-10-01`).expect(200)).body;
      const input = { month: other.month, expectedRevision: other.campaignRevision, previewToken: other.previewToken };
      const results = await Promise.all([f.admin("patch", otherUrl).send(input), f.admin("patch", otherUrl).send(input)]);
      assert.deepEqual(results.map(r => r.status).sort(), [200, 409]);
      assert.equal((await f.database.select().from(f.schema.smQuestionnaireVersions)).length, 2);
    });
  } finally { await f.pg.close(); }
});
