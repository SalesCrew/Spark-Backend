import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";
import { createSyntheticPhotoStorage, syntheticShelfPhoto } from "../tests/synthetic-photo-storage.js";

class Clock extends Date {
  constructor(value?: string | number | Date) { super(value === undefined ? "2026-10-09T12:00:00Z" : value instanceof Date ? value.getTime() : value); }
  static now() { return Date.parse("2026-10-09T12:00:00Z"); }
}

async function fixture(monthly = true) {
  const storage = createSyntheticPhotoStorage();
  const f = await createSMDurcharbeitFixture({ clock: Clock as typeof Date, photoStorage: storage.storage });
  const admin = (method: "get" | "post", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
  const sm = (method: "get" | "post" | "put", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" });
  try {
    f.app.use("/synthetic-photo-storage", storage.router);
    const question = (type: string, required = false) => ({ id: `new-${randomUUID()}`, text: `Synthetic ${type}`, type, required, options: [], config: {}, rules: [] });
    const module = (await admin("post", "/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({
      id: `new-${randomUUID()}`, name: "Synthetic version recovery", description: "", questions: [question("yesno", true), question("text", true), question("photo", true), question("text")],
    }).expect(201)).body.module;
    const form = (await admin("post", "/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({
      id: `new-${randomUUID()}`, name: "Synthetic version recovery", description: "", status: "active", moduleIds: [module.id],
    }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    let visitId: string;
    if (monthly) {
      await f.database.insert(f.schema.smSMDurcharbeitMarkets).values({ smMarketId: f.market, SMDurcharbeitSmUserId: f.employee });
      const campaign = (await admin("post", "/admin/sm-smdurcharbeit-campaigns").send({
        name: "Synthetic recovery", startDate: "2026-10-01", endDate: "2026-10-31", questionnaireVersionId: version!.id,
        rosterDraft: [{ smMarketId: f.market, smUserId: f.employee }],
      }).expect(201)).body.campaign;
      const base = `/admin/sm-smdurcharbeit-campaigns/${campaign.id}`;
      const preview = (await admin("get", `${base}/preview`).expect(200)).body;
      await admin("post", `${base}/publish`).send({ expectedRevision: campaign.revision, previewToken: preview.previewToken }).expect(200);
      const target = (await sm("get", "/sm/smdurcharbeit/targets").expect(200)).body.targets[0];
      visitId = (await sm("post", `/sm/smdurcharbeit/targets/${target.id}/start`).send({
        expectedRevision: target.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID(),
      }).expect(201)).body.visitId;
    } else {
      const [assignment] = await f.database.insert(f.schema.smAssignments).values({ idempotencyKey: randomUUID(), sourceType: "single", status: "planned",
        originalWorkDate: "2026-10-09", originalSmUserId: f.employee, originalSmMarketId: f.market, originalMarketInternalId: "SYNTHETIC-1", originalPlannedMinutes: 15,
        SMDurcharbeitQuestionnaireOverrideVersionId: version!.id, createdByUserId: f.admin, updatedByUserId: f.admin }).returning();
      visitId = assignment!.id;
      await sm("post", `/sm/visits/${visitId}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }).expect(200);
    }
    const path = monthly ? `/sm/smdurcharbeit/visits/${visitId}` : `/sm/visits/${visitId}`;
    const read = async () => (await sm("get", path).expect(200)).body;
    const payload = await read();
    const [parent, conditional, photo, other] = payload.sections[0].questions;
    const save = async (questionId: string, answer: unknown, expectedAnswerVersion?: number, status = 200) => {
      const expected = expectedAnswerVersion ?? (await read()).answerVersions[questionId];
      return sm("put", `${path}/answers/${questionId}`).send({ answer, expectedAnswerVersion: expected, clientMutationToken: randomUUID() }).expect(status);
    };
    const choose = (label: string) => save(parent.id, { kind: "choice", optionCode: parent.options.find((option: { label: string }) => option.label === label).code });
    const hide = async (questionId: string) => {
      const [trigger] = await f.database.select().from(f.schema.smQuestionnaireSubmissionQuestions).where(eq(f.schema.smQuestionnaireSubmissionQuestions.id, parent.id));
      const [target] = await f.database.select().from(f.schema.smQuestionnaireSubmissionQuestions).where(eq(f.schema.smQuestionnaireSubmissionQuestions.id, questionId));
      await f.database.update(f.schema.smQuestionnaireSubmissionQuestions).set({ logicRulesSnapshot: [{
        triggerQuestionId: trigger!.questionCodeSnapshot, operator: "equals", triggerValue: "Nein", action: "hide", targetQuestionIds: [target!.questionCodeSnapshot],
      }] }).where(eq(f.schema.smQuestionnaireSubmissionQuestions.id, parent.id));
      await choose("Nein");
      assert.equal((await read()).sections[0].questions.find((q: { id: string }) => q.id === questionId).applicable, false);
    };
    const upload = async (blue: boolean) => {
      const answerId = (await sm("post", `${path}/photos/initialize`).send({ submissionQuestionId: photo.id }).expect(200)).body.answerId;
      const upload = (await sm("post", `${path}/photos/presign`).send({ answerId, extension: "png" }).expect(200)).body.upload;
      const bytes = syntheticShelfPhoto(blue);
      storage.put(upload.path, bytes);
      const fileId = (await sm("post", `${path}/photos/commit`).send({ answerId, photos: [{ storageBucket: upload.bucket, storagePath: upload.path, mimeType: "image/png", byteSize: bytes.length }] }).expect(200)).body.fileIds[0];
      return { answerId, fileId, bytes, storagePath: upload.path };
    };
    const submit = () => sm("post", `${path}/submit`).send({ visitStartedAt: "2026-10-09T08:00:00Z", visitCompletedAt: "2026-10-09T08:15:00Z", clientMutationToken: randomUUID() }).expect(200);
    const history = (questionId: string) => f.database.select().from(f.schema.smQuestionAnswers).where(eq(f.schema.smQuestionAnswers.submissionQuestionId, questionId));
    return { ...f, admin, sm, path, read, save, choose, hide, upload, submit, history, payload, parent, conditional, photo, other };
  } catch (error) { await f.pg.close(); throw error; }
}

test("existing monthly draft resumes after hide/show, retains saved photos and submits without restarting", async () => {
  const f = await fixture();
  try {
    await f.choose("Ja");
    await f.save(f.conditional.id, { kind: "text", value: "Original answer" });
    await f.save(f.conditional.id, { kind: "text", value: "Second historical answer" });
    await f.save(f.other.id, { kind: "text", value: "Keep this answer" });
    const photo = await f.upload(true);
    const frozenFiles = await f.database.select().from(f.schema.smQuestionAnswerFiles);
    const frozenOther = await f.history(f.other.id);
    await f.hide(f.conditional.id);
    await f.choose("Ja");
    const before = await f.read(), frozenHistory = await f.history(f.conditional.id);
    assert.equal(before.answerVersions[f.conditional.id], 0, "Existing blocked clients have no current revision");
    const result = await f.save(f.conditional.id, { kind: "text", value: "Recovered answer" }, 0);
    assert.equal(result.body.answerVersion, 3, "Append after all historical versions, even without a current answer");
    const recovered = (await f.history(f.conditional.id)).find(row => row.isCurrent)!;
    assert.equal(recovered.supersedesAnswerId, frozenHistory.find(row => row.answerVersion === 2)!.id);
    assert.deepEqual((await f.history(f.conditional.id)).filter(row => row.id !== recovered.id), frozenHistory);
    const frozenRecovered = await f.history(f.conditional.id);
    const stale = await f.save(f.conditional.id, { kind: "text", value: "Stale device" }, 0, 409);
    assert.equal(stale.body.code, "sm_visit_answer_version_conflict");
    assert.deepEqual(await f.history(f.conditional.id), frozenRecovered, "Stale saves do not alter history");
    const retry = await f.save(f.conditional.id, { kind: "text", value: "Recovered answer" }, 0);
    assert.deepEqual(retry.body, { saved: false, answerVersion: 3 }, "A lost response can be retried without another revision");
    await f.submit();
    const after = await f.read();
    assert.equal(after.assignment.id, before.assignment.id);
    assert.equal(after.submission.id, before.submission.id);
    assert.equal(after.submission.status, "submitted");
    assert.equal(after.answers[f.other.id].value, "Keep this answer");
    assert.deepEqual(after.answers[f.photo.id].fileIds, [photo.fileId]);
    assert.deepEqual(await f.database.select().from(f.schema.smQuestionAnswerFiles), frozenFiles);
    assert.deepEqual(await f.history(f.other.id), frozenOther);
    assert.deepEqual((await f.history(f.conditional.id)).filter(row => row.id !== recovered.id), frozenHistory);
    const image = await request(f.app).get(`/synthetic-photo-storage/object?path=${encodeURIComponent(photo.storagePath)}`).expect(200);
    assert.deepEqual(image.body, photo.bytes, "Existing photo bytes remain intact");
    assert.equal((await f.database.select().from(f.schema.smSMDurcharbeitVisits)).length, 1);
    assert.equal((await f.database.select().from(f.schema.smQuestionnaireSubmissions)).length, 1);
  } finally { await f.pg.close(); }
});

test("reappearing photo questions append revisions and preserve every historical photo object", async () => {
  const f = await fixture();
  try {
    await f.choose("Ja");
    await f.save(f.conditional.id, { kind: "text", value: "Still answered" });
    const original = await f.upload(true);
    const originalFiles = await f.database.select().from(f.schema.smQuestionAnswerFiles);
    await f.hide(f.photo.id);
    await f.choose("Ja");
    const originalHistory = await f.history(f.photo.id);
    const next = await f.upload(false);
    assert.notEqual(next.answerId, original.answerId);
    const current = (await f.history(f.photo.id)).find(row => row.isCurrent)!;
    assert.equal(current.answerVersion, 2);
    assert.equal(current.supersedesAnswerId, original.answerId);
    assert.deepEqual((await f.history(f.photo.id)).filter(row => row.id === original.answerId), originalHistory);
    const initialized = await f.sm("post", `${f.path}/photos/initialize`).send({ submissionQuestionId: f.photo.id }).expect(200);
    assert.equal(initialized.body.answerId, next.answerId, "Retrying initialization reuses the current answer");
    assert.equal((await f.history(f.photo.id)).length, 2);
    const events = await f.database.select().from(f.schema.smQuestionAnswerEvents);
    assert.equal(events.find(row => row.answerId === next.answerId && (row.payload as { initializedForUpload?: boolean }).initializedForUpload)!.answerVersion, 2);
    await f.hide(f.photo.id);
    await f.choose("Ja");
    const last = await f.upload(true);
    assert.equal((await f.history(f.photo.id)).find(row => row.isCurrent)!.answerVersion, 3);
    await f.submit();
    const allFiles = await f.database.select().from(f.schema.smQuestionAnswerFiles);
    for (const file of originalFiles) assert.deepEqual(allFiles.find(row => row.id === file.id), file);
    for (const photo of [original, next, last]) {
      const image = await request(f.app).get(`/synthetic-photo-storage/object?path=${encodeURIComponent(photo.storagePath)}`).expect(200);
      assert.deepEqual(image.body, photo.bytes);
    }
    assert.equal((await f.read()).submission.id, f.payload.submission.id);
    assert.equal((await f.database.select().from(f.schema.smSMDurcharbeitVisits)).length, 1);
  } finally { await f.pg.close(); }
});

test("dated visits also recover after retained soft-deleted revisions without renumbering history", async () => {
  const f = await fixture(false);
  try {
    await f.choose("Ja");
    await f.save(f.conditional.id, { kind: "text", value: "Historical answer" });
    await f.upload(true);
    await f.hide(f.conditional.id);
    await f.choose("Ja");
    const [historical] = await f.history(f.conditional.id);
    await f.database.update(f.schema.smQuestionAnswers).set({ answerVersion: 7, isDeleted: true, deletedAt: new Date() }).where(eq(f.schema.smQuestionAnswers.id, historical!.id));
    const frozen = await f.history(f.conditional.id);
    assert.equal((await f.read()).answerVersions[f.conditional.id], 0);
    assert.equal((await f.save(f.conditional.id, { kind: "text", value: "New answer" }, 0)).body.answerVersion, 8);
    await f.submit();
    assert.equal((await f.read()).submission.id, f.payload.submission.id);
    assert.equal((await f.read()).submission.status, "submitted");
    assert.deepEqual((await f.history(f.conditional.id)).filter(row => row.id === historical!.id), frozen);
  } finally { await f.pg.close(); }
});
