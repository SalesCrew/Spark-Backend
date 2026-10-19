import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { and, eq } from "drizzle-orm";
import { createQuestionnaireUpdateFixture } from "./SMDurcharbeit-questionnaire-update-fixture.js";

// Real HTTP routers and migrations, disposable synthetic DB only; no dotenv/application jobs.
test("Durcharbeit campaign visit review, search, corrections and scoped permissions", async t => {
  const f = await createQuestionnaireUpdateFixture();
  try {
    const base = `${f.root}/${f.campaign.id}/review/2026-10-01`;
    const submissionId = f.completed.submissionId;
    const detail = (await f.admin("get", `${base}/${submissionId}`).expect(200)).body;
    const questions = detail.sections.flatMap((s: any) => s.questions);
    const note = questions.find((q: any) => q.type === "text"), choice = questions.find((q: any) => q.type === "yesno");
    const input = { expectedVersion: detail.version, clientMutationToken: randomUUID(), reason: "Synthetic admin review", changes: [{ questionId: note.id, answer: { kind: "text", value: "Kontrolliert und korrigiert", comment: "Zusatzinformation" } }], uploads: [] };
    await t.test("completed records only; snapshot questions, answers and photos", async () => {
      const list = (await f.admin("get", base).expect(200)).body;
      assert.deepEqual(list.visits.map((v: any) => v.id), [submissionId]);
      assert.equal(list.visits[0].SMDurcharbeitContext.campaignId, f.campaign.id);
      assert.equal(questions.length, 7); assert.equal(note.answer.value, "Original note");
      assert.ok(questions.some((q: any) => q.photos.some((p: any) => p.id === f.photoId)));
      assert.equal((await f.admin("get", `${f.root}/${f.otherCampaign.id}/review/2026-10-01`).expect(200)).body.visits.length, 0);
    });
    await t.test("searches metadata, dates, selected answer labels and answer text", async () => {
      for (const search of ["Original note", "19.10.2026", "2026-10-19", "Ja", detail.visit.marketName]) {
        assert.equal((await f.admin("get", `${base}?search=${encodeURIComponent(search)}`).expect(200)).body.visits.length, 1, search);
      }
      assert.equal((await f.admin("get", `${base}?search=Nein`).expect(200)).body.visits.length, 0, "unselected labels must not match");
      assert.equal((await f.admin("get", `${base}?search=no-such-answer`).expect(200)).body.visits.length, 0);
      assert.equal((await f.admin("get", `${base}?smUserId=${f.employee}`).expect(200)).body.visits.length, 1);
      assert.equal((await f.admin("get", `${base}?marketId=${randomUUID()}`).expect(200)).body.visits.length, 0);
    });
    await t.test("planning readers can inspect; only planning updaters can correct", async () => {
      await f.admin("get", base, "synthetic-sm").expect(403);
      await f.admin("get", base, "synthetic-reader").expect(200);
      await f.admin("get", `${base}/${submissionId}/history?questionId=${note.id}`, "synthetic-reader").expect(200);
      await f.admin("post", `${base}/${submissionId}/corrections`, "synthetic-reader").send(input).expect(403);
      await f.admin("get", base, "synthetic-normal-editor").expect(403);
      await f.admin("post", `${base}/${submissionId}/corrections`, "synthetic-normal-editor").send(input).expect(403);
    });
    await t.test("rejects cross-campaign/month IDs, drafts, unknown periods and forged scope", async () => {
      const other = `${f.root}/${f.otherCampaign.id}/review/2026-10-01/${submissionId}`;
      for (const path of [other, `${f.root}/${f.campaign.id}/review/2026-11-01/${submissionId}`, `${base}/${f.draft.submissionId}`]) await f.admin("get", path).expect(404);
      await f.admin("get", `${other}/history?questionId=${note.id}`).expect(404);
      await f.admin("post", `${other}/corrections`).send(input).expect(404);
      await f.admin("post", `${other}/photos/upload-url`).send({ questionId: questions.find((q: any) => q.type === "photo").id, originalFileName: "test.png", mimeType: "image/png", byteSize: 10 }).expect(404);
      await f.admin("get", `${base}?SMDurcharbeitCampaignId=${f.otherCampaign.id}`).expect(400);
      await f.admin("get", `${base}?cursorId=${submissionId}`).expect(400);
      await f.admin("get", `${f.root}/${f.campaign.id}/review/2026-12-01`).expect(404);
      await f.admin("get", `${f.root}/${f.campaign.id}/review/2026-10-02`).expect(400);
    });
    await t.test("correction is versioned, audited, repeat-safe and rejects stale saves", async () => {
      await f.admin("post", `${base}/${submissionId}/corrections`, "synthetic-editor").send(input).expect(200);
      const replay = (await f.admin("post", `${base}/${submissionId}/corrections`, "synthetic-editor").send(input).expect(200)).body;
      assert.equal(replay.replayed, true);
      await f.admin("post", `${base}/${submissionId}/corrections`).send({ ...input, clientMutationToken: randomUUID() }).expect(409);
      const updated = (await f.admin("get", `${base}/${submissionId}`).expect(200)).body;
      assert.equal(updated.sections.flatMap((s: any) => s.questions).find((q: any) => q.id === note.id).answer.value, "Kontrolliert und korrigiert");
      assert.equal(updated.visit.startedAt, detail.visit.startedAt); assert.equal(updated.visit.completedAt, detail.visit.completedAt);
      const history = (await f.admin("get", `${base}/${submissionId}/history?questionId=${note.id}`).expect(200)).body;
      assert.equal(history.entries.length, 2); assert.equal(history.entries[0].reason, input.reason); assert.equal(history.entries[1].value.value, "Original note");
      assert.equal((await f.admin("get", `${base}?search=Original%20note`).expect(200)).body.visits.length, 0);
      assert.equal((await f.admin("get", `${base}?search=Kontrolliert`).expect(200)).body.visits.length, 1);
    });
    await t.test("repeat visits remain separate and keyset pages do not repeat records", async () => {
      const followUp = await f.start(f.targets[0].id, true);
      await f.sm("post", `${f.visitRoot}/${followUp.visitId}/submit`).send({ visitStartedAt: "2026-10-19T09:00:00Z", visitCompletedAt: "2026-10-19T09:20:00Z", clientMutationToken: randomUUID() }).expect(200);
      const first = (await f.admin("get", `${base}?limit=1`).expect(200)).body;
      assert.equal(first.visits.length, 1); assert.ok(first.nextCursor);
      const second = (await f.admin("get", `${base}?limit=1&cursorDate=${first.nextCursor.date}&cursorId=${first.nextCursor.id}`).expect(200)).body;
      assert.equal(second.visits.length, 1); assert.notEqual(first.visits[0].id, second.visits[0].id); assert.equal(second.nextCursor, null);
      assert.equal((await f.admin("get", base).expect(200)).body.visits.length, 2);
    });
    await t.test("campaign month includes visits whose corrected timestamps lie outside that month", async () => {
      await f.database.update(f.schema.smSMDurcharbeitTimeRevisions).set({ startedAt: new Date("2026-11-01T09:00:00Z"), completedAt: new Date("2026-11-01T09:20:00Z") }).where(and(eq(f.schema.smSMDurcharbeitTimeRevisions.visitId, f.completed.visitId), eq(f.schema.smSMDurcharbeitTimeRevisions.isCurrent, true)));
      const list = (await f.admin("get", base).expect(200)).body;
      assert.ok(list.visits.some((v: any) => v.id === submissionId && v.workDate === "2026-11-01"));
      assert.equal((await f.admin("get", `${f.root}/${f.campaign.id}/review/2026-11-01`).expect(200)).body.visits.length, 0);
    });
    assert.ok(choice);
  } finally { await f.pg.close(); }
});
