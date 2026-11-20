import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { createOptionalCommentsFixture } from "../tests/sm-optional-comments-fixture.js";
import { smOptionalCommentsForSubmission } from "./sm-comment-context.shared.js";

for (const dated of [false, true]) test(`${dated ? "dated" : "monthly"} Durcharbeit saves optional notes before answers and photos, preserves uploads and historical work`, async () => {
  const f = await createOptionalCommentsFixture(dated);
  try {
    const qs = f.payload.sections[0].questions;
    assert.ok(qs.every((q: any) => q.optionalCommentEnabled === true));
    assert.ok(qs.every((q: any) => q.questionCode.startsWith("q_")), "Opaque question IDs do not determine questionnaire scope");
    const first = qs[0], photoQ = qs.find((q: any) => q.type === "photo"), required = qs.find((q: any) => q.text === "Pflichtkommentar bei Nein"), noteOnly = qs.find((q: any) => q.text === "Unbeantwortete optionale Frage");
    await f.save(first.id, { kind: "empty", comment: "Vor dem Beantworten notiert" });
    let current = await f.read();
    assert.equal(current.answers[first.id].comment, "Vor dem Beantworten notiert"); assert.equal(current.submission.answeredQuestionCount, 0);
    const initialVersion = current.answerVersions[first.id];
    await f.save(first.id, { kind: "yesnomulti", optionCode: first.options[1].code, subOptions: ["Nicht vorgesehen"] });
    current = await f.read();
    assert.equal(current.answers[first.id].comment, undefined, "Explicitly omitted optional comments can be cleared");
    assert.equal(current.submission.answeredQuestionCount, 1);
    await f.save(first.id, { kind: "yesnomulti", optionCode: first.options[1].code, subOptions: ["Nicht vorgesehen"], comment: "Darf den aktuellen Stand nicht überschreiben" }, initialVersion, 409);
    assert.equal((await f.read()).answers[first.id].comment, undefined);
    await f.save(first.id, { kind: "empty", comment: "x", optionalCommentEnabled: true }, undefined, 400);
    await f.save(photoQ.id, { kind: "empty", comment: "Foto folgt" });
    assert.equal((await f.read()).answers[photoQ.id].comment, "Foto folgt");
    let photo = await f.upload(photoQ.id);
    current = await f.read(); assert.equal(current.answers[photoQ.id].comment, "Foto folgt");
    const filesBefore = await f.database.select().from(f.schema.smQuestionAnswerFiles);
    await f.save(photoQ.id, { kind: "photo", fileIds: [photo.id], comment: "Regalfoto geprüft" });
    assert.deepEqual(await f.database.select().from(f.schema.smQuestionAnswerFiles), filesBefore);
    await f.save(photoQ.id, { kind: "empty", comment: "Dateien versehentlich ersetzen" }, undefined, 409);
    assert.deepEqual((await f.read()).answers[photoQ.id].fileIds, [photo.id]);
    await f.sm("delete", `${f.path}/photos/${photo.id}`).expect(204);
    current = await f.read(); assert.equal(current.answers[photoQ.id].comment, "Regalfoto geprüft"); assert.equal(current.answers[photoQ.id].kind, "empty");
    photo = await f.upload(photoQ.id); assert.equal((await f.read()).answers[photoQ.id].comment, "Regalfoto geprüft");
    await f.save(noteOnly.id, { kind: "empty", comment: "Abstimmung folgt" });
    if (dated) {
      const [standard] = await f.database.insert(f.schema.smQuestionnaireTemplates).values({ stableCode: `standard_${randomUUID().replaceAll("-", "")}`, status: "active" }).returning();
      assert.equal(await smOptionalCommentsForSubmission(f.database as any, { SMDurcharbeitVisitId: null, questionnaireTemplateId: standard!.id }), false);
    } else {
      for (const q of qs.filter((q: any) => ![first.id, photoQ.id, required.id, noteOnly.id].includes(q.id))) {
        const value = q.type === "single" || q.type === "likert" ? { kind: "choice", optionCode: q.options[0].code }
          : q.type === "multiple" ? { kind: "multi", optionCodes: [q.options[0].code] }
          : q.type === "text" ? { kind: "text", value: "Antwort" }
          : q.type === "numeric" ? { kind: "number", value: 0 }
          : q.type === "slider" ? { kind: "number", value: 25 }
          : { kind: "matrix", cells: [{ rowCode: "row_1", columnCode: "column_2", selected: true }] };
        await f.save(q.id, { ...value, comment: `Freiwillig: ${q.type}` });
        assert.equal((await f.read()).answers[q.id].comment, `Freiwillig: ${q.type}`);
      }
      await f.save(required.id, { kind: "choice", optionCode: required.options[1].code });
      const timing = { visitStartedAt: "2026-11-20T09:00:00Z", visitCompletedAt: "2026-11-20T09:20:00Z", clientMutationToken: randomUUID() };
      await f.sm("post", `${f.path}/submit`).send(timing).expect(409);
      await f.save(required.id, { kind: "choice", optionCode: required.options[1].code, comment: "Verbindliche Erläuterung" });
      await f.sm("post", `${f.path}/submit`).send(timing).expect(200);
      current = await f.read(); assert.equal(current.submission.answeredQuestionCount, 10); assert.equal(current.answers[noteOnly.id].comment, "Abstimmung folgt");
      const review = `/admin/sm-smdurcharbeit-campaigns/${f.campaign.id}/review/2026-11-01/${current.submission.id}`;
      let detail = (await f.admin("get", review).expect(200)).body;
      const reviewed = detail.sections[0].questions.find((q: any) => q.id === noteOnly.id);
      assert.equal(reviewed.optionalCommentEnabled, true); assert.equal(reviewed.answer.comment, "Abstimmung folgt"); assert.equal(reviewed.answerState, "unanswered");
      const result = (await f.admin("get", `/admin/sm-smdurcharbeit-campaigns/${f.campaign.id}/results?month=2026-11-01&includeVisitAnswers=true`).expect(200)).body;
      const exported = result.visitAnswers.find((r: any) => r.question.id === noteOnly.id);
      assert.equal(exported.answer.valueJson.comment, "Abstimmung folgt"); assert.equal(exported.question.optionalCommentEnabled, true); assert.equal(exported.answer.answerState, "unanswered");
      const allFiles = await f.database.select().from(f.schema.smQuestionAnswerFiles);
      await f.admin("post", `${review}/corrections`).send({ expectedVersion: detail.version, clientMutationToken: randomUUID(), reason: "Freiwillige Notiz ergänzen", uploads: [], changes: [{ questionId: noteOnly.id, answer: { kind: "empty", comment: "Im Admin geprüft" } }] }).expect(200);
      detail = (await f.admin("get", review).expect(200)).body;
      assert.equal(detail.sections[0].questions.find((q: any) => q.id === noteOnly.id).answer.comment, "Im Admin geprüft");
      assert.deepEqual(await f.database.select().from(f.schema.smQuestionAnswerFiles), allFiles);
      const follow = await f.start(f.secondMarket, true);
      const followPayload = (await f.sm("get", `/sm/smdurcharbeit/visits/${follow.visitId}`).expect(200)).body;
      const inherited = followPayload.sections[0].questions.find((q: any) => q.text === noteOnly.text);
      assert.equal(followPayload.answers[inherited.id].comment, "Im Admin geprüft"); assert.equal(followPayload.answers[inherited.id].kind, "empty");
    }
    assert.equal(await f.fingerprint(), f.originalFingerprint);
    const originalPhoto = await f.sm("get", `/synthetic-photo-storage/object?path=${encodeURIComponent(f.historicalPhoto.path)}`).expect(200);
    assert.deepEqual(originalPhoto.body, f.historicalPhoto.bytes);
  } finally { await f.pg.close(); }
});
