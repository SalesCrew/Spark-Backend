import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import request from "supertest";
import { eq } from "drizzle-orm";
import { spezialfragenPeriodFixture } from "../tests/spezialfragen-period-fixture.js";

test("questionnaire writes preserve links on missing, accidental-empty and stale payloads; deliberate edits still work", async () => {
  const f = await spezialfragenPeriodFixture(new Date("2026-11-03T10:00:00Z"));
  try {
    const admin = (method: "get" | "post" | "patch", path: string) => request(f.app)[method](path).auth("synthetic-gm-admin", { type: "bearer" });
    const moduleIds: string[] = [];
    for (const name of ["First", "Second"]) {
      const result = await admin("post", "/admin/modules/main").send({ name, questions: [{ type: "numeric", text: name, required: false, config: {}, rules: [], scoring: {} }] }).expect(201);
      moduleIds.push(result.body.module.id);
    }
    const form = (await admin("post", "/admin/fragebogen/main").send({ name: "Synthetic Standard", moduleIds }).expect(201)).body.fragebogen;
    const path = `/admin/fragebogen/main/${form.id}`;
    const before = await f.database.select().from(f.schema.fragebogenMainModule);
    await admin("patch", path).send({ name: form.name, updatedAt: form.updatedAt }).expect(400);
    await admin("patch", path).send({ name: form.name, updatedAt: form.updatedAt, moduleIds: [] }).expect(409);
    await admin("patch", path).send({ name: form.name, moduleIds }).expect(409);
    assert.deepEqual(await f.database.select().from(f.schema.fragebogenMainModule), before);
    const saved = (await admin("patch", path).send({ name: form.name, updatedAt: form.updatedAt, moduleIds: [...moduleIds].reverse() }).expect(200)).body.fragebogen;
    assert.deepEqual(saved.moduleIds, [...moduleIds].reverse());
    assert.notEqual(saved.updatedAt, form.updatedAt);
    await admin("patch", path).send({ name: form.name, updatedAt: form.updatedAt, moduleIds }).expect(409);
    const removed = (await admin("patch", path).send({ name: form.name, updatedAt: saved.updatedAt, moduleIds: [], moduleRemovalConfirmed: true }).expect(200)).body.fragebogen;
    assert.deepEqual(removed.moduleIds, []);
  } finally { await f.pg.close(); }
});

test("Standard Distribution survives the RED-month boundary and recovers only untouched fields in the same draft", async () => {
  const f = await spezialfragenPeriodFixture(new Date("2026-10-20T10:00:00Z"), false, true);
  try {
    const admin = (method: "get" | "post" | "patch", path: string) => request(f.app)[method](path).auth("synthetic-gm-admin", { type: "bearer" });
    const gm = (method: "get" | "post" | "patch", path: string) => request(f.app)[method](path).auth("synthetic-gm", { type: "bearer" });
    const questions = Array.from({ length: 7 }, (_, n) => ({ type: "numeric", text: `Stock ${n}`, required: false, config: {}, rules: [], scoring: {} }));
    const module = (await admin("post", "/admin/modules/main").send({ name: "Distributionsziel Q4/2026", questions }).expect(201)).body.module;
    const regularModule = (await admin("post", "/admin/modules/main").send({ name: "Other monthly questions", questions: [questions[0]] }).expect(201)).body.module;
    const form = (await admin("post", "/admin/fragebogen/main").send({ name: "Standard Q4", moduleIds: [module.id, regularModule.id], status: "active" }).expect(201)).body.fragebogen;
    const campaign = randomUUID();
    await f.database.insert(f.schema.campaigns).values({ id: campaign, name: "Synthetic Standard", section: "standard", status: "active", scheduleType: "always", currentFragebogenId: form.id });
    await f.database.insert(f.schema.campaignMarketAssignments).values({ campaignId: campaign, marketId: f.ids.market, gmUserId: f.ids.gm });
    const selection = { marketId: f.ids.market, campaignIds: [campaign] };
    const source = (await gm("post", "/gm/visit-sessions").send({ ...selection, clientSessionToken: "october-source" }).expect(res => assert.equal(res.status, 201, JSON.stringify(res.body)))).body;
    const originalQuestions = source.sections[0].questions;
    for (const [index, question] of originalQuestions.entries()) {
      await gm("patch", `/gm/visit-sessions/${source.session.id}/answers`).send({ visitQuestionId: question.id, answer: index === 0 ? 0 : index + 1 }).expect(200);
    }
    await f.database.update(f.schema.visitSessions).set({ status: "submitted", submittedAt: new Date("2026-10-20T10:30:00Z") }).where(eq(f.schema.visitSessions.id, source.session.id));
    const sourceAnswers = await f.database.select().from(f.schema.visitAnswers);
    await f.database.insert(f.schema.visitAnswerPhotos).values({ visitAnswerId: sourceAnswers[0]!.id, storageBucket: "synthetic", storagePath: "historical/photo.jpg" });
    const sourcePhotos = await f.database.select().from(f.schema.visitAnswerPhotos);

    f.setTime("2026-11-03T10:00:00Z");
    const created = (await gm("post", "/gm/visit-sessions").send({ ...selection, clientSessionToken: "november-draft" }).expect(201)).body;
    const url = `/gm/visit-sessions/${created.session.id}`;
    const read = (await gm("get", url).expect(200)).body;
    const distribution = read.sections[0].questions.slice(0, 7);
    assert.equal(distribution.length, 7);
    assert.equal(Number(distribution[0].answer.valueNumber), 0, "explicit zero is carried");
    assert.ok(distribution.every((q: any) => q.answer?.answerStatus === "answered"));
    assert.equal(read.sections[0].questions[7].answer, null, "non-Distribution remains monthly");
    assert.deepEqual(await f.database.select().from(f.schema.visitAnswerPhotos), sourcePhotos, "no photos copied or changed");

    // Simulate an existing pre-fix empty draft in this disposable database.
    await f.database.delete(f.schema.visitAnswers).where(eq(f.schema.visitAnswers.visitSessionId, created.session.id));
    const field = (n: number) => distribution[n].id;
    await gm("patch", `${url}/answers`).send({ visitQuestionId: field(1), answer: 0 }).expect(200);
    await gm("patch", `${url}/answers`).send({ visitQuestionId: field(2), answer: null }).expect(200);
    await gm("patch", `${url}/answers`).send({ visitQuestionId: field(3), answer: "invalid input" }).expect(200);
    await gm("patch", `${url}/answers`).send({ visitQuestionId: field(4), comment: "Keep my comment" }).expect(200);
    await f.database.insert(f.schema.visitAnswers).values({ visitSessionId: created.session.id, visitSessionSectionId: read.sections[0].id, visitSessionQuestionId: field(5), questionId: distribution[5].questionId, questionType: "numeric", isDeleted: true, deletedAt: new Date() });
    const allBefore = await f.database.select().from(f.schema.visitAnswers);
    const commentsBefore = await f.database.select().from(f.schema.visitQuestionComments);
    assert.equal((await gm("get", url).expect(200)).body.sections[0].questions[0].answer, null, "GET never repairs data");
    await request(f.app).post(`${url}/resume-quarter-answers`).expect(401);
    await request(f.app).post(`/gm/visit-sessions/${randomUUID()}/resume-quarter-answers`).auth("synthetic-gm", { type: "bearer" }).expect(404);
    const ownId = f.ids.gm;
    f.ids.gm = randomUUID();
    await gm("post", `${url}/resume-quarter-answers`).expect(404);
    f.ids.gm = ownId;
    const recovered = (await gm("post", `${url}/resume-quarter-answers`).expect(200)).body;
    assert.equal(recovered.recoveredAnswers, 2);
    const after = (await gm("get", url).expect(200)).body;
    assert.equal(after.session.id, created.session.id);
    assert.equal(Number(after.sections[0].questions[0].answer.valueNumber), 0);
    assert.equal(Number(after.sections[0].questions[6].answer.valueNumber), 7);
    const allAfter = await f.database.select().from(f.schema.visitAnswers);
    for (const old of allBefore) assert.deepEqual(allAfter.find(row => row.id === old.id), old, "never overwrite existing or historical answers");
    assert.deepEqual(await f.database.select().from(f.schema.visitQuestionComments), commentsBefore);
    assert.deepEqual(await f.database.select().from(f.schema.visitAnswerPhotos), sourcePhotos);
    assert.equal((await gm("post", `${url}/resume-quarter-answers`).expect(200)).body.recoveredAnswers, 0, "idempotent resume");
    await f.database.delete(f.schema.visitAnswers).where(eq(f.schema.visitAnswers.visitSessionQuestionId, field(0)));
    const matchedDraft = (await gm("post", "/gm/visit-sessions").send(selection).expect(200)).body;
    assert.equal(matchedDraft.session.id, created.session.id);
    assert.equal(Number((await gm("get", url).expect(200)).body.sections[0].questions[0].answer.valueNumber), 0);
    await f.database.delete(f.schema.visitAnswers).where(eq(f.schema.visitAnswers.visitSessionQuestionId, field(6)));
    const resumed = (await gm("post", "/gm/visit-sessions").send({ ...selection, clientSessionToken: "november-draft" }).expect(200)).body;
    assert.equal(resumed.session.id, created.session.id);
    assert.equal(Number((await gm("get", url).expect(200)).body.sections[0].questions[6].answer.valueNumber), 7);
    for (const old of sourceAnswers) assert.deepEqual(allAfter.find(row => row.id === old.id), old);
    const finished = (await gm("post", `${url}/submit`).send({}).expect(200)).body;
    assert.equal(finished.sessionId, created.session.id, "finish the same recovered visit without restarting");
    assert.equal(finished.status, "submitted");
    assert.equal((await gm("post", `${url}/resume-quarter-answers`).expect(200)).body.recoveredAnswers, 0, "submitted history is immutable");
  } finally { await f.pg.close(); }
});
