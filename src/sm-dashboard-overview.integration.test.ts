import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { and, eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("real authoring → frozen questions → ten answer types → submit → filtered dashboard; history remains intact", { timeout: 30_000 }, async t => {
  const objects = new Set<string>();
  const f = await createSMDurcharbeitFixture({ photoStorage: { from: () => ({
    createSignedUploadUrl: async (path: string) => ({ data: { path, signedUrl: `https://synthetic.invalid/${path}`, token: "synthetic" }, error: null }),
    list: async (folder: string, options: { search: string }) => ({ data: objects.has(`${folder}/${options.search}`) ? [{ name: options.search }] : [], error: null }),
    remove: async () => ({ data: [], error: null }),
  }) } });
  const admin = (method: "get" | "post" | "patch", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
  const sm = (method: "get" | "post" | "put" | "delete", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" });
  const types = ["single", "yesno", "yesnomulti", "multiple", "likert", "text", "numeric", "slider", "photo", "matrix"];
  const config = (type: string) => ({ ...(type === "single" || type === "multiple" ? { options: ["Erledigt", "Offen"] } : {}), ...(type === "yesnomulti" ? { answers: ["Kontrolliert", "Nicht kontrolliert"] } : {}),
    ...(type === "likert" ? { min: 1, max: 3 } : {}), ...(type === "matrix" ? { rows: ["Regal"], columns: ["Erledigt", "Offen"] } : {}),
    basicActivity: { enabled: true, ...(["single", "multiple", "matrix"].includes(type) ? { answerOutcomes: { Erledigt: "yes", Offen: "no" } } : type === "yesno" ? { answerOutcomes: { Ja: "partial", Nein: "no" } } : type === "yesnomulti" ? { answerOutcomes: { Kontrolliert: "yes" } } : type === "likert" ? { answerOutcomes: { "1": "no", "2": "partial", "3": "yes" } } : type === "text" ? { rules: [{ kind: "text", value: "Geprüft", outcome: "yes" }] } : type === "photo" ? { answeredOutcome: "yes" } : { rules: [{ kind: "range", min: 1, max: 10, outcome: "partial" }] }) } });
  const moduleInput = { id: `new-${randomUUID()}`, name: "Alle Basistätigkeiten-Typen", description: "Synthetischer Nachweis", questions: types.map(type => ({ id: type, text: `Aufgabe ${type}`, type, required: true, options: [], config: config(type), rules: [] })) };
  try {
    const created = (await admin("post", "/admin/sm-questionnaires/modules").send(moduleInput).expect(201)).body.module;
    const formInput = { id: `new-${randomUUID()}`, name: "Basistätigkeiten Test", description: "", status: "active", moduleIds: [created.id] };
    const form = (await admin("post", "/admin/sm-questionnaires/questionnaires").send(formInput).expect(201)).body.questionnaire;
    await f.database.insert(f.schema.smQuestionnaireGlobalAssignments).values({ questionnaireTemplateId: form.id, assignedByUserId: f.admin });
    async function open() {
      const [assignment] = await f.database.insert(f.schema.smAssignments).values({ sourceType: "single", idempotencyKey: randomUUID(), originalWorkDate: "2026-10-07", originalSmUserId: f.employee, originalSmMarketId: f.market, originalMarketInternalId: "SYNTHETIC", originalPlannedMinutes: 45, createdByUserId: f.admin, updatedByUserId: f.admin }).returning();
      const opened = (await sm("post", `/sm/visits/${assignment!.id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }).expect(200)).body;
      return { assignment: assignment!, opened };
    }
    const first = await open();
    const questions = first.opened.sections[0].questions;
    let photoId = "";
    for (const q of questions) {
      const type = q.type;
      let answer: any = type === "multiple" ? { kind: "multi", optionCodes: [q.options[0].code] } : type === "yesnomulti" ? { kind: "yesnomulti", optionCode: q.options[0].code, subOptions: [] } : type === "text" ? { kind: "text", value: "Geprüft" } : type === "numeric" || type === "slider" ? { kind: "number", value: 5 } : type === "matrix" ? { kind: "matrix", cells: [{ rowCode: "row_1", columnCode: "column_1", selected: true }] } : type === "photo" ? null : { kind: "choice", optionCode: q.options[type === "likert" ? 2 : 0].code };
      if (type === "photo") {
        const initialized = (await sm("post", `/sm/visits/${first.assignment.id}/photos/initialize`).send({ submissionQuestionId: q.id }).expect(200)).body;
        const upload = (await sm("post", `/sm/visits/${first.assignment.id}/photos/presign`).send({ answerId: initialized.answerId, extension: "png" }).expect(200)).body.upload;
        objects.add(upload.path);
        photoId = (await sm("post", `/sm/visits/${first.assignment.id}/photos/commit`).send({ answerId: initialized.answerId, photos: [{ storageBucket: upload.bucket, storagePath: upload.path, mimeType: "image/png", byteSize: 10 }] }).expect(200)).body.fileIds[0];
      } else await sm("put", `/sm/visits/${first.assignment.id}/answers/${q.id}`).send({ answer, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
    }
    const [submission] = await f.database.select().from(f.schema.smQuestionnaireSubmissions).where(eq(f.schema.smQuestionnaireSubmissions.assignmentId, first.assignment.id));
    const answers = () => f.database.select().from(f.schema.smQuestionAnswers).where(and(eq(f.schema.smQuestionAnswers.submissionId, submission!.id), eq(f.schema.smQuestionAnswers.isCurrent, true)));
    await t.test("every answer stores semantic metadata while the original payload stays unchanged", async () => {
      const saved = await answers(); assert.equal(saved.length, 10);
      assert.equal(saved.filter(row => row.metricOutcomeCode === "basic_activity_yes").length, 7);
      assert.equal(saved.filter(row => row.metricOutcomeCode === "basic_activity_partial").length, 3);
      assert.equal((saved.find(row => (row.valueJson as any)?.kind === "text")!.valueJson as any).value, "Geprüft");
      const photo = saved.find(row => (row.valueJson as any)?.kind === "photo")!;
      assert.equal(photo.metricOutcomeCode, "basic_activity_yes");
      await sm("delete", `/sm/visits/${first.assignment.id}/photos/${photoId}`).expect(204);
      assert.equal((await answers()).find(row => row.id === photo.id)!.metricOutcomeCode, null);
      // Put the same synthetic object back; no storage network calls take place.
      const upload = (await sm("post", `/sm/visits/${first.assignment.id}/photos/presign`).send({ answerId: photo.id, extension: "png" }).expect(200)).body.upload;
      objects.add(upload.path);
      await sm("post", `/sm/visits/${first.assignment.id}/photos/commit`).send({ answerId: photo.id, photos: [{ storageBucket: upload.bucket, storagePath: upload.path, mimeType: "image/png", byteSize: 10 }] }).expect(200);
    });
    await sm("post", `/sm/visits/${first.assignment.id}/submit`).send({ actualMinutes: 30, visitStartedAt: "2026-10-07T08:00:00Z", visitCompletedAt: "2026-10-07T08:30:00Z", clientMutationToken: randomUUID() }).expect(200);
    await f.database.update(f.schema.smQuestionnaireSubmissions).set({ submittedAt: new Date("2026-10-07T08:30:00Z"), reportingAvailableAt: new Date("2026-10-07T08:30:00Z") }).where(eq(f.schema.smQuestionnaireSubmissions.id, submission!.id));
    const history = JSON.stringify(await answers());
    const get = (query: Record<string, string> = {}) => admin("get", "/admin/sm-dashboard").query({ from: "2026-10-01", to: "2026-10-31", ...query }).expect(200);
    await t.test("all upper metrics follow the same filters and actual ZE, with no fabricated OOS checks", async () => {
      const p = (await get()).body.overview;
      assert.equal(p.activity.marketHours, 0.5); assert.equal(p.activity.hoursPerVisit, 0.5); assert.equal(p.activity.visits, 1); assert.equal(p.activity.merchandisers, 1);
      assert.equal(p.performance.basicActivityAnswers, 10); assert.equal(p.performance.basicActivityPoints, 8.5); assert.equal(p.performance.basicActivityRate, 85);
      assert.equal(p.performance.categoryChecks, 0); assert.equal(p.performance.availabilityGain, null);
      assert.equal((await get({ smUserId: f.employee, marketId: f.market, region: "Test", chain: "Billa" })).body.overview.activity.visits, 0);
      assert.equal((await get({ from: "2026-10-07", to: "2026-10-07", smUserId: f.employee, marketId: f.market })).body.overview.performance.basicActivityRate, 85);
      assert.equal((await get({ from: "2026-10-08", to: "2026-10-08" })).body.overview.performance.basicActivityRate, null);
    });
    await t.test("ZE uses only the current accepted revision, without changing past rows or questionnaire answers", async () => {
      const [original] = await f.database.select().from(f.schema.smAssignmentTimeSubmissions).where(eq(f.schema.smAssignmentTimeSubmissions.assignmentId, first.assignment.id));
      assert.equal(original!.actualMinutes, 30);
      await f.database.update(f.schema.smAssignmentTimeSubmissions).set({ isCurrent: false }).where(eq(f.schema.smAssignmentTimeSubmissions.id, original!.id));
      await f.database.insert(f.schema.smAssignmentTimeSubmissions).values({ assignmentId: first.assignment.id, revisionNumber: 2, actualMinutes: 60, supersedesSubmissionId: original!.id, correctionReason: "Synthetisch akzeptierte ZE-Korrektur", submittedByUserId: f.admin });
      const result = (await get()).body.overview.activity;
      assert.equal(result.marketHours, 1); assert.equal(result.hoursPerVisit, 1); assert.equal(result.timedVisits, 1);
      assert.equal((await f.database.select().from(f.schema.smAssignmentTimeSubmissions).where(eq(f.schema.smAssignmentTimeSubmissions.id, original!.id)))[0]!.actualMinutes, 30);
      assert.equal(JSON.stringify(await answers()), history);
    });
    await t.test("published mapping edits create versions and cannot reinterpret submitted history", async () => {
      const patchedQuestions = created.questions.map((q: any) => ({ ...q, config: { ...q.config, basicActivity: q.type === "yesno" ? { enabled: true, answerOutcomes: { Ja: "no", Nein: "yes" } } : q.config.basicActivity } }));
      await admin("patch", `/admin/sm-questionnaires/modules/${created.id}`).send({ ...moduleInput, id: created.id, questions: patchedQuestions }).expect(200);
      assert.equal(JSON.stringify(await answers()), history);
      assert.equal((await get()).body.overview.performance.basicActivityRate, 85);
      const workspace = (await admin("get", "/admin/sm-questionnaires/workspace").expect(200)).body;
      assert.equal(workspace.modules.find((m: any) => m.id === created.id).questions.find((q: any) => q.type === "yesno").config.basicActivity.answerOutcomes.Ja, "no");
    });
    await t.test("invalid mappings are rejected without any version or answer writes", async () => {
      const invalid = { ...moduleInput, id: `new-${randomUUID()}`, questions: [{ ...moduleInput.questions[6], config: { basicActivity: { enabled: true, rules: [{ kind: "range", min: 9, max: 1, outcome: "yes" }] } } }] };
      await admin("post", "/admin/sm-questionnaires/modules").send(invalid).expect(400);
      assert.equal(JSON.stringify(await answers()), history);
    });
    await t.test("admin corrections and approved employee changes stamp only new answer revisions using the frozen mapping", async () => {
      const question = questions.find((q: any) => q.type === "yesno");
      const previous = (await answers()).find(row => row.submissionQuestionId === question.id)!;
      const path = `/admin/sm-activity/completed/${submission!.id}`;
      const detail = (await admin("get", path).expect(200)).body;
      await admin("post", `${path}/corrections`).send({ expectedVersion: detail.version, clientMutationToken: randomUUID(), reason: "Synthetisch geprüfte Korrektur", changes: [{ questionId: question.id, answer: { kind: "choice", optionCode: question.options[1].code } }] }).expect(200);
      assert.equal((await answers()).find(row => row.submissionQuestionId === question.id)!.metricOutcomeCode, "basic_activity_no");
      assert.equal((await get()).body.overview.performance.basicActivityRate, 80);
      const change = (await sm("post", `/sm/activity/submissions/${submission!.id}/questions/${question.id}/change-requests`).send({ answer: { kind: "choice", optionCode: question.options[0].code }, reason: "Synthetisch erneut kontrolliert", clientRequestToken: randomUUID() }).expect(201)).body.request;
      await admin("post", `/admin/sm-activity/answer-change-requests/${change.id}/approve`).send({}).expect(200);
      assert.equal((await answers()).find(row => row.submissionQuestionId === question.id)!.metricOutcomeCode, "basic_activity_partial", "old frozen mapping wins over today's reversed mapping");
      assert.equal((await get()).body.overview.performance.basicActivityRate, 85);
      const [old] = await f.database.select().from(f.schema.smQuestionAnswers).where(eq(f.schema.smQuestionAnswers.id, previous.id));
      assert.equal(old!.isCurrent, false); assert.equal(old!.isDeleted, false); assert.deepEqual(old!.valueJson, previous.valueJson); assert.equal(old!.metricOutcomeCode, previous.metricOutcomeCode);
    });
  } finally { await f.pg.close(); }
});
