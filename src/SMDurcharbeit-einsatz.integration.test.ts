import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("SMDurcharbeit per-Einsatz override is authoritative, immutable after start and safe for history", async t => {
  const f = await createSMDurcharbeitFixture();
  const admin = (method: "get" | "post" | "patch" | "put", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
  const sm = (method: "get" | "post" | "put" | "delete", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" });
  const questionnaire = async (scope: "standard" | "SMDurcharbeit", name: string, oncePerMarket = false) => {
    const module = (await admin("post", `/admin/sm-questionnaires/modules?scope=${scope}`).send({ id: "new-" + randomUUID(), name, description: "", questions: [{ id: "new-" + randomUUID(), text: name + " question", type: "yesno", required: true, options: ["Ja", "Nein"], config: {}, rules: [] }] }).expect(201)).body.module;
    const form = (await admin("post", `/admin/sm-questionnaires/questionnaires?scope=${scope}`).send({ id: "new-" + randomUUID(), name, description: "", status: "active", nurEinmalAusfuellbar: oncePerMarket, moduleIds: [module.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    return { form, module, version: version! };
  };
  const edit = (row: { id: string; updatedAt: Date }, value: string | null, extra = {}) => admin("patch", `/admin/sm-planning/assignments/${row.id}`).send({ expectedUpdatedAt: row.updatedAt.toISOString(), SMDurcharbeitQuestionnaireOverrideVersionId: value, ...extra });
  let completedCount = 0;
  const complete = async (id: string) => {
    const payload = (await sm("post", `/sm/visits/${id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }).expect(200)).body;
    const q = payload.sections[0].questions[0];
    await sm("put", `/sm/visits/${id}/answers/${q.id}`).send({ answer: { kind: "choice", optionCode: q.options[0].code }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
    const hour = String(8 + completedCount++).padStart(2, "0");
    await sm("post", `/sm/visits/${id}/submit`).send({ actualMinutes: 15, visitStartedAt: `2026-10-07T${hour}:00:00Z`, visitCompletedAt: `2026-10-07T${hour}:15:00Z`, clientMutationToken: randomUUID() }).expect(200);
    return payload;
  };
  try {
    const standard = await questionnaire("standard", "Existing standard");
    const durcharbeit = await questionnaire("SMDurcharbeit", "Durcharbeit");
    await admin("put", "/admin/sm-planning/questionnaire-assignment").send({ questionnaireTemplateId: standard.form.id }).expect(200);
    const historical = await f.assignment();
    await complete(historical.id);
    // Legacy corrections/files and an explicitly discarded graph exist before the feature is exercised.
    const [historicalSubmission] = await f.database.select().from(f.schema.smQuestionnaireSubmissions).where(eq(f.schema.smQuestionnaireSubmissions.assignmentId, historical.id));
    const [oldAnswer] = await f.database.select().from(f.schema.smQuestionAnswers).where(eq(f.schema.smQuestionAnswers.submissionId, historicalSubmission!.id));
    await f.database.update(f.schema.smQuestionAnswers).set({ isCurrent: false }).where(eq(f.schema.smQuestionAnswers.id, oldAnswer!.id));
    const [corrected] = await f.database.insert(f.schema.smQuestionAnswers).values({ ...oldAnswer!, id: randomUUID(), answerVersion: 2, supersedesAnswerId: oldAnswer!.id }).returning();
    await f.database.insert(f.schema.smQuestionAnswerFiles).values({ answerId: oldAnswer!.id, storageBucket: "synthetic-history", storagePath: "synthetic/old-photo.jpg", originalFileName: "old-photo.jpg", mimeType: "image/jpeg", byteSize: 123 });
    await f.database.insert(f.schema.smQuestionAnswerEvents).values({ answerId: corrected!.id, submissionId: historicalSubmission!.id, eventType: "correction", answerVersion: 2, payload: { previousAnswerId: oldAnswer!.id }, actorUserId: f.admin });
    const discarded = await f.assignment();
    await sm("post", `/sm/visits/${discarded.id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }).expect(200);
    await sm("delete", `/sm/visits/${discarded.id}`).send({ confirmation: "SOFT_DELETE_SM_VISIT" }).expect(200);
    const historyTables = ["sm_assignments", "sm_questionnaire_submissions", "sm_questionnaire_submission_sections", "sm_questionnaire_submission_questions", "sm_question_answers", "sm_question_answer_options", "sm_question_answer_files", "sm_question_answer_events", "sm_assignment_time_submissions", "sm_assignment_events"];
    const snapshot = async () => Object.fromEntries(await Promise.all(historyTables.map(async table => [table, (await f.pg.query<{ row: Record<string, unknown> }>(`select to_jsonb(t) - 'smdurcharbeit_questionnaire_override_version_id' as row from ${table} t order by id`)).rows.map(item => item.row)])));
    const originalHistory = await snapshot();
    const oldReport = (await admin("get", "/admin/sm-dashboard?from=2026-10-07&to=2026-10-07").expect(200)).body;

    await t.test("additive migration preserves every pre-existing historical value", async () => {
      await f.pg.exec("alter table sm_assignments drop column smdurcharbeit_questionnaire_override_version_id");
      await f.pg.exec(await readFile(new URL("../supabase/migrations/20261007124730_SMDurcharbeit_einsatz_override.sql", import.meta.url), "utf8"));
      assert.deepEqual(await snapshot(), originalHistory);
      const rows = await f.database.select().from(f.schema.smAssignments);
      assert.ok(rows.every(row => row.SMDurcharbeitQuestionnaireOverrideVersionId === null));
    });

    await t.test("date-aware dropdown options include both catalogs with stable type identity", async () => {
      const response = await admin("get", "/admin/sm-planning/SMDurcharbeit-questionnaire-options?workDate=2026-10-07").expect(200);
      assert.equal(response.body.options.length, 2);
      assert.equal(response.body.options.find((row: any) => row.questionnaireTemplateId === durcharbeit.form.id).SMDurcharbeitCatalogScope, "SMDurcharbeit");
      await request(f.app).get("/admin/sm-planning/SMDurcharbeit-questionnaire-options?workDate=2026-10-07").auth("synthetic-sm", { type: "bearer" }).expect(403);
      await admin("get", "/admin/sm-planning/SMDurcharbeit-questionnaire-options?workDate=invalid").expect(400);
    });

    await t.test("one override replaces the central form, while another Einsatz keeps its default", async () => {
      const target = await f.assignment(), normal = await f.assignment();
      await edit(target, durcharbeit.version.id).expect(200);
      const changed = (await sm("get", `/sm/visits/${target.id}`).expect(200)).body;
      const untouched = (await sm("get", `/sm/visits/${normal.id}`).expect(200)).body;
      assert.equal(changed.SMDurcharbeitQuestionnaireSelection.catalogScope, "SMDurcharbeit");
      assert.equal(changed.SMDurcharbeitQuestionnaireSelection.source, "override");
      assert.equal(untouched.SMDurcharbeitQuestionnaireSelection.questionnaireVersionId, standard.version.id);
      const planned = (await admin("get", "/admin/sm-planning/assignments?from=2026-10-07&to=2026-10-07").expect(200)).body.assignments;
      assert.equal(planned.find((row: any) => row.id === target.id).SMDurcharbeitQuestionnaireSelection.catalogScope, "SMDurcharbeit");
      const completed = await complete(target.id);
      assert.equal(completed.submission.questionnaireName, durcharbeit.form.name);
      const [row] = await f.database.select().from(f.schema.smAssignments).where(eq(f.schema.smAssignments.id, target.id));
      await edit(row!, standard.version.id).expect(409);
      const repeat = (await sm("post", `/sm/visits/${target.id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID(), SMDurcharbeitExpectedSelectionRevision: "stale" }).expect(200)).body;
      assert.equal(repeat.submission.id, completed.submission.id);
    });

    await t.test("explicit reset and omitted override have different meanings", async () => {
      const target = await f.assignment();
      await edit(target, durcharbeit.version.id).expect(200);
      let [row] = await f.database.select().from(f.schema.smAssignments).where(eq(f.schema.smAssignments.id, target.id));
      await admin("patch", `/admin/sm-planning/assignments/${target.id}`).send({ expectedUpdatedAt: row!.updatedAt.toISOString(), plannedMinutes: 30 }).expect(200);
      [row] = await f.database.select().from(f.schema.smAssignments).where(eq(f.schema.smAssignments.id, target.id));
      assert.equal(row!.SMDurcharbeitQuestionnaireOverrideVersionId, durcharbeit.version.id);
      await edit(row!, null).expect(200);
      const preview = (await sm("get", `/sm/visits/${target.id}`).expect(200)).body;
      assert.equal(preview.SMDurcharbeitQuestionnaireSelection.source, "central");
      assert.equal(preview.SMDurcharbeitQuestionnaireSelection.questionnaireVersionId, standard.version.id);
    });

    await t.test("stale edits, invalid versions and final-date failures roll back the whole change", async () => {
      const target = await f.assignment();
      await edit(target, randomUUID(), { plannedMinutes: 45, workDate: "2026-10-08" }).expect(409);
      assert.deepEqual((await f.database.select().from(f.schema.smAssignments).where(eq(f.schema.smAssignments.id, target.id)))[0], target);
      const [bounded] = await f.database.insert(f.schema.smQuestionnaireVersions).values({ ...durcharbeit.version, id: randomUUID(), versionNumber: 99, status: "draft", publishedAt: null, publishedByUserId: null, contentHash: null, effectiveTo: "2026-10-07" }).returning();
      const links = await f.database.select().from(f.schema.smQuestionnaireVersionModules).where(eq(f.schema.smQuestionnaireVersionModules.questionnaireVersionId, durcharbeit.version.id));
      await f.database.insert(f.schema.smQuestionnaireVersionModules).values(links.map(row => ({ ...row, id: randomUUID(), questionnaireVersionId: bounded!.id })));
      await f.database.update(f.schema.smQuestionnaireVersions).set({ status: "published", publishedAt: new Date(), publishedByUserId: f.admin, contentHash: "synthetic-bounded-version" }).where(eq(f.schema.smQuestionnaireVersions.id, bounded!.id));
      await edit(target, bounded!.id, { workDate: "2026-10-08", plannedMinutes: 60 }).expect(409);
      assert.deepEqual((await f.database.select().from(f.schema.smAssignments).where(eq(f.schema.smAssignments.id, target.id)))[0], target);
      await edit(target, durcharbeit.version.id).expect(200);
      await edit(target, null).expect(409);
    });

    await t.test("stale cached selection cannot start a different form silently", async () => {
      const target = await f.assignment();
      const original = (await sm("get", `/sm/visits/${target.id}`).expect(200)).body.SMDurcharbeitQuestionnaireSelection;
      await edit(target, durcharbeit.version.id).expect(200);
      await sm("post", `/sm/visits/${target.id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID(), SMDurcharbeitExpectedSelectionRevision: original.revision }).expect(409);
      assert.equal((await f.database.select().from(f.schema.smQuestionnaireSubmissions).where(eq(f.schema.smQuestionnaireSubmissions.assignmentId, target.id))).length, 0);
    });

    await t.test("pending override blocks deactivation/deletion and survives cancel/restore", async () => {
      const target = await f.assignment();
      await edit(target, durcharbeit.version.id).expect(200);
      await admin("patch", `/admin/sm-questionnaires/questionnaires/${durcharbeit.form.id}/delete?scope=SMDurcharbeit`).expect(409);
      await admin("patch", `/admin/sm-questionnaires/questionnaires/${durcharbeit.form.id}?scope=SMDurcharbeit`).send({ ...durcharbeit.form, status: "inactive" }).expect(409);
      let [row] = await f.database.select().from(f.schema.smAssignments).where(eq(f.schema.smAssignments.id, target.id));
      await admin("post", `/admin/sm-planning/assignments/${target.id}/cancel`).send({ expectedUpdatedAt: row!.updatedAt.toISOString(), reason: "Synthetic cancellation" }).expect(200);
      [row] = await f.database.select().from(f.schema.smAssignments).where(eq(f.schema.smAssignments.id, target.id));
      await admin("post", `/admin/sm-planning/assignments/${target.id}/restore`).send({ expectedUpdatedAt: row!.updatedAt.toISOString(), reason: "Synthetic restore" }).expect(200);
      [row] = await f.database.select().from(f.schema.smAssignments).where(eq(f.schema.smAssignments.id, target.id));
      assert.equal(row!.SMDurcharbeitQuestionnaireOverrideVersionId, durcharbeit.version.id);
    });

    await t.test("new published versions do not upgrade a pinned Einsatz", async () => {
      const target = await f.assignment();
      await edit(target, durcharbeit.version.id).expect(200);
      await admin("patch", `/admin/sm-questionnaires/questionnaires/${durcharbeit.form.id}?scope=SMDurcharbeit`).send({ ...durcharbeit.form, description: "A later version" }).expect(200);
      const preview = (await sm("get", `/sm/visits/${target.id}`).expect(200)).body;
      assert.equal(preview.SMDurcharbeitQuestionnaireSelection.questionnaireVersionId, durcharbeit.version.id);
    });

    await t.test("type reporting matches management, and no-OOS Durcharbeit stays unclassified", async () => {
      const all = (await admin("get", "/admin/sm-dashboard?from=2026-10-07&to=2026-10-07").expect(200)).body;
      const only = (await admin("get", "/admin/sm-dashboard?from=2026-10-07&to=2026-10-07&SMDurcharbeitCatalogScope=SMDurcharbeit").expect(200)).body;
      assert.equal(all.summary.completedVisits, 2);
      assert.equal(only.summary.completedVisits, 1);
      assert.equal(only.summary.classifiedChecks, 0);
      assert.equal(only.summary.foundRate, null);
      assert.equal(only.SMDurcharbeitBreakdown.SMDurcharbeit.answeredQuestions, 1);
      const managed = (await admin("get", "/admin/sm-activity/completed?from=2026-10-07&to=2026-10-07&SMDurcharbeitCatalogScope=SMDurcharbeit").expect(200)).body;
      assert.equal(managed.visits.length, 1);
      assert.equal(managed.visits[0].SMDurcharbeitCatalogScope, "SMDurcharbeit");
      const detail = (await admin("get", `/admin/sm-activity/completed/${managed.visits[0].id}`).expect(200)).body;
      assert.equal(detail.visit.SMDurcharbeitCatalogScope, "SMDurcharbeit");
      const legacyReport = (await admin("get", "/admin/sm-dashboard?from=2026-10-07&to=2026-10-07&SMDurcharbeitCatalogScope=standard").expect(200)).body;
      assert.deepEqual(legacyReport.summary, oldReport.summary);
      assert.deepEqual(legacyReport.categories, oldReport.categories);
    });

    await t.test("racing a stale preview start against an edit never starts a different graph", async () => {
      const target = await f.assignment();
      const preview = (await sm("get", `/sm/visits/${target.id}`).expect(200)).body;
      const [edited, started] = await Promise.all([
        edit(target, durcharbeit.version.id),
        sm("post", `/sm/visits/${target.id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID(), SMDurcharbeitExpectedSelectionRevision: preview.SMDurcharbeitQuestionnaireSelection.revision }),
      ]);
      assert.deepEqual([edited.status, started.status].sort(), [200, 409]);
      const final = (await sm("get", `/sm/visits/${target.id}`).expect(200)).body;
      assert.equal(final.SMDurcharbeitQuestionnaireSelection.questionnaireVersionId, started.status === 200 ? standard.version.id : durcharbeit.version.id);
    });

    await t.test("series changes retain overrides only on the existing occurrence", async () => {
      const response = await admin("post", "/admin/sm-planning/series").send({ smMarketId: f.market, smUserId: f.employee, plannedMinutes: 15, frequency: "weekly", weekdays: [3], validFrom: "2026-10-07", validTo: "2026-10-28", idempotencyKey: randomUUID() });
      assert.equal(response.status, 201, JSON.stringify(response.body));
      const created = response.body;
      const rows = await f.database.select().from(f.schema.smAssignments).where(eq(f.schema.smAssignments.seriesId, created.seriesId));
      const target = rows.find(row => row.originalWorkDate === "2026-10-14")!;
      await edit(target, durcharbeit.version.id).expect(200);
      const change = { action: "edit", effectiveFromDate: "2026-10-07", smMarketId: f.market, smUserId: f.employee, plannedMinutes: 30, frequency: "weekly", weekdays: [3], validTo: "2026-11-04" };
      const preview = (await admin("post", `/admin/sm-planning/series/${created.seriesId}/preview`).send(change).expect(200)).body;
      await admin("post", `/admin/sm-planning/series/${created.seriesId}/change`).send({ change, previewToken: preview.previewToken, reason: "Synthetic extension" }).expect(200);
      const updated = await f.database.select().from(f.schema.smAssignments).where(eq(f.schema.smAssignments.seriesId, created.seriesId));
      assert.equal(updated.find(row => row.id === target.id)!.SMDurcharbeitQuestionnaireOverrideVersionId, durcharbeit.version.id);
      assert.ok(updated.filter(row => row.id !== target.id).every(row => row.SMDurcharbeitQuestionnaireOverrideVersionId === null));
      assert.ok(updated.some(row => row.originalWorkDate === "2026-11-04"));
    });

    await t.test("once-per-market still blocks repeat selection and archived history keeps its type", async () => {
      const once = await questionnaire("SMDurcharbeit", "One-time Durcharbeit", true);
      const target = await f.assignment();
      await edit(target, once.version.id).expect(200);
      const done = await complete(target.id);
      await edit(await f.assignment(), once.version.id).expect(409);
      await admin("patch", `/admin/sm-questionnaires/questionnaires/${once.form.id}?scope=SMDurcharbeit`).send({ ...once.form, status: "inactive" }).expect(200);
      const detail = (await admin("get", `/admin/sm-activity/completed/${done.submission.id}`).expect(200)).body;
      assert.equal(detail.visit.SMDurcharbeitCatalogScope, "SMDurcharbeit");
      assert.equal(detail.visit.questionnaireName, once.form.name);
    });

    await t.test("all original historical records and links remain byte-for-byte unchanged", async () => {
      const current = await snapshot();
      for (const table of historyTables) {
        const original = originalHistory[table] as Record<string, unknown>[];
        const ids = new Set(original.map(row => row.id));
        assert.deepEqual((current[table] as Record<string, unknown>[]).filter(row => ids.has(row.id)), original, table);
      }
    });
  } finally { await f.pg.close(); }
});
