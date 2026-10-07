import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("SMDurcharbeit catalogs preserve standard links, full configuration and immutable visits", async t => {
  const f = await createSMDurcharbeitFixture();
  const admin = (method: "get" | "post" | "patch", path: string, scope?: string) => request(f.app)[method](path + (scope ? `?scope=${scope}` : "")).auth("synthetic-sm-admin", { type: "bearer" });
  const sm = (method: "post" | "put" | "get", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" });
  const question = (id: string, type = "yesno", config: Record<string, unknown> = {}) => ({ id, text: `${id} synthetic`, type, required: false, options: ["Ja", "Nein"], config, rules: [] as Array<Record<string, unknown>> });
  try {
    const standardModule = (await admin("post", "/admin/sm-questionnaires/modules").send({ id: "new-standard", name: "Existing standard", description: "Unchanged", questions: [question("standard-q")] }).expect(201)).body.module;
    const standardQuestionnaire = (await admin("post", "/admin/sm-questionnaires/questionnaires").send({ id: "new-standard", name: "Existing standard questionnaire", description: "Unchanged", moduleIds: [standardModule.id], status: "active" }).expect(201)).body.questionnaire;
    const beforeStandard = (await admin("get", "/admin/sm-questionnaires/workspace", "standard").expect(200)).body;
    const beforeLinks = await f.database.select().from(f.schema.smQuestionnaireVersionModules);
    const [standardSelection] = await f.database.insert(f.schema.smQuestionnaireGlobalAssignments).values({ questionnaireTemplateId: standardQuestionnaire.id, assignedByUserId: f.admin }).returning();
    const detection = { ...question("detect", "yesno", { subheading: "Question subtitle", answerSubheadings: ["Yes subtitle", "No subtitle"], images: ["data:image/png;base64,synthetic"], commentTrigger: { mode: "options", optionCodes: ["option_1"] } }), oos: { enabled: true, role: "detection", category: "softdrinks_energy", answerOutcomes: { Ja: "oos_present", Nein: "oos_absent" } } };
    detection.rules = [{ id: "show-remediation", triggerQuestionId: "detect", operator: "equals", triggerValue: "Ja", triggerValueMax: "", action: "show", targetQuestionIds: ["remedy"] }];
    const remedy = { ...question("remedy"), oos: { enabled: true, role: "remediation", category: "softdrinks_energy", detectionQuestionId: "detect", partialCountsAsResolved: true, answerOutcomes: { Ja: "resolved", Nein: "not_resolved" } } };
    const questions = [detection, remedy, question("single", "single", { options: ["A", "B"], answerSubheadings: ["a", "b"] }), question("multi", "multiple", { options: ["A", "B"] }),
      question("branch", "yesnomulti", { answers: ["Ja", "Nein"], branches: [{ answer: "Ja", options: ["A", "B"], answerSubheadings: ["a", "b"] }] }),
      question("likert", "likert", { min: 1, max: 5, minLabel: "low", maxLabel: "high" }), question("text", "text", { placeholder: "Note" }),
      question("numeric", "numeric", { min: 0, max: 20, step: 1, integer: true, unit: "Stück" }), question("slider", "slider", { min: 0, max: 100, step: 5, unit: "%" }),
      question("photo", "photo", { instruction: "Synthetic photo instruction", images: [] }), question("matrix", "matrix", { rows: ["R1", "R2"], columns: ["C1", "C2"], rowSubheadings: ["r1", "r2"], columnSubheadings: ["c1", "c2"] })];
    const module = (await admin("post", "/admin/sm-questionnaires/modules", "SMDurcharbeit").send({ id: "new-Durcharbeit", name: "Durcharbeit full settings", description: "Synthetic", questions }).expect(201)).body.module;
    const questionnaire = (await admin("post", "/admin/sm-questionnaires/questionnaires", "SMDurcharbeit").send({ id: "new-Durcharbeit", name: "Durcharbeit questionnaire", description: "Synthetic", moduleIds: [module.id], status: "active", nurEinmalAusfuellbar: true }).expect(201)).body.questionnaire;

    await t.test("all question types and special configurations survive saving and reloading", async () => {
      assert.equal(module.questions.length, questions.length);
      module.questions.forEach((q: any, index: number) => { assert.equal(q.type, questions[index]!.type); assert.deepEqual(q.config, questions[index]!.config); });
      assert.equal(module.questions[0].rules[0].triggerQuestionId, module.questions[0].id);
      assert.deepEqual(module.questions[0].rules[0].targetQuestionIds, [module.questions[1].id]);
      assert.equal(module.questions[1].oos.detectionQuestionId, module.questions[0].id);
      assert.equal(module.questions[1].oos.partialCountsAsResolved, true);
      assert.equal(questionnaire.nurEinmalAusfuellbar, true);
      const loaded = (await admin("get", "/admin/sm-questionnaires/workspace", "SMDurcharbeit").expect(200)).body;
      assert.deepEqual(loaded.modules, [module]);
      assert.deepEqual(loaded.questionnaires, [questionnaire]);
      const [root] = await f.database.select().from(f.schema.smModules).where(eq(f.schema.smModules.id, module.id));
      assert.match(root!.stableCode, /^smdurcharbeit_module_/);
    });
    await t.test("standard IDs, published links and current global selection remain unchanged", async () => {
      assert.deepEqual((await admin("get", "/admin/sm-questionnaires/workspace", "standard").expect(200)).body, beforeStandard);
      assert.deepEqual((await f.database.select().from(f.schema.smQuestionnaireVersionModules)).filter(link => beforeLinks.some(old => old.id === link.id)), beforeLinks);
      assert.equal((await f.database.select().from(f.schema.smQuestionnaireGlobalAssignments))[0]!.questionnaireTemplateId, standardQuestionnaire.id);
      const assignment = await f.assignment();
      const started = (await sm("post", `/sm/visits/${assignment.id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }).expect(200)).body;
      assert.equal(started.submission.questionnaireName, standardQuestionnaire.name);
      const all = (await admin("get", "/admin/sm-questionnaires/workspace").expect(200)).body;
      assert.equal(all.modules.length, 2, "Existing unscoped readers can still resolve every catalog link");
    });
    await t.test("cross-catalog edits, deletes and module references are rejected without writes", async () => {
      const versionCount = (await f.database.select().from(f.schema.smModuleVersions)).length;
      for (const [target, scope] of [[standardModule, "SMDurcharbeit"], [module, "standard"]] as const) {
        await admin("patch", `/admin/sm-questionnaires/modules/${target.id}`, scope).send(target).expect(404);
        await admin("patch", `/admin/sm-questionnaires/modules/${target.id}/delete`, scope).expect(404);
      }
      await admin("patch", `/admin/sm-questionnaires/questionnaires/${standardQuestionnaire.id}`, "SMDurcharbeit").send(standardQuestionnaire).expect(404);
      await admin("patch", `/admin/sm-questionnaires/questionnaires/${questionnaire.id}/delete`, "standard").expect(404);
      const templateCount = (await f.database.select().from(f.schema.smQuestionnaireTemplates)).length;
      await admin("post", "/admin/sm-questionnaires/questionnaires", "SMDurcharbeit").send({ ...questionnaire, id: "new-invalid", moduleIds: [standardModule.id] }).expect(400);
      assert.equal((await f.database.select().from(f.schema.smQuestionnaireTemplates)).length, templateCount, "Failed save rolls back its new root");
      assert.equal((await f.database.select().from(f.schema.smModuleVersions)).length, versionCount);
      await admin("get", "/admin/sm-questionnaires/workspace", "wrong").expect(400);
      await request(f.app).post("/admin/sm-questionnaires/modules?scope=SMDurcharbeit").auth("synthetic-sm", { type: "bearer" }).send(module).expect(403);
    });
    await t.test("used modules and centrally assigned questionnaires retain deletion safeguards", async () => {
      await admin("patch", `/admin/sm-questionnaires/modules/${module.id}/delete`, "SMDurcharbeit").expect(409);
      await admin("patch", `/admin/sm-questionnaires/modules/${standardModule.id}/delete`, "standard").expect(409);
      await admin("patch", `/admin/sm-questionnaires/questionnaires/${standardQuestionnaire.id}/delete`, "standard").expect(409);
    });

    // Explicit manual planning selection in the disposable DB only; creating a catalog item never assigns it.
    await f.database.update(f.schema.smQuestionnaireGlobalAssignments).set({ supersededAt: new Date(), supersededByUserId: f.admin }).where(eq(f.schema.smQuestionnaireGlobalAssignments.id, standardSelection!.id));
    await f.database.insert(f.schema.smQuestionnaireGlobalAssignments).values({ questionnaireTemplateId: questionnaire.id, assignedByUserId: f.admin });
    const assignment = await f.assignment();
    const visit = (await sm("post", `/sm/visits/${assignment.id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }).expect(200)).body;
    const visitQuestions = visit.sections.flatMap((section: any) => section.questions);
    const q = visitQuestions.find((q: any) => q.text === detection.text);
    await t.test("Durcharbeit resolves through existing planning, version snapshots and answer submission", async () => {
      assert.equal(visit.submission.questionnaireName, questionnaire.name);
      assert.deepEqual(q.config, detection.config);
      await sm("put", `/sm/visits/${assignment.id}/answers/${q.id}`).send({ answer: { kind: "choice", optionCode: q.options[0].code, comment: "Synthetic answer comment" }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
      const submitted = await sm("post", `/sm/visits/${assignment.id}/submit`).send({ actualMinutes: 15, visitStartedAt: "2026-10-07T08:00:00Z", visitCompletedAt: "2026-10-07T08:15:00Z", clientMutationToken: randomUUID() });
      assert.equal(submitted.status, 200, JSON.stringify(submitted.body));
    });
    await t.test("later editing preserves completed visit configuration and answer history", async () => {
      const saved = (await admin("patch", `/admin/sm-questionnaires/modules/${module.id}`, "SMDurcharbeit").send({ ...module, description: "Changed later" }).expect(200)).body.module;
      assert.equal(saved.id, module.id);
      const current = (await sm("get", `/sm/visits/${assignment.id}`).expect(200)).body;
      assert.deepEqual(current.sections.flatMap((section: any) => section.questions).find((row: any) => row.id === q.id).config, detection.config);
      const [answer] = await f.database.select().from(f.schema.smQuestionAnswers);
      assert.ok(answer);
      assert.equal((await f.database.select().from(f.schema.smQuestionnaireSubmissions)).filter(row => row.status === "submitted").length, 1);
    });
  } finally { await f.pg.close(); }
});
