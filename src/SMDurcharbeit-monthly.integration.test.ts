import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { and, eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("monthly SMDurcharbeit uses actual routes, immutable physical visits and isolated calendar-month state", async t => {
  let now = Date.parse("2026-10-09T12:00:00Z");
  class Clock extends Date { constructor(value?: string | number | Date) { super(value === undefined ? now : value instanceof Date ? value.getTime() : value); } static now() { return now; } }
  const objects = new Set<string>(), removed: string[] = [];
  const f = await createSMDurcharbeitFixture({ clock: Clock as typeof Date, photoStorage: { from: () => ({
    createSignedUrl: async (path: string) => ({ data: { signedUrl: `https://synthetic.invalid/${path}` }, error: null }),
    createSignedUploadUrl: async (path: string) => ({ data: { path, signedUrl: `https://synthetic.invalid/${path}`, token: "synthetic" }, error: null }),
    list: async (folder: string, options: { search: string }) => ({ data: objects.has(`${folder}/${options.search}`) ? [{ name: options.search }] : [], error: null }),
    remove: async (paths: string[]) => { paths.forEach(path => { removed.push(path); objects.delete(path); }); return { error: null }; },
  }) } });
  const admin = (method: "get" | "post" | "patch" | "put", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
  const sm = (method: "get" | "post" | "put" | "delete", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" });
  const campaignPath = "/admin/sm-smdurcharbeit-campaigns", targetPath = "/sm/smdurcharbeit/targets";
  const visitPath = (id: string) => `/sm/smdurcharbeit/visits/${id}`;
  const q = (text: string, type = "yesno", config: Record<string, unknown> = {}) => ({ id: "new-" + randomUUID(), text, type, required: text === "yesno", options: ["Ja", "Nein"], config, rules: [] });
  const submit = (id: string, hour = 8, day = "2026-10-09") => sm("post", `${visitPath(id)}/submit`).send({ visitStartedAt: `${day}T${String(hour).padStart(2, "0")}:00:00Z`, visitCompletedAt: `${day}T${String(hour).padStart(2, "0")}:15:00Z`, clientMutationToken: randomUUID() });
  try {
    const specialId = randomUUID();
    await f.database.insert(f.schema.smMarkets).values({ id: specialId, internalMarketId: "SYNTHETIC-MONTHLY", name: "Monthly synthetic market", chain: "Spar", address: "Synthetic 5", postalCode: "1010", city: "Wien", region: "Ost", assignedSmUserId: f.employee });
    await f.database.insert(f.schema.smSMDurcharbeitMarkets).values({ smMarketId: specialId, SMDurcharbeitVerplanung: "Local SM" });
    const questionInputs = [q("yesno", "yesno", { commentTrigger: { mode: "options", optionCodes: ["option_1"] }, subheading: "Inherited configuration" }),
      q("single", "single", { options: ["A", "B"] }), q("multi", "multiple", { options: ["A", "B"] }),
      q("branch", "yesnomulti", { answers: ["Ja", "Nein"], branches: [{ answer: "Ja", options: ["A", "B"] }] }),
      q("likert", "likert", { min: 1, max: 5 }), q("text", "text"), q("clear", "text"), q("number", "numeric", { min: 0, max: 20, integer: true }),
      q("slider", "slider", { min: 0, max: 100, step: 5 }), q("matrix", "matrix", { rows: ["R1", "R2"], columns: ["C1", "C2"] }), q("photo", "photo", { commentTrigger: { mode: "answered" } })];
    const module = (await admin("post", "/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({ id: "new-" + randomUUID(), name: "Synthetic monthly module", description: "", questions: questionInputs }).expect(201)).body.module;
    const form = (await admin("post", "/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({ id: "new-" + randomUUID(), name: "Synthetic monthly questionnaire", description: "", status: "active", nurEinmalAusfuellbar: true, moduleIds: [module.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    const campaign = (await admin("post", campaignPath).send({ name: "October–December synthetic", startDate: "2026-10-01", endDate: "2026-12-31", questionnaireVersionId: version!.id, rosterDraft: [{ smMarketId: specialId, smUserId: f.employee }] }).expect(201)).body.campaign;
    await t.test("options expose only registry markets; publication previews and authorizes stable IDs", async () => {
      const options = (await admin("get", `${campaignPath}/options`).expect(200)).body;
      assert.deepEqual(options.markets.map((market: any) => market.id), [specialId]);
      assert.equal(options.markets[0].assignedSmUserId, f.employee);
      assert.equal(options.questionnaires[0].id, version!.id);
      await request(f.app).get(campaignPath).auth("synthetic-sm", { type: "bearer" }).expect(403);
      const preview = await admin("get", `${campaignPath}/${campaign.id}/preview`);
      assert.equal(preview.status, 200, JSON.stringify(preview.body));
      assert.equal(preview.body.targetCount, 3);
      assert.deepEqual(preview.body.months, ["2026-10-01", "2026-11-01", "2026-12-01"]);
      await admin("post", `${campaignPath}/${campaign.id}/publish`).send({ expectedRevision: 1, previewToken: "0".repeat(64) }).expect(409);
      await admin("post", `${campaignPath}/${campaign.id}/publish`).send({ expectedRevision: 1, previewToken: preview.body.previewToken }).expect(200);
      assert.equal((await f.database.select().from(f.schema.smSMDurcharbeitTargets)).length, 3);
      assert.equal((await f.database.select().from(f.schema.smAssignments)).length, 0, "No dated assignment or invented Soll is created");
    });
    const currentTargets = () => sm("get", `${targetPath}?month=2026-10-01`).expect(200);
    await t.test("month navigation is read-only, exposes only owned periods and cannot open future work", async () => {
      const otherUser = randomUUID(), otherMarket = randomUUID();
      await f.database.insert(f.schema.users).values({ id: otherUser, role: "sm", firstName: "Other", lastName: "Synthetic", email: "other@preview.test", isActive: true });
      await f.database.insert(f.schema.smMarkets).values({ id: otherMarket, internalMarketId: "SYNTHETIC-PRIVATE", name: "Private synthetic market", chain: "Spar", address: "Private 6", postalCode: "1010", city: "Wien", region: "Ost", assignedSmUserId: otherUser });
      await f.database.insert(f.schema.smSMDurcharbeitMarkets).values({ smMarketId: otherMarket, SMDurcharbeitVerplanung: "Other Synthetic" });
      const privateCampaign = (await admin("post", campaignPath).send({ name: "Private future campaign", startDate: "2027-01-01", endDate: "2027-02-28", questionnaireVersionId: version!.id, rosterDraft: [{ smMarketId: otherMarket, smUserId: otherUser }] }).expect(201)).body.campaign;
      const privatePreview = (await admin("get", `${campaignPath}/${privateCampaign.id}/preview`).expect(200)).body;
      await admin("post", `${campaignPath}/${privateCampaign.id}/publish`).send({ expectedRevision: 1, previewToken: privatePreview.previewToken }).expect(200);
      const readSnapshot = () => Promise.all([f.schema.smSMDurcharbeitTargets, f.schema.smSMDurcharbeitVisits, f.schema.smSMDurcharbeitOwnerRevisions, f.schema.smQuestionnaireSubmissions].map(table => f.database.select().from(table as any)));
      const before = await readSnapshot();
      const current = (await currentTargets()).body;
      assert.equal(current.currentMonth, "2026-10-01"); assert.deepEqual(current.months, ["2026-10-01", "2026-11-01", "2026-12-01"]);
      const future = (await sm("get", `${targetPath}?month=2026-11-01`).expect(200)).body;
      assert.equal(future.targets.length, 1); assert.equal(future.targets[0].available, false); assert.equal(future.targets[0].draftVisitId, null);
      assert.equal((await sm("get", `${targetPath}?month=2027-01-01`).expect(200)).body.targets.length, 0, "Another employee's future campaign is not visible");
      await sm("get", `${targetPath}?month=2026-10-01&smUserId=${otherUser}`).expect(400);
      assert.deepEqual(await readSnapshot(), before, "Loading periods or future rosters creates no target, draft or ownership revision");
      await sm("post", `${targetPath}/${future.targets[0].id}/start`).send({ expectedRevision: future.targets[0].revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }).expect(409);
      assert.deepEqual(await readSnapshot(), before, "A rejected future start is atomic");
    });
    let october = (await currentTargets()).body.targets[0], firstVisit = "", firstSubmission = "", photoFile = "", photoPath = "";
    const firstToken = randomUUID();
    await t.test("start is idempotent, rejects non-owner/invalid targets and snapshots an empty monthly visit", async () => {
      await sm("post", `${targetPath}/${randomUUID()}/start`).send({ expectedRevision: 1, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }).expect(404);
      const startInput = { expectedRevision: october.revision, followUp: false, mode: "manual", clientSubmissionToken: firstToken, travelMinutes: 5 };
      const started = await sm("post", `${targetPath}/${october.id}/start`).send(startInput);
      assert.equal(started.status, 201, JSON.stringify(started.body)); firstVisit = started.body.visitId; firstSubmission = started.body.submissionId;
      assert.equal((await sm("post", `${targetPath}/${october.id}/start`).send(startInput).expect(200)).body.visitId, firstVisit);
      assert.equal((await sm("post", `${targetPath}/${october.id}/start`).send({ ...startInput, clientSubmissionToken: randomUUID() }).expect(200)).body.visitId, firstVisit);
      const payload = (await sm("get", visitPath(firstVisit)).expect(200)).body;
      assert.equal(payload.assignment.id, `SMDurcharbeit:${firstVisit}`);
      assert.equal(payload.assignment.workDate, null); assert.equal(payload.assignment.plannedMinutes, null);
      assert.ok(Object.values(payload.answers).every(value => value === null));
      assert.equal(payload.SMDurcharbeitContext.month, "2026-10-01");
      assert.equal(payload.submission.travelMinutes, 5);
      await submit(firstVisit).expect(409);
    });
    const firstPayload = (await sm("get", visitPath(firstVisit)).expect(200)).body;
    const byText = Object.fromEntries(firstPayload.sections.flatMap((section: any) => section.questions).map((question: any) => [question.text, question])) as Record<string, any>;
    const answer = (text: string, value: unknown, expectedAnswerVersion = 0) => sm("put", `${visitPath(firstVisit)}/answers/${byText[text].id}`).send({ answer: value, expectedAnswerVersion, clientMutationToken: randomUUID() });
    const values: Record<string, unknown> = { yesno: { kind: "choice", optionCode: byText.yesno.options[0].code, comment: "Original synthetic comment" },
      single: { kind: "choice", optionCode: byText.single.options[0].code }, multi: { kind: "multi", optionCodes: byText.multi.options.map((option: any) => option.code) },
      branch: { kind: "yesnomulti", optionCode: byText.branch.options[0].code, subOptions: ["A", "B"] }, likert: { kind: "choice", optionCode: byText.likert.options[0].code }, text: { kind: "text", value: "Original note" },
      clear: { kind: "empty" }, number: { kind: "number", value: 8 }, slider: { kind: "number", value: 50 }, matrix: { kind: "matrix", cells: [{ rowCode: "row_1", columnCode: "column_1", selected: true }] } };
    await t.test("all question types, explicit clears and owned photos save through the shared engine", async () => {
      for (const [text, value] of Object.entries(values)) { const result = await answer(text, value); assert.equal(result.status, 200, `${text}: ${JSON.stringify(result.body)}`); }
      const initialized = (await sm("post", `${visitPath(firstVisit)}/photos/initialize`).send({ submissionQuestionId: byText.photo.id }).expect(200)).body;
      const upload = (await sm("post", `${visitPath(firstVisit)}/photos/presign`).send({ answerId: initialized.answerId, extension: "jpg" }).expect(200)).body.upload;
      photoPath = upload.path; objects.add(photoPath);
      const committed = (await sm("post", `${visitPath(firstVisit)}/photos/commit`).send({ answerId: initialized.answerId, photos: [{ storageBucket: "sm-visit-photos", storagePath: photoPath, mimeType: "image/jpeg", byteSize: 123 }] }).expect(200)).body;
      photoFile = committed.fileIds[0];
      await answer("photo", { kind: "photo", fileIds: [photoFile], comment: "Original photo comment" }, 1).expect(200);
      await submit(firstVisit).expect(200);
      await submit(firstVisit).expect(200);
      assert.equal((await f.database.select().from(f.schema.smSMDurcharbeitTimeRevisions)).length, 1);
      const [submission] = await f.database.select().from(f.schema.smQuestionnaireSubmissions).where(eq(f.schema.smQuestionnaireSubmissions.id, firstSubmission));
      assert.equal(submission!.assignmentId, null); assert.equal(submission!.oncePerMarketSnapshot, false);
      assert.equal((await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.id, version!.id)))[0]!.oncePerMarket, true, "Legacy policy is never rewritten");
    });
    const historicalSnapshot = async () => {
      const names = ["sm_questionnaire_submissions", "sm_questionnaire_submission_questions", "sm_questionnaire_submission_sections", "sm_question_answers", "sm_question_answer_options", "sm_question_answer_matrix_cells", "sm_question_answer_files", "sm_question_answer_events"];
      return Object.fromEntries(await Promise.all(names.map(async name => {
        const predicate = name === "sm_questionnaire_submissions" ? "id" : name.includes("answer_options") || name.includes("matrix_cells") || name.includes("answer_files") ? "answer_id in (select id from sm_question_answers where submission_id" : "submission_id";
        const query = predicate.includes(" in (") ? `select * from ${name} where ${predicate} = $1) order by id` : `select * from ${name} where ${predicate} = $1 order by id`;
        return [name, (await f.pg.query(query, [firstSubmission])).rows];
      })));
    };
    const frozenHistory = await historicalSnapshot();
    let followUp = "";
    await t.test("follow-up inherits the complete monthly snapshot with new IDs and explicit photo provenance", async () => {
      october = (await currentTargets()).body.targets[0]; assert.equal(october.completed, true); assert.equal(october.visitCount, 1);
      await sm("post", `${targetPath}/${october.id}/start`).send({ expectedRevision: october.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }).expect(409);
      const start = (await sm("post", `${targetPath}/${october.id}/start`).send({ expectedRevision: october.revision, followUp: true, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
      followUp = start.visitId;
      const payload = (await sm("get", visitPath(followUp)).expect(200)).body;
      assert.equal(payload.SMDurcharbeitContext.basisSubmissionId, firstSubmission);
      assert.equal(payload.submission.travelMinutes, null, "Actual/travel time is not carried over");
      assert.equal(payload.submission.visitStartedAt, null);
      for (const section of payload.sections) for (const question of section.questions) {
        assert.notEqual(question.id, byText[question.text].id);
        assert.deepEqual(payload.answers[question.id], question.text === "photo" ? { kind: "photo", fileIds: [photoFile], comment: "Original photo comment" } : values[question.text]);
        assert.equal(payload.answerVersions[question.id], 1);
      }
      const question = payload.sections[0].questions.find((question: any) => question.text === "photo");
      assert.equal(payload.photoFiles[question.id][0].SMDurcharbeitInherited, true);
      assert.equal(payload.photoFiles[question.id][0].id, photoFile);
      assert.deepEqual(await historicalSnapshot(), frozenHistory);
      await admin("patch", `${campaignPath}/targets/${october.id}`).send({ expectedRevision: (await currentTargets()).body.targets[0].revision, reason: "Synthetic close", scope: "month", eligibility: "waived" }).expect(409);
    });
    await t.test("unlinking an inherited photo and discarding a follow-up never deletes its source or monthly completion", async () => {
      await sm("delete", `${visitPath(followUp)}/photos/${photoFile}`).expect(204);
      assert.ok(objects.has(photoPath)); assert.equal(removed.length, 0);
      assert.deepEqual(await historicalSnapshot(), frozenHistory);
      await sm("delete", visitPath(followUp)).send({ confirmation: "SOFT_DELETE_SM_VISIT" }).expect(200);
      october = (await currentTargets()).body.targets[0]; assert.equal(october.completed, true); assert.equal(october.visitCount, 1); assert.equal(october.draftVisitId, null);
      assert.deepEqual(await historicalSnapshot(), frozenHistory);
    });
    await t.test("second physical visit keeps coverage at one and latest monthly state separate from history", async () => {
      followUp = (await sm("post", `${targetPath}/${october.id}/start`).send({ expectedRevision: october.revision, followUp: true, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body.visitId;
      await submit(followUp, 9).expect(200);
      const report = (await admin("get", `${campaignPath}/${campaign.id}/targets?month=2026-10-01`).expect(200)).body;
      assert.deepEqual(report.summary, { required: 1, completed: 1, waived: 0, physicalVisits: 2 });
      assert.equal(report.targets[0].latestVisitId, followUp);
      const results = (await admin("get", `${campaignPath}/${campaign.id}/results?month=2026-10-01`).expect(200)).body;
      assert.equal(results.summary.latestSubmissions, 1); assert.equal(results.summary.physicalVisits, 2);
      assert.equal(results.summary.availablePhotoUploads, 1, "The carried photo is one canonical upload");
      assert.equal(results.summary.actualMinutes, 30); assert.equal(results.summary.travelMinutes, 5);
      assert.equal(results.questionResults.find((q: any) => q.type === "yesno").answered, 1);
      assert.equal((await f.database.select().from(f.schema.smQuestionnaireSubmissions).where(and(eq(f.schema.smQuestionnaireSubmissions.SMDurcharbeitTargetId, october.id), eq(f.schema.smQuestionnaireSubmissions.status, "submitted"), eq(f.schema.smQuestionnaireSubmissions.isCurrent, true)))).length, 2);
      assert.deepEqual(await historicalSnapshot(), frozenHistory);
    });
    await t.test("new month starts empty while old-month drafts remain readable and cannot alter a closed month", async () => {
      october = (await currentTargets()).body.targets[0];
      const lateDraft = (await sm("post", `${targetPath}/${october.id}/start`).send({ expectedRevision: october.revision, followUp: true, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body.visitId;
      now = Date.parse("2026-11-02T12:00:00Z");
      const closed = (await sm("get", visitPath(lateDraft)).expect(200)).body;
      assert.equal(typeof closed.SMDurcharbeitContext.readOnlyReason, "string");
      const photoQuestion = closed.sections[0].questions.find((q: any) => q.type === "photo");
      const [photoAnswer] = await f.database.select().from(f.schema.smQuestionAnswers).where(and(eq(f.schema.smQuestionAnswers.submissionId, closed.submission.id), eq(f.schema.smQuestionAnswers.submissionQuestionId, photoQuestion.id), eq(f.schema.smQuestionAnswers.isCurrent, true)));
      await sm("post", `${visitPath(lateDraft)}/photos/presign`).send({ answerId: photoAnswer!.id, extension: "jpg" }).expect(409);
      await submit(lateDraft, 10, "2026-10-31").expect(409);
      const november = (await sm("get", `${targetPath}?month=2026-11-01`).expect(200)).body.targets[0];
      assert.equal(november.completed, false); assert.notEqual(november.id, october.id);
      const start = (await sm("post", `${targetPath}/${november.id}/start`).send({ expectedRevision: november.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
      const payload = (await sm("get", visitPath(start.visitId)).expect(200)).body;
      assert.ok(Object.values(payload.answers).every(value => value === null));
      assert.equal(payload.SMDurcharbeitContext.basisSubmissionId, null);
      assert.deepEqual(await historicalSnapshot(), frozenHistory);
    });
    await t.test("new tables reject direct browser access with RLS; malformed cross-campaign and submission links fail constraints", async () => {
      const rls = await f.pg.query<{ relname: string; relrowsecurity: boolean }>("select relname,relrowsecurity from pg_class where relname like 'sm_smdurcharbeit_%' and relkind='r'");
      assert.ok(rls.rows.every(row => row.relrowsecurity));
      await f.pg.exec("set role authenticated");
      try { await assert.rejects(f.pg.query("select * from sm_smdurcharbeit_campaigns"), /permission denied/); }
      finally { await f.pg.exec("reset role"); }
      await assert.rejects(f.pg.query("update sm_questionnaire_submissions set assignment_id=$1 where id=$2", [randomUUID(), firstSubmission]), /check constraint|foreign key/);
      assert.deepEqual(await historicalSnapshot(), frozenHistory);
    });
    await t.test("Activities and management distinguish actual executions; archive deduplicates carried photos", async () => {
      const activities = (await sm("get", "/sm/activity/completed").expect(200)).body.visits;
      assert.equal(activities.length, 2);
      assert.ok(activities.every((visit: any) => visit.assignmentId === null && visit.plannedMinutes === null && visit.actualMinutes === 15 && visit.workDate === "2026-10-09"));
      assert.ok(activities.every((visit: any) => visit.SMDurcharbeitContext.month === "2026-10-01" && visit.totals.photoCount === 1));
      const details = (await admin("get", `/admin/sm-activity/completed/${october.latestSubmissionId}`).expect(200)).body;
      assert.equal(details.visit.SMDurcharbeitContext.campaignId, campaign.id);
      assert.equal(details.sections[0].questions.find((q: any) => q.type === "photo").photos[0].id, photoFile);
      const archive = (await admin("get", `/admin/sm-photos?SMDurcharbeitCampaignId=${campaign.id}&SMDurcharbeitMonth=2026-10-01`).expect(200)).body;
      assert.equal(archive.total, 1); assert.equal(archive.photos[0].id, photoFile);
      assert.equal(archive.photos[0].submissionId, firstSubmission, "Photo metadata retains its original physical visit");
      assert.equal(archive.photos[0].SMDurcharbeitMonth, "2026-10-01");
      const manifest = (await admin("get", `/admin/sm-photos/export?SMDurcharbeitCampaignId=${campaign.id}`).expect(200)).body;
      assert.deepEqual(manifest.photos.map((photo: any) => photo.id), [photoFile]);
    });
    await t.test("actual time corrections and reviews preserve original graphs, reject overlaps and never invent Soll", async () => {
      const timePath = "/sm/smdurcharbeit-times", adminTime = "/admin/sm-smdurcharbeit-times";
      const entries = async () => { const response = await sm("get", `${timePath}?from=2026-10-01&to=2026-10-31`); assert.equal(response.status,200,JSON.stringify(response.body)); return response; };
      assert.equal((await entries()).body.entries.length, 2);
      await sm("get", `${timePath}?from=2026-10-01&to=2026-10-31&smUserId=${f.admin}`).expect(400);
      await request(f.app).get(adminTime + "?from=2026-10-01&to=2026-10-31").auth("synthetic-sm", { type: "bearer" }).expect(403);
      const originalTime = (await f.database.select().from(f.schema.smSMDurcharbeitTimeRevisions).where(eq(f.schema.smSMDurcharbeitTimeRevisions.visitId, firstVisit)))[0]!;
      const legacy = await f.assignment();
      const legacyStart = new Date("2026-10-09T06:00:00Z"), legacyEnd = new Date("2026-10-09T06:20:00Z");
      await f.database.update(f.schema.smAssignments).set({ status: "completed", startedAt: legacyStart, completedAt: legacyEnd }).where(eq(f.schema.smAssignments.id, legacy.id));
      const [legacySubmission] = await f.database.insert(f.schema.smQuestionnaireSubmissions).values({ assignmentId: legacy.id,
        questionnaireTemplateId: form.id, questionnaireVersionId: version!.id, smUserId: f.employee, smMarketId: f.market,
        clientSubmissionToken: randomUUID(), questionnaireNameSnapshot: "Synthetic legacy questionnaire", questionnaireVersionSnapshot: 1,
        smNameSnapshot: "Local SM", marketNameSnapshot: "Synthetic legacy market", status: "submitted", submittedAt: legacyEnd, reportingAvailableAt: legacyEnd,
        visitStartedAt: legacyStart, visitCompletedAt: legacyEnd }).returning();
      await f.database.insert(f.schema.smAssignmentTimeSubmissions).values({ assignmentId: legacy.id, revisionNumber: 1, actualMinutes: 20, submittedByUserId: f.employee });
      const staleInput = { expectedRevision: 1, kind: "time_change", requestedStartedAt: "2026-10-09T07:20:00Z", requestedCompletedAt: "2026-10-09T07:35:00Z", reason: "Synthetic stale request", clientRequestToken: randomUUID() };
      const staleRequest = (await sm("post", `${timePath}/${firstVisit}/requests`).send(staleInput).expect(201)).body.request;
      const requestFeed = async () => (await admin("get", "/admin/sm-activity/requests").expect(200)).body.timeRequests;
      const sharedPending = (await requestFeed()).find((row: any) => row.id === staleRequest.id);
      assert.equal(sharedPending.SMDurcharbeitVisitId, firstVisit); assert.equal(sharedPending.assignmentId, null);
      assert.equal(sharedPending.SMDurcharbeitMonth, "2026-10-01"); assert.equal(sharedPending.originalMinutes, originalTime.actualMinutes);
      const correct = { expectedVisitId: firstSubmission, expectedRevision: 1, expectedStartedAt: originalTime.startedAt.toISOString(), expectedCompletedAt: originalTime.completedAt.toISOString(),
        visitStartedAt: "2026-10-09T07:00:00Z", visitCompletedAt: "2026-10-09T07:20:00Z", reason: "Synthetic direct correction" };
      await admin("patch", `${adminTime}/${firstVisit}`).send({ ...correct, visitStartedAt: "2026-10-09T06:05:00Z", visitCompletedAt: "2026-10-09T06:15:00Z" }).expect(409);
      await admin("patch", `${adminTime}/${firstVisit}`).send({ ...correct, visitStartedAt: "2026-11-01T07:00:00Z", visitCompletedAt: "2026-11-01T07:20:00Z" }).expect(409);
      await admin("patch", `${adminTime}/${firstVisit}`).send(correct).expect(200);
      assert.equal((await requestFeed()).find((row: any) => row.id === staleRequest.id).originalStartedAt, originalTime.startedAt.toISOString(), "The shared review panel keeps the request's original source revision");
      await admin("patch", `${adminTime}/${firstVisit}`).send(correct).expect(409);
      await admin("post", `${adminTime}/requests/${staleRequest.id}/approve`).send({}).expect(409);
      await admin("post", `${adminTime}/requests/${staleRequest.id}/reject`).send({}).expect(200);
      const oldTime = (await f.database.select().from(f.schema.smSMDurcharbeitTimeRevisions).where(eq(f.schema.smSMDurcharbeitTimeRevisions.id, originalTime.id)))[0]!;
      assert.deepEqual({ ...oldTime, isCurrent: true }, originalTime, "Only the current pointer changes; original time values remain immutable");
      assert.deepEqual(await historicalSnapshot(), frozenHistory, "Time correction never rewrites questionnaire timestamps or answers");
      const legacyCorrection = { expectedVisitId: legacySubmission!.id, expectedStartedAt: legacyStart.toISOString(), expectedCompletedAt: legacyEnd.toISOString(), reason: "Synthetic legacy correction", visitStartedAt: "2026-10-09T07:10:00Z", visitCompletedAt: "2026-10-09T07:15:00Z" };
      await admin("patch", `/admin/sm-planning/assignments/${legacy.id}/visit-time`).send(legacyCorrection).expect(409);
      await admin("patch", `/admin/sm-planning/assignments/${legacy.id}/visit-time`).send({ ...legacyCorrection, visitStartedAt: "2026-10-09T06:20:00Z", visitCompletedAt: "2026-10-09T06:30:00Z" }).expect(200);
      const input = { ...staleInput, expectedRevision: 2, reason: "Synthetic reviewed time", clientRequestToken: randomUUID() };
      const requested = (await sm("post", `${timePath}/${firstVisit}/requests`).send(input).expect(201)).body.request;
      const pending = (await entries()).body.entries.find((row: any) => row.visitId === firstVisit).pendingTimeChangeRequest;
      assert.equal(pending.originalStartedAt, correct.visitStartedAt.replace("Z", ".000Z"));
      await admin("post", `${adminTime}/requests/${requested.id}/approve`).send({}).expect(200);
      assert.equal((await admin("post", `${adminTime}/requests/${requested.id}/approve`).send({}).expect(200)).body.replayed, true);
      assert.equal((await requestFeed()).find((row: any) => row.id === requested.id).status, "approved");
      assert.equal((await sm("post", `${timePath}/${firstVisit}/requests`).send(input).expect(200)).body.request.id, requested.id);
      const detail = (await admin("get", `/admin/sm-activity/completed/${firstSubmission}`).expect(200)).body;
      assert.equal(detail.visit.startedAt, "2026-10-09T07:20:00.000Z"); assert.equal(detail.visit.SMDurcharbeitContext.timeRevision, 3);
      const managedList = (await admin("get", `/admin/sm-activity/completed?from=2026-10-01&to=2026-10-31&SMDurcharbeitCampaignId=${campaign.id}`).expect(200)).body;
      assert.equal(managedList.visits.length, 2); assert.ok(managedList.visits.every((row: any) => row.SMDurcharbeitContext.campaignId === campaign.id));
      const activities = (await sm("get", "/sm/activity/completed").expect(200)).body.visits;
      assert.equal(activities.find((row: any) => row.submissionId === firstSubmission).visitStartedAt, "2026-10-09T07:20:00.000Z");
      const timesBeforeReads = await f.database.select().from(f.schema.smSMDurcharbeitTimeRevisions);
      const historyPath = `${timePath}/${firstVisit}/history`;
      const history = (await sm("get", `${historyPath}?limit=2`).expect(200)).body;
      assert.deepEqual(history.revisions.map((row: any) => row.revision), [3, 2]);
      assert.equal(history.nextRevision, 2); assert.equal(history.timeRemoved, false);
      assert.equal(history.originalStartedAt, originalTime.startedAt.toISOString());
      assert.equal(history.originalCompletedAt, originalTime.completedAt.toISOString());
      assert.deepEqual((await sm("get", `${historyPath}?beforeRevision=2&limit=2`).expect(200)).body.revisions.map((row: any) => row.revision), [1]);
      assert.equal((await admin("get", `${adminTime}/${firstVisit}/history`).expect(200)).body.revisions.length, 3);
      await sm("get", `${historyPath}?smUserId=${f.admin}`).expect(400);
      await sm("get", `${timePath}/${randomUUID()}/history`).expect(404);
      await request(f.app).get(historyPath).auth("synthetic-gm", { type: "bearer" }).expect(403);
      assert.deepEqual(await f.database.select().from(f.schema.smSMDurcharbeitTimeRevisions), timesBeforeReads, "Loading time history does not change revisions");
      const removeInput = { expectedRevision: 1, kind: "deletion", requestedStartedAt: null, requestedCompletedAt: null, reason: "Synthetic time only deletion", clientRequestToken: randomUUID() };
      const remove = (await sm("post", `${timePath}/${followUp}/requests`).send(removeInput).expect(201)).body.request;
      await admin("post", `${adminTime}/requests/${remove.id}/approve`).send({}).expect(200);
      await sm("post", `${timePath}/${followUp}/requests`).send(removeInput).expect(200);
      const after = (await entries()).body.entries;
      assert.equal(after.find((row: any) => row.visitId === followUp).actualMinutes, null);
      const removedHistory = (await sm("get", `${timePath}/${followUp}/history`).expect(200)).body;
      assert.equal(removedHistory.timeRemoved, true); assert.equal(removedHistory.revisions.length, 1);
      assert.equal(removedHistory.revisions[0].isCurrent, false);
      assert.equal(after.find((row: any) => row.visitId === firstVisit).actualMinutes, 15);
      const profile = (await sm("get", "/sm/planning/profile").expect(200)).body;
      assert.equal(profile.summary.assignmentCount, 1, "Monthly visits do not become dated profile assignments");
      assert.equal(profile.summary.SMDurcharbeitVisitCount, 1, "Only the current time revision contributes after time-only deletion");
      assert.equal(profile.summary.actualMinutes, 30, "The corrected legacy ten minutes and monthly fifteen plus five travel minutes contribute once");
      assert.equal((await currentTargets()).body.targets[0].completed, true);
      assert.deepEqual(await historicalSnapshot(), frozenHistory);
    });
    await t.test("approved corrections stale a follow-up basis; invalidation falls back and retains authorized photo origins", async () => {
      now = Date.parse("2026-10-09T12:00:00Z");
      october = (await currentTargets()).body.targets[0];
      const latestSubmission = october.latestSubmissionId;
      const managed = (await admin("get", `/admin/sm-activity/completed/${latestSubmission}`).expect(200)).body;
      const question = managed.sections[0].questions.find((q: any) => q.text === "yesno");
      const token = randomUUID(), changed = { kind: "choice", optionCode: question.options[1].code, comment: "Reviewed synthetic correction" };
      const correction = { expectedVersion: managed.version, clientMutationToken: token, reason: "Synthetic review", changes: [{ questionId: question.id, answer: changed }] };
      await admin("post", `/admin/sm-activity/completed/${latestSubmission}/corrections`).send(correction).expect(200);
      await admin("post", `/admin/sm-activity/completed/${latestSubmission}/corrections`).send(correction).expect(200);
      assert.deepEqual(await historicalSnapshot(), frozenHistory, "Correcting a follow-up never rewrites the first visit");
      const conflict = await submit(october.draftVisitId, 10);
      assert.equal(conflict.status, 409); assert.equal(conflict.body.code, "smdurcharbeit_basis_changed");
      const unchangedDraft = (await sm("get", visitPath(october.draftVisitId)).expect(200)).body;
      assert.deepEqual(unchangedDraft.answers[unchangedDraft.sections[0].questions.find((q: any) => q.text === "yesno").id], values.yesno);
      await sm("delete", visitPath(october.draftVisitId)).send({ confirmation: "SOFT_DELETE_SM_VISIT" }).expect(200);
      const invalidate = async (id: string) => {
        const pending = (await sm("post", `/sm/activity/submissions/${id}/delete-requests`).send({ reason: "Synthetic approved invalidation", clientRequestToken: randomUUID() }).expect(201)).body.request;
        const reviewed = await admin("post", `/admin/sm-activity/submission-delete-requests/${pending.id}/approve`).send({ adminNote: "Synthetic review" });
        assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
        await admin("post", `/admin/sm-activity/submission-delete-requests/${pending.id}/approve`).send({ adminNote: "Synthetic review" }).expect(200);
      };
      await invalidate(latestSubmission);
      october = (await currentTargets()).body.targets[0];
      assert.equal(october.completed, true); assert.equal(october.latestSubmissionId, firstSubmission); assert.equal(october.visitCount, 1);
      const start = (await sm("post", `${targetPath}/${october.id}/start`).send({ expectedRevision: october.revision, followUp: true, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
      await submit(start.visitId, 10).expect(200);
      await invalidate(firstSubmission);
      const retained = (await sm("get", visitPath(start.visitId)).expect(200)).body;
      const retainedPhoto = retained.sections[0].questions.find((q: any) => q.type === "photo");
      assert.equal(retained.photoFiles[retainedPhoto.id][0].id, photoFile);
      const archive = (await admin("get", `/admin/sm-photos?SMDurcharbeitCampaignId=${campaign.id}`).expect(200)).body;
      assert.equal(archive.total, 1); assert.equal(archive.photos[0].submissionId, firstSubmission);
      assert.ok(objects.has(photoPath)); assert.equal(removed.length, 0, "Approved questionnaire invalidation does not delete retained storage");
      const photoRequestResponse = await sm("post", `/sm/activity/submissions/${retained.submission.id}/questions/${retainedPhoto.id}/change-requests`).send({ answer: { kind: "empty" }, reason: "Synthetic remove retained photo", clientRequestToken: randomUUID() });
      assert.equal(photoRequestResponse.status, 201, JSON.stringify(photoRequestResponse.body));
      const photoRequest = photoRequestResponse.body.request;
      await admin("post", `/admin/sm-activity/answer-change-requests/${photoRequest.id}/approve`).send({}).expect(200);
      assert.equal((await admin("get", `/admin/sm-photos?SMDurcharbeitCampaignId=${campaign.id}`).expect(200)).body.total, 0);
      await invalidate(retained.submission.id);
      october = (await currentTargets()).body.targets[0];
      assert.equal(october.completed, false); assert.equal(october.latestSubmissionId, null); assert.equal(october.visitCount, 0);
    });
    await t.test("invalidating answers does not erase worked time; explicit time review releases the interval", async () => {
      const times = (await sm("get", "/sm/smdurcharbeit-times?from=2026-10-01&to=2026-10-31").expect(200)).body.entries;
      const retained = times.find((row: any) => row.visitId === firstVisit);
      assert.equal(retained.questionnaireComplete, false); assert.equal(retained.actualMinutes, 15);
      const fresh = (await sm("post", `${targetPath}/${october.id}/start`).send({ expectedRevision: october.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
      const payload = (await sm("get", visitPath(fresh.visitId)).expect(200)).body;
      const question = payload.sections[0].questions.find((row: any) => row.text === "yesno");
      await sm("put", `${visitPath(fresh.visitId)}/answers/${question.id}`).send({ answer: { kind: "choice", optionCode: question.options[0].code, comment: "Synthetic new observation" }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
      const interval = { visitStartedAt: "2026-10-09T07:20:00Z", visitCompletedAt: "2026-10-09T07:35:00Z", clientMutationToken: randomUUID() };
      const conflict = await sm("post", `${visitPath(fresh.visitId)}/submit`).send(interval).expect(409);
      assert.equal(conflict.body.code, "sm_visit_time_overlap");
      const removed = (await sm("post", `/sm/smdurcharbeit-times/${firstVisit}/requests`).send({ expectedRevision: 3, kind: "deletion", requestedStartedAt: null, requestedCompletedAt: null, reason: "Synthetic reviewed time removal", clientRequestToken: randomUUID() }).expect(201)).body.request;
      await admin("post", `/admin/sm-smdurcharbeit-times/requests/${removed.id}/approve`).send({}).expect(200);
      await sm("post", `${visitPath(fresh.visitId)}/submit`).send(interval).expect(200);
      assert.equal((await currentTargets()).body.targets[0].completed, true);
    });
    await t.test("audited admin draft cancellation releases paused and closed work without changing completed history", async () => {
      now = Date.parse("2026-10-09T12:00:00Z");
      const baseline = await historicalSnapshot();
      const fileBaseline = await f.database.select().from(f.schema.smQuestionAnswerFiles);
      const current = (await currentTargets()).body.targets[0];
      const opened = (await sm("post", `${targetPath}/${current.id}/start`).send({ expectedRevision: current.revision, followUp: true, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
      const campaignRow = (await admin("get", campaignPath).expect(200)).body.campaigns.find((row: any) => row.id === campaign.id);
      await admin("patch", `${campaignPath}/${campaign.id}/state`).send({ expectedRevision: campaignRow.revision, status: "paused", reason: "Synthetic recovery pause" }).expect(200);
      const protectedTarget = (await currentTargets()).body.targets[0];
      const cancelPath = `${campaignPath}/targets/${current.id}/cancel-draft`;
      const input = { expectedRevision: protectedTarget.revision, visitId: opened.visitId, reason: "Synthetic reviewed cancellation", confirmation: "CANCEL_SMDURCHARBEIT_DRAFT" };
      await request(f.app).post(cancelPath).auth("synthetic-sm", { type: "bearer" }).send(input).expect(403);
      await admin("post", cancelPath).send({ ...input, expectedRevision: current.revision }).expect(409);
      await admin("post", cancelPath).send({ ...input, visitId: firstVisit }).expect(409);
      await admin("post", cancelPath).send({ ...input, confirmation: "" }).expect(400);
      await sm("delete", visitPath(opened.visitId)).send({ confirmation: "SOFT_DELETE_SM_VISIT" }).expect(409);
      await admin("post", cancelPath).send(input).expect(200);
      await admin("post", cancelPath).send(input).expect(409);
      const restored = (await currentTargets()).body.targets[0];
      assert.equal(restored.draftVisitId, null); assert.equal(restored.completed, true); assert.equal(restored.latestSubmissionId, current.latestSubmissionId);
      const events = (await admin("get", `${campaignPath}/${campaign.id}/history`).expect(200)).body.events;
      assert.ok(events.some((event: any) => event.action === "draft_cancelled_by_admin" && event.actorUserId === f.admin && event.reason === input.reason));
      const paused = (await admin("get", campaignPath).expect(200)).body.campaigns.find((row: any) => row.id === campaign.id);
      await admin("patch", `${campaignPath}/${campaign.id}/state`).send({ expectedRevision: paused.revision, status: "published", reason: "Synthetic recovery resumed" }).expect(200);
      const second = (await sm("post", `${targetPath}/${restored.id}/start`).send({ expectedRevision: restored.revision, followUp: true, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
      now = Date.parse("2026-11-02T12:00:00Z");
      const closedTarget = (await currentTargets()).body.targets[0];
      await admin("post", cancelPath).send({ ...input, expectedRevision: closedTarget.revision, visitId: second.visitId, reason: "Synthetic closed-month resolution" }).expect(200);
      assert.deepEqual(await historicalSnapshot(), baseline);
      const remainingFiles = await f.database.select().from(f.schema.smQuestionAnswerFiles);
      assert.deepEqual(remainingFiles, fileBaseline, "Inherited historical uploads are not deleted");
    });
  } finally { await f.pg.close(); }
});
