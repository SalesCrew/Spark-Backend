import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { and, eq, sql } from "drizzle-orm";
import express from "express";
import request from "supertest";
import { isolatedModule } from "../tests/isolated-module.js";
import * as management from "./sm-management.js";
import * as planning from "./sm-planning.shared.js";
import * as visitShared from "./sm-visit.shared.js";
import * as dashboardShared from "./sm-dashboard.shared.js";
import * as conditionalVisibility from "./lib/conditional-visibility.js";
import * as comments from "./sm-comment.shared.js";
import * as planningLock from "./sm-planning-lock.js";
import * as profileShared from "./sm-profile.shared.js";
import * as timeOverlap from "./sm-time-overlap.js";
import * as holidaysShared from "./sm-holidays.shared.js";
import { isRoleAllowedForEndpoint } from "./lib/admin-role.js";
import * as schema from "./lib/schema.js";
import { applySmAdminCorrection, loadSmManagementState, smAdminCorrectionSchema, smManagementDetail, SmManagementError, type SmManagementTx } from "./sm-management.js";

// These tests cannot open a production connection. SQL runs only in a fresh in-memory PostgreSQL.
test("SM management: real SM schema, immutable history and atomic corrections", async t => {
  const pg = new PGlite(), database = drizzle(pg, { schema });
  const txRun = <T>(action: (tx: SmManagementTx) => Promise<T>) => database.transaction(tx => action(tx as unknown as SmManagementTx));
  try {
    await pg.exec(`create role anon; create role authenticated; create role service_role;
      create table users(id uuid primary key, first_name text, last_name text, role text, is_active boolean default true, deleted_at timestamptz, sm_travel_time_enabled boolean default true);`);
    for (const migration of ["0088_sm_markets.sql", "0089_sm_questionnaire_domain.sql", "0091_sm_enforce_soft_deletes.sql", "0092_sm_market_assignments.sql", "0093_sm_planning.sql", "0097_sm_visit_runtime_timing.sql", "0099_sm_zeiterfassung_requests.sql", "0100_sm_activity_request_audit.sql", "0102_sm_time_request_timestamps.sql", "0103_sm_time_request_equal_duration.sql", "0105_sm_global_questionnaire_assignment.sql", "0108_sm_market_account_assignments.sql"]) {
      await pg.exec(await readFile(new URL(`../drizzle/${migration}`, import.meta.url), "utf8"));
    }
    const admin = randomUUID(), employee = randomUUID(), market = randomUUID();
    await pg.query("insert into users(id,first_name,last_name,role) values ($1,'Local','Admin','sm_admin'),($2,'Local','SM','sm')", [admin, employee]);
    await pg.query("insert into sm_markets(id,name,chain,address,postal_code,city,region) values ($1,'Local Billa','Billa','Testgasse 1','1010','Wien','Ost')", [market]);
    const [template] = await database.insert(schema.smQuestionnaireTemplates).values({ stableCode: "local-template" }).returning();
    const [templateVersion] = await database.insert(schema.smQuestionnaireVersions).values({ questionnaireTemplateId: template!.id, versionNumber: 1, name: "Original Fragebogen" }).returning();
    const [module] = await database.insert(schema.smModules).values({ stableCode: "local-module" }).returning();
    const [moduleVersion] = await database.insert(schema.smModuleVersions).values({ moduleId: module!.id, versionNumber: 1, name: "Original Modul" }).returning();
    const seed = async () => {
      const [submission] = await database.insert(schema.smQuestionnaireSubmissions).values({ questionnaireTemplateId: template!.id, questionnaireVersionId: templateVersion!.id,
        smUserId: employee, smMarketId: market, clientSubmissionToken: randomUUID(), questionnaireNameSnapshot: "Original Fragebogen", questionnaireVersionSnapshot: 1,
        smNameSnapshot: "Original SM", marketNameSnapshot: "Original Markt", status: "submitted", submittedAt: new Date("2026-08-31T10:00:00Z"), reportingAvailableAt: new Date("2026-08-31T10:00:00Z"),
        visitStartedAt: new Date("2026-08-31T09:00:00Z"), visitCompletedAt: new Date("2026-08-31T10:00:00Z"), resolvedQuestionCount: 4, answeredQuestionCount: 1, travelMinutes: 25 }).returning();
      const [section] = await database.insert(schema.smQuestionnaireSubmissionSections).values({ submissionId: submission!.id, moduleVersionId: moduleVersion!.id, moduleCodeSnapshot: "local-module", moduleNameSnapshot: "Original Modul" }).returning();
      const types = ["yesno", "text", "photo", "numeric"] as const;
      const questionRows = [];
      for (const [index, type] of types.entries()) {
        const [root] = await database.insert(schema.smQuestions).values({ stableCode: `q-${randomUUID()}` }).returning();
        const [version] = await database.insert(schema.smQuestionVersions).values({ questionId: root!.id, versionNumber: 1, questionType: type, questionText: "Heutige Vorlage anders" }).returning();
        const [question] = await database.insert(schema.smQuestionnaireSubmissionQuestions).values({ submissionId: submission!.id, submissionSectionId: section!.id,
          questionVersionId: version!.id, questionCodeSnapshot: root!.stableCode, questionTypeSnapshot: type, questionTextSnapshot: `Ursprüngliche Frage ${index + 1}`,
          requiredSnapshot: index === 0, metricRoleSnapshot: index === 0 ? "oos_detection" : "none", oosCategorySnapshot: index === 0 ? "water_near_water" : null,
          configSnapshot: index === 0 ? { commentTrigger: { mode: "options", optionCodes: ["no"] } } : {},
          answerOptionsSnapshot: index === 0 ? [{ code: "yes", label: "Ja", metricOutcomeCode: "oos_present", earnedPoints: "1", possiblePoints: "1" }, { code: "no", label: "Nein", metricOutcomeCode: "oos_absent", earnedPoints: "0", possiblePoints: "1" }] : [], orderIndex: index }).returning();
        questionRows.push(question!);
      }
      const [answer] = await database.insert(schema.smQuestionAnswers).values({ submissionId: submission!.id, submissionQuestionId: questionRows[0]!.id,
        answerVersion: 1, answerState: "answered", valueJson: { kind: "choice", optionCode: "yes" }, answeredByUserId: employee, answeredAt: new Date("2026-08-31T09:30:00Z") }).returning();
      await database.insert(schema.smQuestionAnswerOptions).values({ answerId: answer!.id, optionCodeSnapshot: "yes", optionLabelSnapshot: "Ja", metricOutcomeCodeSnapshot: "oos_present" });
      return { submission: submission!, questions: questionRows, answer: answer! };
    };
    const state = (id: string) => txRun(tx => loadSmManagementState(tx, id));
    const input = async (id: string, changes: Array<{ questionId: string; answer: unknown }>) => smAdminCorrectionSchema.parse({ expectedVersion: (await state(id)).version,
      clientMutationToken: randomUUID(), reason: "Antwort nach Rücksprache korrigiert", changes });
    const apply = (id: string, value: ReturnType<typeof smAdminCorrectionSchema.parse>) => txRun(tx => applySmAdminCorrection(tx, id, value, admin));

    await t.test("snapshot detail uses old question/name and retains original timestamps", async () => {
      const fixture = await seed(), detail = await txRun(tx => smManagementDetail(tx, fixture.submission.id));
      assert.equal(detail.visit.smName, "Original SM"); assert.equal(detail.visit.questionnaireName, "Original Fragebogen");
      assert.match(detail.sections[0]!.questions[0]!.text, /Ursprüngliche/);
      assert.equal(detail.visit.startedAt, "2026-08-31T09:00:00.000Z");
    });
    await t.test("correction versions answers, stores metric snapshot, preserves time and employee", async () => {
      const fixture = await seed(), id = fixture.submission.id;
      const payload = await input(id, [{ questionId: fixture.questions[0]!.id, answer: { kind: "choice", optionCode: "no", comment: "Kein OOS" } }]);
      const result = await apply(id, payload); assert.equal(result.replayed, false);
      const next = await state(id), current = next.answers[0]!;
      assert.equal(current.supersedesAnswerId, fixture.answer.id); assert.equal(current.answerVersion, 2);
      assert.equal(current.answeredByUserId, admin); assert.equal(next.submission.smUserId, employee);
      assert.equal((current.valueJson as { comment: string }).comment, "Kein OOS");
      assert.equal(next.submission.visitStartedAt!.getTime(), fixture.submission.visitStartedAt!.getTime());
      assert.equal(next.submission.submittedAt!.getTime(), fixture.submission.submittedAt!.getTime()); assert.equal(next.submission.travelMinutes, 25);
      const [old] = await database.select().from(schema.smQuestionAnswers).where(eq(schema.smQuestionAnswers.id, fixture.answer.id));
      assert.deepEqual(old!.valueJson, { kind: "choice", optionCode: "yes" }); assert.equal(old!.isCurrent, false); assert.equal(old!.isDeleted, false);
      const selected = await database.select().from(schema.smQuestionAnswerOptions).where(eq(schema.smQuestionAnswerOptions.answerId, current.id));
      assert.equal(selected[0]!.metricOutcomeCodeSnapshot, "oos_absent");
      assert.equal((await database.select().from(schema.smQuestionAnswerEvents).where(eq(schema.smQuestionAnswerEvents.submissionId, id))).length, 2);
    });
    await t.test("lost response retry is idempotent; token reuse for different content rejected", async () => {
      const fixture = await seed(), id = fixture.submission.id;
      const payload = await input(id, [{ questionId: fixture.questions[1]!.id, answer: { kind: "text", value: "Notiz" } }]);
      const first = await apply(id, payload), second = await apply(id, payload);
      assert.equal(second.replayed, true); assert.deepEqual(second.result, first.result);
      await assert.rejects(apply(id, { ...payload, reason: "Anderer Grund" }), (e: unknown) => e instanceof SmManagementError && e.code === "sm_management_token_reused");
      assert.equal((await state(id)).answers.length, 2);
    });
    await t.test("triggered comments are required even on a non-required question", async () => {
      const fixture = await seed(), id = fixture.submission.id, question = fixture.questions[0]!;
      await database.update(schema.smQuestionnaireSubmissionQuestions).set({ requiredSnapshot: false }).where(eq(schema.smQuestionnaireSubmissionQuestions.id, question.id));
      const payload = await input(id, [{ questionId: question.id, answer: { kind: "choice", optionCode: "no" } }]);
      await assert.rejects(apply(id, payload), (e: unknown) => e instanceof SmManagementError && e.code === "sm_management_required_answers");
      assert.equal((await state(id)).answers[0]!.id, fixture.answer.id);
    });
    await t.test("stale version and foreign questions reject with no writes", async () => {
      const fixture = await seed(), other = await seed(), id = fixture.submission.id;
      const payload = await input(id, [{ questionId: fixture.questions[1]!.id, answer: { kind: "text", value: "A" } }]);
      await apply(id, payload);
      await assert.rejects(apply(id, { ...payload, clientMutationToken: randomUUID() }), (e: unknown) => e instanceof SmManagementError && e.code === "sm_management_version_conflict");
      await assert.rejects(apply(id, await input(id, [{ questionId: other.questions[1]!.id, answer: { kind: "text", value: "Fremd" } }])));
      assert.equal((await state(other.submission.id)).answers.length, 1);
    });
    await t.test("newly required conditional question must be completed in the same transaction", async () => {
      const fixture = await seed(), id = fixture.submission.id, trigger = fixture.questions[0]!, target = fixture.questions[1]!;
      await database.update(schema.smQuestionnaireSubmissionQuestions).set({ logicRulesSnapshot: [{ triggerQuestionId: trigger.questionCodeSnapshot, operator: "equals", triggerValue: "Nein", action: "show", targetQuestionIds: [target.questionCodeSnapshot] }] }).where(eq(schema.smQuestionnaireSubmissionQuestions.id, trigger.id));
      await database.update(schema.smQuestionnaireSubmissionQuestions).set({ isApplicable: false, applicabilityReason: "hidden_by_rule", requiredSnapshot: true }).where(eq(schema.smQuestionnaireSubmissionQuestions.id, target.id));
      const change = { questionId: trigger.id, answer: { kind: "choice", optionCode: "no", comment: "Begründung" } };
      await assert.rejects(apply(id, await input(id, [change])), (e: unknown) => e instanceof SmManagementError && e.code === "sm_management_required_answers");
      assert.equal((await state(id)).answers[0]!.id, fixture.answer.id);
      await apply(id, await input(id, [change, { questionId: target.id, answer: { kind: "text", value: "Neue Pflichtantwort" } }]));
      assert.equal((await state(id)).questions.find(row => row.id === target.id)!.isApplicable, true);
      await apply(id, await input(id, [{ questionId: trigger.id, answer: { kind: "choice", optionCode: "yes" } }]));
      assert.equal((await state(id)).answers.some(row => row.submissionQuestionId === target.id), false);
      // Reappearance must append after the invalidated version, never collide with version 1.
      await apply(id, await input(id, [change, { questionId: target.id, answer: { kind: "text", value: "Erneut ausgefüllt" } }]));
      assert.equal((await state(id)).answers.find(row => row.submissionQuestionId === target.id)!.answerVersion, 2);
    });
    await t.test("audit failure rolls back the complete correction", async () => {
      const fixture = await seed(), id = fixture.submission.id, before = await state(id);
      const payload = await input(id, [{ questionId: fixture.questions[1]!.id, answer: { kind: "text", value: "Rollback" } }]);
      await pg.exec("create function local_reject_event() returns trigger language plpgsql as $$ begin raise exception 'Audit failed'; end $$; create trigger local_reject_event before insert on sm_question_answer_events for each row execute function local_reject_event();");
      await assert.rejects(apply(id, payload));
      await pg.exec("drop trigger local_reject_event on sm_question_answer_events; drop function local_reject_event();");
      assert.equal((await state(id)).version, before.version);
    });
    await t.test("photo corrections map new owning file IDs, retain original object, reject foreign files", async () => {
      const fixture = await seed(), id = fixture.submission.id, photoQuestion = fixture.questions[2]!;
      const [photoAnswer] = await database.insert(schema.smQuestionAnswers).values({ submissionId: id, submissionQuestionId: photoQuestion.id,
        answerState: "answered", answeredAt: new Date(), valueJson: { kind: "photo", fileIds: [randomUUID()] } }).returning();
      const [first, second] = await database.insert(schema.smQuestionAnswerFiles).values(["first.jpg", "second.jpg"].map(path => ({ answerId: photoAnswer!.id, storageBucket: "sm-visit-photos", storagePath: `local/${id}/${path}` }))).returning();
      await apply(id, await input(id, [{ questionId: photoQuestion.id, answer: { kind: "photo", fileIds: [first!.id] } }]));
      const next = await state(id), current = next.answers.find(row => row.submissionQuestionId === photoQuestion.id)!;
      const currentFiles = next.photos.filter(row => row.answerId === current.id);
      assert.equal(currentFiles.length, 1); assert.equal(currentFiles[0]!.storagePath, first!.storagePath);
      assert.deepEqual(current.valueJson, { kind: "photo", fileIds: [currentFiles[0]!.id] });
      assert.notEqual(currentFiles[0]!.id, first!.id);
      await assert.rejects(apply(id, await input(id, [{ questionId: photoQuestion.id, answer: { kind: "photo", fileIds: [second!.id] } }])));
      assert.equal((await database.select().from(schema.smQuestionAnswerFiles).where(eq(schema.smQuestionAnswerFiles.answerId, photoAnswer!.id))).length, 2);
    });
    const storedObjects = new Map<string, { size: number; contentType: string; bytes?: Buffer }>();
    const issuedUploadPaths = new Set<string>();
    const authMock = { requireAuth: (roles: schema.UserRole[]) => (req: express.Request, res: express.Response, next: express.NextFunction) => {
      // Auth identity is injected in this isolated HTTP harness; the real endpoint's role declaration is exercised.
      const role = req.header("x-local-role") as schema.UserRole | undefined;
      if (!role) { res.sendStatus(401); return; }
      if (!isRoleAllowedForEndpoint(role, roles)) { res.sendStatus(403); return; }
      Object.assign(req, { authUser: { appUserId: req.header("x-local-actor") ?? admin, role } }); next();
    } };
    const route = await isolatedModule<typeof import("./routes/sm-management.js")>(new URL("./routes/sm-management.ts", import.meta.url), {
      "../lib/db.js": { db: database }, "../lib/schema.js": schema, "../sm-management.js": management,
      "../sm-planning.shared.js": planning, "../sm-visit.shared.js": visitShared, "../middleware/auth.js": authMock,
      "../config/env.js": { env: { JWT_SECRET: "isolated-test-only-key-never-used-outside-this-process" } },
      "../lib/supabase.js": { supabaseAdmin: { storage: { from: (bucket: string) => {
        assert.equal(bucket, "sm-visit-photos");
        return {
          info: async (path: string) => ({ data: storedObjects.get(path) ?? null, error: null }),
          createSignedUrl: async (path: string) => ({ data: { signedUrl: process.env.SM_MANAGEMENT_BROWSER_FIXTURE === "1" ? `http://127.0.0.1:4017/fixture-photo?path=${encodeURIComponent(path)}` : `https://local-storage.invalid/${path}` }, error: null }),
          createSignedUploadUrl: async (path: string, options: { upsert: boolean }) => {
            assert.equal(options.upsert, false); issuedUploadPaths.add(path);
            return { data: { path, token: "local", signedUrl: process.env.SM_MANAGEMENT_BROWSER_FIXTURE === "1" ? `http://127.0.0.1:4017/fixture-upload?path=${encodeURIComponent(path)}` : `https://local-storage.invalid/${path}` }, error: null };
          },
        };
      } } } },
    });
    const app = express();
    if (process.env.SM_MANAGEMENT_BROWSER_FIXTURE === "1") app.use((req, res, next) => {
      res.setHeader("Access-Control-Allow-Origin", "http://localhost:3017");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type, x-local-role");
      res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS");
      if (req.method === "OPTIONS") { res.sendStatus(204); return; } next();
    });
    app.use(express.json()); app.use("/completed", route.adminSmManagementRouter);
    app.use((error: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => { res.status(500).json({ error: error.message }); });
    const http = (method: "get" | "post", path: string) => request(app)[method](`/completed${path}`).set("x-local-role", "sm_admin");

    const activityRoute = await isolatedModule<typeof import("./routes/sm-activity.js")>(new URL("./routes/sm-activity.ts", import.meta.url), {
      "../lib/db.js": { db: database }, "../lib/schema.js": schema, "./sm-management.js": route,
      "../lib/conditional-visibility.js": conditionalVisibility, "../sm-comment.shared.js": comments,
      "../sm-planning.shared.js": planning, "../sm-visit.shared.js": visitShared, "../middleware/auth.js": authMock,
    });
    app.use("/activity", activityRoute.smActivityRouter); app.use("/admin-activity", activityRoute.adminSmActivityRouter);
    app.use((error: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => { res.status(500).json({ error: error.message }); });
    const employeePost = (path: string) => request(app).post(`/activity${path}`).set("x-local-role", "sm").set("x-local-actor", employee);
    const adminPost = (path: string) => request(app).post(`/admin-activity${path}`).set("x-local-role", "sm_admin");
    const harmlessLogger = { logger: { warn: () => {}, error: () => {}, info: () => {} }, logAction: () => {}, startActionTimer: () => () => {} };
    const holidays = await isolatedModule<typeof import("./sm-holiday-planning.js")>(new URL("./sm-holiday-planning.ts", import.meta.url), {
      "./lib/db.js": { db: database }, "./lib/schema.js": schema, "./sm-planning-lock.js": planningLock,
      "./sm-planning.shared.js": planning, "./sm-holidays.shared.js": holidaysShared,
    });
    const planningRoute = await isolatedModule<typeof import("./routes/sm-planning.js")>(new URL("./routes/sm-planning.ts", import.meta.url), {
      "../lib/db.js": { db: database }, "../lib/schema.js": schema, "../lib/logger.js": harmlessLogger, "../middleware/auth.js": authMock,
      "../sm-planning.shared.js": planning, "../sm-planning-lock.js": planningLock, "../sm-time-overlap.js": timeOverlap,
      "../sm-profile.shared.js": profileShared,
      "../sm-holiday-planning.js": holidays, "../sm-series-management.js": { smSeriesManagementRouter: express.Router() },
    });
    const visitRoute = await isolatedModule<typeof import("./routes/sm-visits.js")>(new URL("./routes/sm-visits.ts", import.meta.url), {
      "../lib/db.js": { db: database }, "../lib/schema.js": schema, "../lib/logger.js": harmlessLogger, "../middleware/auth.js": authMock,
      "../lib/conditional-visibility.js": conditionalVisibility, "../sm-comment.shared.js": comments, "../sm-visit.shared.js": visitShared,
      "../sm-planning.shared.js": planning, "../sm-planning-lock.js": planningLock, "../sm-time-overlap.js": timeOverlap,
      "../sm-market-deactivation.js": { smDeactivationToday: () => "2026-09-15" },
      "../lib/supabase.js": { supabaseAdmin: { storage: { from: () => ({ createSignedUrl: async () => ({ data: { signedUrl: "https://local-storage.invalid/photo" }, error: null }) }) } } },
    });
    app.use("/employee-planning", planningRoute.smPlanningRouter); app.use("/admin-planning", planningRoute.adminSmPlanningRouter); app.use("/visits", visitRoute.smVisitsRouter);
    app.use((error: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => { res.status(500).json({ error: error.message }); });
    const employeeGet = (path: string) => request(app).get(path).set("x-local-role", "sm").set("x-local-actor", employee);
    const assignment = async (status: typeof schema.smAssignments.$inferInsert.status = "planned", extra: Partial<typeof schema.smAssignments.$inferInsert> = {}) => {
      const [row] = await database.insert(schema.smAssignments).values({ idempotencyKey: randomUUID(), sourceType: "single", status,
        originalWorkDate: "2026-09-15", originalSmUserId: employee, originalSmMarketId: market, originalMarketInternalId: "LOCAL-1", originalPlannedMinutes: 60,
        createdByUserId: admin, updatedByUserId: admin,
        ...(status === "cancelled" ? { cancelledAt: new Date(), cancelledByUserId: admin, cancellationReason: "Lokaler Absagetest", statusBeforeCancellation: "planned" as const } : {}), ...extra }).returning();
      return row!;
    };

    await t.test("SM admin directly corrects visit stamps and duration with history, guards and no GM access", async () => {
      const originalStart = "2026-08-31T09:00:00.000Z", originalEnd = "2026-08-31T10:00:00.000Z";
      const scheduled = await assignment("completed", { originalWorkDate: "2026-08-31", startedAt: new Date(originalStart), completedAt: new Date(originalEnd) });
      const fixture = await seed();
      await database.update(schema.smQuestionnaireSubmissions).set({ assignmentId: scheduled.id, visitTimeMode: "manual", manualVisitMinutes: 60 }).where(eq(schema.smQuestionnaireSubmissions.id, fixture.submission.id));
      const [originalTime] = await database.insert(schema.smAssignmentTimeSubmissions).values({ assignmentId: scheduled.id, revisionNumber: 1, actualMinutes: 60, submittedByUserId: employee }).returning();
      const endpoint = `/admin-planning/assignments/${scheduled.id}/visit-time`;
      const payload = { expectedVisitId: fixture.submission.id, expectedStartedAt: originalStart, expectedCompletedAt: originalEnd,
        visitStartedAt: "2026-08-31T09:05:00.000Z", visitCompletedAt: "2026-08-31T10:15:00.000Z", reason: "Start vor Ort falsch erfasst" };
      await request(app).patch(endpoint).set("x-local-role", "sm").set("x-local-actor", employee).send(payload).expect(403);
      await request(app).patch(endpoint).set("x-local-role", "gm").send(payload).expect(403);
      await request(app).patch(endpoint).set("x-local-role", "sm_admin").send({ ...payload, visitCompletedAt: payload.visitStartedAt }).expect(400);
      await request(app).patch(endpoint).set("x-local-role", "sm_admin").send({ ...payload, expectedVisitId: randomUUID() }).expect(409);
      const updated = await request(app).patch(endpoint).set("x-local-role", "sm_admin").send(payload).expect(200);
      assert.equal(updated.body.actualMinutes, 70); assert.equal(updated.body.revisionNumber, 2); assert.equal(updated.body.replayed, false);
      const [visit] = await database.select().from(schema.smQuestionnaireSubmissions).where(eq(schema.smQuestionnaireSubmissions.id, fixture.submission.id));
      assert.equal(visit!.visitStartedAt?.toISOString(), payload.visitStartedAt);
      assert.equal(visit!.visitCompletedAt?.toISOString(), payload.visitCompletedAt);
      assert.equal(visit!.manualVisitMinutes, 70);
      assert.equal(visit!.submittedAt?.toISOString(), originalEnd, "submission time remains an audit event, not the corrected end");
      const versions = await database.select().from(schema.smAssignmentTimeSubmissions).where(eq(schema.smAssignmentTimeSubmissions.assignmentId, scheduled.id));
      assert.deepEqual(versions.map(row => [row.revisionNumber, row.actualMinutes, row.isCurrent]), [[1, 60, false], [2, 70, true]]);
      assert.equal(versions[1]!.supersedesSubmissionId, originalTime!.id);
      const [event] = await database.select().from(schema.smAssignmentEvents).where(eq(schema.smAssignmentEvents.assignmentId, scheduled.id));
      assert.equal(event!.beforeState.startedAt, originalStart); assert.equal(event!.afterState.completedAt, payload.visitCompletedAt);
      const replay = await request(app).patch(endpoint).set("x-local-role", "sm_admin").send(payload).expect(200);
      assert.equal(replay.body.replayed, true);
      await request(app).patch(endpoint).set("x-local-role", "sm_admin").send({ ...payload, visitStartedAt: "2026-08-31T09:10:00.000Z" }).expect(409);
      const otherAssignment = await assignment("completed", { originalWorkDate: "2026-08-31" });
      const other = await seed();
      await database.update(schema.smQuestionnaireSubmissions).set({ assignmentId: otherAssignment.id,
        visitStartedAt: new Date("2026-08-31T10:20:00.000Z"), visitCompletedAt: new Date("2026-08-31T10:40:00.000Z") }).where(eq(schema.smQuestionnaireSubmissions.id, other.submission.id));
      await database.insert(schema.smAssignmentTimeSubmissions).values({ assignmentId: otherAssignment.id, revisionNumber: 1, actualMinutes: 20, submittedByUserId: employee });
      const currentPayload = { ...payload, expectedStartedAt: payload.visitStartedAt, expectedCompletedAt: payload.visitCompletedAt,
        visitCompletedAt: "2026-08-31T10:30:00.000Z" };
      const overlap = await request(app).patch(endpoint).set("x-local-role", "sm_admin").send(currentPayload).expect(409);
      assert.equal(overlap.body.code, "sm_visit_time_overlap");
      const [pending] = await database.insert(schema.smAssignmentTimeChangeRequests).values({ assignmentId: scheduled.id, smUserId: employee,
        sourceTimeSubmissionId: versions[1]!.id, requestKind: "time_change", originalMinutes: 70, requestedMinutes: 75,
        originalStartedAt: new Date(payload.visitStartedAt), originalCompletedAt: new Date(payload.visitCompletedAt),
        requestedStartedAt: new Date(payload.visitStartedAt), requestedCompletedAt: new Date("2026-08-31T10:20:00.000Z"),
        requestReason: "Offene SM Anfrage", clientRequestToken: randomUUID() }).returning();
      const blocked = await request(app).patch(endpoint).set("x-local-role", "sm_admin").send(currentPayload).expect(409);
      assert.equal(blocked.body.code, "sm_visit_time_correction_pending_request");
      await database.update(schema.smAssignmentTimeChangeRequests).set({ status: "cancelled" }).where(eq(schema.smAssignmentTimeChangeRequests.id, pending!.id));
      assert.equal((await database.select().from(schema.smAssignmentTimeSubmissions).where(eq(schema.smAssignmentTimeSubmissions.assignmentId, scheduled.id))).length, 2);
    });

    await t.test("employee planning hides cancelled only; admin retains it; restored and moved dates are visible", async () => {
      const cancelled = await assignment("cancelled"), visible = await assignment(), missed = await assignment("missed");
      const moved = await assignment("planned", { originalWorkDate: "2026-08-15", replacementWorkDate: "2026-09-15" });
      const other = await assignment("planned", { originalSmUserId: admin });
      const range = "?from=2026-09-01&to=2026-09-30";
      const response = await employeeGet(`/employee-planning/assignments${range}`).expect(200);
      assert.deepEqual(new Set(response.body.assignments.map((row: { id: string }) => row.id)), new Set([visible.id, missed.id, moved.id]));
      const adminResult = await request(app).get(`/admin-planning/assignments${range}`).set("x-local-role", "sm_admin").expect(200);
      assert.ok(adminResult.body.assignments.some((row: { id: string }) => row.id === cancelled.id));
      await employeeGet(`/visits/${cancelled.id}`).expect(409);
      await request(app).post(`/visits/${cancelled.id}/start`).set("x-local-role", "sm").set("x-local-actor", employee).send({ mode: "timer", clientSubmissionToken: randomUUID() }).expect(409);
      await employeeGet(`/visits/${other.id}`).expect(403);
      await database.update(schema.smAssignments).set({ status: "planned", cancelledAt: null, cancelledByUserId: null, cancellationReason: null, statusBeforeCancellation: null }).where(eq(schema.smAssignments.id, cancelled.id));
      const restored = await employeeGet(`/employee-planning/assignments${range}`).expect(200);
      assert.ok(restored.body.assignments.some((row: { id: string }) => row.id === cancelled.id));
      assert.equal((await employeeGet(`/visits/${cancelled.id}`).expect(200)).body.submission, null);
    });

    await t.test("employee activity and visit detail read corrected answers and keep historical travel/time", async () => {
      const fixture = await seed(), id = fixture.submission.id, planned = await assignment("completed");
      await database.update(schema.smQuestionnaireSubmissions).set({ assignmentId: planned.id }).where(eq(schema.smQuestionnaireSubmissions.id, id));
      await apply(id, await input(id, [{ questionId: fixture.questions[0]!.id, answer: { kind: "choice", optionCode: "no", comment: "Admin korrigiert" } }]));
      await pg.query("update users set sm_travel_time_enabled=false where id=$1", [employee]);
      const detail = await employeeGet(`/visits/${planned.id}`);
      assert.equal(detail.status, 200, JSON.stringify(detail.body));
      assert.equal(detail.body.answers[fixture.questions[0]!.id].optionCode, "no"); assert.equal(detail.body.profile.travelTimeEnabled, false);
      assert.equal(detail.body.submission.travelMinutes, 25); assert.equal(detail.body.submission.visitStartedAt, "2026-08-31T09:00:00.000Z");
      const archive = await employeeGet("/activity/completed").expect(200);
      assert.ok(archive.body.visits.some((row: { submissionId: string }) => row.submissionId === id));
      const draftAssignment = await assignment("in_progress"), draft = await seed();
      await database.update(schema.smQuestionnaireSubmissions).set({ assignmentId: draftAssignment.id, status: "draft", submittedAt: null, reportingAvailableAt: null, visitCompletedAt: null }).where(eq(schema.smQuestionnaireSubmissions.id, draft.submission.id));
      const denied = await request(app).patch(`/visits/${draftAssignment.id}/timing`).set("x-local-role", "sm").set("x-local-actor", employee).send({ travelMinutes: 99 }).expect(403);
      assert.equal(denied.body.code, "sm_visit_travel_time_disabled");
      assert.equal((await database.select().from(schema.smQuestionnaireSubmissions).where(eq(schema.smQuestionnaireSubmissions.id, draft.submission.id)))[0]!.travelMinutes, 25);
      await pg.query("update users set sm_travel_time_enabled=true where id=$1", [employee]);
    });

    await t.test("every non-photo answer family uses the same SM normalizer and stores typed values", async () => {
      const cases: Array<{ type: typeof schema.smQuestionTypeEnum.enumValues[number]; config: Record<string, unknown>; answer: unknown }> = [
        { type: "single", config: {}, answer: { kind: "choice", optionCode: "yes" } },
        { type: "likert", config: {}, answer: { kind: "choice", optionCode: "no" } },
        { type: "multiple", config: {}, answer: { kind: "multi", optionCodes: ["no", "yes", "no"] } },
        { type: "yesnomulti", config: { branches: [{ answer: "Ja", options: ["A", "B"] }] }, answer: { kind: "yesnomulti", optionCode: "yes", subOptions: ["B", "A"] } },
        { type: "text", config: {}, answer: { kind: "text", value: "Freitext" } },
        { type: "numeric", config: { min: "0", max: "10", decimals: true }, answer: { kind: "number", value: 2.5 } },
        { type: "slider", config: { min: "0", max: "10", step: "2" }, answer: { kind: "number", value: 4 } },
        { type: "matrix", config: { rows: ["A", "", "B"], columns: ["Ja", "Nein"] }, answer: { kind: "matrix", cells: [{ rowCode: "row_1", columnCode: "column_2", selected: true }, { rowCode: "row_3", columnCode: "column_1", selected: true }] } },
      ];
      for (const item of cases) {
        const fixture = await seed(), id = fixture.submission.id, question = fixture.questions[1]!;
        await database.update(schema.smQuestionnaireSubmissionQuestions).set({ questionTypeSnapshot: item.type, configSnapshot: item.config,
          requiredSnapshot: true, answerOptionsSnapshot: [{ code: "yes", label: "Ja" }, { code: "no", label: "Nein" }] }).where(eq(schema.smQuestionnaireSubmissionQuestions.id, question.id));
        await apply(id, await input(id, [{ questionId: question.id, answer: item.answer }]));
        const after = await state(id), stored = after.answers.find(answer => answer.submissionQuestionId === question.id)!;
        const normalized = visitShared.normalizeSmVisitAnswer(management.smManagementQuestionSnapshot(after.questions.find(q => q.id === question.id)!), item.answer);
        assert.deepEqual(stored.valueJson, normalized, item.type);
        if (item.type === "matrix") assert.equal((await database.select().from(schema.smQuestionAnswerMatrixCells).where(eq(schema.smQuestionAnswerMatrixCells.answerId, stored.id))).length, 2);
      }
      const fixture = await seed(), question = fixture.questions[1]!;
      await database.update(schema.smQuestionnaireSubmissionQuestions).set({ questionTypeSnapshot: "single", answerOptionsSnapshot: [{ code: "na", label: "Nicht zutreffend", marksNotApplicable: true, earnedPoints: "3", possiblePoints: "5" }] }).where(eq(schema.smQuestionnaireSubmissionQuestions.id, question.id));
      await apply(fixture.submission.id, await input(fixture.submission.id, [{ questionId: question.id, answer: { kind: "choice", optionCode: "na" } }]));
      const answer = (await state(fixture.submission.id)).answers.find(row => row.submissionQuestionId === question.id)!;
      assert.equal(answer.answerState, "not_applicable"); assert.equal(Number(answer.possiblePoints), 0);
    });

    await t.test("concurrent HTTP saves serialize: one current version; identical retries commit once", async () => {
      // PGlite serializes transactions on one connection. This tests the application race, not a multi-server lock benchmark.
      const fixture = await seed(), id = fixture.submission.id, questionId = fixture.questions[1]!.id;
      const payload = await input(id, [{ questionId, answer: { kind: "text", value: "Erster Stand" } }]);
      const results = await Promise.all([http("post", `/${id}/corrections`).send(payload), http("post", `/${id}/corrections`).send({ ...payload, clientMutationToken: randomUUID(), reason: "Zweiter Stand" })]);
      assert.deepEqual(results.map(result => result.status).sort(), [200, 409]);
      const next = await input(id, [{ questionId, answer: { kind: "text", value: "Replay" } }]);
      const retries = await Promise.all([http("post", `/${id}/corrections`).send(next), http("post", `/${id}/corrections`).send(next)]);
      assert.ok(retries.every(result => result.status === 200)); assert.equal(retries.filter(result => result.body.replayed).length, 1);
      assert.equal((await state(id)).answers.filter(answer => answer.submissionQuestionId === questionId).length, 1);
    });

    await t.test("existing employee approval cannot overwrite a direct correction; deletion cannot be revived", async () => {
      const fixture = await seed(), id = fixture.submission.id, questionId = fixture.questions[0]!.id;
      const created = await employeePost(`/submissions/${id}/questions/${questionId}/change-requests`).send({ answer: { kind: "choice", optionCode: "no", comment: "Vom SM angefragt" }, reason: "Bitte prüfen", clientRequestToken: randomUUID() }).expect(201);
      await apply(id, await input(id, [{ questionId, answer: { kind: "choice", optionCode: "no", comment: "Admin hat geprüft" } }]));
      await adminPost(`/answer-change-requests/${created.body.request.id}/approve`).send({}).expect(409);
      const [stillPending] = await database.select().from(schema.smAnswerChangeRequests).where(eq(schema.smAnswerChangeRequests.id, created.body.request.id));
      assert.equal(stillPending!.status, "pending");
      const second = await seed(), secondId = second.submission.id;
      const oldDraft = await input(secondId, [{ questionId: second.questions[1]!.id, answer: { kind: "text", value: "Veralteter Adminentwurf" } }]);
      const employeeChange = await employeePost(`/submissions/${secondId}/questions/${second.questions[1]!.id}/change-requests`).send({ answer: { kind: "text", value: "Mitarbeiterkorrektur" }, reason: "Bitte ergänzen", clientRequestToken: randomUUID() }).expect(201);
      await adminPost(`/answer-change-requests/${employeeChange.body.request.id}/approve`).send({}).expect(200);
      await http("post", `/${secondId}/corrections`).send(oldDraft).expect(409);
      const beforeDelete = await input(secondId, [{ questionId: second.questions[1]!.id, answer: { kind: "text", value: "Nicht wiederbeleben" } }]);
      const deletion = await employeePost(`/submissions/${secondId}/delete-requests`).send({ reason: "Isolierter Löschtest", clientRequestToken: randomUUID() }).expect(201);
      await adminPost(`/submission-delete-requests/${deletion.body.request.id}/approve`).send({}).expect(200);
      await http("post", `/${secondId}/corrections`).send(beforeDelete).expect(409);
      const [deleted] = await database.select().from(schema.smQuestionnaireSubmissions).where(eq(schema.smQuestionnaireSubmissions.id, secondId));
      assert.equal(deleted!.isDeleted, true); assert.equal(deleted!.visitStartedAt!.getTime(), second.submission.visitStartedAt!.getTime());
    });

    await t.test("actual HTTP routes enforce role declarations, filters, pagination and historical inactive SM visibility", async () => {
      const range = "?from=2026-08-01&to=2026-08-31&limit=3";
      await request(app).get(`/completed${range}`).expect(401);
      for (const role of ["gm", "sm"]) await request(app).get(`/completed${range}`).set("x-local-role", role).expect(403);
      await request(app).get(`/completed${range}`).set("x-local-role", "admin").expect(200);
      await http("get", "?from=2026-01-01&to=2026-12-31").expect(400);
      await http("get", "?from=2026-08-31&to=2026-08-01").expect(400);
      await http("get", "?from=2026-08-01&to=2026-08-31&cursorDate=2026-08-31").expect(400);
      await pg.query("update users set is_active=false where id=$1", [employee]);
      const first = await http("get", range).expect(200);
      assert.equal(first.headers["cache-control"], "private, no-store");
      assert.equal(first.body.visits.length, 3); assert.ok(first.body.nextCursor);
      assert.ok(first.body.facets.some((facet: { smUserId: string }) => facet.smUserId === employee));
      const cursor = first.body.nextCursor;
      const next = await http("get", `${range}&cursorDate=${cursor.date}&cursorId=${cursor.id}`).expect(200);
      const seen = new Set(first.body.visits.map((visit: { id: string }) => visit.id));
      assert.ok(next.body.visits.every((visit: { id: string }) => !seen.has(visit.id)));
      assert.equal((await http("get", `${range}&search=not-a-real-market`).expect(200)).body.visits.length, 0);
      assert.equal((await http("get", `${range}&smUserId=${randomUUID()}`).expect(200)).body.visits.length, 0);
    });

    await t.test("real correction/detail/history endpoints preserve versions and reject stale or wrong visits", async () => {
      const fixture = await seed(), id = fixture.submission.id, questionId = fixture.questions[1]!.id;
      const before = await http("get", `/${id}`).expect(200);
      const payload = await input(id, [{ questionId, answer: { kind: "text", value: "Per HTTP korrigiert" } }]);
      await http("post", `/${id}/corrections`).send(payload).expect(200);
      assert.equal((await http("post", `/${id}/corrections`).send(payload).expect(200)).body.replayed, true);
      const after = await http("get", `/${id}`).expect(200);
      assert.notEqual(after.body.version, before.body.version);
      assert.equal(after.body.visit.startedAt, before.body.visit.startedAt);
      assert.equal(after.body.sections[0].questions[1].answer.value, "Per HTTP korrigiert");
      const history = await http("get", `/${id}/history?questionId=${questionId}`).expect(200);
      assert.equal(history.body.entries[0].reason, payload.reason); assert.equal(history.body.entries[0].actor, "Local Admin");
      await http("get", `/${id}/history?questionId=${randomUUID()}`).expect(404);
      await http("post", `/${id}/corrections`).send({ ...payload, clientMutationToken: randomUUID() }).expect(409);
      await database.update(schema.smQuestionnaireSubmissions).set({ status: "draft" }).where(eq(schema.smQuestionnaireSubmissions.id, id));
      await http("get", `/${id}`).expect(409);
    });

    await t.test("actual signed-upload flow binds actor, question and bytes; retry preserves one correction", async () => {
      const fixture = await seed(), id = fixture.submission.id, questionId = fixture.questions[2]!.id;
      const upload = await http("post", `/${id}/photos/upload-url`).send({ questionId, originalFileName: "Foto.png", mimeType: "image/png", byteSize: 42 }).expect(200);
      const receipt = upload.body.receipt;
      const payload = { ...await input(id, [{ questionId, answer: { kind: "photo", fileIds: [receipt.id] } }]), uploads: [receipt] };
      await http("post", `/${id}/corrections`).send(payload).expect(409); // object is not uploaded yet
      storedObjects.set(receipt.storagePath, { size: 42, contentType: "image/png" });
      await http("post", `/${id}/corrections`).set("x-local-actor", employee).send(payload).expect(400);
      await http("post", `/${id}/corrections`).send({ ...payload, uploads: [{ ...receipt, byteSize: 43 }] }).expect(400);
      await http("post", `/${id}/corrections`).send(payload).expect(200);
      storedObjects.clear(); // replay must not need storage verification again
      assert.equal((await http("post", `/${id}/corrections`).send(payload).expect(200)).body.replayed, true);
      const detail = (await http("get", `/${id}`).expect(200)).body;
      const photo = detail.sections[0].questions[2].photos[0];
      assert.ok(photo.signedUrl); assert.equal(photo.storagePath, undefined); assert.equal(photo.storageBucket, undefined);
      assert.deepEqual(detail.sections[0].questions[2].answer.fileIds, [photo.id]);
    });

    await t.test("OOS reads the real corrected SQL result without moving the visit to another month", async () => {
      const fixture = await seed(), id = fixture.submission.id;
      const dashboard = await isolatedModule<typeof import("./routes/sm-dashboard.js")>(new URL("./routes/sm-dashboard.ts", import.meta.url), {
        "../lib/db.js": { db: database }, "../middleware/auth.js": authMock,
        "../sm-dashboard.shared.js": dashboardShared, "../sm-planning.shared.js": planning,
      });
      // PGlite execute returns { rows }; production postgres-js returns the row array.
      const executor = { execute: async (query: Parameters<typeof database.execute>[0]) => (await database.execute(query)).rows };
      const aggregate = async (from = "2026-08-01", to = "2026-08-31") => {
        const result = await dashboard.loadSmDashboardRows({ from, to, marketId: market }, executor as never);
        return dashboardShared.aggregateSmDashboard(result.visits.filter(visit => visit.submissionId === id), result.oosRows.filter(row => row.submissionId === id)).summary;
      };
      assert.equal((await aggregate()).foundCases, 1);
      await apply(id, await input(id, [{ questionId: fixture.questions[0]!.id, answer: { kind: "choice", optionCode: "no", comment: "Kein OOS" } }]));
      assert.equal((await aggregate()).foundCases, 0); assert.equal((await aggregate()).completedVisits, 1);
      const [detection] = await database.select().from(schema.smQuestionVersions).where(eq(schema.smQuestionVersions.id, fixture.questions[0]!.questionVersionId));
      await database.update(schema.smQuestionnaireSubmissionQuestions).set({ questionTypeSnapshot: "yesno", metricRoleSnapshot: "oos_remediation", oosCategorySnapshot: "water_near_water",
        metricConfigSnapshot: { detectionQuestionId: detection!.questionId }, answerOptionsSnapshot: [{ code: "fixed", label: "Ja", metricOutcomeCode: "resolved" }, { code: "open", label: "Nein", metricOutcomeCode: "not_resolved" }],
      }).where(eq(schema.smQuestionnaireSubmissionQuestions.id, fixture.questions[1]!.id));
      await apply(id, await input(id, [{ questionId: fixture.questions[0]!.id, answer: { kind: "choice", optionCode: "yes" } }, { questionId: fixture.questions[1]!.id, answer: { kind: "choice", optionCode: "fixed" } }]));
      const fixed = await aggregate(); assert.equal(fixed.foundCases, 1); assert.equal(fixed.fixedCases, 1); assert.equal(fixed.fixedRate, 100); assert.equal(fixed.affectedMarketRate, 100);
      assert.equal((await aggregate("2026-09-01", "2026-09-30")).completedVisits, 0);
    });

    await t.test("history keeps the original correction reason after a later rule invalidates that answer", async () => {
      const fixture = await seed(), id = fixture.submission.id, parent = fixture.questions[0]!, child = fixture.questions[1]!;
      await database.update(schema.smQuestionnaireSubmissionQuestions).set({ logicRulesSnapshot: [{ triggerQuestionId: parent.questionCodeSnapshot, operator: "equals", triggerValue: "Ja", action: "show", targetQuestionIds: [child.questionCodeSnapshot] }] }).where(eq(schema.smQuestionnaireSubmissionQuestions.id, parent.id));
      const first = await input(id, [{ questionId: child.id, answer: { kind: "text", value: "Ursprüngliche Ergänzung" } }]);
      first.reason = "Grund der ursprünglichen Ergänzung";
      await apply(id, first);
      const second = await input(id, [{ questionId: parent.id, answer: { kind: "choice", optionCode: "no", comment: "Nicht vorhanden" } }]);
      second.reason = "Anderer Grund der Ausblendung";
      await apply(id, second);
      const history = await http("get", `/${id}/history?questionId=${child.id}`).expect(200);
      assert.equal(history.body.entries[0].current, false); assert.equal(history.body.entries[0].state, "invalidated");
      assert.equal(history.body.entries[0].reason, first.reason);
    });

    await t.test("no GM business tables exist in this fixture", async () => {
      const tables = await pg.query<{ tablename: string }>("select tablename from pg_tables where schemaname='public'");
      assert.ok(tables.rows.every(row => row.tablename === "users" || row.tablename.startsWith("sm_")));
    });
    if (process.env.SM_MANAGEMENT_BROWSER_FIXTURE === "1") {
      app.put("/fixture-upload", express.raw({ type: ["image/png", "image/jpeg", "image/webp"], limit: "20mb" }), (req, res) => {
        const path = String(req.query.path);
        if (!issuedUploadPaths.has(path) || storedObjects.has(path) || !Buffer.isBuffer(req.body)) { res.sendStatus(400); return; }
        storedObjects.set(path, { size: req.body.length, contentType: req.header("content-type")!, bytes: req.body }); res.sendStatus(200);
      });
      app.get("/fixture-photo", (req, res) => {
        const photo = storedObjects.get(String(req.query.path));
        if (!photo?.bytes) { res.sendStatus(404); return; }
        res.type(photo.contentType).send(photo.bytes);
      });
      const fixture = await seed(), id = fixture.submission.id;
      await database.update(schema.smQuestionnaireSubmissions).set({ marketNameSnapshot: "Lokaler Testmarkt", smNameSnapshot: "Test SM",
        visitStartedAt: new Date("2026-09-15T09:00:00Z"), visitCompletedAt: new Date("2026-09-15T10:00:00Z"),
        submittedAt: new Date("2026-09-15T10:00:00Z"), reportingAvailableAt: new Date("2026-09-15T10:00:00Z"),
      }).where(eq(schema.smQuestionnaireSubmissions.id, id));
      const dashboard = await isolatedModule<typeof import("./routes/sm-dashboard.js")>(new URL("./routes/sm-dashboard.ts", import.meta.url), {
        "../lib/db.js": { db: database }, "../middleware/auth.js": authMock,
        "../sm-dashboard.shared.js": dashboardShared, "../sm-planning.shared.js": planning,
      });
      app.get("/fixture-state", async (_req, res) => {
        const executor = { execute: async (query: Parameters<typeof database.execute>[0]) => (await database.execute(query)).rows };
        const result = await dashboard.loadSmDashboardRows({ from: "2026-09-01", to: "2026-09-30" }, executor as never);
        const detail = await txRun(tx => smManagementDetail(tx, id));
        res.json({ visit: detail.visit, answers: detail.sections.flatMap(section => section.questions.map(question => ({ text: question.text, answer: question.answer }))),
          oos: dashboardShared.aggregateSmDashboard(result.visits.filter(visit => visit.submissionId === id), result.oosRows.filter(row => row.submissionId === id)).summary });
      });
      await new Promise<void>(resolve => {
        const server = app.listen(4017, "127.0.0.1", () => console.log("ISOLATED_SM_BROWSER_FIXTURE_READY http://127.0.0.1:4017"));
        const stop = () => server.close(() => resolve());
        process.once("SIGINT", stop); process.once("SIGTERM", stop);
      });
    }
  } finally { await pg.close(); }
});
