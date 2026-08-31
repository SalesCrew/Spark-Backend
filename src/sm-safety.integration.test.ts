// Opt-in live SM database verification. Standard cases roll back; the separately
// gated concurrent case soft-deletes its exact newly created fixture IDs.
// No GM tables are imported or modified. No authentication accounts are created/changed.
import assert from "node:assert/strict";
import test, { after } from "node:test";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { and, eq, sql as query } from "drizzle-orm";

const enabled = process.env.SM_SAFETY_DATABASE_TESTS === "1";
Object.assign(process.env, { NODE_ENV: "test", BYPASS_AUTH_FOR_TESTS: "1", BYPASS_AUTH_ROLE: "sm", BYPASS_AUTH_USER_ID: "eff25681-6964-4593-b3e5-5778f6e8eebe" });
if (!enabled) Object.assign(process.env, { DATABASE_URL: "postgres://test:test@127.0.0.1:1/test", SUPABASE_URL: "http://127.0.0.1:1", SUPABASE_ANON_KEY: "test", SUPABASE_SERVICE_ROLE_KEY: "test", JWT_SECRET: "sm-safety-test-secret-not-for-production" });
const { db, sql: client } = await import("./lib/db.js");
const { smMarkets, smAssignments, smAssignmentSeries, smAssignmentSeriesVersions, smAssignmentEvents, smAssignmentTimeSubmissions, smAssignmentTimeChangeRequests, smQuestionnaireSubmissions, smQuestionnaireVersions, users } = await import("./lib/schema.js");
const { assertSmVisitTimeAvailable, SmTimeOverlapError } = await import("./sm-time-overlap.js");
const { deactivateSmMarket, loadSmMarketDeactivationPreview, SmMarketDeactivationError, smDeactivationToday } = await import("./sm-market-deactivation.js");
const { smVisitsRouter } = await import("./routes/sm-visits.js");
const { adminSmMarketsRouter } = await import("./routes/sm-markets.js");
const { adminSmPlanningRouter, smPlanningRouter } = await import("./routes/sm-planning.js");
const { adjustSmHolidayAssignments, loadSmHolidayStates } = await import("./sm-holiday-planning.js");
after(async () => { await client.end(); });
type Tx = Parameters<Parameters<typeof db.transaction>[0]>[0];
const owner = "eff25681-6964-4593-b3e5-5778f6e8eebe";
const admin = "6b97fce7-3372-47eb-9b28-d8df44fa475e";
const app = express(); app.use(express.json()); app.use("/sm/visits", smVisitsRouter); app.use("/admin/sm-markets", adminSmMarketsRouter); app.use("/admin/sm-planning", adminSmPlanningRouter); app.use("/sm/planning", smPlanningRouter);
app.use((error: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => { res.status(500).json({ error: error.message }); });
function asSm() { process.env.BYPASS_AUTH_ROLE = "sm"; process.env.BYPASS_AUTH_USER_ID = owner; }
function asAdmin() { process.env.BYPASS_AUTH_ROLE = "sm_admin"; process.env.BYPASS_AUTH_USER_ID = admin; }

async function fixture(tx: Tx) {
  const accounts = await tx.select({ id: users.id, role: users.role }).from(users).where(query`${users.id} in (${owner}::uuid, ${admin}::uuid)`);
  assert.equal(accounts.find((row) => row.id === owner)?.role, "sm");
  assert.equal(accounts.find((row) => row.id === admin)?.role, "sm_admin");
  const [version] = await tx.select().from(smQuestionnaireVersions).where(and(eq(smQuestionnaireVersions.status, "published"), eq(smQuestionnaireVersions.isDeleted, false))).limit(1);
  assert.ok(version);
  const market = async (active = true) => (await tx.insert(smMarkets).values({ internalMarketId: `SM-SAFETY-${randomUUID()}`, name: "SM SAFETY ROLLBACK TEST", dbName: "SM SAFETY ROLLBACK TEST", chain: "TEST", address: "Teststraße 1", postalCode: "1100", city: "Wien", region: "Wien", isActive: active }).returning())[0]!;
  const source = await market(); const replacement = await market(); const other = await market();
  const assignment = async (marketId = source.id, extra: Partial<typeof smAssignments.$inferInsert> = {}) => (await tx.insert(smAssignments).values({ idempotencyKey: `sm-safety:${randomUUID()}`, sourceType: "single", originalWorkDate: "2034-06-01", originalSmUserId: owner, originalSmMarketId: marketId, originalMarketInternalId: source.internalMarketId!, originalPlannedMinutes: 30, createdByUserId: admin, updatedByUserId: admin, ...extra }).returning())[0]!;
  const visit = async (start: string | null, end: string | null, mode: "timer" | "manual" = "timer") => {
    const a = await assignment(source.id, { status: end ? "completed" : "in_progress" });
    const [submission] = await tx.insert(smQuestionnaireSubmissions).values({ assignmentId: a.id, questionnaireTemplateId: version.questionnaireTemplateId, questionnaireVersionId: version.id, smUserId: owner, smMarketId: source.id, clientSubmissionToken: `sm-safety:${randomUUID()}`, questionnaireNameSnapshot: "SM SAFETY TEST", questionnaireVersionSnapshot: 1, smNameSnapshot: "SM Test", marketNameSnapshot: "Testmarkt Billa", marketAddressSnapshot: "Teststraße 1", marketCitySnapshot: "Wien", visitTimeMode: mode, visitStartedAt: start ? new Date(start) : null, visitCompletedAt: end ? new Date(end) : null, status: end ? "submitted" : "draft", submittedAt: end ? new Date(end) : null, reportingAvailableAt: end ? new Date(end) : null }).returning();
    if (end) await tx.insert(smAssignmentTimeSubmissions).values({ assignmentId: a.id, revisionNumber: 1, actualMinutes: 20, submittedByUserId: owner });
    return { assignment: a, submission: submission! };
  };
  return { source, replacement, other, market, assignment, visit };
}

test("SM live safety: overlap, rejected submit/retry, manual timestamps, deactivation and history (rolled back)", { skip: !enabled }, async (t) => {
  const rollback = new Error("SM_SAFETY_EXPECTED_ROLLBACK");
  await assert.rejects(db.transaction(async (tx) => {
    await tx.execute(query`set local statement_timeout = '15s'`);
    const f = await fixture(tx);
    // Keep route transactions as actual PostgreSQL savepoints under this rollback-only outer transaction.
    t.mock.method(db, "transaction", (callback: (nested: Tx) => Promise<unknown>) => tx.transaction(callback));
    t.mock.method(db, "select", tx.select.bind(tx));
    const existing = await f.visit("2034-06-01T13:10:00Z", "2034-06-01T13:30:00Z");
    const draft = await f.visit(null, null, "manual");
    const interval = (start: string, end: string) => ({ smUserId: owner, assignmentId: draft.assignment.id, startedAt: new Date(start), completedAt: new Date(end) });
    for (const [start, end] of [["13:10", "13:50"], ["13:00", "13:20"], ["13:12", "13:18"], ["13:00", "14:00"], ["13:10", "13:30"]]) {
      await assert.rejects(assertSmVisitTimeAvailable(tx, interval(`2034-06-01T${start}:00Z`, `2034-06-01T${end}:00Z`)), SmTimeOverlapError);
    }
    await assertSmVisitTimeAvailable(tx, interval("2034-06-01T13:30:00Z", "2034-06-01T13:50:00Z"));
    await assertSmVisitTimeAvailable(tx, interval("2034-06-01T12:50:00Z", "2034-06-01T13:10:00Z"));
    asSm();
    const missing = await request(app).post(`/sm/visits/${draft.assignment.id}/submit`).send({ clientMutationToken: randomUUID(), actualMinutes: 40 });
    assert.equal(missing.status, 409, JSON.stringify(missing.body)); assert.equal(missing.body.code, "sm_visit_timestamps_required");
    const conflict = await request(app).post(`/sm/visits/${draft.assignment.id}/submit`).send({ clientMutationToken: randomUUID(), visitStartedAt: "2034-06-01T13:10:00Z", visitCompletedAt: "2034-06-01T13:50:00Z" });
    assert.equal(conflict.status, 409, JSON.stringify(conflict.body)); assert.equal(conflict.body.details.conflicts[0].submissionId, existing.submission.id);
    const [stillDraft] = await tx.select().from(smQuestionnaireSubmissions).where(eq(smQuestionnaireSubmissions.id, draft.submission.id)); assert.equal(stillDraft!.status, "draft");
    assert.equal((await tx.select().from(smAssignmentTimeSubmissions).where(eq(smAssignmentTimeSubmissions.assignmentId, draft.assignment.id))).length, 0);
    const retry = await request(app).post(`/sm/visits/${draft.assignment.id}/submit`).send({ clientMutationToken: randomUUID(), visitStartedAt: "2034-06-01T13:30:00Z", visitCompletedAt: "2034-06-01T13:50:00Z" });
    assert.equal(retry.status, 200, JSON.stringify(retry.body)); assert.equal(retry.body.receipt.actualMinutes, 20);
    const replay = await request(app).post(`/sm/visits/${draft.assignment.id}/submit`).send({ clientMutationToken: randomUUID() }); assert.equal(replay.status, 200); assert.equal(replay.body.receipt.submissionId, retry.body.receipt.submissionId);
    const single = await f.assignment();
    const past = await f.assignment(f.source.id, { originalWorkDate: "2020-01-01" });
    const movedAway = await f.assignment(f.source.id, { replacementSmMarketId: f.other.id, replacementMarketInternalId: f.other.internalMarketId });
    const [series] = await tx.insert(smAssignmentSeries).values({ idempotencyKey: `sm-safety:${randomUUID()}`, createdByUserId: admin }).returning();
    const [seriesVersion] = await tx.insert(smAssignmentSeriesVersions).values({ seriesId: series!.id, versionNumber: 1, effectiveFromDate: "2034-06-01", smMarketId: f.source.id, marketInternalIdSnapshot: f.source.internalMarketId!, defaultSmUserId: owner, plannedMinutes: 30, frequency: "weekly", weekdays: [4], validFrom: "2034-06-01", validTo: "2034-06-30", createdByUserId: admin }).returning();
    const occurrence = await f.assignment(f.source.id, { sourceType: "series", seriesId: series!.id, seriesVersionId: seriesVersion!.id, seriesOccurrenceKey: "2034-06-08", originalWorkDate: "2034-06-08" });
    const occurrence2 = await f.assignment(f.source.id, { sourceType: "series", seriesId: series!.id, seriesVersionId: seriesVersion!.id, seriesOccurrenceKey: "2034-06-15", originalWorkDate: "2034-06-15" });
    asAdmin();
    const previewResponse = await request(app).get(`/admin/sm-markets/${f.source.id}/deactivation-preview`);
    assert.equal(previewResponse.status, 200, JSON.stringify(previewResponse.body));
    const preview = previewResponse.body;
    assert.equal(preview.affectedCount, 3); assert.equal(preview.groups.length, 2); assert.equal(preview.protectedAssignments.length, 2);
    const direct = await request(app).patch(`/admin/sm-markets/${f.source.id}`).send({ isActive: false }); assert.equal(direct.status, 409); assert.equal(direct.body.code, "sm_market_deactivation_required");
    const incomplete = await request(app).post(`/admin/sm-markets/${f.source.id}/deactivate`).send({ previewToken: preview.previewToken, resolutions: [] }); assert.equal(incomplete.status, 400);
    const resolutions = [{ assignmentId: single.id, action: "cancel" }, ...[occurrence, occurrence2].map((row) => ({ assignmentId: row.id, action: "replace", replacementMarketId: f.replacement.id }))];
    await tx.update(smAssignments).set({ replacementPlannedMinutes: 31, updatedAt: new Date(Date.now() + 1000) }).where(eq(smAssignments.id, single.id));
    const stale = await request(app).post(`/admin/sm-markets/${f.source.id}/deactivate`).send({ previewToken: preview.previewToken, resolutions }); assert.equal(stale.status, 409); assert.equal(stale.body.code, "sm_market_deactivation_stale");
    const fresh = await loadSmMarketDeactivationPreview(tx, f.source.id);
    const applied = await request(app).post(`/admin/sm-markets/${f.source.id}/deactivate`).send({ previewToken: fresh.previewToken, resolutions }); assert.equal(applied.status, 200, JSON.stringify(applied.body)); assert.equal(applied.body.cancelled, 1); assert.equal(applied.body.replaced, 2);
    const [cancelled] = await tx.select().from(smAssignments).where(eq(smAssignments.id, single.id)); assert.equal(cancelled!.status, "cancelled"); assert.equal(cancelled!.isDeleted, false);
    const [replaced] = await tx.select().from(smAssignments).where(eq(smAssignments.id, occurrence.id)); assert.equal(replaced!.replacementSmMarketId, f.replacement.id); assert.equal(replaced!.originalSmMarketId, f.source.id); assert.equal(replaced!.seriesVersionId, seriesVersion!.id);
    assert.equal((await tx.select().from(smAssignmentSeriesVersions).where(eq(smAssignmentSeriesVersions.seriesId, series!.id))).length, 2);
    for (const original of [past, movedAway]) { const [row] = await tx.select().from(smAssignments).where(eq(smAssignments.id, original.id)); assert.deepEqual(row, original); }
    assert.equal((await tx.select().from(smAssignmentEvents).where(eq(smAssignmentEvents.assignmentId, single.id))).length, 1);
    const restore = await request(app).post(`/admin/sm-planning/assignments/${single.id}/restore`).send({ expectedUpdatedAt: cancelled!.updatedAt.toISOString(), reason: "SM safety test" }); assert.equal(restore.status, 400); assert.equal(restore.body.code, "sm_market_invalid");
    const create = await request(app).post("/admin/sm-planning/assignments").send({ smMarketId: f.source.id, smUserId: owner, workDate: smDeactivationToday(), plannedMinutes: 30, idempotencyKey: randomUUID() }); assert.equal(create.status, 400); assert.equal(create.body.code, "sm_market_invalid");
    asSm();
    const planning = await request(app).get("/sm/planning/assignments?from=2034-06-01&to=2034-06-30"); assert.equal(planning.status, 200, JSON.stringify(planning.body)); assert.equal(planning.body.assignments.find((row: { id: string }) => row.id === single.id).status, "cancelled");
    const startInactive = await request(app).post(`/sm/visits/${past.id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }); assert.equal(startInactive.status, 409); assert.equal(startInactive.body.code, "sm_visit_market_inactive");
    process.env.BYPASS_AUTH_FOR_TESTS = "0";
    const forbidden = await request(app).get(`/admin/sm-markets/${f.source.id}/deactivation-preview`); assert.equal(forbidden.status, 401);
    process.env.BYPASS_AUTH_FOR_TESTS = "1";
    throw rollback;
  }), (error) => { if (error !== rollback) throw error; return true; });
  t.mock.restoreAll();
  const leftovers = await db.select({ id: smMarkets.id }).from(smMarkets).where(and(eq(smMarkets.name, "SM SAFETY ROLLBACK TEST"), eq(smMarkets.isDeleted, false)));
  assert.equal(leftovers.length, 0, "rollback must leave no fixture markets");
});

test("SM live safety: corrections, released times, inactive replacements, new preview rows and all-cancel series (rolled back)", { skip: !enabled }, async (t) => {
  const rollback = new Error("SM_SAFETY_EXPECTED_ROLLBACK");
  await assert.rejects(db.transaction(async (tx) => {
    const f = await fixture(tx);
    t.mock.method(db, "transaction", (callback: (nested: Tx) => Promise<unknown>) => tx.transaction(callback));
    t.mock.method(db, "select", tx.select.bind(tx));
    const first = await f.visit("2034-06-02T13:10:00Z", "2034-06-02T13:30:00Z");
    const second = await f.visit("2034-06-02T13:30:00Z", "2034-06-02T13:50:00Z");
    const [time] = await tx.select().from(smAssignmentTimeSubmissions).where(eq(smAssignmentTimeSubmissions.assignmentId, second.assignment.id));
    const [correction] = await tx.insert(smAssignmentTimeChangeRequests).values({ assignmentId: second.assignment.id, smUserId: owner, sourceTimeSubmissionId: time!.id, requestKind: "time_change", originalMinutes: 20, requestedMinutes: 40, originalStartedAt: second.submission.visitStartedAt, originalCompletedAt: second.submission.visitCompletedAt, requestedStartedAt: new Date("2034-06-02T13:10:00Z"), requestedCompletedAt: new Date("2034-06-02T13:50:00Z"), requestReason: "SM safety overlap test", clientRequestToken: randomUUID() }).returning();
    asAdmin();
    const rejected = await request(app).post(`/admin/sm-planning/time-change-requests/${correction!.id}/approve`).send({});
    assert.equal(rejected.status, 409, JSON.stringify(rejected.body)); assert.equal(rejected.body.code, "sm_visit_time_overlap");
    const [pending] = await tx.select().from(smAssignmentTimeChangeRequests).where(eq(smAssignmentTimeChangeRequests.id, correction!.id)); assert.equal(pending!.status, "pending");
    const [unchanged] = await tx.select().from(smQuestionnaireSubmissions).where(eq(smQuestionnaireSubmissions.id, second.submission.id)); assert.equal(unchanged!.visitStartedAt?.toISOString(), "2034-06-02T13:30:00.000Z");
    await tx.update(smAssignmentTimeSubmissions).set({ isCurrent: false, isDeleted: true, deletedAt: new Date() }).where(eq(smAssignmentTimeSubmissions.assignmentId, first.assignment.id));
    const approved = await request(app).post(`/admin/sm-planning/time-change-requests/${correction!.id}/approve`).send({}); assert.equal(approved.status, 200, JSON.stringify(approved.body));
    const restoreTime = await request(app).post(`/admin/sm-planning/assignments/${first.assignment.id}/time`).send({ actualMinutes: 20 }); assert.equal(restoreTime.status, 409); assert.equal(restoreTime.body.code, "sm_visit_time_overlap");
    const a = await f.assignment();
    const preview = await loadSmMarketDeactivationPreview(tx, f.source.id);
    const added = await f.assignment();
    await assert.rejects(deactivateSmMarket(tx, f.source.id, admin, { previewToken: preview.previewToken, resolutions: [{ assignmentId: a.id, action: "cancel" }] }), (e) => e instanceof SmMarketDeactivationError && e.code === "sm_market_deactivation_stale");
    const fresh = await loadSmMarketDeactivationPreview(tx, f.source.id);
    await tx.update(smMarkets).set({ isActive: false }).where(eq(smMarkets.id, f.replacement.id));
    await assert.rejects(deactivateSmMarket(tx, f.source.id, admin, { previewToken: fresh.previewToken, resolutions: [a, added].map((row) => ({ assignmentId: row.id, action: "replace", replacementMarketId: f.replacement.id })) }), (e) => e instanceof SmMarketDeactivationError && e.code === "sm_market_replacement_unavailable");
    const [stillActive] = await tx.select().from(smMarkets).where(eq(smMarkets.id, f.source.id)); assert.equal(stillActive!.isActive, true);
    const [series] = await tx.insert(smAssignmentSeries).values({ idempotencyKey: randomUUID(), createdByUserId: admin }).returning();
    const [version] = await tx.insert(smAssignmentSeriesVersions).values({ seriesId: series!.id, versionNumber: 1, effectiveFromDate: "2034-06-01", smMarketId: f.source.id, marketInternalIdSnapshot: f.source.internalMarketId!, defaultSmUserId: owner, plannedMinutes: 30, frequency: "weekly", weekdays: [4], validFrom: "2034-06-01", validTo: "2034-06-30", createdByUserId: admin }).returning();
    await f.assignment(f.source.id, { sourceType: "series", seriesId: series!.id, seriesVersionId: version!.id, seriesOccurrenceKey: "2034-06-08", originalWorkDate: "2034-06-08" });
    const ongoing = await f.visit(null, null, "manual");
    const cancellationPreview = await loadSmMarketDeactivationPreview(tx, f.source.id);
    const result = await deactivateSmMarket(tx, f.source.id, admin, { previewToken: cancellationPreview.previewToken, resolutions: cancellationPreview.groups.flatMap((group) => group.occurrences.map((row) => ({ assignmentId: row.id, action: "cancel" as const }))) });
    assert.equal(result.cancelled, 3); assert.equal(result.replaced, 0);
    const [ended] = await tx.select().from(smAssignmentSeries).where(eq(smAssignmentSeries.id, series!.id)); assert.equal(ended!.status, "ended");
    asSm();
    const discard = await request(app).delete(`/sm/visits/${ongoing.assignment.id}`).send({ confirmation: "SOFT_DELETE_SM_VISIT" });
    assert.equal(discard.status, 200, JSON.stringify(discard.body));
    const [discarded] = await tx.select().from(smAssignments).where(eq(smAssignments.id, ongoing.assignment.id));
    assert.equal(discarded!.status, "cancelled", "discard must not restore a startable visit on an inactive market");
    assert.equal(discarded!.isDeleted, false);
    const emptyPreview = await loadSmMarketDeactivationPreview(tx, f.other.id); assert.equal(emptyPreview.affectedCount, 0);
    const emptyResult = await deactivateSmMarket(tx, f.other.id, admin, { previewToken: emptyPreview.previewToken, resolutions: [] }); assert.equal(emptyResult.market.isActive, false);
    throw rollback;
  }), (error) => { if (error !== rollback) throw error; return true; });
  t.mock.restoreAll();
});

test("SM live concurrency: two independent connections cannot complete overlapping visits; corrected retry succeeds", { skip: !enabled || process.env.SM_SAFETY_CONCURRENCY_TESTS !== "1" }, async (t) => {
  // Only this opt-in case commits NEW isolated SM fixtures, to make them visible to both connections.
  // Finally soft-deletes only the exact fixture IDs; append-only audit events remain as test history.
  const data = await db.transaction(async (tx) => {
    const f = await fixture(tx);
    const a = await f.visit(null, null, "manual"); const b = await f.visit(null, null, "manual");
    return { marketIds: [f.source.id, f.replacement.id, f.other.id], a, b };
  });
  const assignmentIds = [data.a.assignment.id, data.b.assignment.id];
  const submissionIds = [data.a.submission.id, data.b.submission.id];
  try {
    asSm();
    const realTransaction = db.transaction.bind(db);
    const pids = new Set<number>(); let arrivals = 0; let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    const timeout = setTimeout(release, 10_000);
    t.mock.method(db, "transaction", (callback: (tx: Tx) => Promise<unknown>) => realTransaction(async (tx) => {
      const rows = await tx.execute<{ pid: number }>(query`select pg_backend_pid() as pid`); pids.add(rows[0]!.pid);
      arrivals++; if (arrivals === 2) release();
      await gate;
      return callback(tx);
    }));
    const body = { clientMutationToken: randomUUID(), visitStartedAt: "2034-06-03T13:10:00Z", visitCompletedAt: "2034-06-03T13:30:00Z" };
    const responses = await Promise.all(assignmentIds.map((id) => request(app).post(`/sm/visits/${id}/submit`).send(body)));
    clearTimeout(timeout); t.mock.restoreAll();
    assert.equal(pids.size, 2, "must use two independent PostgreSQL connections");
    assert.deepEqual(responses.map((response) => response.status).sort(), [200, 409], JSON.stringify(responses.map((r) => r.body)));
    const loserIndex = responses.findIndex((response) => response.status === 409);
    assert.equal(responses[loserIndex]!.body.code, "sm_visit_time_overlap");
    const retry = await request(app).post(`/sm/visits/${assignmentIds[loserIndex]}/submit`).send({ ...body, visitStartedAt: "2034-06-03T13:30:00Z", visitCompletedAt: "2034-06-03T13:50:00Z" });
    assert.equal(retry.status, 200, JSON.stringify(retry.body));
  } finally {
    t.mock.restoreAll();
    await db.transaction(async (tx) => {
      const now = new Date();
      await tx.update(smAssignmentTimeSubmissions).set({ isCurrent: false, isDeleted: true, deletedAt: now }).where(query`${smAssignmentTimeSubmissions.assignmentId} in (${assignmentIds[0]}::uuid, ${assignmentIds[1]}::uuid)`);
      await tx.update(smQuestionnaireSubmissions).set({ isCurrent: false, isDeleted: true, deletedAt: now }).where(query`${smQuestionnaireSubmissions.id} in (${submissionIds[0]}::uuid, ${submissionIds[1]}::uuid)`);
      await tx.update(smAssignments).set({ isDeleted: true, deletedAt: now }).where(query`${smAssignments.id} in (${assignmentIds[0]}::uuid, ${assignmentIds[1]}::uuid)`);
      for (const id of data.marketIds) await tx.update(smMarkets).set({ isActive: false, isDeleted: true, deletedAt: now }).where(eq(smMarkets.id, id));
    });
    const visible = await db.select({ id: smAssignments.id }).from(smAssignments).where(and(query`${smAssignments.id} in (${assignmentIds[0]}::uuid, ${assignmentIds[1]}::uuid)`, eq(smAssignments.isDeleted, false)));
    assert.equal(visible.length, 0);
    console.log("SM concurrency fixtures soft-deleted by exact IDs; audit history retained:", JSON.stringify({ assignmentIds, submissionIds, marketIds: data.marketIds }));
  }
});

test("SM holidays: creation, single series exception, load balancing, manual override, restore and API parity (rolled back)", { skip: !enabled }, async (t) => {
  const rollback = new Error("SM_HOLIDAYS_EXPECTED_ROLLBACK");
  await assert.rejects(db.transaction(async (tx) => {
    const f = await fixture(tx);
    t.mock.method(db, "transaction", (callback: (nested: Tx) => Promise<unknown>) => tx.transaction(callback));
    t.mock.method(db, "select", tx.select.bind(tx));
    await f.assignment(f.source.id, { originalWorkDate: "2027-10-25", originalPlannedMinutes: 120 });
    await f.assignment(f.source.id, { originalWorkDate: "2027-10-27", originalPlannedMinutes: 360 });
    const idempotencyKey = randomUUID();
    asAdmin();
    const create = await request(app).post("/admin/sm-planning/assignments").send({ smMarketId: f.source.id, smUserId: owner, workDate: "2027-10-26", plannedMinutes: 90, idempotencyKey });
    assert.equal(create.status, 201, JSON.stringify(create.body));
    const singleId = create.body.assignmentId;
    const [single] = await tx.select().from(smAssignments).where(eq(smAssignments.id, singleId));
    assert.equal(single!.originalWorkDate, "2027-10-26"); assert.equal(single!.replacementWorkDate, "2027-10-25");
    const metadata = (await loadSmHolidayStates(tx, [singleId])).get(singleId)!;
    assert.equal(metadata.adjustment!.holidayName, "Nationalfeiertag"); assert.equal(metadata.adjustment!.previousMinutes, 120); assert.equal(metadata.adjustment!.nextMinutes, 360);
    const replay = await request(app).post("/admin/sm-planning/assignments").send({ smMarketId: f.source.id, smUserId: owner, workDate: "2027-10-26", plannedMinutes: 90, idempotencyKey });
    assert.equal(replay.status, 200); assert.equal(replay.body.assignmentId, singleId);
    const series = await request(app).post("/admin/sm-planning/series").send({ smMarketId: f.source.id, smUserId: owner, plannedMinutes: 90, frequency: "weekly", weekdays: [2], validFrom: "2027-10-19", validTo: "2027-11-02", idempotencyKey: randomUUID() });
    assert.equal(series.status, 201, JSON.stringify(series.body));
    const seriesRows = await tx.select().from(smAssignments).where(eq(smAssignments.seriesId, series.body.seriesId));
    assert.equal(seriesRows.length, 3); assert.equal(seriesRows.filter((row) => row.replacementWorkDate).length, 1);
    const holiday = seriesRows.find((row) => row.originalWorkDate === "2027-10-26")!;
    assert.equal(holiday.replacementWorkDate, "2027-10-25"); assert.equal(holiday.seriesOccurrenceKey, "2027-10-26");
    assert.equal((await tx.select().from(smAssignmentSeriesVersions).where(eq(smAssignmentSeriesVersions.seriesId, series.body.seriesId))).length, 1);
    const beforeCount = (await tx.select().from(smAssignmentEvents).where(eq(smAssignmentEvents.assignmentId, singleId))).length;
    assert.deepEqual(await adjustSmHolidayAssignments(tx, { actorUserId: admin, assignmentIds: [singleId, holiday.id] }), []);
    assert.equal((await tx.select().from(smAssignmentEvents).where(eq(smAssignmentEvents.assignmentId, singleId))).length, beforeCount);
    const manual = await request(app).post(`/admin/sm-planning/assignments/${singleId}/reschedule`).send({ workDate: "2027-10-26", expectedUpdatedAt: single!.updatedAt.toISOString(), reason: "Bewusst manuell für diesen Termin" });
    assert.equal(manual.status, 200, JSON.stringify(manual.body));
    assert.deepEqual(await adjustSmHolidayAssignments(tx, { actorUserId: admin, assignmentIds: [singleId] }), []);
    const adminList = await request(app).get("/admin/sm-planning/assignments?from=2027-10-18&to=2027-11-03"); assert.equal(adminList.status, 200);
    const adminSingle = adminList.body.assignments.find((row: { id: string }) => row.id === singleId);
    assert.equal(adminSingle.effective.workDate, "2027-10-26"); assert.equal(adminSingle.holidayAdjustment.manualOverride, true);
    asSm();
    const phoneList = await request(app).get("/sm/planning/assignments?from=2027-10-18&to=2027-11-03"); assert.equal(phoneList.status, 200);
    assert.deepEqual(phoneList.body.assignments.find((row: { id: string }) => row.id === singleId), adminSingle);
    const another = await f.assignment(f.source.id, { originalWorkDate: "2027-10-26", originalPlannedMinutes: 150 });
    const another2 = await f.assignment(f.source.id, { originalWorkDate: "2027-10-26", originalPlannedMinutes: 150 });
    const decisions = await adjustSmHolidayAssignments(tx, { actorUserId: admin, assignmentIds: [another.id, another2.id], dryRun: true });
    assert.deepEqual(decisions.map((d) => d.adjustment.adjustedDate).sort(), ["2027-10-25", "2027-10-27"]);
    const [unchanged] = await tx.select().from(smAssignments).where(eq(smAssignments.id, another.id)); assert.equal(unchanged!.replacementWorkDate, null);
    asAdmin();
    const cancel = await request(app).post(`/admin/sm-planning/assignments/${another.id}/cancel`).send({ expectedUpdatedAt: another.updatedAt.toISOString(), reason: "Feiertags-Testabsage" }); assert.equal(cancel.status, 200);
    const restore = await request(app).post(`/admin/sm-planning/assignments/${another.id}/restore`).send({ expectedUpdatedAt: cancel.body.updatedAt, reason: "Feiertags-Testwiederherstellung" }); assert.equal(restore.status, 200, JSON.stringify(restore.body));
    const [restored] = await tx.select().from(smAssignments).where(eq(smAssignments.id, another.id)); assert.equal(restored!.replacementWorkDate, "2027-10-25");
    const completed = await f.assignment(f.source.id, { originalWorkDate: "2027-10-26", status: "completed" });
    const historical = await f.assignment(f.source.id, { originalWorkDate: "2025-12-08" });
    assert.deepEqual(await adjustSmHolidayAssignments(tx, { actorUserId: admin, assignmentIds: [completed.id, historical.id] }), []);
    throw rollback;
  }), (error) => { if (error !== rollback) throw error; return true; });
  t.mock.restoreAll();
});
