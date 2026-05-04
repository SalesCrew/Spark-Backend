import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test, { after } from "node:test";
import request from "supertest";
import { createApp } from "./app.js";
import { and, eq, inArray, isNull } from "drizzle-orm";
import { db, sql } from "./lib/db.js";
import { finalizeBonusForSubmittedVisitSession, readGmActiveBonusSummary } from "./lib/bonus-finalizer.js";
import { enqueueIppRecalcForQuestionScoringChanges, runIppFinalizerOnce } from "./lib/ipp-finalizer.js";
import { computeMarketIppForPeriod } from "./lib/ipp.js";
import { addDays, getCurrentRedPeriod, getRedPeriodForDate, startOfDay } from "./lib/red-monat.js";
import {
  campaignMarketAssignmentHistory,
  campaignFragebogenHistory,
  campaignMarketAssignments,
  campaigns,
  fragebogenMain,
  fragebogenMainModule,
  markets,
  moduleMain,
  fragebogenMainSpezialItems,
  ippMarketRedmonthResults,
  ippRecalcQueue,
  redMonthCalendarConfig,
  moduleMainQuestion,
  moduleQuestionChains,
  questionAttachments,
  questionPhotoTags,
  questionBankShared,
  photoTags,
  questionRules,
  questionRuleTargets,
  questionScoring,
  praemienWaves,
  praemienWaveThresholds,
  praemienWaveSources,
  praemienWavePillars,
  praemienGmWaveContributions,
  praemienGmWaveTotals,
  gmDaySessionPauses,
  gmDaySessions,
  timeTrackingEntries,
  users,
  visitSessionQuestions,
  visitSessionSections,
  visitSessions,
  visitQuestionComments,
  visitAnswers,
  visitAnswerOptions,
  visitAnswerPhotos,
  visitAnswerPhotoTags,
} from "./lib/schema.js";

function enableTestAuthBypass() {
  process.env.NODE_ENV = "test";
  process.env.BYPASS_AUTH_FOR_TESTS = "1";
  delete process.env.BYPASS_AUTH_ROLE;
  delete process.env.BYPASS_AUTH_USER_ID;
}

function setTestBypassRole(role: "admin" | "gm" | "sm") {
  process.env.BYPASS_AUTH_ROLE = role;
}

function setTestBypassUserId(userId: string) {
  process.env.BYPASS_AUTH_USER_ID = userId;
}

let checkedDbReadiness = false;
let dbHasFragebogenTables = false;
let checkedCampaignReadiness = false;
let dbHasCampaignTables = false;
let checkedVisitSessionReadiness = false;
let dbHasVisitSessionTables = false;
let checkedModuleQuestionChainsReadiness = false;
let dbHasModuleQuestionChainsTable = false;
let checkedIppReadiness = false;
let dbHasIppTables = false;
let checkedRedMonthCalendarReadiness = false;
let dbHasRedMonthCalendarTable = false;
let checkedTimeTrackingReadiness = false;
let dbHasTimeTrackingTables = false;
let checkedDaySessionReadiness = false;
let dbHasDaySessionTable = false;
let checkedAdminZeiterfassungReadiness = false;
let dbHasAdminZeiterfassungTables = false;
let checkedPraemienReadiness = false;
let dbHasPraemienTables = false;

async function ensureDbReadyForFragebogenTests(): Promise<boolean> {
  if (checkedDbReadiness) return dbHasFragebogenTables;
  const result = await sql<{ module_main: string | null }[]>`select to_regclass('public.module_main') as module_main`;
  const row = result[0];
  dbHasFragebogenTables = Boolean(row?.module_main);
  checkedDbReadiness = true;
  return dbHasFragebogenTables;
}

async function ensureDbReadyForCampaignTests(): Promise<boolean> {
  if (checkedCampaignReadiness) return dbHasCampaignTables;
  const result = await sql<{ campaigns: string | null }[]>`select to_regclass('public.campaigns') as campaigns`;
  const row = result[0];
  dbHasCampaignTables = Boolean(row?.campaigns);
  checkedCampaignReadiness = true;
  return dbHasCampaignTables;
}

async function ensureDbReadyForVisitSessionTests(): Promise<boolean> {
  if (checkedVisitSessionReadiness) return dbHasVisitSessionTables;
  const tableResult = await sql<{ visit_sessions: string | null }[]>`
    select to_regclass('public.visit_sessions') as visit_sessions
  `;
  const columnsResult = await sql<{ column_name: string }[]>`
    select column_name
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'visit_session_questions'
      and column_name in ('question_chains_snapshot', 'applies_to_market_chain_snapshot')
  `;
  const row = tableResult[0];
  const columnNames = new Set(columnsResult.map((entry) => entry.column_name));
  dbHasVisitSessionTables = Boolean(
    row?.visit_sessions &&
    columnNames.has("question_chains_snapshot") &&
    columnNames.has("applies_to_market_chain_snapshot"),
  );
  checkedVisitSessionReadiness = true;
  return dbHasVisitSessionTables;
}

async function ensureDbReadyForModuleQuestionChainsTests(): Promise<boolean> {
  if (checkedModuleQuestionChainsReadiness) return dbHasModuleQuestionChainsTable;
  const result = await sql<{ module_question_chains: string | null }[]>`
    select to_regclass('public.module_question_chains') as module_question_chains
  `;
  dbHasModuleQuestionChainsTable = Boolean(result[0]?.module_question_chains);
  checkedModuleQuestionChainsReadiness = true;
  return dbHasModuleQuestionChainsTable;
}

async function ensureDbReadyForIppTests(): Promise<boolean> {
  if (checkedIppReadiness) return dbHasIppTables;
  const result = await sql<{ ipp_market_redmonth_results: string | null; ipp_recalc_queue: string | null; visit_sessions: string | null }[]>`
    select
      to_regclass('public.ipp_market_redmonth_results') as ipp_market_redmonth_results,
      to_regclass('public.ipp_recalc_queue') as ipp_recalc_queue,
      to_regclass('public.visit_sessions') as visit_sessions
  `;
  dbHasIppTables = Boolean(result[0]?.ipp_market_redmonth_results && result[0]?.ipp_recalc_queue && result[0]?.visit_sessions);
  checkedIppReadiness = true;
  return dbHasIppTables;
}

async function ensureDbReadyForRedMonthCalendarTests(): Promise<boolean> {
  if (checkedRedMonthCalendarReadiness) return dbHasRedMonthCalendarTable;
  const result = await sql<{ red_month_calendar_config: string | null }[]>`
    select to_regclass('public.red_month_calendar_config') as red_month_calendar_config
  `;
  dbHasRedMonthCalendarTable = Boolean(result[0]?.red_month_calendar_config);
  checkedRedMonthCalendarReadiness = true;
  return dbHasRedMonthCalendarTable;
}

async function ensureDbReadyForTimeTrackingTests(): Promise<boolean> {
  if (checkedTimeTrackingReadiness) return dbHasTimeTrackingTables;
  const result = await sql<{ time_tracking_entries: string | null; time_tracking_entry_events: string | null }[]>`
    select
      to_regclass('public.time_tracking_entries') as time_tracking_entries,
      to_regclass('public.time_tracking_entry_events') as time_tracking_entry_events
  `;
  dbHasTimeTrackingTables = Boolean(result[0]?.time_tracking_entries && result[0]?.time_tracking_entry_events);
  checkedTimeTrackingReadiness = true;
  return dbHasTimeTrackingTables;
}

async function ensureDbReadyForDaySessionTests(): Promise<boolean> {
  if (checkedDaySessionReadiness) return dbHasDaySessionTable;
  const result = await sql<{ gm_day_sessions: string | null }[]>`
    select to_regclass('public.gm_day_sessions') as gm_day_sessions
  `;
  dbHasDaySessionTable = Boolean(result[0]?.gm_day_sessions);
  checkedDaySessionReadiness = true;
  return dbHasDaySessionTable;
}

async function ensureDbReadyForAdminZeiterfassungTests(): Promise<boolean> {
  if (checkedAdminZeiterfassungReadiness) return dbHasAdminZeiterfassungTables;
  const result = await sql<{
    gm_day_sessions: string | null;
    gm_day_session_pauses: string | null;
    visit_sessions: string | null;
    time_tracking_entries: string | null;
  }[]>`
    select
      to_regclass('public.gm_day_sessions') as gm_day_sessions,
      to_regclass('public.gm_day_session_pauses') as gm_day_session_pauses,
      to_regclass('public.visit_sessions') as visit_sessions,
      to_regclass('public.time_tracking_entries') as time_tracking_entries
  `;
  dbHasAdminZeiterfassungTables = Boolean(
    result[0]?.gm_day_sessions &&
    result[0]?.gm_day_session_pauses &&
    result[0]?.visit_sessions &&
    result[0]?.time_tracking_entries,
  );
  checkedAdminZeiterfassungReadiness = true;
  return dbHasAdminZeiterfassungTables;
}

async function ensureDbReadyForPraemienTests(): Promise<boolean> {
  if (checkedPraemienReadiness) return dbHasPraemienTables;
  const result = await sql<{
    praemien_waves: string | null;
    praemien_gm_wave_totals: string | null;
    praemien_gm_wave_contributions: string | null;
  }[]>`
    select
      to_regclass('public.praemien_waves') as praemien_waves,
      to_regclass('public.praemien_gm_wave_totals') as praemien_gm_wave_totals,
      to_regclass('public.praemien_gm_wave_contributions') as praemien_gm_wave_contributions
  `;
  dbHasPraemienTables = Boolean(
    result[0]?.praemien_waves &&
    result[0]?.praemien_gm_wave_totals &&
    result[0]?.praemien_gm_wave_contributions,
  );
  checkedPraemienReadiness = true;
  return dbHasPraemienTables;
}

async function ensureStartedDayForGm(gmUserId: string): Promise<void> {
  if (!(await ensureDbReadyForDaySessionTests())) return;
  const now = new Date();
  const workDate = toYmd(now);
  const [existing] = await db
    .select()
    .from(gmDaySessions)
    .where(and(eq(gmDaySessions.gmUserId, gmUserId), eq(gmDaySessions.workDate, workDate), eq(gmDaySessions.isDeleted, false)))
    .limit(1);
  if (existing) {
    await db
      .update(gmDaySessions)
      .set({
        status: "started",
        dayStartedAt: existing.dayStartedAt ?? now,
        dayEndedAt: null,
        submittedAt: null,
        cancelledAt: null,
        isDeleted: false,
        deletedAt: null,
        updatedAt: now,
      })
      .where(eq(gmDaySessions.id, existing.id));
    return;
  }
  await db.insert(gmDaySessions).values({
    gmUserId,
    workDate,
    status: "started",
    dayStartedAt: now,
    isStartKmCompleted: true,
    startKm: 1000,
    startKmDeferred: false,
  });
}

function toYmd(date: Date): string {
  return date.toISOString().slice(0, 10);
}

after(async () => {
  await sql.end({ timeout: 1 });
});

test("GET /health returns healthy payload", async () => {
  const app = createApp();
  const response = await request(app).get("/health");
  assert.equal(response.status, 200);
  assert.equal(response.body.status, "ok");
  assert.equal(response.body.service, "coke-spark-backend");
});

test("protected admin routes require bearer token", async () => {
  process.env.NODE_ENV = "test";
  delete process.env.BYPASS_AUTH_FOR_TESTS;
  const app = createApp();
  const response = await request(app).get("/admin/questions");
  assert.equal(response.status, 401);
  assert.equal(response.body.error, "Missing access token.");
});

test("invalid scope is mapped to 400 (not 500)", async () => {
  enableTestAuthBypass();
  const app = createApp();
  const response = await request(app).get("/admin/modules/not-a-valid-scope");
  assert.equal(response.status, 400);
});

test("red-month current endpoint returns period payload", async () => {
  enableTestAuthBypass();
  setTestBypassRole("gm");
  const app = createApp();
  const response = await request(app).get("/red-month/current");
  assert.equal(response.status, 200);
  assert.equal(typeof response.body?.current?.label, "string");
  assert.equal(typeof response.body?.current?.start, "string");
  assert.equal(typeof response.body?.config?.anchorStart, "string");
  assert.equal(Array.isArray(response.body?.config?.cycleWeeks), true);
});

test("red-month config patch is admin-only", async (t) => {
  if (!(await ensureDbReadyForRedMonthCalendarTests())) {
    t.skip("red_month_calendar_config table missing");
    return;
  }
  enableTestAuthBypass();
  setTestBypassRole("gm");
  const app = createApp();
  const response = await request(app).patch("/admin/red-month/config").send({
    anchorStart: "2026-01-27",
    cycleWeeks: [4, 4, 5],
  });
  assert.equal(response.status, 403);
});

test("admin red-month config patch updates active config", async (t) => {
  if (!(await ensureDbReadyForRedMonthCalendarTests())) {
    t.skip("red_month_calendar_config table missing");
    return;
  }
  enableTestAuthBypass();
  setTestBypassRole("admin");
  const app = createApp();
  const cycleTag = Math.floor(Date.now() % 7) + 3;
  const payload = {
    anchorStart: "2026-01-27",
    cycleWeeks: [4, 4, cycleTag],
  };
  const response = await request(app).patch("/admin/red-month/config").send(payload);
  assert.equal(response.status, 200);
  assert.deepEqual(response.body?.config?.cycleWeeks, payload.cycleWeeks);

  const [activeConfig] = await db
    .select()
    .from(redMonthCalendarConfig)
    .where(and(eq(redMonthCalendarConfig.isActive, true), eq(redMonthCalendarConfig.isDeleted, false)))
    .limit(1);
  assert.ok(activeConfig);
  assert.deepEqual(activeConfig.cycleWeeks, payload.cycleWeeks);
});

test("day session lifecycle supports start, km defer/fill, end and submit", async (t) => {
  if (!(await ensureDbReadyForDaySessionTests())) {
    t.skip("gm_day_sessions table missing");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for day session tests.");
    return;
  }

  enableTestAuthBypass();
  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  await ensureStartedDayForGm(gmUser.id);
  const app = createApp();
  const touchedIds: string[] = [];
  try {
    const startRes = await request(app).post("/day-session/start").send({});
    assert.equal(startRes.status, 200);
    const sessionId = startRes.body?.session?.id as string;
    touchedIds.push(sessionId);
    assert.equal(startRes.body?.session?.status, "started");

    const deferStartKmRes = await request(app).patch("/day-session/start-km/defer").send({});
    assert.equal(deferStartKmRes.status, 200);
    assert.equal(deferStartKmRes.body?.session?.isStartKmCompleted, false);

    const setStartKmRes = await request(app).patch("/day-session/start-km").send({ km: 12345 });
    assert.equal(setStartKmRes.status, 200);
    assert.equal(setStartKmRes.body?.session?.isStartKmCompleted, true);

    const endRes = await request(app).patch("/day-session/end").send({});
    assert.equal(endRes.status, 200);
    assert.equal(endRes.body?.session?.status, "ended");

    const deferEndKmRes = await request(app).patch("/day-session/end-km/defer").send({});
    assert.equal(deferEndKmRes.status, 200);
    assert.equal(deferEndKmRes.body?.session?.isEndKmCompleted, false);

    const setEndKmRes = await request(app).patch("/day-session/end-km").send({ km: 12390 });
    assert.equal(setEndKmRes.status, 200);
    assert.equal(setEndKmRes.body?.session?.isEndKmCompleted, true);

    const submitRes = await request(app).post("/day-session/submit").send({ comment: "done" });
    assert.equal(submitRes.status, 200);
    assert.equal(submitRes.body?.session?.status, "submitted");
  } finally {
    if (touchedIds.length > 0) {
      await db
        .update(gmDaySessions)
        .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
        .where(inArray(gmDaySessions.id, touchedIds));
    }
  }
});

test("time tracking start is blocked when day is not started", async (t) => {
  if (!(await ensureDbReadyForTimeTrackingTests()) || !(await ensureDbReadyForDaySessionTests())) {
    t.skip("time_tracking or gm_day_sessions tables missing");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for day gating tests.");
    return;
  }

  const today = toYmd(new Date());
  await db
    .update(gmDaySessions)
    .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
    .where(and(eq(gmDaySessions.gmUserId, gmUser.id), eq(gmDaySessions.workDate, today), eq(gmDaySessions.isDeleted, false)));

  enableTestAuthBypass();
  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  await ensureStartedDayForGm(gmUser.id);
  const app = createApp();
  const blocked = await request(app).post("/time-tracking/entries/draft/start").send({
    activityType: "sonderaufgabe",
    startAt: new Date().toISOString(),
    clientEntryToken: `tt-block-${Date.now()}`,
  });
  assert.equal(blocked.status, 409);
  assert.equal(blocked.body?.code, "day_not_started");
});

test("visit session creation is blocked when day is not started", async (t) => {
  if (!(await ensureDbReadyForDaySessionTests())) {
    t.skip("gm_day_sessions table missing");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for day gating tests.");
    return;
  }

  const today = toYmd(new Date());
  await db
    .update(gmDaySessions)
    .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
    .where(and(eq(gmDaySessions.gmUserId, gmUser.id), eq(gmDaySessions.workDate, today), eq(gmDaySessions.isDeleted, false)));

  enableTestAuthBypass();
  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  await ensureStartedDayForGm(gmUser.id);
  const app = createApp();
  const blocked = await request(app).post("/markets/gm/visit-sessions").send({
    marketId: randomUUID(),
    campaignIds: [randomUUID()],
  });
  assert.equal(blocked.status, 409);
  assert.equal(blocked.body?.code, "day_not_started");
});

test("admin zeiterfassung days returns canonical timeline with fahrtzeit and questionnaire type", async (t) => {
  if (!(await ensureDbReadyForAdminZeiterfassungTests()) || !(await ensureDbReadyForCampaignTests())) {
    t.skip("admin zeiterfassung prerequisite tables missing");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id, firstName: users.firstName, lastName: users.lastName, region: users.region })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  const [market] = await db
    .select({ id: markets.id, name: markets.name, address: markets.address })
    .from(markets)
    .where(eq(markets.isDeleted, false))
    .limit(1);
  const [campaign] = await db
    .select({ id: campaigns.id })
    .from(campaigns)
    .where(eq(campaigns.isDeleted, false))
    .limit(1);
  if (!gmUser?.id || !market?.id || !campaign?.id) {
    t.skip("Missing gm/market/campaign seed data for admin zeiterfassung test.");
    return;
  }

  const workDate = "2099-01-15";
  const dayStart = new Date("2099-01-15T08:00:00.000Z");
  const dayEnd = new Date("2099-01-15T12:00:00.000Z");
  const pauseStart = new Date("2099-01-15T09:00:00.000Z");
  const pauseEnd = new Date("2099-01-15T09:15:00.000Z");
  const visitStart = new Date("2099-01-15T09:30:00.000Z");
  const visitEnd = new Date("2099-01-15T10:00:00.000Z");
  const extraStart = new Date("2099-01-15T10:20:00.000Z");
  const extraEnd = new Date("2099-01-15T11:00:00.000Z");

  const insertedDayId = randomUUID();
  const insertedPauseId = randomUUID();
  const insertedVisitId = randomUUID();
  const insertedExtraId = randomUUID();

  await db.insert(gmDaySessions).values({
    id: insertedDayId,
    gmUserId: gmUser.id,
    workDate,
    timezone: "UTC",
    status: "submitted",
    dayStartedAt: dayStart,
    dayEndedAt: dayEnd,
    startKm: 1200,
    endKm: 1260,
    isStartKmCompleted: true,
    isEndKmCompleted: true,
    submittedAt: dayEnd,
  });
  await db.insert(gmDaySessionPauses).values({
    id: insertedPauseId,
    daySessionId: insertedDayId,
    gmUserId: gmUser.id,
    pauseStartedAt: pauseStart,
    pauseEndedAt: pauseEnd,
  });
  await db.insert(visitSessions).values({
    id: insertedVisitId,
    gmUserId: gmUser.id,
    marketId: market.id,
    status: "submitted",
    startedAt: visitStart,
    submittedAt: visitEnd,
    lastSavedAt: visitEnd,
  });
  await db.insert(visitSessionSections).values({
    visitSessionId: insertedVisitId,
    campaignId: campaign.id,
    section: "standard",
    fragebogenNameSnapshot: "Standard",
    status: "submitted",
    orderIndex: 0,
  });
  await db.insert(timeTrackingEntries).values({
    id: insertedExtraId,
    gmUserId: gmUser.id,
    activityType: "schulung",
    startAt: extraStart,
    endAt: extraEnd,
    status: "submitted",
    submittedAt: extraEnd,
  });

  enableTestAuthBypass();
  setTestBypassRole("admin");
  const app = createApp();
  try {
    const res = await request(app).get(
      `/admin/zeiterfassung/days?from=${workDate}&to=${workDate}&timezone=UTC&includeLive=true&gmUserIds=${gmUser.id}`,
    );
    assert.equal(res.status, 200);
    const sessions = Array.isArray(res.body?.sessions) ? res.body.sessions : [];
    const dayPayload = sessions.find((row: { id?: string }) => row.id === insertedDayId);
    assert.ok(dayPayload);
    assert.equal(dayPayload.entries.some((entry: { kind?: string }) => entry.kind === "marktbesuch"), true);
    assert.equal(dayPayload.entries.some((entry: { kind?: string }) => entry.kind === "zusatzzeit"), true);
    assert.equal(dayPayload.entries.some((entry: { kind?: string }) => entry.kind === "pause"), true);
    assert.equal(dayPayload.entries.some((entry: { questionnaireType?: string }) => entry.questionnaireType === "standard"), true);
    assert.equal(dayPayload.timeline.some((segment: { kind?: string; durationMin?: number }) => segment.kind === "fahrtzeit" && segment.durationMin === 15), true);
  } finally {
    await db.update(gmDaySessionPauses).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(gmDaySessionPauses.id, insertedPauseId));
    await db.update(visitSessionSections).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(visitSessionSections.visitSessionId, insertedVisitId));
    await db.update(visitSessions).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(visitSessions.id, insertedVisitId));
    await db.update(timeTrackingEntries).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(timeTrackingEntries.id, insertedExtraId));
    await db.update(gmDaySessions).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(gmDaySessions.id, insertedDayId));
  }
});

test("admin zeiterfassung splits zusatz around pause windows chronologically", async (t) => {
  if (!(await ensureDbReadyForAdminZeiterfassungTests())) {
    t.skip("admin zeiterfassung prerequisite tables missing");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for split timeline test.");
    return;
  }

  const workDate = "2099-01-16";
  const dayStart = new Date("2099-01-16T09:00:00.000Z");
  const dayEnd = new Date("2099-01-16T14:00:00.000Z");
  const zusatzStart = new Date("2099-01-16T10:00:00.000Z");
  const zusatzEnd = new Date("2099-01-16T13:00:00.000Z");
  const pauseStart = new Date("2099-01-16T11:00:00.000Z");
  const pauseEnd = new Date("2099-01-16T12:00:00.000Z");

  const insertedDayId = randomUUID();
  const insertedPauseId = randomUUID();
  const insertedExtraId = randomUUID();

  await db.insert(gmDaySessions).values({
    id: insertedDayId,
    gmUserId: gmUser.id,
    workDate,
    timezone: "UTC",
    status: "submitted",
    dayStartedAt: dayStart,
    dayEndedAt: dayEnd,
    startKm: 3000,
    endKm: 3060,
    isStartKmCompleted: true,
    isEndKmCompleted: true,
    submittedAt: dayEnd,
  });
  await db.insert(gmDaySessionPauses).values({
    id: insertedPauseId,
    daySessionId: insertedDayId,
    gmUserId: gmUser.id,
    pauseStartedAt: pauseStart,
    pauseEndedAt: pauseEnd,
  });
  await db.insert(timeTrackingEntries).values({
    id: insertedExtraId,
    gmUserId: gmUser.id,
    activityType: "arztbesuch",
    startAt: zusatzStart,
    endAt: zusatzEnd,
    status: "submitted",
    submittedAt: zusatzEnd,
  });

  enableTestAuthBypass();
  setTestBypassRole("admin");
  const app = createApp();
  try {
    const res = await request(app).get(
      `/admin/zeiterfassung/days?from=${workDate}&to=${workDate}&timezone=UTC&includeLive=true&gmUserIds=${gmUser.id}`,
    );
    assert.equal(res.status, 200);
    const sessions = Array.isArray(res.body?.sessions) ? res.body.sessions : [];
    const dayPayload = sessions.find((row: { id?: string }) => row.id === insertedDayId);
    assert.ok(dayPayload);
    const middleSegments = (dayPayload.timeline as Array<{ kind: string; start: string; end: string; title?: string }>).filter(
      (segment) => segment.kind === "zusatzzeit" || segment.kind === "pause",
    );
    assert.deepEqual(
      middleSegments.map((segment) => [segment.kind, segment.start, segment.end, segment.title ?? ""]),
      [
        ["zusatzzeit", "10:00", "11:00", "Arztbesuch"],
        ["pause", "11:00", "12:00", "Pause"],
        ["zusatzzeit", "12:00", "13:00", "Arztbesuch"],
      ],
    );
  } finally {
    await db
      .update(gmDaySessionPauses)
      .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
      .where(eq(gmDaySessionPauses.id, insertedPauseId));
    await db
      .update(timeTrackingEntries)
      .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
      .where(eq(timeTrackingEntries.id, insertedExtraId));
    await db
      .update(gmDaySessions)
      .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
      .where(eq(gmDaySessions.id, insertedDayId));
  }
});

test("admin zeiterfassung gm aggregates include live visibility and strict KPI totals", async (t) => {
  if (!(await ensureDbReadyForAdminZeiterfassungTests())) {
    t.skip("admin zeiterfassung prerequisite tables missing");
    return;
  }
  const gmUserId = randomUUID();
  const gmAuthId = randomUUID();
  const gmEmail = `zeiterfassung-gm-${Date.now()}-${Math.floor(Math.random() * 10000)}@example.com`;
  await db.insert(users).values({
    id: gmUserId,
    supabaseAuthId: gmAuthId,
    email: gmEmail,
    role: "gm",
    firstName: "Zeit",
    lastName: "Tester",
    isActive: true,
  });

  const randomOffset = 20 + Math.floor(Math.random() * 200);
  const submittedDate = new Date(Date.UTC(2099, 0, randomOffset));
  const liveDate = new Date(Date.UTC(2099, 0, randomOffset + 1));
  const submittedYmd = submittedDate.toISOString().slice(0, 10);
  const liveYmd = liveDate.toISOString().slice(0, 10);
  const submittedStartIso = `${submittedYmd}T08:00:00.000Z`;
  const submittedEndIso = `${submittedYmd}T12:00:00.000Z`;
  const liveStartIso = `${liveYmd}T09:00:00.000Z`;

  const submittedId = randomUUID();
  const liveId = randomUUID();
  const insertedIds: string[] = [];
  try {
    await db.insert(gmDaySessions).values({
      id: submittedId,
      gmUserId,
      workDate: submittedYmd,
      timezone: "UTC",
      status: "submitted",
      dayStartedAt: new Date(submittedStartIso),
      dayEndedAt: new Date(submittedEndIso),
      startKm: 1000,
      endKm: 1040,
      isStartKmCompleted: true,
      isEndKmCompleted: true,
      submittedAt: new Date(submittedEndIso),
    });
    insertedIds.push(submittedId);
    await db.insert(gmDaySessions).values({
      id: liveId,
      gmUserId,
      workDate: liveYmd,
      timezone: "UTC",
      status: "started",
      dayStartedAt: new Date(liveStartIso),
      startKm: 1050,
      isStartKmCompleted: true,
    });
    insertedIds.push(liveId);

    enableTestAuthBypass();
    setTestBypassRole("admin");
    const app = createApp();
    const res = await request(app).get(
      `/admin/zeiterfassung/gm-aggregates?from=${submittedYmd}&to=${liveYmd}&timezone=UTC&includeLive=true&gmUserIds=${gmUserId}`,
    );
    assert.equal(res.status, 200);
    const rows = Array.isArray(res.body?.rows) ? res.body.rows : [];
    const row = rows.find((entry: { gmId?: string }) => entry.gmId === gmUserId);
    assert.ok(row);
    assert.equal(row.visibleSessionCount, 2);
    assert.equal(row.liveSessionCount, 1);
    assert.equal(row.trackedDays, 1);
    assert.equal(row.totalKmDriven, 40);
  } finally {
    if (insertedIds.length > 0) {
      await db
        .update(gmDaySessions)
        .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
        .where(inArray(gmDaySessions.id, insertedIds));
    }
    await db
      .update(users)
      .set({ isActive: false, deletedAt: new Date(), updatedAt: new Date() })
      .where(eq(users.id, gmUserId));
  }
});

test("time tracking draft lifecycle supports start, end, comment and submit", async (t) => {
  if (!(await ensureDbReadyForTimeTrackingTests())) {
    t.skip("time_tracking tables missing");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for time tracking tests.");
    return;
  }

  enableTestAuthBypass();
  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  await ensureStartedDayForGm(gmUser.id);
  const app = createApp();
  const createdIds: string[] = [];
  const token = `tt-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
  try {
    const startRes = await request(app).post("/time-tracking/entries/draft/start").send({
      activityType: "hotel",
      startAt: "2026-04-10T08:00:00.000Z",
      clientEntryToken: token,
    });
    assert.equal(startRes.status, 200);
    assert.equal(startRes.body?.entry?.status, "draft");
    assert.equal(startRes.body?.entry?.activityType, "hoteluebernachtung");
    const entryId = startRes.body?.entry?.id as string;
    createdIds.push(entryId);

    const endRes = await request(app).patch(`/time-tracking/entries/${entryId}/draft/end`).send({
      endAt: "2026-04-10T10:15:00.000Z",
    });
    assert.equal(endRes.status, 200);
    assert.equal(endRes.body?.entry?.durationMin, 135);

    const commentRes = await request(app).patch(`/time-tracking/entries/${entryId}/draft/comment`).send({
      comment: "optional comment",
    });
    assert.equal(commentRes.status, 200);
    assert.equal(commentRes.body?.entry?.comment, "optional comment");

    const submitRes = await request(app).post(`/time-tracking/entries/${entryId}/submit`).send({});
    assert.equal(submitRes.status, 200);
    assert.equal(submitRes.body?.entry?.status, "submitted");
  } finally {
    if (createdIds.length > 0) {
      await db
        .update(timeTrackingEntries)
        .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
        .where(inArray(timeTrackingEntries.id, createdIds));
    }
  }
});

test("time tracking submit rejects incomplete draft", async (t) => {
  if (!(await ensureDbReadyForTimeTrackingTests())) {
    t.skip("time_tracking tables missing");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for time tracking tests.");
    return;
  }

  enableTestAuthBypass();
  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  await ensureStartedDayForGm(gmUser.id);
  const app = createApp();
  const createdIds: string[] = [];
  const token = `tt-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
  try {
    const startRes = await request(app).post("/time-tracking/entries/draft/start").send({
      activityType: "sonderaufgabe",
      startAt: "2026-04-10T08:00:00.000Z",
      clientEntryToken: token,
    });
    assert.equal(startRes.status, 200);
    const entryId = startRes.body?.entry?.id as string;
    createdIds.push(entryId);

    const submitRes = await request(app).post(`/time-tracking/entries/${entryId}/submit`).send({});
    assert.equal(submitRes.status, 400);
    assert.equal(submitRes.body?.error, "Start und Ende muessen gesetzt sein.");
  } finally {
    if (createdIds.length > 0) {
      await db
        .update(timeTrackingEntries)
        .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
        .where(inArray(timeTrackingEntries.id, createdIds));
    }
  }
});

test("time tracking cancel is idempotent for drafts", async (t) => {
  if (!(await ensureDbReadyForTimeTrackingTests())) {
    t.skip("time_tracking tables missing");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for time tracking tests.");
    return;
  }

  enableTestAuthBypass();
  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  const app = createApp();
  const token = `tt-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
  const startRes = await request(app).post("/time-tracking/entries/draft/start").send({
    activityType: "lager",
    startAt: "2026-04-10T08:00:00.000Z",
    clientEntryToken: token,
  });
  assert.equal(startRes.status, 200);
  const entryId = startRes.body?.entry?.id as string;

  const cancelRes1 = await request(app).patch(`/time-tracking/entries/${entryId}/cancel`).send({});
  assert.equal(cancelRes1.status, 200);
  assert.equal(cancelRes1.body?.entry?.status, "cancelled");
  const cancelRes2 = await request(app).patch(`/time-tracking/entries/${entryId}/cancel`).send({});
  assert.equal(cancelRes2.status, 200);
});

test("time tracking start is idempotent when clientEntryToken is reused", async (t) => {
  if (!(await ensureDbReadyForTimeTrackingTests())) {
    t.skip("time_tracking tables missing");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for time tracking tests.");
    return;
  }

  enableTestAuthBypass();
  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  const app = createApp();
  const token = `tt-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
  const startRes1 = await request(app).post("/time-tracking/entries/draft/start").send({
    activityType: "homeoffice",
    startAt: "2026-04-10T08:00:00.000Z",
    clientEntryToken: token,
  });
  assert.equal(startRes1.status, 200);
  const entryId = startRes1.body?.entry?.id as string;

  const startRes2 = await request(app).post("/time-tracking/entries/draft/start").send({
    activityType: "homeoffice",
    startAt: "2026-04-10T09:00:00.000Z",
    clientEntryToken: token,
  });
  assert.equal(startRes2.status, 200);
  assert.equal(startRes2.body?.entry?.id, entryId);
  assert.equal(startRes2.body?.entry?.startAt, "2026-04-10T09:00:00.000Z");

  await db
    .update(timeTrackingEntries)
    .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
    .where(eq(timeTrackingEntries.id, entryId));
});

test("time tracking active drafts endpoint exposes offline contract", async (t) => {
  if (!(await ensureDbReadyForTimeTrackingTests())) {
    t.skip("time_tracking tables missing");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for time tracking tests.");
    return;
  }

  enableTestAuthBypass();
  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  const app = createApp();
  const token = `tt-${Date.now()}-${Math.floor(Math.random() * 100000)}`;
  const startRes = await request(app).post("/time-tracking/entries/draft/start").send({
    activityType: "arztbesuch",
    startAt: "2026-04-10T08:00:00.000Z",
    clientEntryToken: token,
  });
  assert.equal(startRes.status, 200);
  const entryId = startRes.body?.entry?.id as string;

  const activeRes = await request(app).get("/time-tracking/entries/draft/active");
  assert.equal(activeRes.status, 200);
  assert.equal(activeRes.body?.offlineContract?.idempotentTokenField, "clientEntryToken");
  assert.equal(typeof activeRes.body?.offlineContract?.retryGuidance, "string");
  assert.equal(Array.isArray(activeRes.body?.entries), true);

  await db
    .update(timeTrackingEntries)
    .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
    .where(eq(timeTrackingEntries.id, entryId));
});

test("kuehler fragebogen rejects spezialfragen with 400", async () => {
  enableTestAuthBypass();
  const app = createApp();
  const response = await request(app)
    .post("/admin/fragebogen/kuehler")
    .send({
      name: "KB Test",
      description: "",
      moduleIds: [],
      scheduleType: "always",
      status: "inactive",
      nurEinmalAusfuellbar: false,
      spezialfragen: [
        {
          type: "text",
          text: "Spezial",
          required: true,
          config: {},
          rules: [],
          scoring: {},
        },
      ],
    });
  assert.equal(response.status, 400);
  assert.equal(response.body.error, "Spezialfragen sind nur fuer den Main-Scope erlaubt.");
});

test("invalid schedule date format is mapped to 400", async () => {
  enableTestAuthBypass();
  const app = createApp();
  const response = await request(app)
    .post("/admin/fragebogen/main")
    .send({
      name: "Date Test",
      description: "",
      moduleIds: [],
      scheduleType: "scheduled",
      startDate: "10/31/2026",
      endDate: "2026-12-31",
      status: "scheduled",
      nurEinmalAusfuellbar: false,
      spezialfragen: [],
      sectionKeywords: ["standard"],
    });
  assert.equal(response.status, 400);
});

test("invalid photo tag payload is mapped to 400", async () => {
  enableTestAuthBypass();
  const app = createApp();
  const response = await request(app).post("/admin/photo-tags").send({ label: "" });
  assert.equal(response.status, 400);
  assert.equal(response.body.error, "Ungueltiger Tag.");
});

test("invalid calendar day is rejected with 400", async () => {
  enableTestAuthBypass();
  const app = createApp();
  const response = await request(app)
    .post("/admin/fragebogen/main")
    .send({
      name: "Invalid Calendar Date",
      description: "",
      moduleIds: [],
      scheduleType: "scheduled",
      startDate: "2026-02-31",
      endDate: "2026-03-05",
      status: "scheduled",
      nurEinmalAusfuellbar: false,
      spezialfragen: [],
      sectionKeywords: ["standard"],
    });
  assert.equal(response.status, 400);
});

test("spezialfrage rejects invalid trigger UUID format with 400", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const response = await request(app)
    .post("/admin/fragebogen/main")
    .send({
      name: "Spezial Invalid Trigger UUID",
      description: "",
      moduleIds: [],
      scheduleType: "always",
      status: "inactive",
      nurEinmalAusfuellbar: false,
      sectionKeywords: ["standard"],
      spezialfragen: [
        {
          type: "text",
          text: "Spezial",
          required: true,
          config: {},
          rules: [
            {
              triggerQuestionId: "not-a-uuid",
              operator: "equals",
              triggerValue: "x",
              action: "show",
              targetQuestionIds: [],
            },
          ],
          scoring: {},
        },
      ],
    });
  assert.equal(response.status, 400);
  assert.equal(response.body.error, "Spezialfrage-Regeltrigger muss eine gueltige UUID sein.");
});

test("spezialfrage rejects missing shared question references with 400", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const missingQuestionId = randomUUID();
  const response = await request(app)
    .post("/admin/fragebogen/main")
    .send({
      name: "Spezial Missing Shared Ref",
      description: "",
      moduleIds: [],
      scheduleType: "always",
      status: "inactive",
      nurEinmalAusfuellbar: false,
      sectionKeywords: ["standard"],
      spezialfragen: [
        {
          type: "text",
          text: "Spezial",
          required: true,
          config: {},
          rules: [
            {
              triggerQuestionId: missingQuestionId,
              operator: "equals",
              triggerValue: "x",
              action: "show",
              targetQuestionIds: [],
            },
          ],
          scoring: {},
        },
      ],
    });
  assert.equal(response.status, 400);
  assert.match(String(response.body.error), /referenzierte Frage existiert nicht oder ist geloescht/);
});

test("question validation rejects empty options for single choice with 400", async () => {
  enableTestAuthBypass();
  const app = createApp();
  const response = await request(app).post("/admin/questions").send({
    type: "single",
    text: "Single ohne Optionen",
    required: true,
    config: { options: [] },
    rules: [],
    scoring: {},
  });
  assert.equal(response.status, 400);
  assert.equal(response.body.error, "Auswahlfragen benoetigen mindestens eine Option.");
});

test("question validation rejects likert without labels or range with 400", async () => {
  enableTestAuthBypass();
  const app = createApp();
  const response = await request(app).post("/admin/questions").send({
    type: "likert",
    text: "Likert ungueltig",
    required: true,
    config: {},
    rules: [],
    scoring: {},
  });
  assert.equal(response.status, 400);
  assert.equal(response.body.error, "Likert-Fragen benoetigen Labels oder einen gueltigen min/max Bereich.");
});

test("question validation rejects photo tagsEnabled without tagIds with 400", async () => {
  enableTestAuthBypass();
  const app = createApp();
  const response = await request(app).post("/admin/questions").send({
    type: "photo",
    text: "Foto ohne Tags",
    required: true,
    config: { tagsEnabled: true, tagIds: [] },
    rules: [],
    scoring: {},
  });
  assert.equal(response.status, 400);
  assert.equal(response.body.error, "Foto-Fragen mit aktivierten Tags benoetigen mindestens ein Tag.");
});

test("question validation rejects text minLength greater than maxLength with 400", async () => {
  enableTestAuthBypass();
  const app = createApp();
  const response = await request(app).post("/admin/questions").send({
    type: "text",
    text: "Text ungueltige Laenge",
    required: true,
    config: { minLength: 20, maxLength: 10 },
    rules: [],
    scoring: {},
  });
  assert.equal(response.status, 400);
  assert.equal(response.body.error, "Textfragen minLength darf nicht groesser als maxLength sein.");
});

test("question validation rejects numeric min greater than max with 400", async () => {
  enableTestAuthBypass();
  const app = createApp();
  const response = await request(app).post("/admin/questions").send({
    type: "numeric",
    text: "Numeric ungueltige Grenzen",
    required: true,
    config: { min: 10, max: 1 },
    rules: [],
    scoring: {
      __value__: { ipp: 1, boni: 1 },
    },
  });
  assert.equal(response.status, 400);
  assert.equal(response.body.error, "Numerische Fragen min darf nicht groesser als max sein.");
});

test("main scope module + fragebogen roundtrip persists spezialfragen table", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const uniq = `it-${Date.now()}`;

  const createModuleRes = await request(app)
    .post("/admin/modules/main")
    .send({
      name: `Module ${uniq}`,
      description: "",
      questions: [
        {
          type: "text",
          text: `Q ${uniq}`,
          required: true,
          config: {},
          rules: [],
          scoring: {},
        },
      ],
      sectionKeywords: ["standard"],
    });
  assert.equal(createModuleRes.status, 201);
  const moduleId = createModuleRes.body.module?.id as string;
  assert.ok(moduleId);

  const createFragebogenRes = await request(app)
    .post("/admin/fragebogen/main")
    .send({
      name: `Fragebogen ${uniq}`,
      description: "",
      moduleIds: [moduleId],
      scheduleType: "always",
      status: "active",
      nurEinmalAusfuellbar: false,
      sectionKeywords: ["standard"],
      spezialfragen: [
        {
          type: "text",
          text: `Spezial ${uniq}`,
          required: true,
          config: {},
          rules: [],
          scoring: {},
        },
      ],
    });
  assert.equal(createFragebogenRes.status, 201);
  const fragebogenId = createFragebogenRes.body.fragebogen?.id as string;
  assert.ok(fragebogenId);
  assert.equal(createFragebogenRes.body.fragebogen?.spezialfragen?.length, 1);

  const persistedSpezial = await db
    .select()
    .from(fragebogenMainSpezialItems)
    .where(eq(fragebogenMainSpezialItems.fragebogenId, fragebogenId));
  assert.equal(persistedSpezial.length, 1);

  const duplicateRes = await request(app)
    .post(`/admin/fragebogen/main/${fragebogenId}/duplicate`)
    .send({ targetScope: "main", sectionKeywords: ["standard"] });
  assert.equal(duplicateRes.status, 201);
  assert.equal(duplicateRes.body.fragebogen?.spezialfragen?.length, 1);

  await request(app).patch(`/admin/fragebogen/main/${fragebogenId}/delete`).send({});
  if (duplicateRes.body.fragebogen?.id) {
    await request(app).patch(`/admin/fragebogen/main/${duplicateRes.body.fragebogen.id}/delete`).send({});
  }
  await request(app).patch(`/admin/modules/main/${moduleId}/delete`).send({});
});

test("duplicate main fragebogen to kuehler fails when modules are missing", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const uniq = `it-cross-${Date.now()}`;

  const createModuleRes = await request(app)
    .post("/admin/modules/main")
    .send({
      name: `Module ${uniq}`,
      description: "",
      questions: [],
      sectionKeywords: ["standard"],
    });
  assert.equal(createModuleRes.status, 201);
  const moduleId = createModuleRes.body.module?.id as string;
  assert.ok(moduleId);

  const createFragebogenRes = await request(app)
    .post("/admin/fragebogen/main")
    .send({
      name: `FB ${uniq}`,
      description: "",
      moduleIds: [moduleId],
      scheduleType: "always",
      status: "inactive",
      nurEinmalAusfuellbar: false,
      sectionKeywords: ["standard"],
      spezialfragen: [],
    });
  assert.equal(createFragebogenRes.status, 201);
  const fragebogenId = createFragebogenRes.body.fragebogen?.id as string;
  assert.ok(fragebogenId);

  const crossDuplicateRes = await request(app)
    .post(`/admin/fragebogen/main/${fragebogenId}/duplicate`)
    .send({ targetScope: "kuehler" });
  assert.equal(crossDuplicateRes.status, 400);

  await request(app).patch(`/admin/fragebogen/main/${fragebogenId}/delete`).send({});
  await request(app).patch(`/admin/modules/main/${moduleId}/delete`).send({});
});

test("module update soft-deletes old module_question links", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const uniq = `it-module-soft-${Date.now()}`;

  const q1Res = await request(app).post("/admin/questions").send({
    type: "text",
    text: `Q1 ${uniq}`,
    required: true,
    config: {},
    rules: [],
    scoring: {},
  });
  assert.equal(q1Res.status, 201);
  const q1Id = q1Res.body.question?.id as string;
  assert.ok(q1Id);

  const q2Res = await request(app).post("/admin/questions").send({
    type: "text",
    text: `Q2 ${uniq}`,
    required: true,
    config: {},
    rules: [],
    scoring: {},
  });
  assert.equal(q2Res.status, 201);
  const q2Id = q2Res.body.question?.id as string;
  assert.ok(q2Id);

  const createModuleRes = await request(app).post("/admin/modules/main").send({
    name: `Module ${uniq}`,
    description: "",
    sectionKeywords: ["standard"],
    questions: [
      {
        id: q1Id,
        type: "text",
        text: `Q1 ${uniq}`,
        required: true,
        config: {},
        rules: [],
        scoring: {},
      },
    ],
  });
  assert.equal(createModuleRes.status, 201);
  const moduleId = createModuleRes.body.module?.id as string;
  assert.ok(moduleId);

  const patchModuleRes = await request(app).patch(`/admin/modules/main/${moduleId}`).send({
    name: `Module ${uniq}`,
    description: "",
    sectionKeywords: ["standard"],
    questions: [
      {
        id: q2Id,
        type: "text",
        text: `Q2 ${uniq}`,
        required: true,
        config: {},
        rules: [],
        scoring: {},
      },
    ],
  });
  assert.equal(patchModuleRes.status, 200);

  const activeRows = await db
    .select()
    .from(moduleMainQuestion)
    .where(eq(moduleMainQuestion.moduleId, moduleId));
  const q1Link = activeRows.find((row) => row.questionId === q1Id);
  const q2Link = activeRows.find((row) => row.questionId === q2Id);
  assert.ok(q1Link);
  assert.ok(q2Link);
  assert.equal(q1Link?.isDeleted, true);
  assert.equal(q2Link?.isDeleted, false);

  await request(app).patch(`/admin/modules/main/${moduleId}/delete`).send({});
});

test("module create accepts yesnomulti answers and branches config", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }

  const app = createApp();
  const uniq = `it-yesnomulti-${Date.now()}`;
  const createModuleRes = await request(app).post("/admin/modules/main").send({
    name: `Module ${uniq}`,
    description: "",
    sectionKeywords: ["standard"],
    questions: [
      {
        id: `tmp-yesnomulti-${uniq}`,
        type: "yesnomulti",
        text: `YesNoMulti ${uniq}`,
        required: true,
        config: {
          answers: ["Ja", "Nein"],
          branches: {
            Ja: {
              enabled: true,
              options: ["A", "B"],
            },
          },
        },
        rules: [],
        scoring: {},
      },
    ],
  });
  assert.equal(createModuleRes.status, 201);
  assert.equal(createModuleRes.body.module?.questions?.[0]?.type, "yesnomulti");
  assert.deepEqual(createModuleRes.body.module?.questions?.[0]?.config?.answers, ["Ja", "Nein"]);

  const moduleId = createModuleRes.body.module?.id as string;
  assert.ok(moduleId);
  await request(app).patch(`/admin/modules/main/${moduleId}/delete`).send({});
});

test("module create accepts yesnomulti branch array shape from editor payload", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }

  const app = createApp();
  const uniq = `it-yesnomulti-array-${Date.now()}`;
  const createModuleRes = await request(app).post("/admin/modules/main").send({
    name: `Module ${uniq}`,
    description: "",
    sectionKeywords: ["standard"],
    questions: [
      {
        id: `tmp-yesnomulti-array-${uniq}`,
        type: "yesnomulti",
        text: `YesNoMulti Array ${uniq}`,
        required: true,
        config: {
          answers: ["Ja", "Nein"],
          branches: [{ answer: "Ja", options: ["Opt A", "Opt B"] }],
        },
        rules: [],
        scoring: {},
      },
    ],
  });
  assert.equal(createModuleRes.status, 201);

  const moduleId = createModuleRes.body.module?.id as string;
  assert.ok(moduleId);
  await request(app).patch(`/admin/modules/main/${moduleId}/delete`).send({});
});

test("module create remaps rule references between new questions in one payload", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }

  const app = createApp();
  const uniq = `it-rule-remap-${Date.now()}`;
  const tempSourceId = `tmp-source-${uniq}`;
  const tempTargetId = `tmp-target-${uniq}`;
  const createModuleRes = await request(app).post("/admin/modules/main").send({
    name: `Module ${uniq}`,
    description: "",
    sectionKeywords: ["standard"],
    questions: [
      {
        id: tempSourceId,
        type: "single",
        text: `Source ${uniq}`,
        required: true,
        config: { options: ["Ja", "Nein"] },
        rules: [
          {
            triggerQuestionId: "",
            operator: "equals",
            triggerValue: "Ja",
            triggerValueMax: "",
            action: "show",
            targetQuestionIds: [tempTargetId],
          },
        ],
        scoring: {},
      },
      {
        id: tempTargetId,
        type: "text",
        text: `Target ${uniq}`,
        required: true,
        config: {},
        rules: [],
        scoring: {},
      },
    ],
  });
  assert.equal(createModuleRes.status, 201);
  const persistedQuestions = createModuleRes.body.module?.questions as Array<{ id: string; text: string }>;
  const source = persistedQuestions.find((question) => question.text === `Source ${uniq}`);
  const target = persistedQuestions.find((question) => question.text === `Target ${uniq}`);
  assert.ok(source?.id);
  assert.ok(target?.id);

  const [persistedRule] = await db
    .select({ id: questionRules.id })
    .from(questionRules)
    .where(and(eq(questionRules.questionId, source!.id), eq(questionRules.isDeleted, false)));
  assert.ok(persistedRule?.id);

  const targets = await db
    .select({ targetQuestionId: questionRuleTargets.targetQuestionId })
    .from(questionRuleTargets)
    .where(and(eq(questionRuleTargets.ruleId, persistedRule.id), eq(questionRuleTargets.isDeleted, false)));
  assert.deepEqual(targets.map((row) => row.targetQuestionId), [target!.id]);

  const moduleId = createModuleRes.body.module?.id as string;
  assert.ok(moduleId);
  await request(app).patch(`/admin/modules/main/${moduleId}/delete`).send({});
});

test("module create rejects unresolved rule references with explicit domain error", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }

  const app = createApp();
  const uniq = `it-rule-missing-${Date.now()}`;
  const response = await request(app).post("/admin/modules/main").send({
    name: `Module ${uniq}`,
    description: "",
    sectionKeywords: ["standard"],
    questions: [
      {
        id: `tmp-source-${uniq}`,
        type: "single",
        text: `Source ${uniq}`,
        required: true,
        config: { options: ["Ja", "Nein"] },
        rules: [
          {
            triggerQuestionId: "",
            operator: "equals",
            triggerValue: "Ja",
            triggerValueMax: "",
            action: "show",
            targetQuestionIds: [`tmp-missing-${uniq}`],
          },
        ],
        scoring: {},
      },
    ],
  });
  assert.equal(response.status, 400);
  assert.equal(response.body.error, "Regel-Ziel verweist auf eine unbekannte Frage-ID.");
});

test("module create allows self-trigger when affected question comes later", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }

  const app = createApp();
  const uniq = `it-rule-self-trigger-${Date.now()}`;
  const tempQuestion1 = `tmp-q1-${uniq}`;
  const tempQuestion2 = `tmp-q2-${uniq}`;
  const tempQuestion3 = `tmp-q3-${uniq}`;
  const response = await request(app).post("/admin/modules/main").send({
    name: `Module ${uniq}`,
    description: "",
    sectionKeywords: ["standard"],
    questions: [
      {
        id: tempQuestion1,
        type: "single",
        text: `Q1 ${uniq}`,
        required: true,
        config: { options: ["Ja", "Nein"] },
        rules: [],
        scoring: {},
      },
      {
        id: tempQuestion2,
        type: "single",
        text: `Q2 ${uniq}`,
        required: true,
        config: { options: ["Ja", "Nein"] },
        rules: [
          {
            triggerQuestionId: tempQuestion2,
            operator: "equals",
            triggerValue: "Nein",
            triggerValueMax: "",
            action: "hide",
            targetQuestionIds: [tempQuestion3],
          },
        ],
        scoring: {},
      },
      {
        id: tempQuestion3,
        type: "text",
        text: `Q3 ${uniq}`,
        required: true,
        config: {},
        rules: [],
        scoring: {},
      },
    ],
  });
  assert.equal(response.status, 201);
  const moduleId = response.body.module?.id as string;
  assert.ok(moduleId);
  await request(app).patch(`/admin/modules/main/${moduleId}/delete`).send({});
});

test("module create rejects affected questions that are not after source question", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }

  const app = createApp();
  const uniq = `it-rule-order-${Date.now()}`;
  const tempQuestion1 = `tmp-q1-${uniq}`;
  const tempQuestion2 = `tmp-q2-${uniq}`;
  const tempQuestion3 = `tmp-q3-${uniq}`;
  const response = await request(app).post("/admin/modules/main").send({
    name: `Module ${uniq}`,
    description: "",
    sectionKeywords: ["standard"],
    questions: [
      {
        id: tempQuestion1,
        type: "single",
        text: `Q1 ${uniq}`,
        required: true,
        config: { options: ["Ja", "Nein"] },
        rules: [],
        scoring: {},
      },
      {
        id: tempQuestion2,
        type: "single",
        text: `Q2 ${uniq}`,
        required: true,
        config: { options: ["Ja", "Nein"] },
        rules: [
          {
            triggerQuestionId: tempQuestion2,
            operator: "equals",
            triggerValue: "Nein",
            triggerValueMax: "",
            action: "hide",
            targetQuestionIds: [tempQuestion1],
          },
        ],
        scoring: {},
      },
      {
        id: tempQuestion3,
        type: "text",
        text: `Q3 ${uniq}`,
        required: true,
        config: {},
        rules: [],
        scoring: {},
      },
    ],
  });
  assert.equal(response.status, 400);
  assert.equal(response.body.error, "Betroffene Fragen muessen nach der ausloesenden Frage im Modul kommen.");
});

test("standalone question save allows self-trigger but rejects self-target", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }

  const app = createApp();
  const uniq = `it-standalone-self-trigger-${Date.now()}`;
  const q1Res = await request(app).post("/admin/questions").send({
    type: "single",
    text: `Q1 ${uniq}`,
    required: true,
    config: { options: ["Ja", "Nein"] },
    rules: [],
    scoring: {},
  });
  assert.equal(q1Res.status, 201);
  const q1Id = q1Res.body.question?.id as string;
  assert.ok(q1Id);

  const q2Res = await request(app).post("/admin/questions").send({
    type: "text",
    text: `Q2 ${uniq}`,
    required: true,
    config: {},
    rules: [],
    scoring: {},
  });
  assert.equal(q2Res.status, 201);
  const q2Id = q2Res.body.question?.id as string;
  assert.ok(q2Id);

  const allowSelfTriggerRes = await request(app).patch(`/admin/questions/${q1Id}`).send({
    config: { options: ["Ja", "Nein"] },
    rules: [
      {
        triggerQuestionId: q1Id,
        operator: "equals",
        triggerValue: "Nein",
        triggerValueMax: "",
        action: "hide",
        targetQuestionIds: [q2Id],
      },
    ],
  });
  assert.equal(allowSelfTriggerRes.status, 200);

  const rejectSelfTargetRes = await request(app).patch(`/admin/questions/${q1Id}`).send({
    config: { options: ["Ja", "Nein"] },
    rules: [
      {
        triggerQuestionId: q1Id,
        operator: "equals",
        triggerValue: "Nein",
        triggerValueMax: "",
        action: "hide",
        targetQuestionIds: [q1Id],
      },
    ],
  });
  assert.equal(rejectSelfTargetRes.status, 400);
  assert.equal(rejectSelfTargetRes.body.error, "Regeln duerfen nicht auf dieselbe Frage verweisen.");
});

test("fragebogen update soft-deletes old module links and spezial rows", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const uniq = `it-fb-soft-${Date.now()}`;

  const createModuleRes = await request(app).post("/admin/modules/main").send({
    name: `Module ${uniq}`,
    description: "",
    sectionKeywords: ["standard"],
    questions: [],
  });
  assert.equal(createModuleRes.status, 201);
  const moduleId = createModuleRes.body.module?.id as string;
  assert.ok(moduleId);

  const createFragebogenRes = await request(app).post("/admin/fragebogen/main").send({
    name: `FB ${uniq}`,
    description: "",
    moduleIds: [moduleId],
    scheduleType: "always",
    status: "inactive",
    nurEinmalAusfuellbar: false,
    sectionKeywords: ["standard"],
    spezialfragen: [
      {
        type: "text",
        text: `Spezial A ${uniq}`,
        required: true,
        config: {},
        rules: [],
        scoring: {},
      },
    ],
  });
  assert.equal(createFragebogenRes.status, 201);
  const fragebogenId = createFragebogenRes.body.fragebogen?.id as string;
  assert.ok(fragebogenId);

  const patchFragebogenRes = await request(app).patch(`/admin/fragebogen/main/${fragebogenId}`).send({
    name: `FB ${uniq}`,
    description: "",
    moduleIds: [],
    scheduleType: "always",
    status: "inactive",
    nurEinmalAusfuellbar: false,
    sectionKeywords: ["standard"],
    spezialfragen: [
      {
        type: "text",
        text: `Spezial B ${uniq}`,
        required: true,
        config: {},
        rules: [],
        scoring: {},
      },
    ],
  });
  assert.equal(patchFragebogenRes.status, 200);

  const fbLinks = await db
    .select()
    .from(fragebogenMainModule)
    .where(eq(fragebogenMainModule.fragebogenId, fragebogenId));
  assert.equal(fbLinks.length, 1);
  assert.equal(fbLinks[0]?.isDeleted, true);

  const spezialRows = await db
    .select()
    .from(fragebogenMainSpezialItems)
    .where(eq(fragebogenMainSpezialItems.fragebogenId, fragebogenId));
  assert.equal(spezialRows.length, 2);
  assert.equal(spezialRows.filter((row) => row.isDeleted).length, 1);
  assert.equal(spezialRows.filter((row) => !row.isDeleted).length, 1);

  await request(app).patch(`/admin/fragebogen/main/${fragebogenId}/delete`).send({});
  await request(app).patch(`/admin/modules/main/${moduleId}/delete`).send({});
});

test("module question chains stay module-scoped and duplicate with module", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }
  if (!(await ensureDbReadyForModuleQuestionChainsTests())) {
    t.skip("module_question_chains table is not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const uniq = `it-module-chains-${Date.now()}`;

  const questionRes = await request(app).post("/admin/questions").send({
    type: "text",
    text: `Q ${uniq}`,
    required: true,
    config: {},
    rules: [],
    scoring: {},
  });
  assert.equal(questionRes.status, 201);
  const sharedQuestionId = questionRes.body.question?.id as string;
  assert.ok(sharedQuestionId);

  const moduleARes = await request(app).post("/admin/modules/main").send({
    name: `Module A ${uniq}`,
    description: "",
    sectionKeywords: ["standard"],
    questions: [
      {
        id: sharedQuestionId,
        type: "text",
        text: `Q ${uniq}`,
        required: true,
        config: {},
        rules: [],
        scoring: {},
        chains: ["SPAR", "BILLA"],
      },
    ],
  });
  assert.equal(moduleARes.status, 201);
  const moduleAId = moduleARes.body.module?.id as string;
  assert.ok(moduleAId);

  const moduleBRes = await request(app).post("/admin/modules/main").send({
    name: `Module B ${uniq}`,
    description: "",
    sectionKeywords: ["standard"],
    questions: [
      {
        id: sharedQuestionId,
        type: "text",
        text: `Q ${uniq}`,
        required: true,
        config: {},
        rules: [],
        scoring: {},
      },
    ],
  });
  assert.equal(moduleBRes.status, 201);
  const moduleBId = moduleBRes.body.module?.id as string;
  assert.ok(moduleBId);

  const modulesReadRes = await request(app).get("/admin/modules/main");
  assert.equal(modulesReadRes.status, 200);
  const moduleA = (modulesReadRes.body.modules as Array<{ id: string; questions?: Array<{ id?: string; chains?: string[] }> }>).find(
    (entry) => entry.id === moduleAId,
  );
  const moduleB = (modulesReadRes.body.modules as Array<{ id: string; questions?: Array<{ id?: string; chains?: string[] }> }>).find(
    (entry) => entry.id === moduleBId,
  );
  assert.ok(moduleA);
  assert.ok(moduleB);
  assert.deepEqual([...(moduleA?.questions?.[0]?.chains ?? [])].sort(), ["BILLA", "SPAR"]);
  assert.equal(moduleB?.questions?.[0]?.chains, undefined);

  const patchModuleBRes = await request(app).patch(`/admin/modules/main/${moduleBId}`).send({
    id: moduleBId,
    name: `Module B ${uniq}`,
    description: "",
    sectionKeywords: ["standard"],
    questions: [
      {
        id: sharedQuestionId,
        type: "text",
        text: `Q ${uniq}`,
        required: true,
        config: {},
        rules: [],
        scoring: {},
        chains: ["HOFER"],
      },
    ],
  });
  assert.equal(patchModuleBRes.status, 200);
  assert.deepEqual(patchModuleBRes.body.module?.questions?.[0]?.chains, ["HOFER"]);

  const moduleAAfterPatchRes = await request(app).get("/admin/modules/main");
  assert.equal(moduleAAfterPatchRes.status, 200);
  const moduleAAfterPatch = (moduleAAfterPatchRes.body.modules as Array<{ id: string; questions?: Array<{ chains?: string[] }> }>).find(
    (entry) => entry.id === moduleAId,
  );
  assert.deepEqual([...(moduleAAfterPatch?.questions?.[0]?.chains ?? [])].sort(), ["BILLA", "SPAR"]);

  const duplicateRes = await request(app).post(`/admin/modules/main/${moduleAId}/duplicate`).send({});
  assert.equal(duplicateRes.status, 201);
  const duplicatedModuleId = duplicateRes.body.module?.id as string;
  assert.ok(duplicatedModuleId);
  assert.deepEqual([...(duplicateRes.body.module?.questions?.[0]?.chains ?? [])].sort(), ["BILLA", "SPAR"]);

  const chainRows = await db
    .select()
    .from(moduleQuestionChains)
    .where(inArray(moduleQuestionChains.moduleId, [moduleAId, moduleBId, duplicatedModuleId]));
  assert.equal(chainRows.some((row) => row.moduleId === moduleAId && row.chainDbName === "SPAR" && !row.isDeleted), true);
  assert.equal(chainRows.some((row) => row.moduleId === moduleAId && row.chainDbName === "BILLA" && !row.isDeleted), true);
  assert.equal(chainRows.some((row) => row.moduleId === moduleBId && row.chainDbName === "HOFER" && !row.isDeleted), true);
  assert.equal(chainRows.some((row) => row.moduleId === moduleBId && row.chainDbName === "SPAR" && !row.isDeleted), false);
  assert.equal(chainRows.some((row) => row.moduleId === duplicatedModuleId && row.chainDbName === "SPAR" && !row.isDeleted), true);

  await request(app).patch(`/admin/modules/main/${moduleAId}/delete`).send({});
  await request(app).patch(`/admin/modules/main/${moduleBId}/delete`).send({});
  await request(app).patch(`/admin/modules/main/${duplicatedModuleId}/delete`).send({});
});

test("question patch soft-deletes prior children and keeps only active graph", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForFragebogenTests())) {
    t.skip("Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const uniq = `it-q-soft-${Date.now()}`;

  const createTagRes = await request(app).post("/admin/photo-tags").send({ label: `Tag ${uniq}` });
  assert.equal(createTagRes.status, 201);
  const tagId = createTagRes.body.tag?.id as string;
  assert.ok(tagId);

  const createQuestionRes = await request(app).post("/admin/questions").send({
    type: "photo",
    text: `Q ${uniq}`,
    required: true,
    config: { tagsEnabled: true, tagIds: [tagId], images: ["img-a"] },
    rules: [],
    scoring: { scoreA: { ipp: 1 } },
  });
  assert.equal(createQuestionRes.status, 201);
  const questionId = createQuestionRes.body.question?.id as string;
  assert.ok(questionId);

  const patchQuestionRes = await request(app).patch(`/admin/questions/${questionId}`).send({
    config: { tagsEnabled: true, tagIds: [tagId], images: ["img-b"] },
    scoring: { scoreB: { boni: 2 } },
  });
  assert.equal(patchQuestionRes.status, 200);

  const scoringRows = await db.select().from(questionScoring).where(eq(questionScoring.questionId, questionId));
  assert.equal(scoringRows.some((row) => row.scoreKey === "scoreA" && row.isDeleted), true);
  assert.equal(scoringRows.some((row) => row.scoreKey === "scoreB" && !row.isDeleted), true);

  const attachmentRows = await db
    .select()
    .from(questionAttachments)
    .where(eq(questionAttachments.questionId, questionId));
  assert.equal(attachmentRows.some((row) => row.payload === "img-a" && row.isDeleted), true);
  assert.equal(attachmentRows.some((row) => row.payload === "img-b" && !row.isDeleted), true);

  const photoTagRows = await db.select().from(questionPhotoTags).where(eq(questionPhotoTags.questionId, questionId));
  assert.equal(photoTagRows.length >= 1, true);
  assert.equal(photoTagRows.some((row) => !row.isDeleted), true);

  const ruleRows = await db.select().from(questionRules).where(eq(questionRules.questionId, questionId));
  assert.equal(ruleRows.length >= 0, true);
});

test("campaign create for section standard succeeds", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests())) {
    t.skip("Campaign tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const uniq = `campaign-${Date.now()}`;
  const createRes = await request(app).post("/admin/campaigns").send({
    name: `Campaign ${uniq}`,
    section: "standard",
    status: "inactive",
    scheduleType: "always",
    marketIds: [],
  });
  assert.equal(createRes.status, 201);
  assert.equal(createRes.body.campaign?.section, "standard");
  assert.equal(createRes.body.campaign?.scheduleType, "always");

  const campaignId = createRes.body.campaign?.id as string;
  await request(app).patch(`/admin/campaigns/${campaignId}/delete`).send({});
});

test("campaign schedule validation rejects bad date window", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests())) {
    t.skip("Campaign tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const res = await request(app).post("/admin/campaigns").send({
    name: `Campaign Bad Date ${Date.now()}`,
    section: "standard",
    status: "scheduled",
    scheduleType: "scheduled",
    startDate: "2026-12-31",
    endDate: "2026-01-01",
    marketIds: [],
  });
  assert.equal(res.status, 400);
  assert.equal(res.body.code, "schedule_invalid");
});

test("campaign market assignment add/remove uses soft delete", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests())) {
    t.skip("Campaign tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const marketsRes = await request(app).get("/markets");
  assert.equal(marketsRes.status, 200);
  const marketId = marketsRes.body.markets?.[0]?.id as string | undefined;
  if (!marketId) {
    t.skip("No markets available for assignment test.");
    return;
  }

  const createRes = await request(app).post("/admin/campaigns").send({
    name: `Campaign Assign ${Date.now()}`,
    section: "standard",
    status: "inactive",
    scheduleType: "always",
    marketIds: [],
  });
  assert.equal(createRes.status, 201);
  const campaignId = createRes.body.campaign?.id as string;

  const addRes = await request(app).post(`/admin/campaigns/${campaignId}/markets`).send({ marketIds: [marketId] });
  assert.equal(addRes.status, 200);
  assert.equal(addRes.body.campaign?.marketIds?.includes(marketId), true);

  const removeRes = await request(app).patch(`/admin/campaigns/${campaignId}/markets/${marketId}/delete`).send({});
  assert.equal(removeRes.status, 200);
  assert.equal(removeRes.body.campaign?.marketIds?.includes(marketId), false);
  assert.equal(removeRes.body.removed, true);

  const removeAgainRes = await request(app).patch(`/admin/campaigns/${campaignId}/markets/${marketId}/delete`).send({});
  assert.equal(removeAgainRes.status, 200);
  assert.equal(removeAgainRes.body.removed, false);

  const addAgainRes = await request(app).post(`/admin/campaigns/${campaignId}/markets`).send({ marketIds: [marketId] });
  assert.equal(addAgainRes.status, 200);
  assert.equal(addAgainRes.body.campaign?.marketIds?.includes(marketId), true);

  const [assignment] = await db
    .select()
    .from(campaignMarketAssignments)
    .where(eq(campaignMarketAssignments.campaignId, campaignId));
  assert.ok(assignment);
  assert.equal(assignment?.isDeleted, false);

  await request(app).patch(`/admin/campaigns/${campaignId}/delete`).send({});
});

test("campaign create validates gmUserId on assignments", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests())) {
    t.skip("Campaign tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const marketsRes = await request(app).get("/markets");
  assert.equal(marketsRes.status, 200);
  const marketId = marketsRes.body.markets?.[0]?.id as string | undefined;
  if (!marketId) {
    t.skip("No markets available for assignment validation test.");
    return;
  }

  const invalidRes = await request(app).post("/admin/campaigns").send({
    name: `Campaign Invalid GM ${Date.now()}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    assignments: [{ marketId, gmUserId: "00000000-0000-4000-8000-000000009999" }],
  });
  assert.equal(invalidRes.status, 400);
  assert.equal(invalidRes.body.code, "gm_missing");
});

test("campaign assignment endpoint persists gmUserId", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests())) {
    t.skip("Campaign tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for assignment persistence test.");
    return;
  }

  const app = createApp();
  const marketsRes = await request(app).get("/markets");
  assert.equal(marketsRes.status, 200);
  const marketId = marketsRes.body.markets?.[0]?.id as string | undefined;
  if (!marketId) {
    t.skip("No markets available for assignment persistence test.");
    return;
  }

  const createRes = await request(app).post("/admin/campaigns").send({
    name: `Campaign Persist GM ${Date.now()}`,
    section: "standard",
    status: "inactive",
    scheduleType: "always",
    marketIds: [],
  });
  assert.equal(createRes.status, 201);
  const campaignId = createRes.body.campaign?.id as string;

  const assignRes = await request(app).post(`/admin/campaigns/${campaignId}/markets`).send({
    assignments: [{ marketId, gmUserId: gmUser.id }],
  });
  assert.equal(assignRes.status, 200);
  assert.equal(assignRes.body.campaign?.marketIds?.includes(marketId), true);

  const [row] = await db
    .select({ gmUserId: campaignMarketAssignments.gmUserId })
    .from(campaignMarketAssignments)
    .where(and(eq(campaignMarketAssignments.campaignId, campaignId), eq(campaignMarketAssignments.marketId, marketId)))
    .limit(1);
  assert.equal(row?.gmUserId, gmUser.id);

  await request(app).patch(`/admin/campaigns/${campaignId}/delete`).send({});
});

test("campaign create blocks same-section overlapping market assignment with 409", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests())) {
    t.skip("Campaign tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for overlap test.");
    return;
  }
  const app = createApp();
  const marketCreateRes = await request(app).post("/admin/markets").send({
    name: `Overlap Markt ${Date.now()}`,
    dbName: "",
    address: "Overlap Testgasse 1",
    postalCode: "1010",
    city: "Wien",
    region: "Wien",
  });
  assert.equal(marketCreateRes.status, 201);
  const marketId = marketCreateRes.body.market?.id as string;
  const uniq = `overlap-${Date.now()}`;

  const source = await request(app).post("/admin/campaigns").send({
    name: `MHD A ${uniq}`,
    section: "mhd",
    status: "active",
    scheduleType: "always",
    assignments: [{ marketId, gmUserId: gmUser.id }],
  });
  assert.equal(source.status, 201);
  const sourceId = source.body.campaign?.id as string;

  const overlap = await request(app).post("/admin/campaigns").send({
    name: `MHD B ${uniq}`,
    section: "mhd",
    status: "active",
    scheduleType: "always",
    assignments: [{ marketId, gmUserId: gmUser.id }],
  });
  assert.equal(overlap.status, 409);
  assert.equal(overlap.body.code, "campaign_market_overlap");
  assert.equal(Array.isArray(overlap.body.conflicts), true);
  assert.equal(overlap.body.conflicts?.[0]?.marketId, marketId);

  const differentSectionAllowed = await request(app).post("/admin/campaigns").send({
    name: `Standard ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    assignments: [{ marketId, gmUserId: gmUser.id }],
  });
  assert.equal(differentSectionAllowed.status, 201);
  const differentSectionId = differentSectionAllowed.body.campaign?.id as string;

  await request(app).patch(`/admin/campaigns/${sourceId}/delete`).send({});
  await request(app).patch(`/admin/campaigns/${differentSectionId}/delete`).send({});
  await request(app).patch(`/admin/markets/${marketId}/delete`).send({});
});

test("campaign create allows same-section non-overlapping scheduled windows", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests())) {
    t.skip("Campaign tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for schedule overlap test.");
    return;
  }
  const app = createApp();
  const marketCreateRes = await request(app).post("/admin/markets").send({
    name: `Window Markt ${Date.now()}`,
    dbName: "",
    address: "Window Testgasse 1",
    postalCode: "1010",
    city: "Wien",
    region: "Wien",
  });
  assert.equal(marketCreateRes.status, 201);
  const marketId = marketCreateRes.body.market?.id as string;
  const uniq = `window-${Date.now()}`;

  const first = await request(app).post("/admin/campaigns").send({
    name: `Standard A ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "scheduled",
    startDate: "2026-01-01",
    endDate: "2026-01-15",
    assignments: [{ marketId, gmUserId: gmUser.id }],
  });
  assert.equal(first.status, 201);
  const firstId = first.body.campaign?.id as string;

  const second = await request(app).post("/admin/campaigns").send({
    name: `Standard B ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "scheduled",
    startDate: "2026-02-01",
    endDate: "2026-02-15",
    assignments: [{ marketId, gmUserId: gmUser.id }],
  });
  assert.equal(second.status, 201);
  const secondId = second.body.campaign?.id as string;

  await request(app).patch(`/admin/campaigns/${firstId}/delete`).send({});
  await request(app).patch(`/admin/campaigns/${secondId}/delete`).send({});
  await request(app).patch(`/admin/markets/${marketId}/delete`).send({});
});

test("campaign migrate endpoint moves assignment and writes history", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests())) {
    t.skip("Campaign tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for migration test.");
    return;
  }
  const app = createApp();
  const marketCreateRes = await request(app).post("/admin/markets").send({
    name: `Migrate Markt ${Date.now()}`,
    dbName: "",
    address: "Migrate Testgasse 1",
    postalCode: "1010",
    city: "Wien",
    region: "Wien",
  });
  assert.equal(marketCreateRes.status, 201);
  const marketId = marketCreateRes.body.market?.id as string;
  const uniq = `migrate-${Date.now()}`;

  const source = await request(app).post("/admin/campaigns").send({
    name: `Source ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    assignments: [{ marketId, gmUserId: gmUser.id }],
  });
  assert.equal(source.status, 201);
  const sourceId = source.body.campaign?.id as string;

  const target = await request(app).post("/admin/campaigns").send({
    name: `Target ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    marketIds: [],
  });
  assert.equal(target.status, 201);
  const targetId = target.body.campaign?.id as string;

  const migrateRes = await request(app).post(`/admin/campaigns/${targetId}/markets/migrate`).send({
    moves: [{ marketId, fromCampaignId: sourceId, gmUserId: gmUser.id }],
  });
  assert.equal(migrateRes.status, 200);
  assert.equal(migrateRes.body.campaign?.marketIds?.includes(marketId), true);

  const [sourceAssignment] = await db
    .select({ isDeleted: campaignMarketAssignments.isDeleted })
    .from(campaignMarketAssignments)
    .where(and(eq(campaignMarketAssignments.campaignId, sourceId), eq(campaignMarketAssignments.marketId, marketId)))
    .limit(1);
  assert.equal(sourceAssignment?.isDeleted, true);

  const [targetAssignment] = await db
    .select({ isDeleted: campaignMarketAssignments.isDeleted })
    .from(campaignMarketAssignments)
    .where(and(eq(campaignMarketAssignments.campaignId, targetId), eq(campaignMarketAssignments.marketId, marketId)))
    .limit(1);
  assert.equal(targetAssignment?.isDeleted, false);

  const [historyRow] = await db
    .select({ fromCampaignId: campaignMarketAssignmentHistory.fromCampaignId, toCampaignId: campaignMarketAssignmentHistory.toCampaignId })
    .from(campaignMarketAssignmentHistory)
    .where(and(eq(campaignMarketAssignmentHistory.marketId, marketId), eq(campaignMarketAssignmentHistory.toCampaignId, targetId)))
    .limit(1);
  assert.equal(historyRow?.fromCampaignId, sourceId);
  assert.equal(historyRow?.toCampaignId, targetId);

  const staleMigrateRes = await request(app).post(`/admin/campaigns/${targetId}/markets/migrate`).send({
    moves: [{ marketId, fromCampaignId: sourceId }],
  });
  assert.equal(staleMigrateRes.status, 409);

  await request(app).patch(`/admin/campaigns/${sourceId}/delete`).send({});
  await request(app).patch(`/admin/campaigns/${targetId}/delete`).send({});
  await request(app).patch(`/admin/markets/${marketId}/delete`).send({});
});

test("markets list includes plannedByActiveStandardGmName from active standard assignment", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests())) {
    t.skip("Campaign tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id, firstName: users.firstName, lastName: users.lastName })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for plannedByActiveStandardGmName test.");
    return;
  }
  const expectedName = `${gmUser.firstName} ${gmUser.lastName}`.trim();
  const app = createApp();
  const marketCreateRes = await request(app).post("/admin/markets").send({
    name: `Verplant Markt ${Date.now()}`,
    dbName: "",
    address: "Verplant Testgasse 1",
    postalCode: "1010",
    city: "Wien",
    region: "Wien",
  });
  assert.equal(marketCreateRes.status, 201);
  const marketId = marketCreateRes.body.market?.id as string;

  const standardCampaign = await request(app).post("/admin/campaigns").send({
    name: `Verplant Standard ${Date.now()}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    assignments: [{ marketId, gmUserId: gmUser.id }],
  });
  assert.equal(standardCampaign.status, 201);
  const standardCampaignId = standardCampaign.body.campaign?.id as string;

  const marketsList = await request(app).get("/markets");
  assert.equal(marketsList.status, 200);
  const listed = (marketsList.body.markets ?? []).find((m: { id: string }) => m.id === marketId);
  assert.equal(listed?.plannedByActiveStandardGmName, expectedName);

  await request(app).patch(`/admin/campaigns/${standardCampaignId}/delete`).send({});
  await request(app).patch(`/admin/markets/${marketId}/delete`).send({});
});

test("campaign fragebogen switch appends history and updates current", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests()) || !(await ensureDbReadyForFragebogenTests())) {
    t.skip("Campaign or Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const uniq = `campaign-switch-${Date.now()}`;

  const fb1 = await request(app).post("/admin/fragebogen/main").send({
    name: `FB1 ${uniq}`,
    description: "",
    moduleIds: [],
    scheduleType: "always",
    status: "inactive",
    nurEinmalAusfuellbar: false,
    sectionKeywords: ["standard"],
    spezialfragen: [],
  });
  assert.equal(fb1.status, 201);
  const fb1Id = fb1.body.fragebogen?.id as string;

  const fb2 = await request(app).post("/admin/fragebogen/main").send({
    name: `FB2 ${uniq}`,
    description: "",
    moduleIds: [],
    scheduleType: "always",
    status: "inactive",
    nurEinmalAusfuellbar: false,
    sectionKeywords: ["standard"],
    spezialfragen: [],
  });
  assert.equal(fb2.status, 201);
  const fb2Id = fb2.body.fragebogen?.id as string;

  const campaignCreate = await request(app).post("/admin/campaigns").send({
    name: `Campaign Switch ${uniq}`,
    section: "standard",
    status: "inactive",
    scheduleType: "always",
    currentFragebogenId: fb1Id,
    marketIds: [],
  });
  assert.equal(campaignCreate.status, 201);
  const campaignId = campaignCreate.body.campaign?.id as string;

  const switchRes = await request(app).post(`/admin/campaigns/${campaignId}/fragebogen/switch`).send({ toFragebogenId: fb2Id });
  assert.equal(switchRes.status, 200);
  assert.equal(switchRes.body.campaign?.currentFragebogenId, fb2Id);
  assert.equal(switchRes.body.switched, true);

  const switchNoOpRes = await request(app).post(`/admin/campaigns/${campaignId}/fragebogen/switch`).send({ toFragebogenId: fb2Id });
  assert.equal(switchNoOpRes.status, 200);
  assert.equal(switchNoOpRes.body.switched, false);

  const switchBackRes = await request(app).post(`/admin/campaigns/${campaignId}/fragebogen/switch`).send({ toFragebogenId: fb1Id });
  assert.equal(switchBackRes.status, 200);
  assert.equal(switchBackRes.body.switched, true);

  const historyRows = await db
    .select()
    .from(campaignFragebogenHistory)
    .where(eq(campaignFragebogenHistory.campaignId, campaignId));
  assert.equal(historyRows.length >= 3, true);
  const orderedHistory = [...historyRows].sort((a, b) => a.changedAt.getTime() - b.changedAt.getTime());
  for (let i = 1; i < orderedHistory.length; i += 1) {
    assert.equal(orderedHistory[i]!.changedAt.getTime() >= orderedHistory[i - 1]!.changedAt.getTime(), true);
  }
  const switchedToFb2 = orderedHistory.find((row) => row.toFragebogenId === fb2Id);
  assert.ok(switchedToFb2);
  assert.equal(switchedToFb2?.fromFragebogenId, fb1Id);
  const switchedBackToFb1 = orderedHistory.reverse().find((row) => row.toFragebogenId === fb1Id && row.fromFragebogenId === fb2Id);
  assert.ok(switchedBackToFb1);

  await request(app).patch(`/admin/campaigns/${campaignId}/delete`).send({});
  await request(app).patch(`/admin/fragebogen/main/${fb1Id}/delete`).send({});
  await request(app).patch(`/admin/fragebogen/main/${fb2Id}/delete`).send({});
});

test("campaign section mismatch rejects fragebogen switch", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests()) || !(await ensureDbReadyForFragebogenTests())) {
    t.skip("Campaign or Fragebogen tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const uniq = `campaign-mismatch-${Date.now()}`;

  const mainFb = await request(app).post("/admin/fragebogen/main").send({
    name: `Main FB ${uniq}`,
    description: "",
    moduleIds: [],
    scheduleType: "always",
    status: "inactive",
    nurEinmalAusfuellbar: false,
    sectionKeywords: ["standard"],
    spezialfragen: [],
  });
  assert.equal(mainFb.status, 201);
  const mainFbId = mainFb.body.fragebogen?.id as string;

  const campaignCreate = await request(app).post("/admin/campaigns").send({
    name: `Campaign Kuehler ${uniq}`,
    section: "kuehler",
    status: "inactive",
    scheduleType: "always",
    marketIds: [],
  });
  assert.equal(campaignCreate.status, 201);
  const campaignId = campaignCreate.body.campaign?.id as string;

  const switchRes = await request(app).post(`/admin/campaigns/${campaignId}/fragebogen/switch`).send({ toFragebogenId: mainFbId });
  assert.equal(switchRes.status, 400);
  assert.equal(switchRes.body.code, "fragebogen_mismatch");

  await request(app).patch(`/admin/campaigns/${campaignId}/delete`).send({});
  await request(app).patch(`/admin/fragebogen/main/${mainFbId}/delete`).send({});
});

test("gm assigned-active returns active only, deduped, and respects soft-delete", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests())) {
    t.skip("Campaign tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for assignment test.");
    return;
  }
  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  const app = createApp();
  const marketCreateRes = await request(app).post("/admin/markets").send({
    name: `GM Assigned Markt ${Date.now()}`,
    dbName: "",
    address: "Hardening Testgasse 1",
    postalCode: "1010",
    city: "Wien",
    region: "Wien",
  });
  assert.equal(marketCreateRes.status, 201);
  const marketId = marketCreateRes.body.market?.id as string;

  const uniq = `gm-assigned-${Date.now()}`;
  const active1 = await request(app).post("/admin/campaigns").send({
    name: `Active A ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    assignments: [{ marketId, gmUserId: gmUser.id, gmNameRaw: "Test GM" }],
  });
  assert.equal(active1.status, 201);
  const active1Id = active1.body.campaign?.id as string;

  const active2 = await request(app).post("/admin/campaigns").send({
    name: `Active B ${uniq}`,
    section: "mhd",
    status: "active",
    scheduleType: "always",
    assignments: [{ marketId, gmUserId: gmUser.id, gmNameRaw: "Test GM" }],
  });
  assert.equal(active2.status, 201);
  const active2Id = active2.body.campaign?.id as string;

  const inactive = await request(app).post("/admin/campaigns").send({
    name: `Inactive ${uniq}`,
    section: "flex",
    status: "inactive",
    scheduleType: "always",
    assignments: [{ marketId, gmUserId: gmUser.id, gmNameRaw: "Test GM" }],
  });
  assert.equal(inactive.status, 201);
  const inactiveId = inactive.body.campaign?.id as string;

  const scheduledOutOfWindow = await request(app).post("/admin/campaigns").send({
    name: `Scheduled Future ${uniq}`,
    section: "billa",
    status: "active",
    scheduleType: "scheduled",
    startDate: "2099-01-01",
    endDate: "2099-01-31",
    assignments: [{ marketId, gmUserId: gmUser.id, gmNameRaw: "Test GM" }],
  });
  assert.equal(scheduledOutOfWindow.status, 201);
  const scheduledOutOfWindowId = scheduledOutOfWindow.body.campaign?.id as string;

  const listRes = await request(app).get("/markets/gm/assigned-active");
  assert.equal(listRes.status, 200);
  const matching = (listRes.body.markets ?? []).filter((m: { id: string }) => m.id === marketId);
  assert.equal(matching.length, 1);

  const removeActive1 = await request(app).patch(`/admin/campaigns/${active1Id}/markets/${marketId}/delete`).send({});
  assert.equal(removeActive1.status, 200);
  const listAfterOneRemove = await request(app).get("/markets/gm/assigned-active");
  const matchingAfterOneRemove = (listAfterOneRemove.body.markets ?? []).filter((m: { id: string }) => m.id === marketId);
  assert.equal(matchingAfterOneRemove.length, 1);

  const deleteActive2 = await request(app).patch(`/admin/campaigns/${active2Id}/delete`).send({});
  assert.equal(deleteActive2.status, 200);
  const listAfterActiveGone = await request(app).get("/markets/gm/assigned-active");
  const matchingAfterActiveGone = (listAfterActiveGone.body.markets ?? []).filter((m: { id: string }) => m.id === marketId);
  assert.equal(matchingAfterActiveGone.length, 0);

  await request(app).patch(`/admin/campaigns/${active1Id}/delete`).send({});
  await request(app).patch(`/admin/campaigns/${inactiveId}/delete`).send({});
  await request(app).patch(`/admin/campaigns/${scheduledOutOfWindowId}/delete`).send({});
  await request(app).patch(`/admin/markets/${marketId}/delete`).send({});
  delete process.env.BYPASS_AUTH_ROLE;
  delete process.env.BYPASS_AUTH_USER_ID;
});

test("gm assigned-active blocks sm role in contract guard", async (t) => {
  enableTestAuthBypass();
  setTestBypassRole("sm");
  if (!(await ensureDbReadyForCampaignTests())) {
    t.skip("Campaign tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const res = await request(app).get("/markets/gm/assigned-active");
  assert.equal(res.status, 403);
  assert.equal(typeof res.body.error, "string");
  delete process.env.BYPASS_AUTH_ROLE;
});

test("gm kuehler-mhd-progress returns assigned totals with submitted dedupe", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests()) || !(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Campaign or visit-session tables are not present in the configured DATABASE_URL.");
    return;
  }

  const app = createApp();
  const uniq = `kuehler-mhd-progress-${Date.now()}`;
  const gmUserId = randomUUID();
  const marketOneId = randomUUID();
  const marketTwoId = randomUUID();
  const kuehlerCampaignId = randomUUID();
  const mhdCampaignId = randomUUID();
  const oldKuehlerSessionId = randomUUID();
  const latestKuehlerSessionId = randomUUID();
  const mhdSessionId = randomUUID();
  const oldKuehlerSectionId = randomUUID();
  const latestKuehlerSectionId = randomUUID();
  const mhdSectionId = randomUUID();
  const now = new Date();
  const startDate = new Date(now);
  startDate.setDate(now.getDate() - 7);
  const endDate = new Date(now);
  endDate.setDate(now.getDate() + 7);
  const startYmd = startDate.toISOString().slice(0, 10);
  const endYmd = endDate.toISOString().slice(0, 10);

  try {
    await db.insert(users).values({
      id: gmUserId,
      supabaseAuthId: `sb-${uniq}`,
      email: `${uniq}@example.com`,
      role: "gm",
      firstName: "Progress",
      lastName: "Tester",
      region: "Wien",
      isActive: true,
    });

    await db.insert(markets).values([
      {
        id: marketOneId,
        name: `Progress Markt 1 ${uniq}`,
        dbName: "SPAR",
        address: "Progressgasse 1",
        postalCode: "1010",
        city: "Wien",
        region: "Wien",
      },
      {
        id: marketTwoId,
        name: `Progress Markt 2 ${uniq}`,
        dbName: "BILLA",
        address: "Progressgasse 2",
        postalCode: "1020",
        city: "Wien",
        region: "Wien",
      },
    ]);

    await db.insert(campaigns).values([
      {
        id: kuehlerCampaignId,
        name: `Kühler ${uniq}`,
        section: "kuehler",
        status: "active",
        scheduleType: "always",
      },
      {
        id: mhdCampaignId,
        name: `MHD ${uniq}`,
        section: "mhd",
        status: "active",
        scheduleType: "scheduled",
        startDate: startYmd,
        endDate: endYmd,
      },
    ]);

    await db.insert(campaignMarketAssignments).values([
      { campaignId: kuehlerCampaignId, marketId: marketOneId, gmUserId },
      { campaignId: kuehlerCampaignId, marketId: marketTwoId, gmUserId },
      { campaignId: mhdCampaignId, marketId: marketOneId, gmUserId },
    ]);

    await db.insert(visitSessions).values([
      {
        id: oldKuehlerSessionId,
        gmUserId,
        marketId: marketOneId,
        status: "submitted",
        startedAt: new Date(now.getTime() - 60 * 60 * 1000),
        submittedAt: new Date(now.getTime() - 50 * 60 * 1000),
      },
      {
        id: latestKuehlerSessionId,
        gmUserId,
        marketId: marketOneId,
        status: "submitted",
        startedAt: new Date(now.getTime() - 40 * 60 * 1000),
        submittedAt: new Date(now.getTime() - 30 * 60 * 1000),
      },
      {
        id: mhdSessionId,
        gmUserId,
        marketId: marketOneId,
        status: "submitted",
        startedAt: new Date(now.getTime() - 20 * 60 * 1000),
        submittedAt: new Date(now.getTime() - 10 * 60 * 1000),
      },
    ]);

    await db.insert(visitSessionSections).values([
      {
        id: oldKuehlerSectionId,
        visitSessionId: oldKuehlerSessionId,
        campaignId: kuehlerCampaignId,
        section: "kuehler",
        status: "submitted",
        fragebogenNameSnapshot: "Kühler FB",
      },
      {
        id: latestKuehlerSectionId,
        visitSessionId: latestKuehlerSessionId,
        campaignId: kuehlerCampaignId,
        section: "kuehler",
        status: "submitted",
        fragebogenNameSnapshot: "Kühler FB",
      },
      {
        id: mhdSectionId,
        visitSessionId: mhdSessionId,
        campaignId: mhdCampaignId,
        section: "mhd",
        status: "submitted",
        fragebogenNameSnapshot: "MHD FB",
      },
    ]);

    setTestBypassRole("gm");
    setTestBypassUserId(gmUserId);
    const res = await request(app).get("/markets/gm/kuehler-mhd-progress");
    assert.equal(res.status, 200);
    assert.equal(res.body.kuehler.total, 2);
    assert.equal(res.body.kuehler.current, 1);
    assert.equal(res.body.mhd.total, 1);
    assert.equal(res.body.mhd.current, 1);
    assert.equal(Array.isArray(res.body.kuehler.markets), true);
    assert.equal(Array.isArray(res.body.mhd.markets), true);
  } finally {
    await db.update(visitSessionSections).set({ isDeleted: true, deletedAt: new Date() }).where(
      inArray(visitSessionSections.id, [oldKuehlerSectionId, latestKuehlerSectionId, mhdSectionId]),
    );
    await db.update(visitSessions).set({ isDeleted: true, deletedAt: new Date(), status: "cancelled" }).where(
      inArray(visitSessions.id, [oldKuehlerSessionId, latestKuehlerSessionId, mhdSessionId]),
    );
    await db
      .update(campaignMarketAssignments)
      .set({ isDeleted: true, deletedAt: new Date() })
      .where(
        and(
          inArray(campaignMarketAssignments.campaignId, [kuehlerCampaignId, mhdCampaignId]),
          inArray(campaignMarketAssignments.marketId, [marketOneId, marketTwoId]),
        ),
      );
    await db
      .update(campaigns)
      .set({ isDeleted: true, deletedAt: new Date(), status: "inactive" })
      .where(inArray(campaigns.id, [kuehlerCampaignId, mhdCampaignId]));
    await db.update(markets).set({ isDeleted: true, deletedAt: new Date() }).where(inArray(markets.id, [marketOneId, marketTwoId]));
    await db.update(users).set({ isActive: false, deletedAt: new Date() }).where(eq(users.id, gmUserId));
    delete process.env.BYPASS_AUTH_ROLE;
    delete process.env.BYPASS_AUTH_USER_ID;
  }
});

test("admin campaign market-visits returns latest submitted replay with rule/chain visibility", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForCampaignTests()) || !(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Campaign or visit-session tables are not present in the configured DATABASE_URL.");
    return;
  }

  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for campaign market-visits test.");
    return;
  }

  const sharedQuestions = await db
    .select({ id: questionBankShared.id })
    .from(questionBankShared)
    .where(eq(questionBankShared.isDeleted, false))
    .limit(3);
  if (sharedQuestions.length === 0) {
    t.skip("No shared questions available for FK-safe campaign market-visits test.");
    return;
  }

  const now = Date.now();
  const uniq = `campaign-market-visits-${now}`;
  const campaignId = randomUUID();
  const oldSessionId = randomUUID();
  const latestSessionId = randomUUID();
  const oldSectionId = randomUUID();
  const latestSectionId = randomUUID();
  const triggerQuestionId = randomUUID();
  const ruleTargetQuestionId = randomUUID();
  const chainHiddenQuestionId = randomUUID();
  let marketId: string | null = null;

  try {
    const [marketRow] = await db
      .insert(markets)
      .values({
        name: `Campaign Visit Markt ${uniq}`,
        dbName: "spar",
        address: "Replay Straße 1",
        postalCode: "1010",
        city: "Wien",
        region: "Wien",
        isDeleted: false,
        createdAt: new Date(now - 1000),
        updatedAt: new Date(now - 1000),
      })
      .returning({ id: markets.id });
    marketId = marketRow?.id ?? null;
    if (!marketId) {
      t.skip("Could not create market for campaign market-visits test.");
      return;
    }

    await db.insert(campaigns).values({
      id: campaignId,
      name: `Campaign Visit ${uniq}`,
      section: "standard",
      status: "active",
      scheduleType: "always",
      isDeleted: false,
      deletedAt: null,
      createdAt: new Date(now - 2000),
      updatedAt: new Date(now - 2000),
    });
    await db.insert(campaignMarketAssignments).values({
      campaignId,
      marketId,
      gmUserId: gmUser.id,
      assignedAt: new Date(now - 1900),
      assignedByUserId: null,
      isDeleted: false,
      deletedAt: null,
      createdAt: new Date(now - 1900),
      updatedAt: new Date(now - 1900),
    });

    await db.insert(visitSessions).values([
      {
        id: oldSessionId,
        gmUserId: gmUser.id,
        marketId,
        status: "submitted",
        startedAt: new Date(now - 60 * 60 * 1000),
        submittedAt: new Date(now - 55 * 60 * 1000),
        lastSavedAt: new Date(now - 55 * 60 * 1000),
        isDeleted: false,
        deletedAt: null,
        createdAt: new Date(now - 60 * 60 * 1000),
        updatedAt: new Date(now - 55 * 60 * 1000),
      },
      {
        id: latestSessionId,
        gmUserId: gmUser.id,
        marketId,
        status: "submitted",
        startedAt: new Date(now - 30 * 60 * 1000),
        submittedAt: new Date(now - 20 * 60 * 1000),
        lastSavedAt: new Date(now - 20 * 60 * 1000),
        isDeleted: false,
        deletedAt: null,
        createdAt: new Date(now - 30 * 60 * 1000),
        updatedAt: new Date(now - 20 * 60 * 1000),
      },
    ]);

    await db.insert(visitSessionSections).values([
      {
        id: oldSectionId,
        visitSessionId: oldSessionId,
        campaignId,
        section: "standard",
        fragebogenId: null,
        fragebogenNameSnapshot: "Old Snapshot",
        orderIndex: 0,
        status: "submitted",
        isDeleted: false,
        deletedAt: null,
        createdAt: new Date(now - 60 * 60 * 1000),
        updatedAt: new Date(now - 55 * 60 * 1000),
      },
      {
        id: latestSectionId,
        visitSessionId: latestSessionId,
        campaignId,
        section: "standard",
        fragebogenId: null,
        fragebogenNameSnapshot: "Latest Snapshot",
        orderIndex: 0,
        status: "submitted",
        isDeleted: false,
        deletedAt: null,
        createdAt: new Date(now - 30 * 60 * 1000),
        updatedAt: new Date(now - 20 * 60 * 1000),
      },
    ]);

    await db.insert(visitSessionQuestions).values([
      {
        id: triggerQuestionId,
        visitSessionSectionId: latestSectionId,
        questionId: sharedQuestions[0]!.id,
        moduleId: null,
        moduleNameSnapshot: "Visibility",
        questionType: "yesno",
        questionTextSnapshot: "Trigger",
        questionConfigSnapshot: { options: ["Ja", "Nein"] },
        questionRulesSnapshot: [
          {
            triggerQuestionId,
            operator: "equals",
            triggerValue: "Ja",
            triggerValueMax: "",
            action: "hide",
            targetQuestionIds: [ruleTargetQuestionId],
          },
        ],
        questionChainsSnapshot: [],
        appliesToMarketChainSnapshot: true,
        requiredSnapshot: true,
        orderIndex: 0,
        isDeleted: false,
        deletedAt: null,
        createdAt: new Date(now - 30 * 60 * 1000),
        updatedAt: new Date(now - 20 * 60 * 1000),
      },
      {
        id: ruleTargetQuestionId,
        visitSessionSectionId: latestSectionId,
        questionId: (sharedQuestions[1]?.id ?? sharedQuestions[0]!.id),
        moduleId: null,
        moduleNameSnapshot: "Visibility",
        questionType: "text",
        questionTextSnapshot: "Rule Target",
        questionConfigSnapshot: {},
        questionRulesSnapshot: [],
        questionChainsSnapshot: [],
        appliesToMarketChainSnapshot: true,
        requiredSnapshot: false,
        orderIndex: 1,
        isDeleted: false,
        deletedAt: null,
        createdAt: new Date(now - 30 * 60 * 1000),
        updatedAt: new Date(now - 20 * 60 * 1000),
      },
      {
        id: chainHiddenQuestionId,
        visitSessionSectionId: latestSectionId,
        questionId: (sharedQuestions[2]?.id ?? sharedQuestions[0]!.id),
        moduleId: null,
        moduleNameSnapshot: "Visibility",
        questionType: "text",
        questionTextSnapshot: "Chain Hidden",
        questionConfigSnapshot: {},
        questionRulesSnapshot: [],
        questionChainsSnapshot: ["billa"],
        appliesToMarketChainSnapshot: false,
        requiredSnapshot: true,
        orderIndex: 2,
        isDeleted: false,
        deletedAt: null,
        createdAt: new Date(now - 30 * 60 * 1000),
        updatedAt: new Date(now - 20 * 60 * 1000),
      },
    ]);

    const [triggerAnswer] = await db
      .insert(visitAnswers)
      .values({
        visitSessionId: latestSessionId,
        visitSessionSectionId: latestSectionId,
        visitSessionQuestionId: triggerQuestionId,
        questionId: sharedQuestions[0]!.id,
        questionType: "yesno",
        answerStatus: "answered",
        valueText: "Ja",
        valueJson: { raw: "Ja" },
        isValid: true,
        validationError: null,
        answeredAt: new Date(now - 21 * 60 * 1000),
        changedAt: new Date(now - 21 * 60 * 1000),
        version: 1,
        isDeleted: false,
        deletedAt: null,
        createdAt: new Date(now - 21 * 60 * 1000),
        updatedAt: new Date(now - 21 * 60 * 1000),
      })
      .returning({ id: visitAnswers.id });
    assert.ok(triggerAnswer?.id);

    await db.insert(visitAnswers).values({
      visitSessionId: latestSessionId,
      visitSessionSectionId: latestSectionId,
      visitSessionQuestionId: ruleTargetQuestionId,
      questionId: (sharedQuestions[1]?.id ?? sharedQuestions[0]!.id),
      questionType: "text",
      answerStatus: "answered",
      valueText: "Soll ausgeblendet sein",
      valueJson: { raw: "Soll ausgeblendet sein" },
      isValid: true,
      validationError: null,
      answeredAt: new Date(now - 20 * 60 * 1000),
      changedAt: new Date(now - 20 * 60 * 1000),
      version: 1,
      isDeleted: false,
      deletedAt: null,
      createdAt: new Date(now - 20 * 60 * 1000),
      updatedAt: new Date(now - 20 * 60 * 1000),
    });

    // Old session question+answer to verify latest-session selection.
    const oldQuestionId = randomUUID();
    await db.insert(visitSessionQuestions).values({
      id: oldQuestionId,
      visitSessionSectionId: oldSectionId,
      questionId: sharedQuestions[0]!.id,
      moduleId: null,
      moduleNameSnapshot: "Old",
      questionType: "text",
      questionTextSnapshot: "Old text",
      questionConfigSnapshot: {},
      questionRulesSnapshot: [],
      questionChainsSnapshot: [],
      appliesToMarketChainSnapshot: true,
      requiredSnapshot: false,
      orderIndex: 0,
      isDeleted: false,
      deletedAt: null,
      createdAt: new Date(now - 60 * 60 * 1000),
      updatedAt: new Date(now - 55 * 60 * 1000),
    });
    await db.insert(visitAnswers).values({
      visitSessionId: oldSessionId,
      visitSessionSectionId: oldSectionId,
      visitSessionQuestionId: oldQuestionId,
      questionId: sharedQuestions[0]!.id,
      questionType: "text",
      answerStatus: "answered",
      valueText: "Old value",
      valueJson: { raw: "Old value" },
      isValid: true,
      validationError: null,
      answeredAt: new Date(now - 55 * 60 * 1000),
      changedAt: new Date(now - 55 * 60 * 1000),
      version: 1,
      isDeleted: false,
      deletedAt: null,
      createdAt: new Date(now - 55 * 60 * 1000),
      updatedAt: new Date(now - 55 * 60 * 1000),
    });

    setTestBypassRole("admin");
    const app = createApp();
    const res = await request(app).get(`/admin/campaigns/${campaignId}/market-visits`);
    assert.equal(res.status, 200);
    assert.equal(res.body.campaignId, campaignId);
    const marketRows = (res.body.markets ?? []) as Array<Record<string, unknown>>;
    const row = marketRows.find((entry) => entry.marketId === marketId);
    assert.ok(row);
    assert.equal(row?.hasSubmittedVisit, true);
    assert.equal(row?.sessionId, latestSessionId);
    assert.equal(typeof row?.durationMinutes, "number");

    const sectionsPayload = (row?.sections ?? []) as Array<Record<string, unknown>>;
    assert.equal(sectionsPayload.length, 1);
    const questionsPayload = (sectionsPayload[0]?.questions ?? []) as Array<Record<string, unknown>>;
    const trigger = questionsPayload.find((entry) => entry.id === triggerQuestionId);
    const ruleTarget = questionsPayload.find((entry) => entry.id === ruleTargetQuestionId);
    const chainHidden = questionsPayload.find((entry) => entry.id === chainHiddenQuestionId);
    assert.equal(trigger?.visibility && (trigger.visibility as Record<string, unknown>).isVisibleAtSubmit, true);
    assert.equal(ruleTarget?.visibility && (ruleTarget.visibility as Record<string, unknown>).isHiddenByRule, true);
    assert.equal(ruleTarget?.visibility && (ruleTarget.visibility as Record<string, unknown>).isVisibleAtSubmit, false);
    assert.equal(chainHidden?.visibility && (chainHidden.visibility as Record<string, unknown>).isHiddenByChain, true);
    assert.equal(chainHidden?.visibility && (chainHidden.visibility as Record<string, unknown>).isHiddenByRule, false);
  } finally {
    delete process.env.BYPASS_AUTH_ROLE;
    if (marketId) {
      await db
        .update(visitSessions)
        .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
        .where(inArray(visitSessions.id, [oldSessionId, latestSessionId]));
      await db
        .update(campaignMarketAssignments)
        .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
        .where(and(eq(campaignMarketAssignments.campaignId, campaignId), eq(campaignMarketAssignments.marketId, marketId)));
      await db
        .update(campaigns)
        .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
        .where(eq(campaigns.id, campaignId));
      await db
        .update(markets)
        .set({ isDeleted: true, updatedAt: new Date() })
        .where(eq(markets.id, marketId));
    }
  }
});

test("gm visit sessions block sm role and require gm/admin contract", async (t) => {
  enableTestAuthBypass();
  setTestBypassRole("sm");
  if (!(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Visit session tables are not present in the configured DATABASE_URL.");
    return;
  }
  const app = createApp();
  const res = await request(app).post("/markets/gm/visit-sessions").send({
    marketId: randomUUID(),
    campaignIds: [randomUUID()],
  });
  assert.ok([403, 404].includes(res.status));
  delete process.env.BYPASS_AUTH_ROLE;
});

test("gm visit answer validation handles matrix/photo and submit gating", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Visit session tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for visit-session test.");
    return;
  }

  const now = new Date();
  const uniq = `visit-test-${Date.now()}`;
  let marketId: string | null = null;
  const campaignId = randomUUID();
  const sessionId = randomUUID();
  const sectionId = randomUUID();
  const matrixQuestionId = randomUUID();
  const photoQuestionId = randomUUID();
  const requiredQuestionId = randomUUID();
  const yesNoQuestionId = randomUUID();
  const numericQuestionId = randomUUID();
  const photoTagId = randomUUID();
  const sharedQuestions = await db
    .select({ id: questionBankShared.id })
    .from(questionBankShared)
    .where(and(eq(questionBankShared.isDeleted, false)))
    .limit(3);
  if (sharedQuestions.length === 0) {
    t.skip("No shared questions available for FK-safe visit-session test.");
    return;
  }
  const sharedIdA = sharedQuestions[0]?.id ?? randomUUID();
  const sharedIdB = sharedQuestions[1]?.id ?? sharedIdA;
  const sharedIdC = sharedQuestions[2]?.id ?? sharedIdA;

  const [marketRow] = await db
    .insert(markets)
    .values({
      name: `Visit Markt ${uniq}`,
      dbName: "",
      address: "Visit Session Straße 1",
      postalCode: "1010",
      city: "Wien",
      region: "Wien",
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    })
    .returning({ id: markets.id });
  marketId = marketRow?.id ?? null;
  if (!marketId) {
    t.skip("Could not create market row for visit-session test.");
    return;
  }
  await db.insert(campaigns).values({
    id: campaignId,
    name: `Visit Campaign ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(photoTags).values({
    id: photoTagId,
    label: `VisitTag-${uniq}`,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessions).values({
    id: sessionId,
    gmUserId: gmUser.id,
    marketId,
    status: "draft",
    startedAt: now,
    lastSavedAt: now,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionSections).values({
    id: sectionId,
    visitSessionId: sessionId,
    campaignId,
    section: "standard",
    fragebogenId: null,
    fragebogenNameSnapshot: "Visit Fragebogen",
    orderIndex: 0,
    status: "draft",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionQuestions).values([
    {
      id: matrixQuestionId,
      visitSessionSectionId: sectionId,
      questionId: sharedIdA,
      moduleId: null,
      moduleNameSnapshot: "Module 1",
      questionType: "matrix",
      questionTextSnapshot: "Datum Matrix",
      questionConfigSnapshot: { matrixSubtype: "datum" },
      questionRulesSnapshot: [],
      requiredSnapshot: false,
      orderIndex: 0,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: yesNoQuestionId,
      visitSessionSectionId: sectionId,
      questionId: sharedIdA,
      moduleId: null,
      moduleNameSnapshot: "Module 1",
      questionType: "yesno",
      questionTextSnapshot: "Custom Yes/No",
      questionConfigSnapshot: { options: ["OK", "NOK"] },
      questionRulesSnapshot: [
        {
          triggerQuestionId: sharedIdA,
          operator: "equals",
          triggerValue: "OK",
          triggerValueMax: "",
          action: "hide",
          targetQuestionIds: [sharedIdB],
        },
      ],
      requiredSnapshot: false,
      orderIndex: 1,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: numericQuestionId,
      visitSessionSectionId: sectionId,
      questionId: sharedIdB,
      moduleId: null,
      moduleNameSnapshot: "Module 1",
      questionType: "numeric",
      questionTextSnapshot: "Numeric with negatives",
      questionConfigSnapshot: { min: -10, max: 10, decimals: true },
      questionRulesSnapshot: [],
      requiredSnapshot: false,
      orderIndex: 2,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: photoQuestionId,
      visitSessionSectionId: sectionId,
      questionId: sharedIdC,
      moduleId: null,
      moduleNameSnapshot: "Module 1",
      questionType: "photo",
      questionTextSnapshot: "Photo with tags",
      questionConfigSnapshot: { tagsEnabled: true, tagIds: [photoTagId] },
      questionRulesSnapshot: [],
      requiredSnapshot: true,
      orderIndex: 3,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: requiredQuestionId,
      visitSessionSectionId: sectionId,
      questionId: sharedIdB,
      moduleId: null,
      moduleNameSnapshot: "Module 2",
      questionType: "text",
      questionTextSnapshot: "Pflichtfrage",
      questionConfigSnapshot: {},
      questionRulesSnapshot: [],
      requiredSnapshot: true,
      orderIndex: 4,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
  ]);

  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  const app = createApp();

  const invalidMatrix = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: matrixQuestionId,
    answer: JSON.stringify({ "row-1::col-1": "31-02-2026" }),
  });
  assert.equal(invalidMatrix.status, 200);
  assert.equal(invalidMatrix.body.result?.isValid, false);

  const validMatrix = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: matrixQuestionId,
    answer: JSON.stringify({ "row-1::col-1": "2026-02-28" }),
  });
  assert.equal(validMatrix.status, 200);
  assert.equal(validMatrix.body.result?.isValid, true);
  assert.equal(validMatrix.body.result?.answerStatus, "answered");

  const invalidYesNo = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: yesNoQuestionId,
    answer: "Ja",
  });
  assert.equal(invalidYesNo.status, 200);
  assert.equal(invalidYesNo.body.result?.isValid, false);

  const validYesNo = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: yesNoQuestionId,
    answer: "OK",
  });
  assert.equal(validYesNo.status, 200);
  assert.equal(validYesNo.body.result?.isValid, true);
  assert.equal(validYesNo.body.result?.answerStatus, "answered");

  const validNegativeNumeric = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: numericQuestionId,
    answer: "-3.5",
  });
  assert.equal(validNegativeNumeric.status, 200);
  assert.equal(validNegativeNumeric.body.result?.isValid, true);
  assert.equal(validNegativeNumeric.body.result?.answerStatus, "answered");

  const invalidNegativeNumeric = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: numericQuestionId,
    answer: "-11",
  });
  assert.equal(invalidNegativeNumeric.status, 200);
  assert.equal(invalidNegativeNumeric.body.result?.isValid, false);

  const invalidPhoto = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: photoQuestionId,
    answer: JSON.stringify({ photos: ["data:image/png;base64,AAA"], selectedTagIds: [] }),
  });
  assert.equal(invalidPhoto.status, 200);
  assert.equal(invalidPhoto.body.result?.isValid, true);
  assert.equal(invalidPhoto.body.result?.answerStatus, "unanswered");

  const submitBlocked = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/submit`).send({});
  assert.equal(submitBlocked.status, 409);
  assert.equal(submitBlocked.body.code, "visit_submit_incomplete_required");
  assert.ok(Array.isArray(submitBlocked.body.missingRequired));
  const missingVisitQuestionIds = (submitBlocked.body.missingRequired as Array<{ visitQuestionId?: unknown }>)
    .map((entry) => (typeof entry.visitQuestionId === "string" ? entry.visitQuestionId : ""));
  assert.ok(missingVisitQuestionIds.includes(photoQuestionId));
  assert.equal(missingVisitQuestionIds.includes(requiredQuestionId), false);

  const saveRequired = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: requiredQuestionId,
    answer: "Erledigt",
  });
  assert.equal(saveRequired.status, 200);
  assert.equal(saveRequired.body.result?.isValid, true);

  const [photoAnswerRow] = await db
    .select({ id: visitAnswers.id })
    .from(visitAnswers)
    .where(and(eq(visitAnswers.visitSessionQuestionId, photoQuestionId), eq(visitAnswers.isDeleted, false)))
    .limit(1);
  assert.ok(photoAnswerRow?.id);

  const invalidCommitPath = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/photos/commit`).send({
    visitAnswerId: photoAnswerRow.id,
    photos: [
      {
        storageBucket: "visit-photos",
        storagePath: `invalid-prefix/${photoAnswerRow.id}/x.jpg`,
        mimeType: "image/jpeg",
        byteSize: 12,
        photoTagIds: [photoTagId],
      },
    ],
  });
  assert.equal(invalidCommitPath.status, 400);

  const validCommit = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/photos/commit`).send({
    visitAnswerId: photoAnswerRow.id,
    photos: [
      {
        storageBucket: "visit-photos",
        storagePath: `visit-photos/${sessionId}/${photoAnswerRow.id}/x.jpg`,
        mimeType: "image/jpeg",
        byteSize: 12,
        photoTagIds: [photoTagId],
      },
    ],
  });
  assert.equal(validCommit.status, 200);

  const submitOk = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/submit`).send({});
  assert.equal(submitOk.status, 200);
  assert.equal(submitOk.body.status, "submitted");

  const saveAfterSubmit = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: requiredQuestionId,
    answer: "Nach Submit",
  });
  assert.equal(saveAfterSubmit.status, 409);

  await db.update(visitSessions).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(visitSessions.id, sessionId));
  await db.update(campaigns).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(campaigns.id, campaignId));
  await db.update(photoTags).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(photoTags.id, photoTagId));
  await db.update(markets).set({ isDeleted: true, updatedAt: new Date() }).where(eq(markets.id, marketId));
  delete process.env.BYPASS_AUTH_ROLE;
  delete process.env.BYPASS_AUTH_USER_ID;
});

async function seedPraemienSourceFixture(uniq: string) {
  const now = new Date();
  const fragebogenId = randomUUID();
  const moduleId = randomUUID();
  const questionWithBoniId = randomUUID();
  const questionWithoutBoniId = randomUUID();

  await db.insert(questionBankShared).values([
    {
      id: questionWithBoniId,
      questionType: "yesno",
      text: `Praemien Boni Frage ${uniq}`,
      required: true,
      config: { options: ["Ja", "Nein"] },
      rules: [],
      scoring: {},
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: questionWithoutBoniId,
      questionType: "yesno",
      text: `Praemien Kein Boni Frage ${uniq}`,
      required: true,
      config: { options: ["Ja", "Nein"] },
      rules: [],
      scoring: {},
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    },
  ]);
  await db.insert(questionScoring).values([
    {
      questionId: questionWithBoniId,
      scoreKey: "Ja",
      ipp: "0.00",
      boni: "3.50",
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    },
    {
      questionId: questionWithoutBoniId,
      scoreKey: "Ja",
      ipp: "1.00",
      boni: null,
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    },
  ]);
  await db.insert(moduleMain).values({
    id: moduleId,
    name: `Praemien Modul ${uniq}`,
    description: "",
    sectionKeywords: ["standard", "flex"],
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(moduleMainQuestion).values([
    {
      moduleId,
      questionId: questionWithBoniId,
      orderIndex: 0,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
    {
      moduleId,
      questionId: questionWithoutBoniId,
      orderIndex: 1,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
  ]);
  await db.insert(fragebogenMain).values({
    id: fragebogenId,
    name: `Praemien Fragebogen ${uniq}`,
    description: "",
    sectionKeywords: ["standard", "flex"],
    nurEinmalAusfuellbar: false,
    status: "active",
    scheduleType: "always",
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(fragebogenMainModule).values({
    fragebogenId,
    moduleId,
    orderIndex: 0,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });

  return { fragebogenId, moduleId, questionWithBoniId, questionWithoutBoniId };
}

async function cleanupPraemienSourceFixture(input: {
  fragebogenId: string;
  moduleId: string;
  questionWithBoniId: string;
  questionWithoutBoniId: string;
}) {
  const now = new Date();
  await db.update(fragebogenMainModule).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(fragebogenMainModule.fragebogenId, input.fragebogenId));
  await db.update(fragebogenMain).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(fragebogenMain.id, input.fragebogenId));
  await db.update(moduleMainQuestion).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(moduleMainQuestion.moduleId, input.moduleId));
  await db.update(moduleMain).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(moduleMain.id, input.moduleId));
  await db
    .update(questionScoring)
    .set({ isDeleted: true, deletedAt: now, updatedAt: now })
    .where(inArray(questionScoring.questionId, [input.questionWithBoniId, input.questionWithoutBoniId]));
  await db
    .update(questionBankShared)
    .set({ isDeleted: true, deletedAt: now, updatedAt: now })
    .where(inArray(questionBankShared.id, [input.questionWithBoniId, input.questionWithoutBoniId]));
}

test("admin praemien create wave persists nested thresholds and pillars", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();
  setTestBypassRole("admin");

  const app = createApp();
  const uniq = randomUUID().slice(0, 8);
  const res = await request(app).post("/admin/praemien/waves").send({
    name: `Q2 ${uniq}`,
    year: 2026,
    quarter: 2,
    startDate: "2026-04-01",
    endDate: "2026-06-30",
    thresholds: [
      { label: "Kein Bonus", orderIndex: 0, minPoints: 0, rewardEur: 0 },
      { label: "Bronze", orderIndex: 1, minPoints: 10, rewardEur: 100 },
    ],
    pillars: [
      { name: "Distributionsziel", orderIndex: 0, description: "", color: "#123456" },
      { name: "Qualitaetsziele", orderIndex: 1, description: "", color: "#654321", isManual: true },
    ],
  });
  assert.equal(res.status, 201);
  assert.equal(res.body.wave.thresholds.length, 2);
  assert.equal(res.body.wave.pillars.length, 2);
  assert.equal(Array.isArray(res.body.wave.qualitySubmissions), true);

  const waveId = String(res.body.wave.id);
  await db.update(praemienWaves).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(praemienWaves.id, waveId));
  delete process.env.BYPASS_AUTH_ROLE;
});

test("admin praemien source catalog includes only boni-enabled scores", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();
  setTestBypassRole("admin");

  const uniq = randomUUID().slice(0, 8);
  const fixture = await seedPraemienSourceFixture(uniq);
  try {
    const app = createApp();
    const res = await request(app).get("/admin/praemien/sources");
    assert.equal(res.status, 200);
    const rows = Array.isArray(res.body.sources) ? res.body.sources : [];
    const boniRows = rows.filter((row: { questionId?: string; boniValue?: number }) => row.questionId === fixture.questionWithBoniId);
    const noBoniRows = rows.filter((row: { questionId?: string }) => row.questionId === fixture.questionWithoutBoniId);
    assert.ok(boniRows.length >= 1);
    assert.equal(noBoniRows.length, 0);
  } finally {
    await cleanupPraemienSourceFixture(fixture);
    delete process.env.BYPASS_AUTH_ROLE;
  }
});

test("admin praemien source validation rejects duplicates and invalid distribution target", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();
  setTestBypassRole("admin");

  const uniq = randomUUID().slice(0, 8);
  const fixture = await seedPraemienSourceFixture(uniq);
  let waveId: string | null = null;
  try {
    const app = createApp();
    const createRes = await request(app).post("/admin/praemien/waves").send({
      name: `Q3 ${uniq}`,
      year: 2026,
      quarter: 3,
      startDate: "2026-07-01",
      endDate: "2026-09-30",
      pillars: [{ name: "Execution", orderIndex: 0, color: "#111111" }],
    });
    assert.equal(createRes.status, 201);
    waveId = String(createRes.body.wave.id);
    const pillarId = String(createRes.body.wave.pillars[0].id);

    const sourceRes = await request(app).get("/admin/praemien/sources");
    const src = sourceRes.body.sources.find((row: { questionId?: string }) => row.questionId === fixture.questionWithBoniId);
    assert.ok(src);

    const duplicateRes = await request(app)
      .put(`/admin/praemien/waves/${waveId}/sources`)
      .send({
        sources: [
          { ...src, pillarId },
          { ...src, pillarId },
        ],
      });
    assert.equal(duplicateRes.status, 400);

    const invalidDistributionRes = await request(app)
      .put(`/admin/praemien/waves/${waveId}/sources`)
      .send({
        sources: [{ ...src, pillarId, distributionFreqRule: "lt8" }],
      });
    assert.equal(invalidDistributionRes.status, 400);
  } finally {
    if (waveId) {
      await db.update(praemienWaves).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(praemienWaves.id, waveId));
    }
    await cleanupPraemienSourceFixture(fixture);
    delete process.env.BYPASS_AUTH_ROLE;
  }
});

test("admin praemien quality score upsert computes total points and validates bounds", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();
  setTestBypassRole("admin");

  const app = createApp();
  const gmUserId = randomUUID();
  const now = new Date();
  await db.insert(users).values({
    id: gmUserId,
    supabaseAuthId: `praemien-gm-${gmUserId}`,
    email: `praemien-gm-${gmUserId}@example.com`,
    role: "gm",
    firstName: "Praemien",
    lastName: "GM",
    isActive: true,
    createdAt: now,
    updatedAt: now,
  });

  const createRes = await request(app).post("/admin/praemien/waves").send({
    name: `Q4 ${randomUUID().slice(0, 8)}`,
    year: 2026,
    quarter: 4,
    startDate: "2026-10-01",
    endDate: "2026-12-31",
    pillars: [{ name: "Qualitaetsziele", orderIndex: 0, color: "#000000", isManual: true }],
  });
  assert.equal(createRes.status, 201);
  const waveId = String(createRes.body.wave.id);

  const upsertRes = await request(app)
    .put(`/admin/praemien/waves/${waveId}/quality-scores`)
    .send({
      qualityScores: [{ gmUserId, zeiterfassung: 90, reporting: 60, accuracy: 30, note: "ok" }],
    });
  assert.equal(upsertRes.status, 200);
  assert.equal(upsertRes.body.wave.qualitySubmissions[0].totalPoints, 60);

  const invalidRes = await request(app)
    .put(`/admin/praemien/waves/${waveId}/quality-scores`)
    .send({
      qualityScores: [{ gmUserId, zeiterfassung: 120, reporting: 60, accuracy: 30 }],
    });
  assert.equal(invalidRes.status, 400);

  await db.update(praemienWaves).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(praemienWaves.id, waveId));
  await db.update(users).set({ deletedAt: new Date(), updatedAt: new Date() }).where(eq(users.id, gmUserId));
  delete process.env.BYPASS_AUTH_ROLE;
});

test("admin praemien replace endpoints soft-delete previous rows", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();
  setTestBypassRole("admin");

  const uniq = randomUUID().slice(0, 8);
  const fixture = await seedPraemienSourceFixture(uniq);
  let waveId: string | null = null;
  try {
    const app = createApp();
    const createRes = await request(app).post("/admin/praemien/waves").send({
      name: `Q1 ${uniq}`,
      year: 2027,
      quarter: 1,
      startDate: "2027-01-01",
      endDate: "2027-03-31",
      thresholds: [
        { label: "Kein Bonus", orderIndex: 0, minPoints: 0, rewardEur: 0 },
        { label: "Bronze", orderIndex: 1, minPoints: 5, rewardEur: 50 },
      ],
      pillars: [{ name: "Distributionsziel", orderIndex: 0, color: "#222222" }],
    });
    assert.equal(createRes.status, 201);
    waveId = String(createRes.body.wave.id);
    const pillarId = String(createRes.body.wave.pillars[0].id);

    const sourceRes = await request(app).get("/admin/praemien/sources");
    const src = sourceRes.body.sources.find((row: { questionId?: string }) => row.questionId === fixture.questionWithBoniId);
    assert.ok(src);
    assert.equal(typeof src.scoringKey, "string");
    const putSourceOne = await request(app)
      .put(`/admin/praemien/waves/${waveId}/sources`)
      .send({ sources: [{ ...src, pillarId }] });
    assert.equal(putSourceOne.status, 200);

    const putThresholds = await request(app)
      .put(`/admin/praemien/waves/${waveId}/thresholds`)
      .send({
        thresholds: [
          { label: "Kein Bonus", orderIndex: 0, minPoints: 0, rewardEur: 0 },
          { label: "Silver", orderIndex: 1, minPoints: 20, rewardEur: 200 },
        ],
      });
    assert.equal(putThresholds.status, 200);

    const clearSources = await request(app).put(`/admin/praemien/waves/${waveId}/sources`).send({ sources: [] });
    assert.equal(clearSources.status, 200);

    const thresholdRows = await db.select().from(praemienWaveThresholds).where(eq(praemienWaveThresholds.waveId, waveId));
    const activeThresholds = thresholdRows.filter((row) => !row.isDeleted);
    const deletedThresholds = thresholdRows.filter((row) => row.isDeleted);
    assert.equal(activeThresholds.length, 2);
    assert.ok(deletedThresholds.length >= 2);

    const sourceRows = await db.select().from(praemienWaveSources).where(eq(praemienWaveSources.waveId, waveId));
    assert.equal(sourceRows.filter((row) => !row.isDeleted).length, 0);
    assert.ok(sourceRows.filter((row) => row.isDeleted).length >= 1);
  } finally {
    if (waveId) {
      await db.update(praemienWaves).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(praemienWaves.id, waveId));
    }
    await cleanupPraemienSourceFixture(fixture);
    delete process.env.BYPASS_AUTH_ROLE;
  }
});

test("admin praemien replace rejects stale writes via expectedUpdatedAt", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();
  setTestBypassRole("admin");

  const app = createApp();
  const createRes = await request(app).post("/admin/praemien/waves").send({
    name: `Q2 stale-${randomUUID().slice(0, 6)}`,
    year: 2028,
    quarter: 2,
    startDate: "2028-04-01",
    endDate: "2028-06-30",
    thresholds: [
      { label: "Kein Bonus", orderIndex: 0, minPoints: 0, rewardEur: 0 },
      { label: "Bronze", orderIndex: 1, minPoints: 10, rewardEur: 100 },
    ],
    pillars: [{ name: "Execution", orderIndex: 0, color: "#222222" }],
  });
  assert.equal(createRes.status, 201);
  const waveId = String(createRes.body.wave.id);
  const staleTs = String(createRes.body.wave.updatedAt);

  const firstReplace = await request(app).put(`/admin/praemien/waves/${waveId}/thresholds`).send({
    expectedUpdatedAt: staleTs,
    thresholds: [
      { label: "Kein Bonus", orderIndex: 0, minPoints: 0, rewardEur: 0 },
      { label: "Silber", orderIndex: 1, minPoints: 15, rewardEur: 150 },
    ],
  });
  assert.equal(firstReplace.status, 200);

  const staleReplace = await request(app).put(`/admin/praemien/waves/${waveId}/thresholds`).send({
    expectedUpdatedAt: staleTs,
    thresholds: [
      { label: "Kein Bonus", orderIndex: 0, minPoints: 0, rewardEur: 0 },
      { label: "Gold", orderIndex: 1, minPoints: 20, rewardEur: 250 },
    ],
  });
  assert.equal(staleReplace.status, 409);
  assert.equal(staleReplace.body.code, "wave_stale_write");

  await db.update(praemienWaves).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(praemienWaves.id, waveId));
  delete process.env.BYPASS_AUTH_ROLE;
});

test("admin praemien allows draft duplicates but blocks active duplicate period", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();
  setTestBypassRole("admin");
  const app = createApp();
  const createdWaveIds: string[] = [];
  try {
    const activeOne = await request(app).post("/admin/praemien/waves").send({
      name: `Active A-${randomUUID().slice(0, 6)}`,
      year: 2029,
      quarter: 1,
      status: "active",
      startDate: "2029-01-01",
      endDate: "2029-03-31",
      pillars: [{ name: "Execution", orderIndex: 0, color: "#000000" }],
    });
    assert.equal(activeOne.status, 201);
    createdWaveIds.push(String(activeOne.body.wave.id));

    const activeTwo = await request(app).post("/admin/praemien/waves").send({
      name: `Active B-${randomUUID().slice(0, 6)}`,
      year: 2029,
      quarter: 1,
      status: "active",
      startDate: "2029-01-01",
      endDate: "2029-03-31",
      pillars: [{ name: "Execution", orderIndex: 0, color: "#111111" }],
    });
    assert.equal(activeTwo.status, 409);
    assert.equal(activeTwo.body.code, "wave_active_conflict");

    const draftWave = await request(app).post("/admin/praemien/waves").send({
      name: `Draft-${randomUUID().slice(0, 6)}`,
      year: 2029,
      quarter: 1,
      status: "draft",
      startDate: "2029-01-01",
      endDate: "2029-03-31",
      pillars: [{ name: "Execution", orderIndex: 0, color: "#222222" }],
    });
    assert.equal(draftWave.status, 201);
    createdWaveIds.push(String(draftWave.body.wave.id));
  } finally {
    if (createdWaveIds.length > 0) {
      await db.update(praemienWaves).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(inArray(praemienWaves.id, createdWaveIds));
    }
    delete process.env.BYPASS_AUTH_ROLE;
  }
});

test("admin praemien keeps boni snapshot stable after scoring changes", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();
  setTestBypassRole("admin");

  const uniq = randomUUID().slice(0, 8);
  const fixture = await seedPraemienSourceFixture(uniq);
  let waveId: string | null = null;
  try {
    const app = createApp();
    const createRes = await request(app).post("/admin/praemien/waves").send({
      name: `Q3 snapshot-${uniq}`,
      year: 2030,
      quarter: 3,
      startDate: "2030-07-01",
      endDate: "2030-09-30",
      pillars: [{ name: "Execution", orderIndex: 0, color: "#333333" }],
    });
    assert.equal(createRes.status, 201);
    waveId = String(createRes.body.wave.id);
    const pillarId = String(createRes.body.wave.pillars[0].id);

    const catalogRes = await request(app).get("/admin/praemien/sources");
    const src = catalogRes.body.sources.find((row: { questionId?: string }) => row.questionId === fixture.questionWithBoniId);
    assert.ok(src);

    const assignRes = await request(app).put(`/admin/praemien/waves/${waveId}/sources`).send({
      sources: [{ ...src, pillarId }],
    });
    assert.equal(assignRes.status, 200);
    const originalBoni = Number(assignRes.body.wave.pillars[0].sourceRefs[0].boniValue);

    await db
      .update(questionScoring)
      .set({ boni: "9.99", updatedAt: new Date() })
      .where(and(eq(questionScoring.questionId, fixture.questionWithBoniId), eq(questionScoring.scoreKey, "Ja"), eq(questionScoring.isDeleted, false)));

    const detailRes = await request(app).get(`/admin/praemien/waves/${waveId}`);
    assert.equal(detailRes.status, 200);
    const persistedBoni = Number(detailRes.body.wave.pillars[0].sourceRefs[0].boniValue);
    assert.equal(persistedBoni, originalBoni);
  } finally {
    if (waveId) {
      await db.update(praemienWaves).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(praemienWaves.id, waveId));
    }
    await cleanupPraemienSourceFixture(fixture);
    delete process.env.BYPASS_AUTH_ROLE;
  }
});

test("admin praemien waves list supports pagination filters and detail contract", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();
  setTestBypassRole("admin");

  const app = createApp();
  const ids: string[] = [];
  try {
    const wavesToCreate = [
      { name: `List A-${randomUUID().slice(0, 6)}`, year: 2031, quarter: 1, status: "draft" as const, startDate: "2031-01-01", endDate: "2031-03-31" },
      { name: `List B-${randomUUID().slice(0, 6)}`, year: 2031, quarter: 2, status: "active" as const, startDate: "2031-04-01", endDate: "2031-06-30" },
      { name: `List C-${randomUUID().slice(0, 6)}`, year: 2031, quarter: 3, status: "archived" as const, startDate: "2031-07-01", endDate: "2031-09-30" },
    ];
    for (const wave of wavesToCreate) {
      const createRes = await request(app).post("/admin/praemien/waves").send({
        ...wave,
        pillars: [{ name: "Execution", orderIndex: 0, color: "#444444" }],
      });
      assert.equal(createRes.status, 201);
      ids.push(String(createRes.body.wave.id));
    }

    const pagedRes = await request(app).get("/admin/praemien/waves").query({ limit: 1, offset: 1, year: 2031 });
    assert.equal(pagedRes.status, 200);
    assert.equal(pagedRes.body.limit, 1);
    assert.equal(pagedRes.body.offset, 1);
    assert.ok(Number(pagedRes.body.total) >= 3);
    assert.equal(Array.isArray(pagedRes.body.waves), true);
    assert.equal(pagedRes.body.waves.length, 1);

    const activeRes = await request(app).get("/admin/praemien/waves").query({ status: "active", year: 2031 });
    assert.equal(activeRes.status, 200);
    assert.ok(activeRes.body.waves.every((wave: { status: string }) => wave.status === "active"));

    const detailRes = await request(app).get(`/admin/praemien/waves/${ids[0]}`);
    assert.equal(detailRes.status, 200);
    assert.equal(Array.isArray(detailRes.body.wave.pillars?.[0]?.sourceRefs ?? []), true);
    assert.equal("qualitySubmissions" in detailRes.body.wave, true);
    assert.equal("qualityScores" in detailRes.body.wave, false);
    assert.equal("sources" in detailRes.body.wave, false);
  } finally {
    if (ids.length > 0) {
      await db.update(praemienWaves).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(inArray(praemienWaves.id, ids));
    }
    delete process.env.BYPASS_AUTH_ROLE;
  }
});

test("admin praemien delete endpoint soft-deletes wave and handles not-found", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();
  setTestBypassRole("admin");

  const app = createApp();
  const createRes = await request(app).post("/admin/praemien/waves").send({
    name: `Delete-${randomUUID().slice(0, 6)}`,
    year: 2032,
    quarter: 4,
    startDate: "2032-10-01",
    endDate: "2032-12-31",
    pillars: [{ name: "Execution", orderIndex: 0, color: "#555555" }],
  });
  assert.equal(createRes.status, 201);
  const waveId = String(createRes.body.wave.id);

  const deleteRes = await request(app).patch(`/admin/praemien/waves/${waveId}/delete`).send({});
  assert.equal(deleteRes.status, 200);
  assert.equal(deleteRes.body.ok, true);
  assert.equal(deleteRes.body.waveId, waveId);

  const detailRes = await request(app).get(`/admin/praemien/waves/${waveId}`);
  assert.equal(detailRes.status, 404);
  assert.equal(detailRes.body.code, "wave_not_found");

  const missingRes = await request(app).patch(`/admin/praemien/waves/${randomUUID()}/delete`).send({});
  assert.equal(missingRes.status, 404);
  assert.equal(missingRes.body.code, "wave_not_found");

  delete process.env.BYPASS_AUTH_ROLE;
});

test("gm bonus finalizer upserts per red-month source and avoids duplicate accumulation", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();
  setTestBypassRole("admin");

  const now = new Date();
  const gmId = randomUUID();
  const marketId = randomUUID();
  const waveId = randomUUID();
  const sessionId = randomUUID();
  const sectionId = randomUUID();
  const visitQuestionId = randomUUID();
  const campaignId = randomUUID();
  const pillarId = randomUUID();
  const sourceId = randomUUID();
  const questionId = randomUUID();
  try {
    await db.insert(users).values({
      id: gmId,
      supabaseAuthId: `bonus-gm-${gmId}`,
      email: `bonus-gm-${gmId}@example.com`,
      role: "gm",
      firstName: "Bonus",
      lastName: "GM",
      isActive: true,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(markets).values({
      id: marketId,
      name: `Bonus Markt ${marketId.slice(0, 6)}`,
      dbName: "",
      address: "Bonus Straße 1",
      postalCode: "1010",
      city: "Wien",
      region: "Wien",
      visitFrequencyPerYear: 12,
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(questionBankShared).values({
      id: questionId,
      type: "numeric",
      text: "Bonus Zahl",
      required: true,
      config: {},
      rules: [],
      scoring: {},
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(campaigns).values({
      id: campaignId,
      name: `Bonus Campaign ${campaignId.slice(0, 6)}`,
      section: "standard",
      status: "active",
      scheduleType: "always",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(praemienWaves).values({
      id: waveId,
      name: `Bonus Wave ${waveId.slice(0, 6)}`,
      year: now.getFullYear(),
      quarter: (Math.floor(now.getMonth() / 3) + 1) as 1 | 2 | 3 | 4,
      status: "active",
      startDate: "2020-01-01",
      endDate: "2099-12-31",
      description: "",
      timezone: "Europe/Vienna",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(praemienWaveThresholds).values([
      {
        waveId,
        label: "Kein Bonus",
        orderIndex: 0,
        minPoints: "0",
        rewardEur: "0",
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      },
      {
        waveId,
        label: "Zielbonus",
        orderIndex: 1,
        minPoints: "10",
        rewardEur: "100",
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      },
    ]);
    await db.insert(praemienWavePillars).values({
      id: pillarId,
      waveId,
      name: "Schütten/Displays",
      description: "",
      color: "#DC2626",
      orderIndex: 0,
      isManual: false,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(praemienWaveSources).values({
      id: sourceId,
      waveId,
      pillarId,
      sectionType: "standard",
      fragebogenId: null,
      fragebogenName: "",
      moduleId: null,
      moduleName: "",
      questionId,
      questionText: "Bonus Zahl",
      scoreKey: "__value__",
      displayLabel: "Wert",
      isFactorMode: true,
      boniValue: "2",
      distributionFreqRule: null,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(visitSessions).values({
      id: sessionId,
      gmUserId: gmId,
      marketId,
      status: "submitted",
      startedAt: now,
      submittedAt: now,
      lastSavedAt: now,
      clientSessionToken: `bonus-session-${sessionId}`,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(visitSessionSections).values({
      id: sectionId,
      visitSessionId: sessionId,
      campaignId,
      section: "standard",
      fragebogenId: null,
      fragebogenNameSnapshot: "",
      orderIndex: 0,
      status: "submitted",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(visitSessionQuestions).values({
      id: visitQuestionId,
      visitSessionSectionId: sectionId,
      questionId,
      moduleId: null,
      moduleNameSnapshot: "",
      questionType: "numeric",
      questionTextSnapshot: "Bonus Zahl",
      questionConfigSnapshot: {},
      questionRulesSnapshot: [],
      questionChainsSnapshot: [],
      appliesToMarketChainSnapshot: true,
      requiredSnapshot: true,
      orderIndex: 0,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    const [answerRow] = await db.insert(visitAnswers).values({
      visitSessionId: sessionId,
      visitSessionSectionId: sectionId,
      visitSessionQuestionId: visitQuestionId,
      questionId,
      questionType: "numeric",
      answerStatus: "answered",
      valueText: null,
      valueNumber: "5",
      valueJson: { raw: 5 },
      isValid: true,
      validationError: null,
      answeredAt: now,
      changedAt: now,
      version: 1,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    }).returning({ id: visitAnswers.id });
    assert.ok(answerRow?.id);

    const firstRun = await finalizeBonusForSubmittedVisitSession({
      sessionId,
      gmUserId: gmId,
      marketId,
      submittedAt: now,
    });
    assert.equal(firstRun.applied, true);
    assert.equal(firstRun.waveId, waveId);

    const secondRun = await finalizeBonusForSubmittedVisitSession({
      sessionId,
      gmUserId: gmId,
      marketId,
      submittedAt: now,
    });
    assert.equal(secondRun.applied, true);

    const contribRows = await db
      .select()
      .from(praemienGmWaveContributions)
      .where(and(eq(praemienGmWaveContributions.waveId, waveId), eq(praemienGmWaveContributions.gmUserId, gmId)));
    assert.equal(contribRows.length, 1);
    assert.equal(Number(contribRows[0]?.appliedValue ?? 0), 10);

    const totalRows = await db
      .select()
      .from(praemienGmWaveTotals)
      .where(and(eq(praemienGmWaveTotals.waveId, waveId), eq(praemienGmWaveTotals.gmUserId, gmId)));
    assert.equal(totalRows.length, 1);
    assert.equal(Number(totalRows[0]?.totalPoints ?? 0), 10);
    assert.equal(Number(totalRows[0]?.currentRewardEur ?? 0), 100);
  } finally {
    await db.delete(praemienGmWaveContributions).where(eq(praemienGmWaveContributions.waveId, waveId));
    await db.delete(praemienGmWaveTotals).where(eq(praemienGmWaveTotals.waveId, waveId));
    await db.update(visitAnswers).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(visitAnswers.visitSessionId, sessionId));
    await db.update(visitSessionQuestions).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(visitSessionQuestions.visitSessionSectionId, sectionId));
    await db.update(visitSessionSections).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(visitSessionSections.visitSessionId, sessionId));
    await db.update(visitSessions).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(visitSessions.id, sessionId));
    await db.update(campaigns).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(campaigns.id, campaignId));
    await db.update(praemienWaves).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(praemienWaves.id, waveId));
    await db.update(questionBankShared).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(questionBankShared.id, questionId));
    await db.update(markets).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(markets.id, marketId));
    await db.update(users).set({ deletedAt: now, updatedAt: now }).where(eq(users.id, gmId));
    delete process.env.BYPASS_AUTH_ROLE;
    delete process.env.BYPASS_AUTH_USER_ID;
  }
});

test("gm bonus summary endpoint returns empty state when no active wave", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();
  setTestBypassRole("gm");
  const gmUserId = randomUUID();
  setTestBypassUserId(gmUserId);
  const now = new Date();
  try {
    await db.insert(users).values({
      id: gmUserId,
      supabaseAuthId: `bonus-summary-${gmUserId}`,
      email: `bonus-summary-${gmUserId}@example.com`,
      role: "gm",
      firstName: "Summary",
      lastName: "GM",
      isActive: true,
      createdAt: now,
      updatedAt: now,
    });
    const app = createApp();
    const response = await request(app).get("/markets/gm/bonus-summary");
    assert.equal(response.status, 200);
    assert.equal(response.body.hasActiveWave, false);
    assert.equal(Array.isArray(response.body.goals), true);
    assert.equal(response.body.goals.length, 0);
    assert.equal(response.body.currentRewardEur, 0);
  } finally {
    await db.update(users).set({ deletedAt: now, updatedAt: now }).where(eq(users.id, gmUserId));
    delete process.env.BYPASS_AUTH_ROLE;
    delete process.env.BYPASS_AUTH_USER_ID;
  }
});

test("gm visit-session submit finalizes bonus and summary reflects persisted totals", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();

  const now = new Date();
  const uniq = randomUUID().slice(0, 8);
  const gmId = randomUUID();
  const marketId = randomUUID();
  const waveId = randomUUID();
  const sessionId = randomUUID();
  const sectionId = randomUUID();
  const visitQuestionId = randomUUID();
  const campaignId = randomUUID();
  const pillarId = randomUUID();
  const sourceId = randomUUID();
  const questionId = randomUUID();

  try {
    await db.insert(users).values({
      id: gmId,
      supabaseAuthId: `submit-bonus-gm-${gmId}`,
      email: `submit-bonus-gm-${gmId}@example.com`,
      role: "gm",
      firstName: "Submit",
      lastName: "Bonus",
      isActive: true,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(markets).values({
      id: marketId,
      name: `Submit Bonus Markt ${uniq}`,
      dbName: "",
      address: "Submit Straße 1",
      postalCode: "1010",
      city: "Wien",
      region: "Wien",
      visitFrequencyPerYear: 12,
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(questionBankShared).values({
      id: questionId,
      type: "numeric",
      text: "Submit Bonus Zahl",
      required: true,
      config: {},
      rules: [],
      scoring: {},
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(campaigns).values({
      id: campaignId,
      name: `Submit Bonus Campaign ${uniq}`,
      section: "standard",
      status: "active",
      scheduleType: "always",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(praemienWaves).values({
      id: waveId,
      name: `Submit Bonus Wave ${uniq}`,
      year: now.getFullYear(),
      quarter: (Math.floor(now.getMonth() / 3) + 1) as 1 | 2 | 3 | 4,
      status: "active",
      startDate: "2020-01-01",
      endDate: "2099-12-31",
      description: "",
      timezone: "Europe/Vienna",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(praemienWaveThresholds).values([
      {
        waveId,
        label: "Kein Bonus",
        orderIndex: 0,
        minPoints: "0",
        rewardEur: "0",
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      },
      {
        waveId,
        label: "Zielbonus",
        orderIndex: 1,
        minPoints: "10",
        rewardEur: "100",
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      },
    ]);
    await db.insert(praemienWavePillars).values({
      id: pillarId,
      waveId,
      name: "Schuetten/Displays",
      description: "",
      color: "#DC2626",
      orderIndex: 0,
      isManual: false,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(praemienWaveSources).values({
      id: sourceId,
      waveId,
      pillarId,
      sectionType: "standard",
      fragebogenId: null,
      fragebogenName: "",
      moduleId: null,
      moduleName: "",
      questionId,
      questionText: "Submit Bonus Zahl",
      scoreKey: "__value__",
      displayLabel: "Wert",
      isFactorMode: true,
      boniValue: "2",
      distributionFreqRule: null,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(visitSessions).values({
      id: sessionId,
      gmUserId: gmId,
      marketId,
      status: "draft",
      startedAt: now,
      submittedAt: null,
      lastSavedAt: now,
      clientSessionToken: `submit-route-bonus-${sessionId}`,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(visitSessionSections).values({
      id: sectionId,
      visitSessionId: sessionId,
      campaignId,
      section: "standard",
      fragebogenId: null,
      fragebogenNameSnapshot: "",
      orderIndex: 0,
      status: "draft",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(visitSessionQuestions).values({
      id: visitQuestionId,
      visitSessionSectionId: sectionId,
      questionId,
      moduleId: null,
      moduleNameSnapshot: "",
      questionType: "numeric",
      questionTextSnapshot: "Submit Bonus Zahl",
      questionConfigSnapshot: {},
      questionRulesSnapshot: [],
      questionChainsSnapshot: [],
      appliesToMarketChainSnapshot: true,
      requiredSnapshot: true,
      orderIndex: 0,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(visitAnswers).values({
      visitSessionId: sessionId,
      visitSessionSectionId: sectionId,
      visitSessionQuestionId: visitQuestionId,
      questionId,
      questionType: "numeric",
      answerStatus: "answered",
      valueText: null,
      valueNumber: "6",
      valueJson: { raw: 6 },
      isValid: true,
      validationError: null,
      answeredAt: now,
      changedAt: now,
      version: 1,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    setTestBypassRole("gm");
    setTestBypassUserId(gmId);
    const app = createApp();

    const submitRes = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/submit`).send({});
    assert.equal(submitRes.status, 200);
    assert.equal(submitRes.body.status, "submitted");

    const contribRows = await db
      .select()
      .from(praemienGmWaveContributions)
      .where(and(eq(praemienGmWaveContributions.waveId, waveId), eq(praemienGmWaveContributions.gmUserId, gmId)));
    assert.equal(contribRows.length, 1);
    assert.equal(Number(contribRows[0]?.appliedValue ?? 0), 12);

    const totalRows = await db
      .select()
      .from(praemienGmWaveTotals)
      .where(and(eq(praemienGmWaveTotals.waveId, waveId), eq(praemienGmWaveTotals.gmUserId, gmId)));
    assert.equal(totalRows.length, 1);
    assert.equal(Number(totalRows[0]?.totalPoints ?? 0), 12);
    assert.equal(Number(totalRows[0]?.currentRewardEur ?? 0), 100);

    const summaryRes = await request(app).get("/markets/gm/bonus-summary");
    assert.equal(summaryRes.status, 200);
    assert.equal(summaryRes.body.hasActiveWave, true);
    assert.equal(summaryRes.body.waveId, waveId);
    assert.equal(summaryRes.body.totalPoints, 12);
    assert.equal(summaryRes.body.currentRewardEur, 100);
    assert.equal(Array.isArray(summaryRes.body.goals), true);
    assert.equal(summaryRes.body.goals.length, 1);
    assert.equal(summaryRes.body.goals[0]?.points, 12);
  } finally {
    await db.delete(praemienGmWaveContributions).where(eq(praemienGmWaveContributions.waveId, waveId));
    await db.delete(praemienGmWaveTotals).where(eq(praemienGmWaveTotals.waveId, waveId));
    await db.update(visitAnswers).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(visitAnswers.visitSessionId, sessionId));
    await db.update(visitSessionQuestions).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(visitSessionQuestions.visitSessionSectionId, sectionId));
    await db.update(visitSessionSections).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(visitSessionSections.visitSessionId, sessionId));
    await db.update(visitSessions).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(visitSessions.id, sessionId));
    await db.update(campaigns).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(campaigns.id, campaignId));
    await db.update(praemienWaves).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(praemienWaves.id, waveId));
    await db.update(questionBankShared).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(questionBankShared.id, questionId));
    await db.update(markets).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(markets.id, marketId));
    await db.update(users).set({ deletedAt: now, updatedAt: now }).where(eq(users.id, gmId));
    delete process.env.BYPASS_AUTH_ROLE;
    delete process.env.BYPASS_AUTH_USER_ID;
  }
});

test("gm bonus summary resolves active wave using wave timezone at utc boundary", async () => {
  if (!(await ensureDbReadyForPraemienTests())) return;
  enableTestAuthBypass();

  const now = new Date();
  const gmId = randomUUID();
  const waveId = randomUUID();

  try {
    await db.insert(users).values({
      id: gmId,
      supabaseAuthId: `tz-bonus-gm-${gmId}`,
      email: `tz-bonus-gm-${gmId}@example.com`,
      role: "gm",
      firstName: "Timezone",
      lastName: "GM",
      isActive: true,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(praemienWaves).values({
      id: waveId,
      name: "Boundary Wave",
      year: 2026,
      quarter: 1,
      status: "active",
      startDate: "2026-01-01",
      endDate: "2026-01-01",
      description: "",
      timezone: "America/Los_Angeles",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    const lateUtcStillLocalJan1 = new Date("2026-01-02T07:30:00.000Z");
    const summaryInside = await readGmActiveBonusSummary(gmId, lateUtcStillLocalJan1);
    assert.equal(summaryInside.hasActiveWave, true);
    assert.equal(summaryInside.waveId, waveId);

    const utcOutsideLocalJan1 = new Date("2026-01-02T09:00:00.000Z");
    const summaryOutside = await readGmActiveBonusSummary(gmId, utcOutsideLocalJan1);
    assert.equal(summaryOutside.hasActiveWave, false);
    assert.equal(summaryOutside.waveId, null);
  } finally {
    await db.update(praemienWaves).set({ isDeleted: true, deletedAt: now, updatedAt: now }).where(eq(praemienWaves.id, waveId));
    await db.update(users).set({ deletedAt: now, updatedAt: now }).where(eq(users.id, gmId));
    delete process.env.BYPASS_AUTH_ROLE;
    delete process.env.BYPASS_AUTH_USER_ID;
  }
});

test("gm visit photo comment-only patch persists and photo commit dedupes duplicates", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Visit session tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for visit-session test.");
    return;
  }
  const [sharedQuestion] = await db
    .select({ id: questionBankShared.id })
    .from(questionBankShared)
    .where(eq(questionBankShared.isDeleted, false))
    .limit(1);
  if (!sharedQuestion?.id) {
    t.skip("No shared question available for FK-safe visit-session test.");
    return;
  }

  const now = new Date();
  const uniq = `visit-photo-comment-${Date.now()}`;
  const campaignId = randomUUID();
  const sessionId = randomUUID();
  const sectionId = randomUUID();
  const photoQuestionId = randomUUID();
  const photoTagId = randomUUID();

  const [marketRow] = await db
    .insert(markets)
    .values({
      name: `Visit Comment Markt ${uniq}`,
      dbName: "",
      address: "Visit Comment Straße 1",
      postalCode: "1010",
      city: "Wien",
      region: "Wien",
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    })
    .returning({ id: markets.id });
  const marketId = marketRow?.id;
  if (!marketId) {
    t.skip("Could not create market row for visit-session test.");
    return;
  }

  await db.insert(campaigns).values({
    id: campaignId,
    name: `Visit Comment Campaign ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(photoTags).values({
    id: photoTagId,
    label: `Visit Comment Tag ${uniq}`,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessions).values({
    id: sessionId,
    gmUserId: gmUser.id,
    marketId,
    status: "draft",
    startedAt: now,
    lastSavedAt: now,
    clientSessionToken: `token-${uniq}`,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionSections).values({
    id: sectionId,
    visitSessionId: sessionId,
    campaignId,
    section: "standard",
    fragebogenId: null,
    fragebogenNameSnapshot: "Visit Foto",
    orderIndex: 0,
    status: "draft",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionQuestions).values({
    id: photoQuestionId,
    visitSessionSectionId: sectionId,
    questionId: sharedQuestion.id,
    moduleId: null,
    moduleNameSnapshot: "Module",
    questionType: "photo",
    questionTextSnapshot: "Foto",
    questionConfigSnapshot: { tagsEnabled: true, tagIds: [photoTagId] },
    questionRulesSnapshot: [],
    requiredSnapshot: false,
    orderIndex: 0,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });

  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  const app = createApp();

  const patchComment = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: photoQuestionId,
    comment: "Kommentar ohne Foto",
  });
  assert.equal(patchComment.status, 200);
  assert.equal(patchComment.body.result?.answerStatus, "unanswered");

  const [answerRow] = await db
    .select({ id: visitAnswers.id, answerStatus: visitAnswers.answerStatus })
    .from(visitAnswers)
    .where(and(eq(visitAnswers.visitSessionQuestionId, photoQuestionId), eq(visitAnswers.isDeleted, false)))
    .limit(1);
  assert.ok(answerRow?.id);
  assert.equal(answerRow?.answerStatus, "unanswered");

  const [commentRow] = await db
    .select({ id: visitQuestionComments.id, commentText: visitQuestionComments.commentText, isDeleted: visitQuestionComments.isDeleted })
    .from(visitQuestionComments)
    .where(eq(visitQuestionComments.visitSessionQuestionId, photoQuestionId))
    .limit(1);
  assert.equal(commentRow?.commentText, "Kommentar ohne Foto");
  assert.equal(commentRow?.isDeleted, false);

  const commitRes = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/photos/commit`).send({
    visitAnswerId: answerRow?.id,
    photos: [
      {
        storageBucket: "visit-photos",
        storagePath: `visit-photos/${sessionId}/${answerRow?.id}/same.jpg`,
        mimeType: "image/jpeg",
        byteSize: 123,
        sha256: "abc123",
        photoTagIds: [photoTagId, photoTagId],
      },
      {
        storageBucket: "visit-photos",
        storagePath: `visit-photos/${sessionId}/${answerRow?.id}/same.jpg`,
        mimeType: "image/jpeg",
        byteSize: 123,
        sha256: "abc123",
        photoTagIds: [photoTagId],
      },
    ],
  });
  assert.equal(commitRes.status, 200);

  const conflictingCommit = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/photos/commit`).send({
    visitAnswerId: answerRow?.id,
    photos: [
      {
        storageBucket: "visit-photos",
        storagePath: `visit-photos/${sessionId}/${answerRow?.id}/same.jpg`,
        mimeType: "image/jpeg",
        byteSize: 123,
        sha256: "conflict-1",
        photoTagIds: [photoTagId],
      },
      {
        storageBucket: "visit-photos",
        storagePath: `visit-photos/${sessionId}/${answerRow?.id}/same.jpg`,
        mimeType: "image/jpeg",
        byteSize: 123,
        sha256: "conflict-2",
        photoTagIds: [photoTagId],
      },
    ],
  });
  assert.equal(conflictingCommit.status, 400);

  const activePhotos = await db
    .select({ id: visitAnswerPhotos.id })
    .from(visitAnswerPhotos)
    .where(and(eq(visitAnswerPhotos.visitAnswerId, answerRow!.id), eq(visitAnswerPhotos.isDeleted, false)));
  assert.equal(activePhotos.length, 1);
  const activePhotoTags = await db
    .select({ id: visitAnswerPhotoTags.id })
    .from(visitAnswerPhotoTags)
    .where(and(eq(visitAnswerPhotoTags.visitAnswerPhotoId, activePhotos[0]!.id), eq(visitAnswerPhotoTags.isDeleted, false)));
  assert.equal(activePhotoTags.length, 1);

  const clearComment = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: photoQuestionId,
    comment: "",
  });
  assert.equal(clearComment.status, 200);
  const [clearedComment] = await db
    .select({ isDeleted: visitQuestionComments.isDeleted })
    .from(visitQuestionComments)
    .where(eq(visitQuestionComments.visitSessionQuestionId, photoQuestionId))
    .limit(1);
  assert.equal(clearedComment?.isDeleted, true);

  await db.update(visitSessions).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(visitSessions.id, sessionId));
  await db.update(campaigns).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(campaigns.id, campaignId));
  await db.update(photoTags).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(photoTags.id, photoTagId));
  await db.update(markets).set({ isDeleted: true, updatedAt: new Date() }).where(eq(markets.id, marketId));
  delete process.env.BYPASS_AUTH_ROLE;
  delete process.env.BYPASS_AUTH_USER_ID;
});

test("gm visit submit ignores non-applicable chain questions and their rules", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Visit session tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for visit-session test.");
    return;
  }

  const uniq = `${Date.now()}-${Math.round(Math.random() * 10000)}`;
  const now = new Date();
  const campaignId = randomUUID();
  const sessionId = randomUUID();
  const sectionId = randomUUID();
  const hiddenTriggerQuestionId = randomUUID();
  const hiddenRequiredQuestionId = randomUUID();
  const visibleRequiredQuestionId = randomUUID();

  let marketId: string | null = null;
  const sharedQuestions = await db
    .select({ id: questionBankShared.id })
    .from(questionBankShared)
    .where(and(eq(questionBankShared.isDeleted, false)))
    .limit(2);
  if (sharedQuestions.length === 0) {
    t.skip("No shared questions available for FK-safe visit-session test.");
    return;
  }
  const sharedTriggerId = sharedQuestions[0]?.id ?? randomUUID();
  const sharedVisibleId = sharedQuestions[1]?.id ?? sharedTriggerId;

  try {
    const [marketRow] = await db
      .insert(markets)
      .values({
        name: `Visit Markt Chains ${uniq}`,
        dbName: "spar",
        address: "Visit Session Straße 2",
        postalCode: "1010",
        city: "Wien",
        region: "Wien",
        isDeleted: false,
        createdAt: now,
        updatedAt: now,
      })
      .returning({ id: markets.id });
    marketId = marketRow?.id ?? null;
    if (!marketId) {
      t.skip("Could not create market row for visit-session test.");
      return;
    }

    await db.insert(campaigns).values({
      id: campaignId,
      name: `Visit Campaign Chains ${uniq}`,
      section: "standard",
      status: "active",
      scheduleType: "always",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(visitSessions).values({
      id: sessionId,
      gmUserId: gmUser.id,
      marketId,
      status: "draft",
      startedAt: now,
      lastSavedAt: now,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(visitSessionSections).values({
      id: sectionId,
      visitSessionId: sessionId,
      campaignId,
      section: "standard",
      fragebogenId: null,
      fragebogenNameSnapshot: "Visit Fragebogen Chains",
      orderIndex: 0,
      status: "draft",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(visitSessionQuestions).values([
      {
        id: hiddenTriggerQuestionId,
        visitSessionSectionId: sectionId,
        questionId: sharedTriggerId,
        moduleId: null,
        moduleNameSnapshot: "Module Hidden Trigger",
        questionType: "yesno",
        questionTextSnapshot: "Hidden trigger",
        questionConfigSnapshot: { options: ["Ja", "Nein"] },
        questionRulesSnapshot: [
          {
            triggerQuestionId: sharedTriggerId,
            operator: "equals",
            triggerValue: "Ja",
            triggerValueMax: "",
            action: "hide",
            targetQuestionIds: [sharedVisibleId],
          },
        ],
        questionChainsSnapshot: ["billa"],
        appliesToMarketChainSnapshot: false,
        requiredSnapshot: false,
        orderIndex: 0,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      },
      {
        id: hiddenRequiredQuestionId,
        visitSessionSectionId: sectionId,
        questionId: sharedTriggerId,
        moduleId: null,
        moduleNameSnapshot: "Module Hidden Required",
        questionType: "text",
        questionTextSnapshot: "Hidden required",
        questionConfigSnapshot: {},
        questionRulesSnapshot: [],
        questionChainsSnapshot: ["billa"],
        appliesToMarketChainSnapshot: false,
        requiredSnapshot: true,
        orderIndex: 1,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      },
      {
        id: visibleRequiredQuestionId,
        visitSessionSectionId: sectionId,
        questionId: sharedVisibleId,
        moduleId: null,
        moduleNameSnapshot: "Module Visible Required",
        questionType: "text",
        questionTextSnapshot: "Visible required",
        questionConfigSnapshot: {},
        questionRulesSnapshot: [],
        questionChainsSnapshot: ["spar"],
        appliesToMarketChainSnapshot: true,
        requiredSnapshot: true,
        orderIndex: 2,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      },
    ]);

    setTestBypassRole("gm");
    setTestBypassUserId(gmUser.id);
    const app = createApp();

    const submitBlocked = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/submit`).send({});
    assert.equal(submitBlocked.status, 409);
    assert.equal(submitBlocked.body.code, "visit_submit_incomplete_required");
    assert.ok(Array.isArray(submitBlocked.body.missingRequired));
    const missingVisitQuestionIds = (submitBlocked.body.missingRequired as Array<{ visitQuestionId?: unknown }>)
      .map((entry) => (typeof entry.visitQuestionId === "string" ? entry.visitQuestionId : ""));
    assert.equal(missingVisitQuestionIds.includes(visibleRequiredQuestionId), true);
    assert.equal(missingVisitQuestionIds.includes(hiddenRequiredQuestionId), false);

    const saveVisible = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
      visitQuestionId: visibleRequiredQuestionId,
      answer: "Erledigt",
    });
    assert.equal(saveVisible.status, 200);
    assert.equal(saveVisible.body.result?.isValid, true);

    const submitOk = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/submit`).send({});
    assert.equal(submitOk.status, 200);
    assert.equal(submitOk.body.status, "submitted");
  } finally {
    if (sessionId) {
      await db.update(visitSessions).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(visitSessions.id, sessionId));
    }
    if (campaignId) {
      await db.update(campaigns).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(campaigns.id, campaignId));
    }
    if (marketId) {
      await db.update(markets).set({ isDeleted: true, updatedAt: new Date() }).where(eq(markets.id, marketId));
    }
    delete process.env.BYPASS_AUTH_ROLE;
    delete process.env.BYPASS_AUTH_USER_ID;
  }
});

test("gm visit session create prefills latest submitted answers within current red month per gm+market+question", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Visit session tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  const [otherGmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt), gmUser?.id ? sql`${users.id} <> ${gmUser.id}` : sql`true`))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for red-month prefill test.");
    return;
  }

  const period = getCurrentRedPeriod(new Date());
  const periodStart = startOfDay(period.start);
  const periodEnd = startOfDay(period.end);
  const withinOld = addDays(periodStart, 1);
  const withinLatest = addDays(periodStart, 2);
  const outsidePeriod = addDays(periodStart, -2);
  const now = new Date();
  const uniq = `visit-redmonth-prefill-${Date.now()}`;

  const campaignId = randomUUID();
  const moduleId = randomUUID();
  const fragebogenId = randomUUID();
  const marketId = randomUUID();
  const otherMarketId = randomUUID();
  const sharedYesNoId = randomUUID();
  const sharedMultipleId = randomUUID();
  const sharedPhotoId = randomUUID();
  const photoTagId = randomUUID();
  let createdSessionId: string | null = null;
  const seededSessionIds: string[] = [];

  try {
    await db.insert(questionBankShared).values([
      {
        id: sharedYesNoId,
        questionType: "yesno",
        text: `Redmonth YesNo ${uniq}`,
        required: true,
        config: { options: ["Ja", "Nein"] },
        rules: [],
        scoring: {},
        isDeleted: false,
        createdAt: now,
        updatedAt: now,
      },
      {
        id: sharedMultipleId,
        questionType: "multiple",
        text: `Redmonth Multiple ${uniq}`,
        required: false,
        config: { options: ["A", "B", "C"] },
        rules: [],
        scoring: {},
        isDeleted: false,
        createdAt: now,
        updatedAt: now,
      },
      {
        id: sharedPhotoId,
        questionType: "photo",
        text: `Redmonth Photo ${uniq}`,
        required: true,
        config: { tagsEnabled: true, tagIds: [photoTagId] },
        rules: [],
        scoring: {},
        isDeleted: false,
        createdAt: now,
        updatedAt: now,
      },
    ]);
    await db.insert(photoTags).values({
      id: photoTagId,
      label: `Redmonth Tag ${uniq}`,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    await db.insert(moduleMain).values({
      id: moduleId,
      name: `Redmonth Module ${uniq}`,
      description: "",
      sectionKeywords: ["standard"],
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(moduleMainQuestion).values([
      {
        moduleId,
        questionId: sharedYesNoId,
        orderIndex: 0,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      },
      {
        moduleId,
        questionId: sharedMultipleId,
        orderIndex: 1,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      },
      {
        moduleId,
        questionId: sharedPhotoId,
        orderIndex: 2,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      },
    ]);
    await db.insert(fragebogenMain).values({
      id: fragebogenId,
      name: `Redmonth FB ${uniq}`,
      description: "",
      sectionKeywords: ["standard"],
      nurEinmalAusfuellbar: false,
      status: "active",
      scheduleType: "always",
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(fragebogenMainModule).values({
      fragebogenId,
      moduleId,
      orderIndex: 0,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    await db.insert(markets).values([
      {
        id: marketId,
        name: `Redmonth Markt ${uniq}`,
        dbName: "",
        address: "Redmonth Straße 1",
        postalCode: "1010",
        city: "Wien",
        region: "Wien",
        isDeleted: false,
        createdAt: now,
        updatedAt: now,
      },
      {
        id: otherMarketId,
        name: `Redmonth Other Markt ${uniq}`,
        dbName: "",
        address: "Redmonth Straße 2",
        postalCode: "1010",
        city: "Wien",
        region: "Wien",
        isDeleted: false,
        createdAt: now,
        updatedAt: now,
      },
    ]);
    await db.insert(campaigns).values({
      id: campaignId,
      name: `Redmonth Campaign ${uniq}`,
      section: "standard",
      currentFragebogenId: fragebogenId,
      status: "active",
      scheduleType: "always",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
    await db.insert(campaignMarketAssignments).values({
      campaignId,
      marketId,
      gmUserId: gmUser.id,
      assignedAt: now,
      assignedByUserId: null,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });

    const seedSubmittedSession = async (input: {
      gmId: string;
      seededMarketId: string;
      submittedAt: Date;
      yesNo: "Ja" | "Nein";
      multiple: string[];
      yesNoComment?: string;
    }): Promise<void> => {
      const sessionId = randomUUID();
      const sectionId = randomUUID();
      const yesNoVisitQuestionId = randomUUID();
      const multipleVisitQuestionId = randomUUID();
      const photoVisitQuestionId = randomUUID();
      const yesNoAnswerId = randomUUID();
      const multipleAnswerId = randomUUID();
      const photoAnswerId = randomUUID();
      seededSessionIds.push(sessionId);

      await db.insert(visitSessions).values({
        id: sessionId,
        gmUserId: input.gmId,
        marketId: input.seededMarketId,
        status: "submitted",
        startedAt: input.submittedAt,
        submittedAt: input.submittedAt,
        lastSavedAt: input.submittedAt,
        isDeleted: false,
        deletedAt: null,
        createdAt: input.submittedAt,
        updatedAt: input.submittedAt,
      });
      await db.insert(visitSessionSections).values({
        id: sectionId,
        visitSessionId: sessionId,
        campaignId,
        section: "standard",
        fragebogenId,
        fragebogenNameSnapshot: `Seed FB ${uniq}`,
        orderIndex: 0,
        status: "submitted",
        isDeleted: false,
        deletedAt: null,
        createdAt: input.submittedAt,
        updatedAt: input.submittedAt,
      });
      await db.insert(visitSessionQuestions).values([
        {
          id: yesNoVisitQuestionId,
          visitSessionSectionId: sectionId,
          questionId: sharedYesNoId,
          moduleId,
          moduleNameSnapshot: `Seed Module ${uniq}`,
          questionType: "yesno",
          questionTextSnapshot: `Redmonth YesNo ${uniq}`,
          questionConfigSnapshot: { options: ["Ja", "Nein"] },
          questionRulesSnapshot: [],
          requiredSnapshot: true,
          orderIndex: 0,
          isDeleted: false,
          deletedAt: null,
          createdAt: input.submittedAt,
          updatedAt: input.submittedAt,
        },
        {
          id: multipleVisitQuestionId,
          visitSessionSectionId: sectionId,
          questionId: sharedMultipleId,
          moduleId,
          moduleNameSnapshot: `Seed Module ${uniq}`,
          questionType: "multiple",
          questionTextSnapshot: `Redmonth Multiple ${uniq}`,
          questionConfigSnapshot: { options: ["A", "B", "C"] },
          questionRulesSnapshot: [],
          requiredSnapshot: false,
          orderIndex: 1,
          isDeleted: false,
          deletedAt: null,
          createdAt: input.submittedAt,
          updatedAt: input.submittedAt,
        },
        {
          id: photoVisitQuestionId,
          visitSessionSectionId: sectionId,
          questionId: sharedPhotoId,
          moduleId,
          moduleNameSnapshot: `Seed Module ${uniq}`,
          questionType: "photo",
          questionTextSnapshot: `Redmonth Photo ${uniq}`,
          questionConfigSnapshot: { tagsEnabled: true, tagIds: [photoTagId] },
          questionRulesSnapshot: [],
          requiredSnapshot: true,
          orderIndex: 2,
          isDeleted: false,
          deletedAt: null,
          createdAt: input.submittedAt,
          updatedAt: input.submittedAt,
        },
      ]);
      await db.insert(visitAnswers).values([
        {
          id: yesNoAnswerId,
          visitSessionId: sessionId,
          visitSessionSectionId: sectionId,
          visitSessionQuestionId: yesNoVisitQuestionId,
          questionId: sharedYesNoId,
          questionType: "yesno",
          answerStatus: "answered",
          valueText: input.yesNo,
          valueJson: { raw: input.yesNo },
          isValid: true,
          validationError: null,
          answeredAt: input.submittedAt,
          changedAt: input.submittedAt,
          version: 1,
          isDeleted: false,
          deletedAt: null,
          createdAt: input.submittedAt,
          updatedAt: input.submittedAt,
        },
        {
          id: multipleAnswerId,
          visitSessionId: sessionId,
          visitSessionSectionId: sectionId,
          visitSessionQuestionId: multipleVisitQuestionId,
          questionId: sharedMultipleId,
          questionType: "multiple",
          answerStatus: "answered",
          valueJson: { raw: input.multiple },
          isValid: true,
          validationError: null,
          answeredAt: input.submittedAt,
          changedAt: input.submittedAt,
          version: 1,
          isDeleted: false,
          deletedAt: null,
          createdAt: input.submittedAt,
          updatedAt: input.submittedAt,
        },
        {
          id: photoAnswerId,
          visitSessionId: sessionId,
          visitSessionSectionId: sectionId,
          visitSessionQuestionId: photoVisitQuestionId,
          questionId: sharedPhotoId,
          questionType: "photo",
          answerStatus: "answered",
          valueJson: {
            storage: [{ path: `visit-photos/${sessionId}/${photoAnswerId}/seed.jpg`, bucket: "visit-photos" }],
          },
          isValid: true,
          validationError: null,
          answeredAt: input.submittedAt,
          changedAt: input.submittedAt,
          version: 1,
          isDeleted: false,
          deletedAt: null,
          createdAt: input.submittedAt,
          updatedAt: input.submittedAt,
        },
      ]);
      if (input.yesNoComment) {
        await db.insert(visitQuestionComments).values({
          visitSessionQuestionId: yesNoVisitQuestionId,
          commentText: input.yesNoComment,
          commentedAt: input.submittedAt,
          isDeleted: false,
          deletedAt: null,
          createdAt: input.submittedAt,
          updatedAt: input.submittedAt,
        });
      }
      if (input.multiple.length > 0) {
        await db.insert(visitAnswerOptions).values(
          input.multiple.map((value, idx) => ({
            visitAnswerId: multipleAnswerId,
            optionRole: "sub",
            optionValue: value,
            orderIndex: idx,
            isDeleted: false,
            deletedAt: null,
            createdAt: input.submittedAt,
            updatedAt: input.submittedAt,
          })),
        );
      }
      const [insertedPhoto] = await db
        .insert(visitAnswerPhotos)
        .values({
          visitAnswerId: photoAnswerId,
          storageBucket: "visit-photos",
          storagePath: `visit-photos/${sessionId}/${photoAnswerId}/seed.jpg`,
          mimeType: "image/jpeg",
          byteSize: 100,
          uploadedAt: input.submittedAt,
          isDeleted: false,
          deletedAt: null,
          createdAt: input.submittedAt,
          updatedAt: input.submittedAt,
        })
        .returning({ id: visitAnswerPhotos.id });
      if (insertedPhoto?.id) {
        await db.insert(visitAnswerPhotoTags).values({
          visitAnswerPhotoId: insertedPhoto.id,
          photoTagId,
          photoTagLabelSnapshot: `Redmonth Tag ${uniq}`,
          isDeleted: false,
          deletedAt: null,
          createdAt: input.submittedAt,
          updatedAt: input.submittedAt,
        });
      }
    };

    await seedSubmittedSession({
      gmId: gmUser.id,
      seededMarketId: marketId,
      submittedAt: withinOld,
      yesNo: "Ja",
      multiple: ["A"],
      yesNoComment: "old comment",
    });
    await seedSubmittedSession({
      gmId: gmUser.id,
      seededMarketId: marketId,
      submittedAt: withinLatest,
      yesNo: "Nein",
      multiple: ["B", "C"],
      yesNoComment: "latest comment",
    });
    await seedSubmittedSession({
      gmId: gmUser.id,
      seededMarketId: marketId,
      submittedAt: outsidePeriod,
      yesNo: "Ja",
      multiple: ["A"],
      yesNoComment: "outside period comment",
    });
    await seedSubmittedSession({
      gmId: gmUser.id,
      seededMarketId: otherMarketId,
      submittedAt: withinLatest,
      yesNo: "Ja",
      multiple: ["A"],
      yesNoComment: "other market comment",
    });
    if (otherGmUser?.id) {
      await seedSubmittedSession({
        gmId: otherGmUser.id,
        seededMarketId: marketId,
        submittedAt: withinLatest,
        yesNo: "Ja",
        multiple: ["A"],
        yesNoComment: "other gm comment",
      });
    }

    setTestBypassRole("gm");
    setTestBypassUserId(gmUser.id);
    const app = createApp();

    const createRes = await request(app).post("/markets/gm/visit-sessions").send({
      marketId,
      campaignIds: [campaignId],
    });
    assert.equal(createRes.status, 201);
    createdSessionId = createRes.body.session?.id as string;
    assert.ok(createdSessionId);

    const getRes = await request(app).get(`/markets/gm/visit-sessions/${createdSessionId}`);
    assert.equal(getRes.status, 200);
    const createdQuestions = (getRes.body.sections?.[0]?.questions ?? []) as Array<{
      questionId?: string;
      answer?: {
        valueText?: string | null;
        options?: Array<{ optionRole?: string; optionValue?: string }>;
      } | null;
      comment?: string;
    }>;
    const yesNoQuestion = createdQuestions.find((q) => q.questionId === sharedYesNoId);
    const multipleQuestion = createdQuestions.find((q) => q.questionId === sharedMultipleId);
    assert.ok(yesNoQuestion);
    assert.ok(multipleQuestion);
    assert.equal(yesNoQuestion?.answer?.valueText, "Nein");
    assert.equal(yesNoQuestion?.comment, "latest comment");
    const selectedMulti = (multipleQuestion?.answer?.options ?? [])
      .filter((entry) => entry.optionRole === "sub")
      .map((entry) => entry.optionValue ?? "");
    assert.deepEqual(selectedMulti, ["B", "C"]);

    assert.notEqual(yesNoQuestion?.answer?.valueText, "Ja");
    assert.notEqual(yesNoQuestion?.comment, "outside period comment");
    assert.ok(withinLatest <= periodEnd);
    const submitRes = await request(app).post(`/markets/gm/visit-sessions/${createdSessionId}/submit`).send({});
    assert.equal(submitRes.status, 200);
    assert.equal(submitRes.body.status, "submitted");
  } finally {
    if (createdSessionId) {
      seededSessionIds.push(createdSessionId);
    }
    if (seededSessionIds.length > 0) {
      await db
        .update(visitSessions)
        .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
        .where(inArray(visitSessions.id, seededSessionIds));
    }
    await db
      .update(campaignMarketAssignments)
      .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
      .where(and(eq(campaignMarketAssignments.campaignId, campaignId), inArray(campaignMarketAssignments.marketId, [marketId])));
    await db
      .update(campaigns)
      .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
      .where(eq(campaigns.id, campaignId));
    await db
      .update(fragebogenMainModule)
      .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
      .where(and(eq(fragebogenMainModule.fragebogenId, fragebogenId), eq(fragebogenMainModule.moduleId, moduleId)));
    await db
      .update(fragebogenMain)
      .set({ isDeleted: true, updatedAt: new Date() })
      .where(eq(fragebogenMain.id, fragebogenId));
    await db
      .update(moduleMainQuestion)
      .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
      .where(eq(moduleMainQuestion.moduleId, moduleId));
    await db
      .update(moduleMain)
      .set({ isDeleted: true, updatedAt: new Date() })
      .where(eq(moduleMain.id, moduleId));
    await db
      .update(questionBankShared)
      .set({ isDeleted: true, updatedAt: new Date() })
      .where(inArray(questionBankShared.id, [sharedYesNoId, sharedMultipleId, sharedPhotoId]));
    await db
      .update(photoTags)
      .set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() })
      .where(eq(photoTags.id, photoTagId));
    await db
      .update(markets)
      .set({ isDeleted: true, updatedAt: new Date() })
      .where(inArray(markets.id, [marketId, otherMarketId]));
    delete process.env.BYPASS_AUTH_ROLE;
    delete process.env.BYPASS_AUTH_USER_ID;
  }
});

test("gm visit duplicate shared question ids stay synchronized for answer/comment updates", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Visit session tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  const [sharedQuestion] = await db
    .select({ id: questionBankShared.id })
    .from(questionBankShared)
    .where(eq(questionBankShared.isDeleted, false))
    .limit(1);
  if (!gmUser?.id || !sharedQuestion?.id) {
    t.skip("Missing GM or shared question for duplicate sync test.");
    return;
  }

  const now = new Date();
  const uniq = `visit-dup-sync-${Date.now()}`;
  const campaignId = randomUUID();
  const sessionId = randomUUID();
  const sectionAId = randomUUID();
  const sectionBId = randomUUID();
  const questionAId = randomUUID();
  const questionBId = randomUUID();

  const [marketRow] = await db
    .insert(markets)
    .values({
      name: `Visit Dup Sync Markt ${uniq}`,
      dbName: "",
      address: "Dup Sync Straße 1",
      postalCode: "1010",
      city: "Wien",
      region: "Wien",
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    })
    .returning({ id: markets.id });
  const marketId = marketRow?.id;
  if (!marketId) {
    t.skip("Could not create market row for duplicate sync test.");
    return;
  }

  await db.insert(campaigns).values({
    id: campaignId,
    name: `Visit Dup Sync Campaign ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessions).values({
    id: sessionId,
    gmUserId: gmUser.id,
    marketId,
    status: "draft",
    startedAt: now,
    lastSavedAt: now,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionSections).values([
    {
      id: sectionAId,
      visitSessionId: sessionId,
      campaignId,
      section: "standard",
      fragebogenId: null,
      fragebogenNameSnapshot: "Dup Sync FB A",
      orderIndex: 0,
      status: "draft",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: sectionBId,
      visitSessionId: sessionId,
      campaignId,
      section: "flex",
      fragebogenId: null,
      fragebogenNameSnapshot: "Dup Sync FB B",
      orderIndex: 1,
      status: "draft",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
  ]);
  await db.insert(visitSessionQuestions).values([
    {
      id: questionAId,
      visitSessionSectionId: sectionAId,
      questionId: sharedQuestion.id,
      moduleId: null,
      moduleNameSnapshot: "Dup Module A",
      questionType: "text",
      questionTextSnapshot: "Dup Text A",
      questionConfigSnapshot: {},
      questionRulesSnapshot: [],
      requiredSnapshot: false,
      orderIndex: 0,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: questionBId,
      visitSessionSectionId: sectionBId,
      questionId: sharedQuestion.id,
      moduleId: null,
      moduleNameSnapshot: "Dup Module B",
      questionType: "text",
      questionTextSnapshot: "Dup Text B",
      questionConfigSnapshot: {},
      questionRulesSnapshot: [],
      requiredSnapshot: false,
      orderIndex: 0,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
  ]);

  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  const app = createApp();

  const patchRes = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: questionAId,
    answer: "Synchronisiert",
    comment: "Ein Kommentar",
  });
  assert.equal(patchRes.status, 200);
  assert.equal(patchRes.body.result?.answerStatus, "answered");

  const syncedAnswers = await db
    .select({ visitSessionQuestionId: visitAnswers.visitSessionQuestionId, valueText: visitAnswers.valueText })
    .from(visitAnswers)
    .where(and(inArray(visitAnswers.visitSessionQuestionId, [questionAId, questionBId]), eq(visitAnswers.isDeleted, false)));
  assert.equal(syncedAnswers.length, 2);
  assert.ok(syncedAnswers.every((row) => row.valueText === "Synchronisiert"));

  const syncedComments = await db
    .select({ visitSessionQuestionId: visitQuestionComments.visitSessionQuestionId, commentText: visitQuestionComments.commentText })
    .from(visitQuestionComments)
    .where(and(inArray(visitQuestionComments.visitSessionQuestionId, [questionAId, questionBId]), eq(visitQuestionComments.isDeleted, false)));
  assert.equal(syncedComments.length, 2);
  assert.ok(syncedComments.every((row) => row.commentText === "Ein Kommentar"));

  await db.update(visitSessions).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(visitSessions.id, sessionId));
  await db.update(campaigns).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(campaigns.id, campaignId));
  await db.update(markets).set({ isDeleted: true, updatedAt: new Date() }).where(eq(markets.id, marketId));
  delete process.env.BYPASS_AUTH_ROLE;
  delete process.env.BYPASS_AUTH_USER_ID;
});

test("ipp engine computes score keys and excludes zero-valued answers", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForIppTests())) {
    t.skip("IPP tables are not present in the configured DATABASE_URL.");
    return;
  }

  const now = new Date();
  const submittedAt = new Date(2026, 1, 2, 10, 0, 0);
  const period = getRedPeriodForDate(submittedAt);

  const gmUserId = randomUUID();
  const marketId = randomUUID();
  const campaignId = randomUUID();
  const sessionId = randomUUID();
  const sectionId = randomUUID();
  const numericQuestionId = randomUUID();
  const yesNoQuestionId = randomUUID();
  const visitNumericQuestionId = randomUUID();
  const visitYesNoQuestionId = randomUUID();
  const numericAnswerId = randomUUID();
  const yesNoAnswerId = randomUUID();

  await db.insert(users).values({
    id: gmUserId,
    supabaseAuthId: `auth-${gmUserId}`,
    email: `ipp-engine-${gmUserId}@example.com`,
    role: "gm",
    firstName: "Ipp",
    lastName: "Engine",
    isActive: true,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(markets).values({
    id: marketId,
    name: "IPP Engine Markt",
    dbName: "billa",
    address: "Musterstraße 1",
    postalCode: "1010",
    city: "Wien",
    region: "Ost",
    currentGmName: "Ipp Engine",
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(campaigns).values({
    id: campaignId,
    name: "IPP Engine Kampagne",
    section: "standard",
    status: "active",
    scheduleType: "always",
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });

  await db.insert(questionBankShared).values([
    {
      id: numericQuestionId,
      questionType: "numeric",
      text: "Anzahl Platzierungen",
      required: true,
      config: { min: 0, decimals: false },
      rules: [],
      scoring: {},
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: yesNoQuestionId,
      questionType: "yesno",
      text: "Display sichtbar",
      required: true,
      config: { options: ["Ja", "Nein"] },
      rules: [],
      scoring: {},
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    },
  ]);
  await db.insert(questionScoring).values([
    {
      questionId: numericQuestionId,
      scoreKey: "__value__",
      ipp: "2.00",
      boni: null,
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    },
    {
      questionId: yesNoQuestionId,
      scoreKey: "Nein",
      ipp: "0.00",
      boni: null,
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    },
  ]);

  await db.insert(visitSessions).values({
    id: sessionId,
    gmUserId,
    marketId,
    status: "submitted",
    startedAt: submittedAt,
    submittedAt,
    lastSavedAt: submittedAt,
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionSections).values({
    id: sectionId,
    visitSessionId: sessionId,
    campaignId,
    section: "standard",
    fragebogenId: null,
    fragebogenNameSnapshot: "IPP Engine FB",
    orderIndex: 0,
    status: "submitted",
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionQuestions).values([
    {
      id: visitNumericQuestionId,
      visitSessionSectionId: sectionId,
      questionId: numericQuestionId,
      moduleId: null,
      moduleNameSnapshot: "IPP",
      questionType: "numeric",
      questionTextSnapshot: "Anzahl Platzierungen",
      questionConfigSnapshot: { min: 0, decimals: false },
      questionRulesSnapshot: [],
      questionChainsSnapshot: [],
      appliesToMarketChainSnapshot: true,
      requiredSnapshot: true,
      orderIndex: 0,
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: visitYesNoQuestionId,
      visitSessionSectionId: sectionId,
      questionId: yesNoQuestionId,
      moduleId: null,
      moduleNameSnapshot: "IPP",
      questionType: "yesno",
      questionTextSnapshot: "Display sichtbar",
      questionConfigSnapshot: { options: ["Ja", "Nein"] },
      questionRulesSnapshot: [],
      questionChainsSnapshot: [],
      appliesToMarketChainSnapshot: true,
      requiredSnapshot: true,
      orderIndex: 1,
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    },
  ]);
  await db.insert(visitAnswers).values([
    {
      id: numericAnswerId,
      visitSessionId: sessionId,
      visitSessionSectionId: sectionId,
      visitSessionQuestionId: visitNumericQuestionId,
      questionId: numericQuestionId,
      questionType: "numeric",
      answerStatus: "answered",
      valueText: "3",
      valueNumber: "3",
      valueJson: { raw: "3" },
      isValid: true,
      validationError: null,
      answeredAt: submittedAt,
      changedAt: submittedAt,
      version: 1,
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: yesNoAnswerId,
      visitSessionId: sessionId,
      visitSessionSectionId: sectionId,
      visitSessionQuestionId: visitYesNoQuestionId,
      questionId: yesNoQuestionId,
      questionType: "yesno",
      answerStatus: "answered",
      valueText: "Nein",
      valueJson: { raw: "Nein" },
      isValid: true,
      validationError: null,
      answeredAt: submittedAt,
      changedAt: submittedAt,
      version: 1,
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    },
  ]);

  const computed = await computeMarketIppForPeriod({
    marketId,
    periodStart: period.start,
    periodEnd: period.end,
  });
  assert.equal(computed.marketIpp, 6);
  assert.equal(computed.contributingQuestionCount, 1);
  assert.equal(computed.sourceSubmissionCount, 1);
  assert.equal(computed.questionRows.length, 2);
  assert.equal(computed.questionRows.filter((row) => row.counted).length, 1);
});

test("ipp finalizer writes one archived row per market+period idempotently", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForIppTests())) {
    t.skip("IPP tables are not present in the configured DATABASE_URL.");
    return;
  }

  const now = new Date();
  const submittedAt = new Date(2026, 0, 28, 9, 30, 0);
  const runAt = new Date(2026, 2, 1, 12, 0, 0);
  const period = getRedPeriodForDate(submittedAt);

  const gmUserId = randomUUID();
  const marketId = randomUUID();
  const campaignId = randomUUID();
  const sessionId = randomUUID();
  const sectionId = randomUUID();
  const questionId = randomUUID();
  const visitQuestionId = randomUUID();
  const answerId = randomUUID();

  await db.insert(users).values({
    id: gmUserId,
    supabaseAuthId: `auth-${gmUserId}`,
    email: `ipp-finalizer-${gmUserId}@example.com`,
    role: "gm",
    firstName: "Ipp",
    lastName: "Finalizer",
    isActive: true,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(markets).values({
    id: marketId,
    name: "IPP Finalizer Markt",
    dbName: "spar",
    address: "Testweg 2",
    postalCode: "4020",
    city: "Linz",
    region: "West",
    currentGmName: "Ipp Finalizer",
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(campaigns).values({
    id: campaignId,
    name: "IPP Finalizer Kampagne",
    section: "standard",
    status: "active",
    scheduleType: "always",
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(questionBankShared).values({
    id: questionId,
    questionType: "numeric",
    text: "Finalizer Numeric",
    required: true,
    config: { min: 0, decimals: false },
    rules: [],
    scoring: {},
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(questionScoring).values({
    questionId,
    scoreKey: "__value__",
    ipp: "1.00",
    boni: null,
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessions).values({
    id: sessionId,
    gmUserId,
    marketId,
    status: "submitted",
    startedAt: submittedAt,
    submittedAt,
    lastSavedAt: submittedAt,
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionSections).values({
    id: sectionId,
    visitSessionId: sessionId,
    campaignId,
    section: "standard",
    fragebogenId: null,
    fragebogenNameSnapshot: "IPP Finalizer FB",
    orderIndex: 0,
    status: "submitted",
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionQuestions).values({
    id: visitQuestionId,
    visitSessionSectionId: sectionId,
    questionId,
    moduleId: null,
    moduleNameSnapshot: "IPP",
    questionType: "numeric",
    questionTextSnapshot: "Finalizer Numeric",
    questionConfigSnapshot: { min: 0, decimals: false },
    questionRulesSnapshot: [],
    questionChainsSnapshot: [],
    appliesToMarketChainSnapshot: true,
    requiredSnapshot: true,
    orderIndex: 0,
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitAnswers).values({
    id: answerId,
    visitSessionId: sessionId,
    visitSessionSectionId: sectionId,
    visitSessionQuestionId: visitQuestionId,
    questionId,
    questionType: "numeric",
    answerStatus: "answered",
    valueText: "5",
    valueNumber: "5",
    valueJson: { raw: "5" },
    isValid: true,
    validationError: null,
    answeredAt: submittedAt,
    changedAt: submittedAt,
    version: 1,
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });

  const firstRun = await runIppFinalizerOnce(runAt);
  const secondRun = await runIppFinalizerOnce(runAt);
  assert.equal(firstRun.ok, true);
  assert.equal(secondRun.ok, true);

  const archivedRows = await db
    .select()
    .from(ippMarketRedmonthResults)
    .where(
      and(
        eq(ippMarketRedmonthResults.marketId, marketId),
        eq(ippMarketRedmonthResults.redPeriodStart, toYmd(period.start)),
        eq(ippMarketRedmonthResults.isDeleted, false),
      ),
    );
  assert.equal(archivedRows.length, 1);
  assert.equal(Number(archivedRows[0]?.marketIpp ?? 0), 5);
  assert.equal(archivedRows[0]?.isFinalized, true);
  assert.equal(Array.isArray(archivedRows[0]?.questionRowsSnapshot), true);
  assert.equal((archivedRows[0]?.questionRowsSnapshot as unknown[] | undefined)?.length ?? 0, 1);
});

test("ipp closed-period detail stays snapshot-stable until queue recalculation runs", async (t) => {
  enableTestAuthBypass();
  setTestBypassRole("admin");
  if (!(await ensureDbReadyForIppTests())) {
    t.skip("IPP tables are not present in the configured DATABASE_URL.");
    return;
  }

  const app = createApp();
  const now = new Date();
  const submittedAt = new Date(2026, 0, 18, 9, 0, 0);
  const runAt = new Date(2026, 2, 1, 12, 0, 0);
  const period = getRedPeriodForDate(submittedAt);

  const gmUserId = randomUUID();
  const marketId = randomUUID();
  const campaignId = randomUUID();
  const sessionId = randomUUID();
  const sectionId = randomUUID();
  const questionId = randomUUID();
  const visitQuestionId = randomUUID();
  const answerId = randomUUID();

  await db.insert(users).values({
    id: gmUserId,
    supabaseAuthId: `auth-${gmUserId}`,
    email: `ipp-recalc-${gmUserId}@example.com`,
    role: "gm",
    firstName: "Ipp",
    lastName: "Queue",
    isActive: true,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(markets).values({
    id: marketId,
    name: "IPP Queue Markt",
    dbName: "billa",
    address: "Queueweg 5",
    postalCode: "1010",
    city: "Wien",
    region: "Ost",
    currentGmName: "Ipp Queue",
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(campaigns).values({
    id: campaignId,
    name: "IPP Queue Kampagne",
    section: "standard",
    status: "active",
    scheduleType: "always",
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(questionBankShared).values({
    id: questionId,
    questionType: "numeric",
    text: "Queue Numeric",
    required: true,
    config: { min: 0, decimals: false },
    rules: [],
    scoring: {},
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(questionScoring).values({
    questionId,
    scoreKey: "__value__",
    ipp: "1.00",
    boni: null,
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessions).values({
    id: sessionId,
    gmUserId,
    marketId,
    status: "submitted",
    startedAt: submittedAt,
    submittedAt,
    lastSavedAt: submittedAt,
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionSections).values({
    id: sectionId,
    visitSessionId: sessionId,
    campaignId,
    section: "standard",
    fragebogenId: null,
    fragebogenNameSnapshot: "IPP Queue FB",
    orderIndex: 0,
    status: "submitted",
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionQuestions).values({
    id: visitQuestionId,
    visitSessionSectionId: sectionId,
    questionId,
    moduleId: null,
    moduleNameSnapshot: "IPP",
    questionType: "numeric",
    questionTextSnapshot: "Queue Numeric",
    questionConfigSnapshot: { min: 0, decimals: false },
    questionRulesSnapshot: [],
    questionChainsSnapshot: [],
    appliesToMarketChainSnapshot: true,
    requiredSnapshot: true,
    orderIndex: 0,
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitAnswers).values({
    id: answerId,
    visitSessionId: sessionId,
    visitSessionSectionId: sectionId,
    visitSessionQuestionId: visitQuestionId,
    questionId,
    questionType: "numeric",
    answerStatus: "answered",
    valueText: "5",
    valueNumber: "5",
    valueJson: { raw: "5" },
    isValid: true,
    validationError: null,
    answeredAt: submittedAt,
    changedAt: submittedAt,
    version: 1,
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });

  const firstRun = await runIppFinalizerOnce(runAt);
  assert.equal(firstRun.ok, true);

  const periodStartYmd = toYmd(period.start);
  const beforeRes = await request(app).get(`/admin/ipp/${marketId}/detail`).query({ periodStart: periodStartYmd });
  assert.equal(beforeRes.status, 200);
  assert.equal(Number(beforeRes.body.record?.marketIpp ?? 0), 5);

  await db
    .update(questionScoring)
    .set({ ipp: "2.00", updatedAt: new Date() })
    .where(and(eq(questionScoring.questionId, questionId), eq(questionScoring.scoreKey, "__value__")));

  const stillSnapshotRes = await request(app).get(`/admin/ipp/${marketId}/detail`).query({ periodStart: periodStartYmd });
  assert.equal(stillSnapshotRes.status, 200);
  assert.equal(Number(stillSnapshotRes.body.record?.marketIpp ?? 0), 5);

  const queued = await enqueueIppRecalcForQuestionScoringChanges([questionId], "test_scoring_change", runAt);
  assert.equal(queued, 1);
  const queueRows = await db
    .select()
    .from(ippRecalcQueue)
    .where(and(eq(ippRecalcQueue.marketId, marketId), eq(ippRecalcQueue.redPeriodStart, periodStartYmd)));
  assert.equal(queueRows.length, 1);
  assert.equal(queueRows[0]?.status, "pending");

  const secondRun = await runIppFinalizerOnce(runAt);
  assert.equal(secondRun.ok, true);
  assert.equal(secondRun.queueItemsProcessed >= 1, true);

  const [updatedArchive] = await db
    .select()
    .from(ippMarketRedmonthResults)
    .where(
      and(
        eq(ippMarketRedmonthResults.marketId, marketId),
        eq(ippMarketRedmonthResults.redPeriodStart, periodStartYmd),
        eq(ippMarketRedmonthResults.isDeleted, false),
      ),
    )
    .limit(1);
  assert.equal(Number(updatedArchive?.marketIpp ?? 0), 10);

  const afterRes = await request(app).get(`/admin/ipp/${marketId}/detail`).query({ periodStart: periodStartYmd });
  assert.equal(afterRes.status, 200);
  assert.equal(Number(afterRes.body.record?.marketIpp ?? 0), 10);

  delete process.env.BYPASS_AUTH_ROLE;
  delete process.env.BYPASS_AUTH_USER_ID;
});

test("GET /admin/ipp remains side-effect free", async (t) => {
  enableTestAuthBypass();
  setTestBypassRole("admin");
  if (!(await ensureDbReadyForIppTests())) {
    t.skip("IPP tables are not present in the configured DATABASE_URL.");
    return;
  }

  const app = createApp();
  const now = new Date();
  const submittedAt = new Date(2026, 0, 10, 9, 0, 0);
  const period = getRedPeriodForDate(submittedAt);
  const periodStartYmd = toYmd(period.start);

  const gmUserId = randomUUID();
  const marketId = randomUUID();
  const campaignId = randomUUID();
  const sessionId = randomUUID();

  await db.insert(users).values({
    id: gmUserId,
    supabaseAuthId: `auth-${gmUserId}`,
    email: `ipp-read-${gmUserId}@example.com`,
    role: "gm",
    firstName: "Read",
    lastName: "Only",
    isActive: true,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(markets).values({
    id: marketId,
    name: "IPP ReadOnly Markt",
    dbName: "spar",
    address: "Readweg 10",
    postalCode: "4020",
    city: "Linz",
    region: "West",
    currentGmName: "Read Only",
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(campaigns).values({
    id: campaignId,
    name: "IPP ReadOnly Kampagne",
    section: "standard",
    status: "active",
    scheduleType: "always",
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessions).values({
    id: sessionId,
    gmUserId,
    marketId,
    status: "submitted",
    startedAt: submittedAt,
    submittedAt,
    lastSavedAt: submittedAt,
    isDeleted: false,
    createdAt: now,
    updatedAt: now,
  });

  const beforeRows = await db
    .select()
    .from(ippMarketRedmonthResults)
    .where(
      and(
        eq(ippMarketRedmonthResults.marketId, marketId),
        eq(ippMarketRedmonthResults.redPeriodStart, periodStartYmd),
        eq(ippMarketRedmonthResults.isDeleted, false),
      ),
    );
  assert.equal(beforeRows.length, 0);

  const response = await request(app).get("/admin/ipp");
  assert.equal(response.status, 200);

  const afterRows = await db
    .select()
    .from(ippMarketRedmonthResults)
    .where(
      and(
        eq(ippMarketRedmonthResults.marketId, marketId),
        eq(ippMarketRedmonthResults.redPeriodStart, periodStartYmd),
        eq(ippMarketRedmonthResults.isDeleted, false),
      ),
    );
  assert.equal(afterRows.length, 0);

  delete process.env.BYPASS_AUTH_ROLE;
  delete process.env.BYPASS_AUTH_USER_ID;
});

test("gm visit duplicate photo commit syncs sibling answers", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Visit session tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  const [sharedQuestion] = await db
    .select({ id: questionBankShared.id })
    .from(questionBankShared)
    .where(eq(questionBankShared.isDeleted, false))
    .limit(1);
  if (!gmUser?.id || !sharedQuestion?.id) {
    t.skip("Missing GM or shared question for duplicate photo sync test.");
    return;
  }

  const now = new Date();
  const uniq = `visit-dup-photo-${Date.now()}`;
  const campaignId = randomUUID();
  const sessionId = randomUUID();
  const sectionAId = randomUUID();
  const sectionBId = randomUUID();
  const questionAId = randomUUID();
  const questionBId = randomUUID();
  const answerAId = randomUUID();
  const photoTagId = randomUUID();

  const [marketRow] = await db
    .insert(markets)
    .values({
      name: `Visit Dup Photo Markt ${uniq}`,
      dbName: "",
      address: "Dup Photo Straße 1",
      postalCode: "1010",
      city: "Wien",
      region: "Wien",
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    })
    .returning({ id: markets.id });
  const marketId = marketRow?.id;
  if (!marketId) {
    t.skip("Could not create market row for duplicate photo sync test.");
    return;
  }

  await db.insert(photoTags).values({
    id: photoTagId,
    label: `DupPhotoTag-${uniq}`,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(campaigns).values({
    id: campaignId,
    name: `Visit Dup Photo Campaign ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessions).values({
    id: sessionId,
    gmUserId: gmUser.id,
    marketId,
    status: "draft",
    startedAt: now,
    lastSavedAt: now,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionSections).values([
    {
      id: sectionAId,
      visitSessionId: sessionId,
      campaignId,
      section: "standard",
      fragebogenId: null,
      fragebogenNameSnapshot: "Dup Photo FB A",
      orderIndex: 0,
      status: "draft",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: sectionBId,
      visitSessionId: sessionId,
      campaignId,
      section: "flex",
      fragebogenId: null,
      fragebogenNameSnapshot: "Dup Photo FB B",
      orderIndex: 1,
      status: "draft",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
  ]);
  await db.insert(visitSessionQuestions).values([
    {
      id: questionAId,
      visitSessionSectionId: sectionAId,
      questionId: sharedQuestion.id,
      moduleId: null,
      moduleNameSnapshot: "Dup Photo Module A",
      questionType: "photo",
      questionTextSnapshot: "Dup Photo A",
      questionConfigSnapshot: { tagsEnabled: true, tagIds: [photoTagId] },
      questionRulesSnapshot: [],
      requiredSnapshot: false,
      orderIndex: 0,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: questionBId,
      visitSessionSectionId: sectionBId,
      questionId: sharedQuestion.id,
      moduleId: null,
      moduleNameSnapshot: "Dup Photo Module B",
      questionType: "photo",
      questionTextSnapshot: "Dup Photo B",
      questionConfigSnapshot: { tagsEnabled: true, tagIds: [photoTagId] },
      questionRulesSnapshot: [],
      requiredSnapshot: false,
      orderIndex: 0,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
  ]);
  await db.insert(visitAnswers).values({
    id: answerAId,
    visitSessionId: sessionId,
    visitSessionSectionId: sectionAId,
    visitSessionQuestionId: questionAId,
    questionId: sharedQuestion.id,
    questionType: "photo",
    answerStatus: "unanswered",
    isValid: true,
    validationError: null,
    answeredAt: null,
    changedAt: now,
    version: 1,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });

  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  const app = createApp();

  const commitRes = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/photos/commit`).send({
    visitAnswerId: answerAId,
    photos: [
      {
        storageBucket: "visit-photos",
        storagePath: `visit-photos/${sessionId}/${answerAId}/sync.jpg`,
        mimeType: "image/jpeg",
        byteSize: 128,
        photoTagIds: [photoTagId],
      },
    ],
  });
  assert.equal(commitRes.status, 200);

  const siblingAnswer = await db
    .select({ id: visitAnswers.id })
    .from(visitAnswers)
    .where(and(eq(visitAnswers.visitSessionQuestionId, questionBId), eq(visitAnswers.isDeleted, false)))
    .limit(1);
  assert.ok(siblingAnswer[0]?.id);

  const siblingPhotos = await db
    .select({ storagePath: visitAnswerPhotos.storagePath })
    .from(visitAnswerPhotos)
    .where(and(eq(visitAnswerPhotos.visitAnswerId, siblingAnswer[0]!.id), eq(visitAnswerPhotos.isDeleted, false)));
  assert.equal(siblingPhotos.length, 1);
  assert.ok(siblingPhotos[0]!.storagePath.startsWith(`visit-photos/${sessionId}/${siblingAnswer[0]!.id}/`));

  const siblingPhotoIds = await db
    .select({ id: visitAnswerPhotos.id })
    .from(visitAnswerPhotos)
    .where(and(eq(visitAnswerPhotos.visitAnswerId, siblingAnswer[0]!.id), eq(visitAnswerPhotos.isDeleted, false)));
  const siblingTags = siblingPhotoIds.length === 0
    ? []
    : await db
        .select({ id: visitAnswerPhotoTags.id })
        .from(visitAnswerPhotoTags)
        .where(and(inArray(visitAnswerPhotoTags.visitAnswerPhotoId, siblingPhotoIds.map((row) => row.id)), eq(visitAnswerPhotoTags.isDeleted, false)));
  assert.ok(siblingTags.length > 0);

  await db.update(visitSessions).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(visitSessions.id, sessionId));
  await db.update(campaigns).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(campaigns.id, campaignId));
  await db.update(photoTags).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(photoTags.id, photoTagId));
  await db.update(markets).set({ isDeleted: true, updatedAt: new Date() }).where(eq(markets.id, marketId));
  delete process.env.BYPASS_AUTH_ROLE;
  delete process.env.BYPASS_AUTH_USER_ID;
});

test("gm visit session create is idempotent for client_session_token", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Visit session tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for visit-session test.");
    return;
  }

  const now = new Date();
  const uniq = `visit-token-${Date.now()}`;
  const campaignId = randomUUID();
  const sectionId = randomUUID();
  const [marketRow] = await db
    .insert(markets)
    .values({
      name: `Visit Token Markt ${uniq}`,
      dbName: "",
      address: "Visit Token Straße 1",
      postalCode: "1010",
      city: "Wien",
      region: "Wien",
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    })
    .returning({ id: markets.id });
  const marketId = marketRow?.id;
  if (!marketId) {
    t.skip("Could not create market row for visit-token test.");
    return;
  }
  await db.insert(campaigns).values({
    id: campaignId,
    name: `Visit Token Campaign ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  const existingSessionId = randomUUID();
  await db.insert(visitSessions).values({
    id: existingSessionId,
    gmUserId: gmUser.id,
    marketId,
    status: "draft",
    startedAt: now,
    lastSavedAt: now,
    clientSessionToken: `idem-${uniq}`,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionSections).values({
    id: sectionId,
    visitSessionId: existingSessionId,
    campaignId,
    section: "standard",
    fragebogenId: null,
    fragebogenNameSnapshot: "Visit Token FB",
    orderIndex: 0,
    status: "draft",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });

  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  const app = createApp();
  const res = await request(app).post("/markets/gm/visit-sessions").send({
    marketId,
    campaignIds: [campaignId],
    clientSessionToken: `idem-${uniq}`,
  });
  assert.equal(res.status, 200);
  assert.equal(res.body.session?.id, existingSessionId);

  await db.update(campaigns).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(campaigns.id, campaignId));
  await db.update(visitSessions).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(visitSessions.id, existingSessionId));
  await db.update(markets).set({ isDeleted: true, updatedAt: new Date() }).where(eq(markets.id, marketId));
  delete process.env.BYPASS_AUTH_ROLE;
  delete process.env.BYPASS_AUTH_USER_ID;
});

test("gm visit session token mismatch returns 409", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Visit session tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  if (!gmUser?.id) {
    t.skip("No active GM user available for visit-token mismatch test.");
    return;
  }

  const now = new Date();
  const uniq = `visit-token-mismatch-${Date.now()}`;
  const existingCampaignId = randomUUID();
  const requestedCampaignId = randomUUID();
  const existingSectionId = randomUUID();
  const existingSessionId = randomUUID();
  const [marketRowA] = await db
    .insert(markets)
    .values({
      name: `Visit Token Mismatch Markt A ${uniq}`,
      dbName: "",
      address: "Visit Token Straße A",
      postalCode: "1010",
      city: "Wien",
      region: "Wien",
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    })
    .returning({ id: markets.id });
  const [marketRowB] = await db
    .insert(markets)
    .values({
      name: `Visit Token Mismatch Markt B ${uniq}`,
      dbName: "",
      address: "Visit Token Straße B",
      postalCode: "1010",
      city: "Wien",
      region: "Wien",
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    })
    .returning({ id: markets.id });
  const marketIdA = marketRowA?.id;
  const marketIdB = marketRowB?.id;
  if (!marketIdA || !marketIdB) {
    t.skip("Could not create market rows for token mismatch test.");
    return;
  }
  await db.insert(campaigns).values([
    {
      id: existingCampaignId,
      name: `Visit Token Existing Campaign ${uniq}`,
      section: "standard",
      status: "active",
      scheduleType: "always",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
    {
      id: requestedCampaignId,
      name: `Visit Token Requested Campaign ${uniq}`,
      section: "standard",
      status: "active",
      scheduleType: "always",
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    },
  ]);
  await db.insert(visitSessions).values({
    id: existingSessionId,
    gmUserId: gmUser.id,
    marketId: marketIdA,
    status: "draft",
    startedAt: now,
    lastSavedAt: now,
    clientSessionToken: `idem-mismatch-${uniq}`,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionSections).values({
    id: existingSectionId,
    visitSessionId: existingSessionId,
    campaignId: existingCampaignId,
    section: "standard",
    fragebogenId: null,
    fragebogenNameSnapshot: "Visit Token FB",
    orderIndex: 0,
    status: "draft",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });

  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  const app = createApp();

  const marketMismatch = await request(app).post("/markets/gm/visit-sessions").send({
    marketId: marketIdB,
    campaignIds: [existingCampaignId],
    clientSessionToken: `idem-mismatch-${uniq}`,
  });
  assert.equal(marketMismatch.status, 409);
  assert.equal(marketMismatch.body.code, "visit_session_token_mismatch");

  const campaignMismatch = await request(app).post("/markets/gm/visit-sessions").send({
    marketId: marketIdA,
    campaignIds: [requestedCampaignId],
    clientSessionToken: `idem-mismatch-${uniq}`,
  });
  assert.equal(campaignMismatch.status, 409);
  assert.equal(campaignMismatch.body.code, "visit_session_token_mismatch");

  await db.update(campaigns).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(inArray(campaigns.id, [existingCampaignId, requestedCampaignId]));
  await db.update(visitSessions).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(visitSessions.id, existingSessionId));
  await db.update(markets).set({ isDeleted: true, updatedAt: new Date() }).where(inArray(markets.id, [marketIdA, marketIdB]));
  delete process.env.BYPASS_AUTH_ROLE;
  delete process.env.BYPASS_AUTH_USER_ID;
});

test("gm visit resume payload includes answer options matrix photos and tags", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Visit session tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  const [sharedQuestion] = await db
    .select({ id: questionBankShared.id })
    .from(questionBankShared)
    .where(eq(questionBankShared.isDeleted, false))
    .limit(1);
  if (!gmUser?.id || !sharedQuestion?.id) {
    t.skip("Missing GM user or shared question for resume payload test.");
    return;
  }

  const now = new Date();
  const uniq = `visit-resume-${Date.now()}`;
  const campaignId = randomUUID();
  const sessionId = randomUUID();
  const sectionId = randomUUID();
  const questionId = randomUUID();
  const photoTagId = randomUUID();

  const [marketRow] = await db
    .insert(markets)
    .values({
      name: `Visit Resume Markt ${uniq}`,
      dbName: "",
      address: "Visit Resume Straße 1",
      postalCode: "1010",
      city: "Wien",
      region: "Wien",
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    })
    .returning({ id: markets.id });
  const marketId = marketRow?.id;
  if (!marketId) {
    t.skip("Could not create market row for visit-resume test.");
    return;
  }
  await db.insert(campaigns).values({
    id: campaignId,
    name: `Visit Resume Campaign ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(photoTags).values({
    id: photoTagId,
    label: `Visit Resume Tag ${uniq}`,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessions).values({
    id: sessionId,
    gmUserId: gmUser.id,
    marketId,
    status: "draft",
    startedAt: now,
    lastSavedAt: now,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionSections).values({
    id: sectionId,
    visitSessionId: sessionId,
    campaignId,
    section: "standard",
    fragebogenId: null,
    fragebogenNameSnapshot: "Visit Resume FB",
    orderIndex: 0,
    status: "draft",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionQuestions).values({
    id: questionId,
    visitSessionSectionId: sectionId,
    questionId: sharedQuestion.id,
    moduleId: null,
    moduleNameSnapshot: "Visit Resume Module",
    questionType: "photo",
    questionTextSnapshot: "Visit Resume Foto",
    questionConfigSnapshot: { tagsEnabled: true, tagIds: [photoTagId] },
    questionRulesSnapshot: [],
    requiredSnapshot: true,
    orderIndex: 0,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  const answerId = randomUUID();
  await db.insert(visitAnswers).values({
    id: answerId,
    visitSessionId: sessionId,
    visitSessionSectionId: sectionId,
    visitSessionQuestionId: questionId,
    questionId: sharedQuestion.id,
    questionType: "photo",
    answerStatus: "answered",
    valueJson: { storage: [{ path: `visit-photos/${sessionId}/${answerId}/resume.jpg` }] },
    isValid: true,
    validationError: null,
    answeredAt: now,
    changedAt: now,
    version: 2,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitAnswerOptions).values({
    visitAnswerId: answerId,
    optionRole: "sub",
    optionValue: "unused",
    orderIndex: 0,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  const [photoRow] = await db.insert(visitAnswerPhotos).values({
    visitAnswerId: answerId,
    storageBucket: "visit-photos",
    storagePath: `visit-photos/${sessionId}/${answerId}/resume.jpg`,
    mimeType: "image/jpeg",
    byteSize: 44,
    uploadedAt: now,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  }).returning({ id: visitAnswerPhotos.id });
  if (photoRow?.id) {
    await db.insert(visitAnswerPhotoTags).values({
      visitAnswerPhotoId: photoRow.id,
      photoTagId,
      photoTagLabelSnapshot: `Visit Resume Tag ${uniq}`,
      isDeleted: false,
      deletedAt: null,
      createdAt: now,
      updatedAt: now,
    });
  }
  await db.insert(visitQuestionComments).values({
    visitSessionQuestionId: questionId,
    commentText: "Resume comment",
    commentedAt: now,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });

  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  const app = createApp();
  const resumeRes = await request(app).get(`/markets/gm/visit-sessions/${sessionId}`);
  assert.equal(resumeRes.status, 200);
  assert.equal(resumeRes.body.market?.id, marketId);
  assert.equal(resumeRes.body.sections?.[0]?.campaignName, `Visit Resume Campaign ${uniq}`);
  assert.equal(resumeRes.body.sections?.[0]?.questions?.[0]?.answer?.photos?.length, 1);
  assert.equal(resumeRes.body.sections?.[0]?.questions?.[0]?.answer?.photos?.[0]?.tags?.length, 1);
  assert.equal(resumeRes.body.sections?.[0]?.questions?.[0]?.comment, "Resume comment");
  assert.equal(Array.isArray(resumeRes.body.sections?.[0]?.questions?.[0]?.config?.tagMeta), true);

  await db.update(visitSessions).set({ status: "submitted", submittedAt: new Date(), updatedAt: new Date() }).where(eq(visitSessions.id, sessionId));
  const postSubmitPatch = await request(app).patch(`/markets/gm/visit-sessions/${sessionId}/answers`).send({
    visitQuestionId: questionId,
    answer: "nope",
  });
  assert.equal(postSubmitPatch.status, 409);
  const postSubmitPresign = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/photos/presign`).send({
    visitAnswerId: answerId,
    extension: "jpg",
  });
  assert.ok([404, 409].includes(postSubmitPresign.status));
  const postSubmitCommit = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/photos/commit`).send({
    visitAnswerId: answerId,
    photos: [],
  });
  assert.ok([404, 409].includes(postSubmitCommit.status));

  await db.update(visitSessions).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(visitSessions.id, sessionId));
  await db.update(campaigns).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(campaigns.id, campaignId));
  await db.update(photoTags).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(photoTags.id, photoTagId));
  await db.update(markets).set({ isDeleted: true, updatedAt: new Date() }).where(eq(markets.id, marketId));
  delete process.env.BYPASS_AUTH_ROLE;
  delete process.env.BYPASS_AUTH_USER_ID;
});

test("gm visit submit rejects required photo when only answer row exists without photo artifacts", async (t) => {
  enableTestAuthBypass();
  if (!(await ensureDbReadyForVisitSessionTests())) {
    t.skip("Visit session tables are not present in the configured DATABASE_URL.");
    return;
  }
  const [gmUser] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.role, "gm"), eq(users.isActive, true), isNull(users.deletedAt)))
    .limit(1);
  const [sharedQuestion] = await db
    .select({ id: questionBankShared.id })
    .from(questionBankShared)
    .where(eq(questionBankShared.isDeleted, false))
    .limit(1);
  if (!gmUser?.id || !sharedQuestion?.id) {
    t.skip("Missing GM user or shared question for artifact submit test.");
    return;
  }
  const now = new Date();
  const uniq = `visit-artifact-${Date.now()}`;
  const campaignId = randomUUID();
  const sessionId = randomUUID();
  const sectionId = randomUUID();
  const photoQuestionId = randomUUID();

  const [marketRow] = await db
    .insert(markets)
    .values({
      name: `Visit Artifact Markt ${uniq}`,
      dbName: "",
      address: "Visit Artifact Straße 1",
      postalCode: "1010",
      city: "Wien",
      region: "Wien",
      isDeleted: false,
      createdAt: now,
      updatedAt: now,
    })
    .returning({ id: markets.id });
  const marketId = marketRow?.id;
  if (!marketId) {
    t.skip("Could not create market row for artifact submit test.");
    return;
  }
  await db.insert(campaigns).values({
    id: campaignId,
    name: `Visit Artifact Campaign ${uniq}`,
    section: "standard",
    status: "active",
    scheduleType: "always",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessions).values({
    id: sessionId,
    gmUserId: gmUser.id,
    marketId,
    status: "draft",
    startedAt: now,
    lastSavedAt: now,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionSections).values({
    id: sectionId,
    visitSessionId: sessionId,
    campaignId,
    section: "standard",
    fragebogenId: null,
    fragebogenNameSnapshot: "Visit Artifact FB",
    orderIndex: 0,
    status: "draft",
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitSessionQuestions).values({
    id: photoQuestionId,
    visitSessionSectionId: sectionId,
    questionId: sharedQuestion.id,
    moduleId: null,
    moduleNameSnapshot: "Module",
    questionType: "photo",
    questionTextSnapshot: "Required Photo",
    questionConfigSnapshot: { tagsEnabled: false, tagIds: [] },
    questionRulesSnapshot: [],
    requiredSnapshot: true,
    orderIndex: 0,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });
  await db.insert(visitAnswers).values({
    visitSessionId: sessionId,
    visitSessionSectionId: sectionId,
    visitSessionQuestionId: photoQuestionId,
    questionId: sharedQuestion.id,
    questionType: "photo",
    answerStatus: "answered",
    valueJson: { storage: [{ path: "visit-photos/fake-path.jpg" }] },
    isValid: true,
    validationError: null,
    answeredAt: now,
    changedAt: now,
    version: 1,
    isDeleted: false,
    deletedAt: null,
    createdAt: now,
    updatedAt: now,
  });

  setTestBypassRole("gm");
  setTestBypassUserId(gmUser.id);
  const app = createApp();
  const submitRes = await request(app).post(`/markets/gm/visit-sessions/${sessionId}/submit`).send({});
  assert.equal(submitRes.status, 409);
  assert.equal(submitRes.body.code, "visit_submit_incomplete_required");

  await db.update(visitSessions).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(visitSessions.id, sessionId));
  await db.update(campaigns).set({ isDeleted: true, deletedAt: new Date(), updatedAt: new Date() }).where(eq(campaigns.id, campaignId));
  await db.update(markets).set({ isDeleted: true, updatedAt: new Date() }).where(eq(markets.id, marketId));
  delete process.env.BYPASS_AUTH_ROLE;
  delete process.env.BYPASS_AUTH_USER_ID;
});
