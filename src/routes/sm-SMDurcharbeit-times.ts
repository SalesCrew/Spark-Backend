import { and, asc, desc, eq, inArray, sql } from "drizzle-orm";
import { Router, type Response } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { smSMDurcharbeitVisits as visits, smSMDurcharbeitTargets as targets, smSMDurcharbeitPeriods as periods,
  smSMDurcharbeitCampaigns as campaigns, smSMDurcharbeitTimeRevisions as times, smSMDurcharbeitTimeRequests as requests,
  smQuestionnaireSubmissions as submissions } from "../lib/schema.js";
import { isIsoDate, isoDateToEpochDay } from "../sm-planning.shared.js";
import { SMDurcharbeitCampaignError, SMDurcharbeitToday, SMDurcharbeitEvent, loadSMDurcharbeitTarget,
  lockSMDurcharbeitTarget, saveSMDurcharbeitVisitTime, type SMDurcharbeitTx } from "../sm-SMDurcharbeit-campaign.shared.js";
import { assertSmVisitTimeAvailable, lockSmVisitTimes, SmTimeOverlapError } from "../sm-time-overlap.js";

const uuid = z.string().uuid();
const instant = z.string().datetime({ offset: true });
const reason = z.string().trim().min(3).max(2000);
const range = z.object({ from: z.string().refine(isIsoDate), to: z.string().refine(isIsoDate), smUserId: uuid.optional() }).strict()
  .refine(input => input.to >= input.from && isoDateToEpochDay(input.to) - isoDateToEpochDay(input.from) < 93, "Zeitraum maximal 93 Tage.");
const correction = z.object({ expectedVisitId: uuid, expectedRevision: z.number().int().positive(), expectedStartedAt: instant,
  expectedCompletedAt: instant, visitStartedAt: instant, visitCompletedAt: instant, reason }).strict();
const requestInput = z.object({ expectedRevision: z.number().int().positive(), kind: z.enum(["time_change", "deletion"]),
  requestedStartedAt: instant.nullable(), requestedCompletedAt: instant.nullable(), reason,
  clientRequestToken: z.string().trim().min(8).max(300) }).strict();
const review = z.object({ adminNote: z.string().trim().max(2000).optional() }).strict();
const historyQuery = z.object({ beforeRevision: z.coerce.number().int().positive().optional(), limit: z.coerce.number().int().min(1).max(100).default(30) }).strict();
const fail = (code: string, message: string, status = 409): never => { throw new SMDurcharbeitCampaignError(status, code, message); };
const same = (a: Date | null, b: string | null) => a?.toISOString() === (b ? new Date(b).toISOString() : undefined);
function send(error: unknown, res: Response) {
  if (error instanceof z.ZodError) { res.status(400).json({ error: "Bitte die Zeiteingaben prüfen.", code: "smdurcharbeit_time_input_invalid" }); return true; }
  if (error instanceof SMDurcharbeitCampaignError || error instanceof SmTimeOverlapError) {
    res.status(error.statusCode).json({ error: error.message, code: error.code, ...(error instanceof SmTimeOverlapError ? { details: error.details } : {}) }); return true;
  }
  return false;
}

async function lockedVisit(tx: SMDurcharbeitTx, visitId: string) {
  const [visit] = await tx.select().from(visits).where(eq(visits.id, visitId)).limit(1);
  if (!visit) return fail("smdurcharbeit_time_visit_missing", "Besuch nicht gefunden.", 404);
  await lockSMDurcharbeitTarget(tx, visit.targetId);
  await lockSmVisitTimes(tx, visit.smUserId);
  const context = await loadSMDurcharbeitTarget(tx, visit.targetId);
  const [submission] = await tx.select().from(submissions).where(eq(submissions.SMDurcharbeitVisitId, visitId)).limit(1).for("update");
  const [current] = await tx.select().from(times).where(and(eq(times.visitId, visitId), eq(times.isCurrent, true))).limit(1).for("update");
  if (!submission || !current) return fail("smdurcharbeit_time_missing", "Keine aktuelle Besuchszeit vorhanden.");
  return { visit, submission, current, context };
}
async function validateInterval(tx: SMDurcharbeitTx, state: Awaited<ReturnType<typeof lockedVisit>>, start: string, end: string) {
  const startedAt = new Date(start), completedAt = new Date(end), elapsed = completedAt.getTime() - startedAt.getTime();
  if (elapsed < 60_000 || elapsed > 86_400_000) return fail("smdurcharbeit_time_interval_invalid", "Besuchszeit muss zwischen einer Minute und 24 Stunden liegen.", 400);
  // Business-month identity never changes when a physical clock is corrected.
  const firstDay = SMDurcharbeitToday(startedAt), lastDay = SMDurcharbeitToday(new Date(completedAt.getTime() - 1));
  if (firstDay.slice(0, 7) !== state.context.period.month.slice(0, 7) || lastDay.slice(0, 7) !== state.context.period.month.slice(0, 7)
    || firstDay < state.context.campaign.startDate || lastDay > state.context.campaign.endDate)
    return fail("smdurcharbeit_time_month_mismatch", "Start und Ende müssen im ursprünglichen Kampagnenmonat liegen.");
  await assertSmVisitTimeAvailable(tx, { smUserId: state.visit.smUserId, SMDurcharbeitVisitId: state.visit.id, startedAt, completedAt });
  return { startedAt, completedAt };
}
function requestDto(row: typeof requests.$inferSelect, source: typeof times.$inferSelect) {
  return { id: row.id, assignmentId: null, SMDurcharbeitVisitId: row.visitId, smUserId: row.smUserId,
    sourceTimeSubmissionId: source.id, expectedRevision: row.expectedRevision, kind: row.kind, originalMinutes: source.actualMinutes,
    requestedMinutes: row.startedAt && row.completedAt ? Math.round((row.completedAt.getTime() - row.startedAt.getTime()) / 60000) : null,
    timestampCorrectionVersion: 1, originalStartedAt: source.startedAt.toISOString(), originalCompletedAt: source.completedAt.toISOString(),
    requestedStartedAt: row.startedAt?.toISOString() ?? null, requestedCompletedAt: row.completedAt?.toISOString() ?? null,
    reason: row.reason, status: row.status, reviewedByUserId: row.reviewedByUserId, reviewedAt: row.reviewedAt?.toISOString() ?? null,
    adminNote: row.adminNote, appliedTimeSubmissionId: null, appliedAt: row.reviewedAt?.toISOString() ?? null,
    createdAt: row.createdAt.toISOString(), updatedAt: (row.reviewedAt ?? row.createdAt).toISOString() };
}
async function serializedResult(result: { request: typeof requests.$inferSelect; replayed: boolean }) {
  const [source] = await db.select().from(times).where(and(eq(times.visitId, result.request.visitId),eq(times.revisionNumber,result.request.expectedRevision))).limit(1);
  if (!source) return fail("smdurcharbeit_time_source_missing", "Der ursprüngliche Zeitstand fehlt.");
  return { ...result, request: requestDto(result.request, source) };
}

async function listTimes(input: z.infer<typeof range>, ownUserId?: string) {
  const workDate = sql<string>`(coalesce(${times.startedAt},${submissions.visitStartedAt}) at time zone 'Europe/Vienna')::date::text`;
  const filters = [sql`${workDate} between ${input.from} and ${input.to}`, sql`exists(select 1 from public.sm_smdurcharbeit_visit_time_revisions old_time where old_time.visit_id = ${visits.id})`];
  if (ownUserId || input.smUserId) filters.push(eq(visits.smUserId, ownUserId ?? input.smUserId!));
  const rows = await db.select({ visitId: visits.id, submissionId: submissions.id, smUserId: visits.smUserId,
    smName: submissions.smNameSnapshot, marketId: submissions.smMarketId, marketName: submissions.marketNameSnapshot,
    marketAddress: submissions.marketAddressSnapshot, marketInternalId: sql<string>`coalesce(${targets.marketSnapshot}->>'internalId','')`,
    workDate, campaignId: campaigns.id, campaignName: campaigns.name, month: periods.month, targetId: targets.id,
    time: times, originalStartedAt: submissions.visitStartedAt, originalCompletedAt: submissions.visitCompletedAt, submittedAt: submissions.submittedAt,
    questionnaireComplete: sql<boolean>`${submissions.status} = 'submitted' and ${submissions.isCurrent} and not ${submissions.isDeleted}`,
  }).from(visits).innerJoin(submissions, eq(submissions.SMDurcharbeitVisitId, visits.id)).innerJoin(targets, eq(targets.id, visits.targetId))
    .innerJoin(periods, eq(periods.id, targets.periodId)).innerJoin(campaigns, eq(campaigns.id, targets.campaignId))
    .leftJoin(times, and(eq(times.visitId, visits.id), eq(times.isCurrent, true))).where(and(...filters)).orderBy(desc(workDate), asc(visits.id));
  const pending = rows.length ? await db.select().from(requests).where(and(inArray(requests.visitId, rows.map(row => row.visitId)), eq(requests.status, "pending"))) : [];
  const sourceTimes = pending.length ? await db.select().from(times).where(inArray(times.visitId, pending.map(row => row.visitId))) : [];
  const sourceByRevision = new Map(sourceTimes.map(row => [`${row.visitId}:${row.revisionNumber}`, row]));
  const byVisit = new Map(pending.map(row => [row.visitId, row]));
  return rows.map(({ time, ...row }) => ({ ...row, startedAt: time?.startedAt.toISOString() ?? row.originalStartedAt?.toISOString() ?? null,
    completedAt: time?.completedAt.toISOString() ?? row.originalCompletedAt?.toISOString() ?? null,
    originalStartedAt: row.originalStartedAt?.toISOString() ?? null, originalCompletedAt: row.originalCompletedAt?.toISOString() ?? null,
    actualMinutes: time?.actualMinutes ?? null, travelMinutes: time?.travelMinutes ?? 0, revision: time?.revisionNumber ?? null,
    pendingTimeChangeRequest: byVisit.has(row.visitId) && sourceByRevision.has(`${row.visitId}:${byVisit.get(row.visitId)!.expectedRevision}`)
      ? requestDto(byVisit.get(row.visitId)!, sourceByRevision.get(`${row.visitId}:${byVisit.get(row.visitId)!.expectedRevision}`)!) : null }));
}

export const SMDurcharbeitTimesRouter = Router();
export const adminSMDurcharbeitTimesRouter = Router();
SMDurcharbeitTimesRouter.use(requireAuth(["sm"]));
adminSMDurcharbeitTimesRouter.use(requireAuth(["admin", "sm_admin"]));
SMDurcharbeitTimesRouter.get("/", async (req: AuthedRequest, res, next) => {
  try { const input = range.parse(req.query); if (input.smUserId) return fail("smdurcharbeit_time_input_invalid", "Der SM wird aus der Anmeldung bestimmt.", 400); res.json({ entries: await listTimes(input, req.authUser!.appUserId) }); } catch (error) { if (!send(error, res)) next(error); }
});
adminSMDurcharbeitTimesRouter.get("/", async (req, res, next) => {
  try { res.json({ entries: await listTimes(range.parse(req.query)) }); } catch (error) { if (!send(error, res)) next(error); }
});
for (const [router, own] of [[SMDurcharbeitTimesRouter, true], [adminSMDurcharbeitTimesRouter, false]] as const) router.get("/:visitId/history", async (req: AuthedRequest, res, next) => {
  try {
    const visitId = uuid.parse(req.params.visitId), input = historyQuery.parse(req.query);
    const [visit] = await db.select().from(visits).where(and(eq(visits.id, visitId), ...(own ? [eq(visits.smUserId, req.authUser!.appUserId)] : []))).limit(1);
    if (!visit) return fail("smdurcharbeit_time_visit_missing", "Besuch nicht gefunden.", 404);
    const [original] = await db.select({ startedAt: submissions.visitStartedAt, completedAt: submissions.visitCompletedAt }).from(submissions).where(eq(submissions.SMDurcharbeitVisitId, visitId)).limit(1);
    const [current, page] = await Promise.all([
      db.select({ hasCurrent: sql<boolean>`coalesce(bool_or(${times.isCurrent}), false)`, count: sql<number>`count(*)::int` }).from(times).where(eq(times.visitId, visitId)),
      db.select({ revision: times.revisionNumber, startedAt: times.startedAt, completedAt: times.completedAt, actualMinutes: times.actualMinutes,
        travelMinutes: times.travelMinutes, reason: times.reason, recordedAt: times.createdAt, isCurrent: times.isCurrent }).from(times)
        .where(and(eq(times.visitId, visitId), ...(input.beforeRevision ? [sql`${times.revisionNumber} < ${input.beforeRevision}`] : [])))
        .orderBy(desc(times.revisionNumber)).limit(input.limit + 1),
    ]);
    const revisions = page.slice(0, input.limit);
    res.json({ visitId, originalStartedAt: original?.startedAt?.toISOString() ?? null, originalCompletedAt: original?.completedAt?.toISOString() ?? null,
      timeRemoved: Boolean(current[0]?.count && !current[0].hasCurrent), revisions, nextRevision: page.length > input.limit ? revisions.at(-1)!.revision : null });
  } catch (error) { if (!send(error, res)) next(error); }
});
adminSMDurcharbeitTimesRouter.patch("/:visitId", async (req: AuthedRequest, res, next) => {
  try {
    const visitId = uuid.parse(req.params.visitId), input = correction.parse(req.body), actor = req.authUser!.appUserId;
    const time = await db.transaction(async tx => {
      const state = await lockedVisit(tx, visitId);
      if (state.submission.id !== input.expectedVisitId || state.current.revisionNumber !== input.expectedRevision || !same(state.current.startedAt, input.expectedStartedAt) || !same(state.current.completedAt, input.expectedCompletedAt))
        return fail("smdurcharbeit_time_stale", "Die Besuchszeit wurde geändert. Bitte den aktuellen Stand laden.");
      const interval = await validateInterval(tx, state, input.visitStartedAt, input.visitCompletedAt);
      if (same(state.current.startedAt, input.visitStartedAt) && same(state.current.completedAt, input.visitCompletedAt)) return fail("smdurcharbeit_time_unchanged", "Start und Ende sind unverändert.", 400);
      const saved = await saveSMDurcharbeitVisitTime(tx, visitId, actor, { ...interval, travelMinutes: state.current.travelMinutes, reason: input.reason, expectedRevision: input.expectedRevision });
      await SMDurcharbeitEvent(tx, { campaignId: state.context.campaign.id, targetId: state.visit.targetId, visitId, actorUserId: actor, action: "time_corrected", reason: input.reason,
        beforeState: { timeRevisionId: state.current.id }, afterState: { timeRevisionId: saved.id } });
      return saved;
    });
    res.json({ time });
  } catch (error) { if (!send(error, res)) next(error); }
});
SMDurcharbeitTimesRouter.post("/:visitId/requests", async (req: AuthedRequest, res, next) => {
  try {
    const visitId = uuid.parse(req.params.visitId), input = requestInput.parse(req.body), actor = req.authUser!.appUserId;
    const result = await db.transaction(async tx => {
      const [identity] = await tx.select().from(visits).where(eq(visits.id, visitId)).limit(1);
      if (!identity || identity.smUserId !== actor) return fail("smdurcharbeit_time_forbidden", "Dieser Besuch gehört einem anderen SM.", 403);
      await lockSMDurcharbeitTarget(tx, identity.targetId); await lockSmVisitTimes(tx, actor);
      const [replayed] = await tx.select().from(requests).where(and(eq(requests.smUserId, actor), eq(requests.clientToken, input.clientRequestToken))).limit(1);
      if (replayed) {
        if (replayed.visitId !== visitId || replayed.kind !== input.kind || replayed.expectedRevision !== input.expectedRevision || replayed.reason !== input.reason || !same(replayed.startedAt, input.requestedStartedAt) || !same(replayed.completedAt, input.requestedCompletedAt))
          return fail("smdurcharbeit_time_token_conflict", "Dieser Anfrage-Token wurde bereits mit anderen Daten verwendet.");
        return { request: replayed, replayed: true };
      }
      const state = await lockedVisit(tx, visitId);
      if (state.current.revisionNumber !== input.expectedRevision) return fail("smdurcharbeit_time_stale", "Die Besuchszeit wurde geändert. Bitte neu laden.");
      const [pending] = await tx.select().from(requests).where(and(eq(requests.visitId, visitId), eq(requests.status, "pending"))).limit(1);
      if (pending) return { request: pending, replayed: true };
      if (input.kind === "deletion" && (input.requestedStartedAt !== null || input.requestedCompletedAt !== null)) return fail("smdurcharbeit_time_input_invalid", "Löschanfragen dürfen keine neuen Zeiten enthalten.", 400);
      if (input.kind === "time_change") {
        if (!input.requestedStartedAt || !input.requestedCompletedAt) return fail("smdurcharbeit_time_input_invalid", "Bitte Start und Ende angeben.", 400);
        await validateInterval(tx, state, input.requestedStartedAt, input.requestedCompletedAt);
        if (same(state.current.startedAt, input.requestedStartedAt) && same(state.current.completedAt, input.requestedCompletedAt)) return fail("smdurcharbeit_time_unchanged", "Start und Ende sind unverändert.", 400);
      }
      const [created] = await tx.insert(requests).values({ visitId, smUserId: actor, expectedRevision: input.expectedRevision, kind: input.kind,
        startedAt: input.requestedStartedAt ? new Date(input.requestedStartedAt) : null, completedAt: input.requestedCompletedAt ? new Date(input.requestedCompletedAt) : null, reason: input.reason, clientToken: input.clientRequestToken }).returning();
      return { request: created!, replayed: false };
    }); res.status(result.replayed ? 200 : 201).json(await serializedResult(result));
  } catch (error) { if (!send(error, res)) next(error); }
});
for (const decision of ["approve", "reject"] as const) adminSMDurcharbeitTimesRouter.post("/requests/:requestId/" + decision, async (req: AuthedRequest, res, next) => {
  try {
    const requestId = uuid.parse(req.params.requestId), input = review.parse(req.body ?? {}), actor = req.authUser!.appUserId;
    const result = await db.transaction(async tx => {
      const [identity] = await tx.select().from(requests).where(eq(requests.id, requestId)).limit(1);
      if (!identity) return fail("smdurcharbeit_time_request_missing", "Anfrage nicht gefunden.", 404);
      // Rejection remains possible after a time was removed or superseded.
      const [visit] = await tx.select().from(visits).where(eq(visits.id, identity.visitId)).limit(1);
      if (!visit) return fail("smdurcharbeit_time_visit_missing", "Besuch nicht gefunden.", 404);
      await lockSMDurcharbeitTarget(tx, visit.targetId); await lockSmVisitTimes(tx, visit.smUserId);
      const [request] = await tx.select().from(requests).where(eq(requests.id, requestId)).limit(1).for("update");
      const status = decision === "approve" ? "approved" : "rejected";
      if (request!.status === status) return { request: request!, replayed: true };
      if (request!.status !== "pending") return fail("smdurcharbeit_time_request_closed", "Diese Anfrage wurde bereits bearbeitet.");
      const context = await loadSMDurcharbeitTarget(tx, visit.targetId);
      if (decision === "approve") {
        const state = await lockedVisit(tx, visit.id);
        if (state.current.revisionNumber !== request!.expectedRevision || state.visit.smUserId !== request!.smUserId) return fail("smdurcharbeit_time_request_stale", "Die Zeit wurde seit der Anfrage geändert. Bitte die veraltete Anfrage ablehnen.");
        if (request!.kind === "time_change") {
          const interval = await validateInterval(tx, state, request!.startedAt!.toISOString(), request!.completedAt!.toISOString());
          await saveSMDurcharbeitVisitTime(tx, visit.id, actor, { ...interval, travelMinutes: state.current.travelMinutes, reason: request!.reason, expectedRevision: request!.expectedRevision });
        } else await tx.update(times).set({ isCurrent: false }).where(eq(times.id, state.current.id));
      }
      const [updated] = await tx.update(requests).set({ status, reviewedByUserId: actor, reviewedAt: new Date(), adminNote: input.adminNote || null }).where(eq(requests.id, requestId)).returning();
      await SMDurcharbeitEvent(tx, { campaignId: context.campaign.id, targetId: visit.targetId, visitId: visit.id, actorUserId: actor, action: `time_request_${status}`, reason: request!.reason,
        afterState: { requestId, kind: request!.kind } });
      return { request: updated!, replayed: false };
    }); res.json(await serializedResult(result));
  } catch (error) { if (!send(error, res)) next(error); }
});
