import { createHash } from "node:crypto";
import { and, asc, desc, eq, inArray, sql } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { db } from "./lib/db.js";
import { smAssignments, smAssignmentSeries, smAssignmentSeriesVersions, smAssignmentEvents, smAssignmentTimeSubmissions, smQuestionnaireSubmissions, smMarkets, users } from "./lib/schema.js";
import type { AuthedRequest } from "./middleware/auth.js";
import { lockSmPlanning } from "./sm-planning-lock.js";
import { adjustSmHolidayAssignments, smHolidayToday } from "./sm-holiday-planning.js";
import { isIsoDate, isoDateToEpochDay, normalizeWeekdays, resolveSmAssignmentValues } from "./sm-planning.shared.js";
import { planSmSeriesChange } from "./sm-series.shared.js";

type Tx = Parameters<Parameters<typeof db.transaction>[0]>[0];
const date = z.string().refine(isIsoDate);
const edit = z.object({ action: z.literal("edit"), effectiveFromDate: date,
  smMarketId: z.string().uuid(), smUserId: z.string().uuid(), plannedMinutes: z.number().int().min(1).max(1440),
  frequency: z.enum(["weekly", "biweekly"]), weekdays: z.array(z.number().int().min(1).max(7)).min(1).max(7), validTo: date }).strict();
const stop = z.object({ action: z.literal("stop"), effectiveFromDate: date }).strict();
export const smSeriesChangeSchema = z.discriminatedUnion("action", [edit, stop]);
const mutationSchema = z.object({ change: smSeriesChangeSchema, previewToken: z.string().regex(/^[a-f0-9]{64}$/), reason: z.string().trim().min(3).max(2000) }).strict();
type Change = z.infer<typeof smSeriesChangeSchema>;

export class SmSeriesError extends Error {
  constructor(public status: number, public code: string, message: string) { super(message); }
}
function fail(status: number, code: string, message: string): never { throw new SmSeriesError(status, code, message); }

async function loadSeries(tx: Tx, id: string) {
  const [series] = await tx.select().from(smAssignmentSeries).where(and(eq(smAssignmentSeries.id, id), eq(smAssignmentSeries.isDeleted, false)));
  if (!series) return fail(404, "sm_series_not_found", "Die Serie wurde nicht gefunden.");
  const [version] = await tx.select().from(smAssignmentSeriesVersions).where(and(eq(smAssignmentSeriesVersions.seriesId, id), eq(smAssignmentSeriesVersions.isDeleted, false))).orderBy(desc(smAssignmentSeriesVersions.versionNumber)).limit(1);
  if (!version) return fail(409, "sm_series_version_missing", "Die Serienversion wurde nicht gefunden.");
  return { series, version };
}

export async function getSmSeriesDetails(tx: Tx, id: string) {
  const { series, version } = await loadSeries(tx, id);
  return { id, status: series.status, versionNumber: version.versionNumber, effectiveFromDate: version.effectiveFromDate,
    smMarketId: version.smMarketId, smUserId: version.defaultSmUserId, plannedMinutes: version.plannedMinutes,
    frequency: version.frequency, weekdays: version.weekdays, validFrom: version.validFrom, validTo: version.validTo,
    today: smHolidayToday() };
}

async function prepare(tx: Tx, id: string, change: Change, today: string, forUpdate = false) {
  const { series, version } = await loadSeries(tx, id);
  if (series.status !== "active") fail(409, "sm_series_ended", "Diese Serie wurde bereits gestoppt. Einzelne Termine bleiben in der Historie erhalten.");
  if (change.effectiveFromDate < today) fail(400, "sm_series_past", "Serien können nur ab heute oder einem zukünftigen Datum geändert werden.");
  let market: { id: string; internalMarketId: string | null } | undefined;
  if (change.action === "edit") {
    if (change.effectiveFromDate < version.effectiveFromDate) fail(409, "sm_series_version_boundary", "Bitte wähle ein Datum ab der neuesten Serienversion.");
    const span = isoDateToEpochDay(change.validTo) - isoDateToEpochDay(change.effectiveFromDate);
    if (span < 0 || span > 731) fail(400, "sm_series_range", "Das Ende muss ab dem Änderungsdatum und innerhalb von zwei Jahren liegen.");
    if (normalizeWeekdays(change.weekdays).length !== change.weekdays.length) fail(400, "sm_series_weekdays", "Wochentage dürfen nicht doppelt gewählt werden.");
    [market] = await tx.select({ id: smMarkets.id, internalMarketId: smMarkets.internalMarketId }).from(smMarkets).where(and(eq(smMarkets.id, change.smMarketId), eq(smMarkets.isDeleted, false), eq(smMarkets.isActive, true)));
    const [sm] = await tx.select({ id: users.id }).from(users).where(and(eq(users.id, change.smUserId), eq(users.role, "sm"), eq(users.isActive, true), sql`${users.deletedAt} is null`));
    if (!market?.internalMarketId || !sm) fail(400, "sm_series_target_invalid", "Ein aktiver SM und ein aktiver SM-Markt mit Stammnummer sind erforderlich.");
  }
  const rowQuery = tx.select().from(smAssignments).where(and(eq(smAssignments.seriesId, id), eq(smAssignments.isDeleted, false))).orderBy(asc(smAssignments.id)).limit(5001);
  const rows = await (forUpdate ? rowQuery.for("update") : rowQuery);
  if (rows.length > 5000) fail(409, "sm_series_too_large", "Diese Serie ist zu groß für eine sichere Sammeländerung.");
  const ids = rows.map((row) => row.id);
  const [times, submissions, cancellations] = ids.length ? await Promise.all([
    tx.select({ assignmentId: smAssignmentTimeSubmissions.assignmentId }).from(smAssignmentTimeSubmissions).where(inArray(smAssignmentTimeSubmissions.assignmentId, ids)),
    tx.select({ assignmentId: smQuestionnaireSubmissions.assignmentId }).from(smQuestionnaireSubmissions).where(inArray(smQuestionnaireSubmissions.assignmentId, ids)),
    tx.select({ assignmentId: smAssignmentEvents.assignmentId, after: smAssignmentEvents.afterState }).from(smAssignmentEvents)
      .where(and(eq(smAssignmentEvents.seriesId, id), eq(smAssignmentEvents.eventType, "cancelled"))).orderBy(desc(smAssignmentEvents.createdAt), desc(smAssignmentEvents.id)),
  ]) : [[], [], []];
  const history = new Set([...times, ...submissions].map((row) => row.assignmentId));
  const latestCancellation = new Map<string, Record<string, unknown>>();
  for (const row of cancellations) if (!latestCancellation.has(row.assignmentId)) latestCancellation.set(row.assignmentId, row.after);
  const occurrences = rows.map((row) => ({ ...row, hasHistory: history.has(row.id), cancelledBySchedule: latestCancellation.get(row.id)?.seriesOperation === "schedule_removed" }));
  let plan: ReturnType<typeof planSmSeriesChange>;
  try { plan = planSmSeriesChange(occurrences, change, version.validFrom); }
  catch { return fail(400, "sm_series_occurrence_limit", "Eine Serie darf höchstens 1.000 Termine im gewählten Zeitraum enthalten."); }
  if (change.action === "edit" && plan.desiredCount === 0) fail(400, "sm_series_empty", "Im gewählten Zeitraum liegt kein passender Termin. Zum Beenden bitte Stoppen wählen.");
  const previewToken = createHash("sha256").update(JSON.stringify({ today, series, version, change, occurrences })).digest("hex");
  const preview = { previewToken, effectiveFromDate: change.effectiveFromDate, updateCount: plan.updateIds.length,
    cancelCount: plan.cancelIds.length, restoreCount: plan.restoreIds.length, createCount: plan.createDates.length,
    protectedCount: plan.protectedIds.length, preservedDateCount: plan.preservedDateIds.length, blockedDateCount: plan.blockedDates.length };
  return { series, version, market, rows, plan, preview };
}

export async function previewSmSeriesChange(tx: Tx, id: string, change: Change, today = smHolidayToday()) {
  return (await prepare(tx, id, change, today)).preview;
}

export async function applySmSeriesChange(tx: Tx, id: string, input: z.infer<typeof mutationSchema>, actorUserId: string, today = smHolidayToday()) {
  await lockSmPlanning(tx);
  const { change } = input;
  const prepared = await prepare(tx, id, change, today, true);
  const { version, market, rows, plan, preview } = prepared;
  if (preview.previewToken !== input.previewToken) fail(409, "sm_series_preview_stale", "Die Planung hat sich geändert. Bitte die Vorschau erneut prüfen; es wurde nichts gespeichert.");
  const now = new Date();
  const beforeById = new Map(rows.map((row) => [row.id, row]));
  const state = (row: typeof smAssignments.$inferSelect) => ({ ...row, effective: resolveSmAssignmentValues(row) });
  const audit = async (changed: typeof rows, operation: string, eventType: "updated" | "cancelled" | "restored" | "created") => {
    if (!changed.length) return;
    await tx.insert(smAssignmentEvents).values(changed.map((row) => ({ assignmentId: row.id, seriesId: id, eventType,
      actorUserId, reason: input.reason, beforeState: beforeById.has(row.id) ? state(beforeById.get(row.id)!) : {},
      afterState: { ...state(row), seriesOperation: operation, effectiveFromDate: change.effectiveFromDate }, createdAt: sql`clock_timestamp()` })));
  };
  if (plan.cancelIds.length) {
    const cancelled = await tx.update(smAssignments).set({ status: "cancelled", statusBeforeCancellation: sql`${smAssignments.status}`,
      cancelledAt: now, cancelledByUserId: actorUserId, cancellationReason: input.reason, updatedAt: now, updatedByUserId: actorUserId })
      .where(inArray(smAssignments.id, plan.cancelIds)).returning();
    await audit(cancelled, change.action === "stop" ? "series_stopped" : "schedule_removed", "cancelled");
  }
  let holidayAdjustedCount = 0;
  if (change.action === "edit") {
    const [next] = await tx.insert(smAssignmentSeriesVersions).values({ seriesId: id, versionNumber: version.versionNumber + 1,
      effectiveFromDate: change.effectiveFromDate, smMarketId: market!.id, marketInternalIdSnapshot: market!.internalMarketId!,
      defaultSmUserId: change.smUserId, plannedMinutes: change.plannedMinutes, frequency: change.frequency, weekdays: normalizeWeekdays(change.weekdays),
      validFrom: version.validFrom, validTo: change.validTo, flatRateCents: version.flatRateCents, currency: version.currency,
      questionnaireVersionId: null, changeReason: input.reason, createdByUserId: actorUserId }).returning();
    if (!next) throw new Error("SM series version write failed");
    const targetValues = {
      replacementSmUserId: sql`case when ${smAssignments.originalSmUserId} = ${change.smUserId}::uuid then null else ${change.smUserId}::uuid end`,
      replacementSmMarketId: sql`case when ${smAssignments.originalSmMarketId} = ${change.smMarketId}::uuid then null else ${change.smMarketId}::uuid end`,
      replacementMarketInternalId: sql`case when ${smAssignments.originalSmMarketId} = ${change.smMarketId}::uuid then null else ${market!.internalMarketId!} end`,
      replacementPlannedMinutes: sql`case when ${smAssignments.originalPlannedMinutes} = ${change.plannedMinutes}::integer then null else ${change.plannedMinutes}::integer end`,
      updatedAt: now, updatedByUserId: actorUserId,
    };
    if (plan.updateIds.length) await audit(await tx.update(smAssignments).set(targetValues).where(inArray(smAssignments.id, plan.updateIds)).returning(), "series_updated", "updated");
    if (plan.restoreIds.length) await audit(await tx.update(smAssignments).set({ ...targetValues, status: "planned", statusBeforeCancellation: null,
      cancelledAt: null, cancelledByUserId: null, cancellationReason: null }).where(inArray(smAssignments.id, plan.restoreIds)).returning(), "schedule_restored", "restored");
    const created = plan.createDates.length ? await tx.insert(smAssignments).values(plan.createDates.map((workDate) => ({
      sourceType: "series" as const, seriesId: id, seriesVersionId: next.id, seriesOccurrenceKey: workDate,
      idempotencyKey: `series-edit:${next.id}:${workDate}`, originalWorkDate: workDate, originalSmUserId: change.smUserId,
      originalSmMarketId: change.smMarketId, originalMarketInternalId: market!.internalMarketId!, originalPlannedMinutes: change.plannedMinutes,
      flatRateCents: version.flatRateCents, currency: version.currency, questionnaireVersionId: null, createdByUserId: actorUserId, updatedByUserId: actorUserId,
    }))).returning() : [];
    await audit(created, "series_extended", "created");
    holidayAdjustedCount = (await adjustSmHolidayAssignments(tx, { actorUserId, today: change.effectiveFromDate, assignmentIds: [...created.map((row) => row.id), ...plan.restoreIds] })).length;
  }
  await tx.update(smAssignmentSeries).set({ status: change.action === "stop" ? "ended" : "active", updatedAt: now }).where(eq(smAssignmentSeries.id, id));
  return { ...preview, seriesId: id, status: change.action === "stop" ? "ended" : "active", holidayAdjustedCount };
}

/** Mounted behind the existing admin/sm_admin auth and audit middleware. */
export const smSeriesManagementRouter = Router();
smSeriesManagementRouter.get("/series/:id", async (req, res, next) => {
  try {
    const id = z.string().uuid().parse(req.params.id);
    res.set("Cache-Control", "no-store").json(await db.transaction((tx) => getSmSeriesDetails(tx, id), { isolationLevel: "repeatable read", accessMode: "read only" }));
  } catch (error) { handle(error, res, next); }
});
smSeriesManagementRouter.post("/series/:id/preview", async (req, res, next) => {
  try {
    const id = z.string().uuid().parse(req.params.id);
    const change = smSeriesChangeSchema.parse(req.body);
    res.set("Cache-Control", "no-store").json(await db.transaction((tx) => previewSmSeriesChange(tx, id, change), { isolationLevel: "repeatable read", accessMode: "read only" }));
  } catch (error) { handle(error, res, next); }
});
smSeriesManagementRouter.post("/series/:id/change", async (req: AuthedRequest, res, next) => {
  try {
    const id = z.string().uuid().parse(req.params.id);
    const input = mutationSchema.parse(req.body);
    res.json(await db.transaction((tx) => applySmSeriesChange(tx, id, input, req.authUser!.appUserId)));
  } catch (error) { handle(error, res, next); }
});
function handle(error: unknown, res: import("express").Response, next: import("express").NextFunction) {
  if (error instanceof SmSeriesError) res.status(error.status).json({ code: error.code, error: error.message });
  else if (error instanceof z.ZodError) res.status(400).json({ code: "sm_series_invalid", error: "Die Serienangaben sind ungültig." });
  else next(error);
}
