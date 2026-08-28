import { randomUUID } from "node:crypto";
import { and, asc, desc, eq, gte, inArray, isNull, lte, sql } from "drizzle-orm";
import { Router, type Response } from "express";
import { z } from "zod";

import { db } from "../lib/db.js";
import { logAction, startActionTimer } from "../lib/logger.js";
import {
  smAssignmentEvents,
  smAssignments,
  smAssignmentSeries,
  smAssignmentSeriesVersions,
  smAssignmentTimeChangeRequests,
  smAssignmentTimeSubmissions,
  smMarkets,
  smQuestionnaireGlobalAssignments,
  smQuestionnaireSubmissions,
  smQuestionnaireTemplates,
  smQuestionnaireVersions,
  users,
} from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import {
  buildSmSeriesDates,
  isAssignmentPlanningMutable,
  isIsoDate,
  isoDateToEpochDay,
  normalizeWeekdays,
  replacementOrNull,
  resolveSmAssignmentValues,
} from "../sm-planning.shared.js";

type AssignmentRow = typeof smAssignments.$inferSelect;
type DbTx = Parameters<Parameters<typeof db.transaction>[0]>[0];
type DbExecutor = typeof db | DbTx;

const isoDateSchema = z.string().refine(isIsoDate, "Ungültiges Datum.");
const expectedUpdatedAtSchema = z.string().datetime({ offset: true });
const optionalFlatRateSchema = z.number().int().min(0).max(10_000_000).nullable().optional();
const globalQuestionnaireAssignmentSchema = z.object({ questionnaireTemplateId: z.string().uuid() }).strict();

const singleAssignmentSchema = z.object({
  smMarketId: z.string().uuid(),
  smUserId: z.string().uuid(),
  workDate: isoDateSchema,
  plannedMinutes: z.number().int().min(1).max(1440),
  flatRateCents: optionalFlatRateSchema,
  idempotencyKey: z.string().trim().min(8).max(300),
}).strict();

const seriesSchema = z.object({
  smMarketId: z.string().uuid(),
  smUserId: z.string().uuid(),
  plannedMinutes: z.number().int().min(1).max(1440),
  flatRateCents: optionalFlatRateSchema,
  frequency: z.enum(["weekly", "biweekly"]),
  weekdays: z.array(z.number().int().min(1).max(7)).min(1).max(7),
  validFrom: isoDateSchema,
  validTo: isoDateSchema,
  idempotencyKey: z.string().trim().min(8).max(300),
}).strict();

const updateOccurrenceSchema = z.object({
  smMarketId: z.string().uuid().optional(),
  plannedMinutes: z.number().int().min(1).max(1440).optional(),
  expectedUpdatedAt: expectedUpdatedAtSchema,
  reason: z.string().trim().max(2_000).optional(),
}).strict().refine((value) => value.smMarketId !== undefined || value.plannedMinutes !== undefined, {
  message: "Mindestens eine Änderung ist erforderlich.",
});

const rescheduleSchema = z.object({
  workDate: isoDateSchema,
  expectedUpdatedAt: expectedUpdatedAtSchema,
  reason: z.string().trim().max(2_000).optional(),
}).strict();

const reassignSchema = z.object({
  smUserId: z.string().uuid(),
  scope: z.enum(["occurrence", "series_future"]),
  expectedUpdatedAt: expectedUpdatedAtSchema,
  reason: z.string().trim().min(3).max(2_000),
}).strict();

const cancelSchema = z.object({
  expectedUpdatedAt: expectedUpdatedAtSchema,
  reason: z.string().trim().min(3).max(2_000),
}).strict();

const restoreSchema = z.object({
  expectedUpdatedAt: expectedUpdatedAtSchema,
  reason: z.string().trim().min(3).max(2_000),
}).strict();

const actualTimeSchema = z.object({
  actualMinutes: z.number().int().min(1).max(1440),
  correctionReason: z.string().trim().min(3).max(2_000).optional(),
}).strict();

const timeChangeRequestSchema = z.object({
  kind: z.enum(["time_change", "deletion"]),
  requestedStartedAt: z.string().datetime({ offset: true }).nullable(),
  requestedCompletedAt: z.string().datetime({ offset: true }).nullable(),
  reason: z.string().trim().min(3).max(2_000),
  clientRequestToken: z.string().trim().min(8).max(300),
}).strict().superRefine((value, context) => {
  if (value.kind === "deletion" && (value.requestedStartedAt !== null || value.requestedCompletedAt !== null)) {
    context.addIssue({ code: "custom", path: ["requestedStartedAt"], message: "Eine Löschanfrage darf keine neuen Zeitstempel enthalten." });
  }
  if (value.kind === "time_change") {
    if (!value.requestedStartedAt || !value.requestedCompletedAt) {
      context.addIssue({ code: "custom", path: ["requestedStartedAt"], message: "Start- und Endzeit müssen vollständig angegeben werden." });
      return;
    }
    const startedAt = new Date(value.requestedStartedAt);
    const completedAt = new Date(value.requestedCompletedAt);
    if (!timestampPairMinutes(startedAt, completedAt)) {
      context.addIssue({ code: "custom", path: ["requestedCompletedAt"], message: "Die Endzeit muss nach der Startzeit und höchstens 24 Stunden später liegen." });
    }
  }
});

const timeChangeReviewSchema = z.object({
  adminNote: z.string().trim().max(2_000).optional(),
}).strict();

class SmPlanningError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly code: string,
    message: string,
  ) {
    super(message);
  }
}

function sendKnownError(error: unknown, res: Response): boolean {
  if (!(error instanceof SmPlanningError)) return false;
  res.status(error.statusCode).json({ error: error.message, code: error.code });
  return true;
}

function requireWrittenRow<T>(row: T | undefined): T {
  if (!row) {
    throw new SmPlanningError(500, "sm_planning_write_failed", "Die Planungsänderung konnte nicht gespeichert werden.");
  }
  return row;
}

type GlobalQuestionnaireOption = {
  questionnaireTemplateId: string;
  latestPublishedVersionId: string;
  versionNumber: number;
  name: string;
  description: string;
};

async function loadGlobalQuestionnaireConfiguration(executor: DbExecutor) {
  const versionRows = await executor.select({
    questionnaireTemplateId: smQuestionnaireTemplates.id,
    latestPublishedVersionId: smQuestionnaireVersions.id,
    versionNumber: smQuestionnaireVersions.versionNumber,
    name: smQuestionnaireVersions.name,
    description: smQuestionnaireVersions.description,
  }).from(smQuestionnaireTemplates)
    .innerJoin(smQuestionnaireVersions, eq(smQuestionnaireVersions.questionnaireTemplateId, smQuestionnaireTemplates.id))
    .where(and(
      eq(smQuestionnaireTemplates.status, "active"),
      eq(smQuestionnaireTemplates.isDeleted, false),
      eq(smQuestionnaireVersions.status, "published"),
      eq(smQuestionnaireVersions.isDeleted, false),
    ))
    .orderBy(asc(smQuestionnaireTemplates.id), desc(smQuestionnaireVersions.versionNumber));

  const latestByTemplate = new Map<string, GlobalQuestionnaireOption>();
  for (const row of versionRows) {
    if (!latestByTemplate.has(row.questionnaireTemplateId)) latestByTemplate.set(row.questionnaireTemplateId, row);
  }
  const options = [...latestByTemplate.values()].sort((left, right) => left.name.localeCompare(right.name, "de-AT"));
  const [assignment] = await executor.select().from(smQuestionnaireGlobalAssignments).where(and(
    eq(smQuestionnaireGlobalAssignments.isDeleted, false),
    isNull(smQuestionnaireGlobalAssignments.supersededAt),
  )).limit(1);

  return {
    assignment: assignment ? {
      id: assignment.id,
      questionnaireTemplateId: assignment.questionnaireTemplateId,
      assignedByUserId: assignment.assignedByUserId,
      assignedAt: assignment.assignedAt.toISOString(),
      questionnaire: latestByTemplate.get(assignment.questionnaireTemplateId) ?? null,
    } : null,
    options,
  };
}

async function requireConfiguredGlobalQuestionnaire(executor: DbExecutor) {
  const configuration = await loadGlobalQuestionnaireConfiguration(executor);
  if (!configuration.assignment) {
    throw new SmPlanningError(409, "sm_global_questionnaire_missing", "Wähle zuerst den zentralen SM-Fragebogen für alle Einsätze aus.");
  }
  if (!configuration.assignment.questionnaire) {
    throw new SmPlanningError(409, "sm_global_questionnaire_unavailable", "Der zentral ausgewählte SM-Fragebogen ist nicht mehr aktiv oder veröffentlicht. Bitte wähle einen anderen aus.");
  }
  return configuration.assignment.questionnaire;
}

function timestampPairMinutes(startedAt: Date, completedAt: Date): number | null {
  const elapsedMs = completedAt.getTime() - startedAt.getTime();
  if (!Number.isFinite(elapsedMs) || elapsedMs <= 0 || elapsedMs > 24 * 60 * 60 * 1_000) return null;
  return Math.max(1, Math.round(elapsedMs / 60_000));
}

function sameInstant(left: Date | null, right: Date | null): boolean {
  return left === null ? right === null : right !== null && left.getTime() === right.getTime();
}

function assignmentState(row: AssignmentRow): Record<string, unknown> {
  const effective = resolveSmAssignmentValues(row);
  return {
    id: row.id,
    sourceType: row.sourceType,
    seriesId: row.seriesId,
    status: row.status,
    original: {
      workDate: row.originalWorkDate,
      smUserId: row.originalSmUserId,
      smMarketId: row.originalSmMarketId,
      marketInternalId: row.originalMarketInternalId,
      plannedMinutes: row.originalPlannedMinutes,
    },
    replacement: {
      workDate: row.replacementWorkDate,
      smUserId: row.replacementSmUserId,
      smMarketId: row.replacementSmMarketId,
      marketInternalId: row.replacementMarketInternalId,
      plannedMinutes: row.replacementPlannedMinutes,
    },
    effective,
    cancellation: row.status === "cancelled" ? {
      cancelledAt: row.cancelledAt?.toISOString() ?? null,
      cancelledByUserId: row.cancelledByUserId,
      reason: row.cancellationReason,
    } : null,
  };
}

function publicTimeChangeRequest(row: typeof smAssignmentTimeChangeRequests.$inferSelect) {
  return {
    id: row.id,
    assignmentId: row.assignmentId,
    smUserId: row.smUserId,
    sourceTimeSubmissionId: row.sourceTimeSubmissionId,
    kind: row.requestKind as "time_change" | "deletion",
    originalMinutes: row.originalMinutes,
    requestedMinutes: row.requestedMinutes,
    timestampCorrectionVersion: row.timestampCorrectionVersion as 0 | 1,
    originalStartedAt: row.originalStartedAt?.toISOString() ?? null,
    originalCompletedAt: row.originalCompletedAt?.toISOString() ?? null,
    requestedStartedAt: row.requestedStartedAt?.toISOString() ?? null,
    requestedCompletedAt: row.requestedCompletedAt?.toISOString() ?? null,
    reason: row.requestReason,
    status: row.status,
    reviewedByUserId: row.reviewedByUserId,
    reviewedAt: row.reviewedAt?.toISOString() ?? null,
    adminNote: row.adminNote,
    appliedTimeSubmissionId: row.appliedTimeSubmissionId,
    appliedAt: row.appliedAt?.toISOString() ?? null,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  };
}

async function loadActiveMarket(executor: DbExecutor, marketId: string) {
  const [market] = await executor.select().from(smMarkets).where(and(
    eq(smMarkets.id, marketId),
    eq(smMarkets.isDeleted, false),
    eq(smMarkets.isActive, true),
  )).limit(1);
  if (!market) throw new SmPlanningError(400, "sm_market_invalid", "Der SM-Markt ist nicht aktiv oder wurde nicht gefunden.");
  if (!market.internalMarketId) throw new SmPlanningError(400, "sm_market_missing_stammnummer", "Der SM-Markt hat keine Stammnummer und kann nicht verplant werden.");
  return market;
}

async function loadActiveSmUser(executor: DbExecutor, userId: string) {
  const [user] = await executor.select().from(users).where(and(
    eq(users.id, userId),
    eq(users.role, "sm"),
    eq(users.isActive, true),
    sql`${users.deletedAt} is null`,
  )).limit(1);
  if (!user) throw new SmPlanningError(400, "sm_user_invalid", "Der Shelf Merchandiser ist nicht aktiv oder wurde nicht gefunden.");
  return user;
}

async function loadAssignmentForUpdate(tx: DbTx, assignmentId: string): Promise<AssignmentRow> {
  const [row] = await tx.select().from(smAssignments).where(and(
    eq(smAssignments.id, assignmentId),
    eq(smAssignments.isDeleted, false),
  )).limit(1).for("update");
  if (!row) throw new SmPlanningError(404, "sm_assignment_not_found", "Der Einsatz wurde nicht gefunden.");
  return row;
}

function assertExpectedUpdatedAt(row: AssignmentRow, expectedUpdatedAt: string) {
  if (row.updatedAt.toISOString() !== new Date(expectedUpdatedAt).toISOString()) {
    throw new SmPlanningError(409, "sm_assignment_stale", "Der Einsatz wurde zwischenzeitlich geändert. Bitte neu laden.");
  }
}

function assertPlanningMutable(row: AssignmentRow) {
  if (row.status === "cancelled") {
    throw new SmPlanningError(409, "sm_assignment_cancelled", "Der Einsatz muss zuerst wiederhergestellt werden.");
  }
  if (!isAssignmentPlanningMutable(row.status)) {
    throw new SmPlanningError(409, "sm_assignment_locked", "Ein laufender oder abgeschlossener Einsatz kann nicht mehr verplant werden.");
  }
}

async function writeEvent(tx: DbTx, input: {
  before: AssignmentRow | null;
  after: AssignmentRow;
  eventType: typeof smAssignmentEvents.$inferInsert.eventType;
  actorUserId: string;
  reason?: string | null | undefined;
}) {
  await tx.insert(smAssignmentEvents).values({
    assignmentId: input.after.id,
    seriesId: input.after.seriesId,
    eventType: input.eventType,
    actorUserId: input.actorUserId,
    reason: input.reason?.trim() || null,
    beforeState: input.before ? assignmentState(input.before) : {},
    afterState: assignmentState(input.after),
  });
}

async function loadAssignments(from: string, to: string, smUserId?: string) {
  const effectiveDate = sql<string>`coalesce(${smAssignments.replacementWorkDate}, ${smAssignments.originalWorkDate})`;
  const effectiveSmUserId = sql<string>`coalesce(${smAssignments.replacementSmUserId}, ${smAssignments.originalSmUserId})`;
  const filters = [
    eq(smAssignments.isDeleted, false),
    gte(effectiveDate, from),
    lte(effectiveDate, to),
  ];
  if (smUserId) filters.push(eq(effectiveSmUserId, smUserId));

  const rows = await db.select().from(smAssignments).where(and(...filters))
    .orderBy(asc(effectiveDate), asc(smAssignments.createdAt));

  if (rows.length === 0) return [];
  const userIds = [...new Set(rows.flatMap((row) => [row.originalSmUserId, row.replacementSmUserId].filter((value): value is string => Boolean(value))))];
  const marketIds = [...new Set(rows.flatMap((row) => [row.originalSmMarketId, row.replacementSmMarketId].filter((value): value is string => Boolean(value))))];
  const seriesVersionIds = [...new Set(rows.map((row) => row.seriesVersionId).filter((value): value is string => Boolean(value)))];
  const assignmentIds = rows.map((row) => row.id);

  const [userRows, marketRows, seriesVersionRows, timeRows, submissionRows, requestRows] = await Promise.all([
    userIds.length ? db.select({ id: users.id, firstName: users.firstName, lastName: users.lastName }).from(users).where(inArray(users.id, userIds)) : [],
    marketIds.length ? db.select({ id: smMarkets.id, name: smMarkets.name, address: smMarkets.address, postalCode: smMarkets.postalCode, city: smMarkets.city, region: smMarkets.region, internalMarketId: smMarkets.internalMarketId }).from(smMarkets).where(inArray(smMarkets.id, marketIds)) : [],
    seriesVersionIds.length ? db.select().from(smAssignmentSeriesVersions).where(inArray(smAssignmentSeriesVersions.id, seriesVersionIds)) : [],
    db.select().from(smAssignmentTimeSubmissions).where(and(
      inArray(smAssignmentTimeSubmissions.assignmentId, assignmentIds),
      eq(smAssignmentTimeSubmissions.isDeleted, false),
      eq(smAssignmentTimeSubmissions.isCurrent, true),
    )),
    db.select({
      id: smQuestionnaireSubmissions.id,
      assignmentId: smQuestionnaireSubmissions.assignmentId,
      status: smQuestionnaireSubmissions.status,
      questionnaireName: smQuestionnaireSubmissions.questionnaireNameSnapshot,
      visitTimeMode: smQuestionnaireSubmissions.visitTimeMode,
      travelMinutes: smQuestionnaireSubmissions.travelMinutes,
      visitStartedAt: smQuestionnaireSubmissions.visitStartedAt,
      visitCompletedAt: smQuestionnaireSubmissions.visitCompletedAt,
      submittedAt: smQuestionnaireSubmissions.submittedAt,
    }).from(smQuestionnaireSubmissions).where(and(
      inArray(smQuestionnaireSubmissions.assignmentId, assignmentIds),
      eq(smQuestionnaireSubmissions.isDeleted, false),
      eq(smQuestionnaireSubmissions.isCurrent, true),
    )),
    db.select().from(smAssignmentTimeChangeRequests).where(and(
      inArray(smAssignmentTimeChangeRequests.assignmentId, assignmentIds),
      eq(smAssignmentTimeChangeRequests.isDeleted, false),
      eq(smAssignmentTimeChangeRequests.status, "pending"),
    )),
  ]);

  const userById = new Map(userRows.map((row) => [row.id, `${row.firstName} ${row.lastName}`.trim()]));
  const marketById = new Map(marketRows.map((row) => [row.id, row]));
  const seriesVersionById = new Map(seriesVersionRows.map((row) => [row.id, row]));
  const timeByAssignmentId = new Map(timeRows.map((row) => [row.assignmentId, row]));
  const submissionByAssignmentId = new Map(submissionRows.map((row) => [row.assignmentId, row]));
  const requestByAssignmentId = new Map(requestRows.map((row) => [row.assignmentId, row]));

  return rows.map((row) => {
    const effective = resolveSmAssignmentValues(row);
    const originalMarket = marketById.get(row.originalSmMarketId);
    const effectiveMarket = marketById.get(effective.smMarketId) ?? originalMarket;
    const seriesVersion = row.seriesVersionId ? seriesVersionById.get(row.seriesVersionId) : undefined;
    const time = timeByAssignmentId.get(row.id);
    const visit = submissionByAssignmentId.get(row.id);
    return {
      id: row.id,
      sourceType: row.sourceType,
      seriesId: row.seriesId,
      seriesVersionId: row.seriesVersionId,
      seriesOccurrenceKey: row.seriesOccurrenceKey,
      status: row.status,
      original: {
        workDate: row.originalWorkDate,
        smUserId: row.originalSmUserId,
        smName: userById.get(row.originalSmUserId) ?? "Unbekannter SM",
        smMarketId: row.originalSmMarketId,
        marketInternalId: row.originalMarketInternalId,
        marketName: originalMarket?.name ?? "Unbekannter Markt",
        plannedMinutes: row.originalPlannedMinutes,
      },
      replacement: {
        workDate: row.replacementWorkDate,
        smUserId: row.replacementSmUserId,
        smName: row.replacementSmUserId ? userById.get(row.replacementSmUserId) ?? "Unbekannter SM" : null,
        smMarketId: row.replacementSmMarketId,
        marketInternalId: row.replacementMarketInternalId,
        plannedMinutes: row.replacementPlannedMinutes,
      },
      effective: {
        ...effective,
        smName: userById.get(effective.smUserId) ?? "Unbekannter SM",
        marketName: effectiveMarket?.name ?? "Unbekannter Markt",
        address: effectiveMarket ? `${effectiveMarket.address} · ${effectiveMarket.postalCode} ${effectiveMarket.city}` : "",
        region: effectiveMarket?.region ?? "",
      },
      series: seriesVersion ? {
        frequency: seriesVersion.frequency,
        weekdays: seriesVersion.weekdays,
        validFrom: seriesVersion.validFrom,
        validTo: seriesVersion.validTo,
        versionNumber: seriesVersion.versionNumber,
      } : null,
      actualMinutes: time?.actualMinutes ?? null,
      timeEntry: time ? {
        id: time.id,
        revisionNumber: time.revisionNumber,
        actualMinutes: time.actualMinutes,
        submittedByUserId: time.submittedByUserId,
        submittedAt: time.submittedAt.toISOString(),
        correctionReason: time.correctionReason,
      } : null,
      visit: visit ? {
        id: visit.id,
        status: visit.status,
        questionnaireName: visit.questionnaireName,
        visitTimeMode: visit.visitTimeMode,
        travelMinutes: visit.travelMinutes,
        visitStartedAt: visit.visitStartedAt?.toISOString() ?? null,
        visitCompletedAt: visit.visitCompletedAt?.toISOString() ?? null,
        submittedAt: visit.submittedAt?.toISOString() ?? null,
      } : null,
      pendingTimeChangeRequest: requestByAssignmentId.has(row.id) ? publicTimeChangeRequest(requestByAssignmentId.get(row.id)!) : null,
      flatRateCents: row.flatRateCents,
      questionnaireComplete: visit?.status === "submitted",
      cancellation: row.status === "cancelled" ? {
        reason: row.cancellationReason,
        cancelledAt: row.cancelledAt?.toISOString() ?? null,
      } : null,
      createdAt: row.createdAt.toISOString(),
      updatedAt: row.updatedAt.toISOString(),
    };
  });
}

function parseAssignmentRange(query: unknown): { from: string; to: string } {
  const parsed = z.object({ from: isoDateSchema, to: isoDateSchema }).safeParse(query);
  if (!parsed.success) throw new SmPlanningError(400, "sm_planning_range_invalid", "Ein gültiger Zeitraum ist erforderlich.");
  if (isoDateToEpochDay(parsed.data.to) < isoDateToEpochDay(parsed.data.from)) {
    throw new SmPlanningError(400, "sm_planning_range_invalid", "Das Enddatum muss nach dem Startdatum liegen.");
  }
  if (isoDateToEpochDay(parsed.data.to) - isoDateToEpochDay(parsed.data.from) > 92) {
    throw new SmPlanningError(400, "sm_planning_range_too_large", "Der Abfragezeitraum darf höchstens 93 Tage umfassen.");
  }
  return parsed.data;
}

export const smPlanningRouter = Router();
smPlanningRouter.use(requireAuth(["sm"]));

smPlanningRouter.get("/assignments", async (req: AuthedRequest, res, next) => {
  try {
    const range = parseAssignmentRange(req.query);
    const smUserId = req.authUser!.appUserId;
    res.status(200).json({ assignments: await loadAssignments(range.from, range.to, smUserId) });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

smPlanningRouter.post("/assignments/:id/time-change-requests", async (req: AuthedRequest, res, next) => {
  try {
    const id = z.string().uuid().safeParse(req.params.id);
    const parsed = timeChangeRequestSchema.safeParse(req.body);
    if (!id.success || !parsed.success) {
      throw new SmPlanningError(400, "sm_assignment_time_request_invalid", "Die Korrekturanfrage ist ungültig.");
    }
    const actorUserId = req.authUser!.appUserId;
    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_time_request:${id.data}`}, 0))`);
      const assignment = await loadAssignmentForUpdate(tx, id.data);
      const effective = resolveSmAssignmentValues(assignment);
      if (effective.smUserId !== actorUserId) {
        throw new SmPlanningError(404, "sm_assignment_not_found", "Der Einsatz wurde nicht gefunden.");
      }
      const [replayed] = await tx.select().from(smAssignmentTimeChangeRequests).where(and(
        eq(smAssignmentTimeChangeRequests.smUserId, actorUserId),
        eq(smAssignmentTimeChangeRequests.clientRequestToken, parsed.data.clientRequestToken),
        eq(smAssignmentTimeChangeRequests.isDeleted, false),
      )).limit(1);
      if (replayed) {
        const replayedStartedAt = parsed.data.requestedStartedAt ? new Date(parsed.data.requestedStartedAt) : null;
        const replayedCompletedAt = parsed.data.requestedCompletedAt ? new Date(parsed.data.requestedCompletedAt) : null;
        if (
          replayed.assignmentId !== id.data
          || replayed.requestKind !== parsed.data.kind
          || replayed.requestReason !== parsed.data.reason
          || !sameInstant(replayed.requestedStartedAt, replayedStartedAt)
          || !sameInstant(replayed.requestedCompletedAt, replayedCompletedAt)
        ) {
          throw new SmPlanningError(409, "sm_assignment_time_request_token_conflict", "Dieser Anfrage-Token wurde bereits mit anderen Korrekturdaten verwendet.");
        }
        return { request: replayed, replayed: true };
      }
      if (assignment.status !== "completed") {
        throw new SmPlanningError(409, "sm_assignment_time_request_not_completed", "Nur eine abgeschlossene Ist-Zeit kann korrigiert werden.");
      }
      const [currentTime] = await tx.select().from(smAssignmentTimeSubmissions).where(and(
        eq(smAssignmentTimeSubmissions.assignmentId, assignment.id),
        eq(smAssignmentTimeSubmissions.isDeleted, false),
        eq(smAssignmentTimeSubmissions.isCurrent, true),
      )).limit(1).for("update");
      if (!currentTime) {
        throw new SmPlanningError(409, "sm_assignment_time_request_missing_time", "Für diesen Einsatz ist keine aktuelle Ist-Zeit vorhanden.");
      }
      const [currentVisit] = await tx.select().from(smQuestionnaireSubmissions).where(and(
        eq(smQuestionnaireSubmissions.assignmentId, assignment.id),
        eq(smQuestionnaireSubmissions.status, "submitted"),
        eq(smQuestionnaireSubmissions.isCurrent, true),
        eq(smQuestionnaireSubmissions.isDeleted, false),
      )).limit(1).for("update");
      if (parsed.data.kind === "time_change" && !currentVisit) {
        throw new SmPlanningError(409, "sm_assignment_time_request_missing_visit", "Der abgeschlossene Fragebogen mit Start- und Endzeit wurde nicht gefunden.");
      }
      const [pending] = await tx.select().from(smAssignmentTimeChangeRequests).where(and(
        eq(smAssignmentTimeChangeRequests.assignmentId, assignment.id),
        eq(smAssignmentTimeChangeRequests.status, "pending"),
        eq(smAssignmentTimeChangeRequests.isDeleted, false),
      )).limit(1).for("update");
      if (pending) return { request: pending, replayed: true };
      const requestedStartedAt = parsed.data.requestedStartedAt ? new Date(parsed.data.requestedStartedAt) : null;
      const requestedCompletedAt = parsed.data.requestedCompletedAt ? new Date(parsed.data.requestedCompletedAt) : null;
      const requestedMinutes = requestedStartedAt && requestedCompletedAt ? timestampPairMinutes(requestedStartedAt, requestedCompletedAt) : null;
      if (parsed.data.kind === "time_change" && !requestedMinutes) {
        throw new SmPlanningError(400, "sm_assignment_time_request_interval_invalid", "Die gewünschte Start- und Endzeit ist ungültig.");
      }
      if (
        parsed.data.kind === "time_change"
        && requestedMinutes === currentTime.actualMinutes
        && sameInstant(currentVisit?.visitStartedAt ?? null, requestedStartedAt)
        && sameInstant(currentVisit?.visitCompletedAt ?? null, requestedCompletedAt)
      ) {
        throw new SmPlanningError(400, "sm_assignment_time_request_unchanged", "Start- und Endzeit entsprechen bereits den gespeicherten Zeitstempeln.");
      }
      const [createdRow] = await tx.insert(smAssignmentTimeChangeRequests).values({
        assignmentId: assignment.id,
        smUserId: actorUserId,
        sourceTimeSubmissionId: currentTime.id,
        requestKind: parsed.data.kind,
        originalMinutes: currentTime.actualMinutes,
        requestedMinutes: parsed.data.kind === "time_change" ? requestedMinutes : null,
        timestampCorrectionVersion: 1,
        originalStartedAt: currentVisit?.visitStartedAt ?? null,
        originalCompletedAt: currentVisit?.visitCompletedAt ?? null,
        requestedStartedAt: parsed.data.kind === "time_change" ? requestedStartedAt : null,
        requestedCompletedAt: parsed.data.kind === "time_change" ? requestedCompletedAt : null,
        requestReason: parsed.data.reason,
        clientRequestToken: parsed.data.clientRequestToken,
      }).returning();
      return { request: requireWrittenRow(createdRow), replayed: false };
    });
    res.status(result.replayed ? 200 : 201).json({ request: publicTimeChangeRequest(result.request), replayed: result.replayed });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

export const adminSmPlanningRouter = Router();
adminSmPlanningRouter.use(requireAuth(["admin"]));

adminSmPlanningRouter.use((req, res, next) => {
  if (req.method === "GET") {
    next();
    return;
  }
  const startedAtNs = startActionTimer();
  res.on("finish", () => logAction(res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info", "sm_planning_action_completed", {
    req,
    action: "sm_planning_mutation",
    result: res.statusCode >= 400 ? "failure" : "success",
    statusCode: res.statusCode,
    requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
    startedAtNs,
    details: { route: req.path, method: req.method },
  }));
  next();
});

adminSmPlanningRouter.get("/questionnaire-assignment", async (_req, res, next) => {
  try {
    res.status(200).json(await loadGlobalQuestionnaireConfiguration(db));
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.put("/questionnaire-assignment", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = globalQuestionnaireAssignmentSchema.safeParse(req.body);
    if (!parsed.success) throw new SmPlanningError(400, "sm_global_questionnaire_invalid", "Bitte wähle einen gültigen SM-Fragebogen aus.");
    const actorUserId = req.authUser!.appUserId;
    const replayed = await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended('sm_global_questionnaire_assignment', 0))`);
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_questionnaire:${parsed.data.questionnaireTemplateId}`}, 0))`);
      const configuration = await loadGlobalQuestionnaireConfiguration(tx);
      const selected = configuration.options.find((option) => option.questionnaireTemplateId === parsed.data.questionnaireTemplateId);
      if (!selected) {
        throw new SmPlanningError(409, "sm_global_questionnaire_unavailable", "Dieser SM-Fragebogen ist nicht aktiv oder besitzt keine veröffentlichte Version.");
      }
      const [current] = await tx.select().from(smQuestionnaireGlobalAssignments).where(and(
        eq(smQuestionnaireGlobalAssignments.isDeleted, false),
        isNull(smQuestionnaireGlobalAssignments.supersededAt),
      )).limit(1).for("update");
      if (current?.questionnaireTemplateId === selected.questionnaireTemplateId) return true;

      const now = new Date();
      if (current) {
        await tx.update(smQuestionnaireGlobalAssignments).set({
          supersededAt: now,
          supersededByUserId: actorUserId,
          updatedAt: now,
        }).where(eq(smQuestionnaireGlobalAssignments.id, current.id));
      }
      await tx.insert(smQuestionnaireGlobalAssignments).values({
        questionnaireTemplateId: selected.questionnaireTemplateId,
        assignedByUserId: actorUserId,
        assignedAt: now,
      });
      return false;
    });
    res.status(200).json({ ...(await loadGlobalQuestionnaireConfiguration(db)), replayed });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.get("/assignments", async (req, res, next) => {
  try {
    const range = parseAssignmentRange(req.query);
    res.status(200).json({ assignments: await loadAssignments(range.from, range.to) });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.post("/time-change-requests/:id/approve", async (req: AuthedRequest, res, next) => {
  try {
    const id = z.string().uuid().safeParse(req.params.id);
    const parsed = timeChangeReviewSchema.safeParse(req.body ?? {});
    if (!id.success || !parsed.success) throw new SmPlanningError(400, "sm_assignment_time_request_review_invalid", "Die Freigabe ist ungültig.");
    const actorUserId = req.authUser!.appUserId;
    const result = await db.transaction(async (tx) => {
      const [identity] = await tx.select({ assignmentId: smAssignmentTimeChangeRequests.assignmentId }).from(smAssignmentTimeChangeRequests).where(and(
        eq(smAssignmentTimeChangeRequests.id, id.data),
        eq(smAssignmentTimeChangeRequests.isDeleted, false),
      )).limit(1);
      if (!identity) throw new SmPlanningError(404, "sm_assignment_time_request_not_found", "Die Korrekturanfrage wurde nicht gefunden.");
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_time_request:${identity.assignmentId}`}, 0))`);
      const assignment = await loadAssignmentForUpdate(tx, identity.assignmentId);
      const [currentTime] = await tx.select().from(smAssignmentTimeSubmissions).where(and(
        eq(smAssignmentTimeSubmissions.assignmentId, identity.assignmentId),
        eq(smAssignmentTimeSubmissions.isDeleted, false),
        eq(smAssignmentTimeSubmissions.isCurrent, true),
      )).limit(1).for("update");
      const [currentVisit] = await tx.select().from(smQuestionnaireSubmissions).where(and(
        eq(smQuestionnaireSubmissions.assignmentId, identity.assignmentId),
        eq(smQuestionnaireSubmissions.status, "submitted"),
        eq(smQuestionnaireSubmissions.isCurrent, true),
        eq(smQuestionnaireSubmissions.isDeleted, false),
      )).limit(1).for("update");
      const [request] = await tx.select().from(smAssignmentTimeChangeRequests).where(and(
        eq(smAssignmentTimeChangeRequests.id, id.data),
        eq(smAssignmentTimeChangeRequests.isDeleted, false),
      )).limit(1).for("update");
      if (!request) throw new SmPlanningError(404, "sm_assignment_time_request_not_found", "Die Korrekturanfrage wurde nicht gefunden.");
      if (request.status === "approved") return { request, replayed: true };
      if (request.status !== "pending") throw new SmPlanningError(409, "sm_assignment_time_request_closed", "Diese Korrekturanfrage wurde bereits bearbeitet.");
      if (!currentTime || currentTime.id !== request.sourceTimeSubmissionId || currentTime.actualMinutes !== request.originalMinutes) {
        throw new SmPlanningError(409, "sm_assignment_time_request_stale", "Die Ist-Zeit wurde seit der Anfrage geändert. Bitte lehne die veraltete Anfrage ab.");
      }
      if (
        request.timestampCorrectionVersion === 1
        && (
          !currentVisit
          || !sameInstant(currentVisit.visitStartedAt, request.originalStartedAt)
          || !sameInstant(currentVisit.visitCompletedAt, request.originalCompletedAt)
        )
      ) {
        throw new SmPlanningError(409, "sm_assignment_time_request_stale", "Start- oder Endzeit wurde seit der Anfrage geändert. Bitte lehne die veraltete Anfrage ab.");
      }
      const now = new Date();
      let appliedTimeSubmissionId: string | null = null;
      if (request.requestKind === "time_change") {
        if (!request.requestedMinutes) throw new SmPlanningError(409, "sm_assignment_time_request_invalid_state", "Der Korrekturanfrage fehlt die gewünschte Zeit.");
        if (request.timestampCorrectionVersion === 1) {
          if (!request.requestedStartedAt || !request.requestedCompletedAt || !currentVisit) {
            throw new SmPlanningError(409, "sm_assignment_time_request_invalid_state", "Der Korrekturanfrage fehlen Start- oder Endzeit.");
          }
          const calculatedMinutes = timestampPairMinutes(request.requestedStartedAt, request.requestedCompletedAt);
          if (calculatedMinutes !== request.requestedMinutes) {
            throw new SmPlanningError(409, "sm_assignment_time_request_invalid_state", "Die gespeicherte Dauer passt nicht zu Start- und Endzeit.");
          }
        }
        await tx.update(smAssignmentTimeSubmissions).set({ isCurrent: false, updatedAt: now }).where(eq(smAssignmentTimeSubmissions.id, currentTime.id));
        const [createdRow] = await tx.insert(smAssignmentTimeSubmissions).values({
          assignmentId: request.assignmentId,
          revisionNumber: currentTime.revisionNumber + 1,
          actualMinutes: request.requestedMinutes,
          isCurrent: true,
          supersedesSubmissionId: currentTime.id,
          submittedByUserId: actorUserId,
          submittedAt: now,
          correctionReason: `SM-Korrekturanfrage: ${request.requestReason}`,
        }).returning();
        appliedTimeSubmissionId = requireWrittenRow(createdRow).id;
        if (request.timestampCorrectionVersion === 1 && currentVisit && request.requestedStartedAt && request.requestedCompletedAt) {
          await tx.update(smQuestionnaireSubmissions).set({
            visitStartedAt: request.requestedStartedAt,
            visitCompletedAt: request.requestedCompletedAt,
            lastSavedAt: now,
            updatedAt: now,
          }).where(eq(smQuestionnaireSubmissions.id, currentVisit.id));
          const [updatedAssignment] = await tx.update(smAssignments).set({
            startedAt: request.requestedStartedAt,
            completedAt: request.requestedCompletedAt,
            updatedByUserId: actorUserId,
            updatedAt: now,
          }).where(eq(smAssignments.id, assignment.id)).returning();
          await writeEvent(tx, {
            before: assignment,
            after: requireWrittenRow(updatedAssignment),
            eventType: "updated",
            actorUserId,
            reason: `SM-Zeitstempelkorrektur freigegeben: ${request.requestReason}`,
          });
        }
      } else {
        await tx.update(smAssignmentTimeSubmissions).set({
          isCurrent: false,
          isDeleted: true,
          deletedAt: now,
          updatedAt: now,
        }).where(eq(smAssignmentTimeSubmissions.id, currentTime.id));
      }
      const [updatedRow] = await tx.update(smAssignmentTimeChangeRequests).set({
        status: "approved",
        reviewedByUserId: actorUserId,
        reviewedAt: now,
        adminNote: parsed.data.adminNote?.trim() || null,
        appliedTimeSubmissionId,
        appliedAt: now,
        updatedAt: now,
      }).where(eq(smAssignmentTimeChangeRequests.id, request.id)).returning();
      return { request: requireWrittenRow(updatedRow), replayed: false };
    });
    res.status(200).json({ request: publicTimeChangeRequest(result.request), replayed: result.replayed });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.post("/time-change-requests/:id/reject", async (req: AuthedRequest, res, next) => {
  try {
    const id = z.string().uuid().safeParse(req.params.id);
    const parsed = timeChangeReviewSchema.safeParse(req.body ?? {});
    if (!id.success || !parsed.success) throw new SmPlanningError(400, "sm_assignment_time_request_review_invalid", "Die Ablehnung ist ungültig.");
    const actorUserId = req.authUser!.appUserId;
    const result = await db.transaction(async (tx) => {
      const [identity] = await tx.select({ assignmentId: smAssignmentTimeChangeRequests.assignmentId }).from(smAssignmentTimeChangeRequests).where(and(
        eq(smAssignmentTimeChangeRequests.id, id.data),
        eq(smAssignmentTimeChangeRequests.isDeleted, false),
      )).limit(1);
      if (!identity) throw new SmPlanningError(404, "sm_assignment_time_request_not_found", "Die Korrekturanfrage wurde nicht gefunden.");
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_time_request:${identity.assignmentId}`}, 0))`);
      await loadAssignmentForUpdate(tx, identity.assignmentId);
      const [request] = await tx.select().from(smAssignmentTimeChangeRequests).where(and(
        eq(smAssignmentTimeChangeRequests.id, id.data),
        eq(smAssignmentTimeChangeRequests.isDeleted, false),
      )).limit(1).for("update");
      if (!request) throw new SmPlanningError(404, "sm_assignment_time_request_not_found", "Die Korrekturanfrage wurde nicht gefunden.");
      if (request.status === "rejected") return { request, replayed: true };
      if (request.status !== "pending") throw new SmPlanningError(409, "sm_assignment_time_request_closed", "Diese Korrekturanfrage wurde bereits bearbeitet.");
      const now = new Date();
      const [updatedRow] = await tx.update(smAssignmentTimeChangeRequests).set({
        status: "rejected",
        reviewedByUserId: actorUserId,
        reviewedAt: now,
        adminNote: parsed.data.adminNote?.trim() || null,
        updatedAt: now,
      }).where(eq(smAssignmentTimeChangeRequests.id, request.id)).returning();
      return { request: requireWrittenRow(updatedRow), replayed: false };
    });
    res.status(200).json({ request: publicTimeChangeRequest(result.request), replayed: result.replayed });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.get("/assignments/:id/reassign-preview", async (req, res, next) => {
  try {
    const id = z.string().uuid().safeParse(req.params.id);
    const query = z.object({ smUserId: z.string().uuid() }).safeParse(req.query);
    if (!id.success || !query.success) throw new SmPlanningError(400, "sm_assignment_invalid", "Die Vorschau ist ungültig.");
    const [assignment] = await db.select().from(smAssignments).where(and(
      eq(smAssignments.id, id.data),
      eq(smAssignments.isDeleted, false),
    )).limit(1);
    if (!assignment) throw new SmPlanningError(404, "sm_assignment_not_found", "Der Einsatz wurde nicht gefunden.");
    if (!assignment.seriesId) throw new SmPlanningError(400, "sm_assignment_not_series", "Dieser Einsatz gehört zu keiner Serie.");
    await loadActiveSmUser(db, query.data.smUserId);
    const effectiveFromDate = resolveSmAssignmentValues(assignment).workDate;
    const effectiveDate = sql<string>`coalesce(${smAssignments.replacementWorkDate}, ${smAssignments.originalWorkDate})`;
    const futureRows = await db.select({ status: smAssignments.status }).from(smAssignments).where(and(
      eq(smAssignments.seriesId, assignment.seriesId),
      eq(smAssignments.isDeleted, false),
      gte(effectiveDate, effectiveFromDate),
    ));
    const affectedCount = futureRows.filter((row) => isAssignmentPlanningMutable(row.status)).length;
    res.status(200).json({
      effectiveFromDate,
      affectedCount,
      skippedCount: futureRows.length - affectedCount,
    });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.post("/assignments", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = singleAssignmentSchema.safeParse(req.body);
    if (!parsed.success) throw new SmPlanningError(400, "sm_assignment_invalid", "Die Einsatzdaten sind ungültig.");
    const actorUserId = req.authUser!.appUserId;
    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_assignment:${parsed.data.idempotencyKey}`}, 0))`);
      const [existing] = await tx.select({ id: smAssignments.id }).from(smAssignments).where(and(
        eq(smAssignments.idempotencyKey, parsed.data.idempotencyKey),
        eq(smAssignments.isDeleted, false),
      )).limit(1);
      if (existing) return { id: existing.id, replayed: true };

      await requireConfiguredGlobalQuestionnaire(tx);

      const [market] = await Promise.all([
        loadActiveMarket(tx, parsed.data.smMarketId),
        loadActiveSmUser(tx, parsed.data.smUserId),
      ]);
      const [createdRow] = await tx.insert(smAssignments).values({
        sourceType: "single",
        idempotencyKey: parsed.data.idempotencyKey,
        originalWorkDate: parsed.data.workDate,
        originalSmUserId: parsed.data.smUserId,
        originalSmMarketId: market.id,
        originalMarketInternalId: market.internalMarketId!,
        originalPlannedMinutes: parsed.data.plannedMinutes,
        questionnaireVersionId: null,
        flatRateCents: parsed.data.flatRateCents ?? null,
        createdByUserId: actorUserId,
        updatedByUserId: actorUserId,
      }).returning();
      const created = requireWrittenRow(createdRow);
      await writeEvent(tx, { before: null, after: created, eventType: "created", actorUserId });
      return { id: created.id, replayed: false };
    });
    res.status(result.replayed ? 200 : 201).json({ assignmentId: result.id, replayed: result.replayed });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.post("/series", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = seriesSchema.safeParse(req.body);
    if (!parsed.success) throw new SmPlanningError(400, "sm_series_invalid", "Die Seriendaten sind ungültig.");
    const input = parsed.data;
    const weekdays = normalizeWeekdays(input.weekdays);
    if (weekdays.length !== input.weekdays.length) throw new SmPlanningError(400, "sm_series_weekdays_duplicate", "Wochentage dürfen nicht doppelt gewählt werden.");
    const daySpan = isoDateToEpochDay(input.validTo) - isoDateToEpochDay(input.validFrom);
    if (daySpan < 0) throw new SmPlanningError(400, "sm_series_range_invalid", "Das Serienende muss nach dem Beginn liegen.");
    if (daySpan > 731) throw new SmPlanningError(400, "sm_series_range_too_large", "Eine Serie darf höchstens zwei Jahre umfassen.");
    const dates = buildSmSeriesDates({ validFrom: input.validFrom, validTo: input.validTo, weekdays, frequency: input.frequency });
    if (dates.length === 0) throw new SmPlanningError(400, "sm_series_empty", "Der Zeitraum enthält keinen passenden Einsatztermin.");
    const actorUserId = req.authUser!.appUserId;

    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_series:${input.idempotencyKey}`}, 0))`);
      const [existing] = await tx.select({ id: smAssignmentSeries.id }).from(smAssignmentSeries).where(and(
        eq(smAssignmentSeries.idempotencyKey, input.idempotencyKey),
        eq(smAssignmentSeries.isDeleted, false),
      )).limit(1);
      if (existing) {
        const existingAssignments = await tx.select({ id: smAssignments.id }).from(smAssignments).where(and(eq(smAssignments.seriesId, existing.id), eq(smAssignments.isDeleted, false)));
        return { seriesId: existing.id, count: existingAssignments.length, replayed: true };
      }

      await requireConfiguredGlobalQuestionnaire(tx);

      const [market] = await Promise.all([
        loadActiveMarket(tx, input.smMarketId),
        loadActiveSmUser(tx, input.smUserId),
      ]);
      const [seriesRow] = await tx.insert(smAssignmentSeries).values({
        idempotencyKey: input.idempotencyKey,
        createdByUserId: actorUserId,
      }).returning();
      const series = requireWrittenRow(seriesRow);
      const [versionRow] = await tx.insert(smAssignmentSeriesVersions).values({
        seriesId: series.id,
        versionNumber: 1,
        effectiveFromDate: input.validFrom,
        smMarketId: market.id,
        marketInternalIdSnapshot: market.internalMarketId!,
        defaultSmUserId: input.smUserId,
        plannedMinutes: input.plannedMinutes,
        questionnaireVersionId: null,
        flatRateCents: input.flatRateCents ?? null,
        frequency: input.frequency,
        weekdays,
        validFrom: input.validFrom,
        validTo: input.validTo,
        createdByUserId: actorUserId,
      }).returning();
      const version = requireWrittenRow(versionRow);
      const created = await tx.insert(smAssignments).values(dates.map((workDate) => ({
        sourceType: "series" as const,
        seriesId: series.id,
        seriesVersionId: version.id,
        seriesOccurrenceKey: workDate,
        idempotencyKey: `${input.idempotencyKey}:${workDate}`,
        originalWorkDate: workDate,
        originalSmUserId: input.smUserId,
        originalSmMarketId: market.id,
        originalMarketInternalId: market.internalMarketId!,
        originalPlannedMinutes: input.plannedMinutes,
        questionnaireVersionId: null,
        flatRateCents: input.flatRateCents ?? null,
        createdByUserId: actorUserId,
        updatedByUserId: actorUserId,
      }))).returning();
      await tx.insert(smAssignmentEvents).values(created.map((assignment) => ({
        assignmentId: assignment.id,
        seriesId: series.id,
        eventType: "created" as const,
        actorUserId,
        beforeState: {},
        afterState: assignmentState(assignment),
      })));
      return { seriesId: series.id, count: created.length, replayed: false };
    });
    res.status(result.replayed ? 200 : 201).json(result);
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.patch("/assignments/:id", async (req: AuthedRequest, res, next) => {
  try {
    const id = z.string().uuid().safeParse(req.params.id);
    const parsed = updateOccurrenceSchema.safeParse(req.body);
    if (!id.success || !parsed.success) throw new SmPlanningError(400, "sm_assignment_invalid", "Die Einsatzänderung ist ungültig.");
    const actorUserId = req.authUser!.appUserId;
    const result = await db.transaction(async (tx) => {
      const before = await loadAssignmentForUpdate(tx, id.data);
      assertExpectedUpdatedAt(before, parsed.data.expectedUpdatedAt);
      assertPlanningMutable(before);
      const set: Partial<typeof smAssignments.$inferInsert> = { updatedAt: new Date(), updatedByUserId: actorUserId };
      if (parsed.data.plannedMinutes !== undefined) {
        set.replacementPlannedMinutes = replacementOrNull(before.originalPlannedMinutes, parsed.data.plannedMinutes);
      }
      if (parsed.data.smMarketId !== undefined) {
        const market = await loadActiveMarket(tx, parsed.data.smMarketId);
        if (market.id === before.originalSmMarketId) {
          set.replacementSmMarketId = null;
          set.replacementMarketInternalId = null;
        } else {
          set.replacementSmMarketId = market.id;
          set.replacementMarketInternalId = market.internalMarketId!;
        }
      }
      const [afterRow] = await tx.update(smAssignments).set(set).where(eq(smAssignments.id, before.id)).returning();
      const after = requireWrittenRow(afterRow);
      const eventType = parsed.data.smMarketId !== undefined ? "market_replaced" : "updated";
      await writeEvent(tx, { before, after, eventType, actorUserId, reason: parsed.data.reason });
      return after;
    });
    res.status(200).json({ assignmentId: result.id, updatedAt: result.updatedAt.toISOString() });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.post("/assignments/:id/reschedule", async (req: AuthedRequest, res, next) => {
  try {
    const id = z.string().uuid().safeParse(req.params.id);
    const parsed = rescheduleSchema.safeParse(req.body);
    if (!id.success || !parsed.success) throw new SmPlanningError(400, "sm_assignment_invalid", "Die Verschiebung ist ungültig.");
    const actorUserId = req.authUser!.appUserId;
    const after = await db.transaction(async (tx) => {
      const before = await loadAssignmentForUpdate(tx, id.data);
      assertExpectedUpdatedAt(before, parsed.data.expectedUpdatedAt);
      assertPlanningMutable(before);
      const replacementWorkDate = replacementOrNull(before.originalWorkDate, parsed.data.workDate);
      const [updatedRow] = await tx.update(smAssignments).set({ replacementWorkDate, updatedAt: new Date(), updatedByUserId: actorUserId }).where(eq(smAssignments.id, before.id)).returning();
      const updated = requireWrittenRow(updatedRow);
      await writeEvent(tx, { before, after: updated, eventType: "rescheduled", actorUserId, reason: parsed.data.reason });
      return updated;
    });
    res.status(200).json({ assignmentId: after.id, updatedAt: after.updatedAt.toISOString() });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.post("/assignments/:id/reassign", async (req: AuthedRequest, res, next) => {
  try {
    const id = z.string().uuid().safeParse(req.params.id);
    const parsed = reassignSchema.safeParse(req.body);
    if (!id.success || !parsed.success) throw new SmPlanningError(400, "sm_assignment_invalid", "Die SM-Änderung ist ungültig.");
    const actorUserId = req.authUser!.appUserId;
    const result = await db.transaction(async (tx) => {
      const before = await loadAssignmentForUpdate(tx, id.data);
      assertExpectedUpdatedAt(before, parsed.data.expectedUpdatedAt);
      assertPlanningMutable(before);
      await loadActiveSmUser(tx, parsed.data.smUserId);
      if (parsed.data.scope === "occurrence") {
        const replacementSmUserId = replacementOrNull(before.originalSmUserId, parsed.data.smUserId);
        const [afterRow] = await tx.update(smAssignments).set({ replacementSmUserId, updatedAt: new Date(), updatedByUserId: actorUserId }).where(eq(smAssignments.id, before.id)).returning();
        const after = requireWrittenRow(afterRow);
        await writeEvent(tx, { before, after, eventType: "sm_replaced", actorUserId, reason: parsed.data.reason });
        return { affectedCount: 1, skippedCount: 0, updatedAt: after.updatedAt.toISOString() };
      }

      if (!before.seriesId) throw new SmPlanningError(400, "sm_assignment_not_series", "Dieser Einsatz gehört zu keiner Serie.");
      await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_series_update:${before.seriesId}`}, 0))`);
      const [latestVersion] = await tx.select().from(smAssignmentSeriesVersions).where(and(
        eq(smAssignmentSeriesVersions.seriesId, before.seriesId),
        eq(smAssignmentSeriesVersions.isDeleted, false),
      )).orderBy(desc(smAssignmentSeriesVersions.versionNumber)).limit(1);
      if (!latestVersion) throw new SmPlanningError(409, "sm_series_version_missing", "Die Serienversion wurde nicht gefunden.");
      const effectiveFromDate = resolveSmAssignmentValues(before).workDate;
      if (effectiveFromDate < latestVersion.effectiveFromDate || effectiveFromDate > latestVersion.validTo) {
        throw new SmPlanningError(409, "sm_series_effective_date_invalid", "Die dauerhafte Änderung liegt außerhalb der aktuellen Serienversion.");
      }

      if (latestVersion.defaultSmUserId !== parsed.data.smUserId) {
        await tx.insert(smAssignmentSeriesVersions).values({
          seriesId: latestVersion.seriesId,
          versionNumber: latestVersion.versionNumber + 1,
          effectiveFromDate,
          smMarketId: latestVersion.smMarketId,
          marketInternalIdSnapshot: latestVersion.marketInternalIdSnapshot,
          defaultSmUserId: parsed.data.smUserId,
          plannedMinutes: latestVersion.plannedMinutes,
          questionnaireVersionId: null,
          flatRateCents: latestVersion.flatRateCents,
          currency: latestVersion.currency,
          frequency: latestVersion.frequency,
          weekdays: latestVersion.weekdays,
          validFrom: latestVersion.validFrom,
          validTo: latestVersion.validTo,
          changeReason: parsed.data.reason,
          createdByUserId: actorUserId,
        });
      }

      const effectiveDate = sql<string>`coalesce(${smAssignments.replacementWorkDate}, ${smAssignments.originalWorkDate})`;
      const futureRows = await tx.select().from(smAssignments).where(and(
        eq(smAssignments.seriesId, before.seriesId),
        eq(smAssignments.isDeleted, false),
        gte(effectiveDate, effectiveFromDate),
      )).for("update");
      const eligible = futureRows.filter((row) => isAssignmentPlanningMutable(row.status));
      if (eligible.length === 0) throw new SmPlanningError(409, "sm_series_no_editable_future", "Die Serie enthält keine änderbaren zukünftigen Einsätze.");
      const eligibleIds = eligible.map((row) => row.id);
      const updatedRows = await tx.update(smAssignments).set({
        replacementSmUserId: sql`case when ${smAssignments.originalSmUserId} = ${parsed.data.smUserId}::uuid then null else ${parsed.data.smUserId}::uuid end`,
        updatedAt: new Date(),
        updatedByUserId: actorUserId,
      }).where(inArray(smAssignments.id, eligibleIds)).returning();
      const beforeById = new Map(eligible.map((row) => [row.id, row]));
      await tx.insert(smAssignmentEvents).values(updatedRows.map((after) => ({
        assignmentId: after.id,
        seriesId: after.seriesId,
        eventType: "series_future_sm_changed" as const,
        actorUserId,
        reason: parsed.data.reason,
        beforeState: assignmentState(beforeById.get(after.id)!),
        afterState: assignmentState(after),
      })));
      const selectedAfter = updatedRows.find((row) => row.id === before.id);
      return {
        affectedCount: updatedRows.length,
        skippedCount: futureRows.length - updatedRows.length,
        updatedAt: selectedAfter?.updatedAt.toISOString() ?? before.updatedAt.toISOString(),
      };
    });
    res.status(200).json(result);
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.post("/assignments/:id/cancel", async (req: AuthedRequest, res, next) => {
  try {
    const id = z.string().uuid().safeParse(req.params.id);
    const parsed = cancelSchema.safeParse(req.body);
    if (!id.success || !parsed.success) throw new SmPlanningError(400, "sm_assignment_invalid", "Die Absage ist ungültig.");
    const actorUserId = req.authUser!.appUserId;
    const after = await db.transaction(async (tx) => {
      const before = await loadAssignmentForUpdate(tx, id.data);
      assertExpectedUpdatedAt(before, parsed.data.expectedUpdatedAt);
      assertPlanningMutable(before);
      const [updatedRow] = await tx.update(smAssignments).set({
        status: "cancelled",
        statusBeforeCancellation: before.status,
        cancelledAt: new Date(),
        cancelledByUserId: actorUserId,
        cancellationReason: parsed.data.reason,
        updatedAt: new Date(),
        updatedByUserId: actorUserId,
      }).where(eq(smAssignments.id, before.id)).returning();
      const updated = requireWrittenRow(updatedRow);
      await writeEvent(tx, { before, after: updated, eventType: "cancelled", actorUserId, reason: parsed.data.reason });
      return updated;
    });
    res.status(200).json({ assignmentId: after.id, updatedAt: after.updatedAt.toISOString() });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.post("/assignments/:id/restore", async (req: AuthedRequest, res, next) => {
  try {
    const id = z.string().uuid().safeParse(req.params.id);
    const parsed = restoreSchema.safeParse(req.body);
    if (!id.success || !parsed.success) throw new SmPlanningError(400, "sm_assignment_invalid", "Die Wiederherstellung ist ungültig.");
    const actorUserId = req.authUser!.appUserId;
    const after = await db.transaction(async (tx) => {
      const before = await loadAssignmentForUpdate(tx, id.data);
      assertExpectedUpdatedAt(before, parsed.data.expectedUpdatedAt);
      if (before.status !== "cancelled") throw new SmPlanningError(409, "sm_assignment_not_cancelled", "Der Einsatz ist nicht abgesagt.");
      const [updatedRow] = await tx.update(smAssignments).set({
        status: before.statusBeforeCancellation ?? "planned",
        statusBeforeCancellation: null,
        cancelledAt: null,
        cancelledByUserId: null,
        cancellationReason: null,
        updatedAt: new Date(),
        updatedByUserId: actorUserId,
      }).where(eq(smAssignments.id, before.id)).returning();
      const updated = requireWrittenRow(updatedRow);
      await writeEvent(tx, { before, after: updated, eventType: "restored", actorUserId, reason: parsed.data.reason });
      return updated;
    });
    res.status(200).json({ assignmentId: after.id, updatedAt: after.updatedAt.toISOString() });
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});

adminSmPlanningRouter.post("/assignments/:id/time", async (req: AuthedRequest, res, next) => {
  try {
    const id = z.string().uuid().safeParse(req.params.id);
    const parsed = actualTimeSchema.safeParse(req.body);
    if (!id.success || !parsed.success) throw new SmPlanningError(400, "sm_assignment_time_invalid", "Die Ist-Zeit ist ungültig.");
    const actorUserId = req.authUser!.appUserId;
    const result = await db.transaction(async (tx) => {
      const assignment = await loadAssignmentForUpdate(tx, id.data);
      if (assignment.status === "cancelled") {
        throw new SmPlanningError(409, "sm_assignment_time_cancelled", "Für einen abgesagten Einsatz kann keine Ist-Zeit gespeichert werden.");
      }
      const [current] = await tx.select().from(smAssignmentTimeSubmissions).where(and(
        eq(smAssignmentTimeSubmissions.assignmentId, assignment.id),
        eq(smAssignmentTimeSubmissions.isDeleted, false),
        eq(smAssignmentTimeSubmissions.isCurrent, true),
      )).limit(1).for("update");
      if (current?.actualMinutes === parsed.data.actualMinutes) {
        return { submissionId: current.id, revisionNumber: current.revisionNumber, actualMinutes: current.actualMinutes, replayed: true };
      }
      if (current && !parsed.data.correctionReason) {
        throw new SmPlanningError(400, "sm_assignment_time_reason_required", "Für eine Korrektur ist eine Begründung erforderlich.");
      }
      if (current) {
        await tx.update(smAssignmentTimeSubmissions).set({ isCurrent: false, updatedAt: new Date() }).where(eq(smAssignmentTimeSubmissions.id, current.id));
      }
      const [createdRow] = await tx.insert(smAssignmentTimeSubmissions).values({
        assignmentId: assignment.id,
        revisionNumber: (current?.revisionNumber ?? 0) + 1,
        actualMinutes: parsed.data.actualMinutes,
        isCurrent: true,
        supersedesSubmissionId: current?.id ?? null,
        submittedByUserId: actorUserId,
        correctionReason: current ? parsed.data.correctionReason! : null,
      }).returning();
      const created = requireWrittenRow(createdRow);
      return { submissionId: created.id, revisionNumber: created.revisionNumber, actualMinutes: created.actualMinutes, replayed: false };
    });
    res.status(result.replayed ? 200 : 201).json(result);
  } catch (error) {
    if (!sendKnownError(error, res)) next(error);
  }
});
