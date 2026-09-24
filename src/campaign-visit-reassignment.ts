import { and, asc, eq, inArray, sql } from "drizzle-orm";
import type { db } from "./lib/db.js";
import { buildKuehlerAssignmentProgress, createKuehlerSubmissionAssigneeResolver, kuehlerProgressKey } from "./lib/kuehler-assignment-progress.js";
import { campaignMarketAssignmentHistory, campaignMarketAssignments, campaigns, marketKuehlerUnits, visitSessions, visitSessionSections } from "./lib/schema.js";

type DbTx = Parameters<Parameters<typeof db.transaction>[0]>[0];

type AssignmentProgressRow = {
  id: string;
  marketId: string;
  gmUserId: string | null;
  assignmentSlot: number;
  visitTargetCount: number;
  currentVisitsCount: number;
};
type SubmittedVisitRow = {
  sessionId: string;
  marketId: string;
  gmUserId: string;
  submittedAt: Date | null;
};
type StartedVisitRow = {
  sessionId: string;
  marketId: string;
  gmUserId: string;
};
type BulkMoveRow = {
  marketId: string;
  fromGmUserId: string | null;
  toGmUserId: string | null;
  migratedAt: Date;
};

/** Submitted sessions are authoritative; the legacy current_visits_count is not maintained by submission. */
export function allocateCompletedCampaignVisits(
  assignments: readonly AssignmentProgressRow[],
  submissions: readonly SubmittedVisitRow[],
  bulkMoves: readonly BulkMoveRow[],
) {
  return allocateCampaignVisitProgress(assignments, submissions, [], bulkMoves).completedByAssignmentId;
}

export function allocateCampaignVisitProgress(
  assignments: readonly AssignmentProgressRow[],
  submissions: readonly SubmittedVisitRow[],
  drafts: readonly StartedVisitRow[],
  bulkMoves: readonly BulkMoveRow[],
) {
  const movesByMarket = new Map<string, Map<number, Map<string, string | null>>>();
  for (const move of bulkMoves) {
    if (!move.fromGmUserId || !move.toGmUserId) continue;
    const timeline = movesByMarket.get(move.marketId) ?? new Map<number, Map<string, string | null>>();
    const at = move.migratedAt.getTime();
    const transfers = timeline.get(at) ?? new Map<string, string | null>();
    const prior = transfers.get(move.fromGmUserId);
    transfers.set(move.fromGmUserId, prior === undefined || prior === move.toGmUserId ? move.toGmUserId : null);
    timeline.set(at, transfers);
    movesByMarket.set(move.marketId, timeline);
  }
  const sortedMoves = new Map([...movesByMarket].map(([marketId, timeline]) => [marketId, [...timeline].sort(([a], [b]) => a - b)]));

  const submittedByMarketGm = new Map<string, Set<string>>();
  for (const submission of submissions) {
    let gmUserId = submission.gmUserId;
    for (const [at, transfers] of sortedMoves.get(submission.marketId) ?? []) {
      if (!submission.submittedAt || at < submission.submittedAt.getTime()) continue;
      gmUserId = transfers.get(gmUserId) ?? gmUserId;
    }
    const key = `${submission.marketId}:${gmUserId}`;
    const sessions = submittedByMarketGm.get(key) ?? new Set<string>();
    sessions.add(submission.sessionId);
    submittedByMarketGm.set(key, sessions);
  }

  const draftByMarketGm = new Map<string, Set<string>>();
  for (const draft of drafts) {
    const key = `${draft.marketId}:${draft.gmUserId}`;
    const sessions = draftByMarketGm.get(key) ?? new Set<string>();
    sessions.add(draft.sessionId);
    draftByMarketGm.set(key, sessions);
  }

  const completedByAssignmentId = new Map<string, number>();
  const startedByAssignmentId = new Map<string, number>();
  const ordered = [...assignments].sort((a, b) => a.marketId.localeCompare(b.marketId)
    || String(a.gmUserId).localeCompare(String(b.gmUserId))
    || a.assignmentSlot - b.assignmentSlot || a.id.localeCompare(b.id));
  const consumedByMarketGm = new Map<string, number>();
  const consumedDraftsByMarketGm = new Map<string, number>();
  for (const assignment of ordered) {
    const key = `${assignment.marketId}:${assignment.gmUserId ?? "unassigned"}`;
    const consumed = consumedByMarketGm.get(key) ?? 0;
    const submitted = submittedByMarketGm.get(key)?.size ?? 0;
    const actual = Math.min(assignment.visitTargetCount, Math.max(0, submitted - consumed));
    const completed = Math.min(assignment.visitTargetCount, Math.max(actual, assignment.currentVisitsCount));
    completedByAssignmentId.set(assignment.id, completed);
    const draftCapacity = assignment.visitTargetCount - completed;
    const draftCount = draftByMarketGm.get(key)?.size ?? 0;
    const consumedDrafts = consumedDraftsByMarketGm.get(key) ?? 0;
    startedByAssignmentId.set(assignment.id, Math.min(draftCapacity, Math.max(0, draftCount - consumedDrafts)));
    consumedByMarketGm.set(key, consumed + assignment.visitTargetCount);
    consumedDraftsByMarketGm.set(key, consumedDrafts + draftCapacity);
  }
  return { completedByAssignmentId, startedByAssignmentId };
}

export async function loadCampaignVisitProgress(tx: DbTx, campaignId: string) {
  const [campaign] = await tx.select({ section: campaigns.section }).from(campaigns)
    .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false))).limit(1);
  if (!campaign) throw new CampaignVisitReassignmentError("campaign_not_found", 404, "Kampagne nicht gefunden.");
  if (campaign.section === "flex") return {
    completedByAssignmentId: new Map<string, number>(), startedByAssignmentId: new Map<string, number>(),
  };
  const [assignments, visits, history] = await Promise.all([
    tx.select({
      id: campaignMarketAssignments.id, marketId: campaignMarketAssignments.marketId,
      gmUserId: campaignMarketAssignments.gmUserId, assignmentSlot: campaignMarketAssignments.assignmentSlot,
      visitTargetCount: campaignMarketAssignments.visitTargetCount, currentVisitsCount: campaignMarketAssignments.currentVisitsCount,
    }).from(campaignMarketAssignments).where(and(eq(campaignMarketAssignments.campaignId, campaignId), eq(campaignMarketAssignments.isDeleted, false))),
    tx.select({
      sessionId: visitSessions.id, marketId: visitSessions.marketId, gmUserId: visitSessions.gmUserId,
      submittedAt: visitSessions.submittedAt, status: visitSessions.status,
      kuehlerUnitId: visitSessions.kuehlerUnitId,
    }).from(visitSessionSections).innerJoin(visitSessions, eq(visitSessions.id, visitSessionSections.visitSessionId))
      .where(and(eq(visitSessionSections.campaignId, campaignId), eq(visitSessionSections.isDeleted, false),
        eq(visitSessionSections.section, campaign.section), eq(visitSessions.isDeleted, false),
        inArray(visitSessions.status, ["draft", "submitted"]))),
    tx.select({
      marketId: campaignMarketAssignmentHistory.marketId,
      fromGmUserId: campaignMarketAssignmentHistory.fromGmUserId,
      toGmUserId: campaignMarketAssignmentHistory.toGmUserId,
      migratedAt: campaignMarketAssignmentHistory.migratedAt,
      section: campaignMarketAssignmentHistory.section,
      fromCampaignId: campaignMarketAssignmentHistory.fromCampaignId,
      toCampaignId: campaignMarketAssignmentHistory.toCampaignId,
      reason: campaignMarketAssignmentHistory.reason,
    }).from(campaignMarketAssignmentHistory).where(and(
      eq(campaignMarketAssignmentHistory.fromCampaignId, campaignId), eq(campaignMarketAssignmentHistory.toCampaignId, campaignId),
      eq(campaignMarketAssignmentHistory.section, campaign.section),
      eq(campaignMarketAssignmentHistory.reason, "campaign_gm_reassignment"),
    )),
  ]);
  if (campaign.section === "kuehler") {
    const marketIds = [...new Set(assignments.map((row) => row.marketId))];
    const units = marketIds.length > 0 ? await tx.select({ id: marketKuehlerUnits.id, marketId: marketKuehlerUnits.marketId })
      .from(marketKuehlerUnits).where(and(inArray(marketKuehlerUnits.marketId, marketIds), eq(marketKuehlerUnits.isDeleted, false)))
      .orderBy(asc(marketKuehlerUnits.kuehlerInternalId), asc(marketKuehlerUnits.createdAt)) : [];
    const slotsByMarketGm = buildKuehlerAssignmentProgress(
      assignments.map((row) => ({ campaignId, marketId: row.marketId, gmUserId: row.gmUserId, visitTargetCount: row.visitTargetCount })),
      units,
      visits.filter((visit) => visit.status === "submitted" && visit.submittedAt).map((visit) => ({ ...visit, campaignId })),
      history.map((row) => ({ ...row, marketId: row.marketId, section: row.section, reason: row.reason })),
    );
    const resolveAssignee = createKuehlerSubmissionAssigneeResolver(history.map((row) => ({ ...row, section: row.section, reason: row.reason })));
    const attributedSessionIds = new Map<string, Set<string>>();
    for (const visit of visits) {
      if (visit.status !== "submitted" || !visit.submittedAt) continue;
      const key = kuehlerProgressKey(campaignId, visit.marketId, resolveAssignee({ ...visit, campaignId }));
      const ids = attributedSessionIds.get(key) ?? new Set<string>();
      ids.add(visit.sessionId);
      attributedSessionIds.set(key, ids);
    }
    const completedByAssignmentId = new Map<string, number>();
    const startedByAssignmentId = new Map<string, number>();
    const offsets = new Map<string, number>();
    const targetTotals = new Map<string, number>();
    const draftKeys = new Set(visits.filter((visit) => visit.status === "draft")
      .map((visit) => kuehlerProgressKey(campaignId, visit.marketId, visit.gmUserId)));
    const submittedMarkets = new Set(visits.filter((visit) => visit.status === "submitted")
      .map((visit) => visit.marketId));
    const draftMarkets = new Set(visits.filter((visit) => visit.status === "draft")
      .map((visit) => visit.marketId));
    for (const assignment of assignments) {
      const key = kuehlerProgressKey(campaignId, assignment.marketId, assignment.gmUserId);
      targetTotals.set(key, (targetTotals.get(key) ?? 0) + assignment.visitTargetCount);
    }
    const hasUnmappedSubmissionByKey = new Map<string, boolean>();
    for (const [key, allSlots] of slotsByMarketGm) {
      const representedIds = new Set(allSlots.flatMap((slot) => slot.submission ? [slot.submission.sessionId] : []));
      hasUnmappedSubmissionByKey.set(key,
        allSlots.slice(targetTotals.get(key) ?? 0).some((slot) => Boolean(slot.submission))
        || [...(attributedSessionIds.get(key) ?? [])].some((id) => !representedIds.has(id)));
    }
    for (const assignment of [...assignments].sort((a, b) => a.marketId.localeCompare(b.marketId)
      || String(a.gmUserId).localeCompare(String(b.gmUserId)) || a.assignmentSlot - b.assignmentSlot || a.id.localeCompare(b.id))) {
      const key = kuehlerProgressKey(campaignId, assignment.marketId, assignment.gmUserId);
      const offset = offsets.get(key) ?? 0;
      const allSlots = slotsByMarketGm.get(key) ?? [];
      const ownSlots = allSlots.slice(offset, offset + assignment.visitTargetCount);
      // Legacy targets can be smaller than the number of coolers. A submission
      // in an overflow slot (or on a removed cooler) is still work on this visit.
      const hasUnmappedSubmission = hasUnmappedSubmissionByKey.get(key) ?? false;
      completedByAssignmentId.set(assignment.id, Math.min(assignment.visitTargetCount,
        Math.max(assignment.currentVisitsCount, ownSlots.filter((slot) => Boolean(slot.submission)).length, hasUnmappedSubmission ? 1 : 0,
          assignment.gmUserId === null && submittedMarkets.has(assignment.marketId) ? assignment.visitTargetCount : 0)));
      // A draft cannot be assigned to a particular cooler round yet. Lock every
      // assignment for that market/GM rather than risk moving its draft away.
      startedByAssignmentId.set(assignment.id, draftKeys.has(key) ? 1 : assignment.gmUserId === null
        && draftMarkets.has(assignment.marketId) ? assignment.visitTargetCount : 0);
      offsets.set(key, offset + assignment.visitTargetCount);
    }
    return { completedByAssignmentId, startedByAssignmentId };
  }
  const progress = allocateCampaignVisitProgress(
    assignments,
    visits.filter((visit) => visit.status === "submitted" && visit.submittedAt),
    visits.filter((visit) => visit.status === "draft"),
    history,
  );
  // Legacy unassigned rows were never tied to a specific GM. Any existing
  // session for that market makes their ownership ambiguous, so fail closed.
  const submittedMarkets = new Set(visits.filter((visit) => visit.status === "submitted").map((visit) => visit.marketId));
  const draftMarkets = new Set(visits.filter((visit) => visit.status === "draft").map((visit) => visit.marketId));
  for (const assignment of assignments) {
    if (assignment.gmUserId !== null) continue;
    if (submittedMarkets.has(assignment.marketId)) progress.completedByAssignmentId.set(assignment.id, assignment.visitTargetCount);
    if (draftMarkets.has(assignment.marketId)) progress.startedByAssignmentId.set(assignment.id, assignment.visitTargetCount);
  }
  return progress;
}

export async function loadCampaignVisitCompletionCounts(tx: DbTx, campaignId: string) {
  return (await loadCampaignVisitProgress(tx, campaignId)).completedByAssignmentId;
}

export class CampaignVisitReassignmentError extends Error {
  constructor(
    public readonly code: "campaign_not_found" | "assignment_not_found" | "assignment_changed" | "visit_already_completed" | "visit_already_started" | "visit_type_not_supported",
    public readonly status: number,
    message: string,
  ) { super(message); }
}

export type CampaignVisitMovePlan =
  | { ok: true; remainingSourceCount: number | null }
  | { ok: false; reason: "changed" | "completed" };

export function planCampaignVisitMove(input: {
  visitTargetCount: number;
  currentVisitsCount: number;
  expectedVisitTargetCount: number;
  visitNumber: number;
}): CampaignVisitMovePlan {
  if (input.visitTargetCount !== input.expectedVisitTargetCount
    || input.visitNumber < 1 || input.visitNumber > input.visitTargetCount) {
    return { ok: false, reason: "changed" };
  }
  if (input.visitNumber <= input.currentVisitsCount
    || input.currentVisitsCount >= input.visitTargetCount) {
    return { ok: false, reason: "completed" };
  }
  return { ok: true, remainingSourceCount: input.visitTargetCount === 1 ? null : input.visitTargetCount - 1 };
}

export async function reassignCampaignVisitTarget(tx: DbTx, input: {
  campaignId: string;
  assignmentId: string;
  toGmUserId: string;
  expectedGmUserId: string | null;
  expectedVisitTargetCount: number;
  visitNumber: number;
  auditUserId: string | null;
  now: Date;
}) {
  const [campaign] = await tx.select({ id: campaigns.id, section: campaigns.section })
    .from(campaigns)
    .where(and(eq(campaigns.id, input.campaignId), eq(campaigns.isDeleted, false)))
    .limit(1)
    .for("update");
  if (!campaign) throw new CampaignVisitReassignmentError("campaign_not_found", 404, "Kampagne nicht gefunden.");
  if (campaign.section === "flex") {
    throw new CampaignVisitReassignmentError("visit_type_not_supported", 409, "Flex-Besuche stehen allen GM zur Verfügung und werden nicht einzeln zugewiesen.");
  }

  const [assignment] = await tx.select().from(campaignMarketAssignments)
    .where(and(
      eq(campaignMarketAssignments.id, input.assignmentId),
      eq(campaignMarketAssignments.campaignId, input.campaignId),
      eq(campaignMarketAssignments.isDeleted, false),
    ))
    .limit(1)
    .for("update");
  if (!assignment) throw new CampaignVisitReassignmentError("assignment_not_found", 404, "Dieser geplante Besuch wurde nicht gefunden.");
  if (assignment.gmUserId !== input.expectedGmUserId) {
    throw new CampaignVisitReassignmentError("assignment_changed", 409, "Die Besuchsplanung wurde inzwischen geändert. Bitte neu laden.");
  }
  if (assignment.gmUserId === input.toGmUserId) return;
  const progress = await loadCampaignVisitProgress(tx, input.campaignId);
  const completedCount = progress.completedByAssignmentId.get(assignment.id) ?? assignment.currentVisitsCount;
  if (campaign.section === "kuehler") {
    if (assignment.visitTargetCount !== input.expectedVisitTargetCount || input.visitNumber !== 1) {
      throw new CampaignVisitReassignmentError("assignment_changed", 409, "Die Kühler-Planung wurde inzwischen geändert. Bitte neu laden.");
    }
    if (completedCount > 0) {
      throw new CampaignVisitReassignmentError("visit_already_completed", 409, "In diesem Kühler-Besuch wurden bereits Geräte erfasst. Er bleibt beim bisherigen GM.");
    }
    if ((progress.startedByAssignmentId.get(assignment.id) ?? 0) > 0) {
      throw new CampaignVisitReassignmentError("visit_already_started", 409, "Für diesen Markt gibt es einen begonnenen Kühler-Besuch. Bitte den Entwurf zuerst abschließen oder verwerfen.");
    }
    const targetSlots = await tx.select({ assignmentSlot: campaignMarketAssignments.assignmentSlot })
      .from(campaignMarketAssignments).where(and(
        eq(campaignMarketAssignments.campaignId, input.campaignId),
        eq(campaignMarketAssignments.marketId, assignment.marketId),
        eq(campaignMarketAssignments.gmUserId, input.toGmUserId),
        eq(campaignMarketAssignments.isDeleted, false),
      )).for("update");
    // Keep the recipient's existing rounds ahead of this new round. Otherwise
    // earlier submitted cooler sessions would appear to belong to the move.
    const targetSlot = Math.max(0, ...targetSlots.map((row) => row.assignmentSlot)) + 1;
    await tx.update(campaignMarketAssignments).set({
      gmUserId: input.toGmUserId, assignmentSlot: targetSlot,
      assignedAt: input.now, assignedByUserId: input.auditUserId, updatedAt: input.now,
    }).where(eq(campaignMarketAssignments.id, assignment.id));
    await tx.insert(campaignMarketAssignmentHistory).values({
      marketId: assignment.marketId, section: campaign.section,
      fromCampaignId: input.campaignId, toCampaignId: input.campaignId,
      fromGmUserId: assignment.gmUserId, toGmUserId: input.toGmUserId,
      migratedByUserId: input.auditUserId, migratedAt: input.now,
      reason: "campaign_visit_reassignment", createdAt: input.now,
    });
    return;
  }
  const plan = planCampaignVisitMove({
    visitTargetCount: assignment.visitTargetCount,
    currentVisitsCount: completedCount,
    expectedVisitTargetCount: input.expectedVisitTargetCount,
    visitNumber: input.visitNumber,
  });
  if (!plan.ok && plan.reason === "changed") {
    throw new CampaignVisitReassignmentError("assignment_changed", 409, "Die Besuchsplanung wurde inzwischen geändert. Bitte neu laden.");
  }
  if (!plan.ok) {
    throw new CampaignVisitReassignmentError("visit_already_completed", 409, "Ein bereits erledigter Besuch kann nicht umgeplant werden.");
  }
  if (input.visitNumber <= completedCount + (progress.startedByAssignmentId.get(assignment.id) ?? 0)) {
    throw new CampaignVisitReassignmentError("visit_already_started", 409, "Ein bereits begonnener Besuch kann nicht umgeplant werden. Bitte den Entwurf zuerst abschließen oder verwerfen.");
  }

  if (plan.remainingSourceCount === null) {
    await tx.update(campaignMarketAssignments).set({ isDeleted: true, deletedAt: input.now, updatedAt: input.now })
      .where(eq(campaignMarketAssignments.id, assignment.id));
  } else {
    await tx.update(campaignMarketAssignments).set({ visitTargetCount: plan.remainingSourceCount, updatedAt: input.now })
      .where(eq(campaignMarketAssignments.id, assignment.id));
  }

  const [target] = await tx.select({ id: campaignMarketAssignments.id })
    .from(campaignMarketAssignments)
    .where(and(
      eq(campaignMarketAssignments.campaignId, input.campaignId),
      eq(campaignMarketAssignments.marketId, assignment.marketId),
      eq(campaignMarketAssignments.gmUserId, input.toGmUserId),
      eq(campaignMarketAssignments.assignmentSlot, assignment.assignmentSlot),
      eq(campaignMarketAssignments.isDeleted, false),
    ))
    .limit(1)
    .for("update");
  if (target) {
    await tx.update(campaignMarketAssignments).set({
      visitTargetCount: sql`${campaignMarketAssignments.visitTargetCount} + 1`,
      assignedAt: input.now,
      assignedByUserId: input.auditUserId,
      updatedAt: input.now,
    }).where(eq(campaignMarketAssignments.id, target.id));
  } else {
    await tx.insert(campaignMarketAssignments).values({
      campaignId: input.campaignId,
      marketId: assignment.marketId,
      gmUserId: input.toGmUserId,
      assignmentSlot: assignment.assignmentSlot,
      visitTargetCount: 1,
      currentVisitsCount: 0,
      assignedAt: input.now,
      assignedByUserId: input.auditUserId,
      createdAt: input.now,
      updatedAt: input.now,
    });
  }
  await tx.insert(campaignMarketAssignmentHistory).values({
    marketId: assignment.marketId,
    section: campaign.section,
    fromCampaignId: input.campaignId,
    toCampaignId: input.campaignId,
    fromGmUserId: assignment.gmUserId,
    toGmUserId: input.toGmUserId,
    migratedByUserId: input.auditUserId,
    migratedAt: input.now,
    reason: "campaign_visit_reassignment",
    createdAt: input.now,
  });
}
