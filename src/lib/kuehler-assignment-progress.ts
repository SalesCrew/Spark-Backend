import { buildKuehlerVisitSlots, type KuehlerSubmission } from "./kuehler-repeat-visits.js";

export type KuehlerProgressAssignment = {
  campaignId: string;
  marketId: string;
  gmUserId: string | null;
  visitTargetCount: number;
};

export type KuehlerReassignment = {
  marketId: string;
  section: string;
  fromCampaignId: string | null;
  toCampaignId: string | null;
  fromGmUserId: string | null;
  toGmUserId: string | null;
  migratedAt: Date;
  reason: string | null;
};

export function kuehlerProgressKey(campaignId: string, marketId: string, gmUserId: string | null) {
  return `${campaignId}:${marketId}:${gmUserId ?? "unassigned"}`;
}

/** Project responsibility, never authorship. Duplicate history rows belong to
 * one atomic reassignment; swaps must advance only once per timestamp. Only
 * submissions completed before that transfer follow it. Other campaigns,
 * independent GMs, and ordinary market migrations must not share completions.
 */
export function createKuehlerSubmissionAssigneeResolver(history: readonly KuehlerReassignment[]) {
  const byScope = new Map<string, Map<number, Map<string, string | null>>>();
  for (const event of history) {
    if (event.section !== "kuehler" || event.reason !== "campaign_gm_reassignment"
      || !event.fromCampaignId || event.fromCampaignId !== event.toCampaignId
      || !event.fromGmUserId || !event.toGmUserId) continue;
    const scope = `${event.fromCampaignId}:${event.marketId}`;
    const at = event.migratedAt.getTime();
    if (!Number.isFinite(at)) continue;
    const timeline = byScope.get(scope) ?? new Map<number, Map<string, string | null>>();
    const transfers = timeline.get(at) ?? new Map<string, string | null>();
    // Fail closed if historical rows disagree about the target of one event.
    const existing = transfers.get(event.fromGmUserId);
    transfers.set(event.fromGmUserId, existing === undefined || existing === event.toGmUserId ? event.toGmUserId : null);
    timeline.set(at, transfers);
    byScope.set(scope, timeline);
  }
  const timelines = new Map([...byScope].map(([scope, timeline]) => [scope, [...timeline].sort(([a], [b]) => a - b)]));
  return (submission: { campaignId: string; marketId: string; gmUserId: string | null; submittedAt: Date | null }) => {
    let assignee = submission.gmUserId;
    const submittedAt = submission.submittedAt?.getTime();
    if (submittedAt == null || !Number.isFinite(submittedAt)) return assignee;
    for (const [at, transfers] of timelines.get(`${submission.campaignId}:${submission.marketId}`) ?? []) {
      if (at < submittedAt || !assignee) continue;
      assignee = transfers.get(assignee) ?? assignee;
    }
    return assignee;
  };
}

/** Shared by admin status, GM progress, market list and market-start summary.
 * Inputs are live assignments/units and non-deleted, submitted sessions only.
 * Each session can enter exactly one assignee's pool, retaining its author.
 */
export function buildKuehlerAssignmentProgress<
  Unit extends { id: string; marketId: string },
  Submission extends KuehlerSubmission & { campaignId: string; marketId: string; gmUserId: string | null },
>(assignments: readonly KuehlerProgressAssignment[], units: readonly Unit[], submissions: readonly Submission[], history: readonly KuehlerReassignment[]) {
  const resolveAssignee = createKuehlerSubmissionAssigneeResolver(history);
  const targets = new Map<string, KuehlerProgressAssignment>();
  for (const assignment of assignments) {
    const key = kuehlerProgressKey(assignment.campaignId, assignment.marketId, assignment.gmUserId);
    const previous = targets.get(key);
    targets.set(key, { ...assignment, visitTargetCount: (previous?.visitTargetCount ?? 0) + assignment.visitTargetCount });
  }
  const unitsByMarket = new Map<string, Unit[]>();
  for (const unit of units) {
    const bucket = unitsByMarket.get(unit.marketId) ?? [];
    bucket.push(unit);
    unitsByMarket.set(unit.marketId, bucket);
  }
  const submissionsByAssignment = new Map<string, Submission[]>();
  for (const submission of submissions) {
    const key = kuehlerProgressKey(submission.campaignId, submission.marketId, resolveAssignee(submission));
    if (!targets.has(key)) continue;
    const bucket = submissionsByAssignment.get(key) ?? [];
    bucket.push(submission);
    submissionsByAssignment.set(key, bucket);
  }
  return new Map([...targets].map(([key, assignment]) => [key,
    buildKuehlerVisitSlots(unitsByMarket.get(assignment.marketId) ?? [], assignment.visitTargetCount, submissionsByAssignment.get(key) ?? []),
  ]));
}
