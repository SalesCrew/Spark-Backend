import { and, asc, eq, inArray, sql } from "drizzle-orm";
import { db } from "./db.js";
import { campaignMarketAssignmentHistory, marketKuehlerUnits, visitSessions, visitSessionSections } from "./schema.js";
import { buildKuehlerAssignmentProgress, type KuehlerProgressAssignment } from "./kuehler-assignment-progress.js";

/** Read-only and bounded to the caller's active campaign/market assignments.
 * Only completion metadata is loaded, never another GM's answer contents.
 * Authorship and all original database records stay untouched.
 */
export async function loadKuehlerAssignmentProgress(assignments: readonly KuehlerProgressAssignment[]) {
  const campaignIds = [...new Set(assignments.map((row) => row.campaignId))];
  const marketIds = [...new Set(assignments.map((row) => row.marketId))];
  if (campaignIds.length === 0 || marketIds.length === 0) return new Map<string, KuehlerProgressSlots>();
  const [units, submissions, history] = await Promise.all([
    db.select().from(marketKuehlerUnits).where(and(
      inArray(marketKuehlerUnits.marketId, marketIds), eq(marketKuehlerUnits.isDeleted, false),
    )).orderBy(asc(marketKuehlerUnits.kuehlerInternalId), asc(marketKuehlerUnits.createdAt)),
    db.select({
      campaignId: visitSessionSections.campaignId,
      marketId: visitSessions.marketId,
      sessionId: visitSessions.id,
      gmUserId: visitSessions.gmUserId,
      kuehlerUnitId: visitSessions.kuehlerUnitId,
      startedAt: visitSessions.startedAt,
      submittedAt: visitSessions.submittedAt,
      createdAt: visitSessions.createdAt,
    }).from(visitSessionSections).innerJoin(visitSessions, eq(visitSessions.id, visitSessionSections.visitSessionId)).where(and(
      inArray(visitSessionSections.campaignId, campaignIds), inArray(visitSessions.marketId, marketIds),
      eq(visitSessionSections.section, "kuehler"), eq(visitSessionSections.isDeleted, false),
      eq(visitSessions.isDeleted, false), eq(visitSessions.status, "submitted"),
    )),
    db.select().from(campaignMarketAssignmentHistory).where(and(
      inArray(campaignMarketAssignmentHistory.fromCampaignId, campaignIds),
      inArray(campaignMarketAssignmentHistory.marketId, marketIds),
      eq(campaignMarketAssignmentHistory.section, "kuehler"),
      eq(campaignMarketAssignmentHistory.reason, "campaign_gm_reassignment"),
      sql`${campaignMarketAssignmentHistory.fromCampaignId} = ${campaignMarketAssignmentHistory.toCampaignId}`,
    )),
  ]);
  return buildKuehlerAssignmentProgress(assignments, units, submissions, history);
}

type KuehlerProgressSlots = ReturnType<typeof buildKuehlerAssignmentProgress<
  typeof marketKuehlerUnits.$inferSelect,
  { campaignId: string; marketId: string; sessionId: string; gmUserId: string; kuehlerUnitId: string | null; startedAt: Date; submittedAt: Date | null; createdAt: Date }
>> extends Map<string, infer Slots> ? Slots : never;
