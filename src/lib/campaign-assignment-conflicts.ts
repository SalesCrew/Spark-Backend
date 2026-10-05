// Shared overlap policy: keeps the existing completed-cooler exception. No environment imports.
import { and, eq, inArray, ne, sql } from "drizzle-orm";
import type { db } from "./db.js";
import { campaignMarketAssignments, campaigns, markets, users } from "./schema.js";
type CampaignSection = "standard" | "flex" | "billa" | "kuehler" | "mhd" | "durcharbeit";
export type CampaignReadDatabase = Pick<typeof db, "select">;
export type CampaignAssignmentConflict = {
  marketId: string;
  marketName: string;
  section: CampaignSection;
  existingCampaignId: string;
  existingCampaignName: string;
  existingScheduleType: "always" | "scheduled";
  existingStartDate: string | null;
  existingEndDate: string | null;
  existingPeriodLabel: string;
  existingGmUserId: string | null;
  existingGmName: string | null;
};

type CampaignScheduleWindow = {
  scheduleType: "always" | "scheduled";
  startDate: string | null;
  endDate: string | null;
};

function toPeriodLabel(window: CampaignScheduleWindow): string {
  if (window.scheduleType === "always") return "Immer aktiv";
  if (!window.startDate || !window.endDate) return "Geplant";
  return `${window.startDate} - ${window.endDate}`;
}

function windowsOverlap(left: CampaignScheduleWindow, right: CampaignScheduleWindow): boolean {
  if (left.scheduleType === "always" || right.scheduleType === "always") return true;
  if (!left.startDate || !left.endDate || !right.startDate || !right.endDate) return false;
  return !(left.endDate < right.startDate || right.endDate < left.startDate);
}

export async function findAssignmentConflicts(database: CampaignReadDatabase, input: {
  targetCampaignId?: string;
  section: CampaignSection;
  targetStatus: "active" | "scheduled" | "inactive";
  targetWindow: CampaignScheduleWindow;
  assignments: Array<{ marketId: string }>;
  includeScheduled?: boolean;
}, completedKuehler: (rows: Array<{ campaignId: string; marketId: string; visitTargetCount: number }>) => Promise<Set<string>>): Promise<CampaignAssignmentConflict[]> {
  if (input.targetStatus !== "active") return [];
  if (input.section === "flex") return [];
  const marketIds = Array.from(new Set(input.assignments.map((assignment) => assignment.marketId)));
  if (marketIds.length === 0) return [];

  const query = database
    .select({
      marketId: campaignMarketAssignments.marketId,
      marketName: markets.name,
      section: campaigns.section,
      campaignId: campaigns.id,
      campaignName: campaigns.name,
      scheduleType: campaigns.scheduleType,
      startDate: campaigns.startDate,
      endDate: campaigns.endDate,
      gmUserId: campaignMarketAssignments.gmUserId,
      visitTargetCount: campaignMarketAssignments.visitTargetCount,
      gmFirstName: users.firstName,
      gmLastName: users.lastName,
    })
    .from(campaignMarketAssignments)
    .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
    .innerJoin(markets, eq(markets.id, campaignMarketAssignments.marketId))
    .leftJoin(users, eq(users.id, campaignMarketAssignments.gmUserId))
    .where(
      and(
        inArray(campaignMarketAssignments.marketId, marketIds),
        eq(campaignMarketAssignments.isDeleted, false),
        eq(campaigns.isDeleted, false),
        eq(campaigns.section, input.section),
        input.includeScheduled ? inArray(campaigns.status, ["active", "scheduled"]) : eq(campaigns.status, "active"),
        input.targetCampaignId ? ne(campaigns.id, input.targetCampaignId) : sql`true`,
      ),
    );

  const rows = await query;
  const completedKuehlerKeys = input.section === "kuehler"
    ? await completedKuehler(rows)
    : new Set<string>();
  const conflicts: CampaignAssignmentConflict[] = [];
  for (const row of rows) {
    if (completedKuehlerKeys.has(`${row.campaignId}:${row.marketId}`)) continue;
    const existingWindow: CampaignScheduleWindow = {
      scheduleType: row.scheduleType,
      startDate: row.startDate ? String(row.startDate) : null,
      endDate: row.endDate ? String(row.endDate) : null,
    };
    if (!windowsOverlap(input.targetWindow, existingWindow)) continue;
    conflicts.push({
      marketId: row.marketId,
      marketName: row.marketName,
      section: row.section,
      existingCampaignId: row.campaignId,
      existingCampaignName: row.campaignName,
      existingScheduleType: row.scheduleType,
      existingStartDate: existingWindow.startDate,
      existingEndDate: existingWindow.endDate,
      existingPeriodLabel: toPeriodLabel(existingWindow),
      existingGmUserId: row.gmUserId ?? null,
      existingGmName: row.gmFirstName && row.gmLastName ? `${row.gmFirstName} ${row.gmLastName}` : null,
    });
  }

  const deduped = new Map<string, CampaignAssignmentConflict>();
  for (const conflict of conflicts) {
    const key = `${conflict.marketId}:${conflict.existingCampaignId}`;
    if (!deduped.has(key)) deduped.set(key, conflict);
  }
  return Array.from(deduped.values());
}
