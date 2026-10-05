import { and, eq, sql } from "drizzle-orm";
import type { db } from "./db.js";
import { campaigns, campaignMarketAssignments } from "./schema.js";
import type { CampaignAssignmentConflict } from "./campaign-assignment-conflicts.js";

export type CampaignExtensionTx = Parameters<Parameters<typeof db.transaction>[0]>[0];
export type CampaignExtensionInput = { endDate: string; expectedEndDate: string; expectedUpdatedAt: string };
export type CampaignExtensionDependencies = {
  transaction<T>(action: (tx: CampaignExtensionTx) => Promise<T>): Promise<T>;
  checkConflicts(tx: CampaignExtensionTx, input: {
    targetCampaignId: string;
    section: typeof campaigns.$inferSelect.section;
    targetStatus: "active";
    targetWindow: { scheduleType: "scheduled"; startDate: string; endDate: string };
    assignments: Array<{ marketId: string; gmUserId: string | null; assignmentSlot: number; visitTargetCount: number }>;
  }): Promise<CampaignAssignmentConflict[]>;
};

export class CampaignExtensionError extends Error {
  constructor(public status: number, public code: string, message: string, public conflicts?: CampaignAssignmentConflict[]) { super(message); }
}

export function viennaCampaignDate(now = new Date()) {
  return new Intl.DateTimeFormat("sv-SE", { timeZone: "Europe/Vienna" }).format(now);
}

export function isCampaignDate(value: string) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(`${value}T00:00:00Z`)) && new Date(`${value}T00:00:00Z`).toISOString().slice(0, 10) === value;
}

export async function extendCampaign(dependencies: CampaignExtensionDependencies, campaignId: string, input: CampaignExtensionInput, now = new Date()) {
  if (!isCampaignDate(input.endDate) || !isCampaignDate(input.expectedEndDate) || !Number.isFinite(Date.parse(input.expectedUpdatedAt))) {
    throw new CampaignExtensionError(400, "invalid_payload", "Bitte ein gültiges Enddatum auswählen.");
  }
  return dependencies.transaction(async (tx) => {
    // Serializes extensions within a section; the row lock also coordinates legacy date/status edits.
    const [sectionRow] = await tx.select({ section: campaigns.section }).from(campaigns).where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false))).limit(1);
    if (!sectionRow) throw new CampaignExtensionError(404, "campaign_not_found", "Kampagne nicht gefunden.");
    await tx.execute(sql`select pg_advisory_xact_lock(hashtext(${`campaign_extension:${sectionRow.section}`}))`);
    const [existing] = await tx.select().from(campaigns).where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false))).limit(1).for("update");
    if (!existing) throw new CampaignExtensionError(404, "campaign_not_found", "Kampagne nicht gefunden.");
    if (existing.updatedAt.getTime() !== Date.parse(input.expectedUpdatedAt) || existing.endDate !== input.expectedEndDate) {
      throw new CampaignExtensionError(409, "campaign_changed", "Die Kampagne wurde inzwischen geändert. Bitte neu laden und erneut auswählen.");
    }
    if (existing.scheduleType !== "scheduled" || !existing.startDate || !existing.endDate) {
      throw new CampaignExtensionError(400, "campaign_unlimited", "Diese Kampagne hat kein Enddatum und läuft bereits unbefristet.");
    }
    const today = viennaCampaignDate(now);
    if (input.endDate <= existing.endDate || input.endDate < today || input.endDate < existing.startDate) {
      throw new CampaignExtensionError(400, "extension_date_invalid", "Das neue Enddatum muss nach dem bisherigen Ende liegen und darf nicht in der Vergangenheit liegen.");
    }
    const assignments = await tx.select({ marketId: campaignMarketAssignments.marketId, gmUserId: campaignMarketAssignments.gmUserId, assignmentSlot: campaignMarketAssignments.assignmentSlot, visitTargetCount: campaignMarketAssignments.visitTargetCount })
      .from(campaignMarketAssignments).where(and(eq(campaignMarketAssignments.campaignId, campaignId), eq(campaignMarketAssignments.isDeleted, false)));
    const conflicts = await dependencies.checkConflicts(tx, { targetCampaignId: campaignId, section: existing.section, targetStatus: "active", targetWindow: { scheduleType: "scheduled", startDate: existing.startDate, endDate: input.endDate }, assignments });
    if (conflicts.length) throw new CampaignExtensionError(409, "campaign_market_overlap", "Die Verlängerung überschneidet sich mit einer anderen Kampagne derselben Art. Es wurde nichts geändert.", conflicts);
    const status = existing.startDate > today ? "scheduled" as const : "active" as const;
    const updatedAt = new Date(Math.max(now.getTime(), existing.updatedAt.getTime() + 1));
    // Only these three campaign fields may change; history, targets and all visit data are retained.
    await tx.update(campaigns).set({ endDate: input.endDate, status, updatedAt }).where(eq(campaigns.id, campaignId));
    return { campaign: { id: campaignId, endDate: input.endDate, status, updatedAt: updatedAt.toISOString() }, previousEndDate: existing.endDate, previousStatus: existing.status };
  });
}
