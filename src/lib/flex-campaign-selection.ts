export type FlexCampaignSelectionRow = {
  campaignId: string;
  assignedGmUserId: string | null;
};

/**
 * A campaign-level GM audience takes precedence over the global Flex scope for one market.
 * Campaigns assigned to another GM are never eligible.
 * Duplicate assignment slots are collapsed so a campaign is offered only once.
 */
export function selectEffectiveFlexCampaigns<T extends FlexCampaignSelectionRow>(
  rows: readonly T[],
  gmUserId: string,
): T[] {
  const assignedRows = rows.filter((row) => row.assignedGmUserId === gmUserId);
  const globalRows = rows.filter((row) => row.assignedGmUserId === null);
  const source = assignedRows.length > 0 ? assignedRows : globalRows;
  return Array.from(new Map(source.map((row) => [row.campaignId, row])).values());
}
