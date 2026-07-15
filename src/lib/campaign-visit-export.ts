export type CampaignVisitExportRequest = {
  marketId: string;
  sessionId: string;
};

export type CampaignVisitExportDetail = {
  marketId: string;
  sessionId: string | null;
  hasSubmittedVisit: boolean;
};

export function campaignVisitExportKey(input: CampaignVisitExportRequest): string {
  return `${input.marketId}:${input.sessionId}`;
}

export function uniqueCampaignVisitExportRequests(
  visits: CampaignVisitExportRequest[],
): CampaignVisitExportRequest[] {
  return Array.from(new Map(visits.map((visit) => [campaignVisitExportKey(visit), visit])).values());
}

export function reconcileCampaignVisitExportDetails<T extends CampaignVisitExportDetail>(
  requestedVisits: CampaignVisitExportRequest[],
  details: T[],
): { orderedDetails: T[]; missingVisits: CampaignVisitExportRequest[] } {
  const detailByKey = new Map<string, T>();
  for (const detail of details) {
    if (!detail.hasSubmittedVisit || !detail.sessionId) continue;
    detailByKey.set(campaignVisitExportKey({ marketId: detail.marketId, sessionId: detail.sessionId }), detail);
  }
  const orderedDetails: T[] = [];
  const missingVisits: CampaignVisitExportRequest[] = [];

  for (const visit of requestedVisits) {
    const detail = detailByKey.get(campaignVisitExportKey(visit));
    if (detail) {
      orderedDetails.push(detail);
    } else {
      missingVisits.push(visit);
    }
  }

  return { orderedDetails, missingVisits };
}
