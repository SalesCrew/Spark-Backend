import assert from "node:assert/strict";
import test from "node:test";
import {
  reconcileCampaignVisitExportDetails,
  uniqueCampaignVisitExportRequests,
} from "./lib/campaign-visit-export.js";

test("keeps multiple submitted visits for the same market", () => {
  const requested = uniqueCampaignVisitExportRequests([
    { marketId: "market-1", sessionId: "session-1" },
    { marketId: "market-1", sessionId: "session-2" },
  ]);
  const result = reconcileCampaignVisitExportDetails(requested, [
    { marketId: "market-1", sessionId: "session-2", hasSubmittedVisit: true },
    { marketId: "market-1", sessionId: "session-1", hasSubmittedVisit: true },
  ]);

  assert.deepEqual(result.missingVisits, []);
  assert.deepEqual(result.orderedDetails.map((detail) => detail.sessionId), ["session-1", "session-2"]);
});

test("reports every missing or unsubmitted visit instead of silently dropping it", () => {
  const requested = [
    { marketId: "market-1", sessionId: "session-1" },
    { marketId: "market-2", sessionId: "session-2" },
  ];
  const result = reconcileCampaignVisitExportDetails(requested, [
    { marketId: "market-1", sessionId: "session-1", hasSubmittedVisit: true },
    { marketId: "market-2", sessionId: "session-2", hasSubmittedVisit: false },
  ]);

  assert.deepEqual(result.orderedDetails.map((detail) => detail.sessionId), ["session-1"]);
  assert.deepEqual(result.missingVisits, [{ marketId: "market-2", sessionId: "session-2" }]);
});
