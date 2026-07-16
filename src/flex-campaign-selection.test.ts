import assert from "node:assert/strict";
import test from "node:test";
import { selectEffectiveFlexCampaigns } from "./lib/flex-campaign-selection.js";

test("uses global Flex campaigns when the GM has no explicit assignment", () => {
  const globalRows = [
    { campaignId: "global-a", campaignName: "Global A", assignedGmUserId: null },
    { campaignId: "global-b", campaignName: "Global B", assignedGmUserId: null },
  ];

  assert.deepEqual(selectEffectiveFlexCampaigns(globalRows, "gm-arthur"), globalRows);
});

test("explicit GM Flex assignments replace every global campaign for the market", () => {
  const assignedRows = [{ campaignId: "arthur-flex", campaignName: "Flex Billa", assignedGmUserId: "gm-arthur" }];
  const globalRows = [{ campaignId: "global-flex", campaignName: "Flex Q3", assignedGmUserId: null }];

  assert.deepEqual(selectEffectiveFlexCampaigns([...globalRows, ...assignedRows], "gm-arthur"), assignedRows);
});

test("campaigns assigned to another GM are never visible", () => {
  const globalRows = [{ campaignId: "global-flex", assignedGmUserId: null }];
  const otherGmRows = [{ campaignId: "arthur-flex", assignedGmUserId: "gm-arthur" }];

  assert.deepEqual(selectEffectiveFlexCampaigns([...globalRows, ...otherGmRows], "gm-eva"), globalRows);
});

test("collapses duplicate assignment slots for the same campaign", () => {
  const assignedRows = [
    { campaignId: "arthur-flex", assignedGmUserId: "gm-arthur", slot: 1 },
    { campaignId: "arthur-flex", assignedGmUserId: "gm-arthur", slot: 2 },
  ];

  assert.deepEqual(selectEffectiveFlexCampaigns(assignedRows, "gm-arthur"), [assignedRows[1]]);
});
