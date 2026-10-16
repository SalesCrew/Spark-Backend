import assert from "node:assert/strict";
import { test } from "node:test";
import { aggregateSmDashboardOverview, type SmOverviewVisit } from "./sm-dashboard-overview.shared.js";
import type { SmDashboardOosRow } from "./sm-dashboard.shared.js";

const visit = (id: string, marketId = id, smUserId = "sm1", actualMinutes: number | null = 60): SmOverviewVisit => ({ submissionId: id, marketId, smUserId, actualMinutes, marketName: marketId, smName: smUserId, chain: "SPAR", region: "Nord" });
const row = (v: SmOverviewVisit, category: SmDashboardOosRow["category"], found: boolean, fixed: SmDashboardOosRow["outcome"] = "resolved"): SmDashboardOosRow[] => [
  { ...v, submissionQuestionId: `d-${category}`, questionRootId: `detect-${category}`, role: "oos_detection", category, detectionQuestionRootId: null, partialCountsAsResolved: false, outcome: found ? "oos_present" : "oos_absent" },
  { ...v, submissionQuestionId: `r-${category}`, questionRootId: `remedy-${category}`, role: "oos_remediation", category, detectionQuestionRootId: `detect-${category}`, partialCountsAsResolved: false, outcome: fixed },
];
test("paper formula counts recovered share of all checks, never relative change", () => {
  const visits = Array.from({ length: 852 }, (_, i) => visit(String(i), String(i % 135), String(i % 9), 30));
  const categories = ["action_placements", "softdrinks_energy", "water_near_water", "juice_iced_tea"] as const;
  const rows = visits.flatMap((v, index) => categories.flatMap((category, i) => row(v, category, index * 4 + i < 599, index * 4 + i < 457 ? "resolved" : "not_resolved")));
  const result = aggregateSmDashboardOverview(visits, rows, []);
  assert.equal(result.activity.visitedMarkets, 135); assert.equal(result.activity.visits, 852);
  assert.equal(result.activity.visitsPerMarket, 852 / 135); assert.equal(result.activity.marketHours, 426); assert.equal(result.activity.hoursPerVisit, 0.5); assert.equal(result.activity.merchandisers, 9);
  const p = result.performance;
  assert.equal(p.categoryChecks, 3408); assert.equal(p.foundCategoryCases, 599); assert.equal(p.fixedCategoryCases, 457);
  assert.equal(p.availabilityGain, 457 / 3408 * 100);
  assert.equal(p.availabilityBefore, 2809 / 3408 * 100); assert.equal(p.availabilityAfter, (2809 + 457) / 3408 * 100);
  assert.ok(Math.abs(p.availabilityAfter! - p.availabilityBefore! - p.availabilityGain!) < 1e-10);
  assert.equal(p.availabilityGain!.toFixed(1), "13.4");
});
test("repeat visits, duplicate questions/options, partial fixes and missing ZE have honest denominators", () => {
  const a = visit("a", "same"), b = visit("b", "same", "sm2", null), c = visit("c", "other");
  const rows = [...row(a, "water_near_water", true, "partially_resolved"), ...row(b, "water_near_water", false), ...row(c, "water_near_water", true, null)];
  rows.push(rows[0]!, { ...rows[0]!, submissionQuestionId: "second-detection", questionRootId: "second-root", outcome: "oos_absent" });
  const result = aggregateSmDashboardOverview([a, a, b, c], rows, [
    { submissionId: "a", submissionQuestionId: "q1", metricOutcomeCode: "basic_activity_yes" },
    { submissionId: "b", submissionQuestionId: "q1", metricOutcomeCode: "basic_activity_partial" },
    { submissionId: "c", submissionQuestionId: "q1", metricOutcomeCode: "basic_activity_no" },
  ]);
  assert.equal(result.activity.visits, 3); assert.equal(result.activity.visitedMarkets, 2); assert.equal(result.activity.merchandisers, 2);
  assert.equal(result.activity.marketHours, 2); assert.equal(result.activity.missingTimeVisits, 1); assert.equal(result.activity.hoursPerVisit, null);
  const p = result.performance;
  assert.equal(p.basicActivityRate, 50); assert.equal(p.categoryChecks, 3); assert.equal(p.foundCategoryCases, 2); assert.equal(p.fixedCategoryCases, 1); assert.equal(p.missingRemediationCases, 1);
  assert.equal(p.affectedMarketRate, 100); assert.equal(p.remediationRate, 50); assert.ok(Math.abs(p.availabilityGain! - 100 / 3) < 1e-10);
});
test("no records and no found OOS are distinct; no synthetic four-check multiplier", () => {
  const empty = aggregateSmDashboardOverview([], [], []).performance;
  assert.equal(empty.availabilityGain, null); assert.equal(empty.basicActivityRate, null); assert.equal(empty.remediationRate, null);
  const v = visit("a");
  const result = aggregateSmDashboardOverview([v], row(v, "water_near_water", false), []).performance;
  assert.equal(result.categoryChecks, 1); assert.equal(result.availabilityGain, 0); assert.equal(result.availabilityAfter, 100); assert.equal(result.remediationRate, null);
});
