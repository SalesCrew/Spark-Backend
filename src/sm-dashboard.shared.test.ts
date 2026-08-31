import assert from "node:assert/strict";
import test from "node:test";

import {
  aggregateSmDashboard,
  aggregateSmHomeVisits,
  type SmDashboardOosRow,
  type SmDashboardVisitRow,
} from "./sm-dashboard.shared.js";

function visit(overrides: Partial<SmDashboardVisitRow> = {}): SmDashboardVisitRow {
  return {
    submissionId: "submission-1",
    marketId: "market-1",
    marketName: "Markt Eins",
    chain: "BILLA PLUS",
    region: "Ost",
    smUserId: "sm-1",
    smName: "Samira Muster",
    ...overrides,
  };
}

function oos(overrides: Partial<SmDashboardOosRow> = {}): SmDashboardOosRow {
  return {
    ...visit(overrides),
    submissionQuestionId: "question-1",
    questionRootId: "root-detection-1",
    role: "oos_detection",
    category: "softdrinks_energy",
    detectionQuestionRootId: null,
    outcome: "oos_present",
    partialCountsAsResolved: false,
    ...overrides,
  };
}

function remediation(outcome: SmDashboardOosRow["outcome"], overrides: Partial<SmDashboardOosRow> = {}): SmDashboardOosRow {
  return oos({
    submissionQuestionId: "question-remediation-1",
    questionRootId: "root-remediation-1",
    role: "oos_remediation",
    detectionQuestionRootId: "root-detection-1",
    outcome,
    ...overrides,
  });
}

test("empty OOS scope keeps denominators nullable instead of reporting zero percent", () => {
  const result = aggregateSmDashboard([visit()], []);
  assert.deepEqual(result.summary, {
    completedVisits: 1,
    submittedMarkets: 1,
    classifiedChecks: 0,
    foundCases: 0,
    foundRate: null,
    fixedCases: 0,
    fixedRate: null,
    documentedRemediations: 0,
    openRemediationDocumentation: 0,
    observedMarkets: 0,
    marketsWithOos: 0,
    affectedMarketRate: null,
  });
});

test("found, fixed and affected-market metrics use their documented denominators", () => {
  const visits = [
    visit(),
    visit({ submissionId: "submission-2", marketId: "market-2", marketName: "Markt Zwei" }),
  ];
  const rows = [
    oos(),
    remediation("resolved"),
    oos({ submissionId: "submission-2", marketId: "market-2", marketName: "Markt Zwei", submissionQuestionId: "question-2", questionRootId: "root-detection-2", outcome: "oos_absent" }),
  ];
  const result = aggregateSmDashboard(visits, rows);
  assert.equal(result.summary.completedVisits, 2);
  assert.equal(result.summary.classifiedChecks, 2);
  assert.equal(result.summary.foundCases, 1);
  assert.equal(result.summary.foundRate, 50);
  assert.equal(result.summary.fixedCases, 1);
  assert.equal(result.summary.fixedRate, 100);
  assert.equal(result.summary.observedMarkets, 2);
  assert.equal(result.summary.marketsWithOos, 1);
  assert.equal(result.summary.affectedMarketRate, 50);
});

test("unanswered remediation remains an open documentation gap and never counts as fixed", () => {
  const result = aggregateSmDashboard([visit()], [oos(), remediation(null)]);
  assert.equal(result.summary.foundCases, 1);
  assert.equal(result.summary.fixedCases, 0);
  assert.equal(result.summary.fixedRate, 0);
  assert.equal(result.summary.documentedRemediations, 0);
  assert.equal(result.summary.openRemediationDocumentation, 1);
});

test("partial remediation follows the immutable partialCountsAsResolved snapshot", () => {
  const counting = aggregateSmDashboard([visit()], [oos(), remediation("partially_resolved", { partialCountsAsResolved: true })]);
  assert.equal(counting.summary.fixedCases, 1);
  assert.equal(counting.summary.fixedRate, 100);

  const notCounting = aggregateSmDashboard([visit()], [oos(), remediation("partially_resolved", { partialCountsAsResolved: false })]);
  assert.equal(notCounting.summary.fixedCases, 0);
  assert.equal(notCounting.summary.fixedRate, 0);
  assert.equal(notCounting.summary.documentedRemediations, 1);
});

test("repeated visits create cases but affected markets remain distinct", () => {
  const secondVisit = visit({ submissionId: "submission-2" });
  const result = aggregateSmDashboard(
    [visit(), secondVisit],
    [
      oos(),
      oos({ submissionId: "submission-2", submissionQuestionId: "question-2" }),
    ],
  );
  assert.equal(result.summary.completedVisits, 2);
  assert.equal(result.summary.foundCases, 2);
  assert.equal(result.summary.observedMarkets, 1);
  assert.equal(result.summary.marketsWithOos, 1);
  assert.equal(result.summary.affectedMarketRate, 100);
});

test("category, chain and region rows reconcile to the same case set", () => {
  const westVisit = visit({ submissionId: "submission-west", marketId: "market-west", chain: "SPAR", region: "West" });
  const result = aggregateSmDashboard(
    [visit(), westVisit],
    [
      oos(),
      remediation("not_resolved"),
      oos({ submissionId: "submission-west", marketId: "market-west", chain: "SPAR", region: "West", submissionQuestionId: "question-west", questionRootId: "root-west", category: "water_near_water" }),
    ],
  );
  assert.equal(result.summary.foundCases, 2);
  assert.equal(result.categories.find((row) => row.category === "softdrinks_energy")?.foundCases, 1);
  assert.equal(result.categories.find((row) => row.category === "water_near_water")?.foundCases, 1);
  assert.equal(result.chains.find((row) => row.label === "BILLA PLUS")?.foundCases, 1);
  assert.equal(result.chains.find((row) => row.label === "SPAR")?.foundCases, 1);
  assert.equal(result.regions.find((row) => row.label === "Ost")?.foundCases, 1);
  assert.equal(result.regions.find((row) => row.label === "West")?.foundCases, 1);
});

test("one current question row wins when a correction replaces an unclassified row", () => {
  const result = aggregateSmDashboard([visit()], [
    oos({ outcome: null }),
    oos({ outcome: "oos_absent" }),
  ]);
  assert.equal(result.summary.classifiedChecks, 1);
  assert.equal(result.summary.foundCases, 0);
});

test("phone hero never labels missing/unclassified results as without OOS", () => {
  assert.deepEqual(aggregateSmHomeVisits([], [oos()]), {
    completed: 0, classified: 0, withoutOos: 0, fixedOos: 0, openOos: 0, unclassified: 0,
  });
  const unclassified = aggregateSmHomeVisits([visit(), visit()], [oos({ outcome: null })]);
  assert.equal(unclassified.completed, 1);
  assert.equal(unclassified.unclassified, 1);
  assert.equal(unclassified.withoutOos, 0);
});

test("phone hero counts visits, not questions, and any unresolved case makes the visit open", () => {
  const base = [oos(), remediation("resolved"), oos({ submissionQuestionId: "q2", questionRootId: "root2" })];
  assert.equal(aggregateSmHomeVisits([visit()], base).openOos, 1);
  assert.equal(aggregateSmHomeVisits([visit()], base).fixedOos, 0);
  const fixed = aggregateSmHomeVisits([visit()], [...base, remediation("resolved", { submissionQuestionId: "r2", detectionQuestionRootId: "root2" })]);
  assert.equal(fixed.fixedOos, 1);
  assert.equal(fixed.classified, 1);
});

test("phone hero respects pairing, category, snapshot partial rules and repeated visits", () => {
  assert.equal(aggregateSmHomeVisits([visit()], [oos(), remediation("resolved", { category: "juice_iced_tea" })]).openOos, 1);
  assert.equal(aggregateSmHomeVisits([visit()], [oos(), remediation("partially_resolved")]).openOos, 1);
  assert.equal(aggregateSmHomeVisits([visit()], [oos(), remediation("partially_resolved", { partialCountsAsResolved: true })]).fixedOos, 1);
  const second = visit({ submissionId: "submission-2" });
  assert.deepEqual(aggregateSmHomeVisits([visit(), second], [
    oos(), remediation("resolved"),
    oos({ ...second, submissionQuestionId: "q2", outcome: "oos_absent" }),
    remediation("resolved", { ...second, submissionQuestionId: "r2" }),
  ]), { completed: 2, classified: 2, withoutOos: 1, fixedOos: 1, openOos: 0, unclassified: 0 });
});

test("all 81 four-category yes/no scenarios reconcile with admin OOS aggregation", () => {
  const categories = ["action_placements", "softdrinks_energy", "water_near_water", "juice_iced_tea"] as const;
  for (let scenario = 0; scenario < 81; scenario += 1) {
    let remaining = scenario;
    const rows: SmDashboardOosRow[] = [];
    for (const [index, category] of categories.entries()) {
      const choice = remaining % 3; // absent / found+fixed / found+open
      remaining = Math.floor(remaining / 3);
      rows.push(oos({ category, submissionQuestionId: `q${index}`, questionRootId: `root${index}`, outcome: choice === 0 ? "oos_absent" : "oos_present" }));
      if (choice > 0) rows.push(remediation(choice === 1 ? "resolved" : "not_resolved", { category, submissionQuestionId: `r${index}`, detectionQuestionRootId: `root${index}` }));
    }
    const admin = aggregateSmDashboard([visit()], rows).summary;
    const phone = aggregateSmHomeVisits([visit()], rows);
    assert.equal(phone.withoutOos, Number(admin.foundCases === 0));
    assert.equal(phone.fixedOos, Number(admin.foundCases > 0 && admin.foundCases === admin.fixedCases));
    assert.equal(phone.openOos, Number(admin.foundCases > admin.fixedCases));
    assert.equal(phone.withoutOos + phone.fixedOos + phone.openOos, 1);
  }
});
