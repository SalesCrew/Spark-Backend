import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { evaluateModel, modelTemplate, type Observation, type MetricEntry } from "./praemien-model.shared.js";

function scenario() {
  const model = modelTemplate("empty"), p = model.pillars[0]!;
  p.kind = "displays"; p.maxRewardEur = 100;
  const sources = ["First", "Second"].map(label => ({ questionId: randomUUID(), label,
    section: "standard", scoreKey: "__value__", factor: true, weight: 1,
    minFrequency: 8, chains: [], counting: "latest" as const }));
  p.metrics = [{ ...p.metrics[0]!, key: "raw", unit: "points", method: "answer_sum", sources },
    { ...p.metrics[0]!, key: "percent", unit: "percent", method: "ratio", inputs: ["raw"], target: 100 }];
  p.tiers = [{ key: "paid", group: "", label: "80 %", rewardEur: 100, conditions: [{ metricKey: "percent", operator: "gte", value: 80 }] }];
  const row = (source: number, numeric: number | null, marketId = "market"): Observation => ({
    id: randomUUID(), gmId: "gm", marketId, questionId: sources[source]!.questionId,
    section: "standard", date: "2026-08-01", numeric, options: [], frequency: 8, chain: "Sparmarkt",
  });
  const run = (observations: Observation[], entries: MetricEntry[] = []) => evaluateModel(model,
    [{ gmId: "gm", name: "Synthetic GM", active: true }], entries, observations)[0]!.pillars[0]!;
  return { p, run, row, model };
}

test("partial points propagate into ratios and potential rewards while remaining provisional", () => {
  const f = scenario(), result = f.run([f.row(0, 80)]);
  assert.equal(result.metrics[0]!.value, 80);
  assert.equal(result.metrics[1]!.value, 80);
  assert.equal(result.metrics[1]!.provisional, true);
  assert.deepEqual(result.metrics[1]!.sourceCoverage, { answered: 1, total: 2 });
  assert.equal(result.metrics[1]!.missingSources!.length, 1);
  assert.equal(result.earned, 100);
  assert.equal(result.pending, true);
  const complete = f.run([f.row(0, 80), f.row(1, 0)]);
  assert.equal(complete.metrics[1]!.value, 80);
  assert.equal(complete.metrics[1]!.provisional, false);
  assert.equal(complete.pending, false);
});

test("no data stays unknown; measured zero and partially missing numeric observations stay distinct", () => {
  const f = scenario();
  assert.equal(f.run([]).metrics[0]!.value, null);
  const zero = f.run([f.row(0, 0)]).metrics[0]!;
  assert.equal(zero.value, 0); assert.equal(zero.provisional, true);
  const partlyUnknown = f.run([f.row(0, 12), f.row(0, null, "other-market")]).metrics[0]!;
  assert.equal(partlyUnknown.value, 12);
  assert.deepEqual(partlyUnknown.sourceCoverage, { answered: 0, total: 2 });
  assert.equal(partlyUnknown.missingSources!.length, 2);
});

test("manual points and legacy percentages have separate meanings, including zero", () => {
  const f = scenario();
  f.p.metrics = [{ ...f.p.metrics[0]!, key: "percent", unit: "percent" }];
  const entry: MetricEntry = { gmId: "gm", pillarKey: f.p.key, metricKey: "percent",
    value: 72.5, rawPoints: 40, target: 80, note: "Synthetic" };
  assert.equal(f.run([], [entry]).metrics[0]!.value, 72.5);
  const calculated = f.run([], [{ ...entry, value: null }]);
  assert.equal(calculated.metrics[0]!.value, 50);
  assert.equal(calculated.metrics[0]!.manualPoints, 40);
  assert.equal(calculated.metrics[0]!.provisional, false);
  assert.equal(calculated.pending, false);
  assert.equal(f.run([], [{ ...entry, value: null, rawPoints: 0 }]).metrics[0]!.value, 0);
  const missingTarget = f.run([], [{ ...entry, value: null, target: null }]).metrics[0]!;
  assert.equal(missingTarget.value, null); assert.equal(missingTarget.rawPoints, 40);
  assert.equal(missingTarget.missingTarget, true);
});

test("availability quotas retain strict completeness instead of inventing a partial denominator", () => {
  const f = scenario();
  const source = f.p.metrics[0]!.sources[0]!;
  f.p.metrics = [{ ...f.p.metrics[0]!, key: "percent", unit: "percent", method: "availability", sources: [source], target: 10 }];
  const result = f.run([f.row(0, 1), f.row(0, null, "other-market")]);
  assert.equal(result.metrics[0]!.value, null);
  assert.equal(result.pending, true);
});
