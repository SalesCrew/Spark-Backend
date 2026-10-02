import test from "node:test";
import assert from "node:assert/strict";
import { evaluateModel, modelTemplate, type WaveModel, type MetricEntry } from "./praemien-model.shared.js";

function payout(model: WaveModel, values: Record<string, number | null>) {
  const p = model.pillars.find((p) => p.key === "flex")!;
  const entries: MetricEntry[] = Object.entries(values).map(([metricKey, value]) => ({ gmId: "gm", pillarKey: p.key, metricKey, value, target: null, note: "synthetic" }));
  return evaluateModel({ ...model, pillars: [p] }, [{ gmId: "gm", name: "Test GM", active: true }], entries, [])[0]!.pillars[0]!;
}
test("two independently selected percentage requirements each must reach 50; no cross compensation", () => {
  const model = modelTemplate("q3"), p = model.pillars.find((p) => p.key === "flex")!;
  p.tiers = [{ key: "both_half", label: "Beide 50 %", group: "", rewardEur: 82.5, conditions: [{ metricKey: "coolers", operator: "gte", value: 50 }, { metricKey: "racks", operator: "gte", value: 50 }] }];
  for (const [coolers, racks, earned] of [[49.99, 100, 0], [100, 49.99, 0], [50, 50, 82.5], [100, 100, 82.5]] as const) assert.equal(payout(model, { coolers, racks }).earned, earned);
  const pending = payout(model, { coolers: 100, racks: null }); assert.equal(pending.earned, 0); assert.equal(pending.pending, true);
});
test("Q1 source: one point per placement, both minimum targets plus total payout threshold", () => {
  const q1 = modelTemplate("q1");
  for (const [placements, scanning, earned] of [[17, 100, 0], [30, 64.99, 0], [18, 65, 82.5], [20, 65, 82.5], [21, 65, 165], [18, 75, 165], [22, 75, 165]] as const) {
    const result = payout(q1, { placements, scanning });
    assert.equal(result.earned, earned, `${placements} placements / ${scanning}% scan`);
    assert.equal(result.metrics.find((m) => m.key === "placement_points")!.value, placements);
  }
});
test("Q2 net coolers and RED independently gate payout even when total points reach a tier", () => {
  const q2 = modelTemplate("q2");
  for (const [net, red_ir, earned] of [[3, 79.99, 0], [1, 100, 0], [2, 80, 82.5], [2, 85, 165], [3, 80, 165], [-1, 100, 0]] as const) assert.equal(payout(q2, { new_coolers: net + 1, returned: 1, red_ir }).earned, earned);
});
