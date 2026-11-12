import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  evaluateModel, modelTemplate,
  type MetricEntry, type Observation,
} from "./praemien-model.shared.js";

function example() {
  const model = modelTemplate("empty"), p = model.pillars[0]!;
  p.kind = "displays";
  const m = p.metrics[0]!;
  Object.assign(m, {
    method: "answer_sum", unit: "percent", target: null,
    inputs: [], sources: Array.from({ length: 38 }, (_, i) => ({
      questionId: randomUUID(), section: "standard", scoreKey: "__value__",
      factor: true, weight: 1, label: `Frage ${i + 1}`,
      minFrequency: 8, chains: ["Sparmarkt"], counting: "latest",
    })),
  });
  const roster = ["gm", "other"].map(gmId => ({ gmId, name: gmId, active: true }));
  const observations: Observation[] = roster.flatMap(gm => m.sources.map((s, i) => ({
    gmId: gm.gmId, marketId: "market", questionId: s.questionId,
    section: "standard", date: "2026-11-01", id: randomUUID(),
    numeric: i ? 0 : 80, options: [], frequency: 8, chain: "Sparmarkt",
  })));
  const entry: MetricEntry = {
    gmId: "gm", pillarKey: p.key, metricKey: m.key,
    value: null, target: 90, note: "100 × (20 − 2) ÷ 20",
  };
  const run = (entries: MetricEntry[] = []) => evaluateModel(model, roster, entries, observations);
  const metric = (entries: MetricEntry[] = []) => run(entries)[0]!.pillars[0]!.metrics[0]!;
  return { model, p, m, observations, entry, run, metric };
}

test("38 display sources expose raw points and require an explicit personal target for percentages", () => {
  const f = example(), sources = structuredClone(f.m.sources);
  f.m.target = 200; // A previously ignored model target must remain ignored.
  assert.equal(f.metric().value, null);
  assert.equal(f.metric().rawPoints, 80);
  assert.equal(f.metric().missingTarget, true);
  const result = f.run([f.entry]);
  assert.equal(result[0]!.pillars[0]!.metrics[0]!.value, 80 / 90 * 100);
  assert.equal(result[0]!.pillars[0]!.metrics[0]!.counted, 38);
  assert.equal(result[1]!.pillars[0]!.metrics[0]!.value, null);
  assert.equal(result[1]!.pillars[0]!.metrics[0]!.rawPoints, 80);
  assert.deepEqual(f.m.sources, sources);
  assert.equal(f.metric([{ ...f.entry, target: null }]).value, null);
});

test("explicit manual percentages including zero remain authoritative, not divided again", () => {
  const f = example();
  for (const value of [0, 85]) {
    const result = f.metric([{ ...f.entry, value }]);
    assert.equal(result.value, value);
    assert.equal(result.origin, "manual");
    assert.equal(result.automatic, 80 / 90 * 100);
  }
});

test("raw point/count sums and non-display percentages retain their calculation", () => {
  for (const unit of ["points", "count", "eur"] as const) {
    const f = example(); f.m.unit = unit;
    assert.equal(f.metric([f.entry]).value, 80, unit);
  }
  for (const kind of ["distribution", "flex", "quality", "custom"] as const) {
    const f = example(); f.p.kind = kind;
    assert.equal(f.metric([f.entry]).value, 80, kind);
  }
});

test("personal targets preserve missing versus zero and allow achievement above 100 percent", () => {
  const f = example();
  f.observations[0]!.numeric = null;
  assert.equal(f.metric([f.entry]).value, 0, "other explicitly measured zeros form a provisional subtotal");
  assert.equal(f.metric([f.entry]).provisional, true);
  f.observations[0]!.numeric = 0;
  assert.equal(f.metric([f.entry]).value, 0);
  f.observations[0]!.numeric = 120;
  assert.equal(f.metric([f.entry]).value, 120 / 90 * 100);
  f.observations.splice(1, 1);
  assert.equal(f.metric([f.entry]).value, 120 / 90 * 100);
  assert.equal(f.metric([f.entry]).provisional, true);
  f.observations.splice(0, f.observations.length);
  assert.equal(f.metric([f.entry]).value, null, "no observations must not become a measured zero");
});

test("a personal target does not change visit deduplication or source eligibility", () => {
  const f = example(), row = f.observations[0]!;
  f.observations.push({ ...row, id: "older", date: "2026-10-01", numeric: 900 });
  f.observations.push({ ...row, id: "ineligible", marketId: "not-eligible", frequency: 1, numeric: 900 });
  const result = f.metric([f.entry]);
  assert.equal(result.value, 80 / 90 * 100);
  assert.equal(result.counted, 38);
  assert.equal(result.excluded, 1);
  f.m.sources[0]!.counting = "once";
  assert.equal(f.metric([f.entry]).value, 900 / 90 * 100);
});
