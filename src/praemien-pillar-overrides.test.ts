import assert from "node:assert/strict";
import test from "node:test";
import {
  resolveManualPillarOverrideMetricValue,
  resolvePillarProgressPercent,
  resolvePillarProgressTarget,
} from "./lib/bonus-finalizer.js";

test("manual pillar overrides dominate raw, quality and flex metrics", () => {
  for (const valueSource of [
    "contribution_points",
    "quality_zeiterfassung",
    "quality_reporting",
    "quality_accuracy",
    "quality_average",
    "flex_total_points",
    "flex_component",
  ]) {
    assert.equal(
      resolveManualPillarOverrideMetricValue({ valueSource, points: 42.5, maxPoints: 100 }),
      42.5,
    );
  }
});

test("manual pillar overrides retain percent semantics for percent conditions", () => {
  assert.equal(
    resolveManualPillarOverrideMetricValue({
      valueSource: "contribution_percent",
      points: 25,
      maxPoints: 50,
    }),
    50,
  );
});

test("manual pillar overrides do not discard a value when no automatic maximum exists", () => {
  assert.equal(
    resolveManualPillarOverrideMetricValue({
      valueSource: "contribution_percent",
      points: 17,
      maxPoints: 0,
    }),
    17,
  );
});

test("configured pillar target is the source of truth for progress percentages", () => {
  assert.equal(resolvePillarProgressTarget(100, 250), 100);
  assert.equal(resolvePillarProgressPercent(60, 100, 250), 60);
  assert.equal(
    resolveManualPillarOverrideMetricValue({
      valueSource: "contribution_percent",
      points: 60,
      maxPoints: 250,
      targetPoints: 100,
    }),
    60,
  );
});

test("source maximum remains a backwards-compatible fallback for draft pillars without a target", () => {
  assert.equal(resolvePillarProgressTarget(null, 250), 250);
  assert.equal(resolvePillarProgressPercent(60, null, 250), 24);
});
