import assert from "node:assert/strict";
import test from "node:test";
import { resolveManualPillarOverrideMetricValue } from "./lib/bonus-finalizer.js";

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
