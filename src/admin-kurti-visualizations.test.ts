import assert from "node:assert/strict";
import test from "node:test";
import { parseAdminKurtiVisualizationToolCall } from "./lib/admin-kurti-visualization-tools.js";
import { appendAdminKurtiVisualizations, parseAdminKurtiStoredVisualizations } from "./lib/admin-kurti-visualizations.js";

const common = {
  title: "Test",
  subtitle: null,
  sourceLabel: "Testdaten",
  timeframe: null,
  note: null,
};

test("validates and persists every Admin Kurti visualization kind", () => {
  const calls: Array<[string, Record<string, unknown>]> = [
    ["render_series_visualization", { ...common, variant: "line", xLabel: null, yLabel: null, valueFormat: "decimal", showLegend: true, series: [{ key: "a", label: "A", display: "line", tone: "red" }], points: [{ label: "1", values: [2] }], referenceLines: [] }],
    ["render_composition_visualization", { ...common, variant: "donut", valueFormat: "number", centerLabel: null, items: [{ label: "A", value: 2, tone: "red" }] }],
    ["render_scatter_visualization", { ...common, xLabel: "X", yLabel: "Y", xFormat: "number", yFormat: "number", series: [{ key: "a", label: "A", tone: "red" }], points: [{ label: "1", seriesKey: "a", x: 1, y: 2, size: null }, { label: "2", seriesKey: "a", x: 2, y: 3, size: null }] }],
    ["render_heatmap_visualization", { ...common, valueFormat: "number", xLabels: ["A"], rows: [{ label: "R", values: [1] }] }],
    ["render_metrics_visualization", { ...common, columns: "2", items: [{ label: "A", value: 1, displayValue: null, valueFormat: "number", delta: null, deltaLabel: null, progress: null, status: "neutral" }] }],
    ["render_table_visualization", { ...common, columns: [{ key: "a", label: "A", align: "left" }], rows: [{ label: "R", values: ["1"], status: "neutral" }] }],
    ["render_timeline_visualization", { ...common, items: [{ date: "Heute", label: "A", description: null, value: null, status: "active" }] }],
    ["render_radar_visualization", { ...common, valueFormat: "number", maximum: null, axes: ["A", "B", "C"], series: [{ label: "S", tone: "red", values: [1, 2, 3] }] }],
  ];
  const visualizations = calls.map(([name, payload]) => parseAdminKurtiVisualizationToolCall(name, JSON.stringify(payload)));
  assert.deepEqual(visualizations.map((visualization) => visualization.kind), ["series", "composition", "scatter", "heatmap", "metrics", "table", "timeline", "radar"]);

  const stored = appendAdminKurtiVisualizations("Antwort", visualizations);
  const parsed = parseAdminKurtiStoredVisualizations(stored);
  assert.equal(parsed.content, "Antwort");
  assert.deepEqual(parsed.visualizations, visualizations);
});

test("rejects positional arrays that do not match their declared dimensions", () => {
  assert.throws(() => parseAdminKurtiVisualizationToolCall("render_series_visualization", JSON.stringify({
    ...common,
    variant: "line",
    xLabel: null,
    yLabel: null,
    valueFormat: "number",
    showLegend: true,
    series: [
      { key: "a", label: "A", display: "line", tone: "red" },
      { key: "b", label: "B", display: "line", tone: "blue" },
    ],
    points: [{ label: "1", values: [1] }],
    referenceLines: [],
  })));
});
