import assert from "node:assert/strict";
import test from "node:test";
import {
  appendAdminKurtiCharts,
  parseAdminKurtiChartArguments,
  parseAdminKurtiStoredContent,
  type AdminKurtiChartSpec,
} from "./lib/admin-kurti-charts.js";

const ippChart: AdminKurtiChartSpec = {
  type: "line",
  title: "IPP-Entwicklung",
  subtitle: "RED 1 bis RED 3 · 6 Markt-Samples",
  xLabel: "RED-Monat",
  yLabel: "IPP",
  valueFormat: "decimal",
  series: [{ key: "ipp", label: "IPP Durchschnitt" }],
  points: [
    { label: "RED 1", values: [{ seriesKey: "ipp", value: 12.4 }] },
    { label: "RED 2", values: [{ seriesKey: "ipp", value: null }] },
    { label: "RED 3", values: [{ seriesKey: "ipp", value: 15.75 }] },
  ],
};

test("parses a validated Admin Kurti chart tool call", () => {
  assert.deepEqual(parseAdminKurtiChartArguments(JSON.stringify(ippChart)), ippChart);
});

test("rejects chart points that do not match the declared series", () => {
  const invalid = {
    ...ippChart,
    points: ippChart.points.map((point) => ({
      ...point,
      values: [{ seriesKey: "unknown", value: 1 }],
    })),
  };
  assert.throws(() => parseAdminKurtiChartArguments(JSON.stringify(invalid)));
});

test("stores and restores charts without exposing the storage marker as message text", () => {
  const stored = appendAdminKurtiCharts("Hier ist die Entwicklung.", [ippChart]);
  assert.match(stored, /admin-kurti-charts-v1/);

  const restored = parseAdminKurtiStoredContent(stored);
  assert.equal(restored.content, "Hier ist die Entwicklung.");
  assert.deepEqual(restored.charts, [ippChart]);
});

test("hides a malformed chart marker and returns no chart", () => {
  const restored = parseAdminKurtiStoredContent("Antwort\n<!--admin-kurti-charts-v1:bm90LWpzb24-->");
  assert.equal(restored.content, "Antwort");
  assert.deepEqual(restored.charts, []);
});
