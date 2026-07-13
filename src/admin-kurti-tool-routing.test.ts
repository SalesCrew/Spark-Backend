import assert from "node:assert/strict";
import test from "node:test";
import { selectAdminKurtiTools } from "./lib/admin-kurti-tool-routing.js";

test("keeps the chart renderer available on follow-up turns without chart keywords", () => {
  const toolNames = selectAdminKurtiTools("Doch, das solltest du schon bereit haben.")
    .map((tool) => tool.name);

  assert.ok(toolNames.includes("render_admin_chart"));
});

test("routes multi-GM work-time charts to precise analytics and specialized renderers", () => {
  const toolNames = selectAdminKurtiTools("Zeig mir die Arbeitszeit aller GMs der letzten vier Wochen als Liniendiagramm")
    .map((tool) => tool.name);

  assert.ok(toolNames.includes("get_worktime_analytics"));
  assert.ok(toolNames.includes("render_series_visualization"));
  assert.ok(toolNames.includes("render_metrics_visualization"));
  assert.ok(toolNames.includes("render_table_visualization"));
  assert.ok(toolNames.includes("load_admin_visualization_skill"));
});

test("loads every specialized renderer for a broad visualization dashboard request", () => {
  const toolNames = new Set(selectAdminKurtiTools("Baue ein Dashboard mit allen verschiedenen Charts").map((tool) => tool.name));
  for (const name of [
    "render_series_visualization",
    "render_composition_visualization",
    "render_scatter_visualization",
    "render_heatmap_visualization",
    "render_metrics_visualization",
    "render_table_visualization",
    "render_timeline_visualization",
    "render_radar_visualization",
    "render_distribution_visualization",
    "render_waterfall_visualization",
    "render_treemap_visualization",
  ]) assert.ok(toolNames.has(name), name);
});

test("routes numeric distribution requests to the raw-observation renderer", () => {
  const toolNames = selectAdminKurtiTools("Zeig die IPP-Verteilung als Histogramm und Boxplot")
    .map((tool) => tool.name);
  assert.ok(toolNames.includes("get_ipp_context"));
  assert.ok(toolNames.includes("render_distribution_visualization"));
  assert.ok(!toolNames.includes("render_composition_visualization"));
});
