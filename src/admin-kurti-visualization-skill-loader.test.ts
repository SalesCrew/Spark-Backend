import assert from "node:assert/strict";
import test from "node:test";
import {
  ADMIN_KURTI_VISUALIZATION_SKILL_IDS,
  buildAdminKurtiVisualizationSkillContext,
  getAdminKurtiVisualizationToolNamesForSkills,
  selectAdminKurtiVisualizationSkillIds,
  type AdminKurtiVisualizationSkillId,
} from "./lib/admin-kurti-visualization-skill-loader.js";

test("selects a precise line skill for an implicit work-time development", () => {
  const selected = selectAdminKurtiVisualizationSkillIds(
    "Zeig mir die Entwicklung der reinen Arbeitszeit aller GMs über die letzten vier Wochen",
  );
  assert.deepEqual(selected, ["choose-visualization", "render-line-chart"]);
  assert.deepEqual(getAdminKurtiVisualizationToolNamesForSkills(selected), [
    "render_series_visualization",
    "render_metrics_visualization",
    "render_table_visualization",
  ]);
});

test("selects distribution skills instead of composition for numeric IPP spread", () => {
  const selected = selectAdminKurtiVisualizationSkillIds(
    "Mach ein Histogramm der IPP-Verteilung und zeig mögliche Ausreißer als Boxplot",
  );
  assert.deepEqual(selected, ["choose-visualization", "render-box-plot", "render-histogram"]);
  assert.ok(getAdminKurtiVisualizationToolNamesForSkills(selected).includes("render_distribution_visualization"));
});

test("bounds broad dashboard skill disclosure", () => {
  const selected = selectAdminKurtiVisualizationSkillIds(
    "Baue ein Dashboard mit Überblick, Entwicklung, Rangliste, Matrix und exakter Tabelle",
  );
  assert.equal(selected[0], "choose-visualization");
  assert.ok(selected.length <= 5);
});

test("loads the selected SKILL.md documents into trusted request context", () => {
  const context = buildAdminKurtiVisualizationSkillContext([
    "choose-visualization",
    "render-waterfall",
  ]);
  assert.ok(context?.includes("name: choose-visualization"));
  assert.ok(context?.includes("name: render-waterfall"));
  assert.ok(context?.includes("Lies alle folgenden SKILL.md-Inhalte vollständig"));
  assert.ok(!context?.includes("TODO"));
});

test("does not load visualization skills for an ordinary navigation question", () => {
  assert.deepEqual(selectAdminKurtiVisualizationSkillIds("Wo starte ich einen Kühlerbesuch?"), []);
  assert.equal(buildAdminKurtiVisualizationSkillContext([]), null);
});

test("loads every registered SKILL.md without placeholders", () => {
  for (const id of ADMIN_KURTI_VISUALIZATION_SKILL_IDS) {
    const context = buildAdminKurtiVisualizationSkillContext([id]);
    assert.ok(context?.includes(`name: ${id}`), id);
    assert.ok(!context?.includes("TODO"), id);
  }
});

test("recognizes every explicit visualization family", () => {
  const cases: Array<[string, AdminKurtiVisualizationSkillId]> = [
    ["Liniendiagramm der letzten Wochen", "render-line-chart"],
    ["Flächendiagramm für das Besuchsvolumen", "render-area-chart"],
    ["Säulendiagramm je Kampagne", "render-bar-chart"],
    ["Gestapelte Balken nach Besuchstyp je GM", "render-stacked-bar-chart"],
    ["Top 10 Märkte als Rangliste", "render-horizontal-bar-chart"],
    ["Kombiniert Balken und Linie für Ist und Ziel", "render-combo-chart"],
    ["Donut für den Anteil am Gesamtwert", "render-donut-chart"],
    ["Tortendiagramm der Status", "render-pie-chart"],
    ["Funnel von zugewiesen bis abgeschlossen", "render-funnel-chart"],
    ["Streudiagramm zum Zusammenhang zwischen IPP und Besuchen", "render-scatter-chart"],
    ["Bubble Chart mit dritter Kennzahl als Punktgröße", "render-bubble-chart"],
    ["Heatmap als GM-mal-Tag-Matrix", "render-heatmap"],
    ["KPI Überblick", "render-metrics"],
    ["Exakte Detailwerte als Tabelle", "render-table"],
    ["Chronologische Timeline der Ereignisse", "render-timeline"],
    ["Radar für das Qualitätsprofil nach Dimension", "render-radar-chart"],
    ["Histogramm der Werteverteilung", "render-histogram"],
    ["Boxplot für Median Quartile und Ausreißer", "render-box-plot"],
    ["Waterfall für die Abweichungszerlegung", "render-waterfall"],
    ["Treemap für viele Anteile", "render-treemap"],
  ];
  for (const [prompt, expected] of cases) {
    assert.ok(selectAdminKurtiVisualizationSkillIds(prompt).includes(expected), `${expected}: ${prompt}`);
  }
});
