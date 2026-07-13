import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const ADMIN_KURTI_VISUALIZATION_SKILL_IDS = [
  "choose-visualization",
  "render-line-chart",
  "render-area-chart",
  "render-bar-chart",
  "render-stacked-bar-chart",
  "render-horizontal-bar-chart",
  "render-combo-chart",
  "render-donut-chart",
  "render-pie-chart",
  "render-funnel-chart",
  "render-scatter-chart",
  "render-bubble-chart",
  "render-heatmap",
  "render-metrics",
  "render-table",
  "render-timeline",
  "render-radar-chart",
  "render-histogram",
  "render-box-plot",
  "render-waterfall",
  "render-treemap",
] as const;

export type AdminKurtiVisualizationSkillId = typeof ADMIN_KURTI_VISUALIZATION_SKILL_IDS[number];

const VISUALIZATION_OPPORTUNITY_PATTERN = /(chart|diagramm|grafik|visualis|dashboard|entwicklung|trend|verlauf|vergleich|ranking|rangliste|anteil|verteilung|zusammensetzung|korrelation|zusammenhang|heatmap|matrix|tabelle|kpi|kennzahl|donut|pie|funnel|scatter|bubble|radar|timeline|torte|kreis|balken|s[äa]ule|histogramm|boxplot|box.?plot|quartil|median|ausrei[ßs]er|waterfall|wasserfall|treemap|baumkarte|top\s*\d*|höchste|hoechste|niedrigste|zeitreihe|auffälligkeit|auffaelligkeit)/i;

const SKILL_MATCHERS: ReadonlyArray<{ id: AdminKurtiVisualizationSkillId; pattern: RegExp }> = [
  { id: "render-waterfall", pattern: /(waterfall|wasserfall|brücke|bruecke|beitrag.*veränder|veraender|abweichung.*(zerleg|beitrag)|von .* zu .* durch)/i },
  { id: "render-box-plot", pattern: /(box.?plot|quartil|median.*streu|interquartil|ausrei[ßs]er|whisker|verteilung.*(gruppe|vergleich))/i },
  { id: "render-histogram", pattern: /(histogramm|häufigkeitsverteilung|haeufigkeitsverteilung|verteilung.*(ipp|wert|dauer|zeit|stunden|km|kilometer|punkte|score)|werteverteilung)/i },
  { id: "render-treemap", pattern: /(treemap|baumkarte|viele.*anteile|long.?tail|beitragsanteil|portfolio.*anteil)/i },
  { id: "render-bubble-chart", pattern: /(bubble|blasen.?diagramm|punktgröße|punktgroesse|dritte.*kennzahl)/i },
  { id: "render-scatter-chart", pattern: /(scatter|streu.?diagramm|korrelation|zusammenhang.*(zwischen|vs\.?|und)|gegenüber.*kennzahl|gegenueber.*kennzahl)/i },
  { id: "render-heatmap", pattern: /(heatmap|matrix|intensität|intensitaet|vollständigkeit|vollstaendigkeit|wochentag.*stunde|gm.*(tag|monat)|markt.*red)/i },
  { id: "render-funnel-chart", pattern: /(funnel|trichter|conversion|konversion|drop.?off|stufen|zugewiesen.*gestartet|gestartet.*abgeschlossen)/i },
  { id: "render-timeline", pattern: /(timeline|zeitstrahl|ereignis|änderungsverlauf|aenderungsverlauf|audit.*verlauf|chronolog)/i },
  { id: "render-radar-chart", pattern: /(radar|spinnen.?diagramm|profil.*dimension|dimension.*vergleich|qualitätsprofil|qualitaetsprofil)/i },
  { id: "render-horizontal-bar-chart", pattern: /(ranking|rangliste|top\s*\d*|bottom\s*\d*|höchste|hoechste|niedrigste|besten|schlechtesten|lange.*label)/i },
  { id: "render-stacked-bar-chart", pattern: /(stacked|gestapelt|aufgeteilt.*je|bestandteil.*gesamt|zusammensetzung.*(pro|je)|teile.*ganzen.*vergleich)/i },
  { id: "render-combo-chart", pattern: /(combo|kombiniert|balken.*linie|linie.*balken|ist.*ziel.*(chart|diagramm)|actual.*target)/i },
  { id: "render-donut-chart", pattern: /(donut|ring.?diagramm|anteil.*gesamt|zusammensetzung|verteilung.*status)/i },
  { id: "render-pie-chart", pattern: /(pie|torten.?diagramm|kreis.?diagramm)/i },
  { id: "render-area-chart", pattern: /(area|flächen.?diagramm|flaechen.?diagramm|volumen.*(zeit|verlauf)|kumuliert|gesamtmenge.*verlauf)/i },
  { id: "render-line-chart", pattern: /(linie|linien.?diagramm|entwicklung|trend|zeitreihe|verlauf.*(tag|woche|monat|jahr|red)|über zeit|ueber zeit)/i },
  { id: "render-bar-chart", pattern: /(balken|säulen.?diagramm|saeulen.?diagramm|vergleich.*(gm|markt|kampagne|kategorie|zeitraum)|gegenüberstellung|gegenueberstellung)/i },
  { id: "render-metrics", pattern: /(kpi|kennzahl|überblick|ueberblick|dashboard|summary|zusammenfassung|headline|statusübersicht|statusuebersicht)/i },
  { id: "render-table", pattern: /(tabelle|exakt|detailwerte|liste|aufschlüsselung|aufschluesselung|alle werte|drilldown)/i },
];

const SKILL_TO_TOOL_NAMES: Record<AdminKurtiVisualizationSkillId, readonly string[]> = {
  "choose-visualization": ["render_series_visualization", "render_metrics_visualization", "render_table_visualization"],
  "render-line-chart": ["render_series_visualization"],
  "render-area-chart": ["render_series_visualization"],
  "render-bar-chart": ["render_series_visualization"],
  "render-stacked-bar-chart": ["render_series_visualization"],
  "render-horizontal-bar-chart": ["render_series_visualization"],
  "render-combo-chart": ["render_series_visualization"],
  "render-donut-chart": ["render_composition_visualization"],
  "render-pie-chart": ["render_composition_visualization"],
  "render-funnel-chart": ["render_composition_visualization"],
  "render-scatter-chart": ["render_scatter_visualization"],
  "render-bubble-chart": ["render_scatter_visualization"],
  "render-heatmap": ["render_heatmap_visualization"],
  "render-metrics": ["render_metrics_visualization"],
  "render-table": ["render_table_visualization"],
  "render-timeline": ["render_timeline_visualization"],
  "render-radar-chart": ["render_radar_visualization"],
  "render-histogram": ["render_distribution_visualization"],
  "render-box-plot": ["render_distribution_visualization"],
  "render-waterfall": ["render_waterfall_visualization"],
  "render-treemap": ["render_treemap_visualization"],
};

const MAX_SELECTED_VARIANT_SKILLS = 4;
const MAX_SKILL_DOCUMENT_CHARACTERS = 24_000;

function addSkill(target: AdminKurtiVisualizationSkillId[], id: AdminKurtiVisualizationSkillId) {
  if (!target.includes(id)) target.push(id);
}

export function selectAdminKurtiVisualizationSkillIds(message: string): AdminKurtiVisualizationSkillId[] {
  if (!VISUALIZATION_OPPORTUNITY_PATTERN.test(message)) return [];
  const selected: AdminKurtiVisualizationSkillId[] = ["choose-visualization"];

  for (const matcher of SKILL_MATCHERS) {
    if (matcher.pattern.test(message)) addSkill(selected, matcher.id);
  }

  if (/dashboard|überblick|ueberblick|summary|zusammenfassung/i.test(message)) {
    addSkill(selected, "render-metrics");
    addSkill(selected, "render-line-chart");
    addSkill(selected, "render-table");
  }
  if (/anteil|zusammensetzung|verteilung/i.test(message) && !/(ipp|wert|dauer|zeit|stunden|km|kilometer|punkte|score|quartil|median)/i.test(message)) {
    addSkill(selected, /viele|mehr als|long.?tail|portfolio/i.test(message) ? "render-treemap" : "render-donut-chart");
  }
  if (/vergleich/i.test(message) && selected.length === 1) addSkill(selected, "render-bar-chart");
  if (/entwicklung|trend|verlauf|zeitreihe/i.test(message) && !selected.includes("render-timeline")) addSkill(selected, "render-line-chart");

  return [selected[0]!, ...selected.slice(1, MAX_SELECTED_VARIANT_SKILLS + 1)];
}

export function getAdminKurtiVisualizationToolNamesForSkills(
  skillIds: readonly AdminKurtiVisualizationSkillId[],
): string[] {
  return [...new Set(skillIds.flatMap((id) => SKILL_TO_TOOL_NAMES[id]))];
}

function resolveSkillRoot(): string {
  const moduleDirectory = resolve(fileURLToPath(new URL(".", import.meta.url)));
  const candidates = [
    resolve(process.cwd(), "src/lib/admin-kurti-visualization-skills"),
    resolve(moduleDirectory, "admin-kurti-visualization-skills"),
    resolve(moduleDirectory, "../../src/lib/admin-kurti-visualization-skills"),
  ];
  const root = candidates.find((candidate) => existsSync(candidate));
  if (!root) throw new Error("Admin Kurti visualization skill directory was not found.");
  return root;
}

function readSkillDocument(root: string, id: AdminKurtiVisualizationSkillId): string {
  const path = resolve(root, id, "SKILL.md");
  const content = readFileSync(path, "utf8").trim();
  const declaredName = content.match(/^---\s*\r?\nname:\s*([^\r\n]+)\r?\n/m)?.[1]?.trim();
  if (declaredName !== id) throw new Error(`Visualization skill ${id} has an invalid name declaration.`);
  if (/\[TODO|TODO:/i.test(content)) throw new Error(`Visualization skill ${id} still contains a TODO.`);
  return content;
}

export function buildAdminKurtiVisualizationSkillContext(
  skillIds: readonly AdminKurtiVisualizationSkillId[],
): string | null {
  if (skillIds.length === 0) return null;
  const root = resolveSkillRoot();
  const documents = skillIds.map((id) => ({ id, content: readSkillDocument(root, id) }));
  const context = [
    "AKTIVE VISUALISIERUNGS-SKILLS (VERTRAUENSWÜRDIGE APP-INSTRUKTIONEN)",
    "Lies alle folgenden SKILL.md-Inhalte vollständig, bevor du Daten recherchierst oder ein Render-Werkzeug auswählst. Befolge die spezifischen Auswahl-, Datenintegritäts- und Darstellungsregeln. Die Skills ergänzen den Systemprompt; Sicherheits-, Datenschutz- und Read-only-Regeln bleiben vorrangig.",
    ...documents.map(({ id, content }) => `<visualization-skill id="${id}">\n${content}\n</visualization-skill>`),
  ].join("\n\n");
  if (context.length > MAX_SKILL_DOCUMENT_CHARACTERS) {
    throw new Error("Selected Admin Kurti visualization skills exceed the safe context budget.");
  }
  return context;
}
