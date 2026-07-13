import type { Responses } from "openai/resources/responses/responses";
import { z } from "zod";
import { ADMIN_KURTI_TOOLS } from "./admin-kurti-tools.js";

export const ADMIN_KURTI_TOOL_GROUP_NAMES = [
  "overview",
  "people",
  "markets_inventory",
  "visits_campaigns",
  "time",
  "ipp",
  "bonus",
  "questionnaires",
  "photos",
  "reviews_audit",
  "visualizations",
] as const;

export type AdminKurtiToolGroup = typeof ADMIN_KURTI_TOOL_GROUP_NAMES[number];

export const ADMIN_KURTI_TOOL_GROUPS: Record<AdminKurtiToolGroup, readonly string[]> = {
  overview: ["get_admin_overview", "get_admin_module_catalog", "get_red_month_context"],
  people: ["get_user_access_context", "search_gms", "get_gm_context", "get_gm_kurti_chat_history"],
  markets_inventory: ["search_markets", "get_market_context", "get_inventory_context"],
  visits_campaigns: ["search_visits", "get_visit_context", "get_campaign_context", "get_visit_analytics", "get_campaign_performance_analytics"],
  time: ["get_time_context", "get_worktime_analytics"],
  ipp: ["get_ipp_context", "get_ipp_gm_analytics", "get_ipp_market_analytics", "get_ipp_visit_analytics", "get_red_month_context"],
  bonus: ["get_bonus_context"],
  questionnaires: ["get_questionnaire_context", "get_module_context", "get_question_context", "get_evaluation_context", "get_answer_distribution_analytics", "get_filter_context"],
  photos: ["search_photo_archive", "get_photo_analytics"],
  reviews_audit: ["get_pending_requests", "get_audit_history_context"],
  visualizations: [
    "render_series_visualization",
    "render_composition_visualization",
    "render_scatter_visualization",
    "render_heatmap_visualization",
    "render_metrics_visualization",
    "render_table_visualization",
    "render_timeline_visualization",
    "render_radar_visualization",
  ],
};

const loaderArgumentsSchema = z.object({
  groups: z.array(z.enum(ADMIN_KURTI_TOOL_GROUP_NAMES)).min(1).max(5),
}).strict();

const TOOL_BY_NAME = new Map(ADMIN_KURTI_TOOLS.map((tool) => [tool.name, tool]));

const GROUP_KEYWORDS: Array<{ group: AdminKurtiToolGroup; pattern: RegExp }> = [
  { group: "people", pattern: /\b(gm|gebietsmanager|mitarbeiter|person|personen|user|nutzer|konto|account|rolle|zugriff|berechtigung|chatverlauf)\b/i },
  { group: "markets_inventory", pattern: /\b(markt|märkte|market|stammnummer|stammnr|flexnummer|kette|handelskette|universum|kühler|kuehler|lager|inventar|seriennummer)\b/i },
  { group: "visits_campaigns", pattern: /\b(besuch|besuche|visit|visits|kampagne|campaign|zuweisung|zielbesuch|submitted|einreichung|abgeschlossen)\b/i },
  { group: "time", pattern: /\b(arbeitszeit|arbeitstag|stunden|stunde|zeiterfassung|pause|pausen|kilometer|km|fahrzeit|zusatzzeit|diäten|diaeten|homeoffice|schulung|werkstatt|arztbesuch)\b/i },
  { group: "ipp", pattern: /\b(ipp|red.?monat|redmonth|red month|ytd|scoring|punkteentwicklung)\b/i },
  { group: "bonus", pattern: /\b(bonus|prämie|praemie|prämien|praemien|welle|wave|belohnung|schwelle|säule|saeule)\b/i },
  { group: "questionnaires", pattern: /\b(fragebogen|fragebögen|frageboegen|questionnaire|modul|frage|antwort|bewertung|auswertung|regel|filter|matrix|pflichtfeld)\b/i },
  { group: "photos", pattern: /\b(foto|fotos|photo|photos|bild|bilder|fotoarchiv|tag|tags)\b/i },
  { group: "reviews_audit", pattern: /\b(anfrage|anfragen|änderungsantrag|aenderungsantrag|löschantrag|loeschantrag|audit|historie|verlauf|datenschutz|dsgvo|dsar|freigabe)\b/i },
];

const VISUALIZATION_PATTERN = /(chart|diagramm|grafik|visualis|dashboard|entwicklung|trend|verlauf|vergleich|ranking|anteil|verteilung|heatmap|tabelle|kpi|donut|pie|funnel|scatter|bubble|radar|timeline|torte|kreis|balken|s[äa]ule)/i;
const COMPOSITION_PATTERN = /(anteil|verteilung|zusammensetzung|donut|pie|torte|kreis|funnel|trichter)/i;
const RELATION_PATTERN = /\b(korrelation|zusammenhang|scatter|bubble|radar|dimension)\b/i;
const HEATMAP_PATTERN = /\b(heatmap|matrix|intensität|intensitaet|vollständigkeit|vollstaendigkeit)\b/i;
const TIMELINE_PATTERN = /\b(timeline|zeitstrahl|ereignis|historie|änderungsverlauf|aenderungsverlauf)\b/i;

function toolsForNames(names: Iterable<string>): Responses.FunctionTool[] {
  const unique = new Set(names);
  return [...unique].flatMap((name) => {
    const tool = TOOL_BY_NAME.get(name);
    return tool ? [tool] : [];
  });
}

export function getAdminKurtiToolsForGroups(groups: Iterable<AdminKurtiToolGroup>): Responses.FunctionTool[] {
  // Keep the legacy chart renderer available for every turn. Follow-up messages
  // such as "yes, do that" do not repeat chart keywords, but still belong to the
  // same visualization request and must be able to complete it.
  const names = new Set<string>(["load_admin_tool_group", "render_admin_chart"]);
  for (const group of groups) {
    for (const name of ADMIN_KURTI_TOOL_GROUPS[group]) names.add(name);
  }
  return toolsForNames(names);
}

export function selectAdminKurtiTools(message: string): Responses.FunctionTool[] {
  const groups = new Set<AdminKurtiToolGroup>();
  for (const entry of GROUP_KEYWORDS) {
    if (entry.pattern.test(message)) groups.add(entry.group);
  }
  if (groups.size === 0) groups.add("overview");
  if (groups.size <= 2) groups.add("overview");

  const tools = getAdminKurtiToolsForGroups(groups);
  if (VISUALIZATION_PATTERN.test(message)) {
    const preferredNames = new Set(["render_series_visualization", "render_metrics_visualization", "render_table_visualization"]);
    if (COMPOSITION_PATTERN.test(message)) preferredNames.add("render_composition_visualization");
    if (RELATION_PATTERN.test(message)) {
      preferredNames.add("render_scatter_visualization");
      preferredNames.add("render_radar_visualization");
    }
    if (HEATMAP_PATTERN.test(message)) preferredNames.add("render_heatmap_visualization");
    if (TIMELINE_PATTERN.test(message)) preferredNames.add("render_timeline_visualization");
    if (/\b(whatever|beliebig|alle|verschieden|dashboard)\b/i.test(message)) {
      for (const name of ADMIN_KURTI_TOOL_GROUPS.visualizations) preferredNames.add(name);
    }
    for (const name of preferredNames) {
      const tool = TOOL_BY_NAME.get(name);
      if (tool) tools.push(tool);
    }
  }
  return [...new Map(tools.map((tool) => [tool.name, tool])).values()];
}

export function addAdminKurtiToolGroups(
  currentTools: readonly Responses.FunctionTool[],
  groups: readonly AdminKurtiToolGroup[],
): Responses.FunctionTool[] {
  const names = new Set(currentTools.map((tool) => tool.name));
  names.add("load_admin_tool_group");
  for (const group of groups) {
    for (const name of ADMIN_KURTI_TOOL_GROUPS[group]) names.add(name);
  }
  return toolsForNames(names);
}

export function parseAdminKurtiToolGroupArguments(rawArguments: string): AdminKurtiToolGroup[] {
  const parsed = rawArguments.trim() ? JSON.parse(rawArguments) as unknown : {};
  return loaderArgumentsSchema.parse(parsed).groups;
}
