import type { Responses } from "openai/resources/responses/responses";
import { z } from "zod";
import { ADMIN_KURTI_TOOLS } from "./admin-kurti-tools.js";
import {
  getAdminKurtiVisualizationToolNamesForSkills,
  selectAdminKurtiVisualizationSkillIds,
} from "./admin-kurti-visualization-skill-loader.js";

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
    "load_admin_visualization_skill",
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
  const names = new Set<string>(["load_admin_tool_group", "load_admin_visualization_skill", "render_admin_chart"]);
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
  const skillIds = selectAdminKurtiVisualizationSkillIds(message);
  const requestedRenderTools = /(alle|all).*?(chart|diagramm|visualisierung)|(verschieden|different).*?(chart|diagramm|visualisierung)/i.test(message)
    ? ADMIN_KURTI_TOOL_GROUPS.visualizations
    : getAdminKurtiVisualizationToolNamesForSkills(skillIds);
  for (const name of requestedRenderTools) {
    const tool = TOOL_BY_NAME.get(name);
    if (tool) tools.push(tool);
  }
  return [...new Map(tools.map((tool) => [tool.name, tool])).values()];
}

export function addAdminKurtiToolGroups(
  currentTools: readonly Responses.FunctionTool[],
  groups: readonly AdminKurtiToolGroup[],
): Responses.FunctionTool[] {
  const names = new Set(currentTools.map((tool) => tool.name));
  names.add("load_admin_tool_group");
  names.add("load_admin_visualization_skill");
  for (const group of groups) {
    for (const name of ADMIN_KURTI_TOOL_GROUPS[group]) names.add(name);
  }
  return toolsForNames(names);
}

export function parseAdminKurtiToolGroupArguments(rawArguments: string): AdminKurtiToolGroup[] {
  const parsed = rawArguments.trim() ? JSON.parse(rawArguments) as unknown : {};
  return loaderArgumentsSchema.parse(parsed).groups;
}
