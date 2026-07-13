import type { Responses } from "openai/resources/responses/responses";
import { z } from "zod";
import { buildGmAggregates } from "./admin-zeiterfassung.js";
import { sql as pgSql } from "./db.js";
import { readGmKpiCache } from "./gm-kpi-cache.js";
import {
  computeMarketIppForPeriod,
  computeMarketIppSummariesForPeriod,
  computeMarketIppSummariesForRedPeriods,
  computeVisitIpp,
  computeVisitIppSummaries,
} from "./ipp.js";
import { buildKurtiGmContext } from "./kurti-context.js";
import { redMonthPeriodToYmd, resolveCurrentRedPeriod, resolveRedPeriodForDate } from "./red-month-periods.js";
import { loadZeiterfassungDaySessions } from "./zeiterfassung-days.js";

const UUID_SCHEMA = { type: "string", format: "uuid" } as const;
const NULLABLE_UUID_SCHEMA = { type: ["string", "null"], format: "uuid" } as const;
const NULLABLE_STRING_SCHEMA = { type: ["string", "null"] } as const;
const NULLABLE_BOOLEAN_SCHEMA = { type: ["boolean", "null"] } as const;
const LIMIT_SCHEMA = { type: "integer", minimum: 1, maximum: 150 } as const;
const CHAT_LIMIT_SCHEMA = { type: "integer", minimum: 1, maximum: 40 } as const;
const IPP_TIMEFRAME_SCHEMA = { type: "string", enum: ["red_month", "ytd", "custom", "all_time"] } as const;
const IPP_COMPARISON_SCHEMA = { type: "string", enum: ["none", "previous_period", "previous_year", "custom"] } as const;
const YMD_REGEX = /^\d{4}-\d{2}-\d{2}$/;
const VIENNA_TIMEZONE = "Europe/Vienna";

type JsonRecord = Record<string, unknown>;

function functionTool(
  name: string,
  description: string,
  properties: Record<string, unknown>,
  required: string[],
): Responses.FunctionTool {
  return {
    type: "function",
    name,
    description,
    strict: true,
    parameters: {
      type: "object",
      properties,
      required,
      additionalProperties: false,
    },
  };
}

export const ADMIN_KURTI_TOOLS: Responses.FunctionTool[] = [
  functionTool(
    "get_admin_overview",
    "Returns the current Coke Spark admin overview across user roles, active GM Kurti conversations, markets, campaigns, visits, photos, time tracking, review queues, and the current RED period. Use this first for broad status questions.",
    {},
    [],
  ),
  functionTool(
    "get_user_access_context",
    "Searches every Coke Spark user role and returns account status, contact/region data, customer page permissions, GM display settings, special filters, latest employee-agreement acceptance, and operational counts. It never returns auth IDs, tokens, passwords, agreement hashes, IP addresses, or user agents.",
    {
      userId: NULLABLE_UUID_SCHEMA,
      query: NULLABLE_STRING_SCHEMA,
      role: { type: ["string", "null"], enum: ["admin", "gm", "sm", "kunde", null] },
      activeOnly: { type: "boolean" },
      limit: LIMIT_SCHEMA,
    },
    ["userId", "query", "role", "activeOnly", "limit"],
  ),
  functionTool(
    "search_gms",
    "Finds GMs by name, email, or region and returns operational counts plus cached IPP and bonus KPIs. Use it to resolve a GM name to an app user ID before requesting deep GM context.",
    {
      query: NULLABLE_STRING_SCHEMA,
      region: NULLABLE_STRING_SCHEMA,
      activeOnly: { type: "boolean" },
      limit: LIMIT_SCHEMA,
    },
    ["query", "region", "activeOnly", "limit"],
  ),
  functionTool(
    "get_gm_context",
    "Returns a deep current context for one GM: profile, current workday, active and recent visits, week time/KM calculations, RED period, campaign assignments/progress, IPP/bonus KPI, and pending requests.",
    { gmUserId: UUID_SCHEMA },
    ["gmUserId"],
  ),
  functionTool(
    "get_gm_kurti_chat_history",
    "Reads GM Kurti messages that are still inside their active 15-minute Coke Spark retention window. Filter by resolved GM ID/name, message role, or content. Use only for a concrete admin support, quality, or troubleshooting question; expired messages are unavailable and must never be reconstructed.",
    {
      gmUserId: NULLABLE_UUID_SCHEMA,
      gmQuery: NULLABLE_STRING_SCHEMA,
      contentQuery: NULLABLE_STRING_SCHEMA,
      role: { type: ["string", "null"], enum: ["user", "assistant", null] },
      limit: CHAT_LIMIT_SCHEMA,
    },
    ["gmUserId", "gmQuery", "contentQuery", "role", "limit"],
  ),
  functionTool(
    "search_markets",
    "Finds markets by name, address, city, PLZ, Stammnummer, Coke master number, Flex number, GM, region, market type, or Universumsmarkt flag. Returns assignment and visit counts.",
    {
      query: NULLABLE_STRING_SCHEMA,
      region: NULLABLE_STRING_SCHEMA,
      gmName: NULLABLE_STRING_SCHEMA,
      marketType: { type: ["string", "null"], enum: ["universum", "kuehler", "both", null] },
      universeMarket: NULLABLE_BOOLEAN_SCHEMA,
      activeOnly: { type: "boolean" },
      limit: LIMIT_SCHEMA,
    },
    ["query", "region", "gmName", "marketType", "universeMarket", "activeOnly", "limit"],
  ),
  functionTool(
    "get_market_context",
    "Returns full admin context for one market: identity fields, address, GM, flags, cooler units, campaign assignments/targets/progress, recent visits, photo/answer counts, and finalized IPP history.",
    { marketId: UUID_SCHEMA },
    ["marketId"],
  ),
  functionTool(
    "search_visits",
    "Searches visit sessions across all GMs and markets with optional status, section, and date filters. Returns section/campaign composition and answer/photo/comment counts.",
    {
      gmUserId: NULLABLE_UUID_SCHEMA,
      marketId: NULLABLE_UUID_SCHEMA,
      section: { type: ["string", "null"], enum: ["standard", "flex", "billa", "kuehler", "mhd", null] },
      status: { type: ["string", "null"], enum: ["draft", "submitted", "cancelled", null] },
      from: NULLABLE_STRING_SCHEMA,
      to: NULLABLE_STRING_SCHEMA,
      limit: LIMIT_SCHEMA,
    },
    ["gmUserId", "marketId", "section", "status", "from", "to", "limit"],
  ),
  functionTool(
    "get_visit_context",
    "Returns a detailed visit snapshot including GM, market, sections, questionnaire names, every captured question/answer, selected options, comments, photo counts, and photo tags. Read-only.",
    { visitSessionId: UUID_SCHEMA },
    ["visitSessionId"],
  ),
  functionTool(
    "get_campaign_context",
    "Lists campaigns or returns one campaign in depth, including schedule, questionnaire, assignments, target/current visit counts, GM distribution, market progress, and latest submissions.",
    {
      campaignId: NULLABLE_UUID_SCHEMA,
      query: NULLABLE_STRING_SCHEMA,
      section: { type: ["string", "null"], enum: ["standard", "flex", "billa", "kuehler", "mhd", null] },
      status: { type: ["string", "null"], enum: ["active", "scheduled", "inactive", null] },
      limit: LIMIT_SCHEMA,
    },
    ["campaignId", "query", "section", "status", "limit"],
  ),
  functionTool(
    "get_time_context",
    "Uses the same authoritative admin Zeiterfassung calculation code as the UI. Returns day sessions and GM aggregates such as pure work minutes, average workday, driven KM, private-use KM, tracked days, and live sessions.",
    {
      gmUserId: NULLABLE_UUID_SCHEMA,
      from: NULLABLE_STRING_SCHEMA,
      to: NULLABLE_STRING_SCHEMA,
      includeLive: { type: "boolean" },
    },
    ["gmUserId", "from", "to", "includeLive"],
  ),
  functionTool(
    "get_ipp_context",
    "Uses the same authoritative IPP calculation as the admin IPP page. For a market it returns question-level applied IPP for a RED period; for a GM it returns cached all-time average/sample count; without filters it returns current-period market summaries.",
    {
      marketId: NULLABLE_UUID_SCHEMA,
      gmUserId: NULLABLE_UUID_SCHEMA,
      periodStart: NULLABLE_STRING_SCHEMA,
      limit: LIMIT_SCHEMA,
    },
    ["marketId", "gmUserId", "periodStart", "limit"],
  ),
  functionTool(
    "get_ipp_gm_analytics",
    "Provides extensive GM/person IPP analytics using market-per-RED-month samples: one RED month, RED-year YTD, custom ranges or all time; per-RED-month series, weighted averages, rankings, highest/lowest market samples, trends, and comparisons against the previous period, previous year, or a custom timeframe. Closed finalized snapshots are authoritative; missing/current periods are calculated live with the IPP engine.",
    {
      gmUserId: NULLABLE_UUID_SCHEMA,
      region: NULLABLE_STRING_SCHEMA,
      timeframe: IPP_TIMEFRAME_SCHEMA,
      periodStart: NULLABLE_STRING_SCHEMA,
      redYear: { type: ["integer", "null"], minimum: 2020, maximum: 2100 },
      fromPeriodStart: NULLABLE_STRING_SCHEMA,
      toPeriodStart: NULLABLE_STRING_SCHEMA,
      comparison: IPP_COMPARISON_SCHEMA,
      compareFromPeriodStart: NULLABLE_STRING_SCHEMA,
      compareToPeriodStart: NULLABLE_STRING_SCHEMA,
      includeMarketBreakdown: { type: "boolean" },
      limit: { type: "integer", minimum: 1, maximum: 100 },
    },
    ["gmUserId", "region", "timeframe", "periodStart", "redYear", "fromPeriodStart", "toPeriodStart", "comparison", "compareFromPeriodStart", "compareToPeriodStart", "includeMarketBreakdown", "limit"],
  ),
  functionTool(
    "get_ipp_market_analytics",
    "Analyzes and ranks market IPP over one RED month, YTD, a custom RED-month range, or all time. Supports market, GM, region and chain filters; returns market averages/history, period averages, highest or lowest market/RED-month samples, source visits, and trend changes. Uses finalized snapshots when available and live IPP calculation otherwise.",
    {
      marketId: NULLABLE_UUID_SCHEMA,
      gmUserId: NULLABLE_UUID_SCHEMA,
      region: NULLABLE_STRING_SCHEMA,
      chain: NULLABLE_STRING_SCHEMA,
      timeframe: IPP_TIMEFRAME_SCHEMA,
      periodStart: NULLABLE_STRING_SCHEMA,
      redYear: { type: ["integer", "null"], minimum: 2020, maximum: 2100 },
      fromPeriodStart: NULLABLE_STRING_SCHEMA,
      toPeriodStart: NULLABLE_STRING_SCHEMA,
      ranking: { type: "string", enum: ["highest", "lowest"] },
      includeHistory: { type: "boolean" },
      limit: { type: "integer", minimum: 1, maximum: 100 },
    },
    ["marketId", "gmUserId", "region", "chain", "timeframe", "periodStart", "redYear", "fromPeriodStart", "toPeriodStart", "ranking", "includeHistory", "limit"],
  ),
  functionTool(
    "get_ipp_visit_analytics",
    "Calculates analytical IPP for one submitted visit or ranks submitted visits by IPP within one RED month, YTD, a custom RED-month range, or all time. Supports GM, market, section, region and chain filters and can return a question-level breakdown for a selected visit. Visit IPP applies the current scoring configuration to that visit only and is not an official finalized market/RED-month KPI.",
    {
      visitSessionId: NULLABLE_UUID_SCHEMA,
      gmUserId: NULLABLE_UUID_SCHEMA,
      marketId: NULLABLE_UUID_SCHEMA,
      section: { type: ["string", "null"], enum: ["standard", "flex", "billa", "kuehler", "mhd", null] },
      region: NULLABLE_STRING_SCHEMA,
      chain: NULLABLE_STRING_SCHEMA,
      timeframe: IPP_TIMEFRAME_SCHEMA,
      periodStart: NULLABLE_STRING_SCHEMA,
      redYear: { type: ["integer", "null"], minimum: 2020, maximum: 2100 },
      fromPeriodStart: NULLABLE_STRING_SCHEMA,
      toPeriodStart: NULLABLE_STRING_SCHEMA,
      ranking: { type: "string", enum: ["highest", "lowest"] },
      includeQuestionBreakdown: { type: "boolean" },
      limit: { type: "integer", minimum: 1, maximum: 60 },
    },
    ["visitSessionId", "gmUserId", "marketId", "section", "region", "chain", "timeframe", "periodStart", "redYear", "fromPeriodStart", "toPeriodStart", "ranking", "includeQuestionBreakdown", "limit"],
  ),
  functionTool(
    "get_bonus_context",
    "Returns configured bonus waves, thresholds, pillars, source/scoring mappings, quality/flex scores, GM points, rewards, and contribution counts. Values come from the finalized bonus tables used by the admin bonus page.",
    {
      waveId: NULLABLE_UUID_SCHEMA,
      gmUserId: NULLABLE_UUID_SCHEMA,
      limit: LIMIT_SCHEMA,
    },
    ["waveId", "gmUserId", "limit"],
  ),
  functionTool(
    "get_admin_module_catalog",
    "Returns the complete read-only Admin-Kurti coverage catalog for every Coke Spark admin page and its authoritative datasets, plus live record counts. Use it when deciding which tool or module contains requested data.",
    {},
    [],
  ),
  functionTool(
    "get_module_context",
    "Lists every Standard, Flex, Billa, Kühlerinventur, and MHD module. For a resolved module it returns all linked questions in order, complete question configuration, normalized rules and targets, scoring, matrices, attachments, photo tags, chain filters, questionnaire usage, and visit usage.",
    {
      scope: { type: ["string", "null"], enum: ["main", "kuehler", "mhd", null] },
      moduleId: NULLABLE_UUID_SCHEMA,
      query: NULLABLE_STRING_SCHEMA,
      includeQuestions: { type: "boolean" },
      limit: LIMIT_SCHEMA,
    },
    ["scope", "moduleId", "query", "includeQuestions", "limit"],
  ),
  functionTool(
    "get_question_context",
    "Searches the complete shared question bank and questionnaire-specific special items. Returns legacy and normalized configuration, conditional rules and targets, scoring/Bewertung rows, matrices, attachment metadata, photo tags, module/questionnaire memberships, chain filters, campaigns, and real visit/answer usage.",
    {
      questionId: NULLABLE_UUID_SCHEMA,
      moduleId: NULLABLE_UUID_SCHEMA,
      questionnaireId: NULLABLE_UUID_SCHEMA,
      scope: { type: ["string", "null"], enum: ["main", "kuehler", "mhd", null] },
      query: NULLABLE_STRING_SCHEMA,
      questionType: NULLABLE_STRING_SCHEMA,
      includeUsage: { type: "boolean" },
      limit: LIMIT_SCHEMA,
    },
    ["questionId", "moduleId", "questionnaireId", "scope", "query", "questionType", "includeUsage", "limit"],
  ),
  functionTool(
    "get_evaluation_context",
    "Reads real questionnaire evaluation data across visits: question/answer snapshots, answer values, selected options, matrix cells, validation, comments, photos/tags, answer-change requests, answer audit events, configured scoring rows, GM, market, campaign, questionnaire, and date/status filters. It is read-only; authoritative period IPP remains in get_ipp_context and bonus totals in get_bonus_context.",
    {
      questionId: NULLABLE_UUID_SCHEMA,
      moduleId: NULLABLE_UUID_SCHEMA,
      questionnaireId: NULLABLE_UUID_SCHEMA,
      campaignId: NULLABLE_UUID_SCHEMA,
      gmUserId: NULLABLE_UUID_SCHEMA,
      marketId: NULLABLE_UUID_SCHEMA,
      section: { type: ["string", "null"], enum: ["standard", "flex", "billa", "kuehler", "mhd", null] },
      visitStatus: { type: "string", enum: ["all", "draft", "submitted", "cancelled"] },
      from: NULLABLE_STRING_SCHEMA,
      to: NULLABLE_STRING_SCHEMA,
      includeInvalid: { type: "boolean" },
      includeDetails: { type: "boolean" },
      limit: LIMIT_SCHEMA,
    },
    ["questionId", "moduleId", "questionnaireId", "campaignId", "gmUserId", "marketId", "section", "visitStatus", "from", "to", "includeInvalid", "includeDetails", "limit"],
  ),
  functionTool(
    "get_filter_context",
    "Returns every relevant filter/rule configuration: normalized and legacy question visibility rules, rule targets, question and module chain filters, single-choice availability filters, GM special filters, market filter facets, campaign status/section filters, and questionnaire scope/schedule filters.",
    {
      kind: { type: "string", enum: ["all", "question_rules", "chain_filters", "gm_filters", "market_filters", "campaign_filters", "questionnaire_filters"] },
      entityId: NULLABLE_UUID_SCHEMA,
      query: NULLABLE_STRING_SCHEMA,
      limit: LIMIT_SCHEMA,
    },
    ["kind", "entityId", "query", "limit"],
  ),
  functionTool(
    "get_questionnaire_context",
    "Lists questionnaire/module/question configuration for Standard, Flex, Billa, Kühlerinventur, and MHD. With a questionnaire ID it returns the linked modules and questions, rules, configuration, photo tags, and scoring metadata.",
    {
      scope: { type: ["string", "null"], enum: ["main", "kuehler", "mhd", null] },
      questionnaireId: NULLABLE_UUID_SCHEMA,
      query: NULLABLE_STRING_SCHEMA,
      includeQuestions: { type: "boolean" },
      limit: LIMIT_SCHEMA,
    },
    ["scope", "questionnaireId", "query", "includeQuestions", "limit"],
  ),
  functionTool(
    "search_photo_archive",
    "Searches photo archive metadata across GMs, markets, visits, questions, campaigns, and photo tags. It returns references and metadata, never signed URLs or storage credentials.",
    {
      query: NULLABLE_STRING_SCHEMA,
      gmUserId: NULLABLE_UUID_SCHEMA,
      marketId: NULLABLE_UUID_SCHEMA,
      tag: NULLABLE_STRING_SCHEMA,
      from: NULLABLE_STRING_SCHEMA,
      to: NULLABLE_STRING_SCHEMA,
      limit: LIMIT_SCHEMA,
    },
    ["query", "gmUserId", "marketId", "tag", "from", "to", "limit"],
  ),
  functionTool(
    "get_inventory_context",
    "Returns warehouse assignments and/or market cooler-unit inventory, including assigned GMs, cooler internal IDs, serial numbers, models, and market identity.",
    {
      kind: { type: "string", enum: ["all", "lager", "kuehler"] },
      gmUserId: NULLABLE_UUID_SCHEMA,
      marketId: NULLABLE_UUID_SCHEMA,
      query: NULLABLE_STRING_SCHEMA,
      limit: LIMIT_SCHEMA,
    },
    ["kind", "gmUserId", "marketId", "query", "limit"],
  ),
  functionTool(
    "get_pending_requests",
    "Returns admin review queues for answer changes, visit deletion requests, time-entry changes, and DSGVO requests, with GM/market context and requested/current values where applicable.",
    {
      kind: { type: "string", enum: ["all", "answer_change", "visit_delete", "time_change", "dsar"] },
      status: NULLABLE_STRING_SCHEMA,
      gmUserId: NULLABLE_UUID_SCHEMA,
      limit: LIMIT_SCHEMA,
    },
    ["kind", "status", "gmUserId", "limit"],
  ),
  functionTool(
    "get_audit_history_context",
    "Returns sanitized read-only history for auth/account events, campaign questionnaire/assignment changes, questionnaire answer-configuration changes, time-entry events, visit-answer events, or DSGVO request events. Sensitive credential, storage, medical-document, IP, and raw-auth fields are removed.",
    {
      kind: { type: "string", enum: ["auth", "campaign", "questionnaire", "time", "visit_answer", "dsar"] },
      entityId: NULLABLE_UUID_SCHEMA,
      gmUserId: NULLABLE_UUID_SCHEMA,
      from: NULLABLE_STRING_SCHEMA,
      to: NULLABLE_STRING_SCHEMA,
      limit: LIMIT_SCHEMA,
    },
    ["kind", "entityId", "gmUserId", "from", "to", "limit"],
  ),
  functionTool(
    "get_red_month_context",
    "Returns the active RED year, current RED month, configured period boundaries, cycle, labels, and visit counts by period. Use it before period-based comparisons.",
    { year: { type: ["integer", "null"], minimum: 2020, maximum: 2100 } },
    ["year"],
  ),
];

const searchGmsSchema = z.object({
  query: z.string().nullable(),
  region: z.string().nullable(),
  activeOnly: z.boolean(),
  limit: z.number().int().min(1).max(150),
});

const userAccessSchema = z.object({
  userId: z.string().uuid().nullable(),
  query: z.string().nullable(),
  role: z.enum(["admin", "gm", "sm", "kunde"]).nullable(),
  activeOnly: z.boolean(),
  limit: z.number().int().min(1).max(150),
});

const gmKurtiChatHistorySchema = z.object({
  gmUserId: z.string().uuid().nullable(),
  gmQuery: z.string().nullable(),
  contentQuery: z.string().nullable(),
  role: z.enum(["user", "assistant"]).nullable(),
  limit: z.number().int().min(1).max(40),
});

const searchMarketsSchema = z.object({
  query: z.string().nullable(),
  region: z.string().nullable(),
  gmName: z.string().nullable(),
  marketType: z.enum(["universum", "kuehler", "both"]).nullable(),
  universeMarket: z.boolean().nullable(),
  activeOnly: z.boolean(),
  limit: z.number().int().min(1).max(150),
});

const searchVisitsSchema = z.object({
  gmUserId: z.string().uuid().nullable(),
  marketId: z.string().uuid().nullable(),
  section: z.enum(["standard", "flex", "billa", "kuehler", "mhd"]).nullable(),
  status: z.enum(["draft", "submitted", "cancelled"]).nullable(),
  from: z.string().nullable(),
  to: z.string().nullable(),
  limit: z.number().int().min(1).max(150),
});

const auditHistorySchema = z.object({
  kind: z.enum(["auth", "campaign", "questionnaire", "time", "visit_answer", "dsar"]),
  entityId: z.string().uuid().nullable(),
  gmUserId: z.string().uuid().nullable(),
  from: z.string().nullable(),
  to: z.string().nullable(),
  limit: z.number().int().min(1).max(150),
});

const moduleContextSchema = z.object({
  scope: z.enum(["main", "kuehler", "mhd"]).nullable(),
  moduleId: z.string().uuid().nullable(),
  query: z.string().nullable(),
  includeQuestions: z.boolean(),
  limit: z.number().int().min(1).max(150),
});

const questionContextSchema = z.object({
  questionId: z.string().uuid().nullable(),
  moduleId: z.string().uuid().nullable(),
  questionnaireId: z.string().uuid().nullable(),
  scope: z.enum(["main", "kuehler", "mhd"]).nullable(),
  query: z.string().nullable(),
  questionType: z.string().nullable(),
  includeUsage: z.boolean(),
  limit: z.number().int().min(1).max(150),
});

const evaluationContextSchema = z.object({
  questionId: z.string().uuid().nullable(),
  moduleId: z.string().uuid().nullable(),
  questionnaireId: z.string().uuid().nullable(),
  campaignId: z.string().uuid().nullable(),
  gmUserId: z.string().uuid().nullable(),
  marketId: z.string().uuid().nullable(),
  section: z.enum(["standard", "flex", "billa", "kuehler", "mhd"]).nullable(),
  visitStatus: z.enum(["all", "draft", "submitted", "cancelled"]),
  from: z.string().nullable(),
  to: z.string().nullable(),
  includeInvalid: z.boolean(),
  includeDetails: z.boolean(),
  limit: z.number().int().min(1).max(150),
});

const filterContextSchema = z.object({
  kind: z.enum(["all", "question_rules", "chain_filters", "gm_filters", "market_filters", "campaign_filters", "questionnaire_filters"]),
  entityId: z.string().uuid().nullable(),
  query: z.string().nullable(),
  limit: z.number().int().min(1).max(150),
});

const ippTimeframeSchema = z.enum(["red_month", "ytd", "custom", "all_time"]);

const ippGmAnalyticsSchema = z.object({
  gmUserId: z.string().uuid().nullable(),
  region: z.string().nullable(),
  timeframe: ippTimeframeSchema,
  periodStart: z.string().nullable(),
  redYear: z.number().int().min(2020).max(2100).nullable(),
  fromPeriodStart: z.string().nullable(),
  toPeriodStart: z.string().nullable(),
  comparison: z.enum(["none", "previous_period", "previous_year", "custom"]),
  compareFromPeriodStart: z.string().nullable(),
  compareToPeriodStart: z.string().nullable(),
  includeMarketBreakdown: z.boolean(),
  limit: z.number().int().min(1).max(100),
});

const ippMarketAnalyticsSchema = z.object({
  marketId: z.string().uuid().nullable(),
  gmUserId: z.string().uuid().nullable(),
  region: z.string().nullable(),
  chain: z.string().nullable(),
  timeframe: ippTimeframeSchema,
  periodStart: z.string().nullable(),
  redYear: z.number().int().min(2020).max(2100).nullable(),
  fromPeriodStart: z.string().nullable(),
  toPeriodStart: z.string().nullable(),
  ranking: z.enum(["highest", "lowest"]),
  includeHistory: z.boolean(),
  limit: z.number().int().min(1).max(100),
});

const ippVisitAnalyticsSchema = z.object({
  visitSessionId: z.string().uuid().nullable(),
  gmUserId: z.string().uuid().nullable(),
  marketId: z.string().uuid().nullable(),
  section: z.enum(["standard", "flex", "billa", "kuehler", "mhd"]).nullable(),
  region: z.string().nullable(),
  chain: z.string().nullable(),
  timeframe: ippTimeframeSchema,
  periodStart: z.string().nullable(),
  redYear: z.number().int().min(2020).max(2100).nullable(),
  fromPeriodStart: z.string().nullable(),
  toPeriodStart: z.string().nullable(),
  ranking: z.enum(["highest", "lowest"]),
  includeQuestionBreakdown: z.boolean(),
  limit: z.number().int().min(1).max(60),
});

function parseYmd(value: string | null, fallback: Date): string {
  return value && YMD_REGEX.test(value) ? value : fallback.toISOString().slice(0, 10);
}

function daysAgo(days: number): Date {
  const value = new Date();
  value.setUTCDate(value.getUTCDate() - days);
  return value;
}

function redactSensitiveText(value: string): string {
  return value
    .replace(/\bsk-[A-Za-z0-9_-]{8,}\b/g, "[REDACTED_API_KEY]")
    .replace(/\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b/g, "[REDACTED_TOKEN]")
    .replace(/\b(password|passwort|api[ _-]?key|token|secret)\s*[:=]\s*\S+/gi, "$1: [REDACTED]");
}

function sanitizeSensitiveValue(value: unknown, depth = 0): unknown {
  if (depth > 8) return "[TRUNCATED_NESTING]";
  if (typeof value === "string") return redactSensitiveText(value);
  if (Array.isArray(value)) return value.map((entry) => sanitizeSensitiveValue(entry, depth + 1));
  if (!value || typeof value !== "object") return value;

  const sanitized: JsonRecord = {};
  for (const [key, entry] of Object.entries(value as JsonRecord)) {
    if (/(password|passwort|secret|token|auth|supabase|service.?role|api.?key|signed.?url|storage.?path|bucket|doctor|medical|user.?agent|ip.?address|accepted.?ip)/i.test(key)) {
      sanitized[key] = "[REDACTED]";
      continue;
    }
    sanitized[key] = sanitizeSensitiveValue(entry, depth + 1);
  }
  return sanitized;
}

function envelope(tool: string, data: unknown, meta?: JsonRecord): JsonRecord {
  return {
    tool,
    generatedAt: new Date().toISOString(),
    authoritativeSource: "Coke Spark backend database and calculation services",
    readOnly: true,
    ...(meta ?? {}),
    data,
  };
}

async function getAdminOverview(): Promise<JsonRecord> {
  const [counts, campaigns, pending, redPeriod] = await Promise.all([
    pgSql<JsonRecord[]>`
      select
        (select count(*)::int from users where role = 'admin' and is_active = true and deleted_at is null) as active_admins,
        (select count(*)::int from users where role = 'gm' and is_active = true and deleted_at is null) as active_gms,
        (select count(*)::int from users where role = 'gm' and (is_active = false or deleted_at is not null)) as inactive_gms,
        (select count(*)::int from users where role = 'sm' and is_active = true and deleted_at is null) as active_sms,
        (select count(*)::int from users where role = 'kunde' and is_active = true and deleted_at is null) as active_customer_users,
        (select count(distinct gm_user_id)::int from gm_kurti_messages where expires_at > now()) as active_gm_kurti_conversations,
        (select count(*)::int from gm_kurti_messages where expires_at > now()) as active_gm_kurti_messages,
        (select count(*)::int from markets where is_deleted = false and is_active = true) as active_markets,
        (select count(*)::int from markets where is_deleted = false and universe_market = true) as universe_markets,
        (select count(*)::int from markets where is_deleted = false and market_type in ('kuehler', 'both')) as cooler_markets,
        (select count(*)::int from market_kuehler_units where is_deleted = false) as cooler_units,
        (select count(*)::int from visit_sessions where is_deleted = false and status = 'submitted') as submitted_visits_all_time,
        (select count(*)::int from visit_sessions where is_deleted = false and status = 'draft') as draft_visits,
        (select count(*)::int from visit_sessions where is_deleted = false and status = 'submitted' and submitted_at >= now() - interval '7 days') as submitted_visits_last_7_days,
        (select count(*)::int from visit_answer_photos where is_deleted = false) as active_photos,
        (select count(*)::int from question_bank_shared where is_deleted = false) as active_questions,
        (select count(*)::int from lager where is_deleted = false) as active_warehouses,
        (select count(*)::int from gm_day_sessions where is_deleted = false and status in ('started', 'ended')) as live_or_unsubmitted_workdays
    `,
    pgSql<JsonRecord[]>`
      select section::text, status::text, count(*)::int as count
      from campaigns
      where is_deleted = false
      group by section, status
      order by section, status
    `,
    pgSql<JsonRecord[]>`
      select
        (select count(*)::int from visit_answer_change_requests where is_deleted = false and status = 'pending') as answer_changes,
        (select count(*)::int from visit_session_delete_requests where is_deleted = false and status = 'pending') as visit_deletions,
        (select count(*)::int from time_entry_change_requests where is_deleted = false and status = 'pending') as time_changes,
        (select count(*)::int from dsar_requests where is_deleted = false and status not in ('closed', 'cancelled')) as dsar_open
    `,
    resolveCurrentRedPeriod(new Date()),
  ]);
  const redRange = redMonthPeriodToYmd(redPeriod);
  const redCounts = await pgSql<JsonRecord[]>`
    select count(*)::int as submitted_visits
    from visit_sessions
    where is_deleted = false
      and status = 'submitted'
      and submitted_at is not null
      and submitted_at >= (${redRange.startYmd}::date at time zone ${VIENNA_TIMEZONE})
      and submitted_at < ((${redRange.endYmd}::date + 1) at time zone ${VIENNA_TIMEZONE})
  `;
  return envelope("get_admin_overview", {
    totals: counts[0] ?? {},
    campaignsBySectionAndStatus: campaigns,
    pendingReviewQueues: pending[0] ?? {},
    currentRedPeriod: {
      label: redPeriod.label,
      redYear: redPeriod.redYear,
      start: redRange.startYmd,
      end: redRange.endYmd,
      submittedVisits: Number(redCounts[0]?.submitted_visits ?? 0),
    },
  });
}

async function getUserAccessContext(args: unknown): Promise<JsonRecord> {
  const input = userAccessSchema.parse(args);
  const query = input.query?.trim() || null;
  const rows = await pgSql<JsonRecord[]>`
    select
      u.id,
      concat_ws(' ', u.first_name, u.last_name) as full_name,
      u.first_name,
      u.last_name,
      u.email,
      u.role::text,
      u.phone,
      u.address,
      u.city,
      u.postal_code,
      u.region,
      u.is_billa_gm,
      u.is_active,
      u.deleted_at,
      u.anonymized_at,
      u.created_at,
      u.updated_at,
      ku.page_permissions as customer_page_permissions,
      case when ku.user_id is null then null else not ku.is_deleted end as customer_access_active,
      ku.created_at as customer_access_created_at,
      ku.updated_at as customer_access_updated_at,
      case when ts.is_deleted = false then ts.text_scale_percent else null end as text_scale_percent,
      (select coalesce(jsonb_agg(jsonb_build_object(
          'matchValue', sf.match_value,
          'createdAt', sf.created_at
        ) order by sf.created_at), '[]'::jsonb)
       from special_arthur_filter sf
       where sf.gm_user_id = u.id and sf.is_deleted = false) as special_filters,
      (select jsonb_build_object(
          'agreementKey', ea.agreement_key,
          'agreementVersion', ea.agreement_version,
          'agreementTitle', ea.agreement_title,
          'acceptedAt', ea.accepted_at
        )
       from employee_agreement_acceptances ea
       where ea.user_id = u.id
       order by ea.accepted_at desc
       limit 1) as latest_employee_agreement,
      (select count(distinct cma.market_id)::int
       from campaign_market_assignments cma
       where cma.gm_user_id = u.id and cma.is_deleted = false) as assigned_markets,
      (select count(*)::int
       from visit_sessions vs
       where vs.gm_user_id = u.id and vs.is_deleted = false and vs.status = 'submitted') as submitted_visits,
      (select count(*)::int
       from gm_kurti_messages km
       where km.gm_user_id = u.id and km.expires_at > now()) as active_kurti_messages,
      (select max(km.created_at)
       from gm_kurti_messages km
       where km.gm_user_id = u.id and km.expires_at > now()) as latest_active_kurti_message_at
    from users u
    left join kunde_users ku on ku.user_id = u.id
    left join gm_text_settings ts on ts.user_id = u.id
    where (${input.userId}::uuid is null or u.id = ${input.userId})
      and (${input.role}::text is null or u.role::text = ${input.role})
      and (${input.activeOnly}::boolean = false or (u.is_active = true and u.deleted_at is null))
      and (${query}::text is null or concat_ws(' ', u.first_name, u.last_name, u.email, u.role::text, u.region, u.city, u.postal_code) ilike '%' || ${query} || '%')
    order by u.is_active desc, u.role, u.last_name, u.first_name
    limit ${input.limit}
  `;
  return envelope("get_user_access_context", rows, { resultCount: rows.length });
}

async function searchGms(args: unknown): Promise<JsonRecord> {
  const input = searchGmsSchema.parse(args);
  const query = input.query?.trim() || null;
  const region = input.region?.trim() || null;
  const rows = await pgSql<JsonRecord[]>`
    select
      u.id,
      u.first_name,
      u.last_name,
      concat_ws(' ', u.first_name, u.last_name) as full_name,
      u.email,
      u.region,
      u.is_billa_gm,
      u.is_active,
      u.created_at,
      coalesce(k.ipp_all_time_avg, 0)::numeric as ipp_all_time_average,
      coalesce(k.ipp_sample_count, 0)::int as ipp_sample_count,
      coalesce(k.bonus_cumulative_eur, 0)::numeric as bonus_cumulative_eur,
      k.last_computed_at as kpi_last_computed_at,
      (select count(distinct cma.market_id)::int from campaign_market_assignments cma where cma.gm_user_id = u.id and cma.is_deleted = false) as assigned_markets,
      (select count(*)::int from visit_sessions vs where vs.gm_user_id = u.id and vs.is_deleted = false and vs.status = 'submitted') as submitted_visits,
      (select count(*)::int from visit_sessions vs where vs.gm_user_id = u.id and vs.is_deleted = false and vs.status = 'draft') as active_visit_drafts,
      (select count(*)::int from gm_day_sessions ds where ds.gm_user_id = u.id and ds.is_deleted = false and ds.status in ('started', 'ended')) as open_workdays,
      (select count(*)::int from visit_answer_change_requests r where r.gm_user_id = u.id and r.is_deleted = false and r.status = 'pending') as pending_answer_changes,
      (select count(*)::int from time_entry_change_requests r where r.gm_user_id = u.id and r.is_deleted = false and r.status = 'pending') as pending_time_changes,
      (select count(*)::int from gm_kurti_messages km where km.gm_user_id = u.id and km.expires_at > now()) as active_kurti_messages,
      (select max(km.created_at) from gm_kurti_messages km where km.gm_user_id = u.id and km.expires_at > now()) as latest_active_kurti_message_at
    from users u
    left join gm_kpi_cache k on k.gm_user_id = u.id and k.is_deleted = false
    where u.role = 'gm'
      and (${input.activeOnly}::boolean = false or (u.is_active = true and u.deleted_at is null))
      and (${query}::text is null or concat_ws(' ', u.first_name, u.last_name, u.email, u.region) ilike '%' || ${query} || '%')
      and (${region}::text is null or u.region ilike ${region})
    order by u.is_active desc, u.last_name, u.first_name
    limit ${input.limit}
  `;
  return envelope("search_gms", rows, { resultCount: rows.length });
}

async function getGmContext(args: unknown): Promise<JsonRecord> {
  const { gmUserId } = z.object({ gmUserId: z.string().uuid() }).parse(args);
  const context = await buildKurtiGmContext(gmUserId, new Date());
  return envelope("get_gm_context", context, { gmUserId });
}

async function getGmKurtiChatHistory(args: unknown): Promise<JsonRecord> {
  const input = gmKurtiChatHistorySchema.parse(args);
  const gmQuery = input.gmQuery?.trim() || null;
  const contentQuery = input.contentQuery?.trim() || null;
  const rows = await pgSql<JsonRecord[]>`
    with latest_messages as (
      select
        km.id,
        km.gm_user_id,
        concat_ws(' ', u.first_name, u.last_name) as gm_name,
        u.email as gm_email,
        u.region as gm_region,
        u.is_active as gm_is_active,
        km.role,
        km.content,
        km.created_at,
        km.expires_at
      from gm_kurti_messages km
      join users u on u.id = km.gm_user_id and u.role = 'gm'
      where km.expires_at > now()
        and (${input.gmUserId}::uuid is null or km.gm_user_id = ${input.gmUserId})
        and (${gmQuery}::text is null or concat_ws(' ', u.first_name, u.last_name, u.email, u.region) ilike '%' || ${gmQuery} || '%')
        and (${contentQuery}::text is null or km.content ilike '%' || ${contentQuery} || '%')
        and (${input.role}::text is null or km.role = ${input.role})
      order by km.created_at desc
      limit ${input.limit}
    )
    select * from latest_messages order by created_at asc
  `;
  const messages = rows.map((row) => ({
    ...row,
    content: redactSensitiveText(String(row.content ?? "")),
  }));
  return envelope("get_gm_kurti_chat_history", messages, {
    resultCount: messages.length,
    retentionWindowMinutes: 15,
    onlyUnexpiredMessages: true,
    sensitiveTextRedaction: true,
  });
}

async function searchMarkets(args: unknown): Promise<JsonRecord> {
  const input = searchMarketsSchema.parse(args);
  const query = input.query?.trim() || null;
  const region = input.region?.trim() || null;
  const gmName = input.gmName?.trim() || null;
  const rows = await pgSql<JsonRecord[]>`
    select
      m.id,
      m.name,
      m.db_name as chain,
      m.standard_market_number,
      m.coke_master_number,
      m.flex_number,
      m.address,
      m.postal_code,
      m.city,
      m.region,
      m.current_gm_name,
      m.employee,
      m.visit_frequency_per_year,
      m.info_flag,
      m.info_note,
      m.universe_market,
      m.market_type::text,
      m.kuehler_stammnr,
      m.is_active,
      (select count(*)::int from market_kuehler_units ku where ku.market_id = m.id and ku.is_deleted = false) as cooler_unit_count,
      (select count(*)::int from campaign_market_assignments cma where cma.market_id = m.id and cma.is_deleted = false) as active_assignment_rows,
      (select coalesce(sum(cma.visit_target_count), 0)::int from campaign_market_assignments cma where cma.market_id = m.id and cma.is_deleted = false) as target_visits,
      (select count(*)::int from visit_sessions vs where vs.market_id = m.id and vs.is_deleted = false and vs.status = 'submitted') as submitted_visits,
      (select max(vs.submitted_at) from visit_sessions vs where vs.market_id = m.id and vs.is_deleted = false and vs.status = 'submitted') as latest_visit_at
    from markets m
    where m.is_deleted = false
      and (${input.activeOnly}::boolean = false or m.is_active = true)
      and (${query}::text is null or concat_ws(' ', m.name, m.db_name, m.standard_market_number, m.coke_master_number, m.flex_number, m.address, m.postal_code, m.city) ilike '%' || ${query} || '%')
      and (${region}::text is null or m.region ilike ${region})
      and (${gmName}::text is null or concat_ws(' ', m.current_gm_name, m.employee) ilike '%' || ${gmName} || '%')
      and (${input.marketType}::text is null or m.market_type::text = ${input.marketType})
      and (${input.universeMarket}::boolean is null or m.universe_market = ${input.universeMarket})
    order by m.is_active desc, m.name, m.postal_code
    limit ${input.limit}
  `;
  return envelope("search_markets", rows, { resultCount: rows.length });
}

async function getMarketContext(args: unknown): Promise<JsonRecord> {
  const { marketId } = z.object({ marketId: z.string().uuid() }).parse(args);
  const [marketRows, coolerUnits, assignments, visits, ippHistory] = await Promise.all([
    pgSql<JsonRecord[]>`select * from markets where id = ${marketId} and is_deleted = false limit 1`,
    pgSql<JsonRecord[]>`
      select id, name, employee, kuehler_internal_id, kuehler_bd, kuehler_anzahl_ks_am_standort,
             kuehler_serial_number, kuehler_model, imported_at
      from market_kuehler_units
      where market_id = ${marketId} and is_deleted = false
      order by kuehler_internal_id nulls last, name
    `,
    pgSql<JsonRecord[]>`
      select c.id as campaign_id, c.name as campaign_name, c.section::text, c.status::text,
             c.schedule_type::text, c.start_date, c.end_date,
             cma.id as assignment_id, cma.assignment_slot, cma.visit_target_count,
             cma.current_visits_count, cma.gm_user_id,
             concat_ws(' ', u.first_name, u.last_name) as gm_name,
             (select count(*)::int from visit_sessions vs
                join visit_session_sections vss on vss.visit_session_id = vs.id and vss.is_deleted = false
               where vs.market_id = ${marketId} and vs.status = 'submitted' and vs.is_deleted = false and vss.campaign_id = c.id) as actual_submitted_visits
      from campaign_market_assignments cma
      join campaigns c on c.id = cma.campaign_id and c.is_deleted = false
      left join users u on u.id = cma.gm_user_id
      where cma.market_id = ${marketId} and cma.is_deleted = false
      order by c.section, c.name, cma.assignment_slot
    `,
    pgSql<JsonRecord[]>`
      select vs.id, vs.status::text, vs.started_at, vs.submitted_at,
             concat_ws(' ', u.first_name, u.last_name) as gm_name,
             u.id as gm_user_id,
             (select jsonb_agg(jsonb_build_object('section', vss.section::text, 'campaignId', c.id, 'campaign', c.name, 'questionnaire', vss.fragebogen_name_snapshot) order by vss.order_index)
                from visit_session_sections vss left join campaigns c on c.id = vss.campaign_id
               where vss.visit_session_id = vs.id and vss.is_deleted = false) as sections,
             (select count(*)::int from visit_answers va where va.visit_session_id = vs.id and va.is_deleted = false) as answer_count,
             (select count(*)::int from visit_answer_photos p join visit_answers va on va.id = p.visit_answer_id where va.visit_session_id = vs.id and va.is_deleted = false and p.is_deleted = false) as photo_count
      from visit_sessions vs
      join users u on u.id = vs.gm_user_id
      where vs.market_id = ${marketId} and vs.is_deleted = false
      order by coalesce(vs.submitted_at, vs.started_at) desc
      limit 30
    `,
    pgSql<JsonRecord[]>`
      select id, red_period_id, red_period_label, red_period_start, red_period_end,
             market_ipp, source_submission_count, contributing_question_count,
             is_finalized, computed_at, finalized_at
      from ipp_market_redmonth_results
      where market_id = ${marketId} and is_deleted = false
      order by red_period_start desc
      limit 24
    `,
  ]);
  if (!marketRows[0]) throw Object.assign(new Error("Markt nicht gefunden."), { statusCode: 404, code: "market_not_found" });
  return envelope("get_market_context", {
    market: marketRows[0],
    coolerUnits,
    campaignAssignments: assignments,
    recentVisits: visits,
    finalizedIppHistory: ippHistory,
  }, { marketId });
}

async function searchVisits(args: unknown): Promise<JsonRecord> {
  const input = searchVisitsSchema.parse(args);
  const from = input.from && YMD_REGEX.test(input.from) ? input.from : null;
  const to = input.to && YMD_REGEX.test(input.to) ? input.to : null;
  const rows = await pgSql<JsonRecord[]>`
    select
      vs.id,
      vs.status::text,
      vs.started_at,
      vs.submitted_at,
      vs.cancelled_at,
      vs.gm_user_id,
      concat_ws(' ', u.first_name, u.last_name) as gm_name,
      u.region as gm_region,
      vs.market_id,
      m.name as market_name,
      m.standard_market_number,
      m.coke_master_number,
      m.flex_number,
      concat_ws(', ', m.address, m.postal_code, m.city) as market_address,
      (select jsonb_agg(jsonb_build_object('section', vss.section::text, 'campaignId', c.id, 'campaign', c.name, 'questionnaire', vss.fragebogen_name_snapshot, 'status', vss.status::text) order by vss.order_index)
         from visit_session_sections vss left join campaigns c on c.id = vss.campaign_id
        where vss.visit_session_id = vs.id and vss.is_deleted = false) as sections,
      (select count(*)::int from visit_answers va where va.visit_session_id = vs.id and va.is_deleted = false and va.answer_status = 'answered') as answered_count,
      (select count(*)::int from visit_answer_photos p join visit_answers va on va.id = p.visit_answer_id where va.visit_session_id = vs.id and va.is_deleted = false and p.is_deleted = false) as photo_count,
      (select count(*)::int from visit_question_comments qc join visit_session_questions q on q.id = qc.visit_session_question_id join visit_session_sections s on s.id = q.visit_session_section_id where s.visit_session_id = vs.id and qc.is_deleted = false and q.is_deleted = false and s.is_deleted = false) as comment_count
    from visit_sessions vs
    join users u on u.id = vs.gm_user_id
    join markets m on m.id = vs.market_id
    where vs.is_deleted = false
      and (${input.gmUserId}::uuid is null or vs.gm_user_id = ${input.gmUserId}::uuid)
      and (${input.marketId}::uuid is null or vs.market_id = ${input.marketId}::uuid)
      and (${input.status}::text is null or vs.status::text = ${input.status})
      and (${input.section}::text is null or exists (select 1 from visit_session_sections f where f.visit_session_id = vs.id and f.is_deleted = false and f.section::text = ${input.section}))
      and (${from}::date is null or coalesce(vs.submitted_at, vs.started_at)::date >= ${from}::date)
      and (${to}::date is null or coalesce(vs.submitted_at, vs.started_at)::date <= ${to}::date)
    order by coalesce(vs.submitted_at, vs.started_at) desc
    limit ${input.limit}
  `;
  return envelope("search_visits", rows, { resultCount: rows.length });
}

async function getVisitContext(args: unknown): Promise<JsonRecord> {
  const { visitSessionId } = z.object({ visitSessionId: z.string().uuid() }).parse(args);
  const [sessionRows, questionRows] = await Promise.all([
    pgSql<JsonRecord[]>`
      select vs.id, vs.status::text, vs.started_at, vs.submitted_at, vs.cancelled_at,
             vs.gm_user_id, concat_ws(' ', u.first_name, u.last_name) as gm_name, u.region as gm_region,
             vs.market_id, m.name as market_name, m.db_name as chain,
             m.standard_market_number, m.coke_master_number, m.flex_number,
             m.address, m.postal_code, m.city, m.region as market_region,
             vs.kuehler_unit_id, ku.kuehler_internal_id, ku.kuehler_serial_number, ku.kuehler_model
      from visit_sessions vs
      join users u on u.id = vs.gm_user_id
      join markets m on m.id = vs.market_id
      left join market_kuehler_units ku on ku.id = vs.kuehler_unit_id
      where vs.id = ${visitSessionId} and vs.is_deleted = false
      limit 1
    `,
    pgSql<JsonRecord[]>`
      select
        s.id as section_id, s.section::text, s.status::text as section_status,
        s.campaign_id, c.name as campaign_name, s.fragebogen_id,
        s.fragebogen_name_snapshot, s.order_index as section_order,
        q.id as visit_question_id, q.question_id, q.module_id, q.module_name_snapshot,
        q.question_type::text, q.question_text_snapshot, q.required_snapshot,
        q.applies_to_market_chain_snapshot, q.red_survey_snapshot, q.order_index as question_order,
        a.id as answer_id, a.answer_status::text, a.value_text, a.value_number,
        a.value_json, a.is_valid, a.validation_error, a.answered_at, a.changed_at, a.version,
        (select jsonb_agg(o.option_value order by o.order_index) from visit_answer_options o where o.visit_answer_id = a.id and o.is_deleted = false) as selected_options,
        (select qc.comment_text from visit_question_comments qc where qc.visit_session_question_id = q.id and qc.is_deleted = false order by qc.updated_at desc limit 1) as comment,
        (select count(*)::int from visit_answer_photos p where p.visit_answer_id = a.id and p.is_deleted = false) as photo_count,
        (select coalesce(jsonb_agg(distinct pt.photo_tag_label_snapshot) filter (where pt.photo_tag_label_snapshot <> ''), '[]'::jsonb)
           from visit_answer_photos p left join visit_answer_photo_tags pt on pt.visit_answer_photo_id = p.id and pt.is_deleted = false
          where p.visit_answer_id = a.id and p.is_deleted = false) as photo_tags
      from visit_session_sections s
      left join campaigns c on c.id = s.campaign_id
      join visit_session_questions q on q.visit_session_section_id = s.id and q.is_deleted = false
      left join visit_answers a on a.visit_session_question_id = q.id and a.is_deleted = false
      where s.visit_session_id = ${visitSessionId} and s.is_deleted = false
      order by s.order_index, q.order_index
      limit 250
    `,
  ]);
  if (!sessionRows[0]) throw Object.assign(new Error("Besuch nicht gefunden."), { statusCode: 404, code: "visit_not_found" });
  return envelope("get_visit_context", { session: sessionRows[0], questionsAndAnswers: questionRows }, { visitSessionId, questionCount: questionRows.length });
}

async function getCampaignContext(args: unknown): Promise<JsonRecord> {
  const input = z.object({
    campaignId: z.string().uuid().nullable(),
    query: z.string().nullable(),
    section: z.enum(["standard", "flex", "billa", "kuehler", "mhd"]).nullable(),
    status: z.enum(["active", "scheduled", "inactive"]).nullable(),
    limit: z.number().int().min(1).max(150),
  }).parse(args);
  const query = input.query?.trim() || null;
  const campaigns = await pgSql<JsonRecord[]>`
    select c.id, c.name, c.section::text, c.status::text, c.schedule_type::text,
           c.start_date, c.end_date, c.current_fragebogen_id,
           coalesce(fm.name, fk.name, fmd.name) as questionnaire_name,
           (select count(distinct cma.market_id)::int from campaign_market_assignments cma where cma.campaign_id = c.id and cma.is_deleted = false) as assigned_markets,
           (select count(distinct cma.gm_user_id)::int from campaign_market_assignments cma where cma.campaign_id = c.id and cma.is_deleted = false and cma.gm_user_id is not null) as assigned_gms,
           (select coalesce(sum(cma.visit_target_count), 0)::int from campaign_market_assignments cma where cma.campaign_id = c.id and cma.is_deleted = false) as target_visits,
           (select count(*)::int from visit_sessions vs join visit_session_sections vss on vss.visit_session_id = vs.id and vss.is_deleted = false where vss.campaign_id = c.id and vs.is_deleted = false and vs.status = 'submitted') as submitted_visits
    from campaigns c
    left join fragebogen_main fm on fm.id = c.current_fragebogen_id and fm.is_deleted = false
    left join fragebogen_kuehler fk on fk.id = c.current_fragebogen_id and fk.is_deleted = false
    left join fragebogen_mhd fmd on fmd.id = c.current_fragebogen_id and fmd.is_deleted = false
    where c.is_deleted = false
      and (${input.campaignId}::uuid is null or c.id = ${input.campaignId}::uuid)
      and (${query}::text is null or c.name ilike '%' || ${query} || '%')
      and (${input.section}::text is null or c.section::text = ${input.section})
      and (${input.status}::text is null or c.status::text = ${input.status})
    order by c.status = 'active' desc, c.section, c.name
    limit ${input.limit}
  `;
  if (!input.campaignId) return envelope("get_campaign_context", campaigns, { resultCount: campaigns.length });
  const [assignments, latestVisits] = await Promise.all([
    pgSql<JsonRecord[]>`
      select cma.id, cma.market_id, m.name as market_name, m.standard_market_number,
             m.coke_master_number, m.flex_number, m.region, m.current_gm_name,
             cma.gm_user_id, concat_ws(' ', u.first_name, u.last_name) as assigned_gm,
             cma.assignment_slot, cma.visit_target_count, cma.current_visits_count,
             (select count(*)::int from visit_sessions vs join visit_session_sections s on s.visit_session_id = vs.id and s.is_deleted = false where vs.market_id = m.id and s.campaign_id = ${input.campaignId} and vs.status = 'submitted' and vs.is_deleted = false) as actual_submitted_visits
      from campaign_market_assignments cma
      join markets m on m.id = cma.market_id
      left join users u on u.id = cma.gm_user_id
      where cma.campaign_id = ${input.campaignId} and cma.is_deleted = false
      order by assigned_gm nulls last, m.name
      limit 300
    `,
    pgSql<JsonRecord[]>`
      select vs.id, vs.market_id, m.name as market_name, vs.gm_user_id,
             concat_ws(' ', u.first_name, u.last_name) as gm_name, vs.submitted_at
      from visit_sessions vs
      join visit_session_sections s on s.visit_session_id = vs.id and s.is_deleted = false
      join markets m on m.id = vs.market_id
      join users u on u.id = vs.gm_user_id
      where s.campaign_id = ${input.campaignId} and vs.is_deleted = false and vs.status = 'submitted'
      order by vs.submitted_at desc
      limit 50
    `,
  ]);
  return envelope("get_campaign_context", { campaign: campaigns[0] ?? null, assignments, latestVisits }, { campaignId: input.campaignId });
}

async function getTimeContext(args: unknown): Promise<JsonRecord> {
  const input = z.object({
    gmUserId: z.string().uuid().nullable(),
    from: z.string().nullable(),
    to: z.string().nullable(),
    includeLive: z.boolean(),
  }).parse(args);
  const from = parseYmd(input.from, daysAgo(30));
  const to = parseYmd(input.to, new Date());
  const sessions = await loadZeiterfassungDaySessions({
    from,
    to,
    gmUserIds: input.gmUserId ? [input.gmUserId] : [],
    includeLive: input.includeLive,
    timezone: VIENNA_TIMEZONE,
  });
  const aggregates = buildGmAggregates({ sessions, timezone: VIENNA_TIMEZONE, now: new Date() });
  return envelope("get_time_context", {
    range: { from, to, includeLive: input.includeLive, timezone: VIENNA_TIMEZONE },
    gmAggregates: aggregates,
    daySessions: sessions.slice(0, 180),
    totalDaySessions: sessions.length,
    truncated: sessions.length > 180,
  });
}

type IppPeriodMeta = {
  id: string;
  redYear: number;
  periodIndex: number;
  label: string;
  startDate: string;
  endDate: string;
  lookupEndDate: string;
};

type IppSample = {
  redPeriodId: string;
  redYear: number;
  periodIndex: number;
  redPeriodLabel: string;
  periodStart: string;
  periodEnd: string;
  marketId: string;
  marketName: string;
  standardMarketNumber: string;
  chain: string;
  marketRegion: string;
  gmUserId: string | null;
  gmName: string;
  gmRegion: string;
  marketIpp: number;
  includedInAverage: boolean;
  sourceSubmissionCount: number;
  contributingQuestionCount: number;
  isFinalized: boolean;
  source: "finalized_snapshot" | "live_calculation";
};

type IppSampleSummary = {
  weightedAverageIpp: number | null;
  sampleCount: number;
  zeroOrUnscoredSampleCount: number;
  uniqueMarkets: number;
  uniqueRedPeriods: number;
  sourceSubmissionCount: number;
  highestSample: IppSample | null;
  lowestPositiveSample: IppSample | null;
};

type IppPeriodSelectionInput = {
  timeframe: z.infer<typeof ippTimeframeSchema>;
  periodStart: string | null;
  redYear: number | null;
  fromPeriodStart: string | null;
  toPeriodStart: string | null;
};

function toIppNumber(value: unknown): number {
  const parsed = typeof value === "number" ? value : Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function roundIpp(value: number): number {
  return Number(value.toFixed(4));
}

function averageIpp(values: number[]): number | null {
  return values.length > 0 ? roundIpp(values.reduce((sum, value) => sum + value, 0) / values.length) : null;
}

function isValidYmd(value: string | null): value is string {
  return Boolean(value && YMD_REGEX.test(value));
}

async function loadIppPeriodCatalog(): Promise<IppPeriodMeta[]> {
  return pgSql<IppPeriodMeta[]>`
    select id, red_year as "redYear", period_index as "periodIndex", label,
           start_date::text as "startDate", end_date::text as "endDate", lookup_end_date::text as "lookupEndDate"
    from red_month_periods
    where is_deleted = false
    order by start_date
  `;
}

function selectIppPeriods(
  catalog: IppPeriodMeta[],
  input: IppPeriodSelectionInput,
  current: { redYear: number; startYmd: string },
): IppPeriodMeta[] {
  const available = catalog.filter((period) => period.startDate <= current.startYmd);
  if (input.timeframe === "red_month") {
    const target = isValidYmd(input.periodStart) ? input.periodStart : current.startYmd;
    const match = available.find((period) => period.startDate === target)
      ?? available.find((period) => period.startDate <= target && period.lookupEndDate >= target);
    return match ? [match] : [];
  }
  if (input.timeframe === "ytd") {
    const redYear = input.redYear ?? current.redYear;
    return available.filter((period) => period.redYear === redYear);
  }
  if (input.timeframe === "custom") {
    if (!isValidYmd(input.fromPeriodStart) || !isValidYmd(input.toPeriodStart)) {
      throw new Error("For custom IPP timeframes, fromPeriodStart and toPeriodStart must use YYYY-MM-DD.");
    }
    return available.filter((period) => period.startDate >= input.fromPeriodStart! && period.startDate <= input.toPeriodStart!);
  }
  return available.slice(-80);
}

function selectIppComparisonPeriods(input: {
  catalog: IppPeriodMeta[];
  primary: IppPeriodMeta[];
  comparison: z.infer<typeof ippGmAnalyticsSchema>["comparison"];
  compareFromPeriodStart: string | null;
  compareToPeriodStart: string | null;
}): IppPeriodMeta[] {
  if (input.comparison === "none" || input.primary.length === 0) return [];
  if (input.comparison === "custom") {
    if (!isValidYmd(input.compareFromPeriodStart) || !isValidYmd(input.compareToPeriodStart)) {
      throw new Error("For custom IPP comparisons, compareFromPeriodStart and compareToPeriodStart must use YYYY-MM-DD.");
    }
    return input.catalog.filter((period) => period.startDate >= input.compareFromPeriodStart! && period.startDate <= input.compareToPeriodStart!);
  }
  if (input.comparison === "previous_year") {
    const keys = new Set(input.primary.map((period) => `${period.redYear - 1}::${period.periodIndex}`));
    return input.catalog.filter((period) => keys.has(`${period.redYear}::${period.periodIndex}`));
  }
  const firstIndex = input.catalog.findIndex((period) => period.id === input.primary[0]?.id);
  const count = input.primary.length;
  return firstIndex > 0 ? input.catalog.slice(Math.max(0, firstIndex - count), firstIndex) : [];
}

function ippPeriodDescriptor(periods: IppPeriodMeta[], timeframe: string): JsonRecord {
  return {
    timeframe,
    periodCount: periods.length,
    fromPeriodStart: periods[0]?.startDate ?? null,
    toPeriodStart: periods.at(-1)?.startDate ?? null,
    fromDate: periods[0]?.startDate ?? null,
    toDate: periods.at(-1)?.endDate ?? null,
    redYears: Array.from(new Set(periods.map((period) => period.redYear))),
    periods: periods.map((period) => ({
      id: period.id,
      label: period.label,
      redYear: period.redYear,
      periodIndex: period.periodIndex,
      startDate: period.startDate,
      endDate: period.endDate,
    })),
  };
}

async function loadIppSamples(periods: IppPeriodMeta[]): Promise<IppSample[]> {
  if (periods.length === 0) return [];
  const periodIds = periods.map((period) => period.id);
  const periodStarts = periods.map((period) => period.startDate);
  const computed = await computeMarketIppSummariesForRedPeriods({ redPeriodIds: periodIds });
  const finalizedRows = await pgSql<Array<{
    red_period_id: string | null;
    red_period_start: string;
    market_id: string;
    market_ipp: string | number;
    source_submission_count: number;
    contributing_question_count: number;
    gm_user_id: string | null;
  }>>`
    select r.red_period_id::text, r.red_period_start::text, r.market_id::text, r.market_ipp,
           r.source_submission_count, r.contributing_question_count, latest.gm_user_id::text
    from ipp_market_redmonth_results r
    left join lateral (
      select vs.gm_user_id
      from visit_sessions vs
      where vs.market_id = r.market_id
        and vs.is_deleted = false
        and vs.status = 'submitted'
        and vs.submitted_at is not null
        and vs.submitted_at >= (r.red_period_start::timestamp at time zone 'Europe/Vienna')
        and vs.submitted_at < ((r.red_period_end + 1)::timestamp at time zone 'Europe/Vienna')
      order by vs.submitted_at desc
      limit 1
    ) latest on true
    where r.is_deleted = false
      and r.is_finalized = true
      and r.red_period_start::text = any(${periodStarts}::text[])
  `;

  const periodById = new Map(periods.map((period) => [period.id, period]));
  const periodByStart = new Map(periods.map((period) => [period.startDate, period]));
  const rawByKey = new Map<string, {
    period: IppPeriodMeta;
    marketId: string;
    gmUserId: string | null;
    marketIpp: number;
    sourceSubmissionCount: number;
    contributingQuestionCount: number;
    isFinalized: boolean;
  }>();
  for (const summary of computed.values()) {
    const period = periodById.get(summary.redPeriodId);
    if (!period) continue;
    rawByKey.set(`${period.startDate}::${summary.marketId}`, {
      period,
      marketId: summary.marketId,
      gmUserId: summary.latestGmUserId,
      marketIpp: summary.marketIpp,
      sourceSubmissionCount: summary.sourceSubmissionCount,
      contributingQuestionCount: summary.contributingQuestionCount,
      isFinalized: false,
    });
  }
  for (const row of finalizedRows) {
    const period = (row.red_period_id ? periodById.get(row.red_period_id) : undefined) ?? periodByStart.get(row.red_period_start);
    if (!period) continue;
    rawByKey.set(`${period.startDate}::${row.market_id}`, {
      period,
      marketId: row.market_id,
      gmUserId: row.gm_user_id,
      marketIpp: toIppNumber(row.market_ipp),
      sourceSubmissionCount: Number(row.source_submission_count ?? 0),
      contributingQuestionCount: Number(row.contributing_question_count ?? 0),
      isFinalized: true,
    });
  }

  const rawSamples = Array.from(rawByKey.values());
  const marketIds = Array.from(new Set(rawSamples.map((sample) => sample.marketId)));
  const gmIds = Array.from(new Set(rawSamples.map((sample) => sample.gmUserId).filter((id): id is string => Boolean(id))));
  const [markets, gms] = await Promise.all([
    marketIds.length > 0 ? pgSql<JsonRecord[]>`
      select id, name, standard_market_number, db_name as chain, region
      from markets where id = any(${marketIds}::uuid[]) and is_deleted = false
    ` : Promise.resolve([]),
    gmIds.length > 0 ? pgSql<JsonRecord[]>`
      select id, concat_ws(' ', first_name, last_name) as name, region
      from users where id = any(${gmIds}::uuid[]) and deleted_at is null
    ` : Promise.resolve([]),
  ]);
  const marketById = new Map(markets.map((row) => [String(row.id), row]));
  const gmById = new Map(gms.map((row) => [String(row.id), row]));
  return rawSamples.map((sample) => {
    const market = marketById.get(sample.marketId);
    const gm = sample.gmUserId ? gmById.get(sample.gmUserId) : undefined;
    return {
      redPeriodId: sample.period.id,
      redYear: sample.period.redYear,
      periodIndex: sample.period.periodIndex,
      redPeriodLabel: sample.period.label,
      periodStart: sample.period.startDate,
      periodEnd: sample.period.endDate,
      marketId: sample.marketId,
      marketName: String(market?.name ?? ""),
      standardMarketNumber: String(market?.standard_market_number ?? ""),
      chain: String(market?.chain ?? ""),
      marketRegion: String(market?.region ?? ""),
      gmUserId: sample.gmUserId,
      gmName: String(gm?.name ?? "Nicht zugeordnet"),
      gmRegion: String(gm?.region ?? ""),
      marketIpp: roundIpp(sample.marketIpp),
      includedInAverage: sample.marketIpp > 0,
      sourceSubmissionCount: sample.sourceSubmissionCount,
      contributingQuestionCount: sample.contributingQuestionCount,
      isFinalized: sample.isFinalized,
      source: sample.isFinalized ? "finalized_snapshot" : "live_calculation",
    } satisfies IppSample;
  });
}

function summarizeIppSamples(samples: IppSample[]): IppSampleSummary {
  const positive = samples.filter((sample) => sample.includedInAverage);
  const sorted = [...positive].sort((left, right) => right.marketIpp - left.marketIpp);
  return {
    weightedAverageIpp: averageIpp(positive.map((sample) => sample.marketIpp)),
    sampleCount: positive.length,
    zeroOrUnscoredSampleCount: samples.length - positive.length,
    uniqueMarkets: new Set(samples.map((sample) => sample.marketId)).size,
    uniqueRedPeriods: new Set(samples.map((sample) => sample.redPeriodId)).size,
    sourceSubmissionCount: samples.reduce((sum, sample) => sum + sample.sourceSubmissionCount, 0),
    highestSample: sorted[0] ?? null,
    lowestPositiveSample: sorted.at(-1) ?? null,
  };
}

function buildIppPeriodSeries(samples: IppSample[]): JsonRecord[] {
  const grouped = new Map<string, IppSample[]>();
  for (const sample of samples) {
    const bucket = grouped.get(sample.redPeriodId) ?? [];
    bucket.push(sample);
    grouped.set(sample.redPeriodId, bucket);
  }
  const series = Array.from(grouped.values()).map((rows) => {
    const first = rows[0]!;
    const summary = summarizeIppSamples(rows);
    return {
      redPeriodId: first.redPeriodId,
      redPeriodLabel: first.redPeriodLabel,
      redYear: first.redYear,
      periodIndex: first.periodIndex,
      periodStart: first.periodStart,
      periodEnd: first.periodEnd,
      ...summary,
    };
  }).sort((left, right) => String(left.periodStart).localeCompare(String(right.periodStart)));
  return series.map((row, index) => {
    const previous = series[index - 1];
    const currentAverage = typeof row.weightedAverageIpp === "number" ? row.weightedAverageIpp : null;
    const previousAverage = previous && typeof previous.weightedAverageIpp === "number" ? previous.weightedAverageIpp : null;
    const change = currentAverage !== null && previousAverage !== null ? roundIpp(currentAverage - previousAverage) : null;
    return {
      ...row,
      changeVsPreviousRedMonth: change,
      changePercentVsPreviousRedMonth: change !== null && previousAverage ? roundIpp((change / previousAverage) * 100) : null,
    };
  });
}

function buildIppMarketBreakdown(samples: IppSample[], limit: number): JsonRecord[] {
  const grouped = new Map<string, IppSample[]>();
  for (const sample of samples) {
    const bucket = grouped.get(sample.marketId) ?? [];
    bucket.push(sample);
    grouped.set(sample.marketId, bucket);
  }
  return Array.from(grouped.values()).map((rows) => ({
    marketId: rows[0]?.marketId,
    marketName: rows[0]?.marketName,
    standardMarketNumber: rows[0]?.standardMarketNumber,
    chain: rows[0]?.chain,
    region: rows[0]?.marketRegion,
    ...summarizeIppSamples(rows),
  })).sort((left, right) => toIppNumber(right.weightedAverageIpp) - toIppNumber(left.weightedAverageIpp)).slice(0, limit);
}

async function getIppGmAnalytics(args: unknown): Promise<JsonRecord> {
  const input = ippGmAnalyticsSchema.parse(args);
  const currentPeriod = await resolveCurrentRedPeriod(new Date());
  const currentRange = redMonthPeriodToYmd(currentPeriod);
  const catalog = await loadIppPeriodCatalog();
  const primaryPeriods = selectIppPeriods(catalog, input, { redYear: currentPeriod.redYear, startYmd: currentRange.startYmd });
  const comparisonPeriods = selectIppComparisonPeriods({
    catalog,
    primary: primaryPeriods,
    comparison: input.comparison,
    compareFromPeriodStart: input.compareFromPeriodStart,
    compareToPeriodStart: input.compareToPeriodStart,
  });
  const periodIds = new Set([...primaryPeriods, ...comparisonPeriods].map((period) => period.id));
  const allSamples = await loadIppSamples(catalog.filter((period) => periodIds.has(period.id)));
  const region = input.region?.trim().toLocaleLowerCase("de") || null;
  const filtered = allSamples.filter((sample) =>
    (!input.gmUserId || sample.gmUserId === input.gmUserId)
    && (!region || sample.gmRegion.toLocaleLowerCase("de") === region));
  const primaryIds = new Set(primaryPeriods.map((period) => period.id));
  const comparisonIds = new Set(comparisonPeriods.map((period) => period.id));
  const primarySamples = filtered.filter((sample) => primaryIds.has(sample.redPeriodId) && sample.gmUserId);
  const comparisonSamples = filtered.filter((sample) => comparisonIds.has(sample.redPeriodId) && sample.gmUserId);

  const primaryByGm = new Map<string, IppSample[]>();
  const comparisonByGm = new Map<string, IppSample[]>();
  for (const sample of primarySamples) {
    const bucket = primaryByGm.get(sample.gmUserId!) ?? [];
    bucket.push(sample);
    primaryByGm.set(sample.gmUserId!, bucket);
  }
  for (const sample of comparisonSamples) {
    const bucket = comparisonByGm.get(sample.gmUserId!) ?? [];
    bucket.push(sample);
    comparisonByGm.set(sample.gmUserId!, bucket);
  }

  const gmRows = Array.from(primaryByGm.entries()).map(([gmUserId, samples]) => {
    const comparison = comparisonByGm.get(gmUserId) ?? [];
    const summary = summarizeIppSamples(samples);
    const comparisonSummary = summarizeIppSamples(comparison);
    const periodSeries = buildIppPeriodSeries(samples);
    const periodAverages = periodSeries.map((row) => row.weightedAverageIpp).filter((value): value is number => typeof value === "number");
    const primaryAverage = typeof summary.weightedAverageIpp === "number" ? summary.weightedAverageIpp : null;
    const comparisonAverage = typeof comparisonSummary.weightedAverageIpp === "number" ? comparisonSummary.weightedAverageIpp : null;
    const comparisonChange = primaryAverage !== null && comparisonAverage !== null ? roundIpp(primaryAverage - comparisonAverage) : null;
    return {
      gmUserId,
      gmName: samples[0]?.gmName ?? "",
      region: samples[0]?.gmRegion ?? "",
      ...summary,
      averageOfRedMonthAverages: averageIpp(periodAverages),
      redMonthSeries: periodSeries,
      comparison: input.comparison === "none" ? null : {
        ...comparisonSummary,
        absoluteChange: comparisonChange,
        percentChange: comparisonChange !== null && comparisonAverage ? roundIpp((comparisonChange / comparisonAverage) * 100) : null,
      },
      marketBreakdown: input.includeMarketBreakdown ? buildIppMarketBreakdown(samples, input.limit) : [],
    };
  }).sort((left, right) => toIppNumber(right.weightedAverageIpp) - toIppNumber(left.weightedAverageIpp));

  return envelope("get_ipp_gm_analytics", {
    selectedTimeframe: ippPeriodDescriptor(primaryPeriods, input.timeframe),
    comparisonTimeframe: input.comparison === "none" ? null : ippPeriodDescriptor(comparisonPeriods, input.comparison),
    overall: summarizeIppSamples(primarySamples),
    gmRanking: gmRows.slice(0, input.limit).map((row, index) => ({ rank: index + 1, ...row })),
    unassignedSampleCount: filtered.filter((sample) => primaryIds.has(sample.redPeriodId) && !sample.gmUserId).length,
  }, {
    resultCount: Math.min(gmRows.length, input.limit),
    totalMatchedGms: gmRows.length,
    averageBasis: "Positive market/RED-month IPP samples; weightedAverageIpp matches the all-time GM KPI sampling basis.",
    attributionBasis: "Each market/RED-month sample belongs to the GM of the latest submitted visit for that market in that RED month.",
  });
}

async function getIppMarketAnalytics(args: unknown): Promise<JsonRecord> {
  const input = ippMarketAnalyticsSchema.parse(args);
  const currentPeriod = await resolveCurrentRedPeriod(new Date());
  const currentRange = redMonthPeriodToYmd(currentPeriod);
  const catalog = await loadIppPeriodCatalog();
  const periods = selectIppPeriods(catalog, input, { redYear: currentPeriod.redYear, startYmd: currentRange.startYmd });
  const periodIds = new Set(periods.map((period) => period.id));
  const region = input.region?.trim().toLocaleLowerCase("de") || null;
  const chain = input.chain?.trim().toLocaleLowerCase("de") || null;
  const samples = (await loadIppSamples(periods)).filter((sample) =>
    (!input.marketId || sample.marketId === input.marketId)
    && (!input.gmUserId || sample.gmUserId === input.gmUserId)
    && (!region || sample.marketRegion.toLocaleLowerCase("de") === region)
    && (!chain || sample.chain.toLocaleLowerCase("de") === chain)
    && periodIds.has(sample.redPeriodId));

  const grouped = new Map<string, IppSample[]>();
  for (const sample of samples) {
    const bucket = grouped.get(sample.marketId) ?? [];
    bucket.push(sample);
    grouped.set(sample.marketId, bucket);
  }
  const markets = Array.from(grouped.values()).map((rows) => ({
    marketId: rows[0]?.marketId,
    marketName: rows[0]?.marketName,
    standardMarketNumber: rows[0]?.standardMarketNumber,
    chain: rows[0]?.chain,
    region: rows[0]?.marketRegion,
    latestAttributedGm: [...rows].sort((left, right) => right.periodStart.localeCompare(left.periodStart))[0]?.gmName ?? "",
    ...summarizeIppSamples(rows),
    history: input.includeHistory ? [...rows].sort((left, right) => left.periodStart.localeCompare(right.periodStart)) : [],
  }));
  const direction = input.ranking === "highest" ? -1 : 1;
  markets.sort((left, right) => direction * (toIppNumber(left.weightedAverageIpp) - toIppNumber(right.weightedAverageIpp)));
  const rankedSamples = samples.filter((sample) => sample.includedInAverage)
    .sort((left, right) => direction * (left.marketIpp - right.marketIpp))
    .slice(0, input.limit);
  return envelope("get_ipp_market_analytics", {
    selectedTimeframe: ippPeriodDescriptor(periods, input.timeframe),
    overall: summarizeIppSamples(samples),
    periodSeries: buildIppPeriodSeries(samples),
    marketRanking: markets.slice(0, input.limit).map((row, index) => ({ rank: index + 1, ...row })),
    marketRedMonthSampleRanking: rankedSamples.map((sample, index) => ({ rank: index + 1, ...sample })),
  }, {
    ranking: input.ranking,
    totalMatchedMarkets: markets.length,
    resultCount: Math.min(markets.length, input.limit),
    averageBasis: "Only positive market/RED-month IPP samples are included in averages; zero/unscored samples are counted separately.",
  });
}

async function getIppVisitAnalytics(args: unknown): Promise<JsonRecord> {
  const input = ippVisitAnalyticsSchema.parse(args);
  const currentPeriod = await resolveCurrentRedPeriod(new Date());
  const currentRange = redMonthPeriodToYmd(currentPeriod);
  const catalog = await loadIppPeriodCatalog();
  const periods = selectIppPeriods(catalog, input, { redYear: currentPeriod.redYear, startYmd: currentRange.startYmd });
  const fromDate = periods[0]?.startDate ?? currentRange.startYmd;
  const toDate = periods.at(-1)?.endDate ?? currentRange.endYmd;
  const region = input.region?.trim() || null;
  const chain = input.chain?.trim() || null;

  const visitRows = input.visitSessionId
    ? await pgSql<JsonRecord[]>`
      select vs.id, vs.status::text, vs.started_at, vs.submitted_at, vs.gm_user_id,
             concat_ws(' ', u.first_name, u.last_name) as gm_name, u.region as gm_region,
             vs.market_id, m.name as market_name, m.standard_market_number, m.db_name as chain, m.region as market_region,
             coalesce(jsonb_agg(distinct s.section::text) filter (where s.section is not null), '[]'::jsonb) as sections
      from visit_sessions vs
      join users u on u.id = vs.gm_user_id
      join markets m on m.id = vs.market_id
      left join visit_session_sections s on s.visit_session_id = vs.id and s.is_deleted = false
      where vs.id = ${input.visitSessionId}::uuid and vs.is_deleted = false
      group by vs.id, u.id, m.id
      limit 1
    `
    : await pgSql<JsonRecord[]>`
      select vs.id, vs.status::text, vs.started_at, vs.submitted_at, vs.gm_user_id,
             concat_ws(' ', u.first_name, u.last_name) as gm_name, u.region as gm_region,
             vs.market_id, m.name as market_name, m.standard_market_number, m.db_name as chain, m.region as market_region,
             coalesce(jsonb_agg(distinct s.section::text) filter (where s.section is not null), '[]'::jsonb) as sections
      from visit_sessions vs
      join users u on u.id = vs.gm_user_id
      join markets m on m.id = vs.market_id
      left join visit_session_sections s on s.visit_session_id = vs.id and s.is_deleted = false
      where vs.is_deleted = false
        and vs.status = 'submitted'
        and vs.submitted_at is not null
        and vs.submitted_at >= (${fromDate}::date::timestamp at time zone 'Europe/Vienna')
        and vs.submitted_at < (((${toDate}::date + 1)::timestamp) at time zone 'Europe/Vienna')
        and (${input.gmUserId}::uuid is null or vs.gm_user_id = ${input.gmUserId})
        and (${input.marketId}::uuid is null or vs.market_id = ${input.marketId})
        and (${region}::text is null or lower(u.region) = lower(${region}))
        and (${chain}::text is null or lower(m.db_name) = lower(${chain}))
        and (${input.section}::text is null or exists (
          select 1 from visit_session_sections section_filter
          where section_filter.visit_session_id = vs.id and section_filter.is_deleted = false and section_filter.section::text = ${input.section}
        ))
      group by vs.id, u.id, m.id
      order by vs.submitted_at desc
      limit 1500
    `;

  const visitIds = visitRows.map((row) => String(row.id));
  const computed = await computeVisitIppSummaries({ visitSessionIds: visitIds });
  const enriched = visitRows.map((row) => {
    const score = computed.get(String(row.id));
    return {
      visitSessionId: row.id,
      status: row.status,
      startedAt: row.started_at,
      submittedAt: row.submitted_at,
      gmUserId: row.gm_user_id,
      gmName: row.gm_name,
      gmRegion: row.gm_region,
      marketId: row.market_id,
      marketName: row.market_name,
      standardMarketNumber: row.standard_market_number,
      chain: row.chain,
      marketRegion: row.market_region,
      sections: row.sections,
      visitIpp: score?.visitIpp ?? 0,
      contributingQuestionCount: score?.contributingQuestionCount ?? 0,
      includedInAverage: (score?.visitIpp ?? 0) > 0,
    };
  });
  const positive = enriched.filter((row) => row.includedInAverage);
  const direction = input.ranking === "highest" ? -1 : 1;
  const ranked = [...positive].sort((left, right) => direction * (left.visitIpp - right.visitIpp));
  const detailTargetId = input.includeQuestionBreakdown
    ? String(input.visitSessionId ?? ranked[0]?.visitSessionId ?? "")
    : "";
  const detail = detailTargetId ? await computeVisitIpp({ visitSessionId: detailTargetId }) : null;
  return envelope("get_ipp_visit_analytics", {
    selectedTimeframe: ippPeriodDescriptor(periods, input.timeframe),
    summary: {
      averageVisitIpp: averageIpp(positive.map((row) => row.visitIpp)),
      submittedVisitCount: enriched.filter((row) => row.status === "submitted").length,
      positiveScoredVisitCount: positive.length,
      zeroOrUnscoredVisitCount: enriched.length - positive.length,
      highestVisit: [...positive].sort((left, right) => right.visitIpp - left.visitIpp)[0] ?? null,
      lowestPositiveVisit: [...positive].sort((left, right) => left.visitIpp - right.visitIpp)[0] ?? null,
    },
    visitRanking: ranked.slice(0, input.limit).map((row, index) => ({ rank: index + 1, ...row })),
    selectedVisit: input.visitSessionId ? enriched[0] ?? null : null,
    questionBreakdown: detail?.questionRows ?? [],
  }, {
    ranking: input.ranking,
    resultCount: Math.min(ranked.length, input.limit),
    candidateVisitCount: enriched.length,
    candidateLimit: 1500,
    scoreBasis: "Analytical single-visit IPP using the latest answer per question inside that visit and the current question_scoring configuration.",
    officialKpiWarning: "A visit IPP is not the authoritative market/RED-month KPI. Official IPP deduplicates the latest answer per question across the whole market and RED month; finalized snapshots take precedence.",
  });
}

async function getIppContext(args: unknown): Promise<JsonRecord> {
  const input = z.object({
    marketId: z.string().uuid().nullable(),
    gmUserId: z.string().uuid().nullable(),
    periodStart: z.string().nullable(),
    limit: z.number().int().min(1).max(150),
  }).parse(args);
  if (input.gmUserId && !input.marketId) {
    return envelope("get_ipp_context", await readGmKpiCache(input.gmUserId), { gmUserId: input.gmUserId, basis: "finalized all-time GM KPI cache" });
  }
  const period = input.periodStart && YMD_REGEX.test(input.periodStart)
    ? await resolveRedPeriodForDate(new Date(`${input.periodStart}T12:00:00.000Z`))
    : await resolveCurrentRedPeriod(new Date());
  const range = redMonthPeriodToYmd(period);
  if (input.marketId) {
    const [market] = await pgSql<JsonRecord[]>`select id, name, db_name as chain, standard_market_number, region, current_gm_name from markets where id = ${input.marketId} and is_deleted = false limit 1`;
    const computation = await computeMarketIppForPeriod({ marketId: input.marketId, periodStart: period.start, periodEnd: period.end });
    return envelope("get_ipp_context", { market: market ?? null, redPeriod: { label: period.label, ...range }, computation }, { marketId: input.marketId });
  }
  const summaries = Array.from((await computeMarketIppSummariesForPeriod({ periodStart: period.start, periodEnd: period.end })).values())
    .sort((a, b) => b.marketIpp - a.marketIpp)
    .slice(0, input.limit);
  const ids = summaries.map((row) => row.marketId);
  const markets = ids.length > 0
    ? await pgSql<JsonRecord[]>`select id, name, db_name as chain, standard_market_number, region, current_gm_name from markets where id = any(${ids}::uuid[])`
    : [];
  const marketById = new Map(markets.map((row) => [String(row.id), row]));
  return envelope("get_ipp_context", summaries.map((row) => ({ ...row, market: marketById.get(row.marketId) ?? null })), { redPeriod: { label: period.label, ...range }, resultCount: summaries.length });
}

async function getBonusContext(args: unknown): Promise<JsonRecord> {
  const input = z.object({ waveId: z.string().uuid().nullable(), gmUserId: z.string().uuid().nullable(), limit: z.number().int().min(1).max(150) }).parse(args);
  const waves = await pgSql<JsonRecord[]>`
    select w.*,
           (select count(*)::int from praemien_wave_thresholds t where t.wave_id = w.id and t.is_deleted = false) as threshold_count,
           (select count(*)::int from praemien_wave_pillars p where p.wave_id = w.id and p.is_deleted = false) as pillar_count,
           (select count(*)::int from praemien_wave_sources s where s.wave_id = w.id and s.is_deleted = false) as source_count,
           (select count(*)::int from praemien_gm_wave_totals gt where gt.wave_id = w.id) as gm_total_count
    from praemien_waves w
    where w.is_deleted = false and (${input.waveId}::uuid is null or w.id = ${input.waveId}::uuid)
    order by w.year desc, w.quarter desc
    limit ${input.limit}
  `;
  if (!input.waveId && !input.gmUserId) return envelope("get_bonus_context", waves, { resultCount: waves.length });
  const [thresholds, pillars, sources, totals, quality, flex] = await Promise.all([
    pgSql<JsonRecord[]>`select * from praemien_wave_thresholds where is_deleted = false and (${input.waveId}::uuid is null or wave_id = ${input.waveId}::uuid) order by wave_id, order_index`,
    pgSql<JsonRecord[]>`select * from praemien_wave_pillars where is_deleted = false and (${input.waveId}::uuid is null or wave_id = ${input.waveId}::uuid) order by wave_id, order_index`,
    pgSql<JsonRecord[]>`select id, wave_id, pillar_id, section_type::text, fragebogen_id, fragebogen_name, module_id, module_name, question_id, question_text, score_key, display_label, is_factor_mode, boni_value, distribution_freq_rule::text from praemien_wave_sources where is_deleted = false and (${input.waveId}::uuid is null or wave_id = ${input.waveId}::uuid) order by wave_id, display_label limit 300`,
    pgSql<JsonRecord[]>`
      select gt.*, concat_ws(' ', u.first_name, u.last_name) as gm_name, u.region
      from praemien_gm_wave_totals gt join users u on u.id = gt.gm_user_id
      where (${input.waveId}::uuid is null or gt.wave_id = ${input.waveId}::uuid)
        and (${input.gmUserId}::uuid is null or gt.gm_user_id = ${input.gmUserId}::uuid)
      order by gt.current_reward_eur desc, gt.total_points desc
      limit ${input.limit}
    `,
    pgSql<JsonRecord[]>`
      select q.*, concat_ws(' ', u.first_name, u.last_name) as gm_name
      from praemien_wave_quality_scores q join users u on u.id = q.gm_user_id
      where q.is_deleted = false and (${input.waveId}::uuid is null or q.wave_id = ${input.waveId}::uuid) and (${input.gmUserId}::uuid is null or q.gm_user_id = ${input.gmUserId}::uuid)
      limit ${input.limit}
    `,
    pgSql<JsonRecord[]>`
      select f.*, concat_ws(' ', u.first_name, u.last_name) as gm_name
      from praemien_wave_flex_scores f join users u on u.id = f.gm_user_id
      where f.is_deleted = false and (${input.waveId}::uuid is null or f.wave_id = ${input.waveId}::uuid) and (${input.gmUserId}::uuid is null or f.gm_user_id = ${input.gmUserId}::uuid)
      limit ${input.limit}
    `,
  ]);
  return envelope("get_bonus_context", { waves, thresholds, pillars, sources, gmTotals: totals, qualityScores: quality, flexScores: flex });
}

async function getAdminModuleCatalog(): Promise<JsonRecord> {
  const [counts] = await pgSql<JsonRecord[]>`
    select
      (select count(*)::int from fragebogen_main where is_deleted = false) as main_questionnaires,
      (select count(*)::int from fragebogen_kuehler where is_deleted = false) as cooler_questionnaires,
      (select count(*)::int from fragebogen_mhd where is_deleted = false) as mhd_questionnaires,
      (select count(*)::int from module_main where is_deleted = false) as main_modules,
      (select count(*)::int from module_kuehler where is_deleted = false) as cooler_modules,
      (select count(*)::int from module_mhd where is_deleted = false) as mhd_modules,
      (select count(*)::int from question_bank_shared where is_deleted = false) as shared_questions,
      (select count(*)::int from fragebogen_main_spezial_items where is_deleted = false) as special_questionnaire_items,
      (select count(*)::int from question_rules where is_deleted = false) as normalized_question_rules,
      (select count(*)::int from question_scoring where is_deleted = false) as scoring_rows,
      (select count(*)::int from module_question_chains where is_deleted = false) as module_chain_filters,
      (select count(*)::int from campaigns where is_deleted = false) as campaigns,
      (select count(*)::int from campaign_market_assignments where is_deleted = false) as campaign_assignments,
      (select count(*)::int from visit_sessions where is_deleted = false) as visit_sessions,
      (select count(*)::int from visit_answers where is_deleted = false) as visit_answers,
      (select count(*)::int from visit_answer_photos where is_deleted = false) as photo_records,
      (select count(*)::int from gm_day_sessions where is_deleted = false) as day_sessions,
      (select count(*)::int from markets where is_deleted = false) as markets,
      (select count(*)::int from users where deleted_at is null) as users,
      (select count(*)::int from lager where is_deleted = false) as warehouses,
      (select count(*)::int from praemien_waves where is_deleted = false) as bonus_waves,
      (select count(*)::int from ipp_market_redmonth_results where is_deleted = false) as finalized_ipp_results
  `;
  return envelope("get_admin_module_catalog", {
    modules: [
      { page: "/admin/gm-dashboard", label: "GM Dashboard", tools: ["get_admin_overview", "search_gms", "get_gm_context"] },
      { page: "/admin/ipp-berechnung", label: "IPP Berechnung", tools: ["get_ipp_context", "get_ipp_gm_analytics", "get_ipp_market_analytics", "get_ipp_visit_analytics", "get_evaluation_context", "get_question_context"] },
      { page: "/admin/praemien", label: "Prämien", tools: ["get_bonus_context", "get_evaluation_context", "get_question_context"] },
      { page: "/admin/fragebogen", label: "Standard-Fragebögen", tools: ["get_questionnaire_context", "get_module_context", "get_question_context", "get_filter_context"] },
      { page: "/admin/flexbesuche", label: "Flex-Fragebögen", tools: ["get_questionnaire_context", "get_module_context", "get_question_context", "get_filter_context"] },
      { page: "/admin/billa", label: "Billa-Fragebögen", tools: ["get_questionnaire_context", "get_module_context", "get_question_context", "get_filter_context"] },
      { page: "/admin/kuehlerinventur", label: "Kühlerinventur", tools: ["get_questionnaire_context", "get_module_context", "get_question_context", "get_inventory_context"] },
      { page: "/admin/mhd", label: "MHD", tools: ["get_questionnaire_context", "get_module_context", "get_question_context"] },
      { page: "/admin/fbmanagement", label: "Kampagnenmanagement", tools: ["get_campaign_context", "get_filter_context", "search_visits", "get_evaluation_context"] },
      { page: "/admin/fotoarchiv", label: "Fotoarchiv", tools: ["search_photo_archive", "get_visit_context"] },
      { page: "/admin/zeiterfassung", label: "Zeiterfassung", tools: ["get_time_context", "get_pending_requests", "get_audit_history_context"] },
      { page: "/admin/maerkte", label: "Märkte", tools: ["search_markets", "get_market_context", "get_filter_context"] },
      { page: "/admin/lager", label: "Lager", tools: ["get_inventory_context"] },
      { page: "/admin/gebietsmanager", label: "Gebietsmanager", tools: ["search_gms", "get_gm_context", "get_user_access_context", "get_filter_context", "get_gm_kurti_chat_history"] },
      { page: "/admin/shelfmerchandiser", label: "Shelf Merchandiser", tools: ["get_user_access_context"] },
      { page: "/admin/datenschutzanfragen", label: "Datenschutzanfragen", tools: ["get_pending_requests", "get_audit_history_context", "get_user_access_context"] },
    ],
    questionnaireDataCoverage: [
      "questionnaires and questionnaire-module ordering",
      "all main/cooler/MHD modules and module-question ordering",
      "shared questions and main-questionnaire special items",
      "legacy JSON configuration/rules/scoring and normalized rules/targets/scoring",
      "matrices, attachment metadata, photo tags, question/module chain filters",
      "campaign and assignment usage",
      "visit question snapshots, answers, options, matrix cells, comments, photo metadata/tags",
      "answer change requests and answer audit events",
    ],
    liveRecordCounts: counts ?? {},
  });
}

async function loadQuestionDetails(input: z.infer<typeof questionContextSchema>): Promise<{ questions: JsonRecord[]; specialItems: JsonRecord[] }> {
  const query = input.query?.trim() || null;
  const questionType = input.questionType?.trim() || null;
  const questions = await pgSql<JsonRecord[]>`
    select
      q.id,
      q.question_type::text,
      q.text,
      q.required,
      q.red_survey,
      q.single_choice_availability,
      q.single_choice_availability_type,
      q.chains,
      q.config,
      q.rules as legacy_rules,
      q.scoring as legacy_scoring,
      q.created_at,
      q.updated_at,
      coalesce((
        select jsonb_agg(jsonb_build_object(
          'id', r.id,
          'operator', r.operator,
          'triggerQuestionId', r.trigger_question_id,
          'triggerQuestionText', tq.text,
          'triggerValue', r.trigger_value,
          'triggerValueMax', r.trigger_value_max,
          'action', r.action::text,
          'orderIndex', r.order_index,
          'targets', coalesce((
            select jsonb_agg(jsonb_build_object(
              'questionId', rt.target_question_id,
              'questionText', target_q.text,
              'orderIndex', rt.order_index
            ) order by rt.order_index)
            from question_rule_targets rt
            join question_bank_shared target_q on target_q.id = rt.target_question_id
            where rt.rule_id = r.id and rt.is_deleted = false
          ), '[]'::jsonb)
        ) order by r.order_index)
        from question_rules r
        left join question_bank_shared tq on tq.id = r.trigger_question_id
        where r.question_id = q.id and r.is_deleted = false
      ), '[]'::jsonb) as normalized_rules,
      coalesce((
        select jsonb_agg(jsonb_build_object(
          'id', s.id,
          'scoreKey', s.score_key,
          'ipp', s.ipp,
          'zweitplatzierung', s.zweitplatzierung,
          'mitbewerberabfrage', s.mitbewerberabfrage,
          'boni', s.boni
        ) order by s.score_key)
        from question_scoring s
        where s.question_id = q.id and s.is_deleted = false
      ), '[]'::jsonb) as scoring_rows,
      (select jsonb_build_object('subtype', mx.matrix_subtype, 'rows', mx.rows, 'columns', mx.columns)
         from question_matrix mx where mx.question_id = q.id and mx.is_deleted = false) as matrix,
      coalesce((
        select jsonb_agg(jsonb_build_object(
          'id', a.id,
          'orderIndex', a.order_index,
          'payload', case when length(a.payload) <= 4000 then a.payload else left(a.payload, 4000) || '…' end,
          'payloadLength', length(a.payload),
          'truncated', length(a.payload) > 4000
        ) order by a.order_index)
        from question_attachments a
        where a.question_id = q.id and a.is_deleted = false
      ), '[]'::jsonb) as attachments,
      coalesce((
        select jsonb_agg(jsonb_build_object('id', pt.id, 'label', pt.label) order by pt.label)
        from question_photo_tags qpt
        join photo_tags pt on pt.id = qpt.photo_tag_id and pt.is_deleted = false
        where qpt.question_id = q.id and qpt.is_deleted = false
      ), '[]'::jsonb) as photo_tags,
      coalesce((
        select jsonb_agg(jsonb_build_object(
          'scope', membership.scope,
          'questionnaireId', membership.questionnaire_id,
          'questionnaireName', membership.questionnaire_name,
          'moduleId', membership.module_id,
          'moduleName', membership.module_name,
          'membershipKind', membership.membership_kind,
          'orderIndex', membership.order_index
        ) order by membership.scope, membership.questionnaire_name, membership.order_index)
        from (
          select 'main'::text as scope, fm.fragebogen_id as questionnaire_id, f.name as questionnaire_name,
                 mmq.module_id, mm.name as module_name, 'module'::text as membership_kind, mmq.order_index
          from module_main_question mmq
          join module_main mm on mm.id = mmq.module_id and mm.is_deleted = false
          join fragebogen_main_module fm on fm.module_id = mmq.module_id and fm.is_deleted = false
          join fragebogen_main f on f.id = fm.fragebogen_id and f.is_deleted = false
          where mmq.question_id = q.id and mmq.is_deleted = false
          union all
          select 'main'::text, fsq.fragebogen_id, f.name, null::uuid, null::text, 'special_question'::text, fsq.order_index
          from fragebogen_main_spezial_question fsq
          join fragebogen_main f on f.id = fsq.fragebogen_id and f.is_deleted = false
          where fsq.question_id = q.id and fsq.is_deleted = false
          union all
          select 'kuehler'::text, fm.fragebogen_id, f.name, mq.module_id, mm.name, 'module'::text, mq.order_index
          from module_kuehler_question mq
          join module_kuehler mm on mm.id = mq.module_id and mm.is_deleted = false
          join fragebogen_kuehler_module fm on fm.module_id = mq.module_id and fm.is_deleted = false
          join fragebogen_kuehler f on f.id = fm.fragebogen_id and f.is_deleted = false
          where mq.question_id = q.id and mq.is_deleted = false
          union all
          select 'mhd'::text, fm.fragebogen_id, f.name, mq.module_id, mm.name, 'module'::text, mq.order_index
          from module_mhd_question mq
          join module_mhd mm on mm.id = mq.module_id and mm.is_deleted = false
          join fragebogen_mhd_module fm on fm.module_id = mq.module_id and fm.is_deleted = false
          join fragebogen_mhd f on f.id = fm.fragebogen_id and f.is_deleted = false
          where mq.question_id = q.id and mq.is_deleted = false
        ) membership
      ), '[]'::jsonb) as questionnaire_memberships,
      coalesce((
        select jsonb_agg(jsonb_build_object(
          'scope', c.scope::text,
          'moduleId', c.module_id,
          'chain', c.chain_db_name
        ) order by c.scope, c.chain_db_name)
        from module_question_chains c
        where c.question_id = q.id and c.is_deleted = false
      ), '[]'::jsonb) as module_chain_filters,
      case when ${input.includeUsage}::boolean then jsonb_build_object(
        'visitQuestionSnapshots', (select count(*)::int from visit_session_questions vq where vq.question_id = q.id and vq.is_deleted = false),
        'answers', (select count(*)::int from visit_answers va where va.question_id = q.id and va.is_deleted = false),
        'submittedVisits', (select count(distinct va.visit_session_id)::int from visit_answers va join visit_sessions vs on vs.id = va.visit_session_id where va.question_id = q.id and va.is_deleted = false and vs.is_deleted = false and vs.status = 'submitted'),
        'answerChangeRequests', (select count(*)::int from visit_answer_change_requests cr where cr.question_id = q.id and cr.is_deleted = false),
        'campaigns', coalesce((
          select jsonb_agg(distinct jsonb_build_object('id', c.id, 'name', c.name, 'section', c.section::text, 'status', c.status::text))
          from campaigns c
          where c.is_deleted = false and c.current_fragebogen_id in (
            select fm.fragebogen_id from module_main_question mq join fragebogen_main_module fm on fm.module_id = mq.module_id and fm.is_deleted = false where mq.question_id = q.id and mq.is_deleted = false
            union select fsq.fragebogen_id from fragebogen_main_spezial_question fsq where fsq.question_id = q.id and fsq.is_deleted = false
            union select fm.fragebogen_id from module_kuehler_question mq join fragebogen_kuehler_module fm on fm.module_id = mq.module_id and fm.is_deleted = false where mq.question_id = q.id and mq.is_deleted = false
            union select fm.fragebogen_id from module_mhd_question mq join fragebogen_mhd_module fm on fm.module_id = mq.module_id and fm.is_deleted = false where mq.question_id = q.id and mq.is_deleted = false
          )
        ), '[]'::jsonb)
      ) else null end as usage
    from question_bank_shared q
    where q.is_deleted = false
      and (${input.questionId}::uuid is null or q.id = ${input.questionId})
      and (${questionType}::text is null or q.question_type::text = ${questionType})
      and (${query}::text is null or concat_ws(' ', q.text, q.question_type::text, q.config::text, q.rules::text, q.scoring::text, array_to_string(q.chains, ' ')) ilike '%' || ${query} || '%')
      and (${input.moduleId}::uuid is null or exists (
        select 1 from module_main_question x where x.question_id = q.id and x.module_id = ${input.moduleId} and x.is_deleted = false
        union all select 1 from module_kuehler_question x where x.question_id = q.id and x.module_id = ${input.moduleId} and x.is_deleted = false
        union all select 1 from module_mhd_question x where x.question_id = q.id and x.module_id = ${input.moduleId} and x.is_deleted = false
      ))
      and (${input.questionnaireId}::uuid is null or exists (
        select 1 from module_main_question mq join fragebogen_main_module fm on fm.module_id = mq.module_id and fm.is_deleted = false where mq.question_id = q.id and mq.is_deleted = false and fm.fragebogen_id = ${input.questionnaireId}
        union all select 1 from fragebogen_main_spezial_question x where x.question_id = q.id and x.fragebogen_id = ${input.questionnaireId} and x.is_deleted = false
        union all select 1 from module_kuehler_question mq join fragebogen_kuehler_module fm on fm.module_id = mq.module_id and fm.is_deleted = false where mq.question_id = q.id and mq.is_deleted = false and fm.fragebogen_id = ${input.questionnaireId}
        union all select 1 from module_mhd_question mq join fragebogen_mhd_module fm on fm.module_id = mq.module_id and fm.is_deleted = false where mq.question_id = q.id and mq.is_deleted = false and fm.fragebogen_id = ${input.questionnaireId}
      ))
      and (${input.scope}::text is null or
        (${input.scope}::text = 'main' and (exists (select 1 from module_main_question x where x.question_id = q.id and x.is_deleted = false) or exists (select 1 from fragebogen_main_spezial_question x where x.question_id = q.id and x.is_deleted = false))) or
        (${input.scope}::text = 'kuehler' and exists (select 1 from module_kuehler_question x where x.question_id = q.id and x.is_deleted = false)) or
        (${input.scope}::text = 'mhd' and exists (select 1 from module_mhd_question x where x.question_id = q.id and x.is_deleted = false))
      )
    order by q.updated_at desc, q.text
    limit ${input.limit}
  `;

  const specialItems = (input.scope && input.scope !== "main") || input.moduleId
    ? []
    : await pgSql<JsonRecord[]>`
      select si.id, 'main'::text as scope, si.fragebogen_id, f.name as questionnaire_name,
             si.question_type::text, si.text, si.required, si.chains, si.config,
             si.rules, si.scoring, si.order_index, si.created_at, si.updated_at
      from fragebogen_main_spezial_items si
      join fragebogen_main f on f.id = si.fragebogen_id and f.is_deleted = false
      where si.is_deleted = false
        and (${input.questionId}::uuid is null or si.id = ${input.questionId})
        and (${input.questionnaireId}::uuid is null or si.fragebogen_id = ${input.questionnaireId})
        and (${questionType}::text is null or si.question_type::text = ${questionType})
        and (${query}::text is null or concat_ws(' ', si.text, si.question_type::text, si.config::text, si.rules::text, si.scoring::text, array_to_string(si.chains, ' '), f.name) ilike '%' || ${query} || '%')
      order by f.name, si.order_index
      limit ${input.limit}
    `;
  return { questions, specialItems };
}

async function getModuleContext(args: unknown): Promise<JsonRecord> {
  const input = moduleContextSchema.parse(args);
  const query = input.query?.trim() || null;
  const modules = await pgSql<JsonRecord[]>`
    select * from (
      select 'main'::text as scope, m.id, m.name, m.description, m.section_keywords::text as sections, m.created_at, m.updated_at,
             (select count(*)::int from module_main_question mq where mq.module_id = m.id and mq.is_deleted = false) as question_count,
             coalesce((select jsonb_agg(jsonb_build_object('id', f.id, 'name', f.name, 'sections', f.section_keywords::text, 'status', f.status::text, 'orderIndex', fm.order_index) order by f.name)
                       from fragebogen_main_module fm join fragebogen_main f on f.id = fm.fragebogen_id and f.is_deleted = false where fm.module_id = m.id and fm.is_deleted = false), '[]'::jsonb) as questionnaires,
             (select count(*)::int from visit_session_questions vq where vq.module_id = m.id and vq.is_deleted = false) as visit_question_snapshots
      from module_main m where m.is_deleted = false
      union all
      select 'kuehler'::text, m.id, m.name, m.description, 'kuehler'::text, m.created_at, m.updated_at,
             (select count(*)::int from module_kuehler_question mq where mq.module_id = m.id and mq.is_deleted = false),
             coalesce((select jsonb_agg(jsonb_build_object('id', f.id, 'name', f.name, 'status', f.status::text, 'orderIndex', fm.order_index) order by f.name)
                       from fragebogen_kuehler_module fm join fragebogen_kuehler f on f.id = fm.fragebogen_id and f.is_deleted = false where fm.module_id = m.id and fm.is_deleted = false), '[]'::jsonb),
             (select count(*)::int from visit_session_questions vq where vq.module_id = m.id and vq.is_deleted = false)
      from module_kuehler m where m.is_deleted = false
      union all
      select 'mhd'::text, m.id, m.name, m.description, 'mhd'::text, m.created_at, m.updated_at,
             (select count(*)::int from module_mhd_question mq where mq.module_id = m.id and mq.is_deleted = false),
             coalesce((select jsonb_agg(jsonb_build_object('id', f.id, 'name', f.name, 'status', f.status::text, 'orderIndex', fm.order_index) order by f.name)
                       from fragebogen_mhd_module fm join fragebogen_mhd f on f.id = fm.fragebogen_id and f.is_deleted = false where fm.module_id = m.id and fm.is_deleted = false), '[]'::jsonb),
             (select count(*)::int from visit_session_questions vq where vq.module_id = m.id and vq.is_deleted = false)
      from module_mhd m where m.is_deleted = false
    ) module
    where (${input.scope}::text is null or module.scope = ${input.scope})
      and (${input.moduleId}::uuid is null or module.id = ${input.moduleId})
      and (${query}::text is null or concat_ws(' ', module.name, module.description, module.sections) ilike '%' || ${query} || '%')
    order by module.scope, module.name
    limit ${input.limit}
  `;
  const details = input.includeQuestions || input.moduleId
    ? await loadQuestionDetails({
        questionId: null,
        moduleId: input.moduleId,
        questionnaireId: null,
        scope: input.scope,
        query,
        questionType: null,
        includeUsage: true,
        limit: input.limit,
      })
    : { questions: [], specialItems: [] };
  return envelope("get_module_context", { modules, ...details }, { resultCount: modules.length });
}

async function getQuestionContext(args: unknown): Promise<JsonRecord> {
  const input = questionContextSchema.parse(args);
  const data = await loadQuestionDetails(input);
  return envelope("get_question_context", data, {
    questionCount: data.questions.length,
    specialItemCount: data.specialItems.length,
  });
}

async function getEvaluationContext(args: unknown): Promise<JsonRecord> {
  const input = evaluationContextSchema.parse(args);
  const from = input.from && YMD_REGEX.test(input.from) ? input.from : null;
  const to = input.to && YMD_REGEX.test(input.to) ? input.to : null;
  const summaries = await pgSql<JsonRecord[]>`
    select
      q.question_id,
      max(q.question_text_snapshot) as question_text,
      q.question_type::text,
      count(*)::int as answer_rows,
      count(distinct a.visit_session_id)::int as visit_count,
      count(distinct vs.gm_user_id)::int as gm_count,
      count(distinct vs.market_id)::int as market_count,
      (count(*) filter (where a.answer_status = 'answered'))::int as answered_count,
      (count(*) filter (where a.answer_status = 'unanswered'))::int as unanswered_count,
      (count(*) filter (where a.answer_status = 'hidden_by_rule'))::int as hidden_by_rule_count,
      (count(*) filter (where a.answer_status = 'skipped'))::int as skipped_count,
      (count(*) filter (where a.is_valid = false))::int as invalid_count,
      avg(a.value_number) filter (where a.value_number is not null) as average_numeric_value,
      min(a.answered_at) as first_answered_at,
      max(a.answered_at) as latest_answered_at,
      coalesce(jsonb_agg(distinct s.section::text), '[]'::jsonb) as sections,
      coalesce((select jsonb_agg(jsonb_build_object(
        'scoreKey', scoring.score_key,
        'ipp', scoring.ipp,
        'zweitplatzierung', scoring.zweitplatzierung,
        'mitbewerberabfrage', scoring.mitbewerberabfrage,
        'boni', scoring.boni
      ) order by scoring.score_key) from question_scoring scoring where scoring.question_id = q.question_id and scoring.is_deleted = false), '[]'::jsonb) as current_scoring_configuration
    from visit_answers a
    join visit_session_questions q on q.id = a.visit_session_question_id and q.is_deleted = false
    join visit_session_sections s on s.id = a.visit_session_section_id and s.is_deleted = false
    join visit_sessions vs on vs.id = a.visit_session_id and vs.is_deleted = false
    where a.is_deleted = false
      and (${input.questionId}::uuid is null or q.question_id = ${input.questionId})
      and (${input.moduleId}::uuid is null or q.module_id = ${input.moduleId})
      and (${input.questionnaireId}::uuid is null or s.fragebogen_id = ${input.questionnaireId})
      and (${input.campaignId}::uuid is null or s.campaign_id = ${input.campaignId})
      and (${input.gmUserId}::uuid is null or vs.gm_user_id = ${input.gmUserId})
      and (${input.marketId}::uuid is null or vs.market_id = ${input.marketId})
      and (${input.section}::text is null or s.section::text = ${input.section})
      and (${input.visitStatus} = 'all' or vs.status::text = ${input.visitStatus})
      and (${input.includeInvalid}::boolean = true or a.is_valid = true)
      and (${from}::date is null or coalesce(vs.submitted_at, vs.started_at)::date >= ${from}::date)
      and (${to}::date is null or coalesce(vs.submitted_at, vs.started_at)::date <= ${to}::date)
    group by q.question_id, q.question_type
    order by answer_rows desc, latest_answered_at desc nulls last
    limit ${input.limit}
  `;

  const details = input.includeDetails ? await pgSql<JsonRecord[]>`
    select
      a.id as answer_id,
      a.visit_session_id,
      a.answer_status::text,
      a.question_type::text,
      a.value_text,
      a.value_number,
      a.value_json,
      a.is_valid,
      a.validation_error,
      a.answered_at,
      a.changed_at,
      a.version,
      q.id as visit_question_id,
      q.question_id,
      q.module_id,
      q.module_name_snapshot,
      q.question_text_snapshot,
      q.question_config_snapshot,
      q.question_rules_snapshot,
      q.question_chains_snapshot,
      q.applies_to_market_chain_snapshot,
      q.required_snapshot,
      q.red_survey_snapshot,
      q.single_choice_availability_snapshot,
      q.single_choice_availability_type_snapshot,
      s.id as visit_section_id,
      s.section::text,
      s.status::text as section_status,
      s.campaign_id,
      c.name as campaign_name,
      s.fragebogen_id,
      s.fragebogen_name_snapshot,
      vs.status::text as visit_status,
      vs.started_at,
      vs.submitted_at,
      vs.gm_user_id,
      concat_ws(' ', u.first_name, u.last_name) as gm_name,
      u.region as gm_region,
      vs.market_id,
      m.name as market_name,
      m.standard_market_number,
      m.coke_master_number,
      m.flex_number,
      m.db_name as market_chain,
      coalesce((select jsonb_agg(jsonb_build_object('role', o.option_role::text, 'value', o.option_value, 'orderIndex', o.order_index) order by o.order_index)
                from visit_answer_options o where o.visit_answer_id = a.id and o.is_deleted = false), '[]'::jsonb) as selected_options,
      coalesce((select jsonb_agg(jsonb_build_object('row', cell.row_key, 'column', cell.column_key, 'text', cell.cell_value_text, 'date', cell.cell_value_date, 'selected', cell.cell_selected, 'orderIndex', cell.order_index) order by cell.order_index)
                from visit_answer_matrix_cells cell where cell.visit_answer_id = a.id and cell.is_deleted = false), '[]'::jsonb) as matrix_cells,
      (select comment.comment_text from visit_question_comments comment where comment.visit_session_question_id = q.id and comment.is_deleted = false order by comment.updated_at desc limit 1) as comment,
      jsonb_build_object(
        'count', (select count(*)::int from visit_answer_photos p where p.visit_answer_id = a.id and p.is_deleted = false),
        'tags', coalesce((select jsonb_agg(distinct tag.photo_tag_label_snapshot) filter (where tag.photo_tag_label_snapshot <> '')
                          from visit_answer_photos p join visit_answer_photo_tags tag on tag.visit_answer_photo_id = p.id and tag.is_deleted = false
                          where p.visit_answer_id = a.id and p.is_deleted = false), '[]'::jsonb)
      ) as photo_metadata,
      coalesce((select jsonb_agg(jsonb_build_object(
        'status', request.status::text,
        'currentAnswerSnapshot', request.current_answer_snapshot,
        'requestedAnswerPayload', request.requested_answer_payload,
        'requestedAnswerSummary', request.requested_answer_summary,
        'requestNote', request.request_note,
        'reviewedBy', concat_ws(' ', reviewer.first_name, reviewer.last_name),
        'reviewedAt', request.reviewed_at,
        'adminNote', request.admin_note,
        'createdAt', request.created_at,
        'updatedAt', request.updated_at
      ) order by request.created_at desc)
      from visit_answer_change_requests request
      left join users reviewer on reviewer.id = request.reviewed_by_user_id
      where request.visit_session_question_id = q.id and request.is_deleted = false), '[]'::jsonb) as answer_change_requests,
      coalesce((select jsonb_agg(jsonb_build_object(
        'eventType', event.event_type::text,
        'payload', event.payload,
        'actorName', concat_ws(' ', actor.first_name, actor.last_name),
        'createdAt', event.created_at
      ) order by event.created_at desc)
      from visit_answer_events event
      left join users actor on actor.id = event.actor_user_id
      where event.visit_answer_id = a.id), '[]'::jsonb) as answer_events,
      coalesce((select jsonb_agg(jsonb_build_object(
        'scoreKey', scoring.score_key,
        'ipp', scoring.ipp,
        'zweitplatzierung', scoring.zweitplatzierung,
        'mitbewerberabfrage', scoring.mitbewerberabfrage,
        'boni', scoring.boni
      ) order by scoring.score_key) from question_scoring scoring where scoring.question_id = q.question_id and scoring.is_deleted = false), '[]'::jsonb) as current_scoring_configuration
    from visit_answers a
    join visit_session_questions q on q.id = a.visit_session_question_id and q.is_deleted = false
    join visit_session_sections s on s.id = a.visit_session_section_id and s.is_deleted = false
    join visit_sessions vs on vs.id = a.visit_session_id and vs.is_deleted = false
    join users u on u.id = vs.gm_user_id
    join markets m on m.id = vs.market_id
    left join campaigns c on c.id = s.campaign_id
    where a.is_deleted = false
      and (${input.questionId}::uuid is null or q.question_id = ${input.questionId})
      and (${input.moduleId}::uuid is null or q.module_id = ${input.moduleId})
      and (${input.questionnaireId}::uuid is null or s.fragebogen_id = ${input.questionnaireId})
      and (${input.campaignId}::uuid is null or s.campaign_id = ${input.campaignId})
      and (${input.gmUserId}::uuid is null or vs.gm_user_id = ${input.gmUserId})
      and (${input.marketId}::uuid is null or vs.market_id = ${input.marketId})
      and (${input.section}::text is null or s.section::text = ${input.section})
      and (${input.visitStatus} = 'all' or vs.status::text = ${input.visitStatus})
      and (${input.includeInvalid}::boolean = true or a.is_valid = true)
      and (${from}::date is null or coalesce(vs.submitted_at, vs.started_at)::date >= ${from}::date)
      and (${to}::date is null or coalesce(vs.submitted_at, vs.started_at)::date <= ${to}::date)
    order by coalesce(vs.submitted_at, vs.started_at) desc, s.order_index, q.order_index
    limit ${input.limit}
  ` : [];

  const safeDetails = sanitizeSensitiveValue(details) as JsonRecord[];
  return envelope("get_evaluation_context", {
    filters: { ...input, from, to },
    questionSummaries: summaries,
    answerDetails: safeDetails,
  }, {
    summaryCount: summaries.length,
    detailCount: safeDetails.length,
    scoringNote: "Rows expose current scoring configuration next to historical answer snapshots. Use get_ipp_context for authoritative RED-period IPP and get_bonus_context for finalized bonus totals.",
  });
}

async function getFilterContext(args: unknown): Promise<JsonRecord> {
  const input = filterContextSchema.parse(args);
  const query = input.query?.trim() || null;
  const include = (kind: typeof input.kind) => input.kind === "all" || input.kind === kind;

  const questionRules = include("question_rules") ? await pgSql<JsonRecord[]>`
    select
      r.id,
      r.question_id,
      q.text as question_text,
      q.question_type::text,
      r.trigger_question_id,
      trigger_q.text as trigger_question_text,
      r.operator,
      r.trigger_value,
      r.trigger_value_max,
      r.action::text,
      r.order_index,
      coalesce((select jsonb_agg(jsonb_build_object('questionId', rt.target_question_id, 'questionText', target_q.text, 'orderIndex', rt.order_index) order by rt.order_index)
                from question_rule_targets rt join question_bank_shared target_q on target_q.id = rt.target_question_id
                where rt.rule_id = r.id and rt.is_deleted = false), '[]'::jsonb) as targets,
      q.rules as legacy_rules
    from question_rules r
    join question_bank_shared q on q.id = r.question_id and q.is_deleted = false
    left join question_bank_shared trigger_q on trigger_q.id = r.trigger_question_id
    where r.is_deleted = false
      and (${input.entityId}::uuid is null or r.id = ${input.entityId} or r.question_id = ${input.entityId} or r.trigger_question_id = ${input.entityId} or exists (select 1 from question_rule_targets rt where rt.rule_id = r.id and rt.target_question_id = ${input.entityId} and rt.is_deleted = false))
      and (${query}::text is null or concat_ws(' ', q.text, trigger_q.text, r.operator, r.trigger_value, r.trigger_value_max, r.action::text, q.rules::text) ilike '%' || ${query} || '%')
    order by q.text, r.order_index
    limit ${input.limit}
  ` : [];

  const chainFilters = include("chain_filters") ? await pgSql<JsonRecord[]>`
    select c.id, c.scope::text, c.module_id,
           coalesce(mm.name, mk.name, mh.name) as module_name,
           c.question_id, q.text as question_text, q.question_type::text,
           c.chain_db_name, q.chains as legacy_question_chains,
           q.single_choice_availability, q.single_choice_availability_type
    from module_question_chains c
    join question_bank_shared q on q.id = c.question_id and q.is_deleted = false
    left join module_main mm on c.scope = 'main' and mm.id = c.module_id
    left join module_kuehler mk on c.scope = 'kuehler' and mk.id = c.module_id
    left join module_mhd mh on c.scope = 'mhd' and mh.id = c.module_id
    where c.is_deleted = false
      and (${input.entityId}::uuid is null or c.id = ${input.entityId} or c.module_id = ${input.entityId} or c.question_id = ${input.entityId})
      and (${query}::text is null or concat_ws(' ', c.scope::text, coalesce(mm.name, mk.name, mh.name), q.text, c.chain_db_name, array_to_string(q.chains, ' '), q.single_choice_availability_type) ilike '%' || ${query} || '%')
    order by c.scope, module_name, q.text, c.chain_db_name
    limit ${input.limit}
  ` : [];

  const gmFilters = include("gm_filters") ? await pgSql<JsonRecord[]>`
    select sf.id, sf.gm_user_id, concat_ws(' ', u.first_name, u.last_name) as gm_name,
           u.email as gm_email, u.region, u.is_active, sf.match_value, sf.created_at, sf.updated_at
    from special_arthur_filter sf
    join users u on u.id = sf.gm_user_id
    where sf.is_deleted = false
      and (${input.entityId}::uuid is null or sf.id = ${input.entityId} or sf.gm_user_id = ${input.entityId})
      and (${query}::text is null or concat_ws(' ', u.first_name, u.last_name, u.email, u.region, sf.match_value) ilike '%' || ${query} || '%')
    order by u.last_name, u.first_name, sf.match_value
    limit ${input.limit}
  ` : [];

  const marketFilters = include("market_filters") ? await pgSql<JsonRecord[]>`
    select id, name, db_name as chain, standard_market_number, coke_master_number, flex_number,
           region, current_gm_name, universe_market, market_type::text, info_flag, is_active
    from markets
    where is_deleted = false
      and (${input.entityId}::uuid is null or id = ${input.entityId})
      and (${query}::text is null or concat_ws(' ', name, db_name, standard_market_number, coke_master_number, flex_number, region, current_gm_name, market_type::text, info_flag) ilike '%' || ${query} || '%')
    order by is_active desc, name
    limit ${input.limit}
  ` : [];
  const marketFacets = include("market_filters") ? await pgSql<JsonRecord[]>`
    select 'region'::text as facet, coalesce(region, '(leer)') as value, count(*)::int as count from markets where is_deleted = false group by region
    union all select 'chain', coalesce(db_name, '(leer)'), count(*)::int from markets where is_deleted = false group by db_name
    union all select 'market_type', market_type::text, count(*)::int from markets where is_deleted = false group by market_type
    union all select 'universe_market', universe_market::text, count(*)::int from markets where is_deleted = false group by universe_market
    union all select 'active', is_active::text, count(*)::int from markets where is_deleted = false group by is_active
    order by facet, value
  ` : [];

  const campaignFilters = include("campaign_filters") ? await pgSql<JsonRecord[]>`
    select c.id, c.name, c.section::text, c.status::text, c.schedule_type::text,
           c.start_date, c.end_date, c.current_fragebogen_id,
           coalesce(fm.name, fk.name, fmd.name) as questionnaire_name,
           (select count(*)::int from campaign_market_assignments a where a.campaign_id = c.id and a.is_deleted = false) as assignment_rows
    from campaigns c
    left join fragebogen_main fm on fm.id = c.current_fragebogen_id and fm.is_deleted = false
    left join fragebogen_kuehler fk on fk.id = c.current_fragebogen_id and fk.is_deleted = false
    left join fragebogen_mhd fmd on fmd.id = c.current_fragebogen_id and fmd.is_deleted = false
    where c.is_deleted = false
      and (${input.entityId}::uuid is null or c.id = ${input.entityId} or c.current_fragebogen_id = ${input.entityId})
      and (${query}::text is null or concat_ws(' ', c.name, c.section::text, c.status::text, c.schedule_type::text, fm.name, fk.name, fmd.name) ilike '%' || ${query} || '%')
    order by c.status = 'active' desc, c.section, c.name
    limit ${input.limit}
  ` : [];

  const questionnaireFilters = include("questionnaire_filters") ? await pgSql<JsonRecord[]>`
    select * from (
      select 'main'::text as scope, id, name, description, section_keywords::text as sections, status::text, schedule_type::text, start_date, end_date, nur_einmal_ausfuellbar, updated_at from fragebogen_main where is_deleted = false
      union all select 'kuehler', id, name, description, 'kuehler', status::text, schedule_type::text, start_date, end_date, nur_einmal_ausfuellbar, updated_at from fragebogen_kuehler where is_deleted = false
      union all select 'mhd', id, name, description, 'mhd', status::text, schedule_type::text, start_date, end_date, nur_einmal_ausfuellbar, updated_at from fragebogen_mhd where is_deleted = false
    ) questionnaire
    where (${input.entityId}::uuid is null or questionnaire.id = ${input.entityId})
      and (${query}::text is null or concat_ws(' ', questionnaire.scope, questionnaire.name, questionnaire.description, questionnaire.sections, questionnaire.status, questionnaire.schedule_type) ilike '%' || ${query} || '%')
    order by questionnaire.scope, questionnaire.name
    limit ${input.limit}
  ` : [];

  return envelope("get_filter_context", {
    kind: input.kind,
    questionRules,
    chainFilters,
    gmSpecialFilters: gmFilters,
    marketFilters,
    marketFacets,
    campaignFilters,
    questionnaireFilters,
  });
}

async function getQuestionnaireContext(args: unknown): Promise<JsonRecord> {
  const input = z.object({
    scope: z.enum(["main", "kuehler", "mhd"]).nullable(),
    questionnaireId: z.string().uuid().nullable(),
    query: z.string().nullable(),
    includeQuestions: z.boolean(),
    limit: z.number().int().min(1).max(150),
  }).parse(args);
  const query = input.query?.trim() || null;
  const questionnaires = await pgSql<JsonRecord[]>`
    select * from (
      select 'main'::text as scope, id, name, description, section_keywords::text as sections, status::text, schedule_type::text, start_date, end_date, nur_einmal_ausfuellbar, updated_at from fragebogen_main where is_deleted = false
      union all
      select 'kuehler'::text as scope, id, name, description, 'kuehler'::text as sections, status::text, schedule_type::text, start_date, end_date, nur_einmal_ausfuellbar, updated_at from fragebogen_kuehler where is_deleted = false
      union all
      select 'mhd'::text as scope, id, name, description, 'mhd'::text as sections, status::text, schedule_type::text, start_date, end_date, nur_einmal_ausfuellbar, updated_at from fragebogen_mhd where is_deleted = false
    ) q
    where (${input.scope}::text is null or q.scope = ${input.scope})
      and (${input.questionnaireId}::uuid is null or q.id = ${input.questionnaireId}::uuid)
      and (${query}::text is null or concat_ws(' ', q.name, q.description, q.sections) ilike '%' || ${query} || '%')
    order by q.scope, q.name
    limit ${input.limit}
  `;
  if (!input.questionnaireId || !input.scope) {
    const questions = input.includeQuestions
      ? await pgSql<JsonRecord[]>`select id, question_type::text, text, required, chains, config, rules, scoring, red_survey, single_choice_availability, single_choice_availability_type from question_bank_shared where is_deleted = false and (${query}::text is null or text ilike '%' || ${query} || '%') order by updated_at desc limit ${input.limit}`
      : [];
    return envelope("get_questionnaire_context", { questionnaires, matchingQuestionBankEntries: questions });
  }
  let modules: JsonRecord[] = [];
  if (input.scope === "main") {
    modules = await pgSql<JsonRecord[]>`
      select m.id, m.name, m.description, m.section_keywords::text as sections, fm.order_index,
             (select jsonb_agg(jsonb_build_object('id', q.id, 'type', q.question_type::text, 'text', q.text, 'required', q.required, 'chains', q.chains, 'config', q.config, 'rules', q.rules, 'scoring', q.scoring, 'redSurvey', q.red_survey) order by mq.order_index)
                from module_main_question mq join question_bank_shared q on q.id = mq.question_id and q.is_deleted = false
               where mq.module_id = m.id and mq.is_deleted = false) as questions
      from fragebogen_main_module fm join module_main m on m.id = fm.module_id and m.is_deleted = false
      where fm.fragebogen_id = ${input.questionnaireId} and fm.is_deleted = false
      order by fm.order_index
    `;
  } else if (input.scope === "kuehler") {
    modules = await pgSql<JsonRecord[]>`
      select m.id, m.name, m.description, fm.order_index,
             (select jsonb_agg(jsonb_build_object('id', q.id, 'type', q.question_type::text, 'text', q.text, 'required', q.required, 'chains', q.chains, 'config', q.config, 'rules', q.rules, 'scoring', q.scoring, 'redSurvey', q.red_survey) order by mq.order_index)
                from module_kuehler_question mq join question_bank_shared q on q.id = mq.question_id and q.is_deleted = false
               where mq.module_id = m.id and mq.is_deleted = false) as questions
      from fragebogen_kuehler_module fm join module_kuehler m on m.id = fm.module_id and m.is_deleted = false
      where fm.fragebogen_id = ${input.questionnaireId} and fm.is_deleted = false
      order by fm.order_index
    `;
  } else {
    modules = await pgSql<JsonRecord[]>`
      select m.id, m.name, m.description, fm.order_index,
             (select jsonb_agg(jsonb_build_object('id', q.id, 'type', q.question_type::text, 'text', q.text, 'required', q.required, 'chains', q.chains, 'config', q.config, 'rules', q.rules, 'scoring', q.scoring, 'redSurvey', q.red_survey) order by mq.order_index)
                from module_mhd_question mq join question_bank_shared q on q.id = mq.question_id and q.is_deleted = false
               where mq.module_id = m.id and mq.is_deleted = false) as questions
      from fragebogen_mhd_module fm join module_mhd m on m.id = fm.module_id and m.is_deleted = false
      where fm.fragebogen_id = ${input.questionnaireId} and fm.is_deleted = false
      order by fm.order_index
    `;
  }
  return envelope("get_questionnaire_context", { questionnaire: questionnaires[0] ?? null, modules });
}

async function searchPhotoArchive(args: unknown): Promise<JsonRecord> {
  const input = z.object({
    query: z.string().nullable(),
    gmUserId: z.string().uuid().nullable(),
    marketId: z.string().uuid().nullable(),
    tag: z.string().nullable(),
    from: z.string().nullable(),
    to: z.string().nullable(),
    limit: z.number().int().min(1).max(150),
  }).parse(args);
  const query = input.query?.trim() || null;
  const tag = input.tag?.trim() || null;
  const from = input.from && YMD_REGEX.test(input.from) ? input.from : null;
  const to = input.to && YMD_REGEX.test(input.to) ? input.to : null;
  const rows = await pgSql<JsonRecord[]>`
    select p.id as photo_id, p.uploaded_at, p.mime_type, p.byte_size, p.width_px, p.height_px,
           vs.id as visit_session_id, vs.submitted_at, vs.gm_user_id,
           concat_ws(' ', u.first_name, u.last_name) as gm_name,
           vs.market_id, m.name as market_name, m.standard_market_number as stammnummer,
           m.coke_master_number, m.flex_number, m.address, m.postal_code, m.city,
           q.question_text_snapshot as question, q.question_type::text,
           s.section::text, c.id as campaign_id, c.name as campaign_name,
           coalesce(jsonb_agg(distinct pt.photo_tag_label_snapshot) filter (where pt.photo_tag_label_snapshot <> ''), '[]'::jsonb) as tags
    from visit_answer_photos p
    join visit_answers a on a.id = p.visit_answer_id and a.is_deleted = false
    join visit_sessions vs on vs.id = a.visit_session_id and vs.is_deleted = false
    join users u on u.id = vs.gm_user_id
    join markets m on m.id = vs.market_id
    join visit_session_questions q on q.id = a.visit_session_question_id and q.is_deleted = false
    join visit_session_sections s on s.id = a.visit_session_section_id and s.is_deleted = false
    left join campaigns c on c.id = s.campaign_id
    left join visit_answer_photo_tags pt on pt.visit_answer_photo_id = p.id and pt.is_deleted = false
    where p.is_deleted = false
      and (${input.gmUserId}::uuid is null or vs.gm_user_id = ${input.gmUserId}::uuid)
      and (${input.marketId}::uuid is null or vs.market_id = ${input.marketId}::uuid)
      and (${query}::text is null or concat_ws(' ', m.name, m.standard_market_number, m.coke_master_number, m.flex_number, m.address, m.postal_code, m.city, q.question_text_snapshot, c.name, u.first_name, u.last_name) ilike '%' || ${query} || '%')
      and (${tag}::text is null or exists (select 1 from visit_answer_photo_tags f where f.visit_answer_photo_id = p.id and f.is_deleted = false and f.photo_tag_label_snapshot ilike '%' || ${tag} || '%'))
      and (${from}::date is null or p.uploaded_at::date >= ${from}::date)
      and (${to}::date is null or p.uploaded_at::date <= ${to}::date)
    group by p.id, vs.id, u.id, m.id, q.id, s.id, c.id
    order by p.uploaded_at desc
    limit ${input.limit}
  `;
  return envelope("search_photo_archive", rows, { resultCount: rows.length, note: "Storage paths and signed URLs are intentionally excluded." });
}

async function getInventoryContext(args: unknown): Promise<JsonRecord> {
  const input = z.object({ kind: z.enum(["all", "lager", "kuehler"]), gmUserId: z.string().uuid().nullable(), marketId: z.string().uuid().nullable(), query: z.string().nullable(), limit: z.number().int().min(1).max(150) }).parse(args);
  const query = input.query?.trim() || null;
  const warehouses = input.kind === "kuehler" ? [] : await pgSql<JsonRecord[]>`
    select l.id, l.address, l.postal_code, l.city,
           coalesce(jsonb_agg(distinct jsonb_build_object('gmUserId', u.id, 'gmName', concat_ws(' ', u.first_name, u.last_name), 'region', u.region)) filter (where u.id is not null), '[]'::jsonb) as assigned_gms
    from lager l
    left join lager_gm_assignments a on a.lager_id = l.id and a.is_deleted = false
    left join users u on u.id = a.gm_user_id
    where l.is_deleted = false
      and (${input.gmUserId}::uuid is null or a.gm_user_id = ${input.gmUserId}::uuid)
      and (${query}::text is null or concat_ws(' ', l.address, l.postal_code, l.city) ilike '%' || ${query} || '%')
    group by l.id
    order by l.city, l.address
    limit ${input.limit}
  `;
  const coolerUnits = input.kind === "lager" ? [] : await pgSql<JsonRecord[]>`
    select ku.id, ku.market_id, m.name as market_name, m.standard_market_number, m.kuehler_stammnr,
           m.address, m.postal_code, m.city, m.region, m.current_gm_name,
           ku.name, ku.employee, ku.kuehler_internal_id, ku.kuehler_bd,
           ku.kuehler_anzahl_ks_am_standort, ku.kuehler_serial_number, ku.kuehler_model, ku.imported_at
    from market_kuehler_units ku join markets m on m.id = ku.market_id
    where ku.is_deleted = false and m.is_deleted = false
      and (${input.marketId}::uuid is null or ku.market_id = ${input.marketId}::uuid)
      and (${input.gmUserId}::uuid is null or exists (select 1 from campaign_market_assignments cma where cma.market_id = m.id and cma.gm_user_id = ${input.gmUserId}::uuid and cma.is_deleted = false))
      and (${query}::text is null or concat_ws(' ', m.name, m.standard_market_number, m.kuehler_stammnr, m.address, m.postal_code, m.city, ku.name, ku.kuehler_internal_id, ku.kuehler_serial_number, ku.kuehler_model) ilike '%' || ${query} || '%')
    order by m.name, ku.kuehler_internal_id nulls last
    limit ${input.limit}
  `;
  return envelope("get_inventory_context", { warehouses, coolerUnits });
}

async function getPendingRequests(args: unknown): Promise<JsonRecord> {
  const input = z.object({ kind: z.enum(["all", "answer_change", "visit_delete", "time_change", "dsar"]), status: z.string().nullable(), gmUserId: z.string().uuid().nullable(), limit: z.number().int().min(1).max(150) }).parse(args);
  const answerChanges = !["all", "answer_change"].includes(input.kind) ? [] : await pgSql<JsonRecord[]>`
    select r.id, r.status::text, r.created_at, r.gm_user_id, concat_ws(' ', u.first_name, u.last_name) as gm_name,
           r.market_id, m.name as market_name, m.standard_market_number,
           r.visit_session_id, r.question_type::text, r.question_text_snapshot,
           r.current_answer_snapshot, r.requested_answer_payload, r.requested_answer_summary,
           r.request_note, r.reviewed_at, r.admin_note
    from visit_answer_change_requests r join users u on u.id = r.gm_user_id join markets m on m.id = r.market_id
    where r.is_deleted = false and (${input.status}::text is null or r.status::text = ${input.status}) and (${input.gmUserId}::uuid is null or r.gm_user_id = ${input.gmUserId}::uuid)
    order by r.created_at desc limit ${input.limit}
  `;
  const visitDeletes = !["all", "visit_delete"].includes(input.kind) ? [] : await pgSql<JsonRecord[]>`
    select r.id, r.status::text, r.created_at, r.gm_user_id, concat_ws(' ', u.first_name, u.last_name) as gm_name,
           r.market_id, r.market_name_snapshot, r.market_address_snapshot, r.market_postal_code_snapshot,
           r.market_city_snapshot, r.visit_session_id, r.campaign_summary_snapshot, r.section_summary_snapshot,
           r.session_started_at_snapshot, r.session_submitted_at_snapshot, r.request_note, r.reviewed_at, r.admin_note
    from visit_session_delete_requests r join users u on u.id = r.gm_user_id
    where r.is_deleted = false and (${input.status}::text is null or r.status::text = ${input.status}) and (${input.gmUserId}::uuid is null or r.gm_user_id = ${input.gmUserId}::uuid)
    order by r.created_at desc limit ${input.limit}
  `;
  const timeChanges = !["all", "time_change"].includes(input.kind) ? [] : await pgSql<JsonRecord[]>`
    select r.id, r.status::text, r.created_at, r.gm_user_id, concat_ws(' ', u.first_name, u.last_name) as gm_name,
           r.day_session_id, r.source_kind::text, r.source_id, r.work_date, r.timezone,
           r.title_snapshot, r.subtitle_snapshot, r.requested_activity_type::text,
           r.original_start_at, r.original_end_at, r.requested_start_at, r.requested_end_at,
           r.original_start_km, r.original_end_km, r.requested_start_km, r.requested_end_km,
           r.request_note, r.reviewed_at, r.applied_at, r.admin_note
    from time_entry_change_requests r join users u on u.id = r.gm_user_id
    where r.is_deleted = false and (${input.status}::text is null or r.status::text = ${input.status}) and (${input.gmUserId}::uuid is null or r.gm_user_id = ${input.gmUserId}::uuid)
    order by r.created_at desc limit ${input.limit}
  `;
  const dsar = !["all", "dsar"].includes(input.kind) ? [] : await pgSql<JsonRecord[]>`
    select id, request_type::text, status::text, intake_channel, requester_name, requester_email,
           subject_user_id, subject_name_snapshot, subject_email_snapshot, subject_role_snapshot,
           request_summary, received_at, due_at, extended_until, identity_verified_at,
           decision_summary, response_sent_at, created_at, updated_at
    from dsar_requests
    where is_deleted = false and (${input.status}::text is null or status::text = ${input.status})
    order by due_at, created_at desc limit ${input.limit}
  `;
  return envelope("get_pending_requests", { answerChanges, visitDeleteRequests: visitDeletes, timeChanges, dsarRequests: dsar });
}

async function getAuditHistoryContext(args: unknown): Promise<JsonRecord> {
  const input = auditHistorySchema.parse(args);
  const from = input.from && YMD_REGEX.test(input.from) ? input.from : null;
  const to = input.to && YMD_REGEX.test(input.to) ? input.to : null;
  let rows: JsonRecord[] = [];

  if (input.kind === "auth") {
    rows = await pgSql<JsonRecord[]>`
      select l.id, l.event_type, l.created_at,
             l.actor_user_id, concat_ws(' ', actor.first_name, actor.last_name) as actor_name, actor.role::text as actor_role,
             l.target_user_id, concat_ws(' ', target.first_name, target.last_name) as target_name, target.role::text as target_role
      from auth_audit_logs l
      left join users actor on actor.id = l.actor_user_id
      left join users target on target.id = l.target_user_id
      where (${input.entityId}::uuid is null or l.actor_user_id = ${input.entityId} or l.target_user_id = ${input.entityId})
        and (${input.gmUserId}::uuid is null or l.actor_user_id = ${input.gmUserId} or l.target_user_id = ${input.gmUserId})
        and (${from}::date is null or l.created_at >= (${from}::date at time zone ${VIENNA_TIMEZONE}))
        and (${to}::date is null or l.created_at < ((${to}::date + 1) at time zone ${VIENNA_TIMEZONE}))
      order by l.created_at desc
      limit ${input.limit}
    `;
  } else if (input.kind === "campaign") {
    rows = await pgSql<JsonRecord[]>`
      select * from (
        select h.id, 'questionnaire_change'::text as event_type, h.changed_at as created_at,
               h.changed_by_user_id as actor_user_id, concat_ws(' ', actor.first_name, actor.last_name) as actor_name,
               null::uuid as gm_user_id, null::text as gm_name,
               h.campaign_id as entity_id, c.name as entity_name,
               null::uuid as market_id, null::text as market_name,
               jsonb_build_object('fromQuestionnaireId', h.from_fragebogen_id, 'toQuestionnaireId', h.to_fragebogen_id) as payload
        from campaign_fragebogen_history h
        join campaigns c on c.id = h.campaign_id
        left join users actor on actor.id = h.changed_by_user_id
        where h.is_deleted = false
          and (${input.entityId}::uuid is null or h.campaign_id = ${input.entityId})
          and ${input.gmUserId}::uuid is null
          and (${from}::date is null or h.changed_at >= (${from}::date at time zone ${VIENNA_TIMEZONE}))
          and (${to}::date is null or h.changed_at < ((${to}::date + 1) at time zone ${VIENNA_TIMEZONE}))
        union all
        select h.id, 'assignment_migration'::text as event_type, h.migrated_at as created_at,
               h.migrated_by_user_id as actor_user_id, concat_ws(' ', actor.first_name, actor.last_name) as actor_name,
               h.to_gm_user_id as gm_user_id, concat_ws(' ', target_gm.first_name, target_gm.last_name) as gm_name,
               h.to_campaign_id as entity_id, target_campaign.name as entity_name,
               h.market_id, m.name as market_name,
               jsonb_build_object(
                 'section', h.section::text,
                 'fromCampaignId', h.from_campaign_id,
                 'fromCampaign', source_campaign.name,
                 'toCampaignId', h.to_campaign_id,
                 'toCampaign', target_campaign.name,
                 'fromGmUserId', h.from_gm_user_id,
                 'fromGm', concat_ws(' ', source_gm.first_name, source_gm.last_name),
                 'toGmUserId', h.to_gm_user_id,
                 'toGm', concat_ws(' ', target_gm.first_name, target_gm.last_name),
                 'reason', h.reason
               ) as payload
        from campaign_market_assignment_history h
        join campaigns source_campaign on source_campaign.id = h.from_campaign_id
        join campaigns target_campaign on target_campaign.id = h.to_campaign_id
        join markets m on m.id = h.market_id
        left join users actor on actor.id = h.migrated_by_user_id
        left join users source_gm on source_gm.id = h.from_gm_user_id
        left join users target_gm on target_gm.id = h.to_gm_user_id
        where (${input.entityId}::uuid is null or h.from_campaign_id = ${input.entityId} or h.to_campaign_id = ${input.entityId} or h.market_id = ${input.entityId})
          and (${input.gmUserId}::uuid is null or h.from_gm_user_id = ${input.gmUserId} or h.to_gm_user_id = ${input.gmUserId})
          and (${from}::date is null or h.migrated_at >= (${from}::date at time zone ${VIENNA_TIMEZONE}))
          and (${to}::date is null or h.migrated_at < ((${to}::date + 1) at time zone ${VIENNA_TIMEZONE}))
      ) history
      order by created_at desc
      limit ${input.limit}
    `;
  } else if (input.kind === "questionnaire") {
    rows = input.gmUserId ? [] : await pgSql<JsonRecord[]>`
      select h.id, h.change_kind as event_type, h.changed_at as created_at,
             h.question_id, q.text as question_text, h.fragebogen_id, f.name as questionnaire_name,
             h.spezial_item_id,
             jsonb_build_object(
               'previousQuestionType', h.previous_question_type::text,
               'nextQuestionType', h.next_question_type::text,
               'previousAnswerState', h.previous_answer_state,
               'nextAnswerState', h.next_answer_state
             ) as payload
      from question_answer_history h
      left join question_bank_shared q on q.id = h.question_id
      left join fragebogen_main f on f.id = h.fragebogen_id
      where h.is_deleted = false
        and (${input.entityId}::uuid is null or h.question_id = ${input.entityId} or h.fragebogen_id = ${input.entityId} or h.spezial_item_id = ${input.entityId})
        and (${from}::date is null or h.changed_at >= (${from}::date at time zone ${VIENNA_TIMEZONE}))
        and (${to}::date is null or h.changed_at < ((${to}::date + 1) at time zone ${VIENNA_TIMEZONE}))
      order by h.changed_at desc
      limit ${input.limit}
    `;
  } else if (input.kind === "time") {
    rows = await pgSql<JsonRecord[]>`
      select e.id, e.event_type::text, e.created_at, e.entry_id, coalesce(e.gm_user_id, t.gm_user_id) as gm_user_id,
             concat_ws(' ', u.first_name, u.last_name) as gm_name,
             t.activity_type::text, t.status::text, t.start_at, t.end_at,
             t.market_id, m.name as market_name, e.payload
      from time_tracking_entry_events e
      join time_tracking_entries t on t.id = e.entry_id
      left join users u on u.id = coalesce(e.gm_user_id, t.gm_user_id)
      left join markets m on m.id = t.market_id
      where (${input.entityId}::uuid is null or e.entry_id = ${input.entityId})
        and (${input.gmUserId}::uuid is null or coalesce(e.gm_user_id, t.gm_user_id) = ${input.gmUserId})
        and (${from}::date is null or e.created_at >= (${from}::date at time zone ${VIENNA_TIMEZONE}))
        and (${to}::date is null or e.created_at < ((${to}::date + 1) at time zone ${VIENNA_TIMEZONE}))
      order by e.created_at desc
      limit ${input.limit}
    `;
  } else if (input.kind === "visit_answer") {
    rows = await pgSql<JsonRecord[]>`
      select e.id, e.event_type::text, e.created_at, e.visit_answer_id,
             va.visit_session_id, vs.gm_user_id, concat_ws(' ', u.first_name, u.last_name) as gm_name,
             vs.market_id, m.name as market_name, m.standard_market_number,
             q.question_text_snapshot as question_text, e.actor_user_id,
             concat_ws(' ', actor.first_name, actor.last_name) as actor_name,
             e.payload
      from visit_answer_events e
      join visit_answers va on va.id = e.visit_answer_id
      join visit_sessions vs on vs.id = va.visit_session_id
      join users u on u.id = vs.gm_user_id
      join markets m on m.id = vs.market_id
      left join visit_session_questions q on q.id = va.visit_session_question_id
      left join users actor on actor.id = e.actor_user_id
      where (${input.entityId}::uuid is null or e.visit_answer_id = ${input.entityId} or va.visit_session_id = ${input.entityId})
        and (${input.gmUserId}::uuid is null or vs.gm_user_id = ${input.gmUserId})
        and (${from}::date is null or e.created_at >= (${from}::date at time zone ${VIENNA_TIMEZONE}))
        and (${to}::date is null or e.created_at < ((${to}::date + 1) at time zone ${VIENNA_TIMEZONE}))
      order by e.created_at desc
      limit ${input.limit}
    `;
  } else {
    rows = await pgSql<JsonRecord[]>`
      select e.id, e.event_type, e.message, e.created_at, e.request_id,
             r.request_type::text, r.status::text, r.subject_user_id,
             r.subject_name_snapshot, r.subject_role_snapshot,
             e.actor_user_id, concat_ws(' ', actor.first_name, actor.last_name) as actor_name,
             e.metadata as payload
      from dsar_request_events e
      join dsar_requests r on r.id = e.request_id
      left join users actor on actor.id = e.actor_user_id
      where r.is_deleted = false
        and (${input.entityId}::uuid is null or e.request_id = ${input.entityId})
        and (${input.gmUserId}::uuid is null or r.subject_user_id = ${input.gmUserId} or r.requester_user_id = ${input.gmUserId})
        and (${from}::date is null or e.created_at >= (${from}::date at time zone ${VIENNA_TIMEZONE}))
        and (${to}::date is null or e.created_at < ((${to}::date + 1) at time zone ${VIENNA_TIMEZONE}))
      order by e.created_at desc
      limit ${input.limit}
    `;
  }

  const sanitizedRows = rows.map((row) => sanitizeSensitiveValue(row) as JsonRecord);
  return envelope("get_audit_history_context", sanitizedRows, {
    kind: input.kind,
    resultCount: sanitizedRows.length,
    sanitized: true,
    note: input.kind === "auth" ? "Raw auth details are intentionally excluded." : undefined,
  });
}

async function getRedMonthContext(args: unknown): Promise<JsonRecord> {
  const input = z.object({ year: z.number().int().min(2020).max(2100).nullable() }).parse(args);
  const current = await resolveCurrentRedPeriod(new Date());
  const currentRange = redMonthPeriodToYmd(current);
  const years = await pgSql<JsonRecord[]>`
    select id, red_year, anchor_start, cycle_weeks, period_count, timezone, status::text, activated_at, created_at, updated_at
    from red_month_years
    where is_deleted = false and (${input.year}::int is null or red_year = ${input.year}::int)
    order by red_year desc
  `;
  const periods = await pgSql<JsonRecord[]>`
    select p.id, p.red_month_year_id, p.red_year, p.period_index, p.label, p.start_date, p.end_date, p.lookup_end_date,
           (select count(*)::int from visit_sessions vs where vs.is_deleted = false and vs.status = 'submitted' and vs.submitted_at::date between p.start_date and p.end_date) as submitted_visits
    from red_month_periods p
    where p.is_deleted = false and (${input.year}::int is null or p.red_year = ${input.year}::int)
    order by p.red_year desc, p.period_index
    limit 100
  `;
  return envelope("get_red_month_context", { current: { id: current.redPeriodId, label: current.label, redYear: current.redYear, ...currentRange }, years, periods });
}

const TOOL_HANDLERS: Record<string, (args: unknown) => Promise<JsonRecord>> = {
  get_admin_overview: getAdminOverview,
  get_user_access_context: getUserAccessContext,
  search_gms: searchGms,
  get_gm_context: getGmContext,
  get_gm_kurti_chat_history: getGmKurtiChatHistory,
  search_markets: searchMarkets,
  get_market_context: getMarketContext,
  search_visits: searchVisits,
  get_visit_context: getVisitContext,
  get_campaign_context: getCampaignContext,
  get_time_context: getTimeContext,
  get_ipp_context: getIppContext,
  get_ipp_gm_analytics: getIppGmAnalytics,
  get_ipp_market_analytics: getIppMarketAnalytics,
  get_ipp_visit_analytics: getIppVisitAnalytics,
  get_bonus_context: getBonusContext,
  get_admin_module_catalog: getAdminModuleCatalog,
  get_module_context: getModuleContext,
  get_question_context: getQuestionContext,
  get_evaluation_context: getEvaluationContext,
  get_filter_context: getFilterContext,
  get_questionnaire_context: getQuestionnaireContext,
  search_photo_archive: searchPhotoArchive,
  get_inventory_context: getInventoryContext,
  get_pending_requests: getPendingRequests,
  get_audit_history_context: getAuditHistoryContext,
  get_red_month_context: getRedMonthContext,
};

export async function executeAdminKurtiTool(name: string, rawArguments: string): Promise<string> {
  const handler = TOOL_HANDLERS[name];
  if (!handler) return JSON.stringify({ error: "unknown_tool", tool: name });
  try {
    const parsed = rawArguments.trim() ? JSON.parse(rawArguments) as unknown : {};
    const result = await handler(parsed);
    const serialized = JSON.stringify(result);
    if (serialized.length <= 120_000) return serialized;
    return JSON.stringify({
      tool: name,
      error: "tool_result_too_large",
      message: "The result exceeded 120,000 characters. Narrow the filters or request a smaller limit.",
      preview: serialized.slice(0, 110_000),
    });
  } catch (error) {
    const parsed = error as { message?: unknown; code?: unknown };
    return JSON.stringify({
      tool: name,
      error: typeof parsed.code === "string" ? parsed.code : "tool_execution_failed",
      message: typeof parsed.message === "string" ? parsed.message : "Tool execution failed.",
    });
  }
}
