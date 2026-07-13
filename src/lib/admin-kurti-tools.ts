import type { Responses } from "openai/resources/responses/responses";
import { z } from "zod";
import { buildGmAggregates } from "./admin-zeiterfassung.js";
import { sql as pgSql } from "./db.js";
import { readGmKpiCache } from "./gm-kpi-cache.js";
import { computeMarketIppForPeriod, computeMarketIppSummariesForPeriod } from "./ipp.js";
import { buildKurtiGmContext } from "./kurti-context.js";
import { redMonthPeriodToYmd, resolveCurrentRedPeriod, resolveRedPeriodForDate } from "./red-month-periods.js";
import { loadZeiterfassungDaySessions } from "./zeiterfassung-days.js";

const UUID_SCHEMA = { type: "string", format: "uuid" } as const;
const NULLABLE_UUID_SCHEMA = { type: ["string", "null"], format: "uuid" } as const;
const NULLABLE_STRING_SCHEMA = { type: ["string", "null"] } as const;
const NULLABLE_BOOLEAN_SCHEMA = { type: ["boolean", "null"] } as const;
const LIMIT_SCHEMA = { type: "integer", minimum: 1, maximum: 150 } as const;
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
    "Returns the current Coke Spark admin overview across users, markets, campaigns, visits, photos, time tracking, review queues, and the current RED period. Use this first for broad status questions.",
    {},
    [],
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

function parseYmd(value: string | null, fallback: Date): string {
  return value && YMD_REGEX.test(value) ? value : fallback.toISOString().slice(0, 10);
}

function daysAgo(days: number): Date {
  const value = new Date();
  value.setUTCDate(value.getUTCDate() - days);
  return value;
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
        (select count(*)::int from users where role = 'gm' and is_active = true and deleted_at is null) as active_gms,
        (select count(*)::int from users where role = 'gm' and (is_active = false or deleted_at is not null)) as inactive_gms,
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
      (select count(*)::int from time_entry_change_requests r where r.gm_user_id = u.id and r.is_deleted = false and r.status = 'pending') as pending_time_changes
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
  search_gms: searchGms,
  get_gm_context: getGmContext,
  search_markets: searchMarkets,
  get_market_context: getMarketContext,
  search_visits: searchVisits,
  get_visit_context: getVisitContext,
  get_campaign_context: getCampaignContext,
  get_time_context: getTimeContext,
  get_ipp_context: getIppContext,
  get_bonus_context: getBonusContext,
  get_questionnaire_context: getQuestionnaireContext,
  search_photo_archive: searchPhotoArchive,
  get_inventory_context: getInventoryContext,
  get_pending_requests: getPendingRequests,
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
