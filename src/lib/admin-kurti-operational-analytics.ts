import type { Responses } from "openai/resources/responses/responses";
import { z } from "zod";
import { sql as pgSql } from "./db.js";

const TIMEZONE = "Europe/Vienna";
const YMD = /^\d{4}-\d{2}-\d{2}$/;
type JsonRecord = Record<string, unknown>;

function tool(name: string, description: string, properties: Record<string, unknown>, required: string[]): Responses.FunctionTool {
  return { type: "function", name, description, strict: true, parameters: { type: "object", properties, required, additionalProperties: false } };
}

const nullableUuid = { type: ["string", "null"], format: "uuid" } as const;
const nullableString = { type: ["string", "null"] } as const;
const dateString = { type: ["string", "null"], pattern: "^\\d{4}-\\d{2}-\\d{2}$" } as const;

export const ADMIN_KURTI_OPERATIONAL_ANALYTICS_TOOLS: Responses.FunctionTool[] = [
  tool("get_visit_analytics", "Returns compact visit time-series and rankings across all real visit sessions. It can group by GM, market, chain, region, status, or total and reports visit count, unique markets/GMs, answers, photos, and average completed-visit duration. Use it for visit trends, comparisons, rankings, scatter inputs, and operational dashboards. Defaults to the last 28 days.", {
    gmUserIds: { type: "array", maxItems: 24, items: { type: "string", format: "uuid" } },
    marketId: nullableUuid,
    campaignId: nullableUuid,
    section: { type: ["string", "null"], enum: ["standard", "flex", "billa", "kuehler", "mhd", null] },
    chain: nullableString,
    region: nullableString,
    status: { type: "string", enum: ["all", "draft", "submitted", "cancelled"] },
    from: dateString,
    to: dateString,
    granularity: { type: "string", enum: ["day", "week", "month"] },
    groupBy: { type: "string", enum: ["total", "gm", "market", "chain", "region", "status"] },
    limitGroups: { type: "integer", minimum: 1, maximum: 24 },
  }, ["gmUserIds", "marketId", "campaignId", "section", "chain", "region", "status", "from", "to", "granularity", "groupBy", "limitGroups"]),
  tool("get_campaign_performance_analytics", "Returns campaign-lifecycle target-versus-actual submitted visit performance. It aggregates assignment visit_target_count correctly (never assignment-row count) and real distinct submitted visits, grouped by campaign, section, market, chain, or region. Use for completion bars, funnels, KPI cards, gaps, and rankings.", {
    campaignId: nullableUuid,
    section: { type: ["string", "null"], enum: ["standard", "flex", "billa", "kuehler", "mhd", null] },
    campaignStatus: { type: ["string", "null"], enum: ["active", "scheduled", "inactive", null] },
    chain: nullableString,
    region: nullableString,
    groupBy: { type: "string", enum: ["campaign", "section", "market", "chain", "region"] },
    limit: { type: "integer", minimum: 1, maximum: 100 },
  }, ["campaignId", "section", "campaignStatus", "chain", "region", "groupBy", "limit"]),
  tool("get_answer_distribution_analytics", "Returns exact stored-answer distributions for one resolved question: answer statuses, selected-option frequencies, text-value frequencies, numeric statistics, and a daily/weekly/monthly answer-count and numeric-average series. Use get_question_context first to resolve the question ID. Historical visit question snapshots remain the basis; current scoring is not silently applied.", {
    questionId: { type: "string", format: "uuid" },
    gmUserId: nullableUuid,
    marketId: nullableUuid,
    campaignId: nullableUuid,
    section: { type: ["string", "null"], enum: ["standard", "flex", "billa", "kuehler", "mhd", null] },
    visitStatus: { type: "string", enum: ["all", "draft", "submitted", "cancelled"] },
    includeInvalid: { type: "boolean" },
    from: dateString,
    to: dateString,
    granularity: { type: "string", enum: ["day", "week", "month"] },
    limitValues: { type: "integer", minimum: 1, maximum: 40 },
  }, ["questionId", "gmUserId", "marketId", "campaignId", "section", "visitStatus", "includeInvalid", "from", "to", "granularity", "limitValues"]),
  tool("get_photo_analytics", "Returns photo archive time-series and rankings by GM, market, chain, section, campaign, tag, or total. Counts distinct photo records so multi-tag joins do not inflate non-tag totals. Tag grouping intentionally counts one photo in every assigned tag. Use for upload trends, tag mix, coverage comparisons, and heatmap/table inputs. Defaults to the last 28 days.", {
    gmUserId: nullableUuid,
    marketId: nullableUuid,
    campaignId: nullableUuid,
    section: { type: ["string", "null"], enum: ["standard", "flex", "billa", "kuehler", "mhd", null] },
    chain: nullableString,
    tag: nullableString,
    from: dateString,
    to: dateString,
    granularity: { type: "string", enum: ["day", "week", "month"] },
    groupBy: { type: "string", enum: ["total", "gm", "market", "chain", "section", "campaign", "tag"] },
    limitGroups: { type: "integer", minimum: 1, maximum: 24 },
  }, ["gmUserId", "marketId", "campaignId", "section", "chain", "tag", "from", "to", "granularity", "groupBy", "limitGroups"]),
];

function currentYmd(): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: TIMEZONE, year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date());
}

function addDays(value: string, days: number): string {
  const date = new Date(`${value}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function range(input: { from: string | null; to: string | null }): { from: string; to: string } {
  const to = input.to ?? currentYmd();
  const from = input.from ?? addDays(to, -27);
  if (!YMD.test(from) || !YMD.test(to) || from > to) throw new Error("Invalid analytics date range.");
  return { from, to };
}

function numericRow(row: JsonRecord): JsonRecord {
  return Object.fromEntries(Object.entries(row).map(([key, value]) => {
    if (typeof value === "string" && /^-?\d+(\.\d+)?$/.test(value) && /(_count|_minutes|_value|percent|visits|markets|gms|answers|photos|target|gap)$/i.test(key)) {
      return [key, Number(value)];
    }
    return [key, value];
  }));
}

const visitSchema = z.object({
  gmUserIds: z.array(z.string().uuid()).max(24), marketId: z.string().uuid().nullable(), campaignId: z.string().uuid().nullable(),
  section: z.enum(["standard", "flex", "billa", "kuehler", "mhd"]).nullable(), chain: z.string().nullable(), region: z.string().nullable(),
  status: z.enum(["all", "draft", "submitted", "cancelled"]), from: z.string().regex(YMD).nullable(), to: z.string().regex(YMD).nullable(),
  granularity: z.enum(["day", "week", "month"]), groupBy: z.enum(["total", "gm", "market", "chain", "region", "status"]), limitGroups: z.number().int().min(1).max(24),
}).strict();

async function getVisitAnalytics(raw: unknown): Promise<JsonRecord> {
  const input = visitSchema.parse(raw);
  const dates = range(input);
  const chain = input.chain?.trim() || null;
  const region = input.region?.trim() || null;
  const rows = await pgSql<JsonRecord[]>`
    with base as (
      select vs.id, coalesce(vs.submitted_at, vs.started_at) as event_at, vs.status::text as visit_status,
             vs.gm_user_id, concat_ws(' ', u.first_name, u.last_name) as gm_name, u.region as gm_region,
             vs.market_id, m.name as market_name, m.db_name as chain, m.region as market_region,
             case when vs.submitted_at is not null and vs.started_at is not null then extract(epoch from (vs.submitted_at - vs.started_at)) / 60.0 else null end as duration_minutes,
             (select count(*) from visit_answers a where a.visit_session_id = vs.id and a.is_deleted = false and a.answer_status = 'answered') as answer_count,
             (select count(*) from visit_answer_photos p join visit_answers a on a.id = p.visit_answer_id and a.is_deleted = false where a.visit_session_id = vs.id and p.is_deleted = false) as photo_count
      from visit_sessions vs join users u on u.id = vs.gm_user_id join markets m on m.id = vs.market_id
      where vs.is_deleted = false
        and (${input.gmUserIds.length === 0}::boolean or vs.gm_user_id = any(${input.gmUserIds}::uuid[]))
        and (${input.marketId}::uuid is null or vs.market_id = ${input.marketId}::uuid)
        and (${input.campaignId}::uuid is null or exists (select 1 from visit_session_sections s where s.visit_session_id = vs.id and s.is_deleted = false and s.campaign_id = ${input.campaignId}::uuid))
        and (${input.section}::text is null or exists (select 1 from visit_session_sections s where s.visit_session_id = vs.id and s.is_deleted = false and s.section::text = ${input.section}))
        and (${chain}::text is null or m.db_name ilike '%' || ${chain} || '%')
        and (${region}::text is null or coalesce(u.region, m.region, '') ilike '%' || ${region} || '%')
        and (${input.status} = 'all' or vs.status::text = ${input.status})
        and coalesce(vs.submitted_at, vs.started_at)::date between ${dates.from}::date and ${dates.to}::date
    ), grouped as (
      select
        case ${input.granularity}
          when 'day' then to_char(event_at at time zone ${TIMEZONE}, 'YYYY-MM-DD')
          when 'week' then to_char(date_trunc('week', event_at at time zone ${TIMEZONE}), 'IYYY-"W"IW')
          else to_char(date_trunc('month', event_at at time zone ${TIMEZONE}), 'YYYY-MM') end as period_key,
        case ${input.groupBy}
          when 'gm' then gm_user_id::text when 'market' then market_id::text when 'chain' then coalesce(chain, 'Ohne Kette')
          when 'region' then coalesce(gm_region, market_region, 'Ohne Region') when 'status' then visit_status else 'total' end as group_key,
        case ${input.groupBy}
          when 'gm' then gm_name when 'market' then market_name when 'chain' then coalesce(chain, 'Ohne Kette')
          when 'region' then coalesce(gm_region, market_region, 'Ohne Region') when 'status' then visit_status else 'Gesamt' end as group_label,
        count(*)::int as visit_count, count(distinct market_id)::int as unique_markets,
        count(distinct gm_user_id)::int as unique_gms, sum(answer_count)::int as answer_count,
        sum(photo_count)::int as photo_count, round(avg(duration_minutes)::numeric, 2)::float8 as average_duration_minutes
      from base group by 1, 2, 3
    ) select * from grouped order by period_key, group_label limit 4000
  `;
  const totals = new Map<string, number>();
  for (const row of rows) totals.set(String(row.group_key), (totals.get(String(row.group_key)) ?? 0) + Number(row.visit_count ?? 0));
  const allowed = new Set([...totals.entries()].sort((a, b) => b[1] - a[1]).slice(0, input.limitGroups).map(([key]) => key));
  return {
    tool: "get_visit_analytics", range: { ...dates, timezone: TIMEZONE, granularity: input.granularity }, groupBy: input.groupBy,
    rows: rows.filter((row) => allowed.has(String(row.group_key))).map(numericRow),
    groupTotals: [...totals.entries()].filter(([key]) => allowed.has(key)).map(([groupKey, visitCount]) => ({ groupKey, visitCount })),
    countingRules: "visit_count counts visit sessions once per group/period; section and campaign are filters, so multi-section sessions are not duplicated.",
  };
}

const campaignSchema = z.object({
  campaignId: z.string().uuid().nullable(), section: z.enum(["standard", "flex", "billa", "kuehler", "mhd"]).nullable(),
  campaignStatus: z.enum(["active", "scheduled", "inactive"]).nullable(), chain: z.string().nullable(), region: z.string().nullable(),
  groupBy: z.enum(["campaign", "section", "market", "chain", "region"]), limit: z.number().int().min(1).max(100),
}).strict();

async function getCampaignPerformanceAnalytics(raw: unknown): Promise<JsonRecord> {
  const input = campaignSchema.parse(raw);
  const chain = input.chain?.trim() || null;
  const region = input.region?.trim() || null;
  const rows = await pgSql<JsonRecord[]>`
    with targets as (
      select case ${input.groupBy} when 'campaign' then c.id::text when 'section' then c.section::text when 'market' then m.id::text when 'chain' then coalesce(m.db_name, 'Ohne Kette') else coalesce(m.region, 'Ohne Region') end as group_key,
             case ${input.groupBy} when 'campaign' then c.name when 'section' then c.section::text when 'market' then concat_ws(' · ', m.name, m.standard_market_number) when 'chain' then coalesce(m.db_name, 'Ohne Kette') else coalesce(m.region, 'Ohne Region') end as group_label,
             sum(cma.visit_target_count)::int as target_visits, count(distinct cma.market_id)::int as assigned_markets, count(distinct cma.gm_user_id)::int as assigned_gms
      from campaign_market_assignments cma join campaigns c on c.id = cma.campaign_id and c.is_deleted = false join markets m on m.id = cma.market_id and m.is_deleted = false
      where cma.is_deleted = false and (${input.campaignId}::uuid is null or c.id = ${input.campaignId}::uuid) and (${input.section}::text is null or c.section::text = ${input.section})
        and (${input.campaignStatus}::text is null or c.status::text = ${input.campaignStatus}) and (${chain}::text is null or m.db_name ilike '%' || ${chain} || '%') and (${region}::text is null or m.region ilike '%' || ${region} || '%')
      group by 1, 2
    ), actuals as (
      select case ${input.groupBy} when 'campaign' then c.id::text when 'section' then s.section::text when 'market' then m.id::text when 'chain' then coalesce(m.db_name, 'Ohne Kette') else coalesce(m.region, 'Ohne Region') end as group_key,
             case ${input.groupBy} when 'campaign' then c.name when 'section' then s.section::text when 'market' then concat_ws(' · ', m.name, m.standard_market_number) when 'chain' then coalesce(m.db_name, 'Ohne Kette') else coalesce(m.region, 'Ohne Region') end as group_label,
             count(distinct vs.id)::int as submitted_visits, count(distinct vs.market_id)::int as visited_markets, count(distinct vs.gm_user_id)::int as submitting_gms
      from visit_session_sections s join visit_sessions vs on vs.id = s.visit_session_id and vs.is_deleted = false and vs.status = 'submitted'
      join campaigns c on c.id = s.campaign_id and c.is_deleted = false join markets m on m.id = vs.market_id and m.is_deleted = false
      where s.is_deleted = false and (${input.campaignId}::uuid is null or c.id = ${input.campaignId}::uuid) and (${input.section}::text is null or s.section::text = ${input.section})
        and (${input.campaignStatus}::text is null or c.status::text = ${input.campaignStatus}) and (${chain}::text is null or m.db_name ilike '%' || ${chain} || '%') and (${region}::text is null or m.region ilike '%' || ${region} || '%')
      group by 1, 2
    )
    select coalesce(t.group_key, a.group_key) as group_key, coalesce(t.group_label, a.group_label) as group_label,
           coalesce(t.target_visits, 0)::int as target_visits, coalesce(a.submitted_visits, 0)::int as submitted_visits,
           coalesce(t.assigned_markets, 0)::int as assigned_markets, coalesce(a.visited_markets, 0)::int as visited_markets,
           coalesce(t.assigned_gms, 0)::int as assigned_gms, coalesce(a.submitting_gms, 0)::int as submitting_gms
    from targets t full join actuals a on a.group_key = t.group_key order by submitted_visits desc, target_visits desc limit 500
  `;
  const enriched = rows.map((row) => {
    const target = Number(row.target_visits ?? 0);
    const actual = Number(row.submitted_visits ?? 0);
    return { ...numericRow(row), gap: target - actual, completionPercent: target > 0 ? Number(((actual / target) * 100).toFixed(2)) : null };
  }).slice(0, input.limit);
  return { tool: "get_campaign_performance_analytics", groupBy: input.groupBy, rows: enriched, countingRules: "Target is the sum of visit_target_count across active assignment rows. Actual is distinct submitted visit sessions with the campaign section. Completion may exceed 100%." };
}

const answerSchema = z.object({
  questionId: z.string().uuid(), gmUserId: z.string().uuid().nullable(), marketId: z.string().uuid().nullable(), campaignId: z.string().uuid().nullable(),
  section: z.enum(["standard", "flex", "billa", "kuehler", "mhd"]).nullable(), visitStatus: z.enum(["all", "draft", "submitted", "cancelled"]),
  includeInvalid: z.boolean(), from: z.string().regex(YMD).nullable(), to: z.string().regex(YMD).nullable(), granularity: z.enum(["day", "week", "month"]), limitValues: z.number().int().min(1).max(40),
}).strict();

async function getAnswerDistributionAnalytics(raw: unknown): Promise<JsonRecord> {
  const input = answerSchema.parse(raw);
  const from = input.from;
  const to = input.to;
  const [summary, statuses, options, texts, series] = await Promise.all([
    pgSql<JsonRecord[]>`
      select max(q.question_text_snapshot) as question, max(q.question_type::text) as question_type, count(*)::int as answer_rows,
             count(distinct a.visit_session_id)::int as visit_count, count(distinct vs.gm_user_id)::int as gm_count, count(distinct vs.market_id)::int as market_count,
             round(avg(a.value_number)::numeric, 3)::float8 as average_numeric_value, min(a.value_number)::float8 as minimum_numeric_value, max(a.value_number)::float8 as maximum_numeric_value
      from visit_answers a join visit_session_questions q on q.id = a.visit_session_question_id and q.is_deleted = false join visit_session_sections s on s.id = a.visit_session_section_id and s.is_deleted = false join visit_sessions vs on vs.id = a.visit_session_id and vs.is_deleted = false
      where a.is_deleted = false and q.question_id = ${input.questionId} and (${input.gmUserId}::uuid is null or vs.gm_user_id = ${input.gmUserId}) and (${input.marketId}::uuid is null or vs.market_id = ${input.marketId})
        and (${input.campaignId}::uuid is null or s.campaign_id = ${input.campaignId}) and (${input.section}::text is null or s.section::text = ${input.section}) and (${input.visitStatus} = 'all' or vs.status::text = ${input.visitStatus})
        and (${input.includeInvalid} or a.is_valid = true) and (${from}::date is null or coalesce(vs.submitted_at, vs.started_at)::date >= ${from}::date) and (${to}::date is null or coalesce(vs.submitted_at, vs.started_at)::date <= ${to}::date)
    `,
    pgSql<JsonRecord[]>`
      select a.answer_status::text as label, count(*)::int as value from visit_answers a join visit_session_questions q on q.id = a.visit_session_question_id join visit_session_sections s on s.id = a.visit_session_section_id join visit_sessions vs on vs.id = a.visit_session_id
      where a.is_deleted = false and q.is_deleted = false and s.is_deleted = false and vs.is_deleted = false and q.question_id = ${input.questionId} and (${input.gmUserId}::uuid is null or vs.gm_user_id = ${input.gmUserId}) and (${input.marketId}::uuid is null or vs.market_id = ${input.marketId}) and (${input.campaignId}::uuid is null or s.campaign_id = ${input.campaignId}) and (${input.section}::text is null or s.section::text = ${input.section}) and (${input.visitStatus} = 'all' or vs.status::text = ${input.visitStatus}) and (${input.includeInvalid} or a.is_valid = true) and (${from}::date is null or coalesce(vs.submitted_at, vs.started_at)::date >= ${from}::date) and (${to}::date is null or coalesce(vs.submitted_at, vs.started_at)::date <= ${to}::date)
      group by a.answer_status order by value desc
    `,
    pgSql<JsonRecord[]>`
      select o.option_value as label, count(*)::int as value from visit_answer_options o join visit_answers a on a.id = o.visit_answer_id and a.is_deleted = false join visit_session_questions q on q.id = a.visit_session_question_id join visit_session_sections s on s.id = a.visit_session_section_id join visit_sessions vs on vs.id = a.visit_session_id
      where o.is_deleted = false and q.is_deleted = false and s.is_deleted = false and vs.is_deleted = false and q.question_id = ${input.questionId} and (${input.gmUserId}::uuid is null or vs.gm_user_id = ${input.gmUserId}) and (${input.marketId}::uuid is null or vs.market_id = ${input.marketId}) and (${input.campaignId}::uuid is null or s.campaign_id = ${input.campaignId}) and (${input.section}::text is null or s.section::text = ${input.section}) and (${input.visitStatus} = 'all' or vs.status::text = ${input.visitStatus}) and (${input.includeInvalid} or a.is_valid = true) and (${from}::date is null or coalesce(vs.submitted_at, vs.started_at)::date >= ${from}::date) and (${to}::date is null or coalesce(vs.submitted_at, vs.started_at)::date <= ${to}::date)
      group by o.option_value order by value desc limit ${input.limitValues}
    `,
    pgSql<JsonRecord[]>`
      select a.value_text as label, count(*)::int as value from visit_answers a join visit_session_questions q on q.id = a.visit_session_question_id join visit_session_sections s on s.id = a.visit_session_section_id join visit_sessions vs on vs.id = a.visit_session_id
      where a.is_deleted = false and q.is_deleted = false and s.is_deleted = false and vs.is_deleted = false and q.question_id = ${input.questionId} and nullif(trim(a.value_text), '') is not null and (${input.gmUserId}::uuid is null or vs.gm_user_id = ${input.gmUserId}) and (${input.marketId}::uuid is null or vs.market_id = ${input.marketId}) and (${input.campaignId}::uuid is null or s.campaign_id = ${input.campaignId}) and (${input.section}::text is null or s.section::text = ${input.section}) and (${input.visitStatus} = 'all' or vs.status::text = ${input.visitStatus}) and (${input.includeInvalid} or a.is_valid = true) and (${from}::date is null or coalesce(vs.submitted_at, vs.started_at)::date >= ${from}::date) and (${to}::date is null or coalesce(vs.submitted_at, vs.started_at)::date <= ${to}::date)
      group by a.value_text order by value desc limit ${input.limitValues}
    `,
    pgSql<JsonRecord[]>`
      select case ${input.granularity} when 'day' then to_char(coalesce(vs.submitted_at, vs.started_at) at time zone ${TIMEZONE}, 'YYYY-MM-DD') when 'week' then to_char(date_trunc('week', coalesce(vs.submitted_at, vs.started_at) at time zone ${TIMEZONE}), 'IYYY-"W"IW') else to_char(date_trunc('month', coalesce(vs.submitted_at, vs.started_at) at time zone ${TIMEZONE}), 'YYYY-MM') end as period_key,
             count(*)::int as answer_count, round(avg(a.value_number)::numeric, 3)::float8 as average_numeric_value
      from visit_answers a join visit_session_questions q on q.id = a.visit_session_question_id join visit_session_sections s on s.id = a.visit_session_section_id join visit_sessions vs on vs.id = a.visit_session_id
      where a.is_deleted = false and q.is_deleted = false and s.is_deleted = false and vs.is_deleted = false and q.question_id = ${input.questionId} and (${input.gmUserId}::uuid is null or vs.gm_user_id = ${input.gmUserId}) and (${input.marketId}::uuid is null or vs.market_id = ${input.marketId}) and (${input.campaignId}::uuid is null or s.campaign_id = ${input.campaignId}) and (${input.section}::text is null or s.section::text = ${input.section}) and (${input.visitStatus} = 'all' or vs.status::text = ${input.visitStatus}) and (${input.includeInvalid} or a.is_valid = true) and (${from}::date is null or coalesce(vs.submitted_at, vs.started_at)::date >= ${from}::date) and (${to}::date is null or coalesce(vs.submitted_at, vs.started_at)::date <= ${to}::date)
      group by 1 order by 1
    `,
  ]);
  return { tool: "get_answer_distribution_analytics", filters: input, summary: numericRow(summary[0] ?? {}), statusDistribution: statuses.map(numericRow), optionDistribution: options.map(numericRow), textDistribution: texts.map(numericRow), timeSeries: series.map(numericRow), interpretationRules: "Option counts are selections, so multi-select totals can exceed answered rows. Text values are exact stored values. Numeric average uses only non-null numeric answers." };
}

const photoSchema = z.object({
  gmUserId: z.string().uuid().nullable(), marketId: z.string().uuid().nullable(), campaignId: z.string().uuid().nullable(), section: z.enum(["standard", "flex", "billa", "kuehler", "mhd"]).nullable(),
  chain: z.string().nullable(), tag: z.string().nullable(), from: z.string().regex(YMD).nullable(), to: z.string().regex(YMD).nullable(), granularity: z.enum(["day", "week", "month"]),
  groupBy: z.enum(["total", "gm", "market", "chain", "section", "campaign", "tag"]), limitGroups: z.number().int().min(1).max(24),
}).strict();

async function getPhotoAnalytics(raw: unknown): Promise<JsonRecord> {
  const input = photoSchema.parse(raw);
  const dates = range(input);
  const chain = input.chain?.trim() || null;
  const tag = input.tag?.trim() || null;
  const rows = await pgSql<JsonRecord[]>`
    select case ${input.granularity} when 'day' then to_char(p.uploaded_at at time zone ${TIMEZONE}, 'YYYY-MM-DD') when 'week' then to_char(date_trunc('week', p.uploaded_at at time zone ${TIMEZONE}), 'IYYY-"W"IW') else to_char(date_trunc('month', p.uploaded_at at time zone ${TIMEZONE}), 'YYYY-MM') end as period_key,
           case ${input.groupBy} when 'gm' then vs.gm_user_id::text when 'market' then vs.market_id::text when 'chain' then coalesce(m.db_name, 'Ohne Kette') when 'section' then s.section::text when 'campaign' then coalesce(c.id::text, 'ohne-kampagne') when 'tag' then coalesce(nullif(pt.photo_tag_label_snapshot, ''), 'Ohne Tag') else 'total' end as group_key,
           case ${input.groupBy} when 'gm' then concat_ws(' ', u.first_name, u.last_name) when 'market' then concat_ws(' · ', m.name, m.standard_market_number) when 'chain' then coalesce(m.db_name, 'Ohne Kette') when 'section' then s.section::text when 'campaign' then coalesce(c.name, 'Ohne Kampagne') when 'tag' then coalesce(nullif(pt.photo_tag_label_snapshot, ''), 'Ohne Tag') else 'Gesamt' end as group_label,
           count(distinct p.id)::int as photo_count, count(distinct vs.id)::int as visit_count, count(distinct vs.market_id)::int as market_count, count(distinct vs.gm_user_id)::int as gm_count
    from visit_answer_photos p join visit_answers a on a.id = p.visit_answer_id and a.is_deleted = false join visit_sessions vs on vs.id = a.visit_session_id and vs.is_deleted = false
    join users u on u.id = vs.gm_user_id join markets m on m.id = vs.market_id join visit_session_sections s on s.id = a.visit_session_section_id and s.is_deleted = false
    left join campaigns c on c.id = s.campaign_id left join visit_answer_photo_tags pt on pt.visit_answer_photo_id = p.id and pt.is_deleted = false
    where p.is_deleted = false and (${input.gmUserId}::uuid is null or vs.gm_user_id = ${input.gmUserId}) and (${input.marketId}::uuid is null or vs.market_id = ${input.marketId})
      and (${input.campaignId}::uuid is null or s.campaign_id = ${input.campaignId}) and (${input.section}::text is null or s.section::text = ${input.section}) and (${chain}::text is null or m.db_name ilike '%' || ${chain} || '%')
      and (${tag}::text is null or exists (select 1 from visit_answer_photo_tags f where f.visit_answer_photo_id = p.id and f.is_deleted = false and f.photo_tag_label_snapshot ilike '%' || ${tag} || '%'))
      and p.uploaded_at::date between ${dates.from}::date and ${dates.to}::date
    group by 1, 2, 3 order by 1, 3 limit 4000
  `;
  const totals = new Map<string, number>();
  for (const row of rows) totals.set(String(row.group_key), (totals.get(String(row.group_key)) ?? 0) + Number(row.photo_count ?? 0));
  const allowed = new Set([...totals.entries()].sort((a, b) => b[1] - a[1]).slice(0, input.limitGroups).map(([key]) => key));
  return { tool: "get_photo_analytics", range: { ...dates, timezone: TIMEZONE, granularity: input.granularity }, groupBy: input.groupBy, rows: rows.filter((row) => allowed.has(String(row.group_key))).map(numericRow), countingRules: input.groupBy === "tag" ? "A photo with multiple tags contributes once to each tag." : "Distinct photo IDs prevent tag joins from inflating totals." };
}

const handlers: Record<string, (raw: unknown) => Promise<JsonRecord>> = {
  get_visit_analytics: getVisitAnalytics,
  get_campaign_performance_analytics: getCampaignPerformanceAnalytics,
  get_answer_distribution_analytics: getAnswerDistributionAnalytics,
  get_photo_analytics: getPhotoAnalytics,
};

export async function executeAdminKurtiOperationalAnalytics(name: string, raw: unknown): Promise<JsonRecord> {
  const handler = handlers[name];
  if (!handler) throw new Error(`Unknown operational analytics tool: ${name}`);
  return handler(raw);
}
