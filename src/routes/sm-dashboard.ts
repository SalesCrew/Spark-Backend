import { aggregateSmDashboardOverview, type SmBasicActivityRow, type SmOverviewVisit } from "../sm-dashboard-overview.shared.js";
import { requireKundeAdminPermission } from "../lib/kunde-access.js";
import { sql, type SQL } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";

import { db } from "../lib/db.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import {
  aggregateSmDashboard,
  aggregateSmHomeVisits,
  SM_OOS_CATEGORY_ORDER,
  type SmDashboardOosCategory,
  type SmDashboardOosOutcome,
  type SmDashboardOosRole,
  type SmDashboardOosRow,
  type SmDashboardVisitRow,
} from "../sm-dashboard.shared.js";
import { isIsoDate, isoDateToEpochDay } from "../sm-planning.shared.js";

export const adminSmDashboardRouter = Router();
export const smHomeDashboardRouter = Router();

const VIENNA_TIMEZONE = "Europe/Vienna";
const OOS_OUTCOMES = new Set<SmDashboardOosOutcome>([
  "oos_present",
  "oos_absent",
  "resolved",
  "partially_resolved",
  "not_resolved",
  "not_applicable",
]);

const isoDateSchema = z.string().refine(isIsoDate, "Ungültiges Datum.");
const querySchema = z.object({
  from: isoDateSchema,
  to: isoDateSchema,
  region: z.string().trim().min(1).max(200).optional(),
  chain: z.string().trim().min(1).max(500).optional(),
  smUserId: z.string().uuid().optional(),
  marketId: z.string().uuid().optional(),
  SMDurcharbeitCatalogScope: z.enum(["standard", "SMDurcharbeit"]).optional(),
}).strict();

type VisitDbRow = {
  submissionId: string;
  marketId: string;
  marketName: string;
  chain: string;
  region: string;
  smUserId: string;
  smName: string;
};

type OosDbRow = VisitDbRow & {
  submissionQuestionId: string;
  questionRootId: string;
  role: string;
  category: string;
  metricConfig: unknown;
  outcome: string | null;
};

type FilterDbRow = { value: string; label: string };
type MarketContext = FilterDbRow & { region: string; chain: string; sms: FilterDbRow[] };
type FilterContextRow = Omit<MarketContext, "sms"> & { smUserId: string | null; smName: string | null };

export function currentViennaDate(now = new Date()): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: VIENNA_TIMEZONE,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(now);
  const value = Object.fromEntries(parts.filter((part) => part.type !== "literal").map((part) => [part.type, part.value]));
  return `${value.year}-${value.month}-${value.day}`;
}

export function defaultSmDashboardRange(): { from: string; to: string } {
  const to = currentViennaDate();
  return { from: `${to.slice(0, 7)}-01`, to };
}

function parseMetricConfig(value: unknown): { detectionQuestionId: string | null; partialCountsAsResolved: boolean } {
  const parsed = typeof value === "string"
    ? (() => { try { return JSON.parse(value) as unknown; } catch { return null; } })()
    : value;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { detectionQuestionId: null, partialCountsAsResolved: false };
  }
  const record = parsed as Record<string, unknown>;
  return {
    detectionQuestionId: typeof record.detectionQuestionId === "string" ? record.detectionQuestionId : null,
    partialCountsAsResolved: record.partialCountsAsResolved === true,
  };
}

function toOosRow(row: OosDbRow): SmDashboardOosRow | null {
  if (row.role !== "oos_detection" && row.role !== "oos_remediation") return null;
  if (!SM_OOS_CATEGORY_ORDER.includes(row.category as SmDashboardOosCategory)) return null;
  const config = parseMetricConfig(row.metricConfig);
  return {
    submissionId: row.submissionId,
    marketId: row.marketId,
    marketName: row.marketName,
    chain: row.chain,
    region: row.region,
    smUserId: row.smUserId,
    smName: row.smName,
    submissionQuestionId: row.submissionQuestionId,
    questionRootId: row.questionRootId,
    role: row.role as SmDashboardOosRole,
    category: row.category as SmDashboardOosCategory,
    detectionQuestionRootId: config.detectionQuestionId,
    outcome: row.outcome && OOS_OUTCOMES.has(row.outcome as SmDashboardOosOutcome)
      ? row.outcome as SmDashboardOosOutcome
      : null,
    partialCountsAsResolved: config.partialCountsAsResolved,
  };
}

function scopeConditions(input: z.infer<typeof querySchema>): SQL[] {
  const conditions: SQL[] = [
    sql`s.status = 'submitted'`,
    sql`s.is_current = true`,
    sql`s.is_deleted = false`,
    sql`s.reporting_available_at is not null`,
    sql`s.submitted_at >= (${input.from}::date::timestamp at time zone ${VIENNA_TIMEZONE})`,
    sql`s.submitted_at < (((${input.to}::date + 1)::timestamp) at time zone ${VIENNA_TIMEZONE})`,
  ];
  if (input.region) conditions.push(sql`m.region = ${input.region}`);
  if (input.chain) conditions.push(sql`m.chain = ${input.chain}`);
  if (input.smUserId) conditions.push(sql`s.sm_user_id = ${input.smUserId}::uuid`);
  if (input.marketId) conditions.push(sql`s.sm_market_id = ${input.marketId}::uuid`);
  if (input.SMDurcharbeitCatalogScope) conditions.push(sql`(case when starts_with(t.stable_code, 'smdurcharbeit_') then 'SMDurcharbeit' else 'standard' end) = ${input.SMDurcharbeitCatalogScope}`);
  if (input.SMDurcharbeitCatalogScope === "standard") conditions.push(standardMarketCondition());
  return conditions;
}

function whereClause(conditions: SQL[]): SQL {
  return sql.join(conditions, sql` and `);
}

export async function loadSmDashboardRows(input: z.infer<typeof querySchema>, executor: Pick<typeof db, "execute"> = db, includeOverview = false) {
  const scopedWhere = whereClause(scopeConditions(input));
  const [visitResult, oosResult, basicResult] = await Promise.all([
    executor.execute(sql<VisitDbRow>`
      select
        s.id::text as "submissionId",
        s.sm_market_id::text as "marketId",
        s.market_name_snapshot as "marketName",
        m.chain as "chain",
        m.region as "region",
        s.sm_user_id::text as "smUserId",
        s.sm_name_snapshot as "smName",
        s.answered_question_count as "SMDurcharbeitAnsweredQuestionCount",
        ${includeOverview ? sql`coalesce(da_time.actual_minutes, ze.actual_minutes)` : sql`null`} as "actualMinutes",
        case when starts_with(t.stable_code, 'smdurcharbeit_') then 'SMDurcharbeit' else 'standard' end as "SMDurcharbeitCatalogScope"
      from sm_questionnaire_submissions s
      ${includeOverview ? sql`left join sm_assignment_time_submissions ze on ze.assignment_id = s.assignment_id and ze.is_current = true and ze.is_deleted = false
      left join sm_smdurcharbeit_visit_time_revisions da_time on da_time.visit_id = s.smdurcharbeit_visit_id and da_time.is_current = true` : sql``}
      inner join sm_markets m on m.id = s.sm_market_id
      left join sm_questionnaire_templates t on t.id = s.questionnaire_template_id
      where ${scopedWhere}
      order by s.submitted_at, s.id
    `),
    executor.execute(sql<OosDbRow>`
      select
        s.id::text as "submissionId",
        s.sm_market_id::text as "marketId",
        s.market_name_snapshot as "marketName",
        m.chain as "chain",
        m.region as "region",
        s.sm_user_id::text as "smUserId",
        s.sm_name_snapshot as "smName",
        q.id::text as "submissionQuestionId",
        qv.question_id::text as "questionRootId",
        q.metric_role_snapshot::text as "role",
        q.oos_category_snapshot::text as "category",
        q.metric_config_snapshot as "metricConfig",
        ao.metric_outcome_code_snapshot as "outcome"
      from sm_questionnaire_submissions s
      inner join sm_markets m on m.id = s.sm_market_id
      left join sm_questionnaire_templates t on t.id = s.questionnaire_template_id
      inner join sm_questionnaire_submission_questions q
        on q.submission_id = s.id
        and q.is_deleted = false
        and q.is_applicable = true
        and q.metric_role_snapshot in ('oos_detection', 'oos_remediation')
      inner join sm_question_versions qv on qv.id = q.question_version_id
      left join sm_question_answers a
        on a.submission_question_id = q.id
        and a.submission_id = s.id
        and a.is_current = true
        and a.is_deleted = false
        and a.answer_state = 'answered'
      left join sm_question_answer_options ao
        on ao.answer_id = a.id
        and ao.is_deleted = false
      where ${scopedWhere}
      order by s.submitted_at, q.order_index, ao.order_index
    `),
    includeOverview ? executor.execute(sql<SmBasicActivityRow>`
      select s.id::text as "submissionId", q.id::text as "submissionQuestionId", a.metric_outcome_code as "metricOutcomeCode"
      from sm_questionnaire_submissions s
      inner join sm_markets m on m.id = s.sm_market_id
      left join sm_questionnaire_templates t on t.id = s.questionnaire_template_id
      inner join sm_questionnaire_submission_questions q on q.submission_id = s.id and q.is_deleted = false and q.is_applicable = true
      inner join sm_question_answers a on a.submission_question_id = q.id and a.submission_id = s.id
        and a.is_current = true and a.is_deleted = false and a.answer_state = 'answered'
        and a.metric_outcome_code in ('basic_activity_yes', 'basic_activity_partial', 'basic_activity_no')
      where ${scopedWhere}
    `) : Promise.resolve([]),
  ]);

  const visits = [...visitResult] as SmOverviewVisit[];
  const oosRows = ([...oosResult] as OosDbRow[]).flatMap((row) => {
    const mapped = toOosRow(row);
    return mapped ? [mapped] : [];
  });

  return { visits, oosRows, overview: includeOverview ? aggregateSmDashboardOverview(visits, oosRows, [...basicResult] as SmBasicActivityRow[]) : undefined };
}

/** Registry membership alone must not hide a market with a genuine standard history or plan. */
function standardMarketCondition(): SQL {
  return sql`(
    not exists (select 1 from sm_smdurcharbeit_markets d where d.sm_market_id = m.id)
    or m.service_days_per_week > 0
    or exists (
      select 1 from sm_assignments a
      left join sm_questionnaire_versions v on v.id = coalesce(a.questionnaire_version_id, a.smdurcharbeit_questionnaire_override_version_id)
      left join sm_questionnaire_templates t on t.id = v.questionnaire_template_id
      where (a.original_sm_market_id = m.id or a.replacement_sm_market_id = m.id)
        and a.is_deleted = false
        and not coalesce(starts_with(t.stable_code, 'smdurcharbeit_'), false)
    )
    or exists (
      select 1 from sm_questionnaire_submissions history
      left join sm_questionnaire_templates t on t.id = history.questionnaire_template_id
      where history.sm_market_id = m.id and history.is_deleted = false
        and not coalesce(starts_with(t.stable_code, 'smdurcharbeit_'), false)
    )
  )`;
}

/** Directory choices include unvisited markets; SM links also preserve period-specific historical visitors. */
export async function loadSmDashboardFilterOptions(input: z.infer<typeof querySchema>, executor: Pick<typeof db, "execute"> = db) {
  const scope = input.SMDurcharbeitCatalogScope ?? "standard";
  const marketCondition = scope === "standard" ? standardMarketCondition() : sql`true`;
  const contextsResult = await executor.execute(sql<FilterContextRow>`
    with eligible_markets as (
      select m.id, m.name, m.postal_code, m.city, m.region, m.chain, m.assigned_sm_user_id
      from sm_markets m where m.is_deleted = false and ${marketCondition}
    ), people as (
      select m.id as market_id, u.id as sm_user_id,
        btrim(concat_ws(' ', u.first_name, u.last_name)) as sm_name, 0 as priority
      from eligible_markets m inner join users u on u.id = m.assigned_sm_user_id
      where u.role = 'sm' and u.deleted_at is null
      union all
      select coalesce(a.replacement_sm_market_id, a.original_sm_market_id), u.id,
        btrim(concat_ws(' ', u.first_name, u.last_name)), 1
      from sm_assignments a
      inner join eligible_markets m on m.id = coalesce(a.replacement_sm_market_id, a.original_sm_market_id)
      inner join users u on u.id = coalesce(a.replacement_sm_user_id, a.original_sm_user_id)
      left join sm_questionnaire_versions v on v.id = coalesce(a.questionnaire_version_id, a.smdurcharbeit_questionnaire_override_version_id)
      left join sm_questionnaire_templates t on t.id = v.questionnaire_template_id
      where a.is_deleted = false and a.status <> 'cancelled' and u.role = 'sm' and u.deleted_at is null
        and coalesce(a.replacement_work_date, a.original_work_date) between ${input.from}::date and ${input.to}::date
        and (case when starts_with(t.stable_code, 'smdurcharbeit_') then 'SMDurcharbeit' else 'standard' end) = ${scope}
      union all
      select s.sm_market_id, s.sm_user_id, s.sm_name_snapshot, 2
      from sm_questionnaire_submissions s
      inner join eligible_markets m on m.id = s.sm_market_id
      left join sm_questionnaire_templates t on t.id = s.questionnaire_template_id
      where s.status = 'submitted' and s.is_current = true and s.is_deleted = false and s.reporting_available_at is not null
        and s.submitted_at >= (${input.from}::date::timestamp at time zone ${VIENNA_TIMEZONE})
        and s.submitted_at < (((${input.to}::date + 1)::timestamp) at time zone ${VIENNA_TIMEZONE})
        and (case when starts_with(t.stable_code, 'smdurcharbeit_') then 'SMDurcharbeit' else 'standard' end) = ${scope}
    )
    select m.id::text as value,
      concat_ws(' · ', m.name, nullif(concat_ws(' ', m.postal_code, m.city), '')) as label,
      m.region, m.chain, p.sm_user_id::text as "smUserId", p.sm_name as "smName"
    from eligible_markets m left join people p on p.market_id = m.id
    order by m.chain, m.name, m.postal_code, m.id, p.priority, p.sm_name, p.sm_user_id
  `);
  const byMarket = new Map<string, MarketContext>();
  for (const row of contextsResult as unknown as FilterContextRow[]) {
    let market = byMarket.get(row.value);
    if (!market) { market = { value: row.value, label: row.label, region: row.region, chain: row.chain, sms: [] }; byMarket.set(row.value, market); }
    if (row.smUserId && !market.sms.some(person => person.value === row.smUserId)) market.sms.push({ value: row.smUserId, label: row.smName || "SM ohne Namen" });
  }
  const marketContexts = [...byMarket.values()];
  const matches = (market: MarketContext, except: string) =>
    (except === "region" || !input.region || market.region === input.region)
    && (except === "chain" || !input.chain || market.chain === input.chain)
    && (except === "marketId" || !input.marketId || market.value === input.marketId)
    && (except === "smUserId" || !input.smUserId || market.sms.some(person => person.value === input.smUserId));
  const unique = (rows: FilterDbRow[]) => [...new Map(rows.map(row => [row.value, row])).values()]
    .sort((a, b) => a.label.localeCompare(b.label, "de-AT") || a.value.localeCompare(b.value));
  return {
    regions: unique(marketContexts.filter(market => matches(market, "region") && market.region).map(market => ({ value: market.region, label: market.region }))),
    chains: unique(marketContexts.filter(market => matches(market, "chain") && market.chain).map(market => ({ value: market.chain, label: market.chain }))),
    markets: unique(marketContexts.filter(market => matches(market, "marketId")).map(({ value, label }) => ({ value, label }))),
    sms: unique(marketContexts.filter(market => matches(market, "smUserId")).flatMap(market => market.sms)),
    marketContexts,
  };
}

adminSmDashboardRouter.use(requireAuth(["admin", "sm_admin", "kunde"]));
adminSmDashboardRouter.use(requireKundeAdminPermission);

adminSmDashboardRouter.get("/", async (req, res, next) => {
  try {
    const defaults = defaultSmDashboardRange();
    const parsed = querySchema.safeParse({
      from: typeof req.query.from === "string" ? req.query.from : defaults.from,
      to: typeof req.query.to === "string" ? req.query.to : defaults.to,
      ...(typeof req.query.region === "string" && req.query.region.trim() ? { region: req.query.region } : {}),
      ...(typeof req.query.chain === "string" && req.query.chain.trim() ? { chain: req.query.chain } : {}),
      ...(typeof req.query.smUserId === "string" && req.query.smUserId.trim() ? { smUserId: req.query.smUserId } : {}),
      ...(typeof req.query.marketId === "string" && req.query.marketId.trim() ? { marketId: req.query.marketId } : {}),
      SMDurcharbeitCatalogScope: typeof req.query.SMDurcharbeitCatalogScope === "string" && req.query.SMDurcharbeitCatalogScope.trim() ? req.query.SMDurcharbeitCatalogScope : "standard",
    });
    if (!parsed.success) {
      res.status(400).json({ error: "Die Dashboard-Filter sind ungültig.", code: "sm_dashboard_query_invalid", details: parsed.error.flatten() });
      return;
    }
    if (isoDateToEpochDay(parsed.data.to) < isoDateToEpochDay(parsed.data.from)) {
      res.status(400).json({ error: "Das Enddatum muss am oder nach dem Startdatum liegen.", code: "sm_dashboard_range_invalid" });
      return;
    }
    if (isoDateToEpochDay(parsed.data.to) - isoDateToEpochDay(parsed.data.from) > 1_830) {
      res.status(400).json({ error: "Der Dashboard-Zeitraum darf höchstens fünf Jahre umfassen.", code: "sm_dashboard_range_too_large" });
      return;
    }

    const [{ visits, oosRows, overview }, filterOptions] = await db.transaction(async tx => Promise.all([
      loadSmDashboardRows(parsed.data, tx, true),
      loadSmDashboardFilterOptions(parsed.data, tx),
    ]), { isolationLevel: "repeatable read", accessMode: "read only" });

    res.json({
      meta: {
        from: parsed.data.from,
        to: parsed.data.to,
        timezone: VIENNA_TIMEZONE,
        generatedAt: new Date().toISOString(),
        filters: {
          region: parsed.data.region ?? null,
          chain: parsed.data.chain ?? null,
          smUserId: parsed.data.smUserId ?? null,
          marketId: parsed.data.marketId ?? null,
          SMDurcharbeitCatalogScope: parsed.data.SMDurcharbeitCatalogScope ?? null,
        },
      },
      ...aggregateSmDashboard(visits, oosRows),
      overview,
      SMDurcharbeitBreakdown: {
        standard: { completedVisits: visits.filter(row => row.SMDurcharbeitCatalogScope !== "SMDurcharbeit").length, answeredQuestions: visits.filter(row => row.SMDurcharbeitCatalogScope !== "SMDurcharbeit").reduce((n, row) => n + (row.SMDurcharbeitAnsweredQuestionCount ?? 0), 0) },
        SMDurcharbeit: { completedVisits: visits.filter(row => row.SMDurcharbeitCatalogScope === "SMDurcharbeit").length, answeredQuestions: visits.filter(row => row.SMDurcharbeitCatalogScope === "SMDurcharbeit").reduce((n, row) => n + (row.SMDurcharbeitAnsweredQuestionCount ?? 0), 0) },
      },
      filterOptions,
    });
  } catch (error) {
    next(error);
  }
});


/** The phone endpoint never accepts an account/date filter from the caller. */
export async function loadSmHomeDashboard(smUserId: string, date = currentViennaDate()) {
  return db.transaction(async (tx) => {
    const [people, { visits, oosRows }] = await Promise.all([
      tx.execute<{ userId: string; name: string; assignmentsToday: number }>(sql`
        select u.id::text as "userId", btrim(concat_ws(' ', u.first_name, u.last_name)) as name,
          (
            select count(*)::int from sm_assignments a
            where a.is_deleted = false
              and a.status <> 'cancelled'
              and coalesce(a.replacement_sm_user_id, a.original_sm_user_id) = u.id
              and coalesce(a.replacement_work_date, a.original_work_date) = ${date}::date
          ) as "assignmentsToday"
        from users u
        where u.id = ${smUserId}::uuid and u.role = 'sm' and u.is_active = true and u.deleted_at is null
      `),
      loadSmDashboardRows({ from: date, to: date, smUserId }, tx),
    ]);
    const person = people[0];
    if (!person) return null;
    return {
      ...person,
      date,
      timezone: VIENNA_TIMEZONE,
      generatedAt: new Date().toISOString(),
      visits: aggregateSmHomeVisits(visits, oosRows),
    };
  }, { isolationLevel: "repeatable read", accessMode: "read only" });
}

smHomeDashboardRouter.use(requireAuth(["sm"]));
smHomeDashboardRouter.get("/", async (req: AuthedRequest, res, next) => {
  res.set("Cache-Control", "private, no-store");
  try {
    // Reject extra filters instead of ever trusting a client-supplied SM identity.
    if (Object.keys(req.query).length > 0) {
      res.status(400).json({ error: "Diese Übersicht zeigt ausschließlich deinen heutigen Tag.", code: "sm_home_query_invalid" });
      return;
    }
    const data = await loadSmHomeDashboard(req.authUser!.appUserId);
    if (!data) {
      res.status(403).json({ error: "SM-Konto nicht verfügbar.", code: "account_inactive" });
      return;
    }
    res.json(data);
  } catch (error) {
    next(error);
  }
});
