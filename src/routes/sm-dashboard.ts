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
  return conditions;
}

function whereClause(conditions: SQL[]): SQL {
  return sql.join(conditions, sql` and `);
}

export async function loadSmDashboardRows(input: z.infer<typeof querySchema>, executor: Pick<typeof db, "execute"> = db) {
  const scopedWhere = whereClause(scopeConditions(input));
  const [visitResult, oosResult] = await Promise.all([
    executor.execute(sql<VisitDbRow>`
      select
        s.id::text as "submissionId",
        s.sm_market_id::text as "marketId",
        s.market_name_snapshot as "marketName",
        m.chain as "chain",
        m.region as "region",
        s.sm_user_id::text as "smUserId",
        s.sm_name_snapshot as "smName"
      from sm_questionnaire_submissions s
      inner join sm_markets m on m.id = s.sm_market_id
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
  ]);

  const visits = [...visitResult] as SmDashboardVisitRow[];
  const oosRows = ([...oosResult] as OosDbRow[]).flatMap((row) => {
    const mapped = toOosRow(row);
    return mapped ? [mapped] : [];
  });

  return { visits, oosRows };
}

async function loadFilterOptions() {
  const [regions, chains, markets, users] = await Promise.all([
    db.execute(sql<FilterDbRow>`
      select distinct region as value, region as label
      from sm_markets
      where is_deleted = false and btrim(region) <> ''
      order by region
    `),
    db.execute(sql<FilterDbRow>`
      select distinct chain as value, chain as label
      from sm_markets
      where is_deleted = false and btrim(chain) <> ''
      order by chain
    `),
    db.execute(sql<FilterDbRow>`
      select id::text as value, concat_ws(' · ', name, nullif(concat_ws(' ', postal_code, city), '')) as label
      from sm_markets
      where is_deleted = false
      order by chain, name, postal_code
    `),
    db.execute(sql<FilterDbRow>`
      select id::text as value, btrim(concat_ws(' ', first_name, last_name)) as label
      from users
      where role = 'sm' and deleted_at is null
      order by first_name, last_name
    `),
  ]);
  return {
    regions: [...regions] as FilterDbRow[],
    chains: [...chains] as FilterDbRow[],
    markets: [...markets] as FilterDbRow[],
    sms: [...users] as FilterDbRow[],
  };
}

adminSmDashboardRouter.use(requireAuth(["admin", "sm_admin"]));

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

    const [{ visits, oosRows }, filterOptions] = await Promise.all([
      loadSmDashboardRows(parsed.data),
      loadFilterOptions(),
    ]);

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
        },
      },
      ...aggregateSmDashboard(visits, oosRows),
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
