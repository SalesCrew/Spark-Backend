import { sql } from "drizzle-orm";
import type { ModelDatabase } from "./praemien-workspace.js";
import {
  availabilityTypes,
  type AvailabilityType,
  type DashboardData,
  type DashboardFacets,
  type DashboardInterval,
  type DashboardPoint,
  type DashboardScope,
} from "../gm-dashboard.shared.js";

export type DashboardObservation = {
  intervalId: string;
  sessionId: string;
  marketId: string;
  gmId: string;
  questionId: string | null;
  submittedAt: string;
  changedAt: string | null;
  answerId: string | null;
  section: string | null;
  duration: number | null;
  available: boolean;
  availabilityType: string | null;
  red: boolean;
  answered: boolean;
  category: string | null;
  selectedAnswer?: string | null;
  questionText?: string | null;
  moduleName?: string | null;
  ipp: number | null;
  placements: number | null;
  competitor: number | null;
};
const round = (n: number) => Math.round(n * 10000) / 10000;
// STC uses the market's configured annual frequency, not completed visit counts.
const stcFrequencyRanges = {
  gold: [12, 24],
  silver: [8, 10],
  bronze: [6, 7],
} as const;
function selectedAnswer(row: DashboardObservation): string {
  return (row.selectedAnswer === undefined ? row.category : row.selectedAnswer)?.trim().toLocaleLowerCase("de-AT") ?? "";
}
export function availabilityCategory(
  value: string | null,
): "top" | "mediocre" | "bad" | null {
  const key = value
    ?.trim()
    .toLocaleLowerCase("de-AT")
    .replace(/[\s_-]/g, "");
  if (["top", "voll", "sehrvoll"].includes(key ?? "")) return "top";
  if (["mediocre", "mittel", "halbvoll"].includes(key ?? "")) return "mediocre";
  if (["bad", "leer", "nichtvoll"].includes(key ?? "")) return "bad";
  return null;
}
// Availability is an observation average, while IPP/placements describe the last
// answer to each market/question in an interval (the established IPP rule).
export function aggregateDashboard(
  intervals: DashboardInterval[],
  observations: DashboardObservation[],
  scope: DashboardScope,
): DashboardData {
  const buckets = new Map<string, DashboardObservation[]>();
  for (const row of observations) {
    const list = buckets.get(row.intervalId) ?? [];
    list.push(row);
    buckets.set(row.intervalId, list);
  }
  const points: DashboardPoint[] = intervals.map((interval) => {
    const rows = buckets.get(interval.id) ?? [];
    const availability = Object.fromEntries(
      availabilityTypes.map((key) => [
        key,
        { top: 0, mediocre: 0, bad: 0, total: 0, average: null },
      ]),
    ) as DashboardPoint["availability"];
    const sessions = new Map<
      string,
      { sections: Set<string>; duration: number | null; red: boolean }
    >();
    const visitQuestions = new Map<string, DashboardObservation>();
    const marketQuestions = new Map<string, DashboardObservation>();
    const newer = (
      a: DashboardObservation,
      b: DashboardObservation,
      withinVisit = false,
    ) => {
      const key = (r: DashboardObservation) =>
        `${withinVisit ? "" : r.submittedAt}|${r.changedAt ?? ""}|${r.answerId ?? ""}`;
      return key(a) > key(b);
    };
    for (const row of rows) {
      const session = sessions.get(row.sessionId) ?? {
        sections: new Set<string>(),
        duration: row.duration == null ? null : Number(row.duration),
        red: false,
      };
      if (row.section) session.sections.add(row.section);
      sessions.set(row.sessionId, session);
      if (!row.questionId) continue;
      const visitKey = `${row.sessionId}:${row.questionId}`,
        marketKey = `${row.marketId}:${row.questionId}`;
      const oldVisit = visitQuestions.get(visitKey),
        oldMarket = marketQuestions.get(marketKey);
      if (!oldVisit || newer(row, oldVisit, true))
        visitQuestions.set(visitKey, row);
      if (row.answered && (!oldMarket || newer(row, oldMarket)))
        marketQuestions.set(marketKey, row);
    }
    let availabilityExpected = 0,
      availabilityAnswered = 0;
    for (const row of visitQuestions.values()) {
      // Apply the latest answer to each question before counting the visit.
      // A completed answer, a sub-option or an earlier Ja is not sufficient.
      if (row.red && row.answered && selectedAnswer(row) === "ja")
        sessions.get(row.sessionId)!.red = true;
      if (
        !row.available ||
        !availabilityTypes.includes(row.availabilityType as AvailabilityType)
      )
        continue;
      availabilityExpected++;
      const category = row.answered ? availabilityCategory(row.category) : null;
      if (category) {
        availability[row.availabilityType as AvailabilityType][category]++;
        availabilityAnswered++;
      }
    }
    for (const counts of Object.values(availability)) {
      counts.total = counts.top + counts.mediocre + counts.bad;
      counts.average = counts.total
        ? round((counts.top * 100 + counts.mediocre * 50) / counts.total)
        : null;
    }
    const marketIpp = new Map<string, number>();
    const competitorQuestions = new Map<string, NonNullable<DashboardPoint["competitorQuestions"]>[number]>();
    let placements: number | null = null,
      competitor: number | null = null;
    for (const row of marketQuestions.values()) {
      if (row.ipp != null)
        marketIpp.set(
          row.marketId,
          (marketIpp.get(row.marketId) ?? 0) + Math.max(0, Number(row.ipp)),
        );
      if (row.placements != null)
        placements = (placements ?? 0) + Number(row.placements);
      if (row.competitor != null) {
        competitor = (competitor ?? 0) + Number(row.competitor);
        const detail = competitorQuestions.get(row.questionId!) ?? {
          questionId: row.questionId!,
          questionText: row.questionText ?? "",
          moduleName: row.moduleName ?? "",
          points: 0,
          marketCount: 0,
          yesCount: 0,
          noCount: 0,
        };
        detail.points += Number(row.competitor);
        detail.marketCount++;
        if (selectedAnswer(row) === "ja") detail.yesCount++;
        if (selectedAnswer(row) === "nein") detail.noCount++;
        competitorQuestions.set(row.questionId!, detail);
      }
    }
    const positive = [...marketIpp.values()].filter((n) => n > 0);
    const durations: number[] = [];
    let standardOnly = 0,
      flexOnly = 0,
      mixed = 0,
      other = 0,
      redSurveys = 0;
    for (const session of sessions.values()) {
      if (session.red) redSurveys++;
      if (
        session.duration != null &&
        session.duration > 0 &&
        session.duration <= 1440
      )
        durations.push(session.duration);
      const standard = session.sections.has("standard"),
        flex = session.sections.has("flex");
      if (standard && flex) mixed++;
      else if (standard) standardOnly++;
      else if (flex) flexOnly++;
      else other++;
    }
    return {
      ...interval,
      ipp: positive.length
        ? round(positive.reduce((a, b) => a + b, 0) / positive.length)
        : marketIpp.size
          ? 0
          : null,
      ippMarketCount: positive.length,
      ippSource: "market_answers",
      ippPlacement: marketIpp.size
        ? round([...marketIpp.values()].reduce((sum, value) => sum + value, 0))
        : null,
      placements: placements == null ? null : round(placements),
      competitor: competitor == null ? null : round(competitor),
      competitorQuestions: [...competitorQuestions.values()]
        .map((detail) => ({ ...detail, points: round(detail.points) }))
        .sort((a, b) => a.moduleName.localeCompare(b.moduleName, "de") || a.questionText.localeCompare(b.questionText, "de") || a.questionId.localeCompare(b.questionId)),
      availability,
      availabilityExpected,
      availabilityAnswered,
      visits: sessions.size,
      redSurveys,
      averageMinutes: durations.length
        ? round(durations.reduce((a, b) => a + b, 0) / durations.length)
        : null,
      standardOnly,
      flexOnly,
      mixed,
      other,
    };
  });
  return {
    points,
    scope,
    calculatedAt: new Date().toISOString(),
    timezone: "Europe/Vienna",
    stcApplied: scope.stc !== null,
  };
}

export async function dashboardMetadata(database: ModelDatabase) {
  // Match the dashboard's completed visits and Vienna calendar dates. Ordering
  // the timestamp uses the existing submitted-period partial index.
  const firstEntries = await database.query<{ firstEntryDate: string }>(sql`
      select (s.submitted_at at time zone 'Europe/Vienna')::date::text as "firstEntryDate"
      from visit_sessions s join markets m on m.id=s.market_id
      where s.is_deleted=false and s.status='submitted' and s.submitted_at is not null
        and s.submitted_at < (((now() at time zone 'Europe/Vienna')::date+1)::timestamp at time zone 'Europe/Vienna')
      order by s.submitted_at limit 1
    `);
  return { firstEntryDate: firstEntries[0]?.firstEntryDate ?? null };
}

export async function dashboardFacets(
  database: ModelDatabase,
): Promise<DashboardFacets> {
  const [markets, gms, firstEntries] = await Promise.all([
    database.query<DashboardFacets["markets"][number]>(
      sql`select id,concat_ws(' · ',nullif(name,''),nullif(address,''),nullif(concat_ws(' ',postal_code,city),'')) as label,coalesce(nullif(region,''),'Unbekannt') as region,'' as "gmName",coalesce(db_name,'') as chain,concat_ws(' ',name,db_name,address,postal_code,city,region,flex_number,standard_market_number,coke_master_number) as "searchText" from markets where is_deleted=false order by name,address,id`,
    ),
    database.query<DashboardFacets["gms"][number]>(
      sql`select id,concat_ws(' ',first_name,last_name) || case when is_active=false then ' (inaktiv)' else '' end as label,coalesce(nullif(region,''),'Unbekannt') as region from users where role='gm' order by first_name,last_name,id`,
    ),
    dashboardMetadata(database),
  ]);
  return { markets, gms, firstEntryDate: firstEntries.firstEntryDate };
}

// A bounded, parameterised read. The submitted-period index and question/scoring
// indexes are used; no Supabase REST page limit and no query per answer/interval.
export function canUseWholeGmIpp(scope: DashboardScope): boolean {
  return !scope.region && !scope.chain && !scope.chains?.length && !scope.chainGroups?.length && !scope.marketId && !scope.marketIds?.length && !scope.stc;
}

export async function loadDashboard(
  database: ModelDatabase,
  intervals: DashboardInterval[],
  scope: DashboardScope,
): Promise<DashboardData> {
  const stcFrequency = scope.stc ? stcFrequencyRanges[scope.stc] : null;
  const rows = await database.query<DashboardObservation>(sql`
    with periods as (
      select id,start::date::timestamp at time zone 'Europe/Vienna' as lo,
        ("end"::date+1)::timestamp at time zone 'Europe/Vienna' as hi
      from jsonb_to_recordset(${JSON.stringify(intervals)}::jsonb) as p(id text,start text,"end" text)
    ), selected_sessions as materialized (
      select p.id as interval_id,s.* from periods p join visit_sessions s on s.submitted_at>=p.lo and s.submitted_at<p.hi
      join markets m on m.id=s.market_id
      where s.is_deleted=false and s.status='submitted'
        and (${scope.stc}::text is null or m.visit_frequency_per_year between ${stcFrequency?.[0] ?? 0} and ${stcFrequency?.[1] ?? 0})
        and (${scope.gmId}::uuid is null or s.gm_user_id=${scope.gmId}::uuid)
        and (${scope.marketId}::uuid is null or m.id=${scope.marketId}::uuid)
        and (${!scope.marketIds?.length} or m.id in (
          select jsonb_array_elements_text(${JSON.stringify(scope.marketIds ?? [])}::jsonb)::uuid
        ))
        and (${scope.region}::text is null or coalesce(nullif(m.region,''),'Unbekannt')=${scope.region})
        and (${scope.chain}::text is null or m.db_name=${scope.chain})
        and (${!scope.chains?.length} or coalesce(m.db_name,'') in (
          select jsonb_array_elements_text(${JSON.stringify(scope.chains ?? [])}::jsonb)
        ))
        and (${!(scope.chainGroups?.length)} or (
          case
            when upper(regexp_replace(coalesce(m.db_name,''), '\\s+', '', 'g')) in ('BILLA','BILLA+','BILLAPLUS','ISP','ESP') then 'rewe'
            when upper(regexp_replace(coalesce(m.db_name,''), '\\s+', '', 'g'))='SPAR' then 'spar'
            else 'other'
          end
        ) in (select jsonb_array_elements_text(${JSON.stringify(scope.chainGroups ?? [])}::jsonb)))
    )
    select s.interval_id as "intervalId",s.id as "sessionId",s.market_id as "marketId",s.gm_user_id as "gmId",
      q.question_id as "questionId",s.submitted_at::text as "submittedAt",a.changed_at::text as "changedAt",a.id as "answerId",sec.section,
      extract(epoch from (s.submitted_at-s.started_at))/60.0 as duration,
      coalesce(q.single_choice_availability_snapshot,false) as available,q.single_choice_availability_type_snapshot as "availabilityType",
      coalesce(q.red_survey_snapshot,false) as red,(a.id is not null) as answered,
      coalesce(nullif(q.question_text_snapshot,''),qb.text,'') as "questionText",
      coalesce(q.module_name_snapshot,'') as "moduleName",
      coalesce(nullif(btrim(a.value_text),''),
        case jsonb_typeof(a.value_json->'raw')
          when 'string' then nullif(btrim(a.value_json->>'raw'),'')
          when 'object' then nullif(btrim(a.value_json->'raw'->>'sel'),'')
        end, opts.top_value) as "selectedAnswer",
      coalesce(nullif(a.value_text,''),a.value_json->>'raw',opts.values->>0) as category,
      weights.ipp,weights.placements,weights.competitor
    from selected_sessions s
    left join visit_session_sections sec on sec.visit_session_id=s.id and sec.is_deleted=false
    left join visit_session_questions q on q.visit_session_section_id=sec.id and q.is_deleted=false and q.applies_to_market_chain_snapshot=true
    left join question_bank_shared qb on qb.id=q.question_id
    left join visit_answers a on a.visit_session_question_id=q.id and a.visit_session_id=s.id and a.is_deleted=false and a.is_valid=true and a.answer_status='answered'
    left join lateral (select jsonb_agg(o.option_value) as values,
      min(o.option_value) filter (where o.option_role='top') as top_value
      from visit_answer_options o where o.visit_answer_id=a.id and o.is_deleted=false) opts on true
    left join lateral (
      select sum(case when sc.ipp is not null then sc.ipp * k.factor end) as ipp,
        sum(case when sc.zweitplatzierung is not null then sc.zweitplatzierung * k.factor end) as placements,
        sum(case when sc.mitbewerberabfrage is not null then sc.mitbewerberabfrage * k.factor end) as competitor
      from (
        select distinct value as key,1::numeric as factor from jsonb_array_elements_text(
          coalesce(opts.values,'[]'::jsonb) || case when a.value_text is not null then jsonb_build_array(a.value_text) else '[]'::jsonb end ||
          case jsonb_typeof(a.value_json->'raw')
            when 'string' then jsonb_build_array(a.value_json->'raw') when 'array' then a.value_json->'raw'
            when 'object' then coalesce(a.value_json->'raw'->'subs','[]'::jsonb) || case when a.value_json->'raw'->>'sel' is not null then jsonb_build_array(a.value_json->'raw'->>'sel') else '[]'::jsonb end
            else '[]'::jsonb end
        )
        union all select '__value__',a.value_number where a.question_type in ('numeric','slider','likert') and a.value_number is not null
      ) k join question_scoring sc on sc.question_id=q.question_id and sc.score_key=k.key and sc.is_deleted=false
    ) weights on true
    order by s.interval_id,s.submitted_at,a.changed_at,a.id
  `);
  return aggregateDashboard(intervals, rows, scope);
}
