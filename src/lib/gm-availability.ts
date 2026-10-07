import { sql } from "drizzle-orm";
import type { ModelDatabase } from "./praemien-workspace.js";
import type { DashboardInterval, DashboardScope } from "../gm-dashboard.shared.js";
import { auditAvailability, type AvailabilityAnswer, type AvailabilityObservation } from "../gm-availability.shared.js";
import { computeHiddenQuestionIds } from "./conditional-visibility.js";

type RecordedQuestion = Omit<AvailabilityObservation, "answer" | "visible"> & {
  available: boolean;
  rules: Array<Record<string, unknown>>;
  orderIndex: number;
  answerId: string | null;
  answerStatus: string | null;
  isValid: boolean | null;
  valueText: string | null;
  valueNumber: string | null;
  valueJson: Record<string, unknown> | null;
  options: NonNullable<AvailabilityAnswer["options"]>;
};
const stcRanges = { gold: [12, 24], silver: [8, 10], bronze: [6, 7] } as const;

// Read historical snapshots, including rule triggers. No schema repair or writes.
export async function loadAvailabilityAudit(database: ModelDatabase, intervals: DashboardInterval[], scope: DashboardScope) {
  const frequency = scope.stc ? stcRanges[scope.stc] : null;
  const questions = await database.query<RecordedQuestion>(sql`
    with periods as (
      select id,start::date::timestamp at time zone 'Europe/Vienna' as lo,
        ("end"::date+1)::timestamp at time zone 'Europe/Vienna' as hi
      from jsonb_to_recordset(${JSON.stringify(intervals)}::jsonb) as p(id text,start text,"end" text)
    ), selected_sessions as materialized (
      select p.id as interval_id,s.*,coalesce(m.db_name,'') as chain,
        coalesce(nullif(m.region,''),'Unbekannt') as region
      from periods p join visit_sessions s
        on ((s.started_at>=p.lo and s.started_at<p.hi)
          or (s.started_at is null and s.submitted_at>=p.lo and s.submitted_at<p.hi))
      join markets m on m.id=s.market_id
      where s.is_deleted=false and s.status='submitted' and s.submitted_at is not null
        and (${scope.gmId}::uuid is null or s.gm_user_id=${scope.gmId}::uuid)
        and (${scope.region}::text is null or coalesce(nullif(m.region,''),'Unbekannt')=${scope.region})
        and (${scope.marketId}::uuid is null or m.id=${scope.marketId}::uuid)
        and (${!scope.marketIds?.length} or m.id in (select jsonb_array_elements_text(${JSON.stringify(scope.marketIds ?? [])}::jsonb)::uuid))
        and (${scope.chain}::text is null or m.db_name=${scope.chain})
        and (${!scope.chains?.length} or coalesce(m.db_name,'') in (select jsonb_array_elements_text(${JSON.stringify(scope.chains ?? [])}::jsonb)))
        and (${!scope.chainGroups?.length} or (
          case when upper(regexp_replace(coalesce(m.db_name,''), '\\s+', '', 'g')) in ('BILLA','BILLA+','BILLAPLUS','BILLACORSO') then 'rewe'
            when upper(regexp_replace(coalesce(m.db_name,''), '\\s+', '', 'g')) in ('SPAR','SPARMARKT','ISP','INTERSPAR','ESP','EUROSPAR') then 'spar' else 'other' end
        ) in (select jsonb_array_elements_text(${JSON.stringify(scope.chainGroups ?? [])}::jsonb)))
        and (${scope.stc}::text is null or m.visit_frequency_per_year between ${frequency?.[0] ?? 0} and ${frequency?.[1] ?? 0})
    )
    select s.interval_id as "intervalId",s.id as "sessionId",s.market_id as "marketId",
      s.chain,s.region,s.gm_user_id as "gmId",s.started_at::text as "startedAt",s.submitted_at::text as "submittedAt",
      q.id as "visitQuestionId",q.question_id as "questionId",sec.id as "sectionId",sec.campaign_id as "campaignId",
      coalesce(q.question_text_snapshot,'') as "questionText",coalesce(q.module_name_snapshot,'') as "moduleName",
      coalesce(q.single_choice_availability_snapshot,false) as available,
      q.single_choice_availability_type_snapshot as "availabilityType",q.applies_to_market_chain_snapshot as "appliesToChain",
      coalesce(q.question_rules_snapshot,'[]'::jsonb) as rules,q.order_index as "orderIndex",
      a.id as "answerId",a.answer_status as "answerStatus",a.is_valid as "isValid",
      a.value_text as "valueText",a.value_number as "valueNumber",a.value_json as "valueJson",a.changed_at::text as "changedAt",coalesce(a.version,0) as version,
      coalesce(opts.values,'[]'::jsonb) as options
    from selected_sessions s
    join visit_session_sections sec on sec.visit_session_id=s.id and sec.is_deleted=false
    join visit_session_questions q on q.visit_session_section_id=sec.id and q.is_deleted=false
    left join lateral (
      select a.* from visit_answers a
      where a.visit_session_question_id=q.id and a.visit_session_id=s.id and a.is_deleted=false
      order by a.changed_at desc nulls last,a.version desc,a.id desc limit 1
    ) a on true
    left join lateral (
      select jsonb_agg(jsonb_build_object('optionRole',o.option_role,'optionValue',o.option_value,'orderIndex',o.order_index) order by o.order_index,o.id) as values
      from visit_answer_options o where o.visit_answer_id=a.id and o.is_deleted=false
    ) opts on true
    order by s.interval_id,s.id,sec.order_index,q.order_index,q.id
  `);
  const sections = new Map<string, RecordedQuestion[]>();
  for (const question of questions) {
    const key = JSON.stringify([question.intervalId, question.sectionId]);
    const rows = sections.get(key) ?? [];
    rows.push(question); sections.set(key, rows);
  }
  const observations: AvailabilityObservation[] = [];
  for (const section of sections.values()) {
    const applicable = section.filter(q => q.appliesToChain);
    const answers = new Map(applicable.map(q => {
      const raw = q.valueJson?.raw;
      const value = Array.isArray(raw) ? raw.filter((v): v is string => typeof v === "string")
        : raw && typeof raw === "object" ? JSON.stringify(raw)
        : typeof raw === "string" ? raw : q.valueText || (q.valueNumber == null ? undefined : String(q.valueNumber));
      return [q.visitQuestionId, value] as const;
    }));
    const hidden = computeHiddenQuestionIds(applicable.map(q => ({ id: q.visitQuestionId, questionId: q.questionId, rules: q.rules })), answers);
    for (const q of section) {
      if (!q.available) continue;
      observations.push({
        intervalId: q.intervalId, sessionId: q.sessionId, visitQuestionId: q.visitQuestionId,
        questionId: q.questionId, sectionId: q.sectionId, campaignId: q.campaignId,
        questionText: q.questionText, moduleName: q.moduleName, marketId: q.marketId,
        chain: q.chain, region: q.region, gmId: q.gmId,
        startedAt: q.startedAt ? new Date(q.startedAt).toISOString() : null,
        submittedAt: new Date(q.submittedAt).toISOString(),
        changedAt: q.changedAt ? new Date(q.changedAt).toISOString() : null,
        version: q.version, availabilityType: q.availabilityType,
        appliesToChain: q.appliesToChain, visible: !hidden.has(q.visitQuestionId),
        answer: q.answerId ? { id: q.answerId, answerStatus: q.answerStatus ?? "unanswered", isValid: Boolean(q.isValid),
          valueText: q.valueText, valueNumber: q.valueNumber, valueJson: q.valueJson, options: q.options } : null,
      });
    }
  }
  return auditAvailability(observations);
}
