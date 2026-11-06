import { sql } from "drizzle-orm";
import type { ModelDatabase } from "./praemien-workspace.js";
import { regularGmVisitSql } from "./gm-reporting-scope.js";
import { loadAvailabilityAudit } from "./gm-availability.js";
import { computeHiddenQuestionIds } from "./conditional-visibility.js";
import { availabilityChainGroup } from "../gm-availability.shared.js";
import { buildAvailabilityReport, buildCompetitorReport, type DashboardReport, type ReportRequest, type ReportQuestion, type ReportVisit } from "../gm-report.shared.js";

export type ReportStoragePhoto = { id: string; bucket: string; path: string; marketName: string; questionText: string; marketId: string };
export type ReportPhotoReader = (photo: ReportStoragePhoto) => Promise<string | null>;
export function reportSessions(input: ReportRequest) {
  const scope = input.scope, frequency = scope.stc ? ({ gold: [12, 24], silver: [8, 10], bronze: [6, 7] } as const)[scope.stc] : null;
  return sql`with periods as (
    select id,start::date::timestamp at time zone 'Europe/Vienna' as lo,
      ("end"::date+1)::timestamp at time zone 'Europe/Vienna' as hi
    from jsonb_to_recordset(${JSON.stringify(input.periods)}::jsonb) p(id text,start text,"end" text)
  ), selected_sessions as materialized (
    select p.id as period_id,s.id as session_id,s.market_id,s.gm_user_id,s.submitted_at,s.started_at,
      coalesce(m.name,'') as market_name,coalesce(m.db_name,'') as chain,coalesce(nullif(m.region,''),'Unbekannt') as region,
      to_char(coalesce(s.started_at,s.submitted_at) at time zone 'Europe/Vienna','YYYY-MM-DD') as visit_date
    from periods p join visit_sessions s on
      coalesce(s.started_at,s.submitted_at)>=p.lo and coalesce(s.started_at,s.submitted_at)<p.hi
    join markets m on m.id=s.market_id
    where s.is_deleted=false and s.status='submitted' and s.submitted_at is not null
      and ${sql.raw(regularGmVisitSql("s"))}
      and (${scope.gmId}::uuid is null or s.gm_user_id=${scope.gmId}::uuid)
      and (${scope.marketId}::uuid is null or s.market_id=${scope.marketId}::uuid)
      and (${scope.region}::text is null or coalesce(nullif(m.region,''),'Unbekannt')=${scope.region})
      and (${scope.chain}::text is null or m.db_name=${scope.chain})
      and (${!scope.marketIds?.length} or m.id in (select jsonb_array_elements_text(${JSON.stringify(scope.marketIds ?? [])}::jsonb)::uuid))
      and (${!scope.chains?.length} or coalesce(m.db_name,'') in (select jsonb_array_elements_text(${JSON.stringify(scope.chains ?? [])}::jsonb)))
      and (${!scope.chainGroups?.length} or (case
        when upper(regexp_replace(coalesce(m.db_name,''),'\\s+','','g')) in ('BILLA','BILLA+','BILLAPLUS','BILLACORSO') then 'rewe'
        when upper(regexp_replace(coalesce(m.db_name,''),'\\s+','','g')) in ('SPAR','SPARMARKT','ISP','INTERSPAR','ESP','EUROSPAR') then 'spar'
        else 'other' end) in (select jsonb_array_elements_text(${JSON.stringify(scope.chainGroups ?? [])}::jsonb)))
      and (${scope.stc}::text is null or m.visit_frequency_per_year between ${frequency?.[0] ?? 0} and ${frequency?.[1] ?? 0})
  )`;
}
const visitColumns = sql`s.period_id as "periodId",s.session_id as "sessionId",s.market_id as "marketId",s.market_name as "marketName",s.chain,s.region,s.visit_date as date,s.submitted_at::text as "submittedAt"`;

export async function loadReportQuestions(database: ModelDatabase, input: ReportRequest): Promise<ReportQuestion[]> {
  const rows = await database.query<ReportQuestion>(sql`${reportSessions(input)}
    select ${visitColumns},q.id,sec.id as "sectionId",q.question_id as "questionId",q.question_text_snapshot as text,
      q.module_name_snapshot as "moduleName",q.question_type as "questionType",coalesce(q.single_choice_availability_snapshot,false) as available,
      q.single_choice_availability_type_snapshot as "availabilityType",q.applies_to_market_chain_snapshot as applies,
      coalesce(q.question_rules_snapshot,'[]'::jsonb) as rules,
      a.id as "answerId",a.value_text as "valueText",a.value_number as "valueNumber",a.value_json->'raw' as raw,
      coalesce(a.answer_status='answered',false) as answered,coalesce(a.is_valid,false) as valid,
      coalesce(a.changed_at,s.submitted_at)::text as "changedAt",coalesce(o.values,'[]'::jsonb) as options,
      coalesce(c.comment_text,'') as comment,
      (coalesce(flags.competitor,false) or q.module_name_snapshot ~* 'mitbewerb|competitor' or
        (q.question_text_snapshot ~* 'kühler|kuehler|cooler|platzierung|display|schütte|schuette|placement' and
         q.question_text_snapshot ~* 'coke|coca|monster|red.?bull|fritz|vöslauer|rauch|bacardi|donat|rogaska')) as "isCompetitor",
      coalesce(flags.coke,false) as "isCokePlacement"
    from selected_sessions s
    join visit_session_sections sec on sec.visit_session_id=s.session_id and sec.is_deleted=false
    join visit_session_questions q on q.visit_session_section_id=sec.id and q.is_deleted=false
    left join lateral (select a.* from visit_answers a where a.visit_session_question_id=q.id and a.visit_session_id=s.session_id and a.is_deleted=false
      order by a.changed_at desc nulls last,a.version desc,a.id desc limit 1) a on true
    left join lateral (select jsonb_agg(o.option_value order by o.order_index,o.id) as values from visit_answer_options o where o.visit_answer_id=a.id and o.is_deleted=false) o on true
    left join lateral (select c.comment_text from visit_question_comments c where c.visit_session_question_id=q.id and c.is_deleted=false order by c.commented_at desc,c.id desc limit 1) c on true
    left join lateral (select bool_or(sc.mitbewerberabfrage is not null) as competitor,bool_or(sc.zweitplatzierung is not null) as coke from question_scoring sc where sc.question_id=q.question_id and sc.is_deleted=false) flags on true
    where q.applies_to_market_chain_snapshot=true and (
      coalesce(flags.competitor,false) or coalesce(flags.coke,false) or q.module_name_snapshot ~* 'mitbewerb|competitor' or
      (q.question_text_snapshot ~* 'kühler|kuehler|cooler|platzierung|display|schütte|schuette|placement' and q.question_text_snapshot ~* 'coke|coca|monster|red.?bull|fritz|vöslauer|rauch|bacardi|donat|rogaska') or
      jsonb_array_length(coalesce(q.question_rules_snapshot,'[]'::jsonb))>0 or exists (
        select 1 from visit_session_questions context,
        lateral jsonb_array_elements(coalesce(context.question_rules_snapshot,'[]'::jsonb)) rule
        where context.visit_session_section_id=sec.id and context.is_deleted=false and context.applies_to_market_chain_snapshot=true
          and (rule->>'triggerQuestionId'=q.question_id::text or rule->>'triggerQuestionId'=q.id::text)
      )
    ) order by s.period_id,s.session_id,sec.order_index,q.order_index,q.id limit 100001`);
  if (rows.length > 100000) throw Object.assign(new Error("Zu viele Antworten für einen Bericht. Bitte den Zeitraum oder die Filter eingrenzen."), { status: 413 });
  const sections = new Map<string, ReportQuestion[]>();
  for (const row of rows) { const key = JSON.stringify([row.periodId, row.sectionId]); const section = sections.get(key) ?? []; section.push(row); sections.set(key, section); }
  for (const section of sections.values()) {
    const answers = new Map(section.map(q => [q.id, Array.isArray(q.raw) ? q.raw.filter((v): v is string => typeof v === "string") : q.raw && typeof q.raw === "object" ? JSON.stringify(q.raw) : q.valueText ?? (q.valueNumber == null ? undefined : String(q.valueNumber))]));
    const hidden = computeHiddenQuestionIds(section.map(q => ({ id: q.id, questionId: q.questionId, rules: q.rules })), answers);
    for (const q of section) { q.visible = !hidden.has(q.id); q.submittedAt = new Date(q.submittedAt).toISOString(); q.changedAt = new Date(q.changedAt).toISOString(); }
  }
  return rows.filter(q => !q.available);
}
export async function prepareDashboardReport(database: ModelDatabase, input: ReportRequest, photoReader?: ReportPhotoReader): Promise<DashboardReport> {
  const visits = await database.query<ReportVisit>(sql`${reportSessions(input)} select ${visitColumns} from selected_sessions s order by s.period_id,s.submitted_at,s.session_id limit 30001`);
  if (visits.length > 30000) throw Object.assign(new Error("Zu viele Besuche für einen Bericht. Bitte Filter oder Zeitraum eingrenzen."), { status: 413 });
  const sessions = new Map(visits.map(v => [v.sessionId, v]));
  const groups = { rewe: 0, spar: 0, other: 0 };
  const groupVisits = input.kind === "availability" ? new Map(visits.map(v => [v.marketId, v])).values() : sessions.values();
  for (const v of groupVisits) groups[availabilityChainGroup(v.chain)]++;
  const base: DashboardReport = { kind: input.kind, periods: input.periods, cutoff: input.cutoff, scope: input.scope,
    generatedAt: new Date().toISOString(), visitCount: sessions.size, uniqueMarketCount: new Set(visits.map(v => v.marketId)).size,
    groups, availability: null, competitor: null, photos: [], photosAllowed: !!photoReader, warnings: [] };
  if (input.kind === "competitor") {
    const rows = await loadReportQuestions(database, input);
    if (input.mappings?.some(m => !rows.some(q => q.questionId === m.questionId && (q.isCompetitor || q.isCokePlacement)))) throw Object.assign(new Error("Eine Zuordnung gehört nicht zur gewählten Datenbasis. Bitte die Vorschau neu laden."), { status: 400 });
    base.competitor = buildCompetitorReport(rows, input.periods, input.mappings);
    if (base.competitor.issues.length) base.warnings.push("Nicht eindeutig erfasste physische Mengen erscheinen als —. Punkte werden nicht in Stückzahlen umgerechnet.");
    if (base.competitor.excludedQuestions) base.warnings.push(`${base.competitor.excludedQuestions} Frage(n) wurden für diesen Bericht bewusst ausgeschlossen.`);
    return base;
  }
  const period = input.periods[0]!;
  const audit = await loadAvailabilityAudit(database, input.periods.map(p => ({ ...p, shortLabel: p.label })), input.scope, true);
  base.availability = buildAvailabilityReport(audit, visits, period);
  const omitted = audit.filter(a => !a.included && !["hidden_by_chain", "hidden_by_rule", "duplicate_visit_question"].includes(a.exclusion ?? "")).length;
  if (omitted) base.warnings.push(`${omitted} nicht auswertbare Verfügbarkeitsantwort(en); Prozentwerte beziehen sich auf gültige Antworten.`);
  if (photoReader) {
    const eligible = audit.filter(a => a.included && a.answer?.id);
    const byAnswer = new Map(eligible.map(a => [a.answer!.id!, a]));
    if (byAnswer.size) {
      const photos = await database.query<{ id: string; answerId: string; bucket: string; path: string }>(sql`
        select p.id,p.visit_answer_id as "answerId",p.storage_bucket as bucket,p.storage_path as path
        from visit_answer_photos p where p.is_deleted=false and p.visit_answer_id in (
          select jsonb_array_elements_text(${JSON.stringify([...byAnswer.keys()])}::jsonb)::uuid
        ) order by p.uploaded_at desc,p.id desc limit 80`);
      const seen = new Set<string>(), chosen: typeof photos = [];
      for (const photo of photos) { const market = byAnswer.get(photo.answerId)!.marketId; if (!seen.has(market)) { chosen.push(photo); seen.add(market); } if (chosen.length === 8) break; }
      if (chosen.length < 8) for (const photo of photos) { if (!chosen.some(p => p.id === photo.id)) chosen.push(photo); if (chosen.length === 8) break; }
      base.photos = await Promise.all(chosen.map(async p => {
        const answer = byAnswer.get(p.answerId)!, visit = sessions.get(answer.sessionId);
        const meta: ReportStoragePhoto = { ...p, marketId: answer.marketId, marketName: visit?.marketName ?? "", questionText: answer.questionText };
        return { id: p.id, marketName: meta.marketName, questionText: meta.questionText, url: await photoReader(meta).catch(() => null) };
      }));
      if (base.photos.some(p => !p.url)) base.warnings.push("Einzelne Fotos konnten nicht geladen werden. Die Fotoseite kennzeichnet fehlende Bilder.");
    }
  }
  return base;
}
