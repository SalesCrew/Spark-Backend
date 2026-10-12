import { sql } from "drizzle-orm";
import type { ModelDatabase } from "./praemien-workspace.js";
import { isSpezialfrageActive } from "./spezialfragen-period.js";

// Questionnaire status is editorial. Campaign status and its inclusive local
// date window determine whether that questionnaire is actually in use.
export async function activeBonusQuestions(database: ModelDatabase, at = new Date()) {
  const today = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Vienna", year: "numeric", month: "2-digit", day: "2-digit",
  }).format(at);
  const scopes = ["main", "kuehler", "mhd", "durcharbeit"] as const;
  const questionLinks = scopes.flatMap((scope) => {
    const questionnaire = sql.raw(`fragebogen_${scope}`);
    const moduleLink = sql.raw(`fragebogen_${scope}_module`);
    const module = sql.raw(`module_${scope}`);
    const questionLink = sql.raw(`module_${scope}_question`);
    const specialLink = sql.raw(`fragebogen_${scope}_spezial_question`);
    const section = scope === "main"
      ? sql`c.section in ('standard','flex','billa') and c.section::text = any(f.section_keywords::text[])`
      : sql`c.section = ${scope}`;
    return [
      sql`select mq.question_id from active_campaigns c
        join ${questionnaire} f on f.id=c.current_fragebogen_id and f.is_deleted=false
        join ${moduleLink} fm on fm.fragebogen_id=f.id and fm.is_deleted=false
        join ${module} m on m.id=fm.module_id and m.is_deleted=false
        join ${questionLink} mq on mq.module_id=m.id and mq.is_deleted=false
        where ${section}`,
      sql`select sq.question_id from active_campaigns c
        join ${questionnaire} f on f.id=c.current_fragebogen_id and f.is_deleted=false
        join ${specialLink} sq on sq.fragebogen_id=f.id and sq.is_deleted=false
        where ${section}`,
    ];
  });
  const questions = await database.query<{
    id: string; text: string; type: string; config: Record<string, unknown>;
    scores: { scoreKey: string; weight: number | null }[];
    updatedAt: string; isSpezial: boolean;
  }>(sql`
    with active_campaigns as (
      select section,current_fragebogen_id from campaigns
      where is_deleted=false and (
        (status='active' and schedule_type='always') or
        (status in ('active','scheduled') and schedule_type='scheduled'
          and start_date <= ${today}::date and end_date >= ${today}::date)
      )
    ), active_questions as (${sql.join(questionLinks, sql` union `)})
    select q.id,q.text,q.question_type as type,q.config,
      q.updated_at::text as "updatedAt",q.is_spezial as "isSpezial",
      coalesce(jsonb_agg(jsonb_build_object('scoreKey',s.score_key,'weight',s.boni))
        filter(where s.id is not null),'[]'::jsonb) as scores
    from question_bank_shared q
    left join question_scoring s on s.question_id=q.id and s.is_deleted=false
    where q.is_deleted=false and exists(select 1 from active_questions a where a.question_id=q.id)
    group by q.id order by q.text
  `);
  return questions
    .filter(q => !q.isSpezial || isSpezialfrageActive(q.config, at))
    .map(({ isSpezial: _isSpezial, ...q }) => q);
}
