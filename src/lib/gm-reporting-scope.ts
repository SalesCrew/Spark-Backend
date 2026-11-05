import { sql, type SQL } from "drizzle-orm";

/** Fixed SQL fragments for reporting reads. Never accept a caller-provided alias. */
export function regularGmVisitSql(alias: "s" | "v" | "vs" | "extra"): string {
  return `(${alias}.is_supplemental_gm_snapshot=false and exists (
    select 1 from users reporting_gm where reporting_gm.id=${alias}.gm_user_id
      and reporting_gm.role='gm' and reporting_gm.is_supplemental_gm=false
  ))`;
}

/** Bonus results belong to an entire GM/wave, so mixed waves stay separate. */
export function regularBonusGmCondition(gmId: SQL, waveId: SQL): SQL {
  return sql`exists (
    select 1 from users reporting_gm join praemien_waves reporting_wave on reporting_wave.id=${waveId}
    where reporting_gm.id=${gmId} and reporting_gm.role='gm' and reporting_gm.is_supplemental_gm=false
      and not exists (
        select 1 from visit_sessions reporting_visit
        where reporting_visit.gm_user_id=reporting_gm.id
          and reporting_visit.is_supplemental_gm_snapshot=true
          and reporting_visit.is_deleted=false and reporting_visit.status='submitted'
          and (reporting_visit.submitted_at at time zone 'Europe/Vienna')::date
            between reporting_wave.start_date and reporting_wave.end_date
      )
  )`;
}

/** Recorded workdays cannot be apportioned between budgets without guessing. */
export async function regularGmReportingDays<T extends { gmId: string; date: string }>(
  database: import("./praemien-workspace.js").ModelDatabase,
  sessions: T[],
): Promise<T[]> {
  if (!sessions.length) return [];
  const excluded = await database.query<{ gmId: string; date: string }>(sql`
    select distinct days.gm_id as "gmId",days.date from
      jsonb_to_recordset(${JSON.stringify(sessions.map(day => ({ gm_id: day.gmId, date: day.date })))}::jsonb) as days(gm_id text,date text)
    join users u on u.id=days.gm_id::uuid
    where u.is_supplemental_gm=true or exists (
      select 1 from visit_sessions v where v.gm_user_id=u.id and v.is_supplemental_gm_snapshot=true
        and v.is_deleted=false and v.status in ('draft','submitted')
        and (coalesce(v.started_at,v.submitted_at) at time zone 'Europe/Vienna')::date=days.date::date
    )
  `);
  const keys = new Set(excluded.map(day => `${day.gmId}/${day.date}`));
  return sessions.filter(day => !keys.has(`${day.gmId}/${day.date}`));
}
