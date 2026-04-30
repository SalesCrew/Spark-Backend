import { sql } from "../lib/db.js";

async function main() {
  const tableRows = await sql<{
    praemien_waves: string | null;
    praemien_wave_thresholds: string | null;
    praemien_wave_pillars: string | null;
    praemien_wave_sources: string | null;
    praemien_wave_quality_scores: string | null;
    praemien_gm_wave_contributions: string | null;
    praemien_gm_wave_totals: string | null;
  }[]>`
    select
      to_regclass('public.praemien_waves') as praemien_waves,
      to_regclass('public.praemien_wave_thresholds') as praemien_wave_thresholds,
      to_regclass('public.praemien_wave_pillars') as praemien_wave_pillars,
      to_regclass('public.praemien_wave_sources') as praemien_wave_sources,
      to_regclass('public.praemien_wave_quality_scores') as praemien_wave_quality_scores,
      to_regclass('public.praemien_gm_wave_contributions') as praemien_gm_wave_contributions,
      to_regclass('public.praemien_gm_wave_totals') as praemien_gm_wave_totals
  `;
  console.log("tables:", tableRows[0]);

  const indexRows = await sql<{ indexname: string }[]>`
    select indexname
    from pg_indexes
    where schemaname = 'public'
      and indexname in (
        'praemien_waves_year_quarter_active_unique',
        'praemien_wave_thresholds_wave_order_active_unique',
        'praemien_wave_pillars_wave_order_active_unique',
        'praemien_wave_sources_wave_question_score_active_unique',
        'praemien_wave_quality_scores_wave_gm_active_unique',
        'question_scoring_boni_active_idx',
        'praemien_gm_wave_contrib_dedupe_unique',
        'praemien_gm_wave_totals_wave_gm_unique'
      )
    order by indexname
  `;
  console.log("indexes:", indexRows.map((row) => row.indexname));

  const now = new Date();
  const year = now.getUTCFullYear() + 10;
  const [insertedWave] = await sql<{ id: string }[]>`
    insert into praemien_waves (name, year, quarter, start_date, end_date, status, description, timezone)
    values ('migration-check', ${year}, 1, '2036-01-01', '2036-03-31', 'draft', '', 'Europe/Vienna')
    returning id
  `;
  console.log("insertedWaveId:", insertedWave?.id ?? null);

  if (insertedWave?.id) {
    await sql`
      update praemien_waves
      set is_deleted = true, deleted_at = now(), updated_at = now()
      where id = ${insertedWave.id}
    `;
  }

  console.log("praemien migration verification completed.");
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await sql.end({ timeout: 5 });
  });
