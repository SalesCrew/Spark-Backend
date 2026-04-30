require("dotenv").config();
const postgres = require("postgres");

async function main() {
  const db = postgres(process.env.DATABASE_URL, { prepare: false });
  try {
    const stats = await db.unsafe(`
      select
        pid,
        state,
        wait_event_type,
        wait_event,
        now() - query_start as running_for,
        left(query, 240) as query
      from pg_stat_activity
      where datname = current_database()
        and pid <> pg_backend_pid()
        and state <> 'idle'
      order by query_start asc
    `);

    const counts = await db.unsafe(`
      select count(*)::int as markets_count from public.markets
    `);

    console.log(JSON.stringify({ active: stats, marketsCount: counts[0]?.markets_count ?? 0 }, null, 2));
  } finally {
    await db.end();
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
