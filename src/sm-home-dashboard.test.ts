import assert from "node:assert/strict";
import test, { after } from "node:test";
import { PgDialect } from "drizzle-orm/pg-core";
import type { SQL } from "drizzle-orm";

// No real DB/Auth/network access. Even an accidental unmocked call targets a closed local port.
process.env.DATABASE_URL = "postgres://test:test@127.0.0.1:1/test";
process.env.SUPABASE_URL = "http://127.0.0.1:1";
process.env.SUPABASE_ANON_KEY = "test-only";
process.env.SUPABASE_SERVICE_ROLE_KEY = "test-only";
process.env.JWT_SECRET = "test-only-placeholder-secret";
const { currentViennaDate, loadSmDashboardRows, loadSmHomeDashboard } = await import("./routes/sm-dashboard.js");
const { db, sql: client } = await import("./lib/db.js");
after(async () => { await client.end(); });
const dialect = new PgDialect();
const owner = "11111111-1111-4111-8111-111111111111";

test("phone metric reads bind the owner and Vienna bounds, and exclude unfinished/deleted/superseded results", async () => {
  const queries: ReturnType<PgDialect["sqlToQuery"]>[] = [];
  const executor = { execute: (async (query: SQL) => {
    queries.push(dialect.sqlToQuery(query));
    return [];
  }) as unknown as typeof db.execute };
  await loadSmDashboardRows({ from: "2026-08-31", to: "2026-08-31", smUserId: owner }, executor);
  assert.equal(queries.length, 2, "phone read must never load the admin filter directory");
  for (const query of queries) {
    assert.match(query.sql, /s\.status = 'submitted'/);
    assert.match(query.sql, /s\.is_current = true/);
    assert.match(query.sql, /s\.is_deleted = false/);
    assert.match(query.sql, /s\.reporting_available_at is not null/);
    assert.match(query.sql, /s\.sm_user_id = \$\d+::uuid/);
    assert.match(query.sql, /s\.submitted_at >=/);
    assert.match(query.sql, /s\.submitted_at </);
    assert.ok(query.params.includes(owner));
    assert.ok(query.params.includes("Europe/Vienna"));
    assert.ok(query.params.includes("2026-08-31"));
    assert.doesNotMatch(query.sql, new RegExp(owner));
  }
  assert.match(queries[1].sql, /q\.is_applicable = true/);
  assert.match(queries[1].sql, /a\.is_current = true/);
  assert.match(queries[1].sql, /a\.answer_state = 'answered'/);
  assert.match(queries[1].sql, /ao\.is_deleted = false/);
});

test("home read is one read-only consistent snapshot; count uses effective owner/date and excludes cancellations", async (context) => {
  const queries: ReturnType<PgDialect["sqlToQuery"]>[] = [];
  const visit = { submissionId: "s", marketId: "m", marketName: "SM Market", chain: "Billa", region: "Ost", smUserId: owner, smName: "SM Person" };
  const executor = { execute: (async (query: SQL) => {
    const compiled = dialect.sqlToQuery(query);
    queries.push(compiled);
    if (compiled.sql.includes("from users u")) return [{ userId: owner, name: "SM Person", assignmentsToday: 2 }];
    if (compiled.sql.includes("submissionQuestionId")) return [{
      ...visit, submissionQuestionId: "q", questionRootId: "root", role: "oos_detection",
      category: "action_placements", metricConfig: {}, outcome: "oos_absent",
    }];
    return [visit];
  }) as unknown as typeof db.execute };
  context.mock.method(db, "transaction", async (callback: (tx: typeof executor) => Promise<unknown>, config: unknown) => {
    assert.deepEqual(config, { isolationLevel: "repeatable read", accessMode: "read only" });
    return callback(executor);
  });
  const result = await loadSmHomeDashboard(owner, "2026-08-31");
  assert.equal(result?.assignmentsToday, 2);
  assert.equal(result?.name, "SM Person");
  assert.equal(result?.visits.withoutOos, 1);
  assert.equal(result?.visits.completed, 1);
  assert.equal(queries.length, 3);
  const count = queries[0];
  assert.match(count.sql, /a\.is_deleted = false/);
  assert.match(count.sql, /a\.status <> 'cancelled'/);
  assert.match(count.sql, /coalesce\(a\.replacement_sm_user_id, a\.original_sm_user_id\) = u\.id/);
  assert.match(count.sql, /coalesce\(a\.replacement_work_date, a\.original_work_date\)/);
  assert.match(count.sql, /u\.role = 'sm' and u\.is_active = true and u\.deleted_at is null/);
  assert.ok(count.params.includes(owner));
});

test("backend day bounds are Vienna-based, including UTC day crossover", () => {
  assert.equal(currentViennaDate(new Date("2026-08-31T22:30:00Z")), "2026-09-01");
  assert.equal(currentViennaDate(new Date("2026-01-31T22:30:00Z")), "2026-01-31");
});
