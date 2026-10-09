import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";

test("SMDurcharbeit: SM link migration preserves imported names and canonical market history without matching", async () => {
  const marketId = randomUUID(), userId = randomUUID();
  let sourceBefore: Record<string, unknown> | undefined;
  let marketBefore: Record<string, unknown> | undefined;
  let accessBefore: Record<string, unknown> | undefined;
  const f = await createSMDurcharbeitFixture({
    beforeSMDurcharbeitMarketSmUserMigration: async pg => {
      await pg.query("insert into users(id,first_name,last_name,role) values($1,'Synthetic','Planner','sm')", [userId]);
      await pg.query("insert into sm_markets(id,name,chain,address,postal_code,city,region,assigned_sm_user_id,created_at,updated_at) values($1,'Synthetic old market','Spar','Synthetic 1','1010','Wien','Ost',$2,'2026-09-01T10:00:00Z','2026-09-01T10:00:00Z')", [marketId, userId]);
      await pg.query("insert into sm_smdurcharbeit_markets(sm_market_id,smdurcharbeit_verplanung,smdurcharbeit_source_values,created_at) values($1,'Synthetic Planner',$2::jsonb,'2026-09-01T10:00:00Z')", [marketId, JSON.stringify({ Verplanung: "Synthetic Planner", Original: "Unchanged" })]);
      sourceBefore = (await pg.query("select * from sm_smdurcharbeit_markets where sm_market_id=$1", [marketId])).rows[0] as Record<string, unknown>;
      marketBefore = (await pg.query("select * from sm_markets where id=$1", [marketId])).rows[0] as Record<string, unknown>;
      accessBefore = (await pg.query("select relrowsecurity,relforcerowsecurity,relacl::text as acl from pg_class where oid='public.sm_smdurcharbeit_markets'::regclass")).rows[0] as Record<string, unknown>;
    },
  });
  try {
    const sourceAfter = (await f.pg.query("select * from sm_smdurcharbeit_markets where sm_market_id=$1", [marketId])).rows[0] as Record<string, unknown>;
    const { smdurcharbeit_sm_user_id: newLink, ...historicalSource } = sourceAfter;
    assert.equal(newLink, null, "Even an exact matching name is not backfilled by the schema migration");
    assert.deepEqual(historicalSource, sourceBefore);
    assert.deepEqual((await f.pg.query("select * from sm_markets where id=$1", [marketId])).rows[0], marketBefore);
    assert.deepEqual((await f.pg.query("select relrowsecurity,relforcerowsecurity,relacl::text as acl from pg_class where oid='public.sm_smdurcharbeit_markets'::regclass")).rows[0], accessBefore);
    const column = (await f.pg.query("select is_nullable,data_type,column_default from information_schema.columns where table_schema='public' and table_name='sm_smdurcharbeit_markets' and column_name='smdurcharbeit_sm_user_id'")).rows[0];
    assert.deepEqual(column, { is_nullable: "YES", data_type: "uuid", column_default: null });
    assert.equal((await f.pg.query("select count(*)::int as count from sm_smdurcharbeit_campaigns")).rows[0]!.count, 0);
    assert.equal((await f.pg.query("select count(*)::int as count from sm_smdurcharbeit_month_targets")).rows[0]!.count, 0);
  } finally { await f.pg.close(); }
});

test("SMDurcharbeit: saved SM link rejects unknown accounts and prevents cascading loss", async () => {
  const f = await createSMDurcharbeitFixture();
  const linkedUser = randomUUID();
  const foreignKeyError = (error: unknown) => (error as { code?: string; constraint_name?: string; constraint?: string }).code === "23503"
    && ((error as { constraint_name?: string; constraint?: string }).constraint_name ?? (error as { constraint?: string }).constraint) === "sm_smdurcharbeit_market_sm_user_fk";
  try {
    await f.pg.query("insert into users(id,first_name,last_name,role) values($1,'Synthetic','Linked SM','sm')", [linkedUser]);
    await f.pg.query("insert into sm_smdurcharbeit_markets(sm_market_id,smdurcharbeit_verplanung) values($1,'Original imported name')", [f.market]);
    await assert.rejects(f.pg.query("update sm_smdurcharbeit_markets set smdurcharbeit_sm_user_id=$1 where sm_market_id=$2", [randomUUID(), f.market]), foreignKeyError);
    await f.pg.query("update sm_smdurcharbeit_markets set smdurcharbeit_sm_user_id=$1 where sm_market_id=$2", [linkedUser, f.market]);
    await assert.rejects(f.pg.query("delete from users where id=$1", [linkedUser]), foreignKeyError);
    const source = (await f.database.select().from(f.schema.smSMDurcharbeitMarkets))[0]!;
    assert.equal(source.SMDurcharbeitSmUserId, linkedUser);
    assert.equal(source.SMDurcharbeitVerplanung, "Original imported name");
    const index = (await f.pg.query("select indisvalid,indisready from pg_index where indexrelid='public.sm_smdurcharbeit_market_sm_user_idx'::regclass")).rows[0];
    assert.deepEqual(index, { indisvalid: true, indisready: true });
  } finally { await f.pg.close(); }
});
