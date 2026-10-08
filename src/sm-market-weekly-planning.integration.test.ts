import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import express from "express";
import request from "supertest";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import * as schema from "./lib/schema.js";
import { smMarketWeekdayColumns, smMarketWeekdayHoursSchema } from "./sm-market-weekly-planning.shared.js";

// Guard against production access even if a future change introduces a new database/auth call.
Object.assign(process.env, {
  NODE_ENV: "test", DATABASE_URL: "postgresql://test:test@127.0.0.1:1/disabled",
  SUPABASE_URL: "http://127.0.0.1:1", SUPABASE_ANON_KEY: "test", SUPABASE_SERVICE_ROLE_KEY: "test",
  JWT_SECRET: "sm-weekly-planning-local-tests", BYPASS_AUTH_FOR_TESTS: "1", BYPASS_AUTH_ROLE: "sm_admin",
});
const { db, sql: remoteSql } = await import("./lib/db.js");
const { adminSmMarketsRouter } = await import("./routes/sm-markets.js");

const marketId = "10000000-0000-4000-8000-000000000001";
const blankWeek = { mo: null, di: null, mi: null, do: null, fr: null };

async function localFixture() {
  const pg = new PGlite();
  const localDb = drizzle(pg, { schema });
  await pg.exec(`create role anon; create role authenticated; create role service_role;
    create table users(id uuid primary key);
    create table sm_assignments(id uuid primary key, sm_market_id uuid, work_date date, planned_minutes integer);
    insert into users values ('20000000-0000-4000-8000-000000000001'), ('20000000-0000-4000-8000-000000000002');`);
  for (const migration of ["0088_sm_markets", "0092_sm_market_assignments", "0108_sm_market_account_assignments"]) {
    await pg.exec(await readFile(new URL(`../drizzle/${migration}.sql`, import.meta.url), "utf8"));
  }
  for (const migration of ["20261007133647_SMDurcharbeit_market_registry.sql", "20261008125753_SMDurcharbeit_market_import.sql"]) await pg.exec(await readFile(new URL(`../supabase/migrations/${migration}`, import.meta.url), "utf8"));
  await pg.query(`insert into sm_markets(id,internal_market_id,name,chain,address,postal_code,city,region,
    thursday_hours,shelf_merchandiser_name,field_service_manager_name,assigned_sm_user_id,
    field_service_manager_user_id,source_info,admin_info_note,import_source_file_name,updated_at)
    values ($1,'1201131807','Interspar','Interspar','Bodenzeile 3','2230','Gänserndorf','Ost',2,
      'SM Local','GM Local','20000000-0000-4000-8000-000000000001',
      '20000000-0000-4000-8000-000000000002','Originalinfo','Admininfo','markets.xlsx','2026-09-01T00:00:00Z')`, [marketId]);
  await pg.query("insert into sm_assignments values ('30000000-0000-4000-8000-000000000001',$1,'2026-10-01',120)", [marketId]);

  // Swap only this isolated test process's database methods, not application config or production.
  const select = db.select;
  const transaction = db.transaction;
  db.select = localDb.select.bind(localDb) as unknown as typeof db.select;
  db.transaction = localDb.transaction.bind(localDb) as unknown as typeof db.transaction;
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async () => { throw new Error("Network access forbidden in SM weekly planning fixture"); };
  const app = express();
  app.use(express.json());
  app.use("/admin/sm-markets", adminSmMarketsRouter);
  app.use((_error: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
    res.status(500).json({ error: "Local fixture failure" });
  });
  return {
    app, pg,
    close: async () => {
      db.select = select;
      db.transaction = transaction;
      globalThis.fetch = originalFetch;
      await pg.close();
      await remoteSql.end({ timeout: 1 });
    },
  };
}

test("complete weekday payload is validated and maps only writable daily columns", () => {
  assert.deepEqual(smMarketWeekdayColumns({ ...blankWeek, di: 2.75 }), {
    mondayHours: null, tuesdayHours: "2.75", wednesdayHours: null, thursdayHours: null, fridayHours: null,
  });
  for (const invalid of [0, -1, 24.01, 0.001, Infinity, "2", "", undefined]) {
    assert.equal(smMarketWeekdayHoursSchema.safeParse({ ...blankWeek, mo: invalid }).success, false);
  }
  assert.equal(smMarketWeekdayHoursSchema.safeParse({ di: 2 }).success, false);
  assert.equal(smMarketWeekdayHoursSchema.safeParse({ ...blankWeek, saturday: 2 }).success, false);
  assert.equal(smMarketWeekdayHoursSchema.safeParse({ ...blankWeek, mo: 0.01, fr: 24 }).success, true);
});

test("SM admin market weekly plan HTTP roundtrip in disposable local Postgres", async (t) => {
  const fixture = await localFixture();
  const api = request(fixture.app);
  const patch = (body: object, id = marketId) => api.patch(`/admin/sm-markets/${id}`).send(body);
  try {
    const initial = (await api.get("/admin/sm-markets").expect(200)).body.markets[0];
    const assignmentsBefore = (await fixture.pg.query("select * from sm_assignments")).rows;
    let current = initial;
    await t.test("move Thursday to Tuesday and derive totals on save/reload without touching other fields", async () => {
      const response = await patch({ weekdayHours: { ...blankWeek, di: 2.5 }, expectedUpdatedAt: initial.updatedAt }).expect(200);
      current = response.body.market;
      assert.deepEqual(current.weekdayHours, { di: 2.5 });
      assert.equal(current.serviceDaysPerWeek, 1);
      assert.equal(current.weeklyHours, 2.5);
      const stableFields = ["id", "internalId", "name", "chain", "address", "postalCode", "city", "region", "assignedSmUserId", "fieldServiceManagerUserId", "sourceInfo", "infoNote", "isActive", "importSourceFileName"];
      for (const field of stableFields) assert.deepEqual(current[field], initial[field], field);
      assert.deepEqual((await api.get("/admin/sm-markets").expect(200)).body.markets[0], current);
      assert.deepEqual((await fixture.pg.query("select * from sm_assignments")).rows, assignmentsBefore);
    });
    await t.test("a concurrent edit returns 409 and keeps the saved plan", async () => {
      const response = await patch({ weekdayHours: { ...blankWeek, fr: 4 }, expectedUpdatedAt: initial.updatedAt }).expect(409);
      assert.equal(response.body.code, "sm_market_plan_changed");
      assert.deepEqual((await api.get("/admin/sm-markets").expect(200)).body.markets[0], current);
    });
    await t.test("several days preserve decimal precision and return generated summaries", async () => {
      current = (await patch({ weekdayHours: { ...blankWeek, mo: 0.1, di: 0.2, fr: 24 }, expectedUpdatedAt: current.updatedAt }).expect(200)).body.market;
      assert.equal(current.serviceDaysPerWeek, 3);
      assert.equal(current.weeklyHours, 24.3);
    });
    await t.test("ordinary market-info edits do not replace the weekly plan", async () => {
      const response = await patch({ adminInfoNote: "Updated note" }).expect(200);
      assert.deepEqual(response.body.market.weekdayHours, current.weekdayHours);
      assert.equal(response.body.market.weeklyHours, 24.3);
    });
    await t.test("invalid, incomplete, client-supplied totals and metadata-only patches are rejected", async () => {
      for (const body of [
        { weekdayHours: { di: 2 } }, { weekdayHours: { ...blankWeek, di: 0 } },
        { weekdayHours: { ...blankWeek, mo: 1.001 } }, { weekdayHours: { ...blankWeek, mo: 25 } },
        { weeklyHours: 100 }, { serviceDaysPerWeek: 1 }, { expectedUpdatedAt: current.updatedAt }, {},
      ]) await patch(body).expect(400);
      assert.deepEqual((await api.get("/admin/sm-markets").expect(200)).body.markets[0].weekdayHours, current.weekdayHours);
    });
    await t.test("all days can be cleared without changing existing scheduled assignments", async () => {
      const response = await patch({ weekdayHours: blankWeek }).expect(200);
      assert.deepEqual(response.body.market.weekdayHours, {});
      assert.equal(response.body.market.serviceDaysPerWeek, 0);
      assert.equal(response.body.market.weeklyHours, 0);
      assert.deepEqual((await fixture.pg.query("select * from sm_assignments")).rows, assignmentsBefore);
    });
    if (process.env.SM_WEEKLY_PLANNING_BROWSER_TEST === "1") {
      await t.test("real browser edits, saves, reloads, cancels and handles conflicts without production access", async () => {
        const { verifySmWeeklyPlanningBrowser } = await import("./sm-market-weekly-planning.browser.test.js");
        await verifySmWeeklyPlanningBrowser(fixture.app, fixture.pg, marketId);
      });
    }
    await t.test("unknown or soft-deleted markets cannot be changed", async () => {
      await patch({ weekdayHours: blankWeek }, "40000000-0000-4000-8000-000000000001").expect(404);
      await fixture.pg.query("update sm_markets set is_deleted = true where id = $1", [marketId]);
      await patch({ weekdayHours: blankWeek }).expect(404);
    });
    await t.test("the actual admin endpoint still requires authentication outside the local bypass", async () => {
      process.env.BYPASS_AUTH_FOR_TESTS = "0";
      try { await patch({ weekdayHours: blankWeek }).expect(401); }
      finally { process.env.BYPASS_AUTH_FOR_TESTS = "1"; }
    });
  } finally {
    await fixture.close();
  }
});
