import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { eq, sql } from "drizzle-orm";
import * as schema from "./lib/schema.js";

// Never connect this suite to Supabase. All SQL runs inside disposable in-memory Postgres.
Object.assign(process.env, { NODE_ENV: "test", DATABASE_URL: "postgresql://test:test@127.0.0.1:1/disabled", SUPABASE_URL: "http://127.0.0.1:1", SUPABASE_ANON_KEY: "test", SUPABASE_SERVICE_ROLE_KEY: "test", JWT_SECRET: "local-series-test-only" });
const { applySmSeriesChange, previewSmSeriesChange, getSmSeriesDetails, SmSeriesError, smSeriesChangeSchema } = await import("./sm-series-management.js");
const { smAssignments: assignments, smAssignmentSeries: series, smAssignmentSeriesVersions: versions, smAssignmentEvents: events } = schema;
const actor = randomUUID(), oldSm = randomUUID(), newSm = randomUUID(), market = randomUUID();
const today = "2026-09-14";

test("SM series persistence against the actual planning migration (no production access)", async (t) => {
  const pg = new PGlite();
  const database = drizzle(pg, { schema });
  await pg.exec(`create role anon; create role authenticated; create role service_role;
    create table users(id uuid primary key,role text,is_active boolean default true,deleted_at timestamptz);
    create table sm_markets(id uuid primary key,internal_market_id text,is_active boolean default true,is_deleted boolean default false);
    create table sm_questionnaire_versions(id uuid primary key);
    create table sm_questionnaire_submissions(id uuid primary key default gen_random_uuid(),assignment_id uuid,status text);
    create function sm_sync_soft_delete_fields() returns trigger language plpgsql as $$ begin new.deleted_at := case when new.is_deleted then coalesce(new.deleted_at,now()) else null end; return new; end $$;
    create function sm_reject_hard_delete() returns trigger language plpgsql as $$ begin raise exception 'SM hard delete forbidden'; end $$;`);
  await pg.exec(await readFile(new URL("../drizzle/0093_sm_planning.sql", import.meta.url), "utf8"));
  await pg.query("insert into users(id,role) values ($1,'sm_admin'),($2,'sm'),($3,'sm');", [actor, oldSm, newSm]);
  await pg.query("insert into sm_markets(id,internal_market_id) values ($1,'SM-LOCAL-123')", [market]);
  const txRun = async <T,>(fn: (tx: Parameters<typeof applySmSeriesChange>[0]) => Promise<T>) => database.transaction((tx) => fn(tx as unknown as Parameters<typeof applySmSeriesChange>[0]));
  const seed = async (days = ["2026-09-14", "2026-09-21", "2026-09-28"]) => {
    const [root] = await database.insert(series).values({ idempotencyKey: randomUUID(), createdByUserId: actor }).returning();
    const [version] = await database.insert(versions).values({ seriesId: root!.id, versionNumber: 1, effectiveFromDate: "2026-09-07", validFrom: "2026-09-07", validTo: "2026-09-28", smMarketId: market, marketInternalIdSnapshot: "SM-LOCAL-123", defaultSmUserId: oldSm, plannedMinutes: 60, frequency: "weekly", weekdays: [1], createdByUserId: actor }).returning();
    const rows = await database.insert(assignments).values(days.map((day) => ({ sourceType: "series" as const, seriesId: root!.id, seriesVersionId: version!.id, seriesOccurrenceKey: day, idempotencyKey: randomUUID(), originalWorkDate: day, originalSmUserId: oldSm, originalSmMarketId: market, originalMarketInternalId: "SM-LOCAL-123", originalPlannedMinutes: 60, createdByUserId: actor, updatedByUserId: actor }))).returning();
    return { id: root!.id, rows, version: version! };
  };
  const edit = { action: "edit" as const, effectiveFromDate: today, smMarketId: market, smUserId: newSm, plannedMinutes: 90, frequency: "weekly" as const, weekdays: [1], validTo: "2026-10-05" };
  const apply = async (id: string, change: Parameters<typeof previewSmSeriesChange>[2]) => {
    const preview = await txRun((tx) => previewSmSeriesChange(tx, id, change, today));
    return txRun((tx) => applySmSeriesChange(tx, id, { change, previewToken: preview.previewToken, reason: "Lokaler Regressionstest" }, actor, today));
  };
  try {
    await t.test("edit is versioned, original IDs/values survive, new dates are materialized and audited", async () => {
      const fixture = await seed();
      const result = await apply(fixture.id, edit);
      assert.equal(result.updateCount, 3); assert.equal(result.createCount, 1);
      const rows = await database.select().from(assignments).where(eq(assignments.seriesId, fixture.id));
      for (const old of fixture.rows) { const current = rows.find((r) => r.id === old.id)!; assert.equal(current.originalSmUserId, oldSm); assert.equal(current.replacementSmUserId, newSm); assert.equal(current.seriesVersionId, fixture.version.id); assert.equal(current.replacementPlannedMinutes, 90); }
      assert.equal((await txRun((tx) => getSmSeriesDetails(tx, fixture.id))).versionNumber, 2);
      assert.equal((await database.select().from(events).where(eq(events.seriesId, fixture.id))).length, 4);
    });
    await t.test("changing weekday and changing back restores only schedule-removed rows", async () => {
      const fixture = await seed();
      await apply(fixture.id, { ...edit, weekdays: [2], validTo: "2026-09-28" });
      const result = await apply(fixture.id, { ...edit, validTo: "2026-09-28" });
      assert.equal(result.restoreCount, 3); assert.equal(result.cancelCount, 2);
      const rows = await database.select().from(assignments).where(eq(assignments.seriesId, fixture.id));
      assert.ok(fixture.rows.every((row) => rows.some((r) => r.id === row.id && r.status === "planned")));
    });
    await t.test("stop protects submitted/completed history and is not repeatable", async () => {
      const fixture = await seed();
      await database.update(assignments).set({ status: "completed", startedAt: new Date("2026-09-14T07:00:00Z"), completedAt: new Date("2026-09-14T08:00:00Z") }).where(eq(assignments.id, fixture.rows[0]!.id));
      await pg.query("insert into sm_questionnaire_submissions(assignment_id,status) values ($1,'submitted')", [fixture.rows[1]!.id]);
      const change = { action: "stop" as const, effectiveFromDate: today };
      const result = await apply(fixture.id, change);
      assert.equal(result.cancelCount, 1); assert.equal(result.protectedCount, 2);
      assert.equal((await txRun((tx) => getSmSeriesDetails(tx, fixture.id))).status, "ended");
      assert.equal((await database.select().from(assignments).where(eq(assignments.id, fixture.rows[0]!.id)))[0]!.status, "completed");
      await assert.rejects(apply(fixture.id, change), (e: unknown) => e instanceof SmSeriesError && e.code === "sm_series_ended");
    });
    await t.test("stale preview rejects before writes if a visit starts", async () => {
      const fixture = await seed();
      const preview = await txRun((tx) => previewSmSeriesChange(tx, fixture.id, edit, today));
      await database.update(assignments).set({ status: "in_progress", startedAt: new Date() }).where(eq(assignments.id, fixture.rows[0]!.id));
      await assert.rejects(txRun((tx) => applySmSeriesChange(tx, fixture.id, { change: edit, previewToken: preview.previewToken, reason: "Test Änderung" }, actor, today)), (e: unknown) => e instanceof SmSeriesError && e.code === "sm_series_preview_stale");
      assert.equal((await database.select().from(versions).where(eq(versions.seriesId, fixture.id))).length, 1);
      assert.equal((await database.select().from(events).where(eq(events.seriesId, fixture.id))).length, 0);
    });
    await t.test("audit write failure rolls cancellations and version changes back", async () => {
      const fixture = await seed();
      await pg.exec(`create function reject_test_audit() returns trigger language plpgsql as $$ begin raise exception 'Injected audit failure'; end $$; create trigger reject_test_audit before insert on sm_assignment_events for each row execute function reject_test_audit();`);
      await assert.rejects(apply(fixture.id, { ...edit, weekdays: [2] }));
      await pg.exec("drop trigger reject_test_audit on sm_assignment_events; drop function reject_test_audit();");
      const rows = await database.select().from(assignments).where(eq(assignments.seriesId, fixture.id));
      assert.ok(rows.every((row) => row.status === "planned" && row.replacementSmUserId === null));
      assert.equal((await database.select().from(versions).where(eq(versions.seriesId, fixture.id))).length, 1);
    });
    await t.test("GM targets and past edits are rejected, strict payload rejects extra fields", async () => {
      const fixture = await seed();
      await assert.rejects(apply(fixture.id, { ...edit, smUserId: actor }), (e: unknown) => e instanceof SmSeriesError && e.code === "sm_series_target_invalid");
      await assert.rejects(apply(fixture.id, { ...edit, effectiveFromDate: "2026-09-13" }), (e: unknown) => e instanceof SmSeriesError && e.code === "sm_series_past");
      assert.equal(smSeriesChangeSchema.safeParse({ ...edit, gmMarketId: market }).success, false);
      await assert.rejects(apply(fixture.id, { ...edit, weekdays: [2], validTo: today }), (e: unknown) => e instanceof SmSeriesError && e.code === "sm_series_empty");
      await assert.rejects(apply(fixture.id, { ...edit, weekdays: [1,2,3,4,5,6,7], validTo: "2029-01-01" }), (e: unknown) => e instanceof SmSeriesError && e.code === "sm_series_range");
    });
    await t.test("history under a plan-like status prevents edits including old time revisions", async () => {
      const fixture = await seed();
      await database.insert(schema.smAssignmentTimeSubmissions).values({ assignmentId: fixture.rows[0]!.id, revisionNumber: 1, actualMinutes: 30, isCurrent: false, submittedByUserId: oldSm });
      const result = await apply(fixture.id, edit); assert.equal(result.protectedCount, 1); assert.equal(result.updateCount, 2);
    });
    await t.test("the database still rejects hard deletes and original-field changes", async () => {
      const fixture = await seed();
      await assert.rejects(database.update(assignments).set({ originalWorkDate: "2026-10-01" }).where(eq(assignments.id, fixture.rows[0]!.id)));
      await assert.rejects(database.execute(sql`delete from ${assignments} where ${assignments.id}=${fixture.rows[0]!.id}`));
    });
    await t.test("new holiday dates cannot be shifted before the selected cutoff", async () => {
      const fixture = await seed();
      const result = await apply(fixture.id, { ...edit, effectiveFromDate: "2026-12-25", validTo: "2026-12-25", weekdays: [5] });
      assert.equal(result.createCount, 1); assert.equal(result.holidayAdjustedCount, 1);
      const rows = await database.select().from(assignments).where(eq(assignments.seriesId, fixture.id));
      assert.equal(rows.find((row) => row.originalWorkDate === "2026-12-25")!.replacementWorkDate, "2026-12-28");
    });
    await t.test("manual cancellations stay cancelled and inactive targets fail validation", async () => {
      const fixture = await seed();
      await database.update(assignments).set({ status: "cancelled", statusBeforeCancellation: "planned", cancelledAt: new Date(), cancelledByUserId: actor, cancellationReason: "Manuelle Absage" }).where(eq(assignments.id, fixture.rows[0]!.id));
      const result = await apply(fixture.id, edit); assert.equal(result.restoreCount, 0); assert.equal(result.protectedCount, 1);
      await pg.query("update users set is_active=false where id=$1", [newSm]);
      await assert.rejects(apply(fixture.id, edit), (e: unknown) => e instanceof SmSeriesError && e.code === "sm_series_target_invalid");
      await pg.query("update users set is_active=true where id=$1", [newSm]);
    });
    await t.test("no GM tables exist in this fixture: all service mutations remain SM-only", async () => {
      const tables = await pg.query<{tablename:string}>("select tablename from pg_tables where schemaname='public'");
      assert.ok(tables.rows.every((row) => row.tablename === "users" || row.tablename.startsWith("sm_")));
    });
  } finally { await pg.close(); }
});
