import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { and, eq } from "drizzle-orm";
import * as schema from "./lib/schema.js";
import { allocateCampaignVisitProgress, allocateCompletedCampaignVisits, CampaignVisitReassignmentError, loadCampaignVisitCompletionCounts, loadCampaignVisitProgress, planCampaignVisitMove, reassignCampaignVisitTarget } from "./campaign-visit-reassignment.js";

type DbTx = Parameters<typeof reassignCampaignVisitTarget>[0];

test("reassigns exactly one planned visit when several targets share a row", () => {
  assert.deepEqual(planCampaignVisitMove({ visitTargetCount: 3, currentVisitsCount: 0, expectedVisitTargetCount: 3, visitNumber: 2 }), {
    ok: true, remainingSourceCount: 2,
  });
});

test("removes a source assignment only when its final planned visit moves", () => {
  assert.deepEqual(planCampaignVisitMove({ visitTargetCount: 1, currentVisitsCount: 0, expectedVisitTargetCount: 1, visitNumber: 1 }), {
    ok: true, remainingSourceCount: null,
  });
});

test("protects already completed targets and rejects a stale row", () => {
  assert.deepEqual(planCampaignVisitMove({ visitTargetCount: 3, currentVisitsCount: 1, expectedVisitTargetCount: 3, visitNumber: 1 }), { ok: false, reason: "completed" });
  assert.deepEqual(planCampaignVisitMove({ visitTargetCount: 2, currentVisitsCount: 0, expectedVisitTargetCount: 3, visitNumber: 2 }), { ok: false, reason: "changed" });
  assert.deepEqual(planCampaignVisitMove({ visitTargetCount: 2, currentVisitsCount: 0, expectedVisitTargetCount: 2, visitNumber: 3 }), { ok: false, reason: "changed" });
});

test("submitted visits, including atomic bulk GM swaps, determine completed slots", () => {
  const at = new Date("2026-09-20T12:00:00Z");
  const counts = allocateCompletedCampaignVisits([
    { id: "a", marketId: "m", gmUserId: "alex", assignmentSlot: 1, visitTargetCount: 2, currentVisitsCount: 0 },
    { id: "b", marketId: "m", gmUserId: "pascal", assignmentSlot: 2, visitTargetCount: 2, currentVisitsCount: 0 },
  ], [
    { sessionId: "visit-1", marketId: "m", gmUserId: "alex", submittedAt: new Date("2026-09-19T10:00:00Z") },
    { sessionId: "visit-1", marketId: "m", gmUserId: "alex", submittedAt: new Date("2026-09-19T10:00:00Z") },
  ], [
    { marketId: "m", fromGmUserId: "alex", toGmUserId: "pascal", migratedAt: at },
    { marketId: "m", fromGmUserId: "pascal", toGmUserId: "alex", migratedAt: at },
  ]);
  assert.equal(counts.get("a"), 0);
  assert.equal(counts.get("b"), 1);
});

test("drafts protect the next open visit without consuming another planned visit", () => {
  const progress = allocateCampaignVisitProgress([
    { id: "a", marketId: "m", gmUserId: "alex", assignmentSlot: 1, visitTargetCount: 3, currentVisitsCount: 0 },
  ], [
    { sessionId: "completed", marketId: "m", gmUserId: "alex", submittedAt: new Date("2026-09-20T10:00:00Z") },
  ], [
    { sessionId: "draft", marketId: "m", gmUserId: "alex" },
  ], []);
  assert.equal(progress.completedByAssignmentId.get("a"), 1);
  assert.equal(progress.startedByAssignmentId.get("a"), 1);
});

test("single visit GM move is atomic against isolated in-memory PostgreSQL", async () => {
  const pg = new PGlite();
  const database = drizzle(pg, { schema });
  const transaction = <T>(action: (tx: DbTx) => Promise<T>) => database.transaction((tx) => action(tx as unknown as DbTx));
  try {
    await pg.exec(`
      create table campaigns (id uuid primary key, section text not null, is_deleted boolean not null default false);
      create table campaign_market_assignments (
        id uuid primary key default gen_random_uuid(), campaign_id uuid not null, market_id uuid not null,
        assigned_at timestamptz not null default now(), gm_user_id uuid, assignment_slot integer not null default 1,
        visit_target_count integer not null default 1 check (visit_target_count > 0), current_visits_count integer not null default 0,
        assigned_by_user_id uuid, is_deleted boolean not null default false, deleted_at timestamptz,
        created_at timestamptz not null default now(), updated_at timestamptz not null default now());
      create unique index campaign_market_assignments_gm_unique on campaign_market_assignments(campaign_id, market_id, gm_user_id, assignment_slot) where not is_deleted;
      create table campaign_market_assignment_history (
        id uuid primary key default gen_random_uuid(), market_id uuid not null, section text not null,
        from_campaign_id uuid not null, to_campaign_id uuid not null, from_gm_user_id uuid, to_gm_user_id uuid,
        migrated_by_user_id uuid, migrated_at timestamptz not null default now(), reason text,
        created_at timestamptz not null default now());
      create table visit_sessions (id uuid primary key, market_id uuid not null, gm_user_id uuid not null,
        submitted_at timestamptz, kuehler_unit_id uuid, is_deleted boolean not null default false, status text not null);
      create table visit_session_sections (id uuid primary key, visit_session_id uuid not null,
        campaign_id uuid, section text not null, is_deleted boolean not null default false);
    `);
    const campaignId = randomUUID(), marketId = randomUUID(), alex = randomUUID(), pascal = randomUUID(), auditor = randomUUID();
    const assignmentId = randomUUID(), visitId = randomUUID();
    await pg.query("insert into campaigns(id,section) values ($1,'standard')", [campaignId]);
    await pg.query("insert into campaign_market_assignments(id,campaign_id,market_id,gm_user_id,visit_target_count) values ($1,$2,$3,$4,3)", [assignmentId, campaignId, marketId, alex]);
    await pg.query("insert into visit_sessions(id,market_id,gm_user_id,status,submitted_at) values ($1,$2,$3,'submitted',$4)", [visitId, marketId, alex, "2026-09-20T10:00:00Z"]);
    await pg.query("insert into visit_session_sections(id,visit_session_id,campaign_id,section) values ($1,$2,$3,'standard')", [randomUUID(), visitId, campaignId]);
    const input = { campaignId, assignmentId, toGmUserId: pascal, expectedGmUserId: alex,
      expectedVisitTargetCount: 3, visitNumber: 2, auditUserId: auditor, now: new Date("2026-09-21T10:00:00Z") };

    const before = await transaction((tx) => loadCampaignVisitCompletionCounts(tx, campaignId));
    assert.equal(before.get(assignmentId), 1);
    await assert.rejects(transaction((tx) => reassignCampaignVisitTarget(tx, { ...input, visitNumber: 1 })),
      (error: unknown) => error instanceof CampaignVisitReassignmentError && error.code === "visit_already_completed");
    await transaction((tx) => reassignCampaignVisitTarget(tx, input));
    const assignments = await database.select().from(schema.campaignMarketAssignments)
      .where(and(eq(schema.campaignMarketAssignments.campaignId, campaignId), eq(schema.campaignMarketAssignments.isDeleted, false)));
    assert.equal(assignments.length, 2);
    assert.equal(assignments.find((row) => row.gmUserId === alex)?.visitTargetCount, 2);
    assert.equal(assignments.find((row) => row.gmUserId === pascal)?.visitTargetCount, 1);
    const after = await transaction((tx) => loadCampaignVisitCompletionCounts(tx, campaignId));
    assert.equal(after.get(assignmentId), 1);
    assert.equal((await database.select().from(schema.campaignMarketAssignmentHistory)).length, 1);
    assert.equal((await database.select({ gmUserId: schema.visitSessions.gmUserId }).from(schema.visitSessions).where(eq(schema.visitSessions.id, visitId)))[0]?.gmUserId, alex);
    await assert.rejects(transaction((tx) => reassignCampaignVisitTarget(tx, input)),
      (error: unknown) => error instanceof CampaignVisitReassignmentError && error.code === "assignment_changed");
    assert.equal((await database.select().from(schema.campaignMarketAssignmentHistory)).length, 1);

    await transaction((tx) => reassignCampaignVisitTarget(tx, { ...input, expectedVisitTargetCount: 2, visitNumber: 2 }));
    const merged = await database.select({ gmUserId: schema.campaignMarketAssignments.gmUserId, visitTargetCount: schema.campaignMarketAssignments.visitTargetCount })
      .from(schema.campaignMarketAssignments).where(and(eq(schema.campaignMarketAssignments.campaignId, campaignId), eq(schema.campaignMarketAssignments.isDeleted, false)));
    assert.equal(merged.find((row) => row.gmUserId === alex)?.visitTargetCount, 1);
    assert.equal(merged.find((row) => row.gmUserId === pascal)?.visitTargetCount, 2);

    const otherMarket = randomUUID(), lastAssignment = randomUUID();
    await pg.query("insert into campaign_market_assignments(id,campaign_id,market_id,gm_user_id,visit_target_count) values ($1,$2,$3,$4,1)", [lastAssignment, campaignId, otherMarket, alex]);
    const draftId = randomUUID();
    await pg.query("insert into visit_sessions(id,market_id,gm_user_id,status) values ($1,$2,$3,'draft')", [draftId, otherMarket, alex]);
    await pg.query("insert into visit_session_sections(id,visit_session_id,campaign_id,section) values ($1,$2,$3,'standard')", [randomUUID(), draftId, campaignId]);
    const draftProgress = await transaction((tx) => loadCampaignVisitProgress(tx, campaignId));
    assert.equal(draftProgress.startedByAssignmentId.get(lastAssignment), 1);
    await assert.rejects(transaction((tx) => reassignCampaignVisitTarget(tx, { ...input, assignmentId: lastAssignment, expectedVisitTargetCount: 1, visitNumber: 1 })),
      (error: unknown) => error instanceof CampaignVisitReassignmentError && error.code === "visit_already_started");
    await pg.query("update visit_sessions set status = 'cancelled' where id = $1", [draftId]);
    await pg.exec("create function reject_visit_audit() returns trigger language plpgsql as $$ begin raise exception 'audit unavailable'; end $$; create trigger reject_visit_audit before insert on campaign_market_assignment_history for each row execute function reject_visit_audit();");
    await assert.rejects(transaction((tx) => reassignCampaignVisitTarget(tx, { ...input, assignmentId: lastAssignment, expectedVisitTargetCount: 1, visitNumber: 1 })));
    assert.equal((await database.select({ visitTargetCount: schema.campaignMarketAssignments.visitTargetCount })
      .from(schema.campaignMarketAssignments).where(eq(schema.campaignMarketAssignments.id, lastAssignment)))[0]?.visitTargetCount, 1);
    await pg.exec("drop trigger reject_visit_audit on campaign_market_assignment_history; drop function reject_visit_audit();");
    await transaction((tx) => reassignCampaignVisitTarget(tx, { ...input, assignmentId: lastAssignment, expectedVisitTargetCount: 1, visitNumber: 1 }));
    assert.equal((await database.select({ isDeleted: schema.campaignMarketAssignments.isDeleted })
      .from(schema.campaignMarketAssignments).where(eq(schema.campaignMarketAssignments.id, lastAssignment)))[0]?.isDeleted, true);
    assert.equal((await database.select().from(schema.campaignMarketAssignmentHistory)).length, 3);

    const flexCampaign = randomUUID(), flexAssignment = randomUUID();
    await pg.query("insert into campaigns(id,section) values ($1,'flex')", [flexCampaign]);
    await pg.query("insert into campaign_market_assignments(id,campaign_id,market_id,visit_target_count) values ($1,$2,$3,1)",
      [flexAssignment, flexCampaign, marketId]);
    await assert.rejects(transaction((tx) => reassignCampaignVisitTarget(tx, {
      ...input, campaignId: flexCampaign, assignmentId: flexAssignment, expectedGmUserId: null, expectedVisitTargetCount: 1, visitNumber: 1,
    })), (error: unknown) => error instanceof CampaignVisitReassignmentError && error.code === "visit_type_not_supported");

    const unassignedMarket = randomUUID(), unassignedAssignment = randomUUID(), unassignedVisit = randomUUID();
    await pg.query("insert into campaign_market_assignments(id,campaign_id,market_id,visit_target_count) values ($1,$2,$3,2)",
      [unassignedAssignment, campaignId, unassignedMarket]);
    await pg.query("insert into visit_sessions(id,market_id,gm_user_id,status,submitted_at) values ($1,$2,$3,'submitted',$4)",
      [unassignedVisit, unassignedMarket, alex, "2026-09-22T10:00:00Z"]);
    await pg.query("insert into visit_session_sections(id,visit_session_id,campaign_id,section) values ($1,$2,$3,'standard')",
      [randomUUID(), unassignedVisit, campaignId]);
    assert.equal((await transaction((tx) => loadCampaignVisitProgress(tx, campaignId))).completedByAssignmentId.get(unassignedAssignment), 2);
    await assert.rejects(transaction((tx) => reassignCampaignVisitTarget(tx, { ...input, assignmentId: unassignedAssignment,
      expectedGmUserId: null, expectedVisitTargetCount: 2, visitNumber: 2 })),
    (error: unknown) => error instanceof CampaignVisitReassignmentError && error.code === "visit_already_completed");
  } finally {
    await pg.close();
  }
});

test("Kühler moves only an untouched complete device round and preserves submitted rounds", async () => {
  const pg = new PGlite();
  const database = drizzle(pg, { schema });
  const transaction = <T>(action: (tx: DbTx) => Promise<T>) => database.transaction((tx) => action(tx as unknown as DbTx));
  try {
    await pg.exec(`
      create table campaigns (id uuid primary key, section text not null, is_deleted boolean not null default false);
      create table campaign_market_assignments (
        id uuid primary key default gen_random_uuid(), campaign_id uuid not null, market_id uuid not null,
        assigned_at timestamptz not null default now(), gm_user_id uuid, assignment_slot integer not null default 1,
        visit_target_count integer not null default 1, current_visits_count integer not null default 0,
        assigned_by_user_id uuid, is_deleted boolean not null default false, deleted_at timestamptz,
        created_at timestamptz not null default now(), updated_at timestamptz not null default now());
      create unique index assignments_gm_slot_unique on campaign_market_assignments(campaign_id,market_id,gm_user_id,assignment_slot) where not is_deleted;
      create table campaign_market_assignment_history (
        id uuid primary key default gen_random_uuid(), market_id uuid not null, section text not null,
        from_campaign_id uuid not null, to_campaign_id uuid not null, from_gm_user_id uuid, to_gm_user_id uuid,
        migrated_by_user_id uuid, migrated_at timestamptz not null default now(), reason text,
        created_at timestamptz not null default now());
      create table market_kuehler_units (id uuid primary key, market_id uuid not null,
        kuehler_internal_id text, is_deleted boolean not null default false, created_at timestamptz not null default now());
      create table visit_sessions (id uuid primary key, market_id uuid not null, gm_user_id uuid not null,
        submitted_at timestamptz, kuehler_unit_id uuid, is_deleted boolean not null default false, status text not null);
      create table visit_session_sections (id uuid primary key, visit_session_id uuid not null,
        campaign_id uuid, section text not null, is_deleted boolean not null default false);
    `);
    const campaignId = randomUUID(), marketId = randomUUID(), alex = randomUUID(), pascal = randomUUID();
    const [unit1, unit2, alexFirst, alexSecond, pascalFirst, alexVisit, pascalVisit] = Array.from({ length: 7 }, () => randomUUID());
    await pg.query("insert into campaigns(id,section) values ($1,'kuehler')", [campaignId]);
    await pg.query("insert into market_kuehler_units(id,market_id,kuehler_internal_id) values ($1,$3,'1'),($2,$3,'2')", [unit1, unit2, marketId]);
    await pg.query("insert into campaign_market_assignments(id,campaign_id,market_id,gm_user_id,assignment_slot,visit_target_count) values ($1,$4,$5,$6,1,2),($2,$4,$5,$6,2,2),($3,$4,$5,$7,1,2)",
      [alexFirst, alexSecond, pascalFirst, campaignId, marketId, alex, pascal]);
    await pg.query("insert into visit_sessions(id,market_id,gm_user_id,kuehler_unit_id,status,submitted_at) values ($1,$3,$4,$5,'submitted',$7),($2,$3,$6,$5,'submitted',$7)",
      [alexVisit, pascalVisit, marketId, alex, unit1, pascal, "2026-09-20T10:00:00Z"]);
    await pg.query("insert into visit_session_sections(id,visit_session_id,campaign_id,section) values ($1,$3,$5,'kuehler'),($2,$4,$5,'kuehler')",
      [randomUUID(), randomUUID(), alexVisit, pascalVisit, campaignId]);
    const progress = await transaction((tx) => loadCampaignVisitProgress(tx, campaignId));
    assert.equal(progress.completedByAssignmentId.get(alexFirst), 1);
    assert.equal(progress.completedByAssignmentId.get(alexSecond), 0);
    const move = { campaignId, toGmUserId: pascal, expectedGmUserId: alex,
      expectedVisitTargetCount: 2, visitNumber: 1, auditUserId: null, now: new Date("2026-09-21T10:00:00Z") };
    await assert.rejects(transaction((tx) => reassignCampaignVisitTarget(tx, { ...move, assignmentId: alexFirst })),
      (error: unknown) => error instanceof CampaignVisitReassignmentError && error.code === "visit_already_completed");
    const draftId = randomUUID();
    await pg.query("insert into visit_sessions(id,market_id,gm_user_id,kuehler_unit_id,status) values ($1,$2,$3,$4,'draft')",
      [draftId, marketId, alex, unit2]);
    await pg.query("insert into visit_session_sections(id,visit_session_id,campaign_id,section) values ($1,$2,$3,'kuehler')",
      [randomUUID(), draftId, campaignId]);
    const withDraft = await transaction((tx) => loadCampaignVisitProgress(tx, campaignId));
    assert.equal(withDraft.startedByAssignmentId.get(alexSecond), 1);
    await assert.rejects(transaction((tx) => reassignCampaignVisitTarget(tx, { ...move, assignmentId: alexSecond })),
      (error: unknown) => error instanceof CampaignVisitReassignmentError && error.code === "visit_already_started");
    await pg.query("update visit_sessions set status = 'cancelled' where id = $1", [draftId]);
    await transaction((tx) => reassignCampaignVisitTarget(tx, { ...move, assignmentId: alexSecond }));
    const moved = await database.select().from(schema.campaignMarketAssignments).where(eq(schema.campaignMarketAssignments.id, alexSecond));
    assert.equal(moved[0]?.gmUserId, pascal);
    assert.equal(moved[0]?.assignmentSlot, 2);
    assert.equal(moved[0]?.visitTargetCount, 2);
    const after = await transaction((tx) => loadCampaignVisitProgress(tx, campaignId));
    assert.equal(after.completedByAssignmentId.get(alexFirst), 1);
    assert.equal(after.completedByAssignmentId.get(pascalFirst), 1);
    assert.equal(after.completedByAssignmentId.get(alexSecond), 0);
    assert.equal((await database.select().from(schema.campaignMarketAssignmentHistory)).length, 1);
    assert.equal((await database.select({ gmUserId: schema.visitSessions.gmUserId }).from(schema.visitSessions)
      .where(eq(schema.visitSessions.id, alexVisit)))[0]?.gmUserId, alex);

    const legacyMarket = randomUUID(), legacyAssignment = randomUUID(), legacyUnit1 = randomUUID(), legacyUnit2 = randomUUID();
    const legacyVisit = randomUUID();
    await pg.query("insert into market_kuehler_units(id,market_id,kuehler_internal_id) values ($1,$3,'3'),($2,$3,'4')",
      [legacyUnit1, legacyUnit2, legacyMarket]);
    await pg.query("insert into campaign_market_assignments(id,campaign_id,market_id,gm_user_id,assignment_slot,visit_target_count) values ($1,$2,$3,$4,1,1)",
      [legacyAssignment, campaignId, legacyMarket, alex]);
    await pg.query("insert into visit_sessions(id,market_id,gm_user_id,kuehler_unit_id,status,submitted_at) values ($1,$2,$3,$4,'submitted',$5)",
      [legacyVisit, legacyMarket, alex, legacyUnit2, "2026-09-22T10:00:00Z"]);
    await pg.query("insert into visit_session_sections(id,visit_session_id,campaign_id,section) values ($1,$2,$3,'kuehler')",
      [randomUUID(), legacyVisit, campaignId]);
    assert.equal((await transaction((tx) => loadCampaignVisitProgress(tx, campaignId))).completedByAssignmentId.get(legacyAssignment), 1);
    await assert.rejects(transaction((tx) => reassignCampaignVisitTarget(tx, { ...move, assignmentId: legacyAssignment,
      expectedVisitTargetCount: 1 })),
    (error: unknown) => error instanceof CampaignVisitReassignmentError && error.code === "visit_already_completed");
  } finally {
    await pg.close();
  }
});
