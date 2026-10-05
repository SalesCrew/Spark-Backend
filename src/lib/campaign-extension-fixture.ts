// Disposable PostgreSQL fixture: never imports db.ts, env.ts, Supabase or application jobs.
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import * as schema from "./schema.js";
import { findAssignmentConflicts } from "./campaign-assignment-conflicts.js";
import type { CampaignExtensionDependencies, CampaignExtensionTx } from "./campaign-extension.js";

export async function campaignExtensionFixture() {
  if (process.env.DATABASE_URL || process.env.SUPABASE_URL || process.env.SUPABASE_SERVICE_ROLE_KEY)
    throw new Error("Campaign fixture requires an isolated environment.");
  const pg = new PGlite();
  await pg.exec(`
    create table users(id uuid primary key, first_name text, last_name text);
    create table markets(id uuid primary key, name text not null);
    create table campaigns(
      id uuid primary key default gen_random_uuid(), name text not null, section text not null,
      assigned_gm_user_id uuid, current_fragebogen_id uuid, status text not null default 'inactive',
      schedule_type text not null default 'scheduled', start_date date, end_date date,
      is_deleted boolean not null default false, deleted_at timestamptz,
      created_at timestamptz not null default now(), updated_at timestamptz not null default now(),
      check(schedule_type='always' or (start_date is not null and end_date is not null and start_date<=end_date)));
    create table campaign_market_assignments(
      id uuid primary key default gen_random_uuid(), campaign_id uuid not null references campaigns(id),
      market_id uuid not null references markets(id), gm_user_id uuid references users(id),
      assignment_slot integer not null default 1, visit_target_count integer not null default 1,
      current_visits_count integer not null default 0, assigned_by_user_id uuid,
      assigned_at timestamptz not null default now(), is_deleted boolean not null default false,
      deleted_at timestamptz, created_at timestamptz not null default now(), updated_at timestamptz not null default now());
    create table campaign_fragebogen_history(id uuid primary key, campaign_id uuid references campaigns(id), payload jsonb);
    create table campaign_market_assignment_history(id uuid primary key, campaign_id uuid references campaigns(id), payload jsonb);
    create table visit_sessions(id uuid primary key, campaign_id uuid references campaigns(id), payload jsonb);
    create table visit_answers(id uuid primary key, visit_session_id uuid references visit_sessions(id), payload jsonb);
    create table visit_answer_photos(id uuid primary key, visit_answer_id uuid references visit_answers(id), payload jsonb);
    create table question_scoring(id uuid primary key, payload jsonb);
  `);
  const database = drizzle(pg, { schema });
  const completedCoolers = new Set<string>();
  const dependencies: CampaignExtensionDependencies = {
    transaction: (action) => database.transaction((tx) => action(tx as unknown as CampaignExtensionTx)),
    checkConflicts: (tx, input) => findAssignmentConflicts(tx, { ...input, includeScheduled: true }, async () => completedCoolers),
  };
  const gm = randomUUID(), questionnaire = randomUUID();
  await pg.query("insert into users values($1,'Synthetischer','GM')", [gm]);
  async function seed(options: Partial<typeof schema.campaigns.$inferInsert> = {}, sharedMarket?: string) {
    const id = options.id ?? randomUUID(), marketId = sharedMarket ?? randomUUID();
    if (!sharedMarket) await pg.query("insert into markets values($1,$2)", [marketId, "Testmarkt " + id.slice(0, 4)]);
    await database.insert(schema.campaigns).values({
      id, name: "Synthetische Kampagne", section: "standard", scheduleType: "scheduled",
      status: "inactive", startDate: "2026-09-21", endDate: "2026-10-02",
      currentFragebogenId: questionnaire, createdAt: new Date("2026-09-01T08:00:00Z"),
      updatedAt: new Date("2026-09-21T08:00:00Z"), ...options,
    });
    await database.insert(schema.campaignMarketAssignments).values({ campaignId: id, marketId, gmUserId: gm, visitTargetCount: 3, currentVisitsCount: 2, assignedByUserId: gm });
    const visit = randomUUID(), answer = randomUUID();
    for (const table of ["campaign_fragebogen_history", "campaign_market_assignment_history"])
      await pg.query(`insert into ${table} values($1,$2,$3)`, [randomUUID(), id, JSON.stringify({ questionnaire, gm, retained: true })]);
    await pg.query("insert into visit_sessions values($1,$2,$3)", [visit, id, JSON.stringify({ gm, status: "submitted", date: "2026-09-28" })]);
    await pg.query("insert into visit_answers values($1,$2,$3)", [answer, visit, JSON.stringify({ number: 5, choices: ["Ja"], valid: true })]);
    await pg.query("insert into visit_answer_photos values($1,$2,$3)", [randomUUID(), answer, JSON.stringify({ storageKey: "synthetic-only/example.jpg" })]);
    await pg.query("insert into question_scoring values($1,$2)", [randomUUID(), JSON.stringify({ points: 10, boni: 20 })]);
    return { id, marketId };
  }
  async function get(id: string) { return (await database.select().from(schema.campaigns).where(eq(schema.campaigns.id, id)))[0]!; }
  async function snapshot() {
    const tables = ["campaigns", "campaign_market_assignments", "campaign_fragebogen_history", "campaign_market_assignment_history", "visit_sessions", "visit_answers", "visit_answer_photos", "question_scoring"];
    return Object.fromEntries(await Promise.all(tables.map(async (table) => [table, (await pg.query(`select * from ${table} order by id`)).rows])));
  }
  return { pg, database, dependencies, completedCoolers, gm, questionnaire, seed, get, snapshot };
}
