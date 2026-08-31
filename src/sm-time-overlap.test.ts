import assert from "node:assert/strict";
import test from "node:test";
import { PgDialect } from "drizzle-orm/pg-core";
import type { SQL } from "drizzle-orm";
import { assertSmVisitTimeAvailable, SmTimeOverlapError, smTimeRangeLabel } from "./sm-time-overlap.js";

const input = { smUserId: "11111111-1111-4111-8111-111111111111", assignmentId: "22222222-2222-4222-8222-222222222222", startedAt: new Date("2026-08-31T13:10:00Z"), completedAt: new Date("2026-08-31T13:50:00Z") };
test("SM overlap query uses a per-person transaction lock, half-open intervals, current time and no GM tables", async () => {
  const queries: ReturnType<PgDialect["sqlToQuery"]>[] = [];
  const tx = { execute: async (query: SQL) => { queries.push(new PgDialect().sqlToQuery(query)); return []; } };
  await assertSmVisitTimeAvailable(tx as never, input);
  assert.equal(queries.length, 2);
  assert.match(queries[0]!.sql, /pg_advisory_xact_lock/);
  assert.deepEqual(queries[0]!.params, [`sm_visit_times:${input.smUserId}`]);
  assert.match(queries[1]!.sql, /visit_started_at <.*::timestamptz/);
  assert.match(queries[1]!.sql, /visit_completed_at >.*::timestamptz/);
  assert.match(queries[1]!.sql, /s\.status = 'submitted' and s\.is_current and not s\.is_deleted/);
  assert.match(queries[1]!.sql, /t\.is_current and not t\.is_deleted/);
  assert.match(queries[1]!.sql, /assignment_id is distinct from/);
  assert.deepEqual(queries[1]!.params, [input.smUserId, input.assignmentId, input.completedAt.toISOString(), input.startedAt.toISOString()]);
  assert.doesNotMatch(queries[1]!.sql, /gm_|public\.markets\b|visit_sessions/);
});
test("conflict explains market, both full intervals, rejection and editable retry in German", () => {
  const error = new SmTimeOverlapError({ proposedStartedAt: input.startedAt.toISOString(), proposedCompletedAt: input.completedAt.toISOString(), conflicts: [{ submissionId: "test", assignmentId: "other", marketName: "Billa", marketAddress: "Teststraße 1, Wien", startedAt: "2026-08-31T13:10:00Z", completedAt: "2026-08-31T13:30:00Z" }] });
  assert.equal(error.code, "sm_visit_time_overlap");
  for (const text of ["Nicht gespeichert", "Billa", "Teststraße", "15:10", "15:30", "15:50", "31.08.2026", "Start und Ende", "Antworten bleiben gespeichert"]) assert.ok(error.message.includes(text), text);
});
test("Vienna rendering spans midnight and DST without losing the date", () => {
  assert.match(smTimeRangeLabel("2026-08-31T21:30:00Z", "2026-08-31T22:30:00Z"), /31\.08\.2026.*23:30.*01\.09\.2026.*00:30/);
  assert.match(smTimeRangeLabel("2026-03-29T00:30:00Z", "2026-03-29T01:30:00Z"), /01:30.*03:30/);
});
