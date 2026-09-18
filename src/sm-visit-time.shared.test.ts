import assert from "node:assert/strict";
import test from "node:test";
import { PGlite } from "@electric-sql/pglite";

import { buildSmAssignmentCompletionUpdate } from "./sm-visit-time.shared.js";

test("completion replaces the provisional assignment start with the submitted visit interval", async () => {
  const pg = new PGlite();
  try {
    await pg.exec(`
      create table sm_assignments (
        id uuid primary key,
        status text not null,
        started_at timestamptz,
        completed_at timestamptz,
        updated_by_user_id uuid not null,
        updated_at timestamptz not null,
        constraint sm_assignments_execution_time_ck
          check (started_at is null or completed_at is null or completed_at >= started_at)
      );
    `);

    const assignmentId = "00000000-0000-4000-8000-000000000001";
    const actorUserId = "00000000-0000-4000-8000-000000000002";
    const provisionalStart = new Date("2026-09-18T16:38:00+02:00");
    const submittedStart = new Date("2026-09-18T14:00:00+02:00");
    const submittedEnd = new Date("2026-09-18T15:00:00+02:00");
    const updatedAt = new Date("2026-09-18T16:40:00+02:00");

    await pg.query(
      "insert into sm_assignments (id, status, started_at, updated_by_user_id, updated_at) values ($1, 'in_progress', $2, $3, $4)",
      [assignmentId, provisionalStart, actorUserId, updatedAt],
    );

    await assert.rejects(
      pg.query("update sm_assignments set status = 'completed', completed_at = $1 where id = $2", [submittedEnd, assignmentId]),
      /sm_assignments_execution_time_ck/,
    );

    const update = buildSmAssignmentCompletionUpdate({
      visitStartedAt: submittedStart,
      visitCompletedAt: submittedEnd,
      actorUserId,
      updatedAt,
    });
    await pg.query(
      "update sm_assignments set status = $1, started_at = $2, completed_at = $3, updated_by_user_id = $4, updated_at = $5 where id = $6",
      [update.status, update.startedAt, update.completedAt, update.updatedByUserId, update.updatedAt, assignmentId],
    );

    const result = await pg.query<{ status: string; started_at: string; completed_at: string }>(
      "select status, started_at, completed_at from sm_assignments where id = $1",
      [assignmentId],
    );
    assert.equal(result.rows[0]?.status, "completed");
    assert.equal(new Date(result.rows[0]!.started_at).toISOString(), submittedStart.toISOString());
    assert.equal(new Date(result.rows[0]!.completed_at).toISOString(), submittedEnd.toISOString());
  } finally {
    await pg.close();
  }
});
