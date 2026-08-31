import { sql } from "drizzle-orm";
import type { db } from "./lib/db.js";

type DbTx = Parameters<Parameters<typeof db.transaction>[0]>[0];
export type SmTimeConflict = {
  submissionId: string; assignmentId: string | null; marketName: string; marketAddress: string;
  startedAt: string; completedAt: string;
};
const viennaDateTime = new Intl.DateTimeFormat("de-AT", {
  timeZone: "Europe/Vienna", day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit", hourCycle: "h23",
});
export function smTimeRangeLabel(start: Date | string, end: Date | string) {
  return `${viennaDateTime.format(new Date(start))} – ${viennaDateTime.format(new Date(end))} Uhr`;
}
export class SmTimeOverlapError extends Error {
  readonly statusCode = 409;
  readonly code = "sm_visit_time_overlap";
  constructor(public readonly details: { proposedStartedAt: string; proposedCompletedAt: string; conflicts: SmTimeConflict[] }) {
    super(`Nicht gespeichert: Deine Besuchszeit (${smTimeRangeLabel(details.proposedStartedAt, details.proposedCompletedAt)}) überschneidet sich mit einem bereits abgeschlossenen Fragebogen: ${details.conflicts.map((conflict) => `${conflict.marketName}${conflict.marketAddress ? `, ${conflict.marketAddress}` : ""} (${smTimeRangeLabel(conflict.startedAt, conflict.completedAt)})`).join("; ")}. Du kannst nicht gleichzeitig in zwei Märkten sein. Bitte ändere Start und Ende und versuche es erneut. Deine Antworten bleiben gespeichert.`);
  }
}

export async function lockSmVisitTimes(tx: DbTx, smUserId: string) {
  // Transaction-level locks also work with transaction-pooling; never use a session lock here.
  await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended(${`sm_visit_times:${smUserId}`}, 0))`);
}

export async function assertSmVisitTimeAvailable(tx: DbTx, input: {
  smUserId: string; assignmentId: string; startedAt: Date; completedAt: Date;
}) {
  await lockSmVisitTimes(tx, input.smUserId);
  const rows = await tx.execute<SmTimeConflict>(sql`
    select s.id as "submissionId", s.assignment_id as "assignmentId",
      s.market_name_snapshot as "marketName",
      concat_ws(', ', nullif(s.market_address_snapshot, ''), nullif(concat_ws(' ', nullif(s.market_postal_code_snapshot, ''), nullif(s.market_city_snapshot, '')), '')) as "marketAddress",
      s.visit_started_at as "startedAt", s.visit_completed_at as "completedAt"
    from public.sm_questionnaire_submissions s
    where s.sm_user_id = ${input.smUserId}::uuid
      and s.assignment_id is distinct from ${input.assignmentId}::uuid
      and s.status = 'submitted' and s.is_current and not s.is_deleted
      and s.visit_started_at < ${input.completedAt.toISOString()}::timestamptz
      and s.visit_completed_at > ${input.startedAt.toISOString()}::timestamptz
      and exists (select 1 from public.sm_assignment_time_submissions t
        where t.assignment_id = s.assignment_id and t.is_current and not t.is_deleted)
    order by s.visit_started_at, s.id
  `);
  if (rows.length) throw new SmTimeOverlapError({
    proposedStartedAt: input.startedAt.toISOString(), proposedCompletedAt: input.completedAt.toISOString(),
    conflicts: rows.map((row) => ({ ...row, startedAt: new Date(row.startedAt).toISOString(), completedAt: new Date(row.completedAt).toISOString() })),
  });
}
