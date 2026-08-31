import { sql } from "drizzle-orm";
import type { db } from "./lib/db.js";

export async function lockSmPlanning(tx: Parameters<Parameters<typeof db.transaction>[0]>[0]) {
  // SM-only coordination for plan creation, edits, starts and market deactivation.
  // Acquire before assignment/market row locks. Never hold across network calls.
  await tx.execute(sql`select pg_advisory_xact_lock(hashtextextended('sm_planning_mutations', 0))`);
}
