// Default is preview only. --apply is explicit and all writes stay in SM tables.
import { and, eq, inArray } from "drizzle-orm";
import { db, sql } from "../lib/db.js";
import { users } from "../lib/schema.js";
import { adjustSmHolidayAssignments } from "../sm-holiday-planning.js";

try {
  const actorUserId = process.argv.find((arg) => arg.startsWith("--actor="))?.slice(8);
  if (!actorUserId || !/^[0-9a-f-]{36}$/i.test(actorUserId)) throw new Error("--actor=<existing SM admin UUID> is required");
  const [actor] = await db.select({ id: users.id }).from(users).where(and(eq(users.id, actorUserId), inArray(users.role, ["admin", "sm_admin"])));
  if (!actor) throw new Error("Actor must be an existing admin or SM admin");
  const dryRun = !process.argv.includes("--apply");
  const changes = await db.transaction((tx) => adjustSmHolidayAssignments(tx, { actorUserId, dryRun }));
  console.log(JSON.stringify({ dryRun, count: changes.length, changes }, null, 2));
} finally { await sql.end(); }
