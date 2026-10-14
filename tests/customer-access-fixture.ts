import { randomUUID } from "node:crypto";
import type { Express } from "express";
import * as schema from "../src/lib/schema.js";
import * as adminRole from "../src/lib/admin-role.js";
import * as userScope from "../src/lib/admin-user-scope.js";
import * as userStatus from "../src/sm-user-status.shared.js";
import { createSMDurcharbeitFixture } from "./SMDurcharbeit-fixture.js";
import { isolatedModule } from "./isolated-module.js";

export async function installCustomerAccessRoutes(app: Express, f: Awaited<ReturnType<typeof createSMDurcharbeitFixture>>) {
  await f.pg.exec(`create table auth_audit_logs(id uuid primary key default gen_random_uuid(),actor_user_id uuid references users(id),target_user_id uuid references users(id),event_type text not null,details text,created_at timestamptz not null default now());`);
  const authCalls: Array<{ operation: string; id: string }> = [];
  const supabase = { auth: { admin: {
    createUser: async () => { const id = randomUUID(); authCalls.push({ operation: "create", id }); return { data: { user: { id } }, error: null }; },
    updateUserById: async (id: string) => { authCalls.push({ operation: "update", id }); return { data: { user: { id } }, error: null }; },
    deleteUser: async (id: string) => { authCalls.push({ operation: "delete", id }); return { data: { user: { id } }, error: null }; },
  } } };
  const replacements = {
    "../lib/db.js": { db: f.dbForRoutes }, "../lib/schema.js": schema, "../lib/admin-role.js": adminRole,
    "../lib/kunde-access.js": f.kundeAccess, "../middleware/auth.js": f.auth,
    "../lib/supabase.js": { supabaseAdmin: supabase }, "../services/password.js": { generatePassword: () => "Synthetic-preview-only-2026" },
    "../lib/logger.js": { ...f.harmlessLogger, markErrorAsLogged() {}, aggregateHighVolumeLoad() {} },
    "../lib/admin-user-scope.js": userScope, "../sm-user-status.shared.js": userStatus,
    "../lib/gm-kpi-cache.js": { readGmKpiCaches: async () => new Map(), recomputeGmKpiCache: async () => { throw new Error("GM KPI work is outside this synthetic customer fixture"); } },
  };
  const customers = await isolatedModule<typeof import("../src/routes/kunden-users.js")>(new URL("../src/routes/kunden-users.ts", import.meta.url), replacements);
  const directory = await isolatedModule<typeof import("../src/routes/admin-users.js")>(new URL("../src/routes/admin-users.ts", import.meta.url), replacements);
  app.use("/admin/kunden-users", customers.kundenUsersRouter);
  app.use("/admin/users", directory.adminUsersRouter);
  return { authCalls };
}
