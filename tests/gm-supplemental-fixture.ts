// Isolated PostgreSQL and real admin routes. No env.ts, db.ts, app entry point,
// production credentials, schedulers or external auth/storage are imported.
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { SQL, is } from "drizzle-orm";
import { getTableConfig, PgTable, PgDialect } from "drizzle-orm/pg-core";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import express from "express";
import * as schema from "../src/lib/schema.js";
import * as roleScope from "../src/lib/admin-user-scope.js";
import * as accountStatus from "../src/sm-user-status.shared.js";
import { isolatedModule } from "./isolated-module.js";
import { modelDatabase } from "../src/lib/praemien-workspace.js";
import { createGmDashboardRouter } from "../src/routes/gm-dashboard.js";

export async function supplementalFixture() {
  if (process.env.DATABASE_URL || process.env.SUPABASE_URL || process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NODE_ENV === "production") throw new Error("Synthetic fixture refuses production configuration");
  const pg = new PGlite();
  const database = drizzle(pg, { schema }), dialect = new PgDialect();
  for (const value of Object.values(schema)) {
    if (typeof value !== "function" || !("enumName" in value) || !("enumValues" in value)) continue;
    const values = value.enumValues as string[];
    await pg.exec('create type "' + value.enumName + '" as enum (' + values.map(v => "'" + v.replaceAll("'", "''") + "'").join(",") + ')');
  }
  for (const table of Object.values(schema)) {
    if (!is(table, PgTable)) continue;
    const config = getTableConfig(table);
    const columns = config.columns.filter(c => !["is_supplemental_gm", "is_supplemental_gm_snapshot"].includes(c.name)).map(column => {
      const type = column.getSQLType();
      const value = column.default;
      const expression = is(value, SQL) ? dialect.sqlToQuery(value).sql : typeof value === "string" ? "'" + value.replaceAll("'", "''") + "'" : typeof value === "number" || typeof value === "boolean" ? String(value) : null;
      return '"' + column.name + '" ' + type + (expression ? " default " + expression : "");
    });
    await pg.exec('create table "' + config.name + '" (' + columns.join(",") + ')');
  }
  const adminId = randomUUID();
  await pg.query("insert into users(id,supabase_auth_id,role,email,first_name,last_name) values($1,'synthetic-admin','admin','admin@example.test','Demo','Admin')", [adminId]);
  await pg.exec(await readFile(new URL("../supabase/migrations/20261029184231_gm_supplemental_budget.sql", import.meta.url), "utf8"));
  const requireAuth = (roles: string[]) => (req: any, res: any, next: () => void) => {
    const token = req.headers.authorization;
    const role = token === "Bearer synthetic-admin" ? "admin" : token === "Bearer synthetic-sm-admin" ? "sm_admin" : token === "Bearer synthetic-gm" ? "gm" : null;
    if (!role) { res.sendStatus(401); return; }
    if (!roles.includes(role)) { res.sendStatus(403); return; }
    req.authUser = { role, appUserId: adminId }; next();
  };
  const route = await isolatedModule<typeof import("../src/routes/admin-users.js")>(new URL("../src/routes/admin-users.ts", import.meta.url), {
    "../lib/db.js": { db: database }, "../lib/schema.js": schema,
    "../middleware/auth.js": { requireAuth },
    "../lib/admin-role.js": { isFullAdminRole: (role: string) => role === "admin" },
    "../lib/admin-user-scope.js": roleScope,
    "../lib/kunde-access.js": { getKundePermissionsForUser: async () => ({}), hasKundePermission: () => false, isKundeSmPageKey: () => false, KUNDE_ADMIN_PAGE_KEYS: [] },
    "../lib/gm-kpi-cache.js": { readGmKpiCaches: async () => new Map(), recomputeGmKpiCache: async () => ({ ipp: 0, ippSampleCount: 0 }) },
    "../lib/supabase.js": { supabaseAdmin: { auth: { admin: {
      createUser: async () => ({ data: { user: { id: randomUUID() } }, error: null }),
      updateUserById: async () => ({ data: { user: {} }, error: null }),
      deleteUser: async () => ({ error: null }),
    } } } },
    "../services/password.js": { generatePassword: () => "Synthetic-Demo-Only!" },
    "../sm-user-status.shared.js": accountStatus,
    "../lib/logger.js": { aggregateHighVolumeLoad() {}, logAction() {}, markErrorAsLogged() {}, startActionTimer: () => 0 },
  });
  const app = express(); app.use(express.json());
  app.use((req, res, next) => { res.set("Access-Control-Allow-Origin", "http://127.0.0.1:3037"); res.set("Access-Control-Allow-Headers", "Authorization,Content-Type,X-Request-Id"); res.set("Access-Control-Allow-Methods", "GET,POST,PATCH,OPTIONS"); if (req.method === "OPTIONS") { res.sendStatus(204); return; } next(); });
  app.use("/admin/users", route.adminUsersRouter);
  app.use("/admin/gm-dashboard", requireAuth(["admin"]), createGmDashboardRouter(modelDatabase(database)));
  app.use((error: Error, _req: any, res: any, _next: any) => res.status(500).json({ error: error.message }));
  return { pg, database, app, adminId };
}
