import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { getTableColumns } from "drizzle-orm";
import express from "express";
import * as schema from "../src/lib/schema.js";
import * as SMDurcharbeitCatalog from "../src/sm-SMDurcharbeit-catalog.shared.js";
import * as planning from "../src/sm-planning.shared.js";
import * as visit from "../src/sm-visit.shared.js";
import * as timing from "../src/sm-visit-time.shared.js";
import * as dashboard from "../src/sm-dashboard.shared.js";
import * as conditional from "../src/lib/conditional-visibility.js";
import * as comments from "../src/sm-comment.shared.js";
import * as SMDurcharbeitSelection from "../src/sm-SMDurcharbeit-selection.shared.js";
import * as profile from "../src/sm-profile.shared.js";
import * as holidaysShared from "../src/sm-holidays.shared.js";
import * as seriesShared from "../src/sm-series.shared.js";
import * as managementShared from "../src/sm-management.js";
import * as sync from "../src/sm-market-user-sync.shared.js";
import * as weekly from "../src/sm-market-weekly-planning.shared.js";
import { z } from "zod";
import * as lock from "../src/sm-planning-lock.js";
import * as overlap from "../src/sm-time-overlap.js";
import { isRoleAllowedForEndpoint } from "../src/lib/admin-role.js";
import { isolatedModule } from "./isolated-module.js";

/** Actual SM routes and migrations, with all external I/O replaced and a fresh disposable DB. */
export async function createSMDurcharbeitFixture(options: { photoStorage?: { from: (bucket: string) => any } } = {}) {
  if (process.env.DATABASE_URL || process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NODE_ENV === "production") {
    throw new Error("Production configuration is forbidden in SMDurcharbeit fixtures.");
  }
  const pg = new PGlite(), database = drizzle(pg, { schema });
  await pg.exec(`create role anon; create role authenticated; create role service_role;
    create table users(id uuid primary key,first_name text,last_name text,email text default 'synthetic@preview.test',role text,is_active boolean default true,deleted_at timestamptz,sm_travel_time_enabled boolean default true);`);
  const existingUserColumns = new Set(["id", "first_name", "last_name", "email", "role", "is_active", "deleted_at", "sm_travel_time_enabled"]);
  for (const column of Object.values(getTableColumns(schema.users))) {
    if (!existingUserColumns.has(column.name)) await pg.exec(`alter table users add column "${column.name}" ${column.getSQLType()}`);
  }
  for (const migration of ["0088_sm_markets.sql", "0089_sm_questionnaire_domain.sql", "0091_sm_enforce_soft_deletes.sql", "0092_sm_market_assignments.sql", "0093_sm_planning.sql", "0097_sm_visit_runtime_timing.sql", "0099_sm_zeiterfassung_requests.sql", "0100_sm_activity_request_audit.sql", "0102_sm_time_request_timestamps.sql", "0103_sm_time_request_equal_duration.sql", "0105_sm_global_questionnaire_assignment.sql", "0108_sm_market_account_assignments.sql"]) {
    await pg.exec(await readFile(new URL(`../drizzle/${migration}`, import.meta.url), "utf8"));
  }
  await pg.exec(await readFile(new URL("../supabase/migrations/20261007124730_SMDurcharbeit_einsatz_override.sql", import.meta.url), "utf8"));
  await pg.exec(await readFile(new URL("../supabase/migrations/20261007133647_SMDurcharbeit_market_registry.sql", import.meta.url), "utf8"));
  const admin = randomUUID(), employee = randomUUID(), market = randomUUID();
  await pg.query("insert into users(id,first_name,last_name,role) values($1,'Local','Admin','sm_admin'),($2,'Local','SM','sm')", [admin, employee]);
  await pg.query("insert into sm_markets(id,name,chain,address,postal_code,city,region,internal_market_id) values($1,'Synthetic Billa','Billa','Testgasse 1','1010','Wien','Ost','SYNTHETIC-1')", [market]);
  const harmlessLogger = { logger: { warn() {}, error() {}, info() {} }, logAction() {}, startActionTimer: () => () => {} };
  const auth = { requireAuth: (roles: schema.UserRole[]) => (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const token = req.header("authorization")?.replace(/^Bearer /, "");
    const tokens: Record<string, schema.UserRole> = { "synthetic-sm-admin": "sm_admin", "synthetic-sm": "sm", "synthetic-admin": "admin", "synthetic-gm-admin": "kunde", "synthetic-gm": "gm" };
    const role = token ? tokens[token] ?? null : null;
    if (!role) { res.sendStatus(401); return; }
    if (!isRoleAllowedForEndpoint(role, roles)) { res.sendStatus(403); return; }
    Object.assign(req, { authUser: { appUserId: role === "sm" ? employee : admin, role } }); next();
  } };
  // Match postgres-js raw execute semantics while retaining the real PGlite ORM and transactions.
  const asPostgres = (target: any): any => new Proxy(target, {
    get(object, key) {
      if (key === "execute") return async (query: unknown) => (await object.execute(query)).rows;
      if (key === "transaction") return (action: (tx: unknown) => unknown) => object.transaction((tx: unknown) => action(asPostgres(tx)));
      const value = Reflect.get(object, key);
      return typeof value === "function" ? value.bind(object) : value;
    },
  });
  const dbForRoutes = asPostgres(database);
  const base = { "../sm-SMDurcharbeit-selection.shared.js": SMDurcharbeitSelection, "../sm-planning-lock.js": lock, "../sm-SMDurcharbeit-catalog.shared.js": SMDurcharbeitCatalog, "../lib/db.js": { db: dbForRoutes }, "../lib/schema.js": schema, "../lib/logger.js": harmlessLogger, "../middleware/auth.js": auth };
  const authoring = await isolatedModule<typeof import("../src/routes/sm-questionnaires.js")>(new URL("../src/routes/sm-questionnaires.ts", import.meta.url), { ...base, "../sm-SMDurcharbeit-catalog.shared.js": SMDurcharbeitCatalog });
  const reporting = await isolatedModule<typeof import("../src/routes/sm-dashboard.js")>(new URL("../src/routes/sm-dashboard.ts", import.meta.url), {
    ...base, "../sm-dashboard.shared.js": dashboard, "../sm-planning.shared.js": planning,
  });
  const runtime = await isolatedModule<typeof import("../src/routes/sm-visits.js")>(new URL("../src/routes/sm-visits.ts", import.meta.url), {
    ...base, "../lib/conditional-visibility.js": conditional, "../sm-comment.shared.js": comments,
    "../sm-visit.shared.js": visit, "../sm-visit-time.shared.js": timing, "../sm-planning.shared.js": planning,
    "../sm-planning-lock.js": lock, "../sm-time-overlap.js": overlap,
    "../sm-market-deactivation.js": { smDeactivationToday: () => "2026-10-07" },
    "../lib/supabase.js": { supabaseAdmin: { storage: options.photoStorage ?? { from: () => { throw new Error("Storage I/O is forbidden in SMDurcharbeit fixtures"); } } } },
  });
  const holidays = await isolatedModule<typeof import("../src/sm-holiday-planning.js")>(new URL("../src/sm-holiday-planning.ts", import.meta.url), {
    "./lib/db.js": { db: dbForRoutes }, "./lib/schema.js": schema, "./sm-planning-lock.js": lock, "./sm-planning.shared.js": planning, "./sm-holidays.shared.js": holidaysShared,
  });
  const series = await isolatedModule<typeof import("../src/sm-series-management.js")>(new URL("../src/sm-series-management.ts", import.meta.url), {
    "./lib/db.js": { db: dbForRoutes }, "./lib/schema.js": schema, "./sm-planning-lock.js": lock, "./sm-planning.shared.js": planning,
    "./sm-holiday-planning.js": holidays, "./sm-series.shared.js": seriesShared, "./sm-SMDurcharbeit-selection.shared.js": SMDurcharbeitSelection,
  });
  const planningRoutes = await isolatedModule<typeof import("../src/routes/sm-planning.js")>(new URL("../src/routes/sm-planning.ts", import.meta.url), {
    ...base, "../sm-time-overlap.js": overlap, "../sm-profile.shared.js": profile, "../sm-series-management.js": series,
    "../sm-holiday-planning.js": holidays, "../sm-planning.shared.js": planning,
  });
  const managed = await isolatedModule<typeof import("../src/routes/sm-management.js")>(new URL("../src/routes/sm-management.ts", import.meta.url), {
    ...base, "../sm-management.js": managementShared, "../sm-planning.shared.js": planning, "../sm-visit.shared.js": visit,
    "../config/env.js": { env: { JWT_SECRET: "synthetic-only-local-secret" } },
    "../lib/supabase.js": { supabaseAdmin: { storage: options.photoStorage ?? { from: () => ({ createSignedUrl: async () => ({ data: { signedUrl: null }, error: null }) }) } } },
  });
  const app = express(); app.use(express.json());
  const photos = await isolatedModule<typeof import("../src/routes/sm-photo-archive.js")>(new URL("../src/routes/sm-photo-archive.ts", import.meta.url), {
    ...base, "../sm-planning.shared.js": planning,
    "../lib/supabase.js": { supabaseAdmin: { storage: options.photoStorage ?? { from: () => ({ createSignedUrls: async () => ({ data: [], error: null }) }) } } },
  });
  app.use("/admin/sm-photos", photos.adminSmPhotoArchiveRouter);
  const activity = await isolatedModule<typeof import("../src/routes/sm-activity.js")>(new URL("../src/routes/sm-activity.ts", import.meta.url), {
    ...base, "./sm-management.js": managed, "../lib/conditional-visibility.js": conditional,
    "../sm-comment.shared.js": comments, "../sm-planning.shared.js": planning, "../sm-visit.shared.js": visit,
  });
  const directory = await isolatedModule<typeof import("../src/routes/sm-markets.js")>(new URL("../src/routes/sm-markets.ts", import.meta.url), {
    ...base, "../sm-market-user-sync.shared.js": sync, "../sm-market-weekly-planning.shared.js": weekly,
    "../sm-market-deactivation.js": { smMarketDeactivationSchema: z.object({}), SmMarketDeactivationError: class extends Error {}, deactivateSmMarket: () => { throw new Error("Not covered by this fixture"); }, loadSmMarketDeactivationPreview: () => { throw new Error("Not covered by this fixture"); } },
  });
  app.use("/admin/sm-markets", directory.adminSmMarketsRouter);
  app.use("/sm/activity", activity.smActivityRouter);
  app.use("/admin/sm-activity", activity.adminSmActivityRouter);
  app.use("/admin/sm-questionnaires", authoring.adminSmQuestionnairesRouter);
  app.use("/admin/sm-dashboard", reporting.adminSmDashboardRouter);
  app.use("/sm/dashboard", reporting.smHomeDashboardRouter);
  app.use("/sm/visits", runtime.smVisitsRouter);
  app.use("/admin/sm-planning", planningRoutes.adminSmPlanningRouter);
  app.use("/sm/planning", planningRoutes.smPlanningRouter);
  app.use("/admin/sm-activity/completed", managed.adminSmManagementRouter);
  app.use((error: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => res.status(500).json({ error: error.message }));
  const assignment = async () => {
    const [row] = await database.insert(schema.smAssignments).values({ idempotencyKey: randomUUID(), sourceType: "single", status: "planned",
      originalWorkDate: "2026-10-07", originalSmUserId: employee, originalSmMarketId: market, originalMarketInternalId: "SYNTHETIC-1", originalPlannedMinutes: 15,
      createdByUserId: admin, updatedByUserId: admin }).returning();
    return row!;
  };
  return { app, pg, database, schema, admin, employee, market, assignment, reporting };
}
