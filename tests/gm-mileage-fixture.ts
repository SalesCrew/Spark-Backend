import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { getTableColumns, getTableName, eq, and, desc } from "drizzle-orm";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import express from "express";
import cors from "cors";
import * as schema from "../src/lib/schema.js";
import * as mileage from "../src/lib/gm-mileage-service.js";
import * as shared from "../src/lib/admin-zeiterfassung.js";
import * as roles from "../src/lib/admin-role.js";
import * as exportsShared from "../src/lib/zeitenaufstellung-export.js";
import { isolatedModule } from "./isolated-module.js";

export async function createMileageFixture() {
  if (process.env.DATABASE_URL || process.env.SUPABASE_URL || process.env.SUPABASE_SERVICE_ROLE_KEY) throw new Error("Production environment is forbidden in mileage fixtures");
  const pg = new PGlite();
  const database = drizzle(pg, { schema });
  for (const table of [schema.users, schema.gmDaySessions, schema.gmDaySessionPauses, schema.timeEntryChangeRequests, schema.markets, schema.visitSessions, schema.visitSessionSections, schema.timeTrackingEntries, schema.lager, schema.lagerGmAssignments]) {
    const columns = Object.values(getTableColumns(table)).map(column => {
      const type = (column as { enumValues?: unknown }).enumValues ? 'text' : column.getSQLType();
      return `"${column.name}" ${type} ${column.name === 'id' ? 'primary key default gen_random_uuid()' : column.name === 'created_at' || column.name === 'updated_at' ? 'default now()' : column.dataType === 'boolean' ? 'default false' : ''}`;
    });
    await pg.exec(`create table "${getTableName(table)}" (${columns.join(',')})`);
  }
  await pg.exec(await readFile(new URL('../supabase/migrations/20261019144952_gm_vehicle_mileage_history.sql', import.meta.url), 'utf8'));
  const admin = randomUUID(), gm = randomUUID(), secondGm = randomUUID();
  await pg.query("insert into users(id,first_name,last_name,role,region,is_active) values ($1,'Preview','Admin','admin','Ost',true),($2,'Alex','Testfahrer','gm','Ost',true),($3,'Other','Driver','gm','Ost',true)", [admin, gm, secondGm]);
  const ids: string[] = [];
  const values = [[20261012,10000,10100],[20261013,22500,22600],[20261014,22615,22735],[20261015,10200,10300]];
  for (const [date,start,end] of values) { const id = randomUUID(); ids.push(id); const dateString = `${String(date).slice(0,4)}-${String(date).slice(4,6)}-${String(date).slice(6,8)}`;
    await pg.query(`insert into gm_day_sessions(id,gm_user_id,work_date,timezone,status,day_started_at,day_ended_at,start_km,end_km,is_start_km_completed,is_end_km_completed,submitted_at)
      values($1,$2,$3,'Europe/Vienna','submitted',$3::date + interval '6 hours',$3::date + interval '14 hours',$4,$5,true,true,$3::date + interval '14 hours')`, [id,gm,dateString,start,end]); }
  const service = mileage.createMileageService(database as unknown as Parameters<typeof mileage.createMileageService>[0]);
  const auth = { requireAuth: (allowed: string[]) => (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const role = req.header('authorization')?.replace('Bearer synthetic-', '');
    if (!role || !allowed.includes(role)) { res.sendStatus(403); return; }
    Object.assign(req, { authUser: { appUserId: role === 'gm' ? gm : admin, role } }); next();
  }};
  const logger = { logAction() {}, startActionTimer: () => 0n, logger: { error() {}, warn() {}, info() {} } };
  const dbModule = { db: database };
  const mileageModule = await isolatedModule<{ mileageRouter: express.Router; segmentSchema: unknown }>(new URL('../src/routes/gm-mileage.ts', import.meta.url), { '../lib/gm-mileage-service.js': mileage, '../lib/db.js': dbModule });
  const daysModule = await isolatedModule<{ loadZeiterfassungDaySessions: (...args: any[]) => any }>(new URL('../src/lib/zeiterfassung-days.ts', import.meta.url), { './admin-zeiterfassung.js': shared, './db.js': dbModule, './schema.js': schema, './gm-mileage-service.js': mileage });
  const adminModule = await isolatedModule<{ adminZeiterfassungRouter: express.Router }>(new URL('../src/routes/admin-zeiterfassung.ts', import.meta.url), {
    './gm-mileage.js': mileageModule, '../lib/gm-mileage-service.js': mileage, '../lib/admin-role.js': roles, '../lib/bonus-finalizer.js': {}, '../lib/gm-kpi-cache.js': {}, '../lib/ipp-finalizer.js': {}, '../lib/logger.js': logger, '../lib/admin-zeiterfassung.js': shared,
    '../lib/db.js': dbModule, '../lib/kunde-access.js': { requireKundeAdminPermission: (_req: unknown, _res: unknown, next: () => void) => next() }, '../lib/schema.js': schema, '../lib/zeitenaufstellung-export.js': exportsShared, '../middleware/auth.js': auth,
  });
  const dayModule = await isolatedModule<{ daySessionRouter: express.Router }>(new URL('../src/routes/day-session.ts', import.meta.url), {
    '../lib/gm-mileage-service.js': mileage, './gm-mileage.js': mileageModule, '../lib/admin-zeiterfassung.js': shared, '../lib/logger.js': logger, '../middleware/auth.js': auth, '../lib/db.js': dbModule, '../lib/schema.js': schema,
    '../lib/day-session.js': { DEFAULT_TIMEZONE: 'Europe/Vienna', toYmdInTimezone: shared.toYmdInTimezone, getCurrentDaySessionForUser: async ({ gmUserId: userId }: { gmUserId: string }) => (await database.select().from(schema.gmDaySessions).where(and(eq(schema.gmDaySessions.gmUserId,userId), eq(schema.gmDaySessions.isDeleted,false))).orderBy(desc(schema.gmDaySessions.workDate)).limit(1))[0] },
    '../lib/zeiterfassung-validation.js': { TimelineValidationError: class extends Error {} }, '../lib/zeiterfassung-days.js': daysModule,
  });
  const app = express(); app.use(cors({ origin: ['http://127.0.0.1:3037','http://localhost:3037'] })); app.use(express.json());
  app.use('/admin/zeiterfassung', adminModule.adminZeiterfassungRouter); app.use('/day-session', dayModule.daySessionRouter);
  app.use((error: any, _req: unknown, res: express.Response, _next: unknown) => res.status(error.status ?? 500).json({ error: error.message }));
  return { app, pg, database, service, ids, admin, gm, secondGm, close: () => pg.close() };
}
