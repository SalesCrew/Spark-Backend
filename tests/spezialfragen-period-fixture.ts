// Actual GM authoring and visit routes; disposable synthetic DB, clock and external I/O.
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { PGlite } from '@electric-sql/pglite';
import { drizzle } from 'drizzle-orm/pglite';
import { is, SQL, sql } from 'drizzle-orm';
import { getTableConfig, PgDialect, PgTable } from 'drizzle-orm/pg-core';
import express from 'express';
import * as schema from '../src/lib/schema.js';
import * as period from '../src/lib/spezialfragen-period.js';
import * as persistence from '../src/lib/spezialfragen-persistence.js';
import * as deepCopy from '../src/lib/fragebogen-deep-copy.js';
import * as moduleLinks from '../src/lib/fragebogen-module-links.js';
import * as moduleCatalog from '../src/lib/module-catalog-state.js';
import * as sessionSync from '../src/lib/spezialfragen-session-sync.js';
import * as answerValidation from '../src/lib/visit-session-answer-validation.js';
import { modelDatabase } from '../src/lib/praemien-workspace.js';
import { isolatedModule } from './isolated-module.js';

export async function spezialfragenPeriodFixture(initialTime?: Date) {
  if (process.env.DATABASE_URL || process.env.SUPABASE_URL || process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.NODE_ENV === 'production') throw new Error('Production configuration is forbidden');
  const pg = new PGlite(), database = drizzle(pg, { schema }), dialect = new PgDialect();
  const quote = (v: string) => '"' + v.replaceAll('"', '""') + '"';
  for (const v of Object.values(schema)) {
    if (typeof v === 'function' && 'enumName' in v && 'enumValues' in v) await pg.exec('create type ' + quote(v.enumName as string) + ' as enum (' + (v.enumValues as string[]).map(x => "'" + x.replaceAll("'", "''") + "'").join(',') + ')');
  }
  const tables = Object.values(schema).filter(v => is(v, PgTable));
  // Real column types/defaults and unique keys support actual save/start transactions.
  for (const table of tables) {
    const c = getTableConfig(table);
    const fields = c.columns.map(column => {
      const v = column.default;
      const def = is(v, SQL) ? dialect.sqlToQuery(v).sql : typeof v === 'string' ? "'" + v.replaceAll("'", "''") + "'" : typeof v === 'number' || typeof v === 'boolean' ? String(v) : null;
      return quote(column.name) + ' ' + column.getSQLType() + (def ? ' default ' + def : '') + (column.notNull ? ' not null' : '') + (column.primary ? ' primary key' : '') + (column.isUnique ? ' unique' : '');
    });
    fields.push(...c.primaryKeys.map(k => 'primary key (' + k.columns.map(z => quote(z.name)).join(',') + ')'));
    fields.push(...c.uniqueConstraints.map(k => 'unique (' + k.columns.map(z => quote(z.name)).join(',') + ')'));
    await pg.exec('create table ' + quote(c.name) + ' (' + fields.join(',') + ')');
  }
  for (const table of tables) {
    const c = getTableConfig(table);
    for (const idx of c.indexes.filter(i => i.config.unique)) {
      const columns = idx.config.columns.map(z => is(z, SQL) ? dialect.sqlToQuery(z).sql.replaceAll(quote(c.name) + '.', '') : quote(z.name));
      const predicate = idx.config.where ? ' where ' + dialect.sqlToQuery(idx.config.where).sql.replaceAll(quote(c.name) + '.', '') : '';
      await pg.exec('create unique index ' + quote(idx.config.name!) + ' on ' + quote(c.name) + ' (' + columns.join(',') + ')' + predicate);
    }
  }
  let now = initialTime ? new Date(initialTime) : null;
  class Clock extends Date {
    constructor(value?: string | number | Date) { super(value === undefined ? now?.getTime() ?? Date.now() : value instanceof Date ? value.getTime() : value); }
    static now() { return now?.getTime() ?? Date.now(); }
  }
  const ids = { admin: randomUUID(), gm: randomUUID(), market: randomUUID() };
  await database.insert(schema.users).values([{ id: ids.admin, supabaseAuthId: randomUUID(), firstName: 'Synthetic', lastName: 'Admin', role: 'admin', email: 'gm-admin@preview.test' }, { id: ids.gm, supabaseAuthId: randomUUID(), firstName: 'Synthetic', lastName: 'GM', role: 'gm', email: 'gm@preview.test' }]);
  await database.insert(schema.markets).values({ id: ids.market, name: 'Synthetic Billa', dbName: 'Billa', region: 'Ost', address: 'Testgasse 1', postalCode: '1010', city: 'Wien' });
  const forbidden = (name: string) => () => { throw new Error('Unexpected external/job operation: ' + name); };
  const pass = (_req: express.Request, _res: express.Response, next: express.NextFunction) => next();
  const auth = { requireAuth: (roles: string[]) => (req: express.Request, res: express.Response, next: express.NextFunction) => {
    const token = req.get('authorization');
    const role = token === 'Bearer synthetic-gm-admin' ? 'admin' : token === 'Bearer synthetic-gm' ? 'gm' : token === 'Bearer synthetic-sm-admin' ? 'sm_admin' : token === 'Bearer synthetic-sm' ? 'sm' : null;
    if (!role) { res.sendStatus(401); return; } if (!roles.includes(role)) { res.sendStatus(403); return; }
    Object.assign(req, { authUser: { appUserId: role === 'gm' ? ids.gm : ids.admin, role } }); next();
  } };
  let activeTransaction: any = null;
  const asPostgres = (target: any): any => new Proxy(target, { get(object, key) {
    if (key === 'execute') return async (query: SQL) => (await (object === database && activeTransaction ? activeTransaction : object).execute(query)).rows;
    if (key === 'transaction') return (action: (tx: unknown) => unknown) => object.transaction(async (tx: unknown) => {
      activeTransaction = tx;
      try { return await action(asPostgres(tx)); } finally { activeTransaction = null; }
    });
    const v = Reflect.get(object, key); return typeof v === 'function' ? v.bind(object) : v;
  } });
  const db = asPostgres(database);
  // PGlite has one connection. Global readiness reads must share the synthetic transaction.
  const pgSql = async (parts: TemplateStringsArray, ...params: unknown[]) => {
    if (params.length) throw new Error('Only fixed schema metadata queries are expected');
    return activeTransaction ? (await activeTransaction.execute(sql.raw(parts.join('')))).rows : (await pg.query(parts.join(''))).rows;
  };
  const logger = { logger: { warn() {}, info() {}, error() {} }, logAction() {}, startActionTimer: () => 0, aggregateHighVolumeLoad() {}, getRequestLogMeta: () => ({}), markErrorAsLogged() {}, serializeError: (error: Error) => ({ message: error.message }) };
  const base = { '../lib/db.js': { db, sql: pgSql }, '../lib/schema.js': schema, '../lib/logger.js': logger, '../middleware/auth.js': auth, '../lib/spezialfragen-period.js': period };
  async function replacementsFor(file: string) {
    const result: Record<string, unknown> = {};
    const src = await readFile(new URL(file, import.meta.url), 'utf8');
    for (const match of src.matchAll(/from\s+["']([^"']+)["']/g)) if (match[1]!.startsWith('.')) result[match[1]!] = new Proxy({}, { get: (_target, key) => forbidden(match[1] + ':' + String(key)) });
    return { ...result, ...base };
  }
  const authoring = await isolatedModule<typeof import('../src/routes/fragebogen.js')>(new URL('../src/routes/fragebogen.ts', import.meta.url), {
    ...await replacementsFor('../src/routes/fragebogen.ts'),
    '../lib/kunde-access.js': { requireKundeAdminPermission: pass }, '../lib/spezialfragen-persistence.js': persistence,
    '../lib/fragebogen-deep-copy.js': deepCopy, '../lib/fragebogen-module-links.js': moduleLinks,
    '../lib/module-catalog-state.js': moduleCatalog,
    '../lib/praemien-workspace.js': { modelDatabase }, './module-catalog-state.js': { createModuleCatalogStateRouter: () => express.Router() },
  }, Clock as typeof Date);
  const visits = await isolatedModule<typeof import('../src/routes/gm-visit-sessions.js')>(new URL('../src/routes/gm-visit-sessions.ts', import.meta.url), {
    ...await replacementsFor('../src/routes/gm-visit-sessions.ts'), './fragebogen.js': authoring,
    '../lib/spezialfragen-session-sync.js': sessionSync, '../lib/visit-session-answer-validation.js': answerValidation,
    '../lib/day-session.js': { DEFAULT_TIMEZONE: 'Europe/Vienna', ensureGmSubmissionGate: async () => ({ ok: true }) },
    '../lib/visit-answer-reuse.js': { loadReusableSubmittedAnswers: async () => new Map() },
    '../lib/zeiterfassung-validation.js': { validateGmTimelineStartPoint: async () => {}, respondTimelineValidationError: () => false },
  }, Clock as typeof Date);
  const app = express(); app.use(express.json()); app.use('/admin', authoring.adminFragebogenRouter); app.use(visits.gmVisitSessionsRouter);
  app.use((error: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => res.status(500).json({ error: error.message }));
  return { app, database, pg, schema, ids, setTime: (value: string) => { now = new Date(value); } };
}
