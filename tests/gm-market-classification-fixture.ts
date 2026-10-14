import express from 'express';
import { readFile } from 'node:fs/promises';
import type { SQL } from 'drizzle-orm';
import { getTableConfig } from 'drizzle-orm/pg-core';
import { spezialfragenPeriodFixture } from './spezialfragen-period-fixture.js';
import { isolatedModule } from './isolated-module.js';
import * as classification from '../src/market-classification.shared.js';
import * as identity from '../src/market-identity.shared.js';
import * as billa from '../src/billa-gm-market-filter.shared.js';

export async function marketClassificationFixture() {
  const fixture = await spezialfragenPeriodFixture();
  const quote = (value: string) => '"' + value.replaceAll('"', '""') + '"';
  for (const table of [fixture.schema.marketKuehlerUnits, fixture.schema.visitSessions, fixture.schema.visitSessionSections, fixture.schema.visitSessionQuestions, fixture.schema.visitAnswers, fixture.schema.specialArthurFilter]) {
    const config = getTableConfig(table);
    for (const key of config.foreignKeys) {
      const ref = key.reference();
      await fixture.pg.exec('alter table ' + quote(config.name) + ' add constraint ' + quote(key.getName()) + ' foreign key (' + ref.columns.map(column => quote(column.name)).join(',') + ') references ' + quote(getTableConfig(ref.foreignTable).name) + ' (' + ref.foreignColumns.map(column => quote(column.name)).join(',') + ') on delete ' + (key.onDelete ?? 'no action'));
    }
  }
  const db = (target: any): any => new Proxy(target, { get(object, key) {
    if (key === 'execute') return async (query: SQL) => (await object.execute(query)).rows;
    if (key === 'transaction') return (action: (tx: unknown) => unknown) => object.transaction((tx: unknown) => action(db(tx)));
    const value = Reflect.get(object, key); return typeof value === 'function' ? value.bind(object) : value;
  } });
  const source = new URL('../src/routes/markets.ts', import.meta.url);
  const replacements: Record<string, unknown> = {};
  for (const match of (await readFile(source, 'utf8')).matchAll(/from\s+["']([^"']+)["']/g)) {
    if (match[1]!.startsWith('.')) replacements[match[1]!] = new Proxy({}, { get: (_target, key) => () => { throw new Error(`Forbidden operation: ${match[1]}:${String(key)}`); } });
  }
  const router = await isolatedModule<typeof import('../src/routes/markets.js')>(source, {
    ...replacements,
    '../lib/db.js': { db: db(fixture.database) }, '../lib/schema.js': fixture.schema,
    '../market-classification.shared.js': classification, '../market-identity.shared.js': identity,
    '../billa-gm-market-filter.shared.js': billa,
    '../lib/logger.js': { logger: { warn() {}, info() {}, error() {} }, logAction() {}, aggregateHighVolumeLoad() {}, markErrorAsLogged() {}, startActionTimer: () => 0 },
    '../middleware/auth.js': { requireAuth: (roles: string[]) => (req: express.Request, res: express.Response, next: express.NextFunction) => {
      const role = req.get('authorization') === 'Bearer synthetic-gm-admin' ? 'admin' : req.get('authorization') === 'Bearer synthetic-gm' ? 'gm' : null;
      if (!role) { res.sendStatus(401); return; } if (!roles.includes(role)) { res.sendStatus(403); return; }
      Object.assign(req, { authUser: { role, appUserId: role === 'admin' ? fixture.ids.admin : fixture.ids.gm } }); next();
    } },
    '../lib/kunde-access.js': { requireKundeAdminPermission: (_req: express.Request, _res: express.Response, next: express.NextFunction) => next() },
  });
  const app = express(); app.use(express.json());
  app.use('/markets', router.marketsRouter); app.use('/admin/markets', router.adminMarketsRouter);
  app.use((error: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => { res.status(500).json({ error: error.message }); });
  return { ...fixture, app };
}
