// Disposable manual/browser preview. Never starts the production backend or imports its environment.
import express from 'express';
import cors from 'cors';
import { randomUUID } from 'node:crypto';
import { eq } from 'drizzle-orm';
import { marketClassificationFixture } from './gm-market-classification-fixture.js';

const fixture = await marketClassificationFixture();
await fixture.database.update(fixture.schema.markets).set({ name: 'BILLA · Kühler Testmarkt', marketType: 'kuehler', universeMarket: false, flexNumber: null, cokeMasterNumber: 'PREVIEW-100', kuehlerStammnr: 'PREVIEW-100' }).where(eq(fixture.schema.markets.id, fixture.ids.market));
await fixture.database.insert(fixture.schema.markets).values([
  { id: randomUUID(), name: 'SPAR · Universum Testmarkt', dbName: 'SPAR', address: 'Teststraße 2', postalCode: '5020', city: 'Salzburg', region: 'West', marketType: 'universum', universeMarket: true, flexNumber: 'PREVIEW-FLEX-TAKEN' },
]);
await fixture.database.insert(fixture.schema.marketKuehlerUnits).values({ marketId: fixture.ids.market, name: 'Testkühler', kuehlerInternalId: 'PREVIEW-UNIT-1', kuehlerTechnicalIdentNo: 'PREVIEW-TECH-1' });
const user = { id: fixture.ids.admin, role: 'admin', email: 'gm-admin@preview.test', firstName: 'Preview', lastName: 'Admin' };
const app = express(); app.use(cors({ origin: 'http://127.0.0.1:3047' })); app.use(express.json());
app.post('/auth/login', (req, res) => {
  if (req.body.email !== user.email || req.body.password !== 'preview-only') { res.sendStatus(401); return; }
  res.json({ user, session: { accessToken: 'synthetic-gm-admin', refreshToken: 'synthetic-refresh', expiresAt: Math.floor(Date.now()/1000)+86400 } });
});
app.get('/auth/me', (_req, res) => res.json({ user }));
app.get('/health', (_req, res) => res.json({ isolated: true, synthetic: true }));
app.get('/red-month/current', (_req, res) => res.json({ current: { id: 'preview-red-month', start: '2026-10-05', end: '2026-10-30', label: 'RED 10', redYear: 2026, periodIndex: 10 }, config: { anchorStart: '2026-01-05', cycleWeeks: [4,4,5], timezone: 'Europe/Vienna' } }));
app.get('/admin/kurti/layout', (_req, res) => res.json({ layout: null }));
app.put('/admin/kurti/layout', (req, res) => res.json({ layout: req.body }));
app.patch('/admin/kurti/layout', (req, res) => res.json({ layout: req.body }));
app.post('/telemetry/events', (_req, res) => res.sendStatus(204));
app.use(fixture.app);
app.get(/.*/, (req, res) => {
  if (req.path.includes('red-month')) { res.json({ current: null, config: null, periods: [], years: [] }); return; }
  if (req.path.includes('agreement')) { res.json({ accepted: true, required: false }); return; }
  res.json({ modules: [], fragebogen: [], requests: [], chains: [], employees: [], campaigns: [], users: [], permissions: {}, unreadCount: 0 });
});
const server = app.listen(4047, '127.0.0.1', () => process.stdout.write('Synthetic GM market preview ready on 4047\n'));
const stop = () => server.close(async () => { await fixture.pg.close(); process.exit(0); });
process.on('SIGINT', stop); process.on('SIGTERM', stop);
