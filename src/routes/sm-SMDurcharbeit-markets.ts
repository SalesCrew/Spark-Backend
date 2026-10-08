import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { and, eq, isNull, sql } from 'drizzle-orm';
import { Router } from 'express';
import { z } from 'zod';
import type { db as productionDb } from '../lib/db.js';
import { smMarkets, smSMDurcharbeitMarkets, users } from '../lib/schema.js';
import { lockSmPlanning } from '../sm-planning-lock.js';
import { SMDurcharbeitImportSchema, SMDurcharbeitNameKey, prepareSMDurcharbeitImport } from '../sm-SMDurcharbeit-market-import.shared.js';

class SMDurcharbeitMarketInputError extends Error {}

/** Mounted inside the authenticated SM market router; never merges with the standard market list. */
export function createSMDurcharbeitMarketsRouter(db: typeof productionDb, list: () => Promise<unknown[]>, map: (row: typeof smMarkets.$inferSelect) => unknown) {
  const router = Router();
  router.post('/import', async (req, res, next) => {
    const parsed = SMDurcharbeitImportSchema.safeParse(req.body);
    if (!parsed.success) { res.status(400).json({ error: 'Bitte die Datei und alle sieben Durcharbeit-Spalten korrekt zuweisen.' }); return; }
    let rows: ReturnType<typeof prepareSMDurcharbeitImport>;
    try { rows = prepareSMDurcharbeitImport(parsed.data); }
    catch (error) { res.status(400).json({ error: error instanceof Error ? error.message : 'Ungültige Spaltenzuweisung.' }); return; }
    const summary = { fileName: parsed.data.fileName, sheetName: parsed.data.sheetName, totalParsedRows: rows.length,
      created: 0, updated: 0, unchanged: 0, skipped: 0, matchedBy: { internalMarketId: 0, flexNumber: 0, namePostalAddress: 0 },
      skippedReasons: [] as Array<{ row: number; reason: string; sample: string }>, SMDurcharbeitDuplicateRows: 0, SMDurcharbeitUnassignedRows: 0 };
    try {
      await db.transaction(async tx => {
        await tx.execute(sql`set local lock_timeout = '5s'`);
        await tx.execute(sql`set local statement_timeout = '90s'`);
        await lockSmPlanning(tx);
        const sourceRows = await tx.select({ source: smSMDurcharbeitMarkets, market: smMarkets }).from(smSMDurcharbeitMarkets).innerJoin(smMarkets, eq(smMarkets.id, smSMDurcharbeitMarkets.smMarketId));
        const byKey = new Map(sourceRows.filter(row => row.source.SMDurcharbeitSourceKey).map(row => [row.source.SMDurcharbeitSourceKey!, row]));
        const people = await tx.select({ id: users.id, firstName: users.firstName, lastName: users.lastName }).from(users).where(and(eq(users.role, 'sm'), eq(users.isActive, true), isNull(users.deletedAt)));
        const byName = new Map<string, string[]>();
        for (const person of people) { const key = SMDurcharbeitNameKey(`${person.firstName} ${person.lastName}`); byName.set(key, [...(byName.get(key) ?? []), person.id]); }
        for (const row of rows) {
          if (row.error) { summary.skipped++; summary.skippedReasons.push({ row: row.row, reason: row.error, sample: `${row.chain} ${row.address}` }); continue; }
          if (row.duplicate) summary.SMDurcharbeitDuplicateRows++;
          const existing = byKey.get(row.sourceKey);
          if (existing?.market.isDeleted) { summary.skipped++; summary.skippedReasons.push({ row: row.row, reason: 'Dieser Durcharbeit-Markt wurde archiviert. Er wird durch einen Import nicht reaktiviert.', sample: row.name }); continue; }
          const matches = row.planner ? byName.get(SMDurcharbeitNameKey(row.planner)) ?? [] : [];
          const assignedSmUserId = existing?.market.assignedSmUserId ?? (matches.length === 1 ? matches[0]! : null);
          if (!assignedSmUserId) summary.SMDurcharbeitUnassignedRows++;
          const sourceValues = {
            SMDurcharbeitSourceKey: row.sourceKey, SMDurcharbeitVertriebstyp: row.chain, SMDurcharbeitFirmaBetrieb: row.company,
            SMDurcharbeitStrasse: row.address, SMDurcharbeitPlz: row.postalCode, SMDurcharbeitOrt: row.city,
            SMDurcharbeitEmEh: row.emEh, SMDurcharbeitVerplanung: row.planner, SMDurcharbeitSourceValues: row.sourceValues,
          };
          const marketValues = { name: row.name, dbName: row.company || row.chain, chain: row.chain, address: row.address, postalCode: row.postalCode, city: row.city, shelfMerchandiserName: row.planner, assignedSmUserId };
          if (existing) {
            summary.matchedBy.namePostalAddress++;
            const unchanged = Object.entries(marketValues).every(([key, value]) => existing.market[key as keyof typeof existing.market] === value)
              && Object.entries(sourceValues).every(([key, value]) => isDeepStrictEqual(existing.source[key as keyof typeof existing.source], value));
            if (unchanged) { summary.unchanged++; continue; }
            await tx.update(smMarkets).set({ ...marketValues, importSourceFileName: parsed.data.fileName, importedAt: new Date(), updatedAt: new Date() }).where(eq(smMarkets.id, existing.market.id));
            await tx.update(smSMDurcharbeitMarkets).set({ ...sourceValues, SMDurcharbeitSourceFile: parsed.data.fileName, SMDurcharbeitSourceSheet: parsed.data.sheetName, SMDurcharbeitSourceRow: row.row, SMDurcharbeitImportedAt: new Date() }).where(eq(smSMDurcharbeitMarkets.smMarketId, existing.market.id));
            summary.updated++;
          } else {
            const id = randomUUID();
            await tx.insert(smMarkets).values({ id, ...marketValues, internalMarketId: `SMD-${id}`, region: 'Ohne Region', importSourceFileName: parsed.data.fileName, importedAt: new Date() });
            await tx.insert(smSMDurcharbeitMarkets).values({ smMarketId: id, ...sourceValues, SMDurcharbeitSourceFile: parsed.data.fileName, SMDurcharbeitSourceSheet: parsed.data.sheetName, SMDurcharbeitSourceRow: row.row, SMDurcharbeitImportedAt: new Date() });
            summary.created++;
          }
        }
      });
      res.json({ markets: await list(), summary });
    } catch (error) { next(error); }
  });
  const createSchema = z.object({ internalMarketId: z.string().max(200).optional(), flexNumber: z.string().nullable().optional(),
    name: z.string().trim().min(1).max(500), dbName: z.string().max(500).optional(), chain: z.string().trim().min(1).max(500),
    address: z.string().trim().min(1).max(1000), postalCode: z.string().regex(/^\d{4}$/), city: z.string().trim().min(1).max(300),
    region: z.string().max(200).optional(), adminInfoNote: z.string().max(10000).optional(),
    assignedSmUserId: z.string().uuid().nullable().optional(), fieldServiceManagerUserId: z.string().uuid().nullable().optional(), isActive: z.boolean().optional() }).strict();
  router.post('/', async (req, res, next) => {
    const parsed = createSchema.safeParse(req.body);
    if (!parsed.success) { res.status(400).json({ error: 'Bitte die Pflichtfelder des Durcharbeit-Markts korrekt ausfüllen.' }); return; }
    try {
      const created = await db.transaction(async tx => {
        await lockSmPlanning(tx);
        const input = parsed.data;
        let planner = '';
        for (const [userId, role] of [[input.assignedSmUserId, 'sm'], [input.fieldServiceManagerUserId, 'gm']] as const) {
          if (!userId) continue;
          const [user] = await tx.select().from(users).where(and(eq(users.id, userId), eq(users.role, role), eq(users.isActive, true), isNull(users.deletedAt))).limit(1);
          if (!user) throw new SMDurcharbeitMarketInputError('Der ausgewählte Mitarbeiter ist nicht aktiv oder hat nicht die passende Rolle.');
          if (role === 'sm') planner = `${user.firstName} ${user.lastName}`;
        }
        const id = randomUUID();
        const [market] = await tx.insert(smMarkets).values({ ...input, id, internalMarketId: `SMD-${id}`, flexNumber: null, region: input.region || 'Ohne Region', shelfMerchandiserName: planner }).returning();
        const source = { Vertriebstyp: input.chain, 'Firma/Betrieb': input.name, Straße: input.address, PLZ: input.postalCode, Ort: input.city, 'EM/EH': '', Verplanung: planner };
        await tx.insert(smSMDurcharbeitMarkets).values({ smMarketId: id, SMDurcharbeitVertriebstyp: input.chain, SMDurcharbeitFirmaBetrieb: input.name, SMDurcharbeitStrasse: input.address, SMDurcharbeitPlz: input.postalCode, SMDurcharbeitOrt: input.city, SMDurcharbeitEmEh: '', SMDurcharbeitVerplanung: planner, SMDurcharbeitSourceValues: source });
        return market!;
      });
      res.status(201).json({ market: { ...(map(created) as object), SMDurcharbeitMarket: true } });
    } catch (error) {
      if (error instanceof SMDurcharbeitMarketInputError) { res.status(400).json({ error: error.message }); return; }
      next(error);
    }
  });
  return router;
}
