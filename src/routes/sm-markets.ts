import { and, asc, eq, inArray, isNull, sql } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import { logAction, startActionTimer } from "../lib/logger.js";
import { smMarkets, users } from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { resolveAutomaticSmNameMatch } from "../sm-market-user-sync.shared.js";
import { lockSmPlanning } from "../sm-planning-lock.js";
import { deactivateSmMarket, loadSmMarketDeactivationPreview, smMarketDeactivationSchema, SmMarketDeactivationError } from "../sm-market-deactivation.js";

export const adminSmMarketsRouter = Router();

const smMarketImportFieldKeys = [
  "flexNumber",
  "internalMarketId",
  "name",
  "address",
  "postalCode",
  "city",
  "region",
  "serviceDaysPerWeek",
  "mondayHours",
  "tuesdayHours",
  "wednesdayHours",
  "thursdayHours",
  "fridayHours",
  "weeklyHours",
  "shelfMerchandiserName",
  "fieldServiceManagerName",
  "sourceInfo",
  "isActive",
] as const;

type SmMarketImportFieldKey = (typeof smMarketImportFieldKeys)[number];
type SmMarketColumnMapping = Partial<Record<SmMarketImportFieldKey, string>>;

const importCellSchema = z.union([z.string().max(20_000), z.number(), z.boolean(), z.null()]);
const smMarketMappingSchema = z
  .object(Object.fromEntries(smMarketImportFieldKeys.map((key) => [key, z.string().max(3).optional()])) as Record<SmMarketImportFieldKey, z.ZodOptional<z.ZodString>>)
  .strict();

const importSmMarketsSchema = z
  .object({
    fileName: z.string().trim().min(1).max(260),
    sheetName: z.string().trim().min(1).max(260),
    rows: z.array(z.array(importCellSchema).max(256)).min(1).max(20_001),
    mapping: smMarketMappingSchema,
  })
  .strict();

const createSmMarketSchema = z
  .object({
    internalMarketId: z.string().trim().min(1).max(200),
    flexNumber: z.string().trim().max(200).nullable().optional(),
    name: z.string().trim().min(1).max(500),
    dbName: z.string().trim().max(500).optional(),
    chain: z.string().trim().min(1).max(500),
    address: z.string().trim().min(1).max(1_000),
    postalCode: z.string().trim().min(1).max(50),
    city: z.string().trim().min(1).max(300),
    region: z.string().trim().min(1).max(200),
    adminInfoNote: z.string().trim().max(10_000).optional(),
    assignedSmUserId: z.string().uuid().nullable().optional(),
    isActive: z.boolean().optional(),
  })
  .strict();

const updateSmMarketSchema = createSmMarketSchema
  .omit({ internalMarketId: true })
  .extend({
    internalMarketId: z.string().trim().min(1).max(200).optional(),
  })
  .partial()
  .strict();

const manualSmUserMatchSchema = z
  .object({
    marketIds: z.array(z.string().uuid()).min(1).max(500),
    smUserId: z.string().uuid(),
  })
  .strict();

const weekdayFields = [
  "mondayHours",
  "tuesdayHours",
  "wednesdayHours",
  "thursdayHours",
  "fridayHours",
] as const;

function normalizeIdentity(value: unknown): string | null {
  const normalized = String(value ?? "").trim();
  return normalized.length > 0 ? normalized : null;
}

function identityKey(value: unknown): string {
  return String(value ?? "").trim().toLocaleLowerCase("de-AT");
}

function locationKey(input: { name: unknown; postalCode: unknown; address: unknown }): string {
  return [input.name, input.postalCode, input.address]
    .map((value) => identityKey(value).replace(/\s+/g, " "))
    .join("|");
}

function isValidColumnLetter(value: string | null | undefined): value is string {
  return typeof value === "string" && /^[A-Za-z]{1,3}$/.test(value.trim());
}

function excelColumnToIndex(value: string): number {
  let index = 0;
  for (const char of value.trim().toUpperCase()) {
    index = index * 26 + (char.charCodeAt(0) - 64);
  }
  return index - 1;
}

function mappedCell(row: string[], mapping: SmMarketColumnMapping, key: SmMarketImportFieldKey): string {
  const column = mapping[key];
  if (!isValidColumnLetter(column)) return "";
  return row[excelColumnToIndex(column)]?.trim() ?? "";
}

function parseDecimal(value: string): number | null {
  const normalized = value.trim().replace(/\s/g, "").replace(",", ".");
  if (!normalized) return null;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : Number.NaN;
}

function parseImportBoolean(value: string): boolean | null {
  const normalized = value.trim().toLocaleLowerCase("de-AT");
  if (!normalized) return null;
  if (["1", "ja", "j", "yes", "true", "aktiv", "active"].includes(normalized)) return true;
  if (["0", "nein", "n", "no", "false", "inaktiv", "inactive"].includes(normalized)) return false;
  return null;
}

function mapSmMarketRow(row: typeof smMarkets.$inferSelect) {
  const weekdayHours = {
    ...(row.mondayHours == null ? {} : { mo: Number(row.mondayHours) }),
    ...(row.tuesdayHours == null ? {} : { di: Number(row.tuesdayHours) }),
    ...(row.wednesdayHours == null ? {} : { mi: Number(row.wednesdayHours) }),
    ...(row.thursdayHours == null ? {} : { do: Number(row.thursdayHours) }),
    ...(row.fridayHours == null ? {} : { fr: Number(row.fridayHours) }),
  };
  return {
    id: row.id,
    internalId: row.internalMarketId ?? "",
    flexNumber: row.flexNumber ?? undefined,
    masterNumber: row.internalMarketId ?? undefined,
    name: row.name,
    dbName: row.dbName,
    chain: row.chain,
    address: row.address,
    postalCode: row.postalCode,
    city: row.city,
    region: row.region,
    infoFlag: row.adminInfoNote.trim().length > 0,
    infoNote: row.adminInfoNote,
    isActive: row.isActive,
    serviceDaysPerWeek: row.serviceDaysPerWeek,
    weekdayHours,
    weeklyHours: Number(row.weeklyHours),
    shelfMerchandiserName: row.shelfMerchandiserName,
    assignedSmUserId: row.assignedSmUserId,
    fieldServiceManagerName: row.fieldServiceManagerName,
    sourceInfo: row.sourceInfo,
    importSourceFileName: row.importSourceFileName,
    importedAt: row.importedAt?.toISOString() ?? null,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  };
}

function postgresConstraint(error: unknown): string | null {
  let current: unknown = error;
  for (let depth = 0; depth < 5 && current && typeof current === "object"; depth += 1) {
    const record = current as { constraint_name?: unknown; constraint?: unknown; cause?: unknown };
    const constraint = record.constraint_name ?? record.constraint;
    if (typeof constraint === "string") return constraint;
    current = record.cause;
  }
  return null;
}

function isUniqueViolation(error: unknown): boolean {
  let current: unknown = error;
  for (let depth = 0; depth < 5 && current && typeof current === "object"; depth += 1) {
    const record = current as { code?: unknown; cause?: unknown };
    if (record.code === "23505") return true;
    current = record.cause;
  }
  return false;
}

function validateMapping(mapping: SmMarketColumnMapping): string | null {
  const required: SmMarketImportFieldKey[] = ["name", "address", "postalCode", "city", "region"];
  for (const key of required) {
    if (!isValidColumnLetter(mapping[key])) return `Pflichtfeld ${key} ist nicht korrekt gemappt.`;
  }
  if (!isValidColumnLetter(mapping.flexNumber) && !isValidColumnLetter(mapping.internalMarketId)) {
    return "Flexnummer oder Stammnummern muss als Identität gemappt sein.";
  }
  const mappedColumns = Object.entries(mapping)
    .filter((entry): entry is [SmMarketImportFieldKey, string] => isValidColumnLetter(entry[1]))
    .map(([key, column]) => [key, column.trim().toUpperCase()] as const);
  const duplicate = mappedColumns.find(([, column], index) => mappedColumns.findIndex(([, value]) => value === column) !== index);
  if (duplicate) return `Excel-Spalte ${duplicate[1]} ist mehrfach zugewiesen.`;
  const summaryMapped = isValidColumnLetter(mapping.serviceDaysPerWeek) || isValidColumnLetter(mapping.weeklyHours);
  if (summaryMapped && !weekdayFields.every((key) => isValidColumnLetter(mapping[key]))) {
    return "Für die Prüfung von Betreuungstagen oder Wochenstunden müssen Mo bis Fr gemappt sein.";
  }
  return null;
}

async function loadSmMarkets() {
  return db
    .select()
    .from(smMarkets)
    .where(eq(smMarkets.isDeleted, false))
    .orderBy(asc(smMarkets.chain), asc(smMarkets.name), asc(smMarkets.postalCode));
}

async function isAssignableSmUser(userId: string): Promise<boolean> {
  const [user] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(
      eq(users.id, userId),
      eq(users.role, "sm"),
      eq(users.isActive, true),
      sql`${users.deletedAt} is null`,
    ))
    .limit(1);
  return Boolean(user);
}

function smUserDisplayName(user: { firstName: string; lastName: string }): string {
  return `${user.firstName} ${user.lastName}`.trim();
}

adminSmMarketsRouter.use(requireAuth(["admin", "sm_admin"]));

adminSmMarketsRouter.get("/", async (_req, res, next) => {
  try {
    const rows = await loadSmMarkets();
    res.status(200).json({ markets: rows.map(mapSmMarketRow) });
  } catch (error) {
    next(error);
  }
});

adminSmMarketsRouter.post("/sync-sm-users", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`set local lock_timeout = '5s'`);
      await tx.execute(sql`set local statement_timeout = '60s'`);
      const lock = await tx.execute<{ locked: boolean }>(sql`select pg_try_advisory_xact_lock(47110334) as locked`);
      if (!lock[0]?.locked) throw new Error("SM_MARKET_SYNC_IN_PROGRESS");

      const marketRows = await tx.select().from(smMarkets).where(eq(smMarkets.isDeleted, false)).orderBy(asc(smMarkets.chain), asc(smMarkets.name));
      const smUserRows = await tx.select({ id: users.id, firstName: users.firstName, lastName: users.lastName, email: users.email })
        .from(users)
        .where(and(eq(users.role, "sm"), eq(users.isActive, true), isNull(users.deletedAt)))
        .orderBy(asc(users.lastName), asc(users.firstName));
      const candidates = smUserRows.map((user) => ({ id: user.id, name: smUserDisplayName(user) }));
      const usersById = new Map(smUserRows.map((user) => [user.id, user]));
      const plannedMatches: Array<{
        market: typeof smMarkets.$inferSelect;
        smUserId: string;
        smName: string;
        score: number;
        method: "exact" | "fuzzy";
      }> = [];
      const unmatched: Array<{
        marketId: string;
        marketName: string;
        marketAddress: string;
        importedName: string;
        suggestions: Array<{ smUserId: string; smName: string; email: string; score: number }>;
      }> = [];
      let skippedAlreadyMatched = 0;
      let withoutImportedName = 0;

      for (const market of marketRows) {
        if (market.assignedSmUserId) {
          skippedAlreadyMatched += 1;
          continue;
        }
        const importedName = market.shelfMerchandiserName.trim();
        if (!importedName) withoutImportedName += 1;
        const resolution = importedName
          ? resolveAutomaticSmNameMatch(importedName, candidates)
          : { match: null, method: null, suggestions: [] };
        if (resolution.match && resolution.method) {
          plannedMatches.push({
            market,
            smUserId: resolution.match.id,
            smName: resolution.match.name,
            score: resolution.match.score,
            method: resolution.method,
          });
          continue;
        }
        unmatched.push({
          marketId: market.id,
          marketName: market.name,
          marketAddress: `${market.address}, ${market.postalCode} ${market.city}`,
          importedName,
          suggestions: resolution.suggestions.map((suggestion) => {
            const user = usersById.get(suggestion.id);
            return {
              smUserId: suggestion.id,
              smName: suggestion.name,
              email: user?.email ?? "",
              score: suggestion.score,
            };
          }),
        });
      }

      const plannedByUserId = new Map<string, string[]>();
      for (const planned of plannedMatches) {
        plannedByUserId.set(planned.smUserId, [...(plannedByUserId.get(planned.smUserId) ?? []), planned.market.id]);
      }
      const updatedIds = new Set<string>();
      const now = new Date();
      for (const [smUserId, marketIds] of plannedByUserId) {
        const updated = await tx.update(smMarkets)
          .set({ assignedSmUserId: smUserId, updatedAt: now })
          .where(and(inArray(smMarkets.id, marketIds), eq(smMarkets.isDeleted, false), isNull(smMarkets.assignedSmUserId)))
          .returning({ id: smMarkets.id });
        for (const row of updated) updatedIds.add(row.id);
      }
      skippedAlreadyMatched += plannedMatches.length - updatedIds.size;
      const matched = plannedMatches
        .filter((planned) => updatedIds.has(planned.market.id))
        .map((planned) => ({
          marketId: planned.market.id,
          marketName: planned.market.name,
          marketAddress: `${planned.market.address}, ${planned.market.postalCode} ${planned.market.city}`,
          importedName: planned.market.shelfMerchandiserName,
          smUserId: planned.smUserId,
          smName: planned.smName,
          score: planned.score,
          method: planned.method,
        }));
      return {
        summary: {
          scanned: marketRows.length,
          matched: matched.length,
          unmatched: unmatched.length,
          skippedAlreadyMatched,
          withoutImportedName,
          activeSmUsers: smUserRows.length,
        },
        matched,
        unmatched,
      };
    });

    const fresh = await loadSmMarkets();
    logAction("info", "sm_market_user_sync_completed", {
      req,
      action: "sm_market_user_sync",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: result.summary,
    });
    res.status(200).json({ ...result, markets: fresh.map(mapSmMarketRow) });
  } catch (error) {
    if (error instanceof Error && error.message === "SM_MARKET_SYNC_IN_PROGRESS") {
      res.status(409).json({ error: "Ein SM-Marktimport oder eine andere SM-Synchronisierung läuft bereits. Bitte gleich erneut versuchen." });
      return;
    }
    next(error);
  }
});

adminSmMarketsRouter.post("/sync-sm-users/manual", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const parsed = manualSmUserMatchSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige manuelle SM-Zuordnung." });
      return;
    }
    const marketIds = [...new Set(parsed.data.marketIds)];
    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`set local lock_timeout = '5s'`);
      const lock = await tx.execute<{ locked: boolean }>(sql`select pg_try_advisory_xact_lock(47110334) as locked`);
      if (!lock[0]?.locked) throw new Error("SM_MARKET_SYNC_IN_PROGRESS");
      const [smUser] = await tx.select({ id: users.id, firstName: users.firstName, lastName: users.lastName, email: users.email })
        .from(users)
        .where(and(eq(users.id, parsed.data.smUserId), eq(users.role, "sm"), eq(users.isActive, true), isNull(users.deletedAt)))
        .limit(1);
      if (!smUser) throw new Error("SM_USER_NOT_ASSIGNABLE");
      const updated = await tx.update(smMarkets)
        .set({ assignedSmUserId: smUser.id, updatedAt: new Date() })
        .where(and(inArray(smMarkets.id, marketIds), eq(smMarkets.isDeleted, false), isNull(smMarkets.assignedSmUserId)))
        .returning();
      return {
        matched: updated.map((market) => ({
          marketId: market.id,
          marketName: market.name,
          marketAddress: `${market.address}, ${market.postalCode} ${market.city}`,
          importedName: market.shelfMerchandiserName,
          smUserId: smUser.id,
          smName: smUserDisplayName(smUser),
          score: 1,
          method: "manual" as const,
        })),
        skippedAlreadyMatched: marketIds.length - updated.length,
      };
    });
    const fresh = await loadSmMarkets();
    logAction("info", "sm_market_user_manual_match_completed", {
      req,
      action: "sm_market_user_manual_match",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { requested: marketIds.length, matched: result.matched.length, skippedAlreadyMatched: result.skippedAlreadyMatched },
    });
    res.status(200).json({ ...result, markets: fresh.map(mapSmMarketRow) });
  } catch (error) {
    if (error instanceof Error && error.message === "SM_USER_NOT_ASSIGNABLE") {
      res.status(400).json({ error: "Der ausgewählte Shelf Merchandiser ist nicht aktiv oder kein SM-Account." });
      return;
    }
    if (error instanceof Error && error.message === "SM_MARKET_SYNC_IN_PROGRESS") {
      res.status(409).json({ error: "Ein SM-Marktimport oder eine andere SM-Synchronisierung läuft bereits. Bitte gleich erneut versuchen." });
      return;
    }
    next(error);
  }
});

adminSmMarketsRouter.post("/import", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const parsed = importSmMarketsSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Importdatei oder Spaltenzuweisung." });
      return;
    }
    const payload = parsed.data;
    const mapping = payload.mapping as SmMarketColumnMapping;
    const mappingError = validateMapping(mapping);
    if (mappingError) {
      res.status(400).json({ error: mappingError });
      return;
    }

    const rows = payload.rows.map((row) => row.map((cell) => (cell == null ? "" : String(cell))));
    const dataRows = rows.slice(1);
    const summary = {
      fileName: payload.fileName,
      sheetName: payload.sheetName,
      totalParsedRows: dataRows.length,
      created: 0,
      updated: 0,
      unchanged: 0,
      skipped: 0,
      matchedBy: { internalMarketId: 0, flexNumber: 0, namePostalAddress: 0 },
      skippedReasons: [] as Array<{ row: number; reason: string; sample: string }>,
    };

    await db.transaction(async (tx) => {
      await tx.execute(sql`set local lock_timeout = '5s'`);
      await tx.execute(sql`set local statement_timeout = '90s'`);
      const lock = await tx.execute<{ locked: boolean }>(sql`select pg_try_advisory_xact_lock(47110334) as locked`);
      if (!lock[0]?.locked) throw new Error("SM_IMPORT_IN_PROGRESS");

      await lockSmPlanning(tx);
      const existingRows = await tx.select().from(smMarkets).where(eq(smMarkets.isDeleted, false));
      const assignableSmUsers = await tx
        .select({ id: users.id, firstName: users.firstName, lastName: users.lastName })
        .from(users)
        .where(and(eq(users.role, "sm"), eq(users.isActive, true), sql`${users.deletedAt} is null`));
      const assignableSmUserIdsByName = new Map<string, string[]>();
      for (const user of assignableSmUsers) {
        const key = identityKey(`${user.firstName} ${user.lastName}`);
        const ids = assignableSmUserIdsByName.get(key) ?? [];
        assignableSmUserIdsByName.set(key, [...ids, user.id]);
      }
      const byInternalId = new Map<string, typeof smMarkets.$inferSelect>();
      const byFlexNumber = new Map<string, typeof smMarkets.$inferSelect>();
      const byLocation = new Map<string, Array<typeof smMarkets.$inferSelect>>();
      const registerLocation = (market: typeof smMarkets.$inferSelect) => {
        const key = locationKey(market);
        const bucket = byLocation.get(key) ?? [];
        if (!bucket.some((entry) => entry.id === market.id)) byLocation.set(key, [...bucket, market]);
      };
      const unregisterLocation = (market: typeof smMarkets.$inferSelect) => {
        const key = locationKey(market);
        const bucket = (byLocation.get(key) ?? []).filter((entry) => entry.id !== market.id);
        if (bucket.length > 0) byLocation.set(key, bucket);
        else byLocation.delete(key);
      };
      for (const existingRow of existingRows) {
        const internalKey = identityKey(existingRow.internalMarketId);
        const flexKey = identityKey(existingRow.flexNumber);
        if (internalKey) byInternalId.set(internalKey, existingRow);
        if (flexKey) byFlexNumber.set(flexKey, existingRow);
        registerLocation(existingRow);
      }
      const touchedIds = new Set<string>();

      for (let index = 0; index < dataRows.length; index += 1) {
        const row = dataRows[index];
        if (!row) continue;
        const rowNumber = index + 2;
        if (row.every((cell) => !cell.trim())) {
          summary.skipped += 1;
          continue;
        }

        const sample = mappedCell(row, mapping, "name") || row.find((cell) => cell.trim()) || `Zeile ${rowNumber}`;
        const internalMarketId = normalizeIdentity(mappedCell(row, mapping, "internalMarketId"));
        const flexNumber = normalizeIdentity(mappedCell(row, mapping, "flexNumber"));
        const name = mappedCell(row, mapping, "name");
        const address = mappedCell(row, mapping, "address");
        const postalCode = mappedCell(row, mapping, "postalCode");
        const city = mappedCell(row, mapping, "city");
        const region = mappedCell(row, mapping, "region");
        const requiredMissing = [
          ["Markt", name],
          ["Adresse", address],
          ["PLZ", postalCode],
          ["Ort", city],
          ["Region", region],
        ].filter(([, value]) => !value);
        if (requiredMissing.length > 0) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) {
            const missing = requiredMissing.map(([label]) => label);
            summary.skippedReasons.push({ row: rowNumber, reason: `Pflichtfelder fehlen: ${missing.join(", ")}`, sample });
          }
          continue;
        }
        if (!internalMarketId && !flexNumber) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) {
            summary.skippedReasons.push({ row: rowNumber, reason: "Flexnummer oder Stammnummern fehlt in dieser Zeile.", sample });
          }
          continue;
        }

        const parsedWeekdays: Record<(typeof weekdayFields)[number], number | null> = {
          mondayHours: null,
          tuesdayHours: null,
          wednesdayHours: null,
          thursdayHours: null,
          fridayHours: null,
        };
        let invalidHours: string | null = null;
        for (const key of weekdayFields) {
          if (!isValidColumnLetter(mapping[key])) continue;
          const raw = mappedCell(row, mapping, key);
          const value = parseDecimal(raw);
          if (Number.isNaN(value) || (value != null && (value <= 0 || value > 24))) {
            invalidHours = `${key}: ${raw || "leer"}`;
            break;
          }
          parsedWeekdays[key] = value;
        }
        if (invalidHours) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) summary.skippedReasons.push({ row: rowNumber, reason: `Ungültige Stunden (${invalidHours})`, sample });
          continue;
        }

        const derivedServiceDays = Object.values(parsedWeekdays).filter((value) => value != null).length;
        const derivedWeeklyHours = Object.values(parsedWeekdays).reduce<number>((sum, value) => sum + (value ?? 0), 0);
        if (isValidColumnLetter(mapping.serviceDaysPerWeek)) {
          const sourceDays = parseDecimal(mappedCell(row, mapping, "serviceDaysPerWeek"));
          if (sourceDays == null || Number.isNaN(sourceDays) || !Number.isInteger(sourceDays) || sourceDays !== derivedServiceDays) {
            summary.skipped += 1;
            if (summary.skippedReasons.length < 50) summary.skippedReasons.push({ row: rowNumber, reason: `Betreuungstage stimmen nicht mit Mo–Fr überein (Datei: ${mappedCell(row, mapping, "serviceDaysPerWeek") || "leer"}, berechnet: ${derivedServiceDays})`, sample });
            continue;
          }
        }
        if (isValidColumnLetter(mapping.weeklyHours)) {
          const sourceHours = parseDecimal(mappedCell(row, mapping, "weeklyHours"));
          if (sourceHours == null || Number.isNaN(sourceHours) || Math.abs(sourceHours - derivedWeeklyHours) > 0.001) {
            summary.skipped += 1;
            if (summary.skippedReasons.length < 50) summary.skippedReasons.push({ row: rowNumber, reason: `Wochenstunden stimmen nicht mit Mo–Fr überein (Datei: ${mappedCell(row, mapping, "weeklyHours") || "leer"}, berechnet: ${derivedWeeklyHours})`, sample });
            continue;
          }
        }

        let isActive: boolean | undefined;
        if (isValidColumnLetter(mapping.isActive)) {
          const rawStatus = mappedCell(row, mapping, "isActive");
          if (rawStatus) {
            const parsedStatus = parseImportBoolean(rawStatus);
            if (parsedStatus == null) {
              summary.skipped += 1;
              if (summary.skippedReasons.length < 50) summary.skippedReasons.push({ row: rowNumber, reason: `Ungültiger Status: ${rawStatus}`, sample });
              continue;
            }
            isActive = parsedStatus;
          }
        }

        const internalMatch = internalMarketId ? byInternalId.get(identityKey(internalMarketId)) : undefined;
        const flexMatch = flexNumber ? byFlexNumber.get(identityKey(flexNumber)) : undefined;
        if (internalMatch && flexMatch && internalMatch.id !== flexMatch.id) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) summary.skippedReasons.push({ row: rowNumber, reason: "Stammnummern und Flexnummer gehören zu unterschiedlichen bestehenden SM-Märkten.", sample });
          continue;
        }
        const locationMatches = byLocation.get(locationKey({ name, postalCode, address })) ?? [];
        if (!internalMatch && !flexMatch && locationMatches.length > 1) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) summary.skippedReasons.push({ row: rowNumber, reason: "Markt, PLZ und Adresse passen zu mehreren bestehenden SM-Märkten.", sample });
          continue;
        }
        const locationMatch = locationMatches.length === 1 ? locationMatches[0] : undefined;
        const existing = internalMatch ?? flexMatch ?? locationMatch;
        if (existing?.isActive && isActive === false) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) summary.skippedReasons.push({ row: rowNumber, reason: "Bitte diesen Markt auf der Marktseite deaktivieren und dort über betroffene Einsätze entscheiden. Die Importzeile wurde nicht gespeichert.", sample });
          continue;
        }
        if (existing && touchedIds.has(existing.id)) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) summary.skippedReasons.push({ row: rowNumber, reason: "Doppelter Markt innerhalb der Importdatei.", sample });
          continue;
        }

        const importedShelfMerchandiserName = isValidColumnLetter(mapping.shelfMerchandiserName)
          ? mappedCell(row, mapping, "shelfMerchandiserName")
          : null;
        const matchingSmUserIds = importedShelfMerchandiserName
          ? assignableSmUserIdsByName.get(identityKey(importedShelfMerchandiserName)) ?? []
          : [];
        const resolvedAssignedSmUserId = matchingSmUserIds.length === 1 ? matchingSmUserIds[0] : undefined;
        const importableValues = {
          ...(internalMarketId ? { internalMarketId } : {}),
          ...(flexNumber ? { flexNumber } : {}),
          name,
          dbName: name,
          chain: name,
          address,
          postalCode,
          city,
          region,
          ...(isValidColumnLetter(mapping.mondayHours) ? { mondayHours: parsedWeekdays.mondayHours == null ? null : String(parsedWeekdays.mondayHours) } : {}),
          ...(isValidColumnLetter(mapping.tuesdayHours) ? { tuesdayHours: parsedWeekdays.tuesdayHours == null ? null : String(parsedWeekdays.tuesdayHours) } : {}),
          ...(isValidColumnLetter(mapping.wednesdayHours) ? { wednesdayHours: parsedWeekdays.wednesdayHours == null ? null : String(parsedWeekdays.wednesdayHours) } : {}),
          ...(isValidColumnLetter(mapping.thursdayHours) ? { thursdayHours: parsedWeekdays.thursdayHours == null ? null : String(parsedWeekdays.thursdayHours) } : {}),
          ...(isValidColumnLetter(mapping.fridayHours) ? { fridayHours: parsedWeekdays.fridayHours == null ? null : String(parsedWeekdays.fridayHours) } : {}),
          ...(importedShelfMerchandiserName == null ? {} : { shelfMerchandiserName: importedShelfMerchandiserName }),
          ...(resolvedAssignedSmUserId === undefined ? {} : { assignedSmUserId: resolvedAssignedSmUserId }),
          ...(isValidColumnLetter(mapping.fieldServiceManagerName) ? { fieldServiceManagerName: mappedCell(row, mapping, "fieldServiceManagerName") } : {}),
          ...(isValidColumnLetter(mapping.sourceInfo) ? { sourceInfo: mappedCell(row, mapping, "sourceInfo") } : {}),
          ...(isActive === undefined ? {} : { isActive }),
        } satisfies Partial<typeof smMarkets.$inferInsert>;

        if (existing) {
          const changedValues = Object.fromEntries(Object.entries(importableValues).filter(([key, value]) => (
            String(existing[key as keyof typeof existing] ?? "") !== String(value ?? "")
          ))) as Partial<typeof smMarkets.$inferInsert>;
          touchedIds.add(existing.id);
          if (internalMatch) summary.matchedBy.internalMarketId += 1;
          else if (flexMatch) summary.matchedBy.flexNumber += 1;
          else summary.matchedBy.namePostalAddress += 1;
          if (Object.keys(changedValues).length === 0) {
            summary.unchanged += 1;
            continue;
          }
          const [updated] = await tx.update(smMarkets).set({
            ...changedValues,
            importSourceFileName: payload.fileName,
            importedAt: new Date(),
            updatedAt: new Date(),
          }).where(and(eq(smMarkets.id, existing.id), eq(smMarkets.isDeleted, false))).returning();
          if (!updated) {
            summary.skipped += 1;
            continue;
          }
          summary.updated += 1;
          if (existing.internalMarketId) byInternalId.delete(identityKey(existing.internalMarketId));
          if (existing.flexNumber) byFlexNumber.delete(identityKey(existing.flexNumber));
          unregisterLocation(existing);
          if (updated.internalMarketId) byInternalId.set(identityKey(updated.internalMarketId), updated);
          if (updated.flexNumber) byFlexNumber.set(identityKey(updated.flexNumber), updated);
          registerLocation(updated);
          continue;
        }

        const [created] = await tx.insert(smMarkets).values({
          ...importableValues,
          isActive: isActive ?? true,
          importSourceFileName: payload.fileName,
          importedAt: new Date(),
          isDeleted: false,
        } as typeof smMarkets.$inferInsert).returning();
        if (!created) throw new Error("SM_MARKET_CREATE_FAILED");
        touchedIds.add(created.id);
        if (created.internalMarketId) byInternalId.set(identityKey(created.internalMarketId), created);
        if (created.flexNumber) byFlexNumber.set(identityKey(created.flexNumber), created);
        registerLocation(created);
        summary.created += 1;
      }
    });

    const fresh = await loadSmMarkets();
    logAction("info", "sm_market_import_completed", {
      req,
      action: "sm_market_import",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { fileName: payload.fileName, created: summary.created, updated: summary.updated, skipped: summary.skipped, unchanged: summary.unchanged },
    });
    res.status(200).json({ markets: fresh.map(mapSmMarketRow), summary });
  } catch (error) {
    if (error instanceof Error && error.message === "SM_IMPORT_IN_PROGRESS") {
      res.status(409).json({ error: "Ein anderer SM-Marktimport läuft bereits. Bitte in wenigen Sekunden erneut versuchen." });
      return;
    }
    if (isUniqueViolation(error)) {
      const constraint = postgresConstraint(error);
      const label = constraint === "sm_markets_flex_number_active_unique" ? "Flexnummer" : "Stammnummern";
      res.status(409).json({ error: `${label} ist bereits einem anderen aktiven SM-Markt zugeordnet. Es wurde nichts aus dieser Datei gespeichert.` });
      return;
    }
    next(error);
  }
});

adminSmMarketsRouter.post("/", async (req, res, next) => {
  try {
    const parsed = createSmMarketSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Bitte alle Pflichtfelder des SM-Markts korrekt ausfüllen." });
      return;
    }
    const input = parsed.data;
    if (input.assignedSmUserId && !(await isAssignableSmUser(input.assignedSmUserId))) {
      res.status(400).json({ error: "Der ausgewählte Shelf Merchandiser ist nicht aktiv oder kein SM-Account." });
      return;
    }
    const [created] = await db.insert(smMarkets).values({
      internalMarketId: input.internalMarketId,
      flexNumber: normalizeIdentity(input.flexNumber),
      name: input.name,
      dbName: input.dbName ?? input.chain,
      chain: input.chain,
      address: input.address,
      postalCode: input.postalCode,
      city: input.city,
      region: input.region,
      adminInfoNote: input.adminInfoNote ?? "",
      assignedSmUserId: input.assignedSmUserId ?? null,
      isActive: input.isActive ?? true,
      isDeleted: false,
    }).returning();
    if (!created) throw new Error("SM_MARKET_CREATE_FAILED");
    res.status(201).json({ market: mapSmMarketRow(created) });
  } catch (error) {
    if (isUniqueViolation(error)) {
      res.status(409).json({ error: "Stammnummern oder Flexnummer ist bereits einem anderen aktiven SM-Markt zugeordnet." });
      return;
    }
    next(error);
  }
});

adminSmMarketsRouter.get("/:id/deactivation-preview", async (req, res, next) => {
  try {
    const id = z.string().uuid().parse(req.params.id);
    const preview = await db.transaction((tx) => loadSmMarketDeactivationPreview(tx, id), { isolationLevel: "repeatable read", accessMode: "read only" });
    res.set("Cache-Control", "private, no-store").json(preview);
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Ungültiger SM-Markt." });
    if (error instanceof SmMarketDeactivationError) return res.status(error.statusCode).json({ error: error.message, code: error.code });
    next(error);
  }
});

adminSmMarketsRouter.post("/:id/deactivate", async (req: AuthedRequest, res, next) => {
  try {
    const id = z.string().uuid().parse(req.params.id);
    const input = smMarketDeactivationSchema.parse(req.body);
    const result = await db.transaction((tx) => deactivateSmMarket(tx, id, req.authUser!.appUserId, input));
    res.json({ ...result, market: mapSmMarketRow(result.market) });
  } catch (error) {
    if (error instanceof z.ZodError) return res.status(400).json({ error: "Bitte entscheide vollständig über die betroffenen Einsätze.", code: "sm_market_deactivation_invalid" });
    if (error instanceof SmMarketDeactivationError) return res.status(error.statusCode).json({ error: error.message, code: error.code });
    next(error);
  }
});

adminSmMarketsRouter.patch("/:id", async (req, res, next) => {
  try {
    const id = z.string().uuid().safeParse(req.params.id);
    const parsed = updateSmMarketSchema.safeParse(req.body);
    if (!id.success || !parsed.success || Object.keys(parsed.data).length === 0) {
      res.status(400).json({ error: "Ungültige SM-Marktänderung." });
      return;
    }
    const input = parsed.data;
    if (input.assignedSmUserId && !(await isAssignableSmUser(input.assignedSmUserId))) {
      res.status(400).json({ error: "Der ausgewählte Shelf Merchandiser ist nicht aktiv oder kein SM-Account." });
      return;
    }
    const updated = await db.transaction(async (tx) => {
      await lockSmPlanning(tx);
      const [current] = await tx.select().from(smMarkets).where(and(eq(smMarkets.id, id.data), eq(smMarkets.isDeleted, false))).limit(1).for("update");
      if (current?.isActive && input.isActive === false) throw new SmMarketDeactivationError(409, "sm_market_deactivation_required", "Bitte öffne die Deaktivierungsvorschau und entscheide zuerst über betroffene Einsätze.");
      const [saved] = await tx.update(smMarkets).set({
      ...(input.internalMarketId === undefined ? {} : { internalMarketId: input.internalMarketId }),
      ...(input.flexNumber === undefined ? {} : { flexNumber: normalizeIdentity(input.flexNumber) }),
      ...(input.name === undefined ? {} : { name: input.name }),
      ...(input.dbName === undefined ? {} : { dbName: input.dbName }),
      ...(input.chain === undefined ? {} : { chain: input.chain }),
      ...(input.address === undefined ? {} : { address: input.address }),
      ...(input.postalCode === undefined ? {} : { postalCode: input.postalCode }),
      ...(input.city === undefined ? {} : { city: input.city }),
      ...(input.region === undefined ? {} : { region: input.region }),
      ...(input.adminInfoNote === undefined ? {} : { adminInfoNote: input.adminInfoNote }),
      ...(input.assignedSmUserId === undefined ? {} : { assignedSmUserId: input.assignedSmUserId }),
      ...(input.isActive === undefined ? {} : { isActive: input.isActive }),
      updatedAt: new Date(),
      }).where(and(eq(smMarkets.id, id.data), eq(smMarkets.isDeleted, false))).returning();
      return saved;
    });
    if (!updated) {
      res.status(404).json({ error: "SM-Markt nicht gefunden." });
      return;
    }
    res.status(200).json({ market: mapSmMarketRow(updated) });
  } catch (error) {
    if (error instanceof SmMarketDeactivationError) return res.status(error.statusCode).json({ error: error.message, code: error.code });
    if (isUniqueViolation(error)) {
      res.status(409).json({ error: "Stammnummern oder Flexnummer ist bereits einem anderen aktiven SM-Markt zugeordnet." });
      return;
    }
    next(error);
  }
});

adminSmMarketsRouter.patch("/:id/delete", async (req, res, next) => {
  try {
    const id = z.string().uuid().safeParse(req.params.id);
    if (!id.success) {
      res.status(400).json({ error: "Ungültiger SM-Markt." });
      return;
    }
    const now = new Date();
    const deleted = await db.transaction(async (tx) => {
      await lockSmPlanning(tx);
      const preview = await loadSmMarketDeactivationPreview(tx, id.data);
      if (preview.affectedCount || preview.endingSeriesCount || preview.protectedAssignments.some((row) => row.status === "in_progress")) throw new SmMarketDeactivationError(409, "sm_market_deactivation_required", "Dieser Markt hat noch Einsätze. Bitte zuerst deaktivieren und über Absage oder Ersatz entscheiden. Laufende Besuche müssen abgeschlossen werden.");
      const [removed] = await tx.update(smMarkets).set({
      isDeleted: true,
      deletedAt: now,
      updatedAt: now,
      }).where(and(eq(smMarkets.id, id.data), eq(smMarkets.isDeleted, false))).returning({ id: smMarkets.id });
      return removed;
    });
    if (!deleted) {
      res.status(404).json({ error: "SM-Markt nicht gefunden." });
      return;
    }
    res.status(200).json({ ok: true, marketId: deleted.id });
  } catch (error) {
    if (error instanceof SmMarketDeactivationError) return res.status(error.statusCode).json({ error: error.message, code: error.code });
    next(error);
  }
});
