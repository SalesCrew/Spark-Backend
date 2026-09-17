import { createHash } from "node:crypto";
import { and, asc, desc, eq, gt, gte, ilike, inArray, isNotNull, isNull, lt, ne, or, sql } from "drizzle-orm";
import { Router } from "express";
import { kuehlerProgressKey } from "../lib/kuehler-assignment-progress.js";
import { loadKuehlerAssignmentProgress } from "../lib/kuehler-assignment-progress-query.js";
import { z } from "zod";
import { fetchFragebogenUi, fetchModulesUi } from "./fragebogen.js";
import { requireKundeAdminPermission } from "../lib/kunde-access.js";
import { aggregateHighVolumeLoad, logAction, logger, markErrorAsLogged, startActionTimer } from "../lib/logger.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { db } from "../lib/db.js";
import { toYmdInTimezone } from "../lib/day-session.js";
import { selectEffectiveFlexCampaigns } from "../lib/flex-campaign-selection.js";
import { normalizeCrossDomainMarketIdentity } from "../market-identity.shared.js";
import { resolveCurrentRedPeriod } from "../lib/red-month-periods.js";
import { resolveBillaGmFilterEnrollment } from "../billa-gm-market-filter.shared.js";
import { planGmMarketSnapshot, planKuehlerMarketSnapshot } from "../gm-market-snapshot.shared.js";
import { classifyUniversumImportRow } from "../universum-import.shared.js";
import {
  campaignMarketAssignments,
  campaigns,
  marketKuehlerUnits,
  markets,
  photoTags,
  smAssignments,
  smMarkets,
  specialArthurFilter,
  visitAnswerPhotos,
  visitAnswers,
  visitQuestionComments,
  visitSessionQuestions,
  visitSessionSections,
  visitSessions,
  users,
} from "../lib/schema.js";

type ImportFieldKey =
  | "standardMarketNumber"
  | "cokeMasterNumber"
  | "flexNumber"
  | "kuehlerStammnr"
  | "kuehlerBd"
  | "kuehlerAnzahlKsAmStandort"
  | "kuehlerInternalId"
  | "kuehlerSerialNumber"
  | "kuehlerTechnicalIdentNo"
  | "kuehlerModel"
  | "kuehlerMatchValue"
  | "name"
  | "address"
  | "postalCode"
  | "city"
  | "dbName"
  | "emEh"
  | "region"
  | "employee"
  | "currentGmName"
  | "universeMarket"
  | "visitFrequencyPerYear"
  | "infoFlag"
  | "infoNote"
  | "isActive";

type MarketDraft = Partial<
  Record<
    ImportFieldKey,
    string | number | boolean
  >
>;
type ImportDatasetType = "universum" | "kuehler" | "kuehler_update" | "kuehler_snapshot" | "update";
type KuehlerUpdateIdentifier =
  | "kuehlerInternalId"
  | "kuehlerSerialNumber"
  | "kuehlerTechnicalIdentNo";

const SECTION_ORDER = ["standard", "flex", "billa", "kuehler", "mhd", "durcharbeit"] as const;

function normalizeUnique(values: string[]): string[] {
  return Array.from(new Set(values.map((v) => v.trim()).filter((v) => v.length > 0)));
}

function normalizeMarketChain(value: string | null | undefined): string {
  return String(value ?? "").trim().toLowerCase();
}

function normalizeQuestionChains(chains: string[] | null | undefined): string[] {
  if (!Array.isArray(chains)) return [];
  return normalizeUnique(
    chains
      .filter((entry): entry is string => typeof entry === "string")
      .map((entry) => normalizeMarketChain(entry))
      .filter((entry) => entry.length > 0),
  );
}

function deriveChainLabel(input: { name: string; dbName: string }): string {
  const dbName = input.dbName?.trim();
  if (dbName) return dbName.toUpperCase();

  const source = `${input.name} ${input.dbName}`.toUpperCase();
  if (source.includes("BILLA+")) return "BILLA+";
  if (source.includes("BILLA")) return "BILLA";
  if (source.includes("SPAR")) return "SPAR";
  if (source.includes("ADEG")) return "ADEG";
  if (source.includes("PENNY")) return "PENNY";
  if (source.includes("HOFER")) return "HOFER";
  if (source.includes("MERKUR")) return "MERKUR";
  return input.name.split(" ")[0]?.toUpperCase() || "MARKT";
}

function isBillaMarketRow(input: { name: string | null | undefined; dbName: string | null | undefined }): boolean {
  return `${input.name ?? ""} ${input.dbName ?? ""}`.toUpperCase().includes("BILLA");
}

function billaMarketSqlCondition() {
  return or(ilike(markets.name, "%billa%"), ilike(markets.dbName, "%billa%"));
}

function normalizeSpecialArthurMatchValue(input: unknown): string | null {
  const normalized = String(input ?? "")
    .trim()
    .replace(/\s+/g, "")
    .toUpperCase();
  return normalized.length > 0 ? normalized : null;
}

function normalizedMarketIdentifierSql(column: unknown) {
  return sql<string>`upper(regexp_replace(coalesce(${column}, ''), '\s+', '', 'g'))`;
}

function specialArthurMarketSqlCondition(matchValues: string[]) {
  if (matchValues.length === 0) return undefined;
  return or(
    inArray(normalizedMarketIdentifierSql(markets.kuehlerStammnr), matchValues),
    inArray(normalizedMarketIdentifierSql(markets.cokeMasterNumber), matchValues),
    inArray(normalizedMarketIdentifierSql(markets.flexNumber), matchValues),
  );
}

function billaGmMarketSqlCondition(matchValues: string[]) {
  return specialArthurMarketSqlCondition(matchValues) ?? billaMarketSqlCondition();
}

async function loadSpecialArthurFilterValues(gmUserId: string): Promise<string[]> {
  const rows = await db
    .select({ matchValue: specialArthurFilter.matchValue })
    .from(specialArthurFilter)
    .where(and(eq(specialArthurFilter.gmUserId, gmUserId), eq(specialArthurFilter.isDeleted, false)));
  return rows.map((row) => row.matchValue).filter((value) => value.length > 0);
}

type MarketWriteTransaction = Parameters<Parameters<typeof db.transaction>[0]>[0];

type BillaGmFilterEnrollmentResult = {
  gmUserId: string;
  matchValue: string;
  inserted: boolean;
} | null;

async function ensureAssignedBillaGmFilterEntry(
  tx: MarketWriteTransaction,
  market: typeof markets.$inferSelect,
  createdByUserId: string | null,
): Promise<BillaGmFilterEnrollmentResult> {
  const candidateRows = await tx
    .select({
      id: users.id,
      firstName: users.firstName,
      lastName: users.lastName,
    })
    .from(users)
    .innerJoin(
      specialArthurFilter,
      and(
        eq(specialArthurFilter.gmUserId, users.id),
        eq(specialArthurFilter.isDeleted, false),
      ),
    )
    .where(
      and(
        eq(users.role, "gm"),
        eq(users.isBillaGm, true),
        eq(users.isActive, true),
        isNull(users.deletedAt),
        isNull(users.anonymizedAt),
      ),
    );

  const enrollment = resolveBillaGmFilterEnrollment(market, candidateRows);
  if (!enrollment) return null;

  const insertedRows = await tx
    .insert(specialArthurFilter)
    .values({
      gmUserId: enrollment.gmUserId,
      matchValue: enrollment.matchValue,
      createdByUserId,
    })
    .onConflictDoNothing()
    .returning({ id: specialArthurFilter.id });

  return {
    ...enrollment,
    inserted: insertedRows.length > 0,
  };
}

function marketMatchesSpecialArthurFilter(
  market: {
    kuehlerStammnr: string | null;
    cokeMasterNumber: string | null;
    flexNumber: string | null;
  },
  matchValues: string[],
): boolean {
  if (matchValues.length === 0) return true;
  const allowed = new Set(matchValues);
  return [market.kuehlerStammnr, market.cokeMasterNumber, market.flexNumber].some((value) => {
    const normalized = normalizeSpecialArthurMatchValue(value);
    return normalized ? allowed.has(normalized) : false;
  });
}

function isMarketVisibleForBillaGm(
  market: {
    name: string | null;
    dbName: string | null;
    kuehlerStammnr: string | null;
    cokeMasterNumber: string | null;
    flexNumber: string | null;
  },
  matchValues: string[],
): boolean {
  return matchValues.length > 0 ? marketMatchesSpecialArthurFilter(market, matchValues) : isBillaMarketRow(market);
}

function isQuestionApplicableToMarketChain(questionChains: string[] | null | undefined, marketChain: string): boolean {
  const normalizedChains = normalizeQuestionChains(questionChains);
  if (normalizedChains.length === 0) return true;
  if (!marketChain) return false;
  return normalizedChains.includes(marketChain);
}

function extractConfiguredPhotoTagIds(config: Record<string, unknown>): string[] {
  if (!Array.isArray(config.tagIds)) return [];
  return normalizeUnique(config.tagIds.filter((id): id is string => typeof id === "string"));
}

function enrichQuestionConfigWithTagMeta(
  questionType: string,
  baseConfig: Record<string, unknown>,
  configuredTagById: Map<string, { id: string; label: string; deletedAt: string | null }>,
): Record<string, unknown> {
  if (questionType !== "photo") return baseConfig;
  const photoTagIds = extractConfiguredPhotoTagIds(baseConfig);
  if (photoTagIds.length === 0) return baseConfig;
  return {
    ...baseConfig,
    tagIds: photoTagIds,
    tagMeta: photoTagIds.map((tagId) => configuredTagById.get(tagId) ?? { id: tagId, label: tagId, deletedAt: null }),
  };
}

function toYmd(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

async function getCurrentRedPeriodBounds(now = new Date()): Promise<{ startYmd: string; endYmd: string }> {
  const period = await resolveCurrentRedPeriod(now);
  return {
    startYmd: toYmd(period.start),
    endYmd: toYmd(period.end),
  };
}

const NEXT_SM_ASSIGNMENT_STATUSES: Array<(typeof smAssignments.$inferSelect)["status"]> = [
  "planned",
  "confirmed",
  "open",
  "in_progress",
];

async function resolveNextSmVisitDatesByGmMarketId(
  marketRows: Array<typeof markets.$inferSelect>,
  now = new Date(),
): Promise<Map<string, string>> {
  const identityByGmMarketId = new Map<string, string>();
  const wantedIdentities = new Set<string>();
  for (const market of marketRows) {
    const identity = normalizeCrossDomainMarketIdentity(market.cokeMasterNumber)
      ?? normalizeCrossDomainMarketIdentity(market.kuehlerStammnr);
    if (!identity) continue;
    identityByGmMarketId.set(market.id, identity);
    wantedIdentities.add(identity);
  }
  if (wantedIdentities.size === 0) return new Map();

  const activeSmMarkets = await db
    .select({ id: smMarkets.id, internalMarketId: smMarkets.internalMarketId })
    .from(smMarkets)
    .where(and(eq(smMarkets.isDeleted, false), eq(smMarkets.isActive, true), isNotNull(smMarkets.internalMarketId)));

  const identityBySmMarketId = new Map<string, string>();
  for (const market of activeSmMarkets) {
    const identity = normalizeCrossDomainMarketIdentity(market.internalMarketId);
    if (!identity || !wantedIdentities.has(identity)) continue;
    identityBySmMarketId.set(market.id, identity);
  }
  const matchedSmMarketIds = Array.from(identityBySmMarketId.keys());
  if (matchedSmMarketIds.length === 0) return new Map();

  const effectiveMarketId = sql<string>`coalesce(${smAssignments.replacementSmMarketId}, ${smAssignments.originalSmMarketId})`;
  const effectiveWorkDate = sql<string>`coalesce(${smAssignments.replacementWorkDate}, ${smAssignments.originalWorkDate})`;
  const upcomingAssignments = await db
    .select({ smMarketId: effectiveMarketId, workDate: effectiveWorkDate })
    .from(smAssignments)
    .where(and(
      eq(smAssignments.isDeleted, false),
      inArray(smAssignments.status, NEXT_SM_ASSIGNMENT_STATUSES),
      gte(effectiveWorkDate, toYmdInTimezone(now, "Europe/Vienna")),
      inArray(effectiveMarketId, matchedSmMarketIds),
    ))
    .orderBy(asc(effectiveWorkDate), asc(smAssignments.createdAt));

  const nextDateByIdentity = new Map<string, string>();
  for (const assignment of upcomingAssignments) {
    const identity = identityBySmMarketId.get(assignment.smMarketId);
    if (!identity || nextDateByIdentity.has(identity)) continue;
    nextDateByIdentity.set(identity, assignment.workDate);
  }

  const nextDateByGmMarketId = new Map<string, string>();
  for (const [marketId, identity] of identityByGmMarketId) {
    const nextDate = nextDateByIdentity.get(identity);
    if (nextDate) nextDateByGmMarketId.set(marketId, nextDate);
  }
  return nextDateByGmMarketId;
}

const universumImportFieldSpecs: Array<{ key: ImportFieldKey; label: string; required: boolean; isIdentity: boolean }> = [
  { key: "standardMarketNumber", label: "Standardmarkt Nr", required: false, isIdentity: false },
  { key: "cokeMasterNumber", label: "Stammnr. von Coke", required: false, isIdentity: false },
  { key: "flexNumber", label: "Flex-Nummer", required: true, isIdentity: true },
  { key: "name", label: "Name", required: false, isIdentity: false },
  { key: "address", label: "Adresse", required: false, isIdentity: false },
  { key: "postalCode", label: "Postleitzahl", required: false, isIdentity: false },
  { key: "city", label: "Ort", required: false, isIdentity: false },
  { key: "dbName", label: "Name f. DB", required: false, isIdentity: false },
  { key: "emEh", label: "EM/EH", required: false, isIdentity: false },
  { key: "region", label: "Region", required: false, isIdentity: false },
  { key: "employee", label: "Mitarbeiter", required: false, isIdentity: false },
  { key: "universeMarket", label: "Universums-Markt", required: false, isIdentity: false },
  { key: "visitFrequencyPerYear", label: "Besuchsrhythmus", required: false, isIdentity: false },
  { key: "infoFlag", label: "Info", required: false, isIdentity: false },
];

const kuehlerImportFieldSpecs: Array<{ key: ImportFieldKey; label: string; required: boolean; isIdentity: boolean }> = [
  { key: "kuehlerStammnr", label: "Stammnr", required: false, isIdentity: true },
  { key: "flexNumber", label: "Flex-Nummer", required: false, isIdentity: true },
  { key: "kuehlerInternalId", label: "internal_id", required: false, isIdentity: false },
  { key: "kuehlerBd", label: "BD", required: false, isIdentity: false },
  { key: "kuehlerAnzahlKsAmStandort", label: "Anzahl KS am Standort", required: false, isIdentity: false },
  { key: "kuehlerSerialNumber", label: "Serial Number", required: false, isIdentity: false },
  { key: "kuehlerTechnicalIdentNo", label: "Tech. Ident. No.", required: false, isIdentity: false },
  { key: "name", label: "Name", required: true, isIdentity: false },
  { key: "address", label: "Street name", required: true, isIdentity: false },
  { key: "postalCode", label: "PLZ", required: true, isIdentity: false },
  { key: "city", label: "Ort", required: true, isIdentity: false },
  { key: "region", label: "Region", required: true, isIdentity: false },
  { key: "kuehlerModel", label: "Model", required: false, isIdentity: false },
  { key: "employee", label: "Mitarbeiter", required: false, isIdentity: false },
];

const kuehlerUpdateImportFieldSpecs: Array<{ key: ImportFieldKey; label: string; required: boolean; isIdentity: boolean }> = [
  { key: "kuehlerMatchValue", label: "Identifikationswert", required: true, isIdentity: true },
  { key: "kuehlerInternalId", label: "internal_id", required: false, isIdentity: false },
  { key: "kuehlerTechnicalIdentNo", label: "Tech. Ident. No.", required: false, isIdentity: false },
  { key: "kuehlerSerialNumber", label: "Serial Number", required: false, isIdentity: false },
  { key: "kuehlerBd", label: "BD", required: false, isIdentity: false },
  { key: "kuehlerAnzahlKsAmStandort", label: "Anzahl KS am Standort", required: false, isIdentity: false },
  { key: "kuehlerModel", label: "Model", required: false, isIdentity: false },
  { key: "name", label: "Name", required: false, isIdentity: false },
  { key: "employee", label: "Mitarbeiter", required: false, isIdentity: false },
];

const updateImportFieldSpecs: Array<{ key: ImportFieldKey; label: string; required: boolean; isIdentity: boolean }> = [
  { key: "flexNumber", label: "Flex-Nummer", required: true, isIdentity: true },
  { key: "standardMarketNumber", label: "Standardmarkt Nr", required: false, isIdentity: false },
  { key: "cokeMasterNumber", label: "Stammnr. von Coke", required: false, isIdentity: false },
  { key: "name", label: "Name", required: false, isIdentity: false },
  { key: "dbName", label: "Name f. DB", required: false, isIdentity: false },
  { key: "address", label: "Adresse", required: false, isIdentity: false },
  { key: "postalCode", label: "Postleitzahl", required: false, isIdentity: false },
  { key: "city", label: "Ort", required: false, isIdentity: false },
  { key: "region", label: "Region", required: false, isIdentity: false },
  { key: "emEh", label: "EM/EH", required: false, isIdentity: false },
  { key: "employee", label: "Mitarbeiter", required: false, isIdentity: false },
  { key: "currentGmName", label: "GM", required: false, isIdentity: false },
  { key: "universeMarket", label: "Universums-Markt", required: false, isIdentity: false },
  { key: "visitFrequencyPerYear", label: "Besuchsrhythmus", required: false, isIdentity: false },
  { key: "infoFlag", label: "Info", required: false, isIdentity: false },
  { key: "infoNote", label: "Info-Notiz", required: false, isIdentity: false },
  { key: "isActive", label: "Status aktiv", required: false, isIdentity: false },
];

function getImportFieldSpecs(importType: ImportDatasetType) {
  if (importType === "kuehler" || importType === "kuehler_snapshot") return kuehlerImportFieldSpecs;
  if (importType === "kuehler_update") return kuehlerUpdateImportFieldSpecs;
  if (importType === "update") return updateImportFieldSpecs;
  return universumImportFieldSpecs;
}

const mappingSchema = z
  .object({
    standardMarketNumber: z.string().optional(),
    cokeMasterNumber: z.string().optional(),
    flexNumber: z.string().optional(),
    kuehlerStammnr: z.string().optional(),
    kuehlerBd: z.string().optional(),
    kuehlerAnzahlKsAmStandort: z.string().optional(),
    kuehlerInternalId: z.string().optional(),
    kuehlerSerialNumber: z.string().optional(),
    kuehlerTechnicalIdentNo: z.string().optional(),
    kuehlerModel: z.string().optional(),
    kuehlerMatchValue: z.string().optional(),
    name: z.string().optional(),
    address: z.string().optional(),
    postalCode: z.string().optional(),
    city: z.string().optional(),
    dbName: z.string().optional(),
    emEh: z.string().optional(),
    region: z.string().optional(),
    employee: z.string().optional(),
    currentGmName: z.string().optional(),
    universeMarket: z.string().optional(),
    visitFrequencyPerYear: z.string().optional(),
    infoFlag: z.string().optional(),
    infoNote: z.string().optional(),
    isActive: z.string().optional(),
  })
  .strict();

const importMarketsSchema = z
  .object({
    importType: z.enum(["universum", "kuehler", "kuehler_update", "kuehler_snapshot", "update"]).optional().default("universum"),
    kuehlerUpdateIdentifier: z
      .enum(["kuehlerInternalId", "kuehlerSerialNumber", "kuehlerTechnicalIdentNo"])
      .optional(),
    allowMissingCokeMasterNumber: z.boolean().optional().default(false),
    fileName: z.string().min(1),
    sheetName: z.string().min(1),
    rows: z.array(z.array(z.union([z.string(), z.number(), z.boolean(), z.null()]))).min(1),
    mapping: mappingSchema,
    snapshotToken: z.string().length(64).optional(),
    confirmSnapshot: z.boolean().optional(),
  })
  .strict();

const marketTypeSchema = z.enum(["universum", "kuehler", "both"]);
type MarketType = z.infer<typeof marketTypeSchema>;

const updateMarketSchema = z
  .object({
    standardMarketNumber: z.string().optional(),
    cokeMasterNumber: z.string().optional(),
    flexNumber: z.string().optional(),
    name: z.string().min(1).optional(),
    dbName: z.string().optional(),
    address: z.string().min(1).optional(),
    postalCode: z.string().min(1).optional(),
    city: z.string().min(1).optional(),
    region: z.string().min(1).optional(),
    emEh: z.string().optional(),
    employee: z.string().optional(),
    currentGmName: z.string().optional(),
    visitFrequencyPerYear: z.number().int().min(0).optional(),
    infoFlag: z.boolean().optional(),
    infoNote: z.string().optional(),
    universeMarket: z.boolean().optional(),
    marketType: marketTypeSchema.optional(),
    kuehlerStammnr: z.string().optional(),
    isActive: z.boolean().optional(),
    importSourceFileName: z.string().optional(),
    importedAt: z.string().datetime().optional(),
    plannedToId: z.string().uuid().nullable().optional(),
  })
  .strict();

const updateUniverseMarketSchema = z
  .object({
    universeMarket: z.boolean(),
  })
  .strict();

const createMarketSchema = z
  .object({
    standardMarketNumber: z.string().optional(),
    cokeMasterNumber: z.string().optional(),
    flexNumber: z.string().optional(),
    name: z.string().min(1),
    dbName: z.string().optional().default(""),
    address: z.string().min(1),
    postalCode: z.string().min(1),
    city: z.string().min(1),
    region: z.string().min(1),
    emEh: z.string().optional().default(""),
    employee: z.string().optional().default(""),
    currentGmName: z.string().optional().default(""),
    visitFrequencyPerYear: z.number().int().min(0).optional().default(0),
    infoFlag: z.boolean().optional().default(false),
    infoNote: z.string().optional().default(""),
    universeMarket: z.boolean().optional().default(true),
    marketType: marketTypeSchema.optional().default("universum"),
    kuehlerStammnr: z.string().optional().default(""),
    isActive: z.boolean().optional().default(true),
    importSourceFileName: z.string().optional().default(""),
    importedAt: z.string().datetime().optional(),
    plannedToId: z.string().uuid().nullable().optional(),
  })
  .strict();

const createKuehlerUnitSchema = z
  .object({
    name: z.string().optional().default(""),
    employee: z.string().optional().default(""),
    kuehlerInternalId: z.string().optional(),
    kuehlerBd: z.string().optional().default(""),
    kuehlerAnzahlKsAmStandort: z.number().int().min(0).nullable().optional(),
    kuehlerSerialNumber: z.string().optional().default(""),
    kuehlerTechnicalIdentNo: z.string().optional().default(""),
    kuehlerModel: z.string().optional().default(""),
    importSourceFileName: z.string().optional().default(""),
    importedAt: z.string().datetime().optional(),
  })
  .strict();

const updateKuehlerUnitSchema = z
  .object({
    name: z.string().optional(),
    employee: z.string().optional(),
    kuehlerInternalId: z.string().optional(),
    kuehlerBd: z.string().optional(),
    kuehlerAnzahlKsAmStandort: z.number().int().min(0).nullable().optional(),
    kuehlerSerialNumber: z.string().optional(),
    kuehlerTechnicalIdentNo: z.string().optional(),
    kuehlerModel: z.string().optional(),
    importSourceFileName: z.string().optional(),
    importedAt: z.string().datetime().optional(),
    isDeleted: z.boolean().optional(),
  })
  .strict();

const normalizeRegionsSchema = z
  .object({
    batchSize: z.coerce.number().int().min(100).max(2000).optional().default(500),
    reportLimit: z.coerce.number().int().min(1).max(500).optional().default(200),
  })
  .strict();

const listQuerySchema = z
  .object({
    q: z.string().optional(),
  })
  .strict();

function parseImportBoolean(value: string): boolean | null {
  const token = value
    .trim()
    .toLowerCase()
    .replace(/[\u00a0\s]+/g, "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "");
  if (["ja", "j", "true", "wahr", "1", "yes", "y", "x"].includes(token)) return true;
  if (["nein", "n", "false", "falsch", "0", "no"].includes(token)) return false;
  return null;
}

function normBool(v: string): boolean {
  return parseImportBoolean(v) ?? false;
}

function normNum(v: string): number {
  const n = parseInt(v.replace(/[^0-9]/g, ""), 10);
  return Number.isNaN(n) ? 0 : n;
}

function normStr(value: string | undefined): string {
  return (value ?? "").trim().toLowerCase();
}

function normalizeOptionalText(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim();
  return normalized.length > 0 ? normalized : null;
}

function hasImportValue(value: unknown): boolean {
  return value != null && (typeof value !== "string" || value.trim().length > 0);
}

function deriveUniverseMarketFromType(type: MarketType): boolean {
  return type !== "kuehler";
}

function marketTypePriority(type: MarketType): number {
  if (type === "both") return 3;
  if (type === "universum") return 2;
  return 1;
}

function choosePreferredMarketByStammnr(
  current: typeof markets.$inferSelect | undefined,
  candidate: typeof markets.$inferSelect,
): typeof markets.$inferSelect {
  if (!current) return candidate;
  const currentPriority = marketTypePriority(current.marketType);
  const candidatePriority = marketTypePriority(candidate.marketType);
  if (candidatePriority !== currentPriority) {
    return candidatePriority > currentPriority ? candidate : current;
  }
  return candidate.createdAt < current.createdAt ? candidate : current;
}

export function resolveKuehlerIdentity(input: {
  marketType: MarketType;
  incomingKuehlerStammnr?: unknown;
  incomingCokeMasterNumber?: unknown;
  existingKuehlerStammnr?: unknown;
  existingCokeMasterNumber?: unknown;
}): { ok: true; stammnr: string | null } | { ok: false; error: string } {
  const incomingStammnr = normalizeStammnrForMatch(input.incomingKuehlerStammnr);
  const incomingCokeStammnr = normalizeStammnrForMatch(input.incomingCokeMasterNumber);
  const existingStammnr = normalizeStammnrForMatch(input.existingKuehlerStammnr);
  const existingCokeStammnr = normalizeStammnrForMatch(input.existingCokeMasterNumber);

  if (input.marketType === "universum") {
    return { ok: true, stammnr: null };
  }

  const existingIdentity = existingStammnr ?? existingCokeStammnr;
  const incomingWasProvided =
    input.incomingKuehlerStammnr !== undefined || input.incomingCokeMasterNumber !== undefined;
  const incomingCandidates = Array.from(
    new Set([incomingStammnr, incomingCokeStammnr].filter((value): value is string => Boolean(value))),
  );
  const changedCandidates = incomingCandidates.filter((value) => value !== existingIdentity);

  if (changedCandidates.length > 1) {
    return {
      ok: false,
      error: "Stammnr. Coke und Kühler-Stammnr dürfen nicht auf unterschiedliche Werte geändert werden.",
    };
  }

  const stammnr =
    changedCandidates[0] ??
    incomingCandidates[0] ??
    (incomingWasProvided ? null : existingIdentity);
  if (!stammnr) {
    return { ok: false, error: "Für Markt-Typ Kühler/Beides ist eine gültige Stammnr erforderlich." };
  }
  return { ok: true, stammnr };
}

const CANONICAL_REGIONS = ["Nord", "West", "Ost", "Süd"] as const;
type CanonicalRegion = (typeof CANONICAL_REGIONS)[number];

const REGION_ALIASES = new Map<string, CanonicalRegion>([
  ["nord", "Nord"],
  ["norden", "Nord"],
  ["north", "Nord"],
  ["west", "West"],
  ["westen", "West"],
  ["ost", "Ost"],
  ["osten", "Ost"],
  ["east", "Ost"],
  ["sud", "Süd"],
  ["sued", "Süd"],
  ["sueden", "Süd"],
  ["south", "Süd"],
]);

type RegionNormalizationResult =
  | { ok: true; canonical: CanonicalRegion; raw: string; token: string }
  | { ok: false; raw: string; token: string };

function normalizeRegionToken(input: string): string {
  return input
    .trim()
    .toLowerCase()
    .replace(/ä/g, "ae")
    .replace(/ö/g, "oe")
    .replace(/ü/g, "ue")
    .replace(/ß/g, "ss")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z]/g, "");
}

function normalizeRegionValue(input: unknown): RegionNormalizationResult {
  const raw = typeof input === "string" ? input.trim() : "";
  const token = normalizeRegionToken(raw);
  if (!token) return { ok: false, raw, token };
  const canonical = REGION_ALIASES.get(token);
  if (!canonical) return { ok: false, raw, token };
  return { ok: true, canonical, raw, token };
}

function normalizeIdentity(value: unknown): string | null {
  return normalizeOptionalText(value);
}

function normalizeStammnrForMatch(value: unknown): string | null {
  const normalized = normalizeOptionalText(value);
  if (!normalized) return null;
  const withoutWhitespace = normalized.replace(/\s+/g, "");
  return withoutWhitespace.length > 0 ? withoutWhitespace : null;
}

function normalizeStammnrKey(value: unknown): string {
  return normStr(normalizeStammnrForMatch(value) ?? "");
}

type MarketIdentityFields = Pick<
  typeof markets.$inferSelect,
  "cokeMasterNumber" | "flexNumber" | "kuehlerStammnr"
>;

type MarketIdentifierPropagationResult = {
  matchedFilterRows: number;
  recreatedFilterRows: number;
  mergedOrRemovedFilterRows: number;
};

export function buildSpecialArthurIdentifierReplacements(
  previous: MarketIdentityFields,
  next: MarketIdentityFields,
): Map<string, string | null> {
  const previousStammnr = normalizeSpecialArthurMatchValue(
    previous.kuehlerStammnr ?? previous.cokeMasterNumber,
  );
  const nextStammnr = normalizeSpecialArthurMatchValue(next.kuehlerStammnr ?? next.cokeMasterNumber);
  const previousCoke = normalizeSpecialArthurMatchValue(previous.cokeMasterNumber);
  const nextCoke = normalizeSpecialArthurMatchValue(next.cokeMasterNumber);
  const previousFlex = normalizeSpecialArthurMatchValue(previous.flexNumber);
  const nextFlex = normalizeSpecialArthurMatchValue(next.flexNumber);
  const preferredNext = nextStammnr ?? nextCoke ?? nextFlex;
  const replacements = new Map<string, string | null>();

  const addReplacement = (previousValue: string | null, nextValue: string | null) => {
    if (!previousValue || previousValue === nextValue) return;
    if (replacements.has(previousValue) && replacements.get(previousValue) !== nextValue) {
      replacements.set(previousValue, preferredNext);
      return;
    }
    replacements.set(previousValue, nextValue);
  };

  addReplacement(previousStammnr, nextStammnr ?? preferredNext);
  addReplacement(previousCoke, nextCoke ?? nextStammnr ?? nextFlex);
  addReplacement(previousFlex, nextFlex ?? nextStammnr ?? nextCoke);
  return replacements;
}

async function propagateSpecialArthurMarketIdentifiers(
  tx: Parameters<Parameters<typeof db.transaction>[0]>[0],
  previous: MarketIdentityFields,
  next: MarketIdentityFields,
  now: Date,
): Promise<MarketIdentifierPropagationResult> {
  const replacements = buildSpecialArthurIdentifierReplacements(previous, next);
  const previousValues = Array.from(replacements.keys());
  if (previousValues.length === 0) {
    return { matchedFilterRows: 0, recreatedFilterRows: 0, mergedOrRemovedFilterRows: 0 };
  }

  const affectedRows = await tx
    .select({
      id: specialArthurFilter.id,
      gmUserId: specialArthurFilter.gmUserId,
      matchValue: specialArthurFilter.matchValue,
      createdByUserId: specialArthurFilter.createdByUserId,
    })
    .from(specialArthurFilter)
    .where(
      and(
        eq(specialArthurFilter.isDeleted, false),
        inArray(specialArthurFilter.matchValue, previousValues),
      ),
    );

  if (affectedRows.length === 0) {
    return { matchedFilterRows: 0, recreatedFilterRows: 0, mergedOrRemovedFilterRows: 0 };
  }

  await tx
    .update(specialArthurFilter)
    .set({ isDeleted: true, deletedAt: now, updatedAt: now })
    .where(inArray(specialArthurFilter.id, affectedRows.map((row) => row.id)));

  const desiredRows = new Map<
    string,
    { gmUserId: string; matchValue: string; createdByUserId: string | null }
  >();
  for (const row of affectedRows) {
    const replacement = replacements.get(row.matchValue) ?? null;
    if (!replacement) continue;
    desiredRows.set(`${row.gmUserId}::${replacement}`, {
      gmUserId: row.gmUserId,
      matchValue: replacement,
      createdByUserId: row.createdByUserId,
    });
  }

  const values = Array.from(desiredRows.values());
  const recreatedRows = values.length > 0
    ? await tx
        .insert(specialArthurFilter)
        .values(values)
        .onConflictDoNothing()
        .returning({ id: specialArthurFilter.id })
    : [];

  return {
    matchedFilterRows: affectedRows.length,
    recreatedFilterRows: recreatedRows.length,
    mergedOrRemovedFilterRows: affectedRows.length - recreatedRows.length,
  };
}

type PgUniqueViolation = {
  code: "23505";
  constraint?: string;
  constraint_name?: string;
  detail?: string;
};

function getPgConstraintName(violation: PgUniqueViolation | null): string | null {
  return violation?.constraint ?? violation?.constraint_name ?? null;
}

function getPgUniqueViolation(err: unknown): PgUniqueViolation | null {
  let current = err;
  const seen = new Set<unknown>();
  for (let depth = 0; depth < 6 && current && typeof current === "object"; depth += 1) {
    if (seen.has(current)) break;
    seen.add(current);
    if ("code" in current && (current as { code?: unknown }).code === "23505") {
      return current as PgUniqueViolation;
    }
    current = "cause" in current ? (current as { cause?: unknown }).cause : null;
  }
  return null;
}

function isIdentityConstraintViolation(err: unknown): boolean {
  const violation = getPgUniqueViolation(err);
  if (!violation) return false;
  const constraint = getPgConstraintName(violation);
  return (
    constraint === "markets_standard_market_number_unique" ||
    constraint === "markets_coke_master_number_unique" ||
    constraint === "markets_flex_number_unique"
  );
}

type NormalizedMarketIdentityInput = {
  standardMarketNumber: string | null;
  stammnr: string | null;
  flexNumber: string | null;
};

type MarketIdentityConflict = {
  marketId: string;
  name: string;
  address: string;
  postalCode: string;
  city: string;
  matches: string[];
};

async function findMarketIdentityConflicts(
  input: NormalizedMarketIdentityInput,
): Promise<MarketIdentityConflict[]> {
  const conditions = [
    ...(input.standardMarketNumber
      ? [eq(markets.standardMarketNumber, input.standardMarketNumber)]
      : []),
    ...(input.stammnr
      ? [eq(markets.cokeMasterNumber, input.stammnr), eq(markets.kuehlerStammnr, input.stammnr)]
      : []),
    ...(input.flexNumber ? [eq(markets.flexNumber, input.flexNumber)] : []),
  ];
  if (conditions.length === 0) return [];

  const rows = await db
    .select({
      id: markets.id,
      name: markets.name,
      address: markets.address,
      postalCode: markets.postalCode,
      city: markets.city,
      standardMarketNumber: markets.standardMarketNumber,
      cokeMasterNumber: markets.cokeMasterNumber,
      kuehlerStammnr: markets.kuehlerStammnr,
      flexNumber: markets.flexNumber,
    })
    .from(markets)
    .where(and(eq(markets.isDeleted, false), or(...conditions)))
    .limit(10);

  return rows.map((row) => {
    const matches: string[] = [];
    if (input.standardMarketNumber && row.standardMarketNumber === input.standardMarketNumber) {
      matches.push(`Standardmarkt Nr. ${input.standardMarketNumber}`);
    }
    if (
      input.stammnr &&
      (row.cokeMasterNumber === input.stammnr || row.kuehlerStammnr === input.stammnr)
    ) {
      matches.push(`Stammnr. ${input.stammnr}`);
    }
    if (input.flexNumber && row.flexNumber === input.flexNumber) {
      matches.push(`Flex-Nr. ${input.flexNumber}`);
    }
    return {
      marketId: row.id,
      name: row.name,
      address: row.address,
      postalCode: row.postalCode,
      city: row.city,
      matches,
    };
  });
}

function formatMarketIdentityConflictError(conflicts: MarketIdentityConflict[]): string {
  const details = conflicts
    .slice(0, 3)
    .map((conflict) => {
      const location = [conflict.address, [conflict.postalCode, conflict.city].filter(Boolean).join(" ")]
        .filter(Boolean)
        .join(", ");
      return `${conflict.matches.join(" / ")} gehört bereits zu „${conflict.name}“${location ? ` (${location})` : ""}`;
    })
    .join("; ");
  return `Markt konnte nicht angelegt werden: ${details}. Bitte den bestehenden Markt bearbeiten statt einen zweiten Datensatz anzulegen.`;
}

function isPgStatementTimeout(err: unknown): err is { code: string } {
  if (!err || typeof err !== "object") return false;
  if (!("cause" in err)) return false;
  const cause = (err as { cause?: unknown }).cause;
  if (!cause || typeof cause !== "object") return false;
  return "code" in cause && (cause as { code?: string }).code === "57014";
}

function isPgLockTimeout(err: unknown): err is { code: string } {
  if (!err || typeof err !== "object") return false;
  if (!("cause" in err)) return false;
  const cause = (err as { cause?: unknown }).cause;
  if (!cause || typeof cause !== "object") return false;
  return "code" in cause && (cause as { code?: string }).code === "55P03";
}

function excelColToIndex(col: string): number {
  const s = col.trim().toUpperCase();
  let idx = 0;
  for (let i = 0; i < s.length; i += 1) {
    idx = idx * 26 + (s.charCodeAt(i) - 64);
  }
  return idx - 1;
}

function isValidColLetter(s: string): boolean {
  return /^[A-Za-z]{1,3}$/.test(s.trim());
}

export function mapRowToDraft(
  row: string[],
  mapping: z.infer<typeof mappingSchema>,
  importType: ImportDatasetType,
): MarketDraft {
  const draft: MarketDraft = {};
  for (const spec of getImportFieldSpecs(importType)) {
    const col = mapping[spec.key];
    if (!col || !isValidColLetter(col)) continue;
    const idx = excelColToIndex(col);
    const raw = row[idx]?.trim() ?? "";
    if (!raw) continue;
    if (spec.key === "universeMarket" || spec.key === "infoFlag" || spec.key === "isActive") {
      const parsed = parseImportBoolean(raw);
      if (parsed == null) continue;
      draft[spec.key] = parsed;
      continue;
    }
    if (spec.key === "visitFrequencyPerYear") {
      draft[spec.key] = normNum(raw);
      continue;
    }
    if (spec.key === "kuehlerAnzahlKsAmStandort") {
      draft[spec.key] = normNum(raw);
      continue;
    }
    draft[spec.key] = raw;
  }
  return draft;
}

function getMappedRawCell(row: string[], mapping: z.infer<typeof mappingSchema>, key: ImportFieldKey): string {
  const col = mapping[key];
  if (!col || !isValidColLetter(col)) return "";
  const idx = excelColToIndex(col);
  return row[idx]?.trim() ?? "";
}

function findInvalidBooleanUpdateValue(
  row: string[],
  mapping: z.infer<typeof mappingSchema>,
  mappedFields: ImportFieldKey[],
): { key: ImportFieldKey; raw: string } | null {
  for (const key of mappedFields) {
    if (key !== "universeMarket" && key !== "infoFlag" && key !== "isActive") continue;
    const raw = getMappedRawCell(row, mapping, key);
    if (!raw) continue;
    if (parseImportBoolean(raw) === null) return { key, raw };
  }
  return null;
}

function buildSkipMeta(
  draft: MarketDraft,
  mapping: z.infer<typeof mappingSchema>,
  importType: ImportDatasetType,
) {
  const missingFields: string[] = [];
  const missingFieldKeys: ImportFieldKey[] = [];
  const fetchedFields: Array<{ label: string; value: string }> = [];

  for (const spec of getImportFieldSpecs(importType)) {
    const col = mapping[spec.key] ?? "";
    const val = draft[spec.key];
    if (!col || !isValidColLetter(col)) {
      if (spec.required || spec.isIdentity) {
        missingFields.push(`${spec.label} (nicht gemappt)`);
        missingFieldKeys.push(spec.key);
      }
    } else if (val == null || String(val).trim() === "") {
      if (spec.required || spec.isIdentity) {
        missingFields.push(`${spec.label} (leer)`);
        missingFieldKeys.push(spec.key);
      }
    } else {
      fetchedFields.push({ label: spec.label, value: String(val) });
    }
  }

  return { missingFields, missingFieldKeys, fetchedFields };
}

const updateImportPatchKeys: ImportFieldKey[] = [
  "standardMarketNumber",
  "cokeMasterNumber",
  "name",
  "dbName",
  "address",
  "postalCode",
  "city",
  "region",
  "emEh",
  "employee",
  "currentGmName",
  "universeMarket",
  "visitFrequencyPerYear",
  "infoFlag",
  "infoNote",
  "isActive",
];

function getMappedUpdateFields(mapping: z.infer<typeof mappingSchema>): ImportFieldKey[] {
  return updateImportPatchKeys.filter((key) => isValidColLetter(mapping[key] ?? ""));
}

export function buildUpdateOnlyMarketPatch(
  draft: MarketDraft,
  mappedFields: ImportFieldKey[],
  normalizedRegion: RegionNormalizationResult | null,
): Partial<typeof markets.$inferInsert> {
  const mapped = new Set(mappedFields);
  const patch: Partial<typeof markets.$inferInsert> = {};

  if (mapped.has("standardMarketNumber") && hasImportValue(draft.standardMarketNumber)) {
    patch.standardMarketNumber = normalizeIdentity(String(draft.standardMarketNumber));
  }
  if (mapped.has("cokeMasterNumber") && hasImportValue(draft.cokeMasterNumber)) {
    patch.cokeMasterNumber = normalizeIdentity(String(draft.cokeMasterNumber));
  }
  if (mapped.has("name") && hasImportValue(draft.name)) patch.name = String(draft.name);
  if (mapped.has("dbName") && hasImportValue(draft.dbName)) {
    patch.dbName = normalizeOptionalText(String(draft.dbName)) ?? "";
  }
  if (mapped.has("address") && hasImportValue(draft.address)) patch.address = String(draft.address);
  if (mapped.has("postalCode") && hasImportValue(draft.postalCode)) patch.postalCode = String(draft.postalCode);
  if (mapped.has("city") && hasImportValue(draft.city)) patch.city = String(draft.city);
  if (mapped.has("region") && hasImportValue(draft.region) && normalizedRegion?.ok) {
    patch.region = normalizedRegion.canonical;
  }
  if (mapped.has("emEh") && hasImportValue(draft.emEh)) {
    patch.emEh = normalizeOptionalText(String(draft.emEh)) ?? "";
  }
  if (mapped.has("employee") && hasImportValue(draft.employee)) {
    patch.employee = normalizeOptionalText(String(draft.employee)) ?? "";
  }
  if (mapped.has("currentGmName") && hasImportValue(draft.currentGmName)) {
    patch.currentGmName = normalizeOptionalText(String(draft.currentGmName)) ?? "";
  }
  if (mapped.has("universeMarket") && hasImportValue(draft.universeMarket)) {
    patch.universeMarket = Boolean(draft.universeMarket);
  }
  if (mapped.has("visitFrequencyPerYear") && hasImportValue(draft.visitFrequencyPerYear)) {
    patch.visitFrequencyPerYear = Number(draft.visitFrequencyPerYear);
  }
  if (mapped.has("infoFlag") && hasImportValue(draft.infoFlag)) {
    patch.infoFlag = Boolean(draft.infoFlag);
  }
  if (mapped.has("infoNote") && hasImportValue(draft.infoNote)) {
    patch.infoNote = normalizeOptionalText(String(draft.infoNote)) ?? "";
  }
  if (mapped.has("isActive") && hasImportValue(draft.isActive)) {
    patch.isActive = Boolean(draft.isActive);
  }

  return patch;
}

function marketPatchHasChanges(
  market: typeof markets.$inferSelect,
  patch: Partial<typeof markets.$inferInsert>,
): boolean {
  return Object.entries(patch).some(([key, value]) => {
    const current = market[key as keyof typeof market];
    if (typeof value === "boolean" || typeof value === "number") return current !== value;
    const nextText = value == null ? "" : String(value);
    const currentText = current == null ? "" : String(current);
    return currentText !== nextText;
  });
}

const kuehlerUpdatePatchKeys: ImportFieldKey[] = [
  "kuehlerInternalId",
  "kuehlerTechnicalIdentNo",
  "kuehlerSerialNumber",
  "kuehlerBd",
  "kuehlerAnzahlKsAmStandort",
  "kuehlerModel",
  "name",
  "employee",
];

function getMappedKuehlerUpdateFields(mapping: z.infer<typeof mappingSchema>): ImportFieldKey[] {
  return kuehlerUpdatePatchKeys.filter((key) => isValidColLetter(mapping[key] ?? ""));
}

export function buildKuehlerUpdatePatch(
  draft: MarketDraft,
  mappedFields: ImportFieldKey[],
): Partial<typeof marketKuehlerUnits.$inferInsert> {
  const mapped = new Set(mappedFields);
  const patch: Partial<typeof marketKuehlerUnits.$inferInsert> = {};

  if (mapped.has("kuehlerInternalId") && hasImportValue(draft.kuehlerInternalId)) {
    patch.kuehlerInternalId = normalizeIdentity(draft.kuehlerInternalId);
  }
  if (mapped.has("kuehlerTechnicalIdentNo") && hasImportValue(draft.kuehlerTechnicalIdentNo)) {
    patch.kuehlerTechnicalIdentNo = normalizeIdentity(draft.kuehlerTechnicalIdentNo);
  }
  if (mapped.has("kuehlerSerialNumber") && hasImportValue(draft.kuehlerSerialNumber)) {
    patch.kuehlerSerialNumber = normalizeOptionalText(draft.kuehlerSerialNumber);
  }
  if (mapped.has("kuehlerBd") && hasImportValue(draft.kuehlerBd)) {
    patch.kuehlerBd = normalizeOptionalText(draft.kuehlerBd);
  }
  if (mapped.has("kuehlerAnzahlKsAmStandort") && hasImportValue(draft.kuehlerAnzahlKsAmStandort)) {
    patch.kuehlerAnzahlKsAmStandort = Number(draft.kuehlerAnzahlKsAmStandort);
  }
  if (mapped.has("kuehlerModel") && hasImportValue(draft.kuehlerModel)) {
    patch.kuehlerModel = normalizeOptionalText(draft.kuehlerModel);
  }
  if (mapped.has("name") && hasImportValue(draft.name)) {
    patch.name = normalizeOptionalText(draft.name) ?? "";
  }
  if (mapped.has("employee") && hasImportValue(draft.employee)) {
    patch.employee = normalizeOptionalText(draft.employee) ?? "";
  }

  return patch;
}

function kuehlerUnitPatchHasChanges(
  unit: typeof marketKuehlerUnits.$inferSelect,
  patch: Partial<typeof marketKuehlerUnits.$inferInsert>,
): boolean {
  return Object.entries(patch).some(([key, value]) => {
    const current = unit[key as keyof typeof unit];
    if (typeof value === "number") return current !== value;
    const nextText = value == null ? "" : String(value);
    const currentText = current == null ? "" : String(current);
    return currentText !== nextText;
  });
}

export function kuehlerSnapshotUnitNeedsUpdate(
  unit: typeof marketKuehlerUnits.$inferSelect,
  marketId: string,
  patch: Partial<typeof marketKuehlerUnits.$inferInsert>,
): boolean {
  return unit.marketId !== marketId || kuehlerUnitPatchHasChanges(unit, patch);
}

function getKuehlerUpdateIdentifierValue(
  unit: typeof marketKuehlerUnits.$inferSelect,
  identifier: KuehlerUpdateIdentifier,
): string | null {
  return normalizeIdentity(unit[identifier]);
}

function getKuehlerUpdateIdentifierLabel(identifier: KuehlerUpdateIdentifier): string {
  if (identifier === "kuehlerInternalId") return "internal_id";
  if (identifier === "kuehlerTechnicalIdentNo") return "Tech. Ident. No.";
  return "Serial Number";
}

async function updateMarketIdsInChunks(
  tx: Parameters<Parameters<typeof db.transaction>[0]>[0],
  ids: string[],
  patch: Partial<typeof markets.$inferInsert>,
): Promise<void> {
  const BATCH_SIZE = 500;
  for (let offset = 0; offset < ids.length; offset += BATCH_SIZE) {
    const batch = ids.slice(offset, offset + BATCH_SIZE);
    if (batch.length === 0) continue;
    await tx.update(markets).set(patch).where(inArray(markets.id, batch));
  }
}

type MarketImportPayload = z.infer<typeof importMarketsSchema>;
type MarketRow = typeof markets.$inferSelect;
type SnapshotIssue = { row: number; reason: string };

function buildGmUpdateSnapshotPreview(payload: MarketImportPayload, existingMarkets: MarketRow[]) {
  const rows = payload.rows.map((row) => row.map((cell) => (cell == null ? "" : String(cell))));
  const dataRows = rows.slice(1);
  const inputRows = dataRows.flatMap((row, index) => row.every((cell) => !cell.trim())
    ? []
    : [{ row: index + 2, flexNumber: getMappedRawCell(row, payload.mapping, "flexNumber") }]);
  const plan = planGmMarketSnapshot(existingMarkets, inputRows, { deactivateSharedMarkets: true });
  const issues: SnapshotIssue[] = [...plan.errors];
  const addIssue = (row: number, reason: string) => { issues.push({ row, reason }); };
  if (inputRows.length === 0) addIssue(0, "Die Datei enthält keine Marktzeilen. Es wird nichts inaktiv gesetzt.");

  const matchedByRow = new Map(plan.matched.map((entry) => [entry.row, entry.market.id]));
  const newRowNumbers = new Set(plan.newRows.map((entry) => entry.row));
  const marketById = new Map(existingMarkets.map((market) => [market.id, market]));
  const mappedUpdateFields = getMappedUpdateFields(payload.mapping);
  const identityFields = [
    { key: "standardMarketNumber" as const, label: "Standardmarkt Nr." },
    { key: "cokeMasterNumber" as const, label: "Stammnr. von Coke" },
  ];
  const identityOwners = new Map<string, string>();
  for (const market of existingMarkets) {
    for (const field of identityFields) {
      const value = normStr(normalizeIdentity(market[field.key]) ?? "");
      if (value) identityOwners.set(`${field.key}:${value}`, market.id);
    }
  }

  let wouldUpdate = 0;
  let unchanged = 0;
  let wouldReactivate = 0;
  for (let index = 0; index < dataRows.length; index += 1) {
    const row = dataRows[index];
    if (!row || row.every((cell) => !cell.trim())) continue;
    const rowNumber = index + 2;
    const marketId = matchedByRow.get(rowNumber);
    if (!marketId && !newRowNumbers.has(rowNumber)) continue;
    const draft = mapRowToDraft(row, payload.mapping, "update");
    const invalidBoolean = findInvalidBooleanUpdateValue(row, payload.mapping, mappedUpdateFields);
    if (invalidBoolean) addIssue(rowNumber, `Ungültiger Ja/Nein-Wert für ${invalidBoolean.key}: ${invalidBoolean.raw}`);
    const normalizedRegion = draft.region != null ? normalizeRegionValue(String(draft.region)) : null;
    if (normalizedRegion && !normalizedRegion.ok) {
      addIssue(rowNumber, `Region konnte nicht normalisiert werden: ${normalizedRegion.raw || "leer"}.`);
    }

    if (newRowNumbers.has(rowNumber)) {
      const missing = (["name", "address", "postalCode", "city", "region"] as const)
        .filter((key) => !String(draft[key] ?? "").trim());
      if (missing.length > 0) {
        addIssue(rowNumber, `Neuer Markt mit Flex ${String(draft.flexNumber ?? "?")}: Pflichtfelder fehlen (${missing.join(", ")}).`);
      }
    }

    for (const field of identityFields) {
      const value = normStr(normalizeIdentity(draft[field.key]) ?? "");
      if (!value) continue;
      const key = `${field.key}:${value}`;
      const owner = identityOwners.get(key);
      if (owner && owner !== (marketId ?? `new:${rowNumber}`)) {
        addIssue(rowNumber, `${field.label} ${String(draft[field.key])} gehört bereits zu einem anderen Markt oder einer anderen Excel-Zeile.`);
      } else {
        identityOwners.set(key, marketId ?? `new:${rowNumber}`);
      }
    }

    if (marketId) {
      const market = marketById.get(marketId);
      if (!market) continue;
      const patch = buildUpdateOnlyMarketPatch(draft, mappedUpdateFields, normalizedRegion);
      if (draft.isActive == null && !market.isActive) patch.isActive = true;
      if (!market.isActive && patch.isActive === true) wouldReactivate += 1;
      if (marketPatchHasChanges(market, patch)) wouldUpdate += 1;
      else unchanged += 1;
    }
  }

  const sourceState = existingMarkets
    .map((market) => [market.id, market.flexNumber, market.marketType, market.isActive, market.updatedAt?.toISOString() ?? null])
    .sort((left, right) => String(left[0]).localeCompare(String(right[0])));
  const snapshotToken = createHash("sha256")
    .update(JSON.stringify({ fileName: payload.fileName, sheetName: payload.sheetName, rows: payload.rows, mapping: payload.mapping, sourceState }))
    .digest("hex");
  const newLabels = plan.newRows.slice(0, 12).map((entry) => ({
    row: entry.row,
    flexNumber: entry.flexNumber ?? "",
    name: String(mapRowToDraft(dataRows[entry.row - 2] ?? [], payload.mapping, "update").name ?? "Neuer Markt"),
  }));

  return {
    snapshotToken,
    canApply: issues.length === 0,
    issueCount: issues.length,
    issues: issues.slice(0, 50),
    sourceRows: inputRows.length,
    existingGmMarkets: existingMarkets.filter((market) => market.marketType !== "kuehler" && market.isActive).length,
    matched: plan.matched.length,
    wouldUpdate,
    unchanged,
    wouldCreate: plan.newRows.length,
    wouldDeactivate: plan.toDeactivate.length,
    wouldReactivate,
    sharedLeftActive: plan.sharedLeftActive.length,
    deactivatedMarkets: plan.toDeactivate.slice(0, 12).map((market) => ({
      id: market.id, flexNumber: market.flexNumber ?? "", name: market.name,
    })),
    createdMarkets: newLabels,
  };
}

function buildKuehlerSnapshotPreview(
  payload: MarketImportPayload,
  existingMarkets: MarketRow[],
  existingUnits: Array<typeof marketKuehlerUnits.$inferSelect>,
) {
  const rows = payload.rows.map((row) => row.map((cell) => (cell == null ? "" : String(cell))));
  const dataRows = rows.slice(1);
  const inputRows = dataRows.flatMap((row, index) => row.every((cell) => !cell.trim()) ? [] : [{
    row: index + 2,
    stammnr: normalizeStammnrForMatch(getMappedRawCell(row, payload.mapping, "kuehlerStammnr")),
    flexNumber: normalizeIdentity(getMappedRawCell(row, payload.mapping, "flexNumber")),
  }]);
  const plan = planKuehlerMarketSnapshot(existingMarkets, inputRows);
  const issues: SnapshotIssue[] = [...plan.errors];
  if (inputRows.length === 0) issues.push({ row: 0, reason: "Die Datei enthält keine Kühlerzeilen. Es wird nichts inaktiv gesetzt." });
  const matchedByRow = new Map(plan.matched.map((item) => [item.row, item.market.id]));
  const matchedMarketByRow = new Map(plan.matched.map((item) => [item.row, item.market]));
  const newMarketRows = new Set(plan.newRows.map((item) => item.row));
  const existingInternalIds = new Map<string, typeof marketKuehlerUnits.$inferSelect>();
  const existingTechnicalIds = new Map<string, typeof marketKuehlerUnits.$inferSelect>();
  const duplicateInternalIds = new Set<string>();
  const duplicateTechnicalIds = new Set<string>();
  for (const unit of existingUnits) {
    const internalKey = normStr(unit.kuehlerInternalId ?? "");
    const technicalKey = normStr(unit.kuehlerTechnicalIdentNo ?? "");
    if (internalKey) {
      if (existingInternalIds.has(internalKey)) duplicateInternalIds.add(internalKey);
      existingInternalIds.set(internalKey, unit);
    }
    if (technicalKey) {
      if (existingTechnicalIds.has(technicalKey)) duplicateTechnicalIds.add(technicalKey);
      existingTechnicalIds.set(technicalKey, unit);
    }
  }
  const seenInternal = new Map<string, number>();
  const seenTechnical = new Map<string, number>();
  const marketFieldsByStammnr = new Map<string, { row: number; values: Record<string, string> }>();
  let existingUnitsMatched = 0;
  let newUnits = 0;
  let unchangedUnits = 0;
  for (let index = 0; index < dataRows.length; index += 1) {
    const row = dataRows[index];
    if (!row || row.every((cell) => !cell.trim())) continue;
    const rowNumber = index + 2;
    const draft = mapRowToDraft(row, payload.mapping, "kuehler_snapshot");
    const matchedMarket = matchedMarketByRow.get(rowNumber);
    const stammnrKey = normalizeStammnrKey(
      draft.kuehlerStammnr ?? matchedMarket?.kuehlerStammnr ?? matchedMarket?.cokeMasterNumber,
    );
    const marketValues = Object.fromEntries(
      (["name", "address", "postalCode", "city", "region", "flexNumber"] as const)
        .map((key) => [key, normStr(String(draft[key] ?? ""))]),
    );
    const previousMarketFields = stammnrKey ? marketFieldsByStammnr.get(stammnrKey) : undefined;
    if (previousMarketFields) {
      for (const [key, value] of Object.entries(marketValues)) {
        if (value && previousMarketFields.values[key] && value !== previousMarketFields.values[key]) {
          issues.push({ row: rowNumber, reason: `${key} widerspricht Zeile ${previousMarketFields.row} für dieselbe Stammnr.` });
        }
        if (value && !previousMarketFields.values[key]) previousMarketFields.values[key] = value;
      }
    } else if (stammnrKey) {
      marketFieldsByStammnr.set(stammnrKey, { row: rowNumber, values: marketValues });
    }
    const internalKey = normStr(normalizeIdentity(draft.kuehlerInternalId) ?? "");
    const technicalKey = normStr(normalizeIdentity(draft.kuehlerTechnicalIdentNo) ?? "");
    if (duplicateInternalIds.has(internalKey) || duplicateTechnicalIds.has(technicalKey)) {
      issues.push({ row: rowNumber, reason: "Kühler-Geräte-ID ist in der Datenbank nicht eindeutig." });
    }
    if (!internalKey && !technicalKey) {
      issues.push({ row: rowNumber, reason: "Für jeden Kühler ist internal_id oder Tech. Ident. No. erforderlich." });
    }
    for (const [key, seen, label] of [
      [internalKey, seenInternal, "internal_id"],
      [technicalKey, seenTechnical, "Tech. Ident. No."],
    ] as const) {
      if (!key) continue;
      const previous = seen.get(key);
      if (previous != null) issues.push({ row: rowNumber, reason: `${label} steht bereits in Zeile ${previous}.` });
      else seen.set(key, rowNumber);
    }
    const internalOwner = internalKey ? existingInternalIds.get(internalKey) : undefined;
    const technicalOwner = technicalKey ? existingTechnicalIds.get(technicalKey) : undefined;
    if (internalOwner && technicalOwner && internalOwner.id !== technicalOwner.id) {
      issues.push({ row: rowNumber, reason: "internal_id und Tech. Ident. No. gehören zu verschiedenen Kühlern." });
    }
    const owner = internalOwner ?? technicalOwner;
    const targetMarketId = matchedByRow.get(rowNumber);
    if (owner && (!targetMarketId || owner.marketId !== targetMarketId)) {
      issues.push({ row: rowNumber, reason: "Der Kühler gehört zu einem anderen Markt. Verschieben bitte separat prüfen." });
    }
    if (owner) {
      existingUnitsMatched += 1;
      if (targetMarketId && !kuehlerSnapshotUnitNeedsUpdate(
        owner,
        targetMarketId,
        buildKuehlerUpdatePatch(draft, getMappedKuehlerUpdateFields(payload.mapping)),
      )) unchangedUnits += 1;
    } else newUnits += 1;
    const normalizedRegion = draft.region != null ? normalizeRegionValue(String(draft.region)) : null;
    if (normalizedRegion && !normalizedRegion.ok) issues.push({ row: rowNumber, reason: `Region konnte nicht normalisiert werden: ${normalizedRegion.raw || "leer"}.` });
    if (newMarketRows.has(rowNumber)) {
      const missing = (["name", "address", "postalCode", "city", "region"] as const)
        .filter((key) => !String(draft[key] ?? "").trim());
      if (missing.length > 0) issues.push({ row: rowNumber, reason: `Neuer Kühler-Markt: Pflichtfelder fehlen (${missing.join(", ")}).` });
    }
  }
  const sourceState = existingMarkets.map((market) => [
    market.id, market.flexNumber, market.cokeMasterNumber, market.kuehlerStammnr,
    market.marketType, market.isActive, market.updatedAt?.toISOString() ?? null,
  ]).sort((left, right) => String(left[0]).localeCompare(String(right[0])));
  const unitState = existingUnits.map((unit) => [
    unit.id, unit.marketId, unit.kuehlerInternalId, unit.kuehlerTechnicalIdentNo, unit.updatedAt?.toISOString() ?? null,
  ]).sort((left, right) => String(left[0]).localeCompare(String(right[0])));
  const snapshotToken = createHash("sha256").update(JSON.stringify({
    fileName: payload.fileName, sheetName: payload.sheetName, rows: payload.rows, mapping: payload.mapping, sourceState, unitState,
  })).digest("hex");
  return {
    snapshotToken,
    canApply: issues.length === 0,
    issueCount: issues.length,
    issues: issues.slice(0, 50),
    sourceRows: inputRows.length,
    existingGmMarkets: existingMarkets.filter((market) => market.marketType !== "universum" && market.isActive).length,
    matched: new Set(plan.matched.map((item) => item.market.id)).size,
    wouldUpdate: new Set(plan.matched.map((item) => item.market.id)).size,
    unchanged: unchangedUnits,
    wouldCreate: plan.newRows.length,
    wouldDeactivate: plan.toDeactivate.length,
    wouldReactivate: new Set(plan.matched.filter((item) => !item.market.isActive).map((item) => item.market.id)).size,
    sharedLeftActive: 0,
    existingUnitsMatched,
    newUnits,
    deactivatedMarkets: plan.toDeactivate.slice(0, 12).map((market) => ({ id: market.id, flexNumber: market.kuehlerStammnr ?? "", name: market.name })),
    createdMarkets: plan.newRows.slice(0, 12).map((item) => ({
      row: item.row, flexNumber: item.stammnr ?? "", name: String(mapRowToDraft(dataRows[item.row - 2] ?? [], payload.mapping, "kuehler_snapshot").name ?? "Neuer Markt"),
    })),
  };
}

function mapMarketRow(row: typeof markets.$inferSelect) {
  const createdAtIso = row.createdAt instanceof Date ? row.createdAt.toISOString() : new Date().toISOString();
  const updatedAtIso = row.updatedAt instanceof Date ? row.updatedAt.toISOString() : createdAtIso;
  const importedAtIso = row.importedAt instanceof Date ? row.importedAt.toISOString() : createdAtIso;
  return {
    id: row.id,
    standardMarketNumber: row.standardMarketNumber,
    cokeMasterNumber: row.cokeMasterNumber,
    flexNumber: row.flexNumber,
    name: row.name,
    dbName: row.dbName,
    address: row.address,
    postalCode: row.postalCode,
    city: row.city,
    region: row.region,
    emEh: row.emEh,
    employee: row.employee,
    currentGmName: row.currentGmName,
    visitFrequencyPerYear: row.visitFrequencyPerYear,
    infoFlag: row.infoFlag,
    infoNote: row.infoNote,
    universeMarket: row.universeMarket,
    marketType: row.marketType,
    kuehlerStammnr: row.kuehlerStammnr,
    isActive: row.isActive,
    importSourceFileName: row.importSourceFileName,
    importedAt: importedAtIso,
    plannedToId: row.plannedToId,
    plannedByActiveStandardGmName: null as string | null,
    isDeleted: row.isDeleted,
    createdAt: createdAtIso,
    updatedAt: updatedAtIso,
  };
}

function mapKuehlerUnitRow(row: typeof marketKuehlerUnits.$inferSelect) {
  const createdAtIso = row.createdAt instanceof Date ? row.createdAt.toISOString() : new Date().toISOString();
  const updatedAtIso = row.updatedAt instanceof Date ? row.updatedAt.toISOString() : createdAtIso;
  const importedAtIso = row.importedAt instanceof Date ? row.importedAt.toISOString() : createdAtIso;
  return {
    id: row.id,
    marketId: row.marketId,
    name: row.name,
    employee: row.employee,
    kuehlerInternalId: row.kuehlerInternalId,
    kuehlerBd: row.kuehlerBd,
    kuehlerAnzahlKsAmStandort: row.kuehlerAnzahlKsAmStandort,
    kuehlerSerialNumber: row.kuehlerSerialNumber,
    kuehlerTechnicalIdentNo: row.kuehlerTechnicalIdentNo,
    kuehlerModel: row.kuehlerModel,
    importSourceFileName: row.importSourceFileName,
    importedAt: importedAtIso,
    isDeleted: row.isDeleted,
    createdAt: createdAtIso,
    updatedAt: updatedAtIso,
  };
}

type ActiveNowCampaignSummary = {
  campaignId: string;
  campaignName: string;
  section: "standard" | "flex" | "kuehler" | "mhd" | "billa" | "durcharbeit";
  targetVisitCount?: number;
  submittedVisitCount?: number;
  isComplete?: boolean;
};

type ProgressSectionKey = "kuehler" | "mhd" | "durcharbeit";

type ProgressMarketRow = {
  marketId: string;
  visitNumber?: number;
  campaignId: string;
  campaignName: string;
  kuehlerUnitId: string | null;
  kuehlerNumber: string | null;
  kuehlerTechnicalIdentNo: string | null;
  chain: string;
  address: string;
  stammnr: string | null;
  done: boolean;
  doneAt: string | null;
};

type ProgressSectionPayload = {
  current: number;
  total: number;
  percent: number;
  startDate: string;
  endDate: string;
  markets: ProgressMarketRow[];
};

type GmMarketDetailSection = ActiveNowCampaignSummary["section"];

type GmMarketDetailCampaign = {
  campaignId: string;
  campaignName: string;
  section: GmMarketDetailSection;
  targetVisitCount: number;
  submittedVisitCount: number;
  isComplete: boolean;
  isStartable: boolean;
};

type GmMarketDetailDraft = {
  sessionId: string;
  startedAt: string;
  campaignIds: string[];
  campaignNames: string[];
};

type GmMarketPastVisitSection = {
  section: GmMarketDetailSection;
  campaignId: string;
  campaignName: string;
  fragebogenName: string;
  answeredQuestionCount: number;
  photoCount: number;
  commentCount: number;
};

function campaignIsLiveNowCondition() {
  return sql`(
    (
      ${campaigns.status} = 'active'
      and (
        ${campaigns.scheduleType} = 'always'
        or (
          ${campaigns.scheduleType} = 'scheduled'
          and ${campaigns.startDate} is not null
          and ${campaigns.endDate} is not null
          and ${campaigns.startDate} <= current_date
          and ${campaigns.endDate} >= current_date
        )
      )
    )
    or (
      ${campaigns.status} = 'scheduled'
      and ${campaigns.scheduleType} = 'scheduled'
      and ${campaigns.startDate} is not null
      and ${campaigns.endDate} is not null
      and ${campaigns.startDate} <= current_date
      and ${campaigns.endDate} >= current_date
    )
  )`;
}

function redPeriodEndExclusive(endDate: Date): Date {
  const next = new Date(endDate);
  next.setDate(next.getDate() + 1);
  next.setHours(0, 0, 0, 0);
  return next;
}

function toIsoOrNull(value: Date | string | null | undefined): string | null {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function durationMinutes(startedAt: Date | string | null | undefined, endedAt: Date | string | null | undefined): number | null {
  const start = startedAt instanceof Date ? startedAt : startedAt ? new Date(startedAt) : null;
  const end = endedAt instanceof Date ? endedAt : endedAt ? new Date(endedAt) : null;
  if (!start || !end || Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return null;
  return Math.max(0, Math.round((end.getTime() - start.getTime()) / 60000));
}

async function resolveActiveStandardGmNamesByMarketIds(marketIds: string[]) {
  const uniqueMarketIds = Array.from(new Set(marketIds));
  if (uniqueMarketIds.length === 0) return new Map<string, string>();

  const rows = await db
    .select({
      marketId: campaignMarketAssignments.marketId,
      assignedAt: campaignMarketAssignments.assignedAt,
      gmFirstName: users.firstName,
      gmLastName: users.lastName,
    })
    .from(campaignMarketAssignments)
    .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
    .innerJoin(users, eq(users.id, campaignMarketAssignments.gmUserId))
    .where(
      and(
        inArray(campaignMarketAssignments.marketId, uniqueMarketIds),
        eq(campaignMarketAssignments.isDeleted, false),
        eq(campaigns.isDeleted, false),
        eq(campaigns.section, "standard"),
        campaignIsLiveNowCondition(),
      ),
    )
    .orderBy(desc(campaignMarketAssignments.assignedAt));

  const byMarket = new Map<string, string>();
  for (const row of rows) {
    if (byMarket.has(row.marketId)) continue;
    const fullName = `${row.gmFirstName ?? ""} ${row.gmLastName ?? ""}`.trim();
    if (fullName) byMarket.set(row.marketId, fullName);
  }
  return byMarket;
}

const marketsRouter = Router();
const adminMarketsRouter = Router();

marketsRouter.use(requireAuth(["admin", "gm", "kunde"]));
marketsRouter.use((req: AuthedRequest, res, next) => {
  if (req.authUser?.role !== "kunde") {
    next();
    return;
  }
  if (req.method.toUpperCase() === "GET" && req.path === "/") {
    requireKundeAdminPermission(req, res, next);
    return;
  }
  res.status(403).json({ error: "Für diese Aktion ist kein Kundenzugriff freigeschaltet.", code: "kunde_permission_denied" });
});
marketsRouter.use((req, res, next) => {
  const shouldTrackRead = req.method.toUpperCase() === "GET" && req.path.startsWith("/gm/");
  if (!shouldTrackRead) {
    next();
    return;
  }
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "markets_gm_read_completed", {
      req,
      action: "markets_gm_read",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

marketsRouter.get("/", async (req, res, next) => {
  try {
    const parsed = listQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid markets query." });
      return;
    }
    const q = parsed.data.q?.trim();
    let rows;
    if (q) {
      const like = `%${q}%`;
      rows = await db
        .select()
        .from(markets)
        .where(
          and(
            eq(markets.isDeleted, false),
            or(
              ilike(markets.name, like),
              ilike(markets.dbName, like),
              ilike(markets.address, like),
              ilike(markets.postalCode, like),
              ilike(markets.city, like),
              ilike(markets.standardMarketNumber, like),
              ilike(markets.cokeMasterNumber, like),
              ilike(markets.flexNumber, like),
              ilike(markets.kuehlerStammnr, like),
              ilike(markets.currentGmName, like),
            ),
          ),
        )
        .orderBy(desc(markets.createdAt));
    } else {
      rows = await db.select().from(markets).where(eq(markets.isDeleted, false)).orderBy(desc(markets.createdAt));
    }
    const deletedRows = await db
      .select({ count: sql<number>`count(*)` })
      .from(markets)
      .where(eq(markets.isDeleted, true));
    const requestUser = req as AuthedRequest & { requestId?: string };
    const gmNamesByMarketId = await resolveActiveStandardGmNamesByMarketIds(rows.map((row) => row.id));
    aggregateHighVolumeLoad({
      metric: "markets",
      scope: q ? "search" : "all",
      loaded: rows.length,
      softDeleted: Number(deletedRows[0]?.count ?? 0),
      requestId: requestUser.requestId ?? null,
      appUserId: requestUser.authUser?.appUserId ?? null,
      role: requestUser.authUser?.role ?? null,
    });
    res.status(200).json({
      markets: rows.map((row) => ({
        ...mapMarketRow(row),
        plannedByActiveStandardGmName: gmNamesByMarketId.get(row.id) ?? null,
      })),
    });
  } catch (err) {
    if (isIdentityConstraintViolation(err)) {
      res.status(409).json({
        error:
          "Import konnte nicht abgeschlossen werden: aktive Märkte mit gleicher Identität existieren bereits.",
      });
      return;
    }
    if (isPgStatementTimeout(err)) {
      res.status(504).json({
        error:
          "Import dauert zu lange (DB-Timeout). Bitte erneut versuchen und parallele Importe vermeiden.",
      });
      return;
    }
    next(err);
  }
});

marketsRouter.get("/gm/assigned-active", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    if (req.authUser?.role === "sm") {
      res.status(403).json({ error: "Nur GMs (oder Admin zur Einsicht) duerfen diese Daten abrufen." });
      return;
    }

    const rows = await db
      .select()
      .from(markets)
      .where(
        and(
          eq(markets.isDeleted, false),
          sql`exists (
            select 1
            from ${campaignMarketAssignments}
            inner join ${campaigns}
              on ${campaigns.id} = ${campaignMarketAssignments.campaignId}
            where ${campaignMarketAssignments.marketId} = ${markets.id}
              and ${campaignMarketAssignments.gmUserId} = ${gmUserId}
              and ${campaignMarketAssignments.isDeleted} = false
              and ${campaigns.isDeleted} = false
              and ${campaignIsLiveNowCondition()}
          )`,
        ),
      )
      .orderBy(desc(markets.createdAt));

    const activeNowCampaignRows = await db
      .select({
        marketId: campaignMarketAssignments.marketId,
        campaignId: campaigns.id,
        campaignName: campaigns.name,
        section: campaigns.section,
        visitTargetCount: campaignMarketAssignments.visitTargetCount,
      })
      .from(campaignMarketAssignments)
      .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
      .where(
        and(
          eq(campaignMarketAssignments.gmUserId, gmUserId),
          eq(campaignMarketAssignments.isDeleted, false),
          eq(campaigns.isDeleted, false),
          campaignIsLiveNowCondition(),
        ),
      );

    const activeNowByMarketId = new Map<string, ActiveNowCampaignSummary[]>();
    const activeNowByKey = new Map<string, ActiveNowCampaignSummary>();
    for (const row of activeNowCampaignRows) {
      const bucket = activeNowByMarketId.get(row.marketId) ?? [];
      const key = `${row.marketId}::${row.campaignId}`;
      const targetCount = Math.max(1, Number(row.visitTargetCount ?? 1));
      const existing = activeNowByKey.get(key);
      if (existing) {
        existing.targetVisitCount = (existing.targetVisitCount ?? 0) + targetCount;
      } else {
        const summary: ActiveNowCampaignSummary = {
          campaignId: row.campaignId,
          campaignName: row.campaignName,
          section: row.section,
          targetVisitCount: targetCount,
          submittedVisitCount: 0,
          isComplete: false,
        };
        activeNowByKey.set(key, summary);
        bucket.push(summary);
      }
      activeNowByMarketId.set(row.marketId, bucket);
    }

    const activeCampaignIds = Array.from(new Set(activeNowCampaignRows.map((row) => row.campaignId)));
    const activeMarketIds = Array.from(new Set(activeNowCampaignRows.map((row) => row.marketId)));
    const kuehlerProgress = await loadKuehlerAssignmentProgress(activeNowCampaignRows
      .filter((row) => row.section === "kuehler").map((row) => ({ ...row, gmUserId })));
    if (activeCampaignIds.length > 0 && activeMarketIds.length > 0) {
      const redPeriod = await resolveCurrentRedPeriod();
      const redEndExclusive = redPeriodEndExclusive(redPeriod.end);
      const submittedRows = await db
        .select({
          marketId: visitSessions.marketId,
          campaignId: visitSessionSections.campaignId,
          visitSessionId: visitSessionSections.visitSessionId,
        })
        .from(visitSessionSections)
        .innerJoin(visitSessions, eq(visitSessions.id, visitSessionSections.visitSessionId))
        .where(
          and(
            inArray(visitSessionSections.campaignId, activeCampaignIds),
            inArray(visitSessions.marketId, activeMarketIds),
            eq(visitSessionSections.isDeleted, false),
            eq(visitSessions.gmUserId, gmUserId),
            eq(visitSessions.status, "submitted"),
            eq(visitSessions.isDeleted, false),
            isNotNull(visitSessions.submittedAt),
            gte(visitSessions.submittedAt, redPeriod.start),
            lt(visitSessions.submittedAt, redEndExclusive),
          ),
        );

      const submittedByKey = new Map<string, Set<string>>();
      for (const row of submittedRows) {
        if (!row.marketId) continue;
        const key = `${row.marketId}::${row.campaignId}`;
        const bucket = submittedByKey.get(key) ?? new Set<string>();
        bucket.add(row.visitSessionId);
        submittedByKey.set(key, bucket);
      }

      for (const [key, summary] of activeNowByKey) {
        const submittedVisitCount = submittedByKey.get(key)?.size ?? 0;
        const targetVisitCount = Math.max(1, Number(summary.targetVisitCount ?? 1));
        summary.submittedVisitCount = submittedVisitCount;
        summary.isComplete = submittedVisitCount >= targetVisitCount;
      }
    }

    for (const [marketId, summaries] of activeNowByMarketId) {
      for (const summary of summaries) {
        if (summary.section !== "kuehler") continue;
        const slots = kuehlerProgress.get(kuehlerProgressKey(summary.campaignId, marketId, gmUserId)) ?? [];
        summary.targetVisitCount = slots.length;
        summary.submittedVisitCount = slots.filter((slot) => slot.submission).length;
        summary.isComplete = slots.length > 0 && summary.submittedVisitCount >= slots.length;
      }
    }

    const [gmNamesByMarketId, nextSmVisitDateByMarketId] = await Promise.all([
      resolveActiveStandardGmNamesByMarketIds(rows.map((row) => row.id)),
      resolveNextSmVisitDatesByGmMarketId(rows),
    ]);
    res.status(200).json({
      markets: rows.map((row) => ({
        ...mapMarketRow(row),
        plannedByActiveStandardGmName: gmNamesByMarketId.get(row.id) ?? null,
        nextSmVisitDate: nextSmVisitDateByMarketId.get(row.id) ?? null,
        activeNowCampaigns: activeNowByMarketId.get(row.id) ?? [],
      })),
    });
  } catch (err) {
    next(err);
  }
});

marketsRouter.get("/gm/flex-start-markets", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    if (req.authUser?.role === "sm") {
      res.status(403).json({ error: "Nur GMs (oder Admin zur Einsicht) duerfen diese Daten abrufen." });
      return;
    }

    const [gmUser] = await db
      .select({ isBillaGm: users.isBillaGm })
      .from(users)
      .where(eq(users.id, gmUserId))
      .limit(1);
    const isBillaGm = Boolean(gmUser?.isBillaGm);
    const specialFilterValues = isBillaGm ? await loadSpecialArthurFilterValues(gmUserId) : [];
    const [globalMarketRows, flexAssignmentRows] = await Promise.all([
      db
        .select()
        .from(markets)
        .where(
          and(
            eq(markets.isDeleted, false),
            eq(markets.isActive, true),
            inArray(markets.marketType, ["universum", "both"]),
            isBillaGm ? billaGmMarketSqlCondition(specialFilterValues) : undefined,
          ),
        )
        .orderBy(desc(markets.createdAt)),
      db
        .select({
          marketId: campaignMarketAssignments.marketId,
          assignedGmUserId: campaigns.assignedGmUserId,
          campaignId: campaigns.id,
          campaignName: campaigns.name,
          section: campaigns.section,
        })
        .from(campaignMarketAssignments)
        .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
        .where(
          and(
            or(isNull(campaigns.assignedGmUserId), eq(campaigns.assignedGmUserId, gmUserId)),
            eq(campaignMarketAssignments.isDeleted, false),
            eq(campaigns.section, "flex"),
            eq(campaigns.isDeleted, false),
            isNotNull(campaigns.currentFragebogenId),
            campaignIsLiveNowCondition(),
          ),
        )
        .orderBy(asc(campaigns.name)),
    ]);

    const rows = globalMarketRows;
    const assignmentsByMarketId = new Map<string, typeof flexAssignmentRows>();
    for (const row of flexAssignmentRows) {
      const bucket = assignmentsByMarketId.get(row.marketId) ?? [];
      bucket.push(row);
      assignmentsByMarketId.set(row.marketId, bucket);
    }
    const gmNamesByMarketId = await resolveActiveStandardGmNamesByMarketIds(rows.map((row) => row.id));
    res.status(200).json({
      markets: rows
        .map((row) => {
          const marketAssignments = assignmentsByMarketId.get(row.id) ?? [];
          const activeNowCampaigns: ActiveNowCampaignSummary[] = selectEffectiveFlexCampaigns(marketAssignments, gmUserId)
            .map((entry) => ({
              campaignId: entry.campaignId,
              campaignName: entry.campaignName,
              section: entry.section,
            }));
          return {
            ...mapMarketRow(row),
            plannedByActiveStandardGmName: gmNamesByMarketId.get(row.id) ?? null,
            activeNowCampaigns,
          };
        })
        .filter((row) => row.activeNowCampaigns.length > 0),
    });
  } catch (err) {
    next(err);
  }
});

marketsRouter.get("/gm/:marketId/detail", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    if (req.authUser?.role === "sm") {
      res.status(403).json({ error: "Nur GMs (oder Admin zur Einsicht) duerfen diese Daten abrufen." });
      return;
    }

    const parsed = z.object({ marketId: z.string().uuid() }).safeParse(req.params);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Markt-ID." });
      return;
    }
    const { marketId } = parsed.data;

    const redPeriod = await resolveCurrentRedPeriod();
    const redEndExclusive = redPeriodEndExclusive(redPeriod.end);

    const [marketRow] = await db
      .select()
      .from(markets)
      .where(and(eq(markets.id, marketId), eq(markets.isDeleted, false)))
      .limit(1);

    if (!marketRow) {
      res.status(404).json({ error: "Markt nicht gefunden." });
      return;
    }
    const [gmUser] = await db
      .select({ isBillaGm: users.isBillaGm })
      .from(users)
      .where(eq(users.id, gmUserId))
      .limit(1);
    const isBillaGm = Boolean(gmUser?.isBillaGm);
    const specialFilterValues = isBillaGm ? await loadSpecialArthurFilterValues(gmUserId) : [];
    const canUseFlexCampaignAtMarket = Boolean(
      marketRow.isActive
      && (marketRow.marketType === "universum" || marketRow.marketType === "both")
      && (!isBillaGm || isMarketVisibleForBillaGm(marketRow, specialFilterValues)),
    );
    const assignedCampaignAudienceCondition = canUseFlexCampaignAtMarket
      ? or(
          and(ne(campaigns.section, "flex"), eq(campaignMarketAssignments.gmUserId, gmUserId)),
          and(eq(campaigns.section, "flex"), eq(campaigns.assignedGmUserId, gmUserId)),
        )
      : and(ne(campaigns.section, "flex"), eq(campaignMarketAssignments.gmUserId, gmUserId));

    const assignedCampaignRows = await db
      .select({
        campaignId: campaigns.id,
        campaignName: campaigns.name,
        section: campaigns.section,
        currentFragebogenId: campaigns.currentFragebogenId,
        visitTargetCount: campaignMarketAssignments.visitTargetCount,
      })
      .from(campaignMarketAssignments)
      .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
      .where(
        and(
          assignedCampaignAudienceCondition,
          eq(campaignMarketAssignments.marketId, marketId),
          eq(campaignMarketAssignments.isDeleted, false),
          eq(campaigns.isDeleted, false),
          campaignIsLiveNowCondition(),
        ),
      )
      .orderBy(asc(campaigns.section), asc(campaigns.name));

    const activeByCampaignId = new Map<
      string,
      {
        campaignId: string;
        campaignName: string;
        section: GmMarketDetailSection;
        currentFragebogenId: string | null;
        targetVisitCount: number;
      }
    >();

    for (const row of assignedCampaignRows) {
      const existing = activeByCampaignId.get(row.campaignId);
      if (existing) {
        existing.targetVisitCount += Math.max(1, Number(row.visitTargetCount ?? 1));
        continue;
      }
      activeByCampaignId.set(row.campaignId, {
        campaignId: row.campaignId,
        campaignName: row.campaignName,
        section: row.section,
        currentFragebogenId: row.currentFragebogenId,
        targetVisitCount: Math.max(1, Number(row.visitTargetCount ?? 1)),
      });
    }

    const hasAssignedFlexCampaign = assignedCampaignRows.some(
      (row) => row.section === "flex" && Boolean(row.currentFragebogenId),
    );

    if (
      !hasAssignedFlexCampaign &&
      canUseFlexCampaignAtMarket
    ) {
      const activeFlexRows = await db
        .select({
          campaignId: campaigns.id,
          campaignName: campaigns.name,
          section: campaigns.section,
          currentFragebogenId: campaigns.currentFragebogenId,
          visitTargetCount: campaignMarketAssignments.visitTargetCount,
        })
        .from(campaignMarketAssignments)
        .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
        .where(
          and(
            eq(campaignMarketAssignments.marketId, marketId),
            eq(campaignMarketAssignments.isDeleted, false),
            eq(campaigns.section, "flex"),
            isNull(campaigns.assignedGmUserId),
            eq(campaigns.isDeleted, false),
            isNotNull(campaigns.currentFragebogenId),
            campaignIsLiveNowCondition(),
          ),
        )
        .orderBy(asc(campaigns.name));

      for (const row of activeFlexRows) {
        if (activeByCampaignId.has(row.campaignId)) continue;
        activeByCampaignId.set(row.campaignId, {
          campaignId: row.campaignId,
          campaignName: row.campaignName,
          section: row.section,
          currentFragebogenId: row.currentFragebogenId,
          targetVisitCount: Math.max(1, Number(row.visitTargetCount ?? 1)),
        });
      }
    }

    const activeCampaignIds = Array.from(activeByCampaignId.keys());
    if (activeCampaignIds.length === 0) {
      res.status(403).json({ error: "Dieser Markt ist für dich aktuell nicht startbar." });
      return;
    }

    const submittedRows = await db
      .select({
        campaignId: visitSessionSections.campaignId,
        visitSessionId: visitSessionSections.visitSessionId,
      })
      .from(visitSessionSections)
      .innerJoin(visitSessions, eq(visitSessions.id, visitSessionSections.visitSessionId))
      .where(
        and(
          inArray(visitSessionSections.campaignId, activeCampaignIds),
          eq(visitSessionSections.isDeleted, false),
          eq(visitSessions.gmUserId, gmUserId),
          eq(visitSessions.marketId, marketId),
          eq(visitSessions.status, "submitted"),
          eq(visitSessions.isDeleted, false),
          isNotNull(visitSessions.submittedAt),
          gte(visitSessions.submittedAt, redPeriod.start),
          lt(visitSessions.submittedAt, redEndExclusive),
        ),
      );

    const submittedByCampaign = new Map<string, Set<string>>();
    for (const row of submittedRows) {
      const bucket = submittedByCampaign.get(row.campaignId) ?? new Set<string>();
      bucket.add(row.visitSessionId);
      submittedByCampaign.set(row.campaignId, bucket);
    }

    const kuehlerProgress = await loadKuehlerAssignmentProgress(Array.from(activeByCampaignId.values())
      .filter((row) => row.section === "kuehler")
      .map((row) => ({ campaignId: row.campaignId, marketId, gmUserId, visitTargetCount: row.targetVisitCount })));
    const activeCampaigns: GmMarketDetailCampaign[] = Array.from(activeByCampaignId.values())
      .map((row) => {
        const slots = row.section === "kuehler" ? kuehlerProgress.get(kuehlerProgressKey(row.campaignId, marketId, gmUserId)) : undefined;
        const submittedVisitCount = slots ? slots.filter((slot) => slot.submission).length : submittedByCampaign.get(row.campaignId)?.size ?? 0;
        const targetVisitCount = slots ? slots.length : row.targetVisitCount;
        const isComplete = submittedVisitCount >= targetVisitCount;
        return {
          campaignId: row.campaignId,
          campaignName: row.campaignName,
          section: row.section,
          targetVisitCount,
          submittedVisitCount,
          isComplete,
          isStartable: Boolean(row.currentFragebogenId),
        };
      })
      .sort((left, right) => {
        const sectionDiff = SECTION_ORDER.indexOf(left.section) - SECTION_ORDER.indexOf(right.section);
        return sectionDiff || left.campaignName.localeCompare(right.campaignName, "de");
      });

    const draftSessionRows = await db
      .select({
        sessionId: visitSessions.id,
        startedAt: visitSessions.startedAt,
      })
      .from(visitSessions)
      .where(
        and(
          eq(visitSessions.gmUserId, gmUserId),
          eq(visitSessions.marketId, marketId),
          eq(visitSessions.status, "draft"),
          eq(visitSessions.isDeleted, false),
          isNull(visitSessions.submittedAt),
        ),
      )
      .orderBy(desc(visitSessions.startedAt))
      .limit(5);

    const draftSessionIds = draftSessionRows.map((row) => row.sessionId);
    const draftSectionsBySession = new Map<string, Array<{ campaignId: string; campaignName: string }>>();
    if (draftSessionIds.length > 0) {
      const draftSectionRows = await db
        .select({
          visitSessionId: visitSessionSections.visitSessionId,
          campaignId: visitSessionSections.campaignId,
          campaignName: campaigns.name,
        })
        .from(visitSessionSections)
        .innerJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
        .where(and(inArray(visitSessionSections.visitSessionId, draftSessionIds), eq(visitSessionSections.isDeleted, false)))
        .orderBy(asc(visitSessionSections.orderIndex));

      for (const row of draftSectionRows) {
        const bucket = draftSectionsBySession.get(row.visitSessionId) ?? [];
        bucket.push({ campaignId: row.campaignId, campaignName: row.campaignName });
        draftSectionsBySession.set(row.visitSessionId, bucket);
      }
    }

    const drafts: GmMarketDetailDraft[] = draftSessionRows.map((row) => {
      const sections = draftSectionsBySession.get(row.sessionId) ?? [];
      return {
        sessionId: row.sessionId,
        startedAt: row.startedAt.toISOString(),
        campaignIds: sections.map((section) => section.campaignId),
        campaignNames: sections.map((section) => section.campaignName),
      };
    });

    const pastSessionRows = await db
      .select({
        sessionId: visitSessions.id,
        startedAt: visitSessions.startedAt,
        submittedAt: visitSessions.submittedAt,
      })
      .from(visitSessions)
      .where(
        and(
          eq(visitSessions.gmUserId, gmUserId),
          eq(visitSessions.marketId, marketId),
          eq(visitSessions.status, "submitted"),
          eq(visitSessions.isDeleted, false),
          isNotNull(visitSessions.submittedAt),
        ),
      )
      .orderBy(desc(visitSessions.submittedAt), desc(visitSessions.createdAt))
      .limit(10);

    const pastSessionIds = pastSessionRows.map((row) => row.sessionId);
    const pastSectionsBySession = new Map<string, GmMarketPastVisitSection[]>();
    if (pastSessionIds.length > 0) {
      const pastSectionRows = await db
        .select({
          sectionId: visitSessionSections.id,
          visitSessionId: visitSessionSections.visitSessionId,
          section: visitSessionSections.section,
          campaignId: visitSessionSections.campaignId,
          campaignName: campaigns.name,
          fragebogenName: visitSessionSections.fragebogenNameSnapshot,
        })
        .from(visitSessionSections)
        .innerJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
        .where(and(inArray(visitSessionSections.visitSessionId, pastSessionIds), eq(visitSessionSections.isDeleted, false)))
        .orderBy(asc(visitSessionSections.orderIndex));

      const answerCountsBySection = new Map<string, number>();
      const photoCountsBySection = new Map<string, number>();
      const commentCountsBySection = new Map<string, number>();

      const answerRows = await db
        .select({
          answerId: visitAnswers.id,
          sectionId: visitAnswers.visitSessionSectionId,
        })
        .from(visitAnswers)
        .where(
          and(
            inArray(visitAnswers.visitSessionId, pastSessionIds),
            eq(visitAnswers.isDeleted, false),
            eq(visitAnswers.answerStatus, "answered"),
          ),
        );

      const answerIds = answerRows.map((row) => row.answerId);
      const sectionByAnswerId = new Map(answerRows.map((row) => [row.answerId, row.sectionId]));
      for (const row of answerRows) {
        answerCountsBySection.set(row.sectionId, (answerCountsBySection.get(row.sectionId) ?? 0) + 1);
      }

      if (answerIds.length > 0) {
        const photoRows = await db
          .select({
            answerId: visitAnswerPhotos.visitAnswerId,
            photoId: visitAnswerPhotos.id,
          })
          .from(visitAnswerPhotos)
          .where(and(inArray(visitAnswerPhotos.visitAnswerId, answerIds), eq(visitAnswerPhotos.isDeleted, false)));

        for (const row of photoRows) {
          const sectionId = sectionByAnswerId.get(row.answerId);
          if (!sectionId) continue;
          photoCountsBySection.set(sectionId, (photoCountsBySection.get(sectionId) ?? 0) + 1);
        }
      }

      const commentRows = await db
        .select({
          sectionId: visitSessionQuestions.visitSessionSectionId,
          commentId: visitQuestionComments.id,
        })
        .from(visitQuestionComments)
        .innerJoin(visitSessionQuestions, eq(visitSessionQuestions.id, visitQuestionComments.visitSessionQuestionId))
        .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitSessionQuestions.visitSessionSectionId))
        .where(
          and(
            inArray(visitSessionSections.visitSessionId, pastSessionIds),
            eq(visitQuestionComments.isDeleted, false),
            eq(visitSessionQuestions.isDeleted, false),
            eq(visitSessionSections.isDeleted, false),
            sql`trim(${visitQuestionComments.commentText}) <> ''`,
          ),
        );

      for (const row of commentRows) {
        commentCountsBySection.set(row.sectionId, (commentCountsBySection.get(row.sectionId) ?? 0) + 1);
      }

      for (const row of pastSectionRows) {
        const bucket = pastSectionsBySession.get(row.visitSessionId) ?? [];
        bucket.push({
          section: row.section,
          campaignId: row.campaignId,
          campaignName: row.campaignName,
          fragebogenName: row.fragebogenName,
          answeredQuestionCount: answerCountsBySection.get(row.sectionId) ?? 0,
          photoCount: photoCountsBySection.get(row.sectionId) ?? 0,
          commentCount: commentCountsBySection.get(row.sectionId) ?? 0,
        });
        pastSectionsBySession.set(row.visitSessionId, bucket);
      }
    }

    const [gmNamesByMarketId, kuehlerTechnicalIdentRows] = await Promise.all([
      resolveActiveStandardGmNamesByMarketIds([marketId]),
      db
        .select({ value: marketKuehlerUnits.kuehlerTechnicalIdentNo })
        .from(marketKuehlerUnits)
        .where(
          and(
            eq(marketKuehlerUnits.marketId, marketId),
            eq(marketKuehlerUnits.isDeleted, false),
            isNotNull(marketKuehlerUnits.kuehlerTechnicalIdentNo),
          ),
        )
        .orderBy(asc(marketKuehlerUnits.kuehlerTechnicalIdentNo)),
    ]);
    const kuehlerTechnicalIdentNos = Array.from(
      new Set(
        kuehlerTechnicalIdentRows
          .map((row) => row.value?.trim() ?? "")
          .filter(Boolean),
      ),
    );
    res.status(200).json({
      period: {
        startDate: toYmd(redPeriod.start),
        endDate: toYmd(redPeriod.end),
      },
      market: {
        ...mapMarketRow(marketRow),
        plannedByActiveStandardGmName: gmNamesByMarketId.get(marketId) ?? null,
      },
      activeCampaigns,
      kuehlerTechnicalIdentNos,
      drafts,
      pastVisits: pastSessionRows.map((row) => ({
        sessionId: row.sessionId,
        startedAt: row.startedAt.toISOString(),
        submittedAt: toIsoOrNull(row.submittedAt),
        durationMinutes: durationMinutes(row.startedAt, row.submittedAt),
        sections: pastSectionsBySession.get(row.sessionId) ?? [],
      })),
    });
  } catch (err) {
    next(err);
  }
});

marketsRouter.get("/gm/kuehler-mhd-progress", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    if (req.authUser?.role === "sm") {
      res.status(403).json({ error: "Nur GMs (oder Admin zur Einsicht) duerfen diese Daten abrufen." });
      return;
    }

    const { startYmd: redStartYmd, endYmd: redEndYmd } = await getCurrentRedPeriodBounds();

    const assignmentRows = await db
      .select({
        marketId: campaignMarketAssignments.marketId,
        campaignId: campaigns.id,
        campaignName: campaigns.name,
        section: campaigns.section,
        scheduleType: campaigns.scheduleType,
        startDate: campaigns.startDate,
        endDate: campaigns.endDate,
        marketName: markets.name,
        marketDbName: markets.dbName,
        marketAddress: markets.address,
        marketPostalCode: markets.postalCode,
        marketCity: markets.city,
        marketKuehlerStammnr: markets.kuehlerStammnr,
        marketCokeMasterNumber: markets.cokeMasterNumber,
        visitTargetCount: campaignMarketAssignments.visitTargetCount,
      })
      .from(campaignMarketAssignments)
      .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
      .innerJoin(markets, eq(markets.id, campaignMarketAssignments.marketId))
      .where(
        and(
          eq(campaignMarketAssignments.gmUserId, gmUserId),
          eq(campaignMarketAssignments.isDeleted, false),
          eq(campaigns.isDeleted, false),
          eq(markets.isDeleted, false),
          campaignIsLiveNowCondition(),
          inArray(campaigns.section, ["kuehler", "mhd", "durcharbeit"]),
        ),
      )
      .orderBy(asc(campaigns.section), asc(campaigns.name), asc(markets.name), asc(markets.address));

    const assignmentKeys = new Map<
      string,
      {
        section: ProgressSectionKey;
        marketId: string;
        campaignId: string;
        campaignName: string;
        chain: string;
        address: string;
        stammnr: string | null;
        visitTargetCount: number;
      }
    >();
    const dateRangeBySection = new Map<ProgressSectionKey, { startDate: string; endDate: string }>([
      ["kuehler", { startDate: redStartYmd, endDate: redEndYmd }],
      ["mhd", { startDate: redStartYmd, endDate: redEndYmd }],
      ["durcharbeit", { startDate: redStartYmd, endDate: redEndYmd }],
    ]);

    for (const row of assignmentRows) {
      const section = row.section as ProgressSectionKey;
      const key = `${row.marketId}__${row.campaignId}`;
      const visitTargetCount = Math.max(1, Number(row.visitTargetCount ?? 1));
      const existing = assignmentKeys.get(key);
      if (existing) {
        existing.visitTargetCount += visitTargetCount;
      } else {
        assignmentKeys.set(key, {
          section,
          marketId: row.marketId,
          campaignId: row.campaignId,
          campaignName: row.campaignName,
          chain: deriveChainLabel({
            name: row.marketName,
            dbName: row.marketDbName ?? "",
          }),
          address: `${row.marketAddress}, ${row.marketPostalCode} ${row.marketCity}`.trim(),
          stammnr: row.marketKuehlerStammnr ?? row.marketCokeMasterNumber ?? null,
          visitTargetCount,
        });
      }
      const currentRange = dateRangeBySection.get(section);
      if (!currentRange) continue;
      const effectiveStart = row.scheduleType === "scheduled" && row.startDate ? String(row.startDate) : redStartYmd;
      const effectiveEnd = row.scheduleType === "scheduled" && row.endDate ? String(row.endDate) : redEndYmd;
      currentRange.startDate = effectiveStart < currentRange.startDate ? effectiveStart : currentRange.startDate;
      currentRange.endDate = effectiveEnd > currentRange.endDate ? effectiveEnd : currentRange.endDate;
      dateRangeBySection.set(section, currentRange);
    }

    const kuehlerProgress = await loadKuehlerAssignmentProgress(Array.from(assignmentKeys.values())
      .filter((row) => row.section === "kuehler").map((row) => ({ ...row, gmUserId })));

    const assignedCampaignIds = Array.from(new Set(Array.from(assignmentKeys.values()).map((row) => row.campaignId)));
    const completionRows =
      assignedCampaignIds.length === 0
        ? []
        : await db
            .select({
              sessionId: visitSessions.id,
              marketId: visitSessions.marketId,
              kuehlerUnitId: visitSessions.kuehlerUnitId,
              campaignId: visitSessionSections.campaignId,
              submittedAt: visitSessions.submittedAt,
            })
            .from(visitSessionSections)
            .innerJoin(visitSessions, eq(visitSessions.id, visitSessionSections.visitSessionId))
            .where(
              and(
                eq(visitSessionSections.isDeleted, false),
                eq(visitSessions.isDeleted, false),
                eq(visitSessions.gmUserId, gmUserId),
                eq(visitSessions.status, "submitted"),
                inArray(visitSessionSections.campaignId, assignedCampaignIds),
                isNotNull(visitSessions.submittedAt),
                inArray(visitSessionSections.section, ["kuehler", "mhd", "durcharbeit"]),
              ),
            )
            .orderBy(desc(visitSessions.submittedAt));

    const completedByKey = new Map<string, string>();
    for (const row of completionRows) {
      if (!row.submittedAt) continue;
      const key = `${row.marketId}__${row.campaignId}`;
      if (!assignmentKeys.has(key)) continue;
      if (!completedByKey.has(key)) completedByKey.set(key, row.submittedAt.toISOString());
    }

    const buildSectionPayload = (section: ProgressSectionKey): ProgressSectionPayload => {
      const rows = Array.from(assignmentKeys.values()).filter((entry) => entry.section === section);
      const markets = rows.flatMap<ProgressMarketRow>((row) => {
        const key = `${row.marketId}__${row.campaignId}`;
        if (section === "kuehler") {
          return (kuehlerProgress.get(kuehlerProgressKey(row.campaignId, row.marketId, gmUserId)) ?? []).map(({ unit, visitNumber, submission }) => {
            const doneAt = submission?.submittedAt?.toISOString() ?? null;
            return {
              marketId: row.marketId,
              visitNumber,
              campaignId: row.campaignId,
              campaignName: row.campaignName,
              kuehlerUnitId: unit?.id ?? null,
              kuehlerNumber: unit?.kuehlerInternalId ?? null,
              kuehlerTechnicalIdentNo: unit?.kuehlerTechnicalIdentNo ?? null,
              chain: row.chain,
              address: row.address,
              stammnr: row.stammnr,
              done: Boolean(doneAt),
              doneAt,
            };
          });
        }
        const doneAt = completedByKey.get(key) ?? null;
        return [{
          marketId: row.marketId,
          campaignId: row.campaignId,
          campaignName: row.campaignName,
          kuehlerUnitId: null,
          kuehlerNumber: null,
          kuehlerTechnicalIdentNo: null,
          chain: row.chain,
          address: row.address,
          stammnr: row.stammnr,
          done: Boolean(doneAt),
          doneAt,
        }];
      });
      const total = markets.length;
      const current = markets.filter((row) => row.done).length;
      const percent = total > 0 ? Math.round((current / total) * 100) : 0;
      const range = dateRangeBySection.get(section) ?? { startDate: redStartYmd, endDate: redEndYmd };
      return {
        current,
        total,
        percent,
        startDate: range.startDate,
        endDate: range.endDate,
        markets,
      };
    };

    const kuehler = buildSectionPayload("kuehler");
    const mhd = buildSectionPayload("mhd");
    const durcharbeit = buildSectionPayload("durcharbeit");
    res.status(200).json({
      kuehler,
      mhd,
      durcharbeit,
      generatedAt: new Date().toISOString(),
      timezone: "Europe/Vienna",
      periodFallback: {
        startDate: redStartYmd,
        endDate: redEndYmd,
      },
    });
  } catch (err) {
    next(err);
  }
});

marketsRouter.get("/gm/visit-start", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    if (req.authUser?.role === "sm") {
      res.status(403).json({ error: "Nur GMs (oder Admin zur Einsicht) duerfen diese Daten abrufen." });
      return;
    }

    const parsed = z
      .object({
        marketId: z.string().uuid(),
        campaignIds: z.string().min(1),
      })
      .safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige visit-start Parameter." });
      return;
    }

    const campaignIds = Array.from(
      new Set(
        parsed.data.campaignIds
          .split(",")
          .map((entry) => entry.trim())
          .filter((entry) => /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(entry)),
      ),
    );
    if (campaignIds.length === 0) {
      res.status(400).json({ error: "Mindestens eine gültige Kampagnen-ID ist erforderlich." });
      return;
    }

    const marketId = parsed.data.marketId;
    const [market] = await db
      .select()
      .from(markets)
      .where(and(eq(markets.id, marketId), eq(markets.isDeleted, false)))
      .limit(1);
    if (!market) {
      res.status(404).json({ error: "Markt nicht gefunden." });
      return;
    }
    const marketChain = normalizeMarketChain(market.dbName);
    const [gmUser] = await db
      .select({ isBillaGm: users.isBillaGm })
      .from(users)
      .where(eq(users.id, gmUserId))
      .limit(1);
    const isBillaGm = Boolean(gmUser?.isBillaGm);
    const specialFilterValues = isBillaGm ? await loadSpecialArthurFilterValues(gmUserId) : [];
    const canUseFlexCampaignAtMarket = Boolean(
      market.isActive
      && (market.marketType === "universum" || market.marketType === "both")
      && (!isBillaGm || isMarketVisibleForBillaGm(market, specialFilterValues)),
    );
    const [dedicatedFlexCampaign] = canUseFlexCampaignAtMarket
      ? await db
          .select({ campaignId: campaigns.id })
          .from(campaignMarketAssignments)
          .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
          .where(
            and(
              eq(campaignMarketAssignments.marketId, marketId),
              eq(campaignMarketAssignments.isDeleted, false),
              eq(campaigns.section, "flex"),
              eq(campaigns.assignedGmUserId, gmUserId),
              eq(campaigns.isDeleted, false),
              isNotNull(campaigns.currentFragebogenId),
              campaignIsLiveNowCondition(),
            ),
          )
          .limit(1)
      : [];
    const selectedCampaignAudienceCondition = or(
      and(ne(campaigns.section, "flex"), eq(campaignMarketAssignments.gmUserId, gmUserId)),
      canUseFlexCampaignAtMarket
        ? and(
            eq(campaigns.section, "flex"),
            dedicatedFlexCampaign
              ? eq(campaigns.assignedGmUserId, gmUserId)
              : isNull(campaigns.assignedGmUserId),
          )
        : sql`false`,
    );

    const selectedRows = await db
      .select({
        campaignId: campaigns.id,
        campaignName: campaigns.name,
        section: campaigns.section,
        currentFragebogenId: campaigns.currentFragebogenId,
      })
      .from(campaignMarketAssignments)
      .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
      .where(
        and(
          eq(campaignMarketAssignments.marketId, marketId),
          selectedCampaignAudienceCondition,
          inArray(campaigns.id, campaignIds),
          eq(campaignMarketAssignments.isDeleted, false),
          eq(campaigns.isDeleted, false),
          campaignIsLiveNowCondition(),
        ),
      );

    const selectedById = new Map(selectedRows.map((row) => [row.campaignId, row]));
    const selectedInRequestOrder = campaignIds
      .map((campaignId) => selectedById.get(campaignId))
      .filter(Boolean) as typeof selectedRows;

    const normalizedSelected = selectedInRequestOrder
      .filter((row) => row.currentFragebogenId)
      .map((row) => ({
        ...row,
        currentFragebogenId: row.currentFragebogenId as string,
      }));

    const grouped = {
      main: normalizedSelected.filter((row) => row.section === "standard" || row.section === "flex" || row.section === "billa"),
      kuehler: normalizedSelected.filter((row) => row.section === "kuehler"),
      mhd: normalizedSelected.filter((row) => row.section === "mhd"),
    };

    const mainFbIds = Array.from(new Set(grouped.main.map((row) => row.currentFragebogenId)));
    const kuehlerFbIds = Array.from(new Set(grouped.kuehler.map((row) => row.currentFragebogenId)));
    const mhdFbIds = Array.from(new Set(grouped.mhd.map((row) => row.currentFragebogenId)));

    const [mainFragebogen, kuehlerFragebogen, mhdFragebogen] = await Promise.all([
      mainFbIds.length > 0 ? fetchFragebogenUi("main", mainFbIds) : Promise.resolve([]),
      kuehlerFbIds.length > 0 ? fetchFragebogenUi("kuehler", kuehlerFbIds) : Promise.resolve([]),
      mhdFbIds.length > 0 ? fetchFragebogenUi("mhd", mhdFbIds) : Promise.resolve([]),
    ]);

    const mainFbById = new Map(mainFragebogen.map((entry) => [entry.id, entry]));
    const kuehlerFbById = new Map(kuehlerFragebogen.map((entry) => [entry.id, entry]));
    const mhdFbById = new Map(mhdFragebogen.map((entry) => [entry.id, entry]));

    const mainModuleIds = Array.from(new Set(mainFragebogen.flatMap((entry) => entry.moduleIds ?? [])));
    const kuehlerModuleIds = Array.from(new Set(kuehlerFragebogen.flatMap((entry) => entry.moduleIds ?? [])));
    const mhdModuleIds = Array.from(new Set(mhdFragebogen.flatMap((entry) => entry.moduleIds ?? [])));

    const [mainModules, kuehlerModules, mhdModules] = await Promise.all([
      mainModuleIds.length > 0 ? fetchModulesUi("main", mainModuleIds) : Promise.resolve([]),
      kuehlerModuleIds.length > 0 ? fetchModulesUi("kuehler", kuehlerModuleIds) : Promise.resolve([]),
      mhdModuleIds.length > 0 ? fetchModulesUi("mhd", mhdModuleIds) : Promise.resolve([]),
    ]);

    const mainModuleById = new Map(mainModules.map((entry) => [entry.id, entry]));
    const kuehlerModuleById = new Map(kuehlerModules.map((entry) => [entry.id, entry]));
    const mhdModuleById = new Map(mhdModules.map((entry) => [entry.id, entry]));
    const configuredTagIds = normalizeUnique(
      [...mainModules, ...kuehlerModules, ...mhdModules].flatMap((module) =>
        (module.questions ?? []).flatMap((question) =>
          extractConfiguredPhotoTagIds((question.config ?? {}) as Record<string, unknown>),
        ),
      ),
    );
    const configuredTagRows =
      configuredTagIds.length === 0
        ? []
        : await db
            .select({ id: photoTags.id, label: photoTags.label, deletedAt: photoTags.deletedAt })
            .from(photoTags)
            .where(inArray(photoTags.id, configuredTagIds));
    const configuredTagById = new Map(
      configuredTagRows.map((row) => [
        row.id,
        {
          id: row.id,
          label: row.label,
          deletedAt: row.deletedAt ? row.deletedAt.toISOString() : null,
        },
      ]),
    );

    const sections = normalizedSelected
      .map((selected) => {
        const getPayload = () => {
          if (selected.section === "kuehler") {
            const fb = kuehlerFbById.get(selected.currentFragebogenId);
            if (!fb) return null;
            const questions = (fb.moduleIds ?? []).flatMap((moduleId) => {
              const module = kuehlerModuleById.get(moduleId);
              if (!module) return [];
              return (module.questions ?? []).map((question) => {
                const baseConfig = (question.config ?? {}) as Record<string, unknown>;
                const config = enrichQuestionConfigWithTagMeta(question.type, baseConfig, configuredTagById);
                return {
                id: question.id,
                type: question.type,
                text: question.text,
                required: question.required,
                config,
                rules: question.rules ?? [],
                scoring: question.scoring ?? {},
                chains: normalizeQuestionChains(question.chains),
                appliesToMarketChain: true,
                options: Array.isArray(config.options) ? (config.options as string[]) : undefined,
                moduleId: module.id,
                moduleName: module.name,
                };
              }).filter((question) => isQuestionApplicableToMarketChain(question.chains, marketChain));
            });
            return { fragebogenId: fb.id, fragebogenName: fb.name, questions };
          }
          if (selected.section === "mhd") {
            const fb = mhdFbById.get(selected.currentFragebogenId);
            if (!fb) return null;
            const questions = (fb.moduleIds ?? []).flatMap((moduleId) => {
              const module = mhdModuleById.get(moduleId);
              if (!module) return [];
              return (module.questions ?? []).map((question) => {
                const baseConfig = (question.config ?? {}) as Record<string, unknown>;
                const config = enrichQuestionConfigWithTagMeta(question.type, baseConfig, configuredTagById);
                return {
                id: question.id,
                type: question.type,
                text: question.text,
                required: question.required,
                config,
                rules: question.rules ?? [],
                scoring: question.scoring ?? {},
                chains: normalizeQuestionChains(question.chains),
                appliesToMarketChain: true,
                options: Array.isArray(config.options) ? (config.options as string[]) : undefined,
                moduleId: module.id,
                moduleName: module.name,
                };
              }).filter((question) => isQuestionApplicableToMarketChain(question.chains, marketChain));
            });
            return { fragebogenId: fb.id, fragebogenName: fb.name, questions };
          }
          const fb = mainFbById.get(selected.currentFragebogenId);
          if (!fb) return null;
          const questions = (fb.moduleIds ?? []).flatMap((moduleId) => {
            const module = mainModuleById.get(moduleId);
            if (!module) return [];
            return (module.questions ?? []).map((question) => {
                const baseConfig = (question.config ?? {}) as Record<string, unknown>;
                const config = enrichQuestionConfigWithTagMeta(question.type, baseConfig, configuredTagById);
                return {
                id: question.id,
                type: question.type,
                text: question.text,
                required: question.required,
                config,
                rules: question.rules ?? [],
                scoring: question.scoring ?? {},
                chains: normalizeQuestionChains(question.chains),
                appliesToMarketChain: true,
                options: Array.isArray(config.options) ? (config.options as string[]) : undefined,
                moduleId: module.id,
                moduleName: module.name,
                };
              }).filter((question) => isQuestionApplicableToMarketChain(question.chains, marketChain));
          });
          return { fragebogenId: fb.id, fragebogenName: fb.name, questions };
        };

        const payload = getPayload();
        if (!payload) return null;
        return {
          section: selected.section,
          campaignId: selected.campaignId,
          campaignName: selected.campaignName,
          fragebogenId: payload.fragebogenId,
          fragebogenName: payload.fragebogenName,
          questions: payload.questions,
        };
      })
      .filter(Boolean)
      .sort((left, right) => {
        const a = left as NonNullable<typeof left>;
        const b = right as NonNullable<typeof right>;
        const ai = SECTION_ORDER.indexOf(a.section as (typeof SECTION_ORDER)[number]);
        const bi = SECTION_ORDER.indexOf(b.section as (typeof SECTION_ORDER)[number]);
        if (ai !== bi) return ai - bi;
        return campaignIds.indexOf(a.campaignId) - campaignIds.indexOf(b.campaignId);
      });

    res.status(200).json({
      market: {
        id: market.id,
        name: market.name,
        address: market.address,
        postalCode: market.postalCode,
        city: market.city,
      },
      sections,
    });
  } catch (err) {
    next(err);
  }
});

adminMarketsRouter.use(requireAuth(["admin", "kunde"]));
adminMarketsRouter.use(requireKundeAdminPermission);
adminMarketsRouter.use((req, res, next) => {
  if (req.method.toUpperCase() === "GET") {
    next();
    return;
  }
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "admin_markets_action_completed", {
      req,
      action: "admin_markets_mutation",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

adminMarketsRouter.post("/import/preview", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = importMarketsSchema.safeParse(req.body);
    if (!parsed.success || (parsed.data.importType !== "update" && parsed.data.importType !== "kuehler_snapshot")) {
      res.status(400).json({ error: "Für diese Vorschau muss eine Marktlisten-Aktualisierung ausgewählt sein." });
      return;
    }
    const payload = parsed.data;
    if (payload.importType === "update") {
      if (!isValidColLetter(payload.mapping.flexNumber ?? "") || getMappedUpdateFields(payload.mapping).length === 0) {
        res.status(400).json({ error: "Flex-Nummer und mindestens ein Aktualisierungsfeld müssen gemappt sein." });
        return;
      }
      const existingMarkets = await db.select().from(markets).where(eq(markets.isDeleted, false));
      res.status(200).json({ preview: buildGmUpdateSnapshotPreview(payload, existingMarkets) });
      return;
    }
    if (!isValidColLetter(payload.mapping.kuehlerStammnr ?? "") && !isValidColLetter(payload.mapping.flexNumber ?? "")) {
      res.status(400).json({ error: "Für die Kühler-Marktliste muss Stammnr oder Flex-Nummer gemappt sein." });
      return;
    }
    const [existingMarkets, existingUnits] = await Promise.all([
      db.select().from(markets).where(eq(markets.isDeleted, false)),
      db.select().from(marketKuehlerUnits).where(eq(marketKuehlerUnits.isDeleted, false)),
    ]);
    res.status(200).json({ preview: buildKuehlerSnapshotPreview(payload, existingMarkets, existingUnits) });
  } catch (error) {
    next(error);
  }
});

adminMarketsRouter.post("/import", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  const activeUpdateRowContextRef: {
    current: {
      row: number;
      flexNumber: string;
      marketName: string;
      identityValues: Partial<Record<"standardMarketNumber" | "cokeMasterNumber", string | null>>;
    } | null;
  } = { current: null };
  try {
    const parsed = importMarketsSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "market_import_invalid_payload", {
        req,
        action: "market_import",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Invalid import payload." });
      return;
    }

    const payload = parsed.data;
    const importType: ImportDatasetType = payload.importType ?? "universum";
    const allowMissingCokeMasterNumber =
      importType === "universum" && Boolean(payload.allowMissingCokeMasterNumber);
    if (importType === "universum" && !isValidColLetter(payload.mapping.flexNumber ?? "")) {
      res.status(400).json({ error: "Für Universumsmärkte muss 'Flex-Nummer' gemappt sein." });
      return;
    }
    if (importType === "kuehler" && !isValidColLetter(payload.mapping.kuehlerStammnr ?? "") && !isValidColLetter(payload.mapping.flexNumber ?? "")) {
      res.status(400).json({ error: "Für Kühlermärkte muss 'Stammnr' oder 'Flex-Nummer' gemappt sein." });
      return;
    }
    if (importType === "kuehler_update") {
      if (!payload.kuehlerUpdateIdentifier) {
        res.status(400).json({ error: "Bitte auswählen, über welche Kühlernummer die Zeilen erkannt werden." });
        return;
      }
      if (!isValidColLetter(payload.mapping.kuehlerMatchValue ?? "")) {
        res.status(400).json({ error: "Für Kühler-Updates muss die Identifikationsspalte gemappt sein." });
        return;
      }
      if (getMappedKuehlerUpdateFields(payload.mapping).length === 0) {
        res.status(400).json({ error: "Bitte mindestens ein Kühlerfeld zum Aktualisieren mappen." });
        return;
      }
    }
    if (importType === "update") {
      if (!isValidColLetter(payload.mapping.flexNumber ?? "")) {
        res.status(400).json({ error: "Für Markt-Updates muss 'Flex-Nummer' gemappt sein." });
        return;
      }
      if (getMappedUpdateFields(payload.mapping).length === 0) {
        res.status(400).json({ error: "Bitte mindestens ein Feld zum Aktualisieren mappen." });
        return;
      }
      if (!payload.confirmSnapshot || !payload.snapshotToken) {
        res.status(428).json({ error: "Bitte zuerst die Vorschau prüfen und die Änderungen ausdrücklich bestätigen." });
        return;
      }
    }
    if (importType === "kuehler_snapshot") {
      if (!isValidColLetter(payload.mapping.kuehlerStammnr ?? "") && !isValidColLetter(payload.mapping.flexNumber ?? "")) {
        res.status(400).json({ error: "Für die Kühler-Marktliste muss Stammnr oder Flex-Nummer gemappt sein." });
        return;
      }
      if (!payload.confirmSnapshot || !payload.snapshotToken) {
        res.status(428).json({ error: "Bitte zuerst die Kühler-Vorschau prüfen und ausdrücklich bestätigen." });
        return;
      }
    }
    const rowsAsStrings = payload.rows.map((row) => row.map((cell) => (cell == null ? "" : String(cell))));
    const summary: {
      fileName: string;
      sheetName: string;
      importType: ImportDatasetType;
      kuehlerUpdateIdentifier: KuehlerUpdateIdentifier | null;
      totalParsedRows: number;
      created: number;
      updated: number;
      existingMatchesSkipped: number;
      duplicateInputRowsSkipped: number;
      deactivated: number;
      reactivated: number;
      sharedLeftActive: number;
      skipped: number;
      unchanged: number;
      duplicateInputRowsMerged: number;
      kuehlerUnitsCreated: number;
      kuehlerUnitsUpdated: number;
      kuehlerUnitsSkipped: number;
      matchedBy: Record<"standardMarketNumber" | "cokeMasterNumber" | "flexNumber" | "namePLZ", number>;
      skippedReasons: Array<{
        row: number;
        reason: string;
        sample: string;
        draft?: MarketDraft;
        missingFields?: string[];
        missingFieldKeys?: ImportFieldKey[];
        fetchedFields?: Array<{ label: string; value: string }>;
      }>;
      deactivatedMarkets: Array<{ id: string; flexNumber: string; name: string }>;
      createdMarkets: Array<{ row: number; flexNumber: string; name: string }>;
    } = {
      fileName: payload.fileName,
      sheetName: payload.sheetName,
      importType,
      kuehlerUpdateIdentifier: payload.kuehlerUpdateIdentifier ?? null,
      totalParsedRows: Math.max(rowsAsStrings.length - 1, 0),
      created: 0,
      updated: 0,
      existingMatchesSkipped: 0,
      duplicateInputRowsSkipped: 0,
      deactivated: 0,
      reactivated: 0,
      sharedLeftActive: 0,
      skipped: 0,
      unchanged: 0,
      duplicateInputRowsMerged: 0,
      kuehlerUnitsCreated: 0,
      kuehlerUnitsUpdated: 0,
      kuehlerUnitsSkipped: 0,
      matchedBy: { standardMarketNumber: 0, cokeMasterNumber: 0, flexNumber: 0, namePLZ: 0 },
      skippedReasons: [],
      deactivatedMarkets: [],
      createdMarkets: [],
    };

    const importedAt = new Date();
    const dataRows = rowsAsStrings.slice(1);
    logAction("info", "market_import_started", {
      req,
      action: "market_import",
      result: "started",
      startedAtNs,
      details: {
        importType,
        fileName: payload.fileName,
        sheetName: payload.sheetName,
        totalRows: dataRows.length,
      },
    });

    if (importType === "update") {
      const mappedUpdateFields = getMappedUpdateFields(payload.mapping);
      const bulkUniverseMarketOnly =
        mappedUpdateFields.length === 1 && mappedUpdateFields[0] === "universeMarket";
      await db.transaction(async (tx) => {
        await tx.execute(sql`set local lock_timeout = '5s'`);
        await tx.execute(sql`set local statement_timeout = '90s'`);
        const importLock = await tx.execute<{ locked: boolean }>(
          sql`select pg_try_advisory_xact_lock(47110333) as locked`,
        );
        if (!importLock[0]?.locked) {
          throw new Error("IMPORT_IN_PROGRESS");
        }

        const existingMarkets = await tx.select().from(markets).where(eq(markets.isDeleted, false));
        const snapshotPreview = buildGmUpdateSnapshotPreview(payload, existingMarkets);
        if (!snapshotPreview.canApply) {
          throw new Error(`SNAPSHOT_INVALID:${snapshotPreview.issues[0]?.reason ?? "Ungültige Excel-Daten"}`);
        }
        if (snapshotPreview.snapshotToken !== payload.snapshotToken) {
          throw new Error("SNAPSHOT_STALE");
        }
        summary.sharedLeftActive = snapshotPreview.sharedLeftActive;
        summary.deactivatedMarkets = snapshotPreview.deactivatedMarkets;
        summary.createdMarkets = snapshotPreview.createdMarkets;
        summary.reactivated = snapshotPreview.wouldReactivate;
        const byFlex = new Map<string, Array<typeof markets.$inferSelect>>();
        const updateIdentityFields = [
          {
            key: "standardMarketNumber" as const,
            label: "Standardmarkt Nr.",
          },
          {
            key: "cokeMasterNumber" as const,
            label: "Stammnr. von Coke",
          },
        ];
        const identityOwners = {
          standardMarketNumber: new Map<string, typeof markets.$inferSelect>(),
          cokeMasterNumber: new Map<string, typeof markets.$inferSelect>(),
        };
        for (const market of existingMarkets) {
          const key = normStr(normalizeIdentity(market.flexNumber) ?? "");
          if (key) {
            const bucket = byFlex.get(key) ?? [];
            bucket.push(market);
            byFlex.set(key, bucket);
          }
          for (const identityField of updateIdentityFields) {
            const identityValue = normalizeIdentity(market[identityField.key]);
            if (identityValue) identityOwners[identityField.key].set(identityValue, market);
          }
        }

        const universeMarketUpdatesByMarketId = new Map<string, boolean>();
        const pendingCreates: Array<typeof markets.$inferInsert> = [];
        let recognizedUniverseMarketTrueRows = 0;
        let recognizedUniverseMarketFalseRows = 0;

        for (let i = 0; i < dataRows.length; i += 1) {
          if (i > 0 && i % 500 === 0) {
            logger.debug("market_update_import_progress", {
              action: "market_import",
              result: "progress",
              importedRows: i,
              totalRows: dataRows.length,
              requestId: (req as { requestId?: string }).requestId ?? null,
              appUserId: req.authUser?.appUserId ?? null,
              role: req.authUser?.role ?? null,
            });
          }

          const row = dataRows[i];
          if (!row) continue;
          const rowNum = i + 2;
          if (row.every((cell) => !cell || !cell.trim())) {
            summary.skipped += 1;
            continue;
          }

          const draft = mapRowToDraft(row, payload.mapping, importType);
          const invalidBoolean = findInvalidBooleanUpdateValue(row, payload.mapping, mappedUpdateFields);
          if (invalidBoolean) {
            throw new Error(`SNAPSHOT_INVALID:Zeile ${rowNum}: Ungültiger Ja/Nein-Wert für ${invalidBoolean.key}: ${invalidBoolean.raw}`);
          }
          const flexIdentity = normalizeIdentity(draft.flexNumber);
          const flexKey = normStr(flexIdentity ?? "");
          if (!flexKey) {
            throw new Error(`SNAPSHOT_INVALID:Zeile ${rowNum}: Flex-Nummer fehlt oder ist leer.`);
          }

          const matches = byFlex.get(flexKey) ?? [];
          if (matches.length > 1) {
            throw new Error(`SNAPSHOT_INVALID:Zeile ${rowNum}: Flex-Nummer ${flexIdentity} ist nicht eindeutig.`);
          }

          const normalizedDraftRegion =
            draft.region != null ? normalizeRegionValue(String(draft.region)) : null;
          if (normalizedDraftRegion && !normalizedDraftRegion.ok) {
            throw new Error(`SNAPSHOT_INVALID:Zeile ${rowNum}: Region konnte nicht normalisiert werden (${normalizedDraftRegion.raw || "leer"}).`);
          }

          if (matches.length === 0) {
            pendingCreates.push({
              standardMarketNumber: normalizeIdentity(draft.standardMarketNumber),
              cokeMasterNumber: normalizeIdentity(draft.cokeMasterNumber),
              flexNumber: flexIdentity,
              name: String(draft.name ?? ""),
              dbName: normalizeOptionalText(draft.dbName) ?? "",
              address: String(draft.address ?? ""),
              postalCode: String(draft.postalCode ?? ""),
              city: String(draft.city ?? ""),
              region: normalizedDraftRegion?.ok ? normalizedDraftRegion.canonical : "",
              emEh: normalizeOptionalText(draft.emEh) ?? "",
              employee: normalizeOptionalText(draft.employee) ?? "",
              currentGmName: normalizeOptionalText(draft.currentGmName) ?? "",
              universeMarket: draft.universeMarket == null ? true : Boolean(draft.universeMarket),
              marketType: "universum",
              isActive: draft.isActive == null ? true : Boolean(draft.isActive),
              visitFrequencyPerYear: Number(draft.visitFrequencyPerYear ?? 0),
              infoFlag: Boolean(draft.infoFlag ?? false),
              infoNote: normalizeOptionalText(draft.infoNote) ?? "",
              importSourceFileName: payload.fileName,
              importedAt,
              isDeleted: false,
            });
            continue;
          }

          const patch = buildUpdateOnlyMarketPatch(draft, mappedUpdateFields, normalizedDraftRegion);
          if (draft.isActive == null && matches[0]?.isActive === false) patch.isActive = true;
          if (Object.keys(patch).length === 0) {
            summary.unchanged += 1;
            continue;
          }
          if (bulkUniverseMarketOnly && patch.universeMarket != null) {
            if (Boolean(patch.universeMarket)) recognizedUniverseMarketTrueRows += 1;
            else recognizedUniverseMarketFalseRows += 1;
          }

          const matched = matches[0];
          if (!matched) {
            throw new Error(`SNAPSHOT_INVALID:Zeile ${rowNum}: Markt konnte nicht eindeutig erkannt werden.`);
          }

          const identityConflict = updateIdentityFields
            .map((identityField) => {
              if (!Object.prototype.hasOwnProperty.call(patch, identityField.key)) return null;
              const proposedValue = normalizeIdentity(patch[identityField.key]);
              if (!proposedValue) return null;
              const owner = identityOwners[identityField.key].get(proposedValue);
              if (!owner || owner.id === matched.id) return null;
              return { ...identityField, value: proposedValue, owner };
            })
            .find((conflict) => conflict != null);

          if (identityConflict) {
            throw new Error(`SNAPSHOT_INVALID:Zeile ${rowNum}: ${identityConflict.label} ${identityConflict.value} gehört bereits zu „${identityConflict.owner.name}“. Import nicht angewendet.`);
          }

          if (!marketPatchHasChanges(matched, patch)) {
            summary.unchanged += 1;
            continue;
          }
          if (bulkUniverseMarketOnly && patch.universeMarket != null) {
            const value = Boolean(patch.universeMarket);
            universeMarketUpdatesByMarketId.set(matched.id, value);
            continue;
          }
          activeUpdateRowContextRef.current = {
            row: rowNum,
            flexNumber: String(draft.flexNumber ?? flexIdentity),
            marketName: matched.name,
            identityValues: Object.fromEntries(
              updateIdentityFields
                .filter((identityField) => Object.prototype.hasOwnProperty.call(patch, identityField.key))
                .map((identityField) => [identityField.key, normalizeIdentity(patch[identityField.key])]),
            ),
          };
          const [updated] = await tx
            .update(markets)
            .set({
              ...patch,
              importSourceFileName: payload.fileName,
              importedAt,
              updatedAt: new Date(),
            })
            .where(eq(markets.id, matched.id))
            .returning();
          activeUpdateRowContextRef.current = null;

          if (updated) {
            for (const identityField of updateIdentityFields) {
              if (!Object.prototype.hasOwnProperty.call(patch, identityField.key)) continue;
              const previousValue = normalizeIdentity(matched[identityField.key]);
              const nextValue = normalizeIdentity(updated[identityField.key]);
              if (
                previousValue &&
                identityOwners[identityField.key].get(previousValue)?.id === matched.id &&
                previousValue !== nextValue
              ) {
                identityOwners[identityField.key].delete(previousValue);
              }
              if (nextValue) identityOwners[identityField.key].set(nextValue, updated);
            }
            matches[0] = updated;
            summary.updated += 1;
            summary.matchedBy.flexNumber += 1;
          }
        }

        for (let offset = 0; offset < pendingCreates.length; offset += 250) {
          const batch = pendingCreates.slice(offset, offset + 250);
          if (batch.length === 0) continue;
          await tx.insert(markets).values(batch);
          summary.created += batch.length;
        }

        if (bulkUniverseMarketOnly) {
          const now = new Date();
          const trueIds: string[] = [];
          const falseIds: string[] = [];
          for (const [marketId, value] of universeMarketUpdatesByMarketId) {
            if (value) trueIds.push(marketId);
            else falseIds.push(marketId);
          }
          await updateMarketIdsInChunks(tx, trueIds, {
            universeMarket: true,
            isActive: true,
            importSourceFileName: payload.fileName,
            importedAt,
            updatedAt: now,
          });
          await updateMarketIdsInChunks(tx, falseIds, {
            universeMarket: false,
            isActive: true,
            importSourceFileName: payload.fileName,
            importedAt,
            updatedAt: now,
          });
          const updatedCount = trueIds.length + falseIds.length;
          summary.updated += updatedCount;
          summary.matchedBy.flexNumber += updatedCount;
          logAction("info", "market_update_import_universe_market_counts", {
            req,
            action: "market_import",
            result: "success",
            startedAtNs,
            details: {
              trueRows: recognizedUniverseMarketTrueRows,
              falseRows: recognizedUniverseMarketFalseRows,
              trueUpdates: trueIds.length,
              falseUpdates: falseIds.length,
            },
          });
        }

        const snapshotPlan = planGmMarketSnapshot(existingMarkets, dataRows.flatMap((row, index) =>
          row && !row.every((cell) => !cell.trim())
            ? [{ row: index + 2, flexNumber: getMappedRawCell(row, payload.mapping, "flexNumber") }]
            : [],
        ), { deactivateSharedMarkets: true });
        const deactivateIds = snapshotPlan.toDeactivate.map((market) => market.id);
        await updateMarketIdsInChunks(tx, deactivateIds, {
          isActive: false,
          importSourceFileName: payload.fileName,
          importedAt,
          updatedAt: new Date(),
        });
        summary.deactivated = deactivateIds.length;
      });

      const fresh = await db
        .select()
        .from(markets)
        .where(eq(markets.isDeleted, false))
        .orderBy(desc(markets.createdAt));
      logAction("info", "market_import_completed", {
        req,
        action: "market_import",
        result: "success",
        statusCode: 200,
        requestClass: "success",
        startedAtNs,
        details: {
          importType,
          created: summary.created,
          updated: summary.updated,
          deactivated: summary.deactivated,
          reactivated: summary.reactivated,
          sharedLeftActive: summary.sharedLeftActive,
          skipped: summary.skipped,
          unchanged: summary.unchanged,
        },
      });
      res.status(200).json({ markets: fresh.map(mapMarketRow), summary });
      return;
    }

    if (importType === "kuehler_update") {
      const identifier = payload.kuehlerUpdateIdentifier as KuehlerUpdateIdentifier;
      const identifierLabel = getKuehlerUpdateIdentifierLabel(identifier);
      const mappedUpdateFields = getMappedKuehlerUpdateFields(payload.mapping);

      await db.transaction(async (tx) => {
        await tx.execute(sql`set local lock_timeout = '5s'`);
        await tx.execute(sql`set local statement_timeout = '90s'`);
        const importLock = await tx.execute<{ locked: boolean }>(
          sql`select pg_try_advisory_xact_lock(47110333) as locked`,
        );
        if (!importLock[0]?.locked) {
          throw new Error("IMPORT_IN_PROGRESS");
        }

        const existingUnits = await tx
          .select()
          .from(marketKuehlerUnits)
          .where(eq(marketKuehlerUnits.isDeleted, false));
        const unitsByIdentifier = new Map<string, Array<typeof marketKuehlerUnits.$inferSelect>>();
        const internalIdOwners = new Map<string, typeof marketKuehlerUnits.$inferSelect>();
        const technicalIdentNoOwners = new Map<string, typeof marketKuehlerUnits.$inferSelect>();

        for (const unit of existingUnits) {
          const identifierKey = normStr(getKuehlerUpdateIdentifierValue(unit, identifier) ?? "");
          if (identifierKey) {
            const bucket = unitsByIdentifier.get(identifierKey) ?? [];
            bucket.push(unit);
            unitsByIdentifier.set(identifierKey, bucket);
          }
          const internalIdKey = normStr(unit.kuehlerInternalId ?? "");
          if (internalIdKey) internalIdOwners.set(internalIdKey, unit);
          const technicalIdentNoKey = normStr(unit.kuehlerTechnicalIdentNo ?? "");
          if (technicalIdentNoKey) technicalIdentNoOwners.set(technicalIdentNoKey, unit);
        }

        const updatedUnitIds = new Set<string>();
        for (let i = 0; i < dataRows.length; i += 1) {
          const row = dataRows[i];
          if (!row) continue;
          const rowNum = i + 2;
          if (row.every((cell) => !cell || !cell.trim())) {
            summary.skipped += 1;
            summary.kuehlerUnitsSkipped += 1;
            continue;
          }

          const draft = mapRowToDraft(row, payload.mapping, importType);
          const sampleText = String(draft.kuehlerMatchValue ?? row.find((cell) => cell?.trim()) ?? "");
          const matchValue = normalizeIdentity(draft.kuehlerMatchValue);
          const matchKey = normStr(matchValue ?? "");
          if (!matchKey) {
            summary.skipped += 1;
            summary.kuehlerUnitsSkipped += 1;
            if (summary.skippedReasons.length < 50) {
              summary.skippedReasons.push({
                row: rowNum,
                reason: `${identifierLabel} fehlt oder ist leer`,
                sample: sampleText,
                draft,
              });
            }
            continue;
          }

          const candidates = unitsByIdentifier.get(matchKey) ?? [];
          if (candidates.length === 0) {
            summary.skipped += 1;
            summary.kuehlerUnitsSkipped += 1;
            if (summary.skippedReasons.length < 50) {
              summary.skippedReasons.push({
                row: rowNum,
                reason: `Kein aktiver Kühler mit ${identifierLabel} ${matchValue} gefunden`,
                sample: sampleText,
                draft,
              });
            }
            continue;
          }
          if (candidates.length > 1) {
            summary.skipped += 1;
            summary.kuehlerUnitsSkipped += 1;
            if (summary.skippedReasons.length < 50) {
              summary.skippedReasons.push({
                row: rowNum,
                reason: `${identifierLabel} ${matchValue} ist nicht eindeutig (${candidates.length} Kühler)`,
                sample: sampleText,
                draft,
              });
            }
            continue;
          }

          const existingUnit = candidates[0];
          if (!existingUnit) continue;
          if (updatedUnitIds.has(existingUnit.id)) {
            summary.skipped += 1;
            summary.kuehlerUnitsSkipped += 1;
            if (summary.skippedReasons.length < 50) {
              summary.skippedReasons.push({
                row: rowNum,
                reason: `Doppelter Kühler in der Datei (${identifierLabel} ${matchValue})`,
                sample: sampleText,
                draft,
              });
            }
            continue;
          }

          const unitPatch = buildKuehlerUpdatePatch(draft, mappedUpdateFields);
          const nextInternalId = Object.prototype.hasOwnProperty.call(unitPatch, "kuehlerInternalId")
            ? normalizeIdentity(unitPatch.kuehlerInternalId)
            : null;
          const nextTechnicalIdentNo = Object.prototype.hasOwnProperty.call(unitPatch, "kuehlerTechnicalIdentNo")
            ? normalizeIdentity(unitPatch.kuehlerTechnicalIdentNo)
            : null;
          const conflictingInternalIdOwner = nextInternalId
            ? internalIdOwners.get(normStr(nextInternalId))
            : undefined;
          const conflictingTechnicalIdentNoOwner = nextTechnicalIdentNo
            ? technicalIdentNoOwners.get(normStr(nextTechnicalIdentNo))
            : undefined;
          if (
            (conflictingInternalIdOwner && conflictingInternalIdOwner.id !== existingUnit.id) ||
            (conflictingTechnicalIdentNoOwner && conflictingTechnicalIdentNoOwner.id !== existingUnit.id)
          ) {
            const conflictLabel = conflictingInternalIdOwner ? "internal_id" : "Tech. Ident. No.";
            const conflictValue = conflictingInternalIdOwner ? nextInternalId : nextTechnicalIdentNo;
            summary.skipped += 1;
            summary.kuehlerUnitsSkipped += 1;
            if (summary.skippedReasons.length < 50) {
              summary.skippedReasons.push({
                row: rowNum,
                reason: `${conflictLabel} ${conflictValue} gehört bereits zu einem anderen Kühler`,
                sample: sampleText,
                draft,
              });
            }
            continue;
          }

          if (!kuehlerUnitPatchHasChanges(existingUnit, unitPatch)) {
            summary.unchanged += 1;
            continue;
          }

          const [updatedUnit] = await tx
            .update(marketKuehlerUnits)
            .set({
              ...unitPatch,
              updatedAt: new Date(),
            })
            .where(and(eq(marketKuehlerUnits.id, existingUnit.id), eq(marketKuehlerUnits.isDeleted, false)))
            .returning();
          if (!updatedUnit) {
            summary.skipped += 1;
            summary.kuehlerUnitsSkipped += 1;
            continue;
          }

          updatedUnitIds.add(existingUnit.id);
          summary.updated += 1;
          summary.kuehlerUnitsUpdated += 1;

          const previousInternalIdKey = normStr(existingUnit.kuehlerInternalId ?? "");
          const updatedInternalIdKey = normStr(updatedUnit.kuehlerInternalId ?? "");
          if (previousInternalIdKey && previousInternalIdKey !== updatedInternalIdKey) {
            internalIdOwners.delete(previousInternalIdKey);
          }
          if (updatedInternalIdKey) internalIdOwners.set(updatedInternalIdKey, updatedUnit);

          const previousTechnicalIdentNoKey = normStr(existingUnit.kuehlerTechnicalIdentNo ?? "");
          const updatedTechnicalIdentNoKey = normStr(updatedUnit.kuehlerTechnicalIdentNo ?? "");
          if (previousTechnicalIdentNoKey && previousTechnicalIdentNoKey !== updatedTechnicalIdentNoKey) {
            technicalIdentNoOwners.delete(previousTechnicalIdentNoKey);
          }
          if (updatedTechnicalIdentNoKey) {
            technicalIdentNoOwners.set(updatedTechnicalIdentNoKey, updatedUnit);
          }
        }
      });

      const fresh = await db
        .select()
        .from(markets)
        .where(eq(markets.isDeleted, false))
        .orderBy(desc(markets.createdAt));
      logAction("info", "market_kuehler_update_import_completed", {
        req,
        action: "market_import",
        result: "success",
        statusCode: 200,
        requestClass: "success",
        startedAtNs,
        details: {
          importType,
          identifier,
          updated: summary.updated,
          unchanged: summary.unchanged,
          skipped: summary.skipped,
        },
      });
      res.status(200).json({ markets: fresh.map(mapMarketRow), summary });
      return;
    }

    if (importType === "kuehler" || importType === "kuehler_snapshot") {
      await db.transaction(async (tx) => {
        await tx.execute(sql`set local lock_timeout = '5s'`);
        await tx.execute(sql`set local statement_timeout = '90s'`);
        const importLock = await tx.execute<{ locked: boolean }>(
          sql`select pg_try_advisory_xact_lock(47110333) as locked`,
        );
        if (!importLock[0]?.locked) {
          throw new Error("IMPORT_IN_PROGRESS");
        }

        const existingMarkets = await tx.select().from(markets).where(eq(markets.isDeleted, false));
        const existingUnits = await tx
          .select()
          .from(marketKuehlerUnits)
          .where(eq(marketKuehlerUnits.isDeleted, false));
        const isKuehlerSnapshot = importType === "kuehler_snapshot";
        const snapshotPresentMarketIds = new Set<string>();
        if (isKuehlerSnapshot) {
          const snapshotPreview = buildKuehlerSnapshotPreview(payload, existingMarkets, existingUnits);
          if (!snapshotPreview.canApply) {
            throw new Error(`SNAPSHOT_INVALID:${snapshotPreview.issues[0]?.reason ?? "Ungültige Kühler-Daten"}`);
          }
          if (snapshotPreview.snapshotToken !== payload.snapshotToken) throw new Error("SNAPSHOT_STALE");
          summary.deactivatedMarkets = snapshotPreview.deactivatedMarkets;
          summary.createdMarkets = snapshotPreview.createdMarkets;
          summary.reactivated = snapshotPreview.wouldReactivate;
        }

        const marketByCanonicalStammnr = new Map<string, typeof markets.$inferSelect>();
        const marketsByCanonicalFlex = new Map<string, Array<typeof markets.$inferSelect>>();
        const registerMarket = (market: typeof markets.$inferSelect) => {
          const keyCandidates = [
            normalizeStammnrForMatch(market.kuehlerStammnr),
            normalizeStammnrForMatch(market.cokeMasterNumber),
          ];
          for (const keyCandidate of keyCandidates) {
            const canonical = normStr(keyCandidate ?? "");
            if (!canonical) continue;
            const current = marketByCanonicalStammnr.get(canonical);
            marketByCanonicalStammnr.set(canonical, choosePreferredMarketByStammnr(current, market));
          }
          const flexKey = normStr(normalizeIdentity(market.flexNumber) ?? "");
          if (flexKey) {
            const bucket = marketsByCanonicalFlex.get(flexKey) ?? [];
            if (!bucket.some((entry) => entry.id === market.id)) {
              bucket.push(market);
              marketsByCanonicalFlex.set(flexKey, bucket);
            }
          }
        };
        for (const market of existingMarkets) {
          registerMarket(market);
        }

        const unitByCanonicalInternalId = new Map<string, typeof marketKuehlerUnits.$inferSelect>();
        const unitByCanonicalTechnicalIdentNo = new Map<string, typeof marketKuehlerUnits.$inferSelect>();
        for (const unit of existingUnits) {
          const internalIdKey = normStr(unit.kuehlerInternalId ?? "");
          if (internalIdKey) unitByCanonicalInternalId.set(internalIdKey, unit);
          const technicalIdentNoKey = normStr(unit.kuehlerTechnicalIdentNo ?? "");
          if (technicalIdentNoKey) unitByCanonicalTechnicalIdentNo.set(technicalIdentNoKey, unit);
        }

        const seedByCanonicalStammnr = new Map<
          string,
          {
            rowNum: number;
            sampleText: string;
            draft: MarketDraft;
            normalizedDraftRegion: RegionNormalizationResult | null;
          }
        >();
        for (let i = 0; i < dataRows.length; i += 1) {
          const row = dataRows[i];
          if (!row) continue;
          if (row.every((cell) => !cell || !cell.trim())) continue;
          const draft = mapRowToDraft(row, payload.mapping, importType);
          const stammnr = normalizeStammnrForMatch(draft.kuehlerStammnr);
          const canonicalStammnr = normStr(stammnr ?? "");
          if (!canonicalStammnr || seedByCanonicalStammnr.has(canonicalStammnr)) continue;
          const sampleText = String(draft.name ?? draft.city ?? row.find((cell) => cell?.trim()) ?? "");
          const normalizedDraftRegion =
            draft.region != null ? normalizeRegionValue(String(draft.region)) : null;
          seedByCanonicalStammnr.set(canonicalStammnr, {
            rowNum: i + 2,
            sampleText,
            draft,
            normalizedDraftRegion,
          });
        }

        for (let i = 0; i < dataRows.length; i += 1) {
          const row = dataRows[i];
          if (!row) continue;
          const rowNum = i + 2;
          if (row.every((cell) => !cell || !cell.trim())) {
            if (!isKuehlerSnapshot) {
              summary.skipped += 1;
              summary.kuehlerUnitsSkipped += 1;
            }
            continue;
          }

          const draft = mapRowToDraft(row, payload.mapping, importType);
          const sampleText = String(draft.name ?? draft.city ?? row.find((cell) => cell?.trim()) ?? "");
          const stammnr = normalizeStammnrForMatch(draft.kuehlerStammnr);
          const canonicalStammnr = normStr(stammnr ?? "");
          const flexIdentity = normalizeIdentity(draft.flexNumber);
          const canonicalFlex = normStr(flexIdentity ?? "");
          if (!canonicalStammnr && !canonicalFlex) {
            summary.skipped += 1;
            summary.kuehlerUnitsSkipped += 1;
            if (summary.skippedReasons.length < 50) {
              summary.skippedReasons.push({
                row: rowNum,
                reason: "Stammnr und Flex-Nummer fehlen oder sind nach Normalisierung leer",
                sample: sampleText,
                draft,
              });
            }
            continue;
          }

          const normalizedDraftRegion =
            draft.region != null ? normalizeRegionValue(String(draft.region)) : null;
          if (normalizedDraftRegion && !normalizedDraftRegion.ok) {
            summary.skipped += 1;
            summary.kuehlerUnitsSkipped += 1;
            if (summary.skippedReasons.length < 50) {
              summary.skippedReasons.push({
                row: rowNum,
                reason: `Region konnte nicht normalisiert werden (${normalizedDraftRegion.raw || "leer"})`,
                sample: sampleText,
                draft,
              });
            }
            continue;
          }

          let resolvedMarket = canonicalStammnr ? marketByCanonicalStammnr.get(canonicalStammnr) : undefined;
          let resolvedByFlexFallback = false;
          if (!resolvedMarket && canonicalFlex) {
            const flexMatches = marketsByCanonicalFlex.get(canonicalFlex) ?? [];
            if (flexMatches.length === 1) {
              resolvedMarket = flexMatches[0];
              resolvedByFlexFallback = true;
              summary.matchedBy.flexNumber += 1;
            } else if (flexMatches.length > 1) {
              summary.skipped += 1;
              summary.kuehlerUnitsSkipped += 1;
              if (summary.skippedReasons.length < 50) {
                summary.skippedReasons.push({
                  row: rowNum,
                  reason: `Flex-Nummer ${String(draft.flexNumber ?? flexIdentity)} ist nicht eindeutig`,
                  sample: sampleText,
                  draft,
                });
              }
              continue;
            }
          }
          if (!resolvedMarket) {
            if (!canonicalStammnr) {
              summary.skipped += 1;
              summary.kuehlerUnitsSkipped += 1;
              if (summary.skippedReasons.length < 50) {
                summary.skippedReasons.push({
                  row: rowNum,
                  reason: `Kein bestehender Markt mit Flex-Nummer ${String(draft.flexNumber ?? flexIdentity)} gefunden`,
                  sample: sampleText,
                  draft,
                });
              }
              continue;
            }
            const seed = seedByCanonicalStammnr.get(canonicalStammnr);
            const seedDraft = seed?.draft ?? draft;
            const seedSampleText = seed?.sampleText ?? sampleText;
            const seedRowNum = seed?.rowNum ?? rowNum;
            const seedNormalizedRegion = seed?.normalizedDraftRegion ?? normalizedDraftRegion;
            if (!seedNormalizedRegion || !seedNormalizedRegion.ok) {
              summary.skipped += 1;
              summary.kuehlerUnitsSkipped += 1;
              if (summary.skippedReasons.length < 50) {
                summary.skippedReasons.push({
                  row: seedRowNum,
                  reason: `Neuer Markt mit ungültiger Seed-Region (${seedNormalizedRegion?.raw || "leer"})`,
                  sample: seedSampleText,
                  draft: seedDraft,
                });
              }
              continue;
            }
            const hasSeedVisibles = Boolean(
              seedDraft.name && (seedDraft.postalCode || seedDraft.city),
            );
            if (!hasSeedVisibles) {
              summary.skipped += 1;
              summary.kuehlerUnitsSkipped += 1;
              if (summary.skippedReasons.length < 50) {
                const { missingFields, missingFieldKeys, fetchedFields } = buildSkipMeta(
                  seedDraft,
                  payload.mapping,
                  importType,
                );
                summary.skippedReasons.push({
                  row: seedRowNum,
                  reason: "Neuer Markt ohne Pflichtfelder (Name, PLZ, Region)",
                  sample: seedSampleText,
                  draft: seedDraft,
                  missingFields,
                  missingFieldKeys,
                  fetchedFields,
                });
              }
              continue;
            }

            const [createdMarket] = await tx
              .insert(markets)
              .values({
                standardMarketNumber: null,
                cokeMasterNumber: stammnr,
                flexNumber: isKuehlerSnapshot ? normalizeIdentity(seedDraft.flexNumber) : null,
                name: String(seedDraft.name ?? ""),
                dbName: "",
                address: String(seedDraft.address ?? ""),
                postalCode: String(seedDraft.postalCode ?? ""),
                city: String(seedDraft.city ?? ""),
                region: seedNormalizedRegion.canonical,
                emEh: "",
                employee: normalizeOptionalText(String(seedDraft.employee ?? "")) ?? "",
                currentGmName: "",
                visitFrequencyPerYear: 0,
                infoFlag: false,
                infoNote: "",
                universeMarket: false,
                marketType: "kuehler",
                kuehlerStammnr: stammnr,
                isActive: true,
                importSourceFileName: payload.fileName,
                importedAt,
                isDeleted: false,
              })
              .returning();
            if (!createdMarket) {
              summary.skipped += 1;
              summary.kuehlerUnitsSkipped += 1;
              continue;
            }
            summary.created += 1;
            resolvedMarket = createdMarket;
            marketByCanonicalStammnr.set(canonicalStammnr, createdMarket);
            registerMarket(createdMarket);
          } else {
            const nextMarketType: MarketType =
              resolvedMarket.marketType === "universum" ? "both" : resolvedMarket.marketType;
            const nextUniverseMarket = deriveUniverseMarketFromType(nextMarketType);
            const preserveGmCokeIdentity = isKuehlerSnapshot && resolvedMarket.marketType !== "kuehler";
            const snapshotMarketPatch: Partial<typeof markets.$inferInsert> = isKuehlerSnapshot
              ? {
                  ...(draft.name != null ? { name: String(draft.name) } : {}),
                  ...(draft.address != null ? { address: String(draft.address) } : {}),
                  ...(draft.postalCode != null ? { postalCode: String(draft.postalCode) } : {}),
                  ...(draft.city != null ? { city: String(draft.city) } : {}),
                  ...(normalizedDraftRegion?.ok ? { region: normalizedDraftRegion.canonical } : {}),
                  ...(resolvedMarket.marketType === "kuehler" && draft.flexNumber != null
                    ? { flexNumber: normalizeIdentity(draft.flexNumber) }
                    : {}),
                  isActive: true,
                }
              : {};
            const shouldUpdateMarket =
              nextMarketType !== resolvedMarket.marketType ||
              nextUniverseMarket !== resolvedMarket.universeMarket ||
              (isKuehlerSnapshot && marketPatchHasChanges(resolvedMarket, snapshotMarketPatch)) ||
              Boolean(
                canonicalStammnr &&
                (
                  normalizeStammnrForMatch(resolvedMarket.kuehlerStammnr) !== stammnr ||
                  (!preserveGmCokeIdentity && normalizeStammnrForMatch(resolvedMarket.cokeMasterNumber) !== stammnr)
                ),
              );

            if (shouldUpdateMarket) {
              const [updatedMarket] = await tx
                .update(markets)
                .set({
                  ...snapshotMarketPatch,
                  marketType: nextMarketType,
                  universeMarket: nextUniverseMarket,
                  ...(canonicalStammnr ? {
                    kuehlerStammnr: stammnr,
                    ...(!preserveGmCokeIdentity ? { cokeMasterNumber: stammnr } : {}),
                  } : {}),
                  importSourceFileName: payload.fileName,
                  importedAt,
                  updatedAt: new Date(),
                })
                .where(eq(markets.id, resolvedMarket.id))
                .returning();
              if (updatedMarket) {
                summary.updated += 1;
                resolvedMarket = updatedMarket;
                if (canonicalStammnr) marketByCanonicalStammnr.set(canonicalStammnr, updatedMarket);
                registerMarket(updatedMarket);
              }
            }
            if (resolvedByFlexFallback) {
              logger.debug("market_kuehler_import_flex_fallback_match", {
                action: "market_import",
                result: "matched",
                row: rowNum,
                flexNumber: flexIdentity,
                marketId: resolvedMarket.id,
                requestId: (req as { requestId?: string }).requestId ?? null,
              });
            }
          }

          if (!resolvedMarket) {
            summary.kuehlerUnitsSkipped += 1;
            continue;
          }
          if (isKuehlerSnapshot) snapshotPresentMarketIds.add(resolvedMarket.id);

          const normalizedInternalId = normalizeIdentity(draft.kuehlerInternalId);
          const canonicalInternalId = normStr(normalizedInternalId ?? "");
          const normalizedTechnicalIdentNo = normalizeIdentity(draft.kuehlerTechnicalIdentNo);
          const canonicalTechnicalIdentNo = normStr(normalizedTechnicalIdentNo ?? "");
          const unitName = normalizeOptionalText(String(draft.name ?? "")) ?? "";
          const unitEmployee = normalizeOptionalText(String(draft.employee ?? "")) ?? "";
          const unitBd = normalizeOptionalText(String(draft.kuehlerBd ?? ""));
          const unitSerial = normalizeOptionalText(String(draft.kuehlerSerialNumber ?? ""));
          const unitModel = normalizeOptionalText(String(draft.kuehlerModel ?? ""));
          const unitCount =
            draft.kuehlerAnzahlKsAmStandort != null
              ? Number(draft.kuehlerAnzahlKsAmStandort)
              : null;

          const existingUnitByInternalId = canonicalInternalId
            ? unitByCanonicalInternalId.get(canonicalInternalId)
            : undefined;
          const existingUnitByTechnicalIdentNo = canonicalTechnicalIdentNo
            ? unitByCanonicalTechnicalIdentNo.get(canonicalTechnicalIdentNo)
            : undefined;
          if (
            existingUnitByInternalId &&
            existingUnitByTechnicalIdentNo &&
            existingUnitByInternalId.id !== existingUnitByTechnicalIdentNo.id
          ) {
            summary.skipped += 1;
            summary.kuehlerUnitsSkipped += 1;
            if (summary.skippedReasons.length < 50) {
              summary.skippedReasons.push({
                row: rowNum,
                reason: "internal_id und Tech. Ident. No. gehören zu unterschiedlichen Kühlern",
                sample: sampleText,
                draft,
              });
            }
            continue;
          }
          const existingUnit = existingUnitByInternalId ?? existingUnitByTechnicalIdentNo;
          if (existingUnit) {
              const snapshotUnitPatch = isKuehlerSnapshot
                ? buildKuehlerUpdatePatch(draft, getMappedKuehlerUpdateFields(payload.mapping))
                : null;
              if (isKuehlerSnapshot && !kuehlerSnapshotUnitNeedsUpdate(existingUnit, resolvedMarket.id, snapshotUnitPatch ?? {})) {
                summary.unchanged += 1;
                continue;
              }
              const [updatedUnit] = await tx
                .update(marketKuehlerUnits)
                .set({
                  ...(snapshotUnitPatch ?? {
                    name: unitName,
                    employee: unitEmployee,
                    kuehlerInternalId: normalizedInternalId,
                    kuehlerBd: unitBd,
                    kuehlerAnzahlKsAmStandort: unitCount,
                    kuehlerSerialNumber: unitSerial,
                    kuehlerTechnicalIdentNo: normalizedTechnicalIdentNo,
                    kuehlerModel: unitModel,
                  }),
                  marketId: resolvedMarket.id,
                  importSourceFileName: payload.fileName,
                  importedAt,
                  isDeleted: false,
                  updatedAt: new Date(),
                })
                .where(eq(marketKuehlerUnits.id, existingUnit.id))
                .returning();
              if (updatedUnit) {
                summary.kuehlerUnitsUpdated += 1;
                if (canonicalInternalId) unitByCanonicalInternalId.set(canonicalInternalId, updatedUnit);
                if (canonicalTechnicalIdentNo) {
                  unitByCanonicalTechnicalIdentNo.set(canonicalTechnicalIdentNo, updatedUnit);
                }
              } else {
                summary.kuehlerUnitsSkipped += 1;
              }
              continue;
          }

          const [createdUnit] = await tx
            .insert(marketKuehlerUnits)
            .values({
              marketId: resolvedMarket.id,
              name: unitName,
              employee: unitEmployee,
              kuehlerInternalId: normalizedInternalId,
              kuehlerBd: unitBd,
              kuehlerAnzahlKsAmStandort: unitCount,
              kuehlerSerialNumber: unitSerial,
              kuehlerTechnicalIdentNo: normalizedTechnicalIdentNo,
              kuehlerModel: unitModel,
              importSourceFileName: payload.fileName,
              importedAt,
              isDeleted: false,
            })
            .returning();
          if (createdUnit) {
            summary.kuehlerUnitsCreated += 1;
            if (canonicalInternalId) {
              unitByCanonicalInternalId.set(canonicalInternalId, createdUnit);
            }
            if (canonicalTechnicalIdentNo) {
              unitByCanonicalTechnicalIdentNo.set(canonicalTechnicalIdentNo, createdUnit);
            }
          } else {
            summary.kuehlerUnitsSkipped += 1;
          }
        }
        if (isKuehlerSnapshot) {
          if (summary.kuehlerUnitsSkipped > 0 || summary.skipped > 0) {
            throw new Error("SNAPSHOT_INVALID:Mindestens eine Kühlerzeile konnte nicht verarbeitet werden. Keine Änderung wurde gespeichert.");
          }
          const missingIds = existingMarkets
            .filter((market) => market.isActive && (market.marketType === "kuehler" || market.marketType === "both") && !snapshotPresentMarketIds.has(market.id))
            .map((market) => market.id);
          await updateMarketIdsInChunks(tx, missingIds, {
            isActive: false,
            importSourceFileName: payload.fileName,
            importedAt,
            updatedAt: new Date(),
          });
          summary.deactivated = missingIds.length;
        }
      });

      const fresh = await db
        .select()
        .from(markets)
        .where(eq(markets.isDeleted, false))
        .orderBy(desc(markets.createdAt));
      logAction("info", "market_import_completed", {
        req,
        action: "market_import",
        result: "success",
        statusCode: 200,
        requestClass: "success",
        startedAtNs,
        details: {
          importType,
          created: summary.created,
          updated: summary.updated,
          skipped: summary.skipped,
          duplicateInputRowsMerged: summary.duplicateInputRowsMerged,
          kuehlerUnitsCreated: summary.kuehlerUnitsCreated,
          kuehlerUnitsUpdated: summary.kuehlerUnitsUpdated,
          kuehlerUnitsSkipped: summary.kuehlerUnitsSkipped,
        },
      });
      res.status(200).json({ markets: fresh.map(mapMarketRow), summary });
      return;
    }

    await db.transaction(async (tx) => {
      await tx.execute(sql`set local lock_timeout = '5s'`);
      await tx.execute(sql`set local statement_timeout = '90s'`);
      const importLock = await tx.execute<{ locked: boolean }>(
        sql`select pg_try_advisory_xact_lock(47110333) as locked`,
      );
      if (!importLock[0]?.locked) {
        throw new Error("IMPORT_IN_PROGRESS");
      }
      const existing = await tx.select().from(markets).where(eq(markets.isDeleted, false));
      const byStandard = new Map<string, typeof markets.$inferSelect>();
      const byCoke = new Map<string, typeof markets.$inferSelect>();
      const byFlex = new Map<string, typeof markets.$inferSelect>();
      const ambiguousFlex = new Set<string>();
      const seenFlex = new Set<string>();
      const pendingByStandard = new Map<string, { id: string }>();
      const pendingByCoke = new Map<string, { id: string }>();
      const pendingByFlex = new Map<string, { id: string }>();
      const pendingCreates: Array<typeof markets.$inferInsert> = [];

      const registerPending = (idx: number) => {
        const row = pendingCreates[idx];
        if (!row) return;
        const std = normStr((row.standardMarketNumber as string | null | undefined) ?? "");
        const coke = normStr((row.cokeMasterNumber as string | null | undefined) ?? "");
        const flex = normStr((row.flexNumber as string | null | undefined) ?? "");
        const owner = { id: `pending:${idx}` };
        if (std) pendingByStandard.set(std, owner);
        if (coke) pendingByCoke.set(coke, owner);
        if (flex) pendingByFlex.set(flex, owner);
      };

      for (const market of existing) {
        if (market.standardMarketNumber) byStandard.set(normStr(market.standardMarketNumber), market);
        if (market.cokeMasterNumber) {
          const cokeKey = normStr(market.cokeMasterNumber);
          if (cokeKey) byCoke.set(cokeKey, market);
        }
        if (market.flexNumber) {
          const flexKey = normStr(market.flexNumber);
          if (byFlex.has(flexKey)) ambiguousFlex.add(flexKey);
          else byFlex.set(flexKey, market);
        }
      }

      for (let i = 0; i < dataRows.length; i += 1) {
        if (i > 0 && i % 500 === 0) {
          logger.debug("market_import_progress", {
            action: "market_import",
            result: "progress",
            importedRows: i,
            totalRows: dataRows.length,
            requestId: (req as { requestId?: string }).requestId ?? null,
            appUserId: req.authUser?.appUserId ?? null,
            role: req.authUser?.role ?? null,
          });
        }
        const row = dataRows[i];
        if (!row) continue;
        const rowNum = i + 2;
        if (row.every((c) => !c || !c.trim())) {
          summary.skipped += 1;
          continue;
        }

        const draft = mapRowToDraft(row, payload.mapping, importType);
        const sampleText = String(draft.name ?? draft.city ?? row.find((c) => c?.trim()) ?? "");
        const normalizedDraftRegion =
          draft.region != null ? normalizeRegionValue(String(draft.region)) : null;

        const stdIdentity = normalizeIdentity(draft.standardMarketNumber);
        const cokeIdentity = normalizeIdentity(draft.cokeMasterNumber);
        const flexIdentity = normalizeIdentity(draft.flexNumber);
        const stdKey = normStr(stdIdentity ?? "");
        const cokeKey = normStr(cokeIdentity ?? "");
        const flexKey = normStr(flexIdentity ?? "");
        if (!flexKey) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) {
            summary.skippedReasons.push({ row: rowNum, reason: "Flex-Nummer fehlt oder ist leer", sample: sampleText, draft, missingFields: ["Flex-Nummer (leer)"], missingFieldKeys: ["flexNumber"] });
          }
          continue;
        }
        if (seenFlex.has(flexKey) || pendingByFlex.has(flexKey)) {
          summary.duplicateInputRowsSkipped += 1;
          continue;
        }
        if (ambiguousFlex.has(flexKey)) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) summary.skippedReasons.push({ row: rowNum, reason: `Flex-Nummer ${flexIdentity} ist in der Datenbank nicht eindeutig`, sample: sampleText, draft });
          continue;
        }
        const classification = classifyUniversumImportRow(
          { standard: stdKey, coke: cokeKey, flex: flexKey },
          { byStandard, byCoke, byFlex },
          { byStandard: pendingByStandard, byCoke: pendingByCoke, byFlex: pendingByFlex },
        );
        if (classification.kind === "identity-conflict") {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) {
            const label = classification.field === "standardMarketNumber" ? "Standardmarkt Nr" : "Stammnr. von Coke";
            summary.skippedReasons.push({ row: rowNum, reason: `${label} ${classification.value} gehört bereits zu einem anderen Markt`, sample: sampleText, draft });
          }
          continue;
        }
        if (classification.kind === "duplicate-in-file") {
          summary.duplicateInputRowsSkipped += 1;
          continue;
        }

        if (normalizedDraftRegion && !normalizedDraftRegion.ok) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) {
            summary.skippedReasons.push({
              row: rowNum,
              reason: `Region konnte nicht normalisiert werden (${normalizedDraftRegion.raw || "leer"})`,
              sample: sampleText,
              draft,
            });
          }
          continue;
        }
        const invalidBoolean = findInvalidBooleanUpdateValue(row, payload.mapping, getMappedUpdateFields(payload.mapping));
        if (invalidBoolean) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) summary.skippedReasons.push({ row: rowNum, reason: `Ungültiger Ja/Nein-Wert für ${invalidBoolean.key}: ${invalidBoolean.raw}`, sample: sampleText, draft });
          continue;
        }
        if (classification.kind === "existing") {
          const matched = classification.value;
          const patch = buildUpdateOnlyMarketPatch(draft, getMappedUpdateFields(payload.mapping), normalizedDraftRegion);
          if (matched.marketType === "kuehler") {
            patch.marketType = "both";
            patch.universeMarket = true;
          }
          seenFlex.add(flexKey);
          summary.matchedBy.flexNumber += 1;
          if (!marketPatchHasChanges(matched, patch)) {
            summary.unchanged += 1;
            continue;
          }
          const [updated] = await tx.update(markets).set({
            ...patch,
            importSourceFileName: payload.fileName,
            importedAt,
            updatedAt: new Date(),
          }).where(eq(markets.id, matched.id)).returning();
          if (updated) {
            if (stdKey && stdKey !== normStr(matched.standardMarketNumber ?? "")) {
              const oldKey = normStr(matched.standardMarketNumber ?? "");
              if (oldKey && byStandard.get(oldKey)?.id === matched.id) byStandard.delete(oldKey);
              byStandard.set(stdKey, updated);
            }
            if (cokeKey && cokeKey !== normStr(matched.cokeMasterNumber ?? "")) {
              const oldKey = normStr(matched.cokeMasterNumber ?? "");
              if (oldKey && byCoke.get(oldKey)?.id === matched.id) byCoke.delete(oldKey);
              byCoke.set(cokeKey, updated);
            }
            byFlex.set(flexKey, updated);
            summary.updated += 1;
          }
          continue;
        }
        const hasRequiredUniversumFields = Boolean(
          flexIdentity &&
          (allowMissingCokeMasterNumber || cokeIdentity) &&
          draft.name &&
          draft.address &&
          draft.postalCode &&
          draft.city &&
          normalizedDraftRegion?.ok,
        );
        if (!hasRequiredUniversumFields) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) {
            const skipMeta = buildSkipMeta(draft, payload.mapping, importType);
            const requiredKeys: ImportFieldKey[] = ["flexNumber", "name", "address", "postalCode", "city", "region"];
            if (!allowMissingCokeMasterNumber) requiredKeys.splice(1, 0, "cokeMasterNumber");
            const labels: Record<string, string> = {
              flexNumber: "Flex-Nummer", cokeMasterNumber: "Stammnr. von Coke", name: "Name",
              address: "Adresse", postalCode: "PLZ", city: "Ort", region: "Region",
            };
            const missingFieldKeys = requiredKeys.filter((key) => !hasImportValue(draft[key]));
            summary.skippedReasons.push({
              row: rowNum,
              reason: allowMissingCokeMasterNumber
                ? "Neuer Universum-Markt ohne Pflichtfelder (Flex-Nummer, Name, Adresse, PLZ, Ort, Region)"
                : "Neuer Universum-Markt ohne Pflichtfelder (Flex-Nummer, Stammnr. von Coke, Name, Adresse, PLZ, Ort, Region)",
              sample: sampleText,
              draft,
              missingFields: missingFieldKeys.map((key) => `${labels[key]} (${isValidColLetter(payload.mapping[key] ?? "") ? "leer" : "nicht gemappt"})`),
              missingFieldKeys,
              fetchedFields: skipMeta.fetchedFields,
            });
          }
          continue;
        }

        const pendingIndex =
          pendingCreates.push({
            standardMarketNumber: normalizeIdentity(draft.standardMarketNumber),
            cokeMasterNumber: normalizeIdentity(draft.cokeMasterNumber),
            flexNumber: normalizeIdentity(draft.flexNumber),
            name: String(draft.name ?? ""),
            dbName: normalizeOptionalText(String(draft.dbName ?? "")) ?? "",
            address: String(draft.address ?? ""),
            postalCode: String(draft.postalCode ?? ""),
            city: String(draft.city ?? ""),
            region: normalizedDraftRegion?.ok ? normalizedDraftRegion.canonical : String(draft.region ?? ""),
            emEh: normalizeOptionalText(String(draft.emEh ?? "")) ?? "",
            employee: normalizeOptionalText(String(draft.employee ?? "")) ?? "",
            currentGmName: "",
            visitFrequencyPerYear: Number(draft.visitFrequencyPerYear ?? 0),
            infoFlag: Boolean(draft.infoFlag ?? false),
            infoNote: "",
            universeMarket: draft.universeMarket == null ? true : Boolean(draft.universeMarket),
            marketType: "universum",
            kuehlerStammnr: null,
            isActive: true,
            importSourceFileName: payload.fileName,
            importedAt,
            isDeleted: false,
          }) - 1;
        summary.created += 1;
        seenFlex.add(flexKey);
        registerPending(pendingIndex);
      }

      if (pendingCreates.length > 0) {
        const BATCH_SIZE = 250;
        for (let offset = 0; offset < pendingCreates.length; offset += BATCH_SIZE) {
          const batch = pendingCreates.slice(offset, offset + BATCH_SIZE);
          await tx.insert(markets).values(batch);
        }
      }
    });

    const fresh = await db
      .select()
      .from(markets)
      .where(eq(markets.isDeleted, false))
      .orderBy(desc(markets.createdAt));
    logAction("info", "market_import_completed", {
      req,
      action: "market_import",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: {
        importType,
        created: summary.created,
        updated: summary.updated,
        existingMatchesSkipped: summary.existingMatchesSkipped,
        duplicateInputRowsSkipped: summary.duplicateInputRowsSkipped,
        skipped: summary.skipped,
        duplicateInputRowsMerged: summary.duplicateInputRowsMerged,
      },
    });
    res.status(200).json({ markets: fresh.map(mapMarketRow), summary });
  } catch (err) {
    if (err instanceof Error && err.message === "SNAPSHOT_STALE") {
      res.status(409).json({ error: "Die Marktdaten haben sich seit der Vorschau geändert. Bitte die Vorschau erneut laden und prüfen." });
      return;
    }
    if (err instanceof Error && err.message.startsWith("SNAPSHOT_INVALID:")) {
      res.status(422).json({ error: err.message.slice("SNAPSHOT_INVALID:".length) });
      return;
    }
    if (err instanceof Error && err.message === "IMPORT_IN_PROGRESS") {
      logAction("warn", "market_import_in_progress", {
        req,
        action: "market_import",
        result: "rejected",
        statusCode: 409,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(409).json({
        error: "Ein anderer Import läuft bereits. Bitte in wenigen Sekunden erneut versuchen.",
      });
      return;
    }
    if (isIdentityConstraintViolation(err)) {
      const activeUpdateRowContext = activeUpdateRowContextRef.current;
      const violation = getPgUniqueViolation(err);
      const constraint = getPgConstraintName(violation);
      const identityField =
        constraint === "markets_standard_market_number_unique"
          ? "standardMarketNumber"
          : constraint === "markets_coke_master_number_unique"
            ? "cokeMasterNumber"
            : null;
      const identityLabel =
        identityField === "standardMarketNumber"
          ? "Standardmarkt Nr."
          : identityField === "cokeMasterNumber"
            ? "Stammnr. von Coke"
            : "Identitätsnummer";
      const identityValue = identityField ? activeUpdateRowContext?.identityValues[identityField] : null;
      const rowPrefix = activeUpdateRowContext
        ? `Excel-Zeile ${activeUpdateRowContext.row} (Flex ${activeUpdateRowContext.flexNumber}, ${activeUpdateRowContext.marketName}): `
        : "";
      logAction("warn", "market_import_identity_conflict", {
        req,
        action: "market_import",
        result: "failure",
        statusCode: 409,
        requestClass: "client_error",
        startedAtNs,
        error: err,
        details: {
          constraint,
          row: activeUpdateRowContext?.row ?? null,
          flexNumber: activeUpdateRowContext?.flexNumber ?? null,
          identityField,
          identityValue,
        },
      });
      markErrorAsLogged(err);
      res.status(409).json({
        error:
          `${rowPrefix}${identityLabel}${identityValue ? ` ${identityValue}` : ""} ist bereits einem anderen aktiven Markt zugeordnet. ` +
          "Es wurde nichts aus dieser Datei gespeichert. Bitte die Nummer in der genannten Excel-Zeile korrigieren und erneut importieren.",
        code: "market_import_identity_conflict",
        details: {
          row: activeUpdateRowContext?.row ?? null,
          flexNumber: activeUpdateRowContext?.flexNumber ?? null,
          marketName: activeUpdateRowContext?.marketName ?? null,
          identityField,
          identityLabel,
          identityValue,
        },
      });
      return;
    }
    const kuehlerUnitUniqueViolation = getPgUniqueViolation(err);
    if (
      kuehlerUnitUniqueViolation?.constraint === "market_kuehler_units_internal_id_active_unique" ||
      kuehlerUnitUniqueViolation?.constraint === "market_kuehler_units_technical_ident_no_active_unique"
    ) {
      const label = kuehlerUnitUniqueViolation.constraint === "market_kuehler_units_internal_id_active_unique"
        ? "internal_id"
        : "Tech. Ident. No.";
      logAction("warn", "market_import_kuehler_identity_conflict", {
        req,
        action: "market_import",
        result: "failure",
        statusCode: 409,
        requestClass: "client_error",
        startedAtNs,
        error: err,
        details: { constraint: kuehlerUnitUniqueViolation.constraint },
      });
      markErrorAsLogged(err);
      res.status(409).json({
        error: `${label} ist bereits einem anderen aktiven Kühler zugeordnet. Es wurde nichts aus dieser Datei gespeichert.`,
        code: "market_import_kuehler_identity_conflict",
      });
      return;
    }
    if (isPgLockTimeout(err)) {
      logAction("warn", "market_import_lock_timeout", {
        req,
        action: "market_import",
        result: "failure",
        statusCode: 409,
        requestClass: "client_error",
        startedAtNs,
        error: err,
      });
      markErrorAsLogged(err);
      res.status(409).json({
        error: "Import konnte keinen DB-Lock erhalten. Bitte erneut versuchen.",
      });
      return;
    }
    if (isPgStatementTimeout(err)) {
      logAction("error", "market_import_statement_timeout", {
        req,
        action: "market_import",
        result: "failure",
        statusCode: 504,
        requestClass: "server_error",
        startedAtNs,
        error: err,
      });
      markErrorAsLogged(err);
      res.status(504).json({
        error: "Import dauerte zu lange (DB-Timeout). Bitte Datei erneut importieren.",
      });
      return;
    }
    logAction("error", "market_import_failed", {
      req,
      action: "market_import",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error: err,
    });
    markErrorAsLogged(err);
    next(err);
  }
});

adminMarketsRouter.get("/:id/kuehler-units", async (req: AuthedRequest, res, next) => {
  try {
    const marketId = String(req.params.id ?? "");
    if (!marketId) {
      res.status(400).json({ error: "Market id is required." });
      return;
    }
    const [existingMarket] = await db
      .select({ id: markets.id })
      .from(markets)
      .where(and(eq(markets.id, marketId), eq(markets.isDeleted, false)))
      .limit(1);
    if (!existingMarket) {
      res.status(404).json({ error: "Market not found." });
      return;
    }
    const rows = await db
      .select()
      .from(marketKuehlerUnits)
      .where(and(eq(marketKuehlerUnits.marketId, marketId), eq(marketKuehlerUnits.isDeleted, false)))
      .orderBy(desc(marketKuehlerUnits.importedAt), desc(marketKuehlerUnits.createdAt));
    res.status(200).json({ units: rows.map(mapKuehlerUnitRow) });
  } catch (err) {
    next(err);
  }
});

adminMarketsRouter.post("/:id/kuehler-units", async (req: AuthedRequest, res, next) => {
  try {
    const marketId = String(req.params.id ?? "");
    if (!marketId) {
      res.status(400).json({ error: "Market id is required." });
      return;
    }
    const parsed = createKuehlerUnitSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid kuehler unit payload." });
      return;
    }
    const payload = parsed.data;
    const normalizedInternalId = normalizeIdentity(payload.kuehlerInternalId);
    const txResult = await db.transaction(async (tx) => {
      const [existingMarket] = await tx
        .select({
          id: markets.id,
          marketType: markets.marketType,
          universeMarket: markets.universeMarket,
          kuehlerStammnr: markets.kuehlerStammnr,
          cokeMasterNumber: markets.cokeMasterNumber,
        })
        .from(markets)
        .where(and(eq(markets.id, marketId), eq(markets.isDeleted, false)))
        .limit(1);
      if (!existingMarket) {
        return { status: "not_found" as const };
      }

      const promotionStammnr =
        normalizeStammnrForMatch(existingMarket.kuehlerStammnr) ??
        normalizeStammnrForMatch(existingMarket.cokeMasterNumber);
      if (existingMarket.marketType === "universum" && !promotionStammnr) {
        return { status: "missing_stammnr" as const };
      }

      const [createdUnit] = await tx
        .insert(marketKuehlerUnits)
        .values({
          marketId,
          name: normalizeOptionalText(payload.name) ?? "",
          employee: normalizeOptionalText(payload.employee) ?? "",
          kuehlerInternalId: normalizedInternalId,
          kuehlerBd: normalizeOptionalText(payload.kuehlerBd),
          kuehlerAnzahlKsAmStandort: payload.kuehlerAnzahlKsAmStandort ?? null,
          kuehlerSerialNumber: normalizeOptionalText(payload.kuehlerSerialNumber),
          kuehlerTechnicalIdentNo: normalizeIdentity(payload.kuehlerTechnicalIdentNo),
          kuehlerModel: normalizeOptionalText(payload.kuehlerModel),
          importSourceFileName: payload.importSourceFileName,
          importedAt: payload.importedAt ? new Date(payload.importedAt) : new Date(),
          isDeleted: false,
        })
        .returning();
      if (!createdUnit) {
        return { status: "create_failed" as const };
      }

      const nextMarketType: MarketType =
        existingMarket.marketType === "universum" ? "both" : existingMarket.marketType;
      if (nextMarketType !== existingMarket.marketType) {
        await tx
          .update(markets)
          .set({
            marketType: nextMarketType,
            universeMarket: deriveUniverseMarketFromType(nextMarketType),
            kuehlerStammnr: promotionStammnr,
            cokeMasterNumber: promotionStammnr,
            updatedAt: new Date(),
          })
          .where(eq(markets.id, marketId));
      }

      return { status: "ok" as const, unit: createdUnit };
    });

    if (txResult.status === "not_found") {
      res.status(404).json({ error: "Market not found." });
      return;
    }
    if (txResult.status === "missing_stammnr") {
      res.status(400).json({
        error:
          "Kühler kann nur angelegt werden, wenn der Markt eine gültige Stammnr (kuehler_stammnr oder coke_master_number) hat.",
      });
      return;
    }
    if (txResult.status === "create_failed") {
      res.status(500).json({ error: "Failed to create kuehler unit." });
      return;
    }

    res.status(201).json({ unit: mapKuehlerUnitRow(txResult.unit) });
  } catch (err) {
    const uniqueViolation = getPgUniqueViolation(err);
    if (
      uniqueViolation?.constraint === "market_kuehler_units_internal_id_active_unique" ||
      uniqueViolation?.constraint === "market_kuehler_units_technical_ident_no_active_unique"
    ) {
      const label = uniqueViolation.constraint === "market_kuehler_units_internal_id_active_unique"
        ? "internal_id"
        : "Tech. Ident. No.";
      res.status(409).json({ error: `Kühler konnte nicht angelegt werden: ${label} bereits vorhanden.` });
      return;
    }
    next(err);
  }
});

adminMarketsRouter.patch("/:marketId/kuehler-units/:unitId", async (req: AuthedRequest, res, next) => {
  try {
    const marketId = String(req.params.marketId ?? "");
    const unitId = String(req.params.unitId ?? "");
    if (!marketId || !unitId) {
      res.status(400).json({ error: "Market id and unit id are required." });
      return;
    }
    const parsed = updateKuehlerUnitSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid kuehler unit update payload." });
      return;
    }

    const [existingUnit] = await db
      .select()
      .from(marketKuehlerUnits)
      .where(
        and(
          eq(marketKuehlerUnits.id, unitId),
          eq(marketKuehlerUnits.marketId, marketId),
          eq(marketKuehlerUnits.isDeleted, false),
        ),
      )
      .limit(1);
    if (!existingUnit) {
      res.status(404).json({ error: "Kühler-Eintrag nicht gefunden." });
      return;
    }

    const payload = parsed.data;
    const [updatedUnit] = await db
      .update(marketKuehlerUnits)
      .set({
        name: payload.name != null ? normalizeOptionalText(payload.name) ?? "" : undefined,
        employee: payload.employee != null ? normalizeOptionalText(payload.employee) ?? "" : undefined,
        kuehlerInternalId:
          payload.kuehlerInternalId != null ? normalizeIdentity(payload.kuehlerInternalId) : undefined,
        kuehlerBd: payload.kuehlerBd != null ? normalizeOptionalText(payload.kuehlerBd) : undefined,
        kuehlerAnzahlKsAmStandort:
          payload.kuehlerAnzahlKsAmStandort != null ? payload.kuehlerAnzahlKsAmStandort : undefined,
        kuehlerSerialNumber:
          payload.kuehlerSerialNumber != null ? normalizeOptionalText(payload.kuehlerSerialNumber) : undefined,
        kuehlerTechnicalIdentNo:
          payload.kuehlerTechnicalIdentNo != null ? normalizeIdentity(payload.kuehlerTechnicalIdentNo) : undefined,
        kuehlerModel: payload.kuehlerModel != null ? normalizeOptionalText(payload.kuehlerModel) : undefined,
        importSourceFileName:
          payload.importSourceFileName != null ? payload.importSourceFileName : undefined,
        importedAt: payload.importedAt ? new Date(payload.importedAt) : undefined,
        isDeleted: payload.isDeleted,
        updatedAt: new Date(),
      })
      .where(eq(marketKuehlerUnits.id, existingUnit.id))
      .returning();
    if (!updatedUnit) {
      res.status(404).json({ error: "Kühler-Eintrag nicht gefunden." });
      return;
    }
    res.status(200).json({ unit: mapKuehlerUnitRow(updatedUnit) });
  } catch (err) {
    const uniqueViolation = getPgUniqueViolation(err);
    if (
      uniqueViolation?.constraint === "market_kuehler_units_internal_id_active_unique" ||
      uniqueViolation?.constraint === "market_kuehler_units_technical_ident_no_active_unique"
    ) {
      const label = uniqueViolation.constraint === "market_kuehler_units_internal_id_active_unique"
        ? "internal_id"
        : "Tech. Ident. No.";
      res.status(409).json({ error: `Kühler konnte nicht aktualisiert werden: ${label} bereits vorhanden.` });
      return;
    }
    next(err);
  }
});

adminMarketsRouter.patch("/:id/universe-market", async (req: AuthedRequest, res, next) => {
  try {
    const id = String(req.params.id ?? "");
    if (!z.string().uuid().safeParse(id).success) {
      res.status(400).json({ error: "Ungültige Markt-ID." });
      return;
    }
    const parsed = updateUniverseMarketSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültiger Universumsmarkt-Wert." });
      return;
    }

    const [updated] = await db
      .update(markets)
      .set({
        universeMarket: parsed.data.universeMarket,
        updatedAt: new Date(),
      })
      .where(and(eq(markets.id, id), eq(markets.isDeleted, false)))
      .returning();

    if (!updated) {
      res.status(404).json({ error: "Markt nicht gefunden." });
      return;
    }
    res.status(200).json({
      market: {
        id: updated.id,
        universeMarket: updated.universeMarket,
      },
    });
  } catch (err) {
    next(err);
  }
});

adminMarketsRouter.patch("/:id", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const id = String(req.params.id ?? "");
    if (!id) {
      logAction("warn", "market_update_invalid_id", {
        req,
        action: "market_update",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Market id is required." });
      return;
    }
    const parsed = updateMarketSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "market_update_invalid_payload", {
        req,
        action: "market_update",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Invalid market update payload." });
      return;
    }

    const payload = parsed.data;
    const [existingMarket] = await db
      .select()
      .from(markets)
      .where(and(eq(markets.id, id), eq(markets.isDeleted, false)))
      .limit(1);

    if (!existingMarket) {
      logAction("warn", "market_update_not_found", {
        req,
        action: "market_update",
        result: "rejected",
        statusCode: 404,
        requestClass: "client_error",
        startedAtNs,
        details: { marketId: id },
      });
      res.status(404).json({ error: "Market not found." });
      return;
    }

    const resolvedMarketType: MarketType = payload.marketType
      ? payload.marketType
      : payload.universeMarket != null
        ? payload.universeMarket
          ? existingMarket.marketType === "kuehler"
            ? "universum"
            : existingMarket.marketType
          : "kuehler"
        : existingMarket.marketType;
    const resolvedUniverseMarket = payload.universeMarket ?? existingMarket.universeMarket;
    const identityResolution = resolveKuehlerIdentity({
      marketType: resolvedMarketType,
      incomingKuehlerStammnr: payload.kuehlerStammnr,
      incomingCokeMasterNumber: payload.cokeMasterNumber,
      existingKuehlerStammnr: existingMarket.kuehlerStammnr,
      existingCokeMasterNumber: existingMarket.cokeMasterNumber,
    });
    if (!identityResolution.ok) {
      res.status(400).json({ error: identityResolution.error });
      return;
    }
    const resolvedStammnr = identityResolution.stammnr;

    const normalizedRegion = payload.region != null ? normalizeRegionValue(payload.region) : null;
    if (normalizedRegion && !normalizedRegion.ok) {
      logAction("warn", "market_update_invalid_region", {
        req,
        action: "market_update",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
        details: { region: normalizedRegion.raw },
      });
      res.status(400).json({
        error: `Unbekannte Region "${normalizedRegion.raw || payload.region}". Erlaubt: ${CANONICAL_REGIONS.join(", ")}.`,
      });
      return;
    }
    const now = new Date();
    const transactionResult = await db.transaction(async (tx) => {
      const [updated] = await tx
        .update(markets)
        .set({
        standardMarketNumber:
          payload.standardMarketNumber != null
            ? normalizeIdentity(payload.standardMarketNumber)
            : undefined,
        cokeMasterNumber:
          resolvedMarketType === "universum"
            ? payload.cokeMasterNumber != null
              ? normalizeIdentity(payload.cokeMasterNumber)
              : undefined
            : resolvedStammnr,
        flexNumber: payload.flexNumber != null ? normalizeIdentity(payload.flexNumber) : undefined,
        name: payload.name,
        dbName: payload.dbName != null ? (normalizeOptionalText(payload.dbName) ?? "") : undefined,
        address: payload.address,
        postalCode: payload.postalCode,
        city: payload.city,
        region: normalizedRegion?.ok ? normalizedRegion.canonical : payload.region,
        emEh: payload.emEh != null ? (normalizeOptionalText(payload.emEh) ?? "") : undefined,
        employee:
          payload.employee != null ? (normalizeOptionalText(payload.employee) ?? "") : undefined,
        currentGmName:
          payload.currentGmName != null
            ? (normalizeOptionalText(payload.currentGmName) ?? "")
            : undefined,
        visitFrequencyPerYear: payload.visitFrequencyPerYear,
        infoFlag: payload.infoFlag,
        infoNote: payload.infoNote,
        universeMarket: resolvedUniverseMarket,
        marketType: resolvedMarketType,
        kuehlerStammnr: resolvedMarketType === "universum" ? null : resolvedStammnr,
        isActive: payload.isActive,
        importSourceFileName: payload.importSourceFileName,
        importedAt: payload.importedAt ? new Date(payload.importedAt) : undefined,
        plannedToId: payload.plannedToId,
          updatedAt: now,
        })
        .where(eq(markets.id, existingMarket.id))
        .returning();

      if (!updated) return null;
      const identifierPropagation = await propagateSpecialArthurMarketIdentifiers(
        tx,
        existingMarket,
        updated,
        now,
      );
      const billaGmFilterEnrollment = await ensureAssignedBillaGmFilterEntry(
        tx,
        updated,
        req.authUser?.appUserId ?? null,
      );
      return { updated, identifierPropagation, billaGmFilterEnrollment };
    });

    const updated = transactionResult?.updated;

    if (!updated) {
      res.status(404).json({ error: "Market not found." });
      return;
    }

    const gmNamesByMarketId = await resolveActiveStandardGmNamesByMarketIds([updated.id]);
    res.status(200).json({
      market: {
        ...mapMarketRow(updated),
        plannedByActiveStandardGmName: gmNamesByMarketId.get(updated.id) ?? null,
      },
      identifierPropagation: transactionResult.identifierPropagation,
    });
    logAction("info", "market_update_success", {
      req,
      action: "market_update",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: {
        marketId: updated.id,
        identifierPropagation: transactionResult.identifierPropagation,
        billaGmFilterEnrollment: transactionResult.billaGmFilterEnrollment,
      },
    });
  } catch (err) {
    if (isIdentityConstraintViolation(err)) {
      logAction("warn", "market_update_identity_conflict", {
        req,
        action: "market_update",
        result: "failure",
        statusCode: 409,
        requestClass: "client_error",
        startedAtNs,
        error: err,
      });
      markErrorAsLogged(err);
      res.status(409).json({
        error: "Markt konnte nicht aktualisiert werden: Identitätsnummer bereits bei aktivem Markt vorhanden.",
      });
      return;
    }
    logAction("error", "market_update_failed", {
      req,
      action: "market_update",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error: err,
    });
    markErrorAsLogged(err);
    next(err);
  }
});

adminMarketsRouter.post("/", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const parsed = createMarketSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "market_create_invalid_payload", {
        req,
        action: "market_create",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Invalid market create payload." });
      return;
    }
    const payload = parsed.data;
    const resolvedMarketType: MarketType = payload.marketType ?? (payload.universeMarket ? "universum" : "kuehler");
    const resolvedUniverseMarket = deriveUniverseMarketFromType(resolvedMarketType);
    const identityResolution = resolveKuehlerIdentity({
      marketType: resolvedMarketType,
      incomingKuehlerStammnr: payload.kuehlerStammnr,
      incomingCokeMasterNumber: payload.cokeMasterNumber,
    });
    if (!identityResolution.ok) {
      res.status(400).json({ error: identityResolution.error });
      return;
    }
    const resolvedStammnr = identityResolution.stammnr;
    const normalizedIdentities: NormalizedMarketIdentityInput = {
      standardMarketNumber: normalizeIdentity(payload.standardMarketNumber),
      stammnr:
        resolvedMarketType === "universum"
          ? normalizeIdentity(payload.cokeMasterNumber)
          : resolvedStammnr,
      flexNumber: normalizeIdentity(payload.flexNumber),
    };
    const identityConflicts = await findMarketIdentityConflicts(normalizedIdentities);
    if (identityConflicts.length > 0) {
      const error = formatMarketIdentityConflictError(identityConflicts);
      logAction("warn", "market_create_identity_conflict", {
        req,
        action: "market_create",
        result: "rejected",
        statusCode: 409,
        requestClass: "client_error",
        startedAtNs,
        details: {
          conflictingMarketIds: identityConflicts.map((conflict) => conflict.marketId),
          matches: identityConflicts.map((conflict) => conflict.matches),
        },
      });
      res.status(409).json({
        error,
        code: "market_identity_conflict",
        conflictingMarket: identityConflicts[0],
        conflicts: identityConflicts,
      });
      return;
    }
    const normalizedRegion = normalizeRegionValue(payload.region);
    if (!normalizedRegion.ok) {
      logAction("warn", "market_create_invalid_region", {
        req,
        action: "market_create",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
        details: { region: normalizedRegion.raw },
      });
      res.status(400).json({
        error: `Unbekannte Region "${normalizedRegion.raw || payload.region}". Erlaubt: ${CANONICAL_REGIONS.join(", ")}.`,
      });
      return;
    }
    const creationResult = await db.transaction(async (tx) => {
      const [createdMarket] = await tx
        .insert(markets)
        .values({
          standardMarketNumber: normalizedIdentities.standardMarketNumber,
          cokeMasterNumber: normalizedIdentities.stammnr,
          flexNumber: normalizedIdentities.flexNumber,
          name: payload.name,
          dbName: normalizeOptionalText(payload.dbName) ?? "",
          address: payload.address,
          postalCode: payload.postalCode,
          city: payload.city,
          region: normalizedRegion.canonical,
          emEh: normalizeOptionalText(payload.emEh) ?? "",
          employee: normalizeOptionalText(payload.employee) ?? "",
          currentGmName: normalizeOptionalText(payload.currentGmName) ?? "",
          visitFrequencyPerYear: payload.visitFrequencyPerYear,
          infoFlag: payload.infoFlag,
          infoNote: payload.infoNote,
          universeMarket: resolvedUniverseMarket,
          marketType: resolvedMarketType,
          kuehlerStammnr: resolvedMarketType === "universum" ? null : resolvedStammnr,
          isActive: payload.isActive,
          importSourceFileName: payload.importSourceFileName,
          importedAt: payload.importedAt ? new Date(payload.importedAt) : new Date(),
          plannedToId: payload.plannedToId ?? null,
          isDeleted: false,
        })
        .returning();
      if (!createdMarket) return null;

      const billaGmFilterEnrollment = await ensureAssignedBillaGmFilterEntry(
        tx,
        createdMarket,
        req.authUser?.appUserId ?? null,
      );
      return { createdMarket, billaGmFilterEnrollment };
    });
    const created = creationResult?.createdMarket;
    if (!created) {
      logAction("error", "market_create_failed_no_row", {
        req,
        action: "market_create",
        result: "failure",
        statusCode: 500,
        requestClass: "server_error",
        startedAtNs,
      });
      res.status(500).json({ error: "Failed to create market." });
      return;
    }
    const gmNamesByMarketId = await resolveActiveStandardGmNamesByMarketIds([created.id]);
    res.status(201).json({
      market: {
        ...mapMarketRow(created),
        plannedByActiveStandardGmName: gmNamesByMarketId.get(created.id) ?? null,
      },
    });
    logAction("info", "market_create_success", {
      req,
      action: "market_create",
      result: "success",
      statusCode: 201,
      requestClass: "success",
      startedAtNs,
      details: {
        marketId: created.id,
        billaGmFilterEnrollment: creationResult.billaGmFilterEnrollment,
      },
    });
  } catch (err) {
    if (isIdentityConstraintViolation(err)) {
      logAction("warn", "market_create_identity_conflict", {
        req,
        action: "market_create",
        result: "rejected",
        statusCode: 409,
        requestClass: "client_error",
        startedAtNs,
        error: err,
        details: { detectedAfterInsertRace: true },
      });
      markErrorAsLogged(err);
      res.status(409).json({
        error:
          "Markt konnte nicht angelegt werden: Mindestens eine Identitätsnummer gehört bereits zu einem aktiven Markt. Bitte den bestehenden Markt bearbeiten.",
        code: "market_identity_conflict",
      });
      return;
    }
    logAction("error", "market_create_failed", {
      req,
      action: "market_create",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error: err,
    });
    markErrorAsLogged(err);
    next(err);
  }
});

adminMarketsRouter.patch("/:id/delete", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const id = String(req.params.id ?? "");
    if (!id) {
      logAction("warn", "market_delete_invalid_id", {
        req,
        action: "market_delete",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Market id is required." });
      return;
    }

    const [updated] = await db
      .update(markets)
      .set({
        isDeleted: true,
        updatedAt: new Date(),
      })
      .where(and(eq(markets.id, id), eq(markets.isDeleted, false)))
      .returning();

    if (!updated) {
      res.status(200).json({ ok: true, alreadyDeleted: true });
      logAction("info", "market_delete_already_deleted", {
        req,
        action: "market_delete",
        result: "success",
        statusCode: 200,
        requestClass: "success",
        startedAtNs,
        details: { marketId: id, alreadyDeleted: true },
      });
      return;
    }
    res.status(200).json({ ok: true });
    logAction("info", "market_delete_success", {
      req,
      action: "market_delete",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { marketId: updated.id },
    });
  } catch (err) {
    logAction("error", "market_delete_failed", {
      req,
      action: "market_delete",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error: err,
    });
    markErrorAsLogged(err);
    next(err);
  }
});

adminMarketsRouter.delete("/:id/hard-delete", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const id = String(req.params.id ?? "");
    if (!id) {
      logAction("warn", "market_hard_delete_invalid_id", {
        req,
        action: "market_hard_delete",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Market id is required." });
      return;
    }

    const [deleted] = await db.delete(markets).where(eq(markets.id, id)).returning({ id: markets.id });

    if (!deleted) {
      res.status(200).json({ ok: true, alreadyDeleted: true });
      logAction("info", "market_hard_delete_already_deleted", {
        req,
        action: "market_hard_delete",
        result: "success",
        statusCode: 200,
        requestClass: "success",
        startedAtNs,
        details: { marketId: id, alreadyDeleted: true },
      });
      return;
    }

    res.status(200).json({ ok: true });
    logAction("info", "market_hard_delete_success", {
      req,
      action: "market_hard_delete",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { marketId: deleted.id },
    });
  } catch (err) {
    logAction("error", "market_hard_delete_failed", {
      req,
      action: "market_hard_delete",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error: err,
    });
    markErrorAsLogged(err);
    next(err);
  }
});

adminMarketsRouter.post("/normalize-regions", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const parsed = normalizeRegionsSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      logAction("warn", "market_normalize_regions_invalid_payload", {
        req,
        action: "market_normalize_regions",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültiger Normalize-Request." });
      return;
    }
    const { batchSize, reportLimit } = parsed.data;
    let cursorId: string | null = null;
    let processedCount = 0;
    let updatedCount = 0;
    let unmatchedCount = 0;
    type RegionBatchRow = { id: string; name: string; region: string };
    const unmatched: Array<{ marketId: string; marketName: string; region: string; normalizedToken: string }> =
      [];

    while (true) {
      let batch: RegionBatchRow[] = [];
      if (cursorId) {
        batch = await db
          .select({
            id: markets.id,
            name: markets.name,
            region: markets.region,
          })
          .from(markets)
          .where(and(eq(markets.isDeleted, false), gt(markets.id, cursorId)))
          .orderBy(asc(markets.id))
          .limit(batchSize);
      } else {
        batch = await db
          .select({
            id: markets.id,
            name: markets.name,
            region: markets.region,
          })
          .from(markets)
          .where(eq(markets.isDeleted, false))
          .orderBy(asc(markets.id))
          .limit(batchSize);
      }
      if (batch.length === 0) break;

      processedCount += batch.length;
      const updatesByCanonical = new Map<CanonicalRegion, string[]>();

      for (const row of batch) {
        const normalized = normalizeRegionValue(row.region);
        if (!normalized.ok) {
          unmatchedCount += 1;
          if (unmatched.length < reportLimit) {
            unmatched.push({
              marketId: row.id,
              marketName: row.name,
              region: row.region,
              normalizedToken: normalized.token,
            });
          }
          continue;
        }
        if (normalized.canonical === row.region) continue;
        const ids = updatesByCanonical.get(normalized.canonical) ?? [];
        ids.push(row.id);
        updatesByCanonical.set(normalized.canonical, ids);
      }

      for (const [canonicalRegion, ids] of updatesByCanonical.entries()) {
        if (ids.length === 0) continue;
        const changed = await db
          .update(markets)
          .set({
            region: canonicalRegion,
            updatedAt: new Date(),
          })
          .where(and(eq(markets.isDeleted, false), inArray(markets.id, ids)))
          .returning({ id: markets.id });
        updatedCount += changed.length;
      }

      cursorId = batch[batch.length - 1]?.id ?? null;
      if (!cursorId) break;
    }

    const unchangedCount = Math.max(0, processedCount - updatedCount - unmatchedCount);
    res.status(200).json({
      ok: true,
      processedCount,
      updatedCount,
      unchangedCount,
      unmatchedCount,
      unmatched,
      allowedRegions: CANONICAL_REGIONS,
    });
    logAction("info", "market_normalize_regions_completed", {
      req,
      action: "market_normalize_regions",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: {
        processedCount,
        updatedCount,
        unmatchedCount,
      },
    });
  } catch (err) {
    logAction("error", "market_normalize_regions_failed", {
      req,
      action: "market_normalize_regions",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error: err,
    });
    markErrorAsLogged(err);
    next(err);
  }
});

export { adminMarketsRouter, marketsRouter };
