import { and, asc, desc, eq, gt, ilike, inArray, isNotNull, or, sql } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { fetchFragebogenUi, fetchModulesUi } from "./fragebogen.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { db } from "../lib/db.js";
import { getCurrentRedPeriod } from "../lib/red-monat.js";
import { refreshRedMonthCalendarConfig } from "../lib/red-month-calendar.js";
import {
  campaignMarketAssignments,
  campaigns,
  markets,
  photoTags,
  visitSessionSections,
  visitSessions,
  users,
} from "../lib/schema.js";

type ImportFieldKey =
  | "standardMarketNumber"
  | "cokeMasterNumber"
  | "flexNumber"
  | "name"
  | "address"
  | "postalCode"
  | "city"
  | "dbName"
  | "emEh"
  | "region"
  | "employee"
  | "universeMarket"
  | "visitFrequencyPerYear"
  | "infoFlag";

type MarketDraft = Partial<
  Record<
    ImportFieldKey,
    string | number | boolean
  >
>;

const SECTION_ORDER = ["standard", "flex", "billa", "kuehler", "mhd"] as const;

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

function getCurrentRedPeriodBounds(now = new Date()): { startYmd: string; endYmd: string } {
  const period = getCurrentRedPeriod(now);
  return {
    startYmd: toYmd(period.start),
    endYmd: toYmd(period.end),
  };
}

const importFieldSpecs: Array<{ key: ImportFieldKey; label: string; required: boolean; isIdentity: boolean }> = [
  { key: "standardMarketNumber", label: "Standardmarkt Nr", required: false, isIdentity: true },
  { key: "cokeMasterNumber", label: "Stammnr. von Coke", required: false, isIdentity: true },
  { key: "flexNumber", label: "Flex-Nummer", required: false, isIdentity: true },
  { key: "name", label: "Name", required: true, isIdentity: false },
  { key: "address", label: "Adresse", required: true, isIdentity: false },
  { key: "postalCode", label: "Postleitzahl", required: true, isIdentity: false },
  { key: "city", label: "Ort", required: true, isIdentity: false },
  { key: "dbName", label: "Name f. DB", required: false, isIdentity: false },
  { key: "emEh", label: "EM/EH", required: false, isIdentity: false },
  { key: "region", label: "Region", required: true, isIdentity: false },
  { key: "employee", label: "Mitarbeiter", required: false, isIdentity: false },
  { key: "universeMarket", label: "Universums-Markt", required: false, isIdentity: false },
  { key: "visitFrequencyPerYear", label: "Besuchsrhythmus", required: false, isIdentity: false },
  { key: "infoFlag", label: "Info", required: false, isIdentity: false },
];

const mappingSchema = z
  .object({
    standardMarketNumber: z.string().optional(),
    cokeMasterNumber: z.string().optional(),
    flexNumber: z.string().optional(),
    name: z.string().optional(),
    address: z.string().optional(),
    postalCode: z.string().optional(),
    city: z.string().optional(),
    dbName: z.string().optional(),
    emEh: z.string().optional(),
    region: z.string().optional(),
    employee: z.string().optional(),
    universeMarket: z.string().optional(),
    visitFrequencyPerYear: z.string().optional(),
    infoFlag: z.string().optional(),
  })
  .strict();

const importMarketsSchema = z
  .object({
    fileName: z.string().min(1),
    sheetName: z.string().min(1),
    rows: z.array(z.array(z.union([z.string(), z.number(), z.boolean(), z.null()]))).min(1),
    mapping: mappingSchema,
  })
  .strict();

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
    isActive: z.boolean().optional(),
    importSourceFileName: z.string().optional(),
    importedAt: z.string().datetime().optional(),
    plannedToId: z.string().uuid().nullable().optional(),
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
    universeMarket: z.boolean().optional().default(false),
    isActive: z.boolean().optional().default(true),
    importSourceFileName: z.string().optional().default(""),
    importedAt: z.string().datetime().optional(),
    plannedToId: z.string().uuid().nullable().optional(),
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

function normBool(v: string): boolean {
  const s = v.trim().toLowerCase();
  return s === "ja" || s === "true" || s === "1" || s === "yes";
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

function isPgUniqueViolation(err: unknown): err is { code: string; constraint?: string } {
  if (!err || typeof err !== "object") return false;
  return "code" in err && (err as { code?: string }).code === "23505";
}

function isIdentityConstraintViolation(err: unknown): boolean {
  if (!isPgUniqueViolation(err)) return false;
  return (
    err.constraint === "markets_standard_market_number_unique" ||
    err.constraint === "markets_coke_master_number_unique" ||
    err.constraint === "markets_flex_number_unique"
  );
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

function mapRowToDraft(row: string[], mapping: z.infer<typeof mappingSchema>): MarketDraft {
  const draft: MarketDraft = {};
  for (const spec of importFieldSpecs) {
    const col = mapping[spec.key];
    if (!col || !isValidColLetter(col)) continue;
    const idx = excelColToIndex(col);
    const raw = row[idx]?.trim() ?? "";
    if (!raw) continue;
    if (spec.key === "universeMarket" || spec.key === "infoFlag") {
      draft[spec.key] = normBool(raw);
      continue;
    }
    if (spec.key === "visitFrequencyPerYear") {
      draft[spec.key] = normNum(raw);
      continue;
    }
    draft[spec.key] = raw;
  }
  return draft;
}

function buildSkipMeta(draft: MarketDraft, mapping: z.infer<typeof mappingSchema>) {
  const missingFields: string[] = [];
  const missingFieldKeys: ImportFieldKey[] = [];
  const fetchedFields: Array<{ label: string; value: string }> = [];

  for (const spec of importFieldSpecs) {
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

function mapMarketRow(row: typeof markets.$inferSelect) {
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
    isActive: row.isActive,
    importSourceFileName: row.importSourceFileName,
    importedAt: row.importedAt.toISOString(),
    plannedToId: row.plannedToId,
    plannedByActiveStandardGmName: null as string | null,
    isDeleted: row.isDeleted,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  };
}

type ActiveNowCampaignSummary = {
  campaignId: string;
  campaignName: string;
  section: "standard" | "flex" | "kuehler" | "mhd" | "billa";
};

type ProgressSectionKey = "kuehler" | "mhd";

type ProgressMarketRow = {
  marketId: string;
  campaignId: string;
  campaignName: string;
  chain: string;
  address: string;
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
        eq(campaigns.status, "active"),
        sql`(
          ${campaigns.scheduleType} = 'always'
          or (
            ${campaigns.scheduleType} = 'scheduled'
            and ${campaigns.startDate} is not null
            and ${campaigns.endDate} is not null
            and ${campaigns.startDate} <= current_date
            and ${campaigns.endDate} >= current_date
          )
        )`,
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

marketsRouter.use(requireAuth(["admin", "gm", "sm"]));

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
              ilike(markets.currentGmName, like),
            ),
          ),
        )
        .orderBy(desc(markets.createdAt));
    } else {
      rows = await db.select().from(markets).where(eq(markets.isDeleted, false)).orderBy(desc(markets.createdAt));
    }
    const gmNamesByMarketId = await resolveActiveStandardGmNamesByMarketIds(rows.map((row) => row.id));
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

    await refreshRedMonthCalendarConfig();
    const { startYmd, endYmd } = getCurrentRedPeriodBounds();
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
              and ${campaigns.status} in ('active', 'scheduled')
              and (
                ${campaigns.scheduleType} = 'always'
                or (
                  ${campaigns.scheduleType} = 'scheduled'
                  and ${campaigns.startDate} is not null
                  and ${campaigns.endDate} is not null
                  and ${campaigns.startDate} <= ${endYmd}
                  and ${campaigns.endDate} >= ${startYmd}
                )
              )
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
      })
      .from(campaignMarketAssignments)
      .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
      .where(
        and(
          eq(campaignMarketAssignments.gmUserId, gmUserId),
          eq(campaignMarketAssignments.isDeleted, false),
          eq(campaigns.isDeleted, false),
          eq(campaigns.status, "active"),
          sql`(
            ${campaigns.scheduleType} = 'always'
            or (
              ${campaigns.scheduleType} = 'scheduled'
              and ${campaigns.startDate} is not null
              and ${campaigns.endDate} is not null
              and ${campaigns.startDate} <= current_date
              and ${campaigns.endDate} >= current_date
            )
          )`,
        ),
      );

    const activeNowByMarketId = new Map<string, ActiveNowCampaignSummary[]>();
    for (const row of activeNowCampaignRows) {
      const bucket = activeNowByMarketId.get(row.marketId) ?? [];
      if (!bucket.some((entry) => entry.campaignId === row.campaignId)) {
        bucket.push({
          campaignId: row.campaignId,
          campaignName: row.campaignName,
          section: row.section,
        });
      }
      activeNowByMarketId.set(row.marketId, bucket);
    }

    const gmNamesByMarketId = await resolveActiveStandardGmNamesByMarketIds(rows.map((row) => row.id));
    res.status(200).json({
      markets: rows.map((row) => ({
        ...mapMarketRow(row),
        plannedByActiveStandardGmName: gmNamesByMarketId.get(row.id) ?? null,
        activeNowCampaigns: activeNowByMarketId.get(row.id) ?? [],
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

    await refreshRedMonthCalendarConfig();
    const { startYmd: redStartYmd, endYmd: redEndYmd } = getCurrentRedPeriodBounds();

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
          eq(campaigns.status, "active"),
          inArray(campaigns.section, ["kuehler", "mhd"]),
          sql`(
            ${campaigns.scheduleType} = 'always'
            or (
              ${campaigns.scheduleType} = 'scheduled'
              and ${campaigns.startDate} is not null
              and ${campaigns.endDate} is not null
              and ${campaigns.startDate} <= current_date
              and ${campaigns.endDate} >= current_date
            )
          )`,
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
      }
    >();
    const dateRangeBySection = new Map<ProgressSectionKey, { startDate: string; endDate: string }>([
      ["kuehler", { startDate: redStartYmd, endDate: redEndYmd }],
      ["mhd", { startDate: redStartYmd, endDate: redEndYmd }],
    ]);

    for (const row of assignmentRows) {
      const section = row.section as ProgressSectionKey;
      const key = `${row.marketId}__${row.campaignId}`;
      if (assignmentKeys.has(key)) continue;
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
      });
      const currentRange = dateRangeBySection.get(section);
      if (!currentRange) continue;
      const effectiveStart = row.scheduleType === "scheduled" && row.startDate ? String(row.startDate) : redStartYmd;
      const effectiveEnd = row.scheduleType === "scheduled" && row.endDate ? String(row.endDate) : redEndYmd;
      currentRange.startDate = effectiveStart < currentRange.startDate ? effectiveStart : currentRange.startDate;
      currentRange.endDate = effectiveEnd > currentRange.endDate ? effectiveEnd : currentRange.endDate;
      dateRangeBySection.set(section, currentRange);
    }

    const assignedCampaignIds = Array.from(new Set(Array.from(assignmentKeys.values()).map((row) => row.campaignId)));
    const completionRows =
      assignedCampaignIds.length === 0
        ? []
        : await db
            .select({
              marketId: visitSessions.marketId,
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
                inArray(visitSessionSections.section, ["kuehler", "mhd"]),
              ),
            )
            .orderBy(desc(visitSessions.submittedAt));

    const completedByKey = new Map<string, string>();
    for (const row of completionRows) {
      if (!row.submittedAt) continue;
      const key = `${row.marketId}__${row.campaignId}`;
      if (!assignmentKeys.has(key)) continue;
      if (!completedByKey.has(key)) {
        completedByKey.set(key, row.submittedAt.toISOString());
      }
    }

    const buildSectionPayload = (section: ProgressSectionKey): ProgressSectionPayload => {
      const rows = Array.from(assignmentKeys.values()).filter((entry) => entry.section === section);
      const markets = rows.map((row) => {
        const key = `${row.marketId}__${row.campaignId}`;
        const doneAt = completedByKey.get(key) ?? null;
        return {
          marketId: row.marketId,
          campaignId: row.campaignId,
          campaignName: row.campaignName,
          chain: row.chain,
          address: row.address,
          done: Boolean(doneAt),
          doneAt,
        };
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
    res.status(200).json({
      kuehler,
      mhd,
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
      res.status(400).json({ error: "Ungueltige visit-start Parameter." });
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
      res.status(400).json({ error: "Mindestens eine gueltige Kampagnen-ID ist erforderlich." });
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
          eq(campaignMarketAssignments.gmUserId, gmUserId),
          inArray(campaigns.id, campaignIds),
          eq(campaignMarketAssignments.isDeleted, false),
          eq(campaigns.isDeleted, false),
          eq(campaigns.status, "active"),
          sql`(
            ${campaigns.scheduleType} = 'always'
            or (
              ${campaigns.scheduleType} = 'scheduled'
              and ${campaigns.startDate} is not null
              and ${campaigns.endDate} is not null
              and ${campaigns.startDate} <= current_date
              and ${campaigns.endDate} >= current_date
            )
          )`,
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

adminMarketsRouter.use(requireAuth(["admin"]));

adminMarketsRouter.post("/import", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = importMarketsSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid import payload." });
      return;
    }

    const payload = parsed.data;
    const rowsAsStrings = payload.rows.map((row) => row.map((cell) => (cell == null ? "" : String(cell))));
    const summary: {
      fileName: string;
      sheetName: string;
      totalParsedRows: number;
      created: number;
      updated: number;
      skipped: number;
      duplicateInputRowsMerged: number;
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
    } = {
      fileName: payload.fileName,
      sheetName: payload.sheetName,
      totalParsedRows: Math.max(rowsAsStrings.length - 1, 0),
      created: 0,
      updated: 0,
      skipped: 0,
      duplicateInputRowsMerged: 0,
      matchedBy: { standardMarketNumber: 0, cokeMasterNumber: 0, flexNumber: 0, namePLZ: 0 },
      skippedReasons: [],
    };

    const importedAt = new Date();
    const dataRows = rowsAsStrings.slice(1);
    console.info(
      `[markets import] start file=${payload.fileName} sheet=${payload.sheetName} rows=${dataRows.length}`,
    );

    const lastRowByIdentity = new Map<string, number>();
    for (let i = 0; i < dataRows.length; i += 1) {
      const row = dataRows[i];
      if (!row) continue;
      const draft = mapRowToDraft(row, payload.mapping);
      const stdKey = normStr(normalizeIdentity(draft.standardMarketNumber)?.toString());
      const cokeKey = normStr(normalizeIdentity(draft.cokeMasterNumber)?.toString());
      const flexKey = normStr(normalizeIdentity(draft.flexNumber)?.toString());
      const identityKey = stdKey ? `std:${stdKey}` : cokeKey ? `coke:${cokeKey}` : flexKey ? `flex:${flexKey}` : "";
      if (!identityKey) continue;
      if (lastRowByIdentity.has(identityKey)) {
        summary.duplicateInputRowsMerged += 1;
      }
      lastRowByIdentity.set(identityKey, i);
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
      const byNamePlz = new Map<string, typeof markets.$inferSelect>();
      const pendingByStandard = new Map<string, number>();
      const pendingByCoke = new Map<string, number>();
      const pendingByFlex = new Map<string, number>();
      const pendingByNamePlz = new Map<string, number>();
      const pendingCreates: Array<typeof markets.$inferInsert> = [];

      const registerPending = (idx: number) => {
        const row = pendingCreates[idx];
        if (!row) return;
        const std = normStr((row.standardMarketNumber as string | null | undefined) ?? "");
        const coke = normStr((row.cokeMasterNumber as string | null | undefined) ?? "");
        const flex = normStr((row.flexNumber as string | null | undefined) ?? "");
        const name = normStr((row.name as string | undefined) ?? "");
        const plz = normStr((row.postalCode as string | undefined) ?? "");
        if (std) pendingByStandard.set(std, idx);
        if (coke) pendingByCoke.set(coke, idx);
        if (flex) pendingByFlex.set(flex, idx);
        pendingByNamePlz.set(`${name}|${plz}`, idx);
      };

      for (const market of existing) {
        if (market.standardMarketNumber) byStandard.set(normStr(market.standardMarketNumber), market);
        if (market.cokeMasterNumber) byCoke.set(normStr(market.cokeMasterNumber), market);
        if (market.flexNumber) byFlex.set(normStr(market.flexNumber), market);
        byNamePlz.set(`${normStr(market.name)}|${normStr(market.postalCode)}`, market);
      }

      for (let i = 0; i < dataRows.length; i += 1) {
        if (i > 0 && i % 500 === 0) {
          console.info(`[markets import] progress ${i}/${dataRows.length}`);
        }
        const row = dataRows[i];
        if (!row) continue;
        const rowNum = i + 2;
        if (row.every((c) => !c || !c.trim())) {
          summary.skipped += 1;
          continue;
        }

        const draft = mapRowToDraft(row, payload.mapping);
        const sampleText = String(draft.name ?? draft.city ?? row.find((c) => c?.trim()) ?? "");
        const hasDraftIdentity = Boolean(draft.standardMarketNumber || draft.cokeMasterNumber || draft.flexNumber);
        const hasDraftVisibles = Boolean(draft.name && (draft.postalCode || draft.city) && draft.region);
        const normalizedDraftRegion =
          draft.region != null ? normalizeRegionValue(String(draft.region)) : null;

        const stdIdentity = normalizeIdentity(draft.standardMarketNumber);
        const cokeIdentity = normalizeIdentity(draft.cokeMasterNumber);
        const flexIdentity = normalizeIdentity(draft.flexNumber);
        const stdKey = normStr(stdIdentity ?? "");
        const cokeKey = normStr(cokeIdentity ?? "");
        const flexKey = normStr(flexIdentity ?? "");
        const identityKey = stdKey ? `std:${stdKey}` : cokeKey ? `coke:${cokeKey}` : flexKey ? `flex:${flexKey}` : "";
        if (identityKey && lastRowByIdentity.get(identityKey) !== i) {
          continue;
        }

        if (!hasDraftIdentity && !hasDraftVisibles) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) {
            const { missingFields, missingFieldKeys, fetchedFields } = buildSkipMeta(draft, payload.mapping);
            summary.skippedReasons.push({
              row: rowNum,
              reason: "Keine Identität und fehlende Pflichtfelder",
              sample: sampleText,
              draft,
              missingFields,
              missingFieldKeys,
              fetchedFields,
            });
          }
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

        let matched: typeof markets.$inferSelect | undefined;
        let matchedPendingIndex: number | undefined;
        let matchKey: keyof typeof summary.matchedBy | null = null;
        const namePlzKey = `${normStr(String(draft.name ?? ""))}|${normStr(String(draft.postalCode ?? ""))}`;

        if (stdKey && byStandard.has(stdKey)) {
          matched = byStandard.get(stdKey);
          matchKey = "standardMarketNumber";
        } else if (stdKey && pendingByStandard.has(stdKey)) {
          matchedPendingIndex = pendingByStandard.get(stdKey);
          matchKey = "standardMarketNumber";
        } else if (cokeKey && byCoke.has(cokeKey)) {
          matched = byCoke.get(cokeKey);
          matchKey = "cokeMasterNumber";
        } else if (cokeKey && pendingByCoke.has(cokeKey)) {
          matchedPendingIndex = pendingByCoke.get(cokeKey);
          matchKey = "cokeMasterNumber";
        } else if (flexKey && byFlex.has(flexKey)) {
          matched = byFlex.get(flexKey);
          matchKey = "flexNumber";
        } else if (flexKey && pendingByFlex.has(flexKey)) {
          matchedPendingIndex = pendingByFlex.get(flexKey);
          matchKey = "flexNumber";
        } else if (namePlzKey && byNamePlz.has(namePlzKey)) {
          matched = byNamePlz.get(namePlzKey);
          matchKey = "namePLZ";
        } else if (namePlzKey && pendingByNamePlz.has(namePlzKey)) {
          matchedPendingIndex = pendingByNamePlz.get(namePlzKey);
          matchKey = "namePLZ";
        }

        if (matched) {
          const [updated] = await tx
            .update(markets)
            .set({
              standardMarketNumber:
                draft.standardMarketNumber != null
                  ? normalizeIdentity(String(draft.standardMarketNumber))
                  : matched.standardMarketNumber,
              cokeMasterNumber:
                draft.cokeMasterNumber != null
                  ? normalizeIdentity(String(draft.cokeMasterNumber))
                  : matched.cokeMasterNumber,
              flexNumber:
                draft.flexNumber != null ? normalizeIdentity(String(draft.flexNumber)) : matched.flexNumber,
              name: draft.name != null ? String(draft.name) : matched.name,
              dbName:
                draft.dbName != null ? (normalizeOptionalText(String(draft.dbName)) ?? "") : matched.dbName,
              address: draft.address != null ? String(draft.address) : matched.address,
              postalCode: draft.postalCode != null ? String(draft.postalCode) : matched.postalCode,
              city: draft.city != null ? String(draft.city) : matched.city,
              region:
                draft.region != null && normalizedDraftRegion?.ok
                  ? normalizedDraftRegion.canonical
                  : matched.region,
              emEh:
                draft.emEh != null ? (normalizeOptionalText(String(draft.emEh)) ?? "") : matched.emEh,
              employee:
                draft.employee != null
                  ? (normalizeOptionalText(String(draft.employee)) ?? "")
                  : matched.employee,
              universeMarket:
                draft.universeMarket != null ? Boolean(draft.universeMarket) : matched.universeMarket,
              visitFrequencyPerYear:
                draft.visitFrequencyPerYear != null
                  ? Number(draft.visitFrequencyPerYear)
                  : matched.visitFrequencyPerYear,
              infoFlag: draft.infoFlag != null ? Boolean(draft.infoFlag) : matched.infoFlag,
              importSourceFileName: payload.fileName,
              importedAt,
              updatedAt: new Date(),
            })
            .where(eq(markets.id, matched.id))
            .returning();

          if (updated) {
            summary.updated += 1;
            if (matchKey) summary.matchedBy[matchKey] += 1;
            if (updated.standardMarketNumber) byStandard.set(normStr(updated.standardMarketNumber), updated);
            if (updated.cokeMasterNumber) byCoke.set(normStr(updated.cokeMasterNumber), updated);
            if (updated.flexNumber) byFlex.set(normStr(updated.flexNumber), updated);
            byNamePlz.set(`${normStr(updated.name)}|${normStr(updated.postalCode)}`, updated);
          }
          continue;
        }

        if (matchedPendingIndex != null) {
          const pending = pendingCreates[matchedPendingIndex];
          if (pending) {
            pending.standardMarketNumber =
              draft.standardMarketNumber != null
                ? normalizeIdentity(String(draft.standardMarketNumber))
                : pending.standardMarketNumber;
            pending.cokeMasterNumber =
              draft.cokeMasterNumber != null
                ? normalizeIdentity(String(draft.cokeMasterNumber))
                : pending.cokeMasterNumber;
            pending.flexNumber =
              draft.flexNumber != null ? normalizeIdentity(String(draft.flexNumber)) : pending.flexNumber;
            pending.name = draft.name != null ? String(draft.name) : pending.name;
            pending.dbName =
              draft.dbName != null ? (normalizeOptionalText(String(draft.dbName)) ?? "") : pending.dbName;
            pending.address = draft.address != null ? String(draft.address) : pending.address;
            pending.postalCode = draft.postalCode != null ? String(draft.postalCode) : pending.postalCode;
            pending.city = draft.city != null ? String(draft.city) : pending.city;
            pending.region =
              draft.region != null && normalizedDraftRegion?.ok
                ? normalizedDraftRegion.canonical
                : pending.region;
            pending.emEh = draft.emEh != null ? (normalizeOptionalText(String(draft.emEh)) ?? "") : pending.emEh;
            pending.employee =
              draft.employee != null ? (normalizeOptionalText(String(draft.employee)) ?? "") : pending.employee;
            pending.universeMarket =
              draft.universeMarket != null ? Boolean(draft.universeMarket) : pending.universeMarket;
            pending.isActive = pending.isActive ?? true;
            pending.visitFrequencyPerYear =
              draft.visitFrequencyPerYear != null
                ? Number(draft.visitFrequencyPerYear)
                : pending.visitFrequencyPerYear;
            pending.infoFlag = draft.infoFlag != null ? Boolean(draft.infoFlag) : pending.infoFlag;
            pending.importSourceFileName = payload.fileName;
            pending.importedAt = importedAt;
            registerPending(matchedPendingIndex);
            summary.updated += 1;
            if (matchKey) summary.matchedBy[matchKey] += 1;
          }
          continue;
        }

        if (!hasDraftVisibles) {
          summary.skipped += 1;
          if (summary.skippedReasons.length < 50) {
            const { missingFields, missingFieldKeys, fetchedFields } = buildSkipMeta(draft, payload.mapping);
            summary.skippedReasons.push({
              row: rowNum,
              reason: "Neuer Markt ohne Pflichtfelder (Name, PLZ, Region)",
              sample: sampleText,
              draft,
              missingFields,
              missingFieldKeys,
              fetchedFields,
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
            universeMarket: Boolean(draft.universeMarket ?? false),
            isActive: true,
            importSourceFileName: payload.fileName,
            importedAt,
            isDeleted: false,
          }) - 1;
        summary.created += 1;
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
    console.info(
      `[markets import] done created=${summary.created} updated=${summary.updated} skipped=${summary.skipped}`,
    );
    res.status(200).json({ markets: fresh.map(mapMarketRow), summary });
  } catch (err) {
    if (err instanceof Error && err.message === "IMPORT_IN_PROGRESS") {
      res.status(409).json({
        error: "Ein anderer Import laeuft bereits. Bitte in wenigen Sekunden erneut versuchen.",
      });
      return;
    }
    if (isIdentityConstraintViolation(err)) {
      res.status(409).json({
        error:
          "Import konnte nicht abgeschlossen werden: aktive Märkte mit gleicher Identität existieren bereits.",
      });
      return;
    }
    if (isPgLockTimeout(err)) {
      res.status(409).json({
        error: "Import konnte keinen DB-Lock erhalten. Bitte erneut versuchen.",
      });
      return;
    }
    if (isPgStatementTimeout(err)) {
      res.status(504).json({
        error: "Import dauerte zu lange (DB-Timeout). Bitte Datei erneut importieren.",
      });
      return;
    }
    next(err);
  }
});

adminMarketsRouter.patch("/:id", async (req: AuthedRequest, res, next) => {
  try {
    const id = String(req.params.id ?? "");
    if (!id) {
      res.status(400).json({ error: "Market id is required." });
      return;
    }
    const parsed = updateMarketSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid market update payload." });
      return;
    }

    const payload = parsed.data;
    const normalizedRegion = payload.region != null ? normalizeRegionValue(payload.region) : null;
    if (normalizedRegion && !normalizedRegion.ok) {
      res.status(400).json({
        error: `Unbekannte Region "${normalizedRegion.raw || payload.region}". Erlaubt: ${CANONICAL_REGIONS.join(", ")}.`,
      });
      return;
    }
    const [updated] = await db
      .update(markets)
      .set({
        standardMarketNumber:
          payload.standardMarketNumber != null
            ? normalizeIdentity(payload.standardMarketNumber)
            : undefined,
        cokeMasterNumber:
          payload.cokeMasterNumber != null ? normalizeIdentity(payload.cokeMasterNumber) : undefined,
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
        universeMarket: payload.universeMarket,
        isActive: payload.isActive,
        importSourceFileName: payload.importSourceFileName,
        importedAt: payload.importedAt ? new Date(payload.importedAt) : undefined,
        plannedToId: payload.plannedToId,
        updatedAt: new Date(),
      })
      .where(and(eq(markets.id, id), eq(markets.isDeleted, false)))
      .returning();

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
    });
  } catch (err) {
    if (isIdentityConstraintViolation(err)) {
      res.status(409).json({
        error: "Markt konnte nicht angelegt werden: Identitätsnummer bereits bei aktivem Markt vorhanden.",
      });
      return;
    }
    next(err);
  }
});

adminMarketsRouter.post("/", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = createMarketSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid market create payload." });
      return;
    }
    const payload = parsed.data;
    const normalizedRegion = normalizeRegionValue(payload.region);
    if (!normalizedRegion.ok) {
      res.status(400).json({
        error: `Unbekannte Region "${normalizedRegion.raw || payload.region}". Erlaubt: ${CANONICAL_REGIONS.join(", ")}.`,
      });
      return;
    }
    const [created] = await db
      .insert(markets)
      .values({
        standardMarketNumber: normalizeIdentity(payload.standardMarketNumber),
        cokeMasterNumber: normalizeIdentity(payload.cokeMasterNumber),
        flexNumber: normalizeIdentity(payload.flexNumber),
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
        universeMarket: payload.universeMarket,
        isActive: payload.isActive,
        importSourceFileName: payload.importSourceFileName,
        importedAt: payload.importedAt ? new Date(payload.importedAt) : new Date(),
        plannedToId: payload.plannedToId ?? null,
        isDeleted: false,
      })
      .returning();
    if (!created) {
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
  } catch (err) {
    next(err);
  }
});

adminMarketsRouter.patch("/:id/delete", async (req: AuthedRequest, res, next) => {
  try {
    const id = String(req.params.id ?? "");
    if (!id) {
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
      return;
    }
    res.status(200).json({ ok: true });
  } catch (err) {
    next(err);
  }
});

adminMarketsRouter.post("/normalize-regions", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = normalizeRegionsSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
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
  } catch (err) {
    next(err);
  }
});

export { adminMarketsRouter, marketsRouter };
