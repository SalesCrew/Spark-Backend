import { and, asc, desc, eq, inArray, isNull, ne, sql } from "drizzle-orm";
import { type Response, Router } from "express";
import { z } from "zod";
import { type AuthedRequest, requireAuth } from "../middleware/auth.js";
import { db } from "../lib/db.js";
import { computeHiddenQuestionIds } from "../lib/conditional-visibility.js";
import {
  campaignMarketAssignmentHistory,
  campaignFragebogenHistory,
  campaignMarketAssignments,
  campaigns,
  fragebogenKuehler,
  fragebogenMain,
  fragebogenMhd,
  markets,
  visitAnswerMatrixCells,
  visitAnswerOptions,
  visitAnswerPhotoTags,
  visitAnswerPhotos,
  visitAnswers,
  visitQuestionComments,
  visitSessionQuestions,
  visitSessionSections,
  visitSessions,
  users,
} from "../lib/schema.js";

const campaignSectionSchema = z.enum(["standard", "flex", "billa", "kuehler", "mhd"]);
const campaignStatusSchema = z.enum(["active", "scheduled", "inactive"]);
const scheduleTypeSchema = z.enum(["always", "scheduled"]);
const campaignAssignmentSchema = z
  .object({
    marketId: z.string().uuid(),
    gmUserId: z.string().uuid().nullable().optional(),
    gmNameRaw: z.string().optional(),
    visitTargetCount: z.coerce.number().int().min(1).optional(),
  })
  .strict();

type CampaignSection = z.infer<typeof campaignSectionSchema>;

const createCampaignSchema = z
  .object({
    name: z.string().trim().min(1),
    section: campaignSectionSchema,
    status: campaignStatusSchema.default("inactive"),
    scheduleType: scheduleTypeSchema.default("always"),
    startDate: z.string().optional(),
    endDate: z.string().optional(),
    currentFragebogenId: z.string().uuid().optional(),
    marketIds: z.array(z.string().uuid()).default([]),
    assignments: z.array(campaignAssignmentSchema).optional(),
  })
  .strict();

const updateCampaignSchema = z
  .object({
    name: z.string().trim().min(1).optional(),
    status: campaignStatusSchema.optional(),
    scheduleType: scheduleTypeSchema.optional(),
    startDate: z.string().optional(),
    endDate: z.string().optional(),
    currentFragebogenId: z.string().uuid().nullable().optional(),
  })
  .strict();

const assignMarketsSchema = z
  .object({
    marketIds: z.array(z.string().uuid()).default([]),
    assignments: z.array(campaignAssignmentSchema).optional(),
  })
  .refine((value) => value.marketIds.length > 0 || (value.assignments?.length ?? 0) > 0, {
    message: "Mindestens ein Markt muss zugewiesen werden.",
    path: ["marketIds"],
  })
  .strict();

const switchFragebogenSchema = z
  .object({
    toFragebogenId: z.string().uuid(),
  })
  .strict();

const migrateMarketsSchema = z
  .object({
    moves: z
      .array(
        z
          .object({
            marketId: z.string().uuid(),
            fromCampaignId: z.string().uuid(),
            gmUserId: z.string().uuid().nullable().optional(),
            reason: z.string().trim().min(1).max(300).optional(),
          })
          .strict(),
      )
      .min(1),
  })
  .strict();

class CampaignDomainError extends Error {
  code:
    | "invalid_id"
    | "invalid_payload"
    | "schedule_invalid"
    | "market_missing"
    | "gm_missing"
    | "campaign_market_overlap"
    | "fragebogen_mismatch"
    | "campaign_not_found";
  status: number;

  constructor(
    code:
      | "invalid_id"
      | "invalid_payload"
      | "schedule_invalid"
      | "market_missing"
      | "gm_missing"
      | "campaign_market_overlap"
      | "fragebogen_mismatch"
      | "campaign_not_found",
    status: number,
    message: string,
  ) {
    super(message);
    this.code = code;
    this.status = status;
  }
}

type CampaignAssignmentConflict = {
  marketId: string;
  marketName: string;
  section: CampaignSection;
  existingCampaignId: string;
  existingCampaignName: string;
  existingScheduleType: "always" | "scheduled";
  existingStartDate: string | null;
  existingEndDate: string | null;
  existingPeriodLabel: string;
  existingGmUserId: string | null;
  existingGmName: string | null;
};

class CampaignOverlapConflictError extends Error {
  status = 409;
  code = "campaign_market_overlap" as const;
  conflicts: CampaignAssignmentConflict[];

  constructor(conflicts: CampaignAssignmentConflict[]) {
    super("Mindestens ein Markt ist bereits in einer überlappenden Kampagne derselben Sektion aktiv.");
    this.conflicts = conflicts;
  }
}

function isUuid(value: string) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function normalizeUnique(values: string[]): string[] {
  return Array.from(new Set(values.map((v) => v.trim()).filter((v) => v.length > 0)));
}

function normalizeQuestionChains(chains: string[] | null | undefined): string[] {
  if (!Array.isArray(chains)) return [];
  return normalizeUnique(
    chains
      .filter((entry): entry is string => typeof entry === "string")
      .map((entry) => entry.trim().toLowerCase())
      .filter((entry) => entry.length > 0),
  );
}

function extractRuleAnswerValue(answer: (typeof visitAnswers.$inferSelect) | undefined): string | string[] | undefined {
  if (!answer) return undefined;
  const raw = (answer.valueJson as { raw?: unknown } | null | undefined)?.raw;
  if (Array.isArray(raw)) {
    return raw.filter((entry): entry is string => typeof entry === "string");
  }
  if (typeof raw === "string") return raw;
  if (raw && typeof raw === "object") {
    return JSON.stringify(raw);
  }
  if (typeof answer.valueText === "string" && answer.valueText.length > 0) return answer.valueText;
  if (answer.valueNumber != null) return String(answer.valueNumber);
  return undefined;
}

function calculateDurationMinutes(startedAt: Date | null, submittedAt: Date | null): number | null {
  if (!startedAt || !submittedAt) return null;
  const deltaMs = submittedAt.getTime() - startedAt.getTime();
  if (!Number.isFinite(deltaMs) || deltaMs < 0) return null;
  return Math.round(deltaMs / 60000);
}

function parseIsoDateOrThrow(raw: string, field: "startDate" | "endDate"): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw new CampaignDomainError("schedule_invalid", 400, `${field} muss im Format YYYY-MM-DD sein.`);
  }
  const parsed = new Date(`${raw}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) {
    throw new CampaignDomainError("schedule_invalid", 400, `${field} ist ungueltig.`);
  }
  const canonical = parsed.toISOString().slice(0, 10);
  if (canonical !== raw) {
    throw new CampaignDomainError("schedule_invalid", 400, `${field} ist ungueltig.`);
  }
  return raw;
}

function normalizeSchedule(input: {
  scheduleType: "always" | "scheduled";
  startDate?: string | undefined;
  endDate?: string | undefined;
}) {
  if (input.scheduleType === "always") {
    return { scheduleType: "always" as const, startDate: null, endDate: null };
  }
  if (!input.startDate || !input.endDate) {
    throw new CampaignDomainError("schedule_invalid", 400, "startDate und endDate sind fuer scheduleType=scheduled erforderlich.");
  }
  const startDate = parseIsoDateOrThrow(input.startDate, "startDate");
  const endDate = parseIsoDateOrThrow(input.endDate, "endDate");
  if (startDate > endDate) {
    throw new CampaignDomainError("schedule_invalid", 400, "startDate darf nicht nach endDate liegen.");
  }
  return { scheduleType: "scheduled" as const, startDate, endDate };
}

async function ensureMarketsExist(marketIds: string[]) {
  const uniqueIds = Array.from(new Set(marketIds));
  if (uniqueIds.length === 0) return;
  const existing = await db
    .select({ id: markets.id })
    .from(markets)
    .where(and(inArray(markets.id, uniqueIds), eq(markets.isDeleted, false)));
  const found = new Set(existing.map((row) => row.id));
  const missing = uniqueIds.filter((id) => !found.has(id));
  if (missing.length > 0) {
    throw new CampaignDomainError("market_missing", 400, "Mindestens ein Markt existiert nicht oder ist geloescht.");
  }
}

type CampaignAssignmentInput = {
  marketId: string;
  gmUserId: string | null;
  gmNameRaw?: string;
  visitTargetCount: number;
};

type CampaignAssignmentDraftInput = {
  marketId: string;
  gmUserId?: string | null | undefined;
  gmNameRaw?: string | undefined;
  visitTargetCount?: number | undefined;
};

type CampaignScheduleWindow = {
  scheduleType: "always" | "scheduled";
  startDate: string | null;
  endDate: string | null;
};

function toPeriodLabel(window: CampaignScheduleWindow): string {
  if (window.scheduleType === "always") return "Immer aktiv";
  if (!window.startDate || !window.endDate) return "Geplant";
  return `${window.startDate} - ${window.endDate}`;
}

function windowsOverlap(left: CampaignScheduleWindow, right: CampaignScheduleWindow): boolean {
  if (left.scheduleType === "always" || right.scheduleType === "always") return true;
  if (!left.startDate || !left.endDate || !right.startDate || !right.endDate) return false;
  return !(left.endDate < right.startDate || right.endDate < left.startDate);
}

function normalizeAssignments(input: { marketIds?: string[]; assignments?: CampaignAssignmentDraftInput[] }) {
  const normalized = new Map<string, CampaignAssignmentInput>();
  const assignmentInput = input.assignments ?? [];
  const hasExplicitAssignments = assignmentInput.length > 0;

  if (!hasExplicitAssignments) {
    for (const marketId of input.marketIds ?? []) {
      const key = `${marketId}:__unassigned__`;
      const existing = normalized.get(key);
      if (existing) {
        existing.visitTargetCount += 1;
        continue;
      }
      normalized.set(key, { marketId, gmUserId: null, visitTargetCount: 1 });
    }
  }

  for (const assignment of assignmentInput) {
    if (!assignment.marketId) continue;
    const gmUserId = assignment.gmUserId ?? null;
    const key = `${assignment.marketId}:${gmUserId ?? "__unassigned__"}`;
    const count = assignment.visitTargetCount ?? 1;
    const existing = normalized.get(key);
    if (existing) {
      existing.visitTargetCount += count;
      if (assignment.gmNameRaw) {
        existing.gmNameRaw = assignment.gmNameRaw;
      }
      continue;
    }
    normalized.set(key, {
      marketId: assignment.marketId,
      gmUserId,
      visitTargetCount: count,
      ...(assignment.gmNameRaw ? { gmNameRaw: assignment.gmNameRaw } : {}),
    });
  }
  return Array.from(normalized.values());
}

async function ensureGmUsersExist(gmUserIds: Array<string | null>) {
  const uniqueIds = Array.from(new Set(gmUserIds.filter((id): id is string => Boolean(id))));
  if (uniqueIds.length === 0) return;
  const existing = await db
    .select({ id: users.id })
    .from(users)
    .where(
      and(
        inArray(users.id, uniqueIds),
        eq(users.role, "gm"),
        eq(users.isActive, true),
        isNull(users.deletedAt),
      ),
    );
  const found = new Set(existing.map((row) => row.id));
  const missing = uniqueIds.filter((id) => !found.has(id));
  if (missing.length > 0) {
    throw new CampaignDomainError("gm_missing", 400, "Mindestens ein zugewiesener GM existiert nicht oder ist inaktiv.");
  }
}

async function findCampaignAssignmentConflicts(input: {
  targetCampaignId?: string;
  section: CampaignSection;
  targetStatus: "active" | "scheduled" | "inactive";
  targetWindow: CampaignScheduleWindow;
  assignments: CampaignAssignmentInput[];
}): Promise<CampaignAssignmentConflict[]> {
  if (input.targetStatus !== "active") return [];
  if (input.section === "flex") return [];
  const marketIds = Array.from(new Set(input.assignments.map((assignment) => assignment.marketId)));
  if (marketIds.length === 0) return [];

  const query = db
    .select({
      marketId: campaignMarketAssignments.marketId,
      marketName: markets.name,
      section: campaigns.section,
      campaignId: campaigns.id,
      campaignName: campaigns.name,
      scheduleType: campaigns.scheduleType,
      startDate: campaigns.startDate,
      endDate: campaigns.endDate,
      gmUserId: campaignMarketAssignments.gmUserId,
      gmFirstName: users.firstName,
      gmLastName: users.lastName,
    })
    .from(campaignMarketAssignments)
    .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
    .innerJoin(markets, eq(markets.id, campaignMarketAssignments.marketId))
    .leftJoin(users, eq(users.id, campaignMarketAssignments.gmUserId))
    .where(
      and(
        inArray(campaignMarketAssignments.marketId, marketIds),
        eq(campaignMarketAssignments.isDeleted, false),
        eq(campaigns.isDeleted, false),
        eq(campaigns.section, input.section),
        eq(campaigns.status, "active"),
        input.targetCampaignId ? ne(campaigns.id, input.targetCampaignId) : sql`true`,
      ),
    );

  const rows = await query;
  const conflicts: CampaignAssignmentConflict[] = [];
  for (const row of rows) {
    const existingWindow: CampaignScheduleWindow = {
      scheduleType: row.scheduleType,
      startDate: row.startDate ? String(row.startDate) : null,
      endDate: row.endDate ? String(row.endDate) : null,
    };
    if (!windowsOverlap(input.targetWindow, existingWindow)) continue;
    conflicts.push({
      marketId: row.marketId,
      marketName: row.marketName,
      section: row.section,
      existingCampaignId: row.campaignId,
      existingCampaignName: row.campaignName,
      existingScheduleType: row.scheduleType,
      existingStartDate: existingWindow.startDate,
      existingEndDate: existingWindow.endDate,
      existingPeriodLabel: toPeriodLabel(existingWindow),
      existingGmUserId: row.gmUserId ?? null,
      existingGmName: row.gmFirstName && row.gmLastName ? `${row.gmFirstName} ${row.gmLastName}` : null,
    });
  }

  const deduped = new Map<string, CampaignAssignmentConflict>();
  for (const conflict of conflicts) {
    const key = `${conflict.marketId}:${conflict.existingCampaignId}`;
    if (!deduped.has(key)) deduped.set(key, conflict);
  }
  return Array.from(deduped.values());
}

async function resolveAuditUserId(rawUserId: string | undefined): Promise<string | null> {
  if (!rawUserId || !isUuid(rawUserId)) return null;
  const [row] = await db
    .select({ id: users.id })
    .from(users)
    .where(and(eq(users.id, rawUserId), isNull(users.deletedAt)))
    .limit(1);
  return row?.id ?? null;
}

async function ensureFragebogenMatchesSection(section: CampaignSection, fragebogenId: string) {
  if (section === "kuehler") {
    const [row] = await db
      .select({ id: fragebogenKuehler.id })
      .from(fragebogenKuehler)
      .where(and(eq(fragebogenKuehler.id, fragebogenId), eq(fragebogenKuehler.isDeleted, false)))
      .limit(1);
    if (!row) throw new CampaignDomainError("fragebogen_mismatch", 400, "Fragebogen passt nicht zur Sektion oder ist geloescht.");
    return;
  }
  if (section === "mhd") {
    const [row] = await db
      .select({ id: fragebogenMhd.id })
      .from(fragebogenMhd)
      .where(and(eq(fragebogenMhd.id, fragebogenId), eq(fragebogenMhd.isDeleted, false)))
      .limit(1);
    if (!row) throw new CampaignDomainError("fragebogen_mismatch", 400, "Fragebogen passt nicht zur Sektion oder ist geloescht.");
    return;
  }

  const [row] = await db
    .select({ id: fragebogenMain.id })
    .from(fragebogenMain)
    .where(
      and(
        eq(fragebogenMain.id, fragebogenId),
        eq(fragebogenMain.isDeleted, false),
        sql`${fragebogenMain.sectionKeywords} @> ARRAY[${section}]::fragebogen_main_section[]`,
      ),
    )
    .limit(1);
  if (!row) throw new CampaignDomainError("fragebogen_mismatch", 400, "Fragebogen passt nicht zur Sektion oder ist geloescht.");
}

async function getFragebogenNameBySection(section: CampaignSection, fragebogenId: string | null) {
  if (!fragebogenId) return null;
  if (section === "kuehler") {
    const [row] = await db
      .select({ name: fragebogenKuehler.name })
      .from(fragebogenKuehler)
      .where(and(eq(fragebogenKuehler.id, fragebogenId), eq(fragebogenKuehler.isDeleted, false)))
      .limit(1);
    return row?.name ?? null;
  }
  if (section === "mhd") {
    const [row] = await db
      .select({ name: fragebogenMhd.name })
      .from(fragebogenMhd)
      .where(and(eq(fragebogenMhd.id, fragebogenId), eq(fragebogenMhd.isDeleted, false)))
      .limit(1);
    return row?.name ?? null;
  }
  const [row] = await db
    .select({ name: fragebogenMain.name })
    .from(fragebogenMain)
    .where(
      and(
        eq(fragebogenMain.id, fragebogenId),
        eq(fragebogenMain.isDeleted, false),
        sql`${fragebogenMain.sectionKeywords} @> ARRAY[${section}]::fragebogen_main_section[]`,
      ),
    )
    .limit(1);
  return row?.name ?? null;
}

async function mapCampaignRows(rows: Array<typeof campaigns.$inferSelect>) {
  if (rows.length === 0) return [];
  const campaignIds = rows.map((row) => row.id);
  const assignments = await db
    .select()
    .from(campaignMarketAssignments)
    .where(and(inArray(campaignMarketAssignments.campaignId, campaignIds), eq(campaignMarketAssignments.isDeleted, false)))
    .orderBy(asc(campaignMarketAssignments.assignedAt));

  const historyRows = await db
    .select()
    .from(campaignFragebogenHistory)
    .where(and(inArray(campaignFragebogenHistory.campaignId, campaignIds), eq(campaignFragebogenHistory.isDeleted, false)))
    .orderBy(desc(campaignFragebogenHistory.changedAt));

  const marketIdsByCampaign = new Map<string, string[]>();
  const assignmentRowsByCampaign = new Map<
    string,
    Array<{ marketId: string; gmUserId: string | null; visitTargetCount: number; currentVisitsCount: number }>
  >();
  for (const row of assignments) {
    const current = marketIdsByCampaign.get(row.campaignId) ?? [];
    current.push(row.marketId);
    marketIdsByCampaign.set(row.campaignId, current);
    const assignmentRows = assignmentRowsByCampaign.get(row.campaignId) ?? [];
    assignmentRows.push({
      marketId: row.marketId,
      gmUserId: row.gmUserId ?? null,
      visitTargetCount: row.visitTargetCount,
      currentVisitsCount: row.currentVisitsCount,
    });
    assignmentRowsByCampaign.set(row.campaignId, assignmentRows);
  }

  const gmUserIds = Array.from(
    new Set(
      assignments
        .map((row) => row.gmUserId)
        .filter((id): id is string => Boolean(id)),
    ),
  );
  const gmNameById = new Map<string, string>();
  if (gmUserIds.length > 0) {
    const gmRows = await db
      .select({ id: users.id, firstName: users.firstName, lastName: users.lastName })
      .from(users)
      .where(inArray(users.id, gmUserIds));
    for (const gmRow of gmRows) {
      gmNameById.set(gmRow.id, `${gmRow.firstName} ${gmRow.lastName}`.trim());
    }
  }

  const historyByCampaign = new Map<string, Array<typeof campaignFragebogenHistory.$inferSelect>>();
  for (const row of historyRows) {
    const current = historyByCampaign.get(row.campaignId) ?? [];
    current.push(row);
    historyByCampaign.set(row.campaignId, current);
  }

  const kuehlerIds = rows
    .filter((row) => row.section === "kuehler" && row.currentFragebogenId)
    .map((row) => row.currentFragebogenId as string);
  const mhdIds = rows
    .filter((row) => row.section === "mhd" && row.currentFragebogenId)
    .map((row) => row.currentFragebogenId as string);
  const standardIds = rows
    .filter((row) => row.section === "standard" && row.currentFragebogenId)
    .map((row) => row.currentFragebogenId as string);
  const flexIds = rows
    .filter((row) => row.section === "flex" && row.currentFragebogenId)
    .map((row) => row.currentFragebogenId as string);
  const billaIds = rows
    .filter((row) => row.section === "billa" && row.currentFragebogenId)
    .map((row) => row.currentFragebogenId as string);

  const fbNameBySectionAndId = new Map<string, string>();
  if (kuehlerIds.length > 0) {
    const kuehlerRows = await db
      .select({ id: fragebogenKuehler.id, name: fragebogenKuehler.name })
      .from(fragebogenKuehler)
      .where(and(inArray(fragebogenKuehler.id, Array.from(new Set(kuehlerIds))), eq(fragebogenKuehler.isDeleted, false)));
    for (const row of kuehlerRows) {
      fbNameBySectionAndId.set(`kuehler:${row.id}`, row.name);
    }
  }
  if (mhdIds.length > 0) {
    const mhdRows = await db
      .select({ id: fragebogenMhd.id, name: fragebogenMhd.name })
      .from(fragebogenMhd)
      .where(and(inArray(fragebogenMhd.id, Array.from(new Set(mhdIds))), eq(fragebogenMhd.isDeleted, false)));
    for (const row of mhdRows) {
      fbNameBySectionAndId.set(`mhd:${row.id}`, row.name);
    }
  }
  const mainSections: Array<{ section: "standard" | "flex" | "billa"; ids: string[] }> = [
    { section: "standard", ids: standardIds },
    { section: "flex", ids: flexIds },
    { section: "billa", ids: billaIds },
  ];
  for (const mainSection of mainSections) {
    if (mainSection.ids.length === 0) continue;
    const mainRows = await db
      .select({ id: fragebogenMain.id, name: fragebogenMain.name })
      .from(fragebogenMain)
      .where(
        and(
          inArray(fragebogenMain.id, Array.from(new Set(mainSection.ids))),
          eq(fragebogenMain.isDeleted, false),
          sql`${fragebogenMain.sectionKeywords} @> ARRAY[${mainSection.section}]::fragebogen_main_section[]`,
        ),
      );
    for (const row of mainRows) {
      fbNameBySectionAndId.set(`${mainSection.section}:${row.id}`, row.name);
    }
  }

  return rows.map((row) => ({
    id: row.id,
    name: row.name,
    section: row.section,
    currentFragebogenId: row.currentFragebogenId,
    currentFragebogenName: row.currentFragebogenId ? (fbNameBySectionAndId.get(`${row.section}:${row.currentFragebogenId}`) ?? null) : null,
    status: row.status,
    scheduleType: row.scheduleType,
    startDate: row.startDate ? String(row.startDate) : null,
    endDate: row.endDate ? String(row.endDate) : null,
    marketIds: normalizeUnique(marketIdsByCampaign.get(row.id) ?? []),
    assignments: (assignmentRowsByCampaign.get(row.id) ?? []).map((assignment) => ({
      marketId: assignment.marketId,
      gmUserId: assignment.gmUserId,
      gmName: assignment.gmUserId ? (gmNameById.get(assignment.gmUserId) ?? null) : null,
      visitTargetCount: assignment.visitTargetCount,
      currentVisitsCount: assignment.currentVisitsCount,
    })),
    history: (historyByCampaign.get(row.id) ?? []).map((historyRow) => ({
      id: historyRow.id,
      fromFragebogenId: historyRow.fromFragebogenId,
      toFragebogenId: historyRow.toFragebogenId,
      changedAt: historyRow.changedAt.toISOString(),
    })),
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  }));
}

type DbTransaction = Parameters<typeof db.transaction>[0] extends (tx: infer T) => unknown ? T : never;

async function mergeCampaignAssignment(
  tx: DbTransaction,
  input: {
    campaignId: string;
    marketId: string;
    gmUserId: string | null;
    visitTargetCountDelta: number;
    now: Date;
    auditUserId: string | null;
  },
) {
  const gmFilter =
    input.gmUserId === null
      ? isNull(campaignMarketAssignments.gmUserId)
      : eq(campaignMarketAssignments.gmUserId, input.gmUserId);

  await tx.execute(sql`SELECT ${campaigns.id} FROM ${campaigns} WHERE ${campaigns.id} = ${input.campaignId} FOR UPDATE`);

  const updated = await tx
    .update(campaignMarketAssignments)
    .set({
      visitTargetCount: sql`${campaignMarketAssignments.visitTargetCount} + ${input.visitTargetCountDelta}`,
      assignedAt: input.now,
      assignedByUserId: input.auditUserId,
      isDeleted: false,
      deletedAt: null,
      updatedAt: input.now,
    })
    .where(
      and(
        eq(campaignMarketAssignments.campaignId, input.campaignId),
        eq(campaignMarketAssignments.marketId, input.marketId),
        gmFilter,
        eq(campaignMarketAssignments.isDeleted, false),
      ),
    )
    .returning({ id: campaignMarketAssignments.id });
  if (updated.length > 0) return;

  await tx.insert(campaignMarketAssignments).values({
    campaignId: input.campaignId,
    marketId: input.marketId,
    gmUserId: input.gmUserId,
    visitTargetCount: input.visitTargetCountDelta,
    currentVisitsCount: 0,
    assignedAt: input.now,
    assignedByUserId: input.auditUserId,
    isDeleted: false,
    deletedAt: null,
    createdAt: input.now,
    updatedAt: input.now,
  });
}

async function buildCampaignMarketVisitSummaries(campaignId: string) {
  const assignedRows = await db
    .select({
      marketId: campaignMarketAssignments.marketId,
    })
    .from(campaignMarketAssignments)
    .where(
      and(
        eq(campaignMarketAssignments.campaignId, campaignId),
        eq(campaignMarketAssignments.isDeleted, false),
      ),
    );
  const assignedMarketIds = normalizeUnique(assignedRows.map((row) => row.marketId));
  if (assignedMarketIds.length === 0) return [];

  const submittedSectionRows = await db
    .select({
      marketId: visitSessions.marketId,
      sessionId: visitSessions.id,
      gmUserId: visitSessions.gmUserId,
      startedAt: visitSessions.startedAt,
      submittedAt: visitSessions.submittedAt,
      createdAt: visitSessions.createdAt,
    })
    .from(visitSessionSections)
    .innerJoin(visitSessions, eq(visitSessions.id, visitSessionSections.visitSessionId))
    .where(
      and(
        eq(visitSessionSections.campaignId, campaignId),
        eq(visitSessionSections.isDeleted, false),
        eq(visitSessions.isDeleted, false),
        eq(visitSessions.status, "submitted"),
        inArray(visitSessions.marketId, assignedMarketIds),
      ),
    )
    .orderBy(desc(visitSessions.submittedAt), desc(visitSessions.createdAt));

  const latestByMarket = new Map<string, (typeof submittedSectionRows)[number]>();
  for (const row of submittedSectionRows) {
    if (!latestByMarket.has(row.marketId)) {
      latestByMarket.set(row.marketId, row);
    }
  }

  const selectedSessionIds = normalizeUnique(Array.from(latestByMarket.values()).map((row) => row.sessionId));
  if (selectedSessionIds.length === 0) {
    return assignedMarketIds.map((marketId) => ({
      marketId,
      hasSubmittedVisit: false,
      sessionId: null,
      startedAt: null,
      submittedAt: null,
      durationMinutes: null,
      gmUserId: null,
      gmName: null,
      sections: [],
    }));
  }

  const sections = await db
    .select()
    .from(visitSessionSections)
    .where(
      and(
        inArray(visitSessionSections.visitSessionId, selectedSessionIds),
        eq(visitSessionSections.campaignId, campaignId),
        eq(visitSessionSections.isDeleted, false),
      ),
    )
    .orderBy(asc(visitSessionSections.orderIndex));
  const sectionIds = sections.map((section) => section.id);
  const questions =
    sectionIds.length === 0
      ? []
      : await db
          .select()
          .from(visitSessionQuestions)
          .where(
            and(
              inArray(visitSessionQuestions.visitSessionSectionId, sectionIds),
              eq(visitSessionQuestions.isDeleted, false),
            ),
          )
          .orderBy(asc(visitSessionQuestions.orderIndex));
  const questionIds = questions.map((question) => question.id);

  const answers =
    questionIds.length === 0
      ? []
      : await db
          .select()
          .from(visitAnswers)
          .where(and(inArray(visitAnswers.visitSessionQuestionId, questionIds), eq(visitAnswers.isDeleted, false)));
  const comments =
    questionIds.length === 0
      ? []
      : await db
          .select()
          .from(visitQuestionComments)
          .where(
            and(
              inArray(visitQuestionComments.visitSessionQuestionId, questionIds),
              eq(visitQuestionComments.isDeleted, false),
            ),
          );
  const answerIds = answers.map((answer) => answer.id);
  const answerOptions =
    answerIds.length === 0
      ? []
      : await db
          .select()
          .from(visitAnswerOptions)
          .where(and(inArray(visitAnswerOptions.visitAnswerId, answerIds), eq(visitAnswerOptions.isDeleted, false)))
          .orderBy(asc(visitAnswerOptions.orderIndex));
  const answerMatrixCells =
    answerIds.length === 0
      ? []
      : await db
          .select()
          .from(visitAnswerMatrixCells)
          .where(
            and(
              inArray(visitAnswerMatrixCells.visitAnswerId, answerIds),
              eq(visitAnswerMatrixCells.isDeleted, false),
            ),
          )
          .orderBy(asc(visitAnswerMatrixCells.orderIndex));
  const answerPhotos =
    answerIds.length === 0
      ? []
      : await db
          .select()
          .from(visitAnswerPhotos)
          .where(and(inArray(visitAnswerPhotos.visitAnswerId, answerIds), eq(visitAnswerPhotos.isDeleted, false)))
          .orderBy(asc(visitAnswerPhotos.createdAt));
  const answerPhotoIds = answerPhotos.map((photo) => photo.id);
  const answerPhotoTags =
    answerPhotoIds.length === 0
      ? []
      : await db
          .select()
          .from(visitAnswerPhotoTags)
          .where(
            and(
              inArray(visitAnswerPhotoTags.visitAnswerPhotoId, answerPhotoIds),
              eq(visitAnswerPhotoTags.isDeleted, false),
            ),
          )
          .orderBy(asc(visitAnswerPhotoTags.createdAt));

  const gmUserIds = normalizeUnique(
    Array.from(latestByMarket.values())
      .map((entry) => entry.gmUserId)
      .filter((entry): entry is string => Boolean(entry)),
  );
  const gmNameById = new Map<string, string>();
  if (gmUserIds.length > 0) {
    const gmRows = await db
      .select({ id: users.id, firstName: users.firstName, lastName: users.lastName })
      .from(users)
      .where(inArray(users.id, gmUserIds));
    for (const row of gmRows) {
      gmNameById.set(row.id, `${row.firstName} ${row.lastName}`.trim());
    }
  }

  const sectionsBySessionId = new Map<string, typeof sections>();
  for (const section of sections) {
    const bucket = sectionsBySessionId.get(section.visitSessionId) ?? [];
    bucket.push(section);
    sectionsBySessionId.set(section.visitSessionId, bucket);
  }
  const questionsBySectionId = new Map<string, typeof questions>();
  for (const question of questions) {
    const bucket = questionsBySectionId.get(question.visitSessionSectionId) ?? [];
    bucket.push(question);
    questionsBySectionId.set(question.visitSessionSectionId, bucket);
  }
  const answersByQuestionId = new Map(answers.map((answer) => [answer.visitSessionQuestionId, answer]));
  const commentsByQuestionId = new Map(comments.map((comment) => [comment.visitSessionQuestionId, comment]));

  const optionsByAnswerId = new Map<string, typeof answerOptions>();
  for (const row of answerOptions) {
    const bucket = optionsByAnswerId.get(row.visitAnswerId) ?? [];
    bucket.push(row);
    optionsByAnswerId.set(row.visitAnswerId, bucket);
  }
  const matrixByAnswerId = new Map<string, typeof answerMatrixCells>();
  for (const row of answerMatrixCells) {
    const bucket = matrixByAnswerId.get(row.visitAnswerId) ?? [];
    bucket.push(row);
    matrixByAnswerId.set(row.visitAnswerId, bucket);
  }
  const photosByAnswerId = new Map<string, typeof answerPhotos>();
  for (const row of answerPhotos) {
    const bucket = photosByAnswerId.get(row.visitAnswerId) ?? [];
    bucket.push(row);
    photosByAnswerId.set(row.visitAnswerId, bucket);
  }
  const tagsByPhotoId = new Map<string, typeof answerPhotoTags>();
  for (const row of answerPhotoTags) {
    const bucket = tagsByPhotoId.get(row.visitAnswerPhotoId) ?? [];
    bucket.push(row);
    tagsByPhotoId.set(row.visitAnswerPhotoId, bucket);
  }

  const byMarket = new Map<
    string,
    {
      marketId: string;
      hasSubmittedVisit: boolean;
      sessionId: string | null;
      startedAt: string | null;
      submittedAt: string | null;
      durationMinutes: number | null;
      gmUserId: string | null;
      gmName: string | null;
      sections: Array<{
        id: string;
        section: "standard" | "flex" | "billa" | "kuehler" | "mhd";
        campaignId: string;
        fragebogenId: string | null;
        fragebogenName: string;
        orderIndex: number;
        questions: Array<{
          id: string;
          questionId: string;
          moduleId: string;
          moduleName: string;
          type: string;
          text: string;
          required: boolean;
          config: Record<string, unknown>;
          rules: Array<Record<string, unknown>>;
          chains: string[];
          appliesToMarketChain: boolean;
          visibility: {
            isHiddenByChain: boolean;
            isHiddenByRule: boolean;
            isVisibleAtSubmit: boolean;
          };
          answer: {
            id: string;
            answerStatus: "unanswered" | "answered" | "invalid" | "hidden_by_rule" | "skipped";
            valueText: string | null;
            valueNumber: string | null;
            valueJson: Record<string, unknown> | null;
            isValid: boolean;
            validationError: string | null;
            version: number;
            options: Array<{ optionRole: "top" | "sub"; optionValue: string; orderIndex: number }>;
            matrixCells: Array<{
              rowKey: string;
              columnKey: string;
              cellValueText: string | null;
              cellValueDate: string | null;
              cellSelected: boolean | null;
              orderIndex: number;
            }>;
            photos: Array<{
              id: string;
              storageBucket: string;
              storagePath: string;
              mimeType: string | null;
              byteSize: number | null;
              widthPx: number | null;
              heightPx: number | null;
              sha256: string | null;
              tags: Array<{
                id: string;
                photoTagId: string | null;
                photoTagLabelSnapshot: string;
              }>;
            }>;
          } | null;
          comment: string;
        }>;
      }>;
    }
  >();

  for (const marketId of assignedMarketIds) {
    const selectedSession = latestByMarket.get(marketId);
    if (!selectedSession) {
      byMarket.set(marketId, {
        marketId,
        hasSubmittedVisit: false,
        sessionId: null,
        startedAt: null,
        submittedAt: null,
        durationMinutes: null,
        gmUserId: null,
        gmName: null,
        sections: [],
      });
      continue;
    }

    const sessionSections = sectionsBySessionId.get(selectedSession.sessionId) ?? [];
    const hydratedSections = sessionSections.map((section) => {
      const sectionQuestions = questionsBySectionId.get(section.id) ?? [];

      const applicableQuestions = sectionQuestions.filter((question) => question.appliesToMarketChainSnapshot);
      const visibilityQuestions = applicableQuestions.map((question) => ({
        id: question.id,
        questionId: question.questionId,
        rules: (question.questionRulesSnapshot ?? []) as Array<Record<string, unknown>>,
      }));
      const answerByVisitQuestionId = new Map<string, string | string[] | undefined>();
      for (const question of applicableQuestions) {
        answerByVisitQuestionId.set(question.id, extractRuleAnswerValue(answersByQuestionId.get(question.id)));
      }
      const hiddenByRule = computeHiddenQuestionIds(visibilityQuestions, answerByVisitQuestionId);

      return {
        id: section.id,
        section: section.section,
        campaignId: section.campaignId,
        fragebogenId: section.fragebogenId,
        fragebogenName: section.fragebogenNameSnapshot,
        orderIndex: section.orderIndex,
        questions: sectionQuestions.map((question) => {
          const answer = answersByQuestionId.get(question.id);
          const photos = answer ? photosByAnswerId.get(answer.id) ?? [] : [];
          const isHiddenByChain = !question.appliesToMarketChainSnapshot;
          const isHiddenByRule = !isHiddenByChain && hiddenByRule.has(question.id);
          return {
            id: question.id,
            questionId: question.questionId,
            moduleId: question.moduleId ?? "",
            moduleName: question.moduleNameSnapshot ?? "",
            type: question.questionType,
            text: question.questionTextSnapshot,
            required: question.requiredSnapshot,
            config: (question.questionConfigSnapshot ?? {}) as Record<string, unknown>,
            rules: (question.questionRulesSnapshot ?? []) as Array<Record<string, unknown>>,
            chains: normalizeQuestionChains(question.questionChainsSnapshot),
            appliesToMarketChain: Boolean(question.appliesToMarketChainSnapshot),
            visibility: {
              isHiddenByChain,
              isHiddenByRule,
              isVisibleAtSubmit: !isHiddenByChain && !isHiddenByRule,
            },
            answer: answer
              ? {
                  id: answer.id,
                  answerStatus: answer.answerStatus,
                  valueText: answer.valueText,
                  valueNumber: answer.valueNumber == null ? null : String(answer.valueNumber),
                  valueJson: (answer.valueJson as Record<string, unknown> | null) ?? null,
                  isValid: answer.isValid,
                  validationError: answer.validationError,
                  version: answer.version,
                  options: (optionsByAnswerId.get(answer.id) ?? []).map((row) => ({
                    optionRole: row.optionRole,
                    optionValue: row.optionValue,
                    orderIndex: row.orderIndex,
                  })),
                  matrixCells: (matrixByAnswerId.get(answer.id) ?? []).map((row) => ({
                    rowKey: row.rowKey,
                    columnKey: row.columnKey,
                    cellValueText: row.cellValueText,
                    cellValueDate: row.cellValueDate,
                    cellSelected: row.cellSelected,
                    orderIndex: row.orderIndex,
                  })),
                  photos: photos.map((photo) => ({
                    id: photo.id,
                    storageBucket: photo.storageBucket,
                    storagePath: photo.storagePath,
                    mimeType: photo.mimeType ?? null,
                    byteSize: photo.byteSize ?? null,
                    widthPx: photo.widthPx ?? null,
                    heightPx: photo.heightPx ?? null,
                    sha256: photo.sha256 ?? null,
                    tags: (tagsByPhotoId.get(photo.id) ?? []).map((tag) => ({
                      id: tag.id,
                      photoTagId: tag.photoTagId ?? null,
                      photoTagLabelSnapshot: tag.photoTagLabelSnapshot,
                    })),
                  })),
                }
              : null,
            comment: commentsByQuestionId.get(question.id)?.commentText ?? "",
          };
        }),
      };
    });

    byMarket.set(marketId, {
      marketId,
      hasSubmittedVisit: true,
      sessionId: selectedSession.sessionId,
      startedAt: selectedSession.startedAt?.toISOString() ?? null,
      submittedAt: selectedSession.submittedAt?.toISOString() ?? null,
      durationMinutes: calculateDurationMinutes(selectedSession.startedAt, selectedSession.submittedAt),
      gmUserId: selectedSession.gmUserId ?? null,
      gmName: selectedSession.gmUserId ? gmNameById.get(selectedSession.gmUserId) ?? null : null,
      sections: hydratedSections,
    });
  }

  return assignedMarketIds.map((marketId) => {
    const found = byMarket.get(marketId);
    if (found) return found;
    return {
      marketId,
      hasSubmittedVisit: false,
      sessionId: null,
      startedAt: null,
      submittedAt: null,
      durationMinutes: null,
      gmUserId: null,
      gmName: null,
      sections: [],
    };
  });
}

function respondDomainError(res: Response, error: CampaignDomainError) {
  return res.status(error.status).json({ error: error.message, code: error.code });
}

const adminCampaignsRouter = Router();
adminCampaignsRouter.use(requireAuth(["admin"]));

adminCampaignsRouter.get("/campaigns", async (_req, res, next) => {
  try {
    const rows = await db.select().from(campaigns).where(eq(campaigns.isDeleted, false)).orderBy(desc(campaigns.createdAt));
    const mapped = await mapCampaignRows(rows);
    res.status(200).json({ campaigns: mapped });
  } catch (error) {
    next(error);
  }
});

adminCampaignsRouter.get("/campaigns/:id/market-visits", async (req, res, next) => {
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      res.status(400).json({ error: "Ungueltige Kampagnen-ID.", code: "invalid_id" });
      return;
    }

    const [campaign] = await db
      .select()
      .from(campaigns)
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .limit(1);
    if (!campaign) {
      res.status(404).json({ error: "Kampagne nicht gefunden.", code: "campaign_not_found" });
      return;
    }

    const summaries = await buildCampaignMarketVisitSummaries(campaignId);
    res.status(200).json({
      campaignId,
      markets: summaries,
    });
  } catch (error) {
    next(error);
  }
});

adminCampaignsRouter.post("/campaigns", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = createCampaignSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Kampagne.", code: "invalid_payload" });
      return;
    }

    const now = new Date();
    const auditUserId = await resolveAuditUserId(req.authUser?.appUserId);
    const schedule = normalizeSchedule(parsed.data);
    const assignments = normalizeAssignments({
      marketIds: parsed.data.marketIds,
      ...(parsed.data.assignments ? { assignments: parsed.data.assignments } : {}),
    });
    const marketIds = assignments.map((assignment) => assignment.marketId);
    if (parsed.data.currentFragebogenId) {
      await ensureFragebogenMatchesSection(parsed.data.section, parsed.data.currentFragebogenId);
    }
    await ensureMarketsExist(marketIds);
    await ensureGmUsersExist(assignments.map((assignment) => assignment.gmUserId));
    const conflicts = await findCampaignAssignmentConflicts({
      section: parsed.data.section,
      targetStatus: parsed.data.status,
      targetWindow: {
        scheduleType: schedule.scheduleType,
        startDate: schedule.startDate,
        endDate: schedule.endDate,
      },
      assignments,
    });
    if (conflicts.length > 0) {
      throw new CampaignOverlapConflictError(conflicts);
    }

    const campaignId = await db.transaction(async (tx) => {
      const [created] = await tx
        .insert(campaigns)
        .values({
          name: parsed.data.name,
          section: parsed.data.section,
          currentFragebogenId: parsed.data.currentFragebogenId ?? null,
          status: parsed.data.status,
          scheduleType: schedule.scheduleType,
          startDate: schedule.startDate,
          endDate: schedule.endDate,
          isDeleted: false,
          deletedAt: null,
          createdAt: now,
          updatedAt: now,
        })
        .returning();
      if (!created) throw new CampaignDomainError("invalid_payload", 400, "Kampagne konnte nicht erstellt werden.");

      if (assignments.length > 0) {
        await tx.insert(campaignMarketAssignments).values(
          assignments.map((assignment) => ({
            campaignId: created.id,
            marketId: assignment.marketId,
            gmUserId: assignment.gmUserId,
            visitTargetCount: assignment.visitTargetCount,
            currentVisitsCount: 0,
            assignedAt: now,
            assignedByUserId: auditUserId,
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          })),
        );
      }

      if (parsed.data.currentFragebogenId) {
        await tx.insert(campaignFragebogenHistory).values({
          campaignId: created.id,
          fromFragebogenId: null,
          toFragebogenId: parsed.data.currentFragebogenId,
          changedAt: now,
          changedByUserId: auditUserId,
          isDeleted: false,
          deletedAt: null,
          createdAt: now,
          updatedAt: now,
        });
      }
      return created.id;
    });

    const [row] = await db
      .select()
      .from(campaigns)
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .limit(1);
    if (!row) {
      res.status(404).json({ error: "Kampagne nicht gefunden.", code: "campaign_not_found" });
      return;
    }
    const [campaign] = await mapCampaignRows([row]);
    res.status(201).json({ campaign });
  } catch (error) {
    if (error instanceof CampaignOverlapConflictError) {
      res.status(error.status).json({ error: error.message, code: error.code, conflicts: error.conflicts });
      return;
    }
    if (error instanceof CampaignDomainError) {
      respondDomainError(res, error);
      return;
    }
    next(error);
  }
});

adminCampaignsRouter.patch("/campaigns/:id", async (req: AuthedRequest, res, next) => {
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      res.status(400).json({ error: "Ungueltige Kampagnen-ID.", code: "invalid_id" });
      return;
    }
    const parsed = updateCampaignSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Kampagnen-Aenderung.", code: "invalid_payload" });
      return;
    }

    const now = new Date();
    const auditUserId = await resolveAuditUserId(req.authUser?.appUserId);
    await db.transaction(async (tx) => {
      const [existing] = await tx
        .select()
        .from(campaigns)
        .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
        .limit(1);
      if (!existing) throw new CampaignDomainError("campaign_not_found", 404, "Kampagne nicht gefunden.");

      const schedule = normalizeSchedule({
        scheduleType: parsed.data.scheduleType ?? existing.scheduleType,
        startDate: parsed.data.startDate ?? (existing.startDate ? String(existing.startDate) : undefined),
        endDate: parsed.data.endDate ?? (existing.endDate ? String(existing.endDate) : undefined),
      });
      const nextStatus = parsed.data.status ?? existing.status;

      const activeAssignments = await tx
        .select({
          marketId: campaignMarketAssignments.marketId,
          gmUserId: campaignMarketAssignments.gmUserId,
        })
        .from(campaignMarketAssignments)
        .where(
          and(
            eq(campaignMarketAssignments.campaignId, campaignId),
            eq(campaignMarketAssignments.isDeleted, false),
          ),
        );
      const conflicts = await findCampaignAssignmentConflicts({
        targetCampaignId: campaignId,
        section: existing.section,
        targetStatus: nextStatus,
        targetWindow: {
          scheduleType: schedule.scheduleType,
          startDate: schedule.startDate,
          endDate: schedule.endDate,
        },
        assignments: activeAssignments.map((assignment) => ({
          marketId: assignment.marketId,
          gmUserId: assignment.gmUserId ?? null,
          visitTargetCount: 1,
        })),
      });
      if (conflicts.length > 0) {
        throw new CampaignOverlapConflictError(conflicts);
      }

      const hasSwitchPayload = Object.prototype.hasOwnProperty.call(parsed.data, "currentFragebogenId");
      const nextFragebogenId = hasSwitchPayload ? parsed.data.currentFragebogenId ?? null : existing.currentFragebogenId;
      if (nextFragebogenId) {
        await ensureFragebogenMatchesSection(existing.section, nextFragebogenId);
      }

      await tx
        .update(campaigns)
        .set({
          name: parsed.data.name ?? existing.name,
          status: nextStatus,
          scheduleType: schedule.scheduleType,
          startDate: schedule.startDate,
          endDate: schedule.endDate,
          currentFragebogenId: nextFragebogenId,
          updatedAt: now,
        })
        .where(eq(campaigns.id, campaignId));

      if (hasSwitchPayload && existing.currentFragebogenId !== nextFragebogenId && nextFragebogenId) {
        await tx.insert(campaignFragebogenHistory).values({
          campaignId,
          fromFragebogenId: existing.currentFragebogenId,
          toFragebogenId: nextFragebogenId,
          changedAt: now,
          changedByUserId: auditUserId,
          isDeleted: false,
          deletedAt: null,
          createdAt: now,
          updatedAt: now,
        });
      }
    });

    const [row] = await db
      .select()
      .from(campaigns)
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .limit(1);
    if (!row) {
      res.status(404).json({ error: "Kampagne nicht gefunden.", code: "campaign_not_found" });
      return;
    }
    const [campaign] = await mapCampaignRows([row]);
    res.status(200).json({ campaign });
  } catch (error) {
    if (error instanceof CampaignOverlapConflictError) {
      res.status(error.status).json({ error: error.message, code: error.code, conflicts: error.conflicts });
      return;
    }
    if (error instanceof CampaignDomainError) {
      respondDomainError(res, error);
      return;
    }
    next(error);
  }
});

adminCampaignsRouter.patch("/campaigns/:id/delete", async (req, res, next) => {
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      res.status(400).json({ error: "Ungueltige Kampagnen-ID.", code: "invalid_id" });
      return;
    }
    const now = new Date();
    const deleted = await db
      .update(campaigns)
      .set({ isDeleted: true, deletedAt: now, updatedAt: now })
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .returning({ id: campaigns.id });
    res.status(200).json({ ok: true, alreadyDeleted: deleted.length === 0 });
  } catch (error) {
    next(error);
  }
});

adminCampaignsRouter.post("/campaigns/:id/markets", async (req: AuthedRequest, res, next) => {
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      res.status(400).json({ error: "Ungueltige Kampagnen-ID.", code: "invalid_id" });
      return;
    }
    const parsed = assignMarketsSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Marktzuweisung.", code: "invalid_payload" });
      return;
    }
    const assignments = normalizeAssignments({
      marketIds: parsed.data.marketIds,
      ...(parsed.data.assignments ? { assignments: parsed.data.assignments } : {}),
    });
    const marketIds = assignments.map((assignment) => assignment.marketId);
    await ensureMarketsExist(marketIds);
    await ensureGmUsersExist(assignments.map((assignment) => assignment.gmUserId));
    const [campaignForValidation] = await db
      .select({
        id: campaigns.id,
        section: campaigns.section,
        status: campaigns.status,
        scheduleType: campaigns.scheduleType,
        startDate: campaigns.startDate,
        endDate: campaigns.endDate,
      })
      .from(campaigns)
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .limit(1);
    if (!campaignForValidation) {
      throw new CampaignDomainError("campaign_not_found", 404, "Kampagne nicht gefunden.");
    }
    const conflicts = await findCampaignAssignmentConflicts({
      targetCampaignId: campaignId,
      section: campaignForValidation.section,
      targetStatus: campaignForValidation.status,
      targetWindow: {
        scheduleType: campaignForValidation.scheduleType,
        startDate: campaignForValidation.startDate ? String(campaignForValidation.startDate) : null,
        endDate: campaignForValidation.endDate ? String(campaignForValidation.endDate) : null,
      },
      assignments,
    });
    if (conflicts.length > 0) {
      throw new CampaignOverlapConflictError(conflicts);
    }
    const now = new Date();
    const auditUserId = await resolveAuditUserId(req.authUser?.appUserId);

    await db.transaction(async (tx) => {
      const [campaignRow] = await tx
        .select({ id: campaigns.id })
        .from(campaigns)
        .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
        .limit(1);
      if (!campaignRow) throw new CampaignDomainError("campaign_not_found", 404, "Kampagne nicht gefunden.");

      for (const assignment of assignments) {
        await mergeCampaignAssignment(tx, {
          campaignId,
          marketId: assignment.marketId,
          gmUserId: assignment.gmUserId,
          visitTargetCountDelta: assignment.visitTargetCount,
          now,
          auditUserId,
        });
      }
    });

    const [row] = await db
      .select()
      .from(campaigns)
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .limit(1);
    if (!row) {
      res.status(404).json({ error: "Kampagne nicht gefunden.", code: "campaign_not_found" });
      return;
    }
    const [campaign] = await mapCampaignRows([row]);
    res.status(200).json({ campaign });
  } catch (error) {
    if (error instanceof CampaignOverlapConflictError) {
      res.status(error.status).json({ error: error.message, code: error.code, conflicts: error.conflicts });
      return;
    }
    if (error instanceof CampaignDomainError) {
      respondDomainError(res, error);
      return;
    }
    next(error);
  }
});

adminCampaignsRouter.post("/campaigns/:id/markets/migrate", async (req: AuthedRequest, res, next) => {
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      res.status(400).json({ error: "Ungueltige Kampagnen-ID.", code: "invalid_id" });
      return;
    }
    const parsed = migrateMarketsSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Marktmigration.", code: "invalid_payload" });
      return;
    }

    const movesByKey = new Map<
      string,
      { marketId: string; fromCampaignId: string; gmUserId: string | null; reason: string | undefined }
    >();
    for (const move of parsed.data.moves) {
      const key = `${move.marketId}:${move.fromCampaignId}`;
      movesByKey.set(key, {
        marketId: move.marketId,
        fromCampaignId: move.fromCampaignId,
        gmUserId: move.gmUserId ?? null,
        reason: move.reason,
      });
    }
    const moves = Array.from(movesByKey.values());
    const marketIds = moves.map((move) => move.marketId);
    await ensureMarketsExist(marketIds);
    await ensureGmUsersExist(moves.map((move) => move.gmUserId));

    const [targetCampaign] = await db
      .select({
        id: campaigns.id,
        section: campaigns.section,
        status: campaigns.status,
        scheduleType: campaigns.scheduleType,
        startDate: campaigns.startDate,
        endDate: campaigns.endDate,
      })
      .from(campaigns)
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .limit(1);
    if (!targetCampaign) {
      throw new CampaignDomainError("campaign_not_found", 404, "Kampagne nicht gefunden.");
    }

    const conflicts = await findCampaignAssignmentConflicts({
      targetCampaignId: campaignId,
      section: targetCampaign.section,
      targetStatus: targetCampaign.status,
      targetWindow: {
        scheduleType: targetCampaign.scheduleType,
        startDate: targetCampaign.startDate ? String(targetCampaign.startDate) : null,
        endDate: targetCampaign.endDate ? String(targetCampaign.endDate) : null,
      },
      assignments: moves.map((move) => ({ marketId: move.marketId, gmUserId: move.gmUserId, visitTargetCount: 1 })),
    });
    const conflictsByKey = new Map(conflicts.map((conflict) => [`${conflict.marketId}:${conflict.existingCampaignId}`, conflict]));
    for (const move of moves) {
      const key = `${move.marketId}:${move.fromCampaignId}`;
      if (!conflictsByKey.has(key)) {
        throw new CampaignDomainError(
          "invalid_payload",
          409,
          "Mindestens ein Konflikt ist nicht mehr aktuell. Bitte Daten neu laden.",
        );
      }
    }

    const now = new Date();
    const auditUserId = await resolveAuditUserId(req.authUser?.appUserId);
    await db.transaction(async (tx) => {
      for (const move of moves) {
        const oldAssignments = await tx
          .select({
            id: campaignMarketAssignments.id,
            campaignId: campaignMarketAssignments.campaignId,
            marketId: campaignMarketAssignments.marketId,
            gmUserId: campaignMarketAssignments.gmUserId,
            visitTargetCount: campaignMarketAssignments.visitTargetCount,
          })
          .from(campaignMarketAssignments)
          .where(
            and(
              eq(campaignMarketAssignments.campaignId, move.fromCampaignId),
              eq(campaignMarketAssignments.marketId, move.marketId),
              eq(campaignMarketAssignments.isDeleted, false),
            ),
          );
        if (oldAssignments.length === 0) {
          throw new CampaignDomainError(
            "invalid_payload",
            409,
            "Mindestens ein Konflikt ist nicht mehr aktuell. Bitte Daten neu laden.",
          );
        }

        const oldAssignmentIds = oldAssignments.map((assignment) => assignment.id);
        await tx
          .update(campaignMarketAssignments)
          .set({
            isDeleted: true,
            deletedAt: now,
            updatedAt: now,
          })
          .where(inArray(campaignMarketAssignments.id, oldAssignmentIds));

        for (const oldAssignment of oldAssignments) {
          const nextGmUserId = move.gmUserId ?? oldAssignment.gmUserId ?? null;
          await mergeCampaignAssignment(tx, {
            campaignId,
            marketId: move.marketId,
            gmUserId: nextGmUserId,
            visitTargetCountDelta: oldAssignment.visitTargetCount,
            now,
            auditUserId,
          });

          await tx.insert(campaignMarketAssignmentHistory).values({
            marketId: move.marketId,
            section: targetCampaign.section,
            fromCampaignId: move.fromCampaignId,
            toCampaignId: campaignId,
            fromGmUserId: oldAssignment.gmUserId ?? null,
            toGmUserId: nextGmUserId,
            migratedByUserId: auditUserId,
            migratedAt: now,
            reason: move.reason ?? "campaign_overlap_resolution",
            createdAt: now,
          });
        }
      }
    });

    const [row] = await db
      .select()
      .from(campaigns)
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .limit(1);
    if (!row) {
      res.status(404).json({ error: "Kampagne nicht gefunden.", code: "campaign_not_found" });
      return;
    }
    const [campaign] = await mapCampaignRows([row]);
    res.status(200).json({ campaign, migrated: moves.length });
  } catch (error) {
    if (error instanceof CampaignOverlapConflictError) {
      res.status(error.status).json({ error: error.message, code: error.code, conflicts: error.conflicts });
      return;
    }
    if (error instanceof CampaignDomainError) {
      respondDomainError(res, error);
      return;
    }
    next(error);
  }
});

adminCampaignsRouter.patch("/campaigns/:id/markets/:marketId/delete", async (req, res, next) => {
  try {
    const campaignId = String(req.params.id ?? "");
    const marketId = String(req.params.marketId ?? "");
    if (!isUuid(campaignId) || !isUuid(marketId)) {
      res.status(400).json({ error: "Ungueltige ID.", code: "invalid_id" });
      return;
    }
    const now = new Date();
    // Removing a market from a campaign is market-scoped: all active assignment rows
    // for this campaign+market are soft-deleted, regardless of GM.
    const removed = await db
      .update(campaignMarketAssignments)
      .set({ isDeleted: true, deletedAt: now, updatedAt: now })
      .where(
        and(
          eq(campaignMarketAssignments.campaignId, campaignId),
          eq(campaignMarketAssignments.marketId, marketId),
          eq(campaignMarketAssignments.isDeleted, false),
        ),
      )
      .returning({ campaignId: campaignMarketAssignments.campaignId });

    const [row] = await db
      .select()
      .from(campaigns)
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .limit(1);
    if (!row) {
      res.status(404).json({ error: "Kampagne nicht gefunden.", code: "campaign_not_found" });
      return;
    }
    const [campaign] = await mapCampaignRows([row]);
    res.status(200).json({ campaign, removed: removed.length > 0 });
  } catch (error) {
    next(error);
  }
});

adminCampaignsRouter.post("/campaigns/:id/fragebogen/switch", async (req: AuthedRequest, res, next) => {
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      res.status(400).json({ error: "Ungueltige Kampagnen-ID.", code: "invalid_id" });
      return;
    }
    const parsed = switchFragebogenSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltiger Fragebogen-Wechsel.", code: "invalid_payload" });
      return;
    }

    const now = new Date();
    const auditUserId = await resolveAuditUserId(req.authUser?.appUserId);
    let switched = false;
    await db.transaction(async (tx) => {
      const [existing] = await tx
        .select()
        .from(campaigns)
        .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
        .limit(1);
      if (!existing) throw new CampaignDomainError("campaign_not_found", 404, "Kampagne nicht gefunden.");

      await ensureFragebogenMatchesSection(existing.section, parsed.data.toFragebogenId);
      if (existing.currentFragebogenId === parsed.data.toFragebogenId) {
        return;
      }

      await tx.insert(campaignFragebogenHistory).values({
        campaignId,
        fromFragebogenId: existing.currentFragebogenId,
        toFragebogenId: parsed.data.toFragebogenId,
        changedAt: now,
        changedByUserId: auditUserId,
        isDeleted: false,
        deletedAt: null,
        createdAt: now,
        updatedAt: now,
      });

      await tx
        .update(campaigns)
        .set({
          currentFragebogenId: parsed.data.toFragebogenId,
          updatedAt: now,
        })
        .where(eq(campaigns.id, campaignId));
      switched = true;
    });

    const [row] = await db
      .select()
      .from(campaigns)
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .limit(1);
    if (!row) {
      res.status(404).json({ error: "Kampagne nicht gefunden.", code: "campaign_not_found" });
      return;
    }
    const [campaign] = await mapCampaignRows([row]);
    res.status(200).json({ campaign, switched });
  } catch (error) {
    if (error instanceof CampaignDomainError) {
      respondDomainError(res, error);
      return;
    }
    next(error);
  }
});

export { adminCampaignsRouter };
