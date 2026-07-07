import { and, asc, desc, eq, inArray, isNull, ne, sql } from "drizzle-orm";
import { type Response, Router } from "express";
import { z } from "zod";
import { finalizeBonusForSubmittedVisitSessionTx, recomputeBonusWaveTx } from "../lib/bonus-finalizer.js";
import { recomputeGmKpiCache } from "../lib/gm-kpi-cache.js";
import { enqueueIppRecalcForDate } from "../lib/ipp-finalizer.js";
import { requireKundeAdminPermission } from "../lib/kunde-access.js";
import { aggregateHighVolumeLoad, logAction, logger, markErrorAsLogged, startActionTimer } from "../lib/logger.js";
import { type AuthedRequest, requireAuth } from "../middleware/auth.js";
import { db } from "../lib/db.js";
import { computeHiddenQuestionIds } from "../lib/conditional-visibility.js";
import { buildVisitAnswerValidationResult, computeMissingRequiredQuestions } from "../lib/visit-session-answer-validation.js";
import {
  campaignMarketAssignmentHistory,
  campaignFragebogenHistory,
  campaignMarketAssignments,
  campaigns,
  fragebogenKuehler,
  fragebogenMain,
  fragebogenMhd,
  markets,
  marketKuehlerUnits,
  praemienGmWaveContributions,
  visitAnswerMatrixCells,
  visitAnswerOptions,
  visitAnswerPhotoTags,
  visitAnswerPhotos,
  visitAnswerChangeRequests,
  visitAnswers,
  visitAnswerEvents,
  visitQuestionComments,
  visitSessionQuestions,
  visitSessionSections,
  visitSessionDeleteRequests,
  visitSessions,
  users,
} from "../lib/schema.js";

const campaignSectionSchema = z.enum(["standard", "flex", "billa", "kuehler", "mhd"]);
const campaignStatusSchema = z.enum(["active", "scheduled", "inactive"]);
const scheduleTypeSchema = z.enum(["always", "scheduled"]);
const CAMPAIGN_ASSIGNMENT_INSERT_BATCH_SIZE = 500;
const campaignAssignmentSchema = z
  .object({
    marketId: z.string().uuid(),
    gmUserId: z.string().uuid().nullable().optional(),
    gmNameRaw: z.string().optional(),
    assignmentSlot: z.coerce.number().int().min(1).optional(),
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

const hardDeleteCampaignSchema = z
  .object({
    confirmationText: z.string(),
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

const reassignCampaignGmsSchema = z
  .object({
    reassignments: z
      .array(
        z
          .object({
            fromGmUserId: z.string().uuid(),
            toGmUserId: z.string().uuid(),
          })
          .strict(),
      )
      .min(1),
  })
  .strict();

const HARD_DELETE_CAMPAIGN_CONFIRMATION_TEXT = "Ich bin mir sicher, dass ich alle Daten zu dieser Kampagne Löschen will!";
const adminSubmittedVisitAnswerPatchSchema = z
  .object({
    visitQuestionId: z.string().uuid(),
    answer: z.unknown(),
    comment: z.string().max(4000).optional(),
  })
  .strict();
const adminChangeRequestDecisionSchema = z
  .object({
    adminNote: z.string().trim().max(700).optional(),
  })
  .strict();

function deriveVisitAnswerEventType(input: {
  previousStatus: "unanswered" | "answered" | "invalid" | "hidden_by_rule" | "skipped" | null;
  nextStatus: "unanswered" | "answered" | "invalid";
}): "set" | "clear" | "status_change" {
  if (input.previousStatus == null) {
    return input.nextStatus === "unanswered" ? "clear" : "set";
  }
  if (input.previousStatus !== input.nextStatus) return "status_change";
  if (input.nextStatus === "unanswered") return "clear";
  return "set";
}

class CampaignDomainError extends Error {
  code:
    | "invalid_id"
    | "invalid_payload"
    | "schedule_invalid"
    | "market_missing"
    | "gm_missing"
    | "gm_required"
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
      | "gm_required"
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

function asPlainRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function stringList(value: unknown): string[] {
  return Array.isArray(value) ? normalizeUnique(value.filter((entry): entry is string => typeof entry === "string")) : [];
}

function normalizeChangeRequestAnswerPayload(input: {
  questionType: string;
  payload: Record<string, unknown>;
}): { ok: true; answer: unknown } | { ok: false; error: string; code: string } {
  const payload = input.payload;
  if ("rawAnswer" in payload) return { ok: true, answer: payload.rawAnswer };

  if (input.questionType === "single" || input.questionType === "yesno" || input.questionType === "text") {
    const value = typeof payload.value === "string" ? payload.value.trim() : "";
    if (!value) return { ok: false, error: "Anfrage enthält keinen direkt anwendbaren Antwortwert.", code: "change_request_not_applicable" };
    return { ok: true, answer: value };
  }

  if (input.questionType === "multiple") {
    const values = stringList(payload.values);
    if (values.length > 0) return { ok: true, answer: values };
    const value = typeof payload.value === "string" ? payload.value.trim() : "";
    if (!value) return { ok: false, error: "Anfrage enthält keine Mehrfachauswahl.", code: "change_request_not_applicable" };
    return { ok: true, answer: [value] };
  }

  if (input.questionType === "yesnomulti") {
    const top = typeof payload.top === "string"
      ? payload.top.trim()
      : typeof payload.sel === "string"
        ? payload.sel.trim()
        : "";
    const subs = stringList(payload.subs);
    if (!top) return { ok: false, error: "Anfrage enthält keine Ja/Nein-Auswahl.", code: "change_request_not_applicable" };
    return { ok: true, answer: JSON.stringify({ sel: top, subs }) };
  }

  if (input.questionType === "likert" || input.questionType === "numeric" || input.questionType === "slider") {
    const value = payload.value;
    if (typeof value !== "number" && typeof value !== "string") {
      return { ok: false, error: "Anfrage enthält keinen Zahlenwert.", code: "change_request_not_applicable" };
    }
    return { ok: true, answer: value };
  }

  if (input.questionType === "matrix") {
    const value = payload.value;
    if (Array.isArray(value) || (typeof value === "string" && value.trim().startsWith("{"))) {
      return { ok: true, answer: value };
    }
    return {
      ok: false,
      error: "Matrix-Anfragen müssen manuell geprüft werden, weil kein strukturierter Matrixwert vorliegt.",
      code: "change_request_manual_review_required",
    };
  }

  if (input.questionType === "photo") {
    return {
      ok: false,
      error: "Foto-Anfragen können ohne neue Datei nicht automatisch angewendet werden.",
      code: "change_request_manual_review_required",
    };
  }

  return { ok: false, error: "Dieser Fragetyp kann nicht automatisch angewendet werden.", code: "change_request_not_applicable" };
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

function isMissingAssignmentSlotColumnError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error ?? "");
  const pgCode = typeof error === "object" && error !== null && "code" in error ? String((error as { code?: unknown }).code ?? "") : "";
  if (pgCode === "42703" && /assignment_slot/i.test(message)) return true;
  return /assignment_slot/i.test(message) && /does not exist/i.test(message);
}

let visitSessionQuestionAvailabilityTypeSnapshotColumnReady: boolean | null = null;

async function ensureVisitSessionQuestionAvailabilityTypeSnapshotColumnReady(): Promise<boolean> {
  if (visitSessionQuestionAvailabilityTypeSnapshotColumnReady != null) {
    return visitSessionQuestionAvailabilityTypeSnapshotColumnReady;
  }
  const result = await db.execute(sql<{ exists: boolean }>`
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = current_schema()
        AND table_name = 'visit_session_questions'
        AND column_name = 'single_choice_availability_type_snapshot'
    ) AS "exists"
  `);
  const firstRow = (result as Array<{ exists?: unknown }>)[0];
  visitSessionQuestionAvailabilityTypeSnapshotColumnReady = Boolean(firstRow?.exists);
  return visitSessionQuestionAvailabilityTypeSnapshotColumnReady;
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

function mapCampaignMarketRow(row: typeof markets.$inferSelect) {
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

type CampaignMarketVisitStatusRow = {
  rowId: string;
  marketId: string;
  kuehlerUnitId: string | null;
  kuehlerNumber: string | null;
  targetVisitCount: number;
  submittedVisitCount: number;
  isComplete: boolean;
  hasSubmittedVisit: boolean;
  sessionId: string | null;
  startedAt: string | null;
  submittedAt: string | null;
  durationMinutes: number | null;
  gmUserId: string | null;
  gmName: string | null;
};

async function buildCampaignMarketVisitStatusBatch(campaignIds: string[]) {
  const uniqueCampaignIds = normalizeUnique(campaignIds).filter(isUuid);
  if (uniqueCampaignIds.length === 0) return [];

  const campaignRows = await db
    .select({ id: campaigns.id, section: campaigns.section })
    .from(campaigns)
    .where(and(inArray(campaigns.id, uniqueCampaignIds), eq(campaigns.isDeleted, false)));
  const campaignSectionById = new Map(campaignRows.map((row) => [row.id, row.section]));

  const assignmentRows = await db
    .select({
      campaignId: campaignMarketAssignments.campaignId,
      marketId: campaignMarketAssignments.marketId,
      visitTargetCount: campaignMarketAssignments.visitTargetCount,
    })
    .from(campaignMarketAssignments)
    .where(and(inArray(campaignMarketAssignments.campaignId, uniqueCampaignIds), eq(campaignMarketAssignments.isDeleted, false)));

  const targetByCampaignMarket = new Map<string, number>();
  const marketsByCampaign = new Map<string, Set<string>>();
  for (const row of assignmentRows) {
    const key = `${row.campaignId}:${row.marketId}`;
    targetByCampaignMarket.set(key, (targetByCampaignMarket.get(key) ?? 0) + row.visitTargetCount);
    const markets = marketsByCampaign.get(row.campaignId) ?? new Set<string>();
    markets.add(row.marketId);
    marketsByCampaign.set(row.campaignId, markets);
  }

  if (targetByCampaignMarket.size === 0) {
    return uniqueCampaignIds.map((campaignId) => ({ campaignId, markets: [] }));
  }

  const submittedRows = await db
    .select({
      campaignId: visitSessionSections.campaignId,
      marketId: visitSessions.marketId,
      sessionId: visitSessions.id,
      kuehlerUnitId: visitSessions.kuehlerUnitId,
      gmUserId: visitSessions.gmUserId,
      startedAt: visitSessions.startedAt,
      submittedAt: visitSessions.submittedAt,
      createdAt: visitSessions.createdAt,
    })
    .from(visitSessionSections)
    .innerJoin(visitSessions, eq(visitSessions.id, visitSessionSections.visitSessionId))
    .where(
      and(
        inArray(visitSessionSections.campaignId, uniqueCampaignIds),
        eq(visitSessionSections.isDeleted, false),
        eq(visitSessions.isDeleted, false),
        eq(visitSessions.status, "submitted"),
      ),
    )
    .orderBy(desc(visitSessions.submittedAt), desc(visitSessions.createdAt));

  const submittedCountByCampaignMarket = new Map<string, number>();
  const latestByCampaignMarket = new Map<string, (typeof submittedRows)[number]>();
  const submittedCountByKuehlerUnit = new Map<string, number>();
  const latestByKuehlerUnit = new Map<string, (typeof submittedRows)[number]>();
  const submittedCountByLegacyKuehlerMarket = new Map<string, number>();
  const latestByLegacyKuehlerMarket = new Map<string, (typeof submittedRows)[number]>();
  const seenSubmittedSession = new Set<string>();
  for (const row of submittedRows) {
    const key = `${row.campaignId}:${row.marketId}`;
    if (!targetByCampaignMarket.has(key)) continue;
    const isKuehlerCampaign = campaignSectionById.get(row.campaignId) === "kuehler";
    const sessionKey = `${key}:${row.sessionId}`;
    if (!seenSubmittedSession.has(sessionKey)) {
      seenSubmittedSession.add(sessionKey);
      submittedCountByCampaignMarket.set(key, (submittedCountByCampaignMarket.get(key) ?? 0) + 1);
      if (isKuehlerCampaign && row.kuehlerUnitId) {
        const unitKey = `${key}:${row.kuehlerUnitId}`;
        submittedCountByKuehlerUnit.set(unitKey, (submittedCountByKuehlerUnit.get(unitKey) ?? 0) + 1);
      } else if (isKuehlerCampaign) {
        submittedCountByLegacyKuehlerMarket.set(key, (submittedCountByLegacyKuehlerMarket.get(key) ?? 0) + 1);
      }
    }
    if (!latestByCampaignMarket.has(key)) {
      latestByCampaignMarket.set(key, row);
    }
    if (isKuehlerCampaign && row.kuehlerUnitId) {
      const unitKey = `${key}:${row.kuehlerUnitId}`;
      if (!latestByKuehlerUnit.has(unitKey)) {
        latestByKuehlerUnit.set(unitKey, row);
      }
    } else if (isKuehlerCampaign && !latestByLegacyKuehlerMarket.has(key)) {
      latestByLegacyKuehlerMarket.set(key, row);
    }
  }

  const kuehlerMarketIds = normalizeUnique(
    Array.from(targetByCampaignMarket.keys())
      .map((key) => {
        const [campaignId, marketId] = key.split(":");
        if (!campaignId || !marketId) return null;
        return campaignSectionById.get(campaignId) === "kuehler" ? marketId : null;
      })
      .filter((entry): entry is string => Boolean(entry)),
  );
  const kuehlerUnitsByMarketId = new Map<string, Array<typeof marketKuehlerUnits.$inferSelect>>();
  if (kuehlerMarketIds.length > 0) {
    const kuehlerUnitRows = await db
      .select()
      .from(marketKuehlerUnits)
      .where(and(inArray(marketKuehlerUnits.marketId, kuehlerMarketIds), eq(marketKuehlerUnits.isDeleted, false)))
      .orderBy(asc(marketKuehlerUnits.kuehlerInternalId), asc(marketKuehlerUnits.createdAt));
    for (const unit of kuehlerUnitRows) {
      const bucket = kuehlerUnitsByMarketId.get(unit.marketId) ?? [];
      bucket.push(unit);
      kuehlerUnitsByMarketId.set(unit.marketId, bucket);
    }
  }

  const gmUserIds = normalizeUnique(
    Array.from(latestByCampaignMarket.values())
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

  return uniqueCampaignIds.map((campaignId) => {
    const marketIds = Array.from(marketsByCampaign.get(campaignId) ?? []);
    const isKuehlerCampaign = campaignSectionById.get(campaignId) === "kuehler";
    return {
      campaignId,
      markets: marketIds.flatMap<CampaignMarketVisitStatusRow>((marketId) => {
        const key = `${campaignId}:${marketId}`;
        const targetVisitCount = targetByCampaignMarket.get(key) ?? 0;
        if (isKuehlerCampaign) {
          const units = kuehlerUnitsByMarketId.get(marketId) ?? [];
          const rowCount = Math.max(targetVisitCount, units.length, 1);
          return Array.from({ length: rowCount }, (_, index) => {
            const unit = units[index] ?? null;
            const unitKey = unit ? `${key}:${unit.id}` : null;
            const legacyAvailable = !unit ? Math.max(0, (submittedCountByLegacyKuehlerMarket.get(key) ?? 0) - index) : 0;
            const submittedVisitCount = unitKey
              ? Math.min(1, submittedCountByKuehlerUnit.get(unitKey) ?? 0)
              : legacyAvailable > 0 ? 1 : 0;
            const latest = unitKey
              ? latestByKuehlerUnit.get(unitKey) ?? null
              : index === 0 ? latestByLegacyKuehlerMarket.get(key) ?? null : null;
            return {
              rowId: `kuehler:${marketId}:${unit?.id ?? `slot-${index + 1}`}`,
              marketId,
              kuehlerUnitId: unit?.id ?? null,
              kuehlerNumber: unit?.kuehlerInternalId ?? (rowCount > 1 ? `Kühler ${index + 1}` : null),
              targetVisitCount: 1,
              submittedVisitCount,
              isComplete: submittedVisitCount >= 1,
              hasSubmittedVisit: submittedVisitCount > 0,
              sessionId: latest?.sessionId ?? null,
              startedAt: latest?.startedAt?.toISOString() ?? null,
              submittedAt: latest?.submittedAt?.toISOString() ?? null,
              durationMinutes: latest ? calculateDurationMinutes(latest.startedAt, latest.submittedAt) : null,
              gmUserId: latest?.gmUserId ?? null,
              gmName: latest?.gmUserId ? gmNameById.get(latest.gmUserId) ?? null : null,
            };
          });
        }
        const submittedVisitCount = submittedCountByCampaignMarket.get(key) ?? 0;
        const latest = latestByCampaignMarket.get(key) ?? null;
        return {
          rowId: marketId,
          marketId,
          kuehlerUnitId: null,
          kuehlerNumber: null,
          targetVisitCount,
          submittedVisitCount,
          isComplete: targetVisitCount > 0 && submittedVisitCount >= targetVisitCount,
          hasSubmittedVisit: submittedVisitCount > 0,
          sessionId: latest?.sessionId ?? null,
          startedAt: latest?.startedAt?.toISOString() ?? null,
          submittedAt: latest?.submittedAt?.toISOString() ?? null,
          durationMinutes: latest ? calculateDurationMinutes(latest.startedAt, latest.submittedAt) : null,
          gmUserId: latest?.gmUserId ?? null,
          gmName: latest?.gmUserId ? gmNameById.get(latest.gmUserId) ?? null : null,
        };
      }),
    };
  });
}

function parseIsoDateOrThrow(raw: string, field: "startDate" | "endDate"): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw new CampaignDomainError("schedule_invalid", 400, `${field} muss im Format YYYY-MM-DD sein.`);
  }
  const parsed = new Date(`${raw}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) {
    throw new CampaignDomainError("schedule_invalid", 400, `${field} ist ungültig.`);
  }
  const canonical = parsed.toISOString().slice(0, 10);
  if (canonical !== raw) {
    throw new CampaignDomainError("schedule_invalid", 400, `${field} ist ungültig.`);
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
    throw new CampaignDomainError("schedule_invalid", 400, "startDate und endDate sind für scheduleType=scheduled erforderlich.");
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
    throw new CampaignDomainError("market_missing", 400, "Mindestens ein Markt existiert nicht oder ist gelöscht.");
  }
}

async function loadActiveUniversumMarketIds(): Promise<string[]> {
  const rows = await db
    .select({ id: markets.id })
    .from(markets)
    .where(
      and(
        eq(markets.isDeleted, false),
        eq(markets.isActive, true),
        inArray(markets.marketType, ["universum", "both"]),
      ),
    );
  return rows.map((row) => row.id);
}

type CampaignAssignmentInput = {
  marketId: string;
  gmUserId: string | null;
  gmNameRaw?: string;
  assignmentSlot: number;
  visitTargetCount: number;
};

type CampaignAssignmentDraftInput = {
  marketId: string;
  gmUserId?: string | null | undefined;
  gmNameRaw?: string | undefined;
  assignmentSlot?: number | undefined;
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

function deriveEffectiveCampaignStatus(input: {
  status: "active" | "scheduled" | "inactive";
  scheduleType: "always" | "scheduled";
  startDate: string | null;
  endDate: string | null;
}): "active" | "scheduled" | "inactive" {
  if (input.status === "inactive") return "inactive";
  if (input.scheduleType === "always") return "active";
  if (!input.startDate || !input.endDate) return input.status;

  const todayYmd = new Date().toISOString().slice(0, 10);
  const startYmd = input.startDate.slice(0, 10);
  const endYmd = input.endDate.slice(0, 10);

  if (todayYmd < startYmd) return "scheduled";
  if (todayYmd > endYmd) return "inactive";
  return "active";
}

function normalizeAssignments(input: {
  section: CampaignSection;
  marketIds?: string[];
  assignments?: CampaignAssignmentDraftInput[];
}) {
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
      normalized.set(key, { marketId, gmUserId: null, assignmentSlot: 1, visitTargetCount: 1 });
    }
  }

  if (hasExplicitAssignments && input.section === "kuehler") {
    const assignmentRows: CampaignAssignmentInput[] = [];
    const nextSlotByKey = new Map<string, number>();
    const usedSlotsByKey = new Map<string, Set<number>>();

    for (const assignment of assignmentInput) {
      if (!assignment.marketId) continue;
      const gmUserId = assignment.gmUserId ?? null;
      const slotKey = `${assignment.marketId}:${gmUserId ?? "__unassigned__"}`;
      const usedSlots = usedSlotsByKey.get(slotKey) ?? new Set<number>();
      usedSlotsByKey.set(slotKey, usedSlots);
      const desiredSlot = assignment.assignmentSlot ?? (nextSlotByKey.get(slotKey) ?? 0) + 1;
      let assignmentSlot = desiredSlot;
      while (usedSlots.has(assignmentSlot)) {
        assignmentSlot += 1;
      }
      usedSlots.add(assignmentSlot);
      nextSlotByKey.set(slotKey, Math.max(nextSlotByKey.get(slotKey) ?? 0, assignmentSlot));
      assignmentRows.push({
        marketId: assignment.marketId,
        gmUserId,
        assignmentSlot,
        visitTargetCount: assignment.visitTargetCount ?? 1,
        ...(assignment.gmNameRaw ? { gmNameRaw: assignment.gmNameRaw } : {}),
      });
    }

    return assignmentRows;
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
      assignmentSlot: 1,
      visitTargetCount: count,
      ...(assignment.gmNameRaw ? { gmNameRaw: assignment.gmNameRaw } : {}),
    });
  }
  return Array.from(normalized.values());
}

function ensureManualAssignmentsHaveGm(section: CampaignSection, assignments: CampaignAssignmentInput[]) {
  if (section === "flex") return;
  const hasUnassignedTarget = assignments.some((assignment) => !assignment.gmUserId);
  if (!hasUnassignedTarget) return;
  throw new CampaignDomainError(
    "gm_required",
    400,
    "Bitte einen GM auswaehlen. Maerkte ohne GM sind fuer GMs nicht sichtbar.",
  );
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
    if (!row) throw new CampaignDomainError("fragebogen_mismatch", 400, "Fragebogen passt nicht zur Sektion oder ist gelöscht.");
    return;
  }
  if (section === "mhd") {
    const [row] = await db
      .select({ id: fragebogenMhd.id })
      .from(fragebogenMhd)
      .where(and(eq(fragebogenMhd.id, fragebogenId), eq(fragebogenMhd.isDeleted, false)))
      .limit(1);
    if (!row) throw new CampaignDomainError("fragebogen_mismatch", 400, "Fragebogen passt nicht zur Sektion oder ist gelöscht.");
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
  if (!row) throw new CampaignDomainError("fragebogen_mismatch", 400, "Fragebogen passt nicht zur Sektion oder ist gelöscht.");
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
  let assignments: Array<{
    campaignId: string;
    marketId: string;
    gmUserId: string | null;
    assignmentSlot: number;
    visitTargetCount: number;
    currentVisitsCount: number;
    assignedAt: Date;
  }> = [];
  try {
    assignments = await db
      .select({
        campaignId: campaignMarketAssignments.campaignId,
        marketId: campaignMarketAssignments.marketId,
        gmUserId: campaignMarketAssignments.gmUserId,
        assignmentSlot: campaignMarketAssignments.assignmentSlot,
        visitTargetCount: campaignMarketAssignments.visitTargetCount,
        currentVisitsCount: campaignMarketAssignments.currentVisitsCount,
        assignedAt: campaignMarketAssignments.assignedAt,
      })
      .from(campaignMarketAssignments)
      .where(and(inArray(campaignMarketAssignments.campaignId, campaignIds), eq(campaignMarketAssignments.isDeleted, false)))
      .orderBy(asc(campaignMarketAssignments.assignedAt), asc(campaignMarketAssignments.assignmentSlot));
  } catch (error) {
    if (!isMissingAssignmentSlotColumnError(error)) throw error;
    const legacyAssignments = await db
      .select({
        campaignId: campaignMarketAssignments.campaignId,
        marketId: campaignMarketAssignments.marketId,
        gmUserId: campaignMarketAssignments.gmUserId,
        visitTargetCount: campaignMarketAssignments.visitTargetCount,
        currentVisitsCount: campaignMarketAssignments.currentVisitsCount,
        assignedAt: campaignMarketAssignments.assignedAt,
      })
      .from(campaignMarketAssignments)
      .where(and(inArray(campaignMarketAssignments.campaignId, campaignIds), eq(campaignMarketAssignments.isDeleted, false)))
      .orderBy(asc(campaignMarketAssignments.assignedAt));
    assignments = legacyAssignments.map((row) => ({
      ...row,
      assignmentSlot: 1,
    }));
  }

  const historyRows = await db
    .select()
    .from(campaignFragebogenHistory)
    .where(and(inArray(campaignFragebogenHistory.campaignId, campaignIds), eq(campaignFragebogenHistory.isDeleted, false)))
    .orderBy(desc(campaignFragebogenHistory.changedAt));

  const marketIdsByCampaign = new Map<string, string[]>();
  const assignmentRowsByCampaign = new Map<
    string,
    Array<{
      marketId: string;
      gmUserId: string | null;
      assignmentSlot: number;
      visitTargetCount: number;
      currentVisitsCount: number;
    }>
  >();
  for (const row of assignments) {
    const current = marketIdsByCampaign.get(row.campaignId) ?? [];
    current.push(row.marketId);
    marketIdsByCampaign.set(row.campaignId, current);
    const assignmentRows = assignmentRowsByCampaign.get(row.campaignId) ?? [];
    assignmentRows.push({
      marketId: row.marketId,
      gmUserId: row.gmUserId ?? null,
      assignmentSlot: row.assignmentSlot,
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
    status: deriveEffectiveCampaignStatus({
      status: row.status,
      scheduleType: row.scheduleType,
      startDate: row.startDate,
      endDate: row.endDate,
    }),
    scheduleType: row.scheduleType,
    startDate: row.startDate ? String(row.startDate) : null,
    endDate: row.endDate ? String(row.endDate) : null,
    marketIds: normalizeUnique(marketIdsByCampaign.get(row.id) ?? []),
    assignments: (assignmentRowsByCampaign.get(row.id) ?? []).map((assignment) => ({
      marketId: assignment.marketId,
      gmUserId: assignment.gmUserId,
      gmName: assignment.gmUserId ? (gmNameById.get(assignment.gmUserId) ?? null) : null,
      assignmentSlot: assignment.assignmentSlot,
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
    assignmentSlot: number;
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
        eq(campaignMarketAssignments.assignmentSlot, input.assignmentSlot),
        eq(campaignMarketAssignments.isDeleted, false),
      ),
    )
    .returning({ id: campaignMarketAssignments.id });
  if (updated.length > 0) return;

  await tx.insert(campaignMarketAssignments).values({
    campaignId: input.campaignId,
    marketId: input.marketId,
    gmUserId: input.gmUserId,
    assignmentSlot: input.assignmentSlot,
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

async function buildCampaignMarketVisitSummaries(
  campaignId: string,
  options?: { marketIds?: string[]; sessionId?: string | null },
) {
  const scopedMarketIds = normalizeUnique(options?.marketIds ?? []).filter(isUuid);
  const assignedConditions = [
    eq(campaignMarketAssignments.campaignId, campaignId),
    eq(campaignMarketAssignments.isDeleted, false),
  ];
  if (scopedMarketIds.length > 0) {
    assignedConditions.push(inArray(campaignMarketAssignments.marketId, scopedMarketIds));
  }
  const assignedRows = await db
    .select({
      marketId: campaignMarketAssignments.marketId,
    })
    .from(campaignMarketAssignments)
    .where(and(...assignedConditions));
  const assignedMarketIds = normalizeUnique(assignedRows.map((row) => row.marketId));
  if (assignedMarketIds.length === 0) return [];

  const submittedConditions = [
    eq(visitSessionSections.campaignId, campaignId),
    eq(visitSessionSections.isDeleted, false),
    eq(visitSessions.isDeleted, false),
    eq(visitSessions.status, "submitted"),
    inArray(visitSessions.marketId, assignedMarketIds),
  ];
  if (options?.sessionId && isUuid(options.sessionId)) {
    submittedConditions.push(eq(visitSessions.id, options.sessionId));
  }
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
    .where(and(...submittedConditions))
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
  const hasAvailabilityTypeSnapshotColumn = await ensureVisitSessionQuestionAvailabilityTypeSnapshotColumnReady();
  const questions =
    sectionIds.length === 0
      ? []
      : await db
          .select({
            id: visitSessionQuestions.id,
            visitSessionSectionId: visitSessionQuestions.visitSessionSectionId,
            questionId: visitSessionQuestions.questionId,
            moduleId: visitSessionQuestions.moduleId,
            moduleNameSnapshot: visitSessionQuestions.moduleNameSnapshot,
            questionType: visitSessionQuestions.questionType,
            questionTextSnapshot: visitSessionQuestions.questionTextSnapshot,
            requiredSnapshot: visitSessionQuestions.requiredSnapshot,
            singleChoiceAvailabilitySnapshot: visitSessionQuestions.singleChoiceAvailabilitySnapshot,
            singleChoiceAvailabilityTypeSnapshot: hasAvailabilityTypeSnapshotColumn
              ? visitSessionQuestions.singleChoiceAvailabilityTypeSnapshot
              : sql<string | null>`null::text`,
            questionConfigSnapshot: visitSessionQuestions.questionConfigSnapshot,
            questionRulesSnapshot: visitSessionQuestions.questionRulesSnapshot,
            questionChainsSnapshot: visitSessionQuestions.questionChainsSnapshot,
            appliesToMarketChainSnapshot: visitSessionQuestions.appliesToMarketChainSnapshot,
            orderIndex: visitSessionQuestions.orderIndex,
          })
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
          singleChoiceAvailability: boolean | null;
          singleChoiceAvailabilityType: string | null;
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
            singleChoiceAvailability: question.singleChoiceAvailabilitySnapshot ?? null,
            singleChoiceAvailabilityType: question.singleChoiceAvailabilityTypeSnapshot ?? null,
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
adminCampaignsRouter.use(requireAuth(["admin", "kunde"]));
adminCampaignsRouter.use(requireKundeAdminPermission);
adminCampaignsRouter.use((req, res, next) => {
  if (req.method.toUpperCase() === "GET") {
    next();
    return;
  }
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "campaigns_action_completed", {
      req,
      action: "campaigns_mutation",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

adminCampaignsRouter.get("/campaigns", async (req, res, next) => {
  try {
    const rows = await db.select().from(campaigns).where(eq(campaigns.isDeleted, false)).orderBy(desc(campaigns.createdAt));
    const mapped = await mapCampaignRows(rows);
    const deletedRows = await db
      .select({ count: sql<number>`count(*)` })
      .from(campaigns)
      .where(eq(campaigns.isDeleted, true));
    aggregateHighVolumeLoad({
      metric: "campaigns",
      loaded: mapped.length,
      softDeleted: Number(deletedRows[0]?.count ?? 0),
      requestId: (req as { requestId?: string }).requestId ?? null,
      appUserId: (req as AuthedRequest).authUser?.appUserId ?? null,
      role: (req as AuthedRequest).authUser?.role ?? null,
    });
    res.status(200).json({ campaigns: mapped });
  } catch (error) {
    next(error);
  }
});

adminCampaignsRouter.get("/campaigns/market-visit-status", async (req, res, next) => {
  try {
    const rawCampaignIds = String(req.query.campaignIds ?? "")
      .split(",")
      .map((entry) => entry.trim())
      .filter(Boolean);
    const campaignIds = normalizeUnique(rawCampaignIds);
    if (campaignIds.length === 0 || campaignIds.some((campaignId) => !isUuid(campaignId))) {
      res.status(400).json({ error: "Ungültige Kampagnen-IDs.", code: "invalid_campaign_ids" });
      return;
    }
    if (campaignIds.length > 50) {
      res.status(400).json({ error: "Maximal 50 Kampagnen pro Status-Anfrage erlaubt.", code: "campaign_status_batch_too_large" });
      return;
    }

    const statusRows = await buildCampaignMarketVisitStatusBatch(campaignIds);
    res.status(200).json({ campaigns: statusRows });
  } catch (error) {
    next(error);
  }
});

adminCampaignsRouter.get("/campaigns/assigned-markets", async (req, res, next) => {
  try {
    const rawCampaignIds = String(req.query.campaignIds ?? "")
      .split(",")
      .map((entry) => entry.trim())
      .filter(Boolean);
    const campaignIds = normalizeUnique(rawCampaignIds);
    if (campaignIds.length === 0 || campaignIds.some((campaignId) => !isUuid(campaignId))) {
      res.status(400).json({ error: "Ungültige Kampagnen-IDs.", code: "invalid_campaign_ids" });
      return;
    }
    if (campaignIds.length > 50) {
      res.status(400).json({ error: "Maximal 50 Kampagnen pro Markt-Anfrage erlaubt.", code: "campaign_market_batch_too_large" });
      return;
    }

    const assignmentRows = await db
      .select({ marketId: campaignMarketAssignments.marketId })
      .from(campaignMarketAssignments)
      .innerJoin(campaigns, eq(campaigns.id, campaignMarketAssignments.campaignId))
      .where(
        and(
          inArray(campaignMarketAssignments.campaignId, campaignIds),
          eq(campaignMarketAssignments.isDeleted, false),
          eq(campaigns.isDeleted, false),
        ),
      );
    const marketIds = normalizeUnique(assignmentRows.map((row) => row.marketId));
    if (marketIds.length === 0) {
      res.status(200).json({ markets: [] });
      return;
    }

    const marketRows = await db
      .select()
      .from(markets)
      .where(and(inArray(markets.id, marketIds), eq(markets.isDeleted, false)))
      .orderBy(asc(markets.name), asc(markets.address));

    res.status(200).json({ markets: marketRows.map(mapCampaignMarketRow) });
  } catch (error) {
    next(error);
  }
});

adminCampaignsRouter.get("/campaigns/:id/market-visits", async (req, res, next) => {
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      res.status(400).json({ error: "Ungültige Kampagnen-ID.", code: "invalid_id" });
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

adminCampaignsRouter.get("/campaigns/:campaignId/markets/:marketId/visit-detail", async (req, res, next) => {
  try {
    const campaignId = String(req.params.campaignId ?? "");
    const marketId = String(req.params.marketId ?? "");
    const sessionId = typeof req.query.sessionId === "string" && req.query.sessionId.trim()
      ? req.query.sessionId.trim()
      : null;
    if (!isUuid(campaignId) || !isUuid(marketId) || (sessionId != null && !isUuid(sessionId))) {
      res.status(400).json({ error: "Ungültige Detail-Anfrage.", code: "invalid_id" });
      return;
    }

    const [campaign] = await db
      .select({ id: campaigns.id })
      .from(campaigns)
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .limit(1);
    if (!campaign) {
      res.status(404).json({ error: "Kampagne nicht gefunden.", code: "campaign_not_found" });
      return;
    }

    const [detail] = await buildCampaignMarketVisitSummaries(campaignId, { marketIds: [marketId], sessionId });
    if (!detail) {
      res.status(404).json({ error: "Markt ist dieser Kampagne nicht zugewiesen.", code: "campaign_market_not_found" });
      return;
    }
    res.status(200).json({ market: detail });
  } catch (error) {
    next(error);
  }
});

adminCampaignsRouter.get("/campaigns/answer-change-requests", async (req: AuthedRequest, res, next) => {
  try {
    if (req.authUser?.role !== "admin") {
      res.status(403).json({ error: "Nur Admins können ?nderungsanfragen prüfen.", code: "admin_required" });
      return;
    }

    const rows = await db
      .select({
        id: visitAnswerChangeRequests.id,
        visitSessionId: visitAnswerChangeRequests.visitSessionId,
        visitSessionQuestionId: visitAnswerChangeRequests.visitSessionQuestionId,
        visitAnswerId: visitAnswerChangeRequests.visitAnswerId,
        questionType: visitAnswerChangeRequests.questionType,
        questionTextSnapshot: visitAnswerChangeRequests.questionTextSnapshot,
        currentAnswerSnapshot: visitAnswerChangeRequests.currentAnswerSnapshot,
        requestedAnswerPayload: visitAnswerChangeRequests.requestedAnswerPayload,
        requestedAnswerSummary: visitAnswerChangeRequests.requestedAnswerSummary,
        requestNote: visitAnswerChangeRequests.requestNote,
        status: visitAnswerChangeRequests.status,
        reviewedByUserId: visitAnswerChangeRequests.reviewedByUserId,
        reviewedAt: visitAnswerChangeRequests.reviewedAt,
        adminNote: visitAnswerChangeRequests.adminNote,
        createdAt: visitAnswerChangeRequests.createdAt,
        updatedAt: visitAnswerChangeRequests.updatedAt,
        gmUserId: users.id,
        gmFirstName: users.firstName,
        gmLastName: users.lastName,
        gmEmail: users.email,
        gmRegion: users.region,
        marketId: markets.id,
        marketName: markets.name,
        marketAddress: markets.address,
        marketPostalCode: markets.postalCode,
        marketCity: markets.city,
        marketRegion: markets.region,
        sessionStartedAt: visitSessions.startedAt,
        sessionSubmittedAt: visitSessions.submittedAt,
        section: visitSessionSections.section,
        campaignId: visitSessionSections.campaignId,
        campaignName: campaigns.name,
        fragebogenName: visitSessionSections.fragebogenNameSnapshot,
      })
      .from(visitAnswerChangeRequests)
      .innerJoin(users, eq(users.id, visitAnswerChangeRequests.gmUserId))
      .innerJoin(markets, eq(markets.id, visitAnswerChangeRequests.marketId))
      .innerJoin(visitSessions, eq(visitSessions.id, visitAnswerChangeRequests.visitSessionId))
      .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitAnswerChangeRequests.visitSessionSectionId))
      .leftJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
      .where(eq(visitAnswerChangeRequests.isDeleted, false))
      .orderBy(
        sql`case when ${visitAnswerChangeRequests.status} = 'pending' then 0 else 1 end`,
        desc(visitAnswerChangeRequests.createdAt),
      )
      .limit(250);

    const requests = rows.map((row) => {
      const applicability = normalizeChangeRequestAnswerPayload({
        questionType: row.questionType,
        payload: asPlainRecord(row.requestedAnswerPayload),
      });
      return {
        id: row.id,
        status: row.status,
        createdAt: row.createdAt.toISOString(),
        updatedAt: row.updatedAt.toISOString(),
        reviewedByUserId: row.reviewedByUserId,
        reviewedAt: row.reviewedAt?.toISOString() ?? null,
        adminNote: row.adminNote,
        visitSessionId: row.visitSessionId,
        visitSessionQuestionId: row.visitSessionQuestionId,
        visitAnswerId: row.visitAnswerId,
        questionType: row.questionType,
        questionText: row.questionTextSnapshot,
        currentAnswerSnapshot: row.currentAnswerSnapshot ?? {},
        requestedAnswerPayload: row.requestedAnswerPayload ?? {},
        requestedAnswerSummary: row.requestedAnswerSummary,
        requestNote: row.requestNote,
        autoApplicable: applicability.ok,
        autoApplicabilityError: applicability.ok ? null : applicability.error,
        gm: {
          id: row.gmUserId,
          name: [row.gmFirstName, row.gmLastName].filter(Boolean).join(" ").trim() || row.gmEmail,
          email: row.gmEmail,
          region: row.gmRegion,
        },
        market: {
          id: row.marketId,
          name: row.marketName,
          address: row.marketAddress,
          postalCode: row.marketPostalCode,
          city: row.marketCity,
          region: row.marketRegion,
        },
        session: {
          id: row.visitSessionId,
          startedAt: row.sessionStartedAt?.toISOString() ?? null,
          submittedAt: row.sessionSubmittedAt?.toISOString() ?? null,
        },
        section: {
          section: row.section,
          campaignId: row.campaignId,
          campaignName: row.campaignName ?? "",
          fragebogenName: row.fragebogenName,
        },
      };
    });

    res.status(200).json({ requests });
  } catch (error) {
    next(error);
  }
});

adminCampaignsRouter.patch("/campaigns/answer-change-requests/:requestId/reject", async (req: AuthedRequest, res, next) => {
  try {
    if (req.authUser?.role !== "admin") {
      res.status(403).json({ error: "Nur Admins können ?nderungsanfragen prüfen.", code: "admin_required" });
      return;
    }

    const requestId = String(req.params.requestId ?? "");
    if (!isUuid(requestId)) {
      res.status(400).json({ error: "Ungültige Anfrage-ID.", code: "invalid_request_id" });
      return;
    }
    const parsed = adminChangeRequestDecisionSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültiges Entscheidungs-Payload.", code: "invalid_payload" });
      return;
    }
    const now = new Date();
    const [updated] = await db
      .update(visitAnswerChangeRequests)
      .set({
        status: "rejected",
        reviewedByUserId: req.authUser?.appUserId ?? null,
        reviewedAt: now,
        adminNote: parsed.data.adminNote || null,
        updatedAt: now,
      })
      .where(and(eq(visitAnswerChangeRequests.id, requestId), eq(visitAnswerChangeRequests.status, "pending"), eq(visitAnswerChangeRequests.isDeleted, false)))
      .returning({
        id: visitAnswerChangeRequests.id,
        status: visitAnswerChangeRequests.status,
        updatedAt: visitAnswerChangeRequests.updatedAt,
      });
    if (!updated) {
      res.status(404).json({ error: "Offene Anfrage wurde nicht gefunden.", code: "request_not_found" });
      return;
    }
    res.status(200).json({ ok: true, request: { id: updated.id, status: updated.status, updatedAt: updated.updatedAt.toISOString() } });
  } catch (error) {
    next(error);
  }
});

adminCampaignsRouter.patch("/campaigns/answer-change-requests/:requestId/approve", async (req: AuthedRequest, res, next) => {
  try {
    if (req.authUser?.role !== "admin") {
      res.status(403).json({ error: "Nur Admins können ?nderungsanfragen prüfen.", code: "admin_required" });
      return;
    }

    const requestId = String(req.params.requestId ?? "");
    if (!isUuid(requestId)) {
      res.status(400).json({ error: "Ungültige Anfrage-ID.", code: "invalid_request_id" });
      return;
    }
    const parsed = adminChangeRequestDecisionSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültiges Entscheidungs-Payload.", code: "invalid_payload" });
      return;
    }
    const actorUserId = req.authUser?.appUserId ?? null;
    const now = new Date();

    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`select id from ${visitAnswerChangeRequests} where ${visitAnswerChangeRequests.id} = ${requestId} for update`);
      const [requestRow] = await tx
        .select()
        .from(visitAnswerChangeRequests)
        .where(and(eq(visitAnswerChangeRequests.id, requestId), eq(visitAnswerChangeRequests.isDeleted, false)))
        .limit(1);
      if (!requestRow || requestRow.status !== "pending") {
        const error = new Error("Offene Anfrage wurde nicht gefunden.") as Error & { status?: number; code?: string };
        error.status = 404;
        error.code = "request_not_found";
        throw error;
      }

      const [session] = await tx
        .select({
          id: visitSessions.id,
          gmUserId: visitSessions.gmUserId,
          marketId: visitSessions.marketId,
          status: visitSessions.status,
          submittedAt: visitSessions.submittedAt,
        })
        .from(visitSessions)
        .where(and(eq(visitSessions.id, requestRow.visitSessionId), eq(visitSessions.isDeleted, false)))
        .limit(1);
      if (!session || session.status !== "submitted") {
        const error = new Error("Nur abgeschlossene Sessions können bearbeitet werden.") as Error & { status?: number; code?: string };
        error.status = 409;
        error.code = "session_not_submitted";
        throw error;
      }

      const [visitQuestion] = await tx
        .select({
          visitQuestionId: visitSessionQuestions.id,
          visitSessionSectionId: visitSessionSections.id,
          questionId: visitSessionQuestions.questionId,
          questionType: visitSessionQuestions.questionType,
          questionConfigSnapshot: visitSessionQuestions.questionConfigSnapshot,
        })
        .from(visitSessionQuestions)
        .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitSessionQuestions.visitSessionSectionId))
        .where(
          and(
            eq(visitSessionQuestions.id, requestRow.visitSessionQuestionId),
            eq(visitSessionQuestions.isDeleted, false),
            eq(visitSessionSections.visitSessionId, session.id),
            eq(visitSessionSections.isDeleted, false),
          ),
        )
        .limit(1);
      if (!visitQuestion) {
        const error = new Error("Frage in Session nicht gefunden.") as Error & { status?: number; code?: string };
        error.status = 404;
        error.code = "question_not_found";
        throw error;
      }

      const rawAnswer = normalizeChangeRequestAnswerPayload({
        questionType: visitQuestion.questionType,
        payload: asPlainRecord(requestRow.requestedAnswerPayload),
      });
      if (!rawAnswer.ok) {
        const error = new Error(rawAnswer.error) as Error & { status?: number; code?: string };
        error.status = rawAnswer.code === "change_request_manual_review_required" ? 422 : 400;
        error.code = rawAnswer.code;
        throw error;
      }

      const validation = buildVisitAnswerValidationResult(
        visitQuestion.questionType,
        (visitQuestion.questionConfigSnapshot ?? {}) as Record<string, unknown>,
        rawAnswer.answer,
      );
      if (validation.answerStatus !== "answered" || !validation.isValid) {
        const error = new Error(validation.validationError ?? "Angefragte Antwort ist nicht gueltig.") as Error & { status?: number; code?: string };
        error.status = 400;
        error.code = "requested_answer_invalid";
        throw error;
      }

      await tx.execute(sql`select id from ${visitSessionQuestions} where ${visitSessionQuestions.id} = ${visitQuestion.visitQuestionId} for update`);
      const [existing] = await tx
        .select()
        .from(visitAnswers)
        .where(and(eq(visitAnswers.visitSessionQuestionId, visitQuestion.visitQuestionId), eq(visitAnswers.isDeleted, false)))
        .limit(1);
      const beforeSnapshot = existing
        ? {
            answerStatus: existing.answerStatus,
            valueText: existing.valueText,
            valueNumber: existing.valueNumber == null ? null : String(existing.valueNumber),
            valueJson: (existing.valueJson as Record<string, unknown> | null) ?? null,
            isValid: existing.isValid,
            validationError: existing.validationError,
            version: existing.version,
          }
        : null;

      let answerId = existing?.id ?? null;
      let afterVersion = 1;
      if (existing) {
        const [updatedAnswer] = await tx
          .update(visitAnswers)
          .set({
            answerStatus: validation.answerStatus,
            valueText: validation.valueText,
            valueNumber: validation.valueNumber,
            valueJson: validation.valueJson,
            isValid: validation.isValid,
            validationError: validation.validationError,
            answeredAt: now,
            changedAt: now,
            version: existing.version + 1,
            updatedAt: now,
          })
          .where(eq(visitAnswers.id, existing.id))
          .returning({ id: visitAnswers.id, version: visitAnswers.version });
        answerId = updatedAnswer?.id ?? existing.id;
        afterVersion = updatedAnswer?.version ?? (existing.version + 1);
      } else {
        const [createdAnswer] = await tx
          .insert(visitAnswers)
          .values({
            visitSessionId: session.id,
            visitSessionSectionId: visitQuestion.visitSessionSectionId,
            visitSessionQuestionId: visitQuestion.visitQuestionId,
            questionId: visitQuestion.questionId,
            questionType: visitQuestion.questionType,
            answerStatus: validation.answerStatus,
            valueText: validation.valueText,
            valueNumber: validation.valueNumber,
            valueJson: validation.valueJson,
            isValid: validation.isValid,
            validationError: validation.validationError,
            answeredAt: now,
            changedAt: now,
            version: 1,
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          })
          .returning({ id: visitAnswers.id, version: visitAnswers.version });
        answerId = createdAnswer?.id ?? null;
        afterVersion = createdAnswer?.version ?? 1;
      }
      if (!answerId) throw new Error("Antwort konnte nicht gespeichert werden.");

      await tx
        .update(visitAnswerOptions)
        .set({ isDeleted: true, deletedAt: now, updatedAt: now })
        .where(and(eq(visitAnswerOptions.visitAnswerId, answerId), eq(visitAnswerOptions.isDeleted, false)));
      if (validation.options.length > 0) {
        await tx.insert(visitAnswerOptions).values(
          validation.options.map((opt) => ({
            visitAnswerId: answerId as string,
            optionRole: opt.optionRole,
            optionValue: opt.optionValue,
            orderIndex: opt.orderIndex,
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          })),
        );
      }

      await tx
        .update(visitAnswerMatrixCells)
        .set({ isDeleted: true, deletedAt: now, updatedAt: now })
        .where(and(eq(visitAnswerMatrixCells.visitAnswerId, answerId), eq(visitAnswerMatrixCells.isDeleted, false)));
      if (validation.matrixCells.length > 0) {
        await tx.insert(visitAnswerMatrixCells).values(
          validation.matrixCells.map((cell) => ({
            visitAnswerId: answerId as string,
            rowKey: cell.rowKey,
            columnKey: cell.columnKey,
            cellValueText: cell.cellValueText,
            cellValueDate: cell.cellValueDate,
            cellSelected: cell.cellSelected,
            orderIndex: cell.orderIndex,
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          })),
        );
      }

      await tx.insert(visitAnswerEvents).values({
        visitAnswerId: answerId as string,
        eventType: deriveVisitAnswerEventType({
          previousStatus: beforeSnapshot?.answerStatus ?? null,
          nextStatus: validation.answerStatus,
        }),
        payload: {
          source: "admin_change_request_approved",
          requestId: requestRow.id,
          sessionId: session.id,
          visitQuestionId: visitQuestion.visitQuestionId,
          questionId: visitQuestion.questionId,
          before: beforeSnapshot,
          after: {
            answerStatus: validation.answerStatus,
            valueText: validation.valueText,
            valueNumber: validation.valueNumber,
            valueJson: validation.valueJson,
            isValid: validation.isValid,
            validationError: validation.validationError,
            version: afterVersion,
          },
        },
        actorUserId,
        createdAt: now,
      });

      await tx
        .update(visitAnswerChangeRequests)
        .set({
          visitAnswerId: answerId as string,
          status: "approved",
          reviewedByUserId: actorUserId,
          reviewedAt: now,
          adminNote: parsed.data.adminNote || null,
          updatedAt: now,
        })
        .where(eq(visitAnswerChangeRequests.id, requestRow.id));

      await tx
        .update(visitSessions)
        .set({ lastSavedAt: now, updatedAt: now })
        .where(eq(visitSessions.id, session.id));

      await finalizeBonusForSubmittedVisitSessionTx(tx, {
        sessionId: session.id,
        gmUserId: session.gmUserId,
        marketId: session.marketId,
        submittedAt: session.submittedAt ?? now,
      });

      return {
        requestId: requestRow.id,
        answerId: answerId as string,
        sessionId: session.id,
        gmUserId: session.gmUserId,
        marketId: session.marketId,
        submittedAt: session.submittedAt ?? now,
      };
    });

    try {
      await enqueueIppRecalcForDate(result.marketId, result.submittedAt, "visit_answer_change_request_approved");
    } catch (enqueueError) {
      logger.error("answer_change_request_ipp_enqueue_failed", {
        action: "answer_change_request_approved",
        result: "failure",
        requestId: result.requestId,
        sessionId: result.sessionId,
        marketId: result.marketId,
        error: enqueueError instanceof Error ? enqueueError.message : String(enqueueError),
      });
    }
    try {
      await recomputeGmKpiCache(result.gmUserId);
    } catch (kpiError) {
      logger.error("answer_change_request_kpi_recompute_failed", {
        action: "answer_change_request_approved",
        result: "failure",
        requestId: result.requestId,
        sessionId: result.sessionId,
        gmUserId: result.gmUserId,
        error: kpiError instanceof Error ? kpiError.message : String(kpiError),
      });
    }

    res.status(200).json({ ok: true, request: { id: result.requestId, status: "approved" }, answerId: result.answerId });
  } catch (error) {
    const err = error as Error & { status?: number; code?: string };
    if (err.status) {
      res.status(err.status).json({ error: err.message, code: err.code });
      return;
    }
    next(error);
  }
});

adminCampaignsRouter.get("/campaigns/visit-session-delete-requests", async (req: AuthedRequest, res, next) => {
  try {
    if (req.authUser?.role !== "admin") {
      res.status(403).json({ error: "Nur Admins koennen Loeschanfragen pruefen.", code: "admin_required" });
      return;
    }

    const rows = await db
      .select({
        id: visitSessionDeleteRequests.id,
        visitSessionId: visitSessionDeleteRequests.visitSessionId,
        requestNote: visitSessionDeleteRequests.requestNote,
        status: visitSessionDeleteRequests.status,
        reviewedByUserId: visitSessionDeleteRequests.reviewedByUserId,
        reviewedAt: visitSessionDeleteRequests.reviewedAt,
        adminNote: visitSessionDeleteRequests.adminNote,
        createdAt: visitSessionDeleteRequests.createdAt,
        updatedAt: visitSessionDeleteRequests.updatedAt,
        gmUserId: users.id,
        gmFirstName: users.firstName,
        gmLastName: users.lastName,
        gmEmail: users.email,
        gmRegion: users.region,
        marketId: visitSessionDeleteRequests.marketId,
        marketName: visitSessionDeleteRequests.marketNameSnapshot,
        marketAddress: visitSessionDeleteRequests.marketAddressSnapshot,
        marketPostalCode: visitSessionDeleteRequests.marketPostalCodeSnapshot,
        marketCity: visitSessionDeleteRequests.marketCitySnapshot,
        campaignSummary: visitSessionDeleteRequests.campaignSummarySnapshot,
        sectionSummary: visitSessionDeleteRequests.sectionSummarySnapshot,
        sessionStartedAt: visitSessionDeleteRequests.sessionStartedAtSnapshot,
        sessionSubmittedAt: visitSessionDeleteRequests.sessionSubmittedAtSnapshot,
      })
      .from(visitSessionDeleteRequests)
      .innerJoin(users, eq(users.id, visitSessionDeleteRequests.gmUserId))
      .where(eq(visitSessionDeleteRequests.isDeleted, false))
      .orderBy(
        sql`case when ${visitSessionDeleteRequests.status} = 'pending' then 0 else 1 end`,
        desc(visitSessionDeleteRequests.createdAt),
      )
      .limit(250);

    const requests = rows.map((row) => ({
      id: row.id,
      status: row.status,
      createdAt: row.createdAt.toISOString(),
      updatedAt: row.updatedAt.toISOString(),
      reviewedByUserId: row.reviewedByUserId,
      reviewedAt: row.reviewedAt?.toISOString() ?? null,
      adminNote: row.adminNote,
      visitSessionId: row.visitSessionId,
      requestNote: row.requestNote,
      campaignSummary: row.campaignSummary,
      sectionSummary: row.sectionSummary,
      gm: {
        id: row.gmUserId,
        name: [row.gmFirstName, row.gmLastName].filter(Boolean).join(" ").trim() || row.gmEmail,
        email: row.gmEmail,
        region: row.gmRegion,
      },
      market: {
        id: row.marketId,
        name: row.marketName,
        address: row.marketAddress,
        postalCode: row.marketPostalCode,
        city: row.marketCity,
        region: null,
      },
      session: {
        id: row.visitSessionId,
        startedAt: row.sessionStartedAt?.toISOString() ?? null,
        submittedAt: row.sessionSubmittedAt?.toISOString() ?? null,
      },
    }));

    res.status(200).json({ requests });
  } catch (error) {
    next(error);
  }
});

adminCampaignsRouter.patch("/campaigns/visit-session-delete-requests/:requestId/reject", async (req: AuthedRequest, res, next) => {
  try {
    if (req.authUser?.role !== "admin") {
      res.status(403).json({ error: "Nur Admins koennen Loeschanfragen pruefen.", code: "admin_required" });
      return;
    }

    const requestId = String(req.params.requestId ?? "");
    if (!isUuid(requestId)) {
      res.status(400).json({ error: "Ungueltige Anfrage-ID.", code: "invalid_request_id" });
      return;
    }
    const parsed = adminChangeRequestDecisionSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltiges Entscheidungs-Payload.", code: "invalid_payload" });
      return;
    }
    const now = new Date();
    const [updated] = await db
      .update(visitSessionDeleteRequests)
      .set({
        status: "rejected",
        reviewedByUserId: req.authUser?.appUserId ?? null,
        reviewedAt: now,
        adminNote: parsed.data.adminNote || null,
        updatedAt: now,
      })
      .where(and(eq(visitSessionDeleteRequests.id, requestId), eq(visitSessionDeleteRequests.status, "pending"), eq(visitSessionDeleteRequests.isDeleted, false)))
      .returning({
        id: visitSessionDeleteRequests.id,
        status: visitSessionDeleteRequests.status,
        updatedAt: visitSessionDeleteRequests.updatedAt,
      });
    if (!updated) {
      res.status(404).json({ error: "Offene Loeschanfrage wurde nicht gefunden.", code: "request_not_found" });
      return;
    }
    res.status(200).json({ ok: true, request: { id: updated.id, status: updated.status, updatedAt: updated.updatedAt.toISOString() } });
  } catch (error) {
    next(error);
  }
});

adminCampaignsRouter.patch("/campaigns/visit-session-delete-requests/:requestId/approve", async (req: AuthedRequest, res, next) => {
  try {
    if (req.authUser?.role !== "admin") {
      res.status(403).json({ error: "Nur Admins koennen Loeschanfragen pruefen.", code: "admin_required" });
      return;
    }

    const requestId = String(req.params.requestId ?? "");
    if (!isUuid(requestId)) {
      res.status(400).json({ error: "Ungueltige Anfrage-ID.", code: "invalid_request_id" });
      return;
    }
    const parsed = adminChangeRequestDecisionSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltiges Entscheidungs-Payload.", code: "invalid_payload" });
      return;
    }
    const actorUserId = req.authUser?.appUserId ?? null;
    const now = new Date();

    const result = await db.transaction(async (tx) => {
      await tx.execute(sql`select id from ${visitSessionDeleteRequests} where ${visitSessionDeleteRequests.id} = ${requestId} for update`);
      const [requestRow] = await tx
        .select()
        .from(visitSessionDeleteRequests)
        .where(and(eq(visitSessionDeleteRequests.id, requestId), eq(visitSessionDeleteRequests.isDeleted, false)))
        .limit(1);
      if (!requestRow || requestRow.status !== "pending") {
        const error = new Error("Offene Loeschanfrage wurde nicht gefunden.") as Error & { status?: number; code?: string };
        error.status = 404;
        error.code = "request_not_found";
        throw error;
      }

      const [session] = await tx
        .select({
          id: visitSessions.id,
          gmUserId: visitSessions.gmUserId,
          marketId: visitSessions.marketId,
          status: visitSessions.status,
          submittedAt: visitSessions.submittedAt,
        })
        .from(visitSessions)
        .where(and(eq(visitSessions.id, requestRow.visitSessionId), eq(visitSessions.isDeleted, false)))
        .limit(1);
      if (!session) {
        const error = new Error("Session wurde nicht gefunden.") as Error & { status?: number; code?: string };
        error.status = 404;
        error.code = "session_not_found";
        throw error;
      }
      if (session.status !== "submitted" || !session.submittedAt) {
        const error = new Error("Nur abgeschlossene Sessions koennen geloescht werden.") as Error & { status?: number; code?: string };
        error.status = 409;
        error.code = "session_not_submitted";
        throw error;
      }

      const waveRows = await tx
        .select({ waveId: praemienGmWaveContributions.waveId })
        .from(praemienGmWaveContributions)
        .where(eq(praemienGmWaveContributions.visitSessionId, session.id));
      const affectedWaveIds = normalizeUnique(waveRows.map((row) => row.waveId));

      const sectionRows = await tx
        .select({ id: visitSessionSections.id })
        .from(visitSessionSections)
        .where(and(eq(visitSessionSections.visitSessionId, session.id), eq(visitSessionSections.isDeleted, false)));
      const sectionIds = sectionRows.map((row) => row.id);

      const questionRows = sectionIds.length === 0
        ? []
        : await tx
            .select({ id: visitSessionQuestions.id })
            .from(visitSessionQuestions)
            .where(and(inArray(visitSessionQuestions.visitSessionSectionId, sectionIds), eq(visitSessionQuestions.isDeleted, false)));
      const questionIds = questionRows.map((row) => row.id);

      const answerRows = await tx
        .select({ id: visitAnswers.id })
        .from(visitAnswers)
        .where(and(eq(visitAnswers.visitSessionId, session.id), eq(visitAnswers.isDeleted, false)));
      const answerIds = answerRows.map((row) => row.id);

      const photoRows = answerIds.length === 0
        ? []
        : await tx
            .select({ id: visitAnswerPhotos.id })
            .from(visitAnswerPhotos)
            .where(and(inArray(visitAnswerPhotos.visitAnswerId, answerIds), eq(visitAnswerPhotos.isDeleted, false)));
      const photoIds = photoRows.map((row) => row.id);

      if (photoIds.length > 0) {
        await tx
          .update(visitAnswerPhotoTags)
          .set({ isDeleted: true, deletedAt: now, updatedAt: now })
          .where(and(inArray(visitAnswerPhotoTags.visitAnswerPhotoId, photoIds), eq(visitAnswerPhotoTags.isDeleted, false)));
      }
      if (answerIds.length > 0) {
        await tx
          .update(visitAnswerPhotos)
          .set({ isDeleted: true, deletedAt: now, updatedAt: now })
          .where(and(inArray(visitAnswerPhotos.visitAnswerId, answerIds), eq(visitAnswerPhotos.isDeleted, false)));
        await tx
          .update(visitAnswerOptions)
          .set({ isDeleted: true, deletedAt: now, updatedAt: now })
          .where(and(inArray(visitAnswerOptions.visitAnswerId, answerIds), eq(visitAnswerOptions.isDeleted, false)));
        await tx
          .update(visitAnswerMatrixCells)
          .set({ isDeleted: true, deletedAt: now, updatedAt: now })
          .where(and(inArray(visitAnswerMatrixCells.visitAnswerId, answerIds), eq(visitAnswerMatrixCells.isDeleted, false)));
      }
      if (questionIds.length > 0) {
        await tx
          .update(visitQuestionComments)
          .set({ isDeleted: true, deletedAt: now, updatedAt: now })
          .where(and(inArray(visitQuestionComments.visitSessionQuestionId, questionIds), eq(visitQuestionComments.isDeleted, false)));
        await tx
          .update(visitSessionQuestions)
          .set({ isDeleted: true, deletedAt: now, updatedAt: now })
          .where(and(inArray(visitSessionQuestions.id, questionIds), eq(visitSessionQuestions.isDeleted, false)));
      }
      if (sectionIds.length > 0) {
        await tx
          .update(visitSessionSections)
          .set({ isDeleted: true, deletedAt: now, updatedAt: now })
          .where(and(inArray(visitSessionSections.id, sectionIds), eq(visitSessionSections.isDeleted, false)));
      }
      await tx
        .update(visitAnswers)
        .set({ isDeleted: true, deletedAt: now, updatedAt: now })
        .where(and(eq(visitAnswers.visitSessionId, session.id), eq(visitAnswers.isDeleted, false)));
      await tx
        .update(visitAnswerChangeRequests)
        .set({ status: "cancelled", isDeleted: true, deletedAt: now, updatedAt: now })
        .where(and(eq(visitAnswerChangeRequests.visitSessionId, session.id), eq(visitAnswerChangeRequests.isDeleted, false)));
      await tx
        .update(visitSessions)
        .set({
          status: "cancelled",
          cancelledAt: now,
          lastSavedAt: now,
          isDeleted: true,
          deletedAt: now,
          updatedAt: now,
        })
        .where(and(eq(visitSessions.id, session.id), eq(visitSessions.isDeleted, false)));
      await tx
        .update(visitSessionDeleteRequests)
        .set({
          status: "approved",
          reviewedByUserId: actorUserId,
          reviewedAt: now,
          adminNote: parsed.data.adminNote || null,
          updatedAt: now,
        })
        .where(eq(visitSessionDeleteRequests.id, requestRow.id));

      return {
        requestId: requestRow.id,
        sessionId: session.id,
        gmUserId: session.gmUserId,
        marketId: session.marketId,
        submittedAt: session.submittedAt,
        affectedWaveIds,
      };
    });

    const affectedGmUserIds = new Set<string>([result.gmUserId]);
    for (const waveId of result.affectedWaveIds) {
      try {
        const recomputeResult = await db.transaction((tx) => recomputeBonusWaveTx(tx, waveId));
        for (const gmUserId of recomputeResult.affectedGmUserIds) affectedGmUserIds.add(gmUserId);
      } catch (bonusError) {
        logger.error("visit_session_delete_request_bonus_recompute_failed", {
          action: "visit_session_delete_request_approved",
          result: "failure",
          requestId: result.requestId,
          sessionId: result.sessionId,
          waveId,
          error: bonusError instanceof Error ? bonusError.message : String(bonusError),
        });
      }
    }

    try {
      await enqueueIppRecalcForDate(result.marketId, result.submittedAt, "visit_session_delete_request_approved");
    } catch (enqueueError) {
      logger.error("visit_session_delete_request_ipp_enqueue_failed", {
        action: "visit_session_delete_request_approved",
        result: "failure",
        requestId: result.requestId,
        sessionId: result.sessionId,
        marketId: result.marketId,
        error: enqueueError instanceof Error ? enqueueError.message : String(enqueueError),
      });
    }

    for (const gmUserId of affectedGmUserIds) {
      try {
        await recomputeGmKpiCache(gmUserId);
      } catch (kpiError) {
        logger.error("visit_session_delete_request_kpi_recompute_failed", {
          action: "visit_session_delete_request_approved",
          result: "failure",
          requestId: result.requestId,
          sessionId: result.sessionId,
          gmUserId,
          error: kpiError instanceof Error ? kpiError.message : String(kpiError),
        });
      }
    }

    res.status(200).json({ ok: true, request: { id: result.requestId, status: "approved" }, sessionId: result.sessionId });
  } catch (error) {
    const err = error as Error & { status?: number; code?: string };
    if (err.status) {
      res.status(err.status).json({ error: err.message, code: err.code });
      return;
    }
    next(error);
  }
});

adminCampaignsRouter.patch("/campaigns/visit-sessions/:sessionId/answers", async (req: AuthedRequest, res, next) => {
  try {
    const sessionId = String(req.params.sessionId ?? "");
    if (!isUuid(sessionId)) {
      res.status(400).json({ error: "Ungültige Session-ID.", code: "invalid_session_id" });
      return;
    }

    const parsed = adminSubmittedVisitAnswerPatchSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültiges Antwort-Payload.", code: "invalid_payload" });
      return;
    }

    const [session] = await db
      .select({
        id: visitSessions.id,
        gmUserId: visitSessions.gmUserId,
        marketId: visitSessions.marketId,
        status: visitSessions.status,
        submittedAt: visitSessions.submittedAt,
      })
      .from(visitSessions)
      .where(and(eq(visitSessions.id, sessionId), eq(visitSessions.isDeleted, false)))
      .limit(1);
    if (!session) {
      res.status(404).json({ error: "Session nicht gefunden.", code: "session_not_found" });
      return;
    }
    if (session.status !== "submitted") {
      res.status(409).json({ error: "Nur abgeschlossene Sessions können bearbeitet werden.", code: "session_not_submitted" });
      return;
    }

    const [visitQuestion] = await db
      .select({
        visitQuestionId: visitSessionQuestions.id,
        visitSessionSectionId: visitSessionSections.id,
        questionId: visitSessionQuestions.questionId,
        questionType: visitSessionQuestions.questionType,
        questionConfigSnapshot: visitSessionQuestions.questionConfigSnapshot,
      })
      .from(visitSessionQuestions)
      .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitSessionQuestions.visitSessionSectionId))
      .where(
        and(
          eq(visitSessionQuestions.id, parsed.data.visitQuestionId),
          eq(visitSessionQuestions.isDeleted, false),
          eq(visitSessionSections.visitSessionId, session.id),
          eq(visitSessionSections.isDeleted, false),
        ),
      )
      .limit(1);
    if (!visitQuestion) {
      res.status(404).json({ error: "Frage in Session nicht gefunden.", code: "question_not_found" });
      return;
    }
    if (visitQuestion.questionType === "photo") {
      res.status(400).json({
        error: "Foto-Fragen können in diesem Schritt nicht bearbeitet werden.",
        code: "photo_edit_not_supported",
      });
      return;
    }

    const validation = buildVisitAnswerValidationResult(
      visitQuestion.questionType,
      (visitQuestion.questionConfigSnapshot ?? {}) as Record<string, unknown>,
      parsed.data.answer,
    );
    const actorUserId = req.authUser?.appUserId ?? null;
    const now = new Date();

    const result = await db.transaction(async (tx) => {
      const siblingQuestions = await tx
        .select({
          visitQuestionId: visitSessionQuestions.id,
          visitSessionSectionId: visitSessionSections.id,
          questionId: visitSessionQuestions.questionId,
          questionType: visitSessionQuestions.questionType,
        })
        .from(visitSessionQuestions)
        .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitSessionQuestions.visitSessionSectionId))
        .where(
          and(
            eq(visitSessionSections.visitSessionId, session.id),
            eq(visitSessionSections.isDeleted, false),
            eq(visitSessionQuestions.questionId, visitQuestion.questionId),
            eq(visitSessionQuestions.isDeleted, false),
          ),
        );
      const siblingQuestionIds = siblingQuestions.map((row) => row.visitQuestionId);
      if (siblingQuestionIds.length === 0) {
        throw new Error("Keine zugehörigen Fragen in der Session gefunden.");
      }

      await tx.execute(
        sql`SELECT id FROM ${visitSessionQuestions} WHERE ${inArray(visitSessionQuestions.id, siblingQuestionIds)} FOR UPDATE`,
      );

      const existingAnswers = await tx
        .select()
        .from(visitAnswers)
        .where(and(inArray(visitAnswers.visitSessionQuestionId, siblingQuestionIds), eq(visitAnswers.isDeleted, false)));
      const existingAnswerByVisitQuestionId = new Map(existingAnswers.map((row) => [row.visitSessionQuestionId, row]));

      const resultByVisitQuestionId = new Map<string, {
        answerId: string;
        answerStatus: "unanswered" | "answered" | "invalid" | "hidden_by_rule" | "skipped";
        isValid: boolean;
        validationError: string | null;
      }>();

      const auditEvents: Array<typeof visitAnswerEvents.$inferInsert> = [];

      for (const sibling of siblingQuestions) {
        const existing = existingAnswerByVisitQuestionId.get(sibling.visitQuestionId);
        const beforeSnapshot = existing
          ? {
              answerStatus: existing.answerStatus,
              valueText: existing.valueText,
              valueNumber: existing.valueNumber == null ? null : String(existing.valueNumber),
              valueJson: (existing.valueJson as Record<string, unknown> | null) ?? null,
              isValid: existing.isValid,
              validationError: existing.validationError,
              version: existing.version,
            }
          : null;

        let answerId = existing?.id ?? null;
        let afterStatus: "unanswered" | "answered" | "invalid";
        let afterIsValid: boolean;
        let afterValidationError: string | null;
        let afterVersion = 1;

        if (existing) {
          const [updated] = await tx
            .update(visitAnswers)
            .set({
              answerStatus: validation.answerStatus,
              valueText: validation.valueText,
              valueNumber: validation.valueNumber,
              valueJson: validation.valueJson,
              isValid: validation.isValid,
              validationError: validation.validationError,
              answeredAt: validation.answerStatus === "answered" ? now : existing.answeredAt,
              changedAt: now,
              version: existing.version + 1,
              updatedAt: now,
            })
            .where(eq(visitAnswers.id, existing.id))
            .returning();
          answerId = updated?.id ?? existing.id;
          afterStatus = (updated?.answerStatus as "unanswered" | "answered" | "invalid" | undefined) ?? validation.answerStatus;
          afterIsValid = updated?.isValid ?? validation.isValid;
          afterValidationError = updated?.validationError ?? validation.validationError;
          afterVersion = updated?.version ?? (existing.version + 1);
        } else {
          const [created] = await tx
            .insert(visitAnswers)
            .values({
              visitSessionId: session.id,
              visitSessionSectionId: sibling.visitSessionSectionId,
              visitSessionQuestionId: sibling.visitQuestionId,
              questionId: sibling.questionId,
              questionType: sibling.questionType,
              answerStatus: validation.answerStatus,
              valueText: validation.valueText,
              valueNumber: validation.valueNumber,
              valueJson: validation.valueJson,
              isValid: validation.isValid,
              validationError: validation.validationError,
              answeredAt: validation.answerStatus === "answered" ? now : null,
              changedAt: now,
              version: 1,
              isDeleted: false,
              deletedAt: null,
              createdAt: now,
              updatedAt: now,
            })
            .returning();
          answerId = created?.id ?? null;
          afterStatus = (created?.answerStatus as "unanswered" | "answered" | "invalid" | undefined) ?? validation.answerStatus;
          afterIsValid = created?.isValid ?? validation.isValid;
          afterValidationError = created?.validationError ?? validation.validationError;
          afterVersion = created?.version ?? 1;
        }
        if (!answerId) throw new Error("Antwort konnte nicht gespeichert werden.");

        await tx
          .update(visitAnswerOptions)
          .set({ isDeleted: true, deletedAt: now, updatedAt: now })
          .where(and(eq(visitAnswerOptions.visitAnswerId, answerId), eq(visitAnswerOptions.isDeleted, false)));
        if (validation.options.length > 0) {
          await tx.insert(visitAnswerOptions).values(
            validation.options.map((opt) => ({
              visitAnswerId: answerId as string,
              optionRole: opt.optionRole,
              optionValue: opt.optionValue,
              orderIndex: opt.orderIndex,
              isDeleted: false,
              deletedAt: null,
              createdAt: now,
              updatedAt: now,
            })),
          );
        }

        await tx
          .update(visitAnswerMatrixCells)
          .set({ isDeleted: true, deletedAt: now, updatedAt: now })
          .where(and(eq(visitAnswerMatrixCells.visitAnswerId, answerId), eq(visitAnswerMatrixCells.isDeleted, false)));
        if (validation.matrixCells.length > 0) {
          await tx.insert(visitAnswerMatrixCells).values(
            validation.matrixCells.map((cell) => ({
              visitAnswerId: answerId as string,
              rowKey: cell.rowKey,
              columnKey: cell.columnKey,
              cellValueText: cell.cellValueText,
              cellValueDate: cell.cellValueDate,
              cellSelected: cell.cellSelected,
              orderIndex: cell.orderIndex,
              isDeleted: false,
              deletedAt: null,
              createdAt: now,
              updatedAt: now,
            })),
          );
        }

        if (parsed.data.comment !== undefined) {
          const [existingComment] = await tx
            .select()
            .from(visitQuestionComments)
            .where(and(eq(visitQuestionComments.visitSessionQuestionId, sibling.visitQuestionId), eq(visitQuestionComments.isDeleted, false)))
            .limit(1);
          if (parsed.data.comment.trim().length === 0) {
            if (existingComment) {
              await tx
                .update(visitQuestionComments)
                .set({ isDeleted: true, deletedAt: now, updatedAt: now })
                .where(eq(visitQuestionComments.id, existingComment.id));
            }
          } else if (existingComment) {
            await tx
              .update(visitQuestionComments)
              .set({ commentText: parsed.data.comment, commentedAt: now, updatedAt: now })
              .where(eq(visitQuestionComments.id, existingComment.id));
          } else {
            await tx.insert(visitQuestionComments).values({
              visitSessionQuestionId: sibling.visitQuestionId,
              commentText: parsed.data.comment,
              commentedAt: now,
              isDeleted: false,
              deletedAt: null,
              createdAt: now,
              updatedAt: now,
            });
          }
        }

        auditEvents.push({
          visitAnswerId: answerId as string,
          eventType: deriveVisitAnswerEventType({
            previousStatus: beforeSnapshot?.answerStatus ?? null,
            nextStatus: afterStatus,
          }),
          payload: {
            source: "admin_submitted_edit",
            sessionId: session.id,
            visitQuestionId: sibling.visitQuestionId,
            questionId: sibling.questionId,
            before: beforeSnapshot,
            after: {
              answerStatus: afterStatus,
              valueText: validation.valueText,
              valueNumber: validation.valueNumber,
              valueJson: validation.valueJson,
              isValid: afterIsValid,
              validationError: afterValidationError,
              version: afterVersion,
            },
            ...(parsed.data.comment !== undefined ? { comment: parsed.data.comment } : {}),
          },
          actorUserId,
          createdAt: now,
        });

        resultByVisitQuestionId.set(sibling.visitQuestionId, {
          answerId: answerId as string,
          answerStatus: afterStatus,
          isValid: afterIsValid,
          validationError: afterValidationError,
        });
      }

      if (auditEvents.length > 0) {
        await tx.insert(visitAnswerEvents).values(auditEvents);
      }

      const sections = await tx
        .select({ id: visitSessionSections.id })
        .from(visitSessionSections)
        .where(and(eq(visitSessionSections.visitSessionId, session.id), eq(visitSessionSections.isDeleted, false)));
      const sectionIds = sections.map((row) => row.id);
      const allQuestions =
        sectionIds.length === 0
          ? []
          : await tx
              .select({
                id: visitSessionQuestions.id,
                questionId: visitSessionQuestions.questionId,
                questionType: visitSessionQuestions.questionType,
                questionTextSnapshot: visitSessionQuestions.questionTextSnapshot,
                questionConfigSnapshot: visitSessionQuestions.questionConfigSnapshot,
                questionRulesSnapshot: visitSessionQuestions.questionRulesSnapshot,
                requiredSnapshot: visitSessionQuestions.requiredSnapshot,
                appliesToMarketChainSnapshot: visitSessionQuestions.appliesToMarketChainSnapshot,
              })
              .from(visitSessionQuestions)
              .where(and(inArray(visitSessionQuestions.visitSessionSectionId, sectionIds), eq(visitSessionQuestions.isDeleted, false)));
      const questionIds = allQuestions.map((row) => row.id);
      const allAnswers =
        questionIds.length === 0
          ? []
          : await tx
              .select({
                id: visitAnswers.id,
                visitSessionQuestionId: visitAnswers.visitSessionQuestionId,
                answerStatus: visitAnswers.answerStatus,
                valueText: visitAnswers.valueText,
                valueNumber: visitAnswers.valueNumber,
                valueJson: visitAnswers.valueJson,
                isValid: visitAnswers.isValid,
              })
              .from(visitAnswers)
              .where(and(inArray(visitAnswers.visitSessionQuestionId, questionIds), eq(visitAnswers.isDeleted, false)));
      const answerIds = allAnswers.map((row) => row.id);
      const allPhotos =
        answerIds.length === 0
          ? []
          : await tx
              .select({
                id: visitAnswerPhotos.id,
                visitAnswerId: visitAnswerPhotos.visitAnswerId,
                storagePath: visitAnswerPhotos.storagePath,
              })
              .from(visitAnswerPhotos)
              .where(and(inArray(visitAnswerPhotos.visitAnswerId, answerIds), eq(visitAnswerPhotos.isDeleted, false)));
      const photoIds = allPhotos.map((row) => row.id);
      const allPhotoTags =
        photoIds.length === 0
          ? []
          : await tx
              .select({
                visitAnswerPhotoId: visitAnswerPhotoTags.visitAnswerPhotoId,
              })
              .from(visitAnswerPhotoTags)
              .where(and(inArray(visitAnswerPhotoTags.visitAnswerPhotoId, photoIds), eq(visitAnswerPhotoTags.isDeleted, false)));

      const { missingRequired } = computeMissingRequiredQuestions({
        questions: allQuestions.map((question) => ({
          id: question.id,
          questionId: question.questionId,
          questionType: question.questionType,
          questionTextSnapshot: question.questionTextSnapshot,
          questionConfigSnapshot: (question.questionConfigSnapshot ?? {}) as Record<string, unknown>,
          questionRulesSnapshot: (question.questionRulesSnapshot ?? []) as Array<Record<string, unknown>>,
          requiredSnapshot: question.requiredSnapshot,
          appliesToMarketChainSnapshot: Boolean(question.appliesToMarketChainSnapshot),
        })),
        answers: allAnswers.map((answer) => ({
          id: answer.id,
          visitSessionQuestionId: answer.visitSessionQuestionId,
          answerStatus: answer.answerStatus,
          valueText: answer.valueText,
          valueNumber: answer.valueNumber == null ? null : String(answer.valueNumber),
          valueJson: (answer.valueJson as Record<string, unknown> | null) ?? null,
          isValid: answer.isValid,
        })),
        photos: allPhotos,
        photoTags: allPhotoTags,
      });
      if (missingRequired.length > 0) {
        const error = new Error("Edit würde eine ungültige Session erzeugen.") as Error & {
          code?: string;
          missingRequired?: Array<{
            visitQuestionId: string;
            questionId: string;
            questionText: string;
            questionType: string;
          }>;
        };
        error.code = "visit_edit_incomplete_required";
        error.missingRequired = missingRequired;
        throw error;
      }

      await tx
        .update(visitSessions)
        .set({ lastSavedAt: now, updatedAt: now })
        .where(eq(visitSessions.id, session.id));

      await finalizeBonusForSubmittedVisitSessionTx(tx, {
        sessionId: session.id,
        gmUserId: session.gmUserId,
        marketId: session.marketId,
        submittedAt: session.submittedAt ?? now,
      });

      const primaryResult = resultByVisitQuestionId.get(visitQuestion.visitQuestionId);
      if (!primaryResult) throw new Error("Antwort konnte nicht gespeichert werden.");
      return primaryResult;
    });

    try {
      await enqueueIppRecalcForDate(session.marketId, session.submittedAt ?? now, "visit_session_admin_edit");
    } catch (enqueueError) {
      logger.error("admin_visit_answer_edit_ipp_enqueue_failed", {
        action: "admin_visit_answer_edit",
        result: "failure",
        sessionId: session.id,
        marketId: session.marketId,
        error: enqueueError instanceof Error ? enqueueError.message : String(enqueueError),
      });
    }
    try {
      await recomputeGmKpiCache(session.gmUserId);
    } catch (kpiError) {
      logger.error("admin_visit_answer_edit_kpi_recompute_failed", {
        action: "admin_visit_answer_edit",
        result: "failure",
        sessionId: session.id,
        gmUserId: session.gmUserId,
        error: kpiError instanceof Error ? kpiError.message : String(kpiError),
      });
    }

    res.status(200).json({ ok: true, result });
  } catch (error) {
    if (error instanceof Error && (error as { code?: string }).code === "visit_edit_incomplete_required") {
      res.status(409).json({
        error: "Änderung verletzt Pflicht-/Regelzustand der Session.",
        code: "visit_edit_incomplete_required",
        missingRequired: (error as { missingRequired?: unknown }).missingRequired ?? [],
      });
      return;
    }
    next(error);
  }
});

adminCampaignsRouter.post("/campaigns", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const parsed = createCampaignSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "campaign_create_invalid_payload", {
        req,
        action: "campaign_create",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige Kampagne.", code: "invalid_payload" });
      return;
    }

    const now = new Date();
    const auditUserId = await resolveAuditUserId(req.authUser?.appUserId);
    const schedule = normalizeSchedule(parsed.data);
    const autoFlexMarketIds = parsed.data.section === "flex"
      ? await loadActiveUniversumMarketIds()
      : null;
    const assignments = normalizeAssignments({
      section: parsed.data.section,
      marketIds: autoFlexMarketIds ?? parsed.data.marketIds,
      ...(parsed.data.section !== "flex" && parsed.data.assignments ? { assignments: parsed.data.assignments } : {}),
    });
    const marketIds = assignments.map((assignment) => assignment.marketId);
    if (parsed.data.section === "flex" && marketIds.length === 0) {
      throw new CampaignDomainError("invalid_payload", 400, "Keine aktiven Universumsmärkte für Flexkampagne gefunden.");
    }
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

    const createResult = await db.transaction(async (tx) => {
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
        for (let index = 0; index < assignments.length; index += CAMPAIGN_ASSIGNMENT_INSERT_BATCH_SIZE) {
          const batch = assignments.slice(index, index + CAMPAIGN_ASSIGNMENT_INSERT_BATCH_SIZE);
          await tx.insert(campaignMarketAssignments).values(
            batch.map((assignment) => ({
              campaignId: created.id,
              marketId: assignment.marketId,
              gmUserId: assignment.gmUserId,
              assignmentSlot: assignment.assignmentSlot,
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
      }

      let historyRow: typeof campaignFragebogenHistory.$inferSelect | null = null;
      if (parsed.data.currentFragebogenId) {
        const [createdHistory] = await tx
          .insert(campaignFragebogenHistory)
          .values({
            campaignId: created.id,
            fromFragebogenId: null,
            toFragebogenId: parsed.data.currentFragebogenId,
            changedAt: now,
            changedByUserId: auditUserId,
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          })
          .returning();
        historyRow = createdHistory ?? null;
      }
      return { campaign: created, historyRow };
    });

    const campaign = {
      id: createResult.campaign.id,
      name: createResult.campaign.name,
      section: createResult.campaign.section,
      currentFragebogenId: createResult.campaign.currentFragebogenId,
      currentFragebogenName: null,
      status: deriveEffectiveCampaignStatus({
        status: createResult.campaign.status,
        scheduleType: createResult.campaign.scheduleType,
        startDate: createResult.campaign.startDate,
        endDate: createResult.campaign.endDate,
      }),
      scheduleType: createResult.campaign.scheduleType,
      startDate: createResult.campaign.startDate ? String(createResult.campaign.startDate) : null,
      endDate: createResult.campaign.endDate ? String(createResult.campaign.endDate) : null,
      marketIds: normalizeUnique(assignments.map((assignment) => assignment.marketId)),
      assignments: assignments.map((assignment) => ({
        marketId: assignment.marketId,
        gmUserId: assignment.gmUserId,
        gmName: null,
        assignmentSlot: assignment.assignmentSlot,
        visitTargetCount: assignment.visitTargetCount,
        currentVisitsCount: 0,
      })),
      history: createResult.historyRow
        ? [
            {
              id: createResult.historyRow.id,
              fromFragebogenId: createResult.historyRow.fromFragebogenId,
              toFragebogenId: createResult.historyRow.toFragebogenId,
              changedAt: createResult.historyRow.changedAt.toISOString(),
            },
          ]
        : [],
      createdAt: createResult.campaign.createdAt.toISOString(),
      updatedAt: createResult.campaign.updatedAt.toISOString(),
    };
    res.status(201).json({ campaign });
    logAction("info", "campaign_create_success", {
      req,
      action: "campaign_create",
      result: "success",
      statusCode: 201,
      requestClass: "success",
      startedAtNs,
      details: {
        campaignId: createResult.campaign.id,
        section: parsed.data.section,
        assignmentsCount: assignments.length,
      },
    });
  } catch (error) {
    if (error instanceof CampaignOverlapConflictError) {
      logAction("warn", "campaign_create_overlap_conflict", {
        req,
        action: "campaign_create",
        result: "rejected",
        statusCode: error.status,
        requestClass: "client_error",
        startedAtNs,
        details: { conflictCount: error.conflicts.length },
      });
      res.status(error.status).json({ error: error.message, code: error.code, conflicts: error.conflicts });
      return;
    }
    if (error instanceof CampaignDomainError) {
      logAction("warn", "campaign_create_domain_error", {
        req,
        action: "campaign_create",
        result: "rejected",
        statusCode: error.status,
        requestClass: "client_error",
        startedAtNs,
        details: { code: error.code, message: error.message },
      });
      respondDomainError(res, error);
      return;
    }
    logAction("error", "campaign_create_failed", {
      req,
      action: "campaign_create",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error,
    });
    markErrorAsLogged(error);
    next(error);
  }
});

adminCampaignsRouter.patch("/campaigns/:id", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      logAction("warn", "campaign_update_invalid_id", {
        req,
        action: "campaign_update",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige Kampagnen-ID.", code: "invalid_id" });
      return;
    }
    const parsed = updateCampaignSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "campaign_update_invalid_payload", {
        req,
        action: "campaign_update",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige Kampagnen-?nderung.", code: "invalid_payload" });
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
          assignmentSlot: campaignMarketAssignments.assignmentSlot,
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
          assignmentSlot: assignment.assignmentSlot,
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
    logAction("info", "campaign_update_success", {
      req,
      action: "campaign_update",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { campaignId, status: row.status },
    });
  } catch (error) {
    if (error instanceof CampaignOverlapConflictError) {
      logAction("warn", "campaign_update_overlap_conflict", {
        req,
        action: "campaign_update",
        result: "rejected",
        statusCode: error.status,
        requestClass: "client_error",
        startedAtNs,
        details: { conflictCount: error.conflicts.length },
      });
      res.status(error.status).json({ error: error.message, code: error.code, conflicts: error.conflicts });
      return;
    }
    if (error instanceof CampaignDomainError) {
      logAction("warn", "campaign_update_domain_error", {
        req,
        action: "campaign_update",
        result: "rejected",
        statusCode: error.status,
        requestClass: "client_error",
        startedAtNs,
        details: { code: error.code, message: error.message },
      });
      respondDomainError(res, error);
      return;
    }
    logAction("error", "campaign_update_failed", {
      req,
      action: "campaign_update",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error,
    });
    markErrorAsLogged(error);
    next(error);
  }
});

adminCampaignsRouter.patch("/campaigns/:id/delete", async (req, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      logAction("warn", "campaign_delete_invalid_id", {
        req,
        action: "campaign_delete",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige Kampagnen-ID.", code: "invalid_id" });
      return;
    }
    const now = new Date();
    const deleted = await db
      .update(campaigns)
      .set({ isDeleted: true, deletedAt: now, updatedAt: now })
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .returning({ id: campaigns.id });
    res.status(200).json({ ok: true, alreadyDeleted: deleted.length === 0 });
    logAction("info", "campaign_delete_success", {
      req,
      action: "campaign_delete",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { campaignId, alreadyDeleted: deleted.length === 0 },
    });
  } catch (error) {
    logAction("error", "campaign_delete_failed", {
      req,
      action: "campaign_delete",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error,
    });
    markErrorAsLogged(error);
    next(error);
  }
});

adminCampaignsRouter.delete("/campaigns/:id/hard-delete", async (req, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      logAction("warn", "campaign_hard_delete_invalid_id", {
        req,
        action: "campaign_hard_delete",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige Kampagnen-ID.", code: "invalid_id" });
      return;
    }

    const parsed = hardDeleteCampaignSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "campaign_hard_delete_invalid_payload", {
        req,
        action: "campaign_hard_delete",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültiger Request-Body.", code: "invalid_payload" });
      return;
    }

    if (parsed.data.confirmationText !== HARD_DELETE_CAMPAIGN_CONFIRMATION_TEXT) {
      logAction("warn", "campaign_hard_delete_confirmation_mismatch", {
        req,
        action: "campaign_hard_delete",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
        details: { campaignId },
      });
      res.status(400).json({ error: "Bestätigungstext stimmt nicht exakt überein.", code: "invalid_confirmation_text" });
      return;
    }

    let alreadyDeleted = false;
    let deletedSectionsCount = 0;
    let deletedOrphanSessionsCount = 0;

    await db.transaction(async (tx) => {
      await tx.execute(sql`SELECT ${campaigns.id} FROM ${campaigns} WHERE ${campaigns.id} = ${campaignId} FOR UPDATE`);

      const deletedSections = await tx
        .delete(visitSessionSections)
        .where(eq(visitSessionSections.campaignId, campaignId))
        .returning({ visitSessionId: visitSessionSections.visitSessionId });
      deletedSectionsCount = deletedSections.length;

      const affectedVisitSessionIds = Array.from(
        new Set(
          deletedSections
            .map((entry) => entry.visitSessionId)
            .filter((value): value is string => typeof value === "string" && value.length > 0),
        ),
      );

      if (affectedVisitSessionIds.length > 0) {
        const deletedSessions = await tx
          .delete(visitSessions)
          .where(
            and(
              inArray(visitSessions.id, affectedVisitSessionIds),
              sql`NOT EXISTS (
                SELECT 1
                FROM ${visitSessionSections}
                WHERE ${visitSessionSections.visitSessionId} = ${visitSessions.id}
              )`,
            ),
          )
          .returning({ id: visitSessions.id });
        deletedOrphanSessionsCount = deletedSessions.length;
      }

      const deletedCampaignRows = await tx
        .delete(campaigns)
        .where(eq(campaigns.id, campaignId))
        .returning({ id: campaigns.id });
      alreadyDeleted = deletedCampaignRows.length === 0;
    });

    res.status(200).json({ ok: true, alreadyDeleted });
    logAction("info", "campaign_hard_delete_success", {
      req,
      action: "campaign_hard_delete",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: {
        campaignId,
        alreadyDeleted,
        deletedSectionsCount,
        deletedOrphanSessionsCount,
      },
    });
  } catch (error) {
    logAction("error", "campaign_hard_delete_failed", {
      req,
      action: "campaign_hard_delete",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error,
    });
    markErrorAsLogged(error);
    next(error);
  }
});

adminCampaignsRouter.post("/campaigns/:id/markets", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      logAction("warn", "campaign_markets_assign_invalid_id", {
        req,
        action: "campaign_markets_assign",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige Kampagnen-ID.", code: "invalid_id" });
      return;
    }
    const parsed = assignMarketsSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "campaign_markets_assign_invalid_payload", {
        req,
        action: "campaign_markets_assign",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige Marktzuweisung.", code: "invalid_payload" });
      return;
    }
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
    const assignments = normalizeAssignments({
      section: campaignForValidation.section,
      marketIds: parsed.data.marketIds,
      ...(parsed.data.assignments ? { assignments: parsed.data.assignments } : {}),
    });
    ensureManualAssignmentsHaveGm(campaignForValidation.section, assignments);
    const marketIds = assignments.map((assignment) => assignment.marketId);
    await ensureMarketsExist(marketIds);
    await ensureGmUsersExist(assignments.map((assignment) => assignment.gmUserId));
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
          assignmentSlot: assignment.assignmentSlot,
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
    logAction("info", "campaign_markets_assign_success", {
      req,
      action: "campaign_markets_assign",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { campaignId, assignedCount: assignments.length },
    });
  } catch (error) {
    if (error instanceof CampaignOverlapConflictError) {
      logAction("warn", "campaign_markets_assign_overlap_conflict", {
        req,
        action: "campaign_markets_assign",
        result: "rejected",
        statusCode: error.status,
        requestClass: "client_error",
        startedAtNs,
        details: { conflictCount: error.conflicts.length },
      });
      res.status(error.status).json({ error: error.message, code: error.code, conflicts: error.conflicts });
      return;
    }
    if (error instanceof CampaignDomainError) {
      logAction("warn", "campaign_markets_assign_domain_error", {
        req,
        action: "campaign_markets_assign",
        result: "rejected",
        statusCode: error.status,
        requestClass: "client_error",
        startedAtNs,
        details: { code: error.code, message: error.message },
      });
      respondDomainError(res, error);
      return;
    }
    logAction("error", "campaign_markets_assign_failed", {
      req,
      action: "campaign_markets_assign",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error,
    });
    markErrorAsLogged(error);
    next(error);
  }
});

adminCampaignsRouter.post("/campaigns/:id/markets/migrate", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      logAction("warn", "campaign_markets_migrate_invalid_id", {
        req,
        action: "campaign_markets_migrate",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige Kampagnen-ID.", code: "invalid_id" });
      return;
    }
    const parsed = migrateMarketsSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "campaign_markets_migrate_invalid_payload", {
        req,
        action: "campaign_markets_migrate",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige Marktmigration.", code: "invalid_payload" });
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
      assignments: moves.map((move) => ({
        marketId: move.marketId,
        gmUserId: move.gmUserId,
        assignmentSlot: 1,
        visitTargetCount: 1,
      })),
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
            assignmentSlot: campaignMarketAssignments.assignmentSlot,
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
            assignmentSlot: oldAssignment.assignmentSlot,
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
    logAction("info", "campaign_markets_migrate_success", {
      req,
      action: "campaign_markets_migrate",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { campaignId, migrated: moves.length },
    });
  } catch (error) {
    if (error instanceof CampaignOverlapConflictError) {
      logAction("warn", "campaign_markets_migrate_overlap_conflict", {
        req,
        action: "campaign_markets_migrate",
        result: "rejected",
        statusCode: error.status,
        requestClass: "client_error",
        startedAtNs,
        details: { conflictCount: error.conflicts.length },
      });
      res.status(error.status).json({ error: error.message, code: error.code, conflicts: error.conflicts });
      return;
    }
    if (error instanceof CampaignDomainError) {
      logAction("warn", "campaign_markets_migrate_domain_error", {
        req,
        action: "campaign_markets_migrate",
        result: "rejected",
        statusCode: error.status,
        requestClass: "client_error",
        startedAtNs,
        details: { code: error.code, message: error.message },
      });
      respondDomainError(res, error);
      return;
    }
    logAction("error", "campaign_markets_migrate_failed", {
      req,
      action: "campaign_markets_migrate",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error,
    });
    markErrorAsLogged(error);
    next(error);
  }
});

adminCampaignsRouter.patch("/campaigns/:id/gm-reassignments", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      logAction("warn", "campaign_gm_reassign_invalid_id", {
        req,
        action: "campaign_gm_reassign",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige Kampagnen-ID.", code: "invalid_id" });
      return;
    }

    const parsed = reassignCampaignGmsSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "campaign_gm_reassign_invalid_payload", {
        req,
        action: "campaign_gm_reassign",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige GM-Umstellung.", code: "invalid_payload" });
      return;
    }

    const reassignmentsBySource = new Map<string, string>();
    for (const reassignment of parsed.data.reassignments) {
      if (reassignment.fromGmUserId === reassignment.toGmUserId) continue;
      reassignmentsBySource.set(reassignment.fromGmUserId, reassignment.toGmUserId);
    }
    const reassignments = Array.from(reassignmentsBySource.entries()).map(([fromGmUserId, toGmUserId]) => ({
      fromGmUserId,
      toGmUserId,
    }));

    if (reassignments.length === 0) {
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
      res.status(200).json({ campaign, reassigned: 0 });
      return;
    }

    const [campaignRow] = await db
      .select({ id: campaigns.id, section: campaigns.section })
      .from(campaigns)
      .where(and(eq(campaigns.id, campaignId), eq(campaigns.isDeleted, false)))
      .limit(1);
    if (!campaignRow) {
      throw new CampaignDomainError("campaign_not_found", 404, "Kampagne nicht gefunden.");
    }

    await ensureGmUsersExist(reassignments.map((entry) => entry.toGmUserId));

    const sourceGmIds = reassignments.map((entry) => entry.fromGmUserId);
    const now = new Date();
    const auditUserId = await resolveAuditUserId(req.authUser?.appUserId);
    let movedRowsCount = 0;
    let movedVisitTargetsCount = 0;

    await db.transaction(async (tx) => {
      await tx.execute(sql`SELECT ${campaigns.id} FROM ${campaigns} WHERE ${campaigns.id} = ${campaignId} FOR UPDATE`);

      const affectedAssignments = await tx
        .select({
          id: campaignMarketAssignments.id,
          marketId: campaignMarketAssignments.marketId,
          gmUserId: campaignMarketAssignments.gmUserId,
          assignmentSlot: campaignMarketAssignments.assignmentSlot,
          visitTargetCount: campaignMarketAssignments.visitTargetCount,
          currentVisitsCount: campaignMarketAssignments.currentVisitsCount,
        })
        .from(campaignMarketAssignments)
        .where(
          and(
            eq(campaignMarketAssignments.campaignId, campaignId),
            inArray(campaignMarketAssignments.gmUserId, sourceGmIds),
            eq(campaignMarketAssignments.isDeleted, false),
          ),
        )
        .orderBy(asc(campaignMarketAssignments.assignedAt), asc(campaignMarketAssignments.assignmentSlot));

      if (affectedAssignments.length === 0) {
        throw new CampaignDomainError(
          "invalid_payload",
          409,
          "Keine aktiven Zuordnungen für die ausgewählten GMs gefunden. Bitte Daten neu laden.",
        );
      }

      const affectedIds = affectedAssignments.map((assignment) => assignment.id);
      await tx
        .update(campaignMarketAssignments)
        .set({
          isDeleted: true,
          deletedAt: now,
          updatedAt: now,
        })
        .where(inArray(campaignMarketAssignments.id, affectedIds));

      for (const assignment of affectedAssignments) {
        if (!assignment.gmUserId) continue;
        const nextGmUserId = reassignmentsBySource.get(assignment.gmUserId);
        if (!nextGmUserId || nextGmUserId === assignment.gmUserId) continue;

        const updated = await tx
          .update(campaignMarketAssignments)
          .set({
            visitTargetCount: sql`${campaignMarketAssignments.visitTargetCount} + ${assignment.visitTargetCount}`,
            currentVisitsCount: sql`${campaignMarketAssignments.currentVisitsCount} + ${assignment.currentVisitsCount}`,
            assignedAt: now,
            assignedByUserId: auditUserId,
            isDeleted: false,
            deletedAt: null,
            updatedAt: now,
          })
          .where(
            and(
              eq(campaignMarketAssignments.campaignId, campaignId),
              eq(campaignMarketAssignments.marketId, assignment.marketId),
              eq(campaignMarketAssignments.gmUserId, nextGmUserId),
              eq(campaignMarketAssignments.assignmentSlot, assignment.assignmentSlot),
              eq(campaignMarketAssignments.isDeleted, false),
            ),
          )
          .returning({ id: campaignMarketAssignments.id });

        if (updated.length === 0) {
          await tx.insert(campaignMarketAssignments).values({
            campaignId,
            marketId: assignment.marketId,
            gmUserId: nextGmUserId,
            assignmentSlot: assignment.assignmentSlot,
            visitTargetCount: assignment.visitTargetCount,
            currentVisitsCount: assignment.currentVisitsCount,
            assignedAt: now,
            assignedByUserId: auditUserId,
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          });
        }

        movedRowsCount += 1;
        movedVisitTargetsCount += Math.max(1, Number(assignment.visitTargetCount ?? 1));

        await tx.insert(campaignMarketAssignmentHistory).values({
          marketId: assignment.marketId,
          section: campaignRow.section,
          fromCampaignId: campaignId,
          toCampaignId: campaignId,
          fromGmUserId: assignment.gmUserId,
          toGmUserId: nextGmUserId,
          migratedByUserId: auditUserId,
          migratedAt: now,
          reason: "campaign_gm_reassignment",
          createdAt: now,
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
    res.status(200).json({ campaign, reassigned: movedRowsCount, visitTargets: movedVisitTargetsCount });
    logAction("info", "campaign_gm_reassign_success", {
      req,
      action: "campaign_gm_reassign",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { campaignId, reassigned: movedRowsCount, visitTargets: movedVisitTargetsCount },
    });
  } catch (error) {
    if (error instanceof CampaignDomainError) {
      logAction("warn", "campaign_gm_reassign_domain_error", {
        req,
        action: "campaign_gm_reassign",
        result: "rejected",
        statusCode: error.status,
        requestClass: "client_error",
        startedAtNs,
        details: { code: error.code, message: error.message },
      });
      respondDomainError(res, error);
      return;
    }
    logAction("error", "campaign_gm_reassign_failed", {
      req,
      action: "campaign_gm_reassign",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error,
    });
    markErrorAsLogged(error);
    next(error);
  }
});

adminCampaignsRouter.patch("/campaigns/:id/markets/:marketId/delete", async (req, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const campaignId = String(req.params.id ?? "");
    const marketId = String(req.params.marketId ?? "");
    if (!isUuid(campaignId) || !isUuid(marketId)) {
      logAction("warn", "campaign_market_remove_invalid_id", {
        req,
        action: "campaign_market_remove",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige ID.", code: "invalid_id" });
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
      logAction("warn", "campaign_market_remove_campaign_not_found", {
        req,
        action: "campaign_market_remove",
        result: "rejected",
        statusCode: 404,
        requestClass: "client_error",
        startedAtNs,
        details: { campaignId },
      });
      res.status(404).json({ error: "Kampagne nicht gefunden.", code: "campaign_not_found" });
      return;
    }
    const [campaign] = await mapCampaignRows([row]);
    res.status(200).json({ campaign, removed: removed.length > 0 });
    logAction("info", "campaign_market_remove_success", {
      req,
      action: "campaign_market_remove",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { campaignId, marketId, removed: removed.length > 0 },
    });
  } catch (error) {
    logAction("error", "campaign_market_remove_failed", {
      req,
      action: "campaign_market_remove",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error,
    });
    markErrorAsLogged(error);
    next(error);
  }
});

adminCampaignsRouter.post("/campaigns/:id/fragebogen/switch", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const campaignId = String(req.params.id ?? "");
    if (!isUuid(campaignId)) {
      logAction("warn", "campaign_fragebogen_switch_invalid_id", {
        req,
        action: "campaign_fragebogen_switch",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültige Kampagnen-ID.", code: "invalid_id" });
      return;
    }
    const parsed = switchFragebogenSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "campaign_fragebogen_switch_invalid_payload", {
        req,
        action: "campaign_fragebogen_switch",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Ungültiger Fragebogen-Wechsel.", code: "invalid_payload" });
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
      logAction("warn", "campaign_fragebogen_switch_not_found", {
        req,
        action: "campaign_fragebogen_switch",
        result: "rejected",
        statusCode: 404,
        requestClass: "client_error",
        startedAtNs,
        details: { campaignId },
      });
      res.status(404).json({ error: "Kampagne nicht gefunden.", code: "campaign_not_found" });
      return;
    }
    const [campaign] = await mapCampaignRows([row]);
    res.status(200).json({ campaign, switched });
    logAction("info", "campaign_fragebogen_switch_success", {
      req,
      action: "campaign_fragebogen_switch",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { campaignId, switched },
    });
  } catch (error) {
    if (error instanceof CampaignDomainError) {
      logAction("warn", "campaign_fragebogen_switch_domain_error", {
        req,
        action: "campaign_fragebogen_switch",
        result: "rejected",
        statusCode: error.status,
        requestClass: "client_error",
        startedAtNs,
        details: { code: error.code, message: error.message },
      });
      respondDomainError(res, error);
      return;
    }
    logAction("error", "campaign_fragebogen_switch_failed", {
      req,
      action: "campaign_fragebogen_switch",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error,
    });
    markErrorAsLogged(error);
    next(error);
  }
});

export { adminCampaignsRouter };
