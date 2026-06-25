import { and, asc, desc, eq, gte, inArray, isNotNull, isNull, lt, sql } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import {
  finalizeBonusForSubmittedVisitSessionTx,
  readGmActiveBonusSummary,
} from "../lib/bonus-finalizer.js";
import { ensureAndGetGmKpiCache, recomputeGmKpiCache } from "../lib/gm-kpi-cache.js";
import { fetchFragebogenUi, fetchModulesUi } from "./fragebogen.js";
import { db } from "../lib/db.js";
import { ensureStartedDaySession } from "../lib/day-session.js";
import { enqueueIppRecalcForDate } from "../lib/ipp-finalizer.js";
import { logAction, logger, startActionTimer } from "../lib/logger.js";
import { addDays, startOfDay } from "../lib/red-monat.js";
import { resolveCurrentRedPeriod } from "../lib/red-month-periods.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { supabaseAdmin } from "../lib/supabase.js";
import {
  respondTimelineValidationError,
  validateGmTimelineInterval,
  validateGmTimelineStartPoint,
} from "../lib/zeiterfassung-validation.js";
import {
  buildVisitAnswerValidationResult,
  computeMissingRequiredQuestions,
} from "../lib/visit-session-answer-validation.js";
import {
  campaignMarketAssignments,
  campaigns,
  marketKuehlerUnits,
  markets,
  photoTags,
  users,
  visitAnswerChangeRequests,
  visitAnswerMatrixCells,
  visitAnswerOptions,
  visitAnswerPhotoTags,
  visitAnswerPhotos,
  visitAnswers,
  visitQuestionComments,
  visitSessionQuestions,
  visitSessionSections,
  visitSessions,
} from "../lib/schema.js";

const gmVisitSessionsRouter = Router();
gmVisitSessionsRouter.use(requireAuth(["admin", "gm"]));
gmVisitSessionsRouter.use((req, res, next) => {
  const method = req.method.toUpperCase();
  const isTracked = method !== "GET" || req.path === "/gm/bonus-summary" || req.path === "/gm/kpi-summary";
  if (!isTracked) {
    next();
    return;
  }
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "gm_visit_sessions_action_completed", {
      req,
      action: method === "GET" ? "gm_visit_sessions_read" : "gm_visit_sessions_mutation",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

const SECTION_ORDER = ["standard", "flex", "billa", "kuehler", "mhd"] as const;
const VISIT_PHOTOS_BUCKET = "visit-photos";
const VISIT_PHOTO_READ_URL_TTL_SECONDS = 30 * 60;

function isBillaMarketRow(input: { name: string | null | undefined; dbName: string | null | undefined }): boolean {
  return `${input.name ?? ""} ${input.dbName ?? ""}`.toUpperCase().includes("BILLA");
}
const VISIT_PHOTO_READ_URL_TIMEOUT_MS = 1800;
const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const isUuid = (value: string | null | undefined): value is string => Boolean(value && uuidRegex.test(value));
const changeRequestBodySchema = z.object({
  visitQuestionId: z.string().regex(uuidRegex),
  requestedAnswerPayload: z.record(z.string(), z.unknown()).default({}),
  requestedAnswerSummary: z.string().trim().min(1).max(700),
  requestNote: z.string().trim().max(700).optional(),
});
const SINGLE_CHOICE_AVAILABILITY_TYPES = ["Cooler", "SingleServe", "MultiServe", "Promos", "Warehouse"] as const;
type SingleChoiceAvailabilityType = (typeof SINGLE_CHOICE_AVAILABILITY_TYPES)[number];
const QUESTION_TYPES = ["single", "yesno", "yesnomulti", "multiple", "likert", "text", "numeric", "slider", "photo", "matrix"] as const;
const QUESTION_TYPE_SET = new Set<string>(QUESTION_TYPES);
type VisitPhotoSignedReadResult = {
  signedUrl: string | null;
  errorMessage: string | null;
  timedOut: boolean;
};

function parseOptionalIsoTimestamp(value: string | undefined): Date | null {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

const MAX_REQUESTED_VISIT_START_AGE_MS = 36 * 60 * 60 * 1000;
const MAX_REQUESTED_VISIT_START_FUTURE_MS = 2 * 60 * 1000;

function resolveAcceptedVisitStartAt(startedAt: string | undefined, now: Date): Date {
  const requested = parseOptionalIsoTimestamp(startedAt);
  if (!requested) return now;
  const requestedMs = requested.getTime();
  const nowMs = now.getTime();
  if (requestedMs > nowMs + MAX_REQUESTED_VISIT_START_FUTURE_MS) return now;
  if (requestedMs < nowMs - MAX_REQUESTED_VISIT_START_AGE_MS) return now;
  return requested;
}

function cleanStoragePath(path: string): string {
  return path.replace(/^\/+/, "").trim();
}

async function createVisitPhotoReadUrl(input: {
  photoId: string;
  storageBucket: string;
  storagePath: string;
}): Promise<{ signedUrl: string | null; expiresAt: string | null }> {
  const expiresAt = new Date(Date.now() + VISIT_PHOTO_READ_URL_TTL_SECONDS * 1000).toISOString();
  if (input.storageBucket !== VISIT_PHOTOS_BUCKET) return { signedUrl: null, expiresAt: null };
  const normalizedPath = cleanStoragePath(input.storagePath);
  if (!normalizedPath) return { signedUrl: null, expiresAt: null };
  const signed = await Promise.race<VisitPhotoSignedReadResult>([
    supabaseAdmin.storage
      .from(input.storageBucket)
      .createSignedUrl(normalizedPath, VISIT_PHOTO_READ_URL_TTL_SECONDS)
      .then(({ data, error }) => ({
        signedUrl: error || !data?.signedUrl ? null : data.signedUrl,
        errorMessage: error?.message ?? null,
        timedOut: false,
      }))
      .catch((error: unknown) => ({
        signedUrl: null,
        errorMessage: error instanceof Error ? error.message : "unknown_error",
        timedOut: false,
      })),
    new Promise<VisitPhotoSignedReadResult>((resolve) => {
      setTimeout(() => {
        resolve({ signedUrl: null, errorMessage: "signed_url_timeout", timedOut: true });
      }, VISIT_PHOTO_READ_URL_TIMEOUT_MS);
    }),
  ]);
  if (!signed.signedUrl) {
    logger.warn("gm_visit_photo_signed_url_failed", {
      photoId: input.photoId,
      storageBucket: input.storageBucket,
      storagePath: input.storagePath,
      error: signed.errorMessage,
      timedOut: signed.timedOut,
    });
    return { signedUrl: null, expiresAt: null };
  }
  return { signedUrl: signed.signedUrl, expiresAt };
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

type ResolvedQuestion = {
  questionId: string;
  type: "single" | "yesno" | "yesnomulti" | "multiple" | "likert" | "text" | "numeric" | "slider" | "photo" | "matrix";
  text: string;
  required: boolean;
  redSurvey: boolean | null;
  singleChoiceAvailability: boolean | null;
  singleChoiceAvailabilityType: SingleChoiceAvailabilityType | null;
  config: Record<string, unknown>;
  rules: Array<Record<string, unknown>>;
  scoring: Record<string, Record<string, unknown>>;
  chains: string[];
  appliesToMarketChain: boolean;
  options: string[];
  moduleId: string;
  moduleName: string;
};

type ResolvedSection = {
  section: "standard" | "flex" | "billa" | "kuehler" | "mhd";
  campaignId: string;
  campaignName: string;
  fragebogenId: string;
  fragebogenName: string;
  questions: ResolvedQuestion[];
};

function buildVisitSessionQuestionSnapshotSelect(hasAvailabilityTypeSnapshotColumn: boolean) {
  return {
    id: visitSessionQuestions.id,
    visitSessionSectionId: visitSessionQuestions.visitSessionSectionId,
    questionId: visitSessionQuestions.questionId,
    moduleId: visitSessionQuestions.moduleId,
    moduleNameSnapshot: visitSessionQuestions.moduleNameSnapshot,
    questionType: visitSessionQuestions.questionType,
    questionTextSnapshot: visitSessionQuestions.questionTextSnapshot,
    questionConfigSnapshot: visitSessionQuestions.questionConfigSnapshot,
    questionRulesSnapshot: visitSessionQuestions.questionRulesSnapshot,
    questionChainsSnapshot: visitSessionQuestions.questionChainsSnapshot,
    appliesToMarketChainSnapshot: visitSessionQuestions.appliesToMarketChainSnapshot,
    requiredSnapshot: visitSessionQuestions.requiredSnapshot,
    redSurveySnapshot: visitSessionQuestions.redSurveySnapshot,
    singleChoiceAvailabilitySnapshot: visitSessionQuestions.singleChoiceAvailabilitySnapshot,
    singleChoiceAvailabilityTypeSnapshot: hasAvailabilityTypeSnapshotColumn
      ? visitSessionQuestions.singleChoiceAvailabilityTypeSnapshot
      : sql<string | null>`null::text`,
    orderIndex: visitSessionQuestions.orderIndex,
  };
}

type PhotoTagMeta = {
  id: string;
  label: string;
  deletedAt: string | null;
};

function normalizeExt(ext: string | null | undefined): string {
  const clean = String(ext ?? "").toLowerCase().replace(/[^a-z0-9]/g, "");
  if (!clean) return "jpg";
  return clean;
}

function toResolvedQuestion(question: {
  id?: string;
  type: string;
  text: string;
  required: boolean;
  redSurvey?: boolean | null;
  singleChoiceAvailability?: boolean | null;
  singleChoiceAvailabilityType?: SingleChoiceAvailabilityType | null;
  config: Record<string, unknown>;
  rules?: Array<Record<string, unknown>>;
  scoring?: Record<string, Record<string, unknown>>;
  chains?: string[];
}, module: { id?: string; name?: string }): ResolvedQuestion | null {
  if (!isUuid(question.id)) return null;
  if (!QUESTION_TYPE_SET.has(question.type)) {
    logger.warn("gm_visit_sessions_invalid_question_type_skipped", {
      action: "gm_visit_sessions_resolve_sections",
      result: "failure",
      questionId: question.id,
      moduleId: module.id ?? null,
      questionType: question.type,
    });
    return null;
  }
  return {
    questionId: question.id,
    type: question.type as ResolvedQuestion["type"],
    text: question.text,
    required: question.required,
    redSurvey: question.redSurvey ?? null,
    singleChoiceAvailability: question.singleChoiceAvailability ?? null,
    singleChoiceAvailabilityType: question.singleChoiceAvailabilityType ?? null,
    config: question.config ?? {},
    rules: question.rules ?? [],
    scoring: question.scoring ?? {},
    chains: normalizeQuestionChains(question.chains),
    appliesToMarketChain: true,
    options: Array.isArray((question.config ?? {}).options) ? ((question.config ?? {}).options as string[]) : [],
    moduleId: module.id ?? "",
    moduleName: module.name ?? "",
  };
}

function decodePhotoAnswer(raw: unknown): { photos: string[]; selectedTagIds: string[] } {
  if (!raw) return { photos: [], selectedTagIds: [] };
  if (Array.isArray(raw)) return { photos: raw.filter((v): v is string => typeof v === "string"), selectedTagIds: [] };
  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw) as { photos?: unknown; selectedTagIds?: unknown };
      const photos = Array.isArray(parsed?.photos) ? parsed.photos.filter((v): v is string => typeof v === "string") : [];
      const selectedTagIds = Array.isArray(parsed?.selectedTagIds)
        ? parsed.selectedTagIds.filter((v): v is string => typeof v === "string")
        : [];
      return { photos, selectedTagIds };
    } catch {
      return { photos: [raw], selectedTagIds: [] };
    }
  }
  return { photos: [], selectedTagIds: [] };
}

function parseDateCellValue(input: string): string | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(input.trim());
  if (!match) return null;
  const yyyy = Number(match[1]);
  const mm = Number(match[2]);
  const dd = Number(match[3]);
  if (mm < 1 || mm > 12 || dd < 1 || dd > 31) return null;
  const candidate = new Date(Date.UTC(yyyy, mm - 1, dd));
  if (
    candidate.getUTCFullYear() !== yyyy ||
    candidate.getUTCMonth() !== mm - 1 ||
    candidate.getUTCDate() !== dd
  ) {
    return null;
  }
  return `${match[1]}-${match[2]}-${match[3]}`;
}

function asStringArray(input: unknown): string[] {
  return Array.isArray(input) ? input.filter((v): v is string => typeof v === "string") : [];
}

function normalizeOptionsFromConfig(config: Record<string, unknown>): string[] {
  const fromOptions = asStringArray(config.options);
  if (fromOptions.length > 0) return fromOptions;
  return [];
}

function normalizeUnique(values: string[]): string[] {
  return Array.from(new Set(values.map((v) => v.trim()).filter((v) => v.length > 0)));
}

function buildAnswerChangeRequestSnapshot(input: {
  answer?: typeof visitAnswers.$inferSelect;
  options: Array<typeof visitAnswerOptions.$inferSelect>;
  matrixCells: Array<typeof visitAnswerMatrixCells.$inferSelect>;
  photos: Array<typeof visitAnswerPhotos.$inferSelect>;
}): Record<string, unknown> {
  const answer = input.answer;
  if (!answer) {
    return {
      answerStatus: "unanswered",
      valueText: null,
      valueNumber: null,
      valueJson: null,
      options: [],
      matrixCells: [],
      photos: [],
    };
  }
  return {
    answerId: answer.id,
    answerStatus: answer.answerStatus,
    valueText: answer.valueText,
    valueNumber: answer.valueNumber,
    valueJson: answer.valueJson ?? null,
    isValid: answer.isValid,
    validationError: answer.validationError,
    version: answer.version,
    answeredAt: answer.answeredAt?.toISOString() ?? null,
    changedAt: answer.changedAt.toISOString(),
    options: input.options.map((option) => ({
      optionRole: option.optionRole,
      optionValue: option.optionValue,
      orderIndex: option.orderIndex,
    })),
    matrixCells: input.matrixCells.map((cell) => ({
      rowKey: cell.rowKey,
      columnKey: cell.columnKey,
      cellValueText: cell.cellValueText,
      cellValueDate: cell.cellValueDate,
      cellSelected: cell.cellSelected,
      orderIndex: cell.orderIndex,
    })),
    photos: input.photos.map((photo) => ({
      id: photo.id,
      storageBucket: photo.storageBucket,
      storagePath: photo.storagePath,
      mimeType: photo.mimeType,
      byteSize: photo.byteSize,
      widthPx: photo.widthPx,
      heightPx: photo.heightPx,
      uploadedAt: photo.uploadedAt.toISOString(),
    })),
  };
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

function isQuestionApplicableToMarketChain(questionChains: string[] | null | undefined, marketChain: string): boolean {
  const normalizedChains = normalizeQuestionChains(questionChains);
  if (normalizedChains.length === 0) return true;
  if (!marketChain) return false;
  return normalizedChains.includes(marketChain);
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

function parseNumericConstraint(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
  }
  return null;
}

function parseMatrixCellKey(cellKey: string): { rowKey: string; columnKey: string } {
  if (cellKey.includes("::")) {
    const split = cellKey.split("::");
    return { rowKey: split[0] ?? cellKey, columnKey: split[1] ?? "" };
  }
  if (cellKey.includes(": ")) {
    const split = cellKey.split(": ");
    return { rowKey: split[0] ?? cellKey, columnKey: split[1] ?? "" };
  }
  return { rowKey: cellKey, columnKey: "" };
}

function extractConfiguredPhotoTagIds(config: Record<string, unknown>): string[] {
  if (!Array.isArray(config.tagIds)) return [];
  return normalizeUnique(config.tagIds.filter((id): id is string => typeof id === "string" && isUuid(id)));
}

async function loadPhotoTagMetaByIds(inputTagIds: string[]): Promise<Map<string, PhotoTagMeta>> {
  const tagIds = normalizeUnique(inputTagIds.filter((id) => isUuid(id)));
  if (tagIds.length === 0) return new Map();
  const rows = await db
    .select({
      id: photoTags.id,
      label: photoTags.label,
      deletedAt: photoTags.deletedAt,
    })
    .from(photoTags)
    .where(inArray(photoTags.id, tagIds));
  return new Map(
    rows.map((row) => [
      row.id,
      {
        id: row.id,
        label: row.label,
        deletedAt: row.deletedAt ? row.deletedAt.toISOString() : null,
      },
    ]),
  );
}

function withPhotoTagMeta(
  questionType: string,
  configInput: Record<string, unknown>,
  tagMetaById: Map<string, PhotoTagMeta>,
): Record<string, unknown> {
  const config: Record<string, unknown> = { ...(configInput ?? {}) };
  if (questionType !== "photo") return config;
  const tagIds = extractConfiguredPhotoTagIds(config);
  if (tagIds.length === 0) return config;
  const tagMeta = tagIds.map((id) => tagMetaById.get(id) ?? { id, label: id, deletedAt: null });
  return {
    ...config,
    tagIds,
    tagMeta,
  };
}

async function verifyStorageObjectExists(bucket: string, storagePath: string): Promise<boolean> {
  const normalized = storagePath.replace(/^\/+/, "");
  const lastSlash = normalized.lastIndexOf("/");
  const folder = lastSlash >= 0 ? normalized.slice(0, lastSlash) : "";
  const fileName = lastSlash >= 0 ? normalized.slice(lastSlash + 1) : normalized;
  if (!fileName) return false;
  const { data, error } = await supabaseAdmin.storage.from(bucket).list(folder, { search: fileName, limit: 10 });
  if (error) return false;
  return Array.isArray(data) && data.some((row) => row.name === fileName);
}

type RedMonthSourceAnswer = {
  answer: typeof visitAnswers.$inferSelect;
  options: Array<typeof visitAnswerOptions.$inferSelect>;
  matrixCells: Array<typeof visitAnswerMatrixCells.$inferSelect>;
  photos: Array<typeof visitAnswerPhotos.$inferSelect>;
  tagsByPhotoId: Map<string, Array<typeof visitAnswerPhotoTags.$inferSelect>>;
  commentText: string | null;
};

function rewritePhotoStoragePathForAnswer(inputPath: string, sessionId: string, answerId: string): string {
  const normalized = inputPath.replace(/^\/+/, "");
  const segments = normalized.split("/").filter((segment) => segment.length > 0);
  const fileName = segments.length > 0 ? segments[segments.length - 1] : `${answerId}.jpg`;
  return `visit-photos/${sessionId}/${answerId}/${fileName}`;
}

function rewritePhotoStoragePayloadForAnswer(
  valueJson: Record<string, unknown> | null | undefined,
  sessionId: string,
  answerId: string,
): Record<string, unknown> | null {
  if (!valueJson || typeof valueJson !== "object") return valueJson ?? null;
  const storageRaw = (valueJson as { storage?: unknown }).storage;
  if (!Array.isArray(storageRaw)) return valueJson;
  const nextStorage = storageRaw.map((entry) => {
    if (!entry || typeof entry !== "object") return entry;
    const row = entry as { path?: unknown };
    if (typeof row.path !== "string" || row.path.trim().length === 0) return entry;
    return {
      ...(entry as Record<string, unknown>),
      path: rewritePhotoStoragePathForAnswer(row.path, sessionId, answerId),
    };
  });
  return {
    ...valueJson,
    storage: nextStorage,
  };
}

type SessionQuestionRow = {
  visitQuestionId: string;
  visitSessionSectionId: string;
  questionId: string;
  questionType: (typeof visitSessionQuestions.$inferSelect)["questionType"];
  requiredSnapshot: boolean;
  questionConfigSnapshot: Record<string, unknown>;
};

async function loadSiblingSessionQuestions(
  tx: any,
  input: { sessionId: string; questionId: string },
): Promise<SessionQuestionRow[]> {
  const rows = await tx
    .select({
      visitQuestionId: visitSessionQuestions.id,
      visitSessionSectionId: visitSessionSections.id,
      questionId: visitSessionQuestions.questionId,
      questionType: visitSessionQuestions.questionType,
      requiredSnapshot: visitSessionQuestions.requiredSnapshot,
      questionConfigSnapshot: visitSessionQuestions.questionConfigSnapshot,
    })
    .from(visitSessionQuestions)
    .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitSessionQuestions.visitSessionSectionId))
    .where(
      and(
        eq(visitSessionSections.visitSessionId, input.sessionId),
        eq(visitSessionSections.isDeleted, false),
        eq(visitSessionQuestions.isDeleted, false),
        eq(visitSessionQuestions.questionId, input.questionId),
      ),
    )
    .orderBy(asc(visitSessionSections.orderIndex), asc(visitSessionQuestions.orderIndex));
  return rows.map((row: SessionQuestionRow) => ({
    ...row,
    questionConfigSnapshot: (row.questionConfigSnapshot ?? {}) as Record<string, unknown>,
  }));
}

async function loadLatestSubmittedAnswersByQuestionIdInCurrentRedMonth(input: {
  gmUserId: string;
  marketId: string;
  questionIds: string[];
  now?: Date;
}): Promise<Map<string, RedMonthSourceAnswer>> {
  const questionIds = normalizeUnique(input.questionIds.filter((id) => isUuid(id)));
  if (questionIds.length === 0) return new Map();

  const period = await resolveCurrentRedPeriod(input.now ?? new Date());
  const periodStart = startOfDay(period.start);
  const periodEndExclusive = addDays(startOfDay(period.end), 1);

  const candidates = await db
    .select({
      answer: visitAnswers,
      submittedAt: visitSessions.submittedAt,
    })
    .from(visitAnswers)
    .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
    .where(
      and(
        inArray(visitAnswers.questionId, questionIds),
        eq(visitAnswers.isDeleted, false),
        eq(visitSessions.isDeleted, false),
        eq(visitSessions.status, "submitted"),
        eq(visitSessions.gmUserId, input.gmUserId),
        eq(visitSessions.marketId, input.marketId),
        isNotNull(visitSessions.submittedAt),
        gte(visitSessions.submittedAt, periodStart),
        lt(visitSessions.submittedAt, periodEndExclusive),
      ),
    )
    .orderBy(
      desc(visitSessions.submittedAt),
      desc(visitAnswers.changedAt),
      desc(visitAnswers.updatedAt),
      desc(visitAnswers.createdAt),
    );

  const selectedByQuestionId = new Map<string, typeof candidates[number]>();
  for (const row of candidates) {
    if (!selectedByQuestionId.has(row.answer.questionId)) {
      selectedByQuestionId.set(row.answer.questionId, row);
    }
  }
  if (selectedByQuestionId.size === 0) return new Map();

  const selectedAnswers = Array.from(selectedByQuestionId.values()).map((row) => row.answer);
  const selectedAnswerIds = selectedAnswers.map((row) => row.id);
  const selectedVisitQuestionIds = selectedAnswers.map((row) => row.visitSessionQuestionId);

  const [options, matrixCells, photos, comments] = await Promise.all([
    selectedAnswerIds.length === 0
      ? Promise.resolve([])
      : db
          .select()
          .from(visitAnswerOptions)
          .where(and(inArray(visitAnswerOptions.visitAnswerId, selectedAnswerIds), eq(visitAnswerOptions.isDeleted, false)))
          .orderBy(asc(visitAnswerOptions.orderIndex)),
    selectedAnswerIds.length === 0
      ? Promise.resolve([])
      : db
          .select()
          .from(visitAnswerMatrixCells)
          .where(and(inArray(visitAnswerMatrixCells.visitAnswerId, selectedAnswerIds), eq(visitAnswerMatrixCells.isDeleted, false)))
          .orderBy(asc(visitAnswerMatrixCells.orderIndex)),
    selectedAnswerIds.length === 0
      ? Promise.resolve([])
      : db
          .select()
          .from(visitAnswerPhotos)
          .where(and(inArray(visitAnswerPhotos.visitAnswerId, selectedAnswerIds), eq(visitAnswerPhotos.isDeleted, false)))
          .orderBy(asc(visitAnswerPhotos.createdAt)),
    selectedVisitQuestionIds.length === 0
      ? Promise.resolve([])
      : db
          .select()
          .from(visitQuestionComments)
          .where(and(inArray(visitQuestionComments.visitSessionQuestionId, selectedVisitQuestionIds), eq(visitQuestionComments.isDeleted, false))),
  ]);

  const photoIds = photos.map((row) => row.id);
  const photoTags =
    photoIds.length === 0
      ? []
      : await db
          .select()
          .from(visitAnswerPhotoTags)
          .where(and(inArray(visitAnswerPhotoTags.visitAnswerPhotoId, photoIds), eq(visitAnswerPhotoTags.isDeleted, false)))
          .orderBy(asc(visitAnswerPhotoTags.createdAt));

  const optionsByAnswerId = new Map<string, Array<typeof visitAnswerOptions.$inferSelect>>();
  for (const row of options) {
    const bucket = optionsByAnswerId.get(row.visitAnswerId) ?? [];
    bucket.push(row);
    optionsByAnswerId.set(row.visitAnswerId, bucket);
  }
  const matrixByAnswerId = new Map<string, Array<typeof visitAnswerMatrixCells.$inferSelect>>();
  for (const row of matrixCells) {
    const bucket = matrixByAnswerId.get(row.visitAnswerId) ?? [];
    bucket.push(row);
    matrixByAnswerId.set(row.visitAnswerId, bucket);
  }
  const photosByAnswerId = new Map<string, Array<typeof visitAnswerPhotos.$inferSelect>>();
  for (const row of photos) {
    const bucket = photosByAnswerId.get(row.visitAnswerId) ?? [];
    bucket.push(row);
    photosByAnswerId.set(row.visitAnswerId, bucket);
  }
  const tagsByPhotoId = new Map<string, Array<typeof visitAnswerPhotoTags.$inferSelect>>();
  for (const row of photoTags) {
    const bucket = tagsByPhotoId.get(row.visitAnswerPhotoId) ?? [];
    bucket.push(row);
    tagsByPhotoId.set(row.visitAnswerPhotoId, bucket);
  }
  const commentByVisitQuestionId = new Map<string, string>();
  for (const row of comments) {
    if (!commentByVisitQuestionId.has(row.visitSessionQuestionId)) {
      commentByVisitQuestionId.set(row.visitSessionQuestionId, row.commentText);
    }
  }

  const output = new Map<string, RedMonthSourceAnswer>();
  for (const row of selectedAnswers) {
    output.set(row.questionId, {
      answer: row,
      options: optionsByAnswerId.get(row.id) ?? [],
      matrixCells: matrixByAnswerId.get(row.id) ?? [],
      photos: photosByAnswerId.get(row.id) ?? [],
      tagsByPhotoId,
      commentText: commentByVisitQuestionId.get(row.visitSessionQuestionId) ?? null,
    });
  }
  return output;
}

async function resolveVisitSectionsForSelection(input: {
  gmUserId: string;
  marketId: string;
  campaignIds: string[];
}): Promise<ResolvedSection[]> {
  const liveNowCondition = sql`(
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
  const [marketRow] = await db
    .select({ name: markets.name, dbName: markets.dbName })
    .from(markets)
    .where(and(eq(markets.id, input.marketId), eq(markets.isDeleted, false)))
    .limit(1);
  const marketChain = normalizeMarketChain(marketRow?.dbName ?? "");
  const [gmUser] = await db
    .select({ isBillaGm: users.isBillaGm })
    .from(users)
    .where(eq(users.id, input.gmUserId))
    .limit(1);
  const canUseGlobalFlexFallback = !gmUser?.isBillaGm || isBillaMarketRow(marketRow ?? { name: "", dbName: "" });

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
        eq(campaignMarketAssignments.marketId, input.marketId),
        eq(campaignMarketAssignments.gmUserId, input.gmUserId),
        inArray(campaigns.id, input.campaignIds),
        eq(campaignMarketAssignments.isDeleted, false),
        eq(campaigns.isDeleted, false),
        liveNowCondition,
      ),
    );

  const selectedById = new Map(selectedRows.map((row) => [row.campaignId, row]));
  const missingAfterAssignment = canUseGlobalFlexFallback
    ? input.campaignIds.filter((id) => !selectedById.has(id))
    : [];
  if (missingAfterAssignment.length > 0) {
    const flexFallbackRows = await db
      .select({
        campaignId: campaigns.id,
        campaignName: campaigns.name,
        section: campaigns.section,
        currentFragebogenId: campaigns.currentFragebogenId,
      })
      .from(campaigns)
      .where(
        and(
          inArray(campaigns.id, missingAfterAssignment),
          eq(campaigns.section, "flex"),
          eq(campaigns.isDeleted, false),
          liveNowCondition,
        ),
      );
    for (const row of flexFallbackRows) {
      selectedById.set(row.campaignId, row);
    }
  }
  const missingRequested = input.campaignIds.filter((id) => !selectedById.has(id));
  if (missingRequested.length > 0) {
    const err = new Error("Mindestens eine ausgewählte Kampagne ist nicht mehr aktiv/zugeordnet.");
    (err as { status?: number; code?: string; details?: unknown }).status = 409;
    (err as { status?: number; code?: string; details?: unknown }).code = "visit_campaign_selection_invalid";
    (err as { status?: number; code?: string; details?: unknown }).details = { missingCampaignIds: missingRequested };
    throw err;
  }

  const normalizedSelected = input.campaignIds
    .map((campaignId) => selectedById.get(campaignId))
    .filter(Boolean)
    .filter((row): row is NonNullable<typeof row> & { currentFragebogenId: string } => Boolean(row?.currentFragebogenId));

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

  const sections = normalizedSelected
    .map((selected): ResolvedSection | null => {
      if (selected.section === "kuehler") {
        const fb = kuehlerFbById.get(selected.currentFragebogenId);
        if (!fb) return null;
        const questions = (fb.moduleIds ?? []).flatMap((moduleId) => {
          const module = kuehlerModuleById.get(moduleId);
          if (!module) return [];
          return (module.questions ?? [])
            .map((question) =>
              toResolvedQuestion(
                {
                  id: question.id ?? "",
                  type: question.type,
                  text: question.text,
                  required: question.required,
                  redSurvey: question.redSurvey ?? null,
                  singleChoiceAvailability: question.singleChoiceAvailability ?? null,
                  singleChoiceAvailabilityType: (question.singleChoiceAvailabilityType as SingleChoiceAvailabilityType | null) ?? null,
                  config: question.config ?? {},
                  rules: (question.rules ?? []) as Array<Record<string, unknown>>,
                  scoring: question.scoring ?? {},
                  chains: question.chains ?? [],
                },
                { id: module.id ?? "", name: module.name },
              ),
            )
            .filter((question): question is ResolvedQuestion =>
              Boolean(question && isQuestionApplicableToMarketChain(question.chains, marketChain)),
            );
        });
        if (!fb.id) return null;
        return {
          section: selected.section,
          campaignId: selected.campaignId,
          campaignName: selected.campaignName,
          fragebogenId: fb.id,
          fragebogenName: fb.name ?? "",
          questions,
        };
      }
      if (selected.section === "mhd") {
        const fb = mhdFbById.get(selected.currentFragebogenId);
        if (!fb) return null;
        const questions = (fb.moduleIds ?? []).flatMap((moduleId) => {
          const module = mhdModuleById.get(moduleId);
          if (!module) return [];
          return (module.questions ?? [])
            .map((question) =>
              toResolvedQuestion(
                {
                  id: question.id ?? "",
                  type: question.type,
                  text: question.text,
                  required: question.required,
                  redSurvey: question.redSurvey ?? null,
                  singleChoiceAvailability: question.singleChoiceAvailability ?? null,
                  singleChoiceAvailabilityType: (question.singleChoiceAvailabilityType as SingleChoiceAvailabilityType | null) ?? null,
                  config: question.config ?? {},
                  rules: (question.rules ?? []) as Array<Record<string, unknown>>,
                  scoring: question.scoring ?? {},
                  chains: question.chains ?? [],
                },
                { id: module.id ?? "", name: module.name },
              ),
            )
            .filter((question): question is ResolvedQuestion =>
              Boolean(question && isQuestionApplicableToMarketChain(question.chains, marketChain)),
            );
        });
        if (!fb.id) return null;
        return {
          section: selected.section,
          campaignId: selected.campaignId,
          campaignName: selected.campaignName,
          fragebogenId: fb.id,
          fragebogenName: fb.name ?? "",
          questions,
        };
      }

      const fb = mainFbById.get(selected.currentFragebogenId);
      if (!fb?.id) return null;
      const questions = (fb.moduleIds ?? []).flatMap((moduleId) => {
        const module = mainModuleById.get(moduleId);
        if (!module) return [];
        return (module.questions ?? [])
          .map((question) =>
            toResolvedQuestion(
              {
                id: question.id ?? "",
                type: question.type,
                text: question.text,
                required: question.required,
                redSurvey: question.redSurvey ?? null,
                singleChoiceAvailability: question.singleChoiceAvailability ?? null,
                singleChoiceAvailabilityType: (question.singleChoiceAvailabilityType as SingleChoiceAvailabilityType | null) ?? null,
                config: question.config ?? {},
                rules: (question.rules ?? []) as Array<Record<string, unknown>>,
                scoring: question.scoring ?? {},
                chains: question.chains ?? [],
              },
              { id: module.id ?? "", name: module.name },
            ),
          )
          .filter((question): question is ResolvedQuestion =>
            Boolean(question && isQuestionApplicableToMarketChain(question.chains, marketChain)),
          );
      });

      return {
        section: selected.section as "standard" | "flex" | "billa",
        campaignId: selected.campaignId,
        campaignName: selected.campaignName,
        fragebogenId: fb.id,
        fragebogenName: fb.name ?? "",
        questions,
      };
    })
    .filter(Boolean)
    .sort((left, right) => {
      const a = left as NonNullable<typeof left>;
      const b = right as NonNullable<typeof right>;
      const ai = SECTION_ORDER.indexOf(a.section);
      const bi = SECTION_ORDER.indexOf(b.section);
      if (ai !== bi) return ai - bi;
      return input.campaignIds.indexOf(a.campaignId) - input.campaignIds.indexOf(b.campaignId);
    }) as ResolvedSection[];

  return sections;
}

async function loadStartSectionsFromSession(sessionId: string): Promise<Array<{
  section: "standard" | "flex" | "billa" | "kuehler" | "mhd";
  campaignId: string;
  campaignName: string;
  fragebogenId: string | null;
  fragebogenName: string;
  questions: Array<{
    id: string;
    questionId: string;
    type: "single" | "yesno" | "yesnomulti" | "multiple" | "likert" | "text" | "numeric" | "slider" | "photo" | "matrix";
    text: string;
    required: boolean;
    singleChoiceAvailability: boolean | null;
    singleChoiceAvailabilityType: SingleChoiceAvailabilityType | null;
    config: Record<string, unknown>;
    rules: Array<Record<string, unknown>>;
    scoring: Record<string, Record<string, unknown>>;
    chains: string[];
    appliesToMarketChain: boolean;
    options: string[];
    moduleId: string;
    moduleName: string;
  }>;
}>> {
  const sections = await db
    .select()
    .from(visitSessionSections)
    .where(and(eq(visitSessionSections.visitSessionId, sessionId), eq(visitSessionSections.isDeleted, false)))
    .orderBy(asc(visitSessionSections.orderIndex));
  const sectionIds = sections.map((section) => section.id);
  const campaignIds = normalizeUnique(sections.map((section) => section.campaignId));
  const campaignRows =
    campaignIds.length === 0
      ? []
      : await db
          .select({ id: campaigns.id, name: campaigns.name })
          .from(campaigns)
          .where(inArray(campaigns.id, campaignIds));
  const campaignNameById = new Map(campaignRows.map((row) => [row.id, row.name]));
  const hasAvailabilityTypeSnapshotColumn = await ensureVisitSessionQuestionAvailabilityTypeSnapshotColumnReady();

  const questions =
    sectionIds.length === 0
      ? []
      : await db
          .select(buildVisitSessionQuestionSnapshotSelect(hasAvailabilityTypeSnapshotColumn))
          .from(visitSessionQuestions)
          .where(and(inArray(visitSessionQuestions.visitSessionSectionId, sectionIds), eq(visitSessionQuestions.isDeleted, false)))
          .orderBy(asc(visitSessionQuestions.orderIndex));
  const tagIds = normalizeUnique(
    questions.flatMap((question) =>
      extractConfiguredPhotoTagIds((question.questionConfigSnapshot ?? {}) as Record<string, unknown>),
    ),
  );
  const tagMetaById = await loadPhotoTagMetaByIds(tagIds);
  const questionsBySectionId = new Map<string, typeof questions>();
  for (const question of questions) {
    const bucket = questionsBySectionId.get(question.visitSessionSectionId) ?? [];
    bucket.push(question);
    questionsBySectionId.set(question.visitSessionSectionId, bucket);
  }

  return sections.map((section) => ({
    section: section.section,
    campaignId: section.campaignId,
    campaignName: campaignNameById.get(section.campaignId) ?? "",
    fragebogenId: section.fragebogenId,
    fragebogenName: section.fragebogenNameSnapshot,
    questions: (questionsBySectionId.get(section.id) ?? []).map((question) => {
      const config = withPhotoTagMeta(
        question.questionType,
        (question.questionConfigSnapshot ?? {}) as Record<string, unknown>,
        tagMetaById,
      );
      return {
        id: question.id,
        questionId: question.questionId,
        type: question.questionType,
        text: question.questionTextSnapshot,
        required: question.requiredSnapshot,
        singleChoiceAvailability: question.singleChoiceAvailabilitySnapshot ?? null,
        singleChoiceAvailabilityType: (question.singleChoiceAvailabilityTypeSnapshot as SingleChoiceAvailabilityType | null) ?? null,
        config,
        rules: (question.questionRulesSnapshot ?? []) as Array<Record<string, unknown>>,
        scoring: {},
        chains: normalizeQuestionChains(question.questionChainsSnapshot),
        appliesToMarketChain: Boolean(question.appliesToMarketChainSnapshot),
        options: normalizeOptionsFromConfig(config),
        moduleId: question.moduleId ?? "",
        moduleName: question.moduleNameSnapshot ?? "",
      };
    }),
  }));
}

function sameCampaignSet(left: string[], right: string[]): boolean {
  const leftSorted = normalizeUnique(left).sort();
  const rightSorted = normalizeUnique(right).sort();
  return leftSorted.length === rightSorted.length && leftSorted.every((campaignId, idx) => campaignId === rightSorted[idx]);
}

async function loadDraftVisitForSelection(input: {
  gmUserId: string;
  marketId: string;
  campaignIds: string[];
  kuehlerUnitId?: string | null;
}): Promise<typeof visitSessions.$inferSelect | null> {
  const requestedCampaignIds = normalizeUnique(input.campaignIds);
  if (requestedCampaignIds.length === 0) return null;
  const candidates = await db
    .select()
    .from(visitSessions)
    .where(
      and(
        eq(visitSessions.gmUserId, input.gmUserId),
        eq(visitSessions.marketId, input.marketId),
        input.kuehlerUnitId ? eq(visitSessions.kuehlerUnitId, input.kuehlerUnitId) : isNull(visitSessions.kuehlerUnitId),
        eq(visitSessions.status, "draft"),
        isNull(visitSessions.submittedAt),
        eq(visitSessions.isDeleted, false),
      ),
    )
    .orderBy(desc(visitSessions.startedAt), desc(visitSessions.updatedAt))
    .limit(10);

  for (const candidate of candidates) {
    const sectionRows = await db
      .select({ campaignId: visitSessionSections.campaignId })
      .from(visitSessionSections)
      .where(and(eq(visitSessionSections.visitSessionId, candidate.id), eq(visitSessionSections.isDeleted, false)));
    if (sameCampaignSet(sectionRows.map((row) => row.campaignId), requestedCampaignIds)) {
      return candidate;
    }
  }
  return null;
}

async function resolveKuehlerUnitForVisit(input: {
  marketId: string;
  kuehlerUnitId?: string | null;
}): Promise<string | null> {
  const kuehlerUnitId = input.kuehlerUnitId?.trim() || null;
  if (!kuehlerUnitId) return null;
  const [unit] = await db
    .select({ id: marketKuehlerUnits.id })
    .from(marketKuehlerUnits)
    .where(
      and(
        eq(marketKuehlerUnits.id, kuehlerUnitId),
        eq(marketKuehlerUnits.marketId, input.marketId),
        eq(marketKuehlerUnits.isDeleted, false),
      ),
    )
    .limit(1);
  if (!unit) {
    const err = new Error("Kühler-Eintrag wurde für diesen Markt nicht gefunden.");
    (err as { status?: number; code?: string; details?: unknown }).status = 400;
    (err as { status?: number; code?: string; details?: unknown }).code = "invalid_kuehler_unit";
    (err as { status?: number; code?: string; details?: unknown }).details = {
      marketId: input.marketId,
      kuehlerUnitId,
    };
    throw err;
  }
  return unit.id;
}

async function buildVisitStartPayloadFromSession(session: typeof visitSessions.$inferSelect) {
  const [market] = await db
    .select()
    .from(markets)
    .where(and(eq(markets.id, session.marketId), eq(markets.isDeleted, false)))
    .limit(1);
  if (!market) return null;
  const sections = await loadStartSectionsFromSession(session.id);
  return {
    session: {
      id: session.id,
      status: session.status,
      startedAt: session.startedAt.toISOString(),
      kuehlerUnitId: session.kuehlerUnitId ?? null,
    },
    market: {
      id: market.id,
      name: market.name,
      address: market.address,
      postalCode: market.postalCode,
      city: market.city,
    },
    sections,
  };
}

async function buildVisitStartPayloadForSelection(input: {
  gmUserId: string;
  marketId: string;
  campaignIds: string[];
}) {
  const [market] = await db
    .select()
    .from(markets)
    .where(and(eq(markets.id, input.marketId), eq(markets.isDeleted, false)))
    .limit(1);
  if (!market) return null;

  const resolvedSections = await resolveVisitSectionsForSelection(input);
  const configuredPhotoTagIds = normalizeUnique(
    resolvedSections.flatMap((section) =>
      section.questions.flatMap((question) => extractConfiguredPhotoTagIds(question.config)),
    ),
  );
  const tagMetaById = await loadPhotoTagMetaByIds(configuredPhotoTagIds);
  const sections = resolvedSections.map((section) => ({
    ...section,
    questions: section.questions.map((question) => ({
      ...question,
      config: withPhotoTagMeta(question.type, question.config, tagMetaById),
    })),
  }));

  if (sections.flatMap((section) => section.questions).length === 0) {
    const err = new Error("Für die ausgewählten Kampagnen wurden keine Besuchsfragen gefunden.");
    (err as { status?: number; code?: string; details?: unknown }).status = 409;
    (err as { status?: number; code?: string; details?: unknown }).code = "visit_start_no_questions";
    (err as { status?: number; code?: string; details?: unknown }).details = {
      marketId: input.marketId,
      campaignIds: input.campaignIds,
      sectionCount: sections.length,
      sections: sections.map((section) => ({
        campaignId: section.campaignId,
        section: section.section,
        fragebogenId: section.fragebogenId,
        questionCount: section.questions.length,
      })),
    };
    throw err;
  }

  return {
    market: {
      id: market.id,
      name: market.name,
      address: market.address,
      postalCode: market.postalCode,
      city: market.city,
    },
    sections,
  };
}

function buildValidationResult(
  questionType: string,
  config: Record<string, unknown>,
  rawAnswer: unknown,
): {
  answerStatus: "unanswered" | "answered" | "invalid";
  valueText: string | null;
  valueNumber: string | null;
  valueJson: Record<string, unknown> | null;
  isValid: boolean;
  validationError: string | null;
  options: Array<{ optionRole: "top" | "sub"; optionValue: string; orderIndex: number }>;
  matrixCells: Array<{ rowKey: string; columnKey: string; cellValueText: string | null; cellValueDate: string | null; cellSelected: boolean | null; orderIndex: number }>;
  inlinePhotos: Array<{ payload: string; selectedTagIds: string[]; orderIndex: number }>;
} {
  const empty = {
    answerStatus: "unanswered" as const,
    valueText: null,
    valueNumber: null,
    valueJson: null,
    isValid: true,
    validationError: null,
    options: [] as Array<{ optionRole: "top" | "sub"; optionValue: string; orderIndex: number }>,
    matrixCells: [] as Array<{ rowKey: string; columnKey: string; cellValueText: string | null; cellValueDate: string | null; cellSelected: boolean | null; orderIndex: number }>,
    inlinePhotos: [] as Array<{ payload: string; selectedTagIds: string[]; orderIndex: number }>,
  };

  if (rawAnswer == null || rawAnswer === "" || (Array.isArray(rawAnswer) && rawAnswer.length === 0)) return empty;

  if (questionType === "yesno" || questionType === "single" || questionType === "text") {
    if (typeof rawAnswer !== "string") return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ungültiges Text-Antwortformat." };
    if (questionType === "yesno") {
      const allowed = normalizeOptionsFromConfig(config);
      const whitelist = allowed.length > 0 ? allowed : ["Ja", "Nein", "ja", "nein"];
      if (!whitelist.includes(rawAnswer)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ja/Nein Antwort ist nicht erlaubt." };
      }
    }
    if (questionType === "single") {
      const allowed = normalizeOptionsFromConfig(config);
      if (allowed.length > 0 && !allowed.includes(rawAnswer)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ausgewählte Option ist nicht erlaubt." };
      }
    }
    if (questionType === "text") {
      const minLength = parseNumericConstraint(config.minLength);
      const maxLength = parseNumericConstraint(config.maxLength);
      if (minLength !== null && rawAnswer.length < minLength) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Text ist kürzer als erlaubt." };
      }
      if (maxLength !== null && rawAnswer.length > maxLength) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Text ist länger als erlaubt." };
      }
      if (typeof config.pattern === "string" && config.pattern.length > 0) {
        try {
          const regex = new RegExp(config.pattern);
          if (!regex.test(rawAnswer)) {
            return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Text entspricht nicht dem erwarteten Muster." };
          }
        } catch {
          return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Text-Muster ist ungültig konfiguriert." };
        }
      }
    }
    return { ...empty, answerStatus: "answered", valueText: rawAnswer, valueJson: { raw: rawAnswer } };
  }

  if (questionType === "multiple") {
    if (!Array.isArray(rawAnswer) || rawAnswer.some((v) => typeof v !== "string")) {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Mehrfachauswahl muss ein String-Array sein." };
    }
    const selected = normalizeUnique(rawAnswer);
    const allowed = normalizeOptionsFromConfig(config);
    if (allowed.length > 0 && selected.some((v) => !allowed.includes(v))) {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Mehrfachauswahl enthält ungültige Option(en)." };
    }
    return {
      ...empty,
      answerStatus: "answered",
      valueJson: { raw: selected },
      options: selected.map((value, idx) => ({ optionRole: "sub", optionValue: value, orderIndex: idx })),
    };
  }

  if (questionType === "yesnomulti") {
    if (typeof rawAnswer !== "string") return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ja/Nein-Multi erwartet JSON-String." };
    try {
      const parsed = JSON.parse(rawAnswer) as { sel?: unknown; subs?: unknown };
      const top = typeof parsed?.sel === "string" ? parsed.sel : "";
      const subs = normalizeUnique(Array.isArray(parsed?.subs) ? parsed.subs.filter((v): v is string => typeof v === "string") : []);
      const configAnswers = asStringArray(config.answers);
      const configBranches = Array.isArray(config.branches) ? (config.branches as Array<Record<string, unknown>>) : [];
      if (configAnswers.length > 0 && top.length > 0 && !configAnswers.includes(top)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ja/Nein-Multi Top-Antwort ist nicht erlaubt." };
      }
      if (top.length > 0 && configBranches.length > 0) {
        const branch = configBranches.find((entry) => entry.answer === top);
        const branchOptions = branch ? asStringArray((branch as Record<string, unknown>).options) : [];
        if (branchOptions.length > 0 && subs.some((v) => !branchOptions.includes(v))) {
          return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ja/Nein-Multi Unteroption ist nicht erlaubt." };
        }
      }
      const options = [
        ...(top ? [{ optionRole: "top" as const, optionValue: top, orderIndex: 0 }] : []),
        ...subs.map((value, idx) => ({ optionRole: "sub" as const, optionValue: value, orderIndex: idx })),
      ];
      return {
        ...empty,
        answerStatus: top ? "answered" : "unanswered",
        valueText: top || null,
        valueJson: { raw: parsed },
        options,
      };
    } catch {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ja/Nein-Multi JSON ungültig." };
    }
  }

  if (questionType === "likert" || questionType === "numeric" || questionType === "slider") {
    const numeric = Number(rawAnswer);
    if (!Number.isFinite(numeric)) {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Zahlenantwort ungültig." };
    }
    const min = parseNumericConstraint(config.min);
    const max = parseNumericConstraint(config.max);
    if (min !== null && numeric < min) {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Wert liegt unter dem erlaubten Minimum." };
    }
    if (max !== null && numeric > max) {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Wert liegt über dem erlaubten Maximum." };
    }
    if (questionType === "slider") {
      const step = parseNumericConstraint(config.step);
      if (step && step > 0 && min !== null) {
        const remainder = Math.abs((numeric - min) % step);
        if (remainder > 1e-6 && Math.abs(step - remainder) > 1e-6) {
          return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Slider-Wert entspricht nicht dem konfigurierten Schritt." };
        }
      }
    }
    if (questionType === "numeric") {
      const decimalsAllowed = Boolean(config.decimals);
      if (!decimalsAllowed && !Number.isInteger(numeric)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ganzzahl erwartet." };
      }
    }
    if (questionType === "likert") {
      if (!Number.isInteger(numeric)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Likert erwartet eine ganze Zahl." };
      }
    }
    return {
      ...empty,
      answerStatus: "answered",
      valueText: String(rawAnswer),
      valueNumber: String(numeric),
      valueJson: { raw: rawAnswer },
    };
  }

  if (questionType === "photo") {
    if (rawAnswer !== undefined && rawAnswer !== null) {
      return {
        ...empty,
        answerStatus: "unanswered",
        valueJson: null,
      };
    }
    const decoded = decodePhotoAnswer(rawAnswer);
    const tagsEnabled = Boolean(config.tagsEnabled) && Array.isArray(config.tagIds) && (config.tagIds as unknown[]).length > 0;
    if (tagsEnabled && decoded.photos.length > 0 && decoded.selectedTagIds.length === 0) {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Foto-Frage benötigt mindestens einen Tag." };
    }
    const inlinePhotos = decoded.photos.map((payload, idx) => ({ payload, selectedTagIds: decoded.selectedTagIds, orderIndex: idx }));
    return {
      ...empty,
      answerStatus: decoded.photos.length > 0 ? "answered" : "unanswered",
      valueJson: { raw: decoded },
      inlinePhotos,
    };
  }

  if (questionType === "matrix") {
    const subtype = String(config.matrixSubtype ?? "toggle");
    const allowedRows = asStringArray(config.rows);
    const allowedCols = asStringArray(config.columns);
    if (subtype === "toggle") {
      if (!Array.isArray(rawAnswer) || rawAnswer.some((v) => typeof v !== "string")) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Matrix-Auswahl muss String-Array sein." };
      }
      const uniqueCells = normalizeUnique(rawAnswer);
      const matrixCells = uniqueCells.map((cell, idx) => {
        const parsedKey = parseMatrixCellKey(String(cell));
        const rowKey = parsedKey.rowKey;
        const columnKey = parsedKey.columnKey;
        if ((allowedRows.length > 0 && !allowedRows.includes(rowKey)) || (allowedCols.length > 0 && !allowedCols.includes(columnKey))) {
          return null;
        }
        return { rowKey, columnKey, cellValueText: null, cellValueDate: null, cellSelected: true, orderIndex: idx };
      }).filter(Boolean) as Array<{ rowKey: string; columnKey: string; cellValueText: string | null; cellValueDate: string | null; cellSelected: boolean | null; orderIndex: number }>;
      if (matrixCells.length !== uniqueCells.length) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Matrix enthält ungültige Koordinaten." };
      }
      return { ...empty, answerStatus: matrixCells.length > 0 ? "answered" : "unanswered", valueJson: { raw: rawAnswer }, matrixCells };
    }
    if (typeof rawAnswer !== "string") {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Matrix erwartet JSON-String." };
    }
    try {
      const parsed = JSON.parse(rawAnswer) as Record<string, string>;
      const entries = Object.entries(parsed);
      const matrixCells = entries.map(([cellKey, val], idx) => {
        const parsedKey = parseMatrixCellKey(String(cellKey));
        const rowKey = parsedKey.rowKey;
        const columnKey = parsedKey.columnKey;
        if ((allowedRows.length > 0 && !allowedRows.includes(rowKey)) || (allowedCols.length > 0 && !allowedCols.includes(columnKey))) {
          return {
            rowKey,
            columnKey,
            cellValueText: null,
            cellValueDate: null,
            cellSelected: null,
            orderIndex: idx,
          };
        }
        if (subtype === "datum") {
          const dateIso = parseDateCellValue(String(val));
          return {
            rowKey,
            columnKey,
            cellValueText: null,
            cellValueDate: dateIso,
            cellSelected: null,
            orderIndex: idx,
          };
        }
        return {
          rowKey,
          columnKey,
          cellValueText: String(val),
          cellValueDate: null,
          cellSelected: null,
          orderIndex: idx,
        };
      });
      if (matrixCells.some((cell) => cell.cellValueDate == null && cell.cellValueText == null && cell.cellSelected == null)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Matrix enthält ungültige Koordinaten." };
      }
      if (subtype === "datum" && matrixCells.some((c) => c.cellValueDate == null && c.cellValueText == null)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Mindestens ein Matrix-Datum ist ungültig." };
      }
      return { ...empty, answerStatus: matrixCells.length > 0 ? "answered" : "unanswered", valueJson: { raw: parsed }, matrixCells };
    } catch {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Matrix-JSON ungültig." };
    }
  }

  return { ...empty, answerStatus: "invalid", isValid: false, validationError: `Unbekannter Fragetyp: ${questionType}` };
}

gmVisitSessionsRouter.get("/gm/visit-sessions/active", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const parsed = z
      .object({
        marketId: z.string().uuid(),
        campaignIds: z.string().min(1),
        kuehlerUnitId: z.string().uuid().optional(),
      })
      .strict()
      .safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige aktive Visit-Session Parameter." });
      return;
    }
    const campaignIds = Array.from(
      new Set(
        parsed.data.campaignIds
          .split(",")
          .map((entry) => entry.trim())
          .filter((entry) => isUuid(entry)),
      ),
    );
    if (campaignIds.length === 0) {
      res.status(400).json({ error: "Keine gültigen Kampagnen übermittelt." });
      return;
    }
    const draft = await loadDraftVisitForSelection({
      gmUserId,
      marketId: parsed.data.marketId,
      campaignIds,
      kuehlerUnitId: parsed.data.kuehlerUnitId ?? null,
    });
    if (!draft) {
      res.status(200).json({ session: null });
      return;
    }
    const payload = await buildVisitStartPayloadFromSession(draft);
    if (!payload) {
      res.status(409).json({ error: "Bestehende Session verweist auf einen nicht mehr verfügbaren Markt." });
      return;
    }
    res.status(200).json(payload);
  } catch (error) {
    next(error);
  }
});

gmVisitSessionsRouter.get("/gm/visit-sessions/latest-active", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const [draft] = await db
      .select()
      .from(visitSessions)
      .where(
        and(
          eq(visitSessions.gmUserId, gmUserId),
          eq(visitSessions.status, "draft"),
          isNull(visitSessions.submittedAt),
          eq(visitSessions.isDeleted, false),
        ),
      )
      .orderBy(desc(visitSessions.startedAt), desc(visitSessions.updatedAt))
      .limit(1);
    if (!draft) {
      res.status(200).json({ session: null });
      return;
    }
    const payload = await buildVisitStartPayloadFromSession(draft);
    if (!payload) {
      res.status(409).json({ error: "Bestehende Session verweist auf einen nicht mehr verfügbaren Markt." });
      return;
    }
    res.status(200).json(payload);
  } catch (error) {
    next(error);
  }
});

gmVisitSessionsRouter.get("/gm/visit-sessions/start-payload", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const parsed = z
      .object({
        marketId: z.string().uuid(),
        campaignIds: z.string().min(1),
        kuehlerUnitId: z.string().uuid().optional(),
      })
      .strict()
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
          .filter((entry) => isUuid(entry)),
      ),
    );
    if (campaignIds.length === 0) {
      res.status(400).json({ error: "Mindestens eine gültige Kampagnen-ID ist erforderlich." });
      return;
    }

    const payload = await buildVisitStartPayloadForSelection({
      gmUserId,
      marketId: parsed.data.marketId,
      campaignIds,
    });
    if (!payload) {
      res.status(404).json({ error: "Markt nicht gefunden." });
      return;
    }
    res.status(200).json(payload);
  } catch (error) {
    next(error);
  }
});

gmVisitSessionsRouter.post("/gm/visit-sessions", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const dayGuard = await ensureStartedDaySession({ gmUserId });
    if (!dayGuard.ok) {
      res.status(409).json({
        error: "Bitte zuerst den Arbeitstag starten.",
        code: dayGuard.reason,
      });
      return;
    }

    const parsed = z
      .object({
        marketId: z.string().uuid(),
        campaignIds: z.array(z.string().uuid()).min(1),
        kuehlerUnitId: z.string().uuid().optional(),
        clientSessionToken: z.string().trim().min(1).max(120).optional(),
        startedAt: z.string().optional(),
      })
      .strict()
      .safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige visit-session Eingabe." });
      return;
    }

    const [market] = await db
      .select()
      .from(markets)
      .where(and(eq(markets.id, parsed.data.marketId), eq(markets.isDeleted, false)))
      .limit(1);
    if (!market) {
      res.status(404).json({ error: "Markt nicht gefunden." });
      return;
    }

    const campaignIds = Array.from(new Set(parsed.data.campaignIds));
    const resolvedKuehlerUnitId = await resolveKuehlerUnitForVisit({
      marketId: market.id,
      kuehlerUnitId: parsed.data.kuehlerUnitId ?? null,
    });
    const requestedToken = parsed.data.clientSessionToken?.trim() || undefined;
    if (requestedToken) {
      const [existingByToken] = await db
        .select()
        .from(visitSessions)
        .where(
          and(
            eq(visitSessions.gmUserId, gmUserId),
            eq(visitSessions.clientSessionToken, requestedToken),
            eq(visitSessions.isDeleted, false),
          ),
        )
        .limit(1);
      if (existingByToken) {
        if (existingByToken.status !== "draft") {
          res.status(409).json({
            error: "Session-Token wurde bereits für eine abgeschlossene Session verwendet.",
            code: "visit_session_token_reused_closed",
            sessionId: existingByToken.id,
          });
          return;
        }
        if (existingByToken.marketId !== parsed.data.marketId) {
          res.status(409).json({
            error: "Session-Token gehört zu einer anderen Markt-Auswahl.",
            code: "visit_session_token_mismatch",
            details: {
              reason: "market_mismatch",
              expectedMarketId: existingByToken.marketId,
              requestedMarketId: parsed.data.marketId,
              sessionId: existingByToken.id,
            },
          });
          return;
        }
        if ((existingByToken.kuehlerUnitId ?? null) !== resolvedKuehlerUnitId) {
          res.status(409).json({
            error: "Session-Token gehört zu einer anderen Kühler-Auswahl.",
            code: "visit_session_token_mismatch",
            details: {
              reason: "kuehler_unit_mismatch",
              expectedKuehlerUnitId: existingByToken.kuehlerUnitId ?? null,
              requestedKuehlerUnitId: resolvedKuehlerUnitId,
              sessionId: existingByToken.id,
            },
          });
          return;
        }
        const existingSectionRows = await db
          .select({ campaignId: visitSessionSections.campaignId })
          .from(visitSessionSections)
          .where(
            and(
              eq(visitSessionSections.visitSessionId, existingByToken.id),
              eq(visitSessionSections.isDeleted, false),
            ),
          );
        const existingCampaignIds = normalizeUnique(existingSectionRows.map((row) => row.campaignId)).sort();
        const requestedCampaignIds = normalizeUnique(campaignIds).sort();
        if (
          existingCampaignIds.length !== requestedCampaignIds.length ||
          existingCampaignIds.some((campaignId, idx) => campaignId !== requestedCampaignIds[idx])
        ) {
          res.status(409).json({
            error: "Session-Token gehört zu einer anderen Kampagnen-Auswahl.",
            code: "visit_session_token_mismatch",
            details: {
              reason: "campaigns_mismatch",
              expectedCampaignIds: existingCampaignIds,
              requestedCampaignIds,
              sessionId: existingByToken.id,
            },
          });
          return;
        }
        const [existingMarket] = await db
          .select()
          .from(markets)
          .where(and(eq(markets.id, existingByToken.marketId), eq(markets.isDeleted, false)))
          .limit(1);
        if (!existingMarket) {
          res.status(409).json({ error: "Bestehende Session verweist auf einen nicht mehr verfügbaren Markt." });
          return;
        }
        const existingSections = await loadStartSectionsFromSession(existingByToken.id);
        res.status(200).json({
          session: {
            id: existingByToken.id,
            status: existingByToken.status,
            startedAt: existingByToken.startedAt.toISOString(),
            kuehlerUnitId: existingByToken.kuehlerUnitId ?? null,
          },
          market: {
            id: existingMarket.id,
            name: existingMarket.name,
            address: existingMarket.address,
            postalCode: existingMarket.postalCode,
            city: existingMarket.city,
          },
          sections: existingSections,
        });
        return;
      }
    }

    const existingDraft = await loadDraftVisitForSelection({
      gmUserId,
      marketId: market.id,
      campaignIds,
      kuehlerUnitId: resolvedKuehlerUnitId,
    });
    if (existingDraft) {
      const existingPayload = await buildVisitStartPayloadFromSession(existingDraft);
      if (!existingPayload) {
        res.status(409).json({ error: "Bestehende Session verweist auf einen nicht mehr verfügbaren Markt." });
        return;
      }
      res.status(200).json(existingPayload);
      return;
    }

    const startValidationNow = new Date();
    const acceptedStartedAt = resolveAcceptedVisitStartAt(parsed.data.startedAt, startValidationNow);

    const resolvedSections = await resolveVisitSectionsForSelection({
      gmUserId,
      marketId: parsed.data.marketId,
      campaignIds,
    });
    const configuredPhotoTagIds = normalizeUnique(
      resolvedSections.flatMap((section) =>
        section.questions.flatMap((question) => extractConfiguredPhotoTagIds(question.config)),
      ),
    );
    const tagMetaById = await loadPhotoTagMetaByIds(configuredPhotoTagIds);
    const sections = resolvedSections.map((section) => ({
      ...section,
      questions: section.questions.map((question) => ({
        ...question,
        config: withPhotoTagMeta(question.type, question.config, tagMetaById),
      })),
    }));
    if (sections.flatMap((section) => section.questions).length === 0) {
      res.status(409).json({
        error: "Für die ausgewählten Kampagnen wurden keine Besuchsfragen gefunden.",
        code: "visit_start_no_questions",
        details: {
          marketId: market.id,
          campaignIds,
          sectionCount: sections.length,
          sections: sections.map((section) => ({
            campaignId: section.campaignId,
            section: section.section,
            fragebogenId: section.fragebogenId,
            questionCount: section.questions.length,
          })),
        },
      });
      return;
    }
    const sessionQuestionIds = normalizeUnique(
      sections.flatMap((section) => section.questions.map((question) => question.questionId)),
    );
    const latestRedMonthAnswersByQuestionId = await loadLatestSubmittedAnswersByQuestionIdInCurrentRedMonth({
      gmUserId,
      marketId: market.id,
      questionIds: sessionQuestionIds,
      now: new Date(),
    });
    const hasAvailabilityTypeSnapshotColumn = await ensureVisitSessionQuestionAvailabilityTypeSnapshotColumnReady();

    const created = await db.transaction(async (tx) => {
      const now = new Date();
      const [sessionRow] = await tx
        .insert(visitSessions)
        .values({
          gmUserId,
          marketId: market.id,
          kuehlerUnitId: resolvedKuehlerUnitId,
          status: "draft",
          startedAt: acceptedStartedAt,
          lastSavedAt: now,
          clientSessionToken: requestedToken,
          isDeleted: false,
          deletedAt: null,
          createdAt: now,
          updatedAt: now,
        })
        .returning();
      if (!sessionRow) throw new Error("visit session konnte nicht erstellt werden.");

      const createdSections: Array<{
        id: string;
        section: "standard" | "flex" | "billa" | "kuehler" | "mhd";
        campaignId: string;
        campaignName: string;
        fragebogenId: string;
        fragebogenName: string;
        questions: Array<ResolvedQuestion & { visitQuestionId: string }>;
      }> = [];

      for (const [sectionOrder, section] of sections.entries()) {
        const [sectionRow] = await tx
          .insert(visitSessionSections)
          .values({
            visitSessionId: sessionRow.id,
            campaignId: section.campaignId,
            section: section.section,
            fragebogenId: section.fragebogenId,
            fragebogenNameSnapshot: section.fragebogenName,
            orderIndex: sectionOrder,
            status: "draft",
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          })
          .returning();
        if (!sectionRow) throw new Error("visit section konnte nicht erstellt werden.");

        const withIds: Array<ResolvedQuestion & { visitQuestionId: string }> = [];
        for (const [questionOrder, question] of section.questions.entries()) {
          const questionInsertValues = {
            visitSessionSectionId: sectionRow.id,
            questionId: question.questionId,
            moduleId: isUuid(question.moduleId) ? question.moduleId : null,
            moduleNameSnapshot: question.moduleName,
            questionType: question.type,
            questionTextSnapshot: question.text,
            questionConfigSnapshot: question.config,
            questionRulesSnapshot: question.rules as unknown as Array<Record<string, unknown>>,
            questionChainsSnapshot: normalizeQuestionChains(question.chains),
            appliesToMarketChainSnapshot: question.appliesToMarketChain,
            requiredSnapshot: question.required,
            redSurveySnapshot: question.redSurvey,
            singleChoiceAvailabilitySnapshot: question.singleChoiceAvailability,
            ...(hasAvailabilityTypeSnapshotColumn
              ? { singleChoiceAvailabilityTypeSnapshot: question.singleChoiceAvailabilityType }
              : {}),
            orderIndex: questionOrder,
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          };
          const [qRow] = await tx
            .insert(visitSessionQuestions)
            .values(questionInsertValues)
            .returning();
          if (!qRow) throw new Error("visit question konnte nicht erstellt werden.");

          const redMonthSource = latestRedMonthAnswersByQuestionId.get(question.questionId);
          if (redMonthSource) {
            const sourceAnswer = redMonthSource.answer;
            const rewrittenValueJson =
              sourceAnswer.questionType === "photo"
                ? rewritePhotoStoragePayloadForAnswer(
                    sourceAnswer.valueJson as Record<string, unknown> | null | undefined,
                    sessionRow.id,
                    qRow.id,
                  )
                : (sourceAnswer.valueJson as Record<string, unknown> | null);
            const [insertedAnswer] = await tx
              .insert(visitAnswers)
              .values({
                visitSessionId: sessionRow.id,
                visitSessionSectionId: sectionRow.id,
                visitSessionQuestionId: qRow.id,
                questionId: question.questionId,
                questionType: sourceAnswer.questionType,
                answerStatus: sourceAnswer.answerStatus,
                valueText: sourceAnswer.valueText,
                valueNumber: sourceAnswer.valueNumber == null ? null : String(sourceAnswer.valueNumber),
                valueJson: rewrittenValueJson,
                isValid: sourceAnswer.isValid,
                validationError: sourceAnswer.validationError,
                answeredAt: sourceAnswer.answeredAt ? now : null,
                changedAt: now,
                version: 1,
                isDeleted: false,
                deletedAt: null,
                createdAt: now,
                updatedAt: now,
              })
              .returning();
            if (!insertedAnswer) throw new Error("Vorbelegte Antwort konnte nicht erstellt werden.");

            if (redMonthSource.options.length > 0) {
              await tx.insert(visitAnswerOptions).values(
                redMonthSource.options.map((option, idx) => ({
                  visitAnswerId: insertedAnswer.id,
                  optionRole: option.optionRole,
                  optionValue: option.optionValue,
                  orderIndex: option.orderIndex ?? idx,
                  isDeleted: false,
                  deletedAt: null,
                  createdAt: now,
                  updatedAt: now,
                })),
              );
            }

            if (redMonthSource.matrixCells.length > 0) {
              await tx.insert(visitAnswerMatrixCells).values(
                redMonthSource.matrixCells.map((cell, idx) => ({
                  visitAnswerId: insertedAnswer.id,
                  rowKey: cell.rowKey,
                  columnKey: cell.columnKey,
                  cellValueText: cell.cellValueText,
                  cellValueDate: cell.cellValueDate,
                  cellSelected: cell.cellSelected,
                  orderIndex: cell.orderIndex ?? idx,
                  isDeleted: false,
                  deletedAt: null,
                  createdAt: now,
                  updatedAt: now,
                })),
              );
            }

            for (const sourcePhoto of redMonthSource.photos) {
              const rewrittenPath = rewritePhotoStoragePathForAnswer(sourcePhoto.storagePath, sessionRow.id, insertedAnswer.id);
              const [insertedPhoto] = await tx
                .insert(visitAnswerPhotos)
                .values({
                  visitAnswerId: insertedAnswer.id,
                  storageBucket: sourcePhoto.storageBucket,
                  storagePath: rewrittenPath,
                  mimeType: sourcePhoto.mimeType,
                  byteSize: sourcePhoto.byteSize,
                  widthPx: sourcePhoto.widthPx,
                  heightPx: sourcePhoto.heightPx,
                  sha256: sourcePhoto.sha256,
                  uploadedAt: sourcePhoto.uploadedAt ?? now,
                  isDeleted: false,
                  deletedAt: null,
                  createdAt: now,
                  updatedAt: now,
                })
                .returning();
              if (!insertedPhoto) continue;
              const sourceTags = redMonthSource.tagsByPhotoId.get(sourcePhoto.id) ?? [];
              if (sourceTags.length > 0) {
                await tx.insert(visitAnswerPhotoTags).values(
                  sourceTags.map((tag) => ({
                    visitAnswerPhotoId: insertedPhoto.id,
                    photoTagId: tag.photoTagId,
                    photoTagLabelSnapshot: tag.photoTagLabelSnapshot,
                    isDeleted: false,
                    deletedAt: null,
                    createdAt: now,
                    updatedAt: now,
                  })),
                );
              }
            }

            const sourceComment = redMonthSource.commentText?.trim() ?? "";
            if (sourceComment.length > 0) {
              await tx.insert(visitQuestionComments).values({
                visitSessionQuestionId: qRow.id,
                commentText: sourceComment,
                commentedAt: now,
                isDeleted: false,
                deletedAt: null,
                createdAt: now,
                updatedAt: now,
              });
            }
          }

          withIds.push({ ...question, visitQuestionId: qRow.id });
        }

        createdSections.push({
          id: sectionRow.id,
          section: section.section,
          campaignId: section.campaignId,
          campaignName: section.campaignName,
          fragebogenId: section.fragebogenId,
          fragebogenName: section.fragebogenName,
          questions: withIds,
        });
      }

      return { session: sessionRow, sections: createdSections };
    });

    res.status(201).json({
      session: {
        id: created.session.id,
        status: created.session.status,
        startedAt: created.session.startedAt.toISOString(),
        kuehlerUnitId: created.session.kuehlerUnitId ?? null,
      },
      market: {
        id: market.id,
        name: market.name,
        address: market.address,
        postalCode: market.postalCode,
        city: market.city,
      },
      sections: created.sections.map((section) => ({
        section: section.section,
        campaignId: section.campaignId,
        campaignName: section.campaignName,
        fragebogenId: section.fragebogenId,
        fragebogenName: section.fragebogenName,
        questions: section.questions.map((q) => ({
          id: q.visitQuestionId,
          questionId: q.questionId,
          type: q.type,
          text: q.text,
          required: q.required,
          singleChoiceAvailability: q.singleChoiceAvailability,
          singleChoiceAvailabilityType: q.singleChoiceAvailabilityType,
          config: q.config,
          rules: q.rules,
          scoring: q.scoring,
          chains: q.chains ?? [],
          appliesToMarketChain: q.appliesToMarketChain,
          options: q.options,
          moduleId: q.moduleId,
          moduleName: q.moduleName,
        })),
      })),
    });
  } catch (error) {
    if (respondTimelineValidationError(res, error)) return;
    const err = error as { status?: number; code?: string; details?: unknown; message?: string };
    if (err?.status) {
      res.status(err.status).json({ error: err.message ?? "visit session Fehler", code: err.code, details: err.details });
      return;
    }
    next(error);
  }
});

gmVisitSessionsRouter.get("/gm/visit-sessions/completed", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }

    const requestedLimit = Number(Array.isArray(req.query.limit) ? req.query.limit[0] : req.query.limit ?? 80);
    const limit = Number.isFinite(requestedLimit)
      ? Math.max(1, Math.min(120, Math.floor(requestedLimit)))
      : 80;

    const sessionRows = await db
      .select({
        id: visitSessions.id,
        startedAt: visitSessions.startedAt,
        submittedAt: visitSessions.submittedAt,
        marketId: markets.id,
        marketName: markets.name,
        marketAddress: markets.address,
        marketPostalCode: markets.postalCode,
        marketCity: markets.city,
        kuehlerUnitId: visitSessions.kuehlerUnitId,
      })
      .from(visitSessions)
      .innerJoin(markets, eq(markets.id, visitSessions.marketId))
      .where(and(
        eq(visitSessions.gmUserId, gmUserId),
        eq(visitSessions.status, "submitted"),
        isNotNull(visitSessions.submittedAt),
        eq(visitSessions.isDeleted, false),
        eq(markets.isDeleted, false),
      ))
      .orderBy(desc(visitSessions.submittedAt), desc(visitSessions.startedAt))
      .limit(limit);

    const sessionIds = sessionRows.map((row) => row.id);
    if (sessionIds.length === 0) {
      res.status(200).json({ visits: [] });
      return;
    }

    const kuehlerUnitIds = normalizeUnique(
      sessionRows.map((row) => row.kuehlerUnitId).filter((entry): entry is string => Boolean(entry)),
    );
    const kuehlerNumberByUnitId = new Map<string, string>();
    if (kuehlerUnitIds.length > 0) {
      const kuehlerUnitRows = await db
        .select({ id: marketKuehlerUnits.id, kuehlerInternalId: marketKuehlerUnits.kuehlerInternalId })
        .from(marketKuehlerUnits)
        .where(and(inArray(marketKuehlerUnits.id, kuehlerUnitIds), eq(marketKuehlerUnits.isDeleted, false)));
      for (const row of kuehlerUnitRows) {
        if (row.kuehlerInternalId) {
          kuehlerNumberByUnitId.set(row.id, row.kuehlerInternalId);
        }
      }
    }

    const sectionRows = await db
      .select({
        id: visitSessionSections.id,
        visitSessionId: visitSessionSections.visitSessionId,
        section: visitSessionSections.section,
        campaignId: visitSessionSections.campaignId,
        campaignName: campaigns.name,
        fragebogenName: visitSessionSections.fragebogenNameSnapshot,
        orderIndex: visitSessionSections.orderIndex,
      })
      .from(visitSessionSections)
      .leftJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
      .where(and(inArray(visitSessionSections.visitSessionId, sessionIds), eq(visitSessionSections.isDeleted, false)))
      .orderBy(asc(visitSessionSections.orderIndex));

    const sectionIds = sectionRows.map((row) => row.id);
    const questionRows = sectionIds.length === 0
      ? []
      : await db
          .select({
            sectionId: visitSessionQuestions.visitSessionSectionId,
            questionId: visitSessionQuestions.id,
            answerId: visitAnswers.id,
            answerStatus: visitAnswers.answerStatus,
            photoId: visitAnswerPhotos.id,
          })
          .from(visitSessionQuestions)
          .leftJoin(
            visitAnswers,
            and(
              eq(visitAnswers.visitSessionQuestionId, visitSessionQuestions.id),
              eq(visitAnswers.isDeleted, false),
            ),
          )
          .leftJoin(
            visitAnswerPhotos,
            and(
              eq(visitAnswerPhotos.visitAnswerId, visitAnswers.id),
              eq(visitAnswerPhotos.isDeleted, false),
            ),
          )
          .where(and(inArray(visitSessionQuestions.visitSessionSectionId, sectionIds), eq(visitSessionQuestions.isDeleted, false)));

    const sectionSummaryById = new Map<string, {
      questionIds: Set<string>;
      answeredQuestionIds: Set<string>;
      photoIds: Set<string>;
    }>();
    for (const row of sectionRows) {
      sectionSummaryById.set(row.id, {
        questionIds: new Set<string>(),
        answeredQuestionIds: new Set<string>(),
        photoIds: new Set<string>(),
      });
    }
    for (const row of questionRows) {
      const summary = sectionSummaryById.get(row.sectionId);
      if (!summary) continue;
      summary.questionIds.add(row.questionId);
      if (row.answerId && row.answerStatus !== "unanswered") {
        summary.answeredQuestionIds.add(row.questionId);
      }
      if (row.photoId) summary.photoIds.add(row.photoId);
    }

    const sectionsBySessionId = new Map<string, typeof sectionRows>();
    for (const row of sectionRows) {
      const bucket = sectionsBySessionId.get(row.visitSessionId) ?? [];
      bucket.push(row);
      sectionsBySessionId.set(row.visitSessionId, bucket);
    }

    const visits = sessionRows.map((session) => {
      const submittedAt = session.submittedAt;
      const durationMinutes = submittedAt
        ? Math.max(0, Math.round((submittedAt.getTime() - session.startedAt.getTime()) / 60000))
        : null;
      const sections = (sectionsBySessionId.get(session.id) ?? []).map((section) => {
        const summary = sectionSummaryById.get(section.id);
        return {
          id: section.id,
          section: section.section,
          campaignId: section.campaignId,
          campaignName: section.campaignName ?? "",
          fragebogenName: section.fragebogenName,
          questionCount: summary?.questionIds.size ?? 0,
          answeredCount: summary?.answeredQuestionIds.size ?? 0,
          photoCount: summary?.photoIds.size ?? 0,
        };
      });
      return {
        id: session.id,
        startedAt: session.startedAt.toISOString(),
        submittedAt: submittedAt?.toISOString() ?? null,
        durationMinutes,
        kuehlerUnitId: session.kuehlerUnitId ?? null,
        kuehlerNumber: session.kuehlerUnitId ? kuehlerNumberByUnitId.get(session.kuehlerUnitId) ?? null : null,
        market: {
          id: session.marketId,
          name: session.marketName,
          address: session.marketAddress,
          postalCode: session.marketPostalCode,
          city: session.marketCity,
        },
        sections,
        totals: {
          questionCount: sections.reduce((sum, section) => sum + section.questionCount, 0),
          answeredCount: sections.reduce((sum, section) => sum + section.answeredCount, 0),
          photoCount: sections.reduce((sum, section) => sum + section.photoCount, 0),
        },
      };
    });

    res.status(200).json({ visits });
  } catch (error) {
    next(error);
  }
});

gmVisitSessionsRouter.post("/gm/visit-sessions/:sessionId/change-requests", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    const sessionId = Array.isArray(req.params.sessionId) ? req.params.sessionId[0] : req.params.sessionId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    if (!isUuid(sessionId)) {
      res.status(400).json({ error: "Ungültige Session-ID." });
      return;
    }

    const parsed = changeRequestBodySchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Änderungsanfrage ist ungültig.", code: "invalid_change_request" });
      return;
    }
    const now = new Date();

    const [target] = await db
      .select({
        sessionId: visitSessions.id,
        marketId: visitSessions.marketId,
        visitSessionSectionId: visitSessionSections.id,
        visitSessionQuestionId: visitSessionQuestions.id,
        questionId: visitSessionQuestions.questionId,
        questionType: visitSessionQuestions.questionType,
        questionTextSnapshot: visitSessionQuestions.questionTextSnapshot,
      })
      .from(visitSessionQuestions)
      .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitSessionQuestions.visitSessionSectionId))
      .innerJoin(visitSessions, eq(visitSessions.id, visitSessionSections.visitSessionId))
      .where(
        and(
          eq(visitSessions.id, sessionId),
          eq(visitSessions.gmUserId, gmUserId),
          eq(visitSessions.status, "submitted"),
          isNotNull(visitSessions.submittedAt),
          eq(visitSessions.isDeleted, false),
          eq(visitSessionSections.isDeleted, false),
          eq(visitSessionQuestions.id, parsed.data.visitQuestionId),
          eq(visitSessionQuestions.isDeleted, false),
        ),
      )
      .limit(1);

    if (!target) {
      res.status(404).json({ error: "Abgeschlossene Frage wurde nicht gefunden." });
      return;
    }

    const [answer] = await db
      .select()
      .from(visitAnswers)
      .where(and(eq(visitAnswers.visitSessionQuestionId, target.visitSessionQuestionId), eq(visitAnswers.isDeleted, false)))
      .limit(1);

    let answerOptions: Array<typeof visitAnswerOptions.$inferSelect> = [];
    let answerMatrixCells: Array<typeof visitAnswerMatrixCells.$inferSelect> = [];
    let answerPhotos: Array<typeof visitAnswerPhotos.$inferSelect> = [];
    if (answer) {
      [answerOptions, answerMatrixCells, answerPhotos] = await Promise.all([
        db
          .select()
          .from(visitAnswerOptions)
          .where(and(eq(visitAnswerOptions.visitAnswerId, answer.id), eq(visitAnswerOptions.isDeleted, false)))
          .orderBy(asc(visitAnswerOptions.orderIndex)),
        db
          .select()
          .from(visitAnswerMatrixCells)
          .where(and(eq(visitAnswerMatrixCells.visitAnswerId, answer.id), eq(visitAnswerMatrixCells.isDeleted, false)))
          .orderBy(asc(visitAnswerMatrixCells.orderIndex)),
        db
          .select()
          .from(visitAnswerPhotos)
          .where(and(eq(visitAnswerPhotos.visitAnswerId, answer.id), eq(visitAnswerPhotos.isDeleted, false)))
          .orderBy(asc(visitAnswerPhotos.createdAt)),
      ]);
    }

    const currentAnswerSnapshot = buildAnswerChangeRequestSnapshot({
      ...(answer ? { answer } : {}),
      options: answerOptions,
      matrixCells: answerMatrixCells,
      photos: answerPhotos,
    });

    const [saved] = await db.transaction(async (tx) => {
      const [existing] = await tx
        .select()
        .from(visitAnswerChangeRequests)
        .where(
          and(
            eq(visitAnswerChangeRequests.gmUserId, gmUserId),
            eq(visitAnswerChangeRequests.visitSessionQuestionId, target.visitSessionQuestionId),
            eq(visitAnswerChangeRequests.status, "pending"),
            eq(visitAnswerChangeRequests.isDeleted, false),
          ),
        )
        .limit(1);

      if (existing) {
        return tx
          .update(visitAnswerChangeRequests)
          .set({
            visitAnswerId: answer?.id ?? null,
            questionType: target.questionType,
            questionTextSnapshot: target.questionTextSnapshot,
            currentAnswerSnapshot,
            requestedAnswerPayload: parsed.data.requestedAnswerPayload,
            requestedAnswerSummary: parsed.data.requestedAnswerSummary,
            requestNote: parsed.data.requestNote || null,
            updatedAt: now,
          })
          .where(eq(visitAnswerChangeRequests.id, existing.id))
          .returning({
            id: visitAnswerChangeRequests.id,
            status: visitAnswerChangeRequests.status,
            createdAt: visitAnswerChangeRequests.createdAt,
            updatedAt: visitAnswerChangeRequests.updatedAt,
          });
      }

      return tx
        .insert(visitAnswerChangeRequests)
        .values({
          visitSessionId: target.sessionId,
          visitSessionSectionId: target.visitSessionSectionId,
          visitSessionQuestionId: target.visitSessionQuestionId,
          visitAnswerId: answer?.id ?? null,
          questionId: target.questionId,
          gmUserId,
          marketId: target.marketId,
          questionType: target.questionType,
          questionTextSnapshot: target.questionTextSnapshot,
          currentAnswerSnapshot,
          requestedAnswerPayload: parsed.data.requestedAnswerPayload,
          requestedAnswerSummary: parsed.data.requestedAnswerSummary,
          requestNote: parsed.data.requestNote || null,
          status: "pending",
          createdAt: now,
          updatedAt: now,
        })
        .returning({
          id: visitAnswerChangeRequests.id,
          status: visitAnswerChangeRequests.status,
          createdAt: visitAnswerChangeRequests.createdAt,
          updatedAt: visitAnswerChangeRequests.updatedAt,
        });
    });

    res.status(201).json({
      ok: true,
      request: saved
        ? {
            id: saved.id,
            status: saved.status,
            createdAt: saved.createdAt.toISOString(),
            updatedAt: saved.updatedAt.toISOString(),
          }
        : null,
    });
  } catch (error) {
    next(error);
  }
});

gmVisitSessionsRouter.get("/gm/visit-sessions/:sessionId", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    const sessionId = Array.isArray(req.params.sessionId) ? req.params.sessionId[0] : req.params.sessionId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    if (!isUuid(sessionId)) {
      res.status(400).json({ error: "Ungültige Session-ID." });
      return;
    }
    const [session] = await db
      .select()
      .from(visitSessions)
      .where(and(eq(visitSessions.id, sessionId), eq(visitSessions.gmUserId, gmUserId), eq(visitSessions.isDeleted, false)))
      .limit(1);
    if (!session) {
      res.status(404).json({ error: "Session nicht gefunden." });
      return;
    }

    const [market] = await db
      .select()
      .from(markets)
      .where(and(eq(markets.id, session.marketId), eq(markets.isDeleted, false)))
      .limit(1);
    if (!market) {
      res.status(404).json({ error: "Markt nicht gefunden." });
      return;
    }

    let kuehlerNumber: string | null = null;
    if (session.kuehlerUnitId) {
      const [kuehlerUnit] = await db
        .select({ kuehlerInternalId: marketKuehlerUnits.kuehlerInternalId })
        .from(marketKuehlerUnits)
        .where(
          and(
            eq(marketKuehlerUnits.id, session.kuehlerUnitId),
            eq(marketKuehlerUnits.marketId, session.marketId),
            eq(marketKuehlerUnits.isDeleted, false),
          ),
        )
        .limit(1);
      kuehlerNumber = kuehlerUnit?.kuehlerInternalId ?? null;
    }

    const sections = await db
      .select()
      .from(visitSessionSections)
      .where(and(eq(visitSessionSections.visitSessionId, session.id), eq(visitSessionSections.isDeleted, false)))
      .orderBy(asc(visitSessionSections.orderIndex));

    const sectionIds = sections.map((s) => s.id);
    const campaignIds = normalizeUnique(sections.map((section) => section.campaignId));
    const campaignRows =
      campaignIds.length === 0
        ? []
        : await db
            .select({ id: campaigns.id, name: campaigns.name })
            .from(campaigns)
            .where(inArray(campaigns.id, campaignIds));
    const campaignNameById = new Map(campaignRows.map((row) => [row.id, row.name]));
    const hasAvailabilityTypeSnapshotColumn = await ensureVisitSessionQuestionAvailabilityTypeSnapshotColumnReady();

    const questions = sectionIds.length === 0
      ? []
      : await db
          .select(buildVisitSessionQuestionSnapshotSelect(hasAvailabilityTypeSnapshotColumn))
          .from(visitSessionQuestions)
          .where(and(inArray(visitSessionQuestions.visitSessionSectionId, sectionIds), eq(visitSessionQuestions.isDeleted, false)))
          .orderBy(asc(visitSessionQuestions.orderIndex));

    const questionIds = questions.map((q) => q.id);
    const answers = questionIds.length === 0
      ? []
      : await db
          .select()
          .from(visitAnswers)
          .where(and(inArray(visitAnswers.visitSessionQuestionId, questionIds), eq(visitAnswers.isDeleted, false)));

    const comments = questionIds.length === 0
      ? []
      : await db
          .select()
          .from(visitQuestionComments)
          .where(and(inArray(visitQuestionComments.visitSessionQuestionId, questionIds), eq(visitQuestionComments.isDeleted, false)));
    const answerIds = answers.map((row) => row.id);
    const answerOptions = answerIds.length === 0
      ? []
      : await db
          .select()
          .from(visitAnswerOptions)
          .where(and(inArray(visitAnswerOptions.visitAnswerId, answerIds), eq(visitAnswerOptions.isDeleted, false)))
          .orderBy(asc(visitAnswerOptions.orderIndex));
    const answerMatrixCells = answerIds.length === 0
      ? []
      : await db
          .select()
          .from(visitAnswerMatrixCells)
          .where(and(inArray(visitAnswerMatrixCells.visitAnswerId, answerIds), eq(visitAnswerMatrixCells.isDeleted, false)))
          .orderBy(asc(visitAnswerMatrixCells.orderIndex));
    const answerPhotos = answerIds.length === 0
      ? []
      : await db
          .select()
          .from(visitAnswerPhotos)
          .where(and(inArray(visitAnswerPhotos.visitAnswerId, answerIds), eq(visitAnswerPhotos.isDeleted, false)))
          .orderBy(asc(visitAnswerPhotos.createdAt));
    const answerPhotoIds = answerPhotos.map((row) => row.id);
    const answerPhotoTags = answerPhotoIds.length === 0
      ? []
      : await db
          .select()
          .from(visitAnswerPhotoTags)
          .where(and(inArray(visitAnswerPhotoTags.visitAnswerPhotoId, answerPhotoIds), eq(visitAnswerPhotoTags.isDeleted, false)))
          .orderBy(asc(visitAnswerPhotoTags.createdAt));

    const questionTagIds = normalizeUnique(
      questions.flatMap((question) =>
        extractConfiguredPhotoTagIds((question.questionConfigSnapshot ?? {}) as Record<string, unknown>),
      ),
    );
    const tagMetaById = await loadPhotoTagMetaByIds(questionTagIds);

    const answersByQuestionId = new Map(answers.map((row) => [row.visitSessionQuestionId, row]));
    const commentsByQuestionId = new Map(comments.map((row) => [row.visitSessionQuestionId, row]));
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
    const questionsBySectionId = new Map<string, typeof questions>();
    for (const q of questions) {
      const bucket = questionsBySectionId.get(q.visitSessionSectionId) ?? [];
      bucket.push(q);
      questionsBySectionId.set(q.visitSessionSectionId, bucket);
    }
    const signedPhotoUrlById = new Map<string, { signedUrl: string | null; expiresAt: string | null }>();
    await Promise.all(
      answerPhotos.map(async (photo) => {
        signedPhotoUrlById.set(
          photo.id,
          await createVisitPhotoReadUrl({
            photoId: photo.id,
            storageBucket: photo.storageBucket,
            storagePath: photo.storagePath,
          }),
        );
      }),
    );

    res.status(200).json({
      session: {
        id: session.id,
        status: session.status,
        startedAt: session.startedAt.toISOString(),
        submittedAt: session.submittedAt?.toISOString() ?? null,
        kuehlerUnitId: session.kuehlerUnitId ?? null,
        kuehlerNumber,
      },
      market: {
        id: market.id,
        name: market.name,
        address: market.address,
        postalCode: market.postalCode,
        city: market.city,
      },
      sections: sections.map((section) => ({
        id: section.id,
        section: section.section,
        campaignId: section.campaignId,
        campaignName: campaignNameById.get(section.campaignId) ?? "",
        fragebogenId: section.fragebogenId,
        fragebogenName: section.fragebogenNameSnapshot,
        orderIndex: section.orderIndex,
        questions: (questionsBySectionId.get(section.id) ?? []).map((question) => {
          const answer = answersByQuestionId.get(question.id);
          const comment = commentsByQuestionId.get(question.id);
          const enrichedConfig = withPhotoTagMeta(
            question.questionType,
            (question.questionConfigSnapshot ?? {}) as Record<string, unknown>,
            tagMetaById,
          );
          const photos = answer ? (photosByAnswerId.get(answer.id) ?? []) : [];
          return {
            id: question.id,
            questionId: question.questionId,
            type: question.questionType,
            text: question.questionTextSnapshot,
            required: question.requiredSnapshot,
            singleChoiceAvailability: question.singleChoiceAvailabilitySnapshot ?? null,
            singleChoiceAvailabilityType: (question.singleChoiceAvailabilityTypeSnapshot as SingleChoiceAvailabilityType | null) ?? null,
            config: enrichedConfig,
            rules: question.questionRulesSnapshot,
            chains: normalizeQuestionChains(question.questionChainsSnapshot),
            appliesToMarketChain: Boolean(question.appliesToMarketChainSnapshot),
            answer: answer
              ? {
                  id: answer.id,
                  answerStatus: answer.answerStatus,
                  valueText: answer.valueText,
                  valueNumber: answer.valueNumber == null ? null : String(answer.valueNumber),
                  valueJson: answer.valueJson,
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
                    signedUrl: signedPhotoUrlById.get(photo.id)?.signedUrl ?? null,
                    signedUrlExpiresAt: signedPhotoUrlById.get(photo.id)?.expiresAt ?? null,
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
            comment: comment?.commentText ?? "",
          };
        }),
      })),
    });
  } catch (error) {
    next(error);
  }
});

gmVisitSessionsRouter.patch("/gm/visit-sessions/:sessionId/start", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    const sessionId = Array.isArray(req.params.sessionId) ? req.params.sessionId[0] : req.params.sessionId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    if (!isUuid(sessionId)) {
      res.status(400).json({ error: "Ungültige Session-ID." });
      return;
    }
    const parsed = z
      .object({
        startedAt: z.string().min(1),
      })
      .strict()
      .safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Startzeit." });
      return;
    }
    const requestedStartedAt = parseOptionalIsoTimestamp(parsed.data.startedAt);
    if (!requestedStartedAt) {
      res.status(400).json({ error: "Ungültige Startzeit." });
      return;
    }
    const now = new Date();
    const requestedMs = requestedStartedAt.getTime();
    if (requestedMs > now.getTime() + MAX_REQUESTED_VISIT_START_FUTURE_MS) {
      res.status(400).json({ error: "Startzeit darf nicht in der Zukunft liegen." });
      return;
    }
    if (requestedMs < now.getTime() - MAX_REQUESTED_VISIT_START_AGE_MS) {
      res.status(400).json({ error: "Startzeit ist zu alt." });
      return;
    }

    const [session] = await db
      .select()
      .from(visitSessions)
      .where(and(eq(visitSessions.id, sessionId), eq(visitSessions.gmUserId, gmUserId), eq(visitSessions.isDeleted, false)))
      .limit(1);
    if (!session) {
      res.status(404).json({ error: "Session nicht gefunden." });
      return;
    }
    if (session.status !== "draft" || session.submittedAt) {
      res.status(409).json({ error: "Startzeit kann nur bei laufenden Fragebögen geändert werden." });
      return;
    }

    await validateGmTimelineStartPoint({
      gmUserId,
      kind: "marktbesuch",
      id: session.id,
      startAt: requestedStartedAt,
      now,
    });

    await db
      .update(visitSessions)
      .set({
        startedAt: requestedStartedAt,
        lastSavedAt: now,
        updatedAt: now,
      })
      .where(eq(visitSessions.id, session.id));

    res.status(200).json({
      ok: true,
      session: {
        id: session.id,
        status: "draft",
        startedAt: requestedStartedAt.toISOString(),
        submittedAt: null,
      },
    });
  } catch (error) {
    if (respondTimelineValidationError(res, error)) return;
    next(error);
  }
});

gmVisitSessionsRouter.patch("/gm/visit-sessions/:sessionId/answers", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    const sessionId = Array.isArray(req.params.sessionId) ? req.params.sessionId[0] : req.params.sessionId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    if (!isUuid(sessionId)) {
      res.status(400).json({ error: "Ungültige Session-ID." });
      return;
    }

    const parsed = z
      .object({
        visitQuestionId: z.string().uuid(),
        answer: z.unknown().optional(),
        comment: z.string().max(4000).optional(),
      })
      .strict()
      .safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültiges Antwort-Payload." });
      return;
    }

    const [session] = await db
      .select()
      .from(visitSessions)
      .where(and(eq(visitSessions.id, sessionId), eq(visitSessions.gmUserId, gmUserId), eq(visitSessions.isDeleted, false)))
      .limit(1);
    if (!session) {
      res.status(404).json({ error: "Session nicht gefunden." });
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
      res.status(404).json({ error: "Frage in Session nicht gefunden." });
      return;
    }
    const isSubmittedPhotoInitializer =
      session.status === "submitted"
      && visitQuestion.questionType === "photo"
      && parsed.data.answer === undefined
      && parsed.data.comment === undefined;
    if (session.status !== "draft" && !isSubmittedPhotoInitializer) {
      res.status(409).json({ error: "Nur Foto-Antworten können in abgeschlossenen Sessions direkt aktualisiert werden." });
      return;
    }

    const validation = buildVisitAnswerValidationResult(
      visitQuestion.questionType,
      (visitQuestion.questionConfigSnapshot ?? {}) as Record<string, unknown>,
      parsed.data.answer,
    );
    const isPhotoPatchWithoutAnswer = visitQuestion.questionType === "photo" && parsed.data.answer === undefined;
    const now = new Date();

    const result = await db.transaction(async (tx) => {
      const siblingQuestions = await loadSiblingSessionQuestions(tx, {
        sessionId: session.id,
        questionId: visitQuestion.questionId,
      });
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

      for (const sibling of siblingQuestions) {
        const existing = existingAnswerByVisitQuestionId.get(sibling.visitQuestionId);
        let answerId = existing?.id ?? null;
        if (existing) {
          const nextAnswerStatus = isPhotoPatchWithoutAnswer ? existing.answerStatus : validation.answerStatus;
          const nextValueText = isPhotoPatchWithoutAnswer ? existing.valueText : validation.valueText;
          const nextValueNumber = isPhotoPatchWithoutAnswer ? existing.valueNumber : validation.valueNumber;
          const nextValueJson = isPhotoPatchWithoutAnswer ? existing.valueJson : validation.valueJson;
          const nextIsValid = isPhotoPatchWithoutAnswer ? existing.isValid : validation.isValid;
          const nextValidationError = isPhotoPatchWithoutAnswer ? existing.validationError : validation.validationError;
          const nextAnsweredAt = isPhotoPatchWithoutAnswer
            ? existing.answeredAt
            : validation.answerStatus === "answered"
              ? now
              : existing.answeredAt;
          const [updated] = await tx
            .update(visitAnswers)
            .set({
              answerStatus: nextAnswerStatus,
              valueText: nextValueText,
              valueNumber: nextValueNumber,
              valueJson: nextValueJson,
              isValid: nextIsValid,
              validationError: nextValidationError,
              answeredAt: nextAnsweredAt,
              changedAt: isPhotoPatchWithoutAnswer ? existing.changedAt : now,
              version: isPhotoPatchWithoutAnswer ? existing.version : existing.version + 1,
              updatedAt: now,
            })
            .where(eq(visitAnswers.id, existing.id))
            .returning();
          answerId = updated?.id ?? existing.id;
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
        }
        if (!answerId) throw new Error("Antwort konnte nicht gespeichert werden.");

        if (!isPhotoPatchWithoutAnswer) {
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

          await tx
            .update(visitAnswerPhotos)
            .set({ isDeleted: true, deletedAt: now, updatedAt: now })
            .where(and(eq(visitAnswerPhotos.visitAnswerId, answerId), eq(visitAnswerPhotos.isDeleted, false)));
          if (validation.inlinePhotos.length > 0) {
            const insertedPhotos = await tx
              .insert(visitAnswerPhotos)
              .values(
                validation.inlinePhotos.map((photo) => ({
                  visitAnswerId: answerId as string,
                  storageBucket: "inline_data_url",
                  storagePath: `inline-${photo.orderIndex}-${Date.now()}`,
                  mimeType: "text/plain",
                  byteSize: photo.payload.length,
                  widthPx: null,
                  heightPx: null,
                  sha256: null,
                  uploadedAt: now,
                  isDeleted: false,
                  deletedAt: null,
                  createdAt: now,
                  updatedAt: now,
                })),
              )
              .returning();

            if (insertedPhotos.length > 0) {
              const allTagIds = Array.from(
                new Set(validation.inlinePhotos.flatMap((photo) => photo.selectedTagIds).filter((id) => isUuid(id))),
              );
              const tagRows =
                allTagIds.length === 0
                  ? []
                  : await tx
                      .select({ id: photoTags.id, label: photoTags.label })
                      .from(photoTags)
                      .where(and(inArray(photoTags.id, allTagIds), eq(photoTags.isDeleted, false)));
              const tagById = new Map(tagRows.map((row) => [row.id, row.label]));

              const photoTagValues: Array<typeof visitAnswerPhotoTags.$inferInsert> = [];
              for (const photo of insertedPhotos) {
                for (const tagId of allTagIds) {
                  const label = tagById.get(tagId);
                  if (!label) continue;
                  photoTagValues.push({
                    visitAnswerPhotoId: photo.id,
                    photoTagId: tagId,
                    photoTagLabelSnapshot: label,
                    isDeleted: false,
                    deletedAt: null,
                    createdAt: now,
                    updatedAt: now,
                  });
                }
              }
              if (photoTagValues.length > 0) {
                await tx.insert(visitAnswerPhotoTags).values(photoTagValues);
              }
            }
          }
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

        resultByVisitQuestionId.set(sibling.visitQuestionId, {
          answerId: answerId as string,
          answerStatus:
            isPhotoPatchWithoutAnswer && existing
              ? existing.answerStatus
              : validation.answerStatus,
          isValid:
            isPhotoPatchWithoutAnswer && existing
              ? existing.isValid
              : validation.isValid,
          validationError:
            isPhotoPatchWithoutAnswer && existing
              ? existing.validationError
              : validation.validationError,
        });
      }

      await tx
        .update(visitSessions)
        .set({ lastSavedAt: now, updatedAt: now })
        .where(eq(visitSessions.id, session.id));

      const primaryResult = resultByVisitQuestionId.get(visitQuestion.visitQuestionId);
      if (!primaryResult) throw new Error("Antwort konnte nicht gespeichert werden.");
      return primaryResult;
    });

    res.status(200).json({ ok: true, result });
  } catch (error) {
    next(error);
  }
});

gmVisitSessionsRouter.post("/gm/visit-sessions/:sessionId/photos/presign", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    const sessionId = Array.isArray(req.params.sessionId) ? req.params.sessionId[0] : req.params.sessionId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    if (!isUuid(sessionId)) {
      res.status(400).json({ error: "Ungültige Session-ID." });
      return;
    }
    const parsed = z
      .object({
        visitAnswerId: z.string().uuid(),
        extension: z.string().max(10).optional(),
      })
      .strict()
      .safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Photo-Presign Eingabe." });
      return;
    }

    const [answerRow] = await db
      .select({
        answerId: visitAnswers.id,
        sessionId: visitAnswers.visitSessionId,
        questionType: visitAnswers.questionType,
      })
      .from(visitAnswers)
      .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
      .where(
        and(
          eq(visitAnswers.id, parsed.data.visitAnswerId),
          eq(visitAnswers.isDeleted, false),
          eq(visitSessions.id, sessionId),
          eq(visitSessions.gmUserId, gmUserId),
          inArray(visitSessions.status, ["draft", "submitted"]),
          eq(visitSessions.isDeleted, false),
        ),
      )
      .limit(1);
    if (!answerRow) {
      res.status(404).json({ error: "Antwort für Photo-Upload nicht gefunden." });
      return;
    }
    if (answerRow.questionType !== "photo") {
      res.status(400).json({ error: "Photo-Presign ist nur für Foto-Fragen erlaubt." });
      return;
    }

    const ext = normalizeExt(parsed.data.extension);
    const path = `visit-photos/${sessionId}/${parsed.data.visitAnswerId}/${crypto.randomUUID()}.${ext}`;
    const { data, error } = await supabaseAdmin.storage.from(VISIT_PHOTOS_BUCKET).createSignedUploadUrl(path);
    if (error || !data) {
      res.status(500).json({ error: "Signed Upload URL konnte nicht erstellt werden." });
      return;
    }

    res.status(200).json({
      upload: {
        bucket: VISIT_PHOTOS_BUCKET,
        path: data.path,
        signedUrl: data.signedUrl,
        token: data.token,
      },
    });
  } catch (error) {
    next(error);
  }
});

gmVisitSessionsRouter.post("/gm/visit-sessions/:sessionId/photos/commit", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    const sessionId = Array.isArray(req.params.sessionId) ? req.params.sessionId[0] : req.params.sessionId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    if (!isUuid(sessionId)) {
      res.status(400).json({ error: "Ungültige Session-ID." });
      return;
    }
    const parsed = z
      .object({
        visitAnswerId: z.string().uuid(),
        photos: z.array(
          z.object({
            storageBucket: z.string().min(1),
            storagePath: z.string().min(1),
            mimeType: z.string().optional(),
            byteSize: z.number().int().min(0).optional(),
            widthPx: z.number().int().min(1).optional(),
            heightPx: z.number().int().min(1).optional(),
            sha256: z.string().optional(),
            photoTagIds: z.array(z.string().uuid()).optional(),
          }).strict(),
        ),
      })
      .strict()
      .safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültiges Photo-Commit Payload." });
      return;
    }

    const [answerRow] = await db
      .select({
        answerId: visitAnswers.id,
        visitSessionQuestionId: visitAnswers.visitSessionQuestionId,
        sharedQuestionId: visitAnswers.questionId,
        questionType: visitAnswers.questionType,
        requiredSnapshot: visitSessionQuestions.requiredSnapshot,
        questionConfigSnapshot: visitSessionQuestions.questionConfigSnapshot,
      })
      .from(visitAnswers)
      .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
      .innerJoin(visitSessionQuestions, eq(visitSessionQuestions.id, visitAnswers.visitSessionQuestionId))
      .where(
        and(
          eq(visitAnswers.id, parsed.data.visitAnswerId),
          eq(visitAnswers.isDeleted, false),
          eq(visitSessions.id, sessionId),
          eq(visitSessions.gmUserId, gmUserId),
          inArray(visitSessions.status, ["draft", "submitted"]),
          eq(visitSessions.isDeleted, false),
        ),
      )
      .limit(1);
    if (!answerRow) {
      res.status(404).json({ error: "Antwort für Photo-Commit nicht gefunden." });
      return;
    }
    if (answerRow.questionType !== "photo") {
      res.status(400).json({ error: "Photo-Commit ist nur für Foto-Fragen erlaubt." });
      return;
    }
    const photoConfig = (answerRow.questionConfigSnapshot ?? {}) as Record<string, unknown>;
    const allowedTagIds = new Set(
      Array.isArray(photoConfig.tagIds) ? photoConfig.tagIds.filter((id): id is string => typeof id === "string") : [],
    );
    const existingRequestedAnswerPhotos = await db
      .select({ storagePath: visitAnswerPhotos.storagePath })
      .from(visitAnswerPhotos)
      .where(and(eq(visitAnswerPhotos.visitAnswerId, parsed.data.visitAnswerId), eq(visitAnswerPhotos.isDeleted, false)));
    const existingRequestedAnswerPaths = new Set(
      existingRequestedAnswerPhotos.map((row) => row.storagePath),
    );
    const expectedPrefix = `visit-photos/${sessionId}/${parsed.data.visitAnswerId}/`;
    const normalizedPhotos: Array<{
      storageBucket: string;
      storagePath: string;
      mimeType: string | undefined;
      byteSize: number | undefined;
      widthPx: number | undefined;
      heightPx: number | undefined;
      sha256: string | undefined;
      photoTagIds: string[];
    }> = [];
    const photoByPath = new Map<string, (typeof normalizedPhotos)[number]>();
    const preferDefined = <T>(current: T | undefined, incoming: T | undefined): T | undefined =>
      current !== undefined ? current : incoming;
    const hasConflict = <T>(left: T | undefined, right: T | undefined): boolean =>
      left !== undefined && right !== undefined && left !== right;
    for (const photo of parsed.data.photos) {
      if (photo.storageBucket !== VISIT_PHOTOS_BUCKET) {
        res.status(400).json({ error: "Ungültiger Storage-Bucket für Foto-Commit." });
        return;
      }
      if (!photo.storagePath.startsWith(expectedPrefix) && !existingRequestedAnswerPaths.has(photo.storagePath)) {
        res.status(400).json({ error: "Ungültiger Storage-Pfad für Foto-Commit." });
        return;
      }
      const dedupedTagIds = normalizeUnique(photo.photoTagIds ?? []);
      const requestedTagIds = dedupedTagIds;
      if (allowedTagIds.size > 0 && requestedTagIds.some((tagId) => !allowedTagIds.has(tagId))) {
        res.status(400).json({ error: "Foto-Commit enthält nicht konfigurierte Tags." });
        return;
      }
      const existing = photoByPath.get(photo.storagePath);
      if (existing) {
        if (hasConflict(existing.storageBucket, photo.storageBucket)) {
          res.status(400).json({ error: "Foto-Commit enthält widersprüchliche Bucket-Daten für denselben Storage-Pfad." });
          return;
        }
        if (hasConflict(existing.sha256, photo.sha256)) {
          res.status(400).json({ error: "Foto-Commit enthält widersprüchliche SHA-Werte für denselben Storage-Pfad." });
          return;
        }
        if (hasConflict(existing.mimeType, photo.mimeType)) {
          res.status(400).json({ error: "Foto-Commit enthält widersprüchliche MIME-Daten für denselben Storage-Pfad." });
          return;
        }
        if (hasConflict(existing.byteSize, photo.byteSize)) {
          res.status(400).json({ error: "Foto-Commit enthält widersprüchliche Größen-Daten für denselben Storage-Pfad." });
          return;
        }
        if (hasConflict(existing.widthPx, photo.widthPx)) {
          res.status(400).json({ error: "Foto-Commit enthält widersprüchliche Breiten-Daten für denselben Storage-Pfad." });
          return;
        }
        if (hasConflict(existing.heightPx, photo.heightPx)) {
          res.status(400).json({ error: "Foto-Commit enthält widersprüchliche Höhen-Daten für denselben Storage-Pfad." });
          return;
        }
        existing.mimeType = preferDefined(existing.mimeType, photo.mimeType);
        existing.byteSize = preferDefined(existing.byteSize, photo.byteSize);
        existing.widthPx = preferDefined(existing.widthPx, photo.widthPx);
        existing.heightPx = preferDefined(existing.heightPx, photo.heightPx);
        existing.sha256 = preferDefined(existing.sha256, photo.sha256);
        existing.photoTagIds = normalizeUnique([...existing.photoTagIds, ...dedupedTagIds]);
        continue;
      }
      const normalized: {
        storageBucket: string;
        storagePath: string;
        mimeType: string | undefined;
        byteSize: number | undefined;
        widthPx: number | undefined;
        heightPx: number | undefined;
        sha256: string | undefined;
        photoTagIds: string[];
      } = {
        storageBucket: photo.storageBucket,
        storagePath: photo.storagePath,
        mimeType: undefined,
        byteSize: undefined,
        widthPx: undefined,
        heightPx: undefined,
        sha256: undefined,
        photoTagIds: dedupedTagIds,
      };
      if (photo.mimeType !== undefined) normalized.mimeType = photo.mimeType;
      if (photo.byteSize !== undefined) normalized.byteSize = photo.byteSize;
      if (photo.widthPx !== undefined) normalized.widthPx = photo.widthPx;
      if (photo.heightPx !== undefined) normalized.heightPx = photo.heightPx;
      if (photo.sha256 !== undefined) normalized.sha256 = photo.sha256;
      photoByPath.set(photo.storagePath, normalized);
      normalizedPhotos.push(normalized);
    }
    if (process.env.NODE_ENV !== "test") {
      for (const photo of normalizedPhotos) {
        if (existingRequestedAnswerPaths.has(photo.storagePath)) continue;
        const exists = await verifyStorageObjectExists(photo.storageBucket, photo.storagePath);
        if (!exists) {
          res.status(400).json({ error: "Ein oder mehrere Fotos sind nicht im Storage auffindbar." });
          return;
        }
      }
    }

    const now = new Date();
    await db.transaction(async (tx) => {
      const siblingQuestions = await loadSiblingSessionQuestions(tx, {
        sessionId,
        questionId: answerRow.sharedQuestionId,
      });
      const siblingQuestionIds = siblingQuestions.map((row) => row.visitQuestionId);
      if (siblingQuestionIds.length === 0) {
        throw new Error("Keine zugehörigen Fragen für Foto-Commit gefunden.");
      }

      await tx.execute(
        sql`SELECT id FROM ${visitSessionQuestions} WHERE ${inArray(visitSessionQuestions.id, siblingQuestionIds)} FOR UPDATE`,
      );

      const siblingAnswers = await tx
        .select()
        .from(visitAnswers)
        .where(and(inArray(visitAnswers.visitSessionQuestionId, siblingQuestionIds), eq(visitAnswers.isDeleted, false)));
      const siblingAnswerByVisitQuestionId = new Map(
        siblingAnswers.map((row) => [row.visitSessionQuestionId, row]),
      );

      const targetAnswers: Array<{ answerId: string; visitQuestionId: string; requiredSnapshot: boolean; questionConfigSnapshot: Record<string, unknown> }> = [];
      for (const sibling of siblingQuestions) {
        const existing = siblingAnswerByVisitQuestionId.get(sibling.visitQuestionId);
        if (existing) {
          targetAnswers.push({
            answerId: existing.id,
            visitQuestionId: sibling.visitQuestionId,
            requiredSnapshot: sibling.requiredSnapshot,
            questionConfigSnapshot: sibling.questionConfigSnapshot,
          });
          continue;
        }
        const [created] = await tx
          .insert(visitAnswers)
          .values({
            visitSessionId: sessionId,
            visitSessionSectionId: sibling.visitSessionSectionId,
            visitSessionQuestionId: sibling.visitQuestionId,
            questionId: sibling.questionId,
            questionType: sibling.questionType,
            answerStatus: "unanswered",
            isValid: true,
            validationError: null,
            answeredAt: null,
            changedAt: now,
            version: 1,
            isDeleted: false,
            deletedAt: null,
            createdAt: now,
            updatedAt: now,
          })
          .returning();
        if (!created) throw new Error("Foto-Antwort konnte nicht erstellt werden.");
        targetAnswers.push({
          answerId: created.id,
          visitQuestionId: sibling.visitQuestionId,
          requiredSnapshot: sibling.requiredSnapshot,
          questionConfigSnapshot: sibling.questionConfigSnapshot,
        });
      }

      const allTagIds = Array.from(
        new Set(normalizedPhotos.flatMap((photo) => photo.photoTagIds ?? [])),
      );
      const tagRows = allTagIds.length === 0
        ? []
        : await tx
            .select({ id: photoTags.id, label: photoTags.label })
            .from(photoTags)
            .where(and(inArray(photoTags.id, allTagIds), eq(photoTags.isDeleted, false)));
      const tagById = new Map(tagRows.map((row) => [row.id, row.label]));

      for (const target of targetAnswers) {
        const targetPrefix = `visit-photos/${sessionId}/${target.answerId}/`;
        const requestedPrefix = `visit-photos/${sessionId}/${parsed.data.visitAnswerId}/`;
        const normalizedForTarget = normalizedPhotos.map((photo) => ({
          ...photo,
          storagePath: photo.storagePath.startsWith(requestedPrefix)
            ? `${targetPrefix}${photo.storagePath.slice(requestedPrefix.length)}`
            : photo.storagePath,
        }));

        const existingPhotos = await tx
          .select({ id: visitAnswerPhotos.id })
          .from(visitAnswerPhotos)
          .where(and(eq(visitAnswerPhotos.visitAnswerId, target.answerId), eq(visitAnswerPhotos.isDeleted, false)));
        const existingPhotoIds = existingPhotos.map((row) => row.id);
        if (existingPhotoIds.length > 0) {
          await tx
            .update(visitAnswerPhotoTags)
            .set({ isDeleted: true, deletedAt: now, updatedAt: now })
            .where(and(inArray(visitAnswerPhotoTags.visitAnswerPhotoId, existingPhotoIds), eq(visitAnswerPhotoTags.isDeleted, false)));
        }
        await tx
          .update(visitAnswerPhotos)
          .set({ isDeleted: true, deletedAt: now, updatedAt: now })
          .where(and(eq(visitAnswerPhotos.visitAnswerId, target.answerId), eq(visitAnswerPhotos.isDeleted, false)));

        const inserted = normalizedForTarget.length === 0
          ? []
          : await tx
              .insert(visitAnswerPhotos)
              .values(
                normalizedForTarget.map((photo) => ({
                  visitAnswerId: target.answerId,
                  storageBucket: photo.storageBucket,
                  storagePath: photo.storagePath,
                  mimeType: photo.mimeType,
                  byteSize: photo.byteSize,
                  widthPx: photo.widthPx,
                  heightPx: photo.heightPx,
                  sha256: photo.sha256,
                  uploadedAt: now,
                  isDeleted: false,
                  deletedAt: null,
                  createdAt: now,
                  updatedAt: now,
                })),
              )
              .returning();

        const photoTagValues: Array<typeof visitAnswerPhotoTags.$inferInsert> = [];
        inserted.forEach((photoRow, idx) => {
          const ids = normalizedForTarget[idx]?.photoTagIds ?? [];
          ids.forEach((tagId) => {
            const label = tagById.get(tagId);
            if (!label) return;
            photoTagValues.push({
              visitAnswerPhotoId: photoRow.id,
              photoTagId: tagId,
              photoTagLabelSnapshot: label,
              isDeleted: false,
              deletedAt: null,
              createdAt: now,
              updatedAt: now,
            });
          });
        });
        if (photoTagValues.length > 0) {
          await tx.insert(visitAnswerPhotoTags).values(photoTagValues);
        }

        const config = target.questionConfigSnapshot ?? {};
        const tagsEnabled = Boolean(config.tagsEnabled) && Array.isArray(config.tagIds) && (config.tagIds as unknown[]).length > 0;
        const requiredPhotoQuestion = Boolean(target.requiredSnapshot);
        const requiresTagSelection = requiredPhotoQuestion && tagsEnabled;
        const taggedPhotoIds = new Set(photoTagValues.map((value) => value.visitAnswerPhotoId));
        const everyPhotoHasTag = !tagsEnabled || inserted.every((photoRow) => taggedPhotoIds.has(photoRow.id));
        const isAnswered = inserted.length > 0;
        const isValid = requiredPhotoQuestion ? isAnswered && everyPhotoHasTag : true;

        await tx
          .update(visitAnswers)
          .set({
            answerStatus: isAnswered ? (isValid ? "answered" : "invalid") : "unanswered",
            isValid,
            validationError: isValid ? null : (requiresTagSelection ? "Foto-Frage benötigt mindestens einen Tag." : null),
            valueJson: {
              storage: inserted.map((row) => ({
                id: row.id,
                bucket: row.storageBucket,
                path: row.storagePath,
              })),
            },
            answeredAt: isAnswered ? now : null,
            changedAt: now,
            version: sql`${visitAnswers.version} + 1`,
            updatedAt: now,
          })
          .where(eq(visitAnswers.id, target.answerId));
      }

      await tx
        .update(visitSessions)
        .set({ lastSavedAt: now, updatedAt: now })
        .where(eq(visitSessions.id, sessionId));
    });

    res.status(200).json({ ok: true });
  } catch (error) {
    next(error);
  }
});

gmVisitSessionsRouter.post("/gm/visit-sessions/:sessionId/submit", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    const sessionId = Array.isArray(req.params.sessionId) ? req.params.sessionId[0] : req.params.sessionId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    if (!isUuid(sessionId)) {
      res.status(400).json({ error: "Ungültige Session-ID." });
      return;
    }
    const submitPayload = z
      .object({
        startedAt: z.string().optional(),
        submittedAt: z.string().optional(),
      })
      .strict()
      .safeParse(req.body ?? {});
    if (!submitPayload.success) {
      res.status(400).json({ error: "Ungültige Abschlussdaten." });
      return;
    }
    const requestedStartedAt = parseOptionalIsoTimestamp(submitPayload.data.startedAt);
    const requestedSubmittedAt = parseOptionalIsoTimestamp(submitPayload.data.submittedAt);
    if ((requestedStartedAt == null) !== (requestedSubmittedAt == null)) {
      res.status(400).json({ error: "Start- und Endzeit müssen gemeinsam übermittelt werden." });
      return;
    }
    if (submitPayload.data.startedAt && requestedStartedAt == null) {
      res.status(400).json({ error: "Ungültige Startzeit." });
      return;
    }
    if (submitPayload.data.submittedAt && requestedSubmittedAt == null) {
      res.status(400).json({ error: "Ungültige Endzeit." });
      return;
    }
    if (requestedStartedAt && requestedSubmittedAt && requestedSubmittedAt.getTime() < requestedStartedAt.getTime()) {
      res.status(400).json({ error: "Endzeit darf nicht vor Startzeit liegen." });
      return;
    }

    const [session] = await db
      .select()
      .from(visitSessions)
      .where(and(eq(visitSessions.id, sessionId), eq(visitSessions.gmUserId, gmUserId), eq(visitSessions.isDeleted, false)))
      .limit(1);
    if (!session) {
      res.status(404).json({ error: "Session nicht gefunden." });
      return;
    }
    if (session.status !== "draft") {
      res.status(409).json({ error: "Session wurde bereits abgeschlossen." });
      return;
    }

    const now = new Date();
    const effectiveStartedAt = requestedStartedAt ?? session.startedAt;
    const effectiveSubmittedAt = requestedSubmittedAt ?? now;
    await validateGmTimelineInterval({
      gmUserId,
      kind: "marktbesuch",
      id: session.id,
      startAt: effectiveStartedAt,
      endAt: effectiveSubmittedAt,
      now,
    });

    const sections = await db
      .select()
      .from(visitSessionSections)
      .where(and(eq(visitSessionSections.visitSessionId, session.id), eq(visitSessionSections.isDeleted, false)));
    const sectionIds = sections.map((row) => row.id);
    const questions =
      sectionIds.length === 0
        ? []
        : await db
            .select({
              id: visitSessionQuestions.id,
              visitSessionSectionId: visitSessionQuestions.visitSessionSectionId,
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
    const questionIds = questions.map((row) => row.id);
    const answers =
      questionIds.length === 0
        ? []
        : await db
            .select()
            .from(visitAnswers)
            .where(and(inArray(visitAnswers.visitSessionQuestionId, questionIds), eq(visitAnswers.isDeleted, false)));
    const answersByQuestionId = new Map(answers.map((row) => [row.visitSessionQuestionId, row]));
    const answerIds = answers.map((row) => row.id);
    const committedPhotos =
      answerIds.length === 0
        ? []
        : await db
            .select()
            .from(visitAnswerPhotos)
            .where(and(inArray(visitAnswerPhotos.visitAnswerId, answerIds), eq(visitAnswerPhotos.isDeleted, false)));
    const photoIds = committedPhotos.map((row) => row.id);
    const committedPhotoTags =
      photoIds.length === 0
        ? []
        : await db
            .select()
            .from(visitAnswerPhotoTags)
            .where(and(inArray(visitAnswerPhotoTags.visitAnswerPhotoId, photoIds), eq(visitAnswerPhotoTags.isDeleted, false)));
    const photosByAnswerId = new Map<string, typeof committedPhotos>();
    for (const row of committedPhotos) {
      const bucket = photosByAnswerId.get(row.visitAnswerId) ?? [];
      bucket.push(row);
      photosByAnswerId.set(row.visitAnswerId, bucket);
    }
    const tagsByPhotoId = new Map<string, typeof committedPhotoTags>();
    for (const row of committedPhotoTags) {
      const bucket = tagsByPhotoId.get(row.visitAnswerPhotoId) ?? [];
      bucket.push(row);
      tagsByPhotoId.set(row.visitAnswerPhotoId, bucket);
    }

    const { missingRequired } = computeMissingRequiredQuestions({
      questions: questions.map((question) => ({
        id: question.id,
        questionId: question.questionId,
        questionType: question.questionType,
        questionTextSnapshot: question.questionTextSnapshot,
        questionConfigSnapshot: (question.questionConfigSnapshot ?? {}) as Record<string, unknown>,
        questionRulesSnapshot: (question.questionRulesSnapshot ?? []) as Array<Record<string, unknown>>,
        requiredSnapshot: question.requiredSnapshot,
        appliesToMarketChainSnapshot: Boolean(question.appliesToMarketChainSnapshot),
      })),
      answers: answers.map((answer) => ({
        id: answer.id,
        visitSessionQuestionId: answer.visitSessionQuestionId,
        answerStatus: answer.answerStatus,
        valueText: answer.valueText,
        valueNumber: answer.valueNumber == null ? null : String(answer.valueNumber),
        valueJson: (answer.valueJson as Record<string, unknown> | null) ?? null,
        isValid: answer.isValid,
      })),
      photos: committedPhotos.map((photo) => ({
        id: photo.id,
        visitAnswerId: photo.visitAnswerId,
        storagePath: photo.storagePath,
      })),
      photoTags: committedPhotoTags.map((tag) => ({
        visitAnswerPhotoId: tag.visitAnswerPhotoId,
      })),
    });

    if (missingRequired.length > 0) {
      res.status(409).json({
        error: "Nicht alle Pflichtfragen sind vollständig beantwortet.",
        code: "visit_submit_incomplete_required",
        missingRequired,
      });
      return;
    }

    await db.transaction(async (tx) => {
      await tx
        .update(visitSessionSections)
        .set({ status: "submitted", updatedAt: now })
        .where(and(eq(visitSessionSections.visitSessionId, session.id), eq(visitSessionSections.isDeleted, false)));
      await tx
        .update(visitSessions)
        .set({
          status: "submitted",
          startedAt: effectiveStartedAt,
          submittedAt: effectiveSubmittedAt,
          lastSavedAt: now,
          updatedAt: now,
        })
        .where(eq(visitSessions.id, session.id));
      await finalizeBonusForSubmittedVisitSessionTx(tx, {
        sessionId: session.id,
        gmUserId,
        marketId: session.marketId,
        submittedAt: effectiveSubmittedAt,
      });
    });

    try {
      await enqueueIppRecalcForDate(session.marketId, effectiveSubmittedAt, "visit_session_submitted");
    } catch (enqueueError) {
      logger.error("gm_visit_session_submit_ipp_enqueue_failed", {
        action: "gm_visit_session_submit",
        result: "failure",
        sessionId: session.id,
        marketId: session.marketId,
        error: enqueueError instanceof Error ? enqueueError.message : String(enqueueError),
      });
    }
    try {
      await recomputeGmKpiCache(gmUserId);
    } catch (kpiError) {
      logger.error("gm_visit_session_submit_kpi_recompute_failed", {
        action: "gm_visit_session_submit",
        result: "failure",
        sessionId: session.id,
        gmUserId,
        error: kpiError instanceof Error ? kpiError.message : String(kpiError),
      });
    }

    res.status(200).json({ ok: true, sessionId: session.id, status: "submitted" });
  } catch (error) {
    if (respondTimelineValidationError(res, error)) return;
    next(error);
  }
});

gmVisitSessionsRouter.delete("/gm/visit-sessions/:sessionId", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const sessionId = Array.isArray(req.params.sessionId) ? req.params.sessionId[0] : req.params.sessionId;
    if (!isUuid(sessionId)) {
      res.status(400).json({ error: "Ungültige Visit-Session-ID." });
      return;
    }
    const [session] = await db
      .select()
      .from(visitSessions)
      .where(and(eq(visitSessions.id, sessionId), eq(visitSessions.gmUserId, gmUserId), eq(visitSessions.isDeleted, false)))
      .limit(1);
    if (!session) {
      res.status(404).json({ error: "Visit-Session nicht gefunden." });
      return;
    }
    if (session.status !== "draft" || session.submittedAt != null) {
      res.status(409).json({
        error: "Nur laufende, nicht abgeschlossene Fragebögen können abgebrochen werden.",
        code: "visit_cancel_not_draft",
      });
      return;
    }

    const photoRows = await db
      .select({
        bucket: visitAnswerPhotos.storageBucket,
        path: visitAnswerPhotos.storagePath,
      })
      .from(visitAnswerPhotos)
      .innerJoin(visitAnswers, eq(visitAnswers.id, visitAnswerPhotos.visitAnswerId))
      .where(
        and(
          eq(visitAnswers.visitSessionId, session.id),
          eq(visitAnswers.isDeleted, false),
          eq(visitAnswerPhotos.isDeleted, false),
        ),
      );

    await db.delete(visitSessions).where(eq(visitSessions.id, session.id));

    const pathsByBucket = new Map<string, string[]>();
    for (const row of photoRows) {
      const bucket = row.bucket.trim();
      const path = row.path.trim();
      if (!bucket || !path) continue;
      pathsByBucket.set(bucket, [...(pathsByBucket.get(bucket) ?? []), path]);
    }
    for (const [bucket, paths] of pathsByBucket.entries()) {
      try {
        const { error } = await supabaseAdmin.storage.from(bucket).remove(Array.from(new Set(paths)));
        if (error) {
          logger.warn("gm_visit_session_cancel_photo_cleanup_failed", {
            action: "gm_visit_session_cancel",
            result: "failure",
            sessionId: session.id,
            bucket,
            error: error.message,
          });
        }
      } catch (cleanupError) {
        logger.warn("gm_visit_session_cancel_photo_cleanup_failed", {
          action: "gm_visit_session_cancel",
          result: "failure",
          sessionId: session.id,
          bucket,
          error: cleanupError instanceof Error ? cleanupError.message : String(cleanupError),
        });
      }
    }

    res.status(200).json({ ok: true, sessionId: session.id, status: "cancelled" });
  } catch (error) {
    next(error);
  }
});

gmVisitSessionsRouter.get("/gm/bonus-summary", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const summary = await readGmActiveBonusSummary(gmUserId, new Date());
    res.status(200).json(summary);
  } catch (error) {
    next(error);
  }
});

gmVisitSessionsRouter.get("/gm/kpi-summary", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const summary = await ensureAndGetGmKpiCache(gmUserId);
    res.status(200).json(summary);
  } catch (error) {
    next(error);
  }
});

export { gmVisitSessionsRouter };
