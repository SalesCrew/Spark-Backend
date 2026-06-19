import { and, count, desc, eq, ilike, inArray, isNotNull, or, sql, type SQL } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import { requireKundeAdminPermission } from "../lib/kunde-access.js";
import { supabaseAdmin } from "../lib/supabase.js";
import { requireAuth } from "../middleware/auth.js";
import {
  campaigns,
  markets,
  users,
  visitAnswerPhotoTags,
  visitAnswerPhotos,
  visitAnswers,
  visitQuestionComments,
  visitSessionQuestions,
  visitSessionSections,
  visitSessions,
} from "../lib/schema.js";

const adminPhotosRouter = Router();
adminPhotosRouter.use(requireAuth(["admin", "kunde"]));
adminPhotosRouter.use(requireKundeAdminPermission);

const campaignSectionSchema = z.enum(["standard", "flex", "billa", "kuehler", "mhd"]);
const photoSignVariantSchema = z.enum(["preview", "original"]);
const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const SIGNED_URL_TTL_SECONDS = 15 * 60;
const SIGNED_URL_CACHE_TTL_MS = 12 * 60 * 1000;
const SIGNED_URL_CACHE_MAX_ENTRIES = 1000;
const PREVIEW_TRANSFORM = {
  width: 640,
  quality: 65,
  resize: "contain" as const,
};

const photoListQuerySchema = z.object({
  page: z.coerce.number().int().min(1).max(10000).default(1),
  pageSize: z.coerce.number().int().min(1).max(80).default(30),
  search: z.string().trim().max(160).optional(),
  campaignId: z.string().regex(uuidRegex).optional(),
  campaignType: campaignSectionSchema.optional(),
  dateFrom: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
  dateTo: z.string().regex(/^\d{4}-\d{2}-\d{2}$/).optional(),
  week: z.string().trim().regex(/^\d{4}-W\d{2}$/).optional(),
  marketId: z.string().regex(uuidRegex).optional(),
  region: z.string().trim().max(120).optional(),
  city: z.string().trim().max(120).optional(),
  postalCode: z.string().trim().max(24).optional(),
  chain: z.string().trim().max(80).optional(),
  gmUserId: z.string().regex(uuidRegex).optional(),
  tagId: z.string().regex(uuidRegex).optional(),
  tagLabel: z.string().trim().max(120).optional(),
  questionId: z.string().regex(uuidRegex).optional(),
  moduleId: z.string().regex(uuidRegex).optional(),
});

type PhotoListQuery = z.infer<typeof photoListQuerySchema>;
type PhotoSignVariant = z.infer<typeof photoSignVariantSchema>;

const photoSignedUrlsBodySchema = z.object({
  photoIds: z.array(z.string().regex(uuidRegex)).min(1).max(40),
  variant: photoSignVariantSchema,
});

type PhotoRow = {
  id: string;
  visitAnswerId: string;
  visitSessionId: string;
  storageBucket: string;
  storagePath: string;
  mimeType: string | null;
  byteSize: number | null;
  widthPx: number | null;
  heightPx: number | null;
  sha256: string | null;
  uploadedAt: Date;
  questionId: string;
  visitQuestionId: string;
  questionText: string;
  moduleId: string | null;
  moduleName: string;
  sectionName: string;
  fragebogenName: string;
  campaignId: string;
  campaignName: string;
  campaignType: "standard" | "flex" | "billa" | "kuehler" | "mhd";
  campaignStartDate: string | null;
  campaignEndDate: string | null;
  marketId: string;
  marketName: string;
  marketStandardMarketNumber: string | null;
  marketCokeMasterNumber: string | null;
  marketKuehlerStammnr: string | null;
  marketAddress: string;
  marketPostalCode: string;
  marketCity: string;
  marketRegion: string;
  marketChain: string;
  gmUserId: string;
  gmFirstName: string;
  gmLastName: string;
  startedAt: Date;
  submittedAt: Date;
  durationMinutes: number | null;
  comment: string | null;
};

type PhotoTagRow = {
  id: string;
  visitAnswerPhotoId: string;
  photoTagId: string | null;
  label: string;
};

type SignedUrlCacheEntry = {
  signedUrl: string | null;
  expiresAt: string;
  cacheUntilMs: number;
};

const signedUrlCache = new Map<string, SignedUrlCacheEntry>();

function cleanStoragePath(path: string): string {
  return path.trim().replace(/^\/+/, "");
}

function signedUrlCacheKey(input: {
  photoId: string;
  bucket: string;
  storagePath: string;
  variant: PhotoSignVariant;
}): string {
  return [
    input.photoId,
    input.variant,
    input.bucket.trim(),
    cleanStoragePath(input.storagePath),
    input.variant === "preview" ? `w${PREVIEW_TRANSFORM.width}-q${PREVIEW_TRANSFORM.quality}-${PREVIEW_TRANSFORM.resize}` : "original",
  ].join(":");
}

function pruneSignedUrlCache(nowMs = Date.now()): void {
  if (signedUrlCache.size <= SIGNED_URL_CACHE_MAX_ENTRIES) return;
  for (const [key, entry] of signedUrlCache) {
    if (entry.cacheUntilMs <= nowMs) signedUrlCache.delete(key);
  }
  if (signedUrlCache.size <= SIGNED_URL_CACHE_MAX_ENTRIES) return;
  const overflow = signedUrlCache.size - SIGNED_URL_CACHE_MAX_ENTRIES;
  let removed = 0;
  for (const key of signedUrlCache.keys()) {
    signedUrlCache.delete(key);
    removed += 1;
    if (removed >= overflow) break;
  }
}

async function createSignedReadUrl(input: {
  photoId: string;
  bucket: string;
  storagePath: string;
  variant: PhotoSignVariant;
}): Promise<{ signedUrl: string | null; expiresAt: string }> {
  const nowMs = Date.now();
  const cacheKey = signedUrlCacheKey(input);
  const cached = signedUrlCache.get(cacheKey);
  if (cached && cached.cacheUntilMs > nowMs) {
    return { signedUrl: cached.signedUrl, expiresAt: cached.expiresAt };
  }

  const expiresAt = new Date(nowMs + SIGNED_URL_TTL_SECONDS * 1000).toISOString();
  const normalizedPath = cleanStoragePath(input.storagePath);
  if (!input.bucket.trim() || !normalizedPath) return { signedUrl: null, expiresAt };

  const { data, error } = await supabaseAdmin.storage
    .from(input.bucket)
    .createSignedUrl(
      normalizedPath,
      SIGNED_URL_TTL_SECONDS,
      input.variant === "preview" ? { transform: PREVIEW_TRANSFORM } : undefined,
    );
  const result = { signedUrl: error || !data?.signedUrl ? null : data.signedUrl, expiresAt };
  signedUrlCache.set(cacheKey, {
    ...result,
    cacheUntilMs: nowMs + SIGNED_URL_CACHE_TTL_MS,
  });
  pruneSignedUrlCache(nowMs);
  return result;
}

function asNumber(value: unknown): number {
  const numeric = typeof value === "number" ? value : Number(value ?? 0);
  return Number.isFinite(numeric) ? numeric : 0;
}

function toIso(value: Date | string | null | undefined): string | null {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function marketChainExpression() {
  return sql<string>`coalesce(nullif(split_part(trim(${markets.name}), ' ', 1), ''), 'Unbekannt')`;
}

function buildPhotoWhere(input: PhotoListQuery, photoId?: string): SQL {
  const conditions: SQL[] = [
    eq(visitAnswerPhotos.isDeleted, false),
    eq(visitAnswers.isDeleted, false),
    eq(visitAnswers.questionType, "photo"),
    eq(visitSessionQuestions.isDeleted, false),
    eq(visitSessionQuestions.questionType, "photo"),
    eq(visitSessionSections.isDeleted, false),
    eq(visitSessions.isDeleted, false),
    eq(visitSessions.status, "submitted"),
    isNotNull(visitSessions.submittedAt),
    eq(campaigns.isDeleted, false),
    eq(markets.isDeleted, false),
  ];

  if (photoId) conditions.push(eq(visitAnswerPhotos.id, photoId));
  if (input.campaignId) conditions.push(eq(campaigns.id, input.campaignId));
  if (input.campaignType) conditions.push(eq(campaigns.section, input.campaignType));
  if (input.marketId) conditions.push(eq(markets.id, input.marketId));
  if (input.gmUserId) conditions.push(eq(users.id, input.gmUserId));
  if (input.questionId) conditions.push(eq(visitSessionQuestions.questionId, input.questionId));
  if (input.moduleId) conditions.push(eq(visitSessionQuestions.moduleId, input.moduleId));
  if (input.region) conditions.push(ilike(markets.region, input.region));
  if (input.city) conditions.push(ilike(markets.city, input.city));
  if (input.postalCode) conditions.push(ilike(markets.postalCode, input.postalCode));
  if (input.chain) conditions.push(sql`${marketChainExpression()} ILIKE ${input.chain}`);
  if (input.dateFrom) {
    conditions.push(sql`${visitSessions.submittedAt} >= (${input.dateFrom}::date::timestamp at time zone 'Europe/Vienna')`);
  }
  if (input.dateTo) {
    conditions.push(sql`${visitSessions.submittedAt} < ((${input.dateTo}::date + interval '1 day')::timestamp at time zone 'Europe/Vienna')`);
  }
  if (input.week) {
    conditions.push(sql`to_char(${visitSessions.submittedAt} AT TIME ZONE 'Europe/Vienna', 'IYYY-"W"IW') = ${input.week}`);
  }
  if (input.tagId) {
    conditions.push(sql`exists (
      select 1
      from visit_answer_photo_tags vapt_filter
      where vapt_filter.visit_answer_photo_id = ${visitAnswerPhotos.id}
        and vapt_filter.is_deleted = false
        and vapt_filter.photo_tag_id = ${input.tagId}
    )`);
  }
  if (input.tagLabel) {
    conditions.push(sql`exists (
      select 1
      from visit_answer_photo_tags vapt_label_filter
      where vapt_label_filter.visit_answer_photo_id = ${visitAnswerPhotos.id}
        and vapt_label_filter.is_deleted = false
        and vapt_label_filter.photo_tag_label_snapshot ilike ${`%${input.tagLabel}%`}
    )`);
  }
  if (input.search) {
    const needle = `%${input.search}%`;
    conditions.push(
      or(
        ilike(markets.name, needle),
        ilike(markets.dbName, needle),
        ilike(markets.address, needle),
        ilike(markets.postalCode, needle),
        ilike(markets.city, needle),
        ilike(markets.region, needle),
        ilike(campaigns.name, needle),
        ilike(users.firstName, needle),
        ilike(users.lastName, needle),
        ilike(visitSessionQuestions.questionTextSnapshot, needle),
        ilike(visitSessionQuestions.moduleNameSnapshot, needle),
        sql`exists (
          select 1
          from visit_answer_photo_tags vapt_search
          where vapt_search.visit_answer_photo_id = ${visitAnswerPhotos.id}
            and vapt_search.is_deleted = false
            and vapt_search.photo_tag_label_snapshot ilike ${needle}
        )`,
      ) ?? sql`false`,
    );
  }

  return and(...conditions) ?? sql`true`;
}

function basePhotoSelect(where: SQL) {
  return db
    .select({
      id: visitAnswerPhotos.id,
      visitAnswerId: visitAnswerPhotos.visitAnswerId,
      visitSessionId: visitSessions.id,
      storageBucket: visitAnswerPhotos.storageBucket,
      storagePath: visitAnswerPhotos.storagePath,
      mimeType: visitAnswerPhotos.mimeType,
      byteSize: visitAnswerPhotos.byteSize,
      widthPx: visitAnswerPhotos.widthPx,
      heightPx: visitAnswerPhotos.heightPx,
      sha256: visitAnswerPhotos.sha256,
      uploadedAt: visitAnswerPhotos.uploadedAt,
      questionId: visitSessionQuestions.questionId,
      visitQuestionId: visitSessionQuestions.id,
      questionText: visitSessionQuestions.questionTextSnapshot,
      moduleId: visitSessionQuestions.moduleId,
      moduleName: visitSessionQuestions.moduleNameSnapshot,
      sectionName: visitSessionSections.section,
      fragebogenName: visitSessionSections.fragebogenNameSnapshot,
      campaignId: campaigns.id,
      campaignName: campaigns.name,
      campaignType: campaigns.section,
      campaignStartDate: campaigns.startDate,
      campaignEndDate: campaigns.endDate,
      marketId: markets.id,
      marketName: markets.name,
      marketStandardMarketNumber: markets.standardMarketNumber,
      marketCokeMasterNumber: markets.cokeMasterNumber,
      marketKuehlerStammnr: markets.kuehlerStammnr,
      marketAddress: markets.address,
      marketPostalCode: markets.postalCode,
      marketCity: markets.city,
      marketRegion: markets.region,
      marketChain: marketChainExpression(),
      gmUserId: users.id,
      gmFirstName: users.firstName,
      gmLastName: users.lastName,
      startedAt: visitSessions.startedAt,
      submittedAt: visitSessions.submittedAt,
      durationMinutes: sql<number | null>`case
        when ${visitSessions.startedAt} is null or ${visitSessions.submittedAt} is null then null
        else greatest(0, round(extract(epoch from (${visitSessions.submittedAt} - ${visitSessions.startedAt})) / 60))::int
      end`,
      comment: visitQuestionComments.commentText,
    })
    .from(visitAnswerPhotos)
    .innerJoin(visitAnswers, eq(visitAnswers.id, visitAnswerPhotos.visitAnswerId))
    .innerJoin(visitSessionQuestions, eq(visitSessionQuestions.id, visitAnswers.visitSessionQuestionId))
    .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitAnswers.visitSessionSectionId))
    .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
    .innerJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
    .innerJoin(markets, eq(markets.id, visitSessions.marketId))
    .innerJoin(users, eq(users.id, visitSessions.gmUserId))
    .leftJoin(
      visitQuestionComments,
      and(
        eq(visitQuestionComments.visitSessionQuestionId, visitSessionQuestions.id),
        eq(visitQuestionComments.isDeleted, false),
      ),
    )
    .where(where);
}

async function loadTagsByPhotoId(photoIds: string[]): Promise<Map<string, PhotoTagRow[]>> {
  if (photoIds.length === 0) return new Map();
  const rows = await db
    .select({
      id: visitAnswerPhotoTags.id,
      visitAnswerPhotoId: visitAnswerPhotoTags.visitAnswerPhotoId,
      photoTagId: visitAnswerPhotoTags.photoTagId,
      label: visitAnswerPhotoTags.photoTagLabelSnapshot,
    })
    .from(visitAnswerPhotoTags)
    .where(and(eq(visitAnswerPhotoTags.isDeleted, false), inArray(visitAnswerPhotoTags.visitAnswerPhotoId, photoIds)));

  const byPhotoId = new Map<string, PhotoTagRow[]>();
  for (const row of rows) {
    const bucket = byPhotoId.get(row.visitAnswerPhotoId) ?? [];
    bucket.push(row);
    byPhotoId.set(row.visitAnswerPhotoId, bucket);
  }
  return byPhotoId;
}

async function mapPhotoRows(rows: PhotoRow[], options: { signOriginal?: boolean } = {}) {
  const tagsByPhotoId = await loadTagsByPhotoId(rows.map((row) => row.id));
  return Promise.all(
    rows.map(async (row) => {
      const signed = options.signOriginal
        ? await createSignedReadUrl({
            photoId: row.id,
            bucket: row.storageBucket,
            storagePath: row.storagePath,
            variant: "original",
          })
        : { signedUrl: null, expiresAt: new Date(Date.now() + SIGNED_URL_TTL_SECONDS * 1000).toISOString() };
      return {
        id: row.id,
        visitAnswerId: row.visitAnswerId,
        visitSessionId: row.visitSessionId,
        storageBucket: row.storageBucket,
        storagePath: row.storagePath,
        signedUrl: signed.signedUrl,
        signedUrlExpiresAt: signed.expiresAt,
        mimeType: row.mimeType,
        byteSize: row.byteSize,
        widthPx: row.widthPx,
        heightPx: row.heightPx,
        sha256: row.sha256,
        uploadedAt: toIso(row.uploadedAt),
        tags: (tagsByPhotoId.get(row.id) ?? []).map((tag) => ({
          id: tag.id,
          photoTagId: tag.photoTagId,
          label: tag.label,
        })),
        question: {
          id: row.questionId,
          visitQuestionId: row.visitQuestionId,
          text: row.questionText,
          moduleId: row.moduleId,
          moduleName: row.moduleName,
          sectionName: row.fragebogenName || row.sectionName,
        },
        campaign: {
          id: row.campaignId,
          name: row.campaignName,
          type: row.campaignType,
          startDate: row.campaignStartDate,
          endDate: row.campaignEndDate,
        },
        market: {
          id: row.marketId,
          name: row.marketName,
          standardMarketNumber: row.marketStandardMarketNumber,
          cokeMasterNumber: row.marketCokeMasterNumber,
          kuehlerStammnr: row.marketKuehlerStammnr,
          address: row.marketAddress,
          postalCode: row.marketPostalCode,
          city: row.marketCity,
          region: row.marketRegion,
          chain: row.marketChain,
        },
        gm: {
          id: row.gmUserId,
          name: `${row.gmFirstName} ${row.gmLastName}`.trim(),
        },
        visit: {
          startedAt: toIso(row.startedAt),
          submittedAt: toIso(row.submittedAt),
          durationMinutes: row.durationMinutes == null ? null : asNumber(row.durationMinutes),
        },
        comment: row.comment ?? "",
      };
    }),
  );
}

async function loadFacets() {
  const baseWhere = buildPhotoWhere({ page: 1, pageSize: 1 });
  const [campaignRows, gmRows, tagRows, regionRows, chainRows] = await Promise.all([
    db
      .selectDistinct({ id: campaigns.id, name: campaigns.name, type: campaigns.section })
      .from(visitAnswerPhotos)
      .innerJoin(visitAnswers, eq(visitAnswers.id, visitAnswerPhotos.visitAnswerId))
      .innerJoin(visitSessionQuestions, eq(visitSessionQuestions.id, visitAnswers.visitSessionQuestionId))
      .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitAnswers.visitSessionSectionId))
      .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
      .innerJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
      .innerJoin(markets, eq(markets.id, visitSessions.marketId))
      .innerJoin(users, eq(users.id, visitSessions.gmUserId))
      .where(baseWhere)
      .orderBy(campaigns.name),
    db
      .selectDistinct({ id: users.id, firstName: users.firstName, lastName: users.lastName })
      .from(visitAnswerPhotos)
      .innerJoin(visitAnswers, eq(visitAnswers.id, visitAnswerPhotos.visitAnswerId))
      .innerJoin(visitSessionQuestions, eq(visitSessionQuestions.id, visitAnswers.visitSessionQuestionId))
      .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitAnswers.visitSessionSectionId))
      .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
      .innerJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
      .innerJoin(markets, eq(markets.id, visitSessions.marketId))
      .innerJoin(users, eq(users.id, visitSessions.gmUserId))
      .where(baseWhere)
      .orderBy(users.lastName, users.firstName),
    db
      .selectDistinct({
        id: visitAnswerPhotoTags.photoTagId,
        label: visitAnswerPhotoTags.photoTagLabelSnapshot,
      })
      .from(visitAnswerPhotoTags)
      .innerJoin(visitAnswerPhotos, eq(visitAnswerPhotos.id, visitAnswerPhotoTags.visitAnswerPhotoId))
      .innerJoin(visitAnswers, eq(visitAnswers.id, visitAnswerPhotos.visitAnswerId))
      .innerJoin(visitSessionQuestions, eq(visitSessionQuestions.id, visitAnswers.visitSessionQuestionId))
      .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitAnswers.visitSessionSectionId))
      .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
      .innerJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
      .innerJoin(markets, eq(markets.id, visitSessions.marketId))
      .innerJoin(users, eq(users.id, visitSessions.gmUserId))
      .where(and(baseWhere, eq(visitAnswerPhotoTags.isDeleted, false)))
      .orderBy(visitAnswerPhotoTags.photoTagLabelSnapshot),
    db
      .selectDistinct({ region: markets.region })
      .from(visitAnswerPhotos)
      .innerJoin(visitAnswers, eq(visitAnswers.id, visitAnswerPhotos.visitAnswerId))
      .innerJoin(visitSessionQuestions, eq(visitSessionQuestions.id, visitAnswers.visitSessionQuestionId))
      .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitAnswers.visitSessionSectionId))
      .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
      .innerJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
      .innerJoin(markets, eq(markets.id, visitSessions.marketId))
      .innerJoin(users, eq(users.id, visitSessions.gmUserId))
      .where(baseWhere)
      .orderBy(markets.region),
    db
      .selectDistinct({ chain: marketChainExpression() })
      .from(visitAnswerPhotos)
      .innerJoin(visitAnswers, eq(visitAnswers.id, visitAnswerPhotos.visitAnswerId))
      .innerJoin(visitSessionQuestions, eq(visitSessionQuestions.id, visitAnswers.visitSessionQuestionId))
      .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitAnswers.visitSessionSectionId))
      .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
      .innerJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
      .innerJoin(markets, eq(markets.id, visitSessions.marketId))
      .innerJoin(users, eq(users.id, visitSessions.gmUserId))
      .where(baseWhere)
      .orderBy(marketChainExpression()),
  ]);

  return {
    campaigns: campaignRows,
    gms: gmRows.map((row) => ({ id: row.id, name: `${row.firstName} ${row.lastName}`.trim() })),
    tags: tagRows
      .filter((row) => row.label.trim().length > 0)
      .map((row) => ({ id: row.id, label: row.label })),
    regions: regionRows.map((row) => row.region).filter(Boolean),
    chains: chainRows.map((row) => row.chain).filter(Boolean),
  };
}

adminPhotosRouter.get("/photos", async (req, res, next) => {
  try {
    const parsed = photoListQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Fotoarchiv-Filter.", code: "invalid_filters" });
      return;
    }

    const filters = parsed.data;
    const where = buildPhotoWhere(filters);
    const offset = (filters.page - 1) * filters.pageSize;

    const [rows, totalRows, statsRows] = await Promise.all([
      basePhotoSelect(where)
        .orderBy(desc(visitSessions.submittedAt), desc(visitAnswerPhotos.uploadedAt))
        .limit(filters.pageSize)
        .offset(offset) as Promise<PhotoRow[]>,
      db
        .select({ value: count() })
        .from(visitAnswerPhotos)
        .innerJoin(visitAnswers, eq(visitAnswers.id, visitAnswerPhotos.visitAnswerId))
        .innerJoin(visitSessionQuestions, eq(visitSessionQuestions.id, visitAnswers.visitSessionQuestionId))
        .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitAnswers.visitSessionSectionId))
        .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
        .innerJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
        .innerJoin(markets, eq(markets.id, visitSessions.marketId))
        .innerJoin(users, eq(users.id, visitSessions.gmUserId))
        .where(where),
      db
        .select({
          visitedMarkets: sql<number>`count(distinct ${markets.id})::int`,
          campaigns: sql<number>`count(distinct ${campaigns.id})::int`,
        })
        .from(visitAnswerPhotos)
        .innerJoin(visitAnswers, eq(visitAnswers.id, visitAnswerPhotos.visitAnswerId))
        .innerJoin(visitSessionQuestions, eq(visitSessionQuestions.id, visitAnswers.visitSessionQuestionId))
        .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitAnswers.visitSessionSectionId))
        .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
        .innerJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
        .innerJoin(markets, eq(markets.id, visitSessions.marketId))
        .innerJoin(users, eq(users.id, visitSessions.gmUserId))
        .where(where),
    ]);

    res.status(200).json({
      photos: await mapPhotoRows(rows),
      total: asNumber(totalRows[0]?.value),
      page: filters.page,
      pageSize: filters.pageSize,
      stats: {
        visitedMarkets: asNumber(statsRows[0]?.visitedMarkets),
        campaigns: asNumber(statsRows[0]?.campaigns),
      },
    });
  } catch (error) {
    next(error);
  }
});

adminPhotosRouter.get("/photos/facets", async (_req, res, next) => {
  try {
    res.status(200).json({ facets: await loadFacets() });
  } catch (error) {
    next(error);
  }
});

adminPhotosRouter.post("/photos/signed-urls", async (req, res, next) => {
  try {
    const parsed = photoSignedUrlsBodySchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Foto-URL-Anfrage.", code: "invalid_photo_signed_url_request" });
      return;
    }

    const uniquePhotoIds = Array.from(new Set(parsed.data.photoIds));
    const where = and(
      buildPhotoWhere({ page: 1, pageSize: 1 }),
      inArray(visitAnswerPhotos.id, uniquePhotoIds),
    ) ?? sql`false`;
    const rows = (await basePhotoSelect(where)) as PhotoRow[];
    const urls = await Promise.all(
      rows.map(async (row) => {
        const signed = await createSignedReadUrl({
          photoId: row.id,
          bucket: row.storageBucket,
          storagePath: row.storagePath,
          variant: parsed.data.variant,
        });
        return {
          photoId: row.id,
          variant: parsed.data.variant,
          signedUrl: signed.signedUrl,
          expiresAt: signed.expiresAt,
        };
      }),
    );

    res.status(200).json({ urls });
  } catch (error) {
    next(error);
  }
});

adminPhotosRouter.get("/photos/:photoId", async (req, res, next) => {
  try {
    const photoId = String(req.params.photoId ?? "");
    if (!uuidRegex.test(photoId)) {
      res.status(400).json({ error: "Ungültige Foto-ID.", code: "invalid_photo_id" });
      return;
    }

    const where = buildPhotoWhere({ page: 1, pageSize: 1 }, photoId);
    const rows = (await basePhotoSelect(where).limit(1)) as PhotoRow[];
    if (!rows[0]) {
      res.status(404).json({ error: "Foto nicht gefunden.", code: "photo_not_found" });
      return;
    }

    const [photo] = await mapPhotoRows(rows, { signOriginal: true });
    res.status(200).json({ photo });
  } catch (error) {
    next(error);
  }
});

export { adminPhotosRouter };
