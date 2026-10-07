import { and, asc, desc, eq, gte, inArray, lte, sql, type SQL } from "drizzle-orm";
import { Router, type Request, type Response, type NextFunction } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import { supabaseAdmin } from "../lib/supabase.js";
import { requireAuth } from "../middleware/auth.js";
import { smAssignments, smQuestionAnswerFiles as files, smQuestionAnswers as answers,
  smQuestionnaireSubmissions as submissions, smQuestionnaireSubmissionQuestions as questions,
  smQuestionnaireSubmissionSections as sections, smQuestionnaireTemplates as templates } from "../lib/schema.js";
import { isIsoDate } from "../sm-planning.shared.js";
import type { SmManagementTx } from "../sm-management.js";

const date = z.string().refine(isIsoDate), uuid = z.string().uuid();
export const smPhotoArchiveFiltersSchema = z.object({
  from: date.optional(), to: date.optional(), smUserId: uuid.optional(), marketId: uuid.optional(),
  questionnaireId: uuid.optional(), SMDurcharbeitCatalogScope: z.enum(["standard", "SMDurcharbeit"]).optional(),
  search: z.string().trim().max(200).optional(),
}).strict().refine(input => !input.from || !input.to || input.from <= input.to, "Das Ende muss nach dem Beginn liegen.");
export const smPhotoArchiveQuerySchema = smPhotoArchiveFiltersSchema.safeExtend({
  page: z.coerce.number().int().min(1).max(100_000).default(1),
  pageSize: z.coerce.number().int().min(1).max(60).default(30),
});
export type SmPhotoArchiveFilters = z.infer<typeof smPhotoArchiveFiltersSchema>;

// Read the frozen visit/question snapshots. Catalog edits, inactive staff and archived
// markets must never remove historical photos. Only current, submitted answers appear.
function photoSource(tx: SmManagementTx) {
  const workDate = sql<string>`coalesce((${submissions.visitStartedAt} at time zone 'Europe/Vienna')::date,
    ${smAssignments.replacementWorkDate}, ${smAssignments.originalWorkDate}, (${submissions.submittedAt} at time zone 'Europe/Vienna')::date)::text`;
  return tx.$with("sm_archive_photos").as(tx.select({
    id: sql<string>`${files.id}`.as("id"), submissionId: sql<string>`${submissions.id}`.as("submission_id"), assignmentId: submissions.assignmentId,
    questionId: sql<string>`${questions.id}`.as("question_id"), questionText: questions.questionTextSnapshot, moduleName: sections.moduleNameSnapshot,
    fileName: files.originalFileName, mimeType: files.mimeType, byteSize: files.byteSize,
    widthPx: files.widthPx, heightPx: files.heightPx, uploadedAt: files.uploadedAt,
    storageBucket: files.storageBucket, storagePath: files.storagePath,
    workDate: workDate.as("work_date"), smUserId: submissions.smUserId, smName: submissions.smNameSnapshot,
    marketId: submissions.smMarketId, marketName: submissions.marketNameSnapshot,
    address: submissions.marketAddressSnapshot, postalCode: submissions.marketPostalCodeSnapshot, city: submissions.marketCitySnapshot,
    questionnaireId: submissions.questionnaireTemplateId, questionnaireName: submissions.questionnaireNameSnapshot,
    questionnaireVersion: submissions.questionnaireVersionSnapshot,
    SMDurcharbeitCatalogScope: sql<"standard" | "SMDurcharbeit">`case when starts_with(${templates.stableCode}, 'smdurcharbeit_') then 'SMDurcharbeit' else 'standard' end`.as("catalog_scope"),
  }).from(files).innerJoin(answers, eq(answers.id, files.answerId))
    .innerJoin(questions, and(eq(questions.id, answers.submissionQuestionId), eq(questions.submissionId, answers.submissionId)))
    .innerJoin(sections, eq(sections.id, questions.submissionSectionId))
    .innerJoin(submissions, eq(submissions.id, answers.submissionId))
    .leftJoin(smAssignments, eq(smAssignments.id, submissions.assignmentId))
    .leftJoin(templates, eq(templates.id, submissions.questionnaireTemplateId))
    .where(and(eq(files.isDeleted, false), eq(answers.isDeleted, false), eq(answers.isCurrent, true), eq(answers.answerState, "answered"),
      eq(questions.isDeleted, false), eq(questions.isApplicable, true), eq(questions.questionTypeSnapshot, "photo"), eq(sections.isDeleted, false),
      eq(submissions.isDeleted, false), eq(submissions.isCurrent, true), eq(submissions.status, "submitted"),
      sql`${answers.valueJson}->>'kind' = 'photo'`,
      sql`(case when jsonb_typeof(${answers.valueJson}->'fileIds') = 'array' then ${answers.valueJson}->'fileIds' else '[]'::jsonb end) ? ${files.id}::text`,
    )));
}

function filtersFor(source: ReturnType<typeof photoSource>, input: SmPhotoArchiveFilters): SQL[] {
  const filters: SQL[] = [];
  if (input.from) filters.push(gte(source.workDate, input.from));
  if (input.to) filters.push(lte(source.workDate, input.to));
  if (input.smUserId) filters.push(eq(source.smUserId, input.smUserId));
  if (input.marketId) filters.push(eq(source.marketId, input.marketId));
  if (input.questionnaireId) filters.push(eq(source.questionnaireId, input.questionnaireId));
  if (input.SMDurcharbeitCatalogScope) filters.push(eq(source.SMDurcharbeitCatalogScope, input.SMDurcharbeitCatalogScope));
  if (input.search) filters.push(sql`strpos(lower(concat_ws(' ', ${source.smName}, ${source.marketName}, ${source.address}, ${source.city}, ${source.questionnaireName}, ${source.questionText}, ${source.fileName})), lower(${input.search})) > 0`);
  return filters;
}

function publicPhoto<T extends { storageBucket: string; storagePath: string }>(photo: T) {
  const { storageBucket: _bucket, storagePath: _path, ...metadata } = photo;
  return metadata;
}

export async function listSmArchivePhotos(tx: SmManagementTx, input: z.infer<typeof smPhotoArchiveQuerySchema>) {
  const source = photoSource(tx), where = and(...filtersFor(source, input));
  const photos = await tx.with(source).select().from(source).where(where)
    .orderBy(desc(source.workDate), desc(source.uploadedAt), desc(source.id)).limit(input.pageSize).offset((input.page - 1) * input.pageSize);
  const [stats] = await tx.with(source).select({ total: sql<number>`count(*)::int`, markets: sql<number>`count(distinct ${source.marketId})::int`,
    questionnaires: sql<number>`count(distinct ${source.questionnaireId})::int` }).from(source).where(where);
  return { photos: photos.map(publicPhoto), total: stats!.total, stats: { markets: stats!.markets, questionnaires: stats!.questionnaires }, page: input.page, pageSize: input.pageSize };
}

export async function smArchivePhotoFacets(tx: SmManagementTx, input: SmPhotoArchiveFilters) {
  const source = photoSource(tx);
  // Keep choices available when another filter is selected; scope/date narrow the catalog.
  const where = and(...filtersFor(source, { from: input.from, to: input.to, SMDurcharbeitCatalogScope: input.SMDurcharbeitCatalogScope }));
  const rows = await tx.with(source).selectDistinct({ smUserId: source.smUserId, smName: source.smName, marketId: source.marketId,
    marketName: source.marketName, questionnaireId: source.questionnaireId, questionnaireName: source.questionnaireName }).from(source).where(where)
    .orderBy(asc(source.smName), asc(source.marketName), asc(source.questionnaireName)).limit(2001);
  return { facets: rows.slice(0, 2000), truncated: rows.length > 2000 };
}

export async function findSmArchivePhotoFiles(tx: SmManagementTx, ids: string[]) {
  const source = photoSource(tx);
  return tx.with(source).select({ id: source.id, storageBucket: source.storageBucket, storagePath: source.storagePath }).from(source).where(inArray(source.id, ids));
}

const URL_TTL_SECONDS = 600;
async function signPhotoFiles(photos: Awaited<ReturnType<typeof findSmArchivePhotoFiles>>) {
  const buckets = new Map<string, typeof photos>();
  for (const photo of photos) buckets.set(photo.storageBucket, [...(buckets.get(photo.storageBucket) ?? []), photo]);
  const expiresAt = new Date(Date.now() + URL_TTL_SECONDS * 1000).toISOString();
  return (await Promise.all(Array.from(buckets, async ([bucket, items]) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const { data, error } = await Promise.race([
        supabaseAdmin.storage.from(bucket).createSignedUrls(items.map(item => item.storagePath), URL_TTL_SECONDS),
        new Promise<never>((_resolve, reject) => { timer = setTimeout(() => reject(new Error("Storage timeout")), 8000); }),
      ]);
      const urls = new Map((error ? [] : data ?? []).map(item => [item.path, item.signedUrl]));
      return items.map(item => ({ id: item.id, signedUrl: urls.get(item.storagePath) || null, expiresAt }));
    } catch { return items.map(item => ({ id: item.id, signedUrl: null, expiresAt })); }
    finally { if (timer) clearTimeout(timer); }
  }))).flat();
}

export const adminSmPhotoArchiveRouter = Router();
adminSmPhotoArchiveRouter.use(requireAuth(["admin", "sm_admin"]));
adminSmPhotoArchiveRouter.use((_req, res, next) => { res.setHeader("Cache-Control", "private, no-store"); next(); });
const handle = (action: (req: Request, res: Response) => Promise<unknown>) => (req: Request, res: Response, next: NextFunction) => {
  void action(req, res).catch(error => {
    if (error instanceof z.ZodError) { res.status(400).json({ code: "sm_photo_archive_input_invalid", error: "Bitte prüfe die Fotofilter." }); return; }
    next(error);
  });
};
adminSmPhotoArchiveRouter.get("/", handle(async (req, res) => {
  const input = smPhotoArchiveQuerySchema.parse(req.query);
  res.json(await db.transaction(tx => listSmArchivePhotos(tx, input)));
}));
adminSmPhotoArchiveRouter.get("/facets", handle(async (req, res) => {
  const input = smPhotoArchiveFiltersSchema.parse(req.query);
  res.json(await db.transaction(tx => smArchivePhotoFacets(tx, input)));
}));
adminSmPhotoArchiveRouter.post("/signed-urls", handle(async (req, res) => {
  const input = z.object({ ids: z.array(uuid).min(1).max(60) }).strict().parse(req.body);
  const photos = await db.transaction(tx => findSmArchivePhotoFiles(tx, [...new Set(input.ids)]));
  res.json({ photos: await signPhotoFiles(photos) });
}));
adminSmPhotoArchiveRouter.get("/export", handle(async (req, res) => {
  const input = smPhotoArchiveFiltersSchema.parse(req.query);
  const source = photoSource(db as unknown as SmManagementTx);
  const photos = await db.with(source).select().from(source).where(and(...filtersFor(source, input)))
    .orderBy(desc(source.workDate), desc(source.uploadedAt), desc(source.id)).limit(251);
  if (photos.length > 250 || photos.reduce((sum, photo) => sum + (photo.byteSize ?? 20 * 1024 * 1024), 0) > 150 * 1024 * 1024) {
    res.status(400).json({ code: "sm_photo_archive_export_too_large", error: "Bitte grenze den Export auf maximal 250 Fotos und 150 MB ein." }); return;
  }
  res.json({ photos: photos.map(publicPhoto) });
}));
