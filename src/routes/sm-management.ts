import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";
import { and, asc, desc, eq, gte, inArray, lte, sql } from "drizzle-orm";
import { Router, type Request, type Response, type NextFunction } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import { supabaseAdmin } from "../lib/supabase.js";
import { env } from "../config/env.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { smQuestionnaireSubmissions as submissions, smAssignments, smQuestionAnswers as answers,
  smQuestionAnswerFiles as files, smQuestionAnswerEvents as events, users } from "../lib/schema.js";
import { isIsoDate, isoDateToEpochDay } from "../sm-planning.shared.js";
import { SmVisitAnswerValidationError } from "../sm-visit.shared.js";
import { applySmAdminCorrection, loadSmManagementState, smAdminCorrectionSchema, smManagementDetail, smManagementHash,
  SmManagementError, type SmAdminPhotoReceipt, type SmManagementTx, type SmVerifiedAdminPhoto } from "../sm-management.js";

const uuid = z.string().uuid();
const date = z.string().refine(isIsoDate);
export const smManagementListSchema = z.object({
  from: date, to: date, smUserId: uuid.optional(), marketId: uuid.optional(), questionnaireId: uuid.optional(),
  search: z.string().trim().max(200).optional(), limit: z.coerce.number().int().min(1).max(100).default(40),
  cursorDate: date.optional(), cursorId: uuid.optional(),
}).strict().refine(input => isoDateToEpochDay(input.to) >= isoDateToEpochDay(input.from) && isoDateToEpochDay(input.to) - isoDateToEpochDay(input.from) < 93, "Zeitraum maximal 93 Tage.")
  .refine(input => Boolean(input.cursorDate) === Boolean(input.cursorId), "Ungültiger Seitencursor.");

const workDate = sql<string>`coalesce((${submissions.visitStartedAt} at time zone 'Europe/Vienna')::date,
  ${smAssignments.replacementWorkDate}, ${smAssignments.originalWorkDate}, (${submissions.submittedAt} at time zone 'Europe/Vienna')::date)`;

export async function listSmManagedVisits(tx: SmManagementTx, input: z.infer<typeof smManagementListSchema>) {
  const scope = [eq(submissions.isDeleted, false), eq(submissions.isCurrent, true), eq(submissions.status, "submitted"),
    gte(workDate, input.from), lte(workDate, input.to)];
  const filters = [...scope];
  if (input.smUserId) filters.push(eq(submissions.smUserId, input.smUserId));
  if (input.marketId) filters.push(eq(submissions.smMarketId, input.marketId));
  if (input.questionnaireId) filters.push(eq(submissions.questionnaireTemplateId, input.questionnaireId));
  if (input.search) filters.push(sql`strpos(lower(${submissions.smNameSnapshot} || ' ' || ${submissions.marketNameSnapshot} || ' ' || ${submissions.marketAddressSnapshot} || ' ' || ${submissions.questionnaireNameSnapshot}), lower(${input.search})) > 0`);
  if (input.cursorDate && input.cursorId) filters.push(sql`(${workDate}, ${submissions.id}) < (${input.cursorDate}::date, ${input.cursorId}::uuid)`);
  const rows = await tx.select({ id: submissions.id, assignmentId: submissions.assignmentId, workDate: sql<string>`${workDate}::text`,
    smUserId: submissions.smUserId, smName: submissions.smNameSnapshot, marketId: submissions.smMarketId,
    marketName: submissions.marketNameSnapshot, address: submissions.marketAddressSnapshot,
    questionnaireId: submissions.questionnaireTemplateId, questionnaireName: submissions.questionnaireNameSnapshot,
    questionnaireVersion: submissions.questionnaireVersionSnapshot, startedAt: submissions.visitStartedAt,
    completedAt: submissions.visitCompletedAt, submittedAt: submissions.submittedAt, answeredCount: submissions.answeredQuestionCount,
  }).from(submissions).leftJoin(smAssignments, eq(smAssignments.id, submissions.assignmentId)).where(and(...filters))
    .orderBy(desc(workDate), desc(submissions.id)).limit(input.limit + 1);
  const facets = await tx.selectDistinct({ smUserId: submissions.smUserId, smName: submissions.smNameSnapshot,
    marketId: submissions.smMarketId, marketName: submissions.marketNameSnapshot,
    questionnaireId: submissions.questionnaireTemplateId, questionnaireName: submissions.questionnaireNameSnapshot,
  }).from(submissions).leftJoin(smAssignments, eq(smAssignments.id, submissions.assignmentId)).where(and(...scope))
    .orderBy(asc(submissions.smNameSnapshot), asc(submissions.marketNameSnapshot), asc(submissions.questionnaireNameSnapshot)).limit(2001);
  const items = rows.slice(0, input.limit), last = items.at(-1);
  return { visits: items, nextCursor: rows.length > input.limit && last ? { date: last.workDate, id: last.id } : null,
    facets: facets.slice(0, 2000), facetsTruncated: facets.length > 2000 };
}

const bucket = "sm-visit-photos";
// Bound external storage waits, especially while a correction holds the submission lock.
async function storageWithin<T>(operation: PromiseLike<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([Promise.resolve(operation), new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new SmManagementError(503, "sm_management_storage_timeout", "Der Fotodienst antwortet nicht. Bitte versuche es erneut.")), 8_000);
    })]);
  } finally { if (timer) clearTimeout(timer); }
}
const photoInput = z.object({ questionId: uuid, originalFileName: z.string().trim().min(1).max(255),
  mimeType: z.enum(["image/jpeg", "image/png", "image/webp"]), byteSize: z.number().int().min(1).max(20 * 1024 * 1024) }).strict();

function photoProof(actor: string, submissionId: string, receipt: Omit<SmAdminPhotoReceipt, "proof">): string {
  return createHmac("sha256", env.JWT_SECRET).update(`sm-admin-photo-v1:${actor}:${submissionId}:${smManagementHash(receipt)}`).digest("hex");
}
async function verifyPhotos(actor: string, submissionId: string, receipts: SmAdminPhotoReceipt[]): Promise<SmVerifiedAdminPhoto[]> {
  return Promise.all(receipts.map(async receipt => {
    const { proof, ...unsigned } = receipt;
    const expected = photoProof(actor, submissionId, unsigned);
    if (!timingSafeEqual(Buffer.from(proof, "hex"), Buffer.from(expected, "hex")) || receipt.expiresAt < Date.now()) {
      throw new SmManagementError(400, "sm_management_photo_receipt_invalid", "Ein Foto-Upload ist ungültig oder abgelaufen. Bitte lade das Foto erneut hoch.");
    }
    const { data, error } = await storageWithin(supabaseAdmin.storage.from(bucket).info(receipt.storagePath));
    if (error || !data || data.size !== receipt.byteSize || data.contentType !== receipt.mimeType) {
      throw new SmManagementError(409, "sm_management_photo_missing", "Ein Foto wurde noch nicht vollständig oder mit abweichendem Format hochgeladen.");
    }
    return { id: receipt.id, questionId: receipt.questionId, storageBucket: bucket, storagePath: receipt.storagePath,
      originalFileName: receipt.originalFileName, mimeType: receipt.mimeType, byteSize: receipt.byteSize, uploadedAt: new Date() };
  }));
}

async function signPhotos<T extends { storageBucket: string; storagePath: string }>(photos: T[]) {
  return Promise.all(photos.map(async photo => {
    const { storageBucket, storagePath, ...metadata } = photo;
    try {
      const { data, error } = await storageWithin(supabaseAdmin.storage.from(storageBucket).createSignedUrl(storagePath, 30 * 60));
      return { ...metadata, signedUrl: error ? null : data?.signedUrl ?? null };
    } catch { return { ...metadata, signedUrl: null }; }
  }));
}

export const adminSmManagementRouter = Router();
adminSmManagementRouter.use(requireAuth(["admin", "sm_admin"]));
adminSmManagementRouter.use((_req, res, next) => { res.setHeader("Cache-Control", "private, no-store"); next(); });
const handle = (action: (req: AuthedRequest, res: Response) => Promise<unknown>) => (req: Request, res: Response, next: NextFunction) => {
  void action(req as AuthedRequest, res).catch(error => {
    if (error instanceof z.ZodError) { res.status(400).json({ code: "sm_management_input_invalid", error: "Bitte prüfe die Eingaben.", details: { issues: error.issues } }); return; }
    if (error instanceof SmVisitAnswerValidationError) { res.status(400).json({ code: "sm_management_answer_invalid", error: error.message }); return; }
    if (error instanceof SmManagementError) { res.status(error.status).json({ code: error.code, error: error.message, details: error.details }); return; }
    next(error);
  });
};

adminSmManagementRouter.get("/", handle(async (req, res) => {
  const input = smManagementListSchema.parse(req.query);
  res.json(await db.transaction(tx => listSmManagedVisits(tx, input)));
}));
adminSmManagementRouter.get("/:submissionId", handle(async (req, res) => {
  const result = await db.transaction(tx => smManagementDetail(tx, uuid.parse(req.params.submissionId)));
  res.json({ ...result, sections: await Promise.all(result.sections.map(async section => ({ ...section,
    questions: await Promise.all(section.questions.map(async question => ({ ...question, photos: await signPhotos(question.photos) }))),
  }))) });
}));
adminSmManagementRouter.get("/:submissionId/history", handle(async (req, res) => {
  const id = uuid.parse(req.params.submissionId);
  const input = z.object({ cursorVersion: z.coerce.number().int().positive().optional(), questionId: uuid }).strict().parse(req.query);
  const result = await db.transaction(async tx => {
    const state = await loadSmManagementState(tx, id);
    if (!state.questions.some(question => question.id === input.questionId)) throw new SmManagementError(404, "sm_management_question_not_found", "Frage nicht gefunden.");
    const rows = await tx.select({ answer: answers, actorFirstName: users.firstName, actorLastName: users.lastName }).from(answers)
      .leftJoin(users, eq(users.id, answers.answeredByUserId)).where(and(eq(answers.submissionId, id), eq(answers.submissionQuestionId, input.questionId), eq(answers.isDeleted, false),
        ...(input.cursorVersion ? [sql`${answers.answerVersion} < ${input.cursorVersion}`] : [])))
      .orderBy(desc(answers.answerVersion)).limit(21);
    const page = rows.slice(0, 20), ids = page.map(row => row.answer.id);
    const photos = ids.length ? await tx.select().from(files).where(and(inArray(files.answerId, ids), eq(files.isDeleted, false))) : [];
    const audit = ids.length ? await tx.select({ answerId: events.answerId, payload: events.payload, createdAt: events.createdAt }).from(events)
      .where(and(eq(events.submissionId, id), inArray(events.answerId, ids), eq(events.eventType, "correction"), sql`${events.payload}->>'source' = 'sm_admin_correction'`)).orderBy(desc(events.createdAt)) : [];
    return { entries: page.map(row => ({ id: row.answer.id, version: row.answer.answerVersion, current: row.answer.isCurrent,
      state: row.answer.answerState, value: row.answer.valueJson, at: row.answer.answeredAt?.toISOString() ?? row.answer.createdAt.toISOString(),
      actor: [row.actorFirstName, row.actorLastName].filter(Boolean).join(" ") || "System",
      reason: audit.find(event => event.answerId === row.answer.id)?.payload.reason ?? null,
      photos: photos.filter(photo => photo.answerId === row.answer.id).map(photo => ({ id: photo.id, fileName: photo.originalFileName, storageBucket: photo.storageBucket, storagePath: photo.storagePath })),
    })), nextCursor: rows.length > 20 ? page.at(-1)!.answer.answerVersion : null };
  });
  res.json({ ...result, entries: await Promise.all(result.entries.map(async entry => ({ ...entry, photos: await signPhotos(entry.photos) }))) });
}));
adminSmManagementRouter.post("/:submissionId/corrections", handle(async (req, res) => {
  const id = uuid.parse(req.params.submissionId), input = smAdminCorrectionSchema.parse(req.body), actor = req.authUser!.appUserId;
  res.json(await db.transaction(tx => applySmAdminCorrection(tx, id, input, actor, uploads => verifyPhotos(actor, id, uploads))));
}));
adminSmManagementRouter.post("/:submissionId/photos/upload-url", handle(async (req, res) => {
  const id = uuid.parse(req.params.submissionId), input = photoInput.parse(req.body), actor = req.authUser!.appUserId;
  await db.transaction(async tx => {
    const state = await loadSmManagementState(tx, id);
    if (!state.questions.some(question => question.id === input.questionId && question.questionTypeSnapshot === "photo")) {
      throw new SmManagementError(400, "sm_management_photo_question_invalid", "Diese Foto-Frage gehört nicht zum Fragebogen.");
    }
  });
  const uploadId = randomUUID(), extension = input.mimeType === "image/jpeg" ? "jpg" : input.mimeType === "image/png" ? "png" : "webp";
  const unsigned = { ...input, id: uploadId, expiresAt: Date.now() + 2 * 60 * 60 * 1000,
    storagePath: `sm-admin-corrections/${actor}/${id}/${input.questionId}/${uploadId}.${extension}` };
  const { data, error } = await storageWithin(supabaseAdmin.storage.from(bucket).createSignedUploadUrl(unsigned.storagePath, { upsert: false }));
  if (error || !data) throw new SmManagementError(502, "sm_management_upload_unavailable", "Foto-Upload konnte nicht vorbereitet werden.");
  res.json({ receipt: { ...unsigned, proof: photoProof(actor, id, unsigned) }, upload: { bucket, path: data.path, token: data.token, signedUrl: data.signedUrl } });
}));
