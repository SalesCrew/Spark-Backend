import crypto from "node:crypto";
import { and, desc, eq } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { logAction, startActionTimer } from "../lib/logger.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { db } from "../lib/db.js";
import { ensureStartedDaySession } from "../lib/day-session.js";
import { supabaseAdmin } from "../lib/supabase.js";
import {
  respondTimelineValidationError,
  validateGmTimelineInterval,
  validateGmTimelineStartPoint,
} from "../lib/zeiterfassung-validation.js";
import {
  timeTrackingEntries,
  timeTrackingEntryEvents,
  type timeTrackingActivityTypeEnum,
} from "../lib/schema.js";

const timeTrackingRouter = Router();
timeTrackingRouter.use(requireAuth(["gm", "admin"]));
timeTrackingRouter.use((req, res, next) => {
  if (req.method.toUpperCase() === "GET") {
    next();
    return;
  }
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "time_tracking_action_completed", {
      req,
      action: "time_tracking_mutation",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

const uuidSchema = z.string().uuid();
const isoDateTimeSchema = z.string().datetime();
const DOCTOR_CONFIRMATION_BUCKET = "visit-photos";
const DOCTOR_CONFIRMATION_READ_TTL_SECONDS = 30 * 60;
const DOCTOR_CONFIRMATION_MAX_BYTES = 10 * 1024 * 1024;
const DOCTOR_CONFIRMATION_MIME_BY_EXT: Record<string, string> = {
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  png: "image/png",
  webp: "image/webp",
  heic: "image/heic",
  heif: "image/heif",
};

const activityTypeSchema = z.enum([
  "sonderaufgabe",
  "arztbesuch",
  "werkstatt",
  "homeoffice",
  "schulung",
  "lager",
  "hotel",
  "hoteluebernachtung",
]);

const startDraftSchema = z
  .object({
    activityType: activityTypeSchema,
    startAt: isoDateTimeSchema,
    endAt: isoDateTimeSchema.optional(),
    marketId: uuidSchema.optional().nullable(),
    clientEntryToken: z.string().trim().min(8).max(120).optional(),
  })
  .strict();

const endDraftSchema = z
  .object({
    endAt: isoDateTimeSchema,
  })
  .strict();

const commentDraftSchema = z
  .object({
    comment: z.string().max(2_000).nullable().optional(),
  })
  .strict();

const activeDraftQuerySchema = z
  .object({
    gmUserId: uuidSchema.optional(),
  })
  .strict();

const doctorConfirmationPresignSchema = z
  .object({
    extension: z.string().max(12).optional(),
    mimeType: z.string().max(80).optional(),
    fileName: z.string().trim().max(240).optional(),
  })
  .strict();

const doctorConfirmationCommitSchema = z
  .object({
    storageBucket: z.literal(DOCTOR_CONFIRMATION_BUCKET),
    storagePath: z.string().min(1),
    fileName: z.string().trim().max(240).optional(),
    mimeType: z.string().max(80).optional(),
    byteSize: z.number().int().min(1).max(DOCTOR_CONFIRMATION_MAX_BYTES).optional(),
  })
  .strict();

type CanonicalActivityType = (typeof timeTrackingActivityTypeEnum.enumValues)[number];

const activityTypeMap: Record<z.infer<typeof activityTypeSchema>, CanonicalActivityType> = {
  sonderaufgabe: "sonderaufgabe",
  arztbesuch: "arztbesuch",
  werkstatt: "werkstatt",
  homeoffice: "homeoffice",
  schulung: "schulung",
  lager: "lager",
  hotel: "hoteluebernachtung",
  hoteluebernachtung: "hoteluebernachtung",
};

function normalizeActivityType(value: z.infer<typeof activityTypeSchema>): CanonicalActivityType {
  return activityTypeMap[value];
}

function parseIso(value: string): Date {
  return new Date(value);
}

function toDurationMinutes(startAt: Date | null, endAt: Date | null): number | null {
  if (!startAt || !endAt) return null;
  const diff = endAt.getTime() - startAt.getTime();
  return diff >= 0 ? Math.floor(diff / 60_000) : null;
}

function serializeEntry(row: typeof timeTrackingEntries.$inferSelect) {
  return {
    id: row.id,
    gmUserId: row.gmUserId,
    marketId: row.marketId,
    activityType: row.activityType,
    clientEntryToken: row.clientEntryToken,
    startAt: row.startAt?.toISOString() ?? null,
    endAt: row.endAt?.toISOString() ?? null,
    comment: row.comment,
    status: row.status,
    submittedAt: row.submittedAt?.toISOString() ?? null,
    cancelledAt: row.cancelledAt?.toISOString() ?? null,
    durationMin: toDurationMinutes(row.startAt ?? null, row.endAt ?? null),
    doctorConfirmation: row.activityType === "arztbesuch"
      ? {
          isRequired: true,
          isUploaded: Boolean(row.doctorConfirmationPath),
          uploadedAt: row.doctorConfirmationUploadedAt?.toISOString() ?? null,
          fileName: row.doctorConfirmationFileName ?? null,
        }
      : null,
  };
}

function canAccessEntry(req: AuthedRequest, entryGmUserId: string): boolean {
  if (req.authUser?.role === "admin") return true;
  return req.authUser?.appUserId === entryGmUserId;
}

async function appendEntryEvent(input: {
  entryId: string;
  gmUserId: string | null;
  eventType: (typeof timeTrackingEntryEvents.$inferInsert)["eventType"];
  payload: Record<string, unknown>;
}) {
  await db.insert(timeTrackingEntryEvents).values({
    entryId: input.entryId,
    gmUserId: input.gmUserId,
    eventType: input.eventType,
    payload: input.payload,
  });
}

function cleanStoragePath(path: string): string {
  return path.replace(/^\/+/, "").replace(/\\/g, "/").trim();
}

function normalizeDoctorConfirmationExt(input?: { extension?: string | undefined; mimeType?: string | undefined }): string {
  const rawExt = (input?.extension ?? "").trim().toLowerCase().replace(/^\./, "");
  if (rawExt && DOCTOR_CONFIRMATION_MIME_BY_EXT[rawExt]) return rawExt === "jpeg" ? "jpg" : rawExt;
  const mime = (input?.mimeType ?? "").trim().toLowerCase();
  const match = Object.entries(DOCTOR_CONFIRMATION_MIME_BY_EXT).find(([, value]) => value === mime);
  if (match) return match[0] === "jpeg" ? "jpg" : match[0];
  return "jpg";
}

function isAllowedDoctorConfirmationMime(mimeType: string | null | undefined): boolean {
  if (!mimeType) return true;
  return Object.values(DOCTOR_CONFIRMATION_MIME_BY_EXT).includes(mimeType.toLowerCase());
}

function safeDoctorConfirmationFileName(value: string | null | undefined): string | null {
  const trimmed = value?.trim();
  if (!trimmed) return null;
  return trimmed.replace(/[<>:"/\\|?*\x00-\x1F]/g, "_").slice(0, 180);
}

async function verifyStorageObjectExists(bucket: string, storagePath: string): Promise<boolean> {
  const normalized = cleanStoragePath(storagePath);
  const lastSlash = normalized.lastIndexOf("/");
  const folder = lastSlash >= 0 ? normalized.slice(0, lastSlash) : "";
  const fileName = lastSlash >= 0 ? normalized.slice(lastSlash + 1) : normalized;
  const { data, error } = await supabaseAdmin.storage.from(bucket).list(folder, { search: fileName, limit: 10 });
  if (error) return false;
  return (data ?? []).some((entry) => entry.name === fileName);
}

timeTrackingRouter.post("/entries/draft/start", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = startDraftSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Startdaten für Zusatzzeiterfassung." });
      return;
    }
    const authUserId = req.authUser?.appUserId;
    if (!authUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const dayGuard = await ensureStartedDaySession({ gmUserId: authUserId });
    if (!dayGuard.ok) {
      res.status(409).json({
        error:
          dayGuard.reason === "stale_day_open"
            ? "Bitte zuerst den offenen Arbeitstag vom Vortag abschließen."
            : "Bitte zuerst den Arbeitstag starten.",
        code: dayGuard.reason,
      });
      return;
    }
    const activityType = normalizeActivityType(parsed.data.activityType);
    const startAt = parseIso(parsed.data.startAt);
    const now = new Date();

    let entry: typeof timeTrackingEntries.$inferSelect | undefined;
    if (parsed.data.clientEntryToken) {
      [entry] = await db
        .select()
        .from(timeTrackingEntries)
        .where(
          and(
            eq(timeTrackingEntries.gmUserId, authUserId),
            eq(timeTrackingEntries.activityType, activityType),
            eq(timeTrackingEntries.status, "draft"),
            eq(timeTrackingEntries.isDeleted, false),
            eq(timeTrackingEntries.clientEntryToken, parsed.data.clientEntryToken),
          ),
        )
        .limit(1);
    }
    if (!entry) {
      [entry] = await db
        .select()
        .from(timeTrackingEntries)
        .where(
          and(
            eq(timeTrackingEntries.gmUserId, authUserId),
            eq(timeTrackingEntries.activityType, activityType),
            eq(timeTrackingEntries.status, "draft"),
            eq(timeTrackingEntries.isDeleted, false),
          ),
        )
        .orderBy(desc(timeTrackingEntries.updatedAt))
        .limit(1);
    }

    const validationId = entry?.id ?? parsed.data.clientEntryToken ?? "new-time-tracking-draft";
    if (parsed.data.endAt) {
      const endAt = parseIso(parsed.data.endAt);
      await validateGmTimelineInterval({
        gmUserId: authUserId,
        kind: "zusatzzeit",
        id: validationId,
        startAt,
        endAt,
        now,
      });
    } else {
      await validateGmTimelineStartPoint({
        gmUserId: authUserId,
        kind: "zusatzzeit",
        id: validationId,
        startAt,
        now,
      });
    }

    if (entry) {
      const [updated] = await db
        .update(timeTrackingEntries)
        .set({
          startAt,
          endAt: parsed.data.endAt ? parseIso(parsed.data.endAt) : null,
          marketId: parsed.data.marketId ?? null,
          updatedAt: now,
        })
        .where(eq(timeTrackingEntries.id, entry.id))
        .returning();
      entry = updated;
    } else {
      const [created] = await db
        .insert(timeTrackingEntries)
        .values({
          gmUserId: authUserId,
          activityType,
          marketId: parsed.data.marketId ?? null,
          clientEntryToken: parsed.data.clientEntryToken,
          startAt,
          status: "draft",
        })
        .returning();
      entry = created;
    }
    if (!entry) {
      throw new Error("Entwurf konnte nicht erstellt oder aktualisiert werden.");
    }

    await appendEntryEvent({
      entryId: entry.id,
      gmUserId: authUserId,
      eventType: "start_set",
      payload: {
        startAt: parsed.data.startAt,
        marketId: parsed.data.marketId ?? null,
        clientEntryToken: parsed.data.clientEntryToken ?? null,
      },
    });

    res.status(200).json({
      entry: serializeEntry(entry),
      offlineContract: {
        clientEntryTokenRequiredForIdempotency: true,
        retryPolicy: "Reuse the same clientEntryToken to upsert the same draft.",
      },
    });
  } catch (error) {
    if (respondTimelineValidationError(res, error)) return;
    next(error);
  }
});

timeTrackingRouter.patch("/entries/:id/draft/end", async (req: AuthedRequest, res, next) => {
  try {
    const idParse = uuidSchema.safeParse(req.params.id);
    const parsed = endDraftSchema.safeParse(req.body ?? {});
    if (!idParse.success || !parsed.success) {
      res.status(400).json({ error: "Ungültige Enddaten für Zusatzzeiterfassung." });
      return;
    }
    const [entry] = await db
      .select()
      .from(timeTrackingEntries)
      .where(and(eq(timeTrackingEntries.id, idParse.data), eq(timeTrackingEntries.isDeleted, false)))
      .limit(1);
    if (!entry) {
      res.status(404).json({ error: "Eintrag nicht gefunden." });
      return;
    }
    if (!canAccessEntry(req, entry.gmUserId)) {
      res.status(403).json({ error: "Keine Berechtigung für diesen Eintrag." });
      return;
    }
    if (entry.status !== "draft") {
      res.status(409).json({ error: "Endzeit kann nur für Entwürfe gesetzt werden." });
      return;
    }
    const endAt = parseIso(parsed.data.endAt);
    if (entry.startAt && endAt.getTime() < entry.startAt.getTime()) {
      res.status(400).json({ error: "Endzeit darf nicht vor Startzeit liegen." });
      return;
    }
    const [updated] = await db
      .update(timeTrackingEntries)
      .set({
        endAt,
        updatedAt: new Date(),
      })
      .where(eq(timeTrackingEntries.id, entry.id))
      .returning();
    if (!updated) {
      throw new Error("Endzeit konnte nicht gespeichert werden.");
    }
    await appendEntryEvent({
      entryId: entry.id,
      gmUserId: req.authUser?.appUserId ?? null,
      eventType: "end_set",
      payload: { endAt: parsed.data.endAt },
    });
    res.status(200).json({ entry: serializeEntry(updated) });
  } catch (error) {
    next(error);
  }
});

timeTrackingRouter.patch("/entries/:id/draft/comment", async (req: AuthedRequest, res, next) => {
  try {
    const idParse = uuidSchema.safeParse(req.params.id);
    const parsed = commentDraftSchema.safeParse(req.body ?? {});
    if (!idParse.success || !parsed.success) {
      res.status(400).json({ error: "Ungültiger Kommentar." });
      return;
    }
    const [entry] = await db
      .select()
      .from(timeTrackingEntries)
      .where(and(eq(timeTrackingEntries.id, idParse.data), eq(timeTrackingEntries.isDeleted, false)))
      .limit(1);
    if (!entry) {
      res.status(404).json({ error: "Eintrag nicht gefunden." });
      return;
    }
    if (!canAccessEntry(req, entry.gmUserId)) {
      res.status(403).json({ error: "Keine Berechtigung für diesen Eintrag." });
      return;
    }
    if (entry.status !== "draft") {
      res.status(409).json({ error: "Kommentar kann nur für Entwürfe gesetzt werden." });
      return;
    }
    const [updated] = await db
      .update(timeTrackingEntries)
      .set({
        comment: parsed.data.comment?.trim() || null,
        updatedAt: new Date(),
      })
      .where(eq(timeTrackingEntries.id, entry.id))
      .returning();
    if (!updated) {
      throw new Error("Kommentar konnte nicht gespeichert werden.");
    }
    await appendEntryEvent({
      entryId: entry.id,
      gmUserId: req.authUser?.appUserId ?? null,
      eventType: "comment_set",
      payload: { comment: parsed.data.comment ?? null },
    });
    res.status(200).json({ entry: serializeEntry(updated) });
  } catch (error) {
    next(error);
  }
});

timeTrackingRouter.post("/entries/:id/doctor-confirmation/presign", async (req: AuthedRequest, res, next) => {
  try {
    const idParse = uuidSchema.safeParse(req.params.id);
    const parsed = doctorConfirmationPresignSchema.safeParse(req.body ?? {});
    if (!idParse.success || !parsed.success) {
      res.status(400).json({ error: "Ungültiges Arztbestätigung-Payload." });
      return;
    }
    const authUserId = req.authUser?.appUserId;
    if (!authUserId || req.authUser?.role !== "gm") {
      res.status(403).json({ error: "Nur der zuständige GM kann eine Arztbestätigung hochladen." });
      return;
    }
    if (!isAllowedDoctorConfirmationMime(parsed.data.mimeType)) {
      res.status(400).json({ error: "Arztbestätigung muss ein Foto im Format JPG, PNG, WebP oder HEIC sein." });
      return;
    }

    const [entry] = await db
      .select()
      .from(timeTrackingEntries)
      .where(and(eq(timeTrackingEntries.id, idParse.data), eq(timeTrackingEntries.isDeleted, false)))
      .limit(1);
    if (!entry) {
      res.status(404).json({ error: "Eintrag nicht gefunden." });
      return;
    }
    if (entry.gmUserId !== authUserId) {
      res.status(403).json({ error: "Diese Arztbestätigung gehört nicht zu deinem Account." });
      return;
    }
    if (entry.activityType !== "arztbesuch") {
      res.status(400).json({ error: "Arztbestätigung ist nur für Arztbesuch-Einträge möglich." });
      return;
    }
    if (entry.doctorConfirmationPath) {
      res.status(409).json({ error: "Für diesen Arztbesuch wurde bereits eine Arztbestätigung hochgeladen." });
      return;
    }

    const ext = normalizeDoctorConfirmationExt(parsed.data);
    const path = `doctor-confirmations/${authUserId}/${entry.id}/${crypto.randomUUID()}.${ext}`;
    const { data, error } = await supabaseAdmin.storage.from(DOCTOR_CONFIRMATION_BUCKET).createSignedUploadUrl(path);
    if (error || !data) {
      res.status(500).json({ error: "Upload-Link konnte nicht erstellt werden." });
      return;
    }

    res.status(200).json({
      upload: {
        bucket: DOCTOR_CONFIRMATION_BUCKET,
        path: data.path,
        signedUrl: data.signedUrl,
        token: data.token,
      },
    });
  } catch (error) {
    next(error);
  }
});

timeTrackingRouter.post("/entries/:id/doctor-confirmation/commit", async (req: AuthedRequest, res, next) => {
  try {
    const idParse = uuidSchema.safeParse(req.params.id);
    const parsed = doctorConfirmationCommitSchema.safeParse(req.body ?? {});
    if (!idParse.success || !parsed.success) {
      res.status(400).json({ error: "Arztbestätigung konnte nicht gespeichert werden." });
      return;
    }
    const authUserId = req.authUser?.appUserId;
    if (!authUserId || req.authUser?.role !== "gm") {
      res.status(403).json({ error: "Nur der zuständige GM kann eine Arztbestätigung hochladen." });
      return;
    }
    if (!isAllowedDoctorConfirmationMime(parsed.data.mimeType)) {
      res.status(400).json({ error: "Arztbestätigung muss ein Foto im Format JPG, PNG, WebP oder HEIC sein." });
      return;
    }

    const [entry] = await db
      .select()
      .from(timeTrackingEntries)
      .where(and(eq(timeTrackingEntries.id, idParse.data), eq(timeTrackingEntries.isDeleted, false)))
      .limit(1);
    if (!entry) {
      res.status(404).json({ error: "Eintrag nicht gefunden." });
      return;
    }
    if (entry.gmUserId !== authUserId) {
      res.status(403).json({ error: "Diese Arztbestätigung gehört nicht zu deinem Account." });
      return;
    }
    if (entry.activityType !== "arztbesuch") {
      res.status(400).json({ error: "Arztbestätigung ist nur für Arztbesuch-Einträge möglich." });
      return;
    }
    if (entry.doctorConfirmationPath) {
      res.status(409).json({ error: "Für diesen Arztbesuch wurde bereits eine Arztbestätigung hochgeladen." });
      return;
    }

    const normalizedPath = cleanStoragePath(parsed.data.storagePath);
    const expectedPrefix = `doctor-confirmations/${authUserId}/${entry.id}/`;
    if (!normalizedPath.startsWith(expectedPrefix)) {
      res.status(403).json({ error: "Diese Arztbestätigung gehört nicht zu diesem Eintrag." });
      return;
    }
    const exists = await verifyStorageObjectExists(DOCTOR_CONFIRMATION_BUCKET, normalizedPath);
    if (!exists) {
      res.status(400).json({ error: "Arztbestätigung wurde noch nicht vollständig hochgeladen." });
      return;
    }

    const now = new Date();
    const [updated] = await db
      .update(timeTrackingEntries)
      .set({
        doctorConfirmationBucket: DOCTOR_CONFIRMATION_BUCKET,
        doctorConfirmationPath: normalizedPath,
        doctorConfirmationFileName: safeDoctorConfirmationFileName(parsed.data.fileName),
        doctorConfirmationMimeType: parsed.data.mimeType ?? null,
        doctorConfirmationByteSize: parsed.data.byteSize ?? null,
        doctorConfirmationUploadedAt: now,
        updatedAt: now,
      })
      .where(eq(timeTrackingEntries.id, entry.id))
      .returning();
    if (!updated) {
      throw new Error("Arztbestätigung konnte nicht gespeichert werden.");
    }

    res.status(200).json({ entry: serializeEntry(updated) });
  } catch (error) {
    next(error);
  }
});

timeTrackingRouter.get("/entries/:id/doctor-confirmation", async (req: AuthedRequest, res, next) => {
  try {
    const idParse = uuidSchema.safeParse(req.params.id);
    if (!idParse.success) {
      res.status(400).json({ error: "Ungültige Eintrags-ID." });
      return;
    }
    const [entry] = await db
      .select()
      .from(timeTrackingEntries)
      .where(and(eq(timeTrackingEntries.id, idParse.data), eq(timeTrackingEntries.isDeleted, false)))
      .limit(1);
    if (!entry) {
      res.status(404).json({ error: "Eintrag nicht gefunden." });
      return;
    }
    if (!canAccessEntry(req, entry.gmUserId)) {
      res.status(403).json({ error: "Keine Berechtigung für diesen Eintrag." });
      return;
    }
    if (entry.activityType !== "arztbesuch" || !entry.doctorConfirmationPath) {
      res.status(404).json({ error: "Keine Arztbestätigung vorhanden." });
      return;
    }
    if (entry.doctorConfirmationBucket !== DOCTOR_CONFIRMATION_BUCKET) {
      res.status(404).json({ error: "Keine Arztbestätigung vorhanden." });
      return;
    }

    const normalizedPath = cleanStoragePath(entry.doctorConfirmationPath);
    const { data, error } = await supabaseAdmin.storage
      .from(DOCTOR_CONFIRMATION_BUCKET)
      .createSignedUrl(normalizedPath, DOCTOR_CONFIRMATION_READ_TTL_SECONDS);
    if (error || !data?.signedUrl) {
      res.status(500).json({ error: "Arztbestätigung konnte nicht geöffnet werden." });
      return;
    }

    res.status(200).json({
      doctorConfirmation: {
        signedUrl: data.signedUrl,
        expiresAt: new Date(Date.now() + DOCTOR_CONFIRMATION_READ_TTL_SECONDS * 1000).toISOString(),
        fileName: entry.doctorConfirmationFileName,
        mimeType: entry.doctorConfirmationMimeType,
      },
    });
  } catch (error) {
    next(error);
  }
});

timeTrackingRouter.post("/entries/:id/submit", async (req: AuthedRequest, res, next) => {
  try {
    const idParse = uuidSchema.safeParse(req.params.id);
    if (!idParse.success) {
      res.status(400).json({ error: "Ungültige Eintrags-ID." });
      return;
    }
    const [entry] = await db
      .select()
      .from(timeTrackingEntries)
      .where(and(eq(timeTrackingEntries.id, idParse.data), eq(timeTrackingEntries.isDeleted, false)))
      .limit(1);
    if (!entry) {
      res.status(404).json({ error: "Eintrag nicht gefunden." });
      return;
    }
    if (!canAccessEntry(req, entry.gmUserId)) {
      res.status(403).json({ error: "Keine Berechtigung für diesen Eintrag." });
      return;
    }
    if (entry.status !== "draft") {
      res.status(409).json({ error: "Nur Entwürfe können gespeichert werden." });
      return;
    }
    if (!entry.startAt || !entry.endAt) {
      res.status(400).json({ error: "Start und Ende müssen gesetzt sein." });
      return;
    }
    if (entry.endAt.getTime() < entry.startAt.getTime()) {
      res.status(400).json({ error: "Endzeit darf nicht vor Startzeit liegen." });
      return;
    }
    const now = new Date();
    await validateGmTimelineInterval({
      gmUserId: entry.gmUserId,
      kind: "zusatzzeit",
      id: entry.id,
      startAt: entry.startAt,
      endAt: entry.endAt,
      now,
    });
    const [updated] = await db
      .update(timeTrackingEntries)
      .set({
        status: "submitted",
        submittedAt: now,
        updatedAt: now,
      })
      .where(eq(timeTrackingEntries.id, entry.id))
      .returning();
    if (!updated) {
      throw new Error("Eintrag konnte nicht gespeichert werden.");
    }
    await appendEntryEvent({
      entryId: entry.id,
      gmUserId: req.authUser?.appUserId ?? null,
      eventType: "submitted",
      payload: {
        submittedAt: now.toISOString(),
      },
    });
    res.status(200).json({ entry: serializeEntry(updated) });
  } catch (error) {
    if (respondTimelineValidationError(res, error)) return;
    next(error);
  }
});

timeTrackingRouter.patch("/entries/:id/cancel", async (req: AuthedRequest, res, next) => {
  try {
    const idParse = uuidSchema.safeParse(req.params.id);
    if (!idParse.success) {
      res.status(400).json({ error: "Ungültige Eintrags-ID." });
      return;
    }
    const [entry] = await db
      .select()
      .from(timeTrackingEntries)
      .where(eq(timeTrackingEntries.id, idParse.data))
      .limit(1);
    if (!entry) {
      res.status(404).json({ error: "Eintrag nicht gefunden." });
      return;
    }
    if (!canAccessEntry(req, entry.gmUserId)) {
      res.status(403).json({ error: "Keine Berechtigung für diesen Eintrag." });
      return;
    }
    if (entry.status === "cancelled" || entry.isDeleted) {
      res.status(200).json({ entry: serializeEntry(entry) });
      return;
    }
    if (entry.status !== "draft") {
      res.status(409).json({ error: "Nur Entwürfe können abgebrochen werden." });
      return;
    }
    const now = new Date();
    const cancelledRows = await db
      .update(timeTrackingEntries)
      .set({
        status: "cancelled",
        cancelledAt: now,
        isDeleted: true,
        deletedAt: now,
        updatedAt: now,
      })
      .where(
        and(
          eq(timeTrackingEntries.gmUserId, entry.gmUserId),
          eq(timeTrackingEntries.activityType, entry.activityType),
          eq(timeTrackingEntries.status, "draft"),
          eq(timeTrackingEntries.isDeleted, false),
        ),
      )
      .returning();
    const updated = cancelledRows.find((row) => row.id === entry.id) ?? cancelledRows[0] ?? entry;
    for (const cancelled of cancelledRows) {
      await appendEntryEvent({
        entryId: cancelled.id,
        gmUserId: req.authUser?.appUserId ?? null,
        eventType: "cancelled",
        payload: {
          cancelledAt: now.toISOString(),
          cancelledByEntryId: entry.id,
        },
      });
    }
    res.status(200).json({ entry: serializeEntry(updated) });
  } catch (error) {
    next(error);
  }
});

timeTrackingRouter.get("/entries/draft/active", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = activeDraftQuerySchema.safeParse(req.query ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Abfrage für aktive Entwürfe." });
      return;
    }
    const authUserId = req.authUser?.appUserId;
    if (!authUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }
    const gmUserId =
      req.authUser?.role === "admin" && parsed.data.gmUserId
        ? parsed.data.gmUserId
        : authUserId;

    const rows = await db
      .select()
      .from(timeTrackingEntries)
      .where(
        and(
          eq(timeTrackingEntries.gmUserId, gmUserId),
          eq(timeTrackingEntries.status, "draft"),
          eq(timeTrackingEntries.isDeleted, false),
        ),
      )
      .orderBy(desc(timeTrackingEntries.updatedAt));
    res.status(200).json({
      entries: rows.map(serializeEntry),
      // Offline/session recovery contract for later UI wiring:
      // keep failed draft payload in session/local storage and retry with identical token.
      offlineContract: {
        idempotentTokenField: "clientEntryToken",
        retryGuidance: "When offline save fails, keep draft payload and retry with same token.",
      },
    });
  } catch (error) {
    next(error);
  }
});

export { timeTrackingRouter, normalizeActivityType };
