import crypto from "node:crypto";
import { and, desc, eq, gte, isNotNull, isNull, lt, sql } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { ensureAndGetGmKpiCache } from "../lib/gm-kpi-cache.js";
import { redMonthPeriodToYmd, resolveCurrentRedPeriod } from "../lib/red-month-periods.js";
import { loadZeiterfassungDaySessions } from "../lib/zeiterfassung-days.js";
import { db } from "../lib/db.js";
import {
  campaigns,
  gmDaySessionPauses,
  gmDaySessions,
  gmTextSettings,
  markets,
  timeTrackingEntries,
  users,
  visitSessionSections,
  visitSessions,
} from "../lib/schema.js";
import { supabaseAdmin } from "../lib/supabase.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const gmProfileRouter = Router();

const VIENNA_TIMEZONE = "Europe/Vienna";
const GM_PROFILE_PHOTOS_BUCKET = "gm-profile-photos";
const PROFILE_PHOTO_SIGNED_URL_SECONDS = 60 * 60;
const PROFILE_PHOTO_MAX_BYTES = 5 * 1024 * 1024;
const PROFILE_PHOTO_MIME_BY_EXT: Record<string, string> = {
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  png: "image/png",
  webp: "image/webp",
  heic: "image/heic",
  heif: "image/heif",
};

const gmTextSettingsPayloadSchema = z.object({
  textScalePercent: z.number().int().min(0).max(50),
});

function ymdInTimezone(date: Date, timezone = VIENNA_TIMEZONE): string {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).formatToParts(date);
  const year = parts.find((part) => part.type === "year")?.value ?? "1970";
  const month = parts.find((part) => part.type === "month")?.value ?? "01";
  const day = parts.find((part) => part.type === "day")?.value ?? "01";
  return `${year}-${month}-${day}`;
}

function startOfWeekYmd(todayYmd: string): string {
  const date = new Date(`${todayYmd}T12:00:00.000Z`);
  const day = (date.getUTCDay() + 6) % 7;
  date.setUTCDate(date.getUTCDate() - day);
  return date.toISOString().slice(0, 10);
}

function addOneDay(date: Date): Date {
  const copy = new Date(date);
  copy.setUTCDate(copy.getUTCDate() + 1);
  return copy;
}

gmProfileRouter.use(requireAuth(["gm"]));

function serializeDashboardDaySession(row: typeof gmDaySessions.$inferSelect | null) {
  if (!row) return null;
  return {
    id: row.id,
    gmUserId: row.gmUserId,
    workDate: row.workDate,
    timezone: row.timezone,
    status: row.status,
    dayStartedAt: row.dayStartedAt?.toISOString() ?? null,
    dayEndedAt: row.dayEndedAt?.toISOString() ?? null,
    startKm: row.startKm,
    endKm: row.endKm,
    startKmDeferred: row.startKmDeferred,
    endKmDeferred: row.endKmDeferred,
    isStartKmCompleted: row.isStartKmCompleted,
    isEndKmCompleted: row.isEndKmCompleted,
    comment: row.comment,
    submittedAt: row.submittedAt?.toISOString() ?? null,
    cancelledAt: row.cancelledAt?.toISOString() ?? null,
  };
}

function serializeDashboardDraft(row: typeof timeTrackingEntries.$inferSelect) {
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
    durationMin: row.startAt && row.endAt ? Math.max(0, Math.floor((row.endAt.getTime() - row.startAt.getTime()) / 60_000)) : null,
  };
}

gmProfileRouter.get("/settings/text-scale", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) return res.status(401).json({ error: "auth_required" });

    const [settings] = await db
      .select({
        textScalePercent: gmTextSettings.textScalePercent,
        updatedAt: gmTextSettings.updatedAt,
      })
      .from(gmTextSettings)
      .where(and(eq(gmTextSettings.userId, gmUserId), eq(gmTextSettings.isDeleted, false)))
      .limit(1);

    return res.json({
      textScalePercent: settings?.textScalePercent ?? 0,
      updatedAt: settings?.updatedAt?.toISOString() ?? null,
    });
  } catch (error) {
    return next(error);
  }
});

gmProfileRouter.patch("/settings/text-scale", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) return res.status(401).json({ error: "auth_required" });

    const parsed = gmTextSettingsPayloadSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      return res.status(400).json({ error: "invalid_payload", issues: parsed.error.issues });
    }

    const now = new Date();
    const [settings] = await db
      .insert(gmTextSettings)
      .values({
        userId: gmUserId,
        textScalePercent: parsed.data.textScalePercent,
        isDeleted: false,
        deletedAt: null,
        updatedAt: now,
      })
      .onConflictDoUpdate({
        target: gmTextSettings.userId,
        set: {
          textScalePercent: parsed.data.textScalePercent,
          isDeleted: false,
          deletedAt: null,
          updatedAt: now,
        },
      })
      .returning({
        textScalePercent: gmTextSettings.textScalePercent,
        updatedAt: gmTextSettings.updatedAt,
      });

    return res.json({
      textScalePercent: settings?.textScalePercent ?? parsed.data.textScalePercent,
      updatedAt: settings?.updatedAt?.toISOString() ?? now.toISOString(),
    });
  } catch (error) {
    return next(error);
  }
});

async function loadDashboardDayGate(gmUserId: string, now: Date) {
  const todayYmd = ymdInTimezone(now);
  const [todaySession] = await db
    .select()
    .from(gmDaySessions)
    .where(
      and(
        eq(gmDaySessions.gmUserId, gmUserId),
        eq(gmDaySessions.workDate, todayYmd),
        eq(gmDaySessions.isDeleted, false),
      ),
    )
    .limit(1);

  const [openSession] = todaySession
    ? [todaySession]
    : await db
        .select()
        .from(gmDaySessions)
        .where(
          and(
            eq(gmDaySessions.gmUserId, gmUserId),
            eq(gmDaySessions.isDeleted, false),
            sql`${gmDaySessions.status}::text in ('started', 'ended')`,
          ),
        )
        .orderBy(desc(gmDaySessions.dayStartedAt), desc(gmDaySessions.updatedAt))
        .limit(1);

  const session = openSession ?? null;
  const [openPause] = session
    ? await db
        .select({ id: gmDaySessionPauses.id })
        .from(gmDaySessionPauses)
        .where(
          and(
            eq(gmDaySessionPauses.gmUserId, gmUserId),
            eq(gmDaySessionPauses.daySessionId, session.id),
            eq(gmDaySessionPauses.isDeleted, false),
            isNull(gmDaySessionPauses.pauseEndedAt),
          ),
        )
        .limit(1)
    : [];
  const sessionTimezone = session?.timezone || VIENNA_TIMEZONE;
  const currentWorkDate = ymdInTimezone(now, sessionTimezone);
  const staleDayOpen = Boolean(
    session &&
      (session.status === "started" || session.status === "ended") &&
      session.workDate < currentWorkDate,
  );

  return {
    session: serializeDashboardDaySession(session),
    gate: {
      dayStarted: Boolean(session && session.status === "started" && session.dayStartedAt && !staleDayOpen),
      startKmPending: Boolean(session && !session.isStartKmCompleted),
      endKmPending: Boolean(session && Boolean(session.dayEndedAt) && !session.isEndKmCompleted),
      pauseOpen: Boolean(openPause),
      staleDayOpen,
    },
  };
}

gmProfileRouter.get("/dashboard/critical", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }

    const now = new Date();
    const [daySession, draftRows, activeVisitRows, redPeriodResult] = await Promise.all([
      loadDashboardDayGate(gmUserId, now),
      db
        .select()
        .from(timeTrackingEntries)
        .where(
          and(
            eq(timeTrackingEntries.gmUserId, gmUserId),
            eq(timeTrackingEntries.status, "draft"),
            eq(timeTrackingEntries.isDeleted, false),
          ),
        )
        .orderBy(desc(timeTrackingEntries.updatedAt)),
      db
        .select({
          sessionId: visitSessions.id,
          startedAt: visitSessions.startedAt,
          marketId: visitSessions.marketId,
          marketName: markets.name,
          marketAddress: markets.address,
          marketPostalCode: markets.postalCode,
          marketCity: markets.city,
        })
        .from(visitSessions)
        .innerJoin(markets, eq(markets.id, visitSessions.marketId))
        .where(
          and(
            eq(visitSessions.gmUserId, gmUserId),
            eq(visitSessions.status, "draft"),
            isNull(visitSessions.submittedAt),
            eq(visitSessions.isDeleted, false),
          ),
        )
        .orderBy(desc(visitSessions.startedAt), desc(visitSessions.updatedAt))
        .limit(1),
      resolveCurrentRedPeriod(now)
        .then((period) => ({ ok: true as const, period }))
        .catch(() => ({ ok: false as const, period: null })),
    ]);

    const activeVisit = activeVisitRows[0] ?? null;
    const sectionRows = activeVisit
      ? await db
          .select({
            campaignId: visitSessionSections.campaignId,
            campaignName: campaigns.name,
          })
          .from(visitSessionSections)
          .innerJoin(campaigns, eq(campaigns.id, visitSessionSections.campaignId))
          .where(
            and(
              eq(visitSessionSections.visitSessionId, activeVisit.sessionId),
              eq(visitSessionSections.isDeleted, false),
              eq(campaigns.isDeleted, false),
            ),
          )
          .orderBy(visitSessionSections.orderIndex)
      : [];

    const campaignIds = Array.from(new Set(sectionRows.map((row) => row.campaignId).filter(Boolean)));
    const campaignNames = Array.from(new Set(sectionRows.map((row) => row.campaignName).filter(Boolean)));
    const redPeriod = redPeriodResult.ok ? redPeriodResult.period : null;
    const redDates = redPeriod ? redMonthPeriodToYmd(redPeriod) : null;
    const todayStart = new Date(now);
    todayStart.setHours(0, 0, 0, 0);

    res.status(200).json({
      serverTime: now.toISOString(),
      daySession,
      activeVisit: activeVisit
        ? {
            sessionId: activeVisit.sessionId,
            startedAt: activeVisit.startedAt.toISOString(),
            marketId: activeVisit.marketId,
            marketName: activeVisit.marketName,
            marketAddress: activeVisit.marketAddress,
            marketPostalCode: activeVisit.marketPostalCode,
            marketCity: activeVisit.marketCity,
            campaignIds,
            campaignNames,
          }
        : null,
      activeTimeTrackingDrafts: draftRows.map(serializeDashboardDraft),
      redPeriod: redPeriod && redDates
        ? {
            redPeriodId: redPeriod.redPeriodId,
            label: redPeriod.label,
            startDate: redDates.startYmd,
            endDate: redDates.endYmd,
            daysUntilEnd: Math.max(0, Math.floor((redPeriod.end.getTime() - todayStart.getTime()) / (24 * 60 * 60 * 1000))),
          }
        : null,
    });
  } catch (error) {
    next(error);
  }
});

function cleanStoragePath(path: string): string {
  return path.replace(/^\/+/, "").replace(/\\/g, "/");
}

function normalizeProfilePhotoExt(input?: { extension?: string | undefined; mimeType?: string | undefined }): string {
  const rawExt = (input?.extension ?? "").trim().toLowerCase().replace(/^\./, "");
  if (rawExt && PROFILE_PHOTO_MIME_BY_EXT[rawExt]) return rawExt === "jpeg" ? "jpg" : rawExt;
  const mime = (input?.mimeType ?? "").trim().toLowerCase();
  const match = Object.entries(PROFILE_PHOTO_MIME_BY_EXT).find(([, value]) => value === mime);
  if (match) return match[0] === "jpeg" ? "jpg" : match[0];
  return "jpg";
}

function isAllowedProfilePhotoMime(mimeType: string | null | undefined): boolean {
  if (!mimeType) return true;
  return Object.values(PROFILE_PHOTO_MIME_BY_EXT).includes(mimeType.toLowerCase());
}

async function signProfilePhotoUrl(input: {
  storageBucket: string | null;
  storagePath: string | null;
}): Promise<{ storageBucket: string; storagePath: string; signedUrl: string; expiresAt: string } | null> {
  if (!input.storageBucket || !input.storagePath) return null;
  if (input.storageBucket !== GM_PROFILE_PHOTOS_BUCKET) return null;
  const normalizedPath = cleanStoragePath(input.storagePath);
  const { data, error } = await supabaseAdmin.storage
    .from(GM_PROFILE_PHOTOS_BUCKET)
    .createSignedUrl(normalizedPath, PROFILE_PHOTO_SIGNED_URL_SECONDS);
  if (error || !data?.signedUrl) return null;
  return {
    storageBucket: GM_PROFILE_PHOTOS_BUCKET,
    storagePath: normalizedPath,
    signedUrl: data.signedUrl,
    expiresAt: new Date(Date.now() + PROFILE_PHOTO_SIGNED_URL_SECONDS * 1000).toISOString(),
  };
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

gmProfileRouter.get("/profile", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }

    const now = new Date();
    const todayYmd = ymdInTimezone(now);
    const weekStartYmd = startOfWeekYmd(todayYmd);
    const redPeriod = await resolveCurrentRedPeriod(now);
    const redPeriodDates = redMonthPeriodToYmd(redPeriod);
    const redPeriodEndExclusive = addOneDay(redPeriod.end);

    const [
      userRows,
      currentRedVisitRows,
      allVisitRows,
      latestVisitRows,
      kpiSummary,
      weekSessions,
    ] = await Promise.all([
      db.select().from(users).where(eq(users.id, gmUserId)).limit(1),
      db
        .select({ count: sql<number>`count(*)::int` })
        .from(visitSessions)
        .where(
          and(
            eq(visitSessions.gmUserId, gmUserId),
            eq(visitSessions.status, "submitted"),
            eq(visitSessions.isDeleted, false),
            isNotNull(visitSessions.submittedAt),
            gte(visitSessions.submittedAt, redPeriod.start),
            lt(visitSessions.submittedAt, redPeriodEndExclusive),
          ),
        ),
      db
        .select({ count: sql<number>`count(*)::int` })
        .from(visitSessions)
        .where(
          and(
            eq(visitSessions.gmUserId, gmUserId),
            eq(visitSessions.status, "submitted"),
            eq(visitSessions.isDeleted, false),
            isNotNull(visitSessions.submittedAt),
          ),
        ),
      db
        .select({
          id: visitSessions.id,
          startedAt: visitSessions.startedAt,
          submittedAt: visitSessions.submittedAt,
          marketName: markets.name,
          marketAddress: markets.address,
          marketPostalCode: markets.postalCode,
          marketCity: markets.city,
        })
        .from(visitSessions)
        .leftJoin(markets, eq(markets.id, visitSessions.marketId))
        .where(
          and(
            eq(visitSessions.gmUserId, gmUserId),
            eq(visitSessions.status, "submitted"),
            eq(visitSessions.isDeleted, false),
            isNotNull(visitSessions.submittedAt),
          ),
        )
        .orderBy(desc(visitSessions.submittedAt))
        .limit(1),
      ensureAndGetGmKpiCache(gmUserId),
      loadZeiterfassungDaySessions({
        from: weekStartYmd,
        to: todayYmd,
        gmUserIds: [gmUserId],
        includeLive: true,
        timezone: VIENNA_TIMEZONE,
      }),
    ]);

    const user = userRows[0];
    if (!user || !user.isActive || user.deletedAt || user.role !== "gm") {
      res.status(404).json({ error: "GM Profil wurde nicht gefunden." });
      return;
    }

    const weekWorkMinutes = weekSessions.reduce((sum, session) => sum + Math.max(0, session.stats.reineArbeitszeit), 0);
    const finishedWeekSessions = weekSessions.filter((session) => session.status === "ended" || session.status === "submitted");
    const averageWorkdayMin = finishedWeekSessions.length > 0
      ? Math.round(finishedWeekSessions.reduce((sum, session) => sum + Math.max(0, session.stats.reineArbeitszeit), 0) / finishedWeekSessions.length)
      : 0;
    const latestVisit = latestVisitRows[0] ?? null;

    res.status(200).json({
      profile: {
        id: user.id,
        firstName: user.firstName,
        lastName: user.lastName,
        email: user.email,
        phone: user.phone ?? "",
        address: user.address ?? "",
        city: user.city ?? "",
        postalCode: user.postalCode ?? "",
        region: user.region ?? "",
        isBillaGm: user.isBillaGm,
        profilePhoto: await signProfilePhotoUrl({
          storageBucket: user.profilePhotoBucket,
          storagePath: user.profilePhotoPath,
        }),
        createdAt: user.createdAt.toISOString(),
        updatedAt: user.updatedAt.toISOString(),
      },
      stats: {
        redPeriod: {
          redPeriodId: redPeriod.redPeriodId,
          redMonthYearId: redPeriod.redMonthYearId,
          label: redPeriod.label,
          startDate: redPeriodDates.startYmd,
          endDate: redPeriodDates.endYmd,
          redYear: redPeriod.redYear,
        },
        currentRedVisitCount: Number(currentRedVisitRows[0]?.count ?? 0),
        allTimeVisitCount: Number(allVisitRows[0]?.count ?? 0),
        latestVisit: latestVisit
          ? {
              id: latestVisit.id,
              startedAt: latestVisit.startedAt?.toISOString() ?? null,
              submittedAt: latestVisit.submittedAt?.toISOString() ?? null,
              marketName: latestVisit.marketName ?? "",
              marketAddress: [
                latestVisit.marketAddress,
                [latestVisit.marketPostalCode, latestVisit.marketCity].filter(Boolean).join(" "),
              ].filter(Boolean).join(", "),
            }
          : null,
        ippAllTimeAvg: kpiSummary.ippAllTimeAvg,
        ippSampleCount: kpiSummary.ippSampleCount,
        bonusCumulativeEur: kpiSummary.bonusCumulativeEur,
        weekWorkMinutes,
        averageWorkdayMin,
        trackedWeekDays: weekSessions.length,
      },
    });
  } catch (error) {
    next(error);
  }
});

gmProfileRouter.post("/profile/photo/presign", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }

    const parsed = z
      .object({
        extension: z.string().max(12).optional(),
        mimeType: z.string().max(80).optional(),
      })
      .strict()
      .safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültiges Profilfoto-Payload." });
      return;
    }
    if (!isAllowedProfilePhotoMime(parsed.data.mimeType)) {
      res.status(400).json({ error: "Profilfoto muss ein Bild im Format JPG, PNG oder WebP sein." });
      return;
    }

    const ext = normalizeProfilePhotoExt(parsed.data);
    const path = `${GM_PROFILE_PHOTOS_BUCKET}/${gmUserId}/${crypto.randomUUID()}.${ext}`;
    const { data, error } = await supabaseAdmin.storage.from(GM_PROFILE_PHOTOS_BUCKET).createSignedUploadUrl(path);
    if (error || !data) {
      res.status(500).json({ error: "Upload-Link konnte nicht erstellt werden." });
      return;
    }

    res.status(200).json({
      upload: {
        bucket: GM_PROFILE_PHOTOS_BUCKET,
        path: data.path,
        signedUrl: data.signedUrl,
        token: data.token,
      },
    });
  } catch (error) {
    next(error);
  }
});

gmProfileRouter.post("/profile/photo/commit", async (req: AuthedRequest, res, next) => {
  try {
    const gmUserId = req.authUser?.appUserId;
    if (!gmUserId) {
      res.status(401).json({ error: "Nicht eingeloggt." });
      return;
    }

    const parsed = z
      .object({
        storageBucket: z.literal(GM_PROFILE_PHOTOS_BUCKET),
        storagePath: z.string().min(1),
        mimeType: z.string().max(80).optional(),
        byteSize: z.number().int().min(1).max(PROFILE_PHOTO_MAX_BYTES).optional(),
      })
      .strict()
      .safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Profilfoto konnte nicht gespeichert werden." });
      return;
    }
    if (!isAllowedProfilePhotoMime(parsed.data.mimeType)) {
      res.status(400).json({ error: "Profilfoto muss ein Bild im Format JPG, PNG oder WebP sein." });
      return;
    }

    const normalizedPath = cleanStoragePath(parsed.data.storagePath);
    const expectedPrefix = `${GM_PROFILE_PHOTOS_BUCKET}/${gmUserId}/`;
    if (!normalizedPath.startsWith(expectedPrefix)) {
      res.status(403).json({ error: "Dieses Profilfoto gehört nicht zu deinem Account." });
      return;
    }
    const exists = await verifyStorageObjectExists(GM_PROFILE_PHOTOS_BUCKET, normalizedPath);
    if (!exists) {
      res.status(400).json({ error: "Profilfoto wurde noch nicht vollständig hochgeladen." });
      return;
    }

    const [previousUser] = await db
      .select({
        profilePhotoBucket: users.profilePhotoBucket,
        profilePhotoPath: users.profilePhotoPath,
      })
      .from(users)
      .where(eq(users.id, gmUserId))
      .limit(1);

    const [updatedUser] = await db
      .update(users)
      .set({
        profilePhotoBucket: GM_PROFILE_PHOTOS_BUCKET,
        profilePhotoPath: normalizedPath,
        profilePhotoUpdatedAt: new Date(),
        updatedAt: new Date(),
      })
      .where(eq(users.id, gmUserId))
      .returning({
        profilePhotoBucket: users.profilePhotoBucket,
        profilePhotoPath: users.profilePhotoPath,
      });

    const previousPath = previousUser?.profilePhotoPath ? cleanStoragePath(previousUser.profilePhotoPath) : null;
    if (
      previousUser?.profilePhotoBucket === GM_PROFILE_PHOTOS_BUCKET &&
      previousPath &&
      previousPath !== normalizedPath
    ) {
      void supabaseAdmin.storage.from(GM_PROFILE_PHOTOS_BUCKET).remove([previousPath]);
    }

    res.status(200).json({
      ok: true,
      profilePhoto: await signProfilePhotoUrl({
        storageBucket: updatedUser?.profilePhotoBucket ?? GM_PROFILE_PHOTOS_BUCKET,
        storagePath: updatedUser?.profilePhotoPath ?? normalizedPath,
      }),
    });
  } catch (error) {
    next(error);
  }
});

export { gmProfileRouter };
