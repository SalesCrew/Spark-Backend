import { and, desc, eq, isNull } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { db } from "../lib/db.js";
import { ensureStartedDaySession } from "../lib/day-session.js";
import {
  timeTrackingEntries,
  timeTrackingEntryEvents,
  type timeTrackingActivityTypeEnum,
} from "../lib/schema.js";

const timeTrackingRouter = Router();
timeTrackingRouter.use(requireAuth(["gm", "admin"]));

const uuidSchema = z.string().uuid();
const isoDateTimeSchema = z.string().datetime();

const activityTypeSchema = z.enum([
  "sonderaufgabe",
  "arztbesuch",
  "werkstatt",
  "homeoffice",
  "schulung",
  "lager",
  "heimfahrt",
  "hotel",
  "hoteluebernachtung",
]);

const startDraftSchema = z
  .object({
    activityType: activityTypeSchema,
    startAt: isoDateTimeSchema,
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

type CanonicalActivityType = (typeof timeTrackingActivityTypeEnum.enumValues)[number];

const activityTypeMap: Record<z.infer<typeof activityTypeSchema>, CanonicalActivityType> = {
  sonderaufgabe: "sonderaufgabe",
  arztbesuch: "arztbesuch",
  werkstatt: "werkstatt",
  homeoffice: "homeoffice",
  schulung: "schulung",
  lager: "lager",
  heimfahrt: "heimfahrt",
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

timeTrackingRouter.post("/entries/draft/start", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = startDraftSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Startdaten fuer Zusatzzeiterfassung." });
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
        error: "Bitte zuerst den Arbeitstag starten.",
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

    if (entry) {
      const [updated] = await db
        .update(timeTrackingEntries)
        .set({
          startAt,
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
    next(error);
  }
});

timeTrackingRouter.patch("/entries/:id/draft/end", async (req: AuthedRequest, res, next) => {
  try {
    const idParse = uuidSchema.safeParse(req.params.id);
    const parsed = endDraftSchema.safeParse(req.body ?? {});
    if (!idParse.success || !parsed.success) {
      res.status(400).json({ error: "Ungueltige Enddaten fuer Zusatzzeiterfassung." });
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
      res.status(403).json({ error: "Keine Berechtigung fuer diesen Eintrag." });
      return;
    }
    if (entry.status !== "draft") {
      res.status(409).json({ error: "Endzeit kann nur fuer Entwuerfe gesetzt werden." });
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
      res.status(400).json({ error: "Ungueltiger Kommentar." });
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
      res.status(403).json({ error: "Keine Berechtigung fuer diesen Eintrag." });
      return;
    }
    if (entry.status !== "draft") {
      res.status(409).json({ error: "Kommentar kann nur fuer Entwuerfe gesetzt werden." });
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

timeTrackingRouter.post("/entries/:id/submit", async (req: AuthedRequest, res, next) => {
  try {
    const idParse = uuidSchema.safeParse(req.params.id);
    if (!idParse.success) {
      res.status(400).json({ error: "Ungueltige Eintrags-ID." });
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
      res.status(403).json({ error: "Keine Berechtigung fuer diesen Eintrag." });
      return;
    }
    if (entry.status !== "draft") {
      res.status(409).json({ error: "Nur Entwuerfe koennen gespeichert werden." });
      return;
    }
    if (!entry.startAt || !entry.endAt) {
      res.status(400).json({ error: "Start und Ende muessen gesetzt sein." });
      return;
    }
    if (entry.endAt.getTime() < entry.startAt.getTime()) {
      res.status(400).json({ error: "Endzeit darf nicht vor Startzeit liegen." });
      return;
    }
    const now = new Date();
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
    next(error);
  }
});

timeTrackingRouter.patch("/entries/:id/cancel", async (req: AuthedRequest, res, next) => {
  try {
    const idParse = uuidSchema.safeParse(req.params.id);
    if (!idParse.success) {
      res.status(400).json({ error: "Ungueltige Eintrags-ID." });
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
      res.status(403).json({ error: "Keine Berechtigung fuer diesen Eintrag." });
      return;
    }
    if (entry.status === "cancelled" || entry.isDeleted) {
      res.status(200).json({ entry: serializeEntry(entry) });
      return;
    }
    if (entry.status !== "draft") {
      res.status(409).json({ error: "Nur Entwuerfe koennen abgebrochen werden." });
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
      res.status(400).json({ error: "Ungueltige Abfrage fuer aktive Entwuerfe." });
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

