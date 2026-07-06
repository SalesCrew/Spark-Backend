import { and, desc, eq, ilike, or, sql, type SQL } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import {
  authAuditLogs,
  dsarRequestEvents,
  dsarRequests,
  employeeAgreementAcceptances,
  gmDaySessionPauses,
  gmDaySessions,
  gmKpiCache,
  timeEntryChangeRequests,
  timeTrackingEntries,
  users,
  visitAnswerChangeRequests,
  visitAnswerPhotos,
  visitAnswers,
  visitSessions,
} from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const requestTypes = ["access", "rectification", "erasure", "restriction", "portability", "objection", "mixed"] as const;
const requestStatuses = ["open", "identity_check", "collecting", "decision", "responded", "closed", "cancelled"] as const;

const listDsarQuerySchema = z
  .object({
    status: z.enum(requestStatuses).optional(),
    search: z.string().trim().max(200).optional(),
  })
  .strict();

const createDsarRequestSchema = z
  .object({
    requestType: z.enum(requestTypes),
    intakeChannel: z.string().trim().min(1).max(80).optional(),
    requesterName: z.string().trim().min(1).max(200),
    requesterEmail: z.string().trim().email(),
    requesterUserId: z.string().uuid().nullable().optional(),
    subjectUserId: z.string().uuid().nullable().optional(),
    subjectName: z.string().trim().min(1).max(200).optional(),
    subjectEmail: z.string().trim().email().optional(),
    subjectRole: z.string().trim().max(80).nullable().optional(),
    requestSummary: z.string().trim().max(5_000).optional(),
    assignedToUserId: z.string().uuid().nullable().optional(),
  })
  .strict();

const updateDsarRequestSchema = z
  .object({
    status: z.enum(requestStatuses).optional(),
    assignedToUserId: z.string().uuid().nullable().optional(),
    identityVerified: z.boolean().optional(),
    extendedUntil: z.string().datetime().nullable().optional(),
    extensionReason: z.string().trim().max(2_000).nullable().optional(),
    decisionSummary: z.string().trim().max(5_000).nullable().optional(),
    legalBlockers: z.string().trim().max(5_000).nullable().optional(),
    responseChannel: z.string().trim().max(120).nullable().optional(),
    responseSentAt: z.string().datetime().nullable().optional(),
    exportPackageSummary: z.record(z.string(), z.unknown()).nullable().optional(),
  })
  .strict();

const addDsarEventSchema = z
  .object({
    eventType: z.string().trim().min(1).max(120),
    message: z.string().trim().max(5_000).optional(),
    metadata: z.record(z.string(), z.unknown()).optional(),
  })
  .strict();

export const adminDsarRouter = Router();

adminDsarRouter.use(requireAuth(["admin"]));

function addOneCalendarMonth(date: Date): Date {
  const copy = new Date(date.getTime());
  copy.setMonth(copy.getMonth() + 1);
  return copy;
}

function toIso(value: Date | null | undefined): string | null {
  return value instanceof Date ? value.toISOString() : null;
}

function serializeDsarRequest(row: typeof dsarRequests.$inferSelect) {
  return {
    id: row.id,
    requestType: row.requestType,
    status: row.status,
    intakeChannel: row.intakeChannel,
    requesterName: row.requesterName,
    requesterEmail: row.requesterEmail,
    requesterUserId: row.requesterUserId,
    subjectUserId: row.subjectUserId,
    subjectNameSnapshot: row.subjectNameSnapshot,
    subjectEmailSnapshot: row.subjectEmailSnapshot,
    subjectRoleSnapshot: row.subjectRoleSnapshot,
    requestSummary: row.requestSummary,
    assignedToUserId: row.assignedToUserId,
    receivedAt: row.receivedAt.toISOString(),
    dueAt: row.dueAt.toISOString(),
    extendedUntil: toIso(row.extendedUntil),
    extensionReason: row.extensionReason,
    identityVerifiedAt: toIso(row.identityVerifiedAt),
    identityVerifiedByUserId: row.identityVerifiedByUserId,
    decisionSummary: row.decisionSummary,
    legalBlockers: row.legalBlockers,
    responseChannel: row.responseChannel,
    responseSentAt: toIso(row.responseSentAt),
    responseSentByUserId: row.responseSentByUserId,
    exportPackageSummary: row.exportPackageSummary ?? null,
    isDeleted: row.isDeleted,
    deletedAt: toIso(row.deletedAt),
    createdByUserId: row.createdByUserId,
    createdAt: row.createdAt.toISOString(),
    updatedAt: row.updatedAt.toISOString(),
  };
}

function serializeDsarEvent(row: typeof dsarRequestEvents.$inferSelect) {
  return {
    id: row.id,
    requestId: row.requestId,
    actorUserId: row.actorUserId,
    eventType: row.eventType,
    message: row.message,
    metadata: row.metadata ?? null,
    createdAt: row.createdAt.toISOString(),
  };
}

async function readRequestOrNull(requestId: string) {
  const [row] = await db
    .select()
    .from(dsarRequests)
    .where(and(eq(dsarRequests.id, requestId), eq(dsarRequests.isDeleted, false)))
    .limit(1);
  return row ?? null;
}

async function insertRequestEvent(
  requestId: string,
  actorUserId: string | null,
  eventType: string,
  message: string,
  metadata?: Record<string, unknown>,
) {
  const [event] = await db
    .insert(dsarRequestEvents)
    .values({
      requestId,
      actorUserId,
      eventType,
      message,
      metadata,
    })
    .returning();
  if (!event) {
    throw new Error("Failed to create DSAR event.");
  }
  return event;
}

async function countWhere(table: unknown, where: SQL<unknown> | undefined): Promise<number> {
  if (!where) return 0;
  const [row] = await db
    .select({ count: sql<number>`count(*)::int` })
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    .from(table as any)
    .where(where)
    .limit(1);
  return Number(row?.count ?? 0);
}

async function buildSubjectDataPackage(subjectUserId: string) {
  const [subject] = await db
    .select({
      id: users.id,
      role: users.role,
      email: users.email,
      firstName: users.firstName,
      lastName: users.lastName,
      phone: users.phone,
      address: users.address,
      city: users.city,
      postalCode: users.postalCode,
      region: users.region,
      isActive: users.isActive,
      deletedAt: users.deletedAt,
      anonymizedAt: users.anonymizedAt,
      createdAt: users.createdAt,
      updatedAt: users.updatedAt,
    })
    .from(users)
    .where(eq(users.id, subjectUserId))
    .limit(1);

  if (!subject) {
    return {
      generatedAt: new Date().toISOString(),
      subject: null,
      categories: [],
      limitations: ["Der betroffene Nutzer wurde nicht gefunden oder bereits entfernt."],
    };
  }

  const [
    visitCount,
    answerCountRow,
    photoCountRow,
    dayCount,
    timeEntryCount,
    pauseCount,
    answerChangeCount,
    timeChangeCount,
    authAuditCount,
    agreementCount,
    kpiCount,
  ] = await Promise.all([
    countWhere(visitSessions, and(eq(visitSessions.gmUserId, subjectUserId), eq(visitSessions.isDeleted, false))),
    db
      .select({ count: sql<number>`count(*)::int` })
      .from(visitAnswers)
      .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
      .where(
        and(
          eq(visitSessions.gmUserId, subjectUserId),
          eq(visitSessions.isDeleted, false),
          eq(visitAnswers.isDeleted, false),
        ),
      )
      .limit(1),
    db
      .select({ count: sql<number>`count(*)::int` })
      .from(visitAnswerPhotos)
      .innerJoin(visitAnswers, eq(visitAnswers.id, visitAnswerPhotos.visitAnswerId))
      .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
      .where(
        and(
          eq(visitSessions.gmUserId, subjectUserId),
          eq(visitSessions.isDeleted, false),
          eq(visitAnswers.isDeleted, false),
          eq(visitAnswerPhotos.isDeleted, false),
        ),
      )
      .limit(1),
    countWhere(gmDaySessions, and(eq(gmDaySessions.gmUserId, subjectUserId), eq(gmDaySessions.isDeleted, false))),
    countWhere(timeTrackingEntries, and(eq(timeTrackingEntries.gmUserId, subjectUserId), eq(timeTrackingEntries.isDeleted, false))),
    countWhere(gmDaySessionPauses, and(eq(gmDaySessionPauses.gmUserId, subjectUserId), eq(gmDaySessionPauses.isDeleted, false))),
    countWhere(
      visitAnswerChangeRequests,
      and(eq(visitAnswerChangeRequests.gmUserId, subjectUserId), eq(visitAnswerChangeRequests.isDeleted, false)),
    ),
    countWhere(
      timeEntryChangeRequests,
      and(eq(timeEntryChangeRequests.gmUserId, subjectUserId), eq(timeEntryChangeRequests.isDeleted, false)),
    ),
    countWhere(
      authAuditLogs,
      or(eq(authAuditLogs.actorUserId, subjectUserId), eq(authAuditLogs.targetUserId, subjectUserId))!,
    ),
    countWhere(employeeAgreementAcceptances, eq(employeeAgreementAcceptances.userId, subjectUserId)),
    countWhere(gmKpiCache, and(eq(gmKpiCache.gmUserId, subjectUserId), eq(gmKpiCache.isDeleted, false))),
  ]);

  const answerCount = Number(answerCountRow[0]?.count ?? 0);
  const photoCount = Number(photoCountRow[0]?.count ?? 0);

  return {
    generatedAt: new Date().toISOString(),
    subject: {
      id: subject.id,
      role: subject.role,
      name: `${subject.firstName} ${subject.lastName}`.trim(),
      email: subject.email,
      phone: subject.phone,
      address: subject.address,
      postalCode: subject.postalCode,
      city: subject.city,
      region: subject.region,
      isActive: subject.isActive,
      deletedAt: toIso(subject.deletedAt),
      anonymizedAt: toIso(subject.anonymizedAt),
      createdAt: subject.createdAt.toISOString(),
      updatedAt: subject.updatedAt.toISOString(),
    },
    categories: [
      {
        key: "profile",
        label: "Profil und Account",
        count: 1,
        retention: "Dauer des aktiven Vertrags-/Arbeitsverhältnisses plus gesetzliche Nachweisfristen.",
        actionHint: "Korrektur über Admin-Stammdaten oder Anonymisierung bei Austritt.",
      },
      {
        key: "visits",
        label: "Marktbesuche",
        count: visitCount,
        retention: "Regelmäßig bis zu 3 Jahre für Reporting, Qualität und Nachweis von Leistungen.",
        actionHint: "Besuchsantworten und Fotos werden bei Personenbezug geprüft; operative Statistik bleibt erhalten.",
      },
      {
        key: "answers",
        label: "Fragebogen-Antworten",
        count: answerCount,
        retention: "Gleiche Frist wie Marktbesuche; fachliche Antworten bleiben dem Markt/Kampagnenkontext zugeordnet.",
        actionHint: "Berichtigung über Antwortprüfung oder Admin-Korrekturprozess.",
      },
      {
        key: "photos",
        label: "Fotos aus Foto-Fragen",
        count: photoCount,
        retention: "So kurz wie möglich für Reporting/Qualitätsnachweis, derzeit operativ bis zu 3 Jahre dokumentiert.",
        actionHint: "Fotos mit unnötigem Personenbezug priorisiert prüfen und löschen/anonymisieren.",
      },
      {
        key: "time",
        label: "Arbeitszeit, Pausen und KM",
        count: dayCount + timeEntryCount + pauseCount,
        retention: "Arbeitszeit-/Entgelt-Nachweise mindestens entsprechend gesetzlichen Aufbewahrungspflichten.",
        actionHint: "Berichtigung über Zeiterfassungs-Änderungsanfrage oder Admin-Zeiterfassung.",
      },
      {
        key: "change_requests",
        label: "Korrekturanfragen",
        count: answerChangeCount + timeChangeCount,
        retention: "Für Nachvollziehbarkeit der Korrekturentscheidung und Audit-Trail.",
        actionHint: "Status, Entscheidung und Begründung im jeweiligen Prüfprozess dokumentieren.",
      },
      {
        key: "security",
        label: "Login-, Sicherheits- und Vereinbarungsnachweise",
        count: authAuditCount + agreementCount,
        retention: "Für Sicherheit, Missbrauchsprävention und Nachweis der Einwilligung/Vereinbarung.",
        actionHint: "Nur technische und rechtliche Nachweise, nicht für operative Leistungsbewertung.",
      },
      {
        key: "kpi",
        label: "KPI-/Bonus-Zwischenspeicher",
        count: kpiCount,
        retention: "Wird aus operativen Daten abgeleitet und bei Neuberechnung ersetzt.",
        actionHint: "Bei Datenkorrekturen neu berechnen lassen.",
      },
    ],
    limitations: [
      "Dieses Paket ist eine Arbeitsübersicht für die DSAR-Bearbeitung, kein ungeprüfter Direkt-Export.",
      "Vor Herausgabe müssen Rechte Dritter, gesetzliche Aufbewahrungspflichten und Kundengeheimnisse geprüft werden.",
      "Exporte außerhalb von Spark müssen intern dokumentiert und sicher gespeichert werden.",
    ],
  };
}

adminDsarRouter.get("/", async (req, res, next) => {
  try {
    const parsed = listDsarQuerySchema.safeParse(req.query);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Filter.", code: "invalid_query", issues: parsed.error.issues });
      return;
    }

    const filters: SQL<unknown>[] = [eq(dsarRequests.isDeleted, false)];
    if (parsed.data.status) {
      filters.push(eq(dsarRequests.status, parsed.data.status));
    }
    const search = parsed.data.search?.trim();
    if (search) {
      const needle = `%${search}%`;
      filters.push(
        or(
          ilike(dsarRequests.requesterName, needle),
          ilike(dsarRequests.requesterEmail, needle),
          ilike(dsarRequests.subjectNameSnapshot, needle),
          ilike(dsarRequests.subjectEmailSnapshot, needle),
          ilike(dsarRequests.requestSummary, needle),
        )!,
      );
    }

    const rows = await db
      .select()
      .from(dsarRequests)
      .where(and(...filters))
      .orderBy(desc(dsarRequests.updatedAt), desc(dsarRequests.createdAt));

    res.status(200).json({ requests: rows.map(serializeDsarRequest) });
  } catch (err) {
    next(err);
  }
});

adminDsarRouter.post("/", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = createDsarRequestSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Anfrage.", code: "invalid_payload", issues: parsed.error.issues });
      return;
    }

    const body = parsed.data;
    const [subjectUser] = body.subjectUserId
      ? await db.select().from(users).where(eq(users.id, body.subjectUserId)).limit(1)
      : [null];
    const [requesterUser] = body.requesterUserId
      ? await db.select().from(users).where(eq(users.id, body.requesterUserId)).limit(1)
      : [null];

    const now = new Date();
    const dueAt = addOneCalendarMonth(now);
    const requesterName = body.requesterName || (requesterUser ? `${requesterUser.firstName} ${requesterUser.lastName}`.trim() : "");
    const requesterEmail = body.requesterEmail || requesterUser?.email;
    const subjectName = body.subjectName || (subjectUser ? `${subjectUser.firstName} ${subjectUser.lastName}`.trim() : "");
    const subjectEmail = body.subjectEmail || subjectUser?.email;

    if (!requesterEmail || !subjectName || !subjectEmail) {
      res.status(400).json({
        error: "Antragsteller und betroffene Person müssen eindeutig befüllt sein.",
        code: "missing_subject_or_requester",
      });
      return;
    }

    const [created] = await db
      .insert(dsarRequests)
      .values({
        requestType: body.requestType,
        status: "open",
        intakeChannel: body.intakeChannel ?? "email",
        requesterName,
        requesterEmail,
        requesterUserId: body.requesterUserId ?? null,
        subjectUserId: body.subjectUserId ?? null,
        subjectNameSnapshot: subjectName,
        subjectEmailSnapshot: subjectEmail,
        subjectRoleSnapshot: body.subjectRole ?? subjectUser?.role ?? null,
        requestSummary: body.requestSummary ?? "",
        assignedToUserId: body.assignedToUserId ?? null,
        receivedAt: now,
        dueAt,
        createdByUserId: req.authUser?.appUserId ?? null,
      })
      .returning();
    if (!created) {
      throw new Error("Failed to create DSAR request.");
    }

    await insertRequestEvent(
      created.id,
      req.authUser?.appUserId ?? null,
      "created",
      "DSAR-Anfrage wurde angelegt.",
      { requestType: created.requestType, dueAt: created.dueAt.toISOString() },
    );

    res.status(201).json({ request: serializeDsarRequest(created) });
  } catch (err) {
    next(err);
  }
});

adminDsarRouter.patch("/:id", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = updateDsarRequestSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Anfrage.", code: "invalid_payload", issues: parsed.error.issues });
      return;
    }

    const requestId = typeof req.params.id === "string" ? req.params.id : null;
    if (!requestId) {
      res.status(400).json({ error: "Ungültige Anfrage-ID.", code: "invalid_request_id" });
      return;
    }
    const existing = await readRequestOrNull(requestId);
    if (!existing) {
      res.status(404).json({ error: "Datenschutzanfrage nicht gefunden.", code: "dsar_not_found" });
      return;
    }

    const body = parsed.data;
    const updateValues: Partial<typeof dsarRequests.$inferInsert> = {
      updatedAt: new Date(),
    };
    const changed: string[] = [];

    if (body.status !== undefined && body.status !== existing.status) {
      updateValues.status = body.status;
      changed.push("status");
    }
    if (body.assignedToUserId !== undefined && body.assignedToUserId !== existing.assignedToUserId) {
      updateValues.assignedToUserId = body.assignedToUserId;
      changed.push("assignedToUserId");
    }
    if (body.extendedUntil !== undefined) {
      updateValues.extendedUntil = body.extendedUntil ? new Date(body.extendedUntil) : null;
      changed.push("extendedUntil");
    }
    if (body.extensionReason !== undefined) {
      updateValues.extensionReason = body.extensionReason;
      changed.push("extensionReason");
    }
    if (body.decisionSummary !== undefined) {
      updateValues.decisionSummary = body.decisionSummary;
      changed.push("decisionSummary");
    }
    if (body.legalBlockers !== undefined) {
      updateValues.legalBlockers = body.legalBlockers;
      changed.push("legalBlockers");
    }
    if (body.responseChannel !== undefined) {
      updateValues.responseChannel = body.responseChannel;
      changed.push("responseChannel");
    }
    if (body.responseSentAt !== undefined) {
      updateValues.responseSentAt = body.responseSentAt ? new Date(body.responseSentAt) : null;
      updateValues.responseSentByUserId = body.responseSentAt ? req.authUser?.appUserId ?? null : null;
      changed.push("responseSentAt");
    }
    if (body.exportPackageSummary !== undefined) {
      updateValues.exportPackageSummary = body.exportPackageSummary;
      changed.push("exportPackageSummary");
    }
    if (body.identityVerified && !existing.identityVerifiedAt) {
      updateValues.identityVerifiedAt = new Date();
      updateValues.identityVerifiedByUserId = req.authUser?.appUserId ?? null;
      changed.push("identityVerified");
    }

    const [updated] = await db
      .update(dsarRequests)
      .set(updateValues)
      .where(and(eq(dsarRequests.id, existing.id), eq(dsarRequests.isDeleted, false)))
      .returning();
    if (!updated) {
      res.status(404).json({ error: "Datenschutzanfrage nicht gefunden.", code: "dsar_not_found" });
      return;
    }

    await insertRequestEvent(
      existing.id,
      req.authUser?.appUserId ?? null,
      "updated",
      changed.length > 0 ? `Geändert: ${changed.join(", ")}` : "Datenschutzanfrage gespeichert.",
      { changed },
    );

    res.status(200).json({ request: serializeDsarRequest(updated) });
  } catch (err) {
    next(err);
  }
});

adminDsarRouter.get("/:id/events", async (req, res, next) => {
  try {
    const requestId = typeof req.params.id === "string" ? req.params.id : null;
    if (!requestId) {
      res.status(400).json({ error: "Ungültige Anfrage-ID.", code: "invalid_request_id" });
      return;
    }
    const existing = await readRequestOrNull(requestId);
    if (!existing) {
      res.status(404).json({ error: "Datenschutzanfrage nicht gefunden.", code: "dsar_not_found" });
      return;
    }
    const rows = await db
      .select()
      .from(dsarRequestEvents)
      .where(eq(dsarRequestEvents.requestId, existing.id))
      .orderBy(desc(dsarRequestEvents.createdAt));
    res.status(200).json({ events: rows.map(serializeDsarEvent) });
  } catch (err) {
    next(err);
  }
});

adminDsarRouter.post("/:id/events", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = addDsarEventSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültiger Eintrag.", code: "invalid_payload", issues: parsed.error.issues });
      return;
    }
    const requestId = typeof req.params.id === "string" ? req.params.id : null;
    if (!requestId) {
      res.status(400).json({ error: "Ungültige Anfrage-ID.", code: "invalid_request_id" });
      return;
    }
    const existing = await readRequestOrNull(requestId);
    if (!existing) {
      res.status(404).json({ error: "Datenschutzanfrage nicht gefunden.", code: "dsar_not_found" });
      return;
    }
    const event = await insertRequestEvent(
      existing.id,
      req.authUser?.appUserId ?? null,
      parsed.data.eventType,
      parsed.data.message ?? "",
      parsed.data.metadata,
    );
    res.status(201).json({ event: serializeDsarEvent(event) });
  } catch (err) {
    next(err);
  }
});

adminDsarRouter.get("/:id/package", async (req, res, next) => {
  try {
    const requestId = typeof req.params.id === "string" ? req.params.id : null;
    if (!requestId) {
      res.status(400).json({ error: "Ungültige Anfrage-ID.", code: "invalid_request_id" });
      return;
    }
    const existing = await readRequestOrNull(requestId);
    if (!existing) {
      res.status(404).json({ error: "Datenschutzanfrage nicht gefunden.", code: "dsar_not_found" });
      return;
    }
    if (!existing.subjectUserId) {
      res.status(200).json({
        package: {
          generatedAt: new Date().toISOString(),
          subject: null,
          categories: [],
          limitations: ["Keine interne Nutzer-ID hinterlegt. Paket muss manuell über E-Mail/Name zusammengestellt werden."],
        },
      });
      return;
    }
    const dataPackage = await buildSubjectDataPackage(existing.subjectUserId);
    res.status(200).json({ package: dataPackage });
  } catch (err) {
    next(err);
  }
});
