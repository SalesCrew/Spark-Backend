import { randomUUID } from "node:crypto";
import { and, asc, desc, eq, inArray, isNull, sql } from "drizzle-orm";
import { Router, type Response } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { smSMDurcharbeitCampaigns as campaigns, smSMDurcharbeitCampaignMarkets as members,
  smSMDurcharbeitPeriods as periods, smSMDurcharbeitTargets as targets, smSMDurcharbeitOwnerRevisions as owners,
  smSMDurcharbeitVisits as visits, smSMDurcharbeitEvents as events, smQuestionnaireSubmissions as submissions,
  smMarkets, smSMDurcharbeitMarkets, smQuestionnaireVersions, smQuestionnaireTemplates, users } from "../lib/schema.js";
import { SMDurcharbeitCampaignError, SMDurcharbeitToday, SMDurcharbeitMonth, SMDurcharbeitMonths, SMDurcharbeitEvent,
  SMDurcharbeitPublicationPreview, publishSMDurcharbeitCampaign, listSMDurcharbeitTargets, loadSMDurcharbeitTarget,
  assertSMDurcharbeitAvailable, loadSMDurcharbeitVersion, editSMDurcharbeitTarget } from "../sm-SMDurcharbeit-campaign.shared.js";
import { initializeSMDurcharbeitVisit, cancelSMDurcharbeitDraft, cleanupSMDurcharbeitDraftPhotos } from "./sm-visits.js";
import { isIsoDate } from "../sm-planning.shared.js";
import { loadSMDurcharbeitReport } from "../sm-SMDurcharbeit-report.shared.js";

const date = z.string().refine(isIsoDate, "Ungültiges Datum.");
const uuid = z.string().uuid();
const roster = z.array(z.object({ smMarketId: uuid, smUserId: uuid.nullable() }).strict()).max(5000)
  .refine(rows => new Set(rows.map(row => row.smMarketId)).size === rows.length, "Märkte dürfen nur einmal ausgewählt werden.");
const draftSchema = z.object({ name: z.string().trim().min(1).max(200), startDate: date, endDate: date,
  questionnaireVersionId: uuid, rosterDraft: roster }).strict().refine(input => input.endDate >= input.startDate, "Das Enddatum liegt vor dem Beginn.");
const updateSchema = draftSchema.extend({ expectedRevision: z.number().int().positive() }).strict();
const startSchema = z.object({ expectedRevision: z.number().int().positive(), followUp: z.boolean(), mode: z.enum(["timer", "manual"]),
  travelMinutes: z.number().int().min(0).max(1440).nullable().optional(), clientSubmissionToken: z.string().trim().min(8).max(300) }).strict();
function id(req: AuthedRequest, key: string) { return uuid.parse(req.params[key]); }
function actor(req: AuthedRequest) { if (!req.authUser) throw new SMDurcharbeitCampaignError(401, "auth_required", "Anmeldung erforderlich."); return req.authUser.appUserId; }
function errorResponse(error: unknown, res: Response) {
  if (error instanceof z.ZodError) { res.status(400).json({ error: "Bitte die Eingaben prüfen.", code: "smdurcharbeit_input_invalid", details: { issues: error.issues } }); return true; }
  if (error instanceof SMDurcharbeitCampaignError) { res.status(error.statusCode).json({ error: error.message, code: error.code }); return true; }
  return false;
}
async function lockedCampaign(tx: Parameters<Parameters<typeof db.transaction>[0]>[0], campaignId: string, revision: number) {
  const [campaign] = await tx.select().from(campaigns).where(eq(campaigns.id, campaignId)).limit(1).for("update");
  if (!campaign) throw new SMDurcharbeitCampaignError(404, "smdurcharbeit_campaign_missing", "Kampagne nicht gefunden.");
  if (campaign.revision !== revision) throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_campaign_stale", "Die Kampagne wurde geändert. Bitte neu laden.");
  return campaign;
}

export const adminSMDurcharbeitCampaignsRouter = Router();
adminSMDurcharbeitCampaignsRouter.use(requireAuth(["admin", "sm_admin"]));
adminSMDurcharbeitCampaignsRouter.get("/options", async (_req, res, next) => {
  try {
    const [markets, people, questionnaires] = await Promise.all([
      db.select({ id: smMarkets.id, name: smMarkets.name, address: smMarkets.address, postalCode: smMarkets.postalCode, city: smMarkets.city,
        region: smMarkets.region, chain: smMarkets.chain,
        assignedSmUserId: sql<string | null>`coalesce(${smSMDurcharbeitMarkets.SMDurcharbeitSmUserId}, ${smMarkets.assignedSmUserId})`,
        sourcePerson: smSMDurcharbeitMarkets.SMDurcharbeitVerplanung })
        .from(smSMDurcharbeitMarkets).innerJoin(smMarkets, eq(smMarkets.id, smSMDurcharbeitMarkets.smMarketId))
        .where(and(eq(smMarkets.isDeleted, false), eq(smMarkets.isActive, true))).orderBy(asc(smMarkets.name), asc(smMarkets.id)),
      db.select({ id: users.id, firstName: users.firstName, lastName: users.lastName }).from(users).where(and(eq(users.role, "sm"), eq(users.isActive, true), isNull(users.deletedAt))).orderBy(asc(users.lastName), asc(users.id)),
      db.select({ id: smQuestionnaireVersions.id, templateId: smQuestionnaireTemplates.id, name: smQuestionnaireVersions.name, versionNumber: smQuestionnaireVersions.versionNumber,
        effectiveFrom: smQuestionnaireVersions.effectiveFrom, effectiveTo: smQuestionnaireVersions.effectiveTo })
        .from(smQuestionnaireVersions).innerJoin(smQuestionnaireTemplates, eq(smQuestionnaireTemplates.id, smQuestionnaireVersions.questionnaireTemplateId))
        .where(and(sql`${smQuestionnaireTemplates.stableCode} like ${"smdurcharbeit\\_%"} escape ${"\\"}`, eq(smQuestionnaireTemplates.status, "active"), eq(smQuestionnaireTemplates.isDeleted, false),
          eq(smQuestionnaireVersions.status, "published"), eq(smQuestionnaireVersions.isDeleted, false))).orderBy(asc(smQuestionnaireVersions.name), desc(smQuestionnaireVersions.versionNumber)),
    ]);
    res.json({ markets, people, questionnaires });
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.get("/", async (_req, res, next) => {
  try { res.json({ campaigns: await db.select().from(campaigns).orderBy(desc(campaigns.startDate), asc(campaigns.name)) }); }
  catch (error) { next(error); }
});
adminSMDurcharbeitCampaignsRouter.post("/", async (req: AuthedRequest, res, next) => {
  try {
    const input = draftSchema.parse(req.body); SMDurcharbeitMonths(input.startDate, input.endDate);
    const created = await db.transaction(async tx => {
      await loadSMDurcharbeitVersion(tx, input.questionnaireVersionId);
      const [campaign] = await tx.insert(campaigns).values({ ...input, createdByUserId: actor(req), updatedByUserId: actor(req) }).returning();
      await SMDurcharbeitEvent(tx, { campaignId: campaign!.id, actorUserId: actor(req), action: "draft_created", reason: "Kampagnenentwurf erstellt" });
      return campaign!;
    });
    res.status(201).json({ campaign: created });
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.patch("/:campaignId", async (req: AuthedRequest, res, next) => {
  try {
    const { expectedRevision, ...input } = updateSchema.parse(req.body); SMDurcharbeitMonths(input.startDate, input.endDate);
    const result = await db.transaction(async tx => {
      const campaign = await lockedCampaign(tx, id(req, "campaignId"), expectedRevision);
      if (campaign.status !== "draft") throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_campaign_not_draft", "Veröffentlichte Kampagnen werden über ihre Monatsziele bearbeitet.");
      await loadSMDurcharbeitVersion(tx, input.questionnaireVersionId);
      const [updated] = await tx.update(campaigns).set({ ...input, revision: expectedRevision + 1, updatedByUserId: actor(req), updatedAt: new Date() }).where(eq(campaigns.id, campaign.id)).returning();
      await SMDurcharbeitEvent(tx, { campaignId: campaign.id, actorUserId: actor(req), action: "draft_updated", reason: "Kampagnenentwurf bearbeitet" });
      return updated;
    }); res.json({ campaign: result });
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.get("/:campaignId/preview", async (req: AuthedRequest, res, next) => {
  try { res.json(await SMDurcharbeitPublicationPreview(db, id(req, "campaignId"))); }
  catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.post("/:campaignId/publish", async (req: AuthedRequest, res, next) => {
  try {
    const input = z.object({ expectedRevision: z.number().int().positive(), previewToken: z.string().length(64), confirmOverlap: z.boolean().optional() }).strict().parse(req.body);
    await db.transaction(tx => publishSMDurcharbeitCampaign(tx, id(req, "campaignId"), actor(req), input)); res.json({ published: true });
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.get("/:campaignId/targets", async (req: AuthedRequest, res, next) => {
  try {
    const rows = await listSMDurcharbeitTargets(db, { campaignId: id(req, "campaignId"), month: date.parse(req.query.month ?? SMDurcharbeitMonth()) });
    res.json({ targets: rows, summary: { required: rows.filter(r => r.eligibility === "required").length,
      completed: rows.filter(r => r.eligibility === "required" && r.completed).length, waived: rows.filter(r => r.eligibility === "waived").length,
      physicalVisits: rows.reduce((sum, r) => sum + r.visitCount, 0) } });
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.get("/:campaignId/periods", async (req: AuthedRequest, res, next) => {
  try { res.json({ periods: await db.select().from(periods).where(eq(periods.campaignId, id(req, "campaignId"))).orderBy(asc(periods.month)) }); }
  catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.patch("/targets/:targetId", async (req: AuthedRequest, res, next) => {
  try {
    const input = z.object({ expectedRevision: z.number().int().positive(), reason: z.string().trim().min(3).max(1000), scope: z.enum(["month", "future"]),
      smUserId: uuid.optional(), eligibility: z.enum(["required", "waived"]).optional() }).strict().refine(input => input.smUserId !== undefined || input.eligibility !== undefined).parse(req.body);
    await db.transaction(tx => editSMDurcharbeitTarget(tx, id(req, "targetId"), actor(req), input)); res.json({ updated: true });
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.post("/targets/:targetId/cancel-draft", async (req: AuthedRequest, res, next) => {
  try {
    const input = z.object({ expectedRevision: z.number().int().positive(), visitId: uuid, reason: z.string().trim().min(3).max(1000),
      confirmation: z.literal("CANCEL_SMDURCHARBEIT_DRAFT") }).strict().parse(req.body);
    const result = await db.transaction(tx => cancelSMDurcharbeitDraft(tx, id(req, "targetId"), actor(req), input));
    await cleanupSMDurcharbeitDraftPhotos(result.photoRows);
    res.json({ cancelled: true });
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.patch("/:campaignId/state", async (req: AuthedRequest, res, next) => {
  try {
    const input = z.object({ expectedRevision: z.number().int().positive(), status: z.enum(["published", "paused", "archived"]), reason: z.string().trim().min(3).max(1000) }).strict().parse(req.body);
    await db.transaction(async tx => {
      const campaign = await lockedCampaign(tx, id(req, "campaignId"), input.expectedRevision);
      if (campaign.status === "draft" && input.status !== "archived") throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_publication_required", "Bitte zuerst die Kampagnenvorschau prüfen und veröffentlichen.");
      if (campaign.status === "archived") throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_campaign_archived", "Archivierte Kampagnen bleiben im Verlauf.");
      await tx.update(campaigns).set({ status: input.status, revision: input.expectedRevision + 1, updatedAt: new Date(), updatedByUserId: actor(req) }).where(eq(campaigns.id, campaign.id));
      await SMDurcharbeitEvent(tx, { campaignId: campaign.id, actorUserId: actor(req), action: "state_changed", reason: input.reason, beforeState: { status: campaign.status }, afterState: { status: input.status } });
    }); res.json({ updated: true });
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.post("/:campaignId/extend", async (req: AuthedRequest, res, next) => {
  try {
    const input = z.object({ expectedRevision: z.number().int().positive(), endDate: date, reason: z.string().trim().min(3).max(1000), reactivate: z.boolean() }).strict().parse(req.body);
    await db.transaction(async tx => {
      const campaign = await lockedCampaign(tx, id(req, "campaignId"), input.expectedRevision);
      if (!["published", "paused"].includes(campaign.status) || input.endDate <= campaign.endDate || input.endDate < SMDurcharbeitToday()) throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_extension_invalid", "Bitte ein späteres Enddatum für eine veröffentlichte Kampagne wählen.");
      const months = SMDurcharbeitMonths(campaign.startDate, input.endDate);
      const existingPeriods = await tx.select().from(periods).where(eq(periods.campaignId, campaign.id)).orderBy(desc(periods.month));
      const currentMonths = new Set(existingPeriods.map(p => p.month)), latestPeriod = existingPeriods[0];
      if (!latestPeriod) throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_period_missing", "Die Kampagnenmonate fehlen.");
      const lastTargets = await tx.select({ target: targets, owner: owners }).from(targets).innerJoin(owners, eq(owners.id, targets.ownerRevisionId)).where(eq(targets.periodId, latestPeriod.id)).orderBy(asc(targets.id));
      for (const month of months.filter(m => !currentMonths.has(m))) {
        const [period] = await tx.insert(periods).values({ campaignId: campaign.id, month, questionnaireVersionId: latestPeriod.questionnaireVersionId }).returning();
        const ownerRows = lastTargets.map(previous => ({ id: randomUUID(), campaignMarketId: previous.target.campaignMarketId, month,
          smUserId: previous.owner.smUserId, sourcePerson: previous.owner.sourcePerson, reason: input.reason, actorUserId: actor(req) }));
        if (ownerRows.length) {
          await tx.insert(owners).values(ownerRows);
          await tx.insert(targets).values(lastTargets.map((previous, index) => ({ campaignId: campaign.id, periodId: period!.id,
            campaignMarketId: previous.target.campaignMarketId, ownerRevisionId: ownerRows[index]!.id,
            marketSnapshot: previous.target.marketSnapshot, eligibility: previous.target.eligibility, waiverReason: previous.target.waiverReason })));
        }
      }
      await tx.update(campaigns).set({ endDate: input.endDate, status: input.reactivate ? "published" : campaign.status, revision: input.expectedRevision + 1, updatedAt: new Date(), updatedByUserId: actor(req) }).where(eq(campaigns.id, campaign.id));
      await SMDurcharbeitEvent(tx, { campaignId: campaign.id, actorUserId: actor(req), action: "extended", reason: input.reason,
        beforeState: { endDate: campaign.endDate, status: campaign.status }, afterState: { endDate: input.endDate, status: input.reactivate ? "published" : campaign.status, addedMonths: months.filter(m => !currentMonths.has(m)) } });
    }); res.json({ extended: true });
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.patch("/:campaignId/periods/:periodId/questionnaire", async (req: AuthedRequest, res, next) => {
  try {
    const input = z.object({ expectedRevision: z.number().int().positive(), questionnaireVersionId: uuid, reason: z.string().trim().min(3).max(1000) }).strict().parse(req.body);
    await db.transaction(async tx => {
      const campaign = await lockedCampaign(tx, id(req, "campaignId"), input.expectedRevision);
      const [period] = await tx.select().from(periods).where(and(eq(periods.id, id(req, "periodId")), eq(periods.campaignId, campaign.id))).limit(1);
      const [started] = period ? await tx.select({ id: visits.id }).from(visits).innerJoin(targets, eq(targets.id, visits.targetId)).where(eq(targets.periodId, period.id)).limit(1) : [];
      if (!period || period.month <= SMDurcharbeitMonth() || started) throw new SMDurcharbeitCampaignError(409, "smdurcharbeit_period_frozen", "Nur zukünftige, noch nicht gestartete Monate können einen neuen Fragebogen erhalten.");
      await loadSMDurcharbeitVersion(tx, input.questionnaireVersionId);
      await tx.update(periods).set({ questionnaireVersionId: input.questionnaireVersionId }).where(eq(periods.id, period.id));
      await tx.update(campaigns).set({ revision: input.expectedRevision + 1, updatedAt: new Date(), updatedByUserId: actor(req) }).where(eq(campaigns.id, campaign.id));
      await SMDurcharbeitEvent(tx, { campaignId: campaign.id, actorUserId: actor(req), action: "future_questionnaire_changed", reason: input.reason, beforeState: { periodId: period.id, versionId: period.questionnaireVersionId }, afterState: { versionId: input.questionnaireVersionId } });
    }); res.json({ updated: true });
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.get("/:campaignId/history", async (req: AuthedRequest, res, next) => {
  try {
    const input = z.object({ limit: z.coerce.number().int().min(1).max(100).default(50), beforeCreatedAt: z.iso.datetime({ offset: true }).optional(), beforeId: uuid.optional() }).strict()
      .refine(value => Boolean(value.beforeCreatedAt) === Boolean(value.beforeId)).parse(req.query);
    const rows = await db.select({ event: events, firstName: users.firstName, lastName: users.lastName }).from(events)
      .leftJoin(users, eq(users.id, events.actorUserId)).where(and(eq(events.campaignId, id(req, "campaignId")),
        input.beforeCreatedAt && input.beforeId ? sql`(${events.createdAt}, ${events.id}) < (${input.beforeCreatedAt}::timestamptz, ${input.beforeId}::uuid)` : undefined))
      .orderBy(desc(events.createdAt), desc(events.id)).limit(input.limit + 1);
    const page = rows.slice(0, input.limit), last = page.at(-1);
    res.json({ events: page.map(row => ({ ...row.event, actorName: [row.firstName, row.lastName].filter(Boolean).join(" ") || null })),
      nextCursor: rows.length > input.limit && last ? { beforeCreatedAt: last.event.createdAt.toISOString(), beforeId: last.event.id } : null });
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});
adminSMDurcharbeitCampaignsRouter.get("/:campaignId/results", async (req: AuthedRequest, res, next) => {
  try {
    const input = z.object({ month: date.optional(), smUserId: uuid.optional() }).strict().parse(req.query);
    const campaignId = id(req, "campaignId"), month = SMDurcharbeitMonth(input.month ?? SMDurcharbeitMonth());
    res.json(await db.transaction(tx => loadSMDurcharbeitReport(tx, { campaignId, month, ...(input.smUserId ? { smUserId: input.smUserId } : {}) }),
      { isolationLevel: "repeatable read", accessMode: "read only" }));
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});

export const SMDurcharbeitCampaignsRouter = Router();
SMDurcharbeitCampaignsRouter.use(requireAuth(["sm"]));
SMDurcharbeitCampaignsRouter.get("/targets", async (req: AuthedRequest, res, next) => {
  try {
    const input = z.object({ month: date.optional() }).strict().parse(req.query);
    const currentMonth = SMDurcharbeitMonth(), month = SMDurcharbeitMonth(input.month ?? currentMonth), smUserId = actor(req);
    const [monthlyTargets, ownedMonths] = await Promise.all([
      listSMDurcharbeitTargets(db, { smUserId, month }),
      db.selectDistinct({ month: periods.month }).from(targets).innerJoin(periods, eq(periods.id, targets.periodId))
        .innerJoin(campaigns, eq(campaigns.id, targets.campaignId)).innerJoin(owners, eq(owners.id, targets.ownerRevisionId))
        .where(and(eq(owners.smUserId, smUserId), sql`${campaigns.status} <> 'draft'`)).orderBy(asc(periods.month)),
    ]);
    res.json({ month, currentMonth, months: ownedMonths.map(period => period.month), targets: monthlyTargets });
  }
  catch (error) { if (!errorResponse(error, res)) next(error); }
});
SMDurcharbeitCampaignsRouter.get("/targets/:targetId", async (req: AuthedRequest, res, next) => {
  try {
    const context = await loadSMDurcharbeitTarget(db, id(req, "targetId"));
    if (context.owner.smUserId !== actor(req)) throw new SMDurcharbeitCampaignError(403, "smdurcharbeit_target_forbidden", "Dieser Markt gehört einem anderen SM.");
    const rows = await listSMDurcharbeitTargets(db, { campaignId: context.campaign.id, month: context.period.month, smUserId: actor(req) });
    const [version] = await db.select({ name: smQuestionnaireVersions.name, versionNumber: smQuestionnaireVersions.versionNumber }).from(smQuestionnaireVersions).where(eq(smQuestionnaireVersions.id, context.period.questionnaireVersionId)).limit(1);
    const [user] = await db.select({ travelTimeEnabled: users.travelTimeEnabled }).from(users).where(eq(users.id, actor(req))).limit(1);
    res.json({ target: rows.find(row => row.id === context.target.id), questionnaire: version, profile: user });
  } catch (error) { if (!errorResponse(error, res)) next(error); }
});
SMDurcharbeitCampaignsRouter.post("/targets/:targetId/start", async (req: AuthedRequest, res, next) => {
  try { const input = startSchema.parse(req.body); const result = await db.transaction(tx => initializeSMDurcharbeitVisit(tx, id(req, "targetId"), actor(req), input)); res.status(result.replayed ? 200 : 201).json(result); }
  catch (error) { if (!errorResponse(error, res)) next(error); }
});
