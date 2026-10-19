import { createHash, randomUUID } from "node:crypto";
import { and, asc, desc, eq, inArray, sql } from "drizzle-orm";
import {
  smSMDurcharbeitCampaigns as campaigns, smSMDurcharbeitPeriods as periods,
  smSMDurcharbeitTargets as targets, smQuestionnaireSubmissions as submissions,
  smQuestionnaireVersions as versions, smQuestionnaireTemplates as templates,
  smQuestionnaireVersionModules as versionModules, smModuleVersions as moduleVersions,
  smModules as modules, smModuleVersionQuestions as moduleQuestions,
  smQuestionVersions as questionVersions, smQuestions as questions,
} from "./lib/schema.js";
import { lockSmPlanning } from "./sm-planning-lock.js";
import {
  SMDurcharbeitCampaignError, SMDurcharbeitEvent, SMDurcharbeitMonth, SMDurcharbeitToday,
  type SMDurcharbeitExecutor, type SMDurcharbeitTx,
} from "./sm-SMDurcharbeit-campaign.shared.js";

const fail = (code: string, message: string, status = 409): never => { throw new SMDurcharbeitCampaignError(status, code, message); };
const hash = (value: unknown) => createHash("sha256").update(JSON.stringify(value)).digest("hex");

async function readUpdate(executor: SMDurcharbeitExecutor, campaignId: string, month: string) {
  if (month !== SMDurcharbeitMonth(month)) return fail("smdurcharbeit_month_invalid", "Bitte den ersten Tag des Kalendermonats angeben.", 400);
  const [campaign] = await executor.select().from(campaigns).where(eq(campaigns.id, campaignId)).limit(1);
  if (!campaign) return fail("smdurcharbeit_campaign_missing", "Kampagne nicht gefunden.", 404);
  if (!["published", "paused"].includes(campaign.status) || month < SMDurcharbeitMonth() || campaign.endDate < SMDurcharbeitToday()) {
    return fail("smdurcharbeit_month_closed", "Abgeschlossene Kampagnen und Monate behalten ihren Fragebogen.");
  }
  const [period] = await executor.select().from(periods).where(and(eq(periods.campaignId, campaignId), eq(periods.month, month))).limit(1);
  if (!period) return fail("smdurcharbeit_period_missing", "Kalendermonat nicht gefunden.", 404);
  const [current] = await executor.select().from(versions).where(eq(versions.id, period.questionnaireVersionId)).limit(1);
  if (!current) return fail("smdurcharbeit_version_unavailable", "Der zugewiesene Fragebogen ist nicht verfügbar.");
  const [template] = await executor.select().from(templates).where(eq(templates.id, current.questionnaireTemplateId)).limit(1);
  if (!template || template.isDeleted || template.status !== "active" || !template.stableCode.startsWith("smdurcharbeit_")) {
    return fail("smdurcharbeit_version_unavailable", "Bitte den Durcharbeit-Fragebogen zuerst aktivieren.");
  }
  const [latest] = await executor.select().from(versions).where(and(eq(versions.questionnaireTemplateId, template.id),
    eq(versions.status, "published"), eq(versions.isDeleted, false))).orderBy(desc(versions.versionNumber)).limit(1);
  if (!latest) return fail("smdurcharbeit_version_unavailable", "Keine veröffentlichte Fragebogenfassung verfügbar.");
  const links = await executor.select({ versionId: versionModules.questionnaireVersionId, moduleVersionId: versionModules.moduleVersionId,
    moduleId: moduleVersions.moduleId, order: versionModules.orderIndex }).from(versionModules)
    .innerJoin(moduleVersions, eq(moduleVersions.id, versionModules.moduleVersionId))
    .where(and(inArray(versionModules.questionnaireVersionId, [...new Set([current.id, latest.id])]), eq(versionModules.isDeleted, false)))
    .orderBy(asc(versionModules.orderIndex));
  const oldLinks = links.filter(link => link.versionId === current.id), latestLinks = links.filter(link => link.versionId === latest.id);
  if (!latestLinks.length) return fail("smdurcharbeit_version_empty", "Die neue Fragebogenfassung enthält keine Module.");
  const newest = await executor.select({ version: moduleVersions, deleted: modules.isDeleted, code: modules.stableCode }).from(moduleVersions)
    .innerJoin(modules, eq(modules.id, moduleVersions.moduleId))
    .where(and(inArray(moduleVersions.moduleId, latestLinks.map(link => link.moduleId)), eq(moduleVersions.status, "published"), eq(moduleVersions.isDeleted, false)))
    .orderBy(desc(moduleVersions.versionNumber));
  const byModule = new Map<string, (typeof newest)[number]>();
  for (const row of newest) if (!byModule.has(row.version.moduleId)) byModule.set(row.version.moduleId, row);
  const nextModules = latestLinks.map(link => {
    const row = byModule.get(link.moduleId);
    if (!row || row.deleted || !row.code.startsWith("smdurcharbeit_")) return fail("smdurcharbeit_module_unavailable", "Ein Modul der neuen Fassung ist nicht mehr verfügbar. Bitte den Fragebogen prüfen.");
    return row.version;
  });
  const graphRows = await executor.select({ moduleVersionId: moduleQuestions.moduleVersionId, moduleId: moduleVersions.moduleId,
    questionId: questionVersions.questionId, versionId: questionVersions.id, rootDeleted: questions.isDeleted })
    .from(moduleQuestions).innerJoin(moduleVersions, eq(moduleVersions.id, moduleQuestions.moduleVersionId))
    .innerJoin(questionVersions, eq(questionVersions.id, moduleQuestions.questionVersionId)).innerJoin(questions, eq(questions.id, questionVersions.questionId))
    .where(and(inArray(moduleQuestions.moduleVersionId, [...new Set([...oldLinks.map(link => link.moduleVersionId), ...nextModules.map(module => module.id)])]),
      eq(moduleQuestions.isDeleted, false), eq(questionVersions.status, "published"), eq(questionVersions.isDeleted, false)));
  const oldIds = new Set(oldLinks.map(link => link.moduleVersionId)), nextIds = new Set(nextModules.map(module => module.id));
  const oldQuestions = graphRows.filter(row => oldIds.has(row.moduleVersionId)), nextQuestions = graphRows.filter(row => nextIds.has(row.moduleVersionId));
  if (!nextQuestions.length || nextQuestions.some(question => question.rootDeleted)) return fail("smdurcharbeit_version_empty", "Die neue Fassung enthält keine vollständigen aktiven Fragen. Bitte die Module prüfen.");
  const oldByRoot = new Map(oldQuestions.map(row => [`${row.moduleId}:${row.questionId}`, row.versionId]));
  const nextByRoot = new Map(nextQuestions.map(row => [`${row.moduleId}:${row.questionId}`, row.versionId]));
  const changes = { added: [...nextByRoot.keys()].filter(id => !oldByRoot.has(id)).length,
    removed: [...oldByRoot.keys()].filter(id => !nextByRoot.has(id)).length,
    changed: [...nextByRoot].filter(([id, version]) => oldByRoot.has(id) && oldByRoot.get(id) !== version).length };
  const requiresPublication = latestLinks.some((link, index) => link.moduleVersionId !== nextModules[index]!.id);
  const saved = await executor.select({ status: submissions.status, id: submissions.id }).from(submissions)
    .innerJoin(targets, eq(targets.id, submissions.SMDurcharbeitTargetId))
    .where(and(eq(targets.periodId, period.id), eq(submissions.isCurrent, true), eq(submissions.isDeleted, false), inArray(submissions.status, ["draft", "submitted"])));
  const preserved = { drafts: saved.filter(row => row.status === "draft").length, completed: saved.filter(row => row.status === "submitted").length };
  const available = latest.id !== current.id || requiresPublication;
  const nextVersion = requiresPublication ? latest.versionNumber + 1 : latest.versionNumber;
  const previewToken = hash({ campaignId, revision: campaign.revision, periodId: period.id, current: current.id,
    latest: latest.id, modules: nextModules.map(module => module.id), questions: nextQuestions.map(row => row.versionId).sort(), saved: saved.map(row => `${row.id}:${row.status}`).sort() });
  const preview = { campaignId, campaignRevision: campaign.revision, month, available, previewToken,
    current: { id: current.id, name: current.name, versionNumber: current.versionNumber, questionCount: oldQuestions.length },
    next: { id: requiresPublication ? null : latest.id, name: latest.name, versionNumber: nextVersion, questionCount: nextQuestions.length },
    requiresPublication, changes, preserved };
  return { campaign, period, latest, nextModules, preview };
}

/** Read-only: edits to modules are detected even before a new questionnaire was explicitly saved. */
export async function previewSMDurcharbeitQuestionnaireUpdate(executor: SMDurcharbeitExecutor, campaignId: string, month: string) {
  return (await readUpdate(executor, campaignId, month)).preview;
}

/** Only the selected month's future-start pointer changes. Existing submission graphs are never written. */
export async function applySMDurcharbeitQuestionnaireUpdate(tx: SMDurcharbeitTx, campaignId: string, actorId: string,
  input: { month: string; expectedRevision: number; previewToken: string }) {
  // Same order as authoring; visit starts serialize on the campaign lock before creating their graph.
  await lockSmPlanning(tx);
  const [locked] = await tx.select().from(campaigns).where(eq(campaigns.id, campaignId)).for("update");
  if (!locked) return fail("smdurcharbeit_campaign_missing", "Kampagne nicht gefunden.", 404);
  if (locked.revision !== input.expectedRevision) return fail("smdurcharbeit_campaign_stale", "Die Kampagne wurde geändert. Bitte die neue Fassung erneut prüfen.");
  const state = await readUpdate(tx, campaignId, input.month), { preview, latest } = state;
  if (preview.previewToken !== input.previewToken) return fail("smdurcharbeit_questionnaire_update_stale", "Fragen oder Besuche wurden geändert. Bitte die neue Fassung erneut prüfen.");
  if (!preview.available) return fail("smdurcharbeit_questionnaire_up_to_date", "Dieser Monat verwendet bereits die neueste Fassung.");
  let versionId = latest.id;
  if (preview.requiresPublication) {
    versionId = randomUUID();
    await tx.insert(versions).values({ id: versionId, questionnaireTemplateId: latest.questionnaireTemplateId,
      versionNumber: preview.next.versionNumber, status: "draft", name: latest.name, description: latest.description,
      oncePerMarket: latest.oncePerMarket, timezone: latest.timezone, effectiveFrom: latest.effectiveFrom, effectiveTo: latest.effectiveTo });
    await tx.insert(versionModules).values(state.nextModules.map((module, orderIndex) => ({ questionnaireVersionId: versionId, moduleVersionId: module.id, orderIndex })));
    await tx.update(versions).set({ status: "published", publishedAt: new Date(), publishedByUserId: actorId,
      contentHash: hash({ templateId: latest.questionnaireTemplateId, versionNumber: preview.next.versionNumber, name: latest.name,
        description: latest.description, oncePerMarket: latest.oncePerMarket, moduleVersionIds: state.nextModules.map(module => module.id) }) }).where(eq(versions.id, versionId));
    await tx.update(templates).set({ updatedByUserId: actorId }).where(eq(templates.id, latest.questionnaireTemplateId));
  }
  await tx.update(periods).set({ questionnaireVersionId: versionId }).where(eq(periods.id, state.period.id));
  await tx.update(campaigns).set({ revision: sql`${campaigns.revision}+1`, updatedAt: new Date(), updatedByUserId: actorId }).where(eq(campaigns.id, campaignId));
  await SMDurcharbeitEvent(tx, { campaignId, actorUserId: actorId, action: "questionnaire_update_adopted",
    reason: "Neueste Fragebogenfassung für neue Besuche übernommen", beforeState: { month: input.month, versionId: state.period.questionnaireVersionId },
    afterState: { month: input.month, versionId, changes: preview.changes, preserved: preview.preserved } });
  return { updated: true, month: input.month, questionnaire: { ...preview.next, id: versionId } };
}
