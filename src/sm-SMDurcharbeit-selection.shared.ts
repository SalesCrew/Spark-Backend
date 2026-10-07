import { and, desc, eq, isNull, ne, or, sql } from "drizzle-orm";
import type { db } from "./lib/db.js";
import { smAssignments, smSMDurcharbeitMarkets, smQuestionnaireGlobalAssignments, smQuestionnaireSubmissions, smQuestionnaireTemplates, smQuestionnaireVersions } from "./lib/schema.js";
import { resolveSmAssignmentValues } from "./sm-planning.shared.js";
import { smQuestionnaireCatalogScope, type SmQuestionnaireCatalogScope } from "./sm-SMDurcharbeit-catalog.shared.js";

type Executor = Pick<typeof db, "select">;
type Assignment = typeof smAssignments.$inferSelect;
type Submission = Pick<typeof smQuestionnaireSubmissions.$inferSelect, "id" | "questionnaireVersionId" | "questionnaireNameSnapshot" | "questionnaireVersionSnapshot">;

export class SMDurcharbeitSelectionError extends Error {
  constructor(public readonly statusCode: number, public readonly code: string, message: string, public readonly details?: Record<string, unknown>) { super(message); }
}

export type SMDurcharbeitQuestionnaireSelection = {
  questionnaireTemplateId: string | null;
  questionnaireVersionId: string | null;
  name: string | null;
  versionNumber: number | null;
  catalogScope: SmQuestionnaireCatalogScope | null;
  source: "submission" | "override" | "central" | "legacy";
  available: boolean;
  blockReason: string | null;
  revision: string;
};

/** Includes historical roots/versions for identity, even after catalog archival. Never writes. */
export async function loadSMDurcharbeitSelectionCatalog(executor: Executor) {
  const [versions, globals] = await Promise.all([
    executor.select({
      id: smQuestionnaireVersions.id,
      questionnaireTemplateId: smQuestionnaireVersions.questionnaireTemplateId,
      versionNumber: smQuestionnaireVersions.versionNumber,
      name: smQuestionnaireVersions.name,
      description: smQuestionnaireVersions.description,
      oncePerMarket: smQuestionnaireVersions.oncePerMarket,
      timezone: smQuestionnaireVersions.timezone,
      effectiveFrom: smQuestionnaireVersions.effectiveFrom,
      effectiveTo: smQuestionnaireVersions.effectiveTo,
      status: smQuestionnaireVersions.status,
      isDeleted: smQuestionnaireVersions.isDeleted,
      templateStatus: smQuestionnaireTemplates.status,
      templateDeleted: smQuestionnaireTemplates.isDeleted,
      stableCode: smQuestionnaireTemplates.stableCode,
      hasQuestions: sql<boolean>`exists (
        select 1 from sm_questionnaire_version_modules vm
        join sm_module_version_questions mq on mq.module_version_id = vm.module_version_id
        where vm.questionnaire_version_id = ${smQuestionnaireVersions.id}
          and vm.is_deleted = false and mq.is_deleted = false
      )`,
    }).from(smQuestionnaireVersions)
      .innerJoin(smQuestionnaireTemplates, eq(smQuestionnaireTemplates.id, smQuestionnaireVersions.questionnaireTemplateId))
      .orderBy(desc(smQuestionnaireVersions.versionNumber)),
    executor.select({ questionnaireTemplateId: smQuestionnaireGlobalAssignments.questionnaireTemplateId })
      .from(smQuestionnaireGlobalAssignments)
      .where(and(eq(smQuestionnaireGlobalAssignments.isDeleted, false), isNull(smQuestionnaireGlobalAssignments.supersededAt))).limit(1),
  ]);
  return { versions, centralTemplateId: globals[0]?.questionnaireTemplateId ?? null };
}
export type SMDurcharbeitSelectionCatalog = Awaited<ReturnType<typeof loadSMDurcharbeitSelectionCatalog>>;
type Version = SMDurcharbeitSelectionCatalog["versions"][number];

export function SMDurcharbeitVersionAvailable(version: Version, date: string) {
  return version.status === "published" && !version.isDeleted && version.templateStatus === "active" && !version.templateDeleted
    && (!version.effectiveFrom || version.effectiveFrom <= date) && (!version.effectiveTo || version.effectiveTo >= date);
}

export function resolveSMDurcharbeitSelection(catalog: SMDurcharbeitSelectionCatalog, assignment: Assignment, submission?: Submission | null) {
  const date = resolveSmAssignmentValues(assignment).workDate;
  let source: SMDurcharbeitQuestionnaireSelection["source"] = "legacy";
  let version: Version | undefined;
  let reason: string | null = null;
  let candidates: Version[] = [];
  if (submission) {
    source = "submission";
    version = catalog.versions.find(row => row.id === submission.questionnaireVersionId);
  } else if (assignment.SMDurcharbeitQuestionnaireOverrideVersionId) {
    source = "override";
    version = catalog.versions.find(row => row.id === assignment.SMDurcharbeitQuestionnaireOverrideVersionId);
    if (!version || !SMDurcharbeitVersionAvailable(version, date) || !version.hasQuestions) reason = "Der ausgewählte Fragebogen ist für diesen Einsatztag nicht verfügbar. Bitte die Verplanung prüfen.";
  } else {
    candidates = catalog.versions.filter(row => SMDurcharbeitVersionAvailable(row, date));
    if (catalog.centralTemplateId) {
      source = "central";
      version = candidates.find(row => row.questionnaireTemplateId === catalog.centralTemplateId);
      if (!version) reason = "Der zentrale Fragebogen ist für diesen Einsatztag nicht aktiv oder veröffentlicht.";
    } else if (assignment.questionnaireVersionId) {
      version = candidates.find(row => row.id === assignment.questionnaireVersionId);
      if (!version) reason = "Der geplante Fragebogen ist für diesen Einsatztag nicht verfügbar.";
    } else if (candidates.length === 1) version = candidates[0];
    else reason = candidates.length ? "Mehrere Fragebögen sind gültig. Bitte dem Einsatz einen konkreten Fragebogen zuordnen." : "Für diesen Einsatz ist kein veröffentlichter Fragebogen verfügbar.";
  }
  const selection: SMDurcharbeitQuestionnaireSelection = {
    questionnaireTemplateId: version?.questionnaireTemplateId ?? null,
    questionnaireVersionId: submission?.questionnaireVersionId ?? version?.id ?? null,
    name: submission?.questionnaireNameSnapshot ?? version?.name ?? null,
    versionNumber: submission?.questionnaireVersionSnapshot ?? version?.versionNumber ?? null,
    catalogScope: version ? smQuestionnaireCatalogScope(version.stableCode) : null,
    source, available: !reason && Boolean(version), blockReason: reason,
    revision: [assignment.id, assignment.updatedAt.toISOString(), submission?.id ?? "", source, version?.id ?? "", reason ?? ""].join(":"),
  };
  return { selection, version, count: selection.available ? 1 : source === "legacy" && !assignment.questionnaireVersionId ? candidates.length : 0 };
}

export async function assertSMDurcharbeitOverride(executor: Executor, assignment: Assignment, catalog?: SMDurcharbeitSelectionCatalog) {
  const [SMDurcharbeitMarket] = await executor.select({ id: smSMDurcharbeitMarkets.smMarketId }).from(smSMDurcharbeitMarkets)
    .where(eq(smSMDurcharbeitMarkets.smMarketId, resolveSmAssignmentValues(assignment).smMarketId)).limit(1);
  if (SMDurcharbeitMarket && !assignment.SMDurcharbeitQuestionnaireOverrideVersionId) {
    throw new SMDurcharbeitSelectionError(409, "smdurcharbeit_market_questionnaire_required", "Für diesen Durcharbeit-Markt bitte einen Durcharbeit-Fragebogen auswählen.");
  }
  if (!assignment.SMDurcharbeitQuestionnaireOverrideVersionId) return;
  const resolution = resolveSMDurcharbeitSelection(catalog ?? await loadSMDurcharbeitSelectionCatalog(executor), assignment);
  if (!resolution.selection.available || !resolution.version) throw new SMDurcharbeitSelectionError(409, "smdurcharbeit_questionnaire_unavailable", resolution.selection.blockReason ?? "Der ausgewählte Fragebogen ist nicht verfügbar.");
  if (SMDurcharbeitMarket && resolution.selection.catalogScope !== "SMDurcharbeit") {
    throw new SMDurcharbeitSelectionError(409, "smdurcharbeit_market_questionnaire_required", "Durcharbeit-Märkte benötigen einen Durcharbeit-Fragebogen.");
  }
  if (resolution.version.oncePerMarket) {
    const [completed] = await executor.select({ id: smQuestionnaireSubmissions.id }).from(smQuestionnaireSubmissions).where(and(
      eq(smQuestionnaireSubmissions.questionnaireTemplateId, resolution.version.questionnaireTemplateId),
      eq(smQuestionnaireSubmissions.smMarketId, resolveSmAssignmentValues(assignment).smMarketId),
      eq(smQuestionnaireSubmissions.status, "submitted"), eq(smQuestionnaireSubmissions.isCurrent, true),
      eq(smQuestionnaireSubmissions.isDeleted, false), or(isNull(smQuestionnaireSubmissions.assignmentId), ne(smQuestionnaireSubmissions.assignmentId, assignment.id)),
    )).limit(1);
    if (completed) throw new SMDurcharbeitSelectionError(409, "smdurcharbeit_once_per_market_completed", "Dieser einmalige Fragebogen wurde für den Markt bereits abgeschlossen.");
  }
}

/** Soft deletion/status changes must not strand an explicitly planned, restorable Einsatz. */
export async function hasSMDurcharbeitPendingOverride(executor: Executor, templateId: string) {
  const [row] = await executor.select({ id: smAssignments.id }).from(smAssignments)
    .innerJoin(smQuestionnaireVersions, eq(smQuestionnaireVersions.id, smAssignments.SMDurcharbeitQuestionnaireOverrideVersionId))
    .where(and(eq(smQuestionnaireVersions.questionnaireTemplateId, templateId), eq(smAssignments.isDeleted, false),
      sql`${smAssignments.status} in ('planned', 'confirmed', 'open', 'cancelled')`,
      sql`not exists (select 1 from sm_questionnaire_submissions s where s.assignment_id = ${smAssignments.id} and s.is_current = true and s.is_deleted = false)`)).limit(1);
  return Boolean(row);
}
