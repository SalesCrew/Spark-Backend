import { and, asc, eq, gte, inArray, lte, sql } from "drizzle-orm";
import { z } from "zod";
import { smAssignments, smQuestionnaireSubmissions as submissions, smQuestionnaireTemplates as templates,
  smSMDurcharbeitCampaigns as campaigns, smSMDurcharbeitTargets as targets, smSMDurcharbeitTimeRevisions as times } from "./lib/schema.js";
import { isIsoDate } from "./sm-planning.shared.js";
import { SmManagementError, type SmManagementTx } from "./sm-management.js";
import { smManagedWorkDate } from "./sm-management-list.shared.js";
import { loadSmManagementReport } from "./sm-management-report.shared.js";

const list = <T extends z.ZodType>(item: T, max: number) => z.preprocess(value => {
  if (typeof value !== "string") return value;
  try { return JSON.parse(value); } catch { return value; }
}, z.array(item).max(max).default([]));
const date = z.string().refine(isIsoDate, "Ungültiges Datum.");
export const SMDurcharbeitExportSchema = z.object({
  campaignIds: list(z.string().uuid(), 100), smUserIds: list(z.string().uuid(), 500),
  regions: list(z.string().trim().min(1).max(120), 100), chains: list(z.string().trim().min(1).max(120), 100),
  status: z.enum(["all", "active", "inactive"]).default("all"), from: date.optional(), to: date.optional(),
}).strict().refine(input => Boolean(input.from) === Boolean(input.to) && (!input.from || input.to! >= input.from), "Bitte den vollständigen Zeitraum prüfen.");
export type SMDurcharbeitExportSelection = z.infer<typeof SMDurcharbeitExportSchema>;

const region = sql<string>`coalesce(${targets.marketSnapshot}->>'region','')`;
const chain = sql<string>`coalesce(${targets.marketSnapshot}->>'chain','')`;
function selectionScope(input: SMDurcharbeitExportSelection) {
  return and(
    eq(submissions.isDeleted, false), eq(submissions.isCurrent, true), eq(submissions.status, "submitted"),
    sql`${submissions.SMDurcharbeitVisitId} is not null`,
    sql`starts_with(${templates.stableCode}, 'smdurcharbeit_')`,
    ...(input.campaignIds.length ? [inArray(campaigns.id, input.campaignIds)] : []),
    ...(input.smUserIds.length ? [inArray(submissions.smUserId, input.smUserIds)] : []),
    ...(input.regions.length ? [inArray(region, input.regions)] : []),
    ...(input.chains.length ? [inArray(chain, input.chains)] : []),
    ...(input.status === "active" ? [inArray(campaigns.status, ["published", "paused"])] : []),
    ...(input.status === "inactive" ? [eq(campaigns.status, "archived")] : []),
    ...(input.from ? [gte(smManagedWorkDate, input.from), lte(smManagedWorkDate, input.to!)] : []),
  )!;
}
function exportRows(tx: SmManagementTx, input: SMDurcharbeitExportSelection) {
  return tx.select({ id: sql<string>`${submissions.id}`.as("submission_id"), campaignId: sql<string>`${campaigns.id}`.as("campaign_id"), campaignName: campaigns.name, status: campaigns.status,
    from: campaigns.startDate, to: campaigns.endDate, smUserId: submissions.smUserId, smName: submissions.smNameSnapshot,
    marketId: submissions.smMarketId, market: targets.marketSnapshot, region: region.as("region"), chain: chain.as("chain"),
    workDate: smManagedWorkDate.as("work_date") }).from(submissions)
    .innerJoin(targets, eq(targets.id, submissions.SMDurcharbeitTargetId))
    .innerJoin(campaigns, eq(campaigns.id, targets.campaignId))
    .innerJoin(templates, eq(templates.id, submissions.questionnaireTemplateId))
    .leftJoin(smAssignments, eq(smAssignments.id, submissions.assignmentId))
    .leftJoin(times, and(eq(times.visitId, submissions.SMDurcharbeitVisitId), eq(times.isCurrent, true)))
    .where(selectionScope(input));
}

/** Answer authors and market facets come from submitted visit snapshots, including former staff. */
export async function SMDurcharbeitExportOptions(tx: SmManagementTx) {
  const input = SMDurcharbeitExportSchema.parse({});
  const selected = exportRows(tx, input).as("export_selection");
  const rows = await tx.selectDistinct({ campaignId: selected.campaignId, campaignName: selected.campaignName, status: selected.status,
    from: selected.from, to: selected.to, smUserId: selected.smUserId, smName: selected.smName, region: selected.region, chain: selected.chain })
    .from(selected).orderBy(asc(selected.campaignName), asc(selected.smName)).limit(10001);
  if (rows.length > 10000) throw new SmManagementError(413, "smdurcharbeit_export_catalog_large", "Die Exportauswahl ist zu groß. Bitte wende dich an die Administration.");
  return {
    range: await SMDurcharbeitExportPreview(tx, input),
    campaigns: [...new Map(rows.map(row => [row.campaignId, { id: row.campaignId, name: row.campaignName, status: row.status, from: row.from, to: row.to }])).values()],
    people: [...new Map(rows.map(row => [row.smUserId, { id: row.smUserId, name: row.smName }])).values()],
    regions: [...new Set(rows.map(row => row.region).filter(Boolean))].sort(),
    chains: [...new Set(rows.map(row => row.chain).filter(Boolean))].sort(),
  };
}

export async function SMDurcharbeitExportPreview(tx: SmManagementTx, input: SMDurcharbeitExportSelection) {
  const selected = exportRows(tx, input).as("export_selection");
  const [row] = await tx.select({ visits: sql<number>`count(*)::int`, markets: sql<number>`count(distinct ${selected.marketId})::int`,
    campaigns: sql<number>`count(distinct ${selected.campaignId})::int`, from: sql<string | null>`min(${selected.workDate})::text`,
    to: sql<string | null>`max(${selected.workDate})::text` }).from(selected);
  return row!;
}

/** One read-only repeatable snapshot for exactly the selected submitted Durcharbeit visits. */
export async function SMDurcharbeitExportReport(tx: SmManagementTx, input: SMDurcharbeitExportSelection) {
  const summary = await SMDurcharbeitExportPreview(tx, input);
  if (summary.visits > 3000) throw new SmManagementError(413, "smdurcharbeit_export_large", "Mehr als 3.000 Besuche. Bitte grenze Durcharbeit, Zeitraum oder SM weiter ein.");
  const dates = { from: input.from ?? summary.from ?? "2000-01-01", to: input.to ?? summary.to ?? "2000-01-01" };
  const report = await loadSmManagementReport(tx, { ...dates, SMDurcharbeitCatalogScope: "SMDurcharbeit" }, selectionScope(input));
  if (report.visits.length !== summary.visits) throw new SmManagementError(409, "smdurcharbeit_export_incomplete", "Die Auswertung ist unvollständig. Bitte erneut laden.");
  const selected = exportRows(tx, input).as("export_selection");
  const metadata = await tx.select({ submissionId: selected.id, market: selected.market }).from(selected);
  return { ...report, selection: input, summary, visitMetadata: metadata };
}
