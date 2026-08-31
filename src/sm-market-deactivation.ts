import { createHash } from "node:crypto";
import { and, asc, desc, eq, gte, inArray, sql } from "drizzle-orm";
import { z } from "zod";
import { db } from "./lib/db.js";
import { smMarkets, smAssignments, smAssignmentEvents, smAssignmentSeries, smAssignmentSeriesVersions, users } from "./lib/schema.js";
import { isAssignmentPlanningMutable, replacementOrNull, resolveSmAssignmentValues } from "./sm-planning.shared.js";
import { lockSmPlanning } from "./sm-planning-lock.js";

type DbTx = Parameters<Parameters<typeof db.transaction>[0]>[0];
type Executor = typeof db | DbTx;
export class SmMarketDeactivationError extends Error {
  constructor(public readonly statusCode: number, public readonly code: string, message: string) { super(message); }
}
export const smMarketDeactivationSchema = z.object({
  previewToken: z.string().regex(/^[a-f0-9]{64}$/),
  resolutions: z.array(z.discriminatedUnion("action", [
    z.object({ assignmentId: z.string().uuid(), action: z.literal("cancel") }).strict(),
    z.object({ assignmentId: z.string().uuid(), action: z.literal("replace"), replacementMarketId: z.string().uuid() }).strict(),
  ])).max(20_000),
}).strict();
type Resolution = z.infer<typeof smMarketDeactivationSchema>["resolutions"][number];
export function smDeactivationToday() { return new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Vienna", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date()); }

async function loadState(executor: Executor, marketId: string) {
  const today = smDeactivationToday();
  const [market] = await executor.select().from(smMarkets).where(and(eq(smMarkets.id, marketId), eq(smMarkets.isDeleted, false))).limit(1);
  if (!market) throw new SmMarketDeactivationError(404, "sm_market_not_found", "SM-Markt nicht gefunden.");
  const rows = await executor.select().from(smAssignments).where(and(
    eq(smAssignments.isDeleted, false),
    eq(sql`coalesce(${smAssignments.replacementSmMarketId}, ${smAssignments.originalSmMarketId})`, marketId),
    gte(sql`coalesce(${smAssignments.replacementWorkDate}, ${smAssignments.originalWorkDate})`, today),
  )).orderBy(asc(smAssignments.id));
  // All versions are read only for series that still reference this market, or have affected occurrences.
  const seriesIds = [...new Set(rows.flatMap((row) => row.seriesId ? [row.seriesId] : []))];
  const versionRows = await executor.select({ version: smAssignmentSeriesVersions, series: smAssignmentSeries })
    .from(smAssignmentSeriesVersions).innerJoin(smAssignmentSeries, eq(smAssignmentSeries.id, smAssignmentSeriesVersions.seriesId))
    .where(and(eq(smAssignmentSeries.isDeleted, false), eq(smAssignmentSeriesVersions.isDeleted, false), sql`(
      ${smAssignmentSeriesVersions.seriesId} in (select series_id from public.sm_assignment_series_versions where sm_market_id = ${marketId}::uuid and not is_deleted)
      ${seriesIds.length ? sql`or ${inArray(smAssignmentSeriesVersions.seriesId, seriesIds)}` : sql``}
    )`)).orderBy(asc(smAssignmentSeriesVersions.seriesId), desc(smAssignmentSeriesVersions.versionNumber));
  const latest = [...new Map(versionRows.slice().reverse().map((entry) => [entry.series.id, entry])).values()].sort((a, b) => a.series.id.localeCompare(b.series.id));
  return { today, market, rows, latest };
}

async function publicPreview(executor: Executor, state: Awaited<ReturnType<typeof loadState>>) {
  const { today, market, rows, latest } = state;
  const affected = rows.filter((row) => isAssignmentPlanningMutable(row.status));
  const protectedRows = rows.filter((row) => row.status === "in_progress" || row.status === "completed");
  const smIds = [...new Set([...affected, ...protectedRows].map((row) => resolveSmAssignmentValues(row).smUserId))];
  const owners = smIds.length ? await executor.select({ id: users.id, firstName: users.firstName, lastName: users.lastName }).from(users).where(and(inArray(users.id, smIds), eq(users.role, "sm"))) : [];
  const names = new Map(owners.map((user) => [user.id, `${user.firstName} ${user.lastName}`.trim()]));
  const occurrence = (row: typeof smAssignments.$inferSelect) => {
    const effective = resolveSmAssignmentValues(row);
    return { id: row.id, workDate: effective.workDate, smUserId: effective.smUserId, smName: names.get(effective.smUserId) ?? "Shelf Merchandiser", plannedMinutes: effective.plannedMinutes, status: row.status };
  };
  const grouped = new Map<string, typeof affected>();
  for (const row of affected) { const key = row.seriesId ? `series:${row.seriesId}` : `single:${row.id}`; grouped.set(key, [...(grouped.get(key) ?? []), row]); }
  const groups = [...grouped.entries()].map(([id, entries]) => {
    const series = latest.find((entry) => entry.series.id === entries[0]!.seriesId);
    const occurrences = entries.map(occurrence).sort((a, b) => a.workDate.localeCompare(b.workDate) || a.id.localeCompare(b.id));
    return { id, seriesId: entries[0]!.seriesId, frequency: series?.version.frequency ?? null, weekdays: series?.version.weekdays ?? [], occurrences };
  }).sort((a, b) => a.occurrences[0]!.workDate.localeCompare(b.occurrences[0]!.workDate) || a.id.localeCompare(b.id));
  const previewToken = createHash("sha256").update(JSON.stringify({ today, market, rows, latest })).digest("hex");
  return {
    market: { id: market.id, name: market.name, address: market.address, postalCode: market.postalCode, city: market.city, internalId: market.internalMarketId, isActive: market.isActive },
    effectiveFrom: today, previewToken, affectedCount: affected.length, groups,
    protectedAssignments: protectedRows.map(occurrence),
    endingSeriesCount: latest.filter(({ series, version }) => series.status === "active" && version.smMarketId === market.id && version.validTo >= today).length,
  };
}

export async function loadSmMarketDeactivationPreview(executor: Executor, marketId: string) {
  return publicPreview(executor, await loadState(executor, marketId));
}

export async function deactivateSmMarket(tx: DbTx, marketId: string, actorUserId: string, input: z.infer<typeof smMarketDeactivationSchema>) {
  await lockSmPlanning(tx);
  await tx.select({ id: smMarkets.id }).from(smMarkets).where(eq(smMarkets.id, marketId)).for("update");
  const state = await loadState(tx, marketId);
  const preview = await publicPreview(tx, state);
  if (preview.previewToken !== input.previewToken) throw new SmMarketDeactivationError(409, "sm_market_deactivation_stale", "Markt oder Einsätze wurden inzwischen geändert. Bitte prüfe die neu geladene Liste und bestätige erneut. Es wurde nichts geändert.");
  if (!state.market.isActive) throw new SmMarketDeactivationError(409, "sm_market_already_inactive", "Dieser Markt ist bereits inaktiv. Bitte lade die Marktliste neu.");
  const affected = state.rows.filter((row) => isAssignmentPlanningMutable(row.status));
  const decisions = new Map(input.resolutions.map((item) => [item.assignmentId, item]));
  if (decisions.size !== input.resolutions.length || decisions.size !== affected.length || affected.some((row) => !decisions.has(row.id))) {
    throw new SmMarketDeactivationError(400, "sm_market_deactivation_incomplete", "Bitte entscheide für jeden betroffenen Einsatz: absagen oder Ersatzmarkt auswählen.");
  }
  const targetIds = [...new Set(input.resolutions.flatMap((item) => item.action === "replace" ? [item.replacementMarketId] : []))];
  const targets = targetIds.length ? await tx.select().from(smMarkets).where(inArray(smMarkets.id, targetIds)).orderBy(asc(smMarkets.id)).for("update") : [];
  if (targetIds.includes(marketId) || targets.length !== targetIds.length || targets.some((row) => !row.isActive || row.isDeleted || !row.internalMarketId?.trim())) {
    throw new SmMarketDeactivationError(409, "sm_market_replacement_unavailable", "Ein Ersatzmarkt ist nicht mehr aktiv, hat keine Stammnummer oder ist der bisherige Markt. Bitte wähle einen anderen SM-Markt.");
  }
  const targetById = new Map(targets.map((row) => [row.id, row]));
  const now = new Date();
  let cancelled = 0; let replaced = 0;
  const reason = `SM-Markt deaktiviert: ${state.market.name} (${state.market.internalMarketId ?? state.market.id})`;
  for (const before of affected) {
    const decision = decisions.get(before.id)!;
    const set: Partial<typeof smAssignments.$inferInsert> = { updatedAt: now, updatedByUserId: actorUserId };
    if (decision.action === "cancel") {
      Object.assign(set, { status: "cancelled", statusBeforeCancellation: before.status, cancelledAt: now, cancelledByUserId: actorUserId, cancellationReason: reason });
      cancelled++;
    } else {
      const target = targetById.get(decision.replacementMarketId)!;
      set.replacementSmMarketId = replacementOrNull(before.originalSmMarketId, target.id);
      set.replacementMarketInternalId = set.replacementSmMarketId ? target.internalMarketId : null;
      replaced++;
    }
    const [after] = await tx.update(smAssignments).set(set).where(eq(smAssignments.id, before.id)).returning();
    if (!after) throw new Error("SM_DEACTIVATION_ASSIGNMENT_MISSING");
    await tx.insert(smAssignmentEvents).values({ assignmentId: before.id, seriesId: before.seriesId, eventType: decision.action === "cancel" ? "cancelled" : "market_replaced", actorUserId, reason, beforeState: { ...before, effective: resolveSmAssignmentValues(before) }, afterState: { ...after, effective: resolveSmAssignmentValues(after) } });
  }
  for (const { series, version } of state.latest) {
    if (series.status !== "active" || version.smMarketId !== marketId || version.validTo < state.today) continue;
    const seriesDecisions = affected.filter((row) => row.seriesId === series.id).map((row) => decisions.get(row.id)!);
    const uniformReplacement = seriesDecisions.length > 0 && seriesDecisions.every((item) => item.action === "replace" && (seriesDecisions[0] as Extract<Resolution, { action: "replace" }>).replacementMarketId === item.replacementMarketId);
    if (uniformReplacement) {
      const target = targetById.get((seriesDecisions[0] as Extract<Resolution, { action: "replace" }>).replacementMarketId)!;
      const { id: _id, createdAt: _createdAt, updatedAt: _updatedAt, ...definition } = version;
      await tx.insert(smAssignmentSeriesVersions).values({ ...definition, versionNumber: version.versionNumber + 1, effectiveFromDate: state.today > version.validFrom ? state.today : version.validFrom, smMarketId: target.id, marketInternalIdSnapshot: target.internalMarketId!, changeReason: reason, createdByUserId: actorUserId });
      await tx.update(smAssignmentSeries).set({ updatedAt: now }).where(eq(smAssignmentSeries.id, series.id));
    } else {
      await tx.update(smAssignmentSeries).set({ status: "ended", updatedAt: now }).where(eq(smAssignmentSeries.id, series.id));
    }
  }
  const [market] = await tx.update(smMarkets).set({ isActive: false, updatedAt: now }).where(eq(smMarkets.id, marketId)).returning();
  if (!market) throw new Error("SM_DEACTIVATION_MARKET_MISSING");
  return { market, cancelled, replaced, protectedCount: preview.protectedAssignments.length };
}
