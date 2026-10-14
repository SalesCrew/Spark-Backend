import { createHash, randomUUID } from "node:crypto";
import { and, asc, eq, inArray, isNull, sql } from "drizzle-orm";
import { smSMDurcharbeitCampaigns as campaigns, smSMDurcharbeitCampaignMarkets as members,
  smSMDurcharbeitPeriods as periods, smSMDurcharbeitTargets as targets, smSMDurcharbeitOwnerRevisions as owners,
  smQuestionnaireSubmissions as submissions, smSMDurcharbeitMarkets as registry, smMarkets, users,
  smSMDurcharbeitEvents as events } from "./lib/schema.js";
import { SMDurcharbeitCampaignError, SMDurcharbeitMonth, SMDurcharbeitToday,
  type SMDurcharbeitExecutor, type SMDurcharbeitTx } from "./sm-SMDurcharbeit-campaign.shared.js";

export type SMDurcharbeitMarketEdit = {
  expectedRevision: number; month: string; scope: "month" | "future"; reason: string;
  roster: Array<{ smMarketId: string; smUserId?: string | undefined }>;
};
const fail = (code: string, message: string, status = 409): never => { throw new SMDurcharbeitCampaignError(status, code, message); };
function chunks<T>(rows: T[], size = 1000): T[][] {
  return Array.from({ length: Math.ceil(rows.length / size) }, (_, i) => rows.slice(i * size, (i + 1) * size));
}

/** A complete selection, never a filter/search string. Existing target and visit IDs stay intact. */
async function planSMDurcharbeitMarketEdit(executor: SMDurcharbeitExecutor, campaignId: string, input: SMDurcharbeitMarketEdit) {
  const [campaign] = await executor.select().from(campaigns).where(eq(campaigns.id, campaignId)).limit(1);
  if (!campaign) return fail("smdurcharbeit_campaign_missing", "Kampagne nicht gefunden.", 404);
  if (!["published", "paused"].includes(campaign.status)) return fail("smdurcharbeit_market_edit_unavailable", "Nur veröffentlichte oder pausierte Kampagnen können bearbeitet werden.");
  if (campaign.revision !== input.expectedRevision) return fail("smdurcharbeit_campaign_stale", "Die Kampagne wurde geändert. Bitte neu laden.");
  const month = SMDurcharbeitMonth(input.month);
  if (month !== input.month || month < SMDurcharbeitMonth() || campaign.endDate < SMDurcharbeitToday()) return fail("smdurcharbeit_month_closed", "Vergangene Kampagnenmonate bleiben unverändert.");
  const selectedPeriods = await executor.select().from(periods).where(and(eq(periods.campaignId, campaignId),
    input.scope === "month" ? eq(periods.month, month) : sql`${periods.month} >= ${month}::date`)).orderBy(asc(periods.month));
  if (!selectedPeriods.length || selectedPeriods[0]!.month !== month) return fail("smdurcharbeit_period_missing", "Dieser Monat gehört nicht zur Kampagne.");
  const desired = new Map(input.roster.map(row => [row.smMarketId, row]));
  if (desired.size !== input.roster.length || desired.size > 5000) return fail("smdurcharbeit_roster_invalid", "Bitte unterschiedliche Durcharbeit-Märkte auswählen (maximal 5000).", 400);
  const [memberRows, existing, catalog, people] = await Promise.all([
    executor.select().from(members).where(eq(members.campaignId, campaignId)).orderBy(asc(members.id)),
    executor.select({ target: targets, owner: owners }).from(targets).innerJoin(owners, eq(owners.id, targets.ownerRevisionId))
      .where(and(eq(targets.campaignId, campaignId), inArray(targets.periodId, selectedPeriods.map(p => p.id)))).orderBy(asc(targets.id)),
    desired.size ? executor.select({ market: smMarkets, source: registry }).from(registry)
      .innerJoin(smMarkets, eq(smMarkets.id, registry.smMarketId)).where(inArray(smMarkets.id, [...desired.keys()])).orderBy(asc(smMarkets.id)) : Promise.resolve([]),
    executor.select({ id: users.id }).from(users).where(and(eq(users.role, "sm"), eq(users.isActive, true), isNull(users.deletedAt))).orderBy(asc(users.id)),
  ]);
  const memberByMarket = new Map(memberRows.map(row => [row.smMarketId, row]));
  const memberById = new Map(memberRows.map(row => [row.id, row]));
  if (new Set([...memberByMarket.keys(), ...desired.keys()]).size > 5000) return fail("smdurcharbeit_roster_invalid", "Eine Kampagne darf höchstens 5000 unterschiedliche Märkte enthalten.", 400);
  const sourceByMarket = new Map(catalog.map(row => [row.market.id, row]));
  const activePeople = new Set(people.map(row => row.id));
  for (const marketId of desired.keys()) if (!sourceByMarket.has(marketId)) return fail("smdurcharbeit_roster_market_unavailable", "Ein ausgewählter Markt gehört nicht zum Durcharbeit-Marktbestand.");
  const history = existing.length ? await executor.select({ targetId: submissions.SMDurcharbeitTargetId, id: submissions.id, status: submissions.status })
    .from(submissions).innerJoin(targets, eq(targets.id, submissions.SMDurcharbeitTargetId)).where(and(eq(submissions.isCurrent, true), eq(submissions.isDeleted, false),
      eq(targets.campaignId, campaignId), inArray(targets.periodId, selectedPeriods.map(p => p.id)), inArray(submissions.status, ["draft", "submitted"]))).orderBy(asc(submissions.id)) : [];
  const drafts = new Set(history.filter(row => row.status === "draft").map(row => row.targetId));
  const completed = new Set(history.filter(row => row.status === "submitted").map(row => row.targetId));
  const periodById = new Map(selectedPeriods.map(row => [row.id, row]));
  const targetByPeriodMarket = new Map(existing.map(row => [`${row.target.periodId}:${row.target.campaignMarketId}`, row]));
  const updates: Array<{ id: string; revision: number; month: string; marketId: string; eligibility: "required" | "waived"; before: "required" | "waived" }> = [];
  const protectedRows: Array<{ targetId: string; marketId: string; name: string; month: string; reason: "in_progress" | "completed" }> = [];
  for (const { target } of existing) {
    const member = memberById.get(target.campaignMarketId)!;
    const eligibility = desired.has(member.smMarketId) ? "required" : "waived";
    if (target.eligibility === eligibility) continue;
    if (drafts.has(target.id) || completed.has(target.id) || target.latestSubmissionId) {
      protectedRows.push({ targetId: target.id, marketId: member.smMarketId, name: target.marketSnapshot.name ?? "Markt", month: periodById.get(target.periodId)!.month,
        reason: drafts.has(target.id) ? "in_progress" : "completed" });
      continue;
    }
    if (eligibility === "required") {
      const source = sourceByMarket.get(member.smMarketId)!;
      if (!source.market.isActive || source.market.isDeleted || !source.source.SMDurcharbeitSmUserId || !activePeople.has(source.source.SMDurcharbeitSmUserId)) return fail("smdurcharbeit_roster_market_unavailable", "Für die Aufnahme braucht jeder Markt eine gültige SM-ID im Durcharbeit-Marktbestand.");
    }
    updates.push({ id: target.id, revision: target.revision, month: periodById.get(target.periodId)!.month, marketId: member.smMarketId, eligibility, before: target.eligibility });
  }
  const additions: Array<{ periodId: string; month: string; marketId: string; smUserId: string; sourcePerson: string | null; marketSnapshot: Record<string, string> }> = [];
  for (const period of selectedPeriods) for (const marketId of desired.keys()) {
    const member = memberByMarket.get(marketId);
    if (member && targetByPeriodMarket.has(`${period.id}:${member.id}`)) continue;
    const { market, source } = sourceByMarket.get(marketId)!;
    const smUserId = source.SMDurcharbeitSmUserId;
    if (!market.isActive || market.isDeleted || !smUserId || !activePeople.has(smUserId)) return fail("smdurcharbeit_roster_unassigned", "Ein neuer Markt hat keine gültige SM-ID im Durcharbeit-Marktbestand. Bitte dort die Verplanung prüfen.");
    additions.push({ periodId: period.id, month: period.month, marketId, smUserId, sourcePerson: source.SMDurcharbeitVerplanung,
      marketSnapshot: { id: market.id, name: market.name, chain: market.chain, address: market.address, postalCode: market.postalCode, city: market.city, region: market.region, internalId: market.internalMarketId ?? market.id } });
  }
  const newMarketIds = [...new Set(additions.map(row => row.marketId))];
  const monthEnd = new Date(Date.UTC(Number(month.slice(0, 4)), Number(month.slice(5, 7)), 0, 12)).toISOString().slice(0, 10);
  const endDate = input.scope === "future" || campaign.endDate < monthEnd ? campaign.endDate : monthEnd;
  const startDate = campaign.startDate > month ? campaign.startDate : month;
  const overlapping = newMarketIds.length ? await executor.select({ id: campaigns.id, name: campaigns.name, marketId: members.smMarketId }).from(members)
    .innerJoin(campaigns, eq(campaigns.id, members.campaignId)).where(and(inArray(members.smMarketId, newMarketIds), eq(campaigns.status, "published"),
      sql`${campaigns.id} <> ${campaignId}::uuid and ${campaigns.startDate} <= ${endDate}::date and ${campaigns.endDate} >= ${startDate}::date`)).orderBy(asc(members.id)) : [];
  const legacy = newMarketIds.length ? await executor.execute(sql`select id from sm_assignments where not is_deleted and status in ('planned','confirmed','open','in_progress')
    and ${inArray(sql`coalesce(replacement_sm_market_id,original_sm_market_id)`, newMarketIds)}
    and coalesce(replacement_work_date,original_work_date) between ${startDate}::date and ${endDate}::date order by id`) : [];
  const previewToken = createHash("sha256").update(JSON.stringify({ campaign, input, selectedPeriods, existing, catalog, people, history, overlapping, legacy })).digest("hex");
  const summary = selectedPeriods.map(period => {
    const rows = existing.filter(row => row.target.periodId === period.id);
    const changes = updates.filter(row => row.month === period.month);
    const added = additions.filter(row => row.periodId === period.id).length;
    const included = changes.filter(row => row.eligibility === "required").length, excluded = changes.filter(row => row.eligibility === "waived").length;
    return { month: period.month, required: rows.filter(row => row.target.eligibility === "required").length + added + included - excluded,
      excluded: rows.filter(row => row.target.eligibility === "waived").length + excluded - included, added, included, removed: excluded,
      protected: protectedRows.filter(row => row.month === period.month).length };
  });
  return { campaign, memberByMarket, updates, additions, preview: { previewToken, months: summary, protected: protectedRows,
    changedTargets: updates.length + additions.length, overlapping: overlapping.length, legacy: legacy.length } };
}

export async function previewSMDurcharbeitMarketEdit(executor: SMDurcharbeitExecutor, campaignId: string, input: SMDurcharbeitMarketEdit) {
  return (await planSMDurcharbeitMarketEdit(executor, campaignId, input)).preview;
}

export async function applySMDurcharbeitMarketEdit(tx: SMDurcharbeitTx, campaignId: string, actorId: string,
  input: SMDurcharbeitMarketEdit & { previewToken: string; confirmOverlap?: boolean | undefined }) {
  // Visit start/submit, ownership changes, and this writer share the campaign lock.
  await tx.select({ id: campaigns.id }).from(campaigns).where(eq(campaigns.id, campaignId)).for("update");
  const { previewToken, confirmOverlap, ...selection } = input;
  const plan = await planSMDurcharbeitMarketEdit(tx, campaignId, selection);
  if (plan.preview.previewToken !== previewToken) return fail("smdurcharbeit_market_preview_stale", "Der Monatsstand wurde geändert. Bitte die Vorschau erneut prüfen.");
  if ((plan.preview.overlapping || plan.preview.legacy) && !confirmOverlap) return fail("smdurcharbeit_roster_overlap", "Bitte die Überschneidungen vor dem Speichern bestätigen.");
  if (!plan.preview.changedTargets) return { ...plan.preview, updated: false };
  const newMembers = [...new Set(plan.additions.map(row => row.marketId))].filter(id => !plan.memberByMarket.has(id))
    .map(smMarketId => ({ id: randomUUID(), campaignId, smMarketId }));
  for (const batch of chunks(newMembers)) await tx.insert(members).values(batch);
  for (const member of newMembers) plan.memberByMarket.set(member.smMarketId, { ...member, createdAt: new Date() });
  const ownerRows = plan.additions.map(row => ({ id: randomUUID(), campaignMarketId: plan.memberByMarket.get(row.marketId)!.id, month: row.month,
    smUserId: row.smUserId, sourcePerson: row.sourcePerson, reason: input.reason, actorUserId: actorId }));
  const targetRows = plan.additions.map((row, i) => ({ id: randomUUID(), campaignId, periodId: row.periodId,
    campaignMarketId: ownerRows[i]!.campaignMarketId, ownerRevisionId: ownerRows[i]!.id, marketSnapshot: row.marketSnapshot }));
  for (const batch of chunks(ownerRows)) await tx.insert(owners).values(batch);
  for (const batch of chunks(targetRows)) await tx.insert(targets).values(batch);
  for (const eligibility of ["required", "waived"] as const) for (const batch of chunks(plan.updates.filter(row => row.eligibility === eligibility))) {
    await tx.update(targets).set({ eligibility, waiverReason: eligibility === "waived" ? input.reason : null,
      revision: sql`${targets.revision}+1`, updatedAt: new Date() }).where(inArray(targets.id, batch.map(row => row.id)));
  }
  const auditRows = [...plan.updates.map(row => ({ campaignId, targetId: row.id, actorUserId: actorId, action: "market_selection_changed", reason: input.reason,
    beforeState: { eligibility: row.before, revision: row.revision }, afterState: { eligibility: row.eligibility, revision: row.revision + 1 } })),
  ...targetRows.map((row, i) => ({ campaignId, targetId: row.id, actorUserId: actorId, action: "market_added", reason: input.reason,
    beforeState: null, afterState: { eligibility: "required", smMarketId: plan.additions[i]!.marketId } }))];
  for (const batch of chunks(auditRows)) await tx.insert(events).values(batch);
  await tx.insert(events).values({ campaignId, actorUserId: actorId, action: "markets_updated", reason: input.reason,
    afterState: { month: input.month, scope: input.scope, months: plan.preview.months, protectedTargetIds: plan.preview.protected.map(row => row.targetId) } });
  await tx.update(campaigns).set({ revision: plan.campaign.revision + 1, updatedByUserId: actorId, updatedAt: new Date() }).where(eq(campaigns.id, campaignId));
  return { ...plan.preview, updated: true };
}
