import { createHash, randomUUID } from "node:crypto";
import { and, asc, eq, inArray, isNull, sql } from "drizzle-orm";
import { smMarkets, smSMDurcharbeitMarkets as registry, users,
  smSMDurcharbeitCampaigns as campaigns, smSMDurcharbeitCampaignMarkets as members,
  smSMDurcharbeitPeriods as periods, smSMDurcharbeitTargets as targets,
  smSMDurcharbeitOwnerRevisions as owners, smSMDurcharbeitEvents as events,
  smQuestionnaireSubmissions as submissions } from "./lib/schema.js";
import { lockSmPlanning } from "./sm-planning-lock.js";
import { SMDurcharbeitCampaignError, SMDurcharbeitEffectiveOwner, SMDurcharbeitMonth, SMDurcharbeitToday,
  type SMDurcharbeitTx } from "./sm-SMDurcharbeit-campaign.shared.js";

export type SMDurcharbeitOwnerBulkInput = { SMDurcharbeitSmUserId: string; reason: string; sourceSmUserId?: string | undefined };
export type SMDurcharbeitCampaignOwnerBulkInput = SMDurcharbeitOwnerBulkInput & {
  expectedRevision: number; month: string; marketIds: string[];
};
export type SMDurcharbeitDefaultOwnerBulkInput = SMDurcharbeitOwnerBulkInput & {
  markets: Array<{ id: string; expectedUpdatedAt: string }>;
};
const fail = (code: string, message: string, status = 409): never => { throw new SMDurcharbeitCampaignError(status, code, message); };
const token = (state: unknown) => createHash("sha256").update(JSON.stringify(state)).digest("hex");
async function destination(tx: SMDurcharbeitTx, id: string, lock: boolean) {
  const query = tx.select({ id: users.id, firstName: users.firstName, lastName: users.lastName }).from(users)
    .where(and(eq(users.id, id), eq(users.role, "sm"), eq(users.isActive, true), isNull(users.deletedAt))).limit(1);
  const [person] = await (lock ? query.for("share") : query);
  if (!person) return fail("smdurcharbeit_user_inactive", "Bitte einen aktiven Shelf Merchandiser auswählen.", 400);
  return person;
}

async function defaultPlan(tx: SMDurcharbeitTx, input: SMDurcharbeitDefaultOwnerBulkInput, lock: boolean) {
  if (lock) { await tx.execute(sql`set local lock_timeout = '5s'`); await lockSmPlanning(tx); }
  const ids = input.markets.map(row => row.id).sort();
  const query = tx.select({ market: smMarkets, source: registry }).from(smMarkets)
    .innerJoin(registry, eq(registry.smMarketId, smMarkets.id))
    .where(and(inArray(smMarkets.id, ids), eq(smMarkets.isDeleted, false))).orderBy(asc(smMarkets.id));
  const rows = await (lock ? query.for("update") : query);
  if (rows.length !== ids.length) return fail("smdurcharbeit_market_missing", "Mindestens ein ausgewählter Durcharbeit-Markt ist nicht mehr verfügbar. Bitte neu laden.", 404);
  const versions = new Map(input.markets.map(row => [row.id, row.expectedUpdatedAt]));
  for (const row of rows) {
    if (row.market.updatedAt.toISOString() !== versions.get(row.market.id)) return fail("smdurcharbeit_market_stale", "Die Marktzuordnung wurde inzwischen geändert. Bitte die Liste neu laden.");
    if (input.sourceSmUserId && row.source.SMDurcharbeitSmUserId !== input.sourceSmUserId) return fail("smdurcharbeit_owner_stale", "Die ausgewählten Märkte gehören nicht mehr alle zum bisherigen SM. Bitte neu laden.");
  }
  const person = await destination(tx, input.SMDurcharbeitSmUserId, lock);
  const name = `${person.firstName} ${person.lastName}`.trim();
  const changed = rows.filter(row => row.source.SMDurcharbeitSmUserId !== person.id || row.market.assignedSmUserId !== person.id || row.market.shelfMerchandiserName !== name);
  const preview = { previewToken: token({ input, person, state: rows.map(row => [row.market.id, row.market.updatedAt.toISOString(), row.source.SMDurcharbeitSmUserId]) }),
    marketCount: rows.length, changeCount: changed.length, changedMarketCount: changed.length,
    unchangedCount: rows.length - changed.length, protected: [] as Array<{ name: string; month: string | null; reason: string }>, months: [] as string[] };
  return { rows, changed, person, name, preview };
}

export async function previewSMDurcharbeitDefaultOwners(tx: SMDurcharbeitTx, input: SMDurcharbeitDefaultOwnerBulkInput) {
  return (await defaultPlan(tx, input, false)).preview;
}
export async function applySMDurcharbeitDefaultOwners(tx: SMDurcharbeitTx, input: SMDurcharbeitDefaultOwnerBulkInput & { previewToken: string }) {
  const { previewToken, ...selection } = input;
  const plan = await defaultPlan(tx, selection, true);
  if (plan.preview.previewToken !== previewToken) return fail("smdurcharbeit_owner_preview_stale", "Die Vorschau ist nicht mehr aktuell. Bitte die Änderung erneut prüfen.");
  const ids = plan.changed.map(row => row.market.id);
  if (ids.length) {
    const updatedAt = new Date(Math.max(Date.now(), ...plan.rows.map(row => row.market.updatedAt.getTime() + 1)));
    await tx.update(smMarkets).set({ assignedSmUserId: plan.person.id, shelfMerchandiserName: plan.name, updatedAt }).where(inArray(smMarkets.id, ids));
    await tx.update(registry).set({ SMDurcharbeitSmUserId: plan.person.id }).where(inArray(registry.smMarketId, ids));
  }
  return { updated: ids.length };
}

async function campaignPlan(tx: SMDurcharbeitTx, campaignId: string, input: SMDurcharbeitCampaignOwnerBulkInput, lock: boolean) {
  if (lock) await tx.execute(sql`set local lock_timeout = '5s'`);
  const campaignQuery = tx.select().from(campaigns).where(eq(campaigns.id, campaignId)).limit(1);
  const [campaign] = await (lock ? campaignQuery.for("update") : campaignQuery);
  if (!campaign) return fail("smdurcharbeit_campaign_missing", "Kampagne nicht gefunden.", 404);
  if (campaign.revision !== input.expectedRevision) return fail("smdurcharbeit_campaign_stale", "Die Kampagne wurde geändert. Bitte neu laden.");
  const month = SMDurcharbeitMonth(input.month);
  if (month !== input.month || month < SMDurcharbeitMonth() || month > SMDurcharbeitMonth(campaign.endDate)
    || month < SMDurcharbeitMonth(campaign.startDate) || !["published", "paused"].includes(campaign.status) || campaign.endDate < SMDurcharbeitToday()) {
    return fail("smdurcharbeit_month_closed", "Bitte einen offenen oder zukünftigen Monat einer laufenden Kampagne auswählen.");
  }
  const selected = [...input.marketIds].sort();
  const conditions = [eq(targets.campaignId, campaignId), inArray(members.smMarketId, selected), eq(periods.month, month)];
  if (lock) {
    // Same campaign -> targets ordering as visit start and existing single-target edits.
    const identities = await tx.select({ id: targets.id }).from(targets).innerJoin(members, eq(members.id, targets.campaignMarketId))
      .innerJoin(periods, eq(periods.id, targets.periodId)).where(and(...conditions)).orderBy(asc(targets.id));
    if (identities.length) await tx.select({ id: targets.id }).from(targets).where(inArray(targets.id, identities.map(row => row.id))).orderBy(asc(targets.id)).for("update");
    await tx.select({ id: registry.smMarketId }).from(registry).where(inArray(registry.smMarketId, selected)).orderBy(asc(registry.smMarketId)).for("share");
  }
  const rows = await tx.select({ target: targets, membership: members, month: periods.month, effectiveSmUserId: SMDurcharbeitEffectiveOwner(),
    hasDraft: sql<boolean>`exists (select 1 from ${submissions} where ${submissions.SMDurcharbeitTargetId} = ${targets.id}
      and ${submissions.status} = 'draft' and ${submissions.isCurrent} and not ${submissions.isDeleted})` })
    .from(targets).innerJoin(campaigns, eq(campaigns.id, targets.campaignId)).innerJoin(members, eq(members.id, targets.campaignMarketId))
    .innerJoin(periods, eq(periods.id, targets.periodId)).innerJoin(owners, eq(owners.id, targets.ownerRevisionId))
    .leftJoin(registry, eq(registry.smMarketId, members.smMarketId)).where(and(...conditions)).orderBy(asc(targets.id));
  if (new Set(rows.map(row => row.membership.smMarketId)).size !== selected.length) return fail("smdurcharbeit_market_missing", "Mindestens ein ausgewählter Markt gehört nicht zu diesem Kampagnenzeitraum.", 400);
  if (input.sourceSmUserId && rows.some(row => row.month === month && row.effectiveSmUserId !== input.sourceSmUserId)) {
    return fail("smdurcharbeit_owner_stale", "Die ausgewählten Märkte gehören nicht mehr alle zum bisherigen SM. Bitte neu laden.");
  }
  const person = await destination(tx, input.SMDurcharbeitSmUserId, lock);
  const protectedRows = rows.filter(row => row.hasDraft || row.target.latestSubmissionId || row.target.eligibility !== "required");
  const protectedIds = new Set(protectedRows.map(row => row.target.id));
  const changed = rows.filter(row => !protectedIds.has(row.target.id) && row.effectiveSmUserId !== person.id);
  const preview = { previewToken: token({ input, person, state: rows.map(row => [row.target.id, row.target.revision, row.target.ownerRevisionId, row.target.latestSubmissionId, row.hasDraft, row.target.eligibility, row.effectiveSmUserId]) }),
    marketCount: selected.length, changeCount: changed.length, changedMarketCount: new Set(changed.map(row => row.membership.smMarketId)).size,
    unchangedCount: rows.length - changed.length - protectedRows.length,
    protected: protectedRows.map(row => ({ name: row.target.marketSnapshot.name, month: row.month,
      reason: row.hasDraft ? "Besuch läuft" : row.target.latestSubmissionId ? "Bereits erledigt" : "Markt ausgenommen" })),
    months: [...new Set(rows.map(row => row.month))].sort() };
  return { rows, changed, person, preview };
}

export async function previewSMDurcharbeitCampaignOwners(tx: SMDurcharbeitTx, campaignId: string, input: SMDurcharbeitCampaignOwnerBulkInput) {
  return (await campaignPlan(tx, campaignId, input, false)).preview;
}
export async function applySMDurcharbeitCampaignOwners(tx: SMDurcharbeitTx, campaignId: string, actorId: string, input: SMDurcharbeitCampaignOwnerBulkInput & { previewToken: string }) {
  const { previewToken, ...selection } = input;
  const plan = await campaignPlan(tx, campaignId, selection, true);
  if (plan.preview.previewToken !== previewToken) return fail("smdurcharbeit_owner_preview_stale", "Der Monatsstand oder eine Zuordnung wurde geändert. Bitte die Änderung erneut prüfen.");
  for (let offset = 0; offset < plan.changed.length; offset += 400) {
    const chunk = plan.changed.slice(offset, offset + 400);
    const changes = chunk.map(row => ({ row, ownerId: randomUUID() }));
    await tx.insert(owners).values(changes.map(({ row, ownerId }) => ({ id: ownerId, campaignMarketId: row.membership.id, month: row.month,
      smUserId: plan.person.id, sourcePerson: `${plan.person.firstName} ${plan.person.lastName}`.trim(), reason: input.reason, actorUserId: actorId })));
    const values = sql.join(changes.map(({ row, ownerId }) => sql`(${row.target.id}::uuid, ${ownerId}::uuid)`), sql`, `);
    await tx.execute(sql`update ${targets} as t set owner_revision_id = changes.owner_id, revision = t.revision + 1, updated_at = now()
      from (values ${values}) as changes(target_id, owner_id) where t.id = changes.target_id`);
    await tx.insert(events).values(changes.map(({ row, ownerId }) => ({ campaignId, targetId: row.target.id, actorUserId: actorId,
      action: "SMDurcharbeit_owner_override", reason: input.reason,
      beforeState: { ownerRevisionId: row.target.ownerRevisionId, smUserId: row.effectiveSmUserId },
      afterState: { ownerRevisionId: ownerId, smUserId: plan.person.id, month: row.month, SMDurcharbeitSmOverride: true, bulk: true } })));
  }
  return { updated: plan.changed.length, updatedMarkets: plan.preview.changedMarketCount };
}
