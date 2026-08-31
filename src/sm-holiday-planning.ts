import { and, asc, desc, eq, gte, inArray, sql } from "drizzle-orm";
import { db } from "./lib/db.js";
import { smAssignmentEvents, smAssignments, smMarkets } from "./lib/schema.js";
import { lockSmPlanning } from "./sm-planning-lock.js";
import { resolveSmAssignmentValues, replacementOrNull } from "./sm-planning.shared.js";
import { austrianHoliday, chooseSmHolidayDate, smHolidayCandidates, type SmHolidayAdjustment } from "./sm-holidays.shared.js";

type Tx = Parameters<Parameters<typeof db.transaction>[0]>[0];
type Executor = Tx | typeof db;
export function smHolidayToday() { return new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Vienna", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date()); }

export async function loadSmHolidayStates(executor: Executor, ids: string[]) {
  const states = new Map<string, { adjustment: SmHolidayAdjustment | null; manualOverride: boolean }>();
  if (!ids.length) return states;
  const events = await executor.select({ assignmentId: smAssignmentEvents.assignmentId, after: smAssignmentEvents.afterState }).from(smAssignmentEvents)
    .where(and(inArray(smAssignmentEvents.assignmentId, ids), eq(smAssignmentEvents.eventType, "rescheduled")))
    .orderBy(desc(smAssignmentEvents.createdAt), desc(smAssignmentEvents.id));
  for (const event of events) {
    const state = states.get(event.assignmentId) ?? { adjustment: null, manualOverride: false };
    const adjustment = event.after.holidayAdjustment as SmHolidayAdjustment | undefined;
    if (adjustment?.ruleVersion === 1 && adjustment.holidayDate && adjustment.adjustedDate) {
      state.adjustment ??= adjustment;
    } else {
      // Existing manual date edits are also intentional and must survive reconciliation.
      state.manualOverride = true;
    }
    states.set(event.assignmentId, state);
  }
  return states;
}

export async function adjustSmHolidayAssignments(tx: Tx, input: { actorUserId: string; assignmentIds?: string[]; today?: string; dryRun?: boolean }) {
  await lockSmPlanning(tx);
  const today = input.today ?? smHolidayToday();
  if (input.assignmentIds?.length === 0) return [];
  const effectiveDate = sql<string>`coalesce(${smAssignments.replacementWorkDate}, ${smAssignments.originalWorkDate})`;
  const effectiveUser = sql<string>`coalesce(${smAssignments.replacementSmUserId}, ${smAssignments.originalSmUserId})`;
  const candidates = (await tx.select().from(smAssignments).where(and(
    eq(smAssignments.isDeleted, false), inArray(smAssignments.status, ["planned", "confirmed", "open"]), gte(effectiveDate, today),
    ...(input.assignmentIds ? [inArray(smAssignments.id, input.assignmentIds)] : []),
  )).orderBy(asc(effectiveDate), asc(smAssignments.createdAt), asc(smAssignments.id))).filter((row) => austrianHoliday(resolveSmAssignmentValues(row).workDate));
  if (!candidates.length) return [];
  const states = await loadSmHolidayStates(tx, candidates.map((row) => row.id));
  const marketIds = [...new Set(candidates.map((row) => resolveSmAssignmentValues(row).smMarketId))];
  const activeMarkets = new Set((await tx.select({ id: smMarkets.id }).from(smMarkets).where(and(inArray(smMarkets.id, marketIds), eq(smMarkets.isActive, true), eq(smMarkets.isDeleted, false)))).map((row) => row.id));
  const pending = candidates.filter((row) => !states.get(row.id)?.manualOverride && activeMarkets.has(resolveSmAssignmentValues(row).smMarketId));
  if (!pending.length) return [];
  const dates = [...new Set(pending.flatMap((row) => {
    const { previous, next } = smHolidayCandidates(resolveSmAssignmentValues(row).workDate, today);
    return [previous, next].filter((value): value is string => Boolean(value));
  }))];
  const userIds = [...new Set(pending.map((row) => resolveSmAssignmentValues(row).smUserId))];
  const loads = await tx.select().from(smAssignments).where(and(
    eq(smAssignments.isDeleted, false), inArray(effectiveDate, dates), inArray(effectiveUser, userIds),
    inArray(smAssignments.status, ["planned", "confirmed", "open", "in_progress", "completed"]),
  ));
  const minutes = new Map<string, number>();
  const key = (userId: string, date: string) => `${userId}:${date}`;
  for (const row of loads) { const v = resolveSmAssignmentValues(row); const k = key(v.smUserId, v.workDate); minutes.set(k, (minutes.get(k) ?? 0) + v.plannedMinutes); }
  const changes: Array<{ assignmentId: string; smUserId: string; marketId: string; adjustment: SmHolidayAdjustment }> = [];
  for (const before of pending) {
    const effective = resolveSmAssignmentValues(before);
    const adjustment = chooseSmHolidayDate(effective.workDate, today, (date) => minutes.get(key(effective.smUserId, date)) ?? 0)!;
    changes.push({ assignmentId: before.id, smUserId: effective.smUserId, marketId: effective.smMarketId, adjustment });
    const targetKey = key(effective.smUserId, adjustment.adjustedDate);
    minutes.set(targetKey, (minutes.get(targetKey) ?? 0) + effective.plannedMinutes);
    if (input.dryRun) continue;
    const [after] = await tx.update(smAssignments).set({ replacementWorkDate: replacementOrNull(before.originalWorkDate, adjustment.adjustedDate), updatedByUserId: input.actorUserId, updatedAt: new Date() }).where(eq(smAssignments.id, before.id)).returning();
    if (!after) throw new Error("SM holiday assignment disappeared");
    await tx.insert(smAssignmentEvents).values({
      assignmentId: before.id, seriesId: before.seriesId, eventType: "rescheduled", actorUserId: input.actorUserId,
      reason: `Feiertag: ${adjustment.holidayName} (${adjustment.holidayDate}) → ${adjustment.adjustedDate}. Automatisch nach Werktagen und Sollzeit verplant.`,
      beforeState: { ...before, effective }, afterState: { ...after, effective: resolveSmAssignmentValues(after), holidayAdjustment: adjustment }, createdAt: sql`clock_timestamp()`,
    });
  }
  return changes;
}
