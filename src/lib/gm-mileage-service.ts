import { randomUUID } from "node:crypto";
import { and, asc, desc, eq, gte, inArray, lte, sql } from "drizzle-orm";
import type { db as appDatabase } from "./db.js";
import { gmDaySessions, gmMileageEvents, timeEntryChangeRequests, users } from "./schema.js";
import { buildMileagePlan, mileageError, mileageFingerprint, sameMileage, validateSegments, type MileageDay, type MileageEdit, type MileageState } from "./gm-mileage.js";

type Database = typeof appDatabase;
type Transaction = Parameters<Parameters<Database["transaction"]>[0]>[0];
type Connection = Database | Transaction;
export type MileageCorrection = { sessionId: string; from: string; to: string; edits: MileageEdit[]; returnAfter: boolean; breakBefore: boolean; reason: string; fingerprint?: string | undefined; operationId?: string | undefined };
function missingTable(error: unknown): boolean { const e = error as { code?: string; cause?: unknown }; return e?.code === "42P01" || !!e?.cause && missingTable(e.cause); }

export function createMileageService(database: Database) {
  async function metadata(ids: string[], connection: Connection = database, optional = true) {
    const result = new Map<string, MileageState>();
    if (!ids.length) return result;
    if (optional) {
      // Probe before querying so a missing optional table cannot abort an older caller's transaction.
      const available = await connection.select({ table: sql<string | null>`to_regclass('public.gm_mileage_events')::text` }).from(gmDaySessions).limit(1);
      if (!available[0]?.table) return result;
    }
    try {
      const events = await connection.selectDistinctOn([gmMileageEvents.daySessionId], { sessionId: gmMileageEvents.daySessionId, after: gmMileageEvents.after })
        .from(gmMileageEvents).where(inArray(gmMileageEvents.daySessionId, ids)).orderBy(gmMileageEvents.daySessionId, desc(gmMileageEvents.sequence));
      for (const event of events) if (event.after.mileage) result.set(event.sessionId, event.after.mileage);
    } catch (error) { if (!optional || !missingTable(error)) throw error; }
    return result;
  }
  async function context(sessionId: string, from: string, to: string, connection: Connection = database) {
    const [anchor] = await connection.select({ gmUserId: gmDaySessions.gmUserId }).from(gmDaySessions).where(and(eq(gmDaySessions.id, sessionId), eq(gmDaySessions.isDeleted, false))).limit(1);
    if (!anchor) throw mileageError("Arbeitstag nicht gefunden.", 404);
    // Immediate neighbours make both vehicle boundaries visible, independent of dashboard filters.
    const scope = and(eq(gmDaySessions.gmUserId, anchor.gmUserId), eq(gmDaySessions.isDeleted, false), inArray(gmDaySessions.status, ["started", "ended", "submitted"]));
    const [rows, before, after] = await Promise.all([
      connection.select().from(gmDaySessions).where(and(scope, gte(gmDaySessions.workDate, from), lte(gmDaySessions.workDate, to))).orderBy(asc(gmDaySessions.workDate)),
      connection.select().from(gmDaySessions).where(and(scope, sql`${gmDaySessions.workDate} < ${from}`)).orderBy(desc(gmDaySessions.workDate)).limit(1),
      connection.select().from(gmDaySessions).where(and(scope, sql`${gmDaySessions.workDate} > ${to}`)).orderBy(asc(gmDaySessions.workDate)).limit(1),
    ]);
    if (rows.length > 370) throw mileageError("Bitte höchstens ein Jahr auf einmal korrigieren.");
    const all = [...before, ...rows, ...after];
    const states = await metadata(all.map(row => row.id), connection, false);
    const days: MileageDay[] = all.map(row => ({ id: row.id, gmUserId: row.gmUserId, workDate: row.workDate, status: row.status, startKm: row.startKm, endKm: row.endKm, updatedAt: row.updatedAt, mileage: states.get(row.id) ?? null }));
    return { gmUserId: anchor.gmUserId, days, selected: days.filter(day => day.workDate >= from && day.workDate <= to), fingerprint: mileageFingerprint(days) };
  }
  async function history(sessionId: string) {
    return database.select({ id: gmMileageEvents.id, operationId: gmMileageEvents.operationId, reason: gmMileageEvents.reason, before: gmMileageEvents.before, after: gmMileageEvents.after,
      actor: sql<string>`coalesce(${users.firstName} || ' ' || ${users.lastName}, 'System')`, createdAt: gmMileageEvents.createdAt }).from(gmMileageEvents)
      .leftJoin(users, eq(users.id, gmMileageEvents.actorUserId)).where(eq(gmMileageEvents.daySessionId, sessionId)).orderBy(desc(gmMileageEvents.sequence));
  }
  async function preview(input: MileageCorrection, connection: Connection = database) {
    const data = await context(input.sessionId, input.from, input.to, connection);
    const pending = await connection.select({ id: timeEntryChangeRequests.id }).from(timeEntryChangeRequests).where(and(inArray(timeEntryChangeRequests.daySessionId, data.selected.map(day => day.id)), eq(timeEntryChangeRequests.sourceKind, "day_km"), eq(timeEntryChangeRequests.status, "pending"), eq(timeEntryChangeRequests.isDeleted, false))).limit(1);
    if (pending.length) throw mileageError("Für diesen Zeitraum ist eine KM-Korrektur offen. Bitte zuerst prüfen oder ablehnen.", 409);
    return buildMileagePlan(data.days, input);
  }
  async function apply(input: MileageCorrection, actorUserId: string) {
    if (!input.operationId || !input.fingerprint) throw mileageError("Bitte zuerst die Vorschau prüfen.");
    const operationId = input.operationId;
    return database.transaction(async tx => {
      const [anchor] = await tx.select({ gmUserId: gmDaySessions.gmUserId }).from(gmDaySessions).where(eq(gmDaySessions.id, input.sessionId));
      if (!anchor) throw mileageError("Arbeitstag nicht gefunden.", 404);
      // Serialize all mileage corrections for this GM, including retries and overlapping periods.
      await tx.execute(sql`select pg_advisory_xact_lock(hashtext(${anchor.gmUserId}))`);
      const existing = await tx.select().from(gmMileageEvents).where(eq(gmMileageEvents.operationId, operationId));
      const signature = JSON.stringify({ ...input, fingerprint: undefined, operationId: undefined });
      if (existing.length) {
        if (existing.some(event => event.requestSignature !== signature || event.actorUserId !== actorUserId)) throw mileageError("Diese Korrektur-ID wurde bereits verwendet.", 409);
        return { ok: true, count: existing.length };
      }
      await tx.select({ id: gmDaySessions.id }).from(gmDaySessions).where(and(eq(gmDaySessions.gmUserId, anchor.gmUserId), eq(gmDaySessions.isDeleted, false))).for("update");
      const plan = await preview(input, tx);
      if (plan.fingerprint !== input.fingerprint) throw mileageError("Die Kilometerstände wurden inzwischen geändert. Vorschau bitte neu laden.", 409);
      const current = await context(input.sessionId, input.from, input.to, tx);
      const now = new Date();
      for (const day of plan.days) {
        const before = current.selected.find(row => row.id === day.id)!;
        await tx.insert(gmMileageEvents).values({ daySessionId: day.id, gmUserId: day.gmUserId, actorUserId, operationId, reason: input.reason,
          requestSignature: signature, before: { startKm: before.startKm, endKm: before.endKm, mileage: before.mileage }, after: { startKm: day.startKm, endKm: day.endKm, mileage: day.mileage }, createdAt: now });
        await tx.update(gmDaySessions).set({ startKm: day.startKm, endKm: day.endKm, startKmDeferred: false, endKmDeferred: false, isStartKmCompleted: true, isEndKmCompleted: true, updatedAt: now }).where(eq(gmDaySessions.id, day.id));
      }
      return { ok: true, count: plan.days.length };
    });
  }
  // Live GM switch: explicit stints, rather than comparing unrelated odometers.
  async function liveSwitch(sessionId: string, actorUserId: string, segments: MileageState["segments"], beforeWork: boolean) {
    validateSegments(segments);
    return database.transaction(async tx => {
      const [day] = await tx.select().from(gmDaySessions).where(and(eq(gmDaySessions.id, sessionId), eq(gmDaySessions.gmUserId, actorUserId), eq(gmDaySessions.isDeleted, false))).for("update");
      if (!day || !["started", "ended"].includes(day.status)) throw mileageError("Nur der aktuelle offene Arbeitstag kann geändert werden.", 409);
      const states = await metadata([sessionId], tx, false);
      const previous = states.get(sessionId) ?? null;
      const mileage = { segments, breakBefore: beforeWork || (previous?.breakBefore ?? false), breakAfter: false };
      if (previous) {
        if (sameMileage(previous, mileage)) return day;
        throw mileageError("Dieser Fahrzeugwechsel wurde bereits gespeichert. Weitere Korrekturen bitte im Adminbereich vornehmen.", 409);
      }
      if (!beforeWork && segments[0]?.startKm !== day.startKm) throw mileageError("Der Startstand wurde inzwischen geändert. Bitte neu laden.", 409);
      const endKm = segments.at(-1)!.endKm, startKm = segments[0]!.startKm;
      await tx.insert(gmMileageEvents).values({ daySessionId: day.id, gmUserId: actorUserId, actorUserId, operationId: randomUUID(), reason: "Fahrzeugwechsel durch GM", requestSignature: "live-switch",
        before: { startKm: day.startKm, endKm: day.endKm, mileage: previous }, after: { startKm, endKm, mileage } });
      const [updated] = await tx.update(gmDaySessions).set({ startKm, endKm, isStartKmCompleted: true, isEndKmCompleted: true, startKmDeferred: false, endKmDeferred: false, updatedAt: new Date() }).where(eq(gmDaySessions.id, day.id)).returning();
      return updated!;
    });
  }
  return { metadata, context, history, preview, apply, liveSwitch };
}
