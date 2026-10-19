import { createHash } from "node:crypto";

export type MileageSegment = { startKm: number; endKm: number };
export type MileageState = { segments: MileageSegment[]; breakBefore: boolean; breakAfter: boolean };
export type MileageDay = {
  id: string; gmUserId: string; workDate: string; status: string;
  startKm: number | null; endKm: number | null; updatedAt: Date;
  mileage: MileageState | null;
};
export type MileageEdit = { id: string; segments: MileageSegment[] };
export function sameMileage(left: MileageState | null, right: MileageState | null) {
  return left === null || right === null ? left === right : left.breakBefore === right.breakBefore && left.breakAfter === right.breakAfter && left.segments.length === right.segments.length && left.segments.every((part, i) => part.startKm === right.segments[i]?.startKm && part.endKm === right.segments[i]?.endKm);
}
export const mileageError = (message: string, status = 400) => Object.assign(new Error(message), { status });
export function mileageDistance(startKm: number | null, endKm: number | null, mileage?: MileageState | null): number | null {
  if (mileage?.segments.length) return mileage.segments.reduce((sum, part) => sum + part.endKm - part.startKm, 0);
  return startKm !== null && endKm !== null && endKm >= startKm ? endKm - startKm : null;
}
export function mileageFingerprint(days: MileageDay[]) {
  return createHash("sha256").update(JSON.stringify(days.map(day => ({ ...day, updatedAt: day.updatedAt.toISOString() })))).digest("hex");
}
export function mileageTotals(days: MileageDay[]) {
  const valid = days.filter(day => day.status === "submitted" && day.startKm !== null && day.endKm !== null);
  let privateKm = 0;
  for (let i = 1; i < valid.length; i++) {
    const before = valid[i - 1]!, after = valid[i]!;
    // A different odometer is never private travel. Ordinary overnight gaps remain intact.
    if (!days.some(day => day.workDate >= before.workDate && day.workDate <= after.workDate && ((day.id !== before.id && day.mileage?.breakBefore) || (day.id !== after.id && day.mileage?.breakAfter) || (day.workDate > before.workDate && day.workDate < after.workDate && (day.mileage?.segments.length ?? 0) > 1)))) privateKm += Math.max(0, after.startKm! - before.endKm!);
  }
  return { businessKm: valid.reduce((sum, day) => sum + (mileageDistance(day.startKm, day.endKm, day.mileage) ?? 0), 0), privateKm };
}
export function validateSegments(segments: MileageSegment[]) {
  if (!segments.length || segments.length > 8) throw mileageError("Bitte ein bis acht Fahrzeugabschnitte angeben.");
  for (const part of segments) {
    if (![part.startKm, part.endKm].every(n => Number.isInteger(n) && n >= 0 && n <= 10_000_000) || part.endKm < part.startKm) {
      throw mileageError("Je Fahrzeug muss der Endstand mindestens dem Startstand entsprechen.");
    }
  }
}
export function buildMileagePlan(days: MileageDay[], input: { from: string; to: string; edits: MileageEdit[]; returnAfter: boolean; breakBefore: boolean }) {
  const selected = days.filter(day => day.workDate >= input.from && day.workDate <= input.to);
  if (!selected.length) throw mileageError("Im gewählten Zeitraum gibt es keine Arbeitstage.");
  if (selected.some(day => day.status !== "submitted" && day.status !== "ended")) throw mileageError("Im Zeitraum ist noch ein Arbeitstag offen. Bitte zuerst abschließen.", 409);
  if (input.edits.length !== selected.length || new Set(input.edits.map(edit => edit.id)).size !== selected.length || input.edits.some(edit => !selected.some(day => day.id === edit.id))) {
    throw mileageError("Die Arbeitstage haben sich geändert. Zeitraum neu laden.", 409);
  }
  const changed = days.map(day => {
    const edit = input.edits.find(row => row.id === day.id);
    if (!edit) return day;
    validateSegments(edit.segments);
    const index = selected.findIndex(row => row.id === day.id);
    return { ...day, startKm: edit.segments[0]!.startKm, endKm: edit.segments.at(-1)!.endKm,
      mileage: { segments: edit.segments, breakBefore: index === 0 ? input.breakBefore : (day.mileage?.breakBefore ?? false),
        breakAfter: index === selected.length - 1 ? input.returnAfter : (day.mileage?.breakAfter ?? false) } };
  });
  // Within one car, odometers cannot move backwards between workdays.
  for (let i = 1; i < changed.length; i++) {
    const before = changed[i - 1]!, after = changed[i]!;
    if (!selected.some(row => row.id === before.id || row.id === after.id)) continue;
    if (!before.mileage?.breakAfter && !after.mileage?.breakBefore && before.endKm !== null && after.startKm !== null && after.startKm < before.endKm) {
      throw mileageError(`Kilometerstände passen zwischen ${before.workDate} und ${after.workDate} nicht zusammen. Fahrzeugwechsel oder Werte prüfen.`);
    }
  }
  if (!selected.some(day => {
    const next = changed.find(row => row.id === day.id)!;
    return day.startKm !== next.startKm || day.endKm !== next.endKm || !sameMileage(day.mileage, next.mileage);
  })) throw mileageError("Es gibt keine Änderung an den Kilometerständen oder Fahrzeuggrenzen.");
  return { before: mileageTotals(days), after: mileageTotals(changed), days: changed.filter(day => selected.some(row => row.id === day.id)), fingerprint: mileageFingerprint(days) };
}
