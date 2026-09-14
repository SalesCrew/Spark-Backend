import { buildSmSeriesDates, isAssignmentPlanningMutable, resolveSmAssignmentValues, type SmAssignmentValueSet, type SmPlanningFrequency } from "./sm-planning.shared.js";

export type SeriesOccurrence = SmAssignmentValueSet & {
  id: string;
  status: string;
  startedAt: Date | null;
  completedAt: Date | null;
  hasHistory: boolean;
  cancelledBySchedule: boolean;
};

export type SeriesChange = {
  action: "edit" | "stop";
  effectiveFromDate: string;
  smMarketId?: string;
  smUserId?: string;
  plannedMinutes?: number;
  frequency?: SmPlanningFrequency;
  weekdays?: number[];
  validTo?: string;
};

/** Pure reconciliation; occurrence identity, manual cancellations and execution history survive. */
export function planSmSeriesChange(rows: SeriesOccurrence[], input: SeriesChange, anchorDate: string) {
  const desired = new Set(input.action === "stop" ? [] : buildSmSeriesDates({
    validFrom: input.effectiveFromDate, validTo: input.validTo!, weekdays: input.weekdays!,
    frequency: input.frequency!, anchorDate,
  }));
  const updateIds: string[] = [];
  const cancelIds: string[] = [];
  const restoreIds: string[] = [];
  const protectedIds: string[] = [];
  const preservedDateIds: string[] = [];
  const reserved = new Set(rows.map((row) => row.originalWorkDate));
  for (const row of rows) {
    const effective = resolveSmAssignmentValues(row);
    if (effective.workDate < input.effectiveFromDate && row.originalWorkDate < input.effectiveFromDate) continue;
    const historical = row.startedAt !== null || row.completedAt !== null || row.hasHistory;
    // Stop uses the actual date. Editing also protects occurrences moved across the cutoff.
    const beforeCutoff = effective.workDate < input.effectiveFromDate
      || (input.action === "edit" && row.originalWorkDate < input.effectiveFromDate);
    const canRestore = input.action === "edit" && row.status === "cancelled" && row.cancelledBySchedule;
    if (beforeCutoff || historical || (!isAssignmentPlanningMutable(row.status) && !canRestore)) {
      protectedIds.push(row.id);
      continue;
    }
    if (!desired.has(row.originalWorkDate)) {
      if (row.status !== "cancelled") cancelIds.push(row.id);
      continue;
    }
    if (row.status === "cancelled") restoreIds.push(row.id);
    else if (effective.smUserId !== input.smUserId || effective.smMarketId !== input.smMarketId || effective.plannedMinutes !== input.plannedMinutes) updateIds.push(row.id);
    if (row.replacementWorkDate) preservedDateIds.push(row.id);
  }
  // A manually moved visit already occupying a requested date must not be duplicated.
  const cancelled = new Set(cancelIds);
  const restored = new Set(restoreIds);
  const occupied = new Set(rows.filter((row) => !cancelled.has(row.id) && (row.status !== "cancelled" || restored.has(row.id)))
    .map((row) => resolveSmAssignmentValues(row).workDate));
  const blockedDates = [...desired].filter((date) => !reserved.has(date) && occupied.has(date));
  const createDates = [...desired].filter((date) => !reserved.has(date) && !occupied.has(date));
  return { updateIds, cancelIds, restoreIds, createDates, protectedIds, preservedDateIds, blockedDates, desiredCount: desired.size };
}
