/** One planned market visit covers every currently registered cooler once. */
export function planKuehlerMarketAddition(
  assignments: ReadonlyArray<{ assignmentSlot: number; visitTargetCount: number; isDeleted: boolean }>,
  unitCount: number,
) {
  const roundSize = Math.max(1, unitCount);
  const currentTarget = assignments.reduce((sum, row) => sum + (row.isDeleted ? 0 : row.visitTargetCount), 0);
  return {
    assignmentSlot: assignments.reduce((max, row) => Math.max(max, row.assignmentSlot), 0) + 1,
    // Older manual assignments can have target=1 even when the market has several coolers.
    visitTargetCount: roundSize + (currentTarget > 0 ? Math.max(0, roundSize - currentTarget) : 0),
  };
}

export type KuehlerSubmission = {
  sessionId: string;
  kuehlerUnitId: string | null;
  submittedAt: Date | null;
};

const viennaDateFormatter = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Vienna", year: "numeric", month: "2-digit", day: "2-digit" });

export function kuehlerSubmissionInDateRange(date: Date | null, range: { dateFrom?: string | undefined; dateTo?: string | undefined }) {
  if (!date) return false;
  const day = viennaDateFormatter.format(date);
  return (!range.dateFrom || day >= range.dateFrom) && (!range.dateTo || day <= range.dateTo);
}

/**
 * Call separately for each campaign/market/GM. Each submitted session completes
 * exactly one occurrence of its cooler, oldest first, never all repeat visits.
 * Legacy sessions without a cooler ID fill otherwise empty slots once only.
 */
export function buildKuehlerVisitSlots<Unit extends { id: string }, Submission extends KuehlerSubmission>(
  units: readonly Unit[],
  targetVisitCount: number,
  submissions: readonly Submission[],
) {
  const unique = new Map(submissions.map((row) => [row.sessionId, row]));
  const ordered = [...unique.values()].filter((row) => row.submittedAt).sort((a, b) =>
    a.submittedAt!.getTime() - b.submittedAt!.getTime() || a.sessionId.localeCompare(b.sessionId),
  );
  const byUnit = new Map<string | null, Submission[]>();
  for (const row of ordered) {
    const bucket = byUnit.get(row.kuehlerUnitId) ?? [];
    bucket.push(row);
    byUnit.set(row.kuehlerUnitId, bucket);
  }
  const roundSize = Math.max(units.length, 1);
  const cursors = new Map<string, number>();
  const slots = Array.from({ length: Math.max(targetVisitCount, roundSize) }, (_, index) => {
    const unit = units[index % roundSize] ?? null;
    const occurrence = unit ? cursors.get(unit.id) ?? 0 : 0;
    if (unit) cursors.set(unit.id, occurrence + 1);
    return {
      unit,
      visitNumber: Math.floor(index / roundSize) + 1,
      submission: unit ? byUnit.get(unit.id)?.[occurrence] ?? null : null,
    };
  });
  let legacyCursor = 0;
  const legacy = byUnit.get(null) ?? [];
  for (const slot of slots) {
    if (!slot.submission) slot.submission = legacy[legacyCursor++] ?? null;
  }
  return slots;
}
