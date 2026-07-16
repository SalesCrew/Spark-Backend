export type SpezialfrageSessionCandidate = {
  questionId: string;
};

/**
 * Keeps the activity sync idempotent: questions already snapshotted in the
 * submitted visit are ignored and duplicate candidates are collapsed.
 */
export function selectMissingSpezialfragenForSession<T extends SpezialfrageSessionCandidate>(
  candidates: readonly T[],
  existingQuestionIds: Iterable<string>,
): T[] {
  const seen = new Set(existingQuestionIds);
  const missing: T[] = [];
  for (const candidate of candidates) {
    if (seen.has(candidate.questionId)) continue;
    seen.add(candidate.questionId);
    missing.push(candidate);
  }
  return missing;
}
