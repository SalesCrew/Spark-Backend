function comparisonKey(value: string | null | undefined): string {
  const normalized = String(value ?? "").trim().toLowerCase();
  // Match alternate names without changing stored labels or dashboard filters.
  return /^billa\s*(?:plus|\+)$/.test(normalized) ? "billa+" : normalized;
}

export function isQuestionApplicableToMarketChain(
  questionChains: readonly string[] | null | undefined,
  marketChain: string | null | undefined,
): boolean {
  const configured = Array.isArray(questionChains)
    ? questionChains.filter((entry): entry is string => typeof entry === "string").map(comparisonKey).filter(Boolean)
    : [];
  if (configured.length === 0) return true;
  const market = comparisonKey(marketChain);
  return market.length > 0 && configured.includes(market);
}
