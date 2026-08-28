export type SmNameCandidate = {
  id: string;
  name: string;
};

export type RankedSmNameCandidate = SmNameCandidate & {
  score: number;
};

const IGNORED_NAME_TOKENS = new Set([
  "bsc",
  "ba",
  "dipl",
  "dr",
  "ing",
  "mag",
  "mba",
  "msc",
]);

export function normalizeSmNameTokens(value: string): string[] {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/ß/g, "ss")
    .toLocaleLowerCase("de-AT")
    .replace(/[^a-z0-9]+/g, " ")
    .trim()
    .split(/\s+/)
    .filter((token) => token && !IGNORED_NAME_TOKENS.has(token))
    .sort((left, right) => left.localeCompare(right, "de-AT"));
}

function levenshteinDistance(left: string, right: string): number {
  if (left === right) return 0;
  if (!left) return right.length;
  if (!right) return left.length;
  const previous = Array.from({ length: right.length + 1 }, (_, index) => index);
  const current = new Array<number>(right.length + 1);
  for (let leftIndex = 1; leftIndex <= left.length; leftIndex += 1) {
    current[0] = leftIndex;
    for (let rightIndex = 1; rightIndex <= right.length; rightIndex += 1) {
      const substitution = previous[rightIndex - 1]! + (left[leftIndex - 1] === right[rightIndex - 1] ? 0 : 1);
      current[rightIndex] = Math.min(previous[rightIndex]! + 1, current[rightIndex - 1]! + 1, substitution);
    }
    for (let index = 0; index < current.length; index += 1) previous[index] = current[index]!;
  }
  return previous[right.length] ?? Math.max(left.length, right.length);
}

function normalizedLevenshtein(left: string, right: string): number {
  const longest = Math.max(left.length, right.length);
  if (longest === 0) return 1;
  return 1 - levenshteinDistance(left, right) / longest;
}

function bigrams(value: string): string[] {
  if (value.length < 2) return value ? [value] : [];
  return Array.from({ length: value.length - 1 }, (_, index) => value.slice(index, index + 2));
}

function diceCoefficient(left: string, right: string): number {
  if (left === right) return 1;
  const leftPairs = bigrams(left);
  const rightPairs = bigrams(right);
  if (leftPairs.length === 0 || rightPairs.length === 0) return 0;
  const remaining = [...rightPairs];
  let overlap = 0;
  for (const pair of leftPairs) {
    const index = remaining.indexOf(pair);
    if (index < 0) continue;
    overlap += 1;
    remaining.splice(index, 1);
  }
  return (2 * overlap) / (leftPairs.length + rightPairs.length);
}

export function scoreSmNameMatch(sourceName: string, candidateName: string): number {
  const sourceTokens = normalizeSmNameTokens(sourceName);
  const candidateTokens = normalizeSmNameTokens(candidateName);
  if (sourceTokens.length === 0 || candidateTokens.length === 0) return 0;
  const sourceSorted = sourceTokens.join(" ");
  const candidateSorted = candidateTokens.join(" ");
  if (sourceSorted === candidateSorted) return 1;
  const sourceCompact = sourceTokens.join("");
  const candidateCompact = candidateTokens.join("");
  if (sourceCompact === candidateCompact) return 0.995;
  const score = Math.max(
    normalizedLevenshtein(sourceSorted, candidateSorted),
    normalizedLevenshtein(sourceCompact, candidateCompact),
    diceCoefficient(sourceCompact, candidateCompact),
  );
  return Math.round(Math.max(0, Math.min(score, 0.99)) * 1_000) / 1_000;
}

function sharesExactLongToken(sourceName: string, candidateName: string): boolean {
  const source = normalizeSmNameTokens(sourceName).filter((token) => token.length >= 3);
  const candidate = new Set(normalizeSmNameTokens(candidateName).filter((token) => token.length >= 3));
  return source.some((token) => candidate.has(token));
}

export function rankSmNameCandidates(sourceName: string, candidates: SmNameCandidate[]): RankedSmNameCandidate[] {
  return candidates
    .map((candidate) => ({ ...candidate, score: scoreSmNameMatch(sourceName, candidate.name) }))
    .sort((left, right) => right.score - left.score || left.name.localeCompare(right.name, "de-AT", { sensitivity: "base" }) || left.id.localeCompare(right.id));
}

export function resolveAutomaticSmNameMatch(sourceName: string, candidates: SmNameCandidate[]): {
  match: RankedSmNameCandidate | null;
  method: "exact" | "fuzzy" | null;
  suggestions: RankedSmNameCandidate[];
} {
  const ranked = rankSmNameCandidates(sourceName, candidates);
  const best = ranked[0];
  if (!best) return { match: null, method: null, suggestions: [] };
  const secondScore = ranked[1]?.score ?? 0;
  if (best.score >= 0.995 && secondScore < 0.995) {
    return { match: best, method: "exact", suggestions: ranked.slice(0, 5) };
  }
  const fuzzyIsSafe = best.score >= 0.9
    && best.score - secondScore >= 0.07
    && (best.score >= 0.96 || sharesExactLongToken(sourceName, best.name));
  return {
    match: fuzzyIsSafe ? best : null,
    method: fuzzyIsSafe ? "fuzzy" : null,
    suggestions: ranked.filter((candidate) => candidate.score >= 0.35).slice(0, 5),
  };
}
