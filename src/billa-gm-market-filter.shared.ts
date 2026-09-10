export type BillaGmFilterCandidate = {
  id: string;
  firstName: string;
  lastName: string;
};

export type BillaGmFilterMarket = {
  name: string | null;
  dbName: string | null;
  employee: string | null;
  currentGmName: string | null;
  marketType: "universum" | "kuehler" | "both";
  isActive: boolean;
  isDeleted: boolean;
  kuehlerStammnr: string | null;
  cokeMasterNumber: string | null;
  flexNumber: string | null;
};

export type BillaGmFilterEnrollment = {
  gmUserId: string;
  matchValue: string;
};

function normalizePersonName(input: unknown): string | null {
  const normalized = String(input ?? "")
    .trim()
    .replace(/\s+/g, " ")
    .toLocaleLowerCase("de-AT");
  return normalized.length > 0 ? normalized : null;
}

export function normalizeBillaGmFilterValue(input: unknown): string | null {
  const normalized = String(input ?? "")
    .trim()
    .replace(/\s+/g, "")
    .toUpperCase();
  return normalized.length > 0 ? normalized : null;
}

function isBillaMarket(input: Pick<BillaGmFilterMarket, "name" | "dbName">): boolean {
  return `${input.name ?? ""} ${input.dbName ?? ""}`.toUpperCase().includes("BILLA");
}

function candidateNameKeys(candidate: BillaGmFilterCandidate): Set<string> {
  return new Set(
    [
      normalizePersonName(`${candidate.firstName} ${candidate.lastName}`),
      normalizePersonName(`${candidate.lastName} ${candidate.firstName}`),
    ].filter((value): value is string => Boolean(value)),
  );
}

export function resolveBillaGmFilterEnrollment(
  market: BillaGmFilterMarket,
  candidatesWithExistingFilter: BillaGmFilterCandidate[],
): BillaGmFilterEnrollment | null {
  if (market.isDeleted || !market.isActive) return null;
  if (market.marketType !== "universum" && market.marketType !== "both") return null;
  if (!isBillaMarket(market)) return null;

  const assignedNames = new Set(
    [normalizePersonName(market.currentGmName), normalizePersonName(market.employee)].filter(
      (value): value is string => Boolean(value),
    ),
  );
  if (assignedNames.size === 0) return null;

  const matchingCandidates = new Map<string, BillaGmFilterCandidate>();
  for (const candidate of candidatesWithExistingFilter) {
    if (Array.from(candidateNameKeys(candidate)).some((name) => assignedNames.has(name))) {
      matchingCandidates.set(candidate.id, candidate);
    }
  }
  if (matchingCandidates.size !== 1) return null;

  const matchValue = normalizeBillaGmFilterValue(
    market.kuehlerStammnr ?? market.cokeMasterNumber ?? market.flexNumber,
  );
  if (!matchValue) return null;

  return {
    gmUserId: Array.from(matchingCandidates.keys())[0]!,
    matchValue,
  };
}
