export type UniversumIdentityIndexes<T> = {
  byStandard: Map<string, T>;
  byCoke: Map<string, T>;
  byFlex: Map<string, T>;
};

export type UniversumImportKeys = { standard: string; coke: string; flex: string };

// Only Flex identifies a market in the ordinary Universum import. Other identities
// must never redirect a row to a different market.
export function classifyUniversumImportRow<E extends { id: string }, P extends { id: string }>(
  keys: UniversumImportKeys,
  existing: UniversumIdentityIndexes<E>,
  pending: UniversumIdentityIndexes<P>,
):
  | { kind: "existing"; value: E }
  | { kind: "duplicate-in-file" }
  | { kind: "identity-conflict"; field: "standardMarketNumber" | "cokeMasterNumber"; value: string }
  | { kind: "new" } {
  if (pending.byFlex.has(keys.flex)) return { kind: "duplicate-in-file" };
  const matched = existing.byFlex.get(keys.flex);
  for (const [field, key, existingOwners, pendingOwners] of [
    ["standardMarketNumber", keys.standard, existing.byStandard, pending.byStandard],
    ["cokeMasterNumber", keys.coke, existing.byCoke, pending.byCoke],
  ] as const) {
    if (!key) continue;
    const owner = existingOwners.get(key) ?? pendingOwners.get(key);
    if (owner && owner.id !== matched?.id) return { kind: "identity-conflict", field, value: key };
  }
  return matched ? { kind: "existing", value: matched } : { kind: "new" };
}
