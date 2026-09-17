export type SnapshotMarket = {
  id: string;
  flexNumber: string | null;
  marketType: "universum" | "kuehler" | "both";
  isActive: boolean;
  name: string;
};

export type SnapshotInputRow = { row: number; flexNumber: string | null };

export function flexSnapshotKey(value: string | null | undefined): string {
  return String(value ?? "").trim().toLowerCase();
}

export function planGmMarketSnapshot(
  existingMarkets: SnapshotMarket[],
  inputRows: SnapshotInputRow[],
  options: { deactivateSharedMarkets?: boolean } = {},
) {
  const existingByFlex = new Map<string, SnapshotMarket[]>();
  for (const market of existingMarkets) {
    const key = flexSnapshotKey(market.flexNumber);
    if (!key) continue;
    const bucket = existingByFlex.get(key) ?? [];
    bucket.push(market);
    existingByFlex.set(key, bucket);
  }

  const seenInput = new Map<string, number>();
  const presentFlexKeys = new Set<string>();
  const matched: Array<{ row: number; market: SnapshotMarket }> = [];
  const newRows: SnapshotInputRow[] = [];
  const errors: Array<{ row: number; reason: string }> = [];

  for (const input of inputRows) {
    const key = flexSnapshotKey(input.flexNumber);
    if (!key) {
      errors.push({ row: input.row, reason: "Flex-Nummer fehlt oder ist leer." });
      continue;
    }
    const previousRow = seenInput.get(key);
    if (previousRow != null) {
      errors.push({ row: input.row, reason: `Flex-Nummer ${input.flexNumber} steht bereits in Zeile ${previousRow}.` });
      continue;
    }
    seenInput.set(key, input.row);
    presentFlexKeys.add(key);

    const matches = existingByFlex.get(key) ?? [];
    if (matches.length > 1) {
      errors.push({ row: input.row, reason: `Flex-Nummer ${input.flexNumber} ist in der Markttabelle nicht eindeutig.` });
    } else if (matches[0]?.marketType === "kuehler") {
      errors.push({ row: input.row, reason: `Flex-Nummer ${input.flexNumber} gehört bereits zu einem reinen Kühlermarkt.` });
    } else if (matches[0]) {
      matched.push({ row: input.row, market: matches[0] });
    } else {
      newRows.push(input);
    }
  }

  const toDeactivate = existingMarkets.filter((market) => {
    if (!market.isActive || !flexSnapshotKey(market.flexNumber)) return false;
    if (market.marketType === "kuehler") return false;
    if (market.marketType === "both" && !options.deactivateSharedMarkets) return false;
    return !presentFlexKeys.has(flexSnapshotKey(market.flexNumber));
  });
  const sharedLeftActive = options.deactivateSharedMarkets
    ? []
    : existingMarkets.filter((market) => market.isActive && market.marketType === "both"
      && flexSnapshotKey(market.flexNumber) && !presentFlexKeys.has(flexSnapshotKey(market.flexNumber)));

  return { matched, newRows, toDeactivate, sharedLeftActive, presentFlexKeys, errors };
}

export type KuehlerSnapshotMarket = SnapshotMarket & { kuehlerStammnr: string | null; cokeMasterNumber: string | null };
export type KuehlerSnapshotInputRow = { row: number; stammnr: string | null; flexNumber: string | null };
function stammnrSnapshotKey(value: string | null | undefined): string {
  return flexSnapshotKey(value).replace(/\s+/g, "");
}

export function planKuehlerMarketSnapshot(existingMarkets: KuehlerSnapshotMarket[], inputRows: KuehlerSnapshotInputRow[]) {
  const byStammnr = new Map<string, KuehlerSnapshotMarket[]>();
  const byFlex = new Map<string, KuehlerSnapshotMarket[]>();
  for (const market of existingMarkets) {
    const keys = new Set([stammnrSnapshotKey(market.kuehlerStammnr), stammnrSnapshotKey(market.cokeMasterNumber)].filter(Boolean));
    for (const key of keys) byStammnr.set(key, [...(byStammnr.get(key) ?? []), market]);
    const flex = flexSnapshotKey(market.flexNumber);
    if (flex) byFlex.set(flex, [...(byFlex.get(flex) ?? []), market]);
  }

  const sourceStammnrToFlex = new Map<string, string>();
  const sourceFlexToStammnr = new Map<string, string>();
  const sourceStammnrToMarket = new Map<string, string>();
  const sourceMarketToStammnr = new Map<string, string>();
  const matched: Array<{ row: number; market: KuehlerSnapshotMarket }> = [];
  const newRows: KuehlerSnapshotInputRow[] = [];
  const newKeys = new Set<string>();
  const presentMarketIds = new Set<string>();
  const errors: Array<{ row: number; reason: string }> = [];
  for (const input of inputRows) {
    let key = stammnrSnapshotKey(input.stammnr);
    const flex = flexSnapshotKey(input.flexNumber);
    if (!key) {
      const flexMatches = flex ? byFlex.get(flex) ?? [] : [];
      if (flexMatches.length !== 1) {
        errors.push({ row: input.row, reason: "Ohne Kühler-Stammnr muss die Flex-Nummer genau einen bestehenden Kühlermarkt erkennen." });
        continue;
      }
      const matchedMarket = flexMatches[0];
      if (!matchedMarket || matchedMarket.marketType === "universum") {
        errors.push({ row: input.row, reason: "Ohne Kühler-Stammnr kann kein neuer Kühlermarkt angelegt werden." });
        continue;
      }
      key = stammnrSnapshotKey(matchedMarket.kuehlerStammnr ?? matchedMarket.cokeMasterNumber);
      if (!key) {
        errors.push({ row: input.row, reason: "Der über Flex gefundene Kühlermarkt hat keine gespeicherte Stammnr." });
        continue;
      }
    }
    const previousFlex = sourceStammnrToFlex.get(key);
    if (flex && previousFlex && previousFlex !== flex) {
      errors.push({ row: input.row, reason: `Stammnr ${input.stammnr} hat mehrere Flex-Nummern in der Datei.` });
      continue;
    }
    const previousStammnr = flex ? sourceFlexToStammnr.get(flex) : null;
    if (previousStammnr && previousStammnr !== key) {
      errors.push({ row: input.row, reason: `Flex-Nummer ${input.flexNumber} gehört in der Datei zu mehreren Stammnummern.` });
      continue;
    }
    if (flex) {
      sourceStammnrToFlex.set(key, flex);
      sourceFlexToStammnr.set(flex, key);
    }
    const stammnrMatches = byStammnr.get(key) ?? [];
    const flexMatches = flex ? byFlex.get(flex) ?? [] : [];
    if (stammnrMatches.length > 1 || flexMatches.length > 1) {
      errors.push({ row: input.row, reason: `Stammnr ${input.stammnr} oder Flex-Nummer ist in der Markttabelle nicht eindeutig.` });
      continue;
    }
    if (stammnrMatches[0] && flexMatches[0] && stammnrMatches[0].id !== flexMatches[0].id) {
      errors.push({ row: input.row, reason: `Stammnr ${input.stammnr} und Flex-Nummer ${input.flexNumber} gehören zu verschiedenen Märkten.` });
      continue;
    }
    const market = stammnrMatches[0] ?? flexMatches[0];
    const target = market?.id ?? "new";
    const previousTarget = sourceStammnrToMarket.get(key);
    if (previousTarget && previousTarget !== target) {
      errors.push({ row: input.row, reason: `Stammnr ${input.stammnr} verweist auf mehrere Märkte.` });
      continue;
    }
    const previousStammnrForMarket = market ? sourceMarketToStammnr.get(market.id) : undefined;
    if (previousStammnrForMarket && previousStammnrForMarket !== key) {
      errors.push({ row: input.row, reason: `Derselbe Kühlermarkt hat mehrere Stammnummern in der Datei.` });
      continue;
    }
    sourceStammnrToMarket.set(key, target);
    if (market) {
      sourceMarketToStammnr.set(market.id, key);
      matched.push({ row: input.row, market });
      presentMarketIds.add(market.id);
    } else if (!newKeys.has(key)) {
      newRows.push(input);
      newKeys.add(key);
    }
  }
  const toDeactivate = existingMarkets.filter((market) => market.isActive &&
    (market.marketType === "kuehler" || market.marketType === "both") && !presentMarketIds.has(market.id));
  return { matched, newRows, toDeactivate, presentMarketIds, errors };
}
