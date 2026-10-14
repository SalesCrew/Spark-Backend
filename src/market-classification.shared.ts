export type MarketClassification = "universum" | "kuehler" | "both";

export class MarketClassificationError extends Error {
  constructor(
    public statusCode: number,
    public code: string,
    message: string,
    public fields: string[] = [],
  ) {
    super(message);
  }
}

const universeFields = {
  flexNumber: "Flex-Nummer",
  name: "Name",
  address: "Adresse",
  postalCode: "Postleitzahl",
  city: "Ort",
  region: "Region",
} as const;

export function resolveMarketClassification(
  previous: MarketClassification,
  input: { marketType?: MarketClassification | undefined; universeMarket?: boolean | undefined },
): MarketClassification {
  if (input.marketType) return input.marketType;
  if (input.universeMarket === true && previous === "kuehler") return "both";
  if (input.universeMarket === false) {
    if (previous === "universum") {
      throw new MarketClassificationError(400, "market_type_required", "Universum kann nicht deaktiviert werden, solange der Markt kein Kühlermarkt ist.");
    }
    return "kuehler";
  }
  return previous;
}

export function assertUniversePromotion(
  previous: { marketType: MarketClassification; flexNumber: string | null },
  next: { marketType: MarketClassification; flexNumber: string | null; name: string; address: string; postalCode: string; city: string; region: string },
  canonicalRegion: boolean,
) {
  if (next.marketType === "kuehler") return;
  const enteringUniverse = previous.marketType === "kuehler";
  const keys = Object.keys(universeFields) as Array<keyof typeof universeFields>;
  const missing = enteringUniverse ? keys.filter(key => !String(next[key] ?? "").trim()) : [];
  // Preserve ordinary edits to legacy records, but never clear an existing required Flex number.
  if (!enteringUniverse && previous.flexNumber?.trim() && !next.flexNumber?.trim()) missing.push("flexNumber");
  if (enteringUniverse && !canonicalRegion && !missing.includes("region")) missing.push("region");
  if (missing.length) {
    throw new MarketClassificationError(400, "market_type_requirements", `Für Universum erforderlich: ${missing.map(key => universeFields[key]).join(", ")}.`, missing);
  }
}
