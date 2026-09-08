const MARKET_IDENTITY_NOISE = /[^\p{L}\p{N}]+/gu;

/**
 * Normalizes the business identifier shared by the separate GM and SM market
 * domains. Formatting characters are intentionally ignored, while letters,
 * digits and leading zeroes remain significant.
 */
export function normalizeCrossDomainMarketIdentity(value: string | null | undefined): string | null {
  const normalized = String(value ?? "")
    .normalize("NFKC")
    .toUpperCase()
    .replace(MARKET_IDENTITY_NOISE, "");
  return normalized.length > 0 ? normalized : null;
}
