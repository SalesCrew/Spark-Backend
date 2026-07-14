function normalizePillarSemanticName(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/\p{Diacritic}/gu, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

export function isWaveAnswerPersistencePillarName(name: string): boolean {
  const normalized = normalizePillarSemanticName(name);
  if (normalized === "distributionsziel") return true;
  const isDisplayPillar = normalized.includes("display");
  const isSchaettenPillar = normalized.includes("schutten") || normalized.includes("schuetten");
  return isDisplayPillar && isSchaettenPillar;
}
