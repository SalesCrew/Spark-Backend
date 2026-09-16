export function includeGmIppPeriod(input: {
  isActive: boolean;
  includeEmptyGms: boolean;
  hasSamples: boolean;
  hasAdjustment: boolean;
}): boolean {
  if (input.hasSamples || input.hasAdjustment) return true;
  return input.isActive && input.includeEmptyGms;
}
