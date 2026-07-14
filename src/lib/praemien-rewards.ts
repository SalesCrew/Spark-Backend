export type PraemienRewardModel = "global_thresholds" | "pillar_targets" | "pillar_tiers";
export type PraemienPayoutMode = "highest_tier" | "sum_earned_tiers";
export type PraemienConditionOperator = "gte" | "lte" | "eq";

export type PraemienThresholdRewardInput = {
  minPoints: number;
  rewardEur: number;
};

export type PraemienTierConditionInput = {
  metricKey: string;
  operator: PraemienConditionOperator;
  thresholdValue: number;
};

export type PraemienPillarTierInput = {
  tierId: string;
  label: string;
  orderIndex: number;
  rewardEur: number;
  conditions: PraemienTierConditionInput[];
};

export type PraemienPillarRewardInput = {
  pillarId: string;
  points: number;
  targetPoints: number | null;
  rewardEur: number;
  payoutMode?: PraemienPayoutMode;
  maxRewardEur?: number;
  metricValues?: Record<string, number>;
  tiers?: PraemienPillarTierInput[];
};

export type PraemienTierEvaluation = PraemienPillarTierInput & {
  achieved: boolean;
  missingConditions: PraemienTierConditionInput[];
};

export type PraemienPillarRewardResult = PraemienPillarRewardInput & {
  payoutMode: PraemienPayoutMode;
  maxRewardEur: number;
  metricValues: Record<string, number>;
  tierResults: PraemienTierEvaluation[];
  achievedTierIds: string[];
  achievedTierLabels: string[];
  nextTierId: string | null;
  nextTierLabel: string | null;
  isConfigured: boolean;
  achieved: boolean;
  earnedRewardEur: number;
};

export type PraemienWaveRewardResult = {
  rewardModel: PraemienRewardModel;
  currentRewardEur: number;
  fullRewardEur: number;
  pillarResults: PraemienPillarRewardResult[];
};

function finiteNonNegative(value: number): number {
  return Number.isFinite(value) && value > 0 ? value : 0;
}

function roundMoney(value: number): number {
  return Math.round((value + Number.EPSILON) * 100) / 100;
}

function normalizeMetricValues(values: Record<string, number> | undefined, points: number): Record<string, number> {
  const normalized: Record<string, number> = { points: finiteNonNegative(points) };
  for (const [key, value] of Object.entries(values ?? {})) {
    if (!key.trim() || !Number.isFinite(value)) continue;
    normalized[key] = value;
  }
  return normalized;
}

function conditionMatches(value: number | undefined, condition: PraemienTierConditionInput): boolean {
  if (value == null || !Number.isFinite(value) || !Number.isFinite(condition.thresholdValue)) return false;
  if (condition.operator === "lte") return value <= condition.thresholdValue;
  if (condition.operator === "eq") return Math.abs(value - condition.thresholdValue) < 0.0001;
  return value >= condition.thresholdValue;
}

export function pickGlobalRewardForPoints(
  totalPoints: number,
  thresholds: PraemienThresholdRewardInput[],
): number {
  const safePoints = finiteNonNegative(totalPoints);
  const matched = thresholds
    .filter((entry) => Number.isFinite(entry.minPoints) && Number.isFinite(entry.rewardEur))
    .slice()
    .sort((left, right) => left.minPoints - right.minPoints)
    .filter((entry) => safePoints >= entry.minPoints)
    .at(-1);
  return roundMoney(finiteNonNegative(matched?.rewardEur ?? 0));
}

function calculateLegacyPillarReward(pillar: PraemienPillarRewardInput): PraemienPillarRewardResult {
  const points = finiteNonNegative(pillar.points);
  const targetPoints = pillar.targetPoints != null && Number.isFinite(pillar.targetPoints) && pillar.targetPoints > 0
    ? pillar.targetPoints
    : null;
  const rewardEur = finiteNonNegative(pillar.rewardEur);
  const isConfigured = targetPoints !== null;
  const achieved = isConfigured && points >= targetPoints;
  return {
    ...pillar,
    points,
    targetPoints,
    rewardEur,
    payoutMode: "highest_tier",
    maxRewardEur: rewardEur,
    metricValues: normalizeMetricValues(pillar.metricValues, points),
    tierResults: [],
    achievedTierIds: [],
    achievedTierLabels: [],
    nextTierId: null,
    nextTierLabel: null,
    isConfigured,
    achieved,
    earnedRewardEur: achieved ? rewardEur : 0,
  };
}

export function calculateTieredPillarReward(pillar: PraemienPillarRewardInput): PraemienPillarRewardResult {
  const points = finiteNonNegative(pillar.points);
  const metricValues = normalizeMetricValues(pillar.metricValues, points);
  const payoutMode = pillar.payoutMode ?? "highest_tier";
  const tiers = (pillar.tiers ?? [])
    .filter((tier) => tier.tierId.trim() && tier.label.trim() && Number.isFinite(tier.rewardEur))
    .slice()
    .sort((left, right) => left.orderIndex - right.orderIndex);
  const tierResults: PraemienTierEvaluation[] = tiers.map((tier) => {
    const missingConditions = tier.conditions.filter(
      (condition) => !conditionMatches(metricValues[condition.metricKey], condition),
    );
    return {
      ...tier,
      rewardEur: finiteNonNegative(tier.rewardEur),
      achieved: tier.conditions.length > 0 && missingConditions.length === 0,
      missingConditions,
    };
  });
  const achievedTiers = tierResults.filter((tier) => tier.achieved);
  const configuredMaximum = finiteNonNegative(pillar.maxRewardEur ?? 0);
  const tierMaximum = tierResults.reduce((maximum, tier) => {
    if (payoutMode === "sum_earned_tiers") return maximum + tier.rewardEur;
    return Math.max(maximum, tier.rewardEur);
  }, 0);
  const maxRewardEur = roundMoney(configuredMaximum > 0 ? configuredMaximum : tierMaximum);
  const earnedBeforeCap = payoutMode === "sum_earned_tiers"
    ? achievedTiers.reduce((sum, tier) => sum + tier.rewardEur, 0)
    : (achievedTiers.at(-1)?.rewardEur ?? 0);
  const earnedRewardEur = roundMoney(maxRewardEur > 0 ? Math.min(earnedBeforeCap, maxRewardEur) : earnedBeforeCap);
  const nextTier = tierResults.find((tier) => !tier.achieved) ?? null;

  return {
    ...pillar,
    points,
    payoutMode,
    maxRewardEur,
    metricValues,
    tiers,
    tierResults,
    achievedTierIds: achievedTiers.map((tier) => tier.tierId),
    achievedTierLabels: achievedTiers.map((tier) => tier.label),
    nextTierId: nextTier?.tierId ?? null,
    nextTierLabel: nextTier?.label ?? null,
    isConfigured: tierResults.length > 0 && tierResults.every((tier) => tier.conditions.length > 0),
    achieved: achievedTiers.length > 0,
    earnedRewardEur,
  };
}

export function calculatePillarRewards(
  pillars: PraemienPillarRewardInput[],
  rewardModel: PraemienRewardModel = "pillar_targets",
): PraemienPillarRewardResult[] {
  return pillars.map((pillar) => (
    rewardModel === "pillar_tiers"
      ? calculateTieredPillarReward(pillar)
      : calculateLegacyPillarReward(pillar)
  ));
}

export function calculateWaveReward(input: {
  rewardModel: PraemienRewardModel;
  totalPoints: number;
  thresholds: PraemienThresholdRewardInput[];
  pillars: PraemienPillarRewardInput[];
}): PraemienWaveRewardResult {
  const pillarResults = calculatePillarRewards(input.pillars, input.rewardModel);
  if (input.rewardModel === "pillar_targets" || input.rewardModel === "pillar_tiers") {
    return {
      rewardModel: input.rewardModel,
      currentRewardEur: roundMoney(pillarResults.reduce((sum, pillar) => sum + pillar.earnedRewardEur, 0)),
      fullRewardEur: roundMoney(pillarResults.reduce((sum, pillar) => sum + pillar.maxRewardEur, 0)),
      pillarResults,
    };
  }
  return {
    rewardModel: input.rewardModel,
    currentRewardEur: pickGlobalRewardForPoints(input.totalPoints, input.thresholds),
    fullRewardEur: roundMoney(Math.max(0, ...input.thresholds.map((entry) => finiteNonNegative(entry.rewardEur)))),
    pillarResults,
  };
}
