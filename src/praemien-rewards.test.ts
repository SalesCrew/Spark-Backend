import assert from "node:assert/strict";
import test from "node:test";
import { calculateWaveReward } from "./lib/praemien-rewards.js";

test("pillar rewards are earned independently", () => {
  const result = calculateWaveReward({
    rewardModel: "pillar_targets",
    totalPoints: 180,
    thresholds: [],
    pillars: [
      { pillarId: "execution", points: 150, targetPoints: 100, rewardEur: 300 },
      { pillarId: "distribution", points: 30, targetPoints: 80, rewardEur: 250 },
    ],
  });

  assert.equal(result.currentRewardEur, 300);
  assert.equal(result.fullRewardEur, 550);
  assert.equal(result.pillarResults[0]?.achieved, true);
  assert.equal(result.pillarResults[1]?.achieved, false);
  assert.equal(result.pillarResults[1]?.earnedRewardEur, 0);
});

test("a pillar pays exactly when its own target is reached", () => {
  const result = calculateWaveReward({
    rewardModel: "pillar_targets",
    totalPoints: 50,
    thresholds: [],
    pillars: [{ pillarId: "quality", points: 50, targetPoints: 50, rewardEur: 125 }],
  });

  assert.equal(result.currentRewardEur, 125);
  assert.equal(result.pillarResults[0]?.achieved, true);
});

test("an unconfigured pillar never pays", () => {
  const result = calculateWaveReward({
    rewardModel: "pillar_targets",
    totalPoints: 999,
    thresholds: [],
    pillars: [{ pillarId: "flex", points: 999, targetPoints: null, rewardEur: 500 }],
  });

  assert.equal(result.currentRewardEur, 0);
  assert.equal(result.pillarResults[0]?.isConfigured, false);
});

test("legacy waves continue to use their global thresholds", () => {
  const result = calculateWaveReward({
    rewardModel: "global_thresholds",
    totalPoints: 21,
    thresholds: [
      { minPoints: 0, rewardEur: 0 },
      { minPoints: 10, rewardEur: 100 },
      { minPoints: 20, rewardEur: 250 },
    ],
    pillars: [{ pillarId: "execution", points: 21, targetPoints: 100, rewardEur: 999 }],
  });

  assert.equal(result.currentRewardEur, 250);
  assert.equal(result.fullRewardEur, 250);
});

test("each pillar selects its own highest achieved payout tier", () => {
  const result = calculateWaveReward({
    rewardModel: "pillar_tiers",
    totalPoints: 0,
    thresholds: [],
    pillars: [
      {
        pillarId: "displays",
        points: 0,
        targetPoints: null,
        rewardEur: 0,
        maxRewardEur: 550,
        payoutMode: "highest_tier",
        metricValues: { achievement_percent: 84 },
        tiers: [
          { tierId: "display-50", label: "50 %", orderIndex: 0, rewardEur: 275, conditions: [{ metricKey: "achievement_percent", operator: "gte", thresholdValue: 70 }] },
          { tierId: "display-80", label: "80 %", orderIndex: 1, rewardEur: 440, conditions: [{ metricKey: "achievement_percent", operator: "gte", thresholdValue: 80 }] },
          { tierId: "display-100", label: "100 %", orderIndex: 2, rewardEur: 550, conditions: [{ metricKey: "achievement_percent", operator: "gte", thresholdValue: 95 }] },
        ],
      },
      {
        pillarId: "distribution",
        points: 0,
        targetPoints: null,
        rewardEur: 0,
        maxRewardEur: 165,
        payoutMode: "highest_tier",
        metricValues: { availability_percent: 79.9 },
        tiers: [
          { tierId: "distribution-50", label: "50 %", orderIndex: 0, rewardEur: 82.5, conditions: [{ metricKey: "availability_percent", operator: "gte", thresholdValue: 80 }] },
          { tierId: "distribution-100", label: "100 %", orderIndex: 1, rewardEur: 165, conditions: [{ metricKey: "availability_percent", operator: "gte", thresholdValue: 90 }] },
        ],
      },
    ],
  });

  assert.equal(result.currentRewardEur, 440);
  assert.equal(result.fullRewardEur, 715);
  assert.deepEqual(result.pillarResults[0]?.achievedTierLabels, ["50 %", "80 %"]);
  assert.equal(result.pillarResults[1]?.earnedRewardEur, 0);
});

test("Flex payout requires every configured component condition", () => {
  const makeFlex = (coolerPoints: number, redPoints: number, totalPoints: number) => calculateWaveReward({
    rewardModel: "pillar_tiers",
    totalPoints,
    thresholds: [],
    pillars: [{
      pillarId: "flex",
      points: totalPoints,
      targetPoints: null,
      rewardEur: 0,
      maxRewardEur: 165,
      metricValues: {
        cooler_points: coolerPoints,
        red_points: redPoints,
        total_points: totalPoints,
      },
      tiers: [
        {
          tierId: "flex-50",
          label: "50 %",
          orderIndex: 0,
          rewardEur: 82.5,
          conditions: [
            { metricKey: "cooler_points", operator: "gte", thresholdValue: 5 },
            { metricKey: "red_points", operator: "gte", thresholdValue: 5 },
            { metricKey: "total_points", operator: "gte", thresholdValue: 10 },
          ],
        },
        {
          tierId: "flex-100",
          label: "100 %",
          orderIndex: 1,
          rewardEur: 165,
          conditions: [
            { metricKey: "cooler_points", operator: "gte", thresholdValue: 5 },
            { metricKey: "red_points", operator: "gte", thresholdValue: 5 },
            { metricKey: "total_points", operator: "gte", thresholdValue: 15 },
          ],
        },
      ],
    }],
  });

  assert.equal(makeFlex(10, 0, 10).currentRewardEur, 0);
  assert.equal(makeFlex(5, 5, 10).currentRewardEur, 82.5);
  assert.equal(makeFlex(10, 5, 15).currentRewardEur, 165);
});

test("additive qualitative subgoals pay independently inside their pillar", () => {
  const result = calculateWaveReward({
    rewardModel: "pillar_tiers",
    totalPoints: 0,
    thresholds: [],
    pillars: [{
      pillarId: "quality",
      points: 0,
      targetPoints: null,
      rewardEur: 0,
      payoutMode: "sum_earned_tiers",
      maxRewardEur: 220,
      metricValues: { reporting_percent: 25, survey_percent: 0, time_percent: 25 },
      tiers: [
        { tierId: "reporting", label: "Reporting", orderIndex: 0, rewardEur: 55, conditions: [{ metricKey: "reporting_percent", operator: "gte", thresholdValue: 25 }] },
        { tierId: "survey", label: "Survey", orderIndex: 1, rewardEur: 55, conditions: [{ metricKey: "survey_percent", operator: "gte", thresholdValue: 25 }] },
        { tierId: "time", label: "Zeitmanagement", orderIndex: 2, rewardEur: 110, conditions: [{ metricKey: "time_percent", operator: "gte", thresholdValue: 25 }] },
      ],
    }],
  });

  assert.equal(result.currentRewardEur, 165);
  assert.equal(result.fullRewardEur, 220);
  assert.deepEqual(result.pillarResults[0]?.achievedTierLabels, ["Reporting", "Zeitmanagement"]);
});
