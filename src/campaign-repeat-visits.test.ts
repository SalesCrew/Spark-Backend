import assert from "node:assert/strict";
import test from "node:test";
import { planAdditionalMarketVisit } from "./lib/campaign-repeat-visits.js";

test("an additional standard visit requires an existing active assignment", () => {
  assert.equal(planAdditionalMarketVisit([]), null);
  assert.equal(planAdditionalMarketVisit([{ assignmentSlot: 3, isDeleted: true }]), null);
});

test("an additional visit gets one new slot without changing previous targets", () => {
  const assignments = [
    { assignmentSlot: 1, isDeleted: false },
    { assignmentSlot: 2, isDeleted: true },
  ];
  assert.deepEqual(planAdditionalMarketVisit(assignments), { assignmentSlot: 3, visitTargetCount: 1 });
  assert.deepEqual(assignments, [
    { assignmentSlot: 1, isDeleted: false },
    { assignmentSlot: 2, isDeleted: true },
  ]);
});
