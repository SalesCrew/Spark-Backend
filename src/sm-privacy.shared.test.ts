import assert from "node:assert/strict";
import test from "node:test";

import { buildSmDsarCategories, type SmDsarCounts } from "./sm-privacy.shared.js";

const counts: SmDsarCounts = {
  assignedMarkets: 2,
  assignments: 4,
  submissions: 3,
  answers: 30,
  photos: 5,
  timeRecords: 4,
  messages: 2,
  answerChangeRequests: 1,
  submissionDeleteRequests: 1,
  timeChangeRequests: 2,
  auditEvents: 7,
  securityRecords: 3,
};

test("SM DSAR categories cover every SM personal-data domain", () => {
  const categories = buildSmDsarCategories(counts);
  assert.deepEqual(categories.map((category) => category.key), [
    "profile",
    "sm_planning",
    "sm_visits",
    "sm_answers",
    "sm_photos",
    "sm_time",
    "sm_messages",
    "sm_requests",
    "sm_security",
  ]);
  assert.equal(categories.find((category) => category.key === "sm_planning")?.count, 6);
  assert.equal(categories.find((category) => category.key === "sm_requests")?.count, 4);
  assert.equal(categories.find((category) => category.key === "sm_security")?.count, 10);
});

test("SM DSAR retention notes distinguish visit and time records", () => {
  const categories = buildSmDsarCategories(counts);
  assert.match(categories.find((category) => category.key === "sm_visits")?.retention ?? "", /3 Jahre/);
  assert.match(categories.find((category) => category.key === "sm_time")?.retention ?? "", /7 Jahre/);
});
