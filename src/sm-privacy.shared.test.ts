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
test("monthly SM inventory includes time history, requests and carry-over without changing legacy counts", () => {
  const categories = buildSmDsarCategories({ ...counts, SMDurcharbeit: { targets: 3, ownerRevisions: 4, visits: 2,
    timeRevisions: 3, timeRequests: 1, answerProvenance: 5, fileLinks: 1, events: 8 } });
  assert.equal(categories.find(c => c.key === "sm_time")?.count, 7);
  assert.equal(categories.find(c => c.key === "sm_requests")?.count, 5);
  assert.equal(categories.find(c => c.key === "sm_security")?.count, 18);
  assert.equal(categories.find(c => c.key === "SMDurcharbeit_monthly")?.count, 15);
  assert.equal(categories.find(c => c.key === "sm_answers")?.count, counts.answers);
  assert.equal(categories.find(c => c.key === "sm_photos")?.count, counts.photos);
  assert.match(categories.find(c => c.key === "SMDurcharbeit_monthly")?.actionHint ?? "", /Originaldateien/);
});
