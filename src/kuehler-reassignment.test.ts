import assert from "node:assert/strict";
import test from "node:test";
import { buildKuehlerAssignmentProgress, createKuehlerSubmissionAssigneeResolver, kuehlerProgressKey, type KuehlerReassignment } from "./lib/kuehler-assignment-progress.js";
import { kuehlerSubmissionInDateRange } from "./lib/kuehler-repeat-visits.js";

const units = [{ id: "cooler-a", marketId: "market" }, { id: "cooler-b", marketId: "market" }];
const assignment = (gmUserId = "pascal", visitTargetCount = 2, campaignId = "campaign", marketId = "market") => ({ campaignId, marketId, gmUserId, visitTargetCount });
const submission = (sessionId = "visit", gmUserId = "alex", kuehlerUnitId: string | null = "cooler-a", submittedAt: string | null = "2026-09-11T10:00:00Z") => ({
  sessionId, campaignId: "campaign", marketId: "market", gmUserId, kuehlerUnitId, submittedAt: submittedAt ? new Date(submittedAt) : null,
});
const transfer = (fromGmUserId = "alex", toGmUserId = "pascal", at = "2026-09-14T06:23:31.301Z"): KuehlerReassignment => ({
  marketId: "market", section: "kuehler", fromCampaignId: "campaign", toCampaignId: "campaign", fromGmUserId, toGmUserId,
  migratedAt: new Date(at), reason: "campaign_gm_reassignment",
});
const key = (gm = "pascal") => kuehlerProgressKey("campaign", "market", gm);

test("reassignment keeps completion, exact session and original author without mutating inputs", () => {
  const visits = [submission(), submission("visit-b", "alex", "cooler-b")];
  const before = structuredClone(visits);
  const result = buildKuehlerAssignmentProgress([assignment()], units, visits, [transfer()]);
  assert.deepEqual(result.get(key())?.map((slot) => slot.submission?.sessionId), ["visit", "visit-b"]);
  assert.ok(result.get(key())?.every((slot) => slot.submission?.gmUserId === "alex"));
  assert.deepEqual(visits, before);
});

test("repeated history rows do not repeat a transfer or double-count a visit", () => {
  const first = submission();
  const result = buildKuehlerAssignmentProgress([assignment("pascal", 4)], units, [first, first], Array.from({ length: 78 }, () => transfer()));
  assert.deepEqual(result.get(key())?.map((slot) => slot.submission?.sessionId ?? null), ["visit", null, null, null]);
});

test("unrelated GM, market, campaign and ordinary market migration never inherit completion", () => {
  const visit = submission();
  const unrelated = [
    { ...transfer(), marketId: "elsewhere" }, { ...transfer(), fromCampaignId: "other", toCampaignId: "other" },
    { ...transfer(), toCampaignId: "other" }, { ...transfer(), section: "standard" },
    { ...transfer(), reason: "market_migration" }, transfer("third", "pascal"),
  ];
  assert.equal(createKuehlerSubmissionAssigneeResolver(unrelated)(visit), "alex");
  assert.ok(buildKuehlerAssignmentProgress([assignment()], units, [visit], unrelated).get(key())?.every((slot) => !slot.submission));
});

test("multi-hop transfers, return transfers and subsequent independent visits follow chronological ownership", () => {
  const resolve = createKuehlerSubmissionAssigneeResolver([
    transfer("pascal", "third", "2026-09-16T08:00:00Z"), transfer(),
    transfer("third", "alex", "2026-09-18T08:00:00Z"),
  ]);
  assert.equal(resolve(submission()), "alex");
  assert.equal(resolve(submission("new-pascal", "pascal", "cooler-a", "2026-09-15T08:00:00Z")), "alex");
  assert.equal(resolve(submission("later-pascal", "pascal", "cooler-a", "2026-09-17T08:00:00Z")), "pascal");
});

test("simultaneous swaps are one atomic mapping rather than a chain or loop", () => {
  const resolve = createKuehlerSubmissionAssigneeResolver([transfer(), transfer("pascal", "alex"), transfer()]);
  assert.equal(resolve(submission()), "pascal");
  assert.equal(resolve(submission("pascal-visit", "pascal")), "alex");
});

test("completed source and destination visits merge only once; independent GMs remain separate", () => {
  const result = buildKuehlerAssignmentProgress([assignment("pascal", 1), assignment("pascal", 1), assignment("third", 1)], [units[0]!],
    [submission(), submission("pascal-visit", "pascal"), submission("third-visit", "third")], [transfer()]);
  assert.equal(result.get(key())?.length, 2);
  assert.equal(result.get(key())?.filter((slot) => slot.submission).length, 2);
  assert.equal(result.get(key("third"))?.[0]?.submission?.sessionId, "third-visit");
  assert.equal(new Set([...result.values()].flatMap((slots) => slots.flatMap((slot) => slot.submission ? [slot.submission.sessionId] : []))).size, 3);
});

test("late old-GM submissions do not follow an earlier transfer; drafts do not complete slots", () => {
  const visits = [submission("late", "alex", "cooler-a", "2026-09-14T07:00:00Z"), submission("draft", "alex", "cooler-b", null)];
  const result = buildKuehlerAssignmentProgress([assignment()], units, visits, [transfer()]);
  assert.ok(result.get(key())?.every((slot) => !slot.submission));
  assert.equal(createKuehlerSubmissionAssigneeResolver([transfer()])(submission("boundary", "alex", "cooler-a", "2026-09-14T06:23:31.301Z")), "pascal");
});

test("a fresh assignment to the original GM cannot consume already transferred completions", () => {
  const result = buildKuehlerAssignmentProgress([assignment("alex"), assignment()], units, [submission()], [transfer()]);
  assert.ok(result.get(key("alex"))?.every((slot) => !slot.submission));
  assert.equal(result.get(key())?.[0]?.submission?.sessionId, "visit");
});

test("date filters run after transfer and occurrence allocation, never moving repeat visit 2 into slot 1", () => {
  const result = buildKuehlerAssignmentProgress([assignment("pascal", 2)], [units[0]!],
    [submission(), submission("repeat", "pascal", "cooler-a", "2026-09-15T10:00:00Z")], [transfer()]);
  const visible = result.get(key())?.filter((slot) => kuehlerSubmissionInDateRange(slot.submission?.submittedAt ?? null, { dateFrom: "2026-09-15", dateTo: "2026-09-15" }));
  assert.deepEqual(visible?.map((slot) => [slot.visitNumber, slot.submission?.sessionId]), [[2, "repeat"]]);
});

test("an extra stored submission does not manufacture a target or overwrite an earlier result", () => {
  const visits = [submission(), submission("extra", "alex", "cooler-a", "2026-09-12T10:00:00Z")];
  const result = buildKuehlerAssignmentProgress([assignment("pascal", 1)], [units[0]!], visits, [transfer()]);
  assert.equal(result.get(key())?.length, 1);
  assert.equal(result.get(key())?.[0]?.submission?.sessionId, "visit");
  assert.equal(visits.length, 2); // Original sessions remain available to history/export.
});

test("legacy cooler visits survive reassignment but cannot complete two occurrences", () => {
  const result = buildKuehlerAssignmentProgress([assignment("pascal", 2)], [], [submission("legacy", "alex", null)], [transfer()]);
  assert.deepEqual(result.get(key())?.map((slot) => slot.submission?.sessionId ?? null), ["legacy", null]);
});

test("ambiguous historical targets fail closed and unassigned work does not inherit another GM's submission", () => {
  const resolve = createKuehlerSubmissionAssigneeResolver([transfer(), transfer("alex", "third")]);
  assert.equal(resolve(submission()), "alex");
  const result = buildKuehlerAssignmentProgress([{ ...assignment(), gmUserId: null }], units, [submission()], [transfer()]);
  assert.ok(result.get(kuehlerProgressKey("campaign", "market", null))?.every((slot) => !slot.submission));
});
