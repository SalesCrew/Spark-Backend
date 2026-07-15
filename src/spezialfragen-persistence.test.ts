import assert from "node:assert/strict";
import test from "node:test";
import { canonicalizeSpezialfragenIds } from "./lib/spezialfragen-persistence.js";

test("temporary Spezialfrage IDs and their rule references become durable UUIDs", () => {
  const generatedIds = [
    "11111111-1111-4111-8111-111111111111",
    "22222222-2222-4222-8222-222222222222",
  ];

  const normalized = canonicalizeSpezialfragenIds(
    [
      {
        id: "sq-1-legacy",
        rules: [{ triggerQuestionId: "", targetQuestionIds: ["sq-2-legacy"] }],
      },
      {
        id: "sq-2-legacy",
        rules: [{ triggerQuestionId: "sq-1-legacy", targetQuestionIds: [] }],
      },
    ],
    () => generatedIds.shift() ?? "33333333-3333-4333-8333-333333333333",
  );

  assert.equal(normalized[0]?.id, "11111111-1111-4111-8111-111111111111");
  assert.deepEqual(normalized[0]?.rules[0]?.targetQuestionIds, ["22222222-2222-4222-8222-222222222222"]);
  assert.equal(normalized[1]?.id, "22222222-2222-4222-8222-222222222222");
  assert.equal(normalized[1]?.rules[0]?.triggerQuestionId, "11111111-1111-4111-8111-111111111111");
});

test("already persisted Spezialfrage UUIDs remain stable", () => {
  const existingId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
  const [normalized] = canonicalizeSpezialfragenIds([{ id: existingId, rules: [] }]);
  assert.equal(normalized?.id, existingId);
});

test("duplicate Spezialfrage IDs are rejected before persistence", () => {
  assert.throws(
    () => canonicalizeSpezialfragenIds([
      { id: "sq-duplicate", rules: [] },
      { id: "sq-duplicate", rules: [] },
    ]),
    /eindeutig/,
  );
});
