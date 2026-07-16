import assert from "node:assert/strict";
import test from "node:test";
import { selectMissingSpezialfragenForSession } from "./lib/spezialfragen-session-sync.js";

test("adds only Spezialfragen that are missing from the submitted visit snapshot", () => {
  const existing = ["normal-question", "already-synced-special"];
  const candidates = [
    { questionId: "already-synced-special", text: "Schon vorhanden" },
    { questionId: "new-special", text: "Neu nachtragen" },
  ];

  assert.deepEqual(selectMissingSpezialfragenForSession(candidates, existing), [candidates[1]]);
});

test("collapses duplicate Spezialfrage candidates to keep repeated syncs idempotent", () => {
  const candidates = [
    { questionId: "new-special", order: 1 },
    { questionId: "new-special", order: 2 },
  ];

  assert.deepEqual(selectMissingSpezialfragenForSession(candidates, []), [candidates[0]]);
});
