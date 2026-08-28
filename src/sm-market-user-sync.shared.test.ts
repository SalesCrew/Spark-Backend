import assert from "node:assert/strict";
import test from "node:test";
import { resolveAutomaticSmNameMatch, scoreSmNameMatch } from "./sm-market-user-sync.shared.js";

test("SM name matching ignores order, capitalization, punctuation, and dashes", () => {
  const result = resolveAutomaticSmNameMatch("MUSTERMANN-Max", [
    { id: "max", name: "Max Mustermann" },
    { id: "erika", name: "Erika Musterfrau" },
  ]);
  assert.equal(result.match?.id, "max");
  assert.equal(result.method, "exact");
});

test("SM name matching tolerates umlaut spelling variants and small typos", () => {
  assert.ok(scoreSmNameMatch("Herber Günter", "Guenter Herber") >= 0.9);
  const result = resolveAutomaticSmNameMatch("Herber Günter", [
    { id: "guenter", name: "Guenter Herber" },
    { id: "stocker", name: "Georg Stockreiter" },
  ]);
  assert.equal(result.match?.id, "guenter");
  assert.equal(result.method, "fuzzy");
});

test("SM name matching never auto-selects duplicate or ambiguous accounts", () => {
  const result = resolveAutomaticSmNameMatch("Max Mustermann", [
    { id: "first", name: "Max Mustermann" },
    { id: "second", name: "Mustermann Max" },
  ]);
  assert.equal(result.match, null);
  assert.equal(result.method, null);
});

test("SM name matching leaves unrelated names for manual review", () => {
  const result = resolveAutomaticSmNameMatch("Herber Günter", [
    { id: "test", name: "Test Account" },
  ]);
  assert.equal(result.match, null);
  assert.equal(result.method, null);
});
