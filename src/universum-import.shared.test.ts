import assert from "node:assert/strict";
import test from "node:test";
import { classifyUniversumImportRow, type UniversumIdentityIndexes } from "./universum-import.shared.js";

type Market = { id: string };
function indexes(): UniversumIdentityIndexes<Market> {
  return { byStandard: new Map(), byCoke: new Map(), byFlex: new Map() };
}

test("ordinary Universum import matches by Flex, not by another identity", () => {
  const existing = indexes();
  const market = { id: "saved" };
  existing.byFlex.set("f-100", market);
  existing.byCoke.set("c-100", market);
  assert.deepEqual(classifyUniversumImportRow(
    { standard: "", coke: "c-100", flex: "f-100" }, existing, indexes(),
  ), { kind: "existing", value: market });
  assert.deepEqual(classifyUniversumImportRow(
    { standard: "", coke: "c-100", flex: "f-new" }, existing, indexes(),
  ), { kind: "identity-conflict", field: "cokeMasterNumber", value: "c-100" });
});

test("new Flex creates only when other identities are unclaimed", () => {
  const existing = indexes();
  const pending = indexes();
  const keys = { standard: "s-200", coke: "c-200", flex: "f-200" };
  assert.deepEqual(classifyUniversumImportRow(keys, existing, pending), { kind: "new" });
  pending.byFlex.set("f-200", { id: "pending" });
  assert.deepEqual(classifyUniversumImportRow(keys, existing, pending), { kind: "duplicate-in-file" });
});

test("mapped identities cannot be taken from a different market", () => {
  const existing = indexes();
  const matched = { id: "matched" };
  existing.byFlex.set("f-1", matched);
  existing.byStandard.set("s-1", { id: "other" });
  assert.deepEqual(classifyUniversumImportRow(
    { standard: "s-1", coke: "", flex: "f-1" }, existing, indexes(),
  ), { kind: "identity-conflict", field: "standardMarketNumber", value: "s-1" });
});
