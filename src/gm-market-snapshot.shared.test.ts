import assert from "node:assert/strict";
import test from "node:test";
import { planGmMarketSnapshot, planKuehlerMarketSnapshot, type KuehlerSnapshotMarket, type SnapshotMarket } from "./gm-market-snapshot.shared.js";

test("GM snapshot updates ten matches, adds five new Flex numbers and inactivates absent GM markets", () => {
  const existing: SnapshotMarket[] = Array.from({ length: 11 }, (_, index) => ({
    id: `market-${index + 1}`, flexNumber: `F${index + 1}`, marketType: "universum", isActive: true, name: `Market ${index + 1}`,
  }));
  const incoming = [
    ...Array.from({ length: 10 }, (_, index) => ({ row: index + 2, flexNumber: `F${index + 1}` })),
    ...Array.from({ length: 5 }, (_, index) => ({ row: index + 12, flexNumber: `NEW${index + 1}` })),
  ];
  const plan = planGmMarketSnapshot(existing, incoming);
  assert.equal(plan.matched.length, 10);
  assert.equal(plan.newRows.length, 5);
  assert.deepEqual(plan.toDeactivate.map((market) => market.id), ["market-11"]);
  assert.deepEqual(plan.errors, []);
});

test("shared Kühler markets remain active while a missing pure GM market is inactivated", () => {
  const existing: SnapshotMarket[] = [
    { id: "gm", flexNumber: "GM", marketType: "universum", isActive: true, name: "GM" },
    { id: "shared", flexNumber: "SHARED", marketType: "both", isActive: true, name: "Shared" },
    { id: "kuehler", flexNumber: "COOL", marketType: "kuehler", isActive: true, name: "Kühler" },
  ];
  const plan = planGmMarketSnapshot(existing, [{ row: 2, flexNumber: "NEW" }]);
  assert.deepEqual(plan.toDeactivate.map((market) => market.id), ["gm"]);
  assert.deepEqual(plan.sharedLeftActive.map((market) => market.id), ["shared"]);
  const fullUniversumSnapshot = planGmMarketSnapshot(existing, [{ row: 2, flexNumber: "NEW" }], { deactivateSharedMarkets: true });
  assert.deepEqual(fullUniversumSnapshot.toDeactivate.map((market) => market.id), ["gm", "shared"]);
  assert.deepEqual(fullUniversumSnapshot.sharedLeftActive, []);
});

test("duplicate, blank, ambiguous and Kühler-only Flex identities block the snapshot", () => {
  const existing: SnapshotMarket[] = [
    { id: "cool", flexNumber: "K", marketType: "kuehler", isActive: true, name: "Cool" },
    { id: "a", flexNumber: "D", marketType: "universum", isActive: true, name: "A" },
    { id: "b", flexNumber: "d", marketType: "universum", isActive: true, name: "B" },
  ];
  const plan = planGmMarketSnapshot(existing, [
    { row: 2, flexNumber: "" }, { row: 3, flexNumber: "F" }, { row: 4, flexNumber: " f " },
    { row: 5, flexNumber: "K" }, { row: 6, flexNumber: "D" },
  ]);
  assert.equal(plan.errors.length, 4);
  assert.equal(plan.newRows.length, 1);
});

test("Kühler snapshot groups units by Stammnr and ignores Universum-only markets", () => {
  const existing: KuehlerSnapshotMarket[] = [
    { id: "gm", name: "GM", marketType: "universum", isActive: true, flexNumber: "F1", cokeMasterNumber: "C1", kuehlerStammnr: null },
    { id: "cool", name: "Cool", marketType: "kuehler", isActive: true, flexNumber: null, cokeMasterNumber: "C2", kuehlerStammnr: "C2" },
    { id: "shared", name: "Shared", marketType: "both", isActive: true, flexNumber: "F3", cokeMasterNumber: "C3", kuehlerStammnr: "C3" },
  ];
  const plan = planKuehlerMarketSnapshot(existing, [
    { row: 2, stammnr: "C2", flexNumber: null },
    { row: 3, stammnr: "C2", flexNumber: null },
    { row: 4, stammnr: "C4", flexNumber: null },
  ]);
  assert.equal(plan.matched.length, 2);
  assert.equal(plan.newRows.length, 1);
  assert.deepEqual(plan.toDeactivate.map((market) => market.id), ["shared"]);
  assert.deepEqual(plan.errors, []);
});

test("Kühler snapshot rejects conflicting Stammnr and Flex identities", () => {
  const existing: KuehlerSnapshotMarket[] = [
    { id: "a", name: "A", marketType: "both", isActive: true, flexNumber: "F1", cokeMasterNumber: "C1", kuehlerStammnr: "C1" },
    { id: "b", name: "B", marketType: "both", isActive: true, flexNumber: "F2", cokeMasterNumber: "C2", kuehlerStammnr: "C2" },
  ];
  const plan = planKuehlerMarketSnapshot(existing, [{ row: 2, stammnr: "C1", flexNumber: "F2" }]);
  assert.equal(plan.errors.length, 1);
  assert.equal(plan.matched.length, 0);
});

test("Kühler snapshot keeps a stored Stammnr when its Excel cell is blank but Flex identifies the market", () => {
  const existing: KuehlerSnapshotMarket[] = [
    { id: "cool", name: "Cool", marketType: "both", isActive: true, flexNumber: "F1", cokeMasterNumber: "123456", kuehlerStammnr: "123456" },
  ];
  const plan = planKuehlerMarketSnapshot(existing, [
    { row: 2, stammnr: null, flexNumber: "F1" },
    { row: 3, stammnr: null, flexNumber: "F1" },
  ]);
  assert.deepEqual(plan.matched.map((item) => item.market.id), ["cool", "cool"]);
  assert.deepEqual(plan.toDeactivate, []);
  assert.deepEqual(plan.newRows, []);
  assert.deepEqual(plan.errors, []);
});

test("Kühler snapshot accepts a nonblank corrected Stammnr but rejects an unidentified blank one", () => {
  const existing: KuehlerSnapshotMarket[] = [
    { id: "cool", name: "Cool", marketType: "kuehler", isActive: true, flexNumber: "F1", cokeMasterNumber: "123456", kuehlerStammnr: "123456" },
  ];
  const corrected = planKuehlerMarketSnapshot(existing, [{ row: 2, stammnr: "1234567", flexNumber: "F1" }]);
  assert.equal(corrected.matched[0]?.market.id, "cool");
  assert.deepEqual(corrected.errors, []);
  const unidentified = planKuehlerMarketSnapshot(existing, [{ row: 2, stammnr: null, flexNumber: "UNKNOWN" }]);
  assert.equal(unidentified.matched.length, 0);
  assert.equal(unidentified.errors.length, 1);
});

test("Kühler snapshot rejects two different Stammnummern for the same existing market", () => {
  const existing: KuehlerSnapshotMarket[] = [
    { id: "cool", name: "Cool", marketType: "kuehler", isActive: true, flexNumber: "F1", cokeMasterNumber: "123456", kuehlerStammnr: "123456" },
  ];
  const plan = planKuehlerMarketSnapshot(existing, [
    { row: 2, stammnr: "1234567", flexNumber: "F1" },
    { row: 3, stammnr: "123456", flexNumber: null },
  ]);
  assert.equal(plan.errors.length, 1);
});
