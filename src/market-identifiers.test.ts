import assert from "node:assert/strict";
import test from "node:test";
import {
  buildSpecialArthurIdentifierReplacements,
  resolveKuehlerIdentity,
} from "./routes/markets.js";

test("editing either stored Kühler Stammnr value selects the changed identifier", () => {
  const result = resolveKuehlerIdentity({
    marketType: "both",
    incomingKuehlerStammnr: "1200000001",
    incomingCokeMasterNumber: "1200000099",
    existingKuehlerStammnr: "1200000001",
    existingCokeMasterNumber: "1200000001",
  });

  assert.deepEqual(result, { ok: true, stammnr: "1200000099" });
});

test("conflicting new Kühler identifiers are rejected", () => {
  const result = resolveKuehlerIdentity({
    marketType: "kuehler",
    incomingKuehlerStammnr: "1200000098",
    incomingCokeMasterNumber: "1200000099",
    existingKuehlerStammnr: "1200000001",
    existingCokeMasterNumber: "1200000001",
  });

  assert.equal(result.ok, false);
});

test("Arthur filter identifiers follow Stammnr and Flexnummer changes", () => {
  const replacements = buildSpecialArthurIdentifierReplacements(
    {
      cokeMasterNumber: "1200 000 001",
      kuehlerStammnr: "1200 000 001",
      flexNumber: "F-100",
    },
    {
      cokeMasterNumber: "1200000099",
      kuehlerStammnr: "1200000099",
      flexNumber: "F-200",
    },
  );

  assert.deepEqual(
    Array.from(replacements.entries()).sort(([left], [right]) => left.localeCompare(right)),
    [
      ["1200000001", "1200000099"],
      ["F-100", "F-200"],
    ],
  );
});

test("removed identifiers fall back to another current market identifier", () => {
  const replacements = buildSpecialArthurIdentifierReplacements(
    {
      cokeMasterNumber: "1200000001",
      kuehlerStammnr: null,
      flexNumber: "F-100",
    },
    {
      cokeMasterNumber: null,
      kuehlerStammnr: null,
      flexNumber: "F-100",
    },
  );

  assert.equal(replacements.get("1200000001"), "F-100");
});
