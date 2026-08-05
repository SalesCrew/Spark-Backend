import assert from "node:assert/strict";
import test from "node:test";
import { buildKuehlerUpdatePatch } from "./routes/markets.js";

test("Kühler update import patches only explicitly mapped non-empty values", () => {
  const patch = buildKuehlerUpdatePatch(
    {
      kuehlerInternalId: "INT-NEW",
      kuehlerTechnicalIdentNo: "TECH-NEW",
      kuehlerSerialNumber: "SERIAL-NEW",
      kuehlerBd: "BD-NEW",
      kuehlerAnzahlKsAmStandort: 4,
      kuehlerModel: "MODEL-NEW",
      name: "Kühler Neu",
      employee: "GM Neu",
    },
    ["kuehlerTechnicalIdentNo"],
  );

  assert.deepEqual(patch, { kuehlerTechnicalIdentNo: "TECH-NEW" });
});

test("Kühler update import leaves a mapped but empty spreadsheet cell unchanged", () => {
  const patch = buildKuehlerUpdatePatch(
    {
      kuehlerMatchValue: "INT-OLD",
    },
    ["kuehlerTechnicalIdentNo", "kuehlerModel", "employee"],
  );

  assert.deepEqual(patch, {});
});

test("Kühler update import can patch several selected datasets together", () => {
  const patch = buildKuehlerUpdatePatch(
    {
      kuehlerTechnicalIdentNo: " TECH-4711 ",
      kuehlerAnzahlKsAmStandort: 3,
      kuehlerModel: " CCH-500 ",
    },
    ["kuehlerTechnicalIdentNo", "kuehlerAnzahlKsAmStandort", "kuehlerModel"],
  );

  assert.deepEqual(patch, {
    kuehlerTechnicalIdentNo: "TECH-4711",
    kuehlerAnzahlKsAmStandort: 3,
    kuehlerModel: "CCH-500",
  });
});
