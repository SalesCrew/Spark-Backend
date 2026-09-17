import assert from "node:assert/strict";
import test from "node:test";
import { buildUpdateOnlyMarketPatch, mapRowToDraft } from "./routes/markets.js";

const mapping = {
  flexNumber: "A",
  cokeMasterNumber: "B",
  standardMarketNumber: "C",
  name: "D",
};
const updateFields = ["cokeMasterNumber", "standardMarketNumber", "name"] as const;

test("GM update import leaves blank Stammnr and other blank cells untouched", () => {
  const draft = mapRowToDraft(["F1", "  ", "", "  "], mapping, "update");
  const patch = buildUpdateOnlyMarketPatch(draft, [...updateFields], null);
  assert.deepEqual(patch, {});

  const defensivePatch = buildUpdateOnlyMarketPatch(
    { cokeMasterNumber: " ", standardMarketNumber: "", name: "  " },
    [...updateFields],
    null,
  );
  assert.deepEqual(defensivePatch, {});
});

test("GM update import applies a nonblank corrected Stammnr and leaves other blanks untouched", () => {
  const draft = mapRowToDraft(["F1", "1234567", "", "Neuer Name"], mapping, "update");
  const patch = buildUpdateOnlyMarketPatch(draft, [...updateFields], null);
  assert.deepEqual(patch, { cokeMasterNumber: "1234567", name: "Neuer Name" });
});

test("explicit false and zero remain valid update values", () => {
  const draft = mapRowToDraft(["F1", "nein", "0"], {
    flexNumber: "A", infoFlag: "B", visitFrequencyPerYear: "C",
  }, "update");
  const patch = buildUpdateOnlyMarketPatch(draft, ["infoFlag", "visitFrequencyPerYear"], null);
  assert.deepEqual(patch, { infoFlag: false, visitFrequencyPerYear: 0 });
});

test("ordinary Universum import also keeps blank mapped cells out of matched-market patches", () => {
  const draft = mapRowToDraft(["F1", "  ", "Neuer Name"], {
    flexNumber: "A", cokeMasterNumber: "B", name: "C",
  }, "universum");
  const patch = buildUpdateOnlyMarketPatch(draft, ["cokeMasterNumber", "name"], null);
  assert.deepEqual(patch, { name: "Neuer Name" });
});
