import assert from "node:assert/strict";
import test from "node:test";
import { resolveMainSourceSections } from "./lib/praemien-source-catalog.js";

test("active main questionnaires expose every configured section", () => {
  assert.deepEqual(
    resolveMainSourceSections({
      fragebogenSections: ["standard", "flex"],
      fragebogenStatus: "active",
      campaignSection: null,
    }),
    ["standard", "flex"],
  );
});

test("inactive Billa questionnaires remain available while used by a live Billa campaign", () => {
  assert.deepEqual(
    resolveMainSourceSections({
      fragebogenSections: ["billa"],
      fragebogenStatus: "inactive",
      campaignSection: "billa",
    }),
    ["billa"],
  );
});

test("inactive questionnaires are not exposed through an unrelated campaign section", () => {
  assert.deepEqual(
    resolveMainSourceSections({
      fragebogenSections: ["billa"],
      fragebogenStatus: "inactive",
      campaignSection: "flex",
    }),
    [],
  );
});

test("unused inactive questionnaires stay out of the premium source catalog", () => {
  assert.deepEqual(
    resolveMainSourceSections({
      fragebogenSections: ["billa"],
      fragebogenStatus: "inactive",
      campaignSection: null,
    }),
    [],
  );
});
