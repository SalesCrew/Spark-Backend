import assert from "node:assert/strict";
import test from "node:test";

import { filterFragebogenModuleLinksByActiveModuleIds } from "./lib/fragebogen-module-links.js";

test("questionnaires exclude links to soft-deleted modules", () => {
  const links = [
    { fragebogenId: "fragebogen", moduleId: "active", orderIndex: 0 },
    { fragebogenId: "fragebogen", moduleId: "deleted", orderIndex: 1 },
  ];

  assert.deepEqual(filterFragebogenModuleLinksByActiveModuleIds(links, ["active"]), [links[0]]);
});

test("questionnaire module order is preserved", () => {
  const links = [
    { moduleId: "second", orderIndex: 0 },
    { moduleId: "first", orderIndex: 1 },
  ];

  assert.deepEqual(
    filterFragebogenModuleLinksByActiveModuleIds(links, ["first", "second"]),
    links,
  );
});
