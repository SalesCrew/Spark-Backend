import assert from "node:assert/strict";
import test from "node:test";
import { selectAdminKurtiTools } from "./lib/admin-kurti-tool-routing.js";

test("keeps the chart renderer available on follow-up turns without chart keywords", () => {
  const toolNames = selectAdminKurtiTools("Doch, das solltest du schon bereit haben.")
    .map((tool) => tool.name);

  assert.ok(toolNames.includes("render_admin_chart"));
});
