import assert from "node:assert/strict";
import test from "node:test";
import {
  appendAdminKurtiExcelExports,
  parseAdminKurtiExcelExportArguments,
  parseAdminKurtiStoredExports,
  type AdminKurtiExcelExport,
} from "./lib/admin-kurti-exports.js";

const filters = {
  dateFrom: "2026-07-01",
  dateTo: "2026-07-31",
  gmUserIds: [],
  gmNames: ["Irene Traxler"],
  regions: ["Nord"],
  campaignIds: [],
  campaignNames: [],
  marketIds: [],
  marketSearch: null,
  sections: [],
  statuses: ["abgeschlossen"],
  search: null,
  includeLive: false,
} as const;

test("parses a validated Admin Kurti Excel export request", () => {
  const parsed = parseAdminKurtiExcelExportArguments(JSON.stringify({
    kind: "zeiterfassung",
    title: "Zeiterfassung Juli",
    description: "Nur Irene Traxler in Region Nord.",
    filters,
  }));

  assert.match(parsed.id, /^[0-9a-f-]{36}$/i);
  assert.equal(parsed.kind, "zeiterfassung");
  assert.deepEqual(parsed.filters.gmNames, ["Irene Traxler"]);
});

test("requires an explicit date range for time exports", () => {
  assert.throws(() => parseAdminKurtiExcelExportArguments(JSON.stringify({
    kind: "diaeten",
    title: "Diäten",
    description: null,
    filters: { ...filters, dateFrom: null, dateTo: null },
  })), /explicit date range/i);
});

test("stores and restores export cards without exposing their marker", () => {
  const exportSpec = parseAdminKurtiExcelExportArguments(JSON.stringify({
    kind: "zeiterfassung",
    title: "Zeiterfassung Juli",
    description: null,
    filters,
  })) as AdminKurtiExcelExport;
  const stored = appendAdminKurtiExcelExports("Der Export ist vorbereitet.", [exportSpec]);

  assert.match(stored, /admin-kurti-exports-v1/);
  const restored = parseAdminKurtiStoredExports(stored);
  assert.equal(restored.content, "Der Export ist vorbereitet.");
  assert.deepEqual(restored.exports, [exportSpec]);
});

test("hides malformed export markers and returns no download card", () => {
  const restored = parseAdminKurtiStoredExports("Antwort\n<!--admin-kurti-exports-v1:bm90LWpzb24-->");
  assert.equal(restored.content, "Antwort");
  assert.deepEqual(restored.exports, []);
});
