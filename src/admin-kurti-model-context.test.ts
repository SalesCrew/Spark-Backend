import assert from "node:assert/strict";
import test from "node:test";
import { buildAdminKurtiModelHistory } from "./lib/admin-kurti-model-context.js";
import type { AdminKurtiMemoryMessage } from "./lib/admin-kurti-memory.js";

function memoryMessage(index: number, content: string): AdminKurtiMemoryMessage {
  return {
    id: `message-${index}`,
    role: index % 2 === 0 ? "user" : "assistant",
    content,
    charts: [],
    visualizations: [],
    createdAt: new Date(2026, 6, 13, 8, index).toISOString(),
    expiresAt: new Date(2026, 6, 13, 16, index).toISOString(),
  };
}

test("keeps a small Admin Kurti history unchanged", () => {
  const history = [memoryMessage(0, "Erste Frage"), memoryMessage(1, "Erste Antwort")];
  const result = buildAdminKurtiModelHistory(history);
  assert.deepEqual(result, [
    { role: "user", content: "Erste Frage" },
    { role: "assistant", content: "Erste Antwort" },
  ]);
});

test("bounds a large eight-hour history while preserving recent context and an older digest", () => {
  const history = Array.from({ length: 80 }, (_, index) => memoryMessage(
    index,
    `${index}: ${"lange Nachricht ".repeat(500)}`,
  ));
  const result = buildAdminKurtiModelHistory(history);
  const serialized = JSON.stringify(result);

  assert.equal(result[0]?.role, "developer");
  assert.match(String(result[0]?.content), /GEKÜRZTER ÄLTERER VERLAUF/);
  assert.match(String(result.at(-1)?.content), /^79:/);
  assert.ok(serialized.length < 55_000, `history payload was ${serialized.length} characters`);
});
