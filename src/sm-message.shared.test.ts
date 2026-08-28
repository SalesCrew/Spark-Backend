import assert from "node:assert/strict";
import test from "node:test";

import { isSmMessageVisible, smMessageVisibleUntil } from "./sm-message.shared.js";

const readAt = new Date("2026-08-27T10:00:00.000Z");

test("keeps every unread message visible", () => {
  assert.equal(isSmMessageVisible({ readAt: null, visibleAfterReadDays: 0, now: new Date("2026-09-01T10:00:00.000Z") }), true);
});

test("hides a one-time message immediately after its immutable read timestamp", () => {
  assert.equal(isSmMessageVisible({ readAt, visibleAfterReadDays: 0, now: readAt }), false);
});

test("keeps historical NULL-policy messages visible", () => {
  assert.equal(isSmMessageVisible({ readAt, visibleAfterReadDays: null, now: new Date("2036-08-27T10:00:00.000Z") }), true);
});

test("uses exact 24-hour day windows for duration messages", () => {
  assert.equal(smMessageVisibleUntil(readAt, 2)?.toISOString(), "2026-08-29T10:00:00.000Z");
  assert.equal(isSmMessageVisible({ readAt, visibleAfterReadDays: 2, now: new Date("2026-08-29T09:59:59.999Z") }), true);
  assert.equal(isSmMessageVisible({ readAt, visibleAfterReadDays: 2, now: new Date("2026-08-29T10:00:00.000Z") }), false);
});
