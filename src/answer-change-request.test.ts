import assert from "node:assert/strict";
import test from "node:test";
import {
  readRequestedCommentChange,
  requestedCommentSummary,
} from "./lib/answer-change-request.js";

test("recognizes and trims a requested comment change", () => {
  assert.deepEqual(
    readRequestedCommentChange({
      changeKind: "comment",
      requestedComment: "  Nachtrag für den Besuch  ",
    }),
    { kind: "comment", comment: "Nachtrag für den Besuch" },
  );
});

test("keeps answer-change payloads separate from comment changes", () => {
  assert.deepEqual(
    readRequestedCommentChange({ type: "yesno", value: "Ja" }),
    { kind: "not_comment" },
  );
});

test("rejects malformed and oversized comment requests", () => {
  assert.equal(
    readRequestedCommentChange({ changeKind: "comment" }).kind,
    "invalid",
  );
  assert.equal(
    readRequestedCommentChange({
      changeKind: "comment",
      requestedComment: "x".repeat(4001),
    }).kind,
    "invalid",
  );
});

test("builds a bounded, readable admin summary", () => {
  const summary = requestedCommentSummary("x".repeat(900));
  assert.match(summary, /^Kommentar: /);
  assert.ok(summary.length <= 633);
  assert.equal(requestedCommentSummary(""), "Kommentar entfernen");
});
