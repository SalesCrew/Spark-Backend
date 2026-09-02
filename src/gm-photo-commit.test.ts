import assert from "node:assert/strict";
import test from "node:test";
import { planGmPhotoCommit } from "./lib/gm-photo-commit.js";

test("append keeps photos omitted by a stale client and adds only the new path", () => {
  assert.deepEqual(
    planGmPhotoCommit(["first.jpg"], ["second.jpg"], "append"),
    {
      insertPaths: ["second.jpg"],
      updatePaths: [],
      deletePaths: [],
    },
  );
});

test("append is idempotent for an already committed path", () => {
  assert.deepEqual(
    planGmPhotoCommit(["first.jpg", "second.jpg"], ["second.jpg", "second.jpg"], "append"),
    {
      insertPaths: [],
      updatePaths: ["second.jpg"],
      deletePaths: [],
    },
  );
});

test("replace removes only paths that the complete client snapshot omitted", () => {
  assert.deepEqual(
    planGmPhotoCommit(["first.jpg", "second.jpg"], ["second.jpg"], "replace"),
    {
      insertPaths: [],
      updatePaths: ["second.jpg"],
      deletePaths: ["first.jpg"],
    },
  );
});
