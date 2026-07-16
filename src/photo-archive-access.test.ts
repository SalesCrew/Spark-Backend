import assert from "node:assert/strict";
import test from "node:test";
import { shouldExcludeMhdPhotos } from "./lib/photo-archive-access.js";

test("only the kunde role is excluded from MHD photos", () => {
  assert.equal(shouldExcludeMhdPhotos("kunde"), true);
  assert.equal(shouldExcludeMhdPhotos("admin"), false);
  assert.equal(shouldExcludeMhdPhotos("gm"), false);
  assert.equal(shouldExcludeMhdPhotos("sm"), false);
  assert.equal(shouldExcludeMhdPhotos(null), false);
  assert.equal(shouldExcludeMhdPhotos(undefined), false);
});
