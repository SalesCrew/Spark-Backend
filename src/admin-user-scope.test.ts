import assert from "node:assert/strict";
import test from "node:test";
import { canManageUserRole, getRestrictedDirectoryRole } from "./lib/admin-user-scope.js";
import type { UserRole } from "./lib/schema.js";

const roles: UserRole[] = ["admin", "sm_admin", "gm", "sm", "kunde"];

test("the existing admin role keeps full user-management scope", () => {
  for (const role of roles) {
    assert.equal(canManageUserRole("admin", role), true);
  }
  assert.equal(getRestrictedDirectoryRole("admin"), undefined);
});

test("sm_admin has the same full user-management scope as admin", () => {
  for (const role of roles) {
    assert.equal(canManageUserRole("sm_admin", role), true);
  }
  assert.equal(getRestrictedDirectoryRole("sm_admin"), undefined);
});

test("kunde keeps its existing GM-only management scope", () => {
  for (const role of roles) {
    assert.equal(canManageUserRole("kunde", role), role === "gm");
  }
  assert.equal(getRestrictedDirectoryRole("kunde"), "gm");
});
