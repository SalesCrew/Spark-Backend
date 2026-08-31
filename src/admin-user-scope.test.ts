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

test("sm_admin keeps full user-management scope in both workspaces", () => {
  for (const role of roles) {
    assert.equal(canManageUserRole("sm_admin", role), true);
  }
  assert.equal(getRestrictedDirectoryRole("sm_admin"), undefined);
});

test("SM and GM employees cannot manage user accounts", () => {
  for (const role of roles) {
    assert.equal(canManageUserRole("sm", role), false);
    assert.equal(canManageUserRole("gm", role), false);
  }
});

test("kunde keeps its existing GM-only management scope", () => {
  for (const role of roles) {
    assert.equal(canManageUserRole("kunde", role), role === "gm");
  }
  assert.equal(getRestrictedDirectoryRole("kunde"), "gm");
});
