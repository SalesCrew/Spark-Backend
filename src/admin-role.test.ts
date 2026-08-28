import assert from "node:assert/strict";
import test from "node:test";
import { isFullAdminRole, isRoleAllowedForEndpoint } from "./lib/admin-role.js";

test("admin and sm_admin are both full admin roles", () => {
  assert.equal(isFullAdminRole("admin"), true);
  assert.equal(isFullAdminRole("sm_admin"), true);
  assert.equal(isFullAdminRole("gm"), false);
  assert.equal(isFullAdminRole("sm"), false);
  assert.equal(isFullAdminRole("kunde"), false);
});

test("sm_admin only reaches endpoints that explicitly allow sm_admin", () => {
  assert.equal(isRoleAllowedForEndpoint("sm_admin", ["admin"]), false);
  assert.equal(isRoleAllowedForEndpoint("sm_admin", ["admin", "kunde"]), false);
  assert.equal(isRoleAllowedForEndpoint("sm_admin", ["admin", "sm_admin"]), true);
  assert.equal(isRoleAllowedForEndpoint("sm_admin", ["gm"]), false);
});

test("roles are matched exactly", () => {
  assert.equal(isRoleAllowedForEndpoint("admin", ["sm_admin"]), false);
  assert.equal(isRoleAllowedForEndpoint("admin", ["admin"]), true);
  assert.equal(isRoleAllowedForEndpoint("gm", ["gm"]), true);
});
