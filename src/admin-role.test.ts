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

test("sm_admin inherits admin endpoints across both workspaces, but not worker-only routes", () => {
  assert.equal(isRoleAllowedForEndpoint("sm_admin", ["admin"]), true);
  assert.equal(isRoleAllowedForEndpoint("sm_admin", ["admin", "kunde"]), true);
  assert.equal(isRoleAllowedForEndpoint("sm_admin", ["admin", "sm_admin"]), true);
  assert.equal(isRoleAllowedForEndpoint("sm_admin", ["gm"]), false);
  assert.equal(isRoleAllowedForEndpoint("sm_admin", ["sm"]), false);
  assert.equal(isRoleAllowedForEndpoint("sm_admin", ["kunde"]), false);
  assert.equal(isRoleAllowedForEndpoint("sm_admin", []), false);
});

test("ordinary SM employees never inherit admin or GM access", () => {
  assert.equal(isRoleAllowedForEndpoint("sm", ["admin"]), false);
  assert.equal(isRoleAllowedForEndpoint("sm", ["admin", "sm_admin"]), false);
  assert.equal(isRoleAllowedForEndpoint("sm", ["admin", "gm", "kunde"]), false);
  assert.equal(isRoleAllowedForEndpoint("sm", ["gm"]), false);
  assert.equal(isRoleAllowedForEndpoint("sm", ["sm"]), true);
});

test("roles are matched exactly", () => {
  assert.equal(isRoleAllowedForEndpoint("admin", ["sm_admin"]), false);
  assert.equal(isRoleAllowedForEndpoint("admin", ["admin"]), true);
  assert.equal(isRoleAllowedForEndpoint("gm", ["gm"]), true);
});
