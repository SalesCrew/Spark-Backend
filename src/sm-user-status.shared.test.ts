import assert from "node:assert/strict";
import test from "node:test";
import { INACTIVE_ACCOUNT_BAN_DURATION, SM_INACTIVE_BAN_DURATION, authBanDurationForStatus, canToggleAccountStatus, smAuthBanDurationForStatus } from "./sm-user-status.shared.js";

test("inactive SM accounts remain banned until an admin explicitly reactivates them", () => {
  assert.equal(smAuthBanDurationForStatus(false), SM_INACTIVE_BAN_DURATION);
  assert.equal(smAuthBanDurationForStatus(true), "none");
});

test("GM and SM accounts share reversible inactive-login handling", () => {
  assert.equal(authBanDurationForStatus(false), INACTIVE_ACCOUNT_BAN_DURATION);
  assert.equal(authBanDurationForStatus(true), "none");
  assert.equal(canToggleAccountStatus("gm"), true);
  assert.equal(canToggleAccountStatus("sm"), true);
  assert.equal(canToggleAccountStatus("admin"), false);
  assert.equal(canToggleAccountStatus("sm_admin"), false);
  assert.equal(canToggleAccountStatus("kunde"), false);
});
