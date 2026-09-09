import assert from "node:assert/strict";
import test from "node:test";
import { SM_INACTIVE_BAN_DURATION, smAuthBanDurationForStatus } from "./sm-user-status.shared.js";

test("inactive SM accounts remain banned until an admin explicitly reactivates them", () => {
  assert.equal(smAuthBanDurationForStatus(false), SM_INACTIVE_BAN_DURATION);
  assert.equal(smAuthBanDurationForStatus(true), "none");
});
