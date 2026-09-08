import assert from "node:assert/strict";
import test from "node:test";

import { normalizeCrossDomainMarketIdentity } from "./market-identity.shared.js";

test("matches GM and SM market identities despite spacing and punctuation", () => {
  assert.equal(normalizeCrossDomainMarketIdentity(" 1200-013 951 "), "1200013951");
  assert.equal(normalizeCrossDomainMarketIdentity("1200 013.951"), "1200013951");
  assert.equal(normalizeCrossDomainMarketIdentity("at / sm-42"), "ATSM42");
});

test("normalizes unicode width and case without discarding meaningful characters", () => {
  assert.equal(normalizeCrossDomainMarketIdentity("ａｂ-００７"), "AB007");
  assert.equal(normalizeCrossDomainMarketIdentity("Ä-12"), "Ä12");
});

test("keeps leading zeroes significant and rejects empty identities", () => {
  assert.notEqual(normalizeCrossDomainMarketIdentity("00123"), normalizeCrossDomainMarketIdentity("123"));
  assert.equal(normalizeCrossDomainMarketIdentity(" - / "), null);
  assert.equal(normalizeCrossDomainMarketIdentity(null), null);
});
