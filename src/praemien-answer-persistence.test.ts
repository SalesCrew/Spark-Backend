import assert from "node:assert/strict";
import test from "node:test";
import { isWaveAnswerPersistencePillarName } from "./lib/praemien-answer-persistence.js";

test("wave answer persistence recognizes the two configured pillar semantics", () => {
  assert.equal(isWaveAnswerPersistencePillarName("Schütten / Displays"), true);
  assert.equal(isWaveAnswerPersistencePillarName("Schütten/Displays"), true);
  assert.equal(isWaveAnswerPersistencePillarName("Schuetten / Displays"), true);
  assert.equal(isWaveAnswerPersistencePillarName("Distributionsziel"), true);
});

test("wave answer persistence does not leak into other premium pillars", () => {
  assert.equal(isWaveAnswerPersistencePillarName("Flexziel"), false);
  assert.equal(isWaveAnswerPersistencePillarName("Qualitätsziele"), false);
  assert.equal(isWaveAnswerPersistencePillarName("Displays"), false);
  assert.equal(isWaveAnswerPersistencePillarName("Distribution Reporting"), false);
});
