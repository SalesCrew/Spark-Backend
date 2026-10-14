import assert from "node:assert/strict";
import { test } from "node:test";
import { isQuestionApplicableToMarketChain } from "./lib/question-chain-applicability.js";

test("Billa Plus and Billa+ restrictions match in either direction", () => {
  for (const configured of ["Billa Plus", "Billa+", " BILLA PLUS ", "Billa +"]) {
    for (const market of ["Billa Plus", "Billa+", " BILLA+ "]) {
      assert.equal(isQuestionApplicableToMarketChain([configured], market), true, configured + " / " + market);
    }
  }
});

test("aliases do not broaden restrictions to other individual chains or groups", () => {
  for (const market of ["Billa", "Billa Corso", "REWE", "Spar", "ESP", "ISP", "", null]) {
    assert.equal(isQuestionApplicableToMarketChain(["Billa Plus"], market), false);
  }
  assert.equal(isQuestionApplicableToMarketChain(["REWE"], "Billa+"), false);
  assert.equal(isQuestionApplicableToMarketChain(["Billa"], "Billa Plus"), false);
  assert.equal(isQuestionApplicableToMarketChain(["SPAR"], "ESP"), false);
  assert.equal(isQuestionApplicableToMarketChain(["Billa", "Billa Plus", "REWE"], "Billa+"), true);
});

test("unrestricted questions and ordinary case-insensitive matches keep their behavior", () => {
  for (const chains of [undefined, null, [], ["", " "]]) {
    assert.equal(isQuestionApplicableToMarketChain(chains, "Spar"), true);
    assert.equal(isQuestionApplicableToMarketChain(chains, null), true);
  }
  const configured = [" Billa Plus ", " Spar "];
  assert.equal(isQuestionApplicableToMarketChain(configured, "SPAR"), true);
  assert.deepEqual(configured, [" Billa Plus ", " Spar "]);
});
