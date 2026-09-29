import assert from "node:assert/strict";
import test from "node:test";
import {
  calendarQuarterDateWindow,
  dateWindowsOverlap,
  isQuarterAnswerPersistencePillarName,
  isWaveAnswerPersistencePillarName,
  quarterPersistentQuestionIds,
  revalidateReusableAnswer,
} from "./lib/praemien-answer-persistence.js";

test("quarter answer persistence recognizes Distributionsziel and explicit Flexziel", () => {
  assert.equal(isQuarterAnswerPersistencePillarName("Distributionsziel"), true);
  assert.equal(isQuarterAnswerPersistencePillarName("Distributions-Ziel"), true);
  assert.equal(isQuarterAnswerPersistencePillarName("  DISTRIBUTIONSZIEL  "), true);
  assert.equal(isQuarterAnswerPersistencePillarName("Flexziel"), true);
  assert.equal(isQuarterAnswerPersistencePillarName("  FLEX-ZIEL  "), true);
});

test("quarter answer persistence does not leak into other premium pillars", () => {
  assert.equal(isQuarterAnswerPersistencePillarName("Schütten / Displays"), false);
  assert.equal(isQuarterAnswerPersistencePillarName("Flex"), false);
  assert.equal(isQuarterAnswerPersistencePillarName("Flex Reporting"), false);
  assert.equal(isQuarterAnswerPersistencePillarName("Qualitätsziele"), false);
  assert.equal(isQuarterAnswerPersistencePillarName("Distribution Reporting"), false);
});

test("legacy marker also recognizes new Flexziel source configurations", () => {
  assert.equal(isWaveAnswerPersistencePillarName("Distributionsziel"), true);
  assert.equal(isWaveAnswerPersistencePillarName("Schütten / Displays"), true);
  assert.equal(isWaveAnswerPersistencePillarName("Flexziel"), true);
});

test("calendar quarter uses Vienna local time and resets exactly at the boundary", () => {
  assert.deepEqual(calendarQuarterDateWindow(new Date("2026-09-30T21:59:59.000Z")), {
    startDate: "2026-07-01",
    endDate: "2026-09-30",
    timezone: "Europe/Vienna",
  });
  assert.deepEqual(calendarQuarterDateWindow(new Date("2026-09-30T22:00:00.000Z")), {
    startDate: "2026-10-01",
    endDate: "2026-12-31",
    timezone: "Europe/Vienna",
  });
  assert.deepEqual(calendarQuarterDateWindow(new Date("2028-02-29T12:00:00.000Z")), {
    startDate: "2028-01-01",
    endDate: "2028-03-31",
    timezone: "Europe/Vienna",
  });
  assert.equal(
    calendarQuarterDateWindow(new Date("2026-12-31T22:59:59.000Z")).endDate,
    "2026-12-31",
  );
  assert.equal(
    calendarQuarterDateWindow(new Date("2026-12-31T23:00:00.000Z")).startDate,
    "2027-01-01",
  );
});

test("wave overlap only identifies configuration; the reuse window remains the calendar quarter", () => {
  const quarter = { startDate: "2026-10-01", endDate: "2026-12-31" };
  assert.equal(dateWindowsOverlap(quarter, { startDate: "2026-07-01", endDate: "2026-12-15" }), true);
  assert.equal(dateWindowsOverlap(quarter, { startDate: "2026-12-31", endDate: "2027-01-15" }), true);
  assert.equal(dateWindowsOverlap(quarter, { startDate: "2026-07-01", endDate: "2026-09-30" }), false);
});

test("only linked distribution and Flexziel questions opt into quarter reuse, including existing Flex mappings", () => {
  assert.deepEqual(
    quarterPersistentQuestionIds([
      { questionId: "distribution-a", pillarName: "Distributionsziel", carryAnswersForWave: true },
      { questionId: "distribution-a", pillarName: "Distributionsziel", carryAnswersForWave: true },
      { questionId: "distribution-disabled", pillarName: "Distributionsziel", carryAnswersForWave: false },
      { questionId: "flex-existing", pillarName: "Flexziel", carryAnswersForWave: false },
      { questionId: "flex-new", pillarName: "Flexziel", carryAnswersForWave: true },
      { questionId: "flex-existing", pillarName: "Flex-Ziel", carryAnswersForWave: false },
      { questionId: "not-a-goal", pillarName: "Flex", carryAnswersForWave: true },
      { questionId: "display", pillarName: "Schütten / Displays", carryAnswersForWave: true },
      { questionId: "quality", pillarName: "Qualitätsziele", carryAnswersForWave: true },
    ]),
    ["distribution-a", "flex-existing", "flex-new"],
  );
});

test("reused answers are revalidated against the current question snapshot", () => {
  const source = {
    questionType: "yesno",
    answerStatus: "answered",
    valueText: "Ja",
    valueNumber: null,
    valueJson: { raw: "Ja" },
    isValid: true,
  };
  assert.equal(
    revalidateReusableAnswer(source, { questionType: "yesno", config: { options: ["Ja", "Nein"] } })?.valueText,
    "Ja",
  );
  assert.equal(
    revalidateReusableAnswer(source, { questionType: "yesno", config: { options: ["Vorhanden", "Fehlt"] } }),
    null,
  );
  assert.equal(
    revalidateReusableAnswer({ ...source, answerStatus: "unanswered" }, { questionType: "yesno", config: {} }),
    null,
  );
  assert.equal(
    revalidateReusableAnswer({ ...source, questionType: "single" }, { questionType: "yesno", config: {} }),
    null,
  );
});

test("yes/no multi answers remain filled when their current options are still valid", () => {
  const validation = revalidateReusableAnswer(
    {
      questionType: "yesnomulti",
      answerStatus: "answered",
      valueText: "Ja",
      valueNumber: null,
      valueJson: { raw: { sel: "Ja", subs: ["Produkt A"] } },
      isValid: true,
    },
    {
      questionType: "yesnomulti",
      config: {
        answers: ["Ja", "Nein"],
        branches: [{ answer: "Ja", options: ["Produkt A", "Produkt B"] }],
      },
    },
  );
  assert.equal(validation?.valueText, "Ja");
  assert.deepEqual(validation?.options.map((option) => option.optionValue), ["Ja", "Produkt A"]);
});

test("numeric cumulative totals including zero survive, but changed ranges and photos do not", () => {
  const source = {
    questionType: "numeric", answerStatus: "answered", valueText: null,
    valueNumber: "3", valueJson: { raw: 3 }, isValid: true,
  };
  assert.equal(revalidateReusableAnswer(source, { questionType: "numeric", config: {} })?.valueNumber, "3");
  assert.equal(revalidateReusableAnswer({ ...source, valueNumber: "0", valueJson: { raw: 0 } }, { questionType: "numeric", config: { min: 0 } })?.valueNumber, "0");
  assert.equal(revalidateReusableAnswer(source, { questionType: "numeric", config: { max: 2 } }), null);
  assert.equal(revalidateReusableAnswer({ ...source, isValid: false }, { questionType: "numeric", config: {} }), null);
  assert.equal(revalidateReusableAnswer({ ...source, questionType: "photo", valueJson: { storage: ["old.jpg"] } }, { questionType: "photo", config: {} }), null);
});
