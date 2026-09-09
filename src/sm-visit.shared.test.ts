import assert from "node:assert/strict";
import test from "node:test";

import {
  isCompleteSmVisitAnswer,
  isAnsweredSmVisitPayload,
  normalizeSmVisitAnswer,
  SmVisitAnswerValidationError,
  smVisitAnswerToRuleValue,
} from "./sm-visit.shared.js";
import { computeHiddenQuestionIds } from "./lib/conditional-visibility.js";

test("normalizes multi answers to published option order and removes duplicates", () => {
  const answer = normalizeSmVisitAnswer({
    type: "multiple",
    config: {},
    options: [{ code: "a", label: "Alpha" }, { code: "b", label: "Beta" }],
  }, { kind: "multi", optionCodes: ["b", "a", "b"] });
  assert.deepEqual(answer, { kind: "multi", optionCodes: ["a", "b"] });
  assert.deepEqual(smVisitAnswerToRuleValue(answer, [{ code: "a", label: "Alpha" }, { code: "b", label: "Beta" }]), ["Alpha", "Beta"]);
});

test("rejects a tampered option code", () => {
  assert.throws(() => normalizeSmVisitAnswer({
    type: "single",
    config: {},
    options: [{ code: "yes", label: "Ja" }, { code: "no", label: "Nein" }],
  }, { kind: "choice", optionCode: "admin-only" }), SmVisitAnswerValidationError);
});

test("validates yes/no multi branches and preserves published sub-option order", () => {
  const snapshot = {
    type: "yesnomulti" as const,
    config: { branches: [{ answer: "Ja", options: ["Kühler", "Regal", "Aktionsplatzierung"] }] },
    options: [{ code: "yes", label: "Ja" }, { code: "no", label: "Nein" }],
  };
  assert.deepEqual(normalizeSmVisitAnswer(snapshot, {
    kind: "yesnomulti",
    optionCode: "yes",
    subOptions: ["Regal", "Kühler", "Regal"],
  }), { kind: "yesnomulti", optionCode: "yes", subOptions: ["Kühler", "Regal"] });
  assert.throws(() => normalizeSmVisitAnswer(snapshot, {
    kind: "yesnomulti",
    optionCode: "yes",
    subOptions: ["Nicht veröffentlicht"],
  }), /unbekannte Unteroption/);
  assert.throws(() => normalizeSmVisitAnswer(snapshot, {
    kind: "yesnomulti",
    optionCode: "no",
    subOptions: ["Kühler"],
  }), /unbekannte Unteroption/);
});

test("enforces numeric integer and range configuration", () => {
  const snapshot = { type: "numeric" as const, config: { min: "1", max: "10", decimals: false }, options: [] };
  assert.deepEqual(normalizeSmVisitAnswer(snapshot, { kind: "number", value: 7 }), { kind: "number", value: 7 });
  assert.throws(() => normalizeSmVisitAnswer(snapshot, { kind: "number", value: 7.5 }), /ganze Zahlen/);
  assert.throws(() => normalizeSmVisitAnswer(snapshot, { kind: "number", value: 11 }), /höchstens/);
});

test("validates and orders matrix cells without accepting unknown axes", () => {
  const snapshot = { type: "matrix" as const, config: { rows: ["Sehr langer erster Artikel", "Zweiter Artikel"], columns: ["Ja", "Nein"] }, options: [] };
  const answer = normalizeSmVisitAnswer(snapshot, { kind: "matrix", cells: [
    { rowCode: "row_2", columnCode: "column_2", selected: true },
    { rowCode: "row_1", columnCode: "column_1", selected: true },
  ] });
  assert.deepEqual(answer, { kind: "matrix", cells: [
    { rowCode: "row_1", columnCode: "column_1", selected: true },
    { rowCode: "row_2", columnCode: "column_2", selected: true },
  ] });
  assert.throws(() => normalizeSmVisitAnswer(snapshot, { kind: "matrix", cells: [{ rowCode: "row_99", columnCode: "column_1", selected: true }] }), /unbekannte/);
  assert.equal(isCompleteSmVisitAnswer(snapshot, { kind: "matrix", cells: [{ rowCode: "row_1", columnCode: "column_1", selected: true }] }), false);
  assert.equal(isCompleteSmVisitAnswer(snapshot, answer), true);
});

test("distinguishes blank text and empty selections from answered values", () => {
  assert.equal(isAnsweredSmVisitPayload({ kind: "text", value: "   " }), false);
  assert.equal(isAnsweredSmVisitPayload({ kind: "multi", optionCodes: [] }), false);
  assert.equal(isAnsweredSmVisitPayload({ kind: "matrix", cells: [] }), false);
  assert.equal(isAnsweredSmVisitPayload({ kind: "photo", fileIds: [] }), false);
  assert.equal(isAnsweredSmVisitPayload({ kind: "empty" }), false);
  assert.equal(isAnsweredSmVisitPayload({ kind: "number", value: 0 }), true);
});

test("accepts explicit empty answers for every non-photo question so optional answers can be cleared", () => {
  const types = ["single", "yesno", "yesnomulti", "multiple", "likert", "text", "numeric", "slider", "matrix"] as const;
  for (const type of types) {
    assert.deepEqual(normalizeSmVisitAnswer({
      type,
      config: type === "matrix" ? { rows: ["Zeile"], columns: ["Ja"] } : {},
      options: [
        { code: "yes", label: "Ja" },
        { code: "no", label: "Nein" },
      ],
    }, { kind: "empty" }), { kind: "empty" });
  }
});

test("normalizes single, yes/no, and likert choices only against their published options", () => {
  for (const type of ["single", "yesno", "likert"] as const) {
    const snapshot = {
      type,
      config: {},
      options: [{ code: "left", label: "Links" }, { code: "right", label: "Rechts" }],
    };
    assert.deepEqual(normalizeSmVisitAnswer(snapshot, { kind: "choice", optionCode: "right" }), {
      kind: "choice",
      optionCode: "right",
    });
    assert.throws(() => normalizeSmVisitAnswer(snapshot, { kind: "multi", optionCodes: ["right"] }), /genau eine Auswahl/);
  }
});

test("preserves text exactly while treating whitespace-only text as unanswered", () => {
  const snapshot = { type: "text" as const, config: {}, options: [] };
  const value = "  Erste Zeile\nZweite Zeile mit äöü 😀  ";
  assert.deepEqual(normalizeSmVisitAnswer(snapshot, { kind: "text", value }), { kind: "text", value });
  assert.equal(isCompleteSmVisitAnswer(snapshot, { kind: "text", value: " \n\t " }), false);
  assert.equal(isCompleteSmVisitAnswer(snapshot, { kind: "text", value }), true);
});

test("enforces slider bounds and steps relative to its configured minimum", () => {
  const snapshot = { type: "slider" as const, config: { min: 10, max: 30, step: 5 }, options: [] };
  assert.deepEqual(normalizeSmVisitAnswer(snapshot, { kind: "number", value: 25 }), { kind: "number", value: 25 });
  assert.throws(() => normalizeSmVisitAnswer(snapshot, { kind: "number", value: 24 }), /Schritt 5/);
  assert.throws(() => normalizeSmVisitAnswer(snapshot, { kind: "number", value: 35 }), /höchstens 30/);
});

test("requires every configured matrix row while allowing exactly one selected cell per row from the UI payload", () => {
  const snapshot = {
    type: "matrix" as const,
    config: { rows: ["Kühler", "Regal", "Display"], columns: ["Ja", "Nein"] },
    options: [],
  };
  const partial = { kind: "matrix" as const, cells: [
    { rowCode: "row_1", columnCode: "column_1", selected: true },
    { rowCode: "row_2", columnCode: "column_2", selected: true },
  ] };
  const complete = { kind: "matrix" as const, cells: [
    ...partial.cells,
    { rowCode: "row_3", columnCode: "column_1", selected: true },
  ] };
  assert.equal(isCompleteSmVisitAnswer(snapshot, partial), false);
  assert.equal(isCompleteSmVisitAnswer(snapshot, complete), true);
  assert.throws(() => normalizeSmVisitAnswer(snapshot, { kind: "matrix", cells: [
    { rowCode: "row_1", columnCode: "column_1", selected: true },
    { rowCode: "row_1", columnCode: "column_1", selected: false },
  ] }), /mehrfach/);
});

test("photo completeness depends on committed file ids rather than local previews", () => {
  const snapshot = { type: "photo" as const, config: {}, options: [] };
  assert.equal(isCompleteSmVisitAnswer(snapshot, { kind: "photo", fileIds: [] }), false);
  assert.equal(isCompleteSmVisitAnswer(snapshot, {
    kind: "photo",
    fileIds: ["00000000-0000-4000-8000-000000000001"],
  }), true);
});

test("rule values cover each persisted answer family without exposing empty values", () => {
  const options = [{ code: "yes", label: "Ja" }, { code: "no", label: "Nein" }];
  assert.equal(smVisitAnswerToRuleValue({ kind: "empty" }, options), undefined);
  assert.equal(smVisitAnswerToRuleValue({ kind: "choice", optionCode: "yes" }, options), "Ja");
  assert.deepEqual(smVisitAnswerToRuleValue({ kind: "multi", optionCodes: ["yes", "no"] }, options), ["Ja", "Nein"]);
  assert.equal(smVisitAnswerToRuleValue({ kind: "number", value: 0 }, options), "0");
  assert.equal(smVisitAnswerToRuleValue({ kind: "photo", fileIds: [] }, options), undefined);
  assert.equal(smVisitAnswerToRuleValue({ kind: "photo", fileIds: ["00000000-0000-4000-8000-000000000001"] }, options), "uploaded");
});

test("resolves questionnaire show/hide rules from published answer values", () => {
  const questions = [
    { id: "trigger", questionId: "shared-trigger", rules: [
      { triggerQuestionId: "shared-trigger", operator: "contains", triggerValue: "MHD", action: "show", targetQuestionIds: ["detail"] },
      { triggerQuestionId: "shared-trigger", operator: "is_not_answered", action: "hide", targetQuestionIds: ["fallback"] },
    ] },
    { id: "detail", questionId: "shared-detail" },
    { id: "fallback", questionId: "shared-fallback" },
  ];

  assert.deepEqual([...computeHiddenQuestionIds(questions, new Map())].sort(), ["detail", "fallback"]);

  const branchValue = smVisitAnswerToRuleValue(
    { kind: "yesnomulti", optionCode: "yes", subOptions: ["MHD Kontrolle"] },
    [{ code: "yes", label: "Ja" }, { code: "no", label: "Nein" }],
  );
  assert.deepEqual([...computeHiddenQuestionIds(questions, new Map([["trigger", branchValue]]))], []);
});

test("a show rule for Nein keeps its target hidden for Ja", () => {
  const questions = [
    { id: "frage-1", rules: [
      { triggerQuestionId: "frage-1", operator: "equals", triggerValue: "Nein", action: "show", targetQuestionIds: ["frage-3"] },
    ] },
    { id: "frage-3" },
  ];
  const options = [{ code: "yes", label: "Ja" }, { code: "no", label: "Nein" }];
  const yes = smVisitAnswerToRuleValue({ kind: "choice", optionCode: "yes" }, options);
  const no = smVisitAnswerToRuleValue({ kind: "choice", optionCode: "no" }, options);

  assert.deepEqual([...computeHiddenQuestionIds(questions, new Map([["frage-1", yes]]))], ["frage-3"]);
  assert.deepEqual([...computeHiddenQuestionIds(questions, new Map([["frage-1", no]]))], []);
});
