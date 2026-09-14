import assert from "node:assert/strict";
import test from "node:test";
import { smCommentMissing, smCommentTriggerKey } from "./sm-comment.shared.js";
import { normalizeSmVisitAnswer, isCompleteSmVisitAnswer, smVisitAnswerSchema, smVisitAnswerToRuleValue, type SmVisitQuestionSnapshot } from "./sm-visit.shared.js";

const question: SmVisitQuestionSnapshot = {
  type: "single", config: { commentTrigger: { mode: "options", optionCodes: ["option_4"] } },
  options: ["Ja", "Teilweise", "Später", "Nein"].map((label, i) => ({ label, code: `option_${i + 1}` })),
};
test("only the exact configured option triggers; comment is required but draft is saveable", () => {
  for (const optionCode of ["option_1", "option_2", "option_3"]) {
    assert.equal(smCommentMissing(question, { kind: "choice", optionCode }), false);
    assert.equal(isCompleteSmVisitAnswer(question, { kind: "choice", optionCode }), true);
  }
  const draft = normalizeSmVisitAnswer(question, { kind: "choice", optionCode: "option_4" });
  assert.equal(smCommentMissing(question, draft), true);
  assert.equal(isCompleteSmVisitAnswer(question, draft), false);
  assert.deepEqual(normalizeSmVisitAnswer(question, { ...draft, comment: "  Keine Ware im Lager.  " }), { ...draft, comment: "Keine Ware im Lager." });
  assert.equal(isCompleteSmVisitAnswer(question, { kind: "choice", optionCode: "option_4", comment: "Keine Ware." }), true);
});
test("blank comments, overlong content and unrelated payload fields do not bypass validation", () => {
  assert.equal(isCompleteSmVisitAnswer(question, { kind: "choice", optionCode: "option_4", comment: " \n " }), false);
  assert.equal(smVisitAnswerSchema.safeParse({ kind: "choice", optionCode: "option_4", comment: "x".repeat(2001) }).success, false);
  assert.equal(smVisitAnswerSchema.safeParse({ kind: "empty", comment: "x" }).success, false);
  assert.equal(smVisitAnswerSchema.safeParse({ kind: "choice", optionCode: "option_4", unexpected: true }).success, false);
});
test("non-trigger answers discard stale comments; empty optional questions have no comment requirement", () => {
  assert.deepEqual(normalizeSmVisitAnswer(question, { kind: "choice", optionCode: "option_1", comment: "Old" }), { kind: "choice", optionCode: "option_1" });
  assert.equal(smCommentMissing(question, { kind: "empty" }), false);
  assert.equal(smCommentMissing(question, null), false);
  assert.equal(smCommentMissing({ ...question, config: {} }, { kind: "choice", optionCode: "option_4" }), false);
});
test("multi-choice, duplicate labels and changed trigger selections stay exact", () => {
  const q = { ...question, type: "multiple" as const, options: question.options.map((o) => ({ ...o, label: "Nein" })) };
  assert.equal(smCommentMissing(q, { kind: "multi", optionCodes: ["option_1"] }), false);
  const saved = normalizeSmVisitAnswer(q, { kind: "multi", optionCodes: ["option_4", "option_1", "option_4"], comment: "Grund" });
  assert.deepEqual(saved, { kind: "multi", optionCodes: ["option_1", "option_4"], comment: "Grund" });
  assert.equal(smCommentTriggerKey(q, { kind: "multi", optionCodes: ["option_4"] }), smCommentTriggerKey(q, saved));
});
test("all answer types preserve comments and existing answer validation", () => {
  const cases: Array<[SmVisitQuestionSnapshot, unknown]> = [
    [{ ...question, type: "yesno" }, { kind: "choice", optionCode: "option_4" }],
    [{ ...question, type: "likert" }, { kind: "choice", optionCode: "option_4" }],
    [{ ...question, type: "yesnomulti" }, { kind: "yesnomulti", optionCode: "option_4", subOptions: [] }],
    [{ ...question, type: "text", config: { commentTrigger: { mode: "answered" } } }, { kind: "text", value: "Antwort" }],
    [{ ...question, type: "numeric", config: { commentTrigger: { mode: "answered" } } }, { kind: "number", value: 0 }],
    [{ ...question, type: "slider", config: { commentTrigger: { mode: "answered" } } }, { kind: "number", value: 2 }],
    [{ ...question, type: "matrix", config: { rows: ["A", "B"], columns: ["Ja", "Nein"], commentTrigger: { mode: "options", optionCodes: ["column_2"] } } }, { kind: "matrix", cells: [{ rowCode: "row_1", columnCode: "column_2", selected: true }, { rowCode: "row_2", columnCode: "column_1", selected: true }] }],
    [{ ...question, type: "photo", config: { commentTrigger: { mode: "answered" } } }, { kind: "photo", fileIds: ["00000000-0000-4000-8000-000000000001"] }],
  ];
  for (const [snapshot, raw] of cases) {
    const before = JSON.stringify(snapshot);
    const draft = normalizeSmVisitAnswer(snapshot, raw);
    assert.equal(isCompleteSmVisitAnswer(snapshot, draft), false, snapshot.type);
    const saved = normalizeSmVisitAnswer(snapshot, { ...draft, comment: "Kommentar" });
    assert.equal(isCompleteSmVisitAnswer(snapshot, saved), true, snapshot.type);
    assert.equal(JSON.stringify(snapshot), before);
  }
});
test("comments do not alter answer values consumed by visibility/OOS logic", () => {
  const bare = { kind: "choice" as const, optionCode: "option_4" };
  assert.equal(smVisitAnswerToRuleValue(bare, question.options), smVisitAnswerToRuleValue({ ...bare, comment: "Grund" }, question.options));
  assert.equal(smCommentTriggerKey(question, { kind: "choice", optionCode: "unknown" }), "");
});
