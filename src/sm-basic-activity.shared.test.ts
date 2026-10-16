import assert from "node:assert/strict";
import { test } from "node:test";
import { basicActivityConfigError, smBasicActivityCode } from "./sm-basic-activity.shared.js";
import type { SmVisitAnswerPayload, SmVisitQuestionSnapshot } from "./sm-visit.shared.js";

test("all ten question types map explicit semantics independently of answer wording", () => {
  const choices = ["single", "yesno", "yesnomulti", "multiple", "likert"] as const;
  for (const type of choices) {
    const snapshot: SmVisitQuestionSnapshot = { type, options: [{ code: "arbitrary_code", label: "Anderer Wortlaut" }], config: { basicActivity: { enabled: true, answerOutcomes: { "Anderer Wortlaut": "partial" } } } };
    const answer: SmVisitAnswerPayload = type === "multiple" ? { kind: "multi", optionCodes: ["arbitrary_code"] } : type === "yesnomulti" ? { kind: "yesnomulti", optionCode: "arbitrary_code", subOptions: [] } : { kind: "choice", optionCode: "arbitrary_code" };
    assert.equal(smBasicActivityCode(snapshot, answer), "basic_activity_partial", type);
  }
  const examples: Array<[SmVisitQuestionSnapshot["type"], Record<string, unknown>, SmVisitAnswerPayload]> = [
    ["text", { rules: [{ kind: "text", value: "Kontrolle erledigt", outcome: "yes" }] }, { kind: "text", value: "Kontrolle erledigt" }],
    ["numeric", { rules: [{ kind: "range", min: 3, max: 8, outcome: "yes" }] }, { kind: "number", value: 3 }],
    ["slider", { rules: [{ kind: "range", min: 3, max: 8, outcome: "yes" }] }, { kind: "number", value: 8 }],
    ["photo", { answeredOutcome: "yes" }, { kind: "photo", fileIds: ["photo-id"] }],
    ["matrix", { answerOutcomes: { Erledigt: "yes" } }, { kind: "matrix", cells: [{ rowCode: "row_1", columnCode: "column_1", selected: true }] }],
  ];
  for (const [type, config, answer] of examples) {
    assert.equal(smBasicActivityCode({ type, options: [], config: { columns: ["Erledigt"], basicActivity: { enabled: true, ...config } } }, answer), "basic_activity_yes", type);
  }
});

test("unmapped, disabled, empty and contradictory multi-selection behavior is explicit", () => {
  const snapshot: SmVisitQuestionSnapshot = { type: "multiple", options: [{ code: "a", label: "Fertig" }, { code: "b", label: "Offen" }], config: { basicActivity: { enabled: true, answerOutcomes: { Fertig: "yes", Offen: "no" } } } };
  assert.equal(smBasicActivityCode(snapshot, { kind: "multi", optionCodes: ["a", "b"] }), "basic_activity_no");
  assert.equal(smBasicActivityCode(snapshot, { kind: "multi", optionCodes: [] }), null);
  assert.equal(smBasicActivityCode(snapshot, { kind: "empty" }), null);
  assert.equal(smBasicActivityCode({ ...snapshot, options: [{ code: "unmapped", label: "toString" }] }, { kind: "multi", optionCodes: ["unmapped"] }), null, "only explicitly configured own keys qualify");
  assert.equal(smBasicActivityCode({ ...snapshot, config: {} }, { kind: "multi", optionCodes: ["a"] }), null);
  assert.equal(smBasicActivityCode({ type: "photo", options: [], config: { basicActivity: { enabled: true, answeredOutcome: "yes" } } }, { kind: "photo", fileIds: [] }), null);
  assert.equal(smBasicActivityCode({ type: "text", options: [], config: { basicActivity: { enabled: true, answeredOutcome: "yes" } } }, { kind: "text", value: "  " }), null);
});

test("validation rejects ambiguous ranges, missing bindings and invalid type rules", () => {
  const valid = (basicActivity: unknown, type = "numeric", options: string[] = []) => basicActivityConfigError(type, { basicActivity }, options);
  assert.equal(valid({ enabled: true, rules: [{ kind: "range", min: 0, max: 5, outcome: "partial" }], answeredOutcome: "no" }), null);
  assert.ok(valid({ enabled: true, rules: [{ kind: "range", min: 5, max: 0, outcome: "partial" }] }));
  assert.ok(valid({ enabled: true, rules: [{ kind: "range", min: 0, max: 5, outcome: "yes" }, { kind: "range", min: 5, max: 8, outcome: "no" }] }));
  assert.ok(valid({ enabled: true, rules: [{ kind: "text", value: "done", outcome: "yes" }] }));
  assert.ok(valid({ enabled: true, answerOutcomes: { Gone: "yes" } }, "single", ["Other"]));
  assert.ok(valid({ enabled: true, answerOutcomes: { A: "yes" } }, "single", ["A", "A"]));
  assert.equal(valid({ enabled: false }), null);
  assert.equal(basicActivityConfigError("numeric", {}, []), null);
});
