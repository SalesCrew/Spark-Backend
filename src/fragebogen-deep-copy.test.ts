import assert from "node:assert/strict";
import test from "node:test";
import {
  createQuestionCopyIdMap,
  remapQuestionForDeepCopy,
} from "./lib/fragebogen-deep-copy.js";

test("creates one independent target id per unique source question", () => {
  const generated = ["target-a", "target-b"];
  const idMap = createQuestionCopyIdMap(
    ["source-a", "source-a", "source-b"],
    () => generated.shift()!,
  );

  assert.deepEqual(Array.from(idMap.entries()), [
    ["source-a", "target-a"],
    ["source-b", "target-b"],
  ]);
});

test("remaps question ids and every conditional-rule reference", () => {
  const question = remapQuestionForDeepCopy(
    {
      id: "source-a",
      text: "Frage A",
      rules: [{
        id: "old-rule",
        triggerQuestionId: "source-b",
        operator: "equals",
        triggerValue: "Ja",
        triggerValueMax: "",
        action: "show" as const,
        targetQuestionIds: ["source-c"],
      }],
    },
    new Map([
      ["source-a", "target-a"],
      ["source-b", "target-b"],
      ["source-c", "target-c"],
    ]),
  );

  assert.equal(question.id, "target-a");
  assert.equal(question.rules?.[0]?.id, undefined);
  assert.equal(question.rules?.[0]?.triggerQuestionId, "target-b");
  assert.deepEqual(question.rules?.[0]?.targetQuestionIds, ["target-c"]);
});

test("rejects rule references outside the copied questionnaire", () => {
  assert.throws(
    () => remapQuestionForDeepCopy(
      {
        id: "source-a",
        rules: [{
          triggerQuestionId: "",
          operator: "equals",
          triggerValue: "Ja",
          triggerValueMax: "",
          action: "hide" as const,
          targetQuestionIds: ["outside-question"],
        }],
      },
      new Map([["source-a", "target-a"]]),
    ),
    /außerhalb des zu duplizierenden Fragebogens/,
  );
});
