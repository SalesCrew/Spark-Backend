import assert from "node:assert/strict";
import test from "node:test";
import { computeMissingRequiredQuestions } from "./lib/visit-session-answer-validation.js";

const question = {
  id: "visit-question-1",
  questionId: "shared-question-1",
  questionType: "photo",
  questionTextSnapshot: "Foto aufnehmen",
  questionConfigSnapshot: { tagsEnabled: false, tagIds: [] },
  questionRulesSnapshot: [],
  requiredSnapshot: true,
  appliesToMarketChainSnapshot: true,
};

const unansweredPhotoAnswer = {
  id: "answer-1",
  visitSessionQuestionId: question.id,
  answerStatus: "unanswered" as const,
  valueText: null,
  valueNumber: null,
  valueJson: { storage: [] },
  isValid: true,
};

test("inherited RED-period photos satisfy a required photo question without becoming current storage", () => {
  const result = computeMissingRequiredQuestions({
    questions: [question],
    answers: [unansweredPhotoAnswer],
    photos: [{ id: "photo-old", visitAnswerId: unansweredPhotoAnswer.id, storagePath: "old/photo.jpeg", inherited: true }],
    photoTags: [],
  });

  assert.deepEqual(result.missingRequired, []);
});

test("only newly uploaded photos must appear in the current answer storage payload", () => {
  const result = computeMissingRequiredQuestions({
    questions: [question],
    answers: [{
      ...unansweredPhotoAnswer,
      answerStatus: "answered",
      valueJson: { storage: [{ path: "current/photo.jpeg" }] },
    }],
    photos: [
      { id: "photo-old", visitAnswerId: unansweredPhotoAnswer.id, storagePath: "old/photo.jpeg", inherited: true },
      { id: "photo-new", visitAnswerId: unansweredPhotoAnswer.id, storagePath: "current/photo.jpeg", inherited: false },
    ],
    photoTags: [],
  });

  assert.deepEqual(result.missingRequired, []);
});

test("required photo tags are still validated for inherited photos", () => {
  const taggedQuestion = {
    ...question,
    questionConfigSnapshot: { tagsEnabled: true, tagIds: ["tag-1"] },
  };
  const missingTag = computeMissingRequiredQuestions({
    questions: [taggedQuestion],
    answers: [unansweredPhotoAnswer],
    photos: [{ id: "photo-old", visitAnswerId: unansweredPhotoAnswer.id, storagePath: "old/photo.jpeg", inherited: true }],
    photoTags: [],
  });
  const withTag = computeMissingRequiredQuestions({
    questions: [taggedQuestion],
    answers: [unansweredPhotoAnswer],
    photos: [{ id: "photo-old", visitAnswerId: unansweredPhotoAnswer.id, storagePath: "old/photo.jpeg", inherited: true }],
    photoTags: [{ visitAnswerPhotoId: "photo-old" }],
  });

  assert.equal(missingTag.missingRequired.length, 1);
  assert.deepEqual(withTag.missingRequired, []);
});
