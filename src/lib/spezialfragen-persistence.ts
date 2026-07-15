import { randomUUID } from "node:crypto";

type SpezialRuleIdentity = {
  triggerQuestionId?: string | undefined;
  targetQuestionIds: string[];
  [key: string]: unknown;
};

type SpezialQuestionIdentity = {
  id?: string | undefined;
  rules?: SpezialRuleIdentity[] | undefined;
};

const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export type CanonicalSpezialQuestion<T extends SpezialQuestionIdentity> = T & {
  id: string;
  rules: SpezialRuleIdentity[];
};

/**
 * Gives every Spezialfrage a durable UUID and rewrites rule references that
 * still point at the frontend's former `sq-*` temporary IDs.
 */
export function canonicalizeSpezialfragenIds<T extends SpezialQuestionIdentity>(
  questions: readonly T[],
  createId: () => string = randomUUID,
): CanonicalSpezialQuestion<T>[] {
  const idMap = new Map<string, string>();
  const canonicalIds = new Set<string>();

  const prepared = questions.map((question) => {
    const originalId = question.id;
    if (originalId && idMap.has(originalId)) {
      throw new Error("Spezialfrage-IDs müssen innerhalb eines Fragebogens eindeutig sein.");
    }

    const canonicalId = originalId && uuidRegex.test(originalId) ? originalId : createId();
    if (!uuidRegex.test(canonicalId) || canonicalIds.has(canonicalId)) {
      throw new Error("Spezialfrage-ID konnte nicht eindeutig erzeugt werden.");
    }

    if (originalId) idMap.set(originalId, canonicalId);
    canonicalIds.add(canonicalId);
    return { question, canonicalId };
  });

  return prepared.map(({ question, canonicalId }) => ({
    ...question,
    id: canonicalId,
    rules: (question.rules ?? []).map((rule) => ({
      ...rule,
      triggerQuestionId: rule.triggerQuestionId
        ? (idMap.get(rule.triggerQuestionId) ?? rule.triggerQuestionId)
        : rule.triggerQuestionId,
      targetQuestionIds: rule.targetQuestionIds.map((targetId) => idMap.get(targetId) ?? targetId),
    })),
  }));
}
