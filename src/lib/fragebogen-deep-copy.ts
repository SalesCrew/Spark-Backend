export type CopyableQuestionRule = {
  id?: string | undefined;
  triggerQuestionId?: string | undefined;
  operator: string;
  triggerValue: string;
  triggerValueMax: string;
  action: "hide" | "show";
  targetQuestionIds: string[];
};

export type CopyableQuestion = {
  id?: string | undefined;
  rules?: CopyableQuestionRule[] | undefined;
};

export function createQuestionCopyIdMap(
  sourceQuestionIds: string[],
  createId: () => string,
): Map<string, string> {
  const idMap = new Map<string, string>();
  for (const sourceQuestionId of sourceQuestionIds) {
    if (!sourceQuestionId || idMap.has(sourceQuestionId)) continue;
    idMap.set(sourceQuestionId, createId());
  }
  return idMap;
}

function remapRequiredQuestionRef(
  sourceRef: string,
  idMap: Map<string, string>,
  label: string,
): string {
  const targetRef = idMap.get(sourceRef);
  if (!targetRef) {
    throw new Error(`${label} verweist auf eine Frage außerhalb des zu duplizierenden Fragebogens.`);
  }
  return targetRef;
}

export function remapQuestionForDeepCopy<T extends CopyableQuestion>(
  sourceQuestion: T,
  idMap: Map<string, string>,
): T {
  const sourceQuestionId = sourceQuestion.id;
  if (!sourceQuestionId) {
    throw new Error("Eine Quellfrage besitzt keine stabile ID.");
  }
  const targetQuestionId = idMap.get(sourceQuestionId);
  if (!targetQuestionId) {
    throw new Error("Für eine Quellfrage konnte keine neue ID erzeugt werden.");
  }

  return {
    ...sourceQuestion,
    id: targetQuestionId,
    rules: (sourceQuestion.rules ?? []).map((rule) => ({
      ...rule,
      id: undefined,
      triggerQuestionId: rule.triggerQuestionId
        ? remapRequiredQuestionRef(rule.triggerQuestionId, idMap, "Regel-Trigger")
        : "",
      targetQuestionIds: rule.targetQuestionIds.map((targetQuestionId) =>
        remapRequiredQuestionRef(targetQuestionId, idMap, "Regel-Ziel"),
      ),
    })),
  };
}
