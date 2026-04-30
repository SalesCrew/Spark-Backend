export type ConditionalRule = {
  triggerQuestionId: string;
  operator: string;
  triggerValue: string;
  triggerValueMax: string;
  action: "hide" | "show";
  targetQuestionIds: string[];
};

type VisibilityQuestion = {
  id: string;
  questionId?: string;
  rules?: Array<Record<string, unknown>>;
};

type AnswerValue = string | string[] | undefined;

const OPERATOR_ALIASES: Record<string, string> = {
  equals: "equals",
  not_equals: "not_equals",
  includes: "includes",
  not_includes: "not_includes",
  gt: "gt",
  gte: "gte",
  lt: "lt",
  lte: "lte",
  greater_than: "gt",
  greater_than_or_equal: "gte",
  greater_or_equal: "gte",
  less_than: "lt",
  less_than_or_equal: "lte",
  less_or_equal: "lte",
  between: "between",
};

function normalizeOperator(operator: unknown): string {
  const key = typeof operator === "string" ? operator.trim().toLowerCase() : "";
  return OPERATOR_ALIASES[key] ?? "equals";
}

function normalizeConditionalRules(rules: Array<Record<string, unknown>> | undefined): ConditionalRule[] {
  if (!Array.isArray(rules)) return [];
  return rules
    .map((rule): ConditionalRule | null => {
      const triggerQuestionId = typeof rule.triggerQuestionId === "string" ? rule.triggerQuestionId.trim() : "";
      const targetQuestionIds = Array.isArray(rule.targetQuestionIds)
        ? rule.targetQuestionIds
            .filter((entry): entry is string => typeof entry === "string")
            .map((entry) => entry.trim())
            .filter((entry) => entry.length > 0)
        : [];
      if (!triggerQuestionId || targetQuestionIds.length === 0) return null;
      return {
        triggerQuestionId,
        operator: normalizeOperator(rule.operator),
        triggerValue: typeof rule.triggerValue === "string" ? rule.triggerValue : "",
        triggerValueMax: typeof rule.triggerValueMax === "string" ? rule.triggerValueMax : "",
        action: rule.action === "show" ? "show" : "hide",
        targetQuestionIds,
      };
    })
    .filter((entry): entry is ConditionalRule => entry !== null);
}

function normalizeRuleAnswerValues(answer: AnswerValue): string[] {
  if (Array.isArray(answer)) {
    return answer.map((entry) => String(entry).trim().toLowerCase()).filter((entry) => entry.length > 0);
  }
  if (answer == null) return [];
  const value = String(answer).trim();
  if (value.length === 0) return [];
  if (!value.startsWith("{")) return [value.toLowerCase()];
  try {
    const parsed = JSON.parse(value) as { sel?: unknown; subs?: unknown } | Record<string, unknown>;
    if (parsed && typeof parsed === "object" && "sel" in parsed) {
      const sel = typeof (parsed as { sel?: unknown }).sel === "string" ? (parsed as { sel: string }).sel.trim() : "";
      const subs = Array.isArray((parsed as { subs?: unknown }).subs)
        ? (parsed as { subs: unknown[] }).subs
            .filter((entry): entry is string => typeof entry === "string")
            .map((entry) => entry.trim())
            .filter((entry) => entry.length > 0)
        : [];
      return [sel, ...subs].map((entry) => entry.toLowerCase()).filter((entry) => entry.length > 0);
    }
    return Object.values(parsed)
      .filter((entry): entry is string => typeof entry === "string")
      .map((entry) => entry.trim().toLowerCase())
      .filter((entry) => entry.length > 0);
  } catch {
    return [value.toLowerCase()];
  }
}

function evaluateConditionalRule(rule: ConditionalRule, triggerAnswer: AnswerValue): boolean {
  const values = normalizeRuleAnswerValues(triggerAnswer);
  if (values.length === 0) return false;
  const expected = String(rule.triggerValue ?? "").trim().toLowerCase();
  const expectedMax = String(rule.triggerValueMax ?? "").trim().toLowerCase();

  if (rule.operator === "equals") return values.includes(expected);
  if (rule.operator === "not_equals") return !values.includes(expected);
  if (rule.operator === "includes") return values.some((value) => value.includes(expected));
  if (rule.operator === "not_includes") return values.every((value) => !value.includes(expected));

  const valueNum = Number(values[0]);
  const expectedNum = Number(expected);
  const expectedMaxNum = Number(expectedMax);
  if (Number.isNaN(valueNum) || Number.isNaN(expectedNum)) return false;
  if (rule.operator === "gt") return valueNum > expectedNum;
  if (rule.operator === "gte") return valueNum >= expectedNum;
  if (rule.operator === "lt") return valueNum < expectedNum;
  if (rule.operator === "lte") return valueNum <= expectedNum;
  if (rule.operator === "between" && !Number.isNaN(expectedMaxNum)) {
    return valueNum >= expectedNum && valueNum <= expectedMaxNum;
  }
  return false;
}

function pushUnique(target: string[], values: string[]) {
  for (const value of values) {
    if (!target.includes(value)) target.push(value);
  }
}

export function computeHiddenQuestionIds(
  questions: VisibilityQuestion[],
  answerByQuestionId: Map<string, AnswerValue>,
): Set<string> {
  const orderedIds = questions.map((question) => question.id);
  const visitIdSet = new Set(orderedIds);
  const sharedToVisitIds = new Map<string, string[]>();
  for (const question of questions) {
    const sharedId = typeof question.questionId === "string" ? question.questionId.trim() : "";
    if (!sharedId) continue;
    const bucket = sharedToVisitIds.get(sharedId) ?? [];
    if (!bucket.includes(question.id)) bucket.push(question.id);
    sharedToVisitIds.set(sharedId, bucket);
  }

  const resolveToVisitIds = (refId: string): string[] => {
    const trimmed = refId.trim();
    if (!trimmed) return [];
    const resolved: string[] = [];
    if (visitIdSet.has(trimmed)) resolved.push(trimmed);
    const viaShared = sharedToVisitIds.get(trimmed) ?? [];
    pushUnique(resolved, viaShared);
    return resolved;
  };

  const visibility = new Map<string, boolean>();
  for (const id of orderedIds) visibility.set(id, true);

  const showTargetIds = new Set<string>();
  for (const question of questions) {
    const rules = normalizeConditionalRules(question.rules);
    for (const rule of rules) {
      if (rule.action !== "show") continue;
      for (const targetRef of rule.targetQuestionIds) {
        for (const resolved of resolveToVisitIds(targetRef)) {
          showTargetIds.add(resolved);
        }
      }
    }
  }
  for (const targetId of showTargetIds) {
    if (visibility.has(targetId)) visibility.set(targetId, false);
  }

  for (const question of questions) {
    const rules = normalizeConditionalRules(question.rules);
    if (rules.length === 0) continue;
    for (const rule of rules) {
      const triggerIds = resolveToVisitIds(rule.triggerQuestionId);
      if (triggerIds.length === 0) continue;
      const matched = triggerIds.some((triggerId) =>
        evaluateConditionalRule(rule, answerByQuestionId.get(triggerId)),
      );
      if (!matched) continue;
      for (const targetRef of rule.targetQuestionIds) {
        for (const targetId of resolveToVisitIds(targetRef)) {
          if (!visibility.has(targetId)) continue;
          visibility.set(targetId, rule.action === "show");
        }
      }
    }
  }

  return new Set(
    Array.from(visibility.entries())
      .filter(([, isVisible]) => isVisible === false)
      .map(([questionId]) => questionId),
  );
}
