// Pure availability rules. Keep this file identical in the frontend and backend.
// No database, environment, authentication or network imports.
export const availabilityKinds = ["Cooler", "SingleServe", "MultiServe", "Promos", "Warehouse"] as const;
export type AvailabilityKind = typeof availabilityKinds[number];
export type AvailabilityCategory = "top" | "mediocre" | "bad";
export type AvailabilityAnswer = {
  id?: string | null;
  answerStatus: string;
  isValid: boolean;
  valueText?: string | null;
  valueNumber?: string | number | null;
  valueJson?: Record<string, unknown> | null;
  options?: { optionRole: string; optionValue: string; orderIndex?: number }[];
};
export type AvailabilityObservation = {
  intervalId: string;
  sessionId: string;
  visitQuestionId: string;
  questionId: string;
  sectionId: string;
  campaignId: string | null;
  questionText: string;
  moduleName: string;
  marketId: string;
  chain: string;
  region: string;
  gmId: string | null;
  startedAt: string | null;
  submittedAt: string;
  changedAt: string | null;
  version: number;
  availabilityType: string | null;
  appliesToChain: boolean;
  visible: boolean;
  answer: AvailabilityAnswer | null;
};
export type AvailabilityAudit = AvailabilityObservation & {
  visitDate: string;
  dateBasis: "visit_start" | "submission_fallback";
  category: AvailabilityCategory | null;
  included: boolean;
  exclusion: string | null;
};
export type AvailabilityCounts = {
  top: number; mediocre: number; bad: number; total: number; average: number | null;
};
export function availabilityCategory(value: unknown): AvailabilityCategory | null {
  if (typeof value !== "string" && typeof value !== "number") return null;
  const text = String(value).normalize("NFD").replace(/[\u0300-\u036f]/g, "").trim().toLowerCase();
  const key = text.replace(/[\s_-]/g, "");
  if (["top", "voll", "sehrvoll", "1"].includes(key)) return "top";
  if (["mediocre", "mittel", "halbvoll", "mittelmassig", "3"].includes(key)) return "mediocre";
  if (["bad", "leer", "nichtvoll", "schlecht", "oos", "5"].includes(key)) return "bad";
  // Historical rating labels start with their rating code. Do not search arbitrary
  // combined answers/sub-options for words such as "Top".
  const code = text.match(/^\(([135])\)\s*=/)?.[1];
  return code === "1" ? "top" : code === "3" ? "mediocre" : code === "5" ? "bad" : null;
}
function selectedValue(raw: unknown): unknown[] {
  if (raw && typeof raw === "object" && !Array.isArray(raw)) return [(raw as { sel?: unknown }).sel];
  if (Array.isArray(raw)) return raw;
  if (typeof raw === "string" && raw.trim().startsWith("{")) {
    try { return selectedValue(JSON.parse(raw)); } catch { return [raw]; }
  }
  return [raw];
}
export function availabilityAnswerCategory(answer: AvailabilityAnswer | null): {
  category: AvailabilityCategory | null; exclusion: string | null;
} {
  if (!answer) return { category: null, exclusion: "unanswered" };
  if (!answer.isValid || answer.answerStatus === "invalid") return { category: null, exclusion: "invalid_answer" };
  if (answer.answerStatus !== "answered") return { category: null, exclusion: answer.answerStatus || "unanswered" };
  const values = [
    ...selectedValue(answer.valueText),
    ...selectedValue(answer.valueNumber),
    ...selectedValue(answer.valueJson?.raw),
    ...(answer.options ?? []).filter(o => o.optionRole === "top").map(o => o.optionValue),
  ];
  const categories = new Set(values.map(availabilityCategory).filter((c): c is AvailabilityCategory => c !== null));
  if (categories.size > 1) return { category: null, exclusion: "conflicting_answer" };
  const category = categories.values().next().value ?? null;
  return { category, exclusion: category ? null : "unrecognized_answer" };
}
export function availabilityVisitDate(startedAt: string | null, submittedAt: string): string {
  return new Intl.DateTimeFormat("sv-SE", { timeZone: "Europe/Vienna" }).format(new Date(startedAt ?? submittedAt));
}
export function availabilityChainGroup(chain: string): "rewe" | "spar" | "other" {
  const key = chain.replace(/\s+/g, "").toUpperCase();
  if (["BILLA", "BILLA+", "BILLAPLUS", "BILLACORSO"].includes(key)) return "rewe";
  if (["SPAR", "SPARMARKT", "ISP", "INTERSPAR", "ESP", "EUROSPAR"].includes(key)) return "spar";
  return "other";
}
export function availabilityCounts(categories: Iterable<AvailabilityCategory>): AvailabilityCounts {
  const counts: AvailabilityCounts = { top: 0, mediocre: 0, bad: 0, total: 0, average: null };
  for (const category of categories) counts[category]++;
  counts.total = counts.top + counts.mediocre + counts.bad;
  counts.average = counts.total ? Math.round((100 * counts.top + 50 * counts.mediocre) / counts.total * 10000) / 10000 : null;
  return counts;
}
export function availabilityPercentages(counts: AvailabilityCounts) {
  return {
    top: counts.total ? 100 * counts.top / counts.total : null,
    mediocre: counts.total ? 100 * counts.mediocre / counts.total : null,
    bad: counts.total ? 100 * counts.bad / counts.total : null,
  };
}
export function auditAvailability(observations: AvailabilityObservation[]): AvailabilityAudit[] {
  const selected = new Map<string, AvailabilityObservation>();
  const key = (o: AvailabilityObservation) => JSON.stringify([o.intervalId, o.sessionId, o.questionId]);
  const order = (o: AvailabilityObservation) => [o.answer ? 1 : 0, Date.parse(o.changedAt ?? o.submittedAt), o.version, o.answer?.id ?? "", o.visitQuestionId] as const;
  const newer = (a: AvailabilityObservation, b: AvailabilityObservation) => {
    const left = order(a), right = order(b);
    for (let i = 0; i < left.length; i++) if (left[i] !== right[i]) return left[i]! > right[i]!;
    return false;
  };
  for (const o of observations) {
    if (!o.appliesToChain || !o.visible) continue;
    const old = selected.get(key(o));
    if (!old || newer(o, old)) selected.set(key(o), o);
  }
  return observations.map(o => {
    const answer = availabilityAnswerCategory(o.answer);
    const exclusion = !o.appliesToChain ? "hidden_by_chain"
      : !o.visible ? "hidden_by_rule"
      : !availabilityKinds.includes(o.availabilityType as AvailabilityKind) ? "unknown_availability_type"
      : selected.get(key(o)) !== o ? "duplicate_visit_question"
      : answer.exclusion;
    return { ...o, visitDate: availabilityVisitDate(o.startedAt, o.submittedAt),
      dateBasis: o.startedAt ? "visit_start" : "submission_fallback", category: answer.category,
      included: !exclusion, exclusion };
  });
}
export function summarizeAvailability(audit: AvailabilityAudit[], intervalId: string) {
  const rows = audit.filter(o => o.intervalId === intervalId);
  const availability = Object.fromEntries(availabilityKinds.map(type => [
    type, availabilityCounts(rows.filter(o => o.included && o.availabilityType === type).map(o => o.category!)),
  ])) as Record<AvailabilityKind, AvailabilityCounts>;
  const expected = rows.filter(o => !["hidden_by_chain", "hidden_by_rule", "unknown_availability_type", "duplicate_visit_question"].includes(o.exclusion ?? "")).length;
  return { availability, availabilityExpected: expected, availabilityAnswered: rows.filter(o => o.included).length };
}
