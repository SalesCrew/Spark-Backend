import { z } from "zod";
import type { SmVisitAnswerPayload, SmVisitQuestionSnapshot } from "./sm-visit.shared.js";

export const basicActivityOutcomeSchema = z.enum(["yes", "partial", "no"]);
export type SmBasicActivityOutcome = z.infer<typeof basicActivityOutcomeSchema>;
export const BASIC_ACTIVITY_CREDIT = { yes: 1, partial: 0.5, no: 0 } as const;
const configSchema = z.object({
  enabled: z.boolean(),
  answerOutcomes: z.record(z.string().max(1_000), basicActivityOutcomeSchema).optional(),
  answeredOutcome: basicActivityOutcomeSchema.optional(),
  rules: z.array(z.discriminatedUnion("kind", [
    z.object({ kind: z.literal("text"), value: z.string().trim().min(1).max(20_000), outcome: basicActivityOutcomeSchema }).strict(),
    z.object({ kind: z.literal("range"), min: z.number().finite(), max: z.number().finite(), outcome: basicActivityOutcomeSchema }).strict(),
  ])).max(100).optional(),
}).strict();
export type SmBasicActivityConfig = z.infer<typeof configSchema>;

/** Authoring validation only. Legacy questions without this config remain untouched. */
export function basicActivityConfigError(type: string, config: Record<string, unknown>, options: string[]): string | null {
  if (config.basicActivity === undefined) return null;
  const parsed = configSchema.safeParse(config.basicActivity);
  if (!parsed.success) return "Die Basistätigkeiten-Zuordnung ist ungültig.";
  const value = parsed.data;
  if (!value.enabled) return null;
  const choice = ["single", "multiple", "yesno", "yesnomulti", "likert", "matrix"].includes(type);
  if (choice) {
    if (options.length !== new Set(options).size) return "Antworten für Basistätigkeiten müssen eindeutig sein.";
    if (value.answeredOutcome || value.rules?.length) return "Bitte ordne Basistätigkeiten direkt den Antwortoptionen zu.";
    const mapped = Object.keys(value.answerOutcomes ?? {});
    if (!mapped.length || mapped.some(key => !options.includes(key))) return "Bitte ordne vorhandene Antworten den Basistätigkeiten zu.";
  } else {
    if (Object.keys(value.answerOutcomes ?? {}).length) return "Dieser Fragetyp benötigt eine Wert- oder Antwortzuordnung für Basistätigkeiten.";
    if (!value.answeredOutcome && !value.rules?.length) return "Bitte lege eine Basistätigkeiten-Zuordnung fest.";
    for (const [index, rule] of (value.rules ?? []).entries()) {
      if (rule.kind === "text" && type !== "text" || rule.kind === "range" && type !== "numeric" && type !== "slider") return "Die Basistätigkeiten-Regel passt nicht zum Fragetyp.";
      if (rule.kind === "range" && rule.min > rule.max) return "Das Minimum der Basistätigkeiten-Regel darf nicht größer als das Maximum sein.";
      if ((value.rules ?? []).slice(0, index).some(other => rule.kind === "text" && other.kind === "text" ? rule.value === other.value : rule.kind === "range" && other.kind === "range" && rule.min <= other.max && other.min <= rule.max)) return "Basistätigkeiten-Regeln dürfen sich nicht überschneiden.";
    }
  }
  return null;
}

/** Uses the frozen submission config, never today's module or the visible answer wording. */
export function smBasicActivityOutcome(snapshot: SmVisitQuestionSnapshot, answer: SmVisitAnswerPayload): SmBasicActivityOutcome | null {
  const parsed = configSchema.safeParse(snapshot.config.basicActivity);
  if (!parsed.success || !parsed.data.enabled || answer.kind === "empty") return null;
  const config = parsed.data;
  const mapped = new Map(Object.entries(config.answerOutcomes ?? {}));
  const codes = answer.kind === "choice" || answer.kind === "yesnomulti" ? [answer.optionCode] : answer.kind === "multi" ? answer.optionCodes : [];
  const labels = codes.map(code => snapshot.options.find(option => option.code === code)?.label);
  if (answer.kind === "matrix") {
    const columns = Array.isArray(snapshot.config.columns) ? snapshot.config.columns : [];
    for (const cell of answer.cells.filter(cell => cell.selected)) {
      const index = Number(cell.columnCode.replace(/^column_/, "")) - 1;
      labels.push(typeof columns[index] === "string" ? columns[index] as string : undefined);
    }
  }
  if (["choice", "yesnomulti", "multi", "matrix"].includes(answer.kind)) {
    const outcomes = labels.flatMap(label => label && mapped.has(label) ? [mapped.get(label)!] : []);
    // One observation per question. Among mapped selections, the lowest completion wins.
    return outcomes.sort((a, b) => BASIC_ACTIVITY_CREDIT[a] - BASIC_ACTIVITY_CREDIT[b])[0] ?? null;
  }
  if (answer.kind === "text" && !answer.value.trim() || answer.kind === "photo" && !answer.fileIds.length) return null;
  const matches = (config.rules ?? []).filter(rule => rule.kind === "text" && answer.kind === "text" ? answer.value.trim() === rule.value : rule.kind === "range" && answer.kind === "number" && answer.value >= rule.min && answer.value <= rule.max);
  return matches.sort((a, b) => BASIC_ACTIVITY_CREDIT[a.outcome] - BASIC_ACTIVITY_CREDIT[b.outcome])[0]?.outcome ?? config.answeredOutcome ?? null;
}

export function smBasicActivityCode(snapshot: SmVisitQuestionSnapshot, answer: SmVisitAnswerPayload): string | null {
  const outcome = smBasicActivityOutcome(snapshot, answer);
  return outcome ? `basic_activity_${outcome}` : null;
}

export function smBasicActivityFromCode(code: unknown): SmBasicActivityOutcome | null {
  if (typeof code !== "string" || !code.startsWith("basic_activity_")) return null;
  const result = basicActivityOutcomeSchema.safeParse(code.slice("basic_activity_".length));
  return result.success ? result.data : null;
}
