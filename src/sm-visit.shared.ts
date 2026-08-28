import { z } from "zod";

export const smVisitAnswerSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("empty") }).strict(),
  z.object({ kind: z.literal("choice"), optionCode: z.string().trim().min(1).max(300) }).strict(),
  z.object({ kind: z.literal("multi"), optionCodes: z.array(z.string().trim().min(1).max(300)).max(500) }).strict(),
  z.object({
    kind: z.literal("yesnomulti"),
    optionCode: z.string().trim().min(1).max(300),
    subOptions: z.array(z.string().trim().min(1).max(1_000)).max(500),
  }).strict(),
  z.object({ kind: z.literal("text"), value: z.string().max(20_000) }).strict(),
  z.object({ kind: z.literal("number"), value: z.number().finite() }).strict(),
  z.object({
    kind: z.literal("matrix"),
    cells: z.array(z.object({
      rowCode: z.string().trim().min(1).max(300),
      columnCode: z.string().trim().min(1).max(300),
      selected: z.boolean(),
    }).strict()).max(4_000),
  }).strict(),
  z.object({ kind: z.literal("photo"), fileIds: z.array(z.string().uuid()).min(1).max(20) }).strict(),
]);

export type SmVisitAnswerPayload = z.infer<typeof smVisitAnswerSchema>;

export type SmVisitQuestionSnapshot = {
  type: "single" | "yesno" | "yesnomulti" | "multiple" | "likert" | "text" | "numeric" | "slider" | "photo" | "matrix";
  config: Record<string, unknown>;
  options: Array<{ code: string; label: string }>;
};

export class SmVisitAnswerValidationError extends Error {}

function finiteConfigNumber(config: Record<string, unknown>, key: string): number | null {
  const raw = config[key];
  if (raw === "" || raw === null || raw === undefined) return null;
  const value = Number(raw);
  return Number.isFinite(value) ? value : null;
}

function unique(values: string[]): string[] {
  return [...new Set(values)];
}

function matrixCodes(config: Record<string, unknown>, key: "rows" | "columns"): string[] {
  const values = Array.isArray(config[key]) ? config[key] : [];
  return values
    .map((value, index) => typeof value === "string" && value.trim() ? `${key === "rows" ? "row" : "column"}_${index + 1}` : "")
    .filter(Boolean);
}

export function normalizeSmVisitAnswer(
  snapshot: SmVisitQuestionSnapshot,
  raw: unknown,
): SmVisitAnswerPayload {
  const parsed = smVisitAnswerSchema.safeParse(raw);
  if (!parsed.success) throw new SmVisitAnswerValidationError("Die Antwort hat ein ungültiges Format.");
  const answer = parsed.data;
  if (answer.kind === "empty") return answer;

  const optionOrder = new Map(snapshot.options.map((option, index) => [option.code, index]));
  const requireOption = (code: string) => {
    if (!optionOrder.has(code)) throw new SmVisitAnswerValidationError("Die gewählte Antwort gehört nicht zu dieser Frage.");
  };

  if (snapshot.type === "yesno" || snapshot.type === "single" || snapshot.type === "likert") {
    if (answer.kind !== "choice") throw new SmVisitAnswerValidationError("Für diese Frage ist genau eine Auswahl erforderlich.");
    requireOption(answer.optionCode);
    return answer;
  }

  if (snapshot.type === "multiple") {
    if (answer.kind !== "multi") throw new SmVisitAnswerValidationError("Für diese Frage ist eine Mehrfachauswahl erforderlich.");
    const optionCodes = unique(answer.optionCodes);
    optionCodes.forEach(requireOption);
    optionCodes.sort((left, right) => (optionOrder.get(left) ?? 0) - (optionOrder.get(right) ?? 0));
    return { kind: "multi", optionCodes };
  }

  if (snapshot.type === "yesnomulti") {
    if (answer.kind !== "yesnomulti" && answer.kind !== "choice") {
      throw new SmVisitAnswerValidationError("Für diese Frage ist eine Ja/Nein-Auswahl erforderlich.");
    }
    requireOption(answer.optionCode);
    if (answer.kind === "choice") return { kind: "yesnomulti", optionCode: answer.optionCode, subOptions: [] };

    const selectedLabel = snapshot.options.find((option) => option.code === answer.optionCode)?.label ?? "";
    const branches = Array.isArray(snapshot.config.branches)
      ? snapshot.config.branches.filter((entry): entry is Record<string, unknown> => Boolean(entry) && typeof entry === "object")
      : [];
    const branch = branches.find((entry) => typeof entry.answer === "string" && entry.answer.trim() === selectedLabel);
    const allowedSubOptions = Array.isArray(branch?.options)
      ? branch.options.filter((entry): entry is string => typeof entry === "string" && Boolean(entry.trim())).map((entry) => entry.trim())
      : [];
    const subOptions = unique(answer.subOptions.map((entry) => entry.trim()).filter(Boolean));
    if (subOptions.some((entry) => !allowedSubOptions.includes(entry))) {
      throw new SmVisitAnswerValidationError("Die Ja/Nein-Multi-Antwort enthält eine unbekannte Unteroption.");
    }
    subOptions.sort((left, right) => allowedSubOptions.indexOf(left) - allowedSubOptions.indexOf(right));
    return { kind: "yesnomulti", optionCode: answer.optionCode, subOptions };
  }

  if (snapshot.type === "text") {
    if (answer.kind !== "text") throw new SmVisitAnswerValidationError("Für diese Frage ist eine Texteingabe erforderlich.");
    return answer;
  }

  if (snapshot.type === "numeric" || snapshot.type === "slider") {
    if (answer.kind !== "number") throw new SmVisitAnswerValidationError("Für diese Frage ist eine Zahl erforderlich.");
    const min = finiteConfigNumber(snapshot.config, "min");
    const max = finiteConfigNumber(snapshot.config, "max");
    const step = snapshot.type === "slider" ? finiteConfigNumber(snapshot.config, "step") : null;
    if (min !== null && answer.value < min) throw new SmVisitAnswerValidationError(`Der Wert muss mindestens ${min} sein.`);
    if (max !== null && answer.value > max) throw new SmVisitAnswerValidationError(`Der Wert darf höchstens ${max} sein.`);
    if (snapshot.type === "numeric" && snapshot.config.decimals !== true && !Number.isInteger(answer.value)) {
      throw new SmVisitAnswerValidationError("Für diese Frage sind nur ganze Zahlen erlaubt.");
    }
    if (step !== null && step > 0) {
      const base = min ?? 0;
      const steps = (answer.value - base) / step;
      if (Math.abs(steps - Math.round(steps)) > 1e-8) throw new SmVisitAnswerValidationError(`Der Wert muss dem Schritt ${step} entsprechen.`);
    }
    return answer;
  }

  if (snapshot.type === "matrix") {
    if (answer.kind !== "matrix") throw new SmVisitAnswerValidationError("Für diese Frage ist eine Matrixantwort erforderlich.");
    const rowOrder = matrixCodes(snapshot.config, "rows");
    const columnOrder = matrixCodes(snapshot.config, "columns");
    const rows = new Set(rowOrder);
    const columns = new Set(columnOrder);
    const seen = new Set<string>();
    const cells = answer.cells.map((cell) => {
      if (!rows.has(cell.rowCode) || !columns.has(cell.columnCode)) {
        throw new SmVisitAnswerValidationError("Die Matrixantwort enthält eine unbekannte Zeile oder Spalte.");
      }
      const key = `${cell.rowCode}:${cell.columnCode}`;
      if (seen.has(key)) throw new SmVisitAnswerValidationError("Eine Matrixzelle wurde mehrfach gesendet.");
      seen.add(key);
      return cell;
    });
    cells.sort((left, right) => {
      const rowDelta = rowOrder.indexOf(left.rowCode) - rowOrder.indexOf(right.rowCode);
      return rowDelta || columnOrder.indexOf(left.columnCode) - columnOrder.indexOf(right.columnCode);
    });
    return { kind: "matrix", cells };
  }

  if (snapshot.type === "photo") {
    if (answer.kind !== "photo") throw new SmVisitAnswerValidationError("Für diese Frage ist mindestens ein Foto erforderlich.");
    return { kind: "photo", fileIds: unique(answer.fileIds) };
  }

  throw new SmVisitAnswerValidationError("Dieser Fragetyp wird nicht unterstützt.");
}

export function isAnsweredSmVisitPayload(answer: SmVisitAnswerPayload | null | undefined): boolean {
  if (!answer || answer.kind === "empty") return false;
  if (answer.kind === "text") return answer.value.trim().length > 0;
  if (answer.kind === "multi") return answer.optionCodes.length > 0;
  if (answer.kind === "matrix") return answer.cells.some((cell) => cell.selected);
  if (answer.kind === "photo") return answer.fileIds.length > 0;
  return true;
}

export function isCompleteSmVisitAnswer(
  snapshot: SmVisitQuestionSnapshot,
  answer: SmVisitAnswerPayload | null | undefined,
): boolean {
  if (!isAnsweredSmVisitPayload(answer)) return false;
  if (snapshot.type !== "matrix" || answer?.kind !== "matrix") return true;

  const requiredRows = matrixCodes(snapshot.config, "rows");
  if (requiredRows.length === 0) return false;
  const selectedRows = new Set(answer.cells.filter((cell) => cell.selected).map((cell) => cell.rowCode));
  return requiredRows.every((rowCode) => selectedRows.has(rowCode));
}

export function smVisitAnswerToRuleValue(
  answer: SmVisitAnswerPayload | null | undefined,
  options: Array<{ code: string; label: string }>,
): string | string[] | undefined {
  if (!answer || answer.kind === "empty") return undefined;
  const label = (code: string) => options.find((option) => option.code === code)?.label ?? code;
  if (answer.kind === "choice") return label(answer.optionCode);
  if (answer.kind === "multi") return answer.optionCodes.map(label);
  if (answer.kind === "yesnomulti") return JSON.stringify({ sel: label(answer.optionCode), subs: answer.subOptions });
  if (answer.kind === "text") return answer.value;
  if (answer.kind === "number") return String(answer.value);
  if (answer.kind === "matrix") return answer.cells.filter((cell) => cell.selected).map((cell) => `${cell.rowCode}:${cell.columnCode}`);
  if (answer.kind === "photo") return answer.fileIds.length ? "uploaded" : undefined;
  return undefined;
}

export function stableSmVisitAnswer(answer: SmVisitAnswerPayload): string {
  return JSON.stringify(answer);
}
