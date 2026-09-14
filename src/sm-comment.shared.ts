// Kept identical in frontend and backend; parity is covered by regression tests.
export const SM_COMMENT_MAX_LENGTH = 2000;
export function smAnswerComment(raw: unknown): string {
  if (!raw || typeof raw !== "object" || !("comment" in raw) || typeof raw.comment !== "string") return "";
  return raw.comment.trim();
}
export type SmCommentTrigger = { mode: "answered" } | { mode: "options"; optionCodes: string[] };
type Question = { type: string; config: Record<string, unknown>; options: Array<{ code: string; label: string }> };
type Answer = { kind: string; comment?: string | undefined; optionCode?: string; optionCodes?: string[]; value?: string | number; fileIds?: string[]; cells?: Array<{ columnCode: string; selected: boolean }> };

export function getSmCommentTrigger(config: Record<string, unknown>): SmCommentTrigger | null {
  const raw = config.commentTrigger;
  if (!raw || typeof raw !== "object") return null;
  const trigger = raw as Record<string, unknown>;
  if (trigger.mode === "answered") return { mode: "answered" };
  if (trigger.mode === "options" && Array.isArray(trigger.optionCodes) && trigger.optionCodes.every((code) => typeof code === "string")) {
    return { mode: "options", optionCodes: [...new Set(trigger.optionCodes as string[])] };
  }
  return null;
}

export function smCommentTriggerKey(question: Question, answer: Answer | null | undefined): string {
  const trigger = getSmCommentTrigger(question.config);
  if (!trigger || !answer || answer.kind === "empty") return "";
  const answered = answer.kind === "text" ? typeof answer.value === "string" && Boolean(answer.value.trim())
    : answer.kind === "multi" ? Boolean(answer.optionCodes?.length)
    : answer.kind === "photo" ? Boolean(answer.fileIds?.length)
    : answer.kind === "matrix" ? Boolean(answer.cells?.some((cell) => cell.selected)) : true;
  if (!answered) return "";
  const selected = answer.kind === "choice" || answer.kind === "yesnomulti" ? [answer.optionCode ?? ""]
    : answer.kind === "multi" ? answer.optionCodes ?? []
    : answer.kind === "matrix" ? answer.cells?.filter((cell) => cell.selected).map((cell) => cell.columnCode) ?? [] : [];
  if (trigger.mode === "answered") {
    // Changing a choice must never carry the previous choice's explanation.
    return selected.length ? [...new Set(selected)].sort().join("|") : "answered";
  }
  const allowed = new Set(question.type === "matrix"
    ? (Array.isArray(question.config.columns) ? question.config.columns : []).flatMap((label, index) => typeof label === "string" && label.trim() ? [`column_${index + 1}`] : [])
    : question.options.map((option) => option.code));
  return [...new Set(selected.filter((code) => allowed.has(code) && trigger.optionCodes.includes(code)))].sort().join("|");
}

export function smCommentMissing(question: Question, answer: Answer | null | undefined): boolean {
  return Boolean(smCommentTriggerKey(question, answer)) && !answer?.comment?.trim();
}
