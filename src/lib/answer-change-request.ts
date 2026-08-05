const COMMENT_CHANGE_KIND = "comment";
const MAX_COMMENT_LENGTH = 4000;

export type RequestedCommentChange =
  | { kind: "not_comment" }
  | { kind: "invalid"; error: string }
  | { kind: "comment"; comment: string };

export function readRequestedCommentChange(payload: Record<string, unknown>): RequestedCommentChange {
  if (payload.changeKind !== COMMENT_CHANGE_KIND) {
    return payload.changeKind === undefined
      ? { kind: "not_comment" }
      : { kind: "invalid", error: "Unbekannte Art der Änderungsanfrage." };
  }

  if (typeof payload.requestedComment !== "string") {
    return { kind: "invalid", error: "Der gewünschte Kommentar fehlt." };
  }
  if (payload.requestedComment.length > MAX_COMMENT_LENGTH) {
    return { kind: "invalid", error: `Der Kommentar darf maximal ${MAX_COMMENT_LENGTH} Zeichen lang sein.` };
  }

  return { kind: "comment", comment: payload.requestedComment.trim() };
}

export function requestedCommentSummary(comment: string): string {
  if (!comment) return "Kommentar entfernen";
  const compact = comment.replace(/\s+/g, " ").trim();
  return `Kommentar: ${compact.length > 620 ? `${compact.slice(0, 617)}...` : compact}`;
}
