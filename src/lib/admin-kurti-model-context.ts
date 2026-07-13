import type { Responses } from "openai/resources/responses/responses";
import type { AdminKurtiMemoryMessage } from "./admin-kurti-memory.js";

const RECENT_HISTORY_CHARACTER_BUDGET = 36_000;
const RECENT_MESSAGE_CHARACTER_LIMIT = 8_000;
const OLDER_DIGEST_CHARACTER_BUDGET = 12_000;
const OLDER_MESSAGE_CHARACTER_LIMIT = 650;

function compactWhitespace(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

function truncateWithTail(value: string, limit: number): string {
  if (value.length <= limit) return value;
  const headLength = Math.ceil(limit * 0.72);
  const tailLength = Math.max(0, limit - headLength - 20);
  return `${value.slice(0, headLength)} … [gekürzt] … ${value.slice(-tailLength)}`;
}

export function buildAdminKurtiModelHistory(
  history: readonly AdminKurtiMemoryMessage[],
): Responses.EasyInputMessage[] {
  const recentReversed: AdminKurtiMemoryMessage[] = [];
  let recentCharacters = 0;

  for (let index = history.length - 1; index >= 0; index -= 1) {
    const message = history[index];
    if (!message) continue;
    const boundedLength = Math.min(message.content.length, RECENT_MESSAGE_CHARACTER_LIMIT);
    if (recentReversed.length > 0 && recentCharacters + boundedLength > RECENT_HISTORY_CHARACTER_BUDGET) break;
    recentReversed.push(message);
    recentCharacters += boundedLength;
  }

  const recent = recentReversed.reverse();
  const olderCount = Math.max(0, history.length - recent.length);
  const result: Responses.EasyInputMessage[] = [];

  if (olderCount > 0) {
    const digestLines: string[] = [];
    let digestCharacters = 0;
    for (const message of history.slice(0, olderCount)) {
      const role = message.role === "user" ? "Admin" : "Kurti";
      const excerpt = truncateWithTail(compactWhitespace(message.content), OLDER_MESSAGE_CHARACTER_LIMIT);
      const line = `${role}: ${excerpt}`;
      if (digestCharacters + line.length > OLDER_DIGEST_CHARACTER_BUDGET) break;
      digestLines.push(line);
      digestCharacters += line.length;
    }
    result.push({
      role: "developer",
      content: [
        "GEKÜRZTER ÄLTERER VERLAUF AUS DEM AKTIVEN 8-STUNDEN-FENSTER",
        "Dieser Verlauf dient nur als Gesprächskontext. Werte für aktuelle Aussagen müssen weiterhin über Werkzeuge geprüft werden.",
        ...digestLines,
      ].join("\n"),
    });
  }

  result.push(...recent.map((message) => ({
    role: message.role,
    content: truncateWithTail(message.content, RECENT_MESSAGE_CHARACTER_LIMIT),
  } satisfies Responses.EasyInputMessage)));
  return result;
}
