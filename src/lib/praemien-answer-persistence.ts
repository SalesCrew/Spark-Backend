import { buildVisitAnswerValidationResult, type VisitAnswerValidationResult } from "./visit-session-answer-validation.js";

const FALLBACK_TIMEZONE = "Europe/Vienna";

function normalizePillarSemanticName(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/\p{Diacritic}/gu, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

export function isQuarterAnswerPersistencePillarName(name: string): boolean {
  return normalizePillarSemanticName(name) === "distributionsziel";
}

export function isWaveAnswerPersistencePillarName(name: string): boolean {
  const normalized = normalizePillarSemanticName(name);
  if (normalized === "distributionsziel") return true;
  const isDisplayPillar = normalized.includes("display");
  const isSchaettenPillar = normalized.includes("schutten") || normalized.includes("schuetten");
  return isDisplayPillar && isSchaettenPillar;
}

export type LocalDateWindow = {
  startDate: string;
  endDate: string;
  timezone: string;
};

function localDateParts(at: Date, timezone: string): { year: number; month: number } {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
  }).formatToParts(at);
  const year = Number(parts.find((part) => part.type === "year")?.value);
  const month = Number(parts.find((part) => part.type === "month")?.value);
  if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
    throw new Error("Kalenderquartal konnte nicht bestimmt werden.");
  }
  return { year, month };
}

function toDateString(year: number, month: number, day: number): string {
  return `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

export function calendarQuarterDateWindow(
  at: Date,
  timezone = FALLBACK_TIMEZONE,
): LocalDateWindow {
  const safeTimezone = timezone.trim() || FALLBACK_TIMEZONE;
  const { year, month } = localDateParts(at, safeTimezone);
  const quarterStartMonth = Math.floor((month - 1) / 3) * 3 + 1;
  const quarterEndMonth = quarterStartMonth + 2;
  const quarterEndDay = new Date(Date.UTC(year, quarterEndMonth, 0)).getUTCDate();
  return {
    startDate: toDateString(year, quarterStartMonth, 1),
    endDate: toDateString(year, quarterEndMonth, quarterEndDay),
    timezone: safeTimezone,
  };
}

export function dateWindowsOverlap(
  left: Pick<LocalDateWindow, "startDate" | "endDate">,
  right: Pick<LocalDateWindow, "startDate" | "endDate">,
): boolean {
  return left.startDate <= right.endDate && left.endDate >= right.startDate;
}

export function quarterPersistentQuestionIds(
  rows: Array<{ questionId: string; pillarName: string; carryAnswersForWave: boolean }>,
): string[] {
  return Array.from(new Set(
    rows
      .filter((row) => row.carryAnswersForWave && isQuarterAnswerPersistencePillarName(row.pillarName))
      .map((row) => row.questionId.trim())
      .filter((questionId) => questionId.length > 0),
  ));
}

type PersistedReusableAnswer = {
  questionType: string;
  answerStatus: string;
  valueText: string | null;
  valueNumber: string | number | null;
  valueJson: Record<string, unknown> | null;
  isValid: boolean;
};

function rawAnswerFromPersistedAnswer(
  answer: PersistedReusableAnswer,
  currentConfig: Record<string, unknown>,
): unknown {
  const raw = answer.valueJson?.raw;
  if (raw !== undefined) {
    if (answer.questionType === "yesnomulti") {
      return typeof raw === "string" ? raw : JSON.stringify(raw);
    }
    if (answer.questionType === "matrix" && String(currentConfig.matrixSubtype ?? "toggle") !== "toggle") {
      return typeof raw === "string" ? raw : JSON.stringify(raw);
    }
    return raw;
  }
  if (["numeric", "slider", "likert"].includes(answer.questionType)) {
    return answer.valueNumber ?? answer.valueText;
  }
  return answer.valueText;
}

export function revalidateReusableAnswer(
  source: PersistedReusableAnswer,
  currentQuestion: { questionType: string; config: Record<string, unknown> },
): VisitAnswerValidationResult | null {
  if (
    source.questionType === "photo"
    || source.questionType !== currentQuestion.questionType
    || source.answerStatus !== "answered"
    || !source.isValid
  ) {
    return null;
  }
  const validation = buildVisitAnswerValidationResult(
    currentQuestion.questionType,
    currentQuestion.config,
    rawAnswerFromPersistedAnswer(source, currentQuestion.config),
  );
  return validation.answerStatus === "answered" && validation.isValid ? validation : null;
}
