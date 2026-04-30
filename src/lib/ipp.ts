import { and, asc, desc, eq, gte, inArray, isNotNull, lt } from "drizzle-orm";
import { addDays, startOfDay } from "./red-monat.js";
import { db } from "./db.js";
import {
  questionScoring,
  visitAnswerOptions,
  visitAnswers,
  visitSessionQuestions,
  visitSessionSections,
  visitSessions,
} from "./schema.js";

type SelectedAnswerValue = string | string[];

export type IppQuestionBreakdownRow = {
  questionFingerprint: string;
  questionId: string;
  questionText: string;
  questionType: string;
  selectedAnswer: SelectedAnswerValue;
  appliedIppValue: number;
  counted: boolean;
  countedReason: string;
  sourceSections: string[];
  sourceFrageboegen: string[];
  deduped: boolean;
  section: "standard" | "flex" | "billa" | "kuehler" | "mhd" | null;
  fragebogenName: string | null;
  submittedAt: string | null;
};

export type IppMarketPeriodComputation = {
  marketId: string;
  periodStart: Date;
  periodEnd: Date;
  marketIpp: number;
  sourceSubmissionCount: number;
  contributingQuestionCount: number;
  questionRows: IppQuestionBreakdownRow[];
};

type LatestAnswerRow = {
  answer: typeof visitAnswers.$inferSelect;
  question: typeof visitSessionQuestions.$inferSelect;
  section: typeof visitSessionSections.$inferSelect;
  session: typeof visitSessions.$inferSelect;
};

function toFiniteNumber(input: unknown): number | null {
  if (typeof input === "number" && Number.isFinite(input)) return input;
  if (typeof input === "string" && input.trim().length > 0) {
    const parsed = Number(input);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function normalizeUnique(values: string[]): string[] {
  return Array.from(new Set(values.map((value) => value.trim()).filter((value) => value.length > 0)));
}

function normalizeArray(raw: unknown): string[] {
  return Array.isArray(raw) ? raw.filter((entry): entry is string => typeof entry === "string") : [];
}

function parseRawAnswerKeys(valueJson: Record<string, unknown> | null): string[] {
  const raw = (valueJson as { raw?: unknown } | null | undefined)?.raw;
  if (typeof raw === "string") return [raw];
  if (Array.isArray(raw)) {
    return raw.filter((entry): entry is string => typeof entry === "string");
  }
  if (raw && typeof raw === "object") {
    const top = typeof (raw as { sel?: unknown }).sel === "string" ? [(raw as { sel: string }).sel] : [];
    const subs = normalizeArray((raw as { subs?: unknown }).subs);
    return [...top, ...subs];
  }
  return [];
}

function extractSelectedAnswer(input: {
  questionType: string;
  valueText: string | null;
  valueNumber: unknown;
  options: Array<typeof visitAnswerOptions.$inferSelect>;
  valueJson: Record<string, unknown> | null;
}): SelectedAnswerValue {
  const optionValues = input.options
    .slice()
    .sort((left, right) => left.orderIndex - right.orderIndex)
    .map((row) => row.optionValue);
  if (optionValues.length > 0) {
    return optionValues.length === 1 ? optionValues[0] ?? "" : optionValues;
  }
  if (typeof input.valueText === "string" && input.valueText.trim().length > 0) {
    return input.valueText;
  }
  const numeric = toFiniteNumber(input.valueNumber);
  if (numeric !== null) {
    return String(numeric);
  }
  const rawKeys = parseRawAnswerKeys(input.valueJson);
  if (rawKeys.length === 0) return "";
  return rawKeys.length === 1 ? rawKeys[0] ?? "" : rawKeys;
}

function computeAppliedIpp(input: {
  questionType: string;
  valueText: string | null;
  valueNumber: unknown;
  valueJson: Record<string, unknown> | null;
  options: Array<typeof visitAnswerOptions.$inferSelect>;
  scoringByKey: Record<string, number>;
}): { appliedIpp: number; countedReason: string } {
  const scoringKeys = Object.keys(input.scoringByKey);
  if (scoringKeys.length === 0) {
    return { appliedIpp: 0, countedReason: "Keine IPP-Bewertung für diese Frage." };
  }

  const questionType = String(input.questionType);
  if (["numeric", "slider", "likert"].includes(questionType) && "__value__" in input.scoringByKey) {
    const numeric = toFiniteNumber(input.valueNumber);
    if (numeric === null) {
      return { appliedIpp: 0, countedReason: "Kein gueltiger Zahlenwert fuer die IPP-Berechnung." };
    }
    const factor = input.scoringByKey["__value__"] ?? 0;
    const applied = Number((numeric * factor).toFixed(4));
    if (applied > 0) return { appliedIpp: applied, countedReason: `Numerischer Wert ${numeric} × Faktor ${factor}` };
    return { appliedIpp: 0, countedReason: "Gewaehlte Antwort ergibt IPP 0." };
  }

  const keyCandidates = normalizeUnique([
    ...(typeof input.valueText === "string" ? [input.valueText] : []),
    ...input.options.map((row) => row.optionValue),
    ...parseRawAnswerKeys(input.valueJson),
  ]);
  if (keyCandidates.length === 0) {
    return { appliedIpp: 0, countedReason: "Keine bewertbare Antwort vorhanden." };
  }

  const matchedKeys = keyCandidates.filter((key) => Object.prototype.hasOwnProperty.call(input.scoringByKey, key));
  if (matchedKeys.length === 0) {
    return { appliedIpp: 0, countedReason: "Antwort hat keinen passenden IPP-Schluessel." };
  }

  const applied = Number(
    matchedKeys.reduce((sum, key) => sum + (input.scoringByKey[key] ?? 0), 0).toFixed(4),
  );
  if (applied > 0) return { appliedIpp: applied, countedReason: "IPP-Wert aus Antwortschluessel uebernommen." };
  return { appliedIpp: 0, countedReason: "Gewaehlte Antwort ergibt IPP 0." };
}

function toPeriodBounds(periodStart: Date, periodEnd: Date): { start: Date; endExclusive: Date } {
  const start = startOfDay(periodStart);
  const endExclusive = addDays(startOfDay(periodEnd), 1);
  return { start, endExclusive };
}

function pickLatestAnswersByQuestionId(rows: LatestAnswerRow[]): LatestAnswerRow[] {
  const byQuestionId = new Map<string, LatestAnswerRow>();
  for (const row of rows) {
    if (!byQuestionId.has(row.answer.questionId)) {
      byQuestionId.set(row.answer.questionId, row);
    }
  }
  return Array.from(byQuestionId.values());
}

export async function computeMarketIppForPeriod(input: {
  marketId: string;
  periodStart: Date;
  periodEnd: Date;
}): Promise<IppMarketPeriodComputation> {
  const { start, endExclusive } = toPeriodBounds(input.periodStart, input.periodEnd);

  const sessionRows = await db
    .select({
      id: visitSessions.id,
      submittedAt: visitSessions.submittedAt,
    })
    .from(visitSessions)
    .where(
      and(
        eq(visitSessions.marketId, input.marketId),
        eq(visitSessions.isDeleted, false),
        eq(visitSessions.status, "submitted"),
        isNotNull(visitSessions.submittedAt),
        gte(visitSessions.submittedAt, start),
        lt(visitSessions.submittedAt, endExclusive),
      ),
    );

  const sourceSubmissionCount = sessionRows.length;
  const sessionIds = sessionRows.map((row) => row.id);

  if (sessionIds.length === 0) {
    return {
      marketId: input.marketId,
      periodStart: start,
      periodEnd: addDays(endExclusive, -1),
      marketIpp: 0,
      sourceSubmissionCount: 0,
      contributingQuestionCount: 0,
      questionRows: [],
    };
  }

  const answerRows = await db
    .select({
      answer: visitAnswers,
      question: visitSessionQuestions,
      section: visitSessionSections,
      session: visitSessions,
    })
    .from(visitAnswers)
    .innerJoin(visitSessionQuestions, eq(visitSessionQuestions.id, visitAnswers.visitSessionQuestionId))
    .innerJoin(visitSessionSections, eq(visitSessionSections.id, visitAnswers.visitSessionSectionId))
    .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
    .where(
      and(
        inArray(visitAnswers.visitSessionId, sessionIds),
        eq(visitAnswers.isDeleted, false),
        eq(visitSessionQuestions.isDeleted, false),
        eq(visitSessionSections.isDeleted, false),
        eq(visitSessions.isDeleted, false),
      ),
    )
    .orderBy(
      asc(visitAnswers.questionId),
      desc(visitSessions.submittedAt),
      desc(visitAnswers.changedAt),
      desc(visitAnswers.updatedAt),
      desc(visitAnswers.createdAt),
    );

  const latestAnswers = pickLatestAnswersByQuestionId(answerRows);
  if (latestAnswers.length === 0) {
    return {
      marketId: input.marketId,
      periodStart: start,
      periodEnd: addDays(endExclusive, -1),
      marketIpp: 0,
      sourceSubmissionCount,
      contributingQuestionCount: 0,
      questionRows: [],
    };
  }

  const latestAnswerIds = latestAnswers.map((row) => row.answer.id);
  const questionIds = normalizeUnique(latestAnswers.map((row) => row.answer.questionId));

  const [optionRows, scoringRows] = await Promise.all([
    latestAnswerIds.length === 0
      ? Promise.resolve([])
      : db
          .select()
          .from(visitAnswerOptions)
          .where(and(inArray(visitAnswerOptions.visitAnswerId, latestAnswerIds), eq(visitAnswerOptions.isDeleted, false)))
          .orderBy(asc(visitAnswerOptions.visitAnswerId), asc(visitAnswerOptions.orderIndex)),
    questionIds.length === 0
      ? Promise.resolve([])
      : db
          .select()
          .from(questionScoring)
          .where(
            and(
              inArray(questionScoring.questionId, questionIds),
              eq(questionScoring.isDeleted, false),
              isNotNull(questionScoring.ipp),
            ),
          ),
  ]);

  const optionsByAnswerId = new Map<string, Array<typeof visitAnswerOptions.$inferSelect>>();
  for (const row of optionRows) {
    const bucket = optionsByAnswerId.get(row.visitAnswerId) ?? [];
    bucket.push(row);
    optionsByAnswerId.set(row.visitAnswerId, bucket);
  }

  const scoringByQuestion = new Map<string, Record<string, number>>();
  for (const row of scoringRows) {
    if (row.ipp == null) continue;
    const bucket = scoringByQuestion.get(row.questionId) ?? {};
    bucket[row.scoreKey] = Number(row.ipp);
    scoringByQuestion.set(row.questionId, bucket);
  }

  const questionRows: IppQuestionBreakdownRow[] = latestAnswers.map((row) => {
    const options = optionsByAnswerId.get(row.answer.id) ?? [];
    const scoringByKey = scoringByQuestion.get(row.answer.questionId) ?? {};
    const computed = computeAppliedIpp({
      questionType: row.answer.questionType,
      valueText: row.answer.valueText,
      valueNumber: row.answer.valueNumber,
      valueJson: (row.answer.valueJson as Record<string, unknown> | null) ?? null,
      options,
      scoringByKey,
    });
    return {
      questionFingerprint: row.answer.questionId,
      questionId: row.answer.questionId,
      questionText: row.question.questionTextSnapshot,
      questionType: row.answer.questionType,
      selectedAnswer: extractSelectedAnswer({
        questionType: row.answer.questionType,
        valueText: row.answer.valueText,
        valueNumber: row.answer.valueNumber,
        options,
        valueJson: (row.answer.valueJson as Record<string, unknown> | null) ?? null,
      }),
      appliedIppValue: computed.appliedIpp,
      counted: computed.appliedIpp > 0,
      countedReason: computed.countedReason,
      sourceSections: row.section.section ? [row.section.section] : [],
      sourceFrageboegen: row.section.fragebogenNameSnapshot ? [row.section.fragebogenNameSnapshot] : [],
      deduped: false,
      section: row.section.section,
      fragebogenName: row.section.fragebogenNameSnapshot ?? null,
      submittedAt: row.session.submittedAt ? row.session.submittedAt.toISOString() : null,
    };
  });

  questionRows.sort((left, right) => {
    if (left.counted !== right.counted) return left.counted ? -1 : 1;
    return right.appliedIppValue - left.appliedIppValue;
  });

  const marketIpp = Number(
    questionRows
      .filter((row) => row.counted)
      .reduce((sum, row) => sum + row.appliedIppValue, 0)
      .toFixed(4),
  );
  const contributingQuestionCount = questionRows.filter((row) => row.counted).length;

  return {
    marketId: input.marketId,
    periodStart: start,
    periodEnd: addDays(endExclusive, -1),
    marketIpp,
    sourceSubmissionCount,
    contributingQuestionCount,
    questionRows,
  };
}
