// The production visit-start query, with an injected database for isolated tests.
// This module must not import db.ts, auth, Supabase or background jobs.
import { and, asc, desc, eq, gte, inArray, isNotNull, lt, lte, ne, sql } from "drizzle-orm";
import type { PgDatabase, PgQueryResultHKT } from "drizzle-orm/pg-core";
import { managedQuarterQuestionIds, modelDatabase } from "./praemien-workspace.js";
import { calendarQuarterDateWindow, quarterPersistentQuestionIds, revalidateReusableAnswer } from "./praemien-answer-persistence.js";
import {
  praemienWavePillars, praemienWaveSources, praemienWaves,
  visitAnswers, visitSessions, visitAnswerOptions, visitAnswerMatrixCells,
  visitAnswerPhotos, visitAnswerPhotoTags, visitQuestionComments,
} from "./schema.js";

type AnswerReuseDatabase = Pick<PgDatabase<PgQueryResultHKT>, "select" | "execute">;
type RedPeriodResolver = (at: Date) => Promise<{ start: Date; end: Date }>;
const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const isUuid = (value: string) => uuidRegex.test(value);
const normalizeUnique = (values: string[]) => [...new Set(values.map((v) => v.trim()).filter(Boolean))];

export type ReusableSourceAnswer = {
  reuseScope: "red-month" | "calendar-quarter";
  answer: typeof visitAnswers.$inferSelect;
  options: Array<typeof visitAnswerOptions.$inferSelect>;
  matrixCells: Array<typeof visitAnswerMatrixCells.$inferSelect>;
  photos: Array<typeof visitAnswerPhotos.$inferSelect>;
  tagsByPhotoId: Map<string, Array<typeof visitAnswerPhotoTags.$inferSelect>>;
  commentText: string | null;
};

type AnswerReuseWindow =
  | { kind: "timestamp"; start: Date; endExclusive: Date }
  | { kind: "local-date"; startDate: string; endDate: string; timezone: string };

async function loadLatestSubmittedAnswersByQuestionId(db: AnswerReuseDatabase, input: {
  gmUserId: string;
  marketId: string;
  questionIds: string[];
  window: AnswerReuseWindow;
  reuseScope: ReusableSourceAnswer["reuseScope"];
  requireAnswered?: boolean;
}): Promise<Map<string, ReusableSourceAnswer>> {
  const questionIds = normalizeUnique(input.questionIds.filter((id) => isUuid(id)));
  if (questionIds.length === 0) return new Map();

  const submittedAtWindow = input.window.kind === "timestamp"
    ? and(
        gte(visitSessions.submittedAt, input.window.start),
        lt(visitSessions.submittedAt, input.window.endExclusive),
      )
    : and(
        sql`${visitSessions.submittedAt} >= (${input.window.startDate}::date::timestamp at time zone ${input.window.timezone})`,
        sql`${visitSessions.submittedAt} < (((${input.window.endDate}::date + 1)::timestamp) at time zone ${input.window.timezone})`,
      );

  const candidates = await db
    .select({
      answer: visitAnswers,
      submittedAt: visitSessions.submittedAt,
    })
    .from(visitAnswers)
    .innerJoin(visitSessions, eq(visitSessions.id, visitAnswers.visitSessionId))
    .where(
      and(
        inArray(visitAnswers.questionId, questionIds),
        eq(visitAnswers.isDeleted, false),
        input.requireAnswered ? eq(visitAnswers.answerStatus, "answered") : undefined,
        input.requireAnswered ? eq(visitAnswers.isValid, true) : undefined,
        eq(visitSessions.isDeleted, false),
        eq(visitSessions.status, "submitted"),
        eq(visitSessions.gmUserId, input.gmUserId),
        eq(visitSessions.marketId, input.marketId),
        isNotNull(visitSessions.submittedAt),
        submittedAtWindow,
      ),
    )
    .orderBy(
      desc(visitSessions.submittedAt),
      desc(visitAnswers.changedAt),
      desc(visitAnswers.updatedAt),
      desc(visitAnswers.createdAt),
    );

  const selectedByQuestionId = new Map<string, typeof candidates[number]>();
  for (const row of candidates) {
    if (!selectedByQuestionId.has(row.answer.questionId)) {
      selectedByQuestionId.set(row.answer.questionId, row);
    }
  }
  if (selectedByQuestionId.size === 0) return new Map();

  const selectedAnswers = Array.from(selectedByQuestionId.values()).map((row) => row.answer);
  const selectedAnswerIds = selectedAnswers.map((row) => row.id);
  const selectedVisitQuestionIds = selectedAnswers.map((row) => row.visitSessionQuestionId);

  const [options, matrixCells, photos, comments] = await Promise.all([
    selectedAnswerIds.length === 0
      ? Promise.resolve([])
      : db
          .select()
          .from(visitAnswerOptions)
          .where(and(inArray(visitAnswerOptions.visitAnswerId, selectedAnswerIds), eq(visitAnswerOptions.isDeleted, false)))
          .orderBy(asc(visitAnswerOptions.orderIndex)),
    selectedAnswerIds.length === 0
      ? Promise.resolve([])
      : db
          .select()
          .from(visitAnswerMatrixCells)
          .where(and(inArray(visitAnswerMatrixCells.visitAnswerId, selectedAnswerIds), eq(visitAnswerMatrixCells.isDeleted, false)))
          .orderBy(asc(visitAnswerMatrixCells.orderIndex)),
    selectedAnswerIds.length === 0
      ? Promise.resolve([])
      : db
          .select()
          .from(visitAnswerPhotos)
          .where(and(inArray(visitAnswerPhotos.visitAnswerId, selectedAnswerIds), eq(visitAnswerPhotos.isDeleted, false)))
          .orderBy(asc(visitAnswerPhotos.createdAt)),
    selectedVisitQuestionIds.length === 0
      ? Promise.resolve([])
      : db
          .select()
          .from(visitQuestionComments)
          .where(and(inArray(visitQuestionComments.visitSessionQuestionId, selectedVisitQuestionIds), eq(visitQuestionComments.isDeleted, false))),
  ]);

  const photoIds = photos.map((row) => row.id);
  const photoTags =
    photoIds.length === 0
      ? []
      : await db
          .select()
          .from(visitAnswerPhotoTags)
          .where(and(inArray(visitAnswerPhotoTags.visitAnswerPhotoId, photoIds), eq(visitAnswerPhotoTags.isDeleted, false)))
          .orderBy(asc(visitAnswerPhotoTags.createdAt));

  const optionsByAnswerId = new Map<string, Array<typeof visitAnswerOptions.$inferSelect>>();
  for (const row of options) {
    const bucket = optionsByAnswerId.get(row.visitAnswerId) ?? [];
    bucket.push(row);
    optionsByAnswerId.set(row.visitAnswerId, bucket);
  }
  const matrixByAnswerId = new Map<string, Array<typeof visitAnswerMatrixCells.$inferSelect>>();
  for (const row of matrixCells) {
    const bucket = matrixByAnswerId.get(row.visitAnswerId) ?? [];
    bucket.push(row);
    matrixByAnswerId.set(row.visitAnswerId, bucket);
  }
  const photosByAnswerId = new Map<string, Array<typeof visitAnswerPhotos.$inferSelect>>();
  for (const row of photos) {
    const bucket = photosByAnswerId.get(row.visitAnswerId) ?? [];
    bucket.push(row);
    photosByAnswerId.set(row.visitAnswerId, bucket);
  }
  const tagsByPhotoId = new Map<string, Array<typeof visitAnswerPhotoTags.$inferSelect>>();
  for (const row of photoTags) {
    const bucket = tagsByPhotoId.get(row.visitAnswerPhotoId) ?? [];
    bucket.push(row);
    tagsByPhotoId.set(row.visitAnswerPhotoId, bucket);
  }
  const commentByVisitQuestionId = new Map<string, string>();
  for (const row of comments) {
    if (!commentByVisitQuestionId.has(row.visitSessionQuestionId)) {
      commentByVisitQuestionId.set(row.visitSessionQuestionId, row.commentText);
    }
  }

  const output = new Map<string, ReusableSourceAnswer>();
  for (const row of selectedAnswers) {
    output.set(row.questionId, {
      reuseScope: input.reuseScope,
      answer: row,
      options: optionsByAnswerId.get(row.id) ?? [],
      matrixCells: matrixByAnswerId.get(row.id) ?? [],
      photos: photosByAnswerId.get(row.id) ?? [],
      tagsByPhotoId,
      commentText: commentByVisitQuestionId.get(row.visitSessionQuestionId) ?? null,
    });
  }
  return output;
}

async function loadLatestSubmittedAnswersByQuestionIdInCurrentRedMonth(db: AnswerReuseDatabase, resolveRedPeriod: RedPeriodResolver, input: {
  gmUserId: string;
  marketId: string;
  questionIds: string[];
  now: Date;
}): Promise<Map<string, ReusableSourceAnswer>> {
  const period = await resolveRedPeriod(input.now);
  return loadLatestSubmittedAnswersByQuestionId(db, {
    gmUserId: input.gmUserId,
    marketId: input.marketId,
    questionIds: input.questionIds,
    reuseScope: "red-month",
    window: {
      kind: "timestamp",
      start: new Date(period.start.getFullYear(), period.start.getMonth(), period.start.getDate()),
      endExclusive: new Date(period.end.getFullYear(), period.end.getMonth(), period.end.getDate() + 1),
    },
  });
}

export async function loadReusableSubmittedAnswers(db: AnswerReuseDatabase, input: {
  gmUserId: string;
  marketId: string;
  questionIds: string[];
  now: Date;
}, resolveRedPeriod: RedPeriodResolver): Promise<Map<string, ReusableSourceAnswer>> {
  const questionIds = normalizeUnique(input.questionIds.filter((id) => isUuid(id)));
  if (questionIds.length === 0) return new Map();

  const quarterWindow = calendarQuarterDateWindow(input.now);
  const overlappingWaves = await db
    .select({ id: praemienWaves.id })
    .from(praemienWaves)
    .where(
      and(
        eq(praemienWaves.isDeleted, false),
        ne(praemienWaves.status, "archived"),
        lte(praemienWaves.startDate, quarterWindow.endDate),
        gte(praemienWaves.endDate, quarterWindow.startDate),
      ),
    );
  const overlappingWaveIds = normalizeUnique(overlappingWaves.map((row) => row.id));
  if (overlappingWaveIds.length === 0) {
    return loadLatestSubmittedAnswersByQuestionIdInCurrentRedMonth(db, resolveRedPeriod, { ...input, questionIds });
  }

  const waveQuestionRows = await db
    .select({
      questionId: praemienWaveSources.questionId,
      pillarName: praemienWavePillars.name,
      carryAnswersForWave: praemienWavePillars.carryAnswersForWave,
    })
    .from(praemienWaveSources)
    .innerJoin(
      praemienWavePillars,
      and(
        eq(praemienWavePillars.id, praemienWaveSources.pillarId),
        eq(praemienWavePillars.waveId, praemienWaveSources.waveId),
      ),
    )
    .where(
      and(
        inArray(praemienWaveSources.waveId, overlappingWaveIds),
        inArray(praemienWaveSources.questionId, questionIds),
        eq(praemienWaveSources.isDeleted, false),
        eq(praemienWavePillars.isDeleted, false),
      ),
    );
  const quarterQuestionIds = normalizeUnique([
    ...quarterPersistentQuestionIds(waveQuestionRows),
    ...await managedQuarterQuestionIds(modelDatabase(db), overlappingWaveIds, questionIds),
  ]);
  if (quarterQuestionIds.length === 0) {
    return loadLatestSubmittedAnswersByQuestionIdInCurrentRedMonth(db, resolveRedPeriod, { ...input, questionIds });
  }

  const quarterQuestionIdSet = new Set(quarterQuestionIds);
  const redMonthQuestionIds = questionIds.filter((questionId) => !quarterQuestionIdSet.has(questionId));
  const [redMonthAnswers, quarterAnswers] = await Promise.all([
    loadLatestSubmittedAnswersByQuestionIdInCurrentRedMonth(db, resolveRedPeriod, {
      ...input,
      questionIds: redMonthQuestionIds,
    }),
    loadLatestSubmittedAnswersByQuestionId(db, {
      gmUserId: input.gmUserId,
      marketId: input.marketId,
      questionIds: quarterQuestionIds,
      reuseScope: "calendar-quarter",
      requireAnswered: true,
      window: {
        kind: "local-date",
        ...quarterWindow,
      },
    }),
  ]);

  return new Map([...redMonthAnswers, ...quarterAnswers]);
}

// The same insertion used by the visit route: new draft rows, never a mutation
// of the submitted source. Both reuse scopes retain their existing semantics.
export async function copyReusableAnswer(
  tx: Pick<PgDatabase<PgQueryResultHKT>, "insert">,
  input: {
    sessionId: string;
    sectionId: string;
    visitQuestionId: string;
    question: { questionId: string; type: typeof visitAnswers.$inferInsert.questionType; config: Record<string, unknown> };
    now: Date;
  },
  reusableSource: ReusableSourceAnswer,
): Promise<boolean> {
  const sourceAnswer = reusableSource.answer;
  const isQuarterReuse = reusableSource.reuseScope === "calendar-quarter";
  const quarterValidation = isQuarterReuse
    ? revalidateReusableAnswer(sourceAnswer, {
        questionType: input.question.type,
        config: input.question.config,
      })
    : null;
  if (isQuarterReuse && !quarterValidation) {
    return false;
  }
  const isPhotoAnswer = !isQuarterReuse && sourceAnswer.questionType === "photo";
  const answerOptions = quarterValidation?.options ?? reusableSource.options;
  const answerMatrixCells = quarterValidation?.matrixCells ?? reusableSource.matrixCells;
  const [insertedAnswer] = await tx
    .insert(visitAnswers)
      .values({
        visitSessionId: input.sessionId,
        visitSessionSectionId: input.sectionId,
        visitSessionQuestionId: input.visitQuestionId,
        questionId: input.question.questionId,
        questionType: isQuarterReuse ? input.question.type : sourceAnswer.questionType,
        answerStatus: isQuarterReuse
          ? quarterValidation!.answerStatus
          : (isPhotoAnswer ? "unanswered" : sourceAnswer.answerStatus),
        valueText: isQuarterReuse
          ? quarterValidation!.valueText
          : (isPhotoAnswer ? null : sourceAnswer.valueText),
        valueNumber: isQuarterReuse
          ? quarterValidation!.valueNumber
          : (isPhotoAnswer || sourceAnswer.valueNumber == null ? null : String(sourceAnswer.valueNumber)),
        valueJson: isQuarterReuse
          ? quarterValidation!.valueJson
          : (isPhotoAnswer ? { storage: [] } : sourceAnswer.valueJson),
        isValid: isQuarterReuse ? quarterValidation!.isValid : (isPhotoAnswer ? true : sourceAnswer.isValid),
        validationError: isQuarterReuse
          ? quarterValidation!.validationError
          : (isPhotoAnswer ? null : sourceAnswer.validationError),
        answeredAt: isQuarterReuse
          ? input.now
          : (isPhotoAnswer ? null : (sourceAnswer.answeredAt ? input.now : null)),
      changedAt: input.now,
      version: 1,
      isDeleted: false,
      deletedAt: null,
      createdAt: input.now,
      updatedAt: input.now,
    })
    .returning();
  if (!insertedAnswer) throw new Error("Vorbelegte Antwort konnte nicht erstellt werden.");

  if (answerOptions.length > 0) {
    await tx.insert(visitAnswerOptions).values(
      answerOptions.map((option, idx) => ({
        visitAnswerId: insertedAnswer.id,
        optionRole: option.optionRole,
        optionValue: option.optionValue,
        orderIndex: option.orderIndex ?? idx,
        isDeleted: false,
        deletedAt: null,
        createdAt: input.now,
        updatedAt: input.now,
      })),
    );
  }

  if (answerMatrixCells.length > 0) {
    await tx.insert(visitAnswerMatrixCells).values(
      answerMatrixCells.map((cell, idx) => ({
        visitAnswerId: insertedAnswer.id,
        rowKey: cell.rowKey,
        columnKey: cell.columnKey,
        cellValueText: cell.cellValueText,
        cellValueDate: cell.cellValueDate,
        cellSelected: cell.cellSelected,
        orderIndex: cell.orderIndex ?? idx,
        isDeleted: false,
        deletedAt: null,
        createdAt: input.now,
        updatedAt: input.now,
      })),
    );
  }

  const sourceComment = reusableSource.commentText?.trim() ?? "";
  if (sourceComment.length > 0) {
    await tx.insert(visitQuestionComments).values({
      visitSessionQuestionId: input.visitQuestionId,
      commentText: sourceComment,
      commentedAt: input.now,
      isDeleted: false,
      deletedAt: null,
      createdAt: input.now,
      updatedAt: input.now,
    });
  }
  return true;
}
