import { and, eq, inArray, sql } from "drizzle-orm";
import { db } from "./db.js";
import { visitAnswers, visitSessionQuestions, visitSessionSections } from "./schema.js";

export type RedSurveySessionStats = {
  visitSessionId: string;
  flaggedQuestionCount: number;
  yesCount: number;
  completedCount: number;
};

export async function loadRedSurveySessionStatsBySessionIds(sessionIds: string[]): Promise<Map<string, RedSurveySessionStats>> {
  const uniqueSessionIds = Array.from(new Set(sessionIds.filter((entry) => typeof entry === "string" && entry.length > 0)));
  if (uniqueSessionIds.length === 0) return new Map();

  const rows = await db
    .select({
      visitSessionId: visitSessionSections.visitSessionId,
      flaggedQuestionCount: sql<number>`
        count(*) filter (
          where ${visitSessionQuestions.redSurveySnapshot} is true
        )::int
      `,
      yesCount: sql<number>`
        count(*) filter (
          where ${visitSessionQuestions.redSurveySnapshot} is true
            and ${visitAnswers.answerStatus} = 'answered'
            and ${visitAnswers.isValid} = true
            and lower(trim(coalesce(${visitAnswers.valueText}, ''))) = 'ja'
        )::int
      `,
    })
    .from(visitSessionSections)
    .innerJoin(
      visitSessionQuestions,
      and(
        eq(visitSessionQuestions.visitSessionSectionId, visitSessionSections.id),
        eq(visitSessionQuestions.isDeleted, false),
      ),
    )
    .leftJoin(
      visitAnswers,
      and(
        eq(visitAnswers.visitSessionQuestionId, visitSessionQuestions.id),
        eq(visitAnswers.isDeleted, false),
      ),
    )
    .where(
      and(
        inArray(visitSessionSections.visitSessionId, uniqueSessionIds),
        eq(visitSessionSections.isDeleted, false),
      ),
    )
    .groupBy(visitSessionSections.visitSessionId);

  const result = new Map<string, RedSurveySessionStats>();
  for (const row of rows) {
    const flaggedQuestionCount = Number(row.flaggedQuestionCount ?? 0);
    const yesCount = Number(row.yesCount ?? 0);
    result.set(row.visitSessionId, {
      visitSessionId: row.visitSessionId,
      flaggedQuestionCount,
      yesCount,
      completedCount: yesCount,
    });
  }
  return result;
}
