// Explicit draft-resume operation; no configured connector, auth, storage or jobs.
import { and, asc, eq, inArray } from "drizzle-orm";
import type { PgDatabase, PgQueryResultHKT } from "drizzle-orm/pg-core";
import { copyReusableAnswer, gmQuarterQuestionIds, loadReusableSubmittedAnswers } from "./visit-answer-reuse.js";
import { visitAnswers, visitQuestionComments, visitSessionQuestions, visitSessionSections, visitSessions } from "./schema.js";

export async function recoverQuarterlyDraftAnswers<T extends PgQueryResultHKT, S extends Record<string, unknown>>(
  db: Pick<PgDatabase<T, S>, "transaction">, sessionId: string, gmUserId: string,
) {
  return db.transaction(async tx => {
    const [session] = await tx.select().from(visitSessions).where(and(
      eq(visitSessions.id, sessionId), eq(visitSessions.gmUserId, gmUserId), eq(visitSessions.isDeleted, false),
    )).limit(1).for("update");
    if (!session) return { found: false, recoveredAnswers: 0 };
    if (session.status !== "draft" || session.submittedAt) return { found: true, recoveredAnswers: 0 };

    // The answer writer takes the same question locks. Read touch markers only
    // after these locks so concurrent edits win without being overwritten.
    const questions = await tx.select({
      id: visitSessionQuestions.id, sectionId: visitSessionSections.id,
      section: visitSessionSections.section, questionId: visitSessionQuestions.questionId,
      type: visitSessionQuestions.questionType,
      moduleName: visitSessionQuestions.moduleNameSnapshot,
    }).from(visitSessionQuestions).innerJoin(visitSessionSections, eq(visitSessionSections.id, visitSessionQuestions.visitSessionSectionId))
      .where(and(eq(visitSessionSections.visitSessionId, session.id), eq(visitSessionSections.isDeleted, false), eq(visitSessionQuestions.isDeleted, false), eq(visitSessionQuestions.appliesToMarketChainSnapshot, true)))
      .orderBy(asc(visitSessionQuestions.id)).for("update", { of: visitSessionQuestions });
    const eligibleIds = new Set(gmQuarterQuestionIds(questions.map(question => ({ section: question.section, questions: [question] })), session.startedAt));
    if (!eligibleIds.size) return { found: true, recoveredAnswers: 0 };
    const snapshotIds = questions.map(question => question.id);
    // Include tombstones, blank/invalid/hidden answers and comments. A saved
    // blank can be an intentional deletion; it must never be resurrected.
    const answers = await tx.select({ id: visitAnswers.visitSessionQuestionId }).from(visitAnswers).where(inArray(visitAnswers.visitSessionQuestionId, snapshotIds));
    const comments = await tx.select({ id: visitQuestionComments.visitSessionQuestionId }).from(visitQuestionComments).where(inArray(visitQuestionComments.visitSessionQuestionId, snapshotIds));
    const touched = new Set([...answers, ...comments].map(row => row.id));
    const touchedQuestionIds = new Set(questions.filter(question => touched.has(question.id)).map(question => question.questionId));
    const missing = questions.filter(question => eligibleIds.has(question.questionId) && !touchedQuestionIds.has(question.questionId));
    if (!missing.length) return { found: true, recoveredAnswers: 0 };
    const ids = [...new Set(missing.map(question => question.questionId))];
    // Only fetch configurations for actual recovery targets; unrelated POS
    // snapshots can contain large embedded assets.
    const configs = await tx.select({ id: visitSessionQuestions.id, config: visitSessionQuestions.questionConfigSnapshot })
      .from(visitSessionQuestions).where(inArray(visitSessionQuestions.id, missing.map(question => question.id)));
    const configById = new Map(configs.map(question => [question.id, question.config]));
    const sources = await loadReusableSubmittedAnswers(tx, {
      gmUserId, marketId: session.marketId, questionIds: ids, quarterQuestionIds: ids, now: session.startedAt,
    }, async () => { throw new Error("Draft recovery must never use a RED-month fallback"); });
    let recoveredAnswers = 0;
    for (const question of missing) {
      const source = sources.get(question.questionId);
      if (source && await copyReusableAnswer(tx, {
        sessionId: session.id, sectionId: question.sectionId, visitQuestionId: question.id,
        question: { questionId: question.questionId, type: question.type, config: configById.get(question.id) ?? {} }, now: new Date(),
      }, source)) recoveredAnswers++;
    }
    return { found: true, recoveredAnswers };
  });
}
