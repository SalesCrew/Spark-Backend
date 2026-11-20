import { eq } from "drizzle-orm";
import type { db } from "./lib/db.js";
import { smQuestionnaireTemplates } from "./lib/schema.js";

type Executor = typeof db | Parameters<Parameters<typeof db.transaction>[0]>[0];
/** Authoritative submission scope. Question codes are opaque and must not be used as a scope hint. */
export async function smOptionalCommentsForSubmission(executor: Executor, submission: { SMDurcharbeitVisitId: string | null; questionnaireTemplateId: string }): Promise<boolean> {
  if (submission.SMDurcharbeitVisitId) return true;
  const [template] = await executor.select({ stableCode: smQuestionnaireTemplates.stableCode }).from(smQuestionnaireTemplates)
    .where(eq(smQuestionnaireTemplates.id, submission.questionnaireTemplateId)).limit(1);
  return template?.stableCode.startsWith("smdurcharbeit_") === true;
}
