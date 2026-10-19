import { randomUUID } from "node:crypto";
import { and, eq } from "drizzle-orm";
import express from "express";
import { createQuestionnaireUpdateFixture } from "./SMDurcharbeit-questionnaire-update-fixture.js";

// Disposable-only browser preview. No dotenv, application entrypoint, production client or jobs.
const f = await createQuestionnaireUpdateFixture();
// Add historical typing snapshots and a comment-triggered draft in this disposable DB only.
const q = (await f.database.select().from(f.schema.smQuestionnaireSubmissionQuestions).where(and(
  eq(f.schema.smQuestionnaireSubmissionQuestions.submissionId, f.completed.submissionId),
  eq(f.schema.smQuestionnaireSubmissionQuestions.questionTypeSnapshot, "yesno"))))[0]!;
const option = q.answerOptionsSnapshot[1]!;
const code = String(option.code);
const config = { ...q.configSnapshot, commentTrigger: { mode: "options", optionCodes: [code] } };
await f.database.update(f.schema.smQuestionnaireSubmissionQuestions).set({ configSnapshot: config }).where(eq(f.schema.smQuestionnaireSubmissionQuestions.id, q.id));
const draftQuestion = (await f.database.select().from(f.schema.smQuestionnaireSubmissionQuestions).where(and(
  eq(f.schema.smQuestionnaireSubmissionQuestions.submissionId, f.draft.submissionId),
  eq(f.schema.smQuestionnaireSubmissionQuestions.questionCodeSnapshot, q.questionCodeSnapshot))))[0]!;
await f.database.update(f.schema.smQuestionnaireSubmissionQuestions).set({ configSnapshot: config }).where(eq(f.schema.smQuestionnaireSubmissionQuestions.id, draftQuestion.id));
const original = (await f.database.select().from(f.schema.smQuestionAnswers).where(and(eq(f.schema.smQuestionAnswers.submissionQuestionId, q.id),eq(f.schema.smQuestionAnswers.isCurrent,true))))[0]!;
await f.database.update(f.schema.smQuestionAnswers).set({isCurrent:false}).where(eq(f.schema.smQuestionAnswers.id,original.id));
const finalComment = "Aufgefüllt wird am Nachmittag weil dann mehr Personal da ist.";
let previous = original.id;
for (let version=2;version<=33;version++) {
  const id=randomUUID(), comment=finalComment.slice(0, Math.ceil((version-1)*finalComment.length/32));
  await f.database.insert(f.schema.smQuestionAnswers).values({...original,id,answerVersion:version,isCurrent:version===33,supersedesAnswerId:previous,
    valueJson:{kind:"choice",optionCode:code,comment},answeredAt:new Date(Date.parse("2026-10-19T10:00:00Z")+version*500)});
  previous=id;
}
const app = express(); app.use(express.json());
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "http://127.0.0.1:3037");
  res.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type, x-coke-spark-page-key");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS");
  if (req.method === "OPTIONS") { res.sendStatus(204); return; } next();
});
const admin = { id: f.adminId, role: "sm_admin", email: "sm-admin@preview.test", firstName: "Vorschau", lastName: "Admin", isActive: true };
const employee = { id: f.employee, role: "sm", email: "sm@preview.test", firstName: "Local", lastName: "SM", isActive: true };
const session = (sm: boolean) => ({ user: sm ? employee : admin, session: { accessToken: sm ? "synthetic-sm" : "synthetic-sm-admin", refreshToken: sm ? "synthetic-sm" : "synthetic-sm-admin", expiresAt: Math.floor(Date.now() / 1000) + 86400 } });
app.post("/auth/login", (req, res) => {
  if (![admin.email, employee.email].includes(req.body.email) || req.body.password !== "preview") { res.sendStatus(401); return; }
  res.json(session(req.body.email === employee.email));
});
app.post("/auth/refresh", (req, res) => {
  if (!["synthetic-sm-admin", "synthetic-sm"].includes(req.body.refreshToken)) { res.sendStatus(401); return; }
  res.json(session(req.body.refreshToken === "synthetic-sm"));
});
app.get("/auth/me", (req, res) => {
  const token = req.header("authorization");
  if (!["Bearer synthetic-sm-admin", "Bearer synthetic-sm"].includes(token ?? "")) { res.sendStatus(401); return; }
  res.json({ user: token === "Bearer synthetic-sm" ? employee : admin });
});
app.get("/employee-agreement/current", (req, res) => {
  if (req.header("authorization") !== "Bearer synthetic-sm") { res.sendStatus(401); return; }
  res.json({ accepted: true, agreement: { key: "synthetic", version: "synthetic", title: "Synthetische Vereinbarung", hash: "synthetic", effectiveDate: "2026-10-01", sections: [] }, acceptance: { acceptedAt: "2026-10-01T00:00:00Z" } });
});
app.get("/__fixture/comment-history", async (_req,res) => {
  const rows=await f.database.select({version:f.schema.smQuestionAnswers.answerVersion,value:f.schema.smQuestionAnswers.valueJson,current:f.schema.smQuestionAnswers.isCurrent}).from(f.schema.smQuestionAnswers).where(eq(f.schema.smQuestionAnswers.submissionQuestionId,draftQuestion.id));
  res.json({count:rows.length,rows});
});
app.use(f.app);
const server = app.listen(4037, "127.0.0.1", () => console.log("Durcharbeit visit review/comment preview · disposable synthetic data · 4037"));
for (const signal of ["SIGINT", "SIGTERM"] as const) process.once(signal, () => server.close(() => { void f.pg.close().then(() => process.exit(0)); }));
