import express from "express";
import { createHash } from "node:crypto";
import { createQuestionnairePickerFixture } from "./SMDurcharbeit-questionnaire-picker-fixture.js";

// Tour 2 regression preview. Disposable-only browser preview. No dotenv, application entrypoint, production client or jobs.
const f = await createQuestionnairePickerFixture();
f.setNow(new Date().toISOString());
const preservedTables = [f.schema.smQuestionnaireSubmissions, f.schema.smQuestionnaireSubmissionSections,
  f.schema.smQuestionnaireSubmissionQuestions, f.schema.smQuestionAnswers, f.schema.smQuestionAnswerOptions,
  f.schema.smQuestionAnswerFiles, f.schema.smQuestionAnswerEvents, f.schema.smSMDurcharbeitVisits,
  f.schema.smSMDurcharbeitPeriods, f.schema.smQuestionnaireVersions];
const preservedIds = await Promise.all(preservedTables.map(async table => new Set((await f.database.select().from(table as any)).map((row: any) => row.id))));
const fingerprint = async () => createHash("sha256").update(JSON.stringify(await Promise.all(preservedTables.map(async (table, index) =>
  (await f.database.select().from(table as any)).filter((row: any) => preservedIds[index]!.has(row.id)).sort((a: any, b: any) => a.id.localeCompare(b.id)))))).digest("hex");
const originalFingerprint = await fingerprint();
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
app.get("/__synthetic/status", async (_req, res) => res.json({ kind: "sm-da-questionnaire-picker-disposable", originalFingerprint, currentFingerprint: await fingerprint(), photoCount: f.objects.size }));
// Unrelated shell preferences stay in memory in this disposable preview.
let kurtiLayout: unknown = null;
app.get("/admin/kurti/layout", (_req, res) => res.json({ layout: kurtiLayout }));
app.put("/admin/kurti/layout", (req, res) => { kurtiLayout = { ...req.body, updatedAt: new Date().toISOString() }; res.json({ layout: kurtiLayout }); });
app.post("/telemetry/events", (_req, res) => res.sendStatus(204));
app.get("/sm/settings/text-scale", (_req, res) => res.json({ textScalePercent: 0, updatedAt: null }));
app.use(f.app);
const server = app.listen(4037, "127.0.0.1", () => console.log("Durcharbeit questionnaire picker preview · disposable synthetic data · 4037"));
for (const signal of ["SIGINT", "SIGTERM"] as const) process.once(signal, () => server.close(() => { void f.pg.close().then(() => process.exit(0)); }));
