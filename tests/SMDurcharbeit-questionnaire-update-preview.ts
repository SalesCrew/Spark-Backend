import express from "express";
import { createQuestionnaireUpdateFixture } from "./SMDurcharbeit-questionnaire-update-fixture.js";

// Disposable-only browser preview. No dotenv, application entrypoint, production client or jobs.
const f = await createQuestionnaireUpdateFixture();
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
  res.json({ accepted: true });
});
app.use(f.app);
const server = app.listen(4037, "127.0.0.1", () => console.log("Durcharbeit questionnaire update preview · disposable synthetic data · 4037"));
for (const signal of ["SIGINT", "SIGTERM"] as const) process.once(signal, () => server.close(() => { void f.pg.close().then(() => process.exit(0)); }));
