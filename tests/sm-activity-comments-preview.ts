import express from "express";
import { randomUUID } from "node:crypto";
import { createOptionalCommentsFixture } from "./sm-optional-comments-fixture.js";

// Disposable-only browser preview. No dotenv, application entrypoint, production client or jobs.
for (const key of ["DATABASE_URL", "SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"]) if (process.env[key]) throw Error("Production configuration forbidden");
const f = await createOptionalCommentsFixture();
for (const q of f.payload.sections[0].questions.filter((q:any)=>q.required)) await f.save(q.id,q.type === "yesnomulti" ? {kind:"yesnomulti",optionCode:q.options[0].code,subOptions:[]} : {kind:"choice",optionCode:q.options[0].code});
await f.upload(f.payload.sections[0].questions.find((q:any)=>q.type === "photo").id);
await f.sm("post", f.path+"/submit").send({visitStartedAt:"2026-11-20T09:00:00Z",visitCompletedAt:"2026-11-20T09:30:00Z",clientMutationToken:randomUUID()}).expect(200);
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
app.get("/__synthetic/status", async (_req, res) => res.json({ kind: "sm-activity-comments-disposable", originalFingerprint: f.originalFingerprint, currentFingerprint: await f.fingerprint() }));
// Unrelated shell preferences stay in memory in this disposable preview.
let kurtiLayout: unknown = null;
app.get("/admin/kurti/layout", (_req, res) => res.json({ layout: kurtiLayout }));
app.put("/admin/kurti/layout", (req, res) => { kurtiLayout = { ...req.body, updatedAt: new Date().toISOString() }; res.json({ layout: kurtiLayout }); });
app.post("/telemetry/events", (_req, res) => res.sendStatus(204));
app.get("/sm/settings/text-scale", (_req, res) => res.json({ textScalePercent: 0, updatedAt: null }));
app.get("/__synthetic/photo", (_req,res) => res.type("image/png").send(f.historicalPhoto.bytes));
app.get("/__synthetic/info", (_req,res) => res.json({ visitId: f.visit.visitId, submissionId: f.payload.submission.id, campaignId: f.campaign.id, path: f.path, questions: f.payload.sections[0].questions }));
app.use(f.app);
const server = app.listen(4037, "127.0.0.1", () => console.log("Durcharbeit activity comments preview · disposable synthetic data · 4037"));
for (const signal of ["SIGINT", "SIGTERM"] as const) process.once(signal, () => server.close(() => { void f.pg.close().then(() => process.exit(0)); }));
