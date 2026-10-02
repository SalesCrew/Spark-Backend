// Isolated UI preview. Never imports env.ts, db.ts, createApp or index.ts.
import express from "express";
import cors from "cors";
import { randomUUID } from "node:crypto";
import { praemienFixture } from "../lib/praemien-test-fixture.js";
import { createPraemienWorkspaceRouter } from "../routes/praemien-workspace.js";
import { modelTemplate } from "../praemien-model.shared.js";
import { managedGmSummary, mutateWorkspace } from "../lib/praemien-workspace.js";

if (process.env.NODE_ENV === "production" || process.env.DATABASE_URL || process.env.SUPABASE_URL || process.env.SUPABASE_SERVICE_ROLE_KEY)
  throw new Error("Synthetic preview requires a clean environment without production configuration.");
const fixture = await praemienFixture();
// Synthetic catalog: 80 numeric questions, 20 yes/no, 15 choice, 5 unsupported.
for (let i = 1; i <= 120; i++) {
  const id = randomUUID(), type = i <= 80 ? "numeric" : i <= 100 ? "yesno" : i <= 115 ? "single" : "text";
  const text = `${i <= 80 ? "Display" : i <= 100 ? "Distribution" : i <= 115 ? "Platzierung" : "Kommentar"} ${String(i).padStart(3, "0")} · synthetische Testfrage`;
  await fixture.pg.query("insert into question_bank_shared(id,text,question_type,config) values($1,$2,$3,$4)", [id, text, type, JSON.stringify(type === "single" ? { options: ["Kühler", "Großplatzierung", "Keine"] } : {})]);
  if (type !== "text") await fixture.pg.query("insert into question_scoring(question_id,score_key,boni) values($1,$2,1)", [id, type === "numeric" ? "__value__" : type === "yesno" ? "Ja" : "Kühler"]);
  if (type === "yesno") await fixture.pg.query("insert into question_scoring(question_id,score_key,boni) values($1,'Nein',0)", [id]);
}
// Current example is seeded exclusively in the disposable PGlite database.
const xmasWaveId = randomUUID();
await fixture.pg.query("insert into praemien_waves(id,name,year,quarter,status,start_date,end_date,reward_model) values($1,'Kühler + X-Mas · synthetisches Beispiel',2026,4,'draft','2026-10-01','2026-12-31','pillar_tiers')", [xmasWaveId]);
let xmas = await mutateWorkspace(fixture.database, xmasWaveId, 0, { id: fixture.ids.admin, name: "Synthetic admin" }, { type: "rules", model: modelTemplate("xmas") });
const entries = [fixture.ids.gm, fixture.ids.other, fixture.ids.inactive].flatMap((gmId, i) => {
  const values = { new_coolers: i === 1 ? 1 : 0, recovered: 0, returned: i === 2 ? 1 : 0, lost: 0, trucks: 0, trailers: 0, bins: i === 2 ? 30 : 20, fsdu: 0, sleds: 0, pallets: 0, standees: 0, qualified: 1 };
  return [...Object.entries(values).map(([metricKey, value]) => ({ gmId, pillarKey: "flex", metricKey, value, target: null, note: "Synthetisches Beispiel; kein Produktionsdatensatz" })),
    ...[["displays", "percent", 95], ["distribution", "percent", 90], ["quality", "reporting", 55], ["quality", "tags", 55], ["quality", "time", 110]].map(([pillarKey, metricKey, value]) => ({ gmId, pillarKey: String(pillarKey), metricKey: String(metricKey), value: Number(value), target: null, note: "Synthetisch manuell bewertet" }))];
});
await mutateWorkspace(fixture.database, xmasWaveId, xmas.revision, { id: fixture.ids.admin, name: "Synthetic admin" }, { type: "values", entries });
const app = express();
app.use(cors({ origin: ["http://localhost:3017", "http://127.0.0.1:3017"] }));
app.use(express.json({ limit: "2mb" }));
const user = { id: fixture.ids.admin, role: "admin", email: "synthetic@example.test", firstName: "Lokal", lastName: "Admin" };
app.get("/fixture-info", (_req, res) => res.json({ synthetic: true, database: "PGlite in memory", externalConnections: false, questions: 121 }));
app.get("/fixture-session", (_req, res) => res.json({ user, session: { accessToken: "synthetic-boni-only", refreshToken: "synthetic-no-refresh", expiresAt: Math.floor(Date.now() / 1000) + 86400 } }));
app.post("/telemetry/client", (_req, res) => res.sendStatus(202));
app.get("/markets/gm/bonus-summary", async (_req, res) => res.json(await managedGmSummary(fixture.database, fixture.ids.wave, fixture.ids.gm)));
app.use((req, res, next) => {
  if (req.headers.authorization !== "Bearer synthetic-boni-only") { res.status(401).json({ error: "Synthetic local session required" }); return; }
  (req as express.Request & { authUser: unknown }).authUser = { appUserId: fixture.ids.admin, role: "admin" }; next();
});
let kurtiLayout: unknown = null;
app.get("/admin/kurti/layout", (_req, res) => res.json({ layout: kurtiLayout }));
app.put("/admin/kurti/layout", (req, res) => { kurtiLayout = { ...req.body, updatedAt: new Date().toISOString() }; res.json({ layout: kurtiLayout }); });
app.get("/auth/me", (_req, res) => res.json({ user }));
app.get("/admin/markets/chains", (_req, res) => res.json({ chains: ["Billa", "Billa Plus", "Sparmarkt", "Sonstige"] }));
app.get("/admin/modules/:scope", (_req, res) => res.json({ modules: [] }));
app.get("/admin/fragebogen/:scope", (_req, res) => res.json({ fragebogen: [] }));
app.get("/red-month/current", (_req, res) => res.json({ current: { id: randomUUID(), year: 2026, label: "RED 09 · Test", start: "2026-08-31", end: "2026-10-04", isCurrent: true }, config: { anchorStart: "2026-01-05", cycleWeeks: [4, 4, 5], timezone: "Europe/Vienna" } }));
app.get("/red-month/calendar", (_req, res) => res.json({ periods: [] }));
app.get("/admin/red-month/years", (_req, res) => res.json({ years: [], current: null }));
app.get("/admin/campaigns/answer-change-requests", (_req, res) => res.json({ requests: [] }));
app.get("/admin/time-change-requests", (_req, res) => res.json({ requests: [] }));
app.get("/admin/campaigns/visit-session-delete-requests", (_req, res) => res.json({ requests: [] }));
let delayedRequest: { path: string; until: number } | null = null;
app.post("/fixture-ui-delay", (req, res) => {
  const { path, milliseconds } = req.body;
  if (!["/waves", "/sources"].includes(path) || !Number.isInteger(milliseconds) || milliseconds < 0 || milliseconds > 5000) {
    res.status(400).json({ error: "Only bounded delays on synthetic UI reads are supported" }); return;
  }
  delayedRequest = { path, until: Date.now() + milliseconds };
  res.json({ synthetic: true });
});
app.use("/admin/praemien/workspace", async (req, _res, next) => {
  if (req.method === "GET" && delayedRequest?.path === req.path) {
    const delay = delayedRequest;
    await new Promise((resolve) => setTimeout(resolve, Math.max(0, delay.until - Date.now())));
    if (delayedRequest === delay) delayedRequest = null;
  }
  next();
});
app.use("/admin/praemien/workspace", createPraemienWorkspaceRouter(fixture.database));
app.use((_req, res) => res.status(404).json({ error: "Route not part of isolated preview" }));
const server = app.listen(4017, "127.0.0.1", () => process.stdout.write("Synthetic Boni preview: http://127.0.0.1:4017 (PGlite in memory, no external services)\n"));
async function stop() { server.close(); await fixture.pg.close(); process.exit(0); }
process.once("SIGINT", stop); process.once("SIGTERM", stop);
