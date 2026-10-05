// LOCAL ONLY. No environment loader, production connections, scheduler or write jobs.
import express from "express";
import cors from "cors";
import { randomUUID } from "node:crypto";
import { campaignExtensionFixture } from "../lib/campaign-extension-fixture.js";
import { createCampaignExtensionRouter } from "../routes/campaign-extension.js";
import { viennaCampaignDate } from "../lib/campaign-extension.js";
import type { AuthedRequest } from "../middleware/auth.js";

if (process.env.NODE_ENV === "production" || process.env.DATABASE_URL || process.env.SUPABASE_URL || process.env.SUPABASE_SERVICE_ROLE_KEY)
  throw new Error("Local preview requires an environment without production configuration.");
const fixture = await campaignExtensionFixture();
const today = viennaCampaignDate();
function day(offset: number) { const value = new Date(today + "T12:00:00Z"); value.setUTCDate(value.getUTCDate() + offset); return value.toISOString().slice(0, 10); }
for (const section of ["standard", "flex", "billa", "kuehler", "mhd", "durcharbeit"] as const) {
  const label = { standard: "Standard", flex: "Flex", billa: "Billa", kuehler: "Kühler", mhd: "MHD", durcharbeit: "Durcharbeit" }[section];
  await fixture.seed({ section, name: label + " · abgelaufen (Test)", status: "inactive", startDate: day(-21), endDate: day(-3) });
  await fixture.seed({ section, name: label + " · aktiv (Test)", status: "active", startDate: day(-10), endDate: day(7) });
}
await fixture.seed({ name: "Unbefristet (Test)", scheduleType: "always", startDate: null, endDate: null, status: "active" });
await fixture.seed({ name: "Geplant (Test)", status: "scheduled", startDate: day(15), endDate: day(25) });
const overlap = await fixture.seed({ name: "Überschneidung (Test)", status: "inactive", startDate: day(-21), endDate: day(-3) });
await fixture.seed({ name: "Folgekampagne (Test)", status: "scheduled", startDate: day(2), endDate: day(20) }, overlap.marketId);
const baseline = await fixture.snapshot();
const app = express();
app.use(cors({ origin: ["http://localhost:3037", "http://127.0.0.1:3037"] }));
app.use(express.json());
const user = { id: fixture.gm, role: "admin", email: "preview@example.test", firstName: "Lokal", lastName: "Testdaten" };
const session = () => ({ user, session: { accessToken: "synthetic-campaign-preview", refreshToken: "synthetic-no-refresh", expiresAt: Math.floor(Date.now() / 1000) + 86400 } });
app.get("/fixture-session", (_req, res) => res.json(session()));
app.get("/fixture-info", (_req, res) => res.json({ synthetic: true, database: "disposable PGlite", externalConnections: false }));
app.post("/telemetry/client", (_req, res) => res.sendStatus(202));
app.use((req: AuthedRequest, res, next) => {
  if (req.headers.authorization !== "Bearer synthetic-campaign-preview") return void res.status(401).json({ error: "Isolierte Vorschau: lokale Testsitzung erforderlich." });
  req.authUser = { appUserId: fixture.gm, supabaseAuthId: fixture.gm, role: "admin", email: user.email }; next();
});
app.get("/auth/me", (_req, res) => res.json({ user }));
let kurtiLayout: unknown = null;
app.get("/admin/kurti/layout", (_req, res) => res.json({ layout: kurtiLayout }));
app.put("/admin/kurti/layout", (req, res) => { kurtiLayout = { ...req.body, updatedAt: new Date().toISOString() }; res.json({ layout: kurtiLayout }); });
app.get("/fixture-snapshot", async (_req, res) => res.json(await fixture.snapshot()));
app.get("/fixture-baseline", (_req, res) => res.json(baseline));
// Bounded synthetic failure/delay injection to verify stale edits and duplicate submissions.
let delay = 0;
app.post("/fixture-delay", (req, res) => { delay = Math.min(3000, Math.max(0, Number(req.body.milliseconds) || 0)); res.json({ synthetic: true }); });
app.post("/fixture-change/:id", async (req, res) => {
  const row = await fixture.get(String(req.params.id));
  if (!row) return void res.sendStatus(404);
  await fixture.pg.query("update campaigns set updated_at=$2 where id=$1", [row.id, new Date(row.updatedAt.getTime() + 10).toISOString()]);
  res.json({ synthetic: true });
});
app.use("/admin/campaigns", async (req, _res, next) => { if (req.method === "PATCH" && delay) await new Promise((resolve) => setTimeout(resolve, delay)); next(); });
app.use("/admin/campaigns", createCampaignExtensionRouter(fixture.dependencies));
app.get("/admin/campaigns", async (_req, res) => {
  const rows = await fixture.database.select().from((await import("../lib/schema.js")).campaigns);
  const assignments = await fixture.database.select().from((await import("../lib/schema.js")).campaignMarketAssignments);
  res.json({ campaigns: rows.map((row) => ({
    ...row, status: row.status === "inactive" ? "inactive" : row.scheduleType === "always" ? "active" : row.startDate! > today ? "scheduled" : row.endDate! < today ? "inactive" : "active",
    currentFragebogenName: "Testfragebogen", assignedGmName: null,
    marketIds: assignments.filter((a) => a.campaignId === row.id).map((a) => a.marketId),
    assignments: assignments.filter((a) => a.campaignId === row.id).map((a) => ({ ...a, gmName: "Synthetischer GM" })), history: [],
  })) });
});
app.get("/admin/campaigns/market-visit-status", async (req, res) => {
  const ids = String(req.query.campaignIds ?? "").split(",");
  const rows = (await fixture.snapshot()).campaign_market_assignments as Array<{campaign_id: string; market_id: string; gm_user_id: string}>;
  res.json({ campaigns: ids.map((id) => ({ campaignId: id, markets: rows.filter((a) => a.campaign_id === id).map((a) => ({
    marketId: a.market_id, gmUserId: a.gm_user_id, gmName: "Synthetischer GM", targetVisitCount: 3, submittedVisitCount: 2,
    isComplete: false, hasSubmittedVisit: true, sessionId: null, startedAt: null, submittedAt: null, durationMinutes: null,
  })) })) });
});
app.get("/admin/campaigns/market-visit-export-index", (_req, res) => res.json({ visits: [] }));
async function marketList() {
  return (await fixture.pg.query<{id: string; name: string}>("select * from markets")).rows.map((m) => ({ ...m, dbName: "BILLA", chain: "Billa", city: "Teststadt", address: "Teststraße 1", postalCode: "1010", region: "Nord", isActive: true, assignedGmUserId: fixture.gm, assignedGmName: "Synthetischer GM" }));
}
app.get("/admin/campaigns/assigned-markets", async (_req, res) => res.json({ markets: await marketList() }));
app.get("/markets", async (_req, res) => res.json({ markets: await marketList() }));
app.get("/admin/users", (_req, res) => res.json({ users: [{ ...user, role: "gm", firstName: "Synthetischer", lastName: "GM", isActive: true }] }));
app.get("/admin/markets/chains", (_req, res) => res.json({ chains: ["Billa"] }));
app.get("/admin/modules/:scope", (_req, res) => res.json({ modules: [] }));
app.get("/admin/fragebogen/:scope", (_req, res) => res.json({ fragebogen: [] }));
app.get("/admin/campaigns/answer-change-requests", (_req, res) => res.json({ requests: [] }));
app.get("/admin/time-change-requests", (_req, res) => res.json({ requests: [] }));
app.get("/admin/campaigns/visit-session-delete-requests", (_req, res) => res.json({ requests: [] }));
app.get("/red-month/current", (_req, res) => res.json({ current: { id: randomUUID(), year: 2026, label: "Testmonat", start: day(-21), end: day(20), isCurrent: true }, config: { anchorStart: "2026-01-05", cycleWeeks: [4, 4, 5], timezone: "Europe/Vienna" } }));
app.get("/red-month/calendar", (_req, res) => res.json({ periods: [] }));
app.get("/admin/red-month/years", (_req, res) => res.json({ years: [], current: null }));
app.use((_req, res) => res.status(403).json({ error: "Aktion außerhalb der isolierten Kampagnenvorschau gesperrt." }));
const server = app.listen(4037, "127.0.0.1", () => process.stdout.write("Campaign preview backend: http://127.0.0.1:4037 (synthetic isolated database)\n"));
async function stop() { server.close(); await fixture.pg.close(); process.exit(0); }
process.once("SIGINT", stop); process.once("SIGTERM", stop);
