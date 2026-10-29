// Only an independent preview. Never loaded by app.ts/index.ts or normal local
// hosting. The fixture uses a disposable in-memory DB and simulated auth.
import express from "express";
import request from "supertest";
import { supplementalFixture } from "./gm-supplemental-fixture.js";

async function main() {
  const f = await supplementalFixture();
  for (const [firstName, lastName, region, supplemental] of [["Nora", "Mustermann", "West", false], ["Lukas", "Beispiel", "Nord", false], ["Mara", "Musterfrau", "Ost", true], ["Jonas", "Demowelt", "Süd", true]] as const) {
    const result = await request(f.app).post("/admin/users").set("Authorization", "Bearer synthetic-admin").send({
      role: "gm", firstName, lastName, region, isSupplementalGm: supplemental,
      email: `${firstName.toLowerCase()}@example.test`, phone: "+43 660 000 000", address: "Demogasse 12", city: "Wien", postalCode: "1010",
    });
    if (result.status !== 201) throw new Error("Synthetic creation failed: " + result.body.error);
  }
  const user = { id: f.adminId, role: "admin", email: "admin@example.test", firstName: "Demo", lastName: "Admin", isActive: true };
  const session = { accessToken: "synthetic-admin", refreshToken: "synthetic-admin", expiresAt: Math.floor(Date.now()/1000) + 86400 };
  const app = express(); app.use(express.json());
  app.use((req, res, next) => {
    res.set("Access-Control-Allow-Origin", "http://127.0.0.1:3037");
    res.set("Access-Control-Allow-Headers", "Authorization,Content-Type,x-coke-spark-page-key,X-Request-Id");
    res.set("Access-Control-Allow-Methods", "GET,POST,PATCH,OPTIONS");
    if (req.method === "OPTIONS") { res.sendStatus(204); return; } next();
  });
  app.post("/auth/login", (req, res) => req.body.email === user.email && req.body.password === "preview" ? res.json({ user, session }) : res.sendStatus(401));
  app.post("/auth/refresh", (_req, res) => res.json({ user, session }));
  app.get("/auth/me", (_req, res) => res.json({ user }));
  app.get("/employee-agreement/current", (_req, res) => res.json({ accepted: true }));
  const current = { id: "synthetic-red-10", label: "RED 10", periodIndex: 10, periodIndexFromAnchor: 9, start: "2026-10-05", end: "2026-10-30", year: 2026, status: "active", isCurrent: true, daysUntilEnd: 1 };
  app.get("/red-month/current", (_req, res) => res.json({ current, config: { anchorStart: "2026-01-05", timezone: "Europe/Vienna", cycleWeeks: [4,4,5] } }));
  app.get("/red-month/calendar", (_req, res) => res.json({ periods: [current] }));
  app.get("/admin/red-month/years", (_req, res) => res.json({ years: [], current }));
  app.get("/admin/kurti/layout", (_req, res) => res.json({ layout: null }));
  app.put("/admin/kurti/layout", (req, res) => res.json({ layout: req.body }));
  app.post("/telemetry/events", (_req, res) => res.sendStatus(204));
  app.use(f.app);
  app.get(/.*/, (_req, res) => res.json({ modules: [], fragebogen: [], campaigns: [], markets: [], chains: [], requests: [], messages: [], users: [] }));
  const server = app.listen(4037, "127.0.0.1", () => console.log("Isolated Zusatz-GM preview: synthetic DB/API on 127.0.0.1:4037; admin@example.test / preview"));
  process.once("SIGTERM", () => server.close(async () => { await f.pg.close(); process.exit(0); }));
}
void main().catch(error => { console.error(error.message); process.exitCode = 1; });
