import express from "express";
import { createSmDashboardFiltersFixture } from "./sm-dashboard-filters-fixture.js";

// Standalone disposable preview; never imports index.ts, dotenv or scheduled jobs.
const f = await createSmDashboardFiltersFixture({ overviewPreview: true });
const preview = express();
preview.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "http://127.0.0.1:3037");
  res.setHeader("Access-Control-Allow-Headers", "Authorization, Content-Type, x-coke-spark-page-key");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS");
  if (req.method === "OPTIONS") { res.sendStatus(204); return; }
  next();
});
preview.use(express.json());
const user = { id: f.admin, role: "sm_admin", email: "sm-admin@preview.test", firstName: "Vorschau", lastName: "Admin", isActive: true };
const session = { user, session: { accessToken: "synthetic-sm-admin", refreshToken: "synthetic-sm-admin", expiresAt: Math.floor(Date.now() / 1000) + 86400 } };
preview.post("/auth/login", (req, res) => {
  if (req.body.email !== user.email || req.body.password !== "preview") { res.sendStatus(401); return; }
  res.json(session);
});
preview.post("/auth/refresh", (req, res) => {
  if (req.body.refreshToken !== "synthetic-sm-admin") { res.sendStatus(401); return; }
  res.json(session);
});
preview.get("/auth/me", (req, res) => {
  if (req.header("authorization") !== "Bearer synthetic-sm-admin") { res.sendStatus(401); return; }
  res.json({ user });
});
preview.use(f.app);
preview.use((_req, res) => { res.status(404).json({ error: "Nicht Teil der synthetischen SM-Dashboard-Vorschau." }); });
const server = preview.listen(4037, "127.0.0.1", () => console.log("SM dashboard filter preview · disposable synthetic DB · http://127.0.0.1:4037"));
for (const signal of ["SIGINT", "SIGTERM"] as const) process.once(signal, () => server.close(() => { void f.pg.close().then(() => process.exit(0)); }));
