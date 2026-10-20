// Isolated preview only: no application bootstrap, environment files, production connections or schedulers.
import {
  createBriefingFixture,
  syntheticBriefingPdf,
} from "./gm-briefing-fixture.js";
import { randomUUID } from "node:crypto";
import { writeFile } from "node:fs/promises";
const f = await createBriefingFixture();
f.setNow(new Date().toISOString());
const bytes = await syntheticBriefingPdf(3);
await writeFile("/tmp/coke-wsp-synthetic.pdf", bytes);
const draft = await f.service.upload(
  bytes,
  "Wochenschwerpunkte KW 43.pdf",
  f.admin,
  randomUUID(),
);
await f.service.save(draft.id, {
  expectedRevision: 1,
  title: "Wochenschwerpunkte KW 43",
  description: "Diese Woche im Fokus: starke Platzierungen und volle Regale.",
  visibleFrom: null,
  visibleUntil: null,
  pages: draft.pages.map((p, i) => ({
    ...p,
    title: [
      "Sichtbarkeit am POS",
      "Aktionsflächen im Blick",
      "Gemeinsam zum Abschluss",
    ][i]!,
    description: [
      "Prüfe die Platzierungen und halte das Ergebnis beim Marktbesuch fest.",
      "Eine gut sichtbare Aktionsfläche macht den Unterschied.",
      "Dokumentiere offene Punkte für den nächsten Besuch.",
    ][i]!,
  })),
});
const ready = (await f.service.list())[0]!;
await f.service.publish(ready.id, ready.draftRevision, randomUUID(), f.admin);
const user = (role: string) => ({
  id: role === "gm" ? f.gm : f.admin,
  role,
  email: `${role}@preview.invalid`,
  firstName: role === "gm" ? "Alex" : "Preview",
  lastName: role === "gm" ? "Berger" : "Admin",
});
f.app.post("/auth/login", (req, res) => {
  const role = req.body.email?.includes("gm@") ? "gm" : "admin";
  res.json({
    user: user(role),
    session: {
      accessToken: `synthetic-${role}`,
      refreshToken: `synthetic-refresh-${role}`,
      expiresAt: Math.floor(Date.now() / 1000) + 86400,
    },
  });
});
f.app.get("/auth/me", (req, res) =>
  res.json({
    user: user(
      req.header("authorization")?.includes("synthetic-gm") ? "gm" : "admin",
    ),
  }),
);
f.app.get("/employee-agreement/current", (_req, res) =>
  res.json({
    accepted: true,
    agreement: {
      key: "preview",
      version: "1",
      title: "Synthetic",
      hash: "preview",
      effectiveDate: "2026-10-19",
      sections: [],
    },
    acceptance: {
      acceptedAt: new Date().toISOString(),
      version: "1",
      hash: "preview",
    },
  }),
);
f.app.get("/gm/settings/text-scale", (_req, res) =>
  res.json({ textScalePercent: 0 }),
);
f.app.patch("/gm/settings/text-scale", (_req, res) =>
  res.json({ textScalePercent: 0 }),
);
const period = {
  id: randomUUID(),
  label: "RED 10",
  startDate: "2026-10-05",
  endDate: "2026-10-30",
  start: "2026-10-05",
  end: "2026-10-30",
  year: 2026,
  isCurrent: true,
  daysUntilEnd: 11,
  redYear: 2026,
};
f.app.get("/red-month/current", (_req, res) =>
  res.json({
    current: period,
    config: {
      anchorStart: "2026-01-05",
      cycleWeeks: [4, 4, 5],
      timezone: "Europe/Vienna",
    },
  }),
);
f.app.get("/gm/dashboard/critical", (_req, res) =>
  res.json({
    serverTime: new Date().toISOString(),
    daySession: {
      session: null,
      gate: {
        dayStarted: false,
        startKmPending: false,
        endKmPending: false,
        pauseOpen: false,
      },
    },
    activeVisit: null,
    activeTimeTrackingDrafts: [],
    redPeriod: { ...period, daysUntilEnd: 11 },
  }),
);
f.app.get("/markets/gm/kpi-summary", (_req, res) =>
  res.json({
    ippAllTimeAvg: 94,
    ippSampleCount: 8,
    bonusCumulativeEur: 240,
    lastComputedAt: new Date().toISOString(),
  }),
);
f.app.get("/markets/gm/bonus-summary", (_req, res) =>
  res.json({ hasActiveWave: false, goals: [] }),
);
f.app.get("/markets/gm/kuehler-mhd-progress", (_req, res) => {
  const section = { current: 0, total: 0, percent: 0, markets: [] };
  res.json({
    kuehler: section,
    mhd: section,
    durcharbeit: section,
    generatedAt: new Date().toISOString(),
    timezone: "Europe/Vienna",
    periodFallback: { startDate: "2026-10-05", endDate: "2026-10-30" },
  });
});
f.app.post("/client-telemetry", (_req, res) => res.sendStatus(204));
// Unrelated admin providers receive synthetic empty catalogs; no production fallback exists.
f.app.get(["/admin/modules", "/admin/fragebogen"], (_req, res) =>
  res.json({ modules: [], fragebogen: [] }),
);
f.app.get(
  [
    "/admin/campaigns/answer-change-requests",
    "/admin/time-change-requests",
    "/admin/campaigns/visit-session-delete-requests",
  ],
  (_req, res) => res.json({ requests: [] }),
);
f.app.get(["/markets/chains", "/admin/markets/chains"], (_req, res) =>
  res.json({ chains: [] }),
);
f.app.get("/day-session/today-submissions", (_req, res) =>
  res.json({ items: [], session: null, stats: null }),
);
f.app.get(
  ["/markets/gm/flex-start-markets", "/markets/gm/assigned-active"],
  (_req, res) => res.json({ markets: [] }),
);
f.app.get("/markets/gm/stats", (_req, res) =>
  res.json({ visited: 0, total: 0, visitedMarkets: 0, totalMarkets: 0 }),
);
f.app.post("/telemetry/events", (_req, res) => res.sendStatus(204));
f.app.get("/admin/kurti/layout", (_req, res) => res.json({ layout: null }));
f.app.put("/admin/kurti/layout", (_req, res) => res.json({ ok: true }));
f.app.get(/^\/admin\/(modules|fragebogen)(\/.*)?$/, (_req, res) =>
  res.json({ modules: [], fragebogen: [], usage: [] }),
);
f.app.get("/admin/campaigns", (_req, res) => res.json({ campaigns: [] }));
let failRead = false,
  failFile = false;
const realRead = f.service.read,
  realFile = f.service.file;
f.service.read = async (...args) => {
  if (failRead)
    throw Object.assign(new Error("Synthetic connection failure"), {
      status: 503,
    });
  return realRead(...args);
};
f.service.file = async (...args) => {
  if (failFile)
    throw Object.assign(new Error("Synthetic PDF failure"), { status: 503 });
  return realFile(...args);
};
f.app.post("/__synthetic/scenario", async (req, res) => {
  failRead = !!req.body.failRead;
  failFile = !!req.body.failFile;
  if (typeof req.body.archived === "boolean")
    await f.service.archive(ready.id, req.body.archived);
  if (req.body.newVersion) {
    const d = await f.service.get(ready.id);
    const n = await f.service.save(d.id, {
      expectedRevision: d.draftRevision,
      title: d.title,
      description: d.description + " Aktualisierte Fassung.",
      visibleFrom: d.visibleFrom,
      visibleUntil: d.visibleUntil,
      pages: d.pages,
    });
    await f.service.publish(d.id, n.draftRevision, randomUUID(), f.admin);
  }
  res.json({ ok: true });
});
f.app.use((req, res) => {
  console.info(`Unimplemented synthetic endpoint: ${req.method} ${req.path}`);
  res.status(404).json({ error: "Not provided in synthetic preview" });
});
f.app.listen(4037, "127.0.0.1", () =>
  console.info(
    "Synthetic Wochenschwerpunkte API on 4037; preview login admin@preview.invalid / gm@preview.invalid (any password)",
  ),
);
