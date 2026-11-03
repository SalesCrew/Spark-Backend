// Disposable browser/API fixture. Never loads environment files or an app entry point.
import express from "express";
import request from "supertest";
import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { spezialfragenPeriodFixture } from "./spezialfragen-period-fixture.js";

const f = await spezialfragenPeriodFixture(new Date("2026-10-20T10:00:00Z"), false, true);
const admin = (path: string) => request(f.app).post(path).auth("synthetic-gm-admin", { type: "bearer" });
const modules = [];
for (const name of ["Distributionsziel Q4/2026", "Monthly questions"]) {
  modules.push((await admin("/admin/modules/main").send({ name, questions: [{ type: "numeric", text: name, required: false, config: {}, rules: [], scoring: {} }] }).expect(201)).body.module);
}
const form = (await admin("/admin/fragebogen/main").send({ name: "Synthetic Standard", status: "active", moduleIds: modules.map(m => m.id) }).expect(201)).body.fragebogen;
const campaign = randomUUID();
await f.database.insert(f.schema.campaigns).values({ id: campaign, name: "Synthetic campaign", section: "standard", status: "active", scheduleType: "always", currentFragebogenId: form.id });
await f.database.insert(f.schema.campaignMarketAssignments).values({ campaignId: campaign, marketId: f.ids.market, gmUserId: f.ids.gm });
const selection = { marketId: f.ids.market, campaignIds: [campaign] };
const source = (await request(f.app).post("/gm/visit-sessions").auth("synthetic-gm", { type: "bearer" }).send(selection).expect(201)).body;
await request(f.app).patch(`/gm/visit-sessions/${source.session.id}/answers`).auth("synthetic-gm", { type: "bearer" }).send({ visitQuestionId: source.sections[0].questions[0].id, answer: 3 }).expect(200);
await f.database.update(f.schema.visitSessions).set({ status: "submitted", submittedAt: new Date("2026-10-20T10:30:00Z") }).where(eq(f.schema.visitSessions.id, source.session.id));
f.setTime("2026-11-03T10:00:00Z");
const draft = (await request(f.app).post("/gm/visit-sessions").auth("synthetic-gm", { type: "bearer" }).send(selection).expect(201)).body;
await f.database.delete(f.schema.visitAnswers).where(eq(f.schema.visitAnswers.visitSessionId, draft.session.id));
const app = express();
app.use((_req, res, next) => { res.setHeader("Access-Control-Allow-Origin", "http://127.0.0.1:3063"); res.setHeader("Access-Control-Allow-Headers", "authorization,content-type"); res.setHeader("Access-Control-Allow-Methods", "GET,POST,PATCH,OPTIONS"); next(); });
app.options(/.*/, (_req, res) => res.sendStatus(204));
app.get("/__fixture", (_req, res) => res.json({ form, modules, draftId: draft.session.id, gmId: f.ids.gm, adminId: f.ids.admin }));
app.use("/markets", f.app);
app.use(f.app);
app.listen(4063, "127.0.0.1", () => console.log("Disposable synthetic browser API ready on 4063"));
