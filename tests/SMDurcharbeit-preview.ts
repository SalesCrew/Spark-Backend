import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { eq } from "drizzle-orm";
import { createSMDurcharbeitFixture } from "./SMDurcharbeit-fixture.js";
import { createSyntheticPhotoStorage, syntheticShelfPhoto } from "./synthetic-photo-storage.js";
import { installSmPhotoArchivePreviewFaults } from "./sm-photo-archive-preview-faults.js";
import { spezialfragenPeriodFixture } from "./spezialfragen-period-fixture.js";

// Independent, disposable preview. Never imports the production app entry point or environment.
const photoStorage = createSyntheticPhotoStorage();
const f = await createSMDurcharbeitFixture({ photoStorage: photoStorage.storage });
const today = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Vienna", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date());
const gmFixture = await spezialfragenPeriodFixture(undefined, true);
await request(gmFixture.app).post("/admin/fragebogen/main").auth("synthetic-gm-admin", { type: "bearer" }).send({
  name: "Vorschau · Spezialfragen", status: "active", moduleIds: [], spezialfragen: [
    { id: randomUUID(), type: "yesno", text: "Ist das Premium Display noch im Markt vorhanden?", required: true, config: {}, rules: [], scoring: {} },
    { id: randomUUID(), type: "yesno", text: "Konnte die Aktivierung umgesetzt werden?", required: false, config: { spezialfragePeriod: { startDate: today, endDate: today } }, rules: [], scoring: {} },
  ],
}).expect(201);
// Reproduce the reported catalog mismatch using only disposable questionnaires and campaigns.
for (const section of ["standard", "flex", "billa", "kuehler", "mhd", "durcharbeit"] as const) {
  const scope = ["standard", "flex", "billa"].includes(section) ? "main" : section;
  const create = async (name: string, status: string) => (await request(gmFixture.app).post(`/admin/fragebogen/${scope}`).auth("synthetic-gm-admin", { type: "bearer" }).send({
    name, status, moduleIds: [], ...(scope === "main" ? { sectionKeywords: [section] } : {}),
    spezialfragen: [{ id: randomUUID(), type: "yesno", text: `${name} · Testfrage`, required: true, config: {}, rules: [], scoring: {} }],
  }).expect(201)).body.fragebogen.id;
  await create(`${section} · Q3`, "active");
  const currentFragebogenId = await create(`${section} · Q4`, "inactive");
  await gmFixture.database.insert(gmFixture.schema.campaigns).values({ name: `${section} · Oktober`, section, status: "active", scheduleType: "always", currentFragebogenId });
  const future = await create(`${section} · Geplant`, "inactive");
  const startDate = new Date(Date.now() + 7 * 86_400_000).toISOString().slice(0, 10), endDate = new Date(Date.now() + 14 * 86_400_000).toISOString().slice(0, 10);
  await gmFixture.database.insert(gmFixture.schema.campaigns).values({ name: `${section} · Nächste Woche`, section, status: "scheduled", scheduleType: "scheduled", startDate, endDate, currentFragebogenId: future });
}
const admin = (method: "post" | "put" | "patch", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
const sm = (method: "post" | "put", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" });
async function seedSMDurcharbeitQuestionnaire(scope: "standard" | "SMDurcharbeit", name: string) {
  const questions = [{ id: "new-" + randomUUID(), text: scope === "SMDurcharbeit" ? "Durcharbeit im Markt durchgeführt?" : "Standardkontrolle durchgeführt?", type: "yesno", required: true, options: ["Ja", "Nein"], config: {}, rules: [] },
    { id: "new-" + randomUUID(), text: scope === "SMDurcharbeit" ? "Durcharbeit-Platzierung fotografieren" : "Standard-Platzierung fotografieren", type: "photo", required: true, options: [], config: { instruction: "Synthetische Vorschauaufnahme" }, rules: [] }];
  const module = (await admin("post", `/admin/sm-questionnaires/modules?scope=${scope}`).send({ id: "new-" + randomUUID(), name: `${name} · Modul`, description: "Ausschließlich synthetische Vorschau", questions }).expect(201)).body.module;
  const form = (await admin("post", `/admin/sm-questionnaires/questionnaires?scope=${scope}`).send({ id: "new-" + randomUUID(), name, description: "Ausschließlich synthetische Vorschau", status: "active", moduleIds: [module.id] }).expect(201)).body.questionnaire;
  const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
  return { form, version: version! };
}
const standard = await seedSMDurcharbeitQuestionnaire("standard", "Vorschau · Standardfragebogen");
const durcharbeit = await seedSMDurcharbeitQuestionnaire("SMDurcharbeit", "Vorschau · Durcharbeit");
await admin("put", "/admin/sm-planning/questionnaire-assignment").send({ questionnaireTemplateId: standard.form.id }).expect(200);
async function seedSMDurcharbeitAssignment() {
  const [row] = await f.database.insert(f.schema.smAssignments).values({
    idempotencyKey: randomUUID(), sourceType: "single", status: "planned",
    originalWorkDate: today, originalSmUserId: f.employee, originalSmMarketId: f.market,
    originalMarketInternalId: "SYNTHETIC-1", originalPlannedMinutes: 45,
    createdByUserId: f.admin, updatedByUserId: f.admin,
  }).returning();
  return row!;
}
const old = await seedSMDurcharbeitAssignment();
const opened = (await sm("post", `/sm/visits/${old.id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }).expect(200)).body;
const q = opened.sections[0].questions[0];
await sm("put", `/sm/visits/${old.id}/answers/${q.id}`).send({ answer: { kind: "choice", optionCode: q.options[0].code }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
async function seedSyntheticVisitPhotos(assignmentId: string, questionId: string, blue: boolean, count: number) {
  const { answerId } = (await sm("post", `/sm/visits/${assignmentId}/photos/initialize`).send({ submissionQuestionId: questionId }).expect(200)).body;
  const photos = [];
  for (let index = 0; index < count; index++) {
    const { upload } = (await sm("post", `/sm/visits/${assignmentId}/photos/presign`).send({ answerId, extension: "png" }).expect(200)).body;
    const bytes = syntheticShelfPhoto(blue); photoStorage.put(upload.path, bytes);
    photos.push({ storageBucket: upload.bucket, storagePath: upload.path, originalFileName: `${blue ? "Durcharbeit" : "Standard"}_Vorschau_${index + 1}.png`, mimeType: "image/png", byteSize: bytes.length, widthPx: 640, heightPx: 480 });
  }
  await sm("post", `/sm/visits/${assignmentId}/photos/commit`).send({ answerId, photos }).expect(200);
}
await seedSyntheticVisitPhotos(old.id, opened.sections[0].questions[1].id, false, 2);
await sm("post", `/sm/visits/${old.id}/submit`).send({ actualMinutes: 15, visitStartedAt: `${today}T07:00:00Z`, visitCompletedAt: `${today}T07:15:00Z`, clientMutationToken: randomUUID() }).expect(200);
await seedSMDurcharbeitAssignment();
const target = await seedSMDurcharbeitAssignment();
await admin("patch", `/admin/sm-planning/assignments/${target.id}`).send({ expectedUpdatedAt: target.updatedAt.toISOString(), SMDurcharbeitQuestionnaireOverrideVersionId: durcharbeit.version.id }).expect(200);

const SMDurcharbeitPreviewImport = (await admin("post", "/admin/sm-markets/SMDurcharbeit/import").send({
  fileName: "Synthetische-Durcharbeit.xlsx", sheetName: "Gesamt",
  mapping: { SMDurcharbeitVertriebstyp: "A", name: "B", address: "C", postalCode: "D", city: "E", SMDurcharbeitEmEh: "F", shelfMerchandiserName: "G" },
  rows: [["Vertriebstyp", "Firma/Betrieb", "Straße", "PLZ", "Ort", "EM/EH", "Verplanung"],
    ["Spar", "Durcharbeit · Markt am Park", "Vorschauweg 8", "1020", "Wien", "EM", "SM Local"],
    ["Billa", "", "Synthetische Gasse 12", "1030", "Wien", "", "Unbekannte Vorschauperson"],
  ],
}).expect(200)).body;
const SMDurcharbeitPreviewMarketId = SMDurcharbeitPreviewImport.markets.find((row: any) => row.name === "Durcharbeit · Markt am Park").id;
await admin("post", "/admin/sm-planning/assignments").send({ smMarketId: SMDurcharbeitPreviewMarketId, smUserId: f.employee, workDate: today, plannedMinutes: 90, SMDurcharbeitQuestionnaireOverrideVersionId: durcharbeit.version.id, idempotencyKey: randomUUID() }).expect(201);
const completedId = (await admin("post", "/admin/sm-planning/assignments").send({ smMarketId: SMDurcharbeitPreviewMarketId, smUserId: f.employee, workDate: today, plannedMinutes: 75, SMDurcharbeitQuestionnaireOverrideVersionId: durcharbeit.version.id, idempotencyKey: randomUUID() }).expect(201)).body.assignmentId;
const completedPayload = (await sm("post", `/sm/visits/${completedId}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }).expect(200)).body;
const completedQuestion = completedPayload.sections[0].questions[0];
await sm("put", `/sm/visits/${completedId}/answers/${completedQuestion.id}`).send({ answer: { kind: "choice", optionCode: completedQuestion.options[0].code }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
await seedSyntheticVisitPhotos(completedId, completedPayload.sections[0].questions[1].id, true, 3);
await sm("post", `/sm/visits/${completedId}/submit`).send({ actualMinutes: 75, visitStartedAt: `${today}T11:00:00Z`, visitCompletedAt: `${today}T12:15:00Z`, clientMutationToken: randomUUID() }).expect(200);

const preview = express();
preview.use((req, res, next) => {
  const origin = req.get("origin");
  if (origin && !["http://127.0.0.1:3037", "http://localhost:3037"].includes(origin)) { res.sendStatus(403); return; }
  if (origin) res.set("Access-Control-Allow-Origin", origin);
  res.set("Access-Control-Allow-Headers", "Content-Type, Authorization, x-coke-spark-page-key");
  res.set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS");
  if (req.method === "OPTIONS") { res.sendStatus(204); return; }
  next();
});
preview.use(express.json());
preview.use("/synthetic-photo-storage", photoStorage.router);
const people = {
  "synthetic-gm-admin": { id: gmFixture.ids.admin, role: "admin", email: "gm-admin@preview.test", firstName: "Vorschau", lastName: "GM Admin", isActive: true },
  "synthetic-gm": { id: gmFixture.ids.gm, role: "gm", email: "gm@preview.test", firstName: "Vorschau", lastName: "GM", isActive: true },
  "synthetic-sm-admin": { id: f.admin, role: "sm_admin", email: "sm-admin@preview.test", firstName: "Vorschau", lastName: "Admin", isActive: true },
  "synthetic-sm": { id: f.employee, role: "sm", email: "sm@preview.test", firstName: "Vorschau", lastName: "SM", isActive: true, travelTimeEnabled: true },
};
const tokenFor = (req: express.Request) => req.get("authorization")?.replace(/^Bearer /, "") as keyof typeof people;
const sessionFor = (token: keyof typeof people) => ({ user: people[token], session: { accessToken: token, refreshToken: token, expiresAt: Math.floor(Date.now() / 1000) + 86_400 } });
preview.post("/auth/login", (req, res) => {
  const token = Object.keys(people).find(key => people[key as keyof typeof people].email === req.body.email) as keyof typeof people | undefined;
  if (!token || req.body.password !== "preview") { res.status(401).json({ error: "Vorschauzugang: sm-admin@preview.test oder sm@preview.test · Passwort preview" }); return; }
  res.json(sessionFor(token));
});
preview.post("/auth/refresh", (req, res) => {
  const token = req.body.refreshToken as keyof typeof people;
  if (!people[token]) { res.sendStatus(401); return; }
  res.json(sessionFor(token));
});
preview.get("/auth/me", (req, res) => {
  const user = people[tokenFor(req)];
  if (!user) { res.sendStatus(401); return; }
  res.json({ user });
});
preview.get("/employee-agreement/current", (req, res) => {
  if (!people[tokenFor(req)]) { res.sendStatus(401); return; }
  res.json({ accepted: true });
});
preview.get("/admin/users", (req, res) => {
  if (!["synthetic-sm-admin", "synthetic-gm-admin"].includes(tokenFor(req))) { res.sendStatus(403); return; }
  res.json({ users: req.query.role === "sm" ? [people["synthetic-sm"]] : [people["synthetic-gm"]] });
});
preview.get("/sm/messages", (req, res) => {
  if (tokenFor(req) !== "synthetic-sm") { res.sendStatus(403); return; }
  res.json({ messages: [] });
});
let SMDurcharbeitPreviewKurtiLayout: Record<string, unknown> | null = null;
preview.get("/admin/kurti/layout", (req, res) => {
  if (tokenFor(req) !== "synthetic-sm-admin") { res.sendStatus(403); return; }
  res.json({ layout: SMDurcharbeitPreviewKurtiLayout });
});
preview.put("/admin/kurti/layout", (req, res) => {
  if (tokenFor(req) !== "synthetic-sm-admin") { res.sendStatus(403); return; }
  SMDurcharbeitPreviewKurtiLayout = { ...req.body, updatedAt: new Date().toISOString() };
  res.json({ layout: SMDurcharbeitPreviewKurtiLayout });
});
installSmPhotoArchivePreviewFaults(preview);
preview.use(f.app);
preview.use(gmFixture.app);
const server = preview.listen(4037, "127.0.0.1", () => console.log("SMDurcharbeit synthetic backend ready at http://127.0.0.1:4037 · disposable PGlite · no production environment"));
for (const signal of ["SIGINT", "SIGTERM"] as const) process.once(signal, () => server.close(() => { void Promise.all([f.pg.close(), gmFixture.pg.close()]).then(() => process.exit(0)); }));
