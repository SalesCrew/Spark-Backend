import { createHash, randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "./SMDurcharbeit-fixture.js";
import { createSyntheticPhotoStorage, syntheticShelfPhoto } from "./synthetic-photo-storage.js";

export async function createOptionalCommentsFixture(dated = false) {
  class Clock extends Date { constructor(value?: string | number | Date) { super(value === undefined ? "2026-11-20T12:00:00Z" : value instanceof Date ? value.getTime() : value); } static now() { return Date.parse("2026-11-20T12:00:00Z"); } }
  const storage = createSyntheticPhotoStorage();
  const f = await createSMDurcharbeitFixture({ clock: Clock as typeof Date, photoStorage: storage.storage });
  try {
    f.app.use("/synthetic-photo-storage", storage.router);
    const admin = (method: "get" | "post", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
    const sm = (method: "get" | "post" | "put" | "delete", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" });
    const q = (text: string, type: string, config: Record<string, unknown> = {}, required = false) => ({ id: `new-${randomUUID()}`, text, type, config, required, options: ["Ja", "Nein"], rules: [] });
    const module = (await admin("post", "/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Durcharbeit · Kommentare", description: "Synthetischer Ablauf",
      questions: [q("Coke 200ml CAN im Kühler vorhanden?", "yesnomulti", { answers: ["Ja", "Nein"], branches: [{ answer: "Nein", options: ["Nicht vorgesehen", "Ware bestellt"] }] }, true),
        q("Auswahl", "single", { options: ["Ja", "Nein"] }), q("Mehrfachauswahl", "multiple", { options: ["Ja", "Nein"] }),
        q("Skala", "likert", { min: 1, max: 5 }), q("Notiz zum Markt", "text"), q("Anzahl", "numeric"), q("Auslastung", "slider", { min: 0, max: 100, step: 5 }),
        q("Regalfoto", "photo"), q("Matrix", "matrix", { rows: ["Regal"], columns: ["Ja", "Nein"] }),
        q("Unbeantwortete optionale Frage", "yesno"), q("Pflichtkommentar bei Nein", "yesno", { commentTrigger: { mode: "options", optionCodes: ["option_2"] } }, true)] }).expect(201)).body.module;
    const form = (await admin("post", "/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Durcharbeit · Kommentar-Vorschau", description: "Synthetische Daten", status: "active", moduleIds: [module.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    const secondMarket = randomUUID();
    await f.database.insert(f.schema.smMarkets).values({ id: secondMarket, internalMarketId: "SYNTHETIC-COMMENT", name: "Vorschau Markt", chain: "Spar", address: "Testgasse 2", postalCode: "1010", city: "Wien", region: "Ost", assignedSmUserId: f.employee });
    await f.database.insert(f.schema.smSMDurcharbeitMarkets).values([f.market, secondMarket].map(smMarketId => ({ smMarketId, SMDurcharbeitSmUserId: f.employee })));
    const root = "/admin/sm-smdurcharbeit-campaigns";
    const campaign = (await admin("post", root).send({ name: "Durcharbeit · Kommentare", startDate: "2026-11-01", endDate: "2026-11-30", questionnaireVersionId: version!.id, rosterDraft: [f.market, secondMarket].map(smMarketId => ({ smMarketId })) }).expect(201)).body.campaign;
    const preview = (await admin("get", `${root}/${campaign.id}/preview`).expect(200)).body;
    await admin("post", `${root}/${campaign.id}/publish`).send({ expectedRevision: campaign.revision, previewToken: preview.previewToken }).expect(200);
    const targets = async () => (await sm("get", "/sm/smdurcharbeit/targets").expect(200)).body.targets;
    const start = async (marketId: string, followUp = false) => {
      const target = (await targets()).find((row: any) => row.market.id === marketId);
      return (await sm("post", `/sm/smdurcharbeit/targets/${target.id}/start`).send({ expectedRevision: target.revision, mode: "manual", followUp, clientSubmissionToken: randomUUID() }).expect(201)).body;
    };
    const readPath = async (path: string) => (await sm("get", path).expect(200)).body;
    const saveAt = async (path: string, questionId: string, answer: unknown, expectedVersion?: number, responseCode = 200) => {
      const expected = expectedVersion ?? (await readPath(path)).answerVersions[questionId];
      return sm("put", `${path}/answers/${questionId}`).send({ answer, expectedAnswerVersion: expected, clientMutationToken: randomUUID() }).expect(responseCode);
    };
    const uploadAt = async (path: string, questionId: string) => {
      const answerId = (await sm("post", `${path}/photos/initialize`).send({ submissionQuestionId: questionId }).expect(200)).body.answerId;
      const upload = (await sm("post", `${path}/photos/presign`).send({ answerId, extension: "png" }).expect(200)).body.upload;
      const bytes = syntheticShelfPhoto(true); storage.put(upload.path, bytes);
      const result = (await sm("post", `${path}/photos/commit`).send({ answerId, photos: [{ storageBucket: upload.bucket, storagePath: upload.path, mimeType: "image/png", byteSize: bytes.length, originalFileName: "Synthetisches Regalfoto.png" }] }).expect(200)).body;
      return { id: result.fileIds[0] as string, path: upload.path as string, bytes };
    };
    const historical = await start(f.market), historicalPath = `/sm/smdurcharbeit/visits/${historical.visitId}`;
    const oldPayload = await readPath(historicalPath), oldQs = oldPayload.sections[0].questions;
    for (const question of oldQs.filter((q: any) => q.required)) await saveAt(historicalPath, question.id, question.type === "yesnomulti" ? { kind: "yesnomulti", optionCode: question.options[0].code, subOptions: [] } : { kind: "choice", optionCode: question.options[0].code });
    const historicalPhoto = await uploadAt(historicalPath, oldQs.find((q: any) => q.type === "photo").id);
    await sm("post", `${historicalPath}/submit`).send({ visitStartedAt: "2026-11-20T08:00:00Z", visitCompletedAt: "2026-11-20T08:15:00Z", clientMutationToken: randomUUID() }).expect(200);
    const tables = [f.schema.smQuestionnaireSubmissions, f.schema.smQuestionnaireSubmissionQuestions, f.schema.smQuestionnaireSubmissionSections, f.schema.smQuestionAnswers,
      f.schema.smQuestionAnswerOptions, f.schema.smQuestionAnswerMatrixCells, f.schema.smQuestionAnswerFiles, f.schema.smQuestionAnswerEvents, f.schema.smSMDurcharbeitVisits, f.schema.smQuestionnaireVersions];
    const protectedIds = await Promise.all(tables.map(async table => new Set((await f.database.select().from(table as any)).map((row: any) => row.id))));
    const fingerprint = async () => createHash("sha256").update(JSON.stringify(await Promise.all(tables.map(async (table, index) =>
      (await f.database.select().from(table as any)).filter((row: any) => protectedIds[index]!.has(row.id)).sort((a: any, b: any) => a.id.localeCompare(b.id)))))).digest("hex");
    const originalFingerprint = await fingerprint();
    let visit: { visitId: string; submissionId?: string };
    if (dated) {
      const [assignment] = await f.database.insert(f.schema.smAssignments).values({ idempotencyKey: randomUUID(), sourceType: "single", status: "planned", originalWorkDate: "2026-11-20", originalSmUserId: f.employee,
        originalSmMarketId: secondMarket, originalMarketInternalId: "SYNTHETIC-COMMENT", originalPlannedMinutes: 30, SMDurcharbeitQuestionnaireOverrideVersionId: version!.id, createdByUserId: f.admin, updatedByUserId: f.admin }).returning();
      const payload = (await sm("post", `/sm/visits/${assignment!.id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }).expect(200)).body;
      visit = { visitId: assignment!.id, submissionId: payload.submission.id };
    } else visit = await start(secondMarket);
    const path = dated ? `/sm/visits/${visit.visitId}` : `/sm/smdurcharbeit/visits/${visit.visitId}`;
    const read = () => readPath(path);
    const payload = await read();
    return { ...f, adminId: f.admin, admin, sm, storage, campaign, visit, historicalPath, historicalPhoto, path, read, payload, secondMarket, start, saveAt,
      save: (id: string, value: unknown, expected?: number, responseCode?: number) => saveAt(path, id, value, expected, responseCode), upload: (id: string) => uploadAt(path, id),
      fingerprint, originalFingerprint };
  } catch (error) { await f.pg.close(); throw error; }
}
