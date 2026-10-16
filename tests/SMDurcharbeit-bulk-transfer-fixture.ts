import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "./SMDurcharbeit-fixture.js";
import { createSyntheticPhotoStorage, syntheticShelfPhoto } from "./synthetic-photo-storage.js";

/** Real routes backed only by a disposable DB and local synthetic photo storage. */
export async function createSMDurcharbeitBulkTransferFixture() {
  let now = "2026-09-15T12:00:00Z";
  class Clock extends Date { constructor(value?: string | number | Date) { super(value === undefined ? now : value instanceof Date ? value.getTime() : value); } static now() { return Date.parse(now); } }
  const second = randomUUID(), third = randomUUID(), reader = randomUUID(), normalEditor = randomUUID(), editor = randomUUID();
  const photos = createSyntheticPhotoStorage("http://127.0.0.1:4038");
  const f = await createSMDurcharbeitFixture({ clock: Clock as typeof Date, photoStorage: photos.storage,
    additionalSmUsers: [{ id: second, token: "synthetic-second-sm" }, { id: third, token: "synthetic-third-sm" }],
    customerUsers: [
      { id: reader, token: "synthetic-reader", permissions: { sm_durcharbeit_maerkte: ["read"], sm_durcharbeit_verplanung: ["read"] } },
      { id: normalEditor, token: "synthetic-normal-editor", permissions: { sm_maerkte: ["read", "update", "write"] } },
      { id: editor, token: "synthetic-bulk-editor", permissions: { sm_durcharbeit_maerkte: ["read", "update"], sm_durcharbeit_verplanung: ["read", "update"] } },
    ] });
  try {
    for (const [id, firstName, lastName] of [[f.employee, "Alex", "Sommer"], [second, "Bea", "Winter"], [third, "Cem", "Herbst"]]) await f.database.update(f.schema.users).set({ firstName, lastName }).where(eq(f.schema.users.id, id!));
    const root = "/admin/sm-smdurcharbeit-campaigns", defaultRoot = "/admin/sm-markets/SMDurcharbeit";
    const admin = (method: "get" | "post" | "patch", path: string, token = "synthetic-sm-admin") => request(f.app)[method](path).auth(token, { type: "bearer" });
    const sm = (method: "get" | "post" | "put", path: string, token = "synthetic-sm") => request(f.app)[method](path).auth(token, { type: "bearer" });
    const imported = await admin("post", `${defaultRoot}/import`).send({ fileName: "Synthetic-transfer.xlsx", sheetName: "Fixture",
      mapping: { SMDurcharbeitVertriebstyp: "A", name: "B", address: "C", postalCode: "D", city: "E", SMDurcharbeitEmEh: "F", shelfMerchandiserName: "G" },
      rows: [["Vertriebstyp", "Firma/Betrieb", "Straße", "PLZ", "Ort", "EM/EH", "Verplanung"],
        ...Array.from({ length: 50 }, (_, index) => [index % 2 ? "Billa" : "Spar", `Vorschau Markt ${String(index + 1).padStart(2, "0")}`, `Vorschauweg ${index + 1}`, "1010", "Vorschaustadt", "EM", "Alex Sommer"])] }).expect(200);
    const markets = imported.body.markets as Array<{ id: string; name: string; updatedAt: string }>;
    markets.sort((a, b) => a.name.localeCompare(b.name));
    const module = (await admin("post", "/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Vorschau · Durcharbeit", description: "Synthetischer SM-Wechsel", questions: [
      { id: "completion", type: "yesno", text: "Durcharbeit durchgeführt?", required: true, options: ["Ja", "Nein"], config: {}, rules: [] },
      { id: "photo", type: "photo", text: "Platzierung fotografieren", required: false, options: [], config: {}, rules: [] },
    ] }).expect(201)).body.module;
    const form = (await admin("post", "/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Vorschau · Durcharbeit", description: "", status: "active", moduleIds: [module.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    async function createCampaign(name: string, ids = markets.map(market => market.id), startDate = "2026-09-01", endDate = "2026-12-31") {
      const created = (await admin("post", root).send({ name, startDate, endDate, questionnaireVersionId: version!.id, rosterDraft: ids.map(smMarketId => ({ smMarketId })) }).expect(201)).body.campaign;
      const preview = (await admin("get", `${root}/${created.id}/preview`).expect(200)).body;
      await admin("post", `${root}/${created.id}/publish`).send({ expectedRevision: created.revision, previewToken: preview.previewToken, confirmOverlap: true }).expect(200);
      return (await admin("get", root).expect(200)).body.campaigns.find((row: { id: string }) => row.id === created.id);
    }
    const campaign = await createCampaign("Vorschau · Herbsttour"), otherCampaign = await createCampaign("Vorschau · Andere Kampagne");
    const targets = async (month = "2026-10-01", campaignId = campaign.id) => (await admin("get", `${root}/${campaignId}/targets?month=${month}`).expect(200)).body.targets as Array<Record<string, any>>;
    const start = async (target: Record<string, any>, token = "synthetic-sm") => (await sm("post", `/sm/smdurcharbeit/targets/${target.id}/start`, token).send({ expectedRevision: target.revision, followUp: false, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
    async function complete(target: Record<string, any>, date: string, token = "synthetic-sm") {
      const opened = await start(target, token), path = `/sm/smdurcharbeit/visits/${opened.visitId}`;
      const payload = (await sm("get", path, token).expect(200)).body;
      const question = payload.sections[0].questions[0], photo = payload.sections[0].questions[1];
      await sm("put", `${path}/answers/${question.id}`, token).send({ answer: { kind: "choice", optionCode: question.options[0].code }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
      const { answerId } = (await sm("post", `${path}/photos/initialize`, token).send({ submissionQuestionId: photo.id }).expect(200)).body;
      const { upload } = (await sm("post", `${path}/photos/presign`, token).send({ answerId, extension: "png" }).expect(200)).body;
      const bytes = syntheticShelfPhoto(true); photos.put(upload.path, bytes);
      await sm("post", `${path}/photos/commit`, token).send({ answerId, photos: [{ storageBucket: upload.bucket, storagePath: upload.path, originalFileName: "Synthetic.png", mimeType: "image/png", byteSize: bytes.length, widthPx: 640, heightPx: 480 }] }).expect(200);
      await sm("post", `${path}/submit`, token).send({ visitStartedAt: `${date}T08:00:00Z`, visitCompletedAt: `${date}T08:20:00Z`, clientMutationToken: randomUUID() }).expect(200);
      return opened;
    }
    const old = (await targets("2026-09-01")).find(row => row.market.id === markets[49]!.id)!;
    await complete(old, "2026-09-15");
    now = "2026-10-16T12:00:00Z";
    return { ...f, adminId: f.admin, second, third, reader, editor, normalEditor, photos, root, defaultRoot, markets, campaign, otherCampaign, admin, sm, targets, start, complete, createCampaign,
      setNow: (value: string) => { now = value; } };
  } catch (error) { await f.pg.close(); throw error; }
}
