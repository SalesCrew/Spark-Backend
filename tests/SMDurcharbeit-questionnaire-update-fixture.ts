import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "./SMDurcharbeit-fixture.js";

/** Disposable real-route setup. No environment files, application entrypoint or cloud storage. */
export async function createQuestionnaireUpdateFixture() {
  const now = Date.parse("2026-10-19T10:00:00Z");
  class Clock extends Date { constructor(value?: string | number | Date) { super(value === undefined ? now : value instanceof Date ? value.getTime() : value); } static now() { return now; } }
  const objects = new Set<string>(), reader = randomUUID(), editor = randomUUID(), wrongEditor = randomUUID();
  const f = await createSMDurcharbeitFixture({ clock: Clock as typeof Date,
    customerUsers: [{ id: reader, token: "synthetic-reader", permissions: { sm_durcharbeit_verplanung: ["read"] } },
      { id: editor, token: "synthetic-editor", permissions: { sm_durcharbeit_verplanung: ["read", "update"] } },
      { id: wrongEditor, token: "synthetic-normal-editor", permissions: { sm_verplanung: ["read", "update"] } }],
    photoStorage: { from: () => ({
      createSignedUploadUrl: async (path: string) => ({ data: { path, signedUrl: `https://synthetic.invalid/${path}`, token: "synthetic" }, error: null }),
      createSignedUrl: async (path: string) => ({ data: { signedUrl: `https://synthetic.invalid/${path}` }, error: null }),
      createSignedUrls: async (paths: string[]) => ({ data: paths.map(path => ({ path, signedUrl: `https://synthetic.invalid/${path}` })), error: null }),
      list: async (folder: string, options: { search: string }) => ({ data: objects.has(`${folder}/${options.search}`) ? [{ name: options.search }] : [], error: null }),
      remove: async () => { throw new Error("Existing synthetic photos must never be removed by a questionnaire update"); },
    }) } });
  try {
    const admin = (method: "get" | "post" | "patch", path: string, token = "synthetic-sm-admin") => request(f.app)[method](path).auth(token, { type: "bearer" });
    const sm = (method: "get" | "post" | "put", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" });
    const root = "/admin/sm-smdurcharbeit-campaigns", employeeRoot = "/sm/smdurcharbeit/targets", visitRoot = "/sm/smdurcharbeit/visits";
    const marketIds = [f.market, randomUUID(), randomUUID()];
    await f.database.insert(f.schema.smMarkets).values(marketIds.slice(1).map((id, index) => ({ id, internalMarketId: `SYNTHETIC-UPDATE-${index}`, name: `Vorschau Markt ${index + 2}`, chain: "Spar", address: "Vorschauweg 1", postalCode: "1010", city: "Wien", region: "Ost" })));
    await f.database.insert(f.schema.smSMDurcharbeitMarkets).values(marketIds.map(smMarketId => ({ smMarketId, SMDurcharbeitSmUserId: f.employee })));
    const moduleRoot = "/admin/sm-questionnaires/modules?scope=SMDurcharbeit", formRoot = "/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit";
    const question = (text: string, type = "yesno") => ({ id: `new-${randomUUID()}`, text, type, required: false, options: ["Ja", "Nein"], config: {}, rules: [] });
    const originalModule = (await admin("post", moduleRoot).send({ id: `new-${randomUUID()}`, name: "SPAR Durcharbeit · Vorschau", description: "Synthetische Fragen", questions: [question("Bestand geprüft"), question("Notiz", "text"), question("Regalfoto", "photo"), ...[1, 2, 3, 4].map(index => question(`Entfernte Frage ${index}`))] }).expect(201)).body.module;
    const form = (await admin("post", formRoot).send({ id: `new-${randomUUID()}`, name: "SPAR DA · Tour 1", description: "Vorschau", status: "active", moduleIds: [originalModule.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    async function publish(name: string) {
      const campaign = (await admin("post", root).send({ name, startDate: "2026-10-01", endDate: "2026-11-30", questionnaireVersionId: version!.id, rosterDraft: marketIds.map(smMarketId => ({ smMarketId })) }).expect(201)).body.campaign;
      const preview = (await admin("get", `${root}/${campaign.id}/preview`).expect(200)).body;
      await admin("post", `${root}/${campaign.id}/publish`).send({ expectedRevision: campaign.revision, previewToken: preview.previewToken, confirmOverlap: true }).expect(200);
      return (await admin("get", root).expect(200)).body.campaigns.find((row: { id: string }) => row.id === campaign.id);
    }
    const campaign = await publish("SPAR DA Tour 1 · Vorschau"), otherCampaign = await publish("Weitere Tour · unverändert");
    const list = async () => (await sm("get", `${employeeRoot}?month=2026-10-01`).expect(200)).body.targets;
    const start = async (targetId: string, followUp = false) => {
      const target = (await list()).find((row: { id: string }) => row.id === targetId);
      return (await sm("post", `${employeeRoot}/${targetId}/start`).send({ expectedRevision: target.revision, followUp, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
    };
    const targets = (await list()).filter((row: { campaignId: string }) => row.campaignId === campaign.id);
    const completed = await start(targets[0].id), draft = await start(targets[1].id);
    const payload = (await sm("get", `${visitRoot}/${completed.visitId}`).expect(200)).body;
    let photoId = "";
    for (const q of payload.sections.flatMap((section: { questions: unknown[] }) => section.questions)) {
      if (q.type === "photo") {
        const initialized = (await sm("post", `${visitRoot}/${completed.visitId}/photos/initialize`).send({ submissionQuestionId: q.id }).expect(200)).body;
        const upload = (await sm("post", `${visitRoot}/${completed.visitId}/photos/presign`).send({ answerId: initialized.answerId, extension: "png" }).expect(200)).body.upload;
        objects.add(upload.path);
        photoId = (await sm("post", `${visitRoot}/${completed.visitId}/photos/commit`).send({ answerId: initialized.answerId, photos: [{ storageBucket: upload.bucket, storagePath: upload.path, mimeType: "image/png", byteSize: 10 }] }).expect(200)).body.fileIds[0];
      } else await sm("put", `${visitRoot}/${completed.visitId}/answers/${q.id}`).send({ answer: q.type === "text" ? { kind: "text", value: "Original note" } : { kind: "choice", optionCode: q.options[0].code }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
    }
    await sm("post", `${visitRoot}/${completed.visitId}/submit`).send({ visitStartedAt: "2026-10-19T08:00:00Z", visitCompletedAt: "2026-10-19T08:20:00Z", clientMutationToken: randomUUID() }).expect(200);
    let latestModule = originalModule;
    const editModule = async (again = false) => {
      latestModule = (await admin("patch", `/admin/sm-questionnaires/modules/${originalModule.id}?scope=SMDurcharbeit`).send({ ...originalModule,
        questions: [...originalModule.questions.filter((q: { text: string }) => !q.text.startsWith("Entfernte Frage")).map((q: { text: string }) => q.text === "Notiz" ? { ...q, text: again ? "Erneut aktualisierte Notiz" : "Aktualisierte Notiz" } : q), question("Neue Kontrollfrage")] }).expect(200)).body.module;
      return latestModule;
    };
    await editModule();
    return { ...f, adminId: f.admin, admin, sm, root, employeeRoot, visitRoot, originalModule, form, version: version!, campaign, otherCampaign, targets, completed, draft, photoId, objects, list, start, editModule };
  } catch (error) { await f.pg.close(); throw error; }
}
