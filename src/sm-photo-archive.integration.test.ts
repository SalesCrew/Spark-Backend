import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import request from "supertest";
import { eq } from "drizzle-orm";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";
import JSZip from "jszip";
import { createRequire } from "node:module";
const { exportSmArchivePhotos } = createRequire(import.meta.url)("../../src/lib/exports/smPhotoArchiveExport.ts") as typeof import("../../src/lib/exports/smPhotoArchiveExport.js");
import type { SmPhotoArchiveApi, SmPhotoArchiveFilters } from "../../src/types/smPhotoArchive.js";

test("SM Fotoarchiv: actual photo submission, filters, private reads and unchanged history", async t => {
  const uploaded = new Set<string>(), signCalls: string[][] = [];
  let storageFails = false, storageThrows = false, storageHangs = false, storagePartial = false;
  const f = await createSMDurcharbeitFixture({ photoStorage: { from: () => ({
    createSignedUploadUrl: async (path: string) => ({ data: { path, token: "synthetic-upload", signedUrl: `http://storage.invalid/${path}` }, error: null }),
    list: async (folder: string) => ({ data: [...uploaded].filter(path => path.startsWith(folder + "/")).map(path => ({ name: path.slice(folder.length + 1) })), error: null }),
    createSignedUrl: async (path: string) => ({ data: { signedUrl: `http://storage.invalid/${path}` }, error: null }),
    createSignedUrls: async (paths: string[]) => {
      signCalls.push(paths);
      if (storageThrows) throw new Error("Synthetic storage exception");
      if (storageHangs) return new Promise<never>(() => {});
      return storageFails ? { data: null, error: { message: "Synthetic storage unavailable" } } : { data: paths.map((path, index) => ({ path, signedUrl: storagePartial && index === 0 ? null : `http://storage.invalid/${path}`, error: null })), error: null };
    },
  }) } });
  const admin = (method: "get" | "post" | "put" | "patch", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
  const sm = (method: "post" | "put", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" });
  try {
    const seed = async (scope: "standard" | "SMDurcharbeit", hour: string, name: string, photoCount: number) => {
      const mod = (await admin("post", `/admin/sm-questionnaires/modules?scope=${scope}`).send({ id: "new-" + randomUUID(), name: name + " Modul", description: "Synthetic only",
        questions: [{ id: "new-" + randomUUID(), type: "photo", text: name + " Fotofrage", required: true, config: { instruction: "Synthetic photo" }, options: [], rules: [] }] }).expect(201)).body.module;
      const form = (await admin("post", `/admin/sm-questionnaires/questionnaires?scope=${scope}`).send({ id: "new-" + randomUUID(), name, status: "active", moduleIds: [mod.id] }).expect(201)).body.questionnaire;
      const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
      if (scope === "standard") await admin("put", "/admin/sm-planning/questionnaire-assignment").send({ questionnaireTemplateId: form.id }).expect(200);
      const assignment = await f.assignment();
      if (scope === "SMDurcharbeit") await admin("patch", `/admin/sm-planning/assignments/${assignment.id}`).send({ expectedUpdatedAt: assignment.updatedAt.toISOString(), SMDurcharbeitQuestionnaireOverrideVersionId: version!.id }).expect(200);
      const started = (await sm("post", `/sm/visits/${assignment.id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }).expect(200)).body;
      const question = started.sections[0].questions[0];
      const { answerId } = (await sm("post", `/sm/visits/${assignment.id}/photos/initialize`).send({ submissionQuestionId: question.id }).expect(200)).body;
      const photos = [];
      for (let index = 0; index < photoCount; index++) {
        const { upload } = (await sm("post", `/sm/visits/${assignment.id}/photos/presign`).send({ answerId, extension: "png" }).expect(200)).body;
        uploaded.add(upload.path);
        photos.push({ storageBucket: upload.bucket, storagePath: upload.path, originalFileName: name + ` ${index + 1}.png`, mimeType: "image/png", byteSize: 32, widthPx: 320, heightPx: 240 });
      }
      await sm("post", `/sm/visits/${assignment.id}/photos/commit`).send({ answerId, photos }).expect(200);
      await sm("post", `/sm/visits/${assignment.id}/submit`).send({ actualMinutes: 15, visitStartedAt: `2026-10-07T${hour}:00:00Z`, visitCompletedAt: `2026-10-07T${hour}:15:00Z`, clientMutationToken: randomUUID() }).expect(r => assert.equal(r.status, 200, JSON.stringify(r.body)));
      return { form, version, assignment, question, answerId };
    };
    const standard = await seed("standard", "07", "Standard original", 2);
    const durcharbeit = await seed("SMDurcharbeit", "08", "Durcharbeit original", 1);

    await t.test("lists each current photo once, with visit snapshots and no storage secrets", async () => {
      const result = (await admin("get", "/admin/sm-photos").expect(200)).body;
      assert.equal(result.total, 3); assert.equal(result.photos.length, 3);
      assert.deepEqual(result.stats, { markets: 1, questionnaires: 2 });
      assert.equal(result.photos[0].marketName, "Synthetic Billa");
      assert.ok(result.photos.every((photo: any) => photo.workDate === "2026-10-07" && photo.questionId && photo.submissionId));
      assert.ok(result.photos.every((photo: any) => !("storagePath" in photo) && !("storageBucket" in photo) && !("signedUrl" in photo)));
    });
    await t.test("Standard and Durcharbeit are classified by questionnaire, including ordinary-market overrides", async () => {
      assert.equal((await admin("get", "/admin/sm-photos?SMDurcharbeitCatalogScope=standard").expect(200)).body.total, 2);
      const da = (await admin("get", "/admin/sm-photos?SMDurcharbeitCatalogScope=SMDurcharbeit").expect(200)).body;
      assert.equal(da.total, 1); assert.equal(da.photos[0].questionnaireId, durcharbeit.form.id);
      assert.equal((await admin("get", `/admin/sm-photos?smUserId=${f.employee}&marketId=${f.market}&questionnaireId=${standard.form.id}`).expect(200)).body.total, 2);
      assert.equal((await admin("get", "/admin/sm-photos?search=Durcharbeit").expect(200)).body.total, 1);
      assert.equal((await admin("get", "/admin/sm-photos?from=2026-10-08").expect(200)).body.total, 0);
      assert.equal((await admin("get", "/admin/sm-photos?to=2026-10-06").expect(200)).body.total, 0);
    });
    await t.test("pagination is deterministic, facets match scope, and exports use the same filters", async () => {
      const first = (await admin("get", "/admin/sm-photos?pageSize=1&page=1").expect(200)).body;
      const second = (await admin("get", "/admin/sm-photos?pageSize=1&page=2").expect(200)).body;
      assert.notEqual(first.photos[0].id, second.photos[0].id); assert.equal(second.total, 3);
      const facets = (await admin("get", "/admin/sm-photos/facets?SMDurcharbeitCatalogScope=SMDurcharbeit").expect(200)).body;
      assert.equal(facets.facets.length, 1); assert.equal(facets.facets[0].questionnaireId, durcharbeit.form.id);
      const exported = (await admin("get", "/admin/sm-photos/export?SMDurcharbeitCatalogScope=standard").expect(200)).body.photos;
      assert.equal(exported.length, 2); assert.ok(exported.every((photo: any) => photo.SMDurcharbeitCatalogScope === "standard" && !photo.storagePath));
    });
    await t.test("real saved answers flow through archive/export/signing into an intact filtered ZIP", async () => {
      const query = (filters: SmPhotoArchiveFilters) => new URLSearchParams(Object.entries(filters).filter(([, value]) => value !== undefined) as [string, string][]).toString();
      const api: SmPhotoArchiveApi = {
        list: async filters => (await admin("get", "/admin/sm-photos?" + query(filters)).expect(200)).body,
        facets: async filters => (await admin("get", "/admin/sm-photos/facets?" + query(filters)).expect(200)).body,
        export: async filters => (await admin("get", "/admin/sm-photos/export?" + query(filters)).expect(200)).body,
        urls: async ids => (await admin("post", "/admin/sm-photos/signed-urls").send({ ids }).expect(200)).body,
      };
      for (const scope of [undefined, "standard", "SMDurcharbeit"] as const) {
        let captured: Blob | null = null;
        const filters = scope ? { SMDurcharbeitCatalogScope: scope } : {};
        const listed = await api.list(filters), result = await exportSmArchivePhotos({ api, filters, isCurrent: () => true, onProgress() {}, save: blob => { captured = blob; },
          fetchPhoto: async url => new Response(new Uint8Array([1, 2, 3, 4]), { status: uploaded.has(new URL(String(url)).pathname.slice(1)) ? 200 : 404 }) });
        const zip = await JSZip.loadAsync(await captured!.arrayBuffer());
        const originals = Object.values(zip.files).filter(file => !file.dir && file.name !== "Fotoliste.csv");
        assert.equal(result.count, listed.total); assert.equal(originals.length, listed.total);
        for (const file of originals) assert.deepEqual(await file.async("uint8array"), new Uint8Array([1, 2, 3, 4]));
        const manifest = await zip.file("Fotoliste.csv")!.async("string");
        for (const item of listed.photos) { assert.ok(manifest.includes(item.id)); assert.ok(originals.some(file => file.name.includes(item.id))); }
        if (scope) assert.ok(originals.every(file => file.name.startsWith(scope === "standard" ? "Standardfragebogen/" : "Durcharbeit/")));
      }
    });
    await t.test("later catalog and market edits preserve the captured names and photo identity", async () => {
      await f.database.update(f.schema.smMarkets).set({ name: "Renamed current market", isActive: false }).where(eq(f.schema.smMarkets.id, f.market));
      await admin("patch", `/admin/sm-questionnaires/questionnaires/${standard.form.id}?scope=standard`).send({ ...standard.form, name: "Renamed current questionnaire" }).expect(200);
      const rows = (await admin("get", "/admin/sm-photos").expect(200)).body.photos;
      assert.ok(rows.every((row: any) => row.marketName === "Synthetic Billa"));
      assert.ok(rows.filter((row: any) => row.SMDurcharbeitCatalogScope === "standard").every((row: any) => row.questionnaireName === "Standard original"));
      // Work date is the actual Vienna visit date, even around UTC midnight.
      const [answer] = await f.database.select().from(f.schema.smQuestionAnswers).where(eq(f.schema.smQuestionAnswers.id, durcharbeit.answerId));
      await f.database.update(f.schema.smQuestionnaireSubmissions).set({ visitStartedAt: new Date("2026-10-06T22:30:00Z") }).where(eq(f.schema.smQuestionnaireSubmissions.id, answer!.submissionId));
      assert.equal((await admin("get", "/admin/sm-photos?from=2026-10-07&to=2026-10-07&SMDurcharbeitCatalogScope=SMDurcharbeit").expect(200)).body.total, 1);
    });
    await t.test("admin-only access and ID-only signing cannot expose arbitrary storage objects", async () => {
      await request(f.app).get("/admin/sm-photos").expect(401);
      await request(f.app).get("/admin/sm-photos").auth("synthetic-sm", { type: "bearer" }).expect(403);
      await request(f.app).post("/admin/sm-photos/signed-urls").auth("synthetic-sm", { type: "bearer" }).send({ ids: [randomUUID()] }).expect(403);
      const photo = (await admin("get", "/admin/sm-photos").expect(200)).body.photos[0];
      const signed = await admin("post", "/admin/sm-photos/signed-urls").send({ ids: [photo.id, photo.id, randomUUID()] }).expect(200);
      assert.equal(signed.body.photos.length, 1); assert.ok(signed.body.photos[0].signedUrl); assert.ok(signed.body.photos[0].expiresAt);
      assert.equal(signCalls.at(-1)?.length, 1);
      await admin("post", "/admin/sm-photos/signed-urls").send({ ids: [photo.id], storagePath: "anything" }).expect(400);
      await admin("get", "/admin/sm-photos?from=2026-02-30").expect(400);
      await admin("get", "/admin/sm-photos?from=2026-10-08&to=2026-10-07").expect(400);
      await admin("get", "/admin/sm-photos?pageSize=999").expect(400);
      await admin("get", "/admin/sm-photos?SMDurcharbeitCatalogScope=anything").expect(400);
      assert.equal(signed.headers["cache-control"], "private, no-store");
      storageFails = true;
      const failure = (await admin("post", "/admin/sm-photos/signed-urls").send({ ids: [photo.id] }).expect(200)).body;
      assert.equal(failure.photos[0].signedUrl, null); storageFails = false;
    });
    await t.test("every endpoint enforces SM admin scope and validates all query/body boundaries", async () => {
      for (const path of ["/admin/sm-photos", "/admin/sm-photos/facets", "/admin/sm-photos/export", "/admin/sm-photos/signed-urls"]) {
        const method = path.endsWith("signed-urls") ? "post" : "get";
        for (const token of [undefined, "synthetic-gm-admin", "synthetic-gm", "synthetic-sm"]) {
          let call = request(f.app)[method](path);
          if (token) call = call.auth(token, { type: "bearer" });
          await call.send(method === "post" ? { ids: [randomUUID()] } : undefined).expect(token ? 403 : 401);
        }
        await request(f.app)[method](path).auth("synthetic-admin", { type: "bearer" }).send(method === "post" ? { ids: [randomUUID()] } : undefined).expect(200);
      }
      for (const query of ["page=0", "page=-1", "page=1.5", "pageSize=0", "pageSize=61", "smUserId=bad", "marketId=bad", "questionnaireId=bad", "search=" + "x".repeat(201), "unexpected=1", "from=invalid", "to=2026-02-29"]) {
        await admin("get", "/admin/sm-photos?" + query).expect(400);
      }
      for (const body of [{ ids: [] }, { ids: ["bad"] }, { ids: Array.from({ length: 61 }, () => randomUUID()) }, {}]) {
        await admin("post", "/admin/sm-photos/signed-urls").send(body).expect(400);
      }
      const literal = (await admin("get", "/admin/sm-photos?search=" + encodeURIComponent("%' OR 1=1 --")).expect(200)).body;
      assert.equal(literal.total, 0);
      const empty = (await admin("get", `/admin/sm-photos?marketId=${randomUUID()}`).expect(200)).body;
      assert.equal(empty.total, 0); assert.deepEqual(empty.stats, { markets: 0, questionnaires: 0 });
      assert.equal((await admin("get", "/admin/sm-photos?page=99999").expect(200)).body.photos.length, 0);
      for (const path of ["/admin/sm-photos", "/admin/sm-photos/facets", "/admin/sm-photos/export"]) assert.equal((await admin("get", path).expect(200)).headers["cache-control"], "private, no-store");
    });
    await t.test("storage exceptions, partial responses and timeouts preserve metadata and recover", async () => {
      const ids = (await admin("get", "/admin/sm-photos").expect(200)).body.photos.map((item: any) => item.id);
      storageThrows = true;
      assert.ok((await admin("post", "/admin/sm-photos/signed-urls").send({ ids }).expect(200)).body.photos.every((item: any) => item.signedUrl === null));
      storageThrows = false; storagePartial = true;
      const partial = (await admin("post", "/admin/sm-photos/signed-urls").send({ ids }).expect(200)).body.photos;
      assert.equal(partial.filter((item: any) => item.signedUrl === null).length, 1); assert.equal(partial.length, 3);
      storagePartial = false; storageHangs = true;
      const began = Date.now();
      const timeout = (await admin("post", "/admin/sm-photos/signed-urls").send({ ids }).expect(200)).body.photos;
      assert.ok(Date.now() - began < 10_000); assert.ok(timeout.every((item: any) => item.signedUrl === null));
      storageHangs = false;
      const recovered = (await admin("post", "/admin/sm-photos/signed-urls").send({ ids }).expect(200)).body.photos;
      assert.ok(recovered.every((item: any) => item.signedUrl && Date.parse(item.expiresAt) > Date.now() + 590_000 && Date.parse(item.expiresAt) <= Date.now() + 600_000));
      assert.equal((await admin("get", "/admin/sm-photos").expect(200)).body.total, 3);
    });
    await t.test("soft deletion at every snapshot layer prevents both listing and signing", async () => {
      const [answer] = await f.database.select().from(f.schema.smQuestionAnswers).where(eq(f.schema.smQuestionAnswers.id, durcharbeit.answerId));
      const [question] = await f.database.select().from(f.schema.smQuestionnaireSubmissionQuestions).where(eq(f.schema.smQuestionnaireSubmissionQuestions.id, answer!.submissionQuestionId));
      const photo = (await admin("get", "/admin/sm-photos?SMDurcharbeitCatalogScope=SMDurcharbeit").expect(200)).body.photos[0];
      const layers = [
        [f.schema.smQuestionnaireSubmissions, answer!.submissionId, { isCurrent: false }, { isCurrent: true }],
        [f.schema.smQuestionnaireSubmissions, answer!.submissionId, { isDeleted: true, deletedAt: new Date() }, { isDeleted: false, deletedAt: null }],
        [f.schema.smQuestionnaireSubmissionSections, question!.submissionSectionId, { isDeleted: true, deletedAt: new Date() }, { isDeleted: false, deletedAt: null }],
        [f.schema.smQuestionnaireSubmissionQuestions, question!.id, { isDeleted: true, deletedAt: new Date() }, { isDeleted: false, deletedAt: null }],
        [f.schema.smQuestionAnswers, answer!.id, { isDeleted: true, deletedAt: new Date() }, { isDeleted: false, deletedAt: null }],
        [f.schema.smQuestionAnswers, answer!.id, { answerState: "invalidated", invalidatedAt: new Date(), invalidationReason: "Synthetic invalidation" }, { answerState: "answered", invalidatedAt: null, invalidationReason: null }],
        [f.schema.smQuestionAnswers, answer!.id, { valueJson: { kind: "photo", fileIds: "invalid-array" } }, { valueJson: answer!.valueJson }],
      ] as const;
      for (const [table, id, changed, restored] of layers) {
        await f.database.update(table as any).set(changed).where(eq(table.id, id));
        assert.equal((await admin("get", "/admin/sm-photos?SMDurcharbeitCatalogScope=SMDurcharbeit").expect(200)).body.total, 0);
        assert.equal((await admin("post", "/admin/sm-photos/signed-urls").send({ ids: [photo.id] }).expect(200)).body.photos.length, 0);
        await f.database.update(table as any).set(restored).where(eq(table.id, id));
      }
      // Staff lifecycle changes cannot remove submitted snapshot photos.
      await f.database.update(f.schema.users).set({ isActive: false, firstName: "Changed", deletedAt: new Date() }).where(eq(f.schema.users.id, f.employee));
      const retained = (await admin("get", "/admin/sm-photos").expect(200)).body.photos;
      assert.equal(retained.length, 3); assert.ok(retained.every((item: any) => item.smName === "Local SM"));
      await f.database.update(f.schema.users).set({ isActive: true, firstName: "Local", deletedAt: null }).where(eq(f.schema.users.id, f.employee));
    });
    await t.test("large selections paginate without duplicates and enforce the exact 250-photo export boundary", async () => {
      await f.database.update(f.schema.smMarkets).set({ isActive: true }).where(eq(f.schema.smMarkets.id, f.market));
      // Every extra photo is committed through real visit routes (20/question limit).
      const extras = [];
      for (let index = 0; index < 13; index++) extras.push(await seed("standard", String(index + 10).padStart(2, "0"), `Load ${index}`, index === 12 ? 8 : 20));
      assert.equal((await admin("get", "/admin/sm-photos").expect(200)).body.total, 251);
      assert.equal((await admin("get", "/admin/sm-photos/export").expect(400)).body.code, "sm_photo_archive_export_too_large");
      const allIds = new Set<string>();
      for (let page = 1; page <= 9; page++) {
        const result = (await admin("get", `/admin/sm-photos?page=${page}`).expect(200)).body;
        assert.equal(result.total, 251); assert.equal(result.photos.length, page === 9 ? 11 : 30);
        for (const item of result.photos) { assert.ok(!allIds.has(item.id)); allIds.add(item.id); }
      }
      assert.equal(allIds.size, 251);
      const signed = (await admin("post", "/admin/sm-photos/signed-urls").send({ ids: [...allIds].slice(0, 60) }).expect(200)).body.photos;
      assert.equal(signed.length, 60);
      const [file] = await f.database.select().from(f.schema.smQuestionAnswerFiles).where(eq(f.schema.smQuestionAnswerFiles.answerId, extras[0]!.answerId));
      await f.database.update(f.schema.smQuestionAnswerFiles).set({ isDeleted: true, deletedAt: new Date() }).where(eq(f.schema.smQuestionAnswerFiles.id, file!.id));
      const exported = (await admin("get", "/admin/sm-photos/export").expect(200)).body.photos;
      assert.equal(exported.length, 250); assert.equal(new Set(exported.map((item: any) => item.id)).size, 250);
      assert.ok(!exported.some((item: any) => item.id === file!.id));
      // Isolate later history checks while retaining every synthetic record.
      for (const item of extras) {
        const [answer] = await f.database.select().from(f.schema.smQuestionAnswers).where(eq(f.schema.smQuestionAnswers.id, item.answerId));
        await f.database.update(f.schema.smQuestionnaireSubmissions).set({ isDeleted: true, deletedAt: new Date() }).where(eq(f.schema.smQuestionnaireSubmissions.id, answer!.submissionId));
      }
      assert.equal((await admin("get", "/admin/sm-photos").expect(200)).body.total, 3);
      await f.database.update(f.schema.smMarkets).set({ isActive: false }).where(eq(f.schema.smMarkets.id, f.market));
    });
    await t.test("corrections supersede photos without deleting originals; draft/deleted/hidden photos are excluded", async () => {
      const [old] = await f.database.select().from(f.schema.smQuestionAnswers).where(eq(f.schema.smQuestionAnswers.id, standard.answerId));
      const files = await f.database.select().from(f.schema.smQuestionAnswerFiles).where(eq(f.schema.smQuestionAnswerFiles.answerId, old!.id));
      await f.database.update(f.schema.smQuestionAnswers).set({ isCurrent: false }).where(eq(f.schema.smQuestionAnswers.id, old!.id));
      const [replacement] = await f.database.insert(f.schema.smQuestionAnswers).values({ submissionId: old!.submissionId, submissionQuestionId: old!.submissionQuestionId,
        supersedesAnswerId: old!.id, answerVersion: old!.answerVersion + 1, isCurrent: true, answerState: "answered", answeredAt: new Date(), valueJson: { kind: "photo", fileIds: [] } }).returning();
      const [replacementFile] = await f.database.insert(f.schema.smQuestionAnswerFiles).values({ answerId: replacement!.id, storageBucket: files[0]!.storageBucket, storagePath: files[0]!.storagePath, originalFileName: "Current.png", byteSize: 32 }).returning();
      await f.database.update(f.schema.smQuestionAnswers).set({ valueJson: { kind: "photo", fileIds: [replacementFile!.id] } }).where(eq(f.schema.smQuestionAnswers.id, replacement!.id));
      assert.equal((await admin("get", "/admin/sm-photos").expect(200)).body.total, 2);
      assert.equal((await admin("post", "/admin/sm-photos/signed-urls").send({ ids: files.map(file => file.id) }).expect(200)).body.photos.length, 0);
      assert.equal((await f.database.select().from(f.schema.smQuestionAnswerFiles).where(eq(f.schema.smQuestionAnswerFiles.answerId, old!.id))).length, 2);
      // A staged photo that is not linked by the current answer must never be counted.
      await f.database.insert(f.schema.smQuestionAnswerFiles).values({ answerId: replacement!.id, storageBucket: "sm-visit-photos", storagePath: "synthetic/staged.png" });
      assert.equal((await admin("get", "/admin/sm-photos").expect(200)).body.total, 2);
      await f.database.update(f.schema.smQuestionnaireSubmissions).set({ status: "draft" }).where(eq(f.schema.smQuestionnaireSubmissions.id, old!.submissionId));
      assert.equal((await admin("get", "/admin/sm-photos").expect(200)).body.total, 1);
      await f.database.update(f.schema.smQuestionnaireSubmissionQuestions).set({ isApplicable: false, applicabilityReason: "Synthetic conditional rule" }).where(eq(f.schema.smQuestionnaireSubmissionQuestions.id, durcharbeit.question.id));
      assert.equal((await admin("get", "/admin/sm-photos").expect(200)).body.total, 0);
    });
    await t.test("all archive endpoints are read-only, including signing and export", async () => {
      const [old] = await f.database.select().from(f.schema.smQuestionAnswers).where(eq(f.schema.smQuestionAnswers.id, standard.answerId));
      await f.database.update(f.schema.smQuestionnaireSubmissions).set({ status: "submitted" }).where(eq(f.schema.smQuestionnaireSubmissions.id, old!.submissionId));
      await f.database.update(f.schema.smQuestionnaireSubmissionQuestions).set({ isApplicable: true, applicabilityReason: null }).where(eq(f.schema.smQuestionnaireSubmissionQuestions.id, durcharbeit.question.id));
      const snapshot = async () => {
        const { rows } = await f.pg.query<{ table_name: string }>("select table_name from information_schema.tables where table_schema='public' and table_type='BASE TABLE' order by table_name");
        return JSON.stringify(await Promise.all(rows.map(async ({ table_name: table }) => [table, (await f.pg.query(`select jsonb_agg(to_jsonb(t) order by to_jsonb(t)::text) from "${table}" t`)).rows])));
      };
      const before = await snapshot();
      const rows = (await admin("get", "/admin/sm-photos").expect(200)).body.photos;
      assert.equal(rows.length, 2);
      await admin("get", "/admin/sm-photos/facets").expect(200); await admin("get", "/admin/sm-photos/export").expect(200);
      await admin("post", "/admin/sm-photos/signed-urls").send({ ids: rows.map((row: any) => row.id) }).expect(200);
      const after = await snapshot();
      assert.equal(after, before);
    });
    await t.test("deleted files are excluded and oversized exports fail clearly", async () => {
      const [photo] = (await admin("get", "/admin/sm-photos?SMDurcharbeitCatalogScope=SMDurcharbeit").expect(200)).body.photos;
      await f.database.update(f.schema.smQuestionAnswerFiles).set({ byteSize: 200 * 1024 * 1024 }).where(eq(f.schema.smQuestionAnswerFiles.id, photo.id));
      assert.equal((await admin("get", "/admin/sm-photos/export").expect(400)).body.code, "sm_photo_archive_export_too_large");
      await f.database.update(f.schema.smQuestionAnswerFiles).set({ byteSize: 32, isDeleted: true, deletedAt: new Date() }).where(eq(f.schema.smQuestionAnswerFiles.id, photo.id));
      assert.equal((await admin("get", "/admin/sm-photos?SMDurcharbeitCatalogScope=SMDurcharbeit").expect(200)).body.total, 0);
      assert.equal((await admin("post", "/admin/sm-photos/signed-urls").send({ ids: [photo.id] }).expect(200)).body.photos.length, 0);
    });
  } finally { await f.pg.close(); }
});
