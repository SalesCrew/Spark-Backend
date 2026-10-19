import { test } from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import { randomUUID } from "node:crypto";
import {
  createBriefingFixture,
  syntheticBriefingPdf,
} from "../tests/gm-briefing-fixture.js";
const admin = { Authorization: "Bearer synthetic-admin" },
  gm = { Authorization: "Bearer synthetic-gm" };
test("PDF -> draft -> immutable publication -> GM page receipts, permissions and new-version badge", async () => {
  const f = await createBriefingFixture();
  try {
    const pdf = await syntheticBriefingPdf(3),
      operation = randomUUID();
    const upload = () =>
      request(f.app)
        .post("/admin/gm-briefings/upload")
        .set(admin)
        .set("Content-Type", "application/pdf")
        .set("x-operation-id", operation)
        .set("x-pdf-filename", encodeURIComponent("Woche 43.pdf"))
        .send(pdf);
    const first = await upload();
    assert.equal(first.status, 200);
    const draft = first.body;
    assert.equal(draft.file.pageCount, 3);
    assert.equal(draft.pages.length, 3);
    assert.equal((await upload()).body.id, draft.id);
    assert.equal(f.objects.size, 1);
    assert.equal(
      (await request(f.app).get("/gm/briefings").set(gm)).body.unreadCount,
      0,
    );
    const input = {
      expectedRevision: 1,
      title: "Woche 43",
      description: "Unser Fokus",
      visibleFrom: "2026-10-19",
      visibleUntil: "2026-10-25",
      pages: draft.pages.map((p: any) => ({
        ...p,
        description: "Bitte im Markt beachten.",
      })),
    };
    const saved = await request(f.app)
      .patch(`/admin/gm-briefings/${draft.id}`)
      .set(admin)
      .send(input);
    assert.equal(saved.status, 200);
    assert.equal(saved.body.draftRevision, 2);
    assert.equal(
      (
        await request(f.app)
          .patch(`/admin/gm-briefings/${draft.id}`)
          .set(admin)
          .send(input)
      ).status,
      409,
    );
    const body = { expectedRevision: 2, operationId: randomUUID() };
    const published = await request(f.app)
      .post(`/admin/gm-briefings/${draft.id}/publish`)
      .set(admin)
      .send(body);
    assert.equal(published.status, 200);
    const id = published.body.id;
    assert.equal(
      (
        await request(f.app)
          .post(`/admin/gm-briefings/${draft.id}/publish`)
          .set(admin)
          .send(body)
      ).body.id,
      id,
    );
    let inbox = await request(f.app).get("/gm/briefings").set(gm);
    assert.equal(inbox.body.unreadCount, 1);
    assert.equal(inbox.body.items[0].description, "Unser Fokus");
    assert.equal(inbox.body.items[0].file, undefined);
    assert.equal(
      (await request(f.app).get(`/gm/briefings/${id}/file`).set(gm)).status,
      200,
    );
    for (const pageNumber of [1, 1, 2])
      assert.equal(
        (
          await request(f.app)
            .post(`/gm/briefings/${id}/read`)
            .set(gm)
            .send({ pageNumber })
        ).status,
        200,
      );
    assert.equal(
      (await request(f.app).get("/gm/briefings").set(gm)).body.unreadCount,
      1,
    );
    await request(f.app)
      .post(`/gm/briefings/${id}/read`)
      .set(gm)
      .send({ pageNumber: 3 })
      .expect(200);
    inbox = await request(f.app).get("/gm/briefings").set(gm);
    assert.equal(inbox.body.unreadCount, 0);
    assert.deepEqual(inbox.body.items[0].seenPages, [1, 2, 3]);
    assert.equal(
      (
        await request(f.app)
          .get("/gm/briefings")
          .set("Authorization", "Bearer synthetic-gm2")
      ).body.unreadCount,
      1,
    );
    assert.equal(
      (
        await request(f.app)
          .post(`/gm/briefings/${id}/read`)
          .set(gm)
          .send({ pageNumber: 4 })
      ).status,
      400,
    );
    assert.equal(
      (
        await request(f.app)
          .get("/gm/briefings")
          .set("Authorization", "Bearer synthetic-sm")
      ).status,
      403,
    );
    assert.equal(
      (
        await request(f.app)
          .get("/admin/gm-briefings")
          .set("Authorization", "Bearer synthetic-gm")
      ).status,
      403,
    );
    assert.equal(
      (
        await request(f.app)
          .get("/admin/gm-briefings")
          .set("Authorization", "Bearer synthetic-kunde")
      ).status,
      200,
    );
    assert.equal(
      (
        await request(f.app)
          .post(`/admin/gm-briefings/${draft.id}/publish`)
          .set("Authorization", "Bearer synthetic-kunde")
          .send(body)
      ).status,
      403,
    );
    await assert.rejects(
      () =>
        f.pg.query(
          "update gm_weekly_briefing_publications set version=9 where id=$1",
          [id],
        ),
      /immutable/,
    );
    const next = await f.service.save(draft.id, {
      ...input,
      expectedRevision: 2,
      title: "Neue Fassung",
    });
    assert.equal((await f.service.inbox(f.gm)).items[0]?.title, "Woche 43");
    const v2 = await f.service.publish(
      draft.id,
      next.draftRevision,
      randomUUID(),
      f.admin,
    );
    assert.notEqual(v2.id, id);
    assert.equal((await f.service.inbox(f.gm)).unreadCount, 1);
    assert.equal(f.objects.size, 1);
    await f.service.archive(draft.id, true);
    assert.equal((await f.service.inbox(f.gm)).items.length, 0);
    await assert.rejects(() => f.service.file(id));
    assert.equal(
      (await f.pg.query("select * from gm_weekly_briefing_reads")).rows.length,
      1,
    );
  } finally {
    await f.close();
  }
});
test("future versions, Vienna inclusive dates, expiration without fallback, replacements and upload failure", async () => {
  const f = await createBriefingFixture();
  try {
    const draft = await f.service.upload(
      await syntheticBriefingPdf(2),
      "Plan.pdf",
      f.admin,
      randomUUID(),
    );
    const initial = {
      expectedRevision: 1,
      title: "Heute",
      description: "",
      visibleFrom: "2026-10-19",
      visibleUntil: null,
      pages: draft.pages,
    };
    const d = await f.service.save(draft.id, initial);
    const p1 = await f.service.publish(
      d.id,
      d.draftRevision,
      randomUUID(),
      f.admin,
    );
    const future = await f.service.save(d.id, {
      ...initial,
      expectedRevision: d.draftRevision,
      title: "Morgen",
      visibleFrom: "2026-10-20",
      visibleUntil: "2026-10-21",
    });
    const p2 = await f.service.publish(
      d.id,
      future.draftRevision,
      randomUUID(),
      f.admin,
    );
    assert.equal((await f.service.inbox(f.gm)).items[0]?.id, p1.id);
    f.setNow("2026-10-19T22:01:00Z");
    assert.equal((await f.service.inbox(f.gm)).items[0]?.id, p2.id);
    f.setNow("2026-10-21T21:59:00Z");
    assert.equal((await f.service.inbox(f.gm)).items.length, 1);
    f.setNow("2026-10-21T22:01:00Z");
    assert.equal((await f.service.inbox(f.gm)).items.length, 0);
    const replacement = await f.service.upload(
      await syntheticBriefingPdf(1),
      "Neu.pdf",
      f.admin,
      randomUUID(),
      { id: d.id, expectedRevision: future.draftRevision },
    );
    assert.equal(replacement.pages.length, 1);
    assert.equal(f.objects.size, 2);
    assert.equal(
      (
        await f.pg.query<any>(
          "select snapshot from gm_weekly_briefing_publications where id=$1",
          [p1.id],
        )
      ).rows[0].snapshot.file.pageCount,
      2,
    );
    f.failUploads(true);
    await assert.rejects(async () =>
      f.service.upload(
        await syntheticBriefingPdf(1),
        "Fehler.pdf",
        f.admin,
        randomUUID(),
      ),
    );
    f.failUploads(false);
    assert.equal((await f.service.list()).length, 1);
    await request(f.app)
      .post("/admin/gm-briefings/upload")
      .set(admin)
      .set("Content-Type", "application/pdf")
      .set("x-operation-id", randomUUID())
      .send(Buffer.from("not a PDF"))
      .expect(400);
  } finally {
    await f.close();
  }
});
test("validation rejects invalid dates, page loss, oversized documents and unauthorized access", async () => {
  const f = await createBriefingFixture();
  try {
    const draft = await f.service.upload(
      await syntheticBriefingPdf(1),
      "One.pdf",
      f.admin,
      randomUUID(),
    );
    const input = {
      expectedRevision: 1,
      title: "One",
      description: "",
      visibleFrom: "2026-02-30",
      visibleUntil: null,
      pages: draft.pages,
    };
    await request(f.app)
      .patch(`/admin/gm-briefings/${draft.id}`)
      .set(admin)
      .send(input)
      .expect(400);
    await request(f.app)
      .patch(`/admin/gm-briefings/${draft.id}`)
      .set(admin)
      .send({ ...input, visibleFrom: null, pages: [] })
      .expect(400);
    await request(f.app).get("/gm/briefings").expect(403);
    await request(f.app)
      .post("/admin/gm-briefings/upload")
      .set("Authorization", "Bearer synthetic-sm")
      .expect(403);
    await request(f.app)
      .get("/admin/gm-briefings")
      .set("Authorization", "Bearer synthetic-sm_admin")
      .expect(200);
    await request(f.app)
      .post("/admin/gm-briefings/upload")
      .set(admin)
      .set("Content-Type", "application/pdf")
      .set("x-operation-id", randomUUID())
      .send(await syntheticBriefingPdf(81))
      .expect(400);
    await request(f.app)
      .post("/admin/gm-briefings/upload")
      .set(admin)
      .set("Content-Type", "application/pdf")
      .set("x-operation-id", randomUUID())
      .send(Buffer.alloc(25 * 1024 * 1024 + 1))
      .expect(413);
    assert.equal((await f.service.list()).length, 1);
    assert.equal(f.objects.size, 1);
  } finally {
    await f.close();
  }
});
test("new tables and private PDFs stay inaccessible to worker/anonymous SQL roles even with broad storage policies", async () => {
  const f = await createBriefingFixture();
  try {
    await f.pg.exec(
      `grant usage on schema storage to authenticated; grant select on storage.objects to authenticated; create policy synthetic_existing_broad_policy on storage.objects for select to authenticated using (true); insert into storage.objects(id,bucket_id) values(gen_random_uuid(),'gm-weekly-briefings'),(gen_random_uuid(),'other-bucket'); set role authenticated;`,
    );
    const visible = await f.pg.query<{ bucket_id: string }>(
      "select bucket_id from storage.objects",
    );
    assert.deepEqual(
      visible.rows.map((row) => row.bucket_id),
      ["other-bucket"],
    );
    await assert.rejects(
      () => f.pg.query("select * from gm_weekly_briefings"),
      /permission denied/,
    );
    await f.pg.exec("reset role");
  } finally {
    await f.close();
  }
});
