import { test } from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import { randomUUID } from "node:crypto";
import {
  createBriefingFixture,
  syntheticBriefingPdf,
} from "../tests/gm-briefing-fixture.js";
const admin = { Authorization: "Bearer synthetic-admin" },
  gm = { Authorization: "Bearer synthetic-gm" },
  gm2 = { Authorization: "Bearer synthetic-gm2" };
type Fixture = Awaited<ReturnType<typeof createBriefingFixture>>;
async function publish(
  f: Fixture,
  title: string,
  from = "2026-11-02",
  until = "2026-11-09",
) {
  const uploaded = await request(f.app)
    .post("/admin/gm-action-briefings/upload")
    .set(admin)
    .set("Content-Type", "application/pdf")
    .set("x-operation-id", randomUUID())
    .set("x-pdf-filename", encodeURIComponent(title + ".pdf"))
    .send(await syntheticBriefingPdf(1))
    .expect(200);
  const saved = await request(f.app)
    .patch("/admin/gm-action-briefings/" + uploaded.body.id)
    .set(admin)
    .send({
      expectedRevision: uploaded.body.draftRevision,
      title,
      description: "",
      visibleFrom: from,
      visibleUntil: until,
      pages: uploaded.body.pages,
    })
    .expect(200);
  const publication = await request(f.app)
    .post("/admin/gm-action-briefings/" + saved.body.id + "/publish")
    .set(admin)
    .send({
      expectedRevision: saved.body.draftRevision,
      operationId: randomUUID(),
    })
    .expect(200);
  return { draft: saved.body, publication: publication.body };
}
test("multiple overlapping named PDFs are visible to every GM without assignments; reads are per GM and WSP remains separate", async () => {
  const f = await createBriefingFixture();
  try {
    f.setNow("2026-11-02T12:00:00Z");
    const a = await publish(f, "SPAR Briefing"),
      b = await publish(f, "Aktionsinformationen");
    for (const auth of [gm, gm2]) {
      const inbox = await request(f.app)
        .get("/gm/action-briefings")
        .set(auth)
        .expect(200);
      assert.deepEqual(inbox.body.items.map((i: any) => i.title).sort(), [
        "Aktionsinformationen",
        "SPAR Briefing",
      ]);
      assert.equal(inbox.body.unreadCount, 2);
      assert.ok(inbox.body.items.every((i: any) => i.file === undefined));
    }
    assert.equal(
      (await request(f.app).get("/gm/briefings").set(gm)).body.items.length,
      0,
    );
    await request(f.app)
      .get("/gm/action-briefings/" + a.publication.id + "/file")
      .set(gm)
      .expect(200);
    await request(f.app)
      .post("/gm/action-briefings/" + a.publication.id + "/read")
      .set(gm)
      .send({ pageNumber: 1 })
      .expect(200);
    assert.equal((await f.actionService.inbox(f.gm)).unreadCount, 1);
    assert.equal((await f.actionService.inbox(f.gm2)).unreadCount, 2);
    const weekly = await f.service.upload(
      await syntheticBriefingPdf(1),
      "WSP.pdf",
      f.admin,
      randomUUID(),
    );
    const weeklySnapshot = await f.service.publish(
      weekly.id,
      weekly.draftRevision,
      randomUUID(),
      f.admin,
    );
    await request(f.app)
      .get("/gm/action-briefings/" + weeklySnapshot.id + "/file")
      .set(gm)
      .expect(404);
    await request(f.app)
      .get("/gm/briefings/" + a.publication.id + "/file")
      .set(gm)
      .expect(404);
    await request(f.app)
      .get("/admin/gm-action-briefings/" + weekly.id + "/preview")
      .set(admin)
      .expect(404);
    await request(f.app)
      .get("/admin/gm-briefings/" + a.draft.id + "/preview")
      .set(admin)
      .expect(404);
    assert.equal((await f.service.inbox(f.gm)).items[0]!.id, weeklySnapshot.id);
    assert.equal((await f.actionService.inbox(f.gm)).items.length, 2);
    assert.equal(
      (await f.pg.query("select * from gm_weekly_briefing_publications")).rows
        .length,
      1,
    );
    assert.equal(
      (await f.pg.query("select * from gm_action_briefing_publications")).rows
        .length,
      2,
    );
  } finally {
    await f.close();
  }
});
test("complete inclusive Vienna windows control inbox and files, with no older-version revival after expiry", async () => {
  const f = await createBriefingFixture();
  try {
    f.setNow("2026-11-01T22:59:59Z");
    const a = await publish(f, "Montag", "2026-11-02", "2026-11-02");
    assert.equal((await f.actionService.inbox(f.gm)).items.length, 0);
    await request(f.app)
      .get("/gm/action-briefings/" + a.publication.id + "/file")
      .set(gm)
      .expect(404);
    f.setNow("2026-11-01T23:00:00Z");
    assert.equal((await f.actionService.inbox(f.gm)).items.length, 1);
    f.setNow("2026-11-02T22:59:59Z");
    assert.equal((await f.actionService.inbox(f.gm)).items.length, 1);
    f.setNow("2026-11-02T23:00:00Z");
    assert.equal((await f.actionService.inbox(f.gm)).items.length, 0);
    await request(f.app)
      .get("/gm/action-briefings/" + a.publication.id + "/file")
      .set(gm)
      .expect(404);
    const draft = await f.actionService.upload(
      await syntheticBriefingPdf(1),
      "Ohne Zeitraum.pdf",
      f.admin,
      randomUUID(),
    );
    await assert.rejects(
      () =>
        f.actionService.publish(
          draft.id,
          draft.draftRevision,
          randomUUID(),
          f.admin,
        ),
      /Anzeigezeitraum/,
    );
    await request(f.app)
      .patch("/admin/gm-action-briefings/" + draft.id)
      .set(admin)
      .send({
        expectedRevision: 1,
        title: "Ungültig",
        description: "",
        visibleFrom: "2026-11-05",
        visibleUntil: "2026-11-04",
        pages: draft.pages,
      })
      .expect(400);
  } finally {
    await f.close();
  }
});
test("archive, edits and PDF replacement preserve all historical publications, files and reading receipts", async () => {
  const f = await createBriefingFixture();
  try {
    f.setNow("2026-11-02T12:00:00Z");
    const a = await publish(f, "Erste Fassung");
    await f.actionService.read(a.publication.id, f.gm, 1);
    const oldSnapshot = (
      await f.pg.query(
        "select snapshot from gm_action_briefing_publications where id=$1",
        [a.publication.id],
      )
    ).rows[0]!.snapshot;
    const originalObjects = [...f.objects.keys()];
    const replaced = await f.actionService.upload(
      await syntheticBriefingPdf(2),
      "Neue PDF.pdf",
      f.admin,
      randomUUID(),
      { id: a.draft.id, expectedRevision: a.draft.draftRevision },
    );
    const updated = await f.actionService.save(replaced.id, {
      expectedRevision: replaced.draftRevision,
      title: "Zweite Fassung",
      description: "",
      visibleFrom: "2026-11-02",
      visibleUntil: "2026-11-09",
      pages: replaced.pages,
    });
    assert.equal(
      (await f.actionService.inbox(f.gm)).items[0]!.title,
      "Erste Fassung",
    );
    const second = await f.actionService.publish(
      updated.id,
      updated.draftRevision,
      randomUUID(),
      f.admin,
    );
    assert.notEqual(second.id, a.publication.id);
    assert.equal((await f.actionService.inbox(f.gm)).unreadCount, 1);
    assert.ok(originalObjects.every((k) => f.objects.has(k)));
    await f.actionService.archive(a.draft.id, true);
    assert.equal((await f.actionService.inbox(f.gm)).items.length, 0);
    await f.actionService.archive(a.draft.id, false);
    assert.equal((await f.actionService.inbox(f.gm)).items[0]!.id, second.id);
    assert.deepEqual(
      (
        await f.pg.query(
          "select snapshot from gm_action_briefing_publications where id=$1",
          [a.publication.id],
        )
      ).rows[0]!.snapshot,
      oldSnapshot,
    );
    assert.equal(
      (
        await f.pg.query(
          "select * from gm_action_briefing_reads where publication_id=$1",
          [a.publication.id],
        )
      ).rows.length,
      1,
    );
    await assert.rejects(
      () =>
        f.pg.query("delete from gm_action_briefing_publications where id=$1", [
          a.publication.id,
        ]),
      /immutable/,
    );
  } finally {
    await f.close();
  }
});
test("new admin page permissions are independent of WSP; worker roles and direct database/storage access stay restricted", async () => {
  const f = await createBriefingFixture();
  try {
    const customer = { Authorization: "Bearer synthetic-kunde" };
    await request(f.app)
      .get("/admin/gm-action-briefings")
      .set(customer)
      .expect(403);
    await f.pg.query(
      "update kunde_users set page_permissions=$1 where user_id=$2",
      [JSON.stringify({ aktionsbriefings: ["read"] }), f.kunde],
    );
    await request(f.app)
      .get("/admin/gm-action-briefings")
      .set(customer)
      .expect(200);
    await request(f.app).get("/admin/gm-briefings").set(customer).expect(403);
    await request(f.app)
      .post("/admin/gm-action-briefings/upload")
      .set(customer)
      .expect(403);
    for (const token of ["synthetic-gm", "synthetic-gm2", "synthetic-sm"])
      await request(f.app)
        .get("/admin/gm-action-briefings")
        .set("Authorization", "Bearer " + token)
        .expect(403);
    await request(f.app)
      .get("/gm/action-briefings")
      .set("Authorization", "Bearer synthetic-sm")
      .expect(403);
    const tables = (
      await f.pg.query<{ relname: string; relrowsecurity: boolean }>(
        "select relname,relrowsecurity from pg_class where relname in ('gm_action_briefings','gm_action_briefing_publications','gm_action_briefing_reads')",
      )
    ).rows;
    assert.equal(tables.length, 3);
    assert.ok(tables.every((t) => t.relrowsecurity));
    await f.pg.exec("set role authenticated");
    await assert.rejects(
      () => f.pg.query("select * from gm_action_briefings"),
      /permission denied/,
    );
    await f.pg.exec("reset role");
    const buckets = (
      await f.pg.query("select * from storage.buckets order by id")
    ).rows;
    assert.equal(buckets.length, 2);
    assert.ok(buckets.every((b) => !b.public));
    assert.equal(
      buckets.find((b) => b.id === "gm-action-briefings")!.file_size_limit,
      524288000,
    );
  } finally {
    await f.close();
  }
});
