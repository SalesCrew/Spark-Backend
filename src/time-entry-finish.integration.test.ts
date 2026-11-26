import { test } from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import { randomUUID } from "node:crypto";
import { createTimeEntryPhotoFixture, syntheticPhoto } from "../tests/time-entry-photo-fixture.js";
const gm = { Authorization: "Bearer synthetic-gm" };

test("one unavailable preview does not hide other photos or remove metadata", async () => {
  const f=await createTimeEntryPhotoFixture();
  try {
    const ids=[randomUUID(),randomUUID()],base=`/time-tracking/entries/${f.ids.entry}/photos`;
    for(const id of ids)await request(f.app).post(base).set(gm).set("Content-Type","image/png").set("x-operation-id",id).send(syntheticPhoto()).expect(200);
    f.failSigning(true,ids[0]);
    const result=(await request(f.app).get(base).set(gm).expect(200)).body;
    assert.equal(result.photos.length,2);
    assert.equal(result.photos.find((row:{id:string})=>row.id===ids[0]).url,null);
    assert.ok(result.photos.find((row:{id:string})=>row.id===ids[1]).url);
    assert.equal(f.objects.size,2);
  } finally {await f.close();}
});

test("photo-preview outage keeps committed photos and permits finishing the real time entry", async () => {
  const f = await createTimeEntryPhotoFixture(undefined, { realTimeline: true });
  try {
    const base = `/time-tracking/entries/${f.ids.entry}`, id = randomUUID();
    await f.pg.query("update time_tracking_entries set start_at=now()-interval '3 hours 48 seconds',end_at=now() where id=$1", [f.ids.entry]);
    const history = await f.pg.query("select * from legacy_answers union all select * from legacy_photos");
    await request(f.app).post(`${base}/photos`).set(gm).set("Content-Type", "image/png").set("x-operation-id", id).send(syntheticPhoto()).expect(200);
    await request(f.app).patch(`${base}/photos/${id}`).set(gm).send({ expectedRevision: 1, tagIds: [f.ids.tag] }).expect(200);
    f.failSigning();
    const metadata = await request(f.app).get(base).set(gm).expect(200);
    assert.equal(metadata.body.entry.status, "draft");
    const photos = (await request(f.app).get(`${base}/photos`).set(gm).expect(200)).body;
    assert.equal(photos.photos.length, 1); assert.equal(photos.photos[0].url, null);
    assert.match(photos.photos[0].previewError, /Vorschau/); assert.equal(photos.photos[0].tags[0].label, "Display");
    await request(f.app).patch(`${base}/draft/comment`).set(gm).send({ comment: "Synthetischer dreistündiger Einsatz" }).expect(200);
    const saved = await request(f.app).post(`${base}/submit`).set(gm).send({}).expect(200);
    assert.equal(saved.body.entry.status, "submitted"); assert.equal(saved.body.entry.durationMin, 180);
    const events = await f.pg.query("select * from time_tracking_entry_events where entry_id=$1", [f.ids.entry]);
    await request(f.app).post(`${base}/submit`).set(gm).send({}).expect(200);
    assert.deepEqual((await f.pg.query("select * from time_tracking_entry_events where entry_id=$1", [f.ids.entry])).rows, events.rows);
    assert.deepEqual((await f.pg.query("select * from legacy_answers union all select * from legacy_photos")).rows, history.rows);
    assert.equal(f.objects.size, 1);
    f.failSigning(false);
    assert.match((await request(f.app).get(`${base}/photos`).set(gm).expect(200)).body.photos[0].url, /^http:\/\/127\.0\.0\.1:/);
  } finally { await f.close(); }
});

test("a committed upload is acknowledged even when preview signing fails; real upload failures stay retryable", async () => {
  const f = await createTimeEntryPhotoFixture(undefined, { realTimeline: true });
  try {
    const base = `/time-tracking/entries/${f.ids.entry}`, id = randomUUID();
    const upload = () => request(f.app).post(`${base}/photos`).set(gm).set("Content-Type", "image/png").set("x-operation-id", id).send(syntheticPhoto());
    f.failSigning(); f.failAfterPutOnce(); await upload().expect(503);
    assert.equal((await request(f.app).get(base).set(gm)).body.entry.status, "draft");
    assert.equal((await upload().expect(200)).body.photos[0].id, id);
    await upload().expect(200); assert.equal(f.objects.size, 1);
    assert.equal((await f.pg.query("select count(*) from time_tracking_entry_photos")).rows[0]?.count, 1);
  } finally { await f.close(); }
});

test("entry status reads enforce ownership and cancellation; timeline rejection retains the draft", async () => {
  const f = await createTimeEntryPhotoFixture(undefined, { realTimeline: true });
  try {
    const base = `/time-tracking/entries/${f.ids.entry}`;
    await request(f.app).get(base).set("Authorization", "Bearer synthetic-gm2").expect(403);
    await request(f.app).get(base).set("Authorization", "Bearer synthetic-admin").expect(200);
    await request(f.app).get(base).set("Authorization", "Bearer synthetic-kunde").expect(403);
    await request(f.app).get("/time-tracking/entries/not-an-id").set(gm).expect(400);
    await request(f.app).get(`/time-tracking/entries/${f.ids.cancelled}`).set(gm).expect(409);
    await f.pg.query("insert into visit_sessions values($1,$2,'submitted',now()-interval '15 minutes',now(),false)", [randomUUID(), f.ids.gm]);
    const conflict = await request(f.app).post(`${base}/submit`).set(gm).send({}).expect(409);
    assert.equal(conflict.body.code, "overlap");
    assert.match(conflict.body.error, /überschneidet/);
    assert.equal((await request(f.app).get(base).set(gm)).body.entry.status, "draft");
  } finally { await f.close(); }
});
