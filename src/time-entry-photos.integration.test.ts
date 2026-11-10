import {test} from 'node:test';
import assert from 'node:assert/strict';
import request from 'supertest';
import {randomUUID} from 'node:crypto';
import {createTimeEntryPhotoFixture,syntheticPhoto} from '../tests/time-entry-photo-fixture.js';
const gm={Authorization:'Bearer synthetic-gm'},gm2={Authorization:'Bearer synthetic-gm2'},admin={Authorization:'Bearer synthetic-admin'};
test('photo lifecycle persists on the same submitted entry; tags optional and historical records/bytes retained',async()=>{
const f=await createTimeEntryPhotoFixture();try{
const id=randomUUID(),base=`/time-tracking/entries/${f.ids.entry}/photos`;
const upload=()=>request(f.app).post(base).set(gm).set('Content-Type','image/png').set('x-operation-id',id).set('x-photo-filename','Nachweis.png').send(syntheticPhoto());
let result=await upload().expect(200);assert.equal(result.body.photos.length,1);assert.deepEqual(result.body.photos[0].tags,[]);
result=await upload().expect(200);assert.equal(result.body.photos.length,1);assert.equal(f.objects.size,1);
await request(f.app).patch(base+'/'+id).set(gm).send({tagIds:[f.ids.tag],expectedRevision:1}).expect(200);
await request(f.app).patch(base+'/'+id).set(gm).send({tagIds:[f.ids.tag2],expectedRevision:1}).expect(409);
await request(f.app).post(`/time-tracking/entries/${f.ids.entry}/submit`).set(gm).send({}).expect(200);
result=await request(f.app).get(base).set(admin).expect(200);assert.deepEqual(result.body.photos[0].tags,[{id:f.ids.tag,label:'Display'}]);
await f.pg.query('update photo_tags set label=\'Renamed\',is_deleted=true where id=$1',[f.ids.tag]);
result=await request(f.app).get(base).set(gm).expect(200);assert.equal(result.body.photos[0].tags[0].label,'Display');
await request(f.app).patch(base+'/'+id).set(gm).send({tagIds:[f.ids.tag,f.ids.tag2],expectedRevision:2}).expect(200);
await request(f.app).delete(base+'/'+id).set(gm).send({expectedRevision:3}).expect(200);
assert.equal((await request(f.app).get(base).set(gm)).body.photos.length,0);assert.equal(f.objects.size,1);
assert.equal((await f.pg.query('select is_deleted from time_tracking_entry_photos')).rows[0]?.is_deleted,true);
assert.equal((await f.pg.query('select status from time_tracking_entries where id=$1',[f.ids.entry])).rows[0]?.status,'submitted');
assert.equal((await f.pg.query('select value from legacy_answers')).rows[0]?.value,'Historical answer remains');
assert.equal((await f.pg.query('select path from legacy_photos')).rows[0]?.path,'historical/original.jpg');
}finally{await f.close();}});
test('owner/admin authorization; reject other GM, customer, SM, non-special and cancelled entries',async()=>{
const f=await createTimeEntryPhotoFixture();try{const path=`/time-tracking/entries/${f.ids.entry}/photos`,file=randomUUID();
for(const method of ['get','post','patch','delete'] as const){const uri=['patch','delete'].includes(method)?path+'/'+file:path;await request(f.app)[method](uri).set(gm2).send(method==='patch'?{expectedRevision:1,tagIds:[]}:method==='delete'?{expectedRevision:1}:{}).expect(method==='post'?415:403);}
for(const token of ['kunde','sm'])await request(f.app).get(path).set('Authorization','Bearer synthetic-'+token).expect(403);
await request(f.app).get(path).expect(403);
await request(f.app).get(`/time-tracking/entries/${f.ids.other}/photos`).set(gm).expect(404);
await request(f.app).get(`/time-tracking/entries/${f.ids.cancelled}/photos`).set(gm).expect(409);
await request(f.app).post(path).set(gm2).set('Content-Type','image/png').set('x-operation-id',file).send(syntheticPhoto()).expect(403);
assert.equal(f.objects.size,0);
}finally{await f.close();}});
test('retry after storage succeeds but request fails is idempotent; format and cross-entry/photo IDs validated',async()=>{
const f=await createTimeEntryPhotoFixture();try{const id=randomUUID(),base=`/time-tracking/entries/${f.ids.entry}/photos`;
const upload=()=>request(f.app).post(base).set(gm).set('Content-Type','image/png').set('x-operation-id',id).send(syntheticPhoto());
f.failAfterPutOnce();await upload().expect(503);await upload().expect(200);assert.equal(f.objects.size,1);assert.equal((await f.pg.query('select count(*) from time_tracking_entry_photos')).rows[0]?.count,1);
await request(f.app).post(base).set(gm).set('Content-Type','image/svg+xml').set('x-operation-id',randomUUID()).send(Buffer.from('<svg/>')).expect(415);
await request(f.app).post(`/time-tracking/entries/${f.ids.entry2}/photos`).set(gm2).set('Content-Type','image/png').set('x-operation-id',id).send(syntheticPhoto()).expect(409);
await request(f.app).patch(base+'/'+id).set(gm).send({tagIds:[randomUUID()],expectedRevision:1}).expect(409);
const state=await f.pg.query('select relrowsecurity from pg_class where relname in (\'time_tracking_entry_photos\',\'time_tracking_entry_photo_tags\')');assert.ok(state.rows.every(r=>r.relrowsecurity===true));
const bucket=(await f.pg.query('select public from storage.buckets where id=\'gm-time-entry-photos\'')).rows[0];assert.equal(bucket?.public,false);
await f.pg.exec('set role authenticated');await assert.rejects(()=>f.pg.query('select * from time_tracking_entry_photos'));await f.pg.exec('reset role');
}finally{await f.close();}});
