import {test} from 'node:test';
import assert from 'node:assert/strict';
import request from 'supertest';
import {randomUUID} from 'node:crypto';
import {createSpecialPhotoArchiveFixture} from '../tests/special-photo-archive-fixture.js';
const admin={Authorization:'Bearer synthetic-admin','X-Photo-Archive-Version':'2'},kunde={Authorization:'Bearer synthetic-kunde','X-Photo-Archive-Version':'2'};
test('submitted special-assignment photos join the existing archive without copying or rewriting history',async()=>{
 const f=await createSpecialPhotoArchiveFixture();try{
 const before=await f.snapshot();
 const result=(await request(f.app).get('/admin/photos').set(admin).expect(200)).body;
 assert.equal(result.total,1);const photo=result.photos[0];assert.equal(photo.source,'special_assignment');assert.equal(photo.timeEntry.id,f.ids.entry);assert.equal(photo.visitSessionId,null);assert.equal(photo.campaign.id,null);assert.equal(photo.market.id,null);assert.equal(photo.tags[0].label,'Display');assert.equal(photo.comment,'Displayaufbau beim Sondereinsatz dokumentiert');assert.deepEqual(result.stats,{visitedMarkets:0,campaigns:0});
 const detail=(await request(f.app).get(`/admin/photos/${f.photoId}`).set(admin).expect(200)).body.photo;assert.ok(detail.signedUrl);assert.equal(f.signCalls[0]?.bucket,'gm-time-entry-photos');
 await request(f.app).post('/admin/photos/signed-urls').set(admin).send({photoIds:[f.photoId],variant:'preview'}).expect(200).expect(r=>assert.equal(r.body.urls.length,1));
 const facets=(await request(f.app).get('/admin/photos/facets').set(admin).expect(200)).body.facets;assert.equal(facets.gms.length,1);assert.equal(facets.campaigns.length,0);assert.equal(facets.tags[0].label,'Display');
 assert.deepEqual(await f.snapshot(),before);
 }finally{await f.close();}
});
test('archive filters, pagination and customer permissions apply to special photos on all read paths',async()=>{
 const f=await createSpecialPhotoArchiveFixture();try{
 for(const query of [{campaignType:'sonderaufgabe'},{gmUserId:f.ids.gm},{tagId:f.ids.tag},{search:'Displayaufbau'},{dateFrom:new Date().toISOString().slice(0,10)}])assert.equal((await request(f.app).get('/admin/photos').query(query).set(admin).expect(200)).body.total,1);
 for(const query of [{campaignType:'standard'},{campaignId:randomUUID()},{chain:'SPAR'},{region:'West'},{gmUserId:f.ids.gm2},{tagId:f.ids.tag2},{search:'NotPresent'},{dateTo:'2020-01-01'}])assert.equal((await request(f.app).get('/admin/photos').query(query).set(admin).expect(200)).body.total,0);
 assert.equal((await request(f.app).get('/admin/photos?page=2&pageSize=1').set(admin).expect(200)).body.photos.length,0);
 assert.equal((await request(f.app).get('/admin/photos').set(kunde).expect(200)).body.total,0);
 await request(f.app).get(`/admin/photos/${f.photoId}`).set(kunde).expect(404);
 assert.equal((await request(f.app).post('/admin/photos/signed-urls').set(kunde).send({photoIds:[f.photoId],variant:'original'}).expect(200)).body.urls.length,0);
 assert.equal((await request(f.app).get('/admin/photos/facets').set(kunde).expect(200)).body.facets.gms.length,0);
 await request(f.app).post('/admin/photos/export').set(kunde).send({filters:{campaignType:'sonderaufgabe'}}).expect(404);
 await f.pg.query(`update time_tracking_entries set status='draft' where id=$1`,[f.ids.entry]);assert.equal((await request(f.app).get('/admin/photos').set(admin).expect(200)).body.total,0);
 }finally{await f.close();}
});
test('special-photo tag edits use the existing revision protection and preserve originals',async()=>{
 const f=await createSpecialPhotoArchiveFixture();try{
 const before=await f.snapshot(),url=`/admin/photos/${f.photoId}/tags`;
 await request(f.app).patch(url).set(kunde).send({photoTagIds:[]}).expect(403);
 await request(f.app).patch(url).set(admin).send({photoTagIds:[]}).expect(409);
 await request(f.app).patch(url).set(admin).send({photoTagIds:[f.ids.tag2],expectedRevision:1}).expect(409);
 await request(f.app).patch(url).set(admin).send({photoTagIds:[f.ids.tag2],expectedRevision:2}).expect(200);
 const detail=(await request(f.app).get(`/admin/photos/${f.photoId}`).set(admin).expect(200)).body.photo;assert.equal(detail.revision,3);assert.equal(detail.tags[0].label,'Nachher');
 const after=await f.snapshot();assert.equal(after.photos[0]?.storage_path,before.photos[0]?.storage_path);assert.equal(after.photos[0]?.sha256,before.photos[0]?.sha256);assert.deepEqual(after.legacy,before.legacy);assert.equal(after.tags.filter((t:any)=>t.is_deleted).length,1);
 }finally{await f.close();}
});

test('regular visit photos retain their context, combined pagination is stable and ZIP includes both original sources',async()=>{
 const f=await createSpecialPhotoArchiveFixture();try{
 const ids=await f.seedVisit(),before=(await f.pg.query('select * from visit_answer_photos')).rows,answersBefore=(await f.pg.query('select * from visit_answers')).rows;
 const all=(await request(f.app).get('/admin/photos?pageSize=1').set(admin).expect(200)).body;
 const second=(await request(f.app).get('/admin/photos?pageSize=1&page=2').set(admin).expect(200)).body;
 assert.equal((await request(f.app).get('/admin/photos').set({Authorization:'Bearer synthetic-admin'}).expect(200)).body.total,1);
 assert.equal(all.total,2);assert.notEqual(all.photos[0].id,second.photos[0].id);assert.deepEqual(all.stats,{visitedMarkets:1,campaigns:1});
 const visit=(await request(f.app).get(`/admin/photos/${ids.photo}`).set(admin).expect(200)).body.photo;assert.equal(visit.source,'visit');assert.equal(visit.question.text,'Foto der Platzierung');assert.equal(visit.campaign.name,'Synthetische Standardkampagne');assert.equal(visit.market.name,'SPAR Vorschau');assert.equal(visit.timeEntry,null);
 await request(f.app).patch(`/admin/photos/${ids.photo}/tags`).set({Authorization:'Bearer synthetic-admin'}).send({photoTagIds:[f.ids.tag]}).expect(200).expect(r=>assert.equal(r.body.tags[0].label,'Display'));
 assert.equal((await request(f.app).get('/admin/photos').query({campaignId:ids.campaign}).set(admin).expect(200)).body.total,1);
 assert.equal((await request(f.app).get('/admin/photos').set(kunde).expect(200)).body.total,1);
 const collect=(res:any,done:any)=>{const parts:Buffer[]=[];res.on('data',(part:Buffer)=>parts.push(part));res.on('end',()=>done(null,Buffer.concat(parts)));};
 const zip=await request(f.app).post('/admin/photos/export').set(admin).send({filters:{}}).buffer(true).parse(collect).expect(200);assert.equal(zip.headers['x-photo-count'],'2');assert.equal(zip.body.subarray(0,2).toString(),'PK');assert.match(zip.body.toString('latin1'),/Sondereinsatz\./);assert.ok(!zip.body.toString('latin1').includes('_Exportfehler.txt'));
 const only=await request(f.app).post('/admin/photos/export').set(admin).send({filters:{campaignType:'sonderaufgabe',tagIds:[f.ids.tag]}}).buffer(true).parse(collect).expect(200);assert.equal(only.headers['x-photo-count'],'1');
 assert.deepEqual((await f.pg.query('select * from visit_answer_photos')).rows,before);assert.deepEqual((await f.pg.query('select * from visit_answers')).rows,answersBefore);
 }finally{await f.close();}
});

test('work-date boundaries, cancelled/deleted records and the existing MHD customer exclusion remain enforced',async()=>{
 const f=await createSpecialPhotoArchiveFixture();try{
 await f.pg.query(`update time_tracking_entries set start_at='2026-11-25T23:30:00Z',end_at='2026-11-26T00:00:00Z',submitted_at='2026-11-27T12:00:00Z' where id=$1`,[f.ids.entry]);
 assert.equal((await request(f.app).get('/admin/photos').query({dateFrom:'2026-11-26',dateTo:'2026-11-26'}).set(admin).expect(200)).body.total,1);
 assert.equal((await request(f.app).get('/admin/photos').query({dateFrom:'2026-11-25',dateTo:'2026-11-25'}).set(admin).expect(200)).body.total,0);
 const ids=await f.seedVisit();await f.pg.query(`update campaigns set section='mhd' where id=$1`,[ids.campaign]);
 assert.equal((await request(f.app).get('/admin/photos').set(kunde).expect(200)).body.total,0);await request(f.app).get(`/admin/photos/${ids.photo}`).set(kunde).expect(404);
 await f.pg.query(`update time_tracking_entries set status='cancelled' where id=$1`,[f.ids.entry]);await request(f.app).get(`/admin/photos/${f.photoId}`).set(admin).expect(404);
 await f.pg.query(`update time_tracking_entries set status='submitted' where id=$1`,[f.ids.entry]);await f.pg.query('update time_tracking_entry_photos set is_deleted=true where id=$1',[f.photoId]);await request(f.app).get(`/admin/photos/${f.photoId}`).set(admin).expect(404);
 }finally{await f.close();}
});
