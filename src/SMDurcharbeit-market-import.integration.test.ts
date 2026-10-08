import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { eq } from 'drizzle-orm';
import request from 'supertest';
import { createSMDurcharbeitFixture } from '../tests/SMDurcharbeit-fixture.js';
import { createSyntheticPhotoStorage, syntheticShelfPhoto } from '../tests/synthetic-photo-storage.js';
import { SMDurcharbeitImportHeaders } from './sm-SMDurcharbeit-market-import.shared.js';

test('SMDurcharbeit: import every synthetic source row, plan, submit answers/photos and reconcile management without changing history', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: Date.parse('2026-10-07T15:00:00Z') });
  const storage = createSyntheticPhotoStorage();
  const f = await createSMDurcharbeitFixture({ photoStorage: storage.storage });
  const admin = (method: 'get'|'post'|'put'|'patch', path: string) => request(f.app)[method](path).auth('synthetic-sm-admin', { type: 'bearer' });
  const sm = (method: 'get'|'post'|'put', path: string) => request(f.app)[method](path).auth('synthetic-sm', { type: 'bearer' });
  const mapping = { SMDurcharbeitVertriebstyp:'A', name:'B', address:'C', postalCode:'D', city:'E', SMDurcharbeitEmEh:'F', shelfMerchandiserName:'G' };
  const payload = { fileName:'synthetic-SMDurcharbeit.xlsx', sheetName:'Gesamt', mapping, rows:[Array.from(SMDurcharbeitImportHeaders), ...Array.from({length:424}, (_, i) => ['Billa', i < 100 ? '' : `Synthetic Company ${i}`, i === 0 ? 'Testgasse 1' : `Synthetic Avenue ${i}`, '1010', 'Wien', i < 100 ? '' : 'EM', 'SM Local'])] };
  // Three exact duplicate pairs and 100 empty company/EM values; no real market data is used.
  for (const i of [9,49,79]) payload.rows[i+1] = [...payload.rows[i]!];
  const createForm = async (scope: 'standard'|'SMDurcharbeit') => {
    const module = (await admin('post', `/admin/sm-questionnaires/modules?scope=${scope}`).send({id:'new-'+randomUUID(),name:`Synthetic ${scope}`,description:'Synthetic only',questions:[
      {id:'new-'+randomUUID(),text:'Synthetic yes/no',type:'yesno',required:true,options:['Ja','Nein'],config:{},rules:[]},
      ...(scope === 'SMDurcharbeit' ? [{id:'new-'+randomUUID(),text:'Synthetic photograph',type:'photo',required:true,options:[],config:{instruction:'Synthetic photo'},rules:[]}] : []),
    ]}).expect(201)).body.module;
    const form = (await admin('post', `/admin/sm-questionnaires/questionnaires?scope=${scope}`).send({id:'new-'+randomUUID(),name:`Synthetic ${scope}`,description:'Synthetic only',status:'active',moduleIds:[module.id]}).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId,form.id));
    return {form,version:version!};
  };
  try {
    const standard = await createForm('standard'), da = await createForm('SMDurcharbeit');
    await admin('put','/admin/sm-planning/questionnaire-assignment').send({questionnaireTemplateId:standard.form.id}).expect(200);
    const old = await f.assignment();
    const oldPayload = (await sm('post',`/sm/visits/${old.id}/start`).send({mode:'manual',clientSubmissionToken:randomUUID()}).expect(200)).body;
    const oldQuestion = oldPayload.sections[0].questions[0];
    await sm('put',`/sm/visits/${old.id}/answers/${oldQuestion.id}`).send({answer:{kind:'choice',optionCode:oldQuestion.options[0].code},expectedAnswerVersion:0,clientMutationToken:randomUUID()}).expect(200);
    await sm('post',`/sm/visits/${old.id}/submit`).send({actualMinutes:15,visitStartedAt:'2026-10-07T07:00:00Z',visitCompletedAt:'2026-10-07T07:15:00Z',clientMutationToken:randomUUID()}).expect(200);
    const historyTables = ['sm_markets','sm_assignments','sm_questionnaire_submissions','sm_questionnaire_submission_sections','sm_questionnaire_submission_questions','sm_question_answers','sm_question_answer_options','sm_question_answer_files','sm_question_answer_events','sm_assignment_time_submissions','sm_assignment_events'];
    const before = Object.fromEntries(await Promise.all(historyTables.map(async table => [table,(await f.pg.query(`select * from ${table} order by id`)).rows])));

    await request(f.app).post('/admin/sm-markets/SMDurcharbeit/import').send(payload).expect(401);
    await sm('post','/admin/sm-markets/SMDurcharbeit/import').send(payload).expect(403);
    await admin('post','/admin/sm-markets/SMDurcharbeit/import').send({...payload,mapping:{...mapping,city:'A'}}).expect(400);
    const imported = (await admin('post','/admin/sm-markets/SMDurcharbeit/import').send(payload).expect(200)).body;
    assert.equal(imported.summary.created,424); assert.equal(imported.summary.skipped,0);
    assert.equal(imported.summary.SMDurcharbeitDuplicateRows,3); assert.equal(imported.summary.SMDurcharbeitUnassignedRows,0);
    assert.equal(imported.markets.length,424);
    const sources = await f.database.select().from(f.schema.smSMDurcharbeitMarkets);
    for (const source of sources) {
      const original = payload.rows[source.SMDurcharbeitSourceRow!-1]!;
      assert.deepEqual(source.SMDurcharbeitSourceValues,Object.fromEntries(SMDurcharbeitImportHeaders.map((h,i)=>[h,original[i]])));
      assert.equal(imported.markets.find((r:any)=>r.id===source.smMarketId).assignedSmUserId,f.employee);
    }
    const ids = sources.map(row=>row.smMarketId).sort();
    const repeat = (await admin('post','/admin/sm-markets/SMDurcharbeit/import').send(payload).expect(200)).body.summary;
    assert.equal(repeat.created,0); assert.equal(repeat.updated,0); assert.equal(repeat.unchanged,424);
    payload.rows[1]![5] = 'EH';
    const changed = (await admin('post','/admin/sm-markets/SMDurcharbeit/import').send(payload).expect(200)).body.summary;
    assert.equal(changed.created,0); assert.equal(changed.updated,1); assert.equal(changed.unchanged,423);
    await admin('post','/admin/sm-markets/SMDurcharbeit/import').send({...payload,rows:[payload.rows[0],...payload.rows.slice(1).reverse()]}).expect(200);
    assert.deepEqual((await f.database.select().from(f.schema.smSMDurcharbeitMarkets)).map(row=>row.smMarketId).sort(),ids);
    const normal = (await admin('get','/admin/sm-markets').expect(200)).body.markets;
    assert.deepEqual(normal.map((r:any)=>r.id),[f.market]);
    assert.equal((await admin('get','/admin/sm-markets?SMDurcharbeitMarketScope=SMDurcharbeit').expect(200)).body.markets.length,424);
    assert.equal((await admin('get','/admin/sm-markets?SMDurcharbeitMarketScope=all').expect(200)).body.markets.length,425);
    const target = sources.find(row=>row.SMDurcharbeitSourceRow===2)!.smMarketId;
    const plan = {smMarketId:target,smUserId:f.employee,workDate:'2026-10-07',plannedMinutes:45,idempotencyKey:randomUUID(),SMDurcharbeitMarketScope:'SMDurcharbeit',SMDurcharbeitQuestionnaireOverrideVersionId:da.version.id};
    await admin('post','/admin/sm-planning/assignments').send({...plan,smMarketId:f.market}).expect(409);
    await admin('post','/admin/sm-planning/assignments').send({...plan,smMarketId:f.market,idempotencyKey:old.idempotencyKey}).expect(409);
    await admin('post','/admin/sm-planning/assignments').send({...plan,SMDurcharbeitQuestionnaireOverrideVersionId:standard.version.id}).expect(409);
    const id = (await admin('post','/admin/sm-planning/assignments').send(plan).expect(201)).body.assignmentId;
    assert.equal((await admin('post','/admin/sm-planning/assignments').send(plan).expect(200)).body.assignmentId,id);
    const employeePlan = (await sm('get','/sm/planning/assignments?from=2026-10-07&to=2026-10-07').expect(200)).body.assignments.find((r:any)=>r.id===id);
    assert.equal(employeePlan.SMDurcharbeitMarket,true); assert.equal(employeePlan.SMDurcharbeitQuestionnaireSelection.questionnaireVersionId,da.version.id);
    const visit = (await sm('post',`/sm/visits/${id}/start`).send({mode:'manual',clientSubmissionToken:randomUUID()}).expect(200)).body;
    assert.equal(visit.submission.questionnaireName,da.form.name);
    const [choice,photo] = visit.sections[0].questions;
    await sm('post',`/sm/visits/${id}/submit`).send({actualMinutes:15,visitStartedAt:'2026-10-07T08:00:00Z',visitCompletedAt:'2026-10-07T08:15:00Z',clientMutationToken:randomUUID()}).expect(409);
    await sm('put',`/sm/visits/${id}/answers/${choice.id}`).send({answer:{kind:'choice',optionCode:choice.options[0].code},expectedAnswerVersion:0,clientMutationToken:randomUUID()}).expect(200);
    const {answerId} = (await sm('post',`/sm/visits/${id}/photos/initialize`).send({submissionQuestionId:photo.id}).expect(200)).body;
    const {upload} = (await sm('post',`/sm/visits/${id}/photos/presign`).send({answerId,extension:'png'}).expect(200)).body;
    const image = syntheticShelfPhoto(true); storage.put(upload.path,image);
    await sm('post',`/sm/visits/${id}/photos/commit`).send({answerId,photos:[{storageBucket:upload.bucket,storagePath:upload.path,originalFileName:'Synthetic shelf.png',mimeType:'image/png',byteSize:image.length,widthPx:640,heightPx:480}]}).expect(200);
    await sm('post',`/sm/visits/${id}/submit`).send({actualMinutes:15,visitStartedAt:'2026-10-07T08:00:00Z',visitCompletedAt:'2026-10-07T08:15:00Z',clientMutationToken:randomUUID()}).expect(200);
    const managed = (await admin('get','/admin/sm-activity/completed?from=2026-10-07&to=2026-10-07&SMDurcharbeitCatalogScope=SMDurcharbeit').expect(200)).body;
    assert.equal(managed.visits.length,1);
    const detail = (await admin('get',`/admin/sm-activity/completed/${managed.visits[0].id}`).expect(200)).body;
    assert.equal(detail.visit.SMDurcharbeitCatalogScope,'SMDurcharbeit');
    assert.equal(detail.sections.flatMap((s:any)=>s.questions).length,2);
    const savedPhoto = detail.sections.flatMap((s:any)=>s.questions).flatMap((q:any)=>q.photos);
    assert.equal(savedPhoto.length,1); assert.ok(savedPhoto[0].signedUrl);
    const archive = (await admin('get',`/admin/sm-photos?marketId=${target}&SMDurcharbeitCatalogScope=SMDurcharbeit`).expect(200)).body;
    assert.equal(archive.total,1); assert.equal(archive.photos[0].submissionId,managed.visits[0].id);
    const report = (await admin('get','/admin/sm-dashboard?from=2026-10-07&to=2026-10-07&SMDurcharbeitCatalogScope=SMDurcharbeit').expect(200)).body;
    assert.equal(report.summary.completedVisits,1);
    // Every pre-existing market, question snapshot, answer, file and audit row remains identical.
    for (const table of historyTables) {
      const after = (await f.pg.query(`select * from ${table}`)).rows as any[];
      for (const row of before[table] as any[]) assert.deepEqual(after.find(value=>value.id===row.id),row,`${table} history preserved`);
    }
  } finally { await f.pg.close(); }
});

test('SMDurcharbeit: invalid rows, ambiguous names, archived markets and manual creation stay isolated and atomic', async () => {
  const f = await createSMDurcharbeitFixture();
  const admin = (path: string) => request(f.app).post(path).auth('synthetic-sm-admin', { type: 'bearer' });
  const route = '/admin/sm-markets/SMDurcharbeit';
  const mapping = { SMDurcharbeitVertriebstyp:'A', name:'B', address:'C', postalCode:'D', city:'E', SMDurcharbeitEmEh:'F', shelfMerchandiserName:'G' };
  try {
    const normal = (await f.database.select().from(f.schema.smMarkets))[0];
    await f.database.insert(f.schema.users).values({id:randomUUID(),role:'sm',firstName:'SM',lastName:'Local',email:'ambiguous@preview.test',isActive:true});
    const payload = {fileName:'synthetic.xlsx',sheetName:'Gesamt',mapping,rows:[Array.from(SMDurcharbeitImportHeaders),
      ['Billa','','Testgasse 1','1010','Wien','','Local SM'],
      ['Spar','Unmatched','Synthetic 2','1020','Wien','EH','Unmatched Synthetic'],
      ['Billa','Invalid','Synthetic 3','bad','Wien','EM',''],
      ['', '', '', '', '', '', ''],
    ]};
    const imported = (await admin(route+'/import').send(payload).expect(200)).body;
    assert.equal(imported.summary.created,2); assert.equal(imported.summary.skipped,1);
    assert.equal(imported.summary.totalParsedRows,3); assert.equal(imported.summary.SMDurcharbeitUnassignedRows,2);
    assert.ok(imported.markets.every((r:any)=>r.assignedSmUserId===null));
    assert.deepEqual((await f.database.select().from(f.schema.smMarkets).where(eq(f.schema.smMarkets.id,f.market)))[0],normal);
    const [target] = await f.database.select().from(f.schema.smSMDurcharbeitMarkets);
    // A reviewed account is kept even when the source names change; archived markets never resurrect.
    await f.database.update(f.schema.smMarkets).set({assignedSmUserId:f.employee}).where(eq(f.schema.smMarkets.id,target!.smMarketId));
    payload.rows[1]![6]='Different Synthetic Person';
    await admin(route+'/import').send(payload).expect(200);
    assert.equal((await f.database.select().from(f.schema.smMarkets).where(eq(f.schema.smMarkets.id,target!.smMarketId)))[0]!.assignedSmUserId,f.employee);
    await f.database.update(f.schema.smMarkets).set({isDeleted:true}).where(eq(f.schema.smMarkets.id,target!.smMarketId));
    const archived=(await admin(route+'/import').send(payload).expect(200)).body;
    assert.equal(archived.summary.created,0); assert.equal(archived.summary.skipped,2);
    assert.equal(archived.markets.length,1);
    const manual={name:'Manual Synthetic',chain:'Spar',address:'Synthetic 4',postalCode:'1040',city:'Wien',assignedSmUserId:f.employee};
    const sizeBefore=(await f.database.select().from(f.schema.smMarkets)).length;
    await admin(route).send({...manual,assignedSmUserId:f.admin}).expect(400);
    assert.equal((await f.database.select().from(f.schema.smMarkets)).length,sizeBefore);
    const created=(await admin(route).send(manual).expect(201)).body.market;
    assert.equal(created.SMDurcharbeitMarket,true); assert.equal(created.assignedSmUserId,f.employee);
    const [source]=await f.database.select().from(f.schema.smSMDurcharbeitMarkets).where(eq(f.schema.smSMDurcharbeitMarkets.smMarketId,created.id));
    assert.equal(source!.SMDurcharbeitOrt,'Wien');
    assert.equal((await f.database.select().from(f.schema.smMarkets)).length,sizeBefore+1);
    assert.equal((await f.pg.query('select count(*)::int as count from sm_smdurcharbeit_markets r left join sm_markets m on m.id=r.sm_market_id where m.id is null')).rows[0]!.count,0);
  } finally { await f.pg.close(); }
});
