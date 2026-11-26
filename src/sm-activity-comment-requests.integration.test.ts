import assert from 'node:assert/strict';
import test from 'node:test';
import {randomUUID} from 'node:crypto';
import {and,eq} from 'drizzle-orm';
import {createOptionalCommentsFixture} from '../tests/sm-optional-comments-fixture.js';

for(const dated of [false,true])test(`${dated?'dated':'monthly'} Durcharbeit comments use approval and cannot overwrite answers or photos`,async t=>{
 const f=await createOptionalCommentsFixture(dated);
 try{
  for(const q of f.payload.sections[0].questions.filter((q:any)=>q.required))await f.save(q.id,q.type==='yesnomulti'?{kind:'yesnomulti',optionCode:q.options[0].code,subOptions:[]}:{kind:'choice',optionCode:q.options[0].code});
  const photoQuestion=f.payload.sections[0].questions.find((q:any)=>q.type==='photo');const photo=await f.upload(photoQuestion.id);
  await f.sm('post',f.path+'/submit').send({...(dated?{actualMinutes:30}:{}),visitStartedAt:'2026-11-20T08:30:00Z',visitCompletedAt:'2026-11-20T09:00:00Z',clientMutationToken:randomUUID()}).expect(200);
  const payload=await f.read(),submission=payload.submission.id;
  const request=(question:any,comment:string,extra={})=>f.sm('post',`/sm/activity/submissions/${submission}/questions/${question.id}/change-requests`).send({answer:{...(payload.answers[question.id]??{kind:'empty'}),comment},commentOnly:true,expectedAnswerVersion:payload.answerVersions[question.id]??0,reason:'Kommentar nach dem Einsatz ergänzen',clientRequestToken:randomUUID(),...extra});
  const approve=(id:string)=>f.admin('post',`/admin/sm-activity/answer-change-requests/${id}/approve`).send({});
  const initialFiles=await f.database.select().from(f.schema.smQuestionAnswerFiles);
  await t.test('proposal and rejection leave stored work untouched; approval adds only a comment revision',async()=>{
   const token=randomUUID(),created=(await request(photoQuestion,'Regalaufbau nachträglich dokumentiert',{clientRequestToken:token}).expect(201)).body.request;
   assert.equal((await f.read()).answers[photoQuestion.id].comment,undefined);
   assert.deepEqual(await f.database.select().from(f.schema.smQuestionAnswerFiles),initialFiles);
   assert.equal(await f.fingerprint(),f.originalFingerprint);
   const replay=(await request(photoQuestion,'Regalaufbau nachträglich dokumentiert',{clientRequestToken:token}).expect(r=>assert.equal(r.status,200,JSON.stringify(r.body)))).body.request;assert.equal(replay.id,created.id);
   await request(photoQuestion,'Andere Notiz aus zweitem Tab').expect(409);
   await f.admin('post',`/admin/sm-activity/answer-change-requests/${created.id}/reject`).send({adminNote:'Synthetisch abgelehnt'}).expect(200);
   assert.equal((await f.read()).answers[photoQuestion.id].comment,undefined);
   const accepted=(await request(photoQuestion,'Regalfoto geprüft').expect(201)).body.request;await approve(accepted.id).expect(200);
   const next=await f.read();assert.equal(next.answers[photoQuestion.id].comment,'Regalfoto geprüft');assert.equal(next.photoFiles[photoQuestion.id].length,1);const currentFile=next.answers[photoQuestion.id].fileIds[0];assert.equal((await f.database.select().from(f.schema.smQuestionAnswerFiles).where(eq(f.schema.smQuestionAnswerFiles.id,currentFile)))[0]!.storagePath,photo.path);
   for(const old of initialFiles)assert.deepEqual((await f.database.select().from(f.schema.smQuestionAnswerFiles).where(eq(f.schema.smQuestionAnswerFiles.id,old.id)))[0],old);
   assert.equal(await f.fingerprint(),f.originalFingerprint,'historical answers and their photos remain unchanged');
  });
  await t.test('an unanswered optional question stays unanswered after a note is approved',async()=>{
   const q=f.payload.sections[0].questions.find((q:any)=>q.text==='Unbeantwortete optionale Frage');
   const created=(await request(q,'Nur eine zusätzliche Notiz').expect(201)).body.request;
   const pending=(await f.sm('get','/sm/activity/requests').expect(200)).body.answerRequests.find((row:any)=>row.id===created.id);assert.match(pending.requestedAnswerSummary,/Nur eine zusätzliche Notiz/);await approve(created.id).expect(200);
   const next=await f.read();assert.equal(next.answers[q.id].kind,'empty');assert.equal(next.answers[q.id].comment,'Nur eine zusätzliche Notiz');
   const [answer]=await f.database.select().from(f.schema.smQuestionAnswers).where(and(eq(f.schema.smQuestionAnswers.submissionQuestionId,q.id),eq(f.schema.smQuestionAnswers.isCurrent,true)));assert.equal(answer!.answerState,'unanswered');assert.equal(answer!.metricOutcomeCode,null);
  });
  await t.test('stale or forged comment-only payloads do not create proposals or revert other values',async()=>{
   const q=f.payload.sections[0].questions.find((q:any)=>q.type==='numeric');
   await request(q,'Versionskonflikt',{expectedAnswerVersion:42}).expect(409);
   await request(q,'Antwort darf nicht wechseln',{answer:{kind:'number',value:999,comment:'Antwort darf nicht wechseln'}}).expect(400);
   const created=(await request(q,'Vor Änderung notiert').expect(201)).body.request;
   const root=`/admin/sm-activity/completed/${submission}`,detail=(await f.admin('get',root).expect(200)).body;
   await f.admin('post',root+'/corrections').send({expectedVersion:detail.version,clientMutationToken:randomUUID(),reason:'Zwischenzeitlich geprüft',changes:[{questionId:q.id,answer:{kind:'number',value:42}}]}).expect(200);
   await approve(created.id).expect(409);assert.equal((await f.read()).answers[q.id].value,42);
   assert.equal(await f.fingerprint(),f.originalFingerprint);
  });
 }finally{await f.pg.close();}
});
