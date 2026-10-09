import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";
import { createSyntheticPhotoStorage, syntheticShelfPhoto } from "../tests/synthetic-photo-storage.js";

test("withdrawn original photos cannot be signed, read or carried through retained monthly links", async () => {
  class Clock extends Date { constructor(value?: string | number | Date) { super(value === undefined ? "2026-10-09T12:00:00Z" : value instanceof Date ? value.getTime() : value); } static now() { return Date.parse("2026-10-09T12:00:00Z"); } }
  const storage=createSyntheticPhotoStorage(), f=await createSMDurcharbeitFixture({clock:Clock as typeof Date,photoStorage:storage.storage});
  const admin=(method:"get"|"post",path:string)=>request(f.app)[method](path).auth("synthetic-sm-admin",{type:"bearer"});
  const sm=(method:"get"|"post",path:string)=>request(f.app)[method](path).auth("synthetic-sm",{type:"bearer"});
  try {
    await f.database.insert(f.schema.smSMDurcharbeitMarkets).values({smMarketId:f.market});
    const module=(await admin("post","/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({id:`new-${randomUUID()}`,name:"Synthetic withdrawal",description:"",questions:[{id:`new-${randomUUID()}`,type:"photo",text:"Synthetic photo",required:false,options:[],config:{},rules:[]}]}).expect(201)).body.module;
    const form=(await admin("post","/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({id:`new-${randomUUID()}`,name:"Synthetic withdrawal",description:"",status:"active",moduleIds:[module.id]}).expect(201)).body.questionnaire;
    const [version]=await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId,form.id));
    const campaign=(await admin("post","/admin/sm-smdurcharbeit-campaigns").send({name:"Synthetic withdrawal",startDate:"2026-10-01",endDate:"2026-10-31",questionnaireVersionId:version!.id,rosterDraft:[{smMarketId:f.market,smUserId:f.employee}]}).expect(201)).body.campaign;
    const preview=(await admin("get",`/admin/sm-smdurcharbeit-campaigns/${campaign.id}/preview`).expect(200)).body;
    await admin("post",`/admin/sm-smdurcharbeit-campaigns/${campaign.id}/publish`).send({expectedRevision:campaign.revision,previewToken:preview.previewToken}).expect(200);
    const start=async(followUp:boolean)=>{const target=(await sm("get","/sm/smdurcharbeit/targets").expect(200)).body.targets[0];return (await sm("post",`/sm/smdurcharbeit/targets/${target.id}/start`).send({expectedRevision:target.revision,followUp,mode:"manual",clientSubmissionToken:randomUUID()}).expect(201)).body;};
    const path=(id:string)=>`/sm/smdurcharbeit/visits/${id}`;
    const first=await start(false),payload=(await sm("get",path(first.visitId)).expect(200)).body;
    const question=payload.sections[0].questions[0], answerId=(await sm("post",`${path(first.visitId)}/photos/initialize`).send({submissionQuestionId:question.id}).expect(200)).body.answerId;
    const upload=(await sm("post",`${path(first.visitId)}/photos/presign`).send({answerId,extension:"png"}).expect(200)).body.upload;
    const bytes=syntheticShelfPhoto(true); storage.put(upload.path,bytes);
    const photo=(await sm("post",`${path(first.visitId)}/photos/commit`).send({answerId,photos:[{storageBucket:upload.bucket,storagePath:upload.path,mimeType:"image/png",byteSize:bytes.length}]}).expect(200)).body.fileIds[0];
    await sm("post",`${path(first.visitId)}/submit`).send({visitStartedAt:"2026-10-09T08:00:00Z",visitCompletedAt:"2026-10-09T08:10:00Z",clientMutationToken:randomUUID()}).expect(200);
    const follow=await start(true);
    await sm("post",`${path(follow.visitId)}/submit`).send({visitStartedAt:"2026-10-09T08:20:00Z",visitCompletedAt:"2026-10-09T08:30:00Z",clientMutationToken:randomUUID()}).expect(200);
    assert.equal((await admin("get","/admin/sm-photos").expect(200)).body.total,1);
    const frozen=await f.database.select().from(f.schema.smQuestionAnswers);
    // A separately authorized privacy withdrawal is represented only in this synthetic fixture.
    await f.database.update(f.schema.smQuestionAnswerFiles).set({isDeleted:true,deletedAt:new Date()}).where(eq(f.schema.smQuestionAnswerFiles.id,photo));
    for(const visit of [first,follow]) {
      const read=(await sm("get",path(visit.visitId)).expect(200)).body;
      assert.ok(Object.values(read.photoFiles).every((files:any)=>files.length===0));
      const managed=(await admin("get",`/admin/sm-activity/completed/${visit.submissionId}`).expect(200)).body;
      assert.equal(managed.sections[0].questions[0].photos.length,0);
    }
    assert.equal((await admin("get","/admin/sm-photos").expect(200)).body.total,0);
    assert.deepEqual((await admin("get","/admin/sm-photos/export").expect(200)).body.photos,[]);
    assert.deepEqual((await admin("post","/admin/sm-photos/signed-urls").send({ids:[photo]}).expect(200)).body.photos,[]);
    const report=(await admin("get",`/admin/sm-smdurcharbeit-campaigns/${campaign.id}/results`).expect(200)).body;
    assert.equal(report.summary.availablePhotoUploads,0); assert.equal(report.questionResults[0].answered,0);
    assert.deepEqual(report.answers[0].answer.valueJson.fileIds,[],"Reports do not expose withdrawn photo references");
    assert.deepEqual(await f.database.select().from(f.schema.smQuestionAnswers),frozen,"Reads do not rewrite historical answer IDs or payloads");
    const fresh=await start(true),read=(await sm("get",path(fresh.visitId)).expect(200)).body,q=read.sections[0].questions[0];
    assert.deepEqual(read.answers[q.id].fileIds,[]); assert.equal(read.photoFiles[q.id].length,0);
    await assert.rejects(f.pg.query("insert into sm_smdurcharbeit_answer_file_links(answer_id,file_id) values($1,$2)",[(await f.database.select().from(f.schema.smQuestionAnswers)).find(row=>row.submissionId===fresh.submissionId)!.id,photo]),/context/);
  } finally { await f.pg.close(); }
});
