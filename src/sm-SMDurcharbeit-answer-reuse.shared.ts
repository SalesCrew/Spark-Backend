import { randomUUID } from "node:crypto";
import { and, eq, inArray } from "drizzle-orm";
import { smQuestionAnswers as answers, smQuestionnaireSubmissionQuestions as questions, smQuestionAnswerOptions as options,
  smQuestionAnswerMatrixCells as cells, smQuestionAnswerFiles as files, smSMDurcharbeitFileLinks as links,
  smSMDurcharbeitAnswerProvenance as provenance, smQuestionAnswerEvents as events } from "./lib/schema.js";
import { normalizeSmVisitAnswer, isAnsweredSmVisitPayload } from "./sm-visit.shared.js";
import type { SMDurcharbeitExecutor, SMDurcharbeitTx } from "./sm-SMDurcharbeit-campaign.shared.js";

/** Canonical object metadata plus authorized answer links; inherited objects are never copied. */
export async function SMDurcharbeitAnswerFiles(executor:SMDurcharbeitExecutor,answerIds:string[]) {
  if(!answerIds.length) return [];
  const [own,inherited]=await Promise.all([
    executor.select().from(files).where(and(inArray(files.answerId,answerIds),eq(files.isDeleted,false))),
    executor.select({file:files,answerId:links.answerId}).from(links).innerJoin(files,eq(files.id,links.fileId)).where(and(inArray(links.answerId,answerIds),eq(links.isDeleted,false),eq(files.isDeleted,false))),
  ]);
  return [...own.map(file=>({...file,SMDurcharbeitInherited:false})),...inherited.map(row=>({...row.file,answerId:row.answerId,SMDurcharbeitInherited:true}))];
}
export async function copySMDurcharbeitMonthlyAnswers(tx:SMDurcharbeitTx,input:{sourceSubmissionId:string;submissionId:string;actorId:string;basisRevision:number}) {
  const [sourceQuestions,newQuestions,sourceAnswers]=await Promise.all([
    tx.select().from(questions).where(and(eq(questions.submissionId,input.sourceSubmissionId),eq(questions.isDeleted,false))),
    tx.select().from(questions).where(and(eq(questions.submissionId,input.submissionId),eq(questions.isDeleted,false))),
    tx.select().from(answers).where(and(eq(answers.submissionId,input.sourceSubmissionId),eq(answers.isCurrent,true),eq(answers.isDeleted,false))),
  ]);
  const sourceQuestionById=new Map(sourceQuestions.map(q=>[q.id,q]));
  const newQuestionByVersion=new Map(newQuestions.map(q=>[`${q.questionVersionId}:${q.questionCodeSnapshot}`,q]));
  const ids=sourceAnswers.map(a=>a.id);
  const [sourceOptions,sourceCells,sourceFiles]=await Promise.all([
    ids.length?tx.select().from(options).where(and(inArray(options.answerId,ids),eq(options.isDeleted,false))):[],
    ids.length?tx.select().from(cells).where(and(inArray(cells.answerId,ids),eq(cells.isDeleted,false))):[],
    SMDurcharbeitAnswerFiles(tx,ids),
  ]);
  for(const source of sourceAnswers) {
    const old=sourceQuestionById.get(source.submissionQuestionId);
    const next=old?newQuestionByVersion.get(`${old.questionVersionId}:${old.questionCodeSnapshot}`):undefined;
    if(!next) continue;
    const snapshot={type:next.questionTypeSnapshot,config:next.configSnapshot,options:(next.answerOptionsSnapshot as Array<{code:string;label:string}>)??[]};
    const normalized=normalizeSmVisitAnswer(snapshot,source.valueJson??{kind:"empty"});
    const availableFiles=new Set(sourceFiles.filter(file=>file.answerId===source.id).map(file=>file.id));
    const value=normalized.kind==="photo"?{...normalized,fileIds:normalized.fileIds.filter(id=>availableFiles.has(id))}:normalized;
    const answered=isAnsweredSmVisitPayload(value),id=randomUUID();
    await tx.insert(answers).values({id,submissionId:input.submissionId,submissionQuestionId:next.id,answerVersion:1,
      answerState:answered?"answered":"unanswered",valueJson:value,
      valueText:value.kind==="text"?value.value:null,valueNumber:value.kind==="number"?String(value.value):null,
      // Preserve source attribution/time; the new actor only initiated carry-over, recorded in the event.
      answeredByUserId:source.answeredByUserId,answeredAt:source.answeredAt});
    await tx.insert(provenance).values({answerId:id,sourceAnswerId:source.id,sourceSubmissionId:input.sourceSubmissionId,sourceRevision:input.basisRevision});
    const copiedOptions=sourceOptions.filter(o=>o.answerId===source.id).map(({id:_id,answerId:_answer,isDeleted:_deleted,deletedAt:_deletedAt,createdAt:_created,updatedAt:_updated,...value})=>({...value,id:randomUUID(),answerId:id}));
    const copiedCells=sourceCells.filter(o=>o.answerId===source.id).map(({id:_id,answerId:_answer,isDeleted:_deleted,deletedAt:_deletedAt,createdAt:_created,updatedAt:_updated,...value})=>({...value,id:randomUUID(),answerId:id}));
    if(copiedOptions.length) await tx.insert(options).values(copiedOptions);
    if(copiedCells.length) await tx.insert(cells).values(copiedCells);
    const copiedFiles=sourceFiles.filter(f=>f.answerId===source.id && value.kind==="photo" && value.fileIds.includes(f.id)).map(f=>({answerId:id,fileId:f.id}));
    if(copiedFiles.length) await tx.insert(links).values(copiedFiles);
    await tx.insert(events).values({answerId:id,submissionId:input.submissionId,eventType:"set",answerVersion:1,actorUserId:input.actorId,
      payload:{SMDurcharbeitCarryOver:true,sourceSubmissionId:input.sourceSubmissionId,sourceAnswerId:source.id,value}});
  }
}
