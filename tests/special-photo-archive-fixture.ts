import {getTableConfig} from 'drizzle-orm/pg-core';
import {randomUUID} from 'node:crypto';
import express from 'express';
import cors from 'cors';
import * as schema from '../src/lib/schema.js';
import * as roles from '../src/lib/admin-role.js';
import * as access from '../src/lib/photo-archive-access.js';
import {createTimeEntryPhotoFixture,syntheticPhoto} from './time-entry-photo-fixture.js';
import {isolatedModule} from './isolated-module.js';
export async function createSpecialPhotoArchiveFixture(origin='http://127.0.0.1:4037', imageBytes=syntheticPhoto()) {
 const f=await createTimeEntryPhotoFixture(origin);
 await f.pg.exec(`alter table users add column first_name text default 'Vorschau';alter table users add column last_name text default 'GM';`);
 const tables=[schema.campaigns,schema.markets,schema.visitSessions,schema.visitSessionSections,schema.visitSessionQuestions,schema.visitAnswers,schema.visitAnswerPhotos,schema.visitAnswerPhotoTags];
 for(const table of tables){const config=getTableConfig(table);const columns=config.columns.map(c=>`"${c.name}" ${c.enumValues?.length?'text':c.getSQLType()}${c.primary?' primary key default gen_random_uuid()':c.default===false?' default false':c.default===true?' default true':''}`);await f.pg.exec(`create table "${config.name}" (${columns.join(',')})`);}
 await f.pg.exec('create table visit_question_comments(id uuid,visit_session_question_id uuid,comment_text text,is_deleted boolean,created_at timestamptz,updated_at timestamptz)');
 const signCalls:Array<{bucket:string;path:string}>=[];
 const router=await isolatedModule<{adminPhotosRouter:express.Router}>(new URL('../src/routes/admin-photos.ts',import.meta.url),{
  '../lib/db.js':{db:f.database},'../lib/schema.js':schema,'../lib/admin-role.js':roles,'../lib/photo-archive-access.js':access,'../lib/time-entry-photos-runtime.js':{timeEntryPhotoService:f.service},
  '../lib/kunde-access.js':{requireKundeAdminPermission:(_q:unknown,_r:unknown,n:()=>void)=>n()},
  '../middleware/auth.js':{requireAuth:(allowed:string[])=>(req:express.Request,res:express.Response,next:express.NextFunction)=>{const name=req.header('Authorization')?.replace('Bearer synthetic-','') as keyof typeof f.ids;const role=name==='gm2'?'gm':name;if(!allowed.includes(role)){res.sendStatus(403);return;}Object.assign(req,{authUser:{appUserId:f.ids[name],role,email:'admin@preview.test'}});next();}},
  '../lib/supabase.js':{supabaseAdmin:{storage:{from(bucket:string){return{async createSignedUrl(path:string){signCalls.push({bucket,path});return{data:{signedUrl:`${origin}/__synthetic/photos/${encodeURIComponent(path)}`},error:null};}};}}}},
 }, Date, (async(input: Parameters<typeof fetch>[0])=>{
  const url=new URL(String(input));if(url.origin!==origin||!url.pathname.startsWith('/__synthetic/photos/'))throw Error('External network forbidden');
  const bytes=f.objects.get(decodeURIComponent(url.pathname.slice('/__synthetic/photos/'.length)));return bytes?new Response(new Uint8Array(bytes),{status:200}):new Response(null,{status:404});
 }) as typeof fetch);
 const app=express();app.use(cors({origin:['http://127.0.0.1:3037','http://localhost:3037']}));app.use(express.json());app.use('/admin',router.adminPhotosRouter);
 app.get('/__synthetic/photos/:path',(req,res)=>{const bytes=f.objects.get(req.params.path);if(!bytes){res.sendStatus(404);return;}if(req.query.download)res.attachment(String(req.query.download));res.type('image/png').send(bytes);});app.use(f.app);
 app.use((e:any,_q:unknown,res:express.Response,_n:unknown)=>res.status(e.status??500).json({error:e.message}));
 const photoId=randomUUID();await f.service.upload(f.ids.entry,{id:f.ids.gm,role:'gm'},photoId,imageBytes,'Displayaufbau.png');await f.service.setTags(f.ids.entry,photoId,{id:f.ids.gm,role:'gm'},1,[f.ids.tag]);
 await f.pg.query(`update time_tracking_entries set status='submitted',submitted_at=now(),comment='Displayaufbau beim Sondereinsatz dokumentiert' where id=$1`,[f.ids.entry]);
 const snapshot=async()=>({photos:(await f.pg.query('select * from time_tracking_entry_photos order by id')).rows,tags:(await f.pg.query('select * from time_tracking_entry_photo_tags order by id')).rows,legacy:(await f.pg.query('select * from legacy_answers union all select * from legacy_photos')).rows});
 const seedVisit=async()=>{
  const ids={campaign:randomUUID(),market:randomUUID(),session:randomUUID(),section:randomUUID(),question:randomUUID(),answer:randomUUID(),photo:randomUUID()};
  const insert=async(table:string,row:Record<string,unknown>)=>{const keys=Object.keys(row);await f.pg.query(`insert into "${table}" (${keys.map(k=>`"${k}"`).join(',')}) values(${keys.map((_,i)=>`$${i+1}`).join(',')})`,Object.values(row));};
  await insert('campaigns',{id:ids.campaign,name:'Synthetische Standardkampagne',section:'standard',is_deleted:false});
  await insert('markets',{id:ids.market,name:'SPAR Vorschau',db_name:'SPAR',address:'Testweg 1',postal_code:'8010',city:'Graz',region:'Süd',is_deleted:false});
  await insert('visit_sessions',{id:ids.session,gm_user_id:f.ids.gm,market_id:ids.market,status:'submitted',is_deleted:false,started_at:new Date(Date.now()-3600000),submitted_at:new Date()});
  await insert('visit_session_sections',{id:ids.section,visit_session_id:ids.session,campaign_id:ids.campaign,section:'standard',fragebogen_name_snapshot:'Synthetischer Fragebogen',is_deleted:false});
  await insert('visit_session_questions',{id:ids.question,question_id:randomUUID(),question_type:'photo',question_text_snapshot:'Foto der Platzierung',module_name_snapshot:'Platzierung',is_deleted:false});
  await insert('visit_answers',{id:ids.answer,visit_session_id:ids.session,visit_session_section_id:ids.section,visit_session_question_id:ids.question,question_type:'photo',is_deleted:false});
  await insert('visit_answer_photos',{id:ids.photo,visit_answer_id:ids.answer,storage_bucket:'visit-answer-photos',storage_path:'synthetic/visit.png',mime_type:'image/png',uploaded_at:new Date(),is_deleted:false});
  f.objects.set('synthetic/visit.png',imageBytes);return ids;
 };
 return {...f,app,photoId,signCalls,snapshot,seedVisit};
}
