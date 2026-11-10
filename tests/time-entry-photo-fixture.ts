import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import cors from "cors";
import express, { type RequestHandler } from "express";
import * as schema from "../src/lib/schema.js";
import { createTimeEntryPhotoService } from "../src/lib/time-entry-photos.js";
import { createTimeEntryPhotoRouter } from "../src/routes/time-entry-photos.js";
import { isolatedModule } from "./isolated-module.js";
import * as roles from "../src/lib/admin-role.js";
export const syntheticPhoto=()=>Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jkAAAAABJRU5ErkJggg==','base64');
export async function createTimeEntryPhotoFixture(apiOrigin="http://127.0.0.1:4038") {
  if(process.env.DATABASE_URL||process.env.SUPABASE_URL||process.env.SUPABASE_SERVICE_ROLE_KEY||process.env.SUPABASE_SECRET_KEY)throw Error("Production configuration forbidden");
  const pg=new PGlite(),database=drizzle(pg,{schema});
  await pg.exec(`create role anon; create role authenticated; create schema storage;
  create table storage.buckets(id text primary key,name text,public boolean,file_size_limit bigint,allowed_mime_types text[]);
  create table storage.objects(id uuid,bucket_id text); alter table storage.objects enable row level security;
  create table users(id uuid primary key,role text);
  create table photo_tags(id uuid primary key default gen_random_uuid(),label text not null,is_deleted boolean not null default false,deleted_at timestamptz,created_at timestamptz not null default now(),updated_at timestamptz not null default now());
  create table time_tracking_entries(id uuid primary key default gen_random_uuid(),gm_user_id uuid not null references users(id),market_id uuid,activity_type text not null,client_entry_token text,start_at timestamptz,end_at timestamptz,comment text,status text not null default 'draft',submitted_at timestamptz,cancelled_at timestamptz,is_deleted boolean not null default false,deleted_at timestamptz,created_at timestamptz not null default now(),updated_at timestamptz not null default now(),doctor_confirmation_bucket text,doctor_confirmation_path text,doctor_confirmation_file_name text,doctor_confirmation_mime_type text,doctor_confirmation_byte_size integer,doctor_confirmation_uploaded_at timestamptz);
  create table time_tracking_entry_events(id uuid primary key default gen_random_uuid(),entry_id uuid,gm_user_id uuid,event_type text,payload jsonb not null default '{}',created_at timestamptz not null default now());
  create table legacy_answers(id uuid primary key,value text);create table legacy_photos(id uuid primary key,path text);
  insert into legacy_answers values(gen_random_uuid(),'Historical answer remains');insert into legacy_photos values(gen_random_uuid(),'historical/original.jpg');`);
  await pg.exec(await readFile(new URL('../supabase/migrations/20261110151935_gm_special_assignment_photos.sql',import.meta.url),'utf8'));
  const ids={gm:randomUUID(),gm2:randomUUID(),admin:randomUUID(),kunde:randomUUID(),sm:randomUUID(),entry:randomUUID(),entry2:randomUUID(),other:randomUUID(),cancelled:randomUUID(),tag:randomUUID(),tag2:randomUUID()};
  for(const [name,role] of [['gm','gm'],['gm2','gm'],['admin','admin'],['kunde','kunde'],['sm','sm']] as const)await pg.query('insert into users values($1,$2)',[ids[name],role]);
  for(const [id,gm,type,status] of [[ids.entry,ids.gm,'sonderaufgabe','draft'],[ids.entry2,ids.gm2,'sonderaufgabe','submitted'],[ids.other,ids.gm,'lager','draft'],[ids.cancelled,ids.gm,'sonderaufgabe','cancelled']])await pg.query('insert into time_tracking_entries(id,gm_user_id,activity_type,status,start_at,end_at)values($1,$2,$3,$4,now()-interval \'30 minutes\',now())',[id,gm,type,status]);
  await pg.query('insert into photo_tags(id,label)values($1,\'Display\'),($2,\'Nachher\')',[ids.tag,ids.tag2]);
  const objects=new Map<string,Buffer>();let failAfterPut=false;
  const service=createTimeEntryPhotoService(database as unknown as Parameters<typeof createTimeEntryPhotoService>[0],{
    async put(path,bytes){const prior=objects.get(path);if(prior&&!prior.equals(bytes))throw Error('Immutable bytes must not be overwritten');objects.set(path,Buffer.from(bytes));if(failAfterPut){failAfterPut=false;throw Object.assign(Error('Synthetic network failure'),{status:503});}},
    async sign(path){if(!objects.has(path))throw Error('Missing synthetic image');return `${apiOrigin}/__synthetic/photos/${encodeURIComponent(path)}`;}
  });
  const auth:(roles:("gm"|"admin")[])=>RequestHandler=allowed=>(req,res,next)=>{
    const token=req.header('Authorization')?.replace('Bearer synthetic-','')??'';const name=token as keyof typeof ids;
    const role=token==='gm2'?'gm':token;
    if(!allowed.includes(role as 'gm'|'admin')||!ids[name]){res.status(403).json({error:'Synthetic authorization denied'});return;}
    Object.assign(req,{authUser:{appUserId:ids[name],role}});next();
  };
  const app=express();app.use(cors({origin:["http://127.0.0.1:3038","http://localhost:3038"]}));app.use(express.json());app.use('/time-tracking',createTimeEntryPhotoRouter(service,auth));
  const tracking=await isolatedModule<{timeTrackingRouter:express.Router}>(new URL('../src/routes/time-tracking.ts',import.meta.url),{
    '../lib/admin-role.js':roles,'../lib/logger.js':{logAction(){},startActionTimer(){return 0n;}},'../middleware/auth.js':{requireAuth:auth},'../lib/db.js':{db:database},
    '../lib/day-session.js':{ensureGmSubmissionGate:async()=>({ok:true}),gmSubmissionGateError:()=>({error:'Synthetic gate'})},'../lib/supabase.js':{supabaseAdmin:{}},
    '../lib/zeiterfassung-validation.js':{respondTimelineValidationError:()=>false,validateGmTimelineInterval:async()=>{},validateGmTimelineStartPoint:async()=>{}},'../lib/schema.js':schema,
  });
  app.use('/time-tracking',tracking.timeTrackingRouter);
  app.get('/__synthetic/identity',(_req,res)=>res.json({synthetic:true,feature:'time-entry-photos',database:'Disposable PGlite'}));
  app.get('/__synthetic/photos/:path',(req,res)=>{const bytes=objects.get(req.params.path);if(!bytes){res.sendStatus(404);return;}res.type(req.params.path.endsWith('.png')?'image/png':'image/jpeg').send(bytes);});
  app.use((error:any,_req:unknown,res:express.Response,_next:unknown)=>res.status(error.status??(error.name==='ZodError'?400:500)).json({error:error.message}));
  return{app,pg,database,service,ids,objects,failAfterPutOnce(){failAfterPut=true;},close:()=>pg.close()};
}
