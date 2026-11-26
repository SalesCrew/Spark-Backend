import { randomUUID } from "node:crypto";
import express from "express";
import { createTimeEntryPhotoFixture, syntheticPhoto } from "./time-entry-photo-fixture.js";
import { createHomeHandler } from "./time-entry-photo-home.mjs";
import { timeTrackingEntries } from "../src/lib/schema.js";

const f = await createTimeEntryPhotoFixture("http://127.0.0.1:4037", { realTimeline: true });
await f.pg.query("update time_tracking_entries set status='cancelled',is_deleted=true where gm_user_id=$1 and id<>$2", [f.ids.gm, f.ids.entry]);
await f.pg.query("update time_tracking_entries set start_at=now()-interval '3 hours 48 seconds',end_at=null where id=$1", [f.ids.entry]);
for(let index=0;index<2;index++) await f.service.upload(f.ids.entry,{id:f.ids.gm,role:"gm"},randomUUID(),syntheticPhoto(),`Synthetisches Foto ${index+1}.png`);
f.failSigning();
const requests: Array<{method:string;path:string}> = [];
const app=express();app.use((req,_res,next)=>{ requests.push({method:req.method,path:req.path}); next(); });app.use(f.app);
const login = {user:{id:f.ids.gm,role:"gm",email:"gm@preview.test",firstName:"Vorschau",lastName:"GM"},session:{accessToken:"synthetic-gm",refreshToken:"synthetic-gm",expiresAt:4102444800}};
f.app.get("/__synthetic/user",(_req,res)=>res.json(login));
f.app.get("/__synthetic/state",async(_req,res)=>res.json({synthetic:true,requests,entries:await f.database.select().from(timeTrackingEntries),photos:(await f.pg.query("select id,entry_id,sha256,storage_path,is_deleted from time_tracking_entry_photos order by id")).rows,events:(await f.pg.query("select entry_id,event_type,payload from time_tracking_entry_events order by created_at")).rows,legacy:(await f.pg.query("select * from legacy_answers union all select * from legacy_photos")).rows}));
f.app.post("/__synthetic/control",(req,res)=>{
  if(req.body.clearRequests)requests.length=0;
  if(req.body.signingUnavailable!==undefined)f.failSigning(Boolean(req.body.signingUnavailable));
  if(req.body.uploadUnavailable!==undefined)f.failUploads(Boolean(req.body.uploadUnavailable));
  res.json({synthetic:true});
});
const readEntries=async()=> (await f.database.select().from(timeTrackingEntries)).filter(row=>row.gmUserId===f.ids.gm&&!row.isDeleted).map(row=>({...row,startAt:row.startAt?.toISOString()??null,endAt:row.endAt?.toISOString()??null}));
f.app.use(createHomeHandler(readEntries,f.ids.gm));
app.listen(4037,"127.0.0.1",()=>console.log("Disposable synthetic GM time-entry preview ready on 4037; real photo/time/timeline routes."));
