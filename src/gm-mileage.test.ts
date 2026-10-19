import { test } from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import { randomUUID } from "node:crypto";
import { createMileageFixture } from "../tests/gm-mileage-fixture.js";
import { buildDaySessionPayload, buildGmAggregates } from "./lib/admin-zeiterfassung.js";
import { mileageDistance } from "./lib/gm-mileage.js";

test('period correction replaces effective readings, preserves history and reconciles real admin/GM routes', async () => {
 const f = await createMileageFixture();
 try {
 const api = request(f.app), base = '/admin/zeiterfassung/mileage';
 const visitId=randomUUID(), pauseId=randomUUID(), marketId=randomUUID();
 await f.pg.query("insert into markets(id,name,address) values($1,'Synthetic Market','Testgasse 1')",[marketId]);
 await f.pg.query("insert into visit_sessions(id,gm_user_id,market_id,status,started_at,submitted_at,is_deleted) values($1,$2,$3,'submitted','2026-10-13T08:00:00Z','2026-10-13T08:20:00Z',false)",[visitId,f.gm,marketId]);
 await f.pg.query("insert into gm_day_session_pauses(id,gm_user_id,day_session_id,pause_started_at,pause_ended_at,is_deleted) values($1,$2,$3,'2026-10-13T10:00:00Z','2026-10-13T10:30:00Z',false)",[pauseId,f.gm,f.ids[1]]);
 const original = await f.pg.query('select * from gm_day_sessions order by work_date');
 const originalVisits=await f.pg.query('select * from visit_sessions');
 const originalPauses=await f.pg.query('select * from gm_day_session_pauses');
 const input = { sessionId: f.ids[1], from:'2026-10-13', to:'2026-10-14', reason:'Leihwagen – vergessener Wechsel', breakBefore:true, returnAfter:true,
 edits:[{id:f.ids[1],segments:[{startKm:5000,endKm:5100}]},{id:f.ids[2],segments:[{startKm:5115,endKm:5235}]}] };
 const context = await api.get(`${base}/context`).query({ sessionId:input.sessionId,from:input.from,to:input.to }).auth('synthetic-admin',{type:'bearer'}); assert.equal(context.status,200,JSON.stringify(context.body)); assert.equal(context.body.selected.length,2);
 const preview = await api.post(`${base}/preview`).auth('synthetic-admin',{type:'bearer'}).send(input); assert.equal(preview.status,200,JSON.stringify(preview.body));
 assert.deepEqual(preview.body.before,{businessKm:420,privateKm:12415}); assert.deepEqual(preview.body.after,{businessKm:420,privateKm:15});
 const apply = {...input,fingerprint:preview.body.fingerprint,operationId:randomUUID()};
 for(let i=0;i<2;i++) { const result=await api.post(`${base}/apply`).auth('synthetic-admin',{type:'bearer'}).send(apply); assert.equal(result.status,200,JSON.stringify(result.body)); }
 const rows:any = await f.pg.query('select * from gm_day_sessions order by work_date');
 assert.deepEqual(rows.rows.map((r:any)=>[r.start_km,r.end_km]),[[10000,10100],[5000,5100],[5115,5235],[10200,10300]]);
 for(let i=0;i<4;i++) for(const key of Object.keys(original.rows[i]!)) if(!['start_km','end_km','updated_at'].includes(key)) assert.deepEqual(rows.rows[i][key],(original.rows[i] as any)[key],key);
 assert.deepEqual((await f.pg.query('select * from visit_sessions')).rows,originalVisits.rows);
 assert.deepEqual((await f.pg.query('select * from gm_day_session_pauses')).rows,originalPauses.rows);
 const events:any=await f.pg.query('select * from gm_mileage_events order by sequence'); assert.equal(events.rows.length,2); assert.equal(events.rows[0].before.startKm,22500); assert.equal(events.rows[0].after.startKm,5000);
 await assert.rejects(()=>f.pg.exec('delete from gm_mileage_events'),/append-only/); await assert.rejects(()=>f.pg.exec('update gm_mileage_events set reason=\'changed\''),/append-only/);
 await assert.rejects(()=>f.pg.query('update gm_day_sessions set end_km=99999 where id=$1',[f.ids[1]]),/audited vehicle correction/);
 for(const url of ['/admin/zeiterfassung/days','/admin/zeiterfassung/gm-aggregates','/day-session/zeiterfassung']) {
 const response=await api.get(url).query({from:'2026-10-12',to:'2026-10-15'}).auth(url.startsWith('/day')?'synthetic-gm':'synthetic-admin',{type:'bearer'}); assert.equal(response.status,200,`${url}: ${JSON.stringify(response.body)}`);
 const aggregate=response.body.aggregate ?? response.body.rows?.[0]; if(aggregate) { assert.equal(aggregate.totalKmDriven,420); assert.equal(aggregate.privatnutzungKm,15); }
 }
 const oldEdit=await api.patch(`/admin/zeiterfassung/day-sessions/${f.ids[1]}`).auth('synthetic-admin',{type:'bearer'}).send({endKm:99999}); assert.equal(oldEdit.status,409);
 const denied=await api.post(`${base}/apply`).auth('synthetic-gm',{type:'bearer'}).send(apply); assert.equal(denied.status,403);
 const nextInput={...input,edits:input.edits.map(edit=>({...edit,segments:edit.segments.map(part=>({startKm:part.startKm+1,endKm:part.endKm+1}))}))};
 const second=await api.post(`${base}/preview`).auth('synthetic-admin',{type:'bearer'}).send(nextInput); assert.equal(second.status,200);
 await f.pg.query('update gm_day_sessions set updated_at=now() where id=$1',[f.ids[1]]);
 const stale=await api.post(`${base}/apply`).auth('synthetic-admin',{type:'bearer'}).send({...nextInput,fingerprint:second.body.fingerprint,operationId:randomUUID()}); assert.equal(stale.status,409);
 const invalid=await api.post(`${base}/preview`).auth('synthetic-admin',{type:'bearer'}).send({...input,edits:[{id:f.ids[1],segments:[{startKm:10,endKm:1}]}]}); assert.equal(invalid.status,409);
 } finally { await f.close(); }
});

test('mixed-car mileage sums stints and preserves the ordinary overnight gap',()=>{
 const mileage={segments:[{startKm:10000,endKm:10030},{startKm:500,endKm:550}],breakBefore:false,breakAfter:false};
 assert.equal(mileageDistance(10000,550,mileage),80);
 const make=(id:string,date:string,startKm:number,endKm:number,m?:typeof mileage)=>buildDaySessionPayload({sessionId:id,date,gmId:'gm',gmName:'Synthetic Driver',region:'Ost',status:'submitted',startAt:new Date(`${date}T06:00:00Z`),endAt:new Date(`${date}T14:00:00Z`),startKm,endKm,mileage:m??null,actions:[],timezone:'Europe/Vienna'});
 const aggregate=buildGmAggregates({sessions:[make('1','2026-10-12',9900,9990),make('2','2026-10-13',10000,550,mileage),make('3','2026-10-14',570,600)],timezone:'Europe/Vienna'})[0]!;
 assert.equal(aggregate.totalKmDriven,200); assert.equal(aggregate.privatnutzungKm,30);
});

test('real GM vehicle-change endpoint persists stints; stale pending requests and incomplete ranges are blocked', async()=>{
 const f=await createMileageFixture();
 try {
  const api=request(f.app), dayId=f.ids[3]!;
  await f.pg.query("update gm_day_sessions set work_date='2026-10-19',status='ended',start_km=10200,end_km=null where id=$1",[dayId]);
  const saved=await api.post('/day-session/vehicle-change').auth('synthetic-gm',{type:'bearer'}).send({beforeWork:false,segments:[{startKm:10200,endKm:10230},{startKm:700,endKm:750}]});
  assert.equal(saved.status,200,JSON.stringify(saved.body)); assert.equal(saved.body.session.endKm,750);
  const again=await api.post('/day-session/vehicle-change').auth('synthetic-gm',{type:'bearer'}).send({beforeWork:false,segments:[{startKm:10200,endKm:10230},{startKm:700,endKm:750}]}); assert.equal(again.status,200);
  const state=(await f.service.metadata([dayId])).get(dayId)!; assert.equal(mileageDistance(10200,750,state),80);
  const partial=await api.post('/admin/zeiterfassung/mileage/preview').auth('synthetic-admin',{type:'bearer'}).send({sessionId:f.ids[1],from:'2026-10-13',to:'2026-10-14',reason:'Leihwagen',breakBefore:true,returnAfter:true,edits:[{id:f.ids[1],segments:[{startKm:100,endKm:150}]}]}); assert.equal(partial.status,409);
  const pendingId=randomUUID(); await f.pg.query("insert into time_entry_change_requests(id,day_session_id,gm_user_id,source_kind,source_id,work_date,status,is_deleted) values($1,$2,$3,'day_km',$2,'2026-10-13','pending',false)",[pendingId,f.ids[1],f.gm]);
  const input={sessionId:f.ids[1]!,from:'2026-10-13',to:'2026-10-13',reason:'Leihwagen',breakBefore:true,returnAfter:true,edits:[{id:f.ids[1]!,segments:[{startKm:100,endKm:150}]}]};
  await assert.rejects(()=>f.service.preview(input),/KM-Korrektur offen/);
 } finally { await f.close(); }
});

test('a failed second day rolls back both effective readings and every history row', async()=>{
 const f=await createMileageFixture();
 try {
  const original=await f.pg.query('select id,start_km,end_km,updated_at from gm_day_sessions order by work_date');
  await f.pg.exec(`create function private.synthetic_mileage_failure() returns trigger language plpgsql as $$ begin if new.day_session_id='${f.ids[2]}'::uuid then raise exception 'synthetic second-day failure'; end if; return new; end $$; create trigger synthetic_mileage_failure before insert on gm_mileage_events for each row execute function private.synthetic_mileage_failure();`);
  const input={sessionId:f.ids[1]!,from:'2026-10-13',to:'2026-10-14',reason:'Leihwagen',breakBefore:true,returnAfter:true,edits:[{id:f.ids[1]!,segments:[{startKm:5000,endKm:5100}]},{id:f.ids[2]!,segments:[{startKm:5115,endKm:5235}]}]};
  const plan=await f.service.preview(input);
  await assert.rejects(()=>f.service.apply({...input,fingerprint:plan.fingerprint,operationId:randomUUID()},f.admin));
  assert.deepEqual((await f.pg.query('select id,start_km,end_km,updated_at from gm_day_sessions order by work_date')).rows,original.rows);
  assert.equal((await f.pg.query('select * from gm_mileage_events')).rows.length,0);
 } finally { await f.close(); }
});

test('migration is additive and absent-table fallback leaves legacy readers usable', async()=>{
 const f=await createMileageFixture();
 try {
  await f.pg.exec('drop trigger gm_day_mileage_audit_guard on gm_day_sessions; drop table gm_mileage_events');
  const states=await f.database.transaction(async tx=>{
    const service=(await import('./lib/gm-mileage-service.js')).createMileageService(tx as any);
    const result=await service.metadata([f.ids[1]!]);
    assert.equal((await tx.select().from((await import('./lib/schema.js')).gmDaySessions)).length,4);
    return result;
  });
  assert.equal(states.size,0);
  const response=await request(f.app).get('/admin/zeiterfassung/days').query({from:'2026-10-12',to:'2026-10-15'}).auth('synthetic-admin',{type:'bearer'}); assert.equal(response.status,200);
  const correction=await request(f.app).get('/admin/zeiterfassung/mileage/context').query({sessionId:f.ids[1],from:'2026-10-13',to:'2026-10-14'}).auth('synthetic-admin',{type:'bearer'}); assert.equal(correction.status,503); assert.equal(correction.body.code,'mileage_schema_required');
 } finally { await f.close(); }
});
