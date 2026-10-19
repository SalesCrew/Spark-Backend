import { createMileageFixture } from './gm-mileage-fixture.js';
const fixture = await createMileageFixture();
await fixture.pg.query("update gm_day_sessions set work_date='2026-10-19',status='ended',start_km=10200,end_km=null,is_end_km_completed=false,day_started_at='2026-10-19T06:00:00Z',day_ended_at='2026-10-19T14:00:00Z' where id=$1",[fixture.ids[3]]);
const user = {id: fixture.admin, role:'admin',email:'admin@preview.test',firstName:'Preview',lastName:'Admin'};
const gmUser = {id: fixture.gm, role:'gm',email:'gm@preview.test',firstName:'Alex',lastName:'Testfahrer'};
fixture.app.post('/auth/login',(req,res)=>{ const actor=req.body.email === gmUser.email ? gmUser : user; res.json({user:actor,session:{accessToken:`synthetic-${actor.role}`,refreshToken:'synthetic-refresh',expiresAt:Math.floor(Date.now()/1000)+86400}}); });
for(const path of ['/admin/modules/main','/admin/modules/kuehler','/admin/modules/mhd','/admin/modules/durcharbeit','/admin/fragebogen/main','/admin/fragebogen/kuehler','/admin/fragebogen/mhd','/admin/fragebogen/durcharbeit','/admin/users','/admin/market-chains']) fixture.app.get(path,(_req,res)=>res.json({modules:[],fragebogen:[],users:[],chains:[]}));
for(const path of ['/admin/campaigns/answer-change-requests','/admin/campaigns/visit-session-delete-requests','/admin/time-change-requests']) fixture.app.get(path,(_req,res)=>res.json({requests:[]}));
const period={id:'11111111-1111-4111-8111-111111111111',name:'RED 10',label:'RED 10',start:'2026-10-05',end:'2026-10-30',startDate:'2026-10-05',endDate:'2026-10-30',lookupEndDate:'2026-11-01',year:2026,periodIndexFromAnchor:9,isCurrent:true,status:'active'};
fixture.app.get('/red-month/current',(_req,res)=>res.json({current:period,config:{}}));

fixture.app.get('/red-month/calendar',(_req,res)=>res.json({periods:[period]}));
fixture.app.get('/admin/requests/count',(_req,res)=>res.json({count:0}));
fixture.app.get('/auth/me',(req,res)=>res.json({user:{id:req.header('authorization')?.includes('gm')?fixture.gm:fixture.admin,role:req.header('authorization')?.includes('gm')?'gm':'admin',email:'synthetic@preview.test',firstName:'Preview',lastName:'Admin'}}));
fixture.app.get('/employee-agreement/current',(_req,res)=>res.json({accepted:true,required:false}));
fixture.app.get('/health',(_req,res)=>res.json({ok:true,synthetic:true}));
const server=fixture.app.listen(4037,'127.0.0.1',()=>console.log('Disposable mileage preview: 4037. Synthetic only.'));
for(const signal of ['SIGTERM','SIGINT']) process.on(signal,()=>server.close(()=>{void fixture.close().then(()=>process.exit(0));}));
