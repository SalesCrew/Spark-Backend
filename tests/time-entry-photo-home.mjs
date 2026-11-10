// Disposable in-memory fixtures. No database library, production configuration or external fetch.
import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';

const root=path.dirname(fileURLToPath(import.meta.url));
const today=new Date().toLocaleDateString('en-CA',{timeZone:'Europe/Vienna'});
const id=n=>`11111111-1111-4111-8111-${String(n).padStart(12,'0')}`;
const user={id:id(1),role:'gm',email:'gm@example.invalid',firstName:'Max',lastName:'Mustermann'};
const campaign=(n,name,section)=>({campaignId:id(n),campaignName:name,section,targetVisitCount:1,submittedVisitCount:0,isComplete:false,isStartable:true});
const campaigns=[campaign(20,'Durcharbeit SPAR & EUROSPAR','durcharbeit'),campaign(21,'Bezahlte Platzierung · Winteraktion','flex'),campaign(22,'Standardbesuch November','standard')];
const markets=Array.from({length:12},(_,i)=>({id:id(100+i),standardMarketNumber:String(1000100000+i),name:i===0?'EUROSPAR Musterstadt':i===1?'SPAR am Stadtplatz':i%3?'SPAR Mustermarkt '+(i+1):'BILLA Beispielmarkt '+(i+1),dbName:i===0?'Eurospar':i%3?'Sparmarkt':'Billa',address:i===0?'Marktstraße 12':i===1?'Stadtplatz 4':'Beispielstraße '+(i+1),postalCode:'8010',city:'Musterstadt',region:'Sued',universeMarket:true,isActive:true,currentGmName:'Max Mustermann',visitFrequencyPerYear:12,activeNowCampaigns:i===11?[]:campaigns}));
let active=null,reads={};const visits=new Map();
const now=()=>new Date().toISOString();
const day={id:id(3),gmUserId:user.id,workDate:today,timezone:'Europe/Vienna',status:'started',dayStartedAt:new Date(Date.now()-3.4*3600000).toISOString(),dayEndedAt:null,startKm:42300,endKm:null,startKmDeferred:false,endKmDeferred:false,isStartKmCompleted:true,isEndKmCompleted:false,comment:null,submittedAt:null,cancelledAt:null};
const gate={dayStarted:true,startKmPending:false,endKmPending:false,pauseOpen:false,staleDayOpen:false};
const period={id:id(4),redPeriodId:id(4),redMonthYearId:id(5),label:'RED 11',periodIndexFromAnchor:10,periodIndex:11,start:'2026-11-02',end:'2026-11-27',lookupEnd:'2026-11-29',year:2026,status:'active',isCurrent:true,daysUntilEnd:25};
const config={redMonthYearId:id(5),redYear:2026,anchorStart:'2026-01-05',cycleWeeks:[4,4,5],periodCount:13,timezone:'Europe/Vienna',status:'active',updatedAt:now()};
function question(n,text,type,config={},module='Innovation & Sortiment'){return{id:id(n),questionId:id(n+1000),type,text,required:true,config,rules:[],scoring:{},chains:[],appliesToMarketChain:true,moduleId:id(9),moduleName:module,options:config.options??[],redSurvey:false,singleChoiceAvailability:null,singleChoiceAvailabilityType:null,answer:null,comment:''};}
function startPayload(marketId=markets[0].id,campaignIds=[id(20)]){
 const m=markets.find(m=>m.id===marketId)??markets[0];
 const selected=campaigns.filter(c=>campaignIds.includes(c.campaignId));
 return{market:m,sections:(selected.length?selected:[campaigns[0]]).map((c,index)=>({id:id(500+index),section:c.section,campaignId:c.campaignId,campaignName:c.campaignName,fragebogenId:id(30+index),fragebogenName:c.campaignName,orderIndex:index,questions:c.section==='flex'?[question(610,'Ist die vereinbarte Platzierung aufgebaut?','yesno',{options:['Ja','Nein']} ,'Bezahlte Platzierung'),question(611,'Wie viele Platzierungen wurden bestückt?','numeric',{min:0},'Bezahlte Platzierung')]:[question(600,'Welche unserer Innovationen sind im Regal verfügbar?','multiple',{options:['Coke 200 ml','Sprite Chill Zero 0,33 l','Mezzo Mix Zero 0,5 l','Keine der genannten']}),question(601,'Sind die Fokusartikel vollständig verfügbar?','yesno',{options:['Ja','Nein']}),question(602,'Ist das vereinbarte Kühlerplanogramm umgesetzt?','yesno',{options:['Ja','Nein']},'Gekühlte Verfügbarkeit'),question(603,'Anmerkung zur Umsetzung','text',{maxLength:1000},'Gekühlte Verfügbarkeit')]}))};
}
function createVisit(marketId,campaignIds,startedAt){const p=startPayload(marketId,campaignIds),session={id:id(700),status:'draft',startedAt:startedAt??new Date(Date.now()-14*60000).toISOString(),submittedAt:null,kuehlerUnitId:null};active={...p,session};visits.set(session.id,active);return active;}
function publication(doc){return{id:'preview-'+doc,briefingId:'preview-'+doc,version:1,title:doc==='listing'?'Listungsübersicht SPAR':doc==='placement'?'Bezahlte Platzierung · Winteraktion':doc==='weekly'?'Wochenschwerpunkte · Beispieldokument':'Durcharbeit SPAR & EUROSPAR',description:'',pageCount:doc==='listing'?7:5,pages:Array.from({length:doc==='listing'?7:5},(_,i)=>({pageNumber:i+1,title:'',description:''})),visibleFrom:'2026-11-02',visibleUntil:'2026-12-31',seenPages:reads[doc]??[],readAt:(reads[doc]?.length??0)===(doc==='listing'?7:5)?now():null};}
function preparationInbox(){
 const action=(id,name,kind,stage,startDate,endDate,marketCount)=>({id,name,kind,stage,startDate,endDate,marketCount,marketIds:markets.slice(0,marketCount).map(m=>m.id),publishedAt:'2026-11-02T08:00:00Z',documents:(kind==='durcharbeit'?['briefing','listing']:['placement']).map(doc=>{const base=publication(doc),key=id+'-'+doc,seen=reads[key]??[];return{...base,id:'preview-'+key,briefingId:'preview-'+key,title:doc==='listing'?'Listungsübersicht SPAR':name,visibleFrom:'2026-11-02',seenPages:seen,readAt:seen.length===base.pageCount?now():null};})});
 return{today:'2026-11-02',items:[action('spar-next','SPAR & EUROSPAR','durcharbeit','upcoming','2026-11-09','2026-11-27',8),action('winter-next','Coke Winteraktion','platzierung','upcoming','2026-11-16','2026-11-27',5),action('spar-current','Durcharbeit November','durcharbeit','active','2026-11-02','2026-11-13',12)]};
}
const headers={'Content-Type':'application/json','Cache-Control':'no-store','Access-Control-Allow-Origin':'http://127.0.0.1:3038','Access-Control-Allow-Methods':'GET,POST,PATCH,PUT,DELETE,OPTIONS','Access-Control-Allow-Headers':'Content-Type,Authorization,Range','Access-Control-Expose-Headers':'Content-Length,Content-Range'};
export function createHomeHandler(readEntries, gmId) { return async(req,res)=>{
 const entryRows=await readEntries();const entries=new Map(entryRows.map(e=>[e.id,e]));user.id=gmId;
 const origin=req.headers.origin;if(origin&&!['http://localhost:3038','http://127.0.0.1:3038','http://localhost:3038','http://localhost:3038'].includes(origin)){res.writeHead(403);return res.end();}
 const h={...headers,...(origin?{'Access-Control-Allow-Origin':origin}:{})};if(req.method==='OPTIONS'){res.writeHead(204,h);return res.end();}
 const url=new URL(req.url,'http://127.0.0.1:4038'),p=url.pathname;let b={};
 try{if(req.method!=='GET'&&req.method!=='HEAD'){let body='';for await(const part of req){body+=part;if(body.length>20000000)throw Error('too big');}b=body?JSON.parse(body):{};}
 const send=(data,status=200)=>{res.writeHead(status,h);res.end(JSON.stringify(data));};

 if(p==='/fixture/reset'){active=null;visits.clear();reads={};if(b.active)createVisit(markets[0].id,[id(b.kind==='platzierung'?21:20)]);return send({ok:true});}
 if(p==='/fixture/state')return send({active,reads});
 if(p.startsWith('/documents/')){const filename=p.endsWith('listing.pdf')?'listing.pdf':'briefing.pdf';const bytes=await readFile(path.join(root,'app/public/preview-documents',filename));res.writeHead(200,{...h,'Content-Type':'application/pdf','Content-Length':bytes.length});return res.end(bytes);}
 if(p==='/health')return send({ok:true,environment:'isolated synthetic fixtures'});
 if(p==='/auth/me')return send({user});
 if(p==='/employee-agreement/current')return send({accepted:true,acceptance:{acceptedAt:now(),version:'fixture',hash:'fixture'},agreement:{version:'fixture',title:'Beispiel',hash:'fixture',effectiveDate:'2026-11-02',sections:[]}});
 if(p.includes('/settings/text-scale'))return send({textScalePercent:0,updatedAt:now()});
 if(p==='/red-month/current')return send({current:period,config});
 if(p==='/red-month/calendar')return send({periods:[period]});
 if(p==='/gm/dashboard/critical')return send({serverTime:now(),daySession:{session:day,gate},activeVisit:active?{sessionId:active.session.id,startedAt:active.session.startedAt,marketId:active.market.id,marketName:active.market.name,marketAddress:active.market.address,marketPostalCode:active.market.postalCode,marketCity:active.market.city,campaignIds:active.sections.map(s=>s.campaignId),campaignNames:active.sections.map(s=>s.campaignName)}:null,activeTimeTrackingDrafts:[...entries.values()].filter(e=>e.status==='draft'),redPeriod:{redPeriodId:period.id,label:period.label,startDate:period.start,endDate:period.end,daysUntilEnd:25}});
 if(p==='/day-session/current')return send({session:day,gate});

 const hhmm=value=>new Date(value).toLocaleTimeString('de-AT',{hour:'2-digit',minute:'2-digit',timeZone:'Europe/Vienna'});
 const timeline=entryRows.filter(e=>e.status==='submitted').map(e=>({id:e.id,kind:'zusatzzeit',subtype:e.activityType,start:hhmm(e.startAt),end:hhmm(e.endAt),durationMin:30,title:'Sondereinsatz',subtitle:e.comment??'',comment:e.comment??''}));
 if(p==='/day-session/today-submissions')return send({items:[],timeline,stats:{pureWorkDurationMin:204,totalKm:42},session:{id:day.id,date:today,status:'started',startTime:hhmm(day.dayStartedAt),endTime:'',startKm:day.startKm,endKm:null}});
 if(p==='/day-session/time-change-requests')return send({requests:[]});
 if(p==='/day-session/zeiterfassung')return send({sessions:timeline.length?[{id:day.id,date:today,gmId:gmId,gmName:'Max Mustermann',region:'Sued',status:'started',isLive:true,timezone:'Europe/Vienna',startTime:hhmm(day.dayStartedAt),endTime:'',startKm:day.startKm,endKm:null,entries:timeline.map(t=>({...t,startTime:t.start,endTime:t.end})),timeline,stats:{arbeitstag:204,pauseMin:0,reineArbeitszeit:204,kmGefahren:42,marktbesuche:0,zusatz:timeline.length}}]:[],aggregate:{totalKmDriven:42,privatnutzungKm:0,averageWorkdayMin:204,currentKwReineArbeitszeitMin:204},meta:{from:today,to:today,timezone:'Europe/Vienna',includeLive:true,totalSessions:timeline.length?1:0}});

 if(p.includes('time-tracking')&&p.includes('active'))return send({entries:[],items:[]});
 if(p==='/markets/gm/kpi-summary')return send({ippAllTimeAvg:5.4,ippSampleCount:18,bonusCumulativeEur:680,lastComputedAt:now()});
 if(p==='/markets/gm/bonus-summary')return send({hasActiveWave:true,waveId:id(8),waveName:'Boni Q4 2026',year:2026,quarter:4,startDate:'2026-10-01',endDate:'2026-12-31',rewardModel:'pillar_tiers',managedModel:true,revision:1,pending:false,totalPoints:78,totalMaxPoints:100,currentRewardEur:680,fullRewardEur:1100,goals:['Schütten/Displays','Distributionsziel','Flexziel','Qualitätsziele'].map((name,i)=>({pillarId:id(80+i),name,percent:[95,82,84,91][i],points:[95,82,84,91][i],maxPoints:100,maxRewardEur:275,earnedRewardEur:170,color:'#dc2626'})),thresholds:[]});
 if(p==='/markets/gm/kuehler-mhd-progress'){const section=(n,cur,total)=>({current:cur,total,percent:Math.round(cur/total*100),startDate:'2026-11-02',endDate:'2026-11-27',markets:markets.slice(0,8).map((m,i)=>({marketId:m.id,campaignId:id(n),campaignName:n===20?campaigns[0].campaignName:n===40?'Kühlerinventur':'MHD Kontrolle',chain:i%3?'SPAR':'EUROSPAR',address:`${m.address}, ${m.postalCode} ${m.city}`,stammnr:m.standardMarketNumber,done:i<2,doneAt:i<2?now():null}))});return send({kuehler:section(40,18,32),mhd:section(41,8,15),durcharbeit:section(20,12,28),generatedAt:now(),timezone:'Europe/Vienna',periodFallback:{startDate:period.start,endDate:period.end}});}
 if(['/markets/gm/assigned-active','/markets/gm/flex-start-markets'].includes(p))return send({markets});
 if(/^\/markets\/gm\/[^/]+\/detail$/.test(p)){const m=markets.find(m=>m.id===p.split('/')[3])??markets[0];return send({period:{startDate:period.start,endDate:period.end},market:m,activeCampaigns:m.activeNowCampaigns,kuehlerTechnicalIdentNos:[],drafts:[],pastVisits:[]});}
 if(p==='/markets/gm/visit-sessions/start-payload')return send(startPayload(url.searchParams.get('marketId')??markets[0].id,(url.searchParams.get('campaignIds')??id(20)).split(',')));
 if(['/markets/gm/visit-sessions/active','/markets/gm/visit-sessions/latest-active'].includes(p))return send(active??{session:null});
 if(p==='/markets/gm/visit-sessions'&&req.method==='POST')return send(createVisit(b.marketId,b.campaignIds,b.startedAt));
 if(/^\/markets\/gm\/visit-sessions\/[^/]+$/.test(p)){const v=visits.get(p.split('/').at(-1));if(req.method==='DELETE'){active=null;return send({ok:true});}return v?send(v):send({error:'Fixture visit not found'},404);}
 if(p.endsWith('/answers')){const q=active?.sections.flatMap(s=>s.questions).find(q=>q.id===b.visitQuestionId);if(q){if('comment'in b)q.comment=b.comment;if('answer'in b){const raw=b.answer,values=Array.isArray(raw)?raw:[String(raw??'')];q.answer={id:q.id+'-answer',answerStatus:'answered',valueText:Array.isArray(raw)?null:String(raw??''),valueNumber:null,valueJson:{raw},isValid:true,validationError:null,version:1,options:values.filter(Boolean).map((v,i)=>({optionRole:q.type==='multiple'?'sub':'top',optionValue:v,orderIndex:i})),matrixCells:[],photos:[]};}}return send({ok:true,result:{answerId:q?.answer?.id??id(900),answerStatus:'answered',isValid:true,validationError:null}});}
 if(p.endsWith('/start')){if(active&&b.startedAt)active.session.startedAt=b.startedAt;return send({ok:true,session:active?.session});}
 if(p.endsWith('/submit')){if(active){active.session.status='submitted';active.session.submittedAt=now();}return send({ok:true,sessionId:active?.session.id,status:'submitted'});}
 if(p==='/gm/action-briefings')return send({items:[],unreadCount:0});
 if(p==='/gm/briefings')return send({items:[],unreadCount:0});
 if(/^\/gm\/briefings\/[^/]+\/file$/.test(p)){const doc=p.split('/')[3].replace('preview-','');return send({url:'http://127.0.0.1:4038/documents/'+(doc.endsWith('listing')?'listing':'briefing')+'.pdf'});}
 if(/^\/gm\/briefings\/[^/]+\/read$/.test(p)){const doc=p.split('/')[3].replace('preview-','');reads[doc]=[...new Set([...(reads[doc]??[]),b.pageNumber])];return send({ok:true});}
 if(p.includes('/kurti/'))return send({messages:[],people:[],configured:false,enabled:false,expiresAt:null});
 if(p.includes('messages')||p.includes('notifications'))return send({items:[],messages:[],unreadCount:0});
 if(p.includes('telemetry'))return send({ok:true});
 console.log('Unmapped fixture route:',req.method,p);send({error:'Unmapped isolated fixture route',path:p},404);
 }catch(e){console.log('Fixture error:',p,e.message);res.writeHead(500,h);res.end(JSON.stringify({error:'Fixture request failed'}));}
}; }
