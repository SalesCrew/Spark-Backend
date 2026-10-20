import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { mkdir,readFile } from "node:fs/promises";
const {chromium}=await import(process.env.PLAYWRIGHT_MODULE??"playwright");
const front="http://127.0.0.1:3038",api="http://127.0.0.1:4038",artifacts=process.env.SM_KURTI_ARTIFACT_DIR??"/tmp/coke-sm-kurti-artifacts";
await mkdir(artifacts,{recursive:true});
let ready=false;
for(let attempt=0;attempt<60;attempt++){
 try{const r=await fetch(api+"/__fixture/state",{signal:AbortSignal.timeout(500)});if(r.ok){ready=true;break;}}catch{}
 await new Promise(resolve=>setTimeout(resolve,100));
}
assert.ok(ready,"The explicitly started synthetic fixture must be ready");
const browser=await chromium.launch({headless:true});
const errors=[],requests=[];
async function context(options){
 const c=await browser.newContext({...options,timezoneId:"Europe/Vienna",acceptDownloads:true});
 await c.route("**/*",route=>{
 const url=new URL(route.request().url());if(!["127.0.0.1"].includes(url.hostname)||![3038,4038].includes(Number(url.port)))return route.abort();
 if(url.pathname.includes("kurti"))requests.push({path:url.pathname,method:route.request().method(),body:route.request().postData()});return route.continue();
 });return c;
}
async function login(p,email,path){
 p.setDefaultTimeout(15000);p.on("pageerror",e=>errors.push(e.message));
 await p.goto(front);await p.locator("#cs-email").fill(email);await p.locator("#cs-pass").fill("preview");
 await p.locator("button[type=submit]").click();await p.waitForURL(url=>url.pathname===path,{waitUntil:"domcontentloaded"});
}
async function mention(p,label,query,aria,screen,arrowDown=0){
 const input=p.getByLabel(aria,{exact:true});await p.waitForFunction(aria=>!document.querySelector(`[aria-label="${aria}"]`)?.disabled,aria);
 await input.fill(`Wie viele Besuche hat @${query}`);
 await p.getByRole("listbox",{name:"Personenvorschläge"}).waitFor();
 await input.press("Escape");assert.equal(await input.getAttribute("aria-expanded"),"false");assert.ok(await input.isVisible());await input.press("Backspace");
 await input.fill(`Wie viele Besuche hat @${query}`);
 for(let i=0;i<arrowDown;i++)await input.press("ArrowDown");
 const option=p.getByRole("option",{name:new RegExp(label)});await option.waitFor();await p.waitForTimeout(550);assert.equal(await option.getAttribute("aria-selected"),"true");
 await p.screenshot({path:artifacts+`/${screen}-mentions.png`});
 const before=requests.filter(r=>r.method==="POST"&&r.path.endsWith("/messages")).length;
 if(screen==="sm")await option.tap();else await input.press("Enter");assert.equal(await input.inputValue(),`Wie viele Besuche hat ${label} `);assert.equal(requests.filter(r=>r.method==="POST"&&r.path.endsWith("/messages")).length,before,"Selecting a mention must not send");
 await input.press("Enter");await p.getByText(`${label}: 12 Besuche.`,{exact:true}).last().waitFor();await p.waitForTimeout(1500);
 const sent=requests.filter(r=>r.method==="POST"&&r.path.endsWith("/messages")).at(-1);assert.match(sent.body,/\[user:[0-9a-f-]{36}\]/i);assert.ok(!sent.body.includes(label));
 const hiddenScroll=await input.evaluate(e=>{let node=e.parentElement;while(node){if(node.scrollLeft!==0)return node.scrollLeft;node=node.parentElement;}return 0;});
 assert.equal(hiddenScroll,0,"Focusing a mention must not horizontally scroll the sliding chat");
 await p.screenshot({path:artifacts+`/${screen}-identity-answer.png`});
}
try{
 const sm=await context({viewport:{width:390,height:844},isMobile:true,hasTouch:true});const phone=await sm.newPage();
 await login(phone,"sm@preview.test","/sm");
 await phone.locator(".gm-menu-scrollbars-hidden").tap();
 await phone.getByText("Frag Kurti...",{exact:true}).click();
 await phone.getByLabel("Nachricht an Kurti",{exact:true}).fill("Wo finde ich die Durcharbeit?");
 await phone.locator("form").filter({has:phone.getByLabel("Nachricht an Kurti",{exact:true})}).locator("button[type=submit]").click();
 await phone.getByText("Deine Durcharbeit findest du als Komponente auf Home.",{exact:false}).last().waitFor();
 await phone.waitForTimeout(2500);
 assert.ok(requests.some(r=>r.path==="/sm/kurti/messages"&&r.method==="POST"));
 assert.ok(!requests.some(r=>r.path==="/gm/kurti/messages"));
 await phone.screenshot({path:artifacts+"/phone-chat.png"});
 assert.equal(await phone.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
 await mention(phone,"Annika Musterfrau","Ann","Nachricht an Kurti","sm");
 const admin=await context({viewport:{width:1440,height:900}});const laptop=await admin.newPage();
 await login(laptop,"admin@preview.test","/admin/sm/dashboard");
 if(await laptop.locator('button[aria-controls="admin-kurti-panel"]').getAttribute("aria-expanded")!=="true")await laptop.locator('button[aria-controls="admin-kurti-panel"]').click();
 await laptop.getByText("Kurti SM Admin",{exact:true}).waitFor();
 const input=laptop.getByLabel("Nachricht an Admin-Kurti",{exact:true});
 await input.fill("Zeig mir SM Kennzahlen als Diagramm");
 await input.press("Enter");
 await laptop.getByText("SM im Überblick",{exact:true}).waitFor();
 await laptop.waitForTimeout(2200);
 await laptop.screenshot({path:artifacts+"/admin-chart.png"});
 await input.fill("Exportiere die SM Märkte als Excel");await input.press("Enter");
 const downloadButton=laptop.getByRole("button",{name:"Excel herunterladen",exact:true});await downloadButton.waitFor();
 const downloadEvent=laptop.waitForEvent("download");await downloadButton.click();const download=await downloadEvent;
 await download.saveAs(artifacts+"/sm-markets.xlsx");
 assert.equal(await download.failure(),null);
 const bytes=await readFile(artifacts+"/sm-markets.xlsx");assert.ok(bytes.length>1000);
 const XLSX=createRequire(new URL("../../package.json",import.meta.url))("xlsx-js-style");
 const workbook=XLSX.read(bytes,{type:"buffer"});
 const sheet=XLSX.utils.sheet_to_json(workbook.Sheets["SM Auszug"],{header:1});
 assert.ok(JSON.stringify(sheet).includes("Synthetic Billa"));
 assert.ok(JSON.stringify(sheet).includes("Handelskette"));
 await laptop.screenshot({path:artifacts+"/admin-export.png"});
 await laptop.reload();if(await laptop.locator('button[aria-controls="admin-kurti-panel"]').getAttribute("aria-expanded")!=="true")await laptop.locator('button[aria-controls="admin-kurti-panel"]').click();
 await laptop.getByRole("button",{name:"Excel herunterladen",exact:true}).waitFor();
 await laptop.goto(front+"/admin/maerkte");
 if(await laptop.locator('button[aria-controls="admin-kurti-panel"]').getAttribute("aria-expanded")!=="true")await laptop.locator('button[aria-controls="admin-kurti-panel"]').click();
 await laptop.getByText("Kurti GM Admin",{exact:true}).waitFor();
 assert.equal(await laptop.getByText("SM im Überblick",{exact:true}).count(),0);
 assert.equal(await laptop.getByRole("button",{name:"Excel herunterladen",exact:true}).count(),0);
 assert.ok(requests.some(r=>r.path==="/admin/sm-kurti/messages"&&r.method==="POST"));
 assert.ok(requests.some(r=>r.path==="/admin/sm-kurti/export"&&r.method==="POST"));
 await laptop.goto(front+"/admin/sm/dashboard");
 if(await laptop.locator('button[aria-controls="admin-kurti-panel"]').getAttribute("aria-expanded")!=="true")await laptop.locator('button[aria-controls="admin-kurti-panel"]').click();
 await mention(laptop,"Annika Musterfrau","Ann","Nachricht an Admin-Kurti","sm-admin");
 await mention(laptop,"Benedikt Musteradmin","","Nachricht an Admin-Kurti","sm-admin-arrow",1);
 await laptop.goto(front+"/admin/maerkte");
 if(await laptop.locator('button[aria-controls="admin-kurti-panel"]').getAttribute("aria-expanded")!=="true")await laptop.locator('button[aria-controls="admin-kurti-panel"]').click();
 await mention(laptop,"Annika Musterfrau","Ann","Nachricht an Admin-Kurti","gm-admin");
 const gm=await context({viewport:{width:834,height:1194},isMobile:true,hasTouch:true});const ipad=await gm.newPage();
 await login(ipad,"gm@preview.test","/gm");
 await ipad.locator(".gm-menu-scrollbars-hidden").tap();await ipad.getByText("Frag Kurti...",{exact:true}).click();
 await mention(ipad,"Benedikt Musteradmin","Ben","Nachricht an Kurti","gm");
 const state=await (await fetch(api+"/__fixture/state")).json();
 assert.ok(state.wire.length>6);for(const call of state.wire){assert.equal(call.model,"gpt-6-luna");assert.ok(!/Annika|Musterfrau|Benedikt|Musteradmin|Carolin|Beispielsm|Denise|Doris/.test(JSON.stringify(call)));}

 assert.ok(requests.some(r=>r.path==="/admin/kurti/messages"&&r.method==="GET"));
 assert.deepEqual(errors,[]);
 console.log(JSON.stringify({result:"PASS",phone:"390×844",admin:"1440×900",synthetic:true,requests:requests.map(({path,method})=>({path,method})),modelCalls:state.wire.length,uncaughtErrors:errors.length,downloadBytes:bytes.length,artifacts}));
}catch(error){for(const c of browser.contexts())for(const p of c.pages()){console.error("Failure page:",p.url(),(await p.locator("body").innerText()).slice(0,1200));await p.screenshot({path:artifacts+"/failure-"+c.pages().indexOf(p)+".png"});}throw error;}finally{await browser.close();}

