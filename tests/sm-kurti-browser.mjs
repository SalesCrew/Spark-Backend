import assert from "node:assert/strict";
import { mkdir,readFile } from "node:fs/promises";
const {chromium}=await import(process.env.PLAYWRIGHT_MODULE??"playwright");
const front="http://127.0.0.1:3038",api="http://127.0.0.1:4038",artifacts=process.env.SM_KURTI_ARTIFACT_DIR??"/tmp/coke-sm-kurti-artifacts";
await mkdir(artifacts,{recursive:true});
const browser=await chromium.launch({headless:true});
const errors=[],requests=[];
async function context(options){
 const c=await browser.newContext({...options,timezoneId:"Europe/Vienna",acceptDownloads:true});
 await c.route("**/*",route=>{
 const url=new URL(route.request().url());if(!["127.0.0.1"].includes(url.hostname)||![3038,4038].includes(Number(url.port)))return route.abort();
 if(url.pathname.includes("kurti"))requests.push({path:url.pathname,method:route.request().method()});return route.continue();
 });return c;
}
async function login(p,email,path){
 p.setDefaultTimeout(15000);p.on("pageerror",e=>errors.push(e.message));
 await p.goto(front);await p.locator("#cs-email").fill(email);await p.locator("#cs-pass").fill("preview");
 await p.locator("button[type=submit]").click();await p.waitForURL(url=>url.pathname===path);
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
 const XLSX=(await import("../../node_modules/xlsx-js-style/xlsx.js")).default;
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
 assert.ok(requests.some(r=>r.path==="/admin/kurti/messages"&&r.method==="GET"));
 assert.deepEqual(errors,[]);
 console.log(JSON.stringify({result:"PASS",phone:"390×844",admin:"1440×900",synthetic:true,requests,uncaughtErrors:errors.length,downloadBytes:bytes.length,artifacts}));
}finally{await browser.close();}

