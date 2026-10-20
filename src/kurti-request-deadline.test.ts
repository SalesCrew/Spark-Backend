import assert from "node:assert/strict";
import test from "node:test";
import { EventEmitter } from "node:events";
import request from "supertest";
import { createKurtiRequestDeadline, KURTI_REQUEST_TIMEOUT_MS } from "./lib/kurti-request-deadline.js";
import { createSmKurtiFixture, toolCall } from "../tests/sm-kurti-fixture.js";
import { EMPLOYEE_AGREEMENT_KEY, EMPLOYEE_AGREEMENT_VERSION } from "./lib/employee-agreement-meta.js";

class Connection extends EventEmitter { writableEnded = false; }

test("five-minute request budget survives former cutoffs and reserves a proxy response margin", t => {
  t.mock.timers.enable({apis:["setTimeout"]});
  const connection = new Connection(), deadline = createKurtiRequestDeadline(connection);
  assert.equal(KURTI_REQUEST_TIMEOUT_MS,300_000);
  t.mock.timers.tick(120_001);assert.equal(deadline.signal.aborted,false);
  t.mock.timers.tick(174_998);assert.equal(deadline.signal.aborted,false);
  t.mock.timers.tick(1);assert.equal(deadline.signal.aborted,true);assert.equal(deadline.timedOut,true);
  deadline.dispose();assert.equal(connection.listenerCount("close"),0);
});

test("disconnect cancels provider work; normal completion removes its timer/listener", t => {
  t.mock.timers.enable({apis:["setTimeout"]});
  const connection=new Connection(),deadline=createKurtiRequestDeadline(connection);
  connection.emit("close");assert.equal(deadline.signal.aborted,true);assert.equal(deadline.timedOut,false);deadline.dispose();
  const completed=new Connection(),success=createKurtiRequestDeadline(completed);
  completed.writableEnded=true;completed.emit("close");assert.equal(success.signal.aborted,false);
  success.dispose();t.mock.timers.tick(300_000);assert.equal(success.signal.aborted,false);assert.equal(completed.listenerCount("close"),0);
});

test("all four routes abort stalled provider work, preserve memory and release request locks",async t=>{
  const f=await createSmKurtiFixture({requestTimeoutMs:200});
  try {
    await f.accept();await f.database.insert(f.schema.employeeAgreementAcceptances).values({userId:f.admin,agreementKey:EMPLOYEE_AGREEMENT_KEY,agreementVersion:EMPLOYEE_AGREEMENT_VERSION,agreementTitle:"Synthetic",agreementHash:"synthetic"});
    assert.ok(f.clientOptions.every(options=>options.timeout===300_000),"Legacy short configuration cannot shorten provider allowance");
    for (const [path,token] of [["/sm/kurti","synthetic-sm"],["/gm/kurti","synthetic-gm"],["/admin/kurti","synthetic-sm-admin"],["/admin/sm-kurti","synthetic-sm-admin"]]) {
      await t.test(path!,async()=>{
        const before=(await request(f.app).get(path+"/messages").auth(token!,{type:"bearer"}).expect(200)).body.messages;
        f.setAnswer((_body,options)=>new Promise((_resolve,reject)=>{
          const abort=()=>reject(new Error("synthetic provider aborted"));
          if(options.signal!.aborted)abort();else options.signal!.addEventListener("abort",abort,{once:true});
        }));
        const reply=(await request(f.app).post(path+"/messages").auth(token!,{type:"bearer"}).send({message:"Zeig mir die erfassten Werte."}).expect(504)).body;
        assert.match(reply.code,/kurti_timeout$/);assert.equal(f.providerOptions.at(-1)!.signal!.aborted,true);
        assert.deepEqual((await request(f.app).get(path+"/messages").auth(token!,{type:"bearer"}).expect(200)).body.messages,before,"Timeout does not overwrite or extend saved history");
        f.setAnswer(()=>({output:[],output_text:"Synthetic successful retry."}));
        await request(f.app).post(path+"/messages").auth(token!,{type:"bearer"}).send({message:"Zeig mir eine Kennzahl."}).expect(200);
      });
    }
    await t.test("admin tool rounds share the same deadline signal",async()=>{
      let step=0;
      f.setAnswer((_body,options)=>step++===0?toolCall("get_sm_overview",{}):new Promise((_resolve,reject)=>{
        const abort=()=>reject(new Error("synthetic provider aborted"));
        if(options.signal!.aborted)abort();else options.signal!.addEventListener("abort",abort,{once:true});
      }));
      const start=f.providerOptions.length;
      await request(f.app).post("/admin/sm-kurti/messages").auth("synthetic-sm-admin",{type:"bearer"}).send({message:"Zeig mir die SM Kennzahlen."}).expect(504);
      const options=f.providerOptions.slice(start);assert.equal(options.length,2);assert.equal(options[0]!.signal,options[1]!.signal);assert.equal(options[1]!.signal!.aborted,true);
    });
  } finally {await f.pg.close();}
});
