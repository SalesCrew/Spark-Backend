import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";
import { installCustomerAccessRoutes } from "../tests/customer-access-fixture.js";

test("separate GM and SM customer permissions persist and enforce actual page routes", async t => {
  const gm = randomUUID(), reader = randomUUID(), writer = randomUUID(), updater = randomUUID(), none = randomUUID();
  const smKeys = ["sm_dashboard","sm_fragebogen","sm_durcharbeit","sm_fbmanagement","sm_fotoarchiv","sm_verplanung","sm_durcharbeit_verplanung","sm_zeiterfassung","sm_maerkte","sm_durcharbeit_maerkte","sm_nachrichten","shelfmerchandiser"];
  const f = await createSMDurcharbeitFixture({ customerUsers: [
    { id: gm, token: "synthetic-customer-gm", permissions: { gm_dashboard: ["read"], fragebogen: ["read","write","update"], maerkte: ["read","write","update"], gebietsmanager: ["read","write","update"] } },
    { id: reader, token: "synthetic-customer-sm-reader", permissions: Object.fromEntries(smKeys.map(key => [key,["read"]])) },
    { id: writer, token: "synthetic-customer-sm-writer", permissions: { sm_fragebogen: ["read","write"], sm_maerkte: ["read","write"], shelfmerchandiser: ["read","write"] } },
    { id: updater, token: "synthetic-customer-sm-updater", permissions: { sm_fragebogen: ["read","update"], sm_maerkte: ["read","update"], shelfmerchandiser: ["read","update"], sm_zeiterfassung: ["read","update"] } },
    { id: none, token: "synthetic-customer-none", permissions: {} },
  ] });
  const { authCalls } = await installCustomerAccessRoutes(f.app, f);
  const api = (token: string, method: "get"|"post"|"patch"|"put"|"delete", path: string, pageKey?: string) => {
    const call = request(f.app)[method](path).auth(token,{ type:"bearer" }); return pageKey ? call.set("x-coke-spark-page-key",pageKey) : call;
  };
  const admin = (method: "get"|"post"|"patch",path:string) => api("synthetic-sm-admin",method,path);
  const readRoutes = [
    ["/admin/sm-dashboard","sm_dashboard"], ["/admin/sm-questionnaires/workspace","sm_fragebogen"], ["/admin/sm-questionnaires/workspace?scope=SMDurcharbeit","sm_durcharbeit"],
    ["/admin/sm-activity/completed?from=2026-10-01&to=2026-10-31","sm_fbmanagement"], ["/admin/sm-photos","sm_fotoarchiv"],
    ["/admin/sm-planning/assignments?from=2026-10-01&to=2026-10-31","sm_verplanung"], ["/admin/sm-smdurcharbeit-campaigns","sm_durcharbeit_verplanung"],
    ["/admin/sm-smdurcharbeit-times?from=2026-10-01&to=2026-10-31","sm_zeiterfassung"], ["/admin/sm-markets","sm_maerkte"],
    ["/admin/sm-markets?SMDurcharbeitMarketScope=SMDurcharbeit","sm_durcharbeit_maerkte"], ["/admin/sm-messages","sm_nachrichten"], ["/admin/users?role=sm","shelfmerchandiser"],
  ];
  const snapshot = async () => {
    const tables = (await f.pg.query<{name:string}>("select tablename as name from pg_tables where schemaname='public' and (tablename like 'sm_%' or tablename='users') order by tablename")).rows;
    return Object.fromEntries(await Promise.all(tables.map(async table => [table.name,(await f.pg.query(`select * from ${table.name} order by to_jsonb(${table.name})::text`)).rows])));
  };
  let module: any;
  try {
    await t.test("GM-only or empty grants never authorize SM pages, forged headers, workers or account administration",async () => {
      const before = await snapshot();
      for(const [path,key] of readRoutes) {
        await api("synthetic-customer-gm","get",path!,key).expect(403);
        await api("synthetic-customer-none","get",path!,key).expect(403);
      }
      await api("synthetic-customer-sm-reader","get","/admin/sm-dashboard","gm_dashboard").expect(200);
      await api("synthetic-customer-gm","get","/admin/sm-dashboard","gm_dashboard").expect(403);
      for(const path of ["/sm/planning/assignments","/sm/dashboard","/sm/smdurcharbeit/targets","/admin/kunden-users","/admin/sm-activity/requests"]) await api("synthetic-customer-sm-reader","get",path).expect(403);
      await api("synthetic-customer-sm-reader","get","/admin/users?role=gm","gebietsmanager").expect(403);
      await api("synthetic-customer-sm-reader","get",`/admin/users/${f.employee}/special-arthur-filter`,"shelfmerchandiser").expect(403);
      assert.deepEqual(await snapshot(),before); assert.equal(authCalls.length,0);
    });
    await t.test("read-only access loads each SM page and its known filter dependencies without changing business data",async () => {
      const before = await snapshot();
      for(const [path,key] of readRoutes) {
        const result = await api("synthetic-customer-sm-reader","get",path!,key);
        assert.equal(result.status,200,`${key}: ${JSON.stringify(result.body)}`);
      }
      for(const key of ["sm_dashboard","sm_verplanung","sm_durcharbeit_verplanung","sm_fbmanagement","sm_fotoarchiv","sm_zeiterfassung","sm_nachrichten"]) await api("synthetic-customer-sm-reader","get","/admin/users?role=sm",key).expect(200);
      await api("synthetic-customer-sm-reader","get","/admin/sm-planning/gm-visits?from=2026-10-01&to=2026-10-31","sm_verplanung").expect(403);
      assert.deepEqual(await snapshot(),before);
    });
    const payload = { id:`new-${randomUUID()}`,name:"Synthetic customer module",description:"",questions:[{id:`new-${randomUUID()}`,type:"yesno",text:"Synthetic answer",required:false,options:["Ja","Nein"],config:{},rules:[]}] };
    await t.test("create and update capabilities are separate and cannot cross questionnaire scopes",async () => {
      const before = await snapshot();
      await api("synthetic-customer-sm-reader","post","/admin/sm-questionnaires/modules","sm_fragebogen").send(payload).expect(403);
      await api("synthetic-customer-sm-updater","post","/admin/sm-questionnaires/modules","sm_fragebogen").send(payload).expect(403);
      await api("synthetic-customer-sm-writer","post","/admin/sm-questionnaires/modules?scope=SMDurcharbeit","sm_fragebogen").send(payload).expect(403);
      assert.deepEqual(await snapshot(),before);
      module=(await api("synthetic-customer-sm-writer","post","/admin/sm-questionnaires/modules","sm_fragebogen").send(payload).expect(201)).body.module;
      await api("synthetic-customer-sm-writer","patch",`/admin/sm-questionnaires/modules/${module.id}`,"sm_fragebogen").send({...module,name:"Forbidden writer edit"}).expect(403);
      const beforePostUpdate = await snapshot();
      await api("synthetic-customer-sm-writer","post","/admin/sm-questionnaires/modules","sm_fragebogen").send({...module,name:"Forbidden POST edit"}).expect(403);
      assert.deepEqual(await snapshot(), beforePostUpdate);
      const updated=(await api("synthetic-customer-sm-updater","patch",`/admin/sm-questionnaires/modules/${module.id}`,"sm_fragebogen").send({...module,name:"Allowed synthetic update"}).expect(200)).body.module;
      assert.equal(updated.name,"Allowed synthetic update");
      await api("synthetic-customer-sm-updater","patch",`/admin/sm-questionnaires/modules/${module.id}?scope=SMDurcharbeit`,"sm_fragebogen").send({...module,name:"Wrong scope"}).expect(403);
    });
    await t.test("SM directory creation and editing cannot escalate roles or cross into GM accounts", async () => {
      const payload = { role: "sm", firstName: "Preview", lastName: "New SM", email: "sm-customer-created@preview.test", isActive: true, travelTimeEnabled: false };
      const before = await snapshot();
      await api("synthetic-customer-sm-reader", "post", "/admin/users", "shelfmerchandiser").send(payload).expect(403);
      await api("synthetic-customer-sm-updater", "post", "/admin/users", "shelfmerchandiser").send(payload).expect(403);
      await api("synthetic-customer-sm-writer", "post", "/admin/users", "shelfmerchandiser").send({...payload, role: "admin"}).expect(403);
      await api("synthetic-customer-sm-writer", "post", "/admin/users", "shelfmerchandiser").send({...payload, role: "gm"}).expect(403);
      assert.deepEqual(await snapshot(), before);
      const created = (await api("synthetic-customer-sm-writer", "post", "/admin/users", "shelfmerchandiser").send(payload).expect(201)).body.user;
      await api("synthetic-customer-sm-writer", "patch", `/admin/users/${created.id}`, "shelfmerchandiser").send({lastName:"Writer edit denied"}).expect(403);
      const edited = (await api("synthetic-customer-sm-updater", "patch", `/admin/users/${created.id}`, "shelfmerchandiser").send({lastName:"Updated SM"}).expect(200)).body.user;
      assert.equal(edited.lastName, "Updated SM");
      await api("synthetic-customer-sm-updater", "patch", `/admin/users/${f.admin}`, "shelfmerchandiser").send({lastName:"Forbidden admin edit"}).expect(403);
      await api("synthetic-customer-sm-updater", "post", `/admin/users/${created.id}/password`, "shelfmerchandiser").send({newPassword:"Synthetic-only-2026"}).expect(403);
    });
    await t.test("SM page grants cannot be forged to update unrelated planning or access photo exports", async () => {
      const before = await snapshot();
      await api("synthetic-customer-sm-reader", "post", "/admin/sm-planning/assignments", "sm_verplanung").send({}).expect(403);
      await api("synthetic-customer-sm-updater", "put", "/admin/sm-planning/questionnaire-assignment", "sm_durcharbeit_verplanung").send({}).expect(403);
      await api("synthetic-customer-sm-updater", "get", "/admin/sm-photos/export", "sm_maerkte").expect(403);
      await api("synthetic-customer-sm-reader", "post", "/admin/sm-photos/signed-urls", "sm_fotoarchiv").send({ids:[randomUUID()]}).expect(200);
      await api("synthetic-customer-sm-reader", "post", "/admin/sm-messages", "sm_nachrichten").send({}).expect(403);
      await api("synthetic-customer-sm-updater", "post", `/admin/sm-activity/completed/${randomUUID()}/corrections`, "sm_zeiterfassung").send({}).expect(403);
      await api("synthetic-customer-sm-updater", "post", `/admin/sm-activity/completed/${randomUUID()}/corrections`, "shelfmerchandiser").send({}).expect(403);
      assert.deepEqual(await snapshot(), before);
    });
    await t.test("customer management saves independent GM and SM grants, exposes them to the account, and revocation applies to its existing token",async () => {
      const permissions={gm_dashboard:["read"],sm_dashboard:["read"],sm_fragebogen:["read","write"],invalid_page:["read"]};
      const created=(await admin("post","/admin/kunden-users").send({firstName:"Synthetic",lastName:"Mixed",email:"mixed@preview.test",permissions}).expect(201)).body.user;
      const token=`synthetic-customer:${created.id}`;
      assert.deepEqual(created.permissions,{gm_dashboard:["read"],sm_dashboard:["read"],sm_fragebogen:["read","write"]});
      const saved=(await admin("patch",`/admin/kunden-users/${created.id}`).send({permissions:{...created.permissions,sm_fragebogen:["read","update"]}}).expect(200)).body.user;
      assert.deepEqual(saved.permissions.gm_dashboard,["read"]); assert.deepEqual(saved.permissions.sm_fragebogen,["read","update"]);
      const self=(await api(token,"get","/admin/kunden-users/me").expect(200)).body.permissions; assert.deepEqual(self,saved.permissions);
      await api(token,"get","/admin/sm-dashboard","sm_dashboard").expect(200);
      await admin("patch",`/admin/kunden-users/${created.id}`).send({permissions:{gm_dashboard:["read"]}}).expect(200);
      await api(token,"get","/admin/sm-dashboard","sm_dashboard").expect(403);
      assert.deepEqual((await f.database.select().from(f.schema.kundeUsers).where(eq(f.schema.kundeUsers.userId,created.id)))[0]!.pagePermissions,{gm_dashboard:["read"]});
      await api(token,"patch",`/admin/kunden-users/${created.id}`).send({permissions:{sm_dashboard:["read","write","update"]}}).expect(403);
      await admin("patch", `/admin/kunden-users/${created.id}`).send({isActive:false}).expect(200);
      await api(token,"get","/admin/kunden-users/me").expect(403);
    });
  } finally { await f.pg.close(); }
});
