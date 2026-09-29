import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { PGlite } from "@electric-sql/pglite";
import { drizzle } from "drizzle-orm/pglite";
import express from "express";
import request from "supertest";
import { modelDatabase } from "./lib/praemien-workspace.js";
import { moduleCatalogTables, readModuleCatalogState } from "./lib/module-catalog-state.js";
import { createModuleCatalogStateRouter } from "./routes/module-catalog-state.js";

export async function moduleCatalogFixture(migrate = true) {
  const pg = new PGlite();
  await pg.exec("create role anon; create role authenticated; create role service_role bypassrls;");
  const ids = { module: randomUUID(), deleted: randomUUID(), question: randomUUID(), questionnaire: randomUUID(), visit: randomUUID() };
  for (const table of Object.values(moduleCatalogTables)) {
    await pg.exec(`create table ${table}(id uuid primary key,name text,description text,revision int default 1,is_deleted boolean default false,updated_at timestamptz default now());`);
    await pg.query(`insert into ${table}(id,name,description,is_deleted) values($1,'Original','Inhalt',false),($2,'Gelöscht','',true)`, [ids.module, ids.deleted]);
  }
  await pg.exec("create table question_bank_shared(id uuid primary key,text text); create table fragebogen_main_module(fragebogen_id uuid,module_id uuid); create table visit_sessions(id uuid primary key,status text);");
  await pg.query("insert into question_bank_shared values($1,'Frage bleibt gleich')", [ids.question]);
  await pg.query("insert into fragebogen_main_module values($1,$2)", [ids.questionnaire, ids.module]);
  await pg.query("insert into visit_sessions values($1,'submitted')", [ids.visit]);
  if (migrate) await pg.exec(await readFile(new URL("../supabase/migrations/20260929140742_module_catalog_state.sql", import.meta.url), "utf8"));
  const database = modelDatabase(drizzle(pg));
  const app = express();
  app.use(express.json());
  app.use("/admin", (req, res, next) => {
    if (!req.headers.authorization) { res.sendStatus(401); return; }
    if (req.headers.authorization !== "Bearer local-admin") { res.sendStatus(403); return; }
    next();
  });
  app.use("/admin", createModuleCatalogStateRouter(database));
  return { pg, database, ids, app };
}

test("HTTP + local Postgres: all GM scopes deactivate, reload, edit and reactivate without changing content or live links", async () => {
  const f = await moduleCatalogFixture();
  try {
    const snapshot = async () => Promise.all(["question_bank_shared", "fragebogen_main_module", "visit_sessions"].map(async (table) => (await f.pg.query(`select * from ${table}`)).rows));
    const before = await snapshot();
    for (const scope of Object.keys(moduleCatalogTables) as Array<keyof typeof moduleCatalogTables>) {
      const url = `/admin/modules/${scope}/${f.ids.module}/catalog-state`;
      const result = await request(f.app).patch(url).set("Authorization", "Bearer local-admin").send({ inactive: true });
      assert.equal(result.status, 200);
      assert.deepEqual(result.body.module, { id: f.ids.module, catalogInactive: true });
      assert.equal((await readModuleCatalogState(f.database, scope)).get(f.ids.module), true);
      // Content save and catalog state are independent even with stale editors.
      await f.pg.query(`update ${moduleCatalogTables[scope]} set name='Bearbeitet',revision=revision+1 where id=$1`, [f.ids.module]);
      assert.equal((await readModuleCatalogState(f.database, scope)).get(f.ids.module), true);
      const restored = await request(f.app).patch(url).set("Authorization", "Bearer local-admin").send({ inactive: false });
      assert.equal(restored.status, 200);
      assert.equal((await readModuleCatalogState(f.database, scope)).get(f.ids.module), false);
      const row = (await f.pg.query(`select name,description,revision,is_deleted from ${moduleCatalogTables[scope]} where id=$1`, [f.ids.module])).rows[0];
      assert.deepEqual(row, { name: "Bearbeitet", description: "Inhalt", revision: 2, is_deleted: false });
    }
    assert.deepEqual(await snapshot(), before);
  } finally { await f.pg.close(); }
});

test("invalid/deleted/nonexistent modules, permissions and missing migration fail safely", async () => {
  const f = await moduleCatalogFixture();
  try {
    const url = `/admin/modules/main/${f.ids.module}/catalog-state`;
    assert.equal((await request(f.app).patch(url).send({ inactive: true })).status, 401);
    assert.equal((await request(f.app).patch(url).set("Authorization", "Bearer local-gm").send({ inactive: true })).status, 403);
    for (const payload of [{ inactive: "false" }, {}, { inactive: false, name: "Rewrite" }]) {
      assert.equal((await request(f.app).patch(url).set("Authorization", "Bearer local-admin").send(payload)).status, 400);
    }
    for (const path of [`/admin/modules/main/not-a-uuid/catalog-state`, `/admin/modules/invalid/${f.ids.module}/catalog-state`]) {
      assert.equal((await request(f.app).patch(path).set("Authorization", "Bearer local-admin").send({ inactive: true })).status, 400);
    }
    for (const id of [f.ids.deleted, randomUUID()]) {
      assert.equal((await request(f.app).patch(`/admin/modules/main/${id}/catalog-state`).set("Authorization", "Bearer local-admin").send({ inactive: true })).status, 404);
    }
    assert.equal((await f.pg.query("select count(*)::int as n from module_catalog_state")).rows[0].n, 0);
    for (const role of ["anon", "authenticated"]) {
      await f.pg.exec(`set role ${role}`);
      await assert.rejects(f.pg.query("select * from module_catalog_state"), /permission denied/);
      await f.pg.exec("reset role");
    }
  } finally { await f.pg.close(); }
  const old = await moduleCatalogFixture(false);
  try {
    assert.equal((await readModuleCatalogState(old.database, "main")).size, 0);
    const result = await request(old.app).patch(`/admin/modules/main/${old.ids.module}/catalog-state`).set("Authorization", "Bearer local-admin").send({ inactive: true });
    assert.equal(result.status, 503);
    assert.equal(result.body.code, "module_catalog_state_not_ready");
  } finally { await old.pg.close(); }
});
