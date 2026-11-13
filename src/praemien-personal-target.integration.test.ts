import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
import { mutateWorkspace, managedGmSummary } from "./lib/praemien-workspace.js";
import { createPraemienWorkspaceRouter } from "./routes/praemien-workspace.js";

test("personal answer-sum target: HTTP preview, save, reread, GM projection and clearing preserve rules and answers", async () => {
  const f = await praemienFixture();
  try {
    // The fixture is a disposable PGlite database, with no env/auth/db entrypoint imports.
    await f.pg.query("delete from praemien_metric_entries where pillar_key='displays'");
    const model = structuredClone(f.model), m = model.pillars[0]!.metrics[0]!;
    Object.assign(m, { method: "answer_sum", target: null, inputs: [], sources: [{
      questionId: f.ids.question, section: "standard", scoreKey: "__value__",
      factor: true, weight: 1, label: "Schütten", minFrequency: 8,
      chains: ["Sparmarkt"], counting: "latest",
    }] });
    const wave = await mutateWorkspace(f.database, f.ids.wave, f.workspace.revision,
      { id: f.ids.admin, name: "Synthetic Admin" }, { type: "rules", model });
    for (const gm of [f.ids.gm, f.ids.other]) {
      const visit = randomUUID(), question = randomUUID(), section = randomUUID();
      await f.pg.query("insert into visit_sessions(id,gm_user_id,market_id,status,submitted_at) values($1,$2,$3,'submitted','2026-08-10T10:00:00Z')", [visit, gm, f.ids.market]);
      await f.pg.query("insert into visit_session_questions(id) values($1)", [question]);
      await f.pg.query("insert into visit_session_sections(id,section) values($1,'standard')", [section]);
      await f.pg.query("insert into visit_answers(id,visit_session_id,visit_session_question_id,visit_session_section_id,question_id,value_number) values($1,$2,$3,$4,$5,80)", [randomUUID(), visit, question, section, f.ids.question]);
    }
    const answers = (await f.pg.query("select * from visit_answers order by id")).rows;
    const app = express(); app.use(express.json());
    app.use((req, _res, next) => {
      Object.assign(req, { authUser: { appUserId: f.ids.admin, role: "admin" } }); next();
    });
    app.use("/admin/praemien/workspace", createPraemienWorkspaceRouter(f.database));
    const path = `/admin/praemien/workspace/waves/${f.ids.wave}`;
    const entry = { gmId: f.ids.gm, pillarKey: "displays", metricKey: m.key,
      value: null, target: 90, note: "Basis-Soll 100, 20 Arbeitstage, 2 Abwesenheitstage" };
    const metric = (workspace: any, gm = f.ids.gm) => workspace.results.find((r: any) => r.gmId === gm).pillars[0].metrics[0];
    let revision = wave.revision;
    assert.equal(metric((await request(app).get(path)).body).value, null);
    assert.equal(metric((await request(app).get(path)).body).rawPoints, 80);
    const preview = await request(app).post(`${path}/preview`).send({ model, entries: [entry] });
    assert.equal(preview.status, 200);
    assert.equal(metric(preview.body).value, 80 / 90 * 100);
    assert.equal(metric((await request(app).get(path)).body).value, null, "preview is not a save");
    for (const target of [0, -1]) {
      const invalid = await request(app).put(path).send({ revision, type: "values", entries: [{ ...entry, target }] });
      assert.equal(invalid.status, 400);
    }
    const saved = await request(app).put(path).send({ revision, type: "values", entries: [entry] });
    assert.equal(saved.status, 200); revision = saved.body.revision;
    const fresh = await request(app).get(path);
    assert.equal(fresh.headers["cache-control"], "private, no-store");
    assert.equal((await request(app).get("/admin/praemien/workspace/waves")).headers["cache-control"], "private, no-store");
    const reread = fresh.body;
    assert.equal(metric(reread).value, 80 / 90 * 100);
    assert.equal(metric(reread).target, 90);
    assert.equal(metric(reread, f.ids.other).value, null);
    assert.deepEqual(reread.model, model, "rules and question mappings are untouched");
    assert.equal(reread.entries.find((e: any) => e.gmId === f.ids.gm && e.pillarKey === "displays").note, entry.note);
    assert.equal((await managedGmSummary(f.database, f.ids.wave, f.ids.gm)).goals[0]!.metricValues[m.key], 80 / 90 * 100);
    const stale = await request(app).put(path).send({ revision: revision - 1, type: "values", entries: [{ ...entry, target: 10 }] });
    assert.equal(stale.status, 409);
    const cleared = await request(app).put(path).send({ revision, type: "values", entries: [{ ...entry, target: null, note: "" }] });
    assert.equal(cleared.status, 200);
    assert.equal(metric(cleared.body).value, null);
    assert.equal(metric(cleared.body).rawPoints, 80);
    assert.deepEqual((await f.pg.query("select * from visit_answers order by id")).rows, answers);
  } finally { await f.pg.close(); }
});
