import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
import { mutateWorkspace, readWorkspace, managedGmSummary } from "./lib/praemien-workspace.js";
import { createPraemienWorkspaceRouter } from "./routes/praemien-workspace.js";

test("separate points preserve old percentages, support old clients, and retain provisional/frozen history", async () => {
  const f = await praemienFixture();
  try {
    const model = structuredClone(f.model), metric = model.pillars[0]!.metrics[0]!;
    const ids = [f.ids.question, randomUUID(), randomUUID()];
    for (const id of ids.slice(1)) await f.pg.query("insert into question_bank_shared(id,text,question_type) values($1,'Synthetic open source','numeric')", [id]);
    Object.assign(metric, { method: "answer_sum", target: null, inputs: [], sources: ids.map((id, i) => ({
      questionId: id, section: "standard", scoreKey: "__value__", factor: true,
      weight: 1, label: `Synthetic source ${i + 1}`, minFrequency: 8, chains: [], counting: "latest",
    })) });
    await mutateWorkspace(f.database, f.ids.wave, f.workspace.revision,
      { id: f.ids.admin, name: "Synthetic admin" }, { type: "rules", model });
    const visit = randomUUID(), question = randomUUID(), section = randomUUID();
    await f.pg.query("insert into visit_sessions(id,gm_user_id,market_id,status,submitted_at) values($1,$2,$3,'submitted','2026-08-14T08:00:00Z')", [visit, f.ids.gm, f.ids.market]);
    await f.pg.query("insert into visit_session_questions(id) values($1)", [question]);
    await f.pg.query("insert into visit_session_sections(id,section) values($1,'standard')", [section]);
    await f.pg.query("insert into visit_answers(id,visit_session_id,visit_session_question_id,visit_session_section_id,question_id,value_number) values($1,$2,$3,$4,$5,80)", [randomUUID(), visit, question, section, f.ids.question]);
    const originalAnswers = (await f.pg.query("select * from visit_answers order by id")).rows;
    const originalModel = structuredClone(model);
    const app = express(); app.use(express.json());
    app.use((req, _res, next) => { Object.assign(req, { authUser: { appUserId: f.ids.admin, role: "admin" } }); next(); });
    app.use("/admin/praemien/workspace", createPraemienWorkspaceRouter(f.database));
    const url = `/admin/praemien/workspace/waves/${f.ids.wave}`;
    const read = async () => (await request(app).get(url)).body;
    const result = (w: any, gm = f.ids.gm) => w.results.find((r: any) => r.gmId === gm);
    const value = (w: any) => result(w).pillars[0].metrics[0];
    let state = await read();
    assert.equal(value(state).value, 85, "legacy percentage is not converted into points");
    assert.equal(value(state).manualPoints, null);
    assert.equal(value(state).rawPoints, 80);
    assert.equal(value(state).provisional, false, "explicit legacy percentage remains authoritative");
    const entry = { gmId: f.ids.gm, pillarKey: "displays", metricKey: metric.key,
      value: 85, target: 90, note: "Synthetic point input", rawPoints: 60 };
    const save = async (changes: Record<string, unknown>) => {
      const response = await request(app).put(url).send({ revision: state.revision, type: "values", entries: [{ ...entry, ...changes }] });
      assert.equal(response.status, 200, JSON.stringify(response.body)); state = response.body;
    };
    await save({});
    assert.equal(value(state).value, 85);
    assert.equal(value(state).manualPoints, 60);
    assert.equal(value(state).automatic, 60 / 90 * 100);
    const stored = (await f.pg.query("select metric_key,value from praemien_metric_entries where wave_id=$1 and gm_user_id=$2 and pillar_key='displays' order by metric_key", [f.ids.wave, f.ids.gm])).rows;
    assert.deepEqual(stored, [{ metric_key: "percent", value: "85.0000" }, { metric_key: "points:percent", value: "60.0000" }]);
    assert.equal(state.entries.some((e: any) => e.metricKey.startsWith("points:")), false, "technical input rows are not exposed as goals");
    const preview = await request(app).post(`${url}/preview`).send({ model, entries: [{ ...entry, value: null, rawPoints: 0 }] });
    assert.equal(preview.status, 200); assert.equal(value(preview.body).value, 0);
    assert.equal(value(await read()).value, 85);
    await save({ value: null });
    assert.equal(value(state).value, 60 / 90 * 100);
    assert.equal(value(state).provisional, false);
    assert.equal(result(state).pending, false);
    const noPointProperty = { ...entry, value: null, target: 100 }; delete (noPointProperty as any).rawPoints;
    const oldPreview = await request(app).post(`${url}/preview`).send({ model, entries: [noPointProperty] });
    assert.equal(value(oldPreview.body).value, 60, "old-client previews retain point inputs");
    const oldSave = await request(app).put(url).send({ revision: state.revision, type: "values", entries: [noPointProperty] });
    assert.equal(oldSave.status, 200); state = oldSave.body;
    assert.equal(value(state).value, 60, "omitted points do not clear saved inputs");
    assert.equal((await managedGmSummary(f.database, f.ids.wave, f.ids.gm)).goals[0]!.metricValues.percent, 60);
    const sameRules = await request(app).put(url).send({ revision: state.revision, type: "rules", model });
    assert.equal(sameRules.status, 200); state = sameRules.body;
    const invalidModel = structuredClone(model); invalidModel.pillars[0]!.metrics[0]!.method = "manual";
    const invalidRules = await request(app).put(url).send({ revision: state.revision, type: "rules", model: invalidModel });
    assert.equal(invalidRules.status, 400, "saved points cannot be reinterpreted by changing calculation kind");
    const invalidPoints = await request(app).put(url).send({ revision: state.revision, type: "values", entries: [{ ...entry, rawPoints: 1000001 }] });
    assert.equal(invalidPoints.status, 400);
    const directNamespace = await request(app).put(url).send({ revision: state.revision, type: "values", entries: [{ ...entry, metricKey: "points:percent" }] });
    assert.equal(directNamespace.status, 400);
    await save({ value: null, rawPoints: null });
    assert.equal(value(state).value, 80 / 90 * 100);
    assert.equal(value(state).provisional, true);
    assert.deepEqual(value(state).sourceCoverage, { answered: 1, total: 3 });
    assert.equal(value(state).missingSources.length, 2);
    assert.equal(result(state).pending, true);
    assert.equal(result(state).pillars[0].earned, 440, "potential reward is visible but remains provisional");
    assert.equal(result(state, f.ids.other).pillars[0].metrics[0].value, 85);
    const activate = await request(app).put(url).send({ revision: state.revision, type: "activate" });
    assert.equal(activate.status, 200); state = activate.body;
    const blocked = await request(app).put(url).send({ revision: state.revision, type: "archive" });
    assert.equal(blocked.status, 400, "incomplete source results must not become confirmed frozen payouts");
    await save({ value: null, rawPoints: 60 });
    assert.deepEqual(state.model, originalModel);
    assert.deepEqual((await f.pg.query("select * from visit_answers order by id")).rows, originalAnswers);
    const archived = await request(app).put(url).send({ revision: state.revision, type: "archive" });
    assert.equal(archived.status, 200);
    const frozen = structuredClone(archived.body.results);
    assert.equal(archived.body.entries.find((e: any) => e.gmId === f.ids.gm && e.pillarKey === "displays").rawPoints, 60);
    assert.deepEqual((await readWorkspace(f.database, f.ids.wave)).results, frozen);
    const blockedEdit = await request(app).put(url).send({ revision: archived.body.revision, type: "values", entries: [{ ...entry, rawPoints: 900 }] });
    assert.equal(blockedEdit.status, 409);
  } finally { await f.pg.close(); }
});
