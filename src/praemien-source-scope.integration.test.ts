import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
import { mutateWorkspace, managedGmSummary, readWorkspace } from "./lib/praemien-workspace.js";
import { evaluateModel, type WaveModel } from "./praemien-model.shared.js";
import { createPraemienWorkspaceRouter } from "./routes/praemien-workspace.js";

test("employee-specific display scope: real preview/save/read/GM projection, unique cross-area counting and intact history", async t => {
  const f = await praemienFixture();
  try {
    const model = structuredClone(f.model), p = model.pillars[0]!, metric = p.metrics[0]!;
    const sourceIds = [f.ids.question, ...Array.from({ length: 37 }, () => randomUUID())];
    for (const [i, id] of sourceIds.entries()) if (i) await f.pg.query(
      "insert into question_bank_shared(id,text,question_type) values($1,$2,$3)",
      [id, `Synthetic display source ${i}`, i === 1 ? "yesno" : "numeric"]);
    Object.assign(metric, { method: "answer_sum", inputs: [], target: null, sources: sourceIds.map((id, i) => ({
      questionId: id, section: "standard", scoreKey: i === 1 ? "Ja" : "__value__", factor: i !== 1,
      weight: i === 1 ? 3 : 2, label: `Synthetic source ${i}`, minFrequency: 8, chains: ["Sparmarkt"], counting: "latest",
    })) });
    let state = await mutateWorkspace(f.database, f.ids.wave, f.workspace.revision,
      { id: f.ids.admin, name: "Synthetic admin" }, { type: "rules", model });
    await mutateWorkspace(f.database, f.ids.wave, state.revision,
      { id: f.ids.admin, name: "Synthetic admin" }, { type: "values", entries: [f.ids.gm, f.ids.other].map(gmId => ({
        gmId, pillarKey: p.key, metricKey: metric.key, value: 72.5, target: 80, note: "Synthetic saved target",
      })) });
    const extraMarket = randomUUID();
    await f.pg.query("insert into markets values($1,8,'Sparmarkt')", [extraMarket]);
    async function answer(gm: string, questionId: string, section: string, day: string, numeric: number | null, options: string[] = [], market = f.ids.market, valid = true, status = "submitted") {
      const v = randomUUID(), q = randomUUID(), sec = randomUUID(), a = randomUUID();
      await f.pg.query("insert into visit_sessions(id,gm_user_id,market_id,status,submitted_at) values($1,$2,$3,$4,$5)", [v, gm, market, status, `${day}T09:00:00Z`]);
      await f.pg.query("insert into visit_session_questions(id) values($1)", [q]);
      await f.pg.query("insert into visit_session_sections(id,section) values($1,$2)", [sec, section]);
      await f.pg.query("insert into visit_answers(id,visit_session_id,visit_session_question_id,visit_session_section_id,question_id,value_number,is_valid) values($1,$2,$3,$4,$5,$6,$7)", [a, v, q, sec, questionId, numeric, valid]);
      for (const option of options) await f.pg.query("insert into visit_answer_options(visit_answer_id,option_value) values($1,$2)", [a, option]);
    }
    await answer(f.ids.gm, sourceIds[0]!, "standard", "2026-08-01", 10);
    await answer(f.ids.gm, sourceIds[0]!, "flex", "2026-08-02", 15);
    await answer(f.ids.gm, sourceIds[0]!, "billa", "2026-08-03", 20);
    await answer(f.ids.gm, sourceIds[0]!, "flex", "2026-08-04", 4, [], extraMarket);
    await answer(f.ids.gm, sourceIds[1]!, "standard", "2026-08-01", null, ["Ja"]);
    await answer(f.ids.gm, sourceIds[1]!, "flex", "2026-08-03", null, ["Nein"]);
    await answer(f.ids.gm, sourceIds[0]!, "kuehler", "2026-08-05", 999);
    await answer(f.ids.gm, sourceIds[0]!, "flex", "2026-08-06", 999, [], f.ids.market, false);
    await answer(f.ids.gm, sourceIds[0]!, "billa", "2026-08-07", 999, [], f.ids.market, true, "draft");
    await answer(f.ids.gm, sourceIds[0]!, "flex", "2026-10-01", 999);
    await answer(f.ids.other, sourceIds[0]!, "standard", "2026-08-01", 3);
    await answer(f.ids.other, sourceIds[0]!, "flex", "2026-08-02", 100);
    await answer(f.ids.other, sourceIds[0]!, "billa", "2026-08-03", 100);
    const businessSnapshot = async () => (await f.pg.query(`select jsonb_build_object(
      'answers',(select jsonb_agg(to_jsonb(t) order by id) from visit_answers t),
      'options',(select jsonb_agg(to_jsonb(t) order by id) from visit_answer_options t),
      'visits',(select jsonb_agg(to_jsonb(t) order by id) from visit_sessions t),
      'entries',(select jsonb_agg(to_jsonb(t) order by gm_user_id,pillar_key,metric_key) from praemien_metric_entries t)) as data`)).rows;
    const originalBusiness = await businessSnapshot();
    const app = express(); app.use(express.json());
    app.use((req, _res, next) => { Object.assign(req, { authUser: { appUserId: f.ids.admin, role: "admin" } }); next(); });
    app.use("/admin/praemien/workspace", createPraemienWorkspaceRouter(f.database));
    const url = `/admin/praemien/workspace/waves/${f.ids.wave}`;
    const get = async () => (await request(app).get(url).expect(200)).body;
    const result = (w: any, gm = f.ids.gm) => w.results.find((r: any) => r.gmId === gm).pillars[0].metrics[0];
    state = await get(); const baseline = structuredClone(state);
    assert.equal(result(state).automaticPoints, 23);
    const proposed = structuredClone(model);
    proposed.pillars[0]!.metrics[0]!.sourceSectionOverrides = [{ gmId: f.ids.gm, sections: ["standard", "flex", "billa"] }];
    await t.test("preview is read-only; scope is per employee and coverage still counts logical sources", async () => {
      const preview = (await request(app).post(`${url}/preview`).send({ model: proposed }).expect(200)).body;
      assert.equal(result(preview).automaticPoints, 48);
      assert.equal(result(preview).automatic, 60);
      assert.equal(result(preview).value, 72.5, "the saved percentage is not silently removed");
      assert.deepEqual(result(preview).sourceCoverage, { answered: 2, total: 38 });
      assert.deepEqual(result(preview, f.ids.other), result(baseline, f.ids.other));
      assert.equal((await get()).revision, baseline.revision);
      assert.deepEqual((await get()).model, baseline.model);
    });
    await t.test("save and reload persist the explicit scope, with existing rules and all employee inputs retained", async () => {
      state = (await request(app).put(url).send({ type: "rules", revision: state.revision, model: proposed }).expect(200)).body;
      assert.equal(result(state).automaticPoints, 48);
      assert.equal(result(state).value, 72.5);
      assert.deepEqual(result(state, f.ids.other), result(baseline, f.ids.other));
      assert.deepEqual(state.model.pillars[0].metrics[0].sources, metric.sources);
      const fresh = await get(); assert.equal(result(fresh).automaticPoints, 48);
      const summary = await managedGmSummary(f.database, f.ids.wave, f.ids.gm);
      assert.equal(summary.goals[0]!.metricDetails[0]!.automaticPoints, 48);
      assert.equal(summary.goals[0]!.metricDetails[0]!.value, 72.5);
      assert.equal(state.history[0].type, "rules");
      assert.deepEqual(state.history[0].payload.model.pillars[0].metrics[0].sourceSectionOverrides, proposed.pillars[0]!.metrics[0]!.sourceSectionOverrides);
    });
    await t.test("an older client omitting the additive setting cannot erase it in preview or rules saves", async () => {
      const preview = (await request(app).post(`${url}/preview`).send({ model }).expect(200)).body;
      assert.equal(result(preview).automaticPoints, 48);
      state = (await request(app).put(url).send({ type: "rules", revision: state.revision, model }).expect(200)).body;
      assert.equal(result(state).automaticPoints, 48);
    });
    await t.test("invalid employee, area and metric changes are rejected atomically", async () => {
      for (const change of [
        (m: WaveModel) => { m.pillars[0]!.metrics[0]!.sourceSectionOverrides![0]!.gmId = randomUUID(); },
        (m: WaveModel) => { m.pillars[0]!.metrics[0]!.sourceSectionOverrides![0]!.gmId = f.ids.admin; },
        (m: WaveModel) => { (m.pillars[0]!.metrics[0]!.sourceSectionOverrides![0]!.sections as string[]) = ["standard", "kuehler"]; },
        (m: WaveModel) => { m.pillars[0]!.metrics[0]!.sourceSectionOverrides![0]!.sections = ["flex", "billa"]; },
        (m: WaveModel) => { m.pillars[0]!.metrics[0]!.sourceSectionOverrides![0]!.sections = ["standard", "flex", "flex"]; },
        (m: WaveModel) => { m.pillars[0]!.metrics[0]!.sourceSectionOverrides!.push({ ...m.pillars[0]!.metrics[0]!.sourceSectionOverrides![0]! }); },
        (m: WaveModel) => { m.pillars[0]!.metrics[0]!.sources[0]!.section = "flex"; },
        (m: WaveModel) => { m.pillars[0]!.kind = "flex"; },
      ]) {
        const invalid = structuredClone(proposed); change(invalid);
        await request(app).put(url).send({ type: "rules", revision: state.revision, model: invalid }).expect(400);
      }
      assert.equal((await get()).revision, state.revision);
      await request(app).put(url).send({ type: "rules", revision: 0, model: proposed }).expect(409);
    });
    await t.test("explicit removal restores original eligibility; frozen snapshots remain immutable", async () => {
      const cleared = structuredClone(proposed); cleared.pillars[0]!.metrics[0]!.sourceSectionOverrides = [];
      state = (await request(app).put(url).send({ type: "rules", revision: state.revision, model: cleared }).expect(200)).body;
      assert.equal(result(state).automaticPoints, 23);
      assert.deepEqual(await businessSnapshot(), originalBusiness);
      const frozenResults = structuredClone(state.results), frozenEntries = structuredClone(state.entries);
      const frozen = { ...state, closedAt: "2026-10-01T10:00:00Z" };
      await f.pg.query("update praemien_wave_settings set closed_snapshot=$2::jsonb,closed_at=$3 where wave_id=$1", [f.ids.wave, JSON.stringify(frozen), frozen.closedAt]);
      const persisted = await readWorkspace(f.database, f.ids.wave);
      assert.deepEqual(persisted.results, frozenResults); assert.deepEqual(persisted.entries, frozenEntries);
      await request(app).put(url).send({ type: "rules", revision: state.revision, model: proposed }).expect(409);
    });
    // The evaluator retains a measured zero rather than confusing it with no data.
    const pure = evaluateModel(proposed, [{ gmId: f.ids.gm, name: "Synthetic employee", active: true }], [], [{
      id: randomUUID(), gmId: f.ids.gm, marketId: f.ids.market, questionId: sourceIds[0]!, section: "billa",
      date: "2026-08-03", numeric: 0, options: [], frequency: 8, chain: "Sparmarkt",
    }]);
    assert.equal(pure[0]!.pillars[0]!.metrics[0]!.automaticPoints, 0);
    await t.test("latest/once, weights, chains and frequency retain their meanings across allowed areas", () => {
      const sample = structuredClone(proposed), source = sample.pillars[0]!.metrics[0]!.sources[0]!;
      sample.pillars[0]!.metrics[0]!.sources = [source];
      const observations = ["standard", "flex", "billa"].map((section, i) => ({
        id: randomUUID(), gmId: f.ids.gm, marketId: f.ids.market, questionId: sourceIds[0]!, section,
        date: `2026-08-0${i + 1}`, numeric: [10, 50, 20][i]!, options: [], frequency: 8, chain: "Sparmarkt",
      }));
      const first = observations[0]!;
      observations.push({ ...first, id: randomUUID(), marketId: "ineligible-frequency", numeric: 999, frequency: 7 });
      observations.push({ ...first, id: randomUUID(), marketId: "ineligible-chain", numeric: 999, chain: "Other" });
      const calculate = () => evaluateModel(sample, [{ gmId: f.ids.gm, name: "Synthetic employee", active: true }], [], observations)[0]!.pillars[0]!.metrics[0]!.automaticPoints;
      assert.equal(calculate(), 40);
      source.counting = "once";
      assert.equal(calculate(), 100);
    });
  } finally { await f.pg.close(); }
});
