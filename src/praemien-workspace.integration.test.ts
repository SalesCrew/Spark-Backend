import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
import { createPraemienWorkspaceRouter } from "./routes/praemien-workspace.js";
import {
  managedGmSummary,
  mutateWorkspace,
  readWorkspace,
  simulateWorkspace,
  managedCumulative,
  managedQuarterQuestionIds,
} from "./lib/praemien-workspace.js";
import {
  evaluateModel,
  modelTemplate,
  validateModel,
  type Observation,
} from "./praemien-model.shared.js";

test("real PostgreSQL migration + HTTP save + GM result + zero/clear + conflict + frozen history", async () => {
  const f = await praemienFixture();
  try {
    const app = express();
    app.use(express.json());
    app.use((req, res, next) => {
      if (req.headers.authorization !== "Bearer local-admin") {
        res.status(401).end();
        return;
      }
      (req as any).authUser = { appUserId: f.ids.admin, role: "admin" };
      next();
    });
    app.use(
      "/admin/praemien/workspace",
      createPraemienWorkspaceRouter(f.database),
    );
    const url = `/admin/praemien/workspace/waves/${f.ids.wave}`;
    assert.equal((await request(app).get(url)).status, 401);
    const initial = await request(app)
      .get(url)
      .set("Authorization", "Bearer local-admin");
    assert.equal(initial.status, 200);
    assert.equal(initial.body.results.length, 3);
    assert.equal(initial.body.results[0].earned, 907.5);
    let revision = initial.body.revision;
    const value = {
      gmId: f.ids.gm,
      pillarKey: "displays",
      metricKey: "percent",
      value: 80,
      target: null,
      note: "Prozent, nicht Punkte",
    };
    const saved = await request(app)
      .put(url)
      .set("Authorization", "Bearer local-admin")
      .send({ revision, type: "values", entries: [value] });
    assert.equal(saved.status, 200);
    revision = saved.body.revision;
    assert.equal(
      saved.body.results.find((r: any) => r.gmId === f.ids.gm).pillars[0]
        .earned,
      440,
    );
    const summary = await managedGmSummary(f.database, f.ids.wave, f.ids.gm);
    assert.equal(summary.goals[0]!.metricValues.percent, 80);
    assert.equal(summary.currentRewardEur, 907.5);
    assert.equal(summary.goals[0]!.earnedRewardEur, 440);
    const beforeCreate = (
      await f.pg.query<{ count: string }>(
        "select count(*)::text as count from praemien_waves",
      )
    ).rows[0]!.count;
    const invalidCopy = structuredClone(f.model);
    invalidCopy.pillars[0]!.maxRewardEur = 1;
    const failedCreation = await request(app)
      .post("/admin/praemien/workspace/waves")
      .set("Authorization", "Bearer local-admin")
      .send({
        name: "Ungültiger Entwurf",
        year: 2027,
        quarter: 1,
        template: "empty",
        model: invalidCopy,
      });
    assert.equal(failedCreation.status, 400);
    assert.equal(
      (
        await f.pg.query<{ count: string }>(
          "select count(*)::text as count from praemien_waves",
        )
      ).rows[0]!.count,
      beforeCreate,
      "Invalid rules roll back the entire new wave",
    );
    const copied = await request(app)
      .post("/admin/praemien/workspace/waves")
      .set("Authorization", "Bearer local-admin")
      .send({
        name: "Kopie",
        year: 2027,
        quarter: 1,
        template: "empty",
        model: f.model,
      });
    assert.equal(copied.status, 201);
    assert.equal(copied.body.entries.length, 0);
    assert.equal(copied.body.results.length, 3);
    assert.equal(
      copied.body.results.every((r: any) => r.pending),
      true,
    );
    const proposed = await simulateWorkspace(f.database, f.ids.wave, f.model, [
      { ...value, value: 95 },
    ]);
    assert.equal(
      proposed.results.find((r) => r.gmId === f.ids.gm)!.earned,
      1017.5,
    );
    assert.equal(
      (await managedGmSummary(f.database, f.ids.wave, f.ids.gm))
        .currentRewardEur,
      907.5,
    );
    assert.equal(
      (await managedCumulative(f.database, [f.ids.gm])).totals.get(f.ids.gm),
      undefined,
      "Draft does not contribute to GM cumulative bonus",
    );
    assert.deepEqual(
      (await readWorkspace(f.database, f.ids.wave, f.ids.gm)).results.map(
        (r) => r.gmId,
      ),
      [f.ids.gm],
    );
    const stale = await request(app)
      .put(url)
      .set("Authorization", "Bearer local-admin")
      .send({
        revision: revision - 1,
        type: "values",
        entries: [{ ...value, value: 99 }],
      });
    assert.equal(stale.status, 409);
    let w = await mutateWorkspace(
      f.database,
      f.ids.wave,
      revision,
      { id: f.ids.admin, name: "Lokal Admin" },
      { type: "values", entries: [{ ...value, value: 0 }] },
    );
    assert.equal(
      w.results.find((r) => r.gmId === f.ids.gm)!.pillars[0]!.metrics[0]!.value,
      0,
    );
    assert.equal(
      w.results.find((r) => r.gmId === f.ids.gm)!.pillars[0]!.pending,
      false,
    );
    w = await mutateWorkspace(
      f.database,
      f.ids.wave,
      w.revision,
      { id: f.ids.admin, name: "Lokal Admin" },
      { type: "values", entries: [{ ...value, value: null }] },
    );
    assert.equal(
      w.results.find((r) => r.gmId === f.ids.gm)!.pillars[0]!.pending,
      true,
    );
    w = await mutateWorkspace(
      f.database,
      f.ids.wave,
      w.revision,
      { id: f.ids.admin, name: "Lokal Admin" },
      { type: "activate" },
    );
    await assert.rejects(
      mutateWorkspace(
        f.database,
        f.ids.wave,
        w.revision,
        { id: f.ids.admin, name: "Lokal Admin" },
        { type: "archive" },
      ),
      /fehlen/,
    );
    w = await mutateWorkspace(
      f.database,
      f.ids.wave,
      w.revision,
      { id: f.ids.admin, name: "Lokal Admin" },
      { type: "values", entries: [value] },
    );
    const preview = await simulateWorkspace(f.database, f.ids.wave, {
      ...f.model,
      provenance: "Simulation",
    });
    assert.equal(preview.revision, w.revision);
    assert.equal(
      (await readWorkspace(f.database, f.ids.wave)).model!.provenance,
      f.model.provenance,
    );
    w = await mutateWorkspace(
      f.database,
      f.ids.wave,
      w.revision,
      { id: f.ids.admin, name: "Lokal Admin" },
      { type: "archive" },
    );
    const frozen = structuredClone(w.results);
    assert.equal(w.history[0]!.type, "archive");
    assert.equal(
      (await managedCumulative(f.database, [f.ids.gm])).totals.get(f.ids.gm),
      907.5,
    );
    await f.pg.query(
      `update users set first_name='Umbenannt',is_active=false where id=$1`,
      [f.ids.gm],
    );
    assert.deepEqual(
      (await readWorkspace(f.database, f.ids.wave)).results,
      frozen,
    );
    await assert.rejects(
      mutateWorkspace(
        f.database,
        f.ids.wave,
        w.revision,
        { id: f.ids.admin, name: "Lokal Admin" },
        { type: "values", entries: [value] },
      ),
      /eingefroren/,
    );
    assert.equal(
      (
        await request(app)
          .get("/admin/praemien/workspace/leaderboard")
          .set("Authorization", "Bearer local-admin")
      ).body.results.length,
      3,
    );
    const policy = await f.pg.query<{ relrowsecurity: boolean }>(
      `select relrowsecurity from pg_class where relname='praemien_metric_entries'`,
    );
    assert.equal(policy.rows[0]!.relrowsecurity, true);
    await f.pg.exec("set role authenticated");
    await assert.rejects(
      f.pg.query("select * from praemien_metric_entries"),
      /permission denied/,
    );
    await f.pg.exec("reset role");
  } finally {
    await f.pg.close();
  }
});

test("legacy totals stay readable without converting points into percentages", async () => {
  const f = await praemienFixture();
  try {
    const oldWave = randomUUID();
    await f.pg.exec(`create table praemien_gm_wave_totals (
      wave_id uuid,gm_user_id uuid,current_reward_eur numeric(12,2),total_points numeric(14,4)
    )`);
    await f.pg.query(
      `insert into praemien_waves(id,name,year,quarter,status,start_date,end_date,reward_model)
       values($1,'Historischer Altstand',2026,1,'archived','2026-01-01','2026-03-31','pillar_tiers')`,
      [oldWave],
    );
    await f.pg.query(
      `insert into praemien_gm_wave_totals values($1,$2,82.50,25)`,
      [oldWave, f.ids.gm],
    );
    const w = await readWorkspace(f.database, oldWave);
    assert.equal(w.model, null);
    assert.equal(w.results.length, 0);
    assert.deepEqual(w.legacyTotals, [
      { gmId: f.ids.gm, name: "GM Test Nord", earned: 82.5, totalPoints: 25 },
    ]);
    assert.equal(
      (await readWorkspace(f.database, oldWave, f.ids.gm)).legacyTotals,
      undefined,
    );
  } finally {
    await f.pg.close();
  }
});

test("quarter deduplication, >=8, source denominator, latest rather than edited older visit", async () => {
  const f = await praemienFixture();
  try {
    await f.pg.exec("delete from praemien_metric_entries");
    const model = modelTemplate("empty"),
      p = model.pillars[0]!;
    p.maxRewardEur = 86.5;
    p.kind = "distribution";
    p.metrics = [
      {
        key: "raw",
        label: "Racks",
        unit: "count",
        method: "answer_sum",
        inputs: [],
        target: null,
        steps: [],
        sources: [
          {
            questionId: f.ids.question,
            section: "flex",
            scoreKey: "__value__",
            weight: 1,
            factor: true,
            label: "Racks",
            minFrequency: 8,
            chains: [],
            counting: "latest",
          },
        ],
      },
      {
        key: "percent",
        label: "Erreichung",
        unit: "percent",
        method: "ratio",
        inputs: ["raw"],
        target: 10,
        sources: [],
        steps: [],
      },
    ];
    assert.equal(
      evaluateModel(
        model,
        [{ gmId: f.ids.gm, name: "Ohne Daten", active: true }],
        [],
        [],
      )[0]!.pillars[0]!.metrics[0]!.value,
      null,
      "Missing source observations stay open",
    );
    p.tiers = [
      {
        key: "tier_80",
        label: "80 %",
        group: "",
        rewardEur: 86.5,
        conditions: [{ metricKey: "percent", operator: "gte", value: 80 }],
      },
    ];
    let w = await mutateWorkspace(
      f.database,
      f.ids.wave,
      f.workspace.revision,
      { id: f.ids.admin, name: "Lokal Admin" },
      { type: "rules", model },
    );
    for (const [date, value] of [
      ["2026-07-15T12:00:00Z", 10],
      ["2026-09-10T12:00:00Z", 8],
    ] as const) {
      const sid = randomUUID(),
        qid = randomUUID(),
        section = randomUUID(),
        aid = randomUUID();
      await f.pg.query(
        `insert into visit_sessions(id,gm_user_id,market_id,status,submitted_at) values($1,$2,$3,'submitted',$4)`,
        [sid, f.ids.gm, f.ids.market, date],
      );
      await f.pg.query("insert into visit_session_questions(id) values($1)", [
        qid,
      ]);
      await f.pg.query(
        `insert into visit_session_sections(id,section) values($1,'flex')`,
        [section],
      );
      await f.pg.query(
        `insert into visit_answers(id,visit_session_id,visit_session_question_id,visit_session_section_id,question_id,value_number) values($1,$2,$3,$4,$5,$6)`,
        [aid, sid, qid, section, f.ids.question, value],
      );
    }
    w = await readWorkspace(f.database, f.ids.wave);
    assert.deepEqual(
      await managedQuarterQuestionIds(
        f.database,
        [f.ids.wave],
        [f.ids.question],
      ),
      [f.ids.question],
    );
    const receiptsBefore = (
      await f.pg.query(
        "select visit_session_id,received_at::text as at from praemien_visit_receipts order by visit_session_id",
      )
    ).rows;
    assert.equal(receiptsBefore.length, 2);
    await f.pg.exec("update visit_sessions set status='submitted'");
    assert.deepEqual(
      (
        await f.pg.query(
          "select visit_session_id,received_at::text as at from praemien_visit_receipts order by visit_session_id",
        )
      ).rows,
      receiptsBefore,
      "Receipt does not change when a submitted visit is edited",
    );
    const r = w.results.find((r) => r.gmId === f.ids.gm)!;
    assert.equal(r.earned, 86.5);
    assert.equal(r.pillars[0]!.metrics[0]!.value, 8);
    assert.equal(r.pillars[0]!.metrics[0]!.excluded, 1);
    await f.pg.query(
      "update visit_answers set value_number=100 where value_number=10",
    );
    assert.equal(
      (await readWorkspace(f.database, f.ids.wave)).results.find(
        (r) => r.gmId === f.ids.gm,
      )!.pillars[0]!.metrics[0]!.value,
      8,
    );
    model.pillars[0]!.metrics[1]!.target = null;
    assert.equal(
      (await simulateWorkspace(f.database, f.ids.wave, model)).results.find(
        (r) => r.gmId === f.ids.gm,
      )!.pending,
      true,
    );
  } finally {
    await f.pg.close();
  }
});

test("templates, AND gates, grouped quality (no double payment), missing vs 0, max and unit checks", () => {
  for (const template of ["q1", "q2", "q3"] as const)
    assert.deepEqual(validateModel(modelTemplate(template)), []);
  const m = modelTemplate("q2"),
    gm = { gmId: "gm", name: "GM", active: true };
  const entry = (pillarKey: string, metricKey: string, value: number) => ({
    gmId: "gm",
    pillarKey,
    metricKey,
    value,
    target: null,
    note: "",
  });
  let r = evaluateModel(
    m,
    [gm],
    [
      entry("flex", "new_coolers", 10),
      entry("flex", "returned", 0),
      entry("flex", "red_ir", 0),
    ],
    [],
  )[0]!;
  assert.equal(r.pillars[2]!.earned, 0);
  r = evaluateModel(
    m,
    [gm],
    [
      entry("flex", "new_coolers", 3),
      entry("flex", "returned", 0),
      entry("flex", "red_ir", 85),
    ],
    [],
  )[0]!;
  assert.equal(r.pillars[2]!.earned, 165);
  assert.equal(r.pillars[3]!.pending, true);
  const p = m.pillars[3]!;
  p.tiers = [
    {
      key: "half",
      group: "time",
      label: "Zeit halb",
      rewardEur: 55,
      conditions: [{ metricKey: "time", operator: "gte", value: 80 }],
    },
    {
      key: "full",
      group: "time",
      label: "Zeit voll",
      rewardEur: 110,
      conditions: [{ metricKey: "time", operator: "gte", value: 90 }],
    },
  ];
  r = evaluateModel(m, [gm], [entry("quality", "time", 95)], [])[0]!;
  assert.equal(r.pillars[3]!.earned, 110);
  assert.equal(r.pillars[3]!.pending, false);
  p.maxRewardEur = 100;
  assert.ok(validateModel(m).some((e) => e.includes("Maximalprämie")));
});
