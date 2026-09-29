import assert from "node:assert/strict";
import test from "node:test";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { visitAnswerReuseFixture } from "./lib/visit-answer-reuse-test-fixture.js";
import { copyReusableAnswer, loadReusableSubmittedAnswers } from "./lib/visit-answer-reuse.js";
import { modelTemplate } from "./praemien-model.shared.js";
import { createPraemienWorkspaceRouter } from "./routes/praemien-workspace.js";

type Fixture = Awaited<ReturnType<typeof visitAnswerReuseFixture>>;

// The RED-period resolver is injected; these synthetic periods deliberately
// change between July/August/September, while the production quarter is fixed.
const redPeriod = async (at: Date) => ({
  start: new Date(at.getFullYear(), at.getMonth(), 1),
  end: new Date(at.getFullYear(), at.getMonth() + 1, 0),
});
const input = (f: Fixture, questionIds: string[], date = "2026-09-20T12:00:00Z") => ({
  gmUserId: f.ids.gm, marketId: f.ids.market, questionIds, now: new Date(date),
});

async function newVisit(f: Fixture, at: string, gm = f.ids.gm, market = f.ids.market) {
  const id = randomUUID(), sectionId = randomUUID();
  await f.pg.query("insert into visit_sessions(id,gm_user_id,market_id,status,submitted_at) values($1,$2,$3,'draft',null)", [id, gm, market]);
  await f.pg.query("insert into visit_session_sections(id,section) values($1,'flex')", [sectionId]);
  return { id, sectionId, at };
}

async function numericAnswer(f: Fixture, visit: Awaited<ReturnType<typeof newVisit>>, questionId: string, value: number, comment?: string) {
  const visitQuestionId = randomUUID(), answerId = randomUUID();
  await f.pg.query("insert into visit_session_questions(id) values($1)", [visitQuestionId]);
  await f.pg.query(
    "insert into visit_answers(id,visit_session_id,visit_session_section_id,visit_session_question_id,question_id,question_type,answer_status,value_number,value_json) values($1,$2,$3,$4,$5,'numeric','answered',$6,$7)",
    [answerId, visit.id, visit.sectionId, visitQuestionId, questionId, value, JSON.stringify({ raw: value })],
  );
  if (comment) await f.pg.query("insert into visit_question_comments(visit_session_question_id,comment_text) values($1,$2)", [visitQuestionId, comment]);
  return { visitQuestionId, answerId };
}

async function submit(f: Fixture, visit: Awaited<ReturnType<typeof newVisit>>) {
  await f.pg.query("update visit_sessions set status='submitted',submitted_at=$2 where id=$1", [visit.id, visit.at]);
}

function adminApp(f: Fixture) {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    (req as any).authUser = { appUserId: f.ids.admin, role: "admin" };
    next();
  });
  app.use("/admin/praemien/workspace", createPraemienWorkspaceRouter(f.database));
  return app;
}

test("linked Flex sources: HTTP configuration → July answer → editable August carry → September carry → bonus without double count → October reset", async () => {
  const f = await visitAnswerReuseFixture();
  try {
    const app = adminApp(f), url = `/admin/praemien/workspace/waves/${f.ids.wave}`;
    await f.pg.exec("delete from praemien_metric_entries"); // Remove synthetic template evaluations before replacing its metrics.
    const e3 = randomUUID(), unlinked = randomUUID();
    await f.pg.query("insert into question_bank_shared(id,text,question_type) values($1,'E3','numeric'),($2,'Nicht verknüpft','numeric')", [e3, unlinked]);
    const model = modelTemplate("empty"), pillar = model.pillars[0]!;
    pillar.kind = "flex";
    pillar.name = "Kühler & RED/IR"; // Kind, not the display name or visit section.
    pillar.maxRewardEur = 86.5;
    pillar.metrics = [
      ...[{ key: "racks", questionId: f.ids.question, weight: 3 }, { key: "e3", questionId: e3, weight: 5 }].map((m) => ({
        key: m.key, label: m.key, unit: "points" as const, method: "answer_sum" as const,
        inputs: [], target: null, steps: [],
        sources: [{ questionId: m.questionId, section: "flex", scoreKey: "__value__", weight: m.weight, factor: true, label: m.key, minFrequency: 0, chains: [], counting: "latest" as const }],
      })),
      { key: "total", label: "Punkte", unit: "points", method: "sum", inputs: ["racks", "e3"], target: null, sources: [], steps: [] },
    ];
    pillar.tiers = [{ key: "test", label: "Synthetische Teststufe", group: "", rewardEur: 86.5, conditions: [{ metricKey: "total", operator: "gte", value: 14 }] }];
    const configured = await request(app).put(url).send({ revision: f.workspace.revision, type: "rules", model });
    assert.equal(configured.status, 200, JSON.stringify(configured.body));

    const july = await newVisit(f, "2026-07-15T12:00:00Z");
    const original = await numericAnswer(f, july, f.ids.question, 1, "Rack bleibt stehen");
    await numericAnswer(f, july, e3, 1);
    await numericAnswer(f, july, unlinked, 7);
    await submit(f, july);
    const augustInput = input(f, [f.ids.question, e3, unlinked], "2026-08-15T12:00:00Z");
    const carried = await loadReusableSubmittedAnswers(f.visitDb, augustInput, redPeriod);
    assert.equal(carried.get(f.ids.question)?.reuseScope, "calendar-quarter");
    assert.equal(Number(carried.get(f.ids.question)?.answer.valueNumber), 1);
    assert.equal(carried.get(f.ids.question)?.commentText, "Rack bleibt stehen");
    assert.equal(carried.has(unlinked), false, "A Flex visit does not opt every question into quarterly reuse");

    const august = await newVisit(f, "2026-08-15T12:00:00Z");
    const augustQuestions = new Map<string, string>();
    for (const questionId of [f.ids.question, e3]) {
      const visitQuestionId = randomUUID();
      augustQuestions.set(questionId, visitQuestionId);
      await f.pg.query("insert into visit_session_questions(id) values($1)", [visitQuestionId]);
      assert.equal(await f.visitDb.transaction((tx) => copyReusableAnswer(tx, {
        sessionId: august.id, sectionId: august.sectionId, visitQuestionId,
        question: { questionId, type: "numeric", config: {} }, now: augustInput.now,
      }, carried.get(questionId)!)), true);
    }
    // The GM edits the NEW draft's cumulative total, leaving the July visit intact.
    await f.pg.query("update visit_answers set value_number=3,value_json=$2 where visit_session_question_id=$1", [augustQuestions.get(f.ids.question), JSON.stringify({ raw: 3 })]);
    const sourceRow = (await f.pg.query<{ value_number: string }>("select value_number from visit_answers where id=$1", [original.answerId])).rows[0]!;
    assert.equal(Number(sourceRow.value_number), 1);
    await submit(f, august);

    const septemberInput = input(f, [f.ids.question, e3, unlinked]);
    const septemberCarry = await loadReusableSubmittedAnswers(f.visitDb, septemberInput, redPeriod);
    assert.equal(Number(septemberCarry.get(f.ids.question)?.answer.valueNumber), 3);
    const september = await newVisit(f, "2026-09-20T12:00:00Z");
    for (const questionId of [f.ids.question, e3]) {
      const visitQuestionId = randomUUID();
      await f.pg.query("insert into visit_session_questions(id) values($1)", [visitQuestionId]);
      await copyReusableAnswer(f.visitDb, {
        sessionId: september.id, sectionId: september.sectionId, visitQuestionId,
        question: { questionId, type: "numeric", config: {} }, now: septemberInput.now,
      }, septemberCarry.get(questionId)!);
    }
    await submit(f, september);
    const response = await request(app).get(url).expect(200);
    const result = response.body.results.find((r: { gmId: string }) => r.gmId === f.ids.gm);
    const values = result.pillars[0].metrics;
    assert.equal(values.find((m: { key: string }) => m.key === "racks").value, 9, "1 → 3 → 3 is three racks, weighted by 3, not seven racks");
    assert.equal(values.find((m: { key: string }) => m.key === "e3").value, 5);
    assert.equal(values.find((m: { key: string }) => m.key === "total").value, 14);
    assert.equal(result.earned, 86.5);
    assert.equal((await f.pg.query("select * from visit_question_comments where visit_session_question_id=$1", [augustQuestions.get(f.ids.question)])).rows.length, 1);

    // Keep the wave configured across Q4 to prove the boundary is calendar-based.
    await f.pg.query("update praemien_waves set end_date='2026-12-31' where id=$1", [f.ids.wave]);
    assert.equal((await loadReusableSubmittedAnswers(f.visitDb, input(f, [f.ids.question, e3], "2026-09-30T21:59:59Z"), redPeriod)).size, 2);
    assert.equal((await loadReusableSubmittedAnswers(f.visitDb, input(f, [f.ids.question, e3], "2026-09-30T22:00:00Z"), redPeriod)).size, 0);
  } finally { await f.pg.close(); }
});

test("existing legacy Flex linkage works without resaving; invalid/draft/deleted/other-GM/other-market sources do not leak", async () => {
  const f = await visitAnswerReuseFixture();
  try {
    const pillar = randomUUID(), source = randomUUID();
    await f.pg.query("insert into praemien_wave_pillars(id,wave_id,name,carry_answers_for_wave) values($1,$2,'Flexziel',false)", [pillar, f.ids.wave]);
    await f.pg.query("insert into praemien_wave_sources(id,wave_id,pillar_id,question_id,section_type,score_key) values($1,$2,$3,$4,'flex','__value__')", [source, f.ids.wave, pillar, f.ids.question]);
    const july = await newVisit(f, "2026-07-15T12:00:00Z");
    await numericAnswer(f, july, f.ids.question, 3);
    await submit(f, july);
    for (const reason of ["invalid", "unanswered", "deleted-answer", "deleted-session", "draft", "other-gm", "other-market"] as const) {
      const visit = await newVisit(f, "2026-08-15T12:00:00Z", reason === "other-gm" ? f.ids.other : f.ids.gm, reason === "other-market" ? randomUUID() : f.ids.market);
      const answer = await numericAnswer(f, visit, f.ids.question, 999);
      if (reason !== "draft") await submit(f, visit);
      if (reason === "invalid") await f.pg.query("update visit_answers set is_valid=false where id=$1", [answer.answerId]);
      if (reason === "unanswered") await f.pg.query("update visit_answers set answer_status='unanswered' where id=$1", [answer.answerId]);
      if (reason === "deleted-answer") await f.pg.query("update visit_answers set is_deleted=true where id=$1", [answer.answerId]);
      if (reason === "deleted-session") await f.pg.query("update visit_sessions set is_deleted=true where id=$1", [visit.id]);
    }
    assert.equal(Number((await loadReusableSubmittedAnswers(f.visitDb, input(f, [f.ids.question]), redPeriod)).get(f.ids.question)?.answer.valueNumber), 3);
    assert.equal((await loadReusableSubmittedAnswers(f.visitDb, input(f, [randomUUID()]), redPeriod)).size, 0);
    const newVisitRow = await newVisit(f, "2026-09-20T12:00:00Z");
    const visitQuestionId = randomUUID();
    await f.pg.query("insert into visit_session_questions(id) values($1)", [visitQuestionId]);
    const carried = (await loadReusableSubmittedAnswers(f.visitDb, input(f, [f.ids.question]), redPeriod)).get(f.ids.question)!;
    assert.equal(await copyReusableAnswer(f.visitDb, {
      sessionId: newVisitRow.id, sectionId: newVisitRow.sectionId, visitQuestionId,
      question: { questionId: f.ids.question, type: "yesno", config: {} }, now: new Date(),
    }, carried), false, "A changed question type is not silently inherited");
    assert.equal((await f.pg.query("select * from visit_answers where visit_session_question_id=$1", [visitQuestionId])).rows.length, 0);
    for (const query of [
      "update praemien_wave_sources set is_deleted=true",
      "update praemien_wave_sources set is_deleted=false; update praemien_wave_pillars set is_deleted=true",
      "update praemien_wave_pillars set is_deleted=false; update praemien_waves set status='archived'",
      "update praemien_waves set status='draft',is_deleted=true",
    ]) {
      await f.pg.exec(query);
      assert.equal((await loadReusableSubmittedAnswers(f.visitDb, input(f, [f.ids.question]), redPeriod)).size, 0);
    }
  } finally { await f.pg.close(); }
});

test("unlinked questions still reuse only their RED month, including photos as unanswered historical references", async () => {
  const f = await visitAnswerReuseFixture();
  try {
    const august = await newVisit(f, "2026-08-15T12:00:00Z");
    await numericAnswer(f, august, f.ids.question, 4);
    await submit(f, august);
    assert.equal((await loadReusableSubmittedAnswers(f.visitDb, input(f, [f.ids.question]), redPeriod)).size, 0);
    const september = await newVisit(f, "2026-09-15T12:00:00Z");
    const answer = await numericAnswer(f, september, f.ids.question, 5);
    await submit(f, september);
    const map = await loadReusableSubmittedAnswers(f.visitDb, input(f, [f.ids.question]), redPeriod);
    assert.equal(map.get(f.ids.question)?.reuseScope, "red-month");
    assert.equal(Number(map.get(f.ids.question)?.answer.valueNumber), 5);
    await f.pg.query("update visit_answers set question_type='photo',value_json=$2 where id=$1", [answer.answerId, JSON.stringify({ storage: ["old.jpg"] })]);
    const photo = (await loadReusableSubmittedAnswers(f.visitDb, input(f, [f.ids.question]), redPeriod)).get(f.ids.question)!;
    const draft = await newVisit(f, "2026-09-20T12:00:00Z"), visitQuestionId = randomUUID();
    await f.pg.query("insert into visit_session_questions(id) values($1)", [visitQuestionId]);
    await copyReusableAnswer(f.visitDb, {
      sessionId: draft.id, sectionId: draft.sectionId, visitQuestionId,
      question: { questionId: f.ids.question, type: "photo", config: {} }, now: new Date(),
    }, photo);
    const newAnswer = (await f.pg.query<{ answer_status: string; value_json: unknown }>("select answer_status,value_json from visit_answers where visit_session_question_id=$1", [visitQuestionId])).rows[0]!;
    assert.equal(newAnswer.answer_status, "unanswered");
    assert.deepEqual(newAnswer.value_json, { storage: [] });
    assert.equal(await copyReusableAnswer(f.visitDb, {
      sessionId: draft.id, sectionId: draft.sectionId, visitQuestionId: randomUUID(),
      question: { questionId: f.ids.question, type: "photo", config: {} }, now: new Date(),
    }, { ...photo, reuseScope: "calendar-quarter" }), false);
  } finally { await f.pg.close(); }
});

test("Distribution and Flex keep validated option/matrix answers and comments in new draft rows", async () => {
  const f = await visitAnswerReuseFixture();
  try {
    for (const [name, type, raw, config] of [
      ["Distributionsziel", "yesnomulti", { sel: "Ja", subs: ["Produkt A"] }, { answers: ["Ja", "Nein"], branches: [{ answer: "Ja", options: ["Produkt A"] }] }],
      ["Flexziel", "matrix", ["Rack::Top"], { rows: ["Rack"], columns: ["Top", "Bad"] }],
    ] as const) {
      const questionId = randomUUID(), pillarId = randomUUID();
      await f.pg.query("insert into praemien_wave_pillars(id,wave_id,name,carry_answers_for_wave) values($1,$2,$3,true)", [pillarId, f.ids.wave, name]);
      await f.pg.query("insert into praemien_wave_sources(wave_id,pillar_id,question_id,section_type,score_key) values($1,$2,$3,'flex','__value__')", [f.ids.wave, pillarId, questionId]);
      const july = await newVisit(f, "2026-07-15T12:00:00Z");
      const old = await numericAnswer(f, july, questionId, 1, "Übernommener Kommentar");
      await f.pg.query("update visit_answers set question_type=$2,value_json=$3 where id=$1", [old.answerId, type, JSON.stringify({ raw })]);
      await submit(f, july);
      const source = (await loadReusableSubmittedAnswers(f.visitDb, input(f, [questionId]), redPeriod)).get(questionId)!;
      const draft = await newVisit(f, "2026-09-20T12:00:00Z"), visitQuestionId = randomUUID();
      await f.pg.query("insert into visit_session_questions(id) values($1)", [visitQuestionId]);
      assert.equal(await copyReusableAnswer(f.visitDb, {
        sessionId: draft.id, sectionId: draft.sectionId, visitQuestionId,
        question: { questionId, type, config }, now: new Date(),
      }, source), true);
      const saved = (await f.pg.query<{ id: string; answer_status: string }>("select id,answer_status from visit_answers where visit_session_question_id=$1", [visitQuestionId])).rows[0]!;
      assert.equal(saved.answer_status, "answered");
      assert.notEqual(saved.id, old.answerId);
      assert.equal((await f.pg.query<{ comment_text: string }>("select comment_text from visit_question_comments where visit_session_question_id=$1", [visitQuestionId])).rows[0]!.comment_text, "Übernommener Kommentar");
      if (type === "yesnomulti") {
        assert.deepEqual((await f.pg.query<{ option_value: string }>("select option_value from visit_answer_options where visit_answer_id=$1 order by order_index", [saved.id])).rows.map((r) => r.option_value), ["Ja", "Produkt A"]);
      } else {
        assert.deepEqual((await f.pg.query("select row_key,column_key,cell_selected from visit_answer_matrix_cells where visit_answer_id=$1", [saved.id])).rows, [{ row_key: "Rack", column_key: "Top", cell_selected: true }]);
      }
    }
  } finally { await f.pg.close(); }
});
