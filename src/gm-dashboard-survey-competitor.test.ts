import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { aggregateDashboard, type DashboardObservation } from "./lib/gm-dashboard.js";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
import { installDashboardFixture } from "./lib/gm-dashboard-test-fixture.js";
import { createGmDashboardRouter } from "./routes/gm-dashboard.js";

const scope = { region: null, gmId: null, chain: null, marketId: null, stc: null };
const intervals = [{ id: "sep", label: "September", shortLabel: "Sep", start: "2026-09-01", end: "2026-09-30" }];
const observation = (sessionId: string, extra: Partial<DashboardObservation> = {}): DashboardObservation => ({
  intervalId: "sep", sessionId, marketId: sessionId, gmId: "gm", questionId: "survey",
  submittedAt: "2026-09-25", changedAt: "2026-09-25T08:00:00Z", answerId: sessionId,
  section: "standard", duration: 10, available: false, availabilityType: null,
  red: true, answered: true, category: "Ja", selectedAnswer: "Ja", ipp: null, placements: null, competitor: null,
  ...extra,
});

test("RED counts only the latest explicit Ja, once per visit, never Nein, availability or sub-options", () => {
  const rows = [
    observation("changed"), observation("changed", { selectedAnswer: "Nein", changedAt: "2026-09-25T09:00:00Z" }),
    observation("no", { selectedAnswer: "Nein" }),
    observation("availability", { selectedAnswer: "Top" }),
    observation("unanswered", { answered: false }),
    observation("unmarked", { red: false }),
    observation("yes"), observation("yes", { questionId: "second-survey", section: "flex" }),
    observation("sub-only", { selectedAnswer: null }),
    observation("trimmed", { selectedAnswer: " JA " }),
  ];
  const point = aggregateDashboard(intervals, rows, scope).points[0]!;
  assert.equal(point.visits, 8);
  assert.equal(point.redSurveys, 2);
});

test("HTTP + isolated PostgreSQL: RED answer storage formats, eligibility and read-only queries", async () => {
  const f = await praemienFixture();
  try {
    const fixture = await installDashboardFixture(f);
    await f.pg.exec("update visit_sessions set is_deleted=true");
    const cases = [
      { text: "Ja", raw: null, yes: true },
      { text: null, raw: "Ja", yes: true },
      { text: null, raw: { sel: "Ja", subs: ["Brand"] }, yes: true },
      { text: null, raw: null, option: "Ja", role: "top", yes: true },
      { text: "Nein", raw: null, option: "Ja", role: "top", yes: false },
      { text: null, raw: { sel: "Nein", subs: ["Ja"] }, yes: false },
      { text: null, raw: null, option: "Ja", role: "sub", yes: false },
      { text: "Top", raw: null, yes: false },
      { text: null, raw: null, yes: false },
      { text: "Ja", raw: null, valid: false, yes: false },
      { text: "Ja", raw: null, applies: false, yes: false },
      { text: "Ja", raw: null, red: false, yes: false },
      { text: "Ja", raw: null, draft: true, yes: false },
      { text: "Ja", raw: null, deleted: true, yes: false },
    ];
    for (const c of cases) {
      const session = await fixture.seedVisit({ when: "2026-09-25T08:00:00+02:00", category: "Top", weight: "Nein", status: "draft" in c ? "draft" : "submitted", deleted: "deleted" in c });
      await f.pg.query("update visit_session_questions set red_survey_snapshot=(question_id=$2 and $3),applies_to_market_chain_snapshot=$4 where visit_session_section_id in (select id from visit_session_sections where visit_session_id=$1)", [session, fixture.qPlacement, !("red" in c), !("applies" in c)]);
      const answer = (await f.pg.query<{ id: string }>("select id from visit_answers where visit_session_id=$1 and question_id=$2", [session, fixture.qPlacement])).rows[0]!.id;
      await f.pg.query("update visit_answer_options set is_deleted=true where visit_answer_id=$1", [answer]);
      await f.pg.query("update visit_answers set value_text=$2,value_json=$3::jsonb,is_valid=$4 where id=$1", [answer, c.text, c.raw === null ? null : JSON.stringify({ raw: c.raw }), !("valid" in c)]);
      if ("option" in c) await f.pg.query("insert into visit_answer_options(visit_answer_id,option_value,option_role) values($1,$2,$3)", [answer, c.option, c.role]);
    }
    const snapshot = async () => Promise.all(["visit_sessions", "visit_session_questions", "visit_answers", "visit_answer_options", "question_scoring"].map(async (table) => (await f.pg.query(`select jsonb_agg(to_jsonb(t) order by id) as rows from ${table} t`)).rows));
    const before = await snapshot();
    const app = express(); app.use(express.json()); app.use(createGmDashboardRouter(f.database));
    const result = await request(app).post("/query").send({ intervals, scope });
    assert.equal(result.status, 200, JSON.stringify(result.body));
    assert.equal(result.body.points[0].visits, 12);
    assert.equal(result.body.points[0].redSurveys, cases.filter((c) => c.yes).length);
    assert.deepEqual(await snapshot(), before);
  } finally { await f.pg.close(); }
});

test("competitor detail keeps cooler and large-placement questions separate, with latest market answers and configured points", async () => {
  const f = await praemienFixture();
  try {
    const fixture = await installDashboardFixture(f);
    await f.pg.exec("update visit_sessions set is_deleted=true; update question_scoring set mitbewerberabfrage=null");
    const market = randomUUID();
    await f.pg.query("insert into markets(id,db_name) values($1,'Billa')", [market]);
    const questions = [
      { id: randomUUID(), text: "Ist ein markeneigener Kühler vorhanden?", weight: 2 },
      { id: randomUUID(), text: "Sind Großplatzierungen vom Mitbewerb vorhanden?", weight: 5 },
    ];
    for (const q of questions) {
      await f.pg.query("insert into question_bank_shared(id,text,question_type) values($1,'Aktuell umbenannte Frage','single_choice')", [q.id]);
      await f.pg.query("insert into question_scoring(question_id,score_key,mitbewerberabfrage) values($1,'Ja',$2),($1,'Nein',0)", [q.id, q.weight]);
    }
    for (const [i, answers] of [["Ja", "Nein"], ["Nein", "Ja"], ["Ja", "Ja"]].entries()) {
      const session = await fixture.seedVisit({ when: `2026-09-${24 + i}T08:00:00+02:00`, category: "Top", market: i === 2 ? market : f.ids.market });
      const section = (await f.pg.query<{ id: string }>("select id from visit_session_sections where visit_session_id=$1", [session])).rows[0]!.id;
      for (const [j, q] of questions.entries()) {
        const instance = randomUUID();
        await f.pg.query("insert into visit_session_questions(id,visit_session_section_id,question_id,question_text_snapshot,module_name_snapshot) values($1,$2,$3,$4,'Abfrage Mitbewerb')", [instance, section, q.id, q.text]);
        await f.pg.query("insert into visit_answers(id,visit_session_id,visit_session_section_id,visit_session_question_id,question_id,value_text,changed_at) values($1,$2,$3,$4,$5,$6,$7)", [randomUUID(), session, section, instance, q.id, answers[j], `2026-09-${24 + i}T08:00:00+02:00`]);
      }
    }
    const app = express(); app.use(express.json()); app.use(createGmDashboardRouter(f.database));
    const result = await request(app).post("/query").send({ intervals, scope });
    assert.equal(result.status, 200, JSON.stringify(result.body));
    const point = result.body.points[0];
    assert.equal(point.competitor, 12);
    const cooler = point.competitorQuestions.find((q: { questionId: string }) => q.questionId === questions[0]!.id);
    const large = point.competitorQuestions.find((q: { questionId: string }) => q.questionId === questions[1]!.id);
    assert.deepEqual(cooler, { questionId: questions[0]!.id, questionText: questions[0]!.text, moduleName: "Abfrage Mitbewerb", points: 2, marketCount: 2, yesCount: 1, noCount: 1 });
    assert.equal(large.points, 10); assert.equal(large.yesCount, 2); assert.equal(large.noCount, 0);
    const filtered = await request(app).post("/query").send({ intervals, scope: { ...scope, chains: ["Billa"] } });
    assert.equal(filtered.body.points[0].competitor, 7);
    assert.equal(filtered.body.points[0].competitorQuestions.length, 2);
    assert.equal(filtered.body.points[0].competitorQuestions[0].marketCount, 1);
  } finally { await f.pg.close(); }
});
