import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
import { installDashboardFixture } from "./lib/gm-dashboard-test-fixture.js";
import { createGmDashboardRouter } from "./routes/gm-dashboard.js";
import { createCampaignVisitExportIndexRouter } from "./routes/campaign-visit-export-index.js";
import { availabilityAnswerCategory } from "./gm-availability.shared.js";
import type { DashboardScope } from "./gm-dashboard.shared.js";
import { availabilityHttpFixture } from "../tests/gm-availability-fixture.js";

const scope: DashboardScope = { gmId: null, region: null, chain: null, marketId: null, stc: null };
const weeks = [
  { id: "kw38", label: "KW 38", shortLabel: "KW38", start: "2026-09-14", end: "2026-09-20" },
  { id: "kw39", label: "KW 39", shortLabel: "KW39", start: "2026-09-21", end: "2026-09-27" },
];
function appFor(database: Parameters<typeof createGmDashboardRouter>[0]) {
  const app = express(); app.use(express.json());
  app.use((req, res, next) => { if (req.headers.authorization !== "Bearer synthetic") { res.sendStatus(401); return; } next(); });
  app.use("/admin/gm-dashboard", createGmDashboardRouter(database));
  app.use("/admin", createCampaignVisitExportIndexRouter(database));
  return app;
}
test("real HTTP audit uses the visit week, latest answer and historical snapshots; read-only queries preserve every stored answer", async () => {
  const f = await praemienFixture();
  try {
    const fixture = await installDashboardFixture(f);
    await f.pg.exec("update visit_sessions set is_deleted=true");
    const late = await fixture.seedVisit({ when: "2026-09-21T08:00:00+02:00", startedAt: "2026-09-20T23:50:00+02:00", category: "Top" });
    await fixture.seedVisit({ when: "2026-09-21T01:00:00+02:00", startedAt: "2026-09-20T22:00:00Z", category: "Mediocre" }); // Monday in Vienna.
    const mixed = await fixture.seedVisit({ when: "2026-09-15T08:00:00+02:00", category: "Top", mixed: true });
    await f.pg.query("update visit_answers a set value_text='Bad',changed_at='2026-09-15T09:00:00Z',version=2 from visit_session_sections sec where a.visit_session_section_id=sec.id and sec.section='flex' and a.visit_session_id=$1", [mixed]);
    const invalid = await fixture.seedVisit({ when: "2026-09-15T08:00:00+02:00", category: "Top" });
    await f.pg.query(`insert into visit_answers(id,visit_session_id,visit_session_question_id,visit_session_section_id,question_id,value_text,is_valid,answer_status,changed_at,version)
      select $1,visit_session_id,visit_session_question_id,visit_session_section_id,question_id,'Top',false,'invalid','2026-09-15T10:00:00Z',2
      from visit_answers where visit_session_id=$2 and question_id=$3`, [randomUUID(), invalid, fixture.qAvailability]);
    const raw = await fixture.seedVisit({ when: "2026-09-15T08:00:00+02:00", category: "Top" });
    await f.pg.query(`update visit_answers set value_text=null,value_json='{"raw":{"sel":"Top","subs":["Bad"]}}' where visit_session_id=$1 and question_id=$2`, [raw, fixture.qAvailability]);
    await f.pg.query(`insert into visit_answer_options(visit_answer_id,option_role,option_value,order_index)
      select id,'sub','Bad',0 from visit_answers where visit_session_id=$1 and question_id=$2`, [raw, fixture.qAvailability]);
    await fixture.seedVisit({ when: "2026-09-15T08:00:00+02:00", category: "(3) = mittelmäßig, Verfügbarkeit gewährleistet" });
    await fixture.seedVisit({ when: "2026-09-15T08:00:00+02:00", category: "(5) = Verfügbarkeit schlecht, OOS vorhanden" });
    await fixture.seedVisit({ when: "2026-09-15T08:00:00+02:00", category: "unrecognized" });
    await fixture.seedVisit({ when: "2026-09-15T08:00:00+02:00", category: "Top", hidden: true });
    await fixture.seedVisit({ when: "2026-09-15T08:00:00+02:00", category: "Top", status: "draft" });
    const hidden = await fixture.seedVisit({ when: "2026-09-15T08:00:00+02:00", category: "Top" });
    await f.pg.query(`update visit_session_questions q set question_rules_snapshot=$1::jsonb
      from visit_session_sections sec where q.visit_session_section_id=sec.id and sec.visit_session_id=$2 and q.question_id=$3`,
      [JSON.stringify([{ triggerQuestionId: fixture.qPlacement, operator: "equals", triggerValue: "Ja", action: "hide", targetQuestionIds: [fixture.qAvailability] }]), hidden, fixture.qPlacement]);
    // Current catalog deletion must not erase recorded availability metadata.
    await f.pg.query("update question_bank_shared set is_deleted=true where id=$1", [fixture.qAvailability]);
    const snapshot = async () => (await f.pg.query(`select
      (select jsonb_agg(s order by id) from visit_sessions s) as visits,
      (select jsonb_agg(a order by id) from visit_answers a) as answers,
      (select jsonb_agg(q order by id) from visit_session_questions q) as questions`)).rows;
    const before = await snapshot();
    await f.pg.exec("set default_transaction_read_only=on");
    const app = appFor(f.database);
    const response = await request(app).post("/admin/gm-dashboard/query").set("Authorization", "Bearer synthetic")
      .send({ intervals: weeks, scope, includeAvailabilityAudit: true });
    assert.equal(response.status, 200, JSON.stringify(response.body));
    assert.deepEqual(response.body.points[0].availability.Cooler, { top: 2, mediocre: 1, bad: 2, total: 5, average: 50 });
    assert.deepEqual(response.body.points[1].availability.Cooler, { top: 0, mediocre: 1, bad: 0, total: 1, average: 50 });
    assert.equal(response.body.availabilityAudit.find((o: any) => o.sessionId === late)?.visitDate, "2026-09-20");
    assert.equal(response.body.availabilityAudit.find((o: any) => o.sessionId === late)?.startedAt, "2026-09-20T21:50:00.000Z");
    const reasons = new Set(response.body.availabilityAudit.map((o: any) => o.exclusion));
    for (const reason of ["invalid_answer", "hidden_by_chain", "hidden_by_rule", "duplicate_visit_question", "unrecognized_answer"]) assert.ok(reasons.has(reason), reason);
    const latest = response.body.availabilityAudit.find((o: any) => o.sessionId === invalid);
    assert.equal(latest.included, false); assert.equal(latest.version, 2);
    assert.equal(response.body.points[0].availabilityExpected, 7); // Valid five + invalid + unrecognized.
    assert.equal(response.body.points[0].availabilityAnswered, 5);
    const light = await request(app).post("/admin/gm-dashboard/query").set("Authorization", "Bearer synthetic").send({ intervals: weeks, scope });
    assert.equal(light.body.availabilityAudit, undefined); // No raw rows added to normal chart payloads.
    assert.deepEqual(light.body.points, response.body.points);
    assert.deepEqual(await snapshot(), before);
  } finally { await f.pg.close(); }
});
test("campaign export HTTP selects the same Vienna visit-date boundaries, even after a market is no longer assigned", async () => {
  const f = await praemienFixture();
  try {
    const fixture = await installDashboardFixture(f), campaign = randomUUID();
    await f.pg.exec("create table campaigns(id uuid primary key,is_deleted boolean default false); update visit_sessions set is_deleted=true");
    await f.pg.query("insert into campaigns(id) values($1)", [campaign]);
    const late = await fixture.seedVisit({ when: "2026-09-21T08:00:00+02:00", startedAt: "2026-09-20T23:59:00+02:00", category: "Top", mixed: true });
    const monday = await fixture.seedVisit({ when: "2026-09-21T08:00:00+02:00", startedAt: "2026-09-20T22:00:00Z", category: "Bad" });
    await f.pg.query("update visit_session_sections set campaign_id=$1 where visit_session_id in ($2,$3)", [campaign, late, monday]);
    await f.pg.exec("set default_transaction_read_only=on");
    const app = appFor(f.database);
    const get = (from: string, to: string) => request(app).get("/admin/campaigns/market-visit-export-index")
      .set("Authorization", "Bearer synthetic").query({ campaignIds: campaign, dateFrom: from, dateTo: to });
    const w38 = await get("2026-09-14", "2026-09-20");
    assert.equal(w38.status, 200, JSON.stringify(w38.body));
    assert.deepEqual(w38.body.visits.map((v: any) => v.sessionId), [late]); // Two sections still produce one visit.
    const w39 = await get("2026-09-21", "2026-09-27");
    assert.deepEqual(w39.body.visits.map((v: any) => v.sessionId), [monday]);
    assert.equal((await get("2026-02-30", "2026-03-01")).status, 400);
    assert.equal((await get("2026-09-20", "2026-09-14")).status, 400);
    assert.equal((await request(app).get("/admin/campaigns/market-visit-export-index").query({ campaignIds: campaign })).status, 401);
  } finally { await f.pg.close(); }
});
test("availability classification keeps selected ratings separate from sub-options and reports conflicts", () => {
  const answer = { answerStatus: "answered", isValid: true, valueText: null, valueJson: { raw: { sel: "Top", subs: ["Bad"] } },
    options: [{ optionRole: "sub", optionValue: "Bad", orderIndex: 0 }] };
  assert.deepEqual(availabilityAnswerCategory(answer), { category: "top", exclusion: null });
  assert.deepEqual(availabilityAnswerCategory({ ...answer, valueText: "Bad" }), { category: null, exclusion: "conflicting_answer" });
  assert.deepEqual(availabilityAnswerCategory({ ...answer, isValid: false }), { category: null, exclusion: "invalid_answer" });
});

test("actual campaign index, assigned history, batch details and individual fallback preserve orphaned-from-catalog visits read-only", async () => {
  const f = await availabilityHttpFixture();
  try {
    const get = (path: string) => request(f.app).get(path).set("Authorization", "Bearer synthetic-availability");
    const before = (await f.pg.query("select jsonb_agg(a order by id) as answers from visit_answers a")).rows;
    const campaignIds = [f.ids.campaign, f.ids.secondCampaign].join(",");
    const assigned = await get("/admin/campaigns/assigned-markets").query({ campaignIds });
    assert.deepEqual(assigned.body.markets, []);
    const history = await get("/admin/campaigns/assigned-markets").query({ campaignIds, includeSubmittedHistory: true });
    assert.equal(history.status, 200, JSON.stringify(history.body));
    assert.equal(history.body.markets.length, 4);
    const index = await get("/admin/campaigns/market-visit-export-index").query({ campaignIds, dateFrom: weeks[0]!.start, dateTo: weeks[0]!.end });
    assert.equal(index.status, 200, JSON.stringify(index.body));
    const targets = index.body.visits.filter((v: any) => v.campaignId === f.ids.campaign);
    assert.equal(targets.length, 6);
    const details = await request(f.app).post(`/admin/campaigns/${f.ids.campaign}/market-visits/export-details`)
      .set("Authorization", "Bearer synthetic-availability").send({ visits: targets.map((v: any) => ({ marketId: v.marketId, sessionId: v.sessionId })) });
    assert.equal(details.status, 200, JSON.stringify(details.body));
    assert.equal(details.body.markets.length, 6);
    const first = details.body.markets.find((v: any) => v.sessionId === f.visits[0]!.session);
    assert.equal(first.sections[0].questions[0].singleChoiceAvailabilityType, "Cooler");
    assert.equal(first.sections[0].questions[0].text, "Historical Cooler");
    assert.equal(first.sections[0].questions[0].answer.changedAt, "2026-09-21T06:00:00.000Z");
    const fallback = await get(`/admin/campaigns/${f.ids.campaign}/markets/${f.ids.market}/visit-detail`).query({ sessionId: f.visits[0]!.session, includePhotoSignedUrls: false });
    assert.equal(fallback.status, 200, JSON.stringify(fallback.body));
    assert.equal(fallback.body.market.sessionId, f.visits[0]!.session);
    // A real spaced Billa Corso label is correctly included only in REWE.
    const rewe = await request(f.app).post("/admin/gm-dashboard/query").set("Authorization", "Bearer synthetic-availability")
      .send({ intervals: weeks, scope: { ...scope, chainGroups: ["rewe"] }, includeAvailabilityAudit: true });
    assert.equal(rewe.status, 200, JSON.stringify(rewe.body));
    assert.deepEqual(rewe.body.points[0].availability.Cooler, { top: 1, mediocre: 2, bad: 1, total: 4, average: 50 });
    assert.equal(rewe.body.points[1].availability.Cooler.total, 1);
    assert.deepEqual(await f.pg.query("show default_transaction_read_only").then(r => r.rows), [{ default_transaction_read_only: "on" }]);
    assert.deepEqual((await f.pg.query("select jsonb_agg(a order by id) as answers from visit_answers a")).rows, before);
  } finally { await f.pg.close(); }
});
