import assert from "node:assert/strict";
import test from "node:test";
import request from "supertest";
import { createSmDashboardFiltersFixture } from "../tests/sm-dashboard-filters-fixture.js";

test("SM admin dashboard facets and every result use the same standard-market scope", async t => {
  const f = await createSmDashboardFiltersFixture();
  const get = async (query: Record<string, string> = {}, token = "synthetic-sm-admin") => (await request(f.app).get("/admin/sm-dashboard").auth(token, { type: "bearer" }).query({ from: "2026-10-01", to: "2026-10-15", ...query }).expect(200)).body;
  const ids = (options: Array<{ value: string }>) => options.map(option => option.value).sort();
  const snapshot = async () => (await f.pg.query(`select jsonb_build_object(
    'markets', (select jsonb_agg(to_jsonb(t) order by id) from sm_markets t),
    'registry', (select jsonb_agg(to_jsonb(t) order by sm_market_id) from sm_smdurcharbeit_markets t),
    'assignments', (select jsonb_agg(to_jsonb(t) order by id) from sm_assignments t),
    'submissions', (select jsonb_agg(to_jsonb(t) order by id) from sm_questionnaire_submissions t),
    'questions', (select jsonb_agg(to_jsonb(t) order by id) from sm_questionnaire_submission_questions t),
    'answers', (select jsonb_agg(to_jsonb(t) order by id) from sm_question_answers t),
    'answerOptions', (select jsonb_agg(to_jsonb(t) order by id) from sm_question_answer_options t)) as snapshot`)).rows[0];
  try {
    const before = await snapshot();
    await t.test("default excludes Durcharbeit-only markets but keeps all three shared-market cases", async () => {
      const data = await get();
      assert.equal(data.meta.filters.SMDurcharbeitCatalogScope, "standard");
      assert.equal(data.summary.completedVisits, 4);
      assert.equal(data.overview.activity.visits, 4); assert.equal(data.overview.activity.visitedMarkets, 4); assert.equal(data.overview.activity.merchandisers, 2);
      assert.equal(data.overview.activity.marketHours, 2); assert.equal(data.overview.activity.hoursPerVisit, 0.5);
      assert.equal(data.overview.performance.categoryChecks, 4); assert.equal(data.overview.performance.availabilityGain, 50); assert.equal(data.overview.performance.availabilityAfter, 75);
      assert.equal(data.overview.performance.basicActivityRate, null, "unmapped historical answers must not be reclassified");
      assert.equal(data.summary.foundCases, 3);
      assert.equal(data.summary.fixedCases, 2);
      assert.equal(data.summary.foundRate, 75);
      assert.equal(data.summary.fixedRate, 2 / 3 * 100);
      assert.deepEqual(ids(data.filterOptions.markets), Object.entries(f.markets).filter(([key]) => key !== "dedicated").map(([, id]) => id).sort());
      assert.ok(!ids(data.filterOptions.chains).includes("Nur DA"));
      assert.ok(!ids(data.filterOptions.regions).includes("Nur DA"));
      assert.deepEqual(ids(data.filterOptions.sms), [f.employee, f.second].sort());
      assert.equal(data.SMDurcharbeitBreakdown.SMDurcharbeit.completedVisits, 0);
      assert.deepEqual(data.chains.map((row: any) => row.id).sort(), ["Billa", "Spar"]);
      const energy = data.categories.find((row: any) => row.category === "softdrinks_energy");
      assert.equal(energy.foundCases, 3); assert.equal(energy.fixedCases, 2);
    });
    await t.test("combined filters scope cards, categories, chain/region tables and facets together", async () => {
      const data = await get({ region: "Nord", chain: "Billa", smUserId: f.employee, marketId: f.markets.northBilla });
      assert.equal(data.overview.activity.visits, 1); assert.equal(data.overview.activity.merchandisers, 1); assert.equal(data.overview.activity.marketHours, 0.5);
      assert.equal(data.overview.performance.categoryChecks, 1); assert.equal(data.overview.performance.availabilityGain, 100);
      assert.equal(data.summary.completedVisits, 1); assert.equal(data.summary.foundCases, 1); assert.equal(data.summary.fixedRate, 100);
      assert.equal(data.categories.find((row: any) => row.category === "softdrinks_energy").foundCases, 1);
      assert.equal(data.chains.length, 1); assert.equal(data.chains[0].id, "Billa");
      assert.equal(data.regions.length, 1); assert.equal(data.regions[0].id, "Nord");
      assert.deepEqual(ids(data.filterOptions.markets), [f.markets.northBilla, f.markets.unvisited].sort());
      assert.deepEqual(ids(data.filterOptions.sms), [f.employee, f.second].sort());
    });
    await t.test("SM membership includes the original submitter alongside today's directory owner", async () => {
      const data = await get({ marketId: f.markets.northBilla });
      assert.deepEqual(ids(data.filterOptions.sms), [f.employee, f.second].sort());
      assert.equal((await get({ marketId: f.markets.northBilla, smUserId: f.employee })).summary.completedVisits, 1);
      assert.equal((await get({ marketId: f.markets.northBilla, smUserId: f.second })).summary.completedVisits, 0);
      const oldPeriod = await get({ from: "2026-09-01", to: "2026-09-30", marketId: f.markets.northBilla });
      assert.deepEqual(ids(oldPeriod.filterOptions.sms), [f.second]);
    });
    await t.test("empty periods retain unvisited directory choices and genuine shared Stammmärkte", async () => {
      const data = await get({ from: "2026-11-01", to: "2026-11-30" });
      assert.equal(data.summary.completedVisits, 0);
      assert.equal(data.summary.foundRate, null);
      assert.ok(ids(data.filterOptions.markets).includes(f.markets.sharedHistory));
      assert.ok(ids(data.filterOptions.markets).includes(f.markets.sharedPlan));
      assert.ok(ids(data.filterOptions.markets).includes(f.markets.sharedWeekly));
      assert.ok(ids(data.filterOptions.markets).includes(f.markets.unvisited));
      assert.ok(!ids(data.filterOptions.markets).includes(f.markets.dedicated));
    });
    await t.test("date bounds restrict actual visitors and results, with an inclusive last day", async () => {
      const first = await get({ from: "2026-10-05", to: "2026-10-05" });
      assert.equal(first.summary.completedVisits, 1);
      assert.equal(first.summary.fixedRate, 100);
      assert.equal((await get({ from: "2026-10-06", to: "2026-10-08" })).summary.completedVisits, 3);
      const byPerson = await get({ smUserId: f.employee });
      assert.deepEqual(ids(byPerson.filterOptions.regions), ["Nord"]);
      assert.ok(!ids(byPerson.filterOptions.markets).includes(f.markets.southBilla));
    });
    await t.test("the explicit Durcharbeit API scope remains available without leaking into standard", async () => {
      const data = await get({ SMDurcharbeitCatalogScope: "SMDurcharbeit" });
      assert.equal(data.summary.completedVisits, 2);
      assert.equal((await get({ marketId: f.markets.dedicated })).summary.completedVisits, 0);
    });
    await t.test("authentication and invalid-filter validation remain enforced", async () => {
      for (const token of ["synthetic-sm", "synthetic-gm", "synthetic-gm-admin"]) await request(f.app).get("/admin/sm-dashboard").auth(token, { type: "bearer" }).expect(403);
      await request(f.app).get("/admin/sm-dashboard").expect(401);
      for (const query of [{ from: "2026-10-15", to: "2026-10-01" }, { marketId: "invalid" }, { smUserId: "invalid" }]) await request(f.app).get("/admin/sm-dashboard").auth("synthetic-sm-admin", { type: "bearer" }).query(query).expect(400);
      assert.equal((await get({}, "synthetic-admin")).summary.completedVisits, 4);
    });
    assert.deepEqual(await snapshot(), before, "dashboard GETs must not change even synthetic business records");
  } finally { await f.pg.close(); }
});
