import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
import { installDashboardFixture } from "./lib/gm-dashboard-test-fixture.js";
import type { DashboardScope } from "./gm-dashboard.shared.js";
import { canUseWholeGmIpp } from "./lib/gm-dashboard.js";
import { createGmDashboardRouter } from "./routes/gm-dashboard.js";

const scope: DashboardScope = { region: null, gmId: null, chain: null, marketId: null, stc: null };
const intervals = [
  { id: "sep", label: "September", shortLabel: "Sep", start: "2026-09-01", end: "2026-09-30" },
  { id: "empty", label: "Oktober", shortLabel: "Okt", start: "2026-10-01", end: "2026-10-31" },
];

test("STC HTTP + isolated PostgreSQL: inclusive configured frequency ranges, gaps, combined filters and read-only results", async () => {
  const f = await praemienFixture();
  try {
    const fixture = await installDashboardFixture(f);
    // Only synthetic in-memory rows are cleared; no env/database clients imported.
    await f.pg.exec("update visit_sessions set is_deleted=true");
    const markets = new Map<number | null, string>();
    const chains = new Map([[6, "Billa"], [7, "Spar"], [8, "Billa"], [9, "Spar"], [10, "Lidl"], [11, "Billa"], [12, "Billa"], [13, "Spar"], [23, "Billa+"], [24, "Lidl"]]);
    for (const frequency of [null, -1, 0, 5, 6, 7, 8, 9, 10, 11, 12, 13, 23, 24, 25]) {
      const market = randomUUID();
      markets.set(frequency, market);
      await f.pg.query("insert into markets(id,visit_frequency_per_year,db_name,region) values($1,$2,$3,$4)", [market, frequency, chains.get(frequency ?? -1) ?? "Testkette", frequency === 13 ? "Süd" : "Nord"]);
      await fixture.seedVisit({ market, gm: frequency === 13 ? f.ids.other : f.ids.gm, when: "2026-09-25T08:00:00+02:00", category: "Top" });
    }
    // A second actual visit must not change a market's planned STC classification.
    await fixture.seedVisit({ market: markets.get(6)!, when: "2026-09-26T08:00:00+02:00", category: "Leer", mixed: true });
    const app = express();
    app.use(express.json());
    app.use("/dashboard", createGmDashboardRouter(f.database, async (_intervals, selectedScope, data) => {
      // Model the existing whole-GM cached IPP fallback without importing the app.
      if (canUseWholeGmIpp(selectedScope)) data.points[0]!.ipp = 999;
    }));
    const query = async (filters: Partial<DashboardScope> = {}) => {
      const response = await request(app).post("/dashboard/query").send({ intervals, scope: { ...scope, ...filters } });
      assert.equal(response.status, 200, JSON.stringify(response.body));
      return response.body;
    };
    const snapshot = async () => Promise.all(["markets", "users", "visit_sessions", "visit_session_sections", "visit_session_questions", "visit_answers", "visit_answer_options", "question_scoring"].map(async table => (await f.pg.query(`select jsonb_agg(to_jsonb(t) order by id) as data from ${table} t`)).rows));
    const before = await snapshot();
    const all = await query();
    assert.equal(all.stcApplied, false);
    assert.equal(all.points[0].visits, 16); // Unclassified frequencies remain visible with no STC selected.
    for (const [stc, visits, scoredMarkets, average] of [
      ["gold", 4, 4, 100], ["silver", 3, 3, 100], ["bronze", 3, 2, 66.6667],
    ] as const) {
      const data = await query({ stc });
      const p = data.points[0];
      assert.equal(data.stcApplied, true);
      assert.equal(data.scope.stc, stc);
      assert.equal(p.visits, visits);
      assert.equal(p.redSurveys, visits);
      assert.equal(p.availability.Cooler.total, visits);
      assert.equal(p.availability.Cooler.average, average);
      assert.equal(p.placements, scoredMarkets * 2);
      assert.equal(p.competitor, scoredMarkets * 3);
      assert.equal(p.ippMarketCount, scoredMarkets);
      assert.equal(p.ipp, 2); // STC must never be replaced by the whole-GM 999 fallback.
      assert.equal(p.standardOnly, stc === "bronze" ? 2 : visits);
      assert.equal(p.mixed, stc === "bronze" ? 1 : 0);
      assert.equal(data.points[1].visits, 0);
      assert.equal(data.points[1].ipp, null);
      assert.equal(data.points[1].availability.Cooler.average, null);
    }
    for (const [frequency, stc] of [[6, "bronze"], [7, "bronze"], [8, "silver"], [10, "silver"], [12, "gold"], [24, "gold"]] as const) {
      const p = (await query({ stc, marketId: markets.get(frequency)! })).points[0];
      assert.equal(p.visits, frequency === 6 ? 2 : 1, `${stc} boundary ${frequency}`);
    }
    for (const frequency of [null, -1, 0, 5, 11, 25]) {
      for (const stc of ["gold", "silver", "bronze"] as const) {
        assert.equal((await query({ stc, marketId: markets.get(frequency)! })).points[0].visits, 0, `${frequency} is unclassified`);
      }
    }
    assert.equal((await query({ stc: "gold", chainGroups: ["rewe"] })).points[0].visits, 2);
    assert.equal((await query({ stc: "gold", chains: ["Billa", "Spar"], region: "Nord", gmId: f.ids.gm })).points[0].visits, 1);
    assert.equal((await query({ stc: "gold", marketIds: [markets.get(12)!, markets.get(8)!] })).points[0].visits, 1);
    assert.equal((await query({ stc: "gold", region: "Süd", gmId: f.ids.other })).points[0].visits, 1);
    assert.equal((await query({ stc: "gold", marketId: markets.get(6)! })).points[0].visits, 0);
    const invalid = await request(app).post("/dashboard/query").send({ intervals, scope: { ...scope, stc: "platinum" } });
    assert.equal(invalid.status, 400);
    assert.deepEqual(await snapshot(), before);
  } finally {
    await f.pg.close();
  }
});
