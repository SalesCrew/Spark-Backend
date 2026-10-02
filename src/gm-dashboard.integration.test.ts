import test from "node:test";
import assert from "node:assert/strict";
import express from "express";
import request from "supertest";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
import { installDashboardFixture } from "./lib/gm-dashboard-test-fixture.js";
import { loadDashboard, dashboardFacets, availabilityCategory } from "./lib/gm-dashboard.js";
import { createGmDashboardRouter } from "./routes/gm-dashboard.js";
import type { DashboardScope } from "./gm-dashboard.shared.js";

const scope: DashboardScope = {
  gmId: null,
  region: null,
  chain: null,
  marketId: null,
  stc: null,
};
const months = [
  {
    id: "aug",
    label: "August",
    shortLabel: "Aug",
    start: "2026-08-01",
    end: "2026-08-31",
  },
  {
    id: "sep",
    label: "September",
    shortLabel: "Sep",
    start: "2026-09-01",
    end: "2026-09-30",
  },
  {
    id: "oct",
    label: "Oktober",
    shortLabel: "Okt",
    start: "2026-10-01",
    end: "2026-10-31",
  },
  {
    id: "empty",
    label: "Juli",
    shortLabel: "Jul",
    start: "2026-07-01",
    end: "2026-07-31",
  },
];
test("real local PostgreSQL + HTTP: answer weights, all-visit means/splits, dedup, filters, dates, read-only", async () => {
  const f = await praemienFixture();
  try {
    const fixture = await installDashboardFixture(f);
    const before = (
      await f.pg.query(`select count(*)::int as n from visit_sessions`)
    ).rows[0];
    const app = express();
    app.use(express.json());
    app.use((req, res, next) => {
      if (req.headers.authorization !== "Bearer local") {
        res.status(401).end();
        return;
      }
      next();
    });
    app.use("/admin/gm-dashboard", createGmDashboardRouter(f.database));
    const query = () =>
      request(app)
        .post("/admin/gm-dashboard/query")
        .set("Authorization", "Bearer local");
    assert.equal(
      (
        await request(app)
          .post("/admin/gm-dashboard/query")
          .send({ intervals: months, scope })
      ).status,
      401,
    );
    const result = await query().send({ intervals: months, scope });
    assert.equal(result.status, 200, JSON.stringify(result.body));
    const [aug, sep, oct, empty] = result.body.points;
    assert.equal(aug.availability.Cooler.average, 50); // Top+Bad, not latest Top=100.
    assert.deepEqual(sep.availability.Cooler, {
      top: 2,
      mediocre: 1,
      bad: 1,
      total: 4,
      average: 62.5,
    });
    assert.equal(sep.visits, 4);
    assert.equal(sep.redSurveys, 4);
    assert.equal(sep.mixed, 1);
    assert.equal(sep.standardOnly, 3);
    assert.equal(sep.averageMinutes, 45);
    assert.equal(sep.placements, 2);
    assert.equal(sep.competitor, 3);
    assert.equal(sep.ipp, 2);
    assert.equal(sep.availabilityAnswered, 4);
    assert.equal(sep.availabilityExpected, 4);
    assert.equal(oct.visits, 1);
    assert.equal(oct.availability.Cooler.average, 100); // Vienna October midnight NOT September.
    assert.equal(empty.ipp, null);
    assert.equal(empty.placements, null);
    assert.equal(empty.availability.Cooler.average, null);
    assert.equal(empty.visits, 0);
    const gm = await query().send({
      intervals: months,
      scope: { ...scope, gmId: f.ids.gm },
    });
    assert.equal(gm.body.points[1].visits, 2);
    assert.equal(gm.body.points[1].availability.Cooler.average, 75);
    const inactive = await query().send({
      intervals: months,
      scope: { ...scope, gmId: f.ids.inactive },
    });
    assert.equal(inactive.body.points[1].visits, 1);
    assert.equal(inactive.body.points[1].availability.Cooler.average, 0);
    const missing = await query().send({
      intervals: months,
      scope: { ...scope, chain: "Billa" },
    });
    assert.equal(missing.body.points[1].visits, 0);
    const stc = await query().send({
      intervals: months,
      scope: { ...scope, stc: "gold" },
    });
    assert.equal(stc.body.stcApplied, true);
    assert.equal(stc.body.points[1].visits, 0); // Fixture market frequency is 8, not Gold.
    const silver = await query().send({
      intervals: months,
      scope: { ...scope, stc: "silver" },
    });
    assert.equal(silver.body.stcApplied, true);
    assert.deepEqual(silver.body.points, result.body.points);
    assert.equal(result.body.stcApplied, false);
    const facets = await request(app)
      .get("/admin/gm-dashboard/facets")
      .set("Authorization", "Bearer local");
    assert.equal(facets.status, 200);
    assert.equal(facets.body.firstEntryDate, "2026-08-18");
    assert.equal(facets.body.markets.length, 1);
    assert.match(
      facets.body.gms.find((gm: any) => gm.id === f.ids.inactive).label,
      /inaktiv/,
    );
    assert.equal(
      (
        await query().send({
          intervals: [{ ...months[0], start: "2026-02-30" }],
          scope,
        })
      ).status,
      400,
    );
    assert.equal(
      (
        await query().send({
          intervals: months,
          scope: { ...scope, gmId: "SQL injection" },
        })
      ).status,
      400,
    );
    assert.equal(
      (await query().send({ intervals: [months[0], months[0]], scope })).status,
      400,
    );
    assert.equal(
      (
        await query().send({
          intervals: [{ ...months[0], start: "2020-01-01" }],
          scope,
        })
      ).status,
      400,
    );
    assert.deepEqual(
      (await f.pg.query(`select count(*)::int as n from visit_sessions`))
        .rows[0],
      before,
    );
    // Changed intervals trend upward; invalid/hidden/deleted answers never enter it.
    await fixture.seedVisit({
      when: "2026-09-25T08:00:00+02:00",
      category: "Top",
      invalid: true,
    });
    await fixture.seedVisit({
      when: "2026-09-26T08:00:00+02:00",
      category: "Top",
      hidden: true,
    });
    const filtered = await loadDashboard(f.database, months, scope);
    assert.equal(filtered.points[1]!.availability.Cooler.total, 4);
    assert.equal(filtered.points[1]!.availabilityExpected, 5);
    assert.equal(filtered.points[1]!.placements, 2);
    await f.pg.query(
      `update question_scoring set zweitplatzierung=-2 where question_id=$1 and score_key='Ja'`,
      [fixture.qPlacement],
    );
    assert.equal(
      (await loadDashboard(f.database, months, scope)).points[1]!.placements,
      -2,
    );
    // Facets are not silently truncated to the first Supabase REST page.
    await f.pg.exec(
      `insert into markets(id,name) select gen_random_uuid(),'Lokaler Test ' || i from generate_series(1,1001) i`,
    );
    const allMarkets = await request(app)
      .get("/admin/gm-dashboard/facets")
      .set("Authorization", "Bearer local");
    assert.equal(allMarkets.body.markets.length, 1002);
    // Raw object answer selection, no normalized options, must use configured keys.
    await f.pg.query(
      `update visit_answers set value_text=null,value_json='{"raw":{"sel":"Ja","subs":[]}}' where question_id=$1`,
      [fixture.qPlacement],
    );
    await f.pg.query(`delete from visit_answer_options`);
    assert.equal(
      (await loadDashboard(f.database, months, scope)).points[1]!.placements,
      -2,
    );
    await f.pg.query(
      `update visit_answers set value_json='{"raw":"Nein"}' where question_id=$1`,
      [fixture.qPlacement],
    );
    const zero = (await loadDashboard(f.database, months, scope)).points[1]!;
    assert.equal(zero.placements, 0);
    assert.equal(zero.ipp, 0); // real zero != missing.
  } finally {
    await f.pg.close();
  }
});

test("first visible entry comes from submitted history in Vienna, not creation dates/drafts/deleted visits; empty history is null", async () => {
  const f = await praemienFixture();
  try {
    const fixture = await installDashboardFixture(f);
    await fixture.seedVisit({ when: "2024-01-01T08:00:00Z", category: "Top", status: "draft" });
    await fixture.seedVisit({ when: "2024-02-01T08:00:00Z", category: "Top", deleted: true });
    await fixture.seedVisit({ when: "2099-01-01T08:00:00Z", category: "Top" });
    assert.equal((await dashboardFacets(f.database)).firstEntryDate, "2026-08-18");
    await fixture.seedVisit({ when: "2026-07-05T22:15:00Z", category: "Bad" });
    const before = (await f.pg.query(`select count(*)::int as n from visit_sessions`)).rows[0];
    assert.equal((await dashboardFacets(f.database)).firstEntryDate, "2026-07-06");
    assert.deepEqual((await f.pg.query(`select count(*)::int as n from visit_sessions`)).rows[0], before);
    await f.pg.exec(`update visit_sessions set is_deleted=true`);
    assert.equal((await dashboardFacets(f.database)).firstEntryDate, null);
  } finally {
    await f.pg.close();
  }
});
test("availability aliases and no-data semantics", () => {
  assert.equal(availabilityCategory("Sehr voll"), "top");
  assert.equal(availabilityCategory("Halbvoll"), "mediocre");
  assert.equal(availabilityCategory("nicht voll"), "bad");
  assert.equal(availabilityCategory("Ja"), null);
});

test("annual HTTP query retains older history and distinguishes empty months (no last-three-month limit)", async () => {
  const f = await praemienFixture();
  try {
    const fixture = await installDashboardFixture(f);
    await fixture.seedVisit({ when: "2026-01-06T08:00:00+01:00", category: "Bad" });
    await fixture.seedVisit({ when: "2026-04-06T08:00:00+02:00", category: "Top" });
    const intervals = Array.from({ length: 9 }, (_, i) => {
      const month = String(i + 1).padStart(2, "0");
      const last = String(new Date(Date.UTC(2026, i + 1, 0)).getUTCDate());
      return { id: month, label: month, shortLabel: month, start: `2026-${month}-01`, end: `2026-${month}-${last}` };
    });
    const before = (await f.pg.query(`select count(*)::int as n from visit_sessions`)).rows[0];
    const app = express();
    app.use(express.json());
    app.use("/admin/gm-dashboard", createGmDashboardRouter(f.database));
    const response = await request(app).post("/admin/gm-dashboard/query").send({ intervals, scope });
    assert.equal(response.status, 200, JSON.stringify(response.body));
    assert.equal(response.body.points.length, 9);
    const [jan, feb, , apr, , , jul, aug, sep] = response.body.points;
    assert.equal(jan.visits, 1);
    assert.equal(jan.availability.Cooler.average, 0); // Bad is real data, not missing.
    assert.equal(apr.visits, 1);
    assert.equal(apr.availability.Cooler.average, 100);
    assert.equal(feb.visits, 0);
    assert.equal(feb.ipp, null);
    assert.equal(feb.availability.Cooler.average, null);
    assert.equal(jul.visits, 0);
    assert.equal(aug.visits, 2);
    assert.equal(sep.visits, 4);
    assert.deepEqual((await f.pg.query(`select count(*)::int as n from visit_sessions`)).rows[0], before);
  } finally {
    await f.pg.close();
  }
});

test("small metadata read matches legacy facets and authenticated HTTP stays private/no-store without changing rows", async () => {
  const f = await praemienFixture();
  try {
    await installDashboardFixture(f);
    const app = express();
    app.use((req, res, next) => {
      if (req.headers.authorization !== "Bearer local") { res.sendStatus(401); return; }
      next();
    });
    app.use("/admin/gm-dashboard", createGmDashboardRouter(f.database));
    assert.equal((await request(app).get("/admin/gm-dashboard/metadata")).status, 401);
    const before = await f.pg.query("select * from visit_sessions order by id");
    const metadata = await request(app).get("/admin/gm-dashboard/metadata").set("Authorization", "Bearer local");
    assert.equal(metadata.status, 200);
    assert.equal(metadata.headers["cache-control"], "private, no-store");
    assert.deepEqual(metadata.body, { firstEntryDate: (await dashboardFacets(f.database)).firstEntryDate });
    assert.deepEqual(await f.pg.query("select * from visit_sessions order by id"), before);
  } finally { await f.pg.close(); }
});
