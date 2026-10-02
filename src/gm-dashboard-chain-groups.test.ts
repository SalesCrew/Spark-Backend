import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
import { installDashboardFixture } from "./lib/gm-dashboard-test-fixture.js";
import { canUseWholeGmIpp } from "./lib/gm-dashboard.js";
import { createGmDashboardRouter } from "./routes/gm-dashboard.js";
import type { DashboardScope, DashboardChainGroup } from "./gm-dashboard.shared.js";

const scope: DashboardScope = { region: null, gmId: null, chain: null, marketId: null, stc: null };
const intervals = [{ id: "sep", label: "September", shortLabel: "Sep", start: "2026-09-01", end: "2026-09-30" }];

test("HTTP + PostgreSQL: multi-chain and multi-market unions, exact mapping, combined filters, validation and legacy requests", async () => {
  const f = await praemienFixture();
  try {
    const fixture = await installDashboardFixture(f);
    await f.pg.exec("update visit_sessions set is_deleted=true");
    const chains = ["Billa", " bIlLa + ", "Billa Plus", "ISP", "ESP", "Spar", " SPAR ", "Hofer", "Billa Corso", "REWE Zentrallager", "Spar Zentrallager", "", null];
    const markets: string[] = [];
    for (const [i, chain] of chains.entries()) {
      const market = randomUUID(); markets.push(market);
      await f.pg.query("insert into markets(id,db_name,name,region) values($1,$2,$3,$4)", [market, chain, `Markt ${i}`, i === 0 ? "Süd" : "Nord"]);
      await fixture.seedVisit({ market, when: "2026-09-25T08:00:00+02:00", category: i < 5 ? "Top" : i < 7 ? "Mittel" : "Leer", gm: i === 0 ? f.ids.other : f.ids.gm, weight: i < 5 ? "Ja" : "Nein" });
    }
    // Deleted and draft visits never enter any selected group.
    await fixture.seedVisit({ market: markets[0], when: "2026-09-25T08:00:00+02:00", category: "Top", deleted: true });
    await fixture.seedVisit({ market: markets[0], when: "2026-09-25T08:00:00+02:00", category: "Top", status: "draft" });
    const before = (await f.pg.query("select count(*)::int as n from visit_sessions")).rows[0];
    const app = express(); app.use(express.json()); app.use(createGmDashboardRouter(f.database));
    const query = (extra: Partial<DashboardScope> = {}) => request(app).post("/query").send({ intervals, scope: { ...scope, ...extra } });
    const all = await query(); assert.equal(all.status, 200); assert.equal(all.body.points[0].visits, 13);
    const expected: [DashboardChainGroup[], number, number][] = [
      [["rewe"], 5, 100], [["spar"], 2, 50], [["other"], 6, 0],
      [["rewe", "spar"], 7, 600 / 7], [["spar", "other"], 8, 12.5],
      [["rewe", "other"], 11, 500 / 11], [["rewe", "spar", "other"], 13, 600 / 13], [[], 13, 600 / 13],
    ];
    for (const [chainGroups, visits, average] of expected) {
      const result = await query({ chainGroups }); assert.equal(result.status, 200, JSON.stringify(result.body));
      const p = result.body.points[0];
      assert.equal(p.visits, visits); assert.equal(p.redSurveys, chainGroups.length === 0 || chainGroups.includes("rewe") ? 5 : 0); assert.equal(p.availability.Cooler.total, visits);
      assert.equal(p.availability.Cooler.average, Math.round(average * 10000) / 10000);
      assert.equal(p.placements, chainGroups.length === 0 || chainGroups.includes("rewe") ? 10 : 0);
      assert.equal(p.competitor, visits * 3);
      assert.equal(p.ipp, chainGroups.length === 0 || chainGroups.includes("rewe") ? 2 : 0);
      assert.deepEqual(result.body.scope.chainGroups, chainGroups);
    }
    assert.equal((await query({ chainGroups: ["rewe", "rewe"] })).body.points[0].visits, 5);
    assert.equal((await query({ chainGroups: ["rewe", "spar"], region: "Süd" })).body.points[0].visits, 1);
    assert.equal((await query({ chainGroups: ["rewe", "spar"], gmId: f.ids.gm })).body.points[0].visits, 6);
    assert.equal((await query({ chainGroups: ["spar"], marketId: markets[0] })).body.points[0].visits, 0);
    assert.equal((await query({ chainGroups: ["rewe"], marketId: markets[0] })).body.points[0].visits, 1);
    assert.equal((await query({ chain: "Billa" })).body.points[0].visits, 1);
    const individual = await query({ chains: ["Billa", "Spar"], marketIds: [markets[0]!, markets[5]!] });
    assert.equal(individual.status, 200);
    assert.equal(individual.body.points[0].visits, 2);
    assert.equal(individual.body.points[0].availability.Cooler.average, 75);
    assert.deepEqual(individual.body.scope.chains, ["Billa", "Spar"]);
    assert.equal((await query({ chains: ["Billa"] })).body.points[0].visits, 1);
    assert.equal((await query({ chains: ["ISP", "ESP"] })).body.points[0].visits, 2);
    assert.equal((await query({ chains: ["Billa", "Billa"] })).body.points[0].visits, 1);
    assert.equal((await query({ chains: ["", "Billa Corso"] })).body.points[0].visits, 3);
    assert.equal((await query({ chains: [] })).body.points[0].visits, 13);
    for (const chains of ["Billa", null, [42], ["x".repeat(121)]]) {
      assert.equal((await request(app).post("/query").send({ intervals, scope: { ...scope, chains } })).status, 400);
    }
    const both = await query({ chainGroups: ["rewe", "spar"], marketIds: [markets[0]!, markets[5]!] });
    assert.equal(both.status, 200);
    assert.equal(both.body.points[0].visits, 2);
    // IPP counts positive scoring markets; the Spar visit has a zero score.
    assert.equal(both.body.points[0].ippMarketCount, 1);
    assert.equal(both.body.points[0].availability.Cooler.total, 2);
    assert.equal(both.body.points[0].availability.Cooler.average, 75);
    assert.equal(both.body.points[0].placements, 2);
    assert.equal(both.body.points[0].ipp, 2);
    assert.deepEqual(both.body.scope.marketIds, [markets[0], markets[5]]);
    assert.equal((await query({ marketIds: [markets[0]!, markets[5]!, markets[0]!] })).body.points[0].visits, 2);
    assert.equal((await query({ chainGroups: ["rewe"], marketIds: [markets[0]!, markets[5]!] })).body.points[0].visits, 1);
    assert.equal((await query({ chainGroups: ["other"], marketIds: [markets[0]!, markets[5]!] })).body.points[0].visits, 0);
    assert.equal((await query({ marketIds: [markets[0]!, markets[5]!], region: "Süd" })).body.points[0].visits, 1);
    assert.equal((await query({ marketIds: [markets[0]!, markets[5]!], gmId: f.ids.gm })).body.points[0].visits, 1);
    assert.equal((await query({ marketIds: [] })).body.points[0].visits, 13);
    assert.equal((await query({ marketIds: [randomUUID()] })).body.points[0].visits, 0);
    for (const marketIds of [["invalid"], markets[0], null, [42]]) {
      const result = await request(app).post("/query").send({ intervals, scope: { ...scope, marketIds } });
      assert.equal(result.status, 400);
    }
    for (const chainGroups of [["invalid"], "rewe", ["rewe", "spar", "other", "rewe"]]) {
      const result = await request(app).post("/query").send({ intervals, scope: { ...scope, chainGroups } });
      assert.equal(result.status, 400);
    }
    assert.deepEqual((await f.pg.query("select count(*)::int as n from visit_sessions")).rows[0], before);
  } finally { await f.pg.close(); }
});

test("chain and market subsets cannot be replaced with whole-GM archived IPP", () => {
  assert.equal(canUseWholeGmIpp(scope), true);
  assert.equal(canUseWholeGmIpp({ ...scope, chainGroups: [] }), true);
  assert.equal(canUseWholeGmIpp({ ...scope, gmId: randomUUID() }), true);
  for (const chainGroups of [["rewe"], ["spar", "other"], ["rewe", "spar", "other"]] as DashboardChainGroup[][]) {
    assert.equal(canUseWholeGmIpp({ ...scope, chainGroups }), false);
  }
  assert.equal(canUseWholeGmIpp({ ...scope, region: "Nord" }), false);
  assert.equal(canUseWholeGmIpp({ ...scope, marketId: randomUUID() }), false);
  assert.equal(canUseWholeGmIpp({ ...scope, marketIds: [randomUUID(), randomUUID()] }), false);
  assert.equal(canUseWholeGmIpp({ ...scope, marketIds: [] }), true);
  assert.equal(canUseWholeGmIpp({ ...scope, chain: "Billa" }), false);
  assert.equal(canUseWholeGmIpp({ ...scope, chains: ["Billa", "Spar"] }), false);
  assert.equal(canUseWholeGmIpp({ ...scope, chains: [] }), true);
});
