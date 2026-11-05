import test from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import { randomUUID } from "node:crypto";
import { supplementalFixture } from "./gm-supplemental-fixture.js";
import { praemienFixture } from "../src/lib/praemien-test-fixture.js";
import { installDashboardFixture } from "../src/lib/gm-dashboard-test-fixture.js";
import { loadDashboard, dashboardFacets } from "../src/lib/gm-dashboard.js";
import { readDashboardWorkspace, readWorkspace } from "../src/lib/praemien-workspace.js";
import type { DashboardScope } from "../src/gm-dashboard.shared.js";

const scope: DashboardScope = { gmId: null, region: null, chain: null, marketId: null, stc: null };
const intervals = [{ id: "sep", label: "September", shortLabel: "Sep", start: "2026-09-01", end: "2026-09-30" }];

test("real admin CRUD defaults to regular; Zusatz remains a normal GM; frozen work survives conversion and edits", async () => {
  const f = await supplementalFixture();
  try {
    const payload = { role: "gm", firstName: "Synthetic", lastName: "Extra", email: "extra@example.test", region: "Nord" };
    const create = (body: object, token = "synthetic-admin") => request(f.app).post("/admin/users").set("Authorization", `Bearer ${token}`).send(body);
    assert.equal((await create(payload, "synthetic-gm")).status, 403);
    assert.equal((await create(payload, "synthetic-sm-admin")).status, 403);
    assert.equal((await create({ ...payload, isSupplementalGm: "true" })).status, 400);
    const normal = await create({ ...payload, email: "regular@example.test" });
    assert.equal(normal.status, 201, JSON.stringify(normal.body));
    assert.equal(normal.body.user.isSupplementalGm, false);
    const extra = await create({ ...payload, isSupplementalGm: true });
    assert.equal(extra.status, 201, JSON.stringify(extra.body));
    const id = extra.body.user.id;
    assert.equal(extra.body.user.role, "gm");
    assert.equal(extra.body.user.isSupplementalGm, true);
    const list = await request(f.app).get("/admin/users?role=gm").set("Authorization", "Bearer synthetic-admin");
    assert.equal(list.status, 200, JSON.stringify(list.body));
    assert.equal(list.body.users.some((u: any) => u.id === id), true, "operational directory includes supplemental GMs");
    const market = randomUUID(), visit = randomUUID();
    await f.pg.query(`insert into visit_sessions(id,gm_user_id,market_id,status,is_supplemental_gm_snapshot) values($1,$2,$3,'draft',false)`, [visit, id, market]);
    assert.equal((await f.pg.query<{ extra: boolean }>(`select is_supplemental_gm_snapshot as extra from visit_sessions where id=$1`, [visit])).rows[0]?.extra, true);
    const update = await request(f.app).patch(`/admin/users/${id}`).set("Authorization", "Bearer synthetic-admin").send({ isSupplementalGm: false });
    assert.equal(update.status, 200, JSON.stringify(update.body));
    assert.equal(update.body.user.isSupplementalGm, false);
    await f.pg.query(`update visit_sessions set status='submitted',is_supplemental_gm_snapshot=false where id=$1`, [visit]);
    assert.equal((await f.pg.query<{ extra: boolean }>(`select is_supplemental_gm_snapshot as extra from visit_sessions where id=$1`, [visit])).rows[0]?.extra, true, "history cannot be reclassified by update");
    const next = randomUUID();
    await f.pg.query(`insert into visit_sessions(id,gm_user_id,market_id,status,is_supplemental_gm_snapshot) values($1,$2,$3,'draft',true)`, [next, id, market]);
    assert.equal((await f.pg.query<{ extra: boolean }>(`select is_supplemental_gm_snapshot as extra from visit_sessions where id=$1`, [next])).rows[0]?.extra, false, "new regular work cannot spoof supplemental classification");
  } finally { await f.pg.close(); }
});

test("source exclusion precedes latest-per-market, means and filters; all statistics exclude current Zusatz accounts and converted supplemental work", async () => {
  const f = await praemienFixture();
  try {
    const seeded = await installDashboardFixture(f);
    const baseline = await loadDashboard(f.database, intervals, scope, true);
    const before = baseline.points[0]!;
    const baselineScoped = await loadDashboard(f.database, intervals, { ...scope, gmId: f.ids.other });
    await f.pg.query(`update users set is_supplemental_gm=true where id=$1`, [f.ids.other]);
    const extra = await seeded.seedVisit({ gm: f.ids.other, when: "2026-09-29T12:00:00Z", category: "Bad", weight: "Nein" });
    const separateMarket = randomUUID();
    await f.pg.query(`insert into markets(id,name,db_name) values($1,'Zusatz Test','Zusatz-Kette')`, [separateMarket]);
    await seeded.seedVisit({ gm: f.ids.other, market: separateMarket, when: "2026-09-30T12:00:00Z", category: "Bad", weight: "Nein" });
    const clean = await loadDashboard(f.database, intervals, scope, true);
    assert.equal(clean.points[0]!.visits, before.visits - baselineScoped.points[0]!.visits, "legacy regular snapshots of a current Zusatz account are also excluded");
    assert.equal(clean.availabilityAudit?.some(answer => answer.gmId === f.ids.other), false);
    assert.equal(clean.availabilityAudit?.some(a => a.sessionId === extra), false);
    const facets = await dashboardFacets(f.database);
    assert.equal(facets.gms.some(g => g.id === f.ids.other), false);
    assert.equal(facets.markets.some(m => m.id === separateMarket), false);
    assert.equal(facets.markets.some(m => m.id === f.ids.market), true, "shared markets remain");
    await f.pg.query(`update users set is_supplemental_gm=false where id=$1`, [f.ids.other]);
    assert.deepEqual((await loadDashboard(f.database, intervals, scope)).points, baseline.points);
    const requestedInclusive = await loadDashboard(f.database, intervals, scope, true, true);
    assert.deepEqual(requestedInclusive.points, baseline.points, "legacy inclusion flags cannot put Zusatz work back into statistics");
    assert.equal(requestedInclusive.availabilityAudit?.some(a => a.sessionId === extra), false);
    const scoped = await loadDashboard(f.database, intervals, { ...scope, gmId: f.ids.other });
    assert.deepEqual(scoped.points, baselineScoped.points, "stale/manually submitted GM filter cannot bypass source exclusion");
    await seeded.seedVisit({ gm: f.ids.other, when: "2026-09-30T13:00:00Z", category: "Top" });
    assert.ok((await loadDashboard(f.database, intervals, scope)).points[0]!.visits > before.visits, "new regular work contributes");
  } finally { await f.pg.close(); }
});

test("bonus dashboard separates complete wave totals, including closed waves and converted GMs; administration and closed-wave reads use the same exclusion", async () => {
  const f = await praemienFixture();
  try {
    const full = await readWorkspace(f.database, f.ids.wave);
    await f.pg.query(`update users set is_supplemental_gm=true where id=$1`, [f.ids.gm]);
    const visit = randomUUID();
    await f.pg.query(`insert into visit_sessions(id,gm_user_id,market_id,status,submitted_at) values($1,$2,$3,'submitted','2026-09-15T10:00:00Z')`, [visit, f.ids.gm, f.ids.market]);
    await f.pg.query(`update users set is_supplemental_gm=false where id=$1`, [f.ids.gm]);
    const filtered = await readDashboardWorkspace(f.database, f.ids.wave);
    assert.equal(filtered.results.some(r => r.gmId === f.ids.gm), false);
    assert.equal(filtered.entries.some(r => r.gmId === f.ids.gm), false);
    assert.deepEqual((await readWorkspace(f.database, f.ids.wave)).results, filtered.results);
    await f.pg.query(`update praemien_wave_settings set closed_snapshot=$1::jsonb,closed_at=now() where wave_id=$2`, [JSON.stringify(full), f.ids.wave]);
    assert.equal((await readDashboardWorkspace(f.database, f.ids.wave)).results.some(r => r.gmId === f.ids.gm), false);
    assert.equal((await readWorkspace(f.database, f.ids.wave)).results.some(row => row.gmId === f.ids.gm), false);
    assert.deepEqual((await f.pg.query<{ closed_snapshot: unknown }>("select closed_snapshot from praemien_wave_settings where wave_id=$1", [f.ids.wave])).rows[0]!.closed_snapshot, JSON.parse(JSON.stringify(full)), "stored historical payouts are preserved");
  } finally { await f.pg.close(); }
});

test("bonus HTTP, preview, stale edits, cumulative totals and frozen leaderboards exclude Zusatz GMs without deleting records", async () => {
  const f = await praemienFixture();
  try {
    const express = (await import("express")).default;
    const { createPraemienWorkspaceRouter } = await import("../src/routes/praemien-workspace.js");
    const { createPraemienDashboardReadRouter } = await import("../src/routes/praemien-dashboard-read.js");
    const { managedCumulative, managedGmSummary } = await import("../src/lib/praemien-workspace.js");
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      (req as any).authUser = { appUserId: f.ids.admin, role: "admin" };
      next();
    });
    app.use("/bonus", createPraemienWorkspaceRouter(f.database));
    app.use("/dashboard-bonus", createPraemienDashboardReadRouter(f.database));
    const url = `/bonus/waves/${f.ids.wave}`;
    const full = (await request(app).get(url)).body;
    const preservedEntries = (await f.pg.query("select * from praemien_metric_entries order by gm_user_id,pillar_key,metric_key")).rows;

    // No submitted visits are required for the current account classification to apply.
    await f.pg.query("update users set is_supplemental_gm=true where id=$1", [f.ids.gm]);
    for (const path of [url, `/dashboard-bonus/waves/${f.ids.wave}?includeSupplementalGms=true`]) {
      const response = await request(app).get(path);
      assert.equal(response.status, 200, JSON.stringify(response.body));
      assert.equal(response.body.results.some((row: any) => row.gmId === f.ids.gm), false);
      assert.equal(response.body.entries.some((row: any) => row.gmId === f.ids.gm), false);
      assert.equal(response.body.results.length, 2);
    }
    const preview = await request(app).post(url + "/preview").send({ model: f.model });
    assert.equal(preview.status, 200, JSON.stringify(preview.body));
    assert.equal(preview.body.results.some((row: any) => row.gmId === f.ids.gm), false);
    const staleEntry = { gmId: f.ids.gm, pillarKey: "displays", metricKey: "percent", value: 100, target: null, note: "Stale UI" };
    assert.equal((await request(app).post(url + "/preview").send({ model: f.model, entries: [staleEntry] })).status, 400);
    assert.equal((await request(app).put(url).send({ revision: full.revision, type: "values", entries: [staleEntry] })).status, 400);
    assert.deepEqual((await f.pg.query("select * from praemien_metric_entries order by gm_user_id,pillar_key,metric_key")).rows, preservedEntries);

    await f.pg.query("update praemien_waves set status='active' where id=$1", [f.ids.wave]);
    assert.equal((await managedCumulative(f.database, [f.ids.gm, f.ids.other])).totals.has(f.ids.gm), false);
    assert.equal((await managedGmSummary(f.database, f.ids.wave, f.ids.gm)).currentRewardEur, 0);

    // Exclusion applies to the frozen projection while preserving its original rewards and ranks.
    const snapshot = structuredClone(full);
    snapshot.results.sort((a: any, b: any) => Number(b.gmId === f.ids.gm) - Number(a.gmId === f.ids.gm));
    snapshot.results.forEach((row: any, index: number) => { row.rank = index + 1; row.earned = 9999 - index; });
    await f.pg.query("update praemien_wave_settings set closed_snapshot=$1::jsonb,closed_at=now() where wave_id=$2", [JSON.stringify(snapshot), f.ids.wave]);
    const frozen = (await request(app).get(url)).body;
    assert.equal(frozen.results.length, 2);
    assert.equal(frozen.results[0].rank, 1);
    const leaderboard = await request(app).get("/bonus/leaderboard");
    assert.equal(leaderboard.status, 200, JSON.stringify(leaderboard.body));
    assert.equal(leaderboard.body.results.length, 2);
    assert.equal(leaderboard.body.results.some((row: any) => row.gmId === f.ids.gm), false);
    assert.deepEqual((await f.pg.query<{ snapshot: unknown }>("select closed_snapshot as snapshot from praemien_wave_settings where wave_id=$1", [f.ids.wave])).rows[0]!.snapshot, snapshot);

    // Conversion only permits new regular work; the previous Zusatz wave remains separate.
    const visitId = randomUUID();
    await f.pg.query("insert into visit_sessions(id,gm_user_id,market_id,status,submitted_at) values($1,$2,$3,'submitted','2026-09-15T10:00:00Z')", [visitId, f.ids.gm, f.ids.market]);
    await f.pg.query("update users set is_supplemental_gm=false where id=$1", [f.ids.gm]);
    assert.equal((await request(app).get(url)).body.results.some((row: any) => row.gmId === f.ids.gm), false);
    assert.equal((await request(app).get("/bonus/leaderboard")).body.results.some((row: any) => row.gmId === f.ids.gm), false);
    assert.equal((await f.pg.query<{ count: number }>("select count(*)::int as count from visit_sessions where id=$1", [visitId])).rows[0]!.count, 1);
  } finally { await f.pg.close(); }
});

test("dashboard HTTP cannot opt Zusatz work back in and hides legacy visits from current supplemental accounts", async () => {
  const f = await praemienFixture();
  try {
    const express = (await import("express")).default;
    const { createGmDashboardRouter } = await import("../src/routes/gm-dashboard.js");
    const seeded = await installDashboardFixture(f);
    const legacyVisit = await seeded.seedVisit({ gm: f.ids.other, when: "2026-09-30T10:00:00Z", category: "Bad", weight: "Nein" });
    await f.pg.query("update users set is_supplemental_gm=true where id=$1", [f.ids.other]);
    const expected = await loadDashboard(f.database, intervals, scope, true);
    assert.equal(expected.points[0]!.visits, 3);
    const app = express();
    app.use(express.json(), createGmDashboardRouter(f.database));
    const response = await request(app).post("/query").send({ intervals, scope, includeAvailabilityAudit: true, includeSupplementalGms: true });
    assert.equal(response.status, 200, JSON.stringify(response.body));
    assert.deepEqual(response.body.points, expected.points);
    assert.equal(response.body.availabilityAudit.some((row: any) => row.sessionId === legacyVisit), false);
    const filtered = await request(app).post("/query").send({ intervals, scope: { ...scope, gmId: f.ids.other }, includeSupplementalGms: true });
    assert.equal(filtered.body.points[0].visits, 0);
    const facets = (await request(app).get("/facets")).body;
    assert.equal(facets.gms.some((gm: any) => gm.id === f.ids.other), false);
  } finally { await f.pg.close(); }
});
