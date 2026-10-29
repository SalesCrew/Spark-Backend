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

test("source exclusion precedes latest-per-market, means and filters; exports retain extra work after conversion", async () => {
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
    assert.deepEqual(clean.points, baseline.points, "supplemental answers must not change any statistic or denominator");
    assert.equal(clean.availabilityAudit?.some(a => a.sessionId === extra), false);
    const facets = await dashboardFacets(f.database);
    assert.equal(facets.gms.some(g => g.id === f.ids.other), false);
    assert.equal(facets.markets.some(m => m.id === separateMarket), false);
    assert.equal(facets.markets.some(m => m.id === f.ids.market), true, "shared markets remain");
    await f.pg.query(`update users set is_supplemental_gm=false where id=$1`, [f.ids.other]);
    assert.deepEqual((await loadDashboard(f.database, intervals, scope)).points, baseline.points);
    const inclusive = await loadDashboard(f.database, intervals, scope, true, true);
    assert.ok(inclusive.points[0]!.visits > before.visits);
    assert.equal(inclusive.availabilityAudit?.some(a => a.sessionId === extra), true);
    const scoped = await loadDashboard(f.database, intervals, { ...scope, gmId: f.ids.other });
    assert.deepEqual(scoped.points, baselineScoped.points, "stale/manually submitted GM filter cannot bypass source exclusion");
    await seeded.seedVisit({ gm: f.ids.other, when: "2026-09-30T13:00:00Z", category: "Top" });
    assert.ok((await loadDashboard(f.database, intervals, scope)).points[0]!.visits > before.visits, "new regular work contributes");
  } finally { await f.pg.close(); }
});

test("bonus dashboard separates complete wave totals, including closed waves and converted GMs; full workspace remains inclusive", async () => {
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
    assert.deepEqual((await readWorkspace(f.database, f.ids.wave)).results, full.results);
    await f.pg.query(`update praemien_wave_settings set closed_snapshot=$1::jsonb,closed_at=now() where wave_id=$2`, [JSON.stringify(full), f.ids.wave]);
    assert.equal((await readDashboardWorkspace(f.database, f.ids.wave)).results.some(r => r.gmId === f.ids.gm), false);
    assert.deepEqual((await readWorkspace(f.database, f.ids.wave)).results, JSON.parse(JSON.stringify(full.results)));
  } finally { await f.pg.close(); }
});
