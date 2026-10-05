import assert from "node:assert/strict";
import test from "node:test";
import { randomUUID } from "node:crypto";
import express from "express";
import request from "supertest";
import { readFile } from "node:fs/promises";
import { eq } from "drizzle-orm";
import { campaigns } from "./lib/schema.js";
import { campaignExtensionFixture } from "./lib/campaign-extension-fixture.js";
import { CampaignExtensionError, extendCampaign, isCampaignDate, viennaCampaignDate } from "./lib/campaign-extension.js";
import { createCampaignExtensionRouter } from "./routes/campaign-extension.js";
import type { AuthedRequest } from "./middleware/auth.js";

const now = new Date("2026-10-05T12:00:00Z");
const token = (row: {endDate: string | null; updatedAt: Date}, endDate = "2026-10-16") => ({ endDate, expectedEndDate: row.endDate!, expectedUpdatedAt: row.updatedAt.toISOString() });
const code = (expected: string) => (error: unknown) => error instanceof CampaignExtensionError && error.code === expected;

test("strict calendar dates and Vienna midnight/DST boundaries", () => {
  for (const value of ["2026-02-29", "2026-04-31", "2026-13-01", "26-10-05", "2026-10-05T12:00:00Z"]) assert.equal(isCampaignDate(value), false);
  assert.equal(isCampaignDate("2028-02-29"), true);
  assert.equal(viennaCampaignDate(new Date("2026-10-04T22:01:00Z")), "2026-10-05");
  assert.equal(viennaCampaignDate(new Date("2026-10-25T23:01:00Z")), "2026-10-26");
});

test("all six campaign types extend/reactivate atomically without changing any other data", async () => {
  const f = await campaignExtensionFixture();
  try {
    for (const section of ["standard", "flex", "billa", "kuehler", "mhd", "durcharbeit"] as const) {
      for (const status of ["active", "inactive"] as const) {
        const { id } = await f.seed({ section, status, ...(section === "flex" ? { assignedGmUserId: f.gm } : {}) });
        const row = await f.get(id), before = await f.snapshot();
        const result = await extendCampaign(f.dependencies, id, token(row), now);
        const after = await f.snapshot();
        assert.deepEqual(result.campaign, { id, endDate: "2026-10-16", status: "active", updatedAt: now.toISOString() });
        const expectedRow = { ...row, endDate: "2026-10-16", status: "active", updatedAt: now };
        assert.deepEqual(await f.get(id), expectedRow);
        delete before.campaigns; delete after.campaigns;
        assert.deepEqual(after, before, section + " retained assignments, targets, histories, answers, photos and scoring");
      }
    }
    const future = await f.seed({ startDate: "2026-11-01", endDate: "2026-11-10", status: "inactive" });
    assert.equal((await extendCampaign(f.dependencies, future.id, token(await f.get(future.id), "2026-11-20"), now)).campaign.status, "scheduled");
    const inclusive = await f.seed();
    assert.equal((await extendCampaign(f.dependencies, inclusive.id, token(await f.get(inclusive.id), "2026-10-05"), now)).campaign.status, "active");
  } finally { await f.pg.close(); }
});

test("invalid, stale, deleted and unlimited attempts leave the database unchanged; replay and double submit are safe", async () => {
  const f = await campaignExtensionFixture();
  try {
    const { id } = await f.seed(), row = await f.get(id);
    for (const end of ["2026-10-02", "2026-09-30", "2026-10-04", "2026-02-30"]) {
      const before = await f.snapshot();
      await assert.rejects(extendCampaign(f.dependencies, id, token(row, end), now), code(end === "2026-02-30" ? "invalid_payload" : "extension_date_invalid"));
      assert.deepEqual(await f.snapshot(), before);
    }
    for (const stale of [{ ...token(row), expectedEndDate: "2026-10-01" }, { ...token(row), expectedUpdatedAt: "2026-09-01T08:00:00Z" }]) {
      const before = await f.snapshot();
      await assert.rejects(extendCampaign(f.dependencies, id, stale, now), code("campaign_changed"));
      assert.deepEqual(await f.snapshot(), before);
    }
    await assert.rejects(extendCampaign(f.dependencies, randomUUID(), token(row), now), code("campaign_not_found"));
    const deleted = await f.seed({ isDeleted: true });
    await assert.rejects(extendCampaign(f.dependencies, deleted.id, token(await f.get(deleted.id)), now), code("campaign_not_found"));
    const unlimited = await f.seed({ scheduleType: "always", startDate: null, endDate: null });
    await assert.rejects(extendCampaign(f.dependencies, unlimited.id, { ...token(await f.get(unlimited.id)), expectedEndDate: "2026-10-02" }, now), code("campaign_changed"));
    await f.database.update(campaigns).set({ endDate: "2026-10-02" }).where(eq(campaigns.id, unlimited.id));
    await assert.rejects(extendCampaign(f.dependencies, unlimited.id, token(await f.get(unlimited.id)), now), code("campaign_unlimited"));
    const results = await Promise.allSettled([extendCampaign(f.dependencies, id, token(row), now), extendCampaign(f.dependencies, id, token(row), now)]);
    assert.equal(results.filter((x) => x.status === "fulfilled").length, 1);
    assert.equal(results.filter((x) => x.status === "rejected" && code("campaign_changed")(x.reason)).length, 1);
    const after = await f.snapshot();
    await assert.rejects(extendCampaign(f.dependencies, id, token(row), now), code("campaign_changed"));
    assert.deepEqual(await f.snapshot(), after);
  } finally { await f.pg.close(); }
});

test("same-type active and planned overlaps reject without any writes; existing flex/cooler rules are preserved", async () => {
  const f = await campaignExtensionFixture();
  try {
    for (const status of ["active", "scheduled"] as const) {
      const a = await f.seed();
      await f.seed({ name: "Konflikt", startDate: "2026-10-10", endDate: "2026-10-20", status }, a.marketId);
      const before = await f.snapshot();
      await assert.rejects(extendCampaign(f.dependencies, a.id, token(await f.get(a.id)), now), (error: unknown) =>
        code("campaign_market_overlap")(error) && (error as CampaignExtensionError).conflicts?.[0]?.existingCampaignName === "Konflikt");
      assert.deepEqual(await f.snapshot(), before);
    }
    const ignored = await f.seed();
    await f.seed({ section: "mhd", status: "active" }, ignored.marketId);
    await f.seed({ status: "inactive" }, ignored.marketId);
    await f.seed({ status: "active", isDeleted: true }, ignored.marketId);
    await f.seed({ status: "active", startDate: "2026-11-01", endDate: "2026-11-20" }, ignored.marketId);
    await extendCampaign(f.dependencies, ignored.id, token(await f.get(ignored.id)), now);
    const flex = await f.seed({ section: "flex" });
    await f.seed({ section: "flex", status: "active" }, flex.marketId);
    await extendCampaign(f.dependencies, flex.id, token(await f.get(flex.id)), now);
    const cooler = await f.seed({ section: "kuehler" });
    const other = await f.seed({ section: "kuehler", status: "active" }, cooler.marketId);
    f.completedCoolers.add(other.id + ":" + cooler.marketId);
    await extendCampaign(f.dependencies, cooler.id, token(await f.get(cooler.id)), now);
  } finally { await f.pg.close(); }
});

test("HTTP mutation validates the entire payload and role, persists the patch and rejects a stale second request", async () => {
  const f = await campaignExtensionFixture();
  try {
    const { id } = await f.seed(), row = await f.get(id);
    const app = express(); app.use(express.json());
    app.use((req: AuthedRequest, _res, next) => {
      const role = req.headers["x-synthetic-role"];
      if (role) req.authUser = { appUserId: f.gm, supabaseAuthId: f.gm, role: role as "admin", email: "synthetic@example.test" };
      next();
    });
    app.use("/admin/campaigns", createCampaignExtensionRouter(f.dependencies));
    const url = "/admin/campaigns/" + id + "/extend", payload = token(row, "2099-10-16"), before = await f.snapshot();
    await request(app).patch(url).send(payload).expect(401);
    await request(app).patch(url).set("x-synthetic-role", "gm").send(payload).expect(403);
    for (const invalid of [{ ...payload, status: "active" }, { ...payload, name: "Tamper" }, { ...payload, assignments: [] }, { endDate: payload.endDate }, { ...payload, expectedUpdatedAt: "bad" }])
      await request(app).patch(url).set("x-synthetic-role", "admin").send(invalid).expect(400);
    assert.deepEqual(await f.snapshot(), before);
    const response = await request(app).patch(url).set("x-synthetic-role", "admin").send(payload).expect(200);
    assert.equal(response.body.campaign.endDate, "2099-10-16");
    assert.equal((await f.get(id)).endDate, "2099-10-16");
    await request(app).patch(url).set("x-synthetic-role", "admin").send(payload).expect(409);
    // Production parent must retain authentication and per-page Kunde update permission before mounting.
    const production = await readFile(new URL("./routes/campaigns.ts", import.meta.url), "utf8");
    assert.ok(production.indexOf("adminCampaignsRouter.use(requireAuth") < production.indexOf('adminCampaignsRouter.use("/campaigns", createCampaignExtensionRouter'));
    assert.ok(production.indexOf("adminCampaignsRouter.use(requireKundeAdminPermission)") < production.indexOf('adminCampaignsRouter.use("/campaigns", createCampaignExtensionRouter'));
  } finally { await f.pg.close(); }
});
