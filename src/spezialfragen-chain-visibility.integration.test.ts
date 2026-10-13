import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import request from "supertest";
import { eq } from "drizzle-orm";
import { spezialfragenPeriodFixture } from "../tests/spezialfragen-period-fixture.js";

test("Billa Plus Spezialfragen reach Billa+ visits with or without a timeframe", async () => {
  const f = await spezialfragenPeriodFixture(new Date("2030-05-10T10:00:00Z"));
  try {
    await f.database.update(f.schema.markets).set({ dbName: "Billa+" }).where(eq(f.schema.markets.id, f.ids.market));
    const admin = (verb: "post" | "patch", path: string) => request(f.app)[verb](path).auth("synthetic-gm-admin", { type: "bearer" });
    const gm = (verb: "post" | "get", path: string) => request(f.app)[verb](path).auth("synthetic-gm", { type: "bearer" });
    const question = (text: string, config: Record<string, unknown> = {}, chains = ["Billa Plus"]) => ({
      id: randomUUID(), type: "yesno", text, required: false, config, chains, rules: [], scoring: {},
    });
    const unlimited = question("Synthetic unlimited special");
    const active = question("Synthetic timed special", { spezialfragePeriod: { startDate: "2030-05-10", endDate: "2030-05-10" } });
    const future = question("Synthetic future special", { spezialfragePeriod: { startDate: "2030-05-11", endDate: "2030-05-12" } });
    const expired = question("Synthetic expired special", { spezialfragePeriod: { startDate: "2030-05-01", endDate: "2030-05-09" } });
    const unrelated = question("Synthetic other chain", {}, ["Spar"]);
    const unlinked = question("Synthetic unlinked special");
    const questions = [unlimited, active, future, expired, unrelated];
    const forms: Array<{ id: string; campaignId: string }> = [];
    for (const section of ["standard", "flex", "billa"] as const) {
      const created = await admin("post", "/admin/fragebogen/main").send({
        name: "Synthetic " + section, status: "inactive", moduleIds: [], spezialfragen: questions,
      });
      assert.equal(created.status, 201, JSON.stringify(created.body));
      const campaignId = randomUUID();
      await f.database.insert(f.schema.campaigns).values({
        id: campaignId, name: "Synthetic " + section, section, status: "active", scheduleType: "always",
        currentFragebogenId: created.body.fragebogen.id, assignedGmUserId: section === "flex" ? f.ids.gm : null,
      });
      await f.database.insert(f.schema.campaignMarketAssignments).values({ campaignId, marketId: f.ids.market, gmUserId: f.ids.gm });
      forms.push({ id: created.body.fragebogen.id, campaignId });
    }
    await admin("post", "/admin/fragebogen/main").send({ name: "Synthetic unassigned form", moduleIds: [], spezialfragen: [unlinked] }).expect(201);
    const bankBefore = await f.database.select().from(f.schema.questionBankShared);
    const selection = { marketId: f.ids.market, campaignIds: forms.map(form => form.campaignId) };
    const preview = await gm("get", "/gm/visit-sessions/start-payload").query({ ...selection, campaignIds: selection.campaignIds.join(",") });
    assert.equal(preview.status, 200, JSON.stringify(preview.body));
    assert.equal(preview.body.sections.length, 3);
    for (const section of preview.body.sections) {
      assert.deepEqual(section.questions.map((q: { questionId: string }) => q.questionId), [unlimited.id, active.id]);
      assert.ok(section.questions.every((q: { appliesToMarketChain: boolean }) => q.appliesToMarketChain));
    }
    const started = await gm("post", "/gm/visit-sessions").send({ ...selection, clientSessionToken: "synthetic-chain-visit" });
    assert.equal(started.status, 201, JSON.stringify(started.body));
    const snapshotBefore = await f.database.select().from(f.schema.visitSessionQuestions);
    assert.equal(snapshotBefore.length, 6);
    assert.ok(snapshotBefore.every(q => q.questionChainsSnapshot[0] === "billa plus"), "matching does not rewrite saved chain labels");
    const read = await gm("get", "/gm/visit-sessions/" + started.body.session.id);
    assert.equal(read.status, 200, JSON.stringify(read.body));
    for (const section of read.body.sections) {
      assert.deepEqual(section.questions.map((q: { questionId: string }) => q.questionId), [unlimited.id, active.id]);
    }
    const resumed = await gm("post", "/gm/visit-sessions").send({ ...selection, clientSessionToken: "synthetic-chain-visit" });
    assert.equal(resumed.status, 200, JSON.stringify(resumed.body));
    assert.equal(resumed.body.session.id, started.body.session.id);
    f.setTime("2030-05-13T10:00:00Z");
    const later = await gm("get", "/gm/visit-sessions/start-payload").query({ ...selection, campaignIds: selection.campaignIds.join(",") });
    assert.equal(later.status, 200, JSON.stringify(later.body));
    for (const section of later.body.sections) assert.deepEqual(section.questions.map((q: { questionId: string }) => q.questionId), [unlimited.id]);
    assert.deepEqual(await f.database.select().from(f.schema.visitSessionQuestions), snapshotBefore);
    assert.deepEqual(await f.database.select().from(f.schema.questionBankShared), bankBefore);
  } finally {
    await f.pg.close();
  }
});
