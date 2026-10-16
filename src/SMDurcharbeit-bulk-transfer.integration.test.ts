import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitBulkTransferFixture } from "../tests/SMDurcharbeit-bulk-transfer-fixture.js";

test("single, selected and all-source Durcharbeit transfers preserve the entire historical graph", { timeout: 60_000 }, async t => {
  const f = await createSMDurcharbeitBulkTransferFixture();
  const { admin, root, defaultRoot, markets } = f;
  const row = async (index: number, month = "2026-10-01", campaignId = f.campaign.id) => (await f.targets(month, campaignId)).find(target => target.market.id === markets[index]!.id)!;
  const list = async () => (await admin("get", "/admin/sm-markets?SMDurcharbeitMarketScope=SMDurcharbeit").expect(200)).body.markets as Array<Record<string, any>>;
  const rows = async (table: string) => (await f.pg.query(`select row_to_json(t) as row from ${table} t`)).rows.map(result => JSON.stringify(result)).sort();
  const historyTables = ["sm_smdurcharbeit_visits", "sm_questionnaire_submissions", "sm_questionnaire_submission_questions", "sm_question_answers", "sm_question_answer_options", "sm_question_answer_files", "sm_smdurcharbeit_visit_time_revisions"];
  const snapshot = async (tables: string[]) => Object.fromEntries(await Promise.all(tables.map(async table => [table, await rows(table)])));
  const graphTables = [...historyTables, "sm_smdurcharbeit_assignment_revisions", "sm_smdurcharbeit_month_targets", "sm_smdurcharbeit_events", "sm_markets", "sm_smdurcharbeit_markets"];
  const campaignInput = (ids: string[]) => ({ expectedRevision: f.campaign.revision, month: "2026-10-01", marketIds: ids, SMDurcharbeitSmUserId: f.second, reason: "Synthetic absence cover" });
  const campaignPreview = async (input: ReturnType<typeof campaignInput>) => (await admin("post", `${root}/${f.campaign.id}/owners/preview`).send(input).expect(200)).body;
  const applyCampaign = (input: ReturnType<typeof campaignInput>, previewToken: string) => admin("patch", `${root}/${f.campaign.id}/owners`).send({ ...input, previewToken });
  const defaultInput = async (ids: string[], person = f.third) => ({ SMDurcharbeitSmUserId: person, reason: "Synthetic new market default", markets: (await list()).filter(market => ids.includes(market.id)).map(market => ({ id: market.id, expectedUpdatedAt: market.updatedAt })) });
  try {
    const standardBefore = await f.database.select().from(f.schema.smMarkets).where(eq(f.schema.smMarkets.id, f.market));
    const oldHistory = await snapshot(historyTables);
    const sourceBefore = await f.database.select().from(f.schema.smSMDurcharbeitMarkets);
    await t.test("preview is read-only and one market changes only in the chosen campaign/month", async () => {
      const input = campaignInput([markets[0]!.id]);
      const before = await snapshot(graphTables), preview = await campaignPreview(input);
      assert.equal(preview.changeCount, 1); assert.equal(preview.changedMarketCount, 1);
      assert.deepEqual(await snapshot(graphTables), before);
      await applyCampaign(input, preview.previewToken).expect(200);
      assert.equal((await row(0)).smUserId, f.second);
      assert.equal((await row(0, "2026-11-01")).smUserId, f.employee);
      assert.equal((await row(0, "2026-10-01", f.otherCampaign.id)).smUserId, f.employee);
      assert.equal((await list()).find(market => market.id === markets[0]!.id).assignedSmUserId, f.employee);
      assert.deepEqual(await snapshot(historyTables), oldHistory);
    });
    await t.test("a selection changes only the planned month, with other months, defaults and campaigns untouched", async () => {
      const input = campaignInput([markets[1]!.id, markets[2]!.id]), preview = await campaignPreview(input);
      assert.equal(preview.changedMarketCount, 2); assert.equal(preview.changeCount, 2);
      await applyCampaign(input, preview.previewToken).expect(200);
      assert.equal((await row(1)).smUserId, f.second);
      for (const month of ["2026-11-01", "2026-12-01"]) assert.equal((await row(1, month)).smUserId, f.employee);
      assert.equal((await row(1, "2026-09-01")).smUserId, f.employee);
      assert.equal((await row(1, "2026-10-01", f.otherCampaign.id)).smUserId, f.employee);
      assert.equal((await list()).find(market => market.id === markets[1]!.id).assignedSmUserId, f.employee);
    });
    await t.test("all markets of one SM skip completed and in-progress targets and update the employee's visible work", async () => {
      await f.complete(await row(4), "2026-10-16");
      await f.start(await row(5));
      const planned = await row(3, "2026-11-01");
      await admin("patch", `${root}/targets/${planned.id}/owner`).send({ expectedRevision: planned.revision, SMDurcharbeitSmUserId: f.third, reason: "Synthetic preplanned November cover" }).expect(200);
      await f.database.update(f.schema.users).set({ isActive: false }).where(eq(f.schema.users.id, f.employee));
      const beforeHistory = await snapshot(historyTables);
      const sourceIds = (await f.targets()).filter(target => target.smUserId === f.employee).map(target => target.market.id);
      assert.equal(sourceIds.length, 47);
      const input = { ...campaignInput(sourceIds), sourceSmUserId: f.employee };
      const preview = await campaignPreview(input);
      assert.equal(preview.marketCount, 47); assert.equal(preview.protected.length, 2); assert.equal(preview.changeCount, 45);
      await applyCampaign(input, preview.previewToken).expect(200);
      assert.equal((await row(4)).smUserId, f.employee); assert.equal((await row(5)).smUserId, f.employee);
      assert.equal((await row(6)).smUserId, f.second);
      assert.equal((await row(3, "2026-11-01")).smUserId, f.third, "a different planned future substitute is protected");
      assert.equal((await row(6, "2026-11-01")).smUserId, f.employee, "the exchange cannot affect the next month");
      assert.deepEqual(await snapshot(historyTables), beforeHistory);
      await f.database.update(f.schema.users).set({ isActive: true }).where(eq(f.schema.users.id, f.employee));
      const newWork = (await f.sm("get", "/sm/smdurcharbeit/targets", "synthetic-second-sm").expect(200)).body.targets.filter((target: any) => target.campaignId === f.campaign.id);
      assert.equal(newWork.length, 48);
      const oldWork = (await f.sm("get", "/sm/smdurcharbeit/targets").expect(200)).body.targets.filter((target: any) => target.campaignId === f.campaign.id);
      assert.equal(oldWork.length, 2);
    });
    await t.test("the bulk recipient can start, answer, photograph and submit a transferred visit", async () => {
      const before = await snapshot(historyTables);
      const visit = await f.complete(await row(6), "2026-10-16", "synthetic-second-sm");
      const detail = (await f.sm("get", `/sm/smdurcharbeit/visits/${visit.visitId}`, "synthetic-second-sm").expect(200)).body;
      assert.equal(detail.submission.status, "submitted");
      assert.equal((await row(6)).smUserId, f.second);
      const after = await snapshot(historyTables);
      for (const table of historyTables) assert.ok(before[table]!.every(value => after[table]!.includes(value)));
    });
    await t.test("a stale preview or stale source fails without any partial transfer", async () => {
      const ids = [markets[7]!.id, markets[8]!.id], input = { ...campaignInput(ids), SMDurcharbeitSmUserId: f.third };
      const preview = await campaignPreview(input), current = await row(7);
      await admin("patch", `${root}/targets/${current.id}/owner`).send({ expectedRevision: current.revision, SMDurcharbeitSmUserId: f.employee, reason: "Synthetic intervening edit" }).expect(200);
      const before = await snapshot(graphTables);
      await applyCampaign(input, preview.previewToken).expect(409);
      await admin("post", `${root}/${f.campaign.id}/owners/preview`).send({ ...input, sourceSmUserId: f.second }).expect(409);
      assert.deepEqual(await snapshot(graphTables), before);
    });
    await t.test("bulk market defaults save names/IDs together while preserving import values and explicit campaign overrides", async () => {
      const ids = markets.map(market => market.id), input = { ...await defaultInput(ids), sourceSmUserId: f.employee };
      const before = await snapshot(graphTables), preview = (await admin("post", `${defaultRoot}/owners/preview`).send(input).expect(200)).body;
      assert.equal(preview.changeCount, 50); assert.deepEqual(await snapshot(graphTables), before);
      await admin("patch", `${defaultRoot}/owners`).send({ ...input, previewToken: preview.previewToken }).expect(200);
      for (const market of await list()) { assert.equal(market.assignedSmUserId, f.third); assert.equal(market.shelfMerchandiserName, "Cem Herbst"); }
      assert.equal((await row(6)).smUserId, f.second, "explicit campaign cover survives a default change");
      assert.equal((await row(6, "2026-10-01", f.otherCampaign.id)).smUserId, f.third, "untouched automatic campaign follows the new default");
      assert.equal((await row(4)).smUserId, f.employee); assert.equal((await row(5)).smUserId, f.employee);
      const sources = await f.database.select().from(f.schema.smSMDurcharbeitMarkets);
      for (const old of sourceBefore) assert.deepEqual(sources.find(source => source.smMarketId === old.smMarketId), { ...old, SMDurcharbeitSmUserId: f.third });
      assert.deepEqual(await f.database.select().from(f.schema.smMarkets).where(eq(f.schema.smMarkets.id, f.market)), standardBefore);
    });
    await t.test("one and selected default transfers are atomic; stale timestamps and wrong-domain rows are rejected", async () => {
      for (const ids of [[markets[0]!.id], [markets[1]!.id, markets[2]!.id]]) {
        const input = await defaultInput(ids, f.second), preview = (await admin("post", `${defaultRoot}/owners/preview`).send(input).expect(200)).body;
        assert.equal(preview.changeCount, ids.length);
        await admin("patch", `${defaultRoot}/owners`).send({ ...input, previewToken: preview.previewToken }).expect(200);
        const before = await snapshot(graphTables);
        await admin("patch", `${defaultRoot}/owners`).send({ ...input, previewToken: preview.previewToken }).expect(409);
        assert.deepEqual(await snapshot(graphTables), before);
      }
      const normal = standardBefore[0]!;
      await admin("post", `${defaultRoot}/owners/preview`).send({ ...await defaultInput([markets[0]!.id]), markets: [{ id: f.market, expectedUpdatedAt: normal.updatedAt.toISOString() }] }).expect(404);
    });
    await t.test("permissions, closed months, invalid people and duplicate IDs do not mutate any records", async () => {
      const input = campaignInput([markets[8]!.id]), def = await defaultInput([markets[8]!.id]);
      const before = await snapshot(graphTables);
      for (const token of ["synthetic-sm", "synthetic-gm", "synthetic-normal-editor", "synthetic-reader"]) {
        await admin("patch", `${root}/${f.campaign.id}/owners`, token).send({ ...input, previewToken: "0".repeat(64) }).expect(403);
        await admin("patch", `${defaultRoot}/owners`, token).send({ ...def, previewToken: "0".repeat(64) }).expect(403);
      }
      await admin("post", `${defaultRoot}/owners/preview`, "synthetic-reader").send(def).expect(200);
      await admin("post", `${root}/${f.campaign.id}/owners/preview`).send({ ...input, marketIds: [f.market] }).expect(400);
      await admin("post", `${root}/${f.campaign.id}/owners/preview`).send({ ...input, month: "2026-09-01" }).expect(409);
      await admin("post", `${root}/${f.campaign.id}/owners/preview`).send({ ...input, scope: "future" }).expect(400);
      for (const id of [f.adminId, randomUUID()]) {
        await admin("post", `${root}/${f.campaign.id}/owners/preview`).send({ ...input, SMDurcharbeitSmUserId: id }).expect(400);
        await admin("post", `${defaultRoot}/owners/preview`).send({ ...def, SMDurcharbeitSmUserId: id }).expect(400);
      }
      await admin("post", `${root}/${f.campaign.id}/owners/preview`).send({ ...input, marketIds: [markets[8]!.id, markets[8]!.id] }).expect(400);
      await admin("post", `${defaultRoot}/owners/preview`).send({ ...def, markets: [...def.markets, ...def.markets] }).expect(400);
      assert.deepEqual(await snapshot(graphTables), before);
      await f.database.update(f.schema.users).set({ isActive: false }).where(eq(f.schema.users.id, f.third));
      await admin("post", `${defaultRoot}/owners/preview`).send({ ...def, SMDurcharbeitSmUserId: f.third }).expect(400);
      await f.database.update(f.schema.users).set({ isActive: true }).where(eq(f.schema.users.id, f.third));
      const preview = (await admin("post", `${defaultRoot}/owners/preview`, "synthetic-bulk-editor").send(def).expect(200)).body;
      await admin("patch", `${defaultRoot}/owners`, "synthetic-bulk-editor").send({ ...def, previewToken: preview.previewToken }).expect(200);
    });
    await t.test("after the campaign ends the next campaign follows defaults, with all earlier visits and photos intact", async () => {
      const before = await snapshot(historyTables);
      f.setNow("2027-01-16T12:00:00Z");
      const next = await f.createCampaign("Vorschau · Neue Tour", [markets[6]!.id], "2027-01-01", "2027-01-31");
      assert.equal((await f.targets("2027-01-01", next.id))[0]!.smUserId, f.third);
      assert.deepEqual(await snapshot(historyTables), before);
      const receipts = (await f.sm("get", "/sm/smdurcharbeit/targets", "synthetic-third-sm").expect(200)).body.targets;
      assert.ok(receipts.some((target: any) => target.campaignId === next.id));
    });
    const allHistory = await snapshot(historyTables);
    for (const table of historyTables) assert.ok(oldHistory[table]!.every(value => allHistory[table]!.includes(value)), `${table}: historical rows remain byte-for-byte unchanged`);
  } finally { await f.pg.close(); }
});
