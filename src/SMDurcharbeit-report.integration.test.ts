import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "../tests/SMDurcharbeit-fixture.js";
import { loadSMDurcharbeitPrivacyCounts } from "./sm-SMDurcharbeit-privacy.shared.js";

test("monthly report counts latest required market answers once and every physical time once", async () => {
  class Clock extends Date { constructor(value?: string | number | Date) { super(value === undefined ? "2026-10-09T12:00:00Z" : value instanceof Date ? value.getTime() : value); } static now() { return Date.parse("2026-10-09T12:00:00Z"); } }
  const f = await createSMDurcharbeitFixture({ clock: Clock as typeof Date });
  const admin = (method: "get" | "post" | "patch", path: string) => request(f.app)[method](path).auth("synthetic-sm-admin", { type: "bearer" });
  const sm = (method: "get" | "post" | "put", path: string) => request(f.app)[method](path).auth("synthetic-sm", { type: "bearer" });
  try {
    const markets = [f.market, randomUUID(), randomUUID()];
    for (const [index, id] of markets.entries()) {
      if (index) await f.database.insert(f.schema.smMarkets).values({ id, internalMarketId: `SYNTHETIC-REPORT-${index}`, name: `Report market ${index}`, chain: "Spar", address: "Synthetic 1", postalCode: "1010", city: "Wien", region: "Ost", assignedSmUserId: f.employee });
      await f.database.insert(f.schema.smSMDurcharbeitMarkets).values({ smMarketId: id });
    }
    const q = (type: string, required = false, config = {}) => ({ id: `new-${randomUUID()}`, text: type, type, required, options: [], config, rules: [] });
    const module = (await admin("post", "/admin/sm-questionnaires/modules?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Report questions", description: "", questions: [q("yesno", true), q("numeric"), q("multiple", false, { options: ["A", "B"] }), q("text")] }).expect(201)).body.module;
    const form = (await admin("post", "/admin/sm-questionnaires/questionnaires?scope=SMDurcharbeit").send({ id: `new-${randomUUID()}`, name: "Report", description: "", status: "active", moduleIds: [module.id] }).expect(201)).body.questionnaire;
    const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
    const campaign = (await admin("post", "/admin/sm-smdurcharbeit-campaigns").send({ name: "Report October–November", startDate: "2026-10-01", endDate: "2026-11-30", questionnaireVersionId: version!.id, rosterDraft: markets.map(smMarketId => ({ smMarketId, smUserId: f.employee })) }).expect(201)).body.campaign;
    const base = `/admin/sm-smdurcharbeit-campaigns/${campaign.id}`;
    const preview = (await admin("get", `${base}/preview`).expect(200)).body;
    await admin("post", `${base}/publish`).send({ expectedRevision: campaign.revision, previewToken: preview.previewToken }).expect(200);
    const targets = () => sm("get", "/sm/smdurcharbeit/targets").expect(200);
    const execute = async (marketId: string, followUp: boolean, yes: boolean, number: number, hour: number) => {
      const target = (await targets()).body.targets.find((target: any) => target.market.id === marketId);
      const started = (await sm("post", `/sm/smdurcharbeit/targets/${target.id}/start`).send({ expectedRevision: target.revision, followUp, mode: "manual", clientSubmissionToken: randomUUID() }).expect(201)).body;
      const visit = `/sm/smdurcharbeit/visits/${started.visitId}`, payload = (await sm("get", visit).expect(200)).body;
      for (const question of payload.sections[0].questions) {
        const value = question.type === "yesno" ? { kind: "choice", optionCode: question.options[yes ? 0 : 1].code }
          : question.type === "numeric" ? { kind: "number", value: number }
            : question.type === "multiple" ? { kind: "multi", optionCodes: question.options.map((option: any) => option.code) } : null;
        if (value) await sm("put", `${visit}/answers/${question.id}`).send({ expectedAnswerVersion: payload.answerVersions[question.id] ?? 0, clientMutationToken: randomUUID(), answer: value }).expect(200);
      }
      const clockHour = String(hour).padStart(2, "0");
      await sm("post", `${visit}/submit`).send({ visitStartedAt: `2026-10-09T${clockHour}:00:00Z`, visitCompletedAt: `2026-10-09T${clockHour}:10:00Z`, clientMutationToken: randomUUID() }).expect(200);
      return started;
    };
    await execute(markets[0]!, false, true, 2, 8);
    await execute(markets[1]!, false, true, 10, 9);
    await execute(markets[0]!, true, false, 0, 10);
    const frozen = await Promise.all([f.schema.smQuestionnaireSubmissions, f.schema.smQuestionAnswers, f.schema.smSMDurcharbeitTargets, f.schema.smSMDurcharbeitTimeRevisions, f.schema.smSMDurcharbeitEvents].map(table => f.database.select().from(table as any)));
    const read = async (suffix = "") => (await admin("get", `${base}/results?month=2026-10-09${suffix}`).expect(200)).body;
    const report = await read();
    assert.deepEqual(report.summary, { required: 3, completed: 2, waived: 0, coveragePercentage: 200 / 3, latestSubmissions: 2,
      physicalVisits: 3, validQuestionnaireVisits: 3, actualMinutes: 30, travelMinutes: 0, availablePhotoUploads: 0 });
    assert.equal(report.month, "2026-10-01"); assert.equal(report.answers.length, 8);
    const yesno = report.questionResults.find((q: any) => q.type === "yesno");
    assert.deepEqual(yesno.distribution.map((option: any) => [option.label, option.count, option.percentage]), [["Ja", 1, 50], ["Nein", 1, 50]]);
    assert.equal(report.questionResults.find((q: any) => q.type === "numeric").average, 5, "Zero is answered; superseded first value is excluded");
    assert.deepEqual(report.questionResults.find((q: any) => q.type === "multiple").distribution.map((o: any) => o.percentage), [100, 100]);
    assert.equal(report.questionResults.find((q: any) => q.type === "text").unanswered, 2, "Missing current answers remain in applicable denominator");
    assert.equal(report.answers.filter((row: any) => row.sourceSubmissionId).length, 1, "Only the unchanged inherited answer keeps carry-over provenance");
    const inventory = await loadSMDurcharbeitPrivacyCounts(f.database, f.employee);
    assert.equal(inventory.targets, 6); assert.equal(inventory.ownerRevisions, 6);
    assert.equal(inventory.visits, 3); assert.equal(inventory.timeRevisions, 3);
    assert.equal(inventory.answerProvenance, 3); assert.equal(inventory.fileLinks, 0);
    assert.ok(inventory.events >= 3);
    assert.ok(Object.values(await loadSMDurcharbeitPrivacyCounts(f.database, randomUUID())).every(value => value === 0));
    assert.equal((await read(`&smUserId=${randomUUID()}`)).summary.required, 0);
    await admin("get", `${base}/results?smUserId=invalid`).expect(400);
    await admin("get", `${base}/results?month=2026-10-01&employeeId=${f.employee}`).expect(400);
    await request(f.app).get(`${base}/results`).auth("synthetic-sm", { type: "bearer" }).expect(403);
    assert.deepEqual(await Promise.all([f.schema.smQuestionnaireSubmissions, f.schema.smQuestionAnswers, f.schema.smSMDurcharbeitTargets, f.schema.smSMDurcharbeitTimeRevisions, f.schema.smSMDurcharbeitEvents].map(table => f.database.select().from(table as any))), frozen, "Report GETs leave every original graph unchanged");
    const november = (await admin("get", `${base}/results?month=2026-11-01`).expect(200)).body;
    assert.equal(november.summary.completed, 0); assert.equal(november.summary.physicalVisits, 0); assert.deepEqual(november.questionResults, []);
    const third = (await targets()).body.targets.find((target: any) => target.market.id === markets[2]);
    await admin("patch", `/admin/sm-smdurcharbeit-campaigns/targets/${third.id}`).send({ expectedRevision: third.revision, scope: "month", reason: "Synthetic waiver", eligibility: "waived" }).expect(200);
    const waived = await read(); assert.equal(waived.summary.required, 2); assert.equal(waived.summary.waived, 1); assert.equal(waived.summary.coveragePercentage, 100); assert.equal(waived.summary.physicalVisits, 3);
  } finally { await f.pg.close(); }
});
