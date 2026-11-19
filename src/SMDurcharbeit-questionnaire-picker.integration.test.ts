import assert from "node:assert/strict";
import test from "node:test";
import { eq } from "drizzle-orm";
import { createQuestionnairePickerFixture } from "../tests/SMDurcharbeit-questionnaire-picker-fixture.js";

test("four saved questionnaires → four latest choices → inactive Tour 2 draft → publish/activate → correct SM questions; history stays intact", async () => {
  const f = await createQuestionnairePickerFixture();
  const history = async () => JSON.stringify(await Promise.all([
    f.schema.smQuestionnaireSubmissions, f.schema.smQuestionnaireSubmissionSections, f.schema.smQuestionnaireSubmissionQuestions,
    f.schema.smQuestionAnswers, f.schema.smQuestionAnswerOptions, f.schema.smQuestionAnswerFiles, f.schema.smQuestionAnswerEvents,
    f.schema.smSMDurcharbeitVisits, f.schema.smSMDurcharbeitPeriods,
  ].map(table => f.database.select().from(table as any))));
  const status = async (id: string) => (await f.database.select().from(f.schema.smQuestionnaireTemplates).where(eq(f.schema.smQuestionnaireTemplates.id, id)))[0]!.status;
  const options = async (suffix = "") => (await f.admin("get", `${f.root}/options${suffix}`).expect(200)).body;
  try {
    const before = await history(), catalog = await options();
    assert.equal(catalog.questionnaires.length, 4);
    assert.equal(new Set(catalog.questionnaires.map((row: any) => row.templateId)).size, 4);
    assert.deepEqual(Object.fromEntries(catalog.questionnaires.map((row: any) => [row.name, row.versionNumber])), {
      "Rewe DA 200ml - Tour 1": 5, "Rewe DA 200ml - Tour 2": 2, "Spar DA 200 ml - Tour 1": 2, "Spar DA 200 ml - Tour 2": 2,
    });
    assert.equal((await options(`?selectedVersionId=${f.version.id}`)).selectedQuestionnaire.id, f.version.id);
    assert.equal((await options(`?selectedVersionId=${f.version.id}`)).questionnaires.length, 4, "Pinned history is returned separately, never as five Tour 1 choices");
    await f.admin("get", `${f.root}/options?selectedVersionId=invalid`).expect(400);
    for (const token of ["synthetic-sm", "synthetic-gm"]) await f.admin("get", `${f.root}/options`, token).expect(403);
    const selected = catalog.questionnaires.find((row: any) => row.templateId === f.spar2.id);
    const draft = { name: "Tour 2 campaign", startDate: "2026-10-19", endDate: "2026-11-30", questionnaireVersionId: selected.id, rosterDraft: [{ smMarketId: f.market }] };
    let campaign = (await f.admin("post", f.root).send(draft).expect(201)).body.campaign;
    campaign = (await f.admin("patch", `${f.root}/${campaign.id}`).send({ ...draft, expectedRevision: campaign.revision }).expect(200)).body.campaign;
    assert.equal(await status(f.spar2.id), "inactive");
    assert.ok(!(await f.list()).some((row: any) => row.campaignId === campaign.id), "Selecting/saving a copy does not publish it to SMs");
    const preview = (await f.admin("get", `${f.root}/${campaign.id}/preview`).expect(200)).body;
    assert.equal(preview.questionnaireRequiresActivation, true);
    await f.admin("post", `${f.root}/${campaign.id}/publish`).send({ expectedRevision: campaign.revision, previewToken: "0".repeat(64), confirmOverlap: true }).expect(409);
    assert.equal(await status(f.spar2.id), "inactive", "Rejected publication never activates a questionnaire");
    assert.equal(await history(), before, "Catalog loading, draft saves, preview/cancel/rejected publish do not change historical answers/photos/periods");
    const versionsBefore = await f.database.select().from(f.schema.smQuestionnaireVersions);
    const oldVisit = (await f.sm("get", `${f.visitRoot}/${f.completed.visitId}`).expect(200)).body;
    await f.admin("post", `${f.root}/${campaign.id}/publish`).send({ expectedRevision: campaign.revision, previewToken: preview.previewToken, confirmOverlap: true }).expect(200);
    assert.equal(await status(f.spar2.id), "active");
    assert.deepEqual(await f.database.select().from(f.schema.smQuestionnaireVersions), versionsBefore, "Publication activates only the selected root, never rewrites or republishes versions");
    const target = (await f.list()).find((row: any) => row.campaignId === campaign.id);
    const started = await f.start(target.id);
    const visit = (await f.sm("get", `${f.visitRoot}/${started.visitId}`).expect(200)).body;
    assert.equal(visit.SMDurcharbeitQuestionnaireSelection.questionnaireVersionId, selected.id);
    assert.equal(visit.SMDurcharbeitQuestionnaireSelection.name, f.spar2.name);
    assert.equal(visit.sections[0].questions.length, 4);
    const preserved = (await f.sm("get", `${f.visitRoot}/${f.completed.visitId}`).expect(200)).body;
    assert.deepEqual(preserved.answers, oldVisit.answers); assert.deepEqual(preserved.photoFiles, oldVisit.photoFiles); assert.deepEqual(preserved.sections, oldVisit.sections);
    assert.equal(f.objects.size, 1);
    const published = (await f.admin("get", f.root).expect(200)).body.campaigns.find((row: any) => row.id === campaign.id);
    const periods = (await f.admin("get", `${f.root}/${campaign.id}/periods`).expect(200)).body.periods;
    const next = periods.find((row: any) => row.month === "2026-11-01");
    const rewe2 = catalog.questionnaires.find((row: any) => row.templateId === f.rewe2.id);
    await f.admin("patch", `${f.root}/${campaign.id}/periods/${next.id}/questionnaire`).send({ expectedRevision: published.revision + 1, questionnaireVersionId: rewe2.id, reason: "Test" }).expect(409);
    assert.equal(await status(f.rewe2.id), "inactive", "A rejected future-month edit does not activate a copy");
    await f.admin("patch", `${f.root}/${campaign.id}/periods/${next.id}/questionnaire`).send({ expectedRevision: published.revision, questionnaireVersionId: rewe2.id, reason: "November vorbereiten" }).expect(200);
    assert.equal(await status(f.rewe2.id), "active");
    const afterPeriods = (await f.admin("get", `${f.root}/${campaign.id}/periods`).expect(200)).body.periods;
    assert.equal(afterPeriods.find((row: any) => row.month === "2026-10-01").questionnaireVersionId, selected.id);
    assert.equal(afterPeriods.find((row: any) => row.month === "2026-11-01").questionnaireVersionId, rewe2.id);
    const afterEvents = (await f.database.select().from(f.schema.smSMDurcharbeitEvents)).filter(row => row.campaignId === campaign.id);
    assert.ok(afterEvents.some(row => row.action === "published" && (row.afterState as any).questionnaireActivated));
    assert.ok(afterEvents.some(row => row.action === "future_questionnaire_changed" && (row.afterState as any).questionnaireActivated));
    const historicalDraft = (await f.admin("get", f.root).expect(200)).body.campaigns.find((row: any) => row.id === f.historicalDraft.id);
    assert.equal(historicalDraft.questionnaireVersionId, f.version.id);
  } finally { await f.pg.close(); }
});

test("archived/deleted/standard/unpublished questionnaire versions stay unavailable to campaign creation", async () => {
  const f = await createQuestionnairePickerFixture();
  try {
    const selected = (await f.admin("get", `${f.root}/options`).expect(200)).body.questionnaires.find((row: any) => row.templateId === f.rewe2.id);
    const body = { name: "Invalid", startDate: "2026-10-19", endDate: "2026-10-31", questionnaireVersionId: selected.id, rosterDraft: [{ smMarketId: f.market }] };
    for (const change of [{ status: "archived" as const }, { status: "inactive" as const, isDeleted: true }, { status: "active" as const, isDeleted: false, stableCode: "standard_synthetic" }]) {
      await f.database.update(f.schema.smQuestionnaireTemplates).set(change).where(eq(f.schema.smQuestionnaireTemplates.id, f.rewe2.id));
      await f.admin("post", f.root).send(body).expect(409);
      assert.ok(!(await f.admin("get", `${f.root}/options`).expect(200)).body.questionnaires.some((row: any) => row.templateId === f.rewe2.id));
    }
    await f.database.update(f.schema.smQuestionnaireTemplates).set({ status: "inactive", stableCode: "smdurcharbeit_synthetic" }).where(eq(f.schema.smQuestionnaireTemplates.id, f.rewe2.id));
    const [unpublished] = await f.database.insert(f.schema.smQuestionnaireVersions).values({ questionnaireTemplateId: f.rewe2.id, versionNumber: 3, status: "draft", name: "Noch nicht gespeichert" }).returning();
    await f.admin("post", f.root).send({ ...body, questionnaireVersionId: unpublished!.id }).expect(409);
    assert.ok(!(await f.admin("get", `${f.root}/options`).expect(200)).body.questionnaires.some((row: any) => row.id === unpublished!.id));
  } finally { await f.pg.close(); }
});

test("another campaign's activation invalidates an older publish preview instead of overwriting its decision", async () => {
  const f = await createQuestionnairePickerFixture();
  try {
    const selected = (await f.admin("get", `${f.root}/options`).expect(200)).body.questionnaires.find((row: any) => row.templateId === f.spar2.id);
    const body = { name: "Gemeinsame Fassung", startDate: "2026-10-19", endDate: "2026-10-31", questionnaireVersionId: selected.id, rosterDraft: [{ smMarketId: f.market }] };
    const campaigns = await Promise.all([1, 2].map(async index => (await f.admin("post", f.root).send({ ...body, name: body.name + index }).expect(201)).body.campaign));
    const previews = await Promise.all(campaigns.map(async campaign => (await f.admin("get", `${f.root}/${campaign.id}/preview`).expect(200)).body));
    await f.admin("post", `${f.root}/${campaigns[0].id}/publish`).send({ expectedRevision: campaigns[0].revision, previewToken: previews[0].previewToken, confirmOverlap: true }).expect(200);
    await f.admin("post", `${f.root}/${campaigns[1].id}/publish`).send({ expectedRevision: campaigns[1].revision, previewToken: previews[1].previewToken, confirmOverlap: true }).expect(409);
    const renewed = (await f.admin("get", `${f.root}/${campaigns[1].id}/preview`).expect(200)).body;
    assert.equal(renewed.questionnaireRequiresActivation, false);
    const stillDraft = (await f.admin("get", f.root).expect(200)).body.campaigns.find((row: any) => row.id === campaigns[1].id);
    assert.equal(stillDraft.status, "draft");
    assert.equal((await f.database.select().from(f.schema.smQuestionnaireTemplates).where(eq(f.schema.smQuestionnaireTemplates.id, f.spar2.id)))[0]!.status, "active");
  } finally { await f.pg.close(); }
});
