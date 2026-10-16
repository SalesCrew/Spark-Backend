import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import request from "supertest";
import { createSMDurcharbeitFixture } from "./SMDurcharbeit-fixture.js";

/** Synthetic relationships and real submitted OOS answers. Never loads application env files. */
export async function createSmDashboardFiltersFixture(options: { overviewPreview?: boolean } = {}) {
  const second = randomUUID(), daOnlySm = randomUUID();
  const f = await createSMDurcharbeitFixture({ additionalSmUsers: [{ id: second, token: "synthetic-second-sm" }, { id: daOnlySm, token: "synthetic-da-only-sm" }] });
  try {
    await f.database.update(f.schema.users).set({ firstName: "Anna", lastName: "Vorschau" }).where(eq(f.schema.users.id, f.employee));
    await f.database.update(f.schema.users).set({ firstName: "Berta", lastName: "Vorschau" }).where(eq(f.schema.users.id, second));
    await f.database.update(f.schema.users).set({ firstName: "Cora", lastName: "Nur Durcharbeit" }).where(eq(f.schema.users.id, daOnlySm));
    const markets = {
      northBilla: f.market, northSpar: randomUUID(), southBilla: randomUUID(),
      dedicated: randomUUID(), sharedWeekly: randomUUID(), sharedHistory: randomUUID(), sharedPlan: randomUUID(), unvisited: randomUUID(),
    };
    await f.database.update(f.schema.smMarkets).set({ name: "Billa · Am Stadtpark", region: "Nord", assignedSmUserId: second }).where(eq(f.schema.smMarkets.id, f.market));
    await f.database.insert(f.schema.smMarkets).values([
      { id: markets.northSpar, name: "Spar · An der Allee", chain: "Spar", region: "Nord", assignedSmUserId: f.employee },
      { id: markets.southBilla, name: "Billa · Im Zentrum", chain: "Billa", region: "Süd", assignedSmUserId: second },
      { id: markets.dedicated, name: "Nur Durcharbeit", chain: "Nur DA", region: "Nur DA", assignedSmUserId: daOnlySm },
      { id: markets.sharedWeekly, name: "Geteilter Stammmarkt · Wochenplan", chain: "Billa", region: "Süd", assignedSmUserId: second, mondayHours: "2" },
      { id: markets.sharedHistory, name: "Geteilter Stammmarkt · Historie", chain: "Spar", region: "Süd", assignedSmUserId: second },
      { id: markets.sharedPlan, name: "Geteilter Stammmarkt · Einsatz", chain: "Spar", region: "Nord", assignedSmUserId: f.employee },
      { id: markets.unvisited, name: "Neuer Stammmarkt ohne Besuch", chain: "Billa", region: "Nord", assignedSmUserId: f.employee },
    ].map(row => ({ ...row, address: "Vorschauweg 1", postalCode: "1010", city: "Vorschaustadt", internalMarketId: `SYNTHETIC-${row.id}` })));
    const admin = (path: string, scope = "standard") => request(f.app).post(`${path}?scope=${scope}`).auth("synthetic-sm-admin", { type: "bearer" });
    async function questionnaire(scope: string) {
      const base = { type: "yesno", required: true, options: ["Ja", "Nein"], config: {}, rules: [] };
      const module = (await admin("/admin/sm-questionnaires/modules", scope).send({ id: `new-${randomUUID()}`, name: `Vorschau ${scope}`, description: "Synthetische Filterprüfung", questions: [
        { ...base, id: "detect", text: "OOS aufgefunden?", oos: { enabled: true, role: "detection", category: "softdrinks_energy", answerOutcomes: { Ja: "oos_present", Nein: "oos_absent" } } },
        { ...base, id: "remedy", text: "OOS behoben?", oos: { enabled: true, role: "remediation", category: "softdrinks_energy", detectionQuestionId: "detect", partialCountsAsResolved: false, answerOutcomes: { Ja: "resolved", Nein: "not_resolved" } } },
        ...(options.overviewPreview ? [
          { id: "basic", text: "Basistätigkeiten im Markt durchgeführt?", type: "single", required: true, options: ["Erledigt", "Teilweise", "Offen"], config: { options: ["Erledigt", "Teilweise", "Offen"], basicActivity: { enabled: true, answerOutcomes: { Erledigt: "yes", Teilweise: "partial", Offen: "no" } } }, rules: [] },
          ...["action_placements", "water_near_water", "juice_iced_tea"].map(category => ({ ...base, id: `detect-${category}`, text: `OOS-Kontrolle ${category}`, oos: { enabled: true, role: "detection", category, answerOutcomes: { Ja: "oos_present", Nein: "oos_absent" } } })),
        ] : []),
      ] }).expect(201)).body.module;
      const form = (await admin("/admin/sm-questionnaires/questionnaires", scope).send({ id: `new-${randomUUID()}`, name: `Vorschau ${scope}`, description: "Synthetische Filterprüfung", status: "active", moduleIds: [module.id] }).expect(201)).body.questionnaire;
      const [version] = await f.database.select().from(f.schema.smQuestionnaireVersions).where(eq(f.schema.smQuestionnaireVersions.questionnaireTemplateId, form.id));
      return { form, version: version! };
    }
    const standard = await questionnaire("standard"), durcharbeit = await questionnaire("SMDurcharbeit");
    await f.database.insert(f.schema.smQuestionnaireGlobalAssignments).values({ questionnaireTemplateId: standard.form.id, assignedByUserId: f.admin });
    async function assignment(marketId: string, personId: string, isDa = false) {
      const [row] = await f.database.insert(f.schema.smAssignments).values({ sourceType: "single", idempotencyKey: randomUUID(), originalWorkDate: "2026-10-07", originalSmUserId: personId, originalSmMarketId: marketId, originalMarketInternalId: `SYNTHETIC-${marketId}`, originalPlannedMinutes: 30, createdByUserId: f.admin, updatedByUserId: f.admin,
        ...(isDa ? { SMDurcharbeitQuestionnaireOverrideVersionId: durcharbeit.version.id } : {}) }).returning();
      return row!;
    }
    async function submit(marketId: string, personId: string, day: string, found: boolean, fixed: boolean, isDa = false) {
      const row = await assignment(marketId, personId, isDa);
      const sm = (method: "post" | "put", path: string) => request(f.app)[method](path).auth(personId === f.employee ? "synthetic-sm" : "synthetic-second-sm", { type: "bearer" });
      const opened = (await sm("post", `/sm/visits/${row.id}/start`).send({ mode: "manual", clientSubmissionToken: randomUUID() }).expect(200)).body;
      for (const [index, q] of opened.sections[0].questions.entries()) {
        const yes = index === 0 ? found : index < 3 ? fixed : false;
        await sm("put", `/sm/visits/${row.id}/answers/${q.id}`).send({ answer: { kind: "choice", optionCode: q.options[yes ? 0 : 1].code }, expectedAnswerVersion: 0, clientMutationToken: randomUUID() }).expect(200);
      }
      await sm("post", `/sm/visits/${row.id}/submit`).send({ actualMinutes: 30, visitStartedAt: `${day}T09:00:00Z`, visitCompletedAt: `${day}T09:30:00Z`, clientMutationToken: randomUUID() }).expect(200);
      // Place the synthetic real submission into a deterministic reporting period.
      await f.database.update(f.schema.smQuestionnaireSubmissions).set({ submittedAt: new Date(`${day}T09:30:00Z`), reportingAvailableAt: new Date(`${day}T09:30:00Z`) }).where(eq(f.schema.smQuestionnaireSubmissions.assignmentId, row.id));
    }
    await submit(markets.northBilla, f.employee, "2026-10-05", true, true);
    await submit(markets.northSpar, f.employee, "2026-10-06", true, false);
    await submit(markets.southBilla, second, "2026-10-07", false, false);
    await submit(markets.sharedHistory, second, "2026-10-08", true, true);
    // Existing standard history predates the additive Durcharbeit registry import.
    await f.database.insert(f.schema.smSMDurcharbeitMarkets).values([markets.dedicated, markets.sharedWeekly, markets.sharedHistory, markets.sharedPlan].map(smMarketId => ({ smMarketId, SMDurcharbeitSmUserId: second })));
    await submit(markets.dedicated, second, "2026-10-09", true, true, true);
    await submit(markets.sharedWeekly, second, "2026-10-10", true, true, true);
    await assignment(markets.sharedPlan, f.employee);
    return { ...f, second, daOnlySm, markets };
  } catch (error) { await f.pg.close(); throw error; }
}
