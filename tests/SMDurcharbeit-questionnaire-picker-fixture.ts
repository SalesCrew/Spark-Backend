import { randomUUID } from "node:crypto";
import { createQuestionnaireUpdateFixture } from "./SMDurcharbeit-questionnaire-update-fixture.js";

/** Four synthetic questionnaire roots, matching the reported version/status pattern, plus real historical answers/photos. */
export async function createQuestionnairePickerFixture() {
  const f = await createQuestionnaireUpdateFixture();
  const forms = "/admin/sm-questionnaires/questionnaires", scope = "?scope=SMDurcharbeit";
  try {
    const spar1 = (await f.admin("patch", `${forms}/${f.form.id}${scope}`).send({ ...f.form, name: "Spar DA 200 ml - Tour 1" }).expect(200)).body.questionnaire;
    let rewe1 = (await f.admin("post", forms + scope).send({ ...f.form, id: `new-${randomUUID()}`, name: "Unbenannter Fragebogen", status: "active" }).expect(201)).body.questionnaire;
    for (let version = 2; version <= 5; version++) rewe1 = (await f.admin("patch", `${forms}/${rewe1.id}${scope}`).send({ ...rewe1, name: "Rewe DA 200ml - Tour 1", description: `Synthetische Fassung ${version}` }).expect(200)).body.questionnaire;
    async function copy(source: typeof spar1, name: string) {
      const row = (await f.admin("post", forms + scope).send({ ...source, id: `new-${randomUUID()}`, name: `Kopie von ${source.name}`, status: "inactive" }).expect(201)).body.questionnaire;
      return (await f.admin("patch", `${forms}/${row.id}${scope}`).send({ ...row, name }).expect(200)).body.questionnaire;
    }
    const spar2 = await copy(spar1, "Spar DA 200 ml - Tour 2"), rewe2 = await copy(rewe1, "Rewe DA 200ml - Tour 2");
    const historicalDraft = (await f.admin("post", f.root).send({ name: "Historischer Kampagnenentwurf", startDate: "2026-11-01", endDate: "2026-12-31", questionnaireVersionId: f.version.id, rosterDraft: [{ smMarketId: f.market }] }).expect(201)).body.campaign;
    return { ...f, forms, scope, spar1, rewe1, spar2, rewe2, historicalDraft };
  } catch (error) { await f.pg.close(); throw error; }
}
