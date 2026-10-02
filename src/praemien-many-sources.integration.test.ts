import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
import { mutateWorkspace, readWorkspace, simulateWorkspace } from "./lib/praemien-workspace.js";
import { modelTemplate, type ModelSource } from "./praemien-model.shared.js";

test("80 assigned questions calculate from real normalized synthetic visits; preview is read-only", async () => {
  const f = await praemienFixture();
  try {
    await f.pg.exec("delete from praemien_metric_entries");
    const model = modelTemplate("empty"), pillar = model.pillars[0]!;
    pillar.name = "80 Testfragen"; pillar.maxRewardEur = 100;
    const sources: ModelSource[] = [];
    const section = randomUUID(), visits = [randomUUID(), randomUUID()];
    await f.pg.query("insert into visit_session_sections values($1,'standard',false)", [section]);
    for (const [index, visit] of visits.entries()) await f.pg.query("insert into visit_sessions values($1,$2,$3,'submitted',$4,false)", [visit, f.ids.gm, f.ids.market, index === 0 ? "2026-07-10T12:00:00Z" : "2026-08-10T12:00:00Z"]);
    for (let i = 0; i < 80; i++) {
      const question = randomUUID(), instance = randomUUID();
      await f.pg.query("insert into question_bank_shared(id,text,question_type) values($1,$2,'numeric')", [question, `Synthetic question ${i}`]);
      await f.pg.query("insert into visit_session_questions values($1,true,false)", [instance]);
      sources.push({ questionId: question, section: "standard", scoreKey: "__value__", factor: true, weight: 1, label: `Synthetic question ${i}`, minFrequency: 8, chains: ["Sparmarkt"], counting: "latest" });
      for (const [index, visit] of visits.entries()) await f.pg.query("insert into visit_answers values($1,$2,$3,$4,$5,$6,null,true,'answered',false)", [randomUUID(), visit, instance, section, question, index === 0 ? 2 : 1]);
    }
    pillar.metrics = [{ key: "questions", label: "Fragenpunkte", unit: "points", method: "answer_sum", inputs: [], target: null, steps: [], sources }];
    pillar.tiers = [{ key: "eighty", label: "80 Punkte", group: "", rewardEur: 100, conditions: [{ metricKey: "questions", operator: "gte", value: 80 }] }];
    const saved = await mutateWorkspace(f.database, f.ids.wave, f.workspace.revision, { id: f.ids.admin, name: "Synthetic admin" }, { type: "rules", model });
    const result = saved.results.find((r) => r.gmId === f.ids.gm)!;
    assert.equal(result.earned, 100); assert.equal(result.pillars[0]!.metrics[0]!.value, 80);
    assert.equal(result.pillars[0]!.metrics[0]!.counted, 80); assert.equal(result.pillars[0]!.metrics[0]!.excluded, 80);
    assert.equal(saved.model!.pillars[0]!.metrics[0]!.sources.length, 80);
    const simulated = structuredClone(model); simulated.pillars[0]!.metrics[0]!.sources[0]!.weight = 0;
    const preview = await simulateWorkspace(f.database, f.ids.wave, simulated);
    assert.equal(preview.results.find((r) => r.gmId === f.ids.gm)!.earned, 0);
    const after = await readWorkspace(f.database, f.ids.wave);
    assert.equal(after.revision, saved.revision); assert.equal(after.results.find((r) => r.gmId === f.ids.gm)!.earned, 100);
    assert.equal(after.model!.pillars[0]!.metrics[0]!.sources[0]!.weight, 1);
  } finally { await f.pg.close(); }
});
