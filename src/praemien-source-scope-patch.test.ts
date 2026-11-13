import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
import { mutateWorkspace, readWorkspace } from "./lib/praemien-workspace.js";

test("guarded configuration patch changes one display exception and audits it atomically", async () => {
  const f = await praemienFixture();
  try {
    await f.pg.query("update praemien_waves set year=2026,quarter=4,start_date='2026-10-01',end_date='2026-12-31' where id=$1", [f.ids.wave]);
    const model = structuredClone(f.model), p = model.pillars[0]!, m = p.metrics[0]!;
    Object.assign(m, { method: "answer_sum", inputs: [], sources: [{ questionId: f.ids.question, section: "standard", scoreKey: "__value__", factor: true, weight: 1, label: "Synthetic source", minFrequency: 0, chains: [], counting: "latest" }] });
    let w = await mutateWorkspace(f.database, f.ids.wave, f.workspace.revision, { id: f.ids.admin, name: "Synthetic admin" }, { type: "rules", model });
    w = await mutateWorkspace(f.database, f.ids.wave, w.revision, { id: f.ids.admin, name: "Synthetic admin" }, { type: "activate" });
    const original = structuredClone(w.model);
    const entries = (await f.pg.query("select * from praemien_metric_entries order by gm_user_id,pillar_key,metric_key")).rows;
    const historyBefore = w.history.length;
    const template = await readFile(new URL("../tests/sql/praemien-display-source-scope.sql", import.meta.url), "utf8");
    const patch = (gmId = f.ids.gm, revision = w.revision, count = 1) => template
      .replaceAll("__WAVE_ID__", f.ids.wave).replaceAll("__GM_ID__", gmId)
      .replaceAll("__EXPECTED_REVISION__", String(revision)).replaceAll("__EXPECTED_SOURCE_COUNT__", String(count))
      .replaceAll("__PILLAR_KEY__", p.key).replaceAll("__METRIC_KEY__", m.key);
    for (const sql of [patch(randomUUID()), patch(f.ids.gm, 0), patch(f.ids.gm, w.revision, 2)]) {
      await assert.rejects(f.pg.exec(sql)); await f.pg.exec("ROLLBACK");
      assert.equal((await readWorkspace(f.database, f.ids.wave)).revision, w.revision);
    }
    await f.pg.exec(patch());
    const changed = await readWorkspace(f.database, f.ids.wave);
    assert.equal(changed.revision, w.revision + 1); assert.equal(changed.history.length, historyBefore + 1);
    const actual = structuredClone(changed.model!);
    assert.deepEqual(actual.pillars[0]!.metrics[0]!.sourceSectionOverrides, [{ gmId: f.ids.gm, sections: ["standard", "flex", "billa"] }]);
    delete actual.pillars[0]!.metrics[0]!.sourceSectionOverrides;
    assert.deepEqual(actual, original);
    assert.deepEqual((await f.pg.query("select * from praemien_metric_entries order by gm_user_id,pillar_key,metric_key")).rows, entries);
    const event = changed.history[0]!;
    assert.equal(event.type, "rules"); assert.deepEqual((event.payload as any).previousModel, original);
    await assert.rejects(f.pg.exec(patch())); await f.pg.exec("ROLLBACK");
    await f.pg.query("update praemien_wave_settings set closed_snapshot=$2::jsonb,closed_at=now() where wave_id=$1", [f.ids.wave, JSON.stringify(changed)]);
    await assert.rejects(f.pg.exec(patch(f.ids.gm, changed.revision))); await f.pg.exec("ROLLBACK");
  } finally { await f.pg.close(); }
});
