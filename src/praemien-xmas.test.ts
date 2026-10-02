import test from "node:test";
import assert from "node:assert/strict";
import { evaluateModel, modelTemplate, validateModel, type MetricEntry } from "./praemien-model.shared.js";
import { modelSchema, mutateWorkspace, simulateWorkspace, readWorkspace } from "./lib/praemien-workspace.js";
import { praemienFixture } from "./lib/praemien-test-fixture.js";
const gm = { gmId: "gm", name: "Synthetic GM", active: true };
const raw = { new_coolers: 0, recovered: 0, returned: 0, lost: 0, trucks: 0, trailers: 0, bins: 0, fsdu: 0, sleds: 0, pallets: 0, standees: 0, qualified: 1 };
function result(values: Record<string, number | null>, pillar = "flex") {
 const model = modelTemplate("xmas"); model.pillars = model.pillars.filter(p => p.key === pillar);
 return evaluateModel(model, [gm], Object.entries(values).map(([metricKey,value]): MetricEntry => ({ gmId: gm.gmId, pillarKey: pillar, metricKey, value, target: null, note: "synthetic" })), [])[0]!.pillars[0]!;
}
test("new Xmas template survives JSON schema with unchanged legacy templates", () => {
 for (const template of ["empty", "q1", "q2", "q3", "xmas"] as const) {
  const model = modelTemplate(template); assert.deepEqual(validateModel(model), []); assert.deepEqual(modelSchema.parse(model), model);
 }
 assert.equal(modelTemplate("q3").pillars[2]!.tiers.length, 0);
});
test("both 50% minima and combined 25/30 points determine payout", () => {
 for (const [net, points, amount] of [[-1,40,0],[0,19.5,0],[0,20,82.5],[1,20,165],[0,24.5,82.5],[0,25,165],[0,28,165],[1,28,165]] as const) {
  const r = result({ ...raw, new_coolers: Math.max(net,0), returned: Math.max(-net,0), bins: Math.floor(points), standees: points % 1 ? 1 : 0 });
  assert.equal(r.earned, amount, `net ${net}, Xmas ${points}`); assert.equal(r.pending, false);
 }
});
test("all seven Xmas categories have the documented weights, no normalization to 5/10", () => {
 const r = result({ ...raw, trucks: 2, trailers: 3, bins: 4, fsdu: 5, sleds: 6, pallets: 7, standees: 8 });
 assert.equal(r.metrics.find(m => m.key === "xmas_points")!.value, 31.5);
 assert.equal(r.metrics.find(m => m.key === "total")!.value, 36.5);
});
test("recovered coolers count as new; separate returns and lost reduce net", () => {
 const r = result({ ...raw, new_coolers: 2, recovered: 2, returned: 1, lost: 2, bins: 20 });
 assert.equal(r.metrics.find(m => m.key === "net")!.value, 1); assert.equal(r.earned, 165);
});
test("missing raw values/review stay pending; failed review denies payout; derived overrides ignored", () => {
 for (const key of ["new_coolers", "lost", "bins", "qualified"]) {
  const r = result({ ...raw, bins: 30, [key]: null }); assert.equal(r.pending, true); assert.equal(r.earned, 0);
 }
 assert.equal(result({ ...raw, bins: 30, qualified: 0 }).earned, 0);
 assert.equal(result({ ...raw, bins: 0, xmas_points: 50, total: 60 }).earned, 0);
});
test("manual quality accepts reviewed euros without invented thresholds; missing differs from zero; caps enforced", () => {
 const open = result({ reporting: 55, tags: 55 }, "quality"); assert.equal(open.earned, 110); assert.equal(open.pending, true);
 const done = result({ reporting: 55, tags: 0, time: 110 }, "quality"); assert.equal(done.earned, 165); assert.equal(done.pending, false);
 assert.equal(result({ reporting: 1000, tags: 1000, time: 1000 }, "quality").earned, 220);
 assert.equal(result({ reporting: -1, tags: 0, time: 0 }, "quality").earned, 0);
});
test("synthetic PostgreSQL persistence/preview enforce range, qualification, derived locks and optimistic revisions", async () => {
 const f = await praemienFixture();
 try {
  // Reset only this fresh disposable fixture, never production.
  await f.pg.exec("delete from praemien_metric_entries");
  let w = await readWorkspace(f.database, f.ids.wave);
  w = await mutateWorkspace(f.database, f.ids.wave, w.revision, { id: f.ids.admin, name: "Synthetic admin" }, { type: "rules", model: modelTemplate("xmas") });
  const entry = (metricKey: string, value: number | null, pillarKey = "flex"): MetricEntry => ({ gmId: f.ids.gm, pillarKey, metricKey, value, target: null, note: "synthetic" });
  for (const bad of [entry("new_coolers", -1), entry("trucks", 0.5), entry("qualified", 2), entry("total", 30), entry("reporting", 55.01, "quality"), entry("time", -1, "quality")]) {
   await assert.rejects(simulateWorkspace(f.database, f.ids.wave, w.model!, [bad]));
   await assert.rejects(mutateWorkspace(f.database, f.ids.wave, w.revision, { id: f.ids.admin, name: "Synthetic admin" }, { type: "values", entries: [bad] }));
  }
  const entries = Object.entries({ ...raw, bins: 25 }).map(([k,v]) => entry(k,v));
  entries.push(entry("reporting",55,"quality"),entry("tags",0,"quality"),entry("time",110,"quality"));
  const preview = await simulateWorkspace(f.database,f.ids.wave,w.model!,entries);
  assert.equal(preview.results.find(r=>r.gmId===f.ids.gm)!.pillars[2]!.earned,165);
  assert.equal((await readWorkspace(f.database,f.ids.wave)).revision,w.revision);
  w = await mutateWorkspace(f.database,f.ids.wave,w.revision,{id:f.ids.admin,name:"Synthetic admin"},{type:"values",entries});
  const reloaded = await readWorkspace(f.database,f.ids.wave);
  assert.deepEqual(reloaded.model,modelTemplate("xmas"));
  assert.equal(reloaded.results.find(r=>r.gmId===f.ids.gm)!.pillars[3]!.earned,165);
  assert.equal(reloaded.entries.length,15);
  const activated=await mutateWorkspace(f.database,f.ids.wave,w.revision,{id:f.ids.admin,name:"Synthetic admin"},{type:"activate"});
  assert.equal(activated.wave.status,"active");
  await assert.rejects(mutateWorkspace(f.database,f.ids.wave,activated.revision,{id:f.ids.admin,name:"Synthetic admin"},{type:"archive"}),/fehlen Bewertungen/);
  await assert.rejects(mutateWorkspace(f.database,f.ids.wave,w.revision-1,{id:f.ids.admin,name:"Synthetic admin"},{type:"values",entries}),/Zwischenzeitlich/);
 } finally { await f.pg.close(); }
});

test("invalid automatic category counts cannot quietly contribute weighted points", () => {
 const model=modelTemplate("xmas"); const p=model.pillars[2]!; model.pillars=[p];
 const trucks=p.metrics.find(m=>m.key==="trucks")!; trucks.method="answer_sum";
 trucks.sources=[{questionId:"synthetic",section:"flex",scoreKey:"__value__",factor:true,weight:1,label:"Synthetic count",minFrequency:0,chains:[],counting:"latest"}];
 const entries=Object.entries({...raw,bins:30}).filter(([k])=>k!=="trucks").map(([metricKey,value])=>({gmId:gm.gmId,pillarKey:"flex",metricKey,value,target:null,note:""}));
 const r=evaluateModel(model,[gm],entries,[{gmId:gm.gmId,marketId:"synthetic",questionId:"synthetic",section:"flex",date:"2026-10-01",id:"synthetic",numeric:0.5,options:[],frequency:0,chain:"Synthetic"}])[0]!.pillars[0]!;
 assert.equal(r.pending,true); assert.equal(r.earned,0);
});
