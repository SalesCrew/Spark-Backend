import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { test } from 'node:test';
import request from 'supertest';
import { spezialfragenPeriodFixture } from '../tests/spezialfragen-period-fixture.js';
import { isolatedModule } from '../tests/isolated-module.js';
import * as adminRole from './lib/admin-role.js';
import type { Request } from 'express';

test('campaign usage uses the existing questionnaire read permissions for every catalog', async () => {
  const access = await isolatedModule<typeof import('./lib/kunde-access.js')>(new URL('./lib/kunde-access.ts', import.meta.url), {
    './admin-role.js': adminRole,
    './db.js': { db: new Proxy({}, { get() { throw new Error('Permission resolution must not query a database'); } }) },
    './schema.js': {},
  });
  for (const [scope, pageKey] of [['main', 'fragebogen'], ['main', 'flexbesuche'], ['main', 'billa'], ['kuehler', 'kuehlerinventur'], ['mhd', 'mhd'], ['durcharbeit', 'durcharbeit']]) {
    const path = `/fragebogen/${scope}/campaign-usage`;
    const req = { path, originalUrl: `/admin${path}`, method: 'GET', get: (header: string) => header === 'x-coke-spark-page-key' ? pageKey : undefined } as Request;
    assert.deepEqual(JSON.parse(JSON.stringify(access.resolveKundeAdminRequirement(req))), { pageKey, action: 'read' });
  }
});

test('campaign switches drive all six GM catalog indicators and new visits without altering saved statuses or history', async () => {
  const f = await spezialfragenPeriodFixture(new Date('2026-10-08T10:00:00Z'), true);
  try {
    const admin = (verb: 'post' | 'get', path: string) => request(f.app)[verb](path).auth('synthetic-gm-admin', { type:'bearer' });
    const gm = (verb: 'post' | 'get', path: string) => request(f.app)[verb](path).auth('synthetic-gm', { type:'bearer' });
    const forms: Array<{ section: 'standard'|'flex'|'billa'|'kuehler'|'mhd'|'durcharbeit'; scope: string; old: string; next: string; campaign: string }> = [];
    for (const section of ['standard','flex','billa','kuehler','mhd','durcharbeit'] as const) {
      const scope = ['standard','flex','billa'].includes(section) ? 'main' : section;
      const create = async (status: string, name: string) => (await admin('post','/admin/fragebogen/' + scope).send({
        name, status, moduleIds: [], ...(scope === 'main' ? { sectionKeywords:[section] } : {}),
        spezialfragen:[{ id:randomUUID(),type:'yesno',text:name + ' question',required:true,config:{},rules:[],scoring:{} }],
      }).expect(201)).body.fragebogen.id;
      const old = await create('active',section + ' Q3'), next = await create('inactive',section + ' Q4'), campaign = randomUUID();
      await f.database.insert(f.schema.campaigns).values({ id:campaign,name:section + ' campaign',section,status:'active',scheduleType:'always',currentFragebogenId:old,assignedGmUserId:section==='flex'?f.ids.gm:null });
      await f.database.insert(f.schema.campaignMarketAssignments).values({ campaignId:campaign,marketId:f.ids.market,gmUserId:f.ids.gm });
      forms.push({ section,scope,old,next,campaign });
    }
    const selection = { marketId:f.ids.market,campaignIds:forms.map(x=>x.campaign) };
    const started = await gm('post','/gm/visit-sessions').send({ ...selection,clientSessionToken:'before-usage-switch' }).expect(201);
    const snapshots = await f.database.select().from(f.schema.visitSessionQuestions);
    const first = snapshots[0];
    await f.database.insert(f.schema.visitAnswers).values({ visitSessionId:started.body.session.id,visitSessionSectionId:first.visitSessionSectionId,visitSessionQuestionId:first.id,questionId:first.questionId,questionType:'yesno',answerStatus:'answered',valueText:'Ja',isValid:true });
    const answers = await f.database.select().from(f.schema.visitAnswers);
    const storedFormsBefore = await Promise.all([f.database.select().from(f.schema.fragebogenMain),f.database.select().from(f.schema.fragebogenKuehler),f.database.select().from(f.schema.fragebogenMhd),f.database.select().from(f.schema.fragebogenDurcharbeit)]);
    for (const form of forms) {
      const before = await admin('get','/admin/fragebogen/' + form.scope + '/campaign-usage').expect(200);
      assert.equal(before.headers['cache-control'],'no-store');
      const usageBefore = before.body.campaigns.filter((c: { section: string }) => c.section === form.section);
      assert.deepEqual(usageBefore.map((c: { currentFragebogenId: string }) => c.currentFragebogenId), [form.old]);
      await admin('post','/admin/campaigns/' + form.campaign + '/fragebogen/switch').send({ toFragebogenId:form.next }).expect(200);
      const after = await admin('get','/admin/fragebogen/' + form.scope + '/campaign-usage').expect(200);
      const usageAfter = after.body.campaigns.filter((c: { section: string }) => c.section === form.section);
      assert.deepEqual(usageAfter.map((c: { currentFragebogenId: string }) => c.currentFragebogenId), [form.next], 'saved inactive Q4 is really in use');
    }
    const newPreview = await gm('get','/gm/visit-sessions/start-payload').query({ marketId:f.ids.market,campaignIds:selection.campaignIds.join(',') }).expect(200);
    assert.deepEqual(newPreview.body.sections.map((s:any)=>s.fragebogenId).sort(),forms.map(f=>f.next).sort());
    const resumed = await gm('post','/gm/visit-sessions').send({ ...selection,clientSessionToken:'before-usage-switch' }).expect(200);
    assert.deepEqual(resumed.body.sections.map((s:any)=>s.fragebogenId).sort(),forms.map(f=>f.old).sort());
    assert.deepEqual(await f.database.select().from(f.schema.visitSessionQuestions),snapshots);
    assert.deepEqual(await f.database.select().from(f.schema.visitAnswers),answers);
    assert.deepEqual(await Promise.all([f.database.select().from(f.schema.fragebogenMain),f.database.select().from(f.schema.fragebogenKuehler),f.database.select().from(f.schema.fragebogenMhd),f.database.select().from(f.schema.fragebogenDurcharbeit)]),storedFormsBefore);
    assert.equal((await f.database.select().from(f.schema.campaignFragebogenHistory)).length,6,'only normal explicit switch history');

    const extra = forms[1];
    for (const [name,changes] of [['paused',{status:'inactive'}],['deleted',{isDeleted:true}],['unassigned',{currentFragebogenId:null}],['other scope',{section:'mhd'}]] as const) {
      await f.database.insert(f.schema.campaigns).values({ name,...{ section:'flex' as const,status:'active' as const,currentFragebogenId:extra.next },...changes });
    }
    const campaignsBeforeRead = await f.database.select().from(f.schema.campaigns);
    const read = await admin('get','/admin/fragebogen/main/campaign-usage').expect(200);
    assert.equal(read.body.campaigns.length,3);
    assert.equal(read.body.campaigns.some((c:any)=>['paused','deleted','unassigned','other scope'].includes(c.name)),false);
    assert.deepEqual(await f.database.select().from(f.schema.campaigns),campaignsBeforeRead,'GET is read-only');
    await request(f.app).get('/admin/fragebogen/main/campaign-usage').expect(401);
    await gm('get','/admin/fragebogen/main/campaign-usage').expect(403);
    await admin('get','/admin/fragebogen/invalid/campaign-usage').expect(400);
  } finally { await f.pg.close(); }
});
