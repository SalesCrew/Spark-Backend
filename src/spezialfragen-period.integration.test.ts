import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { test } from 'node:test';
import request from 'supertest';
import { eq } from 'drizzle-orm';
import { spezialfragenPeriodFixture } from '../tests/spezialfragen-period-fixture.js';

test('actual authoring and visit lifecycle preserve timed selection, IDs and historical snapshots', async () => {
  const f = await spezialfragenPeriodFixture(new Date('2026-10-07T10:00:00Z'));
  try {
    const admin = (verb: 'post'|'patch'|'get', path: string) => request(f.app)[verb](path).auth('synthetic-gm-admin', { type: 'bearer' });
    const gm = (verb: 'post'|'get', path: string) => request(f.app)[verb](path).auth('synthetic-gm', { type: 'bearer' });
    const question = (text: string, config: Record<string,unknown> = {}, chains: string[] = []) => ({ id: randomUUID(), type: 'yesno', text, required: true, config, chains, rules: [], scoring: {} });
    const period = { spezialfragePeriod: { startDate: '2026-10-07', endDate: '2026-10-07' } };
    const timeless = question('Always available'), timed = question('Only today', period), future = question('Starts tomorrow', { spezialfragePeriod: { startDate: '2026-10-08', endDate: '2026-10-09' } }), expired = question('Ended yesterday', { spezialfragePeriod: { startDate: '2026-10-01', endDate: '2026-10-06' } }), otherChain = question('SPAR only', period, ['SPAR']);
    const allQuestions = [timeless, timed, future, expired, otherChain];
    const forms: Array<{scope:string;id:string; campaign:string}> = [];
    for (const [scope, section] of [['main','standard'],['main','flex'],['main','billa'],['kuehler','kuehler'],['mhd','mhd'],['durcharbeit','durcharbeit']]) {
      const body = { name: 'Synthetic ' + scope, status: 'active', moduleIds: [], spezialfragen: scope === 'durcharbeit' ? allQuestions.map(q => ({ ...q, id: randomUUID() })) : allQuestions };
      const result = await admin('post', '/admin/fragebogen/' + scope).send(body);
      assert.equal(result.status, 201, JSON.stringify(result.body));
      assert.equal(result.body.fragebogen.spezialfragen.length, 5, 'authoring shows future and expired questions');
      const campaign = randomUUID();
      await f.database.insert(f.schema.campaigns).values({ id: campaign, name: body.name, section: section as 'standard'|'flex'|'billa'|'kuehler'|'mhd'|'durcharbeit', assignedGmUserId: section === 'flex' ? f.ids.gm : null, status: 'active', currentFragebogenId: result.body.fragebogen.id, scheduleType: 'always' });
      await f.database.insert(f.schema.campaignMarketAssignments).values({ campaignId: campaign, marketId: f.ids.market, gmUserId: f.ids.gm });
      forms.push({ scope, id: result.body.fragebogen.id, campaign });
    }
    const select = { marketId: f.ids.market, campaignIds: forms.map(x => x.campaign) };
    for (const [time, expected] of [['2026-10-06T21:59:59.999Z','Ended yesterday'],['2026-10-06T22:00:00Z','Only today'],['2026-10-07T21:59:59.999Z','Only today'],['2026-10-07T22:00:00Z','Starts tomorrow']]) {
      f.setTime(time);
      const edge = await gm('get','/gm/visit-sessions/start-payload').query({ ...select, campaignIds:select.campaignIds.join(',') });
      assert.equal(edge.status,200,JSON.stringify(edge.body));
      for (const section of edge.body.sections) assert.deepEqual(section.questions.map((q:any)=>q.text),['Always available',expected]);
    }
    f.setTime('2026-10-07T10:00:00Z');
    const preview = await gm('get', '/gm/visit-sessions/start-payload').query({ ...select, campaignIds: select.campaignIds.join(',') });
    assert.equal(preview.status, 200, JSON.stringify(preview.body));
    assert.equal(preview.body.sections.length, 6);
    for (const section of preview.body.sections) assert.deepEqual(section.questions.map((q:any)=>q.text), ['Always available','Only today']);
    const start = await gm('post', '/gm/visit-sessions').send({ ...select, clientSessionToken: 'synthetic-period-session' });
    assert.equal(start.status, 201, JSON.stringify(start.body));
    const sessionId = start.body.session.id;
    const snapshotsBefore = await f.database.select().from(f.schema.visitSessionQuestions);
    const firstQuestion = start.body.sections[0].questions[1];
    const snapshot = snapshotsBefore.find(q => q.id === firstQuestion.id)!;
    await f.database.insert(f.schema.visitAnswers).values({ visitSessionId: sessionId, visitSessionSectionId: snapshot.visitSessionSectionId, visitSessionQuestionId: snapshot.id, questionId: firstQuestion.questionId, questionType: 'yesno', answerStatus: 'answered', valueText: 'Ja', isValid: true });
    const answersBefore = await f.database.select().from(f.schema.visitAnswers);
    // Scheduling metadata alone must not create semantic answer history or change saved snapshots.
    const main = forms[0];
    const changed = { ...timed, config: { spezialfragePeriod: { startDate:'2026-10-20', endDate:'2026-10-21' } } };
    const save = await admin('patch', '/admin/fragebogen/main/' + main.id).send({ name:'Synthetic main', status:'active', moduleIds:[], spezialfragen: allQuestions.map(q=>q.id === timed.id ? changed : q) });
    assert.equal(save.status, 200, JSON.stringify(save.body));
    assert.equal(save.body.fragebogen.spezialfragen[1].id, timed.id);
    assert.deepEqual(save.body.fragebogen.spezialfragen[1].config.spezialfragePeriod, changed.config.spezialfragePeriod);
    assert.deepEqual(await f.database.select().from(f.schema.questionAnswerHistory), []);
    assert.deepEqual(await f.database.select().from(f.schema.visitSessionQuestions), snapshotsBefore);
    assert.deepEqual(await f.database.select().from(f.schema.visitAnswers), answersBefore);
    const beforeBadSave = await f.database.select().from(f.schema.questionBankShared);
    const bad = await admin('patch', '/admin/fragebogen/main/' + main.id).send({ name:'Must roll back', status:'active', moduleIds:[], spezialfragen: [{ ...timed, config: { spezialfragePeriod: { startDate:'2026-10-09', endDate:'2026-10-08' } } }] });
    assert.equal(bad.status, 400, JSON.stringify(bad.body));
    assert.deepEqual(await f.database.select().from(f.schema.questionBankShared), beforeBadSave);
    assert.equal((await admin('get','/admin/fragebogen/main')).body.fragebogen.find((fb:any)=>fb.id===main.id).name, 'Synthetic main');
    f.setTime('2026-10-08T10:00:00Z');
    const resumed = await gm('post','/gm/visit-sessions').send({ ...select, clientSessionToken:'synthetic-period-session' });
    assert.equal(resumed.status,200,JSON.stringify(resumed.body));
    for (const section of resumed.body.sections) assert.deepEqual(section.questions.map((q:any)=>q.text), ['Always available','Only today']);
    assert.deepEqual(await f.database.select().from(f.schema.visitAnswers), answersBefore);
    // Completed visit syncing must not add tomorrow's question to a visit from yesterday.
    await f.database.update(f.schema.visitSessions).set({ status:'submitted', submittedAt:new Date('2026-10-07T11:00:00Z') }).where(eq(f.schema.visitSessions.id,sessionId));
    const sync = await gm('post','/gm/visit-sessions/' + sessionId + '/spezialfragen/sync').send({});
    assert.equal(sync.status,200,JSON.stringify(sync.body)); assert.equal(sync.body.addedQuestionCount,0);
    assert.deepEqual(await f.database.select().from(f.schema.visitSessionQuestions),snapshotsBefore);
    assert.deepEqual(await f.database.select().from(f.schema.visitAnswers),answersBefore);
    const nextPreview = await gm('get','/gm/visit-sessions/start-payload').query({ ...select, campaignIds:select.campaignIds.join(',') });
    assert.equal(nextPreview.status,200,JSON.stringify(nextPreview.body));
    for (const section of nextPreview.body.sections) assert.deepEqual(section.questions.map((q:any)=>q.text), ['Always available','Starts tomorrow']);
    const newStart = await gm('post','/gm/visit-sessions').send({ ...select, clientSessionToken:'synthetic-period-second-session' });
    assert.equal(newStart.status,201,JSON.stringify(newStart.body));
    for (const section of newStart.body.sections) assert.deepEqual(section.questions.map((q:any)=>q.text), ['Always available','Starts tomorrow']);
    const removeWindow = await admin('patch','/admin/fragebogen/main/' + main.id).send({ name:'Synthetic main', status:'active', moduleIds:[], spezialfragen:allQuestions.map(q=>q.id===timed.id ? { ...q, config:{} } : q) });
    assert.equal(removeWindow.status,200,JSON.stringify(removeWindow.body));
    const library = await admin('get','/admin/spezialfragen').query({ scope:'main' });
    assert.equal(library.status,200);
    assert.equal(library.body.spezialfragen.find((q:any)=>q.id===timed.id).config.spezialfragePeriod,undefined);
    assert.deepEqual(await f.database.select().from(f.schema.questionAnswerHistory),[]);
    assert.deepEqual(await f.database.select().from(f.schema.visitAnswers),answersBefore);
  } finally { await f.pg.close(); }
});
