import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { eq } from 'drizzle-orm';
import request from 'supertest';
import { marketClassificationFixture } from './gm-market-classification-fixture.js';

test('GM market classifications save atomically, preserve history, and reject invalid/concurrent edits', async t => {
  const f = await marketClassificationFixture(); t.after(() => f.pg.close());
  const { markets, marketKuehlerUnits, visitSessions, visitSessionSections, visitSessionQuestions, visitAnswers, specialArthurFilter } = f.schema;
  const cooler = f.ids.market;
  await f.database.update(markets).set({ marketType: 'kuehler', universeMarket: false, flexNumber: null, cokeMasterNumber: 'synthetic-100', kuehlerStammnr: 'synthetic-100', updatedAt: new Date('2026-10-01T00:00:00Z') }).where(eq(markets.id, cooler));
  const unit = randomUUID(), visit = randomUUID(), section = randomUUID(), question = randomUUID(), answer = randomUUID(), campaign = randomUUID(), bankQuestion = randomUUID();
  await f.database.insert(f.schema.campaigns).values({ id: campaign, name: "Historical synthetic campaign", section: "kuehler", startDate: "2026-09-01", endDate: "2026-09-30" });
  await f.database.insert(f.schema.questionBankShared).values({ id: bankQuestion, questionType: "numeric", text: "Historical bank question" });
  await f.database.insert(marketKuehlerUnits).values({ id: unit, marketId: cooler, kuehlerInternalId: 'synthetic-unit-1' });
  await f.database.insert(visitSessions).values({ id: visit, marketId: cooler, gmUserId: f.ids.gm, kuehlerUnitId: unit, status: 'submitted', submittedAt: new Date('2026-09-20T12:00:00Z') });
  await f.database.insert(visitSessionSections).values({ id: section, visitSessionId: visit, campaignId: campaign, section: 'kuehler', fragebogenNameSnapshot: 'Historical synthetic cooler questionnaire', status: 'submitted' });
  await f.database.insert(visitSessionQuestions).values({ id: question, visitSessionSectionId: section, questionId: bankQuestion, questionType: 'numeric', questionTextSnapshot: 'Historical snapshot' });
  const questionRow = (await f.database.select().from(visitSessionQuestions))[0]!;
  await f.database.insert(visitAnswers).values({ id: answer, visitSessionId: visit, visitSessionSectionId: section, visitSessionQuestionId: question, questionId: questionRow.questionId, questionType: 'numeric', valueNumber: '7', answerStatus: 'answered' });
  await f.database.insert(specialArthurFilter).values({ gmUserId: f.ids.gm, matchValue: 'synthetic-100' });
  const history = async () => ({ units: await f.database.select().from(marketKuehlerUnits), visits: await f.database.select().from(visitSessions), sections: await f.database.select().from(visitSessionSections), questions: await f.database.select().from(visitSessionQuestions), answers: await f.database.select().from(visitAnswers), filters: await f.database.select().from(specialArthurFilter) });
  const originalHistory = await history();
  const read = async (id = cooler) => (await f.database.select().from(markets).where(eq(markets.id, id)))[0]!;
  const patch = (body: object, id = cooler, suffix = '') => request(f.app).patch(`/admin/markets/${id}${suffix}`).set('Authorization', 'Bearer synthetic-gm-admin').send(body);
  await t.test('authorization and invalid type', async () => {
    assert.equal((await request(f.app).patch(`/admin/markets/${cooler}`).send({ marketType: 'both' })).status, 401);
    assert.equal((await request(f.app).patch(`/admin/markets/${cooler}`).set('Authorization', 'Bearer synthetic-gm').send({ marketType: 'both' })).status, 403);
    assert.equal((await patch({ marketType: 'none' })).status, 400);
  });
  await t.test('missing requirements never write a partial promotion', async () => {
    const before = await read();
    for (const body of [{ marketType: 'both', universeMarket: true }, { marketType: 'both', flexNumber: '   ' }, { marketType: 'both', flexNumber: 'new-flex', address: '  ' }, { marketType: 'both', flexNumber: 'new-flex', region: 'Invalid region' }]) {
      const result = await patch(body); assert.equal(result.status, 400, JSON.stringify(result.body));
      assert.deepEqual(await read(), before); assert.deepEqual(await history(), originalHistory);
    }
    assert.equal((await patch({ universeMarket: true }, cooler, '/universe-market')).status, 400);
    assert.deepEqual(await read(), before);
  });
  await t.test('unique Flex collision rolls back all edited fields', async () => {
    await f.database.insert(markets).values({ id: randomUUID(), name: 'Other synthetic', address: 'Testgasse 2', postalCode: '1010', city: 'Wien', region: 'Ost', flexNumber: 'taken-flex' });
    const before = await read(); const result = await patch({ marketType: 'both', universeMarket: true, flexNumber: 'taken-flex', name: 'Changed' });
    assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(await read(), before); assert.deepEqual(await history(), originalHistory);
  });
  await t.test('valid promotion keeps market, cooler and every historical snapshot', async () => {
    const before = await read(); const result = await patch({ marketType: 'both', universeMarket: true, flexNumber: '  new-flex  ', expectedUpdatedAt: before.updatedAt.toISOString() });
    assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.market.marketType, 'both'); assert.equal(result.body.market.universeMarket, true);
    const saved = await read(); assert.equal(saved.id, cooler); assert.equal(saved.flexNumber, 'new-flex'); assert.equal(saved.kuehlerStammnr, 'synthetic-100'); assert.equal(saved.cokeMasterNumber, 'synthetic-100');
    assert.deepEqual(await history(), originalHistory);
    const list = await request(f.app).get('/markets').set('Authorization', 'Bearer synthetic-gm-admin'); assert.equal(list.status, 200, JSON.stringify(list.body));
    assert.equal(list.body.markets.find((m: any) => m.id === cooler).marketType, 'both');
  });
  await t.test('removing either type retains all identifiers and history; last type cannot be removed', async () => {
    assert.equal((await patch({ marketType: 'universum', universeMarket: true })).status, 200);
    assert.equal((await read()).kuehlerStammnr, 'synthetic-100');
    assert.equal((await patch({ universeMarket: false }, cooler, '/universe-market')).status, 400);
    assert.equal((await patch({ flexNumber: '' })).status, 400);
    assert.equal((await patch({ marketType: 'both', universeMarket: true })).status, 200);
    assert.equal((await patch({ universeMarket: false }, cooler, '/universe-market')).status, 200);
    assert.equal((await read()).marketType, 'kuehler'); assert.equal((await read()).flexNumber, 'new-flex');
    const result = await patch({ universeMarket: true }, cooler, '/universe-market'); assert.equal(result.status, 200); assert.equal(result.body.market.marketType, 'both');
    assert.deepEqual(await history(), originalHistory);
  });
  await t.test('each successful save advances the revision, even after a future timestamp', async () => {
    await f.database.update(markets).set({ updatedAt: new Date('2099-01-01T00:00:00Z') }).where(eq(markets.id, cooler));
    const old = await read();
    assert.equal((await patch({ infoNote: 'Revision test', expectedUpdatedAt: old.updatedAt.toISOString() })).status, 200);
    assert.ok((await read()).updatedAt.getTime() > old.updatedAt.getTime());
    assert.equal((await patch({ infoNote: 'Stale revision', expectedUpdatedAt: old.updatedAt.toISOString() })).status, 409);
  });
  await t.test('stale and contradictory edits fail before writing', async () => {
    const old = await read(); await f.database.update(markets).set({ name: 'Updated by another synthetic admin', updatedAt: new Date('2026-10-14T20:00:00Z') }).where(eq(markets.id, cooler));
    const before = await read(); const result = await patch({ name: 'Stale draft', expectedUpdatedAt: old.updatedAt.toISOString() });
    assert.equal(result.status, 409, JSON.stringify(result.body)); assert.equal(result.body.code, 'market_changed'); assert.deepEqual(await read(), before);
    assert.equal((await patch({ marketType: 'kuehler', universeMarket: true })).status, 400); assert.deepEqual(await read(), before);
  });
  await t.test('adding cooler requires a Stammnummer; ordinary legacy Universum edits stay possible', async () => {
    const id = randomUUID(); await f.database.insert(markets).values({ id, name: 'Legacy synthetic Universum', address: 'Testgasse 3', postalCode: '1010', city: 'Wien', region: 'Ost', marketType: 'universum', universeMarket: false });
    const before = await read(id); assert.equal((await patch({ marketType: 'both', universeMarket: true }, id)).status, 400); assert.deepEqual(await read(id), before);
    assert.equal((await patch({ infoNote: 'Ordinary edit', marketType: 'universum', universeMarket: false }, id)).status, 200);
    assert.equal((await read(id)).universeMarket, false); assert.equal((await read(id)).flexNumber, null);
    assert.equal((await patch({ marketType: 'both', universeMarket: true, cokeMasterNumber: 'new-cooler-200', kuehlerStammnr: 'new-cooler-200' }, id)).status, 200);
  });
});
