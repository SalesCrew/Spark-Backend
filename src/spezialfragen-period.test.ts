import assert from 'node:assert/strict';
import { test } from 'node:test';
import { isSpezialfrageActive, spezialfragePeriodError } from './lib/spezialfragen-period.js';
const config = (startDate: string, endDate = startDate) => ({ spezialfragePeriod: { startDate, endDate } });
test('existing unlimited questions stay available', () => {
  assert.equal(isSpezialfrageActive({}, new Date('2050-01-01')), true);
});
test('inclusive Vienna summer dates at the exact UTC boundary', () => {
  const c = config('2026-10-07');
  for (const [time, active] of [['2026-10-06T21:59:59.999Z', false], ['2026-10-06T22:00:00Z', true], ['2026-10-07T21:59:59.999Z', true], ['2026-10-07T22:00:00Z', false]] as const) assert.equal(isSpezialfrageActive(c, new Date(time)), active, time);
});
test('winter and daylight saving transitions use calendar dates rather than 24-hour durations', () => {
  for (const [day, start, end] of [['2026-01-05','2026-01-04T23:00:00Z','2026-01-05T23:00:00Z'],['2026-03-29','2026-03-28T23:00:00Z','2026-03-29T22:00:00Z'],['2026-10-25','2026-10-24T22:00:00Z','2026-10-25T23:00:00Z']]) {
    assert.equal(isSpezialfrageActive(config(day), new Date(start)), true);
    assert.equal(isSpezialfrageActive(config(day), new Date(Date.parse(end)-1)), true);
    assert.equal(isSpezialfrageActive(config(day), new Date(end)), false);
  }
});
test('malformed, incomplete, impossible and reversed dates cannot activate a question', () => {
  for (const value of [null, '', [], {}, { startDate: '2026-10-07' }, { startDate: '2026-02-29', endDate: '2026-03-01' }, { startDate: '2026-10-08', endDate: '2026-10-07' }, { startDate: '2026-10-07T00:00:00Z', endDate: '2026-10-08' }]) {
    const c = { spezialfragePeriod: value }; assert.ok(spezialfragePeriodError(c)); assert.equal(isSpezialfrageActive(c, new Date('2026-10-07T10:00:00Z')), false);
  }
  assert.equal(spezialfragePeriodError(config('2028-02-29')), null);
  assert.equal(isSpezialfrageActive(config('2026-10-07'), new Date('invalid')), false);
});
