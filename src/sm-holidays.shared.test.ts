import assert from "node:assert/strict";
import test from "node:test";
import { austrianHoliday, austrianHolidays, chooseSmHolidayDate, holidayWeekday } from "./sm-holidays.shared.js";

test("Austrian holidays match the official 2026–2028 calendar", () => {
  const official = {
    2026: ["01-01", "01-06", "04-06", "05-01", "05-14", "05-25", "06-04", "08-15", "10-26", "11-01", "12-08", "12-25", "12-26"],
    2027: ["01-01", "01-06", "03-29", "05-01", "05-06", "05-17", "05-27", "08-15", "10-26", "11-01", "12-08", "12-25", "12-26"],
    2028: ["01-01", "01-06", "04-17", "05-01", "05-25", "06-05", "06-15", "08-15", "10-26", "11-01", "12-08", "12-25", "12-26"],
  };
  for (const [year, dates] of Object.entries(official)) assert.deepEqual(austrianHolidays(Number(year)).map((h) => h.date), dates.map((d) => `${year}-${d}`));
  assert.equal(austrianHoliday("2026-12-24"), null);
  assert.equal(austrianHoliday("2026-04-03"), null); // Good Friday is not a nationwide public holiday.
});

test("the next ten years and beyond are calculated locally, with correct movable weekdays", () => {
  for (let year = 2026; year <= 2040; year++) {
    const holidays = austrianHolidays(year);
    assert.equal(holidays.length, 13);
    for (const h of holidays) {
      assert.equal(new Date(`${h.date}T12:00:00Z`).toISOString().slice(0, 10), h.date);
      if (["Ostermontag", "Pfingstmontag"].includes(h.name)) assert.equal(holidayWeekday(h.date), 1);
      if (["Christi Himmelfahrt", "Fronleichnam"].includes(h.name)) assert.equal(holidayWeekday(h.date), 4);
    }
  }
});

test("Monday only forwards, Friday only backwards even against lower load", () => {
  assert.equal(chooseSmHolidayDate("2026-10-26", "2026-01-01", () => 600)!.adjustedDate, "2026-10-27");
  assert.equal(chooseSmHolidayDate("2026-05-01", "2026-01-01", () => 600)!.adjustedDate, "2026-04-30");
});

test("Tuesday: Monday 2h versus Wednesday 6h selects Monday; ties prefer later", () => {
  const result = chooseSmHolidayDate("2026-12-08", "2026-01-01", (date) => date === "2026-12-07" ? 120 : 360)!;
  assert.equal(result.adjustedDate, "2026-12-07"); assert.equal(result.previousMinutes, 120); assert.equal(result.nextMinutes, 360);
  assert.equal(chooseSmHolidayDate("2026-12-08", "2026-01-01", () => 120)!.adjustedDate, "2026-12-09");
});

test("adjacent holidays, weekends and year boundaries never produce a non-working target", () => {
  for (let year = 2026; year <= 2036; year++) for (const h of austrianHolidays(year)) {
    const result = chooseSmHolidayDate(h.date, "2025-01-01", () => 0)!;
    assert.ok(holidayWeekday(result.adjustedDate) <= 5);
    assert.equal(austrianHoliday(result.adjustedDate), null);
  }
  assert.equal(chooseSmHolidayDate("2028-12-25", "2028-01-01", () => 0)!.adjustedDate, "2028-12-27");
  assert.equal(chooseSmHolidayDate("2027-01-01", "2026-01-01", () => 0)!.adjustedDate, "2026-12-31");
});

test("never backdate a new plan; a same-day Friday falls back to the next working day", () => {
  assert.equal(chooseSmHolidayDate("2026-12-08", "2026-12-08", () => 0)!.adjustedDate, "2026-12-09");
  assert.equal(chooseSmHolidayDate("2026-05-01", "2026-05-01", () => 0)!.adjustedDate, "2026-05-04");
  assert.equal(chooseSmHolidayDate("2026-08-31", "2026-01-01", () => 0), null);
});
