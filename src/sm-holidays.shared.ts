// Bundesweite Feiertage Österreichs. UI mirror: src/lib/sm/austrianHolidays.ts.
// Source: https://www.wien.gv.at/inhalt/feiertage (verified 2026-08-31).
export type AustrianHoliday = { date: string; name: string };
const DAY = 86_400_000;
const cache = new Map<number, readonly AustrianHoliday[]>();
export function shiftHolidayDate(date: string, days: number): string {
  return new Date(Date.parse(`${date}T12:00:00Z`) + days * DAY).toISOString().slice(0, 10);
}
export function holidayWeekday(date: string): number { return new Date(`${date}T12:00:00Z`).getUTCDay() || 7; }
export function austrianHolidays(year: number): readonly AustrianHoliday[] {
  if (!Number.isInteger(year) || year < 1900 || year > 2199) throw new RangeError("Feiertagsjahr muss zwischen 1900 und 2199 liegen.");
  const cached = cache.get(year); if (cached) return cached;
  // Gregorian Meeus/Jones/Butcher computus; UTC calendar dates avoid DST shifts.
  const a = year % 19, b = Math.floor(year / 100), c = year % 100;
  const d = Math.floor(b / 4), e = b % 4, f = Math.floor((b + 8) / 25), g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30, i = Math.floor(c / 4), k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7, m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31), day = ((h + l - 7 * m + 114) % 31) + 1;
  const easter = `${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
  const fixed = [["01-01", "Neujahr"], ["01-06", "Heilige Drei Könige"], ["05-01", "Staatsfeiertag"], ["08-15", "Mariä Himmelfahrt"], ["10-26", "Nationalfeiertag"], ["11-01", "Allerheiligen"], ["12-08", "Mariä Empfängnis"], ["12-25", "Christtag"], ["12-26", "Stephanstag"]];
  const movable: [number, string][] = [[1, "Ostermontag"], [39, "Christi Himmelfahrt"], [50, "Pfingstmontag"], [60, "Fronleichnam"]];
  const result = Object.freeze([...fixed.map(([date, name]) => Object.freeze({ date: `${year}-${date}`, name: name! })), ...movable.map(([offset, name]) => Object.freeze({ date: shiftHolidayDate(easter, offset), name }))].sort((x, y) => x.date.localeCompare(y.date)));
  cache.set(year, result); return result;
}
export function austrianHoliday(date: string): AustrianHoliday | null {
  const year = Number(date.slice(0, 4));
  if (!Number.isInteger(year) || year < 1900 || year > 2199) return null;
  return austrianHolidays(year).find((holiday) => holiday.date === date) ?? null;
}
export function smHolidayCandidates(date: string, notBefore: string): { previous: string | null; next: string | null } {
  const weekday = holidayWeekday(date);
  const seek = (direction: number): string | null => {
    for (let distance = 1; distance <= 14; distance++) {
      const candidate = shiftHolidayDate(date, direction * distance);
      if (candidate < notBefore) { if (direction < 0) return null; continue; }
      if (holidayWeekday(candidate) <= 5 && !austrianHoliday(candidate)) return candidate;
    }
    return null;
  };
  const previous = weekday === 1 ? null : seek(-1);
  // A Friday normally moves back; if that would backdate a live plan, use the next working day.
  const next = weekday === 5 && previous ? null : seek(1);
  return { previous, next };
}
export type SmHolidayAdjustment = {
  ruleVersion: 1; holidayDate: string; holidayName: string; adjustedDate: string;
  previousDate: string | null; nextDate: string | null; previousMinutes: number | null; nextMinutes: number | null;
  reason: "previous_only" | "next_only" | "less_load" | "tie_forward";
  manualOverride?: boolean;
};
export function chooseSmHolidayDate(date: string, notBefore: string, minutes: (date: string) => number): SmHolidayAdjustment | null {
  const holiday = austrianHoliday(date); if (!holiday) return null;
  const { previous, next } = smHolidayCandidates(date, notBefore);
  const previousMinutes = previous ? minutes(previous) : null, nextMinutes = next ? minutes(next) : null;
  if (!previous && !next) throw new Error("Kein zulässiger Ersatz-Werktag für Feiertag gefunden.");
  const adjustedDate = !previous ? next! : !next ? previous : previousMinutes! < nextMinutes! ? previous : next;
  return { ruleVersion: 1, holidayDate: date, holidayName: holiday.name, adjustedDate, previousDate: previous, nextDate: next, previousMinutes, nextMinutes, reason: !previous ? "next_only" : !next ? "previous_only" : previousMinutes === nextMinutes ? "tie_forward" : "less_load" };
}
