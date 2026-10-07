export type SpezialfragePeriod = { startDate: string; endDate: string };

function isCalendarDate(value: unknown): value is string {
  if (typeof value !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T12:00:00Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

export function spezialfragePeriodError(config: Record<string, unknown>): string | null {
  if (config.spezialfragePeriod === undefined) return null;
  const period = config.spezialfragePeriod as Partial<SpezialfragePeriod> | null;
  if (!period || typeof period !== "object" || Array.isArray(period)
    || !isCalendarDate(period.startDate) || !isCalendarDate(period.endDate)) {
    return "Bitte wähle einen gültigen Beginn und ein gültiges Ende für den Spezialfragen-Zeitraum.";
  }
  if (period.startDate > period.endDate) return "Das Ende des Spezialfragen-Zeitraums darf nicht vor dem Beginn liegen.";
  return null;
}

export function isSpezialfrageActive(config: Record<string, unknown>, at: Date = new Date()): boolean {
  if (config.spezialfragePeriod === undefined) return true;
  if (spezialfragePeriodError(config) || !Number.isFinite(at.getTime())) return false;
  const period = config.spezialfragePeriod as SpezialfragePeriod;
  const date = new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Vienna", year: "numeric", month: "2-digit", day: "2-digit" }).format(at);
  return period.startDate <= date && date <= period.endDate;
}
