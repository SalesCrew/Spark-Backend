// Shared report contract and physical-quantity rules. No database or environment imports.
import type { DashboardScope } from "./gm-dashboard.shared.js";
import { availabilityKinds, availabilityCounts, availabilityChainGroup, type AvailabilityAudit, type AvailabilityCounts, type AvailabilityKind } from "./gm-availability.shared.js";

export type ReportPeriod = { id: string; label: string; start: string; end: string };
export type PlacementKind = "cooler" | "placement";
export const coolerBrands = ["Coke", "Monster", "Jack&Coke", "Bacardi&Coke", "RedBull", "Fritz-Kola", "Vöslauer", "Rauch", "Hell", "Sonstige"] as const;
export const placementBrands = ["Coke", "RedBull", "Fritz-Kola", "Vöslauer", "Donat/Rogaska", "Sonstige"] as const;
export const reportBrands = [...new Set<string>([...coolerBrands, ...placementBrands])];
export type ReportMapping = { questionId: string; kind: PlacementKind | null; brand: string | null; excluded?: boolean | undefined };
export type ReportRequest = { kind: "availability" | "competitor"; periods: ReportPeriod[]; cutoff: string; scope: DashboardScope; mappings?: ReportMapping[] | undefined };
export type ReportVisit = { periodId: string; sessionId: string; marketId: string; marketName: string; chain: string; region: string; date: string; submittedAt: string };
export type ReportQuestion = ReportVisit & {
  id: string; sectionId: string; questionId: string; text: string; moduleName: string; questionType: string;
  available: boolean; availabilityType: string | null; applies: boolean; visible: boolean;
  rules: Array<Record<string, unknown>>; valueText: string | null; valueNumber: string | number | null; raw: unknown;
  options: string[]; comment: string; answerId: string | null; answered: boolean; valid: boolean; changedAt: string;
  isCompetitor: boolean; isCokePlacement: boolean;
};
export type ReportCatalogQuestion = ReportMapping & { text: string; moduleName: string; quantityAnswers: number; answers: number };
export type ReportPhoto = { id: string; url: string | null; marketName: string; questionText: string };
export type ReportIssue = { periodId: string; questionId: string; brand: string | null; text: string; count: number };
export type ReportCountRow = { label: string; total: number; counts: Record<AvailabilityKind, AvailabilityCounts> };
export type ReportWeeklyPoint = { label: string; start: string; end: string; counts: Record<AvailabilityKind, AvailabilityCounts> };
export type DashboardReport = {
  kind: ReportRequest["kind"]; periods: ReportPeriod[]; cutoff: string; scope: DashboardScope; generatedAt: string;
  visitCount: number; uniqueMarketCount: number; groups: { rewe: number; spar: number; other: number };
  availability: { total: ReportCountRow; regions: ReportCountRow[]; chains: ReportCountRow[]; weeks: { rewe: ReportWeeklyPoint[]; spar: ReportWeeklyPoint[] } } | null;
  competitor: { cooler: { brand: string; values: Array<number | null> }[]; placement: { brand: string; values: Array<number | null> }[]; catalog: ReportCatalogQuestion[]; issues: ReportIssue[]; countedAnswers: number; eligibleAnswers: number; excludedQuestions: number } | null;
  photos: ReportPhoto[]; photosAllowed: boolean; warnings: string[];
};
const normalize = (text: string) => text.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]/g, "");
const aliases: Record<string, string[]> = {
  Coke: ["coke", "coca cola", "coca-cola"], Monster: ["monster"], "Jack&Coke": ["jack&coke", "jack and coke", "jack daniels coke"],
  "Bacardi&Coke": ["bacardi&coke", "bacardi coke"], RedBull: ["redbull", "red bull"], "Fritz-Kola": ["fritz-kola", "fritz kola"],
  "Vöslauer": ["vöslauer", "voslauer"], Rauch: ["rauch"], Hell: ["hell"], "Donat/Rogaska": ["donat/rogaska", "donat", "rogaska"], Sonstige: ["sonstige", "sonstiges", "other"],
};
export function canonicalReportBrand(value: string): string | null {
  const key = normalize(value);
  return reportBrands.find(brand => aliases[brand]!.some(alias => normalize(alias) === key)) ?? null;
}
export function suggestReportMapping(q: Pick<ReportQuestion, "questionId" | "text" | "isCokePlacement">): ReportMapping {
  const text = normalize(q.text);
  const kind = /kuhl|kuel|cooler|kuehl/.test(text) ? "cooler" : /grossplatz|groplatz|platzierung|display|schutt|schuett|placement|rack/.test(text) ? "placement" : null;
  // Longer brand names must win over Coke, which also appears in Jack&Coke.
  const matches = reportBrands.filter(brand => aliases[brand]!.some(alias => {
    const words = alias.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[.*+?^${}()|[\]\\]/g, "\\$&").replace(/\s+/g, "[ -]*");
    return new RegExp(`(?:^|[^a-z])${words}(?:$|[^a-z])`, "i").test(q.text.normalize("NFD").replace(/[\u0300-\u036f]/g, ""));
  })).sort((a, b) => b.length - a.length);
  return { questionId: q.questionId, kind, brand: matches[0] ?? (q.isCokePlacement ? "Coke" : null) };
}
function quantity(value: unknown): number | null {
  if (value === null || value === undefined || value === "" || typeof value === "boolean") return null;
  if (typeof value !== "number" && (typeof value !== "string" || !/^\d+(?:[.,]0+)?$/.test(value.trim()))) return null;
  const n = Number(typeof value === "string" ? value.replace(",", ".") : value);
  return Number.isSafeInteger(n) && n >= 0 && n <= 1_000_000 ? n : null;
}
function strings(raw: unknown): string[] {
  if (typeof raw === "string") { try { if (/^[{[]/.test(raw)) return strings(JSON.parse(raw)); } catch {} return [raw]; }
  if (Array.isArray(raw)) return raw.flatMap(strings);
  if (raw && typeof raw === "object") return [strings((raw as Record<string, unknown>).sel), strings((raw as Record<string, unknown>).subs)].flat();
  return [];
}
export function physicalQuantities(q: Pick<ReportQuestion, "valueText" | "valueNumber" | "raw" | "options" | "comment">, mapping: ReportMapping): { values: Map<string, number>; issue: string | null } {
  const values = new Map<string, number>();
  let conflict = false;
  const put = (brand: string, value: number) => { if (values.has(brand) && values.get(brand) !== value) conflict = true; values.set(brand, value); };
  const direct = quantity(q.valueNumber) ?? quantity(q.raw) ?? quantity(q.valueText);
  if (direct !== null && mapping.brand) put(mapping.brand, direct);
  const selected = [...new Set([q.valueText ?? "", ...strings(q.raw), ...q.options].map(s => s.trim()).filter(Boolean))];
  const absent = selected.some(s => /^(nein|no|nicht vorhanden|keine|0)$/i.test(s));
  if (absent) {
    if (selected.some(s => /^(ja|yes)$/i.test(s))) return { values, issue: "Widersprüchliche Antwort" };
  }
  const raw = q.raw && typeof q.raw === "object" && !Array.isArray(q.raw) ? q.raw as Record<string, unknown> : null;
  const structured = raw?.quantities;
  if (structured && typeof structured === "object" && !Array.isArray(structured)) {
    for (const [label, value] of Object.entries(structured)) { const brand = canonicalReportBrand(label), n = quantity(value); if (brand && n !== null) put(brand, n); else conflict = true; }
  }
  const fragments = [...selected, ...q.comment.split(/[;\n]/)].map(s => s.trim()).filter(Boolean);
  for (const fragment of fragments) {
    const forward = fragment.match(/^(.+?)\s*[:=×x]\s*(\d+)(?:\s*(?:Stück|Stk\.?|Kühler|Platzierungen?))?$/i);
    const backward = fragment.match(/^(\d+)\s*[x×]\s*(.+)$/i);
    const brand = forward ? canonicalReportBrand(forward[1]!) : backward ? canonicalReportBrand(backward[2]!) : null;
    const n = quantity(forward?.[2] ?? backward?.[1]);
    if (brand && n !== null) put(brand, n);
    else if (forward || backward) conflict = true;
  }
  if (!values.size && mapping.brand) {
    const bare = q.comment.trim().match(/^(\d+)(?:\s*(?:Stück|Stk\.?|Kühler|Platzierungen?))?$/i);
    const n = quantity(bare?.[1]);
    if (n !== null) put(mapping.brand, n);
  }
  if (conflict || absent && [...values.values()].some(n => n > 0)) return { values: new Map(), issue: "Mengenangaben widersprechen sich" };
  if (absent) return { values: new Map((mapping.brand ? [mapping.brand] : (mapping.kind === "cooler" ? coolerBrands : placementBrands)).map(brand => [brand, 0])), issue: null };
  if (!values.size) return { values, issue: mapping.brand ? "Keine eindeutige physische Menge erfasst" : "Marke und Menge sind nicht eindeutig erfasst" };
  if (mapping.brand && [...values.keys()].some(b => b !== mapping.brand)) return { values: new Map(), issue: "Marke der Mengenangabe passt nicht zur Zuordnung" };
  return { values, issue: null };
}
export function buildCompetitorReport(rows: ReportQuestion[], periods: ReportPeriod[], mappings: ReportMapping[] = []): NonNullable<DashboardReport["competitor"]> {
  const map = new Map(mappings.map(m => [m.questionId, m]));
  const catalog = new Map<string, ReportCatalogQuestion>();
  const latest = new Map<string, ReportQuestion>();
  for (const q of rows) {
    if (!q.applies || !q.visible || !q.isCompetitor && !q.isCokePlacement) continue;
    const mapping = map.get(q.questionId) ?? suggestReportMapping(q);
    const item = catalog.get(q.questionId) ?? { ...mapping, text: q.text, moduleName: q.moduleName, quantityAnswers: 0, answers: 0 };
    if (q.answered && q.valid) { item.answers++; if (physicalQuantities(q, mapping).issue === null) item.quantityAnswers++; }
    catalog.set(q.questionId, item);
    const key = JSON.stringify([q.periodId, q.marketId, q.questionId]);
    const old = latest.get(key);
    const rank = (v: ReportQuestion) => `${v.submittedAt}|${v.changedAt}|${v.answerId ?? ""}|${v.id}`;
    if (!old || rank(q) > rank(old)) latest.set(key, q);
  }
  const totals = new Map<string, number>(), known = new Set<string>(), unknown = new Set<string>();
  const issues = new Map<string, ReportIssue>();
  let countedAnswers = 0, eligibleAnswers = 0;
  const key = (type: PlacementKind, brand: string, period: string) => {
    const templateBrands: readonly string[] = type === "cooler" ? coolerBrands : placementBrands;
    return JSON.stringify([type, templateBrands.includes(brand) ? brand : "Sonstige", period]);
  };
  for (const q of latest.values()) {
    const mapping = map.get(q.questionId) ?? suggestReportMapping(q);
    if (mapping.excluded) continue;
    eligibleAnswers++;
    const parsed = q.answered && q.valid ? physicalQuantities(q, mapping) : { values: new Map<string, number>(), issue: q.answerId && !q.valid ? "Antwort ungültig" : "Mengenfrage nicht beantwortet" };
    const issue = !mapping.kind ? "Frage muss Kühlern oder Großplatzierungen zugeordnet werden" : parsed.issue;
    if (issue) {
      const issueKey = JSON.stringify([q.periodId, q.questionId, issue]);
      const item = issues.get(issueKey) ?? { periodId: q.periodId, questionId: q.questionId, brand: mapping.brand, text: issue, count: 0 };
      item.count++; issues.set(issueKey, item);
      for (const type of mapping.kind ? [mapping.kind] : ["cooler", "placement"] as const) for (const brand of mapping.brand ? [mapping.brand] : reportBrands) unknown.add(key(type, brand, q.periodId));
      continue;
    }
    countedAnswers++;
    for (const [brand, n] of parsed.values) {
      const k = key(mapping.kind!, brand, q.periodId); known.add(k); totals.set(k, (totals.get(k) ?? 0) + n);
    }
  }
  const series = (type: PlacementKind, brands: readonly string[]) => brands.map(brand => ({ brand, values: periods.map(p => { const k = key(type, brand, p.id); return known.has(k) && !unknown.has(k) ? totals.get(k) ?? 0 : null; }) }));
  return { cooler: series("cooler", coolerBrands), placement: series("placement", placementBrands), catalog: [...catalog.values()], issues: [...issues.values()], countedAnswers, eligibleAnswers, excludedQuestions: [...catalog.values()].filter(q => q.excluded).length };
}
export function weekPeriods(start: string, end: string): ReportPeriod[] {
  const out: ReportPeriod[] = [], cursor = new Date(start + "T12:00:00Z"), last = Date.parse(end + "T12:00:00Z");
  while (cursor.getTime() <= last) {
    const from = cursor.toISOString().slice(0, 10), offset = (cursor.getUTCDay() + 6) % 7;
    const sunday = new Date(cursor); sunday.setUTCDate(sunday.getUTCDate() + 6 - offset);
    const to = sunday.toISOString().slice(0, 10) < end ? sunday.toISOString().slice(0, 10) : end;
    const thursday = new Date(cursor); thursday.setUTCDate(thursday.getUTCDate() + 3 - offset);
    const year = thursday.getUTCFullYear(), jan4 = new Date(Date.UTC(year, 0, 4, 12)), jan4Offset = (jan4.getUTCDay() + 6) % 7;
    const monday = new Date(jan4); monday.setUTCDate(monday.getUTCDate() - jan4Offset);
    const week = Math.floor((thursday.getTime() - monday.getTime()) / (7 * 86400000)) + 1;
    out.push({ id: `${year}-W${String(week).padStart(2, "0")}`, label: `KW${week}`, start: from, end: to });
    cursor.setTime(Date.parse(to + "T12:00:00Z") + 86400000);
  }
  return out;
}
export function buildAvailabilityReport(audit: AvailabilityAudit[], visits: ReportVisit[], period: ReportPeriod): NonNullable<DashboardReport["availability"]> {
  const relevant = audit.filter(a => a.intervalId === period.id);
  const makeRow = (label: string, predicate: (a: AvailabilityAudit) => boolean, visitPredicate: (v: ReportVisit) => boolean): ReportCountRow => {
    const rows = relevant.filter(predicate);
    return { label, total: new Set(visits.filter(visitPredicate).map(v => v.sessionId)).size,
      counts: Object.fromEntries(availabilityKinds.map(type => [type, availabilityCounts(rows.filter(a => a.included && a.availabilityType === type).map(a => a.category!))])) as ReportCountRow["counts"] };
  };
  const regions = [...new Set(visits.map(v => v.region))].sort((a,b) => a.localeCompare(b,"de"));
  const chains = [...new Set(visits.map(v => v.chain || "Sonstige"))].sort((a,b) => a.localeCompare(b,"de"));
  const weekly = (group: "rewe" | "spar") => weekPeriods(period.start, period.end).map(week => {
    const row = makeRow(week.label, a => availabilityChainGroup(a.chain) === group && a.visitDate >= week.start && a.visitDate <= week.end, v => availabilityChainGroup(v.chain) === group && v.date >= week.start && v.date <= week.end);
    return { label: week.label, start: week.start, end: week.end, counts: row.counts };
  });
  return { total: makeRow("Gesamt", () => true, () => true), regions: regions.map(r => makeRow(r, a => a.region === r, v => v.region === r)),
    chains: chains.map(c => makeRow(c, a => (a.chain || "Sonstige") === c, v => (v.chain || "Sonstige") === c)), weeks: { rewe: weekly("rewe"), spar: weekly("spar") } };
}
