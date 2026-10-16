import type { SmDashboardOosRow, SmDashboardVisitRow } from "./sm-dashboard.shared.js";
import { BASIC_ACTIVITY_CREDIT, smBasicActivityFromCode } from "./sm-basic-activity.shared.js";

export type SmOverviewVisit = SmDashboardVisitRow & { actualMinutes?: number | null };
export type SmBasicActivityRow = { submissionId: string; submissionQuestionId: string; metricOutcomeCode: string | null };
const rate = (numerator: number, denominator: number) => denominator ? numerator / denominator * 100 : null;

/** Separate upper KPIs: the lower charts and employee dashboard retain their existing contract. */
export function aggregateSmDashboardOverview(visitRows: readonly SmOverviewVisit[], rows: readonly SmDashboardOosRow[], basicRows: readonly SmBasicActivityRow[]) {
  const visits = [...new Map(visitRows.map(row => [row.submissionId, row])).values()];
  const ids = new Set(visits.map(row => row.submissionId));
  const markets = new Set(visits.map(row => row.marketId));
  const timed = visits.filter(row => row.actualMinutes != null && Number.isFinite(Number(row.actualMinutes)) && Number(row.actualMinutes) > 0);
  const marketMinutes = timed.reduce((sum, row) => sum + Number(row.actualMinutes), 0);
  const missingTimeVisits = visits.length - timed.length;
  const marketHours = timed.length ? marketMinutes / 60 : null;
  // A partial ZE total is shown with coverage; the average is unavailable until complete.
  const hoursPerVisit = visits.length && !missingTimeVisits ? marketMinutes / 60 / visits.length : null;
  const questions = new Map<string, SmDashboardOosRow[]>();
  for (const row of rows) {
    if (!ids.has(row.submissionId)) continue;
    const key = `${row.submissionId}:${row.submissionQuestionId}`;
    questions.set(key, [...(questions.get(key) ?? []), row]);
  }
  const collapsed = [...questions.values()].map(group => {
    const outcomes = new Set(group.map(row => row.outcome));
    const row = group[0]!;
    const priority = row.role === "oos_detection" ? ["oos_present", "oos_absent", "not_applicable"] : ["not_resolved", "partially_resolved", "resolved", "not_applicable"];
    return { ...row, outcome: priority.find(value => outcomes.has(value as SmDashboardOosRow["outcome"])) ?? null };
  });
  const remedies = new Map<string, typeof collapsed>();
  for (const row of collapsed) {
    if (row.role !== "oos_remediation" || !row.detectionQuestionRootId) continue;
    const key = `${row.submissionId}:${row.detectionQuestionRootId}:${row.category}`;
    remedies.set(key, [...(remedies.get(key) ?? []), row]);
  }
  const categories = new Map<string, { marketId: string; found: boolean; fixed: boolean; missing: boolean }>();
  for (const detection of collapsed.filter(row => row.role === "oos_detection" && (row.outcome === "oos_present" || row.outcome === "oos_absent"))) {
    const linked = remedies.get(`${detection.submissionId}:${detection.questionRootId}:${detection.category}`) ?? [];
    const documented = linked.filter(row => ["resolved", "partially_resolved", "not_resolved"].includes(row.outcome ?? ""));
    const found = detection.outcome === "oos_present";
    // Each found detection in a category must be fixed; one successful sibling cannot hide an unresolved case.
    const fixed = documented.some(row => row.outcome === "resolved" || row.outcome === "partially_resolved");
    const key = `${detection.submissionId}:${detection.category}`;
    const previous = categories.get(key);
    categories.set(key, { marketId: detection.marketId, found: found || previous?.found === true,
      fixed: (previous?.fixed ?? true) && (!found || fixed), missing: previous?.missing === true || found && !documented.length });
  }
  const checks = [...categories.values()];
  const found = checks.filter(row => row.found), fixed = found.filter(row => row.fixed);
  const observedMarkets = new Set(checks.map(row => row.marketId)).size;
  const affectedMarkets = new Set(found.map(row => row.marketId)).size;
  const basics = [...new Map(basicRows.filter(row => ids.has(row.submissionId)).map(row => [`${row.submissionId}:${row.submissionQuestionId}`, row])).values()]
    .flatMap(row => { const value = smBasicActivityFromCode(row.metricOutcomeCode); return value ? [value] : []; });
  const points = basics.reduce((sum, outcome) => sum + BASIC_ACTIVITY_CREDIT[outcome], 0);
  return {
    activity: { visitedMarkets: markets.size, visits: visits.length, merchandisers: new Set(visits.map(row => row.smUserId)).size,
      visitsPerMarket: visits.length && markets.size ? visits.length / markets.size : null,
      marketMinutes, timedVisits: timed.length, missingTimeVisits, marketHours, hoursPerVisit },
    performance: { basicActivityRate: rate(points, basics.length), basicActivityAnswers: basics.length, basicActivityPoints: points,
      basicActivityYes: basics.filter(value => value === "yes").length, basicActivityPartial: basics.filter(value => value === "partial").length, basicActivityNo: basics.filter(value => value === "no").length,
      affectedMarketRate: rate(affectedMarkets, observedMarkets), affectedMarkets, observedMarkets,
      foundCategoryCases: found.length, fixedCategoryCases: fixed.length, categoryChecks: checks.length,
      remediationRate: rate(fixed.length, found.length), availabilityBefore: rate(checks.length - found.length, checks.length),
      // Added share of the whole (+13.4%), never relative before/after growth.
      availabilityGain: rate(fixed.length, checks.length), availabilityAfter: rate(checks.length - found.length + fixed.length, checks.length),
      missingRemediationCases: found.filter(row => row.missing).length },
  };
}
