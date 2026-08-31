export const SM_OOS_CATEGORY_ORDER = [
  "action_placements",
  "softdrinks_energy",
  "water_near_water",
  "juice_iced_tea",
] as const;

export type SmDashboardOosCategory = (typeof SM_OOS_CATEGORY_ORDER)[number];
export type SmDashboardOosRole = "oos_detection" | "oos_remediation";
export type SmDashboardOosOutcome =
  | "oos_present"
  | "oos_absent"
  | "resolved"
  | "partially_resolved"
  | "not_resolved"
  | "not_applicable";

export type SmDashboardVisitRow = {
  submissionId: string;
  marketId: string;
  marketName: string;
  chain: string;
  region: string;
  smUserId: string;
  smName: string;
};

export type SmDashboardOosRow = SmDashboardVisitRow & {
  submissionQuestionId: string;
  questionRootId: string;
  role: SmDashboardOosRole;
  category: SmDashboardOosCategory;
  detectionQuestionRootId: string | null;
  outcome: SmDashboardOosOutcome | null;
  partialCountsAsResolved: boolean;
};

export type SmDashboardMetricSummary = {
  completedVisits: number;
  submittedMarkets: number;
  classifiedChecks: number;
  foundCases: number;
  foundRate: number | null;
  fixedCases: number;
  fixedRate: number | null;
  documentedRemediations: number;
  openRemediationDocumentation: number;
  observedMarkets: number;
  marketsWithOos: number;
  affectedMarketRate: number | null;
};

export type SmDashboardCategoryRow = SmDashboardMetricSummary & {
  category: SmDashboardOosCategory;
  label: string;
};

export type SmDashboardDimensionRow = SmDashboardMetricSummary & {
  id: string;
  label: string;
};

export type SmDashboardAggregation = {
  summary: SmDashboardMetricSummary;
  categories: SmDashboardCategoryRow[];
  chains: SmDashboardDimensionRow[];
  regions: SmDashboardDimensionRow[];
};

const CATEGORY_LABELS: Record<SmDashboardOosCategory, string> = {
  action_placements: "Aktionsplatzierungen",
  softdrinks_energy: "Limonaden & Energy",
  water_near_water: "Wasser & Near Water",
  juice_iced_tea: "Säfte & Eistee",
};

const DETECTION_OUTCOMES = new Set<SmDashboardOosOutcome>(["oos_present", "oos_absent"]);
const REMEDIATION_OUTCOMES = new Set<SmDashboardOosOutcome>(["resolved", "partially_resolved", "not_resolved"]);

type DetectionCase = {
  row: SmDashboardOosRow;
  documented: boolean;
  fixed: boolean;
};

function percentage(numerator: number, denominator: number): number | null {
  return denominator > 0 ? (numerator / denominator) * 100 : null;
}

function uniqueCount(values: Iterable<string>): number {
  return new Set(values).size;
}

function dedupeVisits(rows: readonly SmDashboardVisitRow[]): SmDashboardVisitRow[] {
  return [...new Map(rows.map((row) => [row.submissionId, row])).values()];
}

function dedupeQuestionRows(rows: readonly SmDashboardOosRow[]): SmDashboardOosRow[] {
  const byQuestion = new Map<string, SmDashboardOosRow>();
  for (const row of rows) {
    const current = byQuestion.get(row.submissionQuestionId);
    if (!current || (current.outcome === null && row.outcome !== null)) byQuestion.set(row.submissionQuestionId, row);
  }
  return [...byQuestion.values()];
}

function buildDetectionCases(rows: readonly SmDashboardOosRow[]): DetectionCase[] {
  const uniqueRows = dedupeQuestionRows(rows);
  const remediationByDetection = new Map<string, SmDashboardOosRow[]>();

  for (const row of uniqueRows) {
    if (row.role !== "oos_remediation" || !row.detectionQuestionRootId) continue;
    const key = `${row.submissionId}:${row.detectionQuestionRootId}:${row.category}`;
    remediationByDetection.set(key, [...(remediationByDetection.get(key) ?? []), row]);
  }

  return uniqueRows
    .filter((row) => row.role === "oos_detection" && DETECTION_OUTCOMES.has(row.outcome as SmDashboardOosOutcome))
    .map((row) => {
      const linked = remediationByDetection.get(`${row.submissionId}:${row.questionRootId}:${row.category}`) ?? [];
      const documentedRows = linked.filter((candidate) => REMEDIATION_OUTCOMES.has(candidate.outcome as SmDashboardOosOutcome));
      const fixed = documentedRows.some((candidate) => (
        candidate.outcome === "resolved"
        || (candidate.outcome === "partially_resolved" && candidate.partialCountsAsResolved)
      ));
      return { row, documented: documentedRows.length > 0, fixed };
    });
}

function summarize(rows: readonly SmDashboardVisitRow[], cases: readonly DetectionCase[]): SmDashboardMetricSummary {
  const visits = dedupeVisits(rows);
  const classified = cases.length;
  const found = cases.filter((item) => item.row.outcome === "oos_present");
  const fixed = found.filter((item) => item.fixed);
  const documented = found.filter((item) => item.documented);
  const observedMarkets = uniqueCount(cases.map((item) => item.row.marketId));
  const marketsWithOos = uniqueCount(found.map((item) => item.row.marketId));

  return {
    completedVisits: visits.length,
    submittedMarkets: uniqueCount(visits.map((row) => row.marketId)),
    classifiedChecks: classified,
    foundCases: found.length,
    foundRate: percentage(found.length, classified),
    fixedCases: fixed.length,
    fixedRate: percentage(fixed.length, found.length),
    documentedRemediations: documented.length,
    openRemediationDocumentation: Math.max(0, found.length - documented.length),
    observedMarkets,
    marketsWithOos,
    affectedMarketRate: percentage(marketsWithOos, observedMarkets),
  };
}

function dimensionRows(
  visits: readonly SmDashboardVisitRow[],
  cases: readonly DetectionCase[],
  selector: (row: SmDashboardVisitRow) => string,
): SmDashboardDimensionRow[] {
  const groups = new Map<string, SmDashboardVisitRow[]>();
  for (const row of dedupeVisits(visits)) {
    const key = selector(row).trim() || "Nicht zugeordnet";
    groups.set(key, [...(groups.get(key) ?? []), row]);
  }

  return [...groups.entries()]
    .map(([label, groupVisits]) => ({
      id: label,
      label,
      ...summarize(groupVisits, cases.filter((item) => selector(item.row).trim() === (label === "Nicht zugeordnet" ? "" : label))),
    }))
    .sort((left, right) => right.completedVisits - left.completedVisits || left.label.localeCompare(right.label, "de-AT"));
}

export function aggregateSmDashboard(
  visitRows: readonly SmDashboardVisitRow[],
  oosRows: readonly SmDashboardOosRow[],
): SmDashboardAggregation {
  const visits = dedupeVisits(visitRows);
  const cases = buildDetectionCases(oosRows);

  return {
    summary: summarize(visits, cases),
    categories: SM_OOS_CATEGORY_ORDER.map((category) => ({
      category,
      label: CATEGORY_LABELS[category],
      ...summarize(
        visits.filter((visit) => cases.some((item) => item.row.category === category && item.row.submissionId === visit.submissionId)),
        cases.filter((item) => item.row.category === category),
      ),
    })),
    chains: dimensionRows(visits, cases, (row) => row.chain),
    regions: dimensionRows(visits, cases, (row) => row.region),
  };
}

/** One segment per completed visit, never one per answer or scheduled assignment. */
export function aggregateSmHomeVisits(
  visitRows: readonly SmDashboardVisitRow[],
  oosRows: readonly SmDashboardOosRow[],
) {
  const visits = dedupeVisits(visitRows);
  const casesByVisit = new Map<string, DetectionCase[]>();
  for (const item of buildDetectionCases(oosRows)) {
    const group = casesByVisit.get(item.row.submissionId) ?? [];
    group.push(item);
    casesByVisit.set(item.row.submissionId, group);
  }
  const result = { completed: visits.length, classified: 0, withoutOos: 0, fixedOos: 0, openOos: 0, unclassified: 0 };
  for (const visit of visits) {
    const cases = casesByVisit.get(visit.submissionId) ?? [];
    if (!cases.length) {
      result.unclassified += 1;
      continue;
    }
    result.classified += 1;
    const found = cases.filter((item) => item.row.outcome === "oos_present");
    if (!found.length) result.withoutOos += 1;
    else if (found.every((item) => item.fixed)) result.fixedOos += 1;
    else result.openOos += 1;
  }
  return result;
}
