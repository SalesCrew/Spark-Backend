// Shared read-only dashboard contract. No production credentials or fixtures here.
export type DashboardChainGroup = "rewe" | "spar" | "other";
export type DashboardScope = {
  region: string | null;
  gmId: string | null;
  chain: string | null;
  chains?: string[] | undefined;
  chainGroups?: DashboardChainGroup[] | undefined;
  marketId: string | null;
  marketIds?: string[] | undefined;
  stc: "gold" | "silver" | "bronze" | null;
};
export type DashboardInterval = {
  id: string;
  label: string;
  shortLabel: string;
  start: string;
  end: string;
};
export const availabilityTypes = [
  "Cooler",
  "SingleServe",
  "MultiServe",
  "Promos",
  "Warehouse",
] as const;
export type AvailabilityType = (typeof availabilityTypes)[number];
export type AvailabilityCounts = {
  top: number;
  mediocre: number;
  bad: number;
  total: number;
  average: number | null;
};
export type DashboardPoint = DashboardInterval & {
  ipp: number | null;
  ippMarketCount: number;
  ippSource: "market_answers" | "effective_red";
  ippPlacement: number | null;
  placements: number | null;
  competitor: number | null;
  availability: Record<AvailabilityType, AvailabilityCounts>;
  availabilityAnswered: number;
  availabilityExpected: number;
  visits: number;
  redSurveys: number;
  averageMinutes: number | null;
  standardOnly: number;
  flexOnly: number;
  mixed: number;
  other: number;
};
export type DashboardData = {
  points: DashboardPoint[];
  scope: DashboardScope;
  calculatedAt: string;
  timezone: "Europe/Vienna";
  stcApplied: false;
};
export type DashboardFacets = {
  firstEntryDate: string | null;
  markets: {
    id: string;
    label: string;
    region: string;
    gmName: string;
    chain: string;
    searchText: string;
  }[];
  gms: { id: string; label: string; region: string }[];
};
export type DashboardExport = {
  title: string;
  data: DashboardData;
  selectedIntervalId: string | null;
  highlightedType?: AvailabilityType | null;
  comparisonPreset?: string;
  comparisonIntervalId?: string | null;
};
