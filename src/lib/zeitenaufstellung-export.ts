export type ZeitenaufstellungMarket = {
  name: string;
  dbName: string;
  address: string;
  postalCode: string;
  city: string;
  region: string;
  cokeMasterNumber: string | null;
  standardMarketNumber: string | null;
  flexNumber: string | null;
  kuehlerStammnr: string | null;
  universeMarket: boolean;
};

export type ZeitenaufstellungDay = {
  id: string;
  gmId: string;
  workDate: string;
  startedAt: Date;
  endedAt: Date | null;
};

export type ZeitenaufstellungVisit = {
  id: string;
  gmId: string;
  workDate: string;
  startedAt: Date;
  submittedAt: Date;
  person: string;
  market: ZeitenaufstellungMarket;
};

export type ZeitenaufstellungVisitSection = {
  id: string;
  visitSessionId: string;
  section: "standard" | "flex" | "billa" | "kuehler" | "mhd";
  orderIndex: number;
  fragebogenName: string;
  campaignName: string;
  createdAt: Date;
  updatedAt: Date;
};

export type ZeitenaufstellungExtra = {
  id: string;
  gmId: string;
  workDate: string;
  startedAt: Date;
  endedAt: Date;
  activityType: string;
  comment: string | null;
  person: string;
};

export type ZeitenaufstellungPause = {
  id: string;
  gmId: string;
  workDate: string;
  startedAt: Date;
  endedAt: Date;
};

export type ZeitenaufstellungSectionMetrics = {
  photoCount: number;
  fillDurationMin: number;
  comments: string[];
};

export type ZeitenaufstellungExportRow = {
  targetObject: string;
  customerNumber: string;
  visitDate: string;
  visitStartTime: string;
  person: string;
  imageCount: number;
  travelDurationMin: number | null;
  visitDurationMin: number | null;
  calculatedFillDurationMin: number;
  comment: string;
  notEvaluable: boolean;
  reason: string;
  questionnaire: string;
};

type TimedAction = {
  id: string;
  gmId: string;
  workDate: string;
  startedAt: Date;
  endedAt: Date;
  kind: "visit" | "extra" | "pause";
  activityType?: string;
};

type SortableRow = ZeitenaufstellungExportRow & {
  categoryRank: number;
  sortStartedAt: number;
  sortOrder: number;
};

const SECTION_RANK: Record<ZeitenaufstellungVisitSection["section"], number> = {
  standard: 0,
  flex: 2,
  billa: 3,
  mhd: 4,
  kuehler: 5,
};

const EXTRA_LABELS: Record<string, string> = {
  homeoffice: "Home-Office",
  lager: "Lager",
  werkstatt: "Auto-Service",
  sonderaufgabe: "Sondereinsatz",
  schulung: "Einschulung durch GL",
  arztbesuch: "Arztbesuch",
  hoteluebernachtung: "Hotelübernachtung",
  heimfahrt: "Heimfahrt",
};

function clean(value: string | null | undefined): string {
  return value?.trim() ?? "";
}

function minutesBetween(startAt: Date, endAt: Date): number {
  const diff = endAt.getTime() - startAt.getTime();
  return diff > 0 ? Math.max(1, Math.floor(diff / 60_000)) : 0;
}

function toHm(date: Date, timezone: string): string {
  return new Intl.DateTimeFormat("de-AT", {
    timeZone: timezone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}

function chainCode(value: string): string {
  const normalized = value.toLocaleLowerCase("de");
  if (normalized.includes("interspar")) return "ISP";
  if (normalized.includes("eurospar")) return "ESP";
  if (normalized.includes("billa plus") || normalized.includes("billa+")) return "Billa+";
  if (normalized.includes("billa")) return "Billa";
  if (normalized.includes("spar")) return "Spar";
  if (normalized.includes("maximarkt")) return "Maxi";
  if (normalized.includes("penny")) return "Penny";
  if (normalized.includes("lidl")) return "Lidl";
  if (normalized.includes("hofer")) return "Hofer";
  return value || "Markt";
}

function regionCode(region: string): string {
  const normalized = region.toLocaleLowerCase("de");
  if (normalized.startsWith("n")) return "N";
  if (normalized.startsWith("o") || normalized.includes("ost")) return "O";
  if (normalized.startsWith("s") || normalized.includes("süd") || normalized.includes("sued")) return "S";
  if (normalized.startsWith("w")) return "W";
  return region.slice(0, 1).toUpperCase();
}

export function formatZeitenaufstellungTargetObject(market: ZeitenaufstellungMarket): string {
  const marketLabel = clean(market.dbName) || clean(market.name);
  const identity = [marketLabel, clean(market.name) !== marketLabel ? clean(market.name) : ""]
    .filter(Boolean)
    .join(", ");
  const address = [clean(market.address), clean(market.postalCode), clean(market.city)].filter(Boolean).join(", ");
  return [
    `${chainCode(marketLabel)}; ${regionCode(market.region)}`,
    [identity, address].filter(Boolean).join(", "),
    `Universums-Markt, ${market.universeMarket ? "Ja" : "Nein"}`,
    market.region ? `Re. ${market.region}` : "",
  ].filter(Boolean).join("; ");
}

export function resolveZeitenaufstellungCustomerNumber(market: ZeitenaufstellungMarket): string {
  return [market.cokeMasterNumber, market.standardMarketNumber, market.flexNumber, market.kuehlerStammnr]
    .map(clean)
    .find(Boolean) ?? "";
}

function buildTravelDurations(input: {
  days: ZeitenaufstellungDay[];
  visits: ZeitenaufstellungVisit[];
  extras: ZeitenaufstellungExtra[];
  pauses: ZeitenaufstellungPause[];
}): {
  byActionId: Map<string, number>;
  homeByDayKey: Map<string, { startedAt: Date; durationMin: number }>;
} {
  const actionsByDayKey = new Map<string, TimedAction[]>();
  const addAction = (action: TimedAction) => {
    const key = `${action.gmId}__${action.workDate}`;
    const bucket = actionsByDayKey.get(key) ?? [];
    bucket.push(action);
    actionsByDayKey.set(key, bucket);
  };
  for (const visit of input.visits) {
    addAction({
      id: visit.id,
      gmId: visit.gmId,
      workDate: visit.workDate,
      startedAt: visit.startedAt,
      endedAt: visit.submittedAt,
      kind: "visit",
    });
  }
  for (const extra of input.extras) {
    addAction({
      id: extra.id,
      gmId: extra.gmId,
      workDate: extra.workDate,
      startedAt: extra.startedAt,
      endedAt: extra.endedAt,
      kind: "extra",
      activityType: extra.activityType,
    });
  }
  for (const pause of input.pauses) {
    addAction({
      id: pause.id,
      gmId: pause.gmId,
      workDate: pause.workDate,
      startedAt: pause.startedAt,
      endedAt: pause.endedAt,
      kind: "pause",
    });
  }

  const byActionId = new Map<string, number>();
  const homeByDayKey = new Map<string, { startedAt: Date; durationMin: number }>();
  for (const day of input.days) {
    const dayKey = `${day.gmId}__${day.workDate}`;
    const actions = (actionsByDayKey.get(dayKey) ?? [])
      .filter((action) => action.endedAt.getTime() > action.startedAt.getTime())
      .sort((left, right) => left.startedAt.getTime() - right.startedAt.getTime() || left.id.localeCompare(right.id));
    let previousEnd = day.startedAt;
    for (const action of actions) {
      const gap = minutesBetween(previousEnd, action.startedAt);
      if (action.kind !== "pause" && action.activityType !== "heimfahrt" && gap > 0) {
        byActionId.set(action.id, gap);
      }
      if (action.endedAt.getTime() > previousEnd.getTime()) previousEnd = action.endedAt;
    }
    const lastAction = actions.at(-1);
    if (
      actions.length > 0 &&
      day.endedAt &&
      day.endedAt.getTime() > previousEnd.getTime() &&
      !(lastAction?.kind === "extra" && lastAction.activityType === "heimfahrt")
    ) {
      homeByDayKey.set(dayKey, {
        startedAt: previousEnd,
        durationMin: minutesBetween(previousEnd, day.endedAt),
      });
    }
  }
  return { byActionId, homeByDayKey };
}

export function buildZeitenaufstellungRows(input: {
  timezone: string;
  days: ZeitenaufstellungDay[];
  visits: ZeitenaufstellungVisit[];
  sections: ZeitenaufstellungVisitSection[];
  extras: ZeitenaufstellungExtra[];
  pauses: ZeitenaufstellungPause[];
  sectionMetrics: Map<string, ZeitenaufstellungSectionMetrics>;
  personByGmId: Map<string, string>;
}): ZeitenaufstellungExportRow[] {
  const travel = buildTravelDurations(input);
  const sectionsByVisitId = new Map<string, ZeitenaufstellungVisitSection[]>();
  for (const section of input.sections) {
    const bucket = sectionsByVisitId.get(section.visitSessionId) ?? [];
    bucket.push(section);
    sectionsByVisitId.set(section.visitSessionId, bucket);
  }

  const rows: SortableRow[] = [];
  for (const visit of input.visits) {
    const visitSections = (sectionsByVisitId.get(visit.id) ?? [])
      .slice()
      .sort((left, right) => left.orderIndex - right.orderIndex || left.id.localeCompare(right.id));
    visitSections.forEach((section, sectionIndex) => {
      const metrics = input.sectionMetrics.get(section.id) ?? { photoCount: 0, fillDurationMin: 0, comments: [] };
      const sectionStart = section.createdAt.getTime() > visit.startedAt.getTime() ? section.createdAt : visit.startedAt;
      const sectionEnd = section.updatedAt.getTime() < visit.submittedAt.getTime() ? section.updatedAt : visit.submittedAt;
      const sectionDuration = minutesBetween(sectionStart, sectionEnd) || minutesBetween(visit.startedAt, visit.submittedAt);
      rows.push({
        targetObject: formatZeitenaufstellungTargetObject(visit.market),
        customerNumber: resolveZeitenaufstellungCustomerNumber(visit.market),
        visitDate: visit.workDate,
        visitStartTime: toHm(visit.startedAt, input.timezone),
        person: visit.person,
        imageCount: Math.max(0, metrics.photoCount),
        travelDurationMin: sectionIndex === 0 ? travel.byActionId.get(visit.id) ?? null : null,
        visitDurationMin: sectionDuration,
        calculatedFillDurationMin: Math.max(0, metrics.fillDurationMin),
        comment: Array.from(new Set(metrics.comments.map(clean).filter(Boolean))).join("\n"),
        notEvaluable: false,
        reason: "",
        questionnaire: clean(section.fragebogenName) || clean(section.campaignName),
        categoryRank: SECTION_RANK[section.section],
        sortStartedAt: visit.startedAt.getTime(),
        sortOrder: section.orderIndex,
      });
    });
  }

  for (const extra of input.extras) {
    const isHeimfahrt = extra.activityType === "heimfahrt";
    const ownDuration = minutesBetween(extra.startedAt, extra.endedAt);
    rows.push({
      targetObject: EXTRA_LABELS[extra.activityType] ?? extra.activityType,
      customerNumber: "",
      visitDate: extra.workDate,
      visitStartTime: toHm(extra.startedAt, input.timezone),
      person: extra.person,
      imageCount: 0,
      travelDurationMin: isHeimfahrt ? ownDuration : travel.byActionId.get(extra.id) ?? null,
      visitDurationMin: isHeimfahrt ? null : ownDuration,
      calculatedFillDurationMin: 0,
      comment: clean(extra.comment),
      notEvaluable: false,
      reason: "",
      questionnaire: `${extra.workDate.slice(0, 4)}_Coca-Cola Zeit_neu`,
      categoryRank: 1,
      sortStartedAt: extra.startedAt.getTime(),
      sortOrder: 0,
    });
  }

  for (const day of input.days) {
    const dayKey = `${day.gmId}__${day.workDate}`;
    const home = travel.homeByDayKey.get(dayKey);
    if (!home || home.durationMin <= 0) continue;
    rows.push({
      targetObject: "Heimfahrt",
      customerNumber: "",
      visitDate: day.workDate,
      visitStartTime: toHm(home.startedAt, input.timezone),
      person: input.personByGmId.get(day.gmId) ?? "",
      imageCount: 0,
      travelDurationMin: home.durationMin,
      visitDurationMin: null,
      calculatedFillDurationMin: 0,
      comment: "",
      notEvaluable: false,
      reason: "",
      questionnaire: `${day.workDate.slice(0, 4)}_Coca-Cola Zeit_neu`,
      categoryRank: 1,
      sortStartedAt: home.startedAt.getTime(),
      sortOrder: 1,
    });
  }

  return rows
    .sort((left, right) =>
      left.categoryRank - right.categoryRank ||
      left.sortStartedAt - right.sortStartedAt ||
      left.sortOrder - right.sortOrder ||
      left.person.localeCompare(right.person, "de"),
    )
    .map(({ categoryRank: _categoryRank, sortStartedAt: _sortStartedAt, sortOrder: _sortOrder, ...row }) => row);
}
