const QUESTIONNAIRE_SECTION_ORDER = ["standard", "flex", "billa", "kuehler", "mhd"] as const;
const EXTRA_LABELS: Record<string, string> = {
  schulung: "Schulung",
  sonderaufgabe: "Sonderaufgabe",
  arztbesuch: "Arztbesuch",
  werkstatt: "Werkstatt/Autoreinigung",
  homeoffice: "Homeoffice",
  lager: "Lager",
  heimfahrt: "Heimfahrt",
  hotel: "Hoteluebernachtung",
  hoteluebernachtung: "Hoteluebernachtung",
};

type SessionStatus = "started" | "ended" | "submitted";
type TimelineKind = "anfahrt" | "fahrtzeit" | "marktbesuch" | "pause" | "zusatzzeit" | "heimfahrt";

type BaseAction = {
  id: string;
  kind: "marktbesuch" | "pause" | "zusatzzeit";
  startAt: Date;
  endAt: Date;
  marketName?: string;
  marketAddress?: string;
  subtype?: string;
  questionnaireType?: string;
  questionnaireTypes?: string[];
};

type BuildSessionInput = {
  sessionId: string;
  date: string;
  gmId: string;
  gmName: string;
  region: string;
  status: SessionStatus;
  startAt: Date;
  endAt: Date;
  startKm: number | null;
  endKm: number | null;
  actions: BaseAction[];
  timezone: string;
};

type SessionEntry = {
  id: string;
  kind: "marktbesuch" | "pause" | "zusatzzeit";
  startTime: string;
  endTime: string;
  durationMin: number;
  marketName?: string;
  marketAddress?: string;
  subtype?: string;
  questionnaireType?: string;
  questionnaireTypes?: string[];
};

type TimelineSegment = {
  kind: TimelineKind;
  start: string;
  end: string;
  durationMin: number;
  title: string;
  subtitle?: string;
  subtype?: string;
  questionnaireType?: string;
};

type SessionStats = {
  arbeitstag: number;
  pauseMin: number;
  reineArbeitszeit: number;
  kmGefahren: number | null;
  marktbesuche: number;
  zusatz: number;
};

type DaySessionPayload = {
  id: string;
  date: string;
  gmId: string;
  gmName: string;
  region: string;
  status: SessionStatus;
  isLive: boolean;
  timezone: string;
  startTime: string;
  endTime: string;
  startKm: number | null;
  endKm: number | null;
  entries: SessionEntry[];
  timeline: TimelineSegment[];
  stats: SessionStats;
  hasCompleteKm: boolean;
};

type AggregatePayload = {
  gmId: string;
  gmName: string;
  region: string;
  currentKwNumber: number;
  currentKwReineArbeitszeitMin: number;
  totalReineArbeitszeitMin: number;
  averageWorkdayMin: number;
  totalKmDriven: number;
  privatnutzungKm: number;
  trackedDays: number;
  liveSessionCount: number;
  visibleSessionCount: number;
};

function toHm(date: Date, timezone: string): string {
  return new Intl.DateTimeFormat("de-AT", {
    timeZone: timezone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(date);
}

function diffMinutes(startAt: Date, endAt: Date): number {
  const diff = endAt.getTime() - startAt.getTime();
  return diff > 0 ? Math.floor(diff / 60_000) : 0;
}

function diffMinutesAtLeastOne(startAt: Date, endAt: Date): number {
  const diff = endAt.getTime() - startAt.getTime();
  if (diff <= 0) return 0;
  return Math.max(1, Math.floor(diff / 60_000));
}

function toYmdInTimezone(date: Date, timezone: string): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
}

function resolvePrimaryQuestionnaireType(types: string[]): string | undefined {
  if (types.length === 0) return undefined;
  const normalized = Array.from(
    new Set(types.map((value) => String(value).trim().toLowerCase()).filter((value) => value.length > 0)),
  );
  for (const key of QUESTIONNAIRE_SECTION_ORDER) {
    if (normalized.includes(key)) return key;
  }
  return normalized[0];
}

function clampAction(action: BaseAction, dayStartAt: Date, dayEndAt: Date): BaseAction | null {
  const startAt = action.startAt.getTime() < dayStartAt.getTime() ? dayStartAt : action.startAt;
  const endAt = action.endAt.getTime() > dayEndAt.getTime() ? dayEndAt : action.endAt;
  if (endAt.getTime() <= startAt.getTime()) return null;
  return { ...action, startAt, endAt };
}

function compareActionsByTime(a: BaseAction, b: BaseAction): number {
  const startDiff = a.startAt.getTime() - b.startAt.getTime();
  if (startDiff !== 0) return startDiff;
  const endDiff = a.endAt.getTime() - b.endAt.getTime();
  if (endDiff !== 0) return endDiff;
  return a.id.localeCompare(b.id);
}

function subtractPauseWindowsFromInterval(
  startAt: Date,
  endAt: Date,
  pauseWindows: Array<{ startAt: Date; endAt: Date }>,
): Array<{ startAt: Date; endAt: Date }> {
  let remaining = [{ startAt, endAt }];
  for (const pauseWindow of pauseWindows) {
    if (remaining.length === 0) break;
    const nextRemaining: Array<{ startAt: Date; endAt: Date }> = [];
    for (const segment of remaining) {
      const overlapStartMs = Math.max(segment.startAt.getTime(), pauseWindow.startAt.getTime());
      const overlapEndMs = Math.min(segment.endAt.getTime(), pauseWindow.endAt.getTime());
      if (overlapEndMs <= overlapStartMs) {
        nextRemaining.push(segment);
        continue;
      }
      if (segment.startAt.getTime() < overlapStartMs) {
        nextRemaining.push({
          startAt: segment.startAt,
          endAt: new Date(overlapStartMs),
        });
      }
      if (overlapEndMs < segment.endAt.getTime()) {
        nextRemaining.push({
          startAt: new Date(overlapEndMs),
          endAt: segment.endAt,
        });
      }
    }
    remaining = nextRemaining;
  }
  return remaining.filter((segment) => segment.endAt.getTime() > segment.startAt.getTime());
}

function splitZusatzIntervalsByPauseOverlaps(actions: BaseAction[]): BaseAction[] {
  const sorted = [...actions].sort(compareActionsByTime);
  const pauseWindows = sorted
    .filter((action): action is BaseAction & { kind: "pause" } => action.kind === "pause")
    .map((pause) => ({ startAt: pause.startAt, endAt: pause.endAt }));

  const splitActions: BaseAction[] = [];
  for (const action of sorted) {
    if (action.kind !== "zusatzzeit") {
      splitActions.push(action);
      continue;
    }
    const segments = subtractPauseWindowsFromInterval(action.startAt, action.endAt, pauseWindows);
    if (segments.length === 0) continue;
    if (
      segments.length === 1 &&
      segments[0]?.startAt.getTime() === action.startAt.getTime() &&
      segments[0]?.endAt.getTime() === action.endAt.getTime()
    ) {
      splitActions.push(action);
      continue;
    }
    segments.forEach((segment, index) => {
      splitActions.push({
        ...action,
        id: `${action.id}::seg:${index + 1}`,
        startAt: segment.startAt,
        endAt: segment.endAt,
      });
    });
  }

  return splitActions.sort(compareActionsByTime);
}

function formatExtraLabel(subtype: string | undefined): string {
  if (!subtype) return "Zusatzzeit";
  return EXTRA_LABELS[subtype] ?? subtype;
}

function getIsoWeekFromYmd(ymd: string): number {
  const parts = ymd.split("-");
  const yearRaw = Number(parts[0] ?? "0");
  const monthRaw = Number(parts[1] ?? "1");
  const dayRaw = Number(parts[2] ?? "1");
  if (!Number.isFinite(yearRaw) || !Number.isFinite(monthRaw) || !Number.isFinite(dayRaw)) return 1;
  const target = new Date(Date.UTC(yearRaw, monthRaw - 1, dayRaw));
  const dayNr = (target.getUTCDay() + 6) % 7;
  target.setUTCDate(target.getUTCDate() - dayNr + 3);
  const firstThursday = new Date(Date.UTC(target.getUTCFullYear(), 0, 4));
  const firstDayNr = (firstThursday.getUTCDay() + 6) % 7;
  firstThursday.setUTCDate(firstThursday.getUTCDate() - firstDayNr + 3);
  const diffMs = target.getTime() - firstThursday.getTime();
  return 1 + Math.round(diffMs / (7 * 24 * 60 * 60 * 1000));
}

function buildDaySessionPayload(input: BuildSessionInput): DaySessionPayload {
  const sortedActions = [...input.actions]
    .sort(compareActionsByTime)
    .map((action) => clampAction(action, input.startAt, input.endAt))
    .filter((action): action is BaseAction => Boolean(action));
  const canonicalActions = splitZusatzIntervalsByPauseOverlaps(sortedActions);

  const entries: SessionEntry[] = canonicalActions.map((action) => ({
    id: action.id,
    kind: action.kind,
    startTime: toHm(action.startAt, input.timezone),
    endTime: toHm(action.endAt, input.timezone),
    durationMin: diffMinutes(action.startAt, action.endAt),
    ...(action.marketName ? { marketName: action.marketName } : {}),
    ...(action.marketAddress ? { marketAddress: action.marketAddress } : {}),
    ...(action.subtype ? { subtype: action.subtype } : {}),
    ...(action.questionnaireType ? { questionnaireType: action.questionnaireType } : {}),
    ...(action.questionnaireTypes && action.questionnaireTypes.length > 0
      ? { questionnaireTypes: action.questionnaireTypes }
      : {}),
  }));

  const timeline: TimelineSegment[] = [];
  const dayStartLabel = toHm(input.startAt, input.timezone);
  const dayEndLabel = toHm(input.endAt, input.timezone);

  if (canonicalActions.length > 0) {
    const first = canonicalActions[0];
    if (first) {
      const anfahrtDuration = diffMinutesAtLeastOne(input.startAt, first.startAt);
      if (anfahrtDuration > 0) {
        timeline.push({
          kind: "anfahrt",
          start: dayStartLabel,
          end: toHm(first.startAt, input.timezone),
          durationMin: anfahrtDuration,
          title: "Anfahrt",
        });
      }
    }
  }

  for (let index = 0; index < canonicalActions.length; index += 1) {
    const current = canonicalActions[index];
    if (!current) continue;
    if (index > 0) {
      const prev = canonicalActions[index - 1];
      if (!prev) continue;
      const gap = diffMinutesAtLeastOne(prev.endAt, current.startAt);
      if (gap > 0) {
        timeline.push({
          kind: "fahrtzeit",
          start: toHm(prev.endAt, input.timezone),
          end: toHm(current.startAt, input.timezone),
          durationMin: gap,
          title: "Fahrtzeit",
        });
      }
    }

    const timelineSegment: TimelineSegment = {
      kind: current.kind,
      start: toHm(current.startAt, input.timezone),
      end: toHm(current.endAt, input.timezone),
      durationMin: diffMinutes(current.startAt, current.endAt),
      title:
        current.kind === "marktbesuch"
          ? current.marketName?.trim() || "Marktbesuch"
          : current.kind === "pause"
            ? "Pause"
            : formatExtraLabel(current.subtype),
      ...(current.kind === "marktbesuch" && current.marketAddress
        ? { subtitle: current.marketAddress }
        : {}),
      ...(current.kind === "zusatzzeit" && current.subtype
        ? { subtype: current.subtype }
        : {}),
      ...(current.kind === "marktbesuch" && current.questionnaireType
        ? { questionnaireType: current.questionnaireType }
        : {}),
    };
    timeline.push(timelineSegment);
  }

  if (canonicalActions.length > 0 && input.status !== "started") {
    const last = canonicalActions[canonicalActions.length - 1];
    if (!last) {
      // no-op
    } else {
    const heimfahrtDuration = diffMinutesAtLeastOne(last.endAt, input.endAt);
    if (heimfahrtDuration > 0) {
      timeline.push({
        kind: "heimfahrt",
        start: toHm(last.endAt, input.timezone),
        end: dayEndLabel,
        durationMin: heimfahrtDuration,
        title: "Heimfahrt",
      });
    }
    }
  }

  const arbeitstag = diffMinutes(input.startAt, input.endAt);
  const pauseMin = entries.filter((entry) => entry.kind === "pause").reduce((sum, entry) => sum + entry.durationMin, 0);
  const reineArbeitszeit = Math.max(0, arbeitstag - pauseMin);
  const kmGefahren =
    input.startKm != null && input.endKm != null && input.endKm >= input.startKm
      ? input.endKm - input.startKm
      : null;

  return {
    id: input.sessionId,
    date: input.date,
    gmId: input.gmId,
    gmName: input.gmName,
    region: input.region,
    status: input.status,
    isLive: input.status !== "submitted",
    timezone: input.timezone,
    startTime: dayStartLabel,
    endTime: dayEndLabel,
    startKm: input.startKm,
    endKm: input.endKm,
    entries,
    timeline,
    stats: {
      arbeitstag,
      pauseMin,
      reineArbeitszeit,
      kmGefahren,
      marktbesuche: entries.filter((entry) => entry.kind === "marktbesuch").length,
      zusatz: entries.filter((entry) => entry.kind === "zusatzzeit").length,
    },
    hasCompleteKm: input.startKm != null && input.endKm != null,
  };
}

function buildGmAggregates(input: { sessions: DaySessionPayload[]; timezone: string; now?: Date }): AggregatePayload[] {
  const now = input.now ?? new Date();
  const currentWeek = getIsoWeekFromYmd(toYmdInTimezone(now, input.timezone));
  const gmMap = new Map<string, DaySessionPayload[]>();
  for (const session of input.sessions) {
    const bucket = gmMap.get(session.gmId) ?? [];
    bucket.push(session);
    gmMap.set(session.gmId, bucket);
  }

  const result: AggregatePayload[] = [];
  for (const [gmId, sessions] of gmMap.entries()) {
    const sorted = [...sessions].sort((a, b) => a.date.localeCompare(b.date));
    const strict = sorted.filter((session) => session.status === "submitted" && session.hasCompleteKm);
    const currentKwReineArbeitszeitMin = strict
      .filter((session) => getIsoWeekFromYmd(session.date) === currentWeek)
      .reduce((sum, session) => sum + session.stats.reineArbeitszeit, 0);
    const totalReineArbeitszeitMin = strict.reduce((sum, session) => sum + session.stats.reineArbeitszeit, 0);
    const averageWorkdayMin = strict.length > 0 ? Math.round(totalReineArbeitszeitMin / strict.length) : 0;
    const totalKmDriven = strict.reduce((sum, session) => sum + (session.stats.kmGefahren ?? 0), 0);

    let privatnutzungKm = 0;
    for (let index = 0; index < strict.length - 1; index += 1) {
      const current = strict[index];
      const next = strict[index + 1];
      if (!current || !next) continue;
      const currentEndKm = current.endKm;
      const nextStartKm = next.startKm;
      if (currentEndKm == null || nextStartKm == null) continue;
      const gap = nextStartKm - currentEndKm;
      if (gap > 0) privatnutzungKm += gap;
    }

    const base = sorted[0];
    result.push({
      gmId,
      gmName: base?.gmName ?? "",
      region: base?.region ?? "",
      currentKwNumber: currentWeek,
      currentKwReineArbeitszeitMin,
      totalReineArbeitszeitMin,
      averageWorkdayMin,
      totalKmDriven,
      privatnutzungKm,
      trackedDays: strict.length,
      liveSessionCount: sorted.filter((session) => session.isLive).length,
      visibleSessionCount: sorted.length,
    });
  }

  return result.sort((a, b) => a.gmName.localeCompare(b.gmName));
}

export {
  buildDaySessionPayload,
  buildGmAggregates,
  formatExtraLabel,
  resolvePrimaryQuestionnaireType,
  splitZusatzIntervalsByPauseOverlaps,
  toYmdInTimezone,
  type AggregatePayload,
  type BaseAction,
  type DaySessionPayload,
};
