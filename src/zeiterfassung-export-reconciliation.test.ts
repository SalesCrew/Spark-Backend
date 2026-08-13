import assert from "node:assert/strict";
import test from "node:test";

import { buildDaySessionPayload } from "./lib/admin-zeiterfassung.js";

const TIMEZONE = "Europe/Vienna";

type CapturedAction = {
  id: string;
  kind: "marktbesuch" | "zusatzzeit";
  startAt: string;
  endAt: string;
  subtype?: string;
};

type CapturedDay = {
  date: string;
  startAt: string;
  endAt: string;
  startKm: number;
  endKm: number;
  actions: CapturedAction[];
  expected: {
    arbeitstag: number;
    reineArbeitszeit: number;
    pause: number;
    anfahrt: number;
    fahrtzeit: number;
    heimfahrt: number;
    marktbesuch: number;
    zusatz: number;
  };
};

// Read-only production snapshot behind the visible Arthur Neuhold export rows.
// The 10 July fixture preserves the export-time state; that day was corrected later.
const capturedDays: CapturedDay[] = [
  {
    date: "2026-07-06",
    startAt: "2026-07-06T05:52:51.523Z",
    endAt: "2026-07-06T14:34:48.276Z",
    startKm: 43302,
    endKm: 43378,
    actions: [
      {
        id: "extra-2026-07-06",
        kind: "zusatzzeit",
        subtype: "lager",
        startAt: "2026-07-06T06:10:07.306Z",
        endAt: "2026-07-06T08:12:00.842Z",
      },
      {
        id: "visit-2026-07-06-1",
        kind: "marktbesuch",
        startAt: "2026-07-06T08:25:00.000Z",
        endAt: "2026-07-06T10:00:00.000Z",
      },
      {
        id: "visit-2026-07-06-2",
        kind: "marktbesuch",
        startAt: "2026-07-06T10:15:00.000Z",
        endAt: "2026-07-06T10:56:00.000Z",
      },
      {
        id: "visit-2026-07-06-3",
        kind: "marktbesuch",
        startAt: "2026-07-06T11:10:00.000Z",
        endAt: "2026-07-06T12:21:00.000Z",
      },
      {
        id: "visit-2026-07-06-4",
        kind: "marktbesuch",
        startAt: "2026-07-06T12:47:00.000Z",
        endAt: "2026-07-06T13:26:00.000Z",
      },
    ],
    expected: {
      arbeitstag: 521,
      reineArbeitszeit: 491,
      pause: 30,
      anfahrt: 17,
      fahrtzeit: 67,
      heimfahrt: 68,
      marktbesuch: 246,
      zusatz: 121,
    },
  },
  {
    date: "2026-07-07",
    startAt: "2026-07-07T05:49:07.572Z",
    endAt: "2026-07-07T15:37:00.602Z",
    startKm: 43378,
    endKm: 43451,
    actions: [
      {
        id: "extra-2026-07-07",
        kind: "zusatzzeit",
        subtype: "lager",
        startAt: "2026-07-07T06:07:34.546Z",
        endAt: "2026-07-07T07:58:00.987Z",
      },
      {
        id: "visit-2026-07-07-1",
        kind: "marktbesuch",
        startAt: "2026-07-07T08:16:07.950Z",
        endAt: "2026-07-07T14:34:21.188Z",
      },
    ],
    expected: {
      arbeitstag: 587,
      reineArbeitszeit: 557,
      pause: 30,
      anfahrt: 18,
      fahrtzeit: 18,
      heimfahrt: 62,
      marktbesuch: 378,
      zusatz: 110,
    },
  },
  {
    date: "2026-07-08",
    startAt: "2026-07-08T05:52:41.022Z",
    endAt: "2026-07-08T14:45:00.000Z",
    startKm: 43451,
    endKm: 43491,
    actions: [
      {
        id: "extra-2026-07-08",
        kind: "zusatzzeit",
        subtype: "lager",
        startAt: "2026-07-08T06:10:00.000Z",
        endAt: "2026-07-08T07:08:00.000Z",
      },
      {
        id: "visit-2026-07-08-1",
        kind: "marktbesuch",
        startAt: "2026-07-08T07:23:00.000Z",
        endAt: "2026-07-08T13:38:00.000Z",
      },
    ],
    expected: {
      arbeitstag: 532,
      reineArbeitszeit: 502,
      pause: 30,
      anfahrt: 17,
      fahrtzeit: 15,
      heimfahrt: 67,
      marktbesuch: 375,
      zusatz: 58,
    },
  },
  {
    date: "2026-07-09",
    startAt: "2026-07-09T05:30:50.441Z",
    endAt: "2026-07-09T14:29:56.516Z",
    startKm: 43491,
    endKm: 43502,
    actions: [
      {
        id: "extra-2026-07-09-1",
        kind: "zusatzzeit",
        subtype: "lager",
        startAt: "2026-07-09T05:51:47.359Z",
        endAt: "2026-07-09T07:01:39.702Z",
      },
      {
        id: "visit-2026-07-09-1",
        kind: "marktbesuch",
        startAt: "2026-07-09T07:25:00.000Z",
        endAt: "2026-07-09T08:15:00.000Z",
      },
      {
        id: "extra-2026-07-09-2",
        kind: "zusatzzeit",
        subtype: "lager",
        startAt: "2026-07-09T08:30:00.000Z",
        endAt: "2026-07-09T08:50:00.000Z",
      },
      {
        id: "extra-2026-07-09-3",
        kind: "zusatzzeit",
        subtype: "werkstatt",
        startAt: "2026-07-09T09:03:00.000Z",
        endAt: "2026-07-09T09:35:00.000Z",
      },
      {
        id: "extra-2026-07-09-4",
        kind: "zusatzzeit",
        subtype: "lager",
        startAt: "2026-07-09T09:50:00.000Z",
        endAt: "2026-07-09T10:00:00.000Z",
      },
      {
        id: "visit-2026-07-09-2",
        kind: "marktbesuch",
        startAt: "2026-07-09T10:10:00.000Z",
        endAt: "2026-07-09T13:29:00.000Z",
      },
    ],
    expected: {
      arbeitstag: 539,
      reineArbeitszeit: 509,
      pause: 30,
      anfahrt: 20,
      fahrtzeit: 76,
      heimfahrt: 60,
      marktbesuch: 249,
      zusatz: 131,
    },
  },
  {
    date: "2026-07-10",
    startAt: "2026-07-10T05:45:37.575Z",
    endAt: "2026-07-10T12:45:32.053Z",
    startKm: 91,
    endKm: 157,
    actions: [
      {
        id: "extra-2026-07-10",
        kind: "zusatzzeit",
        subtype: "lager",
        startAt: "2026-07-10T06:05:00.000Z",
        endAt: "2026-07-10T08:10:00.000Z",
      },
      {
        id: "visit-2026-07-10-1",
        kind: "marktbesuch",
        startAt: "2026-07-10T09:25:00.000Z",
        endAt: "2026-07-10T10:35:00.000Z",
      },
    ],
    expected: {
      arbeitstag: 419,
      reineArbeitszeit: 389,
      pause: 30,
      anfahrt: 19,
      fahrtzeit: 75,
      heimfahrt: 130,
      marktbesuch: 70,
      zusatz: 125,
    },
  },
];

function buildCapturedDay(day: CapturedDay) {
  return buildDaySessionPayload({
    sessionId: `session-${day.date}`,
    date: day.date,
    gmId: "arthur-read-only-snapshot",
    gmName: "Arthur Neuhold",
    region: "Ost",
    status: "submitted",
    startAt: new Date(day.startAt),
    endAt: new Date(day.endAt),
    startKm: day.startKm,
    endKm: day.endKm,
    actions: day.actions.map((action) => ({
      id: action.id,
      kind: action.kind,
      startAt: new Date(action.startAt),
      endAt: new Date(action.endAt),
      ...(action.subtype ? { subtype: action.subtype } : {}),
    })),
    timezone: TIMEZONE,
  });
}

function sumTimeline(day: ReturnType<typeof buildCapturedDay>, kind: string): number {
  return day.timeline
    .filter((segment) => segment.kind === kind)
    .reduce((sum, segment) => sum + segment.durationMin, 0);
}

test("Arthur export snapshot reconciles with the canonical Zeiterfassung calculation", () => {
  for (const captured of capturedDays) {
    const day = buildCapturedDay(captured);
    const actual = {
      arbeitstag: day.stats.arbeitstag,
      reineArbeitszeit: day.stats.reineArbeitszeit,
      pause: day.stats.pauseMin,
      anfahrt: sumTimeline(day, "anfahrt"),
      fahrtzeit: sumTimeline(day, "fahrtzeit"),
      heimfahrt: sumTimeline(day, "heimfahrt"),
      marktbesuch: sumTimeline(day, "marktbesuch"),
      zusatz: sumTimeline(day, "zusatzzeit"),
    };

    assert.deepEqual(actual, captured.expected, captured.date);
    assert.equal(
      actual.reineArbeitszeit,
      actual.arbeitstag - actual.pause,
      `${captured.date}: Reine AZ must already be the final net work time`,
    );
  }
});

test("yellow activity columns are a gross timeline breakdown, not values to add to Reine AZ", () => {
  const totals = capturedDays.map((captured) => {
    const day = buildCapturedDay(captured);
    const breakdown = ["anfahrt", "fahrtzeit", "heimfahrt", "marktbesuch", "zusatzzeit"]
      .map((kind) => sumTimeline(day, kind))
      .reduce((sum, value) => sum + value, 0);
    return {
      date: captured.date,
      arbeitstag: day.stats.arbeitstag,
      reineArbeitszeit: day.stats.reineArbeitszeit,
      pause: day.stats.pauseMin,
      breakdown,
    };
  });

  assert.equal(totals.reduce((sum, day) => sum + day.arbeitstag, 0), 2598);
  assert.equal(totals.reduce((sum, day) => sum + day.pause, 0), 150);
  assert.equal(totals.reduce((sum, day) => sum + day.reineArbeitszeit, 0), 2448);
  assert.equal(totals.reduce((sum, day) => sum + day.breakdown, 0), 2592);

  // Each interval is floored separately, so the breakdown may be a few minutes
  // below a workday that is floored only once from start to end.
  assert.deepEqual(
    totals.map((day) => day.arbeitstag - day.breakdown),
    [2, 1, 0, 3, 0],
  );
});
