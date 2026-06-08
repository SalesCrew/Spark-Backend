import assert from "node:assert/strict";
import test from "node:test";
import {
  TIMELINE_OVERLAP_ERROR_MESSAGE,
  buildDaySessionPayload,
  validateTimelineInterval,
  type BaseAction,
  type TimelineInterval,
} from "./lib/admin-zeiterfassung.js";

function at(hm: string): Date {
  return new Date(`2026-06-08T${hm}:00.000Z`);
}

function buildSession(input: {
  start?: string;
  end?: string;
  status?: "started" | "ended" | "submitted";
  actions?: BaseAction[];
}) {
  return buildDaySessionPayload({
    sessionId: "day-1",
    date: "2026-06-08",
    gmId: "gm-1",
    gmName: "GM Test",
    region: "West",
    status: input.status ?? "submitted",
    startAt: at(input.start ?? "10:00"),
    endAt: at(input.end ?? "14:00"),
    startKm: 100,
    endKm: 140,
    actions: input.actions ?? [],
    timezone: "UTC",
  });
}

test("admin zeiterfassung timeline renders wrapper, actions, gaps, and heimfahrt in order", () => {
  const session = buildSession({
    end: "14:15",
    actions: [
      { id: "market-1", kind: "marktbesuch", startAt: at("10:10"), endAt: at("10:50"), marketName: "Markt 1" },
      { id: "extra-1", kind: "zusatzzeit", startAt: at("12:10"), endAt: at("13:00"), subtype: "schulung" },
      { id: "market-2", kind: "marktbesuch", startAt: at("13:25"), endAt: at("14:00"), marketName: "Markt 2" },
    ],
  });

  assert.deepEqual(
    session.timeline.map((segment) => ({
      kind: segment.kind,
      start: segment.start,
      end: segment.end,
      title: segment.title,
    })),
    [
      { kind: "anfahrt", start: "10:00", end: "10:10", title: "Anfahrt" },
      { kind: "marktbesuch", start: "10:10", end: "10:50", title: "Markt 1" },
      { kind: "fahrtzeit", start: "10:50", end: "12:10", title: "Fahrtzeit" },
      { kind: "zusatzzeit", start: "12:10", end: "13:00", title: "Schulung" },
      { kind: "fahrtzeit", start: "13:00", end: "13:25", title: "Fahrtzeit" },
      { kind: "marktbesuch", start: "13:25", end: "14:00", title: "Markt 2" },
      { kind: "heimfahrt", start: "14:00", end: "14:15", title: "Heimfahrt" },
    ],
  );
});

test("admin zeiterfassung timeline emits no middle fahrtzeit when actions touch", () => {
  const session = buildSession({
    actions: [
      { id: "market-1", kind: "marktbesuch", startAt: at("10:00"), endAt: at("10:10"), marketName: "Markt 1" },
      { id: "extra-1", kind: "zusatzzeit", startAt: at("10:10"), endAt: at("10:20"), subtype: "lager" },
    ],
  });

  assert.equal(session.timeline.filter((segment) => segment.kind === "fahrtzeit").length, 0);
});

test("admin zeiterfassung timeline with no real actions shows one anfahrt row only", () => {
  const session = buildSession({ start: "10:00", end: "14:15", actions: [] });

  assert.deepEqual(
    session.timeline.map((segment) => ({ kind: segment.kind, start: segment.start, end: segment.end })),
    [{ kind: "anfahrt", start: "10:00", end: "14:15" }],
  );
});

test("admin zeiterfassung timeline splits zusatz around overlapping pause", () => {
  const session = buildSession({
    start: "09:00",
    end: "12:00",
    actions: [
      { id: "extra-1", kind: "zusatzzeit", startAt: at("10:00"), endAt: at("11:00"), subtype: "lager" },
      { id: "pause-1", kind: "pause", startAt: at("10:20"), endAt: at("10:30") },
    ],
  });

  assert.deepEqual(
    session.timeline.map((segment) => ({ id: segment.id, kind: segment.kind, start: segment.start, end: segment.end })),
    [
      { id: "anfahrt-day-1", kind: "anfahrt", start: "09:00", end: "10:00" },
      { id: "extra-1::seg:1", kind: "zusatzzeit", start: "10:00", end: "10:20" },
      { id: "pause-1", kind: "pause", start: "10:20", end: "10:30" },
      { id: "extra-1::seg:2", kind: "zusatzzeit", start: "10:30", end: "11:00" },
      { id: "heimfahrt-day-1", kind: "heimfahrt", start: "11:00", end: "12:00" },
    ],
  );
});

test("admin zeiterfassung validation blocks impossible overlaps except pause plus zusatz", () => {
  const intervals: TimelineInterval[] = [
    { id: "market-1", kind: "marktbesuch", startAt: at("10:00"), endAt: at("11:00") },
    { id: "pause-1", kind: "pause", startAt: at("12:00"), endAt: at("12:30") },
  ];

  const marketOverlap = validateTimelineInterval({
    kind: "zusatzzeit",
    id: "extra-1",
    startAt: at("10:30"),
    endAt: at("11:30"),
    dayStartAt: at("09:00"),
    dayEndAt: at("17:00"),
    intervals,
  });
  assert.deepEqual(marketOverlap, {
    ok: false,
    code: "overlap",
    message: TIMELINE_OVERLAP_ERROR_MESSAGE,
    conflict: intervals[0],
  });

  const pauseOverlap = validateTimelineInterval({
    kind: "zusatzzeit",
    id: "extra-2",
    startAt: at("12:10"),
    endAt: at("12:20"),
    dayStartAt: at("09:00"),
    dayEndAt: at("17:00"),
    intervals,
  });
  assert.deepEqual(pauseOverlap, { ok: true });
});
