import { Router } from "express";
import { z } from "zod";
import { addDays, getRedPeriodForDate, getRedPeriodLabel, getRedYear, startOfDay } from "../lib/red-monat.js";
import {
  getCachedRedMonthCalendarConfig,
  refreshRedMonthCalendarConfig,
  setActiveRedMonthCalendarConfig,
} from "../lib/red-month-calendar.js";
import { requireAuth } from "../middleware/auth.js";

const redMonthRouter = Router();
const adminRedMonthRouter = Router();

const ymdRegex = /^\d{4}-\d{2}-\d{2}$/;

const getCalendarQuerySchema = z.object({
  from: z.string().regex(ymdRegex).optional(),
  to: z.string().regex(ymdRegex).optional(),
});

const updateConfigSchema = z
  .object({
    anchorStart: z.string().regex(ymdRegex),
    cycleWeeks: z.array(z.number().int().positive()).min(1),
    timezone: z.string().trim().min(1).optional(),
  })
  .strict();

function toYmd(date: Date): string {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, "0");
  const d = String(date.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function parseYmd(value: string): Date {
  const [rawYear, rawMonth, rawDay] = value.split("-");
  const year = Number(rawYear ?? "0");
  const month = Number(rawMonth ?? "0");
  const day = Number(rawDay ?? "0");
  return new Date(year, Math.max(0, month - 1), Math.max(1, day));
}

function buildPeriodDto(input: { start: Date; end: Date; now: Date }) {
  const start = startOfDay(input.start);
  const end = startOfDay(input.end);
  const today = startOfDay(input.now);
  return {
    id: `${toYmd(start)}`,
    label: getRedPeriodLabel(start),
    periodIndexFromAnchor: getRedPeriodForDate(start).periodIndexFromAnchor,
    start: toYmd(start),
    end: toYmd(end),
    year: getRedYear(start),
    isCurrent: today >= start && today <= end,
    daysUntilEnd: Math.max(0, Math.floor((end.getTime() - today.getTime()) / (24 * 60 * 60 * 1000))),
  };
}

function buildPeriodsBetween(input: { from: Date; to: Date; now: Date }): ReturnType<typeof buildPeriodDto>[] {
  const from = startOfDay(input.from);
  const to = startOfDay(input.to);
  if (from > to) return [];
  const periods: ReturnType<typeof buildPeriodDto>[] = [];
  let cursor = getRedPeriodForDate(from).start;
  for (let i = 0; i < 500; i += 1) {
    const info = getRedPeriodForDate(cursor);
    if (info.start > to) break;
    periods.push(buildPeriodDto({ start: info.start, end: info.end, now: input.now }));
    cursor = addDays(info.end, 1);
  }
  return periods;
}

redMonthRouter.use(requireAuth(["admin", "gm", "sm"]));
adminRedMonthRouter.use(requireAuth(["admin"]));

redMonthRouter.get("/current", async (_req, res, next) => {
  try {
    await refreshRedMonthCalendarConfig();
    const now = new Date();
    const current = getRedPeriodForDate(now);
    const config = getCachedRedMonthCalendarConfig();
    res.status(200).json({
      current: buildPeriodDto({ start: current.start, end: current.end, now }),
      config: {
        anchorStart: toYmd(config.anchorStart),
        cycleWeeks: config.cycleWeeks,
        timezone: config.timezone,
        updatedAt: config.updatedAt?.toISOString() ?? null,
      },
    });
  } catch (error) {
    next(error);
  }
});

redMonthRouter.get("/calendar", async (req, res, next) => {
  try {
    await refreshRedMonthCalendarConfig();
    const parsed = getCalendarQuerySchema.safeParse(req.query ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige Kalender-Parameter." });
      return;
    }
    const now = new Date();
    const current = getRedPeriodForDate(now);
    const from = parsed.data.from ? parseYmd(parsed.data.from) : addDays(current.start, -200);
    const to = parsed.data.to ? parseYmd(parsed.data.to) : addDays(current.end, 400);
    res.status(200).json({
      periods: buildPeriodsBetween({ from, to, now }),
    });
  } catch (error) {
    next(error);
  }
});

adminRedMonthRouter.patch("/red-month/config", async (req, res, next) => {
  try {
    const parsed = updateConfigSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungueltige RED-Monat Konfiguration." });
      return;
    }
    const updated = await setActiveRedMonthCalendarConfig({
      anchorStart: parsed.data.anchorStart,
      cycleWeeks: parsed.data.cycleWeeks,
      ...(parsed.data.timezone ? { timezone: parsed.data.timezone } : {}),
    });
    const now = new Date();
    const current = getRedPeriodForDate(now);
    res.status(200).json({
      config: {
        anchorStart: toYmd(updated.anchorStart),
        cycleWeeks: updated.cycleWeeks,
        timezone: updated.timezone,
        updatedAt: updated.updatedAt?.toISOString() ?? null,
      },
      current: buildPeriodDto({ start: current.start, end: current.end, now }),
    });
  } catch (error) {
    next(error);
  }
});

export { redMonthRouter, adminRedMonthRouter };

