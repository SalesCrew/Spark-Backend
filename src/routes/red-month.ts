import { Router } from "express";
import { z } from "zod";
import { requireKundeAdminPermission } from "../lib/kunde-access.js";
import { logAction, startActionTimer } from "../lib/logger.js";
import {
  activateRedMonthYear,
  createRedMonthYear,
  generateRedMonthPeriodsPreview,
  listRedMonthPeriods,
  listRedMonthYears,
  redMonthPeriodToYmd,
  resolveCurrentRedPeriod,
  updateDraftRedMonthYear,
  type GeneratedRedMonthPeriod,
  type RedMonthYearRecord,
  type ResolvedRedMonthPeriod,
} from "../lib/red-month-periods.js";
import {
  refreshRedMonthCalendarConfig,
  setActiveRedMonthCalendarConfig,
} from "../lib/red-month-calendar.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const redMonthRouter = Router();
const adminRedMonthRouter = Router();

const ymdRegex = /^\d{4}-\d{2}-\d{2}$/;

const getCalendarQuerySchema = z.object({
  from: z.string().regex(ymdRegex).optional(),
  to: z.string().regex(ymdRegex).optional(),
});

const previewYearSchema = z
  .object({
    redYear: z.number().int().min(2000).max(2100),
    anchorStart: z.string().regex(ymdRegex),
    cycleWeeks: z.array(z.number().int().positive()).min(1).default([4, 4, 5]),
    periodCount: z.number().int().positive().max(20).default(13),
    timezone: z.string().trim().min(1).optional(),
  })
  .strict();

const createYearSchema = previewYearSchema.extend({
  status: z.enum(["draft", "active", "locked"]).optional(),
});

const updateYearSchema = z
  .object({
    anchorStart: z.string().regex(ymdRegex),
    cycleWeeks: z.array(z.number().int().positive()).min(1),
    periodCount: z.number().int().positive().max(20).default(13),
    timezone: z.string().trim().min(1).optional(),
  })
  .strict();

const updateLegacyConfigSchema = z
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

function formatPeriodDto(period: ResolvedRedMonthPeriod, current: ResolvedRedMonthPeriod) {
  const { startYmd, endYmd } = redMonthPeriodToYmd(period);
  const today = new Date();
  const todayStart = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  return {
    id: period.redPeriodId ?? startYmd,
    redPeriodId: period.redPeriodId,
    redMonthYearId: period.redMonthYearId,
    label: period.label,
    periodIndexFromAnchor: period.periodIndexFromAnchor,
    periodIndex: period.periodIndex,
    start: startYmd,
    end: endYmd,
    lookupEnd: toYmd(period.lookupEnd),
    year: period.redYear,
    status: period.status,
    isCurrent: period.redPeriodId
      ? period.redPeriodId === current.redPeriodId
      : period.periodIndexFromAnchor === current.periodIndexFromAnchor && startYmd === toYmd(current.start),
    daysUntilEnd: Math.max(0, Math.floor((period.end.getTime() - todayStart.getTime()) / (24 * 60 * 60 * 1000))),
  };
}

function formatGeneratedPeriodDto(period: GeneratedRedMonthPeriod) {
  return {
    id: `preview-${period.periodIndex}`,
    redPeriodId: null,
    redMonthYearId: null,
    label: period.label,
    periodIndexFromAnchor: period.periodIndex - 1,
    periodIndex: period.periodIndex,
    start: toYmd(period.start),
    end: toYmd(period.end),
    lookupEnd: toYmd(period.lookupEnd),
    year: period.start.getFullYear(),
    status: "draft",
    isCurrent: false,
    daysUntilEnd: 0,
  };
}

function formatYearDto(year: RedMonthYearRecord) {
  return {
    id: year.id,
    redMonthYearId: year.id,
    redYear: year.redYear,
    anchorStart: toYmd(year.anchorStart),
    cycleWeeks: year.cycleWeeks,
    periodCount: year.periodCount,
    timezone: year.timezone,
    status: year.status,
    createdAt: year.createdAt.toISOString(),
    updatedAt: year.updatedAt.toISOString(),
  };
}

function formatConfigDto(current: ResolvedRedMonthPeriod) {
  return {
    redMonthYearId: current.redMonthYearId,
    redYear: current.redYear,
    anchorStart: toYmd(current.anchorStart),
    cycleWeeks: current.yearCycleWeeks,
    periodCount: current.periodCount,
    timezone: current.timezone,
    status: current.status,
    updatedAt: current.updatedAt?.toISOString() ?? null,
  };
}

redMonthRouter.use(requireAuth(["admin", "gm", "sm", "kunde"]));
adminRedMonthRouter.use(requireAuth(["admin", "kunde"]));
adminRedMonthRouter.use(requireKundeAdminPermission);

redMonthRouter.use((req, res, next) => {
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "red_month_read_completed", {
      req,
      action: "red_month_read",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

adminRedMonthRouter.use("/red-month", (req, res, next) => {
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "red_month_admin_action_completed", {
      req,
      action: "red_month_admin",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

redMonthRouter.get("/current", async (_req, res, next) => {
  try {
    const current = await resolveCurrentRedPeriod(new Date());
    res.status(200).json({
      current: formatPeriodDto(current, current),
      config: formatConfigDto(current),
    });
  } catch (error) {
    next(error);
  }
});

redMonthRouter.get("/calendar", async (req, res, next) => {
  try {
    const parsed = getCalendarQuerySchema.safeParse(req.query ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: "Ungültige Kalender-Parameter." });
      return;
    }
    const now = new Date();
    const current = await resolveCurrentRedPeriod(now);
    const periodInput: { now: Date; from?: string; to?: string } = { now };
    if (parsed.data.from) periodInput.from = parsed.data.from;
    if (parsed.data.to) periodInput.to = parsed.data.to;
    const periods = await listRedMonthPeriods(periodInput);
    res.status(200).json({
      periods: periods.map((period) => formatPeriodDto(period, current)),
    });
  } catch (error) {
    next(error);
  }
});

adminRedMonthRouter.get("/red-month/years", async (_req, res, next) => {
  try {
    const [years, nowCurrent] = await Promise.all([listRedMonthYears(), resolveCurrentRedPeriod(new Date())]);
    res.status(200).json({
      years: years.map(formatYearDto),
      current: formatPeriodDto(nowCurrent, nowCurrent),
    });
  } catch (error) {
    next(error);
  }
});

adminRedMonthRouter.post("/red-month/years/preview", async (req, res, next) => {
  try {
    const parsed = previewYearSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.issues[0]?.message ?? "Ungültiges RED-Jahr." });
      return;
    }
    const periods = generateRedMonthPeriodsPreview(parsed.data);
    res.status(200).json({
      year: {
        redYear: parsed.data.redYear,
        anchorStart: parsed.data.anchorStart,
        cycleWeeks: parsed.data.cycleWeeks,
        periodCount: parsed.data.periodCount,
        timezone: parsed.data.timezone ?? "Europe/Vienna",
        status: "draft",
      },
      periods: periods.map(formatGeneratedPeriodDto),
    });
  } catch (error) {
    next(error);
  }
});

adminRedMonthRouter.post("/red-month/years", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = createYearSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.issues[0]?.message ?? "Ungültiges RED-Jahr." });
      return;
    }
    const created = await createRedMonthYear({
      redYear: parsed.data.redYear,
      anchorStart: parsed.data.anchorStart,
      cycleWeeks: parsed.data.cycleWeeks,
      periodCount: parsed.data.periodCount,
      ...(parsed.data.timezone ? { timezone: parsed.data.timezone } : {}),
      ...(parsed.data.status ? { status: parsed.data.status } : {}),
      createdByUserId: req.authUser?.appUserId ?? null,
    });
    const current = await resolveCurrentRedPeriod(new Date());
    res.status(201).json({
      year: formatYearDto(created.year),
      periods: created.periods.map((period) => formatPeriodDto(period, current)),
    });
  } catch (error) {
    next(error);
  }
});

adminRedMonthRouter.patch("/red-month/years/:id", async (req, res, next) => {
  try {
    const parsed = updateYearSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.issues[0]?.message ?? "Ungültiges RED-Jahr." });
      return;
    }
    const updated = await updateDraftRedMonthYear({
      id: String(req.params.id ?? ""),
      anchorStart: parsed.data.anchorStart,
      cycleWeeks: parsed.data.cycleWeeks,
      periodCount: parsed.data.periodCount,
      ...(parsed.data.timezone ? { timezone: parsed.data.timezone } : {}),
    });
    const current = await resolveCurrentRedPeriod(new Date());
    res.status(200).json({
      year: formatYearDto(updated.year),
      periods: updated.periods.map((period) => formatPeriodDto(period, current)),
    });
  } catch (error) {
    next(error);
  }
});

adminRedMonthRouter.post("/red-month/years/:id/activate", async (req, res, next) => {
  try {
    const year = await activateRedMonthYear(String(req.params.id ?? ""));
    res.status(200).json({ year: formatYearDto(year) });
  } catch (error) {
    next(error);
  }
});

adminRedMonthRouter.patch("/red-month/config", async (req, res, next) => {
  try {
    const parsed = updateLegacyConfigSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      res.status(400).json({ error: parsed.error.issues[0]?.message ?? "Ungültige RED-Monat Konfiguration." });
      return;
    }

    const years = await listRedMonthYears();
    if (years.length > 0) {
      res.status(409).json({
        error: "RED-Monate werden jetzt als Jahre gespeichert. Bitte den Plus-Button nutzen, statt die aktive Historie zu ?berschreiben.",
        code: "red_month_immutable_years_enabled",
      });
      return;
    }

    const updated = await setActiveRedMonthCalendarConfig({
      anchorStart: parsed.data.anchorStart,
      cycleWeeks: parsed.data.cycleWeeks,
      ...(parsed.data.timezone ? { timezone: parsed.data.timezone } : {}),
    });
    await refreshRedMonthCalendarConfig();
    const current = await resolveCurrentRedPeriod(new Date());
    res.status(200).json({
      config: {
        anchorStart: toYmd(updated.anchorStart),
        cycleWeeks: updated.cycleWeeks,
        timezone: updated.timezone,
        updatedAt: updated.updatedAt?.toISOString() ?? null,
      },
      current: formatPeriodDto(current, current),
    });
  } catch (error) {
    next(error);
  }
});

export { redMonthRouter, adminRedMonthRouter };
