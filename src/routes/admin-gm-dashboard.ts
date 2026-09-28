import { Router } from "express";
import { requireAuth } from "../middleware/auth.js";
import { requireKundeAdminPermission } from "../lib/kunde-access.js";
import { db } from "../lib/db.js";
import { modelDatabase } from "../lib/praemien-workspace.js";
import { loadEffectiveGmIppPeriods } from "../lib/ipp-gm-effective.js";
import { createGmDashboardRouter } from "./gm-dashboard.js";
export const adminGmDashboardRouter = Router();
adminGmDashboardRouter.use(
  requireAuth(["admin", "kunde"]),
  requireKundeAdminPermission,
);
adminGmDashboardRouter.use(
  createGmDashboardRouter(modelDatabase(db), async (intervals, scope, data) => {
    // Whole-GM RED views use the existing archived/corrected IPP service. A
    // correction for an entire GM must not be spread arbitrarily across markets.
    if (scope.region || scope.chain || scope.marketId) return;
    const ids = intervals
      .filter((i) => /^[0-9a-f-]{36}$/i.test(i.id))
      .map((i) => i.id);
    if (!ids.length) return;
    const rows = await loadEffectiveGmIppPeriods({
      redPeriodIds: ids,
      ...(scope.gmId ? { gmUserIds: [scope.gmId] } : {}),
      includeEmptyGms: false,
    });
    for (const point of data.points) {
      if (!ids.includes(point.id)) continue;
      const samples = rows.filter(
        (row) =>
          row.redPeriodId === point.id &&
          (row.calculationSource !== "no_data" || row.adjustment),
      );
      const weight = (row: (typeof samples)[number]) =>
        row.marketSampleCount || (row.adjustment ? 1 : 0);
      const total = samples.reduce((sum, row) => sum + weight(row), 0);
      point.ipp = total
        ? Math.round(
            (samples.reduce(
              (sum, row) => sum + row.effectiveIpp * weight(row),
              0,
            ) /
              total) *
              10000,
          ) / 10000
        : samples.length
          ? 0
          : null;
      point.ippMarketCount = samples.reduce(
        (sum, row) => sum + row.marketSampleCount,
        0,
      );
      point.ippSource = "effective_red";
    }
  }),
);
