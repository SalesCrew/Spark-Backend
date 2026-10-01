import { Router } from "express";
import { z } from "zod";
import { dashboardFacets, loadDashboard } from "../lib/gm-dashboard.js";
import type { ModelDatabase } from "../lib/praemien-workspace.js";
import type {
  DashboardData,
  DashboardInterval,
  DashboardScope,
} from "../gm-dashboard.shared.js";
const date = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}$/)
  .refine(
    (v) =>
      Number.isFinite(Date.parse(v)) &&
      new Date(v).toISOString().slice(0, 10) === v,
  );
const interval = z
  .object({
    id: z.string().min(1).max(100),
    label: z.string().max(160),
    shortLabel: z.string().max(80),
    start: date,
    end: date,
  })
  .refine(
    (v) =>
      v.start <= v.end &&
      Date.parse(v.end) - Date.parse(v.start) <= 370 * 86400000,
  );
const scope = z.object({
  region: z.string().max(120).nullable(),
  gmId: z.string().uuid().nullable(),
  chain: z.string().max(120).nullable(),
  chainGroups: z.array(z.enum(["rewe", "spar", "other"])).max(3).optional(),
  marketId: z.string().uuid().nullable(),
  marketIds: z.array(z.string().uuid()).optional(),
  stc: z.enum(["gold", "silver", "bronze"]).nullable(),
});
export function createGmDashboardRouter(
  database: ModelDatabase,
  effectiveIpp?: (
    intervals: DashboardInterval[],
    scope: DashboardScope,
    data: DashboardData,
  ) => Promise<void>,
) {
  const router = Router();
  router.get("/facets", async (_req, res, next) => {
    try {
      res
        .set("Cache-Control", "private, no-store")
        .json(await dashboardFacets(database));
    } catch (error) {
      next(error);
    }
  });
  router.post("/query", async (req, res, next) => {
    try {
      const input = z
        .object({
          intervals: z
            .array(interval)
            .min(1)
            .max(80)
            .refine(
              (list) =>
                new Set(list.map((v) => v.id)).size === list.length &&
                Math.max(...list.map((v) => Date.parse(v.end))) -
                  Math.min(...list.map((v) => Date.parse(v.start))) <=
                  3 * 366 * 86400000,
            ),
          scope,
        })
        .safeParse(req.body);
      if (!input.success) {
        res
          .status(400)
          .json({ error: "Ungültiger Dashboard-Zeitraum oder Filter." });
        return;
      }
      const data = await loadDashboard(
        database,
        input.data.intervals,
        input.data.scope,
      );
      if (effectiveIpp)
        await effectiveIpp(input.data.intervals, input.data.scope, data);
      res.set("Cache-Control", "private, no-store").json(data);
    } catch (error) {
      if (error instanceof SyntaxError) {
        res.status(400).json({ error: "Ungültige Dashboard-Abfrage." });
        return;
      }
      next(error);
    }
  });
  return router;
}
