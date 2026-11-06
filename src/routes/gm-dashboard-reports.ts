import { Router, type Request } from "express";
import { z } from "zod";
import type { ModelDatabase } from "../lib/praemien-workspace.js";
import { prepareDashboardReport, type ReportPhotoReader } from "../lib/gm-report-service.js";
import { reportBrands } from "../gm-report.shared.js";

const date = z.string().regex(/^\d{4}-\d{2}-\d{2}$/).refine(v => Number.isFinite(Date.parse(v)) && new Date(v).toISOString().slice(0,10) === v);
export const dashboardReportSchema = z.object({
  kind: z.enum(["availability", "competitor"]),
  periods: z.array(z.object({ id: z.string().min(1).max(100), label: z.string().min(1).max(100), start: date, end: date }).strict().refine(p => p.end >= p.start && Date.parse(p.end)-Date.parse(p.start) <= 370*86400000)).min(1).max(3),
  cutoff: date,
  scope: z.object({ region: z.string().max(120).nullable(), gmId: z.string().uuid().nullable(), marketId: z.string().uuid().nullable(), chain: z.string().max(120).nullable(),
    marketIds: z.array(z.string().uuid()).max(10000).optional(), chains: z.array(z.string().max(120)).max(200).optional(), chainGroups: z.array(z.enum(["rewe","spar","other"])).max(3).optional(), stc: z.enum(["gold","silver","bronze"]).nullable() }).strict(),
  mappings: z.array(z.object({ questionId: z.string().uuid(), kind: z.enum(["cooler","placement"]).nullable(), brand: z.string().nullable().refine(v => v === null || reportBrands.includes(v)), excluded: z.boolean().optional() }).strict()).max(150).optional(),
}).strict().superRefine((value, ctx) => {
  const periods = [...value.periods].sort((a,b) => a.start.localeCompare(b.start));
  if (new Set(periods.map(p => p.id)).size !== periods.length || periods.some((p,i) => i > 0 && p.start <= periods[i-1]!.end)) ctx.addIssue({ code: "custom", message: "Vergleichszeiträume dürfen sich nicht überschneiden." });
  if (value.kind === "availability" && periods.length !== 1) ctx.addIssue({ code: "custom", message: "Für Verfügbarkeit bitte einen Zeitraum wählen." });
  if (Date.parse(periods[periods.length-1]!.end)-Date.parse(periods[0]!.start) > 3*366*86400000) ctx.addIssue({ code: "custom", message: "Maximal drei Jahre vergleichbar." });
  if (periods.some(p => p.start > value.cutoff)) ctx.addIssue({ code: "custom", message: "Stichtag liegt vor einem gewählten Zeitraum." });
  if (new Set(value.mappings?.map(m => m.questionId)).size !== (value.mappings?.length ?? 0)) ctx.addIssue({ code: "custom", message: "Fragen dürfen nur einmal zugeordnet werden." });
});
export function createDashboardReportsRouter(database: ModelDatabase, photos?: { canRead: (req: Request) => Promise<boolean>; sign: ReportPhotoReader }) {
  const router = Router();
  router.post("/reports/query", async (req, res, next) => {
    try {
      const input = dashboardReportSchema.safeParse(req.body);
      if (!input.success) { res.status(400).json({ error: input.error.issues[0]?.message ?? "Ungültige Berichtsauswahl." }); return; }
      const today = new Intl.DateTimeFormat("sv-SE", { timeZone: "Europe/Vienna" }).format(new Date());
      if (input.data.cutoff > today) { res.status(400).json({ error: "Der Stichtag darf nicht in der Zukunft liegen." }); return; }
      const periods = input.data.periods.map(p => ({ ...p, end: p.end < input.data.cutoff ? p.end : input.data.cutoff })).sort((a,b) => a.start.localeCompare(b.start));
      const reader = photos && input.data.kind === "availability" && await photos.canRead(req) ? photos.sign : undefined;
      const result = await prepareDashboardReport(database, { ...input.data, periods }, reader);
      res.set("Cache-Control", "private, no-store").json(result);
    } catch (error) {
      const status = (error as { status?: number }).status;
      if (status === 400 || status === 413) { res.status(status).json({ error: (error as Error).message }); return; }
      next(error);
    }
  });
  return router;
}
