import { Router } from "express";
import { z } from "zod";
import { createMileageService } from "../lib/gm-mileage-service.js";
import { db } from "../lib/db.js";
import type { AuthedRequest } from "../middleware/auth.js";

export const segmentSchema = z.object({ startKm: z.number().int().min(0).max(10_000_000), endKm: z.number().int().min(0).max(10_000_000) });
const date = z.iso.date();
const rangeSchema = z.object({ sessionId: z.uuid(), from: date, to: date }).refine(v => v.from <= v.to && Date.parse(v.to) - Date.parse(v.from) <= 366 * 86400000, "Bitte einen gültigen Zeitraum bis zu einem Jahr wählen.");
const correctionSchema = rangeSchema.extend({ edits: z.array(z.object({ id: z.uuid(), segments: z.array(segmentSchema).min(1).max(8) })).min(1).max(370), returnAfter: z.boolean(), breakBefore: z.boolean(), reason: z.string().trim().min(2).max(500), fingerprint: z.string().length(64).optional(), operationId: z.uuid().optional() });
export const mileageRouter = Router();
const service = createMileageService(db);
// Mounted after Zeiterfassung's authentication and customer page-permission middleware.
mileageRouter.use((req: AuthedRequest, res, next) => {
  if (req.authUser?.role !== "admin") { res.status(403).json({ error: "Fahrzeugkorrekturen sind nur für Administratoren verfügbar." }); return; }
  next();
});
function failure(error: unknown, res: import("express").Response, next: import("express").NextFunction) {
  let cause = error as { code?: string; cause?: unknown };
  while (cause?.cause) cause = cause.cause as typeof cause;
  if (cause?.code === "42P01") { res.status(503).json({ error: "Fahrzeugkorrekturen sind nach Freigabe der Datenbank-Erweiterung verfügbar. Bestehende Zeiten bleiben unverändert.", code: "mileage_schema_required" }); return; }
  next(error);
}
mileageRouter.get("/context", async (req, res, next) => {
  try {
    const parsed = rangeSchema.safeParse(req.query);
    if (!parsed.success) { res.status(400).json({ error: "Bitte einen gültigen Zeitraum wählen." }); return; }
    const context = await service.context(parsed.data.sessionId, parsed.data.from, parsed.data.to);
    res.json({ ...context, history: await service.history(parsed.data.sessionId) });
  } catch (error) { failure(error, res, next); }
});
for (const action of ["preview", "apply"] as const) {
  mileageRouter.post(`/${action}`, async (req: AuthedRequest, res, next) => {
    try {
      const parsed = correctionSchema.safeParse(req.body);
      if (!parsed.success) { res.status(400).json({ error: "Bitte Zeitraum, Grund und alle Kilometerstände vollständig angeben." }); return; }
      res.json(action === "preview" ? await service.preview(parsed.data) : await service.apply(parsed.data, req.authUser!.appUserId));
    } catch (error) { failure(error, res, next); }
  });
}
