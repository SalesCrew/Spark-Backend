import { Router } from "express";
import { z } from "zod";
import type { AuthedRequest } from "../middleware/auth.js";
import { isFullAdminRole } from "../lib/admin-role.js";
import { logAction } from "../lib/logger.js";
import { CampaignExtensionError, extendCampaign, type CampaignExtensionDependencies } from "../lib/campaign-extension.js";

const body = z.object({ endDate: z.string(), expectedEndDate: z.string(), expectedUpdatedAt: z.iso.datetime({ offset: true }) }).strict();

/** Mount after the existing campaign auth and Kunde update-permission middleware. */
export function createCampaignExtensionRouter(dependencies: CampaignExtensionDependencies) {
  const router = Router();
  router.patch("/:id/extend", async (req: AuthedRequest, res, next) => {
    if (!req.authUser) return void res.status(401).json({ error: "Anmeldung erforderlich." });
    if (!isFullAdminRole(req.authUser.role) && req.authUser.role !== "kunde") return void res.status(403).json({ error: "Keine Berechtigung." });
    const id = z.uuid().safeParse(req.params.id), parsed = body.safeParse(req.body);
    if (!id.success || !parsed.success) return void res.status(400).json({ error: "Ungültige Kampagnenverlängerung.", code: "invalid_payload" });
    try {
      const result = await extendCampaign(dependencies, id.data, parsed.data);
      logAction("info", "campaign_extend_success", { req, action: "campaign_extend", result: "success", statusCode: 200, details: { campaignId: id.data, previousEndDate: result.previousEndDate, endDate: result.campaign.endDate, previousStatus: result.previousStatus, status: result.campaign.status } });
      res.set("Cache-Control", "private, no-store").json({ campaign: result.campaign });
    } catch (error) {
      if (error instanceof CampaignExtensionError) return void res.status(error.status).json({ error: error.message, code: error.code, ...(error.conflicts ? { conflicts: error.conflicts } : {}) });
      next(error);
    }
  });
  return router;
}
