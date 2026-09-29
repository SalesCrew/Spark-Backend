import { Router } from "express";
import { z } from "zod";
import type { ModelDatabase } from "../lib/praemien-workspace.js";
import { setModuleCatalogState } from "../lib/module-catalog-state.js";

// Mounted inside the existing authenticated, permission-checked admin router.
export function createModuleCatalogStateRouter(database: ModelDatabase) {
  const router = Router();
  router.patch("/modules/:scope/:id/catalog-state", async (req, res, next) => {
    try {
      const params = z.object({ scope: z.enum(["main", "kuehler", "mhd", "durcharbeit"]), id: z.string().uuid() }).safeParse(req.params);
      const body = z.object({ inactive: z.boolean() }).strict().safeParse(req.body);
      if (!params.success || !body.success) {
        res.status(400).json({ error: "Ungültiger Modulstatus." });
        return;
      }
      const result = await setModuleCatalogState(database, params.data.scope, params.data.id, body.data.inactive);
      if (result.status === 503) {
        res.status(503).json({ error: "Die Modulstatus-Erweiterung ist noch nicht eingerichtet.", code: "module_catalog_state_not_ready" });
      } else if (result.status === 404) {
        res.status(404).json({ error: "Modul nicht gefunden." });
      } else {
        res.set("Cache-Control", "private, no-store").json({ module: result.module });
      }
    } catch (error) { next(error); }
  });
  return router;
}
