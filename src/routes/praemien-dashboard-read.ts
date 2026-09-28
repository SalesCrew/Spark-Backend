import { Router } from "express";
import { sql } from "drizzle-orm";
import { z } from "zod";
import {
  ModelError,
  readWorkspace,
  workspaceReady,
  type ModelDatabase,
} from "../lib/praemien-workspace.js";

// Dashboard-only reads, mounted behind the existing Prämien auth/page checks.
// The separately prepared editor and its mutation routes are not enabled here.
export function createPraemienDashboardReadRouter(database: ModelDatabase) {
  const router = Router();
  router.get("/status", async (_req, res, next) => {
    try {
      res.set("Cache-Control", "private, no-store").json({
        ready: await workspaceReady(database),
      });
    } catch (error) {
      next(error);
    }
  });
  router.get("/waves", async (_req, res, next) => {
    try {
      if (!(await workspaceReady(database))) {
        res.status(503).json({ error: "Prämien noch nicht eingerichtet." });
        return;
      }
      const waves = await database.query(
        sql`select w.id,w.name,w.year,w.quarter,w.status,w.start_date::text as "startDate",w.end_date::text as "endDate",s.revision,s.closed_at::text as "closedAt" from praemien_waves w left join praemien_wave_settings s on s.wave_id=w.id where w.is_deleted=false order by w.year desc,w.quarter desc,w.created_at desc`,
      );
      res.set("Cache-Control", "private, no-store").json({ waves });
    } catch (error) {
      next(error);
    }
  });
  router.get("/waves/:id", async (req, res, next) => {
    const id = z.string().uuid().safeParse(req.params.id);
    if (!id.success) {
      res.status(400).json({ error: "Ungültige Prämienwelle." });
      return;
    }
    try {
      res.set("Cache-Control", "private, no-store").json(
        await readWorkspace(database, id.data),
      );
    } catch (error) {
      if (error instanceof ModelError) {
        res.status(error.status).json({ error: error.message });
        return;
      }
      next(error);
    }
  });
  return router;
}
