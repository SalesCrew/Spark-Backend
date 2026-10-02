import { Router, type Request } from "express";
import { sql } from "drizzle-orm";
import { z } from "zod";
import {
  ModelError,
  modelSchema,
  modelTemplate,
  mutateWorkspace,
  readWorkspace,
  simulateWorkspace,
  workspaceReady,
  type ModelDatabase,
} from "../lib/praemien-workspace.js";
import type { AuthedRequest } from "../middleware/auth.js";

// Mounted below the existing admin/kunde auth and page-permission middleware.
// The same router is mounted behind synthetic authentication only in local tests.
export function createPraemienWorkspaceRouter(database: ModelDatabase) {
  const router = Router();
  // Feature detection is a normal read, not an error on an unmigrated database.
  router.get("/status", async (_req, res, next) => {
    try {
      res.json({ ready: await workspaceReady(database) });
    } catch (error) {
      next(error);
    }
  });
  router.use(async (_req, res, next) => {
    try {
      if (!(await workspaceReady(database)))
        throw new ModelError(
          503,
          "Prämien-Erweiterung noch nicht eingerichtet. Zuerst die vorbereitete Migration anwenden.",
        );
      next();
    } catch (error) {
      next(error);
    }
  });
  router.get("/templates/:template", (req, res) => {
    const template = z
      .enum(["empty", "q1", "q2", "q3", "xmas"])
      .parse(req.params.template);
    res.json({ model: modelTemplate(template) });
  });
  router.get("/waves", async (_req, res, next) => {
    try {
      res.json({
        waves: await database.query(
          sql`select w.id,w.name,w.year,w.quarter,w.status,w.start_date::text as "startDate",w.end_date::text as "endDate",s.revision,s.closed_at::text as "closedAt" from praemien_waves w left join praemien_wave_settings s on s.wave_id=w.id where w.is_deleted=false order by w.year desc,w.quarter desc,w.created_at desc`,
        ),
      });
    } catch (error) {
      next(error);
    }
  });
  router.post("/waves", async (req: AuthedRequest, res, next) => {
    try {
      const input = z
        .object({
          name: z.string().trim().min(1).max(120),
          year: z.number().int().min(2020).max(2100),
          quarter: z.number().int().min(1).max(4),
          template: z.enum(["empty", "q1", "q2", "q3", "xmas"]),
          model: modelSchema.optional(),
        })
        .parse(req.body);
      const start = new Date(Date.UTC(input.year, (input.quarter - 1) * 3, 1))
          .toISOString()
          .slice(0, 10),
        end = new Date(Date.UTC(input.year, input.quarter * 3, 0))
          .toISOString()
          .slice(0, 10);
      const workspace = await database.transaction(async (tx) => {
        const [wave] = await tx.query<{ id: string }>(
          sql`insert into praemien_waves(name,year,quarter,start_date,end_date,status,reward_model) values(${input.name},${input.year},${input.quarter},${start},${end},'draft','pillar_tiers') returning id`,
        );
        if (!wave)
          throw new ModelError(500, "Entwurf konnte nicht erstellt werden.");
        return mutateWorkspace(tx, wave.id, 0, await actor(tx, req), {
          type: "rules",
          model: input.model ?? modelTemplate(input.template),
        });
      });
      res.status(201).json(workspace);
    } catch (error) {
      next(error);
    }
  });
  router.get("/sources", async (_req, res, next) => {
    try {
      res.json({
        questions: await database.query(
          sql`select q.id,q.text,q.question_type as type,q.config, q.updated_at::text as "updatedAt", coalesce(jsonb_agg(jsonb_build_object('scoreKey',s.score_key,'weight',s.boni)) filter(where s.id is not null),'[]'::jsonb) as scores from question_bank_shared q left join question_scoring s on s.question_id=q.id and s.is_deleted=false where q.is_deleted=false group by q.id order by q.text`,
        ),
      });
    } catch (error) {
      next(error);
    }
  });
  router.get("/sources/:questionId/usage", async (req, res, next) => {
    try {
      const id = z.string().uuid().parse(req.params.questionId);
      const settings = await database.query<{
        waveId: string;
        name: string;
        status: string;
        model: import("../praemien-model.shared.js").WaveModel;
      }>(
        sql`select s.wave_id as "waveId",w.name,w.status,s.model from praemien_wave_settings s join praemien_waves w on w.id=s.wave_id where w.is_deleted=false`,
      );
      const usage = settings.flatMap((w) =>
        w.model.pillars.flatMap((p) =>
          p.metrics.flatMap((m) =>
            m.sources
              .filter((s) => s.questionId === id)
              .map((s) => ({
                waveId: w.waveId,
                waveName: w.name,
                status: w.status,
                pillar: p.name,
                metric: m.label,
                unit: m.unit,
                source: s,
              })),
          ),
        ),
      );
      res.json({ usage });
    } catch (error) {
      next(error);
    }
  });
  router.get("/leaderboard", async (_req, res, next) => {
    try {
      const snapshots = await database.query<{
        snapshot: {
          results: {
            gmId: string;
            name: string;
            active: boolean;
            earned: number;
          }[];
        };
      }>(
        sql`select closed_snapshot as snapshot from praemien_wave_settings where closed_snapshot is not null`,
      );
      const totals = new Map<
        string,
        {
          gmId: string;
          name: string;
          active: boolean;
          earned: number;
          quarters: number;
        }
      >();
      for (const row of snapshots)
        for (const gm of row.snapshot.results) {
          const old = totals.get(gm.gmId) ?? { ...gm, earned: 0, quarters: 0 };
          old.earned = Math.round((old.earned + gm.earned) * 100) / 100;
          old.quarters++;
          totals.set(gm.gmId, old);
        }
      res.json({
        results: [...totals.values()].sort(
          (a, b) => b.earned - a.earned || a.name.localeCompare(b.name, "de"),
        ),
      });
    } catch (error) {
      next(error);
    }
  });
  router.get("/waves/:id", async (req, res, next) => {
    try {
      res.json(
        await readWorkspace(database, z.string().uuid().parse(req.params.id)),
      );
    } catch (error) {
      next(error);
    }
  });
  router.post("/waves/:id/preview", async (req, res, next) => {
    try {
      res.json(
        await simulateWorkspace(
          database,
          z.string().uuid().parse(req.params.id),
          req.body.model,
          req.body.entries,
        ),
      );
    } catch (error) {
      next(error);
    }
  });
  router.put("/waves/:id", async (req: AuthedRequest, res, next) => {
    try {
      const revision = z.number().int().nonnegative().parse(req.body.revision);
      const type = z
        .enum(["rules", "values", "activate", "archive"])
        .parse(req.body.type);
      res.json(
        await mutateWorkspace(
          database,
          z.string().uuid().parse(req.params.id),
          revision,
          await actor(database, req),
          type === "rules"
            ? { type, model: req.body.model }
            : type === "values"
              ? { type, entries: req.body.entries }
              : { type },
        ),
      );
    } catch (error) {
      next(error);
    }
  });
  router.use(
    (
      error: unknown,
      _req: Request,
      res: import("express").Response,
      next: import("express").NextFunction,
    ) => {
      if (error instanceof ModelError) {
        res
          .status(error.status)
          .json({ error: error.message, code: "praemien_workspace_error" });
        return;
      }
      if (error instanceof z.ZodError) {
        res.status(400).json({
          error:
            "Ungültige Eingabe: " +
            error.issues
              .map((e) => `${e.path.join(".")}: ${e.message}`)
              .join("; "),
          code: "invalid_payload",
        });
        return;
      }
      if ((error as { code?: string })?.code === "23505") {
        res.status(409).json({
          error: "Für dieses Quartal existiert bereits eine laufende Welle.",
        });
        return;
      }
      next(error);
    },
  );
  return router;
}
async function actor(database: ModelDatabase, req: AuthedRequest) {
  const id = req.authUser?.appUserId;
  if (!id) throw new ModelError(401, "Nicht eingeloggt.");
  const [user] = await database.query<{ name: string }>(
    sql`select trim(first_name || ' ' || last_name) as name from users where id=${id}::uuid`,
  );
  if (!user) throw new ModelError(401, "Benutzer nicht gefunden.");
  return { id, name: user.name };
}
