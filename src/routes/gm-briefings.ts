import { Router, raw, type RequestHandler } from "express";
import { z } from "zod";
import type { AuthedRequest } from "../middleware/auth.js";
import type { BriefingService } from "../lib/gm-briefing-service.js";
import {
  briefingDraftSchema,
  briefingError,
  MAX_PDF_BYTES,
} from "../lib/gm-briefings.js";

const uuid = z.string().uuid();
export function createGmBriefingRouters(
  service: BriefingService,
  requireAuth: (roles: ("admin" | "kunde" | "gm")[]) => RequestHandler,
  permission: RequestHandler,
) {
  const admin = Router(),
    gm = Router();
  admin.use(requireAuth(["admin", "kunde"]), permission);
  gm.use(requireAuth(["gm"]));
  for (const router of [admin, gm])
    router.use((_req, res, next) => {
      res.setHeader("Cache-Control", "private, no-store");
      next();
    });
  const handle =
    (fn: (req: AuthedRequest) => Promise<unknown>): RequestHandler =>
    async (req, res, next) => {
      try {
        res.json(await fn(req as AuthedRequest));
      } catch (error) {
        next(error);
      }
    };
  const id = (req: AuthedRequest) => uuid.parse(req.params.id);
  const actor = (req: AuthedRequest) => {
    if (!req.authUser?.appUserId)
      throw briefingError("Bitte erneut anmelden.", 401);
    return req.authUser.appUserId;
  };
  const pdf = raw({ type: "application/pdf", limit: MAX_PDF_BYTES });
  function uploadInput(req: AuthedRequest) {
    if (!Buffer.isBuffer(req.body))
      throw briefingError("Bitte eine PDF hochladen.");
    let filename = "";
    try {
      filename = decodeURIComponent(
        req.header("x-pdf-filename") ?? "Wochenschwerpunkte.pdf",
      );
    } catch {
      throw briefingError("Ungültiger Dateiname.");
    }
    return { bytes: req.body, filename };
  }
  admin.get(
    "/",
    handle(async () => ({ items: await service.list() })),
  );
  admin.post(
    "/upload",
    pdf,
    handle(async (req) => {
      const input = uploadInput(req);
      return service.upload(
        input.bytes,
        input.filename,
        actor(req),
        uuid.parse(req.header("x-operation-id")),
      );
    }),
  );
  admin.get(
    "/:id/preview",
    handle((req) => service.preview(id(req))),
  );
  admin.patch(
    "/:id/pdf",
    pdf,
    handle(async (req) => {
      const input = uploadInput(req);
      const revision = z.coerce
        .number()
        .int()
        .positive()
        .parse(req.header("x-draft-revision"));
      return service.upload(
        input.bytes,
        input.filename,
        actor(req),
        uuid.parse(req.header("x-operation-id")),
        { id: id(req), expectedRevision: revision },
      );
    }),
  );
  admin.patch(
    "/:id",
    handle((req) => service.save(id(req), briefingDraftSchema.parse(req.body))),
  );
  admin.post(
    "/:id/publish",
    handle((req) => {
      const body = z
        .object({
          expectedRevision: z.number().int().positive(),
          operationId: uuid,
        })
        .strict()
        .parse(req.body);
      return service.publish(
        id(req),
        body.expectedRevision,
        body.operationId,
        actor(req),
      );
    }),
  );
  admin.patch(
    "/:id/archive",
    handle((req) =>
      service.archive(
        id(req),
        z.object({ archived: z.boolean() }).strict().parse(req.body).archived,
      ),
    ),
  );
  gm.get(
    "/",
    handle((req) => service.inbox(actor(req))),
  );
  gm.get(
    "/:id/file",
    handle((req) => service.file(id(req))),
  );
  gm.post(
    "/:id/read",
    handle((req) =>
      service.read(
        id(req),
        actor(req),
        z
          .object({ pageNumber: z.number().int().positive() })
          .strict()
          .parse(req.body).pageNumber,
      ),
    ),
  );
  // Keep payload and PDF errors legible without exposing storage paths or SDK details.
  for (const router of [admin, gm])
    router.use(
      (
        error: unknown,
        _req: unknown,
        res: import("express").Response,
        next: import("express").NextFunction,
      ) => {
        if (error instanceof z.ZodError) {
          res
            .status(400)
            .json({ error: "Bitte Eingaben prüfen.", issues: error.issues });
          return;
        }
        const e = error as { status?: number; type?: string; message?: string };
        if (e.type === "entity.too.large") {
          res
            .status(413)
            .json({ error: "Die PDF darf maximal 25 MB groß sein." });
          return;
        }
        if (e.status && e.status < 500) {
          res.status(e.status).json({ error: e.message });
          return;
        }
        next(error);
      },
    );
  return { admin, gm };
}
