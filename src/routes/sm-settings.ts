import { and, eq } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import { gmTextSettings } from "../lib/schema.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const smSettingsRouter = Router();

const textScalePayloadSchema = z.object({
  textScalePercent: z.number().int().min(0).max(50),
});

smSettingsRouter.use(requireAuth(["sm"]));

smSettingsRouter.get("/text-scale", async (req: AuthedRequest, res, next) => {
  try {
    const userId = req.authUser?.appUserId;
    if (!userId) return res.status(401).json({ error: "auth_required" });

    const [settings] = await db
      .select({
        textScalePercent: gmTextSettings.textScalePercent,
        updatedAt: gmTextSettings.updatedAt,
      })
      .from(gmTextSettings)
      .where(and(eq(gmTextSettings.userId, userId), eq(gmTextSettings.isDeleted, false)))
      .limit(1);

    return res.json({
      textScalePercent: settings?.textScalePercent ?? 0,
      updatedAt: settings?.updatedAt?.toISOString() ?? null,
    });
  } catch (error) {
    return next(error);
  }
});

smSettingsRouter.patch("/text-scale", async (req: AuthedRequest, res, next) => {
  try {
    const userId = req.authUser?.appUserId;
    if (!userId) return res.status(401).json({ error: "auth_required" });

    const parsed = textScalePayloadSchema.safeParse(req.body ?? {});
    if (!parsed.success) {
      return res.status(400).json({ error: "invalid_payload", issues: parsed.error.issues });
    }

    const now = new Date();
    const [settings] = await db
      .insert(gmTextSettings)
      .values({
        userId,
        textScalePercent: parsed.data.textScalePercent,
        isDeleted: false,
        deletedAt: null,
        updatedAt: now,
      })
      .onConflictDoUpdate({
        target: gmTextSettings.userId,
        set: {
          textScalePercent: parsed.data.textScalePercent,
          isDeleted: false,
          deletedAt: null,
          updatedAt: now,
        },
      })
      .returning({
        textScalePercent: gmTextSettings.textScalePercent,
        updatedAt: gmTextSettings.updatedAt,
      });

    return res.json({
      textScalePercent: settings?.textScalePercent ?? parsed.data.textScalePercent,
      updatedAt: settings?.updatedAt?.toISOString() ?? now.toISOString(),
    });
  } catch (error) {
    return next(error);
  }
});

export { smSettingsRouter };
