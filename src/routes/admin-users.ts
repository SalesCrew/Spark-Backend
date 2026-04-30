import { and, desc, eq } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { db } from "../lib/db.js";
import { authAuditLogs, type UserRole, users } from "../lib/schema.js";
import { supabaseAdmin } from "../lib/supabase.js";
import { generatePassword } from "../services/password.js";

const createUserSchema = z.object({
  role: z.enum(["admin", "gm", "sm"]),
  firstName: z.string().min(1),
  lastName: z.string().min(1),
  email: z.string().email(),
  phone: z.string().optional(),
  address: z.string().optional(),
  city: z.string().optional(),
  postalCode: z.string().optional(),
  region: z.string().optional(),
  ipp: z.number().min(0).max(99.9).optional(),
});

const updateUserSchema = z.object({
  firstName: z.string().min(1).optional(),
  lastName: z.string().min(1).optional(),
  email: z.string().email().optional(),
  phone: z.string().optional(),
  address: z.string().optional(),
  city: z.string().optional(),
  postalCode: z.string().optional(),
  region: z.string().optional(),
  ipp: z.number().min(0).max(99.9).optional(),
});

const adminUsersRouter = Router();

adminUsersRouter.use(requireAuth(["admin"]));

adminUsersRouter.get("/", async (req: AuthedRequest, res, next) => {
  try {
    const roleParam = req.query.role;
    const role = roleParam === "admin" || roleParam === "gm" || roleParam === "sm" ? roleParam : undefined;
    const whereClause = role ? eq(users.role, role) : undefined;

    const rows = await db
      .select()
      .from(users)
      .where(whereClause)
      .orderBy(desc(users.createdAt));

    res.status(200).json({
      users: rows.map((row) => ({
        id: row.id,
        supabaseAuthId: row.supabaseAuthId,
        role: row.role,
        email: row.email,
        firstName: row.firstName,
        lastName: row.lastName,
        phone: row.phone,
        address: row.address,
        city: row.city,
        postalCode: row.postalCode,
        region: row.region,
        ipp: row.ipp == null ? null : Number(row.ipp),
        isActive: row.isActive,
        deletedAt: row.deletedAt,
        createdAt: row.createdAt,
        updatedAt: row.updatedAt,
      })),
    });
  } catch (err) {
    next(err);
  }
});

adminUsersRouter.post("/", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = createUserSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid create user payload." });
      return;
    }

    const payload = parsed.data;
    const password = generatePassword();

    const { data: authCreated, error: authError } = await supabaseAdmin.auth.admin.createUser({
      email: payload.email,
      password,
      email_confirm: true,
      user_metadata: {
        role: payload.role,
        firstName: payload.firstName,
        lastName: payload.lastName,
      },
    });

    if (authError || !authCreated.user) {
      res.status(400).json({ error: authError?.message ?? "Failed to create auth user." });
      return;
    }

    try {
      const [created] = await db
        .insert(users)
        .values({
          supabaseAuthId: authCreated.user.id,
          role: payload.role,
          email: payload.email,
          firstName: payload.firstName,
          lastName: payload.lastName,
          phone: payload.phone,
          address: payload.address,
          city: payload.city,
          postalCode: payload.postalCode,
          region: payload.region,
          ipp: payload.ipp != null ? payload.ipp.toFixed(1) : null,
        })
        .returning();

      if (!created) {
        throw new Error("Failed to create user row.");
      }

      await db.insert(authAuditLogs).values({
        actorUserId: req.authUser?.appUserId,
        targetUserId: created.id,
        eventType: "user_created",
        details: `role=${created.role}`,
      });

      res.status(201).json({
        user: {
          id: created.id,
          role: created.role,
          email: created.email,
          firstName: created.firstName,
          lastName: created.lastName,
          phone: created.phone,
          address: created.address,
          city: created.city,
          postalCode: created.postalCode,
          region: created.region,
          ipp: created.ipp == null ? null : Number(created.ipp),
          isActive: created.isActive,
          deletedAt: created.deletedAt,
          createdAt: created.createdAt,
          updatedAt: created.updatedAt,
        },
        oneTimePassword: password,
      });
    } catch (dbError) {
      await supabaseAdmin.auth.admin.deleteUser(authCreated.user.id);
      throw dbError;
    }
  } catch (err) {
    next(err);
  }
});

adminUsersRouter.patch("/:id", async (req: AuthedRequest, res, next) => {
  try {
    const parsed = updateUserSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid update payload." });
      return;
    }

    const id = String(req.params.id ?? "");
    if (!id) {
      res.status(400).json({ error: "User id is required." });
      return;
    }

    const payload = parsed.data;
    const [existing] = await db.select().from(users).where(eq(users.id, id)).limit(1);
    if (!existing) {
      res.status(404).json({ error: "User not found." });
      return;
    }

    const [updated] = await db
      .update(users)
      .set({
        firstName: payload.firstName,
        lastName: payload.lastName,
        email: payload.email,
        phone: payload.phone,
        address: payload.address,
        city: payload.city,
        postalCode: payload.postalCode,
        region: payload.region,
        ipp: payload.ipp != null ? payload.ipp.toFixed(1) : undefined,
        updatedAt: new Date(),
      })
      .where(eq(users.id, id))
      .returning();

    if (!updated) {
      res.status(404).json({ error: "User not found after update." });
      return;
    }

    if (payload.email && payload.email !== existing.email) {
      await supabaseAdmin.auth.admin.updateUserById(updated.supabaseAuthId, {
        email: payload.email,
      });
    }

    await db.insert(authAuditLogs).values({
      actorUserId: req.authUser?.appUserId,
      targetUserId: updated.id,
      eventType: "user_updated",
      details: null,
    });

    res.status(200).json({
      user: {
        ...updated,
        ipp: updated.ipp == null ? null : Number(updated.ipp),
      },
    });
  } catch (err) {
    next(err);
  }
});

adminUsersRouter.patch("/:id/deactivate", async (req: AuthedRequest, res, next) => {
  try {
    const id = String(req.params.id ?? "");
    if (!id) {
      res.status(400).json({ error: "User id is required." });
      return;
    }

    const [target] = await db.select().from(users).where(eq(users.id, id)).limit(1);
    if (!target) {
      res.status(404).json({ error: "User not found." });
      return;
    }

    const [updated] = await db
      .update(users)
      .set({
        isActive: false,
        deletedAt: new Date(),
        updatedAt: new Date(),
      })
      .where(and(eq(users.id, id), eq(users.isActive, true)))
      .returning();

    if (!updated) {
      res.status(200).json({ ok: true, alreadyInactive: true });
      return;
    }

    await supabaseAdmin.auth.admin.updateUserById(target.supabaseAuthId, {
      user_metadata: {
        disabled: true,
      },
    });

    await db.insert(authAuditLogs).values({
      actorUserId: req.authUser?.appUserId,
      targetUserId: target.id,
      eventType: "user_deactivated",
      details: null,
    });

    res.status(200).json({ ok: true });
  } catch (err) {
    next(err);
  }
});

export { adminUsersRouter };
