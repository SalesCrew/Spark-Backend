import { and, desc, eq } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { normalizeKundePagePermissions } from "../lib/kunde-access.js";
import { logAction, markErrorAsLogged, startActionTimer } from "../lib/logger.js";
import { authAuditLogs, kundeUsers, users, type KundePagePermissions } from "../lib/schema.js";
import { supabaseAdmin } from "../lib/supabase.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { generatePassword } from "../services/password.js";
import { db } from "../lib/db.js";

const permissionActionSchema = z.enum(["read", "write", "update"]);
const pagePermissionsSchema = z.record(z.string(), z.array(permissionActionSchema)).transform((value) => normalizeKundePagePermissions(value));

const createKundeUserSchema = z.object({
  firstName: z.string().min(1),
  lastName: z.string().min(1),
  email: z.string().email(),
  isActive: z.boolean().optional().default(true),
  permissions: pagePermissionsSchema.optional().default({}),
});

const updateKundeUserSchema = z.object({
  firstName: z.string().min(1).optional(),
  lastName: z.string().min(1).optional(),
  email: z.string().email().optional(),
  isActive: z.boolean().optional(),
  permissions: pagePermissionsSchema.optional(),
});

const kundenUsersRouter = Router();

function serializeKundeUser(row: {
  id: string;
  supabaseAuthId: string;
  email: string;
  firstName: string;
  lastName: string;
  isActive: boolean;
  deletedAt: Date | null;
  createdAt: Date;
  updatedAt: Date;
  pagePermissions: KundePagePermissions | null;
  accessCreatedAt: Date | null;
  accessUpdatedAt: Date | null;
}) {
  return {
    id: row.id,
    supabaseAuthId: row.supabaseAuthId,
    role: "kunde" as const,
    email: row.email,
    firstName: row.firstName,
    lastName: row.lastName,
    isActive: row.isActive,
    deletedAt: row.deletedAt,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    permissions: normalizeKundePagePermissions(row.pagePermissions),
    accessCreatedAt: row.accessCreatedAt,
    accessUpdatedAt: row.accessUpdatedAt,
  };
}

async function readKundeUserRows() {
  return db
    .select({
      id: users.id,
      supabaseAuthId: users.supabaseAuthId,
      email: users.email,
      firstName: users.firstName,
      lastName: users.lastName,
      isActive: users.isActive,
      deletedAt: users.deletedAt,
      createdAt: users.createdAt,
      updatedAt: users.updatedAt,
      pagePermissions: kundeUsers.pagePermissions,
      accessCreatedAt: kundeUsers.createdAt,
      accessUpdatedAt: kundeUsers.updatedAt,
    })
    .from(users)
    .leftJoin(kundeUsers, eq(kundeUsers.userId, users.id))
    .where(eq(users.role, "kunde"))
    .orderBy(desc(users.updatedAt));
}

kundenUsersRouter.get("/me", requireAuth(["admin", "kunde"]), async (req: AuthedRequest, res, next) => {
  try {
    if (!req.authUser) {
      res.status(401).json({ error: "Authentication required.", code: "auth_required" });
      return;
    }
    if (req.authUser.role === "admin") {
      res.status(200).json({ permissions: {} });
      return;
    }
    const [row] = await db
      .select({ pagePermissions: kundeUsers.pagePermissions })
      .from(kundeUsers)
      .where(and(eq(kundeUsers.userId, req.authUser.appUserId), eq(kundeUsers.isDeleted, false)))
      .limit(1);
    res.status(200).json({ permissions: normalizeKundePagePermissions(row?.pagePermissions) });
  } catch (err) {
    next(err);
  }
});

kundenUsersRouter.use(requireAuth(["admin"]));

kundenUsersRouter.use((req, res, next) => {
  if (req.method.toUpperCase() === "GET") {
    next();
    return;
  }
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "admin_kunden_users_action_completed", {
      req,
      action: "admin_kunden_users_mutation",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

kundenUsersRouter.get("/", async (_req, res, next) => {
  try {
    const rows = await readKundeUserRows();
    res.status(200).json({
      users: rows.map((row) => serializeKundeUser(row)),
    });
  } catch (err) {
    next(err);
  }
});

kundenUsersRouter.post("/", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const parsed = createKundeUserSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid create customer user payload." });
      return;
    }

    const payload = parsed.data;
    const password = generatePassword();
    const { data: authCreated, error: authError } = await supabaseAdmin.auth.admin.createUser({
      email: payload.email,
      password,
      email_confirm: true,
      user_metadata: {
        role: "kunde",
        firstName: payload.firstName,
        lastName: payload.lastName,
      },
    });

    if (authError || !authCreated.user) {
      res.status(400).json({ error: authError?.message ?? "Failed to create auth user." });
      return;
    }

    try {
      const created = await db.transaction(async (tx) => {
        const [createdUser] = await tx
          .insert(users)
          .values({
            supabaseAuthId: authCreated.user.id,
            role: "kunde",
            email: payload.email,
            firstName: payload.firstName,
            lastName: payload.lastName,
            isActive: payload.isActive,
            deletedAt: payload.isActive ? null : new Date(),
          })
          .returning();

        if (!createdUser) {
          throw new Error("Failed to create Kunde user row.");
        }

        const [createdAccess] = await tx
          .insert(kundeUsers)
          .values({
            userId: createdUser.id,
            pagePermissions: payload.permissions,
            createdByUserId: req.authUser?.appUserId ?? null,
          })
          .returning();

        if (!createdAccess) {
          throw new Error("Failed to create Kunde access row.");
        }

        await tx.insert(authAuditLogs).values({
          actorUserId: req.authUser?.appUserId,
          targetUserId: createdUser.id,
          eventType: "user_created",
          details: "role=kunde",
        });

        return { user: createdUser, access: createdAccess };
      });

      if (!payload.isActive) {
        await supabaseAdmin.auth.admin.updateUserById(authCreated.user.id, {
          user_metadata: { role: "kunde", disabled: true },
        });
      }

      res.status(201).json({
        user: serializeKundeUser({
          ...created.user,
          pagePermissions: created.access.pagePermissions,
          accessCreatedAt: created.access.createdAt,
          accessUpdatedAt: created.access.updatedAt,
        }),
        oneTimePassword: password,
      });
      logAction("info", "admin_kunde_user_create_success", {
        req,
        action: "admin_kunde_user_create",
        result: "success",
        statusCode: 201,
        requestClass: "success",
        startedAtNs,
        details: { targetUserId: created.user.id },
      });
    } catch (dbError) {
      await supabaseAdmin.auth.admin.deleteUser(authCreated.user.id);
      markErrorAsLogged(dbError);
      throw dbError;
    }
  } catch (err) {
    logAction("error", "admin_kunde_user_create_failed", {
      req,
      action: "admin_kunde_user_create",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error: err,
    });
    markErrorAsLogged(err);
    next(err);
  }
});

kundenUsersRouter.patch("/:id", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const id = String(req.params.id ?? "");
    const parsed = updateKundeUserSchema.safeParse(req.body);
    if (!id || !parsed.success) {
      res.status(400).json({ error: "Invalid update customer user payload." });
      return;
    }

    const payload = parsed.data;
    const [existing] = await db.select().from(users).where(and(eq(users.id, id), eq(users.role, "kunde"))).limit(1);
    if (!existing) {
      res.status(404).json({ error: "Kunde user not found." });
      return;
    }

    const nextActive = payload.isActive ?? existing.isActive;
    const updated = await db.transaction(async (tx) => {
      const [updatedUser] = await tx
        .update(users)
        .set({
          firstName: payload.firstName,
          lastName: payload.lastName,
          email: payload.email,
          isActive: payload.isActive,
          deletedAt: payload.isActive == null ? existing.deletedAt : payload.isActive ? null : new Date(),
          updatedAt: new Date(),
        })
        .where(eq(users.id, id))
        .returning();

      if (!updatedUser) {
        throw new Error("Failed to update Kunde user row.");
      }

      const [updatedAccess] = await tx
        .insert(kundeUsers)
        .values({
          userId: id,
          pagePermissions: payload.permissions ?? {},
          createdByUserId: req.authUser?.appUserId ?? null,
        })
        .onConflictDoUpdate({
          target: kundeUsers.userId,
          set: {
            ...(payload.permissions ? { pagePermissions: payload.permissions } : {}),
            isDeleted: false,
            deletedAt: null,
            updatedAt: new Date(),
          },
        })
        .returning();

      await tx.insert(authAuditLogs).values({
        actorUserId: req.authUser?.appUserId,
        targetUserId: id,
        eventType: "user_updated",
        details: "role=kunde",
      });

      return { user: updatedUser, access: updatedAccess };
    });

    const authUpdates: Parameters<typeof supabaseAdmin.auth.admin.updateUserById>[1] = {
      user_metadata: {
        role: "kunde",
        firstName: updated.user.firstName,
        lastName: updated.user.lastName,
        disabled: !nextActive,
      },
    };
    if (payload.email && payload.email !== existing.email) {
      authUpdates.email = payload.email;
    }
    await supabaseAdmin.auth.admin.updateUserById(existing.supabaseAuthId, authUpdates);

    res.status(200).json({
      user: serializeKundeUser({
        ...updated.user,
        pagePermissions: updated.access?.pagePermissions ?? {},
        accessCreatedAt: updated.access?.createdAt ?? null,
        accessUpdatedAt: updated.access?.updatedAt ?? null,
      }),
    });
    logAction("info", "admin_kunde_user_update_success", {
      req,
      action: "admin_kunde_user_update",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { targetUserId: id },
    });
  } catch (err) {
    logAction("error", "admin_kunde_user_update_failed", {
      req,
      action: "admin_kunde_user_update",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error: err,
    });
    markErrorAsLogged(err);
    next(err);
  }
});

kundenUsersRouter.patch("/:id/deactivate", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const id = String(req.params.id ?? "");
    if (!id) {
      res.status(400).json({ error: "User id is required." });
      return;
    }
    const [target] = await db.select().from(users).where(and(eq(users.id, id), eq(users.role, "kunde"))).limit(1);
    if (!target) {
      res.status(404).json({ error: "Kunde user not found." });
      return;
    }
    await db
      .update(users)
      .set({ isActive: false, deletedAt: new Date(), updatedAt: new Date() })
      .where(eq(users.id, id));
    await supabaseAdmin.auth.admin.updateUserById(target.supabaseAuthId, {
      user_metadata: { role: "kunde", disabled: true },
    });
    await db.insert(authAuditLogs).values({
      actorUserId: req.authUser?.appUserId,
      targetUserId: id,
      eventType: "user_deactivated",
      details: "role=kunde",
    });
    res.status(200).json({ ok: true });
    logAction("info", "admin_kunde_user_deactivate_success", {
      req,
      action: "admin_kunde_user_deactivate",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { targetUserId: id },
    });
  } catch (err) {
    logAction("error", "admin_kunde_user_deactivate_failed", {
      req,
      action: "admin_kunde_user_deactivate",
      result: "failure",
      statusCode: 500,
      requestClass: "server_error",
      startedAtNs,
      error: err,
    });
    markErrorAsLogged(err);
    next(err);
  }
});

export { kundenUsersRouter };
