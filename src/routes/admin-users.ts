import { and, desc, eq } from "drizzle-orm";
import type { NextFunction, Response } from "express";
import { Router } from "express";
import { z } from "zod";
import { aggregateHighVolumeLoad, logAction, markErrorAsLogged, startActionTimer } from "../lib/logger.js";
import { getKundePermissionsForUser, hasKundePermission } from "../lib/kunde-access.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";
import { db } from "../lib/db.js";
import { recomputeGmKpiCache } from "../lib/gm-kpi-cache.js";
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
  isBillaGm: z.boolean().optional(),
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
  isBillaGm: z.boolean().optional(),
});

const updateOwnPasswordSchema = z.object({
  newPassword: z.string().min(8).max(128),
});

const adminUsersRouter = Router();

async function requireAdminUsersAccess(req: AuthedRequest, res: Response, next: NextFunction) {
  if (!req.authUser) {
    res.status(401).json({ error: "Authentication required.", code: "auth_required" });
    return;
  }
  if (req.authUser.role === "admin") {
    next();
    return;
  }
  if (req.authUser.role !== "kunde") {
    res.status(403).json({ error: "Role is not allowed for this endpoint.", code: "role_not_allowed" });
    return;
  }

  const method = req.method.toUpperCase();
  if (method === "GET") {
    next();
    return;
  }
  if (req.path.endsWith("/password")) {
    res.status(403).json({ error: "Password update is currently admin-only.", code: "role_not_allowed" });
    return;
  }

  const action = method === "POST" ? "write" : "update";
  const permissions = await getKundePermissionsForUser(req.authUser.appUserId);
  if (hasKundePermission(permissions, "gebietsmanager", action)) {
    next();
    return;
  }
  res.status(403).json({
    error: "Für diese Aktion ist kein Kundenzugriff freigeschaltet.",
    code: "kunde_permission_denied",
    pageKey: "gebietsmanager",
    action,
  });
}

adminUsersRouter.use(requireAuth(["admin", "kunde"]));
adminUsersRouter.use(requireAdminUsersAccess);
adminUsersRouter.use((req, res, next) => {
  if (req.method.toUpperCase() === "GET") {
    next();
    return;
  }
  const startedAtNs = startActionTimer();
  res.on("finish", () => {
    const level = res.statusCode >= 500 ? "error" : res.statusCode >= 400 ? "warn" : "info";
    logAction(level, "admin_users_action_completed", {
      req,
      action: "admin_users_mutation",
      result: res.statusCode >= 400 ? "failure" : "success",
      statusCode: res.statusCode,
      requestClass: res.statusCode >= 500 ? "server_error" : res.statusCode >= 400 ? "client_error" : "success",
      startedAtNs,
      details: { route: req.path, method: req.method },
    });
  });
  next();
});

adminUsersRouter.get("/", async (req: AuthedRequest, res, next) => {
  try {
    const roleParam = req.query.role;
    const requestedRole = roleParam === "admin" || roleParam === "gm" || roleParam === "sm" ? roleParam : undefined;
    if (req.authUser?.role === "kunde" && requestedRole && requestedRole !== "gm") {
      res.status(200).json({ users: [] });
      return;
    }
    const role = req.authUser?.role === "kunde" ? "gm" : requestedRole;
    const whereClause = role ? eq(users.role, role) : undefined;

    const rows = await db
      .select()
      .from(users)
      .where(whereClause)
      .orderBy(desc(users.createdAt));
    const gmRows = rows.filter((row) => row.role === "gm");
    const gmKpiByUserId = new Map<string, { ipp: number; ippSampleCount: number }>(
      await Promise.all(
        gmRows.map(async (row): Promise<[string, { ipp: number; ippSampleCount: number }]> => {
          try {
            const summary = await recomputeGmKpiCache(row.id);
            return [
              row.id,
              {
                ipp: summary.ippAllTimeAvg,
                ippSampleCount: summary.ippSampleCount,
              },
            ];
          } catch {
            return [row.id, { ipp: 0, ippSampleCount: 0 }];
          }
        }),
      ),
    );
    const softDeletedCount = rows.filter((row) => row.deletedAt != null).length;
    aggregateHighVolumeLoad({
      metric: "users",
      scope: role ?? "all",
      loaded: rows.length,
      softDeleted: softDeletedCount,
      requestId: (req as { requestId?: string }).requestId ?? null,
      appUserId: req.authUser?.appUserId ?? null,
      role: req.authUser?.role ?? null,
    });

    res.status(200).json({
      users: rows.map((row) => ({
        ...(row.role === "gm" ? { ipp: Number(gmKpiByUserId.get(row.id)?.ipp ?? 0), ippSampleCount: Number(gmKpiByUserId.get(row.id)?.ippSampleCount ?? 0) } : {}),
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
        isBillaGm: row.role === "gm" ? row.isBillaGm : false,
        ...(row.role !== "gm" ? { ipp: row.ipp == null ? null : Number(row.ipp), ippSampleCount: null } : {}),
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
  const startedAtNs = startActionTimer();
  try {
    const parsed = createUserSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "admin_user_create_invalid_payload", {
        req,
        action: "admin_user_create",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Invalid create user payload." });
      return;
    }

    const payload = parsed.data;
    if (req.authUser?.role === "kunde" && payload.role !== "gm") {
      res.status(403).json({ error: "Kundenzugaenge duerfen nur GM-Benutzer verwalten.", code: "kunde_permission_denied" });
      return;
    }
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
      logAction("warn", "admin_user_create_auth_failed", {
        req,
        action: "admin_user_create",
        result: "failure",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
        details: { reason: authError?.message ?? "create_auth_user_failed" },
      });
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
          isBillaGm: payload.role === "gm" ? Boolean(payload.isBillaGm ?? false) : false,
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
      const gmKpi =
        created.role === "gm"
          ? await recomputeGmKpiCache(created.id)
              .then((summary) => ({
                ipp: summary.ippAllTimeAvg,
                ippSampleCount: summary.ippSampleCount,
              }))
              .catch(() => ({ ipp: 0, ippSampleCount: 0 }))
          : null;

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
          isBillaGm: created.role === "gm" ? created.isBillaGm : false,
          ipp: created.role === "gm" ? gmKpi?.ipp ?? 0 : created.ipp == null ? null : Number(created.ipp),
          ippSampleCount: created.role === "gm" ? gmKpi?.ippSampleCount ?? 0 : null,
          isActive: created.isActive,
          deletedAt: created.deletedAt,
          createdAt: created.createdAt,
          updatedAt: created.updatedAt,
        },
        oneTimePassword: password,
      });
      logAction("info", "admin_user_create_success", {
        req,
        action: "admin_user_create",
        result: "success",
        statusCode: 201,
        requestClass: "success",
        startedAtNs,
        details: { targetUserId: created.id, role: created.role },
      });
    } catch (dbError) {
      await supabaseAdmin.auth.admin.deleteUser(authCreated.user.id);
      logAction("error", "admin_user_create_rollback", {
        req,
        action: "admin_user_create",
        result: "failure",
        statusCode: 500,
        requestClass: "server_error",
        startedAtNs,
        error: dbError,
      });
      markErrorAsLogged(dbError);
      throw dbError;
    }
  } catch (err) {
    logAction("error", "admin_user_create_failed", {
      req,
      action: "admin_user_create",
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

adminUsersRouter.patch("/:id", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const parsed = updateUserSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "admin_user_update_invalid_payload", {
        req,
        action: "admin_user_update",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Invalid update payload." });
      return;
    }

    const id = String(req.params.id ?? "");
    if (!id) {
      logAction("warn", "admin_user_update_invalid_id", {
        req,
        action: "admin_user_update",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "User id is required." });
      return;
    }

    const payload = parsed.data;
    const [existing] = await db.select().from(users).where(eq(users.id, id)).limit(1);
    if (!existing) {
      logAction("warn", "admin_user_update_not_found", {
        req,
        action: "admin_user_update",
        result: "rejected",
        statusCode: 404,
        requestClass: "client_error",
        startedAtNs,
        details: { targetUserId: id },
      });
      res.status(404).json({ error: "User not found." });
      return;
    }
    if (req.authUser?.role === "kunde" && existing.role !== "gm") {
      res.status(403).json({ error: "Kundenzugaenge duerfen nur GM-Benutzer verwalten.", code: "kunde_permission_denied" });
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
        isBillaGm: existing.role === "gm" ? payload.isBillaGm : undefined,
        updatedAt: new Date(),
      })
      .where(eq(users.id, id))
      .returning();

    if (!updated) {
      logAction("warn", "admin_user_update_not_found_after_update", {
        req,
        action: "admin_user_update",
        result: "failure",
        statusCode: 404,
        requestClass: "client_error",
        startedAtNs,
        details: { targetUserId: id },
      });
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
    const gmKpi =
      updated.role === "gm"
        ? await recomputeGmKpiCache(updated.id)
            .then((summary) => ({
              ipp: summary.ippAllTimeAvg,
              ippSampleCount: summary.ippSampleCount,
            }))
            .catch(() => ({ ipp: 0, ippSampleCount: 0 }))
        : null;

    res.status(200).json({
      user: {
        ...updated,
        isBillaGm: updated.role === "gm" ? updated.isBillaGm : false,
        ipp: updated.role === "gm" ? gmKpi?.ipp ?? 0 : updated.ipp == null ? null : Number(updated.ipp),
        ippSampleCount: updated.role === "gm" ? gmKpi?.ippSampleCount ?? 0 : null,
      },
    });
    logAction("info", "admin_user_update_success", {
      req,
      action: "admin_user_update",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { targetUserId: updated.id },
    });
  } catch (err) {
    logAction("error", "admin_user_update_failed", {
      req,
      action: "admin_user_update",
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

adminUsersRouter.patch("/:id/password", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const id = String(req.params.id ?? "");
    if (!id) {
      logAction("warn", "admin_user_password_invalid_id", {
        req,
        action: "admin_user_password_update",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "User id is required." });
      return;
    }

    if (!req.authUser || req.authUser.role !== "admin") {
      logAction("warn", "admin_user_password_missing_auth", {
        req,
        action: "admin_user_password_update",
        result: "rejected",
        statusCode: 401,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(401).json({ error: "Authentication required." });
      return;
    }

    if (req.authUser.appUserId !== id) {
      logAction("warn", "admin_user_password_forbidden_target", {
        req,
        action: "admin_user_password_update",
        result: "rejected",
        statusCode: 403,
        requestClass: "client_error",
        startedAtNs,
        details: { targetUserId: id, actorUserId: req.authUser.appUserId },
      });
      res.status(403).json({ error: "You can only update your own password." });
      return;
    }

    const parsed = updateOwnPasswordSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "admin_user_password_invalid_payload", {
        req,
        action: "admin_user_password_update",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Invalid password payload." });
      return;
    }

    const [target] = await db.select().from(users).where(eq(users.id, id)).limit(1);
    if (!target) {
      logAction("warn", "admin_user_password_target_not_found", {
        req,
        action: "admin_user_password_update",
        result: "rejected",
        statusCode: 404,
        requestClass: "client_error",
        startedAtNs,
        details: { targetUserId: id },
      });
      res.status(404).json({ error: "User not found." });
      return;
    }

    if (target.role !== "admin") {
      logAction("warn", "admin_user_password_role_not_allowed", {
        req,
        action: "admin_user_password_update",
        result: "rejected",
        statusCode: 403,
        requestClass: "client_error",
        startedAtNs,
        details: { targetUserId: id, targetRole: target.role },
      });
      res.status(403).json({ error: "Password update is currently admin-only." });
      return;
    }

    const { newPassword } = parsed.data;
    const { error: authError } = await supabaseAdmin.auth.admin.updateUserById(target.supabaseAuthId, {
      password: newPassword,
    });
    if (authError) {
      logAction("warn", "admin_user_password_supabase_failed", {
        req,
        action: "admin_user_password_update",
        result: "failure",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
        details: { reason: authError.message },
      });
      res.status(400).json({ error: authError.message || "Password update failed." });
      return;
    }

    await db
      .update(users)
      .set({
        updatedAt: new Date(),
      })
      .where(eq(users.id, id));

    await db.insert(authAuditLogs).values({
      actorUserId: req.authUser.appUserId,
      targetUserId: id,
      eventType: "password_updated",
      details: "scope=self",
    });

    res.status(200).json({ ok: true });
    logAction("info", "admin_user_password_success", {
      req,
      action: "admin_user_password_update",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { targetUserId: id },
    });
  } catch (err) {
    logAction("error", "admin_user_password_failed", {
      req,
      action: "admin_user_password_update",
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

adminUsersRouter.patch("/:id/deactivate", async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const id = String(req.params.id ?? "");
    if (!id) {
      logAction("warn", "admin_user_deactivate_invalid_id", {
        req,
        action: "admin_user_deactivate",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "User id is required." });
      return;
    }

    const [target] = await db.select().from(users).where(eq(users.id, id)).limit(1);
    if (!target) {
      logAction("warn", "admin_user_deactivate_not_found", {
        req,
        action: "admin_user_deactivate",
        result: "rejected",
        statusCode: 404,
        requestClass: "client_error",
        startedAtNs,
        details: { targetUserId: id },
      });
      res.status(404).json({ error: "User not found." });
      return;
    }
    if (req.authUser?.role === "kunde" && target.role !== "gm") {
      res.status(403).json({ error: "Kundenzugaenge duerfen nur GM-Benutzer verwalten.", code: "kunde_permission_denied" });
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
      logAction("info", "admin_user_deactivate_already_inactive", {
        req,
        action: "admin_user_deactivate",
        result: "success",
        statusCode: 200,
        requestClass: "success",
        startedAtNs,
        details: { targetUserId: id, alreadyInactive: true },
      });
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
    logAction("info", "admin_user_deactivate_success", {
      req,
      action: "admin_user_deactivate",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { targetUserId: target.id },
    });
  } catch (err) {
    logAction("error", "admin_user_deactivate_failed", {
      req,
      action: "admin_user_deactivate",
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

export { adminUsersRouter };
