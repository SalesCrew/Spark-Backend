import { and, eq, isNull } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import { getKundePermissionsForUser } from "../lib/kunde-access.js";
import { getRequestLogMeta, logAction, logger, markErrorAsLogged, serializeError, startActionTimer } from "../lib/logger.js";
import { authAuditLogs, users } from "../lib/schema.js";
import { supabaseAdmin, supabaseAnon } from "../lib/supabase.js";
import { requireAuth, type AuthedRequest } from "../middleware/auth.js";

const loginSchema = z.object({
  email: z.string().email(),
  password: z.string().min(1),
  role: z.enum(["gm", "sm", "admin", "kunde"]).optional(),
});

const refreshSchema = z.object({
  refreshToken: z.string().min(1),
});

const updatePasswordSchema = z.object({
  currentPassword: z.string().min(1),
  newPassword: z.string().min(8).max(128),
});

const authRouter = Router();

async function buildAuthUserPayload(appUser: typeof users.$inferSelect) {
  return {
    id: appUser.id,
    role: appUser.role,
    email: appUser.email,
    firstName: appUser.firstName,
    lastName: appUser.lastName,
    permissions: appUser.role === "kunde" ? await getKundePermissionsForUser(appUser.id) : null,
  };
}

authRouter.get("/me", requireAuth(["admin", "gm", "sm", "kunde"]), async (req: AuthedRequest, res, next) => {
  try {
    if (!req.authUser) {
      res.status(401).json({ error: "Authentication required." });
      return;
    }
    const [appUser] = await db.select().from(users).where(eq(users.id, req.authUser.appUserId)).limit(1);
    if (!appUser || !appUser.isActive || appUser.deletedAt) {
      res.status(403).json({ error: "User account is inactive." });
      return;
    }
    res.status(200).json({ user: await buildAuthUserPayload(appUser) });
  } catch (err) {
    next(err);
  }
});

authRouter.patch("/password", requireAuth(["admin", "gm", "sm", "kunde"]), async (req: AuthedRequest, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    if (!req.authUser) {
      res.status(401).json({ error: "Authentication required.", code: "auth_required" });
      return;
    }

    const parsed = updatePasswordSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "auth_password_invalid_payload", {
        req,
        action: "auth_password_update",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(400).json({ error: "Passwort muss mindestens 8 Zeichen enthalten.", code: "invalid_password_payload" });
      return;
    }

    const [appUser] = await db
      .select()
      .from(users)
      .where(and(eq(users.id, req.authUser.appUserId), isNull(users.deletedAt)))
      .limit(1);
    if (!appUser || !appUser.isActive) {
      res.status(403).json({ error: "User account is inactive.", code: "account_inactive" });
      return;
    }

    const { currentPassword, newPassword } = parsed.data;
    const { data: currentLogin, error: currentLoginError } = await supabaseAnon.auth.signInWithPassword({
      email: appUser.email,
      password: currentPassword,
    });
    if (currentLoginError || currentLogin.user?.id !== appUser.supabaseAuthId) {
      logAction("warn", "auth_password_current_invalid", {
        req,
        action: "auth_password_update",
        result: "rejected",
        statusCode: 401,
        requestClass: "client_error",
        startedAtNs,
        details: { targetUserId: appUser.id },
      });
      res.status(401).json({ error: "Aktuelles Passwort ist nicht korrekt.", code: "current_password_invalid" });
      return;
    }

    const { error: updateError } = await supabaseAdmin.auth.admin.updateUserById(appUser.supabaseAuthId, {
      password: newPassword,
    });
    if (updateError) {
      logAction("warn", "auth_password_supabase_failed", {
        req,
        action: "auth_password_update",
        result: "failure",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
        details: { reason: updateError.message },
      });
      res.status(400).json({ error: updateError.message || "Passwort konnte nicht aktualisiert werden.", code: "password_update_failed" });
      return;
    }

    await db
      .update(users)
      .set({ updatedAt: new Date() })
      .where(eq(users.id, appUser.id));

    await db.insert(authAuditLogs).values({
      actorUserId: appUser.id,
      targetUserId: appUser.id,
      eventType: "password_updated",
      details: "scope=self_verified",
    });

    res.status(200).json({ ok: true });
    logAction("info", "auth_password_success", {
      req,
      action: "auth_password_update",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: { targetUserId: appUser.id },
    });
  } catch (err) {
    logAction("error", "auth_password_failed", {
      req,
      action: "auth_password_update",
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

authRouter.post("/login", async (req, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const parsed = loginSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "auth_login_invalid_payload", {
        req,
        action: "auth_login",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
        details: {
          issues: parsed.error.issues.map((issue) => ({
            code: issue.code,
            path: issue.path.join("."),
          })),
        },
      });
      res.status(400).json({ error: "Invalid login payload." });
      return;
    }

    const { email, password, role } = parsed.data;

    const { data, error } = await supabaseAnon.auth.signInWithPassword({
      email,
      password,
    });

    if (error || !data.user || !data.session) {
      logAction("warn", "auth_login_invalid_credentials", {
        req,
        action: "auth_login",
        result: "rejected",
        statusCode: 401,
        requestClass: "client_error",
        startedAtNs,
        details: { role },
      });
      res.status(401).json({ error: "Invalid credentials." });
      return;
    }

    const [appUser] = await db
      .select()
      .from(users)
      .where(and(eq(users.supabaseAuthId, data.user.id), isNull(users.deletedAt)))
      .limit(1);

    if (!appUser || !appUser.isActive) {
      await supabaseAnon.auth.signOut();
      logAction("warn", "auth_login_user_inactive", {
        req,
        action: "auth_login",
        result: "rejected",
        statusCode: 403,
        requestClass: "client_error",
        startedAtNs,
        details: { role },
      });
      res.status(403).json({ error: "User account is inactive." });
      return;
    }

    if (role && appUser.role !== role) {
      await supabaseAnon.auth.signOut();
      logAction("warn", "auth_login_role_mismatch", {
        req,
        action: "auth_login",
        result: "rejected",
        statusCode: 403,
        requestClass: "client_error",
        startedAtNs,
        details: {
          expectedRole: role,
          actualRole: appUser.role,
        },
      });
      res.status(403).json({ error: "Role mismatch for this account." });
      return;
    }

    await db.insert(authAuditLogs).values({
      actorUserId: appUser.id,
      targetUserId: appUser.id,
      eventType: "login_success",
      details: `role=${appUser.role}`,
    });

    res.status(200).json({
      user: await buildAuthUserPayload(appUser),
      session: {
        accessToken: data.session.access_token,
        refreshToken: data.session.refresh_token,
        expiresAt: data.session.expires_at,
      },
    });
    logAction("info", "auth_login_success", {
      req,
      action: "auth_login",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: {
        appUserId: appUser.id,
        role: appUser.role,
      },
    });
  } catch (err) {
    logAction("error", "auth_login_failed", {
      req,
      action: "auth_login",
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

authRouter.post("/refresh", async (req, res, next) => {
  const startedAtNs = startActionTimer();
  try {
    const parsed = refreshSchema.safeParse(req.body);
    if (!parsed.success) {
      logAction("warn", "auth_refresh_invalid_payload", {
        req,
        action: "auth_refresh",
        result: "rejected",
        statusCode: 400,
        requestClass: "client_error",
        startedAtNs,
        details: {
          issues: parsed.error.issues.map((issue) => ({
            code: issue.code,
            path: issue.path.join("."),
          })),
        },
      });
      res.status(400).json({ error: "Invalid refresh payload." });
      return;
    }

    const { refreshToken } = parsed.data;
    const { data, error } = await supabaseAnon.auth.refreshSession({
      refresh_token: refreshToken,
    });

    if (error || !data.user || !data.session) {
      logAction("warn", "auth_refresh_invalid_token", {
        req,
        action: "auth_refresh",
        result: "rejected",
        statusCode: 401,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(401).json({ error: "Refresh token invalid or expired." });
      return;
    }

    const [appUser] = await db
      .select()
      .from(users)
      .where(and(eq(users.supabaseAuthId, data.user.id), isNull(users.deletedAt)))
      .limit(1);

    if (!appUser || !appUser.isActive) {
      await supabaseAnon.auth.signOut();
      logAction("warn", "auth_refresh_user_inactive", {
        req,
        action: "auth_refresh",
        result: "rejected",
        statusCode: 403,
        requestClass: "client_error",
        startedAtNs,
      });
      res.status(403).json({ error: "User account is inactive." });
      return;
    }

    res.status(200).json({
      user: await buildAuthUserPayload(appUser),
      session: {
        accessToken: data.session.access_token,
        refreshToken: data.session.refresh_token,
        expiresAt: data.session.expires_at,
      },
    });
    logAction("info", "auth_refresh_success", {
      req,
      action: "auth_refresh",
      result: "success",
      statusCode: 200,
      requestClass: "success",
      startedAtNs,
      details: {
        appUserId: appUser.id,
        role: appUser.role,
      },
    });
  } catch (err) {
    logAction("error", "auth_refresh_failed", {
      req,
      action: "auth_refresh",
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

export { authRouter };
