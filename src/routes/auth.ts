import { and, eq, isNull } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
import { getRequestLogMeta, logAction, logger, markErrorAsLogged, serializeError, startActionTimer } from "../lib/logger.js";
import { authAuditLogs, users } from "../lib/schema.js";
import { supabaseAnon } from "../lib/supabase.js";

const loginSchema = z.object({
  email: z.string().email(),
  password: z.string().min(1),
  role: z.enum(["gm", "sm", "admin", "coke"]),
});

const refreshSchema = z.object({
  refreshToken: z.string().min(1),
});

const authRouter = Router();

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

    if (role === "coke") {
      logAction("warn", "auth_login_role_not_enabled", {
        req,
        action: "auth_login",
        result: "rejected",
        statusCode: 501,
        requestClass: "client_error",
        startedAtNs,
        details: { role },
      });
      res.status(501).json({ error: "Coke login is not enabled yet." });
      return;
    }

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

    if (appUser.role !== role) {
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
      details: `role=${role}`,
    });

    res.status(200).json({
      user: {
        id: appUser.id,
        role: appUser.role,
        email: appUser.email,
        firstName: appUser.firstName,
        lastName: appUser.lastName,
      },
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
      user: {
        id: appUser.id,
        role: appUser.role,
        email: appUser.email,
        firstName: appUser.firstName,
        lastName: appUser.lastName,
      },
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
