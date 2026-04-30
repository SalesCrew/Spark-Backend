import { and, eq, isNull } from "drizzle-orm";
import { Router } from "express";
import { z } from "zod";
import { db } from "../lib/db.js";
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
  try {
    const parsed = loginSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid login payload." });
      return;
    }

    const { email, password, role } = parsed.data;

    if (role === "coke") {
      res.status(501).json({ error: "Coke login is not enabled yet." });
      return;
    }

    const { data, error } = await supabaseAnon.auth.signInWithPassword({
      email,
      password,
    });

    if (error || !data.user || !data.session) {
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
      res.status(403).json({ error: "User account is inactive." });
      return;
    }

    if (appUser.role !== role) {
      await supabaseAnon.auth.signOut();
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
  } catch (err) {
    next(err);
  }
});

authRouter.post("/refresh", async (req, res, next) => {
  try {
    const parsed = refreshSchema.safeParse(req.body);
    if (!parsed.success) {
      res.status(400).json({ error: "Invalid refresh payload." });
      return;
    }

    const { refreshToken } = parsed.data;
    const { data, error } = await supabaseAnon.auth.refreshSession({
      refresh_token: refreshToken,
    });

    if (error || !data.user || !data.session) {
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
  } catch (err) {
    next(err);
  }
});

export { authRouter };
