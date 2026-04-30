import type { NextFunction, Request, Response } from "express";
import { and, eq, isNull } from "drizzle-orm";
import { db } from "../lib/db.js";
import { supabaseAdmin } from "../lib/supabase.js";
import { type UserRole, users } from "../lib/schema.js";

type AuthedRequest = Request & {
  authUser?: {
    appUserId: string;
    supabaseAuthId: string;
    role: UserRole;
    email: string;
  };
};

function readBearerToken(req: Request): string | null {
  const authHeader = req.headers.authorization;
  if (!authHeader) return null;
  if (!authHeader.startsWith("Bearer ")) return null;
  const token = authHeader.slice("Bearer ".length).trim();
  return token.length > 0 ? token : null;
}

export function requireAuth(allowedRoles?: UserRole[]) {
  return async (req: AuthedRequest, res: Response, next: NextFunction) => {
    try {
      if (process.env.NODE_ENV === "test" && process.env.BYPASS_AUTH_FOR_TESTS === "1") {
        const bypassRoleEnv = process.env.BYPASS_AUTH_ROLE;
        const bypassUserIdEnv = process.env.BYPASS_AUTH_USER_ID;
        const bypassRole: UserRole =
          bypassRoleEnv === "admin" || bypassRoleEnv === "gm" || bypassRoleEnv === "sm"
            ? bypassRoleEnv
            : "admin";
        req.authUser = {
          appUserId: bypassUserIdEnv && bypassUserIdEnv.trim() ? bypassUserIdEnv.trim() : "00000000-0000-4000-8000-000000000001",
          supabaseAuthId: "00000000-0000-4000-8000-000000000002",
          role: bypassRole,
          email: "test-admin@example.com",
        };
        next();
        return;
      }

      const accessToken = readBearerToken(req);
      if (!accessToken) {
        res.status(401).json({ error: "Missing access token." });
        return;
      }

      const { data, error } = await supabaseAdmin.auth.getUser(accessToken);
      if (error || !data.user) {
        res.status(401).json({ error: "Invalid access token." });
        return;
      }

      const authId = data.user.id;
      const [appUser] = await db
        .select()
        .from(users)
        .where(and(eq(users.supabaseAuthId, authId), eq(users.isActive, true), isNull(users.deletedAt)))
        .limit(1);

      if (!appUser) {
        res.status(403).json({ error: "User is deactivated or missing." });
        return;
      }

      if (allowedRoles && !allowedRoles.includes(appUser.role)) {
        res.status(403).json({ error: "Role is not allowed for this endpoint." });
        return;
      }

      req.authUser = {
        appUserId: appUser.id,
        supabaseAuthId: appUser.supabaseAuthId,
        role: appUser.role,
        email: appUser.email,
      };

      next();
    } catch (err) {
      next(err);
    }
  };
}

export type { AuthedRequest };
