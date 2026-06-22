import type { NextFunction, Request, Response } from "express";
import { createHash } from "node:crypto";
import { and, eq, isNull } from "drizzle-orm";
import { db } from "../lib/db.js";
import { getRequestLogMeta, logger, serializeError } from "../lib/logger.js";
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

type VerifiedAccessTokenCacheEntry = {
  authId: string;
  expiresAtMs: number;
};

const VERIFIED_ACCESS_TOKEN_CACHE_TTL_MS = 5 * 60 * 1000;
const VERIFIED_ACCESS_TOKEN_CACHE_MAX_ENTRIES = 500;
const verifiedAccessTokenCache = new Map<string, VerifiedAccessTokenCacheEntry>();
const pendingAccessTokenVerifications = new Map<string, Promise<string | null>>();

function getAccessTokenCacheKey(accessToken: string): string {
  return createHash("sha256").update(accessToken).digest("hex");
}

function readJwtExpirationMs(accessToken: string): number | null {
  const [, payload] = accessToken.split(".");
  if (!payload) return null;

  try {
    const normalized = payload.replace(/-/g, "+").replace(/_/g, "/");
    const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, "=");
    const decoded = Buffer.from(padded, "base64").toString("utf8");
    const parsed = JSON.parse(decoded) as { exp?: unknown };
    return typeof parsed.exp === "number" ? parsed.exp * 1000 : null;
  } catch {
    return null;
  }
}

function pruneVerifiedAccessTokenCache(nowMs = Date.now()) {
  for (const [key, entry] of verifiedAccessTokenCache.entries()) {
    if (entry.expiresAtMs <= nowMs) {
      verifiedAccessTokenCache.delete(key);
    }
  }

  if (verifiedAccessTokenCache.size <= VERIFIED_ACCESS_TOKEN_CACHE_MAX_ENTRIES) return;

  const overflow = verifiedAccessTokenCache.size - VERIFIED_ACCESS_TOKEN_CACHE_MAX_ENTRIES;
  let deleted = 0;
  for (const key of verifiedAccessTokenCache.keys()) {
    verifiedAccessTokenCache.delete(key);
    deleted += 1;
    if (deleted >= overflow) break;
  }
}

function readVerifiedAccessToken(accessToken: string): string | null {
  const nowMs = Date.now();
  const key = getAccessTokenCacheKey(accessToken);
  const cached = verifiedAccessTokenCache.get(key);
  if (!cached) return null;
  if (cached.expiresAtMs <= nowMs) {
    verifiedAccessTokenCache.delete(key);
    return null;
  }
  return cached.authId;
}

function rememberVerifiedAccessToken(accessToken: string, authId: string) {
  const nowMs = Date.now();
  const jwtExpiresAtMs = readJwtExpirationMs(accessToken);
  const ttlExpiresAtMs = nowMs + VERIFIED_ACCESS_TOKEN_CACHE_TTL_MS;
  const expiresAtMs = jwtExpiresAtMs ? Math.min(ttlExpiresAtMs, jwtExpiresAtMs) : ttlExpiresAtMs;

  if (expiresAtMs <= nowMs) return;

  verifiedAccessTokenCache.set(getAccessTokenCacheKey(accessToken), { authId, expiresAtMs });
  pruneVerifiedAccessTokenCache(nowMs);
}

function isRetryableAuthProviderError(error: unknown): boolean {
  const err = error as { status?: unknown; code?: unknown; message?: unknown; name?: unknown; cause?: { code?: unknown; message?: unknown } };
  const status = typeof err?.status === "number" ? err.status : null;
  if (status !== null && status >= 500) return true;

  const text = [err?.name, err?.message, err?.code, err?.cause?.code, err?.cause?.message]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  return (
    text.includes("fetch failed") ||
    text.includes("connect timeout") ||
    text.includes("network") ||
    text.includes("und_err_connect_timeout") ||
    text.includes("econnreset") ||
    text.includes("etimedout") ||
    text.includes("econnrefused")
  );
}

async function verifyAccessToken(accessToken: string): Promise<string | null> {
  const cachedAuthId = readVerifiedAccessToken(accessToken);
  if (cachedAuthId) return cachedAuthId;

  const cacheKey = getAccessTokenCacheKey(accessToken);
  const pending = pendingAccessTokenVerifications.get(cacheKey);
  if (pending) return pending;

  const verification = (async () => {
    const { data, error } = await supabaseAdmin.auth.getUser(accessToken);
    if (error || !data.user) {
      if (error && isRetryableAuthProviderError(error)) throw error;
      return null;
    }

    rememberVerifiedAccessToken(accessToken, data.user.id);
    return data.user.id;
  })();

  pendingAccessTokenVerifications.set(cacheKey, verification);
  try {
    return await verification;
  } finally {
    pendingAccessTokenVerifications.delete(cacheKey);
  }
}

export function requireAuth(allowedRoles?: UserRole[]) {
  return async (req: AuthedRequest, res: Response, next: NextFunction) => {
    try {
      if (process.env.NODE_ENV === "test" && process.env.BYPASS_AUTH_FOR_TESTS === "1") {
        const bypassRoleEnv = process.env.BYPASS_AUTH_ROLE;
        const bypassUserIdEnv = process.env.BYPASS_AUTH_USER_ID;
        const bypassRole: UserRole =
          bypassRoleEnv === "admin" || bypassRoleEnv === "gm" || bypassRoleEnv === "sm" || bypassRoleEnv === "kunde"
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
        logger.warn("auth_missing_access_token", {
          ...getRequestLogMeta(req),
        });
        res.status(401).json({ error: "Missing access token.", code: "auth_missing_access_token" });
        return;
      }

      let authId: string | null = null;
      try {
        authId = await verifyAccessToken(accessToken);
      } catch (err) {
        if (isRetryableAuthProviderError(err)) {
          logger.warn("auth_provider_unreachable", {
            ...getRequestLogMeta(req),
            error: serializeError(err),
          });
          res.status(503).json({
            error: "Auth provider is temporarily unavailable. Please retry.",
            code: "auth_provider_unreachable",
          });
          return;
        }

        throw err;
      }

      if (!authId) {
        logger.warn("auth_invalid_access_token", {
          ...getRequestLogMeta(req),
        });
        res.status(401).json({ error: "Invalid access token.", code: "auth_invalid_access_token" });
        return;
      }

      const [appUser] = await db
        .select()
        .from(users)
        .where(and(eq(users.supabaseAuthId, authId), eq(users.isActive, true), isNull(users.deletedAt)))
        .limit(1);

      if (!appUser) {
        logger.warn("auth_user_not_active_or_missing", {
          ...getRequestLogMeta(req),
          supabaseAuthId: authId,
        });
        res.status(403).json({ error: "User is deactivated or missing.", code: "account_inactive" });
        return;
      }

      if (allowedRoles && !allowedRoles.includes(appUser.role)) {
        logger.warn("auth_role_not_allowed", {
          ...getRequestLogMeta(req),
          appUserId: appUser.id,
          role: appUser.role,
          allowedRoles,
        });
        res.status(403).json({ error: "Role is not allowed for this endpoint.", code: "role_not_allowed" });
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
      logger.error("auth_middleware_failure", {
        ...getRequestLogMeta(req),
        error: serializeError(err),
      });
      next(err);
    }
  };
}

export type { AuthedRequest };
