import type { NextFunction, Request, Response } from "express";
import { and, eq } from "drizzle-orm";
import { db } from "./db.js";
import { kundeUsers, type KundePagePermissions } from "./schema.js";
import type { AuthedRequest } from "../middleware/auth.js";

export const KUNDE_PERMISSION_ACTIONS = ["read", "write", "update"] as const;
export type KundePermissionAction = (typeof KUNDE_PERMISSION_ACTIONS)[number];

export const KUNDE_ADMIN_PAGE_KEYS = [
  "gm_dashboard",
  "ipp_berechnung",
  "praemien",
  "fragebogen",
  "flexbesuche",
  "billa",
  "kuehlerinventur",
  "mhd",
  "fbmanagement",
  "fotoarchiv",
  "zeiterfassung",
  "maerkte",
  "lager",
  "gebietsmanager",
] as const;

export type KundeAdminPageKey = (typeof KUNDE_ADMIN_PAGE_KEYS)[number];

const pageKeySet = new Set<string>(KUNDE_ADMIN_PAGE_KEYS);
const actionSet = new Set<string>(KUNDE_PERMISSION_ACTIONS);

export function isKundeAdminPageKey(value: unknown): value is KundeAdminPageKey {
  return typeof value === "string" && pageKeySet.has(value);
}

export function normalizeKundePagePermissions(input: unknown): KundePagePermissions {
  if (!input || typeof input !== "object" || Array.isArray(input)) return {};
  const normalized: KundePagePermissions = {};
  for (const [pageKey, rawActions] of Object.entries(input as Record<string, unknown>)) {
    if (!isKundeAdminPageKey(pageKey) || !Array.isArray(rawActions)) continue;
    const actions = KUNDE_PERMISSION_ACTIONS.filter((action) => rawActions.includes(action));
    if (actions.length > 0) {
      normalized[pageKey] = actions;
    }
  }
  return normalized;
}

export function hasKundePermission(
  permissions: KundePagePermissions,
  pageKey: string,
  action: KundePermissionAction,
): boolean {
  const actions = permissions[pageKey] ?? [];
  return actions.includes(action);
}

export async function getKundePermissionsForUser(userId: string): Promise<KundePagePermissions> {
  const [row] = await db
    .select({ pagePermissions: kundeUsers.pagePermissions })
    .from(kundeUsers)
    .where(and(eq(kundeUsers.userId, userId), eq(kundeUsers.isDeleted, false)))
    .limit(1);
  return normalizeKundePagePermissions(row?.pagePermissions);
}

function methodToDefaultAction(method: string): KundePermissionAction {
  const normalized = method.toUpperCase();
  if (normalized === "GET" || normalized === "HEAD" || normalized === "OPTIONS") return "read";
  if (normalized === "PATCH" || normalized === "PUT" || normalized === "DELETE") return "update";
  return "write";
}

function routeStartsWith(pathname: string, prefix: string): boolean {
  return pathname === prefix || pathname.startsWith(`${prefix}/`);
}

function resolveSharedQuestionnairePage(req: Request): KundeAdminPageKey | null {
  const headerValue = req.get("x-coke-spark-page-key")?.trim();
  if (
    headerValue === "fragebogen" ||
    headerValue === "flexbesuche" ||
    headerValue === "billa" ||
    headerValue === "kuehlerinventur" ||
    headerValue === "mhd" ||
    headerValue === "fbmanagement"
  ) {
    return headerValue;
  }

  const pathname = req.path;
  if (pathname.includes("/kuehler")) return "kuehlerinventur";
  if (pathname.includes("/mhd")) return "mhd";
  return null;
}

function resolveKundePageKeyHeader(req: Request): KundeAdminPageKey | null {
  const headerValue = req.get("x-coke-spark-page-key")?.trim();
  return isKundeAdminPageKey(headerValue) ? headerValue : null;
}

export function resolveKundeAdminRequirement(req: Request): {
  pageKey: KundeAdminPageKey;
  action: KundePermissionAction;
} | null {
  const pathname = req.path;
  const originalUrl = req.originalUrl || req.url;
  const originalPath = originalUrl.split("?")[0] ?? originalUrl;
  const action = methodToDefaultAction(req.method);

  if (routeStartsWith(pathname, "/kunden-users") || routeStartsWith(originalPath, "/admin/kunden-users")) return null;
  if (routeStartsWith(pathname, "/users") || routeStartsWith(originalPath, "/admin/users")) return null;
  if (routeStartsWith(pathname, "/photos") || routeStartsWith(originalPath, "/admin/photos")) {
    return { pageKey: "fotoarchiv", action: originalUrl.includes("/signed-urls") ? "read" : action };
  }
  if (routeStartsWith(pathname, "/photo-tags") || routeStartsWith(originalPath, "/admin/photo-tags")) {
    const sharedPage = resolveSharedQuestionnairePage(req);
    return { pageKey: sharedPage ?? "fotoarchiv", action };
  }
  if (routeStartsWith(pathname, "/ipp") || routeStartsWith(originalPath, "/admin/ipp")) return { pageKey: "ipp_berechnung", action };
  if (routeStartsWith(pathname, "/praemien") || routeStartsWith(originalPath, "/admin/praemien")) return { pageKey: "praemien", action };
  if (routeStartsWith(pathname, "/zeiterfassung") || routeStartsWith(originalPath, "/admin/zeiterfassung")) return { pageKey: "zeiterfassung", action };
  if (routeStartsWith(pathname, "/markets") || originalUrl === "/markets" || originalUrl.startsWith("/markets?")) {
    const requestedPageKey = resolveKundePageKeyHeader(req);
    if (
      action === "read" &&
      (requestedPageKey === "gm_dashboard" || requestedPageKey === "fbmanagement" || requestedPageKey === "maerkte")
    ) {
      return { pageKey: requestedPageKey, action };
    }
    return { pageKey: "maerkte", action };
  }
  if (routeStartsWith(originalPath, "/admin/markets")) return { pageKey: "maerkte", action };
  if (routeStartsWith(pathname, "/lager") || routeStartsWith(originalPath, "/admin/lager")) return { pageKey: "lager", action };
  if (routeStartsWith(pathname, "/campaigns") || routeStartsWith(originalPath, "/admin/campaigns")) return { pageKey: "fbmanagement", action };
  if (routeStartsWith(pathname, "/red-month") || routeStartsWith(originalPath, "/admin/red-month")) return { pageKey: "gm_dashboard", action };
  if (
    routeStartsWith(pathname, "/modules") ||
    routeStartsWith(pathname, "/fragebogen") ||
    routeStartsWith(originalPath, "/admin/modules") ||
    routeStartsWith(originalPath, "/admin/fragebogen")
  ) {
    const pageKey = resolveSharedQuestionnairePage(req);
    return pageKey ? { pageKey, action } : null;
  }
  return null;
}

export function requireKundeAdminPermission(req: AuthedRequest, res: Response, next: NextFunction) {
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

  const requirement = resolveKundeAdminRequirement(req);
  if (!requirement) {
    res.status(403).json({ error: "Kein Zugriff freigeschaltet.", code: "kunde_page_forbidden" });
    return;
  }

  getKundePermissionsForUser(req.authUser.appUserId)
    .then((permissions) => {
      if (hasKundePermission(permissions, requirement.pageKey, requirement.action)) {
        next();
        return;
      }
      res.status(403).json({
        error: "Für diese Aktion ist kein Kundenzugriff freigeschaltet.",
        code: "kunde_permission_denied",
        pageKey: requirement.pageKey,
        action: requirement.action,
      });
    })
    .catch(next);
}
