import type { NextFunction, Request, Response } from "express";
import { and, eq } from "drizzle-orm";
import { isFullAdminRole } from "./admin-role.js";
import { db } from "./db.js";
import { kundeUsers, smAssignments, type KundePagePermissions } from "./schema.js";
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
  "durcharbeit",
  "fbmanagement",
  "fotoarchiv",
  "zeiterfassung",
  "maerkte",
  "lager",
  "gebietsmanager",
  "shelfmerchandiser",
  "sm_dashboard",
  "sm_fragebogen",
  "sm_durcharbeit",
  "sm_fbmanagement",
  "sm_fotoarchiv",
  "sm_verplanung",
  "sm_durcharbeit_verplanung",
  "sm_zeiterfassung",
  "sm_maerkte",
  "sm_durcharbeit_maerkte",
  "sm_nachrichten",
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
    headerValue === "durcharbeit" ||
    headerValue === "fbmanagement"
  ) {
    return headerValue;
  }

  const pathname = req.path;
  if (pathname.includes("/kuehler")) return "kuehlerinventur";
  if (pathname.includes("/mhd")) return "mhd";
  if (pathname.includes("/durcharbeit")) return "durcharbeit";
  return null;
}

function resolveKundePageKeyHeader(req: Request): KundeAdminPageKey | null {
  const headerValue = req.get("x-coke-spark-page-key")?.trim();
  return isKundeAdminPageKey(headerValue) ? headerValue : null;
}

type KundeAdminRequirement = { pageKey: KundeAdminPageKey; action: KundePermissionAction; allowAnyPagePermission?: boolean };
const smPageKeys = new Set<string>(KUNDE_ADMIN_PAGE_KEYS.filter(key => key.startsWith("sm_") || key === "shelfmerchandiser"));
export function isKundeSmPageKey(value: unknown): value is KundeAdminPageKey {
  return typeof value === "string" && smPageKeys.has(value);
}

/** A page header may select a known read dependency, never an unrelated resource or write scope. */
function smReadPage(req: Request, canonical: KundeAdminPageKey, dependencies: KundeAdminPageKey[] = []): KundeAdminPageKey {
  const header = resolveKundePageKeyHeader(req);
  return header && dependencies.includes(header) ? header : canonical;
}
function resolveKundeSmRequirement(req: Request, path: string, action: KundePermissionAction): KundeAdminRequirement | null | undefined {
  const read = action === "read";
  if (routeStartsWith(path, "/admin/sm-dashboard")) return { pageKey: "sm_dashboard", action };
  if (routeStartsWith(path, "/admin/sm-questionnaires")) {
    const canonical = req.query.scope === "SMDurcharbeit" ? "sm_durcharbeit" : "sm_fragebogen";
    // The existing POST save handler also accepts an existing UUID. That is an update.
    const saveAction = req.method === "POST" && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(req.body?.id ?? "")) ? "update" : action;
    return { pageKey: read ? smReadPage(req, canonical, ["sm_fbmanagement", "sm_verplanung", "sm_durcharbeit_verplanung"]) : canonical, action: saveAction };
  }
  if (routeStartsWith(path, "/admin/sm-photos")) return { pageKey: "sm_fotoarchiv", action: /\/(signed-urls|export)$/.test(path) ? "read" : action };
  if (routeStartsWith(path, "/admin/sm-smdurcharbeit-times")) return { pageKey: smReadPage(req, "sm_zeiterfassung", ["sm_fbmanagement"]), action: read ? action : "update" };
  if (routeStartsWith(path, "/admin/sm-smdurcharbeit-campaigns")) {
    return { pageKey: "sm_durcharbeit_verplanung", action: /\/preview$/.test(path) ? "read" : read ? action : req.method === "POST" && path === "/admin/sm-smdurcharbeit-campaigns" ? "write" : "update" };
  }
  if (routeStartsWith(path, "/admin/sm-activity/completed")) return { pageKey: read ? smReadPage(req, "sm_fbmanagement", ["sm_zeiterfassung", "shelfmerchandiser"]) : "sm_fbmanagement", action: read ? action : "update" };
  // Internal request-review queues are not customer pages.
  if (routeStartsWith(path, "/admin/sm-activity")) return null;
  if (routeStartsWith(path, "/admin/sm-messages")) return { pageKey: "sm_nachrichten", action };
  if (routeStartsWith(path, "/admin/sm-markets")) {
    const dedicated = routeStartsWith(path, "/admin/sm-markets/SMDurcharbeit") || req.query.SMDurcharbeitMarketScope === "SMDurcharbeit";
    const canonical = dedicated ? "sm_durcharbeit_maerkte" : "sm_maerkte";
    return { pageKey: read ? smReadPage(req, canonical, ["sm_dashboard", "sm_verplanung", "sm_durcharbeit_verplanung", "sm_fbmanagement", "sm_fotoarchiv", "shelfmerchandiser"]) : canonical, action: path.endsWith("/deactivate") ? "update" : action };
  }
  if (routeStartsWith(path, "/admin/sm-planning")) {
    if (path.endsWith("/questionnaire-assignment")) return { pageKey: read ? smReadPage(req, "sm_verplanung", ["sm_durcharbeit_verplanung"]) : "sm_verplanung", action };
    if (path.endsWith("/gm-visits")) return { pageKey: "zeiterfassung", action: "read" };
    if (/\/time-change-requests\//.test(path) || /\/(time|visit-time)$/.test(path)) return { pageKey: smReadPage(req, "sm_zeiterfassung", ["sm_fbmanagement"]), action: read ? action : "update" };
    const canonical = resolveKundePageKeyHeader(req) === "sm_durcharbeit_verplanung" ? "sm_durcharbeit_verplanung" : "sm_verplanung";
    return { pageKey: read ? smReadPage(req, canonical, ["sm_dashboard", "sm_zeiterfassung", "shelfmerchandiser"]) : canonical,
      action: path.endsWith("/holidays/reconcile") && req.body?.dryRun === true ? "read" : read ? action : req.method === "POST" && /\/(assignments|series)$/.test(path) ? "write" : "update" };
  }
  return undefined;
}

export function resolveKundeAdminRequirement(req: Request): {
  pageKey: KundeAdminPageKey;
  action: KundePermissionAction;
  allowAnyPagePermission?: boolean;
} | null {
  const pathname = req.path;
  const originalUrl = req.originalUrl || req.url;
  const originalPath = originalUrl.split("?")[0] ?? originalUrl;
  const action = methodToDefaultAction(req.method);
  const isExportRequest = /(?:^|[\/-])export(?:[\/-]|$)/i.test(originalPath);

  const smRequirement = resolveKundeSmRequirement(req, originalPath, action);
  if (smRequirement !== undefined) return smRequirement;

  if (routeStartsWith(pathname, "/kunden-users") || routeStartsWith(originalPath, "/admin/kunden-users")) return null;
  if (routeStartsWith(pathname, "/users") || routeStartsWith(originalPath, "/admin/users")) return null;
  if (routeStartsWith(pathname, "/photos") || routeStartsWith(originalPath, "/admin/photos")) {
    return {
      pageKey: "fotoarchiv",
      action: originalUrl.includes("/signed-urls") ? "read" : action,
      allowAnyPagePermission: isExportRequest,
    };
  }
  if (routeStartsWith(pathname, "/photo-tags") || routeStartsWith(originalPath, "/admin/photo-tags")) {
    const sharedPage = resolveSharedQuestionnairePage(req);
    return { pageKey: sharedPage ?? "fotoarchiv", action };
  }
  if (routeStartsWith(pathname, "/gm-dashboard") || routeStartsWith(originalPath, "/admin/gm-dashboard")) return { pageKey: "gm_dashboard", action: "read" };
  if (routeStartsWith(pathname, "/ipp") || routeStartsWith(originalPath, "/admin/ipp")) return { pageKey: "ipp_berechnung", action };
  if (routeStartsWith(pathname, "/praemien") || routeStartsWith(originalPath, "/admin/praemien")) return { pageKey: "praemien", action: req.method === "POST" && /^\/admin\/praemien\/workspace\/waves\/[^/]+\/preview$/.test(originalPath) ? "read" : action };
  if (routeStartsWith(pathname, "/zeiterfassung") || routeStartsWith(originalPath, "/admin/zeiterfassung")) return { pageKey: "zeiterfassung", action, allowAnyPagePermission: isExportRequest };
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
  if (routeStartsWith(pathname, "/campaigns") || routeStartsWith(originalPath, "/admin/campaigns")) return { pageKey: "fbmanagement", action, allowAnyPagePermission: isExportRequest };
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
  if (isFullAdminRole(req.authUser.role)) {
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
    .then(async (permissions) => {
      const planningPath = (req.originalUrl || req.url).split("?")[0] ?? "";
      if (planningPath.startsWith("/admin/sm-planning/") && requirement.action !== "read" && ["sm_verplanung", "sm_durcharbeit_verplanung"].includes(requirement.pageKey)) {
        const targetId = planningPath.match(/^\/admin\/sm-planning\/assignments\/([0-9a-f-]{36})(?:\/|$)/i)?.[1];
        const [target] = targetId ? await db.select({ override: smAssignments.SMDurcharbeitQuestionnaireOverrideVersionId }).from(smAssignments).where(eq(smAssignments.id, targetId)).limit(1) : [];
        requirement.pageKey = target ? target.override ? "sm_durcharbeit_verplanung" : "sm_verplanung" : req.body?.SMDurcharbeitQuestionnaireOverrideVersionId ? "sm_durcharbeit_verplanung" : "sm_verplanung";
        // Moving an existing Einsatz to another planning scope requires both scopes.
        if (target && Object.hasOwn(req.body ?? {}, "SMDurcharbeitQuestionnaireOverrideVersionId")) {
          const destination = req.body.SMDurcharbeitQuestionnaireOverrideVersionId ? "sm_durcharbeit_verplanung" : "sm_verplanung";
          if (!hasKundePermission(permissions, destination, "update")) {
            res.status(403).json({ error: "Für diese Aktion ist kein Kundenzugriff freigeschaltet.", code: "kunde_permission_denied", pageKey: destination, action: "update" });
            return;
          }
        }
      }
      const pageActions = permissions[requirement.pageKey] ?? [];
      const requestPath = (req.originalUrl || req.url).split("?")[0] ?? "";
      const isExportRequest = /(?:^|[\/-])export(?:[\/-]|$)/i.test(requestPath);
      if (
        hasKundePermission(permissions, requirement.pageKey, requirement.action) ||
        ((requirement.allowAnyPagePermission === true || (isExportRequest && !isKundeSmPageKey(requirement.pageKey))) && pageActions.length > 0)
      ) {
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
