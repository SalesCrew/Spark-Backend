import type { UserRole } from "./schema.js";
import { isFullAdminRole } from "./admin-role.js";

export function canManageUserRole(actorRole: UserRole, targetRole: UserRole, customerRole: "gm" | "sm" = "gm"): boolean {
  if (isFullAdminRole(actorRole)) return true;
  if (actorRole === "kunde") return targetRole === customerRole;
  return false;
}

export function getRestrictedDirectoryRole(actorRole: UserRole, customerRole: "gm" | "sm" = "gm"): UserRole | undefined {
  if (actorRole === "kunde") return customerRole;
  return undefined;
}
