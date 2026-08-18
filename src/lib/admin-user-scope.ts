import type { UserRole } from "./schema.js";
import { isFullAdminRole } from "./admin-role.js";

export function canManageUserRole(actorRole: UserRole, targetRole: UserRole): boolean {
  if (isFullAdminRole(actorRole)) return true;
  if (actorRole === "kunde") return targetRole === "gm";
  return false;
}

export function getRestrictedDirectoryRole(actorRole: UserRole): UserRole | undefined {
  if (actorRole === "kunde") return "gm";
  return undefined;
}
