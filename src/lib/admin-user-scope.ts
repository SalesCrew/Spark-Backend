import type { UserRole } from "./schema.js";

export function canManageUserRole(actorRole: UserRole, targetRole: UserRole): boolean {
  if (actorRole === "admin") return true;
  if (actorRole === "sm_admin") return targetRole === "sm";
  if (actorRole === "kunde") return targetRole === "gm";
  return false;
}

export function getRestrictedDirectoryRole(actorRole: UserRole): UserRole | undefined {
  if (actorRole === "sm_admin") return "sm";
  if (actorRole === "kunde") return "gm";
  return undefined;
}
