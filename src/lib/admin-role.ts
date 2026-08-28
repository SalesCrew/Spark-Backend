import type { UserRole } from "./schema.js";

export type FullAdminRole = Extract<UserRole, "admin" | "sm_admin">;

export function isFullAdminRole(role: UserRole | null | undefined): role is FullAdminRole {
  return role === "admin" || role === "sm_admin";
}

export function isRoleAllowedForEndpoint(role: UserRole, allowedRoles: readonly UserRole[]): boolean {
  return allowedRoles.includes(role);
}
