import type { UserRole } from "./schema.js";

export type FullAdminRole = Extract<UserRole, "admin" | "sm_admin">;

export function isFullAdminRole(role: UserRole | null | undefined): role is FullAdminRole {
  return role === "admin" || role === "sm_admin";
}

export function isRoleAllowedForEndpoint(role: UserRole, allowedRoles: readonly UserRole[]): boolean {
  // sm_admin differs from admin only in its default workspace. Worker-only routes stay explicit.
  return allowedRoles.includes(role) || (role === "sm_admin" && allowedRoles.includes("admin"));
}
