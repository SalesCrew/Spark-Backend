import type { UserRole } from "./schema.js";

export function shouldExcludeMhdPhotos(role: UserRole | null | undefined): boolean {
  return role === "kunde";
}
