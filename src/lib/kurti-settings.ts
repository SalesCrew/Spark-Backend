import { z } from "zod";

/** Default preserves normal service. Reject typos rather than silently enabling a disabled service. */
export const kurtiEnabledSchema = z.preprocess(value => {
  if (typeof value !== "string") return value;
  const normalized = value.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return value;
}, z.boolean().default(true));
