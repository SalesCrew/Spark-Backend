const PASSWORD_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789!@#";

export function generatePassword(length = 12): string {
  return Array.from({ length }, () => {
    const idx = Math.floor(Math.random() * PASSWORD_CHARS.length);
    return PASSWORD_CHARS[idx] ?? "A";
  }).join("");
}
