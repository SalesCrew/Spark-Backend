import { randomInt } from "node:crypto";

const PASSWORD_GROUPS = [
  "ABCDEFGHJKLMNPQRSTUVWXYZ",
  "abcdefghjkmnpqrstuvwxyz",
  "23456789",
  "!@#",
] as const;
const PASSWORD_CHARS = PASSWORD_GROUPS.join("");

function pickRandomChar(characters: string): string {
  return characters[randomInt(characters.length)] ?? "A";
}

export function generatePassword(length = 12): string {
  if (!Number.isInteger(length) || length < PASSWORD_GROUPS.length) {
    throw new RangeError(`Password length must be an integer of at least ${PASSWORD_GROUPS.length}.`);
  }

  const password = [
    ...PASSWORD_GROUPS.map((group) => pickRandomChar(group)),
    ...Array.from({ length: length - PASSWORD_GROUPS.length }, () => pickRandomChar(PASSWORD_CHARS)),
  ];

  for (let index = password.length - 1; index > 0; index -= 1) {
    const swapIndex = randomInt(index + 1);
    [password[index], password[swapIndex]] = [password[swapIndex] ?? "A", password[index] ?? "A"];
  }

  return password.join("");
}
