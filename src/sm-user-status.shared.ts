export const INACTIVE_ACCOUNT_BAN_DURATION = "876000h";
export const SM_INACTIVE_BAN_DURATION = INACTIVE_ACCOUNT_BAN_DURATION;

export function canToggleAccountStatus(role: string): boolean {
  return role === "sm" || role === "gm";
}

export function authBanDurationForStatus(isActive: boolean): string {
  return isActive ? "none" : INACTIVE_ACCOUNT_BAN_DURATION;
}

export function smAuthBanDurationForStatus(isActive: boolean): string {
  return authBanDurationForStatus(isActive);
}
