export const SM_INACTIVE_BAN_DURATION = "876000h";

export function smAuthBanDurationForStatus(isActive: boolean): string {
  return isActive ? "none" : SM_INACTIVE_BAN_DURATION;
}
