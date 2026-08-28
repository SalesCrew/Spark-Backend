export function smMessageVisibleUntil(readAt: Date | null, visibleAfterReadDays: number | null): Date | null {
  if (!readAt || visibleAfterReadDays === null || visibleAfterReadDays <= 0) return null;
  return new Date(readAt.getTime() + visibleAfterReadDays * 86_400_000);
}

export function isSmMessageVisible(input: {
  readAt: Date | null;
  visibleAfterReadDays: number | null;
  now: Date;
}): boolean {
  if (!input.readAt) return true;
  if (input.visibleAfterReadDays === null) return true;
  if (input.visibleAfterReadDays === 0) return false;
  const visibleUntil = smMessageVisibleUntil(input.readAt, input.visibleAfterReadDays);
  return Boolean(visibleUntil && visibleUntil.getTime() > input.now.getTime());
}
