export function planAdditionalMarketVisit(
  assignments: ReadonlyArray<{ assignmentSlot: number; isDeleted: boolean }>,
): { assignmentSlot: number; visitTargetCount: 1 } | null {
  if (!assignments.some((assignment) => !assignment.isDeleted)) return null;
  return {
    assignmentSlot: assignments.reduce((max, assignment) => Math.max(max, assignment.assignmentSlot), 0) + 1,
    visitTargetCount: 1,
  };
}
