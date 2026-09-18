export type SmAssignmentCompletionUpdate = {
  status: "completed";
  startedAt: Date;
  completedAt: Date;
  updatedByUserId: string;
  updatedAt: Date;
};

export function buildSmAssignmentCompletionUpdate(input: {
  visitStartedAt: Date;
  visitCompletedAt: Date;
  actorUserId: string;
  updatedAt: Date;
}): SmAssignmentCompletionUpdate {
  return {
    status: "completed",
    startedAt: input.visitStartedAt,
    completedAt: input.visitCompletedAt,
    updatedByUserId: input.actorUserId,
    updatedAt: input.updatedAt,
  };
}
