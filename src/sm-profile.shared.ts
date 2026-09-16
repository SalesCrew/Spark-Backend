import { epochDayToIsoDate, isoDateToEpochDay, isoWeekday } from "./sm-planning.shared.js";

export type SmProfileAssignment = {
  status: string;
  effective: { plannedMinutes: number };
  actualMinutes: number | null;
};

export function smProfileWeek(today: string): { from: string; to: string } {
  const monday = isoDateToEpochDay(today) - isoWeekday(today) + 1;
  return { from: epochDayToIsoDate(monday), to: epochDayToIsoDate(monday + 6) };
}

export function summarizeSmProfileWeek(assignments: SmProfileAssignment[]) {
  const active = assignments.filter((assignment) => assignment.status !== "cancelled");
  return {
    assignmentCount: active.length,
    completedAssignmentCount: active.filter((assignment) => assignment.status === "completed").length,
    plannedMinutes: active.reduce((total, assignment) => total + assignment.effective.plannedMinutes, 0),
    actualMinutes: active.reduce((total, assignment) => total + (assignment.actualMinutes ?? 0), 0),
  };
}
