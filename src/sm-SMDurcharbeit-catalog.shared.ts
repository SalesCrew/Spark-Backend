export type SmQuestionnaireCatalogScope = "standard" | "SMDurcharbeit";

// Existing database constraints require lowercase stable codes. Only newly created
// Durcharbeit roots receive this namespace; legacy IDs, codes and links stay intact.
export const SMDurcharbeitStableCodePrefix = "smdurcharbeit_";

export function smQuestionnaireCatalogScope(stableCode: string): SmQuestionnaireCatalogScope {
  return stableCode.startsWith(SMDurcharbeitStableCodePrefix) ? "SMDurcharbeit" : "standard";
}

export function SMDurcharbeitCatalogStableCode(kind: "module" | "questionnaire", id: string): string {
  return `${SMDurcharbeitStableCodePrefix}${kind}_${id.replaceAll("-", "")}`;
}
