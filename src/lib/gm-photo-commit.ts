export type GmPhotoCommitMode = "replace" | "append";

export type GmPhotoCommitPlan = {
  insertPaths: string[];
  updatePaths: string[];
  deletePaths: string[];
};

export function planGmPhotoCommit(
  existingPaths: readonly string[],
  requestedPaths: readonly string[],
  mode: GmPhotoCommitMode,
): GmPhotoCommitPlan {
  const existing = new Set(existingPaths);
  const requested = Array.from(new Set(requestedPaths));
  const requestedSet = new Set(requested);

  return {
    insertPaths: requested.filter((path) => !existing.has(path)),
    updatePaths: requested.filter((path) => existing.has(path)),
    deletePaths: mode === "replace"
      ? Array.from(existing).filter((path) => !requestedSet.has(path))
      : [],
  };
}
