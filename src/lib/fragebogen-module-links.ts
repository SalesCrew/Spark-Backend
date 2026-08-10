export function filterFragebogenModuleLinksByActiveModuleIds<T extends { moduleId: string }>(
  links: T[],
  activeModuleIds: Iterable<string>,
): T[] {
  const activeIds = new Set(activeModuleIds);
  return links.filter((link) => activeIds.has(link.moduleId));
}
