import { and, eq, isNotNull, or } from "drizzle-orm";
import { db } from "./db.js";
import {
  campaigns,
  fragebogenKuehler,
  fragebogenKuehlerModule,
  fragebogenMain,
  fragebogenMainModule,
  fragebogenMhd,
  fragebogenMhdModule,
  moduleKuehler,
  moduleKuehlerQuestion,
  moduleMain,
  moduleMainQuestion,
  moduleMhd,
  moduleMhdQuestion,
  questionBankShared,
  questionScoring,
} from "./schema.js";

type PraemienSectionType = "standard" | "flex" | "billa" | "kuehler" | "mhd" | "durcharbeit";
type PraemienMainSectionType = Extract<PraemienSectionType, "standard" | "flex" | "billa">;

type BonusSourceCatalogRow = {
  key: string;
  sectionType: PraemienSectionType;
  fragebogenId: string | null;
  fragebogenName: string;
  moduleId: string | null;
  moduleName: string;
  questionId: string;
  questionText: string;
  scoringKey: string;
  boniValue: number;
  isFactorMode: boolean;
  displayLabel: string;
};

function normalizeMoney(value: unknown): number {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return 0;
}

function sourceCatalogKey(input: {
  sectionType: string;
  fragebogenId: string | null;
  moduleId: string | null;
  questionId: string;
  scoringKey: string;
}): string {
  return [input.sectionType, input.fragebogenId ?? "", input.moduleId ?? "", input.questionId, input.scoringKey].join("__");
}

function resolveMainSourceSections(input: {
  fragebogenSections: unknown[] | null;
  fragebogenStatus: string;
  campaignSection: string | null;
}): PraemienMainSectionType[] {
  const configuredSections = new Set(
    (input.fragebogenSections ?? ["standard"])
      .map((entry) => String(entry))
      .filter((entry): entry is PraemienMainSectionType =>
        entry === "standard" || entry === "flex" || entry === "billa",
      ),
  );

  if (input.fragebogenStatus === "active" || input.fragebogenStatus === "scheduled") {
    return Array.from(configuredSections);
  }

  const campaignSection = input.campaignSection;
  if (
    (campaignSection === "standard" || campaignSection === "flex" || campaignSection === "billa") &&
    configuredSections.has(campaignSection)
  ) {
    return [campaignSection];
  }

  return [];
}

async function buildSourceCatalog(): Promise<BonusSourceCatalogRow[]> {
  const map = new Map<string, BonusSourceCatalogRow>();

  const mainRows = await db
    .select({
      fragebogenId: fragebogenMain.id,
      fragebogenName: fragebogenMain.name,
      fragebogenStatus: fragebogenMain.status,
      sectionKeywords: fragebogenMain.sectionKeywords,
      campaignSection: campaigns.section,
      moduleId: moduleMain.id,
      moduleName: moduleMain.name,
      questionId: questionBankShared.id,
      questionText: questionBankShared.text,
      scoringKey: questionScoring.scoreKey,
      boni: questionScoring.boni,
    })
    .from(questionScoring)
    .innerJoin(questionBankShared, eq(questionBankShared.id, questionScoring.questionId))
    .innerJoin(moduleMainQuestion, eq(moduleMainQuestion.questionId, questionBankShared.id))
    .innerJoin(moduleMain, eq(moduleMain.id, moduleMainQuestion.moduleId))
    .innerJoin(fragebogenMainModule, eq(fragebogenMainModule.moduleId, moduleMain.id))
    .innerJoin(fragebogenMain, eq(fragebogenMain.id, fragebogenMainModule.fragebogenId))
    .leftJoin(
      campaigns,
      and(
        eq(campaigns.currentFragebogenId, fragebogenMain.id),
        eq(campaigns.isDeleted, false),
        or(eq(campaigns.status, "active"), eq(campaigns.status, "scheduled")),
      ),
    )
    .where(
      and(
        eq(questionScoring.isDeleted, false),
        eq(questionBankShared.isDeleted, false),
        eq(moduleMainQuestion.isDeleted, false),
        eq(moduleMain.isDeleted, false),
        eq(fragebogenMainModule.isDeleted, false),
        eq(fragebogenMain.isDeleted, false),
        isNotNull(questionScoring.boni),
        or(
          eq(fragebogenMain.status, "active"),
          eq(fragebogenMain.status, "scheduled"),
          isNotNull(campaigns.id),
        ),
      ),
    );

  for (const row of mainRows) {
    const boniValue = normalizeMoney(row.boni);
    if (!Number.isFinite(boniValue) || boniValue === 0) continue;
    const sections = resolveMainSourceSections({
      fragebogenSections: row.sectionKeywords,
      fragebogenStatus: row.fragebogenStatus,
      campaignSection: row.campaignSection,
    });
    for (const sectionType of sections) {
      const key = sourceCatalogKey({
        sectionType,
        fragebogenId: row.fragebogenId,
        moduleId: row.moduleId,
        questionId: row.questionId,
        scoringKey: row.scoringKey,
      });
      if (map.has(key)) continue;
      const isFactorMode = row.scoringKey === "__value__";
      map.set(key, {
        key,
        sectionType,
        fragebogenId: row.fragebogenId,
        fragebogenName: row.fragebogenName,
        moduleId: row.moduleId,
        moduleName: row.moduleName,
        questionId: row.questionId,
        questionText: row.questionText,
        scoringKey: row.scoringKey,
        boniValue,
        isFactorMode,
        displayLabel: isFactorMode ? `Wert × ${boniValue}` : `Antwort: ${row.scoringKey}`,
      });
    }
  }

  const [kuehlerRows, mhdRows] = await Promise.all([
    db
      .select({
        fragebogenId: fragebogenKuehler.id,
        fragebogenName: fragebogenKuehler.name,
        moduleId: moduleKuehler.id,
        moduleName: moduleKuehler.name,
        questionId: questionBankShared.id,
        questionText: questionBankShared.text,
        scoringKey: questionScoring.scoreKey,
        boni: questionScoring.boni,
      })
      .from(questionScoring)
      .innerJoin(questionBankShared, eq(questionBankShared.id, questionScoring.questionId))
      .innerJoin(moduleKuehlerQuestion, eq(moduleKuehlerQuestion.questionId, questionBankShared.id))
      .innerJoin(moduleKuehler, eq(moduleKuehler.id, moduleKuehlerQuestion.moduleId))
      .innerJoin(fragebogenKuehlerModule, eq(fragebogenKuehlerModule.moduleId, moduleKuehler.id))
      .innerJoin(fragebogenKuehler, eq(fragebogenKuehler.id, fragebogenKuehlerModule.fragebogenId))
      .where(
        and(
          eq(questionScoring.isDeleted, false),
          eq(questionBankShared.isDeleted, false),
          eq(moduleKuehlerQuestion.isDeleted, false),
          eq(moduleKuehler.isDeleted, false),
          eq(fragebogenKuehlerModule.isDeleted, false),
          eq(fragebogenKuehler.isDeleted, false),
          isNotNull(questionScoring.boni),
          or(eq(fragebogenKuehler.status, "active"), eq(fragebogenKuehler.status, "scheduled")),
        ),
      ),
    db
      .select({
        fragebogenId: fragebogenMhd.id,
        fragebogenName: fragebogenMhd.name,
        moduleId: moduleMhd.id,
        moduleName: moduleMhd.name,
        questionId: questionBankShared.id,
        questionText: questionBankShared.text,
        scoringKey: questionScoring.scoreKey,
        boni: questionScoring.boni,
      })
      .from(questionScoring)
      .innerJoin(questionBankShared, eq(questionBankShared.id, questionScoring.questionId))
      .innerJoin(moduleMhdQuestion, eq(moduleMhdQuestion.questionId, questionBankShared.id))
      .innerJoin(moduleMhd, eq(moduleMhd.id, moduleMhdQuestion.moduleId))
      .innerJoin(fragebogenMhdModule, eq(fragebogenMhdModule.moduleId, moduleMhd.id))
      .innerJoin(fragebogenMhd, eq(fragebogenMhd.id, fragebogenMhdModule.fragebogenId))
      .where(
        and(
          eq(questionScoring.isDeleted, false),
          eq(questionBankShared.isDeleted, false),
          eq(moduleMhdQuestion.isDeleted, false),
          eq(moduleMhd.isDeleted, false),
          eq(fragebogenMhdModule.isDeleted, false),
          eq(fragebogenMhd.isDeleted, false),
          isNotNull(questionScoring.boni),
          or(eq(fragebogenMhd.status, "active"), eq(fragebogenMhd.status, "scheduled")),
        ),
      ),
  ]);

  for (const row of kuehlerRows) {
    const boniValue = normalizeMoney(row.boni);
    if (!Number.isFinite(boniValue) || boniValue === 0) continue;
    const key = sourceCatalogKey({
      sectionType: "kuehler",
      fragebogenId: row.fragebogenId,
      moduleId: row.moduleId,
      questionId: row.questionId,
      scoringKey: row.scoringKey,
    });
    if (map.has(key)) continue;
    const isFactorMode = row.scoringKey === "__value__";
    map.set(key, {
      key,
      sectionType: "kuehler",
      fragebogenId: row.fragebogenId,
      fragebogenName: row.fragebogenName,
      moduleId: row.moduleId,
      moduleName: row.moduleName,
      questionId: row.questionId,
      questionText: row.questionText,
      scoringKey: row.scoringKey,
      boniValue,
      isFactorMode,
      displayLabel: isFactorMode ? `Wert × ${boniValue}` : `Antwort: ${row.scoringKey}`,
    });
  }

  for (const row of mhdRows) {
    const boniValue = normalizeMoney(row.boni);
    if (!Number.isFinite(boniValue) || boniValue === 0) continue;
    const key = sourceCatalogKey({
      sectionType: "mhd",
      fragebogenId: row.fragebogenId,
      moduleId: row.moduleId,
      questionId: row.questionId,
      scoringKey: row.scoringKey,
    });
    if (map.has(key)) continue;
    const isFactorMode = row.scoringKey === "__value__";
    map.set(key, {
      key,
      sectionType: "mhd",
      fragebogenId: row.fragebogenId,
      fragebogenName: row.fragebogenName,
      moduleId: row.moduleId,
      moduleName: row.moduleName,
      questionId: row.questionId,
      questionText: row.questionText,
      scoringKey: row.scoringKey,
      boniValue,
      isFactorMode,
      displayLabel: isFactorMode ? `Wert × ${boniValue}` : `Antwort: ${row.scoringKey}`,
    });
  }

  return Array.from(map.values()).sort((a, b) =>
    a.sectionType.localeCompare(b.sectionType) ||
    a.fragebogenName.localeCompare(b.fragebogenName) ||
    a.moduleName.localeCompare(b.moduleName) ||
    a.questionText.localeCompare(b.questionText) ||
    a.scoringKey.localeCompare(b.scoringKey),
  );
}

export { buildSourceCatalog, normalizeMoney, resolveMainSourceSections, sourceCatalogKey };
export type { BonusSourceCatalogRow, PraemienSectionType };
