import { computeHiddenQuestionIds } from "./conditional-visibility.js";

type AnswerStatus = "unanswered" | "answered" | "invalid" | "hidden_by_rule" | "skipped";

export type VisitAnswerValidationResult = {
  answerStatus: "unanswered" | "answered" | "invalid";
  valueText: string | null;
  valueNumber: string | null;
  valueJson: Record<string, unknown> | null;
  isValid: boolean;
  validationError: string | null;
  options: Array<{ optionRole: "top" | "sub"; optionValue: string; orderIndex: number }>;
  matrixCells: Array<{
    rowKey: string;
    columnKey: string;
    cellValueText: string | null;
    cellValueDate: string | null;
    cellSelected: boolean | null;
    orderIndex: number;
  }>;
  inlinePhotos: Array<{ payload: string; selectedTagIds: string[]; orderIndex: number }>;
};

export type RequiredCheckQuestion = {
  id: string;
  questionId: string;
  questionType: string;
  questionTextSnapshot: string;
  questionConfigSnapshot: Record<string, unknown> | null;
  questionRulesSnapshot: Array<Record<string, unknown>> | null;
  requiredSnapshot: boolean;
  appliesToMarketChainSnapshot: boolean;
};

export type RequiredCheckAnswer = {
  id: string;
  visitSessionQuestionId: string;
  answerStatus: AnswerStatus;
  valueText: string | null;
  valueNumber: string | number | null;
  valueJson: Record<string, unknown> | null;
  isValid: boolean;
};

export type RequiredCheckPhoto = {
  id: string;
  visitAnswerId: string;
  storagePath: string;
  inherited?: boolean;
};

export type RequiredCheckPhotoTag = {
  visitAnswerPhotoId: string;
};

export type MissingRequiredQuestion = {
  visitQuestionId: string;
  questionId: string;
  questionText: string;
  questionType: string;
};

function decodePhotoAnswer(raw: unknown): { photos: string[]; selectedTagIds: string[] } {
  if (!raw) return { photos: [], selectedTagIds: [] };
  if (Array.isArray(raw)) return { photos: raw.filter((v): v is string => typeof v === "string"), selectedTagIds: [] };
  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw) as { photos?: unknown; selectedTagIds?: unknown };
      const photos = Array.isArray(parsed?.photos) ? parsed.photos.filter((v): v is string => typeof v === "string") : [];
      const selectedTagIds = Array.isArray(parsed?.selectedTagIds)
        ? parsed.selectedTagIds.filter((v): v is string => typeof v === "string")
        : [];
      return { photos, selectedTagIds };
    } catch {
      return { photos: [raw], selectedTagIds: [] };
    }
  }
  return { photos: [], selectedTagIds: [] };
}

function parseDateCellValue(input: string): string | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(input.trim());
  if (!match) return null;
  const yyyy = Number(match[1]);
  const mm = Number(match[2]);
  const dd = Number(match[3]);
  if (mm < 1 || mm > 12 || dd < 1 || dd > 31) return null;
  const candidate = new Date(Date.UTC(yyyy, mm - 1, dd));
  if (
    candidate.getUTCFullYear() !== yyyy ||
    candidate.getUTCMonth() !== mm - 1 ||
    candidate.getUTCDate() !== dd
  ) {
    return null;
  }
  return `${match[1]}-${match[2]}-${match[3]}`;
}

function asStringArray(input: unknown): string[] {
  return Array.isArray(input) ? input.filter((v): v is string => typeof v === "string") : [];
}

function normalizeOptionsFromConfig(config: Record<string, unknown>): string[] {
  const fromOptions = asStringArray(config.options);
  if (fromOptions.length > 0) return fromOptions;
  return [];
}

function normalizeUnique(values: string[]): string[] {
  return Array.from(new Set(values.map((v) => v.trim()).filter((v) => v.length > 0)));
}

function parseNumericConstraint(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function parseMatrixCellKey(cellKey: string): { rowKey: string; columnKey: string } {
  const idx = cellKey.indexOf("::");
  if (idx >= 0) {
    return {
      rowKey: cellKey.slice(0, idx).trim(),
      columnKey: cellKey.slice(idx + 2).trim(),
    };
  }
  const splitColon = cellKey.split(":");
  if (splitColon.length >= 2) {
    const rowKey = splitColon.shift() ?? "";
    return { rowKey: rowKey.trim(), columnKey: splitColon.join(":").trim() };
  }
  return { rowKey: cellKey.trim(), columnKey: "" };
}

export function buildVisitAnswerValidationResult(
  questionType: string,
  config: Record<string, unknown>,
  rawAnswer: unknown,
): VisitAnswerValidationResult {
  const empty: VisitAnswerValidationResult = {
    answerStatus: "unanswered",
    valueText: null,
    valueNumber: null,
    valueJson: null,
    isValid: true,
    validationError: null,
    options: [],
    matrixCells: [],
    inlinePhotos: [],
  };

  if (rawAnswer == null || rawAnswer === "" || (Array.isArray(rawAnswer) && rawAnswer.length === 0)) return empty;

  if (questionType === "yesno" || questionType === "single" || questionType === "text") {
    if (typeof rawAnswer !== "string") return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ungültiges Text-Antwortformat." };
    if (questionType === "yesno") {
      const allowed = normalizeOptionsFromConfig(config);
      const whitelist = allowed.length > 0 ? allowed : ["Ja", "Nein", "ja", "nein"];
      if (!whitelist.includes(rawAnswer)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ja/Nein Antwort ist nicht erlaubt." };
      }
    }
    if (questionType === "single") {
      const allowed = normalizeOptionsFromConfig(config);
      if (allowed.length > 0 && !allowed.includes(rawAnswer)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ausgewählte Option ist nicht erlaubt." };
      }
    }
    if (questionType === "text") {
      const minLength = parseNumericConstraint(config.minLength);
      const maxLength = parseNumericConstraint(config.maxLength);
      if (minLength !== null && rawAnswer.length < minLength) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Text ist kürzer als erlaubt." };
      }
      if (maxLength !== null && rawAnswer.length > maxLength) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Text ist länger als erlaubt." };
      }
      if (typeof config.pattern === "string" && config.pattern.length > 0) {
        try {
          const regex = new RegExp(config.pattern);
          if (!regex.test(rawAnswer)) {
            return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Text entspricht nicht dem erwarteten Muster." };
          }
        } catch {
          return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Text-Muster ist ungültig konfiguriert." };
        }
      }
    }
    return { ...empty, answerStatus: "answered", valueText: rawAnswer, valueJson: { raw: rawAnswer } };
  }

  if (questionType === "multiple") {
    if (!Array.isArray(rawAnswer) || rawAnswer.some((v) => typeof v !== "string")) {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Mehrfachauswahl muss ein String-Array sein." };
    }
    const selected = normalizeUnique(rawAnswer);
    const allowed = normalizeOptionsFromConfig(config);
    if (allowed.length > 0 && selected.some((v) => !allowed.includes(v))) {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Mehrfachauswahl enthält ungültige Option(en)." };
    }
    return {
      ...empty,
      answerStatus: "answered",
      valueJson: { raw: selected },
      options: selected.map((value, idx) => ({ optionRole: "sub", optionValue: value, orderIndex: idx })),
    };
  }

  if (questionType === "yesnomulti") {
    if (typeof rawAnswer !== "string") return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ja/Nein-Multi erwartet JSON-String." };
    try {
      const parsed = JSON.parse(rawAnswer) as { sel?: unknown; subs?: unknown };
      const top = typeof parsed?.sel === "string" ? parsed.sel : "";
      const subs = normalizeUnique(Array.isArray(parsed?.subs) ? parsed.subs.filter((v): v is string => typeof v === "string") : []);
      const configAnswers = asStringArray(config.answers);
      const configBranches = Array.isArray(config.branches) ? (config.branches as Array<Record<string, unknown>>) : [];
      if (configAnswers.length > 0 && top.length > 0 && !configAnswers.includes(top)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ja/Nein-Multi Top-Antwort ist nicht erlaubt." };
      }
      if (top.length > 0 && configBranches.length > 0) {
        const branch = configBranches.find((entry) => entry.answer === top);
        const branchOptions = branch ? asStringArray((branch as Record<string, unknown>).options) : [];
        if (branchOptions.length > 0 && subs.some((v) => !branchOptions.includes(v))) {
          return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ja/Nein-Multi Unteroption ist nicht erlaubt." };
        }
      }
      const options = [
        ...(top ? [{ optionRole: "top" as const, optionValue: top, orderIndex: 0 }] : []),
        ...subs.map((value, idx) => ({ optionRole: "sub" as const, optionValue: value, orderIndex: idx })),
      ];
      return {
        ...empty,
        answerStatus: top ? "answered" : "unanswered",
        valueText: top || null,
        valueJson: { raw: parsed },
        options,
      };
    } catch {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ja/Nein-Multi JSON ungültig." };
    }
  }

  if (questionType === "likert" || questionType === "numeric" || questionType === "slider") {
    const numeric = Number(rawAnswer);
    if (!Number.isFinite(numeric)) {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Zahlenantwort ungültig." };
    }
    const min = parseNumericConstraint(config.min);
    const max = parseNumericConstraint(config.max);
    if (min !== null && numeric < min) {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Wert liegt unter dem erlaubten Minimum." };
    }
    if (max !== null && numeric > max) {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Wert liegt über dem erlaubten Maximum." };
    }
    if (questionType === "slider") {
      const step = parseNumericConstraint(config.step);
      if (step && step > 0 && min !== null) {
        const remainder = Math.abs((numeric - min) % step);
        if (remainder > 1e-6 && Math.abs(step - remainder) > 1e-6) {
          return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Slider-Wert entspricht nicht dem konfigurierten Schritt." };
        }
      }
    }
    if (questionType === "numeric") {
      const decimalsAllowed = Boolean(config.decimals);
      if (!decimalsAllowed && !Number.isInteger(numeric)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Ganzzahl erwartet." };
      }
    }
    if (questionType === "likert") {
      if (!Number.isInteger(numeric)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Likert erwartet eine ganze Zahl." };
      }
    }
    return {
      ...empty,
      answerStatus: "answered",
      valueText: String(rawAnswer),
      valueNumber: String(numeric),
      valueJson: { raw: rawAnswer },
    };
  }

  if (questionType === "photo") {
    if (rawAnswer !== undefined && rawAnswer !== null) {
      return {
        ...empty,
        answerStatus: "unanswered",
        valueJson: null,
      };
    }
    const decoded = decodePhotoAnswer(rawAnswer);
    const tagsEnabled = Boolean(config.tagsEnabled) && Array.isArray(config.tagIds) && (config.tagIds as unknown[]).length > 0;
    if (tagsEnabled && decoded.photos.length > 0 && decoded.selectedTagIds.length === 0) {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Foto-Frage benötigt mindestens einen Tag." };
    }
    const inlinePhotos = decoded.photos.map((payload, idx) => ({ payload, selectedTagIds: decoded.selectedTagIds, orderIndex: idx }));
    return {
      ...empty,
      answerStatus: decoded.photos.length > 0 ? "answered" : "unanswered",
      valueJson: { raw: decoded },
      inlinePhotos,
    };
  }

  if (questionType === "matrix") {
    const subtype = String(config.matrixSubtype ?? "toggle");
    const allowedRows = asStringArray(config.rows);
    const allowedCols = asStringArray(config.columns);
    if (subtype === "toggle") {
      if (!Array.isArray(rawAnswer) || rawAnswer.some((v) => typeof v !== "string")) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Matrix-Auswahl muss String-Array sein." };
      }
      const uniqueCells = normalizeUnique(rawAnswer);
      const matrixCells = uniqueCells.map((cell, idx) => {
        const parsedKey = parseMatrixCellKey(String(cell));
        const rowKey = parsedKey.rowKey;
        const columnKey = parsedKey.columnKey;
        if ((allowedRows.length > 0 && !allowedRows.includes(rowKey)) || (allowedCols.length > 0 && !allowedCols.includes(columnKey))) {
          return null;
        }
        return { rowKey, columnKey, cellValueText: null, cellValueDate: null, cellSelected: true, orderIndex: idx };
      }).filter(Boolean) as Array<{ rowKey: string; columnKey: string; cellValueText: string | null; cellValueDate: string | null; cellSelected: boolean | null; orderIndex: number }>;
      if (matrixCells.length !== uniqueCells.length) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Matrix enthält ungültige Koordinaten." };
      }
      return { ...empty, answerStatus: matrixCells.length > 0 ? "answered" : "unanswered", valueJson: { raw: rawAnswer }, matrixCells };
    }
    if (typeof rawAnswer !== "string") {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Matrix erwartet JSON-String." };
    }
    try {
      const parsed = JSON.parse(rawAnswer) as Record<string, string>;
      const entries = Object.entries(parsed);
      const matrixCells = entries.map(([cellKey, val], idx) => {
        const parsedKey = parseMatrixCellKey(String(cellKey));
        const rowKey = parsedKey.rowKey;
        const columnKey = parsedKey.columnKey;
        if ((allowedRows.length > 0 && !allowedRows.includes(rowKey)) || (allowedCols.length > 0 && !allowedCols.includes(columnKey))) {
          return {
            rowKey,
            columnKey,
            cellValueText: null,
            cellValueDate: null,
            cellSelected: null,
            orderIndex: idx,
          };
        }
        if (subtype === "datum") {
          const dateIso = parseDateCellValue(String(val));
          return {
            rowKey,
            columnKey,
            cellValueText: null,
            cellValueDate: dateIso,
            cellSelected: null,
            orderIndex: idx,
          };
        }
        return {
          rowKey,
          columnKey,
          cellValueText: String(val),
          cellValueDate: null,
          cellSelected: null,
          orderIndex: idx,
        };
      });
      if (matrixCells.some((cell) => cell.cellValueDate == null && cell.cellValueText == null && cell.cellSelected == null)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Matrix enthält ungültige Koordinaten." };
      }
      if (subtype === "datum" && matrixCells.some((c) => c.cellValueDate == null && c.cellValueText == null)) {
        return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Mindestens ein Matrix-Datum ist ungültig." };
      }
      return { ...empty, answerStatus: matrixCells.length > 0 ? "answered" : "unanswered", valueJson: { raw: parsed }, matrixCells };
    } catch {
      return { ...empty, answerStatus: "invalid", isValid: false, validationError: "Matrix-JSON ungültig." };
    }
  }

  return { ...empty, answerStatus: "invalid", isValid: false, validationError: `Unbekannter Fragetyp: ${questionType}` };
}

export function extractRuleAnswerValue(answer: RequiredCheckAnswer | undefined): string | string[] | undefined {
  if (!answer) return undefined;
  const raw = (answer.valueJson as { raw?: unknown } | null | undefined)?.raw;
  if (Array.isArray(raw)) {
    return raw.filter((entry): entry is string => typeof entry === "string");
  }
  if (typeof raw === "string") return raw;
  if (raw && typeof raw === "object") {
    return JSON.stringify(raw);
  }
  if (typeof answer.valueText === "string" && answer.valueText.length > 0) return answer.valueText;
  if (answer.valueNumber != null) return String(answer.valueNumber);
  return undefined;
}

export function computeMissingRequiredQuestions(input: {
  questions: RequiredCheckQuestion[];
  answers: RequiredCheckAnswer[];
  photos: RequiredCheckPhoto[];
  photoTags: RequiredCheckPhotoTag[];
}): {
  hiddenQuestionIds: Set<string>;
  missingRequired: MissingRequiredQuestion[];
} {
  const answersByQuestionId = new Map(input.answers.map((row) => [row.visitSessionQuestionId, row]));
  const photosByAnswerId = new Map<string, RequiredCheckPhoto[]>();
  for (const row of input.photos) {
    const bucket = photosByAnswerId.get(row.visitAnswerId) ?? [];
    bucket.push(row);
    photosByAnswerId.set(row.visitAnswerId, bucket);
  }
  const tagsByPhotoId = new Map<string, RequiredCheckPhotoTag[]>();
  for (const row of input.photoTags) {
    const bucket = tagsByPhotoId.get(row.visitAnswerPhotoId) ?? [];
    bucket.push(row);
    tagsByPhotoId.set(row.visitAnswerPhotoId, bucket);
  }

  const applicableQuestions = input.questions.filter((question) => question.appliesToMarketChainSnapshot);
  const visibilityQuestions = applicableQuestions.map((question) => ({
    id: question.id,
    questionId: question.questionId,
    rules: (question.questionRulesSnapshot ?? []) as Array<Record<string, unknown>>,
  }));
  const answerByVisitQuestionId = new Map<string, string | string[] | undefined>();
  for (const question of applicableQuestions) {
    answerByVisitQuestionId.set(
      question.id,
      extractRuleAnswerValue(answersByQuestionId.get(question.id)),
    );
  }
  const hiddenQuestionIds = computeHiddenQuestionIds(visibilityQuestions, answerByVisitQuestionId);

  const missingRequired = applicableQuestions
    .filter((q) => q.requiredSnapshot && !hiddenQuestionIds.has(q.id))
    .filter((q) => {
      const answer = answersByQuestionId.get(q.id);
      if (!answer) return true;
      if (q.questionType !== "photo") return answer.answerStatus !== "answered" || !answer.isValid;
      const photos = photosByAnswerId.get(answer.id) ?? [];
      if (photos.length === 0) return true;
      const cfg = (q.questionConfigSnapshot ?? {}) as Record<string, unknown>;
      const tagsEnabled = Boolean(cfg.tagsEnabled) && Array.isArray(cfg.tagIds) && (cfg.tagIds as unknown[]).length > 0;
      const persistedStorage = Array.isArray((answer.valueJson as { storage?: unknown } | null)?.storage)
        ? ((answer.valueJson as { storage?: Array<{ path?: unknown }> }).storage ?? [])
            .map((entry) => (typeof entry.path === "string" ? entry.path : ""))
            .filter((entry) => entry.length > 0)
        : [];
      const currentPhotoPaths = photos.filter((photo) => !photo.inherited).map((photo) => photo.storagePath);
      if (persistedStorage.length !== currentPhotoPaths.length) return true;
      const persistedSet = new Set(persistedStorage);
      if (currentPhotoPaths.some((path) => !persistedSet.has(path))) return true;
      if (!tagsEnabled) return false;
      const missingPerPhotoTag = photos.some((photo) => (tagsByPhotoId.get(photo.id) ?? []).length === 0);
      if (missingPerPhotoTag) return true;
      const tagsCount = photos.reduce((sum, photo) => sum + ((tagsByPhotoId.get(photo.id) ?? []).length), 0);
      return tagsCount === 0;
    })
    .map((q) => ({
      visitQuestionId: q.id,
      questionId: q.questionId,
      questionText: q.questionTextSnapshot,
      questionType: q.questionType,
    }));

  return { hiddenQuestionIds, missingRequired };
}
