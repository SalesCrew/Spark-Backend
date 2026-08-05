import { randomUUID } from "node:crypto";
import type { Responses } from "openai/resources/responses/responses";
import { z } from "zod";

const EXPORT_MARKER_PATTERN = /\n?<!--admin-kurti-exports-v1:([A-Za-z0-9_-]+)-->\s*$/;

export const ADMIN_KURTI_EXCEL_EXPORT_KINDS = [
  "zeiterfassung", "zeitenaufstellung", "diaeten", "maerkte", "gebietsmanager",
  "shelf_merchandiser", "lager", "fragebogen_standard", "fragebogen_flex",
  "fragebogen_billa", "fragebogen_kuehler", "fragebogen_mhd",
  "fragebogen_durcharbeit", "fotoarchiv",
] as const;

const exportKindSchema = z.enum(ADMIN_KURTI_EXCEL_EXPORT_KINDS);
const optionalTextArraySchema = z.array(z.string().trim().min(1).max(160)).max(100);
const ymdSchema = z.string().regex(/^\d{4}-\d{2}-\d{2}$/).nullable();
const exportFiltersSchema = z.object({
  dateFrom: ymdSchema,
  dateTo: ymdSchema,
  gmUserIds: z.array(z.string().uuid()).max(100),
  gmNames: optionalTextArraySchema,
  regions: optionalTextArraySchema,
  campaignIds: z.array(z.string().uuid()).max(100),
  campaignNames: optionalTextArraySchema,
  marketIds: z.array(z.string().uuid()).max(200),
  marketSearch: z.string().trim().max(200).nullable(),
  sections: z.array(z.enum(["standard", "flex", "billa", "kuehler", "mhd", "durcharbeit"])).max(6),
  statuses: optionalTextArraySchema,
  search: z.string().trim().max(200).nullable(),
  includeLive: z.boolean(),
}).strict();

function validateRange(
  value: { kind: z.infer<typeof exportKindSchema>; filters: z.infer<typeof exportFiltersSchema> },
  context: z.RefinementCtx,
) {
  const { dateFrom, dateTo } = value.filters;
  if ((dateFrom === null) !== (dateTo === null)) {
    context.addIssue({ code: "custom", path: ["filters", "dateFrom"], message: "dateFrom and dateTo must be provided together." });
  }
  if (dateFrom && dateTo && dateFrom > dateTo) {
    context.addIssue({ code: "custom", path: ["filters", "dateTo"], message: "dateTo must not be before dateFrom." });
  }
  if (["zeiterfassung", "zeitenaufstellung", "diaeten"].includes(value.kind) && (!dateFrom || !dateTo)) {
    context.addIssue({ code: "custom", path: ["filters", "dateFrom"], message: "This export requires an explicit date range." });
  }
}

const exportFields = {
  kind: exportKindSchema,
  title: z.string().trim().min(1).max(100),
  description: z.string().trim().max(240).nullable(),
  filters: exportFiltersSchema,
};
const exportInputSchema = z.object(exportFields).strict().superRefine(validateRange);
export const adminKurtiExcelExportSchema = z.object({ id: z.string().uuid(), ...exportFields }).strict().superRefine(validateRange);
const exportListSchema = z.array(adminKurtiExcelExportSchema).max(3);

export type AdminKurtiExcelExport = z.infer<typeof adminKurtiExcelExportSchema>;

export const ADMIN_KURTI_EXPORT_TOOL: Responses.FunctionTool = {
  type: "function",
  name: "prepare_admin_excel_export",
  description: "Prepares an existing Coke Spark Excel export as a downloadable chat card. Resolve requested names and filters first. Time exports require explicit dateFrom/dateTo values.",
  strict: true,
  parameters: {
    type: "object",
    properties: {
      kind: { type: "string", enum: ADMIN_KURTI_EXCEL_EXPORT_KINDS },
      title: { type: "string", minLength: 1, maxLength: 100 },
      description: { type: ["string", "null"], maxLength: 240 },
      filters: {
        type: "object",
        properties: {
          dateFrom: { type: ["string", "null"], pattern: "^\\d{4}-\\d{2}-\\d{2}$" },
          dateTo: { type: ["string", "null"], pattern: "^\\d{4}-\\d{2}-\\d{2}$" },
          gmUserIds: { type: "array", maxItems: 100, items: { type: "string", format: "uuid" } },
          gmNames: { type: "array", maxItems: 100, items: { type: "string", minLength: 1, maxLength: 160 } },
          regions: { type: "array", maxItems: 100, items: { type: "string", minLength: 1, maxLength: 160 } },
          campaignIds: { type: "array", maxItems: 100, items: { type: "string", format: "uuid" } },
          campaignNames: { type: "array", maxItems: 100, items: { type: "string", minLength: 1, maxLength: 160 } },
          marketIds: { type: "array", maxItems: 200, items: { type: "string", format: "uuid" } },
          marketSearch: { type: ["string", "null"], maxLength: 200 },
          sections: { type: "array", maxItems: 6, items: { type: "string", enum: ["standard", "flex", "billa", "kuehler", "mhd", "durcharbeit"] } },
          statuses: { type: "array", maxItems: 100, items: { type: "string", minLength: 1, maxLength: 160 } },
          search: { type: ["string", "null"], maxLength: 200 },
          includeLive: { type: "boolean" },
        },
        required: ["dateFrom", "dateTo", "gmUserIds", "gmNames", "regions", "campaignIds", "campaignNames", "marketIds", "marketSearch", "sections", "statuses", "search", "includeLive"],
        additionalProperties: false,
      },
    },
    required: ["kind", "title", "description", "filters"],
    additionalProperties: false,
  },
};

export function parseAdminKurtiExcelExportArguments(rawArguments: string): AdminKurtiExcelExport {
  const input = exportInputSchema.parse(rawArguments.trim() ? JSON.parse(rawArguments) as unknown : {});
  return adminKurtiExcelExportSchema.parse({ id: randomUUID(), ...input });
}

export function appendAdminKurtiExcelExports(content: string, exports: AdminKurtiExcelExport[]): string {
  if (exports.length === 0) return content;
  const encoded = Buffer.from(JSON.stringify(exportListSchema.parse(exports)), "utf8").toString("base64url");
  return `${content.trimEnd()}\n<!--admin-kurti-exports-v1:${encoded}-->`;
}

export function parseAdminKurtiStoredExports(content: string): { content: string; exports: AdminKurtiExcelExport[] } {
  const match = content.match(EXPORT_MARKER_PATTERN);
  if (!match || typeof match.index !== "number") return { content, exports: [] };
  const visibleContent = content.slice(0, match.index).trimEnd();
  try {
    const decoded = Buffer.from(match[1]!, "base64url").toString("utf8");
    return { content: visibleContent, exports: exportListSchema.parse(JSON.parse(decoded) as unknown) };
  } catch {
    return { content: visibleContent, exports: [] };
  }
}
