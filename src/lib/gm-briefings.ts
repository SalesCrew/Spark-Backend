import { createHash } from "node:crypto";
import { PDFDocument } from "pdf-lib";
import { z } from "zod";

export const BRIEFING_BUCKET = "gm-weekly-briefings";
export const MAX_PDF_BYTES = 25 * 1024 * 1024;
export const MAX_PDF_PAGES = 80;
export type BriefingFile = {
  path: string;
  name: string;
  byteSize: number;
  sha256: string;
  pageCount: number;
  dimensions: { width: number; height: number }[];
};
export type BriefingPage = {
  pageNumber: number;
  title: string;
  description: string;
};
export type BriefingSnapshot = {
  title: string;
  description: string;
  file: BriefingFile;
  pages: BriefingPage[];
};
export function briefingError(message: string, status = 400) {
  return Object.assign(new Error(message), { status });
}
export function viennaDate(now: Date) {
  return new Intl.DateTimeFormat("sv-SE", {
    timeZone: "Europe/Vienna",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(now);
}
const calendarDate = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}$/)
  .refine(
    (value) =>
      !Number.isNaN(Date.parse(value)) &&
      new Date(value).toISOString().slice(0, 10) === value,
    "Ungültiges Datum",
  );
export const briefingDraftSchema = z
  .object({
    expectedRevision: z.number().int().positive(),
    title: z.string().trim().min(1).max(160),
    description: z.string().max(2000),
    visibleFrom: calendarDate.nullable(),
    visibleUntil: calendarDate.nullable(),
    pages: z
      .array(
        z
          .object({
            pageNumber: z.number().int().positive(),
            title: z.string().trim().max(160),
            description: z.string().max(4000),
          })
          .strict(),
      )
      .min(1)
      .max(MAX_PDF_PAGES),
  })
  .strict()
  .refine(
    (value) =>
      !value.visibleFrom ||
      !value.visibleUntil ||
      value.visibleUntil >= value.visibleFrom,
    {
      message: "Das Enddatum liegt vor dem Startdatum.",
      path: ["visibleUntil"],
    },
  );
export type BriefingDraftInput = z.infer<typeof briefingDraftSchema>;
export async function inspectBriefingPdf(
  bytes: Buffer,
  filename: string,
): Promise<Omit<BriefingFile, "path">> {
  if (!bytes.length || bytes.length > MAX_PDF_BYTES)
    throw briefingError("Bitte eine PDF mit maximal 25 MB wählen.");
  if (!bytes.subarray(0, 1024).includes(Buffer.from("%PDF-")))
    throw briefingError("Die Datei ist keine gültige PDF.");
  let pdf: PDFDocument;
  try {
    pdf = await PDFDocument.load(bytes, { updateMetadata: false });
  } catch {
    throw briefingError(
      "Diese PDF kann nicht geöffnet werden. Bitte ohne Kennwort erneut exportieren.",
    );
  }
  const pages = pdf.getPages();
  if (pdf.isEncrypted || !pages.length || pages.length > MAX_PDF_PAGES)
    throw briefingError(
      "Bitte eine unverschlüsselte PDF mit 1 bis 80 Seiten wählen.",
    );
  const dimensions = pages.map((page) => ({
    width: page.getWidth(),
    height: page.getHeight(),
  }));
  if (
    dimensions.some(
      (page) =>
        !Number.isFinite(page.width) ||
        !Number.isFinite(page.height) ||
        page.width < 1 ||
        page.height < 1 ||
        page.width > 14400 ||
        page.height > 14400,
    )
  )
    throw briefingError("Die PDF enthält ein ungültiges Seitenformat.");
  const name =
    filename.replace(/[\x00-\x1f/\\]/g, "").slice(0, 180) ||
    "Wochenschwerpunkte.pdf";
  return {
    name,
    byteSize: bytes.length,
    sha256: createHash("sha256").update(bytes).digest("hex"),
    pageCount: pages.length,
    dimensions,
  };
}
