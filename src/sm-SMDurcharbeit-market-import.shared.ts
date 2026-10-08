import { createHash } from 'node:crypto';
import { z } from 'zod';

export const SMDurcharbeitImportFields = ['SMDurcharbeitVertriebstyp', 'name', 'address', 'postalCode', 'city', 'SMDurcharbeitEmEh', 'shelfMerchandiserName'] as const;
export const SMDurcharbeitImportHeaders = ['Vertriebstyp', 'Firma/Betrieb', 'Straße', 'PLZ', 'Ort', 'EM/EH', 'Verplanung'] as const;
const SMDurcharbeitMapping = z.object(Object.fromEntries(SMDurcharbeitImportFields.map(key => [key, z.string().regex(/^[A-Z]{1,3}$/i)])) as Record<typeof SMDurcharbeitImportFields[number], z.ZodString>).strict();
export const SMDurcharbeitImportSchema = z.object({
  fileName: z.string().trim().min(1).max(260), sheetName: z.string().trim().min(1).max(260),
  mapping: SMDurcharbeitMapping,
  rows: z.array(z.array(z.union([z.string().max(20_000), z.number(), z.boolean(), z.null()])).max(256)).min(2).max(20_001),
}).strict();
export function SMDurcharbeitNameKey(value: string) {
  return value.normalize('NFKD').replace(/[\u0300-\u036f]/g, '').toLocaleLowerCase('de-AT').replace(/ß/g, 'ss').split(/[^a-z0-9]+/).filter(Boolean).sort().join(' ');
}
export function prepareSMDurcharbeitImport(input: z.infer<typeof SMDurcharbeitImportSchema>) {
  const columns = SMDurcharbeitImportFields.map(key => input.mapping[key].toUpperCase());
  if (new Set(columns).size !== columns.length) throw new Error('Jede der sieben Spalten muss getrennt zugewiesen werden.');
  const indices = columns.map(c => [...c].reduce((value, letter) => value * 26 + letter.charCodeAt(0) - 64, 0) - 1);
  if (indices.some(index => index >= 256)) throw new Error('Eine zugewiesene Spalte liegt außerhalb der unterstützten Tabelle.');
  const occurrences = new Map<string, number>();
  return input.rows.slice(1).flatMap((row, index) => {
    if (!row.some(value => String(value ?? '').trim())) return [];
    const values = indices.map(column => String(row[column] ?? '').trim());
    const [chain, company, address, postalCode, city, emEh, planner] = values as [string,string,string,string,string,string,string];
    const error = !chain || !address || !postalCode || !city ? 'Vertriebstyp, Straße, PLZ und Ort müssen befüllt sein.'
      : !/^\d{4}$/.test(postalCode) ? 'PLZ muss vierstellig sein.'
      : [chain, company, city, planner].some(value => value.length > 500) || address.length > 1000 || emEh.length > 500 ? 'Ein Feld ist zu lang.' : null;
    const identity = [chain, address, postalCode, city].map(value => value.normalize('NFKC').toLocaleLowerCase('de-AT').replace(/\s+/g, ' '));
    const base = createHash('sha256').update(JSON.stringify(identity)).digest('hex');
    const occurrence = (occurrences.get(base) ?? 0) + 1; occurrences.set(base, occurrence);
    return [{ row: index + 2, error, sourceKey: `${base}:${occurrence}`, duplicate: occurrence > 1,
      chain, company, address, postalCode, city, emEh, planner,
      sourceValues: Object.fromEntries(SMDurcharbeitImportHeaders.map((header, i) => [header, values[i]!])),
      name: company || `${chain} · ${address}`, }];
  });
}
