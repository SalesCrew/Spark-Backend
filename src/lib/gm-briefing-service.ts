import { randomUUID } from "node:crypto";
import { and, desc, eq, isNull, lte, sql } from "drizzle-orm";
import type { db as appDatabase } from "./db.js";
import {
  gmWeeklyBriefings as drafts,
  gmWeeklyBriefingPublications as publications,
  gmWeeklyBriefingReads as reads,
} from "./schema.js";
import {
  briefingError,
  inspectBriefingPdf,
  viennaDate,
  type BriefingDraftInput,
} from "./gm-briefings.js";

type Database = typeof appDatabase;
export type BriefingStorage = {
  upload(path: string, bytes: Buffer): Promise<void>;
  remove(path: string): Promise<void>;
  sign(path: string): Promise<string>;
};
export function createBriefingService(
  database: Database,
  storage: BriefingStorage,
  clock: () => Date = () => new Date(),
) {
  async function get(id: string) {
    const [row] = await database.select().from(drafts).where(eq(drafts.id, id));
    if (!row) throw briefingError("Wochenschwerpunkt nicht gefunden.", 404);
    return row;
  }
  async function list() {
    const rows = await database
      .select()
      .from(drafts)
      .orderBy(desc(drafts.updatedAt));
    const versions = await database
      .select({
        id: publications.id,
        briefingId: publications.briefingId,
        version: publications.version,
        sourceDraftRevision: publications.sourceDraftRevision,
        visibleFrom: publications.visibleFrom,
        visibleUntil: publications.visibleUntil,
        publishedAt: publications.publishedAt,
      })
      .from(publications)
      .orderBy(desc(publications.version));
    return rows.map((row) => ({
      ...row,
      publications: versions.filter((p) => p.briefingId === row.id),
    }));
  }
  async function upload(
    bytes: Buffer,
    filename: string,
    actor: string,
    operationId: string,
    replace?: { id: string; expectedRevision: number },
  ) {
    const fileInfo = await inspectBriefingPdf(bytes, filename);
    if (!replace) {
      const prior = await database
        .select()
        .from(drafts)
        .where(eq(drafts.id, operationId));
      if (prior[0]) {
        if (
          prior[0].createdByUserId !== actor ||
          prior[0].file.sha256 !== fileInfo.sha256
        )
          throw briefingError("Diese Upload-ID wurde bereits verwendet.", 409);
        return prior[0];
      }
    }
    if (replace) {
      const current = await get(replace.id);
      if (current.draftRevision !== replace.expectedRevision)
        throw briefingError(
          "Der Entwurf wurde inzwischen geändert. Bitte neu laden.",
          409,
        );
    }
    const id = replace?.id ?? operationId,
      path = `${id}/${randomUUID()}.pdf`,
      file = { ...fileInfo, path };
    await storage.upload(path, bytes);
    try {
      return await database
        .transaction(async (tx) => {
          if (replace) {
            const [current] = await tx
              .select()
              .from(drafts)
              .where(eq(drafts.id, id))
              .for("update");
            if (!current || current.draftRevision !== replace.expectedRevision)
              throw briefingError(
                "Der Entwurf wurde inzwischen geändert. Bitte neu laden.",
                409,
              );
            const pages = Array.from({ length: file.pageCount }, (_, i) => ({
              pageNumber: i + 1,
              title: current.pages[i]?.title ?? "",
              description: current.pages[i]?.description ?? "",
            }));
            const [row] = await tx
              .update(drafts)
              .set({
                file,
                pages,
                draftRevision: current.draftRevision + 1,
                updatedAt: clock(),
              })
              .where(eq(drafts.id, id))
              .returning();
            return row!;
          }
          const [row] = await tx
            .insert(drafts)
            .values({
              id,
              file,
              pages: Array.from({ length: file.pageCount }, (_, i) => ({
                pageNumber: i + 1,
                title: "",
                description: "",
              })),
              title: file.name.replace(/\.pdf$/i, "").trim() || "Wochenschwerpunkte",
              createdByUserId: actor,
              createdAt: clock(),
              updatedAt: clock(),
            })
            .onConflictDoNothing()
            .returning();
          if (row) return row;
          // Concurrent retry: preserve the winning immutable object, remove only this attempt's unused object.
          const [prior] = await tx
            .select()
            .from(drafts)
            .where(eq(drafts.id, id));
          if (
            !prior ||
            prior.createdByUserId !== actor ||
            prior.file.sha256 !== file.sha256
          )
            throw briefingError(
              "Diese Upload-ID wurde bereits verwendet.",
              409,
            );
          return prior;
        })
        .then(async (row) => {
          if (row.file.path !== path)
            await storage.remove(path).catch(() => {});
          return row;
        });
    } catch (error) {
      await storage.remove(path).catch(() => {});
      throw error;
    }
  }
  async function save(id: string, input: BriefingDraftInput) {
    return database.transaction(async (tx) => {
      const [current] = await tx
        .select()
        .from(drafts)
        .where(eq(drafts.id, id))
        .for("update");
      if (!current)
        throw briefingError("Wochenschwerpunkt nicht gefunden.", 404);
      if (current.draftRevision !== input.expectedRevision)
        throw briefingError(
          "Der Entwurf wurde inzwischen geändert. Bitte neu laden.",
          409,
        );
      if (
        input.pages.length !== current.file.pageCount ||
        input.pages.some((p, i) => p.pageNumber !== i + 1)
      )
        throw briefingError(
          "Die Seiten müssen vollständig und in PDF-Reihenfolge bleiben.",
        );
      const { expectedRevision: _revision, ...fields } = input;
      const [row] = await tx
        .update(drafts)
        .set({
          ...fields,
          draftRevision: current.draftRevision + 1,
          updatedAt: clock(),
        })
        .where(eq(drafts.id, id))
        .returning();
      return row!;
    });
  }
  async function publish(
    id: string,
    expectedRevision: number,
    operationId: string,
    actor: string,
  ) {
    return database.transaction(async (tx) => {
      const [current] = await tx
        .select()
        .from(drafts)
        .where(eq(drafts.id, id))
        .for("update");
      if (!current)
        throw briefingError("Wochenschwerpunkt nicht gefunden.", 404);
      const [prior] = await tx
        .select()
        .from(publications)
        .where(eq(publications.operationId, operationId));
      if (prior) {
        if (
          prior.briefingId !== id ||
          prior.sourceDraftRevision !== expectedRevision ||
          prior.publishedByUserId !== actor
        )
          throw briefingError(
            "Diese Veröffentlichungs-ID wurde bereits verwendet.",
            409,
          );
        return prior;
      }
      if (current.archivedAt)
        throw briefingError("Bitte zuerst aus dem Archiv holen.", 409);
      if (current.draftRevision !== expectedRevision)
        throw briefingError(
          "Der Entwurf wurde inzwischen geändert. Bitte neu laden.",
          409,
        );
      const visibleFrom = current.visibleFrom ?? viennaDate(clock());
      if (current.visibleUntil && current.visibleUntil < visibleFrom)
        throw briefingError(
          "Der Veröffentlichungszeitraum ist bereits vorbei.",
        );
      const [last] = await tx
        .select()
        .from(publications)
        .where(eq(publications.briefingId, id))
        .orderBy(desc(publications.version))
        .limit(1);
      if (last?.sourceDraftRevision === expectedRevision) return last;
      const [publication] = await tx
        .insert(publications)
        .values({
          briefingId: id,
          version: (last?.version ?? 0) + 1,
          sourceDraftRevision: expectedRevision,
          operationId,
          publishedByUserId: actor,
          visibleFrom,
          visibleUntil: current.visibleUntil,
          publishedAt: clock(),
          snapshot: {
            title: current.title,
            description: current.description,
            file: current.file,
            pages: current.pages,
          },
        })
        .returning();
      return publication!;
    });
  }
  async function archive(id: string, archived: boolean) {
    await get(id);
    const [row] = await database
      .update(drafts)
      .set({ archivedAt: archived ? clock() : null, updatedAt: clock() })
      .where(eq(drafts.id, id))
      .returning();
    return row!;
  }
  async function active() {
    const today = viennaDate(clock());
    // Pick the newest started version BEFORE checking its end date; older versions never reappear after expiry.
    const rows = await database
      .selectDistinctOn([publications.briefingId], {
        publication: publications,
      })
      .from(publications)
      .innerJoin(drafts, eq(drafts.id, publications.briefingId))
      .where(
        and(isNull(drafts.archivedAt), lte(publications.visibleFrom, today)),
      )
      .orderBy(publications.briefingId, desc(publications.version));
    return rows
      .map((row) => row.publication)
      .filter((row) => !row.visibleUntil || row.visibleUntil >= today)
      .sort((a, b) => b.publishedAt.getTime() - a.publishedAt.getTime());
  }
  async function activePublication(id: string) {
    const row = (await active()).find((p) => p.id === id);
    if (!row)
      throw briefingError(
        "Dieser Wochenschwerpunkt ist derzeit nicht verfügbar.",
        404,
      );
    return row;
  }
  async function inbox(userId: string) {
    const [visible, receipts] = await Promise.all([
      active(),
      database.select().from(reads).where(eq(reads.gmUserId, userId)),
    ]);
    const items = visible.map((p) => {
      const receipt = receipts.find((r) => r.publicationId === p.id);
      const { file: _file, ...content } = p.snapshot;
      return {
        id: p.id,
        briefingId: p.briefingId,
        version: p.version,
        visibleFrom: p.visibleFrom,
        visibleUntil: p.visibleUntil,
        ...content,
        pageCount: p.snapshot.file.pageCount,
        seenPages: receipt?.seenPages ?? [],
        readAt: receipt?.readAt ?? null,
      };
    });
    return { items, unreadCount: items.filter((item) => !item.readAt).length };
  }
  async function file(id: string) {
    const p = await activePublication(id);
    return {
      url: await storage.sign(p.snapshot.file.path),
      pageCount: p.snapshot.file.pageCount,
    };
  }
  async function preview(id: string) {
    const row = await get(id);
    return {
      url: await storage.sign(row.file.path),
      revision: row.draftRevision,
    };
  }
  async function read(id: string, userId: string, pageNumber: number) {
    // A publication that is archived/expired between display and acknowledgement must not be marked as a different version.
    const p = await activePublication(id);
    if (
      !Number.isInteger(pageNumber) ||
      pageNumber < 1 ||
      pageNumber > p.snapshot.file.pageCount
    )
      throw briefingError("Ungültige Seite.");
    const merged = sql`array(select distinct page from unnest(${reads.seenPages} || ARRAY[${pageNumber}]::integer[]) page order by page)`;
    const now = clock();
    const [row] = await database
      .insert(reads)
      .values({
        publicationId: id,
        gmUserId: userId,
        seenPages: [pageNumber],
        readAt: p.snapshot.file.pageCount === 1 ? now : null,
        updatedAt: now,
      })
      .onConflictDoUpdate({
        target: [reads.publicationId, reads.gmUserId],
        set: {
          seenPages: merged,
          readAt: sql`coalesce(${reads.readAt}, case when cardinality(${merged}) = ${p.snapshot.file.pageCount} then ${now.toISOString()}::timestamptz else null end)`,
          updatedAt: now,
        },
      })
      .returning();
    return row!;
  }
  return {
    get,
    list,
    upload,
    save,
    publish,
    archive,
    inbox,
    file,
    preview,
    read,
  };
}
export type BriefingService = ReturnType<typeof createBriefingService>;
