import { createHash } from "node:crypto";
import { and, eq, inArray, asc, sql } from "drizzle-orm";
import type { db as appDatabase } from "./db.js";
import { timeTrackingEntries as entries, timeTrackingEntryPhotos as photos, timeTrackingEntryPhotoTags as links, photoTags } from "./schema.js";

export const TIME_ENTRY_PHOTO_BUCKET = "gm-time-entry-photos";
export const MAX_TIME_ENTRY_PHOTO_BYTES = 10 * 1024 * 1024;
export type PhotoActor = { id: string; role: string };
export type TimeEntryPhotoStorage = { put(path: string, bytes: Buffer, mime: string): Promise<void>; sign(path: string): Promise<string> };
export function photoError(message: string, status = 400) { return Object.assign(new Error(message), { status }); }
export function inspectTimeEntryPhoto(bytes: Buffer) {
  if (!bytes.length || bytes.length > MAX_TIME_ENTRY_PHOTO_BYTES) throw photoError("Ein Foto darf maximal 10 MB groß sein.", 413);
  let mime = "", ext = "";
  if (bytes[0] === 255 && bytes[1] === 216 && bytes[2] === 255) { mime = "image/jpeg"; ext = "jpg"; }
  else if (bytes.subarray(0,8).equals(Buffer.from([137,80,78,71,13,10,26,10]))) { mime = "image/png"; ext = "png"; }
  else if (bytes.toString("ascii",0,4) === "RIFF" && bytes.toString("ascii",8,12) === "WEBP") { mime = "image/webp"; ext = "webp"; }
  else if (bytes.toString("ascii",4,8) === "ftyp") {
    const brands = bytes.toString("ascii",8,Math.min(bytes.length,64));
    if (/heic|heix|hevc|hevx/.test(brands)) { mime = "image/heic"; ext = "heic"; }
    else if (/mif1|msf1/.test(brands) && !/avif|avis/.test(brands)) { mime = "image/heif"; ext = "heif"; }
  }
  if (!mime) throw photoError("Bitte ein JPG-, PNG-, WebP- oder HEIC-Foto auswählen.", 415);
  return { mime, ext, sha256: createHash("sha256").update(bytes).digest("hex") };
}
export function createTimeEntryPhotoService(database: typeof appDatabase, storage: TimeEntryPhotoStorage) {
  async function entry(id: string, actor: PhotoActor, connection = database) {
    if (!["gm", "admin"].includes(actor.role)) throw photoError("Keine Berechtigung für diese Fotos.",403);
    const [row] = await connection.select().from(entries).where(and(eq(entries.id,id),eq(entries.isDeleted,false)));
    if (!row || row.activityType !== "sonderaufgabe") throw photoError("Sondereinsatz nicht gefunden.",404);
    if (actor.role !== "admin" && row.gmUserId !== actor.id) throw photoError("Keine Berechtigung für diesen Sondereinsatz.",403);
    if (row.status === "cancelled") throw photoError("Dieser Sondereinsatz wurde abgebrochen.",409);
    return row;
  }
  async function availableTags() {
    return database.select({ id: photoTags.id, label: photoTags.label }).from(photoTags).where(eq(photoTags.isDeleted,false)).orderBy(asc(photoTags.label));
  }
  async function list(entryId: string, actor: PhotoActor) {
    const context = await entry(entryId,actor);
    const rows = await database.select().from(photos).where(and(eq(photos.entryId,entryId),eq(photos.isDeleted,false))).orderBy(asc(photos.uploadedAt));
    const [tagRows, pool] = await Promise.all([
      rows.length ? database.select().from(links).where(and(inArray(links.photoId,rows.map(r=>r.id)),eq(links.isDeleted,false))) : Promise.resolve([]),
      availableTags(),
    ]);
    return { photos: await Promise.all(rows.map(async row => ({ id: row.id, name: row.originalFileName, url: await storage.sign(row.storagePath), uploadedAt: row.uploadedAt.toISOString(), revision: row.revision,
      tags: tagRows.filter(t=>t.photoId===row.id).map(t=>({ id:t.photoTagId ?? t.id, label:t.labelSnapshot })) }))), availableTags: pool, canEdit: true, entry: { id:context.id,status:context.status } };
  }
  async function upload(entryId: string, actor: PhotoActor, id: string, bytes: Buffer, fileName: string) {
    await entry(entryId,actor);
    const info = inspectTimeEntryPhoto(bytes);
    const [prior] = await database.select().from(photos).where(eq(photos.id,id));
    if (prior) {
      if (prior.entryId !== entryId || prior.sha256 !== info.sha256 || prior.isDeleted) throw photoError("Diese Foto-ID wurde bereits verwendet.",409);
      return list(entryId,actor);
    }
    const path = `${entryId}/${id}-${info.sha256}.${info.ext}`;
    await storage.put(path,bytes,info.mime);
    await database.transaction(async tx => {
      await entry(entryId,actor,tx as unknown as typeof database);
      await tx.insert(photos).values({ id,entryId,storagePath:path,originalFileName:fileName.replace(/[<>:"/\\|?*\x00-\x1F]/g,"_").slice(0,180)||`Foto.${info.ext}`,mimeType:info.mime,byteSize:bytes.length,sha256:info.sha256,uploadedByUserId:actor.id }).onConflictDoNothing();
      const [saved] = await tx.select().from(photos).where(eq(photos.id,id));
      if (!saved || saved.entryId !== entryId || saved.sha256 !== info.sha256 || saved.isDeleted) throw photoError("Diese Foto-ID wurde bereits verwendet.",409);
    });
    return list(entryId,actor);
  }
  async function mutate(entryId: string, photoId: string, actor: PhotoActor, expectedRevision: number, tagIds: string[] | null) {
    await database.transaction(async tx => {
      await entry(entryId,actor,tx as unknown as typeof database);
      const [photo] = await tx.select().from(photos).where(and(eq(photos.id,photoId),eq(photos.entryId,entryId),eq(photos.isDeleted,false))).for("update");
      if (!photo) throw photoError("Foto nicht gefunden.",404);
      if (photo.revision !== expectedRevision) throw photoError("Das Foto wurde inzwischen geändert. Bitte neu öffnen.",409);
      const now = new Date();
      if (tagIds === null) {
        // Preserve both the original bytes and their historical metadata.
        await tx.update(photos).set({isDeleted:true,deletedAt:now,deletedByUserId:actor.id,updatedAt:now,revision:sql`${photos.revision}+1`}).where(eq(photos.id,photoId));
        return;
      }
      const selected = [...new Set(tagIds)];
      const old = await tx.select().from(links).where(and(eq(links.photoId,photoId),eq(links.isDeleted,false)));
      const pool = selected.length ? await tx.select().from(photoTags).where(inArray(photoTags.id,selected)) : [];
      for (const id of selected) if (!old.some(l=>(l.photoTagId??l.id)===id) && !pool.some(t=>t.id===id&&!t.isDeleted)) throw photoError("Ein ausgewählter Tag ist nicht mehr verfügbar.",409);
      for (const tag of old) if (!selected.includes(tag.photoTagId??tag.id)) await tx.update(links).set({isDeleted:true,deletedAt:now}).where(eq(links.id,tag.id));
      for (const id of selected) if (!old.some(l=>(l.photoTagId??l.id)===id)) {
        const tag = pool.find(t=>t.id===id)!;
        await tx.insert(links).values({photoId,photoTagId:id,labelSnapshot:tag.label,createdByUserId:actor.id});
      }
      await tx.update(photos).set({updatedAt:now,revision:sql`${photos.revision}+1`}).where(eq(photos.id,photoId));
    });
    return list(entryId,actor);
  }
  return { availableTags,list,upload,setTags:(entryId:string,photoId:string,actor:PhotoActor,revision:number,ids:string[])=>mutate(entryId,photoId,actor,revision,ids),remove:(entryId:string,photoId:string,actor:PhotoActor,revision:number)=>mutate(entryId,photoId,actor,revision,null) };
}
export type TimeEntryPhotoService = ReturnType<typeof createTimeEntryPhotoService>;
