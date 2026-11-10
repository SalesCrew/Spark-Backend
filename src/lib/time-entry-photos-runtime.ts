import { createHash } from "node:crypto";
import { db } from "./db.js";
import { supabaseAdmin } from "./supabase.js";
import { requireAuth } from "../middleware/auth.js";
import { createTimeEntryPhotoRouter } from "../routes/time-entry-photos.js";
import { createTimeEntryPhotoService, TIME_ENTRY_PHOTO_BUCKET, photoError } from "./time-entry-photos.js";

const storage = supabaseAdmin.storage.from(TIME_ENTRY_PHOTO_BUCKET);
export const timeEntryPhotoRouter = createTimeEntryPhotoRouter(createTimeEntryPhotoService(db, {
  async put(path,bytes,mime) {
    const { error } = await storage.upload(path,bytes,{contentType:mime,upsert:false});
    if (!error) return;
    if (String((error as {statusCode?:string}).statusCode) === "409" || /already exists|duplicate/i.test(error.message)) {
      const existing = await storage.download(path);
      if (!existing.error && existing.data && createHash("sha256").update(Buffer.from(await existing.data.arrayBuffer())).digest("hex") === createHash("sha256").update(bytes).digest("hex")) return;
    }
    throw photoError("Foto konnte nicht gespeichert werden. Bitte erneut versuchen.",503);
  },
  async sign(path) {
    const { data,error } = await storage.createSignedUrl(path,30*60);
    if(error||!data?.signedUrl)throw photoError("Foto konnte nicht geöffnet werden. Bitte erneut versuchen.",503);
    return data.signedUrl;
  },
}),requireAuth);
