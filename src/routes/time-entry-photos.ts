import { Router, raw, type RequestHandler } from "express";
import { z } from "zod";
import type { AuthedRequest } from "../middleware/auth.js";
import { MAX_TIME_ENTRY_PHOTO_BYTES, photoError, type TimeEntryPhotoService } from "../lib/time-entry-photos.js";

export function createTimeEntryPhotoRouter(service: TimeEntryPhotoService, requireAuth:(roles:("gm"|"admin")[])=>RequestHandler) {
  const router = Router(), uuid=z.string().uuid();
  const auth = requireAuth(["gm","admin"]);
  const handle=(fn:(req:AuthedRequest)=>Promise<unknown>):RequestHandler=>async(req,res,next)=>{
    res.setHeader("Cache-Control","private, no-store");
    try { res.json(await fn(req as AuthedRequest)); } catch(error) { next(error); }
  };
  const actor=(req:AuthedRequest)=>{if(!req.authUser?.appUserId)throw photoError("Bitte erneut anmelden.",401);return {id:req.authUser.appUserId,role:req.authUser.role};};
  const entryId=(req:AuthedRequest)=>uuid.parse(req.params.id),photoId=(req:AuthedRequest)=>uuid.parse(req.params.photoId);
  router.get("/photo-tags",auth,handle(async()=>({tags:await service.availableTags()})));
  router.get("/entries/:id/photos",auth,handle(req=>service.list(entryId(req),actor(req))));
  router.post("/entries/:id/photos",auth,raw({type:"image/*",limit:MAX_TIME_ENTRY_PHOTO_BYTES}),handle(req=>{
    if(!Buffer.isBuffer(req.body))throw photoError("Bitte ein Foto hochladen.",415);
    let name:string;try{name=decodeURIComponent(req.header("x-photo-filename")??"Foto");}catch{throw photoError("Ungültiger Dateiname.");}
    return service.upload(entryId(req),actor(req),uuid.parse(req.header("x-operation-id")),req.body,name);
  }));
  router.patch("/entries/:id/photos/:photoId",auth,handle(req=>{
    const input=z.object({tagIds:z.array(uuid).max(50),expectedRevision:z.number().int().positive()}).strict().parse(req.body);
    return service.setTags(entryId(req),photoId(req),actor(req),input.expectedRevision,input.tagIds);
  }));
  router.delete("/entries/:id/photos/:photoId",auth,handle(req=>{
    const input=z.object({expectedRevision:z.number().int().positive()}).strict().parse(req.body);
    return service.remove(entryId(req),photoId(req),actor(req),input.expectedRevision);
  }));
  return router;
}
