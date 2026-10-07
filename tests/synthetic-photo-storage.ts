import { deflateSync } from "node:zlib";
import { Router, raw } from "express";

// Synthetic-only, loopback storage. It has no cloud client, credentials or production I/O.
export function syntheticShelfPhoto(blue: boolean) {
  const width = 640, height = 480, pixels = Buffer.alloc((width * 3 + 1) * height);
  for (let y = 0; y < height; y++) for (let x = 0; x < width; x++) {
    let color = [239, 241, 244];
    if (x > 45 && x < 595 && y > 35 && y < 425) color = [255, 255, 255];
    if (x > 55 && x < 585 && [143, 267, 391].some(shelf => y > shelf && y < shelf + 12)) color = [125, 137, 153];
    for (const top of [60, 184, 308]) {
      const column = Math.floor((x - 72) / 74), localX = (x - 72) % 74;
      if (column >= 0 && column < 7 && localX >= 0 && localX < 43 && y > top && y < top + 82) {
        color = blue ? [45 + column * 5, 101 + column * 6, 202] : [211, 44 + column * 5, 52];
        if (y > top + 31 && y < top + 51) color = [251, 250, 247];
      }
    }
    const offset = y * (width * 3 + 1) + 1 + x * 3;
    pixels[offset] = color[0]!; pixels[offset + 1] = color[1]!; pixels[offset + 2] = color[2]!;
  }
  const crc = (bytes: Buffer) => { let value = 0xffffffff; for (const byte of bytes) { value ^= byte; for (let bit = 0; bit < 8; bit++) value = (value >>> 1) ^ (value & 1 ? 0xedb88320 : 0); } return (value ^ 0xffffffff) >>> 0; };
  const chunk = (name: string, bytes: Buffer) => { const header = Buffer.alloc(4), footer = Buffer.alloc(4), kind = Buffer.from(name); header.writeUInt32BE(bytes.length); footer.writeUInt32BE(crc(Buffer.concat([kind, bytes]))); return Buffer.concat([header, kind, bytes, footer]); };
  const header = Buffer.alloc(13); header.writeUInt32BE(width); header.writeUInt32BE(height, 4); header[8] = 8; header[9] = 2;
  return Buffer.concat([Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]), chunk("IHDR", header), chunk("IDAT", deflateSync(pixels)), chunk("IEND", Buffer.alloc(0))]);
}

export function createSyntheticPhotoStorage() {
  const objects = new Map<string, { bytes: Buffer; mimeType: string }>(), issued = new Set<string>();
  const origin = "http://127.0.0.1:4037";
  const readUrl = (path: string) => `${origin}/synthetic-photo-storage/object?path=${encodeURIComponent(path)}`;
  const storage = { from: (bucket: string) => {
    if (bucket !== "sm-visit-photos") throw new Error("Synthetic storage only supports SM photos");
    return {
      createSignedUploadUrl: async (path: string) => { issued.add(path); return { data: { path, token: "synthetic-only", signedUrl: `${origin}/synthetic-photo-storage/upload?path=${encodeURIComponent(path)}` }, error: null }; },
      createSignedUrl: async (path: string) => ({ data: { signedUrl: objects.has(path) ? readUrl(path) : null }, error: null }),
      createSignedUrls: async (paths: string[]) => ({ data: paths.map(path => ({ path, signedUrl: objects.has(path) ? readUrl(path) : null, error: objects.has(path) ? null : "Not found" })), error: null }),
      list: async (folder: string) => ({ data: [...objects.keys()].filter(path => path.startsWith(folder + "/")).map(path => ({ name: path.slice(folder.length + 1) })), error: null }),
      info: async (path: string) => { const object = objects.get(path); return { data: object ? { size: object.bytes.length, contentType: object.mimeType } : null, error: object ? null : "Not found" }; },
      remove: async (paths: string[]) => { paths.forEach(path => objects.delete(path)); return { data: [], error: null }; },
    };
  } };
  const router = Router();
  router.get("/object", (req, res) => {
    const object = objects.get(String(req.query.path ?? ""));
    if (!object) { res.sendStatus(404); return; }
    res.set("Cache-Control", "private, no-store"); res.type(object.mimeType).send(object.bytes);
  });
  router.put("/upload", raw({ type: ["image/jpeg", "image/png", "image/webp"], limit: "20mb" }), (req, res) => {
    const path = String(req.query.path ?? "");
    if (!issued.has(path) || !Buffer.isBuffer(req.body)) { res.sendStatus(400); return; }
    objects.set(path, { bytes: req.body, mimeType: req.get("content-type") ?? "image/png" }); res.json({ ok: true });
  });
  return { storage, router, put: (path: string, bytes: Buffer) => { if (!issued.has(path)) throw new Error("Unissued synthetic path"); objects.set(path, { bytes, mimeType: "image/png" }); } };
}
