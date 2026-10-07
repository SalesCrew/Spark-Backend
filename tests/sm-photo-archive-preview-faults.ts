import type { Express } from "express";

// Imported exclusively by the disposable preview, never by src/app or production.
export function installSmPhotoArchivePreviewFaults(app: Express) {
  const modes = ["normal", "list-error", "facets-error", "urls-error", "missing-url", "broken-image", "slow-standard", "many-photos"] as const;
  let mode: typeof modes[number] = "normal", pending = 0;
  let pageSource: Array<Record<string, unknown>> = [];
  const requests: Array<{ path: string; scope: string }> = [];
  app.post("/__preview/sm-photo-archive-control", (req, res) => {
    if (req.get("authorization") !== "Bearer synthetic-sm-admin") { res.sendStatus(403); return; }
    if (!modes.includes(req.body.mode)) { res.sendStatus(400); return; }
    mode = req.body.mode; requests.length = 0; pageSource = []; res.json({ mode });
  });
  app.get("/__preview/sm-photo-archive-control", (req, res) => {
    if (req.get("authorization") !== "Bearer synthetic-sm-admin") { res.sendStatus(403); return; }
    res.json({ mode, pending, requests });
  });
  app.use("/admin/sm-photos", (req, res, next) => {
    const requestMode = mode;
    requests.push({ path: req.path, scope: String(req.query.SMDurcharbeitCatalogScope ?? "all") });
    if (requests.length > 100) requests.shift();
    if ((requestMode === "list-error" && req.path === "/") || (requestMode === "facets-error" && req.path === "/facets") || (requestMode === "urls-error" && req.path === "/signed-urls")) {
      res.status(503).json({ error: "Synthetischer Ausfall für die isolierte Prüfung." }); return;
    }
    if (req.path === "/signed-urls" && ["missing-url", "broken-image"].includes(requestMode)) {
      const send = res.json.bind(res);
      res.json = body => send({ ...body, photos: requestMode === "missing-url" ? body.photos.slice(1) : body.photos.map((photo: object) => ({ ...photo, signedUrl: "http://127.0.0.1:4037/synthetic-photo-storage/object?path=synthetic-missing" })) });
    }
    // Exercise paging and a shrinking result set without creating or deleting rows.
    if (req.path === "/" && requestMode === "many-photos") {
      const send = res.json.bind(res);
      res.json = body => {
        if (!Array.isArray(body.photos)) return send(body);
        if (body.photos.length) pageSource = body.photos;
        if (!pageSource.length) return send(body);
        const page = Number(req.query.page ?? 1), pageSize = Number(req.query.pageSize ?? 30);
        const photos = Array.from({ length: 61 }, (_, index) => ({ ...pageSource[index % pageSource.length],
          id: `00000000-0000-4000-8000-${(index + 1).toString(16).padStart(12, "0")}`, fileName: `Synthetic_paging_${index + 1}.png` }));
        return send({ ...body, total: 61, photos: photos.slice((page - 1) * pageSize, page * pageSize) });
      };
    }
    if (requestMode === "slow-standard" && req.path === "/" && req.query.SMDurcharbeitCatalogScope === "standard") {
      pending++; setTimeout(() => { pending--; next(); }, 1500); return;
    }
    next();
  });
}
