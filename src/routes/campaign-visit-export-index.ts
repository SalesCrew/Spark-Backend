import { Router } from "express";
import { sql } from "drizzle-orm";
import { z } from "zod";
import type { ModelDatabase } from "../lib/praemien-workspace.js";

// This router is mounted behind the existing campaign admin authorization.
// Keeping database injection explicit lets the real HTTP path run read-only
// against a disposable database without importing production configuration.
export function createCampaignVisitExportIndexRouter(database: ModelDatabase) {
  const router = Router();
  const day = z.string().regex(/^\d{4}-\d{2}-\d{2}$/).refine(value =>
    Number.isFinite(Date.parse(value)) && new Date(value).toISOString().slice(0, 10) === value);
  router.get("/campaigns/market-visit-export-index", async (req, res, next) => {
    try {
      const campaignIds = [...new Set(String(req.query.campaignIds ?? "").split(",").map(s => s.trim()).filter(Boolean))];
      if (!campaignIds.length || campaignIds.some(id => !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id))) {
        res.status(400).json({ error: "Ungültige Kampagnen-IDs.", code: "invalid_campaign_ids" }); return;
      }
      if (campaignIds.length > 50) {
        res.status(400).json({ error: "Maximal 50 Kampagnen pro Export-Anfrage erlaubt.", code: "campaign_export_index_batch_too_large" }); return;
      }
      const range = z.object({ dateFrom: day.optional(), dateTo: day.optional() })
        .refine(r => !r.dateFrom || !r.dateTo || r.dateFrom <= r.dateTo).safeParse({
          dateFrom: typeof req.query.dateFrom === "string" && req.query.dateFrom ? req.query.dateFrom : undefined,
          dateTo: typeof req.query.dateTo === "string" && req.query.dateTo ? req.query.dateTo : undefined,
        });
      if (!range.success) {
        res.status(400).json({ error: "Ungültiger Export-Zeitraum.", code: "invalid_campaign_export_date_range" }); return;
      }
      const rows = await database.query<{
        campaignId: string; marketId: string; sessionId: string; gmUserId: string | null;
        gmName: string | null; startedAt: string; submittedAt: string;
      }>(sql`
        select distinct sec.campaign_id as "campaignId",s.market_id as "marketId",s.id as "sessionId",
          s.gm_user_id as "gmUserId",
          case when u.first_name is not null and u.last_name is not null then trim(concat_ws(' ',u.first_name,u.last_name)) end as "gmName",
          coalesce(s.started_at,s.submitted_at)::text as "startedAt",s.submitted_at::text as "submittedAt"
        from visit_session_sections sec
        join visit_sessions s on s.id=sec.visit_session_id
        join campaigns c on c.id=sec.campaign_id
        left join users u on u.id=s.gm_user_id
        where sec.campaign_id in (select jsonb_array_elements_text(${JSON.stringify(campaignIds)}::jsonb)::uuid)
          and sec.is_deleted=false and c.is_deleted=false and s.is_deleted=false and s.status='submitted'
          and (${range.data.dateFrom ?? null}::date is null or coalesce(s.started_at,s.submitted_at)>=(${range.data.dateFrom ?? null}::date::timestamp at time zone 'Europe/Vienna'))
          and (${range.data.dateTo ?? null}::date is null or coalesce(s.started_at,s.submitted_at)<((${range.data.dateTo ?? null}::date+1)::timestamp at time zone 'Europe/Vienna'))
        order by "campaignId","submittedAt","sessionId"
      `);
      res.set("Cache-Control", "private, no-store").status(200).json({ visits: rows.map(row => ({
        ...row, startedAt: new Date(row.startedAt).toISOString(), submittedAt: new Date(row.submittedAt).toISOString(),
      })) });
    } catch (error) { next(error); }
  });
  return router;
}
