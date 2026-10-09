import { createHash } from "node:crypto";
import { z } from "zod";
import { SMDurcharbeitNameKey } from "../src/sm-SMDurcharbeit-market-import.shared.js";

const person = z.object({ id: z.string().uuid(), firstName: z.string().nullable(), lastName: z.string().nullable() }).strict();
const market = z.object({ marketId: z.string().uuid(), sourcePerson: z.string().nullable(),
  linkedUserId: z.string().uuid().nullable(), assignedUserId: z.string().uuid().nullable(),
  active: z.boolean(), deleted: z.boolean() }).strict();
const inventory = z.object({ markets: z.array(market).max(5000), people: z.array(person).max(5000) }).strict();
export type SMDurcharbeitSmLinkInventory = z.infer<typeof inventory>;

/** Offline review only: no environment files, database client, startup job or fuzzy matching. */
export function reviewSMDurcharbeitSmLinks(input: SMDurcharbeitSmLinkInventory) {
  const data = inventory.parse(input);
  for (const [rows, identity] of [[data.markets, "marketId"], [data.people, "id"]] as const) {
    if (new Set(rows.map(row => (row as unknown as Record<string,string>)[identity])).size !== rows.length) throw new Error("Duplicate snapshot identity.");
  }
  const byName = new Map<string, string[]>();
  for (const user of data.people) {
    const key = SMDurcharbeitNameKey(`${user.firstName ?? ""} ${user.lastName ?? ""}`);
    if (key) byName.set(key, [...(byName.get(key) ?? []), user.id]);
  }
  const rows = data.markets.map(row => {
    const matches = byName.get(SMDurcharbeitNameKey(row.sourcePerson ?? "")) ?? [];
    const userId = matches.length === 1 ? matches[0]! : null;
    const reason = !row.active || row.deleted ? "inactive_market"
      : matches.length === 0 ? "no_match" : matches.length > 1 ? "ambiguous_name"
      : row.linkedUserId && row.linkedUserId !== userId ? "existing_link_conflict"
      : row.assignedUserId && row.assignedUserId !== userId ? "assignment_conflict"
      : row.linkedUserId ? "already_linked" : "ready";
    return { ...row, userId, reason };
  });
  const approved = rows.filter(row => row.reason === "ready" || row.reason === "already_linked").map(row => ({
    marketId: row.marketId, userId: row.userId!, sourcePerson: row.sourcePerson, assignedUserId: row.assignedUserId,
  }));
  const directory = [...data.people].sort((a,b) => a.id.localeCompare(b.id)).map(user => `${user.id}\t${user.firstName ?? ""}\t${user.lastName ?? ""}`).join("\n");
  return { rows, approved, directoryFingerprint: createHash("md5").update(directory).digest("hex") };
}

/** Explicit operational artifact. Never automatically executed by the application or a migration. */
export function buildSMDurcharbeitSmLinkBackfill(input: SMDurcharbeitSmLinkInventory) {
  const review = reviewSMDurcharbeitSmLinks(input);
  if (!review.approved.length) throw new Error("No reviewed SM links to apply.");
  const payload = JSON.stringify(review.approved).replaceAll("'", "''");
  if (payload.includes("$SMDurcharbeit_backfill$")) throw new Error("Unsupported SQL delimiter in snapshot.");
  return { review, query: `BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SET LOCAL standard_conforming_strings = on;
DO $SMDurcharbeit_backfill$
DECLARE
  approved jsonb := '${payload}'::jsonb;
  expected integer := ${review.approved.length};
  linked_before integer;
  updated integer;
  source_before text;
  source_after text;
  directory_now text;
BEGIN
  PERFORM pg_advisory_xact_lock(hashtextextended('sm_planning_mutations', 0));
  PERFORM u.id FROM public.users u
    JOIN (SELECT DISTINCT x."userId" FROM jsonb_to_recordset(approved) AS x("userId" uuid)) x ON x."userId" = u.id
    ORDER BY u.id FOR SHARE OF u;
  SELECT md5(coalesce(string_agg(id::text || E'\\t' || coalesce(first_name,'') || E'\\t' || coalesce(last_name,''), E'\\n' ORDER BY id),''))
    INTO directory_now FROM public.users WHERE role = 'sm' AND is_active = true AND deleted_at IS NULL;
  IF directory_now <> '${review.directoryFingerprint}' THEN RAISE EXCEPTION 'SMDurcharbeit SM directory changed; review again.'; END IF;
  PERFORM m.id FROM public.sm_markets m
    JOIN jsonb_to_recordset(approved) AS x("marketId" uuid) ON x."marketId" = m.id
    ORDER BY m.id FOR UPDATE OF m;
  PERFORM r.sm_market_id FROM public.sm_smdurcharbeit_markets r
    JOIN jsonb_to_recordset(approved) AS x("marketId" uuid) ON x."marketId" = r.sm_market_id
    ORDER BY r.sm_market_id FOR UPDATE OF r;
  IF EXISTS (
    SELECT 1 FROM jsonb_to_recordset(approved) AS x("marketId" uuid,"userId" uuid,"sourcePerson" text,"assignedUserId" uuid)
    LEFT JOIN public.sm_smdurcharbeit_markets r ON r.sm_market_id = x."marketId"
    LEFT JOIN public.sm_markets m ON m.id = x."marketId"
    LEFT JOIN public.users u ON u.id = x."userId"
    WHERE r.sm_market_id IS NULL OR m.id IS NULL OR u.id IS NULL
      OR m.is_deleted OR NOT m.is_active OR u.role <> 'sm' OR NOT u.is_active OR u.deleted_at IS NOT NULL
      OR r.smdurcharbeit_verplanung IS DISTINCT FROM x."sourcePerson"
      OR m.assigned_sm_user_id IS DISTINCT FROM x."assignedUserId"
      OR (r.smdurcharbeit_sm_user_id IS NOT NULL AND r.smdurcharbeit_sm_user_id <> x."userId")
  ) THEN RAISE EXCEPTION 'SMDurcharbeit reviewed market/account context changed; no links written.'; END IF;
  SELECT count(*) INTO linked_before FROM public.sm_smdurcharbeit_markets r
    JOIN jsonb_to_recordset(approved) AS x("marketId" uuid,"userId" uuid) ON r.sm_market_id=x."marketId"
    WHERE r.smdurcharbeit_sm_user_id=x."userId";
  SELECT md5(string_agg((to_jsonb(r)-'smdurcharbeit_sm_user_id')::text, E'\\n' ORDER BY r.sm_market_id)) INTO source_before
    FROM public.sm_smdurcharbeit_markets r JOIN jsonb_to_recordset(approved) AS x("marketId" uuid) ON r.sm_market_id=x."marketId";
  UPDATE public.sm_smdurcharbeit_markets r SET smdurcharbeit_sm_user_id=x."userId"
    FROM jsonb_to_recordset(approved) AS x("marketId" uuid,"userId" uuid)
    WHERE r.sm_market_id=x."marketId" AND r.smdurcharbeit_sm_user_id IS NULL;
  GET DIAGNOSTICS updated = ROW_COUNT;
  IF updated + linked_before <> expected THEN RAISE EXCEPTION 'SMDurcharbeit link count mismatch; rollback.'; END IF;
  SELECT md5(string_agg((to_jsonb(r)-'smdurcharbeit_sm_user_id')::text, E'\\n' ORDER BY r.sm_market_id)) INTO source_after
    FROM public.sm_smdurcharbeit_markets r JOIN jsonb_to_recordset(approved) AS x("marketId" uuid) ON r.sm_market_id=x."marketId";
  IF source_before IS DISTINCT FROM source_after THEN RAISE EXCEPTION 'SMDurcharbeit source fields changed; rollback.'; END IF;
  PERFORM set_config('smdurcharbeit.backfill_receipt', jsonb_build_object(
    'reviewed',expected,'updated',updated,'alreadyLinked',linked_before,'sourceFieldsPreserved',true)::text, true);
END
$SMDurcharbeit_backfill$;
SELECT current_setting('smdurcharbeit.backfill_receipt')::jsonb AS receipt;
COMMIT;` };
}
