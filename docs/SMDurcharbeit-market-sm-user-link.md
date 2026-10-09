# SMDurcharbeit market SM account link

Applied 2026-10-09 to the existing Coke Spark production schema in response to the user's specific instruction: add the SM ID column and migrate it now; perform the name backfill only after the next instruction.

## Schema artifact

- Table: `public.sm_smdurcharbeit_markets`.
- New field: `smdurcharbeit_sm_user_id`, nullable UUID, no default.
- ORM field: `SMDurcharbeitSmUserId`.
- Foreign key: `sm_smdurcharbeit_market_sm_user_fk` references `public.users(id)`, `ON DELETE RESTRICT`.
- Partial index: `sm_smdurcharbeit_market_sm_user_idx`, non-NULL links only.
- Source: `supabase/migrations/20261009160312_SMDurcharbeit_market_sm_user_id.sql`.
- SHA-256: `7ae41703e19d19a81086299a9ca0f5b98295e28c90db06bf29dd5896143392f6`.
- Production ledger: `20261009160556_smdurcharbeit_market_sm_user_id`.

The migration is transactional with five-second lock and sixty-second statement timeouts. It has no row INSERT, UPDATE, DELETE, name matching, import, backfill, assignment generation or campaign activation. The existing imported name field `smdurcharbeit_verplanung` and original source values remain intact. Production access was limited to this specifically requested migration and necessary schema/migration metadata; no business rows were queried.

Catalog-only confirmation: nullable UUID without a default, validated restrictive FK, ready/valid partial index, original name column, and unchanged table owner/RLS/forced RLS/grants.

## Isolated verification

19 native disposable PostgreSQL checks passed, including upgraded-schema historical sentinels, invalid account rejection, restrictive deletion, re-import link retention and the existing monthly visit/answer/photo/time flow. The first fresh-cluster parallel run raced while creating shared test-only roles; sequential rerun passed all checks without application behavior changes. Backend TypeScript build and whitespace checks passed. Tests ran with an empty process environment plus synthetic settings, no production environment files.

## Deferred connection

No IDs have been populated by this release. Existing imports and manual market creation keep their current behavior; adding this field does not reinterpret names or silently overwrite a reviewed ID on re-import.

The later authorized backfill must resolve names only to reviewed active SM accounts, leave missing/ambiguous matches unresolved, and preserve the original name/source columns. The FK checks account existence; role/activity validation belongs to the reviewed matching step.

Employee visibility is determined by published campaign/month owner revisions and targets, rather than this registry field alone. Wiring matched IDs into new campaign rosters or explicitly reviewed current-owner transitions is a separate step; historical visit owners and submissions must not be rewritten.

Application source is recorded locally with this migration. This schema-only step does not introduce a frontend change, deploy application code, or populate production links.
