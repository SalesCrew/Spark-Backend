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

## Reviewed connection, 2026-10-09

The schema-only step initially populated no IDs. The user subsequently confirmed the separate production backfill and clarified: no campaign should be created yet; visits should become visible when assignments are made and the campaign starts.

The necessary backfill inventory contained 424 registry markets and 62 active, non-deleted SM accounts. The eleven distinct imported full names each resolved to exactly one eligible account using the existing full-name normalization (name order, accents, whitespace and sharp S). Every canonical market already had the same matching account; there were no conflicts, missing matches or ambiguous matches. No canonical market ownership needed to change.

The reviewed operational transaction changed **only** `sm_smdurcharbeit_markets.smdurcharbeit_sm_user_id`: receipt `reviewed=424`, `updated=424`, `alreadyLinked=0`, `sourceFieldsPreserved=true`. The exact SQL artifact SHA-256 is `704e27b1fa50e5550c454ee0ebfd81514448f8da627d69c0b2143c0899a5db85`. Private reviewed inputs/SQL remain outside the repository; no production personal records are used as test fixtures or committed. The registry had no non-internal triggers. No canonical market, imported name/source field, assignment, visit, answer, photo, time record, campaign or target was inserted, deleted or updated by this operation.

`operations/SMDurcharbeit-sm-user-backfill.ts` is an explicit offline artifact builder, with no environment loading, database client or automatic execution. It skips missing/ambiguous/inactive/conflicting links, validates snapshot identities, takes the existing SM planning lock and bounded row locks, aborts changed account/market context, updates only NULL IDs, checks row counts and all other registry fields inside the transaction, and tolerates a repeat after successful application. No production tests or smoke requests were run.

The monthly campaign options endpoint now uses the saved Durcharbeit ID first and the existing canonical account as a fallback. The unchanged UI uses that option when selecting markets for a **new** roster; explicit choices, saved rosters and historical owner revisions are not rewritten. Employee visibility remains determined by published campaign/month owner revisions and targets, with the existing date/month checks. At backfill time there were no monthly campaigns, and none were created, activated or published. Separate questionnaires and campaign dates remain the human administrator's choice.

Thirty checks passed in a native disposable synthetic PostgreSQL database, covering the guarded operation, replay, stale account/source/ownership rollback, duplicate-row retention, link precedence/fallback, publication-gated employee visibility, submission/reporting and the existing answer/photo/time/import flow. Backend TypeScript compilation and whitespace checks passed. Application startup cannot invoke the operational backfill.
