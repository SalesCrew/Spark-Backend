# SM visit answer revision recovery — 2026-10-20

## Problem and change

SM Durcharbeit production observability showed repeated answer-save HTTP 500s around 11:32–11:38 Europe/Vienna on 2026-10-20. PostgreSQL reported SQLSTATE 23505: the question's answer version 1 already existed. A photo commit for the same visit succeeded at 11:31:54; the unresolved answer save also blocked the frontend's final submission queue.

Question visibility rules retain an invalidated answer as history when hiding a question. When the question reappears, there is no current answer. Previously the SM save route treated that as a new history and attempted version 1 again. Photo initialization had the same reset. The production error confirms the collision; the exact visibility clicks are not present in observability. The hide/show sequence was independently reproduced with synthetic data.

`src/routes/sm-visits.ts` now allocates the next revision from the highest version across **all** answers for the submission question, including invalidated and soft-deleted rows. New rows link to that last historical answer. The existing visit/submission transaction locks, optimistic current-version check and identical-answer retry behavior remain in place. Photo initialization events use the allocated revision as well.

## Existing visits and historical data

- Existing drafts, visit IDs, submission IDs and saved answers remain usable. A client with expected current version 0 can append after retained history and finish the same visit.
- No bulk repair, backfill, database migration, renumbering, history deletion or photo cleanup is part of this release.
- Old answer rows, file metadata and stored photo bytes are retained. Hiding a question continues to invalidate its answer according to existing questionnaire rules; the fix does not silently reactivate invalidated answers or withdrawn photos.
- Active saved photos on other questions stay attached to the same draft and submission.
- Stale conflicting saves still return 409. Identical retries do not create duplicate revisions.
- The shared routes also support dated SM visits; no GM routes or frontend behavior change.

## Verification

All verification uses the existing disposable PGlite fixture, actual SM HTTP routes and in-memory synthetic photo storage. Commands run with `env -i` and the bundled Node executable; no production environment files, production connections or application job entry points are loaded.

New regression tests failed on the original code and passed after the fix:

1. A pre-existing monthly draft with two historical answers, another saved answer and a committed photo survives hide/show, accepts the pending expected-version-0 save, preserves old rows and photo bytes, rejects stale edits, accepts identical retries, and submits with the original visit/submission IDs.
2. Repeatedly hidden/reappearing photo questions initialize versions 2 and 3, retain historical photo metadata and bytes, reuse current initialization on retry, record correct event versions and submit the same visit.
3. A dated SM visit appends after retained soft-deleted version 7, leaves that history unchanged and submits without a restart.

The production TypeScript build passed. Additional isolated suites for photo withdrawal, monthly reporting, admin corrections/review, answer validation and comments passed.

The broader `SMDurcharbeit-monthly.integration.test.ts` has two failing subtests (profile assignment count; paused campaign cancellation's missing campaign row). Both failures were reproduced unchanged on the untouched production commit `892065f320df90c076e8a231f997bb79c041bed5`. They are outside this patch; its other 12 subtests passed. These failures must not be represented as a fully green broad suite.

Run the focused regression suite with `npm run test:sm-answer-recovery` in an isolated environment. Deployment verification is limited to Railway build/deployment state, with no agent requests against production business endpoints or production records.
