# Durcharbeit market defaults and monthly SM substitutes

The existing registry ID remains authoritative for automatic planning. Admins can now change that default through `PATCH /admin/sm-markets/SMDurcharbeit/:id/owner` with `SMDurcharbeitSmUserId` and `expectedUpdatedAt`. The authenticated, permission-checked transaction locks the market/registry, validates an active non-deleted SM account, and updates the canonical account ID, current SM name, registry account ID and optimistic timestamp together. Clearing the default is explicit. All other market fields and the original imported planner/source cells remain unchanged. Standard markets cannot use this endpoint.

Reimports retain an existing registry ID, including an explicit null, and its current saved display name. They do not silently restore the original spreadsheet's SM after a manual change.

`PATCH /admin/sm-smdurcharbeit-campaigns/targets/:targetId/owner` changes one campaign/month target with `expectedRevision`, `SMDurcharbeitSmUserId` and a reason. A non-null account selects a temporary substitute; null restores the live registry default. Campaign → target → registry locks serialize it with visit start. Closed months, ended/archived campaigns, stale revisions, inactive/deleted/non-SM accounts, live drafts and completed monthly targets are rejected. There is no future-month scope parameter.

The save appends an immutable assignment revision, points only this target to it, increments its revision and appends an atomic audit event. A structured `SMDurcharbeit_owner_override` event referencing that target's current owner revision makes its existing owner authoritative. `SMDurcharbeit_owner_automatic` restores the default. The indexed target event lookup distinguishes explicit monthly choices from automatic revisions without a new column or ambiguous name matching.

The shared effective-owner expression is used by the employee queue, visit authorization and monthly admin results. Other months and other campaigns retain automatic registry ownership. Extensions create fresh owner revisions from the registry and do not copy an earlier month's override. Availability ends at the campaign end/month boundary through the existing date gates. There is no scheduled reset job: the previous month keeps its recorded assignment as history, while subsequent months/new campaigns use the default.

Previous owner revisions and physical visits retain their original account links. Answers, photos, submissions, time revisions, dated assignments, questionnaire versions and imported source fields are never rewritten by either owner endpoint. A draft must be completed or explicitly discarded through the existing workflow before reassignment; completed monthly work stays with its original employee.

## Isolated verification and release

No schema migration, backfill, production business-data query, production application smoke test or production data edit is part of this release. Existing production environment files and human localhost:3000/4000 are not used for verification.

The dedicated integration test covers default edits, ID/name synchronization, source preservation, reimport/unassignment, monthly visibility, restoring defaults, month rollover, independent new campaigns, expiry, auth/permissions, stale revisions, account eligibility, draft/completion protection and the full answer/photo/time/report/archive flow. Native PostgreSQL races cover two saves, monthly reassignment versus visit start, and default reassignment versus visit start. Snapshots assert old graph rows remain unchanged.

The existing disposable fixture uses either in-memory PGlite or a private marked synthetic PostgreSQL socket; it accepts no production URL or credentials. Run the new test with a clean environment and the backend tsx runner:

```sh
env -i PATH="$PATH" node node_modules/tsx/dist/cli.mjs --test src/SMDurcharbeit-owner-edit.integration.test.ts
```

For native concurrency verification, use only the fixture's marked disposable socket mechanism. Frontend screenshots and build/release receipts remain in untracked local outputs. Deploy backend first, confirm its exact commit through Railway deployment metadata, then deploy the frontend and matching backend Git pointer. Frontend production readiness/alias is confirmed through Vercel metadata; do not issue production application requests.
