# Red Survey QA Checklist

1. **Migration**
   - Run latest migration.
   - Verify `question_bank_shared.red_survey` exists and is nullable.
   - Verify `visit_session_questions.red_survey_snapshot` exists and is nullable.

2. **Admin Module Editor**
   - Open each module editor (`standard`, `flex`, `billa`, `kuehler`, `mhd`).
   - Create a `yesno` question and confirm `Red Survey` toggle is visible.
   - Create a non-`yesno` question and confirm `Red Survey` toggle is hidden.
   - Enable `Red Survey` on `yesno`, save, reload, and confirm persisted state.

3. **Domain Guard**
   - Try sending `redSurvey: true` on a non-`yesno` question via API.
   - Confirm request is rejected with validation error.

4. **Historical Null Behavior**
   - Take an existing historical `yesno` question with `red_survey = NULL`.
   - Edit text only (without toggle update) and confirm `red_survey` stays `NULL`.
   - Toggle `Red Survey` on and save; confirm `red_survey = TRUE`.

5. **GM Visit Snapshot**
   - Start a GM visit for a campaign containing one `yesno` question with `Red Survey=true` and one with `false`.
   - Confirm created rows in `visit_session_questions` have `red_survey_snapshot = true/false` respectively.

6. **Derived Completion Consistency**
   - Answer the flagged question with `Ja`, submit.
   - Validate derived stats: `yesCount` equals `completedCount`.
   - Answer `Nein` in a second run and confirm no completed increment.
