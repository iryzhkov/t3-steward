# Review of unit c1: Campaign compile skeleton

Goal: review the implementation of unit c1 (plan section
"C-1") of project t3-steward-github against its brief, and decide whether
it is ready.

Inputs:
- `.t3/inputs/inputs/unit.md` is the unit's brief, byte for byte. Check every acceptance
  criterion in it.
- `.t3/inputs/inputs/plan.md` is the whole plan, for context.
- `.t3/dependencies/` holds one directory for the implement task; list it
  to find that directory. It contains `handoff.md` and `implementation`, the
  provenance record of the implementation commit. The commit itself is
  already fetched into this checkout under the campaign ref the record names.

The pinned base is d2e827681336e76413fdab34392f029a3e131cf4. Confirm that the implementation commit descends
from it, and review `git diff d2e827681336e76413fdab34392f029a3e131cf4..<implementation commit>`.

Scope: read and test only. Do not modify, commit or push source.

Outputs:
- `review.md`: the first line is exactly `VERDICT: ACCEPT` or
  `VERDICT: CHANGES_REQUESTED`. Then the reviewed commit, the findings (each
  with a severity, file:line, a concrete failure scenario and a reproducing
  command), the criteria checked with their evidence, and the commands you
  ran with their exit codes. The coordinator records that first line as the
  review's verdict, and a first line in any other form fails this task.
- `continuation.md`: kept current as you work.

Verification: rerun these on the implementation commit:
- `go test ./...`
- `git diff --check`

Request changes only for real defects or unmet criteria, not for style.
