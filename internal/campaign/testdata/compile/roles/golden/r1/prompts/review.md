# Review of unit r1: Role routing

Goal: review the implementation of unit r1 (plan section
"R-1") of project t3-steward-github against its brief, and decide whether
it is ready.

Inputs:
- `.t3/inputs/inputs/unit.md` is the unit's brief, byte for byte. Check every acceptance
  criterion in it.
- `.t3/inputs/inputs/plan.md` is the whole plan, for context.
- `.t3/inputs/inputs/design/notes.md` is supporting material the plan bundles with every unit.
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

Verification: first reproduce any suspected defect with a focused test or command.
Then run `make test-affected BASE=d2e827681336e76413fdab34392f029a3e131cf4` where the repository has that
make target; with Huyang, use `verify_run` with `test_scope=affected`. Otherwise
run focused checks of the changed packages and their importers.
Do not rerun the full declared gate (`make test`): the implement task only succeeds after the
worker has run that gate on the implementation commit. Record focused commands
and their exit codes.
Request changes only for real defects or unmet criteria, not for style.

Calibrate blocking findings to realistic author input. Each blocking finding must
name the violated acceptance criterion, give a concrete plausible author input,
explain why that input is plausible, and provide a reproducing command or test.
An ordinary plan with direct scalar fields and conventional Markdown remains
eligible for blocking findings when acceptance fails; do not excuse that failure.
Syntactically representable input alone does not establish plausibility.
Label exotic encodings (including Unicode-encoded tokens), adversarial YAML, and
Unicode edge cases as FOLLOW-UPS with reproduction evidence; these nonblocking
findings cannot change the verdict.
