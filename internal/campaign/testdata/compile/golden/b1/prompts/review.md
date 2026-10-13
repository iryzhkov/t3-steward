# Review of unit b1: Thread filter

Goal: review the implementation of unit b1 (plan section
"B-1") of project t3-steward-github against its brief, and decide whether
it is ready.

Inputs:
- `.t3/inputs/inputs/unit.md` is the unit's brief, byte for byte. Check every acceptance
  criterion in it.
- `.t3/inputs/inputs/plan.md` is the whole plan, for context.
- `.t3/dependencies/` holds one directory for the implement task; list it
  to find that directory. It contains `artifacts/handoff.md`, `artifacts/verification.log` and `implementation`, the
  provenance record of the implementation commit. The commit itself is
  already fetched into this checkout under the campaign ref the record names.

The pinned base is d2e827681336e76413fdab34392f029a3e131cf4. Confirm that the implementation commit descends
from it, and review `git diff d2e827681336e76413fdab34392f029a3e131cf4..<implementation commit>`.

Scope: read and test only. Do not modify, commit or push source.

Outputs:
- `artifacts/review.md`: the first line is exactly `VERDICT: ACCEPT` or
  `VERDICT: CHANGES_REQUESTED`. Then the reviewed commit, the findings (each
  with a severity, file:line, a concrete failure scenario and a reproducing
  command), the criteria checked with their evidence, and the commands you
  ran with their exit codes. The coordinator records that first line as the
  review's verdict, and a first line in any other form fails this task.
- `artifacts/continuation.md`: kept current as you work.
- `artifacts/handoff.md`: exact reviewed HEAD, findings/criteria, command exits and limitations.

Verification: first reproduce any suspected defect with a focused test or command.
Then run `make test-affected BASE=d2e827681336e76413fdab34392f029a3e131cf4` where the repository has that
make target; with Huyang, use `verify_run` with `test_scope=affected`. Otherwise
run focused checks of the changed packages and their importers.
Do not rerun the full declared gate (`go test ./...`, `git diff --check`): the implement task only succeeds after the
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

Before any report or source write, rerun the current collision guard below, even when
preflight reused a cached receipt: that receipt is baseline evidence, not the current index.
Stop on failure; never follow links, delete existing bytes or repair collisions automatically.
```sh
if [ -L artifacts ] || { [ -e artifacts ] && [ ! -d artifacts ]; }; then printf '%s\n' 'report-path collision: artifacts parent'; exit 1; fi; if [ ! -d .git ] && [ ! -f .git ]; then printf '%s\n' 'report-path check: Git workspace required'; exit 1; fi; inside=$(git rev-parse --is-inside-work-tree 2>/dev/null) && prefix=$(git rev-parse --show-prefix 2>/dev/null) && [ "$inside" = true ] && [ -z "$prefix" ] || { printf '%s\n' 'report-path check: Git workspace failed'; exit 1; }; entries=$(git ls-files --stage -- ':(top,literal)artifacts' 2>/dev/null) || { printf '%s\n' 'report-path check: Git index failed'; exit 1; }; tab=$(printf '\t'); case "$entries" in *"$tab"artifacts) printf '%s\n' 'report-path collision: artifacts index entry'; exit 1;; esac; for p in artifacts/continuation.md artifacts/handoff.md artifacts/verification.log artifacts/review.md; do if [ -L "$p" ] || { [ -e "$p" ] && [ ! -f "$p" ]; }; then printf 'report-path collision: %s nonregular destination\n' "$p"; exit 1; fi; entries=$(git ls-files -- ':(top,literal)'"$p" 2>/dev/null) || { printf '%s\n' 'report-path check: Git index failed'; exit 1; }; if [ -n "$entries" ]; then printf 'report-path collision: %s index entry\n' "$p"; exit 1; fi; done
```
Create `artifacts/` as a real writable directory, then create fresh regular reports/logs.
Write your own `artifacts/verification.log` with actual commands, full output, exits,
tested or reviewed final HEAD, host/toolchain/environment and limitations. Never overwrite immutable dependency logs.
This log supports evidence; worker verification and custody remain authoritative.
Keep reports uncommitted. Stage only source/tests/product docs explicitly when authorized;
before declaring HEAD, rerun the guard and `git diff --quiet && git diff --cached --quiet`.
