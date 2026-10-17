# Unit b1: Thread filter

Goal: implement unit b1 (plan section "B-1") of project
t3-steward-github, as a commit on top of the pinned base d2e827681336e76413fdab34392f029a3e131cf4.

Inputs:
- `.t3/inputs/inputs/unit.md` is this unit's section of the plan, byte for byte. It is the
  brief: its scope, contract and acceptance criteria are authoritative.
- `.t3/inputs/inputs/plan.md` is the whole plan, for context only. Do not implement any
  other unit.

Before editing, check that `git rev-parse HEAD` prints d2e827681336e76413fdab34392f029a3e131cf4; the workspace
is pinned to it, and the pin is also recorded at `.t3/base-commit`. If it does
not, stop and say so in `artifacts/handoff.md`.

Scope: the change unit b1 asks for and nothing else. No push, tag,
release or deployment, and no nested campaign, task or review submission.

Outputs:
- The commit, declared as `implementation`: commit the change and its tests.
  HEAD when you finish is what the reviewer receives.
- `artifacts/continuation.md`: goal, checklist, current step, blockers and the last
  verification, kept current as you work.
- `artifacts/handoff.md`: base d2e827681336e76413fdab34392f029a3e131cf4, resulting head, changed paths, design decisions
  and why, verification commands with their exit codes, limitations and open
  risks.
- `artifacts/verification.log`: your fresh command log for this final HEAD.

While iterating, run the tests of the packages you changed and of their
importers rather than the whole suite. In a repository with a `test-affected`
make target, such as t3-steward, that is `make test-affected BASE=d2e827681336e76413fdab34392f029a3e131cf4`;
otherwise run focused checks of the changed packages and their importers
through the shell. This does not replace the verification below, which
still runs once.

Verification: these commands run in the workspace after you stop, and every
one must exit 0:
- `go test ./...`
- `git diff --check`

Review: a separate review task reads the commit and `artifacts/handoff.md` next. Do not
claim acceptance.

Acceptance criteria: the ones in `.t3/inputs/inputs/unit.md`.

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
