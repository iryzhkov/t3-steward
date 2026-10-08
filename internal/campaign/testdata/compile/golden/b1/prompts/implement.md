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
not, stop and say so in `handoff.md`.

Scope: the change unit b1 asks for and nothing else. No push, tag,
release or deployment, and no nested campaign, task or review submission.

Outputs:
- The commit, declared as `implementation`: commit the change and its tests.
  HEAD when you finish is what the reviewer receives.
- `continuation.md`: goal, checklist, current step, blockers and the last
  verification, kept current as you work.
- `handoff.md`: base d2e827681336e76413fdab34392f029a3e131cf4, resulting head, changed paths, design decisions
  and why, verification commands with their exit codes, limitations and open
  risks.

While iterating, run the tests of the packages you changed and of their
importers rather than the whole suite. In a repository with a `test-affected`
make target, such as t3-steward, that is `make test-affected BASE=d2e827681336e76413fdab34392f029a3e131cf4`;
where Huyang is your editing interface, it is Huyang `verify_run` with
`test_scope=affected`. This does not replace the verification below, which
still runs once.

Verification: these commands run in the workspace after you stop, and every
one must exit 0:
- `go test ./...`
- `git diff --check`

Review: a separate review task reads the commit and `handoff.md` next. Do not
claim acceptance.

Acceptance criteria: the ones in `.t3/inputs/inputs/unit.md`.
