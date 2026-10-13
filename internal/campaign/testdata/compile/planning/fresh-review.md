# Review plan for unit a: A

Read `.t3/inputs/inputs/unit.md` as the authoritative planning brief and `.t3/inputs/inputs/plan.md` for context.
This is an empty fresh workspace for project p, with no Git repository.
List `.t3/dependencies/` to discover the planning task's directory.
Read its `artifacts/handoff.md`, `artifacts/verification.log` and every declared plan output:
- `plan.md`
- `risks.md`
Check the produced plans against every criterion in the planning brief.
Do not change source, execute the plans, make commits, push or submit tasks or campaigns.
Write `artifacts/review.md` with the first line exactly `VERDICT: ACCEPT` or
`VERDICT: CHANGES_REQUESTED`, the existing coordinator verdict contract.
Include findings with concrete failure scenarios, criteria evidence and verification exit codes.
The handoff records reviewed document/input digests, findings, checks and limitations.
Keep task state in `artifacts/continuation.md` and `artifacts/handoff.md`.
Request changes only for defects or unmet criteria.

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
if [ -L artifacts ] || { [ -e artifacts ] && [ ! -d artifacts ]; }; then printf '%s\n' 'report-path collision: artifacts parent'; exit 1; fi; for p in artifacts/continuation.md artifacts/handoff.md artifacts/verification.log artifacts/review.md; do if [ -L "$p" ] || { [ -e "$p" ] && [ ! -f "$p" ]; }; then printf 'report-path collision: %s nonregular destination\n' "$p"; exit 1; fi; done
```
Create `artifacts/` as a real writable directory, then create fresh regular reports/logs.
Write your own `artifacts/verification.log` with actual commands, full output, exits,
document/input digests and limitations. Never overwrite immutable dependency logs.
If no check is declared or run, record "no verification command declared".
This log supports evidence; worker verification and custody remain authoritative.
