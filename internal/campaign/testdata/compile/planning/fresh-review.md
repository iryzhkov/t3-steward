# Review plan for unit a: A

Read `.t3/inputs/inputs/unit.md` as the authoritative planning brief and `.t3/inputs/inputs/plan.md` for context.
This is an empty fresh workspace for project p, with no Git repository.
List `.t3/dependencies/` to discover the planning task's directory.
Read its `artifacts/handoff.md` and every declared plan output:
- `plan.md`
- `risks.md`
Check the produced plans against every criterion in the planning brief.
Do not change source, execute the plans, make commits, push or submit tasks or campaigns.
Write `review.md` with the first line exactly `VERDICT: ACCEPT` or
`VERDICT: CHANGES_REQUESTED`, the existing coordinator verdict contract.
Include findings with concrete failure scenarios, criteria evidence and verification exit codes.
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
