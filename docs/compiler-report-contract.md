# Compiler report contract

New compile-v1 implement-review and plan-review units use a single report contract:

| Role | Declared reports | Additional outputs |
| --- | --- | --- |
| Implement | artifacts/continuation.md, artifacts/handoff.md, artifacts/verification.log | implementation commit |
| Plan | Same three reports | Authored plan files in their original order |
| Review | Same three reports plus artifacts/review.md | None |

Review dependencies carry the producer's handoff and verification log, plus its
implementation commit or authored plans. The coordinator's existing first-line
verdict parser remains the sole authority at artifacts/review.md:
VERDICT: ACCEPT or VERDICT: CHANGES_REQUESTED. Missing or invalid verdicts fail;
report text does not grant source acceptance or execution authority.

The compiler reserves all four report destinations and their ancestors/descendants
against authored output collisions. The historical review.md reservation remains.
Arbitrary brief text and old hand-authored workflows are not rewritten.

Each task has an existing-schema require-pass report-path-collision preflight,
bounded to 4096 output bytes and two minutes. Git tasks check exact report index
entries/descendants and the artifacts ancestor entry, plus filesystem collisions.
Unrelated product files below artifacts/ and regular untracked reports are allowed.
Symlink/non-directory parents and nonregular destinations refuse without repair.
Git errors fail closed; a Git task must have its own repository at the workspace root.
Fresh planning uses only filesystem checks and needs no Git repository.

Cached preflight receipts establish baseline evidence. Every prompt requires the
current guard before writes, and final verification repeats it. Git tasks additionally
require a clean tracked tree and index after the unchanged authored command list.
Only exact generated checks are deduplicated; authored bytes/order are preserved.

Reports remain uncommitted. Agents create fresh writable regular logs of their own
commands, output, exits and final HEAD or fresh document/input digests. Incoming logs
remain immutable. A role that ran no check records "no verification command declared".
Worker verification and retained custody remain authoritative. Reviewers write no source
or commits; generic reviews use focused checks without duplicating the producer's full gate.
Compilation remains pure; unit publication keeps its existing per-unit staging/rollback
semantics. No runtime cache, FIX generator, schema, custody or review-policy changes apply.
