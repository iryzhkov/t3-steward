Implement fix2 from the recorded preceding review.
Read .t3/inputs/inputs/fix/rules.md first and follow it, then the original brief and context. List .t3/dependencies/ and inspect the retained commit, handoff and review artifacts. The original reviewed commit is bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb; continue from the commit consumed by this task.

Read the preceding review's verdict.json. If it says accept, check out the preceding fix commit unchanged, declare it as fix, and write the one-line no-op handoff and verification.log described in the shared rules. Still write continuation.md. Otherwise reproduce and repair only the listed blocking findings within the original brief.

Write continuation.md, handoff.md and verification.log. Commit the fix as HEAD (declared commit name fix). Run the affected-test loop against the original pinned base as described in the shared rules, then the test-integrity check before finishing. Change nothing for notes. Record anything else in a FOLLOW-UPS section of handoff.md. If a listed finding cannot be fixed within the original brief, stop and report rather than redesign. Leave the tracked tree clean.
The worker executes the declared final gate after your verification; do not run a separate agent gate.
