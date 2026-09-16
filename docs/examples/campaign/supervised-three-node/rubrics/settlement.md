# Rubric: final settlement

The standard the `final_report` gate applies to `combined.md`. This gate guards
the run's settlement rather than a downstream task, so accepting it is the last
supervisory decision of the run. Replace this file with your own before
submitting.

Accept only when all of the following hold.

1. **Both analyses were used.** Every section draws on the analyses by name, and
   neither one was quietly ignored.
2. **Disagreements were resolved against the source**, not against whichever
   analysis sounded more confident. The resolution names what was checked.
3. **The join earned its place.** At least one finding is something neither
   analysis states on its own.
4. **Recommendations are bounded and not started.** At most three, ordered, and
   none of them carried out: the synthesis task recommends, it does not act.
5. **Nothing is presented as more certain than the evidence allows.** A claim the
   analyses left open is still open here.

Hold this gate only when a correction is actually possible, because holding it
keeps the run open. Escalate when it is not: an unresolved final gate waiting for
a correction nobody can make is the one failure mode this gate has.

Accepting this gate does not turn a failed task into a successful one. If a
worker task failed, that outcome stands, and the honest disposition is to
acknowledge the failure for settlement or to escalate, never to review the
failure away.
