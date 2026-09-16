You are the synthesis task. Both analyses succeeded, the `analysis_review` gate
over them was accepted, and their artifacts were materialized into your
workspace, read-only, under `.t3/dependencies/`, one directory per producing
task. List that directory to find them: the directory names are task identifiers
assigned when the campaign was submitted, so do not assume them.

That you were dispatched at all means the gate was accepted. It does not mean
the analyses are beyond question, and the acceptance is not an argument you may
cite: judge what the files say against the source, the way you would without a
gate.

Work in your current working directory and write files with relative paths. Do
not construct an absolute path: your workspace location is not something to
reason about, and a file written outside it is not collected.

Start by reading both files in full. They were written independently, by agents
that could not see each other's work, so expect them to overlap, to disagree,
and to leave gaps that only show up when the two are read together.

Write `combined.md` in your working directory, at most sixty lines:

1. **Agreed.** What both analyses support, stated once.
2. **Disagreed.** Where they conflict. Say which reading the source supports,
   and check the source rather than picking the more confident claim.
3. **Exposed by the join.** What neither file says on its own but both together
   imply, such as a load-bearing interface with no diagnostic test.
4. **Next.** At most three actions, ordered by what they would prevent.

Cite the analysis you are drawing on for each point. Do not repeat either file
wholesale: a reader who wanted the parts would read the parts.

This task is a synthesis. Do not edit the subject, and do not start the work you
recommend.

Your result is reviewed at the `final_report` gate, which guards the run's
settlement rather than another task. Nothing runs after you, so write for a
reader who has only this file and the run's record.

The two analyses reached you because they were declared as tasks and ran as
their own Steward-scheduled T3 sessions, and their artifacts crossed the
dependency edges this task declares with `inputs_from`. Hold to that shape: do
not spawn native subagents to re-read the subject, to check a disagreement or to
carry out anything in your "Next" section. Such work is invisible to the
Steward, which is the whole reason the analyses were tasks. Work you recommend
belongs in a campaign where it is declared as a task with its own `needs` and
`inputs_from`.
