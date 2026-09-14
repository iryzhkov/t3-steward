You are the join task. Both analysis tasks have succeeded and their artifacts
were materialized into your workspace, read-only, under `.t3/dependencies/`,
one directory per producing task. List that directory to find them: the
directory names are task identifiers assigned when the campaign was submitted,
so do not assume them.

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
