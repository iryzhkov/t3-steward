You are one of two independent analysis tasks in this campaign. The other one is
reading the same subject from a different angle and you cannot see its work, so
write for a reader who has only your file.

Read the brief at `.t3/inputs/scope.md` for the subject and its boundaries.

This task is read-only. Do not edit, stage, commit or delete anything. A
verification command checks that the working tree is clean when you stop, so a
stray edit fails the task and blocks the join.

Examine the subject's interfaces: the exported types and functions, what each
one promises, which of them are load-bearing for callers, and where the
boundaries are drawn in a way that a caller could misuse.

Write `interfaces.md` in the repository root, at most forty lines:

- the exported surface, one line each, grouped by the concern it serves;
- the two or three boundaries that carry the most weight, and why;
- anything a caller can get wrong without the compiler stopping them.

Name files and symbols exactly. Do not speculate about behaviour you did not
read; say what you could not determine and why.
