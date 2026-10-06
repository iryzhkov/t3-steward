# Worker-owned final gate

A task can declare the final checks that Steward runs after its agent stops:

```yaml
tasks:
  implement:
    prompt_file: implement.md
    verify: [test -f handoff.md]
    gate:
      commands: [make check-review]
      timeout: 45m
  review:
    prompt_file: review.md
    needs: [implement]
    inputs_from:
      implement: [gate, gate/log.txt]
```

The gate runs only after ordinary verification passes. It runs on the producing
worker with the final workspace HEAD, outside the agent turn. A failure fails the
task while preserving the result and log for inspection.

Each command's timeout defaults to 45m. Offline validation bounds it to a positive duration
no greater than 6h. At dispatch the declared timeout must also fit
`backlog_v2.verification.command_timeout`, the worker execution maximum. That
setting defaults to 30m, so set it to at least 45m to use the default gate timeout.
An explicit smaller gate timeout can fit the existing maximum.

```yaml
backlog_v2:
  verification:
    command_timeout: 45m
    gate_cache_age: 24h
```

The worker records structured evidence as the artifact `gate` and a bounded text
log as `gate/log.txt`. They are implicit gate outputs: do not duplicate these
reserved names in `outputs` or `commits`. Dependents may request them through
`inputs_from`. Task results and explanations expose the recorded gate evidence.
The result identifies commands, exit codes, durations, final tree, worker and
toolchain. A truncated retained log reports its limit. The logical `gate` report
arrives as `gate/report.json` beside `gate/log.txt` in result and dependency
directories.

Gate workspaces must have clean tracked files and no undeclared untracked files
(except `.t3` metadata). Declared output files may remain untracked. Metadata
reads disable repository executable hooks and conversion filters; a worktree
that needs a conversion filter to match its committed bytes fails this check.

Successful cache evidence is keyed by the final HEAD tree, ordered commands,
command timeout and toolchain identity. Reuse is marked cached and names the original attempt;
changing any key input reruns the gate. Cache evidence survives worker restart.
`gate_cache_age` defaults to 24h; zero disables reuse.

Gate tasks require the worker capability `worker-owned-gate-v1`. Older workers
remain eligible for tasks without a gate; they are excluded from placement for
gate tasks and cannot silently omit a declared gate.

Both ordinary and contained collection execute gates. The contained lane uses
the same validated shell invocation contract as ordinary verification. Current
contained execution exposes its invocation summary rather than full command output;
the retained gate log explicitly reports this limitation. Contained gate cache reuse
is disabled because a matching host toolchain does not prove a matching sandbox
toolchain. Full ordinary-lane output and caching remain available.
