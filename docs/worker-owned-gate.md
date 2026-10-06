# Worker-owned final gate

A task can declare the final checks that Steward runs after its agent stops:

```yaml
tasks:
  implement:
    prompt_file: implement.md
    verify: [test -f handoff.md]
    gate:
      commands: [make check-review]
      timeout: 30m
  review:
    prompt_file: review.md
    needs: [implement]
    inputs_from:
      implement: [gate, gate/log.txt]
```

The gate runs only after ordinary verification passes. It runs on the producing
worker with the final workspace HEAD, outside the agent turn. A failure fails the
task while preserving the result and log for inspection.

A gate allows at most 64 unique commands, at most 4096 bytes per command and
16 KiB of commands in total, so structured evidence fits the coordinator limit.
Each command's timeout defaults to 30m. Offline validation bounds it to a positive duration
no greater than 6h. The declared timeout must also fit
`backlog_v2.verification.command_timeout`, the worker execution maximum, which
defaults to 30m, so a gate with the default timeout dispatches under the default
configuration. Submission refuses a task whose gate timeout exceeds the
coordinator's setting and names the task and the setting. To run a longer gate,
raise the setting first:

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
Repositories with submodules or manual gitlinks are rejected explicitly because
the outer tree cannot attest a nested mutable worktree. Assume-unchanged and
skip-worktree index entries are also rejected. Workspace file bytes are checked
against committed blobs, and Git metadata is bound to the actual workspace.

Successful cache evidence is keyed by the final HEAD tree, ordered commands,
command timeout and toolchain identity. Reuse is marked cached and names the original attempt;
changing any key input reruns the gate. Cache evidence survives worker restart.
The toolchain identity includes a digest of only the inherited environment
variables that can change what a command does: `PATH`, `HOME`, `SHELL`,
`TMPDIR`, `TZ`, `LANG`, `LANGUAGE`, compiler and linker flags (`CC`, `CXX`, `AR`,
`CFLAGS`, `CPPFLAGS`, `CXXFLAGS`, `LDFLAGS`), make flags (`MAKEFLAGS`,
`GNUMAKEFLAGS`, `MFLAGS`, `MAKEFILES`), loader and pkg-config paths
(`LD_LIBRARY_PATH`, `LD_PRELOAD`, `PKG_CONFIG_PATH`, `PKG_CONFIG_LIBDIR`),
`XDG_CACHE_HOME`, `XDG_CONFIG_HOME`, and every variable starting with `GO`,
`CGO_`, `LC_` or `GIT_`. Per-start service variables such as `INVOCATION_ID` and
`JOURNAL_STREAM` are excluded, so a restart alone does not miss the cache. A
variable outside this list that changes a gate's behaviour is not detected.
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
