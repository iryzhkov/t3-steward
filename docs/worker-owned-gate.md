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
`TMPDIR`, `TZ`, `LANG`, `LANGUAGE`, the gate shell's own variables (`SHELLOPTS`,
`BASHOPTS`, `CDPATH`, `BASH_ENV`, `ENV`), compiler and linker flags (`CC`, `CXX`, `AR`,
`CFLAGS`, `CPPFLAGS`, `CXXFLAGS`, `LDFLAGS`), make flags (`MAKEFLAGS`,
`GNUMAKEFLAGS`, `MFLAGS`, `MAKEFILES`), loader and pkg-config paths
(`LD_LIBRARY_PATH`, `LD_PRELOAD`, `PKG_CONFIG_PATH`, `PKG_CONFIG_LIBDIR`),
`XDG_CACHE_HOME`, `XDG_CONFIG_HOME`, `XDG_DATA_HOME`, and every variable starting
with `GO`, `CGO_`, `LC_`, `GIT_`, `PYTHON`, `NODE_`, `NPM_CONFIG_`,
`npm_config_`, `CARGO_`, `RUST` or `JAVA_`. Per-start service variables such as
`INVOCATION_ID` and `JOURNAL_STREAM` are excluded, so a restart alone does not
miss the cache. A variable outside this list that changes a gate's behaviour is
not detected, and neither is a change to files outside the tree that a command
reads, such as `~/.config/go/env` or `~/.gitconfig`. In the uncontained lane the
agent runs as the worker's user and can write those files; set
`gate_cache_age: 0` where that matters.

The worker's cache directory is writable by that same account, so a cache
record is never trusted on its own. The coordinator is the authority: each gate
package lists the attempts whose passing gate report the coordinator recorded
from that worker within `gate_cache_age` (at most 256, newest first), and the
worker reuses a record only when its original attempt is in that list. On
import the coordinator then compares a cached report with the uncached passing
report it recorded for the original attempt (same worker, cache key, tree and
completion time) and rejects the result when they differ or the original is
missing. Forging a cached pass therefore requires an authentic pass of the same
key on the same worker. A record whose original attempt was never imported is
ignored and the gate runs again.

When a stored task's gate timeout exceeds the coordinator's current
`command_timeout` (a rerun of a task accepted under a larger setting, or a
setting lowered after submission), dispatch bounds each gate command by
`command_timeout` instead of withholding the task, and a gate that needs longer
fails with a structured timeout.
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
