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
Git metadata reads ignore replace refs, and every declared commit's revision
must resolve to HEAD, so the attested tree is the tree of the commit that is
published.

Every attempt runs its gate commands afresh, even when an earlier attempt
passed the same gate on the same tree: no gate result is cached or reused, and
the report names the attempt that produced it. The coordinator rejects, as
invalid evidence, a report that claims to replay another attempt's result. In
the ordinary lane each gate command runs in its own transient systemd scope,
and anything a command leaves running is killed when it exits, so a command
cannot start a service for a later one. The worker waits until systemd reports
the scope inactive; if it cannot establish that within 30 seconds, because a
kill or the state query fails, the command fails with the systemctl output as
its reason, even if it exited 0. A
declared file output, tracked or not, must still have the content the gate saw
when it is captured; otherwise the gate report is amended to a failure naming
the output, and the coordinator fails the attempt from that evidence.

When a stored task's gate timeout exceeds the coordinator's current
`command_timeout` (a rerun of a task accepted under a larger setting, or a
setting lowered after submission), dispatch bounds each gate command by
`command_timeout` instead of withholding the task, and a gate that needs longer
fails with a structured timeout.

Gate tasks require the worker capability `worker-owned-gate-v1`. Older workers
remain eligible for tasks without a gate; they are excluded from placement for
gate tasks and cannot silently omit a declared gate.

Both ordinary and contained collection execute gates. The contained lane uses
the same validated shell invocation contract as ordinary verification. Current
contained execution exposes its invocation summary rather than full command output;
the retained gate log explicitly reports this limitation, and its tool versions
describe the host rather than the sandbox. Full output remains available in the
ordinary lane.
