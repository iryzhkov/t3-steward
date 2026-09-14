# The disposable qualification harness

This is gate 1 of
[the hardening publication procedure](plans/hardening-publication-gates.md): the
disposable, multi-process qualification that must pass before any live fleet
state is touched. Everything before it is unit and contract testing, which
proves that the parts agree with each other and proves nothing about what
happens across real processes.

```sh
scripts/qualification/run.sh            # build, run every case, clean up
scripts/qualification/run.sh --keep     # keep the temporary root and its evidence
scripts/qualification/run.sh --only 3   # one case group
```

It exits 0 only when every case passed. A case that could not be attempted is a
failure, never a skip.

## What it builds

One temporary root, created with `mktemp -d` and removed again on exit, holds
the whole fleet:

- A coordinator process, started from a binary built out of this worktree, with
  its own configuration, SQLite state, owner-only admin socket, bundle,
  artifact and workspace roots, and legacy drop directory.
- Two worker processes, `worker-a` and `worker-b`, each a real
  `t3-steward worker-exchange control` invoked by a real `sshd` forced command,
  with its own configuration, storage roots, journals and secret store.
- A client host context: its own home, its own secret store, its own
  `backlog_v2.coordinator_client` configuration, and no access to the
  coordinator's socket.
- A real OpenSSH `sshd`, started unprivileged on a loopback high port, with its
  own host key and its own `authorized_keys` whose every line is a
  `restrict,command="..."` forced command.
- Four throwaway bare Git repositories served over that `sshd` through forced
  `git-upload-pack` commands, plus one forced command that answers with
  Forgejo's exact "Cannot find repository" wording.
- A synthetic T3 provider: a small HTTP stub that journals every request and
  refuses every write.

Nothing under the real home is read or written. No live coordinator is
contacted, no worker is enrolled, no provider turn is executed, and `upkeeper`
is never run.

## What is real and what is not

Being precise about this is the point of the gate. "We tested the wrapper, not
sshd" is an honest result; a fabricated sshd test is not.

Real:

- The SSH server is OpenSSH `sshd`. The restriction under test is enforced by
  `sshd` from an `authorized_keys` forced command, not by a stand-in. The
  `restricted-ssh` case presents an arbitrary command on the admin key and
  checks that it did not run and that `sshd` logged the forced command instead.
- The coordinator, both workers and every client invocation are separate
  operating-system processes.
- The repository probe is the product's own `git ls-remote` probe, dispatched
  over the worker protocol and executed on the worker under the worker's own
  environment and credentials.
- The private HTTPS case reaches `github.com` over the network with no
  credential helper available.

Not real, and declared:

- **The provider.** No agent turn is executed. The fleet's Claude and Codex
  quota is live and shared and the contract calls for synthetic provider
  observations, so the workers are pointed at a stub whose journal is checked
  afterwards to confirm that nothing tried to start a turn.
- **Two wrappers around the forced commands.** Each forced command is a short
  script that replaces the login account's ambient environment with the
  disposable one and then executes exactly the documented command. It takes no
  argument from the SSH invocation, unsets `SSH_ORIGINAL_COMMAND` and never
  reads it. It exists because `sshd` gives a forced command the login account's
  real `HOME`, which this gate may not touch.
- **An `ssh` wrapper on the PATH of every harness process.** It adds
  `-F <disposable ssh_config>` and passes every other argument through
  unchanged. It exists because OpenSSH resolves `~/.ssh/config` from the passwd
  database rather than from `$HOME`, so a process given a disposable home would
  otherwise read the real account's SSH configuration and could not be given one
  of its own. The protocol, the server, the forced commands and the per-key
  selection are all real; only the location of the client configuration file is
  supplied by the harness.
- **`XDG_RUNTIME_DIR` and `DBUS_SESSION_BUS_ADDRESS`** are passed to the worker
  processes. The worker runs its child processes in transient systemd user
  scopes, and this `sshd` runs without PAM, so it does not set up the session
  that `pam_systemd` would provide on a real host.
- **The Forgejo case is the wording, not the forge.** A nonexistent repository
  on a real Forgejo instance over SSH answers
  `Forgejo: Cannot find repository: <owner>/<name>`, and that exact refusal is
  what the classifier has to read as permanent. The harness reproduces the
  refusal from a local forced command rather than depending on a live forge.

## Why the coordinator is restarted half way through

A project whose repository syntax is invalid cannot coexist with working
workers: building the worker execution-package catalog fails on it, so every
worker reconciliation fails and the coordinator holds no worker snapshot at all.
The harness therefore runs everything that needs observed workers against a
coordinator whose catalog is clean, and then restarts the coordinator with the
two malformed projects to prove the two syntax refusals, which are task-level
findings and do not need a candidate.

## Why the client has two configurations

A coordinator client declares one SSH destination. The forced command pins one
operation word, and OpenSSH selects the `authorized_keys` line by key, so one
`backlog_v2.coordinator_client` block can reach exactly one
`coordinator-exchange` operation. The harness writes `config-query.yaml` and
`config-submission.yaml`, which differ only in the alias they name, and uses each
for the operation its key is authorized for. This is a property of the product
rather than of the harness; see the harness report for what it means for
`campaign submit`, which needs both operations.

## The cases

| Case | What it proves |
| --- | --- |
| `restricted-ssh` | The forced command refuses an arbitrary command; `sshd` enforces it. |
| `case1-identity` | `coordinator identity` from the client host reports the coordinator and the `ssh` carrier. |
| `case1-submit` | One campaign submitted from the client host creates exactly one workflow run. |
| `remote-viability` | `campaign check` from a host that is not the coordinator returns a matrix. |
| `case2-loss` | The client is killed the instant the coordinator writes, so the response is really lost. |
| `case2-retry` | The same idempotency key leaves exactly one run. |
| `case3-*` | Five impossible campaigns are classified permanent and create zero runs. |
| `case4` | The per-worker candidate matrix distinguishes the worker that can read a private SSH repository from the one that cannot. |
| `provider-turns` | The synthetic provider received no write request. |

Each case prints its verdict and its evidence separately, so a reader can tell a
proof from an assumption. With `--keep`, every command's output stays under
`<root>/evidence`, together with the coordinator log, the `sshd` log and the
synthetic provider's journal.

## Requirements

`go`, `git`, `git-upload-pack`, `ssh`, `sshd`, `ssh-keygen`, `python3` and
`curl`, a systemd user session, and outbound HTTPS to `github.com` for the
unauthenticated private repository case. Override that repository with
`T3_QUAL_PRIVATE_HTTPS` when the default stops being private.
