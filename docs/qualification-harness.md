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
- Two workers, one per transport, because both are documented and neither
  should lose coverage.

  `worker-a` is the one-shot SSH path: one key, one `authorized_keys` line
  running `t3-steward worker-exchange` with **no operation word**, and the
  operation taken from the verified envelope. A pinned operation word narrows
  that key to one operation, and a worker pinned to `control` cannot take
  artifact delivery, so the lifecycle cases run over the unpinned form. It has
  no daemon: `sshd` starts it once per request.

  `worker-b` is the persistent bridge, a long-running `worker serve` daemon with
  its own bootstrap document and Unix socket, reached through a forced command
  that runs `t3-steward worker bridge`. It is what the fleet runs today.
  Persistent workers must be enrolled, so the harness enrolls it against the
  coordinator's current catalog before any case runs; the one-shot worker has
  no enrollment requirement.

  Each worker has its own configuration, storage roots, journals and secret
  store.
- A client host context: its own home, its own secret store, its own
  `backlog_v2.coordinator_client` configuration, and no access to the
  coordinator's socket.
- A real OpenSSH `sshd`, started unprivileged on a loopback high port, with its
  own host key and its own `authorized_keys` whose every line is a
  `restrict,command="..."` forced command.
- Four throwaway bare Git repositories served over that `sshd` through forced
  `git-upload-pack` commands, plus one forced command that answers with
  Forgejo's exact "Cannot find repository" wording.
- A synthetic T3 provider: a small HTTP server that speaks the endpoints the
  worker and the watchdog use, journals every request, and runs a *scripted
  turn* in the prepared workspace whenever a turn starts. The script stands in
  for the agent. That is what makes the lifecycle cases testable: turn one of a
  parking scenario registers a task-bound wait with the real CLI against the
  real coordinator and stops, and turn two, started by the steward's own wake
  message, writes the declared outputs.

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

- **The provider.** No real agent turn is executed and no provider quota is
  consumed. The fleet's Claude and Codex quota is live and shared and the
  contract calls for synthetic provider observations, so the workers are pointed
  at a stub. Every turn that ran is checked afterwards against the harness's own
  scripts, so a turn that came from anywhere else would be visible.

  What this does prove is everything on the steward's side of the provider
  boundary: workspace preparation, dispatch, the parked lifecycle, collection,
  verification and settlement all run for real. What it cannot prove is how a
  real agent behaves when it is told to end its turn.
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

The two syntax sub-cases need projects whose repository values are invalid, and
those projects must not be in the catalog while the other cases run: a campaign
naming one of them would otherwise be indistinguishable from a campaign naming a
good one. The harness therefore runs everything else against a clean catalog and
then restarts the coordinator with the malformed projects added. The restart is
otherwise ordinary: both workers keep their usual transports, because a
malformed project no longer prevents the coordinator from starting.

## Why the client has two configurations

Both documented shapes of the admin forced command are exercised, because both
are supported and a harness that covered only one could not catch a regression
in the other.

`config-main.yaml` names the unpinned line, which carries no operation word and
takes the operation from the signed frame. That is the ordinary agent path: one
client block and one key serve `campaign check` and then `campaign submit`,
which issues a viability query and a submission in turn.

`config-pinned.yaml` names a line pinned to the `query` operation, which narrows
that key to reads. `coordinator identity` runs over it.

A client cannot vary its SSH destination per operation, so a client that needs
several operations needs the unpinned line.

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
| `execution-path` | A campaign really runs: prepared workspace, dispatched thread, collected output, verification, success. |
| `case11-park` | A task-bound wait moves the attempt to `waiting-external`. |
| `case11-identity-revision` | The execution identity the worker wrote names the attempt revision the coordinator holds. |
| `case11-quiet` | A parked attempt collects nothing, verifies nothing, and leaves its campaign non-terminal. |
| `case11-capacity` | A parked attempt releases its provider slot. |
| `case11-wake` | The same thread resumes, once. |
| `case11-complete` | The woken task collects its output, verification runs exactly once, and the campaign succeeds. |
| `case12` | Racing registration against turn completion never leaves an attempt both waiting and terminal. |
| `case13` | A park survives a worker restart and a coordinator restart: one wake, one resumed turn, one verification. |
| `case14` | A task-bound wait for a terminal attempt is refused and leaves nothing behind. |
| `case15` | An interactive wait from outside task execution wakes its thread and changes no workflow state. |
| `case7` | Three preparation failures keep three immutable logs and the first causal error. |
| `case8` | A legacy intake conflict is reported once across restarts and shown by the quarantine view. |
| `case16-multi-wait` | A second task-bound wait can be registered on an attempt that is already parked. |
| `case16-wake-each` | Settling one wait of an `each` set wakes the attempt. |
| `case16-wake-all` | An `all` set holds the park until every wait in it settles. |
| `attack-commanded-collect` | A control command against a parked attempt collects nothing. |
| `attack-lease-expiry` | An expired lease on a parked attempt collects nothing and does not make it succeed. |
| `identity-survives-the-park` | The workspace identity record is still there for the turn that resumes. |
| `attack-replayed-request-id` | Replaying a settled wait request id is refused rather than answered with the end-your-turn message. |
| `synthetic-provider` | Every turn that ran came from a script in the harness root. |

Some of those cases fail today, and they are meant to. A case that pins an open
defect stays in the suite as a failing case rather than being removed or
softened, so that the day the defect is fixed the suite turns green by itself and
the day it comes back the suite says so. A red run is therefore read case by
case: the report that accompanies a run names which failures are known.

Each case prints its verdict and its evidence separately, so a reader can tell a
proof from an assumption. With `--keep`, every command's output stays under
`<root>/evidence`, together with the coordinator log, the `sshd` log and the
synthetic provider's journal.

## Requirements

`go`, `git`, `git-upload-pack`, `ssh`, `sshd`, `ssh-keygen`, `python3` and
`curl`, a systemd user session, and outbound HTTPS to `github.com` for the
unauthenticated private repository case. Override that repository with
`T3_QUAL_PRIVATE_HTTPS` when the default stops being private.

Two host facts the workers depend on, both reported rather than worked around:
the worker runs its child processes in transient systemd user scopes, so a user
manager must be reachable; and its capability probe asks
`systemctl --user is-active huyang.service`, so a host without that unit
reports the capability missing and enrollment is refused.

`QUAL_RACE_ITERATIONS` sets how many iterations case 12 runs per bias; the
default is three, so six in total, and each is a real campaign.
